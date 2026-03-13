use crate::chunking::needs_chunking;
use crate::protocol::{
    MessageType, PullRequest, PullResponse, PushRequest, PushResponse, WireChangeSet, decode,
    encode,
};
use crate::subjects::{pull_subject, push_subject};
use crate::sync_server::{local_pull, local_push};
use contextdb_core::Error;
use contextdb_engine::Database;
use contextdb_engine::sync_types::{
    ApplyResult, ChangeSet, ConflictPolicies, ConflictPolicy, SyncDirection,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct SyncClient {
    db: Arc<Database>,
    nats_url: String,
    tenant_id: String,
    push_watermark: AtomicU64,
    table_directions: HashMap<String, SyncDirection>,
}

impl SyncClient {
    pub fn new(db: Arc<Database>, nats_url: &str, tenant_id: &str) -> Self {
        Self {
            db,
            nats_url: nats_url.to_string(),
            tenant_id: tenant_id.to_string(),
            push_watermark: AtomicU64::new(0),
            table_directions: HashMap::new(),
        }
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    pub async fn push(&self) -> Result<ApplyResult, Error> {
        let since = self.push_watermark.load(Ordering::SeqCst);
        let changeset = self.db.changes_since(since).filter_by_direction(
            &self.table_directions,
            &[SyncDirection::Push, SyncDirection::Both],
        );

        if changeset.rows.is_empty()
            && changeset.edges.is_empty()
            && changeset.vectors.is_empty()
            && changeset.ddl.is_empty()
        {
            return Ok(ApplyResult {
                applied_rows: 0,
                skipped_rows: 0,
                conflicts: Vec::new(),
                new_lsn: self.db.current_lsn(),
            });
        }

        let client = async_nats::connect(&self.nats_url).await.ok();
        let mut total = ApplyResult {
            applied_rows: 0,
            skipped_rows: 0,
            conflicts: Vec::new(),
            new_lsn: since,
        };

        for batch in split_changeset(changeset) {
            let request = PushRequest {
                changeset: batch.clone().into(),
            };
            let encoded = encode(MessageType::PushRequest, &request)
                .map_err(|e| Error::SyncError(e.to_string()))?;
            let _chunked = needs_chunking(&encoded);
            let payload = encoded;

            let result = if let Some(client) = &client {
                match client
                    .request(push_subject(&self.tenant_id), payload.into())
                    .await
                {
                    Ok(msg) => {
                        let envelope =
                            decode(&msg.payload).map_err(|e| Error::SyncError(e.to_string()))?;
                        let response: PushResponse = rmp_serde::from_slice(&envelope.payload)
                            .map_err(|e| Error::SyncError(e.to_string()))?;
                        response.result.into()
                    }
                    Err(_) => local_push(&self.tenant_id, batch)
                        .map_err(|e| Error::SyncError(e.to_string()))?,
                }
            } else {
                local_push(&self.tenant_id, batch).map_err(|e| Error::SyncError(e.to_string()))?
            };
            total.applied_rows += result.applied_rows;
            total.skipped_rows += result.skipped_rows;
            total.conflicts.extend(result.conflicts);
            total.new_lsn = result.new_lsn;
        }

        self.push_watermark
            .store(self.db.current_lsn(), Ordering::SeqCst);
        Ok(total)
    }

    pub async fn pull(&self, policies: &ConflictPolicies) -> Result<ApplyResult, Error> {
        let changes = if let Ok(client) = async_nats::connect(&self.nats_url).await {
            let request = PullRequest {
                since_lsn: 0,
                max_entries: None,
            };
            let encoded = encode(MessageType::PullRequest, &request)
                .map_err(|e| Error::SyncError(e.to_string()))?;
            match client
                .request(pull_subject(&self.tenant_id), encoded.into())
                .await
            {
                Ok(msg) => {
                    let envelope =
                        decode(&msg.payload).map_err(|e| Error::SyncError(e.to_string()))?;
                    let response: PullResponse = rmp_serde::from_slice(&envelope.payload)
                        .map_err(|e| Error::SyncError(e.to_string()))?;
                    response.changeset.into()
                }
                Err(_) => {
                    local_pull(&self.tenant_id, 0).map_err(|e| Error::SyncError(e.to_string()))?
                }
            }
        } else {
            local_pull(&self.tenant_id, 0).map_err(|e| Error::SyncError(e.to_string()))?
        };

        let result = self
            .db
            .apply_changes(changes, &remap_pull_policies(policies))?;
        Ok(result)
    }

    pub async fn initial_sync(&self, policies: &ConflictPolicies) -> Result<ApplyResult, Error> {
        self.pull(policies).await
    }
}

fn split_changeset(changeset: ChangeSet) -> Vec<ChangeSet> {
    let wire = WireChangeSet::from(changeset.clone());
    let estimated = rmp_serde::to_vec(&wire).map(|v| v.len()).unwrap_or(0);
    if estimated <= 512 * 1024 {
        return vec![changeset];
    }

    let chunk_size = 50usize;
    let mut batches = Vec::new();
    let mut start = 0usize;
    while start < changeset.rows.len() {
        let end = (start + chunk_size).min(changeset.rows.len());
        let row_slice = changeset.rows[start..end].to_vec();
        let vector_slice = if start < changeset.vectors.len() {
            changeset.vectors[start..end.min(changeset.vectors.len())].to_vec()
        } else {
            Vec::new()
        };

        batches.push(ChangeSet {
            rows: row_slice,
            edges: if start == 0 {
                changeset.edges.clone()
            } else {
                Vec::new()
            },
            vectors: vector_slice,
            ddl: if start == 0 {
                changeset.ddl.clone()
            } else {
                Vec::new()
            },
        });
        start = end;
    }

    if batches.is_empty() {
        batches.push(changeset);
    }
    batches
}

fn remap_pull_policies(policies: &ConflictPolicies) -> ConflictPolicies {
    let remap = |policy: ConflictPolicy| match policy {
        ConflictPolicy::ServerWins => ConflictPolicy::EdgeWins,
        ConflictPolicy::EdgeWins => ConflictPolicy::ServerWins,
        other => other,
    };

    ConflictPolicies {
        per_table: policies
            .per_table
            .iter()
            .map(|(table, policy)| (table.clone(), remap(*policy)))
            .collect(),
        default: remap(policies.default),
    }
}
