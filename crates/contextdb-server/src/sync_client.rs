use crate::chunking::needs_chunking;
use crate::protocol::{
    MessageType, PullRequest, PullResponse, PushRequest, PushResponse, WireChangeSet,
    WireRowChange, decode, encode,
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
use std::time::Duration;

const SYNC_TIMEOUT: Duration = Duration::from_secs(10);
const PULL_PAGE_SIZE: u32 = 500;
const MAX_BATCH_BYTES: usize = 512 * 1024;

pub struct SyncClient {
    db: Arc<Database>,
    nats: tokio::sync::Mutex<Option<async_nats::Client>>,
    nats_url: String,
    tenant_id: String,
    push_watermark: AtomicU64,
    pull_watermark: AtomicU64,
    table_directions: std::sync::RwLock<HashMap<String, SyncDirection>>,
    conflict_policies: std::sync::RwLock<ConflictPolicies>,
}

impl SyncClient {
    pub fn new(db: Arc<Database>, nats_url: &str, tenant_id: &str) -> Self {
        assert!(
            !tenant_id.is_empty()
                && tenant_id
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '-' || c == '_'),
            "tenant_id must be non-empty and alphanumeric (hyphens and underscores allowed): {tenant_id}"
        );
        Self {
            db,
            nats: tokio::sync::Mutex::new(None),
            nats_url: nats_url.to_string(),
            tenant_id: tenant_id.to_string(),
            push_watermark: AtomicU64::new(0),
            pull_watermark: AtomicU64::new(0),
            table_directions: std::sync::RwLock::new(HashMap::new()),
            conflict_policies: std::sync::RwLock::new(ConflictPolicies {
                per_table: HashMap::new(),
                default: ConflictPolicy::ServerWins,
            }),
        }
    }

    /// Lazily connect to NATS, reuse existing connection.
    /// Returns cloned Client (cheap — Arc internally) so the mutex is not held during NATS ops.
    /// Returns Err with connection error message if NATS is unreachable.
    async fn ensure_connected(&self) -> Result<Option<async_nats::Client>, String> {
        let mut guard = self.nats.lock().await;
        if guard.is_none() {
            match async_nats::connect(&self.nats_url).await {
                Ok(client) => *guard = Some(client),
                Err(e) => return Err(format!("cannot connect to NATS at {}: {e}", self.nats_url)),
            }
        }
        Ok((*guard).clone())
    }

    /// Drop existing connection and reconnect.
    pub async fn reconnect(&self) {
        let mut guard = self.nats.lock().await;
        *guard = None;
        *guard = async_nats::connect(&self.nats_url).await.ok();
    }

    pub async fn is_connected(&self) -> bool {
        let guard = self.nats.lock().await;
        guard.is_some()
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    pub async fn push(&self) -> Result<ApplyResult, Error> {
        let since = self.push_watermark.load(Ordering::SeqCst);
        // Clone directions out of RwLock BEFORE any .await
        let directions = self.table_directions.read().unwrap().clone();
        let changeset = self
            .db
            .changes_since(since)
            .filter_by_direction(&directions, &[SyncDirection::Push, SyncDirection::Both]);

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

        // Track NATS connection error for proper error reporting when local fallback also fails
        let (nats_client, nats_conn_err) = match self.ensure_connected().await {
            Ok(client) => (client, None),
            Err(e) => (None, Some(e)),
        };

        let mut total = ApplyResult {
            applied_rows: 0,
            skipped_rows: 0,
            conflicts: Vec::new(),
            new_lsn: since,
        };

        let mut last_successful_lsn = since;
        for batch in split_changeset(changeset) {
            let batch_max_lsn = [
                batch.rows.last().map(|r| r.lsn),
                batch.edges.last().map(|e| e.lsn),
                batch.vectors.last().map(|v| v.lsn),
            ]
            .into_iter()
            .flatten()
            .max()
            .unwrap_or(since);

            let request = PushRequest {
                changeset: batch.clone().into(),
            };
            let encoded = encode(MessageType::PushRequest, &request)
                .map_err(|e| Error::SyncError(e.to_string()))?;
            let _chunked = needs_chunking(&encoded);
            let payload = encoded;

            let result = if let Some(client) = &nats_client {
                match tokio::time::timeout(
                    SYNC_TIMEOUT,
                    client.request(push_subject(&self.tenant_id), payload.into()),
                )
                .await
                {
                    Ok(Ok(msg)) => {
                        let envelope =
                            decode(&msg.payload).map_err(|e| Error::SyncError(e.to_string()))?;
                        let response: PushResponse = rmp_serde::from_slice(&envelope.payload)
                            .map_err(|e| Error::SyncError(e.to_string()))?;
                        response.result.into()
                    }
                    Ok(Err(_)) | Err(_) => match local_push(&self.tenant_id, batch) {
                        Ok(r) => r,
                        Err(_) => {
                            return Err(Error::SyncError(
                                "NATS request failed and local fallback unavailable".to_string(),
                            ));
                        }
                    },
                }
            } else {
                match local_push(&self.tenant_id, batch) {
                    Ok(r) => r,
                    Err(_) => {
                        // Return NATS error (actionable) instead of local fallback error
                        let msg = nats_conn_err
                            .as_deref()
                            .unwrap_or("NATS not connected and local fallback unavailable");
                        return Err(Error::SyncError(msg.to_string()));
                    }
                }
            };
            last_successful_lsn = batch_max_lsn;
            total.applied_rows += result.applied_rows;
            total.skipped_rows += result.skipped_rows;
            total.conflicts.extend(result.conflicts);
            total.new_lsn = result.new_lsn;
        }

        self.push_watermark
            .store(last_successful_lsn, Ordering::SeqCst);
        Ok(total)
    }

    /// Pull with explicit policies (frozen test contract, library consumers).
    pub async fn pull(&self, policies: &ConflictPolicies) -> Result<ApplyResult, Error> {
        let nats_client: Option<async_nats::Client> =
            self.ensure_connected().await.unwrap_or_default();
        let directions = self.table_directions.read().unwrap().clone();

        let mut since_lsn = self.pull_watermark.load(Ordering::SeqCst);
        #[allow(unused_assignments)]
        let mut last_server_lsn = since_lsn;
        let mut total = ApplyResult {
            applied_rows: 0,
            skipped_rows: 0,
            conflicts: vec![],
            new_lsn: since_lsn,
        };

        loop {
            let request = PullRequest {
                since_lsn,
                max_entries: Some(PULL_PAGE_SIZE),
            };

            let (changes, has_more, cursor) = if let Some(client) = &nats_client {
                let encoded = encode(MessageType::PullRequest, &request)
                    .map_err(|e| Error::SyncError(e.to_string()))?;
                match tokio::time::timeout(
                    SYNC_TIMEOUT,
                    client.request(pull_subject(&self.tenant_id), encoded.into()),
                )
                .await
                {
                    Ok(Ok(msg)) => {
                        let envelope =
                            decode(&msg.payload).map_err(|e| Error::SyncError(e.to_string()))?;
                        let response: PullResponse = rmp_serde::from_slice(&envelope.payload)
                            .map_err(|e| Error::SyncError(e.to_string()))?;
                        (
                            ChangeSet::from(response.changeset),
                            response.has_more,
                            response.cursor,
                        )
                    }
                    Ok(Err(_)) | Err(_) => {
                        let changes = local_pull(&self.tenant_id, since_lsn)
                            .map_err(|e| Error::SyncError(e.to_string()))?;
                        (changes, false, None)
                    }
                }
            } else {
                let changes = local_pull(&self.tenant_id, since_lsn)
                    .map_err(|e| Error::SyncError(e.to_string()))?;
                (changes, false, None)
            };

            // Extract server-side max LSN BEFORE filtering/applying
            let server_lsn = [
                changes.rows.last().map(|r| r.lsn),
                changes.edges.last().map(|e| e.lsn),
                changes.vectors.last().map(|v| v.lsn),
            ]
            .into_iter()
            .flatten()
            .max()
            .unwrap_or(since_lsn);

            let filtered = changes
                .filter_by_direction(&directions, &[SyncDirection::Pull, SyncDirection::Both]);
            let result = self
                .db
                .apply_changes(filtered, &remap_pull_policies(policies))?;
            total.applied_rows += result.applied_rows;
            total.skipped_rows += result.skipped_rows;
            total.conflicts.extend(result.conflicts);
            total.new_lsn = result.new_lsn;
            last_server_lsn = server_lsn;

            if !has_more {
                break;
            }
            since_lsn = cursor.unwrap_or(since_lsn);
        }

        self.pull_watermark.store(last_server_lsn, Ordering::SeqCst);
        Ok(total)
    }

    /// Pull using internally configured conflict policies (used by CLI).
    pub async fn pull_default(&self) -> Result<ApplyResult, Error> {
        let policies = self.conflict_policies.read().unwrap().clone();
        self.pull(&policies).await
    }

    /// Initial sync using explicit policies (frozen test contract).
    pub async fn initial_sync(&self, policies: &ConflictPolicies) -> Result<ApplyResult, Error> {
        self.pull(policies).await
    }

    pub fn push_watermark(&self) -> u64 {
        self.push_watermark.load(Ordering::SeqCst)
    }

    pub fn pull_watermark(&self) -> u64 {
        self.pull_watermark.load(Ordering::SeqCst)
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub fn nats_url(&self) -> &str {
        &self.nats_url
    }

    pub fn set_table_direction(&self, table: &str, direction: SyncDirection) {
        self.table_directions
            .write()
            .unwrap()
            .insert(table.to_string(), direction);
    }

    pub fn set_conflict_policy(&self, table: &str, policy: ConflictPolicy) {
        self.conflict_policies
            .write()
            .unwrap()
            .per_table
            .insert(table.to_string(), policy);
    }

    pub fn set_default_conflict_policy(&self, policy: ConflictPolicy) {
        self.conflict_policies.write().unwrap().default = policy;
    }
}

pub(crate) fn split_changeset(changeset: ChangeSet) -> Vec<ChangeSet> {
    let wire = WireChangeSet::from(changeset.clone());
    let estimated = rmp_serde::to_vec(&wire).map(|v| v.len()).unwrap_or(0);
    if estimated <= MAX_BATCH_BYTES {
        return vec![changeset];
    }

    // Estimate per-row sizes by serializing each WireRowChange individually
    let row_sizes: Vec<usize> = changeset
        .rows
        .iter()
        .map(|r| {
            let wire_row = WireRowChange::from(r.clone());
            rmp_serde::to_vec(&wire_row).map(|v| v.len()).unwrap_or(128)
        })
        .collect();

    let mut batches = Vec::new();
    let mut batch_rows = Vec::new();
    let mut batch_vectors = Vec::new();
    let mut batch_size = 0usize;
    // Extract edges, vectors, and ddl BEFORE consuming rows via into_iter()
    let changeset_edges = changeset.edges;
    let changeset_vectors = changeset.vectors;
    let changeset_ddl = changeset.ddl;

    // Overhead for edges + DDL in first batch
    let first_batch_overhead = {
        let overhead_set = ChangeSet {
            rows: Vec::new(),
            edges: changeset_edges.clone(),
            vectors: Vec::new(),
            ddl: changeset_ddl.clone(),
        };
        rmp_serde::to_vec(&WireChangeSet::from(overhead_set))
            .map(|v| v.len())
            .unwrap_or(0)
    };

    for (i, row) in changeset.rows.into_iter().enumerate() {
        let row_size = row_sizes.get(i).copied().unwrap_or(128);
        let overhead = if batches.is_empty() {
            first_batch_overhead
        } else {
            0
        };

        if batch_size + row_size + overhead > MAX_BATCH_BYTES && !batch_rows.is_empty() {
            batches.push(ChangeSet {
                rows: std::mem::take(&mut batch_rows),
                edges: if batches.is_empty() {
                    changeset_edges.clone()
                } else {
                    Vec::new()
                },
                vectors: std::mem::take(&mut batch_vectors),
                ddl: if batches.is_empty() {
                    changeset_ddl.clone()
                } else {
                    Vec::new()
                },
            });
            batch_size = 0;
        }

        // Pair with vector at same index if available
        if i < changeset_vectors.len() {
            batch_vectors.push(changeset_vectors[i].clone());
        }
        batch_rows.push(row);
        batch_size += row_size;
    }

    if !batch_rows.is_empty() {
        batches.push(ChangeSet {
            rows: batch_rows,
            edges: if batches.is_empty() {
                changeset_edges
            } else {
                Vec::new()
            },
            vectors: batch_vectors,
            ddl: if batches.is_empty() {
                changeset_ddl
            } else {
                Vec::new()
            },
        });
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

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_core::Value;
    use contextdb_engine::sync_types::NaturalKey;
    use contextdb_engine::sync_types::RowChange;
    use uuid::Uuid;

    // A14: Batch splitting respects byte size limits
    #[test]
    fn a14_batch_splitting_respects_byte_limits() {
        // Build a changeset with 10 rows, each ~100KB of data (total ~1MB)
        let large_text = "x".repeat(100 * 1024); // ~100KB per row
        let mut rows = Vec::new();
        for _ in 0..10 {
            let id = Uuid::new_v4();
            let mut values = HashMap::new();
            values.insert("id".to_string(), Value::Uuid(id));
            values.insert("data".to_string(), Value::Text(large_text.clone()));
            rows.push(RowChange {
                table: "t".to_string(),
                natural_key: NaturalKey {
                    column: "id".to_string(),
                    value: Value::Uuid(id),
                },
                values,
                lsn: 1,
            });
        }

        let changeset = ChangeSet {
            rows,
            edges: Vec::new(),
            vectors: Vec::new(),
            ddl: vec![contextdb_engine::sync_types::DdlChange::CreateTable {
                name: "t".to_string(),
                columns: vec![
                    ("id".to_string(), "UUID".to_string()),
                    ("data".to_string(), "TEXT".to_string()),
                ],
                constraints: vec!["PRIMARY KEY (id)".to_string()],
            }],
        };

        let batches = split_changeset(changeset);

        // Must split into 2+ batches (10 rows * ~100KB > 512KB)
        assert!(
            batches.len() >= 2,
            "10 rows of ~100KB each (~1MB total) must split into at least 2 batches, got {}",
            batches.len()
        );

        // Each batch's serialized size must be under 512KB
        for (i, batch) in batches.iter().enumerate() {
            let wire = WireChangeSet::from(batch.clone());
            let size = rmp_serde::to_vec(&wire).unwrap().len();
            assert!(
                size <= 512 * 1024,
                "batch {} serialized to {} bytes, exceeds 512KB limit",
                i,
                size
            );
        }

        // DDL only in first batch
        assert!(!batches[0].ddl.is_empty(), "DDL must be in first batch");
        for batch in &batches[1..] {
            assert!(
                batch.ddl.is_empty(),
                "DDL must NOT be in subsequent batches"
            );
            assert!(
                batch.edges.is_empty(),
                "edges must NOT be in subsequent batches"
            );
        }
    }
}
