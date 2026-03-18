use crate::protocol::{
    ChunkAck, MessageType, PullRequest, PullResponse, PushRequest, PushResponse, WireChangeSet,
    WireRowChange, decode, encode,
};
use crate::subjects::{pull_subject, push_subject};
use contextdb_core::Error;
use contextdb_engine::Database;
use contextdb_engine::sync_types::{
    ApplyResult, ChangeSet, ConflictPolicies, ConflictPolicy, SyncDirection,
};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const SYNC_TIMEOUT: Duration = Duration::from_secs(10);
/// Overall deadline for collecting all chunks in a chunked pull response.
const CHUNK_COLLECT_TIMEOUT: Duration = Duration::from_secs(30);
const PULL_PAGE_SIZE: u32 = 500;
const MAX_BATCH_BYTES: usize = 800 * 1024;

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
    pub async fn ensure_connected(&self) -> Result<async_nats::Client, String> {
        let mut guard = self.nats.lock().await;
        if guard.is_none() {
            match async_nats::connect(&self.nats_url).await {
                Ok(client) => *guard = Some(client),
                Err(e) => return Err(format!("cannot connect to NATS at {}: {e}", self.nats_url)),
            }
        }
        guard
            .clone()
            .ok_or_else(|| format!("cannot connect to NATS at {}", self.nats_url))
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

        let nats_client = self.ensure_connected().await.map_err(Error::SyncError)?;

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

            let result: ApplyResult = if crate::chunking::needs_chunking(&encoded) {
                use crate::chunking::chunk;

                tracing::info!(
                    payload_size = encoded.len(),
                    "push payload exceeds chunking threshold, using chunked send"
                );

                let inbox = nats_client.new_inbox();
                let mut inbox_sub = nats_client
                    .subscribe(inbox.clone())
                    .await
                    .map_err(|e| Error::SyncError(e.to_string()))?;

                let subject = push_subject(&self.tenant_id);
                let chunks = chunk(&encoded);
                let chunk_id = chunks[0].chunk_id;
                let total_chunks = chunks[0].total_chunks;

                tracing::debug!(
                    %chunk_id,
                    total_chunks,
                    "sending {} chunks for push request",
                    total_chunks
                );

                for chunk_msg in &chunks {
                    let chunk_encoded = encode(MessageType::Chunk, chunk_msg)
                        .map_err(|e| Error::SyncError(e.to_string()))?;
                    nats_client
                        .publish(subject.clone(), chunk_encoded.into())
                        .await
                        .map_err(|e| Error::SyncError(e.to_string()))?;
                }

                nats_client
                    .flush()
                    .await
                    .map_err(|e| Error::SyncError(e.to_string()))?;

                let ack = ChunkAck {
                    chunk_id,
                    total_chunks,
                    reply_inbox: inbox.clone(),
                };
                let ack_encoded = encode(MessageType::ChunkAck, &ack)
                    .map_err(|e| Error::SyncError(e.to_string()))?;
                nats_client
                    .publish(subject, ack_encoded.into())
                    .await
                    .map_err(|e| Error::SyncError(e.to_string()))?;
                nats_client
                    .flush()
                    .await
                    .map_err(|e| Error::SyncError(e.to_string()))?;

                let msg = tokio::time::timeout(SYNC_TIMEOUT, inbox_sub.next())
                    .await
                    .map_err(|_| Error::SyncError("chunked push timed out".to_string()))?
                    .ok_or_else(|| {
                        Error::SyncError("inbox closed before push response".to_string())
                    })?;
                let envelope = decode(&msg.payload).map_err(|e| Error::SyncError(e.to_string()))?;
                let response: PushResponse = rmp_serde::from_slice(&envelope.payload)
                    .map_err(|e| Error::SyncError(e.to_string()))?;
                response.result.into()
            } else {
                match tokio::time::timeout(
                    SYNC_TIMEOUT,
                    nats_client.request(push_subject(&self.tenant_id), encoded.into()),
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
                    Ok(Err(e)) => return Err(Error::SyncError(e.to_string())),
                    Err(_) => {
                        return Err(Error::SyncError(
                            "NATS request timed out waiting for push response".to_string(),
                        ));
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
        let nats_client = self.ensure_connected().await.map_err(Error::SyncError)?;
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

            let (changes, has_more, cursor) = {
                let encoded = encode(MessageType::PullRequest, &request)
                    .map_err(|e| Error::SyncError(e.to_string()))?;

                let inbox = nats_client.new_inbox();
                let mut inbox_sub = nats_client
                    .subscribe(inbox.clone())
                    .await
                    .map_err(|e| Error::SyncError(e.to_string()))?;

                nats_client
                    .publish_with_reply(
                        pull_subject(&self.tenant_id),
                        inbox.clone(),
                        encoded.into(),
                    )
                    .await
                    .map_err(|e| Error::SyncError(e.to_string()))?;

                let first_msg = tokio::time::timeout(SYNC_TIMEOUT, inbox_sub.next())
                    .await
                    .map_err(|_| {
                        Error::SyncError(
                            "NATS request timed out waiting for pull response".to_string(),
                        )
                    })?
                    .ok_or_else(|| Error::SyncError("pull inbox closed".to_string()))?;

                let first_envelope =
                    decode(&first_msg.payload).map_err(|e| Error::SyncError(e.to_string()))?;

                let response_envelope = match first_envelope.message_type {
                    MessageType::PullResponse => first_envelope,
                    MessageType::Chunk => {
                        let first_chunk: crate::protocol::ChunkMessage =
                            rmp_serde::from_slice(&first_envelope.payload)
                                .map_err(|e| Error::SyncError(e.to_string()))?;
                        let total = first_chunk.total_chunks;
                        let mut collected = vec![first_chunk];

                        tracing::debug!(
                            total_chunks = total,
                            "pull response is chunked, collecting chunks"
                        );

                        let deadline = tokio::time::Instant::now() + CHUNK_COLLECT_TIMEOUT;

                        while collected.len() < total as usize {
                            let remaining = deadline.duration_since(tokio::time::Instant::now());
                            if remaining.is_zero() {
                                return Err(Error::SyncError(format!(
                                    "overall chunk collection deadline exceeded after {}/{} chunks",
                                    collected.len(),
                                    total
                                )));
                            }
                            let chunk_msg = tokio::time::timeout_at(deadline, inbox_sub.next())
                                .await
                                .map_err(|_| {
                                    Error::SyncError(format!(
                                        "timeout collecting pull chunks ({}/{})",
                                        collected.len(),
                                        total
                                    ))
                                })?
                                .ok_or_else(|| {
                                    Error::SyncError("pull chunk stream ended".to_string())
                                })?;
                            let env = decode(&chunk_msg.payload)
                                .map_err(|e| Error::SyncError(e.to_string()))?;
                            if matches!(env.message_type, MessageType::Chunk) {
                                let c: crate::protocol::ChunkMessage =
                                    rmp_serde::from_slice(&env.payload)
                                        .map_err(|e| Error::SyncError(e.to_string()))?;
                                collected.push(c);
                            } else {
                                return Err(Error::SyncError(format!(
                                    "unexpected message type {:?} while collecting pull chunks",
                                    env.message_type
                                )));
                            }
                        }
                        let reassembled = crate::chunking::reassemble(&mut collected);
                        decode(&reassembled).map_err(|e| Error::SyncError(e.to_string()))?
                    }
                    _ => {
                        return Err(Error::SyncError(
                            "unexpected message type in pull response".to_string(),
                        ));
                    }
                };

                let response: PullResponse = rmp_serde::from_slice(&response_envelope.payload)
                    .map_err(|e| Error::SyncError(e.to_string()))?;
                (
                    ChangeSet::from(response.changeset),
                    response.has_more,
                    response.cursor,
                )
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
    let vector_sizes: Vec<usize> = changeset
        .vectors
        .iter()
        .map(|v| {
            let wire_vec = crate::protocol::WireVectorChange::from(v.clone());
            rmp_serde::to_vec(&wire_vec).map(|v| v.len()).unwrap_or(64)
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
    let edges_size: usize = {
        let edges_wire: Vec<crate::protocol::WireEdgeChange> =
            changeset_edges.iter().cloned().map(Into::into).collect();
        rmp_serde::to_vec(&edges_wire).map(|v| v.len()).unwrap_or(0)
    };
    let ddl_size: usize = {
        let ddl_wire: Vec<crate::protocol::WireDdlChange> =
            changeset_ddl.iter().cloned().map(Into::into).collect();
        rmp_serde::to_vec(&ddl_wire).map(|v| v.len()).unwrap_or(0)
    };
    let first_batch_overhead = edges_size + ddl_size;

    for (i, row) in changeset.rows.into_iter().enumerate() {
        let row_size = row_sizes.get(i).copied().unwrap_or(128);
        let vec_size_for_i = vector_sizes.get(i).copied().unwrap_or(64);
        let overhead = if batches.is_empty() {
            first_batch_overhead
        } else {
            0
        };

        let should_flush = if batch_rows.is_empty() {
            false
        } else {
            let mut trial_rows = batch_rows.clone();
            trial_rows.push(row.clone());
            let mut trial_vectors = batch_vectors.clone();
            if i < changeset_vectors.len() {
                trial_vectors.push(changeset_vectors[i].clone());
            }
            let trial = ChangeSet {
                rows: trial_rows.clone(),
                edges: if batches.is_empty() {
                    changeset_edges.clone()
                } else {
                    Vec::new()
                },
                vectors: trial_vectors,
                ddl: if batches.is_empty() {
                    changeset_ddl.clone()
                } else {
                    Vec::new()
                },
            };
            let actual_size = rmp_serde::to_vec(&WireChangeSet::from(trial))
                .map(|v| v.len())
                .unwrap_or(usize::MAX);
            batch_size + row_size + vec_size_for_i + overhead > MAX_BATCH_BYTES
                || actual_size > MAX_BATCH_BYTES
        };

        if should_flush {
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
            batch_size += vector_sizes.get(i).copied().unwrap_or(64);
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
    } else if batches.is_empty() && (!changeset_edges.is_empty() || !changeset_ddl.is_empty()) {
        batches.push(ChangeSet {
            rows: Vec::new(),
            edges: changeset_edges,
            vectors: Vec::new(),
            ddl: changeset_ddl,
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
    use contextdb_engine::sync_types::{NaturalKey, RowChange, VectorChange};
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

        // Must split into 2+ batches (10 rows * ~100KB > 800KB)
        assert!(
            batches.len() >= 2,
            "10 rows of ~100KB each (~1MB total) must split into at least 2 batches, got {}",
            batches.len()
        );

        // Each batch's serialized size must be under 800KB
        for (i, batch) in batches.iter().enumerate() {
            let wire = WireChangeSet::from(batch.clone());
            let size = rmp_serde::to_vec(&wire).unwrap().len();
            assert!(
                size <= 800 * 1024,
                "batch {} serialized to {} bytes, exceeds 800KB limit",
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

    #[test]
    fn a14b_batch_splitting_accounts_for_vector_sizes() {
        let mut rows = Vec::new();
        let mut vectors = Vec::new();
        for _ in 0..200 {
            let id = Uuid::new_v4();
            let mut values = HashMap::new();
            values.insert("id".to_string(), Value::Uuid(id));
            values.insert("data".to_string(), Value::Text("x".repeat(3000)));
            rows.push(RowChange {
                table: "t".to_string(),
                natural_key: NaturalKey {
                    column: "id".to_string(),
                    value: Value::Uuid(id),
                },
                values,
                lsn: 1,
            });
            vectors.push(VectorChange {
                row_id: 0,
                vector: (0..384).map(|j| j as f32).collect(),
                lsn: 1,
            });
        }
        let changeset = ChangeSet {
            rows,
            edges: Vec::new(),
            vectors,
            ddl: vec![],
        };
        let batches = split_changeset(changeset);
        assert!(
            batches.len() >= 2,
            "200 rows with 384-dim vectors must split into 2+ batches with correct accounting, got {}",
            batches.len()
        );
        for (i, batch) in batches.iter().enumerate() {
            let wire = WireChangeSet::from(batch.clone());
            let size = rmp_serde::to_vec(&wire).unwrap().len();
            assert!(
                size <= 800 * 1024,
                "batch {} serialized to {} bytes, exceeds 800KB limit",
                i,
                size
            );
        }
    }

    // A15: split_changeset handles a single row that alone exceeds MAX_BATCH_BYTES
    #[test]
    fn a15_split_changeset_single_oversized_row() {
        let oversized_text = "x".repeat(600 * 1024);
        let id = Uuid::new_v4();
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::Uuid(id));
        values.insert("data".to_string(), Value::Text(oversized_text));
        let row = RowChange {
            table: "observations".to_string(),
            natural_key: NaturalKey {
                column: "id".to_string(),
                value: Value::Uuid(id),
            },
            values,
            lsn: 1,
        };
        let changeset = ChangeSet {
            rows: vec![row],
            edges: Vec::new(),
            vectors: Vec::new(),
            ddl: Vec::new(),
        };

        let batches = split_changeset(changeset);

        assert!(
            !batches.is_empty(),
            "split_changeset must return at least one batch, got {}",
            batches.len()
        );
        let total_rows: usize = batches.iter().map(|b| b.rows.len()).sum();
        assert_eq!(
            total_rows, 1,
            "the single oversized row must appear in exactly one batch, got {}",
            total_rows
        );
    }

    // A16: split_changeset preserves row/vector pairing across batch boundaries
    #[test]
    fn a16_split_changeset_preserves_row_vector_pairing() {
        use contextdb_engine::sync_types::VectorChange;

        let mut rows = Vec::new();
        let mut vectors = Vec::new();
        for i in 0..10usize {
            let id = Uuid::new_v4();
            let mut values = HashMap::new();
            values.insert("id".to_string(), Value::Uuid(id));
            values.insert("data".to_string(), Value::Text("x".repeat(100 * 1024)));
            rows.push(RowChange {
                table: "observations".to_string(),
                natural_key: NaturalKey {
                    column: "id".to_string(),
                    value: Value::Uuid(id),
                },
                values,
                lsn: (i + 1) as u64,
            });
            vectors.push(VectorChange {
                row_id: (i + 1) as u64,
                vector: vec![i as f32; 3],
                lsn: (i + 1) as u64,
            });
        }
        let changeset = ChangeSet {
            rows,
            edges: Vec::new(),
            vectors,
            ddl: Vec::new(),
        };

        let batches = split_changeset(changeset);

        assert!(
            batches.len() >= 2,
            "10 rows * ~100KB each must split into at least 2 batches, got {}",
            batches.len()
        );
        let total_rows: usize = batches.iter().map(|b| b.rows.len()).sum();
        let total_vecs: usize = batches.iter().map(|b| b.vectors.len()).sum();
        assert_eq!(total_rows, 10, "all 10 rows must be present across batches");
        assert_eq!(
            total_vecs, 10,
            "all 10 vectors must be present across batches"
        );
        for (i, batch) in batches.iter().enumerate() {
            assert_eq!(
                batch.rows.len(),
                batch.vectors.len(),
                "batch {} must have equal row and vector counts: rows={}, vectors={}",
                i,
                batch.rows.len(),
                batch.vectors.len()
            );
            for j in 0..batch.rows.len() {
                assert_eq!(
                    batch.rows[j].lsn, batch.vectors[j].lsn,
                    "batch {} position {}: row.lsn={} != vector.lsn={} — pairing is broken",
                    i, j, batch.rows[j].lsn, batch.vectors[j].lsn
                );
            }
        }
    }

    // A17: split_changeset on empty input returns exactly one empty batch
    #[test]
    fn a17_split_changeset_empty_input_returns_one_batch() {
        let changeset = ChangeSet {
            rows: Vec::new(),
            edges: Vec::new(),
            vectors: Vec::new(),
            ddl: Vec::new(),
        };

        let batches = split_changeset(changeset);

        assert_eq!(
            batches.len(),
            1,
            "empty changeset must produce exactly 1 batch (not 0), got {}",
            batches.len()
        );
        assert!(
            batches[0].rows.is_empty(),
            "the single batch for an empty input must have no rows"
        );
    }

    // A18: split_changeset with edge-only changeset must not return vec![]
    #[test]
    fn a18_split_changeset_edge_only_not_dropped() {
        use contextdb_engine::sync_types::EdgeChange;

        let mut edges = Vec::new();
        for _ in 0..200 {
            edges.push(EdgeChange {
                source: Uuid::new_v4(),
                target: Uuid::new_v4(),
                edge_type: "x".repeat(5_000),
                properties: HashMap::new(),
                lsn: 1,
            });
        }
        let changeset = ChangeSet {
            rows: Vec::new(),
            edges,
            vectors: Vec::new(),
            ddl: Vec::new(),
        };

        let batches = split_changeset(changeset);

        assert!(
            !batches.is_empty(),
            "edge-only changeset must produce at least 1 batch, got {} — edges silently dropped",
            batches.len()
        );
        let total_edges: usize = batches.iter().map(|b| b.edges.len()).sum();
        assert_eq!(
            total_edges, 200,
            "all 200 edges must be present across batches, got {}",
            total_edges
        );
    }

    // A19: split_changeset with DDL-only changeset must not return vec![]
    // Column names are padded to force estimated size > MAX_BATCH_BYTES
    #[test]
    fn a19_split_changeset_ddl_only_not_dropped() {
        use contextdb_engine::sync_types::DdlChange;

        let mut ddl = Vec::new();
        for i in 0..20 {
            ddl.push(DdlChange::CreateTable {
                name: format!("table_{}", i),
                columns: (0..100)
                    .map(|j| (format!("col_{}_{}", j, "x".repeat(500)), "TEXT".to_string()))
                    .collect(),
                constraints: vec![format!("PRIMARY KEY (col_{})", "x".repeat(500))],
            });
        }
        let changeset = ChangeSet {
            rows: Vec::new(),
            edges: Vec::new(),
            vectors: Vec::new(),
            ddl,
        };

        let batches = split_changeset(changeset);

        assert!(
            !batches.is_empty(),
            "DDL-only changeset must produce at least 1 batch, got {} — DDL silently dropped",
            batches.len()
        );
        let total_ddl: usize = batches.iter().map(|b| b.ddl.len()).sum();
        assert_eq!(
            total_ddl, 20,
            "all 20 DDL entries must be present across batches, got {}",
            total_ddl
        );
    }
}
