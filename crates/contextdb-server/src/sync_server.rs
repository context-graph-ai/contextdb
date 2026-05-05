use crate::protocol::{
    MessageType, PullRequest, PullResponse, PushRequest, PushResponse, decode, encode,
};
use crate::subjects::{pull_subject, push_subject};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

/// Max concurrent in-flight chunked push sessions. Rejects new chunk_ids past this limit.
const MAX_CHUNK_SESSIONS: usize = 64;

async fn maybe_wait_for_test_push_barrier(row_count: usize) {
    let Some(min_rows) = std::env::var("CONTEXTDB_TEST_PUSH_BARRIER_MIN_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    else {
        return;
    };
    if row_count < min_rows {
        return;
    }

    let Some(barrier_path) = std::env::var_os("CONTEXTDB_TEST_PUSH_BARRIER_FILE") else {
        return;
    };
    let Some(release_path) = std::env::var_os("CONTEXTDB_TEST_PUSH_RELEASE_FILE") else {
        return;
    };

    let barrier_path = std::path::PathBuf::from(barrier_path);
    let release_path = std::path::PathBuf::from(release_path);
    let _ = std::fs::write(&barrier_path, b"push-handler-started");

    while !release_path.exists() {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

pub struct SyncServer {
    db: Arc<Database>,
    nats_url: String,
    tenant_id: String,
    policies: ConflictPolicies,
    chunk_buffer:
        std::sync::Mutex<HashMap<uuid::Uuid, (Instant, Vec<crate::protocol::ChunkMessage>)>>,
}

impl SyncServer {
    pub fn new(
        db: Arc<Database>,
        nats_url: &str,
        tenant_id: &str,
        policies: ConflictPolicies,
    ) -> Self {
        assert!(
            !tenant_id.is_empty()
                && tenant_id
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '-' || c == '_'),
            "tenant_id must be non-empty and alphanumeric (hyphens and underscores allowed): {tenant_id}"
        );
        Self {
            db,
            nats_url: nats_url.to_string(),
            tenant_id: tenant_id.to_string(),
            policies,
            chunk_buffer: std::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    pub async fn run(&self) {
        self.run_until(Arc::new(AtomicBool::new(false))).await;
    }

    pub async fn run_until(&self, shutdown: Arc<AtomicBool>) {
        let client = loop {
            if shutdown.load(Ordering::SeqCst) {
                return;
            }
            match async_nats::connect(&self.nats_url).await {
                Ok(client) => break client,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(200)).await,
            }
        };

        let (mut push_sub, mut pull_sub) = loop {
            if shutdown.load(Ordering::SeqCst) {
                return;
            }
            match self.bootstrap_subscriptions(&client).await {
                Ok(subscriptions) => break subscriptions,
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        tenant_id = %self.tenant_id,
                        "sync server bootstrap failed; retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            }
        };
        let mut cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(10));
        let mut shutdown_poll = tokio::time::interval(std::time::Duration::from_millis(50));

        while !shutdown.load(Ordering::SeqCst) {
            tokio::select! {
                maybe_msg = push_sub.next() => {
                    if let Some(msg) = maybe_msg
                        && let Err(e) = self.handle_push(&client, msg).await {
                        tracing::error!(error = %e, "handle_push failed");
                    }
                }
                maybe_msg = pull_sub.next() => {
                    if let Some(msg) = maybe_msg
                        && let Err(e) = self.handle_pull(&client, msg).await {
                        tracing::error!(error = %e, "handle_pull failed");
                    }
                }
                _ = cleanup_interval.tick() => {
                    let mut buf = self.chunk_buffer.lock().unwrap_or_else(|e| e.into_inner());
                    let before = buf.len();
                    buf.retain(|id, (ts, _chunks)| {
                        let keep = ts.elapsed() < std::time::Duration::from_secs(30);
                        if !keep {
                            tracing::warn!(%id, "evicting stale chunk session (30s TTL expired)");
                        }
                        keep
                    });
                    if buf.len() < before {
                        tracing::info!(evicted = before - buf.len(), remaining = buf.len(), "chunk buffer cleanup");
                    }
                }
                _ = shutdown_poll.tick() => {}
            }
        }
    }

    async fn bootstrap_subscriptions(
        &self,
        client: &async_nats::Client,
    ) -> contextdb_core::Result<(async_nats::Subscriber, async_nats::Subscriber)> {
        let push_sub = client
            .subscribe(push_subject(&self.tenant_id))
            .await
            .map_err(|e| contextdb_core::Error::SyncError(format!("subscribe push: {e}")))?;
        let pull_sub = client
            .subscribe(pull_subject(&self.tenant_id))
            .await
            .map_err(|e| contextdb_core::Error::SyncError(format!("subscribe pull: {e}")))?;
        client
            .flush()
            .await
            .map_err(|e| contextdb_core::Error::SyncError(format!("flush subscriptions: {e}")))?;
        Ok((push_sub, pull_sub))
    }

    async fn handle_push(
        &self,
        client: &async_nats::Client,
        msg: async_nats::Message,
    ) -> contextdb_core::Result<()> {
        let envelope =
            decode(&msg.payload).map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;

        match envelope.message_type {
            MessageType::Chunk => {
                let chunk_msg: crate::protocol::ChunkMessage =
                    rmp_serde::from_slice(&envelope.payload)
                        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
                let mut buf = self.chunk_buffer.lock().unwrap_or_else(|e| e.into_inner());
                if !buf.contains_key(&chunk_msg.chunk_id) && buf.len() >= MAX_CHUNK_SESSIONS {
                    tracing::warn!(
                        chunk_id = %chunk_msg.chunk_id,
                        active_sessions = buf.len(),
                        "chunk buffer full, rejecting new chunk session"
                    );
                    return Err(contextdb_core::Error::SyncError(
                        "chunk buffer full".to_string(),
                    ));
                }
                buf.entry(chunk_msg.chunk_id)
                    .or_insert_with(|| (std::time::Instant::now(), Vec::new()))
                    .1
                    .push(chunk_msg);
                Ok(())
            }
            MessageType::ChunkAck => {
                let ack: crate::protocol::ChunkAck = rmp_serde::from_slice(&envelope.payload)
                    .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;

                tracing::info!(
                    chunk_id = %ack.chunk_id,
                    total_chunks = ack.total_chunks,
                    "received ChunkAck, attempting reassembly"
                );

                let process_result: contextdb_core::Result<Vec<u8>> = (|| {
                    let mut chunks = {
                        let mut buf = self.chunk_buffer.lock().unwrap_or_else(|e| e.into_inner());
                        let (_ts, chunks) = buf.remove(&ack.chunk_id).ok_or_else(|| {
                            contextdb_core::Error::SyncError(format!(
                                "no chunks buffered for chunk_id {}",
                                ack.chunk_id
                            ))
                        })?;
                        chunks
                    };

                    if chunks.len() != ack.total_chunks as usize {
                        return Err(contextdb_core::Error::SyncError(format!(
                            "expected {} chunks for {}, got {}",
                            ack.total_chunks,
                            ack.chunk_id,
                            chunks.len()
                        )));
                    }

                    let assembled = crate::chunking::reassemble(&mut chunks);
                    let inner_envelope = decode(&assembled)
                        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
                    let request: PushRequest = rmp_serde::from_slice(&inner_envelope.payload)
                        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;

                    let changeset = match ChangeSet::try_from(request.changeset) {
                        Ok(changeset) => changeset,
                        Err(err) => {
                            let response = PushResponse {
                                result: None,
                                error: Some(err.to_string()),
                            };
                            return encode(MessageType::PushResponse, &response)
                                .map_err(|e| contextdb_core::Error::SyncError(e.to_string()));
                        }
                    };

                    match self.db.apply_changes(changeset, &self.policies) {
                        Ok(result) => {
                            let response = PushResponse {
                                result: Some(result.into()),
                                error: None,
                            };
                            encode(MessageType::PushResponse, &response)
                                .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))
                        }
                        Err(err) => {
                            let response = PushResponse {
                                result: None,
                                error: Some(err.to_string()),
                            };
                            encode(MessageType::PushResponse, &response)
                                .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))
                        }
                    }
                })();

                match process_result {
                    Ok(payload) => {
                        client
                            .publish(ack.reply_inbox, payload.into())
                            .await
                            .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
                        Ok(())
                    }
                    Err(e) => {
                        tracing::error!(
                            chunk_id = %ack.chunk_id,
                            error = %e,
                            "chunked push processing failed, client will timeout"
                        );
                        Err(e)
                    }
                }
            }
            MessageType::PushRequest => {
                let request: PushRequest = rmp_serde::from_slice(&envelope.payload)
                    .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
                maybe_wait_for_test_push_barrier(request.changeset.rows.len()).await;
                let response = match ChangeSet::try_from(request.changeset) {
                    Ok(changeset) => match self.db.apply_changes(changeset, &self.policies) {
                        Ok(result) => PushResponse {
                            result: Some(result.into()),
                            error: None,
                        },
                        Err(err) => PushResponse {
                            result: None,
                            error: Some(err.to_string()),
                        },
                    },
                    Err(err) => PushResponse {
                        result: None,
                        error: Some(err.to_string()),
                    },
                };
                let payload = encode(MessageType::PushResponse, &response)
                    .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;

                if let Some(reply) = msg.reply {
                    client
                        .publish(reply, payload.into())
                        .await
                        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
                    client
                        .flush()
                        .await
                        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
                }
                Ok(())
            }
            _ => Err(contextdb_core::Error::SyncError(
                "unexpected message type on push subject".to_string(),
            )),
        }
    }

    async fn handle_pull(
        &self,
        client: &async_nats::Client,
        msg: async_nats::Message,
    ) -> contextdb_core::Result<()> {
        let envelope =
            decode(&msg.payload).map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
        if !matches!(envelope.message_type, MessageType::PullRequest) {
            return Err(contextdb_core::Error::SyncError(
                "unexpected message type on pull subject".to_string(),
            ));
        }

        let request: PullRequest = rmp_serde::from_slice(&envelope.payload)
            .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
        let mut changes = self.db.changes_since(request.since_lsn);

        let mut has_more = false;
        let mut cursor = None;
        if let Some(max_entries) = request.max_entries {
            let max = max_entries as usize;
            let groups = changes
                .clone()
                .split_by_data_lsn()
                .into_iter()
                .filter(|group| group.data_entry_count() > 0 || !group.ddl.is_empty())
                .collect::<Vec<_>>();

            if !groups.is_empty()
                && groups
                    .iter()
                    .map(ChangeSet::data_entry_count)
                    .sum::<usize>()
                    > max
            {
                let mut selected = Vec::new();
                let mut selected_entries = 0usize;
                for group in &groups {
                    let group_entries = group.data_entry_count().max(group.ddl.len()).max(1);
                    if !selected.is_empty() && selected_entries + group_entries > max {
                        break;
                    }
                    selected_entries = selected_entries.saturating_add(group_entries);
                    selected.push(group.clone());
                    if selected_entries >= max {
                        break;
                    }
                }
                has_more = selected.len() < groups.len();
                changes = merge_changeset_groups(selected);
            }
        }
        let mut bootstrap_batches = changes.clone().split_at_trigger_bootstrap_barriers();
        if bootstrap_batches.len() > 1 {
            changes = bootstrap_batches.remove(0);
            has_more = true;
        }
        if request.max_entries.is_some() || has_more {
            cursor = changes.max_lsn().or_else(|| {
                if changes.ddl.is_empty() {
                    None
                } else {
                    Some(self.db.current_lsn())
                }
            });
        }

        let response = PullResponse {
            changeset: changes.into(),
            has_more,
            cursor,
        };
        let payload = encode(MessageType::PullResponse, &response)
            .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;

        if let Some(reply) = msg.reply {
            if crate::chunking::needs_chunking(&payload) {
                let chunks = crate::chunking::chunk(&payload);
                tracing::info!(
                    total_chunks = chunks.len(),
                    payload_size = payload.len(),
                    "sending chunked pull response"
                );
                for chunk_msg in &chunks {
                    let chunk_encoded = encode(MessageType::Chunk, chunk_msg)
                        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
                    client
                        .publish(reply.clone(), chunk_encoded.into())
                        .await
                        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
                }
                client
                    .flush()
                    .await
                    .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
            } else {
                client
                    .publish(reply, payload.into())
                    .await
                    .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
            }
        }
        Ok(())
    }
}

fn merge_changeset_groups(groups: Vec<ChangeSet>) -> ChangeSet {
    let mut merged = ChangeSet::default();
    for group in groups {
        merged.rows.extend(group.rows);
        merged.edges.extend(group.edges);
        merged.vectors.extend(group.vectors);
        merged.ddl.extend(group.ddl);
        merged.ddl_lsn.extend(group.ddl_lsn);
    }
    merged
}
