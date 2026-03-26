use crate::protocol::{
    MessageType, PullRequest, PullResponse, PushRequest, PushResponse, decode, encode,
};
use crate::subjects::{pull_subject, push_subject};
use contextdb_engine::Database;
use contextdb_engine::sync_types::ConflictPolicies;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Max concurrent in-flight chunked push sessions. Rejects new chunk_ids past this limit.
const MAX_CHUNK_SESSIONS: usize = 64;

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
        let client = loop {
            match async_nats::connect(&self.nats_url).await {
                Ok(client) => break client,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(200)).await,
            }
        };

        let mut push_sub = client
            .subscribe(push_subject(&self.tenant_id))
            .await
            .expect("subscribe push");
        let mut pull_sub = client
            .subscribe(pull_subject(&self.tenant_id))
            .await
            .expect("subscribe pull");
        client.flush().await.expect("flush subscriptions");
        let mut cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(10));

        loop {
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
            }
        }
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

                    let result = self
                        .db
                        .apply_changes(request.changeset.into(), &self.policies)?;
                    let response = PushResponse {
                        result: result.into(),
                    };
                    encode(MessageType::PushResponse, &response)
                        .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))
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
                let result = self
                    .db
                    .apply_changes(request.changeset.into(), &self.policies)?;
                let response = PushResponse {
                    result: result.into(),
                };
                let payload = encode(MessageType::PushResponse, &response)
                    .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;

                if let Some(reply) = msg.reply {
                    client
                        .publish(reply, payload.into())
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
            if changes.rows.len() > max {
                let mut remainder = changes.rows.split_off(max);
                // Don't split in the middle of a same-LSN group: extend returned set
                // to include all rows sharing the LSN at the split boundary.
                if let (Some(last_returned), Some(first_remainder)) =
                    (changes.rows.last(), remainder.first())
                    && last_returned.lsn == first_remainder.lsn
                {
                    let boundary_lsn = last_returned.lsn;
                    let split_idx = remainder.partition_point(|r| r.lsn == boundary_lsn);
                    let moved: Vec<_> = remainder.drain(..split_idx).collect();
                    changes.rows.extend(moved);
                }
                // Cursor = last LSN of returned set (not remainder)
                cursor = changes.rows.last().map(|r| r.lsn);
                has_more = !remainder.is_empty();
            }
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
