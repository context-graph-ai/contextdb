use crate::protocol::{
    MessageType, PullRequest, PullResponse, PushRequest, PushResponse, decode, encode,
};
use crate::subjects::{pull_subject, push_subject};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ApplyResult, ChangeSet, ConflictPolicies};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Clone)]
struct LocalServer {
    db: Arc<Database>,
    policies: ConflictPolicies,
}

fn local_registry() -> &'static Mutex<HashMap<(String, String), LocalServer>> {
    static REGISTRY: OnceLock<Mutex<HashMap<(String, String), LocalServer>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn runtime_scope_key() -> String {
    tokio::runtime::Handle::try_current()
        .map(|h| format!("{:?}", h.id()))
        .unwrap_or_else(|_| "no-runtime".to_string())
}

pub(crate) fn local_push(tenant: &str, changes: ChangeSet) -> contextdb_core::Result<ApplyResult> {
    let scope = runtime_scope_key();
    let state = local_registry()
        .lock()
        .expect("local registry poisoned")
        .get(&(tenant.to_string(), scope))
        .cloned();
    let state = state
        .ok_or_else(|| contextdb_core::Error::SyncError("tenant not registered".to_string()))?;
    state.db.apply_changes(changes, &state.policies)
}

pub(crate) fn local_pull(tenant: &str, since_lsn: u64) -> contextdb_core::Result<ChangeSet> {
    let scope = runtime_scope_key();
    let state = local_registry()
        .lock()
        .expect("local registry poisoned")
        .get(&(tenant.to_string(), scope))
        .cloned();
    let state = state
        .ok_or_else(|| contextdb_core::Error::SyncError("tenant not registered".to_string()))?;
    Ok(state.db.changes_since(since_lsn))
}

pub struct SyncServer {
    db: Arc<Database>,
    nats_url: String,
    tenant_id: String,
    policies: ConflictPolicies,
}

impl SyncServer {
    pub fn new(
        db: Arc<Database>,
        nats_url: &str,
        tenant_id: &str,
        policies: ConflictPolicies,
    ) -> Self {
        local_registry()
            .lock()
            .expect("local registry poisoned")
            .insert(
                (tenant_id.to_string(), runtime_scope_key()),
                LocalServer {
                    db: db.clone(),
                    policies: policies.clone(),
                },
            );
        Self {
            db,
            nats_url: nats_url.to_string(),
            tenant_id: tenant_id.to_string(),
            policies,
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

        loop {
            tokio::select! {
                maybe_msg = push_sub.next() => {
                    if let Some(msg) = maybe_msg {
                        let _ = self.handle_push(&client, msg).await;
                    }
                }
                maybe_msg = pull_sub.next() => {
                    if let Some(msg) = maybe_msg {
                        let _ = self.handle_pull(&client, msg).await;
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
        if !matches!(envelope.message_type, MessageType::PushRequest) {
            return Err(contextdb_core::Error::SyncError(
                "unexpected message type on push subject".to_string(),
            ));
        }

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
                let remainder = changes.rows.split_off(max);
                cursor = remainder.last().map(|r| r.lsn);
                has_more = true;
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
            client
                .publish(reply, payload.into())
                .await
                .map_err(|e| contextdb_core::Error::SyncError(e.to_string()))?;
        }
        Ok(())
    }
}
