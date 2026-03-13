use contextdb_core::Error;
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ApplyResult, ConflictPolicies};
use std::sync::Arc;

pub struct SyncClient {
    db: Arc<Database>,
    #[allow(dead_code)]
    nats_url: String,
    #[allow(dead_code)]
    tenant_id: String,
}

impl SyncClient {
    pub fn new(db: Arc<Database>, nats_url: &str, tenant_id: &str) -> Self {
        Self {
            db,
            nats_url: nats_url.to_string(),
            tenant_id: tenant_id.to_string(),
        }
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    pub async fn push(&self) -> Result<ApplyResult, Error> {
        unimplemented!("SyncClient::push — requires NATS implementation")
    }

    pub async fn pull(&self, _policies: &ConflictPolicies) -> Result<ApplyResult, Error> {
        unimplemented!("SyncClient::pull — requires NATS implementation")
    }

    pub async fn initial_sync(&self, _policies: &ConflictPolicies) -> Result<ApplyResult, Error> {
        unimplemented!("SyncClient::initial_sync — requires NATS implementation")
    }
}
