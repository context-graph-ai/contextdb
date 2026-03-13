use contextdb_engine::Database;
use contextdb_engine::sync_types::ConflictPolicies;
use std::sync::Arc;

pub struct SyncServer {
    db: Arc<Database>,
    #[allow(dead_code)]
    nats_url: String,
    #[allow(dead_code)]
    tenant_id: String,
    #[allow(dead_code)]
    policies: ConflictPolicies,
}

impl SyncServer {
    pub fn new(
        db: Arc<Database>,
        nats_url: &str,
        tenant_id: &str,
        policies: ConflictPolicies,
    ) -> Self {
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
        unimplemented!("SyncServer::run — requires NATS implementation")
    }
}
