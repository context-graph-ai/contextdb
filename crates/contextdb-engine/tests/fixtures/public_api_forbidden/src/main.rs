use std::sync::Arc;

#[cfg(any(
    feature = "accountant_constructor",
    feature = "raw_accountant_mutation",
    feature = "raw_accountant_allocate",
    feature = "raw_accountant_allocate_for",
    feature = "raw_accountant_release",
    feature = "plugin_accountant_constructor",
    feature = "file_accountant_constructor",
    feature = "file_disk_accountant_constructor"
))]
use contextdb_core::MemoryAccountant;
#[cfg(any(
    feature = "sync_watermark_setter",
    feature = "sync_incarnation_writer",
    feature = "push_watermark_writer",
    feature = "pending_confirmation_writer",
    feature = "pull_watermark_writer",
    feature = "pull_cursor_writer",
    feature = "applied_push_writer",
    feature = "callerless_applied_push_writer",
    feature = "outbound_pending_writer",
    feature = "record_hub_acceptance_writer",
    feature = "invalidate_hub_regression_writer",
    feature = "change_retention_peer_writer",
    feature = "client_pull_policy_map",
    feature = "client_initial_sync_policy_map",
    feature = "client_direction_setter",
    feature = "client_policy_setter",
    feature = "client_default_policy_setter",
    feature = "client_arbitrary_transport",
    feature = "server_arbitrary_transport",
    feature = "server_policy_constructor"
))]
use contextdb_core::{Incarnation, Lsn, TenantId};
use contextdb_engine::Database;
#[cfg(any(
    feature = "plugin_accountant_constructor",
    feature = "file_accountant_constructor",
    feature = "file_disk_accountant_constructor"
))]
use contextdb_engine::plugin::CorePlugin;
#[cfg(feature = "role_relative_policy_types")]
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
#[cfg(any(
    feature = "client_pull_policy_map",
    feature = "client_initial_sync_policy_map",
    feature = "client_direction_setter",
    feature = "client_policy_setter",
    feature = "client_default_policy_setter",
    feature = "client_arbitrary_transport"
))]
use contextdb_server::SyncClient;
#[cfg(any(
    feature = "server_arbitrary_transport",
    feature = "server_policy_constructor"
))]
use contextdb_server::SyncServer;

fn db() -> Arc<Database> {
    Arc::new(Database::open_memory())
}
#[cfg(any(
    feature = "client_pull_policy_map",
    feature = "client_initial_sync_policy_map",
    feature = "client_direction_setter",
    feature = "client_policy_setter",
    feature = "client_default_policy_setter",
    feature = "client_arbitrary_transport"
))]
fn client() -> SyncClient {
    SyncClient::new(db(), "iroh:ticket", TenantId::from("tenant"))
}

#[cfg(feature = "accountant_accessor")]
fn main() {
    let _ = db().accountant().set_budget(Some(1024));
}
#[cfg(feature = "raw_accountant_mutation")]
fn main() {
    let _ = MemoryAccountant::no_limit().set_budget(Some(1024));
}
#[cfg(feature = "raw_accountant_allocate")]
fn main() {
    let _ = MemoryAccountant::no_limit().try_allocate(1);
}
#[cfg(feature = "raw_accountant_allocate_for")]
fn main() {
    let _ = MemoryAccountant::no_limit().try_allocate_for(1, "fixture", "allocate", "hint");
}
#[cfg(feature = "raw_accountant_release")]
fn main() {
    MemoryAccountant::no_limit().release(1);
}
#[cfg(feature = "accountant_constructor")]
fn main() {
    let _ = Database::open_memory_with_accountant(Arc::new(MemoryAccountant::no_limit()));
}
#[cfg(feature = "plugin_accountant_constructor")]
fn main() {
    let _ = Database::open_memory_with_plugin_and_accountant(
        Arc::new(CorePlugin),
        Arc::new(MemoryAccountant::no_limit()),
    );
}
#[cfg(feature = "file_accountant_constructor")]
fn main() {
    let _ = Database::open_with_config(
        ":memory:",
        Arc::new(CorePlugin),
        Arc::new(MemoryAccountant::no_limit()),
    );
}
#[cfg(feature = "file_disk_accountant_constructor")]
fn main() {
    let _ = Database::open_with_config_and_disk_limit(
        ":memory:",
        Arc::new(CorePlugin),
        Arc::new(MemoryAccountant::no_limit()),
        Some(1024),
    );
}
#[cfg(feature = "sync_watermark_setter")]
fn main() {
    db().set_sync_watermark(Lsn(7));
}
#[cfg(feature = "sync_incarnation_writer")]
fn main() {
    let _ = db().sync_incarnation(&TenantId::from("tenant"));
}
#[cfg(feature = "push_watermark_writer")]
fn main() {
    let _ = db().persist_sync_push_watermark(&TenantId::from("tenant"), Lsn(7));
}
#[cfg(feature = "pending_confirmation_writer")]
fn main() {
    let _ = db().persist_sync_pending_push_confirmation(&TenantId::from("tenant"), Some(Lsn(7)));
}
#[cfg(feature = "pull_watermark_writer")]
fn main() {
    let _ = db().persist_sync_pull_watermark(&TenantId::from("tenant"), Lsn(7));
}
#[cfg(feature = "pull_cursor_writer")]
fn main() {
    let _ = db().persist_sync_pull_cursor(&TenantId::from("tenant"), Incarnation::mint(), Lsn(7));
}
#[cfg(feature = "applied_push_writer")]
fn main() {
    let _ = db().persist_sync_applied_push_watermark(&TenantId::from("tenant"), Lsn(7));
}
#[cfg(feature = "callerless_applied_push_writer")]
fn main() {
    let _ = db().persist_sync_applied_push_watermark_for_node(
        &TenantId::from("tenant"),
        "node",
        Incarnation::mint(),
        Lsn(7),
    );
}
#[cfg(feature = "outbound_pending_writer")]
fn main() {
    let _ = db().mark_outbound_rows_pending(Lsn(0), None);
}
#[cfg(feature = "confirmed_pending_writer")]
fn main() {
    let _ = db().refresh_confirmed_pending_rows(&[], &std::collections::HashMap::new());
}
#[cfg(feature = "record_hub_acceptance_writer")]
fn main() {
    let _ = db().record_hub_accepted_rows(&[], Lsn(7));
}
#[cfg(feature = "invalidate_hub_regression_writer")]
fn main() {
    let _ = db().invalidate_accepted_local_ordering_after_hub_regression(Lsn(7));
}
#[cfg(feature = "register_retention_peer_writer")]
fn main() {
    let _ = db().register_retention_sync_peer("hub");
}
#[cfg(feature = "change_retention_peer_writer")]
fn main() {
    let _ = db().change_retention_sync_peer(&TenantId::from("tenant"), "hub");
}
#[cfg(feature = "sync_relay_mode_writer")]
fn main() {
    db().enable_sync_relay_mode();
}
#[cfg(feature = "database_policy_accessor")]
fn main() {
    let _ = db().conflict_policies();
}
#[cfg(feature = "client_pull_policy_map")]
fn main() {
    let _ = client().pull(panic!("infer removed policy argument"));
}
#[cfg(feature = "client_initial_sync_policy_map")]
fn main() {
    let _ = client().initial_sync(panic!("infer removed policy argument"));
}
#[cfg(feature = "client_direction_setter")]
fn main() {
    let _ = client().set_table_direction("notes", panic!("infer removed direction argument"));
}
#[cfg(feature = "client_policy_setter")]
fn main() {
    client().set_conflict_policy("notes", panic!("infer removed policy argument"));
}
#[cfg(feature = "client_default_policy_setter")]
fn main() {
    client().set_default_conflict_policy(panic!("infer removed policy argument"));
}
#[cfg(feature = "client_arbitrary_transport")]
fn main() {
    let _ = SyncClient::with_transport(
        db(),
        panic!("infer removed transport argument"),
        TenantId::from("tenant"),
    );
}
#[cfg(feature = "server_arbitrary_transport")]
fn main() {
    let _ = SyncServer::with_transport(
        db(),
        panic!("infer removed transport argument"),
        TenantId::from("tenant"),
        panic!("infer removed policy argument"),
    );
}
#[cfg(feature = "server_policy_constructor")]
fn main() {
    let _ = SyncServer::new(
        db(),
        "iroh:?identity=/tmp/fixture.key",
        TenantId::from("tenant"),
        panic!("infer removed policy argument"),
    );
}
#[cfg(feature = "raw_policy_apply")]
fn main() {
    let _ = db().apply_synced_changes(
        panic!("infer removed changes"),
        panic!("infer removed policies"),
        panic!("infer removed arrivals"),
        panic!("infer removed adoption"),
    );
}
#[cfg(feature = "raw_apply_changes")]
fn main() {
    let _ = db().apply_changes(
        panic!("infer removed changes"),
        panic!("infer removed policies"),
    );
}
#[cfg(feature = "raw_apply_changes_with_receipt")]
fn main() {
    let _ = db().apply_synced_changes_with_receipt(
        panic!("infer removed changes"),
        panic!("infer removed policies"),
        panic!("infer removed arrivals"),
        panic!("infer removed adoption"),
        panic!("infer removed receipt"),
    );
}
#[cfg(feature = "role_relative_policy_types")]
fn main() {
    let _ = ConflictPolicy::LatestWins;
    let _ = ConflictPolicies::uniform(ConflictPolicy::LatestWins);
}
#[cfg(feature = "legacy_source_mutation")]
fn main() {
    let source = Database::open_legacy_for_migration("legacy.db").expect("legacy source");
    let _ = source.execute(
        "CREATE TABLE forged (id UUID PRIMARY KEY)",
        &Default::default(),
    );
}
#[cfg(feature = "raw_blob_repository_type")]
fn main() {
    let _: contextdb_engine::blob_repository::BlobRepository = panic!("type must stay private");
}
#[cfg(feature = "raw_blob_repository_accessor")]
fn main() {
    let _ = db().blob_repository();
}

#[cfg(not(any(
    feature = "accountant_accessor",
    feature = "raw_accountant_mutation",
    feature = "raw_accountant_allocate",
    feature = "raw_accountant_allocate_for",
    feature = "raw_accountant_release",
    feature = "accountant_constructor",
    feature = "plugin_accountant_constructor",
    feature = "file_accountant_constructor",
    feature = "file_disk_accountant_constructor",
    feature = "sync_watermark_setter",
    feature = "sync_incarnation_writer",
    feature = "push_watermark_writer",
    feature = "pending_confirmation_writer",
    feature = "pull_watermark_writer",
    feature = "pull_cursor_writer",
    feature = "applied_push_writer",
    feature = "callerless_applied_push_writer",
    feature = "outbound_pending_writer",
    feature = "confirmed_pending_writer",
    feature = "record_hub_acceptance_writer",
    feature = "invalidate_hub_regression_writer",
    feature = "register_retention_peer_writer",
    feature = "change_retention_peer_writer",
    feature = "sync_relay_mode_writer",
    feature = "database_policy_accessor",
    feature = "client_pull_policy_map",
    feature = "client_initial_sync_policy_map",
    feature = "client_direction_setter",
    feature = "client_policy_setter",
    feature = "client_default_policy_setter",
    feature = "client_arbitrary_transport",
    feature = "server_arbitrary_transport",
    feature = "server_policy_constructor",
    feature = "raw_policy_apply",
    feature = "raw_apply_changes",
    feature = "raw_apply_changes_with_receipt",
    feature = "role_relative_policy_types",
    feature = "legacy_source_mutation",
    feature = "raw_blob_repository_type",
    feature = "raw_blob_repository_accessor"
)))]
fn main() {}
