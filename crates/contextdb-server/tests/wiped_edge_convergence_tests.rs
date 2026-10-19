//! A wiped edge restores from the hub: whatever the hub's maintenance, erasure
//! or schema history has removed, an edge that starts empty and pulls ends with
//! exactly the hub's rows and the hub's final declarations, also after the hub
//! restarts, and a second pull from it changes nothing.
//!
//! Each test names one way the hub's history is shortened or rewritten, and
//! compares the restored edge with the hub itself rather than with a literal.

use contextdb_core::{Value, Wallclock};
use contextdb_engine::Database;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(120), future)
        .await
        .expect("bounded in-process exchange")
}

fn key(id: i64) -> Value {
    Value::Uuid(uuid::Uuid::from_u128(id as u128 + 1))
}

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

/// The hub served in-process for one restore.
struct Served {
    shutdown: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<()>,
    fabric: InProcessBroker,
    tenant: String,
}

async fn serve(hub: Arc<Database>, tenant: &str, identity: Arc<FabricIdentity>) -> Served {
    let fabric = InProcessBroker::new();
    let node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            hub,
            fabric.server_as(&node_id),
            contextdb_core::TenantId::from(tenant),
            node_id,
            identity,
        )
        .expect("construct the hub server"),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = shutdown.clone();
    let handle = tokio::spawn(async move { server.run_until(flag).await });
    within(
        fabric.wait_for_registered_route_for_test(&contextdb_server::subjects::status_subject(
            tenant,
        )),
    )
    .await;
    Served {
        shutdown,
        handle,
        fabric,
        tenant: tenant.to_string(),
    }
}

impl Served {
    /// A brand new edge with no tables pulls everything the hub serves.
    async fn restore_wiped_edge(
        &self,
        why: &str,
    ) -> (Arc<Database>, SyncClient, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("edge directory");
        let edge =
            Arc::new(Database::open(dir.path().join("edge.db")).expect("open the wiped edge"));
        let identity = Arc::new(FabricIdentity::generate());
        let node_id = identity.node_id();
        let client = SyncClient::with_authenticated_transport_and_identity_for_test(
            edge.clone(),
            self.fabric.client_as(&node_id),
            contextdb_core::TenantId::from(self.tenant.as_str()),
            identity,
        )
        .expect("construct the wiped edge's client");
        within(client.pull_default())
            .await
            .unwrap_or_else(|error| panic!("{why}: a wiped edge restores from the hub: {error}"));
        (edge, client, dir)
    }

    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        within(self.handle).await.expect("the hub stops");
    }
}

/// The columns and every row of `table`, ordered, as the store reports them.
fn contents(db: &Database, table: &str) -> (Vec<String>, BTreeSet<String>) {
    let result = db
        .execute(&format!("SELECT * FROM {table}"), &p())
        .unwrap_or_else(|error| panic!("{table} must be readable: {error}"));
    let rows = result
        .rows
        .iter()
        .map(|row| format!("{row:?}"))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        rows.len(),
        result.rows.len(),
        "{table}: a row appears twice"
    );
    (result.columns.clone(), rows)
}

fn declared_columns(db: &Database, table: &str) -> Vec<(String, String)> {
    db.table_meta(table)
        .unwrap_or_else(|| panic!("{table} must be declared"))
        .columns
        .iter()
        .map(|column| (column.name.clone(), format!("{:?}", column.column_type)))
        .collect()
}

fn put(db: &Database, table: &str, id: i64, body: &str) {
    db.execute(
        &format!("INSERT INTO {table} (id, body) VALUES ($id, $body)"),
        &HashMap::from([
            ("id".to_string(), key(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert");
}

fn rewrite(db: &Database, table: &str, id: i64, body: &str) {
    db.execute(
        &format!("UPDATE {table} SET body = $body WHERE id = $id"),
        &HashMap::from([
            ("id".to_string(), key(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("update");
}

/// Restore a wiped edge and require it to equal the hub in `tables`, then pull
/// again and require that nothing moves.
async fn assert_restores(
    hub: Arc<Database>,
    identity: &Arc<FabricIdentity>,
    tenant: &str,
    tables: &[&str],
    why: &str,
) {
    let served = serve(hub.clone(), tenant, identity.clone()).await;
    let (edge, client, _edge_dir) = served.restore_wiped_edge(why).await;
    for table in tables {
        assert_eq!(
            contents(&edge, table),
            contents(&hub, table),
            "{why}: {table} restored from a wiped edge equals the hub"
        );
        assert_eq!(
            declared_columns(&edge, table),
            declared_columns(&hub, table),
            "{why}: {table} carries the hub's final declaration"
        );
    }
    let watermark = client.pull_watermark();
    let again = within(client.pull_default()).await.expect("second pull");
    assert_eq!(
        (again.applied_rows, again.skipped_rows),
        (0, 0),
        "{why}: a second pull from the restored edge moves nothing"
    );
    assert_eq!(
        client.pull_watermark(),
        watermark,
        "{why}: cursor is stable"
    );
    served.stop().await;
}

/// The hub is closed and opened again from its file before the restore.
fn reopen(path: &Path, hub: Database) -> Arc<Database> {
    hub.close().expect("close the hub");
    Arc::new(Database::open(path).expect("reopen the hub"))
}

// Superseded versions of a current-only table are reclaimed with their log
// entries. The wiped edge must still resolve every row to its latest value.
async fn run_a_wiped_edge_converges_after_current_only_compaction(restart: bool) {
    let dir = tempfile::tempdir().expect("hub directory");
    let path = dir.path().join("hub.db");
    let identity = Arc::new(FabricIdentity::generate());
    let hub = Database::open(&path).expect("open hub");
    hub.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) \
         HISTORY CURRENT ONLY SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("declare notes");
    for id in 0..30 {
        put(&hub, "notes", id, &format!("first-{id}"));
    }
    for round in 0..2 {
        for id in 0..30 {
            rewrite(&hub, "notes", id, &format!("round-{round}-{id}"));
        }
    }
    let report = hub.compact_currency_versions().expect("compact");
    assert!(
        report.pruned_change_log_entries > 0,
        "the hub shortened its log: {report:?}"
    );
    let hub = Arc::new(hub);
    assert_restores(
        hub.clone(),
        &identity,
        "wiped-compaction",
        &["notes"],
        "after compaction",
    )
    .await;
    if !restart {
        return;
    }
    let hub = reopen(
        &path,
        Arc::try_unwrap(hub).unwrap_or_else(|_| panic!("the server released the hub")),
    );
    assert_restores(
        hub,
        &identity,
        "wiped-compaction",
        &["notes"],
        "after reopen",
    )
    .await;
}

// Rows older than the declared retention window are pruned with their log
// entries. The wiped edge ends with the rows the hub still holds, no more.
async fn run_a_wiped_edge_converges_after_retention_pruning(restart: bool) {
    let cell = Arc::new(AtomicU64::new(1_800_000_000_000));
    let clock = {
        let cell = cell.clone();
        Wallclock::test_clock_guard(move || cell.load(Ordering::SeqCst))
    };
    let dir = tempfile::tempdir().expect("hub directory");
    let path = dir.path().join("hub.db");
    let identity = Arc::new(FabricIdentity::generate());
    let hub = Database::open(&path).expect("open hub");
    hub.execute(
        "CREATE TABLE windows (id UUID PRIMARY KEY, body TEXT) \
         RETAIN 1 HOURS SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("declare windows");
    for id in 0..20 {
        put(&hub, "windows", id, &format!("old-{id}"));
    }
    cell.fetch_add(2 * 3_600_000, Ordering::SeqCst);
    for id in 20..30 {
        put(&hub, "windows", id, &format!("new-{id}"));
    }
    let report = hub.run_pruning_cycle_checked().expect("prune");
    assert_eq!(report.pruned_rows, 20, "the old rows expired: {report:?}");
    let hub = Arc::new(hub);
    assert_restores(
        hub.clone(),
        &identity,
        "wiped-retention",
        &["windows"],
        "after retention",
    )
    .await;
    if !restart {
        return;
    }
    let hub = reopen(
        &path,
        Arc::try_unwrap(hub).unwrap_or_else(|_| panic!("the server released the hub")),
    );
    assert_restores(
        hub,
        &identity,
        "wiped-retention",
        &["windows"],
        "after reopen",
    )
    .await;
    drop(clock);
}

// Rows the hub deleted are not restored, rows it kept are, whatever the
// deletion left behind in its log and lineage.
async fn run_a_wiped_edge_converges_after_the_hub_deletes_rows(restart: bool) {
    let dir = tempfile::tempdir().expect("hub directory");
    let path = dir.path().join("hub.db");
    let identity = Arc::new(FabricIdentity::generate());
    let hub = Database::open(&path).expect("open hub");
    hub.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("declare notes");
    for id in 0..24 {
        put(&hub, "notes", id, &format!("n-{id}"));
    }
    for id in (0..24).step_by(3) {
        hub.execute(
            "DELETE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), key(id))]),
        )
        .expect("delete");
    }
    let hub = Arc::new(hub);
    assert_restores(
        hub.clone(),
        &identity,
        "wiped-delete",
        &["notes"],
        "after deletes",
    )
    .await;
    if !restart {
        return;
    }
    let hub = reopen(
        &path,
        Arc::try_unwrap(hub).unwrap_or_else(|_| panic!("the server released the hub")),
    );
    assert_restores(hub, &identity, "wiped-delete", &["notes"], "after reopen").await;
}

// Authoritative erasure removes every copy, including the log entries that
// named the row. The wiped edge never receives the erased row.
async fn run_a_wiped_edge_converges_after_authoritative_erasure(restart: bool) {
    let dir = tempfile::tempdir().expect("hub directory");
    let path = dir.path().join("hub.db");
    let identity = Arc::new(FabricIdentity::generate());
    let hub = Database::open(&path).expect("open hub");
    hub.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("declare notes");
    for id in 0..20 {
        put(&hub, "notes", id, &format!("n-{id}"));
        rewrite(&hub, "notes", id, &format!("n-{id}-again"));
    }
    for id in [3, 4, 11] {
        hub.execute(
            "PURGE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), key(id))]),
        )
        .expect("authoritative erasure");
    }
    let hub = Arc::new(hub);
    assert_restores(
        hub.clone(),
        &identity,
        "wiped-erasure",
        &["notes"],
        "after erasure",
    )
    .await;
    if !restart {
        return;
    }
    let hub = reopen(
        &path,
        Arc::try_unwrap(hub).unwrap_or_else(|_| panic!("the server released the hub")),
    );
    assert_restores(hub, &identity, "wiped-erasure", &["notes"], "after reopen").await;
}

// A table that held rows before it was declared for sync still restores them:
// rows written before the switch are part of what the hub serves.
async fn run_a_wiped_edge_receives_rows_written_before_the_table_was_synced(restart: bool) {
    let dir = tempfile::tempdir().expect("hub directory");
    let path = dir.path().join("hub.db");
    let identity = Arc::new(FabricIdentity::generate());
    let hub = Database::open(&path).expect("open hub");
    hub.execute("CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)", &p())
        .expect("declare an unsynced table");
    for id in 0..12 {
        put(&hub, "notes", id, &format!("before-{id}"));
    }
    hub.execute("ALTER TABLE notes SET SYNC TWO WAY", &p())
        .expect("switch the table to sync");
    for id in 12..18 {
        put(&hub, "notes", id, &format!("after-{id}"));
    }
    let hub = Arc::new(hub);
    assert_restores(
        hub.clone(),
        &identity,
        "wiped-late-sync",
        &["notes"],
        "after the switch",
    )
    .await;
    if !restart {
        return;
    }
    let hub = reopen(
        &path,
        Arc::try_unwrap(hub).unwrap_or_else(|_| panic!("the server released the hub")),
    );
    assert_restores(
        hub,
        &identity,
        "wiped-late-sync",
        &["notes"],
        "after reopen",
    )
    .await;
}

// Declarations change between rows. The wiped edge ends with the hub's final
// declaration and every row the hub holds, values intact: a column added,
// renamed and dropped between rows does not lose or invent a value.
async fn run_a_wiped_edge_converges_across_alter_rename_and_drop(restart: bool) {
    let dir = tempfile::tempdir().expect("hub directory");
    let path = dir.path().join("hub.db");
    let identity = Arc::new(FabricIdentity::generate());
    let hub = Database::open(&path).expect("open hub");
    hub.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("declare notes");
    for id in 0..5 {
        put(&hub, "notes", id, &format!("v1-{id}"));
    }
    hub.execute("ALTER TABLE notes ADD COLUMN extra TEXT", &p())
        .expect("add a column");
    for id in 5..10 {
        hub.execute(
            "INSERT INTO notes (id, body, extra) VALUES ($id, $body, $extra)",
            &HashMap::from([
                ("id".to_string(), key(id)),
                ("body".to_string(), Value::Text(format!("v2-{id}"))),
                ("extra".to_string(), Value::Text(format!("extra-{id}"))),
            ]),
        )
        .expect("insert under the added column");
    }
    hub.execute("ALTER TABLE notes RENAME COLUMN extra TO detail", &p())
        .expect("rename the column");
    for id in 10..15 {
        hub.execute(
            "INSERT INTO notes (id, body, detail) VALUES ($id, $body, $detail)",
            &HashMap::from([
                ("id".to_string(), key(id)),
                ("body".to_string(), Value::Text(format!("v3-{id}"))),
                ("detail".to_string(), Value::Text(format!("detail-{id}"))),
            ]),
        )
        .expect("insert under the renamed column");
    }
    hub.execute("ALTER TABLE notes DROP COLUMN body", &p())
        .expect("drop a column");
    for id in 15..20 {
        hub.execute(
            "INSERT INTO notes (id, detail) VALUES ($id, $detail)",
            &HashMap::from([
                ("id".to_string(), key(id)),
                ("detail".to_string(), Value::Text(format!("detail-{id}"))),
            ]),
        )
        .expect("insert after the drop");
    }
    let hub = Arc::new(hub);
    assert_restores(
        hub.clone(),
        &identity,
        "wiped-alter",
        &["notes"],
        "across alter",
    )
    .await;
    if !restart {
        return;
    }
    let hub = reopen(
        &path,
        Arc::try_unwrap(hub).unwrap_or_else(|_| panic!("the server released the hub")),
    );
    assert_restores(hub, &identity, "wiped-alter", &["notes"], "after reopen").await;
}

// A table dropped and created again is a new generation. The wiped edge
// receives the new generation's rows and none of the old generation's.
async fn run_a_wiped_edge_converges_across_drop_and_recreate(restart: bool) {
    let dir = tempfile::tempdir().expect("hub directory");
    let path = dir.path().join("hub.db");
    let identity = Arc::new(FabricIdentity::generate());
    let hub = Database::open(&path).expect("open hub");
    hub.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("declare notes");
    for id in 0..10 {
        put(&hub, "notes", id, &format!("old-{id}"));
    }
    hub.execute("DROP TABLE notes", &p()).expect("drop notes");
    hub.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT, tag TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("recreate notes with another shape");
    for id in 5..12 {
        put(&hub, "notes", id, &format!("new-{id}"));
    }
    let hub = Arc::new(hub);
    assert_restores(
        hub.clone(),
        &identity,
        "wiped-recreate",
        &["notes"],
        "after recreate",
    )
    .await;
    if !restart {
        return;
    }
    let hub = reopen(
        &path,
        Arc::try_unwrap(hub).unwrap_or_else(|_| panic!("the server released the hub")),
    );
    assert_restores(hub, &identity, "wiped-recreate", &["notes"], "after reopen").await;
}

// A child written before its parent's table was altered, whose parent row
// exists only in the later declaration, restores together: the parent is
// delivered with the child it satisfies and both match the hub.
async fn run_a_wiped_edge_converges_when_a_parent_survives_only_after_its_child(restart: bool) {
    let dir = tempfile::tempdir().expect("hub directory");
    let path = dir.path().join("hub.db");
    let identity = Arc::new(FabricIdentity::generate());
    let hub = Database::open(&path).expect("open hub");
    hub.execute(
        "CREATE TABLE parents (id UUID PRIMARY KEY, body TEXT) HISTORY CURRENT ONLY SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("declare parents");
    hub.execute(
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id), body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("declare children");
    put(&hub, "parents", 1, "parent v1");
    hub.execute(
        "INSERT INTO children (id, parent_id, body) VALUES ($id, $parent, 'child')",
        &HashMap::from([("id".to_string(), key(1)), ("parent".to_string(), key(1))]),
    )
    .expect("insert the child");
    hub.execute("ALTER TABLE parents ADD COLUMN extra TEXT", &p())
        .expect("alter the parent after the child");
    rewrite(&hub, "parents", 1, "parent v2");
    hub.compact_currency_versions().expect("compact the parent");
    let hub = Arc::new(hub);
    assert_restores(
        hub.clone(),
        &identity,
        "wiped-parent-after-child",
        &["parents", "children"],
        "parent survives only after its child",
    )
    .await;
    if !restart {
        return;
    }
    let hub = reopen(
        &path,
        Arc::try_unwrap(hub).unwrap_or_else(|_| panic!("the server released the hub")),
    );
    assert_restores(
        hub,
        &identity,
        "wiped-parent-after-child",
        &["parents", "children"],
        "after reopen",
    )
    .await;
}

#[tokio::test]
async fn a_wiped_edge_converges_after_current_only_compaction() {
    run_a_wiped_edge_converges_after_current_only_compaction(false).await;
}

#[tokio::test]
async fn a_wiped_edge_converges_after_current_only_compaction_after_the_hub_restarts() {
    run_a_wiped_edge_converges_after_current_only_compaction(true).await;
}

#[tokio::test]
async fn a_wiped_edge_converges_after_retention_pruning() {
    run_a_wiped_edge_converges_after_retention_pruning(false).await;
}

#[tokio::test]
async fn a_wiped_edge_converges_after_retention_pruning_after_the_hub_restarts() {
    run_a_wiped_edge_converges_after_retention_pruning(true).await;
}

#[tokio::test]
async fn a_wiped_edge_converges_after_the_hub_deletes_rows() {
    run_a_wiped_edge_converges_after_the_hub_deletes_rows(false).await;
}

#[tokio::test]
async fn a_wiped_edge_converges_after_the_hub_deletes_rows_after_the_hub_restarts() {
    run_a_wiped_edge_converges_after_the_hub_deletes_rows(true).await;
}

#[tokio::test]
async fn a_wiped_edge_converges_after_authoritative_erasure() {
    run_a_wiped_edge_converges_after_authoritative_erasure(false).await;
}

#[tokio::test]
async fn a_wiped_edge_converges_after_authoritative_erasure_after_the_hub_restarts() {
    run_a_wiped_edge_converges_after_authoritative_erasure(true).await;
}

#[tokio::test]
async fn a_wiped_edge_receives_rows_written_before_the_table_was_synced() {
    run_a_wiped_edge_receives_rows_written_before_the_table_was_synced(false).await;
}

#[tokio::test]
async fn a_wiped_edge_receives_rows_written_before_the_table_was_synced_after_the_hub_restarts() {
    run_a_wiped_edge_receives_rows_written_before_the_table_was_synced(true).await;
}

#[tokio::test]
async fn a_wiped_edge_converges_across_alter_rename_and_drop() {
    run_a_wiped_edge_converges_across_alter_rename_and_drop(false).await;
}

#[tokio::test]
async fn a_wiped_edge_converges_across_alter_rename_and_drop_after_the_hub_restarts() {
    run_a_wiped_edge_converges_across_alter_rename_and_drop(true).await;
}

#[tokio::test]
async fn a_wiped_edge_converges_across_drop_and_recreate() {
    run_a_wiped_edge_converges_across_drop_and_recreate(false).await;
}

#[tokio::test]
async fn a_wiped_edge_converges_across_drop_and_recreate_after_the_hub_restarts() {
    run_a_wiped_edge_converges_across_drop_and_recreate(true).await;
}

#[tokio::test]
async fn a_wiped_edge_converges_when_a_parent_survives_only_after_its_child() {
    run_a_wiped_edge_converges_when_a_parent_survives_only_after_its_child(false).await;
}

#[tokio::test]
async fn a_wiped_edge_converges_when_a_parent_survives_only_after_its_child_after_the_hub_restarts()
{
    run_a_wiped_edge_converges_when_a_parent_survives_only_after_its_child(true).await;
}
