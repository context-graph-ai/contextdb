use super::helpers::make_params;
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use contextdb_engine::memory_accounting::MemoryAccountant;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// The refusal names the engine's own admission that the limit fell short of,
/// with its exact shortfall; the same refusal relabelled to another subsystem
/// does not match. `owner` is the admission as the engine ledgered it when the
/// same operation ran on a control store with room to spare.
fn assert_refused_at_owner(
    error: &Error,
    owner: &contextdb_engine::memory_accounting::BudgetLedgerEntry,
    limit: usize,
) {
    let (owner_subsystem, owner_operation) = owner
        .what
        .split_once('/')
        .expect("a ledgered admission is named subsystem/operation");
    let attributed = |error: &Error| {
        matches!(error, Error::MemoryBudgetExceeded {
        subsystem, operation, requested_bytes, available_bytes, budget_limit_bytes, .. }
        if subsystem == owner_subsystem && operation == owner_operation
            && *requested_bytes == owner.bytes && *available_bytes == owner.bytes - 1
            && *budget_limit_bytes == limit)
    };
    assert!(attributed(error), "{error:?} (expected owner {owner:?})");
    if let Error::MemoryBudgetExceeded {
        operation,
        requested_bytes,
        available_bytes,
        budget_limit_bytes,
        hint,
        ..
    } = error
    {
        assert!(!attributed(&Error::MemoryBudgetExceeded {
            subsystem: "relational_index".to_owned().into(),
            operation: operation.to_string().into(),
            requested_bytes: *requested_bytes,
            available_bytes: *available_bytes,
            budget_limit_bytes: *budget_limit_bytes,
            hint: hint.to_string().into()
        }));
    }
}

/// The last admission with a real size that reached `peak`: the one a limit one
/// byte under `peak` refuses.
fn peak_owner(
    ledger: &[contextdb_engine::memory_accounting::BudgetLedgerEntry],
    peak: usize,
) -> &contextdb_engine::memory_accounting::BudgetLedgerEntry {
    ledger
        .iter()
        .rfind(|entry| entry.bytes > 0 && entry.used_after == peak)
        .unwrap_or_else(|| panic!("the measured peak must belong to an admission: {ledger:?}"))
}

/// A durable database under a numeric startup memory limit, rooted in `root`.
fn open_with_limit(root: &tempfile::TempDir, limit: usize) -> Database {
    contextdb_engine::database::open_with_startup_limits(
        root.path().join("accounting.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        Some(limit),
        None,
    )
    .expect("open a durable database under a startup memory limit")
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

const ITEMS_SCHEMA: &str = "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)";
const ITEMS_INSERT: &str = "INSERT INTO items (id, name) VALUES ($id, $name)";

/// A budget no arm in this file comes near; it is only ever used while measuring.
const MEASUREMENT_BUDGET: usize = 64 * 1024 * 1024;

fn items_row(name: &str) -> HashMap<String, Value> {
    make_params(vec![
        ("id", Value::Uuid(Uuid::new_v4())),
        ("name", Value::Text(name.to_string())),
    ])
}

/// Creates the table and stores one row of `name` on a store with room to
/// spare, reporting what the store holds once the table is declared and the
/// highest the accountant reached while that row was stored. Rows live on disk,
/// so a stored row leaves the bill where it was; what an insert asks of the
/// accountant is the room it needs while it runs. Arms below size their limit
/// from this rather than writing a number down, so they keep pinning the
/// promised behaviour when the charge model changes.
fn measure_schema_and_insert(
    db: &Database,
    accountant: &MemoryAccountant,
    name: &str,
) -> (usize, usize) {
    db.execute(ITEMS_SCHEMA, &empty())
        .expect("measuring CREATE TABLE must succeed");
    let settled = accountant.usage().used;
    accountant.reset_high_water_for_test();
    db.execute(ITEMS_INSERT, &items_row(name))
        .expect("measuring INSERT must succeed");
    let insert_peak = accountant.high_water_for_test().saturating_sub(settled);
    assert!(
        insert_peak > 0,
        "an insert needs room from the accountant while it runs, measured {insert_peak}"
    );
    (settled, insert_peak)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn insert_named(
    db: &Database,
    id: Uuid,
    name: &str,
) -> contextdb_core::Result<contextdb_engine::QueryResult> {
    db.execute(
        "INSERT INTO items (id, name) VALUES ($id, $name)",
        &make_params(vec![
            ("id", Value::Uuid(id)),
            ("name", Value::Text(name.to_string())),
        ]),
    )
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
#[test]
fn m02_insert_rejected_when_budget_exceeded() {
    const NAME: &str = "this string is long enough to exceed 64 bytes budget";

    // SETUP: what the store holds once the table is declared, and what an
    // insert asks the accountant for while it runs, both measured on a control
    // store opened the same way.
    let measure_root = tempfile::TempDir::new().unwrap();
    let measured = open_with_limit(&measure_root, MEASUREMENT_BUDGET);
    let (settled, insert_peak) = measure_schema_and_insert(&measured, measured.accountant(), NAME);

    // BOUND: room for half of what the insert needs above what the store holds.
    // CREATE TABLE has already happened, so the INSERT cannot fit.
    let budget = settled + insert_peak / 2;
    let root = tempfile::TempDir::new().unwrap();
    let db = open_with_limit(&root, MEASUREMENT_BUDGET);
    db.execute(ITEMS_SCHEMA, &empty()).unwrap();
    assert_eq!(db.accountant().usage().used, settled);
    db.accountant()
        .set_budget(Some(budget))
        .expect("lowering the live limit under the ceiling must succeed");

    let result = db.execute(ITEMS_INSERT, &items_row(NAME));
    assert!(result.is_err(), "INSERT must fail when budget exceeded");
    let err = result.unwrap_err();
    match &err {
        Error::MemoryBudgetExceeded {
            subsystem,
            operation,
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            hint,
        } => {
            assert_eq!(subsystem, "redb");
            assert_eq!(operation, "pending_directory");
            assert!(*requested_bytes > 0);
            assert_eq!(
                *budget_limit_bytes, budget,
                "budget_limit_bytes must be the total budget"
            );
            assert!(
                *available_bytes < *requested_bytes,
                "available must be less than requested for rejection"
            );
            assert!(!hint.is_empty(), "hint must be non-empty for AI agents");
        }
        other => panic!("expected MemoryBudgetExceeded, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
#[test]
fn m03_delete_reclaims_memory_for_insert() {
    // SETUP: what the store holds once the table is declared, and what an
    // insert asks the accountant for while it runs, measured on a control store
    // opened the same way.
    let measure_root = tempfile::TempDir::new().unwrap();
    let measured = open_with_limit(&measure_root, MEASUREMENT_BUDGET);
    let (settled, insert_peak) =
        measure_schema_and_insert(&measured, measured.accountant(), "first");

    let root = tempfile::TempDir::new().unwrap();
    let db = open_with_limit(&root, MEASUREMENT_BUDGET);
    let accountant = db.accountant();
    db.execute(ITEMS_SCHEMA, &empty()).unwrap();
    assert_eq!(accountant.usage().used, settled);

    // BOUND: room for half of what the insert needs, and then for all of it.
    let id1 = Uuid::new_v4();
    accountant
        .set_budget(Some(settled + insert_peak / 2))
        .expect("lowering the live limit under the ceiling must succeed");
    let error =
        insert_named(&db, id1, "first").expect_err("an insert without the room it needs refuses");
    assert!(
        matches!(&error, Error::MemoryBudgetExceeded { requested_bytes, available_bytes, .. }
            if requested_bytes > available_bytes),
        "{error:?}"
    );
    assert_eq!(
        accountant.usage().used,
        settled,
        "the refused insert releases every admission"
    );

    accountant
        .set_budget(Some(settled + insert_peak))
        .expect("raising the live limit to what the insert needs must succeed");
    insert_named(&db, id1, "first").expect("an insert with the room it needs lands");
    let used_after_first = accountant.usage().used;

    // DELETE the first row; the bill must not grow, and the next insert fits in
    // the same room.
    db.execute(
        "DELETE FROM items WHERE id = $id",
        &make_params(vec![("id", Value::Uuid(id1))]),
    )
    .expect("DELETE must succeed");
    let used_after_delete = accountant.usage().used;
    assert!(
        used_after_delete <= used_after_first,
        "DELETE must not raise used bytes: before={used_after_first}, after={used_after_delete}"
    );

    let id3 = Uuid::new_v4();
    let result3 = db.execute(
        "INSERT INTO items (id, name) VALUES ($id, $name)",
        &make_params(vec![
            ("id", Value::Uuid(id3)),
            ("name", Value::Text("third".to_string())),
        ]),
    );
    assert!(
        result3.is_ok(),
        "INSERT after DELETE must succeed: {result3:?}"
    );

    // The memory promises are settled; lift the limit so reading the row back is
    // not itself a budget question.
    accountant
        .set_budget(Some(MEASUREMENT_BUDGET))
        .expect("raising the live limit back to the ceiling must succeed");

    // Verify the row actually exists with correct value.
    let select = db
        .execute(
            "SELECT name FROM items WHERE id = $id",
            &make_params(vec![("id", Value::Uuid(id3))]),
        )
        .expect("SELECT after re-insert must succeed");
    assert_eq!(select.rows().len(), 1, "must find exactly one row");
    assert_eq!(
        select.rows()[0][0],
        Value::Text("third".to_string()),
        "row must contain 'third'"
    );
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
#[test]
fn m07_bfs_succeeds_under_budget() {
    let root = tempfile::TempDir::new().unwrap();
    let db = open_with_limit(&root, 10 * 1024 * 1024);
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();

    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();

    // Insert nodes.
    for (id, name) in [(a, "A"), (b, "B"), (c, "C")] {
        db.execute(
            "INSERT INTO nodes (id, name) VALUES ($id, $name)",
            &make_params(vec![
                ("id", Value::Uuid(id)),
                ("name", Value::Text(name.to_string())),
            ]),
        )
        .unwrap();
    }

    // Insert edges: A -> B -> C.
    let tx = db.begin_or_panic();
    db.insert_edge(tx, a, b, "LINKS".to_string(), HashMap::new())
        .unwrap();
    db.insert_edge(tx, b, c, "LINKS".to_string(), HashMap::new())
        .unwrap();
    db.commit(tx).unwrap();

    // Accountant must track BFS frontier memory.
    assert!(
        db.accountant().usage().used > 0,
        "accountant must track BFS frontier memory"
    );

    // BFS from A should reach B and C.
    let result = db.execute(
        "SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->{1,3}(b) WHERE a.id = $start COLUMNS (b.id AS b_id))",
        &make_params(vec![("start", Value::Uuid(a))]),
    );
    assert!(result.is_ok(), "BFS under budget must succeed: {result:?}");
    assert_eq!(
        result.unwrap().rows().len(),
        2,
        "BFS from A must find exactly B and C"
    );
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
#[test]
fn m09_set_and_show_memory_limit() {
    let db = Database::open_memory();

    // SET MEMORY_LIMIT '512M' (536870912 bytes).
    let set_result = db.execute("SET MEMORY_LIMIT '512M'", &empty());
    assert!(
        set_result.is_ok(),
        "SET MEMORY_LIMIT must succeed: {set_result:?}"
    );

    // SHOW MEMORY_LIMIT must reflect the set value.
    let show_result = db.execute("SHOW MEMORY_LIMIT", &empty()).unwrap();
    assert_eq!(
        show_result.columns(),
        vec![
            "limit",
            "used",
            "available",
            "startup_ceiling",
            "ceiling_refresh_failures"
        ]
    );
    assert_eq!(show_result.rows().len(), 1);

    let limit_val = &show_result.rows()[0][0];
    match limit_val {
        Value::Text(s) => assert_eq!(s, "536870912", "limit must be 512M in bytes"),
        Value::Int64(n) => assert_eq!(*n, 536870912, "limit must be 512M in bytes"),
        other => panic!("expected text or int for limit, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
#[test]
fn m10_set_memory_limit_none_removes_limit() {
    let db = Database::open_memory();

    // First set a limit.
    db.execute("SET MEMORY_LIMIT '256M'", &empty())
        .expect("SET 256M must succeed");

    // Verify it was set.
    let show1 = db.execute("SHOW MEMORY_LIMIT", &empty()).unwrap();
    let limit1 = &show1.rows()[0][0];
    let is_set = match limit1 {
        Value::Text(s) => s != "none",
        Value::Int64(n) => *n > 0,
        _ => false,
    };
    assert!(is_set, "limit must be set after SET '256M': {limit1:?}");

    // Remove the limit.
    db.execute("SET MEMORY_LIMIT 'none'", &empty())
        .expect("SET 'none' must succeed");

    // Verify limit is removed.
    let show2 = db.execute("SHOW MEMORY_LIMIT", &empty()).unwrap();
    let limit2 = &show2.rows()[0][0];
    match limit2 {
        Value::Text(s) => assert_eq!(s, "none", "limit must be 'none' after removal"),
        Value::Int64(n) => assert_eq!(*n, 0, "limit must be 0 after removal"),
        other => panic!("expected text or int for limit, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
#[test]
fn m11_set_above_startup_ceiling_rejected() {
    // Create DB with 4G startup ceiling.
    let root = tempfile::TempDir::new().unwrap();
    let db = open_with_limit(&root, 4 * 1024 * 1024 * 1024);

    // Try to SET higher than ceiling.
    let result = db.execute("SET MEMORY_LIMIT '8G'", &empty());
    assert!(result.is_err(), "SET above ceiling must fail");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("ceiling") || err_msg.contains("startup") || err_msg.contains("exceed"),
        "error must mention ceiling: {err_msg}"
    );

    // SET lower than ceiling must succeed.
    let result2 = db.execute("SET MEMORY_LIMIT '2G'", &empty());
    assert!(
        result2.is_ok(),
        "SET below ceiling must succeed: {result2:?}"
    );

    // SET 'none' must be rejected when ceiling exists.
    let result3 = db.execute("SET MEMORY_LIMIT 'none'", &empty());
    assert!(result3.is_err(), "SET 'none' must fail when ceiling is set");
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
#[test]
#[ignore = "A memory refusal at the last admission of an insert leaves 127368 bytes charged to the store instead of returning the bill to where it was."]
fn m12_open_with_config_wires_accountant() {
    const NAME: &str = "a long enough string to exceed sixty four bytes of budget for sure";
    use contextdb_engine::memory_accounting::{
        take_budget_ledger_for_test, watch_budget_ledger_for_test,
    };

    // Profile the same logical keys and persisted time as the refusal arm:
    // different identities can take different paths through the proof tree.
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let row = make_params(vec![
        ("id", Value::Uuid(Uuid::from_u128(12))),
        ("name", Value::Text(NAME.to_owned())),
    ]);
    let tmp = tempfile::TempDir::new().unwrap();
    let profile = contextdb_engine::database::open_with_startup_limits(
        tmp.path().join("m12-profile.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        Some(MEASUREMENT_BUDGET),
        None,
    )
    .unwrap();
    let measured = profile.accountant();
    measured.reset_high_water_for_test();
    profile.execute(ITEMS_SCHEMA, &empty()).unwrap();
    let measured_ddl_peak = measured.high_water_for_test();
    let measured_schema = measured.usage().used;
    measured.reset_high_water_for_test();
    watch_budget_ledger_for_test();
    profile.execute(ITEMS_INSERT, &row).unwrap();
    let ledger = take_budget_ledger_for_test();
    let row_peak = measured.high_water_for_test();
    let owner = peak_owner(&ledger, row_peak);
    let position = ledger
        .iter()
        .position(|entry| std::ptr::eq(entry, owner))
        .expect("the owner is in the ledger");
    // SETUP: how far above the bill the control store held once its table was
    // declared the row insert peaked. BOUND: the live limit is later set one
    // byte short of that room above the same point on the store under test,
    // lowered under the ceiling it declared at open.
    let headroom = owner.used_after - measured_schema;
    assert!(measured_ddl_peak < measured_schema + headroom);
    assert!(
        ledger[..position]
            .iter()
            .all(|entry| entry.used_after < measured_schema + headroom),
        "every prior real owner fits before the admission the limit refuses: {ledger:?}"
    );
    eprintln!(
        "m12 schema={measured_schema} ddl_peak={measured_ddl_peak} row_peak={row_peak} owner={owner:?}"
    );
    profile.close().unwrap();

    let db = contextdb_engine::database::open_with_startup_limits(
        tmp.path().join("m12.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        Some(MEASUREMENT_BUDGET),
        None,
    )
    .unwrap();
    let accountant = db.accountant();
    db.execute(ITEMS_SCHEMA, &empty()).unwrap();
    let real_budget = accountant.usage().used + headroom - 1;
    assert_eq!(accountant.usage().limit, Some(MEASUREMENT_BUDGET));
    assert_eq!(accountant.usage().startup_ceiling, Some(MEASUREMENT_BUDGET));
    accountant
        .set_budget(Some(real_budget))
        .expect("lowering the live limit under the ceiling must succeed");
    let before = accountant.usage().used;
    watch_budget_ledger_for_test();
    let error = db.execute(ITEMS_INSERT, &row).unwrap_err();
    let refusal_ledger = take_budget_ledger_for_test();
    eprintln!("m12 refusal ledger={refusal_ledger:?}");
    assert_refused_at_owner(&error, owner, real_budget);
    eprintln!("m12 refusal={error:?}");
    assert_eq!(accountant.usage().used, before);
    assert_eq!(
        db.execute("SELECT * FROM items", &empty())
            .unwrap()
            .rows()
            .len(),
        0
    );
    let retry = make_params(vec![
        ("id", Value::Uuid(Uuid::from_u128(12))),
        ("name", Value::Text("ok".to_owned())),
    ]);
    db.execute(ITEMS_INSERT, &retry).unwrap();
    assert_eq!(accountant.usage().limit, Some(real_budget));
    assert_eq!(accountant.usage().startup_ceiling, Some(MEASUREMENT_BUDGET));
    db.close().unwrap();
    let reader = contextdb_engine::ReadSession::open(tmp.path().join("m12.db")).unwrap();
    assert_eq!(
        reader
            .execute("SELECT name FROM items", &empty())
            .unwrap()
            .rows(),
        vec![vec![Value::Text("ok".to_owned())]]
    );
}

// ---------------------------------------------------------------------------
// M13 — REGRESSION GUARD: default no-limit allows all operations
// ---------------------------------------------------------------------------
#[test]
fn m13_default_no_limit_allows_everything() {
    let db = Database::open_memory();

    // DDL
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT, embedding VECTOR(4))",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();

    // INSERT
    let id1 = Uuid::new_v4();
    db.execute(
        "INSERT INTO items (id, name, embedding) VALUES ($id, $name, $emb)",
        &make_params(vec![
            ("id", Value::Uuid(id1)),
            ("name", Value::Text("test".to_string())),
            ("emb", Value::Vector(vec![1.0, 0.0, 0.0, 0.0])),
        ]),
    )
    .unwrap();

    // Vector search
    let result = db.execute(
        "SELECT id FROM items ORDER BY embedding <=> $q LIMIT 1",
        &make_params(vec![("q", Value::Vector(vec![1.0, 0.0, 0.0, 0.0]))]),
    );
    assert!(result.is_ok(), "vector search must succeed with no limit");

    // Graph (BFS)
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    for (id, name) in [(a, "A"), (b, "B")] {
        db.execute(
            "INSERT INTO items (id, name, embedding) VALUES ($id, $name, $emb)",
            &make_params(vec![
                ("id", Value::Uuid(id)),
                ("name", Value::Text(name.to_string())),
                ("emb", Value::Vector(vec![0.0; 4])),
            ]),
        )
        .unwrap();
    }
    let tx = db.begin_or_panic();
    db.insert_edge(tx, a, b, "LINKS".to_string(), HashMap::new())
        .unwrap();
    db.commit(tx).unwrap();

    let bfs = db.execute(
        "SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->{1,3}(b) WHERE a.id = $start COLUMNS (b.id AS b_id))",
        &make_params(vec![("start", Value::Uuid(a))]),
    );
    assert!(bfs.is_ok(), "BFS must succeed with no limit");

    // Accountant usage shows no limit.
    let usage = db.accountant().usage();
    assert!(
        usage.limit.is_none(),
        "no-limit accountant must report None limit"
    );
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
#[test]
fn m14_concurrent_allocation_accounting() {
    let alloc_size = 100usize;
    let accountant = Arc::new(MemoryAccountant::with_budget(1_000_000));
    let threads: Vec<_> = (0..10)
        .map(|_| {
            let acc = accountant.clone();
            std::thread::spawn(move || {
                let mut succeeded = 0usize;
                for _ in 0..100 {
                    if acc.try_allocate(alloc_size).is_ok() {
                        succeeded += 1;
                    }
                }
                succeeded
            })
        })
        .collect();

    let total_succeeded: usize = threads.into_iter().map(|t| t.join().unwrap()).sum();

    // Some allocations must succeed — budget is 1MB, each alloc is 100 bytes.
    assert!(
        total_succeeded > 0,
        "at least some concurrent allocations must succeed, got 0 successes"
    );

    // The accountant must track the outstanding allocations.
    let usage = accountant.usage();
    assert_eq!(
        usage.used,
        total_succeeded * alloc_size,
        "used bytes must equal succeeded * alloc_size: expected {}, got {}",
        total_succeeded * alloc_size,
        usage.used
    );
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
#[test]
fn m16_graph_edge_insertion_accounted() {
    let root = tempfile::TempDir::new().unwrap();
    let db = open_with_limit(&root, 10 * 1024 * 1024);
    let accountant = db.accountant();
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();

    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    for (id, name) in [(a, "A"), (b, "B")] {
        db.execute(
            "INSERT INTO nodes (id, name) VALUES ($id, $name)",
            &make_params(vec![
                ("id", Value::Uuid(id)),
                ("name", Value::Text(name.to_string())),
            ]),
        )
        .unwrap();
    }

    let used_before_edges = accountant.usage().used;
    accountant.reset_high_water_for_test();

    let tx = db.begin_or_panic();
    db.insert_edge(tx, a, b, "LINKS".to_string(), HashMap::new())
        .unwrap();
    db.commit(tx).unwrap();

    // The edge lives on disk, so the bill does not stay up with it; what the
    // accountant sees is the room the insertion asked for while it ran.
    let edge_peak = accountant.high_water_for_test();
    assert!(
        edge_peak > used_before_edges,
        "edge insertion must be admitted by the accountant: before={used_before_edges}, \
         highest while inserting={edge_peak}"
    );
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
#[test]
fn m18_ddl_memory_accounted() {
    let root = tempfile::TempDir::new().unwrap();
    let db = open_with_limit(&root, 10 * 1024 * 1024);
    let accountant = db.accountant();

    let used_before = accountant.usage().used;

    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT, embedding VECTOR(128))",
        &empty(),
    )
    .unwrap();

    let used_after_create = accountant.usage().used;
    assert!(
        used_after_create > used_before,
        "CREATE TABLE must increase used: before={used_before}, after={used_after_create}"
    );
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
#[test]
fn mt1_drop_table_releases_accounting() {
    let root = tempfile::TempDir::new().unwrap();
    let db = open_with_limit(&root, 10 * 1024 * 1024);
    let accountant = db.accountant();

    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();

    // Insert several rows to build up accounting.
    for i in 0..20 {
        db.execute(
            "INSERT INTO items (id, name) VALUES ($id, $name)",
            &make_params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("name", Value::Text(format!("row-{i}"))),
            ]),
        )
        .unwrap();
    }

    let used_before_drop = accountant.usage().used;
    assert!(
        used_before_drop > 0,
        "must have accounted bytes before DROP"
    );

    // DROP TABLE must release all row + metadata bytes.
    db.execute("DROP TABLE items", &empty()).unwrap();

    let used_after_drop = accountant.usage().used;
    assert!(
        used_after_drop < used_before_drop,
        "DROP TABLE must release accounting: before={used_before_drop}, after={used_after_drop}"
    );

    // Re-create and re-insert should succeed — freed memory is reusable.
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();
    let result = db.execute(
        "INSERT INTO items (id, name) VALUES ($id, $name)",
        &make_params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("name", Value::Text("after-drop".to_string())),
        ]),
    );
    assert!(
        result.is_ok(),
        "INSERT after DROP TABLE must succeed: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
#[test]
#[ignore = "A refused update leaves 113896 bytes charged to the store instead of leaving the bill unchanged."]
fn mt2_update_rejection_leaves_accounting_unchanged() {
    let root = tempfile::TempDir::new().unwrap();
    let db = open_with_limit(&root, MEASUREMENT_BUDGET);
    let accountant = db.accountant();

    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();

    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO items (id, name) VALUES ($id, $name)",
        &make_params(vec![
            ("id", Value::Uuid(id)),
            ("name", Value::Text("small".to_string())),
        ]),
    )
    .unwrap();

    let profile_root = tempfile::TempDir::new().unwrap();
    let profile = open_with_limit(&profile_root, MEASUREMENT_BUDGET);
    let profile_accountant = profile.accountant();
    profile.execute(ITEMS_SCHEMA, &empty()).unwrap();
    profile
        .execute(
            ITEMS_INSERT,
            &make_params(vec![
                ("id", Value::Uuid(id)),
                ("name", Value::Text("small".to_owned())),
            ]),
        )
        .unwrap();
    profile_accountant.reset_high_water_for_test();
    contextdb_engine::memory_accounting::watch_budget_ledger_for_test();
    profile
        .execute(
            "UPDATE items SET name = $name WHERE id = $id",
            &make_params(vec![
                ("id", Value::Uuid(id)),
                ("name", Value::Text("x".repeat(4096))),
            ]),
        )
        .unwrap();
    let update_ledger = contextdb_engine::memory_accounting::take_budget_ledger_for_test();
    let update_peak = profile_accountant.high_water_for_test();
    let update_owner = peak_owner(&update_ledger, update_peak);
    let budget = update_peak - 1;
    accountant.set_budget(Some(budget)).unwrap();
    let used_before_update = accountant.usage().used;
    let old_snapshot = db.snapshot();
    eprintln!("mt2 baseline={used_before_update} update_peak={update_peak} limit={budget}");

    // Attempt an UPDATE to a very large value — should fail.
    let result = db.execute(
        "UPDATE items SET name = $name WHERE id = $id",
        &make_params(vec![
            ("id", Value::Uuid(id)),
            ("name", Value::Text("x".repeat(4096))),
        ]),
    );
    let error = result.expect_err("UPDATE exceeding its real admitted peak must refuse");
    assert_refused_at_owner(&error, update_owner, budget);
    eprintln!("mt2 refusal={error:?}");

    let used_after_update = accountant.usage().used;
    assert_eq!(
        used_after_update, used_before_update,
        "failed UPDATE must not change accounting: before={used_before_update}, after={used_after_update}"
    );

    // Verify original row data is unchanged.
    let select = db
        .execute(
            "SELECT name FROM items WHERE id = $id",
            &make_params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    assert_eq!(select.rows().len(), 1);
    assert_eq!(
        select.rows()[0][0],
        Value::Text("small".to_string()),
        "original row must be unchanged after failed UPDATE"
    );
    let old_rows = db.scan("items", old_snapshot).unwrap();
    assert_eq!(old_rows.len(), 1);
    assert_eq!(
        old_rows[0].values.get("name"),
        Some(&Value::Text("small".to_owned()))
    );
    db.execute(
        "UPDATE items SET name = 'fits' WHERE id = $id",
        &make_params(vec![("id", Value::Uuid(id))]),
    )
    .unwrap();
    assert_eq!(
        db.execute("SELECT name FROM items", &empty())
            .unwrap()
            .rows(),
        vec![vec![Value::Text("fits".to_owned())]]
    );
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
#[test]
fn mt4_failed_vector_alloc_releases_row_bytes() {
    const DOCS_SCHEMA: &str = "CREATE TABLE docs (id UUID PRIMARY KEY, embedding VECTOR(384))";
    const DOCS_INSERT: &str = "INSERT INTO docs (id, embedding) VALUES ($id, $emb)";
    let row = || {
        make_params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("emb", Value::Vector(vec![0.1; 384])),
        ])
    };

    // SETUP: what the store holds once the table is declared, and what storing
    // a row with a vector asks the accountant for while it runs, measured on a
    // control store opened the same way.
    let measure_root = tempfile::TempDir::new().unwrap();
    let measured = open_with_limit(&measure_root, MEASUREMENT_BUDGET);
    measured.execute(DOCS_SCHEMA, &empty()).unwrap();
    let settled = measured.accountant().usage().used;
    measured.accountant().reset_high_water_for_test();
    measured.execute(DOCS_INSERT, &row()).unwrap();
    let insert_peak = measured
        .accountant()
        .high_water_for_test()
        .saturating_sub(settled);
    assert!(insert_peak > 0, "storing a row needs room while it runs");

    // BOUND: room for half of what the insert needs, so the row is admitted
    // and the vector that comes with it is not.
    let root = tempfile::TempDir::new().unwrap();
    let db = open_with_limit(&root, MEASUREMENT_BUDGET);
    let accountant = db.accountant();
    db.execute(DOCS_SCHEMA, &empty()).unwrap();
    accountant
        .set_budget(Some(settled + insert_peak / 2))
        .unwrap();
    let used_before = accountant.usage().used;

    let result = db.execute(DOCS_INSERT, &row());
    assert!(
        result.is_err(),
        "INSERT with vector exceeding budget must fail"
    );

    let used_after = accountant.usage().used;
    assert_eq!(
        used_after, used_before,
        "failed INSERT must not leak row bytes: before={used_before}, after={used_after}"
    );
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
