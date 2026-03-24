use super::helpers::make_params;
use contextdb_core::{Error, MemoryAccountant, Value};
use contextdb_engine::Database;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

// ---------------------------------------------------------------------------
// M01 — RED: INSERT succeeds when under budget
// ---------------------------------------------------------------------------
#[test]
fn m01_insert_succeeds_under_budget() {
    // 10MB budget — plenty for a single row.
    let accountant = Arc::new(MemoryAccountant::with_budget(10 * 1024 * 1024));
    let db = Database::open_memory_with_accountant(accountant.clone());
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();

    // INSERT should succeed — budget is large enough.
    let result = db.execute(
        "INSERT INTO items (id, name) VALUES ($id, $name)",
        &make_params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("name", Value::Text("hello".to_string())),
        ]),
    );
    assert!(
        result.is_ok(),
        "INSERT under budget must succeed: {result:?}"
    );

    // Memory used must reflect the actual row size (UUID=16 + "hello"=5 + overhead).
    // A single row with a UUID and short TEXT should use between 32 and 1024 bytes.
    let usage = accountant.usage();
    assert!(
        usage.used >= 32,
        "used bytes must be >= 32 after INSERT, got {}",
        usage.used
    );
    assert!(
        usage.used <= 1024,
        "used bytes must be <= 1024 for a single small row, got {}",
        usage.used
    );
}

// ---------------------------------------------------------------------------
// M02 — RED: INSERT rejected when budget exceeded
// ---------------------------------------------------------------------------
#[test]
fn m02_insert_rejected_when_budget_exceeded() {
    // 4KB budget — enough for CREATE TABLE overhead, too small for a large INSERT.
    let accountant = Arc::new(MemoryAccountant::with_budget(4096));
    let db = Database::open_memory_with_accountant(accountant.clone());
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();

    let result = db.execute(
        "INSERT INTO items (id, name) VALUES ($id, $name)",
        &make_params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            (
                "name",
                Value::Text("this string is long enough to exceed 64 bytes budget".to_string()),
            ),
        ]),
    );
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
            assert_eq!(subsystem, "insert");
            assert!(!operation.is_empty());
            assert!(*requested_bytes > 0);
            assert!(*budget_limit_bytes == 64);
            assert!(*available_bytes <= 64);
            assert!(!hint.is_empty(), "hint must be non-empty for AI agents");
        }
        other => panic!("expected MemoryBudgetExceeded, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// M03 — RED: DELETE reclaims memory, allowing previously-rejected INSERT
// ---------------------------------------------------------------------------
#[test]
fn m03_delete_reclaims_memory_for_insert() {
    // Budget just big enough for one row, not two.
    let accountant = Arc::new(MemoryAccountant::with_budget(512));
    let db = Database::open_memory_with_accountant(accountant.clone());
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();

    let id1 = Uuid::new_v4();
    // First INSERT should succeed.
    db.execute(
        "INSERT INTO items (id, name) VALUES ($id, $name)",
        &make_params(vec![
            ("id", Value::Uuid(id1)),
            ("name", Value::Text("first".to_string())),
        ]),
    )
    .expect("first INSERT must succeed");

    let used_after_first = accountant.usage().used;
    assert!(used_after_first > 0);

    // Second INSERT should fail — budget exhausted.
    let result2 = db.execute(
        "INSERT INTO items (id, name) VALUES ($id, $name)",
        &make_params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("name", Value::Text("second".to_string())),
        ]),
    );
    assert!(result2.is_err(), "second INSERT must fail — budget full");

    // DELETE the first row to free memory.
    db.execute(
        "DELETE FROM items WHERE id = $id",
        &make_params(vec![("id", Value::Uuid(id1))]),
    )
    .expect("DELETE must succeed");

    let used_after_delete = accountant.usage().used;
    assert!(
        used_after_delete < used_after_first,
        "DELETE must reduce used bytes: before={used_after_first}, after={used_after_delete}"
    );

    // Third INSERT should now succeed — memory was reclaimed.
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

    // Verify the row actually exists with correct value.
    let select = db
        .execute(
            "SELECT name FROM items WHERE id = $id",
            &make_params(vec![("id", Value::Uuid(id3))]),
        )
        .expect("SELECT after re-insert must succeed");
    assert_eq!(select.rows.len(), 1, "must find exactly one row");
    assert_eq!(
        select.rows[0][0],
        Value::Text("third".to_string()),
        "row must contain 'third'"
    );
}

// ---------------------------------------------------------------------------
// M04 — RED: Vector search succeeds under budget
// ---------------------------------------------------------------------------
#[test]
fn m04_vector_search_succeeds_under_budget() {
    // 10MB budget — plenty for a small vector search.
    let accountant = Arc::new(MemoryAccountant::with_budget(10 * 1024 * 1024));
    let db = Database::open_memory_with_accountant(accountant);
    db.execute(
        "CREATE TABLE docs (id UUID PRIMARY KEY, embedding VECTOR(4))",
        &empty(),
    )
    .unwrap();

    // Insert a few vectors.
    for _ in 0..5 {
        db.execute(
            "INSERT INTO docs (id, embedding) VALUES ($id, $emb)",
            &make_params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("emb", Value::Vector(vec![1.0, 0.0, 0.0, 0.0])),
            ]),
        )
        .unwrap();
    }

    // Vector search under budget must succeed.
    let result = db.execute(
        "SELECT id FROM docs ORDER BY embedding <=> $q LIMIT 3",
        &make_params(vec![("q", Value::Vector(vec![1.0, 0.0, 0.0, 0.0]))]),
    );
    assert!(
        result.is_ok(),
        "vector search under budget must succeed: {result:?}"
    );
    assert_eq!(result.unwrap().rows.len(), 3);
}

// ---------------------------------------------------------------------------
// M05 — RED: Vector search rejected when budget exceeded
// ---------------------------------------------------------------------------
#[test]
fn m05_vector_search_rejected_over_budget() {
    // 8KB budget — enough for CREATE TABLE + one vector insert, too small for search pre-flight.
    let accountant = Arc::new(MemoryAccountant::with_budget(8192));
    let db = Database::open_memory_with_accountant(accountant.clone());
    db.execute(
        "CREATE TABLE docs (id UUID PRIMARY KEY, embedding VECTOR(384))",
        &empty(),
    )
    .unwrap();

    // Insert one vector (should succeed within 8KB budget).
    let _ = db.execute(
        "INSERT INTO docs (id, embedding) VALUES ($id, $emb)",
        &make_params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("emb", Value::Vector(vec![0.1; 384])),
        ]),
    );

    // Vector search pre-flight: k=10 * overfetch=3 * 384 dims * 4 bytes = 46080 bytes.
    // Budget is 128 bytes — must fail.
    let result = db.execute(
        "SELECT id FROM docs ORDER BY embedding <=> $q LIMIT 10",
        &make_params(vec![("q", Value::Vector(vec![1.0; 384]))]),
    );
    assert!(
        result.is_err(),
        "vector search must fail when budget exceeded"
    );
    let err = result.unwrap_err();
    match &err {
        Error::MemoryBudgetExceeded { subsystem, .. } => {
            assert_eq!(subsystem, "vector_search");
        }
        other => panic!("expected MemoryBudgetExceeded, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// M06 — RED: HNSW build falls back to brute-force under tight budget
// ---------------------------------------------------------------------------
#[test]
fn m06_hnsw_fallback_to_brute_force_under_budget() {
    // Budget too small for HNSW index build but large enough for brute-force.
    // HNSW requires significant memory for graph structure.
    // We set budget to allow inserts but not the HNSW build.
    let accountant = Arc::new(MemoryAccountant::with_budget(256 * 1024)); // 256KB
    let db = Database::open_memory_with_accountant(accountant);
    db.execute(
        "CREATE TABLE docs (id UUID PRIMARY KEY, embedding VECTOR(128))",
        &empty(),
    )
    .unwrap();

    // Insert >= 1000 vectors to trigger HNSW build attempt.
    for i in 0..1000 {
        let mut vec = vec![0.0f32; 128];
        vec[0] = i as f32;
        // Ignore insert failures — some may fail due to budget.
        // We only need enough vectors in the store.
        let _ = db.execute(
            "INSERT INTO docs (id, embedding) VALUES ($id, $emb)",
            &make_params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("emb", Value::Vector(vec)),
            ]),
        );
    }

    // EXPLAIN should show brute-force, not HNSW — budget prevented HNSW build.
    let explain_output = db.explain("SELECT id FROM docs ORDER BY embedding <=> $q LIMIT 5");
    assert!(explain_output.is_ok(), "explain must succeed");
    let explain_output = explain_output.unwrap();
    // After implementation: tight budget prevents HNSW build → BruteForce scan.
    // Without memory accounting: HNSW would build → HnswSearch.
    assert!(
        explain_output.contains("BruteForce"),
        "tight budget should prevent HNSW, showing BruteForce: {explain_output}"
    );
}

// ---------------------------------------------------------------------------
// M07 — RED: BFS traversal succeeds under budget
// ---------------------------------------------------------------------------
#[test]
fn m07_bfs_succeeds_under_budget() {
    let accountant = Arc::new(MemoryAccountant::with_budget(10 * 1024 * 1024));
    let db = Database::open_memory_with_accountant(accountant);
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
    let tx = db.begin();
    db.insert_edge(tx, a, b, "LINKS".to_string(), HashMap::new())
        .unwrap();
    db.insert_edge(tx, b, c, "LINKS".to_string(), HashMap::new())
        .unwrap();
    db.commit(tx).unwrap();

    // BFS from A should reach B and C.
    let result = db.execute(
        "SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->{1,3}(b) WHERE a.id = $start COLUMNS (b.id AS b_id))",
        &make_params(vec![("start", Value::Uuid(a))]),
    );
    assert!(result.is_ok(), "BFS under budget must succeed: {result:?}");
    assert_eq!(
        result.unwrap().rows.len(),
        2,
        "BFS from A must find exactly B and C"
    );
}

// ---------------------------------------------------------------------------
// M08 — RED: BFS traversal rejected when budget exceeded
// ---------------------------------------------------------------------------
#[test]
fn m08_bfs_rejected_over_budget() {
    // 4KB budget — enough for DDL + node inserts, too small for deep BFS frontier.
    let accountant = Arc::new(MemoryAccountant::with_budget(4096));
    let db = Database::open_memory_with_accountant(accountant.clone());
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
        let _ = db.execute(
            "INSERT INTO nodes (id, name) VALUES ($id, $name)",
            &make_params(vec![
                ("id", Value::Uuid(id)),
                ("name", Value::Text(name.to_string())),
            ]),
        );
    }

    let tx = db.begin();
    let _ = db.insert_edge(tx, a, b, "LINKS".to_string(), HashMap::new());
    let _ = db.commit(tx);

    let result = db.execute(
        "SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->{1,3}(b) WHERE a.id = $start COLUMNS (b.id AS b_id))",
        &make_params(vec![("start", Value::Uuid(a))]),
    );
    assert!(result.is_err(), "BFS must fail when budget exceeded");
    let err = result.unwrap_err();
    match &err {
        Error::MemoryBudgetExceeded { subsystem, .. } => {
            assert_eq!(subsystem, "bfs_frontier");
        }
        other => panic!("expected MemoryBudgetExceeded, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// M09 — RED: SET MEMORY_LIMIT and SHOW MEMORY_LIMIT
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
        show_result.columns,
        vec!["limit", "used", "available", "startup_ceiling"]
    );
    assert_eq!(show_result.rows.len(), 1);

    let limit_val = &show_result.rows[0][0];
    match limit_val {
        Value::Text(s) => assert_eq!(s, "536870912", "limit must be 512M in bytes"),
        Value::Int64(n) => assert_eq!(*n, 536870912, "limit must be 512M in bytes"),
        other => panic!("expected text or int for limit, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// M10 — RED: SET MEMORY_LIMIT 'none' removes limit
// ---------------------------------------------------------------------------
#[test]
fn m10_set_memory_limit_none_removes_limit() {
    let db = Database::open_memory();

    // First set a limit.
    db.execute("SET MEMORY_LIMIT '256M'", &empty())
        .expect("SET 256M must succeed");

    // Verify it was set.
    let show1 = db.execute("SHOW MEMORY_LIMIT", &empty()).unwrap();
    let limit1 = &show1.rows[0][0];
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
    let limit2 = &show2.rows[0][0];
    match limit2 {
        Value::Text(s) => assert_eq!(s, "none", "limit must be 'none' after removal"),
        Value::Int64(n) => assert_eq!(*n, 0, "limit must be 0 after removal"),
        other => panic!("expected text or int for limit, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// M11 — RED: SET MEMORY_LIMIT above startup ceiling is rejected
// ---------------------------------------------------------------------------
#[test]
fn m11_set_above_startup_ceiling_rejected() {
    // Create DB with 4G startup ceiling.
    let accountant = Arc::new(MemoryAccountant::with_budget(4 * 1024 * 1024 * 1024));
    let db = Database::open_memory_with_accountant(accountant);

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
// M12 — RED: open_with_config wires accountant to INSERT
// ---------------------------------------------------------------------------
#[test]
fn m12_open_with_config_wires_accountant() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("m12.db");
    let accountant = Arc::new(MemoryAccountant::with_budget(4096)); // tight but fits DDL

    let db = Database::open_with_config(
        &db_path,
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant,
    )
    .unwrap();

    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
        &empty(),
    )
    .unwrap();

    // INSERT with 64-byte budget should fail.
    let result = db.execute(
        "INSERT INTO items (id, name) VALUES ($id, $name)",
        &make_params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            (
                "name",
                Value::Text(
                    "a long enough string to exceed sixty four bytes of budget for sure"
                        .to_string(),
                ),
            ),
        ]),
    );
    assert!(
        result.is_err(),
        "INSERT must fail with tight budget via open_with_config"
    );
    assert!(
        matches!(result.unwrap_err(), Error::MemoryBudgetExceeded { .. }),
        "error must be MemoryBudgetExceeded"
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
    let tx = db.begin();
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
// M14 — RED: thread-safe concurrent allocations
// ---------------------------------------------------------------------------
#[test]
fn m14_concurrent_allocation_accounting() {
    let accountant = Arc::new(MemoryAccountant::with_budget(1_000_000));
    let threads: Vec<_> = (0..10)
        .map(|_| {
            let acc = accountant.clone();
            std::thread::spawn(move || {
                let mut succeeded = 0usize;
                for _ in 0..100 {
                    if acc.try_allocate(100).is_ok() {
                        succeeded += 1;
                    }
                }
                // Release only the allocations that succeeded.
                for _ in 0..succeeded {
                    acc.release(100);
                }
            })
        })
        .collect();

    for t in threads {
        t.join().unwrap();
    }

    // After all threads allocate and release the same amount, used should be 0.
    let usage = accountant.usage();
    assert_eq!(
        usage.used, 0,
        "after balanced alloc/release across threads, used must be 0"
    );
}

// ---------------------------------------------------------------------------
// M15 — RED: UPDATE to larger row tracks memory growth
// ---------------------------------------------------------------------------
#[test]
fn m15_update_tracks_memory_growth() {
    let accountant = Arc::new(MemoryAccountant::with_budget(10 * 1024 * 1024));
    let db = Database::open_memory_with_accountant(accountant.clone());
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
            ("name", Value::Text("short".to_string())),
        ]),
    )
    .unwrap();

    let used_before_update = accountant.usage().used;
    assert!(used_before_update > 0);

    // UPDATE to a much larger value.
    db.execute(
        "UPDATE items SET name = $name WHERE id = $id",
        &make_params(vec![
            ("id", Value::Uuid(id)),
            ("name", Value::Text("x".repeat(2000))),
        ]),
    )
    .unwrap();

    let used_after_update = accountant.usage().used;
    assert!(
        used_after_update > used_before_update,
        "UPDATE to larger row must increase used: before={used_before_update}, after={used_after_update}"
    );
}

// ---------------------------------------------------------------------------
// M16 — RED: Graph edge insertion is memory-accounted
// ---------------------------------------------------------------------------
#[test]
fn m16_graph_edge_insertion_accounted() {
    let accountant = Arc::new(MemoryAccountant::with_budget(10 * 1024 * 1024));
    let db = Database::open_memory_with_accountant(accountant.clone());
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

    let tx = db.begin();
    db.insert_edge(tx, a, b, "LINKS".to_string(), HashMap::new())
        .unwrap();
    db.commit(tx).unwrap();

    let used_after_edges = accountant.usage().used;
    assert!(
        used_after_edges > used_before_edges,
        "edge insertion must increase used: before={used_before_edges}, after={used_after_edges}"
    );
}

// ---------------------------------------------------------------------------
// M17 — RED: Reopen pre-populated database reflects loaded data
// ---------------------------------------------------------------------------
#[test]
fn m17_reopen_reflects_loaded_data() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("m17.db");

    // Populate the database.
    {
        let db = Database::open(&db_path).unwrap();
        db.execute(
            "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)",
            &empty(),
        )
        .unwrap();
        for i in 0..50 {
            db.execute(
                "INSERT INTO items (id, name) VALUES ($id, $name)",
                &make_params(vec![
                    ("id", Value::Uuid(Uuid::new_v4())),
                    ("name", Value::Text(format!("row-{i}"))),
                ]),
            )
            .unwrap();
        }
    }

    // Reopen with an accountant and verify it reflects loaded data.
    let accountant = Arc::new(MemoryAccountant::with_budget(100 * 1024 * 1024));
    let _db = Database::open_with_config(
        &db_path,
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .unwrap();

    let usage = accountant.usage();
    assert!(
        usage.used > 0,
        "reopened database with 50 rows must report used > 0, got {}",
        usage.used
    );
}

// ---------------------------------------------------------------------------
// M18 — RED: DDL operations (CREATE TABLE, CREATE INDEX) are memory-accounted
// ---------------------------------------------------------------------------
#[test]
fn m18_ddl_memory_accounted() {
    let accountant = Arc::new(MemoryAccountant::with_budget(10 * 1024 * 1024));
    let db = Database::open_memory_with_accountant(accountant.clone());

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
