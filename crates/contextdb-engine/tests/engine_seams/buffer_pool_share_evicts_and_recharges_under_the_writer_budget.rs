//! "Working memory covers current rows, index pages and write buffers. A cache
//! keeps recently used pages within an accounted limit and releases them."
use crate::resident_history_support as support;
use contextdb_core::{Value, VectorPartitionKey, Wallclock};
use contextdb_engine::persistence::redb_writer_cache_bytes_for_limit_for_test;
use support::*;

#[test]
fn buffer_pool_share_evicts_and_recharges_under_the_writer_budget() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pool.db");
    const LIMIT: usize = 16 * 1024 * 1024;
    let db = open(&path, Some(LIMIT));
    exec(
        &db,
        "CREATE TABLE pages (id INT PRIMARY KEY, body TEXT, probe INT NOT NULL)",
    );
    exec(&db, "CREATE INDEX pages_probe ON pages (probe,id)");
    exec(
        &db,
        "CREATE TABLE memberships (id INT PRIMARY KEY, v VECTOR(16)) HISTORY ALL",
    );
    for id in 0..256 {
        db.execute(
            "INSERT INTO pages VALUES ($id,$body,$probe)",
            &std::collections::HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("probe".into(), Value::Int64(id)),
                (
                    "body".into(),
                    Value::Text(format!("{id}:{}", "p".repeat(16384))),
                ),
            ]),
        )
        .unwrap();
        insert_vector(&db, "memberships", id as usize, None, 16);
        let tx = db.begin_or_panic();
        db.insert_edge(
            tx,
            uuid::Uuid::from_u128(1000 + id as u128),
            uuid::Uuid::from_u128(1001 + id as u128),
            "links".into(),
            std::collections::HashMap::from([("body".into(), Value::Text("e".repeat(16384)))]),
        )
        .unwrap();
        db.commit(tx).unwrap();
    }
    db.close().unwrap();
    drop(db);
    let db = open(&path, Some(LIMIT));
    let before = db.__writer_cache_stats_for_test().unwrap();
    for id in 0..256 {
        assert_eq!(
            exec(&db, &format!("SELECT id FROM pages WHERE id = {id}")),
            vec![vec![Value::Int64(id)]]
        );
    }
    let indexed = db
        .execute(
            "SELECT id FROM pages WHERE probe >= 0 AND probe < 4 ORDER BY probe,id",
            &empty(),
        )
        .unwrap();
    assert_eq!(indexed.trace.index_used.as_deref(), Some("pages_probe"));
    assert_eq!(
        indexed.rows,
        (0..4).map(|id| vec![Value::Int64(id)]).collect::<Vec<_>>()
    );
    let indexes = db.__writer_cache_stats_for_test().unwrap();
    let mut passes = Vec::new();
    for _ in 0..2 {
        for id in 0..256 {
            assert_eq!(
                exec(&db, &format!("SELECT body FROM pages WHERE id = {id}")),
                vec![vec![Value::Text(format!("{id}:{}", "p".repeat(16384)))]]
            );
        }
        passes.push(db.__writer_cache_stats_for_test().unwrap());
    }
    // Repetition without intervening pressure establishes a real cache hit.
    exec(&db, "SELECT body FROM pages WHERE id = 255");
    let warm = db.__writer_cache_stats_for_test().unwrap();
    for id in 0..256 {
        assert_eq!(
            db.edge_count(uuid::Uuid::from_u128(1000 + id), "links", db.snapshot())
                .unwrap(),
            1
        );
    }
    let graph = db.__writer_cache_stats_for_test().unwrap();
    for id in 0..256 {
        exec(&db, &format!("SELECT body FROM pages WHERE id = {id}"));
    }
    let before_reload = db.__writer_cache_stats_for_test().unwrap();
    let indexed = db
        .execute(
            "SELECT id FROM pages WHERE probe >= 0 AND probe < 4 ORDER BY probe,id",
            &empty(),
        )
        .unwrap();
    assert_eq!(indexed.trace.index_used.as_deref(), Some("pages_probe"));
    assert_eq!(
        indexed.rows,
        (0..4).map(|id| vec![Value::Int64(id)]).collect::<Vec<_>>()
    );
    let index_reload = db.__writer_cache_stats_for_test().unwrap();
    assert!(
        index_reload.1 > before_reload.1,
        "displaced secondary posting pages miss again"
    );
    assert_eq!(
        db.edge_count(uuid::Uuid::from_u128(1000), "links", db.snapshot())
            .unwrap(),
        1
    );
    let graph_reload = db.__writer_cache_stats_for_test().unwrap();
    assert!(
        graph_reload.1 > index_reload.1,
        "displaced graph pages miss again"
    );
    let store = db.vector_store_for_test();
    query_vector(&db, "memberships", 0, 16, "EXACT");
    store.evict_raw_partition_for_test(&index("memberships"), &VectorPartitionKey::unpartitioned());
    // A streamed membership route may own no whole-partition body to drop.
    // Actual disjoint source reads provide the eviction control in both cases.
    for id in 0..256 {
        exec(&db, &format!("SELECT body FROM pages WHERE id = {id}"));
    }
    let before_membership = db.__writer_cache_stats_for_test().unwrap();
    assert_eq!(
        exact_partition_ids(&db, "memberships", db.snapshot(), None, 16, 256),
        (0..256).collect::<Vec<_>>()
    );
    let membership = db.__writer_cache_stats_for_test().unwrap();

    // This is a real storage-page AccessGuard, not a synthetic accountant token.
    let held = db.__hold_row_page_for_test("pages", 0).unwrap();
    let held_body = held.value().to_vec();
    for id in 1..256 {
        exec(&db, &format!("SELECT body FROM pages WHERE id = {id}"));
    }
    assert_eq!(
        held.value(),
        held_body.as_slice(),
        "an evicted page stays readable to its statement holder"
    );
    let during = db.__writer_cache_stats_for_test().unwrap();
    drop(held);
    let released = db.__writer_cache_stats_for_test().unwrap();
    let index_bytes = db.__writer_cache_index_bytes_for_test().unwrap();
    let receipt = db.writer_memory_ownership_receipt_for_test();
    assert_eq!(receipt.reconciliation_gap, 0);
    assert!(receipt.named_owner_total <= LIMIT);
    assert_eq!(
        receipt.persistence_cache,
        redb_writer_cache_bytes_for_limit_for_test(Some(LIMIT)) + index_bytes,
        "one named cache owner"
    );
    assert!(
        during.3 <= receipt.persistence_cache - index_bytes,
        "held pages stay inside accounted cache ownership"
    );
    // Preserve the fixed cache owner and leave only a bounded source-page
    // allowance. Once held pages fill the cache, uncached holders must refuse.
    let saturation_limit = db.accountant().usage().used + 64 * 1024;
    db.set_memory_limit(Some(saturation_limit)).unwrap();
    db.accountant().reset_high_water_for_test();
    // Pin distinct pages until the real page source cannot admit another.
    let mut held_pages = Vec::new();
    let mut page_refusal = None;
    for ordinal in 0..256 {
        match db.__hold_row_page_for_test("pages", ordinal) {
            Ok(page) => held_pages.push(page),
            Err(error) => {
                page_refusal = Some(error);
                break;
            }
        }
    }
    let saturated = db.__writer_cache_stats_for_test().unwrap();
    assert!(
        saturated.3 <= receipt.persistence_cache - index_bytes,
        "saturation never overshoots the charged page owner"
    );
    assert!(
        db.accountant().high_water_for_test() <= saturation_limit,
        "all pinned source pages remain admitted under the declared budget"
    );
    drop(held_pages);
    db.set_memory_limit(Some(LIMIT)).unwrap();
    let before_refusal = exec(&db, "SELECT COUNT(*) FROM pages");
    db.set_memory_limit(Some(1)).unwrap();
    let refused = db.execute(
        "INSERT INTO pages VALUES (1000,'must not publish',1000)",
        &empty(),
    );
    assert!(
        matches!(
            refused,
            Err(contextdb_core::Error::MemoryBudgetExceeded { .. })
        ),
        "writer saturation has the typed refusal: {refused:?}"
    );
    db.set_memory_limit(Some(LIMIT)).unwrap();
    assert_eq!(
        exec(&db, "SELECT COUNT(*) FROM pages"),
        before_refusal,
        "refusal preserves committed progress"
    );
    eprintln!(
        "cache before={before:?} index={indexes:?} passes={passes:?} warm={warm:?} graph={graph:?} member_before={before_membership:?} member={membership:?} held={during:?} released={released:?} saturated={saturated:?} refusal={page_refusal:?}"
    );
    assert!(
        passes[0].2 > indexes.2 && passes[1].2 > passes[0].2,
        "disjoint passes must cause actual evictions"
    );
    assert!(
        passes[1].1 > passes[0].1,
        "evicted pages are fetched from durable source on the next pass"
    );
    assert!(
        warm.0 > passes[1].0,
        "warm reread reaches the same page cache"
    );
    assert!(
        released.3 < during.3,
        "only the last holder releases the evicted page charge"
    );
    assert!(
        membership.1 > before_membership.1,
        "membership/body reload reaches the writer page source"
    );
    assert!(
        indexes.0 + indexes.1 > before.0 + before.1 && graph.0 + graph.1 > warm.0 + warm.1,
        "index and graph reads must use the same durable page source, not whole resident mirrors"
    );
    assert!(
        matches!(
            page_refusal,
            Some(contextdb_core::Error::MemoryBudgetExceeded { .. })
        ),
        "page saturation must map to the existing typed memory refusal: {page_refusal:?}"
    );
    assert_eq!(
        receipt.persistence_cache,
        (LIMIT / 16)
            .min(64 * 1024 * 1024)
            .max((LIMIT / 16).min(256 * 1024))
            + index_bytes,
        "declared bounded writer share"
    );
    let inventory = &receipt.relational_inventory;
    assert_eq!(
        inventory.index_trees + inventory.index_exact + inventory.index_membership,
        0,
        "secondary posting mirrors remain absent after real durable reads"
    );
    assert_eq!(
        receipt.graph_entries, 0,
        "graph reads retain no whole forward adjacency mirror"
    );
    db.close().unwrap();
    assert_native_cache_index_charge_on_writer_settle_and_read_only_open();
}

fn assert_native_cache_index_charge_on_writer_settle_and_read_only_open() {
    use contextdb_engine::memory_accounting::{
        take_budget_ledger_for_test, watch_budget_ledger_for_test,
    };
    use contextdb_engine::{ReadSession, ReadSessionOptions};
    // The native startup cap is 8 MiB: these exercise settle-down and settle-up.
    for limit in [64 * 1024 * 1024, 256 * 1024 * 1024] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("exact-cache-index.db");
        let database = open(&path, Some(limit));
        exec(
            &database,
            "CREATE TABLE cache_charge_rows (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL",
        );
        exec(
            &database,
            "INSERT INTO cache_charge_rows VALUES (1, 'held exactly')",
        );
        let page_cap = redb_writer_cache_bytes_for_limit_for_test(Some(limit));
        let index_bytes = database.__writer_cache_index_bytes_for_test().unwrap();
        assert!(index_bytes > 0);
        let receipt = database.writer_memory_ownership_receipt_for_test();
        assert_eq!(
            receipt.persistence_cache,
            page_cap + index_bytes,
            "the actual writer charge is the native settle return, including its exact index"
        );
        assert_eq!(receipt.reconciliation_gap, 0);
        assert_eq!(
            database.__writer_cache_index_bytes_for_test().unwrap(),
            index_bytes
        );
        database.close().unwrap();
        drop(database);

        struct StopLedger;
        impl Drop for StopLedger {
            fn drop(&mut self) {
                let _ = take_budget_ledger_for_test();
            }
        }
        watch_budget_ledger_for_test();
        let _stop_ledger = StopLedger;
        let reader = ReadSession::open_with_options(
            &path,
            ReadSessionOptions {
                memory_limit: Some(64 * 1024 * 1024),
                ..Default::default()
            },
        )
        .unwrap();
        let ledger = take_budget_ledger_for_test();
        let (reader_index, source_charge) = reader.file_cache_index_charge_for_test().unwrap();
        assert!(reader_index > 0);
        // Read the unchanged source reserve independently of the actual native index.
        let (read_page_cap, fixed_source_bytes) =
            contextdb_engine::persistence::read_source_cache_reservation_for_limit_for_test(Some(
                64 * 1024 * 1024,
            ));
        let base_source_charge = read_page_cap + fixed_source_bytes;
        let base = ledger
            .iter()
            .filter(|entry| {
                entry.what == "direct_read/hydrate_owned_image" && entry.bytes == base_source_charge
            })
            .collect::<Vec<_>>();
        let index = ledger
            .iter()
            .filter(|entry| {
                entry.what == "direct_read/hydrate_owned_image" && entry.bytes == reader_index
            })
            .collect::<Vec<_>>();
        assert_eq!(
            base.len(),
            1,
            "the unchanged source reservation is admitted once"
        );
        assert_eq!(
            index.len(),
            1,
            "the actual read-only index is separately admitted once"
        );
        assert_eq!(source_charge, base_source_charge + reader_index);
        assert_eq!(
            source_charge,
            base[0].bytes + index[0].bytes,
            "the retained source and its actual budget admissions have zero gap"
        );
        assert_eq!(
            reader.file_retained_charge_bytes_for_test(),
            Some(0),
            "read-only open never uses the writer retention door"
        );
        assert_eq!(
            reader
                .execute(
                    "SELECT id, body FROM cache_charge_rows ORDER BY id",
                    &std::collections::HashMap::new()
                )
                .unwrap()
                .rows,
            vec![vec![Value::Int64(1), Value::Text("held exactly".into())]]
        );
        assert_eq!(
            reader.file_cache_index_charge_for_test(),
            Some((reader_index, source_charge))
        );
        drop(reader);
    }
}
