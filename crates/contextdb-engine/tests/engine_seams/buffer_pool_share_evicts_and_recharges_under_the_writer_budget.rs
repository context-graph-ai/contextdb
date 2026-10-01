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
    exec(&db, "CREATE TABLE pages (id INT PRIMARY KEY, body TEXT)");
    exec(&db, "CREATE INDEX pages_id ON pages (id)");
    exec(
        &db,
        "CREATE TABLE memberships (id INT PRIMARY KEY, v VECTOR(16)) HISTORY ALL",
    );
    for id in 0..256 {
        db.execute(
            "INSERT INTO pages VALUES ($id,$body)",
            &std::collections::HashMap::from([
                ("id".into(), Value::Int64(id)),
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
    let receipt = db.writer_memory_ownership_receipt_for_test();
    assert_eq!(receipt.reconciliation_gap, 0);
    assert!(receipt.named_owner_total <= LIMIT);
    assert_eq!(
        receipt.persistence_cache,
        redb_writer_cache_bytes_for_limit_for_test(Some(LIMIT)),
        "one named cache owner"
    );
    assert!(
        during.3 <= receipt.persistence_cache,
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
        saturated.3 <= receipt.persistence_cache,
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
        "INSERT INTO pages VALUES (1000,'must not publish')",
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
            .max((LIMIT / 16).min(256 * 1024)),
        "declared bounded writer share"
    );
    db.close().unwrap();
}
