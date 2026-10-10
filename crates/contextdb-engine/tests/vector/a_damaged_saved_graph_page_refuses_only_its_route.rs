//! A saved graph's committed page set is bound by one final root published in
//! its generation metadata. A load verifies that root once, under admitted
//! memory, before the route answers. Any damage to the route's pages -- a
//! flipped byte, a page the find never visits, a well-formed page swapped in
//! under the same key, another route's page, an extra page or an oversized
//! one -- refuses that route and no other, and the reader still opens.

use contextdb_core::read_contract::ReadLimits;
use contextdb_core::{Error, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{
    Database, MaintenancePolicy, PagedRouteFaultForTest, ReadSession, ReadSessionOptions,
};
use contextdb_vector::VectorPartitionRef;
use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

const ROWS_PER_ROUTE: u128 = 32;
const MAX_CYCLES: usize = 32;
const CORRUPT_PAGE: &str = "vector partition graph physical page is corrupt";

fn alpha() -> Uuid {
    Uuid::from_u128(0xA218)
}

fn bravo() -> Uuid {
    Uuid::from_u128(0xB218)
}

fn route(scope: Uuid) -> VectorPartitionRef {
    VectorPartitionRef::new(
        VectorIndexRef::new("paged_items", "embedding"),
        VectorPartitionKey::from_values(&[Value::Uuid(scope)]).expect("one UUID key"),
    )
}

fn ranked(axis: usize, ordinal: u128) -> Vec<f32> {
    let score = 1.0 - ordinal as f32 * 0.01;
    let remainder = (1.0 - score * score).max(0.0).sqrt();
    let mut vector = vec![0.0; 3];
    vector[axis] = score;
    vector[(axis + 1) % 3] = remainder;
    vector
}

fn axis(axis: usize) -> Vec<f32> {
    let mut vector = vec![0.0; 3];
    vector[axis] = 1.0;
    vector
}

fn graph_ready(db: &Database, scope: Uuid) -> bool {
    let mut owner = contextdb_engine::vector_read_memory::admission(
        db.declared_reader_memory()
            .expect("admit the partition observation reader"),
    );
    let partition = route(scope);
    db.vector_store_for_test()
        .partition_info(&partition.index, &partition.partition_key, &mut owner)
        .unwrap()
        .is_some_and(|info| info.graph_available)
}

fn seed(path: &std::path::Path) {
    let db = Database::open(path).expect("open the durable two-route store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE paged_items (\
         id UUID PRIMARY KEY, \
         scope_id UUID NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 SEARCH_MODE INDEXED\
         )",
        &HashMap::new(),
    )
    .expect("declare the partitioned vector table");
    for (scope, first, axis) in [(alpha(), 0xA000, 0), (bravo(), 0xB000, 1)] {
        let tx = db.begin_or_panic();
        for ordinal in 0..ROWS_PER_ROUTE {
            db.insert_row(
                tx,
                "paged_items",
                HashMap::from([
                    ("id".to_owned(), Value::Uuid(Uuid::from_u128(first + ordinal))),
                    ("scope_id".to_owned(), Value::Uuid(scope)),
                    ("embedding".to_owned(), Value::Vector(ranked(axis, ordinal))),
                ]),
            )
            .expect("stage one route vector");
        }
        db.commit(tx).expect("commit one route");
    }
    for _ in 0..MAX_CYCLES {
        if graph_ready(&db, alpha()) && graph_ready(&db, bravo()) {
            break;
        }
        db.run_maintenance_cycle().expect("one maintenance cycle");
    }
    assert!(
        graph_ready(&db, alpha()) && graph_ready(&db, bravo()),
        "both routes publish a saved graph"
    );
    db.close().expect("close the sealed store");
}

fn reader(path: &std::path::Path, memory: Option<u64>) -> ReadSession {
    let mut options = ReadSessionOptions::default();
    if let Some(memory) = memory {
        options.limits = ReadLimits {
            memory,
            ..options.limits
        };
    }
    ReadSession::open_with_options(path, options).expect("the reader opens over any page damage")
}

fn find(reader: &ReadSession, scope: Uuid, query: Vec<f32>) -> contextdb_core::Result<Vec<Uuid>> {
    reader
        .execute(
            "SELECT id FROM paged_items WHERE scope_id = $scope \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 4",
            &HashMap::from([
                ("scope".to_owned(), Value::Uuid(scope)),
                ("query".to_owned(), Value::Vector(query)),
            ]),
        )
        .map(|result| {
            result
                .rows
                .into_iter()
                .map(|row| match row.as_slice() {
                    [Value::Uuid(id)] => *id,
                    other => panic!("one id column: {other:?}"),
                })
                .collect()
        })
}

fn bravo_answers(reader: &ReadSession, what: &str) {
    assert_eq!(
        find(reader, bravo(), axis(1))
            .unwrap_or_else(|error| panic!("{what}: the healthy route answers: {error:?}"))
            .first(),
        Some(&Uuid::from_u128(0xB000)),
        "{what}: the healthy route answers its nearest row"
    );
}

fn is_memory_refusal(error: &Error) -> bool {
    let text = format!("{error:?}");
    text.contains("MemoryBudgetExceeded")
        || (text.contains("OwnerLimitExceeded") && text.contains("Memory"))
}

#[test]
fn every_kind_of_page_damage_refuses_only_the_damaged_route() {
    for fault in [
        PagedRouteFaultForTest::FlipFirstPointPage,
        PagedRouteFaultForTest::FlipLastPage,
        PagedRouteFaultForTest::ResealFirstPointPage,
        PagedRouteFaultForTest::CrossRouteFirstPointPage,
        PagedRouteFaultForTest::ExtraPage,
        PagedRouteFaultForTest::OversizedPage,
    ] {
        let root = TempDir::new().expect("temporary durable store directory");
        let path = root.path().join("paged-damage.redb");
        seed(&path);
        Database::__damage_paged_route_pages_for_test(
            &path,
            &route(alpha()),
            Some(&route(bravo())),
            fault,
        )
        .unwrap_or_else(|error| panic!("{fault:?}: damage alpha's committed pages: {error:?}"));

        let session = reader(&path, None);
        let before = Database::__paged_route_verifications_for_test();
        let damaged = find(&session, alpha(), axis(0));
        assert!(
            matches!(
                &damaged,
                Err(Error::StoreCorrupted { reason, .. }) if reason == CORRUPT_PAGE
            ),
            "{fault:?}: the damaged route is refused as corrupt, got {damaged:?}"
        );
        let after = Database::__paged_route_verifications_for_test();
        if matches!(
            fault,
            PagedRouteFaultForTest::ExtraPage | PagedRouteFaultForTest::OversizedPage
        ) {
            assert_eq!(
                after.0, before.0,
                "{fault:?}: an extra or oversized page is refused before the route is hashed"
            );
        }
        bravo_answers(&session, &format!("{fault:?}"));
        assert!(
            matches!(
                find(&session, alpha(), axis(0)),
                Err(Error::StoreCorrupted { ref reason, .. }) if reason == CORRUPT_PAGE
            ),
            "{fault:?}: a repeated find never serves the damaged route"
        );
    }
}

#[test]
fn a_healthy_route_is_verified_once_per_load_with_its_actual_page_set() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("paged-verify-once.redb");
    seed(&path);

    // The first find verifies alpha's committed page set once, walking its
    // actual pages. A warm repeat and a reload over the unchanged store reuse
    // that proof and hash nothing.
    let first = reader(&path, None);
    let start = Database::__paged_route_verifications_for_test();
    assert_eq!(
        find(&first, alpha(), axis(0)).expect("alpha answers").first(),
        Some(&Uuid::from_u128(0xA000))
    );
    let loaded = Database::__paged_route_verifications_for_test();
    assert_eq!(loaded.0 - start.0, 1, "the first find verifies alpha once");
    assert!(
        loaded.1 - start.1 > 0 && loaded.2 - start.2 > 0,
        "the verification walked alpha's actual pages: {start:?} -> {loaded:?}"
    );
    find(&first, alpha(), axis(0)).expect("a warm repeat answers");
    find(&reader(&path, None), alpha(), axis(0)).expect("a reload answers");
    assert_eq!(
        Database::__paged_route_verifications_for_test(),
        loaded,
        "an unchanged page set is not hashed again"
    );

    // Damage after the proof moves the page table's root: the next load
    // verifies again and refuses, while bravo keeps answering.
    Database::__damage_paged_route_pages_for_test(
        &path,
        &route(alpha()),
        None,
        PagedRouteFaultForTest::FlipLastPage,
    )
    .expect("damage alpha after its proof");
    let after_damage = reader(&path, None);
    assert!(
        matches!(
            find(&after_damage, alpha(), axis(0)),
            Err(Error::StoreCorrupted { ref reason, .. }) if reason == CORRUPT_PAGE
        ),
        "a proof never outlives a change to the page table"
    );
    bravo_answers(&after_damage, "after damage past an earlier proof");
}

#[test]
fn a_reader_without_memory_for_the_verification_window_is_refused_before_loading() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("paged-verify-budget.redb");
    seed(&path);

    // The least statement memory with which the find answers.
    let answers = |memory: u64| find(&reader(&path, Some(memory)), alpha(), axis(0));
    let mut high = ReadLimits::default().memory;
    answers(high).expect("the default statement memory answers");
    let mut low = 1u64;
    while low < high {
        let middle = low + (high - low) / 2;
        if answers(middle).is_ok() {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    let least = high;
    assert!(least > 1, "a verified load needs statement memory");
    let near = find(&reader(&path, Some(least - 1)), alpha(), axis(0))
        .expect_err("one byte below the least memory refuses");
    assert!(
        is_memory_refusal(&near),
        "one byte below the least memory is a typed memory refusal and serves nothing, got {near:?}"
    );
    bravo_answers(&reader(&path, None), "after a refused load");
}

/// The page table is the only bound table whose mismatch is left to the
/// per-route roots. Damage to the schema metadata bound beside it still
/// refuses every paged route; it is never served as a page-only mismatch.
#[test]
fn schema_metadata_damage_is_not_treated_as_page_damage() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("paged-schema-damage.redb");
    seed(&path);
    {
        let database = redb::Database::create(&path).expect("open the raw store");
        let write = database.begin_write().expect("raw write");
        write
            .open_table(redb::TableDefinition::<&str, &[u8]>::new("meta"))
            .expect("the schema metadata table")
            .insert("zz-unbound-schema-damage", b"x".as_slice())
            .expect("damage the schema metadata without rebinding");
        write.commit().expect("commit the damage");
    }
    let outcome = ReadSession::open_with_options(&path, ReadSessionOptions::default())
        .and_then(|session| find(&session, bravo(), axis(1)));
    assert!(
        matches!(outcome, Err(Error::StoreCorrupted { .. })),
        "schema metadata damage refuses even the undamaged route, got {outcome:?}"
    );
}
