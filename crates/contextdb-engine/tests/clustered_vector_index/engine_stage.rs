//! SQL mutation and file-reader outcomes across the first build and replacement.
//! The ordinary SQL controls do not certify native journal or descriptor binding.
use super::support::*;
use contextdb_core::{Value, Wallclock};
use contextdb_engine::{Database, ReadRoute, ReadSession};

const ROWS: i64 = 64;
const DIM: usize = 8;
fn bind(id: i64, seed: u64) -> std::collections::HashMap<String, Value> {
    params([
        ("id", Value::Int64(id)),
        ("v", Value::Vector(unit_vector(seed, DIM))),
    ])
}
fn seed(db: &Database) {
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(8) SEARCH_MODE EXACT) HISTORY ALL",
        &empty(),
    )
    .unwrap();
    let tx = db.begin_or_panic();
    for id in 0..ROWS {
        db.execute_in_tx(
            tx,
            "INSERT INTO items VALUES ($id, $v)",
            &bind(id, id as u64),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
}
fn answer(db: &Database, mode: &str, seed: u64) -> Vec<i64> {
    int_ids(
        &db.execute(
            &format!("SELECT id FROM items ORDER BY v <=> $v USE VECTOR {mode} LIMIT 64"),
            &bind(0, seed),
        )
        .unwrap(),
    )
}
fn change(db: &Database) {
    db.execute("UPDATE items SET v = $v WHERE id = $id", &bind(7, 70_007))
        .unwrap();
    db.execute("DELETE FROM items WHERE id = 9", &empty())
        .unwrap();
    // Exercise the already-deleted path without turning it into a new insert.
    assert_eq!(
        db.execute("DELETE FROM items WHERE id = 9", &empty())
            .unwrap()
            .rows_affected(),
        0
    );
    let tx = db.begin_or_panic();
    db.execute_in_tx(
        tx,
        "UPDATE items SET v = $v WHERE id = $id",
        &bind(11, 70_011),
    )
    .unwrap();
    db.execute_in_tx(tx, "DELETE FROM items WHERE id = 13", &empty())
        .unwrap();
    db.rollback(tx).unwrap();
}
fn assert_current(db: &Database, mode: &str) {
    let ids = answer(db, mode, 70_007);
    assert_eq!(
        ids.first(),
        Some(&7),
        "the updated vector is the current version"
    );
    assert_eq!(ids.len(), (ROWS - 1) as usize);
    let mut unique = ids.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(
        unique,
        (0..ROWS).filter(|id| *id != 9).collect::<Vec<_>>(),
        "one result per surviving row; no resurrected deletion"
    );
    for id in [11, 13] {
        assert_eq!(
            answer(db, mode, id as u64).first(),
            Some(&id),
            "aborted mutations leave the old source and vector visible"
        );
    }
}
#[test]
fn canonical_mutations_and_rollback_survive_restart_control() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("canonical.db");
    let db = open_caller_driven(&path);
    seed(&db);
    change(&db);
    assert_current(&db, "EXACT");
    db.close().unwrap();
    drop(db);
    let db = open_caller_driven(&path);
    assert_current(&db, "EXACT");
    db.close().unwrap();
}
#[test]
fn canonical_file_reader_returns_updated_rows_and_excludes_deleted_rows_control() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("file-control.db");
    let db = open_caller_driven(&path);
    seed(&db);
    change(&db);
    let expected = answer(&db, "EXACT", 70_007);
    db.close().unwrap();
    drop(db);
    let reader = ReadSession::open(&path).unwrap();
    assert_eq!(reader.route(), ReadRoute::File);
    let found = reader
        .execute(
            "SELECT id FROM items ORDER BY v <=> $v USE VECTOR EXACT LIMIT 64",
            &bind(0, 70_007),
        )
        .unwrap();
    assert_eq!(int_ids(&found), expected);
    drop(reader);
}

fn index() -> contextdb_core::VectorIndexRef {
    contextdb_core::VectorIndexRef::new("items", "v")
}
fn tuple() -> contextdb_core::VectorPartitionKey {
    contextdb_core::VectorPartitionKey::unpartitioned()
}
fn assert_codebook_layout(a: ProducerCodebookLayouts, b: ProducerCodebookLayouts) {
    for (left, right) in [
        (a.control, b.control),
        (a.values, b.values),
        (a.counts, b.counts),
    ] {
        assert_eq!((left.size(), left.align()), (right.size(), right.align()));
    }
    assert_eq!(a.charged_bytes, b.charged_bytes);
    assert_eq!(
        a.charged_bytes,
        a.control.size() + a.values.size() + a.counts.size()
    );
}
fn assert_workspace_layout(a: ProducerWorkspaceLayouts, b: ProducerWorkspaceLayouts) {
    for (left, right) in [
        (a.controls, b.controls),
        (a.buffer, b.buffer),
        (a.raw, b.raw),
        (a.values, b.values),
        (a.encoded, b.encoded),
        (a.order, b.order),
        (a.combined, b.combined),
        (a.page_scratch, b.page_scratch),
        (a.catalog, b.catalog),
    ] {
        assert_eq!((left.size(), left.align()), (right.size(), right.align()));
    }
    assert_eq!(a.charged_bytes, b.charged_bytes);
    assert_eq!(a.buffer_record_bytes, b.buffer_record_bytes);
    assert_eq!(
        a.charged_bytes,
        a.combined.size() + a.page_scratch.size() + a.catalog.size()
    );
    let mut combined = a.controls;
    for part in [a.buffer, a.raw, a.values, a.encoded, a.order] {
        combined = combined.extend(part).unwrap().0;
    }
    assert_eq!(combined.pad_to_align(), a.combined);
}
fn assert_constructor(attempt: ConstructorAttempt, bytes: usize, admitted: bool) {
    assert!(!attempt.overflow);
    assert!(attempt.event_count <= attempt.events.len());
    assert_eq!(attempt.retained_after_drop, 0);
    let events = &attempt.events[..attempt.event_count];
    if admitted {
        attempt.outcome.unwrap();
        assert!(matches!(events.first(),Some(Some(ConstructorEvent::Debited(n))) if *n==bytes));
        assert!(matches!(events.last(),Some(Some(ConstructorEvent::Released(n))) if *n==bytes));
        let mut allocations = 0;
        let mut frees = 0;
        for event in &events[1..events.len() - 1] {
            match event {
                Some(ConstructorEvent::Allocated) => allocations += 1,
                Some(ConstructorEvent::PhysicallyFreed) => frees += 1,
                _ => panic!("one actual constructor invoice around its allocations"),
            }
        }
        assert!(allocations > 0);
        assert_eq!(allocations, frees);
        let first_free = events
            .iter()
            .position(|e| matches!(e, Some(ConstructorEvent::PhysicallyFreed)))
            .unwrap();
        assert!(
            !events[first_free..]
                .iter()
                .any(|e| matches!(e, Some(ConstructorEvent::Allocated)))
        );
    } else {
        assert!(matches!(
            attempt.outcome,
            Err(contextdb_core::Error::MemoryBudgetExceeded { .. })
        ));
        assert_eq!(
            attempt.event_count, 0,
            "refusal precedes debit and physical allocation"
        );
    }
}
#[test]
fn default_policy_resolution_matches_producer_checked_layout_and_constructor_debits() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    for (dimensions, population, expected_vectors, memory_limit) in [
        (1, 0, 1, 8 * 1024 * 1024),
        (7, 17, 4096, 32 * 1024 * 1024),
        (129, 65, 1_000_000, 8 * 1024 * 1024),
    ] {
        let root = tempfile::tempdir().unwrap();
        let db = Database::open_with_options(
            root.path().join("layouts.db"),
            contextdb_engine::DatabaseOpenOptions {
                memory_limit: Some(memory_limit),
                maintenance_policy: contextdb_engine::MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        db.execute(&format!("CREATE TABLE items (id INT PRIMARY KEY, v VECTOR({dimensions}) WITH (write_buffer_bytes = DEFAULT) SEARCH_MODE INDEXED SEARCH (EXPECTED_VECTORS = {expected_vectors}) IVF (LISTS = DEFAULT))"),&empty()).unwrap();
        for id in 0..population {
            db.execute(
                "INSERT INTO items VALUES ($id,$v)",
                &params([
                    ("id", Value::Int64(id)),
                    ("v", Value::Vector(unit_vector(id as u64, dimensions))),
                ]),
            )
            .unwrap();
        }
        let resolved = native_ivf_resolved_policy_for_test(&db, &index(), &tuple()).unwrap();
        assert_eq!(
            (
                resolved.inputs.dimensions,
                resolved.population,
                resolved.expected_vectors
            ),
            (dimensions, population as u64, expected_vectors)
        );
        assert_eq!(
            resolved.codebook_budget_bytes,
            resolved.build_budget_bytes / 16
        );
        let desired = 64_u64.max((resolved.population.max(expected_vectors)).div_ceil(1000));
        let mut low = 0_u32;
        let mut high = u32::try_from(desired).unwrap();
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            let fits = codebook_allocation_layout(dimensions, mid.min(population as u32), true)
                .is_ok_and(|layout| layout.charged_bytes as u64 <= resolved.codebook_budget_bytes);
            if fits {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        assert!(low > 0);
        assert_eq!(resolved.inputs.lists, low);
        assert_eq!(resolved.active_lists, low.min(population as u32));
        let producer_book =
            codebook_allocation_layout(dimensions, resolved.active_lists, true).unwrap();
        assert_codebook_layout(resolved.codebook, producer_book);
        let producer_workspace = workspace_allocation_layout(resolved.inputs).unwrap();
        assert_workspace_layout(resolved.workspace, producer_workspace);
        let record = producer_workspace.buffer_record_bytes;
        assert_eq!(record, dimensions * 4 + 40);
        assert_eq!(resolved.inputs.buffer_bytes % record, 0);
        assert!(producer_workspace.charged_bytes <= resolved.workspace_budget_bytes);
        let population_cap = resolved.population.max(expected_vectors).max(1) as usize;
        let records = resolved.inputs.buffer_bytes / record;
        assert!(records <= population_cap);
        if records < population_cap {
            let mut one_more = resolved.inputs;
            one_more.buffer_bytes = one_more.buffer_bytes.checked_add(record).unwrap();
            assert!(
                workspace_allocation_layout(one_more).map_or(true, |layout| layout.charged_bytes
                    > resolved.workspace_budget_bytes),
                "DEFAULT is the smaller of the population cap and the producer-checked complete-record capacity of the actual remaining workspace"
            );
        }
        for training_counts in [false, true] {
            let layout =
                codebook_allocation_layout(dimensions, resolved.active_lists, training_counts)
                    .unwrap();
            if resolved.active_lists == 0 {
                assert_eq!(population, 0);
                assert_eq!(layout.charged_bytes, 0);
                let attempt = native_ivf_constructor_attempt_for_test(
                    &db,
                    &index(),
                    &tuple(),
                    ConstructorAllocation::Codebook { training_counts },
                    0,
                );
                assert!(!attempt.overflow);
                attempt.outcome.unwrap();
                assert_eq!(attempt.event_count, 0, "empty codebook has no reservation");
                assert_eq!(attempt.retained_after_drop, 0);
                continue;
            }
            assert!(layout.charged_bytes > 0);
            if !training_counts {
                assert_eq!(layout.counts.size(), 0);
            }
            for admitted in [true, false] {
                assert_constructor(
                    native_ivf_constructor_attempt_for_test(
                        &db,
                        &index(),
                        &tuple(),
                        ConstructorAllocation::Codebook { training_counts },
                        layout.charged_bytes - usize::from(!admitted),
                    ),
                    layout.charged_bytes,
                    admitted,
                );
            }
        }
        for admitted in [true, false] {
            assert_constructor(
                native_ivf_constructor_attempt_for_test(
                    &db,
                    &index(),
                    &tuple(),
                    ConstructorAllocation::WriterWorkspace,
                    producer_workspace.charged_bytes - usize::from(!admitted),
                ),
                producer_workspace.charged_bytes,
                admitted,
            );
        }
        let error = db
            .execute(
                "ALTER TABLE items ALTER COLUMN v SET IVF (LISTS = 4294967295)",
                &empty(),
            )
            .unwrap_err();
        assert!(
            matches!(error, contextdb_core::Error::MemoryBudgetExceeded { .. }),
            "explicit over-budget LISTS refuses, never clamps: {error:?}"
        );
        assert_eq!(
            native_ivf_resolved_policy_for_test(&db, &index(), &tuple())
                .unwrap()
                .inputs
                .lists,
            resolved.inputs.lists
        );
        let contradictory = db
            .execute(
                "ALTER TABLE items ALTER COLUMN v SET IVF (LISTS = 4, PROBES = 8)",
                &empty(),
            )
            .unwrap_err();
        assert!(
            matches!(
                contradictory,
                contextdb_core::Error::InvalidVectorPolicyDeclaration { .. }
            ),
            "explicit LISTS below explicit PROBES is refused at the declaration: {contradictory:?}"
        );
        db.execute(
            "ALTER TABLE items ALTER COLUMN v SET IVF (PROBES = 512)",
            &empty(),
        )
        .unwrap();
        let partial = native_ivf_resolved_policy_for_test(&db, &index(), &tuple()).unwrap();
        assert_eq!(
            partial.inputs.probes, 512,
            "an explicit PROBES is never clamped"
        );
        assert!(
            partial.inputs.lists >= 512,
            "an omitted LISTS resolves compatibly with an explicit PROBES: {}",
            partial.inputs.lists
        );
    }
    assert!(matches!(
        codebook_allocation_layout(usize::MAX, u32::MAX, true),
        Err(contextdb_core::Error::MemoryBudgetExceeded { .. })
    ));
}
