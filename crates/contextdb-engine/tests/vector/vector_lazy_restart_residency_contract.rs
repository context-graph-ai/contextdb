//! Restart working-set contract for durable partitioned vector indexes.
//!
//! A reopened database is allowed to know which saved routes it has.  It is
//! not allowed to decode every saved graph just because the database opened.
//! This fixture uses the production file-backed open and SQL search doors; the
//! test seams passively observe graph and raw-body residency, then prove that
//! pressure evicts only the least-recently-used route needed to admit another.

use contextdb_core::{Error, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

const INDEXED_ROWS_PER_PARTITION: usize = 1_000;
const VECTOR_DIMENSION: usize = 512;
const MAX_MAINTENANCE_CYCLES: usize = 64;
// Admit both encoded record owners, including the record envelope. The raw
// partition control below must still exceed this same fixed working-set cap.
const LOAD_WORKSPACE_HEADROOM: usize = 2 * 1024 * 1024 + 64 * 1024;
const SQ4_STORED_BYTES_PER_ENTRY: usize = VECTOR_DIMENSION.div_ceil(2) + 8;
const DECODED_F32_BYTES_PER_ENTRY: usize = VECTOR_DIMENSION * std::mem::size_of::<f32>();
const BULK_RAW_LOAD_RESERVATION: usize = INDEXED_ROWS_PER_PARTITION
    * (SQ4_STORED_BYTES_PER_ENTRY + DECODED_F32_BYTES_PER_ENTRY)
    + SQ4_STORED_BYTES_PER_ENTRY
    + DECODED_F32_BYTES_PER_ENTRY;
const _: () = assert!(BULK_RAW_LOAD_RESERVATION > LOAD_WORKSPACE_HEADROOM);

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("lazy_restart_items", "embedding")
}

fn partition(scope: Uuid) -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Uuid(scope)])
        .expect("a UUID partition component has one canonical key")
}

fn ranked_vector(rank: usize, leading_axis: usize) -> Vec<f32> {
    let score = (1.0 - rank as f32 * 0.0005).clamp(0.05, 1.0);
    let remainder = (1.0 - score * score).max(0.0).sqrt();
    let mut vector = axis(leading_axis);
    vector[leading_axis] = score;
    vector[1 - leading_axis] = remainder;
    vector
}

fn axis(leading_axis: usize) -> Vec<f32> {
    assert!(leading_axis < 2, "the fixture has two deterministic axes");
    let mut vector = vec![0.0; VECTOR_DIMENSION];
    vector[leading_axis] = 1.0;
    vector
}

fn ids(result: &QueryResult) -> Vec<Uuid> {
    let id_column = result
        .columns
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .expect("vector search projects id");
    result
        .rows
        .iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(id)) => *id,
            value => panic!("vector search returned a non-UUID id: {value:?}"),
        })
        .collect()
}

fn graph_status(
    db: &Database,
    index: &VectorIndexRef,
    scope: Uuid,
) -> contextdb_vector::store::VectorGraphGenerationStatus {
    db.vector_store_for_test()
        .partition_graph_generation_status(&contextdb_vector::VectorPartitionRef::new(
            index.clone(),
            partition(scope),
        ))
        .unwrap_or_else(|| panic!("the seeded partition {scope} retains generation status"))
}

fn graph_ready(db: &Database, index: &VectorIndexRef, scope: Uuid) -> bool {
    db.vector_store_for_test()
        .partition_info(index, &partition(scope))
        .is_some_and(|info| info.graph_available)
}

fn drive_maintenance_until_all_ready(db: &Database, scopes: &[Uuid]) {
    let index = index();
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        if scopes
            .iter()
            .copied()
            .all(|scope| graph_ready(db, &index, scope))
        {
            return;
        }
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns");
    }
    panic!(
        "caller-driven maintenance did not publish every seeded partition within \
         {MAX_MAINTENANCE_CYCLES} finite cycles"
    );
}

fn indexed_search(db: &Database, scope: Uuid, query: Vec<f32>) -> Vec<Uuid> {
    ids(&db
        .execute(
            "SELECT id FROM lazy_restart_items WHERE scope_id = $scope \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
            &params([
                ("scope", Value::Uuid(scope)),
                ("query", Value::Vector(query)),
            ]),
        )
        .unwrap_or_else(|error| panic!("indexed search for selected partition: {error}")))
}

fn create_table(db: &Database) {
    db.execute(
        &format!(
            "CREATE TABLE lazy_restart_items (\
             id UUID PRIMARY KEY, \
             scope_id UUID NOT NULL, \
             embedding VECTOR({VECTOR_DIMENSION}) WITH (quantization = 'SQ4') \
             PARTITION_KEY (scope_id) MAX_PARTITIONS 4 \
             SEARCH_MODE INDEXED\
             )"
        ),
        &empty(),
    )
    .expect("create the partitioned lazy-restart fixture");
}

fn seed_partition(db: &Database, scope: Uuid, axis: usize, first_id: u128) {
    let tx = db.begin_or_panic();
    for rank in 0..INDEXED_ROWS_PER_PARTITION {
        db.insert_row(
            tx,
            "lazy_restart_items",
            params([
                ("id", Value::Uuid(Uuid::from_u128(first_id + rank as u128))),
                ("scope_id", Value::Uuid(scope)),
                ("embedding", Value::Vector(ranked_vector(rank, axis))),
            ]),
        )
        .expect("stage one deterministic vector row");
    }
    db.commit(tx).expect("commit one complete vector partition");
}

#[test]
fn reopen_keeps_saved_partitions_dormant_then_reclaims_one_least_recently_used_route_under_pressure()
 {
    let directory = TempDir::new().expect("temporary store directory");
    let path = directory.path().join("lazy-restart-residency.db");
    let alpha = Uuid::from_u128(0xa11);
    let bravo = Uuid::from_u128(0xb22);
    let charlie = Uuid::from_u128(0xc33);
    let scopes = [alpha, bravo, charlie];
    let index = index();

    {
        let db = Database::open(&path).expect("open file-backed seed store");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        create_table(&db);
        seed_partition(&db, alpha, 0, 0x100_000);
        seed_partition(&db, bravo, 1, 0x200_000);
        seed_partition(&db, charlie, 0, 0x300_000);
        drive_maintenance_until_all_ready(&db, &scopes);
        db.close().expect("close the fully sealed fixture");
    }

    let db = Database::open(&path).expect("reopen the durable partitioned index");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let opened_charge = db.accountant().usage().used;

    for scope in scopes {
        let status = graph_status(&db, &index, scope);
        assert!(
            status.base.is_none() && status.dormant_base.is_some(),
            "reopen may retain the saved route descriptor for {scope}, but must not eagerly decode its base graph: {status:?}"
        );
        assert!(
            !status.base_resident && !status.change_resident,
            "no saved graph for idle partition {scope} may be resident immediately after reopen: {status:?}"
        );
        assert_eq!(
            status.fresh_tail_entries, 0,
            "opening idle partition {scope} must not replay a tail before that partition is selected"
        );
    }

    let activity_before_alpha = db.__vector_passive_activity_counters_for_test();
    db.__reset_last_query_vector_trace_for_test();
    assert_eq!(
        indexed_search(&db, alpha, axis(0)),
        vec![Uuid::from_u128(0x100_000)],
        "the production SQL route selects alpha through its maintained index"
    );
    let alpha_trace = db
        .__take_last_query_vector_trace_for_test()
        .expect("the selected indexed route publishes its trace");
    assert!(
        alpha_trace.used_hnsw,
        "the selected answer stays on the maintained indexed route"
    );
    let activity_after_alpha = db.__vector_passive_activity_counters_for_test();
    let selected_charge = db.accountant().usage().used;
    assert!(
        selected_charge > opened_charge,
        "selecting alpha must account for its newly resident saved graph: open {opened_charge}, \
         selected {selected_charge}"
    );
    let alpha_status = graph_status(&db, &index, alpha);
    let bravo_status = graph_status(&db, &index, bravo);
    assert!(
        alpha_status.base_resident,
        "the selected partition becomes resident after its SQL indexed search: {alpha_status:?}"
    );
    assert!(
        !bravo_status.base_resident && bravo_status.dormant_base.is_some(),
        "selecting alpha must not load bravo's saved graph: {bravo_status:?}"
    );
    assert!(
        !db.vector_store_for_test()
            .raw_partition_resident_for_test(&index, &partition(alpha)),
        "the selected ordinary-version graph route must not materialize alpha's raw body"
    );
    assert!(
        !db.vector_store_for_test()
            .raw_partition_resident_for_test(&index, &partition(bravo)),
        "selecting alpha must not materialize bravo's raw body"
    );
    assert_eq!(
        activity_after_alpha.raw_partition_loads, activity_before_alpha.raw_partition_loads,
        "the indexed route must not disguise candidate scoring as a complete raw-partition load"
    );
    assert_eq!(
        activity_after_alpha.raw_candidate_point_loads
            - activity_before_alpha.raw_candidate_point_loads,
        alpha_trace
            .hnsw_candidate_row_ids
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len() as u64,
        "native scoring point-loads each distinct candidate without loading the raw partition"
    );

    assert_eq!(
        indexed_search(&db, bravo, axis(1)),
        vec![Uuid::from_u128(0x200_000)],
        "with memory available, selecting bravo does not evict alpha"
    );
    let two_route_charge = db.accountant().usage().used;
    assert!(
        two_route_charge > selected_charge,
        "warming bravo retains a second measured graph charge"
    );
    let activity_after_warmup = db.__vector_passive_activity_counters_for_test();
    assert_eq!(
        activity_after_warmup.sealed_generation_loads
            - activity_before_alpha.sealed_generation_loads,
        2,
        "the first alternating round loads each dormant saved graph exactly once"
    );
    for (scope, query, expected) in [
        (alpha, axis(0), Uuid::from_u128(0x100_000)),
        (bravo, axis(1), Uuid::from_u128(0x200_000)),
    ] {
        assert_eq!(indexed_search(&db, scope, query), vec![expected]);
    }
    let activity_after_second_round = db.__vector_passive_activity_counters_for_test();
    assert_eq!(
        activity_after_second_round.sealed_generation_loads,
        activity_after_warmup.sealed_generation_loads,
        "alternating over a fitting working set performs zero repeat graph loads"
    );
    for scope in scopes {
        assert_eq!(
            graph_status(&db, &index, scope).base_resident,
            scope != charlie,
            "the two searched partitions stay warm while unselected charlie stays dormant"
        );
    }

    assert_eq!(
        indexed_search(&db, alpha, axis(0)),
        vec![Uuid::from_u128(0x100_000)],
        "touch alpha after bravo so bravo is the least-recently-used resident route"
    );

    let load_limit = two_route_charge
        .checked_add(LOAD_WORKSPACE_HEADROOM)
        .expect("the fixed fixture's decode headroom fits usize");
    db.set_memory_limit(Some(load_limit)).expect(
        "the declared limit admits two resident partitions plus temporary decode workspace",
    );
    let activity_before_pressure = db.__vector_passive_activity_counters_for_test();
    db.__reset_last_query_vector_trace_for_test();
    assert_eq!(
        indexed_search(&db, charlie, axis(0)),
        vec![Uuid::from_u128(0x300_000)],
        "pressure reclaims the least-recently-used bravo graph and admits charlie"
    );
    let pressure_trace = db
        .__take_last_query_vector_trace_for_test()
        .expect("the selected indexed route publishes its trace");
    assert!(
        pressure_trace.used_hnsw,
        "the bounded answer stays on the maintained indexed route"
    );
    assert_eq!(
        db.accountant().usage().used,
        two_route_charge,
        "loading charlie returns temporary decode bytes and retains exactly two measured routes"
    );
    let alpha_status = graph_status(&db, &index, alpha);
    let bravo_status = graph_status(&db, &index, bravo);
    let charlie_status = graph_status(&db, &index, charlie);
    assert!(
        alpha_status.base_resident,
        "recently used alpha stays resident under pressure: {alpha_status:?}"
    );
    assert!(
        !bravo_status.base_resident && bravo_status.dormant_base.is_some(),
        "the idle bravo route is evicted only when charlie's admission needs its space: {bravo_status:?}"
    );
    assert!(
        charlie_status.base_resident,
        "the pressure-selected charlie route becomes resident: {charlie_status:?}"
    );
    let activity_after_pressure = db.__vector_passive_activity_counters_for_test();
    assert_eq!(
        activity_after_pressure.sealed_generation_evictions
            - activity_before_pressure.sealed_generation_evictions,
        1,
        "one graph chain supplies the full load shortfall; pressure does not sweep the store"
    );
    assert_eq!(
        activity_after_pressure.raw_partition_loads, activity_before_alpha.raw_partition_loads,
        "both indexed searches leave complete raw partitions dormant"
    );
    assert_eq!(
        activity_after_pressure.raw_candidate_point_loads
            - activity_before_pressure.raw_candidate_point_loads,
        pressure_trace
            .hnsw_candidate_row_ids
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len() as u64,
        "native scoring point-loads each distinct candidate without loading the raw partition"
    );
    assert!(
        !db.vector_store_for_test()
            .raw_partition_resident_for_test(&index, &partition(alpha))
            && !db
                .vector_store_for_test()
                .raw_partition_resident_for_test(&index, &partition(bravo))
            && !db
                .vector_store_for_test()
                .raw_partition_resident_for_test(&index, &partition(charlie)),
        "indexed pressure leaves complete raw bodies dormant"
    );

    let bulk_error = db
        .execute(
            "SELECT id FROM lazy_restart_items WHERE scope_id = $scope \
             ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1",
            &params([
                ("scope", Value::Uuid(alpha)),
                ("query", Value::Vector(axis(0))),
            ]),
        )
        .expect_err("the same memory ceiling must refuse a complete raw-partition load");
    assert!(
        matches!(
            bulk_error,
            Error::MemoryBudgetExceeded { operation, .. } if operation == "load_raw_vector_partition"
        ),
        "the raw-load control must fail at bulk raw-vector admission"
    );
    let activity_after_bulk_refusal = db.__vector_passive_activity_counters_for_test();
    assert_eq!(
        activity_after_bulk_refusal.raw_partition_loads,
        activity_after_pressure.raw_partition_loads,
        "the refused bulk control must not install a raw partition"
    );
    assert_eq!(
        db.accountant().usage().used,
        two_route_charge,
        "the selected graph working set remains inside the same memory ceiling after the refused bulk control"
    );
}
