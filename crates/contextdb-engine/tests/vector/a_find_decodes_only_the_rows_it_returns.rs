//! A filtered partitioned vector search must decode whole stored row versions
//! only for the rows it actually returns.
//!
//! The rows a semantic search is asked for are the ten nearest. Deciding which
//! rows are in scope is index work — partition key plus the indexed residual
//! columns — and the graph decides which of them are near. A read that instead
//! decodes one whole row version per in-scope vector pays for every vector body
//! in the authorized set to publish ten of them, which is the cost the
//! maintained index exists to avoid, and it grows with the scope rather than
//! with the answer.

use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    contradict_indexed_column_for_test, misfile_relational_index_posting_for_test,
};
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

const PARTITIONS: usize = 8;
const ROWS_PER_PARTITION: usize = 400;
const DIMENSIONS: usize = 16;
const LIMIT: usize = 10;
/// A row planted in the first Context whose embedding is the query vector
/// itself, so it is the nearest row in its partition and heads every answer.
/// Seeded before maintenance builds the graphs, so it is a real searchable
/// member of its partition rather than a row the graph has never seen.
const PLANTED_ROW_ID: i64 = (PARTITIONS * ROWS_PER_PARTITION) as i64 + 1;

struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn limits() -> ReadLimits {
    ReadLimits {
        work: 5_000_000,
        memory: 512 * 1024 * 1024,
        ..ReadLimits::default()
    }
}

#[derive(Default)]
struct TouchCounter(Mutex<BTreeMap<bounded::TestSourceTouch, u64>>);

impl bounded::ExecutionProbe for TouchCounter {
    fn before_work(&self, _: bounded::TestWorkSource, _: u64) {}

    fn before_source_touch(&self, touch: bounded::TestSourceTouch, _: u64) {
        *self
            .0
            .lock()
            .expect("the touch counter is never poisoned")
            .entry(touch)
            .or_default() += 1;
    }

    fn cancellation_observed(&self, _: u64) {}
}

fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC048_0000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn generation_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC048_1000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn embedding(ordinal: usize) -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|axis| {
            let seed = (ordinal
                .wrapping_mul(0x9e37_79b9)
                .wrapping_add(axis.wrapping_mul(0x85eb_ca6b)))
                % 10_007;
            (seed as f32 / 10_007.0) - 0.5
        })
        .collect()
}

fn query_vector(probe: usize) -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|axis| {
            let seed = (probe
                .wrapping_mul(0xc2b2_ae35)
                .wrapping_add(axis.wrapping_mul(0x27d4_eb2f)))
                % 10_009;
            (seed as f32 / 10_009.0) - 0.5
        })
        .collect()
}

fn fixture(path: &std::path::Path) -> Database {
    // File-backed on purpose: the decode counters live in the persistence
    // layer, so an in-memory store cannot show what a real read decodes.
    fixture_in(Database::open(path).expect("open the fixture store"), false)
}

fn fixture_in(db: Database, plant_a_non_current_row: bool) -> Database {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE scoped_rows (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
         generation_id UUID NOT NULL, is_current BOOL NOT NULL, body TEXT NOT NULL, \
         embedding VECTOR(16) PARTITION_KEY (context_id, generation_id) \
         MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
        &HashMap::new(),
    )
    .expect("declare the partitioned fixture");
    db.execute(
        "CREATE INDEX scoped_rows_active_idx ON scoped_rows \
         (context_id, generation_id, is_current)",
        &HashMap::new(),
    )
    .expect("declare the index the residual filter is served from");
    for partition in 0..PARTITIONS {
        for local in 0..ROWS_PER_PARTITION {
            let ordinal = partition * ROWS_PER_PARTITION + local;
            db.execute(
                "INSERT INTO scoped_rows VALUES \
                 ($id, $context, $generation, TRUE, $body, $embedding)",
                &HashMap::from([
                    ("id".into(), Value::Int64(ordinal as i64 + 1)),
                    ("context".into(), Value::Uuid(context_id(partition))),
                    ("generation".into(), Value::Uuid(generation_id(partition))),
                    // A body wide enough that decoding one row version is
                    // visible in the stored-byte count.
                    ("body".into(), Value::Text("p".repeat(512))),
                    ("embedding".into(), Value::Vector(embedding(ordinal))),
                ]),
            )
            .expect("seed one partitioned row");
        }
    }
    if plant_a_non_current_row {
        db.execute(
            "INSERT INTO scoped_rows VALUES ($id, $context, $generation, TRUE, $body, $embedding)",
            &HashMap::from([
                ("id".into(), Value::Int64(PLANTED_ROW_ID)),
                ("context".into(), Value::Uuid(context_id(0))),
                ("generation".into(), Value::Uuid(generation_id(0))),
                ("body".into(), Value::Text("p".repeat(512))),
                ("embedding".into(), Value::Vector(query_vector(0))),
            ]),
        )
        .expect("plant the nearest row before the graphs are built");
    }
    for _ in 0..512 {
        db.run_maintenance_cycle()
            .expect("advance deterministic maintenance");
        let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
            db.declared_reader_memory()
                .expect("admit the partition observation reader"),
        );

        let ready = db
            .vector_store_for_test()
            .partition_infos(
                &VectorIndexRef::new("scoped_rows", "embedding"),
                &mut partition_observation_owner_1,
            )
            .expect("inspect partition readiness");
        if ready.len() == PARTITIONS && ready.iter().all(|partition| partition.graph_available) {
            return db;
        }
    }
    panic!("every partition must publish a maintained route before the read is measured");
}

struct Measured {
    rows: usize,
    returned: Vec<i64>,
    access_rows: u64,
    plan: String,
    decoded_rows: u64,
    stored_row_bytes: u64,
    verified_runs_built: u64,
    verified_postings_walked: u64,
    hashed_bytes: u64,
    rows_examined: u64,
}

fn measure(db: &Database, partitions: usize) -> Measured {
    measure_contexts(db, &(0..partitions).collect::<Vec<_>>())
}

/// The statement this proof measures, over exactly the Contexts named.
fn statement(selected: &[usize]) -> (String, HashMap<String, Value>) {
    let partitions = selected.len();
    let mut params = HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(query_vector(0))),
    ]);
    let mut contexts = Vec::with_capacity(partitions);
    let mut generations = Vec::with_capacity(partitions);
    for &partition in selected {
        let context = format!("context_{partition}");
        let generation = format!("generation_{partition}");
        params.insert(context.clone(), Value::Uuid(context_id(partition)));
        params.insert(generation.clone(), Value::Uuid(generation_id(partition)));
        contexts.push(format!("${context}"));
        generations.push(format!("${generation}"));
    }
    let sql = if selected.is_empty() {
        format!(
            "SELECT id FROM scoped_rows WHERE is_current = $is_current \
             ORDER BY embedding <=> $query LIMIT {LIMIT}"
        )
    } else {
        format!(
            "SELECT id FROM scoped_rows WHERE context_id IN ({}) AND generation_id IN ({}) \
             AND is_current = $is_current ORDER BY embedding <=> $query LIMIT {LIMIT}",
            contexts.join(", "),
            generations.join(", "),
        )
    };
    (sql, params)
}

/// The same read over exactly the Contexts named, or over none at all.
fn measure_contexts(db: &Database, selected: &[usize]) -> Measured {
    try_measure_contexts(db, selected).expect("the bounded read succeeds")
}

/// The same read, handing back a refusal instead of panicking on one: damage
/// is reported through the read's own error, so a proof about damage has to be
/// able to read it.
fn try_measure_contexts(db: &Database, selected: &[usize]) -> Result<Measured, String> {
    let (sql, params) = statement(selected);
    let counter = Arc::new(TouchCounter::default());
    let mut request =
        bounded::BoundedReadRequest::new(sql, params, limits(), Arc::new(FrozenClock));
    request.probe = Some(counter.clone());
    let work = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let executed = bounded::execute(db, &request).map_err(|error| format!("{error:?}"))?;
    let counters = work.counters();
    drop(work);
    let touches = counter
        .0
        .lock()
        .expect("the touch counter is never poisoned")
        .clone();
    Ok(Measured {
        rows: executed.result.rows.len(),
        returned: executed
            .result
            .rows
            .iter()
            .map(|row| match row.first() {
                Some(Value::Int64(id)) => *id,
                other => panic!("the projection returns one integer id, got {other:?}"),
            })
            .collect(),
        access_rows: touches
            .get(&bounded::TestSourceTouch::AccessRow)
            .copied()
            .unwrap_or(0),
        decoded_rows: counters.decoded_rows,
        stored_row_bytes: counters.stored_row_bytes,
        verified_runs_built: counters.verified_runs_built,
        verified_postings_walked: counters.verified_postings_walked,
        hashed_bytes: counters.hashed_bytes,
        rows_examined: executed.result.trace.rows_examined,
        plan: format!(
            "{} index={:?} pushed={:?}",
            executed.result.trace.physical_plan,
            executed.result.trace.index_used,
            executed.result.trace.predicates_pushed,
        ),
    })
}

/// The reader a consumer actually uses: it declares the Contexts it may see,
/// which is what makes the table's `CONTEXT_ID` column a read gate.
fn authorized_reader(db: &Database) -> Database {
    db.scoped_with_contexts(
        (0..PARTITIONS)
            .map(|partition| contextdb_core::types::ContextId::new(context_id(partition)))
            .collect::<BTreeSet<_>>(),
    )
}

/// The route a consumer actually reads through: a read session over the
/// committed file image. It is the route that carries the retained relational
/// source, and so the only route on which a find can answer from authenticated
/// index identities instead of stored row versions.
#[cfg(unix)]
#[test]
fn a_find_decodes_only_the_rows_it_returns() {
    use contextdb_core::read_contract::ReadRoute;
    use contextdb_core::types::ContextId;
    use contextdb_engine::{DatabaseOpenOptions, OwnerReadConfig, ReadSession, ReadSessionOptions};
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("scoped-rows.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    let admin = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the fixture store");
    let owner = fixture_in(admin, false);
    owner.close().expect("seal the fixture store");

    let reader = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_options(
            &path,
            ReadSessionOptions {
                contexts: Some(
                    (0..PARTITIONS)
                        .map(|partition| ContextId::new(context_id(partition)))
                        .collect::<BTreeSet<_>>(),
                ),
                limits: limits(),
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open the committed file image the consumer reads through");
    assert_eq!(
        reader.route(),
        ReadRoute::File,
        "this proof measures the file route, not the owner service"
    );

    // Warm the read path once so nothing below counts a first-touch load.
    let _ = measure_reader(&reader, 1);
    let mut measured = Vec::new();
    for partitions in [1_usize, 3, PARTITIONS] {
        let observed = measure_reader(&reader, partitions);
        println!(
            "find_decode partitions={partitions} in_scope={} rows={} decoded_rows={} \
             stored_row_bytes={} rows_examined={} runs_built={} postings_walked={} plan={}",
            partitions * ROWS_PER_PARTITION,
            observed.rows,
            observed.decoded_rows,
            observed.stored_row_bytes,
            observed.rows_examined,
            observed.verified_runs_built,
            observed.verified_postings_walked,
            observed.plan,
        );
        assert_eq!(observed.rows, LIMIT, "every scope answers a full page");
        // The ten rows this read publishes are decoded to be projected, so a
        // zero here would mean the counters never saw this read at all -- and a
        // proof that passes because it measured nothing is worse than no proof.
        assert!(
            observed.decoded_rows >= LIMIT as u64,
            "the work counters observed this read: {} decoded rows for {LIMIT} published",
            observed.decoded_rows,
        );
        measured.push((partitions, observed));
    }

    let mut failures = Vec::new();
    for (partitions, observed) in &measured {
        // One decode per returned row, plus a small fixed allowance for the
        // schema and generation rows a read resolves once.
        if observed.decoded_rows > (LIMIT as u64) * 2 {
            failures.push(format!(
                "a {partitions}-partition find over {} in-scope vectors decoded {} whole row \
                 versions to return {LIMIT}",
                partitions * ROWS_PER_PARTITION,
                observed.decoded_rows,
            ));
        }
    }
    let (_, narrow) = &measured[0];
    let (_, broad) = &measured[measured.len() - 1];
    if broad.stored_row_bytes > narrow.stored_row_bytes.saturating_mul(2) {
        failures.push(format!(
            "the stored row bytes a find decodes grow with its scope: {} bytes over one \
             partition against {} bytes over {PARTITIONS}",
            narrow.stored_row_bytes, broad.stored_row_bytes,
        ));
    }
    assert!(
        failures.is_empty(),
        "a find decodes the rows it returns, not the rows it searched:\n{}",
        failures.join("\n")
    );
}

/// The same measurement over the read session, which has no bounded-kernel
/// probe of its own: what it can show is what the persistence layer decoded.
#[cfg(unix)]
fn measure_reader(reader: &contextdb_engine::ReadSession, partitions: usize) -> Measured {
    let selected = (0..partitions).collect::<Vec<_>>();
    let (sql, params) = statement(&selected);
    let work = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let answer = reader
        .execute(&sql, &params)
        .expect("the read session answers the find");
    let counters = work.counters();
    drop(work);
    Measured {
        rows: answer.rows.len(),
        returned: answer
            .rows
            .iter()
            .map(|row| match row.first() {
                Some(Value::Int64(id)) => *id,
                other => panic!("the projection returns one integer id, got {other:?}"),
            })
            .collect(),
        // The read session has no bounded-kernel probe; the gate count that
        // belongs to this repair is asserted on the bounded arm below.
        access_rows: 0,
        decoded_rows: counters.decoded_rows,
        stored_row_bytes: counters.stored_row_bytes,
        verified_runs_built: counters.verified_runs_built,
        verified_postings_walked: counters.verified_postings_walked,
        hashed_bytes: counters.hashed_bytes,
        rows_examined: answer.trace.rows_examined,
        plan: format!(
            "{} index={:?} pushed={:?}",
            answer.trace.physical_plan, answer.trace.index_used, answer.trace.predicates_pushed,
        ),
    }
}

/// The reader's row gate runs once per row an answer publishes, and not once
/// per vector the search considered.
///
/// A candidate drawn from an index range the reader's own restrictions already
/// bound has nothing left for the row gate to decide, and asking anyway costs a
/// stored row version per candidate. What must never move is the other half:
/// every row that IS published is still judged at the row.
#[test]
fn only_the_rows_an_answer_publishes_are_put_through_the_row_gate() {
    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let owner = fixture(&directory.path().join("scoped-rows.db"));
    let db = authorized_reader(&owner);
    let _ = measure(&db, 1);
    for partitions in [1_usize, 3, PARTITIONS] {
        let observed = measure(&db, partitions);
        assert_eq!(observed.rows, LIMIT, "every scope answers a full page");
        assert_eq!(
            observed.access_rows,
            LIMIT as u64,
            "a {partitions}-partition find over {} in-scope vectors put {} rows through the \
             reader's row gate to publish {LIMIT}",
            partitions * ROWS_PER_PARTITION,
            observed.access_rows,
        );
    }
}

/// A find verifies the postings its answer needs, not the postings its scope
/// contains.
///
/// The authenticated identity route proves a window of postings before it
/// delivers any identity, and each window costs a canonical-tree descent, a
/// fresh read transaction and a physical index range open. For a `LIMIT 10`
/// find none of that may grow with how much the reader is allowed to see: a
/// statement over eight Contexts asks for the same ten rows as a statement over
/// one, and must not pay eight times over to answer it.
///
/// Measured on the file route on purpose. A `Database::open` handle has no
/// retained relational source, so it builds no authenticated windows at all and
/// would report zero for every scope — a proof that passes by measuring
/// nothing.
#[cfg(unix)]
#[test]
fn a_find_verifies_what_it_walks_not_the_scope_it_searches() {
    use contextdb_core::read_contract::ReadRoute;
    use contextdb_core::types::ContextId;
    use contextdb_engine::{DatabaseOpenOptions, OwnerReadConfig, ReadSession, ReadSessionOptions};
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("scope-cost.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    let admin = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the fixture store");
    fixture_in(admin, false)
        .close()
        .expect("seal the fixture store");

    let reader = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_options(
            &path,
            ReadSessionOptions {
                contexts: Some(
                    (0..PARTITIONS)
                        .map(|partition| ContextId::new(context_id(partition)))
                        .collect::<BTreeSet<_>>(),
                ),
                limits: limits(),
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open the committed file image the consumer reads through");
    assert_eq!(reader.route(), ReadRoute::File);

    let mut measured = Vec::new();
    for partitions in [1_usize, 2, 4, PARTITIONS] {
        // A FRESH reader per scope: the run cache is per reader, so reusing one
        // would charge the first scope for windows the later scopes then get
        // free, and the growth law would be unreadable.
        let reader = ReadSession::with_runtime_directory_for_test(&runtime, || {
            ReadSession::open_with_options(
                &path,
                ReadSessionOptions {
                    contexts: Some(
                        (0..PARTITIONS)
                            .map(|partition| ContextId::new(context_id(partition)))
                            .collect::<BTreeSet<_>>(),
                    ),
                    limits: limits(),
                    ..ReadSessionOptions::default()
                },
            )
        })
        .expect("open a fresh committed file image for this scope");
        let observed = measure_reader(&reader, partitions);
        println!(
            "verify_cost partitions={partitions} in_scope={} rows={} runs_built={} \
             postings_walked={} hashed_bytes={}",
            partitions * ROWS_PER_PARTITION,
            observed.rows,
            observed.verified_runs_built,
            observed.verified_postings_walked,
            observed.hashed_bytes,
        );
        assert_eq!(observed.rows, LIMIT, "every scope answers a full page");
        measured.push((partitions, observed));
    }
    let (_, narrow) = &measured[0];
    let (_, broad) = &measured[measured.len() - 1];
    let mut failures = Vec::new();
    if broad.verified_postings_walked > narrow.verified_postings_walked.max(4 * LIMIT as u64) * 2 {
        failures.push(format!(
            "the postings a find proves grow with its scope: {} over one partition against {} \
             over {PARTITIONS}",
            narrow.verified_postings_walked, broad.verified_postings_walked,
        ));
    }
    if broad.verified_runs_built > narrow.verified_runs_built.max(2) * 2 {
        failures.push(format!(
            "the authenticated windows a find builds grow with its scope: {} over one partition \
             against {} over {PARTITIONS}",
            narrow.verified_runs_built, broad.verified_runs_built,
        ));
    }
    assert!(
        failures.is_empty(),
        "a find pays for the answer it returns, not the scope it may see:\n{}",
        failures.join("\n")
    );
}

/// The row every answer publishes is measured against the statement that asked
/// for it, at the row, not at the index entry that led to it.
///
/// On the identity route nothing reads the row body before it is scored, so
/// `context_id`, `generation_id` and `is_current` are believed from the index
/// posting alone. A row whose stored body contradicts the posting that names it
/// would otherwise be published although the statement excluded it -- and a
/// non-current row is exactly that, which no authorization gate can catch,
/// because being non-current is not an authorization fact.
///
/// The damage is applied to the ROW, not to the posting. Moving a posting is
/// caught earlier, by the window reconciliation that compares the canonical
/// posting tree against the physical index before any identity is delivered;
/// changing the row leaves both of those agreeing with each other, so the check
/// made where the row is published is the only one that can see it.
#[cfg(unix)]
#[test]
fn a_row_that_contradicts_its_index_posting_is_reported_as_damage() {
    use contextdb_core::read_contract::ReadRoute;
    use contextdb_core::types::ContextId;
    use contextdb_engine::{DatabaseOpenOptions, OwnerReadConfig, ReadSession, ReadSessionOptions};
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("contradicting.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    let admin = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the fixture store");
    let owner = fixture_in(admin, true);
    owner.close().expect("seal the fixture store");

    let open_reader = || {
        ReadSession::with_runtime_directory_for_test(&runtime, || {
            ReadSession::open_with_options(
                &path,
                ReadSessionOptions {
                    contexts: Some(BTreeSet::from([ContextId::new(context_id(0))])),
                    limits: limits(),
                    ..ReadSessionOptions::default()
                },
            )
        })
        .expect("open the committed file image the consumer reads through")
    };
    let (sql, params) = statement(&[0]);

    let healthy = open_reader();
    assert_eq!(healthy.route(), ReadRoute::File);
    let answer = healthy
        .execute(&sql, &params)
        .expect("the undamaged store answers");
    assert_eq!(
        answer.rows.len(),
        LIMIT,
        "the undamaged store answers a full page"
    );
    assert_eq!(
        answer.rows.first().and_then(|row| row.first()),
        Some(&Value::Int64(PLANTED_ROW_ID)),
        "the planted row is the nearest searchable row in its partition, so it heads the answer"
    );
    drop(healthy);

    // Every index still says this row is current; only its stored body now
    // says otherwise.
    contradict_indexed_column_for_test(
        &path,
        "scoped_rows",
        contextdb_core::RowId(PLANTED_ROW_ID as u64),
        "is_current",
        Value::Bool(false),
    )
    .expect("make one row's body contradict the postings that name it");

    match open_reader().execute(&sql, &params) {
        Ok(served) => panic!(
            "a row whose body contradicts its index posting was served in an answer of {} rows",
            served.rows.len(),
        ),
        Err(refusal) => {
            let refusal = format!("{refusal:?}");
            assert!(
                refusal.contains("StoreCorrupted"),
                "a row that contradicts its posting is damage, not {refusal}"
            );
        }
    }
}

/// The other direction of the same divergence: the posting moves and the row
/// stays. Both are damage, and neither is ever served.
///
/// Damage is reported on the path that reads it. A find whose filter admits
/// its whole pair answers from the committed canonical index and the partition
/// and never reads the physical posting page this damage sits on, so it answers
/// correctly and reports nothing; a find that proves its candidates through a
/// window over the damaged range reads that page, and reports it. Both arms are
/// asserted, over the same Context.
#[cfg(unix)]
#[test]
fn a_moved_index_posting_is_reported_as_damage() {
    use contextdb_core::read_contract::ReadRoute;
    use contextdb_core::types::ContextId;
    use contextdb_engine::{DatabaseOpenOptions, OwnerReadConfig, ReadSession, ReadSessionOptions};
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("moved-posting.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    let admin = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the fixture store");
    fixture_in(admin, false)
        .close()
        .expect("seal the fixture store");

    // One row of the SECOND Context, filed under the FIRST Context's key. The
    // row body is untouched, so only the index disagrees with it.
    misfile_relational_index_posting_for_test(
        &path,
        "scoped_rows",
        "scoped_rows_active_idx",
        contextdb_core::RowId((ROWS_PER_PARTITION + 1) as u64),
        contextdb_core::RowId(1),
    )
    .expect("file one Context's row under another Context's key");

    let reader = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_options(
            &path,
            ReadSessionOptions {
                contexts: Some(BTreeSet::from([ContextId::new(context_id(0))])),
                limits: limits(),
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open the committed file image the consumer reads through");
    assert_eq!(reader.route(), ReadRoute::File);
    // The whole-pair arm. This find's filter admits every live vector of the
    // pair it names, which the committed canonical index proves with one
    // authenticated descent; it then walks the partition and never reads the
    // physical posting page the damage sits on. A structure is reported damaged
    // when a read reads it, so this read owes no report -- but it must never
    // serve the misfiled row: the page is full and every row is the first
    // Context's own.
    let (sql, params) = statement(&[0]);
    let served = reader
        .execute(&sql, &params)
        .expect("the whole-pair find never reads the damaged page and answers");
    let ids = served
        .rows
        .iter()
        .filter_map(|row| match row.first() {
            Some(Value::Int64(id)) => Some(*id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        served.rows.len() == LIMIT
            && ids
                .iter()
                .all(|id| (1..=ROWS_PER_PARTITION as i64).contains(id)),
        "a posting filed under another Context's key was served in an answer of {} rows: {ids:?}",
        served.rows.len(),
    );

    // The window arm, over the same Context. Pinning only `context_id` names
    // no whole partition, so this find proves its candidates through an
    // authenticated window over the first Context's key range -- the range the
    // misfiled posting sits in -- and that window's physical reconciliation
    // reads the damaged page and reports it.
    let windowed = format!(
        "SELECT id FROM scoped_rows WHERE context_id = $context_0 \
         ORDER BY embedding <=> $query LIMIT {LIMIT}"
    );
    match reader.execute(&windowed, &params) {
        Ok(served) => panic!(
            "a find that reads the damaged posting page served {} rows instead of reporting it",
            served.rows.len(),
        ),
        Err(refusal) => {
            let refusal = format!("{refusal:?}");
            assert!(
                refusal.contains("StoreCorrupted"),
                "a posting that names a row in another Context is damage, not {refusal}"
            );
        }
    }
}

/// A row whose indexed columns change is not damage, and must never be
/// reported as damage.
///
/// The refusal above rests on one claim: inside a single snapshot a posting and
/// the row version it names are written and retired by the same transaction, so
/// they cannot disagree. This is the other half of that claim. A committed
/// change to `is_current` or to the Context, and a change this reader's own
/// open transaction has staged but not committed — where the committed index
/// legitimately has not seen it yet — all answer whole, with no refusal.
#[test]
fn a_row_whose_indexed_columns_change_is_never_reported_as_damage() {
    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let owner = fixture(&directory.path().join("changing.db"));
    let reader = authorized_reader(&owner);

    let before = measure(&reader, 1);
    assert_eq!(
        before.rows, LIMIT,
        "the unchanged store answers a full page"
    );
    let retired = *before
        .returned
        .first()
        .expect("the answer names at least one row");

    // A committed change to the residual column, read at the newer snapshot.
    owner
        .execute(
            "UPDATE scoped_rows SET is_current = FALSE WHERE id = $id",
            &HashMap::from([("id".into(), Value::Int64(retired))]),
        )
        .expect("retire one of the rows the answer named");
    let after = try_measure_contexts(&reader, &[0])
        .expect("a committed change to an indexed column is not damage");
    assert_eq!(
        after.rows, LIMIT,
        "the answer stays whole after a row is retired"
    );
    assert!(
        !after.returned.contains(&retired),
        "the retired row is gone from the answer: {:?}",
        after.returned
    );

    // A committed change to the partition column.
    let moved = *after
        .returned
        .first()
        .expect("the answer names at least one row");
    owner
        .execute(
            "UPDATE scoped_rows SET context_id = $context, generation_id = $generation \
             WHERE id = $id",
            &HashMap::from([
                ("id".into(), Value::Int64(moved)),
                ("context".into(), Value::Uuid(context_id(1))),
                ("generation".into(), Value::Uuid(generation_id(1))),
            ]),
        )
        .expect("move one row to a neighbouring Context");
    let moved_away =
        try_measure_contexts(&reader, &[0]).expect("a committed Context change is not damage");
    assert_eq!(
        moved_away.rows, LIMIT,
        "the answer stays whole after a row moves Context"
    );
    assert!(
        !moved_away.returned.contains(&moved),
        "the moved row is gone from the answer: {:?}",
        moved_away.returned
    );

    // A change this reader's own transaction has staged and not committed: the
    // committed index still files the row as current, the row this reader sees
    // does not, and that disagreement is ordinary rather than damage.
    owner
        .execute("BEGIN", &HashMap::new())
        .expect("open a transaction on the reader's own handle");
    let staged = *moved_away
        .returned
        .first()
        .expect("the answer names at least one row");
    owner
        .execute(
            "UPDATE scoped_rows SET is_current = FALSE WHERE id = $id",
            &HashMap::from([("id".into(), Value::Int64(staged))]),
        )
        .expect("stage a retirement without committing it");
    let mid_transaction = try_measure_contexts(&owner, &[0])
        .expect("a staged change the committed index has not seen is not damage");
    assert!(
        !mid_transaction.returned.contains(&staged),
        "the row this transaction retired is gone from its own answer: {:?}",
        mid_transaction.returned
    );
    owner
        .execute("COMMIT", &HashMap::new())
        .expect("commit the staged retirement");
}

/// A statement may name a Context its reader may not see. What comes back is
/// what the reader may see, and the Context it may not see is simply absent.
#[test]
fn a_statement_naming_an_unauthorized_context_gets_only_what_its_reader_may_see() {
    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let owner = fixture(&directory.path().join("partly-authorized.db"));
    // The reader may see the first two Contexts; the statement names the first
    // and the third.
    let reader = owner.scoped_with_contexts(
        (0..2)
            .map(|partition| contextdb_core::types::ContextId::new(context_id(partition)))
            .collect::<BTreeSet<_>>(),
    );
    let observed = measure_contexts(&reader, &[0, 2]);
    assert!(
        !observed.returned.is_empty(),
        "the Context the reader may see still answers"
    );
    let authorized_rows = 1..=(ROWS_PER_PARTITION as i64);
    for row in &observed.returned {
        assert!(
            authorized_rows.contains(row),
            "row {row} comes from a Context this reader may not see"
        );
    }
}

/// A statement that pins no Context at all is still answered only from the
/// Contexts its reader may see.
#[test]
fn a_statement_that_pins_no_context_is_still_answered_only_from_authorized_ones() {
    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let owner = fixture(&directory.path().join("unpinned.db"));
    let reader =
        owner.scoped_with_contexts(BTreeSet::from([contextdb_core::types::ContextId::new(
            context_id(0),
        )]));
    let observed = measure_contexts(&reader, &[]);
    assert!(
        !observed.returned.is_empty(),
        "an unpinned statement still answers from what its reader may see"
    );
    let authorized_rows = 1..=(ROWS_PER_PARTITION as i64);
    for row in &observed.returned {
        assert!(
            authorized_rows.contains(row),
            "row {row} comes from a Context this reader may not see"
        );
    }
}
