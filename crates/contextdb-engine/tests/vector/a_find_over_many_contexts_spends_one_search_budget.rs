//! A find over many authorized Contexts must spend ONE search, not one per
//! Context.
//!
//! A user asks for the ten nearest passages. Whether those passages live in one
//! Context or in every Context the reader may see is a fact about their data,
//! not a reason to do eight times the work: the answer is one global top ten,
//! so the search that produces it gets one candidate budget, spent best-first
//! across every admitted partition, and one result heap. What a wider scope
//! legitimately adds is ADMISSION -- every non-empty snapshot-compatible layer
//! of every admitted partition has to be opened and descended, because an
//! unopened layer's entry point is not a lower bound on what lies behind it --
//! and that is a small floor per layer, not another whole walk.
//!
//! Measured on the file route through a real `ReadSession`, which is the door
//! the consumer reads through, with a fresh reader per scope so no scope is
//! charged for work an earlier scope already paid.
//!
//! The work assertion catches a separate full walk per admitted partition:
//! distance evaluations must stay within the shared search budget as the
//! number of authorized Contexts grows.

use contextdb_core::read_contract::{OwnerReadCancellation, ReadLimits, ReadRoute};
use contextdb_core::types::ContextId;
use contextdb_core::{Error, Value, VectorIndexRef};
use contextdb_engine::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};
use contextdb_engine::read_session::{
    ReadKernelCancellationEvent, ReadKernelResidualRejectionEvent, ReadKernelSourceEvent,
    ReadKernelTestObserver, ReadSessionOperation,
};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, ReadSession,
    ReadSessionOptions,
};
use contextdb_vector::hnsw::{
    HnswCandidateDistanceEvent, HnswCandidateObserver, PagedVectorObserver,
    with_hnsw_candidate_observer, with_paged_vector_observer,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use uuid::Uuid;

const PARTITIONS: usize = 8;
const ROWS_PER_PARTITION: usize = 500;
const DIMENSIONS: usize = 16;
const LIMIT: usize = 10;

/// What admitting one more partition may cost: its non-empty layers' entry
/// evaluations and the greedy descent above layer zero. Read off the graph's
/// own shape rather than chosen to make a number pass -- about
/// `M * (levels - 1) + 1` per layer, which at this fixture's degree 16 and
/// three levels is 33, doubled for the second layer a partition may carry.
const ADMISSION_ALLOWANCE_PER_PARTITION: u64 = 66;

/// A budget smaller than the sum of what this fixture's partitions ask the
/// cache to free for them, so the search cannot simply hold everything and has
/// to decide what to keep. Read off the fixture: eight partitions of 500
/// sixteen-dimension rows.
const TIGHT_MEMORY_BUDGET_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Default)]
struct Counts {
    evaluations: AtomicU64,
    vectors_decoded: AtomicU64,
    page_hits: AtomicU64,
    page_misses: AtomicU64,
    pages_filled: AtomicU64,
}

impl HnswCandidateObserver for Counts {
    fn before_candidate_distance(&self, _: HnswCandidateDistanceEvent) {
        self.evaluations.fetch_add(1, Ordering::Relaxed);
    }
}

impl PagedVectorObserver for Counts {
    /// One stored vector read out of the serving pages, for a walk distance
    /// or for a candidate's published score. Kept apart from `evaluations`:
    /// the paged walk reports each of its distances through
    /// `HnswCandidateObserver` exactly as the in-memory walk does, so adding
    /// this in would count every paged walk distance twice.
    fn vector_decoded(&self) {
        self.vectors_decoded.fetch_add(1, Ordering::Relaxed);
    }

    fn page_filled(&self) {
        self.pages_filled.fetch_add(1, Ordering::Relaxed);
    }

    fn page_looked_up(&self, hit: bool) {
        if hit {
            self.page_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.page_misses.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn context_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC04C_0000_0000_0000_0000_0000_0000_0000 + partition as u128)
}

fn generation_id(partition: usize) -> Uuid {
    Uuid::from_u128(0xC04C_1000_0000_0000_0000_0000_0000_0000 + partition as u128)
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

fn query_vector() -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|axis| {
            let seed = axis.wrapping_mul(0x27d4_eb2f) % 10_009;
            (seed as f32 / 10_009.0) - 0.5
        })
        .collect()
}

fn limits() -> ReadLimits {
    ReadLimits {
        work: 5_000_000,
        memory: 512 * 1024 * 1024,
        ..ReadLimits::default()
    }
}

#[cfg(unix)]
fn seed(path: &std::path::Path, runtime: &std::path::Path) {
    seed_shape(path, runtime, PARTITIONS, ROWS_PER_PARTITION);
}

/// The fixture's rows, split over `partitions` Contexts. Row ordinals, and so
/// the vectors, are the same whatever the split.
#[cfg(unix)]
fn seed_shape(
    path: &std::path::Path,
    runtime: &std::path::Path,
    partitions: usize,
    rows_per_partition: usize,
) {
    let admin = Database::open_with_options(
        path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.to_path_buf()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the fixture store");
    admin.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    admin
        .execute(
            "CREATE TABLE scoped_rows (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
             generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
             embedding VECTOR(16) PARTITION_KEY (context_id, generation_id) \
             MAX_PARTITIONS 8 SEARCH_MODE AUTO AUTO_INDEX_AT 1)",
            &HashMap::new(),
        )
        .expect("declare the partitioned fixture");
    admin
        .execute(
            "CREATE INDEX scoped_rows_active_idx ON scoped_rows \
             (context_id, generation_id, is_current)",
            &HashMap::new(),
        )
        .expect("declare the index the candidate identities come from");
    for partition in 0..partitions {
        for local in 0..rows_per_partition {
            let ordinal = partition * rows_per_partition + local;
            admin
                .execute(
                    "INSERT INTO scoped_rows VALUES ($id, $context, $generation, TRUE, $embedding)",
                    &HashMap::from([
                        ("id".into(), Value::Int64(ordinal as i64 + 1)),
                        ("context".into(), Value::Uuid(context_id(partition))),
                        ("generation".into(), Value::Uuid(generation_id(partition))),
                        ("embedding".into(), Value::Vector(embedding(ordinal))),
                    ]),
                )
                .expect("seed one partitioned row");
        }
    }
    let index = VectorIndexRef::new("scoped_rows", "embedding");
    for _ in 0..4096 {
        admin
            .run_maintenance_cycle()
            .expect("advance deterministic maintenance");
        let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
            admin
                .declared_reader_memory()
                .expect("admit the partition observation reader"),
        );

        let ready = admin
            .vector_store_for_test()
            .partition_infos(&index, &mut partition_observation_owner_1)
            .expect("inspect partition readiness");
        if ready.len() == partitions && ready.iter().all(|partition| partition.graph_available) {
            admin.close().expect("seal the fixture store");
            return;
        }
    }
    panic!("every partition must publish a maintained route before the read is measured");
}

fn statement(selected: usize) -> (String, HashMap<String, Value>) {
    let mut params = HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(query_vector())),
    ]);
    let mut contexts = Vec::with_capacity(selected);
    let mut generations = Vec::with_capacity(selected);
    for partition in 0..selected {
        let context = format!("context_{partition}");
        let generation = format!("generation_{partition}");
        params.insert(context.clone(), Value::Uuid(context_id(partition)));
        params.insert(generation.clone(), Value::Uuid(generation_id(partition)));
        contexts.push(format!("${context}"));
        generations.push(format!("${generation}"));
    }
    (
        format!(
            "SELECT id FROM scoped_rows WHERE context_id IN ({}) AND generation_id IN ({}) \
             AND is_current = $is_current ORDER BY embedding <=> $query LIMIT {LIMIT}",
            contexts.join(", "),
            generations.join(", "),
        ),
        params,
    )
}

struct Observed {
    evaluations: u64,
    vectors_decoded: u64,
    page_hits: u64,
    pages_filled: u64,
    page_misses: u64,
    decoded_rows: u64,
    verified_runs_built: u64,
    verified_postings_walked: u64,
}

#[cfg(unix)]
fn measure(path: &std::path::Path, runtime: &std::path::Path, selected: usize) -> Observed {
    measure_within(path, runtime, selected, limits())
}

/// The same read under a caller-declared memory budget, so a proof can say what
/// a find does when its partitions' graphs do not all fit at once.
#[cfg(unix)]
fn measure_within(
    path: &std::path::Path,
    runtime: &std::path::Path,
    selected: usize,
    limits: ReadLimits,
) -> Observed {
    // A fresh reader per scope: run caches and loaded graphs are per reader, so
    // reusing one would charge the first scope for work the later scopes then
    // get free and the growth law would be unreadable.
    let reader = ReadSession::with_runtime_directory_for_test(runtime, || {
        ReadSession::open_with_options(
            path,
            ReadSessionOptions {
                contexts: Some(
                    (0..PARTITIONS)
                        .map(|partition| ContextId::new(context_id(partition)))
                        .collect::<BTreeSet<_>>(),
                ),
                limits,
                memory_limit: Some(limits.memory as usize),
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open the committed file image the consumer reads through");
    assert_eq!(reader.route(), ReadRoute::File);
    let (sql, params) = statement(selected);
    // One find to load this scope's graphs and fill its serving pages, then the
    // measured one: a warm find is what the promise is about.
    let first = reader
        .execute(&sql, &params)
        .expect("the first find answers");
    assert_eq!(first.rows.len(), LIMIT, "every scope answers a full page");

    let counts = Arc::new(Counts::default());
    let work = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let answer = with_paged_vector_observer(counts.clone(), || {
        with_hnsw_candidate_observer(counts.clone(), || {
            reader
                .execute(&sql, &params)
                .expect("the warm find answers")
        })
    });
    let counters = work.counters();
    drop(work);
    assert_eq!(answer.rows.len(), LIMIT, "every scope answers a full page");
    println!(
        "budget route contexts={selected} vector_search={:?} rows_examined={}",
        answer.trace.vector_search, answer.trace.rows_examined,
    );
    Observed {
        evaluations: counts.evaluations.load(Ordering::Relaxed),
        vectors_decoded: counts.vectors_decoded.load(Ordering::Relaxed),
        page_hits: counts.page_hits.load(Ordering::Relaxed),
        pages_filled: counts.pages_filled.load(Ordering::Relaxed),
        page_misses: counts.page_misses.load(Ordering::Relaxed),
        decoded_rows: counters.decoded_rows,
        verified_runs_built: counters.verified_runs_built,
        verified_postings_walked: counters.verified_postings_walked,
    }
}

#[cfg(unix)]
#[test]
fn a_find_over_many_contexts_spends_one_search_budget() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("many-contexts.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    seed(&path, &runtime);

    let mut measured = Vec::new();
    for selected in [1_usize, 2, 4, PARTITIONS] {
        let observed = measure(&path, &runtime, selected);
        println!(
            "budget contexts={selected} in_scope={} evaluations={} vectors_decoded={} \
             pages_filled={} page_hits={} page_misses={} decoded_rows={} runs_built={} \
             postings_walked={}",
            selected * ROWS_PER_PARTITION,
            observed.evaluations,
            observed.vectors_decoded,
            observed.pages_filled,
            observed.page_hits,
            observed.page_misses,
            observed.decoded_rows,
            observed.verified_runs_built,
            observed.verified_postings_walked,
        );
        measured.push((selected, observed));
    }

    let narrow = &measured[0].1;
    // A partition small enough to compare exhaustively never walks a graph, and
    // an evaluation count of zero would satisfy any ceiling. The fixture is
    // sized so every partition takes the maintained route, and the proof says
    // so rather than passing on a vacuous zero.
    assert!(
        narrow.evaluations > 0,
        "the fixture must be large enough that a partition takes the maintained route"
    );
    let broad = &measured[measured.len() - 1].1;
    let admission = ADMISSION_ALLOWANCE_PER_PARTITION * (PARTITIONS as u64 - 1);
    let ceiling = narrow.evaluations + narrow.evaluations / 10 + admission;
    assert!(
        broad.evaluations <= ceiling,
        "a find over {PARTITIONS} Contexts spends one search budget plus admission: \
         {} evaluations against a ceiling of {ceiling} ({} at one Context, \
         {ADMISSION_ALLOWANCE_PER_PARTITION} admitted per extra partition)",
        broad.evaluations,
        narrow.evaluations,
    );
    for (selected, observed) in &measured {
        assert_eq!(
            observed.decoded_rows, LIMIT as u64,
            "a find at {selected} Contexts decodes only the rows it returns"
        );
    }
}

/// A second find over the same Contexts must not read the same graphs again.
///
/// A search over several partitions loads each one's maintained graph. If
/// loading one takes the graph the same search has just loaded, every find pays
/// to read every graph again, and the wider the scope the more of the store a
/// warm find re-reads. Measured under a memory budget deliberately smaller than
/// the sum of the partitions' own reclaim prices, so the search has to decide
/// what to keep.
#[cfg(unix)]
#[test]
fn a_warm_find_over_many_contexts_reads_no_graph_twice() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("many-contexts-budget.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    seed(&path, &runtime);

    let tight = ReadLimits {
        memory: TIGHT_MEMORY_BUDGET_BYTES,
        ..limits()
    };
    let observed = measure_within(&path, &runtime, PARTITIONS, tight);
    println!(
        "warm_reload contexts={PARTITIONS} budget={TIGHT_MEMORY_BUDGET_BYTES} evaluations={} \
         pages_filled={} page_hits={} page_misses={}",
        observed.evaluations, observed.pages_filled, observed.page_hits, observed.page_misses,
    );
    assert!(
        observed.pages_filled == 0,
        "a warm find over {PARTITIONS} Contexts reads no graph twice, filled {} serving pages",
        observed.pages_filled,
    );
}

/// What a wider scope adds to one search is admission, never another walk.
///
/// The same rows are stored once in a single Context and once split over
/// eight, and the one search's own receipt is read on a fresh reader for each.
/// Splitting the rows adds layers to open and, because the nearest rows are
/// now spread over eight graphs, at most one first expansion of each extra
/// layer's descended entry -- one layer-zero neighbourhood per layer. Beyond
/// that floor, expansion stays within a tenth of the one-Context walk: it is
/// never a whole walk per Context. The walk stays inside the one budget, the
/// same number at both scopes, with opening the layers a floor on top.
#[cfg(unix)]
#[test]
fn expansion_work_does_not_grow_with_context_count() {
    use contextdb_vector::hnsw::with_union_search_receipts;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    let whole = directory.path().join("one-context.db");
    seed_shape(&whole, &runtime, 1, PARTITIONS * ROWS_PER_PARTITION);
    let split = directory.path().join("eight-contexts.db");
    seed(&split, &runtime);

    let mut receipts = Vec::new();
    for (path, selected) in [(&whole, 1_usize), (&split, PARTITIONS)] {
        let reader = ReadSession::with_runtime_directory_for_test(&runtime, || {
            ReadSession::open_with_options(
                path,
                ReadSessionOptions {
                    contexts: Some(
                        (0..PARTITIONS)
                            .map(|partition| ContextId::new(context_id(partition)))
                            .collect::<BTreeSet<_>>(),
                    ),
                    limits: limits(),
                    memory_limit: Some(limits().memory as usize),
                    ..ReadSessionOptions::default()
                },
            )
        })
        .expect("open the committed file image the consumer reads through");
        let (sql, params) = statement(selected);
        reader
            .execute(&sql, &params)
            .expect("the first find answers");
        let (answer, searched) =
            with_union_search_receipts(|| reader.execute(&sql, &params).expect("warm find"));
        assert_eq!(answer.rows.len(), LIMIT, "every scope answers a full page");
        assert_eq!(
            searched.len(),
            1,
            "a find over {selected} Contexts runs exactly one search: {searched:?}"
        );
        let receipt = searched[0];
        println!("expansion contexts={selected} receipt={receipt:?}");
        assert_eq!(
            receipt.partitions, selected,
            "every named Context is admitted"
        );
        assert!(
            receipt.expansion_evaluations <= receipt.budget,
            "the walk stays inside the one budget, with opening the layers a floor on top: \
             {receipt:?}"
        );
        receipts.push(receipt);
    }
    let (narrow, broad) = (receipts[0], receipts[1]);
    assert!(
        broad.layers_admitted >= PARTITIONS && broad.layers_admitted > narrow.layers_admitted,
        "every admitted Context's layers are opened: {narrow:?} against {broad:?}"
    );
    let first_expansions =
        (broad.layers_admitted - narrow.layers_admitted) * broad.layer_zero_degree;
    let ceiling =
        narrow.expansion_evaluations + narrow.expansion_evaluations / 10 + first_expansions;
    assert!(
        broad.expansion_evaluations <= ceiling,
        "expansion over {PARTITIONS} Contexts is one walk plus one first expansion per extra \
         layer: {} against a ceiling of {ceiling} ({} over one Context, {first_expansions} for \
         the extra layers)",
        broad.expansion_evaluations,
        narrow.expansion_evaluations,
    );
    assert_eq!(
        broad.budget, narrow.budget,
        "the budget is one search's, not one per Context"
    );
}

/// Opening a layer is a floor the search pays on top of its one budget, never
/// a cost taken out of the walk. The walk over every admitted layer may spend
/// the whole budget however many layers were opened before it: otherwise each
/// Context a find names shrinks the walk every other Context shares, and a
/// wide find refuses a query a narrow one answers. On the owner's laptop at
/// eight Contexts, opening nine layers took 802 of a 9,600 budget and a query
/// whose walk needed the rest was refused, then compared exhaustively against
/// all 100,000 passages past the time ceiling.
#[cfg(unix)]
#[test]
fn opening_more_layers_never_shrinks_the_walk() {
    use contextdb_vector::hnsw::with_union_search_receipts;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("many-contexts-walk.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    seed(&path, &runtime);

    let mut failures = Vec::new();
    for selected in [1_usize, 2, 4, PARTITIONS] {
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
                    memory_limit: Some(limits().memory as usize),
                    ..ReadSessionOptions::default()
                },
            )
        })
        .expect("open the committed file image the consumer reads through");
        let (sql, params) = statement(selected);
        let (answer, searched) =
            with_union_search_receipts(|| reader.execute(&sql, &params).expect("the find"));
        assert_eq!(answer.rows.len(), LIMIT);
        let receipt = searched[0];
        println!("walk contexts={selected} receipt={receipt:?}");
        let disclosed = answer
            .trace
            .vector_search
            .as_ref()
            .expect("the executed route publishes its search budget");
        assert_eq!(disclosed.global_candidate_budget, Some(receipt.budget));
        assert_eq!(
            disclosed.admission_evaluations,
            Some(receipt.admission_evaluations)
        );
        assert_eq!(
            disclosed.expansion_evaluations,
            Some(receipt.expansion_evaluations)
        );
        assert_eq!(disclosed.layers_admitted, Some(receipt.layers_admitted));
        assert_eq!(disclosed.budget_terminal.as_deref(), Some(receipt.terminal));
        assert!(
            receipt.admission_evaluations > 0,
            "layers were opened: {receipt:?}"
        );
        if receipt.expansion_cap != receipt.budget {
            failures.push(format!(
                "at {selected} Contexts opening {} layers cost {} evaluations and left the walk \
                 {} of its {} budget",
                receipt.layers_admitted,
                receipt.admission_evaluations,
                receipt.expansion_cap,
                receipt.budget
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A search that runs out of its budget refuses; it never answers short.
///
/// The same find over every Context is run under a range of declared work
/// limits, from too small for even opening every layer to plenty, each on a
/// fresh reader, once asking for the maintained route explicitly and once
/// leaving the choice to AUTO. Whatever the limit, the answer is a full page
/// or a typed refusal. The proof also requires that the refusals it relies on
/// were real: some limit must leave the one search `refused`, and some must
/// stop it below its own admission floor, which is reported rather than met by
/// silently skipping layers.
#[cfg(unix)]
#[test]
fn an_exhausted_budget_refuses_and_never_truncates() {
    use contextdb_vector::hnsw::with_union_search_receipts;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("many-contexts-refusal.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    seed(&path, &runtime);

    let (sql, params) = statement(PARTITIONS);
    let indexed = sql.replace(" LIMIT ", " USE VECTOR INDEXED LIMIT ");
    let mut saw_refused_search = false;
    let mut saw_floor_refusal = false;
    let mut saw_indexed_refusal = false;
    let mut saw_full_page = false;
    // The statement's candidate windows cost nothing here: its filter admits
    // every live vector of the Contexts it names, which the committed index
    // proves with one descent per Context, so the declared work is the one
    // search's own. This band runs from below its admission floor to a
    // complete answer.
    for work in (200_u64..=1_500).step_by(50).chain([5_000_000]) {
        for (mode, statement) in [("auto", &sql), ("indexed", &indexed)] {
            let limits = ReadLimits { work, ..limits() };
            let reader = ReadSession::with_runtime_directory_for_test(&runtime, || {
                ReadSession::open_with_options(
                    &path,
                    ReadSessionOptions {
                        contexts: Some(
                            (0..PARTITIONS)
                                .map(|partition| ContextId::new(context_id(partition)))
                                .collect::<BTreeSet<_>>(),
                        ),
                        limits,
                        memory_limit: Some(limits.memory as usize),
                        ..ReadSessionOptions::default()
                    },
                )
            })
            .expect("open the committed file image the consumer reads through");
            let (answer, receipts) =
                with_union_search_receipts(|| reader.execute(statement, &params));
            let terminal = receipts.first().map(|receipt| receipt.terminal);
            let below_floor = receipts
                .first()
                .is_some_and(|receipt| receipt.below_admission_floor);
            println!(
                "refusal work={work} mode={mode} terminal={terminal:?} below_floor={below_floor} \
                 answer={:?}",
                answer
                    .as_ref()
                    .map(|answer| answer.rows.len())
                    .map_err(|error| format!("{error}").chars().take(90).collect::<String>()),
            );
            match &answer {
                Ok(answer) => {
                    assert_eq!(
                        answer.rows.len(),
                        LIMIT,
                        "work={work} mode={mode}: an answer is a full page, never a short one"
                    );
                    saw_full_page = true;
                }
                Err(_) if mode == "indexed" => saw_indexed_refusal = true,
                Err(_) => {}
            }
            if terminal == Some("refused") {
                saw_refused_search = true;
                if below_floor && mode == "indexed" {
                    assert!(
                        answer.is_err(),
                        "work={work}: a search below its admission floor refuses INDEXED"
                    );
                    saw_floor_refusal = true;
                }
            }
        }
    }
    assert!(saw_full_page, "a large enough budget answers");
    assert!(saw_indexed_refusal, "a small enough budget refuses INDEXED");
    assert!(
        saw_refused_search,
        "some budget leaves the one search refused"
    );
    assert!(
        saw_floor_refusal,
        "some budget stops the search below its admission floor, and it is refused"
    );
}

/// When fewer than k rows pass the indexed residual, the search must prove
/// that there is no remaining eligible row before returning a short page.
#[cfg(unix)]
#[test]
fn a_short_page_exhausts_the_eligible_frontier_on_both_routes() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("short-page.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("private reader runtime");
    seed_shape(&path, &runtime, 1, 64);
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
    .expect("open fixture for deterministic residual changes");
    for id in 1_i64..=61 {
        admin
            .execute(
                "UPDATE scoped_rows SET is_current = FALSE WHERE id = $id",
                &HashMap::from([("id".into(), Value::Int64(id))]),
            )
            .expect("reject one near candidate");
    }
    admin.close().expect("seal fixture before route checks");

    let (sql, params) = statement(1);
    let indexed = sql.replace(" LIMIT ", " USE VECTOR INDEXED LIMIT ");
    let exact = sql.replace(" LIMIT ", " USE VECTOR EXACT LIMIT ");
    for route in [ReadRoute::Owner, ReadRoute::File] {
        let owner = (route == ReadRoute::Owner).then(|| {
            Database::open_with_options(
                &path,
                DatabaseOpenOptions {
                    owner_reads: OwnerReadConfig {
                        runtime_dir: Some(runtime.clone()),
                        ..OwnerReadConfig::default()
                    },
                    ..DatabaseOpenOptions::default()
                },
            )
            .expect("owner opens the short-page fixture")
        });
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
        .expect("open requested read route");
        assert_eq!(reader.route(), route);
        let expected = reader.execute(&exact, &params).expect("exact answer");
        let answer = reader.execute(&indexed, &params).expect("indexed answer");
        let ids = |answer: &contextdb_engine::QueryResult| {
            answer
                .rows
                .iter()
                .map(|row| match row[0] {
                    Value::Int64(id) => id,
                    ref other => panic!("id is an integer, got {other:?}"),
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&expected).len(), 3, "fixture has three eligible rows");
        assert_eq!(
            ids(&answer),
            ids(&expected),
            "{route:?} short page is complete"
        );
        assert_eq!(
            answer
                .trace
                .vector_search
                .as_ref()
                .and_then(|trace| trace.budget_terminal.as_deref()),
            Some("frontier_exhausted"),
            "{route:?} short page has an exhausted-frontier receipt"
        );
        drop(reader);
        if let Some(owner) = owner {
            owner.close().expect("close owner");
        }
    }
}

/// A residual may discard more near points than the search's declared
/// breadth, but enough farther points must refill the requested page.
#[cfg(unix)]
#[test]
fn rejected_neighbours_refill_the_page_on_both_routes() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("refill.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("private reader runtime");
    seed_shape(&path, &runtime, 1, 128);
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
    .expect("open fixture for deterministic residual changes");
    admin
        .execute(
            "ALTER TABLE scoped_rows ALTER COLUMN embedding SET HNSW (EF_SEARCH = 40)",
            &HashMap::new(),
        )
        .expect("declare the search breadth");
    let query = query_vector();
    let mut ranked = (0..128)
        .map(|ordinal| {
            (
                ordinal as i64 + 1,
                contextdb_vector::cosine_similarity(&query, &embedding(ordinal)),
            )
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    for (id, _) in ranked.iter().take(50) {
        admin
            .execute(
                "UPDATE scoped_rows SET is_current = FALSE WHERE id = $id",
                &HashMap::from([("id".into(), Value::Int64(*id))]),
            )
            .expect("reject a near candidate");
    }
    admin.close().expect("seal fixture before route checks");

    let (sql, params) = statement(1);
    let indexed = sql.replace(" LIMIT ", " USE VECTOR INDEXED LIMIT ");
    let exact = sql.replace(" LIMIT ", " USE VECTOR EXACT LIMIT ");
    for route in [ReadRoute::Owner, ReadRoute::File] {
        let owner = (route == ReadRoute::Owner).then(|| {
            Database::open_with_options(
                &path,
                DatabaseOpenOptions {
                    owner_reads: OwnerReadConfig {
                        runtime_dir: Some(runtime.clone()),
                        ..OwnerReadConfig::default()
                    },
                    ..DatabaseOpenOptions::default()
                },
            )
            .expect("owner opens refill fixture")
        });
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
        .expect("open requested read route");
        assert_eq!(reader.route(), route);
        let expected = reader.execute(&exact, &params).expect("exact answer");
        let answer = reader.execute(&indexed, &params).expect("indexed refill");
        let ids = |answer: &contextdb_engine::QueryResult| {
            answer
                .rows
                .iter()
                .map(|row| match row[0] {
                    Value::Int64(id) => id,
                    ref other => panic!("id is an integer, got {other:?}"),
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            ids(&answer).len(),
            LIMIT,
            "{route:?} refills the whole page"
        );
        assert_eq!(
            ids(&answer),
            ids(&expected),
            "{route:?} refills with the nearest eligible rows"
        );
        assert_eq!(
            answer
                .trace
                .vector_search
                .as_ref()
                .and_then(|trace| trace.budget_terminal.as_deref()),
            Some("answer_complete"),
            "{route:?} records a completed answer"
        );
        drop(reader);
        if let Some(owner) = owner {
            owner.close().expect("close owner");
        }
    }
}

#[derive(Default)]
struct RejectionGate {
    state: Mutex<RejectionGateState>,
    changed: Condvar,
}

#[derive(Default)]
struct RejectionGateState {
    armed: bool,
    event: Option<ReadKernelResidualRejectionEvent>,
    token: Option<OwnerReadCancellation>,
    released: bool,
    cancellation_observed: bool,
}

impl ReadKernelTestObserver for RejectionGate {
    fn before_source_touch(
        &self,
        _event: ReadKernelSourceEvent,
        _cancellation: OwnerReadCancellation,
    ) {
    }

    fn after_vector_residual_rejection(
        &self,
        event: ReadKernelResidualRejectionEvent,
        cancellation: OwnerReadCancellation,
    ) {
        let mut state = self.state.lock().expect("rejection gate lock");
        if !state.armed || state.event.is_some() {
            return;
        }
        state.event = Some(event);
        state.token = Some(cancellation);
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).expect("rejection gate wait");
        }
    }

    fn cancellation_observed(
        &self,
        event: ReadKernelCancellationEvent,
        cancellation: OwnerReadCancellation,
    ) {
        assert_eq!(event.operation, ReadSessionOperation::Execute);
        assert!(cancellation.is_cancelled());
        self.state
            .lock()
            .expect("rejection gate lock")
            .cancellation_observed = true;
    }
}

impl RejectionGate {
    fn arm(&self) {
        self.state.lock().expect("rejection gate lock").armed = true;
    }

    fn wait_at_rejection(&self) -> (ReadKernelResidualRejectionEvent, OwnerReadCancellation) {
        let mut state = self.state.lock().expect("rejection gate lock");
        while state.event.is_none() {
            state = self.changed.wait(state).expect("rejection event wait");
        }
        (
            state.event.expect("residual rejection event"),
            state.token.clone().expect("kernel cancellation token"),
        )
    }

    fn release(&self) {
        self.state.lock().expect("rejection gate lock").released = true;
        self.changed.notify_all();
    }

    fn assert_cancelled_in_kernel(&self) {
        assert!(
            self.state
                .lock()
                .expect("rejection gate lock")
                .cancellation_observed
        );
    }
}

fn wait_for_kernel_cancellation(token: &OwnerReadCancellation) {
    let landed = Arc::new((Mutex::new(token.is_cancelled()), Condvar::new()));
    let waiting = Arc::clone(&landed);
    let _listener = token.tell_on_cancel(move || {
        let (lock, changed) = &*waiting;
        *lock.lock().expect("cancellation signal lock") = true;
        changed.notify_all();
    });
    let (lock, changed) = &*landed;
    let mut cancelled = lock.lock().expect("cancellation wait lock");
    while !*cancelled {
        cancelled = changed.wait(cancelled).expect("cancellation wait");
    }
}

/// Cancellation lands after the source rejects a near candidate and before
/// replacement work, on the selected backend of each public read route.
#[cfg(unix)]
#[test]
fn cancellation_after_residual_rejection_restores_memory_on_both_routes() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("cancel-refill.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("private reader runtime");
    seed_shape(&path, &runtime, 1, 128);
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
    .expect("open residual fixture");
    admin
        .execute(
            "ALTER TABLE scoped_rows ALTER COLUMN embedding SET HNSW (EF_SEARCH = 40)",
            &HashMap::new(),
        )
        .expect("declare search breadth");
    let query = query_vector();
    let mut ranked = (0..128)
        .map(|ordinal| {
            (
                ordinal as i64 + 1,
                contextdb_vector::cosine_similarity(&query, &embedding(ordinal)),
            )
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    for (id, _) in ranked.iter().take(50) {
        admin
            .execute(
                "UPDATE scoped_rows SET is_current = FALSE WHERE id = $id",
                &HashMap::from([("id".into(), Value::Int64(*id))]),
            )
            .expect("reject a near candidate");
    }
    admin.close().expect("seal residual fixture");
    let (sql, params) = statement(1);
    let indexed = sql.replace(" LIMIT ", " USE VECTOR INDEXED LIMIT ");

    for route in [ReadRoute::Owner, ReadRoute::File] {
        let gate = Arc::new(RejectionGate::default());
        let owner = (route == ReadRoute::Owner).then(|| {
            Database::open_with_options(
                &path,
                DatabaseOpenOptions {
                    owner_reads: OwnerReadConfig {
                        runtime_dir: Some(runtime.clone()),
                        ..OwnerReadConfig::default()
                    },
                    test_kernel_observer: Some(gate.clone()),
                    ..DatabaseOpenOptions::default()
                },
            )
            .expect("open owner route")
        });
        let reader = ReadSession::with_runtime_directory_for_test(&runtime, || {
            ReadSession::open_with_kernel_observer_for_test(
                &path,
                ReadSessionOptions {
                    contexts: Some(BTreeSet::from([ContextId::new(context_id(0))])),
                    limits: limits(),
                    ..ReadSessionOptions::default()
                },
                gate.clone(),
            )
        })
        .expect("open selected route");
        let reader = Arc::new(reader);
        assert_eq!(reader.route(), route);
        let memory_used = || match &owner {
            Some(db) => db.accountant().usage().used,
            None => {
                reader
                    .file_memory_for_test()
                    .expect("file accountant")
                    .0
                    .used
            }
        };
        reader
            .execute(&indexed, &params)
            .expect("warm serving pages before charge proof");
        let baseline = memory_used();
        gate.arm();
        let cancellation = OwnerReadCancellation::new();
        let operation_cancellation = cancellation.clone();
        let executing = Arc::clone(&reader);
        let sql = indexed.clone();
        let params = params.clone();
        let worker = std::thread::spawn(move || {
            executing.execute_with_cancellation(&sql, &params, &operation_cancellation)
        });
        let (event, kernel_token) = gate.wait_at_rejection();
        assert_eq!(event.operation, ReadSessionOperation::Execute);
        assert_eq!(event.route, route);
        assert!(event.row_id.0 > 0);
        assert!(!kernel_token.is_cancelled());
        cancellation.cancel();
        wait_for_kernel_cancellation(&kernel_token);
        gate.release();
        assert!(matches!(
            worker.join().expect("indexed read worker"),
            Err(Error::ReadCancelled)
        ));
        gate.assert_cancelled_in_kernel();
        assert_eq!(
            memory_used(),
            baseline,
            "{route:?} returns the search's accountant charge"
        );
        drop(reader);
        if let Some(owner) = owner {
            owner.close().expect("close owner");
        }
    }
}
