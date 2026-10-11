#![allow(clippy::type_complexity)] // Existing compatibility and test seam shape.
//! One find is one search on every route and in every publication window.
//!
//! A Context's snapshot-compatible graph is up to three layers: the sealed
//! base, at most one sealed change beside it, and the mutable tail that holds
//! what was committed since. A find over many Contexts is answered by ONE
//! search: one bounded candidate budget spent best-first over one frontier and
//! one result heap, across every non-empty layer of every admitted Context.
//! That has to hold on the owner route exactly as on the file route, whose
//! sealed graphs are paged -- and it has to hold in
//! the ordinary window after a commit, when a Context carries a live tail. A
//! Context that falls back to its own per-layer walks spends a whole breadth
//! per layer per Context again, which is the defect the one search exists to
//! remove.
//!
//! Every cell here asks the same fixture the same questions on both routes:
//! four Contexts the reader may see, each a saved base, a sealed change and a
//! live tail; a fifth Context the reader may not see; and rows that are not
//! current. The answer must be exactly what an exhaustive comparison over the
//! same snapshot returns, from one search whose receipt shows every non-empty
//! layer was opened and whose walk stayed inside its one budget.

use contextdb_core::read_contract::{ReadLimits, ReadRoute};
use contextdb_core::types::{ContextId, VectorPartitionKey};
use contextdb_core::{Value, VectorIndexRef};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig, QueryResult, ReadSession,
    ReadSessionOptions, VectorGraphCallbackPhaseForTest,
};
use contextdb_vector::hnsw::{UnionSearchReceipt, with_union_search_receipts};
use contextdb_vector::{VectorPartitionRef, VectorStore};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, mpsc};
use std::time::Duration;
use uuid::Uuid;

const DIMENSIONS: usize = 16;
/// The Contexts every reader here may see.
const SEEN: usize = 4;
/// The fifth Context, which the statement names and no reader may see.
const HIDDEN: usize = SEEN;
const BASE_ROWS: usize = 300;
/// Past the tenth of the base the table declares as the point a change is
/// sealed, so every Context seals these into a change graph.
const CHANGE_ROWS: usize = 60;
/// Committed after the change is sealed and below the point the next one
/// would be: a live tail no maintenance consumes.
const TAIL_ROWS: usize = 12;
const LIMIT: usize = 10;
/// Row ids are `context * ID_SLOT + local + 1`.
const ID_SLOT: usize = 1_000;
/// Every seen Context carries a base, a change and a tail.
const LAYERS_PER_CONTEXT: usize = 3;
const SAFETY_TIMEOUT: Duration = Duration::from_secs(120);

/// The row whose vector is nearest the change query, planted in the second
/// Context's sealed change and nowhere else.
const CHANGE_NEAREST: (usize, usize) = (2, BASE_ROWS);
/// The row whose vector is nearest the tail query, planted in the first
/// Context's live tail and nowhere else.
const TAIL_NEAREST: (usize, usize) = (1, BASE_ROWS + CHANGE_ROWS);
/// Rows nearer each query than anything the reader may see, planted where the
/// reader must never find them: in the Context it may not see, and as rows
/// that are not current in a seen Context's change and tail.
const HIDDEN_CHANGE_ROW: (usize, usize) = (HIDDEN, BASE_ROWS);
const HIDDEN_TAIL_ROW: (usize, usize) = (HIDDEN, BASE_ROWS + CHANGE_ROWS);
const NOT_CURRENT_CHANGE_ROW: (usize, usize) = (3, BASE_ROWS + 1);
const NOT_CURRENT_TAIL_ROW: (usize, usize) = (3, BASE_ROWS + CHANGE_ROWS + 1);

fn context_id(context: usize) -> Uuid {
    Uuid::from_u128(0xC0B1_0000_0000_0000_0000_0000_0000_0000 + context as u128)
}

fn generation_id(context: usize) -> Uuid {
    Uuid::from_u128(0xC0B1_1000_0000_0000_0000_0000_0000_0000 + context as u128)
}

fn row_id((context, local): (usize, usize)) -> i64 {
    (context * ID_SLOT + local + 1) as i64
}

fn noise(ordinal: usize, axis: usize) -> f32 {
    let seed = (ordinal
        .wrapping_mul(0x9e37_79b9)
        .wrapping_add(axis.wrapping_mul(0x85eb_ca6b)))
        % 10_007;
    (seed as f32 / 10_007.0) - 0.5
}

/// A vector along `direction`, scattered by `spread`.
fn around(direction: usize, spread: f32, ordinal: usize) -> Vec<f32> {
    let mut vector = (0..DIMENSIONS)
        .map(|axis| spread * noise(ordinal, axis))
        .collect::<Vec<_>>();
    vector[direction] += 1.0;
    vector
}

/// A vector with no preferred direction.
fn scattered(ordinal: usize) -> Vec<f32> {
    (0..DIMENSIONS).map(|axis| noise(ordinal, axis)).collect()
}

fn axis(direction: usize) -> Vec<f32> {
    around(direction, 0.0, 0)
}

/// What each row is: current or not, and its vector. The seen Contexts' bases
/// are full of near-misses for both queries, so the one row nearer than all of
/// them has to be found in the layer it lives in.
fn row(context: usize, local: usize) -> (bool, Vec<f32>) {
    let ordinal = context * ID_SLOT + local;
    let at = (context, local);
    if at == CHANGE_NEAREST {
        return (true, around(0, 0.02, ordinal));
    }
    if at == TAIL_NEAREST {
        return (true, around(1, 0.02, ordinal));
    }
    if at == HIDDEN_CHANGE_ROW {
        return (true, around(0, 0.002, ordinal));
    }
    if at == HIDDEN_TAIL_ROW {
        return (true, around(1, 0.002, ordinal));
    }
    if at == NOT_CURRENT_CHANGE_ROW {
        return (false, around(0, 0.004, ordinal));
    }
    if at == NOT_CURRENT_TAIL_ROW {
        return (false, around(1, 0.004, ordinal));
    }
    if local < BASE_ROWS {
        return (true, around(local % 2, 0.35, ordinal));
    }
    (true, scattered(ordinal))
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("layered_rows", "embedding")
}

fn partition(context: usize) -> VectorPartitionRef {
    VectorPartitionRef::new(
        index(),
        VectorPartitionKey::from_values(&[
            Value::Uuid(context_id(context)),
            Value::Uuid(generation_id(context)),
        ])
        .expect("a context and generation pair is a partition key"),
    )
}

fn insert(db: &Database, context: usize, locals: std::ops::Range<usize>) {
    for local in locals {
        let (current, vector) = row(context, local);
        db.execute(
            "INSERT INTO layered_rows VALUES ($id, $context, $generation, $current, $embedding)",
            &HashMap::from([
                ("id".into(), Value::Int64(row_id((context, local)))),
                ("context".into(), Value::Uuid(context_id(context))),
                ("generation".into(), Value::Uuid(generation_id(context))),
                ("current".into(), Value::Bool(current)),
                ("embedding".into(), Value::Vector(vector)),
            ]),
        )
        .expect("seed one layered row");
    }
}

fn drive_maintenance(db: &Database, until: impl Fn(&Database) -> bool, what: &str) {
    let reached = (0..4096).any(|_| {
        db.run_maintenance_cycle()
            .expect("advance deterministic maintenance");
        until(db)
    });
    assert!(reached, "{what}");
}

/// Every Context saved as a base, a sealed change on top of it, and then a
/// live tail committed after the change. `extra_tail` more rows go into the
/// last seen Context's tail, enough that maintenance has work there.
fn seed(db: &Database, extra_tail: usize) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE layered_rows (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
             generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
             embedding VECTOR({DIMENSIONS}) PARTITION_KEY (context_id, generation_id) \
             MAX_PARTITIONS 8 SEARCH_MODE AUTO AUTO_INDEX_AT 1 \
             CONSOLIDATION (CHANGE_PERCENT = 10))"
        ),
        &HashMap::new(),
    )
    .expect("declare the layered fixture");
    db.execute(
        "CREATE INDEX layered_rows_active_idx ON layered_rows \
         (context_id, generation_id, is_current)",
        &HashMap::new(),
    )
    .expect("declare the index the candidate identities come from");
    let contexts = SEEN + 1;
    for context in 0..contexts {
        insert(db, context, 0..BASE_ROWS);
    }
    drive_maintenance(
        db,
        |db| {
            let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
                db.declared_reader_memory()
                    .expect("admit the partition observation reader"),
            );

            let ready = db
                .vector_store_for_test()
                .partition_infos(&index(), &mut partition_observation_owner_1)
                .expect("inspect partition readiness");
            ready.len() == contexts && ready.iter().all(|info| info.graph_available)
        },
        "every Context saves its base graph",
    );
    for context in 0..contexts {
        insert(db, context, BASE_ROWS..BASE_ROWS + CHANGE_ROWS);
    }
    drive_maintenance(
        db,
        |db| {
            let store = db.vector_store_for_test();
            (0..contexts).all(|context| {
                store
                    .partition_graph_generation_status(&partition(context))
                    .is_some_and(|status| {
                        (status.base.is_some() || status.dormant_base.is_some())
                            && (status.change.is_some() || status.dormant_change.is_some())
                            && status.fresh_tail_entries == 0
                    })
            })
        },
        "every Context seals its later rows into a change graph on top of its base",
    );
    let tail = BASE_ROWS + CHANGE_ROWS;
    for context in 0..contexts {
        insert(db, context, tail..tail + TAIL_ROWS);
    }
    if extra_tail != 0 {
        insert(
            db,
            SEEN - 1,
            tail + TAIL_ROWS..tail + TAIL_ROWS + extra_tail,
        );
    }
    for context in 0..contexts {
        let suffix = db
            .inspect_captured_vector_suffix_for_test(
                &partition(context),
                db.snapshot(),
                db.declared_reader_memory()
                    .expect("admit the suffix reader"),
                &mut || Ok(()),
            )
            .expect("inspect the captured suffix")
            .expect("each seeded Context has its committed suffix");
        assert!(suffix.points >= TAIL_ROWS);
        let status = db
            .vector_store_for_test()
            .partition_graph_generation_status(&partition(context))
            .expect("each seeded Context has a partition");
        assert_eq!(status.fresh_tail_entries, 0);
        assert!(
            db.vector_store_for_test()
                .with_partition_fresh_tail(&partition(context), |graph| graph.len())
                .is_none()
        );
    }
}

fn seen() -> BTreeSet<ContextId> {
    (0..SEEN)
        .map(|context| ContextId::new(context_id(context)))
        .collect()
}

/// A find naming `contexts`, the hidden one included when asked, in `mode`.
fn statement(contexts: &[usize], query: Vec<f32>, mode: &str) -> (String, HashMap<String, Value>) {
    let mut params = HashMap::from([
        ("is_current".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(query)),
    ]);
    let mut context_names = Vec::new();
    let mut generation_names = Vec::new();
    for context in contexts {
        params.insert(format!("c{context}"), Value::Uuid(context_id(*context)));
        params.insert(format!("g{context}"), Value::Uuid(generation_id(*context)));
        context_names.push(format!("$c{context}"));
        generation_names.push(format!("$g{context}"));
    }
    (
        format!(
            "SELECT id FROM layered_rows WHERE context_id IN ({}) AND generation_id IN ({}) \
             AND is_current = $is_current ORDER BY embedding <=> $query USE VECTOR {mode} \
             LIMIT {LIMIT}",
            context_names.join(", "),
            generation_names.join(", "),
        ),
        params,
    )
}

fn ids(result: &QueryResult) -> Vec<i64> {
    result
        .rows
        .iter()
        .map(|row| match row[0] {
            Value::Int64(id) => id,
            ref other => panic!("an id column holds an integer, not {other:?}"),
        })
        .collect()
}

/// The non-empty layers a Context's snapshot-compatible chain holds.
fn nonempty_layers(store: &VectorStore, context: usize) -> usize {
    let status = store
        .partition_graph_generation_status(&partition(context))
        .expect("every seeded Context has a partition");
    usize::from(status.base.is_some() || status.dormant_base.is_some())
        + usize::from(status.change.is_some() || status.dormant_change.is_some())
        // `seed` inspected the actual captured suffix under an operation
        // reader. It is deliberately not a resident mutable graph.
        + 1
}

fn every_context() -> Vec<usize> {
    (0..=HIDDEN).collect()
}

/// Ask one route every question and record what does not hold, so a failure
/// shows every cell at once.
fn check_route(
    route: &str,
    store: &VectorStore,
    execute: &dyn Fn(&str, &HashMap<String, Value>) -> contextdb_core::Result<QueryResult>,
) -> Vec<String> {
    for context in 0..SEEN {
        assert_eq!(
            nonempty_layers(store, context),
            LAYERS_PER_CONTEXT,
            "{route}: the fixture gives Context {context} a base, a change and a live tail"
        );
    }
    let expected_layers = SEEN * LAYERS_PER_CONTEXT;
    let failures = std::cell::RefCell::new(Vec::new());
    // `exact` asks for the whole page an exhaustive comparison returns. The
    // near-miss cells below ask only that the one nearest row is found where
    // it lives: their bases hold hundreds of rows at almost the same distance,
    // which is a recall question for the global search budget, not a
    // question of which layers the one search opens.
    let ask = |cell: &str,
               contexts: &[usize],
               query: Vec<f32>,
               whole_page: bool|
     -> Option<(Vec<i64>, UnionSearchReceipt)> {
        let (exact_sql, params) = statement(contexts, query.clone(), "EXACT");
        let exact = ids(&execute(&exact_sql, &params).expect("the exhaustive find answers"));
        let (sql, params) = statement(contexts, query, "INDEXED");
        let (answer, receipts) = with_union_search_receipts(|| execute(&sql, &params));
        let answer = answer.expect("the graph find answers");
        println!(
            "{route} {cell}: route={:?} receipts={receipts:?} answer={:?} exact={exact:?}",
            answer.trace.vector_search,
            ids(&answer),
        );
        if receipts.len() != 1 {
            failures.borrow_mut().push(format!(
                "{route} {cell}: one union search answers the find, but {} ran -- the \
                 Contexts were walked one partition and one layer at a time",
                receipts.len()
            ));
            return None;
        }
        let receipt = receipts[0];
        let disclosed = answer
            .trace
            .vector_search
            .as_ref()
            .expect("executed route disclosure");
        if disclosed.global_candidate_budget != Some(receipt.budget)
            || disclosed.admission_evaluations != Some(receipt.admission_evaluations)
            || disclosed.expansion_evaluations != Some(receipt.expansion_evaluations)
            || disclosed.layers_admitted != Some(receipt.layers_admitted)
            || disclosed.budget_terminal.as_deref() != Some(receipt.terminal)
        {
            failures.borrow_mut().push(format!(
                "{route} {cell}: disclosure {disclosed:?} disagrees with the walk {receipt:?}"
            ));
        }
        let answered = ids(&answer);
        let recalled = answered.iter().filter(|id| exact.contains(id)).count();
        println!("{route} {cell}: recalled {recalled} of {}", exact.len());
        if whole_page && answered != exact {
            failures.borrow_mut().push(format!(
                "{route} {cell}: the one search answers what an exhaustive comparison over the \
                 same snapshot answers: {answered:?} against {exact:?}"
            ));
        }
        if receipt.expansion_evaluations > receipt.budget {
            failures.borrow_mut().push(format!(
                "{route} {cell}: the walk stays inside its one budget: {receipt:?}"
            ));
        }
        Some((answered, receipt))
    };

    // (a) Every non-empty layer of every admitted Context is opened by the one
    // request. INDEXED gives each admitted index its declared cap, so the
    // request reports their aggregate rather than a made-up global cap.
    let open_query = scattered(999_983);
    let narrow = ask("one Context", &[0], open_query.clone(), true);
    let wide = ask("every Context", &every_context(), open_query, true);
    if let Some((_, receipt)) = &wide {
        if receipt.partitions != SEEN {
            failures.borrow_mut().push(format!(
                "{route}: every Context the reader may see is admitted, and only those: \
                 {receipt:?}"
            ));
        }
        if receipt.layers_admitted != expected_layers {
            failures.borrow_mut().push(format!(
                "{route}: the one search opens every non-empty layer -- {expected_layers} over \
                 {SEEN} Contexts of base, change and tail -- but opened {}",
                receipt.layers_admitted
            ));
        }
    }
    if let (Some((_, narrow)), Some((_, wide))) = (&narrow, &wide) {
        let per_index_cap = narrow
            .resolved_ef_search
            .saturating_mul(narrow.layer_zero_degree);
        if narrow.budget != 3_200
            || narrow.budget != per_index_cap
            || wide.budget != SEEN * per_index_cap
        {
            failures.borrow_mut().push(format!(
                "{route}: INDEXED gives each index its declared cap: expected 3,200 for one \
                 Context and {} for {SEEN}, got {} and {}; receipts {narrow:?} {wide:?}",
                SEEN * per_index_cap,
                narrow.budget,
                wide.budget
            ));
        }
    }

    // (b) The nearest row lives only in a sealed change, then only in a live
    // tail, behind bases full of near-misses. The one search finds it there.
    for (cell, query, nearest) in [
        ("nearest in a change", axis(0), CHANGE_NEAREST),
        ("nearest in a tail", axis(1), TAIL_NEAREST),
    ] {
        if let Some((answer, receipt)) = ask(cell, &every_context(), query, false) {
            if answer.first() != Some(&row_id(nearest)) {
                failures.borrow_mut().push(format!(
                    "{route} {cell}: row {} is the nearest the reader may see and heads the \
                     answer: {answer:?}",
                    row_id(nearest)
                ));
            }
            if receipt.layers_admitted != expected_layers {
                failures.borrow_mut().push(format!(
                    "{route} {cell}: every non-empty layer is opened: {receipt:?}"
                ));
            }
            // (d) Nothing from the Context the reader may not see and no row
            // that is not current, though each is nearer than the answer.
            for forbidden in [
                HIDDEN_CHANGE_ROW,
                HIDDEN_TAIL_ROW,
                NOT_CURRENT_CHANGE_ROW,
                NOT_CURRENT_TAIL_ROW,
            ] {
                if answer.contains(&row_id(forbidden)) {
                    failures.borrow_mut().push(format!(
                        "{route} {cell}: row {} is hidden from this reader or not current and \
                         never enters the answer: {answer:?}",
                        row_id(forbidden)
                    ));
                }
            }
        }
    }
    failures.into_inner()
}

#[test]
fn one_search_opens_every_layer_of_every_context_on_the_owner_route() {
    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let owner =
        Database::open(directory.path().join("owner-layers.db")).expect("open the fixture store");
    seed(&owner, 0);

    // The rows the reader must never see really are the nearest when nothing
    // hides them, so their absence below is the reader's scope at work.
    let (sql, params) = statement(&every_context(), axis(0), "EXACT");
    let unscoped = owner
        .execute(&sql.replace("AND is_current = $is_current ", ""), &params)
        .expect("the owner answers an unscoped exhaustive find");
    assert_eq!(
        ids(&unscoped).first(),
        Some(&row_id(HIDDEN_CHANGE_ROW)),
        "the hidden Context holds the nearest row of all"
    );

    let reader = owner.scoped_with_contexts(seen());
    let failures = check_route(
        "owner",
        owner.vector_store_for_test().as_ref(),
        &|sql, params| reader.execute(sql, params),
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[cfg(unix)]
#[test]
fn one_search_opens_every_layer_of_every_context_on_the_file_route() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("file-layers.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    let owner = Database::open_with_options(
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
    seed(&owner, 0);
    owner.close().expect("seal the fixture store");

    let reader = ReadSession::with_runtime_directory_for_test(&runtime, || {
        ReadSession::open_with_options(
            &path,
            ReadSessionOptions {
                contexts: Some(seen()),
                limits: ReadLimits {
                    work: 5_000_000,
                    memory: 512 * 1024 * 1024,
                    ..ReadLimits::default()
                },
                memory_limit: Some(512 * 1024 * 1024),
                ..ReadSessionOptions::default()
            },
        )
    })
    .expect("open the committed file image the consumer reads through");
    assert_eq!(reader.route(), ReadRoute::File);
    // Load every layer once so the reader's store reports what it serves.
    let (sql, params) = statement(&every_context(), axis(0), "INDEXED");
    reader
        .execute(&sql, &params)
        .expect("the first find answers");
    let store = reader
        .file_vector_store_for_test()
        .expect("a file-route reader serves its own vector store");
    let failures = check_route("file", store.as_ref(), &|sql, params| {
        reader.execute(sql, params)
    });
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A generation published after a read pinned its snapshot changes nothing
/// that read answers.
///
/// The read is held just after it pinned the first Context's layers. While it
/// waits, maintenance publishes a new generation over the last Context, which
/// the read has not reached, and a writer then commits rows nearer the query
/// than anything the read's snapshot holds. The read resumes and answers from
/// exactly the layers its snapshot selects: the same answer an exhaustive
/// comparison gave before any of it, from one search, with nothing committed
/// after its snapshot.
#[test]
fn a_generation_published_after_a_read_pinned_its_snapshot_changes_nothing_it_answers() {
    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let owner = Arc::new(
        Database::open(directory.path().join("published-over.db")).expect("open the fixture store"),
    );
    // Enough more rows in the last Context's tail that maintenance publishes
    // a new generation there; the other tails stay below that point.
    seed(&owner, 60);
    let last = partition(SEEN - 1);
    let status_before = owner
        .vector_store_for_test()
        .partition_graph_generation_status(&last)
        .expect("the last Context has a partition");
    let reader = Arc::new(owner.scoped_with_contexts(seen()));
    let (exact_sql, params) = statement(&every_context(), axis(1), "EXACT");
    let expected = ids(&reader
        .execute(&exact_sql, &params)
        .expect("the exhaustive find answers"));
    assert_eq!(expected.first(), Some(&row_id(TAIL_NEAREST)));

    let pause = owner.__arm_one_shot_vector_graph_callback_pause_for_test(
        &partition(0),
        VectorGraphCallbackPhaseForTest::BeforeSearchCallback,
    );
    let (sql, params) = statement(&every_context(), axis(1), "INDEXED");
    let (read_tx, read_rx) = mpsc::channel();
    let read_thread = {
        let reader = Arc::clone(&reader);
        std::thread::spawn(move || {
            let searched = with_union_search_receipts(|| reader.execute(&sql, &params));
            read_tx
                .send(searched)
                .expect("the read result has a receiver");
        })
    };
    if let Err(error) = pause.wait_until_reached_timeout(SAFETY_TIMEOUT) {
        pause.release();
        panic!("the read did not pin its first Context's layers in time: {error}");
    }

    let (published_tx, published_rx) = mpsc::channel();
    let maintenance_thread = {
        let owner = Arc::clone(&owner);
        let last = last.clone();
        let base_before = status_before.base.or(status_before.dormant_base);
        std::thread::spawn(move || {
            let published = (0..256).any(|_| {
                owner
                    .run_maintenance_cycle()
                    .expect("advance deterministic maintenance");
                owner
                    .vector_store_for_test()
                    .partition_graph_generation_status(&last)
                    .is_some_and(|status| status.base.or(status.dormant_base) != base_before)
            });
            published_tx
                .send(published)
                .expect("the publication result has a receiver");
        })
    };
    let published = published_rx.recv_timeout(SAFETY_TIMEOUT);
    if !matches!(published, Ok(true)) {
        pause.release();
        panic!(
            "maintenance publishes a new generation over a Context the paused read has not \
             reached: {published:?}"
        );
    }
    maintenance_thread
        .join()
        .expect("the maintenance thread exits after publishing");
    // Rows nearer the query than anything, committed after the read's snapshot.
    let late = [(SEEN - 1, 900), (SEEN - 1, 901)];
    for at in late {
        owner
            .execute(
                "INSERT INTO layered_rows VALUES ($id, $context, $generation, TRUE, $embedding)",
                &HashMap::from([
                    ("id".into(), Value::Int64(row_id(at))),
                    ("context".into(), Value::Uuid(context_id(at.0))),
                    ("generation".into(), Value::Uuid(generation_id(at.0))),
                    ("embedding".into(), Value::Vector(around(1, 0.001, at.1))),
                ]),
            )
            .expect("commit a row after the read's snapshot");
    }
    pause.release();
    let (answer, receipts) = read_rx
        .recv_timeout(SAFETY_TIMEOUT)
        .expect("the released read returns");
    read_thread.join().expect("the read thread exits");
    let answer = answer.expect("the pinned read answers");
    let answered = ids(&answer);
    println!("pinned read: receipts={receipts:?} answer={answered:?} expected={expected:?}");
    let mut failures = Vec::new();
    if receipts.len() != 1 {
        failures.push(format!(
            "one union search answers the pinned read, but {} ran -- the Contexts were walked \
             one partition and one layer at a time",
            receipts.len()
        ));
    } else if receipts[0].layers_admitted < (SEEN - 1) * LAYERS_PER_CONTEXT + 1 {
        failures.push(format!(
            "the pinned read opens every layer its snapshot selects: {:?}",
            receipts[0]
        ));
    }
    if answered != expected {
        failures.push(format!(
            "the pinned read answers from exactly its snapshot: {answered:?} against \
             {expected:?}"
        ));
    }
    for at in late {
        if answered.contains(&row_id(at)) {
            failures.push(format!(
                "row {} was committed after the read's snapshot and never enters its answer",
                row_id(at)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
