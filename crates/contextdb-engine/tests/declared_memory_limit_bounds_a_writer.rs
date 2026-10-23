//! A writer opened with a declared MEMORY_LIMIT is refused at that limit, and
//! the store stays usable.
//!
//! The promise an operator is sold is that `MEMORY_LIMIT` bounds what the
//! process holds: declare it, and an import that would need more is REFUSED
//! with a typed error instead of growing past it until a cgroup kills the
//! process. Declared memory, disk, work, time and cancellation limits remain
//! effective together.
//!
//! That promise requires the accountant to charge everything the open writer
//! retains, including the relational store's per-row side maps and each
//! index's posting structures for the life of the writer.
//!
//! The oracle here is a counting global allocator -- every byte the process
//! asks the allocator for, less every byte it gives back. That is the bill,
//! and it is not the same as the resident set: an allocator keeps a freed heap
//! mapped, so resident bytes over-state what is live, and allocator slack is
//! not something a charge model can be asked to predict.

#[path = "fixtures/isolated_process.rs"]
mod isolated_process;

use contextdb_core::Error;
use contextdb_core::Value;
use contextdb_engine::database::ReceivedSchemaApplyObserverForTest;
use contextdb_engine::memory_accounting::{ADMITTED_SCHEMA_UNIT_EXCESS_BYTES, MemoryAccountant};
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange};
use contextdb_engine::{Database, MaintenancePolicy, RetainedStructureBytes};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::io::BufRead;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};

/// The identity a fixture schema author signs with. Authenticated schema in
/// this file is signed by a real ed25519 key, the way a peer signs schema it
/// wrote, so the receiver checks authorship against the author's public key
/// rather than trusting the session the unit arrived on.
fn fixture_schema_author() -> &'static contextdb_engine::identity::FabricIdentity {
    static AUTHOR: std::sync::OnceLock<contextdb_engine::identity::FabricIdentity> =
        std::sync::OnceLock::new();
    AUTHOR.get_or_init(contextdb_engine::identity::FabricIdentity::generate)
}

/// Sign as that author, the way a transport-owned signer does.
fn fixture_signer() -> impl Fn(&[u8]) -> contextdb_core::Result<Vec<u8>> {
    |bytes: &[u8]| Ok(fixture_schema_author().sign_for_test(bytes))
}

/// Live heap bytes: everything allocated, less everything freed.
static LIVE_BYTES: AtomicIsize = AtomicIsize::new(0);
/// The highest `LIVE_BYTES` has reached since the last `restart_peak`,
/// updated inline on every allocation so a spike that is freed before the
/// caller can sample `live_bytes()` again is still caught.
static PEAK_BYTES: AtomicIsize = AtomicIsize::new(0);

// The received-schema metadata comparison runs in this test binary alongside
// other allocator tests. Keep its own peak on the caller thread so unrelated
// test threads cannot make a proportionality result depend on scheduling.
thread_local! {
    static THREAD_PEAK: Cell<Option<(i64, i64)>> = const { Cell::new(None) };
}

struct CountingAllocator;

fn note_growth(delta: isize) {
    let live = LIVE_BYTES.fetch_add(delta, Ordering::Relaxed) + delta;
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
    let _ = THREAD_PEAK.try_with(|tracked| {
        if let Some((live, peak)) = tracked.get() {
            let live = live + delta as i64;
            tracked.set(Some((live, peak.max(live))));
        }
    });
}

fn note_thread_release(bytes: usize) {
    let _ = THREAD_PEAK.try_with(|tracked| {
        if let Some((live, peak)) = tracked.get() {
            tracked.set(Some((live - bytes as i64, peak)));
        }
    });
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note_growth(layout.size() as isize);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        note_thread_release(layout.size());
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note_growth(layout.size() as isize);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note_growth(new_size as isize - layout.size() as isize);
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn live_bytes() -> i64 {
    LIVE_BYTES.load(Ordering::Relaxed) as i64
}

/// Reset the peak tracker to the current live figure, so a later
/// `peak_growth_since` reports only what happened after this call.
fn restart_peak() {
    PEAK_BYTES.store(LIVE_BYTES.load(Ordering::Relaxed), Ordering::Relaxed);
}

/// How far above `before` the real allocator rose at its highest point since
/// the last `restart_peak`, including a spike that was freed again before the
/// caller could observe it directly.
fn peak_growth_since(before: i64) -> i64 {
    PEAK_BYTES.load(Ordering::Relaxed) as i64 - before
}

fn measure_thread_peak<T>(action: impl FnOnce() -> T) -> (T, i64) {
    THREAD_PEAK.with(|tracked| {
        assert!(
            tracked.replace(Some((0, 0))).is_none(),
            "nested peak measurement"
        );
    });
    let outcome = action();
    let (_, peak) =
        THREAD_PEAK.with(|tracked| tracked.replace(None).expect("active peak measurement"));
    (outcome, peak)
}

/// A one-table authenticated ALTER must not privately copy unrelated event
/// routing and trigger metadata before its own memory admission. Route filters
/// dominate this fixture's unrelated definitions; the changed table has no
/// rows, so a whole-metadata clone cannot hide behind the table-copy cost.
#[test]
fn a_received_table_alter_does_not_clone_unrelated_event_metadata_before_admission() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_received_table_alter_does_not_clone_unrelated_event_metadata_before_admission",
    ) {
        return;
    }
    const ROUTES: usize = 8;
    const FILTER_VALUES: usize = 512;
    const VALUE_BYTES: usize = 320;
    const CHANGING_DDL: &str = "CREATE TABLE changing_metadata (id UUID PRIMARY KEY) SYNC TWO WAY";
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);
    let author = Database::open(root.path().join("event-metadata-author.db"))
        .expect("open the schema author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(CHANGING_DDL, &HashMap::new())
        .expect("author declares the changed table");
    let before_alter = author.current_lsn();
    author
        .execute(
            "ALTER TABLE changing_metadata ADD COLUMN note TEXT",
            &HashMap::new(),
        )
        .expect("author reshapes one table");
    let unit = author.changes_since(before_alter);
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &tenant,
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("author signs the one-table schema unit");
    author.close().expect("close the author");

    let mut peaks = [0_i64; 2];
    for (case, expanded) in [false, true].into_iter().enumerate() {
        let accountant = Arc::new(MemoryAccountant::no_limit());
        let receiver = Database::open_with_config(
            root.path()
                .join(format!("event-metadata-receiver-{case}.db")),
            Arc::new(contextdb_engine::plugin::CorePlugin),
            accountant.clone(),
        )
        .expect("open the receiver");
        receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        receiver
            .execute(CHANGING_DDL, &HashMap::new())
            .expect("receiver declares the changed table");

        if expanded {
            receiver
                .execute(
                    "CREATE TABLE unrelated_metadata (id UUID PRIMARY KEY, tag TEXT) SYNC OFF",
                    &HashMap::new(),
                )
                .expect("receiver declares an unrelated event table");
            let value = "x".repeat(VALUE_BYTES);
            let filter = (0..FILTER_VALUES)
                .map(|number| format!("'v{number:04}_{value}'"))
                .collect::<Vec<_>>()
                .join(", ");
            for index in 0..ROUTES {
                let event = format!("unrelated_event_{index}");
                let trigger = format!("unrelated_trigger_{index}");
                let sink = format!("unrelated_sink_{index}");
                let route = format!("unrelated_route_{index}");
                for ddl in [
                    format!("CREATE EVENT TYPE {event} WHEN INSERT ON unrelated_metadata"),
                    format!("CREATE TRIGGER {trigger} ON unrelated_metadata WHEN INSERT"),
                    format!("CREATE SINK {sink} TYPE callback"),
                    format!("CREATE ROUTE {route} EVENT {event} TO {sink} WHERE tag IN ({filter})"),
                ] {
                    receiver
                        .execute(&ddl, &HashMap::new())
                        .expect("receiver declares unrelated event metadata");
                }
                receiver
                    .register_trigger_callback(&trigger, |_db, _ctx| Ok(()))
                    .expect("register the unrelated trigger callback");
                receiver
                    .register_sink(&sink, None, |_event| Ok(()))
                    .expect("register the unrelated sink callback");
            }
            receiver
                .complete_initialization()
                .expect("all unrelated trigger callbacks are ready");
        }

        // With no room for even the changed table's metadata, admission must
        // refuse on a typed memory error and restore the exact prior bill.
        let settled = accountant.usage().used;
        receiver
            .set_memory_limit(Some(settled))
            .expect("bound the received migration at the settled bill");
        let (refusal, refused_peak) = measure_thread_peak(|| {
            receiver.apply_received_schema_for_test(
                unit.clone(),
                provenance.clone(),
                tenant.clone(),
                HELD_SOURCE,
                HELD_INCARNATION,
            )
        });
        let refusal = refusal.expect_err("the table copy must be refused at the settled bill");
        assert!(
            matches!(refusal, Error::MemoryBudgetExceeded { .. }),
            "the refusal is typed memory admission, not {refusal:?}"
        );
        assert_eq!(
            accountant.usage().used,
            settled,
            "a refused one-table unit releases every temporary charge"
        );
        if expanded {
            let unrelated_filter_bytes = ROUTES * FILTER_VALUES * VALUE_BYTES;
            assert!(
                refused_peak < unrelated_filter_bytes as i64 / 4,
                "a refused table ALTER copied {refused_peak} B before admission although its \
                 unrelated route filters alone retain {unrelated_filter_bytes} B"
            );
        }

        receiver
            .set_memory_limit(None)
            .expect("lift the limit for the same authenticated unit");
        let (applied, peak) = measure_thread_peak(|| {
            receiver.apply_received_schema_for_test(
                unit.clone(),
                provenance.clone(),
                tenant.clone(),
                HELD_SOURCE,
                HELD_INCARNATION,
            )
        });
        applied.expect("the received one-table ALTER installs with room");
        assert!(
            receiver
                .table_meta("changing_metadata")
                .expect("changed table still exists")
                .columns
                .iter()
                .any(|column| column.name == "note"),
            "the authenticated ALTER took effect"
        );
        peaks[case] = peak;
        receiver.close().expect("close the receiver");
    }
    let unrelated_filter_bytes = ROUTES * FILTER_VALUES * VALUE_BYTES;
    assert!(
        peaks[1] - peaks[0] < unrelated_filter_bytes as i64 / 4,
        "a one-table ALTER must not grow with unrelated event metadata: its thread peak \
         changed from {} B to {} B with {unrelated_filter_bytes} B of route filters",
        peaks[0],
        peaks[1]
    );
}

// The consumer's own `events` table and its five declared indexes, copied
// verbatim from the pinned cg startup schema in this tree
// (`tests/fixtures/pinned_cg_startup/mod.rs`, generated from cg's `schema.rs`).
// `contexts` comes with it because `events` declares a foreign key into it.
const CONTEXTS_DDL: &str = "CREATE TABLE contexts (id UUID PRIMARY KEY CONTEXT_ID, name TEXT NOT \
                            NULL UNIQUE, labels JSON, properties JSON, created_at TXID NOT NULL) \
                            SYNC TWO WAY SYNC CONFLICT KEEP FIRST";

const EVENTS_DDL: &str = "CREATE TABLE events (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID \
    NOT NULL REFERENCES contexts(id), connector_id TEXT NOT NULL, adapter_version TEXT NOT NULL, \
    anchor_declaration_version TEXT NOT NULL, source_instance_ref TEXT NOT NULL, session_ref TEXT \
    NOT NULL, source_event_key TEXT NOT NULL, source_position JSON NOT NULL, seq INTEGER NOT \
    NULL, content_hash TEXT NOT NULL, anchor_count INTEGER NOT NULL, anchor_set_hash TEXT NOT \
    NULL, occurred_at TIMESTAMP, author TEXT NOT NULL, kind TEXT NOT NULL, render_role TEXT NOT \
    NULL, interaction_ref TEXT, text TEXT, payload JSON NOT NULL DEFAULT '{}', anchors JSON NOT \
    NULL DEFAULT '{}', source_remainder JSON NOT NULL DEFAULT '{}', coverage TEXT NOT NULL, \
    UNIQUE (context_id, id), UNIQUE (context_id, connector_id, session_ref, source_event_key), \
    UNIQUE (context_id, connector_id, source_instance_ref, seq)) IMMUTABLE SYNC OFF SYNC \
    CONFLICT KEEP FIRST";

const EVENTS_INDEXES: &[&str] = &[
    "CREATE INDEX idx_events_source_stream ON events (context_id, connector_id, \
     source_instance_ref, seq)",
    "CREATE INDEX idx_events_session ON events (context_id, connector_id, session_ref, seq)",
    "CREATE INDEX idx_events_source_key ON events (context_id, connector_id, session_ref, \
     source_event_key)",
    "CREATE INDEX idx_events_source_time ON events (context_id, connector_id, occurred_at, seq)",
    "CREATE INDEX idx_events_author_kind ON events (context_id, author, kind, seq)",
];

/// A narrow table the store can still be asked to write after it has refused
/// an event. Nothing about it is cg-shaped; it exists to prove the refusal
/// left a usable store rather than a wedged one.
const MARKS_DDL: &str = "CREATE TABLE marks (id INTEGER PRIMARY KEY, n INTEGER NOT NULL) SYNC OFF";

const EVENT_INSERT: &str = "INSERT INTO events (id, context_id, connector_id, adapter_version, \
    anchor_declaration_version, source_instance_ref, session_ref, source_event_key, \
    source_position, seq, content_hash, anchor_count, anchor_set_hash, occurred_at, author, kind, \
    render_role, interaction_ref, text, payload, anchors, source_remainder, coverage) VALUES \
    ($id, $context_id, 'claude-code', 'v1', 'v1', $source, $session, $key, $position, $seq, \
    $hash, 1, $hash, $occurred_at, 'agent', 'message', 'primary', NULL, $text, $payload, \
    $anchors, $remainder, 'complete')";

fn context_uuid() -> uuid::Uuid {
    uuid::Uuid::from_u128(0xc0de_0000_0000_0000_0000_0000_0000_0001)
}

fn event_uuid(event: usize) -> uuid::Uuid {
    uuid::Uuid::from_u128(0xe7e7_0000_0000_0000_0000_0000_0000_0000 + event as u128)
}

fn event_params(event: usize) -> HashMap<String, Value> {
    HashMap::from([
        ("id".into(), Value::Uuid(event_uuid(event))),
        ("context_id".into(), Value::Uuid(context_uuid())),
        ("source".into(), Value::Text("source-000-0000.jsonl".into())),
        ("session".into(), Value::Text("session-000-0000".into())),
        ("key".into(), Value::Text(format!("source-{event:08x}"))),
        (
            "position".into(),
            Value::Json(serde_json::json!({ "line": event })),
        ),
        ("seq".into(), Value::Int64(event as i64)),
        ("hash".into(), Value::Text(format!("{event:064x}"))),
        ("occurred_at".into(), Value::Timestamp(1_700_000_000_000)),
        ("text".into(), Value::Text("x".repeat(512))),
        ("payload".into(), Value::Json(serde_json::json!({}))),
        ("anchors".into(), Value::Json(serde_json::json!({}))),
        ("remainder".into(), Value::Json(serde_json::json!({}))),
    ])
}

/// The cg-shaped schema, plus the one parent Context row every event needs.
/// `indexes` says how many of the five declared event indexes to install, so
/// the same fixture answers "what does ONE more index cost".
fn declare(db: &Database, indexes: usize) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for ddl in [CONTEXTS_DDL, EVENTS_DDL, MARKS_DDL] {
        db.execute(ddl, &HashMap::new())
            .unwrap_or_else(|error| panic!("declare {ddl}: {error}"));
    }
    for ddl in &EVENTS_INDEXES[..indexes] {
        db.execute(ddl, &HashMap::new())
            .unwrap_or_else(|error| panic!("declare {ddl}: {error}"));
    }
    db.execute(
        "INSERT INTO contexts (id, name, labels, properties, created_at) VALUES ($id, $name, \
         $labels, $properties, $created_at)",
        &HashMap::from([
            ("id".into(), Value::Uuid(context_uuid())),
            ("name".into(), Value::Text("declared-limit".into())),
            ("labels".into(), Value::Json(serde_json::json!([]))),
            ("properties".into(), Value::Json(serde_json::json!({}))),
            ("created_at".into(), Value::TxId(contextdb_core::TxId(1))),
        ]),
    )
    .expect("insert the parent Context row");
}

/// How many indexes the store actually carries for `events`: the five declared
/// above are only the user-declared half, and the primary key, each UNIQUE
/// constraint and each foreign key bring one of their own.
fn event_index_count(db: &Database) -> usize {
    db.table_meta("events")
        .expect("the events table is declared")
        .indexes
        .len()
}

struct PerRow {
    /// Bytes the store itself is holding, counted from its own structures.
    counted: f64,
    /// Bytes the accountant admitted.
    charged: f64,
    /// Bytes the process asked the allocator for and kept, including
    /// everything outside the relational store -- the durable writer's own
    /// working set above all.
    process: f64,
    indexes: usize,
    parts: Vec<(&'static str, f64)>,
}

fn structure_parts(
    before: RetainedStructureBytes,
    after: RetainedStructureBytes,
    rows: usize,
) -> Vec<(&'static str, f64)> {
    let per = |a: usize, b: usize| (a as f64 - b as f64) / rows as f64;
    vec![
        ("row versions", per(after.row_versions, before.row_versions)),
        (
            "row_positions",
            per(after.row_positions, before.row_positions),
        ),
        (
            "row_version_positions",
            per(after.row_version_positions, before.row_version_positions),
        ),
        (
            "sync provenance",
            per(after.sync_provenance, before.sync_provenance),
        ),
        ("index trees", per(after.index_trees, before.index_trees)),
        (
            "index exact maps",
            per(after.index_exact, before.index_exact),
        ),
        (
            "index exact filters",
            per(after.index_exact_filters, before.index_exact_filters),
        ),
        (
            "index membership",
            per(after.index_membership, before.index_membership),
        ),
    ]
}

/// What one event costs an open writer, measured over `rows` inserts: the
/// bytes the process asks the allocator for and keeps, against the bytes the
/// accountant says it admitted.
fn cost_per_event(indexes: usize, rows: usize) -> PerRow {
    cost_per_event_in(indexes, rows, Durability::FileBacked)
}

/// Whether the store under measurement keeps a durable writer. The durable
/// writer holds its own working set -- Redb's pages and the write-ahead buffer
/// it has not yet checkpointed -- which is not relational retention and which
/// no charge in this model covers. Measuring the same rows with and without it
/// is what separates the two.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Durability {
    FileBacked,
    InMemory,
}

fn cost_per_event_in(indexes: usize, rows: usize, durability: Durability) -> PerRow {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = match durability {
        Durability::FileBacked => Database::open_with_config(
            root.path().join("cost.db"),
            Arc::new(contextdb_engine::plugin::CorePlugin),
            accountant.clone(),
        )
        .expect("open the writable store"),
        Durability::InMemory => Database::open_memory_with_accountant(accountant.clone()),
    };
    // From the OPEN, so a structure the schema charges for at DDL time and
    // allocates lazily on the first posting is inside the same window as its
    // charge. Every arm carries the same schema, so the per-index slope
    // between two arms is unchanged by where the window begins.
    let live_before = live_bytes();
    let charged_before = accountant.usage().used as i64;
    let retained_before = db.retained_structure_bytes_for_test();
    declare(&db, indexes);
    let declared_indexes = event_index_count(&db);
    for event in 0..rows {
        db.execute(EVENT_INSERT, &event_params(event))
            .expect("store one event");
    }
    let retained_after = db.retained_structure_bytes_for_test();
    let counted = (retained_after.total() as f64 - retained_before.total() as f64) / rows as f64;
    let process = (live_bytes() - live_before) as f64 / rows as f64;
    let charged = (accountant.usage().used as i64 - charged_before) as f64 / rows as f64;
    db.close().expect("close the store");
    PerRow {
        counted,
        charged,
        process,
        indexes: declared_indexes,
        parts: structure_parts(retained_before, retained_after, rows),
    }
}

const PROOF_ROWS: usize = 200;

/// What the store is charged for the ROWS it holds: the accountant's total
/// net of the durable writer's own retention.
///
/// The two are different things with different lifetimes. A row's charge
/// comes back when the row does; the in-memory change log keeps the history
/// of that row being stored and deleted until a maintenance cycle trims it,
/// and the commit index keeps one entry per commit. Both are memory the store
/// is still holding, so a declared limit sees them -- but a proof about what
/// storing a row costs has to read past them or it is measuring the log.
fn charged_for_rows(db: &Database, accountant: &MemoryAccountant) -> usize {
    accountant
        .usage()
        .used
        .saturating_sub(db.__durable_writer_charged_bytes_for_test())
}

/// THE PROMISE. A writer opened with a declared `MEMORY_LIMIT` refuses the
/// import with the existing typed refusal BEFORE it retains more than that
/// limit, and the store it leaves behind still reads, still takes a smaller
/// write, and still has every committed row after a close and reopen.
#[test]
fn a_declared_memory_limit_refuses_before_the_writer_retains_more_than_it() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_declared_memory_limit_refuses_before_the_writer_retains_more_than_it",
    ) {
        return;
    }
    // Size the budget from what an event actually costs the accountant, so
    // the limit is one an operator could plausibly have set: room for about
    // PROOF_ROWS events on top of the schema.
    let sighting = cost_per_event(EVENTS_INDEXES.len(), PROOF_ROWS);
    eprintln!(
        "one cg event through {} indexes: {:.0} B retained by the store, {:.0} B charged ({:+.0} \
         B), and {:.0} B held by the whole process",
        sighting.indexes,
        sighting.counted,
        sighting.charged,
        sighting.counted - sighting.charged,
        sighting.process,
    );
    for (what, bytes) in &sighting.parts {
        eprintln!("  {what:<22} {bytes:>9.0} B/event");
    }

    // What the whole PROCESS holds, against what the accountant admitted, and
    // where the difference goes. A store with no durable writer holds the same
    // relational structures and none of Redb's pages or its unflushed write
    // buffer, so the difference between the two arms IS the durable writer's
    // working set -- the separate item the accepted design leaves open, and
    // the reason a declared limit bounds what the STORE retains rather than
    // what the process holds.
    let in_memory = cost_per_event_in(EVENTS_INDEXES.len(), PROOF_ROWS, Durability::InMemory);
    let durable_writer = sighting.process - in_memory.process;
    eprintln!(
        "the same event in a store with no durable writer: {:.0} B retained, {:.0} B charged, \
         {:.0} B held by the process. The durable writer's own working set is the difference \
         between the two process figures, {durable_writer:.0} B/event.",
        in_memory.counted, in_memory.charged, in_memory.process,
    );

    // The assertion, not a print: once the durable writer's working set is
    // taken out, what the process holds for each event is within a stated
    // slack of what the accountant admitted for it. The slack is the
    // allocator's own rounding -- it serves a request from a size class, and
    // that rounding is not something a charge model can be asked to predict.
    const ALLOCATOR_SLACK_PER_EVENT: f64 = 1_500.0;
    let unaccounted = in_memory.process - in_memory.charged;
    assert!(
        unaccounted <= ALLOCATOR_SLACK_PER_EVENT,
        "with the durable writer's working set excluded, the process holds {:.0} B per event \
         against the {:.0} B the accountant admitted -- {unaccounted:.0} B above it, past the \
         {ALLOCATOR_SLACK_PER_EVENT:.0} B of allocator rounding this measurement allows. A \
         declared MEMORY_LIMIT does not bound the rest.",
        in_memory.process,
        in_memory.charged,
    );

    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let path = root.path().join("declared-limit.db");

    // Open unlimited only long enough to learn what this schema itself costs;
    // the store under proof is a fresh one opened with the declared limit.
    let schema_charge = {
        let accountant = Arc::new(MemoryAccountant::no_limit());
        let db = Database::open_with_config(
            root.path().join("schema-cost.db"),
            Arc::new(contextdb_engine::plugin::CorePlugin),
            accountant.clone(),
        )
        .expect("open the writable store");
        declare(&db, EVENTS_INDEXES.len());
        let used = accountant.usage().used;
        db.close().expect("close the store");
        used
    };
    let headroom = (sighting.charged * PROOF_ROWS as f64) as usize;
    let limit = schema_charge + headroom;

    let accountant = Arc::new(MemoryAccountant::with_budget(limit));
    let db = Database::open_with_config(
        path.clone(),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the writable store under a declared memory limit");

    // The window opens at the OPEN, not after the schema. A structure the
    // schema declares and the accountant charges for at DDL time -- each exact
    // index's probe filter -- is allocated lazily, on the first posting that
    // lands in it, so a window that began after the schema would count the
    // allocation without the charge that already paid for it.
    let live_at_open = live_bytes();
    let charged_at_open = accountant.usage().used as i64;
    let retained_at_open = db.retained_structure_bytes_for_test();
    declare(&db, EVENTS_INDEXES.len());

    let mut stored = 0usize;
    let mut refusal = None;
    let mut live_at_refusal = live_at_open;
    let mut retained_at_refusal = retained_at_open;
    for event in 0..100_000 {
        match db.execute(EVENT_INSERT, &event_params(event)) {
            Ok(_) => {
                stored += 1;
                live_at_refusal = live_bytes();
                retained_at_refusal = db.retained_structure_bytes_for_test();
            }
            Err(error) => {
                refusal = Some(error);
                break;
            }
        }
    }

    let refusal = refusal.expect(
        "a declared memory limit sized for a few hundred events must refuse the import, not \
         admit a hundred thousand",
    );
    assert!(
        matches!(refusal, Error::MemoryBudgetExceeded { .. }),
        "the refusal must be the existing typed memory refusal, not {refusal:?}"
    );

    let counted_growth = retained_at_refusal.total() as i64 - retained_at_open.total() as i64;
    let process_growth = live_at_refusal - live_at_open;
    let charged_growth = accountant.usage().used as i64 - charged_at_open;
    eprintln!(
        "declared limit {limit} B; {stored} events stored before the refusal; the accountant \
         admitted {charged_growth} B for the schema and the import together, the store is \
         holding {counted_growth} B of it ({:+} B), and the whole process is holding \
         {process_growth} B",
        counted_growth - charged_growth,
    );
    for (what, bytes) in structure_parts(retained_at_open, retained_at_refusal, stored.max(1)) {
        eprintln!("  {what:<22} {bytes:>9.0} B/event");
    }

    // The promise itself. Every byte the writer retains has to have passed the
    // accountant, or the declared limit bounds something other than what the
    // machine has to find.
    assert!(
        counted_growth <= charged_growth,
        "the store retains {counted_growth} B for the schema and {stored} events while the \
         accountant admitted only {charged_growth} B, so a declared MEMORY_LIMIT does not bound \
         what this import holds: {} B per event are outside every charge",
        (counted_growth - charged_growth) / stored.max(1) as i64,
    );

    // ... and the store the refusal left behind is a usable one.
    let read = db
        .execute("SELECT id FROM events", &HashMap::new())
        .expect("a store that refused a write still answers a read");
    assert_eq!(
        read.rows.len(),
        stored,
        "the read must see exactly the events that were admitted"
    );
    db.execute("INSERT INTO marks (id, n) VALUES (1, 1)", &HashMap::new())
        .expect("a store that refused a large write still accepts a smaller one");

    db.close().expect("close the store");
    drop(db);

    let reopened = Database::open_with_config(
        path,
        Arc::new(contextdb_engine::plugin::CorePlugin),
        Arc::new(MemoryAccountant::no_limit()),
    )
    .expect("reopen the store the refusal left behind");
    let after = reopened
        .execute("SELECT id FROM events", &HashMap::new())
        .expect("read the reopened store");
    assert_eq!(
        after.rows.len(),
        stored,
        "every committed event must survive the close and reopen"
    );
    reopened.close().expect("close the reopened store");
}

/// What an index costs the writer, per stored row. The `events` table carries
/// nine of them in the consumer's own schema, so whether this cost is per
/// TABLE or per INDEX decides whether the uncharged share is a fixed slice or
/// grows with the schema.
#[test]
fn what_one_more_index_costs_the_writer_per_row() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "what_one_more_index_costs_the_writer_per_row",
    ) {
        return;
    }
    const ROWS: usize = 400;
    eprintln!(
        "{:<21}  {:>7}  {:>11}  {:>11}  {:>13}  {:>11}",
        "declared user indexes",
        "indexes",
        "counted/row",
        "charged/row",
        "uncharged/row",
        "process/row"
    );
    let mut measured = Vec::new();
    for declared in [0usize, 1, 3, 5] {
        let per_row = cost_per_event(declared, ROWS);
        eprintln!(
            "{declared:<21}  {:>7}  {:>11.0}  {:>11.0}  {:>13.0}  {:>11.0}",
            per_row.indexes,
            per_row.counted,
            per_row.charged,
            per_row.counted - per_row.charged,
            per_row.process,
        );
        measured.push(per_row);
    }
    let first = &measured[0];
    let last = &measured[measured.len() - 1];
    let added_indexes = (last.indexes - first.indexes) as f64;
    eprintln!(
        "{} more indexes add {:.0} B counted and {:.0} B charged per row: {:.0} B counted and \
         {:.0} B charged per index per row",
        added_indexes,
        last.counted - first.counted,
        last.charged - first.charged,
        (last.counted - first.counted) / added_indexes,
        (last.charged - first.charged) / added_indexes,
    );
    assert!(
        last.charged - first.charged > 0.0,
        "adding {added_indexes} indexes to a table must cost the accountant something per row; it \
         charges {:.0} B more",
        last.charged - first.charged,
    );
}

/// A charge that is taken and never given back is a leak the operator sees as
/// a store that refuses work it has room for, so every charge added here has
/// to come back exactly when the structure it paid for goes away. Fill,
/// delete every row, and fill again: the empty store has to be back at what it
/// charged before the first row, and the refilled store back at what it
/// charged after it. A transaction that is rolled back has to leave no trace
/// either.
#[test]
fn every_charge_a_row_takes_comes_back_when_the_row_does() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "every_charge_a_row_takes_comes_back_when_the_row_does",
    ) {
        return;
    }
    const ROWS: usize = 120;
    const DDL: &str = "CREATE TABLE notes (id UUID PRIMARY KEY, context_id UUID NOT NULL,                        author TEXT NOT NULL, kind TEXT NOT NULL, seq INTEGER NOT NULL, body                        TEXT NOT NULL, UNIQUE (context_id, seq)) SYNC OFF";
    const INSERT: &str = "INSERT INTO notes (id, context_id, author, kind, seq, body) VALUES                           ($id, $context_id, $author, $kind, $seq, $body)";

    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_with_config(
        root.path().join("drift.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the writable store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(DDL, &HashMap::new()).expect("declare notes");
    for index in [
        "CREATE INDEX notes_by_author ON notes (context_id, author, seq)",
        "CREATE INDEX notes_by_kind ON notes (context_id, kind, seq)",
    ] {
        db.execute(index, &HashMap::new())
            .expect("declare the index");
    }

    let params = |row: usize| {
        HashMap::from([
            ("id".to_owned(), Value::Uuid(event_uuid(row))),
            ("context_id".to_owned(), Value::Uuid(context_uuid())),
            ("author".to_owned(), Value::Text(format!("author-{row:04}"))),
            ("kind".to_owned(), Value::Text("message".to_owned())),
            ("seq".to_owned(), Value::Int64(row as i64)),
            ("body".to_owned(), Value::Text("b".repeat(256))),
        ])
    };
    let fill = || {
        for row in 0..ROWS {
            db.execute(INSERT, &params(row)).expect("store one note");
        }
    };

    let empty = charged_for_rows(&db, &accountant);
    fill();
    let full = charged_for_rows(&db, &accountant);
    assert!(
        full > empty,
        "filling a table with {ROWS} indexed rows must charge something"
    );

    db.execute("DELETE FROM notes", &HashMap::new())
        .expect("delete every note");
    let emptied = charged_for_rows(&db, &accountant);
    assert_eq!(
        emptied,
        empty,
        "deleting every row must give back exactly what storing them took: {} B are still \
         charged for a table with no rows in it",
        emptied as i64 - empty as i64,
    );

    fill();
    let refilled = charged_for_rows(&db, &accountant);
    assert_eq!(
        refilled,
        full,
        "the same rows must cost the same the second time: the refilled store is charged {} B \
         against the first fill's {full} B",
        refilled as i64 - full as i64,
    );

    // And a transaction that never commits leaves nothing behind.
    let tx = db
        .begin()
        .expect("begin a transaction that will not commit");
    for row in ROWS..(ROWS + 20) {
        db.execute_in_tx(tx, INSERT, &params(row))
            .expect("stage one note");
    }
    db.rollback(tx).expect("roll the transaction back");
    assert_eq!(
        charged_for_rows(&db, &accountant),
        full,
        "a rolled-back transaction must leave the charge exactly where it found it"
    );
    db.close().expect("close the store");
}

// ---------------------------------------------------------------------------
// What a change to a table's INDEX SET costs the rows already in it.
// ---------------------------------------------------------------------------

/// Every door that can add or remove an index over a table that already has
/// rows. A row's charge counts one posting per declared index, so each of
/// these changes what every row in the table costs, and the door that makes
/// the change is the only place that can settle the difference.
#[derive(Clone, Copy, Debug)]
enum IndexSetDoor {
    LocalCreateIndex,
    LocalDropIndex,
    ReceivedCreateIndex,
    ReceivedDropIndex,
    ReceivedAlterTableUnique,
    CloseAndReopen,
    /// A CREATE INDEX issued INSIDE a transaction. It takes the transactional
    /// schema commit rather than the local statement path, and that path
    /// prices only the table's declaration -- while the publication it
    /// assembles builds one posting for every row already in the table.
    TransactionalCreateIndex,
}

const NOTES_DDL: &str = "CREATE TABLE notes (id UUID PRIMARY KEY, context_id UUID NOT NULL, \
                         author TEXT NOT NULL, kind TEXT NOT NULL, seq INTEGER NOT NULL, body \
                         TEXT NOT NULL, UNIQUE (context_id, seq)) SYNC TWO WAY";
const NOTES_INSERT: &str = "INSERT INTO notes (id, context_id, author, kind, seq, body) VALUES \
                            ($id, $context_id, $author, $kind, $seq, $body)";
const NOTES_EXTRA_INDEX: &str = "notes_by_author";
const NOTES_EXTRA_COLUMNS: &str = "(context_id, author, seq)";

fn note_params(row: usize) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_owned(), Value::Uuid(event_uuid(row))),
        ("context_id".to_owned(), Value::Uuid(context_uuid())),
        ("author".to_owned(), Value::Text(format!("author-{row:04}"))),
        ("kind".to_owned(), Value::Text("message".to_owned())),
        ("seq".to_owned(), Value::Int64(row as i64)),
        ("body".to_owned(), Value::Text("b".repeat(256))),
    ])
}

fn extra_index_columns() -> Vec<(String, contextdb_core::SortDirection)> {
    ["context_id", "author", "seq"]
        .into_iter()
        .map(|column| (column.to_owned(), contextdb_core::SortDirection::Asc))
        .collect()
}

fn received(ddl: DdlChange) -> ChangeSet {
    ChangeSet {
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: vec![ddl],
        ddl_lsn: vec![contextdb_core::Lsn(1)],
    }
}

/// Runs one door and reports what the accountant holds for a table with no
/// rows in it afterwards. `rows` says whether the table was filled and emptied
/// again first: with the charge settled correctly the two answers are the same
/// number, because an empty table costs what an empty table costs however it
/// got that way.
fn charged_for_an_empty_table_after(door: IndexSetDoor, rows: usize) -> usize {
    charged_for_an_empty_table_after_with_staging(door, rows, 0)
}

/// As above, and with `staged` rows held in an OPEN transaction ACROSS the
/// door. Those rows were admitted under the old index set and are released
/// under the new one, so a settlement that reads only committed rows misses
/// them in both directions.
fn charged_for_an_empty_table_after_with_staging(
    door: IndexSetDoor,
    rows: usize,
    staged: usize,
) -> usize {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let path = root.path().join("index-set.db");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let mut db = Database::open_with_config(
        path.clone(),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the writable store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(NOTES_DDL, &HashMap::new())
        .expect("declare notes");
    if matches!(
        door,
        IndexSetDoor::LocalDropIndex | IndexSetDoor::ReceivedDropIndex
    ) {
        db.execute(
            &format!("CREATE INDEX {NOTES_EXTRA_INDEX} ON notes {NOTES_EXTRA_COLUMNS}"),
            &HashMap::new(),
        )
        .expect("declare the index this door removes");
    }

    for row in 0..rows {
        db.execute(NOTES_INSERT, &note_params(row))
            .expect("store one note");
    }
    let open_tx = (staged > 0).then(|| {
        let tx = db
            .begin()
            .expect("begin the transaction held across the door");
        for row in rows..(rows + staged) {
            db.execute_in_tx(tx, NOTES_INSERT, &note_params(row))
                .expect("stage one note across the door");
        }
        tx
    });

    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    match door {
        IndexSetDoor::LocalCreateIndex => {
            db.execute(
                &format!("CREATE INDEX {NOTES_EXTRA_INDEX} ON notes {NOTES_EXTRA_COLUMNS}"),
                &HashMap::new(),
            )
            .expect("CREATE INDEX over the rows already stored");
        }
        IndexSetDoor::LocalDropIndex => {
            db.execute(
                &format!("DROP INDEX {NOTES_EXTRA_INDEX} ON notes"),
                &HashMap::new(),
            )
            .expect("DROP INDEX over the rows already stored");
        }
        IndexSetDoor::ReceivedCreateIndex => {
            db.apply_changes(
                received(DdlChange::CreateIndex {
                    table: "notes".to_owned(),
                    name: NOTES_EXTRA_INDEX.to_owned(),
                    columns: extra_index_columns(),
                }),
                &policies,
            )
            .expect("a CREATE INDEX arriving over sync");
        }
        IndexSetDoor::ReceivedDropIndex => {
            db.apply_changes(
                received(DdlChange::DropIndex {
                    table: "notes".to_owned(),
                    name: NOTES_EXTRA_INDEX.to_owned(),
                }),
                &policies,
            )
            .expect("a DROP INDEX arriving over sync");
        }
        IndexSetDoor::ReceivedAlterTableUnique => {
            db.apply_changes(
                received(DdlChange::AlterTable {
                    name: "notes".to_owned(),
                    columns: [
                        ("id", "UUID"),
                        ("context_id", "UUID"),
                        ("author", "TEXT"),
                        ("kind", "TEXT"),
                        ("seq", "INTEGER"),
                        ("body", "TEXT"),
                    ]
                    .into_iter()
                    .map(|(name, kind)| (name.to_owned(), kind.to_owned()))
                    .collect(),
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: vec![vec!["context_id".to_owned(), "author".to_owned()]],
                }),
                &policies,
            )
            .expect("an ALTER TABLE arriving over sync that adds a constraint index");
        }
        IndexSetDoor::TransactionalCreateIndex => {
            let tx = db.begin().expect("begin the schema transaction");
            if let Err(error) = db.execute_in_tx(
                tx,
                &format!("CREATE INDEX {NOTES_EXTRA_INDEX} ON notes {NOTES_EXTRA_COLUMNS}"),
                &HashMap::new(),
            ) {
                db.rollback(tx).expect("roll back the refused schema");
                panic!("CREATE INDEX inside a transaction: {error}");
            }
            db.commit(tx).expect("commit the schema transaction");
        }
        IndexSetDoor::CloseAndReopen => {
            db.close().expect("close the store");
            db = Database::open_with_config(
                path,
                Arc::new(contextdb_engine::plugin::CorePlugin),
                accountant.clone(),
            )
            .expect("reopen the store");
            db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        }
    }

    if let Some(tx) = open_tx {
        db.commit(tx)
            .expect("commit the transaction held across the door");
    }
    if rows + staged > 0 {
        db.execute("DELETE FROM notes", &HashMap::new())
            .expect("delete every note");
    }
    let used = charged_for_rows(&db, &accountant);
    db.close().expect("close the store");
    used
}

/// THE PROMISE. A declared `MEMORY_LIMIT` keeps bounding the store after the
/// schema changes, whichever door the change came through -- a local
/// statement, a schema push or pull, or a reopen.
///
/// It cannot, unless the door that changes a table's index set also settles
/// what that change does to the rows already in the table. Since a row is
/// charged one posting per declared index, adding an index over N existing
/// rows adds N postings the accountant never saw, and deleting those rows
/// afterwards hands back the larger figure: the accountant drifts DOWN and the
/// limit stops bounding. A removed index drifts it the other way, into
/// refusing work the store has room for.
#[test]
fn changing_the_index_set_over_existing_rows_settles_what_its_rows_now_cost() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "changing_the_index_set_over_existing_rows_settles_what_its_rows_now_cost",
    ) {
        return;
    }
    const ROWS: usize = 60;
    let mut drifted = Vec::new();
    for door in [
        IndexSetDoor::LocalCreateIndex,
        IndexSetDoor::LocalDropIndex,
        IndexSetDoor::ReceivedCreateIndex,
        IndexSetDoor::ReceivedDropIndex,
        IndexSetDoor::ReceivedAlterTableUnique,
        IndexSetDoor::CloseAndReopen,
        IndexSetDoor::TransactionalCreateIndex,
    ] {
        let never_had_rows = charged_for_an_empty_table_after(door, 0);
        let filled_and_emptied = charged_for_an_empty_table_after(door, ROWS);
        let drift = filled_and_emptied as i64 - never_had_rows as i64;
        eprintln!(
            "{door:<26}  empty {never_had_rows:>8} B   filled and emptied {filled_and_emptied:>8} \
             B   drift {drift:>+9} B ({:+} B/row)",
            drift / ROWS as i64,
            door = format!("{door:?}"),
        );
        if drift != 0 {
            drifted.push((format!("{door:?}"), drift, drift / ROWS as i64));
        }
    }
    assert!(
        drifted.is_empty(),
        "a table with no rows in it must cost what a table with no rows in it costs, however it \
         got there; these doors left the accountant somewhere else: {drifted:?}"
    );
}

// ---------------------------------------------------------------------------
// Every door that gives a row back.
// ---------------------------------------------------------------------------

/// How a stored row stops being stored. Each of these releases the charge the
/// row took when it was admitted, and each has to release the SAME figure:
/// a release that hands back less than admission took strands bytes the store
/// is no longer holding, and an operator sees a limit refuse an import it has
/// room for.
#[derive(Clone, Copy, Debug)]
enum ReleaseDoor {
    Delete,
    TransactionRollback,
    RefusedStatement,
    DropTableAfterCommit,
    CloseWithAnInsertingTransactionStillOpen,
}

/// Opens a store, creates `notes`, takes `door`, removes whatever is left, and
/// reports what the accountant holds at the start and at the end. A store that
/// has given everything back holds what it held before any of it existed.
fn charged_before_and_after(door: ReleaseDoor, rows: usize) -> (usize, usize) {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_with_config(
        root.path().join("release.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the writable store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);

    let before = charged_for_rows(&db, &accountant);
    db.execute(NOTES_DDL, &HashMap::new())
        .expect("declare notes");
    db.execute(
        &format!("CREATE INDEX {NOTES_EXTRA_INDEX} ON notes {NOTES_EXTRA_COLUMNS}"),
        &HashMap::new(),
    )
    .expect("declare the index");

    let mut table_is_gone = false;
    match door {
        ReleaseDoor::Delete => {
            for row in 0..rows {
                db.execute(NOTES_INSERT, &note_params(row))
                    .expect("store one note");
            }
            db.execute("DELETE FROM notes", &HashMap::new())
                .expect("delete every note");
        }
        ReleaseDoor::TransactionRollback => {
            let tx = db.begin().expect("begin");
            for row in 0..rows {
                db.execute_in_tx(tx, NOTES_INSERT, &note_params(row))
                    .expect("stage one note");
            }
            db.rollback(tx).expect("roll the transaction back");
        }
        ReleaseDoor::RefusedStatement => {
            for row in 0..rows {
                db.execute(NOTES_INSERT, &note_params(row))
                    .expect("store one note");
            }
            // The same natural key twice: the second statement is refused and
            // must leave nothing of itself charged.
            for row in 0..rows {
                db.execute(NOTES_INSERT, &note_params(row))
                    .expect_err("a duplicate primary key must be refused");
            }
            db.execute("DELETE FROM notes", &HashMap::new())
                .expect("delete every note");
        }
        ReleaseDoor::DropTableAfterCommit => {
            for row in 0..rows {
                db.execute(NOTES_INSERT, &note_params(row))
                    .expect("store one note");
            }
            db.execute("DROP TABLE notes", &HashMap::new())
                .expect("drop the table with its rows in it");
            table_is_gone = true;
        }
        ReleaseDoor::CloseWithAnInsertingTransactionStillOpen => {
            let tx = db.begin().expect("begin");
            for row in 0..rows {
                db.execute_in_tx(tx, NOTES_INSERT, &note_params(row))
                    .expect("stage one note");
            }
            db.close().expect("close with the transaction still open");
            // The handle's own drop is the last release: everything the store
            // held -- the staged inserts the close handed back, and the
            // schema -- has to be back with the accountant once it is gone.
            // The close already handed back the durable writer's own
            // retention, so this total is directly comparable to `before`,
            // which was read net of it.
            drop(db);
            let after = accountant.usage().used;
            return (before, after);
        }
    }

    if !table_is_gone {
        db.execute("DROP TABLE notes", &HashMap::new())
            .expect("drop the empty table");
    }
    let after = charged_for_rows(&db, &accountant);
    db.close().expect("close the store");
    (before, after)
}

/// THE PROMISE. A store that has given everything back holds what it held
/// before any of it existed, whichever door gave it back.
///
/// Charge and release go through one function so that this holds by
/// construction. The one place it can stop holding is the fallback each
/// release path takes when a table's metadata is already gone; the engine
/// refuses the statement that would reach it from here (dropping a table
/// inside the transaction that filled it is not a supported schema staging
/// operation), so that door is proven at the function itself --
/// `fallback_retained_row_bytes` in `database.rs`, and its unit test there.
#[test]
fn every_door_that_gives_a_row_back_returns_what_storing_it_took() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "every_door_that_gives_a_row_back_returns_what_storing_it_took",
    ) {
        return;
    }
    const ROWS: usize = 40;
    let mut stranded = Vec::new();
    for door in [
        ReleaseDoor::Delete,
        ReleaseDoor::TransactionRollback,
        ReleaseDoor::RefusedStatement,
        ReleaseDoor::DropTableAfterCommit,
        ReleaseDoor::CloseWithAnInsertingTransactionStillOpen,
    ] {
        let (before, after) = charged_before_and_after(door, ROWS);
        let left = after as i64 - before as i64;
        eprintln!(
            "{door:<48}  before {before:>8} B   after {after:>8} B   left {left:>+9} B ({:+} \
             B/row)",
            left / ROWS as i64,
            door = format!("{door:?}"),
        );
        if left != 0 {
            stranded.push((format!("{door:?}"), left, left / ROWS as i64));
        }
    }
    assert!(
        stranded.is_empty(),
        "these doors did not give back what storing the rows took: {stranded:?}"
    );
}

// ---------------------------------------------------------------------------
// The exact-probe filter: charged where it is allocated, once.
// ---------------------------------------------------------------------------

/// A declared index that shares another index's physical storage costs nothing
/// more, and a table is charged for the filters it actually allocated -- not
/// for one per declaration, and not for four while a schema commits.
#[test]
fn a_table_is_charged_for_the_exact_probe_filters_it_actually_holds() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_table_is_charged_for_the_exact_probe_filters_it_actually_holds",
    ) {
        return;
    }
    const DDL: &str = "CREATE TABLE holders (id UUID PRIMARY KEY, owner UUID NOT NULL UNIQUE, \
                       note TEXT NOT NULL) SYNC OFF";
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_with_config(
        root.path().join("filters.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the writable store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);

    let before = accountant.usage().used;
    db.execute(DDL, &HashMap::new()).expect("declare holders");
    let charged = accountant.usage().used - before;
    let held = db.retained_structure_bytes_for_test().index_exact_filters;
    eprintln!(
        "declaring one table charged {charged} B and allocated {held} B of exact-probe filter \
         ({} filters)",
        held / contextdb_core::EXACT_PROBE_FILTER_BYTES,
    );

    assert!(
        charged >= held,
        "the declaration must charge for every filter it allocated: charged {charged} B against \
         {held} B held"
    );
    assert!(
        charged - held < contextdb_core::EXACT_PROBE_FILTER_BYTES,
        "and must not charge for a filter it did not allocate: {} B above what it holds, which is \
         a whole filter or more",
        charged - held,
    );
    db.close().expect("close the store");
}

/// A schema installed in one transaction reserves what it keeps, not a
/// multiple of it.
///
/// The transactional schema commit sizes its staging reservation as four times
/// the table metadata. That is a fair figure for metadata, which really is
/// copied while a projection is built. It is not a fair figure for an index's
/// 64 KiB exact-probe filter, which is allocated once and kept; multiplying it
/// turns a six-table startup schema into more than a megabyte of reservation
/// against a declared limit, and refuses work the machine has room for.
#[test]
fn a_schema_installed_in_one_transaction_reserves_what_it_keeps() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_schema_installed_in_one_transaction_reserves_what_it_keeps",
    ) {
        return;
    }
    const TABLES: usize = 6;
    /// Room for the genuine transient staging of a six-table schema commit, and
    /// not room for three more copies of every filter in it.
    const STAGING_SLACK: usize = 512 * 1024;
    // A store's Redb page cache is charged at open against the limit it was
    // opened under, so a budget sized from what the SCHEMA retains has to name
    // it as its own term -- exactly as it names the staging a projection needs.
    let ddl = |ordinal: usize| {
        format!(
            "CREATE TABLE part_{ordinal} (id UUID PRIMARY KEY, owner UUID NOT NULL, note TEXT \
             NOT NULL) SYNC OFF"
        )
    };

    // What the same schema costs when each statement stands alone.
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let measured = Database::open_with_config(
        root.path().join("statement-by-statement.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the writable store");
    measured.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for ordinal in 0..TABLES {
        measured
            .execute(&ddl(ordinal), &HashMap::new())
            .expect("declare one table on its own");
    }
    let retained = accountant.usage().used;
    measured.close().expect("close the measuring store");

    // The same schema, in one transaction, under a limit that has room for it
    // and for the staging a projection genuinely needs.
    let limit = retained + STAGING_SLACK + contextdb_engine::page_cache_floor_bytes();
    let transactional = Arc::new(MemoryAccountant::with_budget(limit));
    let db = Database::open_with_config(
        root.path().join("one-transaction.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        transactional.clone(),
    )
    .expect("open the writable store under a declared memory limit");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let tx = db.begin().expect("begin the schema transaction");
    for ordinal in 0..TABLES {
        if let Err(error) = db.execute_in_tx(tx, &ddl(ordinal), &HashMap::new()) {
            db.rollback(tx).expect("roll back the refused schema");
            panic!("declaring table {ordinal} inside the transaction: {error}");
        }
    }
    let outcome = db.commit(tx);
    eprintln!(
        "a {TABLES}-table schema retains {retained} B; installed in one transaction under a \
         {limit} B limit the commit answered {:?}",
        outcome.as_ref().map(|_| ()),
    );
    outcome.expect(
        "a schema that fits in the declared limit with half a megabyte to spare must install in \
         one transaction",
    );
    db.close().expect("close the store");
}

// ---------------------------------------------------------------------------
// The durable writer's own working set.
// ---------------------------------------------------------------------------

/// What the DURABLE writer holds beside the store, per event, as an import
/// grows.
///
/// The relational store's retention is charged and bounded. The durable writer
/// holds more: Redb's page cache, the in-memory change log every commit
/// appends to, the commit index, the content-identity proof records a
/// transaction carries. Whether that is a fixed cost or one that grows with
/// every event until the writer closes decides whether a declared
/// `MEMORY_LIMIT` bounds an import at all, so it is measured at three scales
/// rather than asserted at one: a per-event figure that holds across them is
/// linear growth, and one that falls is something with a ceiling.
#[test]
fn what_the_durable_writer_holds_per_event_as_an_import_grows() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "what_the_durable_writer_holds_per_event_as_an_import_grows",
    ) {
        return;
    }
    const SCALES: [usize; 3] = [300, 900, 2_700];
    eprintln!(
        "{:<8}  {:>12}  {:>12}  {:>14}  {:>16}",
        "events", "charged/ev", "durable/ev", "in-memory/ev", "durable total"
    );
    let mut measured = Vec::new();
    for events in SCALES {
        let durable = cost_per_event_in(EVENTS_INDEXES.len(), events, Durability::FileBacked);
        let in_memory = cost_per_event_in(EVENTS_INDEXES.len(), events, Durability::InMemory);
        let writer_per_event = durable.process - in_memory.process;
        eprintln!(
            "{events:<8}  {:>12.0}  {:>12.0}  {:>14.0}  {:>16.0}",
            durable.charged,
            writer_per_event,
            in_memory.process,
            writer_per_event * events as f64,
        );
        measured.push((events, writer_per_event));
    }
    let (small_events, small) = measured[0];
    let (large_events, large) = measured[measured.len() - 1];
    let small_total = small * small_events as f64;
    let large_total = large * large_events as f64;
    eprintln!(
        "over {}x the events the durable writer's total grew {:.1}x ({small_total:.0} B to \
         {large_total:.0} B) and its per-event figure went from {small:.0} to {large:.0}. A \
         per-event figure that HOLDS is linear growth for as long as the writer is open; one \
         that FALLS is approaching a ceiling.",
        large_events / small_events,
        large_total / small_total.max(1.0),
    );
    assert!(
        large > 0.0 && small > 0.0,
        "the durable writer must cost the process something, or this measurement is broken \
         rather than the finding"
    );
}

/// THE PROMISE, at the process rather than at the store. A writer opened with
/// a declared `MEMORY_LIMIT` is refused before the PROCESS holds more than
/// that limit -- which is the number a cgroup kills for.
///
/// Measured at two limits a decade apart, because the question is not whether
/// one store fits under one allowance: it is whether what the process holds
/// ABOVE the limit GROWS with the import. A fixed allowance larger than the
/// limit it guards cannot answer that, and neither can a single store size.
///
/// It measured 5,718 B per event above the limit at 159 events and 5,846 B at
/// 1,679 -- an overage that does not fall, so it was not the cost of opening
/// a store. Those bytes were the durable writer's own working set: the
/// in-memory change log, its two side indexes and the transaction manager's
/// commit index, none of which passed a `try_allocate_for` and all of which
/// are trimmed only by a maintenance cycle the caller drives. They are
/// charged now, so what remains above the limit is the fixed cost of having a
/// store open -- Redb's own page cache, which Redb caps and evicts at 8 MiB.
#[test]
fn a_durable_writer_is_refused_before_the_process_holds_more_than_its_limit() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_durable_writer_is_refused_before_the_process_holds_more_than_its_limit",
    ) {
        return;
    }
    /// A store costs something to open whatever it then stores. This is that
    /// one-time cost, and it is the only fixed allowance this test gives:
    /// everything else it permits has to fall per event as the import grows.
    const OPEN_ALLOWANCE: f64 = 8.0 * 1024.0 * 1024.0;
    /// The process figure is the whole process's live heap, which carries
    /// allocator rounding no accountant can see. The observed allowance is
    /// about 1,500 B per event on this fixture, so a per-event overage inside
    /// it is rounding rather than retention that grows.
    const ALLOCATOR_ROUNDING_PER_EVENT: f64 = 1_500.0;

    let sighting = cost_per_event(EVENTS_INDEXES.len(), 200);
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let schema_charge = {
        let accountant = Arc::new(MemoryAccountant::no_limit());
        let db = Database::open_with_config(
            root.path().join("process-schema-cost.db"),
            Arc::new(contextdb_engine::plugin::CorePlugin),
            accountant.clone(),
        )
        .expect("open the writable store");
        declare(&db, EVENTS_INDEXES.len());
        let used = accountant.usage().used;
        db.close().expect("close the store");
        used
    };

    let mut measured = Vec::new();
    for (arm, events) in [("short", 150usize), ("ten-times-longer", 1_500usize)] {
        let limit = schema_charge + (sighting.charged * events as f64) as usize;
        let accountant = Arc::new(MemoryAccountant::with_budget(limit));
        let db = Database::open_with_config(
            root.path().join(format!("process-limit-{arm}.db")),
            Arc::new(contextdb_engine::plugin::CorePlugin),
            accountant.clone(),
        )
        .expect("open the writable store under a declared memory limit");
        let live_at_open = live_bytes();
        declare(&db, EVENTS_INDEXES.len());

        let mut stored = 0usize;
        let mut live_at_refusal = live_at_open;
        let mut refused = false;
        for event in 0..1_000_000 {
            match db.execute(EVENT_INSERT, &event_params(event)) {
                Ok(_) => {
                    stored += 1;
                    live_at_refusal = live_bytes();
                }
                Err(_) => {
                    refused = true;
                    break;
                }
            }
        }
        assert!(
            refused,
            "{arm}: the import must be refused, not admitted in full"
        );
        let process_growth = (live_at_refusal - live_at_open) as f64;
        let overage = process_growth - limit as f64;
        eprintln!(
            "{arm:<18} limit {limit:>12} B   {stored:>6} events stored   process \
             {process_growth:>12.0} B   over the limit {overage:>+12.0} B ({:+.0} B/event)",
            overage / stored.max(1) as f64,
        );
        measured.push((arm, stored, overage));
        db.close().expect("close the store");
    }

    // The limit is effective only if what the process holds above it is a
    // FIXED cost of having a store open. Per event it has to FALL as the
    // import gets longer; a figure that holds is linear growth the limit does
    // not see.
    let (short_arm, short_events, short_over) = measured[0];
    let (long_arm, long_events, long_over) = measured[1];
    let short_per_event = short_over / short_events.max(1) as f64;
    let long_per_event = long_over / long_events.max(1) as f64;
    eprintln!(
        "overage per event: {short_per_event:.0} B at {short_events} events ({short_arm}), \
         {long_per_event:.0} B at {long_events} ({long_arm})"
    );
    assert!(
        long_over <= OPEN_ALLOWANCE,
        "{long_arm}: the process holds {long_over:.0} B above the declared limit after \
         {long_events} events, past the {OPEN_ALLOWANCE:.0} B a store may cost to open. A \
         declared MEMORY_LIMIT does not bound this writer."
    );
    assert!(
        long_per_event <= short_per_event.max(ALLOCATOR_ROUNDING_PER_EVENT),
        "the process holds {long_per_event:.0} B per event above the limit at {long_events} \
         events against {short_per_event:.0} B at {short_events}: the overage GROWS with the \
         import, so it is not the cost of opening a store -- it is memory no declared limit \
         sees. (A figure at or below zero is the limit doing its job, and one inside \
         {ALLOCATOR_ROUNDING_PER_EVENT:.0} B is the allocator's own rounding: only a real \
         positive overage has to fall as the import gets longer.)"
    );
}

/// THE SAME PROMISE, for a transaction that is OPEN while the index set
/// changes under it.
///
/// A staged row is admitted under the index set the table had when the
/// statement ran, and released under the set it has when the transaction ends.
/// A settlement that re-prices only the COMMITTED rows misses it in both
/// directions: one posting per row too many after a CREATE INDEX that arrives
/// mid-transaction, one too few after a DROP INDEX.
#[test]
fn a_transaction_open_across_an_index_set_change_settles_the_rows_it_staged() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_transaction_open_across_an_index_set_change_settles_the_rows_it_staged",
    ) {
        return;
    }
    const COMMITTED: usize = 40;
    const STAGED: usize = 20;
    let mut drifted = Vec::new();
    for door in [
        IndexSetDoor::ReceivedCreateIndex,
        IndexSetDoor::ReceivedDropIndex,
        IndexSetDoor::LocalCreateIndex,
        IndexSetDoor::LocalDropIndex,
    ] {
        let never_had_rows = charged_for_an_empty_table_after(door, 0);
        let with_staging = charged_for_an_empty_table_after_with_staging(door, COMMITTED, STAGED);
        let drift = with_staging as i64 - never_had_rows as i64;
        eprintln!(
            "{door:<26}  empty {never_had_rows:>8} B   {COMMITTED} committed + {STAGED} staged \
             across the door, then emptied {with_staging:>8} B   drift {drift:>+8} B ({:+} \
             B/staged row)",
            drift / STAGED as i64,
            door = format!("{door:?}"),
        );
        if drift != 0 {
            drifted.push((format!("{door:?}"), drift, drift / STAGED as i64));
        }
    }
    assert!(
        drifted.is_empty(),
        "a row staged while the index set changed under it must be settled like any other row: \
         {drifted:?}"
    );
}

/// A received schema group installs atomically: after a memory refusal, the
/// durable schema and receipt must still describe the complete old state.
/// If the limit admits an early index but refuses a later one, publishing that
/// prefix would leave a schema that is neither the old state nor the received
/// group. The whole group must be admitted before any part becomes durable.
#[test]
fn a_received_schema_group_refused_for_memory_installs_none_of_itself() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_received_schema_group_refused_for_memory_installs_none_of_itself",
    ) {
        return;
    }
    // Enough rows that what the table retains dwarfs the store's fixed open
    // cost, so a limit with room for two of the three arriving indexes and not
    // one index over them is a figure the accountant can tell apart from the
    // store's own open cost.
    const ROWS: usize = 150;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_with_config(
        root.path().join("group.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the writable store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(NOTES_DDL, &HashMap::new())
        .expect("declare notes");
    for row in 0..ROWS {
        db.execute(NOTES_INSERT, &note_params(row))
            .expect("store one note");
    }

    // What ONE index over these rows costs, so the limit can be set to admit
    // two of the group's three and not the third.
    let before_one = accountant.usage().used;
    db.execute(
        &format!("CREATE INDEX sizing_probe ON notes {NOTES_EXTRA_COLUMNS}"),
        &HashMap::new(),
    )
    .expect("declare the sizing index");
    let one_index = accountant.usage().used - before_one;
    db.execute("DROP INDEX sizing_probe ON notes", &HashMap::new())
        .expect("drop the sizing index");

    let settled = accountant.usage().used;
    db.set_memory_limit(Some(settled + one_index * 2))
        .expect("declare a limit with room for two of the three indexes");

    let group = ChangeSet {
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: ["group_one", "group_two", "group_three"]
            .into_iter()
            .map(|name| DdlChange::CreateIndex {
                table: "notes".to_owned(),
                name: name.to_owned(),
                columns: extra_index_columns(),
            })
            .collect(),
        ddl_lsn: vec![
            contextdb_core::Lsn(1),
            contextdb_core::Lsn(2),
            contextdb_core::Lsn(3),
        ],
    };
    // The price the receiver puts on the whole unit has to cover what its items
    // will really cost, or the refusal never fires and the store overshoots the
    // limit instead. Pinned directly, against the figure the accountant measured
    // for one index over these rows.
    let priced = db.projected_sync_ddl_unit_bytes_for_test(&group.ddl);
    assert!(
        priced >= one_index * 3,
        "the unit's price ({priced} B) must cover what its three indexes really cost \
         ({} B, {one_index} B each)",
        one_index * 3,
    );

    let outcome = db.apply_changes(
        group,
        &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
    );
    let error = outcome.expect_err(
        "a limit with room for two of three indexes must refuse the group that declares three",
    );
    assert!(
        matches!(error, Error::MemoryBudgetExceeded { .. }),
        "the refusal must be the typed memory one, not {error:?}"
    );

    let installed = db
        .table_meta("notes")
        .expect("the table is still declared")
        .indexes
        .iter()
        .filter(|index| index.name.starts_with("group_"))
        .map(|index| index.name.clone())
        .collect::<Vec<_>>();
    assert!(
        installed.is_empty(),
        "a received unit refused for memory must leave the old complete state, not a proper \
         subset of itself; these were installed and kept: {installed:?}"
    );
    db.close().expect("close the store");
}

const WIDE_DDL: &str = "CREATE TABLE wide (id UUID PRIMARY KEY, body TEXT NOT NULL) SYNC TWO WAY";
const WIDE_INSERT: &str = "INSERT INTO wide (id, body) VALUES ($id, $body)";

/// Rows whose indexed column is WIDE and distinct, so an index over it holds far
/// more than the table's narrow primary-key index does -- which is the shape the
/// received unit's price gets wrong.
fn wide_params(row: usize) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_owned(), Value::Uuid(event_uuid(row))),
        (
            "body".to_owned(),
            Value::Text(format!("{row:08}-{}", "w".repeat(512))),
        ),
    ])
}

/// A DECLARED LIMIT BOUNDS THE PROCESS EVEN WHEN THE PRICE OF AN ARRIVING UNIT
/// IS WRONG.
///
/// An already-admitted unit installs whole or not at all, so an allocation its
/// price did not foresee is absorbed rather than refused half way through. But
/// what the price can miss is decided by the PEER: an arriving index is priced
/// at the AVERAGE of what the table's existing indexes hold, so one wider than
/// those is under-priced by the difference on every row, and the row count is
/// the peer's too. Absorbing that without a ceiling hands the peer the
/// operator's declared limit: the store is left permanently over it -- the
/// bytes are retained, not transient -- and from then on every ordinary
/// statement is refused until a human raises the limit, by an amount the peer
/// chose.
///
/// So what an admitted unit may take past what it was priced at is bounded by
/// `ADMITTED_SCHEMA_UNIT_EXCESS_BYTES`. A unit whose under-modelled part is
/// bigger than that is refused, and installs none of itself.
#[test]
fn a_received_unit_priced_far_too_low_is_refused_not_installed_past_the_limit() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_received_unit_priced_far_too_low_is_refused_not_installed_past_the_limit",
    ) {
        return;
    }
    // Enough wide rows that the index the unit brings is under-priced by more
    // than the bound -- asserted below rather than assumed.
    const ROWS: usize = 400;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_with_config(
        root.path().join("wide.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the writable store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(WIDE_DDL, &HashMap::new())
        .expect("declare the wide table");
    for row in 0..ROWS {
        db.execute(WIDE_INSERT, &wide_params(row))
            .expect("store one wide row");
    }

    // What an index over that wide column REALLY costs this store, measured on
    // its own rows rather than guessed.
    let before_probe = accountant.usage().used;
    db.execute("CREATE INDEX sizing_probe ON wide (body)", &HashMap::new())
        .expect("declare the sizing index");
    let real = accountant.usage().used - before_probe;
    db.execute("DROP INDEX sizing_probe ON wide", &HashMap::new())
        .expect("drop the sizing index");
    let settled = accountant.usage().used;

    let unit = ChangeSet {
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: vec![DdlChange::CreateIndex {
            table: "wide".to_owned(),
            name: "wide_by_body".to_owned(),
            columns: vec![("body".to_owned(), contextdb_core::SortDirection::Asc)],
        }],
        ddl_lsn: vec![contextdb_core::Lsn(1)],
    };
    let priced = db.projected_sync_ddl_unit_bytes_for_test(&unit.ddl);
    assert!(
        real > priced + ADMITTED_SCHEMA_UNIT_EXCESS_BYTES,
        "this proof needs a unit the price gets wrong by MORE than the bound: the index really \
         costs {real} B, the unit is priced at {priced} B, and the bound is \
         {ADMITTED_SCHEMA_UNIT_EXCESS_BYTES} B"
    );

    // A limit the unit's own price fits exactly. The preflight admits the unit,
    // so only the apply discovers what the price missed.
    let limit = settled + priced;
    db.set_memory_limit(Some(limit))
        .expect("declare a limit the arriving unit's own price fits");

    let error = db
        .apply_changes(
            unit,
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .expect_err("a unit under-priced by more than the bound must be refused, not absorbed");
    assert!(
        matches!(error, Error::MemoryBudgetExceeded { .. }),
        "the refusal must be the typed memory one, not {error:?}"
    );
    assert!(
        db.table_meta("wide")
            .expect("the table is still declared")
            .indexes
            .iter()
            .all(|index| index.name != "wide_by_body"),
        "a refused unit installs none of itself"
    );
    // And the promise the bound exists for: what the operator declared still
    // bounds the process. The most a unit may leave behind past that limit is
    // the bound, not whatever the peer's rows and index widths came to.
    let held = accountant.usage().used;
    assert!(
        held <= limit + ADMITTED_SCHEMA_UNIT_EXCESS_BYTES,
        "the store is {held} B over a declared limit of {limit} B, which is more than the \
         {ADMITTED_SCHEMA_UNIT_EXCESS_BYTES} B an admitted unit may take past its price"
    );
    db.close().expect("close the store");
}

/// A RESHAPED TABLE IS EITHER RESHAPED OR NOT, AND A REFUSED UNIT LEAVES NO
/// TRACE OF ITSELF ANYWHERE.
///
/// Bounding what an admitted unit may take past its price made a refusal
/// reachable in the MIDDLE of an item, which it never was while the absorption
/// was unconditional. An item that has already changed the table's shape in
/// memory and is then refused leaves the receiver serving a shape no author
/// ever sent it -- a proper subset of an arriving schema, which is the one
/// outcome the design forbids outright.
///
/// So the refusal has to leave three things as they were: the shape the table
/// had, the replicated DDL log, and what the store reads back after a restart.
/// The log is the one that must never be repaired by writing to it -- a
/// compensating DDL entry would replicate this node's memory shortfall to every
/// peer as a schema revert -- so the only correct answer is that nothing was
/// written to it at all.
#[test]
fn a_received_alter_priced_far_too_low_leaves_the_old_shape_the_log_and_the_restart() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_received_alter_priced_far_too_low_leaves_the_old_shape_the_log_and_the_restart",
    ) {
        return;
    }
    const ROWS: usize = 400;
    let root = tempfile::tempdir().expect("task-scoped fixture root");

    // The author declares the same table and reshapes it, so the unit the
    // receiver is sent is one a real author really emits.
    let author = Database::open(root.path().join("alter-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(WIDE_DDL, &HashMap::new())
        .expect("author declares the wide table");
    let before_unit = author.current_lsn();
    author
        .execute(
            "ALTER TABLE wide ADD COLUMN embedding VECTOR(3)",
            &HashMap::new(),
        )
        .expect("author reshapes the table");
    let unit = author.changes_since(before_unit);
    assert!(
        unit.ddl
            .iter()
            .any(|change| matches!(change, DdlChange::AlterTable { .. })),
        "the unit has to be an ALTER TABLE to reach the arm under test: {:?}",
        unit.ddl
    );
    author.close().expect("close the author");

    let path = root.path().join("alter-receiver.db");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_with_config(
        path.clone(),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the receiver");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(WIDE_DDL, &HashMap::new())
        .expect("receiver declares the wide table");
    for row in 0..ROWS {
        db.execute(WIDE_INSERT, &wide_params(row))
            .expect("receiver stores one wide row");
    }
    let settled = accountant.usage().used;

    // A limit the unit's own price fits exactly, so the preflight admits it and
    // only the apply discovers what reshaping 400 rows really costs.
    let priced = db.projected_sync_ddl_unit_bytes_for_test(&unit.ddl);
    let limit = settled + priced;
    db.set_memory_limit(Some(limit))
        .expect("declare a limit the arriving unit's own price fits");

    let columns_of = |db: &Database| {
        let mut names = db
            .table_meta("wide")
            .expect("the table is declared")
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names
    };
    let shape_before = columns_of(&db);
    let log_before = format!("{:?}", db.ddl_log_since(contextdb_core::Lsn(0)));

    let error = db
        .apply_changes(
            unit,
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .expect_err("a reshape that costs far more than its price must be refused");
    assert!(
        matches!(error, Error::MemoryBudgetExceeded { .. }),
        "the refusal must be the typed memory one, not {error:?}"
    );

    assert_eq!(
        columns_of(&db),
        shape_before,
        "a refused unit installs none of itself: the table still has the shape it had"
    );
    assert_eq!(
        format!("{:?}", db.ddl_log_since(contextdb_core::Lsn(0))),
        log_before,
        "the replicated DDL log is exactly what it was before the unit arrived -- nothing was \
         appended by the refused item, and nothing was written to take it back"
    );

    // And the answer survives a restart, which is the only check an operator
    // can make: in-memory state that disagrees with what is on disk shows up
    // here. The limit goes up first, because reopening a store holding 400 wide
    // rows needs more than the deliberately tight figure the refusal was
    // provoked with -- that reopen floor is its own open question and not what
    // this proof is about.
    db.set_memory_limit(Some(limit + 8 * 1024 * 1024))
        .expect("raise the limit so the store can be reopened at all");
    db.close().expect("close the receiver");
    let reopened = Database::open(&path).expect("the receiver reopens");
    assert!(
        reopened.table_meta("narrow").is_none(),
        "and no item of the refused unit comes back after a restart"
    );
    assert_eq!(
        columns_of(&reopened),
        shape_before,
        "the store reads back the old complete schema, not a shape the refusal left behind"
    );
    reopened.close().expect("close the reopened receiver");
}

/// A TINY DECLARED LIMIT LEAVES AN EMPTY STORE ABLE TO ANSWER, AND TO RAISE
/// ITSELF.
///
/// A file-backed store holds Redb's page cache for as long as it is open, and
/// that cache has a floor -- below it every buffered page write flushes
/// synchronously. Flooring it UNCONDITIONALLY broke the contract that an
/// operator may declare a limit below what the store currently holds and meet
/// the refusal at the NEXT admission: a store declaring 1 KiB was handed a
/// 256 KiB cache, charged for it, and was over budget the moment it opened, so
/// it could not serve even the read that would raise the limit again.
///
/// The floor is capped at the limit's own share of itself. So a tiny limit gets
/// a tiny cache -- never zero, and never more than the limit budgets for it.
///
/// This covers an EMPTY store, and the name says so because the difference
/// decides the answer. This fixture declares its limit before it has a table,
/// so the reopen has no table metadata to admit. A store that already holds one
/// table with a single-column primary key must admit about 66 KiB at open -- a
/// 64 KiB exact-probe bitmap plus the table's own metadata -- and a limit below
/// that refuses the open outright, which leaves no way to read the store or to
/// raise the limit again. That floor is open work, not something this proof
/// covers.
#[test]
fn a_tiny_declared_limit_still_opens_an_empty_store_that_answers_and_can_raise_itself() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_tiny_declared_limit_still_opens_an_empty_store_that_answers_and_can_raise_itself",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let path = root.path().join("tiny-limit.db");

    let db = Database::open(&path).expect("open the store");
    // 128 KiB is below the ordinary 256 KiB page-cache floor; the cache must
    // shrink to fit the declaration before schema is read.
    db.execute("SET MEMORY_LIMIT '128K'", &HashMap::new())
        .expect("a limit below the cache floor is still a limit an operator may declare");
    db.close().expect("close the store");

    // Reopened under its own persisted limit. The cache it is handed is at most
    // the limit's share of itself, so the cache alone does not put the store over
    // budget and it still reads its own schema and answers.
    let reopened = Database::open(&path).expect("a store reopens under its own tiny limit");
    let shown = reopened
        .execute("SHOW MEMORY_LIMIT", &HashMap::new())
        .expect("the store answers a read under its tiny limit");
    assert_eq!(shown.rows[0][0], Value::Int64(128 * 1024));

    // And the statement that raises the limit still goes through, which is the
    // way back in an unconditional cache floor took away.
    reopened
        .execute("SET MEMORY_LIMIT '8M'", &HashMap::new())
        .expect("a small limit is recoverable: the store can still raise it");
    reopened
        .execute(
            "CREATE TABLE tiny (id INTEGER PRIMARY KEY, note TEXT)",
            &HashMap::new(),
        )
        .expect("and then take a write");
    reopened
        .execute("INSERT INTO tiny VALUES (1, 'one')", &HashMap::new())
        .expect("and store a row");
    assert_eq!(
        reopened
            .execute("SELECT id FROM tiny", &HashMap::new())
            .expect("and read it back")
            .rows
            .len(),
        1
    );
    reopened.close().expect("close the reopened store");
}

/// THE SAME PROMISE for a unit whose items are of DIFFERENT KINDS.
///
/// The unit's price used to cover only the metadata each item installs, and the
/// unwind that was tried instead covered only its CREATE/DROP INDEX items -- so
/// a refusal at a later item left an already-applied CREATE TABLE or ALTER
/// TABLE installed and durable. The whole unit is priced now, item kind by item
/// kind, and refused ahead of all of them.
#[test]
fn a_mixed_kind_received_unit_refused_for_memory_installs_none_of_itself() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_mixed_kind_received_unit_refused_for_memory_installs_none_of_itself",
    ) {
        return;
    }
    const ROWS: usize = 150;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_with_config(
        root.path().join("mixed.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the writable store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(NOTES_DDL, &HashMap::new())
        .expect("declare notes");
    for row in 0..ROWS {
        db.execute(NOTES_INSERT, &note_params(row))
            .expect("store one note");
    }

    // One index over these rows, so the limit can be set to admit less than the
    // unit needs without being so small that the store cannot work at all.
    let before_one = accountant.usage().used;
    db.execute(
        &format!("CREATE INDEX sizing_probe ON notes {NOTES_EXTRA_COLUMNS}"),
        &HashMap::new(),
    )
    .expect("declare the sizing index");
    let one_index = accountant.usage().used - before_one;
    db.execute("DROP INDEX sizing_probe ON notes", &HashMap::new())
        .expect("drop the sizing index");
    let settled = accountant.usage().used;
    db.set_memory_limit(Some(settled + one_index / 2))
        .expect("declare a limit with room for less than one of the unit's indexes");

    let unit = ChangeSet {
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: vec![
            DdlChange::CreateTable {
                name: "mixed_child".to_owned(),
                columns: vec![
                    ("id".to_owned(), "UUID PRIMARY KEY".to_owned()),
                    ("label".to_owned(), "TEXT NOT NULL".to_owned()),
                ],
                constraints: Vec::new(),
                foreign_keys: Vec::new(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            },
            DdlChange::CreateIndex {
                table: "notes".to_owned(),
                name: "mixed_notes_index".to_owned(),
                columns: extra_index_columns(),
            },
            DdlChange::AlterTable {
                name: "notes".to_owned(),
                columns: vec![
                    ("id".to_owned(), "UUID PRIMARY KEY".to_owned()),
                    ("context_id".to_owned(), "UUID NOT NULL".to_owned()),
                    ("author".to_owned(), "TEXT NOT NULL".to_owned()),
                    ("kind".to_owned(), "TEXT NOT NULL".to_owned()),
                    ("seq".to_owned(), "INTEGER NOT NULL".to_owned()),
                    ("body".to_owned(), "TEXT NOT NULL".to_owned()),
                    ("tag".to_owned(), "TEXT".to_owned()),
                ],
                constraints: Vec::new(),
                foreign_keys: Vec::new(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            },
        ],
        ddl_lsn: vec![
            contextdb_core::Lsn(1),
            contextdb_core::Lsn(2),
            contextdb_core::Lsn(3),
        ],
    };

    let error = db
        .apply_changes(
            unit,
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .expect_err("a limit that cannot hold the whole mixed unit refuses it");
    assert!(
        matches!(error, Error::MemoryBudgetExceeded { .. }),
        "the refusal must be the typed memory one, not {error:?}"
    );

    assert!(
        db.table_meta("mixed_child").is_none(),
        "the unit's CREATE TABLE must not be installed when the unit is refused"
    );
    let notes = db.table_meta("notes").expect("notes is still declared");
    assert!(
        !notes
            .indexes
            .iter()
            .any(|index| index.name == "mixed_notes_index"),
        "the unit's CREATE INDEX must not be installed when the unit is refused"
    );
    assert!(
        !notes.columns.iter().any(|column| column.name == "tag"),
        "the unit's ALTER TABLE must not be installed when the unit is refused"
    );
    db.close().expect("close the store");
}

const LABELS_DDL: &str =
    "CREATE TABLE labels (id UUID PRIMARY KEY, label TEXT NOT NULL) SYNC TWO WAY";
const LABELS_INSERT: &str = "INSERT INTO labels (id, label) VALUES ($id, $label)";

fn label_params(row: usize) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_owned(), Value::Uuid(event_uuid(row))),
        ("label".to_owned(), Value::Text(format!("label-{row:04}"))),
    ])
}

/// AN ARRIVING INDEX THE DECLARED LIMIT CAN HOLD IS INSTALLED, NOT PARKED.
///
/// A unit the receiver's limit has room for must install. Being refused and
/// parked in a durable hold stops that table tracking its author and tells the
/// operator to raise a limit that was already big enough.
///
/// The exact-probe bitmap is what made that happen. A table's metadata estimate
/// already carries 64 KiB for every single-column automatic index it has, and
/// the arriving index was ALSO priced at the average of what the table's
/// existing indexes hold -- which contains that same bitmap. So an arriving
/// user-declared index, which allocates no bitmap at all, was charged one; and
/// an arriving automatic one was charged two. This table has exactly one index,
/// its automatic primary key, so the whole 64 KiB lands on the arriving index.
#[test]
fn a_received_index_the_declared_limit_can_hold_is_installed_rather_than_parked() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_received_index_the_declared_limit_can_hold_is_installed_rather_than_parked",
    ) {
        return;
    }
    const ROWS: usize = 60;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    let author = Database::open(root.path().join("labels-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(LABELS_DDL, &HashMap::new())
        .expect("author declares the labels table");
    for row in 0..ROWS {
        author
            .execute(LABELS_INSERT, &label_params(row))
            .expect("author stores one label row");
    }
    let before_unit = author.current_lsn();
    author
        .execute(
            "CREATE INDEX labels_by_label ON labels (label)",
            &HashMap::new(),
        )
        .expect("author declares the index the receiver will be sent");
    let unit = author.changes_since(before_unit);
    assert_eq!(unit.ddl.len(), 1, "the unit is the one index");
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs its own schema unit");

    // What taking delivery of that unit really costs a receiver, measured on an
    // identical store with no limit at all. The price the receiver puts on the
    // unit is the thing under test, so it cannot also be the yardstick the
    // limit is set from.
    let unlimited_accountant = Arc::new(MemoryAccountant::no_limit());
    let unlimited = Database::open_with_config(
        root.path().join("labels-unlimited.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        unlimited_accountant.clone(),
    )
    .expect("open the unlimited receiver");
    unlimited.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    unlimited
        .execute(LABELS_DDL, &HashMap::new())
        .expect("the unlimited receiver declares the labels table");
    for row in 0..ROWS {
        unlimited
            .execute(LABELS_INSERT, &label_params(row))
            .expect("the unlimited receiver stores one label row");
    }
    let before_apply = unlimited_accountant.usage().used;
    unlimited
        .apply_received_schema_for_test(
            unit.clone(),
            provenance.clone(),
            tenant.clone(),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect("a receiver with no limit takes the unit");
    // At its WIDEST point, not at what it settles to: taking delivery holds more
    // while it runs than it keeps, and a limit has to cover the wider figure or
    // the apply is refused part way through for a reason that has nothing to do
    // with the price.
    let widest = unlimited_accountant.high_water_for_test() - before_apply;
    unlimited.close().expect("close the unlimited receiver");

    let accountant = Arc::new(MemoryAccountant::no_limit());
    let receiver = Database::open_with_config(
        root.path().join("labels-receiver.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(LABELS_DDL, &HashMap::new())
        .expect("receiver declares the labels table");
    for row in 0..ROWS {
        receiver
            .execute(LABELS_INSERT, &label_params(row))
            .expect("receiver stores one label row");
    }

    let settled = accountant.usage().used;

    // Room for everything taking delivery of that unit really holds, and 16 KiB
    // over. So the limit fits the true cost with slack and does not fit a whole
    // spurious 64 KiB bitmap on top of it.
    let limit = settled + widest + contextdb_core::EXACT_PROBE_FILTER_BYTES / 4;
    receiver
        .set_memory_limit(Some(limit))
        .expect("declare a limit with room for the arriving unit");

    receiver
        .apply_received_schema_for_test(
            unit,
            provenance,
            tenant.clone(),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect("an index the declared limit can hold must install, not be parked in a hold");
    assert!(
        receiver
            .table_meta("labels")
            .expect("the labels table is still declared")
            .indexes
            .iter()
            .any(|index| index.name == "labels_by_label"),
        "the arriving index installed"
    );
    assert!(
        receiver
            .memory_held_received_schema_units(&tenant)
            .expect("the receiver reports what it is holding")
            .is_empty(),
        "a unit that fits is not held: nothing should be waiting for memory"
    );
    receiver.close().expect("close the receiver");
    author.close().expect("close the author");
}

/// AN INSTALLED UNIT IS NEVER REPORTED TO ITS AUTHOR AS REFUSED.
///
/// A failure answer means the receiver kept its old state. Once the unit is
/// committed and durable that is no longer true, so nothing after the commit
/// may turn the answer into an error.
///
/// Clearing the memory hold the unit was waiting behind is one of those things.
/// It is a durable write, and it can fail -- a full disk, or the config write
/// itself being refused under a limit the unit has just filled. Failing the
/// apply for it costs the author twice over: the pull cursor never advances, so
/// the unit the receiver is already holding is offered again forever, and the
/// hold this call was clearing stays behind to refuse every later unit for that
/// table. So the release is best effort, the same way recording the hold is,
/// and a hold left behind is cleared by the next delivery for that table.
#[test]
fn a_received_unit_that_installed_is_reported_applied_even_when_its_hold_will_not_clear() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_received_unit_that_installed_is_reported_applied_even_when_its_hold_will_not_clear",
    ) {
        return;
    }
    const ROWS: usize = 150;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    let author = Database::open(root.path().join("author.db")).expect("open the author store");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(NOTES_DDL, &HashMap::new())
        .expect("author declares notes");
    for row in 0..ROWS {
        author
            .execute(NOTES_INSERT, &note_params(row))
            .expect("author stores one note");
    }
    let before_unit = author.current_lsn();
    for name in ["group_one", "group_two", "group_three"] {
        author
            .execute(
                &format!("CREATE INDEX {name} ON notes {NOTES_EXTRA_COLUMNS}"),
                &HashMap::new(),
            )
            .expect("author declares one of the unit's indexes");
    }
    let unit = author.changes_since(before_unit);
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs its own schema unit");

    let accountant = Arc::new(MemoryAccountant::no_limit());
    let receiver = Database::open_with_config(
        root.path().join("receiver.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the receiver store");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(NOTES_DDL, &HashMap::new())
        .expect("receiver declares notes");
    for row in 0..ROWS {
        receiver
            .execute(NOTES_INSERT, &note_params(row))
            .expect("receiver stores one note");
    }
    let before_one = accountant.usage().used;
    receiver
        .execute(
            &format!("CREATE INDEX sizing_probe ON notes {NOTES_EXTRA_COLUMNS}"),
            &HashMap::new(),
        )
        .expect("declare the sizing index");
    let one_index = accountant.usage().used - before_one;
    receiver
        .execute("DROP INDEX sizing_probe ON notes", &HashMap::new())
        .expect("drop the sizing index");
    let settled = accountant.usage().used;
    let tight = settled + one_index * 2;
    receiver
        .set_memory_limit(Some(tight))
        .expect("declare a limit with room for two of the three indexes");

    receiver
        .apply_received_schema_for_test(
            unit.clone(),
            provenance.clone(),
            tenant.clone(),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect_err("the unit does not fit yet, so it is refused and held");
    assert_eq!(
        receiver
            .memory_held_received_schema_units(&tenant)
            .expect("the receiver reports what it is holding")
            .len(),
        1,
        "the table is held"
    );

    // Room for the unit now, and the durable hold record refusing every write
    // the way a full disk does.
    // Room for the unit AND for taking delivery of it. Taking delivery copies
    // every table the unit reshapes into a private working image before it
    // publishes anything, and that copy is charged to this receiver's declared
    // limit -- so the limit has to cover a second copy of what the affected
    // tables already hold, not just the shape the unit settles to.
    receiver
        .set_memory_limit(Some(tight + one_index * 4 + settled))
        .expect("raise the limit above what the held unit needs to arrive and to install");
    Database::refuse_memory_schema_holdback_writes_for_test(true);
    let applied = receiver.apply_received_schema_for_test(
        unit.clone(),
        provenance.clone(),
        tenant.clone(),
        HELD_SOURCE,
        HELD_INCARNATION,
    );
    Database::refuse_memory_schema_holdback_writes_for_test(false);
    applied.expect(
        "the unit is committed and durable, so its author is told it landed -- a hold record that \
         cannot be written must not turn an installed unit into a refusal",
    );
    let installed = receiver
        .table_meta("notes")
        .expect("notes is still declared")
        .indexes
        .iter()
        .filter(|index| index.name.starts_with("group_"))
        .count();
    assert_eq!(
        installed, 3,
        "all three of the unit's indexes are installed"
    );

    // The hold that could not be cleared is still on record, and the next
    // delivery for that table clears it rather than queueing behind it.
    assert_eq!(
        receiver
            .memory_held_received_schema_units(&tenant)
            .expect("the receiver reports what it is holding")
            .len(),
        1,
        "the hold outlived the apply, because clearing it was best effort"
    );
    receiver
        .apply_received_schema_for_test(
            unit,
            provenance,
            tenant.clone(),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect("the next delivery for that table is not queued behind the stale hold");
    assert!(
        receiver
            .memory_held_received_schema_units(&tenant)
            .expect("the receiver reports what it is holding")
            .is_empty(),
        "and it clears the hold the failed write left behind"
    );
    receiver.close().expect("close the receiver");
    author.close().expect("close the author");
}

/// The receiver, its author, and the two schema units this pair of proofs
/// drives: the one that does not fit, and a LATER one for the same table.
struct HeldTablePair {
    author: Database,
    receiver: Database,
    receiver_path: std::path::PathBuf,
    tight: usize,
    one_index: usize,
    unit: ChangeSet,
    provenance: Vec<contextdb_engine::protocol::WireDdlProvenance>,
    later_unit: ChangeSet,
    later_provenance: Vec<contextdb_engine::protocol::WireDdlProvenance>,
}

/// An author and a receiver holding the same table and the same rows, the
/// receiver declaring a limit with room for two of the three indexes the first
/// unit brings -- and a second, LATER unit for that same table waiting behind
/// it.
fn a_receiver_holding_a_table_back_for_memory(root: &std::path::Path) -> HeldTablePair {
    const ROWS: usize = 150;

    let author = Database::open(root.join("author.db")).expect("open the author store");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(NOTES_DDL, &HashMap::new())
        .expect("author declares notes");
    for row in 0..ROWS {
        author
            .execute(NOTES_INSERT, &note_params(row))
            .expect("author stores one note");
    }

    let before_unit = author.current_lsn();
    for name in ["group_one", "group_two", "group_three"] {
        author
            .execute(
                &format!("CREATE INDEX {name} ON notes {NOTES_EXTRA_COLUMNS}"),
                &HashMap::new(),
            )
            .expect("author declares one of the unit's indexes");
    }
    let unit = author.changes_since(before_unit);
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs its own schema unit");

    let before_later = author.current_lsn();
    author
        .execute("CREATE INDEX group_four ON notes (kind)", &HashMap::new())
        .expect("author declares a LATER index on the same table");
    let later_unit = author.changes_since(before_later);
    assert_eq!(later_unit.ddl.len(), 1, "the later unit is the one index");
    let later_provenance = author
        .outbound_ddl_provenance_for_test(
            &later_unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs the later unit");

    let receiver_path = root.join("receiver.db");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let receiver = Database::open_with_config(
        receiver_path.clone(),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the receiver store");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(NOTES_DDL, &HashMap::new())
        .expect("receiver declares notes");
    for row in 0..ROWS {
        receiver
            .execute(NOTES_INSERT, &note_params(row))
            .expect("receiver stores one note");
    }
    let before_one = accountant.usage().used;
    receiver
        .execute(
            &format!("CREATE INDEX sizing_probe ON notes {NOTES_EXTRA_COLUMNS}"),
            &HashMap::new(),
        )
        .expect("declare the sizing index");
    let one_index = accountant.usage().used - before_one;
    receiver
        .execute("DROP INDEX sizing_probe ON notes", &HashMap::new())
        .expect("drop the sizing index");
    let settled = accountant.usage().used;
    let tight = settled + one_index * 2;
    receiver
        .set_memory_limit(Some(tight))
        .expect("declare a limit with room for two of the three indexes");

    receiver
        .apply_received_schema_for_test(
            unit.clone(),
            provenance.clone(),
            contextdb_core::TenantId::from(HELD_TENANT),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect_err(
            "a limit with room for two of three indexes refuses the unit that declares three",
        );

    HeldTablePair {
        author,
        receiver,
        receiver_path,
        tight,
        one_index,
        unit,
        provenance,
        later_unit,
        later_provenance,
    }
}

/// What the refusal a held table gives a later unit has to say.
fn holdback_refusal_operation(refusal: &Error) -> &str {
    match refusal {
        Error::MemoryBudgetExceeded { operation, .. } => operation,
        other => panic!("the refusal must be the typed memory one, not {other:?}"),
    }
}

/// A LATER UNIT FOR A HELD TABLE WAITS FOR THE HELD ONE, AND SAYS SO.
///
/// The hold is only worth having if it actually queues. A receiver that could
/// not fit one unit for a table must not then install the NEXT one for that
/// table: the author's schema would arrive out of order and the receiver would
/// be holding a shape its author never asked it to hold on its own.
///
/// So a unit arriving for a held table at a LATER author position is refused
/// with a typed error that names the table, the position it is waiting at and
/// what that unit needed -- and once the held unit lands, the later one applies.
#[test]
fn a_later_unit_for_a_held_table_waits_until_the_held_one_lands() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_later_unit_for_a_held_table_waits_until_the_held_one_lands",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let held = a_receiver_holding_a_table_back_for_memory(root.path());
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    let refusal = held
        .receiver
        .apply_received_schema_for_test(
            held.later_unit.clone(),
            held.later_provenance.clone(),
            tenant.clone(),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect_err("a later unit for a held table must wait, not skip ahead of the held one");
    assert_eq!(
        holdback_refusal_operation(&refusal),
        "received_schema_memory_holdback",
        "the refusal names the hold it is waiting behind, not a fresh shortfall"
    );
    assert!(
        format!("{refusal:?}").contains("notes"),
        "the refusal names the table that is waiting: {refusal:?}"
    );
    assert!(
        held.receiver
            .table_meta("notes")
            .expect("notes is still declared")
            .indexes
            .iter()
            .all(|index| index.name != "group_four"),
        "the later unit installed nothing while the table was held"
    );

    // The held unit lands once there is room, and the later one is no longer
    // waiting for anything.
    held.receiver
        .set_memory_limit(Some(held.tight + held.one_index * 12))
        .expect("raise the limit above what the held unit and the one behind it need");
    held.receiver
        .apply_received_schema_for_test(
            held.unit,
            held.provenance,
            tenant.clone(),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect("the held unit applies once memory allows");
    assert!(
        held.receiver
            .memory_held_received_schema_units(&tenant)
            .expect("the receiver reports what it is holding")
            .is_empty(),
        "the hold is released by the unit it was holding"
    );
    held.receiver
        .apply_received_schema_for_test(
            held.later_unit,
            held.later_provenance,
            tenant,
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect("and the later unit, no longer behind a hold, applies");
    assert!(
        held.receiver
            .table_meta("notes")
            .expect("notes is still declared")
            .indexes
            .iter()
            .any(|index| index.name == "group_four"),
        "the later unit installed once the held one had landed"
    );
    held.receiver.close().expect("close the receiver");
    held.author.close().expect("close the author");
}

/// THE HOLD SURVIVES THE RECEIVER BEING RESTARTED.
///
/// Being durable is the whole reason the hold is a record rather than a field
/// in memory: a receiver that restarts between the refusal and the next
/// delivery must still know that table is waiting. If it forgot, the next unit
/// for that table would install out of author order -- exactly the state the
/// hold exists to prevent -- and nothing would report the table as waiting.
#[test]
fn a_memory_hold_on_a_table_survives_the_receiver_closing_and_reopening() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_memory_hold_on_a_table_survives_the_receiver_closing_and_reopening",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let held = a_receiver_holding_a_table_back_for_memory(root.path());
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    let before_restart = held
        .receiver
        .memory_held_received_schema_units(&tenant)
        .expect("the receiver reports what it is holding");
    assert_eq!(
        before_restart.len(),
        1,
        "one table is held: {before_restart:?}"
    );
    held.receiver.close().expect("close the receiver");

    let reopened =
        Database::open(&held.receiver_path).expect("the receiver reopens under its own limit");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let after_restart = reopened
        .memory_held_received_schema_units(&tenant)
        .expect("the reopened receiver reports what it is holding");
    assert_eq!(
        after_restart, before_restart,
        "the hold is read back from disk unchanged: peer, table, position and bytes"
    );

    let refusal = reopened
        .apply_received_schema_for_test(
            held.later_unit,
            held.later_provenance,
            tenant,
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect_err("a restarted receiver still queues a later unit behind the hold it read back");
    assert_eq!(
        holdback_refusal_operation(&refusal),
        "received_schema_memory_holdback",
        "and it refuses for the hold, not for a fresh shortfall"
    );
    reopened.close().expect("close the reopened receiver");
    held.author.close().expect("close the author");
}

const HELD_TENANT: &str = "memory-holdback-tenant";
const HELD_SOURCE: &str = "memory-holdback-author";
const HELD_INCARNATION: contextdb_core::Incarnation = contextdb_core::Incarnation(23);

/// A HELD unit blocks its own table and nothing else, and the receiver is never
/// stuck: raise the limit, the same unit is re-delivered, and it installs.
///
/// A memory refusal that aborted the whole receive would be a permanently stuck
/// receiver -- the pull cursor never advances past the unit that cannot fit, so
/// every later change for every table queues behind it forever. So the hold is
/// per TABLE, recorded on the receiver's own durable schema-sync holdback
/// record, and the author is never told the unit landed.
#[test]
fn a_received_unit_held_for_memory_blocks_only_its_table_and_is_retried() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_received_unit_held_for_memory_blocks_only_its_table_and_is_retried",
    ) {
        return;
    }
    const ROWS: usize = 150;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    // The AUTHOR: the same table with the same rows, so what it declares and
    // what the receiver ends up holding can be compared directly.
    let author = Database::open(root.path().join("author.db")).expect("open the author store");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(NOTES_DDL, &HashMap::new())
        .expect("author declares notes");
    author
        .execute(
            "CREATE TABLE other (id UUID PRIMARY KEY, label TEXT NOT NULL) SYNC TWO WAY",
            &HashMap::new(),
        )
        .expect("author declares the unrelated table");
    for row in 0..ROWS {
        author
            .execute(NOTES_INSERT, &note_params(row))
            .expect("author stores one note");
    }

    // Two schema-only units, each taken from the author at its own position.
    let before_notes_unit = author.current_lsn();
    for name in ["group_one", "group_two", "group_three"] {
        author
            .execute(
                &format!("CREATE INDEX {name} ON notes {NOTES_EXTRA_COLUMNS}"),
                &HashMap::new(),
            )
            .expect("author declares one of the unit's indexes");
    }
    let notes_unit = author.changes_since(before_notes_unit);
    assert_eq!(
        notes_unit.ddl.len(),
        3,
        "the notes unit is the three indexes"
    );
    let notes_provenance = author
        .outbound_ddl_provenance_for_test(
            &notes_unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs its own schema unit");

    let before_other_unit = author.current_lsn();
    author
        .execute("CREATE INDEX other_label ON other (label)", &HashMap::new())
        .expect("author declares the unrelated table's index");
    let other_unit = author.changes_since(before_other_unit);
    assert_eq!(other_unit.ddl.len(), 1);
    let other_provenance = author
        .outbound_ddl_provenance_for_test(
            &other_unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs the unrelated unit");

    // The RECEIVER: same two tables, same rows, and a limit with room for two of
    // the three arriving indexes.
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let receiver = Database::open_with_config(
        root.path().join("receiver.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the receiver store");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(NOTES_DDL, &HashMap::new())
        .expect("receiver declares notes");
    receiver
        .execute(
            "CREATE TABLE other (id UUID PRIMARY KEY, label TEXT NOT NULL) SYNC TWO WAY",
            &HashMap::new(),
        )
        .expect("receiver declares the unrelated table");
    for row in 0..ROWS {
        receiver
            .execute(NOTES_INSERT, &note_params(row))
            .expect("receiver stores one note");
    }
    let before_one = accountant.usage().used;
    receiver
        .execute(
            &format!("CREATE INDEX sizing_probe ON notes {NOTES_EXTRA_COLUMNS}"),
            &HashMap::new(),
        )
        .expect("declare the sizing index");
    let one_index = accountant.usage().used - before_one;
    receiver
        .execute("DROP INDEX sizing_probe ON notes", &HashMap::new())
        .expect("drop the sizing index");
    let settled = accountant.usage().used;
    let tight = settled + one_index * 2;
    receiver
        .set_memory_limit(Some(tight))
        .expect("declare a limit with room for two of the three indexes");

    let refusal = receiver
        .apply_received_schema_for_test(
            notes_unit.clone(),
            notes_provenance.clone(),
            tenant.clone(),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect_err(
            "a limit with room for two of three indexes refuses the unit that declares three",
        );
    assert!(matches!(refusal, Error::MemoryBudgetExceeded { .. }));
    assert!(
        receiver
            .table_meta("notes")
            .expect("notes is still declared")
            .indexes
            .iter()
            .all(|index| !index.name.starts_with("group_")),
        "a refused unit installs none of itself"
    );

    // The hold is durable, per table, and names the author's position.
    let held = receiver
        .memory_held_received_schema_units(&tenant)
        .expect("the receiver reports what it is holding");
    assert_eq!(held.len(), 1, "one table is held: {held:?}");
    assert_eq!(held[0].table, "notes");
    assert_eq!(held[0].peer_node_id, HELD_SOURCE);
    assert!(held[0].required_bytes > 0);

    // An unrelated table's unit still applies while that hold stands.
    receiver
        .apply_received_schema_for_test(
            other_unit,
            other_provenance,
            tenant.clone(),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect("a held table does not stop another table's schema from arriving");
    assert!(
        receiver
            .table_meta("other")
            .expect("the unrelated table is declared")
            .indexes
            .iter()
            .any(|index| index.name == "other_label"),
        "the unrelated unit installed while the first was held"
    );

    // Raise the limit and the SAME unit -- the one the author never stopped
    // offering, because nothing acknowledged it -- installs.
    // Room for the unit AND for taking delivery of it. Taking delivery copies
    // every table the unit reshapes into a private working image before it
    // publishes anything, and that copy is charged to this receiver's declared
    // limit -- so the limit has to cover a second copy of what the affected
    // tables already hold, not just the shape the unit settles to.
    receiver
        .set_memory_limit(Some(tight + one_index * 4 + settled))
        .expect("raise the limit above what the held unit needs to arrive and to install");
    receiver
        .apply_received_schema_for_test(
            notes_unit,
            notes_provenance,
            tenant.clone(),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect("the held unit applies once memory allows: a refusal is never permanent");
    assert!(
        receiver
            .memory_held_received_schema_units(&tenant)
            .expect("the receiver reports what it is holding")
            .is_empty(),
        "the hold is cleared by the unit it was holding"
    );

    // Both nodes now hold the same schema and the same content for that table.
    let mut author_indexes = author
        .table_meta("notes")
        .expect("author still declares notes")
        .indexes
        .iter()
        .map(|index| index.name.clone())
        .collect::<Vec<_>>();
    let mut receiver_indexes = receiver
        .table_meta("notes")
        .expect("receiver still declares notes")
        .indexes
        .iter()
        .map(|index| index.name.clone())
        .collect::<Vec<_>>();
    author_indexes.sort();
    receiver_indexes.sort();
    assert_eq!(
        author_indexes, receiver_indexes,
        "the two nodes' index sets agree once the held unit applies"
    );
    let read_back = |db: &Database| {
        let mut rows = db
            .execute("SELECT author, seq FROM notes", &HashMap::new())
            .expect("read the table back")
            .rows;
        rows.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
        rows
    };
    assert_eq!(
        read_back(&author),
        read_back(&receiver),
        "the two nodes' content for that table agrees"
    );
    receiver.close().expect("close the receiver");
    author.close().expect("close the author");
}

/// TAKING DELIVERY OF A RECEIVED UNIT IS CHARGED TO THE RECEIVER'S DECLARED
/// LIMIT.
///
/// A receiver takes a peer's schema change by copying every table the change
/// affects -- its rows, its graph edges, its vectors -- into a private working
/// image, reshaping that, and publishing the result. The copy is the widest
/// this receiver ever gets, and an operator who declared `MEMORY_LIMIT` is
/// entitled to have it bound THAT, not just the shape the unit settles to.
///
/// The oracle is the receiver accountant's own high-water mark: taking delivery
/// of a unit over a table holding N bytes of rows must move it by at least N,
/// because for a moment both copies of those rows exist.
#[test]
fn taking_delivery_of_a_received_unit_is_charged_to_the_receivers_declared_limit() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "taking_delivery_of_a_received_unit_is_charged_to_the_receivers_declared_limit",
    ) {
        return;
    }
    const ROWS: usize = 400;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    // A real author's reshape of a table it has already populated.
    let author = Database::open(root.path().join("charged-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(WIDE_DDL, &HashMap::new())
        .expect("author declares the wide table");
    let before_unit = author.current_lsn();
    author
        .execute(
            "ALTER TABLE wide ADD COLUMN embedding VECTOR(3)",
            &HashMap::new(),
        )
        .expect("author reshapes the table");
    let unit = author.changes_since(before_unit);
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs its own schema unit");
    author.close().expect("close the author");

    let accountant = Arc::new(MemoryAccountant::no_limit());
    let receiver = Database::open_with_config(
        root.path().join("charged-receiver.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(WIDE_DDL, &HashMap::new())
        .expect("receiver declares the wide table");

    // What the row versions the copy will duplicate really hold. Measured from
    // the store's own retained-structure report rather than from the
    // accountant's total, because the total also carries the one exact-probe
    // filter the primary key allocates when the first row lands -- a fixed
    // structure the copy does not duplicate and no charge on either side
    // covers.
    let structure_before = receiver.retained_structure_bytes_for_test();
    for row in 0..ROWS {
        receiver
            .execute(WIDE_INSERT, &wide_params(row))
            .expect("receiver stores one wide row");
    }
    let row_versions =
        receiver.retained_structure_bytes_for_test().row_versions - structure_before.row_versions;
    assert!(
        row_versions > 64 * 1024,
        "the fixture must hold enough rows for the copy to be worth bounding; {row_versions} B"
    );

    let before_apply = accountant.usage().used;
    let high_water_before = accountant.high_water_for_test();
    receiver
        .apply_received_schema_for_test(
            unit,
            provenance,
            tenant.clone(),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect("a receiver with no declared limit takes the unit");
    let widest = accountant.high_water_for_test().max(high_water_before) - before_apply;

    assert!(
        widest >= row_versions,
        "taking delivery copies all {ROWS} rows into a private working image, so the receiver's \
         own accountant must see at least the {row_versions} B those row versions hold while both \
         copies exist; it moved by {widest} B, which means the copy is admitted somewhere a \
         declared MEMORY_LIMIT cannot reach it"
    );
    receiver.close().expect("close the receiver");
}

/// A COPY FAR OVER THE DECLARED LIMIT IS REFUSED BEFORE THE PROCESS ALLOCATES
/// IT, NOT AFTER.
///
/// Taking delivery prices each row version and admits it before that version
/// is cloned into the private working image. Pricing a materialised whole-
/// table clone -- or a whole vector column's payload -- and only THEN
/// checking whether it fits refuses just as correctly, but not before the
/// real allocator already holds the oversized copy: an operator's declared
/// MEMORY_LIMIT is entitled to bound the PROCESS while that copy is made, not
/// only the figure a refusal settles back to.
///
/// So this measures the REAL allocator's peak, with `PEAK_BYTES` tracked
/// inline on every allocation -- a spike freed before the test can sample
/// `live_bytes()` again still shows up. A receiver refused for memory keeps
/// an accurate bill either way; only the peak tells the two apart.
#[test]
fn an_oversized_received_units_row_copy_is_refused_before_it_is_allocated() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "an_oversized_received_units_row_copy_is_refused_before_it_is_allocated",
    ) {
        return;
    }
    const ROWS: usize = 4_000;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    let author = Database::open(root.path().join("peak-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(WIDE_DDL, &HashMap::new())
        .expect("author declares the wide table");
    let before_unit = author.current_lsn();
    author
        .execute(
            "ALTER TABLE wide ADD COLUMN embedding VECTOR(3)",
            &HashMap::new(),
        )
        .expect("author reshapes the table");
    let unit = author.changes_since(before_unit);
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs its own schema unit");
    author.close().expect("close the author");

    let accountant = Arc::new(MemoryAccountant::no_limit());
    let receiver = Database::open_with_config(
        root.path().join("peak-receiver.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(WIDE_DDL, &HashMap::new())
        .expect("receiver declares the wide table");

    // The receiver's OWN rows are what the copy duplicates -- the arriving
    // unit carries no rows of its own, only the ALTER.
    let structure_before = receiver.retained_structure_bytes_for_test();
    for row in 0..ROWS {
        receiver
            .execute(WIDE_INSERT, &wide_params(row))
            .expect("receiver stores one wide row");
    }
    let row_versions =
        receiver.retained_structure_bytes_for_test().row_versions - structure_before.row_versions;
    assert!(
        row_versions > 1024 * 1024,
        "the fixture must hold a copy worth bounding, well over any per-row overshoot; \
         {row_versions} B"
    );

    // A limit with headroom for ordinary operation but nowhere near what a
    // whole-table copy needs.
    let settled = accountant.usage().used;
    let limit = settled + 16 * 1024;
    receiver
        .set_memory_limit(Some(limit))
        .expect("declare a limit far below the copy's real cost");

    restart_peak();
    let start_live = live_bytes();
    let error = receiver
        .apply_received_schema_for_test(
            unit.clone(),
            provenance.clone(),
            tenant.clone(),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect_err("a copy this far over the limit must be refused, not installed past it");
    assert!(
        matches!(error, Error::MemoryBudgetExceeded { .. }),
        "the refusal must be the typed memory one, not {error:?}"
    );

    let peak_growth = peak_growth_since(start_live);
    assert!(
        peak_growth < row_versions as i64 / 4,
        "a copy refused before it is allocated must not let the real allocator grow anywhere \
         near the {row_versions} B the whole table holds; it grew by {peak_growth} B, which means \
         the table was cloned whole before any row was priced"
    );

    let after = accountant.usage().used;
    assert_eq!(
        after, settled,
        "charged bytes return exactly to the pre-unit figure after the refusal: the receiver was \
         at {settled} B before the unit arrived and is at {after} B after the refusal"
    );

    // A following unit gets the correct admission outcome from that exact
    // bill: raise the limit past what the copy really costs and the SAME
    // unit -- still at the position the hold recorded -- now installs.
    receiver
        .set_memory_limit(None)
        .expect("clear the limit so the retried unit is judged on room alone");
    receiver
        .apply_received_schema_for_test(unit, provenance, tenant, HELD_SOURCE, HELD_INCARNATION)
        .expect("a receiver with room now takes the unit it had refused for memory");
    assert!(
        receiver
            .table_meta("wide")
            .expect("the wide table is still declared")
            .columns
            .iter()
            .any(|column| column.name == "embedding"),
        "the retried unit installed"
    );
    receiver.close().expect("close the receiver");
}

/// PREPARATION'S OWN CHARGES DRAW ON THE RESERVATION HELD FOR THIS UNIT, AND
/// ITS BOUNDED EXCESS -- NOT A ROOM THE DECLARED LIMIT HAS NONE OF PAST THE
/// UNIT'S EXACT PRICE.
///
/// The preflight prices a whole arriving unit and reserves that room ahead of
/// every item. What the price can miss, by a margin bounded by
/// `ADMITTED_SCHEMA_UNIT_EXCESS_BYTES`, is real allocator overhead a rough
/// model does not carry -- struct padding, capacity rounding, the one
/// exact-probe filter a primary key allocates. The direct, item-by-item door
/// already absorbs that gap: it enters `AdmittedSchemaUnit` for its own items
/// and keeps installing. Preparation's detached copy has to do the same
/// thing for the SAME reason -- if the reservation this unit was priced
/// against is dropped before the copy starts, the copy's own per-object
/// charges compete for room the declared limit has none of past the unit's
/// exact price, and even the smallest natural overrun is refused outright
/// instead of absorbed.
#[test]
fn preparations_own_charges_draw_the_units_reservation_and_its_bounded_excess() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "preparations_own_charges_draw_the_units_reservation_and_its_bounded_excess",
    ) {
        return;
    }
    const ROWS: usize = 40;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    // Comfortably inside `ADMITTED_SCHEMA_UNIT_EXCESS_BYTES`: a receiver's
    // declared limit that falls this far short of what preparation's copy
    // actually needs -- the copied table's metadata alone, at
    // `contextdb_core::EXACT_PROBE_FILTER_BYTES` per exact-probed index, is
    // never covered by the arriving unit's own DDL price -- must still be
    // absorbed rather than refused.
    const GAP: usize = 24_576;

    let unit = ChangeSet {
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: vec![DdlChange::CreateIndex {
            table: "wide".to_owned(),
            name: "wide_by_body".to_owned(),
            columns: vec![("body".to_owned(), contextdb_core::SortDirection::Asc)],
        }],
        ddl_lsn: vec![contextdb_core::Lsn(1)],
    };

    // Preparation's REAL peak cost for this exact unit, measured on a
    // separate instance shaped like the one under test: the whole affected
    // table's metadata and rows copied into the detached worker, plus the
    // new index's own postings -- what `accountant.high_water_for_test()`
    // moves by while an unlimited receiver takes the unit.
    let probe_accountant = Arc::new(MemoryAccountant::no_limit());
    let probe = Database::open_with_config(
        root.path().join("excess-probe.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        probe_accountant.clone(),
    )
    .expect("open the probe store");
    probe.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    probe
        .execute(WIDE_DDL, &HashMap::new())
        .expect("probe declares the wide table");
    for row in 0..ROWS {
        probe
            .execute(WIDE_INSERT, &wide_params(row))
            .expect("probe stores one wide row");
    }
    let before_probe = probe_accountant.usage().used;
    let high_water_before_probe = probe_accountant.high_water_for_test();
    probe
        .apply_changes(
            unit.clone(),
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .expect("an unlimited receiver takes the unit");
    let real = probe_accountant
        .high_water_for_test()
        .max(high_water_before_probe)
        - before_probe;
    probe.close().expect("close the probe store");
    assert!(
        real > GAP,
        "this fixture needs preparation's real peak cost to exceed the deliberate gap, or there \
         is nothing for the reservation's excess to absorb; real is {real} B, gap is {GAP} B"
    );

    // The receiver under test: a limit exactly `GAP` bytes short of what the
    // SAME unit's preparation really needs. Every one of those bytes has to
    // come from the reservation's bounded excess, or nowhere.
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_with_config(
        root.path().join("excess-wide.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the writable store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(WIDE_DDL, &HashMap::new())
        .expect("declare the wide table");
    for row in 0..ROWS {
        db.execute(WIDE_INSERT, &wide_params(row))
            .expect("store one wide row");
    }
    let settled = accountant.usage().used;
    let limit = settled + real - GAP;
    db.set_memory_limit(Some(limit))
        .expect("declare a limit short of preparation's real peak cost by exactly the gap");

    // This unit carries no provenance, so it still routes through the SAME
    // staged preparation an authenticated unit does (the routed condition is
    // `received_ddl.is_some() || !changes.ddl.is_empty()`), which is what
    // this proof is about.
    db.apply_changes(
        unit,
        &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
    )
    .expect(
        "preparation's own per-object charges must draw on the reservation held for this \
             unit and, past it, on the bounded excess -- not on room this exact limit has none \
             of",
    );
    assert!(
        db.table_meta("wide")
            .expect("the wide table is still declared")
            .indexes
            .iter()
            .any(|index| index.name == "wide_by_body"),
        "the unit installed"
    );
    db.close().expect("close the store");
}

/// A RECEIVER THAT REFUSES A PEER'S SCHEMA CHANGE KEEPS AN ACCURATE MEMORY
/// BILL.
///
/// Taking delivery copies every table the arriving unit reshapes into a private
/// worker, and that copy is charged to the receiver's own declared limit. What
/// the receiver is then entitled to is that the charge comes back EXACTLY when
/// the delivery does not complete -- not approximately, and not twice over.
///
/// Both directions are real failures with the same shape. A copy that installs
/// without admitting anything makes the worker's drop RELEASE bytes nothing
/// added, so a refusal leaves the bill BELOW the truth and a later write spends
/// memory the store is still holding. A copy covered by a blanket reservation
/// as well as by its own objects is charged twice, so the bill sits ABOVE the
/// truth and the receiver refuses work it has room for.
///
/// The fault fires at the widest point -- the whole old image copied, no
/// arriving DDL applied, nothing durable, nothing published -- once as the
/// typed error and once as an unwind, which no `Result` branch cleans up
/// after. Unrelated live data is charged throughout, so a release that is too
/// large has somewhere to come from and shows up as a number below the start.
#[test]
fn a_refused_delivery_leaves_the_receivers_memory_bill_exactly_as_it_was() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_refused_delivery_leaves_the_receivers_memory_bill_exactly_as_it_was",
    ) {
        return;
    }
    const ROWS: usize = 250;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    let author = Database::open(root.path().join("bill-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(WIDE_DDL, &HashMap::new())
        .expect("author declares the wide table");
    let before_unit = author.current_lsn();
    author
        .execute(
            "ALTER TABLE wide ADD COLUMN embedding VECTOR(3)",
            &HashMap::new(),
        )
        .expect("author reshapes the table");
    let unit = author.changes_since(before_unit);
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs its own schema unit");
    author.close().expect("close the author");

    let accountant = Arc::new(MemoryAccountant::no_limit());
    let receiver = Database::open_with_config(
        root.path().join("bill-receiver.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(WIDE_DDL, &HashMap::new())
        .expect("receiver declares the wide table");
    for row in 0..ROWS {
        receiver
            .execute(WIDE_INSERT, &wide_params(row))
            .expect("receiver stores one wide row");
    }
    // Live data the arriving unit has nothing to do with. An over-release has
    // to come out of somebody's charge; this is what it would come out of.
    receiver
        .execute(MARKS_DDL, &HashMap::new())
        .expect("receiver declares an unrelated table");
    for row in 0..ROWS {
        receiver
            .execute(
                "INSERT INTO marks (id, n) VALUES ($id, $n)",
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(row as i64)),
                    ("n".to_owned(), Value::Int64((row * 7) as i64)),
                ]),
            )
            .expect("receiver stores one unrelated row");
    }

    let start = accountant.usage().used;

    for fault in [
        contextdb_engine::ReceivedUnitCopyFault::Error,
        contextdb_engine::ReceivedUnitCopyFault::Unwind,
    ] {
        Database::arm_received_unit_fault_for_test(
            contextdb_engine::ReceivedUnitFaultPoint::AfterWorkingCopy,
            fault,
        );
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            receiver.apply_received_schema_for_test(
                unit.clone(),
                provenance.clone(),
                tenant.clone(),
                HELD_SOURCE,
                HELD_INCARNATION,
            )
        }));
        match fault {
            contextdb_engine::ReceivedUnitCopyFault::Error => {
                let result = outcome.expect("the error case must not unwind");
                assert!(
                    result.is_err(),
                    "a fault during the copy must refuse the delivery, not report it applied"
                );
            }
            contextdb_engine::ReceivedUnitCopyFault::Unwind => {
                assert!(outcome.is_err(), "the unwind case must unwind");
            }
            contextdb_engine::ReceivedUnitCopyFault::Abort => {
                unreachable!("this loop only arms Error/Unwind")
            }
        }

        let after = accountant.usage().used;
        assert_eq!(
            after,
            start,
            "a delivery refused at its widest point leaves the receiver charged for exactly what \
             it held before the unit arrived; it started at {start} B and is at {after} B after \
             the {} case",
            match fault {
                contextdb_engine::ReceivedUnitCopyFault::Error => "error",
                contextdb_engine::ReceivedUnitCopyFault::Unwind => "unwind",
                contextdb_engine::ReceivedUnitCopyFault::Abort =>
                    unreachable!("this loop only arms Error/Unwind"),
            }
        );
    }

    // And the bill still MEANS something: a limit set from it admits or refuses
    // the next unit exactly as that figure implies.
    assert!(
        receiver
            .table_meta("wide")
            .expect("the wide table is still declared")
            .columns
            .iter()
            .all(|column| column.name != "embedding"),
        "a refused delivery installs none of itself"
    );
    receiver
        .set_memory_limit(Some(start))
        .expect("declare a limit at exactly what the store is charged for");
    let refused = receiver
        .apply_received_schema_for_test(
            unit.clone(),
            provenance.clone(),
            tenant.clone(),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect_err("a limit with no room at all must refuse the unit");
    assert!(
        matches!(refused, Error::MemoryBudgetExceeded { .. }),
        "the refusal must be the typed memory one, not {refused:?}"
    );
    receiver.close().expect("close the receiver");
}

/// A RECEIVER THAT CATCHES A PANIC DURING APPLY HOLDS NO ORPHANED STAGE.
///
/// The prepared image of an arriving unit waits for its durable commit in a map
/// the database owns. If a panic after staging left the map entry behind, it
/// would retain memory, projections and the queue-mutation lease used to stop
/// a purge splitting a queue-definition change for the life of the database.
///
/// Cleanup runs when staging unwinds, returning the reservation and lease
/// without publishing a partial unit.
#[test]
fn a_panic_between_staging_and_the_durable_commit_leaves_no_stage_behind() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_panic_between_staging_and_the_durable_commit_leaves_no_stage_behind",
    ) {
        return;
    }
    const ROWS: usize = 120;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    let author = Database::open(root.path().join("orphan-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(WIDE_DDL, &HashMap::new())
        .expect("author declares the wide table");
    let before_unit = author.current_lsn();
    author
        .execute(
            "ALTER TABLE wide ADD COLUMN embedding VECTOR(3)",
            &HashMap::new(),
        )
        .expect("author reshapes the table");
    let unit = author.changes_since(before_unit);
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs its own schema unit");
    author.close().expect("close the author");

    let accountant = Arc::new(MemoryAccountant::no_limit());
    let receiver = Database::open_with_config(
        root.path().join("orphan-receiver.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(WIDE_DDL, &HashMap::new())
        .expect("receiver declares the wide table");
    for row in 0..ROWS {
        receiver
            .execute(WIDE_INSERT, &wide_params(row))
            .expect("receiver stores one wide row");
    }

    let start = accountant.usage().used;
    assert_eq!(
        receiver.received_schema_stage_count_for_test(),
        0,
        "the receiver starts with nothing staged"
    );

    Database::arm_received_unit_fault_for_test(
        contextdb_engine::ReceivedUnitFaultPoint::AfterStageRegistration,
        contextdb_engine::ReceivedUnitCopyFault::Unwind,
    );
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        receiver.apply_received_schema_for_test(
            unit.clone(),
            provenance.clone(),
            tenant.clone(),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
    }));
    assert!(
        outcome.is_err(),
        "the injected unwind must unwind through the apply"
    );

    assert_eq!(
        receiver.received_schema_stage_count_for_test(),
        0,
        "a delivery that unwound holds no prepared stage: the map is database-owned and unwinding \
         does not clear it, so the stage has to be removed by something that runs on the unwind"
    );
    let after = accountant.usage().used;
    assert_eq!(
        after, start,
        "the memory that stage reserved for its publication comes back with it; the receiver was \
         charged {start} B before the unit arrived and is charged {after} B after the unwind"
    );

    // And the receiver still takes the unit afterwards, which is what the
    // stage-and-lease cleanup is for: a held queue-mutation lease would stop it.
    receiver
        .apply_received_schema_for_test(unit, provenance, tenant, HELD_SOURCE, HELD_INCARNATION)
        .expect("a receiver that cleaned up after the unwind still takes the unit");
    assert!(
        receiver
            .table_meta("wide")
            .expect("the wide table is still declared")
            .columns
            .iter()
            .any(|column| column.name == "embedding"),
        "the retried unit installed"
    );
    receiver.close().expect("close the receiver");
}

/// A PANIC IMMEDIATELY BEFORE THE DURABLE COMMIT LEAVES NO ORPHANED STAGE
/// EITHER -- THE SAME GUARD, AT A LATER POINT.
///
/// The stage is registered, `after_stage_registration` has already
/// succeeded, and the receiver's own pre-commit plugin hook has already run;
/// the durable (Redb) write has not. Nothing durable exists yet, so the only
/// correct outcome of a fault here -- caught or not -- is the receiver's OLD
/// complete state: no stage left behind, no memory left spent on it, no
/// trace of the arriving unit anywhere.
#[test]
fn a_panic_immediately_before_the_durable_commit_leaves_no_stage_behind() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_panic_immediately_before_the_durable_commit_leaves_no_stage_behind",
    ) {
        return;
    }
    const ROWS: usize = 120;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    let author = Database::open(root.path().join("precommit-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(WIDE_DDL, &HashMap::new())
        .expect("author declares the wide table");
    let before_unit = author.current_lsn();
    author
        .execute(
            "ALTER TABLE wide ADD COLUMN embedding VECTOR(3)",
            &HashMap::new(),
        )
        .expect("author reshapes the table");
    let unit = author.changes_since(before_unit);
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs its own schema unit");
    author.close().expect("close the author");

    let accountant = Arc::new(MemoryAccountant::no_limit());
    let receiver = Database::open_with_config(
        root.path().join("precommit-receiver.db"),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(WIDE_DDL, &HashMap::new())
        .expect("receiver declares the wide table");
    for row in 0..ROWS {
        receiver
            .execute(WIDE_INSERT, &wide_params(row))
            .expect("receiver stores one wide row");
    }

    let start = accountant.usage().used;
    Database::arm_received_unit_fault_for_test(
        contextdb_engine::ReceivedUnitFaultPoint::BeforeDurableCommit,
        contextdb_engine::ReceivedUnitCopyFault::Unwind,
    );
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        receiver.apply_received_schema_for_test(
            unit.clone(),
            provenance.clone(),
            tenant.clone(),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
    }));
    assert!(
        outcome.is_err(),
        "the injected unwind must unwind through the apply"
    );
    assert_eq!(
        receiver.received_schema_stage_count_for_test(),
        0,
        "a delivery that unwound immediately before its durable commit holds no prepared stage"
    );
    let after = accountant.usage().used;
    assert_eq!(
        after, start,
        "the memory that stage reserved comes back with it; the receiver was charged {start} B \
         before the unit arrived and is charged {after} B after the unwind"
    );
    assert!(
        receiver
            .table_meta("wide")
            .expect("the wide table is still declared")
            .columns
            .iter()
            .all(|column| column.name != "embedding"),
        "nothing durable existed yet, so the OLD complete state is what survives"
    );

    receiver
        .apply_received_schema_for_test(unit, provenance, tenant, HELD_SOURCE, HELD_INCARNATION)
        .expect("a receiver that cleaned up after the unwind still takes the unit");
    assert!(
        receiver
            .table_meta("wide")
            .expect("the wide table is still declared")
            .columns
            .iter()
            .any(|column| column.name == "embedding"),
        "the retried unit installed"
    );
    receiver.close().expect("close the receiver");
}

/// THE STOP-SERVING RULE AFTER THE DURABLE COMMIT.
///
/// Every fallible artifact is prepared before
/// durability, and already-prepared memory state publishes infallibly after
/// the commit. So an unwind between the durable commit and the end of
/// publication is an engine bug, not a resource outcome -- and there is no
/// correct partial publication to unwind back to. The process stops serving and a
/// restart recovers the complete committed state from the durable image.
/// Concretely: the handle that took the unwind refuses every later read, write
/// and transaction with the typed `PublicationInterrupted` error, and only a
/// reopen serves again.
fn assert_refuses_service_after_interrupted_publication(db: &Database) {
    let refusals = [
        (
            "a read",
            db.execute("SELECT id FROM wide", &HashMap::new())
                .map(|_| ()),
        ),
        (
            "a write",
            db.execute(WIDE_INSERT, &wide_params(10_000)).map(|_| ()),
        ),
        ("a new transaction", db.begin().map(|_| ())),
    ];
    for (what, result) in refusals {
        assert!(
            matches!(result, Err(Error::PublicationInterrupted)),
            "{what} on a handle whose publication was interrupted after the durable commit must \
             be refused until a reopen recovers the committed state, got {result:?}"
        );
    }
}

/// A PANIC AFTER THE DURABLE COMMIT BUT BEFORE PUBLICATION IS RECOVERED BY A
/// RESTART, NOT BY A PARTIAL-PUBLICATION UNWIND.
///
/// The durable (Redb) write has already succeeded -- the NEW state is the
/// authoritative one from this point on -- but publication, which moves the
/// prepared image into the live in-memory store, has not run at all. There is
/// no correct partial publication to unwind back to, so the design's answer
/// is that a restart recovers the durable image whole: this proves it on a
/// CAUGHT unwind specifically, since the durable image is what a restart
/// always re-derives from regardless of what the live process was doing.
#[test]
fn a_panic_after_the_durable_commit_before_publication_is_recovered_by_restart() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_panic_after_the_durable_commit_before_publication_is_recovered_by_restart",
    ) {
        return;
    }
    const ROWS: usize = 30;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    let author = Database::open(root.path().join("postcommit-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(WIDE_DDL, &HashMap::new())
        .expect("author declares the wide table");
    let before_unit = author.current_lsn();
    author
        .execute(
            "ALTER TABLE wide ADD COLUMN embedding VECTOR(3)",
            &HashMap::new(),
        )
        .expect("author reshapes the table");
    let unit = author.changes_since(before_unit);
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs its own schema unit");
    author.close().expect("close the author");

    let receiver_path = root.path().join("postcommit-receiver.db");
    let receiver = Database::open(&receiver_path).expect("open the receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(WIDE_DDL, &HashMap::new())
        .expect("receiver declares the wide table");
    for row in 0..ROWS {
        receiver
            .execute(WIDE_INSERT, &wide_params(row))
            .expect("receiver stores one wide row");
    }

    Database::arm_received_unit_fault_for_test(
        contextdb_engine::ReceivedUnitFaultPoint::AfterDurableCommitBeforePublish,
        contextdb_engine::ReceivedUnitCopyFault::Unwind,
    );
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        receiver.apply_received_schema_for_test(
            unit,
            provenance,
            tenant,
            HELD_SOURCE,
            HELD_INCARNATION,
        )
    }));
    assert!(
        outcome.is_err(),
        "the injected unwind must unwind through the apply, after the durable commit already \
         succeeded"
    );
    // Publication never ran, so this handle's memory does not match the
    // durable image. It must stop serving -- the rule above -- and a reopen,
    // not a live patch-up, recovers the new state.
    assert_refuses_service_after_interrupted_publication(&receiver);
    drop(receiver);

    let restarted = Database::open(&receiver_path).expect("reopen the receiver");
    assert!(
        restarted
            .table_meta("wide")
            .expect("the wide table is still declared")
            .columns
            .iter()
            .any(|column| column.name == "embedding"),
        "the durable image already carried the NEW state; a restart must serve it whole even \
         though publication never ran on the process that took the unit"
    );
    restarted
        .execute(
            "INSERT INTO wide (id, body) VALUES ($id, $body)",
            &wide_params(ROWS),
        )
        .expect("the restarted receiver is a usable, unwedged store");
    restarted.close().expect("close the restarted receiver");
}

/// A PANIC BETWEEN TWO OF PUBLICATION'S OWN COMPONENTS -- THE RELATIONAL
/// PROJECTION REPLACED, THE VECTOR IMAGE NOT YET -- IS ALSO RECOVERED BY A
/// RESTART.
///
/// Publication is documented to do no fallible work past the durable commit,
/// so a panic partway through it is a bug in this engine, not a resource
/// outcome -- the design's answer is still that the process stops serving
/// that unit's effects correctly and a restart recovers the complete NEW
/// state from the durable image, never a live patch-up of the half-published
/// components.
#[test]
fn a_panic_between_publication_components_is_recovered_by_restart() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_panic_between_publication_components_is_recovered_by_restart",
    ) {
        return;
    }
    const ROWS: usize = 30;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    let author = Database::open(root.path().join("midpublish-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(WIDE_DDL, &HashMap::new())
        .expect("author declares the wide table");
    let before_unit = author.current_lsn();
    author
        .execute(
            "ALTER TABLE wide ADD COLUMN embedding VECTOR(3)",
            &HashMap::new(),
        )
        .expect("author reshapes the table");
    let unit = author.changes_since(before_unit);
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs its own schema unit");
    author.close().expect("close the author");

    let receiver_path = root.path().join("midpublish-receiver.db");
    let receiver = Database::open(&receiver_path).expect("open the receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(WIDE_DDL, &HashMap::new())
        .expect("receiver declares the wide table");
    for row in 0..ROWS {
        receiver
            .execute(WIDE_INSERT, &wide_params(row))
            .expect("receiver stores one wide row");
    }

    Database::arm_received_unit_fault_for_test(
        contextdb_engine::ReceivedUnitFaultPoint::BetweenPublicationComponents,
        contextdb_engine::ReceivedUnitCopyFault::Unwind,
    );
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        receiver.apply_received_schema_for_test(
            unit,
            provenance,
            tenant,
            HELD_SOURCE,
            HELD_INCARNATION,
        )
    }));
    assert!(
        outcome.is_err(),
        "the injected unwind must unwind through publication itself"
    );
    // Half of publication ran. The handle stops serving (the rule above) and
    // a reopen recovers the complete new state.
    assert_refuses_service_after_interrupted_publication(&receiver);
    drop(receiver);

    let restarted = Database::open(&receiver_path).expect("reopen the receiver");
    assert!(
        restarted
            .table_meta("wide")
            .expect("the wide table is still declared")
            .columns
            .iter()
            .any(|column| column.name == "embedding"),
        "the durable image already carried the NEW state; a restart serves it whole -- relational \
         AND vector -- even though this process's publication stopped partway through"
    );
    restarted
        .execute(
            "INSERT INTO wide (id, body, embedding) VALUES ($id, $body, $embedding)",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(event_uuid(ROWS))),
                ("body".to_owned(), Value::Text("after-restart".to_owned())),
                ("embedding".to_owned(), Value::Vector(vec![0.1, 0.2, 0.3])),
            ]),
        )
        .expect("the restarted receiver's vector column is usable, not left half-registered");
    restarted.close().expect("close the restarted receiver");
}

/// Env var naming which of the three post-admission received-schema fault
/// points the crash child below aborts at. Set only on the child process; a
/// plain `cargo test`/nextest run of `a_received_schema_crash_child` finds it
/// unset and returns immediately, the same guard shape
/// `writer_opens_then_dies_without_closing_child` already uses in
/// `engine_seams`.
const CRASH_CHILD_FAULT_POINT_ENV: &str = "CONTEXTDB_RECEIVED_SCHEMA_CRASH_FAULT_POINT";
/// Printed once the crash child's receiver is declared, populated, and about
/// to arm its fault. There is no timing race to win: the very next call
/// aborts the process unconditionally, so the marker only has to carry the
/// receiver path back to the parent before that happens.
const CRASH_CHILD_READY_MARKER: &str = "CONTEXTDB_CRASH_CHILD_READY receiver_path=";

fn crash_child_fault_point_from_env(value: &str) -> contextdb_engine::ReceivedUnitFaultPoint {
    match value {
        "BeforeDurableCommit" => contextdb_engine::ReceivedUnitFaultPoint::BeforeDurableCommit,
        "AfterDurableCommitBeforePublish" => {
            contextdb_engine::ReceivedUnitFaultPoint::AfterDurableCommitBeforePublish
        }
        "BetweenPublicationComponents" => {
            contextdb_engine::ReceivedUnitFaultPoint::BetweenPublicationComponents
        }
        other => panic!("unknown crash child fault point: {other}"),
    }
}

/// A REAL process death -- `std::process::abort()`, no unwind, no `Drop`, no
/// panic hook -- at one of the three post-admission received-schema fault
/// points, run only as a re-executed child of the three
/// `a_process_crash_..._is_recovered_by_restart` proofs below. An ordinary
/// test run of this function (no `CRASH_CHILD_FAULT_POINT_ENV`) is a no-op.
///
/// The three in-process proofs above this one (`a_panic_immediately_before_
/// the_durable_commit_leaves_no_stage_behind`,
/// `a_panic_after_the_durable_commit_before_publication_is_recovered_by_
/// restart`, `a_panic_between_publication_components_is_recovered_by_
/// restart`) prove the same fault points with `std::panic::catch_unwind` in
/// the SAME process, so they can additionally check that process's own
/// in-memory bookkeeping (the stage map, the accountant). A caught unwind
/// still runs every `Drop` on the unwound stack, though, and shares whatever
/// global or thread-local state the rest of that process holds -- neither is
/// true of a genuine crash. This proves the design's answer does not
/// secretly depend on either: after a real process death, the only thing
/// left to ask is what the design actually promises, the durable image an
/// ordinary `Database::open` re-derives from in a different process entirely.
#[test]
fn a_received_schema_crash_child() {
    let Ok(fault_point) = std::env::var(CRASH_CHILD_FAULT_POINT_ENV) else {
        return;
    };
    const ROWS: usize = 30;
    let root = tempfile::tempdir().expect("crash child fixture root");
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    let author = Database::open(root.path().join("crash-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(WIDE_DDL, &HashMap::new())
        .expect("author declares the wide table");
    let before_unit = author.current_lsn();
    author
        .execute(
            "ALTER TABLE wide ADD COLUMN embedding VECTOR(3)",
            &HashMap::new(),
        )
        .expect("author reshapes the table");
    let unit = author.changes_since(before_unit);
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs its own schema unit");
    author.close().expect("close the author");

    let receiver_path = root.path().join("crash-receiver.db");
    let receiver = Database::open(&receiver_path).expect("open the receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(WIDE_DDL, &HashMap::new())
        .expect("receiver declares the wide table");
    for row in 0..ROWS {
        receiver
            .execute(WIDE_INSERT, &wide_params(row))
            .expect("receiver stores one wide row");
    }

    println!("{CRASH_CHILD_READY_MARKER}{}", receiver_path.display());
    std::io::Write::flush(&mut std::io::stdout()).expect("flush crash child readiness marker");

    Database::arm_received_unit_fault_for_test(
        crash_child_fault_point_from_env(&fault_point),
        contextdb_engine::ReceivedUnitCopyFault::Abort,
    );
    let _ = receiver.apply_received_schema_for_test(
        unit,
        provenance,
        tenant,
        HELD_SOURCE,
        HELD_INCARNATION,
    );
    panic!("the armed fault must abort the process before apply ever returns");
}

/// Spawn `a_received_schema_crash_child` as a genuinely separate process
/// armed at `fault_point`, wait for it to die, and hand back the receiver
/// path it printed before dying so the caller can reopen it.
fn spawn_received_schema_crash_child(fault_point: &str) -> std::path::PathBuf {
    let mut command = Command::new(
        std::env::current_exe().expect("current declared_memory_limit_bounds_a_writer binary"),
    );
    command
        .args(["--exact", "a_received_schema_crash_child", "--nocapture"])
        .env(CRASH_CHILD_FAULT_POINT_ENV, fault_point)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut child = command
        .spawn()
        .expect("spawn the received-schema crash child");
    let stdout = child.stdout.take().expect("crash child stdout pipe");
    let mut receiver_path = None;
    for line in std::io::BufReader::new(stdout).lines() {
        let line = line.expect("read crash child stdout");
        if let Some(path) = line.strip_prefix(CRASH_CHILD_READY_MARKER) {
            receiver_path = Some(std::path::PathBuf::from(path));
            break;
        }
    }
    let receiver_path = receiver_path
        .expect("the crash child must print its receiver path before the armed fault aborts it");
    let status = child
        .wait()
        .expect("wait for the received-schema crash child");
    assert!(
        !status.success(),
        "the armed fault must abort the crash child, not let it exit cleanly: {status:?}"
    );
    receiver_path
}

/// PROCESS-DEATH VARIANT OF `a_panic_immediately_before_the_durable_commit_
/// leaves_no_stage_behind`: A REAL PROCESS CRASH IMMEDIATELY BEFORE THE
/// DURABLE COMMIT LEAVES THE OLD COMPLETE STATE, PROVEN ACROSS A REAL
/// RESTART OF A DIFFERENT PROCESS.
#[test]
fn a_process_crash_immediately_before_the_durable_commit_leaves_the_old_complete_state() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_process_crash_immediately_before_the_durable_commit_leaves_the_old_complete_state",
    ) {
        return;
    }
    let receiver_path = spawn_received_schema_crash_child("BeforeDurableCommit");
    let restarted = Database::open(&receiver_path).expect("reopen the crashed receiver");
    assert!(
        restarted
            .table_meta("wide")
            .expect("the wide table is still declared")
            .columns
            .iter()
            .all(|column| column.name != "embedding"),
        "nothing durable existed yet when the process died, so the OLD complete state survives"
    );
    restarted
        .execute(WIDE_INSERT, &wide_params(999))
        .expect("the crashed-and-restarted receiver is a usable, unwedged store");
    restarted.close().expect("close the restarted receiver");
}

/// PROCESS-DEATH VARIANT OF `a_panic_after_the_durable_commit_before_
/// publication_is_recovered_by_restart`: A REAL PROCESS CRASH AFTER THE
/// DURABLE COMMIT BUT BEFORE PUBLICATION IS RECOVERED BY A REAL RESTART OF A
/// DIFFERENT PROCESS, NOT BY ANY PARTIAL-PUBLICATION CLEANUP THE DEAD
/// PROCESS NEVER RAN.
#[test]
fn a_process_crash_after_the_durable_commit_before_publication_is_recovered_by_restart() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_process_crash_after_the_durable_commit_before_publication_is_recovered_by_restart",
    ) {
        return;
    }
    let receiver_path = spawn_received_schema_crash_child("AfterDurableCommitBeforePublish");
    let restarted = Database::open(&receiver_path).expect("reopen the crashed receiver");
    assert!(
        restarted
            .table_meta("wide")
            .expect("the wide table is still declared")
            .columns
            .iter()
            .any(|column| column.name == "embedding"),
        "the durable image already carried the NEW state when the process died; a restart must \
         serve it whole even though publication never ran on the process that took the unit"
    );
    restarted
        .execute(
            "INSERT INTO wide (id, body) VALUES ($id, $body)",
            &wide_params(999),
        )
        .expect("the crashed-and-restarted receiver is a usable, unwedged store");
    restarted.close().expect("close the restarted receiver");
}

/// PROCESS-DEATH VARIANT OF `a_panic_between_publication_components_is_
/// recovered_by_restart`: A REAL PROCESS CRASH BETWEEN TWO OF PUBLICATION'S
/// OWN COMPONENTS IS ALSO RECOVERED BY A REAL RESTART OF A DIFFERENT PROCESS.
#[test]
fn a_process_crash_between_publication_components_is_recovered_by_restart() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_process_crash_between_publication_components_is_recovered_by_restart",
    ) {
        return;
    }
    let receiver_path = spawn_received_schema_crash_child("BetweenPublicationComponents");
    let restarted = Database::open(&receiver_path).expect("reopen the crashed receiver");
    assert!(
        restarted
            .table_meta("wide")
            .expect("the wide table is still declared")
            .columns
            .iter()
            .any(|column| column.name == "embedding"),
        "the durable image already carried the NEW state when the process died; a restart serves \
         it whole -- relational AND vector -- even though publication stopped mid-way"
    );
    restarted
        .execute(
            "INSERT INTO wide (id, body, embedding) VALUES ($id, $body, $embedding)",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(event_uuid(999))),
                (
                    "body".to_owned(),
                    Value::Text("after-crash-restart".to_owned()),
                ),
                ("embedding".to_owned(), Value::Vector(vec![0.1, 0.2, 0.3])),
            ]),
        )
        .expect(
            "the crashed-and-restarted receiver's vector column is usable, not left \
             half-registered",
        );
    restarted.close().expect("close the restarted receiver");
}

/// A received DROP COLUMN and RENAME COLUMN install whole and survive restart.
///
/// The authenticated DROP COLUMN unit must remove the named column from the
/// receiver's table metadata before restart, and the later RENAME must preserve
/// the author's explicit operation. The projected and durable schema must
/// agree after each received unit.
const DROP_RENAME_DDL: &str =
    "CREATE TABLE wide (id UUID PRIMARY KEY, body TEXT NOT NULL, extra TEXT) SYNC TWO WAY";

fn author_add_then_drop_column(path: &std::path::Path) -> Database {
    let author = Database::open(path).expect("open the schema author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for statement in [
        "CREATE TABLE replayed (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY",
        "ALTER TABLE replayed ADD COLUMN temporary TEXT",
        "ALTER TABLE replayed DROP COLUMN temporary",
    ] {
        author
            .execute(statement, &HashMap::new())
            .unwrap_or_else(|error| panic!("author runs {statement}: {error:?}"));
    }
    let live = author
        .table_meta("replayed")
        .expect("the authored table exists");
    assert_eq!(
        live.columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "body"],
        "the author's final declaration excludes the dropped column"
    );
    author
}

#[cfg(unix)]
#[test]
fn add_then_drop_column_history_answer_keeps_the_authored_order_after_a_change_log_gap() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "add_then_drop_column_history_answer_keeps_the_authored_order_after_a_change_log_gap",
    ) {
        return;
    }
    use std::io::{Read, Write};
    use std::os::unix::net::UnixListener;

    let root = tempfile::tempdir().expect("task-scoped schema fixture");
    let author = author_add_then_drop_column(&root.path().join("author.db"));
    let direct = author.changes_since(contextdb_core::Lsn(0));
    assert_eq!(
        direct.ddl.len(),
        3,
        "the authored unit contains all three statements"
    );

    let socket = root.path().join("persisted-state.sock");
    let listener = UnixListener::bind(&socket).expect("listen for persisted-state extraction");
    author.arm_persisted_state_schema_frontier_pause_for_test(socket);
    let fallback = std::thread::scope(|scope| {
        let extraction = scope.spawn(|| author.changes_since(contextdb_core::Lsn(0)));
        let (mut stream, _) = listener
            .accept()
            .expect("extraction reaches persisted state");
        let mut reached = [0u8; 1];
        stream
            .read_exact(&mut reached)
            .expect("read persisted-state milestone");
        assert_eq!(reached, [1]);
        stream
            .write_all(&[1])
            .expect("release persisted-state extraction");
        extraction.join().expect("join persisted-state extraction")
    });
    assert_eq!(
        fallback.ddl, direct.ddl,
        "a change-log gap must answer CREATE, ADD, and DROP in authored order"
    );
    assert_eq!(
        fallback.ddl_lsn, direct.ddl_lsn,
        "the fallback keeps each statement's authored commit position"
    );
    author.close().expect("close the schema author");
}

#[test]
fn received_add_then_drop_column_installs_the_authors_final_schema() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "received_add_then_drop_column_installs_the_authors_final_schema",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("task-scoped schema fixture");
    let author = author_add_then_drop_column(&root.path().join("author.db"));
    let unit = author.changes_since(contextdb_core::Lsn(0));
    assert_eq!(
        unit.ddl.len(),
        3,
        "the authored unit contains all three statements"
    );
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &tenant,
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs the complete schema unit");
    let receiver =
        Database::open(root.path().join("receiver.db")).expect("open a fresh schema receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .apply_received_schema_for_test(unit, provenance, tenant, HELD_SOURCE, HELD_INCARNATION)
        .expect("the complete authored schema unit installs");
    let installed = receiver
        .table_meta("replayed")
        .expect("the receiver installs the table");
    assert_eq!(
        installed
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "body"],
        "the receiver's final schema excludes the authored dropped column"
    );
    author.close().expect("close the schema author");
    receiver.close().expect("close the receiver");
}

#[test]
fn a_received_drop_column_and_rename_column_install_whole_and_survive_restart() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_received_drop_column_and_rename_column_install_whole_and_survive_restart",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    let author = Database::open(root.path().join("dropcol-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(DROP_RENAME_DDL, &HashMap::new())
        .expect("author declares the table");

    let receiver_path = root.path().join("dropcol-receiver.db");
    let receiver = Database::open(&receiver_path).expect("open the receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(DROP_RENAME_DDL, &HashMap::new())
        .expect("receiver declares the table");
    receiver
        .execute(
            "INSERT INTO wide (id, body, extra) VALUES ($id, $body, $extra)",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(event_uuid(0))),
                ("body".to_owned(), Value::Text("hello".to_owned())),
                ("extra".to_owned(), Value::Text("gone-soon".to_owned())),
            ]),
        )
        .expect("receiver stores one row");

    // The DROP and the RENAME are delivered as two separate units, each
    // asserted whole and durable on its own before the next arrives.
    let before_drop = author.current_lsn();
    author
        .execute("ALTER TABLE wide DROP COLUMN extra", &HashMap::new())
        .expect("author drops the column");
    let drop_unit = author.changes_since(before_drop);
    let drop_provenance = author
        .outbound_ddl_provenance_for_test(
            &drop_unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs the drop unit");
    receiver
        .apply_received_schema_for_test(
            drop_unit,
            drop_provenance,
            tenant.clone(),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect("a drop-column unit installs whole");
    {
        let meta = receiver
            .table_meta("wide")
            .expect("the table is still declared after the drop");
        assert!(
            meta.columns.iter().all(|column| column.name != "extra"),
            "the dropped column is gone: {:?}",
            meta.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
        );
    }

    let before_rename = author.current_lsn();
    author
        .execute(
            "ALTER TABLE wide RENAME COLUMN body TO content",
            &HashMap::new(),
        )
        .expect("author renames the column");
    let rename_unit = author.changes_since(before_rename);
    let rename_provenance = author
        .outbound_ddl_provenance_for_test(
            &rename_unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs the rename unit");
    receiver
        .apply_received_schema_for_test(
            rename_unit,
            rename_provenance,
            tenant,
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect("a rename-column unit installs whole");
    author.close().expect("close the author");

    let assert_shape = |db: &Database, when: &str| {
        let meta = db
            .table_meta("wide")
            .unwrap_or_else(|| panic!("the table is still declared {when}"));
        assert!(
            meta.columns.iter().all(|column| column.name != "extra"),
            "the dropped column is gone {when}"
        );
        assert!(
            meta.columns.iter().all(|column| column.name != "body"),
            "the old column name is gone {when}"
        );
        assert!(
            meta.columns.iter().any(|column| column.name == "content"),
            "the renamed column is present {when}"
        );
        let rows = db
            .execute("SELECT content FROM wide", &HashMap::new())
            .unwrap_or_else(|error| panic!("the renamed column is queryable {when}: {error:?}"));
        assert_eq!(rows.rows.len(), 1, "the row survived the reshape {when}");
        assert_eq!(
            rows.rows[0][0],
            Value::Text("hello".to_owned()),
            "the row's value under its new column name is unchanged {when}"
        );
    };
    assert_shape(&receiver, "before restart");
    receiver.close().expect("close the receiver");

    let restarted = Database::open(&receiver_path).expect("reopen the receiver");
    assert_shape(&restarted, "after restart");
    restarted.close().expect("close the restarted receiver");
}

/// A RECEIVING PEER RENAMES A COLUMN ONLY WHEN THE AUTHOR'S SIGNED SCHEMA
/// CHANGE SAYS IT DID.
///
/// Authenticated received DDL replays exactly as its author wrote it. A
/// receiver holding `(id, old TEXT)` with `old = 'private'` must not infer a
/// `RENAME COLUMN old TO new` from an author's policy-only ALTER that omits
/// `old` and adds `new`; otherwise it would retain private data under a name
/// the author never gave it.
const RENAME_EVIDENCE_TENANT: &str = "rename-evidence-tenant";
const RENAME_EVIDENCE_AUTHOR: &str = "rename-evidence-author";
const RENAME_EVIDENCE_FORWARDER: &str = "rename-evidence-forwarder";

fn rename_evidence_peer(path: &std::path::Path, shape: &str, value: Option<&str>) -> Database {
    let db = Database::open(path).expect("open a rename-evidence peer");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(shape, &HashMap::new())
        .expect("the peer declares its table");
    if let Some(value) = value {
        db.execute(
            "INSERT INTO div (id, old) VALUES ($id, $old)",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(event_uuid(0))),
                ("old".to_owned(), Value::Text(value.to_owned())),
            ]),
        )
        .expect("the peer stores one row");
    }
    db
}

/// Delivers `unit` (signed by `from`) to `to`, as `sender`.
fn deliver_rename_evidence_unit(from: &Database, unit: ChangeSet, to: &Database, sender: &str) {
    let provenance = from
        .outbound_ddl_provenance_for_test(
            &unit,
            &contextdb_core::TenantId::from(RENAME_EVIDENCE_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the sender carries the unit's signed provenance");
    let applied = to
        .apply_received_schema_for_test(
            unit,
            provenance,
            contextdb_core::TenantId::from(RENAME_EVIDENCE_TENANT),
            sender,
            HELD_INCARNATION,
        )
        .expect("the authenticated schema unit installs whole");
    assert!(
        applied.conflicts.is_empty(),
        "the unit is delivered, not refused: {:?}",
        applied.conflicts
    );
}

fn assert_rename_evidence_outcome(
    db: &Database,
    expected: Option<&str>,
    lost: &str,
    // A partial alteration preserves the columns it does not mention; a column
    // is gone only when the author's unit carries an explicit drop. So a
    // journey whose author steps never mention `old` keeps it, and the
    // receiver's held columns are the ones this journey actually declares
    // here, not a hardcoded `["id", "new"]`.
    expected_columns: &[&str],
    when: &str,
) {
    let meta = db
        .table_meta("div")
        .unwrap_or_else(|| panic!("the table is still declared {when}"));
    let names = meta
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        expected_columns
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<Vec<_>>(),
        "the receiver holds exactly the columns this journey's authored operation leaves behind \
         {when}"
    );
    let rows = db
        .execute("SELECT * FROM div", &HashMap::new())
        .unwrap_or_else(|error| panic!("the table answers {when}: {error:?}"));
    assert_eq!(rows.rows.len(), 1, "the row keyed by id survives {when}");
    let new_at = rows
        .columns
        .iter()
        .position(|column| column == "new")
        .unwrap_or_else(|| panic!("`new` is selectable {when}: {:?}", rows.columns));
    assert_eq!(
        rows.rows[0][new_at],
        expected.map_or(Value::Null, |value| Value::Text(value.to_owned())),
        "`new` holds only what the author's schema change put there {when}"
    );
    let old_preserved = expected_columns.contains(&"old");
    if expected.is_none() && !old_preserved {
        assert!(
            rows.rows
                .iter()
                .flatten()
                .all(|value| *value != Value::Text(lost.to_owned())),
            "the dropped column's value `{lost}` appears in no column {when}: {:?}",
            rows.rows
        );
    }
    if old_preserved {
        // An omitted column is preserved under its own name, never folded
        // into `new` -- so the value the receiver already held stays exactly
        // where it was, not lost and not renamed.
        let old_at = rows
            .columns
            .iter()
            .position(|column| column == "old")
            .unwrap_or_else(|| panic!("`old` is selectable {when}: {:?}", rows.columns));
        assert_eq!(
            rows.rows[0][old_at],
            Value::Text(lost.to_owned()),
            "the omitted column keeps its own value under its own name, neither dropped nor \
             renamed, {when}"
        );
    }
}

/// Runs one journey: `author_steps` run on an author holding `author_shape`;
/// their schema unit goes to a receiver holding `receiver_shape` with a row,
/// is checked, checked again after restart, and forwarded by the receiver to
/// a third peer holding the same shape and row, which is checked too.
fn rename_evidence_journey(
    label: &str,
    author_shape: &str,
    receiver_shape: &str,
    author_steps: &[&str],
    expected: Option<&str>,
    expected_columns: &[&str],
) {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let author = rename_evidence_peer(&root.path().join("author.db"), author_shape, None);
    let receiver_path = root.path().join("receiver.db");
    let receiver = rename_evidence_peer(&receiver_path, receiver_shape, Some("private"));
    let third = rename_evidence_peer(
        &root.path().join("third.db"),
        receiver_shape,
        Some("private"),
    );

    let before_unit = author.current_lsn();
    for step in author_steps {
        author
            .execute(step, &HashMap::new())
            .unwrap_or_else(|error| panic!("{label}: author runs `{step}`: {error:?}"));
    }
    let unit = author.changes_since(before_unit);
    let before_receipt = receiver.current_lsn();
    deliver_rename_evidence_unit(&author, unit, &receiver, RENAME_EVIDENCE_AUTHOR);
    author.close().expect("close the author");
    assert_rename_evidence_outcome(
        &receiver,
        expected,
        "private",
        expected_columns,
        &format!("{label}: on the receiver"),
    );
    receiver.close().expect("close the receiver");

    let restarted = Database::open(&receiver_path).expect("reopen the receiver");
    restarted.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_rename_evidence_outcome(
        &restarted,
        expected,
        "private",
        expected_columns,
        &format!("{label}: on the receiver after restart"),
    );

    let forwarded = restarted.changes_since(before_receipt);
    assert!(
        !forwarded.ddl.is_empty(),
        "{label}: the receiver has the received schema change to forward"
    );
    deliver_rename_evidence_unit(&restarted, forwarded, &third, RENAME_EVIDENCE_FORWARDER);
    assert_rename_evidence_outcome(
        &third,
        expected,
        "private",
        expected_columns,
        &format!("{label}: on a third peer it was forwarded to"),
    );
    restarted.close().expect("close the restarted receiver");
    third.close().expect("close the third peer");
}

#[test]
fn a_receiver_renames_a_column_only_when_the_authors_signed_change_says_so() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_receiver_renames_a_column_only_when_the_authors_signed_change_says_so",
    ) {
        return;
    }
    // A divergent peer: the author never had `old`, and sends only a policy
    // change restating its own full shape. Nothing was renamed: partial
    // alterations preserve the columns they omit, whether or not the
    // primary key is included -- nothing was dropped either, because the
    // author's unit carries no explicit `COLUMN_DROP` marker for `old`. The
    // receiver keeps its own `old` (and its value) alongside the author's
    // `new`. (The receiver already declares the policy the author arrives
    // at, so its own declared policy is not what decides this delivery.)
    rename_evidence_journey(
        "divergent policy-only change",
        "CREATE TABLE div (id UUID PRIMARY KEY, new TEXT) SYNC TWO WAY",
        "CREATE TABLE div (id UUID PRIMARY KEY, old TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &["ALTER TABLE div SET SYNC CONFLICT KEEP LATEST"],
        None,
        &["id", "old", "new"],
    );
    // A genuine authored rename keeps the value under its new name.
    rename_evidence_journey(
        "authored rename",
        "CREATE TABLE div (id UUID PRIMARY KEY, old TEXT) SYNC TWO WAY",
        "CREATE TABLE div (id UUID PRIMARY KEY, old TEXT) SYNC TWO WAY",
        &["ALTER TABLE div RENAME COLUMN old TO new"],
        Some("private"),
        &["id", "new"],
    );
    // A genuine drop of one column and add of another same-typed column,
    // delivered together in one unit, is not a rename: the old value goes
    // with the dropped column. (A local schema transaction refuses DROP
    // COLUMN, so the author commits the two statements back to back.) The
    // explicit `DROP COLUMN` is the author's `COLUMN_DROP` marker the owner's
    // 2026-09-27 ruling requires for a column to actually disappear.
    rename_evidence_journey(
        "authored drop then add",
        "CREATE TABLE div (id UUID PRIMARY KEY, old TEXT) SYNC TWO WAY",
        "CREATE TABLE div (id UUID PRIMARY KEY, old TEXT) SYNC TWO WAY",
        &[
            "ALTER TABLE div DROP COLUMN old",
            "ALTER TABLE div ADD COLUMN new TEXT",
        ],
        None,
        &["id", "new"],
    );
}

/// A duplicate delivery after restart returns the stored manifest result
/// and applies nothing twice.
///
/// `classify_received_schema_manifest` recognises this unit's authenticated
/// identity from what the FIRST delivery durably stored, before this call
/// even opens a transaction. If it works, redelivering the identical unit
/// after a restart returns the same `new_lsn` the original commit produced --
/// no new transaction, no new LSN -- rather than trying to reapply an ALTER
/// that already landed.
#[test]
fn a_duplicate_delivery_after_restart_replays_the_stored_result_and_applies_nothing_twice() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_duplicate_delivery_after_restart_replays_the_stored_result_and_applies_nothing_twice",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    let author = Database::open(root.path().join("dup-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(WIDE_DDL, &HashMap::new())
        .expect("author declares the wide table");
    let before_unit = author.current_lsn();
    author
        .execute(
            "ALTER TABLE wide ADD COLUMN embedding VECTOR(3)",
            &HashMap::new(),
        )
        .expect("author reshapes the table");
    let unit = author.changes_since(before_unit);
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs its own schema unit");
    author.close().expect("close the author");

    let receiver_path = root.path().join("dup-receiver.db");
    let receiver = Database::open(&receiver_path).expect("open the receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(WIDE_DDL, &HashMap::new())
        .expect("receiver declares the wide table");

    let first = receiver
        .apply_received_schema_for_test(
            unit.clone(),
            provenance.clone(),
            tenant.clone(),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect("the first delivery installs");
    receiver.close().expect("close the receiver");

    let restarted = Database::open(&receiver_path).expect("reopen the receiver");
    restarted.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let second = restarted
        .apply_received_schema_for_test(unit, provenance, tenant, HELD_SOURCE, HELD_INCARNATION)
        .expect("a duplicate delivery after restart must not be refused, just replayed");

    assert_eq!(
        first.new_lsn, second.new_lsn,
        "a duplicate delivery returns the SAME stored manifest result -- no new commit, no new \
         LSN -- not a freshly reapplied one"
    );
    assert_eq!(
        first.applied_rows, second.applied_rows,
        "the replayed result matches the original exactly"
    );
    assert_eq!(
        restarted
            .table_meta("wide")
            .expect("the wide table is still declared")
            .columns
            .iter()
            .filter(|column| column.name == "embedding")
            .count(),
        1,
        "the column exists once -- the duplicate delivery installed nothing a second time"
    );
    restarted.close().expect("close the restarted receiver");
}

/// A UNIT WITH NO SIGNED PROVENANCE INSTALLS WHOLE OR NOT AT ALL, LIKE EVERY
/// OTHER ONE.
///
/// An arriving unit that carries no authenticated provenance used to take a
/// different door: it reshaped the live store item by item -- table metadata,
/// index storage, row columns, vector registrations -- and appended each item's
/// DDL to the REPLICATED log as that item's last step. So a unit refused half
/// way through left the receiver serving a schema no author ever sent it, and
/// replicated the part that landed onward to every peer.
///
/// The take-back that used to patch this could only restore `TableMeta`. It
/// could not put back a removed row column, and it never ran at all when the
/// refusal came after the DDL phase. There is nothing to put back now: the unit
/// is prepared off to one side and a refusal discards the stage.
#[test]
fn a_unit_with_no_provenance_installs_whole_or_not_at_all() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_unit_with_no_provenance_installs_whole_or_not_at_all",
    ) {
        return;
    }
    const ROWS: usize = 300;
    let root = tempfile::tempdir().expect("task-scoped fixture root");

    let author = Database::open(root.path().join("plain-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(WIDE_DDL, &HashMap::new())
        .expect("author declares the wide table");
    let before_unit = author.current_lsn();
    // Two items, in the order the author wrote them. The first is priced
    // accurately and would install; the second is a reshape of an already
    // populated table, which the receiver's own price under-estimates by far
    // more than the bounded excess -- so the refusal lands on item two with
    // item one already done.
    author
        .execute(
            "CREATE TABLE narrow (id UUID PRIMARY KEY, tag TEXT NOT NULL) SYNC TWO WAY",
            &HashMap::new(),
        )
        .expect("author declares the unit's first item");
    author
        .execute(
            "ALTER TABLE wide ADD COLUMN embedding VECTOR(3)",
            &HashMap::new(),
        )
        .expect("author reshapes the table in the same unit");
    let unit = author.changes_since(before_unit);
    assert_eq!(
        unit.ddl.len(),
        2,
        "the unit has to carry TWO items, so one can succeed while the other is refused: {:?}",
        unit.ddl
    );
    author.close().expect("close the author");

    let path = root.path().join("plain-receiver.db");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_with_config(
        path.clone(),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the receiver");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(WIDE_DDL, &HashMap::new())
        .expect("receiver declares the wide table");
    for row in 0..ROWS {
        db.execute(WIDE_INSERT, &wide_params(row))
            .expect("receiver stores one wide row");
    }

    let columns_of = |db: &Database| {
        let mut names = db
            .table_meta("wide")
            .expect("the table is declared")
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names
    };
    let shape_before = columns_of(&db);
    let log_before = format!("{:?}", db.ddl_log_since(contextdb_core::Lsn(0)));
    let rows_before = db
        .execute("SELECT id FROM wide", &HashMap::new())
        .expect("the receiver answers before the unit")
        .rows
        .len();

    // Room for the unit's own price and not for what installing it really
    // costs, so the refusal lands with the FIRST item already done and the
    // second one refused. That is the case a shape take-back can never repair:
    // the first item has already written its entry to the REPLICATED log, and a
    // compensating entry to undo it would send this receiver's memory shortfall
    // to every peer as a schema revert.
    let settled = accountant.usage().used;
    let priced = db.projected_sync_ddl_unit_bytes_for_test(&unit.ddl);
    db.set_memory_limit(Some(settled + priced))
        .expect("declare a limit the unit's own price fits and installing it does not");

    let error = db
        .apply_changes(
            unit,
            &ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists),
        )
        .expect_err("a delivery that does not fit the declared limit must be refused");
    assert!(
        matches!(error, Error::MemoryBudgetExceeded { .. }),
        "the refusal must be the typed memory one, not {error:?}"
    );

    assert!(
        db.table_meta("narrow").is_none(),
        "a refused unit installs none of itself, not even the item that would have fitted"
    );
    assert_eq!(
        columns_of(&db),
        shape_before,
        "the table still has the shape it had"
    );
    assert_eq!(
        format!("{:?}", db.ddl_log_since(contextdb_core::Lsn(0))),
        log_before,
        "the replicated DDL log is exactly what it was before the unit arrived -- the item that \
         would have succeeded appended nothing, so no peer that pulls afterwards sees half of \
         this migration, and nothing was written to take it back"
    );
    assert_eq!(
        db.execute("SELECT id FROM wide", &HashMap::new())
            .expect("the receiver still answers")
            .rows
            .len(),
        rows_before,
        "every row the receiver held is still there"
    );

    db.set_memory_limit(Some(settled * 4 + 8 * 1024 * 1024))
        .expect("raise the limit so the store can be reopened");
    db.close().expect("close the receiver");
    let reopened = Database::open(&path).expect("the receiver reopens");
    assert_eq!(
        columns_of(&reopened),
        shape_before,
        "the store reads back the old complete schema, not a shape the refusal left behind"
    );
    assert_eq!(
        reopened
            .execute("SELECT id FROM wide", &HashMap::new())
            .expect("the reopened receiver answers")
            .rows
            .len(),
        rows_before,
        "and every row, after a restart"
    );
    reopened.close().expect("close the reopened receiver");
}

/// Which door a received schema unit comes through: signed by its author, or
/// with no provenance at all.
#[derive(Clone, Copy, Debug)]
enum ReceivedDoor {
    Authenticated,
    Unattested,
}

/// A unit the receiver's limit PRICES as fitting, whose private copy of the
/// table it reshapes does not fit, and the two units that follow it: a later
/// one for the same table and one for an unrelated table.
struct StagedRefusal {
    author: Database,
    receiver_path: std::path::PathBuf,
    units: [(
        ChangeSet,
        Vec<contextdb_engine::protocol::WireDdlProvenance>,
    ); 3],
}

const SIDE_DDL: &str = "CREATE TABLE side (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY";

fn a_receiver_that_prices_a_unit_as_fitting_but_cannot_copy_it(
    root: &std::path::Path,
) -> (StagedRefusal, Database) {
    const ROWS: usize = 400;
    let author = Database::open(root.join("staged-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(WIDE_DDL, &HashMap::new())
        .expect("author declares the wide table");
    author
        .execute(SIDE_DDL, &HashMap::new())
        .expect("author declares an unrelated table");
    let unit_for = |sql: &str| {
        let before = author.current_lsn();
        author
            .execute(sql, &HashMap::new())
            .expect("author reshapes");
        let unit = author.changes_since(before);
        let provenance = author
            .outbound_ddl_provenance_for_test(
                &unit,
                &contextdb_core::TenantId::from(HELD_TENANT),
                &fixture_schema_author().node_id(),
                HELD_INCARNATION,
                &fixture_signer(),
            )
            .expect("the author signs its own schema unit");
        (unit, provenance)
    };
    let held = unit_for("ALTER TABLE wide ADD COLUMN extra TEXT");
    let later = unit_for("ALTER TABLE wide ADD COLUMN later TEXT");
    let unrelated = unit_for("ALTER TABLE side ADD COLUMN extra TEXT");

    let receiver_path = root.join("staged-receiver.db");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let receiver = Database::open_with_config(
        receiver_path.clone(),
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("open the receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(WIDE_DDL, &HashMap::new())
        .expect("receiver declares the wide table");
    receiver
        .execute(SIDE_DDL, &HashMap::new())
        .expect("receiver declares the unrelated table");
    let structure_before = receiver.retained_structure_bytes_for_test();
    for row in 0..ROWS {
        receiver
            .execute(WIDE_INSERT, &wide_params(row))
            .expect("receiver stores one wide row");
    }
    let row_versions =
        receiver.retained_structure_bytes_for_test().row_versions - structure_before.row_versions;
    receiver
        .set_memory_limit(Some(accountant.usage().used + row_versions / 4))
        .expect("declare room for the unit's shape but not for a second copy of its table");
    (
        StagedRefusal {
            author,
            receiver_path,
            units: [held, later, unrelated],
        },
        receiver,
    )
}

fn deliver(
    receiver: &Database,
    door: ReceivedDoor,
    unit: &(
        ChangeSet,
        Vec<contextdb_engine::protocol::WireDdlProvenance>,
    ),
) -> contextdb_core::Result<()> {
    match door {
        ReceivedDoor::Authenticated => receiver
            .apply_received_schema_for_test(
                unit.0.clone(),
                unit.1.clone(),
                contextdb_core::TenantId::from(HELD_TENANT),
                HELD_SOURCE,
                HELD_INCARNATION,
            )
            .map(|_| ()),
        ReceivedDoor::Unattested => receiver
            .apply_changes(
                unit.0.clone(),
                &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
            )
            .map(|_| ()),
    }
}

fn refusal_operation(refusal: contextdb_core::Result<()>) -> String {
    match refusal {
        Err(Error::MemoryBudgetExceeded { operation, .. }) => operation,
        other => panic!("the refusal must be the typed memory one, not {other:?}"),
    }
}

/// A UNIT REFUSED WHILE IT IS BEING STAGED HOLDS ITS TABLE, EXACTLY AS ONE
/// REFUSED BY ITS PRICE DOES.
///
/// The price covers the shape a unit installs; the private copy of the table
/// it reshapes is charged only while it is made. So a unit can be admitted by
/// its price and still be refused by the copy -- and then it is just as unable
/// to install, and a later unit for the same table must wait behind it just as
/// much. A receiver that recorded the hold only for a price refusal would let
/// a cheaper later unit install first, out of author order. Unrelated tables
/// keep moving, the hold survives a restart, and a retry once the limit allows
/// it installs the unit and releases the table. Through both doors.
fn a_unit_refused_while_staging_holds_its_table(door: ReceivedDoor) {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let (fixture, receiver) =
        a_receiver_that_prices_a_unit_as_fitting_but_cannot_copy_it(root.path());
    let [held, later, unrelated] = &fixture.units;

    let operation = refusal_operation(deliver(&receiver, door, held));
    assert!(
        operation.starts_with("stage_received_unit"),
        "{door:?}: the unit must pass its price and be refused while its table is copied, \
         not by the price itself; refused by `{operation}`"
    );
    assert_eq!(
        refusal_operation(deliver(&receiver, door, later)),
        "received_schema_memory_holdback",
        "{door:?}: a later unit for the table the refused unit reshapes waits behind it"
    );
    deliver(&receiver, door, unrelated)
        .unwrap_or_else(|err| panic!("{door:?}: an unrelated table keeps moving: {err:?}"));
    if let ReceivedDoor::Authenticated = door {
        let held_tables = receiver
            .memory_held_received_schema_units(&contextdb_core::TenantId::from(HELD_TENANT))
            .expect("the receiver reports what it is holding");
        assert_eq!(
            held_tables
                .iter()
                .map(|hold| hold.table.as_str())
                .collect::<Vec<_>>(),
            vec!["wide"],
            "the operator sees exactly the table the refused unit is waiting on"
        );
    }
    // The operator raises the limit and restarts. Memory is no longer short,
    // so what still queues the later unit after the restart is the hold alone,
    // read back from disk.
    receiver
        .set_memory_limit(None)
        .expect("the operator raises the limit");
    receiver.close().expect("close the receiver");

    let reopened = Database::open(&fixture.receiver_path).expect("reopen the receiver");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        refusal_operation(deliver(&reopened, door, later)),
        "received_schema_memory_holdback",
        "{door:?}: the hold is read back after a restart and still queues the later unit"
    );
    deliver(&reopened, door, held)
        .unwrap_or_else(|err| panic!("{door:?}: the retried unit installs: {err:?}"));
    deliver(&reopened, door, later).unwrap_or_else(|err| {
        panic!("{door:?}: once the held unit installs, the later one follows: {err:?}")
    });
    reopened.close().expect("close the reopened receiver");
    fixture.author.close().expect("close the author");
}

#[test]
fn an_authenticated_unit_refused_while_staging_holds_its_table() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "an_authenticated_unit_refused_while_staging_holds_its_table",
    ) {
        return;
    }
    a_unit_refused_while_staging_holds_its_table(ReceivedDoor::Authenticated);
}

#[test]
fn a_unit_with_no_provenance_refused_while_staging_holds_its_table() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_unit_with_no_provenance_refused_while_staging_holds_its_table",
    ) {
        return;
    }
    a_unit_refused_while_staging_holds_its_table(ReceivedDoor::Unattested);
}

/// Where one probing delivery stopped while its table was being copied.
#[derive(Clone, Debug, PartialEq, Eq)]
enum CopyStop {
    /// Refused anywhere else, named `subsystem/operation`: the unit's price,
    /// the table's metadata, the vector copy.
    Refused(String),
    /// Refused while a row version was being charged.
    RowCharge,
    /// Refused while that row's index membership was being rebuilt -- after
    /// the row itself was already admitted.
    IndexMembership,
    /// The whole copy fitted; the armed fault stopped the delivery just past
    /// it, so nothing installed.
    PastCopy,
}

/// An author's reshape of a table that carries a real range index, and a
/// receiver holding that table populated, with nothing declared as a limit
/// yet. `resident` picks an in-memory receiver (whose committed rows stay in
/// memory) or a file-backed one (whose committed rows live on disk).
fn an_indexed_table_pair(
    root: &std::path::Path,
    resident: bool,
    rows: usize,
) -> (
    ChangeSet,
    Vec<contextdb_engine::protocol::WireDdlProvenance>,
    Database,
    Arc<MemoryAccountant>,
) {
    const INDEX: &str = "CREATE INDEX wide_by_body ON wide (body)";
    let author = Database::open(root.join("indexed-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(WIDE_DDL, &HashMap::new())
        .expect("author declares the wide table");
    author
        .execute(INDEX, &HashMap::new())
        .expect("author declares the range index");
    let before_unit = author.current_lsn();
    author
        .execute("ALTER TABLE wide ADD COLUMN extra TEXT", &HashMap::new())
        .expect("author reshapes the table");
    let unit = author.changes_since(before_unit);
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs its own schema unit");
    author.close().expect("close the author");

    let accountant = Arc::new(MemoryAccountant::no_limit());
    let receiver = if resident {
        Database::open_memory_with_accountant(accountant.clone())
    } else {
        Database::open_with_config(
            root.join("indexed-receiver.db"),
            Arc::new(contextdb_engine::plugin::CorePlugin),
            accountant.clone(),
        )
        .expect("open the receiver")
    };
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(WIDE_DDL, &HashMap::new())
        .expect("receiver declares the wide table");
    receiver
        .execute(INDEX, &HashMap::new())
        .expect("receiver declares the range index");
    for row in 0..rows {
        receiver
            .execute(WIDE_INSERT, &wide_params(row))
            .expect("receiver stores one wide row");
    }
    (unit, provenance, receiver, accountant)
}

/// Deliver `unit` under `limit` with a fault armed just past the copy, so no
/// probe ever installs it, and report where it stopped. Every probe, however
/// it stops, must leave the receiver's bill exactly at `settled`.
fn probe_copy_stop(
    receiver: &Database,
    accountant: &MemoryAccountant,
    unit: &ChangeSet,
    provenance: &[contextdb_engine::protocol::WireDdlProvenance],
    settled: usize,
    limit: usize,
) -> CopyStop {
    let stop = probe_copy_stop_unchecked(receiver, unit, provenance, limit);
    let after = accountant.usage().used;
    assert_eq!(
        after, settled,
        "a delivery refused under a {limit} B limit ({stop:?}) must leave the receiver charged \
         for exactly what it held before the unit arrived: it was at {settled} B and is at \
         {after} B"
    );
    stop
}

/// `probe_copy_stop` without the bill check, for the one probe whose
/// delivery legitimately leaves something resident behind.
fn probe_copy_stop_unchecked(
    receiver: &Database,
    unit: &ChangeSet,
    provenance: &[contextdb_engine::protocol::WireDdlProvenance],
    limit: usize,
) -> CopyStop {
    receiver
        .set_memory_limit(Some(limit))
        .expect("declare the probe's limit");
    Database::arm_received_unit_fault_for_test(
        contextdb_engine::ReceivedUnitFaultPoint::AfterWorkingCopy,
        contextdb_engine::ReceivedUnitCopyFault::Error,
    );
    let outcome = receiver.apply_received_schema_for_test(
        unit.clone(),
        provenance.to_vec(),
        contextdb_core::TenantId::from(HELD_TENANT),
        HELD_SOURCE,
        HELD_INCARNATION,
    );
    Database::disarm_received_unit_fault_for_test();
    match outcome {
        Err(Error::MemoryBudgetExceeded {
            subsystem,
            operation,
            ..
        }) => match (subsystem.as_str(), operation.as_str()) {
            (_, "stage_received_unit_row_image") => CopyStop::RowCharge,
            ("relational_index", "membership") => CopyStop::IndexMembership,
            _ => CopyStop::Refused(format!("{subsystem}/{operation}")),
        },
        Err(Error::SyncError(message)) if message.contains("injected fault") => CopyStop::PastCopy,
        other => panic!("a probe either refuses for memory or stops at the armed fault: {other:?}"),
    }
}

/// A REFUSED INDEX REBUILD GIVES BACK THE MEMORY ITS ROW WAS ALREADY
/// CHARGED.
///
/// Copying a table into the private worker charges each row version, then
/// rebuilds that row's place in every index the table declares -- and each
/// index node is admitted against the same declared limit. So a limit can
/// admit the row and refuse its index node. At that moment the row is charged
/// but has not reached the worker's table, and the worker's drop only gives
/// back what its tables hold: a charge with no owner would stay on the
/// receiver's bill for good, and every later unit would be judged against
/// memory the receiver does not hold.
///
/// So every limit across the last rows of the copy -- refusals by the row
/// charge and by the index node alike -- must leave the bill exactly where it
/// was, and a following delivery with room installs. Both the in-memory and
/// the file-backed receiver take this path.
fn a_refused_index_rebuild_returns_the_rows_charge(resident: bool) {
    const ROWS: usize = 40;
    const STEP: usize = 61;
    const SWEEP: usize = 6 * 1024;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let (unit, provenance, receiver, accountant) =
        an_indexed_table_pair(root.path(), resident, ROWS);
    let settled = accountant.usage().used;

    // Find the highest limit that still stops inside the rows. Stops are
    // ordered by limit: a larger limit never stops earlier.
    let mut low = settled;
    let mut high = settled + 16 * 1024 * 1024;
    assert_eq!(
        probe_copy_stop(&receiver, &accountant, &unit, &provenance, settled, high),
        CopyStop::PastCopy,
        "the search's upper bound fits the whole copy"
    );
    assert_ne!(
        probe_copy_stop(&receiver, &accountant, &unit, &provenance, settled, low),
        CopyStop::PastCopy,
        "a limit with no room at all stops before the copy completes"
    );
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        match probe_copy_stop(&receiver, &accountant, &unit, &provenance, settled, middle) {
            CopyStop::PastCopy => high = middle,
            _ => low = middle,
        }
    }

    let mut index_refusals = 0usize;
    let mut row_refusals = 0usize;
    let mut limit = low;
    while limit + SWEEP > low && limit > settled {
        match probe_copy_stop(&receiver, &accountant, &unit, &provenance, settled, limit) {
            CopyStop::IndexMembership => index_refusals += 1,
            CopyStop::RowCharge => row_refusals += 1,
            CopyStop::Refused(_) | CopyStop::PastCopy => {}
        }
        limit -= STEP;
    }
    assert!(
        index_refusals > 0 && row_refusals > 0,
        "the sweep must reach both a refused row and a refused index node after its row was \
         admitted; saw {row_refusals} row refusals and {index_refusals} index refusals"
    );

    // The bill still means something: with room for the copy and for what the
    // unit installs, the same delivery goes through.
    receiver
        .set_memory_limit(Some(high + 1024 * 1024))
        .expect("declare room for the unit");
    receiver
        .apply_received_schema_for_test(
            unit,
            provenance,
            contextdb_core::TenantId::from(HELD_TENANT),
            HELD_SOURCE,
            HELD_INCARNATION,
        )
        .expect("a delivery with room installs after the refusals");
    assert!(
        receiver
            .table_meta("wide")
            .expect("the wide table is still declared")
            .columns
            .iter()
            .any(|column| column.name == "extra"),
        "the unit installed"
    );
    receiver.close().expect("close the receiver");
}

#[test]
fn a_refused_index_rebuild_returns_the_rows_charge_in_memory() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_refused_index_rebuild_returns_the_rows_charge_in_memory",
    ) {
        return;
    }
    a_refused_index_rebuild_returns_the_rows_charge(true);
}

#[test]
fn a_refused_index_rebuild_returns_the_rows_charge_on_disk() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_refused_index_rebuild_returns_the_rows_charge_on_disk",
    ) {
        return;
    }
    a_refused_index_rebuild_returns_the_rows_charge(false);
}

/// The highest limit under which a probing delivery still stops before its
/// copy is complete, and the lowest one under which the whole copy fits.
fn copy_boundary(
    receiver: &Database,
    accountant: &MemoryAccountant,
    unit: &ChangeSet,
    provenance: &[contextdb_engine::protocol::WireDdlProvenance],
    settled: usize,
) -> (usize, usize) {
    let mut low = settled;
    let mut high = settled + 64 * 1024 * 1024;
    assert_eq!(
        probe_copy_stop(receiver, accountant, unit, provenance, settled, high),
        CopyStop::PastCopy,
        "the search's upper bound fits the whole copy"
    );
    assert_ne!(
        probe_copy_stop(receiver, accountant, unit, provenance, settled, low),
        CopyStop::PastCopy,
        "a limit with no room at all stops before the copy completes"
    );
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        match probe_copy_stop(receiver, accountant, unit, provenance, settled, middle) {
            CopyStop::PastCopy => high = middle,
            _ => low = middle,
        }
    }
    (low, high)
}

const DOCS_DDL: &str = "CREATE TABLE docs (id UUID PRIMARY KEY, embedding VECTOR(64)) SYNC TWO WAY";
const BULK_DDL: &str = "CREATE TABLE bulk (id UUID PRIMARY KEY, embedding VECTOR(64)) SYNC TWO WAY";
const VECTOR_DIMENSIONS: usize = 64;

fn vector_params(row: usize) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_owned(), Value::Uuid(event_uuid(row))),
        (
            "embedding".to_owned(),
            Value::Vector(
                (0..VECTOR_DIMENSIONS)
                    .map(|dimension| ((row * 31 + dimension) % 97) as f32 + 1.0)
                    .collect(),
            ),
        ),
    ])
}

/// A VECTOR COPY IS ADMITTED BEFORE IT IS ALLOCATED, AND ONLY THE VECTORS A
/// UNIT RESHAPES ARE COPIED.
///
/// Taking delivery of a unit that reshapes a table with a vector column
/// copies that column's vectors into the private worker: for a moment the
/// receiver holds them twice. The declared limit bounds that moment only if
/// the copy is admitted before it exists and the admission is held until the
/// copied vectors carry their own charges. And a copy that begins by cloning
/// EVERY vector the receiver holds -- tables the unit never touches included
/// -- puts the process over its limit before anything is asked.
///
/// Beside the reshaped `docs` table sits a much larger, unrelated `bulk`
/// table. A delivery that gets past the copy must have cloned `docs`' vectors
/// and nothing else; a limit below what the `docs` copy needs refuses it with
/// the typed memory refusal, without the process growing by anywhere near
/// `bulk`'s size, and leaves the bill exactly where it was. `saved` reopens a
/// file-backed receiver first, so its vectors start on disk.
fn a_vector_copy_is_admitted_before_it_is_allocated(resident: bool, saved: bool) {
    const AFFECTED: usize = 120;
    const UNRELATED: usize = 3_000;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    let author = Database::open(root.path().join("vector-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for ddl in [DOCS_DDL, BULK_DDL] {
        author
            .execute(ddl, &HashMap::new())
            .expect("author declares its tables");
    }
    let before_unit = author.current_lsn();
    author
        .execute(
            "ALTER TABLE docs ADD COLUMN second VECTOR(8)",
            &HashMap::new(),
        )
        .expect("author reshapes docs");
    let unit = author.changes_since(before_unit);
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs its own schema unit");
    author.close().expect("close the author");

    let receiver_path = root.path().join("vector-receiver.db");
    let open_receiver = |accountant: &Arc<MemoryAccountant>| {
        let receiver = if resident {
            Database::open_memory_with_accountant(accountant.clone())
        } else {
            Database::open_with_config(
                receiver_path.clone(),
                Arc::new(contextdb_engine::plugin::CorePlugin),
                accountant.clone(),
            )
            .expect("open the receiver")
        };
        receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        receiver
    };
    let mut accountant = Arc::new(MemoryAccountant::no_limit());
    let mut receiver = open_receiver(&accountant);
    for ddl in [DOCS_DDL, BULK_DDL] {
        receiver
            .execute(ddl, &HashMap::new())
            .expect("receiver declares its tables");
    }
    for (table, rows, offset) in [("docs", AFFECTED, 0), ("bulk", UNRELATED, 1_000_000)] {
        let tx = receiver.begin().expect("begin the fill");
        for row in 0..rows {
            receiver
                .execute_in_tx(
                    tx,
                    &format!("INSERT INTO {table} (id, embedding) VALUES ($id, $embedding)"),
                    &vector_params(offset + row),
                )
                .expect("receiver stores one vector row");
        }
        receiver.commit(tx).expect("commit the fill");
    }
    if saved {
        receiver.close().expect("close the receiver");
        accountant = Arc::new(MemoryAccountant::no_limit());
        receiver = open_receiver(&accountant);
    }
    let unrelated_payload = UNRELATED * VECTOR_DIMENSIONS * std::mem::size_of::<f32>();
    let before = accountant.usage().used;

    // Past the copy: exactly the reshaped table's vectors were cloned.
    let trace = ReceivedSchemaApplyObserverForTest::new();
    let scope = receiver.observe_received_schema_applies_for_test(&trace);
    assert_eq!(
        probe_copy_stop_unchecked(&receiver, &unit, &provenance, before + 64 * 1024 * 1024),
        CopyStop::PastCopy,
        "a limit with ample room gets past the copy"
    );
    drop(scope);
    // Vectors saved on disk are read into the receiver to be copied, and stay
    // resident -- charged -- as any read of them would leave them. Only the
    // reshaped table's may be: that is `docs`' payload, not `bulk`'s.
    let settled = accountant.usage().used;
    let expected_residency = if saved {
        AFFECTED * VECTOR_DIMENSIONS * std::mem::size_of::<f32>()
    } else {
        0
    };
    assert_eq!(
        settled - before,
        expected_residency,
        "getting past the copy leaves resident only the reshaped table's saved vectors \
         ({expected_residency} B); the receiver grew by {} B",
        settled - before
    );
    let cloned = trace
        .storage_observation()
        .profile_work
        .get("staged-vector-entries-cloned")
        .copied()
        .unwrap_or_default();
    assert_eq!(
        cloned, AFFECTED as u64,
        "taking delivery of a unit that reshapes docs clones docs' {AFFECTED} vectors and none \
         of the {UNRELATED} in the unrelated table; it cloned {cloned}"
    );

    // Just below what the whole copy needs: refused while the vectors are
    // admitted, before any is allocated.
    let (below, _) = copy_boundary(&receiver, &accountant, &unit, &provenance, settled);
    let trace = ReceivedSchemaApplyObserverForTest::new();
    let scope = receiver.observe_received_schema_applies_for_test(&trace);
    restart_peak();
    let start_live = live_bytes();
    let stop = probe_copy_stop(&receiver, &accountant, &unit, &provenance, settled, below);
    let peak_growth = peak_growth_since(start_live);
    drop(scope);
    assert!(
        matches!(&stop, CopyStop::Refused(operation) if operation.contains("vector")),
        "the last thing a limit just short of the copy refuses is the vector copy; it stopped \
         at {stop:?}"
    );
    let cloned_at_refusal = trace
        .storage_observation()
        .profile_work
        .get("staged-vector-entries-cloned")
        .copied()
        .unwrap_or_default();
    assert_eq!(
        cloned_at_refusal, 0,
        "a vector copy refused for memory is refused before a single vector is cloned; \
         {cloned_at_refusal} were cloned first"
    );
    assert!(
        peak_growth < unrelated_payload as i64 / 2,
        "a refused vector copy must not let the process grow anywhere near the {unrelated_payload} \
         B the unrelated table's vectors hold; it grew by {peak_growth} B"
    );

    // With room, the same delivery installs.
    receiver.set_memory_limit(None).expect("clear the limit");
    receiver
        .apply_received_schema_for_test(unit, provenance, tenant, HELD_SOURCE, HELD_INCARNATION)
        .expect("a delivery with room installs after the refusals");
    assert!(
        receiver
            .table_meta("docs")
            .expect("docs is still declared")
            .columns
            .iter()
            .any(|column| column.name == "second"),
        "the unit installed"
    );
    receiver.close().expect("close the receiver");
}

#[test]
fn a_vector_copy_is_admitted_before_it_is_allocated_in_memory() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_vector_copy_is_admitted_before_it_is_allocated_in_memory",
    ) {
        return;
    }
    a_vector_copy_is_admitted_before_it_is_allocated(true, false);
}

#[test]
fn a_vector_copy_is_admitted_before_it_is_allocated_on_disk() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_vector_copy_is_admitted_before_it_is_allocated_on_disk",
    ) {
        return;
    }
    a_vector_copy_is_admitted_before_it_is_allocated(false, false);
}

#[test]
fn a_saved_vector_copy_is_admitted_before_it_is_loaded() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_saved_vector_copy_is_admitted_before_it_is_loaded",
    ) {
        return;
    }
    a_vector_copy_is_admitted_before_it_is_allocated(false, true);
}

/// A FILE-BACKED RECEIVER OBTAINS MEMORY FOR EACH ROW OF THE TABLE IT COPIES
/// BEFORE IT LOADS THAT ROW.
///
/// A file-backed receiver keeps its committed rows on disk, so copying a
/// table a unit reshapes means reading them back. Reading the whole table
/// into memory and only then charging it row by row puts the process over a
/// declared limit before the first charge can refuse anything: the limit
/// bounds the figure the refusal settles back to, not the process.
///
/// A limit with room for the table's metadata and a few of its rows, far
/// below the whole table, must refuse the copy with the typed memory refusal
/// after reading only those rows -- the process never grows by anywhere near
/// the table's size -- leave the bill exactly where it was, and a later
/// delivery with room installs.
#[test]
fn a_file_backed_receiver_obtains_memory_before_loading_the_table_it_copies() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "a_file_backed_receiver_obtains_memory_before_loading_the_table_it_copies",
    ) {
        return;
    }
    const ROWS: usize = 4_000;
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let tenant = contextdb_core::TenantId::from(HELD_TENANT);

    let author = Database::open(root.path().join("paged-author.db")).expect("open the author");
    author.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    author
        .execute(WIDE_DDL, &HashMap::new())
        .expect("author declares the wide table");
    let before_unit = author.current_lsn();
    author
        .execute("ALTER TABLE wide ADD COLUMN extra TEXT", &HashMap::new())
        .expect("author reshapes the table");
    let unit = author.changes_since(before_unit);
    let provenance = author
        .outbound_ddl_provenance_for_test(
            &unit,
            &contextdb_core::TenantId::from(HELD_TENANT),
            &fixture_schema_author().node_id(),
            HELD_INCARNATION,
            &fixture_signer(),
        )
        .expect("the author signs its own schema unit");
    author.close().expect("close the author");

    let receiver_path = root.path().join("paged-receiver.db");
    let receiver = Database::open(&receiver_path).expect("open the receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .execute(WIDE_DDL, &HashMap::new())
        .expect("receiver declares the wide table");
    let tx = receiver.begin().expect("begin the fill");
    for row in 0..ROWS {
        receiver
            .execute_in_tx(tx, WIDE_INSERT, &wide_params(row))
            .expect("receiver stores one wide row");
    }
    receiver.commit(tx).expect("commit the fill");
    receiver.close().expect("close the filled receiver");
    // Reopened, its committed rows are on disk, not in memory.
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let receiver = Database::open_with_config(
        receiver_path,
        Arc::new(contextdb_engine::plugin::CorePlugin),
        accountant.clone(),
    )
    .expect("reopen the receiver");
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let settled = accountant.usage().used;
    let table_payload = ROWS * wide_params(0)["body"].estimated_bytes();

    // The lowest limit under which the copy reaches the rows at all.
    let mut low = settled;
    let mut high = settled + 64 * 1024 * 1024;
    assert_eq!(
        probe_copy_stop(&receiver, &accountant, &unit, &provenance, settled, high),
        CopyStop::PastCopy,
        "the search's upper bound fits the whole copy"
    );
    let reaches_rows = |stop: &CopyStop| !matches!(stop, CopyStop::Refused(_));
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        if reaches_rows(&probe_copy_stop(
            &receiver,
            &accountant,
            &unit,
            &provenance,
            settled,
            middle,
        )) {
            high = middle;
        } else {
            low = middle;
        }
    }

    // Room for the metadata and a handful of rows.
    let limit = high + 8 * 1024;
    let trace = ReceivedSchemaApplyObserverForTest::new();
    let scope = receiver.observe_received_schema_applies_for_test(&trace);
    restart_peak();
    let start_live = live_bytes();
    let stop = probe_copy_stop(&receiver, &accountant, &unit, &provenance, settled, limit);
    let peak_growth = peak_growth_since(start_live);
    drop(scope);
    assert_eq!(
        stop,
        CopyStop::RowCharge,
        "a limit with room for a few rows refuses the copy at a row"
    );
    let observation = trace.storage_observation();
    let loaded = observation
        .profile_work
        .get("staged-rows-loaded")
        .copied()
        .unwrap_or_default();
    let charged = observation.staged_rows_copied as u64;
    assert!(
        loaded <= charged,
        "rows are read from disk one at a time, each charged before the next is read; the \
         refused copy read {loaded} rows of {ROWS} while charging {charged}"
    );
    assert!(
        peak_growth < table_payload as i64 / 4,
        "a copy refused a few rows in must not let the process grow anywhere near the \
         {table_payload} B the table holds; it grew by {peak_growth} B"
    );

    receiver.set_memory_limit(None).expect("clear the limit");
    receiver
        .apply_received_schema_for_test(unit, provenance, tenant, HELD_SOURCE, HELD_INCARNATION)
        .expect("a delivery with room installs after the refusals");
    assert!(
        receiver
            .table_meta("wide")
            .expect("the wide table is still declared")
            .columns
            .iter()
            .any(|column| column.name == "extra"),
        "the unit installed"
    );
    receiver.close().expect("close the receiver");
}

/// A receiver takes schema only from the author whose own key signed it.
///
/// Authenticated received DDL is immutable after transport validation, and
/// that its SIGNED provenance -- source LSN, ordinal, generation and canonical
/// digest -- attests to the schema the AUTHOR actually sent. Rows already keep
/// that promise: every row carries its creator's signature and every hop
/// checks it. Schema did not: the provenance carried an unkeyed digest,
/// believed because it arrived on the delivering peer's authenticated session.
/// A hub in the middle could therefore forward schema under any author's name,
/// or rewrite the statement and re-derive the digest over its own substitute,
/// and no edge downstream could tell. An edge that wipes and restores its
/// schema from a hub was trusting the hub's word for who wrote it.
///
/// These peers are real, file-backed databases, each holding its own ed25519
/// fabric identity key on disk, so the author identity a unit names is a
/// public key some peer actually has the secret for.
const AUTHORED_SCHEMA_TENANT: &str = "authored-schema-tenant";
const AUTHORED_SCHEMA_INCARNATION: contextdb_core::Incarnation = contextdb_core::Incarnation(31);
const AUTHORED_SCHEMA_DDL: &str = "CREATE TABLE div (id UUID PRIMARY KEY, old TEXT) SYNC TWO WAY";
const AUTHORED_SCHEMA_CHANGE: &str = "ALTER TABLE div ADD COLUMN note TEXT";
const SUBSTITUTED_SCHEMA_CHANGE: &str = "ALTER TABLE div ADD COLUMN secret TEXT";

/// One peer in a schema journey: its store, its own identity key on disk, and
/// the path both live under.
struct SchemaPeer {
    db: Database,
    identity: contextdb_engine::identity::FabricIdentity,
    path: std::path::PathBuf,
}

impl SchemaPeer {
    fn open(root: &std::path::Path, name: &str) -> Self {
        let path = root.join(format!("{name}.db"));
        let identity = contextdb_engine::identity::FabricIdentity::load_or_generate(
            &root.join(format!("{name}.fabric-identity.key")),
        )
        .expect("the peer holds its own fabric identity key on disk");
        let db = Database::open(&path).expect("open a schema-journey peer");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute(AUTHORED_SCHEMA_DDL, &HashMap::new())
            .expect("the peer declares the table the journey reshapes");
        Self { db, identity, path }
    }

    fn node_id(&self) -> String {
        self.identity.node_id()
    }

    /// The provenance this peer would send with `unit`: its own signature when
    /// it authored the schema, and the author's unchanged when it is
    /// forwarding schema it received.
    fn outbound(&self, unit: &ChangeSet) -> Vec<contextdb_engine::protocol::WireDdlProvenance> {
        self.db
            .outbound_ddl_provenance_for_test(
                unit,
                &contextdb_core::TenantId::from(AUTHORED_SCHEMA_TENANT),
                &self.node_id(),
                AUTHORED_SCHEMA_INCARNATION,
                &|bytes| Ok(self.identity.sign_for_test(bytes)),
            )
            .expect("the sender carries the unit's provenance")
    }

    fn reopen(self) -> Self {
        self.db.close().expect("close the peer");
        drop(self.db);
        let db = Database::open(&self.path).expect("reopen a schema-journey peer");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        Self {
            db,
            identity: self.identity,
            path: self.path,
        }
    }

    fn take(
        &self,
        unit: ChangeSet,
        provenance: Vec<contextdb_engine::protocol::WireDdlProvenance>,
        sender: &str,
    ) -> contextdb_core::Result<()> {
        self.db
            .apply_received_schema_for_test(
                unit,
                provenance,
                contextdb_core::TenantId::from(AUTHORED_SCHEMA_TENANT),
                sender,
                AUTHORED_SCHEMA_INCARNATION,
            )
            .map(|_| ())
    }

    fn holds_column(&self, column: &str) -> bool {
        self.db
            .table_meta("div")
            .expect("the table is still declared")
            .columns
            .iter()
            .any(|declared| declared.name == column)
    }
}

/// The author's own reshaping of its table, as one schema-only unit.
fn authored_schema_unit(author: &SchemaPeer, statement: &str) -> ChangeSet {
    let before = author.db.current_lsn();
    author
        .db
        .execute(statement, &HashMap::new())
        .expect("the author reshapes its own table");
    author.db.changes_since(before)
}

#[test]
fn an_edge_refuses_schema_a_hub_forwards_under_another_authors_name() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "an_edge_refuses_schema_a_hub_forwards_under_another_authors_name",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let author = SchemaPeer::open(root.path(), "author");
    let hub = SchemaPeer::open(root.path(), "hub");
    let edge = SchemaPeer::open(root.path(), "edge");
    let impostor = contextdb_engine::identity::FabricIdentity::load_or_generate(
        &root.path().join("impostor.fabric-identity.key"),
    )
    .expect("the impostor holds a key of its own");

    let unit = authored_schema_unit(&author, AUTHORED_SCHEMA_CHANGE);
    let provenance = author.outbound(&unit);
    let hub_before = hub.db.current_lsn();
    hub.take(unit, provenance, &author.node_id())
        .expect("the hub takes the author's own signed unit");

    // The hub forwards what it received, but names a different author for it.
    let forwarded = hub.db.changes_since(hub_before);
    assert!(
        !forwarded.ddl.is_empty(),
        "the hub has the received schema change to forward"
    );
    let mut renamed = hub.outbound(&forwarded);
    for entry in &mut renamed {
        let named = entry
            .author
            .as_mut()
            .expect("a forwarded unit names the author it came from");
        named.node_id = impostor.node_id();
    }

    let refusal = edge.take(forwarded, renamed, &hub.node_id());
    let error = refusal.expect_err(
        "an edge must refuse schema whose named author never signed it -- otherwise a hub that \
         renames the author of what it forwards is simply believed, and an edge that wipes and \
         restores takes the hub's word for who wrote its schema",
    );
    assert!(
        format!("{error:?}").contains("author"),
        "the refusal names the author check it failed: {error:?}"
    );
    assert!(
        !edge.holds_column("note"),
        "nothing of the renamed unit is installed"
    );
    author.db.close().expect("close the author");
    hub.db.close().expect("close the hub");
    edge.db.close().expect("close the edge");
}

#[test]
fn an_edge_refuses_a_statement_that_was_swapped_in_flight() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "an_edge_refuses_a_statement_that_was_swapped_in_flight",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let author = SchemaPeer::open(root.path(), "swap-author");
    let edge = SchemaPeer::open(root.path(), "swap-edge");
    let substitute_source = SchemaPeer::open(root.path(), "swap-substitute");

    let unit = authored_schema_unit(&author, AUTHORED_SCHEMA_CHANGE);
    let provenance = author.outbound(&unit);

    // Somewhere between the author and the edge, the statement is replaced and
    // the transport digest re-derived over the substitute, so every unkeyed
    // check still agrees with itself. Only the author's own signature over the
    // statement it really sent can catch this.
    let substitute = authored_schema_unit(&substitute_source, SUBSTITUTED_SCHEMA_CHANGE);
    let mut swapped = unit.clone();
    swapped.ddl = substitute.ddl.clone();
    let mut swapped_provenance = provenance.clone();
    for (entry, ddl) in swapped_provenance.iter_mut().zip(&swapped.ddl) {
        entry.digest = contextdb_engine::protocol::canonical_ddl_provenance_digest(
            &contextdb_engine::protocol::WireDdlChange::from(ddl.clone()),
            entry.source_ddl_lsn,
            entry.ordinal,
            entry.table.as_deref(),
            entry.table_generation,
        )
        .expect("the substitute's own transport digest");
    }

    let refusal = edge.take(swapped, swapped_provenance, &author.node_id());
    let error = refusal.expect_err(
        "an edge must refuse a statement its named author never signed -- a re-derived transport \
         digest agrees with whatever statement was put in front of it",
    );
    assert!(
        !edge.holds_column("secret"),
        "the substituted column is not installed: {error:?}"
    );
    assert!(
        !edge.holds_column("note"),
        "and neither is the author's own, since the unit was refused whole"
    );
    author.db.close().expect("close the author");
    edge.db.close().expect("close the edge");
    substitute_source
        .db
        .close()
        .expect("close the substitute source");
}

#[test]
fn an_authors_signature_travels_two_hops_a_restart_and_a_third_peer() {
    if isolated_process::run_in_own_process(
        module_path!(),
        "an_authors_signature_travels_two_hops_a_restart_and_a_third_peer",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let author = SchemaPeer::open(root.path(), "journey-author");
    let hub = SchemaPeer::open(root.path(), "journey-hub");
    let edge = SchemaPeer::open(root.path(), "journey-edge");
    let far = SchemaPeer::open(root.path(), "journey-far");

    let unit = authored_schema_unit(&author, AUTHORED_SCHEMA_CHANGE);
    let provenance = author.outbound(&unit);
    assert_eq!(
        provenance
            .iter()
            .map(|entry| entry
                .author
                .as_ref()
                .expect("the author names itself")
                .node_id
                .clone())
            .collect::<Vec<_>>(),
        vec![author.node_id(); provenance.len()],
        "the author sends its own identity with the schema it wrote"
    );
    assert!(
        provenance
            .iter()
            .all(|entry| !entry.author.as_ref().unwrap().attestation.is_empty()),
        "and signs it"
    );

    // Hop one: the hub takes it, then forwards it to an edge.
    let hub_before = hub.db.current_lsn();
    hub.take(unit, provenance, &author.node_id())
        .expect("the hub takes the author's signed unit");
    let forwarded = hub.db.changes_since(hub_before);
    let forwarded_provenance = hub.outbound(&forwarded);
    assert_eq!(
        forwarded_provenance[0]
            .author
            .as_ref()
            .expect("the hub forwards a named author")
            .node_id,
        author.node_id(),
        "the hub forwards the author's identity, not its own"
    );
    let edge_before = edge.db.current_lsn();
    edge.take(forwarded, forwarded_provenance, &hub.node_id())
        .expect("the edge takes schema two hops from its author");
    assert!(
        edge.holds_column("note"),
        "the honestly delivered change installs"
    );

    // The signature is durable, not a property of the session it arrived on.
    let edge = edge.reopen();
    assert!(
        edge.holds_column("note"),
        "the change is still installed after the edge restarts"
    );

    // Hop two: the restarted edge serves it on to a fourth peer, still under
    // the author's name and still with the author's own signature.
    let onward = edge.db.changes_since(edge_before);
    assert!(
        !onward.ddl.is_empty(),
        "the restarted edge has the received change to serve on"
    );
    let onward_provenance = edge.outbound(&onward);
    let named = onward_provenance[0]
        .author
        .as_ref()
        .expect("the restarted edge names the author");
    assert_eq!(
        named.node_id,
        author.node_id(),
        "a restart does not turn the edge into the author"
    );
    assert!(
        !named.attestation.is_empty(),
        "and the author's own signature is served with it, not re-made by the edge"
    );
    far.take(onward, onward_provenance, &edge.node_id())
        .expect("a fourth peer takes schema three hops from its author");
    assert!(
        far.holds_column("note"),
        "the change installs at the far end of the journey"
    );

    author.db.close().expect("close the author");
    hub.db.close().expect("close the hub");
    edge.db.close().expect("close the edge");
    far.db.close().expect("close the far peer");
}
