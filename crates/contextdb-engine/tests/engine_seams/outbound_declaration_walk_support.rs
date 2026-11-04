//! Shared setup for the rows proving that an outbound push or pending read
//! holds none of the received declarations it suppresses.
//!
//! Every row has two schedules. The PR gate runs one history of
//! [`PR_SIZE`] received declarations and checks exactly what the operation
//! walked, what reached the final filter, what it dropped, how many
//! validation records it held, which epoch value it used and what it sent;
//! every one of those is observed in an engine access function the outbound
//! walk cannot route around, plus what the walk kept charged with the memory
//! accountant from one walked occurrence to the next. The nightly schedule
//! (`--features nightly-tests`) runs [`NIGHTLY_SIZES`] with the same outcome
//! checks, leaves those structural checks to the PR gate, and bounds the
//! growth of the accountant peak by a control pair (see
//! [`assert_bounded_by_control`]).
//!
//! A run is deterministic: fixed fabric seeds, pinned sync incarnations and
//! one fixed wall clock, because the push envelope encodes each payload byte
//! at or above 0x80 in two bytes and so moves with any random or clock-derived
//! byte. For the same reason the edge must push from the same LSN at both
//! nightly sizes: the pulled-edge fixtures pad the smaller history with local
//! commits behind the first push's frontier, and the moved-edge and
//! first-pull fixtures receive their history in a fixed number of commits.
//! Each nightly row asserts the equal LSN before comparing peaks.
#![allow(
    dead_code,
    reason = "each row uses the part of the shared setup it needs"
)]

use contextdb_core::{Incarnation, Lsn, TenantId, Wallclock, WallclockTestClockGuard};
use contextdb_engine::database::DdlOccurrenceForTest;
use contextdb_engine::outbound_ddl_test_seams::{
    Occurrence, OutboundDdlEvent, OutboundDdlWork, SentDdl, observe,
};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// The received-history size the PR gate runs.
pub(crate) const PR_SIZE: usize = 16;

/// The two received-history sizes the nightly schedule compares.
pub(crate) const NIGHTLY_SIZES: [usize; 2] = [16, 1_024];

/// Walk-from-zero pushes separate walk custody from the durable reply commit.
pub(crate) const WALK_FROM_ZERO_SIZES: [usize; 3] = [32, 256, 1_024];

/// Slack for cursor-window rounding: the history and row cursors charge whole
/// windows, so the same walk over a larger store can end a window at a
/// different byte and hold up to one window more at its peak.
pub(crate) const CURSOR_WINDOW_SLACK: usize = 4_096;

/// The most the store-size term may contribute between the two nightly sizes.
/// A bigger file costs a few more page reads and proof bytes while the same
/// history is walked (about 31 KB measured on the parent); a control delta
/// above this bound means the control no longer measures only store size.
pub(crate) const STORE_SIZE_TERM_BOUND: usize = 64 * 1_024;

const FIXED_WALLCLOCK_MS: u64 = 1_800_000_000_000;

pub(crate) fn tenant() -> TenantId {
    TenantId::from("outbound-declaration-walk")
}

/// One fixed clock on this thread and every runtime worker.
pub(crate) fn fixed_clock() -> WallclockTestClockGuard {
    Wallclock::test_clock_guard(|| FIXED_WALLCLOCK_MS)
}

pub(crate) fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .on_thread_start(|| Wallclock::set_test_clock(|| FIXED_WALLCLOCK_MS))
        .enable_all()
        .build()
        .unwrap()
}

pub(crate) fn open(path: &Path) -> Arc<Database> {
    Arc::new(
        Database::open_with_options(
            path,
            DatabaseOpenOptions {
                memory_limit: Some(64 << 20),
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap(),
    )
}

pub(crate) fn sql(db: &Database, statement: &str) {
    db.execute(statement, &HashMap::new())
        .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
}

/// One transaction holding every statement, so all of them share one LSN.
pub(crate) fn one_commit(db: &Database, statements: &[String]) {
    sql(db, "BEGIN");
    for statement in statements {
        sql(db, statement);
    }
    sql(db, "COMMIT");
}

/// `declarations` unrelated declarations, four per cycle, each its own commit.
pub(crate) fn churn(db: &Database, prefix: &str, declarations: usize) {
    assert_eq!(declarations % 4, 0);
    for cycle in 0..declarations / 4 {
        let table = format!("{prefix}_{cycle}");
        sql(
            db,
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY) SYNC OFF"),
        );
        sql(db, &format!("ALTER TABLE {table} ADD COLUMN spare INTEGER"));
        sql(db, &format!("ALTER TABLE {table} SET SYNC OFF"));
        sql(db, &format!("DROP TABLE {table}"));
    }
}

/// The number of commits [`grouped_churn`] always makes.
pub(crate) const GROUPED_CHURN_COMMITS: usize = 16;

/// `declarations` unrelated declarations in exactly
/// [`GROUPED_CHURN_COMMITS`] commits, so a receiver's LSN advances the same
/// at every size.
pub(crate) fn grouped_churn(db: &Database, prefix: &str, declarations: usize) {
    assert_eq!(declarations % GROUPED_CHURN_COMMITS, 0);
    for commit in 0..GROUPED_CHURN_COMMITS {
        one_commit(
            db,
            &large_commit(
                &format!("{prefix}_{commit}"),
                declarations / GROUPED_CHURN_COMMITS,
            ),
        );
    }
}

/// The statements of one large schema commit: `declarations` declarations on
/// one table whose shape stays small (a transaction cannot drop a table).
pub(crate) fn large_commit(prefix: &str, declarations: usize) -> Vec<String> {
    let table = format!("{prefix}_table");
    std::iter::once(format!(
        "CREATE TABLE {table} (id INTEGER PRIMARY KEY) SYNC OFF"
    ))
    .chain((1..declarations).map(|index| {
        if index % 2 == 1 {
            format!("ALTER TABLE {table} ADD COLUMN spare INTEGER")
        } else {
            format!("ALTER TABLE {table} DROP COLUMN spare")
        }
    }))
    .collect()
}

/// `commits` local single-row commits into a table that never syncs. Run
/// before a frontier, they move every later LSN without adding anything a
/// later push walks.
pub(crate) fn pad_commits(db: &Database, commits: usize) {
    sql(db, "CREATE TABLE lsn_pad (id INTEGER PRIMARY KEY) SYNC OFF");
    for id in 0..commits {
        sql(db, &format!("INSERT INTO lsn_pad VALUES ({id})"));
    }
}

/// Rows of store ballast a control sample adds per received declaration of the
/// size it stands in for. Four id-only rows grow the edge's file by about as
/// much as one received declaration does (measured on the parent: 1,008
/// declarations grow a pulled edge by 6.1 MB, 4,032 ballast rows by 6.8 MB).
pub(crate) const BALLAST_ROWS_PER_DECLARATION: usize = 4;

/// The fixed number of commits ballast is written in, at every size, so both
/// control samples work at the same LSN.
pub(crate) const BALLAST_COMMITS: usize = 16;

/// Ballast ids start here, above every id a fixture writes itself.
const BALLAST_FIRST_ID: usize = 10_000_000;

thread_local! {
    static CONTROL_BALLAST: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// Run one nightly sample as the control for `size`: the row runs at
/// [`PR_SIZE`] received declarations, so it walks exactly the history the
/// small measured sample walks, and its fixture adds [`store_ballast`]
/// standing in for `size` declarations. The two control samples therefore
/// walk the same history and differ only in how big the store is.
pub(crate) fn control_sample<T>(size: usize, row: impl FnOnce() -> T) -> T {
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            CONTROL_BALLAST.with(|ballast| ballast.set(None));
        }
    }
    CONTROL_BALLAST.with(|ballast| ballast.set(Some(size)));
    let _restore = Restore;
    row()
}

/// Whether this sample is a control sample.
pub(crate) fn in_control_sample() -> bool {
    CONTROL_BALLAST.with(std::cell::Cell::get).is_some()
}

/// In a control sample, write `size` x [`BALLAST_ROWS_PER_DECLARATION`] rows
/// into `table`, a `SYNC OFF` table, in [`BALLAST_COMMITS`] commits. The rows
/// add no declaration and never leave the edge; they only grow its file and
/// B-trees. Every control uses this one shape. A fixture writes them behind
/// the push frontier wherever it has one, so no later walk passes them; a
/// push from frontier 0 and a destination re-upload walk the whole store, so
/// there the rows sit inside the walk. Outside a control sample it does
/// nothing.
pub(crate) fn store_ballast(db: &Database, table: &str) {
    let Some(size) = CONTROL_BALLAST.with(std::cell::Cell::get) else {
        return;
    };
    let per_commit = size * BALLAST_ROWS_PER_DECLARATION / BALLAST_COMMITS;
    for commit in 0..BALLAST_COMMITS {
        one_commit(
            db,
            &(0..per_commit)
                .map(|row| {
                    format!(
                        "INSERT INTO {table} (id) VALUES ({})",
                        BALLAST_FIRST_ID + commit * per_commit + row
                    )
                })
                .collect::<Vec<_>>(),
        );
    }
}

/// The local commits a nightly fixture of `size` adds so its LSNs match the
/// largest size's; the PR gate adds none.
pub(crate) fn nightly_pad(size: usize, nightly: bool) -> Option<usize> {
    nightly.then(|| NIGHTLY_SIZES[1] - size)
}

pub(crate) fn ids(db: &Database, table: &str) -> Vec<i64> {
    db.execute(
        &format!("SELECT id FROM {table} ORDER BY id"),
        &HashMap::new(),
    )
    .unwrap()
    .rows()
    .iter()
    .map(|row| match &row[0] {
        contextdb_core::Value::Int64(id) => *id,
        other => panic!("{table}.id is an integer, not {other:?}"),
    })
    .collect()
}

pub(crate) fn occurrences(db: &Database) -> Vec<DdlOccurrenceForTest> {
    db.ddl_occurrences_for_test()
        .expect("read the durable declaration occurrences")
}

/// The local source LSNs whose every occurrence is marked received, with the
/// group sizes, in history order.
pub(crate) fn received_groups(db: &Database) -> Vec<(Lsn, u32)> {
    let mut groups = Vec::<(Lsn, u32, bool)>::new();
    for occurrence in occurrences(db) {
        match groups.last_mut() {
            Some((lsn, size, all)) if *lsn == occurrence.lsn => {
                *size += 1;
                *all &= occurrence.received;
            }
            _ => groups.push((occurrence.lsn, 1, occurrence.received)),
        }
    }
    groups
        .into_iter()
        .filter(|(_, _, all)| *all)
        .map(|(lsn, size, _)| (lsn, size))
        .collect()
}

/// A fabric identity from a fixed seed.
fn fixed_identity(directory: &Path, name: &str, byte: u8) -> Arc<FabricIdentity> {
    let path = directory.join(format!("{name}.identity"));
    std::fs::write(&path, [byte; 32]).unwrap();
    Arc::new(FabricIdentity::load_or_generate(&path).unwrap())
}

/// One store with a fixed identity and a pinned sync incarnation.
pub(crate) struct Peer {
    pub(crate) db: Arc<Database>,
    pub(crate) path: PathBuf,
    pub(crate) identity: Arc<FabricIdentity>,
}

impl Peer {
    pub(crate) fn new(directory: &Path, name: &str, seed: u8) -> Self {
        let path = directory.join(format!("{name}.redb"));
        let db = open(&path);
        db.pin_sync_incarnation_for_test(
            &tenant(),
            Incarnation(0x5eed_0000_0000_0000_0000_0000_0000_0000 | u128::from(seed)),
        )
        .unwrap();
        Self {
            db,
            path,
            identity: fixed_identity(directory, name, seed),
        }
    }

    pub(crate) fn node_id(&self) -> String {
        self.identity.node_id().to_string()
    }

    /// Reopen a store this setup created, keeping its identity and pinned
    /// incarnation.
    pub(crate) fn reopen(directory: &Path, name: &str) -> Self {
        let path = directory.join(format!("{name}.redb"));
        let identity_path = directory.join(format!("{name}.identity"));
        Self {
            db: open(&path),
            identity: Arc::new(FabricIdentity::load_or_generate(&identity_path).unwrap()),
            path,
        }
    }

    pub(crate) fn close(self) {
        let db = self.db;
        db.close().unwrap();
        drop(db);
    }
}

/// A hub serving one store over the in-process authenticated transport.
pub(crate) struct Hub {
    stop: Arc<AtomicBool>,
    task: Option<tokio::task::JoinHandle<()>>,
    server: Option<Arc<SyncServer>>,
}

impl Hub {
    pub(crate) fn serve(
        runtime: &tokio::runtime::Runtime,
        broker: &InProcessBroker,
        hub: &Peer,
    ) -> Self {
        let node = hub.identity.node_id();
        let server = Arc::new(
            SyncServer::with_authenticated_transport_and_identity_for_test(
                hub.db.clone(),
                broker.server_as(&node),
                tenant(),
                node,
                hub.identity.clone(),
            )
            .expect("construct the authenticated sync endpoint"),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let task = runtime.spawn({
            let server = server.clone();
            let stop = stop.clone();
            async move {
                server.run_until(stop).await;
            }
        });
        runtime.block_on(broker.wait_for_registered_route_for_test(
            &contextdb_server::subjects::push_subject(tenant().as_str()),
        ));
        Self {
            stop,
            task: Some(task),
            server: Some(server),
        }
    }

    pub(crate) fn stop(mut self, runtime: &tokio::runtime::Runtime) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(task) = self.task.take() {
            runtime.block_on(task).unwrap();
        }
        drop(self.server.take());
    }
}

pub(crate) fn client(broker: &InProcessBroker, edge: &Peer) -> SyncClient {
    SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.db.clone(),
        broker.client_as(&edge.identity.node_id()),
        tenant(),
        edge.identity.clone(),
    )
    .expect("construct the authenticated sync endpoint")
}

/// One measured outbound operation on `edge`: its result, the accountant peak
/// above the baseline (state held before an early refusal included, since it
/// is a high-water mark), what the outbound declaration path did, and the
/// edge's LSN when it started.
pub(crate) struct Measured<T> {
    pub(crate) value: T,
    pub(crate) peak: usize,
    pub(crate) pre_reply_peak: Option<usize>,
    pub(crate) work: OutboundDdlWork,
    pub(crate) lsn: Lsn,
}

pub(crate) fn measure<T>(edge: &Database, operation: impl FnOnce() -> T) -> Measured<T> {
    let lsn = edge.current_lsn();
    edge.accountant().reset_high_water_for_test();
    let baseline = edge.accountant().usage().used;
    let (value, work) = observe(operation);
    let peak = edge
        .accountant()
        .high_water_for_test()
        .checked_sub(baseline)
        .unwrap();
    let pre_reply_peak = work.pre_reply_high_water.map(|peak| {
        peak.checked_sub(baseline)
            .expect("pre-reply high-water includes the measured baseline")
    });
    Measured {
        value,
        peak,
        pre_reply_peak,
        work,
        lsn,
    }
}

/// Print the receipt line for one measured operation.
pub(crate) fn receipt<T>(row: &str, size: usize, measured: &Measured<T>) {
    let work = &measured.work;
    eprintln!(
        "receipt {row} size={size} lsn={} peak={} pre_reply_peak={:?} walked={} collected={} dropped={} raw_reads={} \
         arrival_lookups={} superseded_lookups={} digests_checked={} \
         validation_records_peak={} sent_ddl={} events={:?} fallback={:?}",
        measured.lsn.0,
        measured.peak,
        measured.pre_reply_peak,
        work.walked.len(),
        work.collected.len(),
        dropped(work).len(),
        work.raw_reads.len(),
        work.arrival_lookups.len(),
        work.superseded_lookups.len(),
        work.digests_checked.len(),
        work.validation_records_peak,
        work.sent_ddl,
        compact_events(&work.events),
        work.fallback_collector_hits,
    );
}

/// Events with each run of walked occurrences folded to its length.
fn compact_events(events: &[OutboundDdlEvent]) -> Vec<String> {
    let mut compact = Vec::<String>::new();
    let mut walked = 0usize;
    for event in events {
        if let OutboundDdlEvent::Walked(_) = event {
            walked += 1;
            continue;
        }
        if walked > 0 {
            compact.push(format!("Walked x{walked}"));
            walked = 0;
        }
        compact.push(format!("{event:?}"));
    }
    if walked > 0 {
        compact.push(format!("Walked x{walked}"));
    }
    compact
}

/// What was walked and never reached the final filter, in walk order.
pub(crate) fn dropped(work: &OutboundDdlWork) -> Vec<Occurrence> {
    let collected = work.collected.iter().collect::<BTreeSet<_>>();
    work.walked
        .iter()
        .filter(|occurrence| !collected.contains(occurrence))
        .copied()
        .collect()
}

fn key(occurrence: &DdlOccurrenceForTest) -> Occurrence {
    (occurrence.lsn, occurrence.ordinal)
}

/// What a correct outbound walk over `history` does from `since`, under the
/// re-upload `frontier` (None for an ordinary push): it decodes every
/// occurrence after `since` once, in order; it drops a whole source-LSN group
/// iff every member is marked received and the group is above the frontier;
/// everything else reaches the final filter unchanged. The validators then
/// hold one record per kept group, plus one per kept received occurrence and
/// one per kept superseded occurrence, and nothing for dropped history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Expected {
    pub(crate) walked: Vec<Occurrence>,
    pub(crate) collected: Vec<Occurrence>,
    pub(crate) dropped: Vec<Occurrence>,
    pub(crate) validation_records: u64,
    /// Every walked occurrence carrying a received or superseded mark.
    pub(crate) marked: Vec<Occurrence>,
}

pub(crate) fn expected_walk(
    history: &[DdlOccurrenceForTest],
    since: Lsn,
    frontier: Option<Lsn>,
) -> Expected {
    let suffix = history
        .iter()
        .filter(|occurrence| occurrence.lsn > since)
        .collect::<Vec<_>>();
    let dropped_groups = suffix
        .iter()
        .map(|occurrence| occurrence.lsn)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|lsn| {
            frontier.is_none_or(|frontier| *lsn > frontier)
                && suffix
                    .iter()
                    .filter(|occurrence| occurrence.lsn == *lsn)
                    .all(|occurrence| occurrence.received)
        })
        .collect::<BTreeSet<_>>();
    let (dropped, kept): (Vec<&&DdlOccurrenceForTest>, Vec<&&DdlOccurrenceForTest>) = suffix
        .iter()
        .partition(|occurrence| dropped_groups.contains(&occurrence.lsn));
    let kept_groups = kept
        .iter()
        .map(|occurrence| occurrence.lsn)
        .collect::<BTreeSet<_>>()
        .len();
    let kept_received = kept.iter().filter(|occurrence| occurrence.received).count();
    let kept_superseded = kept
        .iter()
        .filter(|occurrence| occurrence.superseded)
        .count();
    Expected {
        walked: suffix.iter().map(|occurrence| key(occurrence)).collect(),
        collected: kept.iter().map(|occurrence| key(occurrence)).collect(),
        dropped: dropped.iter().map(|occurrence| key(occurrence)).collect(),
        validation_records: (kept_groups + kept_received + kept_superseded) as u64,
        marked: suffix
            .iter()
            .filter(|occurrence| occurrence.received || occurrence.superseded)
            .map(|occurrence| key(occurrence))
            .collect(),
    }
}

fn multiplicity(occurrences: &[Occurrence]) -> HashMap<Occurrence, usize> {
    let mut counts = HashMap::new();
    for occurrence in occurrences {
        *counts.entry(*occurrence).or_insert(0) += 1;
    }
    counts
}

/// The exact facts of one completed extraction: walked, collected and
/// dropped sets, evidence of every walked occurrence looked up and checked,
/// a dropped occurrence's evidence looked up exactly once per kind, the
/// validation records equal to what kept output alone needs, and no
/// whole-history collector.
pub(crate) fn assert_walk(what: &str, work: &OutboundDdlWork, expected: &Expected) {
    assert!(
        !work.walked.is_empty(),
        "{what}: the observation saw no walked history; the extraction must run on the observing thread: {work:?}"
    );
    assert_eq!(
        work.walked, expected.walked,
        "{what}: the walk decodes every occurrence after the frontier exactly once, in order"
    );
    assert_eq!(
        work.collected, expected.collected,
        "{what}: exactly the kept groups reach the final filter; no droppable received group does"
    );
    assert_eq!(
        dropped(work),
        expected.dropped,
        "{what}: exactly the received groups above the frontier are dropped before the final filter"
    );
    assert_evidence(what, work, expected);
    assert_dropped_history_not_held(what, work, expected);
    assert_eq!(
        work.validation_records_peak, expected.validation_records,
        "{what}: the validators and any set the walk builds hold records for kept output only, none for dropped history"
    );
    assert_no_fallback(what, work);
}

/// The most the accountant's charge may rise across a run of dropped
/// occurrences while the walk keeps nothing of them: one cursor window's
/// worth of charges that come and go as the cursor crosses storage pages.
/// On the parent each dropped occurrence stays charged at about 640 B, so a
/// buffered group of [`PR_SIZE`] declarations exceeds this by more than
/// twice.
pub(crate) const DROPPED_RUN_SLACK: usize = 4_096;

/// A walk owns at most one dropped occurrence at a time. Across every run of
/// consecutive dropped occurrences in walk order, the accountant's charge
/// when the cursor begins each decode (and the first walked occurrence after
/// the run) stays within [`DROPPED_RUN_SLACK`] of the lowest charge seen in
/// the run. A walk that buffers a received group, or admits dropped history
/// into an output vector, before discarding it keeps one charged decode per
/// member and fails here at the PR size.
pub(crate) fn assert_dropped_history_not_held(
    what: &str,
    work: &OutboundDdlWork,
    expected: &Expected,
) {
    assert_eq!(
        work.walked_held.len(),
        work.walked.len(),
        "{what}: every walked occurrence records the accountant's charge as its decode began"
    );
    let dropped = expected.dropped.iter().collect::<BTreeSet<_>>();
    let mut run: Option<usize> = None;
    for (index, occurrence) in work.walked.iter().enumerate() {
        let in_run = dropped.contains(occurrence);
        if let Some(start) = run {
            let floor = *work.walked_held[start..index.max(start + 1)]
                .iter()
                .min()
                .unwrap();
            let grown = work.walked_held[index].saturating_sub(floor);
            assert!(
                grown <= DROPPED_RUN_SLACK,
                "{what}: walking dropped history from {:?} to {occurrence:?} left {grown} B more charged with the accountant (at most {DROPPED_RUN_SLACK} B): the walk holds dropped occurrences; charges by walked occurrence {:?}",
                work.walked[start],
                work.walked_held[start..=index]
                    .iter()
                    .map(|held| held.saturating_sub(floor))
                    .collect::<Vec<_>>()
            );
        }
        run = match (in_run, run) {
            (true, None) => Some(index),
            (true, Some(start)) => Some(start),
            (false, _) => None,
        };
    }
}

fn assert_evidence(what: &str, work: &OutboundDdlWork, expected: &Expected) {
    let walked = work.walked.iter().copied().collect::<BTreeSet<_>>();
    for (kind, lookups) in [
        ("arrival", &work.arrival_lookups),
        ("superseded", &work.superseded_lookups),
    ] {
        let looked_up = multiplicity(lookups);
        let missing = walked
            .iter()
            .filter(|occurrence| !looked_up.contains_key(occurrence))
            .collect::<Vec<_>>();
        assert!(
            missing.is_empty(),
            "{what}: every walked occurrence has its {kind} evidence looked up; missing {missing:?}"
        );
        let repeated = dropped(work)
            .into_iter()
            .filter(|occurrence| looked_up[occurrence] != 1)
            .collect::<Vec<_>>();
        assert!(
            repeated.is_empty(),
            "{what}: a dropped occurrence's {kind} evidence is looked up exactly once; repeated {repeated:?}"
        );
    }
    let checked = work.digests_checked.iter().collect::<BTreeSet<_>>();
    let unchecked = expected
        .marked
        .iter()
        .filter(|occurrence| !checked.contains(occurrence))
        .collect::<Vec<_>>();
    assert!(
        unchecked.is_empty(),
        "{what}: every walked occurrence carrying a mark has its digest checked against the stored declaration; unchecked {unchecked:?}"
    );
}

/// The facts of an extraction that refused at `reaches`: the walk read a
/// prefix of the expected walk that reaches it (nothing after a refusal is
/// demanded), only kept groups reached the final filter, and the validators
/// held no more than kept output needs.
pub(crate) fn assert_refused_walk(
    what: &str,
    work: &OutboundDdlWork,
    expected: &Expected,
    reaches: Occurrence,
) {
    assert!(
        !work.walked.is_empty(),
        "{what}: the observation saw no walked history: {work:?}"
    );
    assert!(
        expected.walked.starts_with(&work.walked) && work.walked.contains(&reaches),
        "{what}: the walk reads, in order, a prefix of the history after the frontier that reaches the planted fault {reaches:?}: walked {:?}",
        work.walked
    );
    let kept = expected.collected.iter().collect::<BTreeSet<_>>();
    let extra = work
        .collected
        .iter()
        .filter(|occurrence| !kept.contains(occurrence))
        .collect::<Vec<_>>();
    assert!(
        extra.is_empty(),
        "{what}: no droppable received group reaches the final filter before the refusal; reached {extra:?}"
    );
    let reached = Expected {
        walked: work.walked.clone(),
        dropped: expected
            .dropped
            .iter()
            .filter(|occurrence| work.walked.contains(occurrence))
            .copied()
            .collect(),
        ..expected.clone()
    };
    assert_dropped_history_not_held(what, work, &reached);
    assert!(
        work.validation_records_peak <= expected.validation_records,
        "{what}: before the refusal the validators held {} records; kept output needs at most {}",
        work.validation_records_peak,
        expected.validation_records
    );
    assert_no_fallback(what, work);
}

/// The outbound fixture path never reaches a whole-history collector.
pub(crate) fn assert_no_fallback(what: &str, work: &OutboundDdlWork) {
    assert_eq!(
        work.fallback_collector_hits, [0; 4],
        "{what}: the outbound path reached a whole-history collector: {work:?}"
    );
}

/// One epoch value per operation, read before the first history window, and
/// that same value handed to every frontier consumer. `completed` operations
/// must also have reached the final filter and the arrived-by-sync row filter,
/// and an armed epoch the delete synthesis.
pub(crate) fn assert_one_epoch(
    what: &str,
    work: &OutboundDdlWork,
    frontier: Option<Lsn>,
    completed: bool,
) {
    let reads = work
        .events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match event {
            OutboundDdlEvent::EpochRead(read) => Some((index, *read)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        reads.iter().map(|(_, read)| *read).collect::<Vec<_>>(),
        vec![frontier],
        "{what}: exactly one epoch value is read per operation: {:?}",
        compact_events(&work.events)
    );
    if let Some(first_walked) = work
        .events
        .iter()
        .position(|event| matches!(event, OutboundDdlEvent::Walked(_)))
    {
        assert!(
            reads[0].0 < first_walked,
            "{what}: the epoch is read before the first history window, so the walk can use it: {:?}",
            compact_events(&work.events)
        );
    }
    let mut filters = 0;
    let mut arrivals = 0;
    let mut deletes = 0;
    for event in &work.events {
        match event {
            OutboundDdlEvent::FilterFrontier(seen) | OutboundDdlEvent::ArrivalFrontier(seen) => {
                if matches!(event, OutboundDdlEvent::FilterFrontier(_)) {
                    filters += 1;
                } else {
                    arrivals += 1;
                }
                assert_eq!(
                    *seen, frontier,
                    "{what}: {event:?} uses the operation's one epoch value"
                );
            }
            OutboundDdlEvent::DeleteFrontier(seen) => {
                deletes += 1;
                assert_eq!(
                    Some(*seen),
                    frontier,
                    "{what}: {event:?} uses the operation's one epoch value"
                );
            }
            OutboundDdlEvent::EpochRead(_) | OutboundDdlEvent::Walked(_) => {}
        }
    }
    if completed {
        assert!(filters >= 1, "{what}: the final filter ran");
        assert!(
            arrivals >= 1,
            "{what}: the arrived-by-sync row filter ran: {:?}",
            compact_events(&work.events)
        );
        assert_eq!(
            deletes,
            usize::from(frontier.is_some()),
            "{what}: re-upload delete synthesis runs once, only under an armed epoch"
        );
    }
}

/// The declarations a push placed into its requests, with their provenance.
pub(crate) fn assert_sent(what: &str, work: &OutboundDdlWork, expected: &[SentDdl]) {
    assert_eq!(
        work.sent, expected,
        "{what}: the push request carries exactly these declarations and provenance"
    );
    assert_eq!(
        work.sent_ddl,
        expected.len() as u64,
        "{what}: one declaration per provenance record"
    );
}

/// What a push sends for local occurrences: each names the edge as author at
/// its own source LSN and ordinal.
pub(crate) fn sent_as_local(edge_node: &str, sent: &[DdlOccurrenceForTest]) -> Vec<SentDdl> {
    sent.iter()
        .map(|occurrence| SentDdl {
            source: key(occurrence),
            author: Some((edge_node.to_string(), occurrence.lsn, occurrence.ordinal)),
        })
        .collect()
}

/// One nightly sample: (received declarations, edge LSN at the operation,
/// accountant peak).
pub(crate) type Sample = (usize, Lsn, usize);

/// Where a control sample's [`store_ballast`] sits relative to the measured
/// operation's walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BallastIn {
    /// Behind the push frontier: no walk passes it, so the control measures
    /// store size alone.
    NoWalk,
    /// Inside the walk (a push from frontier 0 or a destination re-upload,
    /// which walk the whole store): the control also carries what the
    /// operation holds for each never-synced row it walks past, which is
    /// CDB-243.
    TheWalk,
}

/// Nightly: the accountant peak may grow from the small to the large received
/// history by no more than the store-size term, which the control pair
/// measures, plus [`CURSOR_WINDOW_SLACK`].
///
/// `measured` is the row at [`NIGHTLY_SIZES`]; `control` is the same row run
/// through [`control_sample`] at each size, walking the small history over a
/// store grown to stand in for the large one. Within each pair the edge must
/// work at the same LSN, or the envelope width differs. The control delta
/// must stay within [`STORE_SIZE_TERM_BOUND`], or the row fails as "store-size
/// term out of bound"; where the ballast sits inside the operation's walk
/// ([`BallastIn::TheWalk`]) that failure names CDB-243. Then
/// `peak(1024) - peak(16) <= control(1024) - control(16) + CURSOR_WINDOW_SLACK`.
pub(crate) fn assert_bounded_by_control(
    what: &str,
    measured: &[Sample; 2],
    control: &[Sample; 2],
    ballast: BallastIn,
) {
    let measured_delta = measured[1].2 as i64 - measured[0].2 as i64;
    let control_delta = control[1].2 as i64 - control[0].2 as i64;
    eprintln!(
        "nightly {what}: measured peaks {} / {} (delta {measured_delta}) at LSN {} / {}; control peaks {} / {} (delta {control_delta}) at LSN {} / {}; bound {}",
        measured[0].2,
        measured[1].2,
        measured[0].1.0,
        measured[1].1.0,
        control[0].2,
        control[1].2,
        control[0].1.0,
        control[1].1.0,
        control_delta + CURSOR_WINDOW_SLACK as i64,
    );
    assert_eq!(
        measured[0].1, measured[1].1,
        "{what}: fixture defect: the edge must work from the same LSN at both sizes, or the envelope width differs: {measured:?}"
    );
    assert_eq!(
        control[0].1, control[1].1,
        "{what}: fixture defect: the control pair must work from the same LSN at both sizes: {control:?}"
    );
    let cause = match ballast {
        BallastIn::NoWalk => "",
        BallastIn::TheWalk => {
            "; this operation walks the whole store, so the control's never-synced ballast rows sit inside its walk, and holding them is CDB-243 (an outbound push or re-upload holds every local-only row it walks past and sends none)"
        }
    };
    assert!(
        control_delta <= STORE_SIZE_TERM_BOUND as i64,
        "{what}: store-size term out of bound: the control pair's peaks differ by {control_delta} B, more than {STORE_SIZE_TERM_BOUND} B{cause}: {control:?}"
    );
    assert!(
        measured_delta <= control_delta + CURSOR_WINDOW_SLACK as i64,
        "{what}: the accountant peak grows with received declaration history: {measured_delta} B from {} to {} declarations, against a store-size term of {control_delta} B plus {CURSOR_WINDOW_SLACK} B of cursor-window slack: measured {measured:?}, control {control:?}",
        measured[0].0,
        measured[1].0,
    );
}

/// A successful push must hit the real pre-reply boundary. An ordinary
/// pre-send refusal has no reply commit, so all its memory belongs to the walk.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PushSample {
    pub(crate) pre_reply: Sample,
    pub(crate) whole: Sample,
}

pub(crate) fn push_sample<T>(
    size: usize,
    measured: &Measured<T>,
    commits_reply: bool,
) -> PushSample {
    let pre_reply = if commits_reply {
        measured.pre_reply_peak.expect(
            "successful push must sample its source accountant before the real reply commit",
        )
    } else {
        assert!(
            measured.pre_reply_peak.is_none(),
            "pre-send refusal cannot commit reply effects"
        );
        measured.peak
    };
    PushSample {
        pre_reply: (size, measured.lsn, pre_reply),
        whole: (size, measured.lsn, measured.peak),
    }
}

/// Bound the walk by its control, one window and one CDB-249 storage page.
/// The whole push separately admits B-tree curvature and the same single
/// maximum config branch invoice until CDB-249 is fixed.
pub(crate) fn assert_walk_from_zero_push(
    what: &str,
    measured: &[PushSample; 3],
    control: &[PushSample; 3],
) {
    const PAGE_INVOICE: i128 = 4_224;
    // CDB-249: one maximum config branch backing (65,536) plus its native
    // 128-byte page control, copied by lineage or reply storage writes.
    const CDB_249_STORAGE_PAGE_ALLOWANCE: i128 = 65_664;
    let mut pre_reply_ok = true;
    let mut control_ok = true;
    // Preserve the original endpoint control cap and one-window slack as well
    // as checking each new adjacent pair; three sizes cannot double either.
    for (small_index, large_index) in [(0, 1), (1, 2), (0, 2)] {
        let small = measured[small_index].pre_reply;
        let large = measured[large_index].pre_reply;
        let small_control = control[small_index].pre_reply;
        let large_control = control[large_index].pre_reply;
        assert_eq!(small.1, large.1, "{what}: measured source LSN stays fixed");
        assert_eq!(
            small_control.1, large_control.1,
            "{what}: control source LSN stays fixed"
        );
        let delta = large.2 as i128 - small.2 as i128;
        let control_delta = large_control.2 as i128 - small_control.2 as i128;
        let bound = control_delta + CURSOR_WINDOW_SLACK as i128 + CDB_249_STORAGE_PAGE_ALLOWANCE;
        let passes = delta <= bound;
        control_ok &= control_delta <= STORE_SIZE_TERM_BOUND as i128;
        pre_reply_ok &= passes;
        eprintln!(
            "PRE_REPLY_WALK_BOUND {what} sizes={}/{} peaks={}/{} delta={delta} control_delta={control_delta} cdb249_allowance={CDB_249_STORAGE_PAGE_ALLOWANCE} bound={bound} passes={passes}",
            small.0, large.0, small.2, large.2
        );
    }
    let peaks = measured.map(|sample| sample.whole.2);
    let first_delta = peaks[1] as i128 - peaks[0] as i128;
    let next_delta = peaks[2] as i128 - peaks[1] as i128;
    let bound = first_delta + 2 * PAGE_INVOICE + CDB_249_STORAGE_PAGE_ALLOWANCE;
    let whole_ok = next_delta <= bound;
    eprintln!(
        "WHOLE_PUSH_CURVATURE {what} peaks={peaks:?} first_delta={first_delta} next_delta={next_delta} page_slack={} cdb249_allowance={CDB_249_STORAGE_PAGE_ALLOWANCE} bound={bound} passes={whole_ok}",
        2 * PAGE_INVOICE
    );
    assert!(
        control_ok,
        "{what}: pre-reply control exceeds the existing store-size bound; a walk over never-synced ballast can expose CDB-243: {control:?}"
    );
    assert!(
        pre_reply_ok && whole_ok,
        "{what}: pre-reply walk control plus CDB-249 bound passes={pre_reply_ok}; whole-push curvature plus CDB-249 allowance passes={whole_ok}; measured={measured:?}; control={control:?}"
    );
}

/// Where a re-upload push walks from. The new hub has applied nothing, so the
/// push's status exchange regresses its frontier to 0 and it re-uploads from
/// the start (`sync_client.rs`, the pre-push status exchange). A pending read
/// does no status exchange and walks from the stored push watermark.
pub(crate) const REUPLOAD_PUSH_SINCE: Lsn = Lsn(0);

/// Copy every file of a closed setup into a fresh directory.
pub(crate) fn copy_setup(from: &Path, to: &Path) {
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            std::fs::create_dir_all(&target).unwrap();
            copy_setup(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// An edge and its hub, connected and serving.
pub(crate) struct Network {
    pub(crate) edge: Peer,
    pub(crate) hub: Peer,
    pub(crate) broker: InProcessBroker,
    pub(crate) server: Option<Hub>,
    pub(crate) client: SyncClient,
}

impl Network {
    pub(crate) fn connect(runtime: &tokio::runtime::Runtime, edge: Peer, hub: Peer) -> Self {
        let broker = InProcessBroker::new();
        let server = Hub::serve(runtime, &broker, &hub);
        let client = client(&broker, &edge);
        Self {
            edge,
            hub,
            broker,
            server: Some(server),
            client,
        }
    }

    pub(crate) fn close(mut self, runtime: &tokio::runtime::Runtime) {
        drop(self.client);
        if let Some(server) = self.server.take() {
            server.stop(runtime);
        }
        self.edge.close();
        self.hub.close();
    }
}

/// The hub's one multi-statement schema commit, so the edge receives at least
/// one received group of several occurrences.
pub(crate) fn hub_unit() -> Vec<String> {
    large_commit("hub_unit", 3)
}

/// An edge that authored `notes` and pushed it, while its hub authored `size`
/// unrelated declarations and one multi-statement unit. The edge then pulled:
/// its own `notes` echoed back plus everything the hub authored, all marked
/// received above its push frontier. `before_pull` runs on the edge between
/// the push and the pull. `pad` local commits precede row 1, so the first
/// push's frontier lands above them and no later push walks them; a control
/// sample's [`store_ballast`] goes into the same table right after them.
pub(crate) fn pulled_edge(
    runtime: &tokio::runtime::Runtime,
    directory: &Path,
    size: usize,
    pad: Option<usize>,
    before_pull: &dyn Fn(&Database),
) -> Network {
    let edge = Peer::new(directory, "edge", 0x11);
    let hub = Peer::new(directory, "hub", 0x22);
    sql(
        &edge.db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
    );
    if let Some(pad) = pad {
        pad_commits(&edge.db, pad);
    }
    store_ballast(&edge.db, "lsn_pad");
    sql(&edge.db, "INSERT INTO notes VALUES (1, 'first')");
    let network = Network::connect(runtime, edge, hub);
    runtime.block_on(network.client.push()).unwrap();
    churn(&network.hub.db, "unrelated", size);
    one_commit(&network.hub.db, &hub_unit());
    before_pull(&network.edge.db);
    runtime.block_on(network.client.pull_default()).unwrap();
    network
}

/// The last received group of more than one occurrence.
pub(crate) fn last_multi_occurrence_received_group(db: &Database) -> (Lsn, u32) {
    *received_groups(db)
        .iter()
        .rev()
        .find(|(_, size)| *size > 1)
        .expect("the hub's multi-statement unit arrives as one received group")
}

/// Where the fixed two-declaration `prefix_a` schema an edge holds at its move
/// comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Prefix {
    /// The first hub authors it and the edge pulls it, so it is received.
    ReceivedFromTheFirstHub,
    /// The edge authors it, so it is local.
    AuthoredByTheEdge,
}

/// An edge that synced `notes` with a first hub, holds the fixed
/// two-declaration `prefix_a` schema, then moved to a second hub. The second
/// hub's `size` declarations arrive in [`GROUPED_CHURN_COMMITS`] commits, so
/// the edge's LSN at the re-upload is the same at every size: a re-upload
/// walks from 0, so padding before the move would sit inside its walk. The
/// move's frontier F is the edge's LSN at the move, so the prefix sits at or
/// below F. The edge then pulls the second hub's `size` declarations, which
/// arrive above F; with `received_row`, the second hub also declares
/// `hub_rows` and writes row 20 before that pull, so a data row arrives by
/// sync above F (its declaration is one more received above F). A control
/// sample's [`store_ballast`] goes into the received `moved_0_table` after the
/// pull, inside the re-upload's walk from 0. `after_move` runs on the edge
/// after that.
///
/// Two boundaries stay out of this fixture, each on an open defect: a row
/// received at or below F needs a pull before the move, and that edge's
/// re-upload then refuses on CDB-235 (the received `notes` schema has no
/// authoritative generation); a row deleted at or below F is resurrected on
/// the new hub by CDB-237.
pub(crate) struct MovedEdge {
    pub(crate) network: Network,
    pub(crate) frontier: Lsn,
    pub(crate) first_hub_node: String,
}

pub(crate) fn moved_edge(
    runtime: &tokio::runtime::Runtime,
    directory: &Path,
    size: usize,
    prefix: Prefix,
    received_row: bool,
    after_move: &dyn Fn(&Database),
) -> MovedEdge {
    let edge = Peer::new(directory, "edge", 0x11);
    let first = Peer::new(directory, "first_hub", 0x22);
    let second = Peer::new(directory, "second_hub", 0x33);
    let first_hub_node = first.node_id();
    sql(
        &edge.db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
    );
    sql(&edge.db, "INSERT INTO notes VALUES (1, 'first')");
    let network = Network::connect(runtime, edge, first);
    runtime.block_on(network.client.push()).unwrap();
    let author = match prefix {
        Prefix::ReceivedFromTheFirstHub => &network.hub.db,
        Prefix::AuthoredByTheEdge => &network.edge.db,
    };
    sql(
        author,
        "CREATE TABLE prefix_a (id INTEGER PRIMARY KEY) SYNC TWO WAY",
    );
    sql(author, "ALTER TABLE prefix_a ADD COLUMN extra INTEGER");
    if prefix == Prefix::ReceivedFromTheFirstHub {
        runtime.block_on(network.client.pull_default()).unwrap();
    }
    let frontier = network.edge.db.current_lsn();
    let Network {
        edge,
        hub: first,
        broker: _broker,
        server,
        client,
    } = network;
    drop(client);
    if let Some(server) = server {
        server.stop(runtime);
    }
    first.close();
    let network = Network::connect(runtime, edge, second);
    network
        .client
        .change_destination(&network.hub.node_id())
        .unwrap();
    if received_row {
        sql(
            &network.hub.db,
            "CREATE TABLE hub_rows (id INTEGER PRIMARY KEY) SYNC TWO WAY",
        );
        sql(&network.hub.db, "INSERT INTO hub_rows VALUES (20)");
    }
    grouped_churn(&network.hub.db, "moved", size);
    runtime.block_on(network.client.pull_default()).unwrap();
    store_ballast(&network.edge.db, "moved_0_table");
    after_move(&network.edge.db);
    MovedEdge {
        network,
        frontier,
        first_hub_node,
    }
}

/// Whether a declaration names a table whose name starts with `prefix`.
pub(crate) fn names(ddl: &contextdb_engine::sync_types::DdlChange, prefix: &str) -> bool {
    format!("{ddl:?}").contains(&format!("\"{prefix}"))
}
