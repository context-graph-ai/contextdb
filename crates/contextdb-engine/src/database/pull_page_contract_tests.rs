//! A hub pull page keeps its promises as history grows and is pruned:
//! it admits what it selects before copying it, releases everything on every
//! exit, resumes exactly after the last group it consumed, and loses or
//! repeats no occurrence at or below that point, whether the hub is resident
//! or durable and whatever maintenance removes between pages.
use super::resident_history_test_seams::{observe, Work};
use super::*;
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

const PAGE_BYTES: usize = 64 * 1024;

/// A real read budget behind the trait that records every reservation, so a
/// test can read the exact bytes a page admitted, its peak, and whether it
/// handed everything back.
#[derive(Debug)]
struct CountingBudget {
    inner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    reserved: AtomicUsize,
    released: AtomicUsize,
    held_high: AtomicUsize,
    watch_native: bool,
    native_excess: AtomicUsize,
    refuse_selected_copy: bool,
}
impl CountingBudget {
    fn new(inner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            reserved: AtomicUsize::new(0),
            released: AtomicUsize::new(0),
            held_high: AtomicUsize::new(0),
            watch_native: false,
            native_excess: AtomicUsize::new(0),
            refuse_selected_copy: false,
        })
    }
    fn held(&self) -> usize {
        self.reserved
            .load(AtomicOrdering::SeqCst)
            .saturating_sub(self.released.load(AtomicOrdering::SeqCst))
    }
    fn observe_native(&self) {
        if self.watch_native {
            let native = crate::persistence::content_identity::proof_allocation_oracle_tests::measured_live_bytes_for_test();
            self.native_excess
                .fetch_max(native.saturating_sub(self.held()), AtomicOrdering::SeqCst);
        }
    }
}
impl contextdb_core::read_memory::ReadMemoryBudget for CountingBudget {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        self.observe_native();
        if self.refuse_selected_copy && operation == "select_sync_changes" {
            return Err(Error::MemoryBudgetExceeded {
                subsystem: subsystem.into(),
                operation: operation.into(),
                requested_bytes: bytes,
                available_bytes: 0,
                budget_limit_bytes: 0,
                hint: hint.into(),
            });
        }
        self.inner.try_reserve(bytes, subsystem, operation, hint)?;
        self.reserved.fetch_add(bytes, AtomicOrdering::SeqCst);
        self.held_high
            .fetch_max(self.held(), AtomicOrdering::SeqCst);
        Ok(())
    }
    fn release(&self, bytes: usize) {
        self.released.fetch_add(bytes, AtomicOrdering::SeqCst);
        self.inner.release(bytes);
        self.observe_native();
    }
}

/// One occurrence a page carried: (lsn, table, natural key, deleted).
type Occurrence = (u64, String, String, bool);

struct Pulled {
    occurrences: Vec<Occurrence>,
    bodies: Vec<String>,
    purges: serde_json::Value,
    ddl: usize,
    through: Option<Lsn>,
    work: Work,
    admitted: usize,
    peak: usize,
    held_after: usize,
}

/// One page from `since`, selected on this thread so its work is observable,
/// funded by `budget`, exactly as a hub serves it. The page's output is dropped
/// before the budget is read back.
fn page_from(
    db: &Database,
    since: Lsn,
    page_bytes: usize,
    budget: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> (Result<Pulled>, Arc<CountingBudget>) {
    let counting = CountingBudget::new(budget);
    let memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget> = counting.clone();
    let outcome = (|| {
        let mut vector_copy = crate::vector_read_memory::admission(memory.clone());
        let mut credit = contextdb_core::read_memory::ReadCredit::new(memory.clone());
        let through = Cell::new(None);
        let purge_items = std::cell::RefCell::new(Vec::new());
        let (selected, work) = observe(|| {
            db.checked_changes_since_with_arrivals(
                since,
                &mut vector_copy,
                memory.clone(),
                &mut credit,
                Some(PullPageRequest {
                    bytes: page_bytes,
                    through: &through,
                    purge_items: &purge_items,
                }),
            )
        });
        let (changes, _, _) = selected?;
        let occurrences = changes
            .rows
            .iter()
            .map(|row| {
                (
                    row.lsn.0,
                    row.table.clone(),
                    format!("{:?}", row.natural_key),
                    row.deleted,
                )
            })
            .collect();
        let bodies = changes
            .rows
            .iter()
            .map(|row| format!("{:?}", row.values.get("body")))
            .collect();
        Ok(Pulled {
            occurrences,
            bodies,
            // Test-owned observations, like the body strings above, keep the
            // full selected purge fields after the engine's page is released.
            purges: serde_json::to_value(purge_items.borrow().as_slice()).unwrap(),
            ddl: changes.ddl.len(),
            through: through.get(),
            work,
            admitted: 0,
            peak: 0,
            held_after: 0,
        })
    })();
    let outcome = outcome.map(|mut pulled: Pulled| {
        pulled.admitted = counting.reserved.load(AtomicOrdering::SeqCst);
        pulled.peak = counting.held_high.load(AtomicOrdering::SeqCst);
        pulled
    });
    (outcome, counting)
}

fn page(db: &Database, since: Lsn, page_bytes: usize) -> Pulled {
    let memory = db.engine_sync_read_memory().expect("hub read memory");
    let (outcome, counting) = page_from(db, since, page_bytes, memory);
    let mut pulled = outcome.expect("a page");
    pulled.held_after = counting.held();
    pulled
}

/// Every page from the start as an edge pulls it, with `between` run after each
/// page that has more behind it.
fn pull_all_with(
    db: &Database,
    page_bytes: usize,
    mut between: impl FnMut(usize, &Pulled),
) -> Vec<Pulled> {
    let mut since = Lsn(0);
    let mut pages = Vec::new();
    loop {
        let next = page(db, since, page_bytes);
        let through = next.through;
        let index = pages.len();
        between(index, &next);
        pages.push(next);
        match through {
            Some(through) => {
                assert!(through > since, "every page moves the cursor forward");
                since = through;
            }
            None => break,
        }
        assert!(pages.len() < 100_000, "the pull makes progress");
    }
    pages
}

fn caller_driven(db: Database) -> Database {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db
}

fn synced_notes(db: &Database) {
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare notes");
}

/// Only the latest value of each row is kept, so superseded versions and the
/// log entries that name them are reclaimable.
fn current_notes(db: &Database) {
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) HISTORY CURRENT ONLY SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &HashMap::new(),
    )
    .expect("declare current-only notes");
}

fn write_note(db: &Database, id: i64, body: &str) {
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Int64(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("insert a note");
}

fn rewrite_note(db: &Database, id: i64, body: &str) {
    db.execute(
        "UPDATE notes SET body = $body WHERE id = $id",
        &HashMap::from([
            ("id".to_string(), Value::Int64(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .expect("rewrite a note");
}

fn fat(seed: usize) -> String {
    format!("{seed:08} {}", "x".repeat(4096))
}

/// `count` single-row commits of about 4 KiB each into `notes` (UUID keys).
fn commit_uuid_rows(db: &Database, from: usize, count: usize) {
    for i in from..from + count {
        db.execute(
            "INSERT INTO notes (id, body) VALUES ($id, $body)",
            &HashMap::from([
                (
                    "id".to_string(),
                    Value::Uuid(uuid::Uuid::from_u128(i as u128 + 1)),
                ),
                ("body".to_string(), Value::Text(fat(i))),
            ]),
        )
        .expect("commit a row");
    }
}

/// The latest value each key resolves to when `pulls` are applied in order.
fn resolved(pages: &[Pulled]) -> BTreeMap<String, u64> {
    let mut latest = BTreeMap::new();
    for pulled in pages {
        for (lsn, table, key, _) in &pulled.occurrences {
            let slot = latest.entry(format!("{table}/{key}")).or_insert(0);
            *slot = (*slot).max(*lsn);
        }
    }
    latest
}

fn every_occurrence(pages: &[Pulled]) -> Vec<Occurrence> {
    pages
        .iter()
        .flat_map(|pulled| pulled.occurrences.iter().cloned())
        .collect()
}

// A resident page admits what it selects and no more: the bytes it admits and
// the peak it holds are the same behind a doubled tail, exactly.
#[test]
#[serial_test::serial]
fn a_resident_page_admits_the_same_bytes_as_the_tail_doubles() {
    let mut admitted = Vec::new();
    let mut peaks = Vec::new();
    for count in [400usize, 800] {
        let db = caller_driven(Database::open_memory());
        synced_notes(&db);
        commit_uuid_rows(&db, 0, count);
        let first = page(&db, Lsn(0), PAGE_BYTES);
        assert!(first.through.is_some(), "the tail does not fit one page");
        admitted.push(first.admitted);
        peaks.push(first.peak);
    }
    assert_eq!(
        admitted[0], admitted[1],
        "a page admits the same bytes behind a doubled tail"
    );
    assert_eq!(
        peaks[0], peaks[1],
        "a page holds the same peak behind a doubled tail"
    );
    // The allocator independently observes every admission boundary and the
    // live callback. Doubling only the selected body cannot grow unadmitted
    // native storage; fixed operation controls are the fresh small control.
    let mut excess = Vec::new();
    for body_bytes in [64 * 1024, 128 * 1024] {
        let db = caller_driven(Database::open_memory());
        synced_notes(&db);
        db.execute(
            "INSERT INTO notes (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(uuid::Uuid::from_u128(1))),
                ("body".to_string(), Value::Text("x".repeat(body_bytes))),
            ]),
        )
        .expect("one wide selected row");
        let since = Lsn(1);
        let _ = page(&db, since, 1);
        let mut counting = CountingBudget::new(db.engine_sync_read_memory().expect("page owner"));
        Arc::get_mut(&mut counting).unwrap().watch_native = true;
        let memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget> = counting.clone();
        let (outcome, _, _) =
            crate::persistence::content_identity::proof_allocation_oracle_tests::measured(|| {
                db.with_checked_changes_page_for_test(since, 1, memory, |changes, _| {
                    assert_eq!(changes.rows.len(), 1);
                    assert_eq!(
                        changes.rows[0]
                            .values
                            .get("body")
                            .and_then(|value| match value {
                                Value::Text(body) => Some(body.len()),
                                _ => None,
                            }),
                        Some(body_bytes)
                    );
                    counting.observe_native();
                })
            });
        outcome.expect("funded selected page");
        assert_eq!(counting.held(), 0);
        excess.push(counting.native_excess.load(AtomicOrdering::SeqCst));
    }
    assert!(
        excess[1] <= excess[0],
        "doubling the selected body grows no unadmitted native allocation: {excess:?}"
    );
}

// A page that is refused for want of memory hands back everything it admitted
// and moves nothing: the budget reads zero held afterwards, and a later page
// from the same cursor under a sufficient budget succeeds in full.
#[test]
#[serial_test::serial]
fn a_refused_resident_page_releases_everything_it_admitted() {
    let db = caller_driven(Database::open_memory());
    synced_notes(&db);
    commit_uuid_rows(&db, 0, 200);
    let tiny = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(1024)
        .child()
        .expect("a tiny reader");
    let (outcome, counting) = page_from(&db, Lsn(0), PAGE_BYTES, tiny);
    assert!(
        matches!(outcome, Err(Error::MemoryBudgetExceeded { .. })),
        "a 1 KiB reader cannot hold a page"
    );
    assert_eq!(counting.held(), 0, "a refused page holds nothing");
    let retry = page(&db, Lsn(0), PAGE_BYTES);
    assert!(!retry.occurrences.is_empty(), "the retry delivers a page");

    // Refuse the pending record's transfer after its source has decoded it.
    // Every field, including a wide key not transferred to an answer, must
    // die before that source's admission is refunded.
    let mut excess = Vec::new();
    for key_bytes in [64 * 1024, 128 * 1024] {
        let db = caller_driven(Database::open_memory());
        db.execute(
            "CREATE TABLE wide_notes (id TEXT PRIMARY KEY, body TEXT) HISTORY ALL SYNC TWO WAY",
            &HashMap::new(),
        )
        .expect("declare the wide pending key");
        let since = db.current_lsn();
        let key = Value::Text("k".repeat(key_bytes));
        db.execute(
            "INSERT INTO wide_notes (id, body) VALUES ($id, 'pending')",
            &HashMap::from([("id".to_string(), key.clone())]),
        )
        .expect("insert the wide pending key");
        db.execute(
            "DELETE FROM wide_notes WHERE id = $id",
            &HashMap::from([("id".to_string(), key)]),
        )
        .expect("delete the wide pending key");
        db.__force_change_log_gap_for_test();
        let _ = page(&db, since, 1);
        let mut counting = CountingBudget::new(db.engine_sync_read_memory().expect("page owner"));
        let options = Arc::get_mut(&mut counting).unwrap();
        options.watch_native = true;
        options.refuse_selected_copy = true;
        let memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget> = counting.clone();
        let (outcome, _, _) =
            crate::persistence::content_identity::proof_allocation_oracle_tests::measured(|| {
                db.with_checked_changes_page_for_test(since, 1, memory, |_, _| {
                    panic!("the selected pending copy must be refused")
                })
            });
        assert!(matches!(
            outcome,
            Err(Error::MemoryBudgetExceeded { operation, .. }) if operation == "select_sync_changes"
        ));
        assert_eq!(counting.held(), 0);
        assert_eq!(db.active_pull_history_registrations_for_test(), 0);
        excess.push(counting.native_excess.load(AtomicOrdering::SeqCst));
    }
    assert!(
        excess[1] <= excess[0],
        "pending refusal frees its decoded record before refunding it: {excess:?}"
    );

    // The durable purge window has the same transfer boundary. Use a real
    // standalone PURGE so its published journal record is the only source
    // occurrence after the cursor, then refuse that record's output copy.
    let mut purge_excess = Vec::new();
    for key_bytes in [64 * 1024, 128 * 1024] {
        let dir = tempfile::tempdir().expect("purge source directory");
        let db =
            caller_driven(Database::open(dir.path().join("purge.db")).expect("open purge store"));
        db.execute(
            "CREATE TABLE purge_keys (id TEXT PRIMARY KEY, body TEXT) HISTORY ALL SYNC TWO WAY",
            &HashMap::new(),
        )
        .expect("declare purge source");
        let params = HashMap::from([("id".to_string(), Value::Text("p".repeat(key_bytes)))]);
        db.execute(
            "INSERT INTO purge_keys (id, body) VALUES ($id, 'erased')",
            &params,
        )
        .expect("insert the selected purge life");
        let since = db.current_lsn();
        db.execute("PURGE FROM purge_keys WHERE id = $id", &params)
            .expect("publish the task-scoped standalone purge");
        let journal = db
            .authoritative_purge_delivery_items_since(since)
            .expect("read the actual purge journal control");
        assert_eq!(journal.len(), 1, "one purge obligation is the source");
        assert_eq!(journal[0].natural_key.value, params["id"]);
        let control = page(&db, since, 1);
        assert_eq!(control.purges.as_array().unwrap().len(), 1);
        let mut counting =
            CountingBudget::new(db.engine_sync_read_memory().expect("purge page owner"));
        let options = Arc::get_mut(&mut counting).unwrap();
        options.watch_native = true;
        options.refuse_selected_copy = true;
        let memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget> = counting.clone();
        let (outcome, _, _) =
            crate::persistence::content_identity::proof_allocation_oracle_tests::measured(|| {
                db.with_checked_changes_page_for_test(since, 1, memory, |_, _| {
                    panic!("the selected purge copy must be refused")
                })
            });
        assert!(matches!(
            outcome,
            Err(Error::MemoryBudgetExceeded { operation, .. }) if operation == "select_sync_changes"
        ));
        assert_eq!(counting.held(), 0);
        purge_excess.push(counting.native_excess.load(AtomicOrdering::SeqCst));
        let retry = page(&db, since, 1);
        assert_eq!(
            retry.purges, control.purges,
            "refusal preserves the complete purge for retry"
        );
        assert_eq!(retry.through, control.through);
        assert_eq!(retry.held_after, 0);
    }
    assert!(
        purge_excess[1] <= purge_excess[0],
        "purge refusal frees its decoded item before refunding it: {purge_excess:?}"
    );
}

// A resident tail larger than the reader's whole budget pulls in pages that
// each fit: under a 2 MiB reader a 12 MiB tail is delivered once, in order.
#[test]
fn a_resident_tail_larger_than_the_reader_budget_pulls_in_pages() {
    let db = caller_driven(Database::open_memory());
    synced_notes(&db);
    commit_uuid_rows(&db, 0, 3_000);
    let reader = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(2 * 1024 * 1024);
    let mut since = Lsn(0);
    let mut delivered = Vec::new();
    loop {
        let (outcome, counting) = page_from(
            &db,
            since,
            PAGE_BYTES,
            reader.child().expect("a request reader"),
        );
        let pulled = outcome.expect("a page that fits the reader");
        assert_eq!(counting.held(), 0, "the page returned its whole admission");
        delivered.extend(pulled.occurrences);
        match pulled.through {
            Some(through) => since = through,
            None => break,
        }
    }
    assert_eq!(delivered.len(), 3_000, "every row exactly once");
    let lsns = delivered.iter().map(|o| o.0).collect::<Vec<_>>();
    assert!(
        lsns.windows(2).all(|pair| pair[0] < pair[1]),
        "delivered in increasing LSN order, none repeated"
    );
}

// Transactions of several rows are never split: every page holds whole groups,
// consecutive pages cover disjoint LSN ranges, and the union is every row once.
#[test]
fn equal_lsn_groups_are_never_split_across_resident_or_durable_pages() {
    for durable in [false, true] {
        let dir = tempfile::tempdir().expect("store directory");
        let db = caller_driven(if durable {
            Database::open(dir.path().join("hub.db")).expect("open hub")
        } else {
            Database::open_memory()
        });
        synced_notes(&db);
        for group in 0..40usize {
            let tx = db.begin().expect("begin");
            for member in 0..7usize {
                let i = group * 7 + member;
                db.insert_row(
                    tx,
                    "notes",
                    HashMap::from([
                        (
                            "id".to_string(),
                            Value::Uuid(uuid::Uuid::from_u128(i as u128 + 1)),
                        ),
                        ("body".to_string(), Value::Text(fat(i))),
                    ]),
                )
                .expect("insert a member");
            }
            db.commit(tx).expect("commit a group");
        }
        let pages = pull_all_with(&db, 8 * 1024, |_, _| {});
        assert!(pages.len() > 1, "durable={durable}: the pull needs pages");
        let mut seen_lsns = std::collections::BTreeSet::new();
        let mut last_max = 0u64;
        for pulled in &pages {
            let mut per_lsn: BTreeMap<u64, usize> = BTreeMap::new();
            for (lsn, ..) in &pulled.occurrences {
                *per_lsn.entry(*lsn).or_default() += 1;
            }
            assert!(
                per_lsn.values().all(|members| *members == 7),
                "durable={durable}: a page holds only whole groups: {per_lsn:?}"
            );
            for lsn in per_lsn.keys() {
                assert!(*lsn > last_max, "durable={durable}: ranges are disjoint");
                assert!(seen_lsns.insert(*lsn), "durable={durable}: group repeated");
            }
            last_max = per_lsn.keys().next_back().copied().unwrap_or(last_max);
        }
        assert_eq!(every_occurrence(&pages).len(), 280, "durable={durable}");
    }
}

// A cursor at or past the committed frontier, including the largest LSN, is an
// empty final page: no overflow, no entry, no cursor.
#[test]
fn a_cursor_at_the_frontier_or_the_maximum_lsn_is_an_empty_final_page() {
    for durable in [false, true] {
        let dir = tempfile::tempdir().expect("store directory");
        let db = caller_driven(if durable {
            Database::open(dir.path().join("hub.db")).expect("open hub")
        } else {
            Database::open_memory()
        });
        synced_notes(&db);
        commit_uuid_rows(&db, 0, 5);
        for since in [db.current_lsn(), Lsn(u64::MAX)] {
            let empty = page(&db, since, PAGE_BYTES);
            assert!(
                empty.occurrences.is_empty() && empty.ddl == 0,
                "durable={durable} since={since:?}: nothing after the frontier"
            );
            assert_eq!(empty.through, None, "durable={durable}: final page");
            assert_eq!(empty.held_after, 0, "durable={durable}: nothing held");
        }
    }
}

// Rows of a table no peer pulls (PUSH ONLY) fill the log between the rows a
// pull does owe. The page selection consumes every group it passes, filtered or
// not: the cursor advances past them, each occurrence is consumed once, and the
// pull ends.
#[test]
fn filtered_entries_are_consumed_and_the_cursor_moves_past_them() {
    for durable in [false, true] {
        let dir = tempfile::tempdir().expect("store directory");
        let db = caller_driven(if durable {
            Database::open(dir.path().join("hub.db")).expect("open hub")
        } else {
            Database::open_memory()
        });
        db.execute(
            "CREATE TABLE owed (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
            &HashMap::new(),
        )
        .expect("declare owed");
        db.execute(
            "CREATE TABLE hidden (id INTEGER PRIMARY KEY, body TEXT) SYNC PUSH ONLY",
            &HashMap::new(),
        )
        .expect("declare private");
        for i in 0..60i64 {
            let (table, body) = if i % 6 == 0 {
                ("owed", fat(i as usize))
            } else {
                ("hidden", fat(i as usize))
            };
            db.execute(
                &format!("INSERT INTO {table} (id, body) VALUES ($id, $body)"),
                &HashMap::from([
                    ("id".to_string(), Value::Int64(i)),
                    ("body".to_string(), Value::Text(body)),
                ]),
            )
            .expect("insert");
        }
        let pages = pull_all_with(&db, 8 * 1024, |_, _| {});
        let owed = every_occurrence(&pages)
            .into_iter()
            .filter(|o| o.1 == "owed")
            .collect::<Vec<_>>();
        assert_eq!(owed.len(), 10, "durable={durable}: each owed row once");
        let all = every_occurrence(&pages);
        let distinct = all.iter().collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            (all.len(), distinct.len()),
            (60, 60),
            "durable={durable}: every logged occurrence consumed exactly once"
        );
    }
}

// Pruning between pages (current-only history compaction) changes what a later
// page can see but never what an earlier page consumed: no occurrence is
// delivered twice, none the control delivered at or below a cursor is lost
// when it is still current, and the rows the edge resolves to are identical to
// an unpruned control hub.
#[test]
fn pruning_between_resident_pages_matches_the_unpruned_control() {
    let build = || {
        let db = caller_driven(Database::open_memory());
        current_notes(&db);
        for id in 0..60i64 {
            write_note(&db, id, &fat(id as usize));
        }
        for id in 0..60i64 {
            rewrite_note(&db, id, &fat(id as usize + 1000));
        }
        db
    };
    let control = build();
    let control_pages = pull_all_with(&control, 16 * 1024, |_, _| {});
    let pruned = build();
    let mut removed = 0u64;
    let pruned_pages = pull_all_with(&pruned, 16 * 1024, |index, _| {
        if index == 0 {
            removed = pruned
                .compact_currency_versions()
                .expect("compact between pages")
                .pruned_change_log_entries;
        }
    });
    assert!(removed > 0, "the pass removed superseded log entries");
    let delivered = every_occurrence(&pruned_pages);
    let unique = delivered.iter().collect::<std::collections::BTreeSet<_>>();
    assert_eq!(unique.len(), delivered.len(), "no occurrence twice");
    assert_eq!(
        resolved(&pruned_pages),
        resolved(&control_pages),
        "the edge resolves to the control's rows"
    );
    let first_through = control_pages[0].through.expect("control needs pages");
    let first_page_delivered = pruned_pages[0]
        .occurrences
        .iter()
        .map(|o| o.0)
        .collect::<Vec<_>>();
    assert!(
        first_page_delivered
            .iter()
            .all(|lsn| *lsn <= first_through.0),
        "the first page stays at or below its own cursor"
    );
}

// Cancellation: a page whose output is dropped, or that is refused, leaves the
// cursor where it was; the next request from the same cursor returns the same
// page. The page's budget is whole afterwards.
#[test]
fn a_dropped_page_changes_nothing_for_the_next_request() {
    for durable in [false, true] {
        let dir = tempfile::tempdir().expect("store directory");
        let db = caller_driven(if durable {
            Database::open(dir.path().join("hub.db")).expect("open hub")
        } else {
            Database::open_memory()
        });
        synced_notes(&db);
        commit_uuid_rows(&db, 0, 120);
        let first = page(&db, Lsn(0), 16 * 1024);
        let again = page(&db, Lsn(0), 16 * 1024);
        assert_eq!(first.occurrences, again.occurrences, "durable={durable}");
        assert_eq!(first.through, again.through, "durable={durable}");
        assert_eq!(first.held_after, 0, "durable={durable}");
        assert_eq!(again.held_after, 0, "durable={durable}");
    }
}

// A hub that has deleted rows owes their deletions to every peer. A page
// selects those deletions from where the cursor is, a page at a time, so the
// bytes a page admits do not grow with the deletions the later pages will
// carry, and each deletion arrives exactly once in order.
#[test]
fn pending_deletes_are_selected_a_page_at_a_time() {
    for durable in [false, true] {
        pending_deletes_page_is_flat(durable);
    }
    // Only the pending source still carries this deletion. Accept its
    // canonical obligation after capture, before the first source read; the
    // already captured page must retain the original Pending occurrence.
    let db = caller_driven(Database::open_memory());
    synced_notes(&db);
    commit_uuid_rows(&db, 0, 2);
    db.execute(
        "DELETE FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(uuid::Uuid::from_u128(1)))]),
    )
    .expect("create the pending deletion");
    db.__force_change_log_gap_for_test();
    commit_uuid_rows(&db, 2, 1);
    let control = page_with_work_between_reads(&db, |_| Ok(())).expect("pending control");
    assert_eq!(control.rows.iter().filter(|row| row.deleted).count(), 1);
    let transitioned = Cell::new(false);
    let captured = page_with_work_between_reads(&db, |call| {
        if call == 0 {
            assert_eq!(db.active_pull_history_registrations_for_test(), 1);
            let mut state = db.lineage_state_lock.lock();
            let (key, mut record) = state
                .records
                .iter()
                .find(|(_, record)| record.delete_obligation == DurableDeleteObligation::Pending)
                .map(|(key, record)| (key.clone(), record.clone()))
                .expect("the canonical obligation is still pending");
            record.delete_obligation = DurableDeleteObligation::Accepted;
            record.accepted_hub_lsn = Some(db.current_lsn().0);
            db.store_lineage_record(&mut state, &key, &record)?;
            assert_eq!(
                state.records[&key].delete_obligation,
                DurableDeleteObligation::Accepted
            );
            transitioned.set(true);
        }
        Ok(())
    })
    .expect("captured pending page after acceptance");
    assert!(transitioned.get());
    assert_eq!(
        serde_json::to_value(&captured).expect("captured page values"),
        serde_json::to_value(&control).expect("control page values")
    );
    assert_eq!(db.active_pull_history_registrations_for_test(), 0);
}

fn pending_deletes_page_is_flat(durable: bool) {
    let mut admitted = Vec::new();
    for count in [200usize, 400] {
        let dir = tempfile::tempdir().expect("store directory");
        let db = caller_driven(if durable {
            Database::open(dir.path().join("hub.db")).expect("open hub")
        } else {
            Database::open_memory()
        });
        synced_notes(&db);
        commit_uuid_rows(&db, 0, count);
        for i in 0..count {
            db.execute(
                "DELETE FROM notes WHERE id = $id",
                &HashMap::from([(
                    "id".to_string(),
                    Value::Uuid(uuid::Uuid::from_u128(i as u128 + 1)),
                )]),
            )
            .expect("delete a row");
        }
        let after_inserts = Lsn(count as u64 + 1);
        let first = page(&db, after_inserts, 16 * 1024);
        assert!(
            first.through.is_some(),
            "{count} deletions do not fit one page"
        );
        admitted.push((first.admitted, first.occurrences.len()));
        let pages = pull_all_with(&db, 16 * 1024, |_, _| {});
        let deletes = every_occurrence(&pages)
            .into_iter()
            .filter(|o| o.3)
            .collect::<Vec<_>>();
        let mut keys = deletes.iter().map(|o| o.2.clone()).collect::<Vec<_>>();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), count, "every deletion present");
        assert_eq!(deletes.len(), count, "every deletion exactly once");
        let lsns = deletes.iter().map(|o| o.0).collect::<Vec<_>>();
        assert!(
            lsns.windows(2).all(|pair| pair[0] <= pair[1]),
            "deletions arrive in LSN order"
        );
    }
    // Where a page ends depends on the width of the keys and positions it
    // carries, so the two pages may differ by a group. They may not differ by
    // the history behind them: doubling the pending deletions may grow what the
    // page admits by no more than a tenth (it nearly doubles when the whole
    // pending set is admitted).
    let (small, small_rows) = admitted[0];
    let (large, _) = admitted[1];
    assert!(
        large <= small + small / 10,
        "durable={durable}: a page admits {small} bytes for {small_rows} deletions at 200 and {large} at 400"
    );
}

// A page that carries an OLD version of a row reads exactly that version, never
// the versions written after it: the first insertion is selected (the page
// stops after one group) while 50 or 100 newer versions of the same row sit
// behind it, and the page holds the old value at the old LSN and costs the same
// row-version visits either way.
#[test]
fn a_page_of_an_old_version_reads_only_that_version_behind_newer_ones() {
    for durable in [false, true] {
        old_version_page_is_flat(durable);
        old_version_page_survives_column_retirement(durable);
    }
}

// Retiring a column changes the new declaration, while an older source
// occurrence still carries its original authored values after reopen.
fn old_version_page_survives_column_retirement(durable: bool) {
    let dir = tempfile::tempdir().expect("historical declaration directory");
    let path = dir.path().join("schema.db");
    let db = caller_driven(if durable {
        Database::open(&path).expect("open historical declaration store")
    } else {
        Database::open_memory()
    });
    db.execute(
        "CREATE TABLE retired_values (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL SYNC TWO WAY",
        &HashMap::new(),
    )
    .expect("declare the original layout");
    let since = db.current_lsn();
    db.execute(
        "INSERT INTO retired_values VALUES (1, 'original authored value')",
        &HashMap::new(),
    )
    .expect("write the original layout");
    db.execute(
        "INSERT INTO retired_values VALUES (2, 'a later complete group')",
        &HashMap::new(),
    )
    .expect("place a later group beyond the first page cut");
    let read_original = |db: &Database| {
        db.with_checked_changes_page_for_test(
            since,
            1,
            db.engine_sync_read_memory().expect("historical page owner"),
            |changes, through| (serde_json::to_value(changes).unwrap(), through),
        )
        .expect("read the historical occurrence")
    };
    let original = read_original(&db);
    db.execute(
        "ALTER TABLE retired_values DROP COLUMN body",
        &HashMap::new(),
    )
    .expect("retire the column");
    assert_eq!(
        read_original(&db),
        original,
        "column retirement preserves the complete original historical page"
    );
    if durable {
        drop(db);
        let reopened = caller_driven(Database::open(&path).expect("reopen historical declaration"));
        assert_eq!(read_original(&reopened), original);
    }
}

// The historical page is an authored occurrence, not a projection of the
// table that happens to be live when an edge asks for it.  Keep the resident,
// durable, and reopened routes independent so a resident failure cannot hide
// either file-backed route.
fn historical_page_after_later_schema_change(durable: bool, reopen: bool, recreate: bool) {
    let dir = tempfile::tempdir().expect("historical schema directory");
    let path = dir.path().join("schema.db");
    let db = caller_driven(if durable {
        Database::open(&path).expect("open historical schema store")
    } else {
        Database::open_memory()
    });
    db.execute(
        "CREATE TABLE historical_values (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL SYNC TWO WAY",
        &HashMap::new(),
    )
    .expect("declare the authored layout");
    let authored_schema = db.table_meta("historical_values").expect("authored schema");
    let since = db.current_lsn();
    db.execute(
        "INSERT INTO historical_values VALUES (1, 'original authored value')",
        &HashMap::new(),
    )
    .expect("write the authored row");
    db.execute(
        "INSERT INTO historical_values VALUES (2, 'later independent row')",
        &HashMap::new(),
    )
    .expect("put an independent later group beyond the page cut");

    let original = historical_page_json(&db, since);
    assert_eq!(
        original.0["rows"][0]["values"]["body"],
        serde_json::to_value(Value::Text("original authored value".to_owned()))
            .expect("serialize the existing authored Value form")
    );

    if recreate {
        db.execute("DROP TABLE historical_values", &HashMap::new())
            .expect("retire the old physical table");
        db.execute(
            "CREATE TABLE historical_values (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL SYNC TWO WAY",
            &HashMap::new(),
        )
        .expect("create a new physical table with the same declared schema");
        assert_eq!(
            db.table_meta("historical_values")
                .expect("recreated schema"),
            authored_schema,
            "the current declaration alone cannot identify the old physical incarnation"
        );
    } else {
        db.execute(
            "ALTER TABLE historical_values DROP COLUMN body",
            &HashMap::new(),
        )
        .expect("retire the authored column");
        assert_ne!(
            db.table_meta("historical_values").expect("retired schema"),
            authored_schema,
            "the live declaration changed after the selected occurrence"
        );
    }

    let after = if reopen {
        drop(db);
        let reopened =
            caller_driven(Database::open(&path).expect("reopen historical schema store"));
        historical_page_json(&reopened, since)
    } else {
        historical_page_json(&db, since)
    };
    assert_eq!(
        after, original,
        "the complete old page and cursor retain its authored body and schema despite a later physical change"
    );
}

fn historical_page_json(db: &Database, since: Lsn) -> (serde_json::Value, Option<Lsn>) {
    db.with_checked_changes_page_for_test(
        since,
        1,
        db.engine_sync_read_memory().expect("historical page owner"),
        |changes, through| (serde_json::to_value(changes).unwrap(), through),
    )
    .expect("read the selected historical occurrence")
}

#[test]
fn a_resident_historical_page_keeps_its_authored_body_after_column_retirement() {
    historical_page_after_later_schema_change(false, false, false);
}

#[test]
fn a_resident_historical_page_keeps_its_authored_body_after_table_recreation() {
    historical_page_after_later_schema_change(false, false, true);
}

#[test]
fn a_durable_historical_page_keeps_its_authored_body_after_column_retirement() {
    historical_page_after_later_schema_change(true, false, false);
}

#[test]
fn a_durable_historical_page_keeps_its_authored_body_after_table_recreation() {
    historical_page_after_later_schema_change(true, false, true);
}

#[test]
fn a_reopened_historical_page_keeps_its_authored_body_after_column_retirement() {
    historical_page_after_later_schema_change(true, true, false);
}

#[test]
fn a_reopened_historical_page_keeps_its_authored_body_after_table_recreation() {
    historical_page_after_later_schema_change(true, true, true);
}

// A plain HISTORY ALL row has no manifested custody or unresolved foreign-key
// dependency. Its future versions therefore cannot enlarge the earlier page.
#[test]
fn an_independent_historical_row_does_not_drag_its_future_version_into_a_page() {
    for durable in [false, true] {
        let dir = tempfile::tempdir().expect("independent history directory");
        let db = caller_driven(if durable {
            Database::open(dir.path().join("independent.db")).expect("open independent store")
        } else {
            Database::open_memory()
        });
        db.execute(
            "CREATE TABLE independent_history (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL SYNC TWO WAY",
            &HashMap::new(),
        )
        .expect("declare independent history");
        let since = db.current_lsn();
        db.execute(
            "INSERT INTO independent_history VALUES (1, 'old independent body')",
            &HashMap::new(),
        )
        .expect("write the old independent row");
        let old_lsn = db.current_lsn();
        db.execute(
            "UPDATE independent_history SET body = 'future independent body' WHERE id = 1",
            &HashMap::new(),
        )
        .expect("write the future independent version");
        let selected = page(&db, since, 1);
        assert_eq!(selected.through, Some(old_lsn), "durable={durable}");
        assert_eq!(
            selected
                .occurrences
                .iter()
                .map(|entry| entry.0)
                .collect::<Vec<_>>(),
            vec![old_lsn.0]
        );
        assert_eq!(
            selected.bodies,
            vec![format!(
                "{:?}",
                Some(&Value::Text("old independent body".to_string()))
            )],
            "durable={durable}: an ordinary row does not pull its future version"
        );
    }
}

fn old_version_page_is_flat(durable: bool) {
    let mut visited = Vec::new();
    for newer in [50usize, 100] {
        let dir = tempfile::tempdir().expect("store directory");
        let db = caller_driven(if durable {
            Database::open(dir.path().join("hub.db")).expect("open hub")
        } else {
            Database::open_memory()
        });
        db.execute(
            "CREATE TABLE hot (id INT PRIMARY KEY, body TEXT) HISTORY ALL SYNC TWO WAY",
            &HashMap::new(),
        )
        .expect("declare hot");
        let created = db.current_lsn();
        let write = |sql: &str, version: usize| {
            db.execute(
                sql,
                &HashMap::from([
                    ("id".to_string(), Value::Int64(1)),
                    (
                        "body".to_string(),
                        Value::Text(format!("version {version:06}")),
                    ),
                ]),
            )
            .expect("write the hot row");
        };
        write("INSERT INTO hot (id, body) VALUES ($id, $body)", 0);
        let inserted = db.current_lsn();
        for version in 1..=newer {
            write("UPDATE hot SET body = $body WHERE id = $id", version);
        }
        let scope = crate::read_work_probe::ReadWorkObservationScope::enter(
            crate::read_work_probe::ReadWorkPhase::Query,
        );
        let (selected, history) = crate::writer_query_test_seams::observe(|| page(&db, created, 1));
        let work = scope.counters();
        assert_eq!(
            selected.through,
            Some(inserted),
            "the page stops after one group"
        );
        assert_eq!(
            selected.occurrences.iter().map(|o| o.0).collect::<Vec<_>>(),
            vec![inserted.0],
            "the page carries the insertion, at its own LSN"
        );
        assert_eq!(
            selected.bodies,
            vec![format!(
                "{:?}",
                Some(&Value::Text("version 000000".to_string()))
            )],
            "the page carries the old value"
        );
        visited.push((
            work.decoded_rows,
            work.stored_row_bytes,
            work.row_version_range_reads,
            work.row_version_point_reads,
            history.history.row_versions,
        ));
        if durable && newer == 50 {
            // Fault the actual selected indexed body after physical-key
            // validation. A mismatched occurrence refuses the entire page;
            // it cannot become a successful missing-row answer.
            db.arm_exact_row_lsn_fault_for_test();
            let (faulted, counting) = page_from(
                &db,
                created,
                1,
                db.engine_sync_read_memory().expect("faulted page owner"),
            );
            match faulted {
                Err(Error::StoreCorrupted { path, reason }) => {
                    assert_eq!(path, "hot");
                    assert!(reason.contains(&format!("LSN {}", inserted.0)));
                }
                Err(other) => panic!("the exact occurrence requires typed corruption: {other:?}"),
                Ok(_) => panic!("a different row LSN cannot yield a successful page"),
            }
            assert_eq!(counting.held(), 0, "the refused page releases its owner");
            let retried = page(&db, created, 1);
            assert_eq!(retried.occurrences, selected.occurrences);
            assert_eq!(retried.bodies, selected.bodies);
            assert_eq!(retried.through, selected.through);
            assert_eq!(retried.held_after, 0);
        }
    }
    assert_eq!(
        visited[0], visited[1],
        "durable={durable}: the page decodes the same rows and bytes and reads the same versions behind a doubled count of newer versions"
    );
    assert!(
        !durable || visited[0].0 >= 1,
        "the page decoded the row it carries"
    );
}

/// A whole-store page (every group, one page) read with `during` run at each
/// per-entry checkpoint, the way a page in flight lets other work run between
/// two of its source reads. `during` receives the checkpoint's call number
/// (0 is the first source entry's).
fn page_with_work_between_reads(
    db: &Database,
    during: impl Fn(usize) -> Result<()>,
) -> Result<ChangeSet> {
    let memory = db.engine_sync_read_memory()?;
    let mut vector_copy = crate::vector_read_memory::admission(memory.clone());
    let calls = Cell::new(0usize);
    let checkpoint = || {
        let call = calls.get();
        calls.set(call + 1);
        during(call)
    };
    let admit = |_: usize| Ok(());
    let release = |_: usize| Ok(());
    let admitted = || 0usize;
    let through = Cell::new(None);
    let purge_items = std::cell::RefCell::new(Vec::new());
    let page = SelectionPage {
        bytes: usize::MAX,
        admitted: &admitted,
        through: &through,
        purge_items: &purge_items,
    };
    let projection = ChangeProjection {
        checkpoint: &checkpoint,
        admit: &admit,
        release: &release,
        bounded: true,
        ddl: true,
        page: Some(&page),
    };
    db.changes_since_base_projected(Lsn(0), &projection, &mut vector_copy, memory)
        .map(|(changes, _)| changes)
}

/// Three rows written by one transaction at LSN L, each superseded afterwards,
/// on a table whose superseded versions and log entries are reclaimable.
fn one_group_then_supersessions() -> Database {
    let db = caller_driven(Database::open_memory());
    current_notes(&db);
    let tx = db.begin().expect("begin");
    for id in 0..3i64 {
        db.insert_row(
            tx,
            "notes",
            HashMap::from([
                ("id".to_string(), Value::Int64(id)),
                ("body".to_string(), Value::Text(format!("first-{id}"))),
            ]),
        )
        .expect("insert a member");
    }
    db.commit(tx).expect("commit the group");
    for id in 0..3i64 {
        rewrite_note(&db, id, &format!("second-{id}"));
    }
    db
}

fn lsns(changes: &ChangeSet) -> Vec<u64> {
    changes.rows.iter().map(|row| row.lsn.0).collect()
}

// A page in flight protects the group it is reading: after it has taken e0 of
// the three-member group [e0,e1,e2] at one LSN, a reclaim pass finds nothing it
// may remove (the removal defers, not just the member being read), the page
// still delivers e0, e1 and e2 exactly once, and the same pass run after the
// page is gone removes all three.
#[test]
fn a_page_in_flight_defers_the_removal_of_the_group_it_is_reading() {
    let db = one_group_then_supersessions();
    assert_eq!(db.active_pull_history_registrations_for_test(), 0);
    let during_page = Cell::new(None);
    let page = page_with_work_between_reads(&db, |call| {
        if call == 1 {
            assert_eq!(db.active_pull_history_registrations_for_test(), 1);
            during_page.set(Some(
                db.compact_currency_versions()?.pruned_change_log_entries,
            ));
        }
        Ok(())
    })
    .expect("the page");
    assert_eq!(db.active_pull_history_registrations_for_test(), 0);
    assert_eq!(
        during_page.get(),
        Some(0),
        "a reclaim pass run while the page is in flight removes none of its entries"
    );
    let control = page_with_work_between_reads(&one_group_then_supersessions(), |_| Ok(()))
        .expect("the control page");
    let shape = |changes: &ChangeSet| {
        changes
            .rows
            .iter()
            .map(|row| (row.lsn.0, format!("{:?}", row.natural_key), row.deleted))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        shape(&page),
        shape(&control),
        "the page read while a reclaim pass ran delivers what an undisturbed page delivers"
    );
    let after = db
        .compact_currency_versions()
        .expect("reclaim after the page")
        .pruned_change_log_entries;
    assert_eq!(after, 3, "the deferred removal runs once the page is gone");

    // Capture starts while a removal pass owns the boundary. Observe an
    // actual failed acquisition, rather than inferring a parked reader from
    // a delay or a thread-start signal.
    let db = Arc::new(one_group_then_supersessions());
    let removal_pause = db.pause_after_currency_floor_sample_for_test();
    let removal_db = db.clone();
    let removal = std::thread::spawn(move || removal_db.compact_currency_versions());
    assert!(removal_pause.wait_until_reached(std::time::Duration::from_secs(30)));
    assert!(db.active_removal_pass_for_test());
    let capture_pause = db.pause_before_pull_history_capture_for_test();
    let (wait_tx, wait_rx) = std::sync::mpsc::sync_channel(0);
    let (copy_tx, copy_rx) = std::sync::mpsc::sync_channel(0);
    let (finish_tx, finish_rx) = std::sync::mpsc::sync_channel(0);
    let reader_db = db.clone();
    let reader = std::thread::spawn(move || {
        let noticed = std::sync::atomic::AtomicBool::new(false);
        contextdb_tx::commit_wait_test_seams::observe(
            Arc::new(move || {
                if !noticed.swap(true, AtomicOrdering::SeqCst) {
                    wait_tx.send(()).expect("capture wait is observed");
                }
            }),
            || {
                page_with_work_between_reads(&reader_db, |call| {
                    if call == 0 {
                        assert!(!reader_db.active_removal_pass_for_test());
                        assert_eq!(reader_db.active_pull_history_registrations_for_test(), 1);
                        copy_tx.send(()).expect("captured page is observed");
                        finish_rx.recv().expect("release the captured page");
                    }
                    Ok(())
                })
            },
        )
    });
    assert!(capture_pause.wait_until_reached(std::time::Duration::from_secs(30)));
    assert_eq!(db.active_pull_history_registrations_for_test(), 0);
    capture_pause.release();
    wait_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("capture really contends with the parked removal pass");
    assert!(db.active_removal_pass_for_test());
    assert_eq!(db.active_pull_history_registrations_for_test(), 0);
    removal_pause.release();
    assert_eq!(
        removal.join().unwrap().unwrap().pruned_change_log_entries,
        3
    );
    copy_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("capture proceeds after the removal pass");
    let control = page_with_work_between_reads(&db, |_| Ok(())).expect("post-removal control");
    let frontier = db.current_lsn();
    let writer_db = db.clone();
    let (written_tx, written_rx) = std::sync::mpsc::sync_channel(0);
    let writer = std::thread::spawn(move || {
        write_note(
            &writer_db,
            99,
            "committed while the captured page is paused",
        );
        written_tx.send(()).expect("writer completion is observed");
    });
    written_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("a writer completes before the captured page is released");
    writer.join().unwrap();
    finish_tx.send(()).expect("release the captured page");
    let captured = reader.join().unwrap().expect("captured page completes");
    assert_eq!(
        serde_json::to_value(&captured).unwrap(),
        serde_json::to_value(&control).unwrap()
    );
    assert!(captured.rows.iter().all(|row| row.lsn <= frontier));
    assert_eq!(db.active_pull_history_registrations_for_test(), 0);
}

// A page that is refused or cancelled mid-read lets go of what it protected:
// the pass that deferred while it was in flight removes the group afterwards.
#[test]
fn a_cancelled_page_in_flight_releases_its_protection() {
    let db = one_group_then_supersessions();
    let during_page = Cell::new(None);
    let outcome = page_with_work_between_reads(&db, |call| {
        if call == 1 {
            assert_eq!(db.active_pull_history_registrations_for_test(), 1);
            during_page.set(Some(
                db.compact_currency_versions()?.pruned_change_log_entries,
            ));
            return Err(Error::Other("cancelled between two source reads".into()));
        }
        Ok(())
    });
    assert!(outcome.is_err(), "the page ends with the cancellation");
    assert_eq!(db.active_pull_history_registrations_for_test(), 0);
    assert_eq!(during_page.get(), Some(0), "removal deferred while it read");
    let after = db
        .compact_currency_versions()
        .expect("reclaim after the cancellation")
        .pruned_change_log_entries;
    assert_eq!(after, 3, "cancellation released the protection");
}

// Ordinary writes are not held up by a page in flight, and nothing written
// after the page began belongs to it: a commit between two source reads
// succeeds, the page ends at the frontier it captured, and the next page from
// that frontier carries the new row once.
#[test]
fn commits_during_a_page_proceed_and_are_not_part_of_it() {
    let db = one_group_then_supersessions();
    let frontier = db.current_lsn();
    let written = Cell::new(false);
    let page = page_with_work_between_reads(&db, |call| {
        if call == 1 {
            write_note(&db, 99, "written while the page was in flight");
            written.set(true);
        }
        Ok(())
    })
    .expect("the page");
    assert!(written.get(), "the writer was not held up by the page");
    assert!(
        lsns(&page).iter().all(|lsn| *lsn <= frontier.0),
        "nothing committed after the page began is part of it: {:?}",
        lsns(&page)
    );
    let next = page_from(
        &db,
        frontier,
        PAGE_BYTES,
        db.engine_sync_read_memory().expect("read memory"),
    )
    .0
    .expect("the next page");
    assert_eq!(next.occurrences.len(), 1, "the new row arrives once");
}

fn gap_hub(rows: usize, versions: usize) -> (tempfile::TempDir, Database) {
    let dir = tempfile::tempdir().expect("store directory");
    let db = caller_driven(Database::open(dir.path().join("hub.db")).expect("open hub"));
    db.execute(
        "CREATE TABLE hot (id INT PRIMARY KEY, body TEXT) HISTORY ALL SYNC TWO WAY",
        &HashMap::new(),
    )
    .expect("declare hot");
    for id in 0..rows as i64 {
        db.execute(
            "INSERT INTO hot (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_string(), Value::Int64(id)),
                (
                    "body".to_string(),
                    Value::Text(format!("{id:06} {}", "x".repeat(200))),
                ),
            ]),
        )
        .expect("insert");
    }
    for round in 1..versions {
        for id in 0..rows as i64 {
            rewrite_hot(&db, id, round);
        }
    }
    (dir, db)
}

fn rewrite_hot(db: &Database, id: i64, round: usize) {
    db.execute(
        "UPDATE hot SET body = $body WHERE id = $id",
        &HashMap::from([
            ("id".to_string(), Value::Int64(id)),
            (
                "body".to_string(),
                Value::Text(format!("{id:06} {round:04} {}", "y".repeat(195))),
            ),
        ]),
    )
    .expect("supersede");
}

// A store whose in-memory change log no longer covers the cursor still serves
// a page from its surviving durable log: the work of a fixed-size page is the
// same at N, 2N and 4N rows with every row superseded once, for the first page
// and for a page deep into the history. The page never walks the rows or
// versions before or after its own groups.
#[test]
fn a_page_served_after_a_log_gap_costs_the_same_at_n_2n_and_4n() {
    let mut first_costs = Vec::new();
    let mut late_costs = Vec::new();
    for rows in [150usize, 300, 600] {
        let (_dir, db) = gap_hub(rows, 2);
        db.__force_change_log_gap_for_test();
        let measure = |since: Lsn| {
            let scope = crate::read_work_probe::ReadWorkObservationScope::enter(
                crate::read_work_probe::ReadWorkPhase::Query,
            );
            let pulled = page(&db, since, 8 * 1024);
            let work = scope.counters();
            (
                pulled.through,
                pulled.occurrences.len(),
                work.decoded_rows,
                work.row_version_range_reads,
                work.row_version_point_reads,
            )
        };
        let first = measure(Lsn(0));
        assert!(first.0.is_some(), "rows={rows}: the history needs pages");
        // A cursor a fixed distance from the first page, then the same distance
        // from the end: both are far from either end of a larger history.
        let late_cursor = Lsn(db.current_lsn().0 - 100);
        let late = measure(late_cursor);
        first_costs.push((first.1, first.2, first.3, first.4));
        late_costs.push((late.1, late.2, late.3, late.4));
    }
    assert_eq!(first_costs[0], first_costs[1], "first page: N against 2N");
    assert_eq!(first_costs[1], first_costs[2], "first page: 2N against 4N");
    assert_eq!(late_costs[0], late_costs[1], "late page: N against 2N");
    assert_eq!(late_costs[1], late_costs[2], "late page: 2N against 4N");
}

/// One transaction of `members` rows of `member_bytes` each, on a hub whose
/// in-memory change log has a forced gap.
fn wide_group_hub(members: usize, member_bytes: usize) -> (tempfile::TempDir, Database) {
    let dir = tempfile::tempdir().expect("store directory");
    let db = caller_driven(Database::open(dir.path().join("hub.db")).expect("open hub"));
    db.execute(
        "CREATE TABLE wide (id INT PRIMARY KEY, body TEXT) SYNC TWO WAY",
        &HashMap::new(),
    )
    .expect("declare wide");
    let tx = db.begin().expect("begin");
    for id in 0..members as i64 {
        db.insert_row(
            tx,
            "wide",
            HashMap::from([
                ("id".to_string(), Value::Int64(id)),
                ("body".to_string(), Value::Text("w".repeat(member_bytes))),
            ]),
        )
        .expect("insert a wide member");
    }
    db.commit(tx).expect("commit the group");
    db.__force_change_log_gap_for_test();
    (dir, db)
}

// An indivisible group larger than the reader's whole budget is refused with a
// typed memory refusal BEFORE any of it is copied: the budget never holds more
// than its limit, holds nothing afterwards, no cursor moves, and the same
// request under a sufficient budget then delivers the whole group once.
#[test]
fn an_oversized_group_after_a_log_gap_is_refused_before_any_copy() {
    let (_dir, db) = wide_group_hub(10, 512 * 1024);
    let limit = 2 * 1024 * 1024;
    let reader = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(limit);
    for attempt in 0..2 {
        let (outcome, counting) = page_from(
            &db,
            Lsn(0),
            PAGE_BYTES,
            reader.child().expect("a request reader"),
        );
        assert!(
            matches!(outcome, Err(Error::MemoryBudgetExceeded { .. })),
            "attempt {attempt}: a 5 MiB group does not fit a 2 MiB reader"
        );
        assert_eq!(counting.held(), 0, "attempt {attempt}: nothing stays held");
        assert!(
            counting.held_high.load(AtomicOrdering::SeqCst) <= limit as usize,
            "attempt {attempt}: admission never exceeded the limit, so no copy outran it"
        );
    }
    let roomy = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(64 << 20);
    let (outcome, counting) = page_from(
        &db,
        Lsn(0),
        PAGE_BYTES,
        roomy.child().expect("a roomy reader"),
    );
    let pulled = outcome.expect("the same request under a sufficient budget");
    assert_eq!(
        pulled.occurrences.len(),
        10,
        "the group arrives whole, once"
    );
    assert_eq!(pulled.through, None, "and it is the whole history");
    assert_eq!(counting.held(), 0);
}

// The cut selector is funded like everything else: with a reader that cannot
// hold even the selector's own storage, the page is refused with the typed
// memory refusal, holds nothing, and leaves the next request unchanged.
#[test]
fn an_exhausted_budget_refuses_the_cut_selector_and_leaves_no_trace() {
    let (_dir, db) = gap_hub(150, 2);
    db.__force_change_log_gap_for_test();
    let starved = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(2 * 1024);
    let (outcome, counting) = page_from(
        &db,
        Lsn(0),
        8 * 1024,
        starved.child().expect("a starved reader"),
    );
    assert!(
        matches!(outcome, Err(Error::MemoryBudgetExceeded { .. })),
        "a 2 KiB reader cannot fund a page"
    );
    assert_eq!(counting.held(), 0, "nothing stays held");
    let after = page(&db, Lsn(0), 8 * 1024);
    assert!(
        after.through.is_some() && !after.occurrences.is_empty(),
        "the next request is served normally"
    );
}

/// One `page_bytes = 1` walk from the origin. The cursor is the page `through`
/// LSN, and the final page is the empty frontier page.
fn authored_history_pages(db: &Database) -> Vec<(serde_json::Value, Option<u64>)> {
    let mut since = Lsn(0);
    let mut pages = Vec::new();
    loop {
        let (changes, through) = db
            .with_checked_changes_page_for_test(
                since,
                1,
                db.engine_sync_read_memory().expect("historical page owner"),
                |changes, through| (serde_json::to_value(changes).unwrap(), through),
            )
            .expect("read one authored history page");
        let through_raw = through.map(|lsn| lsn.0);
        pages.push((changes, through_raw));
        match through {
            Some(next) => {
                assert!(next > since, "every page moves the cursor forward");
                since = next;
            }
            None => break,
        }
        assert!(pages.len() < 100, "the pull makes progress");
    }
    pages
}

fn history_rows_with_body(
    pages: &[(serde_json::Value, Option<u64>)],
) -> Vec<(u64, serde_json::Value)> {
    let mut rows = Vec::new();
    for (changes, _) in pages {
        let Some(entries) = changes["rows"].as_array() else {
            continue;
        };
        for row in entries {
            let Some(body) = row["values"].get("body") else {
                continue;
            };
            let lsn = row["lsn"]
                .as_u64()
                .expect("a row LSN serializes as the transparent Lsn newtype");
            rows.push((lsn, body.clone()));
        }
    }
    rows
}

fn live_body(db: &Database, id: i64) -> Vec<Vec<Value>> {
    db.execute(
        "SELECT body FROM historical_values WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Int64(id))]),
    )
    .expect("read the live incarnation")
    .rows
}

// Policy 16: a snapshot exported after DROP/recreate still replays the authored
// occurrence, its body, its schema, and the source LSN cursor. Policy 12: the
// fresh same-key insert is a new life, distinct from that occurrence.
// The six historical page tests reopen the same store file. None reopen an
// `export_snapshot` artifact, and none place the fresh life in that stream.
#[test]
fn an_exported_snapshot_after_recreation_replays_authored_body_schema_and_a_distinct_fresh_occurrence()
{
    let dir = tempfile::tempdir().expect("historical export directory");
    let path = dir.path().join("schema.db");
    let db = caller_driven(Database::open(&path).expect("open historical schema store"));
    db.execute(
        "CREATE TABLE historical_values (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL SYNC TWO WAY",
        &HashMap::new(),
    )
    .expect("declare the authored layout");
    let authored_schema = db.table_meta("historical_values").expect("authored schema");
    let since = db.current_lsn();
    db.execute(
        "INSERT INTO historical_values VALUES (1, 'original authored value')",
        &HashMap::new(),
    )
    .expect("write the authored row");
    db.execute(
        "INSERT INTO historical_values VALUES (2, 'later independent row')",
        &HashMap::new(),
    )
    .expect("write an unrelated neighbor");
    let authored_occurrence = historical_page_json(&db, since);
    let original_body = serde_json::to_value(Value::Text("original authored value".to_owned()))
        .expect("serialize the existing authored Value form");
    assert_eq!(
        authored_occurrence.0["rows"][0]["values"]["body"],
        original_body,
        "the occurrence carries the authored Value::Text body"
    );
    assert!(
        authored_occurrence.0["rows"][0]["values"]
            .as_object()
            .expect("authored values")
            .contains_key("body"),
        "the occurrence keeps the authored body column"
    );

    db.execute("DROP TABLE historical_values", &HashMap::new())
        .expect("retire the old physical table");
    db.execute(
        "CREATE TABLE historical_values (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL SYNC TWO WAY",
        &HashMap::new(),
    )
    .expect("create a new physical table with the same declared schema");
    assert_eq!(
        db.table_meta("historical_values")
            .expect("recreated schema"),
        authored_schema,
        "the current declaration alone cannot identify the old physical incarnation"
    );
    db.execute(
        "INSERT INTO historical_values VALUES (1, 'fresh incarnation')",
        &HashMap::new(),
    )
    .expect("write a new life at the same key");
    // Two incarnations of id 2 share the body 'later independent row'.
    // The pre-DROP insert stays the first history neighbor; source_rows find
    // keeps that occurrence LSN. This insert is a fresh generation after
    // DROP/recreate and is the live row. It does not reconstruct the dropped
    // row as new history evidence. The new source page captures this fresh
    // neighbor separately.
    db.execute(
        "INSERT INTO historical_values VALUES (2, 'later independent row')",
        &HashMap::new(),
    )
    .expect("write a fresh neighbor life at id 2");

    let fresh_body = serde_json::to_value(Value::Text("fresh incarnation".to_owned()))
        .expect("serialize the fresh Value form");
    let neighbor_body = serde_json::to_value(Value::Text("later independent row".to_owned()))
        .expect("serialize the neighbor Value form");
    let source_pages = authored_history_pages(&db);
    assert!(
        source_pages.iter().any(|(changes, through)| {
            changes == &authored_occurrence.0 && *through == authored_occurrence.1.map(|lsn| lsn.0)
        }),
        "the post-recreation stream still serves the captured authored page and cursor"
    );
    let ddl_stream = source_pages
        .iter()
        .map(|(changes, _)| changes["ddl"].to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        ddl_stream.contains("historical_values") && ddl_stream.contains("body"),
        "the stream still carries the authored table schema: {ddl_stream}"
    );
    let source_rows = history_rows_with_body(&source_pages);
    let original_lsn = source_rows
        .iter()
        .find(|(_, body)| body == &original_body)
        .map(|(lsn, _)| *lsn)
        .expect("the authored body remains in the stream");
    let neighbor_lsn = source_rows
        .iter()
        .find(|(_, body)| body == &neighbor_body)
        .map(|(lsn, _)| *lsn)
        .expect("the unrelated neighbor remains in the stream");
    let fresh_lsn = source_rows
        .iter()
        .find(|(_, body)| body == &fresh_body)
        .map(|(lsn, _)| *lsn)
        .expect("the fresh incarnation is its own stream occurrence");
    assert_eq!(
        source_rows
            .iter()
            .filter(|(_, body)| body == &original_body)
            .count(),
        1
    );
    assert!(
        original_lsn < neighbor_lsn && neighbor_lsn < fresh_lsn,
        "source order is authored row, neighbor, then the fresh life: {source_rows:?}"
    );
    let mut cursor = 0u64;
    let mut moved = 0usize;
    for (_, through) in &source_pages {
        match through {
            Some(next) => {
                assert!(*next > cursor, "cursor progression is strict");
                cursor = *next;
                moved += 1;
            }
            None => {}
        }
    }
    assert!(moved >= 3, "the walk crossed more than one authored group");
    assert_eq!(source_pages.last().map(|(_, through)| *through), Some(None));
    assert_eq!(
        live_body(&db, 1),
        vec![vec![Value::Text("fresh incarnation".to_string())]]
    );
    assert_eq!(
        live_body(&db, 2),
        vec![vec![Value::Text("later independent row".to_string())]]
    );

    let artifact = dir.path().join("historical-export.db");
    db.export_snapshot(&artifact)
        .expect("export the store after recreation");
    assert_eq!(
        authored_history_pages(&db),
        source_pages,
        "export leaves the source stream unchanged"
    );
    drop(db);
    let reopened = caller_driven(Database::open(&artifact).expect("reopen the export artifact"));
    assert_eq!(
        authored_history_pages(&reopened),
        source_pages,
        "the artifact replays the authored body, schema, LSN order, and cursor"
    );
    assert_eq!(
        historical_page_json(&reopened, since),
        authored_occurrence,
        "reopening the artifact still serves the pre-DROP occurrence"
    );
    assert_eq!(
        live_body(&reopened, 1),
        vec![vec![Value::Text("fresh incarnation".to_string())]],
        "the live key on the artifact is the fresh life"
    );
    assert_eq!(
        live_body(&reopened, 2),
        vec![vec![Value::Text("later independent row".to_string())]],
        "the unrelated neighbor survives the artifact reopen"
    );
}
