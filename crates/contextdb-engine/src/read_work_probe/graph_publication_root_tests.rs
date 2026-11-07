//! Observer root ownership is independent of allocation-count qualification.
//! These rows use real native transactions and the existing event recorder.
//! The only injected behavior is a one-shot panic AFTER a real recorded event.

use super::*;
use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};

const TABLES: [&str; 7] = [
    "graph_fwd",
    "graph_rev",
    "graph_contributions_v1",
    "graph_occurrence_locations_v1",
    "graph_predecessor_lifecycle_v1",
    "change_log",
    "writer_change_log",
];
const OLD: &str = "caller-owned graph context with deliberately spare capacity";
const OPERATION: &str = "graph publication root ownership";
const INJECTED: &str = "graph observer injected recorder panic";
const BODY_PANIC: &str = "graph publication body panic";

type Allocation = (usize, usize, usize);
type NativeRoot = Result<Option<[u8; 32]>, redb::TableError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    Prebinding(usize),
    FailedBinding,
    Begin,
    Committed,
    Aborted,
}

thread_local! {
    // Fixed-size, thread-local, one-shot injection. No history or heap buffer.
    static RECORDER_FAULT: Cell<Option<Fault>> = const { Cell::new(None) };
}

/// Called at the real recorder's exit, after its ACTIVE and event-lock borrows
/// have ended. Never replaces a binding, changes a root, or synthesizes events.
pub(super) fn after_graph_event(family: &str, operation: &str, key: &[u8]) {
    RECORDER_FAULT.with(|slot| {
        let matches = match slot.get() {
            Some(Fault::Prebinding(index)) => {
                family == "root_binding" && operation == "root" && key == TABLES[index].as_bytes()
            }
            Some(Fault::FailedBinding) => family == "root_binding" && operation == "UNOBSERVED",
            Some(Fault::Begin) => family == "publication" && operation == "begin",
            Some(Fault::Committed) => family == "publication" && operation == "committed",
            Some(Fault::Aborted) => family == "publication" && operation == "aborted",
            None => false,
        };
        if matches {
            slot.set(None);
            panic_any(INJECTED);
        }
    });
}

struct RecorderFault;
impl RecorderFault {
    fn arm(fault: Fault) -> Self {
        RECORDER_FAULT.with(|slot| assert_eq!(slot.replace(Some(fault)), None));
        Self
    }
}
impl Drop for RecorderFault {
    fn drop(&mut self) {
        RECORDER_FAULT.with(|slot| slot.set(None));
    }
}

struct Fixture {
    // Database drops before its temporary directory, including on test unwind.
    database: redb::Database,
    _directory: tempfile::TempDir,
}
impl Fixture {
    fn new(seed: u8, valid: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let database = redb::Database::create(directory.path().join("graph-context.redb")).unwrap();
        let write = database.begin_write().unwrap();
        for name in TABLES {
            if name == "change_log" || (name == "graph_fwd" && !valid) {
                // A wrong *real native key type* makes only graph_fwd fail.
                // No fabricated fingerprint or altered production binder.
                write
                    .open_table(redb::TableDefinition::<&str, &[u8]>::new(name))
                    .unwrap()
                    .insert("fixture-key", [seed; 16].as_slice())
                    .unwrap();
            } else {
                write
                    .open_table(redb::TableDefinition::<&[u8], &[u8]>::new(name))
                    .unwrap()
                    .insert(b"fixture-key".as_slice(), [seed; 16].as_slice())
                    .unwrap();
            }
        }
        write.commit().unwrap();
        Self {
            database,
            _directory: directory,
        }
    }
}

struct ExpectedBinding {
    acquired: Vec<String>,
    installed: Option<String>,
}
impl ExpectedBinding {
    fn from_native(write: &redb::WriteTransaction, valid: bool) -> Self {
        // Independent low-level oracle: neither binding helper nor attempt is
        // called to obtain expectations. Include all seven descriptors in order.
        let roots: Vec<(&str, NativeRoot)> = vec![
            (
                "graph_fwd",
                write.table_root_fingerprint(redb::TableDefinition::<&[u8], &[u8]>::new(
                    "graph_fwd",
                )),
            ),
            (
                "graph_rev",
                write.table_root_fingerprint(redb::TableDefinition::<&[u8], &[u8]>::new(
                    "graph_rev",
                )),
            ),
            (
                "graph_contributions_v1",
                write.table_root_fingerprint(redb::TableDefinition::<&[u8], &[u8]>::new(
                    "graph_contributions_v1",
                )),
            ),
            (
                "graph_occurrence_locations_v1",
                write.table_root_fingerprint(redb::TableDefinition::<&[u8], &[u8]>::new(
                    "graph_occurrence_locations_v1",
                )),
            ),
            (
                "graph_predecessor_lifecycle_v1",
                write.table_root_fingerprint(redb::TableDefinition::<&[u8], &[u8]>::new(
                    "graph_predecessor_lifecycle_v1",
                )),
            ),
            (
                "change_log",
                write.table_root_fingerprint(redb::TableDefinition::<&str, &[u8]>::new(
                    "change_log",
                )),
            ),
            (
                "writer_change_log",
                write.table_root_fingerprint(redb::TableDefinition::<&[u8], &[u8]>::new(
                    "writer_change_log",
                )),
            ),
        ];
        if valid {
            assert!(roots.iter().all(|(_, root)| matches!(root, Ok(Some(_)))));
        } else {
            assert!(
                roots[0].1.is_err(),
                "real key-type mismatch must refuse fingerprint"
            );
            assert!(
                roots[1..]
                    .iter()
                    .all(|(_, root)| matches!(root, Ok(Some(_))))
            );
        }
        let acquired = roots.iter().map(|(_, root)| format!("{root:?}")).collect();
        let installed = valid.then(|| format!("{roots:?}"));
        Self {
            acquired,
            installed,
        }
    }

    fn prebinding(&self, old: Option<&str>) -> Vec<GraphWorkEvent> {
        TABLES
            .iter()
            .zip(&self.acquired)
            .map(|(name, acquired)| {
                expected_event("root_binding", "root", name.as_bytes(), old, Some(acquired))
            })
            .collect()
    }

    fn binding(&self, old: Option<&str>) -> Vec<GraphWorkEvent> {
        let mut events = self.prebinding(old);
        if self.installed.is_none() {
            events.push(expected_event(
                "root_binding",
                "UNOBSERVED",
                &[],
                None,
                Some("transaction roots unavailable"),
            ));
        }
        events
    }

    fn publication(&self, operation: &str) -> GraphWorkEvent {
        expected_event(
            "publication",
            operation,
            &[],
            self.installed.as_deref(),
            None,
        )
    }
}

fn expected_event(
    family: &str,
    operation: &str,
    key: &[u8],
    root: Option<&str>,
    role: Option<&str>,
) -> GraphWorkEvent {
    GraphWorkEvent {
        phase: ReadWorkPhase::Commit,
        operation_id: Some(OPERATION.into()),
        worker_request_id: None,
        acquired_root: if operation == "root" {
            role.map(str::to_owned)
        } else {
            None
        },
        observation_status: if root.is_some() {
            "OBSERVED"
        } else {
            "UNOBSERVED"
        }
        .into(),
        captured_root: root.map(str::to_owned),
        family: family.into(),
        operation: operation.into(),
        key: key.into(),
        queried_lsn: None,
        role: role.map(str::to_owned),
        funding_owner: None,
        control_reservation_bytes: None,
    }
}

fn observe() -> ReadWorkObservationScope {
    let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Commit);
    scope.set_operation_id(OPERATION);
    scope
}

fn allocation() -> Option<Allocation> {
    GRAPH_ROOT.with(|root| {
        root.borrow()
            .as_ref()
            .map(|s| (s.as_ptr() as usize, s.len(), s.capacity()))
    })
}

fn install_old(some: bool) -> Option<Allocation> {
    let value = some.then(|| {
        let mut value = String::with_capacity(379);
        value.push_str(OLD);
        value
    });
    let identity = value
        .as_ref()
        .map(|s| (s.as_ptr() as usize, s.len(), s.capacity()));
    GRAPH_ROOT.with(|root| {
        root.replace(value);
    });
    identity
}

fn assert_root(text: Option<&str>, identity: Option<Allocation>) {
    GRAPH_ROOT.with(|root| assert_eq!(root.borrow().as_deref(), text));
    assert_eq!(
        allocation(),
        identity,
        "restore must move the very same pointer/len/cap, including None"
    );
}

fn assert_events(scope: &ReadWorkObservationScope, events: &[GraphWorkEvent], commits: u64) {
    assert_eq!(
        scope.graph_events(),
        events,
        "full event fields, status, and order must match"
    );
    assert_eq!(scope.graph_counters().graph_publication_commits, commits);
}

fn assert_injected(result: std::thread::Result<()>) {
    let payload = result.expect_err("recorder panic must propagate");
    assert_eq!(payload.downcast_ref::<&'static str>(), Some(&INJECTED));
    RECORDER_FAULT.with(|slot| assert_eq!(slot.get(), None, "fault must have fired exactly once"));
}

// Acceptance: both old contexts, both binding outcomes, both terminal outcomes.
// Decisive mutants: cloned old String, early take, lost Some(None), wrong root.
#[test]
fn publication_restores_original_allocation_and_none_for_both_outcomes() {
    for valid in [true, false] {
        let fixture = Fixture::new(11, valid);
        let write = fixture.database.begin_write().unwrap();
        let expected = ExpectedBinding::from_native(&write, valid);
        for some in [true, false] {
            for committed in [true, false] {
                let scope = observe();
                let original = install_old(some);
                let old = some.then_some(OLD);
                let mut events = expected.binding(old);
                let mut attempt = GraphPublicationAttempt::begin_in_write(&write);
                events.push(expected.publication("begin"));
                let writer = allocation();
                assert_root(expected.installed.as_deref(), writer);
                // Catch a copied saved allocation BEFORE it can be deallocated
                // and its address recycled on drop.
                let saved = attempt
                    .root
                    .previous_root
                    .as_ref()
                    .expect("handoff is armed");
                assert_eq!(
                    saved
                        .as_ref()
                        .map(|s| (s.as_ptr() as usize, s.len(), s.capacity())),
                    original
                );
                if committed {
                    attempt.committed();
                    events.push(expected.publication("committed"));
                }
                drop(attempt);
                if !committed {
                    events.push(expected.publication("aborted"));
                }
                assert_root(old, original);
                assert_events(&scope, &events, u64::from(committed));
            }
        }
    }
}

// Acceptance: immediate parent ownership, including outer None, at each depth.
// Decisive mutants: restore oldest context, clone parent, restore before abort.
#[test]
fn nested_publications_restore_each_immediate_owner_and_event_sequence() {
    for outer_valid in [true, false] {
        let outer_fixture = Fixture::new(21, outer_valid);
        let outer_write = outer_fixture.database.begin_write().unwrap();
        let outer_expected = ExpectedBinding::from_native(&outer_write, outer_valid);
        for inner_valid in [true, false] {
            let inner_fixture = Fixture::new(22, inner_valid);
            let inner_write = inner_fixture.database.begin_write().unwrap();
            let inner_expected = ExpectedBinding::from_native(&inner_write, inner_valid);
            if outer_valid && inner_valid {
                assert_ne!(
                    outer_expected.installed, inner_expected.installed,
                    "distinct native writers must be distinguishable"
                );
            }
            for some in [true, false] {
                for outer_commit in [true, false] {
                    for inner_commit in [true, false] {
                        let scope = observe();
                        let original = install_old(some);
                        let old = some.then_some(OLD);
                        let mut events = outer_expected.binding(old);
                        let mut outer = GraphPublicationAttempt::begin_in_write(&outer_write);
                        events.push(outer_expected.publication("begin"));
                        let outer_identity = allocation();
                        assert_root(outer_expected.installed.as_deref(), outer_identity);
                        events.extend(inner_expected.binding(outer_expected.installed.as_deref()));
                        let mut inner = GraphPublicationAttempt::begin_in_write(&inner_write);
                        events.push(inner_expected.publication("begin"));
                        assert_root(inner_expected.installed.as_deref(), allocation());
                        if inner_commit {
                            inner.committed();
                            events.push(inner_expected.publication("committed"));
                        }
                        drop(inner);
                        if !inner_commit {
                            events.push(inner_expected.publication("aborted"));
                        }
                        assert_root(outer_expected.installed.as_deref(), outer_identity);
                        if outer_commit {
                            outer.committed();
                            events.push(outer_expected.publication("committed"));
                        }
                        drop(outer);
                        if !outer_commit {
                            events.push(outer_expected.publication("aborted"));
                        }
                        assert_root(old, original);
                        assert_events(
                            &scope,
                            &events,
                            u64::from(outer_commit) + u64::from(inner_commit),
                        );
                    }
                }
            }
        }
    }
}

// Acceptance: unscoped binder keeps its replacement; no attempt restoration.
// Decisive mutant: leaking the scoped restoration semantics into normal binding.
#[test]
fn ordinary_binding_keeps_replacement_and_original_observation_order() {
    for valid in [true, false] {
        let fixture = Fixture::new(31, valid);
        let write = fixture.database.begin_write().unwrap();
        let expected = ExpectedBinding::from_native(&write, valid);
        for some in [true, false] {
            let scope = observe();
            let original = install_old(some);
            let old = some.then_some(OLD);
            let events = expected.binding(old);
            bind_graph_write_root(&write);
            let installed = allocation();
            assert_root(expected.installed.as_deref(), installed);
            if some || valid {
                assert_ne!(
                    installed, original,
                    "normal binding discards its old context"
                );
            } else {
                assert_eq!(installed, None, "failed binding remains unobserved");
            }
            assert_events(&scope, &events, 0);
            note_graph(
                "publication",
                "normal-binder-still-installed",
                &[],
                None,
                None,
            );
            let mut events = events;
            events.push(expected.publication("normal-binder-still-installed"));
            assert_events(&scope, &events, 0);
            assert_root(expected.installed.as_deref(), installed);
        }
    }
}

// Acceptance: every prebinding event, failed-bind event, and begin can unwind;
// old allocation survives and no incomplete constructor records an abort.
// Decisive mutants: early take, guard armed too late, incomplete-attempt Drop.
#[test]
fn constructor_recorder_panics_restore_context_without_fabricated_abort() {
    for valid in [true, false] {
        let fixture = Fixture::new(41, valid);
        let write = fixture.database.begin_write().unwrap();
        let expected = ExpectedBinding::from_native(&write, valid);
        let mut faults: Vec<Fault> = (0..7).map(Fault::Prebinding).collect();
        faults.push(Fault::Begin);
        if !valid {
            faults.push(Fault::FailedBinding);
        }
        for some in [true, false] {
            for fault in &faults {
                let scope = observe();
                let original = install_old(some);
                let old = some.then_some(OLD);
                let _fault = RecorderFault::arm(*fault);
                assert_injected(catch_unwind(AssertUnwindSafe(|| {
                    let _attempt = GraphPublicationAttempt::begin_in_write(&write);
                })));
                assert_root(old, original);
                let events = match fault {
                    Fault::Prebinding(index) => expected.prebinding(old)[..=*index].to_vec(),
                    Fault::FailedBinding => expected.binding(old),
                    Fault::Begin => {
                        let mut events = expected.binding(old);
                        events.push(expected.publication("begin"));
                        events
                    }
                    _ => unreachable!(),
                };
                assert_events(&scope, &events, 0);
            }
        }
    }
}

// Acceptance: replace can refuse its mutable RefCell borrow after all seven
// observations; the unarmed guard must not erase old Some OR fabricate None.
// Decisive mutant: eagerly taking the old root before fingerprint acquisition.
#[test]
fn refused_handoff_borrow_leaves_original_context_and_no_publication() {
    for valid in [true, false] {
        let fixture = Fixture::new(51, valid);
        let write = fixture.database.begin_write().unwrap();
        let expected = ExpectedBinding::from_native(&write, valid);
        for some in [true, false] {
            let scope = observe();
            let original = install_old(some);
            let old = some.then_some(OLD);
            GRAPH_ROOT.with(|root| {
                let borrowed = root.borrow();
                let panic = catch_unwind(AssertUnwindSafe(|| {
                    let _attempt = GraphPublicationAttempt::begin_in_write(&write);
                }))
                .expect_err("replacement must refuse while an immutable borrow is held");
                let message = panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .expect("RefCell borrow panic has a textual payload");
                assert!(message.contains("borrow"), "unexpected panic: {message}");
                assert_eq!(borrowed.as_deref(), old);
            });
            assert_root(old, original);
            assert_events(&scope, &expected.prebinding(old), 0);
        }
    }
}

// Acceptance: completed abort records writer context before restoration even
// when that recorder panics; normal body unwind records one abort. Committed
// recorder panic propagates with the committed state, without a false abort.
// Decisive mutant: restore only after note_graph returns, or restore too early.
#[test]
fn completed_attempt_unwinds_restore_allocation_and_preserve_outcome_events() {
    for valid in [true, false] {
        let fixture = Fixture::new(61, valid);
        let write = fixture.database.begin_write().unwrap();
        let expected = ExpectedBinding::from_native(&write, valid);
        for some in [true, false] {
            for fault in [Some(Fault::Aborted), Some(Fault::Committed), None] {
                let scope = observe();
                let original = install_old(some);
                let old = some.then_some(OLD);
                let mut events = expected.binding(old);
                let mut attempt = GraphPublicationAttempt::begin_in_write(&write);
                events.push(expected.publication("begin"));
                let result: Option<std::thread::Result<()>> = match fault {
                    Some(Fault::Aborted) => {
                        let _fault = RecorderFault::arm(Fault::Aborted);
                        let result = catch_unwind(AssertUnwindSafe(|| drop(attempt)));
                        assert_injected(result);
                        None
                    }
                    Some(Fault::Committed) => {
                        let _fault = RecorderFault::arm(Fault::Committed);
                        assert_injected(catch_unwind(AssertUnwindSafe(|| attempt.committed())));
                        // The completed attempt still owns writer context until
                        // it drops, despite committed() having unwound.
                        assert_root(expected.installed.as_deref(), allocation());
                        drop(attempt);
                        None
                    }
                    None => Some(catch_unwind(AssertUnwindSafe(move || {
                        let _attempt = attempt;
                        panic_any(BODY_PANIC);
                    }))),
                    _ => unreachable!(),
                };
                if let Some(result) = result {
                    let payload = result.expect_err("body panic must propagate");
                    assert_eq!(payload.downcast_ref::<&'static str>(), Some(&BODY_PANIC));
                }
                let committed = fault == Some(Fault::Committed);
                events.push(expected.publication(if committed { "committed" } else { "aborted" }));
                assert_root(old, original);
                assert_events(&scope, &events, u64::from(committed));
                // A fresh caller context must survive subsequent observation;
                // there can be no pending second restoration after catch_unwind.
                let sentinel = install_old(!some);
                let sentinel_text = (!some).then_some(OLD);
                note_graph("publication", "after-unwind", &[], None, None);
                events.push(expected_event(
                    "publication",
                    "after-unwind",
                    &[],
                    sentinel_text,
                    None,
                ));
                assert_root(sentinel_text, sentinel);
                assert_events(&scope, &events, u64::from(committed));
            }
        }
    }

    // The same failure boundaries must restore the immediate parent, including
    // an actively installed None, before that parent's own abort restores OLD.
    for outer_valid in [true, false] {
        let outer_fixture = Fixture::new(71, outer_valid);
        let outer_write = outer_fixture.database.begin_write().unwrap();
        let outer_expected = ExpectedBinding::from_native(&outer_write, outer_valid);
        for inner_valid in [true, false] {
            let inner_fixture = Fixture::new(72, inner_valid);
            let inner_write = inner_fixture.database.begin_write().unwrap();
            let inner_expected = ExpectedBinding::from_native(&inner_write, inner_valid);
            for some in [true, false] {
                for fault in [Some(Fault::Begin), Some(Fault::Aborted), None] {
                    let scope = observe();
                    let original = install_old(some);
                    let old = some.then_some(OLD);
                    let mut events = outer_expected.binding(old);
                    let outer = GraphPublicationAttempt::begin_in_write(&outer_write);
                    events.push(outer_expected.publication("begin"));
                    let parent = allocation();
                    events.extend(inner_expected.binding(outer_expected.installed.as_deref()));
                    events.push(inner_expected.publication("begin"));
                    if fault == Some(Fault::Begin) {
                        let _fault = RecorderFault::arm(Fault::Begin);
                        assert_injected(catch_unwind(AssertUnwindSafe(|| {
                            let _inner = GraphPublicationAttempt::begin_in_write(&inner_write);
                        })));
                    } else {
                        let inner = GraphPublicationAttempt::begin_in_write(&inner_write);
                        if fault == Some(Fault::Aborted) {
                            let _fault = RecorderFault::arm(Fault::Aborted);
                            assert_injected(catch_unwind(AssertUnwindSafe(|| drop(inner))));
                        } else {
                            let result: std::thread::Result<()> =
                                catch_unwind(AssertUnwindSafe(move || {
                                    let _inner = inner;
                                    panic_any(BODY_PANIC);
                                }));
                            let payload = result.expect_err("nested body panic must propagate");
                            assert_eq!(payload.downcast_ref::<&'static str>(), Some(&BODY_PANIC));
                        }
                        events.push(inner_expected.publication("aborted"));
                    }
                    assert_root(outer_expected.installed.as_deref(), parent);
                    assert_events(&scope, &events, 0);
                    drop(outer);
                    events.push(outer_expected.publication("aborted"));
                    assert_root(old, original);
                    assert_events(&scope, &events, 0);
                }
            }
        }
    }
}
