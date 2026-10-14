//! Every labelled custody admission is bracketed and refusable. Each
//! production custody caller (selection, authority, submission, table, root,
//! rows, pending), commit validation and root retirement runs once with ample
//! memory while every admission bracket is recorded. Then:
//! - each bracket that funds an allocation saw no more allocated, between its
//!   reservation and the allocation's completion, than its own invoice plus the
//!   invoices of brackets nested inside it;
//! - each labelled admission is refused deterministically at its first, middle
//!   and last occurrence: the refusal is typed, the labelled occurrence was
//!   reached, nothing stays admitted after the call returns, and the next
//!   ample call answers exactly as before.
//!
//! Each mode runs in a fresh child process, where the counted allocator sees
//! only this test's work.
use crate::custody_fixture::*;
use crate::writer_reopen_and_doubling_keep_a_fixed_resident_envelope::{
    allocation_peak_for_test, live_allocations_for_test, reset_allocation_peak_for_test,
};
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use contextdb_engine::database::DeliveryManifest;
use contextdb_server::InProcessBroker;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::Mutex;

const MODE: &str = "CONTEXTDB_CUSTODY_BRACKET_MODE";
const AMPLE: usize = 256 * 1024 * 1024;
/// Allocator rounding a funded allocation may show beyond its invoice.
const BRACKET_SLACK: usize = 256;

const PRE: u8 = 0;
const POST: u8 = 1;
const REFUSED: u8 = 2;

/// One open bracket: its label, invoice, the live bytes at its pre-probe, the
/// peak seen inside it before a nested bracket reset the allocator's peak,
/// and the invoices of the brackets completed inside it.
struct Frame {
    label: &'static str,
    invoice: usize,
    live: usize,
    peak: usize,
    nested: usize,
}
#[derive(Default)]
struct Record {
    open: Vec<Frame>,
    /// Admissions per label, in order of occurrence.
    counts: BTreeMap<&'static str, usize>,
    /// Funded brackets whose allocation exceeded their invoice.
    excess: Vec<String>,
    /// Funded brackets checked.
    checked: usize,
    refusals: Vec<&'static str>,
}
static RECORD: Mutex<Option<Record>> = Mutex::new(None);

fn probe(label: &'static str, phase: u8, invoice: usize, _held: usize) {
    let peak = allocation_peak_for_test();
    let mut guard = RECORD.lock().unwrap();
    let Some(record) = guard.as_mut() else {
        return;
    };
    match phase {
        PRE => {
            for frame in &mut record.open {
                frame.peak = frame.peak.max(peak);
            }
            *record.counts.entry(label).or_default() += 1;
            let live = live_allocations_for_test();
            record.open.push(Frame {
                label,
                invoice,
                live,
                peak: live,
                nested: 0,
            });
            drop(guard);
            reset_allocation_peak_for_test();
        }
        POST | REFUSED => {
            // A bracket an invariant error left open is discarded.
            let Some(at) = record.open.iter().rposition(|frame| frame.label == label) else {
                return;
            };
            record.open.truncate(at + 1);
            let frame = record.open.pop().unwrap();
            let peak = frame.peak.max(peak);
            let grown = peak.saturating_sub(frame.live);
            let invoice = frame.invoice.max(invoice);
            if phase == REFUSED {
                record.refusals.push(label);
                if grown > BRACKET_SLACK {
                    record
                        .excess
                        .push(format!("{label}: refused after {grown} bytes allocated"));
                }
            } else if invoice > 0 {
                // A bracket whose invoice is zero (charged decoding, native
                // checks) is funded by the charges it makes as it allocates;
                // its own witnesses are the decoding tests.
                record.checked += 1;
                if grown > invoice + frame.nested + BRACKET_SLACK {
                    record.excess.push(format!(
                        "{label}: {grown} bytes allocated against invoice {invoice} \
                         (+{} nested)",
                        frame.nested
                    ));
                }
                if let Some(parent) = record.open.last_mut() {
                    parent.nested += invoice + frame.nested;
                    parent.peak = parent.peak.max(peak);
                }
            }
        }
        _ => {}
    }
}

/// Run `operation` with brackets recorded.
fn recorded<T>(operation: impl FnOnce() -> T) -> (T, Record) {
    *RECORD.lock().unwrap() = Some(Record::default());
    Database::__set_custody_bracket_probe_for_test(Some(probe));
    let answer = operation();
    Database::__set_custody_bracket_probe_for_test(None);
    let record = RECORD.lock().unwrap().take().unwrap();
    (answer, record)
}

fn budget(error: &Error) -> bool {
    matches!(error, Error::MemoryBudgetExceeded { .. })
}

/// The occurrences of a label each control refuses at.
fn occurrences(count: usize) -> Vec<usize> {
    let mut at = vec![1, count.div_ceil(2), count];
    at.dedup();
    at
}

/// One operation: drives the call, answers its result (keys, or nothing for
/// a write) and the bytes its owner still holds once it returned.
type Operation<'a> = Box<dyn FnMut() -> (Result<Vec<String>, Error>, usize) + 'a>;

/// Record `operation` amply, check its funded brackets, then refuse each
/// labelled admission at its first, middle and last occurrence. `prepare`
/// runs, unrecorded, before every call (a write stages its next unit there).
fn bracketed(name: &str, prepare: &mut dyn FnMut(), mut operation: Operation<'_>) {
    let mut call = |recording: bool| {
        prepare();
        if recording {
            recorded(&mut operation)
        } else {
            (operation(), Record::default())
        }
    };
    // Warm the pages the operation reads.
    let ((warm, held), _) = call(false);
    let expected = warm.unwrap_or_else(|error| panic!("{name}: the ample call answers: {error}"));
    assert_eq!(held, 0, "{name}: the ample call holds nothing afterwards");
    let ((answer, held), record) = call(true);
    assert_eq!(
        answer.unwrap(),
        expected,
        "{name}: the recorded call answers alike"
    );
    assert_eq!(held, 0);
    assert!(
        record.excess.is_empty(),
        "{name}: funded allocations exceeded their invoices:\n{}",
        record.excess.join("\n")
    );
    assert!(
        record.checked > 0,
        "{name}: some funded bracket was checked"
    );
    let mut refused = 0usize;
    for (label, count) in &record.counts {
        for occurrence in occurrences(*count) {
            Database::__custody_refuse_at_for_test(Some((label, occurrence)));
            let ((answer, held), refusal) = call(true);
            let left = Database::__custody_refuse_at_for_test(None);
            assert_eq!(
                left, None,
                "{name}: occurrence {occurrence} of {label} was reached"
            );
            let error = match answer {
                Err(error) => error,
                Ok(keys) => panic!("{name}: refusing {label}#{occurrence} answered {keys:?}"),
            };
            assert!(
                budget(&error) && error.to_string().contains(label),
                "{name}: refusing {label}#{occurrence} is the typed refusal: {error}"
            );
            assert_eq!(
                held, 0,
                "{name}: nothing stays admitted after {label}#{occurrence} refused"
            );
            assert!(
                refusal.excess.is_empty(),
                "{name}: before {label}#{occurrence} refused:\n{}",
                refusal.excess.join("\n")
            );
            let ((retry, held), _) = call(false);
            assert_eq!(held, 0);
            assert_eq!(
                retry.unwrap_or_else(|error| panic!(
                    "{name}: the retry after {label}#{occurrence} refused: {error}"
                )),
                expected,
                "{name}: the retry after {label}#{occurrence} answers exactly"
            );
            refused += 1;
        }
    }
    println!(
        "S3_BRACKET {name} labels={} funded_checked={} refusals={refused}",
        record.counts.len(),
        record.checked
    );
}

/// Four delivered units and two more still pending.
async fn delivered_edge(
    dir: &std::path::Path,
) -> (Arc<Database>, Hub, contextdb_server::SyncClient) {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, Arc::new(Database::open_memory())).await;
    let (edge, client) = bound_edge(&broker, dir, None).await;
    for unit in 0..4 {
        commit_unit(&edge, unit);
    }
    client.push().await.unwrap();
    for unit in 4..6 {
        commit_unit(&edge, unit);
    }
    (edge, hub, client)
}

async fn reads() {
    let dir = tempfile::tempdir().unwrap();
    let (edge, _hub, _client) = delivered_edge(dir.path()).await;
    let terminal = edge
        .__custody_keys_for_test(None, "delivery_edge_outcome.v1.")
        .unwrap()[0]
        .clone();
    let manifest = edge
        .__custody_keys_for_test(None, "delivery_manifest.v1.")
        .unwrap()[0]
        .clone();
    let submission = uuid::Uuid::parse_str(manifest.rsplit('.').next().unwrap())
        .unwrap()
        .to_string();
    let callers: [(&str, &str); 9] = [
        ("select", "delivery_root.v1."),
        ("select", terminal.as_str()),
        ("select", "@authority"),
        ("authority", ""),
        ("submission", submission.as_str()),
        ("table", "records"),
        ("root", manifest.as_str()),
        ("rows", manifest.as_str()),
        ("pending", ""),
    ];
    for (caller, arg) in callers {
        let edge = edge.clone();
        bracketed(
            &format!("{caller}({})", arg.get(..24).unwrap_or(arg)),
            &mut || {},
            Box::new(move || edge.__custody_caller_with_limit_for_test(caller, arg, AMPLE)),
        );
    }
}

/// Stage one unit: a root, a member and its registration.
fn stage_unit(edge: &Database, unit: usize) -> contextdb_core::TxId {
    let tx = edge.begin().unwrap();
    edge.insert_row(
        tx,
        "records",
        HashMap::from([
            ("id".into(), Value::Uuid(root_id(unit))),
            ("body".into(), Value::Text(format!("unit {unit}"))),
        ]),
    )
    .unwrap();
    edge.insert_row(
        tx,
        "parts",
        HashMap::from([
            ("id".into(), Value::Uuid(member_id(unit))),
            ("record_id".into(), Value::Uuid(root_id(unit))),
            ("body".into(), Value::Text(format!("member {unit}"))),
        ]),
    )
    .unwrap();
    edge.register_delivery_manifest(
        tx,
        DeliveryManifest {
            root_table: "records",
            root_key: key(root_id(unit)),
            members: vec![("parts", key(member_id(unit)))],
        },
    )
    .unwrap();
    tx
}

/// Commit validation: every commit stages a fresh unit; a refused commit
/// leaves validation holding nothing, and the next commit succeeds.
async fn commit() {
    let dir = tempfile::tempdir().unwrap();
    let (edge, _hub, _client) = delivered_edge(dir.path()).await;
    let staged = std::cell::Cell::new(None);
    let mut unit = 100usize;
    let mut prepare = || {
        staged.set(Some(stage_unit(&edge, unit)));
        unit += 1;
    };
    let operation = || {
        let committed = edge.commit(staged.take().unwrap());
        let (held, _) = Database::__last_custody_validation_for_test();
        (committed.map(|()| Vec::new()), held.unwrap_or(0))
    };
    bracketed("commit", &mut prepare, Box::new(operation));
}

/// Retirement: an UPDATE of a registered root retires its ownership; every
/// retirement updates a fresh registered root. What the writer accountant
/// holds after the statement is compared with before it.
async fn retire() {
    let dir = tempfile::tempdir().unwrap();
    let (edge, _hub, _client) = delivered_edge(dir.path()).await;
    for unit in 200..400 {
        commit_unit(&edge, unit);
    }
    let unit = std::cell::Cell::new(200usize);
    let operation = || {
        let at = unit.get();
        unit.set(at + 1);
        assert!(at < 400, "enough registered roots");
        let before = edge.accountant().usage().used;
        let updated = edge.execute(
            "UPDATE records SET body = $body WHERE id = $id",
            &HashMap::from([
                ("id".into(), Value::Uuid(root_id(at))),
                ("body".into(), Value::Text(format!("retired {at}"))),
            ]),
        );
        let after = edge.accountant().usage().used;
        (updated.map(|_| Vec::new()), after.saturating_sub(before))
    };
    bracketed("retire", &mut || {}, Box::new(operation));
}

#[tokio::test(flavor = "current_thread")]
async fn custody_admissions_are_bracketed_and_refusable() {
    if let Ok(mode) = std::env::var(MODE) {
        match mode.as_str() {
            "reads" => reads().await,
            "commit" => commit().await,
            "retire" => retire().await,
            other => panic!("unknown mode {other}"),
        }
        return;
    }
    for mode in ["reads", "commit", "retire"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &crate::child_test_filter(
                    module_path!(),
                    "custody_admissions_are_bracketed_and_refusable",
                ),
                "--nocapture",
                "--test-threads=1",
            ])
            .env(MODE, mode)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines().filter(|line| line.contains("S3_BRACKET")) {
            eprintln!("{line}");
        }
        assert!(
            output.status.success(),
            "mode {mode} failed:\n{stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
