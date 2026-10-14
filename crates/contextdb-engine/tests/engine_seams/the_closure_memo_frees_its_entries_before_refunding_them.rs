//! A durable custody read's closure walk keeps loaded companions in a memo
//! whose keys can be kilobytes long (here: the roots of a table with an
//! 4 KiB name, reached from their units' delivered terminals). On every exit, success or a refusal
//! late in the walk with the memo populated, each entry is freed before its
//! key bytes are refunded: at every refund, what this read has allocated
//! never exceeds what its reader memory still holds admitted.
//!
//! Runs in a fresh child process, where the counted allocator sees only this
//! test's work.
use crate::custody_fixture::*;
use crate::custody_long_key_fixture::*;
use crate::writer_reopen_and_doubling_keep_a_fixed_resident_envelope::live_allocations_for_test;
use contextdb_core::Error;
use contextdb_engine::Database;
use contextdb_server::InProcessBroker;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

const MODE: &str = "CONTEXTDB_CLOSURE_MEMO_ORDER";
const UNITS: usize = 40;
const AMPLE: usize = 256 * 1024 * 1024;
/// Allocations outside the reader's admission (allocator rounding, the redb
/// read view): far below the memo's long keys this guards.
const SLACK: isize = 32 * 1024;
const RELEASED: u8 = 3;
const DESTROY: u8 = 4;

static BASE: AtomicUsize = AtomicUsize::new(0);
static EXCESS: AtomicIsize = AtomicIsize::new(isize::MIN);
static RELEASES: AtomicUsize = AtomicUsize::new(0);
/// Live allocations when the memo's destruction began, and how far they had
/// fallen when its key bytes were refunded.
static MEMO_DESTROY_LIVE: AtomicUsize = AtomicUsize::new(0);
static FREED_BEFORE_KEY_REFUND: AtomicIsize = AtomicIsize::new(isize::MIN);
static COUNTS: Mutex<Option<BTreeMap<&'static str, usize>>> = Mutex::new(None);

fn probe(label: &'static str, phase: u8, _invoice: usize, _held: usize) {
    if phase == 0
        && let Ok(mut counts) = COUNTS.try_lock()
        && let Some(counts) = counts.as_mut()
    {
        *counts.entry(label).or_default() += 1;
    }
    if phase == DESTROY && label == "custody_closure_memo_destroy" {
        MEMO_DESTROY_LIVE.store(live_allocations_for_test(), Ordering::SeqCst);
        return;
    }
    if phase == RELEASED && label == "custody_closure_memo_keys" {
        let freed = MEMO_DESTROY_LIVE.load(Ordering::SeqCst) as isize
            - live_allocations_for_test() as isize;
        FREED_BEFORE_KEY_REFUND.store(freed, Ordering::SeqCst);
        return;
    }
    if phase != RELEASED {
        return;
    }
    let Some(held) = Database::__custody_measured_held_for_test() else {
        return;
    };
    let unadmitted =
        live_allocations_for_test() as isize - BASE.load(Ordering::SeqCst) as isize - held as isize;
    EXCESS.fetch_max(unadmitted, Ordering::SeqCst);
    RELEASES.fetch_add(1, Ordering::SeqCst);
}

/// Run one terminal selection observed at every refund: its answer, the
/// largest unadmitted excess at a refund, and how many refunds were seen.
/// A row of a local table is committed first, so the read's root is new and
/// no closure check certified at an earlier root is skipped, while the custody
/// journal is unchanged: every read walks the same whole closure.
fn observed(edge: &Database) -> (Result<Vec<String>, Error>, usize, isize, usize) {
    static NEXT: AtomicUsize = AtomicUsize::new(1);
    edge.execute(
        "INSERT INTO local_notes (id, body) VALUES ($id, 'moves the root')",
        &std::collections::HashMap::from([(
            "id".into(),
            contextdb_core::Value::Int64(NEXT.fetch_add(1, Ordering::SeqCst) as i64),
        )]),
    )
    .unwrap();
    EXCESS.store(isize::MIN, Ordering::SeqCst);
    RELEASES.store(0, Ordering::SeqCst);
    FREED_BEFORE_KEY_REFUND.store(isize::MIN, Ordering::SeqCst);
    BASE.store(live_allocations_for_test(), Ordering::SeqCst);
    Database::__set_custody_bracket_probe_for_test(Some(probe));
    let (answer, held) =
        edge.__custody_caller_with_limit_for_test("select", "delivery_edge_outcome.v1.", AMPLE);
    Database::__set_custody_bracket_probe_for_test(None);
    (
        answer,
        held,
        EXCESS.load(Ordering::SeqCst),
        RELEASES.load(Ordering::SeqCst),
    )
}

async fn memo_order() {
    let broker = InProcessBroker::new();
    let hub = start_long_hub(&broker).await;
    let dir = tempfile::tempdir().unwrap();
    let (edge, client) = bound_long_edge(&broker, dir.path()).await;
    for unit in 0..UNITS {
        commit_long_unit(&edge, unit);
    }
    // Delivered: each terminal's closure reaches its unit's long-keyed root.
    client.push().await.unwrap();
    edge.execute(
        "CREATE TABLE local_notes (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF",
        &std::collections::HashMap::new(),
    )
    .unwrap();
    // Warm the pages and signature checks, counting the walk's view admissions.
    *COUNTS.lock().unwrap() = Some(BTreeMap::new());
    let (warm, held, _, _) = observed(&edge);
    let counts = COUNTS.lock().unwrap().take().unwrap();
    let terminals = warm.unwrap();
    assert_eq!(terminals.len(), UNITS, "one terminal per unit");
    assert_eq!(held, 0);
    let views = counts["custody_closure_view"];
    assert!(
        views > UNITS,
        "the walk validates the answer and its companions"
    );

    let (answer, held, excess, releases) = observed(&edge);
    assert_eq!(answer.unwrap(), terminals);
    assert_eq!(held, 0);
    assert!(releases > 0);
    assert!(
        excess <= SLACK,
        "an ample read refunded {excess} bytes still allocated"
    );

    // Refused at its last companion view: the memo is populated with long
    // root keys when the walk unwinds.
    for occurrence in [views - 1, views] {
        Database::__custody_refuse_at_for_test(Some(("custody_closure_view", occurrence)));
        let (answer, held, excess, releases) = observed(&edge);
        assert_eq!(
            Database::__custody_refuse_at_for_test(None),
            None,
            "view #{occurrence} was reached"
        );
        assert!(
            answer
                .as_ref()
                .is_err_and(|error| matches!(error, Error::MemoryBudgetExceeded { .. })),
            "view #{occurrence}: the read refuses typed: {answer:?}"
        );
        assert_eq!(held, 0, "view #{occurrence}: nothing stays admitted");
        // The memo the unwind destroyed held the long keys this guards.
        let (entries, key_bytes) = Database::__last_closure_memo_for_test();
        // The memo's entries, its long keys among them, were freed before
        // their key bytes were refunded.
        let freed = FREED_BEFORE_KEY_REFUND.load(Ordering::SeqCst);
        assert!(
            freed >= key_bytes as isize,
            "view #{occurrence}: only {freed} bytes were freed before the memo's \
             {key_bytes} key bytes were refunded"
        );
        assert!(
            // Beyond the slack and the operation's admitted-but-unused
            // error reserve (about 32 KiB), which the reader's held bytes
            // include, by a clear margin.
            key_bytes > SLACK as usize + 48 * 1024,
            "view #{occurrence}: the memo held {entries} entries with only {key_bytes} key bytes"
        );
        assert!(releases > 0);
        assert!(
            excess <= SLACK,
            "view #{occurrence}: at a refund during the unwind, {excess} bytes were still \
             allocated beyond what the reader held admitted"
        );
        println!(
            "S3_MEMO_ORDER view={occurrence}/{views} excess={excess} releases={releases} \
             memo_entries={entries} memo_key_bytes={key_bytes} freed_before_refund={}",
            FREED_BEFORE_KEY_REFUND.load(Ordering::SeqCst)
        );
    }
    drop(client);
    drop(edge);
    hub.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn the_closure_memo_frees_its_entries_before_refunding_them() {
    if std::env::var(MODE).is_ok() {
        memo_order().await;
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &crate::child_test_filter(
                module_path!(),
                "the_closure_memo_frees_its_entries_before_refunding_them",
            ),
            "--nocapture",
            "--test-threads=1",
        ])
        .env(MODE, "1")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines().filter(|line| line.contains("S3_MEMO_ORDER")) {
        eprintln!("{line}");
    }
    assert!(
        output.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
