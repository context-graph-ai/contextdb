//! Durable custody work admits each buffer to its operation's memory before it
//! allocates it: the per-store companion memo (ordinary and received-unit
//! stores), a group head too long for the stack, the pending read's key list as
//! it grows, and commit validation's companion planning. Each mode runs in a
//! fresh child process, where the counted allocator sees only this thread's
//! work. At every admission point, and at every refusal, what the thread holds
//! allocated beyond its starting point may exceed what its memory has admitted
//! by at most a small fixed slack; a refused operation leaves nothing admitted.
use crate::custody_fixture::*;
use crate::writer_reopen_and_doubling_keep_a_fixed_resident_envelope::live_allocations_for_test;
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use contextdb_engine::database::DeliveryManifest;
use contextdb_server::InProcessBroker;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

const MODE: &str = "CONTEXTDB_CUSTODY_ADMISSION_MODE";
/// Fixed allocations no custody owner admits: allocator rounding, the redb
/// read view and its iterators. Every defect this test guards is larger.
const SLACK: isize = 16 * 1024;

static BASE: AtomicUsize = AtomicUsize::new(0);
static EXCESS: AtomicIsize = AtomicIsize::new(isize::MIN);
static PROBES: AtomicUsize = AtomicUsize::new(0);

fn probe(held: usize) {
    let live = live_allocations_for_test() as isize - BASE.load(Ordering::SeqCst) as isize;
    EXCESS.fetch_max(live - held as isize, Ordering::SeqCst);
    PROBES.fetch_add(1, Ordering::SeqCst);
}
/// Observe one operation: `(its answer, the largest unadmitted excess seen at
/// any admission point, how many points were seen)`.
fn observed<T>(operation: impl FnOnce() -> T) -> (T, isize, usize) {
    EXCESS.store(isize::MIN, Ordering::SeqCst);
    PROBES.store(0, Ordering::SeqCst);
    BASE.store(live_allocations_for_test(), Ordering::SeqCst);
    Database::__set_custody_admission_probe_for_test(Some(probe));
    let answer = operation();
    Database::__set_custody_admission_probe_for_test(None);
    (
        answer,
        EXCESS.load(Ordering::SeqCst),
        PROBES.load(Ordering::SeqCst),
    )
}
fn budget(error: &Error) -> bool {
    matches!(error, Error::MemoryBudgetExceeded { .. })
}

async fn edge_with_units(dir: &std::path::Path, units: usize) -> (Arc<Database>, Hub) {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, Arc::new(Database::open_memory())).await;
    let (edge, client) = bound_edge(&broker, dir, None).await;
    for unit in 0..units {
        commit_unit(&edge, unit);
    }
    drop(client);
    (edge, hub)
}

/// The companion memo of an ordinary and of a received-unit store is admitted
/// before it exists, held while the store lives and released when it drops.
async fn memo() {
    let dir = tempfile::tempdir().unwrap();
    let (edge, _hub) = edge_with_units(dir.path(), 1).await;
    let bytes = Database::__custody_memo_bytes_for_test();
    for pinned in [false, true] {
        let before = live_allocations_for_test();
        let refused = edge.__custody_memo_admission_for_test(bytes - 1, pinned);
        let grown = live_allocations_for_test().saturating_sub(before);
        assert!(
            refused.as_ref().is_err_and(budget),
            "pinned={pinned}: refused below its size: {refused:?}"
        );
        assert!(
            grown < bytes / 2,
            "pinned={pinned}: the refused memo was never allocated ({grown} bytes grew)"
        );
        assert_eq!(
            edge.__custody_memo_admission_for_test(bytes, pinned)
                .unwrap(),
            (bytes, 0),
            "pinned={pinned}: held exactly while the store lived, released after"
        );
    }
    // The handle's own store is charged to its writer accountant.
    edge.__custody_keys_for_test(None, "delivery_root.v1.")
        .unwrap();
    let with_store = edge.accountant().usage().used;
    edge.__drop_custody_store_for_test();
    let without_store = edge.accountant().usage().used;
    assert_eq!(
        with_store - without_store,
        bytes,
        "dropping the store releases exactly its memo"
    );
    println!("S3_ADMIT memo bytes={bytes}");
}

/// A group head longer than the stack head is admitted before allocation.
async fn group_head() {
    let dir = tempfile::tempdir().unwrap();
    let (edge, _hub) = edge_with_units(dir.path(), 1).await;
    let group = format!("root:{}", "ab".repeat(100_000));
    let before = live_allocations_for_test();
    let (refused, held) = edge.__visit_custody_group_for_test(&group, 64 * 1024);
    let grown = live_allocations_for_test().saturating_sub(before);
    assert!(refused.as_ref().is_err_and(budget), "{refused:?}");
    assert_eq!(held, 0, "the refused head holds nothing");
    assert!(
        grown < 64 * 1024,
        "the refused head was never allocated ({grown} bytes grew)"
    );
    let ((visited, held), excess, probes) =
        observed(|| edge.__visit_custody_group_for_test(&group, 1024 * 1024));
    assert_eq!(visited.unwrap(), 0);
    assert_eq!(held, 0);
    assert!(probes > 0);
    assert!(
        excess <= SLACK,
        "the admitted head covers its allocation (excess {excess})"
    );
    println!("S3_ADMIT group_head excess={excess}");
}

/// The pending read admits its key list's growth before it reallocates.
async fn pending_growth() {
    const UNITS: usize = 1024;
    let dir = tempfile::tempdir().unwrap();
    let (edge, _hub) = edge_with_units(dir.path(), UNITS).await;
    // Warm the storage pages the read touches.
    let (warm, examined, held) = edge.__custody_pending_with_limit_for_test(1 << 30);
    assert_eq!(warm.unwrap().len(), UNITS);
    assert_eq!((examined, held), (UNITS as u64, 0));
    let mut refusals_during_visit = 0;
    let mut largest = isize::MIN;
    for step in 1..=64usize {
        let limit = step * 2048;
        let ((answer, examined, held), excess, _) =
            observed(|| edge.__custody_pending_with_limit_for_test(limit));
        match answer {
            Ok(keys) => assert_eq!(keys.len(), UNITS),
            Err(error) => {
                assert!(budget(&error), "{error:?}");
                assert_eq!(held, 0, "a refused pending read holds nothing");
                if examined < UNITS as u64 {
                    refusals_during_visit += 1;
                    largest = largest.max(excess);
                    assert!(
                        excess <= SLACK,
                        "limit {limit}: at the refusal after {examined} entries the key \
                         list held {excess} unadmitted bytes"
                    );
                }
            }
        }
    }
    assert!(
        refusals_during_visit > 0,
        "some limit refuses part way through the pending pages"
    );
    println!("S3_ADMIT pending refusals_during_visit={refusals_during_visit} excess={largest}");
}

/// Commit validation admits its companion planning before allocating it.
async fn companion_planning() {
    const MEMBERS: usize = 400;
    let dir = tempfile::tempdir().unwrap();
    let (edge, _hub) = edge_with_units(dir.path(), 1).await;
    let tx = edge.begin().unwrap();
    let root = root_id(1);
    edge.insert_row(
        tx,
        "records",
        HashMap::from([
            ("id".into(), Value::Uuid(root)),
            ("body".into(), Value::Text("wide unit".into())),
        ]),
    )
    .unwrap();
    let mut members = Vec::new();
    for member in 0..MEMBERS {
        let id = member_id(1_000 + member);
        edge.insert_row(
            tx,
            "parts",
            HashMap::from([
                ("id".into(), Value::Uuid(id)),
                ("record_id".into(), Value::Uuid(root)),
                ("body".into(), Value::Text(format!("member {member}"))),
            ]),
        )
        .unwrap();
        members.push(("parts", key(id)));
    }
    edge.register_delivery_manifest(
        tx,
        DeliveryManifest {
            root_table: "records",
            root_key: key(root),
            members,
        },
    )
    .unwrap();
    let (writes, deletes) = edge.__staged_custody_writes_for_test(tx).unwrap();
    // Find the limit that admits the whole validation, warming the pages.
    let mut enough = 64 * 1024;
    loop {
        let (validated, held) = edge.__validate_custody_writes_for_test(&writes, &deletes, enough);
        assert_eq!(held, 0);
        match validated {
            Ok(()) => break,
            Err(error) => {
                assert!(budget(&error), "{error:?}");
                enough *= 2;
                assert!(enough <= 1 << 30, "validation fits one gibibyte");
            }
        }
    }
    let mut refusals = 0;
    let mut largest = isize::MIN;
    for step in 1..=96usize {
        let limit = enough * step / 97;
        let ((validated, held), excess, _) =
            observed(|| edge.__validate_custody_writes_for_test(&writes, &deletes, limit));
        assert_eq!(
            held, 0,
            "limit {limit}: validation holds nothing afterwards"
        );
        if let Err(error) = validated {
            assert!(budget(&error), "{error:?}");
            refusals += 1;
            largest = largest.max(excess);
            assert!(
                excess <= SLACK,
                "limit {limit}: at the refusal, {excess} bytes were allocated unadmitted"
            );
        }
    }
    assert!(refusals > 0);
    edge.rollback(tx).unwrap();
    println!("S3_ADMIT planning enough={enough} refusals={refusals} excess={largest}");
}

#[tokio::test(flavor = "current_thread")]
async fn custody_reads_admit_every_buffer_before_allocating() {
    if let Ok(mode) = std::env::var(MODE) {
        match mode.as_str() {
            "memo" => memo().await,
            "group_head" => group_head().await,
            "pending_growth" => pending_growth().await,
            "companion_planning" => companion_planning().await,
            other => panic!("unknown mode {other}"),
        }
        return;
    }
    for mode in ["memo", "group_head", "pending_growth", "companion_planning"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &crate::child_test_filter(
                    module_path!(),
                    "custody_reads_admit_every_buffer_before_allocating",
                ),
                "--nocapture",
                "--test-threads=1",
            ])
            .env(MODE, mode)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines().filter(|line| line.starts_with("S3_ADMIT")) {
            println!("{line}");
        }
        assert!(
            output.status.success(),
            "mode {mode} failed:\n{stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
