//! Durable custody decoding is charged as it allocates, on commit validation
//! and on the transient probes reads use, so a record whose decoded size far
//! exceeds its encoding (a natural key holding a large JSON array) refuses
//! under a limit between its encoded and its decoded size before the decoded
//! value exists, and holds nothing afterwards. A malformed copy of it refuses
//! with the budget refusal or the established codec diagnostic, never by
//! decoding a second, uncharged time. Each arm runs in a fresh child process,
//! where the counted allocator's peak sees only this thread's work.
use crate::custody_fixture::*;
use crate::writer_reopen_and_doubling_keep_a_fixed_resident_envelope::{
    allocation_peak_for_test, live_allocations_for_test, reset_allocation_peak_for_test,
};
use contextdb_core::Error;
use contextdb_engine::Database;
use contextdb_server::InProcessBroker;
use std::sync::Arc;

const MODE: &str = "CONTEXTDB_CUSTODY_DECODE_MODE";
const ELEMENTS: usize = 200_000;
/// Between the inflated record's encoding (about 0.4 MB) and its decoded
/// JSON tree (more than 6 MB).
const LIMIT: usize = 2 * 1024 * 1024;
const AMPLE: usize = 256 * 1024 * 1024;
/// Allocations no custody owner admits: allocator rounding, the redb view.
const SLACK: usize = 256 * 1024;

fn budget(error: &Error) -> bool {
    matches!(error, Error::MemoryBudgetExceeded { .. })
}
fn corruption(error: &Error) -> bool {
    error
        .to_string()
        .contains("invalid canonical delivery metadata")
}
/// `(answer, how far this call's allocations peaked above where they began)`.
fn peaked<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    let base = live_allocations_for_test();
    reset_allocation_peak_for_test();
    let answer = operation();
    (answer, allocation_peak_for_test().saturating_sub(base))
}

async fn arms() {
    let dir = tempfile::tempdir().unwrap();
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, Arc::new(Database::open_memory())).await;
    let (edge, client) = bound_edge(&broker, dir.path(), None).await;
    commit_unit(&edge, 0);
    client.push().await.unwrap();
    let terminal = edge
        .__custody_keys_for_test(None, "delivery_edge_outcome.v1.")
        .unwrap()[0]
        .clone();
    let healthy = edge
        .__custody_record_bytes_for_test(&terminal)
        .unwrap()
        .unwrap();
    let inflated = edge
        .__custody_inflated_terminal_for_test(&terminal, ELEMENTS)
        .unwrap();
    assert!(inflated.len() < LIMIT / 2, "encoded {}", inflated.len());

    // Commit validation of the inflated write.
    let writes = vec![(terminal.clone(), inflated.clone())];
    let ((refused, held), peak) =
        peaked(|| edge.__validate_custody_writes_for_test(&writes, &[], LIMIT));
    assert!(refused.as_ref().is_err_and(budget), "{refused:?}");
    assert_eq!(held, 0);
    assert!(
        peak <= LIMIT + SLACK,
        "validation refused before the decoded value existed (peak {peak})"
    );
    let (completed, held) = edge.__validate_custody_writes_for_test(&writes, &[], AMPLE);
    assert!(
        completed.as_ref().is_err_and(corruption),
        "decoded whole, its altered signature refuses: {completed:?}"
    );
    assert_eq!(held, 0, "nothing stays admitted after a completed decode");
    let healthy_writes = vec![(terminal.clone(), healthy.clone())];
    let (validated, held) = edge.__validate_custody_writes_for_test(&healthy_writes, &[], AMPLE);
    validated.unwrap();
    assert_eq!(
        held, 0,
        "nothing stays admitted after a validated delta drops"
    );

    // The transient probe reads use.
    let ((refused, held), peak) =
        peaked(|| Database::__custody_probe_for_test(&terminal, &inflated, LIMIT));
    assert!(refused.as_ref().is_err_and(budget), "{refused:?}");
    assert_eq!(held, 0);
    assert!(
        peak <= LIMIT + SLACK,
        "the probe refused before the decoded value existed (peak {peak})"
    );
    let (completed, held) = Database::__custody_probe_for_test(&terminal, &inflated, AMPLE);
    assert!(completed.as_ref().is_err_and(corruption), "{completed:?}");
    assert_eq!(held, 0);
    let (probed, held) = Database::__custody_probe_for_test(&terminal, &healthy, AMPLE);
    probed.unwrap();
    assert_eq!(held, 0);

    // A malformed copy: refused within the limit, by budget or codec.
    let mut malformed = inflated.clone();
    malformed.truncate(inflated.len() - 1);
    let ((refused, held), peak) =
        peaked(|| Database::__custody_probe_for_test(&terminal, &malformed, LIMIT));
    assert!(
        refused.as_ref().is_err_and(
            |error| budget(error) || error.to_string().contains("delivery metadata decode")
        ),
        "{refused:?}"
    );
    assert_eq!(held, 0);
    assert!(
        peak <= LIMIT + SLACK,
        "no uncharged second decode (peak {peak})"
    );
    println!(
        "S3_DECODE encoded={} limit={LIMIT} peak={peak}",
        inflated.len()
    );
    drop(client);
    hub.stop().await;
}

#[tokio::test(flavor = "current_thread")]
async fn custody_decoding_charges_what_it_allocates() {
    if std::env::var(MODE).is_ok() {
        arms().await;
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &crate::child_test_filter(module_path!(), "custody_decoding_charges_what_it_allocates"),
            "--nocapture",
            "--test-threads=1",
        ])
        .env(MODE, "arms")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines().filter(|line| line.starts_with("S3_DECODE")) {
        println!("{line}");
    }
    assert!(
        output.status.success(),
        "arms failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
