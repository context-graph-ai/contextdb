//! Test-estate discipline audit (see AGENTS.md "Testing discipline").
//!
//! Ratchet guard: the per-file counts below freeze the EXISTING sleep sites and
//! raw clock reads in the test tree (ledgered debt, being worked down by the
//! test-estate round). A NEW sleep or raw clock read in any test file fails
//! this audit. The fix is never "raise the number": use the
//! `Wallclock::test_clock_guard` mock clock for time-dependent behavior, and
//! counter/state/event waits for synchronization. Lower a count when you remove
//! a site. Raising a count is an explicitly approved exception only.
//!
//! Every baseline entry is `(path, count, reason)`. `reason` names the debt
//! class and the pay-down prescription so a reader never has to re-derive
//! "why is this here" from scratch; an empty reason fails the audit (see
//! `assert_reasons_present`). The sleep needle set was widened to also catch
//! `tokio::time::sleep_until(`, `tokio::time::interval(`, and `park_timeout(`
//! — at the time of the widening this did not push any file over its frozen
//! count, so no counts changed; a future widening that *does* push a file over
//! is the one case where raising that file's count is the approved exception,
//! same as any other ratchet raise.

use std::collections::BTreeMap;

use walkdir::WalkDir;

#[path = "audit_support/mod.rs"]
mod audit_support;
use audit_support::workspace_root;

/// Debt class: a blind or fixed-interval sleep standing in for real
/// synchronization (waiting on another thread/process/task to reach a state).
/// Pay-down: drive the wait from `Wallclock::test_clock_guard` where the
/// waited-on code reads `Wallclock::now()`, or from a counter/atomic/channel
/// state poll with a bounded deadline where it doesn't.
const RSN_SYNC_BARRIER: &str =
    "sleep-as-sync barrier; replace with paused time or bounded state polling";

/// Debt class: the sleep/clock read only shapes a test fixture's timestamp
/// value (e.g. a TTL-expiry row) rather than synchronizing with anything.
/// Pay-down: construct the fixture timestamp via `Wallclock` so it stays
/// mockable, once the surrounding code path accepts injected time.
const RSN_FIXTURE_TIMING: &str = "fixture timing";

/// Every test file allowed to contain `thread::sleep(` / `tokio::time::sleep(`
/// call sites, with the frozen maximum count. Counts, not line numbers — a
/// pure line shift must not trip the audit.
const SLEEP_RATCHET: &[(&str, usize, &str)] = &[
    (
        "crates/contextdb-engine/tests/checkpoint_export_tests.rs",
        6,
        RSN_SYNC_BARRIER,
    ),
    (
        "crates/contextdb-engine/tests/sql_surface_tests.rs",
        3,
        "one site waits out a real 1s TTL before asserting pruning (needs paused time); two \
         sites are cross-process file-based barrier polls between a parent and a spawned child \
         test process",
    ),
    (
        "crates/contextdb-server/tests/iroh_transport_tests.rs",
        2,
        RSN_SYNC_BARRIER,
    ),
    (
        "crates/contextdb-server/tests/media_transfer_reclaim_driver.rs",
        1,
        RSN_SYNC_BARRIER,
    ),
    (
        "crates/contextdb-server/tests/stale_restore_tests.rs",
        1,
        "stale ratchet headroom; this file has no current sleep site, safe to lower to 0 on \
         next touch",
    ),
    (
        "crates/contextdb-server/tests/sync_integration.rs",
        14,
        "stale ratchet headroom; this file has no current sleep site, safe to lower to 0 on \
         next touch",
    ),
    (
        "crates/contextdb-server/tests/work_ledger_tests.rs",
        7,
        "most sites are sleep-as-sync-barrier bounded state polling (replace with bounded state \
         polling on the counter/flag already read); two sites run inside a #[tokio::test(start_paused \
         = true)] runtime and already advance deterministically on virtual time, no wall-clock cost",
    ),
    (
        "tests/acceptance/cli_ux.rs",
        1,
        "stale ratchet headroom; this file has no current sleep site, safe to lower to 0 on \
         next touch",
    ),
    ("tests/acceptance/common.rs", 7, RSN_SYNC_BARRIER),
    (
        "tests/acceptance/db_lock_visibility.rs",
        1,
        RSN_SYNC_BARRIER,
    ),
    ("tests/acceptance/engine_cron.rs", 10, RSN_SYNC_BARRIER),
    ("tests/acceptance/event_bus.rs", 27, RSN_SYNC_BARRIER),
    (
        "tests/acceptance/event_bus_self_join.rs",
        11,
        RSN_SYNC_BARRIER,
    ),
    (
        "tests/acceptance/multi_table_atomic_visibility.rs",
        7,
        RSN_SYNC_BARRIER,
    ),
    ("tests/acceptance/persistence.rs", 1, RSN_SYNC_BARRIER),
    ("tests/acceptance/sync.rs", 2, RSN_SYNC_BARRIER),
    ("tests/acceptance/trigger.rs", 1, RSN_SYNC_BARRIER),
    (
        "tests/acceptance/trigger_concurrency_panic_freedom.rs",
        10,
        RSN_SYNC_BARRIER,
    ),
    (
        "tests/acceptance/trigger_same_db_progress.rs",
        29,
        RSN_SYNC_BARRIER,
    ),
    (
        "tests/acceptance/txid_monotonic_concurrent_commits.rs",
        1,
        RSN_SYNC_BARRIER,
    ),
    (
        "tests/integration/auto_sync_update_push_visibility_tests.rs",
        3,
        "stale ratchet headroom; this file has no current sleep site, safe to lower to 0 on \
         next touch",
    ),
    (
        "tests/integration/gate_a_sync.rs",
        16,
        "stale ratchet headroom; this file has no current sleep site, safe to lower to 0 on \
         next touch",
    ),
    (
        "tests/integration/cross_cutting_acceptance_tests.rs",
        2,
        "stale ratchet headroom; this file has no current sleep site, safe to lower to 0 on \
         next touch",
    ),
    (
        "tests/integration/indexed_scan_filter_tests.rs",
        1,
        RSN_SYNC_BARRIER,
    ),
    ("tests/integration/peak_rss_harness.rs", 1, RSN_SYNC_BARRIER),
    (
        "tests/integration/persistence_tests.rs",
        4,
        RSN_SYNC_BARRIER,
    ),
    ("tests/integration/retention_tests.rs", 3, RSN_SYNC_BARRIER),
    (
        "tests/integration/sync_relay_trigger_tables_tests.rs",
        1,
        "stale ratchet headroom; this file has no current sleep site, safe to lower to 0 on \
         next touch",
    ),
    (
        "tests/integration/sync_server_nonblocking_apply.rs",
        4,
        RSN_SYNC_BARRIER,
    ),
    // Explicitly approved exception: this test drives the REAL engine-owned
    // maintenance thread (not `run_maintenance_cycle()`), which by
    // construction cannot observe `Wallclock::test_clock_guard` -- the mock
    // clock is thread-local to the calling test thread and the background
    // thread is spawned by the engine, outside the test's control (see the
    // module doc). The bounded state-polled wait is the same pattern already
    // ratcheted once for the identical class of test in
    // `crates/contextdb-engine/src/database.rs` (see
    // `SRC_TEST_SLEEP_RATCHET` below).
    (
        "tests/integration/trigger_audit_retention_config_tests.rs",
        1,
        "drives a real background thread outside the mock clock's thread-local reach; bounded \
         real-time state poll is the closest available substitute (see \
         crates/contextdb-engine/src/database.rs below)",
    ),
];

/// Test files allowed to read the raw system clock (`SystemTime::now(` or
/// inline epoch math via `duration_since(UNIX_EPOCH)`). Everything else uses
/// `Wallclock::now()` / the mock-clock seam.
const RAW_CLOCK_RATCHET: &[(&str, usize, &str)] = &[
    // Counts are NEEDLE OCCURRENCES, not sites: a read written as
    // `SystemTime::now().duration_since(std::time::UNIX_EPOCH)` contributes 2.
    // `crates/contextdb-cli/tests/corrupt_store_open_tests.rs` used to carry
    // 2 (a scratch-dir naming site); removed in favor of `tempfile`, which
    // needs no raw clock read at all — lowered to 0 (absent from this list).
    (
        "crates/contextdb-engine/tests/sql_surface_tests.rs",
        4,
        "asserts the SQL NOW()/CURRENT_TIMESTAMP function's output against the real wall clock; \
         needs an actual system-time reference for the comparison, not a sync barrier",
    ),
    (
        "tests/integration/retention_tests.rs",
        2,
        RSN_FIXTURE_TIMING,
    ),
];

/// The bare `set_test_clock`/`reset_test_clock`/`clear_test_clock` calls leak
/// the override on a mid-test panic; only the seam's own contract test may use
/// them, at exactly its current count. Everyone else uses the RAII
/// `test_clock_guard`.
const BARE_CLOCK_RATCHET: &[(&str, usize, &str)] = &[(
    "crates/contextdb-core/tests/txid_newtype_tests.rs",
    3,
    "Wallclock set/reset/clear contract test for the mock-clock seam itself; exempt by design, \
     not general test debt",
)];

/// Unit-test modules living inside `src/` files (everything from the first
/// `#[cfg(test)]`-attributed MODULE to end-of-file — an attribute on a lone
/// item, e.g. a test-only accessor, does not open the region) are audited with
/// the same needles. Production code above the marker is exempt — it may
/// legitimately sleep (e.g. `sleep_with_shutdown`) and read real clocks.
const SRC_TEST_SLEEP_RATCHET: &[(&str, usize, &str)] = &[
    ("crates/contextdb-cli/src/auto_sync.rs", 4, RSN_SYNC_BARRIER),
    (
        "crates/contextdb-engine/src/database.rs",
        1,
        "drives the real engine-owned maintenance thread, which cannot observe the thread-local \
         mock clock; bounded real-time state poll is the closest available substitute",
    ),
];

/// In-src test modules allowed bare `Wallclock::` clock calls: the seam's home
/// module tests the set/reset contract itself.
const SRC_TEST_BARE_CLOCK_RATCHET: &[(&str, usize, &str)] = &[(
    "crates/contextdb-core/src/types.rs",
    1,
    "Wallclock reset contract test for the mock-clock seam itself; exempt by design, not \
     general test debt",
)];

/// In-src test modules allowed raw clock reads. Currently none — a pure guard.
const SRC_TEST_RAW_CLOCK_RATCHET: &[(&str, usize, &str)] = &[];

fn is_test_file(path: &str) -> bool {
    path.ends_with(".rs")
        && !path.contains("/fixtures/")
        // This audit's own pattern tables and messages contain the needle
        // strings as literals — never scan self.
        && path != "crates/contextdb-core/tests/test_estate_audit.rs"
        && (path.starts_with("tests/") || (path.starts_with("crates/") && path.contains("/tests/")))
}

fn count_occurrences(haystack: &str, needles: &[&str]) -> usize {
    needles.iter().map(|n| haystack.matches(n).count()).sum()
}

const SLEEP_NEEDLES: &[&str] = &[
    "thread::sleep(",
    "tokio::time::sleep(",
    "tokio::time::sleep_until(",
    "tokio::time::interval(",
    "park_timeout(",
];
const RAW_CLOCK_NEEDLES: &[&str] = &[
    "SystemTime::now(",
    "duration_since(UNIX_EPOCH)",
    "duration_since(std::time::UNIX_EPOCH)",
];
// Qualified needles: `Wallclock::` prefix keeps these from matching the blob
// service's unrelated `set_test_clock` method, and naming reset explicitly
// avoids the `reset_test_clock` ⊃ `set_test_clock` substring trap.
const BARE_CLOCK_NEEDLES: &[&str] = &[
    "Wallclock::set_test_clock(",
    "Wallclock::reset_test_clock(",
    "Wallclock::clear_test_clock(",
];

fn is_src_file(path: &str) -> bool {
    path.ends_with(".rs") && path.starts_with("crates/") && path.contains("/src/")
}

/// Every baseline entry must carry a non-empty reason: an entry with an empty
/// third field defeats the point of the (path, count, reason) shape and fails
/// the audit outright, rather than silently degrading to an unexplained
/// number.
fn assert_reasons_present(table: &[(&str, usize, &str)]) -> Vec<String> {
    table
        .iter()
        .filter(|(_, _, reason)| reason.trim().is_empty())
        .map(|(path, _, _)| format!("{path}: baseline entry has an empty reason"))
        .collect()
}

#[test]
fn test_estate_audit_baseline_entries_have_reasons() {
    let mut violations = Vec::new();
    violations.extend(assert_reasons_present(SLEEP_RATCHET));
    violations.extend(assert_reasons_present(RAW_CLOCK_RATCHET));
    violations.extend(assert_reasons_present(BARE_CLOCK_RATCHET));
    violations.extend(assert_reasons_present(SRC_TEST_SLEEP_RATCHET));
    violations.extend(assert_reasons_present(SRC_TEST_BARE_CLOCK_RATCHET));
    violations.extend(assert_reasons_present(SRC_TEST_RAW_CLOCK_RATCHET));
    assert!(
        violations.is_empty(),
        "test-estate audit baseline entries missing a reason:\n{}",
        violations.join("\n")
    );
}

#[test]
fn test_estate_audit_no_new_sleeps_or_raw_clock_reads() {
    let root = workspace_root();
    let ratchet: BTreeMap<&str, usize> = SLEEP_RATCHET.iter().map(|(p, c, _)| (*p, *c)).collect();
    let raw_ratchet: BTreeMap<&str, usize> =
        RAW_CLOCK_RATCHET.iter().map(|(p, c, _)| (*p, *c)).collect();
    let bare_ratchet: BTreeMap<&str, usize> = BARE_CLOCK_RATCHET
        .iter()
        .map(|(p, c, _)| (*p, *c))
        .collect();
    let src_sleep_ratchet: BTreeMap<&str, usize> = SRC_TEST_SLEEP_RATCHET
        .iter()
        .map(|(p, c, _)| (*p, *c))
        .collect();
    let src_bare_ratchet: BTreeMap<&str, usize> = SRC_TEST_BARE_CLOCK_RATCHET
        .iter()
        .map(|(p, c, _)| (*p, *c))
        .collect();
    let src_raw_ratchet: BTreeMap<&str, usize> = SRC_TEST_RAW_CLOCK_RATCHET
        .iter()
        .map(|(p, c, _)| (*p, *c))
        .collect();
    let mut violations = Vec::new();

    for entry in WalkDir::new(&root)
        .into_iter()
        .filter_entry(|e| e.file_name() != "target" && e.file_name() != ".git")
        .filter_map(Result::ok)
    {
        let rel = match entry.path().strip_prefix(&root) {
            Ok(rel) => rel.to_string_lossy().replace('\\', "/"),
            Err(_) => continue,
        };

        // In-src unit-test modules: audit the region from the first
        // `#[cfg(test)]` marker to end-of-file.
        if is_src_file(&rel) {
            let Ok(content) = std::fs::read_to_string(entry.path()) else {
                continue;
            };
            // The region opens at a cfg(test) MODULE — the attribute followed
            // (whitespace-tolerantly) by a mod declaration — not at any lone
            // cfg(test) item like a test-only accessor.
            let region_start = content
                .match_indices("#[cfg(test)]")
                .filter_map(|(idx, marker)| {
                    let rest = content[idx + marker.len()..].trim_start();
                    (rest.starts_with("mod ") || rest.starts_with("pub mod ")).then_some(idx)
                })
                .min();
            if let Some(idx) = region_start {
                let test_region = &content[idx..];
                let sleeps = count_occurrences(test_region, SLEEP_NEEDLES);
                let allowed = src_sleep_ratchet.get(rel.as_str()).copied().unwrap_or(0);
                if sleeps > allowed {
                    violations.push(format!(
                        "{rel} (in-src test region): {sleeps} sleep site(s), ratchet allows \
                         {allowed} — use a counter/state/event wait or the Wallclock mock clock"
                    ));
                }
                let bare = count_occurrences(test_region, BARE_CLOCK_NEEDLES);
                let bare_allowed = src_bare_ratchet.get(rel.as_str()).copied().unwrap_or(0);
                if bare > bare_allowed {
                    violations.push(format!(
                        "{rel} (in-src test region): {bare} bare Wallclock set/reset/clear \
                         call(s), ratchet allows {bare_allowed} — use the RAII \
                         Wallclock::test_clock_guard"
                    ));
                }
                let raw = count_occurrences(test_region, RAW_CLOCK_NEEDLES);
                let raw_allowed = src_raw_ratchet.get(rel.as_str()).copied().unwrap_or(0);
                if raw > raw_allowed {
                    violations.push(format!(
                        "{rel} (in-src test region): {raw} raw clock read(s), ratchet allows \
                         {raw_allowed} — use Wallclock::now() so the mock-clock seam governs it"
                    ));
                }
            }
            continue;
        }

        if !is_test_file(&rel) {
            continue;
        }
        let Ok(content) = std::fs::read_to_string(entry.path()) else {
            continue;
        };

        let sleeps = count_occurrences(&content, SLEEP_NEEDLES);
        let allowed = ratchet.get(rel.as_str()).copied().unwrap_or(0);
        if sleeps > allowed {
            violations.push(format!(
                "{rel}: {sleeps} sleep site(s), ratchet allows {allowed} — use a \
                 counter/state/event wait or the Wallclock mock clock (AGENTS.md testing discipline)"
            ));
        }

        let raw = count_occurrences(&content, RAW_CLOCK_NEEDLES);
        let raw_allowed = raw_ratchet.get(rel.as_str()).copied().unwrap_or(0);
        if raw > raw_allowed {
            violations.push(format!(
                "{rel}: {raw} raw clock read(s) (SystemTime::now / epoch math), ratchet allows \
                 {raw_allowed} — use Wallclock::now() so the mock-clock seam governs it"
            ));
        }

        let bare = count_occurrences(&content, BARE_CLOCK_NEEDLES);
        let bare_allowed = bare_ratchet.get(rel.as_str()).copied().unwrap_or(0);
        if bare > bare_allowed {
            violations.push(format!(
                "{rel}: {bare} bare Wallclock set/reset/clear call(s), ratchet allows \
                 {bare_allowed} — use the RAII Wallclock::test_clock_guard (panic-safe) instead"
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "test-estate audit violations:\n{}",
        violations.join("\n")
    );
}
