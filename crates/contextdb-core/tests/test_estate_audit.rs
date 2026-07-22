//! Test-estate discipline audit (see AGENTS.md "Testing discipline").
//!
//! Ratchet guard: the per-file counts below freeze the EXISTING sleep sites and
//! raw clock reads in the test tree (ledgered debt, being worked down by the
//! test-estate round). A NEW sleep or raw clock read in any test file fails
//! this audit. The fix is never "raise the number": use the
//! `Wallclock::test_clock_guard` mock clock for time-dependent behavior, and
//! counter/state/event waits for synchronization. Lower a count when you remove
//! a site. Raising a count is an explicitly approved exception only.

use std::collections::BTreeMap;

use walkdir::WalkDir;

#[path = "audit_support/mod.rs"]
mod audit_support;
use audit_support::workspace_root;

/// Every test file allowed to contain `thread::sleep(` / `tokio::time::sleep(`
/// call sites, with the frozen maximum count. Counts, not line numbers — a
/// pure line shift must not trip the audit.
const SLEEP_RATCHET: &[(&str, usize)] = &[
    (
        "crates/contextdb-engine/tests/checkpoint_export_tests.rs",
        6,
    ),
    ("crates/contextdb-engine/tests/sql_surface_tests.rs", 3),
    ("crates/contextdb-server/tests/iroh_transport_tests.rs", 2),
    (
        "crates/contextdb-server/tests/media_transfer_reclaim_driver.rs",
        1,
    ),
    ("crates/contextdb-server/tests/stale_restore_tests.rs", 1),
    ("crates/contextdb-server/tests/sync_integration.rs", 14),
    ("crates/contextdb-server/tests/work_ledger_tests.rs", 7),
    ("tests/acceptance/cli_ux.rs", 1),
    ("tests/acceptance/common.rs", 7),
    ("tests/acceptance/db_lock_visibility.rs", 1),
    ("tests/acceptance/engine_cron.rs", 10),
    ("tests/acceptance/event_bus.rs", 27),
    ("tests/acceptance/event_bus_self_join.rs", 11),
    ("tests/acceptance/multi_table_atomic_visibility.rs", 7),
    ("tests/acceptance/observation_trigger.rs", 1),
    ("tests/acceptance/persistence.rs", 1),
    ("tests/acceptance/sync.rs", 2),
    ("tests/acceptance/trigger_concurrency_panic_freedom.rs", 10),
    ("tests/acceptance/trigger_same_db_progress.rs", 29),
    ("tests/acceptance/txid_monotonic_concurrent_commits.rs", 1),
    (
        "tests/integration/auto_sync_update_push_visibility_tests.rs",
        3,
    ),
    ("tests/integration/gate_a_sync.rs", 16),
    ("tests/integration/cross_cutting_acceptance_tests.rs", 2),
    ("tests/integration/indexed_scan_filter_tests.rs", 1),
    ("tests/integration/peak_rss_harness.rs", 1),
    ("tests/integration/persistence_tests.rs", 4),
    ("tests/integration/retention_tests.rs", 3),
    ("tests/integration/sync_relay_trigger_tables_tests.rs", 1),
    ("tests/integration/sync_server_nonblocking_apply.rs", 4),
];

/// Test files allowed to read the raw system clock (`SystemTime::now(` or
/// inline epoch math via `duration_since(UNIX_EPOCH)`). Everything else uses
/// `Wallclock::now()` / the mock-clock seam.
const RAW_CLOCK_RATCHET: &[(&str, usize)] = &[
    // Counts are NEEDLE OCCURRENCES, not sites: a read written as
    // `SystemTime::now().duration_since(std::time::UNIX_EPOCH)` contributes 2.
    ("crates/contextdb-cli/tests/corrupt_store_open_tests.rs", 2),
    ("crates/contextdb-engine/tests/sql_surface_tests.rs", 4),
    ("tests/integration/retention_tests.rs", 2),
];

/// The bare `set_test_clock`/`reset_test_clock`/`clear_test_clock` calls leak
/// the override on a mid-test panic; only the seam's own contract test may use
/// them, at exactly its current count. Everyone else uses the RAII
/// `test_clock_guard`.
const BARE_CLOCK_RATCHET: &[(&str, usize)] =
    &[("crates/contextdb-core/tests/txid_newtype_tests.rs", 3)];

/// Unit-test modules living inside `src/` files (everything from the first
/// `#[cfg(test)]`-attributed MODULE to end-of-file — an attribute on a lone
/// item, e.g. a test-only accessor, does not open the region) are audited with
/// the same needles. Production code above the marker is exempt — it may
/// legitimately sleep (e.g. `sleep_with_shutdown`) and read real clocks.
const SRC_TEST_SLEEP_RATCHET: &[(&str, usize)] = &[
    ("crates/contextdb-cli/src/auto_sync.rs", 4),
    ("crates/contextdb-engine/src/database.rs", 1),
];

/// In-src test modules allowed bare `Wallclock::` clock calls: the seam's home
/// module tests the set/reset contract itself.
const SRC_TEST_BARE_CLOCK_RATCHET: &[(&str, usize)] = &[("crates/contextdb-core/src/types.rs", 1)];

/// In-src test modules allowed raw clock reads. Currently none — a pure guard.
const SRC_TEST_RAW_CLOCK_RATCHET: &[(&str, usize)] = &[];

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

const SLEEP_NEEDLES: &[&str] = &["thread::sleep(", "tokio::time::sleep("];
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

#[test]
fn test_estate_audit_no_new_sleeps_or_raw_clock_reads() {
    let root = workspace_root();
    let ratchet: BTreeMap<&str, usize> = SLEEP_RATCHET.iter().copied().collect();
    let raw_ratchet: BTreeMap<&str, usize> = RAW_CLOCK_RATCHET.iter().copied().collect();
    let bare_ratchet: BTreeMap<&str, usize> = BARE_CLOCK_RATCHET.iter().copied().collect();
    let src_sleep_ratchet: BTreeMap<&str, usize> = SRC_TEST_SLEEP_RATCHET.iter().copied().collect();
    let src_bare_ratchet: BTreeMap<&str, usize> =
        SRC_TEST_BARE_CLOCK_RATCHET.iter().copied().collect();
    let src_raw_ratchet: BTreeMap<&str, usize> =
        SRC_TEST_RAW_CLOCK_RATCHET.iter().copied().collect();
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
