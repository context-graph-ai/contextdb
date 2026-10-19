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
use std::path::{Path, PathBuf};

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
        "crates/contextdb-redb/tests/basic_tests.rs",
        1,
        "upstream redb 4.1.0 write-transaction blocking regression; the maintained upstream suite stays byte-identical, including this one synchronization sleep",
    ),
    (
        "crates/contextdb-engine/tests/engine/checkpoint_export_tests.rs",
        6,
        RSN_SYNC_BARRIER,
    ),
    (
        "crates/contextdb-engine/tests/sql_surface/sql_surface_other.rs",
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
        "crates/contextdb-engine/tests/sql_surface/sql_surface_tests.rs",
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

/// Return the bodies of cfg(test) modules rather than everything after the
/// first module. Source files may place ordinary production items between
/// test modules, so the old suffix-based scan could classify production clock
/// reads as test-estate debt.
fn test_module_regions(content: &str) -> Vec<&str> {
    let mut regions = Vec::new();
    let mut search_from = 0;
    while let Some(marker_offset) = content[search_from..].find("#[cfg(test)]") {
        let marker = search_from + marker_offset;
        let rest = content[marker + "#[cfg(test)]".len()..].trim_start();
        if !(rest.starts_with("mod ") || rest.starts_with("pub mod ")) {
            search_from = marker + "#[cfg(test)]".len();
            continue;
        }
        let Some(open_relative) = rest.find('{') else {
            search_from = marker + "#[cfg(test)]".len();
            continue;
        };
        let open = marker + "#[cfg(test)]".len() + rest[..open_relative].len();
        let mut depth = 0usize;
        let mut close = None;
        for (offset, byte) in content.as_bytes()[open..].iter().enumerate() {
            match byte {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        close = Some(open + offset + 1);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(close) = close else {
            break;
        };
        regions.push(&content[marker..close]);
        search_from = close;
    }
    regions
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
            // Audit each cfg(test) MODULE body, not the suffix after the first
            // one. Production items can follow an earlier test module.
            for test_region in test_module_regions(&content) {
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

/// Default-tier test count per workspace crate: every `#[test]` and
/// `#[tokio::test]` attribute in a module Cargo compiles for that crate's
/// targets (`src/lib.rs`, `src/main.rs`, `src/bin/*.rs`, `tests/*.rs` and each
/// `[[test]]` path, following `mod name;` and `#[path]`), minus tests behind
/// a feature in `OUTSIDE_DEFAULT_TIER_FEATURES` or `#[ignore]`. A macro that expands one
/// written attribute into several tests counts once, and a source file no
/// module declares counts zero. The workspace-root `tests/` tree builds as
/// `contextdb-engine` targets. The count must equal its entry: a new test
/// raises it only as an approved exception that names the criterion it
/// proves; a merged or removed test lowers it in the same change.
// Approved inventory reconciliation: retained source-selection, parser boundary,
// and engine publication/restart proofs are all counted in the default tier.
// No tests or clock/sleep allowances are removed or changed.
const DEFAULT_TIER_TEST_RATCHET: &[(&str, usize, &str)] = &[
    // Inventory includes three native custody source-selection proofs.
    ("contextdb-core", 89, RSN_TEST_COUNT),
    ("contextdb-tx", 19, RSN_TEST_COUNT),
    ("contextdb-relational", 10, RSN_TEST_COUNT),
    ("contextdb-graph", 10, RSN_TEST_COUNT),
    // Inventory: the fork proof that appended links keep their order, survive encoding and
    // need a declared slack.
    ("contextdb-hnsw", 22, RSN_TEST_COUNT),
    // Inventory additions (+5): the declared index cap funds each selected
    // partition and keeps opening and expansion inside the original cap
    // one proof each; the policy layout update admits, refunds and publishes
    // only the policy, three proofs.
    // Inventory (204 -> 221): the graph-identity, union-walk and disconnected-count proofs,
    // and the reachability proofs (owed links span every point; built graphs and insertion
    // streams reach every point; serial builds repeat).
    // Inventory increase (221 -> 224, +3): a policy recheck looks at no partition without
    // failures, only the failed one with one, and every one once when its index is incomplete.
    // Inventory increase (224 -> 225, +1): a refused full policy recheck keeps the failure
    // index incomplete, so a funded retry still finds every outstanding failure.
    ("contextdb-vector", 225, RSN_TEST_COUNT),
    ("contextdb-parser", 137, RSN_TEST_COUNT),
    ("contextdb-planner", 10, RSN_TEST_COUNT),
    // Approved continued-growth proofs: unique-key predicate writes match current-entry reads;
    // a maintenance cycle that prunes nothing walks no storage statistics as the store grows;
    // automatic compaction starts on reclaimable free pages and not on in-page slack;
    // storage compaction is declarable and unmeasured fragmentation reads unknown (5 proofs).
    // Inventory reconciliation (4516 -> 4681, +165): each group's tests prove
    // the criterion named. The earlier inventory count was 4503;
    //  - Inventory coverage (4503 -> 4521, +18): existing source tests
    //    countable in the build, identified by the
    //    audit's own name list: database/snapshot_pin_pool_tests.rs
    //    overflow_pins_spend_reader_memory_and_refuse_without_changing_committed_state;
    //    database/vector_suffix_inspection_tests.rs local_current_identity_admission_refuses_the_complete_transaction_before_durability,
    //    sync_current_identity_admission_refuses_rows_vectors_and_the_authenticated_receipt,
    //    captured_suffix_inspection_uses_actual_reader_and_root_without_resident_tail,
    //    seeded_suffix_writer_final_owner_refunds_without_injected_failure;
    //    persistence/proof_allocation_oracle_tests.rs general_checkpoint_preserves_original_native_ownership;
    //    persistence/relational_posting_prune.rs borrowed_terminal_keys_preserve_the_canonical_scalar_codec,
    //    expired_last_indexed_version_stays_absent_from_later_historical_reads,
    //    reclaimed_intervals_split_postings_reanchor_current_and_preserve_old_roots;
    //    persistence/vector_current_identity.rs repeated_current_probes_release_transient_decode_before_the_next_row,
    //    current_identity_probe_work_is_independent_of_retained_history,
    //    signed_duplicates_are_counted_atomically_and_do_not_change_old_captures,
    //    current_moves_replay_and_refusals_preserve_exact_owner_and_captured_roots,
    //    missing_and_mismatched_current_witnesses_refuse_without_reconstruction,
    //    cancelled_current_identity_refuses_before_probes_and_refunds_its_owner;
    //    tests/engine_seams/edge_property_write_contract.rs
    //    replacing_a_small_property_preserves_and_funds_the_large_existing_payload,
    //    deleting_then_reinserting_an_edge_does_not_merge_retired_properties;
    //    tests/native_backing_allocator.rs
    //    non_test_engine_native_controls_match_actual_allocator_requests_and_final_frees;
    //    renames in the same span (cdb120_partition_insert_cost, paged_generation_load_pricing_contract,
    //    vector_tail_transaction_contract) change no count;
    //  - native delivery served beside the legacy handler (7), the custody
    //    history residency check (1), the delivery journal read through the durable
    //    group index (1), an import whose vectors exceed the default read memory
    //    (1): +10;
    //  - Inventory (+26): commit cost scales with the transaction, not the store: open table handles
    //    charged to the writer budget, pending version deltas indexed by position, bulk deletes
    //    and alternating tables reopen each table once (14); held relations charge
    //    their funded memory through real commits (2); a synced policy-only vector
    //    ALTER keeps the receiver's graphs, manifest seeks over pending deltas, and the sender
    //    ships a policy ALTER apart from vector rows (10);
    //  - Inventory (+42 net): open-time journal record validation and open charge flat as history
    //    grows (13); sink queue windows (10; 7); a covered journal
    //    cleanup holds the commit lock one step at a time (1); two legacy
    //    policy-hold upgrade tests removed (-2); read-only open stays flat as history
    //    grows (7); a row pays for its partition key once and a refused lineage
    //    scratch reservation copies nothing (2); a quarantined route's
    //    repair replaces its damaged catalog, base or change while damage elsewhere still
    //    refuses (catalog repair, 1; damaged base or change repair, 2); a finished vector
    //    rebuild behind many small commits publishes within a few cycles (catch-up, 1);
    //  - Inventory (+39 net): import-tail decodes charged once to the writer, exact-length decode,
    //    reads and writes charged to their own limits (41); two full-size stress cases
    //    moved to the nightly tier (-2);
    //  - Inventory (+25): bounded custody storage, durable custody reads, validation,
    //    pending delivery and retirement admission with their regression controls;
    //  - Inventory (+9): an in-transaction COUNT is served from the declared MAINTAIN COUNT (5);
    //    FLOAT equality and order follow PostgreSQL float8 on every route (4).
    // 2 + 11 compaction proofs were already in the 4516 entry.
    // Inventory (+1): a draining owner keeps its exact channel until admitted work
    // leaves, the read contract's close order (Vigil store/shutdown).
    //  - Inventory (+1): a historical read costs the same per returned row as rows and
    //    history grow, before and after reopen
    //    (a_historical_read_costs_the_same_per_returned_row_as_rows_and_history_grow).
    //  - Inventory (+2): a long row history adds no work to a historical read; dead
    //    (key, row) groups are stepped once each whatever their history
    //    (a_historical_index_read_costs_its_rows_and_dead_groups).
    //  - Inventory (+7): historical index postings must cover their row versions; a
    //    skipped posting change refuses the commit in each writer family (commit, rebuild,
    //    purge, retention split, sync replay), a dropped index's commitment goes with it, and
    //    a reader refuses a commitment that does not authenticate
    //    (historical_index_postings_must_cover_their_row_versions).
    //  - Inventory (+1): a historical index verification answer survives the read wire
    //    (a_historical_index_verification_survives_the_wire).
    //  - Inventory (+30): native sync delivery is paged, owner-funded and refusal-typed:
    //    the in-process, Iroh and decode proofs of native delivery (page cut counted before
    //    building, result reserved before the custody commit, typed refusals, cancellation and
    //    final-holder release, the excluded oversized outcome) (28); the two custody-index
    //    proofs over native delivery, transaction repair restoring it after reopen and a lost
    //    marker refused over Iroh with the typed owner-route refusal (2).
    //  - Historical index coverage (14 tests): every historical index consumer steps a
    //    row's interval history once (historical_group_consumer_tests, 1); commitment memory
    //    is admitted and a refusal rolls back (2), a dropped index leaves nothing behind,
    //    also across PURGE, and a skipped retirement is refused (3); `.verify history` stops
    //    mid-walk and is charged to its reader (verification_tests 3, file route 1); posting
    //    decomposition is enforced (interval_invariant_tests, 4).
    // Inventory additions (+47): a writable open waits once for readers that are starting, instead of
    // refusing. The proofs cover:
    //  - the writer's single wait, and its caller's cancellation and deadline;
    //  - the cancellable wait's helper process, contained when killed, when its parent dies,
    //    and when its death cannot be confirmed;
    //  - admission closed by the physical claim through every name of the store, with or
    //    without a companion;
    //  - a reader's probe never refusing a writer;
    //  - the reader's provisional-then-exact registration, and each failure under the data lock;
    //  - erasure that is never served to a reader mid-acquisition;
    //  - registry contention retaining instead of refusing.
    //  - History-check funding (5 tests): history-check charges stay held until their
    //    publications are dropped, and an oversized name is refused before it is kept
    //    (funding_lifetime_tests, 2); verification stops inside one wide body or key and a wide
    //    key is refused by its own reader (wide_record_verification_tests, 3).
    //  - Inventory (+2): validating a write's unique key is funded by the writer budget,
    //    not the statement's read limit (a_unique_key_larger_than_the_read_limit_validates_on_the_writer_budget,
    //    a_constraint_key_the_writer_budget_cannot_fund_refuses_typed_with_nothing_allocated).
    // Inventory (4812 -> 4855, +43): the vector search proofs (graph identity, union walk,
    // disconnected counts, version scoring, restamp, every graph route reaches every row).
    // Inventory increase (4855 -> 4861, +6): a one-table commit visits no other table's
    // declaration or saved row count (1); a synced policy change clears a failure its new
    // policy no longer needs (1); an UPDATE or DELETE bound through a full index prefix, and the
    // same WHERE as a SELECT, examine only their rows, tied indexes chosen by range (3); every
    // route counts the same entries examined (1).
    // Inventory (+1): a one-row commit reads the process memory ceiling once, not once
    // per admission (a_one_row_commit_reads_the_process_ceiling_once).
    // Inventory increase (4862 -> 4864, +2): counting a context through its maintained
    // count and reading its newest row through an index that holds the order stay flat as
    // the context grows (tied_indexes_binding_the_same_columns_read_no_range_to_compare).
    // Inventory increase (4864 -> 4865, +1): an update authenticates only the row version
    // it replaces, whatever the row's history
    // (a_row_update_authenticates_its_latest_version_not_its_history).
    //  - Wide-key verification (2 tests): every phase of one wide key's verification
    //    checks the caller's request, and cancellation or expiry stops the walk inside each
    //    phase (wide_key_phase_tests, 2).
    // Inventory (+2): a ranked search on the file route holds one visited join identity
    // and one joined body at a time (a_ranked_file_route_read_holds_bounded_memory).
    // Inventory (+3): a crashed writer's store is read at the commit repair keeps --
    // killed before any write of a commit, admission flat across store size, and refused
    // typed when the reader cannot vouch for the boot, filesystem or data file
    // (read a_crashed_writers_store_opens_for_a_reader).
    // Inventory increase (4873 -> 4879, +6): a commit decides each table's retention clock
    // once (1); tied index probes stay inside the read's work, deadline and memory limits (3);
    // equal vector scores across partitions keep row order on every route (1); one event
    // append costs the same at any stored history (1).
    //  - Inventory (+2, retention regression): retention erases an expired live row from
    //    its indexes, and never hides a version a pinned snapshot still sees
    //    (historical_index_postings_must_cover_their_row_versions, 2).
    // Inventory (+4): process-ceiling discovery never reads a failure as "no limit" --
    // an unreadable limit file fails discovery, an interrupted read is retried, a failed
    // refresh keeps the last valid ceiling, and with no safe ceiling an open is refused typed
    // (cgroup_ceiling_tests +2, undeclared_writer_ceiling_tests +2).
    // Inventory (+1): a maintenance cycle and a pruning pass each observe a lowered
    // process ceiling before they admit (a_maintenance_cycle_and_a_pruning_pass_each_observe_a_lowered_ceiling).
    // Inventory (+3): a hub pull is cut into pages at source-LSN group boundaries and
    // funded by the writer budget, flat as the table doubles, from the change log and from
    // persisted state, with a transaction larger than a page arriving whole
    // (pull_page_tests: a_large_two_way_table_pulls_in_pages_with_a_flat_peak,
    // a_pull_served_from_persisted_state_pages_with_a_flat_peak,
    // a_transaction_larger_than_a_page_arrives_whole).
    // Root-capture admission (1 test): choosing a crashed store's root is
    // charged to the reader and stops typed at a cancellation, an expired deadline or a
    // refused charge (a_crashed_stores_root_capture_is_charged_and_stoppable).
    //  - Inventory (+4): a constraint key is funded for the native key it builds, staged
    //    and committed probes refuse before building it, and a multi-row commit releases its
    //    validation charge (a_constraint_key_charge_covers_the_native_key_it_builds,
    //    a_wide_scalar_staged_probe_refuses_before_it_builds_its_key,
    //    a_wide_scalar_committed_probe_refuses_before_it_builds_its_key,
    //    a_multi_row_wide_scalar_commit_validates_each_row_and_releases_its_charge).
    // Inventory (+1): reading the visible scores of a restamped group's rows is paid
    // from the search's rescoring allowance; a group past it is refused typed, never
    // answered in part (restamped_group_rescoring_stays_inside_the_breadth_allowance).
    //  - Retention count sharing (1 test): erasing an expired live row leaves a count
    //    two indexes share exactly once
    //    (erasing_a_live_row_leaves_a_count_shared_by_two_indexes_once).
    // Inventory additions (+10): reading a store's format and finishing a read leave nothing behind and
    // never hold a writer back. The proofs cover:
    //  - a format check through a hard link takes no data lock past a queued writer, and reads
    //    a format-bounded number of pages at any table count;
    //  - a finished reader removes its own registration record;
    //  - a quarantine that resolves releases its helper's stack;
    //  - a healthy reader never makes a writable open fail, and the writer confirms a live
    //    reader's descriptor and refuses every forged or stale one;
    //  - a crashed store's reader admission does not grow with the store.
    // Inventory (+5): process-ceiling discovery reads kernel tables whole -- a file
    // larger than its buffer is unreadable, mountinfo is streamed line by line, a cgroup
    // mount line that does not fit fails discovery, a failed mount read is not kept, and a
    // unified membership without its mount is unavailable unless v1 supplies the limit
    // (cgroup_ceiling_tests +5).
    //  - Wide-row retention (1 test): erasing a wide live row retains no count
    //    key per maintained prefix (erasing_a_wide_live_row_retains_no_count_key_per_prefix).
    // Inventory increase (4912 -> 4914, +2): a commit admits the grouping that decides its
    // retention clocks (1); comparing equally ranked indexes allocates no more than it
    // admits (1).
    //  - Stored-vector projection (4 tests): projecting a stored vector checks its request
    //    every 4096 components, a malformed stored vector is refused before any component work,
    //    the hashed projection charges the same whatever the width or count, and the streamed
    //    key and elements equal the legacy encoder (vector_projection_tests, 4).
    // Inventory increase (4918 -> 4925, +7, graph-edge identity fix): a graph edge keeps the
    // identity its table declares (tests/sql_surface/an_edge_keeps_the_identity_its_table_declares.rs,
    // with the matching engine behavior): a declared key that is exactly
    // the triple still deduplicates it, a decorated edge table keeps both subtypes, a true
    // duplicate under the declared key is still not inserted twice, an edge differing only by an
    // extra declared key column is inserted (plain, in a transaction, on disk), and with no
    // declared key the triple is still deduplicated.
    //  - Inventory (+2, compaction receipts and regrowth): the automatic and explicit
    //    receipts report the same reclaimable share, and the first write after compaction
    //    grows the file by the declared stop ratio, never by doubling it
    //    (storage_compaction_is_declarable_and_unmeasured_fragmentation_reads_unknown, 2).
    // Inventory additions (+3): a format scan paused in this store or another store
    // cannot delay a reader and queued writer (two distinct dependency paths);
    // a crashed store without a boot identity gives the typed needs-writer
    // refusal on macOS. The classification schedules fail with the global
    // hook mutex; the macOS row guards the supported-platform expectation.
    // A declared stop ratio bounds each growth after SET, reopen and handle
    // recycling (a_declared_stop_ratio_never_lets_one_growth_exceed_it).
    // A truncated or overlong stored vector refuses typed corruption across
    // persisted encodings (a_truncated_or_overlong_stored_vector_is_refused_as_corruption).
    // Serializer cleanup coverage (+22): 13 runtime cleanup journeys, four
    // last-reader cleanup units and five purge-publication cleanup units.
    ("contextdb-engine", 4954, RSN_TEST_COUNT),
    //  - Inventory (+6): an oversized pending unit pushes from a reopened source whose read
    //    limit is smaller than the unit, the hub refuses a unit its writer can never fit before
    //    it transfers, a transiently refused unit stays pending and lands on retry, and a second
    //    edge pulls it and converges (oversized_dependency_unit_reopened_tests, 6 tests).
    ("contextdb-server", 427, RSN_TEST_COUNT),
    //  - Inventory (+1): `.verify history <table>` checks each historical index from a
    //    read session (verify_history_checks_each_historical_index_from_a_read_session).
    //  - Inventory (+1): quitting the CLI pushes a pending oversized unit and exits clean
    //    (quitting_the_cli_pushes_a_pending_oversized_unit_and_exits_clean).
    ("contextdb-cli", 273, RSN_TEST_COUNT),
];

/// Reason shared by every default-tier count entry.
const RSN_TEST_COUNT: &str = "default-tier test budget; one proof per acceptance criterion, \
     a second test for one criterion only with a recorded reason";

/// Features the default tier never enables. `test-seams` is not listed: the
/// CLI's dev-dependencies unify it into every default-tier test build.
const OUTSIDE_DEFAULT_TIER_FEATURES: &[&str] = &["nightly-tests", "mdns"];

/// Whether a raw `#[cfg(...)]` / `#![cfg(...)]` attribute line requires a
/// feature the default tier never enables, or a release build (the default
/// tier is a debug build).
fn gated_outside_default_tier(attribute: &str) -> bool {
    attribute == "#[cfg(not(debug_assertions))]"
        || (attribute.starts_with("#[cfg(") || attribute.starts_with("#![cfg("))
            && !attribute.contains("not(")
            && OUTSIDE_DEFAULT_TIER_FEATURES
                .iter()
                .any(|feature| attribute.contains(&format!("feature = \"{feature}\"")))
}

fn is_attribute_line(line: &str) -> bool {
    let line = line.trim();
    line.starts_with("#[") || line.ends_with(")]") || line.ends_with(',')
}

/// Indexes of the attribute lines written directly above line `index`,
/// nearest first. Comments are already blanked, so blank lines are skipped.
fn attributes_above(lines: &[&str], index: usize) -> Vec<usize> {
    let mut group = Vec::new();
    for above in (0..index).rev() {
        if lines[above].trim().is_empty() {
            continue;
        }
        if !is_attribute_line(lines[above]) {
            break;
        }
        group.push(above);
    }
    group
}

fn is_fn_signature(line: &str) -> bool {
    let line = line.strip_prefix("pub ").unwrap_or(line);
    let line = line.strip_prefix("async ").unwrap_or(line);
    line.starts_with("fn ")
}

/// Default-tier test attributes in one module file. `code` is `source` with
/// comments and literals blanked (same line numbers): structure is read from
/// `code`, attribute contents from `source`.
fn default_tier_tests_in(source: &str, code: &str) -> usize {
    let raw: Vec<&str> = source.lines().collect();
    let lines: Vec<&str> = code.lines().collect();
    let mut count = 0;
    for (index, line) in lines.iter().enumerate() {
        let line = line.trim();
        if line != "#[test]" && !line.starts_with("#[tokio::test") {
            continue;
        }
        let below = (index + 1..lines.len())
            .take_while(|&after| !is_fn_signature(lines[after].trim()))
            .map(|after| raw[after].trim());
        let excluded = attributes_above(&lines, index)
            .into_iter()
            .map(|above| raw[above].trim())
            .chain(below)
            .any(|attribute| {
                gated_outside_default_tier(attribute) || attribute.starts_with("#[ignore")
            });
        if !excluded {
            count += 1;
        }
    }
    count
}

/// Child module files a module file declares with `mod name;`, skipping
/// declarations gated outside the default tier. `source` and its mask `code` share
/// line numbers; a `#[path]` value is read from `source`.
fn declared_modules(file: &Path, source: &str, code: &str, is_root: bool) -> Vec<PathBuf> {
    let directory = file.parent().expect("a module file has a directory");
    let owner_directory = if is_root || file.file_name().is_some_and(|name| name == "mod.rs") {
        directory.to_path_buf()
    } else {
        directory.join(file.file_stem().expect("a module file has a stem"))
    };
    let raw: Vec<&str> = source.lines().collect();
    let lines: Vec<&str> = code.lines().collect();
    let mut modules = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        let declaration = line.trim();
        let declaration = ["pub(crate) ", "pub(super) ", "pub "]
            .iter()
            .find_map(|visibility| declaration.strip_prefix(visibility))
            .unwrap_or(declaration);
        let Some(name) = declaration
            .strip_prefix("mod ")
            .and_then(|rest| rest.strip_suffix(';'))
            .map(str::trim)
        else {
            continue;
        };
        let attributes = attributes_above(&lines, index);
        if attributes
            .iter()
            .any(|&above| gated_outside_default_tier(raw[above].trim()))
        {
            continue;
        }
        let explicit = attributes.iter().find_map(|&above| {
            let attribute = raw[above].trim();
            attribute
                .strip_prefix("#[path = \"")
                .and_then(|rest| rest.strip_suffix("\"]"))
        });
        if let Some(path) = explicit {
            modules.push(directory.join(path));
            continue;
        }
        if let Some(found) = [
            owner_directory.join(format!("{name}.rs")),
            owner_directory.join(name).join("mod.rs"),
        ]
        .into_iter()
        .find(|candidate| candidate.exists())
        {
            modules.push(found);
        }
    }
    modules
}

/// Every target root Cargo builds for one workspace member's default tier.
fn crate_target_roots(crate_directory: &Path) -> Vec<PathBuf> {
    let mut roots: Vec<PathBuf> = ["src/lib.rs", "src/main.rs"]
        .iter()
        .map(|relative| crate_directory.join(relative))
        .filter(|path| path.exists())
        .collect();
    let rust_files_in = |directory: PathBuf| {
        let mut files: Vec<PathBuf> = std::fs::read_dir(directory)
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
            .collect();
        files.sort();
        files
    };
    roots.extend(rust_files_in(crate_directory.join("src/bin")));
    let manifest: toml::Value = toml::from_str(
        &std::fs::read_to_string(crate_directory.join("Cargo.toml")).expect("crate manifest"),
    )
    .expect("crate manifest parses");
    let autotests = manifest
        .get("package")
        .and_then(|package| package.get("autotests"))
        .and_then(toml::Value::as_bool)
        .unwrap_or(true);
    if autotests {
        roots.extend(rust_files_in(crate_directory.join("tests")));
    }
    for target in manifest
        .get("test")
        .and_then(toml::Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(path) = target.get("path").and_then(toml::Value::as_str) {
            roots.push(crate_directory.join(path));
        }
    }
    roots
}

fn default_tier_tests_in_crate(crate_directory: &Path) -> usize {
    let mut visited = std::collections::BTreeSet::new();
    let mut pending: Vec<(PathBuf, bool)> = crate_target_roots(crate_directory)
        .into_iter()
        .map(|root| (root, true))
        .collect();
    let mut count = 0;
    while let Some((file, is_root)) = pending.pop() {
        let Ok(canonical) = file.canonicalize() else {
            continue;
        };
        if !visited.insert(canonical.clone()) {
            continue;
        }
        let source = std::fs::read_to_string(&canonical).expect("module source");
        if source.lines().any(|line| {
            line.trim().starts_with("#![cfg(") && gated_outside_default_tier(line.trim())
        }) {
            continue;
        }
        let code = audit_support::rust_scope_text(&source);
        count += default_tier_tests_in(&source, &code);
        pending.extend(
            declared_modules(&canonical, &source, &code, is_root)
                .into_iter()
                .map(|module| (module, false)),
        );
    }
    count
}

#[test]
fn test_estate_default_tier_test_count_matches_its_ratchet() {
    let root = workspace_root();
    let manifest: toml::Value = toml::from_str(
        &std::fs::read_to_string(root.join("Cargo.toml")).expect("workspace manifest"),
    )
    .expect("workspace manifest parses");
    let members: Vec<&str> = manifest["workspace"]["members"]
        .as_array()
        .expect("workspace members")
        .iter()
        .filter_map(toml::Value::as_str)
        .collect();
    let ratchet: BTreeMap<&str, usize> = DEFAULT_TIER_TEST_RATCHET
        .iter()
        .map(|(name, count, _)| (*name, *count))
        .collect();
    let mut violations = assert_reasons_present(DEFAULT_TIER_TEST_RATCHET);
    for member in &members {
        let name = member
            .rsplit('/')
            .next()
            .expect("member path names a crate");
        let count = default_tier_tests_in_crate(&root.join(member));
        match ratchet.get(name) {
            None => violations.push(format!(
                "{name}: {count} default-tier test(s) and no ratchet entry; add one with its reason"
            )),
            Some(&allowed) if count > allowed => violations.push(format!(
                "{name}: {count} default-tier tests, ratchet allows {allowed} — make the new \
                 proof a row of the mapped test, or record the approved exception"
            )),
            Some(&allowed) if count < allowed => violations.push(format!(
                "{name}: {count} default-tier tests, ratchet still says {allowed} — lower the \
                 entry to {count}"
            )),
            Some(_) => {}
        }
    }
    for (name, _, _) in DEFAULT_TIER_TEST_RATCHET {
        if !members.iter().any(|member| member.ends_with(name)) {
            violations.push(format!("{name}: ratchet entry names no workspace member"));
        }
    }
    assert!(
        violations.is_empty(),
        "default-tier test count violations:\n{}",
        violations.join("\n")
    );
}
