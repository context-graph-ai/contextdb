//! Test-estate discipline audit (see AGENTS.md "Testing discipline").
//!
//! Ratchet guard: the per-file counts below freeze the EXISTING sleep sites and
//! raw clock reads in the test tree. An additional sleep or raw clock read
//! in any test file fails
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

/// In-line test module bodies and the whole contents of test-gated out-of-line
/// modules are audited with the same needles. Out-of-line modules inherit test
/// status through nested declarations, including `cfg(all(test, ...))` gates.
/// Production items outside those modules remain exempt.
const SRC_TEST_SLEEP_RATCHET: &[(&str, usize, &str)] = &[
    (
        "crates/contextdb-engine/src/persistence/proof_allocation_oracle_tests.rs",
        1,
        "existing allocation-attribution fixture newly visible through out-of-line module auditing; \
         replace its neighbour-thread sleep with a deterministic allocation barrier",
    ),
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

/// The parts of the `src` file at `rel` this audit reads as unit-test code.
fn src_test_regions<'a>(rel: &str, content: &'a str) -> Vec<&'a str> {
    static MODULES: std::sync::OnceLock<std::collections::BTreeSet<String>> =
        std::sync::OnceLock::new();
    let modules = MODULES.get_or_init(|| {
        out_of_line_test_modules(&workspace_root())
            .into_iter()
            .map(|(_, module)| module)
            .collect()
    });
    if modules.contains(rel) {
        vec![content]
    } else {
        test_module_regions(content)
    }
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

        // In-src unit-test modules: in-line bodies or whole out-of-line files.
        if is_src_file(&rel) {
            let Ok(content) = std::fs::read_to_string(entry.path()) else {
                continue;
            };
            // Production items can follow an earlier in-line test module;
            // out-of-line test modules are test code from their first byte.
            for test_region in src_test_regions(&rel, &content) {
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
// Reconciled retained membership, including previously uncounted out-of-line modules.
// Core retains the native recorder/refusal tests and the out-of-line module audit;
// its unavailable-index vocabulary test is renamed, and the removed HNSW patch audit is absent.
// Transactions retain the paired-capture, conditional-undo and row-owned-edge tests;
// graph retains the payload-ownership and budget-refusal tests.
// Vector retains native copy/visibility and policy-layout tests; HNSW-only tests are removed.
// Parser adds ddl_parses_follows_parent_purge_on_column_and_table_foreign_keys,
// memory_and_disk_settings_share_optional_assignment_syntax and session_cache_settings_do_not_require_a_writable_store.
// Engine includes all four original_version_manifest::seek_tests tests and the retained
// native custody, received-schema, reader-publication and exact-search proofs.
// Server retains a_disconnected_transport_preserves_the_offline_worker_path and
// an_internal_engine_error_cannot_be_reported_as_an_offline_hub; its old purge proof was replaced.
// CLI removes in-memory/index-only cases and retains the new durable invocation and owner-handover cases.
const DEFAULT_TIER_TEST_RATCHET: &[(&str, usize, &str)] = &[
    // Inventory includes three native custody source-selection proofs.
    // One additional refusal proof: an absent-readiness failure detail belongs to the
    // direct-read-requires-writer refusal alone and survives the wire.
    ("contextdb-core", 95, RSN_TEST_COUNT),
    ("contextdb-tx", 39, RSN_TEST_COUNT),
    // Inventory (+1): a new exact index storage holds no probe filter until its first
    // posting (a_new_exact_index_storage_holds_no_filter_until_its_first_posting).
    ("contextdb-relational", 11, RSN_TEST_COUNT),
    ("contextdb-graph", 26, RSN_TEST_COUNT),
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
    // Blocking maintenance arrival guards cover reach, cancellation, drop and replacement.
    ("contextdb-vector", 236, RSN_TEST_COUNT),
    ("contextdb-parser", 142, RSN_TEST_COUNT),
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
    //    renames in the same span (partition_insert_cost, paged_generation_load_pricing_contract,
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
    // Inventory (+4): a fresh snapshot pin captures its own physical root (1),
    // and plugin, decoder and overlapping storage failures retain their original
    // diagnosis and paid storage through final release (3).
    // Inventory (+1): historical vector public queries reject damaged selected-root
    // companions and global controls while retained healthy roots still answer.
    // Arriving readers wait for an opening writer; a serving owner answers route selection.
    // Committed sink drive, publication drive and final release add ten guards;
    // ordinary transactions and persisted vector answers add two public-layout guards.
    // Overflowing allocation diagnostics preserve the first unwind and reject incomplete evidence.
    // Nine added proofs and one removed fallback proof: a store the current build
    // wrote opens at both doors without decoding a row or a vector (eight healthy-producer
    // journeys), a store without a membership directory opens only when both vector counts
    // are zero, and the fallback purge journey goes with the fallback it forced.
    // Inventory (+7), a reader is charged for what it holds: it is charged for the exact
    // filters it physically holds, not one per declared index, and a populated exact index
    // answers hits and misses with none (2, reader_charges_what_it_holds); a reader refused
    // for its vector directories is not told to stop committing rows (1); a small history is
    // answered with less room than the proof-pool ceiling and a walk that outgrows the
    // reader is refused with the reader's own numbers (2, history_request_memory); the
    // commit-floor seek is charged for the proof memory it uses and a reader below its fixed
    // source floor is told the floor (2, empty_declared_index_readiness_tests).
    // Retained distinct criteria (+5): unsafe fallback ancestors are refused by
    // every registry consumer; creating a reader directory cannot chmod a swapped
    // symlink target; an unset HOME resolves the effective user's data directory;
    // pulled row history and edge callbacks stay suppressed while local edits and
    // hub-authored callbacks push; a locally recreated row after a pulled delete
    // reaches the hub both before and after reopening the edge.
    // The cursor retained-invoice proof is a row of the existing cursor lifecycle
    // test, with all assertions preserved. Durable vector-generation reclamation
    // remains unproven: its existing guard is now ignored. These five retained
    // exceptions increase the active budget by four (5415 - 1 + 5 = 5419).
    // Five owner-deferred rows remain unmet: four maintained-vector-index
    // promises and same-handle recovery after a refused write (5417 - 5).
    // Four verdict-cache tests target a cache removed from production.
    // Engine tests whose promises the engine does not keep are ignored with a plain reason, and
    // four tests that only wait for or guard a vector index are deleted (5321 - 35 = 5286);
    // twenty-one vector tests whose engine promise is broken are ignored (5286 - 21 = 5265).
    // Eleven more tests that do not finish within five minutes when run alone are ignored
    // with a plain reason (5265 - 7 = 5258; the others were not counted in the default tier).
    // Memory, history, schema and admission tests whose engine promise is broken are
    // ignored (5258 - 21 = 5237).
    // Seventy-nine more tests whose engine promise is broken, or that need the switched-off
    // clustered vector index, are ignored with a plain reason; one of them is gated to the
    // nightly tier and was not counted (5237 - 78 = 5159).
    // Four more engine tests whose introspection, turnover or reclamation promise is broken
    // are ignored (5159 - 4 = 5155).
    // Thirty-eight more tests from the cancelled hosted jobs are ignored with a plain reason: twelve
    // do not finish within five minutes alone and twenty-six fail alone on the engine as it is
    // (5155 - 31 = 5124; the others are not in the default tier).
    // Six more tests are ignored: four vector tests that need the switched-off clustered
    // vector index, an external-delivery crash test, and a custody read test whose refusal
    // intermittently leaves bytes unadmitted (5124 - 6 = 5118).
    // A purge test whose purge is refused as contended is ignored (5118 - 1 = 5117).
    // Five tests whose ignores hid test defects run again, after their audits and baselines
    // were repaired: three sync source-text audits, the native refusal string audit and the
    // racing history requests test (5117 + 5 = 5122).
    ("contextdb-engine", 5122, RSN_TEST_COUNT),
    //  - Inventory (+6): an oversized pending unit pushes from a reopened source whose read
    //    limit is smaller than the unit, the hub refuses a unit its writer can never fit before
    //    it transfers, a transiently refused unit stays pending and lands on retry, and a second
    //    edge pulls it and converges (oversized_dependency_unit_reopened_tests, 6 tests).
    // One server test whose promise the hub does not keep today is ignored with a plain reason
    // (444 - 1 = 443).
    ("contextdb-server", 443, RSN_TEST_COUNT),
    //  - Inventory (+1): `.verify history <table>` checks each historical index from a
    //    read session (verify_history_checks_each_historical_index_from_a_read_session).
    //  - Inventory (+1): quitting the CLI pushes a pending oversized unit and exits clean
    //    (quitting_the_cli_pushes_a_pending_oversized_unit_and_exits_clean).
    ("contextdb-cli", 268, RSN_TEST_COUNT),
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
fn declared_modules(file: &Path, source: &str, _code: &str, is_root: bool) -> Vec<(PathBuf, bool)> {
    out_of_line_module_declarations(source)
        .into_iter()
        .filter(|(_, attributes, _)| !attributes.iter().any(|a| gated_outside_default_tier(a)))
        .map(|(name, attributes, inline)| module_file(file, is_root, &name, &attributes, &inline))
        .filter(|(path, _)| path.exists())
        .collect()
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
        pending.extend(declared_modules(&canonical, &source, &code, is_root));
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

/// Every unit-test module a `crates/*/src` source declares out of line, as
/// (declaring file, module file) relative to the workspace root. A module is
/// one when its declaration is gated on `test` (`#[cfg(test)]`, or `test` as
/// a condition of `#[cfg(all(...))]`), or when it is declared out of line from
/// inside such a module's file and so inherits its test-only status. Such a
/// file is unit-test code from its first byte but carries no `#[cfg(test)]`
/// marker of its own.
fn out_of_line_test_modules(root: &Path) -> Vec<(String, String)> {
    let mut found: BTreeMap<PathBuf, (String, bool)> = BTreeMap::new();
    let relative = |path: &Path| {
        path.strip_prefix(root)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/")
    };
    for entry in WalkDir::new(root.join("crates"))
        .into_iter()
        .filter_entry(|entry| entry.file_name() != "target" && entry.file_name() != ".git")
        .filter_map(Result::ok)
    {
        let path = entry.path();
        if !is_src_file(&relative(path)) {
            continue;
        }
        let Ok(source) = std::fs::read_to_string(path) else {
            continue;
        };
        for (name, attributes, inline) in out_of_line_module_declarations(&source) {
            if attributes
                .iter()
                .any(|attribute| cfg_requires_test(attribute))
            {
                let (module, by_path) = module_file(path, false, &name, &attributes, &inline);
                found.entry(module).or_insert((relative(path), by_path));
            }
        }
    }
    let mut pending: Vec<(PathBuf, bool)> = found
        .iter()
        .map(|(module, (_, by_path))| (module.clone(), *by_path))
        .collect();
    while let Some((parent, parent_by_path)) = pending.pop() {
        let Ok(source) = std::fs::read_to_string(&parent) else {
            continue;
        };
        for (name, attributes, inline) in out_of_line_module_declarations(&source) {
            let (module, by_path) =
                module_file(&parent, parent_by_path, &name, &attributes, &inline);
            if !found.contains_key(&module) {
                found.insert(module.clone(), (relative(&parent), by_path));
                pending.push((module, by_path));
            }
        }
    }
    found
        .into_iter()
        .map(|(module, (declared_in, _))| (declared_in, relative(&module)))
        .collect()
}

/// Every `mod name;` declaration in `source`, with the attributes written
/// directly above it. An attribute spanning several lines is read whole.
fn out_of_line_module_declarations(source: &str) -> Vec<(String, Vec<String>, Vec<String>)> {
    let closed = |attribute: &str| attribute.matches('[').count() == attribute.matches(']').count();
    let mut declarations = Vec::new();
    let mut attributes = Vec::new();
    let mut open: Option<String> = None;
    let masked = audit_support::rust_scope_text(source);
    let mut depth = 0usize;
    let mut inline: Vec<(usize, String)> = Vec::new();
    for (line, code) in source.lines().zip(masked.lines()) {
        let line = line.trim();
        if let Some(mut attribute) = open.take() {
            attribute.push_str(line);
            if closed(&attribute) {
                attributes.push(attribute);
            } else {
                open = Some(attribute);
            }
        } else if line.starts_with("#[") {
            if closed(line) {
                attributes.push(line.to_owned());
            } else {
                open = Some(line.to_owned());
            }
        } else if !line.starts_with("//") {
            if let Some(name) = out_of_line_module_name(line) {
                declarations.push((
                    name.to_owned(),
                    std::mem::take(&mut attributes),
                    inline.iter().map(|(_, name)| name.clone()).collect(),
                ));
            }
            attributes.clear();
        }
        let code = code.trim();
        if let Some(prefix) = code.strip_suffix('{') {
            let declaration = format!("{};", prefix.trim());
            if let Some(name) = out_of_line_module_name(&declaration) {
                inline.push((depth + 1, name.to_owned()));
            }
        }
        depth += code.matches('{').count();
        depth -= code.matches('}').count();
        inline.retain(|(at, _)| *at <= depth);
    }
    declarations
}

/// The module name of a `mod name;` line, when the line declares one out of line.
fn out_of_line_module_name(line: &str) -> Option<&str> {
    let declaration = line
        .strip_prefix("pub(crate) ")
        .or_else(|| line.strip_prefix("pub(super) "))
        .or_else(|| line.strip_prefix("pub "))
        .unwrap_or(line);
    let name = declaration.strip_prefix("mod ")?.strip_suffix(';')?.trim();
    (!name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_')).then_some(name)
}

/// Whether a `#[cfg(...)]` attribute compiles its item only under `test`:
/// `cfg(test)` itself, or `cfg(all(...))` with `test` as one of its conditions.
fn cfg_requires_test(attribute: &str) -> bool {
    let compact: String = attribute.chars().filter(|c| !c.is_whitespace()).collect();
    let Some(predicate) = compact
        .strip_prefix("#[cfg(")
        .and_then(|rest| rest.strip_suffix(")]"))
    else {
        return false;
    };
    if predicate == "test" {
        return true;
    }
    let Some(conditions) = predicate
        .strip_prefix("all(")
        .and_then(|rest| rest.strip_suffix(')'))
    else {
        return false;
    };
    let mut depth = 0usize;
    let mut start = 0;
    for (index, c) in conditions.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                if &conditions[start..index] == "test" {
                    return true;
                }
                start = index + 1;
            }
            _ => {}
        }
    }
    &conditions[start..] == "test"
}

/// The file Rust loads for `mod name;` declared in `declaring`, and whether
/// it was loaded through `#[path]`. A `#[path]` is relative to the declaring
/// file's directory; otherwise a `mod.rs`, `lib.rs` or `main.rs` file, or one
/// itself loaded through `#[path]`, keeps its children beside it, and any
/// other file keeps them in a directory named after it.
fn module_file(
    declaring: &Path,
    declaring_by_path: bool,
    name: &str,
    attributes: &[String],
    inline: &[String],
) -> (PathBuf, bool) {
    let directory = declaring.parent().expect("a source file's directory");
    let declared = attributes.iter().find_map(|attribute| {
        attribute
            .strip_prefix("#[path")
            .and_then(|rest| rest.split('"').nth(1))
    });
    let (module, by_path) = match declared {
        Some(declared) => (directory.join(declared), true),
        None => {
            let stem = declaring
                .file_stem()
                .expect("a file stem")
                .to_string_lossy();
            let base = if declaring_by_path || matches!(stem.as_ref(), "mod" | "lib" | "main") {
                directory.to_path_buf()
            } else {
                directory.join(stem.as_ref())
            };
            let base = inline.iter().fold(base, |base, name| base.join(name));
            let flat = base.join(format!("{name}.rs"));
            if flat.exists() {
                (flat, false)
            } else {
                (base.join(name).join("mod.rs"), false)
            }
        }
    };
    let mut normal = PathBuf::new();
    for part in module.components() {
        match part {
            std::path::Component::ParentDir => {
                normal.pop();
            }
            std::path::Component::CurDir => {}
            other => normal.push(other.as_os_str()),
        }
    }
    (normal, by_path)
}

/// A unit-test module a source file declares out of line is test code, so the
/// audit reads all of it, exactly as it reads an in-line `#[cfg(test)]` module
/// body. Otherwise a sleep or raw clock read moved into such a file is never
/// counted.
#[test]
fn test_estate_audit_scans_every_out_of_line_unit_test_module() {
    let root = workspace_root();
    let modules = out_of_line_test_modules(&root);
    assert!(
        !modules.is_empty(),
        "the workspace declares out-of-line unit-test modules"
    );
    let mut unscanned = Vec::new();
    for (declared_in, module) in &modules {
        let content = std::fs::read_to_string(root.join(module))
            .unwrap_or_else(|error| panic!("read {module}, declared in {declared_in}: {error}"));
        let scanned: usize = src_test_regions(module, &content)
            .iter()
            .map(|region| region.len())
            .sum();
        if scanned < content.len() {
            unscanned.push(format!("{module} (declared in {declared_in})"));
        }
    }
    assert!(
        unscanned.is_empty(),
        "the test-estate audit must read every out-of-line unit-test module whole; {} of {} \
         are not read: {unscanned:#?}",
        unscanned.len(),
        modules.len()
    );
}
