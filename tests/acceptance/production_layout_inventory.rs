//! Built without test seams, this binary still runs every acceptance test
//! that needs none: a module that mixes seam-using and production-shaped
//! tests is not left out of the production layout as a whole.

/// Every test in these modules that needs no test seam: it uses only the
/// public engine surface, directly and through its local helpers.
const PRODUCTION_SHAPED: &[&str] = &[
    "precommit_context_guard::t4_01_select_excludes_other_contexts",
    "precommit_context_guard::t4_02_insert_into_other_context_rejected",
    "precommit_context_guard::t4_03_update_other_context_rejected",
    "precommit_context_guard::t4_04_delete_other_context_rejected",
    "precommit_context_guard::t4_05_graph_traversal_excludes_other_contexts",
    "precommit_context_guard::t4_06_vector_query_excludes_other_contexts",
    "precommit_context_guard::t4_07_multi_context_set_sees_subset_excludes_others",
    "precommit_context_guard::t4_07b_scoped_view_over_open_handle_filters_without_second_open",
    "precommit_context_guard::t4_07c_scoped_view_cannot_widen_or_clear_scope",
    "precommit_context_guard::t4_07d_scoped_view_observes_owner_close",
    "precommit_context_guard::t4_08_admin_handle_sees_all_contexts",
    "precommit_context_guard::t4_09_table_without_context_id_unaffected",
    "precommit_context_guard::t4_10_insert_context_id_omitted_handle_one_context_auto_stamps",
    "precommit_context_guard::t4_11_insert_context_id_omitted_handle_multi_or_zero_context_rejects",
    "precommit_context_guard::t4_12_update_post_image_label_flip_rejected",
    "precommit_context_guard::t4_13_scan_filter_predicate_sees_only_context_visible_rows",
    "precommit_context_guard::t4_15_state_propagation_respects_context_on_children",
    "precommit_context_guard::t4_16_graph_edge_metadata_context_gate_hides_visible_endpoint_edge",
    "precommit_context_guard::t4_17_changes_since_filters_context_rows_and_schema",
    "precommit_context_guard::t4_17b_changes_since_emits_delete_when_row_moves_out_of_context_after_ddl_lsn_gap",
    "precommit_context_guard::t4_19_scoped_raw_helpers_respect_context_boundary",
    "precommit_context_guard::t4_20_scoped_sql_ddl_rejected",
    "precommit_context_guard::t4_21_scoped_graph_only_edges_require_relational_metadata",
    "event_bus::t5_01_route_invokes_sink_with_row_values",
    "event_bus::t5_02_severity_filter_respected",
    "event_bus::t5_03_sink_isolation_slow_a_does_not_block_b",
    "event_bus::t5_08_subscribe_primitive_still_works",
    "event_bus::t5_13_sink_excludes_acl_denied_rows",
    "event_bus::t5_15_durable_acl_replay_uses_route_time_gate_snapshot_after_schema_drop",
    "event_bus::t5_16_principal_scoped_sink_uses_handle_principal_when_registration_omits_one",
    "event_bus::t5_22_execute_in_tx_event_bus_ddl_is_not_a_noop",
    "event_bus::t5_25_event_bus_ddl_rollback_does_not_leak_sink",
    "event_bus::t5_26_event_bus_ddl_in_tx_routes_rows_from_same_commit",
    "event_bus::t5_38_scoped_handle_register_sink_unknown_sink_returns_err",
    "event_bus_self_join::t34_08_explicit_close_then_drop_is_idempotent_no_double_join",
    "txid_monotonic_concurrent_commits::t18_01_concurrent_committers_stamp_distinct_txids",
    "txid_monotonic_concurrent_commits::t18_02_pagination_cursor_after_concurrent_commits_misses_no_rows",
    "txid_monotonic_concurrent_commits::t18_03_auto_stamped_txid_order_matches_commit_lsn_order",
    "txid_monotonic_concurrent_commits::t18_04_caller_supplied_txid_above_watermark_rejected",
    "txid_monotonic_concurrent_commits::t18_06_commit_time_upsert_rewrites_txid_post_image_to_canonical",
    "txid_monotonic_concurrent_commits::t18_07_commit_time_rewrites_nullable_txid_update_to_canonical",
    "storage_scale_hardening::ssa_commit_contract_journey",
    "storage_scale_hardening::ssa_crash_recovery_journey",
    "storage_scale_hardening::ssa_explain_schema_parity_journey",
    "storage_scale_hardening::ssa_open_locality_journey",
];

#[test]
fn the_production_layout_runs_every_production_shaped_test_of_its_mixed_modules() {
    let output = std::process::Command::new(std::env::current_exe().expect("this test binary"))
        .args(["--list", "--format", "terse"])
        .output()
        .expect("list the tests this binary runs");
    assert!(
        output.status.success(),
        "listing this binary's tests failed"
    );
    let listed: std::collections::BTreeSet<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.strip_suffix(": test").map(str::to_owned))
        .collect();
    let missing: Vec<&str> = PRODUCTION_SHAPED
        .iter()
        .copied()
        .filter(|name| !listed.contains(*name))
        .collect();
    assert!(
        missing.is_empty(),
        "the production layout of the acceptance tests must run every test that needs no test \
         seam; {} of {} are not built: {missing:#?}",
        missing.len(),
        PRODUCTION_SHAPED.len()
    );
}
