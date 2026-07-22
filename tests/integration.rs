#[path = "integration/acceptance_retention.rs"]
mod acceptance_retention;
#[path = "integration/alter_table_tests.rs"]
mod alter_table_tests;
#[path = "integration/anti_tests.rs"]
mod anti_tests;
#[cfg(feature = "nats-tests")]
#[path = "integration/auto_sync_update_push_visibility_tests.rs"]
mod auto_sync_update_push_visibility_tests;
#[path = "integration/boundary_conditions.rs"]
mod boundary_conditions;
#[path = "integration/bounded_tables_tests.rs"]
mod bounded_tables_tests;
#[path = "integration/cli_spawn.rs"]
mod cli_spawn;
#[path = "integration/column_level_immutable_tests.rs"]
mod column_level_immutable_tests;
#[path = "integration/commit_failed_observer_tests.rs"]
mod commit_failed_observer_tests;
#[path = "integration/commit_write_volume_scaling_tests.rs"]
mod commit_write_volume_scaling_tests;
#[path = "integration/composite_foreign_key_tests.rs"]
mod composite_foreign_key_tests;
#[path = "integration/ddl_sync_tests.rs"]
mod ddl_sync_tests;
#[path = "integration/gate_a_primitives.rs"]
mod gate_a_primitives;
#[cfg(feature = "nats-tests")]
#[path = "integration/gate_a_sync.rs"]
mod gate_a_sync;
#[path = "integration/gate_b_workflows.rs"]
mod gate_b_workflows;
#[path = "integration/gate_c_d_lifecycle.rs"]
mod gate_c_d_lifecycle;
#[path = "integration/golden_output.rs"]
mod golden_output;
#[path = "integration/helpers.rs"]
mod helpers;
#[path = "integration/cross_cutting_acceptance_tests.rs"]
mod cross_cutting_acceptance_tests;
#[path = "integration/hnsw_tests.rs"]
mod hnsw_tests;
#[path = "integration/indexed_scan_filter_composite_routing_tests.rs"]
mod indexed_scan_filter_composite_routing_tests;
#[path = "integration/indexed_scan_filter_tests.rs"]
mod indexed_scan_filter_tests;
#[path = "integration/job_level.rs"]
mod job_level;
#[path = "integration/memory_accounting_tests.rs"]
mod memory_accounting_tests;
#[path = "integration/named_vector_indexes_tests.rs"]
mod named_vector_indexes_tests;
#[cfg(target_os = "linux")]
#[path = "integration/peak_rss_harness.rs"]
mod peak_rss_harness;
#[path = "integration/persistence_tests.rs"]
mod persistence_tests;
#[path = "integration/plugin_tests.rs"]
mod plugin_tests;
#[path = "integration/rank_policy_tests.rs"]
mod rank_policy_tests;
#[path = "integration/retention_tests.rs"]
mod retention_tests;
#[path = "integration/safety_integrity.rs"]
mod safety_integrity;
#[path = "integration/state_propagation.rs"]
mod state_propagation;
#[path = "integration/storage_scale_hardening_tests.rs"]
mod storage_scale_hardening_tests;
#[path = "integration/subscription_tests.rs"]
mod subscription_tests;
#[cfg(feature = "nats-tests")]
#[path = "integration/sync_relay_trigger_tables_tests.rs"]
mod sync_relay_trigger_tables_tests;
#[cfg(feature = "nats-tests")]
#[path = "integration/sync_server_nonblocking_apply.rs"]
mod sync_server_nonblocking_apply;
#[path = "integration/work_ledger_tests.rs"]
mod work_ledger_tests;
