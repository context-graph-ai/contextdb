#[path = "integration/acceptance_retention.rs"]
mod acceptance_retention;
#[path = "integration/alter_table_tests.rs"]
mod alter_table_tests;
#[path = "integration/anti_tests.rs"]
mod anti_tests;
#[path = "integration/boundary_conditions.rs"]
mod boundary_conditions;
#[path = "integration/column_level_immutable_tests.rs"]
mod column_level_immutable_tests;
#[path = "integration/ddl_sync_tests.rs"]
mod ddl_sync_tests;
#[path = "integration/gate_a_primitives.rs"]
mod gate_a_primitives;
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
#[path = "integration/hiring_criteria.rs"]
mod hiring_criteria;
#[path = "integration/hnsw_tests.rs"]
mod hnsw_tests;
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
#[path = "integration/subscription_tests.rs"]
mod subscription_tests;
