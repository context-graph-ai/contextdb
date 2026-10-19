#![allow(clippy::duplicate_mod)] // Shared fixture modules are included by test families.

#[path = "custody/custody_inspection_contract.rs"]
mod custody_inspection_contract;
#[path = "custody/custody_protocol_contract.rs"]
mod custody_protocol_contract;
#[path = "custody/custody_purge_and_discard_contract.rs"]
mod custody_purge_and_discard_contract;
#[path = "custody/delivery_manifest_contract.rs"]
mod delivery_manifest_contract;
#[path = "custody/delivery_outcome_contract.rs"]
mod delivery_outcome_contract;
#[cfg(feature = "nightly-tests")]
#[path = "schema/find_show_status_stay_fast_while_schema_replays.rs"]
mod find_show_status_stay_fast_while_schema_replays;
#[path = "schema/schema_replay_is_proportional_to_affected_tables.rs"]
mod schema_replay_is_proportional_to_affected_tables;
#[path = "custody/server_resource_policy_contract_tests.rs"]
mod server_resource_policy_contract_tests;
#[path = "custody/tenant_table_policy_contract.rs"]
mod tenant_table_policy_contract;
#[path = "schema/the_whole_startup_schema_installs_in_one_transaction.rs"]
mod the_whole_startup_schema_installs_in_one_transaction;
