#[path = "sql/a_local_create_table_never_replaces_an_existing_table.rs"]
mod a_local_create_table_never_replaces_an_existing_table;
#[path = "sql/a_write_bound_through_a_full_index_prefix_examines_only_its_rows.rs"]
mod a_write_bound_through_a_full_index_prefix_examines_only_its_rows;
#[path = "sql/composite_index_elides_a_descending_sort.rs"]
mod composite_index_elides_a_descending_sort;
#[path = "sql/composite_index_elides_a_still_needed_sort_over_whole_key.rs"]
mod composite_index_elides_a_still_needed_sort_over_whole_key;
#[path = "sql/composite_primary_key_serves_its_prefix.rs"]
mod composite_primary_key_serves_its_prefix;
#[path = "sql/descending_sort_disclosure_matches_every_route.rs"]
mod descending_sort_disclosure_matches_every_route;
#[path = "sql/four_cte_graph_composition_runs_on_a_two_mebibyte_thread.rs"]
mod four_cte_graph_composition_runs_on_a_two_mebibyte_thread;
#[path = "sql/in_list_scope_uses_the_declared_composite_index.rs"]
mod in_list_scope_uses_the_declared_composite_index;
#[path = "sql/index_ranking_is_a_total_order.rs"]
mod index_ranking_is_a_total_order;
#[path = "sql/ordered_and_bounded_finite_index_runs.rs"]
mod ordered_and_bounded_finite_index_runs;
#[path = "schema/schema_ddl_and_rows_in_one_transaction_are_still_refused.rs"]
mod schema_ddl_and_rows_in_one_transaction_are_still_refused;
#[path = "sql/sql_tests.rs"]
mod sql_tests;
#[path = "sql/unique_index_one_row_claim_is_honest.rs"]
mod unique_index_one_row_claim_is_honest;
