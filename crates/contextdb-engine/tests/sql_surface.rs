/// Harness filter for a child process of this test binary: `--exact` needs
/// the module-qualified test name.
pub(crate) fn child_test_filter(module_path: &str, fn_name: &str) -> String {
    match module_path.split_once("::") {
        Some((_, rest)) => format!("{rest}::{fn_name}"),
        None => fn_name.to_string(),
    }
}

#[path = "sql_surface/an_edge_keeps_the_identity_its_table_declares.rs"]
mod an_edge_keeps_the_identity_its_table_declares;
#[cfg(not(feature = "test-seams"))]
#[path = "sql_surface/production_layout_inventory.rs"]
mod production_layout_inventory;
#[path = "sql_surface/purging_an_edge_row_erases_exactly_its_own_graph_witness.rs"]
mod purging_an_edge_row_erases_exactly_its_own_graph_witness;
#[path = "sql_surface/sql_surface_other.rs"]
mod sql_surface_other;
#[path = "sql_surface/sql_surface_tests.rs"]
mod sql_surface_tests;
#[path = "sql_surface/the_same_edge_through_both_doors_is_one_edge.rs"]
mod the_same_edge_through_both_doors_is_one_edge;
