/// Harness filter for a child process of this test binary: `--exact` needs
/// the module-qualified test name.
pub(crate) fn child_test_filter(module_path: &str, fn_name: &str) -> String {
    match module_path.split_once("::") {
        Some((_, rest)) => format!("{rest}::{fn_name}"),
        None => fn_name.to_string(),
    }
}

#[path = "sql_surface/sql_surface_other.rs"]
mod sql_surface_other;
#[path = "sql_surface/sql_surface_tests.rs"]
mod sql_surface_tests;
