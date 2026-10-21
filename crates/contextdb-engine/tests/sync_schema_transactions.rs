/// `--exact` expects a module-qualified name without the integration-test
/// crate prefix printed by `module_path!()`.
#[cfg(unix)]
pub(crate) fn child_test_filter(module_path: &str, fn_name: &str) -> String {
    match module_path.split_once("::") {
        Some((_, rest)) => format!("{rest}::{fn_name}"),
        None => fn_name.to_string(),
    }
}

#[cfg(unix)]
#[path = "sync/a_received_schema_transaction_never_publishes_a_prefix.rs"]
mod a_received_schema_transaction_never_publishes_a_prefix;
