/// Returns a scope suffix derived from the current tokio runtime ID.
/// Each `#[tokio::test]` creates a separate runtime, so this isolates
/// NATS subjects when tests run in parallel with the same tenant_id.
fn runtime_scope_suffix() -> String {
    tokio::runtime::Handle::try_current()
        .ok()
        .map(|h| format!(".{:?}", h.id()))
        .unwrap_or_default()
}

pub fn push_subject(tenant_id: &str) -> String {
    format!("sync.{tenant_id}.push{}", runtime_scope_suffix())
}

pub fn pull_subject(tenant_id: &str) -> String {
    format!("sync.{tenant_id}.pull{}", runtime_scope_suffix())
}

pub fn command_subject(tenant_id: &str, command: &str) -> String {
    format!(
        "sync.{tenant_id}.command.{command}{}",
        runtime_scope_suffix()
    )
}
