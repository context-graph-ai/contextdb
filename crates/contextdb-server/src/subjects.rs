pub fn push_subject(tenant_id: &str) -> String {
    format!("sync.{tenant_id}.push")
}

pub fn pull_subject(tenant_id: &str) -> String {
    format!("sync.{tenant_id}.pull")
}

pub fn command_subject(tenant_id: &str, command: &str) -> String {
    format!("sync.{tenant_id}.command.{command}")
}
