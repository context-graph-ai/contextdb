pub fn push_subject(tenant_id: &str) -> String {
    format!("sync.{tenant_id}.push")
}

pub fn pull_subject(tenant_id: &str) -> String {
    format!("sync.{tenant_id}.pull")
}

pub fn command_subject(tenant_id: &str, command: &str) -> String {
    format!("sync.{tenant_id}.command.{command}")
}

pub fn status_subject(tenant_id: &str) -> String {
    format!("sync.{tenant_id}.status")
}

// Manifested units share push_subject with ordinary row traffic.

pub fn binding_subject(tenant_id: &str) -> String {
    format!("sync.{tenant_id}.delivery.bind")
}

pub fn delivery_outcomes_subject(tenant_id: &str) -> String {
    format!("sync.{tenant_id}.delivery.outcomes")
}
