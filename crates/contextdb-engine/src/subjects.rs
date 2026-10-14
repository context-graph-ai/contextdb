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

/// Format a native subject from borrowed pieces under its original invoice.
/// Existing public subject constructors retain their legacy signatures.
#[allow(
    dead_code,
    reason = "native delivery code is staged ahead of its first non-test caller"
)]
pub(crate) fn read_bound_subject(
    owner: contextdb_core::read_memory::ReadMemoryOwner,
    tenant_id: &str,
    suffix: &str,
) -> crate::transport::WireResult<crate::transport::NativeWireString> {
    crate::transport::NativeWireString::try_from_parts(owner, &["sync.", tenant_id, ".", suffix])
}
