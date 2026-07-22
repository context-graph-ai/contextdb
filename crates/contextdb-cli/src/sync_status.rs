//! Sync status rendering: the operator-facing text for `.sync status` and
//! reconnect outcomes. Speaks transport-neutral sync-endpoint language — the
//! vocabulary the enterprise readiness rewrite builds on. No transport brand
//! names on the default path.

/// A view over one sync client's endpoint state, ready to render.
pub struct SyncEndpointStatusView {
    pub tenant_id: String,
    pub endpoint: String,
    pub transport_connected: bool,
    pub database_lsn: String,
    pub push_watermark: String,
    pub pull_watermark: String,
}

/// Render the `.sync status` text. Field vocabulary (stable, consumed by
/// operators and the enterprise adoption plan): `tenant`, `endpoint`,
/// `transport: connected|unreachable`, `database LSN`, push/pull watermarks.
pub fn render_sync_endpoint_status(view: &SyncEndpointStatusView) -> String {
    let transport = if view.transport_connected {
        "connected"
    } else {
        "unreachable"
    };
    format!(
        "Sync: tenant={}, endpoint={}\nTransport: {transport}\nDatabase LSN: {}\nPush watermark: LSN {}\nPull watermark: LSN {}",
        view.tenant_id, view.endpoint, view.database_lsn, view.push_watermark, view.pull_watermark
    )
}

/// Render the reconnect outcome line.
pub fn render_reconnect_outcome(connected: bool) -> String {
    if connected {
        "Reconnected to sync endpoint".to_string()
    } else {
        "Reconnection failed — sync endpoint unreachable".to_string()
    }
}
