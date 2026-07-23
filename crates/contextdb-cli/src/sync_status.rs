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

/// The same status as a JSON document, for `--json`. Carries every field the
/// text renders, under the stable key names the text uses as labels; log
/// sequence numbers are JSON numbers, not strings, so a consumer can compare
/// them without parsing.
pub fn sync_endpoint_status_json(view: &SyncEndpointStatusView) -> serde_json::Value {
    serde_json::json!({
        "configured": true,
        "tenant": view.tenant_id,
        "endpoint": view.endpoint,
        "transport": if view.transport_connected { "connected" } else { "unreachable" },
        "database_lsn": lsn_value(&view.database_lsn),
        "push_watermark": lsn_value(&view.push_watermark),
        "pull_watermark": lsn_value(&view.pull_watermark),
    })
}

/// A log sequence number as a number. The view carries what the client
/// rendered; anything that is not a plain integer is passed through verbatim
/// rather than silently zeroed.
fn lsn_value(raw: &str) -> serde_json::Value {
    raw.parse::<u64>()
        .map(serde_json::Value::from)
        .unwrap_or_else(|_| serde_json::Value::from(raw))
}

/// Render the reconnect outcome line.
pub fn render_reconnect_outcome(connected: bool) -> String {
    if connected {
        "Reconnected to sync endpoint".to_string()
    } else {
        "Reconnection failed — sync endpoint unreachable".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The machine status carries every fact the text carries, and log
    /// sequence numbers arrive as numbers a consumer can compare without
    /// parsing.
    #[test]
    fn sync_status_json_reports_the_same_values_as_the_text() {
        let view = SyncEndpointStatusView {
            tenant_id: "tenant-a".to_string(),
            endpoint: "ticket-abc123".to_string(),
            transport_connected: true,
            database_lsn: "42".to_string(),
            push_watermark: "40".to_string(),
            pull_watermark: "41".to_string(),
        };
        let text = render_sync_endpoint_status(&view);
        let json = sync_endpoint_status_json(&view);

        assert_eq!(json["configured"], serde_json::json!(true));
        assert_eq!(json["tenant"], serde_json::json!("tenant-a"));
        assert_eq!(json["endpoint"], serde_json::json!("ticket-abc123"));
        assert_eq!(json["transport"], serde_json::json!("connected"));
        assert_eq!(json["database_lsn"], serde_json::json!(42));
        assert_eq!(json["push_watermark"], serde_json::json!(40));
        assert_eq!(json["pull_watermark"], serde_json::json!(41));

        for (field, value) in [
            ("database_lsn", 42u64),
            ("push_watermark", 40),
            ("pull_watermark", 41),
        ] {
            assert!(
                json[field].is_u64(),
                "{field} must be a JSON number, not a string: {json}"
            );
            assert!(
                text.contains(&value.to_string()),
                "the text renders the same {field} value"
            );
        }
        assert!(text.contains("tenant=tenant-a") && text.contains("Transport: connected"));

        let unreachable = SyncEndpointStatusView {
            transport_connected: false,
            ..view
        };
        assert_eq!(
            sync_endpoint_status_json(&unreachable)["transport"],
            serde_json::json!("unreachable")
        );
    }
}
