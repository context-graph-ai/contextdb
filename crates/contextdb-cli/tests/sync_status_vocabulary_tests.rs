//! The operator-facing sync status speaks
//! transport-neutral sync-endpoint language on the default path. This is the
//! stable vocabulary the enterprise readiness rewrite builds on.

use contextdb_cli::sync_status::{
    SyncEndpointStatusView, render_reconnect_outcome, render_sync_endpoint_status,
};

fn sample_view(connected: bool) -> SyncEndpointStatusView {
    SyncEndpointStatusView {
        tenant_id: "tenant-a".to_string(),
        endpoint: "ticket-abc123".to_string(),
        transport_connected: connected,
        database_lsn: "42".to_string(),
        push_watermark: "40".to_string(),
        pull_watermark: "41".to_string(),
    }
}

#[test]
fn sync_status_output_speaks_endpoint_language() {
    let rendered = render_sync_endpoint_status(&sample_view(true));
    let lower = rendered.to_ascii_lowercase();

    assert!(
        lower.contains("tenant=tenant-a"),
        "status must carry the tenant, got: {rendered}"
    );
    assert!(
        lower.contains("endpoint=ticket-abc123"),
        "status must name the sync endpoint, got: {rendered}"
    );
    assert!(
        lower.contains("transport: connected"),
        "status must report transport connectivity in neutral vocabulary, got: {rendered}"
    );
    for line in [
        "database lsn: 42",
        "push watermark: lsn 40",
        "pull watermark: lsn 41",
    ] {
        assert!(
            lower.contains(line),
            "status must keep the watermark lines ({line}), got: {rendered}"
        );
    }
    assert!(
        !lower.contains("url="),
        "URL vocabulary is replaced by endpoint vocabulary, got: {rendered}"
    );
}

#[test]
fn sync_status_reports_unreachable_in_neutral_vocabulary() {
    let rendered = render_sync_endpoint_status(&sample_view(false));
    let lower = rendered.to_ascii_lowercase();
    assert!(
        lower.contains("transport: unreachable"),
        "a down endpoint must render as 'transport: unreachable', got: {rendered}"
    );
}

#[test]
fn reconnect_outcome_speaks_endpoint_language() {
    let ok = render_reconnect_outcome(true).to_ascii_lowercase();
    assert!(
        ok.contains("reconnected") && ok.contains("endpoint"),
        "reconnect success must speak endpoint language, got: {ok}"
    );
    let failed = render_reconnect_outcome(false).to_ascii_lowercase();
    assert!(
        failed.contains("unreachable") && failed.contains("endpoint"),
        "reconnect failure must speak endpoint language, got: {failed}"
    );
}
