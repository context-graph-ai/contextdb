//! Machine-readable operational output (`--json`) for the server (usability jobs
//! USR-8 / AGT-8 / AGT-15).
//!
//! An agent enrolling a machine needs the enrollment ticket and the exact dial
//! command structurally, not scraped from human text. `--json` emits the
//! operational surface as one stable JSON object. `--show-ticket` bounds the run
//! (print and exit) so the test never depends on a long-lived socket.

use std::process::{Command, Stdio};

#[test]
fn show_ticket_json_emits_structured_object() {
    let out = Command::new(env!("CARGO_BIN_EXE_contextdb-server"))
        .args([
            "--db-path",
            ":memory:",
            "--tenant-id",
            "acme",
            "--show-ticket",
            "--json",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn contextdb-server");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "server must exit 0. stderr:\n{stderr}\nstdout:\n{stdout}"
    );

    // The JSON object is a single stdout line; other stdout content (if any) is
    // not valid JSON, so scan for the object.
    let v: serde_json::Value = stdout
        .lines()
        .rev()
        .find_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .filter(|v| v.is_object())
        .unwrap_or_else(|| panic!("no JSON object on stdout. stdout:\n{stdout}"));

    let ticket = v
        .get("enrollment_ticket")
        .and_then(|t| t.as_str())
        .unwrap_or_default();
    assert!(!ticket.is_empty(), "enrollment_ticket must be present: {v}");

    assert_eq!(
        v.get("tenant_id").and_then(|t| t.as_str()),
        Some("acme"),
        "tenant_id must round-trip: {v}"
    );

    let dial = v
        .get("dial_command")
        .and_then(|t| t.as_str())
        .unwrap_or_default();
    assert!(
        dial.contains("--tenant-id acme") && dial.contains(ticket),
        "dial_command must carry the tenant and the ticket: {v}"
    );

    assert!(
        v.get("endpoint").is_some(),
        "endpoint field must exist: {v}"
    );
}
