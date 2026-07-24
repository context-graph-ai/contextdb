//! Frozen wire-bytes regression guard for `PROTOCOL_VERSION` 6.
//!
//! Pure encode/decode against fixed, fully deterministic fixtures — no
//! broker, no server, no async runtime. It used to live inside
//! `stale_restore_tests.rs`, whose `[[test]]` entry carries
//! `required-features = ["nats"]` for its OTHER (genuinely NATS-dependent)
//! tests; `cargo test --workspace` skips a `required-features`-gated binary
//! entirely, so this guard never ran in the default suite even though it
//! needs no broker at all. This file has no such gate and is auto-discovered
//! like any other `tests/*.rs` file, so it runs on every default
//! `cargo test`.

use contextdb_core::{Incarnation, Lsn, Value};
use contextdb_server::protocol::{
    MessageType, PullRequest, PullResponse, PushRequest, PushResponse, WireApplyResult,
    WireChangeSet, WireNaturalKey, WireRowChange, decode, encode,
};
use std::collections::HashMap;

// ======== sr7 — REGRESSION GUARD: the wire bytes are frozen at PROTOCOL_VERSION 6 ========
//
// These hex constants are the actual encoder output for a fixed, fully
// deterministic fixture (single-entry maps only). They are a snapshot guard: any
// field add/remove/reorder, MessageType name change, or PROTOCOL_VERSION bump
// changes these bytes, so an UNINTENDED wire change fails here loudly. The
// arrival-ordering + cursor-source-binding change deliberately reshaped this
// surface — `WireRowChange` gained a trailing `arrival` field, `PullResponse`
// gained a trailing `source` field, and every envelope's version byte went 5→6
// (a clean break; a peer speaking the old version is rejected at the envelope
// version check, not by struct shape) — so the constants were regenerated to
// the true new v6 encoding and re-frozen here.

fn wire_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

const PUSH_REQUEST_WIRE: &str = "9306ab5075736852657175657374dc002acc92cc95cc90cc90cc91cc97cca174cc93cca26964cc81cca5496e74363407cc90cc81cca26964cc81cca5496e74363407ccc207ccc0ccc0cc90cc90cc920000";
const PUSH_RESPONSE_WIRE: &str = "9306ac50757368526573706f6e736597cc92cc940100cc9007ccc0";
const PULL_REQUEST_WIRE: &str = "9306ab50756c6c5265717565737495cc922acccd01ccf4";
const PULL_RESPONSE_WIRE: &str =
    "9306ac50756c6c526573706f6e73659acc94cc95cc90cc90cc90cc90cc90ccc22accc0";

#[test]
fn sr7_guard_push_and_pull_wire_bytes_are_frozen() {
    // PushRequest: one row, single-entry values map (deterministic encoding).
    let row = WireRowChange {
        table: "t".to_string(),
        natural_key: WireNaturalKey {
            column: "id".to_string(),
            value: Value::Int64(7),
            rest: Vec::new(),
        },
        values: HashMap::from([("id".to_string(), Value::Int64(7))]),
        deleted: false,
        lsn: Lsn(7),
        created_at: None,
        arrival: None,
    };
    let push_request = PushRequest {
        changeset: WireChangeSet {
            ddl: Vec::new(),
            ddl_lsn: Vec::new(),
            rows: vec![row],
            edges: Vec::new(),
            vectors: Vec::new(),
        },
        incarnation: Incarnation::default(),
    };
    let push_request_bytes = encode(MessageType::PushRequest, &push_request).unwrap();
    assert_eq!(
        wire_hex(&push_request_bytes),
        PUSH_REQUEST_WIRE,
        "PushRequest wire bytes changed — old servers cannot decode a reshaped \
         push request; existing subjects must stay byte-identical"
    );
    let envelope = decode(&push_request_bytes).unwrap();
    let decoded: PushRequest = rmp_serde::from_slice(&envelope.payload).unwrap();
    assert_eq!(
        decoded, push_request,
        "pinned PushRequest bytes must round-trip"
    );

    // PushResponse.
    let push_response = PushResponse {
        result: Some(WireApplyResult {
            applied_rows: 1,
            skipped_rows: 0,
            conflicts: Vec::new(),
            new_lsn: Lsn(7),
        }),
        error: None,
    };
    let push_response_bytes = encode(MessageType::PushResponse, &push_response).unwrap();
    assert_eq!(
        wire_hex(&push_response_bytes),
        PUSH_RESPONSE_WIRE,
        "PushResponse wire bytes changed — old clients cannot decode a reshaped \
         push reply (trailing fields fail with LengthMismatch); regression data \
         belongs on the status subject, not here"
    );
    let envelope = decode(&push_response_bytes).unwrap();
    let decoded: PushResponse = rmp_serde::from_slice(&envelope.payload).unwrap();
    assert_eq!(
        decoded, push_response,
        "pinned PushResponse bytes must round-trip"
    );

    // PullRequest.
    let pull_request = PullRequest {
        since_lsn: Lsn(42),
        max_entries: Some(500),
    };
    let pull_request_bytes = encode(MessageType::PullRequest, &pull_request).unwrap();
    assert_eq!(
        wire_hex(&pull_request_bytes),
        PULL_REQUEST_WIRE,
        "PullRequest wire bytes changed — old servers cannot decode a reshaped \
         pull request"
    );
    let envelope = decode(&pull_request_bytes).unwrap();
    let decoded: PullRequest = rmp_serde::from_slice(&envelope.payload).unwrap();
    assert_eq!(
        decoded, pull_request,
        "pinned PullRequest bytes must round-trip"
    );

    // PullResponse.
    let pull_response = PullResponse {
        changeset: WireChangeSet::default(),
        has_more: false,
        cursor: Some(Lsn(42)),
        source: None,
    };
    let pull_response_bytes = encode(MessageType::PullResponse, &pull_response).unwrap();
    assert_eq!(
        wire_hex(&pull_response_bytes),
        PULL_RESPONSE_WIRE,
        "PullResponse wire bytes changed — old clients cannot decode a reshaped \
         pull reply; regression data belongs on the status subject, not here"
    );
    let envelope = decode(&pull_response_bytes).unwrap();
    let decoded: PullResponse = rmp_serde::from_slice(&envelope.payload).unwrap();
    assert_eq!(
        decoded, pull_response,
        "pinned PullResponse bytes must round-trip"
    );
}
