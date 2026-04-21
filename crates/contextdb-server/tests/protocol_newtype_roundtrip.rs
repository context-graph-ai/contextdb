// ======== T7b ========

#[test]
fn sync_protocol_types_roundtrip_under_rmp_serde() {
    use contextdb_core::{Lsn, RowId};
    use contextdb_server::protocol::{
        Envelope, PullRequest, PullResponse, WireApplyResult, WireEdgeChange, WireRowChange,
        WireVectorChange,
    };

    // ---- PullRequest ----
    let original = PullRequest {
        since_lsn: Lsn(42),
        ..PullRequest::default()
    };
    let bytes = rmp_serde::to_vec(&original).expect("PullRequest must encode under rmp_serde");
    let decoded: PullRequest =
        rmp_serde::from_slice(&bytes).expect("PullRequest must decode under rmp_serde");
    assert_eq!(decoded, original, "PullRequest round-trip inequality");

    // ---- PullResponse ----
    let original = PullResponse {
        cursor: Some(Lsn(99)),
        ..PullResponse::default()
    };
    let bytes = rmp_serde::to_vec(&original).expect("PullResponse encode");
    let decoded: PullResponse = rmp_serde::from_slice(&bytes).expect("PullResponse decode");
    assert_eq!(decoded, original, "PullResponse round-trip inequality");

    // ---- WireRowChange ----
    let original = WireRowChange {
        lsn: Lsn(7),
        ..WireRowChange::default()
    };
    let bytes = rmp_serde::to_vec(&original).expect("WireRowChange encode");
    let decoded: WireRowChange = rmp_serde::from_slice(&bytes).expect("WireRowChange decode");
    assert_eq!(decoded, original, "WireRowChange round-trip inequality");

    // ---- WireEdgeChange ----
    let original = WireEdgeChange {
        lsn: Lsn(8),
        ..WireEdgeChange::default()
    };
    let bytes = rmp_serde::to_vec(&original).expect("WireEdgeChange encode");
    let decoded: WireEdgeChange = rmp_serde::from_slice(&bytes).expect("WireEdgeChange decode");
    assert_eq!(decoded, original, "WireEdgeChange round-trip inequality");

    // ---- WireVectorChange ----
    let original = WireVectorChange {
        row_id: RowId(123),
        lsn: Lsn(9),
        ..WireVectorChange::default()
    };
    let bytes = rmp_serde::to_vec(&original).expect("WireVectorChange encode");
    let decoded: WireVectorChange = rmp_serde::from_slice(&bytes).expect("WireVectorChange decode");
    assert_eq!(decoded, original, "WireVectorChange round-trip inequality");

    // ---- WireApplyResult ----
    let original = WireApplyResult {
        new_lsn: Lsn(100),
        applied_rows: 3,
        skipped_rows: 0,
        conflicts: vec![],
    };
    let bytes = rmp_serde::to_vec(&original).expect("WireApplyResult encode");
    let decoded: WireApplyResult = rmp_serde::from_slice(&bytes).expect("WireApplyResult decode");
    assert_eq!(decoded, original, "WireApplyResult round-trip inequality");

    // ---- Envelope ----
    // Envelope has explicit fields `version: u8`, `message_type: MessageType`,
    // `payload: Vec<u8>`. Build via `default_pull_request()` which selects the
    // `MessageType::PullRequest` flavor — stub returns WRONG `version = 2`, so
    // TU7 goes RED; T7b only asserts round-trip identity, so it passes regardless
    // of what `version` value the stub picks (identity equality is version-agnostic).
    use contextdb_server::protocol::MessageType;
    let original = Envelope::default_pull_request();
    let bytes = rmp_serde::to_vec(&original).expect("Envelope encode");
    let decoded: Envelope = rmp_serde::from_slice(&bytes).expect("Envelope decode");
    assert_eq!(decoded, original, "Envelope round-trip inequality");
    assert!(
        matches!(decoded.message_type, MessageType::PullRequest),
        "default_pull_request must produce MessageType::PullRequest",
    );

    // ---- u64-mirror cross-decode proves #[serde(transparent)] byte-compat ----
    #[derive(serde::Serialize)]
    struct PullRequestMirror {
        since_lsn: u64,
    }
    let mirror = PullRequestMirror { since_lsn: 42 };
    let mirror_bytes = rmp_serde::to_vec(&mirror).expect("PullRequestMirror encode");
    let decoded_as_typed: PullRequest = rmp_serde::from_slice(&mirror_bytes)
        .expect("typed PullRequest must decode from u64-mirror bytes");
    assert_eq!(
        decoded_as_typed.since_lsn,
        Lsn(42),
        "u64-mirror bytes must decode into Lsn(42)"
    );
}

// ======== TU7 ========

#[test]
fn protocol_version_stays_at_one() {
    use contextdb_server::protocol::Envelope;

    // Construct the default envelope used by the pull path.
    let env = Envelope::default_pull_request();
    assert_eq!(
        env.version, 1,
        "Envelope.version must remain 1 across the TxId retype; got {}",
        env.version
    );

    // Round-trip through rmp_serde; decoded version must still be 1.
    let bytes = rmp_serde::to_vec(&env).expect("serialize Envelope via rmp_serde");
    let decoded: Envelope =
        rmp_serde::from_slice(&bytes).expect("deserialize Envelope via rmp_serde");
    assert_eq!(
        decoded.version, 1,
        "round-tripped Envelope.version must remain 1; got {}",
        decoded.version
    );

    // The bytes must also contain the integer 1 as the version field's serialized form.
    // rmp_serde encodes small u32 as a one-byte positive-fixint (0x01). Assert the payload
    // contains this byte in any field-ordering-independent form.
    assert!(
        bytes.contains(&0x01),
        "serialized Envelope must contain the positive-fixint byte 0x01 (version=1); bytes = {:?}",
        bytes
    );
}
