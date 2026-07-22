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
    // `MessageType::PullRequest` flavor. T7b asserts round-trip identity; TU7
    // below asserts the concrete protocol version.
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
fn protocol_version_uses_current_constant_for_structured_constraint_wire() {
    use contextdb_server::protocol::{Envelope, PROTOCOL_VERSION};

    // Construct the default envelope used by the pull path.
    let env = Envelope::default_pull_request();
    assert_eq!(
        env.version, PROTOCOL_VERSION,
        "Envelope.version must use the named-vector protocol version; got {}",
        env.version
    );

    // Round-trip through rmp_serde; decoded version must still match the protocol constant.
    let bytes = rmp_serde::to_vec(&env).expect("serialize Envelope via rmp_serde");
    let decoded: Envelope =
        rmp_serde::from_slice(&bytes).expect("deserialize Envelope via rmp_serde");
    assert_eq!(
        decoded.version, PROTOCOL_VERSION,
        "round-tripped Envelope.version must match PROTOCOL_VERSION; got {}",
        decoded.version
    );

    // The bytes must also contain the protocol version field's serialized form.
    // rmp_serde encodes small u8 as a one-byte positive-fixint. Assert the payload
    // contains this byte in any field-ordering-independent form.
    assert!(
        bytes.contains(&PROTOCOL_VERSION),
        "serialized Envelope must contain PROTOCOL_VERSION={PROTOCOL_VERSION}; bytes = {:?}",
        bytes
    );
}

#[test]
fn wire_changeset_requires_ddl_lsn_field_for_current_protocol() {
    use contextdb_server::protocol::{
        WireChangeSet, WireDdlChange, WireEdgeChange, WireRowChange, WireVectorChange,
    };

    #[derive(serde::Serialize)]
    struct LegacyWireChangeSetV2 {
        ddl: Vec<WireDdlChange>,
        rows: Vec<WireRowChange>,
        edges: Vec<WireEdgeChange>,
        vectors: Vec<WireVectorChange>,
    }

    let legacy = LegacyWireChangeSetV2 {
        ddl: vec![WireDdlChange::CreateTrigger {
            name: "legacy_trigger".into(),
            table: "observations".into(),
            on_events: vec!["INSERT".into()],
        }],
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
    };
    let bytes = rmp_serde::to_vec(&legacy).expect("legacy WireChangeSet encodes");
    let decoded = rmp_serde::from_slice::<WireChangeSet>(&bytes);
    assert!(
        decoded.is_err(),
        "current WireChangeSet protocol must reject payloads that omit ddl_lsn; got {decoded:?}"
    );
}

#[test]
fn wire_changeset_rejects_mismatched_ddl_lsn_lengths_for_current_protocol() {
    use contextdb_core::Lsn;
    use contextdb_engine::sync_types::ChangeSet;
    use contextdb_server::protocol::{WireChangeSet, WireDdlChange};

    fn trigger_ddl(name: &str) -> WireDdlChange {
        WireDdlChange::CreateTrigger {
            name: name.into(),
            table: "observations".into(),
            on_events: vec!["INSERT".into()],
        }
    }

    let cases = [
        WireChangeSet {
            ddl: vec![trigger_ddl("missing_lsn")],
            ddl_lsn: Vec::new(),
            ..WireChangeSet::default()
        },
        WireChangeSet {
            ddl: vec![trigger_ddl("short_lsn_a"), trigger_ddl("short_lsn_b")],
            ddl_lsn: vec![Lsn(1)],
            ..WireChangeSet::default()
        },
        WireChangeSet {
            ddl: vec![trigger_ddl("extra_lsn")],
            ddl_lsn: vec![Lsn(1), Lsn(2)],
            ..WireChangeSet::default()
        },
    ];

    for wire in cases {
        let err = ChangeSet::try_from(wire).expect_err("mismatched ddl_lsn length must fail");
        assert!(
            err.to_string().contains("ddl_lsn length"),
            "error should name ddl_lsn length mismatch, got {err}"
        );
    }
}
