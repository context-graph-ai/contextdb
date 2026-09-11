//! Frozen wire-bytes regression guard for the first released `PROTOCOL_VERSION` 7.
//!
//! Pure encode/decode against fixed, fully deterministic fixtures — no
//! server and no async runtime. It lives in its own auto-discovered test
//! target so the default workspace suite always runs it.

use contextdb_core::{Incarnation, Lsn, Value};
use contextdb_server::protocol::{
    MessageType, PROTOCOL_VERSION, PullRequest, PullResponse, PushRequest, PushResponse,
    SchemaRecoveryPage, SchemaRecoveryRequest, WireApplyResult, WireChangeSet, WireConflict,
    WireDeliveryManifest, WireDeliveryOutcome, WireNaturalKey, WirePushError, WireRefusalCause,
    WireRowChange, decode, encode,
};
use std::collections::HashMap;

// ======== REGRESSION GUARD: the wire bytes are frozen at protocol 7 ========
//
// The greenfield protocol-7 surface includes a distinct trailing PURGE lane, keeps its
// schema-provenance slot present even when empty so later positional slots stay
// stable, and lets `PushResponse` carry a structured authority error. No v6 peer
// shipped before this completed shape. The constants below freeze it.

fn wire_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).expect("hex pair"))
        .collect()
}

const PUSH_REQUEST_WIRE: &str = "9307ab5075736852657175657374dc002ccc92cc96cc90cc90cc91cc98cca174cc93cca26964cc81cca5496e74363407cc90cc81cca26964cc81cca5496e74363407ccc207ccc0ccc0ccc0cc90cc90cc90cc920000";
const PUSH_RESPONSE_WIRE: &str = "9307ac50757368526573706f6e7365dc007acc92cc940101cc91cc98cc93cca26964cc81cca5496e74363407cc90ccaa6b6565705f6669727374ccaa6b6565705f6669727374cca56e6f746573cca465646974ccd9406162616261626162616261626162616261626162616261626162616261626162616261626162616261626162616261626162616261626162616261626162616229ccc007ccc0";
const AUTHORITY_ERROR_PUSH_RESPONSE_WIRE: &str = "9307ac50757368526573706f6e7365dc0065cc93ccc0ccc0cc81ccbd50757267655265717569726573417574686f7269746174697665487562cc91ccd94063646364636463646364636463646364636463646364636463646364636463646364636463646364636463646364636463646364636463646364636463646364";
const GAPPED_CONFLICT_WIRE: &str =
    "9893a2696481a5496e7436340790aa6b6565705f6669727374a57075726765a56e6f746573a57075726765c029c0";
const CAUSED_CONFLICT_WIRE: &str = "9893a2696481a5496e7436340990aa6b6565705f6669727374bb646570656e64656e63795f636f6d706c6574655f72656675736564ab6e6f74655f67726f757073a465646974c0c092a56e6f74657393a2696481a5496e7436340790";
/// The previous seven-slot shape, kept so a peer that predates the named-row
/// slot is proven to still decode.
const CONFLICT_WITHOUT_CAUSE_SLOT_WIRE: &str =
    "9793a2696481a5496e7436340790aa6b6565705f6669727374a57075726765a56e6f746573a57075726765c029";
const PULL_REQUEST_WIRE: &str = "9307ab50756c6c5265717565737495cc922acccd01ccf4";
const PULL_RESPONSE_WIRE: &str =
    "9307ac50756c6c526573706f6e73659bcc94cc96cc90cc90cc90cc90cc90cc90ccc22accc0";
const SCHEMA_RECOVERY_PULL_REQUEST_WIRE: &str =
    "9307ab50756c6c52657175657374dc0012cc935acccd01ccf4cc81cca8436f6e74696e7565cc925a28";
const SCHEMA_RECOVERY_PULL_RESPONSE_WIRE: &str = "9307ac50756c6c526573706f6e7365dc0011cc95cc96cc90cc90cc90cc90cc90cc90ccc35acc92002acc935a28ccc2";
const POPULATED_CUSTODY_PUSH_REQUEST_WIRE: &str = "9307ab5075736852657175657374dc0036cc92cc98cc90cc90cc90cc90cc90cc90cc90cc91cc9bccc403010203ccc4020405cc92cc9106cc920708ccc40109ccc4020a0bcc91cc920c0d02ccc4010eccc4020f10ccc40111cc921213cc920017";
const POPULATED_CUSTODY_PUSH_RESPONSE_WIRE: &str = "9307ac50757368526573706f6e7365dc005dcc95ccc0ccc0ccc0cc91cc96ccc4021415ccdc00201616161616161616161616161616161616161616161616161616161616161616ccdc00201717171717171717171717171717171717171717171717171717171717171717ccc40118ccc402191accc4011bcc92001d";

#[test]
fn protocol_seven_populated_custody_lanes_are_frozen() {
    let request = PushRequest {
        changeset: WireChangeSet {
            manifests: vec![WireDeliveryManifest {
                submission_id: vec![1, 2, 3],
                seal: vec![4, 5],
                life_evidence: vec![vec![6], vec![7, 8]],
                materialization_projection: vec![9],
                policy_evidence: vec![10, 11],
                retained_slots: vec![vec![12, 13]],
                erased_slot_count: 2,
                submission_signature: vec![14],
                disclosure: vec![15, 16],
                disclosure_signature: vec![17],
                erasure_authorization: Some(vec![18, 19]),
            }],
            ..WireChangeSet::default()
        },
        incarnation: Incarnation(23),
    };
    let request_bytes = encode(MessageType::PushRequest, &request).unwrap();
    assert_eq!(
        wire_hex(&request_bytes),
        POPULATED_CUSTODY_PUSH_REQUEST_WIRE,
        "protocol-seven populated manifest bytes changed without a protocol review"
    );
    let decoded: PushRequest =
        rmp_serde::from_slice(&decode(&request_bytes).unwrap().payload).unwrap();
    assert_eq!(decoded, request);

    let response = PushResponse {
        outcomes: vec![WireDeliveryOutcome {
            lookup_submission: vec![20, 21],
            lookup_seal_digest: [22; 32],
            lookup_origin_life_digest: [23; 32],
            lookup_source: vec![24],
            signed_core: vec![25, 26],
            diagnostic_body: vec![27],
        }],
        hub_incarnation: Some(Incarnation(29)),
        ..PushResponse::default()
    };
    let response_bytes = encode(MessageType::PushResponse, &response).unwrap();
    assert_eq!(
        wire_hex(&response_bytes),
        POPULATED_CUSTODY_PUSH_RESPONSE_WIRE,
        "protocol-seven populated outcome and hub incarnation bytes changed without a protocol review"
    );
    let decoded: PushResponse =
        rmp_serde::from_slice(&decode(&response_bytes).unwrap().payload).unwrap();
    assert_eq!(decoded, response);
}

#[test]
fn protocol_seven_push_and_pull_wire_bytes_are_frozen() {
    assert_eq!(
        PROTOCOL_VERSION, 7,
        "these fixtures belong to the current protocol-seven surface"
    );
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
        lineage: None,
    };
    let push_request = PushRequest {
        changeset: WireChangeSet {
            ddl: Vec::new(),
            ddl_lsn: Vec::new(),
            ddl_provenance: Vec::new(),
            rows: vec![row],
            edges: Vec::new(),
            vectors: Vec::new(),
            purges: Vec::new(),
            // Ordinary manifest lane compile prerequisite.
            manifests: Vec::new(),
        },
        incarnation: Incarnation::default(),
    };
    let push_request_bytes = encode(MessageType::PushRequest, &push_request).unwrap();
    assert_eq!(
        wire_hex(&push_request_bytes),
        PUSH_REQUEST_WIRE,
        "protocol-seven PushRequest wire bytes changed without an explicit version review"
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
            skipped_rows: 1,
            conflicts: vec![WireConflict {
                natural_key: WireNaturalKey {
                    column: "id".to_string(),
                    value: Value::Int64(7),
                    rest: Vec::new(),
                },
                resolution: "keep_first".to_string(),
                reason: Some("keep_first".to_string()),
                table: Some("notes".to_string()),
                mutation_kind: Some("edit".to_string()),
                winning_author_node_id: Some("ab".repeat(32)),
                hub_acceptance_position: Some(Lsn(41)),
                refusal_cause: None,
            }],
            new_lsn: Lsn(7),
        }),
        error: None,
        application_error: None,
        // Ordinary response lane compile prerequisite.
        ..Default::default()
    };
    let push_response_bytes = encode(MessageType::PushResponse, &push_response).unwrap();
    assert_eq!(
        wire_hex(&push_response_bytes),
        PUSH_RESPONSE_WIRE,
        "protocol-seven PushResponse wire bytes changed without an explicit version review"
    );
    let envelope = decode(&push_response_bytes).unwrap();
    let decoded: PushResponse = rmp_serde::from_slice(&envelope.payload).unwrap();
    assert_eq!(
        decoded, push_response,
        "pinned PushResponse bytes must round-trip"
    );

    let authority_error = PushResponse {
        result: None,
        error: None,
        application_error: Some(WirePushError::PurgeRequiresAuthoritativeHub {
            hub_node_id: "cd".repeat(32),
        }),
        // Ordinary response lane compile prerequisite.
        ..Default::default()
    };
    let authority_error_bytes = encode(MessageType::PushResponse, &authority_error).unwrap();
    assert_eq!(
        wire_hex(&authority_error_bytes),
        AUTHORITY_ERROR_PUSH_RESPONSE_WIRE,
        "protocol-seven structured purge-authority refusal bytes changed without an explicit version review"
    );
    let envelope = decode(&authority_error_bytes).unwrap();
    let decoded: PushResponse = rmp_serde::from_slice(&envelope.payload).unwrap();
    assert_eq!(decoded, authority_error);

    // Positional optional fields keep their slots. In particular, a hub
    // position without a winning author must decode as a position, never
    // shift left into the author field.
    let gapped_conflict = WireConflict {
        natural_key: WireNaturalKey {
            column: "id".to_string(),
            value: Value::Int64(7),
            rest: Vec::new(),
        },
        resolution: "keep_first".to_string(),
        reason: Some("purge".to_string()),
        table: Some("notes".to_string()),
        mutation_kind: Some("purge".to_string()),
        winning_author_node_id: None,
        hub_acceptance_position: Some(Lsn(41)),
        refusal_cause: None,
    };
    let gapped_bytes = rmp_serde::to_vec(&gapped_conflict).unwrap();
    assert_eq!(wire_hex(&gapped_bytes), GAPPED_CONFLICT_WIRE);
    let decoded: WireConflict = rmp_serde::from_slice(&gapped_bytes).unwrap();
    assert_eq!(decoded, gapped_conflict);

    // A member refused because a sibling row had already been accepted names
    // that sibling and reports no author or position of its own. The named
    // row rides the last slot, so every earlier slot is untouched.
    let caused_conflict = WireConflict {
        natural_key: WireNaturalKey {
            column: "id".to_string(),
            value: Value::Int64(9),
            rest: Vec::new(),
        },
        resolution: "keep_first".to_string(),
        reason: Some("dependency_complete_refused".to_string()),
        table: Some("note_groups".to_string()),
        mutation_kind: Some("edit".to_string()),
        winning_author_node_id: None,
        hub_acceptance_position: None,
        refusal_cause: Some(WireRefusalCause {
            table: "notes".to_string(),
            natural_key: WireNaturalKey {
                column: "id".to_string(),
                value: Value::Int64(7),
                rest: Vec::new(),
            },
        }),
    };
    let caused_bytes = rmp_serde::to_vec(&caused_conflict).unwrap();
    assert_eq!(wire_hex(&caused_bytes), CAUSED_CONFLICT_WIRE);
    let decoded: WireConflict = rmp_serde::from_slice(&caused_bytes).unwrap();
    assert_eq!(decoded, caused_conflict);

    // A peer that stops at the previous last slot still decodes: the named
    // row reads as absent rather than shifting an earlier slot's meaning.
    let without_trailing_slot: WireConflict =
        rmp_serde::from_slice(&hex_bytes(CONFLICT_WITHOUT_CAUSE_SLOT_WIRE)).unwrap();
    assert_eq!(without_trailing_slot, gapped_conflict);

    // PullRequest.
    let pull_request = PullRequest {
        since_lsn: Lsn(42),
        max_entries: Some(500),
        schema_recovery: None,
    };
    let pull_request_bytes = encode(MessageType::PullRequest, &pull_request).unwrap();
    assert_eq!(
        wire_hex(&pull_request_bytes),
        PULL_REQUEST_WIRE,
        "protocol-seven plain PullRequest wire bytes changed without an explicit version review"
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
        schema_recovery: None,
    };
    let pull_response_bytes = encode(MessageType::PullResponse, &pull_response).unwrap();
    assert_eq!(
        wire_hex(&pull_response_bytes),
        PULL_RESPONSE_WIRE,
        "protocol-seven plain PullResponse wire bytes changed without an explicit version review"
    );
    let envelope = decode(&pull_response_bytes).unwrap();
    let decoded: PullResponse = rmp_serde::from_slice(&envelope.payload).unwrap();
    assert_eq!(
        decoded, pull_response,
        "pinned PullResponse bytes must round-trip"
    );
}

#[test]
fn protocol_seven_schema_recovery_present_wire_bytes_are_frozen() {
    assert_eq!(
        PROTOCOL_VERSION, 7,
        "these fixtures belong to the current protocol-seven surface"
    );

    let request = PullRequest {
        since_lsn: Lsn(90),
        max_entries: Some(500),
        schema_recovery: Some(SchemaRecoveryRequest::Continue {
            target_lsn: Lsn(90),
            after_lsn: Lsn(40),
        }),
    };
    let request_bytes = encode(MessageType::PullRequest, &request).unwrap();

    let response = PullResponse {
        changeset: WireChangeSet::default(),
        has_more: true,
        cursor: Some(Lsn(90)),
        source: Some(Incarnation(42)),
        schema_recovery: Some(SchemaRecoveryPage {
            target_lsn: Lsn(90),
            next_lsn: Lsn(40),
            complete: false,
        }),
    };
    let response_bytes = encode(MessageType::PullResponse, &response).unwrap();

    assert_eq!(
        format!(
            "request={} response={}",
            wire_hex(&request_bytes),
            wire_hex(&response_bytes)
        ),
        format!(
            "request={SCHEMA_RECOVERY_PULL_REQUEST_WIRE} \
             response={SCHEMA_RECOVERY_PULL_RESPONSE_WIRE}"
        ),
        "protocol-seven present schema-recovery request/response bytes changed without an \
         explicit version review"
    );

    let request_envelope = decode(&request_bytes).unwrap();
    let decoded_request: PullRequest = rmp_serde::from_slice(&request_envelope.payload).unwrap();
    assert_eq!(decoded_request, request);

    let response_envelope = decode(&response_bytes).unwrap();
    let decoded_response: PullResponse = rmp_serde::from_slice(&response_envelope.payload).unwrap();
    assert_eq!(decoded_response, response);
}
