//! Frozen wire-bytes regression guard for the completed `PROTOCOL_VERSION` 6.
//!
//! Pure encode/decode against fixed, fully deterministic fixtures — no
//! server and no async runtime. It lives in its own auto-discovered test
//! target so the default workspace suite always runs it.

use contextdb_core::{Incarnation, Lsn, Value};
use contextdb_server::protocol::{
    MessageType, PullRequest, PullResponse, PushRequest, PushResponse, WireApplyResult,
    WireChangeSet, WireConflict, WireDdlChange, WireDdlProvenance, WireNaturalKey, WirePushError,
    WireRefusalCause, WireRowChange, canonical_ddl_provenance_digest, decode, encode,
    validate_wire_ddl_provenance,
};
use std::collections::HashMap;

// ======== sr7 — REGRESSION GUARD: the wire bytes are frozen at protocol v6 ========
//
// The greenfield v6 surface includes a distinct trailing PURGE lane, keeps its
// schema-provenance slot present even when empty so later positional slots stay
// stable, and lets `PushResponse` carry a structured authority error. No v6 peer
// shipped before this completed shape. The six constants below freeze it.

fn wire_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).expect("hex pair"))
        .collect()
}

#[test]
fn nonempty_schema_provenance_round_trips_and_validates() {
    let ddl = WireDdlChange::CreateTable {
        name: "empty_recreated".to_string(),
        columns: vec![("id".to_string(), "INTEGER".to_string())],
        constraints: vec!["PRIMARY KEY (id)".to_string()],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let provenance = WireDdlProvenance {
        source_ddl_lsn: Lsn(9),
        ordinal: 0,
        table: Some("empty_recreated".to_string()),
        table_generation: Some(2),
        digest: canonical_ddl_provenance_digest(&ddl, Lsn(9), 0, Some("empty_recreated"), Some(2))
            .unwrap(),
    };
    let wire = WireChangeSet {
        ddl: vec![ddl],
        ddl_lsn: vec![Lsn(9)],
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl_provenance: vec![provenance],
        purges: Vec::new(),
    };
    validate_wire_ddl_provenance(&wire).unwrap();
    let bytes = rmp_serde::to_vec(&wire).unwrap();
    let decoded: WireChangeSet = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(decoded, wire);
    validate_wire_ddl_provenance(&decoded).unwrap();
}

#[test]
fn schema_provenance_rejects_missing_source_lsn_before_ordinal_lookup() {
    let ddl = WireDdlChange::DropTable {
        name: "memories".to_string(),
    };
    let wire = WireChangeSet {
        ddl: vec![ddl.clone()],
        ddl_lsn: Vec::new(),
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl_provenance: vec![WireDdlProvenance {
            source_ddl_lsn: Lsn(9),
            ordinal: 0,
            table: Some("memories".to_string()),
            table_generation: Some(2),
            digest: canonical_ddl_provenance_digest(&ddl, Lsn(9), 0, Some("memories"), Some(2))
                .unwrap(),
        }],
        purges: Vec::new(),
    };

    let error = validate_wire_ddl_provenance(&wire)
        .expect_err("schema provenance without its source LSN must be rejected");
    assert!(
        error.to_string().contains("ddl_lsn length"),
        "cardinality error must be reported before ordinal lookup: {error}"
    );
}

#[test]
fn filtered_schema_entry_keeps_its_original_nonzero_ordinal() {
    let ddl = WireDdlChange::CreateTable {
        name: "pulled_memories".to_string(),
        columns: vec![("id".to_string(), "INTEGER".to_string())],
        constraints: vec!["PRIMARY KEY (id)".to_string()],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let wire = WireChangeSet {
        ddl: vec![ddl.clone()],
        ddl_lsn: vec![Lsn(17)],
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl_provenance: vec![WireDdlProvenance {
            source_ddl_lsn: Lsn(17),
            ordinal: 1,
            table: Some("pulled_memories".to_string()),
            table_generation: Some(1),
            digest: canonical_ddl_provenance_digest(
                &ddl,
                Lsn(17),
                1,
                Some("pulled_memories"),
                Some(1),
            )
            .unwrap(),
        }],
        purges: Vec::new(),
    };

    validate_wire_ddl_provenance(&wire)
        .expect("direction filtering must not renumber the surviving schema identity");
}

const PUSH_REQUEST_WIRE: &str = "9306ab5075736852657175657374dc002ccc92cc96cc90cc90cc91cc98cca174cc93cca26964cc81cca5496e74363407cc90cc81cca26964cc81cca5496e74363407ccc207ccc0ccc0ccc0cc90cc90cc90cc920000";
const PUSH_RESPONSE_WIRE: &str = "9306ac50757368526573706f6e7365dc007acc92cc940101cc91cc98cc93cca26964cc81cca5496e74363407cc90ccaa6b6565705f6669727374ccaa6b6565705f6669727374cca56e6f746573cca465646974ccd9406162616261626162616261626162616261626162616261626162616261626162616261626162616261626162616261626162616261626162616261626162616229ccc007ccc0";
const AUTHORITY_ERROR_PUSH_RESPONSE_WIRE: &str = "9306ac50757368526573706f6e7365dc0065cc93ccc0ccc0cc81ccbd50757267655265717569726573417574686f7269746174697665487562cc91ccd94063646364636463646364636463646364636463646364636463646364636463646364636463646364636463646364636463646364636463646364636463646364";
const GAPPED_CONFLICT_WIRE: &str =
    "9893a2696481a5496e7436340790aa6b6565705f6669727374a57075726765a56e6f746573a57075726765c029c0";
const CAUSED_CONFLICT_WIRE: &str = "9893a2696481a5496e7436340990aa6b6565705f6669727374bb646570656e64656e63795f636f6d706c6574655f72656675736564ab6e6f74655f67726f757073a465646974c0c092a56e6f74657393a2696481a5496e7436340790";
/// The previous seven-slot shape, kept so a peer that predates the named-row
/// slot is proven to still decode.
const CONFLICT_WITHOUT_CAUSE_SLOT_WIRE: &str =
    "9793a2696481a5496e7436340790aa6b6565705f6669727374a57075726765a56e6f746573a57075726765c029";
const PULL_REQUEST_WIRE: &str = "9306ab50756c6c5265717565737495cc922acccd01ccf4";
const PULL_RESPONSE_WIRE: &str =
    "9306ac50756c6c526573706f6e73659bcc94cc96cc90cc90cc90cc90cc90cc90ccc22accc0";

#[test]
fn sr7_guard_amended_v6_push_and_pull_wire_bytes_are_frozen() {
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
        },
        incarnation: Incarnation::default(),
    };
    let push_request_bytes = encode(MessageType::PushRequest, &push_request).unwrap();
    assert_eq!(
        wire_hex(&push_request_bytes),
        PUSH_REQUEST_WIRE,
        "protocol v6 PushRequest wire bytes changed without an explicit version review"
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
    };
    let push_response_bytes = encode(MessageType::PushResponse, &push_response).unwrap();
    assert_eq!(
        wire_hex(&push_response_bytes),
        PUSH_RESPONSE_WIRE,
        "protocol v6 PushResponse wire bytes changed without an explicit version review"
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
    };
    let authority_error_bytes = encode(MessageType::PushResponse, &authority_error).unwrap();
    assert_eq!(
        wire_hex(&authority_error_bytes),
        AUTHORITY_ERROR_PUSH_RESPONSE_WIRE,
        "protocol v6 structured purge-authority refusal bytes changed without an explicit version review"
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
    };
    let pull_request_bytes = encode(MessageType::PullRequest, &pull_request).unwrap();
    assert_eq!(
        wire_hex(&pull_request_bytes),
        PULL_REQUEST_WIRE,
        "protocol v6 PullRequest wire bytes changed without an explicit version review"
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
        "protocol v6 PullResponse wire bytes changed without an explicit version review"
    );
    let envelope = decode(&pull_response_bytes).unwrap();
    let decoded: PullResponse = rmp_serde::from_slice(&envelope.payload).unwrap();
    assert_eq!(
        decoded, pull_response,
        "pinned PullResponse bytes must round-trip"
    );
}
