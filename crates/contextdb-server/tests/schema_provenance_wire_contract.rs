//! Current protocol-seven guards for schema provenance and conflict shape.

use contextdb_core::{Lsn, Value};
use contextdb_server::protocol::{
    PROTOCOL_VERSION, WireChangeSet, WireConflict, WireDdlChange, WireDdlProvenance,
    WireNaturalKey, canonical_ddl_provenance_digest, validate_wire_ddl_provenance,
};

fn wire_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
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
            .expect("canonical provenance digest"),
    };
    let wire = WireChangeSet {
        ddl: vec![ddl],
        ddl_lsn: vec![Lsn(9)],
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl_provenance: vec![provenance],
        manifests: Vec::new(),
        purges: Vec::new(),
    };

    validate_wire_ddl_provenance(&wire).expect("valid schema provenance");
    let bytes = rmp_serde::to_vec(&wire).expect("encode schema provenance");
    let decoded: WireChangeSet = rmp_serde::from_slice(&bytes).expect("decode schema provenance");
    assert_eq!(decoded, wire);
    validate_wire_ddl_provenance(&decoded).expect("decoded schema provenance remains valid");
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
                .expect("canonical provenance digest"),
        }],
        manifests: Vec::new(),
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
            .expect("canonical provenance digest"),
        }],
        manifests: Vec::new(),
        purges: Vec::new(),
    };

    validate_wire_ddl_provenance(&wire)
        .expect("direction filtering must not renumber the surviving schema identity");
}

#[test]
fn protocol_seven_wire_conflict_keeps_optional_slots_in_place() {
    assert_eq!(
        PROTOCOL_VERSION, 7,
        "this freeze belongs to the released protocol"
    );
    let conflict = WireConflict {
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

    let encoded = rmp_serde::to_vec(&conflict).expect("encode current WireConflict");
    assert_eq!(
        wire_hex(&encoded),
        "9893a2696481a5496e7436340790aa6b6565705f6669727374a57075726765a56e6f746573a57075726765c029c0",
        "protocol-seven WireConflict must retain all optional field positions"
    );
    let decoded: WireConflict =
        rmp_serde::from_slice(&encoded).expect("decode current WireConflict");
    assert_eq!(decoded, conflict);
}
