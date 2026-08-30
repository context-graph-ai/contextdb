//! A metadata answer read back is the answer that was published.
//!
//! Two routes answer the same question about a store, and both publish the
//! same canonical bytes -- that is what makes the answers comparable at all.
//! A reader that gets those bytes over a channel has to turn them back into
//! the answer, and the only way that is trustworthy is if reading is the exact
//! inverse of writing: every tag, every length prefix, every optional, for
//! every kind of question and every corner of the schema document.
//!
//! So each kind below is encoded, read back, and required to be equal to what
//! went in -- and re-encoded, to prove the bytes are the same bytes rather
//! than a second shape that merely decodes alike.

#![cfg(all(unix, feature = "test-seams"))]

use contextdb_core::{Lsn, TxId};
use contextdb_engine::read_contract::{decode_metadata_body, encode_metadata_body};
use contextdb_engine::{
    DdlChange, DirectColumnReference, DirectEventTypeStatus, DirectEventsStatus, DirectImageState,
    DirectIndexColumn, DirectIndexDirection, DirectMaintenanceStatus, DirectPropagationRule,
    DirectRankPolicy, DirectReferencePropagation, DirectRetainPolicy, DirectRouteStatus,
    DirectScheduleStatus, DirectSchema, DirectSchemaColumn, DirectSchemaIndex,
    DirectScopeLabelKind, DirectSinkStatus, DirectStateMachine, DirectVectorQuantization,
    MetadataBody,
};
use std::collections::BTreeMap;

/// Read one body back and require it to be the body that was written, in both
/// directions: the same value, and the same bytes.
fn survives_the_wire(body: &MetadataBody) {
    let written = encode_metadata_body(body).expect("encode the metadata body");
    let read = decode_metadata_body(&written).expect("read the metadata body back");
    assert_eq!(&read, body, "reading is the inverse of writing");
    let rewritten = encode_metadata_body(&read).expect("encode what was read back");
    assert_eq!(
        rewritten, written,
        "the body that was read publishes the same bytes it arrived as"
    );
}

/// A schema using every corner of the document: both index directions, all
/// three quantizations, a reference with and without propagation, every
/// propagation rule, a state machine, a retain policy, and every optional
/// present -- because an optional that is never present is an untested branch.
fn a_schema_with_every_corner_filled() -> DirectSchema {
    DirectSchema {
        table: "documents".to_owned(),
        immutable: true,
        columns: vec![
            DirectSchemaColumn {
                name: "id".to_owned(),
                data_type: "UUID".to_owned(),
                nullable: false,
                primary_key: true,
                unique: true,
                immutable: true,
                expires: false,
                default: Some("gen_random_uuid()".to_owned()),
                references: Some(DirectColumnReference {
                    table: "owners".to_owned(),
                    column: "id".to_owned(),
                    propagation: Some(DirectReferencePropagation {
                        on_state: "archived".to_owned(),
                        set_state: "archived".to_owned(),
                        max_depth: 7,
                        abort_on_failure: true,
                    }),
                }),
                quantization: None,
                rank: Some(DirectRankPolicy {
                    sort_key: "score".to_owned(),
                    formula: "recency".to_owned(),
                    joined_table: "scores".to_owned(),
                    joined_column: "document_id".to_owned(),
                }),
                // The Split form, so the read set and the write set are read
                // back as two separate lists rather than one list twice.
                scope_label: Some(DirectScopeLabelKind::Split {
                    read_labels: vec!["tenant_a".to_owned(), "tenant_b".to_owned()],
                    write_labels: vec!["tenant_a".to_owned()],
                }),
                // The access-control declaration: present, so the tag is read
                // back rather than left an untested branch. An ACL reference
                // propagates nothing, so the inner optional stays absent.
                acl_ref: Some(DirectColumnReference {
                    table: "acl_grants".to_owned(),
                    column: "acl_id".to_owned(),
                    propagation: None,
                }),
            },
            DirectSchemaColumn {
                name: "body".to_owned(),
                data_type: "TEXT".to_owned(),
                nullable: true,
                primary_key: false,
                unique: false,
                immutable: false,
                expires: true,
                default: None,
                // A reference that propagates nothing: the inner optional is
                // absent while the outer one is present.
                references: Some(DirectColumnReference {
                    table: "sources".to_owned(),
                    column: "id".to_owned(),
                    propagation: None,
                }),
                quantization: Some(DirectVectorQuantization::F32),
                rank: None,
                // The other side of the tag: the Simple form, one label set.
                scope_label: Some(DirectScopeLabelKind::Simple {
                    write_labels: vec!["tenant_a".to_owned()],
                }),
                acl_ref: None,
            },
            DirectSchemaColumn {
                name: "coarse".to_owned(),
                data_type: "VECTOR".to_owned(),
                nullable: false,
                primary_key: false,
                unique: false,
                immutable: false,
                expires: false,
                default: None,
                references: None,
                quantization: Some(DirectVectorQuantization::Sq8),
                rank: None,
                scope_label: None,
                acl_ref: None,
            },
            DirectSchemaColumn {
                name: "coarsest".to_owned(),
                data_type: "VECTOR".to_owned(),
                nullable: false,
                primary_key: false,
                unique: false,
                immutable: false,
                expires: false,
                default: None,
                references: None,
                quantization: Some(DirectVectorQuantization::Sq4),
                rank: None,
                scope_label: None,
                acl_ref: None,
            },
        ],
        primary_key: vec!["id".to_owned()],
        indexes: vec![
            DirectSchemaIndex {
                name: "by_body".to_owned(),
                columns: vec![DirectIndexColumn {
                    column: "body".to_owned(),
                    direction: DirectIndexDirection::Asc,
                }],
            },
            DirectSchemaIndex {
                name: "by_id_desc".to_owned(),
                columns: vec![DirectIndexColumn {
                    column: "id".to_owned(),
                    direction: DirectIndexDirection::Desc,
                }],
            },
        ],
        state_machine: Some(DirectStateMachine {
            column: "state".to_owned(),
            transitions: BTreeMap::from([
                (
                    "draft".to_owned(),
                    vec!["published".to_owned(), "archived".to_owned()],
                ),
                ("published".to_owned(), vec!["archived".to_owned()]),
            ]),
        }),
        retain: Some(DirectRetainPolicy {
            window: 30,
            unit: "days".to_owned(),
            seconds: 2_592_000,
            sync_safe: true,
        }),
        history: Some("full".to_owned()),
        sync_direction: Some("two_way".to_owned()),
        conflict_policy: Some("keep_first".to_owned()),
        dag_edge_types: vec!["derived_from".to_owned(), "supersedes".to_owned()],
        propagate: vec![
            DirectPropagationRule::Edge {
                edge_type: "derived_from".to_owned(),
                direction: "outgoing".to_owned(),
                on_state: "archived".to_owned(),
                set_state: "archived".to_owned(),
                max_depth: 3,
                abort_on_failure: false,
            },
            DirectPropagationRule::VectorExclusion {
                on_state: "archived".to_owned(),
            },
            DirectPropagationRule::ForeignKey {
                column: "id".to_owned(),
                references_table: "owners".to_owned(),
                references_column: "id".to_owned(),
                on_state: "deleted".to_owned(),
                set_state: "deleted".to_owned(),
                max_depth: 1,
                abort_on_failure: true,
            },
        ],
        ddl: "CREATE TABLE documents (id UUID PRIMARY KEY)".to_owned(),
    }
}

/// A schema with every optional absent, so the other side of each tag is read
/// too.
fn a_schema_with_nothing_optional() -> DirectSchema {
    DirectSchema {
        table: "bare".to_owned(),
        immutable: false,
        columns: vec![DirectSchemaColumn {
            name: "id".to_owned(),
            data_type: "INTEGER".to_owned(),
            nullable: false,
            primary_key: true,
            unique: false,
            immutable: false,
            expires: false,
            default: None,
            references: None,
            quantization: None,
            rank: None,
            scope_label: None,
            acl_ref: None,
        }],
        primary_key: Vec::new(),
        indexes: Vec::new(),
        state_machine: None,
        retain: None,
        history: None,
        sync_direction: None,
        conflict_policy: None,
        dag_edge_types: Vec::new(),
        propagate: Vec::new(),
        ddl: "CREATE TABLE bare (id INTEGER PRIMARY KEY)".to_owned(),
    }
}

fn an_events_status() -> DirectEventsStatus {
    DirectEventsStatus {
        event_types: vec![DirectEventTypeStatus {
            name: "document_written".to_owned(),
            trigger: "after_insert".to_owned(),
            table: "documents".to_owned(),
        }],
        sinks: vec![DirectSinkStatus {
            name: "audit".to_owned(),
            sink_type: "callback".to_owned(),
            callback_registered: true,
            delivered: 11,
            queued: 2,
            retried: 3,
            permanent_failures: 1,
            examined: 17,
        }],
        routes: vec![DirectRouteStatus {
            name: "documents_to_audit".to_owned(),
            event_type: "document_written".to_owned(),
            sink: "audit".to_owned(),
        }],
        schedules: vec![
            DirectScheduleStatus {
                name: "nightly".to_owned(),
                every: "1d".to_owned(),
                callback: "compact".to_owned(),
                callback_registered: true,
                next_fire_at_ms: 1_000,
                last_fire_at_ms: Some(900),
                fire_count: 4,
            },
            DirectScheduleStatus {
                name: "never_fired".to_owned(),
                every: "1h".to_owned(),
                callback: "vacuum".to_owned(),
                callback_registered: false,
                next_fire_at_ms: 2_000,
                last_fire_at_ms: None,
                fire_count: 0,
            },
        ],
    }
}

#[test]
fn a_table_inventory_survives_the_wire() {
    survives_the_wire(&MetadataBody::Tables {
        items: vec!["documents".to_owned(), "owners".to_owned()],
        has_more: true,
    });
    survives_the_wire(&MetadataBody::Tables {
        items: Vec::new(),
        has_more: false,
    });
}

#[test]
fn a_schema_survives_the_wire_with_every_optional_present_and_absent() {
    survives_the_wire(&MetadataBody::Schema {
        schema: a_schema_with_every_corner_filled(),
    });
    survives_the_wire(&MetadataBody::Schema {
        schema: a_schema_with_nothing_optional(),
    });
}

#[test]
fn an_explained_statement_survives_the_wire() {
    survives_the_wire(&MetadataBody::Explain {
        sql: "SELECT id FROM documents WHERE body = $body".to_owned(),
        physical_plan: "IndexScan".to_owned(),
        index: Some("by_body".to_owned()),
    });
    survives_the_wire(&MetadataBody::Explain {
        sql: "SELECT id FROM documents".to_owned(),
        physical_plan: "Scan".to_owned(),
        index: None,
    });
}

#[test]
fn an_event_inventory_survives_the_wire() {
    survives_the_wire(&MetadataBody::EventsStatus {
        status: an_events_status(),
        has_more: true,
        continuation: Some("sink:audit".to_owned()),
    });
    survives_the_wire(&MetadataBody::EventsStatus {
        status: DirectEventsStatus {
            event_types: Vec::new(),
            sinks: Vec::new(),
            routes: Vec::new(),
            schedules: Vec::new(),
        },
        has_more: false,
        continuation: None,
    });
}

#[test]
fn a_maintenance_status_survives_the_wire() {
    survives_the_wire(&MetadataBody::MaintenanceStatus {
        status: DirectMaintenanceStatus {
            policy: "engine_owned".to_owned(),
            running: true,
            retention_enabled: true,
            currency_compaction_enabled: false,
            active_maintenance_loops: 3,
        },
    });
}

#[test]
fn every_image_state_survives_the_wire() {
    survives_the_wire(&MetadataBody::ImageState {
        state: DirectImageState::Sync(contextdb_engine::DirectSyncState {
            watermark: Lsn(42),
            sources: vec![
                contextdb_engine::DirectSyncSource {
                    row_lsn: Lsn(1),
                    source_lsn: Some(Lsn(9)),
                },
                contextdb_engine::DirectSyncSource {
                    row_lsn: Lsn(2),
                    source_lsn: None,
                },
            ],
            tables: vec![contextdb_engine::DirectTableSyncPolicy {
                table: "documents".to_owned(),
                direction: "two_way".to_owned(),
                conflict_policy: "keep_first".to_owned(),
            }],
        }),
    });

    survives_the_wire(&MetadataBody::ImageState {
        state: DirectImageState::ChangeLog(contextdb_engine::DirectChangeState {
            current_lsn: Lsn(77),
            committed_watermark: TxId(12),
            next_tx: TxId(13),
            commit_index: vec![(Lsn(1), TxId(1)), (Lsn(2), TxId(2))],
            rows: Vec::new(),
            edges: Vec::new(),
            vectors: Vec::new(),
            // One rendered entry, because a list that is always empty never
            // proves its entries survive the trip.
            ddl: vec![DdlChange::DropTable {
                name: "documents".to_owned(),
            }],
            ddl_lsn: vec![Lsn(3)],
        }),
    });

    survives_the_wire(&MetadataBody::ImageState {
        state: DirectImageState::Configuration(contextdb_engine::DirectConfigurationState {
            memory_limit_bytes: Some(16 * 1024 * 1024),
            disk_limit_bytes: Some(1024 * 1024 * 1024),
            schemas: vec![
                a_schema_with_every_corner_filled(),
                a_schema_with_nothing_optional(),
            ],
        }),
    });

    survives_the_wire(&MetadataBody::ImageState {
        state: DirectImageState::Configuration(contextdb_engine::DirectConfigurationState {
            memory_limit_bytes: None,
            disk_limit_bytes: None,
            schemas: Vec::new(),
        }),
    });
}

#[test]
fn bytes_that_do_not_spell_a_body_are_refused_rather_than_guessed_at() {
    let complete = encode_metadata_body(&MetadataBody::Tables {
        items: vec!["documents".to_owned()],
        has_more: false,
    })
    .expect("encode a table inventory");

    assert!(
        decode_metadata_body(&complete[..complete.len() - 1]).is_err(),
        "a body that stops early is not a body"
    );
    let mut trailing = complete.clone();
    trailing.push(0);
    assert!(
        decode_metadata_body(&trailing).is_err(),
        "bytes after a complete body were never part of the answer"
    );
    assert!(
        decode_metadata_body(&[99]).is_err(),
        "a tag no kind uses is not a kind this build should invent"
    );
    assert!(
        decode_metadata_body(&[]).is_err(),
        "no bytes is not an empty answer"
    );

    // A length prefix the payload does not back must be refused rather than
    // trusted into an allocation.
    let mut lying = vec![0u8];
    lying.extend_from_slice(&u64::MAX.to_le_bytes());
    assert!(
        decode_metadata_body(&lying).is_err(),
        "a count the bytes cannot back is refused, not allocated"
    );
}
