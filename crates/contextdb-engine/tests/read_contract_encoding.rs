use bincode::config::standard;
use bincode::serde::encode_to_vec;
use contextdb_core::read_contract::{
    CursorPage, MetadataItem, MetadataPage, MetadataPageVocabulary,
};
use contextdb_core::{TxId, Value, VectorIndexRef};
use contextdb_engine::read_contract::{
    CanonicalCascadeReport, CanonicalIndexCandidate, CanonicalQueryResult, CanonicalQueryTrace,
    ReadEncodingError, cursor_page_encoded_size, decode_cursor_page, decode_metadata_page,
    decode_query_result, encode_cursor_page, encode_metadata_page, encode_query_result,
    metadata_page_encoded_size, query_result_encoded_size,
};
use contextdb_engine::{CascadeReport, IndexCandidate};
pub use contextdb_engine::{QueryResult, QueryTrace};
use serde::Serialize;
use std::collections::BTreeMap;
use uuid::Uuid;

fn standard_bytes<T: Serialize>(value: &T) -> Vec<u8> {
    encode_to_vec(value, standard()).expect("bincode standard encoding")
}

fn result_with_every_value_kind() -> QueryResult {
    QueryResult {
        columns: vec![
            "null".to_owned(),
            "bool".to_owned(),
            "int".to_owned(),
            "float".to_owned(),
            "text".to_owned(),
            "uuid".to_owned(),
            "timestamp".to_owned(),
            "json".to_owned(),
            "vector".to_owned(),
            "tx".to_owned(),
        ],
        rows: vec![vec![
            Value::Null,
            Value::Bool(true),
            Value::Int64(-7),
            Value::Float64(3.5),
            Value::Text("read contract".to_owned()),
            Value::Uuid(Uuid::from_u128(0x1234)),
            Value::Timestamp(1_725_000_000_000),
            Value::Json(serde_json::json!({"route": "neutral"})),
            Value::Vector(vec![1.0, -2.0, 3.5]),
            Value::TxId(TxId(42)),
        ]],
        rows_affected: 3,
        trace: QueryTrace {
            physical_plan: "IndexScan",
            index_used: Some("events_created_at".to_owned()),
            predicates_pushed: smallvec::smallvec!["created_at >= $since".into()],
            indexes_considered: smallvec::smallvec![IndexCandidate {
                name: "events_id".to_owned(),
                rejected_reason: "predicate is on created_at".into(),
            }],
            sort_elided: true,
            query_vector_source: Some(VectorIndexRef::new("events", "embedding")),
            rows_examined: 9,
        },
        cascade: Some(CascadeReport {
            dropped_indexes: vec!["events_stale".to_owned()],
        }),
    }
}

fn materially_different_result() -> QueryResult {
    QueryResult {
        columns: vec!["count".to_owned()],
        rows: vec![vec![Value::Int64(0)], vec![Value::Int64(1)]],
        rows_affected: 0,
        trace: QueryTrace {
            physical_plan: "Scan",
            index_used: None,
            predicates_pushed: smallvec::SmallVec::new(),
            indexes_considered: smallvec::SmallVec::new(),
            sort_elided: false,
            query_vector_source: None,
            rows_examined: 2,
        },
        cascade: None,
    }
}

fn expected_result_with_every_value_kind() -> CanonicalQueryResult {
    CanonicalQueryResult {
        columns: vec![
            "null".to_owned(),
            "bool".to_owned(),
            "int".to_owned(),
            "float".to_owned(),
            "text".to_owned(),
            "uuid".to_owned(),
            "timestamp".to_owned(),
            "json".to_owned(),
            "vector".to_owned(),
            "tx".to_owned(),
        ],
        rows: vec![vec![
            Value::Null,
            Value::Bool(true),
            Value::Int64(-7),
            Value::Float64(3.5),
            Value::Text("read contract".to_owned()),
            Value::Uuid(Uuid::from_u128(0x1234)),
            Value::Timestamp(1_725_000_000_000),
            Value::Json(serde_json::json!({"route": "neutral"})),
            Value::Vector(vec![1.0, -2.0, 3.5]),
            Value::TxId(TxId(42)),
        ]],
        rows_affected: 3,
        trace: CanonicalQueryTrace {
            physical_plan: "IndexScan".to_owned(),
            index_used: Some("events_created_at".to_owned()),
            predicates_pushed: vec!["created_at >= $since".to_owned()],
            indexes_considered: vec![CanonicalIndexCandidate {
                name: "events_id".to_owned(),
                rejected_reason: "predicate is on created_at".to_owned(),
            }],
            sort_elided: true,
            query_vector_source: Some(VectorIndexRef::new("events", "embedding")),
            rows_examined: 9,
        },
        cascade: Some(CanonicalCascadeReport {
            dropped_indexes: vec!["events_stale".to_owned()],
        }),
    }
}

fn expected_materially_different_result() -> CanonicalQueryResult {
    CanonicalQueryResult {
        columns: vec!["count".to_owned()],
        rows: vec![vec![Value::Int64(0)], vec![Value::Int64(1)]],
        rows_affected: 0,
        trace: CanonicalQueryTrace {
            physical_plan: "Scan".to_owned(),
            index_used: None,
            predicates_pushed: vec![],
            indexes_considered: vec![],
            sort_elided: false,
            query_vector_source: None,
            rows_examined: 2,
        },
        cascade: None,
    }
}

fn assert_query_codec_case(result: &QueryResult, expected: &CanonicalQueryResult) {
    let expected_bytes = standard_bytes(expected);
    let encoded = encode_query_result(result).expect("canonical query-result encoding");
    assert_eq!(encoded, expected_bytes);
    assert_eq!(
        query_result_encoded_size(result).expect("canonical query-result encoded size"),
        encoded.len()
    );
    assert_eq!(
        decode_query_result(&encoded).expect("canonical query-result decoding"),
        expected.clone()
    );
}

#[test]
fn canonical_query_projection_preserves_every_success_field() {
    assert_eq!(
        CanonicalQueryResult::from(&result_with_every_value_kind()),
        expected_result_with_every_value_kind()
    );
    assert_eq!(
        CanonicalQueryResult::from(&materially_different_result()),
        expected_materially_different_result()
    );
}

#[test]
fn query_result_encoding_is_canonical_dynamic_and_route_neutral() {
    let rich = result_with_every_value_kind();
    let sparse = materially_different_result();
    assert_eq!(rich.rows[0].len(), 10, "every Value kind must be covered");

    let rich_dto = expected_result_with_every_value_kind();
    let sparse_dto = expected_materially_different_result();
    let rich_expected = standard_bytes(&rich_dto);
    let sparse_expected = standard_bytes(&sparse_dto);

    let rich_bytes = encode_query_result(&rich).expect("canonical rich-result encoding");
    let sparse_bytes = encode_query_result(&sparse).expect("canonical sparse-result encoding");
    assert_eq!(rich_bytes, rich_expected);
    assert_eq!(sparse_bytes, sparse_expected);
    assert_ne!(rich_bytes, sparse_bytes);
    assert_eq!(
        query_result_encoded_size(&rich).expect("rich-result encoded size"),
        rich_bytes.len()
    );
    assert_eq!(
        query_result_encoded_size(&sparse).expect("sparse-result encoded size"),
        sparse_bytes.len()
    );
    assert_eq!(
        decode_query_result(&rich_bytes).expect("decode rich result"),
        rich_dto
    );
    assert_eq!(
        decode_query_result(&sparse_bytes).expect("decode sparse result"),
        sparse_dto
    );
    assert_eq!(
        encode_query_result(&rich).expect("same value through the other route"),
        rich_bytes,
        "the canonical encoder has no route-dependent state"
    );
}

#[test]
fn query_result_codec_mutation_matrix_covers_every_encoded_field() {
    let rich = result_with_every_value_kind();
    let rich_expected = expected_result_with_every_value_kind();
    let sparse = materially_different_result();
    let sparse_expected = expected_materially_different_result();

    let mut renamed_columns = rich.clone();
    let mut renamed_columns_expected = rich_expected.clone();
    renamed_columns.columns[0] = "null_value".to_owned();
    renamed_columns_expected.columns[0] = "null_value".to_owned();

    let mut additional_column = rich.clone();
    let mut additional_column_expected = rich_expected.clone();
    additional_column.columns.push("extra".to_owned());
    additional_column.rows[0].push(Value::Text("present".to_owned()));
    additional_column_expected.columns.push("extra".to_owned());
    additional_column_expected.rows[0].push(Value::Text("present".to_owned()));

    let mut additional_row = rich.clone();
    let mut additional_row_expected = rich_expected.clone();
    let second_row = vec![
        Value::Null,
        Value::Bool(false),
        Value::Int64(71),
        Value::Float64(-3.5),
        Value::Text("second result row".to_owned()),
        Value::Uuid(Uuid::from_u128(0x4321)),
        Value::Timestamp(1_725_000_000_001),
        Value::Json(serde_json::json!(["another", "shape"])),
        Value::Vector(vec![0.0, 4.0]),
        Value::TxId(TxId(43)),
    ];
    additional_row.rows.push(second_row.clone());
    additional_row_expected.rows.push(second_row);

    let mut rows_affected = rich.clone();
    let mut rows_affected_expected = rich_expected.clone();
    rows_affected.rows_affected = 17;
    rows_affected_expected.rows_affected = 17;

    let mut trace_text = rich.clone();
    let mut trace_text_expected = rich_expected.clone();
    trace_text.trace.physical_plan = "NestedLoop";
    trace_text.trace.index_used = Some("events_payload_idx".to_owned());
    trace_text.trace.rows_examined = 91;
    trace_text_expected.trace.physical_plan = "NestedLoop".to_owned();
    trace_text_expected.trace.index_used = Some("events_payload_idx".to_owned());
    trace_text_expected.trace.rows_examined = 91;

    let mut more_predicates = rich.clone();
    let mut more_predicates_expected = rich_expected.clone();
    more_predicates
        .trace
        .predicates_pushed
        .push("id > 7".into());
    more_predicates_expected
        .trace
        .predicates_pushed
        .push("id > 7".to_owned());

    let mut more_candidates = rich.clone();
    let mut more_candidates_expected = rich_expected.clone();
    more_candidates
        .trace
        .indexes_considered
        .push(IndexCandidate {
            name: "events_payload_idx".to_owned(),
            rejected_reason: "ordering is incompatible".into(),
        });
    more_candidates_expected
        .trace
        .indexes_considered
        .push(CanonicalIndexCandidate {
            name: "events_payload_idx".to_owned(),
            rejected_reason: "ordering is incompatible".to_owned(),
        });

    let mut trace_flags = rich.clone();
    let mut trace_flags_expected = rich_expected.clone();
    trace_flags.trace.sort_elided = false;
    trace_flags.trace.query_vector_source = Some(VectorIndexRef::new("events", "alternate"));
    trace_flags_expected.trace.sort_elided = false;
    trace_flags_expected.trace.query_vector_source =
        Some(VectorIndexRef::new("events", "alternate"));

    let mut more_dropped_indexes = rich.clone();
    let mut more_dropped_indexes_expected = rich_expected.clone();
    more_dropped_indexes
        .cascade
        .as_mut()
        .expect("rich result has a cascade report")
        .dropped_indexes
        .push("events_payload_idx".to_owned());
    more_dropped_indexes_expected
        .cascade
        .as_mut()
        .expect("rich expected result has a cascade report")
        .dropped_indexes
        .push("events_payload_idx".to_owned());

    let cases = [
        ("all value variants", rich, rich_expected),
        (
            "empty collections and absent options",
            sparse,
            sparse_expected,
        ),
        ("column value", renamed_columns, renamed_columns_expected),
        (
            "column and row item count",
            additional_column,
            additional_column_expected,
        ),
        (
            "row count and value payloads",
            additional_row,
            additional_row_expected,
        ),
        ("rows affected", rows_affected, rows_affected_expected),
        ("trace scalar fields", trace_text, trace_text_expected),
        (
            "predicate values and count",
            more_predicates,
            more_predicates_expected,
        ),
        (
            "index-candidate values and count",
            more_candidates,
            more_candidates_expected,
        ),
        (
            "trace boolean and vector source",
            trace_flags,
            trace_flags_expected,
        ),
        (
            "cascade dropped-index values and count",
            more_dropped_indexes,
            more_dropped_indexes_expected,
        ),
    ];
    for (name, result, expected) in cases {
        assert_query_codec_case(&result, &expected);
        assert_eq!(
            encode_query_result(&result).expect("repeat route-neutral encoding"),
            standard_bytes(&expected),
            "fixture-keyed encoding must not satisfy {name}",
        );
    }
}

#[test]
fn query_result_decoding_requires_one_complete_payload() {
    let expected = expected_result_with_every_value_kind();
    let bytes = standard_bytes(&expected);
    assert_eq!(
        decode_query_result(&bytes).expect("decode one complete query result"),
        expected
    );

    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(matches!(
        decode_query_result(&trailing),
        Err(ReadEncodingError::InvalidPayload)
    ));
    assert!(matches!(
        decode_query_result(&bytes[..bytes.len() - 1]),
        Err(ReadEncodingError::InvalidPayload)
    ));
    assert!(matches!(
        decode_query_result(&[0xff]),
        Err(ReadEncodingError::InvalidPayload)
    ));
}

#[test]
fn query_result_codec_rejects_wrong_row_arity_at_every_boundary() {
    let invalid_cases = [
        (
            QueryResult {
                columns: vec!["id".to_owned(), "payload".to_owned()],
                rows: vec![vec![Value::Int64(7)]],
                rows_affected: 0,
                trace: QueryTrace {
                    physical_plan: "Scan",
                    index_used: None,
                    predicates_pushed: smallvec::SmallVec::new(),
                    indexes_considered: smallvec::SmallVec::new(),
                    sort_elided: false,
                    query_vector_source: None,
                    rows_examined: 1,
                },
                cascade: None,
            },
            CanonicalQueryResult {
                columns: vec!["id".to_owned(), "payload".to_owned()],
                rows: vec![vec![Value::Int64(7)]],
                rows_affected: 0,
                trace: CanonicalQueryTrace {
                    physical_plan: "Scan".to_owned(),
                    index_used: None,
                    predicates_pushed: vec![],
                    indexes_considered: vec![],
                    sort_elided: false,
                    query_vector_source: None,
                    rows_examined: 1,
                },
                cascade: None,
            },
        ),
        (
            QueryResult {
                columns: vec!["id".to_owned()],
                rows: vec![vec![
                    Value::Int64(8),
                    Value::Text("unexpected second value".to_owned()),
                ]],
                rows_affected: 0,
                trace: QueryTrace {
                    physical_plan: "IndexScan",
                    index_used: Some("events_id".to_owned()),
                    predicates_pushed: smallvec::smallvec!["id = $id".into()],
                    indexes_considered: smallvec::SmallVec::new(),
                    sort_elided: true,
                    query_vector_source: None,
                    rows_examined: 1,
                },
                cascade: None,
            },
            CanonicalQueryResult {
                columns: vec!["id".to_owned()],
                rows: vec![vec![
                    Value::Int64(8),
                    Value::Text("unexpected second value".to_owned()),
                ]],
                rows_affected: 0,
                trace: CanonicalQueryTrace {
                    physical_plan: "IndexScan".to_owned(),
                    index_used: Some("events_id".to_owned()),
                    predicates_pushed: vec!["id = $id".to_owned()],
                    indexes_considered: vec![],
                    sort_elided: true,
                    query_vector_source: None,
                    rows_examined: 1,
                },
                cascade: None,
            },
        ),
    ];

    for (invalid, canonical) in invalid_cases {
        assert!(matches!(
            encode_query_result(&invalid),
            Err(ReadEncodingError::InvalidPayload)
        ));
        assert!(matches!(
            query_result_encoded_size(&invalid),
            Err(ReadEncodingError::InvalidPayload)
        ));
        let independently_constructed_bytes = standard_bytes(&canonical);
        assert!(matches!(
            decode_query_result(&independently_constructed_bytes),
            Err(ReadEncodingError::InvalidPayload)
        ));
    }
}

#[test]
fn cursor_encoding_covers_nonempty_more_and_empty_exhausted_pages() {
    let pages = [
        CursorPage {
            columns: vec!["id".to_owned(), "payload".to_owned()],
            rows: vec![vec![
                Value::Int64(7),
                Value::Text("complete row".to_owned()),
            ]],
            has_more: true,
        },
        CursorPage {
            columns: vec!["id".to_owned(), "payload".to_owned()],
            rows: vec![],
            has_more: false,
        },
    ];

    let mut encodings = Vec::new();
    for page in &pages {
        let expected = standard_bytes(page);
        let encoded = encode_cursor_page(page).expect("canonical cursor-page encoding");
        assert_eq!(encoded, expected);
        assert_eq!(
            cursor_page_encoded_size(page).expect("canonical cursor-page encoded size"),
            encoded.len()
        );
        assert_eq!(
            decode_cursor_page(&encoded).expect("canonical cursor-page decoding"),
            page.clone()
        );
        encodings.push(encoded);
    }
    assert_ne!(encodings[0], encodings[1]);
}

fn assert_cursor_codec_case(page: &CursorPage) {
    let expected = standard_bytes(page);
    let encoded = encode_cursor_page(page).expect("canonical cursor-page encoding");
    assert_eq!(encoded, expected);
    assert_eq!(
        cursor_page_encoded_size(page).expect("canonical cursor-page encoded size"),
        encoded.len()
    );
    assert_eq!(
        decode_cursor_page(&encoded).expect("canonical cursor-page decoding"),
        page.clone()
    );
}

#[test]
fn cursor_codec_mutation_matrix_covers_columns_rows_values_and_valid_page_shapes() {
    let full_row = vec![
        Value::Null,
        Value::Bool(false),
        Value::Int64(19),
        Value::Float64(-2.5),
        Value::Text("cursor mutation".to_owned()),
        Value::Uuid(Uuid::from_u128(0x9876)),
        Value::Timestamp(1_725_000_000_002),
        Value::Json(serde_json::json!({"page": 2})),
        Value::Vector(vec![8.0, 13.0]),
        Value::TxId(TxId(44)),
    ];
    let columns = [
        "null",
        "bool",
        "int",
        "float",
        "text",
        "uuid",
        "timestamp",
        "json",
        "vector",
        "tx",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    let cases = [
        CursorPage {
            columns: columns.clone(),
            rows: vec![full_row.clone()],
            has_more: true,
        },
        CursorPage {
            columns: columns.clone(),
            rows: vec![full_row.clone(), full_row.clone()],
            has_more: false,
        },
        CursorPage {
            columns: vec!["renamed".to_owned()],
            rows: vec![vec![Value::Text("different value".to_owned())]],
            has_more: false,
        },
        CursorPage {
            columns: vec!["id".to_owned()],
            rows: vec![],
            has_more: true,
        },
        CursorPage {
            columns: vec!["id".to_owned(), "payload".to_owned()],
            rows: vec![],
            has_more: false,
        },
    ];
    for page in cases {
        assert_cursor_codec_case(&page);
    }
}

#[test]
fn cursor_decoding_requires_one_complete_payload() {
    let page = CursorPage {
        columns: vec!["id".to_owned(), "payload".to_owned()],
        rows: vec![vec![
            Value::Int64(7),
            Value::Text("complete row".to_owned()),
        ]],
        has_more: true,
    };
    let bytes = standard_bytes(&page);
    assert_eq!(
        decode_cursor_page(&bytes).expect("decode one complete cursor page"),
        page
    );

    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(matches!(
        decode_cursor_page(&trailing),
        Err(ReadEncodingError::InvalidPayload)
    ));
    assert!(matches!(
        decode_cursor_page(&bytes[..bytes.len() - 1]),
        Err(ReadEncodingError::InvalidPayload)
    ));
    assert!(matches!(
        decode_cursor_page(&[0xff]),
        Err(ReadEncodingError::InvalidPayload)
    ));
}

#[test]
fn cursor_codec_rejects_wrong_row_arity_at_every_boundary() {
    let invalid = CursorPage {
        columns: vec!["id".to_owned(), "payload".to_owned()],
        rows: vec![vec![Value::Int64(7)]],
        has_more: true,
    };
    assert!(matches!(
        encode_cursor_page(&invalid),
        Err(ReadEncodingError::InvalidPayload)
    ));
    assert!(matches!(
        cursor_page_encoded_size(&invalid),
        Err(ReadEncodingError::InvalidPayload)
    ));
    assert!(matches!(
        decode_cursor_page(&standard_bytes(&invalid)),
        Err(ReadEncodingError::InvalidPayload)
    ));
}

fn tables_metadata_page() -> MetadataPage {
    MetadataPage {
        vocabulary: MetadataPageVocabulary::Tables,
        items: vec![
            MetadataItem::Table("events".to_owned()),
            MetadataItem::Table("observations".to_owned()),
        ],
        has_more: true,
        continuation: Some("tables-next".to_owned()),
    }
}

fn events_status_metadata_page() -> MetadataPage {
    let mut event = BTreeMap::new();
    event.insert("name".to_owned(), Value::Text("nightly".to_owned()));
    event.insert("queued".to_owned(), Value::Int64(3));
    MetadataPage {
        vocabulary: MetadataPageVocabulary::EventsStatus,
        items: vec![MetadataItem::EventStatus(event)],
        has_more: false,
        continuation: None,
    }
}

#[test]
fn metadata_encoding_covers_both_vocabularies_and_page_shapes() {
    let tables = tables_metadata_page();
    let events_status = events_status_metadata_page();

    let mut encodings = Vec::new();
    for page in [&tables, &events_status] {
        let expected = standard_bytes(page);
        let encoded = encode_metadata_page(page).expect("canonical metadata-page encoding");
        assert_eq!(encoded, expected);
        assert_eq!(
            metadata_page_encoded_size(page).expect("canonical metadata-page encoded size"),
            encoded.len()
        );
        assert_eq!(
            decode_metadata_page(&encoded).expect("canonical metadata-page decoding"),
            page.clone()
        );
        encodings.push(encoded);
    }
    assert_ne!(encodings[0], encodings[1]);
}

fn assert_metadata_codec_case(page: &MetadataPage) {
    let expected = standard_bytes(page);
    let encoded = encode_metadata_page(page).expect("canonical metadata-page encoding");
    assert_eq!(encoded, expected);
    assert_eq!(
        metadata_page_encoded_size(page).expect("canonical metadata-page encoded size"),
        encoded.len()
    );
    assert_eq!(
        decode_metadata_page(&encoded).expect("canonical metadata-page decoding"),
        page.clone()
    );
}

#[test]
fn metadata_codec_mutation_matrix_covers_vocabularies_items_continuations_and_page_shapes() {
    let mut first_event = BTreeMap::new();
    first_event.insert("null".to_owned(), Value::Null);
    first_event.insert("bool".to_owned(), Value::Bool(false));
    first_event.insert("int".to_owned(), Value::Int64(-31));
    first_event.insert("float".to_owned(), Value::Float64(7.25));
    first_event.insert("name".to_owned(), Value::Text("hourly".to_owned()));
    first_event.insert("uuid".to_owned(), Value::Uuid(Uuid::from_u128(0x123456)));
    first_event.insert("timestamp".to_owned(), Value::Timestamp(1_725_000_000_003));
    first_event.insert("json".to_owned(), Value::Json(serde_json::json!([1, 2, 3])));
    first_event.insert("vector".to_owned(), Value::Vector(vec![1.0, 2.0]));
    first_event.insert("tx".to_owned(), Value::TxId(TxId(45)));
    let mut second_event = BTreeMap::new();
    second_event.insert("name".to_owned(), Value::Text("weekly".to_owned()));
    second_event.insert("enabled".to_owned(), Value::Bool(true));

    let cases = [
        MetadataPage {
            vocabulary: MetadataPageVocabulary::Tables,
            items: vec![
                MetadataItem::Table("events".to_owned()),
                MetadataItem::Table("observations".to_owned()),
                MetadataItem::Table("z_archive".to_owned()),
            ],
            has_more: true,
            continuation: Some("tables-after-z_archive".to_owned()),
        },
        MetadataPage {
            vocabulary: MetadataPageVocabulary::Tables,
            items: vec![],
            has_more: false,
            continuation: None,
        },
        MetadataPage {
            vocabulary: MetadataPageVocabulary::EventsStatus,
            items: vec![MetadataItem::EventStatus(first_event)],
            has_more: true,
            continuation: Some("events-after-hourly".to_owned()),
        },
        MetadataPage {
            vocabulary: MetadataPageVocabulary::EventsStatus,
            items: vec![
                MetadataItem::EventStatus(second_event),
                MetadataItem::EventStatus(BTreeMap::new()),
            ],
            has_more: false,
            continuation: None,
        },
    ];
    for page in cases {
        assert_metadata_codec_case(&page);
    }
}

#[test]
fn metadata_decoding_requires_one_complete_payload() {
    let pages = [tables_metadata_page(), events_status_metadata_page()];
    for page in pages {
        let bytes = standard_bytes(&page);
        assert_eq!(
            decode_metadata_page(&bytes).expect("decode one complete metadata page"),
            page
        );

        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(matches!(
            decode_metadata_page(&trailing),
            Err(ReadEncodingError::InvalidPayload)
        ));
        assert!(matches!(
            decode_metadata_page(&bytes[..bytes.len() - 1]),
            Err(ReadEncodingError::InvalidPayload)
        ));
    }
    assert!(matches!(
        decode_metadata_page(&[0xff]),
        Err(ReadEncodingError::InvalidPayload)
    ));
}

#[test]
fn metadata_codec_rejects_every_structurally_invalid_page() {
    let tables = tables_metadata_page();
    let events_status = events_status_metadata_page();

    let missing_continuation = MetadataPage {
        continuation: None,
        ..tables.clone()
    };
    let unexpected_continuation = MetadataPage {
        has_more: false,
        continuation: Some("events-next".to_owned()),
        ..events_status.clone()
    };
    let table_item_in_events_page = MetadataPage {
        vocabulary: MetadataPageVocabulary::EventsStatus,
        items: vec![MetadataItem::Table("events".to_owned())],
        has_more: false,
        continuation: None,
    };
    let event_item = match &events_status.items[0] {
        MetadataItem::EventStatus(event) => event.clone(),
        MetadataItem::Table(_) => unreachable!("events fixture carries an event-status item"),
    };
    let event_item_in_tables_page = MetadataPage {
        vocabulary: MetadataPageVocabulary::Tables,
        items: vec![MetadataItem::EventStatus(event_item)],
        has_more: false,
        continuation: None,
    };

    let invalid_pages = [
        missing_continuation,
        unexpected_continuation,
        table_item_in_events_page,
        event_item_in_tables_page,
    ];
    for page in invalid_pages {
        assert!(matches!(
            encode_metadata_page(&page),
            Err(ReadEncodingError::InvalidPayload)
        ));
        assert!(matches!(
            metadata_page_encoded_size(&page),
            Err(ReadEncodingError::InvalidPayload)
        ));
        let bytes = standard_bytes(&page);
        assert!(matches!(
            decode_metadata_page(&bytes),
            Err(ReadEncodingError::InvalidPayload)
        ));
    }
}

#[test]
fn metadata_codec_handles_empty_pages_for_every_vocabulary_at_every_boundary() {
    for vocabulary in [
        MetadataPageVocabulary::Tables,
        MetadataPageVocabulary::EventsStatus,
    ] {
        let non_progressing = MetadataPage {
            vocabulary,
            items: vec![],
            has_more: true,
            continuation: Some("next".to_owned()),
        };
        assert!(matches!(
            encode_metadata_page(&non_progressing),
            Err(ReadEncodingError::InvalidPayload)
        ));
        assert!(matches!(
            metadata_page_encoded_size(&non_progressing),
            Err(ReadEncodingError::InvalidPayload)
        ));
        assert!(matches!(
            decode_metadata_page(&standard_bytes(&non_progressing)),
            Err(ReadEncodingError::InvalidPayload)
        ));

        let exhausted = MetadataPage {
            has_more: false,
            continuation: None,
            ..non_progressing
        };
        assert_metadata_codec_case(&exhausted);
    }
}
