//! Independent byte and allocation oracles for encoded response leaves.
//! Source fixtures and legacy DTO oracles are prepared before measurement.
//! These tests do not claim that upstream DTO construction or history scratch is funded.
use super::*;
use crate::database::{CascadeReport, IndexCandidate, QueryResultData};
use crate::direct_file_reader::{
    DirectMetadataBody, DirectMetadataResponse, DirectQuery, DirectSnapshot,
};
use crate::local_transport::query_funding::Funded;
use crate::memory_accounting::{AccountantHandle, MemoryAccountant};
use crate::persistence::content_identity::proof_allocation_oracle_tests as physical;
use contextdb_core::read_contract::{MetadataItem, MetadataPageVocabulary};
use contextdb_core::read_memory::ReadMemoryOwner;
use serial_test::serial;
use std::collections::BTreeMap;
use std::sync::Arc;

const CEILING: u64 = 8 * 1024 * 1024;

// Existing global free watches must not retain a counter address past the
// original accountant, including when an assertion unwinds.
struct FreeWatch;
impl Drop for FreeWatch {
    fn drop(&mut self) {
        physical::disarm_source_free_watch();
    }
}

// A real host-supplied budget, passed unchanged to the production serializer.
// The accountant pin observes the payer; it never retains encoded output.
fn budget() -> (AccountantHandle, ReadMemoryOwner) {
    let host = Arc::new(MemoryAccountant::with_budget(CEILING as usize));
    let handle = AccountantHandle::from_host(host.clone());
    (handle, host.into())
}

fn trace() -> QueryTrace {
    let mut trace = QueryTrace::scan();
    trace.index_used = Some("ordered_π".into());
    trace.predicates_pushed.push("body != NULL".into());
    trace.indexes_considered.push(IndexCandidate {
        name: "other_index".into(),
        rejected_reason: "wrong prefix".into(),
    });
    trace.query_vector_source = Some(VectorIndexRef::new("events", "embedding"));
    trace.sort_elided = true;
    trace.rows_examined = 913;
    trace.maintained_count = Some(Box::new(
        crate::maintained_counts::MaintainedCountDisclosure::NotDeclared {
            table: "events".into(),
            columns: vec![("id".into(), contextdb_core::SortDirection::Desc)],
            already_declared: vec![vec![("body".into(), contextdb_core::SortDirection::Asc)]],
        },
    ));
    // Go through the old owned disclosure conversion, never the new borrowed view.
    trace.vector_search = Some(Box::new(
        VectorSearchDisclosure::try_from(CanonicalVectorSearchDisclosure {
            route_evidence: "executed".into(),
            requested_mode: "AUTO".into(),
            resolved_mode: "INDEXED".into(),
            aggregate_allowed_vectors: Some(1234),
            effective_auto_index_at: 1000,
            auto_index_at_source: "default".into(),
            effective_flat_below: 31,
            flat_below_source: "declared".into(),
            effective_search_radius_millionths: 2345,
            search_radius_source: "declared".into(),
            effective_auto_expansion_ceiling: 67,
            auto_expansion_ceiling_source: "declared".into(),
            partition_key_columns: vec!["context".into()],
            scope: "one".into(),
            route: Some("indexed".into()),
            base: "present".into(),
            change: "absent".into(),
            tail: "empty".into(),
            residual: "bounded".into(),
            fallback: Some("fallback text".into()),
            refusal: Some("refusal text".into()),
            recovery: Some("recovery text".into()),
            query_source: "<vector>".into(),
            partition_hnsw: vec![CanonicalVectorPartitionHnswDisclosure {
                partition: "partition-0".into(),
                route: Some("indexed".into()),
                reason: Some("cost".into()),
                hnsw_m: 17,
                hnsw_ef_construction: 201,
                hnsw_ef_search: 301,
                ef_search_source: "declared".into(),
                policy_revision: 73,
            }],
            global_candidate_budget: Some(101),
            admission_evaluations: Some(102),
            expansion_evaluations: Some(103),
            rescore_evaluations: Some(104),
            layers_admitted: Some(105),
            budget_terminal: Some("complete".into()),
        })
        .unwrap(),
    ));
    trace
}

fn data(repeat: usize) -> QueryResultData {
    let row = vec![
        Value::Null,
        Value::Bool(true),
        Value::Int64(-8193),
        Value::Float64(-0.0),
        Value::Text("\0\n\"\\π".repeat(repeat)),
        Value::Uuid(uuid::Uuid::from_u128(19)),
        Value::Timestamp(-23),
        Value::Json(serde_json::json!({"z": [null, true, 2.5], "a": "\u{0000}π\n\\\""})),
        Value::Vector(vec![0.0, -0.0, 1.25]),
        Value::TxId(contextdb_core::TxId(257)),
    ];
    QueryResultData {
        columns: (0..row.len()).map(|i| format!("column_{i}")).collect(),
        rows: vec![row],
        rows_affected: 251,
        trace: trace(),
        cascade: Some(CascadeReport {
            dropped_indexes: vec!["index_π".into()],
        }),
    }
}

fn prior_query_bytes(data: &QueryResultData) -> Vec<u8> {
    // Serialize the existing owned public DTO with Value's original Serialize.
    // No QueryRows, QueryValue, QueryTraceView or new encode helper is used here.
    bincode::serde::encode_to_vec(
        CanonicalQueryResult {
            columns: data.columns.clone(),
            rows: data.rows.clone(),
            rows_affected: data.rows_affected,
            trace: CanonicalQueryTrace::from(&data.trace),
            cascade: data.cascade.as_ref().map(|c| CanonicalCascadeReport {
                dropped_indexes: c.dropped_indexes.clone(),
            }),
        },
        standard(),
    )
    .unwrap()
}

// Promise: funded query output preserves every original wire field and Value tag.
// Decisive mutant: swap the borrowed vector partition/tail fields or stringify JSON differently.
#[test]
#[serial]
fn funded_query_bytes_match_the_prior_owned_dto() {
    let (accountant, owner) = budget();
    let baseline = accountant.usage().used;
    // Empty and sparse rows cover absent disclosures independently of the rich
    // rows below; all remain rows of the same exact-byte proof.
    let sparse = QueryResultData {
        columns: vec!["id".into()],
        rows: vec![vec![Value::Int64(0)]],
        rows_affected: 0,
        trace: QueryTrace::scan(),
        cascade: None,
    };
    for data in [
        QueryResultData::empty(),
        sparse,
        data(0),
        data(1),
        data(32 * 1024),
    ] {
        let expected = prior_query_bytes(&data);
        let actual = encode_query_result_funded(&data, owner.clone(), CEILING).unwrap();
        assert_eq!(actual.value, expected);
        assert_eq!(actual.value.capacity(), expected.len());
        assert_eq!(accountant.usage().used - baseline, expected.len());
        drop(actual);
        assert_eq!(accountant.usage().used, baseline);
    }
}

// Promise: live cursor output has the legacy page/owned-trace wire order, including
// the observation's rows_examined override. Mutant: serialize the trace's stale count.
#[test]
#[serial]
fn funded_cursor_bytes_match_the_prior_public_page_and_owned_trace() {
    let (accountant, owner) = budget();
    for repeat in [0, 1, 32 * 1024] {
        for has_more in [false, true] {
            let data = data(repeat);
            let public = CursorPage::from_owned_parts_for_test(
                owner.clone(),
                data.columns.clone(),
                data.rows.clone(),
                has_more,
            )
            .unwrap();
            let page = crate::owned_cursor_page::OwnedCursorPage::from_parts(
                data.columns,
                data.rows,
                has_more,
            );
            let observation = crate::executor::CursorObservation {
                trace: Arc::new(data.trace),
                rows_examined: 719,
                charge: None,
                group_page_returns: None,
            };
            let mut prior_trace = CanonicalQueryTrace::from(observation.trace.as_ref());
            prior_trace.rows_examined = observation.rows_examined;
            // Tuple fields encode like the old observed-page struct; the page is
            // the pre-existing PUBLIC serializer, not BorrowedObservedPage.
            let expected =
                bincode::serde::encode_to_vec((&public, prior_trace), standard()).unwrap();
            let baseline = accountant.usage().used;
            let actual = cursor_observation::encode_observed_cursor_page_funded(
                &page,
                &observation,
                owner.clone(),
                CEILING,
            )
            .unwrap();
            assert_eq!(actual.value, expected);
            assert_eq!(
                cursor_observation::observed_cursor_page_encoded_size(&page, &observation).unwrap(),
                expected.len()
            );
            assert_eq!(accountant.usage().used - baseline, expected.len());
            drop(actual);
            assert_eq!(accountant.usage().used, baseline);
        }
    }
}

fn metadata_page(repeat: usize) -> MetadataPage {
    MetadataPage {
        vocabulary: MetadataPageVocabulary::EventsStatus,
        items: vec![MetadataItem::EventStatus(BTreeMap::from([
            ("z".into(), Value::Text("\n\"π".repeat(repeat))),
            (
                "a".into(),
                Value::Json(serde_json::json!({"z": [1, false], "a": "\\\nπ"})),
            ),
        ]))],
        has_more: true,
        continuation: Some("next\0π".into()),
    }
}

fn prefixed_json(tag: u8, has_more: bool, json: &[u8]) -> Vec<u8> {
    // Fixed external byte oracle, no CanonicalWriter or production rendering.
    let mut bytes = vec![tag, u8::from(has_more)];
    bytes.extend_from_slice(&(json.len() as u64).to_le_bytes());
    bytes.extend_from_slice(json);
    bytes
}

// Promise: custody JSON retains struct field order; metadata pages retain Value
// tags and map order. Mutant: route json_text through a sorted serde_json::Value.
#[test]
#[serial]
fn funded_metadata_keeps_fixed_json_order_and_prior_page_bytes() {
    let (accountant, owner) = budget();
    let baseline = accountant.usage().used;
    let bodies = [
        (DirectMetadataBody::DeliveryStatus { counts: Some(crate::DeliveryStatusCounts {
            disabled: false, eligible: 17, accepted: 3, equivalent: 5, pending: 7, refused: 2,
        }), has_more: true }, prefixed_json(6, true, br#"{"disabled":false,"eligible":17,"accepted":3,"equivalent":5,"pending":7,"refused":2}"#)),
        (DirectMetadataBody::DeliveryStatus { counts: None, has_more: false }, prefixed_json(6, false, b"null")),
        (DirectMetadataBody::DeliveryOutcome { outcome: None, has_more: false }, prefixed_json(7, false, b"null")),
    ];
    for (body, expected) in bodies {
        let actual = encode_metadata_body_funded(&body, owner.clone(), CEILING).unwrap();
        assert_eq!(actual.value, expected);
        assert_eq!(encoded_metadata_body_len(&body), expected.len());
        assert_eq!(accountant.usage().used - baseline, expected.len());
        drop(actual);
        assert_eq!(accountant.usage().used, baseline);
    }
    let tables = MetadataPage {
        vocabulary: MetadataPageVocabulary::Tables,
        items: vec![
            MetadataItem::Table("z_π".into()),
            MetadataItem::Table("a".into()),
        ],
        has_more: false,
        continuation: None,
    };
    let empty = MetadataPage {
        vocabulary: MetadataPageVocabulary::EventsStatus,
        items: vec![],
        has_more: false,
        continuation: None,
    };
    for page in [
        tables,
        empty,
        metadata_page(0),
        metadata_page(1),
        metadata_page(32 * 1024),
    ] {
        let expected = bincode::serde::encode_to_vec(&page, standard()).unwrap();
        let actual = encode_metadata_page_funded(&page, owner.clone(), CEILING).unwrap();
        assert_eq!(actual.value, expected);
        assert_eq!(accountant.usage().used - baseline, expected.len());
        drop(actual);
        assert_eq!(accountant.usage().used, baseline);
    }
}

// Promise: the immutable output keeps the SAME payer through real response-field
// escape, allocation-free clones and final physical free. Mutant: omit/rebill a
// credit, deep-copy Clone, or release either invoice before System.dealloc.
#[test]
#[serial]
fn encoded_response_field_escape_keeps_exact_original_invoices_until_physical_free() {
    for metadata in [false, true] {
        let (accountant, owner) = budget();
        let data = data(1024);
        let body = DirectMetadataBody::Tables {
            items: vec!["table_π".repeat(2048)],
            has_more: false,
        };
        let expected = if metadata {
            // Independent Tables document oracle: tag, item count, UTF-8
            // length/content, then has_more. Never call its production writer.
            let name = "table_π".repeat(2048);
            let mut bytes = vec![0];
            bytes.extend_from_slice(&1u64.to_le_bytes());
            bytes.extend_from_slice(&(name.len() as u64).to_le_bytes());
            bytes.extend_from_slice(name.as_bytes());
            bytes.push(0);
            bytes
        } else {
            prior_query_bytes(&data)
        };
        let pre_fixture = accountant.usage().used;
        // Fund the real input before measuring only output allocation and escape.
        // Keep it live until both output invoices have been physically released.
        let metadata_fixture = metadata.then(|| {
            crate::read_session::MetadataAnswer::try_new(
                body,
                None,
                Default::default(),
                owner.clone(),
            )
            .unwrap()
        });
        let baseline = accountant.usage().used;
        let admitted = accountant.admitted_bytes_for_test();
        let counter = accountant.used_counter_for_observation();
        let (encoded, requests, count) = physical::source_requests_for_test(counter, || {
            let funded = if metadata {
                encode_metadata_body_funded(
                    &metadata_fixture.as_ref().unwrap().body,
                    owner.clone(),
                    CEILING,
                )
            } else {
                encode_query_result_funded(&data, owner.clone(), CEILING)
            }
            .unwrap();
            EncodedReadBytes::from_funded(funded, CEILING).unwrap()
        });
        assert_eq!(
            count, 2,
            "one real output buffer plus one real native carrier request"
        );
        let payload = requests[0];
        let control = requests[1];
        assert_eq!(payload.0, encoded.as_ptr() as usize);
        assert_eq!(payload.1, expected.len());
        assert_eq!(payload.2, std::mem::align_of::<u8>());
        assert_ne!(control.0, payload.0);
        let total = payload.1 + control.1;
        assert_eq!(accountant.usage().used - baseline, total);
        assert_eq!(accountant.admitted_bytes_for_test() - admitted, total);
        let sampled = physical::source_request_credit_for_test();
        assert_eq!(
            sampled[0] - baseline,
            payload.1,
            "payload admission precedes System allocation"
        );
        assert_eq!(
            sampled[1] - baseline,
            total,
            "carrier admission precedes System allocation"
        );
        assert_eq!(physical::source_unfunded_peak_for_test().0, 0);
        physical::watch_source_free_for_test(&requests[..count], counter);
        let _free_watch = FreeWatch;
        let snapshot = DirectSnapshot { image_id: [29; 32] };
        let ((escaped, alias), _, clone_count) = physical::native_requests_for_test(|| {
            let alias = encoded.clone();
            let escaped = if metadata {
                let response = DirectMetadataResponse {
                    metadata: metadata_fixture.as_ref().unwrap().clone(),
                    canonical_bytes: encoded,
                    snapshot,
                };
                response.canonical_bytes
            } else {
                let response = DirectQuery {
                    result: QueryResult::empty(),
                    canonical_bytes: encoded,
                    snapshot,
                };
                response.canonical_bytes
            };
            (escaped, alias)
        });
        assert_eq!(
            clone_count, 0,
            "Clone and owning field projection allocate nothing"
        );
        assert_eq!(escaped.as_ptr(), alias.as_ptr());
        assert_eq!(escaped.as_slice(), expected);
        assert_eq!(
            accountant.admitted_bytes_for_test() - admitted,
            total,
            "no second invoice or payer"
        );
        drop(escaped);
        assert_eq!(accountant.usage().used - baseline, total);
        assert_eq!(physical::source_free_for_test(count)[0].2, 0);
        assert_eq!(physical::source_free_for_test(count)[1].2, 0);
        assert_eq!(alias.as_slice(), expected);
        drop(alias);
        let free = physical::source_free_for_test(count);
        assert_eq!(
            free[0],
            (baseline + total, baseline + total, 1),
            "payload physically freed before payload invoice refunds"
        );
        assert_eq!(
            free[1],
            (baseline + control.1, baseline + control.1, 1),
            "native control physically freed before control invoice refunds"
        );
        assert_eq!(accountant.usage().used, baseline);
        drop(metadata_fixture);
        assert_eq!(accountant.usage().used, pre_fixture);
        assert_eq!(accountant.usage().used, 0);
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }
}

// Promise: size-only query/cursor/metadata operations never build complete output.
// Mutant: size via encode(...).len(), or json_text via to_string. Historical
// ChangeLog write_rendered scratch is deliberately NOT covered by this assertion.
#[test]
#[serial]
fn response_size_only_operations_do_not_allocate_output_or_json_scratch() {
    let (accountant, owner) = budget();
    let data = data(32 * 1024);
    let expected_query = prior_query_bytes(&data);
    let page = crate::owned_cursor_page::OwnedCursorPage::from_parts(
        data.columns.clone(),
        data.rows.clone(),
        false,
    );
    let observation = crate::executor::CursorObservation {
        trace: Arc::new(data.trace.clone()),
        rows_examined: 719,
        charge: None,
        group_page_returns: None,
    };
    let expected_cursor = cursor_observation::encode_observed_cursor_page(&page, &observation)
        .unwrap()
        .len();
    let result = QueryResult::from_owned_parts_for_test(
        owner,
        data.columns,
        data.rows,
        data.rows_affected,
        data.trace,
        data.cascade,
    )
    .unwrap();
    let metadata = metadata_page(32 * 1024);
    let expected_metadata = bincode::serde::encode_to_vec(&metadata, standard())
        .unwrap()
        .len();
    let body = DirectMetadataBody::DeliveryStatus {
        counts: Some(crate::DeliveryStatusCounts::default()),
        has_more: false,
    };
    let expected_body = prefixed_json(
        6,
        false,
        br#"{"disabled":false,"eligible":0,"accepted":0,"equivalent":0,"pending":0,"refused":0}"#,
    )
    .len();
    let baseline = accountant.usage().used;
    let (sizes, _, count) = physical::native_requests_for_test(|| {
        (
            query_result_encoded_size(&result).unwrap(),
            cursor_observation::observed_cursor_page_encoded_size(&page, &observation).unwrap(),
            metadata_page_encoded_size(&metadata).unwrap(),
            encoded_metadata_body_len(&body),
        )
    });
    assert_eq!(
        sizes,
        (
            expected_query.len(),
            expected_cursor,
            expected_metadata,
            expected_body
        )
    );
    assert_eq!(
        count, 0,
        "existing allocator must observe zero requests, not merely zero retained bytes"
    );
    assert_eq!(accountant.usage().used, baseline);
}

// Promise: an already-open real budget refuses the output leaf before requesting
// the output allocation. Mutant: allocate Vec first, then seek its invoice.
// Error-diagnostic allocations are not confused with the refused output buffer.
#[test]
#[serial]
fn original_budget_refuses_each_response_leaf_before_its_output_allocation() {
    let data = data(32 * 1024);
    let metadata = metadata_page(32 * 1024);
    let body = DirectMetadataBody::Tables {
        items: vec!["x".repeat(128 * 1024)],
        has_more: false,
    };
    let page = crate::owned_cursor_page::OwnedCursorPage::from_parts(
        data.columns.clone(),
        data.rows.clone(),
        false,
    );
    let observation = crate::executor::CursorObservation {
        trace: Arc::new(data.trace.clone()),
        rows_examined: 719,
        charge: None,
        group_page_returns: None,
    };
    let lengths = [
        prior_query_bytes(&data).len(),
        bincode::serde::encode_to_vec(&metadata, standard())
            .unwrap()
            .len(),
        encoded_metadata_body_len(&body),
        cursor_observation::encode_observed_cursor_page(&page, &observation)
            .unwrap()
            .len(),
    ];
    for (leaf, length) in lengths.into_iter().enumerate() {
        let (accountant, owner) = budget();
        // Occupy real storage on this same owner, not a made-up reservation or
        // a root constructor failure. Exactly length-1 bytes remain available.
        let pressure_bytes = accountant.usage().available.unwrap() - (length - 1);
        let pressure = Funded::allocate_owned(Some(owner.clone()), pressure_bytes, || {
            vec![0u8; pressure_bytes]
        })
        .unwrap();
        assert_eq!(accountant.usage().available, Some(length - 1));
        let used = accountant.usage().used;
        let admitted = accountant.admitted_bytes_for_test();
        let (refusal, requests, count) =
            physical::source_requests_for_test(accountant.used_counter_for_observation(), || {
                match leaf {
                    0 => encode_query_result_funded(&data, owner.clone(), CEILING),
                    1 => encode_metadata_page_funded(&metadata, owner.clone(), CEILING),
                    2 => encode_metadata_body_funded(&body, owner.clone(), CEILING),
                    3 => cursor_observation::encode_observed_cursor_page_funded(
                        &page,
                        &observation,
                        owner.clone(),
                        CEILING,
                    ),
                    _ => unreachable!(),
                }
            });
        assert!(
            matches!(
                refusal,
                Err(ReadEncodingError::MemoryCeilingExceeded { ceiling: CEILING })
            ),
            "leaf {leaf}: actual output admission must refuse"
        );
        assert!(
            count <= requests.len(),
            "allocation receipt must not truncate"
        );
        assert!(
            requests[..count].iter().all(|request| request.1 != length),
            "leaf {leaf}: refused output must have no real allocation request"
        );
        assert_eq!(accountant.admitted_bytes_for_test(), admitted);
        assert_eq!(accountant.usage().used, used);
        drop(refusal);
        drop(pressure);
        assert_eq!(accountant.usage().used, 0);
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }
}

// Learn the control Layout from an actual request, not the implementation's
// ReadShared::layout() or a hand-priced control-header formula.
fn actual_encoded_control_layout(
    data: &QueryResultData,
    owner: &ReadMemoryOwner,
) -> std::alloc::Layout {
    let (probe, requests, count) = physical::native_requests_for_test(|| {
        EncodedReadBytes::from_funded(
            encode_query_result_funded(data, owner.clone(), CEILING).unwrap(),
            CEILING,
        )
        .unwrap()
    });
    assert_eq!(count, 2);
    let layout = std::alloc::Layout::from_size_align(requests[1].1, requests[1].2).unwrap();
    drop(probe);
    layout
}

// Promise: control admission refusal and a real null native-cell allocation both
// release the original payload, with its own credit still live at physical free.
// Mutants: leak the payload invoice on failure, or drop it before dropping Vec.
#[test]
#[serial]
fn encoded_carrier_control_refusal_and_null_allocation_roll_back_the_original_budget() {
    for null_allocation in [false, true] {
        let (accountant, owner) = budget();
        let data = data(1024);
        let layout = actual_encoded_control_layout(&data, &owner);
        let counter = accountant.used_counter_for_observation();
        let (funded, requests, count) = physical::source_requests_for_test(counter, || {
            encode_query_result_funded(&data, owner.clone(), CEILING).unwrap()
        });
        assert_eq!(count, 1);
        let payload = requests[0];
        assert_eq!(payload.0, funded.value.as_ptr() as usize);
        physical::watch_source_free_for_test(&requests[..count], counter);
        let _free_watch = FreeWatch;
        let pressure = if null_allocation {
            None
        } else {
            let bytes = accountant.usage().available.unwrap() - (layout.size() - 1);
            Some(Funded::allocate_owned(Some(owner.clone()), bytes, || vec![0u8; bytes]).unwrap())
        };
        let before = accountant.usage().used;
        if null_allocation {
            crate::vector_read_memory::native_ownership_proof::arm_refuse_one_live_layout(layout);
        }
        let (failure, actual_requests, attempted) =
            physical::source_requests_for_test(counter, || {
                EncodedReadBytes::from_funded(funded, CEILING)
            });
        if null_allocation {
            assert!(
                !crate::vector_read_memory::native_ownership_proof::refusal_armed_for_test(),
                "the real native allocation reached the existing null hook"
            );
            assert!(matches!(failure, Err(ReadEncodingError::InvalidPayload)));
        } else {
            assert!(matches!(
                failure,
                Err(ReadEncodingError::MemoryCeilingExceeded { ceiling: CEILING })
            ));
            assert!(attempted <= actual_requests.len());
            assert!(
                actual_requests[..attempted]
                    .iter()
                    .all(|r| (r.1, r.2) != (layout.size(), layout.align())),
                "refused control must never request physical storage"
            );
        }
        assert_eq!(
            accountant.usage().used,
            before - payload.1,
            "payload and tentative control invoices both roll back"
        );
        let freed = physical::source_free_for_test(1)[0];
        assert_eq!(
            freed.2, 1,
            "the original real buffer was physically freed exactly once"
        );
        assert_eq!(freed.0, freed.1, "payload invoice survives System.dealloc");
        assert!(
            freed.0 >= before,
            "original payload invoice remains paid through its physical free"
        );
        drop(failure);
        drop(pressure);
        assert_eq!(accountant.usage().used, 0);
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }
}

// Promise: failing or unwinding serialization after admission returns the output
// invoice. Mutant: forget the admitted output on an encode error or unwind.
// This tests only the generic funded encoder's rollback; it cannot make an
// arbitrary standalone codec a funded consumer boundary.
#[test]
#[serial]
fn funded_output_rolls_back_on_second_pass_serializer_failure_and_unwind() {
    struct FailsOnWrite {
        visits: std::cell::Cell<usize>,
        unwind: bool,
    }
    impl Serialize for FailsOnWrite {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let visit = self.visits.get();
            self.visits.set(visit + 1);
            if visit != 0 {
                if self.unwind {
                    panic!("independent serializer unwind after admission");
                }
                return Err(serde::ser::Error::custom(
                    "independent serializer failure after admission",
                ));
            }
            serializer.serialize_bytes(&[71; 4096])
        }
    }
    for unwind in [false, true] {
        let (accountant, owner) = budget();
        let value = FailsOnWrite {
            visits: std::cell::Cell::new(0),
            unwind,
        };
        let length =
            bincode::serde::encode_to_vec(serde_bytes::Bytes::new(&[71; 4096]), standard())
                .unwrap()
                .len();
        let admitted = accountant.admitted_bytes_for_test();
        let (outcome, requests, count) =
            physical::source_requests_for_test(accountant.used_counter_for_observation(), || {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    encode_transport_funded(&value, Some(owner.clone()), CEILING)
                }))
            });
        assert_eq!(
            value.visits.get(),
            2,
            "failure is after counting, on the real write pass"
        );
        if unwind {
            assert!(outcome.is_err());
        } else {
            assert!(matches!(
                outcome,
                Ok(Err(ReadEncodingError::InvalidPayload))
            ));
        }
        assert!(count <= requests.len());
        assert_eq!(
            requests[..count].iter().filter(|r| r.1 == length).count(),
            1,
            "the actual output allocation happened before failure"
        );
        assert_eq!(accountant.admitted_bytes_for_test() - admitted, length);
        assert_eq!(accountant.usage().used, 0);
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }
}

// Promise: native default request ownership survives after the caller drops its
// child handle; neither cloning nor a refusal invents a new child or parent.
// Mutant: replace the real child with a fresh owner or release its invoice early.
// Physical byte/control free ordering is asserted separately on the same carrier
// with a real host budget; this row does not pin the native child for observation.
#[test]
#[serial]
fn encoded_output_retains_the_original_native_request_without_an_observer_alias() {
    let parent = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(CEILING).unwrap();
    let root_only = parent.retained_bytes_for_test();
    let (child, _) =
        crate::vector_read_memory::statement_child_proof::observe(&parent, CEILING, || {
            parent.child().unwrap()
        });
    let observer = crate::vector_read_memory::declared_child_observer(child.clone()).unwrap();
    let standing = parent.retained_bytes_for_test();
    let initial = observer.sample();
    assert!(initial.alive);
    assert_eq!(initial.charged_bytes, 0);
    let data = data(1024);
    let expected = prior_query_bytes(&data);
    let (encoded, requests, count) = physical::native_requests_for_test(|| {
        EncodedReadBytes::from_funded(
            encode_query_result_funded(&data, child.clone(), CEILING).unwrap(),
            CEILING,
        )
        .unwrap()
    });
    assert_eq!(count, 2);
    let total = requests[..count].iter().map(|r| r.1 as u64).sum::<u64>();
    let funded = observer.sample();
    assert_eq!(funded.owner_identity, initial.owner_identity);
    assert_eq!(funded.parent_identity, initial.parent_identity);
    assert_eq!(funded.charged_bytes, total);
    assert_eq!(parent.retained_bytes_for_test() - standing, total);
    let alias = encoded.clone();
    drop(child);
    drop(encoded);
    let escaped = observer.sample();
    assert!(
        escaped.alive,
        "only the escaping output retains the real child"
    );
    assert_eq!(escaped.charged_bytes, total);
    assert_eq!(alias.as_slice(), expected);
    drop(alias);
    let retired = observer.sample();
    assert!(
        !retired.alive,
        "no strong observation alias keeps the original child alive"
    );
    assert_eq!(retired.charged_bytes, 0);
    assert_eq!(parent.retained_bytes_for_test(), root_only);

    // A new, normally admitted request can be filled with real storage; refusal
    // must occur at its output leaf rather than at root/child construction.
    let (child, _) =
        crate::vector_read_memory::statement_child_proof::observe(&parent, CEILING, || {
            parent.child().unwrap()
        });
    let observer = crate::vector_read_memory::declared_child_observer(child.clone()).unwrap();
    let pressure_bytes =
        CEILING as usize - parent.retained_bytes_for_test() as usize - (expected.len() - 1);
    let pressure = Funded::allocate_owned(Some(child.clone()), pressure_bytes, || {
        vec![0u8; pressure_bytes]
    })
    .unwrap();
    let before = observer.sample();
    let (refusal, _, count) = physical::native_requests_for_test(|| {
        encode_query_result_funded(&data, child.clone(), CEILING)
    });
    assert!(matches!(
        refusal,
        Err(ReadEncodingError::MemoryCeilingExceeded { ceiling: CEILING })
    ));
    assert_eq!(
        count, 0,
        "native original-budget refusal makes no output or diagnostic allocation"
    );
    assert_eq!(observer.sample().charged_bytes, before.charged_bytes);
    assert_eq!(
        parent.retained_bytes_for_test(),
        before.parent_charged_bytes
    );
    drop(refusal);
    drop(pressure);
    assert_eq!(observer.sample().charged_bytes, 0);
    drop(child);
    assert!(!observer.sample().alive);
    assert_eq!(parent.retained_bytes_for_test(), root_only);
}
