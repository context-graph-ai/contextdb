#![cfg(feature = "test-seams")]
//! The read channel's error document is the boundary.
//!
//! A caller reading over the local channel is entitled to the answer an
//! in-process call would have given. That entitlement used to be met by
//! putting the engine's whole error enum on the wire, which made the engine's
//! error maintenance a protocol event: a positional encoder writes a variant's
//! POSITION, so appending a sync or trigger error anywhere but the end moved
//! the bytes a reader parses.
//!
//! This file proves the replacement. Every class a read can return travels
//! under a tag WRITTEN DOWN in the channel's own source; the bytes below were
//! derived by hand from bincode's standard grammar (an integer below 251 and a
//! length below 251 are each one byte) rather than printed from the encoder;
//! every class round-trips through the production encode/decode boundary under
//! a declared memory ceiling; and a class the channel has no tag for still
//! arrives carrying its name and its words.

use contextdb_core::Error;
use contextdb_core::read_contract::{
    OwnerLimitExceededDetail, ReadFailure, ReadFailureDetail, ReadFailureKind, ReadFailureLimit,
    ReadLimits,
};
use contextdb_core::types::{ContextId, Principal, RowId, ScopeLabel, Value, VectorIndexRef};
use contextdb_engine::local_transport::{
    ALL_TAGS, BodyField, LocalEngineFailure, ReadChannelError, TAG_ACL_DENIED,
    TAG_BFS_DEPTH_EXCEEDED, TAG_BFS_VISITED_EXCEEDED, TAG_COLUMN_NOT_FOUND,
    TAG_MEMORY_BUDGET_EXCEEDED, TAG_READ_CANCELLED, TAG_SCOPE_LABEL_VIOLATION, TAG_TABLE_NOT_FOUND,
    TAG_UNKNOWN, body_grammar, decode_message_exact, encode_message,
};
use std::collections::BTreeSet;

/// The ceiling a caller declares for an ordinary exchange in these proofs.
fn ceiling() -> u64 {
    ReadLimits::default().memory
}

/// One engine answer per class this channel names. Every sample carries real
/// field values, because a class that only ever travels empty proves nothing
/// about the fields a caller reads.
fn one_answer_per_named_class() -> Vec<Error> {
    vec![
        Error::ReadSessionNotImplemented,
        Error::OwnerReadDrainTimeout,
        Error::ReadCancelled,
        Error::ReadFailure(
            ReadFailure::new(ReadFailureKind::OwnerTimeout, ReadFailureDetail::None)
                .expect("a canonical empty-detail refusal"),
        ),
        Error::ParseError("unexpected token at 3".to_owned()),
        Error::PlanError("no plan for this shape".to_owned()),
        Error::SchemaInvalid {
            reason: "widgets has no primary key".to_owned(),
        },
        Error::TableNotFound("widgets".to_owned()),
        Error::ColumnNotFound {
            table: "widgets".to_owned(),
            column: "label".to_owned(),
        },
        Error::ColumnTypeMismatch {
            table: "widgets".to_owned(),
            column: "label".to_owned(),
            expected: "TEXT".to_owned(),
            actual: "TxId".to_owned(),
        },
        Error::IndexNotFound {
            table: "widgets".to_owned(),
            index: "widgets_label_idx".to_owned(),
        },
        Error::NotFound("no such snapshot".to_owned()),
        Error::RecursiveCteNotSupported,
        Error::WindowFunctionNotSupported,
        Error::StoredProcNotSupported,
        Error::SubqueryNotSupported,
        Error::FullTextSearchNotSupported,
        Error::OrderByExpressionNotSupported,
        Error::UnboundedTraversal,
        Error::UnboundedVectorSearch,
        Error::BfsDepthExceeded(7),
        Error::BfsVisitedExceeded(4_096),
        Error::UnknownVectorIndex {
            index: VectorIndexRef::new("widgets", "embedding"),
        },
        Error::VectorIndexDimensionMismatch {
            index: VectorIndexRef::new("widgets", "embedding"),
            expected: 768,
            actual: 384,
        },
        Error::PersistedRowVectorRowMissing {
            index: VectorIndexRef::new("widgets", "embedding"),
            key: "widgets/9".to_owned(),
        },
        Error::PersistedRowVectorCellNull {
            index: VectorIndexRef::new("widgets", "embedding"),
            key: "widgets/9".to_owned(),
        },
        Error::UseRankRequiresVectorOrder,
        Error::UseRankRequiresLimit,
        Error::RankPolicyNotFound {
            index: "widgets_embedding".to_owned(),
            sort_key: "freshness".to_owned(),
        },
        Error::RankPolicyColumnUnknown {
            index: "widgets_embedding".to_owned(),
            column: "updated".to_owned(),
        },
        Error::RankPolicyColumnAmbiguous {
            index: "widgets_embedding".to_owned(),
            column: "updated".to_owned(),
        },
        Error::RankPolicyColumnType {
            index: "widgets_embedding".to_owned(),
            column: "updated".to_owned(),
            expected: "INTEGER".to_owned(),
            actual: "TEXT".to_owned(),
        },
        Error::RankPolicyJoinTableUnknown {
            index: "widgets_embedding".to_owned(),
            table: "owners".to_owned(),
        },
        Error::RankPolicyJoinColumnUnknown {
            index: "widgets_embedding".to_owned(),
            table: "owners".to_owned(),
            column: "rank".to_owned(),
        },
        Error::RankPolicyJoinColumnUnindexed {
            index: "widgets_embedding".to_owned(),
            joined_table: "owners".to_owned(),
            column: "rank".to_owned(),
        },
        Error::RankPolicyFormulaParse {
            index: "widgets_embedding".to_owned(),
            position: 12,
            reason: "unbalanced parenthesis".to_owned(),
        },
        Error::PrincipalRequired {
            table: "widgets".to_owned(),
        },
        Error::AclDenied {
            table: "widgets".to_owned(),
            row_id: RowId(9),
            principal: Principal::Human("ada".to_owned()),
        },
        Error::ContextScopeViolation {
            requested: ContextId(uuid::Uuid::from_u128(11)),
            allowed: BTreeSet::from([
                ContextId(uuid::Uuid::from_u128(12)),
                ContextId(uuid::Uuid::from_u128(13)),
            ]),
        },
        Error::ScopeLabelViolation {
            requested: ScopeLabel("server".to_owned()),
            allowed: BTreeSet::from([ScopeLabel("edge".to_owned())]),
        },
        Error::StoreCorrupted {
            path: "/store/widgets.db".to_owned(),
            reason: "commit index missing".to_owned(),
        },
        Error::StoreIdentityUnprovable {
            path: "/store/widgets.db".to_owned(),
        },
        Error::LegacyVectorStoreDetected {
            found_format_marker: "0.9".to_owned(),
            expected_release: "1.0".to_owned(),
        },
        Error::DatabaseLocked {
            holder_pid: 4_321,
            path: std::path::PathBuf::from("/store/widgets.db"),
        },
        Error::MemoryBudgetExceeded {
            subsystem: "vector".to_owned(),
            operation: "candidate materialization".to_owned(),
            requested_bytes: 4_096,
            available_bytes: 128,
            budget_limit_bytes: 8_192,
            hint: "raise the read memory ceiling".to_owned(),
        },
        Error::DiskBudgetExceeded {
            operation: "checkpoint".to_owned(),
            current_bytes: 4_096,
            budget_limit_bytes: 2_048,
            hint: "raise the disk budget".to_owned(),
        },
        Error::Other("the owner could not say more".to_owned()),
    ]
}

/// Take one answer the whole way a caller's answer travels: the production
/// document, the production encoder, the production decoder under the caller's
/// declared ceiling, and back into an engine answer.
fn over_the_channel(answer: &Error) -> Error {
    LocalEngineFailure::from_error(answer).into_error(ceiling())
}

fn document_bytes(answer: &Error) -> Vec<u8> {
    encode_message(&ReadChannelError::from(answer)).expect("encode the read-channel document")
}

#[test]
fn every_class_the_channel_names_arrives_as_the_class_the_engine_gave() {
    let answers = one_answer_per_named_class();
    for answer in &answers {
        let in_process = format!("{answer:?}");
        let over_channel = format!("{:?}", over_the_channel(answer));
        assert_eq!(
            over_channel, in_process,
            "a caller reading over the channel must get the class and fields an in-process call gave",
        );
        assert_eq!(
            over_the_channel(answer).to_string(),
            answer.to_string(),
            "the words a caller reads must not change with the route",
        );
    }
}

/// The tags this build writes and the classes it names are the same set. A tag
/// declared with no class behind it, or a class that quietly falls through to
/// the prose fallback, are both caught here.
#[test]
fn the_named_classes_and_the_declared_tags_are_the_same_set() {
    let mut written: Vec<u16> = one_answer_per_named_class()
        .iter()
        .map(|answer| ReadChannelError::from(answer).tag())
        .collect();
    written.sort_unstable();
    written.dedup();

    let mut declared: Vec<u16> = ALL_TAGS
        .iter()
        .copied()
        .filter(|tag| *tag != TAG_UNKNOWN)
        .collect();
    declared.sort_unstable();

    assert_eq!(
        written, declared,
        "every declared tag has a class that writes it, and no class writes an undeclared tag",
    );
    assert!(
        ALL_TAGS.iter().all(|tag| body_grammar(*tag).is_some()),
        "every declared tag declares a body shape",
    );
    let mut unique = ALL_TAGS.to_vec();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), ALL_TAGS.len(), "no tag is used twice");
    assert!(
        ALL_TAGS.iter().all(|tag| *tag < 251),
        "a tag stays inside the one-byte integer form the checked-in bytes assume",
    );
}

/// Hand-derived bytes, one per body shape the grammar has: an empty body, one
/// word, two words, a small number, a number past the one-byte form, a mixed
/// body, a sequence of words, and the fallback.
#[test]
fn the_document_has_literal_bytes_for_each_body_shape() {
    assert_eq!(
        document_bytes(&Error::ReadCancelled),
        vec![TAG_READ_CANCELLED as u8],
        "a class with no fields is its tag and nothing else",
    );
    assert_eq!(
        document_bytes(&Error::TableNotFound("widgets".to_owned())),
        vec![
            TAG_TABLE_NOT_FOUND as u8,
            7,
            b'w',
            b'i',
            b'd',
            b'g',
            b'e',
            b't',
            b's'
        ],
    );
    assert_eq!(
        document_bytes(&Error::ColumnNotFound {
            table: "t".to_owned(),
            column: "c".to_owned(),
        }),
        vec![TAG_COLUMN_NOT_FOUND as u8, 1, b't', 1, b'c'],
    );
    assert_eq!(
        document_bytes(&Error::BfsDepthExceeded(7)),
        vec![TAG_BFS_DEPTH_EXCEEDED as u8, 7],
    );
    assert_eq!(
        document_bytes(&Error::BfsVisitedExceeded(4_096)),
        vec![TAG_BFS_VISITED_EXCEEDED as u8, 0xfb, 0x00, 0x10],
        "a number past the one-byte form takes bincode's two-byte marker",
    );
    assert_eq!(
        document_bytes(&Error::AclDenied {
            table: "t".to_owned(),
            row_id: RowId(9),
            principal: Principal::Human("h".to_owned()),
        }),
        vec![TAG_ACL_DENIED as u8, 1, b't', 9, 2, 1, b'h'],
        "the principal travels as an explicitly numbered kind and its identifier",
    );
    assert_eq!(
        document_bytes(&Error::ScopeLabelViolation {
            requested: ScopeLabel("a".to_owned()),
            allowed: BTreeSet::from([ScopeLabel("a".to_owned()), ScopeLabel("b".to_owned())]),
        }),
        vec![
            TAG_SCOPE_LABEL_VIOLATION as u8,
            1,
            b'a',
            2,
            1,
            b'a',
            1,
            b'b'
        ],
    );
    assert_eq!(
        document_bytes(&Error::MemoryBudgetExceeded {
            subsystem: "v".to_owned(),
            operation: "o".to_owned(),
            requested_bytes: 4_096,
            available_bytes: 128,
            budget_limit_bytes: 8_192,
            hint: "h".to_owned(),
        }),
        vec![
            TAG_MEMORY_BUDGET_EXCEEDED as u8,
            1,
            b'v',
            1,
            b'o',
            0xfb,
            0x00,
            0x10,
            128,
            0xfb,
            0x00,
            0x20,
            1,
            b'h',
        ],
    );
    assert_eq!(
        document_bytes(&Error::SyncError("boom".to_owned())),
        vec![
            TAG_UNKNOWN as u8,
            9,
            b'S',
            b'y',
            b'n',
            b'c',
            b'E',
            b'r',
            b'r',
            b'o',
            b'r',
            16,
            b's',
            b'y',
            b'n',
            b'c',
            b' ',
            b'e',
            b'r',
            b'r',
            b'o',
            b'r',
            b':',
            b' ',
            b'b',
            b'o',
            b'o',
            b'm',
        ],
        "an answer this channel has no tag for still carries its class name and its words",
    );
}

/// The bytes a reader parses are the tag written in the channel's source, not
/// where the class happens to sit in the engine's error enum. The two differ
/// for every class below, so a positional encoding could not produce them.
#[test]
fn the_document_bytes_are_the_written_tag_and_not_a_variant_position() {
    // Where these classes sit in `contextdb_core::Error`, counted from its
    // first variant. If the wire were positional these would be the leading
    // bytes; they are not.
    let positions = [
        (Error::ReadCancelled, 2_u8, TAG_READ_CANCELLED),
        (Error::TableNotFound("t".to_owned()), 4, TAG_TABLE_NOT_FOUND),
    ];
    for (answer, position, tag) in positions {
        let bytes = document_bytes(&answer);
        assert_eq!(bytes[0], tag as u8, "the leading byte is the written tag");
        assert_ne!(
            bytes[0], position,
            "the leading byte is not the class's position in the engine's enum",
        );
    }

    // What a positional wire costs, shown directly: inserting one variant
    // rewrites what every later variant encodes as. This is the hazard the
    // document removes, and the reason the engine's enum is no longer
    // serializable at all.
    #[derive(serde::Serialize)]
    #[allow(dead_code)]
    enum Taxonomy {
        _First,
        Second,
        _Third,
    }
    #[derive(serde::Serialize)]
    #[allow(dead_code)]
    enum TaxonomyAfterAnInsertion {
        _First,
        _Inserted,
        Second,
        _Third,
    }
    assert_eq!(
        encode_message(&Taxonomy::Second).expect("encode a positional taxonomy"),
        vec![1],
    );
    assert_eq!(
        encode_message(&TaxonomyAfterAnInsertion::Second)
            .expect("encode the same class after an insertion"),
        vec![2],
        "one inserted variant moves what an unrelated class encodes as",
    );
}

/// A body is admitted under the shape its tag declares. A tag with a body the
/// grammar does not describe, or a body that ends early, is refused rather
/// than half-read.
#[test]
fn a_document_is_refused_when_its_body_does_not_match_the_tag_it_names() {
    let intact = document_bytes(&Error::ColumnNotFound {
        table: "t".to_owned(),
        column: "c".to_owned(),
    });
    decode_message_exact::<ReadChannelError>(&intact).expect("the intact document decodes");

    let mut truncated = intact.clone();
    truncated.pop();
    assert!(
        decode_message_exact::<ReadChannelError>(&truncated).is_err(),
        "a body that ends part-way through is refused",
    );

    let mut trailing = intact.clone();
    trailing.push(0);
    assert!(
        decode_message_exact::<ReadChannelError>(&trailing).is_err(),
        "a body followed by bytes the grammar does not account for is refused",
    );

    let mut unnamed_tag = intact;
    unnamed_tag[0] = 250;
    assert!(
        decode_message_exact::<ReadChannelError>(&unnamed_tag).is_err(),
        "a tag this build does not write is refused, never guessed at",
    );
}

/// Every class walks the grammar its tag declares. This is what keeps the
/// allocation-free admission pass and the encoder from drifting apart: the
/// admission pass reads the same table the encoder's shape is written against.
#[test]
fn every_class_encodes_under_the_grammar_its_tag_declares() {
    for answer in one_answer_per_named_class() {
        let document = ReadChannelError::from(&answer);
        let tag = document.tag();
        let grammar = body_grammar(tag).expect("a written class declares its body shape");
        let bytes = document_bytes(&answer);
        assert_eq!(bytes[0], tag as u8);
        if grammar.is_empty() {
            assert_eq!(
                bytes.len(),
                1,
                "an empty body occupies no bytes: {answer:?}"
            );
        } else {
            assert!(
                bytes.len() > 1,
                "a class with a declared body writes one: {answer:?}",
            );
        }
        // Whatever the shape, the production decoder walks it and rebuilds the
        // same document -- the round trip through the real admission pass.
        assert_eq!(
            decode_message_exact::<ReadChannelError>(&bytes).expect("the document decodes"),
            document,
        );
        assert!(
            grammar.iter().all(|field| matches!(
                field,
                BodyField::Word
                    | BodyField::Number32
                    | BodyField::Number64
                    | BodyField::WordSequence
                    | BodyField::Failure
            )),
            "a declared field is one the admission pass knows how to walk",
        );
    }
}

/// A caller who declared a small ceiling is not handed an unbounded decode
/// because the thing being decoded happens to be an error.
#[test]
fn a_document_is_admitted_against_the_ceiling_the_caller_declared() {
    let long = Error::Other("w".repeat(4_096));
    let carried = LocalEngineFailure::from_error(&long);
    assert_eq!(
        carried.clone().into_error(ceiling()).to_string(),
        long.to_string(),
        "a roomy ceiling hands the answer back whole",
    );
    let cramped = carried.into_error(64);
    assert_ne!(
        cramped.to_string(),
        long.to_string(),
        "an answer past the declared ceiling is refused before it is held",
    );
    assert!(
        cramped.to_string().contains("readable shape"),
        "the refusal says the answer did not arrive, rather than inventing one: {cramped}",
    );
}

/// The classes a read meets are the same class and the same fields whichever
/// route served the read. The channel is the only thing that differs, so the
/// comparison is between the answer as the engine gave it and the answer after
/// it has been through the channel's document.
#[test]
fn representative_failures_carry_the_same_class_and_fields_across_routes() {
    let representative = [
        // A statement the reader got wrong.
        Error::ParseError("unexpected token at 3".to_owned()),
        Error::PlanError("no plan for this shape".to_owned()),
        Error::ColumnTypeMismatch {
            table: "widgets".to_owned(),
            column: "label".to_owned(),
            expected: "TEXT".to_owned(),
            actual: "TxId".to_owned(),
        },
        // A read that went past what it declared.
        Error::BfsVisitedExceeded(4_096),
        Error::MemoryBudgetExceeded {
            subsystem: "vector".to_owned(),
            operation: "candidate materialization".to_owned(),
            requested_bytes: 4_096,
            available_bytes: 128,
            budget_limit_bytes: 8_192,
            hint: "raise the read memory ceiling".to_owned(),
        },
        Error::ReadFailure(
            ReadFailure::new(
                ReadFailureKind::OwnerLimitExceeded,
                ReadFailureDetail::OwnerLimitExceeded(OwnerLimitExceededDetail {
                    limit: ReadFailureLimit::Memory,
                    value: 8_192,
                    required: None,
                    statement: None,
                }),
            )
            .expect("a canonical owner-limit refusal"),
        ),
        // A row the reader is not entitled to.
        Error::PrincipalRequired {
            table: "widgets".to_owned(),
        },
        Error::AclDenied {
            table: "widgets".to_owned(),
            row_id: RowId(9),
            principal: Principal::Agent("ingest".to_owned()),
        },
        Error::ContextScopeViolation {
            requested: ContextId(uuid::Uuid::from_u128(u128::MAX)),
            allowed: BTreeSet::from([ContextId(uuid::Uuid::from_u128(12))]),
        },
        Error::ScopeLabelViolation {
            requested: ScopeLabel("server".to_owned()),
            allowed: BTreeSet::from([ScopeLabel("edge".to_owned())]),
        },
        // A read that was stopped.
        Error::ReadCancelled,
        Error::OwnerReadDrainTimeout,
        // The store underneath, not the query.
        Error::StoreCorrupted {
            path: "/store/widgets.db".to_owned(),
            reason: "commit index missing".to_owned(),
        },
        Error::DatabaseLocked {
            holder_pid: 4_321,
            path: std::path::PathBuf::from("/store/widgets.db"),
        },
    ];
    for answer in representative {
        let over_channel = over_the_channel(&answer);
        assert_eq!(
            format!("{over_channel:?}"),
            format!("{answer:?}"),
            "the class and its fields must not depend on the route",
        );
        assert!(
            !matches!(over_channel, Error::Other(_)) || matches!(answer, Error::Other(_)),
            "a classified answer must never arrive as unclassified prose",
        );
    }
}

/// An answer this channel has no tag for is not lost and is not disguised: the
/// caller gets prose that names the class and repeats the engine's words. The
/// sentinel that used to hide here is a classified answer silently becoming
/// prose, so the classes above assert the opposite.
#[test]
fn an_unnamed_class_arrives_as_prose_that_names_it() {
    let unnamed = Error::SyncReplayOfAcceptedDelete {
        table: "widgets".to_owned(),
        key: vec![("id".to_owned(), Value::Int64(9))],
    };
    let words = unnamed.to_string();
    let over_channel = over_the_channel(&unnamed);
    let Error::Other(prose) = &over_channel else {
        panic!("an unnamed class arrives as prose, got {over_channel:?}");
    };
    assert!(
        prose.starts_with("SyncReplayOfAcceptedDelete: "),
        "the prose names the class: {prose}",
    );
    assert!(
        prose.ends_with(&words),
        "the prose repeats the words: {prose}"
    );
    assert_eq!(
        ReadChannelError::from(&unnamed).tag(),
        TAG_UNKNOWN,
        "an unnamed class travels under the one fallback tag",
    );
}
