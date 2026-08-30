//! What a refused read reads like when a person sees it.
//!
//! A refusal carries typed fields so a script can branch on class, kind, and
//! detail.  The rendered form is the other half of that promise: it is what an
//! embedder prints, what lands in a log line, and what an error chain shows a
//! human.  It has to be prose that names what happened and, where a ceiling was
//! crossed, which ceiling and at what value.  A structure dump is not prose:
//! braces, field names, and Rust type names are the shape of the internal
//! representation, not an explanation.

use contextdb_core::Error;
use contextdb_core::read_contract::{
    CursorExpiryKind, HeldByReadersDetail, OwnerLimitExceededDetail, OwnerRouteUnsupportedDetail,
    ProcessStartIdentity, ReadFailure, ReadFailureDetail, ReadFailureKind, ReadFailureLimit,
    ReaderBreadcrumb,
};

/// Debug-dump artifacts.  Any of these in the rendered text means the reader is
/// looking at the internal representation instead of a sentence.
const STRUCTURE_ARTIFACTS: &[&str] = &["{", "}", "Detail", "ReadFailure", "kind:", "detail:"];

/// The human word each ceiling must be named by.
fn ceiling_wording(limit: ReadFailureLimit) -> &'static str {
    match limit {
        ReadFailureLimit::ResultRows => "row",
        ReadFailureLimit::ResultBytes => "byte",
        ReadFailureLimit::Work => "work",
        ReadFailureLimit::ActiveMs => "time",
        ReadFailureLimit::Memory => "memory",
        ReadFailureLimit::CursorPageBytes => "page",
    }
}

/// Every kind's Rust identifier, which must never reach a reader.
fn kind_identifier(kind: ReadFailureKind) -> String {
    format!("{kind:?}")
}

fn refusal(kind: ReadFailureKind, detail: ReadFailureDetail) -> ReadFailure {
    ReadFailure::new(kind, detail).expect("kind and detail pair is valid")
}

/// One refusal per kind, plus the specialized details a kind can carry, so
/// nothing is proven by a single lucky variant.
fn every_refusal() -> Vec<ReadFailure> {
    let mut refusals = Vec::new();
    for kind in ReadFailureKind::ALL {
        match kind {
            ReadFailureKind::OwnerLimitExceeded => {
                for limit in [
                    ReadFailureLimit::ResultRows,
                    ReadFailureLimit::ResultBytes,
                    ReadFailureLimit::Work,
                    ReadFailureLimit::ActiveMs,
                    ReadFailureLimit::Memory,
                    ReadFailureLimit::CursorPageBytes,
                ] {
                    refusals.push(ReadFailure::owner_limit_exceeded(
                        OwnerLimitExceededDetail {
                            limit,
                            value: 4_096,
                            required: None,
                            statement: None,
                        },
                    ));
                }
            }
            ReadFailureKind::CursorExpired => {
                for expiry in [CursorExpiryKind::Idle, CursorExpiryKind::Lifetime] {
                    refusals.push(ReadFailure::cursor_expired(expiry));
                }
            }
            ReadFailureKind::HeldByReaders => {
                refusals.push(
                    ReadFailure::held_by_readers(HeldByReadersDetail {
                        observed_direct_readers: 2,
                        verified_readers: vec![ReaderBreadcrumb {
                            process_id: 4_242,
                            process_name: "contextdb-cli".to_owned(),
                            process_start: ProcessStartIdentity(9_001),
                        }],
                    })
                    .expect("held-by-readers detail is valid"),
                );
                refusals.push(
                    ReadFailure::held_by_readers(HeldByReadersDetail {
                        observed_direct_readers: 3,
                        verified_readers: Vec::new(),
                    })
                    .expect("held-by-readers detail without breadcrumbs is valid"),
                );
            }
            // The kind that exists to say what was asked for cannot be built
            // without it, so the ordinary vocabulary is not a shape it has.
            ReadFailureKind::OwnerRouteUnsupported => {
                refusals.push(refusal(
                    kind,
                    ReadFailureDetail::OwnerRouteUnsupported(OwnerRouteUnsupportedDetail {
                        inspection: "image_state".to_owned(),
                    }),
                ));
            }
            other => {
                refusals.push(refusal(other, ReadFailureDetail::None));
                refusals.push(refusal(
                    other,
                    ReadFailureDetail::Reason {
                        reason: "the owner closed the store".to_owned(),
                    },
                ));
            }
        }
    }
    refusals
}

/// Every kind renders as a sentence a person can read, never as a dump of the
/// refusal's internal structure.
#[test]
fn every_refusal_kind_renders_as_human_prose() {
    let mut unreadable = Vec::new();
    for failure in every_refusal() {
        let rendered = failure.to_string();
        let kind = failure.kind();
        for artifact in STRUCTURE_ARTIFACTS {
            if rendered.contains(artifact) {
                unreadable.push(format!(
                    "{kind:?}: rendered text carries the structure artifact {artifact:?}: \
                     {rendered}"
                ));
            }
        }
        let identifier = kind_identifier(kind);
        if rendered.contains(&identifier) {
            unreadable.push(format!(
                "{kind:?}: rendered text shows the internal name {identifier}: {rendered}"
            ));
        }
        if !rendered.contains(' ') {
            unreadable.push(format!(
                "{kind:?}: rendered text is a single token rather than a sentence: {rendered}"
            ));
        }
    }
    assert!(
        unreadable.is_empty(),
        "a refused read must explain itself in prose; these did not:\n{}",
        unreadable.join("\n")
    );
}

/// A crossed ceiling is named, with the value that was crossed, so the reader
/// knows which setting to change.
#[test]
fn a_crossed_ceiling_is_named_in_the_rendered_refusal() {
    let mut silent = Vec::new();
    for limit in [
        ReadFailureLimit::ResultRows,
        ReadFailureLimit::ResultBytes,
        ReadFailureLimit::Work,
        ReadFailureLimit::ActiveMs,
        ReadFailureLimit::Memory,
        ReadFailureLimit::CursorPageBytes,
    ] {
        let failure = ReadFailure::owner_limit_exceeded(OwnerLimitExceededDetail {
            limit,
            value: 4_096,
            required: None,
            statement: None,
        });
        let rendered = failure.to_string().to_lowercase();
        let wording = ceiling_wording(limit);
        if !rendered.contains(wording) {
            silent.push(format!(
                "{limit:?}: rendered text never says {wording:?}: {rendered}"
            ));
        }
        // Only the run-together spellings are checkable: a ceiling whose
        // internal name is already the word for it ("work", "memory") cannot
        // be told apart from prose that names it.
        let run_together = format!("{limit:?}").to_lowercase();
        if run_together != wording && rendered.contains(&run_together) {
            silent.push(format!(
                "{limit:?}: rendered text shows the ceiling's internal name rather than \
                 naming it in words: {rendered}"
            ));
        }
        for artifact in STRUCTURE_ARTIFACTS {
            if rendered.contains(&artifact.to_lowercase()) {
                silent.push(format!(
                    "{limit:?}: rendered text carries the structure artifact {artifact:?}: \
                     {rendered}"
                ));
            }
        }
        if !rendered.contains("4096") {
            silent.push(format!(
                "{limit:?}: rendered text never states the crossed value 4096: {rendered}"
            ));
        }
    }
    assert!(
        silent.is_empty(),
        "a ceiling refusal must name the ceiling and its value; these did not:\n{}",
        silent.join("\n")
    );
}

/// The refusal reads the same through the error type an embedder catches.
#[test]
fn a_refusal_reads_the_same_through_the_error_it_is_carried_in() {
    for failure in every_refusal() {
        let direct = failure.to_string();
        let carried = Error::ReadFailure(failure.clone()).to_string();
        assert_eq!(
            carried,
            direct,
            "the error an embedder catches must read exactly as the refusal it carries \
             ({:?})",
            failure.kind()
        );
        for artifact in STRUCTURE_ARTIFACTS {
            assert!(
                !carried.contains(artifact),
                "the error an embedder catches must not carry the structure artifact \
                 {artifact:?} ({:?}): {carried}",
                failure.kind()
            );
        }
    }
}
