//! Typed internal owner-page fixtures. Public CursorPage encoding stays unchanged.
use contextdb_core::read_contract::CursorPage;
use contextdb_engine::read_contract::{CanonicalQueryTrace, ReadEncodingError};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
struct ObservedPage {
    page: CursorPage,
    trace: CanonicalQueryTrace,
}

fn decode(bytes: &[u8]) -> Result<ObservedPage, ReadEncodingError> {
    let (observed, consumed): (ObservedPage, usize) =
        bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|_| ReadEncodingError::InvalidPayload)?;
    if consumed != bytes.len() {
        return Err(ReadEncodingError::InvalidPayload);
    }
    observed
        .page
        .validate()
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    Ok(observed)
}

pub fn decode_owner_cursor_page(bytes: &[u8]) -> Result<CursorPage, ReadEncodingError> {
    Ok(decode(bytes)?.page)
}

pub fn replace_owner_cursor_page(
    bytes: &[u8],
    page: &CursorPage,
) -> Result<Vec<u8>, ReadEncodingError> {
    let observed = decode(bytes)?;
    page.validate()
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    #[derive(Serialize)]
    struct BorrowedPage<'a> {
        page: &'a CursorPage,
        trace: &'a CanonicalQueryTrace,
    }
    bincode::serde::encode_to_vec(
        BorrowedPage {
            page,
            trace: &observed.trace,
        },
        bincode::config::standard(),
    )
    .map_err(|_| ReadEncodingError::InvalidPayload)
}

pub fn with_empty_observation(page_bytes: Vec<u8>) -> Vec<u8> {
    // Decode the typed page without admission/validation: callers deliberately
    // construct oversized pages to test the receiver's admission boundary.
    let (page, consumed): (CursorPage, usize) =
        bincode::serde::decode_from_slice(&page_bytes, bincode::config::standard())
            .expect("fixture contains a complete typed page");
    assert_eq!(consumed, page_bytes.len());
    let trace = CanonicalQueryTrace::from(&contextdb_engine::QueryTrace::scan());
    bincode::serde::encode_to_vec(ObservedPage { page, trace }, bincode::config::standard())
        .expect("encode the typed owner page and observation")
}

pub fn assert_empty_observation(bytes: &[u8], page: &CursorPage) {
    let observed = decode(bytes).expect("decode the complete owner observation");
    assert_eq!(&observed.page, page);
    assert_eq!(observed.trace.physical_plan, "Scan");
    assert_eq!(observed.trace.index_used, None);
    assert_eq!(observed.trace.rows_examined, 0);
    assert!(!observed.trace.sort_elided);
    assert_eq!(
        observed.trace,
        CanonicalQueryTrace::from(&contextdb_engine::QueryTrace::scan()),
        "all decoded trace fields match the fixture's scan observation"
    );
}

pub fn owner_response_fixture(prefix: &[u8], page_bytes: &[u8]) -> Vec<u8> {
    let payload = with_empty_observation(page_bytes.to_vec());
    let mut bytes = prefix.to_vec();
    bytes.extend(
        bincode::serde::encode_to_vec(payload.len() as u64, bincode::config::standard())
            .expect("encode the outer payload length"),
    );
    bytes.extend(payload);
    bytes
}
