//! Typed internal owner-page fixtures. Public CursorPage encoding stays unchanged.
use contextdb_core::read_contract::CursorPage;
use contextdb_engine::read_contract::{CanonicalQueryTrace, ReadEncodingError};
use serde::{Deserialize, Serialize};

// The observing application owns its decoded fixture and may mutate it before
// injecting replacement wire bytes. It is never an engine-owned answer handle.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename = "CursorPage")]
pub struct OwnerPageFixture {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<contextdb_core::Value>>,
    pub has_more: bool,
}
impl OwnerPageFixture {
    #[allow(
        dead_code,
        reason = "KEEP-SHARED: shared test helper used by owner_read, read"
    )]
    pub fn rows(&self) -> &[Vec<contextdb_core::Value>] {
        &self.rows
    }
    #[allow(
        dead_code,
        reason = "KEEP-SHARED: shared test helper; other test targets or modules use it"
    )]
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
    #[allow(
        dead_code,
        reason = "KEEP-SHARED: shared test helper used by owner_read"
    )]
    pub fn has_more(&self) -> bool {
        self.has_more
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn validate(&self) -> Result<(), ReadEncodingError> {
        if self.rows.iter().any(|row| row.len() != self.columns.len()) {
            return Err(ReadEncodingError::InvalidPayload);
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
struct ObservedPage {
    page: OwnerPageFixture,
    trace: CanonicalQueryTrace,
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
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

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub fn decode_owner_cursor_page(bytes: &[u8]) -> Result<OwnerPageFixture, ReadEncodingError> {
    Ok(decode(bytes)?.page)
}

#[allow(dead_code, reason = "KEEP-SHARED: shared test helper used by read")]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
#[cfg(target_os = "linux")]
pub fn replace_owner_cursor_page(
    bytes: &[u8],
    page: &OwnerPageFixture,
) -> Result<Vec<u8>, ReadEncodingError> {
    let observed = decode(bytes)?;
    page.validate()
        .map_err(|_| ReadEncodingError::InvalidPayload)?;
    #[derive(Serialize)]
    struct BorrowedPage<'a> {
        page: &'a OwnerPageFixture,
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

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by local_transport"
)]
pub fn with_empty_observation(page_bytes: Vec<u8>) -> Vec<u8> {
    // Decode the typed page without admission/validation: callers deliberately
    // construct oversized pages to test the receiver's admission boundary.
    let (page, consumed): (OwnerPageFixture, usize) =
        bincode::serde::decode_from_slice(&page_bytes, bincode::config::standard())
            .expect("fixture contains a complete typed page");
    assert_eq!(consumed, page_bytes.len());
    let trace = CanonicalQueryTrace::from(&contextdb_engine::QueryTrace::scan());
    bincode::serde::encode_to_vec(ObservedPage { page, trace }, bincode::config::standard())
        .expect("encode the typed owner page and observation")
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by local_transport"
)]
pub fn assert_empty_observation(bytes: &[u8], page: &CursorPage) {
    let observed = decode(bytes).expect("decode the complete owner observation");
    assert_eq!(
        (
            &observed.page.columns[..],
            &observed.page.rows[..],
            observed.page.has_more
        ),
        (page.columns(), page.rows(), page.has_more())
    );
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

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by local_transport"
)]
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
