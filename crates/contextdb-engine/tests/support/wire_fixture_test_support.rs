//! App-owned wire inputs are mutable only before real immutable publication.
//! These helpers preserve the old app-owned byte fixtures. Engine-created
//! carriers and encodings use one finite counted input owner per fixture;
//! codec output limits and receiver owners remain at their original call sites.
use contextdb_core::Value;
use contextdb_core::read_memory::{ReadCredit, ReadMemoryOwner, ReadShared};
use contextdb_engine::memory_accounting::MemoryAccountant;
use contextdb_engine::read_contract::{
    CanonicalCascadeReport, CanonicalQueryResult, CanonicalQueryTrace, ReadEncodingError,
};
use contextdb_engine::{CascadeReport, QueryTrace};
use serde::Serialize;
use std::sync::Arc;

// EncodedReadBytes stores ReadShared<Funded<Vec<u8>>>. Funded is exactly
// (Vec<u8>, Option<ReadCredit>); account for its native publication cell too.
fn encoded_cell_bytes() -> usize {
    ReadShared::<(Vec<u8>, Option<ReadCredit>)>::layout().size()
}

pub fn owner_for_bytes(bytes: usize) -> ReadMemoryOwner {
    Arc::new(MemoryAccountant::with_budget(bytes)).into()
}

// The original DTO is caller-owned setup. Its independent serializer measures
// the exact output allocation, before invoking the production encoder under test.
pub fn owner_for_serialized<T: Serialize>(value: &T, framing_bytes: usize) -> ReadMemoryOwner {
    let bytes = bincode::serde::encode_to_vec(value, bincode::config::standard())
        .expect("measure the app-owned wire input")
        .len();
    let simultaneous = if framing_bytes == 0 {
        bytes
    } else {
        bytes.checked_mul(2).expect("payload and frame fit")
    };
    owner_for_bytes(
        simultaneous
            .checked_add(framing_bytes)
            .and_then(|n| n.checked_add(encoded_cell_bytes()))
            .expect("fixture size fits"),
    )
}

pub fn encode_message<T: Serialize>(
    value: &T,
) -> Result<Vec<u8>, contextdb_engine::local_transport::LocalTransportError> {
    contextdb_engine::local_transport::encode_message(value, owner_for_serialized(value, 0))
        .map(|bytes| bytes.to_vec())
}

pub fn encode_payload_frame(
    payload: &[u8],
) -> Result<Vec<u8>, contextdb_engine::local_transport::LocalTransportError> {
    contextdb_engine::local_transport::encode_payload_frame(
        payload,
        owner_for_bytes(
            payload
                .len()
                .checked_add(4 + encoded_cell_bytes())
                .expect("fixture frame fits"),
        ),
    )
    .map(|bytes| bytes.to_vec())
}

#[derive(Clone)]
pub struct QueryFixture {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub rows_affected: u64,
    pub trace: QueryTrace,
    pub cascade: Option<CascadeReport>,
}
impl QueryFixture {
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: 0,
            trace: QueryTrace::scan(),
            cascade: None,
        }
    }
}

pub fn encode_query_result(input: &QueryFixture) -> Result<Vec<u8>, ReadEncodingError> {
    let input = input.clone(); // A copy of caller-created fixture data, before publication.
    let wire_size = bincode::serde::encode_to_vec(
        CanonicalQueryResult {
            columns: input.columns.clone(),
            rows: input.rows.clone(),
            rows_affected: input.rows_affected,
            trace: CanonicalQueryTrace::from(&input.trace),
            cascade: input.cascade.as_ref().map(|c| CanonicalCascadeReport {
                dropped_indexes: c.dropped_indexes.clone(),
            }),
        },
        bincode::config::standard(),
    )
    .expect("measure app-owned query fixture")
    .len();
    // QueryResultData has exactly the fixture's fields. Each of the two private
    // charge enums fits within (ReadCredit, ReadMemoryOwner); the retained owner
    // is optional. ReadShared adds the actual reference count and invoice layout.
    // This is a type-derived upper bound on the input carrier, plus exact output
    // bytes, not a replacement for any receiver/codec ceiling under test.
    let control = ReadShared::<(
        QueryFixture,
        [(ReadCredit, ReadMemoryOwner); 2],
        Option<ReadMemoryOwner>,
    )>::layout()
    .size();
    let owner = owner_for_bytes(
        control
            .checked_add(wire_size)
            .and_then(|n| n.checked_add(encoded_cell_bytes()))
            .expect("fixture query fits"),
    );
    let published = contextdb_engine::QueryResult::from_owned_parts_for_test(
        owner.clone(),
        input.columns,
        input.rows,
        input.rows_affected,
        input.trace,
        input.cascade,
    )
    .expect("prepay the immutable input carrier before encoding");
    contextdb_engine::read_contract::encode_query_result(&published, owner)
        .map(|bytes| bytes.to_vec())
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by local_transport"
)]
pub fn cursor_page(
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    has_more: bool,
) -> contextdb_engine::CursorPage {
    let wire_size =
        bincode::serde::encode_to_vec((&columns, &rows, has_more), bincode::config::standard())
            .expect("measure app-owned page input")
            .len();
    let control = ReadShared::<(Vec<String>, Vec<Vec<Value>>, bool)>::layout().size();
    contextdb_engine::CursorPage::from_owned_parts_for_test(
        owner_for_bytes(control + wire_size + encoded_cell_bytes()),
        columns,
        rows,
        has_more,
    )
    .expect("prepay the immutable page input carrier")
}

pub fn encode_cursor_page(
    page: &contextdb_engine::CursorPage,
) -> Result<Vec<u8>, ReadEncodingError> {
    let owner = page
        .encoding_owner()
        .unwrap_or_else(|| owner_for_serialized(page, 0));
    contextdb_engine::read_contract::encode_cursor_page(page, owner).map(|bytes| bytes.to_vec())
}

// Outbound framing retains payload and frame simultaneously. The finite setup
// owner covers exactly both native buffers; the boundary's limits still apply.
#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by local_transport"
)]
pub trait FixtureFrame {
    fn encode_fixture_frame(
        &self,
        message: contextdb_engine::local_transport::LocalOutboundMessage<'_>,
    ) -> Result<Vec<u8>, contextdb_engine::local_transport::LocalTransportError>;
}
impl FixtureFrame for contextdb_engine::local_transport::LocalProtocolBoundary {
    fn encode_fixture_frame(
        &self,
        message: contextdb_engine::local_transport::LocalOutboundMessage<'_>,
    ) -> Result<Vec<u8>, contextdb_engine::local_transport::LocalTransportError> {
        use contextdb_engine::local_transport::LocalOutboundMessage;
        fn size<T: Serialize>(value: &T) -> usize {
            bincode::serde::encode_to_vec(value, bincode::config::standard())
                .expect("measure caller-owned frame input")
                .len()
        }
        let length = match &message {
            LocalOutboundMessage::Handshake(value) => size(value),
            LocalOutboundMessage::Request(value) => size(value),
            LocalOutboundMessage::Response { response, .. } => size(response),
        };
        self.encode_frame(
            message,
            owner_for_bytes(
                length
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(4 + encoded_cell_bytes()))
                    .expect("frame input fits"),
            ),
        )
        .map(|bytes| bytes.to_vec())
    }
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by local_transport"
)]
pub fn encode_metadata_page(
    page: &contextdb_core::read_contract::MetadataPage,
) -> Result<Vec<u8>, ReadEncodingError> {
    contextdb_engine::read_contract::encode_metadata_page(page, owner_for_serialized(page, 0))
        .map(|bytes| bytes.to_vec())
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by local_transport"
)]
pub fn engine_failure(
    error: &contextdb_core::Error,
) -> contextdb_engine::local_transport::LocalEngineFailure {
    let input = contextdb_engine::local_transport::ReadChannelError::from(error);
    contextdb_engine::local_transport::LocalEngineFailure::from_error(
        error,
        owner_for_serialized(&input, 0),
    )
    .expect("prepay the exact error document input")
}
