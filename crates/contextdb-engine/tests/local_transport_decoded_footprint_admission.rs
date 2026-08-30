#![cfg(feature = "test-seams")]
//! Decoded-footprint admission on the local owner channel.
//!
//! The wire ceiling bounds how many bytes a peer may put on the channel. It
//! does not, on its own, bound how much live memory those bytes turn into once
//! they are decoded. These proofs craft payloads that sit inside every wire
//! ceiling the channel enforces and whose decoded form nevertheless exceeds the
//! read-contract memory ceiling, and require the channel to refuse them with a
//! typed transport error rather than allocate first and answer later.

use contextdb_core::Value;
use contextdb_core::read_contract::{CursorPage, MetadataPage, ReadLimits};
use contextdb_engine::local_transport::{
    CursorPageResponse, LocalRequest, LocalRequestEnvelope, LocalResponse, LocalTransportError,
    MAX_FRAME_BYTES, decode_message_exact, encode_message,
};
use std::collections::BTreeMap;
use std::mem::size_of;

/// Nine rows of the widest value list that still fits every wire ceiling.
const CRAFTED_ROWS: usize = 9;
/// One list entry per column keeps the crafted page arity-consistent, so it is
/// a page the channel is expected to carry rather than a malformed one.
const CRAFTED_COLUMNS: usize = 104_852;
/// Eight status items is the smallest count whose decoded maps clear the
/// memory ceiling while the whole payload stays inside the result ceiling.
const CRAFTED_STATUS_ITEMS: usize = 8;
/// The widest single status map that still fits the per-payload wire ceiling.
const CRAFTED_STATUS_ENTRIES: usize = 65_529;
/// Room for the fixed request preamble ahead of the parameter text.
const REQUEST_PREAMBLE_RESERVE: usize = 512;

/// Append one unsigned integer in the channel's length encoding.
fn push_unsigned(bytes: &mut Vec<u8>, value: u64) {
    if value <= 250 {
        bytes.push(u8::try_from(value).expect("a small length is one byte"));
    } else if let Ok(narrow) = u16::try_from(value) {
        bytes.push(251);
        bytes.extend_from_slice(&narrow.to_le_bytes());
    } else if let Ok(narrow) = u32::try_from(value) {
        bytes.push(252);
        bytes.extend_from_slice(&narrow.to_le_bytes());
    } else {
        bytes.push(253);
        bytes.extend_from_slice(&value.to_le_bytes());
    }
}

fn push_length(bytes: &mut Vec<u8>, value: usize) {
    push_unsigned(
        bytes,
        u64::try_from(value).expect("a length fits an unsigned word"),
    );
}

/// A cursor page of `rows` rows, each holding one null per column. An empty
/// column name and a null value are both a single zero byte on the wire.
fn crafted_cursor_page_bytes(rows: usize, columns: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    push_length(&mut bytes, columns);
    bytes.resize(bytes.len() + columns, 0);
    push_length(&mut bytes, rows);
    for _ in 0..rows {
        push_length(&mut bytes, columns);
        bytes.resize(bytes.len() + columns, 0);
    }
    bytes.push(0);
    bytes
}

/// Three printable bytes that are distinct for every index below 262 144, so
/// every crafted map entry survives insertion instead of collapsing onto a
/// key that is already present.
fn distinct_key(index: usize) -> [u8; 3] {
    const ALPHABET: &[u8; 64] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";
    [
        ALPHABET[index & 63],
        ALPHABET[(index >> 6) & 63],
        ALPHABET[(index >> 12) & 63],
    ]
}

/// An event-status metadata page of `items` items, each a map of `entries`
/// distinctly keyed null values.
fn crafted_metadata_page_bytes(items: usize, entries: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(1);
    push_length(&mut bytes, items);
    for _ in 0..items {
        bytes.push(1);
        push_length(&mut bytes, entries);
        for entry in 0..entries {
            let key = distinct_key(entry);
            push_length(&mut bytes, key.len());
            bytes.extend_from_slice(&key);
            bytes.push(0);
        }
    }
    bytes.push(0);
    bytes.push(0);
    bytes
}

/// A query request carrying one JSON-typed parameter whose text is supplied
/// verbatim, exactly as a peer would put it on the wire.
fn crafted_json_parameter_request_bytes(json_text: &[u8]) -> Vec<u8> {
    let limits = ReadLimits::default();
    let mut bytes = Vec::new();
    for field in [
        limits.result_rows,
        limits.result_bytes,
        limits.work,
        limits.active_ms,
        limits.memory,
        limits.cursor_page_rows,
        limits.cursor_page_bytes,
        limits.cursor_idle_ms,
        limits.cursor_lifetime_ms,
    ] {
        push_unsigned(&mut bytes, field);
    }
    bytes.push(0);
    push_length(&mut bytes, 0);
    push_length(&mut bytes, 1);
    push_length(&mut bytes, 1);
    bytes.push(b'p');
    bytes.push(7);
    push_length(&mut bytes, json_text.len());
    bytes.extend_from_slice(json_text);
    bytes
}

/// The text of a JSON array of `elements` zeroes.
fn json_zero_array_text(elements: usize) -> Vec<u8> {
    let mut text = Vec::with_capacity(elements * 2 + 1);
    text.push(b'[');
    for element in 0..elements {
        if element > 0 {
            text.push(b',');
        }
        text.push(b'0');
    }
    text.push(b']');
    text
}

fn refusal_is_typed(error: &LocalTransportError) -> bool {
    matches!(
        error,
        LocalTransportError::Payload(_)
            | LocalTransportError::Frame(_)
            | LocalTransportError::Refusal(_)
    )
}

#[test]
fn the_crafted_request_bytes_match_the_channel_encoder() {
    let text = json_zero_array_text(2);
    assert_eq!(text, b"[0,0]");
    let crafted = crafted_json_parameter_request_bytes(&text);

    let mut params = BTreeMap::new();
    params.insert(
        "p".to_owned(),
        Value::Json(serde_json::Value::Array(vec![
            serde_json::Value::from(0u8),
            serde_json::Value::from(0u8),
        ])),
    );
    let envelope = LocalRequestEnvelope {
        limits: ReadLimits::default(),
        request: LocalRequest::Query {
            statement: String::new(),
            params,
        },
    };
    let encoded = encode_message(&envelope).expect("the channel encodes a query request");

    assert_eq!(
        crafted, encoded,
        "the crafted request bytes must be byte-identical to what the channel encoder produces"
    );
}

#[test]
fn a_json_parameter_request_within_the_wire_ceiling_is_refused_before_its_tree_is_built() {
    let elements = (MAX_FRAME_BYTES - REQUEST_PREAMBLE_RESERVE) / 2;
    let text = json_zero_array_text(elements);
    let frame = crafted_json_parameter_request_bytes(&text);
    assert!(
        frame.len() <= MAX_FRAME_BYTES,
        "the crafted request must sit inside the wire ceiling to prove admission, observed {} bytes",
        frame.len()
    );

    let decoded_footprint = u64::try_from(elements * size_of::<serde_json::Value>())
        .expect("a decoded footprint fits an unsigned word");
    assert!(
        decoded_footprint > ReadLimits::SHIPPED_MEMORY,
        "the crafted parameter must decode past the memory ceiling to be worth refusing, \
         observed {decoded_footprint} bytes against {} bytes",
        ReadLimits::SHIPPED_MEMORY
    );

    let outcome = decode_message_exact::<LocalRequestEnvelope>(&frame);
    let error = match outcome {
        Ok(_) => panic!(
            "a {} byte request frame carrying a JSON parameter was admitted; decoding it builds \
             {elements} nodes and at least {decoded_footprint} bytes of live memory against the \
             {} byte memory ceiling, before any admission decision is taken",
            frame.len(),
            ReadLimits::SHIPPED_MEMORY
        ),
        Err(error) => error,
    };
    assert!(
        refusal_is_typed(&error),
        "an oversized JSON parameter must be refused with a typed transport error, observed {error:?}"
    );
}

#[test]
fn an_ordinary_json_parameter_request_is_still_admitted() {
    let text = json_zero_array_text(4);
    let frame = crafted_json_parameter_request_bytes(&text);
    let envelope = decode_message_exact::<LocalRequestEnvelope>(&frame)
        .expect("an ordinary JSON parameter stays admissible");
    let LocalRequest::Query { params, .. } = envelope.request else {
        panic!("the crafted request is a query");
    };
    assert_eq!(
        params.get("p"),
        Some(&Value::Json(serde_json::Value::Array(vec![
            serde_json::Value::from(0u8),
            serde_json::Value::from(0u8),
            serde_json::Value::from(0u8),
            serde_json::Value::from(0u8),
        ])))
    );
}

#[test]
fn a_cursor_page_whose_decoded_rows_exceed_the_memory_ceiling_is_refused() {
    let page_bytes = crafted_cursor_page_bytes(CRAFTED_ROWS, CRAFTED_COLUMNS);
    let page_length = u64::try_from(page_bytes.len()).expect("a page length fits an unsigned word");
    assert!(
        page_length <= ReadLimits::SHIPPED_CURSOR_PAGE_BYTES,
        "the crafted page must sit inside the cursor-page ceiling to prove admission, \
         observed {page_length} bytes against {} bytes",
        ReadLimits::SHIPPED_CURSOR_PAGE_BYTES
    );

    let decoded_footprint = u64::try_from(CRAFTED_ROWS * CRAFTED_COLUMNS * size_of::<Value>())
        .expect("a decoded footprint fits an unsigned word");
    assert!(
        decoded_footprint > ReadLimits::SHIPPED_MEMORY,
        "the crafted page must decode past the memory ceiling to be worth refusing, \
         observed {decoded_footprint} bytes against {} bytes",
        ReadLimits::SHIPPED_MEMORY
    );

    let outcome = decode_message_exact::<CursorPage>(&page_bytes);
    let error = match outcome {
        Ok(_) => panic!(
            "a {page_length} byte cursor page was admitted; it decodes into at least \
             {decoded_footprint} bytes of live values against the {} byte memory ceiling",
            ReadLimits::SHIPPED_MEMORY
        ),
        Err(error) => error,
    };
    assert!(
        refusal_is_typed(&error),
        "an oversized cursor page must be refused with a typed transport error, observed {error:?}"
    );
}

#[test]
fn a_response_frame_carrying_that_cursor_page_is_refused_before_the_page_is_decoded() {
    let page_bytes = crafted_cursor_page_bytes(CRAFTED_ROWS, CRAFTED_COLUMNS);
    let payload_length = page_bytes.len();
    let frame = encode_message(&LocalResponse::CursorPage {
        page: CursorPageResponse {
            payload: page_bytes,
        },
    })
    .expect("the channel encodes a cursor-page response");
    assert!(
        frame.len() <= MAX_FRAME_BYTES,
        "the crafted response must sit inside the wire ceiling to prove admission, observed {} bytes",
        frame.len()
    );

    let decoded_footprint = u64::try_from(CRAFTED_ROWS * CRAFTED_COLUMNS * size_of::<Value>())
        .expect("a decoded footprint fits an unsigned word");
    let outcome = decode_message_exact::<LocalResponse>(&frame);
    let error = match outcome {
        Ok(_) => panic!(
            "a {} byte response frame carrying a {payload_length} byte cursor page was admitted; \
             the page decodes into at least {decoded_footprint} bytes of live values, past the \
             {} byte memory ceiling",
            frame.len(),
            ReadLimits::SHIPPED_MEMORY
        ),
        Err(error) => error,
    };
    assert!(
        refusal_is_typed(&error),
        "a response frame carrying an oversized cursor page must be refused with a typed \
         transport error, observed {error:?}"
    );
}

#[test]
fn an_ordinary_cursor_page_response_is_still_admitted() {
    let page_bytes = crafted_cursor_page_bytes(100, 8);
    let page = decode_message_exact::<CursorPage>(&page_bytes)
        .expect("an ordinary cursor page stays admissible");
    assert_eq!(page.columns.len(), 8);
    assert_eq!(page.rows.len(), 100);

    let frame = encode_message(&LocalResponse::CursorPage {
        page: CursorPageResponse {
            payload: page_bytes,
        },
    })
    .expect("the channel encodes a cursor-page response");
    decode_message_exact::<LocalResponse>(&frame)
        .expect("an ordinary cursor-page response stays admissible");
}

#[test]
fn a_metadata_page_whose_decoded_maps_exceed_the_memory_ceiling_is_refused() {
    let page_bytes = crafted_metadata_page_bytes(CRAFTED_STATUS_ITEMS, CRAFTED_STATUS_ENTRIES);
    let page_length = u64::try_from(page_bytes.len()).expect("a page length fits an unsigned word");
    assert!(
        page_length <= ReadLimits::SHIPPED_RESULT_BYTES,
        "the crafted page must sit inside the result ceiling to prove admission, \
         observed {page_length} bytes against {} bytes",
        ReadLimits::SHIPPED_RESULT_BYTES
    );

    let decoded_footprint =
        u64::try_from(CRAFTED_STATUS_ITEMS * CRAFTED_STATUS_ENTRIES * size_of::<(String, Value)>())
            .expect("a decoded footprint fits an unsigned word");
    assert!(
        decoded_footprint > ReadLimits::SHIPPED_MEMORY,
        "the crafted page must decode past the memory ceiling to be worth refusing, \
         observed {decoded_footprint} bytes against {} bytes",
        ReadLimits::SHIPPED_MEMORY
    );

    let outcome = decode_message_exact::<MetadataPage>(&page_bytes);
    let error = match outcome {
        Ok(_) => panic!(
            "a {page_length} byte metadata page was admitted; it decodes into at least \
             {decoded_footprint} bytes of live map entries against the {} byte memory ceiling",
            ReadLimits::SHIPPED_MEMORY
        ),
        Err(error) => error,
    };
    assert!(
        refusal_is_typed(&error),
        "an oversized metadata page must be refused with a typed transport error, observed {error:?}"
    );
}

#[test]
fn an_ordinary_metadata_page_is_still_admitted() {
    let page_bytes = crafted_metadata_page_bytes(4, 16);
    let page = decode_message_exact::<MetadataPage>(&page_bytes)
        .expect("an ordinary metadata page stays admissible");
    assert_eq!(page.items.len(), 4);
}
