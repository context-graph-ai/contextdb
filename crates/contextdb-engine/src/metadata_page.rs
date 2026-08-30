//! Cutting one inventory into pages, and putting the pieces back together.
//!
//! What tables a store has, and what it does on its own, are lists that can be
//! any length, so they travel a page at a time under whatever byte ceiling the
//! caller set. Every route answers those questions, which means every route
//! has to cut the same list at the same places -- otherwise "resume after this
//! item" means something different depending on who answered, and a caller
//! that changed routes would silently skip or repeat entries.
//!
//! So the cutting happens once, here, and both the owner service and the
//! committed-file projection call it. The same goes for the flattening: an
//! event inventory is one ordered list of kind-tagged items precisely so a
//! page boundary can fall anywhere in it, and it is put back together into a
//! typed status by the inverse function sitting beside the one that flattened
//! it.

use crate::direct_file_reader::{
    DirectEventTypeStatus, DirectEventsStatus, DirectRouteStatus, DirectScheduleStatus,
    DirectSinkStatus,
};
use crate::read_contract::ReadEncodingError;
use contextdb_core::Value;
use contextdb_core::read_contract::{
    MetadataItem, MetadataPage, MetadataPageVocabulary, OwnerLimitExceededDetail, ReadFailure,
    ReadFailureLimit, RequiredBytesSetting,
};
use std::collections::BTreeMap;

/// Why an inventory could not be cut into the page that was asked for.
pub(crate) enum MetadataPagingError {
    /// The continuation offered was not issued by this question.
    Continuation(ReadFailure),
    /// A single item does not fit the byte ceiling in force, so there is no
    /// page to publish.
    Oversized(ReadFailure),
    Encoding(ReadEncodingError),
}

/// The question a continuation belongs to, written into the token itself.
///
/// A continuation says "resume after this item", and which items exist
/// depends entirely on WHICH inventory was being read. A token from the table
/// inventory handed to the event inventory is not a smaller answer, it is a
/// different question resumed at a position that means nothing there -- and
/// what came back was an empty page, indistinguishable from "there is nothing
/// left". So the token carries the name of the question that issued it and is
/// refused by any other.
const fn issuing_question(vocabulary: MetadataPageVocabulary) -> &'static str {
    match vocabulary {
        MetadataPageVocabulary::Tables => "tables",
        MetadataPageVocabulary::EventsStatus => "events_status",
    }
}

fn issued_continuation(vocabulary: MetadataPageVocabulary, key: &str) -> String {
    format!("{}:{key}", issuing_question(vocabulary))
}

/// Where a resumed page starts, or the refusal for a token this question did
/// not issue.
fn resume_after(
    vocabulary: MetadataPageVocabulary,
    token: &str,
) -> std::result::Result<String, MetadataPagingError> {
    let question = issuing_question(vocabulary);
    token
        .strip_prefix(question)
        .and_then(|rest| rest.strip_prefix(':'))
        .map(str::to_owned)
        .ok_or_else(|| {
            MetadataPagingError::Continuation(ReadFailure::invalid_continuation(format!(
                "this continuation was not issued by {question}"
            )))
        })
}

/// Refuse a complete metadata answer that does not fit the byte ceiling in
/// force, naming the setting that would carry it.
///
/// Both routes measure the same encoded bytes through this one function, so an
/// answer that is refused over a channel is refused when the same store is
/// read as a file, with the same number in the same words.
pub(crate) fn admit_complete_metadata(
    payload_len: usize,
    result_bytes: u64,
) -> Option<ReadFailure> {
    let required_bytes = u64::try_from(payload_len).unwrap_or(u64::MAX);
    if required_bytes <= result_bytes {
        return None;
    }
    Some(oversized_refusal(result_bytes, required_bytes))
}

fn oversized_refusal(ceiling: u64, required_bytes: u64) -> ReadFailure {
    ReadFailure::owner_limit_exceeded(OwnerLimitExceededDetail {
        limit: ReadFailureLimit::ResultBytes,
        value: ceiling,
        required: Some(RequiredBytesSetting {
            required_bytes,
            required_setting: format!("effective result_bytes >= {required_bytes}"),
        }),
        statement: None,
    })
}

pub(crate) fn metadata_table_key(item: &MetadataItem) -> String {
    match item {
        MetadataItem::Table(table) => table.clone(),
        MetadataItem::EventStatus(_) => String::new(),
    }
}

pub(crate) fn metadata_event_key(item: &MetadataItem) -> String {
    match item {
        MetadataItem::EventStatus(status) => status
            .get("name")
            .map(|name| format!("{name:?}"))
            .unwrap_or_default(),
        MetadataItem::Table(table) => table.clone(),
    }
}

/// Cut one page out of a complete inventory. The continuation is the key of
/// the last item published, so a resumed page starts strictly after it.
pub(crate) fn continuation_page(
    vocabulary: MetadataPageVocabulary,
    items: Vec<MetadataItem>,
    continuation: Option<&str>,
    page_bytes: u64,
    key: fn(&MetadataItem) -> String,
) -> std::result::Result<MetadataPage, MetadataPagingError> {
    let remaining: Vec<_> = match continuation {
        Some(token) => {
            let resume = resume_after(vocabulary, token)?;
            items
                .into_iter()
                .filter(|item| key(item) > resume)
                .collect()
        }
        None => items,
    };
    let mut page = MetadataPage {
        vocabulary,
        items: Vec::new(),
        has_more: false,
        continuation: None,
    };
    // Each candidate is measured in the shape it would actually be PUBLISHED
    // in, which is why the pager has to know whether another item is waiting
    // before it measures: a page that ends the inventory carries no
    // `has_more` and no continuation, and measuring the last item as though
    // it did charged the caller for bytes that were never going to be sent --
    // enough to refuse an answer that fits, and to name a size larger than
    // the one it would have published.
    let mut remaining = remaining.into_iter().peekable();
    while let Some(item) = remaining.next() {
        let key = key(&item);
        let more_after_this = remaining.peek().is_some();
        page.items.push(item);
        page.has_more = more_after_this;
        page.continuation = more_after_this.then(|| issued_continuation(vocabulary, &key));
        let encoded = crate::read_contract::metadata_page_encoded_size(&page)
            .map_err(|_| MetadataPagingError::Encoding(ReadEncodingError::InvalidPayload))?;
        let encoded = u64::try_from(encoded).unwrap_or(u64::MAX);
        if encoded > page_bytes {
            if page.items.len() > 1 {
                // The page without this item is a page that stops early, so
                // it says so and names where to resume.
                let _overflowed = page.items.pop();
                page.has_more = true;
                page.continuation = page
                    .items
                    .last()
                    .map(|item| issued_continuation(vocabulary, &key_of(item)));
                return finish_metadata_page(page);
            }
            // One item, and the shape it would be published in does not fit.
            // Publishing it anyway would hand the caller more bytes than it
            // said it could take, which is the one thing the ceiling exists
            // to prevent; there is no smaller page to fall back to, so the
            // answer is the size that shape really needs.
            return Err(MetadataPagingError::Oversized(oversized_refusal(
                page_bytes, encoded,
            )));
        }
    }
    finish_metadata_page(page)
}

fn key_of(item: &MetadataItem) -> String {
    match item {
        MetadataItem::Table(table) => table.clone(),
        MetadataItem::EventStatus(status) => status
            .get("name")
            .map(|name| format!("{name:?}"))
            .unwrap_or_default(),
    }
}

fn finish_metadata_page(
    page: MetadataPage,
) -> std::result::Result<MetadataPage, MetadataPagingError> {
    page.validate()
        .map_err(|_| MetadataPagingError::Encoding(ReadEncodingError::InvalidPayload))?;
    Ok(page)
}

/// Every declared event type, sink, and route as one ordered inventory, so a
/// page boundary can fall anywhere in it without losing an entry.
pub(crate) fn event_status_items(
    status: &crate::EventBusStatus,
    schedules: &[crate::database::CronScheduleStatus],
) -> Vec<MetadataItem> {
    let mut items = Vec::new();
    for event_type in &status.event_types {
        items.push(MetadataItem::EventStatus(BTreeMap::from([
            (
                "name".to_owned(),
                Value::Text(format!("event_type:{}", event_type.name)),
            ),
            ("kind".to_owned(), Value::Text("event_type".to_owned())),
            (
                "trigger".to_owned(),
                Value::Text(event_type.trigger.clone()),
            ),
            ("table".to_owned(), Value::Text(event_type.table.clone())),
        ])));
    }
    for sink in &status.sinks {
        items.push(MetadataItem::EventStatus(BTreeMap::from([
            (
                "name".to_owned(),
                Value::Text(format!("sink:{}", sink.name)),
            ),
            ("kind".to_owned(), Value::Text("sink".to_owned())),
            ("sink_type".to_owned(), Value::Text(sink.sink_type.clone())),
            (
                "callback_registered".to_owned(),
                Value::Bool(sink.callback_registered),
            ),
            (
                "delivered".to_owned(),
                Value::Int64(sink.metrics.delivered as i64),
            ),
            (
                "queued".to_owned(),
                Value::Int64(sink.metrics.queued as i64),
            ),
            (
                "retried".to_owned(),
                Value::Int64(sink.metrics.retried as i64),
            ),
            (
                "permanent_failures".to_owned(),
                Value::Int64(sink.metrics.permanent_failures as i64),
            ),
            (
                "examined".to_owned(),
                Value::Int64(sink.metrics.examined as i64),
            ),
        ])));
    }
    for route in &status.routes {
        items.push(MetadataItem::EventStatus(BTreeMap::from([
            (
                "name".to_owned(),
                Value::Text(format!("route:{}", route.name)),
            ),
            ("kind".to_owned(), Value::Text("route".to_owned())),
            (
                "event_type".to_owned(),
                Value::Text(route.event_type.clone()),
            ),
            ("sink".to_owned(), Value::Text(route.sink.clone())),
        ])));
    }
    // A schedule is part of what this store does on its own, so it belongs in
    // the same inventory: a reader that asks the owner what fires here and is
    // handed an empty list cannot tell that from a store with no schedules.
    for schedule in schedules {
        let mut fields = BTreeMap::from([
            (
                "name".to_owned(),
                Value::Text(format!("schedule:{}", schedule.name)),
            ),
            ("kind".to_owned(), Value::Text("schedule".to_owned())),
            ("every".to_owned(), Value::Text(schedule.every_text.clone())),
            (
                "callback".to_owned(),
                Value::Text(schedule.callback.clone()),
            ),
            (
                "callback_registered".to_owned(),
                Value::Bool(schedule.callback_registered),
            ),
            (
                "next_fire_at_ms".to_owned(),
                Value::Int64(schedule.next_fire_at_ms as i64),
            ),
            (
                "fire_count".to_owned(),
                Value::Int64(schedule.fire_count as i64),
            ),
        ]);
        // A schedule that has never fired says nothing about when it last
        // did, rather than saying it fired at the epoch.
        if let Some(fired) = schedule.last_fire_at_ms {
            fields.insert("last_fire_at_ms".to_owned(), Value::Int64(fired as i64));
        }
        items.push(MetadataItem::EventStatus(fields));
    }
    items.sort_by_key(key_of);
    items
}

/// Put one flattened event-inventory item back where it came from.
///
/// The owner pages the whole event surface as one list so a byte ceiling can
/// cut it anywhere, tagging each item with the kind it was flattened from.
/// An item tagged with a kind this build does not know is left out rather than
/// guessed into the wrong list.
pub(crate) fn promote_event_status_item(
    fields: &BTreeMap<String, Value>,
    status: &mut DirectEventsStatus,
) {
    let text = |key: &str| -> Option<String> {
        match fields.get(key) {
            Some(Value::Text(value)) => Some(value.clone()),
            _ => None,
        }
    };
    let flag = |key: &str| -> bool { matches!(fields.get(key), Some(Value::Bool(true))) };
    let number = |key: &str| -> u64 {
        match fields.get(key) {
            Some(Value::Int64(value)) => u64::try_from(*value).unwrap_or_default(),
            _ => 0,
        }
    };
    let Some(kind) = text("kind") else {
        return;
    };
    // The published name carries the kind it belongs to, so that one sorted
    // list can be cut anywhere; the name itself is what follows the colon.
    let named = |prefix: &str| -> String {
        text("name")
            .and_then(|name| name.strip_prefix(prefix).map(str::to_owned))
            .unwrap_or_default()
    };
    match kind.as_str() {
        "event_type" => status.event_types.push(DirectEventTypeStatus {
            name: named("event_type:"),
            trigger: text("trigger").unwrap_or_default(),
            table: text("table").unwrap_or_default(),
        }),
        "sink" => status.sinks.push(DirectSinkStatus {
            name: named("sink:"),
            sink_type: text("sink_type").unwrap_or_default(),
            callback_registered: flag("callback_registered"),
            delivered: number("delivered"),
            queued: number("queued"),
            retried: number("retried"),
            permanent_failures: number("permanent_failures"),
            examined: number("examined"),
        }),
        "route" => status.routes.push(DirectRouteStatus {
            name: named("route:"),
            event_type: text("event_type").unwrap_or_default(),
            sink: text("sink").unwrap_or_default(),
        }),
        "schedule" => status.schedules.push(DirectScheduleStatus {
            name: named("schedule:"),
            every: text("every").unwrap_or_default(),
            callback: text("callback").unwrap_or_default(),
            callback_registered: flag("callback_registered"),
            next_fire_at_ms: number("next_fire_at_ms"),
            // Absent means never fired, which is not the same as having
            // fired at the epoch.
            last_fire_at_ms: match fields.get("last_fire_at_ms") {
                Some(Value::Int64(value)) => u64::try_from(*value).ok(),
                _ => None,
            },
            fire_count: number("fire_count"),
        }),
        _ => {}
    }
}

/// The same ordered event inventory, built from a status that is already
/// typed.
///
/// The committed file holds its event surface as a `DirectEventsStatus`
/// rather than as the live bus, so it flattens from there -- through this one
/// function, so the items and their keys are identical to the ones the owner
/// publishes and a page boundary falls in the same place on both routes.
pub(crate) fn event_status_items_of(status: &DirectEventsStatus) -> Vec<MetadataItem> {
    let mut items = Vec::new();
    for event_type in &status.event_types {
        items.push(MetadataItem::EventStatus(BTreeMap::from([
            (
                "name".to_owned(),
                Value::Text(format!("event_type:{}", event_type.name)),
            ),
            ("kind".to_owned(), Value::Text("event_type".to_owned())),
            (
                "trigger".to_owned(),
                Value::Text(event_type.trigger.clone()),
            ),
            ("table".to_owned(), Value::Text(event_type.table.clone())),
        ])));
    }
    for sink in &status.sinks {
        items.push(MetadataItem::EventStatus(BTreeMap::from([
            (
                "name".to_owned(),
                Value::Text(format!("sink:{}", sink.name)),
            ),
            ("kind".to_owned(), Value::Text("sink".to_owned())),
            ("sink_type".to_owned(), Value::Text(sink.sink_type.clone())),
            (
                "callback_registered".to_owned(),
                Value::Bool(sink.callback_registered),
            ),
            ("delivered".to_owned(), Value::Int64(sink.delivered as i64)),
            ("queued".to_owned(), Value::Int64(sink.queued as i64)),
            ("retried".to_owned(), Value::Int64(sink.retried as i64)),
            (
                "permanent_failures".to_owned(),
                Value::Int64(sink.permanent_failures as i64),
            ),
            ("examined".to_owned(), Value::Int64(sink.examined as i64)),
        ])));
    }
    for route in &status.routes {
        items.push(MetadataItem::EventStatus(BTreeMap::from([
            (
                "name".to_owned(),
                Value::Text(format!("route:{}", route.name)),
            ),
            ("kind".to_owned(), Value::Text("route".to_owned())),
            (
                "event_type".to_owned(),
                Value::Text(route.event_type.clone()),
            ),
            ("sink".to_owned(), Value::Text(route.sink.clone())),
        ])));
    }
    for schedule in &status.schedules {
        let mut fields = BTreeMap::from([
            (
                "name".to_owned(),
                Value::Text(format!("schedule:{}", schedule.name)),
            ),
            ("kind".to_owned(), Value::Text("schedule".to_owned())),
            ("every".to_owned(), Value::Text(schedule.every.clone())),
            (
                "callback".to_owned(),
                Value::Text(schedule.callback.clone()),
            ),
            (
                "callback_registered".to_owned(),
                Value::Bool(schedule.callback_registered),
            ),
            (
                "next_fire_at_ms".to_owned(),
                Value::Int64(schedule.next_fire_at_ms as i64),
            ),
            (
                "fire_count".to_owned(),
                Value::Int64(schedule.fire_count as i64),
            ),
        ]);
        if let Some(fired) = schedule.last_fire_at_ms {
            fields.insert("last_fire_at_ms".to_owned(), Value::Int64(fired as i64));
        }
        items.push(MetadataItem::EventStatus(fields));
    }
    items.sort_by_key(key_of);
    items
}

/// Every table name as one ordered inventory, so a page boundary can fall
/// anywhere in it.
pub(crate) fn table_items(mut names: Vec<String>) -> Vec<MetadataItem> {
    names.sort();
    names.dedup();
    names.into_iter().map(MetadataItem::Table).collect()
}

/// The table names one page of a table inventory published.
pub(crate) fn table_names_of(page: &MetadataPage) -> Vec<String> {
    page.items
        .iter()
        .filter_map(|item| match item {
            MetadataItem::Table(name) => Some(name.clone()),
            MetadataItem::EventStatus(_) => None,
        })
        .collect()
}

/// The typed event inventory one page of a flattened event inventory
/// published.
pub(crate) fn events_of(page: &MetadataPage) -> DirectEventsStatus {
    let mut status = DirectEventsStatus {
        event_types: Vec::new(),
        sinks: Vec::new(),
        routes: Vec::new(),
        schedules: Vec::new(),
    };
    for item in &page.items {
        if let MetadataItem::EventStatus(fields) = item {
            promote_event_status_item(fields, &mut status);
        }
    }
    status
}
