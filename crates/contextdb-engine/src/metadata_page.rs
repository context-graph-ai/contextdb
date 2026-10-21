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
                "uncertain".to_owned(),
                Value::Int64(sink.metrics.uncertain as i64),
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
    let Some(Value::Text(kind)) = fields.get("kind") else {
        return;
    };
    // The published name carries the kind it belongs to, so that one sorted
    // list can be cut anywhere; the name itself is what follows the colon.
    let named = |prefix: &str| -> String {
        match fields.get("name") {
            Some(Value::Text(name)) => name.strip_prefix(prefix).map(str::to_owned).unwrap_or_default(),
            _ => String::new(),
        }
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
            uncertain: number("uncertain"),
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
            ("uncertain".to_owned(), Value::Int64(sink.uncertain as i64)),
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

/// Admit only the actual new promotion buffers; original page storage retains
/// its separate original decode/source invoice throughout this borrow.
pub(crate) fn table_names_of_funded(
    page: &MetadataPage, memory: &crate::read_image_memory::ImageMemoryHandle,
) -> contextdb_core::Result<Vec<String>> {
    let names = page.items.iter().filter_map(|item| match item { MetadataItem::Table(name) => Some(name), _ => None });
    let count = names.clone().count();
    let mut bytes = count.checked_mul(std::mem::size_of::<String>()).ok_or_else(promotion_size_error)?;
    for name in names.clone() { bytes = bytes.checked_add(name.len()).ok_or_else(promotion_size_error)?; }
    memory.reserve(bytes)?;
    let mut output = Vec::with_capacity(count);
    for name in names { output.push(name.clone()); }
    Ok(output)
}
fn promotion_size_error() -> contextdb_core::Error {
    contextdb_core::Error::Other("metadata promotion size overflow".into())
}
pub(crate) fn events_of_funded(
    page: &MetadataPage, memory: &crate::read_image_memory::ImageMemoryHandle,
) -> contextdb_core::Result<DirectEventsStatus> {
    let mut counts = [0usize; 4];
    let mut bytes = 0usize;
    let mut add = |value: usize| -> contextdb_core::Result<()> {
        bytes = bytes.checked_add(value).ok_or_else(promotion_size_error)?; Ok(())
    };
    for item in &page.items {
        let MetadataItem::EventStatus(fields) = item else { continue; };
        let Some(Value::Text(kind)) = fields.get("kind") else { continue; };
        let (slot, prefix, width, texts): (usize, &str, usize, &[&str]) = match kind.as_str() {
            "event_type" => (0, "event_type:", std::mem::size_of::<DirectEventTypeStatus>(), &["trigger", "table"]),
            "sink" => (1, "sink:", std::mem::size_of::<DirectSinkStatus>(), &["sink_type"]),
            "route" => (2, "route:", std::mem::size_of::<DirectRouteStatus>(), &["event_type", "sink"]),
            "schedule" => (3, "schedule:", std::mem::size_of::<DirectScheduleStatus>(), &["every", "callback"]),
            _ => continue,
        };
        counts[slot] = counts[slot].checked_add(1).ok_or_else(promotion_size_error)?;
        add(width)?;
        if let Some(Value::Text(name)) = fields.get("name") { add(name.strip_prefix(prefix).map_or(0, str::len))?; }
        for key in texts { if let Some(Value::Text(text)) = fields.get(*key) { add(text.len())?; } }
    }
    memory.reserve(bytes)?;
    let mut status = DirectEventsStatus {
        event_types: Vec::with_capacity(counts[0]), sinks: Vec::with_capacity(counts[1]),
        routes: Vec::with_capacity(counts[2]), schedules: Vec::with_capacity(counts[3]),
    };
    for item in &page.items {
        if let MetadataItem::EventStatus(fields) = item { promote_event_status_item(fields, &mut status); }
    }
    Ok(status)
}

// Production inventories borrow their captured source. Only the selected page
// is materialized, through the existing admitted decoder on the same owner.
use crate::local_transport::query_funding::Funded;
use contextdb_core::read_memory::ReadMemoryOwner;
use serde::Serialize;

#[derive(Clone, Copy)]
enum InventoryValue<'a> { Text(&'a str), Bool(bool), Int(i64) }
impl Serialize for InventoryValue<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Text(text) => s.serialize_newtype_variant("Value", 4, "Text", text),
            Self::Bool(value) => s.serialize_newtype_variant("Value", 1, "Bool", value),
            Self::Int(value) => s.serialize_newtype_variant("Value", 2, "Int64", value),
        }
    }
}
struct EventProjection<'a> {
    name: Funded<String>,
    key: Funded<String>,
    kind: &'static str,
    fields: [Option<(&'static str, InventoryValue<'a>)>; 8],
}
impl Serialize for EventProjection<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut fields = [None; 10];
        fields[..8].copy_from_slice(&self.fields);
        fields[8] = Some(("name", InventoryValue::Text(&self.name.value)));
        fields[9] = Some(("kind", InventoryValue::Text(self.kind)));
        fields.sort_unstable_by_key(|entry| entry.map(|(key, _)| key));
        let mut map = s.serialize_map(Some(fields.iter().flatten().count()))?;
        for (key, value) in fields.into_iter().flatten() { map.serialize_entry(key, &value)?; }
        map.end()
    }
}
fn inventory_text(owner: &ReadMemoryOwner, text: std::fmt::Arguments<'_>) -> contextdb_core::Result<Funded<String>> {
    struct Count(usize);
    impl std::fmt::Write for Count {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            self.0 = self.0.checked_add(text.len()).ok_or(std::fmt::Error)?; Ok(())
        }
    }
    let mut count = Count(0);
    std::fmt::write(&mut count, text).map_err(|_| promotion_size_error())?;
    let mut output = Funded::allocate_owned(Some(owner.clone()), count.0, || String::with_capacity(count.0))?;
    std::fmt::write(&mut output.value, text).map_err(|_| promotion_size_error())?;
    Ok(output)
}
fn event_projection<'a>(
    owner: &ReadMemoryOwner, kind: &'static str, name: &str,
    fields: &[(&'static str, InventoryValue<'a>)],
) -> contextdb_core::Result<EventProjection<'a>> {
    let name = inventory_text(owner, format_args!("{kind}:{name}"))?;
    // This is the established key_of(Value::Text) spelling, including Debug
    // escaping. Keep the funded key through sorting and page selection.
    let key = inventory_text(owner, format_args!("Text({:?})", name.value))?;
    let mut projected = EventProjection { name, key, kind, fields: [None; 8] };
    for (slot, field) in projected.fields.iter_mut().zip(fields) { *slot = Some(*field); }
    Ok(projected)
}
#[derive(Clone, Copy)]
enum Inventory<'a> { Tables(&'a [&'a str]), Events(&'a [EventProjection<'a>]) }
impl Inventory<'_> {
    fn len(&self) -> usize { match self { Self::Tables(items) => items.len(), Self::Events(items) => items.len() } }
    fn key(&self, at: usize) -> &str { match self { Self::Tables(items) => items[at], Self::Events(items) => &items[at].key.value } }
    fn window(&self, start: usize, end: usize) -> Self {
        match self { Self::Tables(items) => Self::Tables(&items[start..end]), Self::Events(items) => Self::Events(&items[start..end]) }
    }
}
impl Serialize for Inventory<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        #[derive(Serialize)]
        enum Item<'a> { Table(&'a str), EventStatus(&'a EventProjection<'a>) }
        let mut seq = s.serialize_seq(Some(self.len()))?;
        match self {
            Self::Tables(items) => for item in *items { seq.serialize_element(&Item::Table(item))?; },
            Self::Events(items) => for item in *items { seq.serialize_element(&Item::EventStatus(item))?; },
        }
        seq.end()
    }
}
#[derive(Serialize)]
struct InventoryPage<'a> {
    vocabulary: MetadataPageVocabulary,
    items: Inventory<'a>,
    has_more: bool,
    continuation: Option<&'a str>,
}
fn inventory_page(
    owner: ReadMemoryOwner, vocabulary: MetadataPageVocabulary, inventory: Inventory<'_>,
    continuation: Option<&str>, page_bytes: u64,
) -> Result<Funded<MetadataPage>, MetadataPagingError> {
    let error = |error| MetadataPagingError::Encoding(ReadEncodingError::Admission(error));
    let resume = continuation.map(|token| token.strip_prefix(issuing_question(vocabulary))
        .and_then(|rest| rest.strip_prefix(':')).ok_or_else(|| MetadataPagingError::Continuation(
            ReadFailure::invalid_continuation(format!("this continuation was not issued by {}", issuing_question(vocabulary)))))).transpose()?;
    let start = (0..inventory.len()).find(|&at| resume.is_none_or(|resume| inventory.key(at) > resume)).unwrap_or(inventory.len());
    let mut end = start;
    // Measure each candidate in exactly the same terminal/nonterminal shape
    // as the original pager. No candidate item or source map is cloned.
    while end < inventory.len() {
        let candidate = end + 1;
        let has_more = candidate < inventory.len();
        let token = if has_more { Some(inventory_text(&owner,
            format_args!("{}:{}", issuing_question(vocabulary), inventory.key(candidate - 1))).map_err(error)?) } else { None };
        let view = InventoryPage { vocabulary, items: inventory.window(start, candidate), has_more,
            continuation: token.as_ref().map(|token| token.value.as_str()) };
        let size = crate::read_contract::encoded_size(&view).map_err(MetadataPagingError::Encoding)? as u64;
        if size > page_bytes {
            if end == start { return Err(MetadataPagingError::Oversized(oversized_refusal(page_bytes, size))); }
            break;
        }
        end = candidate;
    }
    let has_more = end < inventory.len();
    let token = if has_more { Some(inventory_text(&owner,
        format_args!("{}:{}", issuing_question(vocabulary), inventory.key(end - 1))).map_err(error)?) } else { None };
    let view = InventoryPage { vocabulary, items: inventory.window(start, end), has_more,
        continuation: token.as_ref().map(|token| token.value.as_str()) };
    let bytes = crate::read_contract::encode_transport_funded(&view, Some(owner.clone()), u64::MAX)
        .map_err(MetadataPagingError::Encoding)?;
    let memory = crate::local_transport::query_funding::QueryMemory::with_original_owner(owner, u64::MAX)
        .map_err(error)?.image();
    let page: Funded<MetadataPage> = crate::local_transport::query_funding::decode(&bytes.value, &memory).map_err(error)?;
    page.value.validate().map_err(|_| MetadataPagingError::Encoding(ReadEncodingError::InvalidPayload))?;
    Ok(page)
}
pub(crate) fn borrowed_table_page<'a>(
    owner: ReadMemoryOwner, names: impl ExactSizeIterator<Item = &'a str>,
    continuation: Option<&str>, page_bytes: u64,
) -> Result<Funded<MetadataPage>, MetadataPagingError> {
    let mut names = Funded::allocate_owned(Some(owner.clone()), names.len() * std::mem::size_of::<&str>(),
        || names.collect::<Vec<_>>()).map_err(|error| MetadataPagingError::Encoding(ReadEncodingError::Admission(error)))?;
    names.value.sort_unstable(); names.value.dedup();
    inventory_page(owner, MetadataPageVocabulary::Tables, Inventory::Tables(&names.value), continuation, page_bytes)
}
pub(crate) fn borrowed_event_page(
    owner: ReadMemoryOwner, status: &DirectEventsStatus, continuation: Option<&str>, page_bytes: u64,
) -> Result<Funded<MetadataPage>, MetadataPagingError> {
    use InventoryValue::{Text, Bool, Int};
    let error = |error| MetadataPagingError::Encoding(ReadEncodingError::Admission(error));
    let count = status.event_types.len() + status.sinks.len() + status.routes.len() + status.schedules.len();
    let mut items = Funded::allocate_owned(Some(owner.clone()), count * std::mem::size_of::<EventProjection<'_>>(),
        || Vec::with_capacity(count)).map_err(error)?;
    for event in &status.event_types { items.value.push(event_projection(&owner, "event_type", &event.name,
        &[("trigger", Text(&event.trigger)), ("table", Text(&event.table))]).map_err(error)?); }
    for sink in &status.sinks { items.value.push(event_projection(&owner, "sink", &sink.name,
        &[("sink_type", Text(&sink.sink_type)), ("callback_registered", Bool(sink.callback_registered)),
          ("delivered", Int(sink.delivered as i64)), ("queued", Int(sink.queued as i64)),
          ("retried", Int(sink.retried as i64)), ("uncertain", Int(sink.uncertain as i64)),
          ("permanent_failures", Int(sink.permanent_failures as i64)), ("examined", Int(sink.examined as i64))]).map_err(error)?); }
    for route in &status.routes { items.value.push(event_projection(&owner, "route", &route.name,
        &[("event_type", Text(&route.event_type)), ("sink", Text(&route.sink))]).map_err(error)?); }
    for schedule in &status.schedules {
        let mut item = event_projection(&owner, "schedule", &schedule.name,
            &[("every", Text(&schedule.every)), ("callback", Text(&schedule.callback)),
              ("callback_registered", Bool(schedule.callback_registered)), ("next_fire_at_ms", Int(schedule.next_fire_at_ms as i64)),
              ("fire_count", Int(schedule.fire_count as i64))]).map_err(error)?;
        if let Some(last) = schedule.last_fire_at_ms { item.fields[5] = Some(("last_fire_at_ms", Int(last as i64))); }
        items.value.push(item);
    }
    items.value.sort_unstable_by(|left, right| left.key.value.cmp(&right.key.value));
    inventory_page(owner, MetadataPageVocabulary::EventsStatus, Inventory::Events(&items.value), continuation, page_bytes)
}
