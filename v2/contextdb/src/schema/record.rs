//! Storing a declaration: the bytes of a table's schema record, and the table read back from
//! them.
//!
//! A record begins with one byte that says which version of the record follows, and then holds
//! the table field by field. A number is its bytes, low byte first; the number of a column and
//! a length are four bytes. A text is its length in bytes and then its bytes. A list is its
//! length and then its items. A value that may be absent is one byte, 0 or 1, and then the
//! value when it is there. One of several kinds is one byte that says which.
//!
//! Reading trusts no byte: a length past the record's end, a byte that names no kind, a column
//! number that is no column and bytes left over are all damage.

use super::rules::{Count, RankPolicy, Reference};
use super::{Compaction, Kind, Quantization, Rules, Table, VectorColumn};
use crate::memory::{Passing, Reservation};
use crate::sql::ast;
use crate::store::{CountId, IndexId, Read, TableId, VectorId, View};
use crate::types::{
    ColumnDef, ColumnDefault, ColumnType, ConflictPolicy, Error, EventTypeDecl, EventTypeTrigger,
    History, IndexDecl, KeptCounts, MissedTickPolicy, Result, Retain, RetainUnit, RouteDecl,
    RouteWhereIn, ScheduleDecl, SinkDecl, SinkType, SortDirection, StateMachine, SyncDirection,
    TableMeta, TriggerDecl, TriggerEvent, Value, VectorSearchMode,
};

/// The version of the record this code writes, and the only one it reads.
const VERSION: u8 = 1; // Also the table kind; event records prepend their kind to this same version.

type Encoded = (Vec<u8>, u64);

/// The schema record of a table.
pub(super) fn encode(table: &mut Table) -> Vec<u8> {
    let mut out = (vec![VERSION], 0);
    table.put(&mut out);
    table.stored = out.0.len() as u32;
    table.heap = out.1 + table.shape_bytes();
    out.0
}

/// The table a schema record was written from; damage when the bytes are not a record.
pub(super) fn decode(
    record: &[u8],
    view: &View,
    held: &mut Reservation,
    cover: &mut Passing,
) -> Result<(Table, u64)> {
    let mut read = Reader::new(record, held, cover)?;
    let table = match u8::get(&mut read) {
        Some(VERSION) => Table::get(&mut read),
        _ => None,
    };
    if let Some(error) = read.refused.take() {
        return Err(error);
    }
    table
        .filter(|table| read.bytes.is_empty() && agrees(table))
        .map(|mut table| {
            read.held.shrink(read.funded - read.heap);
            table.heap = read.heap + table.shape_bytes();
            (table, read.heap)
        })
        .ok_or_else(|| view.damaged("a schema record does not read as a table"))
}

/// Whether every column and index a table's rules name by number is one of the table's.
fn agrees(table: &Table) -> bool {
    let (meta, rules) = (&table.meta, &table.rules);
    let has = |name: &String| meta.columns.iter().any(|c| c.name == *name);
    let references = rules.references.iter();
    let policies = rules.rank_policies.iter();
    let mut columns = (table.key.iter())
        .chain(&rules.immutable_columns)
        .chain(references.clone().flat_map(|reference| &reference.columns))
        .chain(&rules.context_column)
        .chain(&rules.expires_column)
        .chain(rules.scope_labels.iter().map(|(column, _)| column))
        .chain(rules.acl.iter().map(|(column, ..)| column))
        .chain(policies.flat_map(|policy| [&policy.column, &policy.anchor]));
    columns.all(|column| *column < meta.columns.len())
        && meta
            .indexes
            .iter()
            .flat_map(|i| &i.columns)
            .all(|(name, _)| has(name))
        && rules.counts.iter().all(|c| {
            meta.indexes.get(c.index).is_some_and(|i| {
                c.prefix > 0 && c.prefix <= i.columns.len() && i.json_path.is_empty()
            })
        })
        && table.vectors.iter().all(|v| {
            let column = meta.columns.iter().find(|c| c.name == v.column);
            column.is_some_and(|c| matches!(c.column_type, ColumnType::Vector(_)))
                && v.partition_key.iter().all(has)
        })
        && meta.state_machine.as_ref().is_none_or(|m| has(&m.column))
        && table.missing.len() == meta.columns.len()
        && table.index_ids.len() == meta.indexes.len() + usize::from(meta.retain.is_some())
        && { references }.all(|reference| reference.index < meta.indexes.len())
}

/// What is left of a record being read.
struct Reader<'a, 'b> {
    bytes: &'a [u8],
    heap: u64,
    funded: u64,
    held: &'b mut Reservation,
    cover: &'b mut Passing,
    refused: Option<Error>,
}

impl<'a, 'b> Reader<'a, 'b> {
    fn new(bytes: &'a [u8], held: &'b mut Reservation, cover: &'b mut Passing) -> Result<Self> {
        let funded = super::SCHEMA * bytes.len() as u64;
        held.grow(funded)?;
        Ok(Self {
            bytes,
            heap: 0,
            funded,
            held,
            cover,
            refused: None,
        })
    }

    fn count(&mut self, bytes: u64) -> Option<()> {
        self.heap += bytes;
        if self.heap > self.funded {
            if let Err(error) = self.held.grow(self.heap - self.funded) {
                self.refused = Some(error);
                return None;
            }
            self.funded = self.heap;
        }
        Some(())
    }

    fn bytes(&mut self, count: usize) -> Option<&'a [u8]> {
        let (taken, rest) = self.bytes.split_at_checked(count)?;
        self.bytes = rest;
        Some(taken)
    }
}

/// A value as a schema record holds it.
trait Stored: Sized {
    fn put(&self, out: &mut Encoded);

    /// The value at the front of what is left; `None` when the bytes there are not one.
    fn get(read: &mut Reader) -> Option<Self>;
}

macro_rules! numbers {
    ($($number:ty),*) => {$(
        impl Stored for $number {
            fn put(&self, out: &mut Encoded) {
                out.0.extend(self.to_le_bytes());
            }

            fn get(read: &mut Reader) -> Option<Self> {
                let bytes = read.bytes(size_of::<Self>())?;
                Some(Self::from_le_bytes(bytes.try_into().ok()?))
            }
        }
    )*};
}
numbers!(u8, u32, u64, u128, i64, f32, f64);

impl Stored for usize {
    fn put(&self, out: &mut Encoded) {
        (*self as u32).put(out);
    }

    fn get(read: &mut Reader) -> Option<Self> {
        u32::get(read).map(|number| number as usize)
    }
}

impl Stored for bool {
    fn put(&self, out: &mut Encoded) {
        out.0.push(u8::from(*self));
    }

    fn get(read: &mut Reader) -> Option<Self> {
        match u8::get(read)? {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }
}

impl Stored for String {
    fn put(&self, out: &mut Encoded) {
        out.1 += self.len() as u64;
        self.len().put(out);
        out.0.extend(self.as_bytes());
    }

    fn get(read: &mut Reader) -> Option<Self> {
        let count = usize::get(read)?;
        let bytes = read.bytes(count)?;
        read.count(count as u64)?;
        String::from_utf8(bytes.to_vec()).ok()
    }
}

impl<T: Stored> Stored for Vec<T> {
    fn put(&self, out: &mut Encoded) {
        out.1 += (self.len() * size_of::<T>()) as u64;
        self.len().put(out);
        self.iter().for_each(|item| item.put(out));
    }

    fn get(read: &mut Reader) -> Option<Self> {
        // Every item takes a byte at least, so a length past the record's end stops there.
        let count = usize::get(read).filter(|&count| count <= read.bytes.len())?;
        read.count((count * size_of::<T>()) as u64)?;
        let mut values = Vec::with_capacity(count);
        values.extend((0..count).map_while(|_| T::get(read)));
        (values.len() == count).then_some(values)
    }
}

impl<T: Stored> Stored for Option<T> {
    fn put(&self, out: &mut Encoded) {
        self.is_some().put(out);
        self.iter().for_each(|value| value.put(out));
    }

    fn get(read: &mut Reader) -> Option<Self> {
        match bool::get(read)? {
            true => T::get(read).map(Some),
            false => Some(None),
        }
    }
}

macro_rules! tuples {
    ($(($($item:ident $at:tt),*))*) => {$(
        impl<$($item: Stored),*> Stored for ($($item,)*) {
            fn put(&self, out: &mut Encoded) {
                $(self.$at.put(out);)*
            }

            fn get(read: &mut Reader) -> Option<Self> {
                Some(($($item::get(read)?,)*))
            }
        }
    )*};
}
tuples!((A 0, B 1) (A 0, B 1, C 2));

/// A type whose record is its fields in this order. A field after `;` is not stored: it is
/// worked out from the others when the table is loaded.
macro_rules! fields {
    ($($name:ty { $($field:tt),* $(; $($made:ident),*)? })*) => {$(
        impl Stored for $name {
            fn put(&self, out: &mut Encoded) {
                $(self.$field.put(out);)*
            }

            fn get(read: &mut Reader) -> Option<Self> {
                Some(Self {
                    $($field: Stored::get(read)?,)*
                    $($($made: Default::default(),)*)?
                })
            }
        }
    )*};
}
fields! {
    TableId { 0 }
    IndexId { 0 }
    VectorId { 0 }
    CountId { 0 }
    Table { id, name, meta, index_ids, vectors, key, key_is_element, missing, rules; shapes, stored, heap }
    TableMeta {
        columns, unique_constraints, indexes, immutable, state_machine, dag, retain, history,
        sync, conflict, kept_counts; immutable_columns, expires_column, delivery_manifest, edge_discard, propagation_rules, propagate_edges, exclude_vector_states, vectors, scope_label
    }
    ColumnDef { name, column_type, nullable, primary_key, unique, default }
    IndexDecl { name, columns, json_path }
    VectorColumn {
        id, column, partition_key, max_partitions, search_mode, indexing_threshold, probes, compaction,
        quantization
    }
    StateMachine { column, transitions }
    Retain { count, unit, sync_safe }
    Rules {
        immutable_columns, references, propagate_edges, exclude_vector_states,
        delivery_manifest, edge_discard, counts, context_column, scope_labels, acl,
        rank_policies, expires_column, retain_since
    }
    Reference { columns, parent, parent_columns, on_state, index, follows_parent_purge }
    Count { id, index, prefix }
    RankPolicy { column, anchor, policy }
    ast::Propagation { on_state, set, max_depth, abort_on_failure }
    ast::EdgePropagation { edge_type, direction, rule }
    ast::ScopeLabels { read, write }
    ast::RankPolicy { table, column, formula, sort_key }
}

/// A type that is one of these kinds and nothing more: one byte, the kind's place here.
macro_rules! kinds {
    ($($name:ty { $($kind:ident),* })*) => {$(
        impl Stored for $name {
            fn put(&self, out: &mut Encoded) {
                out.0.push(*self as u8);
            }

            fn get(read: &mut Reader) -> Option<Self> {
                let kind = u8::get(read)?;
                [$(Self::$kind),*].into_iter().find(|known| *known as u8 == kind)
            }
        }
    )*};
}
kinds! {
    SortDirection { Asc, Desc }
    VectorSearchMode { Auto, Exact, Indexed }
    Quantization { F32, Sq8 }
    RetainUnit { Seconds, Minutes, Hours, Days }
    History { All, CurrentOnly }
    SyncDirection { Off, PushOnly, PullOnly, TwoWay }
    ConflictPolicy { KeepFirst, KeepLatest }
    ast::EdgeDiscard { Never, AfterOutcome, Always }
}

impl Stored for ColumnType {
    fn put(&self, out: &mut Encoded) {
        let kind: (u8, usize) = match *self {
            ColumnType::Integer => (0, 0),
            ColumnType::Real => (1, 0),
            ColumnType::Text => (2, 0),
            ColumnType::Boolean => (3, 0),
            ColumnType::Json => (4, 0),
            ColumnType::Uuid => (5, 0),
            ColumnType::Timestamp => (6, 0),
            ColumnType::TxId => (7, 0),
            ColumnType::Vector(size) => (8, size),
        };
        kind.put(out);
    }

    fn get(read: &mut Reader) -> Option<Self> {
        let (kind, size) = <(u8, usize)>::get(read)?;
        Some(match kind {
            0 => ColumnType::Integer,
            1 => ColumnType::Real,
            2 => ColumnType::Text,
            3 => ColumnType::Boolean,
            4 => ColumnType::Json,
            5 => ColumnType::Uuid,
            6 => ColumnType::Timestamp,
            7 => ColumnType::TxId,
            8 => ColumnType::Vector(size),
            _ => return None,
        })
    }
}

impl Stored for ColumnDefault {
    fn put(&self, out: &mut Encoded) {
        match self {
            ColumnDefault::Now => out.0.push(0),
            ColumnDefault::Literal(value) => (1u8, value.clone()).put(out),
        }
    }

    fn get(read: &mut Reader) -> Option<Self> {
        match u8::get(read)? {
            0 => Some(ColumnDefault::Now),
            1 if !matches!(read.bytes.first().copied(), Some(7 | 8 | 0x88)) => Value::get(read)
                .filter(|v| {
                    matches!(
                        v,
                        Value::Null
                            | Value::Bool(_)
                            | Value::Int64(_)
                            | Value::Float64(_)
                            | Value::Text(_)
                    )
                })
                .map(ColumnDefault::Literal),
            _ => None,
        }
    }
}

/// A default's value, and what a row with no cell for a column reads as.
impl Stored for Value {
    fn put(&self, out: &mut Encoded) {
        out.1 += crate::types::heap_bytes(self);
        crate::store::put_value(&mut out.0, self);
    }

    fn get(read: &mut Reader) -> Option<Self> {
        match crate::store::value_prefix(&mut read.bytes, read.cover) {
            Ok(Some(value)) => {
                read.count(crate::types::heap_bytes(&value))?;
                Some(value)
            }
            Ok(None) => None,
            Err(error) => {
                read.refused = Some(error);
                None
            }
        }
    }
}

impl Stored for Compaction {
    fn put(&self, out: &mut Encoded) {
        match *self {
            Compaction::None => out.0.push(0),
            Compaction::Percent { change, tombstone } => (1u8, change, tombstone).put(out),
        }
    }

    fn get(read: &mut Reader) -> Option<Self> {
        match u8::get(read)? {
            0 => Some(Compaction::None),
            1 => Some(Compaction::Percent {
                change: Stored::get(read)?,
                tombstone: Stored::get(read)?,
            }),
            _ => None,
        }
    }
}

impl Stored for KeptCounts {
    fn put(&self, out: &mut Encoded) {
        match self {
            KeptCounts::None => out.0.push(0),
            KeptCounts::AllPrefixes => out.0.push(1),
            KeptCounts::Lists(lists) => (2u8, lists.clone()).put(out),
        }
    }

    fn get(read: &mut Reader) -> Option<Self> {
        match u8::get(read)? {
            0 => Some(KeptCounts::None),
            1 => Some(KeptCounts::AllPrefixes),
            2 => Stored::get(read).map(KeptCounts::Lists),
            _ => None,
        }
    }
}

fields! {
    TriggerDecl { name, table, event, including_sync }
    EventTypeDecl { name, table, event }
    SinkDecl { name, sink_type, url }
    RouteWhereIn { column, values }
    RouteDecl { name, event_type, sink, where_in }
    ScheduleDecl { name, every_ms, callback, missed_tick_policy, catch_up_within_seconds }
}
kinds! {
    Kind { Table, Trigger, EventType, Sink, Route, Schedule }
    MissedTickPolicy { SkipAndAudit, CatchUp, FailLoud }
    TriggerEvent { Insert, Update }
    EventTypeTrigger { Insert, Update, Delete }
    SinkType { Callback, Webhook }
}
pub(super) fn encode_event(event: &ast::EventDeclaration) -> Encoded {
    use ast::EventDeclaration::*;
    let mut out = (vec![event.identity().0 as u8, VERSION], 0);
    match event {
        Trigger(d) => d.put(&mut out),
        EventType(d) => d.put(&mut out),
        Sink(d) => d.put(&mut out),
        Route(d) => d.put(&mut out),
        Schedule(d) => d.put(&mut out),
    }
    out
}
pub(super) fn decode_event(
    bytes: &[u8],
    view: &View,
    held: &mut Reservation,
    cover: &mut Passing,
) -> Result<(ast::EventDeclaration, u64)> {
    use ast::EventDeclaration::*;
    let mut read = Reader::new(bytes, held, cover)?;
    let parsed = (|| {
        let kind = Kind::get(&mut read)?;
        if u8::get(&mut read)? != VERSION {
            return None;
        }
        Some(match kind {
            Kind::Trigger => Trigger(Stored::get(&mut read)?),
            Kind::EventType => EventType(Stored::get(&mut read)?),
            Kind::Sink => Sink(Stored::get(&mut read)?),
            Kind::Route => Route(Stored::get(&mut read)?),
            Kind::Schedule => Schedule(Stored::get(&mut read)?),
            _ => return None,
        })
    })();
    if let Some(error) = read.refused.take() {
        return Err(error);
    }
    parsed
        .filter(|_| read.bytes.is_empty())
        .map(|event| {
            read.held.shrink(read.funded - read.heap);
            (event, read.heap)
        })
        .ok_or_else(|| view.damaged("a schema record does not read as an event declaration"))
}
