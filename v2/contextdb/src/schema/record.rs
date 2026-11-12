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
use super::{Consolidation, Quantization, Rules, Table, VectorColumn};
use crate::sql::ast;
use crate::store::{CountId, IndexId, Read, TableId, VectorId, View};
use crate::types::{
    ColumnDef, ColumnDefault, ColumnType, ConflictPolicy, History, IndexDecl, KeptCounts, Result,
    Retain, RetainUnit, SortDirection, StateMachine, SyncDirection, TableMeta, TxId, Uuid, Value,
    VectorSearchMode,
};

/// The version of the record this code writes, and the only one it reads.
const VERSION: u8 = 1;

/// The schema record of a table.
pub(super) fn encode(table: &Table) -> Vec<u8> {
    let mut out = vec![VERSION];
    table.put(&mut out);
    out
}

/// The table a schema record was written from; damage when the bytes are not a record.
pub(super) fn decode(record: &[u8], view: &View) -> Result<Table> {
    let mut read = Reader(record);
    let table = match u8::get(&mut read) {
        Some(VERSION) => Table::get(&mut read),
        _ => None,
    };
    table
        .filter(|table| read.0.is_empty() && agrees(table))
        .ok_or_else(|| view.damaged("a schema record does not read as a table"))
}

/// Whether every column and index a table's rules name by number is one of the table's.
fn agrees(table: &Table) -> bool {
    let (meta, rules) = (&table.meta, &table.rules);
    let references = rules.references.iter();
    let counts = rules.counts.iter();
    let policies = rules.rank_policies.iter();
    let mut columns = (table.key.iter())
        .chain(&rules.immutable_columns)
        .chain(references.clone().flat_map(|reference| &reference.columns))
        .chain(counts.flat_map(|count| count.columns.iter().map(|(column, _)| column)))
        .chain(&rules.context_column)
        .chain(rules.scope_labels.iter().map(|(column, _)| column))
        .chain(rules.acl.iter().map(|(column, ..)| column))
        .chain(policies.flat_map(|policy| [&policy.column, &policy.anchor]));
    columns.all(|column| *column < meta.columns.len())
        && table.missing.len() == meta.columns.len()
        && table.index_ids.len() == meta.indexes.len()
        && { references }.all(|reference| reference.index < meta.indexes.len())
}

/// What is left of a record being read.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn bytes(&mut self, count: usize) -> Option<&'a [u8]> {
        let (taken, rest) = self.0.split_at_checked(count)?;
        self.0 = rest;
        Some(taken)
    }
}

/// A value as a schema record holds it.
trait Stored: Sized {
    fn put(&self, out: &mut Vec<u8>);

    /// The value at the front of what is left; `None` when the bytes there are not one.
    fn get(read: &mut Reader) -> Option<Self>;
}

macro_rules! numbers {
    ($($number:ty),*) => {$(
        impl Stored for $number {
            fn put(&self, out: &mut Vec<u8>) {
                out.extend(self.to_le_bytes());
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
    fn put(&self, out: &mut Vec<u8>) {
        (*self as u32).put(out);
    }

    fn get(read: &mut Reader) -> Option<Self> {
        u32::get(read).map(|number| number as usize)
    }
}

impl Stored for bool {
    fn put(&self, out: &mut Vec<u8>) {
        out.push(u8::from(*self));
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
    fn put(&self, out: &mut Vec<u8>) {
        self.len().put(out);
        out.extend(self.as_bytes());
    }

    fn get(read: &mut Reader) -> Option<Self> {
        let count = usize::get(read)?;
        String::from_utf8(read.bytes(count)?.to_vec()).ok()
    }
}

impl<T: Stored> Stored for Vec<T> {
    fn put(&self, out: &mut Vec<u8>) {
        self.len().put(out);
        self.iter().for_each(|item| item.put(out));
    }

    fn get(read: &mut Reader) -> Option<Self> {
        // Every item takes a byte at least, so a length past the record's end stops there.
        (0..usize::get(read)?).map(|_| T::get(read)).collect()
    }
}

impl<T: Stored> Stored for Option<T> {
    fn put(&self, out: &mut Vec<u8>) {
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
            fn put(&self, out: &mut Vec<u8>) {
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
    ($($name:ty { $($field:tt),* $(; $made:ident)? })*) => {$(
        impl Stored for $name {
            fn put(&self, out: &mut Vec<u8>) {
                $(self.$field.put(out);)*
            }

            fn get(read: &mut Reader) -> Option<Self> {
                Some(Self {
                    $($field: Stored::get(read)?,)*
                    $($made: Default::default(),)?
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
    Table { id, name, meta, index_ids, vectors, key, key_is_element, missing, rules; shapes }
    TableMeta {
        columns, unique_constraints, indexes, immutable, state_machine, dag, retain, history,
        sync, conflict, kept_counts
    }
    ColumnDef { name, column_type, nullable, primary_key, unique, default }
    IndexDecl { name, columns, json_path }
    VectorColumn {
        id, column, partition_key, max_partitions, search_mode, auto_index_at, consolidation,
        quantization
    }
    StateMachine { column, transitions }
    Retain { count, unit, sync_safe }
    Rules {
        immutable_columns, references, propagate_edges, exclude_vector_states,
        delivery_manifest, edge_discard, counts, context_column, scope_labels, acl,
        rank_policies
    }
    Reference { columns, parent, parent_columns, on_state, index }
    Count { id, columns }
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
            fn put(&self, out: &mut Vec<u8>) {
                out.push(*self as u8);
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
    fn put(&self, out: &mut Vec<u8>) {
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
    fn put(&self, out: &mut Vec<u8>) {
        match self {
            ColumnDefault::Now => out.push(0),
            ColumnDefault::Literal(value) => (1u8, value.clone()).put(out),
        }
    }

    fn get(read: &mut Reader) -> Option<Self> {
        match u8::get(read)? {
            0 => Some(ColumnDefault::Now),
            1 => Value::get(read).map(ColumnDefault::Literal),
            _ => None,
        }
    }
}

/// A default's value, and what a row with no cell for a column reads as.
impl Stored for Value {
    fn put(&self, out: &mut Vec<u8>) {
        match self {
            Value::Null => out.push(0),
            Value::Bool(flag) => (1u8, *flag).put(out),
            Value::Int64(whole) => (2u8, *whole).put(out),
            Value::Float64(real) => (3u8, *real).put(out),
            Value::Text(words) => (4u8, words.clone()).put(out),
            Value::Uuid(uuid) => (5u8, uuid.as_u128()).put(out),
            Value::Timestamp(millis) => (6u8, *millis).put(out),
            Value::Json(json) => (7u8, json.to_string()).put(out),
            Value::Vector(vector) => (8u8, vector.clone()).put(out),
            Value::TxId(tx) => (9u8, tx.0).put(out),
        }
    }

    fn get(read: &mut Reader) -> Option<Self> {
        Some(match u8::get(read)? {
            0 => Value::Null,
            1 => Value::Bool(Stored::get(read)?),
            2 => Value::Int64(Stored::get(read)?),
            3 => Value::Float64(Stored::get(read)?),
            4 => Value::Text(Stored::get(read)?),
            5 => Value::Uuid(Uuid::from_u128(Stored::get(read)?)),
            6 => Value::Timestamp(Stored::get(read)?),
            7 => Value::Json(serde_json::from_str(&String::get(read)?).ok()?),
            8 => Value::Vector(Stored::get(read)?),
            9 => Value::TxId(TxId(Stored::get(read)?)),
            _ => return None,
        })
    }
}

impl Stored for Consolidation {
    fn put(&self, out: &mut Vec<u8>) {
        match *self {
            Consolidation::None => out.push(0),
            Consolidation::Percent { change, tombstone } => (1u8, change, tombstone).put(out),
        }
    }

    fn get(read: &mut Reader) -> Option<Self> {
        match u8::get(read)? {
            0 => Some(Consolidation::None),
            1 => Some(Consolidation::Percent {
                change: Stored::get(read)?,
                tombstone: Stored::get(read)?,
            }),
            _ => None,
        }
    }
}

impl Stored for KeptCounts {
    fn put(&self, out: &mut Vec<u8>) {
        match self {
            KeptCounts::None => out.push(0),
            KeptCounts::AllPrefixes => out.push(1),
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
