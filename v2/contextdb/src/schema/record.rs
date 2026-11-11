//! Storing a declaration: the bytes of a table's schema record, and the table read back from
//! them.
//!
//! Every number is four bytes, low byte first. A text is its length in bytes and then its
//! bytes. A list is its length and then its items.
//!
//! Reading trusts the bytes: a record is only ever what `encode` wrote, and the store checks
//! every page it reads. Where a byte says which of several kinds follows, the last kind is read
//! for any byte the earlier ones do not claim.

use super::{Table, VectorColumn};
use crate::store::{IndexId, TableId, VectorId};
use crate::types::{
    ColumnDef, ColumnDefault, ColumnType, IndexDecl, SortDirection, TableMeta, Value,
    VectorSearchMode,
};

/// The schema record of a table.
pub(super) fn encode(table: &Table) -> Vec<u8> {
    let meta = &table.meta;
    let mut out = Vec::new();
    number(&mut out, table.id.0 as usize);
    text(&mut out, &table.name);
    number(&mut out, meta.columns.len());
    for column in &meta.columns {
        text(&mut out, &column.name);
        let (kind, size) = match column.column_type {
            ColumnType::Integer => (0, 0),
            ColumnType::Real => (1, 0),
            ColumnType::Text => (2, 0),
            ColumnType::Boolean => (3, 0),
            ColumnType::Json => (4, 0),
            ColumnType::Uuid => (5, 0),
            ColumnType::Timestamp => (6, 0),
            ColumnType::Vector(size) => (8, size),
        };
        out.push(kind);
        number(&mut out, size);
        out.push(u8::from(column.nullable));
        out.push(u8::from(column.primary_key));
        out.push(u8::from(column.unique));
        match &column.default {
            None => out.push(0),
            Some(ColumnDefault::Now) => out.push(1),
            Some(ColumnDefault::Literal(Value::Null)) => out.push(2),
            Some(ColumnDefault::Literal(Value::Bool(flag))) => out.extend([3, u8::from(*flag)]),
            Some(ColumnDefault::Literal(Value::Int64(whole))) => {
                out.push(4);
                out.extend(whole.to_le_bytes());
            }
            Some(ColumnDefault::Literal(Value::Float64(real))) => {
                out.push(5);
                out.extend(real.to_le_bytes());
            }
            Some(ColumnDefault::Literal(Value::Text(words))) => {
                out.push(6);
                text(&mut out, words);
            }
            Some(ColumnDefault::Literal(other)) => {
                unreachable!("a default is declared as a literal, never as {other:?}")
            }
        }
    }
    number(&mut out, meta.unique_constraints.len());
    for rule in &meta.unique_constraints {
        number(&mut out, rule.len());
        for column in rule {
            text(&mut out, column);
        }
    }
    number(&mut out, meta.indexes.len());
    for (index, id) in meta.indexes.iter().zip(&table.index_ids) {
        number(&mut out, id.0 as usize);
        text(&mut out, &index.name);
        number(&mut out, index.columns.len());
        for (column, direction) in &index.columns {
            text(&mut out, column);
            out.push(u8::from(*direction == SortDirection::Desc));
        }
    }
    number(&mut out, table.vectors.len());
    for vector in &table.vectors {
        number(&mut out, vector.id.0 as usize);
        text(&mut out, &vector.column);
        number(&mut out, vector.partition_key.len());
        for column in &vector.partition_key {
            text(&mut out, column);
        }
        // A declared MAX_PARTITIONS is never zero, so zero says that none was declared.
        number(&mut out, vector.max_partitions.unwrap_or(0) as usize);
        out.push(match vector.search_mode {
            None => 0,
            Some(VectorSearchMode::Auto) => 1,
            Some(VectorSearchMode::Exact) => 2,
            Some(VectorSearchMode::Indexed) => 3,
        });
    }
    out
}

/// The table a schema record was written from.
pub(super) fn decode(record: &[u8]) -> Table {
    let mut read = Reader(record);
    let id = TableId(read.number() as u32);
    let name = read.text();
    let columns = read.list(|read| {
        let name = read.text();
        let kind = read.byte();
        let size = read.number();
        ColumnDef {
            name,
            column_type: match kind {
                0 => ColumnType::Integer,
                1 => ColumnType::Real,
                2 => ColumnType::Text,
                3 => ColumnType::Boolean,
                4 => ColumnType::Json,
                5 => ColumnType::Uuid,
                6 => ColumnType::Timestamp,
                _ => ColumnType::Vector(size),
            },
            nullable: read.flag(),
            primary_key: read.flag(),
            unique: read.flag(),
            default: match read.byte() {
                0 => None,
                1 => Some(ColumnDefault::Now),
                kind => Some(ColumnDefault::Literal(match kind {
                    2 => Value::Null,
                    3 => Value::Bool(read.flag()),
                    4 => Value::Int64(i64::from_le_bytes(read.array())),
                    5 => Value::Float64(f64::from_le_bytes(read.array())),
                    _ => Value::Text(read.text()),
                })),
            },
        }
    });
    let unique_constraints = read.list(|read| read.list(Reader::text));
    let mut index_ids = Vec::new();
    let indexes = read.list(|read| {
        index_ids.push(IndexId(read.number() as u32));
        let name = read.text();
        let columns = read.list(|read| {
            let column = read.text();
            let direction = if read.flag() {
                SortDirection::Desc
            } else {
                SortDirection::Asc
            };
            (column, direction)
        });
        IndexDecl { name, columns }
    });
    let vectors = read.list(|read| VectorColumn {
        id: VectorId(read.number() as u32),
        column: read.text(),
        partition_key: read.list(Reader::text),
        max_partitions: Some(read.number() as u32).filter(|limit| *limit != 0),
        search_mode: match read.byte() {
            0 => None,
            1 => Some(VectorSearchMode::Auto),
            2 => Some(VectorSearchMode::Exact),
            _ => Some(VectorSearchMode::Indexed),
        },
    });
    Table {
        id,
        name,
        meta: TableMeta {
            columns,
            unique_constraints,
            indexes,
        },
        index_ids,
        vectors,
    }
}

fn number(out: &mut Vec<u8>, number: usize) {
    out.extend((number as u32).to_le_bytes());
}

fn text(out: &mut Vec<u8>, text: &str) {
    number(out, text.len());
    out.extend(text.as_bytes());
}

/// What is left of a record being read.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn bytes(&mut self, count: usize) -> &'a [u8] {
        let (taken, rest) = self.0.split_at(count);
        self.0 = rest;
        taken
    }

    fn array<const N: usize>(&mut self) -> [u8; N] {
        let mut array = [0; N];
        array.copy_from_slice(self.bytes(N));
        array
    }

    fn byte(&mut self) -> u8 {
        self.bytes(1)[0]
    }

    fn flag(&mut self) -> bool {
        self.byte() != 0
    }

    fn number(&mut self) -> usize {
        u32::from_le_bytes(self.array()) as usize
    }

    fn text(&mut self) -> String {
        let count = self.number();
        String::from_utf8_lossy(self.bytes(count)).into_owned()
    }

    fn list<T>(&mut self, mut item: impl FnMut(&mut Self) -> T) -> Vec<T> {
        (0..self.number()).map(|_| item(self)).collect()
    }
}
