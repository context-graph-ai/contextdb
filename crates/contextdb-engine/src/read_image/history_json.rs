//! Borrowed history JSON preserves the object order of to_value's sorted
//! maps, including nested SQL JSON strings, without an intermediate DOM.
use crate::sync_types::{DdlChange, EdgeChange, NaturalKey, RowChange, VectorChange};
use contextdb_core::{CompositeForeignKey, SingleColumnForeignKey, Value, VectorIndexRef};
use serde::ser::{SerializeMap, SerializeSeq, SerializeStructVariant};
use serde::{Serialize, Serializer};

pub(super) trait HistoryJson {
    fn serialize_canonical<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error>;
}
pub(super) struct Canonical<'a, T: ?Sized>(pub &'a T);
impl<T: HistoryJson + ?Sized> Serialize for Canonical<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize_canonical(serializer)
    }
}
struct SqlValue<'a>(&'a Value);
impl Serialize for SqlValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        struct JsonText<'a>(&'a serde_json::Value);
        impl Serialize for JsonText<'_> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self.0)
            }
        }
        match self.0 {
            Value::Json(value) => {
                serializer.serialize_newtype_variant("Value", 7, "Json", &JsonText(value))
            }
            Value::Vector(value) => {
                serializer.serialize_newtype_variant("Value", 8, "Vector", &FloatVector(value))
            }
            value => value.serialize(serializer),
        }
    }
}
struct FloatVector<'a>(&'a [f32]);
impl Serialize for FloatVector<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        // to_value first widens f32 to the JSON Number's f64 representation.
        for value in self.0 {
            sequence.serialize_element(&f64::from(*value))?;
        }
        sequence.end()
    }
}
struct Values<'a>(&'a std::collections::HashMap<String, Value>);
impl Serialize for Values<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        let mut after: Option<&str> = None;
        while let Some((key, value)) = self
            .0
            .iter()
            .filter(|(key, _)| after.is_none_or(|prior| key.as_str() > prior))
            .min_by(|(a, _), (b, _)| a.cmp(b))
        {
            map.serialize_entry(key, &SqlValue(value))?;
            after = Some(key);
        }
        map.end()
    }
}
struct KeyTail<'a>(&'a [(String, Value)]);
impl Serialize for KeyTail<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for (key, value) in self.0 {
            sequence.serialize_element(&(key, SqlValue(value)))?;
        }
        sequence.end()
    }
}
impl HistoryJson for NaturalKey {
    fn serialize_canonical<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Key<'a> {
            column: &'a str,
            rest: KeyTail<'a>,
            value: SqlValue<'a>,
        }
        Key {
            column: &self.column,
            rest: KeyTail(&self.rest),
            value: SqlValue(&self.value),
        }
        .serialize(serializer)
    }
}
impl HistoryJson for RowChange {
    fn serialize_canonical<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Row<'a> {
            created_at: &'a Option<contextdb_core::Wallclock>,
            deleted: bool,
            lsn: contextdb_core::Lsn,
            natural_key: Canonical<'a, NaturalKey>,
            table: &'a str,
            values: Values<'a>,
        }
        Row {
            created_at: &self.created_at,
            deleted: self.deleted,
            lsn: self.lsn,
            natural_key: Canonical(&self.natural_key),
            table: &self.table,
            values: Values(&self.values),
        }
        .serialize(serializer)
    }
}
impl HistoryJson for EdgeChange {
    fn serialize_canonical<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Edge<'a> {
            edge_type: &'a str,
            lsn: contextdb_core::Lsn,
            properties: Values<'a>,
            source: &'a uuid::Uuid,
            target: &'a uuid::Uuid,
        }
        Edge {
            edge_type: &self.edge_type,
            lsn: self.lsn,
            properties: Values(&self.properties),
            source: &self.source,
            target: &self.target,
        }
        .serialize(serializer)
    }
}
impl HistoryJson for VectorIndexRef {
    fn serialize_canonical<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Index<'a> {
            column: &'a str,
            table: &'a str,
        }
        Index {
            column: &self.column,
            table: &self.table,
        }
        .serialize(serializer)
    }
}
impl HistoryJson for VectorChange {
    fn serialize_canonical<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Vector<'a> {
            index: Canonical<'a, VectorIndexRef>,
            lsn: contextdb_core::Lsn,
            row_id: contextdb_core::RowId,
            vector: FloatVector<'a>,
        }
        Vector {
            index: Canonical(&self.index),
            lsn: self.lsn,
            row_id: self.row_id,
            vector: FloatVector(&self.vector),
        }
        .serialize(serializer)
    }
}
struct ForeignKeys<'a, T>(&'a [T]);
impl<T: HistoryJson> Serialize for ForeignKeys<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for value in self.0 {
            sequence.serialize_element(&Canonical(value))?;
        }
        sequence.end()
    }
}
impl HistoryJson for SingleColumnForeignKey {
    fn serialize_canonical<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Key<'a> {
            child_column: &'a str,
            #[serde(skip_serializing_if = "std::ops::Not::not")]
            follows_parent_purge: bool,
            parent_column: &'a str,
            parent_table: &'a str,
        }
        Key {
            child_column: &self.child_column,
            follows_parent_purge: self.follows_parent_purge,
            parent_column: &self.parent_column,
            parent_table: &self.parent_table,
        }
        .serialize(serializer)
    }
}
impl HistoryJson for CompositeForeignKey {
    fn serialize_canonical<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Key<'a> {
            child_columns: &'a [String],
            #[serde(skip_serializing_if = "std::ops::Not::not")]
            follows_parent_purge: bool,
            parent_columns: &'a [String],
            parent_table: &'a str,
        }
        Key {
            child_columns: &self.child_columns,
            follows_parent_purge: self.follows_parent_purge,
            parent_columns: &self.parent_columns,
            parent_table: &self.parent_table,
        }
        .serialize(serializer)
    }
}
impl HistoryJson for DdlChange {
    fn serialize_canonical<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            DdlChange::CreateTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } => {
                let mut state =
                    serializer.serialize_struct_variant("DdlChange", 0, "CreateTable", 6)?;
                state.serialize_field("columns", columns)?;
                state.serialize_field(
                    "composite_foreign_keys",
                    &ForeignKeys(composite_foreign_keys),
                )?;
                state.serialize_field("composite_unique", composite_unique)?;
                state.serialize_field("constraints", constraints)?;
                state.serialize_field("foreign_keys", &ForeignKeys(foreign_keys))?;
                state.serialize_field("name", name)?;
                state.end()
            }
            DdlChange::DropTable { name } => {
                let mut state =
                    serializer.serialize_struct_variant("DdlChange", 1, "DropTable", 1)?;
                state.serialize_field("name", name)?;
                state.end()
            }
            DdlChange::AlterTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } => {
                let mut state =
                    serializer.serialize_struct_variant("DdlChange", 2, "AlterTable", 6)?;
                state.serialize_field("columns", columns)?;
                state.serialize_field(
                    "composite_foreign_keys",
                    &ForeignKeys(composite_foreign_keys),
                )?;
                state.serialize_field("composite_unique", composite_unique)?;
                state.serialize_field("constraints", constraints)?;
                state.serialize_field("foreign_keys", &ForeignKeys(foreign_keys))?;
                state.serialize_field("name", name)?;
                state.end()
            }
            DdlChange::CreateIndex {
                table,
                name,
                columns,
            } => {
                let mut state =
                    serializer.serialize_struct_variant("DdlChange", 3, "CreateIndex", 3)?;
                state.serialize_field("columns", columns)?;
                state.serialize_field("name", name)?;
                state.serialize_field("table", table)?;
                state.end()
            }
            DdlChange::DropIndex { table, name } => {
                let mut state =
                    serializer.serialize_struct_variant("DdlChange", 4, "DropIndex", 2)?;
                state.serialize_field("name", name)?;
                state.serialize_field("table", table)?;
                state.end()
            }
            DdlChange::CreateTrigger {
                name,
                table,
                on_events,
            } => {
                let mut state =
                    serializer.serialize_struct_variant("DdlChange", 5, "CreateTrigger", 3)?;
                state.serialize_field("name", name)?;
                state.serialize_field("on_events", on_events)?;
                state.serialize_field("table", table)?;
                state.end()
            }
            DdlChange::DropTrigger { name } => {
                let mut state =
                    serializer.serialize_struct_variant("DdlChange", 6, "DropTrigger", 1)?;
                state.serialize_field("name", name)?;
                state.end()
            }
            DdlChange::CreateEventType {
                name,
                trigger,
                table,
            } => {
                let mut state =
                    serializer.serialize_struct_variant("DdlChange", 7, "CreateEventType", 3)?;
                state.serialize_field("name", name)?;
                state.serialize_field("table", table)?;
                state.serialize_field("trigger", trigger)?;
                state.end()
            }
            DdlChange::CreateSink {
                name,
                sink_type,
                url,
            } => {
                let mut state =
                    serializer.serialize_struct_variant("DdlChange", 8, "CreateSink", 3)?;
                state.serialize_field("name", name)?;
                state.serialize_field("sink_type", sink_type)?;
                state.serialize_field("url", url)?;
                state.end()
            }
            DdlChange::CreateRoute {
                name,
                event_type,
                sink,
                table,
                where_in,
            } => {
                let mut state =
                    serializer.serialize_struct_variant("DdlChange", 9, "CreateRoute", 5)?;
                state.serialize_field("event_type", event_type)?;
                state.serialize_field("name", name)?;
                state.serialize_field("sink", sink)?;
                state.serialize_field("table", table)?;
                state.serialize_field("where_in", where_in)?;
                state.end()
            }
            DdlChange::DropRoute { name, table } => {
                let mut state =
                    serializer.serialize_struct_variant("DdlChange", 10, "DropRoute", 2)?;
                state.serialize_field("name", name)?;
                state.serialize_field("table", table)?;
                state.end()
            }
            DdlChange::CreateTriggerIncludingSync {
                name,
                table,
                on_events,
            } => {
                let mut state = serializer.serialize_struct_variant(
                    "DdlChange",
                    11,
                    "CreateTriggerIncludingSync",
                    3,
                )?;
                state.serialize_field("name", name)?;
                state.serialize_field("on_events", on_events)?;
                state.serialize_field("table", table)?;
                state.end()
            }
        }
    }
}
