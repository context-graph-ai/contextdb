use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

pub type TxId = u64;
pub type SnapshotId = u64;
pub type RowId = u64;
pub type NodeId = Uuid;
pub type EdgeType = String;
pub type TableName = String;
pub type ColName = String;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(String),
    Uuid(Uuid),
    Timestamp(i64),
    /// JSON values are serialized as strings for bincode compatibility.
    /// `serde_json::Value` uses `deserialize_any` which bincode does not support.
    Json(#[serde(with = "json_as_string")] serde_json::Value),
    Vector(Vec<f32>),
}

mod json_as_string {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(value: &serde_json::Value, ser: S) -> Result<S::Ok, S::Error> {
        let s = serde_json::to_string(value).map_err(serde::ser::Error::custom)?;
        s.serialize(ser)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<serde_json::Value, D::Error> {
        let s = String::deserialize(de)?;
        serde_json::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl Value {
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_uuid(&self) -> Option<&Uuid> {
        match self {
            Value::Uuid(u) => Some(u),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedRow {
    pub row_id: RowId,
    pub values: HashMap<ColName, Value>,
    pub created_tx: TxId,
    pub deleted_tx: Option<TxId>,
    pub lsn: u64,
}

impl VersionedRow {
    pub fn visible_at(&self, snapshot: SnapshotId) -> bool {
        self.created_tx <= snapshot
            && (self.deleted_tx.is_none() || self.deleted_tx.unwrap() > snapshot)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdjEntry {
    pub source: NodeId,
    pub target: NodeId,
    pub edge_type: EdgeType,
    pub properties: HashMap<String, Value>,
    pub created_tx: TxId,
    pub deleted_tx: Option<TxId>,
    pub lsn: u64,
}

impl AdjEntry {
    pub fn visible_at(&self, snapshot: SnapshotId) -> bool {
        self.created_tx <= snapshot
            && (self.deleted_tx.is_none() || self.deleted_tx.unwrap() > snapshot)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorEntry {
    pub row_id: RowId,
    pub vector: Vec<f32>,
    pub created_tx: TxId,
    pub deleted_tx: Option<TxId>,
    pub lsn: u64,
}

impl VectorEntry {
    pub fn visible_at(&self, snapshot: SnapshotId) -> bool {
        self.created_tx <= snapshot
            && (self.deleted_tx.is_none() || self.deleted_tx.unwrap() > snapshot)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraversalNode {
    pub id: NodeId,
    pub depth: u32,
    pub path: Vec<(NodeId, EdgeType)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraversalResult {
    pub nodes: Vec<TraversalNode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpsertResult {
    Inserted,
    Updated,
    NoOp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_conversion_helpers() {
        let text = Value::Text("abc".to_string());
        let int = Value::Int64(42);
        let id = Uuid::new_v4();
        let uuid = Value::Uuid(id);

        assert_eq!(text.as_text(), Some("abc"));
        assert_eq!(int.as_i64(), Some(42));
        assert_eq!(uuid.as_uuid(), Some(&id));
    }
}
