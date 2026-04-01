use crate::error::SyncError;
use contextdb_core::Value;
use contextdb_engine::sync_types::{
    ApplyResult, ChangeSet, Conflict, DdlChange, EdgeChange, NaturalKey, RowChange, VectorChange,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const PROTOCOL_VERSION: u8 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub version: u8,
    pub message_type: MessageType,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageType {
    PushRequest,
    PushResponse,
    PullRequest,
    PullResponse,
    Chunk,
    ChunkAck,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushRequest {
    pub changeset: WireChangeSet,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushResponse {
    pub result: Option<WireApplyResult>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequest {
    pub since_lsn: u64,
    pub max_entries: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullResponse {
    pub changeset: WireChangeSet,
    pub has_more: bool,
    pub cursor: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMessage {
    pub chunk_id: uuid::Uuid,
    pub sequence: u32,
    pub total_chunks: u32,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkAck {
    pub chunk_id: uuid::Uuid,
    pub total_chunks: u32,
    pub reply_inbox: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WireChangeSet {
    pub rows: Vec<WireRowChange>,
    pub edges: Vec<WireEdgeChange>,
    pub vectors: Vec<WireVectorChange>,
    pub ddl: Vec<WireDdlChange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireRowChange {
    pub table: String,
    pub natural_key: WireNaturalKey,
    pub values: HashMap<String, Value>,
    #[serde(default)]
    pub deleted: bool,
    pub lsn: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireEdgeChange {
    pub source: uuid::Uuid,
    pub target: uuid::Uuid,
    pub edge_type: String,
    pub properties: HashMap<String, Value>,
    pub lsn: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireVectorChange {
    pub row_id: u64,
    pub vector: Vec<f32>,
    pub lsn: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireDdlChange {
    CreateTable {
        name: String,
        columns: Vec<(String, String)>,
        constraints: Vec<String>,
    },
    DropTable {
        name: String,
    },
    AlterTable {
        name: String,
        columns: Vec<(String, String)>,
        constraints: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireNaturalKey {
    pub column: String,
    pub value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireApplyResult {
    pub applied_rows: usize,
    pub skipped_rows: usize,
    pub conflicts: Vec<WireConflict>,
    pub new_lsn: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireConflict {
    pub natural_key: WireNaturalKey,
    pub resolution: String,
    pub reason: Option<String>,
}

pub fn encode<T: Serialize>(msg_type: MessageType, msg: &T) -> Result<Vec<u8>, SyncError> {
    let payload = rmp_serde::to_vec(msg).map_err(|e| SyncError::Serde(e.to_string()))?;
    let envelope = Envelope {
        version: PROTOCOL_VERSION,
        message_type: msg_type,
        payload,
    };
    rmp_serde::to_vec(&envelope).map_err(|e| SyncError::Serde(e.to_string()))
}

pub fn decode(data: &[u8]) -> Result<Envelope, SyncError> {
    let envelope: Envelope =
        rmp_serde::from_slice(data).map_err(|e| SyncError::Serde(e.to_string()))?;
    if envelope.version > PROTOCOL_VERSION {
        return Err(SyncError::UnsupportedVersion(envelope.version));
    }
    Ok(envelope)
}

impl From<ChangeSet> for WireChangeSet {
    fn from(value: ChangeSet) -> Self {
        Self {
            rows: value.rows.into_iter().map(Into::into).collect(),
            edges: value.edges.into_iter().map(Into::into).collect(),
            vectors: value.vectors.into_iter().map(Into::into).collect(),
            ddl: value.ddl.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<WireChangeSet> for ChangeSet {
    fn from(value: WireChangeSet) -> Self {
        Self {
            rows: value.rows.into_iter().map(Into::into).collect(),
            edges: value.edges.into_iter().map(Into::into).collect(),
            vectors: value.vectors.into_iter().map(Into::into).collect(),
            ddl: value.ddl.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<RowChange> for WireRowChange {
    fn from(value: RowChange) -> Self {
        Self {
            table: value.table,
            natural_key: value.natural_key.into(),
            values: value.values,
            deleted: value.deleted,
            lsn: value.lsn,
        }
    }
}

impl From<WireRowChange> for RowChange {
    fn from(value: WireRowChange) -> Self {
        Self {
            table: value.table,
            natural_key: value.natural_key.into(),
            values: value.values,
            deleted: value.deleted,
            lsn: value.lsn,
        }
    }
}

impl From<EdgeChange> for WireEdgeChange {
    fn from(value: EdgeChange) -> Self {
        Self {
            source: value.source,
            target: value.target,
            edge_type: value.edge_type,
            properties: value.properties,
            lsn: value.lsn,
        }
    }
}

impl From<WireEdgeChange> for EdgeChange {
    fn from(value: WireEdgeChange) -> Self {
        Self {
            source: value.source,
            target: value.target,
            edge_type: value.edge_type,
            properties: value.properties,
            lsn: value.lsn,
        }
    }
}

impl From<VectorChange> for WireVectorChange {
    fn from(value: VectorChange) -> Self {
        Self {
            row_id: value.row_id,
            vector: value.vector,
            lsn: value.lsn,
        }
    }
}

impl From<WireVectorChange> for VectorChange {
    fn from(value: WireVectorChange) -> Self {
        Self {
            row_id: value.row_id,
            vector: value.vector,
            lsn: value.lsn,
        }
    }
}

impl From<DdlChange> for WireDdlChange {
    fn from(value: DdlChange) -> Self {
        match value {
            DdlChange::CreateTable {
                name,
                columns,
                constraints,
            } => Self::CreateTable {
                name,
                columns,
                constraints,
            },
            DdlChange::DropTable { name } => Self::DropTable { name },
            DdlChange::AlterTable {
                name,
                columns,
                constraints,
            } => Self::AlterTable {
                name,
                columns,
                constraints,
            },
        }
    }
}

impl From<WireDdlChange> for DdlChange {
    fn from(value: WireDdlChange) -> Self {
        match value {
            WireDdlChange::CreateTable {
                name,
                columns,
                constraints,
            } => Self::CreateTable {
                name,
                columns,
                constraints,
            },
            WireDdlChange::DropTable { name } => Self::DropTable { name },
            WireDdlChange::AlterTable {
                name,
                columns,
                constraints,
            } => Self::AlterTable {
                name,
                columns,
                constraints,
            },
        }
    }
}

impl From<NaturalKey> for WireNaturalKey {
    fn from(value: NaturalKey) -> Self {
        Self {
            column: value.column,
            value: value.value,
        }
    }
}

impl From<WireNaturalKey> for NaturalKey {
    fn from(value: WireNaturalKey) -> Self {
        Self {
            column: value.column,
            value: value.value,
        }
    }
}

impl From<ApplyResult> for WireApplyResult {
    fn from(value: ApplyResult) -> Self {
        Self {
            applied_rows: value.applied_rows,
            skipped_rows: value.skipped_rows,
            conflicts: value.conflicts.into_iter().map(Into::into).collect(),
            new_lsn: value.new_lsn,
        }
    }
}

impl From<WireApplyResult> for ApplyResult {
    fn from(value: WireApplyResult) -> Self {
        Self {
            applied_rows: value.applied_rows,
            skipped_rows: value.skipped_rows,
            conflicts: value.conflicts.into_iter().map(Into::into).collect(),
            new_lsn: value.new_lsn,
        }
    }
}

impl From<Conflict> for WireConflict {
    fn from(value: Conflict) -> Self {
        Self {
            natural_key: value.natural_key.into(),
            resolution: format!("{:?}", value.resolution),
            reason: value.reason,
        }
    }
}

impl From<WireConflict> for Conflict {
    fn from(value: WireConflict) -> Self {
        let resolution = match value.resolution.as_str() {
            "InsertIfNotExists" => contextdb_engine::sync_types::ConflictPolicy::InsertIfNotExists,
            "ServerWins" => contextdb_engine::sync_types::ConflictPolicy::ServerWins,
            "EdgeWins" => contextdb_engine::sync_types::ConflictPolicy::EdgeWins,
            _ => contextdb_engine::sync_types::ConflictPolicy::LatestWins,
        };
        Self {
            natural_key: value.natural_key.into(),
            resolution,
            reason: value.reason,
        }
    }
}
