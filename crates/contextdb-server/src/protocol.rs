use crate::error::SyncError;
use contextdb_core::{
    CompositeForeignKey, Lsn, RowId, SingleColumnForeignKey, Value, VectorIndexRef,
};
use contextdb_engine::sync_types::{
    ApplyResult, ChangeSet, Conflict, DdlChange, EdgeChange, NaturalKey, RowChange, VectorChange,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const PROTOCOL_VERSION: u8 = 4;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub version: u8,
    pub message_type: MessageType,
    pub payload: Vec<u8>,
}

impl Envelope {
    /// Constructs an Envelope pre-populated for a pull request with the current
    /// protocol version and empty payload.
    pub fn default_pull_request() -> Self {
        Self {
            version: PROTOCOL_VERSION,
            message_type: MessageType::PullRequest,
            payload: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub enum MessageType {
    PushRequest,
    PushResponse,
    #[default]
    PullRequest,
    PullResponse,
    Chunk,
    ChunkAck,
    /// The sync-status probe (dedicated per-tenant status subject only).
    /// New variants encode as their name string; existing variants' bytes are
    /// untouched, and old peers never see these (old servers do not subscribe
    /// to the status subject; old clients never send the request).
    StatusRequest,
    StatusResponse,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PushRequest {
    pub changeset: WireChangeSet,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PushResponse {
    pub result: Option<WireApplyResult>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PullRequest {
    pub since_lsn: Lsn,
    pub max_entries: Option<u32>,
}

impl<'de> serde::Deserialize<'de> for PullRequest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{SeqAccess, Visitor};

        struct PullRequestVisitor;

        impl<'de> Visitor<'de> for PullRequestVisitor {
            type Value = PullRequest;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("PullRequest with 1 or 2 elements")
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let since_lsn: Lsn = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let max_entries: Option<u32> = seq.next_element()?.unwrap_or(None);
                Ok(PullRequest {
                    since_lsn,
                    max_entries,
                })
            }
        }

        deserializer.deserialize_tuple(2, PullRequestVisitor)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PullResponse {
    pub changeset: WireChangeSet,
    pub has_more: bool,
    pub cursor: Option<Lsn>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkMessage {
    pub chunk_id: uuid::Uuid,
    pub sequence: u32,
    pub total_chunks: u32,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkAck {
    pub chunk_id: uuid::Uuid,
    pub total_chunks: u32,
    pub reply_inbox: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WireChangeSet {
    pub ddl: Vec<WireDdlChange>,
    pub ddl_lsn: Vec<Lsn>,
    pub rows: Vec<WireRowChange>,
    pub edges: Vec<WireEdgeChange>,
    pub vectors: Vec<WireVectorChange>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WireRowChange {
    pub table: String,
    pub natural_key: WireNaturalKey,
    pub values: HashMap<String, Value>,
    #[serde(default)]
    pub deleted: bool,
    pub lsn: Lsn,
    /// The row's birth time on the node that wrote it, so retention judges a
    /// replicated row by the age it actually has. Defaulted, so a peer built
    /// before this field decodes as "no stamp" and the receiver stamps its own.
    #[serde(default)]
    pub created_at: Option<contextdb_core::Wallclock>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WireEdgeChange {
    pub source: uuid::Uuid,
    pub target: uuid::Uuid,
    pub edge_type: String,
    pub properties: HashMap<String, Value>,
    pub lsn: Lsn,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WireVectorChange {
    pub index: VectorIndexRef,
    pub row_id: RowId,
    pub vector: Vec<f32>,
    pub lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireDdlChange {
    CreateTable {
        name: String,
        columns: Vec<(String, String)>,
        constraints: Vec<String>,
        #[serde(default)]
        foreign_keys: Vec<SingleColumnForeignKey>,
        #[serde(default)]
        composite_foreign_keys: Vec<CompositeForeignKey>,
        #[serde(default)]
        composite_unique: Vec<Vec<String>>,
    },
    DropTable {
        name: String,
    },
    AlterTable {
        name: String,
        columns: Vec<(String, String)>,
        constraints: Vec<String>,
        #[serde(default)]
        foreign_keys: Vec<SingleColumnForeignKey>,
        #[serde(default)]
        composite_foreign_keys: Vec<CompositeForeignKey>,
        #[serde(default)]
        composite_unique: Vec<Vec<String>>,
    },
    // Direction is encoded as a string ("ASC" / "DESC") on the wire so the
    // server does not depend on contextdb-core's SortDirection type shape.
    CreateIndex {
        table: String,
        name: String,
        columns: Vec<(String, String)>,
    },
    DropIndex {
        table: String,
        name: String,
    },
    CreateTrigger {
        name: String,
        table: String,
        on_events: Vec<String>,
    },
    DropTrigger {
        name: String,
    },
    CreateEventType {
        name: String,
        trigger: String,
        table: String,
    },
    CreateSink {
        name: String,
        sink_type: String,
        url: Option<String>,
    },
    CreateRoute {
        name: String,
        event_type: String,
        sink: String,
        #[serde(default)]
        table: String,
        where_in: Option<(String, Vec<String>)>,
    },
    DropRoute {
        name: String,
        #[serde(default)]
        table: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireNaturalKey {
    pub column: String,
    pub value: Value,
}

impl Default for WireNaturalKey {
    fn default() -> Self {
        Self {
            column: String::new(),
            value: Value::Null,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WireApplyResult {
    pub applied_rows: usize,
    pub skipped_rows: usize,
    pub conflicts: Vec<WireConflict>,
    pub new_lsn: Lsn,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
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
    if envelope.version != PROTOCOL_VERSION {
        return Err(SyncError::ProtocolVersionMismatch {
            received: envelope.version,
            supported: PROTOCOL_VERSION,
        });
    }
    Ok(envelope)
}

impl From<ChangeSet> for WireChangeSet {
    fn from(value: ChangeSet) -> Self {
        Self {
            ddl: value.ddl.into_iter().map(Into::into).collect(),
            ddl_lsn: value.ddl_lsn,
            rows: value.rows.into_iter().map(Into::into).collect(),
            edges: value.edges.into_iter().map(Into::into).collect(),
            vectors: value.vectors.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<WireChangeSet> for ChangeSet {
    type Error = SyncError;

    fn try_from(value: WireChangeSet) -> Result<Self, Self::Error> {
        if value.ddl_lsn.len() != value.ddl.len() {
            return Err(SyncError::Protocol(format!(
                "WireChangeSet ddl_lsn length {} does not match ddl length {}",
                value.ddl_lsn.len(),
                value.ddl.len()
            )));
        }
        Ok(Self {
            ddl: value.ddl.into_iter().map(Into::into).collect(),
            ddl_lsn: value.ddl_lsn,
            rows: value.rows.into_iter().map(Into::into).collect(),
            edges: value.edges.into_iter().map(Into::into).collect(),
            vectors: value.vectors.into_iter().map(Into::into).collect(),
        })
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
            created_at: value.created_at,
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
            created_at: value.created_at,
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
            index: value.index,
            row_id: value.row_id,
            vector: value.vector,
            lsn: value.lsn,
        }
    }
}

impl From<WireVectorChange> for VectorChange {
    fn from(value: WireVectorChange) -> Self {
        Self {
            index: value.index,
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
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } => Self::CreateTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            },
            DdlChange::DropTable { name } => Self::DropTable { name },
            DdlChange::AlterTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } => Self::AlterTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            },
            DdlChange::CreateIndex {
                table,
                name,
                columns,
            } => {
                let wire_cols = columns
                    .into_iter()
                    .map(|(c, dir)| {
                        let dir_str = match dir {
                            contextdb_core::SortDirection::Asc => "ASC".to_string(),
                            contextdb_core::SortDirection::Desc => "DESC".to_string(),
                        };
                        (c, dir_str)
                    })
                    .collect();
                Self::CreateIndex {
                    table,
                    name,
                    columns: wire_cols,
                }
            }
            DdlChange::DropIndex { table, name } => Self::DropIndex { table, name },
            DdlChange::CreateTrigger {
                name,
                table,
                on_events,
            } => Self::CreateTrigger {
                name,
                table,
                on_events,
            },
            DdlChange::DropTrigger { name } => Self::DropTrigger { name },
            DdlChange::CreateEventType {
                name,
                trigger,
                table,
            } => Self::CreateEventType {
                name,
                trigger,
                table,
            },
            DdlChange::CreateSink {
                name,
                sink_type,
                url,
            } => Self::CreateSink {
                name,
                sink_type,
                url,
            },
            DdlChange::CreateRoute {
                name,
                event_type,
                sink,
                table,
                where_in,
            } => Self::CreateRoute {
                name,
                event_type,
                sink,
                table,
                where_in,
            },
            DdlChange::DropRoute { name, table } => Self::DropRoute { name, table },
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
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } => Self::CreateTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            },
            WireDdlChange::DropTable { name } => Self::DropTable { name },
            WireDdlChange::AlterTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } => Self::AlterTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            },
            WireDdlChange::CreateIndex {
                table,
                name,
                columns,
            } => {
                let engine_cols = columns
                    .into_iter()
                    .map(|(c, dir_str)| {
                        let dir = if dir_str.eq_ignore_ascii_case("DESC") {
                            contextdb_core::SortDirection::Desc
                        } else {
                            contextdb_core::SortDirection::Asc
                        };
                        (c, dir)
                    })
                    .collect();
                Self::CreateIndex {
                    table,
                    name,
                    columns: engine_cols,
                }
            }
            WireDdlChange::DropIndex { table, name } => Self::DropIndex { table, name },
            WireDdlChange::CreateTrigger {
                name,
                table,
                on_events,
            } => Self::CreateTrigger {
                name,
                table,
                on_events,
            },
            WireDdlChange::DropTrigger { name } => Self::DropTrigger { name },
            WireDdlChange::CreateEventType {
                name,
                trigger,
                table,
            } => Self::CreateEventType {
                name,
                trigger,
                table,
            },
            WireDdlChange::CreateSink {
                name,
                sink_type,
                url,
            } => Self::CreateSink {
                name,
                sink_type,
                url,
            },
            WireDdlChange::CreateRoute {
                name,
                event_type,
                sink,
                table,
                where_in,
            } => Self::CreateRoute {
                name,
                event_type,
                sink,
                table,
                where_in,
            },
            WireDdlChange::DropRoute { name, table } => Self::DropRoute { name, table },
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

/// Request on the per-tenant sync-status subject. Tenant identity rides
/// the subject; the body is intentionally empty and growth-tolerant.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncStatusRequest {}

/// Reply on the per-tenant sync-status subject. This struct is NEW wire
/// surface — old peers never see it (old servers are not subscribed; old
/// clients never send the request). Both fields are `#[serde(default)]`
/// Options from day one: absent = unknown = no regression detection = client
/// behaves exactly as today.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SyncStatusResponse {
    /// The server's persisted per-tenant record of the highest edge-LSN it has
    /// applied (contract item 3 — rides `export_snapshot` artifacts).
    #[serde(default)]
    pub applied_push_watermark: Option<Lsn>,
    /// The server's current LSN clock (contract item 4, pull-side resume).
    #[serde(default)]
    pub server_current_lsn: Option<Lsn>,
}

/// The payload bytes a set of row changes carries: each row's own values,
/// serialized. Table names, natural keys, the protocol envelope and transport
/// framing are all excluded — this is what the data itself weighs, and it is
/// what a transfer receipt counts.
pub(crate) fn row_payload_bytes(rows: &[RowChange]) -> u64 {
    rows.iter()
        .map(|row| {
            rmp_serde::to_vec(&row.values)
                .map(|bytes| bytes.len())
                .unwrap_or(0) as u64
        })
        .sum()
}
