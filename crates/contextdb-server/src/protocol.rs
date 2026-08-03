use crate::error::SyncError;
use contextdb_core::{
    CompositeForeignKey, Incarnation, Lsn, RowId, SingleColumnForeignKey, Value, VectorIndexRef,
};
use contextdb_engine::sync_types::{
    ApplyResult, ChangeSet, Conflict, DdlChange, EdgeChange, NaturalKey, RowChange, VectorChange,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub const PROTOCOL_VERSION: u8 = 6;

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
    DependencyCompletePushRequest,
    PushResponse,
    #[default]
    PullRequest,
    PullResponse,
    DependencyCompletePullResponse,
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
    /// The pushing edge's per-life incarnation. The hub keys its per-edge
    /// received-up-to record by `(node_id, incarnation)`, so a rebuilt edge
    /// reusing its node id is recorded under a fresh key and never confirmed by
    /// the prior life's stale watermark.
    pub incarnation: Incarnation,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PushResponse {
    pub result: Option<WireApplyResult>,
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_error: Option<WirePushError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WirePushError {
    PurgeRequiresAuthoritativeHub { hub_node_id: String },
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
    /// The serving store's per-tenant incarnation, so a puller can bind its
    /// cursor to the specific store that issued it — a served page from a
    /// store other than the one the cursor addresses is discarded, never
    /// partially trusted (`SyncClient::pull`). Defaulted so decoding an older
    /// peer's reply is unaffected.
    #[serde(default)]
    pub source: Option<Incarnation>,
}

/// A pull page that carries one or more connected final-state units beside
/// ordinary source-LSN work. Only pages that need atomic dependency handling
/// use this envelope; the ordinary pull response bytes remain unchanged.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DependencyCompletePullResponse {
    pub ordinary: PullResponse,
    pub units: Vec<WireChangeSet>,
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
    /// Per-DDL schema-instance evidence.  This remains wire-only: callers of
    /// the public engine `DdlChange` surface do not get a second schema
    /// counter. It remains present in the positional wire layout even when
    /// empty, so later trailing fields retain their assigned slots.
    #[serde(default)]
    pub ddl_provenance: Vec<WireDdlProvenance>,
    /// Deliberate fleet-removal instructions are a distinct wire category.
    /// This slice carries and refuses edge-originated instructions; no apply
    /// path is enabled here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub purges: Vec<WirePurgeChange>,
}

/// The authenticated sender's immutable account of one DDL entry.  `table`
/// and `table_generation` are present only for DDL that changes or relies on
/// a table schema instance.  `ordinal` distinguishes several DDL entries at
/// one source LSN (including synthetic full snapshots).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireDdlProvenance {
    pub source_ddl_lsn: Lsn,
    pub ordinal: u32,
    #[serde(default)]
    pub table: Option<String>,
    #[serde(default)]
    pub table_generation: Option<u64>,
    #[serde(with = "serde_bytes")]
    pub digest: Vec<u8>,
}

/// Private wire-to-apply handoff.  The server mirror owns the same private
/// type, so the engine's public schema API never exposes transport identity.
#[derive(Debug, Clone)]
pub(crate) struct ReceivedDdlContext {
    pub tenant_id: contextdb_core::TenantId,
    pub source_node_id: String,
    pub source_incarnation: Incarnation,
    pub entries: Vec<ReceivedDdlEntry>,
}

#[derive(Debug, Clone)]
pub(crate) struct ReceivedDdlEntry {
    pub source_ddl_lsn: Lsn,
    pub ordinal: u32,
    pub table: Option<String>,
    pub table_generation: Option<u64>,
    pub digest: Vec<u8>,
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
    /// The row's ordering position — this row's sync provenance sidecar on
    /// whichever node is SENDING it, `None` when that node authored the row
    /// itself and never yet synced it anywhere (see
    /// `wire_row_arrivals`/`wire_changeset_with_arrivals`). Arbitration
    /// compares this, never `lsn` (which is the sender's own unrelated local
    /// clock): an incoming row with no arrival always wins, one carrying an
    /// arrival at or below the stored position is a stale echo, and one
    /// above it wins. Defaulted so decoding an older peer's row is
    /// unaffected — that peer is refused outright by the protocol version
    /// check before this ever matters.
    #[serde(default)]
    pub arrival: Option<Lsn>,
    /// Immutable creation provenance. Decode accepts an absent field only so
    /// older bytes can be parsed deterministically; current validation rejects it
    /// before any sync mutation.
    #[serde(default)]
    pub lineage: Option<WireRowLineage>,
}

/// Original creator identity, not the forwarding peer.  The tuple is the
/// existing fabric node id plus database incarnation and creator-local
/// mutation position; `lineage_root` is derived from precisely that tuple.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireRowLineage {
    pub author_node_id: String,
    pub author_database_incarnation: Incarnation,
    pub author_local_mutation_position: Lsn,
    pub table_generation: u64,
    pub lineage_root: String,
    /// Ed25519 signature by the immutable original creator over the complete
    /// lineage attestation, including tenant and the whole natural key.
    #[serde(default)]
    pub attestation: Vec<u8>,
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
    CreateTriggerIncludingSync {
        name: String,
        table: String,
        on_events: Vec<String>,
    },
}

/// The wire mirror of `NaturalKey`: the leading key column/value plus every
/// further key column in declared order. `rest` is a required field — msgpack
/// encodes this struct as a sequence, so a single-column identity encodes as a
/// three-element sequence carrying an EMPTY `rest`, which is the one current
/// shape. A multi-column `PRIMARY KEY (a, b, c)` carries `a` flat and `b, c` in
/// `rest`, so the whole identity crosses the wire and an arriving row is matched
/// on all its columns. The obsolete two-element encoding is rejected at decode.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireNaturalKey {
    pub column: String,
    pub value: Value,
    pub rest: Vec<(String, Value)>,
}

impl Default for WireNaturalKey {
    fn default() -> Self {
        Self {
            column: String::new(),
            value: Value::Null,
            rest: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WirePurgeChange {
    pub table: String,
    pub table_generation: u64,
    pub natural_key: WireNaturalKey,
    pub purged_lineage_roots: Vec<String>,
    pub purge_frontier: Lsn,
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
    #[serde(default)]
    pub table: Option<String>,
    #[serde(default)]
    pub mutation_kind: Option<String>,
    #[serde(default)]
    pub winning_author_node_id: Option<String>,
    #[serde(default)]
    pub hub_acceptance_position: Option<Lsn>,
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
            ddl_provenance: Vec::new(),
            purges: Vec::new(),
            rows: value.rows.into_iter().map(Into::into).collect(),
            edges: value.edges.into_iter().map(Into::into).collect(),
            vectors: value.vectors.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<WireChangeSet> for ChangeSet {
    type Error = SyncError;

    fn try_from(value: WireChangeSet) -> Result<Self, Self::Error> {
        if !value.purges.is_empty() {
            return Err(SyncError::Protocol(
                "PURGE instructions cannot be converted through ordinary row apply".to_string(),
            ));
        }
        if value.ddl_lsn.len() != value.ddl.len() {
            return Err(SyncError::Protocol(format!(
                "WireChangeSet ddl_lsn length {} does not match ddl length {}",
                value.ddl_lsn.len(),
                value.ddl.len()
            )));
        }
        validate_wire_ddl_provenance(&value)?;
        Ok(Self {
            ddl: value.ddl.into_iter().map(Into::into).collect(),
            ddl_lsn: value.ddl_lsn,
            rows: value.rows.into_iter().map(Into::into).collect(),
            edges: value.edges.into_iter().map(Into::into).collect(),
            vectors: value.vectors.into_iter().map(Into::into).collect(),
        })
    }
}

/// Canonical digest for one schema entry.  The source identity itself is
/// carried by the authenticated request/response context; this bytestring
/// binds the sender's LSN, ordinal, DDL spelling, and schema generation.
pub fn canonical_ddl_provenance_digest(
    ddl: &WireDdlChange,
    source_ddl_lsn: Lsn,
    ordinal: u32,
    table: Option<&str>,
    table_generation: Option<u64>,
) -> Result<Vec<u8>, SyncError> {
    let bytes = rmp_serde::to_vec(&(
        "contextdb.sync-ddl-provenance.v6",
        ddl,
        source_ddl_lsn.0,
        ordinal,
        table,
        table_generation,
    ))
    .map_err(|err| SyncError::Protocol(format!("cannot encode DDL provenance: {err}")))?;
    Ok(blake3::hash(&bytes).as_bytes().to_vec())
}

fn wire_ddl_affected_table(ddl: &WireDdlChange) -> Option<&str> {
    match ddl {
        WireDdlChange::CreateTable { name, .. }
        | WireDdlChange::DropTable { name }
        | WireDdlChange::AlterTable { name, .. } => Some(name),
        WireDdlChange::CreateIndex { table, .. } | WireDdlChange::DropIndex { table, .. } => {
            Some(table)
        }
        WireDdlChange::CreateTrigger { table, .. }
        | WireDdlChange::CreateTriggerIncludingSync { table, .. }
        | WireDdlChange::CreateEventType { table, .. }
        | WireDdlChange::CreateRoute { table, .. }
        | WireDdlChange::DropRoute { table, .. }
            if !table.is_empty() =>
        {
            Some(table)
        }
        WireDdlChange::DropTrigger { .. }
        | WireDdlChange::CreateSink { .. }
        | WireDdlChange::CreateTrigger { .. }
        | WireDdlChange::CreateTriggerIncludingSync { .. }
        | WireDdlChange::CreateEventType { .. }
        | WireDdlChange::CreateRoute { .. }
        | WireDdlChange::DropRoute { .. } => None,
    }
}

/// Reject malformed or pre-foundation DDL before conversion can discard the
/// wire evidence.  Rows retain their existing lineage path; any schema entry
/// requires this evidence even when the table is empty.
pub fn validate_wire_ddl_provenance(changeset: &WireChangeSet) -> Result<(), SyncError> {
    if changeset.ddl_lsn.len() != changeset.ddl.len() {
        return Err(SyncError::Protocol(format!(
            "WireChangeSet ddl_lsn length {} does not match ddl length {}",
            changeset.ddl_lsn.len(),
            changeset.ddl.len()
        )));
    }
    if changeset.ddl.is_empty() {
        if !changeset.ddl_provenance.is_empty() {
            return Err(SyncError::Protocol(
                "DDL provenance present without DDL entries".to_string(),
            ));
        }
        return Ok(());
    }
    if changeset.ddl_provenance.len() != changeset.ddl.len() {
        return Err(SyncError::Protocol(format!(
            "WireChangeSet ddl_provenance length {} does not match ddl length {}",
            changeset.ddl_provenance.len(),
            changeset.ddl.len()
        )));
    }
    let mut identities = HashSet::new();
    for (index, ((ddl, ddl_lsn), provenance)) in changeset
        .ddl
        .iter()
        .zip(changeset.ddl_lsn.iter())
        .zip(changeset.ddl_provenance.iter())
        .enumerate()
    {
        if provenance.source_ddl_lsn != *ddl_lsn {
            return Err(SyncError::Protocol(format!(
                "DDL provenance at index {index} does not match its source LSN"
            )));
        }
        if !identities.insert((provenance.source_ddl_lsn, provenance.ordinal)) {
            return Err(SyncError::Protocol(format!(
                "DDL provenance at index {index} repeats a source LSN/ordinal identity"
            )));
        }
        if provenance.digest.len() != blake3::OUT_LEN {
            return Err(SyncError::Protocol(format!(
                "DDL provenance at index {index} has an invalid digest length"
            )));
        }
        match (
            wire_ddl_affected_table(ddl),
            &provenance.table,
            provenance.table_generation,
        ) {
            (Some(expected), Some(table), Some(generation))
                if table == expected && generation != 0 => {}
            (None, None, None) => {}
            _ => {
                return Err(SyncError::Protocol(format!(
                    "DDL provenance at index {index} does not bind its affected table exactly"
                )));
            }
        }
        let expected = canonical_ddl_provenance_digest(
            ddl,
            provenance.source_ddl_lsn,
            provenance.ordinal,
            provenance.table.as_deref(),
            provenance.table_generation,
        )?;
        if provenance.digest != expected {
            return Err(SyncError::Protocol(format!(
                "DDL provenance at index {index} has a digest mismatch"
            )));
        }
    }
    Ok(())
}

/// Preserve validated schema identity across the wire-to-engine conversion.
/// The caller supplies transport-authenticated source identity; bytes in a
/// changeset never nominate their own schema authority.
pub(crate) fn received_ddl_context(
    changeset: &WireChangeSet,
    tenant_id: &contextdb_core::TenantId,
    source_node_id: &str,
    source_incarnation: Incarnation,
) -> Result<Option<ReceivedDdlContext>, SyncError> {
    validate_wire_ddl_provenance(changeset)?;
    if changeset.ddl.is_empty() {
        return Ok(None);
    }
    if source_node_id.is_empty() {
        return Err(SyncError::Protocol(
            "DDL provenance requires an authenticated source node".to_string(),
        ));
    }
    Ok(Some(ReceivedDdlContext {
        tenant_id: tenant_id.clone(),
        source_node_id: source_node_id.to_string(),
        source_incarnation,
        entries: changeset
            .ddl_provenance
            .iter()
            .map(|entry| ReceivedDdlEntry {
                source_ddl_lsn: entry.source_ddl_lsn,
                ordinal: entry.ordinal,
                table: entry.table.clone(),
                table_generation: entry.table_generation,
                digest: entry.digest.clone(),
            })
            .collect(),
    }))
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
            // The engine's `RowChange` does not carry an ordering position
            // yet, so every sender sends "none" here.
            arrival: None,
            lineage: None,
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
            DdlChange::CreateTriggerIncludingSync {
                name,
                table,
                on_events,
            } => Self::CreateTriggerIncludingSync {
                name,
                table,
                on_events,
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
            WireDdlChange::CreateTriggerIncludingSync {
                name,
                table,
                on_events,
            } => Self::CreateTriggerIncludingSync {
                name,
                table,
                on_events,
            },
        }
    }
}

impl From<NaturalKey> for WireNaturalKey {
    fn from(value: NaturalKey) -> Self {
        Self {
            column: value.column,
            value: value.value,
            rest: value.rest,
        }
    }
}

impl From<WireNaturalKey> for NaturalKey {
    fn from(value: WireNaturalKey) -> Self {
        Self {
            column: value.column,
            value: value.value,
            rest: value.rest,
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
        let resolution = match value.resolution {
            contextdb_engine::sync_types::ConflictPolicy::InsertIfNotExists => "keep_first",
            contextdb_engine::sync_types::ConflictPolicy::LatestWins => "keep_latest",
            contextdb_engine::sync_types::ConflictPolicy::ServerWins => "receiver_keeps",
            contextdb_engine::sync_types::ConflictPolicy::EdgeWins => "incoming_applies",
        };
        Self {
            natural_key: value.natural_key.into(),
            resolution: resolution.to_string(),
            reason: value.reason,
            table: value.table,
            mutation_kind: value.mutation_kind,
            winning_author_node_id: value.winning_author_node_id,
            hub_acceptance_position: value.hub_acceptance_position,
        }
    }
}

impl From<WireConflict> for Conflict {
    fn from(value: WireConflict) -> Self {
        let resolution = match value.resolution.as_str() {
            "keep_first" => contextdb_engine::sync_types::ConflictPolicy::InsertIfNotExists,
            "receiver_keeps" => contextdb_engine::sync_types::ConflictPolicy::ServerWins,
            "incoming_applies" => contextdb_engine::sync_types::ConflictPolicy::EdgeWins,
            _ => contextdb_engine::sync_types::ConflictPolicy::LatestWins,
        };
        Self {
            natural_key: value.natural_key.into(),
            resolution,
            reason: value.reason,
            table: value.table,
            mutation_kind: value.mutation_kind,
            winning_author_node_id: value.winning_author_node_id,
            hub_acceptance_position: value.hub_acceptance_position,
        }
    }
}

/// Request on the per-tenant sync-status subject. Tenant identity rides the
/// subject and the asking node id rides the authenticated connection; the body
/// carries the asking edge's per-life incarnation so the hub reads back the
/// record for THIS life of the edge — a rebuilt edge (fresh incarnation) is
/// answered `Lsn(0)`, never the prior life's stale-high watermark.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncStatusRequest {
    pub incarnation: Incarnation,
}

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

/// Per-row arrival positions read off a served/pushed wire changeset, keyed
/// by each row's own `.lsn` (rows sharing an `.lsn` share one accepted commit
/// and its fate, so the key is never ambiguous within one changeset). Built
/// BEFORE the wire rows are converted into engine `RowChange`s — a plain
/// conversion drops the field entirely, since the engine's own arbitration
/// never reads a raw wire row — so this map is the one channel `arrival`
/// crosses into `Database::apply_synced_changes`.
pub fn wire_row_arrivals(changeset: &WireChangeSet) -> HashMap<Lsn, Option<Lsn>> {
    changeset
        .rows
        .iter()
        .map(|row| (row.lsn, row.arrival))
        .collect()
}

/// Convert an outbound engine changeset to its wire form, additionally
/// stamping each row's `arrival` from a pre-computed per-row map (see
/// [`contextdb_engine::Database::changes_since_with_arrivals`]) instead of
/// leaving it absent. Keyed by each row's own `.lsn`, which survives every
/// filter/page/merge step between fetching the changeset and encoding it.
pub fn wire_changeset_with_arrivals(
    changes: ChangeSet,
    arrivals: &HashMap<Lsn, Option<Lsn>>,
) -> WireChangeSet {
    wire_changeset_with_arrivals_and_lineages(changes, arrivals, &HashMap::new())
}

/// Sender path: preserve the ordinary arrival stamps and additionally
/// carry the immutable original-creator provenance for every row.
pub fn wire_changeset_with_arrivals_and_lineages(
    changes: ChangeSet,
    arrivals: &HashMap<Lsn, Option<Lsn>>,
    lineages: &HashMap<(String, Vec<u8>, Lsn), WireRowLineage>,
) -> WireChangeSet {
    wire_changeset_with_arrivals_lineages_and_ddl_provenance(
        changes,
        arrivals,
        lineages,
        Vec::new(),
    )
}

/// Production sender path. Schema evidence is supplied by the database
/// because only its durable generation sidecars can describe compacted or
/// recreated tables.
pub fn wire_changeset_with_arrivals_lineages_and_ddl_provenance(
    changes: ChangeSet,
    arrivals: &HashMap<Lsn, Option<Lsn>>,
    lineages: &HashMap<(String, Vec<u8>, Lsn), WireRowLineage>,
    ddl_provenance: Vec<WireDdlProvenance>,
) -> WireChangeSet {
    let mut wire: WireChangeSet = changes.into();
    wire.ddl_provenance = ddl_provenance;
    for row in &mut wire.rows {
        if let Some(arrival) = arrivals.get(&row.lsn) {
            row.arrival = *arrival;
        }
        row.lineage = lineages
            .get(&(
                row.table.clone(),
                rmp_serde::to_vec(&NaturalKey::from(row.natural_key.clone()))
                    .expect("sync natural key serializes"),
                row.lsn,
            ))
            .cloned();
    }
    wire
}

/// Preserve the per-row key and source LSN with the provenance so a
/// same-transaction multi-row group cannot collide in a map keyed only by LSN.
pub fn wire_row_lineages(
    changeset: &WireChangeSet,
) -> Vec<(String, NaturalKey, Lsn, WireRowLineage)> {
    changeset
        .rows
        .iter()
        .filter_map(|row| {
            row.lineage.clone().map(|lineage| {
                (
                    row.table.clone(),
                    row.natural_key.clone().into(),
                    row.lsn,
                    lineage,
                )
            })
        })
        .collect()
}
