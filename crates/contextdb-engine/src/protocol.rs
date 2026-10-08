#![allow(clippy::needless_update)] // Existing compatibility and test seam shape.
use crate::error::SyncError;
use contextdb_core::{
    CompositeForeignKey, Incarnation, Lsn, RowId, SingleColumnForeignKey, Value, VectorIndexRef,
};
use contextdb_engine::ApplicationTablePolicyExpectation;
use contextdb_engine::sync_types::{
    ApplyResult, ChangeSet, Conflict, DdlChange, EdgeChange, NaturalKey, RefusalCause, RowChange,
    VectorChange,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// The schema-vocabulary protocol this build emits.
pub const PROTOCOL_VERSION: u8 = 7;
/// The first release has no released predecessor. The support window grows
/// only as protocol versions are released, never from development revisions.
pub const OLDEST_SUPPORTED_PROTOCOL_VERSION: u8 = 7;

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
    // Manifested units use the existing push request and response.
    BindApplicationTablePolicyRequest,
    BindApplicationTablePolicyResponse,
    FetchDeliveryOutcomesRequest,
    FetchDeliveryOutcomesResponse,
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

/// A whole signed schema unit refused by the receiving hub's declared policy.
/// This is an outcome, never an acceptance receipt or an applied watermark.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefusedSchemaUnitOutcome {
    pub tenant_id: contextdb_core::TenantId,
    pub pushing_node_id: String,
    pub pushing_incarnation: Incarnation,
    pub author_node_id: String,
    pub author_incarnation: Incarnation,
    pub source_ddl_lsn: Lsn,
    pub unit_digest: [u8; 32],
    pub hub_node_id: String,
    pub hub_incarnation: Incarnation,
    pub table: String,
    pub reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct PushResponse {
    pub result: Option<WireApplyResult>,
    pub error: Option<String>,
    // Optional slots precede the ordinary outcome lane.
    #[serde(default)]
    pub application_error: Option<WirePushError>,
    #[serde(default)]
    pub outcomes: Vec<WireDeliveryOutcome>,
    #[serde(default)]
    pub hub_incarnation: Option<Incarnation>,
    #[serde(default)]
    pub refused_schema_units: Vec<RefusedSchemaUnitOutcome>,
}

// Populate ordinary response lanes without shifting optional slots.
impl Serialize for PushResponse {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let has_refusals = !self.refused_schema_units.is_empty();
        let has_hub = self.hub_incarnation.is_some() || has_refusals;
        let has_outcomes = !self.outcomes.is_empty() || has_hub;
        let has_application_error = self.application_error.is_some() || has_outcomes;
        let mut value = serializer.serialize_struct(
            "PushResponse",
            2 + usize::from(has_application_error)
                + usize::from(has_outcomes)
                + usize::from(has_hub)
                + usize::from(has_refusals),
        )?;
        value.serialize_field("result", &self.result)?;
        value.serialize_field("error", &self.error)?;
        if has_application_error {
            value.serialize_field("application_error", &self.application_error)?;
        }
        if has_outcomes {
            value.serialize_field("outcomes", &self.outcomes)?;
        }
        if has_hub {
            value.serialize_field("hub_incarnation", &self.hub_incarnation)?;
        }
        if has_refusals {
            value.serialize_field("refused_schema_units", &self.refused_schema_units)?;
        }
        value.end()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WirePushError {
    PurgeRequiresAuthoritativeHub {
        hub_node_id: String,
    },
    /// A push re-offered a row this hub already terminated by an accepted
    /// delete -- the store already agrees, this is benign convergence, not
    /// a definitive failure. Additive variant (mirrors how
    /// `application_error` itself was added to `PushResponse` without a
    /// `PROTOCOL_VERSION` bump): an old server never sends it, so an old
    /// client never has to decode it.
    ReplaysAcceptedDelete {
        table: String,
        key: Vec<(String, Value)>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequest {
    pub since_lsn: Lsn,
    pub max_entries: Option<u32>,
    /// Continuation or acknowledgement for a table image the sender withheld
    /// while this peer lacked its schema capability. The first capable request
    /// omits this field; the sender finds its durable per-peer holdback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_recovery: Option<SchemaRecoveryRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaRecoveryRequest {
    /// Ask for the next held-table page after the last page durably applied by
    /// this receiver. `target_lsn` binds the continuation to the sender image
    /// announced on the first page.
    Continue { target_lsn: Lsn, after_lsn: Lsn },
    /// The receiver sends this only after the final held-table page committed.
    /// Once this request arrives, retaining the holdback is no longer needed;
    /// a lost acknowledgement can safely retry the same request.
    Acknowledge { target_lsn: Lsn },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRecoveryPage {
    /// Stable source frontier covered by this recovery image. Work committed
    /// after it remains ordinary sync work and follows after acknowledgement.
    pub target_lsn: Lsn,
    /// Private cursor for the next held-table page. It never replaces the
    /// receiver's public pull cursor.
    pub next_lsn: Lsn,
    /// True only on the final data page. The receiver still sends an explicit
    /// acknowledgement request after durably applying that page.
    pub complete: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PullResponse {
    pub changeset: WireChangeSet,
    pub has_more: bool,
    pub cursor: Option<Lsn>,
    /// The serving store's per-tenant incarnation, so a puller can bind its
    /// cursor to the specific store that issued it — a served page from a
    /// store other than the one the cursor addresses is discarded, never
    /// partially trusted (`SyncClient::pull`). An absent source remains unknown;
    /// it is never replaced by a fabricated store identity.
    #[serde(default)]
    pub source: Option<Incarnation>,
    /// Progress for a bounded held-table recovery after a capability upgrade.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_recovery: Option<SchemaRecoveryPage>,
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

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
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
    // Purges keep their slot when the following manifests lane is populated.
    #[serde(default)]
    pub purges: Vec<WirePurgeChange>,
    #[serde(default)]
    pub manifests: Vec<WireDeliveryManifest>,
}

// Only populated new lanes extend ordinary changesets. An empty purge slot
// still precedes manifests, so positional encoding cannot mistake manifests for purges.
impl Serialize for WireChangeSet {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let has_purges = !self.purges.is_empty() || !self.manifests.is_empty();
        let mut value = serializer.serialize_struct(
            "WireChangeSet",
            6 + usize::from(has_purges) + usize::from(!self.manifests.is_empty()),
        )?;
        value.serialize_field("ddl", &self.ddl)?;
        value.serialize_field("ddl_lsn", &self.ddl_lsn)?;
        value.serialize_field("rows", &self.rows)?;
        value.serialize_field("edges", &self.edges)?;
        value.serialize_field("vectors", &self.vectors)?;
        value.serialize_field("ddl_provenance", &self.ddl_provenance)?;
        if has_purges {
            value.serialize_field("purges", &self.purges)?;
        }
        if !self.manifests.is_empty() {
            value.serialize_field("manifests", &self.manifests)?;
        }
        value.end()
    }
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
    /// The schema author's own signed identity for this record. Authored DDL
    /// carries it across every hop unchanged. A declaration received through
    /// the unattested apply door has no author block and sets `unattested`.
    #[serde(default)]
    pub author: Option<WireDdlAuthor>,
    /// The sender received this declaration through the unattested apply
    /// door. It must preserve that trust class rather than claiming authorship.
    #[serde(default, skip_serializing_if = "is_false")]
    pub unattested: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// The author's identity for one schema record: who wrote it, in which life
/// of their database, and the source LSN, ordinal, table generation and
/// canonical digest the author attested when it first sent the record. A
/// forwarding peer carries this unchanged, so every later holder names the
/// author, never the peer that happened to deliver it.
///
/// `unit_statements` is how many statements the author's source transaction
/// (its node, incarnation and source LSN) holds. With the signed ordinal it
/// fixes the author's complete unit and its order, so a peer that forwards
/// the unit cannot drop, reorder or split its statements without every later
/// receiver seeing it.
///
/// `attestation` is the author's own ed25519 signature over exactly those
/// fields plus the statement itself, so a later holder checks authorship
/// against the author's public key rather than trusting whichever peer
/// delivered the record. It is the same signature shape the custody lane's
/// schema dependency carries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireDdlAuthor {
    pub node_id: String,
    pub incarnation: Incarnation,
    pub source_ddl_lsn: Lsn,
    pub ordinal: u32,
    #[serde(default)]
    pub table: Option<String>,
    #[serde(default)]
    pub table_generation: Option<u64>,
    #[serde(with = "serde_bytes")]
    pub digest: Vec<u8>,
    #[serde(default, with = "serde_bytes")]
    pub attestation: Vec<u8>,
    #[serde(default)]
    pub unit_statements: u32,
}

/// Digest of the complete ordered signed author blocks. Each block already
/// binds its statement bytes, generation, ordinal and authored unit length.
pub(crate) fn authored_schema_unit_digest(
    authors: &[&WireDdlAuthor],
) -> Result<[u8; 32], SyncError> {
    check_authored_units_whole_and_in_order(authors.iter().copied())?;
    let bytes =
        rmp_serde::to_vec(authors).map_err(|error| SyncError::Protocol(error.to_string()))?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

/// Private wire-to-apply handoff. The engine's public schema API never
/// exposes transport identity.
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
    /// The original author's identity when the sender forwarded this record;
    /// `None` when the authenticated sender is itself the author.
    pub author: Option<WireDdlAuthor>,
}

impl ReceivedDdlContext {
    /// The author of `entry`: the signed identity the sender carried, whether
    /// the sender wrote the schema or forwarded it.
    pub(crate) fn author_of(&self, entry: &ReceivedDdlEntry) -> WireDdlAuthor {
        // Every wire lane carries a signed author, checked before the unit
        // reaches an install path. An in-crate context assembled without one
        // names its authenticated sender and shows no signature, so anything
        // it forwards is refused rather than believed.
        entry.author.clone().unwrap_or_else(|| WireDdlAuthor {
            attestation: Vec::new(),
            unit_statements: 0,
            node_id: self.source_node_id.clone(),
            incarnation: self.source_incarnation,
            source_ddl_lsn: entry.source_ddl_lsn,
            ordinal: entry.ordinal,
            table: entry.table.clone(),
            table_generation: entry.table_generation,
            digest: entry.digest.clone(),
        })
    }
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
    /// replicated row by the age it actually has. An absent stamp lets the
    /// receiver stamp its own time.
    #[serde(default)]
    pub created_at: Option<contextdb_core::Wallclock>,
    /// The row's ordering position — this row's sync provenance sidecar on
    /// whichever node is SENDING it, `None` when that node authored the row
    /// itself and never yet synced it anywhere (see
    /// `wire_row_arrivals`/`wire_changeset_with_arrivals`). Arbitration
    /// compares this, never `lsn` (which is the sender's own unrelated local
    /// clock): an incoming row with no arrival always wins, one carrying an
    /// arrival at or below the stored position is a stale echo, and one
    /// above it wins. An absent ordering position denotes unstamped work;
    /// protocol acceptance is checked before decoding this payload.
    #[serde(default)]
    pub arrival: Option<Lsn>,
    /// Immutable creation provenance. Current validation rejects absent
    /// provenance before any sync mutation.
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
    /// Opaque self-contained selection for a listed node-local table.
    #[serde(default)]
    pub node_local_predicate: Option<Vec<u8>>,
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
    /// Carried only by a refused member that has no accepted value of its
    /// own, naming the row that caused its whole connected unit to be
    /// refused. A trailing optional slot: a peer that omits it decodes as
    /// absent, and every earlier slot keeps its position.
    #[serde(default)]
    pub refusal_cause: Option<WireRefusalCause>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WireRefusalCause {
    pub table: String,
    pub natural_key: WireNaturalKey,
}

impl From<RefusalCause> for WireRefusalCause {
    fn from(value: RefusalCause) -> Self {
        Self {
            table: value.table,
            natural_key: value.natural_key.into(),
        }
    }
}

impl From<WireRefusalCause> for RefusalCause {
    fn from(value: WireRefusalCause) -> Self {
        Self {
            table: value.table,
            natural_key: value.natural_key.into(),
        }
    }
}

pub fn encode<T: Serialize>(msg_type: MessageType, msg: &T) -> Result<Vec<u8>, SyncError> {
    encode_for_version(PROTOCOL_VERSION, msg_type, msg)
}

/// Encode one response/request only at an accepted protocol version.
pub fn encode_for_version<T: Serialize>(
    version: u8,
    msg_type: MessageType,
    msg: &T,
) -> Result<Vec<u8>, SyncError> {
    if !supports_protocol_version(version) {
        return Err(SyncError::ProtocolVersionMismatch {
            received: version,
            oldest_supported: OLDEST_SUPPORTED_PROTOCOL_VERSION,
            newest_supported: PROTOCOL_VERSION,
        });
    }
    let payload = rmp_serde::to_vec(msg).map_err(|error| SyncError::Serde(error.to_string()))?;
    let envelope = Envelope {
        version,
        message_type: msg_type,
        payload,
    };
    rmp_serde::to_vec(&envelope).map_err(|e| SyncError::Serde(e.to_string()))
}

/// Count both MessagePack buffers without constructing either buffer. The
/// payload stream is serialized as the same u8 sequence Envelope uses.
fn serialized_size_for_version<T: Serialize>(
    version: u8,
    message_type: &MessageType,
    message: &T,
) -> Result<(usize, usize), SyncError> {
    #[derive(Default)]
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("serialized request size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    struct Payload<'a, T> {
        message: &'a T,
        bytes: usize,
    }
    impl<T: Serialize> Serialize for Payload<'_, T> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            use serde::ser::SerializeSeq;
            struct SequenceWriter<'a, Q: SerializeSeq>(&'a mut Q);
            impl<Q: SerializeSeq> std::io::Write for SequenceWriter<'_, Q> {
                fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                    for byte in bytes {
                        self.0
                            .serialize_element(byte)
                            .map_err(|error| std::io::Error::other(error.to_string()))?;
                    }
                    Ok(bytes.len())
                }
                fn flush(&mut self) -> std::io::Result<()> {
                    Ok(())
                }
            }
            let mut sequence = serializer.serialize_seq(Some(self.bytes))?;
            self.message
                .serialize(&mut rmp_serde::Serializer::new(SequenceWriter(
                    &mut sequence,
                )))
                .map_err(serde::ser::Error::custom)?;
            sequence.end()
        }
    }
    #[derive(Serialize)]
    struct BorrowedEnvelope<'a, T> {
        version: u8,
        message_type: &'a MessageType,
        payload: Payload<'a, T>,
    }
    let mut payload = Counter::default();
    message
        .serialize(&mut rmp_serde::Serializer::new(&mut payload))
        .map_err(|error| SyncError::Serde(error.to_string()))?;
    let mut envelope = Counter::default();
    BorrowedEnvelope {
        version,
        message_type,
        payload: Payload {
            message,
            bytes: payload.0,
        },
    }
    .serialize(&mut rmp_serde::Serializer::new(&mut envelope))
    .map_err(|error| SyncError::Serde(error.to_string()))?;
    Ok((payload.0, envelope.0))
}

pub(crate) fn encode_for_version_with_read_owner<T: Serialize>(
    version: u8,
    message_type: MessageType,
    message: &T,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> contextdb_core::Result<(Vec<u8>, contextdb_core::read_memory::ReadCredit)> {
    if !supports_protocol_version(version) {
        return Err(contextdb_core::Error::SyncError(
            SyncError::ProtocolVersionMismatch {
                received: version,
                oldest_supported: OLDEST_SUPPORTED_PROTOCOL_VERSION,
                newest_supported: PROTOCOL_VERSION,
            }
            .to_string(),
        ));
    }
    let (payload_bytes, envelope_bytes) =
        serialized_size_for_version(version, &message_type, message)
            .map_err(|error| contextdb_core::Error::SyncError(error.to_string()))?;
    let payload_credit = contextdb_core::read_memory::ReadCredit::try_new(
        read_memory.clone(),
        payload_bytes,
        "sync_push",
        "encode_payload",
        "Reduce the request or raise the executing session's memory limit.",
    )?;
    let envelope_credit = contextdb_core::read_memory::ReadCredit::try_new(
        read_memory,
        envelope_bytes,
        "sync_push",
        "encode_envelope",
        "Reduce the request or raise the executing session's memory limit.",
    )?;
    let mut payload = Vec::with_capacity(payload_bytes);
    message
        .serialize(&mut rmp_serde::Serializer::new(&mut payload))
        .map_err(|error| contextdb_core::Error::SyncError(error.to_string()))?;
    let envelope = Envelope {
        version,
        message_type,
        payload,
    };
    let mut encoded = Vec::with_capacity(envelope_bytes);
    envelope
        .serialize(&mut rmp_serde::Serializer::new(&mut encoded))
        .map_err(|error| contextdb_core::Error::SyncError(error.to_string()))?;
    drop(envelope);
    drop(payload_credit);
    Ok((encoded, envelope_credit))
}

/// Encode an additive custody payload as a named MessagePack map inside the
/// unchanged protocol envelope.
pub fn encode_named<T: Serialize>(msg_type: MessageType, msg: &T) -> Result<Vec<u8>, SyncError> {
    let payload = rmp_serde::to_vec_named(msg).map_err(|e| SyncError::Serde(e.to_string()))?;
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
    if !supports_protocol_version(envelope.version) {
        return Err(SyncError::ProtocolVersionMismatch {
            received: envelope.version,
            oldest_supported: OLDEST_SUPPORTED_PROTOCOL_VERSION,
            newest_supported: PROTOCOL_VERSION,
        });
    }
    Ok(envelope)
}

/// Decode a reply only when it mirrors the version used for the request. This
/// keeps negotiation explicit instead of accepting an unexplained downgrade.
pub fn decode_for_version(data: &[u8], expected: u8) -> Result<Envelope, SyncError> {
    let envelope = decode(data)?;
    if envelope.version != expected {
        return Err(SyncError::ProtocolVersionMismatch {
            received: envelope.version,
            oldest_supported: expected,
            newest_supported: expected,
        });
    }
    Ok(envelope)
}

pub const fn supports_protocol_version(version: u8) -> bool {
    version == PROTOCOL_VERSION
}

impl From<ChangeSet> for WireChangeSet {
    fn from(value: ChangeSet) -> Self {
        Self {
            ddl: value.ddl.into_iter().map(Into::into).collect(),
            ddl_lsn: value.ddl_lsn,
            ddl_provenance: Vec::new(),
            purges: Vec::new(),
            // Ordinary unmanifested changes start with an empty lane.
            manifests: Vec::new(),
            rows: value.rows.into_iter().map(Into::into).collect(),
            edges: value.edges.into_iter().map(Into::into).collect(),
            vectors: value.vectors.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<WireChangeSet> for ChangeSet {
    type Error = SyncError;

    fn try_from(value: WireChangeSet) -> Result<Self, Self::Error> {
        // Row-only conversion must never discard unverified manifests.
        if !value.manifests.is_empty() {
            return Err(SyncError::Protocol(
                "delivery manifest verification is not implemented".to_string(),
            ));
        }
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
/// The digest domain is an identity-format label, independent of the envelope
/// protocol version. Renaming it would change authenticated schema identities.
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
        // A declaration relayed from the unattested apply door stays marked
        // unattested. An authenticated receiver refuses it below, with its
        // table name, before constructing any received schema context.
        if provenance.unattested {
            if provenance.author.is_some() {
                return Err(SyncError::Protocol(format!(
                    "DDL provenance at index {index} is both unattested and signed"
                )));
            }
            continue;
        }
        // Authored schema carries its author's own signature. This shape
        // check precedes signature verification where the tenant is known.
        let author = provenance.author.as_ref().ok_or_else(|| {
            SyncError::Protocol(format!(
                "DDL provenance at index {index} names no signed author"
            ))
        })?;
        if author.node_id.is_empty()
            || author.digest.len() != blake3::OUT_LEN
            || author.table != provenance.table
            || author.attestation.is_empty()
        {
            return Err(SyncError::Protocol(format!(
                "DDL provenance at index {index} carries a malformed author identity"
            )));
        }
        // The delivered generation is the one installed. A forwarding peer
        // owns its own delivery position, never the schema instance the
        // author wrote against.
        if author.table_generation != provenance.table_generation {
            return Err(SyncError::Protocol(format!(
                "DDL provenance at index {index} delivers a table generation its author did not \
                 sign"
            )));
        }
    }
    check_authored_units_whole_and_in_order(
        changeset
            .ddl_provenance
            .iter()
            .filter_map(|provenance| provenance.author.as_ref()),
    )
}

/// Every author's source transaction in one delivery -- and in what a
/// receiver stages from it -- arrives whole, once, and in the author's own
/// statement order: its signed ordinals run 0, 1, ... up to the statement
/// count the author signed, contiguously in the vector. A forwarding peer
/// that reorders, drops or splits a signed unit is refused here before
/// anything is staged, and so is a receiver that would stage part of one: its
/// declared table policy refuses a whole transaction or none of it. Separate
/// source transactions keep the order the delivering peer holds them in:
/// per-table holdback legitimately lets a later unrelated unit through first.
pub(crate) fn check_authored_units_whole_and_in_order<'a>(
    authors: impl IntoIterator<Item = &'a WireDdlAuthor>,
) -> Result<(), SyncError> {
    // (author node, incarnation, source LSN, signed statement count, lowest
    // ordinal the next statement may carry)
    let mut open: Option<(&'a str, Incarnation, Lsn, u32, u32)> = None;
    let mut delivered = HashSet::<(&'a str, Incarnation, Lsn)>::new();
    let incomplete = |lsn: Lsn| {
        SyncError::Protocol(format!(
            "authenticated schema delivers only part of its author's source transaction at LSN {}",
            lsn.0
        ))
    };
    let out_of_order = |lsn: Lsn| {
        SyncError::Protocol(format!(
            "authenticated schema does not deliver its author's statements at LSN {} in the \
             author's signed order",
            lsn.0
        ))
    };
    let in_place = |ordinal: u32, next: u32, count: u32| ordinal < count && ordinal == next;
    for author in authors {
        let unit = (
            author.node_id.as_str(),
            author.incarnation,
            author.source_ddl_lsn,
        );
        match open.as_mut() {
            Some((node, incarnation, lsn, count, next)) if (*node, *incarnation, *lsn) == unit => {
                if author.unit_statements != *count || !in_place(author.ordinal, *next, *count) {
                    return Err(out_of_order(*lsn));
                }
                *next = author.ordinal + 1;
            }
            _ => {
                if let Some((_, _, lsn, count, next)) = open.take()
                    && next != count
                {
                    return Err(incomplete(lsn));
                }
                if !delivered.insert(unit) {
                    return Err(SyncError::Protocol(format!(
                        "authenticated schema splits its author's source transaction at LSN {}",
                        author.source_ddl_lsn.0
                    )));
                }
                if !in_place(author.ordinal, 0, author.unit_statements) {
                    return Err(out_of_order(author.source_ddl_lsn));
                }
                open = Some((
                    unit.0,
                    unit.1,
                    unit.2,
                    author.unit_statements,
                    author.ordinal + 1,
                ));
            }
        }
    }
    if let Some((_, _, lsn, count, next)) = open
        && next != count
    {
        return Err(incomplete(lsn));
    }
    Ok(())
}

/// Check one schema record against the author it names: the author's own
/// signature over the statement it sent, its position, and the schema
/// generation it wrote against. A peer that forwards schema carries this block
/// unchanged, so this holds at every hop, however many peers the record
/// crossed and whatever session it last arrived on.
pub(crate) fn verify_ddl_author_attestation(
    tenant_id: &contextdb_core::TenantId,
    ddl: &WireDdlChange,
    author: &WireDdlAuthor,
) -> Result<(), SyncError> {
    if author.attestation.is_empty() {
        return Err(SyncError::Protocol(
            "authenticated schema provenance carries no author signature".to_string(),
        ));
    }
    let attested = canonical_ddl_provenance_digest(
        ddl,
        author.source_ddl_lsn,
        author.ordinal,
        author.table.as_deref(),
        author.table_generation,
    )?;
    if attested != author.digest {
        return Err(SyncError::Protocol(
            "authenticated schema provenance does not digest the statement its author attested"
                .to_string(),
        ));
    }
    let ddl_bytes = rmp_serde::to_vec(ddl)
        .map_err(|err| SyncError::Protocol(format!("cannot encode authored DDL: {err}")))?;
    let bytes = crate::custody::records::ddl_author_attestation_bytes(
        tenant_id,
        &author.node_id,
        author.incarnation,
        author.source_ddl_lsn,
        author.ordinal,
        author.unit_statements,
        author.table.as_deref(),
        author.table_generation,
        &ddl_bytes,
        &author.digest,
    )
    .map_err(|err| SyncError::Protocol(err.to_string()))?;
    crate::identity::FabricIdentity::verify_lineage_by_node_id(
        &author.node_id,
        &bytes,
        &author.attestation,
    )
    .map_err(|_| {
        SyncError::Protocol(
            "authenticated schema provenance does not verify for the author it names".to_string(),
        )
    })
}

/// The author block for one schema record, signed as `identity` the way an
/// author's own send signs it. A fixture that stands in for a peer restating
/// schema that peer authored uses this; no other way of making a block that
/// verifies exists, which is the point.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn signed_ddl_author_for_test(
    tenant_id: &contextdb_core::TenantId,
    identity: &crate::identity::FabricIdentity,
    incarnation: Incarnation,
    ddl: &WireDdlChange,
    provenance: &WireDdlProvenance,
    unit_statements: u32,
) -> Result<WireDdlAuthor, SyncError> {
    let node_id = identity.node_id();
    let ddl_bytes = rmp_serde::to_vec(ddl)
        .map_err(|err| SyncError::Protocol(format!("cannot encode authored DDL: {err}")))?;
    let bytes = crate::custody::records::ddl_author_attestation_bytes(
        tenant_id,
        &node_id,
        incarnation,
        provenance.source_ddl_lsn,
        provenance.ordinal,
        unit_statements,
        provenance.table.as_deref(),
        provenance.table_generation,
        &ddl_bytes,
        &provenance.digest,
    )
    .map_err(|err| SyncError::Protocol(err.to_string()))?;
    Ok(WireDdlAuthor {
        node_id,
        incarnation,
        source_ddl_lsn: provenance.source_ddl_lsn,
        ordinal: provenance.ordinal,
        table: provenance.table.clone(),
        table_generation: provenance.table_generation,
        digest: provenance.digest.clone(),
        attestation: identity.sign_for_test(&bytes),
        unit_statements,
    })
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
    // The author's signature is checked before the unit can reach any install
    // path, on every hop. A delivering peer's authenticated session says only
    // who handed the record over, never who wrote it.
    for (ddl, provenance) in changeset.ddl.iter().zip(changeset.ddl_provenance.iter()) {
        if provenance.unattested {
            return Err(SyncError::UnattestedReceivedSchema {
                table: provenance.table.clone().unwrap_or_else(|| {
                    wire_ddl_affected_table(ddl).unwrap_or("schema").to_string()
                }),
            });
        }
        let author = provenance.author.as_ref().ok_or_else(|| {
            SyncError::Protocol("authenticated schema names no signed author".to_string())
        })?;
        verify_ddl_author_attestation(tenant_id, ddl, author)?;
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
                author: entry.author.clone(),
            })
            .collect(),
    }))
}

pub(crate) fn received_ddl_error(error: SyncError) -> contextdb_core::Error {
    match error {
        SyncError::UnattestedReceivedSchema { table } => {
            contextdb_core::Error::ReceivedSchemaAuthorshipUnavailable {
                table,
                reason: "the declaration arrived without its author's signature".to_string(),
            }
        }
        error => contextdb_core::Error::SyncError(error.to_string()),
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
            refusal_cause: value.refusal_cause.map(Into::into),
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
            refusal_cause: value.refusal_cause.map(Into::into),
        }
    }
}

/// Request on the per-tenant sync-status subject. Tenant identity rides the
/// subject and the asking node id rides the authenticated connection; the body
/// carries the asking edge's per-life incarnation so the hub reads back the
/// record for THIS life of the edge — a rebuilt edge (fresh incarnation) is
/// answered `Lsn(0)`, never the prior life's stale-high watermark.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncStatusRequest {
    pub incarnation: Incarnation,
    /// Exact candidate digests; bounded to 128 per authenticated request.
    pub refused_schema_unit_digests: Vec<[u8; 32]>,
}

impl Serialize for SyncStatusRequest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut value = serializer.serialize_struct("SyncStatusRequest", 3)?;
        value.serialize_field("incarnation", &self.incarnation)?;
        // The custody status probe owns this tuple slot when it has a signed checkpoint.
        value.serialize_field("checkpoint", &None::<()>)?;
        value.serialize_field(
            "refused_schema_unit_digests",
            &self.refused_schema_unit_digests,
        )?;
        value.end()
    }
}

impl<'de> Deserialize<'de> for SyncStatusRequest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Probe {
            incarnation: Incarnation,
            #[serde(default, rename = "checkpoint")]
            _checkpoint: Option<serde::de::IgnoredAny>,
            refused_schema_unit_digests: Vec<[u8; 32]>,
        }
        let Probe {
            incarnation,
            _checkpoint: _,
            refused_schema_unit_digests,
        } = Probe::deserialize(deserializer)?;
        Ok(Self {
            incarnation,
            refused_schema_unit_digests,
        })
    }
}

/// Reply on the per-tenant sync-status subject. The exact digest acknowledgment
/// binds refusal facts to the authenticated candidate request.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct SyncStatusResponse {
    /// The server's persisted per-tenant record of the highest edge-LSN it has
    /// applied (contract item 3 — rides `export_snapshot` artifacts).
    #[serde(default)]
    pub applied_push_watermark: Option<Lsn>,
    /// The server's current LSN clock (contract item 4, pull-side resume).
    #[serde(default)]
    pub server_current_lsn: Option<Lsn>,
    // Ordinary status reports the current custody authority.
    #[serde(default)]
    pub hub_incarnation: Option<Incarnation>,
    #[serde(default)]
    pub refused_schema_units: Vec<RefusedSchemaUnitOutcome>,
    /// Authenticated acknowledgment of exactly the requested candidate batch.
    #[serde(default)]
    pub refused_schema_unit_digests_ack: Option<Vec<[u8; 32]>>,
}
impl Serialize for SyncStatusResponse {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let has_ack = self.refused_schema_unit_digests_ack.is_some();
        let has_refusals = !self.refused_schema_units.is_empty() || has_ack;
        let mut value = serializer.serialize_struct(
            "SyncStatusResponse",
            3 + usize::from(has_refusals) + usize::from(has_ack),
        )?;
        value.serialize_field("applied_push_watermark", &self.applied_push_watermark)?;
        value.serialize_field("server_current_lsn", &self.server_current_lsn)?;
        value.serialize_field("hub_incarnation", &self.hub_incarnation)?;
        if has_refusals {
            value.serialize_field("refused_schema_units", &self.refused_schema_units)?;
        }
        if has_ack {
            value.serialize_field(
                "refused_schema_unit_digests_ack",
                &self.refused_schema_unit_digests_ack,
            )?;
        }
        value.end()
    }
}

/// One atomically persisted candidate pass. This is not an audit-history cursor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RefusedSchemaLookupCheckpoint {
    pub candidate_fingerprint: [u8; 32],
    pub hub_incarnation: Incarnation,
    pub next_index: usize,
}

// Manifests live directly in the ordinary changeset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireDeliveryManifest {
    #[serde(with = "serde_bytes")]
    pub submission_id: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub seal: Vec<u8>,
    pub life_evidence: Vec<Vec<u8>>,
    #[serde(with = "serde_bytes")]
    pub materialization_projection: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub policy_evidence: Vec<u8>,
    pub retained_slots: Vec<Vec<u8>>,
    pub erased_slot_count: u64,
    #[serde(with = "serde_bytes")]
    pub submission_signature: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub disclosure: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub disclosure_signature: Vec<u8>,
    #[serde(deserialize_with = "required_custody_field")]
    pub erasure_authorization: Option<Vec<u8>>,
}

#[cfg(feature = "test-seams")]
impl WireDeliveryManifest {
    /// Inspect the canonical seal without treating inspection as authentication.
    #[doc(hidden)]
    pub fn root_table_for_test(&self) -> Option<&str> {
        crate::custody::decoder::SealView::read(&self.seal)
            .ok()
            .map(|s| s.root_table)
    }

    #[doc(hidden)]
    pub fn member_count_for_test(&self) -> Option<u64> {
        crate::custody::decoder::SealView::read(&self.seal)
            .ok()
            .map(|s| s.member_count)
    }

    #[doc(hidden)]
    pub fn unit_digest_for_test(&self) -> Option<[u8; 32]> {
        crate::custody::decoder::SealView::read(&self.seal)
            .ok()
            .map(|s| s.unit_digest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireDeliveryOutcome {
    #[serde(with = "serde_bytes")]
    pub lookup_submission: Vec<u8>,
    pub lookup_seal_digest: [u8; 32],
    pub lookup_origin_life_digest: [u8; 32],
    #[serde(with = "serde_bytes")]
    pub lookup_source: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub signed_core: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub diagnostic_body: Vec<u8>,
}

#[cfg(feature = "test-seams")]
impl WireDeliveryOutcome {
    /// Inspect the canonical core without treating inspection as authentication.
    #[doc(hidden)]
    pub fn outcome_for_test(&self) -> Option<&str> {
        crate::custody::decoder::TerminalView::read(&self.signed_core)
            .ok()
            .map(|s| s.kind)
    }

    #[doc(hidden)]
    pub fn cause_for_test(&self) -> Option<&str> {
        crate::custody::decoder::TerminalView::read(&self.signed_core)
            .ok()
            .and_then(|s| s.cause)
    }
}

// No separate delivery push request, wrapper, or result.
fn required_custody_field<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    T::deserialize(deserializer)
}

macro_rules! custody_request {
    ($name:ident { $($field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            pub format: u8,
            pub tenant_id: String,
            pub edge_incarnation: Incarnation,
            pub request_nonce: [u8; 16],
            $(#[serde(deserialize_with = "required_custody_field")] pub $field: $ty,)*
        }
    };
}

macro_rules! custody_response {
    ($name:ident, $result:ty) => {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            pub format: u8,
            pub request_nonce: [u8; 16],
            pub hub_node_id: String,
            pub hub_incarnation: Incarnation,
            pub tenant_id: String,
            pub edge_node_id: String,
            pub edge_incarnation: Incarnation,
            #[serde(deserialize_with = "required_custody_field")]
            pub result: Option<$result>,
            #[serde(deserialize_with = "required_custody_field")]
            pub error: Option<String>,
        }
    };
}

custody_request!(BindApplicationTablePolicyRequest {
    expectation: ApplicationTablePolicyExpectation,
});
/// A failed comparison and progress response carry no authority at all.
/// Closed branches prevent a decoder from silently ignoring an injected scope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BindApplicationTablePolicyResponse {
    Error(BindPolicyFailure),
    Success(BindPolicySuccess),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindPolicyFailure {
    pub format: u8,
    pub request_nonce: [u8; 16],
    pub error: BindPolicyError,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum BindPolicyError {
    TenantPolicyMismatch { table: String, clause: String },
    TenantPolicyNotDeclared { table: String },
    Operational { message: String },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindPolicySuccess {
    pub format: u8,
    pub request_nonce: [u8; 16],
    pub hub_node_id: String,
    pub hub_incarnation: Incarnation,
    pub tenant_id: String,
    pub edge_node_id: String,
    pub edge_incarnation: Incarnation,
    pub binding: Vec<u8>,
}
custody_request!(FetchDeliveryOutcomesRequest {
    since: Option<Vec<u8>>,
});
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryOutcomePage {
    pub outcomes: Vec<WireDeliveryOutcome>,
}
custody_response!(FetchDeliveryOutcomesResponse, DeliveryOutcomePage);

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

#[cfg(test)]
mod refused_schema_wire_tests {
    use super::*;

    #[test]
    fn empty_refusal_status_keeps_its_previous_wire_shape() {
        #[derive(Serialize)]
        struct PreviousResponse {
            applied_push_watermark: Option<Lsn>,
            server_current_lsn: Option<Lsn>,
            hub_incarnation: Option<Incarnation>,
        }
        let response = SyncStatusResponse {
            applied_push_watermark: Some(Lsn(4)),
            server_current_lsn: Some(Lsn(9)),
            hub_incarnation: Some(Incarnation(7)),
            ..Default::default()
        };
        let bytes = rmp_serde::to_vec(&response).unwrap();
        assert_eq!(
            bytes,
            rmp_serde::to_vec(&PreviousResponse {
                applied_push_watermark: response.applied_push_watermark,
                server_current_lsn: response.server_current_lsn,
                hub_incarnation: response.hub_incarnation
            })
            .unwrap()
        );
        assert_eq!(
            rmp_serde::from_slice::<SyncStatusResponse>(&bytes).unwrap(),
            response
        );
    }

    #[test]
    fn exact_refusal_queries_share_the_custody_tuple_and_require_an_explicit_ack() {
        let digests = vec![[7; 32], [19; 32]];
        let request = SyncStatusRequest {
            incarnation: Incarnation(7),
            refused_schema_unit_digests: digests.clone(),
            ..Default::default()
        };
        let bytes = rmp_serde::to_vec(&request).unwrap();
        let probe: crate::custody::incarnation::StatusProbe =
            rmp_serde::from_slice(&bytes).unwrap();
        assert!(probe.checkpoint.is_none());
        assert_eq!(probe.refused_schema_unit_digests, digests);
        assert_eq!(
            rmp_serde::from_slice::<SyncStatusRequest>(&rmp_serde::to_vec(&probe).unwrap())
                .unwrap(),
            request
        );
        let response = SyncStatusResponse {
            hub_incarnation: Some(Incarnation(9)),
            refused_schema_unit_digests_ack: Some(digests),
            ..Default::default()
        };
        assert_eq!(
            rmp_serde::from_slice::<SyncStatusResponse>(&rmp_serde::to_vec(&response).unwrap())
                .unwrap(),
            response
        );
        let old = SyncStatusResponse {
            refused_schema_unit_digests_ack: None,
            ..response
        };
        let decoded: SyncStatusResponse =
            rmp_serde::from_slice(&rmp_serde::to_vec(&old).unwrap()).unwrap();
        assert!(decoded.refused_schema_unit_digests_ack.is_none());
    }

    #[test]
    fn ordinary_status_probe_carries_an_empty_exact_lookup() {
        #[derive(Serialize, Deserialize)]
        struct ExactRequest {
            incarnation: Incarnation,
            checkpoint: Option<()>,
            refused_schema_unit_digests: Vec<[u8; 32]>,
        }
        let current = SyncStatusRequest {
            incarnation: Incarnation(7),
            refused_schema_unit_digests: Vec::new(),
        };
        let bytes = rmp_serde::to_vec(&current).unwrap();
        assert_eq!(
            bytes,
            rmp_serde::to_vec(&ExactRequest {
                incarnation: Incarnation(7),
                checkpoint: None,
                refused_schema_unit_digests: Vec::new(),
            })
            .unwrap()
        );
        assert_eq!(
            rmp_serde::from_slice::<SyncStatusRequest>(&bytes).unwrap(),
            current
        );
        let exact = SyncStatusRequest {
            refused_schema_unit_digests: vec![[7; 32]],
            ..current
        };
        let server_probe: crate::custody::incarnation::StatusProbe =
            rmp_serde::from_slice(&rmp_serde::to_vec(&exact).unwrap()).unwrap();
        assert!(server_probe.checkpoint.is_none());
        assert_eq!(
            server_probe.refused_schema_unit_digests,
            exact.refused_schema_unit_digests
        );
        assert_eq!(
            rmp_serde::from_slice::<SyncStatusRequest>(&rmp_serde::to_vec(&server_probe).unwrap())
                .unwrap(),
            exact
        );
    }
}
