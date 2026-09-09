use crate::sync_types::{Conflict, NaturalKey};
use contextdb_core::{
    ConflictPolicy, EdgeDiscardMode, HistoryPolicy, Incarnation, Lsn, RetainUnit, SyncDirection,
    TenantId, TxId,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionDeclaration {
    pub seconds: u64,
    pub declared_unit: RetainUnit,
    pub sync_safe: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationTablePolicy {
    pub sync_direction: SyncDirection,
    pub sync_conflict: ConflictPolicy,
    pub immutable: bool,
    pub retain: Option<RetentionDeclaration>,
    pub history: HistoryPolicy,
    pub manifest_tables: Option<Vec<String>>,
    pub edge_discard: EdgeDiscardMode,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationTablePolicyExpectation {
    pub tables: BTreeMap<String, ApplicationTablePolicy>,
}

impl ApplicationTablePolicyExpectation {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add caller input using the same clause words as
    /// `DECLARE TENANT TABLE POLICY`. The hub still resolves and
    /// authenticates this expectation before it can become authority.
    pub fn expect_table(
        mut self,
        table: impl Into<String>,
        clauses: &str,
    ) -> contextdb_core::Result<Self> {
        let table = table.into();
        let policy = parse_expectation_clauses(&table, clauses)?;
        if self.tables.insert(table.clone(), policy).is_some() {
            return Err(contextdb_core::Error::SchemaInvalid {
                reason: format!("table {table} is listed more than once in the expectation"),
            });
        }
        Ok(self)
    }
}

fn parse_expectation_clauses(
    table: &str,
    clauses: &str,
) -> contextdb_core::Result<ApplicationTablePolicy> {
    let quoted = table.replace('"', "\"\"");
    let statement = contextdb_parser::parse(&format!(
        "DECLARE TENANT TABLE POLICY \"{quoted}\" {clauses}"
    ))?;
    let contextdb_parser::Statement::DeclareTenantTablePolicy(p) = statement else {
        return Err(contextdb_core::Error::SchemaInvalid {
            reason: "expected table policy".into(),
        });
    };
    Ok(ApplicationTablePolicy {
        sync_direction: p
            .sync_direction
            .unwrap_or(contextdb_core::DEFAULT_SYNC_DIRECTION),
        sync_conflict: p
            .conflict_policy
            .unwrap_or(contextdb_core::DEFAULT_CONFLICT_POLICY),
        immutable: p.immutable,
        retain: p.retain.map(|r| RetentionDeclaration {
            seconds: r.duration_seconds,
            declared_unit: r.declared_unit,
            sync_safe: r.sync_safe,
        }),
        history: p.history.unwrap_or(contextdb_core::DEFAULT_HISTORY_POLICY),
        manifest_tables: p.delivery_manifest_tables,
        edge_discard: p.edge_discard.unwrap_or_default(),
    })
}

fn empty_inspection(columns: &[&str]) -> crate::database::QueryResult {
    let mut result = crate::database::QueryResult::empty();
    result.columns = columns.iter().map(|column| (*column).to_string()).collect();
    result
}

pub(crate) fn empty_policy_inspection() -> crate::database::QueryResult {
    empty_inspection(&[
        "table",
        "tenant_id",
        "version",
        "digest",
        "sync_direction",
        "sync_conflict",
        "immutable",
        "retain_seconds",
        "retain_unit",
        "sync_safe",
        "history",
        "manifest_tables",
        "edge_discard",
        "first_bound_position",
    ])
}

pub(crate) fn empty_binding_inspection() -> crate::database::QueryResult {
    empty_inspection(&[
        "table",
        "tenant_id",
        "hub_node_id",
        "hub_incarnation",
        "edge_node_id",
        "edge_incarnation",
        "version",
        "digest",
        "sync_direction",
        "sync_conflict",
        "immutable",
        "retain_seconds",
        "retain_unit",
        "sync_safe",
        "history",
        "manifest_tables",
        "edge_discard",
    ])
}

pub(crate) fn empty_outcome_inspection() -> crate::database::QueryResult {
    empty_inspection(&[
        "root_table",
        "root_key",
        "unit_digest",
        "outcome",
        "cause",
        "hub_node_id",
        "hub_incarnation",
        "edge_node_id",
        "edge_incarnation",
        "outcome_position",
        "conflicts",
    ])
}

#[derive(Debug, Clone)]
pub struct DeliveryManifest<'a> {
    pub root_table: &'a str,
    pub root_key: NaturalKey,
    pub members: Vec<(&'a str, NaturalKey)>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryStatusCounts {
    pub disabled: bool,
    pub eligible: u64,
    pub accepted: u64,
    pub equivalent: u64,
    pub pending: u64,
    pub refused: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BoundTablePolicy {
    pub(crate) policy: ApplicationTablePolicy,
    pub(crate) version: u64,
    pub(crate) digest: [u8; 32],
}

impl BoundTablePolicy {
    pub fn policy(&self) -> &ApplicationTablePolicy {
        &self.policy
    }
    pub fn version(&self) -> u64 {
        self.version
    }
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuthenticatedTenantPolicyBinding {
    pub(crate) tenant_id: TenantId,
    pub(crate) hub_node_id: String,
    pub(crate) hub_incarnation: Incarnation,
    pub(crate) edge_node_id: String,
    pub(crate) edge_incarnation: Incarnation,
    pub(crate) tables: BTreeMap<String, BoundTablePolicy>,
}

impl AuthenticatedTenantPolicyBinding {
    pub fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }
    pub fn hub_node_id(&self) -> &str {
        &self.hub_node_id
    }
    pub fn hub_incarnation(&self) -> Incarnation {
        self.hub_incarnation
    }
    pub fn edge_node_id(&self) -> &str {
        &self.edge_node_id
    }
    pub fn edge_incarnation(&self) -> Incarnation {
        self.edge_incarnation
    }
    pub fn tables(&self) -> &BTreeMap<String, BoundTablePolicy> {
        &self.tables
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliveryOutcomeKind {
    Accepted,
    Equivalent,
    Refused,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeliveryOutcomeCursor {
    pub(crate) tenant_id: TenantId,
    pub(crate) hub_node_id: String,
    pub(crate) hub_incarnation: Incarnation,
    pub(crate) edge_node_id: String,
    pub(crate) edge_incarnation: Incarnation,
    pub(crate) position: Lsn,
    pub(crate) ordinal: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DeliveryOutcome {
    pub(crate) tenant_id: TenantId,
    pub(crate) hub_node_id: String,
    pub(crate) hub_incarnation: Incarnation,
    pub(crate) edge_node_id: String,
    pub(crate) edge_incarnation: Incarnation,
    pub(crate) root_table: String,
    pub(crate) root_key: NaturalKey,
    pub(crate) unit_digest: Option<[u8; 32]>,
    pub(crate) kind: DeliveryOutcomeKind,
    pub(crate) cursor: DeliveryOutcomeCursor,
    pub(crate) cause: Option<String>,
    pub(crate) conflicts: Option<Vec<Conflict>>,
}

impl DeliveryOutcome {
    pub fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }
    pub fn hub_node_id(&self) -> &str {
        &self.hub_node_id
    }
    pub fn hub_incarnation(&self) -> Incarnation {
        self.hub_incarnation
    }
    pub fn edge_node_id(&self) -> &str {
        &self.edge_node_id
    }
    pub fn edge_incarnation(&self) -> Incarnation {
        self.edge_incarnation
    }
    pub fn root_table(&self) -> &str {
        &self.root_table
    }
    pub fn root_key(&self) -> &NaturalKey {
        &self.root_key
    }
    pub fn unit_digest(&self) -> Option<[u8; 32]> {
        self.unit_digest
    }
    pub fn kind(&self) -> DeliveryOutcomeKind {
        self.kind
    }
    pub fn cursor(&self) -> DeliveryOutcomeCursor {
        self.cursor.clone()
    }
    pub fn cause(&self) -> Option<&str> {
        self.cause.as_deref()
    }
    pub fn acceptance_position(&self) -> Lsn {
        self.cursor.position
    }
    pub fn conflicts(&self) -> Option<&[Conflict]> {
        self.conflicts.as_deref()
    }
}

impl crate::Database {
    /// Register the complete root/member write set in the same transaction as its rows.
    ///
    /// Requires the root's declared manifest policy and the authenticated source identity.
    /// Construct a `SyncClient` or `SyncServer` on this same `Database` handle in this process
    /// before registration; construction loads the identity, and no live connection is needed.
    /// Otherwise registration returns `SchemaInvalid` with
    /// `delivery registration requires an authenticated sync identity`. Construct the sync
    /// component first, including after reopening the database in another process, then retry.
    /// Members must be named by that policy, written in `tx`, and reference this root. An empty
    /// member list is valid. Invalid registration aborts `tx`; rollback or failed commit leaves
    /// neither rows nor a manifest. The engine computes BLAKE3 over every application column.
    pub fn register_delivery_manifest(
        &self,
        tx: TxId,
        manifest: DeliveryManifest<'_>,
    ) -> contextdb_core::Result<()> {
        let runtime =
            self.custody_runtime()
                .ok_or_else(|| contextdb_core::Error::SchemaInvalid {
                    reason: "delivery registration requires an authenticated sync identity".into(),
                })?;
        self.stage_delivery_registration(
            tx,
            crate::custody::preparation::SourceRegistration {
                tenant: runtime.tenant,
                edge: runtime.node,
                signer: runtime.signer,
                root: crate::custody::records::RowRef {
                    table: manifest.root_table.into(),
                    key: manifest.root_key,
                },
                members: manifest
                    .members
                    .into_iter()
                    .map(|(table, key)| crate::custody::records::RowRef {
                        table: table.into(),
                        key,
                    })
                    .collect(),
            },
        )
    }
    /// Read the durable outcome for the visible current root at the registered destination.
    ///
    /// Returns `None` for a missing, hidden, purged, discarded, pending, or old-destination root.
    /// This metadata read changes no journal, binding, or watermark and obeys the handle's scope.
    pub fn delivery_outcome(
        &self,
        root_table: &str,
        root_key: &NaturalKey,
    ) -> contextdb_core::Result<Option<DeliveryOutcome>> {
        crate::custody::inspection::outcome(self, root_table, root_key)
    }
    /// Count visible eligible units and their accepted, equivalent, refused, and pending states.
    ///
    /// Only an admitted durable outcome grants credit; transport watermarks do not. `SYNC OFF`
    /// returns disabled and zero eligible units. Counts obey this handle's row scope and do not
    /// mutate custody. Administrative `SHOW` queries use the ordinary read session limits.
    pub fn delivery_status(
        &self,
        root_table: &str,
    ) -> contextdb_core::Result<DeliveryStatusCounts> {
        crate::custody::inspection::status(self, root_table)
    }
}

// Only the authenticated local read decoder constructs this projection. Public
// deserialization is deliberately not provided for an admitted outcome or cursor.
pub(crate) fn decode_outcome_projection(
    text: &str,
) -> contextdb_core::Result<Option<DeliveryOutcome>> {
    fn required_nullable<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
    where
        D: serde::Deserializer<'de>,
        T: Deserialize<'de>,
    {
        Option::<T>::deserialize(deserializer)
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct CursorProjection {
        tenant_id: TenantId,
        hub_node_id: String,
        hub_incarnation: Incarnation,
        edge_node_id: String,
        edge_incarnation: Incarnation,
        position: Lsn,
        ordinal: u32,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct OutcomeProjection {
        tenant_id: TenantId,
        hub_node_id: String,
        hub_incarnation: Incarnation,
        edge_node_id: String,
        edge_incarnation: Incarnation,
        root_table: String,
        root_key: NaturalKey,
        #[serde(deserialize_with = "required_nullable")]
        unit_digest: Option<[u8; 32]>,
        kind: DeliveryOutcomeKind,
        cursor: CursorProjection,
        #[serde(deserialize_with = "required_nullable")]
        cause: Option<String>,
        #[serde(deserialize_with = "required_nullable")]
        conflicts: Option<Vec<Conflict>>,
    }
    let Some(p): Option<OutcomeProjection> =
        serde_json::from_str(text).map_err(|_| crate::custody::canonical::invalid())?
    else {
        return Ok(None);
    };
    if p.tenant_id != p.cursor.tenant_id
        || p.hub_node_id != p.cursor.hub_node_id
        || p.hub_incarnation != p.cursor.hub_incarnation
        || p.edge_node_id != p.cursor.edge_node_id
        || p.edge_incarnation != p.cursor.edge_incarnation
    {
        return Err(crate::custody::canonical::invalid());
    }
    Ok(Some(DeliveryOutcome {
        tenant_id: p.tenant_id,
        hub_node_id: p.hub_node_id,
        hub_incarnation: p.hub_incarnation,
        edge_node_id: p.edge_node_id,
        edge_incarnation: p.edge_incarnation,
        root_table: p.root_table,
        root_key: p.root_key,
        unit_digest: p.unit_digest,
        kind: p.kind,
        cursor: DeliveryOutcomeCursor {
            tenant_id: p.cursor.tenant_id,
            hub_node_id: p.cursor.hub_node_id,
            hub_incarnation: p.cursor.hub_incarnation,
            edge_node_id: p.cursor.edge_node_id,
            edge_incarnation: p.cursor.edge_incarnation,
            position: p.cursor.position,
            ordinal: p.cursor.ordinal,
        },
        cause: p.cause,
        conflicts: p.conflicts,
    }))
}
