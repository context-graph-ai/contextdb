#![allow(clippy::too_many_arguments)] // Existing compatibility and test seam shape.
use super::canonical::{Encoder, hex, invalid, policy_digest, reference_key};
use crate::custody_types::ApplicationTablePolicy;
use crate::protocol::WireRowLineage;
use crate::sync_types::{Conflict, NaturalKey};
use contextdb_core::{Incarnation, Lsn, Result, TenantId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) type Digest = [u8; 32];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct RowRef {
    pub table: String,
    pub key: NaturalKey,
}
impl RowRef {
    pub fn encode(&self, e: &mut Encoder) {
        e.reference(&self.table, &self.key);
    }
    pub fn bytes(&self) -> Vec<u8> {
        reference_key(&self.table, &self.key)
    }
    pub fn order_key(&self) -> (String, Vec<u8>) {
        let mut e = Encoder::default();
        e.key(&self.key);
        (self.table.clone(), e.0)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct LocalLifeAnchor {
    pub original_table: String,
    pub table_generation: u64,
    pub local_row_id: u64,
    pub creation_lsn: Lsn,
}
impl LocalLifeAnchor {
    pub fn encode(&self, e: &mut Encoder) {
        e.string(&self.original_table);
        e.u64(self.table_generation);
        e.u64(self.local_row_id);
        e.u64(self.creation_lsn.0);
    }
    pub fn key(&self) -> String {
        let mut e = Encoder::default();
        self.encode(&mut e);
        hex(&e.0)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StateRef {
    pub logical_store_id: Uuid,
    pub incarnation: Incarnation,
    pub birth_id: Uuid,
    pub token: Digest,
}
impl StateRef {
    pub fn encode(&self, e: &mut Encoder) {
        e.raw(self.logical_store_id.as_bytes());
        e.raw(&self.incarnation.0.to_be_bytes());
        e.raw(self.birth_id.as_bytes());
        e.raw(&self.token);
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StateRecord {
    pub state: StateRef,
    pub parent: Option<Digest>,
    pub sequence: u64,
    pub retirement_root: Digest,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct PolicyRecord {
    pub table: String,
    pub policy: ApplicationTablePolicy,
    pub version: u64,
    pub digest: Digest,
    pub tenant: Option<TenantId>,
    pub first_bound_position: Option<Lsn>,
    pub first_bound_at: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct BoundPolicy {
    pub table: String,
    pub policy: ApplicationTablePolicy,
    pub version: u64,
    pub digest: Digest,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Namespace {
    pub tenant: TenantId,
    pub hub_node: String,
    pub hub_incarnation: Incarnation,
    pub edge_node: String,
    pub edge_incarnation: Incarnation,
}
impl Namespace {
    pub fn encode(&self, e: &mut Encoder) -> Result<()> {
        e.string(self.tenant.as_str());
        e.node(&self.hub_node)?;
        e.raw(&self.hub_incarnation.0.to_be_bytes());
        e.node(&self.edge_node)?;
        e.raw(&self.edge_incarnation.0.to_be_bytes());
        Ok(())
    }
    pub fn key(&self) -> Result<String> {
        let mut e = Encoder::default();
        self.encode(&mut e)?;
        Ok(hex(&e.0))
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SignedBinding {
    pub format: u32,
    pub namespace: Namespace,
    pub tables: Vec<BoundPolicy>,
    pub issuance_state: StateRef,
    pub issuance_source: (Lsn, u32),
    pub signature: Vec<u8>,
}
impl SignedBinding {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        let mut e = Encoder::domain("delivery-policy-binding.v1");
        e.u32(self.format);
        self.namespace.encode(&mut e)?;
        e.u64(self.tables.len() as u64);
        for p in &self.tables {
            e.string(&p.table);
            e.u64(p.version);
            e.raw(&p.digest);
            e.policy(&p.policy)?;
        }
        self.issuance_state.encode(&mut e);
        e.u64(self.issuance_source.0.0);
        e.u32(self.issuance_source.1);
        Ok(e.0)
    }
    pub fn verify(&self) -> Result<()> {
        if self.format != 1
            || self.namespace.hub_incarnation != self.issuance_state.incarnation
            || self.tables.windows(2).any(|w| w[0].table >= w[1].table)
        {
            return Err(invalid());
        }
        for p in &self.tables {
            if policy_digest(&p.table, &p.policy)? != p.digest || p.version == 0 {
                return Err(invalid());
            }
        }
        crate::identity::FabricIdentity::verify_lineage_by_node_id(
            &self.namespace.hub_node,
            &self.bytes()?,
            &self.signature,
        )
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct RowEvidence {
    pub reference: RowRef,
    pub creator: WireRowLineage,
    pub authored_source: (Lsn, u32),
    pub row_digest: Digest,
}
impl RowEvidence {
    pub fn origin_digest(&self, tenant: &TenantId) -> Result<Digest> {
        let mut e = Encoder::domain("delivery-origin-life.v1");
        e.string(tenant.as_str());
        self.reference.encode(&mut e);
        self.encode_creator(&mut e)?;
        Ok(e.digest())
    }
    pub fn encode_creator(&self, e: &mut Encoder) -> Result<()> {
        e.node(&self.creator.author_node_id)?;
        e.raw(&self.creator.author_database_incarnation.0.to_be_bytes());
        e.u64(self.creator.author_local_mutation_position.0);
        e.u64(self.creator.table_generation);
        e.string(&self.creator.lineage_root);
        e.bytes(&self.creator.attestation);
        Ok(())
    }
    pub fn encode_life(&self, e: &mut Encoder) -> Result<()> {
        self.reference.encode(e);
        self.encode_creator(e)?;
        e.u8(0);
        e.u64(0);
        Ok(())
    }
    pub fn encode_projection(&self, tenant: &TenantId, e: &mut Encoder) -> Result<()> {
        self.reference.encode(e);
        e.u64(self.authored_source.0.0);
        e.u32(self.authored_source.1);
        e.raw(&self.origin_digest(tenant)?);
        e.raw(&self.row_digest);
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Seal {
    pub root: RowRef,
    pub root_digest: Digest,
    pub origin_life_digest: Digest,
    pub membership_revision: Uuid,
    pub source: (Lsn, u32),
    pub kind: u8,
    pub registered_count: u64,
    pub unit_digest: Digest,
    pub last_complete: Option<Digest>,
    pub projection_digest: Digest,
    pub authority_digest: Digest,
}
impl Seal {
    pub fn encode(&self, e: &mut Encoder) {
        self.root.encode(e);
        e.raw(&self.root_digest);
        e.raw(&self.origin_life_digest);
        e.raw(self.membership_revision.as_bytes());
        e.u64(self.source.0.0);
        e.u32(self.source.1);
        e.u8(self.kind);
        e.u64(self.registered_count);
        e.raw(&self.unit_digest);
        e.option(self.last_complete.as_ref(), |e, d| e.raw(d));
        e.raw(&self.projection_digest);
        e.raw(&self.authority_digest);
    }
    pub fn bytes(&self) -> Vec<u8> {
        let mut e = Encoder::default();
        self.encode(&mut e);
        e.0
    }
    pub fn digest(&self) -> Digest {
        let mut e = Encoder::domain("delivery-submission-seal.v1");
        self.encode(&mut e);
        e.digest()
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct PolicyEvidence {
    pub table: String,
    pub installed: ApplicationTablePolicy,
    pub authority: u8,
    pub bound: Option<BoundPolicy>,
    pub agreement: bool,
}
impl PolicyEvidence {
    pub fn encode(&self, e: &mut Encoder) -> Result<()> {
        e.string(&self.table);
        e.policy(&self.installed)?;
        e.u8(self.authority);
        if self.authority != 0 {
            let p = self.bound.as_ref().ok_or_else(invalid)?;
            e.string(&p.table);
            e.u64(p.version);
            e.raw(&p.digest);
            e.policy(&p.policy)?;
        }
        e.u8(u8::from(!self.agreement));
        Ok(())
    }
}
/// The bytes a schema author signs over one DDL record's provenance: who wrote
/// it, in which life of their database, at which source position, how many
/// statements that source transaction holds (so the signed ordinal and count
/// fix the complete unit and its order), over which schema generation, and
/// the canonical digest of the statement itself. Both
/// delivery lanes share this one shape -- the custody manifest's
/// `SchemaDependency` below and the ordinary sync lane's `WireDdlAuthor` --
/// so a schema author signs its provenance exactly once, one way. A record
/// that binds no table instance (a trigger or sink drop) passes `None` for
/// both table fields, which a bound record can never collide with because a
/// bound record's generation is never zero.
pub(crate) fn ddl_author_attestation_bytes(
    tenant: &TenantId,
    author_node: &str,
    author_incarnation: Incarnation,
    source_lsn: Lsn,
    ordinal: u32,
    unit_statements: u32,
    table: Option<&str>,
    table_generation: Option<u64>,
    ddl_bytes: &[u8],
    ddl_digest: &[u8],
) -> Result<Vec<u8>> {
    let mut e = Encoder::domain("delivery-schema-dependency.v1");
    e.string(tenant.as_str());
    e.node(author_node)?;
    e.raw(&author_incarnation.0.to_be_bytes());
    e.u64(source_lsn.0);
    e.u32(ordinal);
    e.u32(unit_statements);
    e.string(table.unwrap_or_default());
    e.u64(table_generation.unwrap_or_default());
    e.bytes(ddl_bytes);
    e.bytes(ddl_digest);
    Ok(e.0)
}

/// The actual committed DDL history required to interpret the signed row projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SchemaDependency {
    pub tenant: TenantId,
    pub author_node: String,
    pub author_incarnation: Incarnation,
    pub source_lsn: Lsn,
    pub ordinal: u32,
    pub unit_statements: u32,
    pub table: String,
    pub table_generation: u64,
    pub ddl_bytes: Vec<u8>,
    pub ddl_digest: Vec<u8>,
    pub signature: Vec<u8>,
}
impl SchemaDependency {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        ddl_author_attestation_bytes(
            &self.tenant,
            &self.author_node,
            self.author_incarnation,
            self.source_lsn,
            self.ordinal,
            self.unit_statements,
            Some(&self.table),
            Some(self.table_generation),
            &self.ddl_bytes,
            &self.ddl_digest,
        )
    }
    pub fn verify(&self) -> Result<()> {
        let ddl: crate::protocol::WireDdlChange =
            rmp_serde::from_slice(&self.ddl_bytes).map_err(|_| invalid())?;
        if rmp_serde::to_vec(&ddl).map_err(|_| invalid())? != self.ddl_bytes
            || crate::protocol::canonical_ddl_provenance_digest(
                &ddl,
                self.source_lsn,
                self.ordinal,
                Some(&self.table),
                Some(self.table_generation),
            )
            .map_err(|_| invalid())?
                != self.ddl_digest
        {
            return Err(invalid());
        }
        crate::identity::FabricIdentity::verify_lineage_by_node_id(
            &self.author_node,
            &self.bytes()?,
            &self.signature,
        )
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ManifestRecord {
    pub tenant: TenantId,
    pub edge_node: String,
    pub edge_incarnation: Incarnation,
    pub id: Uuid,
    pub root_life: LocalLifeAnchor,
    pub member_lives: Vec<LocalLifeAnchor>,
    pub seal: Seal,
    pub rows: Vec<RowEvidence>,
    pub schemas: Vec<SchemaDependency>,
    pub policies: Vec<PolicyEvidence>,
    pub binding: Option<SignedBinding>,
    pub signature: Vec<u8>,
}
impl ManifestRecord {
    pub fn projection_bytes(&self) -> Result<Vec<u8>> {
        let mut e = Encoder::default();
        e.u8(0);
        e.u64(self.seal.source.0.0);
        e.u32(self.seal.source.1);
        e.u64(self.schemas.len() as u64);
        for schema in &self.schemas {
            e.bytes(&schema.bytes()?);
            e.bytes(&schema.signature);
        }
        e.u64(self.rows.len() as u64);
        let mut rows: Vec<_> = self.rows.iter().collect();
        rows.sort_by_key(|r| r.reference.order_key());
        for row in rows {
            row.encode_projection(&self.tenant, &mut e)?;
        }
        Ok(e.0)
    }
    pub fn policy_bytes(&self) -> Result<Vec<u8>> {
        let mut e = Encoder::default();
        e.u64(self.policies.len() as u64);
        for p in &self.policies {
            p.encode(&mut e)?;
        }
        e.u8(0);
        Ok(e.0)
    }
    pub fn authority_digest(&self) -> Result<Digest> {
        let mut e = Encoder::domain("delivery-authority-evidence.v1");
        e.raw(&self.policy_bytes()?);
        e.u64(self.rows.len() as u64);
        let mut rows: Vec<_> = self.rows.iter().collect();
        rows.sort_by_key(|r| r.reference.order_key());
        for r in rows {
            r.encode_life(&mut e)?;
        }
        e.u64(0);
        Ok(e.digest())
    }
    pub fn signed_bytes(&self) -> Result<Vec<u8>> {
        let mut e = Encoder::domain("delivery-submission.v1");
        e.string(self.tenant.as_str());
        e.node(&self.edge_node)?;
        e.raw(&self.edge_incarnation.0.to_be_bytes());
        e.u8(0);
        e.raw(self.id.as_bytes());
        self.seal.encode(&mut e);
        e.raw(&self.seal.projection_digest);
        e.raw(&self.seal.authority_digest);
        Ok(e.0)
    }
    pub fn verify(&self) -> Result<()> {
        if let Some(binding) = &self.binding {
            binding.verify()?;
            if binding.namespace.edge_node != self.edge_node
                || binding.namespace.edge_incarnation != self.edge_incarnation
                || binding.namespace.tenant != self.tenant
            {
                return Err(invalid());
            }
        }
        if self.policies.windows(2).any(|w| w[0].table >= w[1].table) {
            return Err(invalid());
        }
        for profile in &self.policies {
            let required = match profile.authority {
                0 if profile.bound.is_none() => None,
                1 if self.binding.is_none() => profile.bound.as_ref(),
                2 if self.binding.is_some() => profile.bound.as_ref(),
                _ => return Err(invalid()),
            };
            if profile.authority != 0 && required.is_none() {
                return Err(invalid());
            }
            if let Some(required) = required {
                if required.table != profile.table
                    || required.version == 0
                    || policy_digest(&required.table, &required.policy)? != required.digest
                    || profile.agreement != (profile.installed == required.policy)
                {
                    return Err(invalid());
                }
                if let Some(binding) = &self.binding
                    && !binding.tables.contains(required)
                {
                    return Err(invalid());
                }
            }
        }
        if !self.policies.iter().any(|p| {
            p.table == self.seal.root.table
                && (p.authority != 0
                    || p.installed.sync_direction == contextdb_core::SyncDirection::None)
                && p.installed.manifest_tables.is_some()
        }) || self
            .rows
            .iter()
            .any(|r| !self.policies.iter().any(|p| p.table == r.reference.table))
            || self
                .policies
                .iter()
                .any(|p| !self.rows.iter().any(|r| r.reference.table == p.table))
        {
            return Err(invalid());
        }
        let mut previous = None;
        for schema in &self.schemas {
            schema.verify()?;
            let position = (schema.source_lsn, schema.ordinal);
            if previous.is_some_and(|p| p >= position)
                || schema.tenant != self.tenant
                || schema.author_node != self.edge_node
                || schema.author_incarnation != self.edge_incarnation
            {
                return Err(invalid());
            }
            previous = Some(position);
        }
        for row in &self.rows {
            if !self.schemas.iter().any(|schema| {
                schema.table == row.reference.table
                    && schema.table_generation == row.creator.table_generation
            }) {
                return Err(invalid());
            }
        }
        if self.rows.is_empty()
            || self.seal.kind != 0
            || self.seal.last_complete.is_some()
            || self.seal.registered_count != self.member_lives.len() as u64
            || self.rows.len() != self.member_lives.len() + 1
            || self.rows[0].reference != self.seal.root
            || self.rows[0].row_digest != self.seal.root_digest
            || self.rows[0].origin_digest(&self.tenant)? != self.seal.origin_life_digest
        {
            return Err(invalid());
        }
        let mut e = Encoder::domain("delivery-materialization-projection.v1");
        e.raw(&self.projection_bytes()?);
        if e.digest() != self.seal.projection_digest
            || self.authority_digest()? != self.seal.authority_digest
        {
            return Err(invalid());
        }
        for row in &self.rows {
            crate::Database::verify_lineage_attestation(
                &self.tenant,
                &row.reference.table,
                &row.reference.key,
                &row.creator,
            )?;
        }
        crate::identity::FabricIdentity::verify_lineage_by_node_id(
            &self.edge_node,
            &self.signed_bytes()?,
            &self.signature,
        )
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DiagnosticBlock {
    pub salt: Digest,
    pub subjects: Vec<(RowRef, u8)>,
    pub conflicts: Vec<Conflict>,
}
impl DiagnosticBlock {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        let mut e = Encoder::default();
        e.raw(&self.salt);
        e.u64(self.subjects.len() as u64);
        for (r, m) in &self.subjects {
            r.encode(&mut e);
            e.u8(*m);
        }
        e.u64(self.conflicts.len() as u64);
        for c in &self.conflicts {
            e.key(&c.natural_key);
            e.u8(super::canonical::diagnostic_resolution(c.resolution));
            e.option(c.reason.as_ref(), |e, v| e.string(v));
            e.option(c.table.as_ref(), |e, v| e.string(v));
            e.option(c.mutation_kind.as_ref(), |e, v| e.string(v));
            e.option(c.winning_author_node_id.as_ref(), |e, v| e.string(v));
            e.option(c.hub_acceptance_position.as_ref(), |e, v| e.u64(v.0));
            e.option(c.refusal_cause.as_ref(), |e, v| {
                e.string(&v.table);
                e.key(&v.natural_key);
            });
        }
        Ok(e.0)
    }
    pub fn commitment(&self) -> Result<Digest> {
        let mut e = Encoder::domain("delivery-diagnostics.v1");
        e.raw(&self.bytes()?);
        Ok(e.digest())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DiagnosticClaim {
    pub subject_count: u64,
    pub conflict_count: u64,
    pub canonical_bytes: u64,
    pub commitment: Digest,
}
impl DiagnosticClaim {
    pub fn from_block(b: &DiagnosticBlock) -> Result<Self> {
        Ok(Self {
            subject_count: b.subjects.len() as u64,
            conflict_count: b.conflicts.len() as u64,
            canonical_bytes: b.bytes()?.len() as u64,
            commitment: b.commitment()?,
        })
    }
    pub fn matches(&self, b: &DiagnosticBlock) -> Result<bool> {
        Ok(self.subject_count == b.subjects.len() as u64
            && self.conflict_count == b.conflicts.len() as u64
            && self.canonical_bytes == b.bytes()?.len() as u64
            && self.commitment == b.commitment()?)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TerminalRecord {
    pub namespace: Namespace,
    pub root: RowRef,
    pub origin_life_digest: Digest,
    pub submission: Uuid,
    pub seal_digest: Digest,
    pub unit_digest: Digest,
    pub source: (Lsn, u32),
    pub kind: u8,
    pub cause: Option<String>,
    pub position: (Lsn, u32),
    pub state: StateRef,
    pub diagnostics: DiagnosticClaim,
    pub signature: Vec<u8>,
}
impl TerminalRecord {
    pub fn signed_bytes(&self) -> Result<Vec<u8>> {
        let mut e = Encoder::default();
        e.bytes(&self.bytes()?);
        e.bytes(&self.signature);
        Ok(e.0)
    }
    pub fn wire(&self, block: &DiagnosticBlock) -> Result<crate::protocol::WireDeliveryOutcome> {
        self.verify()?;
        if !self.diagnostics.matches(block)? {
            return Err(invalid());
        }
        let mut submission = Encoder::default();
        submission.u8(0);
        submission.raw(self.submission.as_bytes());
        let mut source = Encoder::default();
        source.u64(self.source.0.0);
        source.u32(self.source.1);
        Ok(crate::protocol::WireDeliveryOutcome {
            lookup_submission: submission.0,
            lookup_seal_digest: self.seal_digest,
            lookup_origin_life_digest: self.origin_life_digest,
            lookup_source: source.0,
            signed_core: self.signed_bytes()?,
            diagnostic_body: block.bytes()?,
        })
    }

    pub fn bytes(&self) -> Result<Vec<u8>> {
        let mut e = Encoder::domain("delivery-terminal-core.v1");
        e.u32(1);
        self.namespace.encode(&mut e)?;
        self.root.encode(&mut e);
        e.raw(&self.origin_life_digest);
        e.u8(0);
        e.raw(self.submission.as_bytes());
        e.raw(&self.seal_digest);
        e.u8(1);
        e.raw(&self.unit_digest);
        e.u8(1);
        e.u8(0);
        e.u64(self.source.0.0);
        e.u32(self.source.1);
        e.u8(self.kind);
        e.option(self.cause.as_ref(), |e, c| e.string(c));
        e.u64(self.position.0.0);
        e.u32(self.position.1);
        e.raw(&self.state.token);
        e.u8(0);
        e.u64(self.diagnostics.subject_count);
        e.u64(self.diagnostics.conflict_count);
        e.u64(self.diagnostics.canonical_bytes);
        e.raw(&self.diagnostics.commitment);
        Ok(e.0)
    }
    pub fn verify(&self) -> Result<()> {
        if self.kind > 2 || self.namespace.hub_incarnation != self.state.incarnation {
            return Err(invalid());
        }
        crate::identity::FabricIdentity::verify_lineage_by_node_id(
            &self.namespace.hub_node,
            &self.bytes()?,
            &self.signature,
        )
    }
}
/// An actual hub row life and the authenticated source owner referring to it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MaterializedOwner {
    pub namespace: Namespace,
    pub submission: Uuid,
    pub source: RowEvidence,
    pub incumbent: LocalLifeAnchor,
    pub incumbent_reference: RowRef,
    pub incumbent_row_digest: Digest,
    pub root_incumbent: LocalLifeAnchor,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Record {
    CheckpointFrontier(super::checkpoint::Frontier),
    CheckpointNode(super::checkpoint::Node),
    EdgeCheckpoint(super::checkpoint::Checkpoint),
    MaterializedOwner(MaterializedOwner),
    Policy(PolicyRecord),
    Binding(SignedBinding),
    State(StateRecord),
    Control(super::authority::Control),
    Birth(super::authority::Birth),
    Certificate(super::authority::Certificate),
    Destination(super::authority::EdgeAuthority),
    ObjectAdmission(super::authority::Admission),
    Root {
        life: LocalLifeAnchor,
        submission: Uuid,
    },
    Manifest(Box<ManifestRecord>),
    MemberOwner {
        member: LocalLifeAnchor,
        root: LocalLifeAnchor,
        submission: Uuid,
    },
    Terminal {
        edge: bool,
        record: TerminalRecord,
    },
    Diagnostic {
        namespace: Namespace,
        submission: Uuid,
        block: DiagnosticBlock,
    },
    DiagnosticOwner {
        namespace: Namespace,
        submission: Uuid,
        life: LocalLifeAnchor,
    },
    SourceHistory {
        root: LocalLifeAnchor,
        submission: Uuid,
        seal_digest: Digest,
        source: (Lsn, u32),
    },
    PolicyProfile {
        life: LocalLifeAnchor,
        submission: Uuid,
        profile: PolicyEvidence,
    },
    Order {
        namespace: Namespace,
        position: (Lsn, u32),
        submission: Uuid,
    },
}
impl Record {
    pub fn key(&self) -> Result<String> {
        Ok(match self {
            Self::CheckpointFrontier(f) => format!(
                "delivery_prefix_frontier.v1.{}",
                f.checkpoint.namespace.key()?
            ),
            Self::CheckpointNode(n) => n.key()?,
            Self::EdgeCheckpoint(c) => super::checkpoint::edge_key(&c.namespace)?,
            Self::MaterializedOwner(o) => format!(
                "delivery_materialization_owner.v1.{}.{}.{}.{}",
                o.incumbent.key(),
                o.namespace.key()?,
                o.submission.simple(),
                hex(&o.source.reference.bytes())
            ),
            Self::Policy(p) => format!("tenant_table_policy.v1.{}", hex(p.table.as_bytes())),
            Self::Binding(b) => format!("tenant_policy_binding.v1.{}", b.namespace.key()?),
            Self::State(s) => format!("delivery_state_history.v1.{}", hex(&s.state.token)),
            Self::Root { life, .. } => format!("delivery_root.v1.{}", life.key()),
            Self::Manifest(m) => format!("delivery_manifest.v1.{}", m.id.simple()),
            Self::MemberOwner {
                member,
                root,
                submission,
            } => format!(
                "delivery_member_owner.v1.{}.{}.{}",
                member.key(),
                root.key(),
                submission.simple()
            ),
            Self::Terminal { edge, record: r } => format!(
                "delivery_{}_outcome.v1.{}.{}",
                if *edge { "edge" } else { "hub" },
                r.namespace.key()?,
                r.submission.simple()
            ),
            Self::Diagnostic {
                namespace,
                submission,
                ..
            } => format!(
                "delivery_diagnostic.v1.{}.{}",
                namespace.key()?,
                submission.simple()
            ),
            Self::DiagnosticOwner {
                namespace,
                submission,
                life,
            } => format!(
                "delivery_diagnostic_owner.v1.{}.{}.{}",
                namespace.key()?,
                submission.simple(),
                life.key()
            ),
            Self::SourceHistory {
                root, submission, ..
            } => format!(
                "delivery_source_history.v1.{}.{}",
                root.key(),
                submission.simple()
            ),
            Self::PolicyProfile {
                life, submission, ..
            } => format!(
                "delivery_policy_profile.v1.{}.{}",
                life.key(),
                submission.simple()
            ),
            Self::Order {
                namespace,
                position,
                submission: _,
            } => format!(
                "delivery_hub_order.v1.{}.{:016x}.{:08x}",
                namespace.key()?,
                position.0.0,
                position.1
            ),
            Self::ObjectAdmission(a) => a.key()?,
            Self::Control(_) => "delivery_hub_state.v1".into(),
            Self::Birth(b) => format!(
                "delivery_incarnation_birth.v1.{}",
                b.identity.birth_id.simple()
            ),
            Self::Certificate(c) => format!("delivery_peer_evidence.v1.{}", hex(&c.id()?)),
            Self::Destination(_) => "delivery_destination.v1".into(),
        })
    }
    pub fn verify(&self) -> Result<()> {
        match self {
            Self::CheckpointFrontier(f) => f.verify(),
            Self::CheckpointNode(n) => n.verify(),
            Self::EdgeCheckpoint(c) => c.verify(),
            Self::Policy(p) => {
                if p.version == 0 || policy_digest(&p.table, &p.policy)? != p.digest {
                    return Err(invalid());
                }
                Ok(())
            }
            Self::Binding(b) => b.verify(),
            Self::Manifest(m) => m.verify(),
            Self::Terminal { record, .. } => record.verify(),
            Self::Certificate(c) => c.verify(),
            _ => Ok(()),
        }
    }
}
pub(crate) fn owned_key(key: &str) -> bool {
    key.starts_with("tenant_table_policy.v1.")
        || key.starts_with("tenant_policy_binding.v1.")
        || key.starts_with("delivery_")
}

// Signatures are checked at admission; companions are
// validated against the final touched-key overlay, without walking prior units.
pub(crate) fn validate_record(record: &Record, records: &[&Record]) -> Result<()> {
    use super::authority::*;
    match record {
        Record::MaterializedOwner(o) => {
            if !records.iter().any(|r|matches!(r,Record::Manifest(m) if m.id==o.submission&&m.tenant==o.namespace.tenant&&m.edge_node==o.namespace.edge_node&&m.edge_incarnation==o.namespace.edge_incarnation&&m.rows.contains(&o.source))) {return Err(invalid());}
            if !records.iter().any(|r|matches!(r,Record::Terminal{edge:false,record:t} if t.submission==o.submission&&t.namespace==o.namespace&&t.kind!=2)){return Err(invalid());}
        }
        Record::Control(c) => {
            if c.format != 1 {
                return Err(invalid());
            }
            let states: Vec<_> = records
                .iter()
                .filter_map(|r| match r {
                    Record::State(s) if BirthRef::from(&s.state) == c.identity => {
                        Some((*s).clone())
                    }
                    _ => None,
                })
                .collect();
            if history_root(&states) != c.active_history_root
                || !records
                    .iter()
                    .any(|r| matches!(r,Record::Birth(b) if b.identity==c.identity))
            {
                return Err(invalid());
            }
            let birth = records
                .iter()
                .find_map(|r| match r {
                    Record::Birth(b) if b.identity == c.identity => Some(b),
                    _ => None,
                })
                .ok_or_else(invalid)?;
            let retired = retired_descriptors(
                &records.iter().map(|r| (*r).clone()).collect::<Vec<_>>(),
                birth,
            )?;
            validate_retirement_closure(birth, &retired)?;
            if retirement_root(&retired)? != c.retired_history_root
                || states
                    .iter()
                    .any(|s| s.retirement_root != c.retired_history_root)
            {
                return Err(invalid());
            }
            for birth in std::iter::once(birth).chain(&retired) {
                for trigger in &birth.triggers {
                    if !records.iter().any(|r| matches!(r, Record::Certificate(cert) if cert.id().ok() == Some(*trigger))) {
                            return Err(invalid());
                        }
                }
            }
            let mut token = Some(c.head);
            let mut seen = std::collections::BTreeSet::new();
            let mut child_sequence = None;
            while let Some(t) = token {
                if !seen.insert(t) {
                    return Err(invalid());
                }
                let state = states
                    .iter()
                    .find(|s| s.state.token == t)
                    .ok_or_else(invalid)?;
                if child_sequence.is_some_and(|seq| state.sequence.checked_add(1) != Some(seq)) {
                    return Err(invalid());
                }
                child_sequence = Some(state.sequence);
                token = state.parent;
            }
            if seen.len() != states.len() || child_sequence != Some(0) {
                return Err(invalid());
            }
        }
        Record::Root { life, submission } => {
            let m = records
                .iter()
                .find_map(|r| match r {
                    Record::Manifest(m) if m.id == *submission && m.root_life == *life => Some(m),
                    _ => None,
                })
                .ok_or_else(invalid)?;
            if !records.iter().any(|r|matches!(r,Record::SourceHistory{root,submission,seal_digest,..} if root==life&&*submission==m.id&&*seal_digest==m.seal.digest())){return Err(invalid());}
            for member in &m.member_lives {
                if !records.iter().any(|r|matches!(r,Record::MemberOwner{member:owner,root,submission} if owner==member&&root==life&&*submission==m.id)){return Err(invalid());}
            }
        }
        Record::ObjectAdmission(a) => {
            let certificate = records
                .iter()
                .find_map(|r| match r {
                    Record::Certificate(c) if c.id().ok() == Some(a.certificate) => Some(c),
                    _ => None,
                })
                .ok_or_else(invalid)?;
            if certificate.state.state != a.owning_state
                || certificate.tenant != a.authority.namespace.tenant
                || certificate.hub_node != a.authority.namespace.hub_node
                || BirthRef::from(&a.owning_state) != a.authority.birth
            {
                return Err(invalid());
            }
            let exists = records.iter().any(|r| match r {
                Record::Binding(b) => {
                    b.bytes()
                        .ok()
                        .map(|bytes| object_digest(0, &bytes, &b.signature))
                        == Some(a.object_digest)
                }
                Record::Terminal { record: t, .. } => {
                    t.bytes()
                        .ok()
                        .map(|bytes| object_digest(1, &bytes, &t.signature))
                        == Some(a.object_digest)
                }
                _ => false,
            });
            if !exists {
                return Err(invalid());
            }
        }
        Record::Terminal { edge, record: t } => {
            let block = records
                .iter()
                .find_map(|r| match r {
                    Record::Diagnostic {
                        namespace,
                        submission,
                        block,
                    } if *namespace == t.namespace && *submission == t.submission => Some(block),
                    _ => None,
                })
                .ok_or_else(invalid)?;
            if !t.diagnostics.matches(block)? {
                return Err(invalid());
            }
            if *edge {
                    if !records.iter().any(|r|matches!(r,Record::ObjectAdmission(a) if a.object_digest==object_digest(1,&t.bytes().unwrap_or_default(),&t.signature))){return Err(invalid());}
                }else if !records.iter().any(|r|matches!(r,Record::Order{namespace,position,submission} if *namespace==t.namespace&&*position==t.position&&*submission==t.submission)){return Err(invalid());}
        }
        _ => {}
    }
    Ok(())
}

// Compact native COPY plans. A producer must use the source native Value
// copier and exact-capacity containers; derived Clone is not a copy authority.
fn native_sum(parts: impl IntoIterator<Item = Result<usize>>) -> Result<usize> {
    parts.into_iter().try_fold(0usize, |sum, bytes| {
        sum.checked_add(bytes?).ok_or_else(invalid)
    })
}
fn native_vec<T>(items: &[T], child: impl Fn(&T) -> Result<usize>) -> Result<usize> {
    items.iter().try_fold(
        super::preparation::preparation_array_bytes::<T>(items.len())?,
        |sum, item| sum.checked_add(child(item)?).ok_or_else(invalid),
    )
}
pub(crate) fn count_key_native(key: &crate::sync_types::NaturalKey) -> Result<usize> {
    use crate::memory_accounting::preparation_value_copy_bytes;
    native_sum([
        Ok(key.column.len()),
        preparation_value_copy_bytes(&key.value),
        native_vec(&key.rest, |(name, value)| {
            native_sum([Ok(name.len()), preparation_value_copy_bytes(value)])
        }),
    ])
}
pub(crate) fn copy_key_native(
    key: &crate::sync_types::NaturalKey,
) -> Result<crate::sync_types::NaturalKey> {
    let mut rest = Vec::with_capacity(key.rest.len());
    if rest.capacity() != key.rest.len() {
        return Err(invalid());
    }
    for (column, value) in &key.rest {
        rest.push((
            column.as_str().to_owned(),
            crate::memory_accounting::preparation_copy_value(value)?,
        ));
    }
    Ok(crate::sync_types::NaturalKey {
        column: key.column.as_str().to_owned(),
        value: crate::memory_accounting::preparation_copy_value(&key.value)?,
        rest,
    })
}

pub(crate) fn count_reference_native(reference: &RowRef) -> Result<usize> {
    native_sum([Ok(reference.table.len()), count_key_native(&reference.key)])
}
pub(crate) fn count_policy_native(policy: &ApplicationTablePolicy) -> Result<usize> {
    policy
        .manifest_tables
        .as_ref()
        .map_or(Ok(0), |tables| native_vec(tables, |name| Ok(name.len())))
}
fn count_namespace_native(ns: &Namespace) -> Result<usize> {
    native_sum([
        Ok(ns.tenant.as_str().len()),
        Ok(ns.hub_node.len()),
        Ok(ns.edge_node.len()),
    ])
}
fn count_bound_policy_native(policy: &BoundPolicy) -> Result<usize> {
    native_sum([Ok(policy.table.len()), count_policy_native(&policy.policy)])
}
fn count_policy_evidence_native(policy: &PolicyEvidence) -> Result<usize> {
    native_sum([
        Ok(policy.table.len()),
        count_policy_native(&policy.installed),
        policy
            .bound
            .as_ref()
            .map_or(Ok(0), count_bound_policy_native),
    ])
}
fn count_binding_native(binding: &SignedBinding) -> Result<usize> {
    native_sum([
        count_namespace_native(&binding.namespace),
        native_vec(&binding.tables, count_bound_policy_native),
        Ok(binding.signature.len()),
    ])
}
fn count_row_evidence_native(row: &RowEvidence) -> Result<usize> {
    native_sum([
        count_reference_native(&row.reference),
        Ok(row.creator.author_node_id.len()),
        Ok(row.creator.lineage_root.len()),
        Ok(row.creator.attestation.len()),
    ])
}
fn count_schema_native(schema: &SchemaDependency) -> Result<usize> {
    native_sum([
        Ok(schema.tenant.as_str().len()),
        Ok(schema.author_node.len()),
        Ok(schema.table.len()),
        Ok(schema.ddl_bytes.len()),
        Ok(schema.signature.len()),
    ])
}
pub(crate) fn count_manifest_native(manifest: &ManifestRecord) -> Result<usize> {
    native_sum([
        Ok(manifest.tenant.as_str().len()),
        Ok(manifest.edge_node.len()),
        Ok(manifest.root_life.original_table.len()),
        native_vec(&manifest.member_lives, |life| Ok(life.original_table.len())),
        count_reference_native(&manifest.seal.root),
        native_vec(&manifest.rows, count_row_evidence_native),
        native_vec(&manifest.schemas, count_schema_native),
        native_vec(&manifest.policies, count_policy_evidence_native),
        manifest
            .binding
            .as_ref()
            .map_or(Ok(0), count_binding_native),
        Ok(manifest.signature.len()),
    ])
}
fn count_birth_native(birth: &super::authority::Birth) -> Result<usize> {
    native_sum([
        super::preparation::preparation_array_bytes::<Digest>(birth.triggers.len()),
        super::preparation::preparation_array_bytes::<super::authority::BirthRef>(
            birth.retired.len(),
        ),
    ])
}
fn count_certificate_native(certificate: &super::authority::Certificate) -> Result<usize> {
    native_sum([
        Ok(certificate.tenant.as_str().len()),
        Ok(certificate.hub_node.len()),
        count_birth_native(&certificate.birth),
        native_vec(&certificate.retired_births, count_birth_native),
        Ok(certificate.signature.len()),
    ])
}
fn count_checkpoint_native(checkpoint: &super::checkpoint::Checkpoint) -> Result<usize> {
    native_sum([
        count_namespace_native(&checkpoint.namespace),
        Ok(checkpoint.signature.len()),
    ])
}
fn count_terminal_native(terminal: &TerminalRecord) -> Result<usize> {
    native_sum([
        count_namespace_native(&terminal.namespace),
        count_reference_native(&terminal.root),
        Ok(terminal.cause.as_ref().map_or(0, String::len)),
        Ok(terminal.signature.len()),
    ])
}
fn count_diagnostic_native(block: &DiagnosticBlock) -> Result<usize> {
    native_sum([
        native_vec(&block.subjects, |(reference, _)| {
            count_reference_native(reference)
        }),
        native_vec(&block.conflicts, |conflict| {
            native_sum([
                count_key_native(&conflict.natural_key),
                Ok(conflict.reason.as_ref().map_or(0, String::len)),
                Ok(conflict.table.as_ref().map_or(0, String::len)),
                Ok(conflict.mutation_kind.as_ref().map_or(0, String::len)),
                Ok(conflict
                    .winning_author_node_id
                    .as_ref()
                    .map_or(0, String::len)),
                conflict.refusal_cause.as_ref().map_or(Ok(0), |cause| {
                    native_sum([Ok(cause.table.len()), count_key_native(&cause.natural_key)])
                }),
            ])
        }),
    ])
}
pub(crate) fn count_record_native(record: &Record) -> Result<usize> {
    match record {
        Record::CheckpointFrontier(frontier) => native_sum([
            count_checkpoint_native(&frontier.checkpoint),
            super::preparation::preparation_array_bytes::<Option<Digest>>(frontier.peaks.len()),
        ]),
        Record::CheckpointNode(node) => count_namespace_native(&node.namespace),
        Record::EdgeCheckpoint(checkpoint) => count_checkpoint_native(checkpoint),
        Record::MaterializedOwner(owner) => native_sum([
            count_namespace_native(&owner.namespace),
            count_row_evidence_native(&owner.source),
            Ok(owner.incumbent.original_table.len()),
            count_reference_native(&owner.incumbent_reference),
            Ok(owner.root_incumbent.original_table.len()),
        ]),
        Record::Policy(policy) => native_sum([
            Ok(policy.table.len()),
            count_policy_native(&policy.policy),
            Ok(policy
                .tenant
                .as_ref()
                .map_or(0, |tenant| tenant.as_str().len())),
        ]),
        Record::Binding(binding) => count_binding_native(binding),
        Record::State(_) => Ok(0),
        Record::Control(control) => native_sum([
            Ok(control.hub_node.as_ref().map_or(0, String::len)),
            Ok(control
                .tenant
                .as_ref()
                .map_or(0, |tenant| tenant.as_str().len())),
        ]),
        Record::Birth(birth) => count_birth_native(birth),
        Record::Certificate(certificate) => count_certificate_native(certificate),
        Record::Destination(authority) => count_namespace_native(&authority.namespace),
        Record::ObjectAdmission(admission) => {
            count_namespace_native(&admission.authority.namespace)
        }
        Record::Root { life, .. } => Ok(life.original_table.len()),
        Record::Manifest(manifest) => native_sum([
            Ok(std::alloc::Layout::new::<ManifestRecord>().size()),
            count_manifest_native(manifest),
        ]),
        Record::MemberOwner { member, root, .. } => native_sum([
            Ok(member.original_table.len()),
            Ok(root.original_table.len()),
        ]),
        Record::Terminal { record, .. } => count_terminal_native(record),
        Record::Diagnostic {
            namespace, block, ..
        } => native_sum([
            count_namespace_native(namespace),
            count_diagnostic_native(block),
        ]),
        Record::DiagnosticOwner {
            namespace, life, ..
        } => native_sum([
            count_namespace_native(namespace),
            Ok(life.original_table.len()),
        ]),
        Record::SourceHistory { root, .. } => Ok(root.original_table.len()),
        Record::PolicyProfile { life, profile, .. } => native_sum([
            Ok(life.original_table.len()),
            count_policy_evidence_native(profile),
        ]),
        Record::Order { namespace, .. } => count_namespace_native(namespace),
    }
}

fn copy_native_vec<T, U>(items: &[T], mut copy: impl FnMut(&T) -> Result<U>) -> Result<Vec<U>> {
    let mut output = Vec::with_capacity(items.len());
    if output.capacity() != items.len() {
        return Err(invalid());
    }
    for item in items {
        output.push(copy(item)?);
    }
    Ok(output)
}
fn copy_reference_native(reference: &RowRef) -> Result<RowRef> {
    Ok(RowRef {
        table: reference.table.as_str().to_owned(),
        key: copy_key_native(&reference.key)?,
    })
}
fn copy_row_evidence_native(row: &RowEvidence) -> Result<RowEvidence> {
    Ok(RowEvidence {
        reference: copy_reference_native(&row.reference)?,
        creator: row.creator.clone(),
        authored_source: row.authored_source,
        row_digest: row.row_digest,
    })
}
fn copy_manifest_native(manifest: &ManifestRecord) -> Result<ManifestRecord> {
    let seal = &manifest.seal;
    Ok(ManifestRecord {
        tenant: manifest.tenant.clone(),
        edge_node: manifest.edge_node.as_str().to_owned(),
        edge_incarnation: manifest.edge_incarnation,
        id: manifest.id,
        root_life: manifest.root_life.clone(),
        member_lives: copy_native_vec(&manifest.member_lives, |life| Ok(life.clone()))?,
        seal: Seal {
            root: copy_reference_native(&seal.root)?,
            root_digest: seal.root_digest,
            origin_life_digest: seal.origin_life_digest,
            membership_revision: seal.membership_revision,
            source: seal.source,
            kind: seal.kind,
            registered_count: seal.registered_count,
            unit_digest: seal.unit_digest,
            last_complete: seal.last_complete,
            projection_digest: seal.projection_digest,
            authority_digest: seal.authority_digest,
        },
        rows: copy_native_vec(&manifest.rows, copy_row_evidence_native)?,
        schemas: copy_native_vec(&manifest.schemas, |schema| Ok(schema.clone()))?,
        policies: copy_native_vec(&manifest.policies, |policy| Ok(policy.clone()))?,
        binding: manifest.binding.clone(),
        signature: manifest.signature.as_slice().to_vec(),
    })
}
fn copy_terminal_native(terminal: &TerminalRecord) -> Result<TerminalRecord> {
    Ok(TerminalRecord {
        namespace: terminal.namespace.clone(),
        root: copy_reference_native(&terminal.root)?,
        origin_life_digest: terminal.origin_life_digest,
        submission: terminal.submission,
        seal_digest: terminal.seal_digest,
        unit_digest: terminal.unit_digest,
        source: terminal.source,
        kind: terminal.kind,
        cause: terminal.cause.clone(),
        position: terminal.position,
        state: terminal.state.clone(),
        diagnostics: terminal.diagnostics.clone(),
        signature: terminal.signature.as_slice().to_vec(),
    })
}
fn copy_diagnostic_native(block: &DiagnosticBlock) -> Result<DiagnosticBlock> {
    Ok(DiagnosticBlock {
        salt: block.salt,
        subjects: copy_native_vec(&block.subjects, |(reference, kind)| {
            Ok((copy_reference_native(reference)?, *kind))
        })?,
        conflicts: copy_native_vec(&block.conflicts, |conflict| {
            Ok(Conflict {
                natural_key: copy_key_native(&conflict.natural_key)?,
                resolution: conflict.resolution,
                reason: conflict.reason.clone(),
                table: conflict.table.clone(),
                mutation_kind: conflict.mutation_kind.clone(),
                winning_author_node_id: conflict.winning_author_node_id.clone(),
                hub_acceptance_position: conflict.hub_acceptance_position,
                refusal_cause: conflict
                    .refusal_cause
                    .as_ref()
                    .map(|cause| -> Result<crate::sync_types::RefusalCause> {
                        Ok(crate::sync_types::RefusalCause {
                            table: cause.table.as_str().to_owned(),
                            natural_key: copy_key_native(&cause.natural_key)?,
                        })
                    })
                    .transpose()?,
            })
        })?,
    })
}
fn copy_record_native(record: &Record) -> Result<Record> {
    Ok(match record {
        Record::CheckpointFrontier(frontier) => Record::CheckpointFrontier(frontier.clone()),
        Record::CheckpointNode(node) => Record::CheckpointNode(node.clone()),
        Record::EdgeCheckpoint(checkpoint) => Record::EdgeCheckpoint(checkpoint.clone()),
        Record::MaterializedOwner(owner) => Record::MaterializedOwner(MaterializedOwner {
            namespace: owner.namespace.clone(),
            submission: owner.submission,
            source: copy_row_evidence_native(&owner.source)?,
            incumbent: owner.incumbent.clone(),
            incumbent_reference: copy_reference_native(&owner.incumbent_reference)?,
            incumbent_row_digest: owner.incumbent_row_digest,
            root_incumbent: owner.root_incumbent.clone(),
        }),
        Record::Policy(policy) => Record::Policy(policy.clone()),
        Record::Binding(binding) => Record::Binding(binding.clone()),
        Record::State(state) => Record::State(state.clone()),
        Record::Control(control) => Record::Control(control.clone()),
        Record::Birth(birth) => Record::Birth(birth.clone()),
        Record::Certificate(certificate) => Record::Certificate(certificate.clone()),
        Record::Destination(authority) => Record::Destination(authority.clone()),
        Record::ObjectAdmission(admission) => Record::ObjectAdmission(admission.clone()),
        Record::Root { life, submission } => Record::Root {
            life: life.clone(),
            submission: *submission,
        },
        Record::Manifest(manifest) => Record::Manifest(Box::new(copy_manifest_native(manifest)?)),
        Record::MemberOwner {
            member,
            root,
            submission,
        } => Record::MemberOwner {
            member: member.clone(),
            root: root.clone(),
            submission: *submission,
        },
        Record::Terminal { edge, record } => Record::Terminal {
            edge: *edge,
            record: copy_terminal_native(record)?,
        },
        Record::Diagnostic {
            namespace,
            submission,
            block,
        } => Record::Diagnostic {
            namespace: namespace.clone(),
            submission: *submission,
            block: copy_diagnostic_native(block)?,
        },
        Record::DiagnosticOwner {
            namespace,
            submission,
            life,
        } => Record::DiagnosticOwner {
            namespace: namespace.clone(),
            submission: *submission,
            life: life.clone(),
        },
        Record::SourceHistory {
            root,
            submission,
            seal_digest,
            source,
        } => Record::SourceHistory {
            root: root.clone(),
            submission: *submission,
            seal_digest: *seal_digest,
            source: *source,
        },
        Record::PolicyProfile {
            life,
            submission,
            profile,
        } => Record::PolicyProfile {
            life: life.clone(),
            submission: *submission,
            profile: profile.clone(),
        },
        Record::Order {
            namespace,
            position,
            submission,
        } => Record::Order {
            namespace: namespace.clone(),
            position: *position,
            submission: *submission,
        },
    })
}
/// A native copy and its ORIGINAL body grant become one opaque result owner.
pub(crate) fn copy_record_owned(
    db: &crate::Database,
    record: &Record,
) -> Result<super::preparation::OwnedPreparationValue<Record>> {
    let bytes = count_record_native(record)?;
    super::preparation::OwnedPreparationValue::try_produce(db, bytes, || copy_record_native(record))
}

/// Run the actual named MessagePack serializer against a nonallocating sink.
pub(crate) fn count_named_encoding<T: Serialize + ?Sized>(value: &T) -> Result<usize> {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("delivery encoding size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    rmp_serde::encode::write_named(&mut count, value)
        .map_err(|e| contextdb_core::Error::SyncError(format!("delivery metadata encode: {e}")))?;
    Ok(count.0)
}

/// Existing serializer and tags, with a writer that refuses capacity growth.
pub(crate) fn emit_named_encoding<T: Serialize + ?Sized>(
    value: &T,
    output: &mut Vec<u8>,
) -> Result<()> {
    struct Bounded<'a>(&'a mut Vec<u8>);
    impl std::io::Write for Bounded<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.0.capacity() - self.0.len() {
                return Err(std::io::Error::other(
                    "delivery encoding exceeded admitted capacity",
                ));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    rmp_serde::encode::write_named(&mut Bounded(output), value)
        .map_err(|e| contextdb_core::Error::SyncError(format!("delivery metadata encode: {e}")))
}

pub(crate) fn prepare_named_encoding_owned<T: Serialize + ?Sized>(
    db: &crate::Database,
    value: &T,
) -> Result<super::preparation::OwnedPreparationValue<Vec<u8>>> {
    let count = count_named_encoding(value)?;
    super::preparation::OwnedPreparationValue::try_produce(db, count, || {
        let mut encoded = Vec::with_capacity(count);
        if encoded.capacity() != count {
            return Err(invalid());
        }
        emit_named_encoding(value, &mut encoded)?;
        if encoded.len() != count {
            return Err(invalid());
        }
        Ok(encoded)
    })
}

pub(crate) fn encode(record: &Record) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(record)
        .map_err(|e| contextdb_core::Error::SyncError(format!("delivery metadata encode: {e}")))
}
pub(crate) fn decode(bytes: &[u8]) -> Result<Record> {
    let mut reader = std::io::Cursor::new(bytes);
    let value = Record::deserialize(&mut rmp_serde::Deserializer::new(&mut reader))
        .map_err(|e| contextdb_core::Error::SyncError(format!("delivery metadata decode: {e}")))?;
    if reader.position() != bytes.len() as u64 || encode(&value)? != bytes {
        return Err(invalid());
    }
    Ok(value)
}

// Checked private canonical walks; original Legacy methods remain exact.
impl RowRef {
    pub(crate) fn encode_finite(&self, e: &mut super::canonical::FiniteEncoder<'_>) {
        e.reference(&self.table, &self.key);
    }
}
impl LocalLifeAnchor {
    pub(crate) fn encode_finite(&self, e: &mut super::canonical::FiniteEncoder<'_>) {
        e.string(&self.original_table);
        e.u64(self.table_generation);
        e.u64(self.local_row_id);
        e.u64(self.creation_lsn.0);
    }
}
impl StateRef {
    pub(crate) fn encode_finite(&self, e: &mut super::canonical::FiniteEncoder<'_>) {
        e.raw(self.logical_store_id.as_bytes());
        e.raw(&self.incarnation.0.to_be_bytes());
        e.raw(self.birth_id.as_bytes());
        e.raw(&self.token);
    }
}
impl Namespace {
    pub(crate) fn encode_finite(&self, e: &mut super::canonical::FiniteEncoder<'_>) -> Result<()> {
        e.string(self.tenant.as_str());
        e.node(&self.hub_node)?;
        e.raw(&self.hub_incarnation.0.to_be_bytes());
        e.node(&self.edge_node)?;
        e.raw(&self.edge_incarnation.0.to_be_bytes());
        Ok(())
    }
}
impl RowEvidence {
    pub(crate) fn encode_creator_finite(
        &self,
        e: &mut super::canonical::FiniteEncoder<'_>,
    ) -> Result<()> {
        e.node(&self.creator.author_node_id)?;
        e.raw(&self.creator.author_database_incarnation.0.to_be_bytes());
        e.u64(self.creator.author_local_mutation_position.0);
        e.u64(self.creator.table_generation);
        e.string(&self.creator.lineage_root);
        e.bytes(&self.creator.attestation);
        Ok(())
    }
    pub(crate) fn encode_life_finite(
        &self,
        e: &mut super::canonical::FiniteEncoder<'_>,
    ) -> Result<()> {
        self.reference.encode_finite(e);
        self.encode_creator_finite(e)?;
        e.u8(0);
        e.u64(0);
        Ok(())
    }
    pub(crate) fn encode_projection_finite(
        &self,
        tenant: &TenantId,
        e: &mut super::canonical::FiniteEncoder<'_>,
    ) -> Result<()> {
        self.reference.encode_finite(e);
        e.u64(self.authored_source.0.0);
        e.u32(self.authored_source.1);
        e.raw(&self.origin_digest_finite(tenant)?);
        e.raw(&self.row_digest);
        Ok(())
    }
}
impl Seal {
    pub(crate) fn encode_finite(&self, e: &mut super::canonical::FiniteEncoder<'_>) {
        self.root.encode_finite(e);
        e.raw(&self.root_digest);
        e.raw(&self.origin_life_digest);
        e.raw(self.membership_revision.as_bytes());
        e.u64(self.source.0.0);
        e.u32(self.source.1);
        e.u8(self.kind);
        e.u64(self.registered_count);
        e.raw(&self.unit_digest);
        e.option(self.last_complete.as_ref(), |e, d| e.raw(d));
        e.raw(&self.projection_digest);
        e.raw(&self.authority_digest);
    }
}
impl PolicyEvidence {
    pub(crate) fn encode_finite(&self, e: &mut super::canonical::FiniteEncoder<'_>) -> Result<()> {
        e.string(&self.table);
        e.policy(&self.installed)?;
        e.u8(self.authority);
        if self.authority != 0 {
            let p = self.bound.as_ref().ok_or_else(invalid)?;
            e.string(&p.table);
            e.u64(p.version);
            e.raw(&p.digest);
            e.policy(&p.policy)?;
        }
        e.u8(u8::from(!self.agreement));
        Ok(())
    }
}
impl SignedBinding {
    pub(crate) fn bytes_finite(&self, e: &mut super::canonical::FiniteEncoder<'_>) -> Result<()> {
        e.string("delivery-policy-binding.v1");
        e.u32(self.format);
        self.namespace.encode_finite(e)?;
        e.u64(self.tables.len() as u64);
        for p in &self.tables {
            e.string(&p.table);
            e.u64(p.version);
            e.raw(&p.digest);
            e.policy(&p.policy)?;
        }
        self.issuance_state.encode_finite(e);
        e.u64(self.issuance_source.0.0);
        e.u32(self.issuance_source.1);
        Ok(())
    }
}
impl TerminalRecord {
    pub(crate) fn bytes_finite(&self, e: &mut super::canonical::FiniteEncoder<'_>) -> Result<()> {
        e.string("delivery-terminal-core.v1");
        e.u32(1);
        self.namespace.encode_finite(e)?;
        self.root.encode_finite(e);
        e.raw(&self.origin_life_digest);
        e.u8(0);
        e.raw(self.submission.as_bytes());
        e.raw(&self.seal_digest);
        e.u8(1);
        e.raw(&self.unit_digest);
        e.u8(1);
        e.u8(0);
        e.u64(self.source.0.0);
        e.u32(self.source.1);
        e.u8(self.kind);
        e.option(self.cause.as_ref(), |e, c| e.string(c));
        e.u64(self.position.0.0);
        e.u32(self.position.1);
        e.raw(&self.state.token);
        e.u8(0);
        e.u64(self.diagnostics.subject_count);
        e.u64(self.diagnostics.conflict_count);
        e.u64(self.diagnostics.canonical_bytes);
        e.raw(&self.diagnostics.commitment);
        Ok(())
    }
}
impl ManifestRecord {
    pub(crate) fn signed_bytes_finite(
        &self,
        e: &mut super::canonical::FiniteEncoder<'_>,
    ) -> Result<()> {
        e.string("delivery-submission.v1");
        e.string(self.tenant.as_str());
        e.node(&self.edge_node)?;
        e.raw(&self.edge_incarnation.0.to_be_bytes());
        e.u8(0);
        e.raw(self.id.as_bytes());
        self.seal.encode_finite(e);
        e.raw(&self.seal.projection_digest);
        e.raw(&self.seal.authority_digest);
        Ok(())
    }
}
impl DiagnosticBlock {
    pub(crate) fn bytes_finite(&self, e: &mut super::canonical::FiniteEncoder<'_>) -> Result<()> {
        e.raw(&self.salt);
        e.u64(self.subjects.len() as u64);
        for (r, m) in &self.subjects {
            r.encode_finite(e);
            e.u8(*m);
        }
        e.u64(self.conflicts.len() as u64);
        for c in &self.conflicts {
            e.key(&c.natural_key);
            e.u8(super::canonical::diagnostic_resolution(c.resolution));
            e.option(c.reason.as_ref(), |e, v| e.string(v));
            e.option(c.table.as_ref(), |e, v| e.string(v));
            e.option(c.mutation_kind.as_ref(), |e, v| e.string(v));
            e.option(c.winning_author_node_id.as_ref(), |e, v| e.string(v));
            e.option(c.hub_acceptance_position.as_ref(), |e, v| e.u64(v.0));
            e.option(c.refusal_cause.as_ref(), |e, v| {
                e.string(&v.table);
                e.key(&v.natural_key);
            });
        }
        Ok(())
    }
}

impl RowEvidence {
    pub(crate) fn origin_digest_finite(&self, tenant: &TenantId) -> Result<Digest> {
        super::canonical::finite_hash(|e| {
            e.string("delivery-origin-life.v1");
            e.string(tenant.as_str());
            self.reference.encode_finite(e);
            self.encode_creator_finite(e)
        })
    }
}
impl Seal {
    pub(crate) fn digest_finite(&self) -> Result<Digest> {
        super::canonical::finite_hash(|e| {
            e.string("delivery-submission-seal.v1");
            self.encode_finite(e);
            Ok(())
        })
    }
}
impl SchemaDependency {
    pub(crate) fn bytes_finite(&self, e: &mut super::canonical::FiniteEncoder<'_>) -> Result<()> {
        e.string("delivery-schema-dependency.v1");
        e.string(self.tenant.as_str());
        e.node(&self.author_node)?;
        e.raw(&self.author_incarnation.0.to_be_bytes());
        e.u64(self.source_lsn.0);
        e.u32(self.ordinal);
        e.u32(self.unit_statements);
        e.string(&self.table);
        e.u64(self.table_generation);
        e.bytes(&self.ddl_bytes);
        e.bytes(&self.ddl_digest);
        Ok(())
    }
}
pub(crate) fn count_positional_encoding<T: Serialize + ?Sized>(value: &T) -> Result<usize> {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("delivery encoding size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    rmp_serde::encode::write(&mut count, value)
        .map_err(|e| contextdb_core::Error::SyncError(format!("delivery metadata encode: {e}")))?;
    Ok(count.0)
}

/// Existing serializer and tags, with a writer that refuses capacity growth.
pub(crate) fn emit_positional_encoding<T: Serialize + ?Sized>(
    value: &T,
    output: &mut Vec<u8>,
) -> Result<()> {
    struct Bounded<'a>(&'a mut Vec<u8>);
    impl std::io::Write for Bounded<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.0.capacity() - self.0.len() {
                return Err(std::io::Error::other(
                    "delivery encoding exceeded admitted capacity",
                ));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    rmp_serde::encode::write(&mut Bounded(output), value)
        .map_err(|e| contextdb_core::Error::SyncError(format!("delivery metadata encode: {e}")))
}

pub(crate) fn prepare_positional_encoding_owned<T: Serialize + ?Sized>(
    db: &crate::Database,
    value: &T,
) -> Result<super::preparation::OwnedPreparationValue<Vec<u8>>> {
    let count = count_positional_encoding(value)?;
    super::preparation::OwnedPreparationValue::try_produce(db, count, || {
        let mut encoded = Vec::with_capacity(count);
        if encoded.capacity() != count {
            return Err(invalid());
        }
        emit_positional_encoding(value, &mut encoded)?;
        if encoded.len() != count {
            return Err(invalid());
        }
        Ok(encoded)
    })
}

pub(crate) fn positional_digest<T: Serialize + ?Sized>(value: &T) -> Result<Digest> {
    struct Hash(blake3::Hasher);
    impl std::io::Write for Hash {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut sink = Hash(blake3::Hasher::new());
    rmp_serde::encode::write(&mut sink, value).map_err(|_| invalid())?;
    Ok(*sink.0.finalize().as_bytes())
}
// Serialize the existing NaturalKey::pairs shape while borrowing its values.
pub(crate) struct BorrowedKeyPairs<'a>(pub &'a NaturalKey);
impl Serialize for BorrowedKeyPairs<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(1 + self.0.rest.len()))?;
        sequence.serialize_element(&(&self.0.column, &self.0.value))?;
        for pair in &self.0.rest {
            sequence.serialize_element(pair)?;
        }
        sequence.end()
    }
}
// Private verification keeps all canonical and decoded scratch under owners.
impl SignedBinding {
    pub(crate) fn verify_owned(&self, db: &crate::Database) -> Result<()> {
        if self.format != 1
            || self.namespace.hub_incarnation != self.issuance_state.incarnation
            || self.tables.windows(2).any(|w| w[0].table >= w[1].table)
        {
            return Err(invalid());
        }
        for p in &self.tables {
            if super::canonical::finite_policy_digest(&p.table, &p.policy)? != p.digest
                || p.version == 0
            {
                return Err(invalid());
            }
        }
        let bytes = super::canonical::finite_bytes_owned(db, |e| self.bytes_finite(e))?;
        bytes.with_value(|bytes| {
            crate::identity::FabricIdentity::verify_lineage_by_node_id(
                &self.namespace.hub_node,
                bytes,
                &self.signature,
            )
        })
    }
}
impl SchemaDependency {
    pub(crate) fn verify_owned(
        &self,
        db: &crate::Database,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        let memory = crate::read_image_memory::ImageMemory::operation_reader(
            db.bounded_read_accountant(),
            read_memory,
        )?;
        let ddl: crate::protocol::WireDdlChange = memory.decode_msgpack(&self.ddl_bytes)?;
        let encoded = prepare_positional_encoding_owned(db, &ddl)?;
        if !encoded.with_value(|bytes| *bytes == self.ddl_bytes)
            || positional_digest(&(
                "contextdb.sync-ddl-provenance.v6",
                &ddl,
                self.source_lsn.0,
                self.ordinal,
                Some(self.table.as_str()),
                Some(self.table_generation),
            ))?
            .as_slice()
                != self.ddl_digest.as_slice()
        {
            return Err(invalid());
        }
        let bytes = super::canonical::finite_bytes_owned(db, |e| self.bytes_finite(e))?;
        bytes.with_value(|bytes| {
            crate::identity::FabricIdentity::verify_lineage_by_node_id(
                &self.author_node,
                bytes,
                &self.signature,
            )
        })
    }
}
impl TerminalRecord {
    pub(crate) fn verify_owned(&self, db: &crate::Database) -> Result<()> {
        if self.kind > 2 || self.namespace.hub_incarnation != self.state.incarnation {
            return Err(invalid());
        }
        let bytes = super::canonical::finite_bytes_owned(db, |e| self.bytes_finite(e))?;
        bytes.with_value(|bytes| {
            crate::identity::FabricIdentity::verify_lineage_by_node_id(
                &self.namespace.hub_node,
                bytes,
                &self.signature,
            )
        })
    }
}
impl ManifestRecord {
    fn ordered_rows_owned<'a>(
        &'a self,
        db: &crate::Database,
    ) -> Result<
        super::preparation::OwnedPreparationValue<
            Vec<(
                &'a RowEvidence,
                super::preparation::OwnedPreparationValue<Vec<u8>>,
                usize,
            )>,
        >,
    > {
        super::preparation::OwnedPreparationValue::try_produce(
            db,
            super::preparation::preparation_array_bytes::<(
                &RowEvidence,
                super::preparation::OwnedPreparationValue<Vec<u8>>,
                usize,
            )>(self.rows.len())?,
            || {
                let mut rows = Vec::with_capacity(self.rows.len());
                if rows.capacity() != self.rows.len() {
                    return Err(invalid());
                }
                for (position, row) in self.rows.iter().enumerate() {
                    let key = super::canonical::finite_bytes_owned(db, |e| {
                        e.key(&row.reference.key);
                        Ok(())
                    })?;
                    rows.push((row, key, position));
                }
                rows.sort_unstable_by(|a, b| {
                    a.0.reference
                        .table
                        .cmp(&b.0.reference.table)
                        .then_with(|| a.1.with_value(|a| b.1.with_value(|b| a.cmp(b))))
                        .then_with(|| a.2.cmp(&b.2))
                });
                Ok(rows)
            },
        )
    }
    pub(crate) fn projection_digest_owned(&self, db: &crate::Database) -> Result<Digest> {
        let rows = self.ordered_rows_owned(db)?;
        rows.with_value(|rows| {
            super::canonical::finite_hash(|e| {
                e.string("delivery-materialization-projection.v1");
                e.u8(0);
                e.u64(self.seal.source.0.0);
                e.u32(self.seal.source.1);
                e.u64(self.schemas.len() as u64);
                for schema in &self.schemas {
                    e.u64(super::canonical::finite_len(|e| schema.bytes_finite(e))? as u64);
                    schema.bytes_finite(e)?;
                    e.bytes(&schema.signature);
                }
                e.u64(rows.len() as u64);
                for (row, _, _) in rows {
                    row.encode_projection_finite(&self.tenant, e)?;
                }
                Ok(())
            })
        })
    }
    pub(crate) fn authority_digest_owned(&self, db: &crate::Database) -> Result<Digest> {
        let rows = self.ordered_rows_owned(db)?;
        rows.with_value(|rows| {
            super::canonical::finite_hash(|e| {
                e.string("delivery-authority-evidence.v1");
                e.u64(self.policies.len() as u64);
                for p in &self.policies {
                    p.encode_finite(e)?;
                }
                e.u8(0);
                e.u64(rows.len() as u64);
                for (row, _, _) in rows {
                    row.encode_life_finite(e)?;
                }
                e.u64(0);
                Ok(())
            })
        })
    }
}

impl ManifestRecord {
    pub(crate) fn verify_owned(
        &self,
        db: &crate::Database,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        if let Some(binding) = &self.binding {
            binding.verify_owned(db)?;
            if binding.namespace.edge_node != self.edge_node
                || binding.namespace.edge_incarnation != self.edge_incarnation
                || binding.namespace.tenant != self.tenant
            {
                return Err(invalid());
            }
        }
        if self.policies.windows(2).any(|w| w[0].table >= w[1].table) {
            return Err(invalid());
        }
        for profile in &self.policies {
            let required = match profile.authority {
                0 if profile.bound.is_none() => None,
                1 if self.binding.is_none() => profile.bound.as_ref(),
                2 if self.binding.is_some() => profile.bound.as_ref(),
                _ => return Err(invalid()),
            };
            if profile.authority != 0 && required.is_none() {
                return Err(invalid());
            }
            if let Some(required) = required {
                if required.table != profile.table
                    || required.version == 0
                    || super::canonical::finite_policy_digest(&required.table, &required.policy)?
                        != required.digest
                    || profile.agreement != (profile.installed == required.policy)
                {
                    return Err(invalid());
                }
                if let Some(binding) = &self.binding
                    && !binding.tables.contains(required)
                {
                    return Err(invalid());
                }
            }
        }
        if !self.policies.iter().any(|p| {
            p.table == self.seal.root.table
                && (p.authority != 0
                    || p.installed.sync_direction == contextdb_core::SyncDirection::None)
                && p.installed.manifest_tables.is_some()
        }) || self
            .rows
            .iter()
            .any(|r| !self.policies.iter().any(|p| p.table == r.reference.table))
            || self
                .policies
                .iter()
                .any(|p| !self.rows.iter().any(|r| r.reference.table == p.table))
        {
            return Err(invalid());
        }
        let mut previous = None;
        for schema in &self.schemas {
            schema.verify_owned(db, read_memory.clone())?;
            let position = (schema.source_lsn, schema.ordinal);
            if previous.is_some_and(|p| p >= position)
                || schema.tenant != self.tenant
                || schema.author_node != self.edge_node
                || schema.author_incarnation != self.edge_incarnation
            {
                return Err(invalid());
            }
            previous = Some(position);
        }
        for row in &self.rows {
            if !self.schemas.iter().any(|schema| {
                schema.table == row.reference.table
                    && schema.table_generation == row.creator.table_generation
            }) {
                return Err(invalid());
            }
        }
        if self.rows.is_empty()
            || self.seal.kind != 0
            || self.seal.last_complete.is_some()
            || self.seal.registered_count != self.member_lives.len() as u64
            || self.rows.len() != self.member_lives.len() + 1
            || self.rows[0].reference != self.seal.root
            || self.rows[0].row_digest != self.seal.root_digest
            || self.rows[0].origin_digest_finite(&self.tenant)? != self.seal.origin_life_digest
        {
            return Err(invalid());
        }
        if self.projection_digest_owned(db)? != self.seal.projection_digest
            || self.authority_digest_owned(db)? != self.seal.authority_digest
        {
            return Err(invalid());
        }
        for row in &self.rows {
            db.verify_lineage_attestation_owned(
                &self.tenant,
                &row.reference.table,
                &row.reference.key,
                &row.creator,
            )?;
        }
        let bytes = super::canonical::finite_bytes_owned(db, |e| self.signed_bytes_finite(e))?;
        bytes.with_value(|bytes| {
            crate::identity::FabricIdentity::verify_lineage_by_node_id(
                &self.edge_node,
                bytes,
                &self.signature,
            )
        })
    }
}

pub(crate) fn binding_native_bytes_owned(value: &SignedBinding) -> Result<usize> {
    count_binding_native(value)
}

pub(crate) fn bound_policy_native_bytes_owned(value: &BoundPolicy) -> Result<usize> {
    count_bound_policy_native(value)
}
pub(crate) fn policy_evidence_native_bytes_owned(value: &PolicyEvidence) -> Result<usize> {
    count_policy_evidence_native(value)
}
pub(crate) fn row_evidence_native_bytes_owned(value: &RowEvidence) -> Result<usize> {
    count_row_evidence_native(value)
}
pub(crate) fn schema_native_bytes_owned(value: &SchemaDependency) -> Result<usize> {
    count_schema_native_finite(value)
}
pub(crate) fn lineage_native_bytes_owned(value: &WireRowLineage) -> Result<usize> {
    native_sum([
        Ok(value.author_node_id.len()),
        Ok(value.lineage_root.len()),
        Ok(value.attestation.len()),
    ])
}
pub(crate) fn row_change_native_bytes_owned(value: &crate::sync_types::RowChange) -> Result<usize> {
    native_sum([
        Ok(value.table.len()),
        count_key_native(&value.natural_key),
        value.values.iter().try_fold(
            crate::memory_accounting::preparation_row_map_bytes(value.values.len())?,
            |n, (key, value)| {
                n.checked_add(key.len())
                    .and_then(|n| {
                        n.checked_add(
                            crate::memory_accounting::preparation_value_copy_bytes(value).ok()?,
                        )
                    })
                    .ok_or_else(invalid)
            },
        ),
    ])
}

pub(crate) fn namespace_native_bytes_owned(value: &Namespace) -> Result<usize> {
    count_namespace_native(value)
}
pub(crate) fn terminal_native_bytes_owned(value: &TerminalRecord) -> Result<usize> {
    count_terminal_native(value)
}
pub(crate) fn diagnostic_native_bytes_owned(value: &DiagnosticBlock) -> Result<usize> {
    count_diagnostic_native(value)
}
pub(crate) fn conflict_native_bytes_owned(value: &Conflict) -> Result<usize> {
    native_sum([
        count_key_native(&value.natural_key),
        Ok(value.reason.as_ref().map_or(0, String::len)),
        Ok(value.table.as_ref().map_or(0, String::len)),
        Ok(value.mutation_kind.as_ref().map_or(0, String::len)),
        Ok(value.winning_author_node_id.as_ref().map_or(0, String::len)),
        value.refusal_cause.as_ref().map_or(Ok(0), |c| {
            native_sum([Ok(c.table.len()), count_key_native(&c.natural_key)])
        }),
    ])
}

pub(crate) fn record_key_owned(
    db: &crate::Database,
    record: &Record,
) -> Result<super::preparation::OwnedPreparationValue<String>> {
    use super::canonical::{finite_bytes_owned, prepare_string_owned};
    use super::preparation::BorrowedHex;
    let life = |life: &LocalLifeAnchor| {
        finite_bytes_owned(db, |e| {
            life.encode_finite(e);
            Ok(())
        })
    };
    let namespace = |ns: &Namespace| finite_bytes_owned(db, |e| ns.encode_finite(e));
    match record {
        Record::CheckpointFrontier(f) => {
            let ns = namespace(&f.checkpoint.namespace)?;
            ns.with_value(|ns| {
                prepare_string_owned(
                    db,
                    format_args!("delivery_prefix_frontier.v1.{}", BorrowedHex(ns)),
                )
            })
        }
        Record::CheckpointNode(n) => {
            let ns = namespace(&n.namespace)?;
            ns.with_value(|ns| {
                prepare_string_owned(
                    db,
                    format_args!(
                        "delivery_prefix_node.v1.{}.{:02x}.{:016x}",
                        BorrowedHex(ns),
                        n.height,
                        n.start
                    ),
                )
            })
        }
        Record::EdgeCheckpoint(c) => {
            let ns = namespace(&c.namespace)?;
            ns.with_value(|ns| {
                prepare_string_owned(
                    db,
                    format_args!("delivery_edge_checkpoint.v1.{}", BorrowedHex(ns)),
                )
            })
        }
        Record::MaterializedOwner(o) => {
            let incumbent = life(&o.incumbent)?;
            let ns = namespace(&o.namespace)?;
            let reference = finite_bytes_owned(db, |e| {
                o.source.reference.encode_finite(e);
                Ok(())
            })?;
            incumbent.with_value(|incumbent| {
                ns.with_value(|ns| {
                    reference.with_value(|reference| {
                        prepare_string_owned(
                            db,
                            format_args!(
                                "delivery_materialization_owner.v1.{}.{}.{}.{}",
                                BorrowedHex(incumbent),
                                BorrowedHex(ns),
                                o.submission.simple(),
                                BorrowedHex(reference)
                            ),
                        )
                    })
                })
            })
        }
        Record::Policy(p) => prepare_string_owned(
            db,
            format_args!("tenant_table_policy.v1.{}", BorrowedHex(p.table.as_bytes())),
        ),
        Record::Binding(b) => {
            let ns = namespace(&b.namespace)?;
            ns.with_value(|ns| {
                prepare_string_owned(
                    db,
                    format_args!("tenant_policy_binding.v1.{}", BorrowedHex(ns)),
                )
            })
        }
        Record::State(s) => prepare_string_owned(
            db,
            format_args!("delivery_state_history.v1.{}", BorrowedHex(&s.state.token)),
        ),
        Record::Root { life: l, .. } => {
            let bytes = life(l)?;
            bytes.with_value(|bytes| {
                prepare_string_owned(db, format_args!("delivery_root.v1.{}", BorrowedHex(bytes)))
            })
        }
        Record::Manifest(m) => {
            prepare_string_owned(db, format_args!("delivery_manifest.v1.{}", m.id.simple()))
        }
        Record::MemberOwner {
            member,
            root,
            submission,
        } => {
            let member = life(member)?;
            let root = life(root)?;
            member.with_value(|member| {
                root.with_value(|root| {
                    prepare_string_owned(
                        db,
                        format_args!(
                            "delivery_member_owner.v1.{}.{}.{}",
                            BorrowedHex(member),
                            BorrowedHex(root),
                            submission.simple()
                        ),
                    )
                })
            })
        }
        Record::Terminal { edge, record: r } => {
            let ns = namespace(&r.namespace)?;
            ns.with_value(|ns| {
                prepare_string_owned(
                    db,
                    format_args!(
                        "delivery_{}_outcome.v1.{}.{}",
                        if *edge { "edge" } else { "hub" },
                        BorrowedHex(ns),
                        r.submission.simple()
                    ),
                )
            })
        }
        Record::Diagnostic {
            namespace: n,
            submission,
            ..
        } => {
            let ns = namespace(n)?;
            ns.with_value(|ns| {
                prepare_string_owned(
                    db,
                    format_args!(
                        "delivery_diagnostic.v1.{}.{}",
                        BorrowedHex(ns),
                        submission.simple()
                    ),
                )
            })
        }
        Record::DiagnosticOwner {
            namespace: n,
            submission,
            life: l,
        } => {
            let ns = namespace(n)?;
            let life = life(l)?;
            ns.with_value(|ns| {
                life.with_value(|life| {
                    prepare_string_owned(
                        db,
                        format_args!(
                            "delivery_diagnostic_owner.v1.{}.{}.{}",
                            BorrowedHex(ns),
                            submission.simple(),
                            BorrowedHex(life)
                        ),
                    )
                })
            })
        }
        Record::SourceHistory {
            root, submission, ..
        } => {
            let root = life(root)?;
            root.with_value(|root| {
                prepare_string_owned(
                    db,
                    format_args!(
                        "delivery_source_history.v1.{}.{}",
                        BorrowedHex(root),
                        submission.simple()
                    ),
                )
            })
        }
        Record::PolicyProfile {
            life: l,
            submission,
            ..
        } => {
            let life = life(l)?;
            life.with_value(|life| {
                prepare_string_owned(
                    db,
                    format_args!(
                        "delivery_policy_profile.v1.{}.{}",
                        BorrowedHex(life),
                        submission.simple()
                    ),
                )
            })
        }
        Record::Order {
            namespace: n,
            position,
            ..
        } => {
            let ns = namespace(n)?;
            ns.with_value(|ns| {
                prepare_string_owned(
                    db,
                    format_args!(
                        "delivery_hub_order.v1.{}.{:016x}.{:08x}",
                        BorrowedHex(ns),
                        position.0.0,
                        position.1
                    ),
                )
            })
        }
        Record::ObjectAdmission(a) => {
            let bytes = finite_bytes_owned(db, |e| {
                e.raw(&a.object_digest);
                a.authority.encode_finite(e)
            })?;
            bytes.with_value(|bytes| {
                prepare_string_owned(
                    db,
                    format_args!("delivery_object_admission.v1.{}", BorrowedHex(bytes)),
                )
            })
        }
        Record::Control(_) => prepare_string_owned(db, format_args!("delivery_hub_state.v1")),
        Record::Birth(b) => prepare_string_owned(
            db,
            format_args!(
                "delivery_incarnation_birth.v1.{}",
                b.identity.birth_id.simple()
            ),
        ),
        Record::Certificate(c) => prepare_string_owned(
            db,
            format_args!("delivery_peer_evidence.v1.{}", BorrowedHex(&c.id_finite()?)),
        ),
        Record::Destination(_) => prepare_string_owned(db, format_args!("delivery_destination.v1")),
    }
}
impl Record {
    pub(crate) fn verify_owned(
        &self,
        db: &crate::Database,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        match self {
            Self::CheckpointFrontier(f) => f.verify_owned(db),
            Self::CheckpointNode(n) => n.verify_owned(),
            Self::EdgeCheckpoint(c) => c.verify_owned(db),
            Self::Policy(p) => {
                if p.version == 0
                    || super::canonical::finite_policy_digest(&p.table, &p.policy)? != p.digest
                {
                    return Err(invalid());
                }
                Ok(())
            }
            Self::Binding(b) => b.verify_owned(db),
            Self::Manifest(m) => m.verify_owned(db, read_memory),
            Self::Terminal { record, .. } => record.verify_owned(db),
            Self::Certificate(c) => c.verify_owned(db),
            _ => Ok(()),
        }
    }
}

pub(crate) fn certificate_native_bytes_owned(
    value: &super::authority::Certificate,
) -> Result<usize> {
    count_certificate_native(value)
}

pub(crate) fn validate_record_owned(
    db: &crate::Database,
    record: &Record,
    records: &[&Record],
) -> Result<()> {
    use super::authority::*;
    use super::canonical::*;
    use super::preparation::{OwnedPreparationValue, preparation_array_bytes};
    match record {
        Record::MaterializedOwner(o) => {
            if !records.iter().any(|r|matches!(r,Record::Manifest(m) if m.id==o.submission&&m.tenant==o.namespace.tenant&&m.edge_node==o.namespace.edge_node&&m.edge_incarnation==o.namespace.edge_incarnation&&m.rows.contains(&o.source))) {return Err(invalid());}
            if !records.iter().any(|r|matches!(r,Record::Terminal {edge:false,record:t} if t.submission==o.submission&&t.namespace==o.namespace&&t.kind!=2)){return Err(invalid());}
        }
        Record::Control(c) => {
            if c.format != 1 {
                return Err(invalid());
            }
            let count = records
                .iter()
                .filter(|r| matches!(r,Record::State(s) if BirthRef::from(&s.state)==c.identity))
                .count();
            let states = OwnedPreparationValue::try_produce(
                db,
                preparation_array_bytes::<&StateRecord>(count)?,
                || {
                    let mut states = Vec::with_capacity(count);
                    for r in records {
                        if let Record::State(s) = r
                            && BirthRef::from(&s.state) == c.identity
                        {
                            states.push(s);
                        }
                    }
                    Ok(states)
                },
            )?;
            if states.with_value(|states| history_root_owned(db, states))? != c.active_history_root
                || !records
                    .iter()
                    .any(|r| matches!(r,Record::Birth(b) if b.identity==c.identity))
            {
                return Err(invalid());
            }
            let birth = records
                .iter()
                .find_map(|r| match r {
                    Record::Birth(b) if b.identity == c.identity => Some(b),
                    _ => None,
                })
                .ok_or_else(invalid)?;
            let mut bytes = preparation_array_bytes::<Birth>(birth.retired.len())?;
            for id in &birth.retired {
                let b = records
                    .iter()
                    .find_map(|r| match r {
                        Record::Birth(b) if b.identity == *id => Some(b),
                        _ => None,
                    })
                    .ok_or_else(invalid)?;
                bytes = bytes
                    .checked_add(birth_native_bytes_owned(b)?)
                    .ok_or_else(invalid)?;
            }
            let retired = OwnedPreparationValue::try_produce(db, bytes, || {
                let mut retired = Vec::with_capacity(birth.retired.len());
                for id in &birth.retired {
                    retired.push(
                        (*records
                            .iter()
                            .find_map(|r| match r {
                                Record::Birth(b) if b.identity == *id => Some(b),
                                _ => None,
                            })
                            .ok_or_else(invalid)?)
                        .clone(),
                    );
                }
                Ok(retired)
            })?;
            retired.with_value(|retired| validate_retirement_closure_owned(db, birth, retired))?;
            if retired.with_value(|retired| retirement_root_finite(retired))?
                != c.retired_history_root
                || states.with_value(|states| {
                    states
                        .iter()
                        .any(|s| s.retirement_root != c.retired_history_root)
                })
            {
                return Err(invalid());
            }
            retired.with_value(|retired|->Result<()>{for birth in std::iter::once(birth).chain(retired) {
                for trigger in &birth.triggers {if !records.iter().any(|r|matches!(r,Record::Certificate(cert) if cert.id_finite().ok()==Some(*trigger))){return Err(invalid());}}
            }Ok(())})?;
            let mut seen: OwnedPreparationValue<Vec<Digest>> = OwnedPreparationValue::try_produce(
                db,
                preparation_array_bytes::<Digest>(count)?,
                || Ok(Vec::with_capacity(count)),
            )?;
            let mut token = Some(c.head);
            let mut child_sequence = None;
            while let Some(t) = token {
                if seen.with_value(|seen| seen.contains(&t)) {
                    return Err(invalid());
                }
                if seen.with_value(|seen| seen.len() == count) {
                    return Err(invalid());
                }
                seen.push_slot(t);
                let pair = states
                    .with_value(|states| {
                        states
                            .iter()
                            .find(|s| s.state.token == t)
                            .map(|s| (s.sequence, s.parent))
                    })
                    .ok_or_else(invalid)?;
                if child_sequence.is_some_and(|sequence| pair.0.checked_add(1) != Some(sequence)) {
                    return Err(invalid());
                }
                child_sequence = Some(pair.0);
                token = pair.1;
            }
            if seen.with_value(|seen| seen.len()) != count || child_sequence != Some(0) {
                return Err(invalid());
            }
        }
        Record::Root { life, submission } => {
            let m = records
                .iter()
                .find_map(|r| match r {
                    Record::Manifest(m) if m.id == *submission && m.root_life == *life => Some(m),
                    _ => None,
                })
                .ok_or_else(invalid)?;
            let seal = m.seal.digest_finite()?;
            if !records.iter().any(|r|matches!(r,Record::SourceHistory {root,submission,seal_digest,..} if root==life&&*submission==m.id&&*seal_digest==seal)){return Err(invalid());}
            for member in &m.member_lives {
                if !records.iter().any(|r|matches!(r,Record::MemberOwner {member:owner,root,submission} if owner==member&&root==life&&*submission==m.id)){return Err(invalid());}
            }
        }
        Record::ObjectAdmission(a) => {
            let certificate = records
                .iter()
                .find_map(|r| match r {
                    Record::Certificate(c) if c.id_finite().ok() == Some(a.certificate) => Some(c),
                    _ => None,
                })
                .ok_or_else(invalid)?;
            if certificate.state.state != a.owning_state
                || certificate.tenant != a.authority.namespace.tenant
                || certificate.hub_node != a.authority.namespace.hub_node
                || BirthRef::from(&a.owning_state) != a.authority.birth
            {
                return Err(invalid());
            }
            if !records.iter().any(|r| match r {
                Record::Binding(b) => {
                    object_digest_finite(0, |e| b.bytes_finite(e), &b.signature).ok()
                        == Some(a.object_digest)
                }
                Record::Terminal { record: t, .. } => {
                    object_digest_finite(1, |e| t.bytes_finite(e), &t.signature).ok()
                        == Some(a.object_digest)
                }
                _ => false,
            }) {
                return Err(invalid());
            }
        }
        Record::Terminal { edge, record: t } => {
            let block = records
                .iter()
                .find_map(|r| match r {
                    Record::Diagnostic {
                        namespace,
                        submission,
                        block,
                    } if *namespace == t.namespace && *submission == t.submission => Some(block),
                    _ => None,
                })
                .ok_or_else(invalid)?;
            if t.diagnostics.subject_count != block.subjects.len() as u64
                || t.diagnostics.conflict_count != block.conflicts.len() as u64
                || t.diagnostics.canonical_bytes != finite_len(|e| block.bytes_finite(e))? as u64
                || t.diagnostics.commitment
                    != finite_hash(|e| {
                        e.string("delivery-diagnostics.v1");
                        block.bytes_finite(e)
                    })?
            {
                return Err(invalid());
            }
            if *edge {
                let digest = object_digest_finite(1, |e| t.bytes_finite(e), &t.signature)?;
                if !records.iter().any(|r|matches!(r,Record::ObjectAdmission(a) if a.object_digest==digest)){return Err(invalid());}
            }else if !records.iter().any(|r|matches!(r,Record::Order {namespace,position,submission} if *namespace==t.namespace&&*position==t.position&&*submission==t.submission)){return Err(invalid());}
        }
        _ => {}
    }
    Ok(())
}
pub(crate) fn birth_native_bytes_owned(value: &super::authority::Birth) -> Result<usize> {
    count_birth_native(value)
}

pub(crate) fn copy_reference_finite(value: &RowRef) -> Result<RowRef> {
    copy_reference_native(value)
}
pub(crate) fn copy_row_evidence_finite(value: &RowEvidence) -> Result<RowEvidence> {
    copy_row_evidence_native(value)
}
pub(crate) fn copy_manifest_finite(value: &ManifestRecord) -> Result<ManifestRecord> {
    copy_manifest_native(value)
}
pub(crate) fn copy_terminal_finite(value: &TerminalRecord) -> Result<TerminalRecord> {
    copy_terminal_native(value)
}
pub(crate) fn copy_diagnostic_finite(value: &DiagnosticBlock) -> Result<DiagnosticBlock> {
    copy_diagnostic_native(value)
}
pub(crate) fn copy_row_change_finite(
    value: &crate::sync_types::RowChange,
) -> Result<crate::sync_types::RowChange> {
    Ok(crate::sync_types::RowChange {
        table: value.table.as_str().to_owned(),
        natural_key: copy_key_native(&value.natural_key)?,
        values: crate::memory_accounting::preparation_copy_values(&value.values)?,
        deleted: value.deleted,
        lsn: value.lsn,
        created_at: value.created_at,
    })
}

pub(crate) fn copy_conflict_finite(conflict: &Conflict) -> Result<Conflict> {
    Ok(Conflict {
        natural_key: copy_key_native(&conflict.natural_key)?,
        resolution: conflict.resolution,
        reason: conflict.reason.clone(),
        table: conflict.table.clone(),
        mutation_kind: conflict.mutation_kind.clone(),
        winning_author_node_id: conflict.winning_author_node_id.clone(),
        hub_acceptance_position: conflict.hub_acceptance_position,
        refusal_cause: conflict
            .refusal_cause
            .as_ref()
            .map(|cause| -> Result<crate::sync_types::RefusalCause> {
                Ok(crate::sync_types::RefusalCause {
                    table: cause.table.as_str().to_owned(),
                    natural_key: copy_key_native(&cause.natural_key)?,
                })
            })
            .transpose()?,
    })
}
pub(crate) fn copy_finite_vec<T, U>(
    items: &[T],
    copy: impl FnMut(&T) -> Result<U>,
) -> Result<Vec<U>> {
    copy_native_vec(items, copy)
}

// Include the schema digest's Vec payload in the native copy plan.
fn count_schema_native_finite(schema: &SchemaDependency) -> Result<usize> {
    count_schema_native(schema)?
        .checked_add(schema.ddl_digest.len())
        .ok_or_else(invalid)
}
pub(crate) fn count_manifest_native_finite(manifest: &ManifestRecord) -> Result<usize> {
    manifest
        .schemas
        .iter()
        .try_fold(count_manifest_native(manifest)?, |n, schema| {
            n.checked_add(schema.ddl_digest.len()).ok_or_else(invalid)
        })
}
fn count_record_native_finite(record: &Record) -> Result<usize> {
    match record {
        Record::Manifest(manifest) => count_manifest_native_finite(manifest)?
            .checked_add(std::mem::size_of::<ManifestRecord>())
            .ok_or_else(invalid),
        _ => count_record_native(record),
    }
}
pub(crate) fn copy_record_finite_owned(
    db: &crate::Database,
    record: &Record,
) -> Result<super::preparation::OwnedPreparationValue<Record>> {
    super::preparation::OwnedPreparationValue::try_produce(
        db,
        count_record_native_finite(record)?,
        || copy_record_native(record),
    )
}
