//! Borrowed typed shapes of the named MessagePack custody codec.
use super::native_decode::*;
use contextdb_core::{EdgeDiscardMode, HistoryPolicy, Incarnation, Lsn, RetainUnit, SyncDirection};
use serde::Serialize;
use uuid::Uuid;
pub(crate) type Digest = [u8; 32];
#[derive(Clone, Copy, Serialize)]
pub(crate) struct RowRef<'a> {
    pub(crate) table: &'a str,
    pub(crate) key: NaturalKey<'a>,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for RowRef<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            table: Decode::decode(n.field("table")?)?,
            key: Decode::decode(n.field("key")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct LocalLifeAnchor<'a> {
    pub(crate) original_table: &'a str,
    pub(crate) table_generation: u64,
    pub(crate) local_row_id: u64,
    pub(crate) creation_lsn: Lsn,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for LocalLifeAnchor<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            original_table: Decode::decode(n.field("original_table")?)?,
            table_generation: Decode::decode(n.field("table_generation")?)?,
            local_row_id: Decode::decode(n.field("local_row_id")?)?,
            creation_lsn: Decode::decode(n.field("creation_lsn")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct StateRef<'a> {
    pub(crate) logical_store_id: Uuid,
    pub(crate) incarnation: Incarnation,
    pub(crate) birth_id: Uuid,
    pub(crate) token: Digest,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for StateRef<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            logical_store_id: Decode::decode(n.field("logical_store_id")?)?,
            incarnation: Decode::decode(n.field("incarnation")?)?,
            birth_id: Decode::decode(n.field("birth_id")?)?,
            token: Decode::decode(n.field("token")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct StateRecord<'a> {
    pub(crate) state: StateRef<'a>,
    pub(crate) parent: Option<Digest>,
    pub(crate) sequence: u64,
    pub(crate) retirement_root: Digest,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for StateRecord<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            state: Decode::decode(n.field("state")?)?,
            parent: Decode::decode(n.optional_field("parent")?.unwrap_or(NodeView::nil()))?,
            sequence: Decode::decode(n.field("sequence")?)?,
            retirement_root: Decode::decode(n.field("retirement_root")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct PolicyRecord<'a> {
    pub(crate) table: &'a str,
    pub(crate) policy: ApplicationTablePolicy<'a>,
    pub(crate) version: u64,
    pub(crate) digest: Digest,
    pub(crate) tenant: Option<&'a str>,
    pub(crate) first_bound_position: Option<Lsn>,
    pub(crate) first_bound_at: Option<u64>,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for PolicyRecord<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            table: Decode::decode(n.field("table")?)?,
            policy: Decode::decode(n.field("policy")?)?,
            version: Decode::decode(n.field("version")?)?,
            digest: Decode::decode(n.field("digest")?)?,
            tenant: Decode::decode(n.optional_field("tenant")?.unwrap_or(NodeView::nil()))?,
            first_bound_position: Decode::decode(
                n.optional_field("first_bound_position")?
                    .unwrap_or(NodeView::nil()),
            )?,
            first_bound_at: Decode::decode(
                n.optional_field("first_bound_at")?
                    .unwrap_or(NodeView::nil()),
            )?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct BoundPolicy<'a> {
    pub(crate) table: &'a str,
    pub(crate) policy: ApplicationTablePolicy<'a>,
    pub(crate) version: u64,
    pub(crate) digest: Digest,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for BoundPolicy<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            table: Decode::decode(n.field("table")?)?,
            policy: Decode::decode(n.field("policy")?)?,
            version: Decode::decode(n.field("version")?)?,
            digest: Decode::decode(n.field("digest")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct Namespace<'a> {
    pub(crate) tenant: &'a str,
    pub(crate) hub_node: &'a str,
    pub(crate) hub_incarnation: Incarnation,
    pub(crate) edge_node: &'a str,
    pub(crate) edge_incarnation: Incarnation,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for Namespace<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            tenant: Decode::decode(n.field("tenant")?)?,
            hub_node: Decode::decode(n.field("hub_node")?)?,
            hub_incarnation: Decode::decode(n.field("hub_incarnation")?)?,
            edge_node: Decode::decode(n.field("edge_node")?)?,
            edge_incarnation: Decode::decode(n.field("edge_incarnation")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct SignedBinding<'a> {
    pub(crate) format: u32,
    pub(crate) namespace: Namespace<'a>,
    pub(crate) tables: Seq<'a, BoundPolicy<'a>>,
    pub(crate) issuance_state: StateRef<'a>,
    pub(crate) issuance_source: (Lsn, u32),
    pub(crate) signature: Bytes<'a>,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for SignedBinding<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            format: Decode::decode(n.field("format")?)?,
            namespace: Decode::decode(n.field("namespace")?)?,
            tables: Decode::decode(n.field("tables")?)?,
            issuance_state: Decode::decode(n.field("issuance_state")?)?,
            issuance_source: Decode::decode(n.field("issuance_source")?)?,
            signature: Decode::decode(n.field("signature")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct RowEvidence<'a> {
    pub(crate) reference: RowRef<'a>,
    pub(crate) creator: WireRowLineage<'a>,
    pub(crate) authored_source: (Lsn, u32),
    pub(crate) row_digest: Digest,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for RowEvidence<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            reference: Decode::decode(n.field("reference")?)?,
            creator: Decode::decode(n.field("creator")?)?,
            authored_source: Decode::decode(n.field("authored_source")?)?,
            row_digest: Decode::decode(n.field("row_digest")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct Seal<'a> {
    pub(crate) root: RowRef<'a>,
    pub(crate) root_digest: Digest,
    pub(crate) origin_life_digest: Digest,
    pub(crate) membership_revision: Uuid,
    pub(crate) source: (Lsn, u32),
    pub(crate) kind: u8,
    pub(crate) registered_count: u64,
    pub(crate) unit_digest: Digest,
    pub(crate) last_complete: Option<Digest>,
    pub(crate) projection_digest: Digest,
    pub(crate) authority_digest: Digest,
    #[serde(skip)]
    codec_source: NodeView<'a>,
}
impl<'a> Decode<'a> for Seal<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            root: Decode::decode(n.field("root")?)?,
            root_digest: Decode::decode(n.field("root_digest")?)?,
            origin_life_digest: Decode::decode(n.field("origin_life_digest")?)?,
            membership_revision: Decode::decode(n.field("membership_revision")?)?,
            source: Decode::decode(n.field("source")?)?,
            kind: Decode::decode(n.field("kind")?)?,
            registered_count: Decode::decode(n.field("registered_count")?)?,
            unit_digest: Decode::decode(n.field("unit_digest")?)?,
            last_complete: Decode::decode(
                n.optional_field("last_complete")?
                    .unwrap_or(NodeView::nil()),
            )?,
            projection_digest: Decode::decode(n.field("projection_digest")?)?,
            authority_digest: Decode::decode(n.field("authority_digest")?)?,
            codec_source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct PolicyEvidence<'a> {
    pub(crate) table: &'a str,
    pub(crate) installed: ApplicationTablePolicy<'a>,
    pub(crate) authority: u8,
    pub(crate) bound: Option<BoundPolicy<'a>>,
    pub(crate) agreement: bool,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for PolicyEvidence<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            table: Decode::decode(n.field("table")?)?,
            installed: Decode::decode(n.field("installed")?)?,
            authority: Decode::decode(n.field("authority")?)?,
            bound: Decode::decode(n.optional_field("bound")?.unwrap_or(NodeView::nil()))?,
            agreement: Decode::decode(n.field("agreement")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct SchemaDependency<'a> {
    pub(crate) tenant: &'a str,
    pub(crate) author_node: &'a str,
    pub(crate) author_incarnation: Incarnation,
    pub(crate) source_lsn: Lsn,
    pub(crate) ordinal: u32,
    pub(crate) unit_statements: u32,
    pub(crate) table: &'a str,
    pub(crate) table_generation: u64,
    pub(crate) ddl_bytes: Bytes<'a>,
    pub(crate) ddl_digest: Bytes<'a>,
    pub(crate) signature: Bytes<'a>,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for SchemaDependency<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            tenant: Decode::decode(n.field("tenant")?)?,
            author_node: Decode::decode(n.field("author_node")?)?,
            author_incarnation: Decode::decode(n.field("author_incarnation")?)?,
            source_lsn: Decode::decode(n.field("source_lsn")?)?,
            ordinal: Decode::decode(n.field("ordinal")?)?,
            unit_statements: Decode::decode(n.field("unit_statements")?)?,
            table: Decode::decode(n.field("table")?)?,
            table_generation: Decode::decode(n.field("table_generation")?)?,
            ddl_bytes: Decode::decode(n.field("ddl_bytes")?)?,
            ddl_digest: Decode::decode(n.field("ddl_digest")?)?,
            signature: Decode::decode(n.field("signature")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct ManifestRecord<'a> {
    pub(crate) tenant: &'a str,
    pub(crate) edge_node: &'a str,
    pub(crate) edge_incarnation: Incarnation,
    pub(crate) id: Uuid,
    pub(crate) root_life: LocalLifeAnchor<'a>,
    pub(crate) member_lives: Seq<'a, LocalLifeAnchor<'a>>,
    pub(crate) seal: Seal<'a>,
    pub(crate) rows: Seq<'a, RowEvidence<'a>>,
    pub(crate) schemas: Seq<'a, SchemaDependency<'a>>,
    pub(crate) policies: Seq<'a, PolicyEvidence<'a>>,
    pub(crate) binding: Option<SignedBinding<'a>>,
    pub(crate) signature: Bytes<'a>,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for ManifestRecord<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            tenant: Decode::decode(n.field("tenant")?)?,
            edge_node: Decode::decode(n.field("edge_node")?)?,
            edge_incarnation: Decode::decode(n.field("edge_incarnation")?)?,
            id: Decode::decode(n.field("id")?)?,
            root_life: Decode::decode(n.field("root_life")?)?,
            member_lives: Decode::decode(n.field("member_lives")?)?,
            seal: Decode::decode(n.field("seal")?)?,
            rows: Decode::decode(n.field("rows")?)?,
            schemas: Decode::decode(n.field("schemas")?)?,
            policies: Decode::decode(n.field("policies")?)?,
            binding: Decode::decode(n.optional_field("binding")?.unwrap_or(NodeView::nil()))?,
            signature: Decode::decode(n.field("signature")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct DiagnosticBlock<'a> {
    pub(crate) salt: Digest,
    pub(crate) subjects: Seq<'a, (RowRef<'a>, u8)>,
    pub(crate) conflicts: Seq<'a, Conflict<'a>>,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for DiagnosticBlock<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            salt: Decode::decode(n.field("salt")?)?,
            subjects: Decode::decode(n.field("subjects")?)?,
            conflicts: Decode::decode(n.field("conflicts")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct DiagnosticClaim<'a> {
    pub(crate) subject_count: u64,
    pub(crate) conflict_count: u64,
    pub(crate) canonical_bytes: u64,
    pub(crate) commitment: Digest,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for DiagnosticClaim<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            subject_count: Decode::decode(n.field("subject_count")?)?,
            conflict_count: Decode::decode(n.field("conflict_count")?)?,
            canonical_bytes: Decode::decode(n.field("canonical_bytes")?)?,
            commitment: Decode::decode(n.field("commitment")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct TerminalRecord<'a> {
    pub(crate) namespace: Namespace<'a>,
    pub(crate) root: RowRef<'a>,
    pub(crate) origin_life_digest: Digest,
    pub(crate) submission: Uuid,
    pub(crate) seal_digest: Digest,
    pub(crate) unit_digest: Digest,
    pub(crate) source: (Lsn, u32),
    pub(crate) kind: u8,
    pub(crate) cause: Option<&'a str>,
    pub(crate) position: (Lsn, u32),
    pub(crate) state: StateRef<'a>,
    pub(crate) diagnostics: DiagnosticClaim<'a>,
    pub(crate) signature: Bytes<'a>,
    #[serde(skip)]
    codec_source: NodeView<'a>,
}
impl<'a> Decode<'a> for TerminalRecord<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            namespace: Decode::decode(n.field("namespace")?)?,
            root: Decode::decode(n.field("root")?)?,
            origin_life_digest: Decode::decode(n.field("origin_life_digest")?)?,
            submission: Decode::decode(n.field("submission")?)?,
            seal_digest: Decode::decode(n.field("seal_digest")?)?,
            unit_digest: Decode::decode(n.field("unit_digest")?)?,
            source: Decode::decode(n.field("source")?)?,
            kind: Decode::decode(n.field("kind")?)?,
            cause: Decode::decode(n.optional_field("cause")?.unwrap_or(NodeView::nil()))?,
            position: Decode::decode(n.field("position")?)?,
            state: Decode::decode(n.field("state")?)?,
            diagnostics: Decode::decode(n.field("diagnostics")?)?,
            signature: Decode::decode(n.field("signature")?)?,
            codec_source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct MaterializedOwner<'a> {
    pub(crate) namespace: Namespace<'a>,
    pub(crate) submission: Uuid,
    pub(crate) source: RowEvidence<'a>,
    pub(crate) incumbent: LocalLifeAnchor<'a>,
    pub(crate) incumbent_reference: RowRef<'a>,
    pub(crate) incumbent_row_digest: Digest,
    pub(crate) root_incumbent: LocalLifeAnchor<'a>,
    #[serde(skip)]
    codec_source: NodeView<'a>,
}
impl<'a> Decode<'a> for MaterializedOwner<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            namespace: Decode::decode(n.field("namespace")?)?,
            submission: Decode::decode(n.field("submission")?)?,
            source: Decode::decode(n.field("source")?)?,
            incumbent: Decode::decode(n.field("incumbent")?)?,
            incumbent_reference: Decode::decode(n.field("incumbent_reference")?)?,
            incumbent_row_digest: Decode::decode(n.field("incumbent_row_digest")?)?,
            root_incumbent: Decode::decode(n.field("root_incumbent")?)?,
            codec_source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct BirthRef<'a> {
    pub(crate) logical_store_id: Uuid,
    pub(crate) incarnation: Incarnation,
    pub(crate) birth_id: Uuid,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for BirthRef<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            logical_store_id: Decode::decode(n.field("logical_store_id")?)?,
            incarnation: Decode::decode(n.field("incarnation")?)?,
            birth_id: Decode::decode(n.field("birth_id")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct Birth<'a> {
    pub(crate) identity: BirthRef<'a>,
    pub(crate) predecessor: Option<StateRef<'a>>,
    pub(crate) selected_image: Option<StateRef<'a>>,
    pub(crate) triggers: Seq<'a, Digest>,
    pub(crate) retired: Seq<'a, BirthRef<'a>>,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for Birth<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            identity: Decode::decode(n.field("identity")?)?,
            predecessor: Decode::decode(
                n.optional_field("predecessor")?.unwrap_or(NodeView::nil()),
            )?,
            selected_image: Decode::decode(
                n.optional_field("selected_image")?
                    .unwrap_or(NodeView::nil()),
            )?,
            triggers: Decode::decode(n.field("triggers")?)?,
            retired: Decode::decode(n.field("retired")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct Control<'a> {
    pub(crate) format: u32,
    pub(crate) identity: BirthRef<'a>,
    pub(crate) hub_node: Option<&'a str>,
    pub(crate) tenant: Option<&'a str>,
    pub(crate) head: Digest,
    pub(crate) namespace_revision: u64,
    pub(crate) active_history_root: Digest,
    pub(crate) retired_history_root: Digest,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for Control<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            format: Decode::decode(n.field("format")?)?,
            identity: Decode::decode(n.field("identity")?)?,
            hub_node: Decode::decode(n.optional_field("hub_node")?.unwrap_or(NodeView::nil()))?,
            tenant: Decode::decode(n.optional_field("tenant")?.unwrap_or(NodeView::nil()))?,
            head: Decode::decode(n.field("head")?)?,
            namespace_revision: Decode::decode(n.field("namespace_revision")?)?,
            active_history_root: Decode::decode(n.field("active_history_root")?)?,
            retired_history_root: Decode::decode(n.field("retired_history_root")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct Certificate<'a> {
    pub(crate) format: u32,
    pub(crate) tenant: &'a str,
    pub(crate) hub_node: &'a str,
    pub(crate) state: StateRecord<'a>,
    pub(crate) birth: Birth<'a>,
    pub(crate) retirement_digest: Digest,
    pub(crate) retired_births: Seq<'a, Birth<'a>>,
    pub(crate) signature: Bytes<'a>,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for Certificate<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            format: Decode::decode(n.field("format")?)?,
            tenant: Decode::decode(n.field("tenant")?)?,
            hub_node: Decode::decode(n.field("hub_node")?)?,
            state: Decode::decode(n.field("state")?)?,
            birth: Decode::decode(n.field("birth")?)?,
            retirement_digest: Decode::decode(n.field("retirement_digest")?)?,
            retired_births: Decode::decode(n.field("retired_births")?)?,
            signature: Decode::decode(n.field("signature")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct EdgeAuthority<'a> {
    pub(crate) destination_epoch: u64,
    pub(crate) namespace: Namespace<'a>,
    pub(crate) birth: BirthRef<'a>,
    pub(crate) validity_revision: u64,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for EdgeAuthority<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            destination_epoch: Decode::decode(n.field("destination_epoch")?)?,
            namespace: Decode::decode(n.field("namespace")?)?,
            birth: Decode::decode(n.field("birth")?)?,
            validity_revision: Decode::decode(n.field("validity_revision")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct Admission<'a> {
    pub(crate) object_digest: Digest,
    pub(crate) authority: EdgeAuthority<'a>,
    pub(crate) owning_state: StateRef<'a>,
    pub(crate) certificate: Digest,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for Admission<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            object_digest: Decode::decode(n.field("object_digest")?)?,
            authority: Decode::decode(n.field("authority")?)?,
            owning_state: Decode::decode(n.field("owning_state")?)?,
            certificate: Decode::decode(n.field("certificate")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct Checkpoint<'a> {
    pub(crate) namespace: Namespace<'a>,
    pub(crate) count: u64,
    pub(crate) digest: Digest,
    pub(crate) signature: Bytes<'a>,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for Checkpoint<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            namespace: Decode::decode(n.field("namespace")?)?,
            count: Decode::decode(n.field("count")?)?,
            digest: Decode::decode(n.field("digest")?)?,
            signature: Decode::decode(n.field("signature")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct Frontier<'a> {
    pub(crate) checkpoint: Checkpoint<'a>,
    pub(crate) peaks: Seq<'a, Option<Digest>>,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for Frontier<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            checkpoint: Decode::decode(n.field("checkpoint")?)?,
            peaks: Decode::decode(n.field("peaks")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct Node<'a> {
    pub(crate) namespace: Namespace<'a>,
    pub(crate) height: u8,
    pub(crate) start: u64,
    pub(crate) digest: Digest,
    pub(crate) checksum: Digest,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for Node<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            namespace: Decode::decode(n.field("namespace")?)?,
            height: Decode::decode(n.field("height")?)?,
            start: Decode::decode(n.field("start")?)?,
            digest: Decode::decode(n.field("digest")?)?,
            checksum: Decode::decode(n.field("checksum")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct RetentionDeclaration<'a> {
    pub(crate) seconds: u64,
    pub(crate) declared_unit: RetainUnit,
    pub(crate) sync_safe: bool,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for RetentionDeclaration<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            seconds: Decode::decode(n.field("seconds")?)?,
            declared_unit: Decode::decode(n.field("declared_unit")?)?,
            sync_safe: Decode::decode(n.field("sync_safe")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct ApplicationTablePolicy<'a> {
    pub(crate) sync_direction: SyncDirection,
    pub(crate) sync_conflict: Resolution,
    pub(crate) immutable: bool,
    pub(crate) retain: Option<RetentionDeclaration<'a>>,
    pub(crate) history: HistoryPolicy,
    pub(crate) manifest_tables: Option<Seq<'a, &'a str>>,
    pub(crate) edge_discard: EdgeDiscardMode,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for ApplicationTablePolicy<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            sync_direction: Decode::decode(n.field("sync_direction")?)?,
            sync_conflict: Decode::decode(n.field("sync_conflict")?)?,
            immutable: Decode::decode(n.field("immutable")?)?,
            retain: Decode::decode(n.optional_field("retain")?.unwrap_or(NodeView::nil()))?,
            history: Decode::decode(n.field("history")?)?,
            manifest_tables: Decode::decode(
                n.optional_field("manifest_tables")?
                    .unwrap_or(NodeView::nil()),
            )?,
            edge_discard: Decode::decode(n.field("edge_discard")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct NaturalKey<'a> {
    pub(crate) column: &'a str,
    pub(crate) value: Value<'a>,
    pub(crate) rest: Seq<'a, (&'a str, Value<'a>)>,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for NaturalKey<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            column: Decode::decode(n.field("column")?)?,
            value: Decode::decode(n.field("value")?)?,
            rest: Decode::decode(n.field("rest")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct RefusalCause<'a> {
    pub(crate) table: &'a str,
    pub(crate) natural_key: NaturalKey<'a>,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for RefusalCause<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            table: Decode::decode(n.field("table")?)?,
            natural_key: Decode::decode(n.field("natural_key")?)?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct Conflict<'a> {
    pub(crate) natural_key: NaturalKey<'a>,
    pub(crate) resolution: Resolution,
    pub(crate) reason: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) table: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mutation_kind: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) winning_author_node_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) hub_acceptance_position: Option<Lsn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) refusal_cause: Option<RefusalCause<'a>>,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for Conflict<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            natural_key: Decode::decode(n.field("natural_key")?)?,
            resolution: Decode::decode(n.field("resolution")?)?,
            reason: Decode::decode(n.optional_field("reason")?.unwrap_or(NodeView::nil()))?,
            table: Decode::decode(n.optional_field("table")?.unwrap_or(NodeView::nil()))?,
            mutation_kind: Decode::decode(
                n.optional_field("mutation_kind")?
                    .unwrap_or(NodeView::nil()),
            )?,
            winning_author_node_id: Decode::decode(
                n.optional_field("winning_author_node_id")?
                    .unwrap_or(NodeView::nil()),
            )?,
            hub_acceptance_position: Decode::decode(
                n.optional_field("hub_acceptance_position")?
                    .unwrap_or(NodeView::nil()),
            )?,
            refusal_cause: Decode::decode(
                n.optional_field("refusal_cause")?
                    .unwrap_or(NodeView::nil()),
            )?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) struct WireRowLineage<'a> {
    pub(crate) author_node_id: &'a str,
    pub(crate) author_database_incarnation: Incarnation,
    pub(crate) author_local_mutation_position: Lsn,
    pub(crate) table_generation: u64,
    pub(crate) lineage_root: &'a str,
    pub(crate) attestation: Bytes<'a>,
    #[serde(skip)]
    source: NodeView<'a>,
}
impl<'a> Decode<'a> for WireRowLineage<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.map()?;
        Ok(Self {
            author_node_id: Decode::decode(n.field("author_node_id")?)?,
            author_database_incarnation: Decode::decode(n.field("author_database_incarnation")?)?,
            author_local_mutation_position: Decode::decode(
                n.field("author_local_mutation_position")?,
            )?,
            table_generation: Decode::decode(n.field("table_generation")?)?,
            lineage_root: Decode::decode(n.field("lineage_root")?)?,
            attestation: Decode::decode(
                n.optional_field("attestation")?
                    .unwrap_or(NodeView::empty_array()),
            )?,
            source: n,
        })
    }
}
#[derive(Clone, Copy, Serialize)]
pub(crate) enum RecordView<'a> {
    CheckpointFrontier(Frontier<'a>),
    CheckpointNode(Node<'a>),
    EdgeCheckpoint(Checkpoint<'a>),
    MaterializedOwner(MaterializedOwner<'a>),
    Policy(PolicyRecord<'a>),
    Binding(SignedBinding<'a>),
    State(StateRecord<'a>),
    Control(Control<'a>),
    Birth(Birth<'a>),
    Certificate(Certificate<'a>),
    Destination(EdgeAuthority<'a>),
    ObjectAdmission(Admission<'a>),
    Root {
        life: LocalLifeAnchor<'a>,
        submission: Uuid,
    },
    Manifest(ManifestRecord<'a>),
    MemberOwner {
        member: LocalLifeAnchor<'a>,
        root: LocalLifeAnchor<'a>,
        submission: Uuid,
    },
    Terminal {
        edge: bool,
        record: TerminalRecord<'a>,
    },
    Diagnostic {
        namespace: Namespace<'a>,
        submission: Uuid,
        block: DiagnosticBlock<'a>,
    },
    DiagnosticOwner {
        namespace: Namespace<'a>,
        submission: Uuid,
        life: LocalLifeAnchor<'a>,
    },
    SourceHistory {
        root: LocalLifeAnchor<'a>,
        submission: Uuid,
        seal_digest: Digest,
        source: (Lsn, u32),
    },
    PolicyProfile {
        life: LocalLifeAnchor<'a>,
        submission: Uuid,
        profile: PolicyEvidence<'a>,
    },
    Order {
        namespace: Namespace<'a>,
        position: (Lsn, u32),
        submission: Uuid,
    },
}
impl<'a> Decode<'a> for RecordView<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        let (tag, body) = n.variant()?;
        Ok(match tag {
            "CheckpointFrontier" => Self::CheckpointFrontier(Decode::decode(body)?),
            "CheckpointNode" => Self::CheckpointNode(Decode::decode(body)?),
            "EdgeCheckpoint" => Self::EdgeCheckpoint(Decode::decode(body)?),
            "MaterializedOwner" => Self::MaterializedOwner(Decode::decode(body)?),
            "Policy" => Self::Policy(Decode::decode(body)?),
            "Binding" => Self::Binding(Decode::decode(body)?),
            "State" => Self::State(Decode::decode(body)?),
            "Control" => Self::Control(Decode::decode(body)?),
            "Birth" => Self::Birth(Decode::decode(body)?),
            "Certificate" => Self::Certificate(Decode::decode(body)?),
            "Destination" => Self::Destination(Decode::decode(body)?),
            "ObjectAdmission" => Self::ObjectAdmission(Decode::decode(body)?),
            "Root" => Self::Root {
                life: Decode::decode(body.field("life")?)?,
                submission: Decode::decode(body.field("submission")?)?,
            },
            "Manifest" => Self::Manifest(Decode::decode(body)?),
            "MemberOwner" => Self::MemberOwner {
                member: Decode::decode(body.field("member")?)?,
                root: Decode::decode(body.field("root")?)?,
                submission: Decode::decode(body.field("submission")?)?,
            },
            "Terminal" => Self::Terminal {
                edge: Decode::decode(body.field("edge")?)?,
                record: Decode::decode(body.field("record")?)?,
            },
            "Diagnostic" => Self::Diagnostic {
                namespace: Decode::decode(body.field("namespace")?)?,
                submission: Decode::decode(body.field("submission")?)?,
                block: Decode::decode(body.field("block")?)?,
            },
            "DiagnosticOwner" => Self::DiagnosticOwner {
                namespace: Decode::decode(body.field("namespace")?)?,
                submission: Decode::decode(body.field("submission")?)?,
                life: Decode::decode(body.field("life")?)?,
            },
            "SourceHistory" => Self::SourceHistory {
                root: Decode::decode(body.field("root")?)?,
                submission: Decode::decode(body.field("submission")?)?,
                seal_digest: Decode::decode(body.field("seal_digest")?)?,
                source: Decode::decode(body.field("source")?)?,
            },
            "PolicyProfile" => Self::PolicyProfile {
                life: Decode::decode(body.field("life")?)?,
                submission: Decode::decode(body.field("submission")?)?,
                profile: Decode::decode(body.field("profile")?)?,
            },
            "Order" => Self::Order {
                namespace: Decode::decode(body.field("namespace")?)?,
                position: Decode::decode(body.field("position")?)?,
                submission: Decode::decode(body.field("submission")?)?,
            },
            _ => return Err(Invalid::Codec),
        })
    }
}
