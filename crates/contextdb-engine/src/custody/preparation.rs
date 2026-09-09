//! Final delivery projections share the ordinary row and schema commit boundary.
use super::{authority::*, canonical::*, records::*};
use crate::Database;
use crate::custody_types::*;
use crate::sync_types::{ChangeSet, NaturalKey, RowChange};
use contextdb_core::{Error, Incarnation, Result, TenantId};
use std::collections::BTreeMap;
use uuid::Uuid;
pub(crate) type LineageSigner = std::sync::Arc<dyn Fn(&[u8]) -> Result<Vec<u8>> + Send + Sync>;
#[derive(Clone)]
pub(crate) struct SourceRegistration {
    pub tenant: TenantId,
    pub edge: String,
    pub root: RowRef,
    pub members: Vec<RowRef>,
    pub signer: LineageSigner,
}
impl std::fmt::Debug for SourceRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceRegistration")
            .field("root", &self.root)
            .field("members", &self.members)
            .finish_non_exhaustive()
    }
}
impl SourceRegistration {
    pub(crate) fn prepare(
        &self,
        db: &Database,
        ws: &mut contextdb_tx::WriteSet,
    ) -> Result<Vec<Record>> {
        prepare_manifest(
            db,
            ws,
            &self.tenant,
            &self.edge,
            DeliveryManifest {
                root_table: &self.root.table,
                root_key: self.root.key.clone(),
                members: self
                    .members
                    .iter()
                    .map(|r| (r.table.as_str(), r.key.clone()))
                    .collect(),
            },
            &self.signer,
        )
    }
}

pub(crate) type CommitPreparation<'a> =
    dyn FnMut(&Database, &mut contextdb_tx::WriteSet) -> Result<Vec<Record>> + 'a;

pub(crate) fn final_row(
    db: &Database,
    ws: &contextdb_tx::WriteSet,
    reference: &RowRef,
    require_write: bool,
) -> Result<contextdb_core::VersionedRow> {
    let meta = db.table_meta(&reference.table).ok_or_else(invalid)?;
    let mut rows = ws.relational_inserts.iter().filter(|(table, row)| {
        *table == reference.table
            && crate::sync_types::natural_key_from_row_values(&meta, &row.values).as_ref()
                == Some(&reference.key)
    });
    if let Some((_, row)) = rows.next() {
        if rows.next().is_some() {
            return Err(invalid());
        }
        let mut row = row.clone();
        for column in &meta.columns {
            if !matches!(column.column_type, contextdb_core::ColumnType::Vector(_)) {
                continue;
            }
            let vector = ws
                .vector_inserts
                .iter()
                .rev()
                .find(|entry| {
                    entry.index.table == reference.table
                        && entry.index.column == column.name
                        && entry.row_id == row.row_id
                })
                .map(|entry| entry.vector.clone())
                .or_else(|| match row.values.get(&column.name) {
                    Some(contextdb_core::Value::Vector(v)) => Some(v.clone()),
                    _ => None,
                });
            if let Some(vector) = vector {
                row.values.insert(
                    column.name.clone(),
                    contextdb_core::Value::Vector(contextdb_vector::stored_vector_value(
                        &vector,
                        column.quantization,
                    )),
                );
            }
        }
        return Ok(row);
    }
    if require_write {
        return Err(Error::ManifestMemberOutsideTransaction {
            table: reference.table.clone(),
        });
    }
    actual_row(db, reference)
}

pub(crate) fn random_digest() -> [u8; 32] {
    let mut digest = [0; 32];
    getrandom::fill(&mut digest).expect("operating system entropy");
    digest
}
pub(crate) fn next_state(db: &Database, incarnation: Incarnation) -> Result<StateRecord> {
    let records = db.custody_authority()?;
    let control = records.iter().find_map(|r| {
        if let Record::Control(c) = r {
            Some(c)
        } else {
            None
        }
    });
    let previous = control
        .map(|c| {
            records
                .iter()
                .find_map(|r| match r {
                    Record::State(s) if s.state.token == c.head => Some(s),
                    _ => None,
                })
                .ok_or_else(invalid)
        })
        .transpose()?;
    if previous.is_some_and(|s| s.state.incarnation != incarnation) {
        return Err(invalid());
    }
    // Statements 14/15: one immutable identity record per incarnation, not a
    // history entry for each database write or accepted unit.
    if let Some(previous) = previous {
        return Ok(previous.clone());
    }
    Ok(StateRecord {
        state: StateRef {
            logical_store_id: previous
                .map(|s| s.state.logical_store_id)
                .unwrap_or_else(Uuid::new_v4),
            incarnation,
            birth_id: previous
                .map(|s| s.state.birth_id)
                .unwrap_or_else(Uuid::new_v4),
            token: random_digest(),
        },
        parent: previous.map(|s| s.state.token),
        sequence: previous
            .map(|s| s.sequence.checked_add(1).ok_or_else(invalid))
            .transpose()?
            .unwrap_or(0),
        retirement_root: previous
            .map(|s| s.retirement_root)
            .unwrap_or_else(empty_history_root),
    })
}
pub(crate) fn state_records(
    db: &Database,
    state: StateRecord,
    tenant: &TenantId,
    hub: &str,
) -> Result<Vec<Record>> {
    let existing = db.custody_authority()?;
    let birth = BirthRef::from(&state.state);
    let mut states: Vec<_> = existing
        .iter()
        .filter_map(|r| match r {
            Record::State(s) if BirthRef::from(&s.state) == birth => Some(s.clone()),
            _ => None,
        })
        .collect();
    if !states.iter().any(|s| s.state == state.state) {
        states.push(state.clone());
    }
    let mut records = vec![
        Record::Control(Control {
            format: 1,
            identity: birth.clone(),
            hub_node: Some(hub.into()),
            tenant: Some(tenant.clone()),
            head: state.state.token,
            namespace_revision: existing
                .iter()
                .find_map(|r| match r {
                    Record::Control(c) => Some(c.namespace_revision),
                    _ => None,
                })
                .unwrap_or(0),
            active_history_root: history_root(&states),
            retired_history_root: state.retirement_root,
        }),
        Record::State(state),
    ];
    if !existing
        .iter()
        .any(|r| matches!(r,Record::Birth(b) if b.identity==birth))
    {
        records.push(Record::Birth(Birth {
            identity: birth,
            predecessor: None,
            selected_image: None,
            triggers: Vec::new(),
            retired: Vec::new(),
        }));
    }
    Ok(records)
}
pub(crate) fn normalized(
    table: &str,
    mut policy: ApplicationTablePolicy,
) -> Result<ApplicationTablePolicy> {
    if let Some(tables) = &mut policy.manifest_tables {
        tables.sort();
        if tables.is_empty() || tables.windows(2).any(|w| w[0] == w[1]) {
            return Err(invalid());
        }
    }
    policy_digest(table, &policy)?;
    Ok(policy)
}
pub(crate) fn binding(db: &Database, edge: &str) -> Result<SignedBinding> {
    let records = db.custody_authority()?;
    let destination = records.iter().find_map(|r| match r {
        Record::Destination(a) => Some(&a.namespace),
        _ => None,
    });
    let local_hub = records.iter().find_map(|r| match r {
        Record::Control(c) if c.hub_node.is_some() => Some(c),
        _ => None,
    });
    records
        .iter()
        .find_map(|r| match r {
            Record::Binding(b)
                if b.namespace.edge_node == edge
                    && destination.is_none_or(|current| current == &b.namespace)
                    && local_hub
                        .is_none_or(|c| c.identity.incarnation == b.namespace.hub_incarnation) =>
            {
                Some(b.clone())
            }
            _ => None,
        })
        .ok_or_else(invalid)
}
pub(crate) fn actual_row(db: &Database, r: &RowRef) -> Result<contextdb_core::VersionedRow> {
    // Statements 7/9/12: indexed natural-key lookup, never a table scan per member.
    db.custody_row(r)?.ok_or_else(|| {
        Error::SyncError(format!(
            "delivery prerequisite row is absent in {}",
            r.table
        ))
    })
}

pub(crate) fn actual_policy(db: &Database, table: &str) -> Result<ApplicationTablePolicy> {
    let m = db
        .table_meta(table)
        .ok_or_else(|| Error::TableNotFound(table.into()))?;
    Ok(ApplicationTablePolicy {
        sync_direction: m
            .sync_direction
            .unwrap_or(contextdb_core::DEFAULT_SYNC_DIRECTION),
        sync_conflict: m
            .conflict_policy
            .unwrap_or(contextdb_core::DEFAULT_CONFLICT_POLICY),
        immutable: m.immutable,
        retain: m.default_ttl_seconds.map(|seconds| RetentionDeclaration {
            seconds,
            declared_unit: m
                .retain_declared_unit
                .unwrap_or(contextdb_core::RetainUnit::Seconds),
            sync_safe: m.sync_safe,
        }),
        history: m
            .history_policy
            .unwrap_or(contextdb_core::DEFAULT_HISTORY_POLICY),
        manifest_tables: m.delivery_manifest_tables,
        edge_discard: m.edge_discard.unwrap_or_default(),
    })
}
// Statements 7/8: registration validates the writing transaction immediately;
// final preparation repeats it after ordinary commit validation has run.
pub(crate) fn validate_registration(
    db: &Database,
    ws: &contextdb_tx::WriteSet,
    input: &SourceRegistration,
) -> Result<()> {
    let fail = || Error::ManifestIncomplete {
        table: input.root.table.clone(),
    };
    let policy = actual_policy(db, &input.root.table)?;
    let allowed = policy
        .manifest_tables
        .as_ref()
        .ok_or_else(|| Error::ManifestRequired {
            table: input.root.table.clone(),
        })?;
    let root = final_row(db, ws, &input.root, true)?;
    let mut members = input.members.iter().collect::<Vec<_>>();
    members.sort_by_key(|r| r.order_key());
    if members.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(fail());
    }
    for reference in members {
        if reference == &input.root || !allowed.contains(&reference.table) {
            return Err(fail());
        }
        let row = final_row(db, ws, reference, true)?;
        let meta = db.table_meta(&reference.table).ok_or_else(fail)?;
        let single = meta.columns.iter().any(|c| {
            c.references.as_ref().is_some_and(|fk| {
                fk.table == input.root.table
                    && row.values.get(&c.name).is_some_and(|v| {
                        *v != contextdb_core::Value::Null && Some(v) == root.values.get(&fk.column)
                    })
            })
        });
        let composite = meta.composite_foreign_keys.iter().any(|fk| {
            fk.parent_table == input.root.table
                && fk
                    .child_columns
                    .iter()
                    .zip(&fk.parent_columns)
                    .all(|(a, b)| {
                        row.values.get(a).is_some_and(|v| {
                            *v != contextdb_core::Value::Null && Some(v) == root.values.get(b)
                        })
                    })
        });
        if !single && !composite {
            return Err(fail());
        }
    }
    Ok(())
}

pub(crate) fn prepare_manifest(
    db: &Database,
    ws: &mut contextdb_tx::WriteSet,
    tenant: &TenantId,
    edge: &str,
    input: DeliveryManifest<'_>,
    signer: &LineageSigner,
) -> Result<Vec<Record>> {
    let prior = db.custody_authority()?;
    let incarnation = db.sync_incarnation(tenant)?;
    let b = if db.retention_sync_peer().is_some() {
        let binding = binding(db, edge)?;
        binding.verify()?;
        if binding.namespace.tenant != *tenant || binding.namespace.edge_incarnation != incarnation
        {
            return Err(invalid());
        }
        Some(binding)
    } else {
        None
    };
    let authority_policy = |table: &str| -> Option<BoundPolicy> {
        if let Some(binding) = &b {
            binding.tables.iter().find(|p| p.table == table).cloned()
        } else {
            prior.iter().find_map(|record| match record {
                Record::Policy(p) if p.table == table && p.tenant.as_ref() == Some(tenant) => {
                    Some(BoundPolicy {
                        table: p.table.clone(),
                        policy: p.policy.clone(),
                        version: p.version,
                        digest: p.digest,
                    })
                }
                _ => None,
            })
        }
    };
    let root = RowRef {
        table: input.root_table.into(),
        key: input.root_key,
    };
    let mut members: Vec<_> = input
        .members
        .into_iter()
        .map(|(table, key)| RowRef {
            table: table.into(),
            key,
        })
        .collect();
    members.sort_by_key(RowRef::order_key);
    if members.windows(2).any(|w| w[0] == w[1]) || members.contains(&root) {
        return Err(invalid());
    }
    let root_policy = authority_policy(&root.table);
    let local_policy = actual_policy(db, &root.table)?;
    let policy = root_policy
        .as_ref()
        .map(|p| &p.policy)
        .or_else(|| {
            (local_policy.sync_direction == contextdb_core::SyncDirection::None)
                .then_some(&local_policy)
        })
        .ok_or_else(invalid)?;
    let allowed = policy.manifest_tables.as_ref().ok_or_else(invalid)?;
    if members.iter().any(|m| !allowed.contains(&m.table)) {
        return Err(invalid());
    }
    let references: Vec<_> = std::iter::once(root.clone()).chain(members).collect();
    let mut changes = ChangeSet::default();
    let mut local_rows = Vec::new();
    let mut policies = BTreeMap::new();
    for reference in &references {
        let row = final_row(db, ws, reference, true)?;
        let installed = actual_policy(db, &reference.table)?;
        let bound = authority_policy(&reference.table);
        if let Some(p) = &bound
            && p.digest != policy_digest(&reference.table, &installed)?
        {
            return Err(Error::SyncError(format!(
                "installed policy differs from prerequisite binding for {}",
                reference.table
            )));
        }
        policies.insert(
            reference.table.clone(),
            PolicyEvidence {
                table: reference.table.clone(),
                installed,
                authority: if bound.is_some() {
                    if b.is_some() { 2 } else { 1 }
                } else {
                    0
                },
                bound,
                agreement: true,
            },
        );
        changes.rows.push(RowChange {
            table: reference.table.clone(),
            natural_key: reference.key.clone(),
            values: row.values.clone(),
            deleted: false,
            lsn: row.lsn,
            created_at: row.created_at,
        });
        local_rows.push(row);
    }
    let root_lsn = local_rows[0].lsn;
    let prior = db.custody_root(&root)?;
    if prior.iter().any(|r|matches!(r,Record::Manifest(m) if m.seal.root==root&&m.edge_node==edge&&m.seal.source.0>=root_lsn)) {
        return Err(Error::SyncError("a new prerequisite submission requires an actual later source writing transaction".into()));
    }

    if local_rows.iter().any(|r| r.lsn != root_lsn) {
        return Err(Error::SyncError(
            "manifest prerequisites must come from one actual committed writing transaction".into(),
        ));
    }
    let lineages =
        db.prepare_delivery_row_lineages(ws, &changes, tenant, edge, incarnation, signer.as_ref())?;
    let mut evidence = Vec::new();
    let mut lives = Vec::new();
    let mut content = Vec::new();
    for ((reference, row), change) in references.iter().zip(&local_rows).zip(&changes.rows) {
        let lineage = lineages
            .get(&(
                reference.table.clone(),
                rmp_serde::to_vec(&reference.key).map_err(|_| invalid())?,
                change.lsn,
            ))
            .cloned()
            .ok_or_else(invalid)?;
        evidence.push(RowEvidence {
            reference: reference.clone(),
            creator: lineage.clone(),
            authored_source: (row.lsn, 0),
            row_digest: full_row_digest(&row.values),
        });
        lives.push(LocalLifeAnchor {
            original_table: reference.table.clone(),
            table_generation: lineage.table_generation,
            local_row_id: row.row_id.0,
            creation_lsn: lineage.author_local_mutation_position,
        });
        content.push(content_row_digest(&row.values, &[]));
    }
    // Validate the real schema relationships, rather than assuming a shared name means membership.
    for (reference, row) in references.iter().zip(&local_rows).skip(1) {
        let meta = db.table_meta(&reference.table).ok_or_else(invalid)?;
        let root_values = &local_rows[0].values;
        let single = meta.columns.iter().any(|c| {
            c.references.as_ref().is_some_and(|fk| {
                fk.table == root.table && row.values.get(&c.name) == root_values.get(&fk.column)
            })
        });
        let composite = meta.composite_foreign_keys.iter().any(|fk| {
            fk.parent_table == root.table
                && fk
                    .child_columns
                    .iter()
                    .zip(&fk.parent_columns)
                    .all(|(a, b)| row.values.get(a) == root_values.get(b))
        });
        if !single && !composite {
            return Err(Error::SyncError(
                "manifest prerequisite member does not reference its root".into(),
            ));
        }
    }
    let mut unit = Encoder::domain("delivery-content-unit.v1");
    root.encode(&mut unit);
    unit.raw(&content[0]);
    unit.u64((references.len() - 1) as u64);
    for (r, d) in references.iter().zip(content).skip(1) {
        r.encode(&mut unit);
        unit.raw(&d);
    }
    // Statements 6/9/15/17: the unit signs its current schema image at its real commit,
    // so a locally retained table needs no replay of a former peer's DDL history.
    let mut schemas = Vec::new();
    let tables = evidence
        .iter()
        .map(|r| (r.reference.table.clone(), r.creator.table_generation))
        .collect::<std::collections::BTreeSet<_>>();
    for (ordinal, (table, generation)) in tables.into_iter().enumerate() {
        let meta = db.table_meta(&table).ok_or_else(invalid)?;
        let ddl = crate::protocol::WireDdlChange::from(crate::database::ddl_change_from_meta(
            &table, &meta,
        ));
        let source_lsn = ws.commit_lsn.ok_or_else(invalid)?;
        let ordinal = u32::try_from(ordinal).map_err(|_| invalid())?;
        let mut schema = SchemaDependency {
            tenant: tenant.clone(),
            author_node: edge.into(),
            author_incarnation: incarnation,
            source_lsn,
            ordinal,
            table: table.clone(),
            table_generation: generation,
            ddl_digest: crate::protocol::canonical_ddl_provenance_digest(
                &ddl,
                source_lsn,
                ordinal,
                Some(&table),
                Some(generation),
            )
            .map_err(|_| invalid())?,
            ddl_bytes: rmp_serde::to_vec(&ddl).map_err(|_| invalid())?,
            signature: Vec::new(),
        };
        schema.signature = signer(&schema.bytes()?)?;
        schemas.push(schema);
    }
    let mut m = ManifestRecord {
        tenant: tenant.clone(),
        edge_node: edge.into(),
        edge_incarnation: incarnation,
        id: Uuid::new_v4(),
        root_life: lives[0].clone(),
        member_lives: lives[1..].to_vec(),
        seal: Seal {
            root: root.clone(),
            root_digest: evidence[0].row_digest,
            origin_life_digest: evidence[0].origin_digest(tenant)?,
            membership_revision: Uuid::new_v4(),
            source: (root_lsn, 0),
            kind: 0,
            registered_count: (references.len() - 1) as u64,
            unit_digest: unit.digest(),
            last_complete: None,
            projection_digest: [0; 32],
            authority_digest: [0; 32],
        },
        rows: evidence,
        schemas,
        policies: policies.into_values().collect(),
        binding: b,
        signature: Vec::new(),
    };
    let mut projection = Encoder::domain("delivery-materialization-projection.v1");
    projection.raw(&m.projection_bytes()?);
    m.seal.projection_digest = projection.digest();
    m.seal.authority_digest = m.authority_digest()?;
    m.signature = signer(&m.signed_bytes()?)?;
    m.verify()?;
    {
        let mut records = vec![
            Record::Root {
                life: m.root_life.clone(),
                submission: m.id,
            },
            Record::Manifest(Box::new(m.clone())),
        ];
        for member in &m.member_lives {
            records.push(Record::MemberOwner {
                member: member.clone(),
                root: m.root_life.clone(),
                submission: m.id,
            });
        }
        records.push(Record::SourceHistory {
            root: m.root_life.clone(),
            submission: m.id,
            seal_digest: m.seal.digest(),
            source: m.seal.source,
        });
        for (row, life) in m
            .rows
            .iter()
            .zip(std::iter::once(&m.root_life).chain(&m.member_lives))
        {
            records.push(Record::PolicyProfile {
                life: life.clone(),
                submission: m.id,
                profile: m
                    .policies
                    .iter()
                    .find(|p| p.table == row.reference.table)
                    .cloned()
                    .ok_or_else(invalid)?,
            });
        }
        Ok(records)
    }
}

/// Advance the complete branch without signing or exposing a new public mutation door.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TerminalInput {
    pub tenant_id: TenantId,
    pub edge_incarnation: Incarnation,
    pub manifest: ManifestRecord,
    pub rows: Vec<RowChange>,
    pub kind: u8,
    pub cause: Option<String>,
}

pub(crate) fn prepare_terminal(
    db: &Database,
    ws: &mut contextdb_tx::WriteSet,
    tenant: TenantId,
    hub: String,
    edge: String,
    request: TerminalInput,
    signer: &LineageSigner,
) -> Result<Vec<Record>> {
    prepare_terminal_at(db, ws, tenant, hub, edge, request, signer, None, 0)
}

#[allow(clippy::too_many_arguments)]
fn prepare_terminal_at(
    db: &Database,
    ws: &mut contextdb_tx::WriteSet,
    tenant: TenantId,
    hub: String,
    edge: String,
    request: TerminalInput,
    signer: &LineageSigner,
    reserved_state: Option<StateRecord>,
    ordinal: u32,
) -> Result<Vec<Record>> {
    let m = request.manifest;
    m.verify()?;
    if request.kind != 2
        && (request.rows.len() != m.rows.len()
            || m.rows.iter().any(|e| {
                request
                    .rows
                    .iter()
                    .filter(|r| {
                        r.table == e.reference.table
                            && r.natural_key == e.reference.key
                            && !r.deleted
                            && full_row_digest(&r.values) == e.row_digest
                            && r.lsn == e.authored_source.0
                    })
                    .count()
                    != 1
            }))
    {
        return Err(invalid());
    }
    if request.tenant_id != tenant
        || m.tenant != tenant
        || m.edge_node != edge
        || m.edge_incarnation != request.edge_incarnation
        || request.kind > 2
    {
        return Err(invalid());
    }
    if request.kind == 0 {
        for evidence in &m.rows {
            let actual = final_row(db, ws, &evidence.reference, true)?;
            if full_row_digest(&actual.values) != evidence.row_digest {
                return Err(invalid());
            }
        }
    }
    let b = binding(db, &edge)?;
    if b.namespace.hub_node != hub || b.namespace.hub_incarnation != db.sync_incarnation(&tenant)? {
        return Err(invalid());
    }
    // An accepted terminal requires every source row in this exact final write set.
    // Equivalence instead validates the complete existing materialization.
    if request.kind < 2 {
        let mut unit = Encoder::domain("delivery-content-unit.v1");
        m.seal.root.encode(&mut unit);
        let root_row = final_row(db, ws, &m.seal.root, request.kind == 0)?;
        unit.raw(&content_row_digest(&root_row.values, &[]));
        unit.u64(m.seal.registered_count);
        for row in m.rows.iter().skip(1) {
            let actual = final_row(db, ws, &row.reference, request.kind == 0)?;
            row.reference.encode(&mut unit);
            unit.raw(&content_row_digest(&actual.values, &[]));
        }
        if unit.digest() != m.seal.unit_digest {
            return Err(Error::SyncError(
                "hub does not hold the prerequisite complete content".into(),
            ));
        }
    }
    let conflicts = if request.kind == 2 {
        match request.cause.as_deref() {
            // Statements 12/16 and root 14: diagnose the actual held rows. A
            // terminal's sender can be a relay or an equivalent writer, and a
            // winnerless member must name its conflicting sibling instead.
            Some(cause @ ("unit_digest_mismatch" | "keep_first_refused")) => {
                let mut conflicts = db.dependency_unit_refusal_conflicts(
                    &request.rows,
                    &crate::sync_types::ConflictPolicies::uniform(
                        crate::sync_types::ConflictPolicy::InsertIfNotExists,
                    ),
                    Some(&hub),
                )?;
                if conflicts.len() != request.rows.len()
                    || conflicts.iter().any(|c| {
                        if c.refusal_cause.is_some() {
                            c.winning_author_node_id.is_some()
                                || c.hub_acceptance_position.is_some()
                        } else {
                            c.winning_author_node_id.is_none()
                                || c.hub_acceptance_position.is_none()
                        }
                    })
                {
                    return Err(invalid());
                }
                for conflict in &mut conflicts {
                    conflict.reason = Some(cause.into());
                }
                conflicts
            }
            Some("manifest_incomplete" | "member_digest_mismatch" | "binding_mismatch") => {
                Vec::new()
            }
            _ => return Err(invalid()),
        }
    } else {
        if request.cause.is_some() {
            return Err(invalid());
        }
        Vec::new()
    };
    // Resolve actual incumbent row lives, including equivalent source aliases.
    let mut incumbent_lives = Vec::new();
    for (source, source_life) in m
        .rows
        .iter()
        .zip(std::iter::once(&m.root_life).chain(&m.member_lives))
    {
        if request.kind == 2 {
            incumbent_lives.push((source_life.clone(), source.row_digest));
            continue;
        }
        let row = final_row(db, ws, &source.reference, request.kind == 0)?;
        incumbent_lives.push((
            LocalLifeAnchor {
                original_table: source.reference.table.clone(),
                table_generation: db.durable_lineage_table_generation(&source.reference.table)?,
                local_row_id: row.row_id.0,
                creation_lsn: row.lsn,
            },
            full_row_digest(&row.values),
        ));
    }
    if request.kind == 1 {
        let existing = db.custody_root(&m.seal.root)?;
        let complete_incumbent = existing.iter().filter_map(|r| match r { Record::Terminal { edge: false, record: t } if t.kind != 2 && t.root == m.seal.root && t.unit_digest == m.seal.unit_digest && t.namespace.hub_node == hub => Some(t), _ => None })
            .any(|t| m.rows.iter().zip(&incumbent_lives).all(|(source, (life, digest))| existing.iter().any(|r| matches!(r, Record::MaterializedOwner(o) if o.namespace == t.namespace && o.submission == t.submission && o.incumbent_reference == source.reference && o.incumbent == *life && o.incumbent_row_digest == *digest))));
        // Statement 12: metadata ownership is unnecessary for a directly held
        // complete unit. Revalidate that unit inside the terminal commit gate.
        if !complete_incumbent
            && db.custody_incumbent_digest(&m.seal.root)? != Some(m.seal.unit_digest)
        {
            return Err(Error::SyncError(
                "equivalence requires a complete authenticated incumbent materialization".into(),
            ));
        }
    }
    let state = match reserved_state {
        Some(state) => state,
        None => next_state(db, b.namespace.hub_incarnation)?,
    };
    let mut subjects: Vec<_> = m.rows.iter().map(|r| (r.reference.clone(), 0)).collect();
    subjects.sort_by_key(|(r, _)| r.order_key());
    let block = DiagnosticBlock {
        salt: random_digest(),
        subjects,
        conflicts,
    };
    let lsn = ws.commit_lsn.ok_or_else(invalid)?;
    {
        let mut t = TerminalRecord {
            namespace: b.namespace.clone(),
            root: m.seal.root.clone(),
            origin_life_digest: m.seal.origin_life_digest,
            submission: m.id,
            seal_digest: m.seal.digest(),
            unit_digest: m.seal.unit_digest,
            source: m.seal.source,
            kind: request.kind,
            cause: request.cause,
            position: (lsn, ordinal),
            state: state.state.clone(),
            diagnostics: DiagnosticClaim::from_block(&block)?,
            signature: Vec::new(),
        };
        super::checkpoint::append(db, ws, &t.namespace, signer)?;
        t.signature = signer(&t.bytes()?)?;
        t.verify()?;
        let mut records = state_records(db, state, &t.namespace.tenant, &t.namespace.hub_node)?;
        for (source, (incumbent, digest)) in m.rows.iter().zip(&incumbent_lives) {
            // A refused source owns diagnostic subjects, never the incumbent materialization.
            if t.kind != 2 {
                records.push(Record::MaterializedOwner(MaterializedOwner {
                    namespace: t.namespace.clone(),
                    submission: t.submission,
                    source: source.clone(),
                    incumbent: incumbent.clone(),
                    incumbent_reference: source.reference.clone(),
                    incumbent_row_digest: *digest,
                    root_incumbent: incumbent_lives[0].0.clone(),
                }));
            }
            records.push(Record::DiagnosticOwner {
                namespace: t.namespace.clone(),
                submission: t.submission,
                life: incumbent.clone(),
            });
        }
        records.extend([
            Record::Terminal {
                edge: false,
                record: t.clone(),
            },
            Record::Order {
                namespace: t.namespace.clone(),
                position: t.position,
                submission: t.submission,
            },
            Record::Manifest(Box::new(m)),
            Record::Diagnostic {
                namespace: t.namespace.clone(),
                submission: t.submission,
                block,
            },
        ]);
        Ok(records)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct SignedBindingPacket {
    pub binding: SignedBinding,
    pub certificate: Certificate,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct SignedOutcomePacket {
    #[serde(default)]
    pub checkpoint: Option<super::checkpoint::Checkpoint>,
    pub record: TerminalRecord,
    pub block: DiagnosticBlock,
    pub certificate: Certificate,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct BindingInput {
    pub tenant_id: TenantId,
    pub edge_incarnation: Incarnation,
    pub expectation: ApplicationTablePolicyExpectation,
}

pub(crate) fn committed_certificate(
    db: &Database,
    namespace: &Namespace,
    state: &StateRef,
    signer: &LineageSigner,
) -> Result<Certificate> {
    let records = db.custody_authority()?;
    certificate_from_records(&records, namespace, state, signer)
}

pub(crate) fn certificate_from_records(
    records: &[Record],
    namespace: &Namespace,
    state: &StateRef,
    signer: &LineageSigner,
) -> Result<Certificate> {
    let state = records
        .iter()
        .find_map(|r| match r {
            Record::State(s) if s.state == *state => Some(s.clone()),
            _ => None,
        })
        .ok_or_else(invalid)?;
    let birth = records
        .iter()
        .find_map(|r| match r {
            Record::Birth(b) if b.identity == BirthRef::from(&state.state) => Some(b.clone()),
            _ => None,
        })
        .ok_or_else(invalid)?;
    let retired_births = retired_descriptors(records, &birth)?;
    let retirement_digest = retirement_root(&retired_births)?;
    let mut certificate = Certificate {
        format: 1,
        tenant: namespace.tenant.clone(),
        hub_node: namespace.hub_node.clone(),
        state,
        birth,
        retirement_digest,
        retired_births,
        signature: Vec::new(),
    };
    certificate.signature = signer(&certificate.bytes()?)?;
    certificate.verify()?;
    Ok(certificate)
}
fn edge_authority(db: &Database, namespace: &Namespace, state: &StateRef) -> Result<EdgeAuthority> {
    let birth = BirthRef::from(state);
    let old = db.custody_authority()?.into_iter().find_map(|r| {
        if let Record::Destination(a) = r {
            Some(a)
        } else {
            None
        }
    });
    if let Some(old) = old {
        if old.namespace != *namespace || old.birth != birth {
            return Ok(EdgeAuthority {
                destination_epoch: old.destination_epoch.checked_add(1).ok_or_else(invalid)?,
                namespace: namespace.clone(),
                birth,
                validity_revision: old.validity_revision.checked_add(1).ok_or_else(invalid)?,
            });
        }
        Ok(old)
    } else {
        Ok(EdgeAuthority {
            destination_epoch: 0,
            namespace: namespace.clone(),
            birth,
            validity_revision: 0,
        })
    }
}
pub(crate) fn commit_binding(
    db: &Database,
    tenant_id: TenantId,
    hub_node_id: String,
    edge_node_id: String,
    request: BindingInput,
    signer: &LineageSigner,
) -> Result<SignedBindingPacket> {
    if request.tenant_id != tenant_id {
        return Err(invalid());
    }
    // Statements 2/3: compare before minting authority, and repeat without a commit.
    let records = db.custody_authority()?;
    for (table, expected) in &request.expectation.tables {
        let declared = records
            .iter()
            .find_map(|r| match r {
                Record::Policy(p) if p.table == *table => Some(p),
                _ => None,
            })
            .ok_or_else(|| Error::TenantPolicyNotDeclared {
                table: table.clone(),
            })?;
        if let Some(clause) =
            super::policy::differing_clause(&declared.policy, &normalized(table, expected.clone())?)
        {
            return Err(Error::TenantPolicyMismatch {
                table: table.clone(),
                clause: clause.into(),
            });
        }
    }
    if let Some(binding) = records.iter().find_map(|r| match r {
        Record::Binding(b)
            if b.namespace.tenant == tenant_id
                && b.namespace.hub_node == hub_node_id
                && b.namespace.edge_node == edge_node_id
                && b.namespace.edge_incarnation == request.edge_incarnation
                && Some(b.namespace.hub_incarnation)
                    == db.existing_sync_incarnation(&tenant_id).ok().flatten()
                && b.tables.len() == request.expectation.tables.len()
                && b.tables.iter().all(|p| {
                    request
                        .expectation
                        .tables
                        .get(&p.table)
                        .is_some_and(|expected| {
                            super::policy::differing_clause(&p.policy, expected).is_none()
                        })
                }) =>
        {
            Some(b.clone())
        }
        _ => None,
    }) {
        let certificate =
            committed_certificate(db, &binding.namespace, &binding.issuance_state, signer)?;
        return Ok(SignedBindingPacket {
            binding,
            certificate,
        });
    }
    let incarnation = db.sync_incarnation(&tenant_id)?;
    let mut result = None;
    db.commit_delivery_metadata(|lsn| {
        // Policy comparison and freeze use the same commit exclusion as issuance.
        let declared: BTreeMap<_, _> = db
            .custody_authority()?
            .into_iter()
            .filter_map(|r| match r {
                Record::Policy(p) => Some((p.table.clone(), p)),
                _ => None,
            })
            .collect();
        let mut policies = Vec::new();
        let mut records = Vec::new();
        for (table, expected) in request.expectation.tables {
            let mut declaration =
                declared
                    .get(&table)
                    .cloned()
                    .ok_or_else(|| Error::TenantPolicyNotDeclared {
                        table: table.clone(),
                    })?;
            if declaration.digest != policy_digest(&table, &normalized(&table, expected)?)? {
                return Err(Error::TenantPolicyMismatch {
                    table,
                    clause: "binding comparison".into(),
                });
            }
            if declaration.tenant.as_ref().is_some_and(|t| t != &tenant_id) {
                return Err(invalid());
            }
            if db.table_meta(&table).is_some()
                && policy_digest(&table, &actual_policy(db, &table)?)? != declaration.digest
            {
                return Err(Error::SyncError(format!(
                    "hub installed policy differs from declaration for {table}"
                )));
            }
            declaration.tenant = Some(tenant_id.clone());
            policies.push(BoundPolicy {
                table,
                policy: declaration.policy.clone(),
                version: declaration.version,
                digest: declaration.digest,
            });
            records.push(declaration);
        }
        let state = next_state(db, incarnation)?;
        let mut b = SignedBinding {
            format: 1,
            namespace: Namespace {
                tenant: tenant_id,
                hub_node: hub_node_id,
                hub_incarnation: incarnation,
                edge_node: edge_node_id,
                edge_incarnation: request.edge_incarnation,
            },
            tables: policies,
            issuance_state: state.state.clone(),
            issuance_source: (lsn, 0),
            signature: Vec::new(),
        };
        b.signature = signer(&b.bytes()?)?;
        b.verify()?;
        result = Some(b.clone());
        let mut out = state_records(db, state, &b.namespace.tenant, &b.namespace.hub_node)?;
        out.push(Record::Binding(b));
        for mut p in records {
            p.first_bound_position.get_or_insert(lsn);
            p.first_bound_at
                .get_or_insert(contextdb_core::Wallclock::now().0);
            out.push(Record::Policy(p));
        }
        Ok(out)
    })?;
    let binding = result.ok_or_else(invalid)?;
    let certificate =
        committed_certificate(db, &binding.namespace, &binding.issuance_state, signer)?;
    Ok(SignedBindingPacket {
        binding,
        certificate,
    })
}
pub(crate) fn admit_binding(
    db: &Database,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    packet: &SignedBindingPacket,
) -> Result<()> {
    let b = &packet.binding;
    let certificate = &packet.certificate;
    certificate.verify()?;
    if certificate.tenant != b.namespace.tenant
        || certificate.hub_node != b.namespace.hub_node
        || certificate.state.state != b.issuance_state
    {
        return Err(invalid());
    }
    b.verify()?;
    if &b.namespace.tenant != tenant
        || b.namespace.hub_node != hub
        || b.namespace.edge_node != edge
        || db
            .existing_sync_incarnation(tenant)?
            .is_some_and(|inc| inc != b.namespace.edge_incarnation)
    {
        return Err(invalid());
    }
    if db
        .custody_authority()?
        .iter()
        .any(|r| matches!(r, Record::Binding(prior) if prior == b))
    {
        return Ok(());
    }
    let tx = db.begin()?;
    db.commit_delivery_prepared(tx, &mut |db, ws| {
        // Statement 4: the accepted binding and edge incarnation persist together.
        ws.config_writes.push((
            tenant.config_key("sync_incarnation"),
            crate::persistence::RedbPersistence::encode_config_value(
                &b.namespace.edge_incarnation.to_hex(),
            )?,
        ));
        let mut existing = db.custody_authority()?;
        let old_objects = existing
            .iter()
            .filter_map(|r| match r {
                Record::Binding(prior) if prior.namespace == b.namespace => Some(prior),
                _ => None,
            })
            .map(|prior| {
                Ok(format!(
                    "@object:{}",
                    hex(&object_digest(0, &prior.bytes()?, &prior.signature))
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        existing.extend(db.custody_records_in(&old_objects)?);
        for prior in existing.iter().filter_map(|r| match r {
            Record::Binding(prior) if prior.namespace == b.namespace => Some(prior),
            _ => None,
        }) {
            let digest = object_digest(0, &prior.bytes()?, &prior.signature);
            if digest != object_digest(0, &b.bytes()?, &b.signature) {
                for record in &existing {
                    if matches!(record, Record::ObjectAdmission(a) if a.object_digest == digest) {
                        ws.config_deletes.push(record.key()?);
                    }
                }
            }
        }
        let authority = edge_authority(db, &b.namespace, &b.issuance_state)?;
        let admission = Admission {
            object_digest: object_digest(0, &b.bytes()?, &b.signature),
            authority: authority.clone(),
            owning_state: b.issuance_state.clone(),
            certificate: certificate.id()?,
        };
        let state = next_state(db, b.namespace.edge_incarnation)?;
        let mut records = state_records(db, state, &b.namespace.tenant, &b.namespace.hub_node)?;
        // The edge owns a local history, not the remote hub's signing identity.
        for record in &mut records {
            if let Record::Control(control) = record {
                control.hub_node = None;
                control.tenant = None;
            }
        }
        records.extend([
            Record::Binding(b.clone()),
            Record::Certificate(certificate.clone()),
            Record::Destination(authority),
            Record::ObjectAdmission(admission),
        ]);
        Ok(records)
    })
}
// Statements 9/10: changed application rows have no current registration;
// malformed or damaged journal records still fail their canonical verification.
pub(crate) fn current_manifest(
    db: &Database,
    table: &str,
    key: &NaturalKey,
) -> Result<Option<ManifestRecord>> {
    let records = db.custody_root(&RowRef {
        table: table.into(),
        key: key.clone(),
    })?;
    let manifest = records.iter().find_map(|r| match r {
        Record::Manifest(m)
            if m.seal.root.table == table
                && m.seal.root.key == *key
                && records.iter().any(|r| {
                    matches!(r, Record::Root { life, submission }
                if *life == m.root_life && *submission == m.id)
                }) =>
        {
            Some(m.as_ref())
        }
        _ => None,
    });
    let Some(m) = manifest else {
        return Ok(None);
    };
    m.verify()?;
    for e in &m.rows {
        let Some(row) = db.custody_row(&e.reference)? else {
            return Ok(None);
        };
        if full_row_digest(&row.values) != e.row_digest {
            return Ok(None);
        }
    }
    Ok(Some(m.clone()))
}
pub(crate) fn commit_terminal(
    db: &Database,
    tenant: TenantId,
    hub: String,
    edge: String,
    request: TerminalInput,
    signer: &LineageSigner,
) -> Result<SignedOutcomePacket> {
    request.manifest.verify()?;
    let submission = request.manifest.id;
    let input = request;
    let mut prepared = Some(input.clone());
    let mut prepare = |db: &Database, ws: &mut contextdb_tx::WriteSet| {
        prepare_terminal(
            db,
            ws,
            tenant.clone(),
            hub.clone(),
            edge.clone(),
            prepared.take().ok_or_else(invalid)?,
            signer,
        )
    };
    if input.kind == 0 {
        let mut changes = ChangeSet {
            rows: input.rows.clone(),
            ..Default::default()
        };
        let received = if input
            .manifest
            .rows
            .iter()
            .any(|r| db.table_meta(&r.reference.table).is_none())
        {
            let mut context = crate::protocol::ReceivedDdlContext {
                tenant_id: tenant.clone(),
                source_node_id: edge.clone(),
                source_incarnation: input.edge_incarnation,
                entries: Vec::new(),
            };
            for schema in &input.manifest.schemas {
                schema.verify()?;
                if schema.author_node != edge
                    || schema.author_incarnation != input.edge_incarnation
                    || schema.tenant != tenant
                {
                    return Err(invalid());
                }
                let ddl: crate::protocol::WireDdlChange =
                    rmp_serde::from_slice(&schema.ddl_bytes).map_err(|_| invalid())?;
                changes.ddl.push(crate::sync_types::DdlChange::from(ddl));
                changes.ddl_lsn.push(schema.source_lsn);
                context.entries.push(crate::protocol::ReceivedDdlEntry {
                    source_ddl_lsn: schema.source_lsn,
                    ordinal: schema.ordinal,
                    table: Some(schema.table.clone()),
                    table_generation: Some(schema.table_generation),
                    digest: schema.ddl_digest.clone(),
                });
            }
            Some(context)
        } else {
            None
        };
        let lineages = input
            .manifest
            .rows
            .iter()
            .map(|r| {
                (
                    r.reference.table.clone(),
                    r.reference.key.clone(),
                    r.authored_source.0,
                    r.creator.clone(),
                )
            })
            .collect::<Vec<_>>();
        let receipt = crate::database::SyncApplyReceipt {
            tenant_id: tenant.clone(),
            node_id: edge.clone(),
            incarnation: input.edge_incarnation,
            source_lsn: input.manifest.seal.source.0,
            dependency_complete: true,
        };
        db.apply_delivery_prepared(
            changes,
            receipt,
            &hub,
            &lineages,
            received.as_ref(),
            &mut prepare,
        )?;
    } else {
        let tx = db.begin()?;
        db.commit_delivery_prepared(tx, &mut prepare)?;
    }
    let records = db.custody_submission(submission)?;
    let record = records
        .iter()
        .find_map(|r| match r {
            Record::Terminal {
                edge: false,
                record,
            } if record.submission == submission
                && record.namespace.hub_node == hub
                && record.namespace.edge_node == edge
                && Some(record.namespace.hub_incarnation)
                    == db.existing_sync_incarnation(&tenant).ok().flatten() =>
            {
                Some(record.clone())
            }
            _ => None,
        })
        .ok_or_else(invalid)?;
    let block = records
        .iter()
        .find_map(|r| match r {
            Record::Diagnostic {
                namespace,
                submission: id,
                block,
            } if *id == submission && *namespace == record.namespace => Some(block.clone()),
            _ => None,
        })
        .ok_or_else(invalid)?;
    let certificate = committed_certificate(db, &record.namespace, &record.state, signer)?;
    Ok(SignedOutcomePacket {
        checkpoint: super::checkpoint::current(db, &record.namespace)?,
        record,
        block,
        certificate,
    })
}
#[cfg(feature = "test-seams")]
pub(crate) fn admit_terminal(
    db: &Database,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    packet: &SignedOutcomePacket,
) -> Result<()> {
    admit_terminals(db, tenant, hub, edge, std::slice::from_ref(packet))
}

// Statements 12/13/14: one admission transaction for the complete returned
// cohort; no row scan, no truncated acceptance position, no partial credit.
pub(crate) fn admit_terminals(
    db: &Database,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    packets: &[SignedOutcomePacket],
) -> Result<()> {
    if packets.is_empty() {
        return Ok(());
    }
    for packet in packets {
        let t = &packet.record;
        packet.certificate.verify()?;
        t.verify()?;
        if let Some(c) = &packet.checkpoint {
            c.verify()?;
            if c.namespace != t.namespace {
                return Err(invalid());
            }
        }
        if packet.certificate.state.state != t.state
            || packet.certificate.tenant != t.namespace.tenant
            || packet.certificate.hub_node != t.namespace.hub_node
            || !t.diagnostics.matches(&packet.block)?
            || t.namespace.tenant != *tenant
            || t.namespace.hub_node != hub
            || t.namespace.edge_node != edge
        {
            return Err(invalid());
        }
    }
    let selectors = std::iter::once("delivery_destination.v1".to_owned())
        .chain(
            packets
                .iter()
                .map(|p| format!("@{}", super::store::submission_group(p.record.submission))),
        )
        .collect::<Vec<_>>();
    let current = db.custody_records_in(&selectors)?;
    if packets.iter().all(|p| current.iter().any(|r| matches!(r, Record::Terminal { edge: true, record: t } if t.signature == p.record.signature))) { return Ok(()); }
    db.commit_delivery_metadata(|_| {
        let current = db.custody_records_in(&selectors)?;
        let destination = current.iter().find_map(|r| match r { Record::Destination(d) => Some(d), _ => None }).ok_or_else(invalid)?;
        let manifests = current.iter().filter_map(|r| match r { Record::Manifest(m) if current.iter().any(|r| matches!(r, Record::Root { submission, .. } if *submission == m.id)) => Some((m.id, m)), _ => None }).collect::<BTreeMap<_, _>>();
        let mut writes = Vec::new();
        let mut strongest: Option<&super::checkpoint::Checkpoint> = None;
        for packet in packets {
            let t = &packet.record;
            if current.iter().any(|r| matches!(r, Record::Terminal { edge: true, record: old } if old.signature == t.signature)) { continue; }
            // A purged/discarded root has no owning manifest; stale replies
            // cannot restore its credit or recreate any metadata.
            let Some(m) = manifests.get(&t.submission) else { continue; };
            if t.namespace != destination.namespace || t.namespace.edge_incarnation != m.edge_incarnation
                || t.root != m.seal.root || t.seal_digest != m.seal.digest() || t.unit_digest != m.seal.unit_digest { return Err(invalid()); }
            if let Some(checkpoint) = &packet.checkpoint {
                if strongest.is_some_and(|old| old.count == checkpoint.count && old.digest != checkpoint.digest) { return Err(invalid()); }
                if strongest.is_none_or(|old| old.count < checkpoint.count) { strongest = Some(checkpoint); }
            }
            writes.extend([
                Record::Terminal { edge: true, record: t.clone() },
                Record::Diagnostic { namespace: t.namespace.clone(), submission: t.submission, block: packet.block.clone() },
                Record::Certificate(packet.certificate.clone()),
                Record::ObjectAdmission(Admission { object_digest: object_digest(1, &t.bytes()?, &t.signature), authority: destination.clone(), owning_state: t.state.clone(), certificate: packet.certificate.id()? }),
            ]);
            for life in std::iter::once(&m.root_life).chain(&m.member_lives) {
                writes.push(Record::DiagnosticOwner { namespace: t.namespace.clone(), submission: t.submission, life: life.clone() });
            }
        }
        // Statements 13/15: packet order cannot weaken already admitted image evidence.
        if let Some(checkpoint) = strongest
            && let Some(record) = super::checkpoint::admission(db,checkpoint)? { writes.push(record); }
        Ok(writes)
    })
}

/// Commit several independently signed source units in one real receiving
/// transaction. Shared subjects must carry identical authenticated row bytes.
#[cfg(feature = "test-seams")]
pub(crate) fn commit_terminal_batch(
    db: &Database,
    tenant: TenantId,
    hub: String,
    edge: String,
    requests: Vec<TerminalInput>,
    signer: &LineageSigner,
) -> Result<()> {
    if requests.is_empty()
        || requests.iter().any(|r| {
            r.kind != 0
                || r.tenant_id != tenant
                || r.edge_incarnation != requests[0].edge_incarnation
                || r.manifest.seal.source.0 != requests[0].manifest.seal.source.0
        })
    {
        return Err(invalid());
    }
    let mut changes = ChangeSet::default();
    let mut lineages = Vec::new();
    let mut seen = BTreeMap::new();
    for input in &requests {
        input.manifest.verify()?;
        for row in &input.rows {
            let key = RowRef {
                table: row.table.clone(),
                key: row.natural_key.clone(),
            }
            .order_key();
            if let Some(prior) = seen.insert(key, full_row_digest(&row.values)) {
                if prior != full_row_digest(&row.values) {
                    return Err(invalid());
                }
            } else {
                changes.rows.push(row.clone());
            }
        }
        for row in &input.manifest.rows {
            lineages.push((
                row.reference.table.clone(),
                row.reference.key.clone(),
                row.authored_source.0,
                row.creator.clone(),
            ));
        }
    }
    let receipt = crate::database::SyncApplyReceipt {
        tenant_id: tenant.clone(),
        node_id: edge.clone(),
        incarnation: requests[0].edge_incarnation,
        source_lsn: requests[0].manifest.seal.source.0,
        dependency_complete: true,
    };
    db.apply_delivery_prepared(changes, receipt, &hub, &lineages, None, &mut |db, ws| {
        let incarnation = binding(db, &edge)?.namespace.hub_incarnation;
        let state = next_state(db, incarnation)?;
        let mut records = BTreeMap::new();
        for (ordinal, request) in requests.iter().enumerate() {
            for record in prepare_terminal_at(
                db,
                ws,
                tenant.clone(),
                hub.clone(),
                edge.clone(),
                request.clone(),
                signer,
                Some(state.clone()),
                u32::try_from(ordinal).map_err(|_| invalid())?,
            )? {
                let key = record.key()?;
                if let Some(prior) = records.insert(key, record.clone())
                    && encode(&prior)? != encode(&record)?
                {
                    return Err(invalid());
                }
            }
        }
        Ok(records.into_values().collect())
    })?;
    Ok(())
}
