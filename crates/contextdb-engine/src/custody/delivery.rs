//! Authenticated custody uses the ordinary push lane.
use super::{canonical::*, preparation::*, records::*};
use crate::{
    Database,
    protocol::*,
    sync_types::{ApplyResult, ChangeSet, NaturalKey, RowChange},
};
use contextdb_core::{Error, Result, TenantId};

pub(crate) fn manifested_request(
    db: &Database,
    tenant: &TenantId,
    table: &str,
    key: &NaturalKey,
    signer: &LineageSigner,
) -> Result<crate::protocol::PushRequest> {
    use crate::protocol::*;
    let m = current_manifest(db, table, key)?.ok_or_else(|| Error::ManifestRequired {
        table: table.into(),
    })?;
    if m.tenant != *tenant {
        return Err(invalid());
    }
    let mut changes = ChangeSet::default();
    let mut lineage = std::collections::HashMap::new();
    for evidence in &m.rows {
        let row = actual_row(db, &evidence.reference)?;
        lineage.insert(
            (
                evidence.reference.table.clone(),
                rmp_serde::to_vec(&evidence.reference.key).map_err(|_| invalid())?,
                row.lsn,
            ),
            evidence.creator.clone(),
        );
        changes.rows.push(RowChange {
            table: evidence.reference.table.clone(),
            natural_key: evidence.reference.key.clone(),
            values: row.values,
            deleted: false,
            lsn: row.lsn,
            created_at: row.created_at,
        });
    }
    let mut life_evidence = Vec::new();
    let mut sorted: Vec<_> = m.rows.iter().collect();
    sorted.sort_by_key(|r| r.reference.order_key());
    for row in sorted {
        let mut e = Encoder::default();
        row.encode_life(&mut e)?;
        life_evidence.push(e.0);
    }
    let mut retained_slots = Vec::new();
    for row in m.rows.iter().skip(1) {
        let mut e = Encoder::default();
        row.reference.encode(&mut e);
        e.u8(0);
        e.u8(1);
        row.reference.encode(&mut e);
        e.u8(1);
        e.raw(&row.row_digest);
        e.u8(1);
        row.encode_life(&mut e)?;
        e.u8(0);
        retained_slots.push(e.0);
    }
    let mut slots = Encoder::default();
    slots.u64(retained_slots.len() as u64);
    for slot in &retained_slots {
        slots.raw(slot);
    }
    let mut retained = Encoder::domain("delivery-retained-body.v1");
    retained.bytes(&m.projection_bytes()?);
    retained.bytes(&slots.0);
    retained.bytes(&m.policy_bytes()?);
    let mut disclosure = Encoder::domain("delivery-disclosure.v1");
    disclosure.raw(&m.seal.digest());
    disclosure.u8(0);
    disclosure.u64(0);
    disclosure.raw(&retained.digest());
    disclosure.u8(0);
    let mut submission = Encoder::default();
    submission.u8(0);
    submission.raw(m.id.as_bytes());
    let manifest = WireDeliveryManifest {
        submission_id: submission.0,
        seal: m.seal.bytes(),
        life_evidence,
        materialization_projection: m.projection_bytes()?,
        policy_evidence: rmp_serde::to_vec_named(&m).map_err(|_| invalid())?,
        retained_slots,
        erased_slot_count: 0,
        submission_signature: m.signature.clone(),
        disclosure_signature: signer(&disclosure.0)?,
        disclosure: disclosure.0,
        erasure_authorization: None,
    };
    let mut wire = wire_changeset_with_arrivals_and_lineages(
        changes,
        &std::collections::HashMap::new(),
        &lineage,
    );
    // The actual ordinary push carries committed rows and manifests.
    wire.manifests = vec![manifest];
    Ok(PushRequest {
        changeset: wire,
        incarnation: m.edge_incarnation,
    })
}

pub(crate) fn wire_packet(packet: &SignedOutcomePacket) -> Result<WireDeliveryOutcome> {
    let mut wire = packet.record.wire(&packet.block)?;
    wire.lookup_source = rmp_serde::to_vec_named(packet).map_err(|_| invalid())?;
    Ok(wire)
}

pub(crate) fn read_packet(wire: &WireDeliveryOutcome) -> Result<SignedOutcomePacket> {
    let packet: SignedOutcomePacket =
        rmp_serde::from_slice(&wire.lookup_source).map_err(|_| invalid())?;
    if wire_packet(&packet)? != *wire {
        return Err(invalid());
    }
    Ok(packet)
}

pub(crate) fn packet_for(
    db: &Database,
    t: &TerminalRecord,
    signer: &LineageSigner,
) -> Result<SignedOutcomePacket> {
    let block = db
        .custody_submission(t.submission)?
        .into_iter()
        .find_map(|r| match r {
            Record::Diagnostic {
                namespace,
                submission,
                block,
            } if namespace == t.namespace && submission == t.submission => Some(block),
            _ => None,
        })
        .ok_or_else(invalid)?;
    Ok(SignedOutcomePacket {
        checkpoint: super::checkpoint::current(db, &t.namespace)?,
        record: t.clone(),
        block,
        certificate: committed_certificate(db, &t.namespace, &t.state, signer)?,
    })
}

pub(crate) fn attach(
    db: &Database,
    tenant: &TenantId,
    changes: &ChangeSet,
    signer: &LineageSigner,
) -> Result<Vec<WireDeliveryManifest>> {
    let mut out = Vec::new();
    for row in &changes.rows {
        if !row.deleted
            && db
                .table_meta(&row.table)
                .is_some_and(|m| m.delivery_manifest_tables.is_some())
            && current_manifest(db, &row.table, &row.natural_key)?.is_some()
        {
            out.extend(
                manifested_request(db, tenant, &row.table, &row.natural_key, signer)?
                    .changeset
                    .manifests,
            );
        }
    }
    Ok(out)
}

pub(crate) struct DeliveryRoute<'a> {
    pub(crate) tenant: &'a TenantId,
    pub(crate) hub: &'a str,
    pub(crate) edge: &'a str,
}

pub(crate) fn apply(
    db: &Database,
    route: DeliveryRoute<'_>,
    request: PushRequest,
    changes: ChangeSet,
    arrivals: &std::collections::HashMap<contextdb_core::Lsn, Option<contextdb_core::Lsn>>,
    lineages: &[(String, NaturalKey, contextdb_core::Lsn, WireRowLineage)],
) -> Result<(ApplyResult, Vec<WireDeliveryOutcome>)> {
    let DeliveryRoute { tenant, hub, edge } = route;
    let runtime = db.custody_runtime().ok_or_else(invalid)?;
    let signer = &runtime.signer;
    let mut result = ApplyResult {
        applied_rows: 0,
        skipped_rows: 0,
        conflicts: Vec::new(),
        new_lsn: db.current_lsn(),
    };
    let mut outcomes = Vec::new();
    let authority = db.custody_authority()?;
    let mut remainder = changes;
    let mut covered = Vec::new();
    let mut manifests = Vec::new();
    for wire in &request.changeset.manifests {
        let m: ManifestRecord =
            rmp_serde::from_slice(&wire.policy_evidence).map_err(|_| invalid())?;
        m.verify()?;
        if m.tenant != *tenant
            || m.edge_node != edge
            || m.edge_incarnation != request.incarnation
            || wire.seal != m.seal.bytes()
            || wire.materialization_projection != m.projection_bytes()?
            || wire.submission_signature != m.signature
        {
            return Err(invalid());
        }
        if covered.iter().any(|root| root == &m.seal.root) {
            return Err(invalid());
        }
        covered.push(m.seal.root.clone());
        manifests.push(m);
    }
    let mut manifested_rows = Vec::new();
    let all_rows = std::mem::take(&mut remainder.rows);
    for row in all_rows {
        if manifests.iter().any(|manifest| {
            manifest.rows.iter().any(|entry| {
                entry.reference.table == row.table && entry.reference.key == row.natural_key
            })
        }) {
            manifested_rows.push(row);
        } else {
            remainder.rows.push(row);
        }
    }
    remainder.vectors.retain(|vector| {
        !manifests.iter().any(|manifest| {
            std::iter::once(&manifest.root_life)
                .chain(&manifest.member_lives)
                .any(|life| {
                    life.original_table == vector.index.table
                        && life.local_row_id == vector.row_id.0
                })
        })
    });
    let (missing, refused) =
        missing_manifest(db, tenant, hub, edge, request.incarnation, &remainder)?;
    result.skipped_rows += missing.skipped_rows;
    result.conflicts.extend(missing.conflicts);
    outcomes.extend(refused);
    remove_missing_units(db, &mut remainder);
    let (after, before): (Vec<_>, Vec<_>) = db
        .custody_remainder_units(remainder)
        .into_iter()
        .partition(|unit| {
            unit.changes.rows.iter().any(|row| {
                manifested_rows
                    .iter()
                    .any(|parent| db.custody_row_references(row, parent))
            })
        });
    let source_lsn = manifested_rows
        .iter()
        .map(|row| row.lsn)
        .chain(
            before
                .iter()
                .chain(&after)
                .filter_map(|unit| unit.changes.max_lsn()),
        )
        .max();
    for unit in before {
        merge_result(
            &mut result,
            apply_remainder(
                db,
                DeliveryRoute { tenant, hub, edge },
                &request,
                arrivals,
                lineages,
                unit.changes,
                unit.dependency_complete,
            )?,
        );
    }
    for m in manifests {
        let mut records = db.custody_root(&m.seal.root)?;
        records.extend(authority.iter().cloned());
        let rows = manifested_rows
            .iter()
            .filter(|r| {
                m.rows
                    .iter()
                    .any(|e| e.reference.table == r.table && e.reference.key == r.natural_key)
            })
            .cloned()
            .collect::<Vec<_>>();
        let existing = records.iter().find_map(|r| match r {
            Record::Terminal {
                edge: false,
                record: t,
            } if t.submission == m.id
                && t.namespace.tenant == *tenant
                && t.namespace.edge_node == edge
                && t.namespace.edge_incarnation == request.incarnation
                && t.namespace.hub_incarnation
                    == db.existing_sync_incarnation(tenant).ok().flatten()? =>
            {
                Some(t)
            }
            _ => None,
        });
        if let Some(t) = existing {
            let packet = packet_for(db, t, signer)?;
            result.skipped_rows += rows.len();
            result.conflicts.extend(packet.block.conflicts.clone());
            outcomes.push(wire_packet(&packet)?);
            continue;
        }
        let cause = if rows.len() != m.rows.len() {
            Some("manifest_incomplete")
        } else if m.rows.iter().any(|e| {
            rows.iter()
                .find(|r| r.table == e.reference.table && r.natural_key == e.reference.key)
                .is_none_or(|r| r.deleted || full_row_digest(&r.values) != e.row_digest)
        }) {
            Some("member_digest_mismatch")
        } else if m
            .policies
            .iter()
            .filter(|p| p.table == m.seal.root.table)
            .any(|p| {
                records.iter().find_map(|r| match r {
                    Record::Policy(declared) if declared.table == p.table => Some(declared.digest),
                    _ => None,
                }) != p.bound.as_ref().map(|b| b.digest)
            })
        {
            Some("binding_mismatch")
        } else {
            None
        };
        let incumbent = records.iter().find_map(|r| match r {
            Record::Terminal {
                edge: false,
                record: t,
            } if t.root == m.seal.root && t.kind != 2 => Some(t),
            _ => None,
        });
        let (kind, cause) = if let Some(cause) = cause {
            (2, Some(cause))
        } else if let Some(t) = incumbent {
            if t.unit_digest == m.seal.unit_digest {
                (1, None)
            } else {
                (2, Some("unit_digest_mismatch"))
            }
        } else if db.table_meta(&m.seal.root.table).is_some_and(|meta| {
            meta.conflict_policy
                .unwrap_or(contextdb_core::DEFAULT_CONFLICT_POLICY)
                == contextdb_core::ConflictPolicy::KEEP_FIRST
        }) && let Some(digest) = db.custody_incumbent_digest(&m.seal.root)?
        {
            // A hub-written root has custody even before any
            // terminal exists. Compare its complete held content and membership.
            if digest == m.seal.unit_digest {
                (1, None)
            } else {
                (2, Some("unit_digest_mismatch"))
            }
        } else {
            (0, None)
        };
        let packet = commit_terminal(
            db,
            tenant.clone(),
            hub.into(),
            edge.into(),
            TerminalInput {
                tenant_id: tenant.clone(),
                edge_incarnation: request.incarnation,
                manifest: m,
                rows: rows.clone(),
                kind,
                cause: cause.map(str::to_string),
            },
            signer,
        )?;
        if kind == 0 {
            result.applied_rows += rows.len();
        } else {
            result.skipped_rows += rows.len();
        }
        result.conflicts.extend(packet.block.conflicts.clone());
        result.new_lsn = packet.record.position.0;
        outcomes.push(wire_packet(&packet)?);
    }
    for unit in after {
        merge_result(
            &mut result,
            apply_remainder(
                db,
                DeliveryRoute { tenant, hub, edge },
                &request,
                arrivals,
                lineages,
                unit.changes,
                unit.dependency_complete,
            )?,
        );
    }
    if let Some(source_lsn) = source_lsn {
        db.finish_custody_push(&crate::database::SyncApplyReceipt {
            tenant_id: tenant.clone(),
            node_id: edge.into(),
            incarnation: request.incarnation,
            source_lsn,
            dependency_complete: true,
        })?;
    }
    Ok((result, outcomes))
}

fn missing_manifest(
    db: &Database,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    incarnation: contextdb_core::Incarnation,
    changes: &ChangeSet,
) -> Result<(ApplyResult, Vec<WireDeliveryOutcome>)> {
    let signer = db.custody_runtime().ok_or_else(invalid)?.signer;
    if !changes.rows.iter().any(|r| {
        !r.deleted
            && db
                .table_meta(&r.table)
                .is_some_and(|m| m.delivery_manifest_tables.is_some())
    }) {
        return Ok((
            ApplyResult {
                applied_rows: 0,
                skipped_rows: 0,
                conflicts: Vec::new(),
                new_lsn: db.current_lsn(),
            },
            Vec::new(),
        ));
    }
    let namespace = Namespace {
        tenant: tenant.clone(),
        hub_node: hub.into(),
        hub_incarnation: db.sync_incarnation(tenant)?,
        edge_node: edge.into(),
        edge_incarnation: incarnation,
    };
    let mut outcomes = Vec::new();
    for root in changes.rows.iter().filter(|r| {
        !r.deleted
            && db
                .table_meta(&r.table)
                .is_some_and(|m| m.delivery_manifest_tables.is_some())
    }) {
        let mut unit = Encoder::domain("delivery-missing-manifest.v1");
        unit.reference(&root.table, &root.natural_key);
        unit.u64(root.lsn.0);
        unit.raw(&full_row_digest(&root.values));
        let digest = unit.digest();
        let submission = uuid::Uuid::from_bytes(digest[..16].try_into().map_err(|_| invalid())?);
        if let Some(existing) = db
            .custody_submission(submission)?
            .iter()
            .find_map(|r| match r {
                Record::Terminal {
                    edge: false,
                    record: t,
                } if t.namespace == namespace => Some(t.clone()),
                _ => None,
            })
        {
            outcomes.push(wire_packet(&packet_for(db, &existing, &signer)?)?);
            continue;
        }
        let state = next_state(db, namespace.hub_incarnation)?;
        let block = DiagnosticBlock {
            salt: random_digest(),
            subjects: vec![(
                RowRef {
                    table: root.table.clone(),
                    key: root.natural_key.clone(),
                },
                0,
            )],
            conflicts: Vec::new(),
        };
        let mut terminal = None;
        db.commit_delivery_metadata(|lsn| {
            let mut t = TerminalRecord {
                namespace: namespace.clone(),
                root: RowRef {
                    table: root.table.clone(),
                    key: root.natural_key.clone(),
                },
                origin_life_digest: digest,
                submission,
                seal_digest: digest,
                unit_digest: digest,
                source: (root.lsn, 0),
                kind: 2,
                cause: Some("manifest_required".into()),
                position: (lsn, 0),
                state: state.state.clone(),
                diagnostics: DiagnosticClaim::from_block(&block)?,
                signature: Vec::new(),
            };
            t.signature = signer(&t.bytes()?)?;
            let mut records = state_records(db, state, &namespace.tenant, &namespace.hub_node)?;
            records.extend([
                Record::Terminal {
                    edge: false,
                    record: t.clone(),
                },
                Record::Diagnostic {
                    namespace: t.namespace.clone(),
                    submission,
                    block: block.clone(),
                },
                Record::Order {
                    namespace: t.namespace.clone(),
                    position: t.position,
                    submission,
                },
            ]);
            terminal = Some(t);
            Ok(records)
        })?;
        outcomes.push(wire_packet(&packet_for(
            db,
            &terminal.ok_or_else(invalid)?,
            &signer,
        )?)?);
    }
    Ok((
        ApplyResult {
            applied_rows: 0,
            skipped_rows: missing_rows(db, changes).len(),
            conflicts: Vec::new(),
            new_lsn: db.current_lsn(),
        },
        outcomes,
    ))
}

pub(crate) fn fetch(
    db: &Database,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    incarnation: contextdb_core::Incarnation,
    since: Option<&[u8]>,
) -> Result<Vec<WireDeliveryOutcome>> {
    #[derive(serde::Deserialize)]
    struct Cursor {
        tenant_id: TenantId,
        hub_node_id: String,
        hub_incarnation: contextdb_core::Incarnation,
        edge_node_id: String,
        edge_incarnation: contextdb_core::Incarnation,
        position: contextdb_core::Lsn,
        ordinal: u32,
    }
    let hub_incarnation = db.existing_sync_incarnation(tenant)?.ok_or_else(invalid)?;
    let after = since
        .map(|bytes| {
            let cursor: Cursor = rmp_serde::from_slice(bytes).map_err(|_| invalid())?;
            if cursor.tenant_id != *tenant
                || cursor.hub_node_id != hub
                || cursor.hub_incarnation != hub_incarnation
                || cursor.edge_node_id != edge
                || cursor.edge_incarnation != incarnation
            {
                return Err(invalid());
            }
            Ok((cursor.position, cursor.ordinal))
        })
        .transpose()?;
    let signer = db.custody_runtime().ok_or_else(invalid)?.signer;
    let namespace = Namespace {
        tenant: tenant.clone(),
        hub_node: hub.into(),
        hub_incarnation,
        edge_node: edge.into(),
        edge_incarnation: incarnation,
    };
    let records =
        db.custody_records_in(&[format!("delivery_hub_outcome.v1.{}.", namespace.key()?)])?;
    let mut terminals = records
        .iter()
        .filter_map(|r| match r {
            Record::Terminal {
                edge: false,
                record: t,
            } if t.namespace == namespace && after.is_none_or(|p| t.position > p) => Some(t),
            _ => None,
        })
        .collect::<Vec<_>>();
    terminals.sort_by_key(|t| t.position);
    terminals
        .into_iter()
        .map(|t| wire_packet(&packet_for(db, t, &signer)?))
        .collect()
}

// Resend and status share the durable outcome decision.
// Watermarks can retire ordinary history but cannot retire an unanswered unit.
pub(crate) fn pending_changes(db: &Database, mut changes: ChangeSet) -> Result<ChangeSet> {
    // Examine the changed identities and the pending index,
    // never all delivered history. Only active local root ownership is eligible.
    let mut terminal_rows = std::collections::BTreeSet::new();
    let mut terminal_vectors = std::collections::BTreeSet::new();
    let destination = db.custody_authority()?.into_iter().find_map(|r| match r {
        Record::Destination(d) => Some(d.namespace),
        _ => None,
    });
    for row in &changes.rows {
        for record in db.custody_records_in(&[format!(
            "@row:{}",
            hex(&RowRef {
                table: row.table.clone(),
                key: row.natural_key.clone()
            }
            .bytes())
        )])? {
            let Record::Manifest(m) = record else {
                continue;
            };
            let owned = db.custody_submission(m.id)?;
            if owned.iter().any(|r| matches!(r, Record::Root {submission, ..} if *submission == m.id)) && owned.iter().any(|r| matches!(r, Record::Terminal {edge:true, record:t} if Some(&t.namespace) == destination.as_ref())) {
                terminal_rows.extend(m.rows.iter().map(|e| e.reference.order_key()));
                terminal_vectors.extend(std::iter::once(&m.root_life).chain(&m.member_lives).map(|life| (life.original_table.clone(),life.local_row_id)));
            }
        }
    }
    changes.rows.retain(|r| {
        !terminal_rows.contains(
            &RowRef {
                table: r.table.clone(),
                key: r.natural_key.clone(),
            }
            .order_key(),
        )
    });
    changes
        .vectors
        .retain(|v| !terminal_vectors.contains(&(v.index.table.clone(), v.row_id.0)));
    for record in db.custody_pending_manifests()? {
        let Record::Manifest(m) = record else {
            continue;
        };
        if !db.table_meta(&m.seal.root.table).is_some_and(|meta| {
            meta.sync_direction
                .unwrap_or(contextdb_core::DEFAULT_SYNC_DIRECTION)
                .delivers()
        }) {
            continue;
        }
        // Absent or superseded roots cannot resurrect a unit.
        let Some(m) = current_manifest(db, &m.seal.root.table, &m.seal.root.key)? else {
            continue;
        };
        for e in &m.rows {
            if changes
                .rows
                .iter()
                .any(|r| r.table == e.reference.table && r.natural_key == e.reference.key)
            {
                continue;
            }
            let row = actual_row(db, &e.reference)?;
            changes.rows.push(RowChange {
                table: e.reference.table.clone(),
                natural_key: e.reference.key.clone(),
                values: row.values,
                deleted: false,
                lsn: row.lsn,
                created_at: row.created_at,
            });
        }
    }
    Ok(changes)
}

// Absent membership refuses the root and its dependent rows;
// unrelated rows remain eligible for ordinary arbitration in the same request.
fn missing_rows(
    db: &Database,
    changes: &ChangeSet,
) -> std::collections::BTreeSet<(String, Vec<u8>)> {
    let identity = |r: &RowChange| {
        RowRef {
            table: r.table.clone(),
            key: r.natural_key.clone(),
        }
        .order_key()
    };
    let mut missing = changes
        .rows
        .iter()
        .filter(|r| {
            !r.deleted
                && db
                    .table_meta(&r.table)
                    .is_some_and(|m| m.delivery_manifest_tables.is_some())
        })
        .map(identity)
        .collect::<std::collections::BTreeSet<_>>();
    loop {
        let old = missing.len();
        for row in &changes.rows {
            let Some(meta) = db.table_meta(&row.table) else {
                continue;
            };
            let depends = changes
                .rows
                .iter()
                .filter(|p| missing.contains(&identity(p)))
                .any(|parent| {
                    meta.columns.iter().any(|c| {
                        c.references.as_ref().is_some_and(|fk| {
                            fk.table == parent.table
                                && row.values.get(&c.name).is_some_and(|v| {
                                    *v != contextdb_core::Value::Null
                                        && Some(v) == parent.values.get(&fk.column)
                                })
                        })
                    }) || meta.composite_foreign_keys.iter().any(|fk| {
                        fk.parent_table == parent.table
                            && fk
                                .child_columns
                                .iter()
                                .zip(&fk.parent_columns)
                                .all(|(c, p)| {
                                    row.values.get(c).is_some_and(|v| {
                                        *v != contextdb_core::Value::Null
                                            && Some(v) == parent.values.get(p)
                                    })
                                })
                    })
                });
            if depends {
                missing.insert(identity(row));
            }
        }
        if old == missing.len() {
            break;
        }
    }
    missing
}
fn remove_missing_units(db: &Database, changes: &mut ChangeSet) {
    let missing = missing_rows(db, changes);
    changes.rows.retain(|r| {
        !missing.contains(
            &RowRef {
                table: r.table.clone(),
                key: r.natural_key.clone(),
            }
            .order_key(),
        )
    });
}
fn apply_remainder(
    db: &Database,
    route: DeliveryRoute<'_>,
    request: &PushRequest,
    arrivals: &std::collections::HashMap<contextdb_core::Lsn, Option<contextdb_core::Lsn>>,
    request_lineages: &[(String, NaturalKey, contextdb_core::Lsn, WireRowLineage)],
    mut changes: ChangeSet,
    dependency_complete: bool,
) -> Result<ApplyResult> {
    let DeliveryRoute { tenant, hub, edge } = route;
    let lineages = request_lineages
        .iter()
        .filter(|(table, key, lsn, _)| {
            changes
                .rows
                .iter()
                .any(|r| &r.table == table && &r.natural_key == key && &r.lsn == lsn)
        })
        .cloned()
        .collect::<Vec<_>>();
    let ddl = if changes.ddl.is_empty() {
        None
    } else {
        validate_wire_ddl_provenance(&request.changeset)
            .map_err(|e| Error::SyncError(e.to_string()))?;
        let mut wire = request.changeset.clone();
        let mut indices = Vec::new();
        for (i, lsn) in wire.ddl_lsn.iter().enumerate() {
            if changes.ddl_lsn.contains(lsn) {
                indices.push(i);
            }
        }
        wire.ddl = indices.iter().map(|i| wire.ddl[*i].clone()).collect();
        wire.ddl_lsn = indices.iter().map(|i| wire.ddl_lsn[*i]).collect();
        wire.ddl_provenance = indices
            .iter()
            .map(|i| wire.ddl_provenance[*i].clone())
            .collect();
        received_ddl_context(&wire, tenant, edge, request.incarnation)
            .map_err(|e| Error::SyncError(e.to_string()))?
    };
    if let Some(ddl) = &ddl {
        db.validate_incoming_push_lineages_with_received_ddl(
            tenant,
            &changes,
            &lineages,
            edge,
            request.incarnation,
            ddl,
        )?;
        changes =
            db.reject_accepted_lineage_replays_with_received_ddl(tenant, changes, &lineages, ddl)?;
    } else {
        db.validate_incoming_push_lineages(tenant, &changes, &lineages, edge, request.incarnation)?;
        changes = db.reject_accepted_lineage_replays(tenant, changes, &lineages)?;
    }
    db.apply_custody_remainder(
        changes,
        arrivals,
        hub,
        tenant,
        edge,
        request.incarnation,
        &lineages,
        ddl.as_ref(),
        dependency_complete,
    )
}

fn merge_result(total: &mut ApplyResult, part: ApplyResult) {
    total.applied_rows += part.applied_rows;
    total.skipped_rows += part.skipped_rows;
    total.conflicts.extend(part.conflicts);
    total.new_lsn = total.new_lsn.max(part.new_lsn);
}
