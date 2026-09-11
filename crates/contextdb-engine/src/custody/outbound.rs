//! Explicit manifested units beside unchanged ordinary FK units.
use super::{canonical::*, preparation::current_manifest, records::*};
use crate::{Database, database::OutboundSyncUnit, sync_types::ChangeSet};
use contextdb_core::{Lsn, Result};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn units(
    db: &Database,
    changes: ChangeSet,
    confirmed: Lsn,
) -> Result<Vec<OutboundSyncUnit>> {
    let mut manifests = Vec::new();
    let mut seen = BTreeSet::new();
    for row in &changes.rows {
        if !row.deleted
            && db
                .table_meta(&row.table)
                .is_some_and(|m| m.delivery_manifest_tables.is_some())
            && seen.insert(
                RowRef {
                    table: row.table.clone(),
                    key: row.natural_key.clone(),
                }
                .order_key(),
            )
        {
            // A missing registration is still offered and individually refused by the hub.
            if let Some(manifest) = current_manifest(db, &row.table, &row.natural_key)? {
                manifests.push(manifest);
            }
        }
    }
    if manifests.is_empty() {
        return db.ordinary_dependency_complete_outbound_units(changes, confirmed);
    }
    let mut remaining = changes;
    let mut units = Vec::new();
    let mut covered = BTreeSet::new();
    let mut covered_vectors = BTreeSet::new();
    for m in manifests {
        let identities = m
            .rows
            .iter()
            .map(|e| e.reference.order_key())
            .collect::<BTreeSet<_>>();
        if identities.iter().any(|key| covered.contains(key)) {
            return Err(invalid());
        }
        covered.extend(identities.iter().cloned());
        covered_vectors.extend(
            std::iter::once(&m.root_life)
                .chain(&m.member_lives)
                .map(|life| (life.original_table.clone(), life.local_row_id)),
        );
        let mut unit = ChangeSet::default();
        remaining.rows.retain(|row| {
            if identities.contains(
                &RowRef {
                    table: row.table.clone(),
                    key: row.natural_key.clone(),
                }
                .order_key(),
            ) {
                unit.rows.push(row.clone());
                false
            } else {
                true
            }
        });
        remaining.vectors.retain(|v| {
            if std::iter::once(&m.root_life)
                .chain(&m.member_lives)
                .any(|life| life.original_table == v.index.table && life.local_row_id == v.row_id.0)
            {
                unit.vectors.push(v.clone());
                false
            } else {
                true
            }
        });
        if unit.rows.len() != m.rows.len() {
            return Err(invalid());
        }
        units.push(OutboundSyncUnit {
            changes: unit,
            dependency_complete: true,
        });
    }
    let manifested_count = units.len();
    // Ordinary rows retain the original dependency-complete builder. Any manifested
    // dependency it found is already present in its own explicit unit above.
    for mut ordinary in db.ordinary_dependency_complete_outbound_units(remaining, confirmed)? {
        ordinary.changes.rows.retain(|row| {
            !covered.contains(
                &RowRef {
                    table: row.table.clone(),
                    key: row.natural_key.clone(),
                }
                .order_key(),
            )
        });
        ordinary
            .changes
            .vectors
            .retain(|v| !covered_vectors.contains(&(v.index.table.clone(), v.row_id.0)));
        if !ordinary.changes.is_empty() {
            if ordinary.dependency_complete {
                units.push(ordinary);
            } else {
                units.extend(
                    ordinary
                        .changes
                        .split_by_data_lsn()
                        .into_iter()
                        .map(|changes| OutboundSyncUnit {
                            changes,
                            dependency_complete: false,
                        }),
                );
            }
        }
    }
    // Transport envelopes may carry several units. Join source-transaction siblings
    // and cross-unit FK dependencies so no earlier bookmark can strand a remainder;
    // this does not change a manifest's membership or grow its accepted unit.
    let mut parent = (0..units.len()).collect::<Vec<_>>();
    fn root(parent: &[usize], mut i: usize) -> usize {
        while parent[i] != i {
            i = parent[i];
        }
        i
    }
    let mut positions = BTreeMap::new();
    let mut rows = BTreeMap::new();
    for (i, unit) in units.iter().enumerate() {
        for row in &unit.changes.rows {
            rows.insert(
                RowRef {
                    table: row.table.clone(),
                    key: row.natural_key.clone(),
                }
                .order_key(),
                i,
            );
        }
        for lsn in unit
            .changes
            .rows
            .iter()
            .map(|r| r.lsn)
            .chain(unit.changes.ddl_lsn.iter().copied())
        {
            if let Some(j) = positions.insert(lsn, i) {
                let (a, b) = (root(&parent, i), root(&parent, j));
                parent[a] = b;
            }
        }
    }
    for (i, unit) in units.iter().enumerate() {
        for row in &unit.changes.rows {
            for reference in db.custody_parent_refs(row)? {
                if let Some(j) = rows.get(&reference.order_key())
                    && (i < manifested_count || *j < manifested_count)
                {
                    let (a, b) = (root(&parent, i), root(&parent, *j));
                    parent[a] = b;
                }
            }
        }
    }
    let mut joined = BTreeMap::<usize, OutboundSyncUnit>::new();
    for (i, unit) in units.into_iter().enumerate() {
        let group = joined
            .entry(root(&parent, i))
            .or_insert_with(|| OutboundSyncUnit {
                changes: ChangeSet::default(),
                dependency_complete: false,
            });
        group.dependency_complete |= unit.dependency_complete;
        group.changes.rows.extend(unit.changes.rows);
        group.changes.vectors.extend(unit.changes.vectors);
        group.changes.edges.extend(unit.changes.edges);
        group.changes.ddl.extend(unit.changes.ddl);
        group.changes.ddl_lsn.extend(unit.changes.ddl_lsn);
    }
    Ok(joined.into_values().collect())
}
