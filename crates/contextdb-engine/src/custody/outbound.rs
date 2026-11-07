//! Explicit manifested units beside unchanged ordinary FK units.

use super::{canonical::*, preparation::current_manifest, records::*};
use crate::{Database, database::OutboundSyncUnit, sync_types::ChangeSet};
use contextdb_core::{Lsn, Result};
use std::collections::{BTreeMap, BTreeSet};

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn units(
    db: &Database,
    changes: ChangeSet,
    confirmed: Lsn,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
) -> Result<Vec<OutboundSyncUnit>> {
    let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

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
            if let Some(manifest) =
                current_manifest(db, &row.table, &row.natural_key, read_memory.clone())?
            {
                manifests.push(manifest);
            }
        }
    }
    if manifests.is_empty() {
        return db.ordinary_dependency_complete_outbound_units(
            changes,
            confirmed,
            vector_copy_owner,
            read_memory.clone(),
        );
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
    for mut ordinary in db.ordinary_dependency_complete_outbound_units(
        remaining,
        confirmed,
        vector_copy_owner,
        read_memory.clone(),
    )? {
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
    join_units(
        db,
        units,
        Some(manifested_count),
        vector_copy_owner,
        read_memory,
    )
}

/// Keep manifested envelopes together, or join data-only acceptances whose
/// selected parent rows are needed before their callbacks can run.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn join_units(
    db: &Database,
    units: Vec<OutboundSyncUnit>,
    manifested_count: Option<usize>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: contextdb_core::read_memory::ReadMemoryOwner,
) -> Result<Vec<OutboundSyncUnit>> {
    // Admit the grouping indexes before cloning identity keys. Row and vector
    // bodies move between units and keep their existing selection admission.
    let key_bytes = units
        .iter()
        .flat_map(|unit| &unit.changes.rows)
        .map(|row| {
            row.table
                .len()
                .saturating_add(row.natural_key.held_bytes())
                .saturating_mul(4)
                .saturating_add(512)
        })
        .fold(
            units
                .iter()
                .fold(units.len().saturating_mul(512), |bytes, unit| {
                    bytes.saturating_add(unit.changes.ddl.len().saturating_mul(128))
                }),
            usize::saturating_add,
        );
    vector_copy_owner.reserve(key_bytes)?;
    let units = if manifested_count.is_none() {
        units
            .into_iter()
            .flat_map(|unit| {
                if !unit.dependency_complete
                    && unit.changes.ddl.is_empty()
                    && unit.changes.edges.is_empty()
                    && unit.changes.vectors.is_empty()
                {
                    unit.changes
                        .rows
                        .into_iter()
                        .map(|row| OutboundSyncUnit {
                            changes: ChangeSet {
                                rows: vec![row],
                                ..ChangeSet::default()
                            },
                            dependency_complete: false,
                        })
                        .collect::<Vec<_>>()
                } else {
                    vec![unit]
                }
            })
            .collect::<Vec<_>>()
    } else {
        units
    };
    let ddl_positions = units
        .iter()
        .flat_map(|unit| unit.changes.ddl_lsn.iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let epochs = units
        .iter()
        .map(|unit| {
            if !unit.changes.ddl.is_empty() {
                return None;
            }
            let first = unit
                .changes
                .rows
                .iter()
                .map(|row| row.lsn)
                .chain(unit.changes.vectors.iter().map(|vector| vector.lsn))
                .chain(unit.changes.edges.iter().map(|edge| edge.lsn))
                .min()?;
            let last = unit.changes.max_lsn()?;
            let before = ddl_positions.partition_point(|position| *position < first);
            (before == ddl_positions.partition_point(|position| *position < last)).then_some(before)
        })
        .collect::<Vec<_>>();
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
        if manifested_count.is_none()
            && (epochs[i].is_none()
                || (!unit.dependency_complete
                    && (!unit.changes.edges.is_empty() || !unit.changes.vectors.is_empty())))
        {
            continue;
        }
        let epoch = if manifested_count.is_some() {
            0
        } else {
            epochs[i].expect("data epoch")
        };
        for row in &unit.changes.rows {
            if let Some(j) = rows.insert(
                (
                    epoch,
                    RowRef {
                        table: row.table.clone(),
                        key: row.natural_key.clone(),
                    }
                    .order_key(),
                ),
                i,
            ) && manifested_count.is_none()
            {
                let (a, b) = (root(&parent, i), root(&parent, j));
                parent[a] = b;
            }
        }
        for lsn in unit
            .changes
            .rows
            .iter()
            .map(|r| r.lsn)
            .chain(unit.changes.ddl_lsn.iter().copied())
        {
            if let Some(j) = positions.insert((epoch, lsn), i)
                && (manifested_count.is_some()
                    || (unit.dependency_complete && units[j].dependency_complete))
            {
                let (a, b) = (root(&parent, i), root(&parent, j));
                parent[a] = b;
            }
        }
    }
    for (i, unit) in units.iter().enumerate() {
        if manifested_count.is_none()
            && (epochs[i].is_none()
                || (!unit.dependency_complete
                    && (!unit.changes.edges.is_empty() || !unit.changes.vectors.is_empty())))
        {
            continue;
        }
        let epoch = if manifested_count.is_some() {
            0
        } else {
            epochs[i].expect("data epoch")
        };
        for row in &unit.changes.rows {
            for reference in db.custody_parent_refs(row, vector_copy_owner, read_memory.clone())? {
                if let Some(j) = rows.get(&(epoch, reference.order_key()))
                    && manifested_count.map_or_else(
                        || units[i].changes.ddl.is_empty() && units[*j].changes.ddl.is_empty(),
                        |count| i < count || *j < count,
                    )
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
    for unit in joined.values_mut() {
        // A row-only dependency envelope carries the current image per key.
        // Earlier authored bodies remain in local history, while ordinary and
        // schema-bearing units retain their distinct source occurrences.
        let current_image = unit.dependency_complete
            && unit.changes.ddl.is_empty()
            && unit.changes.vectors.is_empty()
            && unit.changes.edges.is_empty();
        let mut occurrences = BTreeMap::<_, Vec<usize>>::new();
        let mut unique: Vec<crate::sync_types::RowChange> =
            Vec::with_capacity(unit.changes.rows.len());
        for row in unit.changes.rows.drain(..) {
            let key = (
                RowRef {
                    table: row.table.clone(),
                    key: row.natural_key.clone(),
                }
                .order_key(),
                if current_image { Lsn(0) } else { row.lsn },
            );
            let positions = occurrences.entry(key).or_default();
            if current_image && let Some(index) = positions.first() {
                if row.lsn >= unique[*index].lsn {
                    unique[*index] = row;
                }
                continue;
            }
            if positions.iter().any(|index| unique[*index] == row) {
                continue;
            }
            positions.push(unique.len());
            unique.push(row);
        }
        unit.changes.rows = unique;
    }
    if manifested_count.is_none() {
        let mut ordinary = ChangeSet::default();
        let mut result = Vec::new();
        for unit in joined.into_values() {
            if unit.dependency_complete {
                result.push(unit);
            } else {
                ordinary.rows.extend(unit.changes.rows);
                ordinary.edges.extend(unit.changes.edges);
                ordinary.vectors.extend(unit.changes.vectors);
            }
        }
        result.extend(
            ordinary
                .split_by_data_lsn()
                .into_iter()
                .filter(|changes| !changes.is_empty())
                .map(|changes| OutboundSyncUnit {
                    changes,
                    dependency_complete: false,
                }),
        );
        return Ok(result);
    }
    Ok(joined.into_values().collect())
}
