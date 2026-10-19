//! Custody ownership is erased with the selected row lives.
use super::{authority::object_digest, records::*};
use crate::Database;
use contextdb_core::Result;
use std::collections::BTreeSet;

// A multi-table purge extends the list result without changing the singular result.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum PurgeReportShape {
    None,
    Single,
    PerTable,
}

// Every authenticated source copy of the selected key belongs
// to its purge, including equivalent and refused submissions with local lives.
pub(crate) fn lineage_roots(
    db: &Database,
    table: &str,
    key: &crate::sync_types::NaturalKey,
    generation: u64,
    incumbent: &str,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Vec<String>> {
    let mut custody_hold = crate::custody::store::SelectionHold::new(read_memory.clone());

    let mut roots = BTreeSet::from([incumbent.to_owned()]);
    let reference = RowRef {
        table: table.into(),
        key: key.clone(),
    };
    for record in custody_hold
        .retain(db.custody_rows(std::slice::from_ref(&reference), read_memory.clone())?)?
    {
        if let Record::Manifest(manifest) = record {
            for row in &manifest.rows {
                if row.reference.table == table
                    && row.reference.key == *key
                    && row.creator.table_generation == generation
                {
                    roots.insert(row.creator.lineage_root.clone());
                }
            }
        }
    }
    Ok(roots.into_iter().collect())
}

pub(crate) fn metadata_keys(
    db: &Database,
    selected: &[RowRef],
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Vec<String>> {
    metadata_keys_matching(db, selected, None, read_memory)
}

/// Retired occurrences cannot erase custody for a new same-key incarnation.
/// Immutable creator evidence or an exact materialized local life must match.
pub(crate) fn metadata_keys_for_retired_lives(
    db: &Database,
    selected: &[(RowRef, u64, u64, String)],
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Vec<String>> {
    let _references_credit = contextdb_core::read_memory::ReadCredit::try_new(
        read_memory.clone(),
        selected.iter().map(|(reference, _, _, _)| {
            std::mem::size_of::<RowRef>().saturating_add(reference.table.len())
                .saturating_add(reference.key.held_bytes()).saturating_add(32)
        }).sum(),
        "writer_source", "retired_custody_references",
        "Raise MEMORY_LIMIT to prepare selected retired custody references.",
    )?;
    let references = selected.iter().map(|(reference, _, _, _)| reference.clone()).collect::<Vec<_>>();
    metadata_keys_matching(db, &references, Some(selected), read_memory)
}

fn metadata_keys_matching(
    db: &Database,
    selected: &[RowRef],
    lives: Option<&[(RowRef, u64, u64, String)]>,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Vec<String>> {
    let mut custody_hold = crate::custody::store::SelectionHold::new(read_memory.clone());
    let records = custody_hold.retain(db.custody_rows(selected, read_memory.clone())?)?;
    let matches_anchor = |anchor: &LocalLifeAnchor| lives.is_some_and(|lives|
        lives.iter().any(|(reference, generation, row, _)|
            anchor.original_table == reference.table && anchor.table_generation == *generation
                && anchor.local_row_id == *row));
    let matches_manifest = |manifest: &ManifestRecord| lives.map_or_else(
        || manifest.rows.iter().any(|row| selected.contains(&row.reference)),
        |lives| matches_anchor(&manifest.root_life)
            || manifest.member_lives.iter().any(&matches_anchor)
            || manifest.rows.iter().any(|row| lives.iter().any(|(reference, generation, _, root)|
                row.reference == *reference && row.creator.table_generation == *generation
                    && row.creator.lineage_root == *root)));

    let mut submissions = BTreeSet::new();
    for record in &records {
        match record {
            Record::Manifest(m) if matches_manifest(m) => {
                submissions.insert(m.id);
            }
            Record::MaterializedOwner(o) if lives.map_or_else(
                || selected.contains(&o.incumbent_reference), |_| matches_anchor(&o.incumbent)) => {
                submissions.insert(o.submission);
            }
            Record::Terminal { record: t, .. } if lives.is_none() && selected.contains(&t.root) => {
                submissions.insert(t.submission);
            }
            _ => {}
        }
    }
    let mut objects = BTreeSet::new();
    for record in &records {
        if let Record::Terminal { record: t, .. } = record
            && submissions.contains(&t.submission)
        {
            objects.insert(object_digest(1, &t.bytes()?, &t.signature));
        }
    }
    records
        .iter()
        .filter(|r| match r {
            Record::Root { submission, .. }
            | Record::MemberOwner { submission, .. }
            | Record::Diagnostic { submission, .. }
            | Record::DiagnosticOwner { submission, .. }
            | Record::SourceHistory { submission, .. }
            | Record::PolicyProfile { submission, .. }
            | Record::Order { submission, .. } => submissions.contains(submission),
            Record::Manifest(m) => submissions.contains(&m.id),
            Record::Terminal { record: t, .. } => submissions.contains(&t.submission),
            Record::MaterializedOwner(o) => submissions.contains(&o.submission),
            Record::ObjectAdmission(a) => objects.contains(&a.object_digest),
            _ => false,
        })
        .map(Record::key)
        .collect()
}
