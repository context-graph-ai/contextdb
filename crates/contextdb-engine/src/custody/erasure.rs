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
) -> Result<Vec<String>> {
    let mut roots = BTreeSet::from([incumbent.to_owned()]);
    let reference = RowRef {
        table: table.into(),
        key: key.clone(),
    };
    for record in db.custody_rows(std::slice::from_ref(&reference))? {
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

pub(crate) fn metadata_keys(db: &Database, selected: &[RowRef]) -> Result<Vec<String>> {
    let records = db.custody_rows(selected)?;
    let mut submissions = BTreeSet::new();
    for record in &records {
        match record {
            Record::Manifest(m) if m.rows.iter().any(|e| selected.contains(&e.reference)) => {
                submissions.insert(m.id);
            }
            Record::MaterializedOwner(o) if selected.contains(&o.incumbent_reference) => {
                submissions.insert(o.submission);
            }
            Record::Terminal { record: t, .. } if selected.contains(&t.root) => {
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
