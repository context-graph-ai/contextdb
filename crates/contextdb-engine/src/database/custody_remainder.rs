//! Statements 9/10/14: partition only the ordinary remainder of a custody envelope.
//! The normal apply seam still owns arbitration and dependency-complete refusal.
use super::*;

pub(crate) fn references(db: &Database, child: &RowChange, parent: &RowChange) -> bool {
    if child.deleted || parent.deleted {
        return false;
    }
    let Some(meta) = db.table_meta(&child.table) else {
        return false;
    };
    meta.columns.iter().any(|column| {
        column.references.as_ref().is_some_and(|fk| {
            fk.table == parent.table
                && child
                    .values
                    .get(&column.name)
                    .is_some_and(|v| *v != Value::Null && parent.values.get(&fk.column) == Some(v))
        })
    }) || meta.composite_foreign_keys.iter().any(|fk| {
        fk.parent_table == parent.table
            && fk
                .child_columns
                .iter()
                .zip(&fk.parent_columns)
                .all(|(c, p)| {
                    child
                        .values
                        .get(c)
                        .is_some_and(|v| *v != Value::Null && parent.values.get(p) == Some(v))
                })
    })
}

impl Database {
    pub(crate) fn custody_row_references(&self, child: &RowChange, parent: &RowChange) -> bool {
        references(self, child, parent)
    }
    pub(crate) fn custody_remainder_units(&self, changes: ChangeSet) -> Vec<OutboundSyncUnit> {
        let mut parent = (0..changes.rows.len()).collect::<Vec<_>>();
        fn root(parent: &[usize], mut i: usize) -> usize {
            while parent[i] != i {
                i = parent[i];
            }
            i
        }
        let mut related = HashSet::new();
        for (i, row) in changes.rows.iter().enumerate().filter(|(_, r)| !r.deleted) {
            if let Some(meta) = self.table_meta(&row.table) {
                let has_reference = meta.columns.iter().any(|c| {
                    c.references.is_some()
                        && row.values.get(&c.name).is_some_and(|v| *v != Value::Null)
                }) || meta.composite_foreign_keys.iter().any(|fk| {
                    fk.child_columns
                        .iter()
                        .all(|c| row.values.get(c).is_some_and(|v| *v != Value::Null))
                });
                if has_reference {
                    related.insert(i);
                }
            }
            for (j, other) in changes.rows.iter().enumerate() {
                if references(self, row, other) {
                    let (a, b) = (root(&parent, i), root(&parent, j));
                    parent[a] = b;
                    related.extend([i, j]);
                }
            }
        }
        // Connected components are indivisible. Unrelated rows keep the normal
        // single-source-position grouping, including paired schema migrations.
        let mut units = BTreeMap::<(bool, u64), OutboundSyncUnit>::new();
        let mut owners =
            HashMap::<(String, Lsn, bool), std::collections::VecDeque<(bool, u64)>>::new();
        let migration_lsns = changes.ddl_lsn.iter().copied().collect::<HashSet<_>>();
        for (i, row) in changes.rows.into_iter().enumerate() {
            let component = related.contains(&i) && !migration_lsns.contains(&row.lsn);
            let key = if component {
                (true, root(&parent, i) as u64)
            } else {
                (false, row.lsn.0)
            };
            owners
                .entry((row.table.clone(), row.lsn, row.deleted))
                .or_default()
                .push_back(key);
            units
                .entry(key)
                .or_insert_with(|| OutboundSyncUnit {
                    changes: ChangeSet::default(),
                    dependency_complete: component,
                })
                .changes
                .rows
                .push(row);
        }
        let mut cursor = 0;
        while cursor < changes.vectors.len() {
            let end = vector_row_group_end(&changes.vectors, cursor);
            let first = &changes.vectors[cursor];
            let key = owners
                .get_mut(&(
                    first.index.table.clone(),
                    first.lsn,
                    first.vector.is_empty(),
                ))
                .and_then(|v| v.pop_front())
                .unwrap_or((false, first.lsn.0));
            units
                .entry(key)
                .or_insert_with(|| OutboundSyncUnit {
                    changes: ChangeSet::default(),
                    dependency_complete: false,
                })
                .changes
                .vectors
                .extend_from_slice(&changes.vectors[cursor..end]);
            cursor = end;
        }
        for edge in changes.edges {
            units
                .entry((false, edge.lsn.0))
                .or_insert_with(|| OutboundSyncUnit {
                    changes: ChangeSet::default(),
                    dependency_complete: false,
                })
                .changes
                .edges
                .push(edge);
        }
        for (ddl, lsn) in changes.ddl.into_iter().zip(changes.ddl_lsn) {
            let unit = units
                .entry((false, lsn.0))
                .or_insert_with(|| OutboundSyncUnit {
                    changes: ChangeSet::default(),
                    dependency_complete: true,
                });
            unit.dependency_complete = true;
            unit.changes.ddl.push(ddl);
            unit.changes.ddl_lsn.push(lsn);
        }
        let mut units = units.into_values().collect::<Vec<_>>();
        units.sort_by_key(|unit| (unit.changes.max_lsn(), unit.changes.ddl.is_empty()));
        units
    }
}
