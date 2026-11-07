//! Declared purge-following foreign keys (`REFERENCES ... FOLLOWS PARENT PURGE`).
//!
//! A child declares that its rows go with the parent row they reference when
//! that parent row is purged. Nothing here is inferred: only a declared
//! foreign key follows, and only a PURGE follows it. DELETE and DISCARD are
//! unchanged.
use super::*;

/// One declared following foreign key, read from the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PurgeFollower {
    pub(crate) child_table: String,
    /// The position of this foreign key among the child's following foreign
    /// keys: single-column ones in column order first, then composite ones in
    /// declaration order.
    pub(crate) ordinal: u32,
    pub(crate) child_columns: Vec<String>,
    pub(crate) parent_table: String,
    pub(crate) parent_columns: Vec<String>,
}

/// How many following foreign keys one table declares, counted in place.
pub(crate) fn declared_purge_follower_count(meta: &TableMeta) -> usize {
    meta.columns
        .iter()
        .filter(|column| {
            column
                .references
                .as_ref()
                .is_some_and(|reference| reference.follows_parent_purge)
        })
        .count()
        + meta
            .composite_foreign_keys
            .iter()
            .filter(|fk| fk.follows_parent_purge)
            .count()
}

/// The heap bytes the copies of one table's following foreign keys own, beyond
/// each destination slot: every cloned string's contents and every inner
/// column list's buffer. A cloned string and a cloned list allocate exactly
/// their length, so this is exact, counted in place.
pub(crate) fn declared_purge_follower_bytes(child_table: &str, meta: &TableMeta) -> usize {
    let list = |columns: usize| columns.saturating_mul(std::mem::size_of::<String>());
    let single = meta.columns.iter().filter_map(|column| {
        column
            .references
            .as_ref()
            .filter(|reference| reference.follows_parent_purge)
            .map(|reference| {
                list(1)
                    .saturating_add(column.name.len())
                    .saturating_add(reference.table.len())
                    .saturating_add(list(1))
                    .saturating_add(reference.column.len())
            })
    });
    let composite = meta
        .composite_foreign_keys
        .iter()
        .filter(|fk| fk.follows_parent_purge)
        .map(|fk| {
            fk.child_columns
                .iter()
                .chain(&fk.parent_columns)
                .map(String::len)
                .fold(
                    list(fk.child_columns.len())
                        .saturating_add(list(fk.parent_columns.len()))
                        .saturating_add(fk.parent_table.len()),
                    usize::saturating_add,
                )
        });
    single
        .chain(composite)
        .map(|bytes| bytes.saturating_add(child_table.len()))
        .fold(0, usize::saturating_add)
}

/// Push one table's following foreign keys, in ordinal order, into `out`.
/// With `out` reserved for [`declared_purge_follower_count`] more entries and
/// [`declared_purge_follower_bytes`] admitted, nothing else is allocated.
pub(crate) fn declared_purge_followers_into(
    child_table: &str,
    meta: &TableMeta,
    out: &mut Vec<PurgeFollower>,
) {
    let mut ordinal = 0_u32;
    for column in &meta.columns {
        if let Some(reference) = column
            .references
            .as_ref()
            .filter(|reference| reference.follows_parent_purge)
        {
            out.push(PurgeFollower {
                child_table: child_table.to_string(),
                ordinal,
                child_columns: vec![column.name.clone()],
                parent_table: reference.table.clone(),
                parent_columns: vec![reference.column.clone()],
            });
            ordinal = ordinal.saturating_add(1);
        }
    }
    for fk in meta
        .composite_foreign_keys
        .iter()
        .filter(|fk| fk.follows_parent_purge)
    {
        out.push(PurgeFollower {
            child_table: child_table.to_string(),
            ordinal,
            child_columns: fk.child_columns.clone(),
            parent_table: fk.parent_table.clone(),
            parent_columns: fk.parent_columns.clone(),
        });
        ordinal = ordinal.saturating_add(1);
    }
}

/// The following foreign keys one table declares, in ordinal order.
pub(crate) fn declared_purge_followers(child_table: &str, meta: &TableMeta) -> Vec<PurgeFollower> {
    let mut followers = Vec::with_capacity(declared_purge_follower_count(meta));
    declared_purge_followers_into(child_table, meta, &mut followers);
    followers
}

/// Every following foreign key in a catalog that points at `parent`.
pub(crate) fn purge_followers_of<'a>(
    catalog: impl IntoIterator<Item = (&'a str, &'a TableMeta)>,
    parent: &str,
) -> Vec<PurgeFollower> {
    let mut followers = catalog
        .into_iter()
        .flat_map(|(table, meta)| declared_purge_followers(table, meta))
        .filter(|follower| follower.parent_table == parent)
        .collect::<Vec<_>>();
    followers.sort_by(|left, right| {
        (&left.child_table, left.ordinal).cmp(&(&right.child_table, right.ordinal))
    });
    followers
}

/// The declaration's rules, checked on the catalog as it will be after a DDL
/// change, local or received:
///
/// 1. the parent columns are exactly the parent's erasure key, as
///    [`natural_key_columns_for_meta`] chooses it, so a delivered parent key
///    always supplies every child value;
/// 2. a declared index on the child leads with the child columns, so the
///    child rows are found by an index probe, never by a table scan;
/// 3. following never forms a cycle.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn validate_purge_followers(catalog: &HashMap<String, TableMeta>) -> Result<()> {
    let mut edges = BTreeMap::<String, BTreeSet<String>>::new();
    let mut names = catalog.keys().collect::<Vec<_>>();
    names.sort();
    for child_table in names {
        let meta = &catalog[child_table];
        for follower in declared_purge_followers(child_table, meta) {
            let Some(parent) = catalog.get(&follower.parent_table) else {
                return Err(Error::SchemaInvalid {
                    reason: format!(
                        "{child_table} follows the purge of {}, which does not exist",
                        follower.parent_table
                    ),
                });
            };
            if natural_key_columns_for_meta(parent).as_deref()
                != Some(follower.parent_columns.as_slice())
            {
                return Err(Error::SchemaInvalid {
                    reason: format!(
                        "{child_table}({}) can follow the purge of {} only through its erasure key; \
                         it references {}({})",
                        follower.child_columns.join(", "),
                        follower.parent_table,
                        follower.parent_table,
                        follower.parent_columns.join(", ")
                    ),
                });
            }
            // The engine builds an index for every foreign key; a projected
            // catalog may not have materialized it yet, so count it too.
            let automatic = crate::executor::auto_indexes_for_table_meta(meta);
            let covered = meta.indexes.iter().chain(&automatic).any(|index| {
                index.columns.len() >= follower.child_columns.len()
                    && index
                        .columns
                        .iter()
                        .zip(&follower.child_columns)
                        .all(|((column, _), child)| column == child)
            });
            if !covered {
                return Err(Error::SchemaInvalid {
                    reason: format!(
                        "{child_table}({}) follows the purge of {} but no index on {child_table} \
                         leads with those columns; declare one so the purge finds them by index",
                        follower.child_columns.join(", "),
                        follower.parent_table
                    ),
                });
            }
            edges
                .entry(follower.parent_table.clone())
                .or_default()
                .insert(child_table.clone());
        }
    }
    // A parent's purge reaches its followers' followers; refuse any loop.
    let mut finished = BTreeSet::<String>::new();
    let starts = edges.keys().cloned().collect::<Vec<_>>();
    for start in starts {
        if finished.contains(&start) {
            continue;
        }
        let mut on_path = BTreeSet::<String>::new();
        let children = |node: &str| -> Vec<String> {
            edges
                .get(node)
                .map(|set| set.iter().cloned().collect())
                .unwrap_or_default()
        };
        let mut path = vec![(start.clone(), children(&start))];
        on_path.insert(start);
        while let Some((node, pending)) = path.last_mut() {
            let node = node.clone();
            match pending.pop() {
                Some(next) => {
                    if on_path.contains(&next) {
                        return Err(Error::SchemaInvalid {
                            reason: format!(
                                "FOLLOWS PARENT PURGE forms a cycle through {node} and {next}"
                            ),
                        });
                    }
                    if !finished.contains(&next) {
                        let grandchildren = children(&next);
                        on_path.insert(next.clone());
                        path.push((next, grandchildren));
                    }
                }
                None => {
                    on_path.remove(&node);
                    finished.insert(node);
                    path.pop();
                }
            }
        }
    }
    Ok(())
}

/// The EXPLAIN line for a purge of these tables: every table that follows,
/// transitively, read from the catalog only.
pub(crate) fn explain_purge_followers<'a>(
    catalog: &HashMap<String, TableMeta>,
    named: impl IntoIterator<Item = &'a str>,
) -> Vec<String> {
    let mut lines = Vec::new();
    let mut seen = BTreeSet::<String>::new();
    let mut queue = named.into_iter().map(str::to_string).collect::<Vec<_>>();
    while let Some(parent) = queue.pop() {
        if !seen.insert(parent.clone()) {
            continue;
        }
        for follower in purge_followers_of(catalog.iter().map(|(k, v)| (k.as_str(), v)), &parent) {
            lines.push(format!(
                "follows parent purge: {}({}) -> {}({})",
                follower.child_table,
                follower.child_columns.join(", "),
                follower.parent_table,
                follower.parent_columns.join(", ")
            ));
            queue.push(follower.child_table);
        }
    }
    lines.sort();
    lines.dedup();
    lines
}
