//! Which index-prefix counts a table declares the engine keeps current on
//! every write, and what a read discloses when the count it wanted is not
//! declared.
//!
//! A maintained count is keyed by `(table, ordered column list, prefix
//! values)` and never by index name, so two indexes that share leading
//! columns name ONE count and the engine maintains it once. That is why the
//! declaration scopes to a table over an ordered column list rather than to
//! an index: scoping it to an index would declare the same count several
//! times, and it would not reach the counts that exist only under the
//! engine-derived `__fk_*` / `__unique_*` index names a schema author never
//! writes.
//!
//! An undeclared `COUNT(*)` is not refused and is not approximated: it takes
//! the ordinary index-range aggregate, which opens the exact prefix range and
//! counts live postings, so the answer is EXACT and the cost is one charged
//! index-entry touch per posting under that prefix, inside the statement's
//! own declared work, memory and time budget. [`MaintainedCountDisclosure`]
//! is what makes the difference visible on the trace, naming the exact
//! `ALTER TABLE ... SET MAINTAIN COUNT (...)` that would make it O(1).

use contextdb_core::table_meta::{
    DEFAULT_MAINTAINED_COUNTS, IndexDecl, IndexKind, MaintainedCounts, SortDirection, TableMeta,
};
use contextdb_core::{Error, Result, render_maintained_count_columns};

/// Whether the engine maintains a count over exactly these columns for this
/// table.
///
/// The constraint-bounded rule is a correctness rule, not policy, and runs
/// first: a count over a column list a primary key or unique constraint
/// already bounds at 0 or 1 is answerable from the index range itself, and
/// storing it would store a number nobody can disagree with. It therefore
/// removes a list from `ALL PREFIXES` and from an explicit `Lists` alike —
/// the explicit spelling of such a list is refused at DDL time
/// ([`validate_maintained_counts_for_meta`]), so a stored declaration never
/// contains one.
pub(crate) fn maintained_count_member(
    meta: &TableMeta,
    columns: &[(String, SortDirection)],
) -> bool {
    if crate::persistence::relational_index_prefix_is_constraint_bounded(meta, columns) {
        return false;
    }
    match meta
        .maintained_counts
        .as_ref()
        .unwrap_or(&DEFAULT_MAINTAINED_COUNTS)
    {
        MaintainedCounts::None => false,
        MaintainedCounts::AllPrefixes => true,
        MaintainedCounts::Lists(lists) => lists.iter().any(|list| list.as_slice() == columns),
    }
}

/// The column lists this table already declares, in the order it declared
/// them. Empty for a table that declares nothing, for `NONE`, and for
/// `ALL PREFIXES` — under `ALL PREFIXES` no read reaches the NotDeclared arm
/// over a list an index leads with, so there is no advice to widen.
pub(crate) fn declared_lists(meta: &TableMeta) -> Vec<Vec<(String, SortDirection)>> {
    match meta.maintained_counts.as_ref() {
        Some(MaintainedCounts::Lists(lists)) => lists.clone(),
        _ => Vec::new(),
    }
}

/// The value that actually governs this table's writes, with the declaration
/// resolved against the engine default when it declared nothing.
pub fn effective_maintained_counts(meta: &TableMeta) -> MaintainedCounts {
    meta.maintained_counts
        .clone()
        .unwrap_or(DEFAULT_MAINTAINED_COUNTS)
}

/// Every index declaration this table's counts are maintained under: the
/// engine-derived ones plus the user's own, the same set the write path
/// walks.
pub(crate) fn count_bearing_index_declarations(meta: &TableMeta) -> Vec<IndexDecl> {
    let mut declarations = crate::executor::auto_indexes_for_table_meta(meta);
    declarations.extend(
        meta.indexes
            .iter()
            .filter(|index| index.kind == IndexKind::UserDeclared)
            .cloned(),
    );
    declarations.sort_by(|left, right| left.name.cmp(&right.name));
    declarations.dedup_by(|left, right| left.name == right.name);
    declarations
}

/// The indexes whose leading run is exactly this column list, in order and
/// including directions — the indexes whose writes would keep this count
/// current, and the indexes whose reads can use it.
pub fn indexes_leading_with(meta: &TableMeta, columns: &[(String, SortDirection)]) -> Vec<String> {
    count_bearing_index_declarations(meta)
        .into_iter()
        .filter(|index| {
            index.columns.len() >= columns.len() && index.columns[..columns.len()] == *columns
        })
        .map(|index| index.name)
        .collect()
}

/// Every prefix `ALL PREFIXES` actually maintains: each prefix length of each
/// index declaration, less the ones a key already bounds, de-duplicated —
/// because a count is keyed by columns, so two indexes sharing a prefix name
/// one count.
pub(crate) fn every_maintained_prefix(meta: &TableMeta) -> Vec<Vec<(String, SortDirection)>> {
    let mut lists: Vec<Vec<(String, SortDirection)>> = Vec::new();
    for declaration in count_bearing_index_declarations(meta) {
        for prefix_len in 1..=declaration.columns.len() {
            let list = declaration.columns[..prefix_len].to_vec();
            if !maintained_count_member(meta, &list) || lists.contains(&list) {
                continue;
            }
            lists.push(list);
        }
    }
    lists
}

/// Judge a `MAINTAIN COUNT` declaration against the table it is declared on.
///
/// Every refusal names the offending list and the indexes that were
/// considered, because the author's next move is either to fix the list or to
/// add the index that would carry it.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn validate_maintained_counts_for_meta(table: &str, meta: &TableMeta) -> Result<()> {
    let Some(MaintainedCounts::Lists(lists)) = meta.maintained_counts.as_ref() else {
        return Ok(());
    };
    let considered = count_bearing_index_declarations(meta);
    let index_names = || {
        if considered.is_empty() {
            "none".to_string()
        } else {
            considered
                .iter()
                .map(|index| {
                    format!(
                        "{} ({})",
                        index.name,
                        render_maintained_count_columns(&index.columns)
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        }
    };
    let mut seen: Vec<&Vec<(String, SortDirection)>> = Vec::new();
    for list in lists {
        let spelled = render_maintained_count_columns(list);
        if list.is_empty() {
            return Err(Error::SchemaInvalid {
                reason: format!("MAINTAIN COUNT on '{table}' names an empty column list"),
            });
        }
        if seen.contains(&list) {
            return Err(Error::SchemaInvalid {
                reason: format!(
                    "MAINTAIN COUNT on '{table}' names the column list ({spelled}) twice; one \
                     list is one count"
                ),
            });
        }
        seen.push(list);
        for (column, _) in list {
            if !meta.columns.iter().any(|declared| declared.name == *column) {
                return Err(Error::SchemaInvalid {
                    reason: format!(
                        "MAINTAIN COUNT ({spelled}) on '{table}' names column '{column}', which \
                         the table does not declare"
                    ),
                });
            }
        }
        if list
            .iter()
            .enumerate()
            .any(|(at, (column, _))| list[..at].iter().any(|(earlier, _)| earlier == column))
        {
            return Err(Error::SchemaInvalid {
                reason: format!(
                    "MAINTAIN COUNT ({spelled}) on '{table}' names the same column twice"
                ),
            });
        }
        if crate::persistence::relational_index_prefix_is_constraint_bounded(meta, list) {
            return Err(Error::SchemaInvalid {
                reason: format!(
                    "MAINTAIN COUNT ({spelled}) on '{table}' is already bounded at 0 or 1 by a \
                     primary key or unique constraint over those columns, so an exact-key range \
                     over the index answers it by reading at most one posting; remove the list"
                ),
            });
        }
        if indexes_leading_with(meta, list).is_empty() {
            return Err(Error::SchemaInvalid {
                reason: format!(
                    "MAINTAIN COUNT ({spelled}) on '{table}' names a column list no index leads \
                     with, so nothing could ever read the count; indexes considered: {}",
                    index_names()
                ),
            });
        }
    }
    Ok(())
}

/// What a `COUNT(*)` over an exact index prefix actually got, and — when it
/// got the ordinary index-range aggregate — why, and the declaration that
/// would make it O(1).
///
/// The three states are distinct on purpose. "The stored count did not come
/// back" is not the same fact as "this table declares no such count", and
/// reporting the first as the second would advise a declaration that is
/// already in force.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaintainedCountDisclosure {
    /// The answer came from a maintained count over these columns.
    Used {
        table: String,
        columns: Vec<(String, SortDirection)>,
    },
    /// The table DOES declare a maintained count over these columns, and the
    /// write path keeps it current, but this read route did not serve the
    /// answer from it: the stored count is reachable only through a committed
    /// read image (`Database::open_committed_image`'s retained relational
    /// source), so a writable handle and the owner route answer the identical
    /// number from the index range instead. No DDL changes this.
    DeclaredNotServed {
        table: String,
        columns: Vec<(String, SortDirection)>,
    },
    /// The shape was eligible for a maintained count, but this table declares
    /// none over these columns. The answer is exact and came from the index
    /// range instead.
    NotDeclared {
        table: String,
        columns: Vec<(String, SortDirection)>,
        /// What the table already declares. `SET MAINTAIN COUNT` REPLACES the
        /// whole declared set, so advice that named only the missing list
        /// would tell a consumer to drop every count it already asked for.
        /// The advised statement is these lists plus the missing one.
        already_declared: Vec<Vec<(String, SortDirection)>>,
    },
}

impl MaintainedCountDisclosure {
    /// The `ALTER TABLE` that makes this count O(1) without giving anything
    /// up, ready to run as printed.
    pub fn advised_statement(&self) -> Option<String> {
        let Self::NotDeclared {
            table,
            columns,
            already_declared,
        } = self
        else {
            return None;
        };
        let mut lists = already_declared.clone();
        if !lists.iter().any(|list| list == columns) {
            lists.push(columns.clone());
        }
        Some(format!(
            "ALTER TABLE {} SET MAINTAIN COUNT {}",
            contextdb_core::render_sql_identifier(table),
            lists
                .iter()
                .map(|list| format!("({})", render_maintained_count_columns(list)))
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }
}

impl std::fmt::Display for MaintainedCountDisclosure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Used { columns, .. } => write!(
                formatter,
                "maintained count: used ({})",
                render_maintained_count_columns(columns)
            ),
            Self::DeclaredNotServed { columns, .. } => write!(
                formatter,
                "maintained count: declared ({}), counted from the index range on this read \
                 route",
                render_maintained_count_columns(columns)
            ),
            Self::NotDeclared { .. } => write!(
                formatter,
                "maintained count: not declared — {} makes this O(1)",
                self.advised_statement()
                    .expect("a NotDeclared disclosure advises a statement")
            ),
        }
    }
}
