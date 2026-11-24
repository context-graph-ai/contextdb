//! The rules of a declared table beyond its columns, its key and its UNIQUE rules: what a
//! CREATE TABLE may say of them, the combinations that cannot stand, and what the table keeps.

use super::Catalog;
use super::check::{engine_index, invalid};
use crate::memory::Budget;
use crate::sql::ast;
use crate::sql::plan::CreateTablePlan;
use crate::store::CountId;
use crate::types::{
    ColumnType, ConflictPolicy, Error, History, KeptCounts, Result, SortDirection, SyncDirection,
    TableMeta, VectorIndexRef,
};

/// What a table keeps of its declaration beside `TableMeta`. A column is its place in
/// `meta.columns`; a state, an edge type and a label keep the letters they were written in.
#[derive(Debug, Clone, Default)]
pub(crate) struct Rules {
    /// The columns declared IMMUTABLE.
    pub immutable_columns: Vec<usize>,
    pub references: Vec<Reference>,
    pub propagate_edges: Vec<ast::EdgePropagation>,
    /// The states of `PROPAGATE ON STATE ... EXCLUDE VECTOR`.
    pub exclude_vector_states: Vec<String>,
    pub delivery_manifest: Vec<String>,
    pub edge_discard: Option<ast::EdgeDiscard>,
    /// The counts the table keeps: the lists of MAINTAIN COUNT, or every beginning of every
    /// index under ALL PREFIXES.
    pub counts: Vec<Count>,
    /// The column declared CONTEXT_ID.
    pub context_column: Option<usize>,
    /// The column that holds a row's labels, and the labels a reader and a writer need.
    pub scope_labels: Option<(usize, ast::ScopeLabels)>,
    /// The column declared ACL, then the table and the column of grants it references.
    pub acl: Option<(usize, String, String)>,
    pub rank_policies: Vec<RankPolicy>,
    pub expires_column: Option<usize>,
    pub retain_since: Option<u32>,
}

/// One reference of a table to a parent's key or UNIQUE rule.
#[derive(Debug, Clone)]
pub(crate) struct Reference {
    pub follows_parent_purge: bool,
    pub columns: Vec<usize>,
    pub parent: String,
    pub parent_columns: Vec<String>,
    pub on_state: Option<ast::Propagation>,
    /// The index of the table that leads with `columns`, by its place in `meta.indexes`.
    pub index: usize,
}

/// One kept count: its number in the store, and its columns with the direction of each.
#[derive(Debug, Clone)]
pub(crate) struct Count {
    pub id: CountId,
    pub index: usize,
    pub prefix: usize,
}

/// The RANK_POLICY of a VECTOR column.
#[derive(Debug, Clone)]
pub(crate) struct RankPolicy {
    /// The VECTOR column.
    pub column: usize,
    /// The column of this table whose value finds the joined row.
    pub anchor: usize,
    /// The joined table and column, the formula as it was written, and the sort key.
    pub policy: ast::RankPolicy,
}

pub(super) fn position(table: &str, meta: &TableMeta, name: &str) -> Result<usize> {
    let found = meta.columns.iter().position(|column| column.name == name);
    found.ok_or_else(|| Error::ColumnNotFound {
        table: table.to_owned(),
        column: name.to_owned(),
    })
}

/// Whether the index is one behind a key or a UNIQUE rule.
pub(super) fn is_rule(index: &crate::types::IndexDecl) -> bool {
    index.name.starts_with("__pk_") || index.name.starts_with("__unique_")
}

/// Checks every rule a CREATE TABLE declares and gives what the table keeps of them. Adds to
/// `meta` the indexes the rules need: one for the child side of a reference that no earlier
/// index leads with, and the two of a table of edges.
pub(super) fn of(
    create: &CreateTablePlan,
    meta: &mut TableMeta,
    catalog: Option<&Catalog>,
    budget: &Budget,
) -> Result<Rules> {
    let table = &create.name;
    let options = &create.options;
    let delivers = !matches!(
        meta.sync,
        Some(SyncDirection::Off | SyncDirection::PullOnly)
    );
    let current = meta.history == Some(History::CurrentOnly);
    let state = meta.state_machine.as_ref().map(|machine| &machine.column);
    let mut fixed = create.columns.iter().filter(|c| c.immutable);
    let refused = if meta.immutable && current {
        "IMMUTABLE with HISTORY CURRENT ONLY: an IMMUTABLE table has no older version to drop, \
         so leave one of the two out"
    } else if meta.immutable && meta.retain.is_some() {
        "IMMUTABLE with RETAIN: an IMMUTABLE table has no row to expire, so leave one of the two \
         out"
    } else if state.is_some() && (meta.immutable || fixed.any(|c| Some(&c.name) == state)) {
        "a STATE MACHINE over a state that is IMMUTABLE: a state that is never changed has no \
         next state, so leave one of the two out"
    } else if current && delivers && meta.conflict != Some(ConflictPolicy::KeepLatest) {
        "HISTORY CURRENT ONLY on a table that sends its rows out and keeps the first write: \
         another machine would take a newest value as the first, so write SYNC CONFLICT KEEP \
         LATEST or SYNC OFF"
    } else if meta.retain.as_ref().is_some_and(|retain| retain.sync_safe)
        && !meta.columns.iter().any(|column| column.primary_key)
    {
        "RETAIN ... SYNC SAFE without a PRIMARY KEY: declare a PRIMARY KEY so delivered rows \
         have an identity"
    } else if meta.retain.as_ref().is_some_and(|retain| retain.sync_safe) && !delivers {
        "RETAIN ... SYNC SAFE waits for a delivery that SYNC OFF and SYNC PULL ONLY never make: \
         leave SYNC SAFE out, or choose a direction that delivers"
    } else {
        ""
    };
    if !refused.is_empty() {
        return Err(invalid(format!("table '{table}' declares {refused}")));
    }
    if let Some(machine) = &meta.state_machine {
        position(table, meta, &machine.column)?;
    }
    let machine = meta.state_machine.iter().flat_map(|m| &m.transitions);
    let states = machine.flat_map(|(from, to)| std::iter::once(from).chain(to));
    let mut excluded = options.exclude_vector_states.iter();
    if let Some(state) = excluded.find(|s| !states.clone().any(|k| k == *s)) {
        return Err(invalid(format!(
            "table '{table}' declares EXCLUDE VECTOR for '{state}', no state of its STATE MACHINE"
        )));
    }

    let mut rules = Rules {
        propagate_edges: options.propagate_edges.clone(),
        exclude_vector_states: options.exclude_vector_states.clone(),
        delivery_manifest: options.delivery_manifest.clone(),
        edge_discard: options.edge_discard,
        ..Rules::default()
    };
    let of_columns = create.columns.iter();
    let of_columns =
        of_columns.filter_map(|c| Some((vec![c.name.clone()], c.references.as_ref()?)));
    let of_table = create.foreign_keys.iter();
    for (columns, written) in of_columns.chain(of_table.map(|(columns, to)| (columns.clone(), to)))
    {
        // A self-reference is settled by this declaration; only other tables need a catalog.
        let parent = match catalog.and_then(|catalog| catalog.table(&written.table)) {
            _ if written.table == *table => Some(&*meta),
            Some(parent) => Some(&parent.meta),
            None if catalog.is_none() => None,
            None => return Err(Error::TableNotFound(written.table.clone())),
        };
        let covers = |index: &crate::types::IndexDecl| {
            let names = index.columns.iter().map(|(name, _)| name);
            is_rule(index) && names.eq(&written.columns)
        };
        if parent.is_some_and(|parent| !parent.indexes.iter().any(covers)) {
            return Err(invalid(format!(
                "table '{table}': REFERENCES {}({}) names columns that no PRIMARY KEY or \
                 UNIQUE rule covers in that order; reference a key, or declare the UNIQUE rule on the parent \
                 first",
                written.table,
                written.columns.join(", ")
            )));
        }
        // A key matches only its own type, and REAL cannot be a key in any parent.
        for (n, (column, to)) in columns.iter().zip(&written.columns).enumerate() {
            let at = position(table, meta, column)?;
            let target =
                parent.and_then(|parent| parent.columns.iter().find(|parent| parent.name == *to));
            if written.columns[..n].contains(to)
                || meta.columns[at].column_type == ColumnType::Real
                || target.is_some_and(|to| to.column_type != meta.columns[at].column_type)
            {
                return Err(invalid(format!(
                    "table '{table}': '{column}' REFERENCES {}, but its type or target columns cannot match a key",
                    written.table
                )));
            }
        }
        let leads = |index: &crate::types::IndexDecl| {
            let names = index.columns.iter().map(|(name, _)| name);
            names.take(columns.len()).eq(&columns)
        };
        let index = match meta.indexes.iter().position(leads) {
            Some(index) => index,
            None => {
                let name = format!("__fk_{}", columns.join("_"));
                engine_index(table, meta, name, columns.clone(), false)?;
                meta.indexes.len() - 1
            }
        };
        rules.references.push(Reference {
            follows_parent_purge: written.follows_parent_purge,
            columns: columns
                .iter()
                .map(|c| position(table, meta, c))
                .collect::<Result<_>>()?,
            parent: written.table.clone(),
            parent_columns: written.columns.clone(),
            on_state: written.on_state.clone(),
            index,
        });
    }

    // A table of edges has exactly these three columns, and two indexes the engine makes.
    let has = |name: &str, column_type: ColumnType| {
        let mut columns = meta.columns.iter();
        columns.any(|column| column.name == name && column.column_type == column_type)
    };
    if !rules.propagate_edges.is_empty() && !has("id", ColumnType::Uuid) {
        return Err(invalid(format!(
            "table '{table}' declares PROPAGATE ON EDGE, which finds a row's edges by its id: \
             declare id UUID"
        )));
    }
    if has("source_id", ColumnType::Uuid)
        && has("target_id", ColumnType::Uuid)
        && has("edge_type", ColumnType::Text)
    {
        for (name, columns) in [
            (
                "source_target_type",
                ["source_id", "target_id", "edge_type"],
            ),
            (
                "target_source_type",
                ["target_id", "source_id", "edge_type"],
            ),
        ] {
            let columns = columns.map(str::to_owned).to_vec();
            engine_index(table, meta, format!("__graph_edge_{name}"), columns, false)?;
        }
    } else if !meta.dag.is_empty() {
        return Err(invalid(format!(
            "table '{table}' declares DAG, which is for a table of edges: declare source_id UUID, \
             target_id UUID and edge_type TEXT, or leave DAG out"
        )));
    }

    for (at, column) in create.columns.iter().enumerate() {
        if column.expires {
            if rules.expires_column.replace(at).is_some()
                || meta.columns[at].column_type != ColumnType::Timestamp
            {
                return Err(invalid(format!(
                    "table '{table}': declare one TIMESTAMP EXPIRES column"
                )));
            }
            if meta.retain.is_some() {
                let name = format!("__retain_{}", column.name);
                engine_index(table, meta, name, vec![column.name.clone()], false)?;
            }
        }
        if column.immutable {
            rules.immutable_columns.push(at);
        }
        let typed = |rule, wanted: ColumnType| match meta.columns[at].column_type == wanted {
            true => Ok(()),
            false => Err(invalid(format!(
                "table '{table}': declare {rule} on a {wanted} column"
            ))),
        };
        if column.context_id {
            typed("CONTEXT_ID", ColumnType::Uuid)?;
            rules.context_column = Some(at);
        }
        if let Some(labels) = &column.scope_labels {
            typed("SCOPE_LABEL", ColumnType::Text)?;
            rules.scope_labels = Some((at, labels.clone()));
        }
        if let Some((grants, grant)) = &column.acl {
            typed("ACL", ColumnType::Uuid)?;
            rules.acl = Some((at, grants.clone(), grant.clone()));
        }
        if let Some(policy) = &column.rank_policy {
            let anchor = rank_policy(table, &column.name, policy, meta, catalog, budget)?;
            rules.rank_policies.push(RankPolicy {
                column: at,
                anchor,
                policy: policy.clone(),
            });
        }
    }
    rules.counts = counts(
        table,
        meta,
        &[],
        catalog.unwrap_or(&Catalog::empty(budget)?),
    )?;
    Ok(rules)
}

/// The counts a table keeps under its MAINTAIN COUNT, each with its number: the number it has
/// in `kept` when it is kept already, and the next free number of the catalog otherwise.
pub(super) fn counts(
    table: &str,
    meta: &TableMeta,
    kept: &[Count],
    catalog: &Catalog,
) -> Result<Vec<Count>> {
    let indexes = || {
        meta.indexes
            .iter()
            .filter(|index| index.json_path.is_empty())
    };
    // A list that holds every column of a key or a UNIQUE rule counts one row at most.
    let bounded = |list: &[(String, SortDirection)]| {
        let listed = |(column, _): &(String, SortDirection)| list.iter().any(|(c, _)| c == column);
        indexes().any(|index| is_rule(index) && index.columns.iter().all(listed))
    };
    let lists = match &meta.kept_counts {
        None | Some(KeptCounts::None) => return Ok(Vec::new()),
        Some(KeptCounts::Lists(lists)) => lists.iter().map(Vec::as_slice).collect(),
        Some(KeptCounts::AllPrefixes) => {
            let mut all: Vec<&[(String, SortDirection)]> = Vec::new();
            for index in indexes() {
                for length in 1..=index.columns.len() {
                    let list = &index.columns[..length];
                    if !bounded(list) && !all.contains(&list) {
                        all.push(list);
                    }
                }
            }
            all
        }
    };
    let given = kept.iter().map(|count| count.id.0 + 1);
    let mut next = given.max().unwrap_or(0).max(catalog.numbers[2]);
    let mut found = Vec::with_capacity(lists.len());
    for (at, list) in lists.iter().enumerate() {
        let names: Vec<&str> = list.iter().map(|(column, _)| column.as_str()).collect();
        let unled;
        let refused = if lists[..at].contains(list) {
            "is written twice; write it once"
        } else if (1..names.len()).any(|at| names[..at].contains(&names[at])) {
            "names a column twice; name it once"
        } else if !indexes().any(|index| index.columns.starts_with(list)) {
            let looked: Vec<&str> = indexes().map(|index| index.name.as_str()).collect();
            unled = format!(
                "is led by none of the table's indexes ({}), so nothing could read the count; \
                 declare a UNIQUE rule that begins with these columns, or leave the list out",
                looked.join(", ")
            );
            &unled
        } else if bounded(list) {
            "holds every column of a key or a UNIQUE rule, which already says the count is at \
             most one; leave the list out"
        } else {
            ""
        };
        list.iter()
            .try_for_each(|(column, _)| position(table, meta, column).map(|_| ()))?;
        if !refused.is_empty() {
            return Err(invalid(format!(
                "table '{table}': the MAINTAIN COUNT list ({}) {refused}",
                names.join(", ")
            )));
        }
        let index = meta
            .indexes
            .iter()
            .position(|i| i.json_path.is_empty() && i.columns.starts_with(list))
            .unwrap();
        let prefix = list.len();
        let id = kept
            .iter()
            .find(|count| count.index == index && count.prefix == prefix)
            .map(|count| count.id);
        let id = id.unwrap_or_else(|| {
            next += 1;
            CountId(next - 1)
        });
        found.push(Count { id, index, prefix });
    }
    Ok(found)
}

/// Checks the RANK_POLICY of the VECTOR column `vector` and gives the column of the table that
/// finds the joined row: when the join is by the joined table's key, the one column named for
/// that table (`result_id` or `results_id` for `results`); else the column of the join column's
/// name; else the table's own key.
fn rank_policy(
    table: &str,
    vector: &str,
    policy: &ast::RankPolicy,
    meta: &TableMeta,
    catalog: Option<&Catalog>,
    budget: &Budget,
) -> Result<usize> {
    let refuse = |reason: String| Error::RankPolicyInvalid {
        index: VectorIndexRef {
            table: table.to_owned(),
            column: vector.to_owned(),
        },
        reason,
    };
    let (joined, on) = (&policy.table, &policy.column);
    let other = catalog
        .and_then(|catalog| catalog.table(joined))
        .map(|other| &other.meta);
    if other.is_none() && (catalog.is_some() || joined == table) {
        return Err(refuse(format!("it joins the unknown table '{joined}'")));
    }
    let column = other.and_then(|other| other.columns.iter().find(|column| column.name == *on));
    if other.is_some() && column.is_none() {
        return Err(refuse(format!("'{joined}' has no column '{on}'")));
    }
    let leads = |index: &crate::types::IndexDecl| {
        index.json_path.is_empty() && index.columns.first().is_some_and(|(name, _)| name == on)
    };
    if other.is_some_and(|other| !other.indexes.iter().any(leads)) {
        return Err(refuse(format!(
            "no index of '{joined}' leads with '{on}'; create one first"
        )));
    }
    let own = &meta.columns;
    let singular = joined.strip_suffix('s').unwrap_or(joined);
    let names = [format!("{singular}_id"), format!("{joined}_id")];
    let has = |name: &String| own.iter().any(|column| column.name == *name);
    let column_is_key = column.is_none_or(|column| column.primary_key);
    let mut found: Vec<_> = names
        .into_iter()
        .filter(|name| column_is_key && has(name))
        .collect();
    found.dedup();
    if found.is_empty() || (column.is_none() && found.len() != 1) {
        found = Some(on.clone()).filter(has).into_iter().collect();
    }
    if found.is_empty() {
        let key = own.iter().filter(|column| column.primary_key);
        found.extend(key.map(|column| column.name.clone()));
    }
    let [anchor] = &found[..] else {
        return Err(refuse(format!(
            "'{table}' needs one column to find the row of '{joined}' by, and has {}: {}",
            found.len(),
            found.join(", ")
        )));
    };

    let read = formula_reads(&policy.formula, budget).map_err(|error| match error {
        Error::MemoryBudgetExceeded { .. } => error,
        _ => refuse(format!("its FORMULA is not a formula: {error}")),
    })?;
    for name in read.iter().filter(|name| *name != "vector_score") {
        let local = own.iter().find(|column| column.name == *name);
        let remote =
            other.and_then(|other| other.columns.iter().find(|column| column.name == *name));
        let ambiguous = local.is_some() && (remote.is_some() || name == on);
        if ambiguous || (local.is_none() && remote.is_none() && other.is_some()) {
            return Err(refuse(format!(
                "its FORMULA reads '{name}', which is not a column of exactly one of '{table}' \
                 and '{joined}'"
            )));
        }
        use ColumnType::{Boolean, Integer, Real};
        let kind = local.or(remote).map(|column| column.column_type);
        if !matches!(kind, None | Some(Integer | Real | Boolean)) {
            return Err(refuse(format!(
                "its FORMULA reads '{name}', which is no numeric column"
            )));
        }
    }
    position(table, meta, anchor)
}

/// The names a rank formula reads as `{name}`, `vector_score` among them, or the parser's
/// refusal of a text that is not one.
pub(super) fn formula_reads(formula: &str, budget: &Budget) -> Result<Vec<String>> {
    fn reads(expr: &ast::Expr, names: &mut Vec<String>) {
        match expr {
            ast::Expr::Column(column) => names.push(column.column.clone()),
            ast::Expr::FunctionCall { args, .. } => args.iter().for_each(|arg| reads(arg, names)),
            _ => {}
        }
    }
    let mut names = Vec::new();
    let (formula, _held) = crate::sql::formula(formula, budget)?;
    reads(&formula, &mut names);
    Ok(names)
}
