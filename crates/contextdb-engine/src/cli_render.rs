//! Public rendering helpers used by both the CLI binary and the test suite.

use crate::Database;
use crate::database::{ExplainOutput, QueryTrace};
use crate::sync_types::Conflict;
use contextdb_core::table_meta::{ColumnType, TableMeta};
use contextdb_core::{Result, Value};
use std::fmt::Write;

/// Render a column type as a DDL token.
pub fn render_column_type(col_type: &ColumnType) -> String {
    match col_type {
        ColumnType::Integer => "INTEGER".to_string(),
        ColumnType::Real => "REAL".to_string(),
        ColumnType::Text => "TEXT".to_string(),
        ColumnType::Boolean => "BOOLEAN".to_string(),
        ColumnType::Json => "JSON".to_string(),
        ColumnType::Uuid => "UUID".to_string(),
        ColumnType::Vector(dim) => {
            let text = format_args!("VECTOR({dim})");
            let bytes = metadata_text_len(text).expect("column type fits a String");
            let mut output = String::with_capacity(bytes);
            std::fmt::write(&mut output, text).expect("String rendering");
            output
        }
        ColumnType::Timestamp => "TIMESTAMP".to_string(),
        ColumnType::TxId => "TXID".to_string(),
    }
}

/// Render a table's `.schema` DDL. Auto-indexes (`kind == IndexKind::Auto`)
/// are suppressed from output to keep `.schema` focused on user-authored
/// DDL. Pass `render_table_meta_verbose` to include them.
pub fn render_table_meta(table: &str, meta: &TableMeta) -> String {
    render_table_meta_inner(table, meta, false)
}

/// Render a table's `.schema` DDL INCLUDING auto-indexes. Used by
/// `.schema --verbose` / `EXPLAIN SCHEMA t` for agents that need to see the
/// full picture.
pub fn render_table_meta_verbose(table: &str, meta: &TableMeta) -> String {
    render_table_meta_inner(table, meta, true)
}

fn render_table_meta_inner(table: &str, meta: &TableMeta, verbose: bool) -> String {
    // Public formatting has no engine owner parameter. Native metadata uses
    // the funded preparation below; both render exactly the same borrowed DDL.
    let mut propagation = Vec::with_capacity(meta.propagation_rules.len());
    for rule in &meta.propagation_rules {
        if !matches!(rule, contextdb_core::PropagationRule::ForeignKey { .. }) {
            propagation.push(crate::local_transport::query_funding::Funded::external(
                format!("{}", MetadataPropagation(rule)),
            ));
        }
    }
    propagation.sort_unstable_by(|left, right| left.value.cmp(&right.value));
    format!(
        "{}",
        MetadataDdl {
            table,
            meta,
            propagation: &propagation,
            verbose
        }
    )
}

// Native metadata rendering borrows each declaration. Only the final text and
// the ordered table-clause scratch have storage, admitted before construction.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn metadata_text(
    owner: contextdb_core::read_memory::ReadMemoryOwner,
    text: std::fmt::Arguments<'_>,
) -> Result<crate::local_transport::query_funding::Funded<String>> {
    let bytes = metadata_text_len(text)?;
    let mut result =
        crate::local_transport::query_funding::Funded::allocate_owned(Some(owner), bytes, || {
            String::with_capacity(bytes)
        })?;
    std::fmt::write(&mut result.value, text)
        .map_err(|_| contextdb_core::Error::Other("schema text size overflow".into()))?;
    Ok(result)
}

pub(crate) struct MetadataColumnType<'a>(pub(crate) &'a ColumnType);
impl std::fmt::Display for MetadataColumnType<'_> {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            ColumnType::Integer => out.write_str("INTEGER"),
            ColumnType::Real => out.write_str("REAL"),
            ColumnType::Text => out.write_str("TEXT"),
            ColumnType::Boolean => out.write_str("BOOLEAN"),
            ColumnType::Json => out.write_str("JSON"),
            ColumnType::Uuid => out.write_str("UUID"),
            ColumnType::Vector(dim) => write!(out, "VECTOR({dim})"),
            ColumnType::Timestamp => out.write_str("TIMESTAMP"),
            ColumnType::TxId => out.write_str("TXID"),
        }
    }
}
struct MetadataJoined<'a>(&'a [String]);
impl std::fmt::Display for MetadataJoined<'_> {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (at, item) in self.0.iter().enumerate() {
            if at != 0 {
                out.write_str(", ")?;
            }
            out.write_str(item)?;
        }
        Ok(())
    }
}
struct MetadataQuoted<'a>(&'a str, char);
impl std::fmt::Display for MetadataQuoted<'_> {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for part in self.0.split_inclusive(self.1) {
            out.write_str(part)?;
            if part.ends_with(self.1) {
                write!(out, "{}", self.1)?;
            }
        }
        Ok(())
    }
}
pub(crate) struct MetadataColumn<'a>(
    pub(crate) &'a contextdb_core::ColumnDef,
    pub(crate) &'a [contextdb_core::PropagationRule],
);
impl std::fmt::Display for MetadataColumn<'_> {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use contextdb_core::{PropagationRule, ScopeLabelKind, VectorSearchMode};
        let col = self.0;
        write!(out, "{}", MetadataColumnType(&col.column_type))?;
        write!(out, "{}", crate::database::vector_ivf::policy::With(col))?;
        if matches!(col.column_type, ColumnType::Vector(_)) {
            if let Some(keys) = &col.partition_key_columns
                && !keys.is_empty()
            {
                write!(
                    out,
                    " PARTITION_KEY ({}) MAX_PARTITIONS {}",
                    MetadataJoined(keys),
                    col.effective_max_partitions()
                        .unwrap_or(contextdb_core::DEFAULT_VECTOR_MAX_PARTITIONS)
                )?;
            }
            if col.search_mode != VectorSearchMode::Auto {
                write!(out, " SEARCH_MODE {}", col.search_mode)?;
            }
            if let Some(value) = col.auto_index_at {
                write!(out, " AUTO_INDEX_AT {value}")?;
            }
            if let Some(value) = col.auto_expansion_ceiling {
                write!(out, " AUTO_EXPANSION_CEILING {value}")?;
            }
            if let Some(value) = col.flat_below {
                write!(out, " FLAT_BELOW {value}")?;
            }
            write!(out, "{}", crate::database::vector_ivf::policy::Groups(col))?;
            if col.consolidation_disabled {
                out.write_str(" CONSOLIDATION NONE")?;
            } else {
                let mut first = true;
                for (name, value) in [
                    ("CHANGE_PERCENT", col.consolidation_change_percent),
                    ("TOMBSTONE_PERCENT", col.consolidation_tombstone_percent),
                ] {
                    if let Some(value) = value {
                        out.write_str(if first { " CONSOLIDATION (" } else { ", " })?;
                        first = false;
                        write!(out, "{name} = {value}")?;
                    }
                }
                if !first {
                    out.write_str(")")?;
                }
            }
        }
        if col.context_id {
            out.write_str(" CONTEXT_ID")?;
        }
        if let Some(scope) = &col.scope_label {
            let labels =
                |out: &mut std::fmt::Formatter<'_>, values: &[String]| -> std::fmt::Result {
                    for (at, value) in values.iter().enumerate() {
                        if at != 0 {
                            out.write_str(", ")?;
                        }
                        write!(out, "'{}'", MetadataQuoted(value, '\''))?;
                    }
                    Ok(())
                };
            match scope {
                ScopeLabelKind::Simple { write_labels } => {
                    out.write_str(" SCOPE_LABEL (")?;
                    labels(out, write_labels)?;
                    out.write_str(")")?;
                }
                ScopeLabelKind::Split {
                    read_labels,
                    write_labels,
                } => {
                    out.write_str(" SCOPE_LABEL_READ (")?;
                    labels(out, read_labels)?;
                    out.write_str(") WRITE (")?;
                    labels(out, write_labels)?;
                    out.write_str(")")?;
                }
            }
        }
        let rules = || {
            self.1.iter().filter(|rule| {
                matches!(rule,
            PropagationRule::ForeignKey { fk_column, .. } if fk_column == &col.name)
            })
        };
        if let Some(reference) = &col.references {
            write!(out, " REFERENCES {}({})", reference.table, reference.column)?;
            if reference.follows_parent_purge {
                out.write_str(" FOLLOWS PARENT PURGE")?;
            }
        } else if let Some(PropagationRule::ForeignKey {
            referenced_table,
            referenced_column,
            ..
        }) = rules().next()
        {
            write!(out, " REFERENCES {referenced_table}({referenced_column})")?;
        }
        for rule in rules() {
            if let PropagationRule::ForeignKey {
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
                ..
            } = rule
            {
                write!(
                    out,
                    " ON STATE {trigger_state} PROPAGATE SET {target_state}"
                )?;
                if *max_depth != 10 {
                    write!(out, " MAX DEPTH {max_depth}")?;
                }
                if *abort_on_failure {
                    out.write_str(" ABORT ON FAILURE")?;
                }
            }
        }
        if let Some(acl) = &col.acl_ref {
            write!(out, " ACL REFERENCES {}({})", acl.ref_table, acl.ref_column)?;
        }
        if col.primary_key {
            out.write_str(" PRIMARY KEY")?;
        }
        if !col.nullable && !col.primary_key {
            out.write_str(" NOT NULL")?;
        }
        if let Some(default) = &col.default {
            write!(out, " DEFAULT {default}")?;
        }
        if col.unique {
            out.write_str(" UNIQUE")?;
        }
        if col.expires {
            out.write_str(" EXPIRES")?;
        }
        if col.immutable {
            out.write_str(" IMMUTABLE")?;
        }
        if let Some(policy) = &col.rank_policy {
            write!(
                out,
                " RANK_POLICY (JOIN {} ON {}, FORMULA '{}', SORT_KEY {})",
                policy.joined_table,
                policy.joined_column,
                MetadataQuoted(&policy.formula, '\''),
                policy.sort_key
            )?;
        }
        Ok(())
    }
}
struct MetadataPropagation<'a>(&'a contextdb_core::PropagationRule);
impl std::fmt::Display for MetadataPropagation<'_> {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use contextdb_core::{Direction, PropagationRule};
        match self.0 {
            PropagationRule::Edge {
                edge_type,
                direction,
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } => {
                let direction = match direction {
                    Direction::Incoming => "INCOMING",
                    Direction::Outgoing => "OUTGOING",
                    Direction::Both => "BOTH",
                };
                write!(
                    out,
                    "PROPAGATE ON EDGE {edge_type} {direction} STATE {trigger_state} SET {target_state}"
                )?;
                if *max_depth != 10 {
                    write!(out, " MAX DEPTH {max_depth}")?;
                }
                if *abort_on_failure {
                    out.write_str(" ABORT ON FAILURE")?;
                }
                Ok(())
            }
            PropagationRule::VectorExclusion { trigger_state } => {
                write!(out, "PROPAGATE ON STATE {trigger_state} EXCLUDE VECTOR")
            }
            PropagationRule::ForeignKey { .. } => Ok(()),
        }
    }
}
struct MetadataDdl<'a> {
    table: &'a str,
    meta: &'a TableMeta,
    propagation: &'a [crate::local_transport::query_funding::Funded<String>],
    verbose: bool,
}
impl std::fmt::Display for MetadataDdl<'_> {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let meta = self.meta;
        writeln!(out, "CREATE TABLE {} (", self.table)?;
        for (at, col) in meta.columns.iter().enumerate() {
            if at != 0 {
                out.write_str(",\n")?;
            }
            write!(
                out,
                "  {} {}",
                col.name,
                MetadataColumn(col, &meta.propagation_rules)
            )?;
        }
        if !meta.primary_key_columns.is_empty() {
            write!(
                out,
                ",\n  PRIMARY KEY ({})",
                MetadataJoined(&meta.primary_key_columns)
            )?;
        }
        for columns in &meta.unique_constraints {
            write!(out, ",\n  UNIQUE ({})", MetadataJoined(columns))?;
        }
        out.write_str("\n)")?;
        if meta.immutable {
            out.write_str(" IMMUTABLE")?;
        }
        if let Some(state) = &meta.state_machine {
            write!(out, " STATE MACHINE ({}: ", state.column)?;
            // Borrow in lexical order without constructing a catalog copy or
            // sorting workspace. Equal keys cannot occur in this source map.
            let mut previous: Option<&str> = None;
            while let Some((from, to)) = state
                .transitions
                .iter()
                .filter(|(key, _)| previous.is_none_or(|previous| key.as_str() > previous))
                .min_by(|left, right| left.0.cmp(right.0))
            {
                if previous.is_some() {
                    out.write_str(", ")?;
                }
                write!(out, "{from} -> [{}]", MetadataJoined(to))?;
                previous = Some(from);
            }
            out.write_str(")")?;
        }
        if !meta.dag_edge_types.is_empty() {
            out.write_str(" DAG(")?;
            for (at, edge) in meta.dag_edge_types.iter().enumerate() {
                if at != 0 {
                    out.write_str(", ")?;
                }
                write!(out, "'{edge}'")?;
            }
            out.write_str(")")?;
        }
        if let Some(tables) = &meta.delivery_manifest_tables {
            out.write_str(" DELIVERY MANIFEST OVER ")?;
            for (at, table) in tables.iter().enumerate() {
                if at != 0 {
                    out.write_str(", ")?;
                }
                write!(out, "\"{}\"", MetadataQuoted(table, '"'))?;
            }
        }
        if let Some(mode) = meta.edge_discard {
            write!(
                out,
                " EDGE DISCARD {}",
                match mode {
                    contextdb_core::EdgeDiscardMode::Never => "NEVER",
                    contextdb_core::EdgeDiscardMode::AfterOutcome => "AFTER OUTCOME",
                    contextdb_core::EdgeDiscardMode::Always => "ALWAYS",
                }
            )?;
        }
        if let Some(seconds) = meta.default_ttl_seconds {
            let (window, unit) = match meta.retain_declared_unit {
                Some(unit) if unit.seconds_multiplier() != 0 => {
                    (seconds / unit.seconds_multiplier(), unit.sql())
                }
                _ if seconds.is_multiple_of(86400) => (seconds / 86400, "DAYS"),
                _ if seconds.is_multiple_of(3600) => (seconds / 3600, "HOURS"),
                _ if seconds.is_multiple_of(60) => (seconds / 60, "MINUTES"),
                _ => (seconds, "SECONDS"),
            };
            write!(out, " RETAIN {window} {unit}")?;
            if meta.sync_safe {
                out.write_str(" SYNC SAFE")?;
            }
        }
        if let Some(policy) = meta.history_policy {
            write!(out, " {}", policy.declared_clause())?;
        }
        if let Some(direction) = meta.sync_direction {
            write!(out, " {}", direction.sql())?;
        }
        if let Some(policy) = meta
            .conflict_policy
            .and_then(|policy| policy.declared_clause())
        {
            write!(out, " {policy}")?;
        }
        if let Some(counts) = &meta.maintained_counts {
            use contextdb_core::MaintainedCounts;
            match counts {
                MaintainedCounts::None => out.write_str(" MAINTAIN COUNT NONE")?,
                MaintainedCounts::AllPrefixes => out.write_str(" MAINTAIN COUNT ALL PREFIXES")?,
                MaintainedCounts::Lists(lists) => {
                    out.write_str(" MAINTAIN COUNT ")?;
                    for (at, columns) in lists.iter().enumerate() {
                        if at != 0 {
                            out.write_str(", ")?;
                        }
                        out.write_str("(")?;
                        for (at, (column, direction)) in columns.iter().enumerate() {
                            if at != 0 {
                                out.write_str(", ")?;
                            }
                            out.write_str(column)?;
                            if *direction == contextdb_core::SortDirection::Desc {
                                out.write_str(" DESC")?;
                            }
                        }
                        out.write_str(")")?;
                    }
                }
            }
        }
        for clause in self.propagation {
            write!(out, " {}", clause.value)?;
        }
        out.write_str(";\n")?;
        if self.verbose {
            let effective = crate::maintained_counts::effective_maintained_counts(meta);
            writeln!(
                out,
                "-- effective {}{}",
                effective.declared_clause(),
                if meta.maintained_counts.is_none() {
                    " (engine default; this table declared none)"
                } else {
                    ""
                }
            )?;
            if let contextdb_core::MaintainedCounts::Lists(lists) = &effective {
                for list in lists {
                    let indexes = crate::maintained_counts::indexes_leading_with(meta, list);
                    writeln!(
                        out,
                        "-- MAINTAIN COUNT ({}) is maintained by: {}",
                        contextdb_core::render_maintained_count_columns(list),
                        if indexes.is_empty() {
                            "no index".to_string()
                        } else {
                            indexes.join(", ")
                        }
                    )?;
                }
            }
        }
        for index in &meta.indexes {
            if !self.verbose && index.kind == contextdb_core::IndexKind::Auto {
                continue;
            }
            write!(out, "CREATE INDEX {} ON {} (", index.name, self.table)?;
            for (at, (column, direction)) in index.columns.iter().enumerate() {
                if at != 0 {
                    out.write_str(", ")?;
                }
                write!(
                    out,
                    "{column} {}",
                    match direction {
                        contextdb_core::SortDirection::Asc => "ASC",
                        contextdb_core::SortDirection::Desc => "DESC",
                    }
                )?;
            }
            out.write_str(");\n")?;
        }
        Ok(())
    }
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn render_table_meta_funded(
    owner: contextdb_core::read_memory::ReadMemoryOwner,
    table: &str,
    meta: &TableMeta,
) -> Result<crate::local_transport::query_funding::Funded<String>> {
    use crate::local_transport::query_funding::Funded;
    let count = meta
        .propagation_rules
        .iter()
        .filter(|rule| !matches!(rule, contextdb_core::PropagationRule::ForeignKey { .. }))
        .count();
    let bytes = count
        .checked_mul(std::mem::size_of::<Funded<String>>())
        .ok_or_else(|| contextdb_core::Error::Other("schema clause size overflow".into()))?;
    let mut propagation =
        Funded::allocate_owned(Some(owner.clone()), bytes, || Vec::with_capacity(count))?;
    for rule in &meta.propagation_rules {
        if !matches!(rule, contextdb_core::PropagationRule::ForeignKey { .. }) {
            propagation.value.push(metadata_text(
                owner.clone(),
                format_args!("{}", MetadataPropagation(rule)),
            )?);
        }
    }
    propagation
        .value
        .sort_unstable_by(|left, right| left.value.cmp(&right.value));
    metadata_text(
        owner,
        format_args!(
            "{}",
            MetadataDdl {
                table,
                meta,
                propagation: &propagation.value,
                verbose: false,
            }
        ),
    )
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn metadata_text_len(text: std::fmt::Arguments<'_>) -> Result<usize> {
    struct Count(usize);
    impl std::fmt::Write for Count {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            self.0 = self.0.checked_add(text.len()).ok_or(std::fmt::Error)?;
            Ok(())
        }
    }
    let mut count = Count(0);
    std::fmt::write(&mut count, text)
        .map_err(|_| contextdb_core::Error::Other("schema text size overflow".into()))?;
    Ok(count.0)
}

/// A table's `.schema --verbose` rendering, including the half that needs the
/// STORE rather than the schema: how many distinct prefix values each
/// maintained count currently holds.
///
/// This is the entry point `.schema --verbose` uses when it has a database in
/// hand. [`render_table_meta_verbose`] remains for callers that hold only a
/// `TableMeta`; it renders everything except the stored-value counts.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub fn render_table_meta_verbose_with_counts(db: &Database, table: &str) -> Result<String> {
    let mut rendered = match db.table_meta(table) {
        Some(meta) => render_table_meta_verbose(table, &meta),
        None => return Err(contextdb_core::Error::TableNotFound(table.to_string())),
    };
    rendered.push_str(&render_maintained_count_inventory(db, table)?);
    Ok(rendered)
}

/// What each of a table's maintained index-prefix counts currently costs to
/// keep: the column list, the indexes whose writes keep it current, and how
/// many distinct prefix values are stored for it right now.
///
/// Empty for a table that maintains nothing. A store that cannot be read is an
/// error rather than a report of zero.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn render_maintained_count_inventory(db: &Database, table: &str) -> Result<String> {
    let Some(meta) = db.table_meta(table) else {
        return Ok(String::new());
    };
    let effective = crate::maintained_counts::effective_maintained_counts(&meta);
    let lists = match &effective {
        contextdb_core::MaintainedCounts::Lists(lists) => lists.clone(),
        contextdb_core::MaintainedCounts::AllPrefixes => {
            crate::maintained_counts::every_maintained_prefix(&meta)
        }
        contextdb_core::MaintainedCounts::None => Vec::new(),
    };
    let mut out = String::new();
    for list in lists {
        let stored = db.stored_maintained_count_values(table, &list)?;
        writeln!(
            &mut out,
            "-- MAINTAIN COUNT ({}) holds {} stored prefix value(s), maintained by: {}",
            contextdb_core::render_maintained_count_columns(&list),
            stored,
            {
                let indexes = crate::maintained_counts::indexes_leading_with(&meta, &list);
                if indexes.is_empty() {
                    "no index".to_string()
                } else {
                    indexes.join(", ")
                }
            }
        )
        .unwrap();
    }
    Ok(out)
}

/// Render the route receipt from an ordinary relational `.explain`: the plan
/// that ran, its chosen index and pushdowns, rejected candidates, and whether
/// its ordering came from the route. The caller has already executed the
/// statement through the bounded read path that produced this trace.
pub fn render_explain_trace(trace: &QueryTrace) -> String {
    let mut out = String::new();
    out.push_str(trace.physical_plan);
    if let Some(idx) = &trace.index_used {
        out.push_str(&format!(" {{ index: {idx} }}"));
    }
    out.push('\n');
    if !trace.predicates_pushed.is_empty() {
        out.push_str("  predicates_pushed: [");
        for (i, p) in trace.predicates_pushed.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            out.push_str(p.as_ref());
        }
        out.push_str("]\n");
    }
    if !trace.indexes_considered.is_empty() {
        out.push_str("  indexes_considered: [");
        for (i, c) in trace.indexes_considered.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            out.push_str(&format!("{}: {}", c.name, c.rejected_reason));
        }
        out.push_str("]\n");
    }
    if trace.sort_elided {
        out.push_str("  sort_elided: true\n");
    }
    // Exactly one read path can consume a maintained index-prefix count, and
    // both answers to it are exact. This line is how a consumer tells which
    // one it got, and what DDL would change that.
    if let Some(maintained_count) = &trace.maintained_count {
        writeln!(&mut out, "  {maintained_count}").unwrap();
    }
    if let Some(vector_search) = &trace.vector_search {
        writeln!(&mut out, "{vector_search}").unwrap();
    }
    out
}

/// Render passive vector `.explain` metadata or a statically planned write.
/// These paths deliberately have no rows-examined or runtime-route fields.
pub fn render_explain_output(explained: &ExplainOutput) -> String {
    let mut out = String::new();
    out.push_str(explained.physical_plan.trim_end());
    if let Some(index) = &explained.index_used {
        out.push_str(&format!(" {{ index: {index} }}"));
    }
    out.push('\n');
    if !explained.predicates_pushed.is_empty() {
        out.push_str("  predicates_pushed: [");
        out.push_str(&explained.predicates_pushed.join(", "));
        out.push_str("]\n");
    }
    if !explained.indexes_considered.is_empty() {
        out.push_str("  indexes_considered: [");
        for (position, candidate) in explained.indexes_considered.iter().enumerate() {
            if position > 0 {
                out.push_str(", ");
            }
            write!(
                &mut out,
                "{}: {}",
                candidate.name, candidate.rejected_reason
            )
            .unwrap();
        }
        out.push_str("]\n");
    }
    if explained.sort_elided {
        out.push_str("  sort_elided: true\n");
    }
    if let Some(vector_search) = &explained.vector_search {
        writeln!(&mut out, "{vector_search}").unwrap();
    }
    out
}

/// Render `.explain <sql>` with its established split: execute an ordinary
/// relational SELECT/WITH to report its real route, inspect vector similarity
/// metadata without running the search, and only plan a write.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub fn render_explain(
    db: &Database,
    sql: &str,
    params: &std::collections::HashMap<String, Value>,
) -> contextdb_core::Result<String> {
    let read_memory = db.declared_reader_memory()?;
    render_explain_with_read_memory(db, sql, params, read_memory)
}

/// Execute using the caller's existing operation read owner.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub fn render_explain_with_read_memory(
    db: &Database,
    sql: &str,
    params: &std::collections::HashMap<String, Value>,
    read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
) -> contextdb_core::Result<String> {
    let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

    let statement = contextdb_parser::parse(sql)?;
    let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
    match &statement {
        contextdb_parser::Statement::Select(select)
            if contextdb_parser::classification::select_contains_vector_similarity(select) =>
        {
            db.explain_output_with_params(sql, params, &mut vector_copy_owner, read_memory.clone())
                .map(|explained| render_explain_output(&explained))
        }
        contextdb_parser::Statement::Select(_) => db
            .execute_with_read_memory(sql, params, read_memory.clone())
            .map(|result| render_explain_trace(result.trace())),
        _ => db
            .explain_output_with_params(sql, params, &mut vector_copy_owner, read_memory.clone())
            .map(|explained| render_explain_output(&explained)),
    }
}

pub fn render_query_trace(trace: &QueryTrace, rows_examined: u64) -> String {
    let mut out = format!("trace: {}", trace.physical_plan);
    if let Some(index) = &trace.index_used {
        out.push_str(&format!(" index={index}"));
    }
    if !trace.predicates_pushed.is_empty() {
        out.push_str(" pushed=[");
        for (idx, predicate) in trace.predicates_pushed.iter().enumerate() {
            if idx > 0 {
                out.push_str(", ");
            }
            out.push_str(predicate.as_ref());
        }
        out.push(']');
    }
    out.push_str(&format!(" rows_examined={rows_examined}"));
    if let Some(fraction) = trace.cache_resident_fraction {
        out.push_str(&format!(" cache_resident_fraction={fraction:.6}"));
    }
    if let Some(vector_search) = &trace.vector_search {
        out.push(' ');
        out.push_str(&vector_search.to_string());
    }
    out
}

/// Render a single `Value` as the CLI displays it in SELECT output.
pub fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Uuid(u) => u.to_string(),
        Value::Timestamp(ts) => ts.to_string(),
        Value::Json(j) => j.to_string(),
        Value::Vector(vs) => format!("{vs:?}"),
        Value::TxId(tx) => tx.0.to_string(),
    }
}

/// Render the `.sync status` output buffer. Includes the live committed-TxId.
pub fn render_sync_status(db: &Database) -> String {
    format!("Committed TxId: {}\n", db.committed_watermark().0)
}

/// Stable public document for one synchronous conflict receipt. Optional
/// facts are absent rather than fabricated, which matters for authority
/// refusals that deliberately have no winning value.
pub fn sync_conflict_document(conflict: &Conflict) -> serde_json::Value {
    let mut fields = serde_json::Map::from_iter([(
        "natural_key".to_string(),
        serde_json::to_value(&conflict.natural_key).expect("natural keys are serializable"),
    )]);
    for (name, value) in [
        ("table", conflict.table.clone()),
        ("mutation_kind", conflict.mutation_kind.clone()),
        ("reason", conflict.reason.clone()),
        (
            "winning_author_node_id",
            conflict.winning_author_node_id.clone(),
        ),
    ] {
        if let Some(value) = value {
            fields.insert(name.to_string(), serde_json::Value::String(value));
        }
    }
    if let Some(position) = conflict.hub_acceptance_position {
        fields.insert(
            "hub_acceptance_position".to_string(),
            serde_json::Value::Number(position.0.into()),
        );
    }
    // A member with nothing of its own to report still says why it was
    // turned away, so the reader is never left with a bare key and no cause.
    if let Some(cause) = conflict.refusal_cause.as_ref() {
        fields.insert(
            "refusal_cause".to_string(),
            serde_json::json!({
                "table": cause.table.clone(),
                "natural_key": serde_json::to_value(&cause.natural_key)
                    .expect("natural keys are serializable"),
            }),
        );
    }
    serde_json::Value::Object(fields)
}

/// Human rendering uses the same complete document as JSON and bundled
/// auto-sync, so no path can silently reduce a receipt to its reason string.
pub fn render_sync_conflict(conflict: &Conflict) -> String {
    sync_conflict_document(conflict).to_string()
}
#[cfg(test)]
mod sync_conflict_tests {
    use super::*;
    use crate::sync_types::ConflictPolicy;
    use crate::sync_types::NaturalKey;
    use contextdb_core::{Lsn, Value};

    #[test]
    fn conflict_document_keeps_every_provenance_field() {
        let conflict = Conflict {
            natural_key: NaturalKey::single("id".to_string(), Value::Int64(7)),
            resolution: ConflictPolicy::InsertIfNotExists,
            reason: Some("keep_first".to_string()),
            table: Some("camera_events".to_string()),
            mutation_kind: Some("delete".to_string()),
            winning_author_node_id: Some("ab".repeat(32)),
            hub_acceptance_position: Some(Lsn(41)),
            refusal_cause: None,
        };

        assert_eq!(
            sync_conflict_document(&conflict),
            serde_json::json!({
                "natural_key": { "column": "id", "value": { "Int64": 7 }, "rest": [] },
                "table": "camera_events",
                "mutation_kind": "delete",
                "reason": "keep_first",
                "winning_author_node_id": "ab".repeat(32),
                "hub_acceptance_position": 41,
            })
        );
    }

    #[test]
    fn winnerless_document_does_not_invent_winner_fields() {
        let conflict = Conflict {
            natural_key: NaturalKey::single("id".to_string(), Value::Int64(7)),
            resolution: ConflictPolicy::InsertIfNotExists,
            reason: Some("purge_requires_authoritative_hub".to_string()),
            table: Some("camera_events".to_string()),
            mutation_kind: Some("purge".to_string()),
            winning_author_node_id: None,
            hub_acceptance_position: None,
            refusal_cause: None,
        };

        let document = sync_conflict_document(&conflict);
        assert!(document.get("winning_author_node_id").is_none());
        assert!(document.get("hub_acceptance_position").is_none());
    }

    #[test]
    fn winnerless_document_names_the_row_that_caused_the_refusal() {
        let conflict = Conflict {
            natural_key: NaturalKey::single("id".to_string(), Value::Int64(9)),
            resolution: ConflictPolicy::InsertIfNotExists,
            reason: Some("dependency_complete_refused".to_string()),
            table: Some("camera_event_groups".to_string()),
            mutation_kind: Some("edit".to_string()),
            winning_author_node_id: None,
            hub_acceptance_position: None,
            refusal_cause: Some(crate::sync_types::RefusalCause {
                table: "camera_events".to_string(),
                natural_key: NaturalKey::single("id".to_string(), Value::Int64(7)),
            }),
        };

        let document = sync_conflict_document(&conflict);
        assert!(document.get("winning_author_node_id").is_none());
        assert!(document.get("hub_acceptance_position").is_none());
        assert_eq!(
            document.get("refusal_cause"),
            Some(&serde_json::json!({
                "table": "camera_events",
                "natural_key": { "column": "id", "value": { "Int64": 7 }, "rest": [] },
            }))
        );
    }
}
