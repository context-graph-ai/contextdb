//! Printing a declaration back as SQL.

use super::{Consolidation, Quantization, RESERVED_PREFIXES, Table};
use crate::sql::ast::{EdgeDiscard, Propagation};
use crate::types::{
    ColumnDefault, ConflictPolicy, History, KeptCounts, RetainUnit, SortDirection, SyncDirection,
    Value, VectorSearchMode,
};

/// A table's declaration as SQL: the CREATE TABLE, then one CREATE INDEX for each index the
/// author made, every statement ended by `;` and a new line. The clauses come in one order
/// whatever order they were written in. A clause left at its default is not printed, except
/// `MAX_PARTITIONS` of a column with a `PARTITION_KEY`, and neither are the indexes the engine makes for itself.
pub(super) fn declaration(table: &Table) -> String {
    let (meta, rules) = (&table.meta, &table.rules);
    let column_name = |at: &usize| name(&meta.columns[*at].name);
    let mut lines: Vec<String> = (0..meta.columns.len())
        .map(|at| column(table, at))
        .collect();
    if table.key_is_element {
        lines.push(format!("PRIMARY KEY ({})", list(&table.key, column_name)));
    }
    for rule in &meta.unique_constraints {
        lines.push(format!("UNIQUE ({})", list(rule, |column| name(column))));
    }
    for reference in rules.references.iter().filter(|r| r.columns.len() > 1) {
        let columns = list(&reference.columns, column_name);
        lines.push(format!("FOREIGN KEY ({columns}) {}", parent(reference)));
    }

    let mut options: Vec<String> = Vec::new();
    if meta.immutable {
        options.push("IMMUTABLE".to_owned());
    }
    if let Some(machine) = &meta.state_machine {
        let transition = |(from, to): &(String, Vec<String>)| {
            format!(
                "{} -> [{}]",
                as_written(from),
                list(to, |state| as_written(state))
            )
        };
        let transitions = list(&machine.transitions, transition);
        options.push(format!(
            "STATE MACHINE ({}: {transitions})",
            name(&machine.column)
        ));
    }
    if !meta.dag.is_empty() {
        options.push(format!("DAG({})", list(&meta.dag, |edge| quoted(edge))));
    }
    for edge in &rules.propagate_edges {
        let (edge_type, rule) = (as_written(&edge.edge_type), &edge.rule);
        options.push(format!(
            "PROPAGATE ON EDGE {edge_type} {} STATE {} SET {}{}",
            edge.direction,
            as_written(&rule.on_state),
            as_written(&rule.set),
            limits(rule)
        ));
    }
    for state in &rules.exclude_vector_states {
        options.push(format!(
            "PROPAGATE ON STATE {} EXCLUDE VECTOR",
            as_written(state)
        ));
    }
    if let Some(retain) = &meta.retain {
        let unit = match retain.unit {
            RetainUnit::Seconds => "SECONDS",
            RetainUnit::Minutes => "MINUTES",
            RetainUnit::Hours => "HOURS",
            RetainUnit::Days => "DAYS",
        };
        let safe = if retain.sync_safe { " SYNC SAFE" } else { "" };
        options.push(format!("RETAIN {} {unit}{safe}", retain.count));
    }
    let words = [
        meta.history.map(|history| match history {
            History::All => "HISTORY ALL",
            History::CurrentOnly => "HISTORY CURRENT ONLY",
        }),
        meta.sync.map(|sync| match sync {
            SyncDirection::Off => "SYNC OFF",
            SyncDirection::PushOnly => "SYNC PUSH ONLY",
            SyncDirection::PullOnly => "SYNC PULL ONLY",
            SyncDirection::TwoWay => "SYNC TWO WAY",
        }),
        meta.conflict.map(|conflict| match conflict {
            ConflictPolicy::KeepFirst => "SYNC CONFLICT KEEP FIRST",
            ConflictPolicy::KeepLatest => "SYNC CONFLICT KEEP LATEST",
        }),
    ];
    options.extend(words.into_iter().flatten().map(str::to_owned));
    if !rules.delivery_manifest.is_empty() {
        let over = list(&rules.delivery_manifest, |table| name(table));
        options.push(format!("DELIVERY MANIFEST OVER {over}"));
    }
    options.extend(rules.edge_discard.map(|discard| {
        let when = match discard {
            EdgeDiscard::Never => "NEVER",
            EdgeDiscard::AfterOutcome => "AFTER OUTCOME",
            EdgeDiscard::Always => "ALWAYS",
        };
        format!("EDGE DISCARD {when}")
    }));
    options.extend(meta.kept_counts.as_ref().map(|counts| {
        let counts = match counts {
            KeptCounts::None => "NONE".to_owned(),
            KeptCounts::AllPrefixes => "ALL PREFIXES".to_owned(),
            KeptCounts::Lists(lists) => list(lists, |columns| format!("({})", sorted(columns))),
        };
        format!("MAINTAIN COUNT {counts}")
    }));

    let table = name(&table.name);
    let mut text = format!("CREATE TABLE {table} (\n  {}\n)", lines.join(",\n  "));
    for option in options {
        text.push(' ');
        text.push_str(&option);
    }
    text.push_str(";\n");
    for index in &meta.indexes {
        if RESERVED_PREFIXES
            .iter()
            .any(|prefix| index.name.starts_with(*prefix))
        {
            continue;
        }
        let mut columns = sorted(&index.columns);
        if !index.json_path.is_empty() {
            let path = index.json_path.iter().map(|key| quoted(key));
            let path: Vec<String> = path.collect();
            columns = format!("({columns} -> {})", path.join(" -> "));
        }
        let index = name(&index.name);
        text.push_str(&format!("CREATE INDEX {index} ON {table} ({columns});\n"));
    }
    text
}

/// One column's line: its name, its type, and the clauses it was given.
fn column(table: &Table, at: usize) -> String {
    let (column, rules) = (&table.meta.columns[at], &table.rules);
    let mut text = name(&column.name);
    text.push(' ');
    text.push_str(&column.column_type.to_string());
    if let Some(vector) = table.vector(&column.name) {
        if vector.quantization == Quantization::Sq8 {
            text.push_str(" WITH (quantization = 'SQ8')");
        }
        // The limit on partitions refuses writes, so it is printed for every column with a
        // PARTITION_KEY, whether or not it was written. The clauses after it are printed only
        // when they were written.
        if let Some(limit) = vector.limit() {
            let key = list(&vector.partition_key, |key| name(key));
            text.push_str(&format!(" PARTITION_KEY ({key}) MAX_PARTITIONS {limit}"));
        }
        text.push_str(match vector.search_mode {
            None => "",
            Some(VectorSearchMode::Auto) => " SEARCH_MODE AUTO",
            Some(VectorSearchMode::Exact) => " SEARCH_MODE EXACT",
            Some(VectorSearchMode::Indexed) => " SEARCH_MODE INDEXED",
        });
        if let Some(at) = vector.auto_index_at {
            text.push_str(&format!(" AUTO_INDEX_AT {at}"));
        }
        match vector.consolidation {
            None => {}
            Some(Consolidation::None) => text.push_str(" CONSOLIDATION NONE"),
            Some(Consolidation::Percent { change, tombstone }) => {
                let members = [("CHANGE_PERCENT", change), ("TOMBSTONE_PERCENT", tombstone)];
                let written = members.iter().filter_map(|(member, percent)| {
                    percent.map(|percent| format!("{member} = {percent}"))
                });
                let written: Vec<String> = written.collect();
                text.push_str(&format!(" CONSOLIDATION ({})", written.join(", ")));
            }
        }
    }
    for ranked in rules.rank_policies.iter().filter(|p| p.column == at) {
        let policy = &ranked.policy;
        text.push_str(&format!(
            " RANK_POLICY (JOIN {} ON {}, FORMULA {}, SORT_KEY {})",
            name(&policy.table),
            name(&policy.column),
            quoted(&policy.formula),
            name(&policy.sort_key)
        ));
    }
    if let Some((_, labels)) = rules.scope_labels.as_ref().filter(|(c, _)| *c == at) {
        let read = list(&labels.read, |label| quoted(label));
        text.push_str(&match &labels.write {
            Some(write) => {
                let write = list(write, |label| quoted(label));
                format!(" SCOPE_LABEL_READ ({read}) WRITE ({write})")
            }
            None => format!(" SCOPE_LABEL ({read})"),
        });
    }
    if let Some((_, grants, grant)) = rules.acl.as_ref().filter(|(c, ..)| *c == at) {
        text.push_str(&format!(
            " ACL REFERENCES {}({})",
            name(grants),
            name(grant)
        ));
    }
    if rules.context_column == Some(at) {
        text.push_str(" CONTEXT_ID");
    }
    if column.primary_key && !table.key_is_element {
        text.push_str(" PRIMARY KEY");
    }
    if !column.nullable {
        text.push_str(" NOT NULL");
    }
    match &column.default {
        Some(ColumnDefault::Literal(value)) => {
            text.push_str(" DEFAULT ");
            text.push_str(&literal(value));
        }
        Some(ColumnDefault::Now) => text.push_str(" DEFAULT NOW()"),
        None => {}
    }
    if column.unique {
        text.push_str(" UNIQUE");
    }
    if rules.immutable_columns.contains(&at) {
        text.push_str(" IMMUTABLE");
    }
    for reference in rules.references.iter().filter(|r| r.columns == [at]) {
        text.push(' ');
        text.push_str(&parent(reference));
    }
    text
}

/// What a reference says after its own columns: the parent, its columns, and the rule.
fn parent(reference: &super::rules::Reference) -> String {
    let columns = list(&reference.parent_columns, |column| name(column));
    let mut text = format!("REFERENCES {}({columns})", name(&reference.parent));
    if let Some(rule) = &reference.on_state {
        text.push_str(&format!(
            " ON STATE {} PROPAGATE SET {}{}",
            as_written(&rule.on_state),
            as_written(&rule.set),
            limits(rule)
        ));
    }
    text
}

/// The end of a propagation rule: how far it goes and whether a failure stops it, as written.
fn limits(rule: &Propagation) -> String {
    let depth = rule.max_depth.map(|depth| format!(" MAX DEPTH {depth}"));
    let abort = if rule.abort_on_failure {
        " ABORT ON FAILURE"
    } else {
        ""
    };
    depth.unwrap_or_default() + abort
}

/// Columns with the direction of each, as an index or a kept count lists them.
fn sorted(columns: &[(String, SortDirection)]) -> String {
    list(columns, |(column, direction)| match direction {
        SortDirection::Asc => name(column),
        SortDirection::Desc => format!("{} DESC", name(column)),
    })
}

/// The items, each as `show` prints it, with a comma between two.
fn list<T>(items: &[T], show: impl Fn(&T) -> String) -> String {
    let items: Vec<String> = items.iter().map(show).collect();
    items.join(", ")
}

/// A default's value as the author wrote it.
fn literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::Bool(true) => "TRUE".to_owned(),
        Value::Bool(false) => "FALSE".to_owned(),
        Value::Int64(whole) => whole.to_string(),
        // A whole REAL keeps its decimal point, so it reads back as a REAL.
        Value::Float64(real) if real.fract() == 0.0 => format!("{real:.1}"),
        Value::Float64(real) => real.to_string(),
        Value::Text(words) => quoted(words),
        other => unreachable!("a default is declared as a literal, never as {other:?}"),
    }
}

/// A text between single quotes.
fn quoted(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// The name of a table, a column or an index as SQL reads it back: bare when it is one plain
/// word in lower case, which is how a name without quotes is stored, in double quotes otherwise.
fn name(name: &str) -> String {
    word(name, !name.contains(|c: char| c.is_ascii_uppercase()))
}

/// A state or an edge type, which keeps its letters with or without quotes: bare when it is
/// one plain word.
fn as_written(text: &str) -> String {
    word(text, true)
}

/// `text` bare when `bare` allows it and it is one plain word, in double quotes otherwise.
fn word(text: &str, bare: bool) -> String {
    let plain = text.starts_with(|first: char| first.is_ascii_alphabetic() || first == '_')
        && text.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if bare && plain {
        text.to_owned()
    } else {
        format!("\"{}\"", text.replace('"', "\"\""))
    }
}
