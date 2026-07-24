//! Machine output for the REPL's meta-commands.
//!
//! Under `--json` stdout is JSON Lines: one complete JSON document per line,
//! one line per statement or meta-command that produced a result. Nothing else
//! is written there — help text, execution traces and every error go to stderr,
//! so a consumer can parse the pipe without filtering.
//!
//! This module is private on purpose. The crate's public surface is `run`,
//! `OutputOptions` and the `EXIT_*` codes; the document shapes are a CLI
//! contract documented in `docs/cli.md`, not a Rust API.

use contextdb_core::Direction;
use contextdb_core::table_meta::{
    ConflictPolicy, IndexKind, PropagationRule, SortDirection, SyncDirection, TableMeta,
};
use contextdb_engine::QueryResult;
use contextdb_engine::cli_render;
use contextdb_engine::database::QueryTrace;
use contextdb_engine::{CompactionReport, MaintenancePolicy, MaintenanceReport, MaintenanceStatus};
use serde_json::{Map, Value, json};

/// Which family an error belongs to, for the `--json` error envelope.
///
/// This is the branch a caller actually needs — retry against a different hub,
/// fix the SQL, fix the command line, look at the disk — and it is why the exit
/// codes stay coarse. The engine's own variant NAME is deliberately not
/// published: ~100 enum variants would become a wire contract with no consumer,
/// while `message` already carries the full text.
///
/// Public because the CLI binary reports its own startup and shutdown failures
/// through the same envelope.
///
/// # What is promised
///
/// The four names this renders — `sql`, `sync`, `io`, `usage` — ARE the machine
/// contract, published in `docs/cli.md`. A consumer may branch on them, and they
/// do not change or disappear under a caller. Which family a given failure falls
/// into may be corrected as the engine grows, and the `message` beside the class
/// is human-facing prose whose wording is free to change at any time: read the
/// class to decide what to do, and the message only to show a person. A new
/// family would be an addition, so a consumer should treat an unrecognized class
/// as "something failed" rather than as a parse error.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub enum ErrorClass {
    /// The statement or the data model is wrong: fix the query.
    Sql,
    /// The hub or the transport is the problem: the request may be fine.
    Sync,
    /// The store, the disk or a budget: look at the machine.
    Io,
    /// The invocation itself is wrong: fix the command line.
    Usage,
}

impl ErrorClass {
    /// Classify an engine error.
    pub fn of(error: &contextdb_core::Error) -> Self {
        use contextdb_core::Error;
        match error {
            Error::SyncError(_) | Error::NotSyncEligible(_) | Error::SyncPushUnconfirmed { .. } => {
                ErrorClass::Sync
            }
            Error::StoreCorrupted { .. }
            | Error::LegacyVectorStoreDetected { .. }
            | Error::DatabaseLocked { .. }
            | Error::ExportIo { .. }
            | Error::ExportDestinationExists { .. }
            | Error::DiskBudgetExceeded { .. }
            | Error::MemoryBudgetExceeded { .. } => ErrorClass::Io,
            _ => ErrorClass::Sql,
        }
    }

    /// The value written to the envelope's `class` field.
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorClass::Sql => "sql",
            ErrorClass::Sync => "sync",
            ErrorClass::Io => "io",
            ErrorClass::Usage => "usage",
        }
    }
}

/// The wire word for a table's sync direction.
///
/// Declared here rather than derived from the Rust enum: `format!("{:?}")` would
/// make the identifier spelling of `SyncDirection` the contract, so renaming a
/// variant — or a change in how `Debug` renders — would silently rewrite what
/// every consumer parses, with no product intent behind it. Each word is the
/// DDL clause an operator writes (`SyncDirection::sql`) in lowercase
/// snake_case, so the CLI, the declaration and the persisted meta all say the
/// same thing.
///
/// The match is exhaustive on purpose. A new direction must be given a word
/// here deliberately; it must not fall through to a default.
pub(crate) fn sync_direction_wire_word(direction: SyncDirection) -> &'static str {
    match direction {
        SyncDirection::None => "sync_off",
        SyncDirection::Push => "push_only",
        SyncDirection::Pull => "pull_only",
        SyncDirection::Both => "two_way",
    }
}

/// The wire word for a propagation rule's edge direction — the DDL keyword an
/// operator writes in `PROPAGATE ON EDGE <type> INCOMING|OUTGOING|BOTH`.
///
/// Spelled out for the same reason as the two above: the previous
/// `format!("{:?}").to_uppercase()` happened to produce the DDL keyword, which
/// made a Rust identifier the wire contract by coincidence. The words are
/// unchanged; only the coupling is.
pub(crate) fn propagation_direction_wire_word(direction: &Direction) -> &'static str {
    match direction {
        Direction::Incoming => "INCOMING",
        Direction::Outgoing => "OUTGOING",
        Direction::Both => "BOTH",
    }
}

/// The wire word for a table's conflict policy, on the same terms.
///
/// The two DDL-declarable policies take their clause words
/// (`ConflictPolicy::declared_clause`). `ServerWins` and `EdgeWins` have no
/// clause — the engine bakes them into its own distributed-contract tables
/// rather than letting a schema declare them — but a `.schema` on one of those
/// tables still carries the field, so they take the word their role is named
/// by.
pub(crate) fn conflict_policy_wire_word(policy: ConflictPolicy) -> &'static str {
    match policy {
        ConflictPolicy::InsertIfNotExists => "keep_first",
        ConflictPolicy::LatestWins => "keep_latest",
        ConflictPolicy::ServerWins => "server_wins",
        ConflictPolicy::EdgeWins => "edge_wins",
    }
}

/// The wire word for a table's version-history policy, on the same terms.
pub(crate) fn history_policy_wire_word(policy: contextdb_core::HistoryPolicy) -> &'static str {
    match policy {
        contextdb_core::HistoryPolicy::All => "ALL",
        contextdb_core::HistoryPolicy::CurrentOnly => "CURRENT_ONLY",
    }
}

/// The wire word for who owns a database's maintenance schedule.
pub(crate) fn maintenance_policy_wire_word(policy: MaintenancePolicy) -> &'static str {
    match policy {
        MaintenancePolicy::EngineOwned => "engine_owned",
        MaintenancePolicy::CallerDriven => "caller_driven",
    }
}

/// `.maintenance status` under `--json`: what this database maintains and
/// whether the loop is currently running.
pub(crate) fn maintenance_status_document(status: &MaintenanceStatus) -> Value {
    json!({
        "maintenance": {
            "running": status.running,
            "retention_enabled": status.retention_enabled,
            "currency_compaction_enabled": status.currency_compaction_enabled,
            "active_maintenance_loops": status.active_maintenance_loops,
            "policy": maintenance_policy_wire_word(status.policy),
        }
    })
}

/// `.maintenance run` under `--json`: what the one driven cycle reclaimed.
/// `currency_redb_compacted` now always reads `false` here: currency version
/// cleanup never compacts on its own (see `Database::compact_now`'s doc
/// comment) — the cycle's own separate, rare automatic-compaction attempt is
/// `compaction` below.
pub(crate) fn maintenance_report_document(report: &MaintenanceReport) -> Value {
    json!({
        "maintenance_cycle": {
            "pruned_rows": report.pruning.pruned_rows,
            "reclaimed_bytes": report.pruning.reclaimed_bytes,
            "file_shrank": report.pruning.file_shrank,
            "currency_pruned_versions": report.currency.pruned_versions,
            "currency_versions_deferred_for_readers": report.currency.versions_deferred_for_readers,
            "currency_redb_compacted": report.currency.redb_compacted,
            "pruned_trigger_audit_rows": report.pruned_trigger_audit_rows,
            "compaction": compaction_report_fields(&report.compaction),
        }
    })
}

/// The shared field shape both `.maintenance run`'s `compaction` sub-document
/// and `.maintenance compact`'s own top-level document render — one source
/// for what an operator-visible compaction receipt looks like.
fn compaction_report_fields(report: &CompactionReport) -> Value {
    json!({
        "ran": report.ran,
        "duration_micros": report.duration_micros,
        "bytes_before": report.bytes_before,
        "bytes_after": report.bytes_after,
        "file_shrank": report.file_shrank,
        "fragmentation_before": report.fragmentation_before,
    })
}

/// `.maintenance compact` under `--json`: the explicit, on-demand
/// full-file redb compaction this call just ran (or, on an in-memory
/// database, the honest no-op).
pub(crate) fn compaction_report_document(report: &CompactionReport) -> Value {
    json!({ "compaction": compaction_report_fields(report) })
}

/// Write one error to stderr as a JSON document. `line` is the input line the
/// failing statement started on, when the CLI knows it; the message never
/// repeats it as a prefix, so a consumer reads the two separately.
pub(crate) fn print_error(class: ErrorClass, message: &str, line: Option<usize>) {
    let mut error = Map::new();
    error.insert("class".to_string(), json!(class.as_str()));
    error.insert("message".to_string(), json!(message));
    if let Some(line) = line {
        error.insert("line".to_string(), json!(line));
    }
    eprintln!(
        "{}",
        Value::Object(Map::from_iter([(
            "error".to_string(),
            Value::Object(error)
        )]))
    );
}

/// Write one NOTICE to stderr as a JSON document: something the operator should
/// see that is not a failure — a deprecation, an unreachable endpoint the CLI
/// will retry, a push whose outcome is merely unknown. Calling these errors
/// would tell a consumer the run failed when it did not.
pub(crate) fn print_notice(class: ErrorClass, message: &str) {
    eprintln!(
        "{}",
        json!({ "notice": { "class": class.as_str(), "message": message } })
    );
}

/// Print one result document to stdout.
pub(crate) fn print_document(document: &Value) {
    println!("{document}");
}

/// `.tables` — the table names this database holds, in the order the engine
/// lists them (sorted).
pub(crate) fn tables_document(names: Vec<String>) -> Value {
    json!({ "tables": names })
}

/// `.trace on|off` — the toggle's resulting state.
pub(crate) fn trace_state_document(enabled: bool) -> Value {
    json!({ "trace": if enabled { "on" } else { "off" } })
}

/// The per-statement execution trace. Diagnostics, not a result, so this is
/// written to stderr under `--json`; stdout carries only the statement's own
/// document.
pub(crate) fn print_trace(trace: &QueryTrace, rows_examined: u64) {
    let mut body = trace_body(trace);
    body.insert("rows_examined".to_string(), json!(rows_examined));
    eprintln!("{}", json!({ "trace": Value::Object(body) }));
}

/// `.explain <sql>` for a read-only statement — the plan the engine actually
/// took, field for field with the human rendering in
/// `cli_render::render_explain`. `runtime_trace` is `true` because the
/// statement was run to collect it.
pub(crate) fn explain_document(result: &QueryResult) -> Value {
    let mut body = trace_body(&result.trace);
    body.insert("sort_elided".to_string(), json!(result.trace.sort_elided));
    body.insert("runtime_trace".to_string(), json!(true));
    json!({ "explain": Value::Object(body) })
}

/// `.explain <sql>` for a statement that must NOT be run — the statically
/// planned shape, with `runtime_trace: false`.
///
/// The measured fields (`index_used`, `predicates_pushed`, `indexes_considered`,
/// `sort_elided`) are absent rather than empty: the engine never executed this
/// statement, so it has nothing to report, and an empty array would claim it
/// looked and found none.
pub(crate) fn static_plan_document(plan: &str) -> Value {
    json!({ "explain": { "physical_plan": plan.trim_end(), "runtime_trace": false } })
}

fn trace_body(trace: &QueryTrace) -> Map<String, Value> {
    let mut body = Map::new();
    body.insert("physical_plan".to_string(), json!(trace.physical_plan));
    body.insert(
        "index_used".to_string(),
        match &trace.index_used {
            Some(index) => json!(index),
            None => Value::Null,
        },
    );
    body.insert(
        "predicates_pushed".to_string(),
        Value::Array(
            trace
                .predicates_pushed
                .iter()
                .map(|predicate| json!(predicate.as_ref()))
                .collect(),
        ),
    );
    body.insert(
        "indexes_considered".to_string(),
        Value::Array(
            trace
                .indexes_considered
                .iter()
                .map(|candidate| {
                    json!({
                        "name": candidate.name,
                        "rejected_reason": candidate.rejected_reason.as_ref(),
                    })
                })
                .collect(),
        ),
    );
    body
}

/// `.schema <table>` — the table's declared contract as data, plus the exact
/// DDL the human rendering prints.
///
/// A key is present when the table DECLARED that policy, exactly as the DDL
/// renderer works: an undeclared `RETAIN`, state machine, sync direction or
/// conflict policy is absent rather than filled in with a default an operator
/// never wrote. `ddl` comes from the same renderer the human path uses, so the
/// two can never drift.
pub(crate) fn table_meta_document(table: &str, meta: &TableMeta) -> Value {
    let mut document = Map::new();
    document.insert("table".to_string(), json!(table));
    document.insert("immutable".to_string(), json!(meta.immutable));
    document.insert("columns".to_string(), columns_value(meta));
    document.insert("primary_key".to_string(), primary_key_value(meta));
    document.insert("indexes".to_string(), indexes_value(meta));

    if let Some(ttl_seconds) = meta.default_ttl_seconds {
        document.insert("retain".to_string(), retain_value(meta, ttl_seconds));
    }
    if let Some(state_machine) = &meta.state_machine {
        let transitions: Map<String, Value> = state_machine
            .transitions
            .iter()
            .map(|(from, tos)| (from.clone(), json!(tos)))
            .collect();
        document.insert(
            "state_machine".to_string(),
            json!({ "column": state_machine.column, "transitions": Value::Object(transitions) }),
        );
    }
    if let Some(direction) = meta.sync_direction {
        document.insert(
            "sync_direction".to_string(),
            json!(sync_direction_wire_word(direction)),
        );
    }
    if let Some(policy) = meta.conflict_policy {
        document.insert(
            "conflict_policy".to_string(),
            json!(conflict_policy_wire_word(policy)),
        );
    }
    if let Some(policy) = meta.history_policy {
        // An object, not a bare string, for the same reason "retain" is: the
        // reserved windowed form (`HISTORY <n> <unit>`) would add `window`/
        // `unit`/`seconds` keys next to `policy` without a shape break for an
        // existing reader.
        document.insert(
            "history".to_string(),
            json!({ "policy": history_policy_wire_word(policy) }),
        );
    }
    document.insert("dag_edge_types".to_string(), json!(meta.dag_edge_types));
    document.insert("propagate".to_string(), propagate_value(meta));
    document.insert(
        "ddl".to_string(),
        json!(cli_render::render_table_meta(table, meta)),
    );
    Value::Object(document)
}

fn columns_value(meta: &TableMeta) -> Value {
    Value::Array(
        meta.columns
            .iter()
            .map(|column| {
                let mut value = Map::new();
                value.insert("name".to_string(), json!(column.name));
                value.insert(
                    "type".to_string(),
                    json!(cli_render::render_column_type(&column.column_type)),
                );
                value.insert("nullable".to_string(), json!(column.nullable));
                value.insert("primary_key".to_string(), json!(column.primary_key));
                value.insert("unique".to_string(), json!(column.unique));
                value.insert("immutable".to_string(), json!(column.immutable));
                value.insert("expires".to_string(), json!(column.expires));
                value.insert(
                    "default".to_string(),
                    match &column.default {
                        Some(default) => json!(default),
                        None => Value::Null,
                    },
                );
                if matches!(
                    column.column_type,
                    contextdb_core::table_meta::ColumnType::Vector(_)
                ) {
                    value.insert(
                        "quantization".to_string(),
                        json!(column.quantization.as_str()),
                    );
                }
                if let Some(references) = &column.references {
                    value.insert(
                        "references".to_string(),
                        references_value(meta, &column.name, references),
                    );
                }
                if let Some(rank_policy) = &column.rank_policy {
                    value.insert(
                        "rank_policy".to_string(),
                        json!({
                            "sort_key": rank_policy.sort_key,
                            "formula": rank_policy.formula,
                            "joined_table": rank_policy.joined_table,
                            "joined_column": rank_policy.joined_column,
                        }),
                    );
                }
                Value::Object(value)
            })
            .collect(),
    )
}

/// A foreign key, carrying its `ON STATE ... PROPAGATE SET ...` clause when the
/// column declared one — the same clause the column's DDL renders inline.
fn references_value(
    meta: &TableMeta,
    column: &str,
    references: &contextdb_core::ForeignKeyReference,
) -> Value {
    let mut value = Map::new();
    value.insert("table".to_string(), json!(references.table));
    value.insert("column".to_string(), json!(references.column));
    for rule in &meta.propagation_rules {
        if let PropagationRule::ForeignKey {
            fk_column,
            trigger_state,
            target_state,
            max_depth,
            abort_on_failure,
            ..
        } = rule
            && fk_column == column
        {
            value.insert(
                "propagate".to_string(),
                json!({
                    "on_state": trigger_state,
                    "set_state": target_state,
                    "max_depth": max_depth,
                    "abort_on_failure": abort_on_failure,
                }),
            );
        }
    }
    Value::Object(value)
}

/// The table's identity: the table-level `PRIMARY KEY (a, b, ...)` when it
/// declared one, otherwise the single column carrying the key.
fn primary_key_value(meta: &TableMeta) -> Value {
    if !meta.primary_key_columns.is_empty() {
        return json!(meta.primary_key_columns);
    }
    Value::Array(
        meta.columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|column| json!(column.name))
            .collect(),
    )
}

/// User-declared indexes only — auto-created ones are suppressed here exactly
/// as they are in the DDL rendering, so `.schema` shows what was written.
fn indexes_value(meta: &TableMeta) -> Value {
    Value::Array(
        meta.indexes
            .iter()
            .filter(|index| index.kind != IndexKind::Auto)
            .map(|index| {
                json!({
                    "name": index.name,
                    "kind": "user",
                    "columns": index
                        .columns
                        .iter()
                        .map(|(column, direction)| json!({
                            "column": column,
                            "direction": match direction {
                                SortDirection::Asc => "ASC",
                                SortDirection::Desc => "DESC",
                            },
                        }))
                        .collect::<Vec<_>>(),
                })
            })
            .collect(),
    )
}

/// The retention window as declared (`RETAIN 30 DAYS`), alongside the resolved
/// seconds so a consumer never has to redo the unit arithmetic.
fn retain_value(meta: &TableMeta, ttl_seconds: u64) -> Value {
    let mut value = Map::new();
    match meta.retain_declared_unit {
        Some(unit) if unit.seconds_multiplier() != 0 => {
            value.insert(
                "window".to_string(),
                json!(ttl_seconds / unit.seconds_multiplier()),
            );
            value.insert("unit".to_string(), json!(unit.sql()));
        }
        _ => {
            value.insert("window".to_string(), json!(ttl_seconds));
            value.insert("unit".to_string(), json!("SECONDS"));
        }
    }
    value.insert("seconds".to_string(), json!(ttl_seconds));
    value.insert("sync_safe".to_string(), json!(meta.sync_safe));
    Value::Object(value)
}

/// Every propagation rule the table declared. All three kinds appear, including
/// the foreign-key rules the DDL renders on their column rather than as a
/// table-level clause — a machine reader should not have to parse a column
/// clause to find them.
fn propagate_value(meta: &TableMeta) -> Value {
    let mut rules: Vec<Value> = meta
        .propagation_rules
        .iter()
        .map(|rule| match rule {
            PropagationRule::Edge {
                edge_type,
                direction,
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } => json!({
                "kind": "edge",
                "edge_type": edge_type,
                "direction": propagation_direction_wire_word(direction),
                "on_state": trigger_state,
                "set_state": target_state,
                "max_depth": max_depth,
                "abort_on_failure": abort_on_failure,
            }),
            PropagationRule::VectorExclusion { trigger_state } => json!({
                "kind": "vector_exclusion",
                "on_state": trigger_state,
            }),
            PropagationRule::ForeignKey {
                fk_column,
                referenced_table,
                referenced_column,
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } => json!({
                "kind": "foreign_key",
                "column": fk_column,
                "references_table": referenced_table,
                "references_column": referenced_column,
                "on_state": trigger_state,
                "set_state": target_state,
                "max_depth": max_depth,
                "abort_on_failure": abort_on_failure,
            }),
        })
        .collect();
    // Deterministic output: same declaration, same document, every run.
    rules.sort_by_key(|rule| {
        (
            rule["kind"].as_str().unwrap_or_default().to_string(),
            rule["on_state"].as_str().unwrap_or_default().to_string(),
            rule["edge_type"]
                .as_str()
                .or_else(|| rule["column"].as_str())
                .unwrap_or_default()
                .to_string(),
        )
    });
    Value::Array(rules)
}

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_engine::Database;
    use std::collections::HashMap;

    /// The envelope's class is the branch an agent makes — retry the hub,
    /// look at the disk, or fix the query.
    #[test]
    fn error_class_routes_sync_io_and_sql() {
        use contextdb_core::Error;

        for error in [
            Error::SyncError("hub refused the changeset".to_string()),
            Error::SyncPushUnconfirmed {
                detail: "no acknowledgement".to_string(),
            },
        ] {
            assert_eq!(ErrorClass::of(&error), ErrorClass::Sync, "{error}");
        }

        for error in [
            Error::DatabaseLocked {
                holder_pid: 7,
                path: std::path::PathBuf::from("/tmp/held.db"),
            },
            Error::StoreCorrupted {
                path: "/tmp/store".to_string(),
                reason: "truncated".to_string(),
            },
            Error::MemoryBudgetExceeded {
                subsystem: "vector".to_string(),
                operation: "build".to_string(),
                requested_bytes: 2,
                available_bytes: 1,
                budget_limit_bytes: 1,
                hint: "raise the budget".to_string(),
            },
        ] {
            assert_eq!(ErrorClass::of(&error), ErrorClass::Io, "{error}");
        }

        for error in [
            Error::ParseError("unexpected token".to_string()),
            Error::UniqueViolation {
                table: "t".to_string(),
                column: "id".to_string(),
            },
            Error::ColumnNotFound {
                table: "t".to_string(),
                column: "missing".to_string(),
            },
        ] {
            assert_eq!(ErrorClass::of(&error), ErrorClass::Sql, "{error}");
        }
    }

    /// Every direction has a word, including through the arms no DDL-driven
    /// test can reach. The match is exhaustive with no wildcard, so a new
    /// variant fails to compile until someone chooses its word.
    #[test]
    fn sync_direction_wire_word_covers_every_variant() {
        assert_eq!(sync_direction_wire_word(SyncDirection::None), "sync_off");
        assert_eq!(sync_direction_wire_word(SyncDirection::Push), "push_only");
        assert_eq!(sync_direction_wire_word(SyncDirection::Pull), "pull_only");
        assert_eq!(sync_direction_wire_word(SyncDirection::Both), "two_way");
    }

    /// The same for conflict policies, including the two the engine keeps for
    /// its own distributed-contract tables — no schema can declare those, so
    /// nothing a test can `CREATE` will carry one, but a `.schema` on such a
    /// table still reports the field.
    #[test]
    fn conflict_policy_wire_word_covers_every_variant() {
        assert_eq!(
            conflict_policy_wire_word(ConflictPolicy::InsertIfNotExists),
            "keep_first"
        );
        assert_eq!(
            conflict_policy_wire_word(ConflictPolicy::LatestWins),
            "keep_latest"
        );
        assert_eq!(
            conflict_policy_wire_word(ConflictPolicy::ServerWins),
            "server_wins"
        );
        assert_eq!(
            conflict_policy_wire_word(ConflictPolicy::EdgeWins),
            "edge_wins"
        );
    }

    /// No wire word may coincide with its variant's `Debug` spelling. The
    /// point of declaring these words is that renaming a Rust type cannot
    /// reach a consumer, and an accidental match would hide a regression back
    /// to formatting the identifier.
    #[test]
    fn no_wire_word_repeats_a_rust_identifier() {
        for direction in [
            SyncDirection::None,
            SyncDirection::Push,
            SyncDirection::Pull,
            SyncDirection::Both,
        ] {
            assert_ne!(
                sync_direction_wire_word(direction),
                format!("{direction:?}"),
                "the wire word must not be the Rust identifier"
            );
        }
        for policy in [
            ConflictPolicy::InsertIfNotExists,
            ConflictPolicy::LatestWins,
            ConflictPolicy::ServerWins,
            ConflictPolicy::EdgeWins,
        ] {
            assert_ne!(
                conflict_policy_wire_word(policy),
                format!("{policy:?}"),
                "the wire word must not be the Rust identifier"
            );
        }
    }

    /// Every policy the DDL renders as a clause is also reachable as a
    /// structured field, so a machine reader never has to parse the DDL
    /// string.
    #[test]
    fn table_meta_json_carries_every_clause_the_ddl_renders() {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE intentions (id UUID PRIMARY KEY, status TEXT)",
            &HashMap::new(),
        )
        .expect("create referenced table");
        db.execute(
            "CREATE TABLE decisions (\
               id UUID PRIMARY KEY, \
               status TEXT NOT NULL, \
               intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated, \
               embedding VECTOR(384)\
             ) STATE MACHINE (status: active -> [invalidated, superseded]) \
               RETAIN 30 DAYS SYNC SAFE \
               SYNC PUSH ONLY \
               SYNC CONFLICT KEEP LATEST \
               PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated \
               PROPAGATE ON STATE invalidated EXCLUDE VECTOR",
            &HashMap::new(),
        )
        .expect("create table under test");
        let meta = db.table_meta("decisions").expect("meta");
        let document = table_meta_document("decisions", &meta);
        let ddl = cli_render::render_table_meta("decisions", &meta);

        assert_eq!(document["ddl"], json!(ddl), "ddl is the human rendering");

        // Each clause the DDL renders has a structured counterpart. The pairs
        // read: if the DDL says this, the document must carry that.
        let clause_keys: &[(&str, &str)] = &[
            ("RETAIN", "retain"),
            ("STATE MACHINE", "state_machine"),
            ("SYNC PUSH ONLY", "sync_direction"),
            ("SYNC CONFLICT", "conflict_policy"),
            ("PROPAGATE ON EDGE", "propagate"),
            ("PROPAGATE ON STATE", "propagate"),
        ];
        for (clause, key) in clause_keys {
            if ddl.contains(clause) {
                assert!(
                    document.get(*key).is_some_and(|value| !value.is_null()),
                    "the DDL renders `{clause}` but the document has no `{key}`: {document}"
                );
            }
        }

        assert_eq!(document["retain"]["window"], json!(30));
        assert_eq!(document["retain"]["unit"], json!("DAYS"));
        assert_eq!(document["retain"]["seconds"], json!(2_592_000));
        assert_eq!(document["retain"]["sync_safe"], json!(true));
        assert_eq!(document["state_machine"]["column"], json!("status"));

        let kinds: std::collections::BTreeSet<&str> = document["propagate"]
            .as_array()
            .expect("propagate is an array")
            .iter()
            .filter_map(|rule| rule["kind"].as_str())
            .collect();
        assert!(
            kinds.contains("edge") && kinds.contains("vector_exclusion"),
            "both table-level propagation kinds are present: {kinds:?}"
        );
        assert!(
            kinds.contains("foreign_key"),
            "the column-level foreign-key rule is reachable without parsing the column clause: {kinds:?}"
        );

        // The foreign key's propagate clause also rides its own column, where a
        // reader looking at that column expects it.
        let intention = document["columns"]
            .as_array()
            .expect("columns")
            .iter()
            .find(|column| column["name"] == json!("intention_id"))
            .expect("intention_id column");
        assert_eq!(intention["references"]["table"], json!("intentions"));
        assert_eq!(
            intention["references"]["propagate"]["set_state"],
            json!("invalidated")
        );

        let embedding = document["columns"]
            .as_array()
            .expect("columns")
            .iter()
            .find(|column| column["name"] == json!("embedding"))
            .expect("embedding column");
        assert_eq!(embedding["type"], json!("VECTOR(384)"));
        assert!(
            embedding.get("quantization").is_some(),
            "a vector column reports its quantization: {embedding}"
        );
    }

    /// A table that declares `HISTORY CURRENT ONLY` must render the
    /// declaration in `.schema` (the human DDL) AND carry a structured
    /// `history` key in the `--json` document, exactly as `RETAIN` and
    /// `SYNC CONFLICT` already do -- a declaration an operator wrote must be
    /// visible in both surfaces, never silently dropped between the meta and
    /// the render.
    #[test]
    fn declared_history_policy_renders_in_schema_and_json() {
        let db = contextdb_engine::Database::open_memory();
        db.execute(
            "CREATE TABLE device_status (\
               device_id TEXT PRIMARY KEY, \
               state TEXT NOT NULL\
             ) HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST",
            &HashMap::new(),
        )
        .expect("HISTORY CURRENT ONLY must be declarable");
        let meta = db.table_meta("device_status").expect("meta");
        assert_eq!(
            meta.history_policy,
            Some(contextdb_core::HistoryPolicy::CurrentOnly),
            "the executor must carry the parsed HISTORY clause into TableMeta"
        );

        let ddl = cli_render::render_table_meta("device_status", &meta);
        assert!(
            ddl.contains("HISTORY CURRENT ONLY"),
            ".schema must render the declared HISTORY policy verbatim, got:\n{ddl}"
        );

        let document = table_meta_document("device_status", &meta);
        assert!(
            document
                .get("history")
                .is_some_and(|value| !value.is_null()),
            "the --json document must carry a `history` key for a declared policy: {document}"
        );
        assert_eq!(document["history"]["policy"], json!("CURRENT_ONLY"));
    }

    /// The mirror of the test above: a table that declares no HISTORY clause
    /// shows nothing to puzzle over, matching the treatment an undeclared
    /// RETAIN/conflict-policy/direction already gets.
    #[test]
    fn an_undeclared_history_policy_renders_nothing() {
        let db = contextdb_engine::Database::open_memory();
        db.execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("create");
        let meta = db.table_meta("notes").expect("meta");
        let ddl = cli_render::render_table_meta("notes", &meta);
        assert!(
            !ddl.contains("HISTORY"),
            "an undeclared policy must render nothing, got:\n{ddl}"
        );
        let document = table_meta_document("notes", &meta);
        assert!(
            document.get("history").is_none_or(|value| value.is_null()),
            "an undeclared policy must carry no `history` key: {document}"
        );
    }
}
