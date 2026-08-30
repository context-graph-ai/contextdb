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
use contextdb_core::read_contract::{
    CursorExpiryKind, CursorPage, OwnerReadStatus, OwnerServingReason, OwnerServingState,
    ReadFailure, ReadFailureClass, ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadRoute,
};
use contextdb_core::table_meta::{ConflictPolicy, SyncDirection};
use contextdb_engine::QueryResult;
use contextdb_engine::database::QueryTrace;
use contextdb_engine::{CompactionReport, MaintenancePolicy, MaintenanceReport};
use contextdb_engine::{
    DirectEventsStatus, DirectIndexDirection, DirectMaintenanceStatus, DirectPropagationRule,
    DirectSchema, DirectVectorQuantization, OwnerConfigurationSource, OwnerConfiguredValue,
    OwnerReport,
};
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
            // A read refusal already carries its own ratified class. Letting it
            // fall through to the SQL default would send every script that
            // branches on `class` down the wrong branch for a failure that is
            // about the store, the route, or the invocation rather than the
            // query.
            Error::ReadFailure(failure) => ErrorClass::from(failure.class()),
            Error::SyncError(_)
            | Error::NotSyncEligible(_)
            | Error::SyncPushUnconfirmed { .. }
            | Error::SyncReplayOfAcceptedDelete { .. } => ErrorClass::Sync,
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

impl From<ReadFailureClass> for ErrorClass {
    /// A read refusal's class, in the CLI's own four-name vocabulary. The read
    /// surface names three of the four; `sync` has no read-surface member, so
    /// this bridge is total without a default arm — a new read class must be
    /// given a name here deliberately.
    fn from(class: ReadFailureClass) -> Self {
        match class {
            ReadFailureClass::Sql => ErrorClass::Sql,
            ReadFailureClass::Io => ErrorClass::Io,
            ReadFailureClass::Usage => ErrorClass::Usage,
        }
    }
}

/// The stable word a script branches on for one read refusal.
///
/// Spelled out rather than derived from the Rust identifier, for the same
/// reason every other wire word in this module is: renaming a variant must not
/// silently rewrite what every consumer parses. The match is exhaustive on
/// purpose — a new kind must be given its word deliberately.
pub(crate) fn read_failure_kind_wire_word(kind: ReadFailureKind) -> &'static str {
    match kind {
        ReadFailureKind::WriteRequiresFlag => "write_requires_flag",
        ReadFailureKind::HeldByWriter => "held_by_writer",
        ReadFailureKind::HeldByReaders => "held_by_readers",
        ReadFailureKind::OwnerNotRunning => "owner_not_running",
        ReadFailureKind::OwnerNotServing => "owner_not_serving",
        ReadFailureKind::OwnerUserMismatch => "owner_user_mismatch",
        ReadFailureKind::OwnerMismatch => "owner_mismatch",
        ReadFailureKind::OwnerAtCapacity => "owner_at_capacity",
        ReadFailureKind::OwnerLimitExceeded => "owner_limit_exceeded",
        ReadFailureKind::OwnerTimeout => "owner_timeout",
        ReadFailureKind::OwnerDisconnected => "owner_disconnected",
        ReadFailureKind::InvalidChannelData => "invalid_channel_data",
        ReadFailureKind::LocalProtocolMismatch => "local_protocol_mismatch",
        ReadFailureKind::CursorExpired => "cursor_expired",
        ReadFailureKind::CursorNotFound => "cursor_not_found",
        ReadFailureKind::DirectReadRequiresWriter => "direct_read_requires_writer",
        ReadFailureKind::StoreNotFound => "store_not_found",
        ReadFailureKind::InvalidContinuation => "invalid_continuation",
        ReadFailureKind::CursorAlreadyOpen => "cursor_already_open",
        ReadFailureKind::CursorTransactionActive => "cursor_transaction_active",
        ReadFailureKind::CursorInvalidStatement => "cursor_invalid_statement",
        ReadFailureKind::OperationAlreadyCompleted => "operation_already_completed",
        ReadFailureKind::OwnerRouteUnsupported => "owner_route_unsupported",
        ReadFailureKind::DeclaredPrincipalRefused => "declared_principal_refused",
    }
}

/// The stable word for the ceiling a refused read crossed. It is the flag's own
/// name without its `--read-` prefix, so the refusal and the setting that would
/// let it through are recognizably the same thing.
pub(crate) fn read_failure_limit_wire_word(limit: ReadFailureLimit) -> &'static str {
    match limit {
        ReadFailureLimit::ResultRows => "result_rows",
        ReadFailureLimit::ResultBytes => "result_bytes",
        ReadFailureLimit::Work => "work",
        ReadFailureLimit::ActiveMs => "active_ms",
        ReadFailureLimit::Memory => "memory",
        ReadFailureLimit::CursorPageBytes => "cursor_page_bytes",
    }
}

/// The `detail` object a read refusal carries: always the stable `kind`, plus
/// whatever that kind is specific enough to say. A field is absent rather than
/// null when this refusal has nothing to put in it.
pub(crate) fn read_failure_detail_document(failure: &ReadFailure) -> Value {
    let mut detail = Map::new();
    detail.insert(
        "kind".to_string(),
        json!(read_failure_kind_wire_word(failure.kind())),
    );
    match failure.detail() {
        ReadFailureDetail::None => {}
        ReadFailureDetail::Reason { reason } => {
            detail.insert("reason".to_string(), json!(reason));
        }
        ReadFailureDetail::CursorExpired { expiry } => {
            detail.insert(
                "expiry".to_string(),
                json!(match expiry {
                    CursorExpiryKind::Idle => "idle",
                    CursorExpiryKind::Lifetime => "lifetime",
                }),
            );
        }
        ReadFailureDetail::HeldByWriter(held) => {
            if let Some(process_id) = held.process_id {
                detail.insert("process_id".to_string(), json!(process_id));
            }
            detail.insert("store_path".to_string(), json!(held.store_path));
        }
        ReadFailureDetail::OwnerRouteUnsupported(unsupported) => {
            detail.insert("inspection".to_string(), json!(unsupported.inspection));
        }
        ReadFailureDetail::HeldByReaders(held) => {
            detail.insert(
                "observed_direct_readers".to_string(),
                json!(held.observed_direct_readers),
            );
            detail.insert(
                "verified_readers".to_string(),
                Value::Array(
                    held.verified_readers
                        .iter()
                        .map(|reader| {
                            json!({
                                "process_id": reader.process_id,
                                "process_name": reader.process_name,
                            })
                        })
                        .collect(),
                ),
            );
        }
        ReadFailureDetail::OwnerLimitExceeded(exceeded) => {
            detail.insert(
                "limit".to_string(),
                json!(read_failure_limit_wire_word(exceeded.limit)),
            );
            detail.insert("value".to_string(), json!(exceeded.value));
            if let Some(required) = &exceeded.required {
                detail.insert("required_bytes".to_string(), json!(required.required_bytes));
                detail.insert(
                    "required_setting".to_string(),
                    json!(required.required_setting),
                );
            }
            if let Some(statement) = &exceeded.statement {
                detail.insert("statement".to_string(), json!(statement.statement));
                detail.insert(
                    "remedy_command".to_string(),
                    json!(statement.remedy_command),
                );
            }
        }
    }
    Value::Object(detail)
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
#[allow(dead_code)]
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
#[allow(dead_code)]
pub(crate) fn propagation_direction_wire_word(direction: &Direction) -> &'static str {
    match direction {
        Direction::Incoming => "INCOMING",
        Direction::Outgoing => "OUTGOING",
        Direction::Both => "BOTH",
    }
}

/// The machine word for a DDL-declared conflict policy. Engine-private
/// mechanics have no declared clause and therefore no public representation.
#[allow(dead_code)]
pub(crate) fn conflict_policy_wire_word(policy: ConflictPolicy) -> Option<&'static str> {
    if policy == ConflictPolicy::KEEP_FIRST {
        Some("keep_first")
    } else if policy == ConflictPolicy::KEEP_LATEST {
        Some("keep_latest")
    } else {
        None
    }
}

/// The wire word for a table's version-history policy, on the same terms.
#[allow(dead_code)]
pub(crate) fn history_policy_wire_word(policy: contextdb_core::HistoryPolicy) -> &'static str {
    match policy {
        contextdb_core::HistoryPolicy::All => "ALL",
        contextdb_core::HistoryPolicy::CurrentOnly => "CURRENT_ONLY",
    }
}

/// The wire word for who owns a database's maintenance schedule.
#[allow(dead_code)]
pub(crate) fn maintenance_policy_wire_word(policy: MaintenancePolicy) -> &'static str {
    match policy {
        MaintenancePolicy::EngineOwned => "engine_owned",
        MaintenancePolicy::CallerDriven => "caller_driven",
    }
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
            "rows_deferred_for_readers": report.pruning.rows_deferred_for_readers,
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
    print_error_with_detail(class, message, line, None);
}

/// The same envelope, carrying the typed `detail` a read refusal publishes.
/// `class` and `detail.kind` are what a script branches on; `message` is prose
/// for a person and its wording is free to change.
pub(crate) fn print_error_with_detail(
    class: ErrorClass,
    message: &str,
    line: Option<usize>,
    detail: Option<&Value>,
) {
    let mut error = Map::new();
    error.insert("class".to_string(), json!(class.as_str()));
    error.insert("message".to_string(), json!(message));
    if let Some(line) = line {
        error.insert("line".to_string(), json!(line));
    }
    if let Some(detail) = detail {
        error.insert("detail".to_string(), detail.clone());
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

/// Write one NOTICE with structured detail that remains directly queryable by
/// a JSON consumer instead of being embedded as escaped JSON in `message`.
pub(crate) fn print_notice_document(class: ErrorClass, message: &str, detail: &Value) {
    eprintln!(
        "{}",
        json!({ "notice": { "class": class.as_str(), "message": message, "detail": detail } })
    );
}

/// Print one result document to stdout.
pub(crate) fn print_document(document: &Value) {
    println!("{document}");
}

/// Write one document to stderr, verbatim, as its own top-level shape (not
/// wrapped in the generic `{"notice":{...}}` envelope `print_notice` uses).
/// For a streamed signal with its own stable machine-readable key — e.g.
/// `sync_pull_progress_document` below — where the top-level key itself
/// carries the meaning a consumer filters on.
pub(crate) fn print_stderr_document(document: &Value) {
    eprintln!("{document}");
}

/// A periodic liveness signal streamed to stderr during a long `.sync pull`,
/// under `--json`: how many pages this pull has read so far. Distinct from
/// the generic notice envelope so a consumer can filter on the
/// `sync_pull_progress` key directly, the same way it filters on `sync_pull`
/// for the final result.
pub(crate) fn sync_pull_progress_document(pages_read: u64) -> Value {
    json!({ "sync_pull_progress": { "pages_read": pages_read } })
}

/// `.tables` — one bounded page of table names.
///
/// A page document, not a bare name array: `.tables` is resumable, so it has
/// to say whether another page exists and, when one does, hand back the
/// continuation that fetches it. `continuation` is a string exactly when
/// `has_more` is true and null when it is false, so a caller never has to
/// guess which of the two fields to trust.
pub(crate) fn tables_document(
    names: Vec<String>,
    has_more: bool,
    continuation: Option<&str>,
) -> Value {
    json!({
        "tables": {
            "items": names,
            "has_more": has_more,
            "continuation": match continuation {
                Some(token) => json!(token),
                None => Value::Null,
            },
        }
    })
}

/// One successful ordinary result: a namespaced document that names its
/// columns and carries every row as an object keyed by column name. There is
/// no bare-array rendering to fall back to — a result document a consumer
/// cannot tell apart from a cursor page or an error is not a contract.
pub(crate) fn result_document(result: &QueryResult) -> Value {
    json!({
        "result": {
            "columns": result.columns,
            "rows": crate::formatter::rows_as_objects(&result.columns, &result.rows),
        }
    })
}

/// One cursor page, in the same shape an ordinary result publishes, under the
/// key that names the command that produced it.
pub(crate) fn cursor_page_document(page: &CursorPage) -> Value {
    json!({
        "cursor": {
            "columns": page.columns,
            "rows": crate::formatter::rows_as_objects(&page.columns, &page.rows),
            "has_more": page.has_more,
        }
    })
}

/// `.cursor close` — its own document, so a caller sees the close happen
/// rather than inferring it from silence.
pub(crate) fn cursor_closed_document() -> Value {
    json!({ "cursor": { "closed": true } })
}

/// The serving state of a file-backed process owner, as the word a script
/// branches on.
///
/// `not_serving` is the state on the owner's OWN status surface; the
/// separately-named `owner_not_serving` is the refusal KIND an inspecting
/// session receives when it tries to reach that owner. Two names for the two
/// sides of one situation, so a script branches on each in its own place.
/// The word itself belongs to the read contract, so the engine's own refusals
/// and this surface cannot drift apart.
pub(crate) fn owner_serving_state_wire_word(state: OwnerServingState) -> &'static str {
    state.wire_word()
}

/// Why an owner is not simply serving, when there is a reason to give.
pub(crate) fn owner_serving_reason_wire_word(reason: &OwnerServingReason) -> &'static str {
    reason.wire_word()
}

/// `.owner status` when nobody owns the store.
///
/// A store with no process owner is an ANSWER, not a missing one — and the
/// serving vocabulary has no word for it, because every state in it describes
/// an owner that exists. So the CLI names this state itself, in the same
/// document shape a real owner reports.
pub(crate) fn owner_not_running_document() -> Value {
    json!({ "owner": { "state": "not_running" } })
}

/// One ceiling in force on the owner, and whether it is the shipped default or
/// something this deployment chose — an operator reading a surprising limit
/// needs to know which, because only one of the two is theirs to change.
fn owner_configured_value(configured: OwnerConfiguredValue) -> Value {
    json!({
        "value": configured.value,
        "source": match configured.source {
            OwnerConfigurationSource::Default => "default",
            OwnerConfigurationSource::Override => "override",
        },
    })
}

/// `.owner status` — everything the owner says about how it is serving.
///
/// The state alone answers "is anyone there"; the rest answers "and what will
/// it do for me". All of it is what the owner itself computed, never this
/// reader's guess, and none of it is sync: `.owner status` describes the
/// file-backed process owner and nothing else.
pub(crate) fn owner_report_document(report: &OwnerReport) -> Value {
    let mut owner = Map::new();
    owner.insert(
        "state".to_string(),
        json!(owner_serving_state_wire_word(report.status.state)),
    );
    if let Some(reason) = &report.status.reason {
        owner.insert(
            "reason".to_string(),
            json!(owner_serving_reason_wire_word(reason)),
        );
        if let OwnerServingReason::StartupFailure(detail) = reason {
            owner.insert("detail".to_string(), json!(detail));
        }
    }
    if let Some(serving) = &report.serving {
        let limits = &serving.effective_limits;
        owner.insert(
            "limits".to_string(),
            json!({
                "result_rows": owner_configured_value(limits.result_rows),
                "result_bytes": owner_configured_value(limits.result_bytes),
                "work": owner_configured_value(limits.work),
                "active_ms": owner_configured_value(limits.active_ms),
                "memory": owner_configured_value(limits.memory),
                "cursor_page_rows": owner_configured_value(limits.cursor_page_rows),
                "cursor_page_bytes": owner_configured_value(limits.cursor_page_bytes),
                "cursor_idle_ms": owner_configured_value(limits.cursor_idle_ms),
                "cursor_lifetime_ms": owner_configured_value(limits.cursor_lifetime_ms),
                "concurrency": owner_configured_value(limits.concurrency),
            }),
        );
        owner.insert(
            "timeouts".to_string(),
            json!({
                "request_ms": owner_configured_value(serving.timeouts.request_ms),
                "shutdown_drain_ms": owner_configured_value(serving.timeouts.shutdown_drain_ms),
            }),
        );
        owner.insert(
            "active_readers".to_string(),
            json!(serving.admission.active_readers),
        );
        owner.insert("concurrency".to_string(), json!(serving.admission.capacity));
        owner.insert(
            "database_memory".to_string(),
            json!({
                "used_bytes": serving.memory.used_bytes,
                // Absent as null, not zero: an owner that declares no memory
                // ceiling is not an owner with none left.
                "available_bytes": match serving.memory.available_bytes {
                    Some(available) => json!(available),
                    None => Value::Null,
                },
            }),
        );
    }
    json!({ "owner": Value::Object(owner) })
}

/// `.owner status` — the file-backed process owner, never sync health. A
/// reason is absent rather than null when the state has none to give.
#[allow(dead_code)]
pub(crate) fn owner_status_document(status: &OwnerReadStatus) -> Value {
    let mut owner = Map::new();
    owner.insert(
        "state".to_string(),
        json!(owner_serving_state_wire_word(status.state)),
    );
    if let Some(reason) = &status.reason {
        owner.insert(
            "reason".to_string(),
            json!(owner_serving_reason_wire_word(reason)),
        );
        if let OwnerServingReason::StartupFailure(detail) = reason {
            owner.insert("detail".to_string(), json!(detail));
        }
    }
    json!({ "owner": Value::Object(owner) })
}

/// `.schema <table>` — the table's declared contract as data, rendered from the
/// body the metadata door publishes.
///
/// One renderer for every route. The door answers the same body whether it was
/// projected from a committed file, from the writer's own live state, or from
/// an owner over a channel, so `.schema` cannot say different things about the
/// same table depending on how the session happened to reach it. A policy the
/// table never declared is absent rather than filled with a default nobody
/// wrote.
pub(crate) fn schema_document(schema: &DirectSchema) -> Value {
    let mut document = Map::new();
    document.insert("table".to_string(), json!(schema.table));
    document.insert("immutable".to_string(), json!(schema.immutable));
    document.insert("columns".to_string(), schema_columns_value(schema));
    document.insert("primary_key".to_string(), json!(schema.primary_key));
    document.insert(
        "indexes".to_string(),
        Value::Array(
            schema
                .indexes
                .iter()
                .map(|index| {
                    json!({
                        "name": index.name,
                        "kind": "user",
                        "columns": index.columns.iter().map(|column| json!({
                            "column": column.column,
                            "direction": match column.direction {
                                DirectIndexDirection::Asc => "ASC",
                                DirectIndexDirection::Desc => "DESC",
                            },
                        })).collect::<Vec<_>>(),
                    })
                })
                .collect(),
        ),
    );
    if let Some(retain) = &schema.retain {
        document.insert(
            "retain".to_string(),
            json!({
                "window": retain.window,
                "unit": retain.unit,
                "seconds": retain.seconds,
                "sync_safe": retain.sync_safe,
            }),
        );
    }
    if let Some(state_machine) = &schema.state_machine {
        let transitions: Map<String, Value> = state_machine
            .transitions
            .iter()
            .map(|(from, to)| (from.clone(), json!(to)))
            .collect();
        document.insert(
            "state_machine".to_string(),
            json!({ "column": state_machine.column, "transitions": Value::Object(transitions) }),
        );
    }
    if let Some(direction) = &schema.sync_direction {
        document.insert("sync_direction".to_string(), json!(direction));
    }
    if let Some(policy) = &schema.conflict_policy {
        document.insert("conflict_policy".to_string(), json!(policy));
    }
    if let Some(policy) = &schema.history {
        document.insert("history".to_string(), json!({ "policy": policy }));
    }
    document.insert("dag_edge_types".to_string(), json!(schema.dag_edge_types));
    document.insert("propagate".to_string(), schema_propagate_value(schema));
    document.insert("ddl".to_string(), json!(schema.ddl));
    json!({ "schema": Value::Object(document) })
}

fn schema_columns_value(schema: &DirectSchema) -> Value {
    Value::Array(
        schema
            .columns
            .iter()
            .map(|column| {
                let mut value = Map::new();
                value.insert("name".to_string(), json!(column.name));
                value.insert("type".to_string(), json!(column.data_type));
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
                if let Some(quantization) = &column.quantization {
                    value.insert(
                        "quantization".to_string(),
                        json!(match quantization {
                            DirectVectorQuantization::F32 => "F32",
                            DirectVectorQuantization::Sq8 => "SQ8",
                            DirectVectorQuantization::Sq4 => "SQ4",
                        }),
                    );
                }
                if let Some(references) = &column.references {
                    let mut reference = Map::new();
                    reference.insert("table".to_string(), json!(references.table));
                    reference.insert("column".to_string(), json!(references.column));
                    if let Some(propagation) = &references.propagation {
                        reference.insert(
                            "propagate".to_string(),
                            json!({
                                "on_state": propagation.on_state,
                                "set_state": propagation.set_state,
                                "max_depth": propagation.max_depth,
                                "abort_on_failure": propagation.abort_on_failure,
                            }),
                        );
                    }
                    value.insert("references".to_string(), Value::Object(reference));
                }
                // Access control rides beside the foreign key in the same
                // shape: a reader that tests for a policy finds one or finds
                // nothing, never a column that silently drops the grant table
                // it is authorized against.
                if let Some(acl) = &column.acl_ref {
                    value.insert(
                        "acl_references".to_string(),
                        json!({ "table": acl.table, "column": acl.column }),
                    );
                }
                if let Some(rank) = &column.rank {
                    value.insert(
                        "rank_policy".to_string(),
                        json!({
                            "sort_key": rank.sort_key,
                            "formula": rank.formula,
                            "joined_table": rank.joined_table,
                            "joined_column": rank.joined_column,
                        }),
                    );
                }
                Value::Object(value)
            })
            .collect(),
    )
}

/// Every propagation rule the table declared, including the foreign-key rules
/// the DDL renders on their own column — a machine reader should not have to
/// parse a column clause to find one. Sorted, so the same declaration renders
/// the same document every run.
fn schema_propagate_value(schema: &DirectSchema) -> Value {
    let mut rules: Vec<Value> = schema
        .propagate
        .iter()
        .map(|rule| match rule {
            DirectPropagationRule::Edge {
                edge_type,
                direction,
                on_state,
                set_state,
                max_depth,
                abort_on_failure,
            } => json!({
                "kind": "edge",
                "edge_type": edge_type,
                "direction": direction,
                "on_state": on_state,
                "set_state": set_state,
                "max_depth": max_depth,
                "abort_on_failure": abort_on_failure,
            }),
            DirectPropagationRule::VectorExclusion { on_state } => json!({
                "kind": "vector_exclusion",
                "on_state": on_state,
            }),
            DirectPropagationRule::ForeignKey {
                column,
                references_table,
                references_column,
                on_state,
                set_state,
                max_depth,
                abort_on_failure,
            } => json!({
                "kind": "foreign_key",
                "column": column,
                "references_table": references_table,
                "references_column": references_column,
                "on_state": on_state,
                "set_state": set_state,
                "max_depth": max_depth,
                "abort_on_failure": abort_on_failure,
            }),
        })
        .collect();
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

/// `.maintenance status` from the door's body.
pub(crate) fn maintenance_status_body_document(status: &DirectMaintenanceStatus) -> Value {
    json!({
        "maintenance": {
            "running": status.running,
            "retention_enabled": status.retention_enabled,
            "currency_compaction_enabled": status.currency_compaction_enabled,
            "active_maintenance_loops": status.active_maintenance_loops,
            "policy": status.policy,
        }
    })
}

/// `.events status` from the door's body: one bounded page whose items each
/// say which of the four declared things they are.
pub(crate) fn events_status_body_document(
    status: &DirectEventsStatus,
    has_more: bool,
    continuation: Option<&str>,
) -> Value {
    let mut items: Vec<Value> = Vec::new();
    for event_type in &status.event_types {
        items.push(json!({
            "kind": "event_type",
            "name": event_type.name,
            "trigger": event_type.trigger,
            "table": event_type.table,
        }));
    }
    for sink in &status.sinks {
        items.push(json!({
            "kind": "sink",
            "name": sink.name,
            "type": sink.sink_type,
            "callback_registered": sink.callback_registered,
            "delivered": sink.delivered,
            "queued": sink.queued,
            "retried": sink.retried,
            "permanent_failures": sink.permanent_failures,
            "examined": sink.examined,
        }));
    }
    for route in &status.routes {
        items.push(json!({
            "kind": "route",
            "name": route.name,
            "event_type": route.event_type,
            "sink": route.sink,
        }));
    }
    for schedule in &status.schedules {
        items.push(json!({
            "kind": "schedule",
            "name": schedule.name,
            "every": schedule.every,
            "callback": schedule.callback,
            "callback_registered": schedule.callback_registered,
            "next_fire_at_ms": schedule.next_fire_at_ms,
            "last_fire_at_ms": schedule.last_fire_at_ms,
            "fire_count": schedule.fire_count,
        }));
    }
    json!({
        "events_status": {
            "items": items,
            "has_more": has_more,
            "continuation": match continuation {
                Some(token) => json!(token),
                None => Value::Null,
            },
        }
    })
}

/// `.explain <sql>` from the door's body. The door PLANS and never applies, so
/// no runtime trace was collected and it says so rather than implying one.
#[allow(dead_code)]
pub(crate) fn explain_body_document(physical_plan: &str, index: Option<&str>) -> Value {
    json!({
        "explain": {
            "physical_plan": physical_plan.trim_end(),
            "index_used": match index {
                Some(index) => json!(index),
                None => Value::Null,
            },
            "runtime_trace": false,
        }
    })
}

/// The one notice that says HOW this session reads.
///
/// Emitted once, at the first store-reading command, and never again: the
/// route is chosen once and does not change, so saying it twice would suggest
/// it might have. `snapshot_at` is the committed moment a file-route session
/// serves — a long-open reading terminal is a snapshot, not a live view — and
/// is null on the owner route, where the owner serves live committed state and
/// there is no single moment to report.
pub(crate) fn read_route_notice_document(
    route: ReadRoute,
    snapshot_at: Option<contextdb_core::Wallclock>,
) -> Value {
    let route_word = match route {
        ReadRoute::File => "file",
        ReadRoute::Owner => "owner",
    };
    let message = match snapshot_at {
        Some(instant) => format!(
            "reading the committed snapshot taken at {}",
            rfc3339_utc(instant.0)
        ),
        None => "reading through the live owner's local channel".to_owned(),
    };
    json!({
        "notice": {
            "class": ErrorClass::Io.as_str(),
            "message": message,
            "detail": {
                "kind": "read_route",
                "route": route_word,
                "snapshot_at": match snapshot_at {
                    Some(instant) => json!(rfc3339_utc(instant.0)),
                    None => Value::Null,
                },
            }
        }
    })
}

/// The store is still being loaded and the read has not started yet. Said only
/// once the loading has taken long enough to be worth mentioning.
pub(crate) fn hydration_notice_document(loaded_bytes: u64, total_bytes: Option<u64>) -> Value {
    json!({
        "notice": {
            "class": ErrorClass::Io.as_str(),
            "message": format!("loading the store: {loaded_bytes} bytes so far"),
            "detail": {
                "kind": "hydration",
                "loaded_bytes": loaded_bytes,
                "total_bytes": match total_bytes {
                    Some(total) => json!(total),
                    None => Value::Null,
                },
            }
        }
    })
}

/// A statement is running and has done this much. Refreshed while it runs, so
/// a long deliberate export is never mistaken for a hang.
pub(crate) fn statement_progress_notice_document(elapsed_ms: u64, rows: u64, bytes: u64) -> Value {
    json!({
        "notice": {
            "class": ErrorClass::Io.as_str(),
            "message": format!(
                "still running after {elapsed_ms} ms: {rows} rows, {bytes} bytes so far"
            ),
            "detail": {
                "kind": "statement_progress",
                "elapsed_ms": elapsed_ms,
                "rows": rows,
                "bytes": bytes,
            }
        }
    })
}

/// One instant as RFC 3339 UTC, to the second.
///
/// Written out here rather than taken from a date library because this crate's
/// dependency set is fixed, and because the only thing this has to be is
/// exact — the civil-date arithmetic below is the standard days-from-epoch
/// algorithm, not an approximation of one.
fn rfc3339_utc(milliseconds_since_epoch: u64) -> String {
    let seconds = (milliseconds_since_epoch / 1_000) as i64;
    let days = seconds.div_euclid(86_400);
    let time_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        time_of_day / 3_600,
        (time_of_day % 3_600) / 60,
        time_of_day % 60
    )
}

/// The exact inverse of days-from-civil, for every date this will ever see.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = (shifted - era * 146_097) as u64;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_position = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_position + 2) / 5 + 1) as u32;
    let month = if month_position < 10 {
        month_position + 3
    } else {
        month_position - 9
    } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// `.sync status` in a session that cannot write — this CLI session's own sync
/// state, under the key that names the command.
pub(crate) fn read_session_sync_status_document(message: &str) -> Value {
    json!({ "sync_status": { "message": message } })
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

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_engine::Database;
    use contextdb_engine::cli_render;
    use std::collections::HashMap;

    /// The `.schema` document for `table`, taken the way the CLI takes it:
    /// through the one metadata door, from the same body every route
    /// publishes. Asserting on this is asserting on what an operator sees.
    fn schema_document_for(db: &Database, table: &str) -> Value {
        let reader = db
            .read_session(contextdb_core::read_contract::ReadLimits::default())
            .expect("a live database opens its own bounded read view");
        let answered = reader
            .metadata(
                contextdb_engine::MetadataRequest::Schema {
                    table: table.to_owned(),
                },
                None,
            )
            .expect("the metadata door answers a declared table");
        let contextdb_engine::MetadataBody::Schema { schema } = &answered.body else {
            panic!("a schema question is answered with a schema body");
        };
        schema_document(schema)["schema"].clone()
    }

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
        let document = schema_document_for(&db, "decisions");
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
                    document
                        .get(*key)
                        .is_some_and(|value: &Value| !value.is_null()),
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

        let document = schema_document_for(&db, "device_status");
        assert!(
            document
                .get("history")
                .is_some_and(|value: &Value| !value.is_null()),
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
        let document = schema_document_for(&db, "notes");
        assert!(
            document
                .get("history")
                .is_none_or(|value: &Value| value.is_null()),
            "an undeclared policy must carry no `history` key: {document}"
        );
    }
}
