//! The work ledger: generic coordination tables for units of work.
//!
//! Six append-only WORKFLOW-HISTORY tables record what was asked
//! (`work_jobs`), who took it (`work_claims`), what came back
//! (`work_results`), what went wrong (`work_failures`), what was called off
//! (`work_cancellations`), and the ephemeral input copies (`work_inputs`). A
//! job's state is never stored — it is computed on demand from those rows by
//! [`job_state`]. A seventh table, `work_capabilities`, is a CURRENT-TRUTH
//! registry, not history: it carries what each node can do RIGHT NOW.
//!
//! Ground rules this module enforces by construction:
//!
//! - **Workflow history is append-only.** The six history tables only ever
//!   insert rows; the single DELETE is [`run_input_retention`], which removes
//!   expired `work_inputs` copies (the one table the state computation never
//!   reads). The `work_capabilities` registry is the deliberate exception:
//!   re-advertising a (node, capability) key UPSERTS its `advertised_at` (see
//!   [`advertise_capability`]) so the row always reflects the node's present
//!   offer. The audit trail of who-did-what-when stays in the history tables.
//! - **Class-blind.** Job payloads, requirement tags, and receipts are opaque
//!   data. This module names no work class and imports no product type.
//! - **No transport.** Cross-machine arbitration rides the ordinary sync
//!   changeset path; this module never names a pipe. The per-table conflict
//!   internal per-table policy entries are the whole
//!   distributed contract: single-writer-per-key tables ride
//!   insert-if-not-exists; hub-refereed tables (claims, results,
//!   cancellations) ride server-wins so the hub's first-arrived row stands,
//!   later writers see an attributed conflict in their push reply, and a
//!   losing edge's local row is replaced by the hub's row on pull.
//! - **Identity is handed in.** Every `node_id` comes from the fabric
//!   identity the caller owns; this module never mints one.
//! - **The `work_` table-name prefix is reserved** for this ledger.

use crate::Database;
use crate::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_core::{Error, Result, TxId, Value};
use std::collections::HashMap;

/// Every ledger table, in one place. The `work_` prefix is reserved.
pub const WORK_LEDGER_TABLES: [&str; 7] = [
    "work_jobs",
    "work_claims",
    "work_results",
    "work_failures",
    "work_cancellations",
    "work_inputs",
    "work_capabilities",
];

/// Input reference kind: the bytes are in `work_inputs` rows.
pub const REF_KIND_LEDGER_INPUT: &str = "ledger_input";
/// Input reference kind: the bytes exist only on the submitting node.
pub const REF_KIND_LOCAL_PATH: &str = "local_path";
/// Input reference kind reserved for the content-addressed media plane.
/// Nothing in this module resolves it.
pub const REF_KIND_BLOB_REF: &str = "blob_ref";

const CREATE_WORK_JOBS: &str = "CREATE TABLE work_jobs (\
     job_id TEXT PRIMARY KEY, \
     work_class TEXT NOT NULL, \
     mode TEXT NOT NULL, \
     requirement_tags JSON NOT NULL, \
     input_refs JSON NOT NULL, \
     output_schema TEXT, \
     priority INTEGER NOT NULL, \
     deadline TIMESTAMP, \
     max_attempts INTEGER NOT NULL, \
     submitter_node_id TEXT NOT NULL, \
     provenance JSON, \
     submitted_at TIMESTAMP NOT NULL) SYNC TWO WAY SYNC CONFLICT KEEP FIRST";

const CREATE_WORK_CLAIMS: &str = "CREATE TABLE work_claims (\
     claim_key TEXT PRIMARY KEY, \
     job_id TEXT NOT NULL, \
     attempt INTEGER NOT NULL, \
     node_id TEXT NOT NULL, \
     lease_deadline TIMESTAMP NOT NULL, \
     claimed_at TIMESTAMP NOT NULL) SYNC TWO WAY";

const CREATE_WORK_RESULTS: &str = "CREATE TABLE work_results (\
     job_id TEXT PRIMARY KEY, \
     attempt INTEGER NOT NULL, \
     executor_node_id TEXT NOT NULL, \
     output TEXT NOT NULL, \
     receipt JSON NOT NULL, \
     completed_at TIMESTAMP NOT NULL) SYNC TWO WAY";

const CREATE_WORK_FAILURES: &str = "CREATE TABLE work_failures (\
     failure_key TEXT PRIMARY KEY, \
     job_id TEXT NOT NULL, \
     attempt INTEGER NOT NULL, \
     node_id TEXT NOT NULL, \
     error TEXT NOT NULL, \
     failed_at TIMESTAMP NOT NULL) SYNC TWO WAY SYNC CONFLICT KEEP FIRST";

const CREATE_WORK_CANCELLATIONS: &str = "CREATE TABLE work_cancellations (\
     job_id TEXT PRIMARY KEY, \
     requested_by TEXT NOT NULL, \
     reason TEXT, \
     cancelled_at TIMESTAMP NOT NULL) SYNC TWO WAY";

// `work_inputs` is keep-first and it syncs, so it is not itself eligible for
// `HISTORY CURRENT ONLY` (its rows are immutable copies with no superseded
// versions to reclaim); its family member here is row LIFETIME, `RETAIN`.
// Retiring inputs on a window turns the pre-existing silent-empty read at
// `materialize_inputs` (a job whose ledger inputs aged out previously read
// back as "no rows", indistinguishable from a job with no ledger inputs at
// all) into a reachable production path -- `materialize_inputs` must be
// taught to distinguish the two and return `Error::WorkInputExpired` rather
// than an empty `ExecutionInputs`.
const CREATE_WORK_INPUTS: &str = "CREATE TABLE work_inputs (\
     input_key TEXT PRIMARY KEY, \
     job_id TEXT NOT NULL, \
     seq INTEGER NOT NULL, \
     payload TEXT NOT NULL) RETAIN 7 DAYS SYNC TWO WAY SYNC CONFLICT KEEP FIRST";

// A current-truth registry, re-advertised on a cadence (only the newest
// advertisement per node has consumer value), so it declares its own
// eligibility in its own DDL rather than riding a hardcoded table-name list:
// `HISTORY CURRENT ONLY` reclaims superseded advertisements, and the
// `SYNC CONFLICT KEEP LATEST` it already gets programmatically (see
// `work_ledger_conflict_policy_entries` below) is declared here too, so the
// stored meta and the runtime arbitration agree and `.schema` is honest.
const CREATE_WORK_CAPABILITIES: &str = "CREATE TABLE work_capabilities (\
     capability_key TEXT PRIMARY KEY, \
     node_id TEXT NOT NULL, \
     capability_id TEXT NOT NULL, \
     tags JSON NOT NULL, \
     detail JSON, \
     advertised_at TIMESTAMP NOT NULL) HISTORY CURRENT ONLY SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

/// Materialized input chunks: (sequence number, payload bytes).
pub type ExecutionInputs = Vec<(i64, Vec<u8>)>;

/// A typed reference to a job's input material. Never the bytes themselves.
#[derive(Debug, Clone, PartialEq)]
pub struct InputRef {
    pub kind: String,
    pub detail: serde_json::Value,
}

/// contextdb's own content-hash for the media plane: the BLAKE3 hash of a
/// blob's bytes. The stable `blob_ref` identity, not the fetch backend's hash
/// type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlobHash([u8; 32]);

impl BlobHash {
    pub fn of(bytes: &[u8]) -> BlobHash {
        BlobHash(blake3::hash(bytes).into())
    }

    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for b in self.0 {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
        }
        s
    }

    /// The raw 32 bytes of the content hash — for a caller that carries the
    /// content address as opaque bytes rather than this typed value.
    pub fn as_bytes(&self) -> [u8; 32] {
        self.0
    }

    /// Rebuild from the raw 32 bytes of a content hash.
    pub fn from_bytes(bytes: [u8; 32]) -> BlobHash {
        BlobHash(bytes)
    }

    pub fn from_hex(hex: &str) -> Result<BlobHash> {
        if hex.len() != 64 {
            return Err(Error::Other("blob hash hex must be 64 chars".to_string()));
        }
        let mut out = [0u8; 32];
        for (i, pair) in hex.as_bytes().chunks(2).enumerate() {
            let hi = (pair[0] as char).to_digit(16);
            let lo = (pair[1] as char).to_digit(16);
            match (hi, lo) {
                (Some(hi), Some(lo)) => out[i] = (hi * 16 + lo) as u8,
                _ => return Err(Error::Other("blob hash hex has a non-hex byte".to_string())),
            }
        }
        Ok(BlobHash(out))
    }
}

impl std::fmt::Display for BlobHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl AsRef<[u8]> for BlobHash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<[u8; 32]> for BlobHash {
    /// Mirrors [`BlobHash::from_bytes`]: wraps an existing 32-byte hash
    /// value, does not hash content. Use [`BlobHash::of`] to hash bytes.
    fn from(bytes: [u8; 32]) -> BlobHash {
        BlobHash::from_bytes(bytes)
    }
}

impl TryFrom<&str> for BlobHash {
    type Error = Error;

    /// Mirrors [`BlobHash::from_hex`].
    fn try_from(hex: &str) -> Result<BlobHash> {
        BlobHash::from_hex(hex)
    }
}

impl InputRef {
    /// The bytes travel as `work_inputs` rows keyed by the job id.
    pub fn ledger_input() -> Self {
        Self {
            kind: REF_KIND_LEDGER_INPUT.to_string(),
            detail: serde_json::Value::Null,
        }
    }

    /// The bytes stay on the submitting node; only that node can claim.
    pub fn local_path(path: &str) -> Self {
        Self {
            kind: REF_KIND_LOCAL_PATH.to_string(),
            detail: serde_json::json!({ "path": path }),
        }
    }

    /// The bytes are content-addressed by `hash`; a resolver fetches them
    /// node-to-node.
    pub fn blob_ref(hash: BlobHash) -> InputRef {
        InputRef {
            kind: REF_KIND_BLOB_REF.to_string(),
            detail: serde_json::json!({ "hash": hash.to_hex() }),
        }
    }
}

/// Everything a submitter states about a unit of work.
///
/// The fields are private so contextdb's internal layout is not frozen into
/// the public API: construct a spec through [`JobSpec::builder`] and read it
/// back through the getters. The serialized/persisted shape is unaffected —
/// this type is written to and read from the ledger field-by-field, not via a
/// derived codec.
#[derive(Debug, Clone, PartialEq)]
pub struct JobSpec {
    job_id: String,
    work_class: String,
    mode: String,
    /// Open-vocabulary requirement tags; a node may claim only when every tag
    /// appears in its advertisement.
    requirement_tags: Vec<String>,
    input_refs: Vec<InputRef>,
    /// Opaque required output shape; this module never interprets it.
    output_schema: Option<String>,
    priority: i64,
    deadline_ms: Option<i64>,
    max_attempts: i64,
    submitter_node_id: String,
    provenance: Option<serde_json::Value>,
    submitted_at_ms: i64,
}

impl JobSpec {
    /// Start building a job from its required identity: the id the submitter
    /// gives the work, its class and mode, and the submitting node. The
    /// optional facets default (no requirement tags, no input refs, no output
    /// schema, priority `0`, no deadline, `max_attempts` `2`, no provenance,
    /// `submitted_at_ms` `0`) and are set on the returned builder.
    pub fn builder(
        job_id: impl Into<String>,
        work_class: impl Into<String>,
        mode: impl Into<String>,
        submitter_node_id: impl Into<String>,
    ) -> JobSpecBuilder {
        JobSpecBuilder {
            job_id: job_id.into(),
            work_class: work_class.into(),
            mode: mode.into(),
            submitter_node_id: submitter_node_id.into(),
            requirement_tags: Vec::new(),
            input_refs: Vec::new(),
            output_schema: None,
            priority: 0,
            deadline_ms: None,
            max_attempts: 2,
            provenance: None,
            submitted_at_ms: 0,
        }
    }

    /// The id the submitter gave this unit of work.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// The work class a node advertises to be eligible.
    pub fn work_class(&self) -> &str {
        &self.work_class
    }

    /// The mode string; opaque to this module.
    pub fn mode(&self) -> &str {
        &self.mode
    }

    /// Open-vocabulary requirement tags; every one must be advertised to claim.
    pub fn requirement_tags(&self) -> &[String] {
        &self.requirement_tags
    }

    /// The job's input references.
    pub fn input_refs(&self) -> &[InputRef] {
        &self.input_refs
    }

    /// The opaque required output shape, if any.
    pub fn output_schema(&self) -> Option<&str> {
        self.output_schema.as_deref()
    }

    /// Scheduling priority; higher claims first.
    pub fn priority(&self) -> i64 {
        self.priority
    }

    /// The claim deadline in epoch milliseconds, if any.
    pub fn deadline_ms(&self) -> Option<i64> {
        self.deadline_ms
    }

    /// The attempt ceiling before the job is terminally failed.
    pub fn max_attempts(&self) -> i64 {
        self.max_attempts
    }

    /// The node that submitted the job.
    pub fn submitter_node_id(&self) -> &str {
        &self.submitter_node_id
    }

    /// Free-form provenance the submitter attached, if any.
    pub fn provenance(&self) -> Option<&serde_json::Value> {
        self.provenance.as_ref()
    }

    /// Submission time in epoch milliseconds.
    pub fn submitted_at_ms(&self) -> i64 {
        self.submitted_at_ms
    }
}

/// Builds a [`JobSpec`] from its required identity plus optional facets, so a
/// consumer never depends on the struct's internal field layout. Obtain one
/// from [`JobSpec::builder`].
#[derive(Debug, Clone)]
pub struct JobSpecBuilder {
    job_id: String,
    work_class: String,
    mode: String,
    requirement_tags: Vec<String>,
    input_refs: Vec<InputRef>,
    output_schema: Option<String>,
    priority: i64,
    deadline_ms: Option<i64>,
    max_attempts: i64,
    submitter_node_id: String,
    provenance: Option<serde_json::Value>,
    submitted_at_ms: i64,
}

impl JobSpecBuilder {
    /// Set the requirement tags (default: none).
    pub fn requirement_tags(mut self, tags: Vec<String>) -> Self {
        self.requirement_tags = tags;
        self
    }

    /// Set the input references (default: none).
    pub fn input_refs(mut self, refs: Vec<InputRef>) -> Self {
        self.input_refs = refs;
        self
    }

    /// Set the opaque required output shape (default: `None`).
    pub fn output_schema(mut self, schema: Option<String>) -> Self {
        self.output_schema = schema;
        self
    }

    /// Set the scheduling priority (default: `0`).
    pub fn priority(mut self, priority: i64) -> Self {
        self.priority = priority;
        self
    }

    /// Set the claim deadline in epoch milliseconds (default: `None`).
    pub fn deadline_ms(mut self, deadline_ms: Option<i64>) -> Self {
        self.deadline_ms = deadline_ms;
        self
    }

    /// Set the attempt ceiling (default: `2`).
    pub fn max_attempts(mut self, max_attempts: i64) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Set the free-form provenance (default: `None`).
    pub fn provenance(mut self, provenance: Option<serde_json::Value>) -> Self {
        self.provenance = provenance;
        self
    }

    /// Set the submission time in epoch milliseconds (default: `0`).
    pub fn submitted_at_ms(mut self, submitted_at_ms: i64) -> Self {
        self.submitted_at_ms = submitted_at_ms;
        self
    }

    /// Finish building the [`JobSpec`].
    pub fn build(self) -> JobSpec {
        JobSpec {
            job_id: self.job_id,
            work_class: self.work_class,
            mode: self.mode,
            requirement_tags: self.requirement_tags,
            input_refs: self.input_refs,
            output_schema: self.output_schema,
            priority: self.priority,
            deadline_ms: self.deadline_ms,
            max_attempts: self.max_attempts,
            submitter_node_id: self.submitter_node_id,
            provenance: self.provenance,
            submitted_at_ms: self.submitted_at_ms,
        }
    }
}

/// A job's computed state. Never stored; derived from rows by [`job_state`].
#[derive(Debug, Clone, PartialEq)]
pub enum JobState {
    Pending,
    Leased {
        node_id: String,
        attempt: i64,
        lease_deadline_ms: i64,
    },
    Done,
    Failed,
    Cancelled,
}

/// The read-back form of a submitted job.
#[derive(Debug, Clone, PartialEq)]
pub struct JobSnapshot {
    pub job_id: String,
    pub work_class: String,
    pub mode: String,
    pub requirement_tags: Vec<String>,
    pub input_refs: Vec<InputRef>,
    pub output_schema: Option<String>,
    pub priority: i64,
    pub deadline_ms: Option<i64>,
    pub max_attempts: i64,
    pub submitter_node_id: String,
    pub submitted_at_ms: i64,
}

/// The read-back form of a recorded result.
#[derive(Debug, Clone, PartialEq)]
pub struct JobResult {
    pub job_id: String,
    pub attempt: i64,
    pub executor_node_id: String,
    pub output: Vec<u8>,
    pub receipt: serde_json::Value,
    pub completed_at_ms: i64,
}

/// The local claim-insert outcome: the same-machine half of the lease.
#[derive(Debug, Clone, PartialEq)]
pub enum ClaimInsert {
    Inserted,
    AlreadyHeld { holder: String },
}

/// What may leave a node. One knob for the single-tenant case; enforcement
/// happens where a node claims or reads a job's input.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MovementPolicy {
    /// Off (default): nothing leaves the node; a node claims and reads only
    /// its own submissions. On: input copies may move between the tenant's
    /// machines.
    pub auto_propagate: bool,
}

impl MovementPolicy {
    /// May `node_id` read this job's input under this policy? The submitter
    /// always may. Anyone else may only when the policy propagates AND every
    /// input reference is in the movable allowlist ({`ledger_input`,
    /// `blob_ref`}) — `local_path` never leaves its node, and an
    /// unknown/future ref kind fails closed rather than falling open on
    /// "not local_path → admit".
    pub fn permits_input_read(&self, job: &JobSnapshot, node_id: &str) -> bool {
        if job.submitter_node_id == node_id {
            return true;
        }
        if !self.auto_propagate {
            return false;
        }
        job.input_refs.iter().all(|input_ref| {
            input_ref.kind == REF_KIND_LEDGER_INPUT || input_ref.kind == REF_KIND_BLOB_REF
        })
    }
}

fn text(value: &str) -> Value {
    Value::Text(value.to_string())
}

fn claim_key(job_id: &str, attempt: i64) -> String {
    format!("{job_id}#{attempt}")
}

fn failure_key(job_id: &str, attempt: i64) -> String {
    format!("{job_id}#{attempt}")
}

fn input_key(job_id: &str, seq: i64) -> String {
    format!("{job_id}#{seq}")
}

fn capability_key(node_id: &str, capability_id: &str) -> String {
    format!("{node_id}#{capability_id}")
}

fn encode_payload(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

fn decode_payload(encoded: &str) -> Result<Vec<u8>> {
    if !encoded.len().is_multiple_of(2) {
        return Err(Error::Other(format!(
            "work ledger: malformed payload encoding (odd length {})",
            encoded.len()
        )));
    }
    let mut out = Vec::with_capacity(encoded.len() / 2);
    let bytes = encoded.as_bytes();
    for pair in bytes.chunks(2) {
        let hi = (pair[0] as char).to_digit(16);
        let lo = (pair[1] as char).to_digit(16);
        match (hi, lo) {
            (Some(hi), Some(lo)) => out.push((hi * 16 + lo) as u8),
            _ => {
                return Err(Error::Other(
                    "work ledger: malformed payload encoding (non-hex byte)".to_string(),
                ));
            }
        }
    }
    Ok(out)
}

fn column_index(columns: &[String], name: &str) -> Result<usize> {
    columns
        .iter()
        .position(|c| c == name)
        .ok_or_else(|| Error::Other(format!("work ledger: missing column {name}")))
}

fn text_at(row: &[Value], idx: usize, what: &str) -> Result<String> {
    match &row[idx] {
        Value::Text(s) => Ok(s.clone()),
        other => Err(Error::Other(format!(
            "work ledger: {what} must be TEXT, found {other:?}"
        ))),
    }
}

fn opt_text_at(row: &[Value], idx: usize, what: &str) -> Result<Option<String>> {
    match &row[idx] {
        Value::Null => Ok(None),
        Value::Text(s) => Ok(Some(s.clone())),
        other => Err(Error::Other(format!(
            "work ledger: {what} must be TEXT or NULL, found {other:?}"
        ))),
    }
}

fn int_at(row: &[Value], idx: usize, what: &str) -> Result<i64> {
    match &row[idx] {
        Value::Int64(n) => Ok(*n),
        Value::Timestamp(n) => Ok(*n),
        other => Err(Error::Other(format!(
            "work ledger: {what} must be a number, found {other:?}"
        ))),
    }
}

fn opt_int_at(row: &[Value], idx: usize, what: &str) -> Result<Option<i64>> {
    match &row[idx] {
        Value::Null => Ok(None),
        other => int_at(std::slice::from_ref(other), 0, what).map(Some),
    }
}

fn json_at(row: &[Value], idx: usize, what: &str) -> Result<serde_json::Value> {
    match &row[idx] {
        Value::Json(v) => Ok(v.clone()),
        Value::Null => Ok(serde_json::Value::Null),
        other => Err(Error::Other(format!(
            "work ledger: {what} must be JSON, found {other:?}"
        ))),
    }
}

fn string_list(value: &serde_json::Value, what: &str) -> Result<Vec<String>> {
    let items = value
        .as_array()
        .ok_or_else(|| Error::Other(format!("work ledger: {what} must be a JSON array")))?;
    items
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_string)
                .ok_or_else(|| Error::Other(format!("work ledger: {what} entries must be strings")))
        })
        .collect()
}

fn input_refs_from_json(value: &serde_json::Value) -> Result<Vec<InputRef>> {
    let items = value
        .as_array()
        .ok_or_else(|| Error::Other("work ledger: input_refs must be a JSON array".to_string()))?;
    items
        .iter()
        .map(|item| {
            let kind = item
                .get("kind")
                .and_then(|k| k.as_str())
                .ok_or_else(|| {
                    Error::Other("work ledger: every input ref needs a string kind".to_string())
                })?
                .to_string();
            let detail = item
                .get("detail")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            Ok(InputRef { kind, detail })
        })
        .collect()
}

/// Decode one canonical `work_jobs.input_refs` value and return every
/// content-addressed blob identity in its canonical lowercase form.  This is
/// pure so destructive kernels can validate and prebuild their complete
/// outcome before crossing a persistence boundary.
pub(crate) fn canonical_blob_hashes_from_input_refs(value: &Value) -> Result<Vec<String>> {
    let Value::Json(value) = value else {
        return Err(Error::Other(
            "work ledger: input_refs must be JSON".to_string(),
        ));
    };
    input_refs_from_json(value)?
        .into_iter()
        .filter(|input_ref| input_ref.kind == REF_KIND_BLOB_REF)
        .map(|input_ref| {
            let hash = input_ref
                .detail
                .get("hash")
                .and_then(|value| value.as_str())
                .ok_or_else(|| {
                    Error::Other("work ledger: blob_ref input needs a string hash".to_string())
                })?;
            BlobHash::from_hex(hash).map(|hash| hash.to_hex())
        })
        .collect()
}

fn input_refs_to_json(refs: &[InputRef]) -> serde_json::Value {
    serde_json::Value::Array(
        refs.iter()
            .map(|r| serde_json::json!({ "kind": r.kind, "detail": r.detail }))
            .collect(),
    )
}

/// Create the seven ledger tables if absent. Idempotent; safe to call at
/// every open, and safe when the schema already arrived via sync.
pub fn install_work_ledger_schema(db: &Database) -> Result<()> {
    let existing = db.table_names();
    let ddl: [(&str, &str); 7] = [
        ("work_jobs", CREATE_WORK_JOBS),
        ("work_claims", CREATE_WORK_CLAIMS),
        ("work_results", CREATE_WORK_RESULTS),
        ("work_failures", CREATE_WORK_FAILURES),
        ("work_cancellations", CREATE_WORK_CANCELLATIONS),
        ("work_inputs", CREATE_WORK_INPUTS),
        ("work_capabilities", CREATE_WORK_CAPABILITIES),
    ];
    for (table, create) in ddl {
        if existing.iter().any(|name| name == table) {
            continue;
        }
        db.execute(create, &HashMap::new())?;
    }
    // A root created BEFORE `work_capabilities` declared its own HISTORY /
    // SYNC CONFLICT clauses (this installer is idempotent-by-absence, so an
    // existing table is never re-created) would otherwise silently lose
    // version-cleanup eligibility the moment the hardcoded table-name list
    // it used to ride is gone. Reconcile via the SAME `ALTER TABLE` an
    // operator would type -- no bespoke migration -- so an upgraded root
    // keeps cleaning itself. Order matters: KEEP LATEST first, so the table
    // is never observed keep-first while HISTORY CURRENT ONLY is declared
    // (the combination `refuse_reclaimed_history_under_keep_first` refuses).
    if existing.iter().any(|name| name == "work_capabilities")
        && let Some(meta) = db.table_meta("work_capabilities")
    {
        if meta.conflict_policy != Some(contextdb_core::ConflictPolicy::KEEP_LATEST) {
            db.execute(
                "ALTER TABLE work_capabilities SET SYNC CONFLICT KEEP LATEST",
                &HashMap::new(),
            )?;
        }
        if meta.history_policy != Some(contextdb_core::HistoryPolicy::CurrentOnly) {
            db.execute(
                "ALTER TABLE work_capabilities SET HISTORY CURRENT ONLY",
                &HashMap::new(),
            )?;
        }
    }
    // A root created BEFORE `work_inputs` declared its own `RETAIN 7 DAYS`
    // (this installer is idempotent-by-absence, so an existing table is
    // never re-created) would otherwise keep every input copy forever --
    // the exact unbounded-debris class this installer exists to bound, just
    // on the one table an upgraded operator never got the chance to
    // opt out of. Reconcile via the same `ALTER TABLE` an operator would
    // type, mirroring the `work_capabilities` reconcile above.
    if existing.iter().any(|name| name == "work_inputs")
        && let Some(meta) = db.table_meta("work_inputs")
        && meta.default_ttl_seconds.is_none()
    {
        db.execute("ALTER TABLE work_inputs SET RETAIN 7 DAYS", &HashMap::new())?;
    }
    // Installing the capability registry makes this database compaction-eligible;
    // start the engine-owned maintenance loop now so a fresh node self-bounds
    // within its first session (a reopen is covered by the open-time hook).
    // No-op if it is already running.
    db.reconcile_maintenance_thread();
    Ok(())
}

/// The per-table conflict contract (see the module doc for why each table
/// carries the policy it does).
fn work_ledger_conflict_policy_entries_inner() -> [(&'static str, ConflictPolicy); 7] {
    [
        ("work_jobs", ConflictPolicy::InsertIfNotExists),
        ("work_claims", ConflictPolicy::ServerWins),
        ("work_results", ConflictPolicy::ServerWins),
        ("work_failures", ConflictPolicy::InsertIfNotExists),
        ("work_cancellations", ConflictPolicy::ServerWins),
        ("work_inputs", ConflictPolicy::InsertIfNotExists),
        // The capability registry is current-truth: a re-advertise refreshes
        // `advertised_at` (see `advertise_capability`'s upsert), and that newer
        // claim must WIN at the hub so the fleet converges on the node's
        // present offer. LatestWins is ordered by LSN/TxId, so the later push
        // (higher LSN) supersedes the earlier row — NOT InsertIfNotExists,
        // which would silently drop every refresh after the first.
        ("work_capabilities", ConflictPolicy::LatestWins),
    ]
}

#[cfg(feature = "test-seams")]
pub fn work_ledger_conflict_policy_entries() -> [(&'static str, ConflictPolicy); 7] {
    work_ledger_conflict_policy_entries_inner()
}

/// Merge the ledger's per-table policies over `policies`. Sync chokepoints
/// call this so no caller can arbitrate a work table incorrectly.
fn apply_work_ledger_policy_overrides_inner(policies: &mut ConflictPolicies) {
    for (table, policy) in work_ledger_conflict_policy_entries_inner() {
        policies.per_table.insert(table.to_string(), policy);
    }
}

#[cfg(feature = "test-seams")]
pub fn apply_work_ledger_policy_overrides(policies: &mut ConflictPolicies) {
    apply_work_ledger_policy_overrides_inner(policies);
}

#[cfg(not(feature = "test-seams"))]
pub(crate) fn apply_work_ledger_policy_overrides(policies: &mut ConflictPolicies) {
    apply_work_ledger_policy_overrides_inner(policies);
}

/// Write the job row inside the caller's open transaction, so submission
/// commits atomically with the caller's own writes.
pub fn submit_job_in_tx(db: &Database, tx: TxId, spec: &JobSpec) -> Result<()> {
    if spec.job_id.is_empty() {
        return Err(Error::Other(
            "work ledger: job_id must not be empty".to_string(),
        ));
    }
    if spec.max_attempts < 1 {
        return Err(Error::Other(
            "work ledger: max_attempts must be at least 1".to_string(),
        ));
    }
    let mut params = HashMap::new();
    params.insert("job_id".to_string(), text(&spec.job_id));
    params.insert("work_class".to_string(), text(&spec.work_class));
    params.insert("mode".to_string(), text(&spec.mode));
    params.insert(
        "requirement_tags".to_string(),
        Value::Json(serde_json::json!(spec.requirement_tags)),
    );
    params.insert(
        "input_refs".to_string(),
        Value::Json(input_refs_to_json(&spec.input_refs)),
    );
    params.insert(
        "output_schema".to_string(),
        match &spec.output_schema {
            Some(schema) => text(schema),
            None => Value::Null,
        },
    );
    params.insert("priority".to_string(), Value::Int64(spec.priority));
    params.insert(
        "deadline".to_string(),
        match spec.deadline_ms {
            Some(ms) => Value::Timestamp(ms),
            None => Value::Null,
        },
    );
    params.insert("max_attempts".to_string(), Value::Int64(spec.max_attempts));
    params.insert(
        "submitter_node_id".to_string(),
        text(&spec.submitter_node_id),
    );
    params.insert(
        "provenance".to_string(),
        match &spec.provenance {
            Some(p) => Value::Json(p.clone()),
            None => Value::Null,
        },
    );
    params.insert(
        "submitted_at".to_string(),
        Value::Timestamp(spec.submitted_at_ms),
    );
    db.execute_in_tx(
        tx,
        "INSERT INTO work_jobs (job_id, work_class, mode, requirement_tags, input_refs, \
         output_schema, priority, deadline, max_attempts, submitter_node_id, provenance, \
         submitted_at) VALUES ($job_id, $work_class, $mode, $requirement_tags, $input_refs, \
         $output_schema, $priority, $deadline, $max_attempts, $submitter_node_id, $provenance, \
         $submitted_at)",
        &params,
    )?;
    Ok(())
}

/// Copy one chunk of input material into `work_inputs` inside the caller's
/// open transaction.
pub fn add_job_input_in_tx(
    db: &Database,
    tx: TxId,
    job_id: &str,
    seq: i64,
    payload: &[u8],
) -> Result<()> {
    let mut params = HashMap::new();
    params.insert("input_key".to_string(), text(&input_key(job_id, seq)));
    params.insert("job_id".to_string(), text(job_id));
    params.insert("seq".to_string(), Value::Int64(seq));
    params.insert("payload".to_string(), text(&encode_payload(payload)));
    db.execute_in_tx(
        tx,
        "INSERT INTO work_inputs (input_key, job_id, seq, payload) \
         VALUES ($input_key, $job_id, $seq, $payload)",
        &params,
    )?;
    Ok(())
}

/// Convenience: submit a job plus its input copies in ONE transaction.
///
/// # Errors
///
/// Returns an error (via [`contextdb_core::Error::Other`]) if `spec.job_id`
/// is empty, or if `spec.max_attempts` is less than 1.
pub fn submit_job<I: AsRef<[u8]>>(db: &Database, spec: &JobSpec, inputs: &[I]) -> Result<()> {
    let tx = db.begin()?;
    let write = (|| -> Result<()> {
        submit_job_in_tx(db, tx, spec)?;
        for (seq, payload) in inputs.iter().enumerate() {
            add_job_input_in_tx(db, tx, &spec.job_id, seq as i64, payload.as_ref())?;
        }
        Ok(())
    })();
    match write {
        Ok(()) => db.commit(tx),
        Err(err) => {
            let _ = db.rollback(tx);
            Err(err)
        }
    }
}

fn select_rows(
    db: &Database,
    sql: &str,
    params: &HashMap<String, Value>,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let result = db.execute(sql, params)?;
    Ok((result.columns, result.rows))
}

fn job_row(db: &Database, job_id: &str) -> Result<Option<JobSnapshot>> {
    let mut params = HashMap::new();
    params.insert("job_id".to_string(), text(job_id));
    let (columns, rows) = select_rows(
        db,
        "SELECT * FROM work_jobs WHERE job_id = $job_id",
        &params,
    )?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let snapshot = JobSnapshot {
        job_id: text_at(row, column_index(&columns, "job_id")?, "job_id")?,
        work_class: text_at(row, column_index(&columns, "work_class")?, "work_class")?,
        mode: text_at(row, column_index(&columns, "mode")?, "mode")?,
        requirement_tags: string_list(
            &json_at(
                row,
                column_index(&columns, "requirement_tags")?,
                "requirement_tags",
            )?,
            "requirement_tags",
        )?,
        input_refs: input_refs_from_json(&json_at(
            row,
            column_index(&columns, "input_refs")?,
            "input_refs",
        )?)?,
        output_schema: opt_text_at(
            row,
            column_index(&columns, "output_schema")?,
            "output_schema",
        )?,
        priority: int_at(row, column_index(&columns, "priority")?, "priority")?,
        deadline_ms: opt_int_at(row, column_index(&columns, "deadline")?, "deadline")?,
        max_attempts: int_at(row, column_index(&columns, "max_attempts")?, "max_attempts")?,
        submitter_node_id: text_at(
            row,
            column_index(&columns, "submitter_node_id")?,
            "submitter_node_id",
        )?,
        submitted_at_ms: int_at(row, column_index(&columns, "submitted_at")?, "submitted_at")?,
    };
    Ok(Some(snapshot))
}

/// Read back one job's submitted form.
pub fn job_snapshot(db: &Database, job_id: &str) -> Result<Option<JobSnapshot>> {
    job_row(db, job_id)
}

struct ClaimRow {
    attempt: i64,
    node_id: String,
    lease_deadline_ms: i64,
}

fn claim_rows(db: &Database, job_id: &str) -> Result<Vec<ClaimRow>> {
    let mut params = HashMap::new();
    params.insert("job_id".to_string(), text(job_id));
    let (columns, rows) = select_rows(
        db,
        "SELECT attempt, node_id, lease_deadline FROM work_claims WHERE job_id = $job_id",
        &params,
    )?;
    let attempt_idx = column_index(&columns, "attempt")?;
    let node_idx = column_index(&columns, "node_id")?;
    let lease_idx = column_index(&columns, "lease_deadline")?;
    rows.iter()
        .map(|row| {
            Ok(ClaimRow {
                attempt: int_at(row, attempt_idx, "attempt")?,
                node_id: text_at(row, node_idx, "node_id")?,
                lease_deadline_ms: int_at(row, lease_idx, "lease_deadline")?,
            })
        })
        .collect()
}

fn failure_attempts(db: &Database, job_id: &str) -> Result<Vec<i64>> {
    let mut params = HashMap::new();
    params.insert("job_id".to_string(), text(job_id));
    let (columns, rows) = select_rows(
        db,
        "SELECT attempt, failed_at FROM work_failures WHERE job_id = $job_id",
        &params,
    )?;
    let attempt_idx = column_index(&columns, "attempt")?;
    rows.iter()
        .map(|row| int_at(row, attempt_idx, "attempt"))
        .collect()
}

fn cancellation_exists(db: &Database, job_id: &str) -> Result<bool> {
    let mut params = HashMap::new();
    params.insert("job_id".to_string(), text(job_id));
    let (_, rows) = select_rows(
        db,
        "SELECT job_id FROM work_cancellations WHERE job_id = $job_id",
        &params,
    )?;
    Ok(!rows.is_empty())
}

/// Compute a job's state from its rows. `now_ms` is supplied by the caller —
/// lease expiry is advisory wall-clock, never engine-trusted time.
///
/// The fixed rules, in precedence order: a result row means done; a
/// cancellation row (with no result) means cancelled; failure rows at the
/// attempt ceiling mean failed; a live claim on the highest attempt means
/// leased; otherwise pending. This function reads jobs, claims, results,
/// failures, and cancellations — never `work_inputs` — so input retention
/// provably cannot change any computed state.
pub fn job_state(db: &Database, job_id: &str, now_ms: i64) -> Result<JobState> {
    let Some(job) = job_row(db, job_id)? else {
        return Err(Error::NotFound(format!("work ledger job {job_id}")));
    };
    if job_result(db, job_id)?.is_some() {
        return Ok(JobState::Done);
    }
    if cancellation_exists(db, job_id)? {
        return Ok(JobState::Cancelled);
    }
    let failures = failure_attempts(db, job_id)?;
    if (failures.len() as i64) >= job.max_attempts {
        return Ok(JobState::Failed);
    }
    let claims = claim_rows(db, job_id)?;
    // A claim holds the lease only while its deadline is ahead AND its own
    // attempt has no failure row — a recorded failure releases the lease
    // immediately (the failure row is what makes the next attempt legal).
    if let Some(highest) = claims.iter().max_by_key(|c| c.attempt)
        && highest.lease_deadline_ms > now_ms
        && !failures.contains(&highest.attempt)
    {
        return Ok(JobState::Leased {
            node_id: highest.node_id.clone(),
            attempt: highest.attempt,
            lease_deadline_ms: highest.lease_deadline_ms,
        });
    }
    Ok(JobState::Pending)
}

/// The jobs `node_id` may claim right now: state pending, every requirement
/// tag advertised, input readable under the movement policy. Ordered by
/// priority (desc), then deadline (asc, none last), then submission age.
pub fn claimable_jobs(
    db: &Database,
    node_id: &str,
    advertised_tags: &[String],
    policy: &MovementPolicy,
    now_ms: i64,
) -> Result<Vec<JobSnapshot>> {
    let (columns, rows) = select_rows(db, "SELECT job_id FROM work_jobs", &HashMap::new())?;
    let id_idx = column_index(&columns, "job_id")?;
    let mut matching = Vec::new();
    for row in &rows {
        let job_id = text_at(row, id_idx, "job_id")?;
        if job_state(db, &job_id, now_ms)? != JobState::Pending {
            continue;
        }
        let Some(job) = job_row(db, &job_id)? else {
            continue;
        };
        let tags_satisfied = job
            .requirement_tags
            .iter()
            .all(|needed| advertised_tags.iter().any(|have| have == needed));
        if !tags_satisfied {
            continue;
        }
        if !policy.permits_input_read(&job, node_id) {
            continue;
        }
        matching.push(job);
    }
    matching.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then_with(|| match (a.deadline_ms, b.deadline_ms) {
                (Some(a_dl), Some(b_dl)) => a_dl.cmp(&b_dl),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            })
            .then_with(|| a.submitted_at_ms.cmp(&b.submitted_at_ms))
            .then_with(|| a.job_id.cmp(&b.job_id))
    });
    Ok(matching)
}

/// Does `node_id` hold a live claim on a job whose input_refs contains `hash`?
///
/// Live mirrors [`job_state`]'s failure-aware rule, applied to `node_id`'s OWN
/// claim rather than the job's highest attempt: the claim's lease deadline is
/// still ahead of `now_ms` AND that specific attempt has no failure row. This
/// is deliberately independent of the job's terminal state (Cancelled/Done) —
/// a cancelled job whose claim lease is still live keeps its claimant
/// entitled, so the resolver can authorize and only then discover the bytes
/// were reclaimed (`BlobNotFound`), rather than that distinction collapsing
/// into `Unentitled` here.
pub fn node_holds_claim_for_blob(
    db: &Database,
    node_id: &str,
    hash: &BlobHash,
    now_ms: i64,
) -> Result<bool> {
    let target_hex = hash.to_hex();
    let (columns, rows) = select_rows(
        db,
        "SELECT job_id, attempt, lease_deadline FROM work_claims WHERE node_id = $node_id",
        &{
            let mut params = HashMap::new();
            params.insert("node_id".to_string(), text(node_id));
            params
        },
    )?;
    let job_idx = column_index(&columns, "job_id")?;
    let attempt_idx = column_index(&columns, "attempt")?;
    let lease_idx = column_index(&columns, "lease_deadline")?;
    for row in &rows {
        let lease_deadline_ms = int_at(row, lease_idx, "lease_deadline")?;
        if lease_deadline_ms <= now_ms {
            continue;
        }
        let job_id = text_at(row, job_idx, "job_id")?;
        let attempt = int_at(row, attempt_idx, "attempt")?;
        if failure_attempts(db, &job_id)?.contains(&attempt) {
            continue;
        }
        let Some(job) = job_row(db, &job_id)? else {
            continue;
        };
        let carries_hash = job.input_refs.iter().any(|input_ref| {
            input_ref.kind == REF_KIND_BLOB_REF
                && input_ref
                    .detail
                    .get("hash")
                    .and_then(|v| v.as_str())
                    .is_some_and(|hex| hex == target_hex)
        });
        if carries_hash {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Does ANY node hold a live claim for `hash` in this ledger, regardless of
/// which node it is? Unlike [`node_holds_claim_for_blob`], this does not
/// check the claimant's identity — it exists only as the resolver's own
/// local-mirror convenience pre-check (an optimization to skip a wasted
/// dial when the local ledger mirror has never seen ANY live claim for this
/// hash at all). It is never the security boundary: the holder's own
/// [`node_holds_claim_for_blob`] check against the transport-authenticated
/// caller is what actually authorizes a fetch.
pub fn any_node_holds_claim_for_blob(db: &Database, hash: &BlobHash, now_ms: i64) -> Result<bool> {
    let target_hex = hash.to_hex();
    let (columns, rows) = select_rows(
        db,
        "SELECT job_id, attempt, lease_deadline FROM work_claims",
        &HashMap::new(),
    )?;
    let job_idx = column_index(&columns, "job_id")?;
    let attempt_idx = column_index(&columns, "attempt")?;
    let lease_idx = column_index(&columns, "lease_deadline")?;
    for row in &rows {
        let lease_deadline_ms = int_at(row, lease_idx, "lease_deadline")?;
        if lease_deadline_ms <= now_ms {
            continue;
        }
        let job_id = text_at(row, job_idx, "job_id")?;
        let attempt = int_at(row, attempt_idx, "attempt")?;
        if failure_attempts(db, &job_id)?.contains(&attempt) {
            continue;
        }
        let Some(job) = job_row(db, &job_id)? else {
            continue;
        };
        let carries_hash = job.input_refs.iter().any(|input_ref| {
            input_ref.kind == REF_KIND_BLOB_REF
                && input_ref
                    .detail
                    .get("hash")
                    .and_then(|v| v.as_str())
                    .is_some_and(|hex| hex == target_hex)
        });
        if carries_hash {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The resolve-time policy re-check for one authenticated fetch, given that
/// `node_id` already holds a live claim on `hash`'s job (see
/// [`node_holds_claim_for_blob`], the separate entitlement gate): does
/// `policy` permit `node_id` to read THAT SPECIFIC job's input? Reuses
/// [`MovementPolicy::permits_input_read`] directly, so the submitter is
/// always permitted, propagation-off refuses every non-submitter claimant
/// regardless of input kind, and propagation-on refuses a claimant whenever
/// the job carries even one non-movable (`local_path`) input alongside the
/// `blob_ref` — a mixed job never auto-propagates. Ok(false) (never found a
/// live matching claim) on a caller that already failed the entitlement
/// gate is meaningless dead code, not a security gap: callers must run
/// `node_holds_claim_for_blob` first.
pub fn node_claim_permits_movement(
    db: &Database,
    node_id: &str,
    hash: &BlobHash,
    policy: MovementPolicy,
    now_ms: i64,
) -> Result<bool> {
    let target_hex = hash.to_hex();
    let (columns, rows) = select_rows(
        db,
        "SELECT job_id, attempt, lease_deadline FROM work_claims WHERE node_id = $node_id",
        &{
            let mut params = HashMap::new();
            params.insert("node_id".to_string(), text(node_id));
            params
        },
    )?;
    let job_idx = column_index(&columns, "job_id")?;
    let attempt_idx = column_index(&columns, "attempt")?;
    let lease_idx = column_index(&columns, "lease_deadline")?;
    for row in &rows {
        let lease_deadline_ms = int_at(row, lease_idx, "lease_deadline")?;
        if lease_deadline_ms <= now_ms {
            continue;
        }
        let job_id = text_at(row, job_idx, "job_id")?;
        let attempt = int_at(row, attempt_idx, "attempt")?;
        if failure_attempts(db, &job_id)?.contains(&attempt) {
            continue;
        }
        let Some(job) = job_row(db, &job_id)? else {
            continue;
        };
        let carries_hash = job.input_refs.iter().any(|input_ref| {
            input_ref.kind == REF_KIND_BLOB_REF
                && input_ref
                    .detail
                    .get("hash")
                    .and_then(|v| v.as_str())
                    .is_some_and(|hex| hex == target_hex)
        });
        if carries_hash {
            return Ok(policy.permits_input_read(&job, node_id));
        }
    }
    Ok(false)
}

/// May the local copy of blob `hash` be reclaimed at `now_ms`? True iff no
/// job references it, or EVERY referencing job is terminal
/// (done/failed/cancelled) with its terminal row older than `grace_ms`.
/// Retention (job terminal state) is a separate axis from entitlement
/// (claim-lease liveness) — a reclaimed blob's still-entitled claimant gets
/// BlobNotFound from the resolver, never Unentitled.
pub fn blob_ref_retention_eligible(
    db: &Database,
    hash: &BlobHash,
    now_ms: i64,
    grace_ms: i64,
) -> Result<bool> {
    let target_hex = hash.to_hex();
    let (columns, rows) = select_rows(db, "SELECT job_id FROM work_jobs", &HashMap::new())?;
    let id_idx = column_index(&columns, "job_id")?;
    for row in &rows {
        let job_id = text_at(row, id_idx, "job_id")?;
        let Some(job) = job_row(db, &job_id)? else {
            continue;
        };
        if !input_refs_carry_blob(&job, &target_hex) {
            continue;
        }
        let Some(terminal_at) = terminal_since(db, &job, now_ms)? else {
            // A live referencing job pins the blob.
            return Ok(false);
        };
        if terminal_at + grace_ms > now_ms {
            // Terminal but still inside its grace window.
            return Ok(false);
        }
    }
    Ok(true)
}

/// Does this job's input_refs contain a blob_ref for `target_hex`?
fn input_refs_carry_blob(job: &JobSnapshot, target_hex: &str) -> bool {
    job.input_refs.iter().any(|input_ref| {
        input_ref.kind == REF_KIND_BLOB_REF
            && input_ref
                .detail
                .get("hash")
                .and_then(|v| v.as_str())
                .is_some_and(|hex| hex == target_hex)
    })
}

/// The attempt number a new claim must use: one past the highest attempt any
/// claim or failure row records.
pub fn next_attempt(db: &Database, job_id: &str) -> Result<i64> {
    let highest_claim = claim_rows(db, job_id)?
        .iter()
        .map(|c| c.attempt)
        .max()
        .unwrap_or(0);
    let highest_failure = failure_attempts(db, job_id)?.into_iter().max().unwrap_or(0);
    Ok(highest_claim.max(highest_failure) + 1)
}

/// Insert the claim row locally. The table's primary key is the same-machine
/// half of the lease: a second local claimant gets [`ClaimInsert::AlreadyHeld`].
pub fn insert_claim(
    db: &Database,
    job_id: &str,
    attempt: i64,
    node_id: &str,
    lease_deadline_ms: i64,
    now_ms: i64,
) -> Result<ClaimInsert> {
    if let Some(holder) = claim_holder(db, job_id, attempt)? {
        return Ok(ClaimInsert::AlreadyHeld { holder });
    }
    let mut params = HashMap::new();
    params.insert("claim_key".to_string(), text(&claim_key(job_id, attempt)));
    params.insert("job_id".to_string(), text(job_id));
    params.insert("attempt".to_string(), Value::Int64(attempt));
    params.insert("node_id".to_string(), text(node_id));
    params.insert(
        "lease_deadline".to_string(),
        Value::Timestamp(lease_deadline_ms),
    );
    params.insert("claimed_at".to_string(), Value::Timestamp(now_ms));
    let inserted = db.execute(
        "INSERT INTO work_claims (claim_key, job_id, attempt, node_id, lease_deadline, \
         claimed_at) VALUES ($claim_key, $job_id, $attempt, $node_id, $lease_deadline, \
         $claimed_at)",
        &params,
    );
    match inserted {
        Ok(result) if result.rows_affected == 0 => {
            // A concurrent local claimant won the primary key.
            let holder = claim_holder(db, job_id, attempt)?.unwrap_or_default();
            Ok(ClaimInsert::AlreadyHeld { holder })
        }
        Ok(_) => Ok(ClaimInsert::Inserted),
        Err(Error::UniqueViolation { .. }) => {
            let holder = claim_holder(db, job_id, attempt)?.unwrap_or_default();
            Ok(ClaimInsert::AlreadyHeld { holder })
        }
        Err(err) => Err(err),
    }
}

/// Who holds the claim row for (job, attempt), if anyone.
pub fn claim_holder(db: &Database, job_id: &str, attempt: i64) -> Result<Option<String>> {
    let mut params = HashMap::new();
    params.insert("claim_key".to_string(), text(&claim_key(job_id, attempt)));
    let (columns, rows) = select_rows(
        db,
        "SELECT node_id FROM work_claims WHERE claim_key = $claim_key",
        &params,
    )?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    Ok(Some(text_at(
        row,
        column_index(&columns, "node_id")?,
        "node_id",
    )?))
}

/// Record the structured output plus its execution receipt. Exactly-once by
/// construction: when a result row for the job already exists, this is a
/// no-op `Ok` — duplicate work is caught, never double-recorded.
pub fn record_result(
    db: &Database,
    job_id: &str,
    attempt: i64,
    executor_node_id: &str,
    output: &[u8],
    receipt: serde_json::Value,
    now_ms: i64,
) -> Result<()> {
    if job_result(db, job_id)?.is_some() {
        return Ok(());
    }
    let mut params = HashMap::new();
    params.insert("job_id".to_string(), text(job_id));
    params.insert("attempt".to_string(), Value::Int64(attempt));
    params.insert("executor_node_id".to_string(), text(executor_node_id));
    params.insert("output".to_string(), text(&encode_payload(output)));
    params.insert("receipt".to_string(), Value::Json(receipt));
    params.insert("completed_at".to_string(), Value::Timestamp(now_ms));
    db.execute(
        "INSERT INTO work_results (job_id, attempt, executor_node_id, output, receipt, \
         completed_at) VALUES ($job_id, $attempt, $executor_node_id, $output, $receipt, \
         $completed_at)",
        &params,
    )?;
    Ok(())
}

/// Read back the job's result, if one landed.
pub fn job_result(db: &Database, job_id: &str) -> Result<Option<JobResult>> {
    let mut params = HashMap::new();
    params.insert("job_id".to_string(), text(job_id));
    let (columns, rows) = select_rows(
        db,
        "SELECT * FROM work_results WHERE job_id = $job_id",
        &params,
    )?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    Ok(Some(JobResult {
        job_id: text_at(row, column_index(&columns, "job_id")?, "job_id")?,
        attempt: int_at(row, column_index(&columns, "attempt")?, "attempt")?,
        executor_node_id: text_at(
            row,
            column_index(&columns, "executor_node_id")?,
            "executor_node_id",
        )?,
        output: decode_payload(&text_at(row, column_index(&columns, "output")?, "output")?)?,
        receipt: json_at(row, column_index(&columns, "receipt")?, "receipt")?,
        completed_at_ms: int_at(row, column_index(&columns, "completed_at")?, "completed_at")?,
    }))
}

/// Record one attempt's failure. The existence of this row is what makes the
/// next attempt legal; at the job's attempt ceiling the computed state turns
/// `Failed`. Idempotent for the same (job, attempt).
pub fn record_failure(
    db: &Database,
    job_id: &str,
    attempt: i64,
    node_id: &str,
    error: &str,
    now_ms: i64,
) -> Result<()> {
    let key = failure_key(job_id, attempt);
    let mut probe = HashMap::new();
    probe.insert("failure_key".to_string(), text(&key));
    let (_, rows) = select_rows(
        db,
        "SELECT failure_key FROM work_failures WHERE failure_key = $failure_key",
        &probe,
    )?;
    if !rows.is_empty() {
        return Ok(());
    }
    let mut params = HashMap::new();
    params.insert("failure_key".to_string(), text(&key));
    params.insert("job_id".to_string(), text(job_id));
    params.insert("attempt".to_string(), Value::Int64(attempt));
    params.insert("node_id".to_string(), text(node_id));
    params.insert("error".to_string(), text(error));
    params.insert("failed_at".to_string(), Value::Timestamp(now_ms));
    db.execute(
        "INSERT INTO work_failures (failure_key, job_id, attempt, node_id, error, failed_at) \
         VALUES ($failure_key, $job_id, $attempt, $node_id, $error, $failed_at)",
        &params,
    )?;
    Ok(())
}

/// Write the one cancellation row. Idempotent; a cancellation that loses the
/// race with a result is a no-op (the result stands).
pub fn cancel_job(
    db: &Database,
    job_id: &str,
    requested_by: &str,
    reason: Option<&str>,
    now_ms: i64,
) -> Result<()> {
    if cancellation_exists(db, job_id)? {
        return Ok(());
    }
    let mut params = HashMap::new();
    params.insert("job_id".to_string(), text(job_id));
    params.insert("requested_by".to_string(), text(requested_by));
    params.insert(
        "reason".to_string(),
        match reason {
            Some(r) => text(r),
            None => Value::Null,
        },
    );
    params.insert("cancelled_at".to_string(), Value::Timestamp(now_ms));
    db.execute(
        "INSERT INTO work_cancellations (job_id, requested_by, reason, cancelled_at) \
         VALUES ($job_id, $requested_by, $reason, $cancelled_at)",
        &params,
    )?;
    Ok(())
}

/// Advertise one capability this node offers, as open-vocabulary tags.
///
/// The capability registry is CURRENT-TRUTH, not workflow history: a node's
/// row carries the capability it offers *right now*. Re-advertising the same
/// (node, capability) key is therefore an UPSERT that refreshes
/// `advertised_at` (and `tags`/`detail`), not a write-once no-op — so a worker
/// restarted onto a different-but-same-id backend, or one re-driving the loop
/// on its poll cadence, makes its newer claim current instead of leaving the
/// hub pinned to a stale timestamp. The audit trail of *who did what when*
/// lives in the append-only workflow tables (claims/results/receipts), never
/// here. (See the `work_capabilities` LatestWins sync policy: the refreshed
/// timestamp propagates edge→hub.)
pub fn advertise_capability(
    db: &Database,
    node_id: &str,
    capability_id: &str,
    tags: &[String],
    now_ms: i64,
) -> Result<()> {
    let key = capability_key(node_id, capability_id);
    let mut params = HashMap::new();
    params.insert("capability_key".to_string(), text(&key));
    params.insert("node_id".to_string(), text(node_id));
    params.insert("capability_id".to_string(), text(capability_id));
    params.insert("tags".to_string(), Value::Json(serde_json::json!(tags)));
    params.insert("detail".to_string(), Value::Null);
    params.insert("advertised_at".to_string(), Value::Timestamp(now_ms));
    db.execute(
        "INSERT INTO work_capabilities (capability_key, node_id, capability_id, tags, detail, \
         advertised_at) VALUES ($capability_key, $node_id, $capability_id, $tags, $detail, \
         $advertised_at) ON CONFLICT (capability_key) DO UPDATE SET \
         advertised_at = $advertised_at, tags = $tags, detail = $detail",
        &params,
    )?;
    Ok(())
}

/// The union of tags this node has advertised across its capability rows.
pub fn advertised_tags(db: &Database, node_id: &str) -> Result<Vec<String>> {
    let mut params = HashMap::new();
    params.insert("node_id".to_string(), text(node_id));
    let (columns, rows) = select_rows(
        db,
        "SELECT tags FROM work_capabilities WHERE node_id = $node_id",
        &params,
    )?;
    let tags_idx = column_index(&columns, "tags")?;
    let mut union: Vec<String> = Vec::new();
    for row in &rows {
        for tag in string_list(&json_at(row, tags_idx, "tags")?, "tags")? {
            if !union.contains(&tag) {
                union.push(tag);
            }
        }
    }
    Ok(union)
}

/// Read the job's input copies, enforcing the movement policy at the read.
///
/// # Errors
///
/// - [`contextdb_core::Error::Other`] if the movement policy does not
///   permit `node_id` to read this job's input.
/// - [`contextdb_core::Error::InputRequiresBlobResolver`] if any of the
///   job's inputs is a `blob_ref` — its bytes never ride the ledger, so the
///   caller must fetch it through the blob resolver
///   ([`crate::blob_store::BlobStore::resolve_blob_ref`])
///   instead of this function.
pub fn materialize_inputs(
    db: &Database,
    job_id: &str,
    node_id: &str,
    policy: &MovementPolicy,
) -> Result<ExecutionInputs> {
    let job = job_row(db, job_id)?;
    let mut declares_ledger_input = false;
    if let Some(job) = &job {
        if !policy.permits_input_read(job, node_id) {
            return Err(Error::Other(format!(
                "work ledger: the movement policy does not permit node {node_id} to read the \
                 input of job {job_id}"
            )));
        }
        // A blob_ref input has no work_inputs row (its bytes never ride the
        // ledger); reading only work_inputs would silently return empty (pure
        // blob_ref) or partial (mixed ledger_input/blob_ref) bytes. Route to
        // the resolver instead.
        if job
            .input_refs
            .iter()
            .any(|input_ref| input_ref.kind == REF_KIND_BLOB_REF)
        {
            return Err(Error::InputRequiresBlobResolver {
                job_id: job_id.to_string(),
            });
        }
        declares_ledger_input = job
            .input_refs
            .iter()
            .any(|input_ref| input_ref.kind == REF_KIND_LEDGER_INPUT);
    }
    let mut params = HashMap::new();
    params.insert("job_id".to_string(), text(job_id));
    let (columns, rows) = select_rows(
        db,
        "SELECT seq, payload FROM work_inputs WHERE job_id = $job_id",
        &params,
    )?;
    let seq_idx = column_index(&columns, "seq")?;
    let payload_idx = column_index(&columns, "payload")?;
    let mut inputs: ExecutionInputs = rows
        .iter()
        .map(|row| {
            let seq = int_at(row, seq_idx, "seq")?;
            let payload = decode_payload(&text_at(row, payload_idx, "payload")?)?;
            Ok((seq, payload))
        })
        .collect::<Result<_>>()?;
    inputs.sort_by_key(|(seq, _)| *seq);
    // Distinct from a job that never declared ledger input at all: this job's
    // reference NAMES the ledger-input kind, but `work_inputs`' declared
    // `RETAIN` window (see `CREATE_WORK_INPUTS`) has already aged its rows
    // out. Silently returning empty here would let a worker execute on
    // silently-empty input; a typed refusal makes the gap unmistakable.
    if inputs.is_empty() && declares_ledger_input {
        return Err(Error::WorkInputExpired {
            job_id: job_id.to_string(),
        });
    }
    Ok(inputs)
}

fn terminal_since(db: &Database, job: &JobSnapshot, now_ms: i64) -> Result<Option<i64>> {
    match job_state(db, &job.job_id, now_ms)? {
        JobState::Done => Ok(job_result(db, &job.job_id)?.map(|r| r.completed_at_ms)),
        JobState::Cancelled => {
            let mut params = HashMap::new();
            params.insert("job_id".to_string(), text(&job.job_id));
            let (columns, rows) = select_rows(
                db,
                "SELECT cancelled_at FROM work_cancellations WHERE job_id = $job_id",
                &params,
            )?;
            let idx = column_index(&columns, "cancelled_at")?;
            rows.first()
                .map(|row| int_at(row, idx, "cancelled_at"))
                .transpose()
        }
        JobState::Failed => {
            let mut params = HashMap::new();
            params.insert("job_id".to_string(), text(&job.job_id));
            let (columns, rows) = select_rows(
                db,
                "SELECT failed_at FROM work_failures WHERE job_id = $job_id",
                &params,
            )?;
            let idx = column_index(&columns, "failed_at")?;
            let mut latest = None;
            for row in &rows {
                let at = int_at(row, idx, "failed_at")?;
                latest = Some(latest.map_or(at, |prev: i64| prev.max(at)));
            }
            Ok(latest)
        }
        _ => Ok(None),
    }
}

/// Remove the input copies of jobs that are done, failed, or cancelled and
/// whose terminal row is older than the grace period. The ONLY delete in the
/// ledger workflow; the state computation never reads `work_inputs`, so this
/// provably cannot change any job's state. Returns the number of rows
/// removed on this node.
pub fn run_input_retention(db: &Database, now_ms: i64, grace_ms: i64) -> Result<usize> {
    let (columns, rows) = select_rows(db, "SELECT job_id FROM work_jobs", &HashMap::new())?;
    let id_idx = column_index(&columns, "job_id")?;
    let mut removed = 0usize;
    for row in &rows {
        let job_id = text_at(row, id_idx, "job_id")?;
        let Some(job) = job_row(db, &job_id)? else {
            continue;
        };
        let Some(terminal_at) = terminal_since(db, &job, now_ms)? else {
            continue;
        };
        if terminal_at + grace_ms > now_ms {
            continue;
        }
        let mut params = HashMap::new();
        params.insert("job_id".to_string(), text(&job_id));
        let result = db.execute("DELETE FROM work_inputs WHERE job_id = $job_id", &params)?;
        removed += result.rows_affected as usize;
    }
    Ok(removed)
}

/// Should a worker holding (job, attempt) stop? True when a result already
/// landed, the job was cancelled, or the claim row for this attempt names a
/// different holder (a lost race reconciled under the worker). Lease expiry
/// alone is NOT abandonment — a revived worker may finish; the result stays
/// exactly-once by key.
pub fn should_abandon(
    db: &Database,
    job_id: &str,
    attempt: i64,
    node_id: &str,
    now_ms: i64,
) -> Result<bool> {
    let _ = now_ms;
    if job_result(db, job_id)?.is_some() {
        return Ok(true);
    }
    if cancellation_exists(db, job_id)? {
        return Ok(true);
    }
    match claim_holder(db, job_id, attempt)? {
        Some(holder) => Ok(holder != node_id),
        None => Ok(true),
    }
}

#[cfg(test)]
mod jobspec_builder_tests {
    use super::*;

    // A consumer builds a JobSpec through the builder and reads it back through
    // getters — never a struct literal, never a field access. This is the whole
    // point: the internal field layout is decoupled from the public surface.
    #[test]
    fn jobspec_is_built_via_builder_and_read_via_getters() {
        let spec = JobSpec::builder("job-1", "wl.demo", "demo-transform", "node-a")
            .requirement_tags(vec!["class:wl.demo".to_string()])
            .input_refs(vec![InputRef::ledger_input()])
            .output_schema(Some("demo-output-v1".to_string()))
            .priority(5)
            .deadline_ms(Some(42))
            .max_attempts(3)
            .provenance(Some(serde_json::json!({ "origin": "test" })))
            .submitted_at_ms(1_700_000_000_000)
            .build();

        assert_eq!(spec.job_id(), "job-1");
        assert_eq!(spec.work_class(), "wl.demo");
        assert_eq!(spec.mode(), "demo-transform");
        assert_eq!(spec.submitter_node_id(), "node-a");
        assert_eq!(spec.requirement_tags(), &["class:wl.demo".to_string()]);
        assert_eq!(spec.input_refs().len(), 1);
        assert_eq!(spec.input_refs()[0].kind, REF_KIND_LEDGER_INPUT);
        assert_eq!(spec.output_schema(), Some("demo-output-v1"));
        assert_eq!(spec.priority(), 5);
        assert_eq!(spec.deadline_ms(), Some(42));
        assert_eq!(spec.max_attempts(), 3);
        assert_eq!(
            spec.provenance(),
            Some(&serde_json::json!({ "origin": "test" }))
        );
        assert_eq!(spec.submitted_at_ms(), 1_700_000_000_000);
    }

    // The optional facets default sensibly, so identity-only construction is
    // ergonomic and the common `max_attempts` value (2) needs no setter.
    #[test]
    fn jobspec_builder_defaults_are_sensible() {
        let spec = JobSpec::builder("job-2", "wl.demo", "demo-transform", "node-b").build();

        assert!(spec.requirement_tags().is_empty());
        assert!(spec.input_refs().is_empty());
        assert_eq!(spec.output_schema(), None);
        assert_eq!(spec.priority(), 0);
        assert_eq!(spec.deadline_ms(), None);
        assert_eq!(spec.max_attempts(), 2);
        assert_eq!(spec.provenance(), None);
        assert_eq!(spec.submitted_at_ms(), 0);
    }
}
