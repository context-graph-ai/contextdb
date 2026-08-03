//! `TableMeta.history_policy` is a TAIL field on the hand-written positional
//! `Deserialize` impl, exactly like `conflict_policy` before it: a table
//! declares `HISTORY ALL` / `HISTORY CURRENT ONLY` in its own `CREATE TABLE`,
//! the choice is stored on `TableMeta`, and the persisted bytes must (a) round
//! trip a NEWLY declared policy through the real on-disk encoding (`bincode`
//! via `bincode::serde`, the path the engine's own persistence layer uses --
//! see `crates/contextdb-engine/src/persistence.rs`), and (b) tolerate an
//! EXISTING on-disk `TableMeta` that predates the field, decoding it with
//! `history_policy == None` rather than refusing to load.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads --
//! pure encode/decode round trips.

use contextdb_core::{
    ColumnDef, ColumnType, CompositeForeignKey, ConflictPolicy, HistoryPolicy, IndexDecl,
    PropagationRule, RetainUnit, StateMachineConstraint, SyncDirection, TableMeta,
};
use serde::Serialize;

fn sample_columns() -> Vec<ColumnDef> {
    vec![ColumnDef {
        name: "id".to_string(),
        column_type: ColumnType::Integer,
        nullable: false,
        primary_key: true,
        unique: false,
        default: None,
        references: None,
        expires: false,
        immutable: false,
        quantization: Default::default(),
        rank_policy: None,
        context_id: false,
        scope_label: None,
        acl_ref: None,
    }]
}

fn sample_meta(history_policy: Option<HistoryPolicy>) -> TableMeta {
    TableMeta {
        columns: sample_columns(),
        immutable: false,
        state_machine: None,
        dag_edge_types: Vec::new(),
        unique_constraints: Vec::new(),
        natural_key_column: None,
        propagation_rules: Vec::new(),
        default_ttl_seconds: None,
        sync_safe: false,
        expires_column: None,
        indexes: Vec::new(),
        composite_foreign_keys: Vec::new(),
        sync_direction: Some(SyncDirection::Both),
        retain_declared_unit: None,
        primary_key_columns: Vec::new(),
        conflict_policy: Some(ConflictPolicy::KEEP_LATEST),
        history_policy,
    }
}

/// The exact 16-field shape `TableMeta` had before `history_policy` was
/// appended -- an on-disk payload written by a build that predates this
/// change. `bincode`'s serde path is non-self-describing (no field names, no
/// struct framing beyond the fields themselves), so a struct with the SAME
/// field order and types serializes to byte-identical output to what the
/// pre-change `TableMeta` produced.
#[derive(Serialize)]
struct PreHistoryPolicyTableMeta {
    columns: Vec<ColumnDef>,
    immutable: bool,
    state_machine: Option<StateMachineConstraint>,
    dag_edge_types: Vec<String>,
    unique_constraints: Vec<Vec<String>>,
    natural_key_column: Option<String>,
    propagation_rules: Vec<PropagationRule>,
    default_ttl_seconds: Option<u64>,
    sync_safe: bool,
    expires_column: Option<String>,
    indexes: Vec<IndexDecl>,
    composite_foreign_keys: Vec<CompositeForeignKey>,
    sync_direction: Option<SyncDirection>,
    retain_declared_unit: Option<RetainUnit>,
    primary_key_columns: Vec<String>,
    conflict_policy: Option<ConflictPolicy>,
}

fn legacy_bytes_without_history_policy() -> Vec<u8> {
    let legacy = PreHistoryPolicyTableMeta {
        columns: sample_columns(),
        immutable: false,
        state_machine: None,
        dag_edge_types: Vec::new(),
        unique_constraints: Vec::new(),
        natural_key_column: None,
        propagation_rules: Vec::new(),
        default_ttl_seconds: None,
        sync_safe: false,
        expires_column: None,
        indexes: Vec::new(),
        composite_foreign_keys: Vec::new(),
        sync_direction: Some(SyncDirection::Both),
        retain_declared_unit: None,
        primary_key_columns: Vec::new(),
        conflict_policy: Some(ConflictPolicy::KEEP_LATEST),
    };
    bincode::serde::encode_to_vec(&legacy, bincode::config::standard())
        .expect("legacy shape must encode")
}

/// A NEWLY declared `HISTORY CURRENT ONLY` survives the real on-disk encoding
/// round trip -- the declaration is not silently lost between a `flush_table_meta`
/// and a later `load_all_table_meta` (bincode via `bincode::serde`, the path
/// `RedbPersistence` uses).
#[test]
fn declared_history_policy_round_trips_through_the_real_on_disk_encoding() {
    for policy in [HistoryPolicy::All, HistoryPolicy::CurrentOnly] {
        let meta = sample_meta(Some(policy));
        let bytes = bincode::serde::encode_to_vec(&meta, bincode::config::standard())
            .expect("TableMeta must encode");
        let (decoded, _): (TableMeta, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                .expect("TableMeta must decode");
        assert_eq!(
            decoded.history_policy,
            Some(policy),
            "a declared HISTORY policy must survive the on-disk round trip"
        );
        // The neighboring fields must not be disturbed by the new tail field.
        assert_eq!(decoded.conflict_policy, Some(ConflictPolicy::KEEP_LATEST));
        assert_eq!(decoded.sync_direction, Some(SyncDirection::Both));
    }
}

/// An on-disk `TableMeta` written before `history_policy` existed still loads:
/// the tail is absent, and the field decodes as `None` rather than failing the
/// whole table's metadata load. This is the compatibility half of the tail-field
/// contract -- an existing data root must open unchanged after this build lands.
#[test]
fn a_pre_history_policy_on_disk_table_meta_still_loads_with_no_declared_policy() {
    let bytes = legacy_bytes_without_history_policy();
    let (decoded, _): (TableMeta, usize) =
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap_or_else(
            |err| {
                panic!(
                    "a pre-existing on-disk TableMeta must still decode, not \
                 refuse to load: {err}"
                )
            },
        );
    assert_eq!(
        decoded.history_policy, None,
        "a pre-existing table with no HISTORY declaration must decode as undeclared, not error \
         and not default to a policy nobody wrote"
    );
    // The rest of the legacy payload must still decode intact.
    assert_eq!(decoded.conflict_policy, Some(ConflictPolicy::KEEP_LATEST));
    assert_eq!(decoded.columns.len(), 1);
}

/// The shape `TableMeta` had two fields further back -- BEFORE
/// `conflict_policy` existed either, so this predates the current change
/// entirely. This isolates whether a short-tail decode failure is specific
/// to the newly appended field or a property of the encoding shared by
/// every trailing field this struct has ever grown.
#[derive(Serialize)]
struct PreConflictPolicyTableMeta {
    columns: Vec<ColumnDef>,
    immutable: bool,
    state_machine: Option<StateMachineConstraint>,
    dag_edge_types: Vec<String>,
    unique_constraints: Vec<Vec<String>>,
    natural_key_column: Option<String>,
    propagation_rules: Vec<PropagationRule>,
    default_ttl_seconds: Option<u64>,
    sync_safe: bool,
    expires_column: Option<String>,
    indexes: Vec<IndexDecl>,
    composite_foreign_keys: Vec<CompositeForeignKey>,
    sync_direction: Option<SyncDirection>,
    retain_declared_unit: Option<RetainUnit>,
    primary_key_columns: Vec<String>,
}

/// Whether the compatibility gap above is unique to `history_policy` or a
/// property of every trailing field this struct carries: decoding a payload
/// two fields shorter (missing BOTH `conflict_policy` and `history_policy`)
/// through the exact same on-disk path.
#[test]
fn a_two_fields_shorter_on_disk_table_meta_shows_the_same_short_tail_behavior() {
    let legacy = PreConflictPolicyTableMeta {
        columns: sample_columns(),
        immutable: false,
        state_machine: None,
        dag_edge_types: Vec::new(),
        unique_constraints: Vec::new(),
        natural_key_column: None,
        propagation_rules: Vec::new(),
        default_ttl_seconds: None,
        sync_safe: false,
        expires_column: None,
        indexes: Vec::new(),
        composite_foreign_keys: Vec::new(),
        sync_direction: Some(SyncDirection::Both),
        retain_declared_unit: None,
        primary_key_columns: Vec::new(),
    };
    let bytes = bincode::serde::encode_to_vec(&legacy, bincode::config::standard())
        .expect("legacy shape must encode");
    let result: Result<(TableMeta, usize), _> =
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard());
    // This assertion states what SHOULD happen (graceful tolerance,
    // matching the documented intent of `.unwrap_or_default()` on every
    // trailing field). If it fails, the failure is evidence, not a
    // fixture bug: it shows the same short-tail decode gap the
    // `history_policy` test above hits is not new to this change -- it
    // reaches every field this struct has appended positionally, because
    // bincode's non-self-describing sequence decode has no way to signal
    // "no more elements" other than running out of bytes, which the
    // hand-written visitor's `next_element` calls turn into a hard decode
    // error rather than the `Ok(None)` the `.unwrap_or_default()` calls
    // assume.
    assert!(
        result.is_ok(),
        "a TableMeta payload missing its last two fields must still decode (defaulting both), \
         not hard-fail: {result:?}"
    );
}
