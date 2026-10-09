# contextdb-core — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md).

**Purpose:** the dependency-free vocabulary every other crate speaks — identifier newtypes,
`Value`, `TableMeta`, the executor traits, the one `Error` type, the clock seam, and the typed read
contract.

## Owns / must not own

| Module | Owns |
|---|---|
| `types.rs` | `TxId`, `SnapshotId`, `RowId`, `Lsn` newtypes and their atomics; `Value`; `VersionedRow`; `TenantId`, `Incarnation`, `Principal`, `ScopeLabel`; vector vocabulary (`VectorSearchMode`, `VectorPartitionKey`, `VectorIndexRef`); `Wallclock` and its test guard. |
| `table_meta.rs` | `TableMeta`, `ColumnDef`, `ConflictPolicy`, `HistoryPolicy`, `SyncDirection`, vector policy defaults, `MaintainedCounts` / `DEFAULT_MAINTAINED_COUNTS`, and the hand-written positional `Deserialize` that defaults supported missing tail fields. |
| `error.rs` | `Error` (`#[non_exhaustive]`) and the structural refusal reasons for vector declarations. |
| `traits.rs` | `RelationalExecutor`, `GraphExecutor`, `VectorExecutor`, `TransactionManager`. |
| `read_contract.rs` | Refusal classes and kinds, read limits, owner identities and statuses shared by both read routes — values only. |
| `read_memory.rs` / `memory.rs` | Owned memory charges and the budget capability the engine supplies. |
| `companion.rs` | `store_companion_path` and `store_holders_path` — the one source of the store's `.lock` and `.holders` companion names. |

Must not own: behavior. No storage, routing, transport, persistence, or execution; `read_contract`
states this in its own module doc. No dependency on another contextdb crate (engine and server are
dev-dependencies only, for the audits).

## Seams

Below: nothing in-house (`serde`, `bincode`, `uuid`, `thiserror`, `roaring`). Above: every crate.
`table_meta::*` is re-exported explicitly, never by glob — `SortDirection` collides with the
parser's. vigil links this crate directly, so anything a consumer needs unconditionally (the
companion path) lives here, not in the engine.

## Invariants and their guards

| Invariant | Guard |
|---|---|
| Identifier newtypes never coerce into each other or from bare `u64` | `tests/txid_newtype_tests.rs::newtype_cross_assignments_do_not_compile`, `::no_from_u64_for_typed_identifiers` |
| Newtypes keep their wire bytes identical to the inner `u64` | `tests/txid_newtype_tests.rs::wire_value_variants_match_golden_bytes`, `::wire_struct_round_trip_byte_identical` |
| No bare `u64` in identifier positions anywhere in the workspace | `tests/type_spread_audit.rs::type_spread_no_bare_u64_in_identifier_positions` |
| Typed atomics are used only through their allowed methods | `tests/atomic_wrapper_audit.rs::atomic_wrapper_only_five_methods_used_on_typed_atomics` |
| `Error` cannot be matched exhaustively outside this crate | `tests/error_non_exhaustive_tests.rs::error_enum_rejects_an_exhaustive_match_from_an_external_crate` |
| A reader registry that cannot be authenticated refuses the affected purge or reclamation operation by name | `crates/contextdb-engine/tests/read/authoritative_purge_reader_publication.rs::an_unreadable_candidate_refuses_purge_without_blocking_database_open`, `::an_unreadable_candidate_refuses_writer_reclamation_until_it_is_restored` |
| A newly declared history policy round-trips; a `TableMeta` payload without that tail field defaults it | `tests/history_policy_tail_decode_tests.rs::declared_history_policy_round_trips_through_the_real_on_disk_encoding`, `::a_pre_history_policy_on_disk_table_meta_still_loads_with_no_declared_policy` |
| An undeclared maintained-count policy maintains no count; the engine must still answer the exact number on every read route | `crates/contextdb-engine/tests/read/an_undeclared_count_answers_exactly_on_every_route.rs::an_undeclared_count_answers_exactly_on_every_route` |
| bincode goes through its serde path only | `tests/bincode_serde_path_audit.rs::bincode_serde_path_audit` |
| Every recorded redb fork patch hunk produces the maintained source lines it names | `tests/redb_fork_patch_matches_its_sources.rs::the_redb_fork_patch_produces_the_sources_it_names` |
| The companion lock appends `.lock` to the full store name | `tests/store_companion_path_appends_lock_to_the_full_store_name.rs::store_companion_path_keeps_the_store_s_own_extension_intact` |
| Shipped read limits and timeouts are exact, and invalid ones are refused | `tests/read_contract_types.rs::read_limits_ship_the_exact_read_policy`, `::read_limit_validation_rejects_zero_and_every_invalid_relationship` |
| The owner route takes the stricter limit field by field | `tests/read_contract_types.rs::owner_route_uses_the_stricter_limit_field_by_field` |
| Every refusal renders as prose naming the crossed ceiling | `tests/read_failure_wording.rs::every_refusal_kind_renders_as_human_prose`, `::a_crossed_ceiling_is_named_in_the_rendered_refusal` |
| Clock injection reaches `Wallclock::now()` | `tests/txid_newtype_tests.rs::wallclock_seam_enables_clock_injection` |
| No new sleep or raw clock read in any test file (ratchet; lower, never raise) | `tests/test_estate_audit.rs::test_estate_audit_no_new_sleeps_or_raw_clock_reads` |
| No new timestamp-shaped column outside the whitelist | `tests/timestamp_audit.rs::timestamp_audit_no_new_txid_shaped_columns` |
| Operator recipes in `AGENTS.md`, `skills/`, `docs/` match the CLI contract | `tests/operator_guidance_contract.rs::every_file_backed_mutation_skill_demonstrates_the_write_flag` and siblings |
| `TenantId::config_key` is the one watermark key format | `tests/tenant_id_newtype_tests.rs::config_key_is_byte_identical_to_the_legacy_interpolation` |

The audits scan the whole workspace, not just this crate; clock-seam mechanics are in
[`crates/contextdb-engine/AGENTS.md`](../contextdb-engine/AGENTS.md).

## Where a change lives

| Change | Where it lands |
|---|---|
| A new failure a caller branches on | A variant in `error.rs`; never a string inside `PlanError`/`ParseError`. |
| A new `TableMeta` field | `table_meta.rs`: append it at the tail of the positional encoding and decode it with `decode_tail_field`; add a short-tail case beside the history-policy tests. |
| A new identifier | A newtype in `types.rs`, no `From<u64>`; the type-spread audit then covers it. |
| A new refusal class or read limit | `read_contract.rs`; never a second refusal type downstream. |
| A new executor capability shared by stores | `traits.rs`, implemented in the store crates. |
| Find attribution or serving-cache accounting shared across crates | `find_attribution.rs`, `serving_cache_share.rs`; export shared types from `lib.rs`. |
| Workspace documentation bindings or maintained redb patch consistency | `tests/docs_audit.rs`, `tests/redb_fork_patch_matches_its_sources.rs`; these audit records and source references, while storage behavior stays in redb and the engine. |

## Fast test

```bash
cargo test -p contextdb-core   # builds engine and server as dev-dependencies
```
