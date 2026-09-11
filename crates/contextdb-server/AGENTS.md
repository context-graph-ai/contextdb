# contextdb-server — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md). This crate is the sync mover: the
`contextdb-server` hub binary and the library surface an edge embeds (`SyncClient`,
`SyncServer`, `SyncPlugin`, the distributed work-ledger calls) to move changesets between nodes.

## Owns, and must not own

Owns: the `contextdb-server` binary (`main.rs`: flags, identity file, owner-read options, exit
codes in `exit_codes.rs`); the automatic push trigger (`sync_plugin.rs`); claim-by-push and the
worker poll loop (`work_ledger.rs`); request chunking (`chunking.rs`); the release smoke driver
(`smoke_*.rs`, feature `production-smoke-driver`).

Must not own: sync semantics. The protocol, subjects, conflict arbitration, `SyncClient`,
`SyncServer`, transport and the ledger table contract are implemented in the engine and
re-exported from `lib.rs`. **Never add a mirror module here** — extend the engine and re-export.
`src/transport/` is not compiled (`lib.rs` re-exports the engine's `transport`); its `iroh.rs` and
`large_request_staging.rs` are byte-identical mirrors of the engine's files, kept for the
public-surface audit, and are never edited on their own. No behavior configuration comes from environment
aliases; a behavior reaches the binary as a flag.

## Seams

- **Below:** `contextdb-engine` (everything sync), `contextdb-core` (`Error`, `TenantId`, `Value`).
- **Above:** `contextdb-cli` and embedding hosts link the library; operators run the binary.
- **Features:** `iroh` (default) is the production transport and blob plane; `test-seams` and
  `in-process-test-seams` enable the in-process broker for tests (the crate dev-depends on itself
  with `test-seams`).

## Invariants and their guards

| Invariant | Guard (file → test) |
|---|---|
| Sync logic stays behind the engine's transport and protocol seams | `tests/transport_boundary_tests.rs` → `sync_logic_stays_behind_the_transport_and_protocol_seams` |
| `src/transport/iroh.rs` and `large_request_staging.rs` are byte-identical to the engine's | `crates/contextdb-engine/tests/sync_source_mirror_tests.rs` → `server_sync_sources_are_exact_engine_audit_mirrors` |
| Protocol 7 wire bytes do not drift | `tests/protocol_wire_format_freeze_tests.rs` → `protocol_seven_push_and_pull_wire_bytes_are_frozen` |
| A peer on another protocol is refused, moving no rows and advancing no watermark | `tests/protocol_version_bump_tests.rs` → `a_version_mismatched_peer_is_refused_on_push_moving_no_rows_and_advancing_no_watermark` |
| The ALPN stays `contextdb.sync.v6` | `tests/iroh_transport_tests.rs` → `sync_alpn_is_frozen_at_v6` |
| Each table's declared conflict policy decides a resend | `tests/conflict_policy_honored_tests.rs` → `a_declared_keep_latest_table_takes_the_latest_value_on_a_resend` |
| A pull right after a push never echoes the edge's own write back as a conflict | `tests/self_echo_pull_after_push_tests.rs` → `pull_right_after_push_does_not_self_echo_as_a_conflict` |
| Production sync refuses a transport without a node identity | `tests/authenticated_sync_contract_tests.rs` → `production_sync_refuses_identityless_transport` |
| A usage error creates no database and no identity | `tests/usage_error_no_side_effects_tests.rs` → `usage_error_before_ticket_flag_check_does_not_create_the_database`; `tests/server_resource_policy_contract_tests.rs` → `every_invalid_resource_value_is_a_usage_error_before_database_open` |
| Aliases cannot supply behavior configuration | `tests/server_behavior_alias_contract_tests.rs` → `tenant_and_sync_aliases_cannot_supply_behavior_configuration` |
| No binary exits with a bare integer | `tests/binary_exit_code_contract.rs` → `no_binary_exits_with_a_bare_integer_literal` |
| A vector update syncs with its owner row before delete arbitration | `tests/established_owner_vector_update_sync_tests.rs` → `established_owner_vector_update_syncs_before_delete_arbitration` |
| A pushed vector schema capability the receiving peer lacks is held back per table, never silently applied and never a wholesale refusal of ordinary tables | `crates/contextdb-engine/tests/vector_schema_mixed_version_push_compatibility_contract.rs` → `newer_edge_keeps_ordinary_tables_flowing_then_resumes_the_held_table_after_hub_upgrade`, `a_newer_edge_holds_authored_history_until_the_hub_can_replay_it` (siblings: `vector_schema_mixed_version_recovery_cursor_contract.rs`, `vector_schema_mixed_version_compatibility_contract.rs`, `vector_schema_mixed_version_erasure_frontier_contract.rs`, `vector_schema_mixed_version_restart_contract.rs`) — capability holdback logic (`SchemaSyncCapability`, `SchemaSyncHoldback`) lives in the engine's `sync_server.rs`; these tests drive it through `contextdb-server`'s `SyncClient`/`SyncServer`/`protocol`, so a new server-side refusal test for a schema-capability change belongs beside them, not beside `protocol_version_bump_tests.rs` (that guards protocol *version* mismatch, a different refusal). |
| Exactly one of two racing workers wins a claim | `tests/work_ledger_tests.rs` → `two_workers_race_exactly_one_wins_and_one_result_exists` |

## Where a change lives

| Change | Where it lands |
|---|---|
| Wire vocabulary, request kinds, arbitration, pull cursors | Engine crate (`protocol.rs`, `subjects.rs`, `sync_server.rs`, `sync_client.rs`); re-exported here |
| A `contextdb-server` flag or exit code | `main.rs`, `exit_codes.rs`, `owner_read_options.rs` |
| When an edge pushes automatically | `sync_plugin.rs` |
| Claim-by-push, worker polling, lease stand-down | `work_ledger.rs`; the ledger tables themselves are the engine's `work_ledger` |
| A transport or staging change | The engine's `transport/`; then refresh the mirror files here byte for byte |

## Fast tests

```bash
cargo test -p contextdb-server --test transport_boundary_tests --test protocol_version_bump_tests
cargo test -p contextdb-engine --test sync_source_mirror_tests
```
