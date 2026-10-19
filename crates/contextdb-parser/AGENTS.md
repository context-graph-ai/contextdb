# contextdb-parser — Agent Rules

Repo-wide rules are in the [root `AGENTS.md`](../../AGENTS.md). The grammar a user sees is
documented in [`docs/query-language.md`](../../docs/query-language.md).

**Purpose:** turn one SQL string into one typed `Statement` — SQL, `GRAPH_TABLE ... MATCH`, and the
vector, sync, custody and event extensions — or refuse it with a typed `Error`, never a panic.

## Owns / must not own

Owns `src/grammar.pest` (the pest grammar), `src/ast.rs` (every `Statement` and `Expr` shape),
`src/parser.rs` (`parse()`: raw-text pre-checks, then pest, then AST builders), and
`src/classification.rs` (`statement_effect()` — whether a statement needs a writable store, and
`select_contains_vector_similarity`). The pre-checks refuse unsupported features by name before
pest runs: `CREATE PROCEDURE/FUNCTION`, `WITH RECURSIVE`, full-text `MATCH`;
`validate_select_body` then refuses a vector ordering without `LIMIT`. A window parses only as
`ROW_NUMBER() OVER ([PARTITION BY cols] ORDER BY cols [ASC|DESC])` into `Expr::RowNumber`;
`build_window_call` refuses every other window form by name with
`WindowFunctionNotSupported { form }`. A statement naming
`GROUP BY` parses only in one shape (`is_grouped_count`): `COUNT(*)` and grouped plain columns
from one table, the WHERE clause only `col = literal|parameter` / `col IS NULL` conjuncts, no
CTE, join, `DISTINCT`, `ORDER BY`, `LIMIT` or vector clause. Every other statement naming it —
`HAVING`, `SUM`/`COUNT(col)` per group, a subquery or CTE body — keeps the exact
`ParseError("GROUP BY is not supported")`. Whether the table maintains a count over those columns
is the engine's decision.

The vector declaration grammar lives here, as syntax only. On a `VECTOR(n)` column, after
`WITH (quantization = ...)`, the clauses come in this fixed order, each at most once:
`PARTITION_KEY (col, ...)`, `MAX_PARTITIONS n`, `SEARCH_MODE AUTO|EXACT|INDEXED`,
`AUTO_INDEX_AT n`, `HNSW (M = v, EF_CONSTRUCTION = v, EF_SEARCH = v)` (any value may be
`DEFAULT`), `CONSOLIDATION NONE | (CHANGE_PERCENT = v, TOMBSTONE_PERCENT = v)`. Online changes are
`ALTER TABLE t ALTER COLUMN c SET MAX_PARTITIONS | SEARCH_MODE | AUTO_INDEX_AT | HNSW (...)|DEFAULT
| CONSOLIDATION ...|DEFAULT`. Queries take `ORDER BY col <=> q [USE VECTOR mode] [USE RANK name]
LIMIT k`. Store-level forms: `SET/SHOW MAINTENANCE_POLL_INTERVAL` (`MILLISECONDS|SECONDS`),
`SHOW VECTOR_PARTITIONS [FOR table.column] [LIMIT n [OFFSET m]]`, `SHOW VECTOR_INDEXES`,
and read-classified `SHOW CONTENT_SIZE` (the engine defines its file-backed scope).

Must not own: meaning. Whether `MAX_PARTITIONS` without `PARTITION_KEY` is legal, whether a column
exists, whether a mode fits the column — those are the engine's semantic validation. Plan choice
is the planner's. Keep the parser schema-blind.

## Seams

Below: `contextdb-core` (`Error`, `Result`, shared enums). Entry points: `parse(&str)`,
`statement_effect(&Statement)`. Above: `contextdb-planner` (every statement), `contextdb-engine`
(executes the AST and routes reads by `statement_effect`), `contextdb-cli` (re-parses a statement
to register `:memory:` session callbacks).

## Invariants and their guards

| Invariant | Guard |
|---|---|
| Multi-byte UTF-8 anywhere in the input never panics `parse()` | `tests/utf8_multibyte_boundary_tests.rs::parse_never_panics_across_a_generated_utf8_width_and_offset_matrix` and its sixteen siblings |
| Every statement is explicitly Read or Write (the match has no catch-all) | `tests/statement_effect_contract.rs::statement_effect_marks_only_the_inspection_variants_as_reads` |
| Vector clauses parse on `CREATE TABLE` and `ADD COLUMN` with defaults preserved | `tests/vector_partition_syntax_contract.rs::create_table_accepts_defaulted_and_explicit_partition_declarations`, `::vector_policy_spellings_preserve_values_and_defaults_on_create_add_and_alter` |
| Reordered, repeated, empty or unknown vector clauses are refused | `tests/vector_partition_syntax_contract.rs::vector_declarations_and_queries_reject_reordered_or_repeated_clauses` |
| `MAX_PARTITIONS` without a key is left to semantic validation | `tests/vector_partition_syntax_contract.rs::max_partitions_without_a_partition_key_reaches_semantic_validation` |
| `ALTER ... SET MAX_PARTITIONS / SEARCH_MODE` parse | `tests/vector_partition_syntax_contract.rs::alter_column_accepts_online_partition_limit_and_search_mode_changes` |
| `USE VECTOR` follows the vector ordering and precedes `USE RANK` and `LIMIT` | `tests/vector_partition_syntax_contract.rs::vector_search_override_follows_vector_ordering_and_precedes_rank_and_limit` |
| `SHOW VECTOR_PARTITIONS FOR` needs both table and column | `tests/vector_partition_syntax_contract.rs::partition_inspection_requires_both_table_and_column_after_for` |
| Vector-similarity detection follows nested queries and ignores string text | `tests/vector_partition_syntax_contract.rs::shared_vector_query_detection_follows_nested_queries_and_ignores_text` |
| A vector ordering without `LIMIT` is refused | `tests/parser_tests.rs::rejection_unbounded_vector_search` |
| Unsupported SQL is refused by name | `tests/parser_tests.rs::rejection_recursive_cte`, `::rejection_window_functions`, `::rejection_stored_procs`, `::rejection_full_text_match_operator` |
| A `GROUP BY` outside the grouped `COUNT(*)` shape keeps the exact established refusal before planning | `crates/contextdb-engine/tests/read/read_session_application_contract.rs::legacy_execute_preserves_the_exact_parse_error_before_planning_or_pull` |
| `PURGE` is its own statement, not a `DELETE` | `tests/purge_parser_contract_tests.rs::purge_from_where_parses_as_distinct_statement` |
| The removed conflict-policy statements stay parse errors | `tests/legacy_conflict_policy_ddl_tests.rs::legacy_global_set_sync_conflict_policy_is_a_parse_error` |
| A `WRITE` substring in a comment or quoted label does not split a scope-label list | `tests/scope_label_write_keyword_word_boundary_tests.rs::comment_containing_write_substring_inside_read_list_does_not_corrupt_labels` |
| `SET/SHOW MAINTENANCE_POLL_INTERVAL` declare and read back through the engine | `crates/contextdb-engine/tests/vector/vector_partition_declaration_contract.rs::consolidation_and_maintenance_poll_declarations_round_trip_and_reset` |

**Char-boundary rule.** Never slice `&input[a..b]` on a window whose end you computed
(`idx + token.len()` need not be a char boundary). Use `input.get(a..b)`, which returns `None`
there — and a window ending mid-character can never equal an ASCII token. The shipped pattern is
`contains_token_outside_strings`; `is_word_boundary` may read a byte because it bounds-checks
first and only asks whether the byte is ASCII-alphanumeric or `_`. A new raw-text scanner joins
the generated matrix in `utf8_multibyte_boundary_tests.rs`; never delete a case from it.

## Where a change lives

| Change | Where it lands |
|---|---|
| New statement | Rule in `grammar.pest`, variant in `ast.rs`, builder arm in `parse()`, an explicit arm in `statement_effect` (Read only if it changes nothing), then planner and engine. |
| New vector clause | Its slot in the ordered clause list in `grammar.pest`, field on the AST column/alter shape, rejection cases in `vector_partition_syntax_contract.rs`; semantic checks go to the engine. |
| New refused SQL feature | A pre-check in `parse()` returning a typed `Error`, boundary-safe. |
| New scalar function | Nothing here — function calls parse generically; see the root guide. |
| Read/write statement classification or expression shape | `classification.rs` and `ast.rs`; `parser.rs` builds the AST from `grammar.pest`. |

## Fast test

```bash
cargo test -p contextdb-parser
```
