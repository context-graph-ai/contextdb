#!/usr/bin/env bash
# Deterministic store health/verify probe (R13 script for the `using-contextdb` and
# `sync` skills). Checks that a store diagnoses clean, that `.tables` lists its
# tables, and that every table's row count is readable — the three things "is this
# store OK" actually means. Plain `contextdb <path>` (no `--write`) is a bounded
# read session by default, so every check here reads the store directly; nothing
# is copied first.
#
# Usage:
#   CONTEXTDB_CLI=/path/to/contextdb scripts/store-health-check.sh <db-path>
#
# Exit codes:
#   0  store diagnoses clean AND every table's row count was read successfully
#   1  `contextdb diagnose` reported a problem, OR a table's row count could not
#      be read (both printed to stderr; a wedged store never reaches step 2)
#   2  usage error — missing argument or CONTEXTDB_CLI not runnable
set -euo pipefail

cli="${CONTEXTDB_CLI:-contextdb}"
db_path="${1:-}"

if [[ -z "$db_path" ]]; then
  printf 'usage: CONTEXTDB_CLI=<path> %s <db-path>\n' "$0" >&2
  exit 2
fi
if ! command -v "$cli" >/dev/null 2>&1 && [[ ! -x "$cli" ]]; then
  printf 'FAIL contextdb binary not runnable: %s (set CONTEXTDB_CLI)\n' "$cli" >&2
  exit 2
fi
if [[ ! -e "$db_path" ]]; then
  printf 'FAIL no such store: %s\n' "$db_path" >&2
  exit 2
fi

# Step 1 — non-mutating format/schema diagnosis.
if ! diagnose_out="$("$cli" diagnose "$db_path" 2>&1)"; then
  printf 'FAIL diagnose reported a problem:\n%s\n' "$diagnose_out" >&2
  exit 1
fi
printf 'OK   diagnose: %s\n' "$diagnose_out"

# Step 2 — read the store directly. A plain `contextdb <path>` open (no
# `--write`) is a bounded read session: it never creates or mutates the store,
# so there is no peek copy to make here.
tables_json="$(printf '.tables\n' | "$cli" "$db_path" --json)"
if [[ -z "$tables_json" ]]; then
  printf 'FAIL .tables produced no output\n' >&2
  exit 1
fi
mapfile -t tables < <(printf '%s' "$tables_json" | jq -r '.tables.items[]')
printf 'OK   .tables: %d table(s)\n' "${#tables[@]}"

status=0
for t in "${tables[@]}"; do
  if ! count_json="$(printf 'SELECT COUNT(*) AS n FROM %s;\n' "$t" | "$cli" "$db_path" --json)"; then
    printf 'FAIL row count unreadable for table %s (see the contextdb error above)\n' "$t" >&2
    status=1
    continue
  fi
  n="$(printf '%s' "$count_json" | jq -r '.result.rows[0].n // empty')"
  if [[ -z "$n" ]]; then
    printf 'FAIL row count unreadable for table %s: %s\n' "$t" "$count_json" >&2
    status=1
  else
    printf 'OK   %-32s %s row(s)\n' "$t" "$n"
  fi
done

exit "$status"
