#!/usr/bin/env bash
# Deterministic store health/verify probe (R13 script for the `using-contextdb` and
# `sync` skills). Checks that a store opens, that `.tables` lists its tables, and
# that every table's row count is readable — the three things "is this store OK"
# actually means. Never opens the caller's original file for the inspection half:
# it copies first, per the read-write-peek safety rule (there is no read-only way
# to open a store except `contextdb repair`).
#
# Usage:
#   CONTEXTDB_CLI=/path/to/contextdb scripts/store-health-check.sh <db-path>
#
# Exit codes:
#   0  store repairs clean AND every table's row count was read successfully
#   1  `contextdb repair` reported a problem, OR a table's row count could not
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

# Step 1 — genuinely read-only diagnosis. Never skip this for a store you do not
# own: `repair` never modifies the file, plain `contextdb <path>` always does.
if ! repair_out="$("$cli" repair "$db_path" 2>&1)"; then
  printf 'FAIL repair reported a problem:\n%s\n' "$repair_out" >&2
  exit 1
fi
printf 'OK   repair: %s\n' "$repair_out"

# Step 2 — inspect a COPY, never the original, per the AGENTS.md read-write-peek
# rule (a no-op meta-command still rewrites the file's bytes).
peek_dir="$(mktemp -d)"
trap 'rm -rf "$peek_dir"' EXIT
peek_path="$peek_dir/peek.db"
cp "$db_path" "$peek_path"

tables_json="$(printf '.tables\n' | "$cli" "$peek_path" --json)"
if [[ -z "$tables_json" ]]; then
  printf 'FAIL .tables produced no output against the peek copy\n' >&2
  exit 1
fi
mapfile -t tables < <(printf '%s' "$tables_json" | jq -r '.tables[]')
printf 'OK   .tables: %d table(s)\n' "${#tables[@]}"

status=0
for t in "${tables[@]}"; do
  count_json="$(printf 'SELECT COUNT(*) AS n FROM %s;\n' "$t" | "$cli" "$peek_path" --json 2>&1)" || {
    printf 'FAIL row count unreadable for table %s: %s\n' "$t" "$count_json" >&2
    status=1
    continue
  }
  n="$(printf '%s' "$count_json" | jq -r '.[0].n // "unreadable"')"
  if [[ "$n" == "unreadable" ]]; then
    printf 'FAIL row count unreadable for table %s: %s\n' "$t" "$count_json" >&2
    status=1
  else
    printf 'OK   %-32s %s row(s)\n' "$t" "$n"
  fi
done

exit "$status"
