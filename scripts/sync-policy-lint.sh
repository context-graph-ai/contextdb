#!/usr/bin/env bash
# Sync policy-declaration linter (R13 script for the `sync` skill). Given a store,
# lists every table lacking an explicit SYNC CONFLICT declaration, and flags any
# table where a DELETE would strand on its originating edge: a table that both
# DELIVERS rows (SYNC TWO WAY, the default, or SYNC PUSH ONLY) and arbitrates
# KEEP FIRST (the default when no `SYNC CONFLICT` clause is written) will never
# let a delete propagate off the machine that issued it. See the `sync` skill's
# "a delete that stays deleted" recipe for the fix (declare SYNC CONFLICT KEEP
# LATEST, or SYNC OFF if the table should never leave this machine).
#
# Usage:
#   CONTEXTDB_CLI=/path/to/contextdb scripts/sync-policy-lint.sh <db-path>
#
# Exit codes:
#   0  every table's declared (or defaulted) policy is delete-safe — no findings
#   1  at least one table would strand a delete — findings printed to stdout
#   2  usage error
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

# Inspect a COPY, never the original — same read-write-peek rule as the health
# check script.
peek_dir="$(mktemp -d)"
trap 'rm -rf "$peek_dir"' EXIT
peek_path="$peek_dir/peek.db"
cp "$db_path" "$peek_path"

mapfile -t tables < <(printf '.tables\n' | "$cli" "$peek_path" --json | jq -r '.tables[]')

if [[ "${#tables[@]}" -eq 0 ]]; then
  printf 'no tables — nothing to lint\n'
  exit 0
fi

findings=0
for t in "${tables[@]}"; do
  schema_json="$(printf '.schema %s\n' "$t" | "$cli" "$peek_path" --json)"
  # Absent key == the engine's own documented default.
  policy="$(printf '%s' "$schema_json" | jq -r '.conflict_policy // "keep_first"')"
  direction="$(printf '%s' "$schema_json" | jq -r '.sync_direction // "two_way"')"
  explicit_policy="$(printf '%s' "$schema_json" | jq -r 'has("conflict_policy")')"

  if [[ "$explicit_policy" == "false" ]]; then
    printf 'INFO %-32s no explicit SYNC CONFLICT — defaults to KEEP FIRST\n' "$t"
  fi

  delivers=false
  case "$direction" in
    two_way|push_only) delivers=true ;;
  esac

  if [[ "$delivers" == "true" && "$policy" == "keep_first" ]]; then
    printf 'WARN %-32s KEEP FIRST + %s: a DELETE on this table will NOT propagate off the edge that issued it (it will report skipped, not applied, on every peer). Declare SYNC CONFLICT KEEP LATEST if deletes must sync, or SYNC OFF if the table should never leave this machine.\n' \
      "$t" "$direction"
    findings=1
  fi
done

exit "$findings"
