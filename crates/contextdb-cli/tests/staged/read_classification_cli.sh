#!/usr/bin/env bash
#
# A production-binary integration proof for command classification and
# discovery. It runs outside Cargo because it invokes a supplied, real
# `contextdb` binary and verifies its end-to-end command-line behavior.

set -euo pipefail

contextdb_bin="${CONTEXTDB_CLI:?set CONTEXTDB_CLI to the production contextdb binary}"
if [[ ! -x "$contextdb_bin" ]]; then
    printf 'FAIL contextdb binary is not executable: %s\n' "$contextdb_bin" >&2
    exit 2
fi
if ! command -v jq >/dev/null 2>&1; then
    printf 'FAIL jq is required to verify exact JSON results\n' >&2
    exit 2
fi
if ! command -v rg >/dev/null 2>&1; then
    printf 'FAIL rg is required to verify command-source dataflow\n' >&2
    exit 2
fi

if command -v sha256sum >/dev/null 2>&1; then
    hash_tool="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
    hash_tool="shasum"
else
    printf 'FAIL sha256sum or shasum is required to manifest store contents\n' >&2
    exit 2
fi

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT
store_dir="$work_dir/store"
output_dir="$work_dir/output"
# A write-classified meta-command is refused for the SESSION it is in before its
# argument is ever looked at, so the honest place to pin "this argument is a usage
# error" is a writing session. It gets its own store, outside the manifested fixture
# directory, so the fixture the read journeys share is never opened for writing.
usage_probe_dir="$work_dir/usage-probe"
mkdir -p "$store_dir" "$output_dir" "$usage_probe_dir"
database="$store_dir/read-classification.db"
usage_probe_database="$usage_probe_dir/usage-probe.db"
# mktemp supplies a fresh, non-clock-derived nonce, so a fixture-keyed read
# implementation cannot pass by recognizing one permanent table or value set.
fixture_nonce="${work_dir##*/}"
fixture_nonce="${fixture_nonce//[^[:alnum:]]/}"
read_select_table="read_select_${fixture_nonce}"
read_policy_table="read_policy_${fixture_nonce}"
read_vector_table="read_vector_${fixture_nonce}"
read_entries_table="read_entries_${fixture_nonce}"
read_maintenance_table="read_maintenance_${fixture_nonce}"
read_schedule="read_schedule_${fixture_nonce}"
read_trigger="read_trigger_${fixture_nonce}"
read_event="read_event_${fixture_nonce}"
read_sink="read_sink_${fixture_nonce}"
read_route="read_route_${fixture_nonce}"
fixture_seed="$(printf '%s' "$fixture_nonce" | cksum | awk '{print $1}')"
memory_limit_mebibytes=$((64 + fixture_seed % 31))
disk_limit_mebibytes=$((128 + fixture_seed % 47))
memory_limit_bytes=$((memory_limit_mebibytes * 1024 * 1024))
disk_limit_bytes=$((disk_limit_mebibytes * 1024 * 1024))
read_select_note="select-proof-${fixture_nonce}"
read_entries_first_note="safekept-${fixture_nonce}"
read_entries_second_note="bounded-${fixture_nonce}"
maintenance_empty_database="$store_dir/maintenance-empty.db"
maintenance_empty_table="maintenance_empty_${fixture_nonce}"

readonly -a META_COMMAND_DECLARATIONS=(
    'StoreRead|.tables'
    'StoreRead|.schema'
    'StoreRead|.explain'
    'StoreRead|.events status'
    'StoreRead|.maintenance status'
    'StoreRead|.cursor open'
    'StoreRead|.cursor fetch'
    'StoreRead|.cursor close'
    'StoreWrite|.maintenance run'
    'StoreWrite|.maintenance compact'
    'StoreWrite|.sync push'
    'StoreWrite|.sync pull'
    'StoreWrite|.sync reconnect'
    'StoreWrite|.sync destination'
    'StoreWrite|.sync auto'
    'OwnerStatus|.owner status'
    'SessionOnly|.help'
    'SessionOnly|.trace'
    'SessionOnly|.quit'
    'SessionOnly|.exit'
    'SessionOnly|.sync status'
    'Invalid|.events'
    'Invalid|.maintenance'
    'Invalid|.cursor'
    'Invalid|.sync'
    'Invalid|.owner'
)

readonly -a OPERATIONAL_COMMAND_DECLARATIONS=(
    'Migrate|migrate'
    'Reset|reset'
    'Diagnose|diagnose'
    'Snapshot|snapshot'
    'Inspect|inspect'
    'Purge|purge'
)

# The read surface publishes notices on stderr — the read-route notice, statement
# progress, hydration — beside whatever else a command writes there. They are not
# diagnostics, so a journey that counts a refusal counts the ERROR documents and lets
# the notices past. `$docs` is that filtered list.
readonly diagnostics_only='[.[] | select(has("notice") | not)] as $docs | '

declare -a expected_read_commands=()
declare -a expected_write_commands=()
declare -a expected_session_commands=()
declare -a expected_valid_meta_commands=()
# This is an external acceptance oracle.  It must never be used as the
# implementation's help/effect source; the source-dataflow gate below requires
# all three consumer surfaces to use command_registry instead.
for declaration in "${META_COMMAND_DECLARATIONS[@]}"; do
    effect="${declaration%%|*}"
    spelling="${declaration#*|}"
    case "$effect" in
        StoreRead)
            expected_read_commands+=("$spelling")
            ;;
        StoreWrite)
            expected_write_commands+=("$spelling")
            ;;
        SessionOnly)
            expected_session_commands+=("$spelling")
            ;;
    esac
    if [[ "$effect" != 'Invalid' ]]; then
        expected_valid_meta_commands+=("$spelling")
    fi
done
readonly -a expected_read_commands
readonly -a expected_write_commands
readonly -a expected_session_commands
readonly -a expected_valid_meta_commands
readonly sync_status_message="no sync in this session — this reports the CLI session only, not the store; a live owner's sync state belongs to that owner process."

declare -A exercised_store_read_commands=()
declare -A exercised_session_commands=()

declare -a expected_operational_commands=()
for declaration in "${OPERATIONAL_COMMAND_DECLARATIONS[@]}"; do
    expected_operational_commands+=("${declaration#*|}")
done
readonly -a expected_operational_commands

readonly -a AST_READ_STATEMENTS=(
    "select|SELECT id, note FROM $read_select_table WHERE id = 101 ORDER BY id;"
    'show-memory-limit|SHOW MEMORY_LIMIT'
    'show-disk-limit|SHOW DISK_LIMIT'
    'show-sync-conflict-policy|SHOW SYNC_CONFLICT_POLICY'
    'show-vector-indexes|SHOW VECTOR_INDEXES'
)

# The third field is a same-session writable transcript.  It proves that the
# one statement later refused by a default read session is a real, unique
# mutation, rather than a duplicate DDL, unbound parameter, missing target, or
# a transaction control command that would naturally fail outside a
# transaction.
readonly -a AST_WRITE_STATEMENTS=(
    'create-table|CREATE TABLE proof_create_table (id INTEGER PRIMARY KEY)|CREATE TABLE proof_create_table (id INTEGER PRIMARY KEY)'
    'alter-table|ALTER TABLE proof_alter_table ADD COLUMN note TEXT|ALTER TABLE proof_alter_table ADD COLUMN note TEXT'
    'drop-table|DROP TABLE proof_drop_table|DROP TABLE proof_drop_table'
    'create-index|CREATE INDEX proof_create_index_by_id ON proof_create_index (id)|CREATE INDEX proof_create_index_by_id ON proof_create_index (id)'
    'drop-index|DROP INDEX proof_drop_index_by_id ON proof_drop_index|DROP INDEX proof_drop_index_by_id ON proof_drop_index'
    "insert|INSERT INTO proof_insert (id, note) VALUES (201, 'inserted')|INSERT INTO proof_insert (id, note) VALUES (201, 'inserted')"
    'purge|PURGE FROM proof_purge WHERE id = 210|PURGE FROM proof_purge WHERE id = 210'
    'delete|DELETE FROM proof_delete WHERE id = 220|DELETE FROM proof_delete WHERE id = 220'
    "update|UPDATE proof_update SET note = 'updated' WHERE id = 230|UPDATE proof_update SET note = 'updated' WHERE id = 230"
    'begin|BEGIN|BEGIN'
    $'commit|COMMIT|BEGIN\nCOMMIT'
    $'rollback|ROLLBACK|BEGIN\nROLLBACK'
    "set-memory-limit|SET MEMORY_LIMIT '33M'|SET MEMORY_LIMIT '33M'"
    "set-disk-limit|SET DISK_LIMIT '96M'|SET DISK_LIMIT '96M'"
    "create-schedule|CREATE SCHEDULE proof_create_schedule EVERY '1 hour' TX (refresh_entries)|CREATE SCHEDULE proof_create_schedule EVERY '1 hour' TX (refresh_entries)"
    'drop-schedule|DROP SCHEDULE proof_drop_schedule|DROP SCHEDULE proof_drop_schedule'
    'create-trigger|CREATE TRIGGER proof_create_trigger ON entries WHEN INSERT|CREATE TRIGGER proof_create_trigger ON entries WHEN INSERT'
    'drop-trigger|DROP TRIGGER proof_drop_trigger|DROP TRIGGER proof_drop_trigger'
    'create-event-type|CREATE EVENT TYPE proof_create_event WHEN INSERT ON entries|CREATE EVENT TYPE proof_create_event WHEN INSERT ON entries'
    'create-sink|CREATE SINK proof_create_sink TYPE callback|CREATE SINK proof_create_sink TYPE callback'
    'create-route|CREATE ROUTE proof_create_route EVENT proof_seed_event TO proof_seed_sink|CREATE ROUTE proof_create_route EVENT proof_seed_event TO proof_seed_sink'
    'drop-route|DROP ROUTE proof_drop_route|DROP ROUTE proof_drop_route'
)

fail_plain() {
    printf 'FAIL %s\n' "$1" >&2
    exit 1
}

if (( ${#AST_READ_STATEMENTS[@]} != 5 || ${#AST_WRITE_STATEMENTS[@]} != 22 )); then
    fail_plain 'the production matrix must contain all 27 parser Statement variants'
fi

file_digest() {
    local path="$1"
    if [[ "$hash_tool" == "sha256sum" ]]; then
        sha256sum "$path" | awk '{print $1}'
    else
        shasum -a 256 "$path" | awk '{print $1}'
    fi
}

capture_store_manifest() {
    local destination="$1"
    (
        cd "$store_dir"
        while IFS= read -r path; do
            if [[ -L "$path" ]]; then
                printf 'link %s -> %s\n' "$path" "$(readlink "$path")"
            elif [[ -d "$path" ]]; then
                printf 'directory %s\n' "$path"
            elif [[ -f "$path" ]]; then
                printf 'file %s %s\n' "$path" "$(file_digest "$path")"
            else
                printf 'other %s\n' "$path"
            fi
        done < <(find . -mindepth 1 -print | LC_ALL=C sort)
    ) >"$destination"
}

fail_journey() {
    local journey="$1"
    local message="$2"
    local stdout="${3:-}"
    local stderr="${4:-}"
    local before_manifest="${5:-}"
    local after_manifest="${6:-}"

    printf 'FAIL %s: %s\n' "$journey" "$message" >&2
    if [[ -n "$stdout" && -f "$stdout" ]]; then
        printf '%s\n' "--- $journey stdout ---" >&2
        sed -n '1,400p' "$stdout" >&2
    fi
    if [[ -n "$stderr" && -f "$stderr" ]]; then
        printf '%s\n' "--- $journey stderr ---" >&2
        sed -n '1,400p' "$stderr" >&2
    fi
    if [[ -n "$before_manifest" && -f "$before_manifest" ]]; then
        printf '%s\n' "--- $journey store manifest diff ---" >&2
        diff -u "$before_manifest" "$after_manifest" >&2 || true
    fi
    exit 1
}

assert_store_unchanged() {
    local journey="$1"
    local stdout="$2"
    local stderr="$3"
    local before_manifest="$4"
    local after_manifest="$5"
    if ! cmp -s "$before_manifest" "$after_manifest"; then
        fail_journey \
            "$journey" \
            'the store roster or file contents changed' \
            "$stdout" \
            "$stderr" \
            "$before_manifest" \
            "$after_manifest"
    fi
}

expect_read_success() {
    local journey="$1"
    local input="$2"
    local target_database="${3:-$database}"
    local stdout="$output_dir/$journey.stdout"
    local stderr="$output_dir/$journey.stderr"
    local before_manifest="$output_dir/$journey.before.manifest"
    local after_manifest="$output_dir/$journey.after.manifest"
    local status

    capture_store_manifest "$before_manifest"
    if printf '%s\n' "$input" | "$contextdb_bin" "$target_database" --json >"$stdout" 2>"$stderr"; then
        status=0
    else
        status=$?
    fi
    capture_store_manifest "$after_manifest"
    assert_store_unchanged "$journey" "$stdout" "$stderr" "$before_manifest" "$after_manifest"
    if (( status != 0 )); then
        fail_journey \
            "$journey" \
            "declared read exited $status instead of succeeding" \
            "$stdout" \
            "$stderr" \
            "$before_manifest" \
            "$after_manifest"
    fi
}

expect_write_refusal() {
    local journey="$1"
    local input="$2"
    local stdout="$output_dir/$journey.stdout"
    local stderr="$output_dir/$journey.stderr"
    local before_manifest="$output_dir/$journey.before.manifest"
    local after_manifest="$output_dir/$journey.after.manifest"
    local status

    capture_store_manifest "$before_manifest"
    if printf '%s\n' "$input" | "$contextdb_bin" "$database" --json >"$stdout" 2>"$stderr"; then
        status=0
    else
        status=$?
    fi
    capture_store_manifest "$after_manifest"
    assert_store_unchanged "$journey" "$stdout" "$stderr" "$before_manifest" "$after_manifest"
    if (( status == 0 )); then
        fail_journey \
            "$journey" \
            'write-capable statement or command succeeded in the default read session' \
            "$stdout" \
            "$stderr" \
            "$before_manifest" \
            "$after_manifest"
    fi
    if [[ -s "$stdout" ]]; then
        fail_journey \
            "$journey" \
            'a refusal published a partial result on stdout' \
            "$stdout" \
            "$stderr" \
            "$before_manifest" \
            "$after_manifest"
    fi
    if ! jq -e -s "$diagnostics_only"'
        ($docs | length) == 1 and
        ($docs[0] | keys) == ["error"] and
        $docs[0].error.class == "sql" and
        $docs[0].error.detail.kind == "write_requires_flag" and
        (($docs[0].error.detail.remedy? // $docs[0].error.message) | tostring | contains("--write"))
    ' "$stderr" >/dev/null; then
        fail_journey \
            "$journey" \
            'refusal must be exactly one SQL write_requires_flag error whose remedy names --write' \
            "$stdout" \
            "$stderr" \
            "$before_manifest" \
            "$after_manifest"
    fi
}

# Store manifests alone cannot prove that a StoreWrite meta-command was stopped
# before dispatch: sync configuration can alter only session state, and a
# maintenance action can be a no-op for this fixture.  Keep the refusal inside
# one process with an observable command both before and after it.  A later
# dispatcher that runs first therefore fails even when store bytes happen not
# to change.
expect_dot_write_refusal_before_dispatch() {
    local journey="$1"
    local command="$2"
    local sentinel="$3"
    local before_input
    local after_input
    local stdout="$output_dir/$journey.stdout"
    local stderr="$output_dir/$journey.stderr"
    local before_manifest="$output_dir/$journey.before.manifest"
    local after_manifest="$output_dir/$journey.after.manifest"
    local status

    case "$sentinel" in
        sync)
            before_input='.sync status'
            after_input='.sync status'
            ;;
        maintenance)
            before_input='.maintenance status'
            after_input='.maintenance status'
            ;;
        *) fail_plain "unknown pre-dispatch sentinel $sentinel" ;;
    esac

    capture_store_manifest "$before_manifest"
    if printf '%s\n' "$before_input" "$command" "$after_input" \
        | "$contextdb_bin" "$database" --json >"$stdout" 2>"$stderr"; then
        status=0
    else
        status=$?
    fi
    capture_store_manifest "$after_manifest"
    assert_store_unchanged "$journey" "$stdout" "$stderr" "$before_manifest" "$after_manifest"
    if (( status == 0 )); then
        fail_journey \
            "$journey" \
            'StoreWrite command succeeded instead of being refused before dispatch' \
            "$stdout" "$stderr" "$before_manifest" "$after_manifest"
    fi
    if ! jq -e -s "$diagnostics_only"'
        ($docs | length) == 1 and
        ($docs[0] | keys) == ["error"] and
        $docs[0].error.class == "sql" and
        $docs[0].error.detail.kind == "write_requires_flag" and
        (($docs[0].error.detail.remedy? // $docs[0].error.message) | tostring | contains("--write"))
    ' "$stderr" >/dev/null; then
        fail_journey \
            "$journey" \
            'the refusal must be the sole SQL write_requires_flag diagnostic' \
            "$stdout" "$stderr" "$before_manifest" "$after_manifest"
    fi
    case "$sentinel" in
        sync)
            if ! jq -e -s --arg expected_message "$sync_status_message" '
                length == 2 and
                .[0] == {"sync_status": {"message": $expected_message}} and
                .[1] == .[0]
            ' "$stdout" >/dev/null; then
                fail_journey \
                    "$journey" \
                    'the sync session state differed after its refused StoreWrite command' \
                    "$stdout" "$stderr" "$before_manifest" "$after_manifest"
            fi
            ;;
        maintenance)
            if ! jq -e -s '
                length == 2 and
                .[0] == .[1] and
                (.[0] | keys) == ["maintenance"] and
                (.[0].maintenance | type == "object")
            ' "$stdout" >/dev/null; then
                fail_journey \
                    "$journey" \
                    'the maintenance sentinel differed after its refused StoreWrite command' \
                    "$stdout" "$stderr" "$before_manifest" "$after_manifest"
            fi
            ;;
    esac
}

validate_sql_write_is_executable() {
    local journey="$1"
    local writable_transcript="$2"
    local validation_database="$work_dir/validation-$journey.db"
    local validation_stdout="$output_dir/validation-$journey.stdout"
    local validation_stderr="$output_dir/validation-$journey.stderr"

    if ! printf '%s\n' \
        'CREATE TABLE entries (id INTEGER PRIMARY KEY, note TEXT);' \
        'CREATE TABLE proof_alter_table (id INTEGER PRIMARY KEY);' \
        'CREATE TABLE proof_drop_table (id INTEGER PRIMARY KEY);' \
        'CREATE TABLE proof_create_index (id INTEGER PRIMARY KEY);' \
        'CREATE TABLE proof_drop_index (id INTEGER PRIMARY KEY);' \
        'CREATE INDEX proof_drop_index_by_id ON proof_drop_index (id);' \
        'CREATE TABLE proof_insert (id INTEGER PRIMARY KEY, note TEXT);' \
        'CREATE TABLE proof_purge (id INTEGER PRIMARY KEY, note TEXT);' \
        "INSERT INTO proof_purge VALUES (210, 'purge-target');" \
        'CREATE TABLE proof_delete (id INTEGER PRIMARY KEY, note TEXT);' \
        "INSERT INTO proof_delete VALUES (220, 'delete-target');" \
        'CREATE TABLE proof_update (id INTEGER PRIMARY KEY, note TEXT);' \
        "INSERT INTO proof_update VALUES (230, 'before-update');" \
        "CREATE SCHEDULE proof_drop_schedule EVERY '1 hour' TX (refresh_entries);" \
        'CREATE TRIGGER proof_drop_trigger ON entries WHEN INSERT;' \
        'CREATE EVENT TYPE proof_seed_event WHEN INSERT ON entries;' \
        'CREATE SINK proof_seed_sink TYPE callback;' \
        'CREATE ROUTE proof_drop_route EVENT proof_seed_event TO proof_seed_sink;' \
        "$writable_transcript" \
        | "$contextdb_bin" "$validation_database" --write --json \
            >"$validation_stdout" 2>"$validation_stderr"; then
        fail_journey \
            "$journey" \
            'the refusal target is not a valid same-session writable mutation' \
            "$validation_stdout" \
            "$validation_stderr"
    fi
}

expect_removed_meta_refusal() {
    local journey="$1"
    local input="$2"
    local stdout="$output_dir/$journey.stdout"
    local stderr="$output_dir/$journey.stderr"
    local before_manifest="$output_dir/$journey.before.manifest"
    local after_manifest="$output_dir/$journey.after.manifest"
    local status

    capture_store_manifest "$before_manifest"
    if printf '%s\n' "$input" | "$contextdb_bin" "$database" --json >"$stdout" 2>"$stderr"; then
        status=0
    else
        status=$?
    fi
    capture_store_manifest "$after_manifest"
    assert_store_unchanged "$journey" "$stdout" "$stderr" "$before_manifest" "$after_manifest"
    if (( status == 0 )); then
        fail_journey \
            "$journey" \
            "removed meta-command $input was accepted" \
            "$stdout" \
            "$stderr" \
            "$before_manifest" \
            "$after_manifest"
    fi
    if ! grep -F -- "$input" "$stderr" >/dev/null; then
        fail_journey \
            "$journey" \
            "refusal did not identify removed spelling $input" \
            "$stdout" \
            "$stderr" \
            "$before_manifest" \
            "$after_manifest"
    fi
}

expect_session_only_success() {
    local journey="$1"
    local input="$2"
    local stdout="$output_dir/$journey.stdout"
    local stderr="$output_dir/$journey.stderr"
    local before_manifest="$output_dir/$journey.before.manifest"
    local after_manifest="$output_dir/$journey.after.manifest"
    local status

    capture_store_manifest "$before_manifest"
    if printf '%s\n' "$input" \
        | "$contextdb_bin" "$database" --json >"$stdout" 2>"$stderr"; then
        status=0
    else
        status=$?
    fi
    capture_store_manifest "$after_manifest"
    assert_store_unchanged "$journey" "$stdout" "$stderr" "$before_manifest" "$after_manifest"
    if (( status != 0 )); then
        fail_journey \
            "$journey" \
            "session-only command $input exited $status" \
            "$stdout" \
            "$stderr" \
            "$before_manifest" \
            "$after_manifest"
    fi
}

expect_exact_error_refusal() {
    local journey="$1"
    local input="$2"
    local expected_class="$3"
    # `write` drives the throwaway writable store, for a command whose session
    # classification would otherwise answer before its argument is read.
    local session_mode="${4:-read}"
    local stdout="$output_dir/$journey.stdout"
    local stderr="$output_dir/$journey.stderr"
    local before_manifest="$output_dir/$journey.before.manifest"
    local after_manifest="$output_dir/$journey.after.manifest"
    local status

    local -a session_args=("$database" --json)
    if [[ "$session_mode" == write ]]; then
        session_args=("$usage_probe_database" --write --json)
    fi

    capture_store_manifest "$before_manifest"
    if printf '%s\n' "$input" | "$contextdb_bin" "${session_args[@]}" >"$stdout" 2>"$stderr"; then
        status=0
    else
        status=$?
    fi
    capture_store_manifest "$after_manifest"
    assert_store_unchanged "$journey" "$stdout" "$stderr" "$before_manifest" "$after_manifest"
    if (( status == 0 )) || [[ -s "$stdout" ]] || ! jq -e -s \
        --arg expected_class "$expected_class" \
        --arg input "$input" "$diagnostics_only"'
            ($docs | length) == 1 and
            ($docs[0] | keys) == ["error"] and
            ($docs[0].error | keys) == ["class", "line", "message"] and
            $docs[0].error.class == $expected_class and
            $docs[0].error.line == 1 and
            ($docs[0].error.message | contains($input))
        ' "$stderr" >/dev/null; then
        fail_journey \
            "$journey" \
            "expected exactly one $expected_class error envelope for $input and no stdout" \
            "$stdout" \
            "$stderr" \
            "$before_manifest" \
            "$after_manifest"
    fi
}

expect_cli_refusal() {
    local journey="$1"
    local expected="$2"
    shift 2
    local stdout="$output_dir/$journey.stdout"
    local stderr="$output_dir/$journey.stderr"
    local before_manifest="$output_dir/$journey.before.manifest"
    local after_manifest="$output_dir/$journey.after.manifest"
    local status

    capture_store_manifest "$before_manifest"
    if "$contextdb_bin" "$@" </dev/null >"$stdout" 2>"$stderr"; then
        status=0
    else
        status=$?
    fi
    capture_store_manifest "$after_manifest"
    assert_store_unchanged "$journey" "$stdout" "$stderr" "$before_manifest" "$after_manifest"
    if (( status == 0 )); then
        fail_journey \
            "$journey" \
            "removed command or option $expected was accepted" \
            "$stdout" \
            "$stderr" \
            "$before_manifest" \
            "$after_manifest"
    fi
    if ! grep -F -- "$expected" "$stderr" >/dev/null; then
        fail_journey \
            "$journey" \
            "refusal did not identify $expected" \
            "$stdout" \
            "$stderr" \
            "$before_manifest" \
            "$after_manifest"
    fi
}

printf '%s\n' \
    "CREATE TABLE $read_entries_table (id INTEGER PRIMARY KEY, note TEXT);" \
    "INSERT INTO $read_entries_table VALUES (1, '$read_entries_first_note');" \
    "INSERT INTO $read_entries_table VALUES (2, '$read_entries_second_note');" \
    "CREATE INDEX ${read_entries_table}_by_id ON $read_entries_table (id);" \
    "CREATE TABLE $read_select_table (id INTEGER PRIMARY KEY, note TEXT);" \
    "INSERT INTO $read_select_table VALUES (101, '$read_select_note');" \
    "SET MEMORY_LIMIT '${memory_limit_mebibytes}M';" \
    "SET DISK_LIMIT '${disk_limit_mebibytes}M';" \
    "CREATE TABLE $read_policy_table (id INTEGER PRIMARY KEY) SYNC CONFLICT KEEP LATEST;" \
    "CREATE TABLE $read_vector_table (id INTEGER PRIMARY KEY, embedding VECTOR(3));" \
    "CREATE TABLE $read_maintenance_table (id INTEGER PRIMARY KEY) SYNC CONFLICT KEEP LATEST RETAIN 1 HOURS HISTORY CURRENT ONLY;" \
    "CREATE SCHEDULE $read_schedule EVERY '1 hour' TX (refresh_${fixture_nonce});" \
    "CREATE TRIGGER $read_trigger ON $read_entries_table WHEN INSERT;" \
    "CREATE EVENT TYPE $read_event WHEN INSERT ON $read_entries_table;" \
    "CREATE SINK $read_sink TYPE callback;" \
    "CREATE ROUTE $read_route EVENT $read_event TO $read_sink;" \
    | "$contextdb_bin" "$database" --write --json >"$output_dir/seed.stdout" 2>"$output_dir/seed.stderr" \
    || fail_journey \
        'writable-fixture-setup' \
        'fixture setup must succeed' \
        "$output_dir/seed.stdout" \
        "$output_dir/seed.stderr"

for case_row in "${AST_READ_STATEMENTS[@]}"; do
    journey="ast-read-${case_row%%|*}"
    statement="${case_row#*|}"
    expect_read_success "$journey" "$statement"
    case "$journey" in
        ast-read-select)
            read_assertion='
                length == 1 and .[0] == {
                    "result": {
                        "columns": ["id", "note"],
                        "rows": [{"id": 101, "note": $select_note}]
                    }
                }
            '
            ;;
        ast-read-show-memory-limit)
            read_assertion='
                length == 1 and
                .[0].result.columns == ["limit", "used", "available", "startup_ceiling"] and
                (.[0].result.rows | length) == 1 and
                .[0].result.rows[0].limit == $memory_limit_bytes and
                (.[0].result.rows[0].used | type == "number") and
                .[0].result.rows[0].available == ($memory_limit_bytes - .[0].result.rows[0].used) and
                .[0].result.rows[0].startup_ceiling == "none"
            '
            ;;
        ast-read-show-disk-limit)
            read_assertion='
                length == 1 and
                .[0].result.columns == ["limit", "used", "available", "startup_ceiling"] and
                (.[0].result.rows | length) == 1 and
                .[0].result.rows[0].limit == $disk_limit_bytes and
                (.[0].result.rows[0].used | type == "number") and
                .[0].result.rows[0].available == ($disk_limit_bytes - .[0].result.rows[0].used) and
                .[0].result.rows[0].startup_ceiling == "none"
            '
            ;;
        ast-read-show-sync-conflict-policy)
            read_assertion='
                length == 1 and .[0] == {
                    "result": {
                        "columns": ["policy"],
                        "rows": [
                            {"policy": "keep_first"},
                            {"policy": ($maintenance_table + "=keep_latest")},
                            {"policy": ($policy_table + "=keep_latest")}
                        ]
                    }
                }
            '
            ;;
        ast-read-show-vector-indexes)
            read_assertion='
                length == 1 and .[0] == {
                    "result": {
                        "columns": ["table", "column", "dimension", "quantization", "vector_count", "bytes"],
                        "rows": [{
                            "table": $vector_table, "column": "embedding", "dimension": 3,
                            "quantization": "F32", "vector_count": 0, "bytes": 0
                        }]
                    }
                }
            '
            ;;
    esac
    if ! jq -e -s \
        --arg maintenance_table "$read_maintenance_table" \
        --arg policy_table "$read_policy_table" \
        --arg vector_table "$read_vector_table" \
        --arg select_note "$read_select_note" \
        --argjson memory_limit_bytes "$memory_limit_bytes" \
        --argjson disk_limit_bytes "$disk_limit_bytes" \
        "$read_assertion" "$output_dir/$journey.stdout" >/dev/null; then
        fail_journey \
            "$journey" \
            'the production read did not publish its exact fixture-derived result document' \
            "$output_dir/$journey.stdout" \
            "$output_dir/$journey.stderr" \
            "$output_dir/$journey.before.manifest" \
            "$output_dir/$journey.after.manifest"
    fi
done

for case_row in "${AST_WRITE_STATEMENTS[@]}"; do
    IFS='|' read -r variant statement writable_transcript <<<"$case_row"
    journey="ast-write-$variant"
    validate_sql_write_is_executable "$journey" "$writable_transcript"
    expect_write_refusal "$journey" "$statement"
done

for command in "${expected_write_commands[@]}"; do
    case "$command" in
        '.sync destination') input='.sync destination replacement-hub' ;;
        '.sync auto') input='.sync auto on' ;;
        *) input="$command" ;;
    esac
    journey="dot-write-${command#.}"
    journey="${journey// /-}"
    case "$command" in
        '.maintenance run'|'.maintenance compact') sentinel=maintenance ;;
        *) sentinel=sync ;;
    esac
    expect_dot_write_refusal_before_dispatch "$journey" "$input" "$sentinel"
done

explain_delete_statement=".explain DELETE FROM $read_entries_table WHERE id = 1;"
explain_update_statement=".explain UPDATE $read_select_table SET note = 'would-change-${fixture_nonce}' WHERE id = 101;"
for explain_case in delete update; do
    journey="explain-$explain_case"
    if [[ "$explain_case" == delete ]]; then
        statement="$explain_delete_statement"
        expected_statement="$read_entries_table"
    else
        statement="$explain_update_statement"
        expected_statement="$read_select_table"
    fi
    expect_read_success "$journey" "$statement"
    if ! jq -e -s --arg statement "$expected_statement" '
        length == 1 and
        (.[0] | keys) == ["explain"] and
        (.[0].explain | type == "object") and
        (.[0].explain.physical_plan | type == "string" and length > 0) and
        (.[0].explain.physical_plan | ascii_downcase | contains($statement | ascii_downcase)) and
        .[0].explain.runtime_trace == false
    ' "$output_dir/$journey.stdout" >/dev/null; then
        fail_journey \
            "$journey" \
            ".explain did not publish a plan for its supplied randomized statement" \
            "$output_dir/$journey.stdout" \
            "$output_dir/$journey.stderr" \
            "$output_dir/$journey.before.manifest" \
            "$output_dir/$journey.after.manifest"
    fi
done
if cmp -s "$output_dir/explain-delete.stdout" "$output_dir/explain-update.stdout"; then
    fail_journey \
        'explain-distinct-statements' \
        '.explain returned one fixed plan document for materially different supplied statements' \
        "$output_dir/explain-delete.stdout" \
        "$output_dir/explain-delete.stderr" \
        "$output_dir/explain-delete.before.manifest" \
        "$output_dir/explain-delete.after.manifest"
fi

expect_read_success 'final-two-row-readback' "SELECT id, note FROM $read_entries_table ORDER BY id;"
if ! jq -e -s \
    --arg first_note "$read_entries_first_note" \
    --arg second_note "$read_entries_second_note" '
    length == 1 and
    .[0] == {
        "result": {
            "columns": ["id", "note"],
            "rows": [
                {"id": 1, "note": $first_note},
                {"id": 2, "note": $second_note}
            ]
        }
    }
' "$output_dir/final-two-row-readback.stdout" >/dev/null; then
    fail_journey \
        'final-two-row-readback' \
        'the final result was not exactly the two seeded rows and values' \
        "$output_dir/final-two-row-readback.stdout" \
        "$output_dir/final-two-row-readback.stderr" \
        "$output_dir/final-two-row-readback.before.manifest" \
        "$output_dir/final-two-row-readback.after.manifest"
fi

limit_journey='explain-select-result-row-limit'
limit_stdout="$output_dir/$limit_journey.stdout"
limit_stderr="$output_dir/$limit_journey.stderr"
limit_before="$output_dir/$limit_journey.before.manifest"
limit_after="$output_dir/$limit_journey.after.manifest"
capture_store_manifest "$limit_before"
if printf '%s\n' ".explain SELECT id, note FROM $read_entries_table ORDER BY id;" \
    | "$contextdb_bin" "$database" --json --read-result-rows 1 --read-cursor-page-rows 1 \
        >"$limit_stdout" 2>"$limit_stderr"; then
    limit_status=0
else
    limit_status=$?
fi
capture_store_manifest "$limit_after"
assert_store_unchanged \
    "$limit_journey" "$limit_stdout" "$limit_stderr" "$limit_before" "$limit_after"
if (( limit_status == 0 )); then
    fail_journey \
        "$limit_journey" \
        '.explain SELECT bypassed the bounded read ceiling' \
        "$limit_stdout" \
        "$limit_stderr" \
        "$limit_before" \
        "$limit_after"
fi
if [[ -s "$limit_stdout" ]] || ! grep -F -- 'owner_limit_exceeded' "$limit_stderr" >/dev/null; then
    fail_journey \
        "$limit_journey" \
        'the bounded refusal must publish no result and carry owner_limit_exceeded' \
        "$limit_stdout" \
        "$limit_stderr" \
        "$limit_before" \
        "$limit_after"
fi
exercised_store_read_commands['.explain']=present

expect_read_success 'canonical-tables-payload' '.tables'
expect_read_success 'accepted-alias-dt' '\dt'
if ! jq -e -s \
    --arg entries_table "$read_entries_table" \
    --arg maintenance_table "$read_maintenance_table" \
    --arg policy_table "$read_policy_table" \
    --arg select_table "$read_select_table" \
    --arg vector_table "$read_vector_table" '
    length == 1 and
    .[0] == {
        "tables": {
            "items": [
                $entries_table,
                $maintenance_table,
                $policy_table,
                $select_table,
                $vector_table
            ],
            "has_more": false,
            "continuation": null
        }
    }
' \
    "$output_dir/accepted-alias-dt.stdout" >/dev/null; then
    fail_journey \
        'accepted-alias-dt' \
        '\dt did not expose the expected bounded table-list payload' \
        "$output_dir/accepted-alias-dt.stdout" \
        "$output_dir/accepted-alias-dt.stderr" \
        "$output_dir/accepted-alias-dt.before.manifest" \
        "$output_dir/accepted-alias-dt.after.manifest"
fi
if ! diff -u \
    <(jq -S . "$output_dir/canonical-tables-payload.stdout") \
    <(jq -S . "$output_dir/accepted-alias-dt.stdout") >/dev/null; then
    fail_journey \
        'accepted-alias-dt' \
        '\dt payload differed from canonical .tables' \
        "$output_dir/accepted-alias-dt.stdout" \
        "$output_dir/accepted-alias-dt.stderr" \
        "$output_dir/accepted-alias-dt.before.manifest" \
        "$output_dir/accepted-alias-dt.after.manifest"
fi
exercised_store_read_commands['.tables']=present

expect_read_success 'canonical-schema-payload' ".schema $read_entries_table"
expect_read_success 'accepted-alias-d' "\\d $read_entries_table"
if ! jq -e -s --arg entries_table "$read_entries_table" '
    length == 1 and
    (.[0] | keys) == ["schema"] and
    .[0].schema.table == $entries_table and
    ([.[0].schema.columns[].name] == ["id", "note"]) and
    .[0].schema.primary_key == ["id"] and
    .[0].schema.indexes == [{
        "name": ($entries_table + "_by_id"),
        "kind": "user",
        "columns": [{"column": "id", "direction": "ASC"}]
    }] and
    (.[0].schema.ddl | type == "string")
' \
    "$output_dir/accepted-alias-d.stdout" >/dev/null; then
    fail_journey \
        'accepted-alias-d' \
        '\d entries did not expose the expected schema payload' \
        "$output_dir/accepted-alias-d.stdout" \
        "$output_dir/accepted-alias-d.stderr" \
        "$output_dir/accepted-alias-d.before.manifest" \
        "$output_dir/accepted-alias-d.after.manifest"
fi
if ! diff -u \
    <(jq -S . "$output_dir/canonical-schema-payload.stdout") \
    <(jq -S . "$output_dir/accepted-alias-d.stdout") >/dev/null; then
    fail_journey \
        'accepted-alias-d' \
        '\d entries payload differed from canonical .schema entries' \
        "$output_dir/accepted-alias-d.stdout" \
        "$output_dir/accepted-alias-d.stderr" \
        "$output_dir/accepted-alias-d.before.manifest" \
        "$output_dir/accepted-alias-d.after.manifest"
fi
exercised_store_read_commands['.schema']=present

expect_read_success 'events-status-bounded-page' '.events status'
if ! jq -e -s \
    --arg schedule "$read_schedule" \
    --arg event "$read_event" \
    --arg sink "$read_sink" \
    --arg route "$read_route" '
    length == 1 and
    (.[0] | keys) == ["events_status"] and
    (.[0].events_status | keys) == ["continuation", "has_more", "items"] and
    (.[0].events_status.items | type == "array" and length >= 4) and
    all(.[0].events_status.items[]; type == "object") and
    ([.[0].events_status.items[].name] | sort) as $names |
    ($names | index($schedule)) and
    ($names | index($event)) and
    ($names | index($sink)) and
    ($names | index($route)) and
    .[0].events_status.has_more == false and
    .[0].events_status.continuation == null
' "$output_dir/events-status-bounded-page.stdout" >/dev/null; then
    fail_journey \
        'events-status-bounded-page' \
        '.events status did not return the four declared event objects in one bounded page' \
        "$output_dir/events-status-bounded-page.stdout" \
        "$output_dir/events-status-bounded-page.stderr" \
        "$output_dir/events-status-bounded-page.before.manifest" \
        "$output_dir/events-status-bounded-page.after.manifest"
fi
exercised_store_read_commands['.events status']=present

printf '%s\n' \
    "CREATE TABLE $maintenance_empty_table (id INTEGER PRIMARY KEY);" \
    | "$contextdb_bin" "$maintenance_empty_database" --write --json \
        >"$output_dir/maintenance-empty-seed.stdout" \
        2>"$output_dir/maintenance-empty-seed.stderr" \
    || fail_journey \
        'maintenance-empty-fixture-setup' \
        'the fixture with no maintenance declarations must seed successfully' \
        "$output_dir/maintenance-empty-seed.stdout" \
        "$output_dir/maintenance-empty-seed.stderr"

expect_read_success 'maintenance-status-complete-object' '.maintenance status'
expect_read_success \
    'maintenance-status-empty-fixture' \
    '.maintenance status' \
    "$maintenance_empty_database"
if ! jq -e -s '
    length == 1 and
    (.[0] | keys) == ["maintenance"] and
    (.[0].maintenance | type == "object") and
    (.[0].maintenance.running | type == "boolean") and
    .[0].maintenance.retention_enabled == true and
    .[0].maintenance.currency_compaction_enabled == true and
    (.[0].maintenance.active_maintenance_loops | type == "number") and
    (.[0].maintenance.policy | type == "string" and length > 0)
' "$output_dir/maintenance-status-complete-object.stdout" >/dev/null; then
    fail_journey \
        'maintenance-status-complete-object' \
        '.maintenance status did not return one complete maintenance-state object' \
        "$output_dir/maintenance-status-complete-object.stdout" \
        "$output_dir/maintenance-status-complete-object.stderr" \
        "$output_dir/maintenance-status-complete-object.before.manifest" \
        "$output_dir/maintenance-status-complete-object.after.manifest"
fi
if ! jq -e -s '
    length == 1 and
    (.[0] | keys) == ["maintenance"] and
    .[0].maintenance.retention_enabled == false and
    .[0].maintenance.currency_compaction_enabled == false
' "$output_dir/maintenance-status-empty-fixture.stdout" >/dev/null; then
    fail_journey \
        'maintenance-status-empty-fixture' \
        '.maintenance status did not reflect the contrasting fixture with no maintenance declarations' \
        "$output_dir/maintenance-status-empty-fixture.stdout" \
        "$output_dir/maintenance-status-empty-fixture.stderr" \
        "$output_dir/maintenance-status-empty-fixture.before.manifest" \
        "$output_dir/maintenance-status-empty-fixture.after.manifest"
fi
exercised_store_read_commands['.maintenance status']=present

cursor_journey='cursor-open-fetch-close-one-row-pages'
cursor_stdout="$output_dir/$cursor_journey.stdout"
cursor_stderr="$output_dir/$cursor_journey.stderr"
cursor_before="$output_dir/$cursor_journey.before.manifest"
cursor_after="$output_dir/$cursor_journey.after.manifest"
capture_store_manifest "$cursor_before"
if printf '%s\n' \
    ".cursor open SELECT id, note FROM $read_entries_table ORDER BY id" \
    '.cursor fetch' \
    '.cursor close' \
    | "$contextdb_bin" "$database" --json --read-cursor-page-rows 1 \
        >"$cursor_stdout" 2>"$cursor_stderr"; then
    cursor_status=0
else
    cursor_status=$?
fi
capture_store_manifest "$cursor_after"
assert_store_unchanged \
    "$cursor_journey" "$cursor_stdout" "$cursor_stderr" "$cursor_before" "$cursor_after"
if (( cursor_status != 0 )); then
    fail_journey \
        "$cursor_journey" \
        "the stateful cursor session exited $cursor_status" \
        "$cursor_stdout" \
        "$cursor_stderr" \
        "$cursor_before" \
        "$cursor_after"
fi
if ! jq -e -s \
    --arg first_note "$read_entries_first_note" \
    --arg second_note "$read_entries_second_note" '
    length == 3 and
    .[0] == {
        "cursor": {
            "columns": ["id", "note"],
            "rows": [{"id": 1, "note": $first_note}],
            "has_more": true
        }
    } and
    .[1] == {
        "cursor": {
            "columns": ["id", "note"],
            "rows": [{"id": 2, "note": $second_note}],
            "has_more": false
        }
    } and
    .[2] == {"cursor": {"closed": true}}
' \
    "$cursor_stdout" >/dev/null; then
    fail_journey \
        "$cursor_journey" \
        'cursor open, fetch, and close did not publish the exact advancing page sequence' \
        "$cursor_stdout" \
        "$cursor_stderr" \
        "$cursor_before" \
        "$cursor_after"
fi
if ! jq -e -s '
    ([.[] | select(.notice.detail.kind? == "read_route")]) as $routes |
    all(.[]; type == "object") and
    ([.[] | select(has("error"))] | length) == 0 and
    ($routes | length) == 1 and
    $routes[0].notice.detail.route == "file" and
    ($routes[0].notice.detail.snapshot_at | type == "string")
' "$cursor_stderr" >/dev/null; then
    fail_journey \
        "$cursor_journey" \
        'the piped cursor session did not resolve exactly one successful file read route' \
        "$cursor_stdout" \
        "$cursor_stderr" \
        "$cursor_before" \
        "$cursor_after"
fi
exercised_store_read_commands['.cursor open']=present
exercised_store_read_commands['.cursor fetch']=present
exercised_store_read_commands['.cursor close']=present

# Owner status is neither a store read nor a session-only no-op: it must remain
# available without opening a read route and report the direct-store state.
expect_read_success 'owner-status-without-owner' '.owner status'
if [[ -s "$output_dir/owner-status-without-owner.stderr" ]] \
    || ! jq -e -s 'length == 1 and .[0] == {"owner": {"state": "not_running"}}' \
        "$output_dir/owner-status-without-owner.stdout" >/dev/null; then
    fail_journey \
        'owner-status-without-owner' \
        '.owner status must publish the exact no-owner document without a route notice or diagnostic' \
        "$output_dir/owner-status-without-owner.stdout" \
        "$output_dir/owner-status-without-owner.stderr" \
        "$output_dir/owner-status-without-owner.before.manifest" \
        "$output_dir/owner-status-without-owner.after.manifest"
fi

# Each canonical Invalid declaration and malformed SQL must be rejected before
# any route or execution with one exact error envelope and no partial result.
for invalid_command in '.events' '.maintenance' '.cursor' '.sync' '.owner'; do
    invalid_journey="invalid-${invalid_command#.}"
    case "$invalid_command" in
        # Write-classified families: a reading session answers `write_requires_flag`
        # before the argument is read (pinned by the StoreWrite journeys above), so a
        # writing session is where the usage answer is the honest one.
        '.maintenance' | '.sync')
            expect_exact_error_refusal "$invalid_journey" "$invalid_command" usage write
            ;;
        *)
            expect_exact_error_refusal "$invalid_journey" "$invalid_command" usage
            ;;
    esac
done
expect_exact_error_refusal 'malformed-sql-parse-refusal' 'SELEKT malformed_sql_proof' sql

expect_session_only_success 'session-trace-on-reads-no-store-data' '.trace on'
if [[ -s "$output_dir/session-trace-on-reads-no-store-data.stderr" ]] \
    || ! jq -e -s 'length == 1 and .[0] == {"trace": "on"}' \
        "$output_dir/session-trace-on-reads-no-store-data.stdout" >/dev/null; then
    fail_journey \
        'session-trace-on-reads-no-store-data' \
        '.trace on must emit exactly one enabled trace-state document and no diagnostic' \
        "$output_dir/session-trace-on-reads-no-store-data.stdout" \
        "$output_dir/session-trace-on-reads-no-store-data.stderr" \
        "$output_dir/session-trace-on-reads-no-store-data.before.manifest" \
        "$output_dir/session-trace-on-reads-no-store-data.after.manifest"
fi
exercised_session_commands['.trace']=present

expect_session_only_success 'session-help-reads-no-store-data' '.help'
if [[ -s "$output_dir/session-help-reads-no-store-data.stdout" ]] \
    || ! jq -e -s '
        length == 1 and
        (.[0] | keys) == ["help"] and
        (.[0].help | type == "array" and length > 0 and all(.[]; type == "string"))
    ' "$output_dir/session-help-reads-no-store-data.stderr" >/dev/null; then
    fail_journey \
        'session-help-reads-no-store-data' \
        '.help must keep JSON stdout empty and emit one nonempty help document on stderr' \
        "$output_dir/session-help-reads-no-store-data.stdout" \
        "$output_dir/session-help-reads-no-store-data.stderr" \
        "$output_dir/session-help-reads-no-store-data.before.manifest" \
        "$output_dir/session-help-reads-no-store-data.after.manifest"
fi
exercised_session_commands['.help']=present

for session_exit in '.quit' '.exit'; do
    journey="session-${session_exit#.}-reads-no-store-data"
    session_exit_input="${session_exit}"$'\n''.trace on'
    expect_session_only_success "$journey" "$session_exit_input"
    if [[ -s "$output_dir/$journey.stdout" || -s "$output_dir/$journey.stderr" ]]; then
        fail_journey \
            "$journey" \
            "$session_exit must exit a piped JSON session without publishing output" \
            "$output_dir/$journey.stdout" \
            "$output_dir/$journey.stderr" \
            "$output_dir/$journey.before.manifest" \
            "$output_dir/$journey.after.manifest"
    fi
    exercised_session_commands["$session_exit"]=present
done

expect_session_only_success 'session-sync-status-reads-no-store-data' '.sync status'
if [[ -s "$output_dir/session-sync-status-reads-no-store-data.stderr" ]] \
    || ! jq -e -s --arg expected_message "$sync_status_message" '
        length == 1 and
        .[0] == {"sync_status": {"message": $expected_message}}
    ' "$output_dir/session-sync-status-reads-no-store-data.stdout" >/dev/null; then
    fail_journey \
        'session-sync-status-reads-no-store-data' \
        '.sync status did not emit the pinned read-session status document' \
        "$output_dir/session-sync-status-reads-no-store-data.stdout" \
        "$output_dir/session-sync-status-reads-no-store-data.stderr" \
        "$output_dir/session-sync-status-reads-no-store-data.before.manifest" \
        "$output_dir/session-sync-status-reads-no-store-data.after.manifest"
fi
exercised_session_commands['.sync status']=present

expect_session_only_success 'accepted-alias-q' $'\\q\n.trace on'
if [[ -s "$output_dir/accepted-alias-q.stdout" || -s "$output_dir/accepted-alias-q.stderr" ]]; then
    fail_journey \
        'accepted-alias-q' \
        '\q must exit a piped JSON session cleanly without publishing output' \
        "$output_dir/accepted-alias-q.stdout" \
        "$output_dir/accepted-alias-q.stderr" \
        "$output_dir/accepted-alias-q.before.manifest" \
        "$output_dir/accepted-alias-q.after.manifest"
fi
if ! cmp -s \
    "$output_dir/session-quit-reads-no-store-data.stdout" \
    "$output_dir/accepted-alias-q.stdout" \
    || ! cmp -s \
        "$output_dir/session-quit-reads-no-store-data.stderr" \
        "$output_dir/accepted-alias-q.stderr"; then
    fail_journey \
        'accepted-alias-q' \
        '\q output differed from canonical .quit' \
        "$output_dir/accepted-alias-q.stdout" \
        "$output_dir/accepted-alias-q.stderr" \
        "$output_dir/accepted-alias-q.before.manifest" \
        "$output_dir/accepted-alias-q.after.manifest"
fi

expect_session_only_success 'accepted-alias-help' '\?'
if [[ -s "$output_dir/accepted-alias-help.stdout" ]] \
    || ! jq -e -s '
        length == 1 and
        (.[0] | keys) == ["help"] and
        (.[0].help | type == "array" and length > 0 and all(.[]; type == "string"))
    ' \
        "$output_dir/accepted-alias-help.stderr" >/dev/null; then
    fail_journey \
        'accepted-alias-help' \
        '\? must keep JSON stdout empty and emit one help document on stderr' \
        "$output_dir/accepted-alias-help.stdout" \
        "$output_dir/accepted-alias-help.stderr" \
        "$output_dir/accepted-alias-help.before.manifest" \
        "$output_dir/accepted-alias-help.after.manifest"
fi
if ! cmp -s \
    "$output_dir/session-help-reads-no-store-data.stdout" \
    "$output_dir/accepted-alias-help.stdout" \
    || ! cmp -s \
        "$output_dir/session-help-reads-no-store-data.stderr" \
        "$output_dir/accepted-alias-help.stderr"; then
    fail_journey \
        'accepted-alias-help' \
        '\? output differed from canonical .help' \
        "$output_dir/accepted-alias-help.stdout" \
        "$output_dir/accepted-alias-help.stderr" \
        "$output_dir/accepted-alias-help.before.manifest" \
        "$output_dir/accepted-alias-help.after.manifest"
fi

readonly -a REMOVED_META_CASES=(
    'undeclared-backslash-alias|\foo'
    'removed-trace|\trace'
    'removed-trace-argument|\trace on'
    'removed-sync|\sync'
    'removed-sync-subcommand|\sync status'
    'near-miss-traceback|\traceback'
    'near-miss-syncing|\syncing'
)
for case_row in "${REMOVED_META_CASES[@]}"; do
    expect_removed_meta_refusal "${case_row%%|*}" "${case_row#*|}"
done

expect_cli_refusal 'removed-all-option' '--all' "$database" --all --json
expect_cli_refusal 'removed-repair-command' 'repair' repair "$database"

help_stdout="$output_dir/help.stdout"
help_stderr="$output_dir/help.stderr"
if ! "$contextdb_bin" --help >"$help_stdout" 2>"$help_stderr"; then
    fail_journey 'ordinary-help' 'ordinary --help must succeed' "$help_stdout" "$help_stderr"
fi

# Retired while the frozen proof keeps the exact-signature parser below.  The
# old prose/category matcher is intentionally inert: it could mistake prose
# for a command and must never become a second discovery oracle.
: <<'RETIRED_PERMISSIVE_HELP_MATCHER'
legacy_permissive_help_check() {
help_signature() {
    local line="$1"
    local signature
    signature="${line#"${line%%[![:space:]]*}"}"
    case "$signature" in
        '- '*) signature="${signature#- }" ;;
        '* '*) signature="${signature#\* }" ;;
    esac
    signature="${signature//\`/}"
    signature="${signature%%$'\t'*}"
    signature="${signature%%  *}"
    signature="${signature%% — *}"
    printf '%s\n' "$signature"
}

signature_starts_entry() {
    local signature="$1"
    local entry="$2"
    [[ "$signature" == "$entry" || "$signature" == "$entry "* ]]
}

signature_contains_entry() {
    local normalized="$1"
    local entry="$2"
    normalized="${normalized//,/ }"
    normalized="${normalized//\// }"
    normalized="${normalized//|/ }"
    normalized="${normalized//;/ }"
    [[ " $normalized " == *" $entry "* ]]
}

signature_is_command_or_option_entry() {
    local signature="$1"
    [[ "$signature" == .* || "$signature" == \\* || "$signature" == --* ]]
}

declare -A observed_categorized_help_entries=()
declare -A observed_operational_commands=()

record_categorized_signature() {
    local category="$1"
    local signature="$2"
    local segments="$signature"
    local segment
    local fragment
    local candidate
    local best_match
    local observed_entry

    segments="${segments//,/$'\n'}"
    segments="${segments//;/$'\n'}"
    segments="${segments//|/$'\n'}"
    segments="${segments//\//$'\n'}"
    while IFS= read -r segment; do
        segment="${segment#"${segment%%[![:space:]]*}"}"
        segment="${segment%"${segment##*[![:space:]]}"}"
        [[ "$segment" == *.* ]] || continue
        fragment=".${segment#*.}"
        best_match=''
        for candidate in "${expected_read_commands[@]}" "${expected_write_commands[@]}"; do
            if [[ "$fragment" == "$candidate" || "$fragment" == "$candidate "* ]] \
                && (( ${#candidate} > ${#best_match} )); then
                best_match="$candidate"
            fi
        done
        if [[ -n "$best_match" ]]; then
            observed_entry="$best_match"
        else
            observed_entry="${fragment%%[[:space:]]*}"
            observed_entry="${observed_entry%:}"
        fi
        if [[ "$observed_entry" == .* && "$observed_entry" != '.' ]]; then
            observed_categorized_help_entries["$category|$observed_entry"]=present
        fi
    done <<<"$segments"
}

current_help_category=''
while IFS= read -r help_line; do
    signature="$(help_signature "$help_line")"
    lowercase_line="${help_line,,}"
    line_declares_category=false
    line_starts_registered_entry=false
    for registered_entry in \
        "${expected_valid_meta_commands[@]}" \
        "${expected_operational_commands[@]}"; do
        if signature_starts_entry "$signature" "$registered_entry"; then
            line_starts_registered_entry=true
        fi
    done
    if [[ "$line_starts_registered_entry" == false ]]; then
        if [[ "$lowercase_line" == *'--write'* && "$lowercase_line" == *'requir'* ]]; then
            current_help_category='StoreWrite'
            line_declares_category=true
        elif [[ "$lowercase_line" == *'read'* && "$lowercase_line" != *'--write'* ]]; then
            current_help_category='StoreRead'
            line_declares_category=true
        elif [[ "$lowercase_line" == *'operational'* && "$lowercase_line" == *'command'* ]] \
            || [[ "${lowercase_line//[[:space:]]/}" == 'commands:' ]]; then
            current_help_category='Operational'
            line_declares_category=true
        fi
    fi

    if [[ "$current_help_category" == 'StoreRead' || "$current_help_category" == 'StoreWrite' ]]; then
        categorized_signature=false
        if [[ "$signature" == .* ]] || [[ "$line_declares_category" == true && "$signature" == *.* ]]; then
            categorized_signature=true
        else
            for command in "${expected_read_commands[@]}" "${expected_write_commands[@]}"; do
                if signature_contains_entry "$signature" "$command"; then
                    categorized_signature=true
                fi
            done
        fi
        if [[ "$categorized_signature" == true ]]; then
            record_categorized_signature "$current_help_category" "$signature"
        fi
    fi
    for command in "${expected_operational_commands[@]}"; do
        if signature_starts_entry "$signature" "$command" \
            || { [[ "$current_help_category" == 'Operational' ]] \
                && signature_contains_entry "$signature" "$command"; }; then
            observed_operational_commands["$command"]=present
        fi
    done
done <"$help_stdout"

for observed_entry in "${!observed_categorized_help_entries[@]}"; do
    if [[ "${expected_categorized_help_entries[$observed_entry]:-missing}" != 'present' ]]; then
        observed_category="${observed_entry%%|*}"
        command="${observed_entry#*|}"
        fail_journey \
            'ordinary-help' \
            "unexpected or miscategorized $observed_category entry $command" \
            "$help_stdout" \
            "$help_stderr"
    fi
done

for expected_entry in "${!expected_categorized_help_entries[@]}"; do
    if [[ "${observed_categorized_help_entries[$expected_entry]:-missing}" != 'present' ]]; then
        expected_category="${expected_entry%%|*}"
        command="${expected_entry#*|}"
        fail_journey \
            'ordinary-help' \
            "missing categorized $expected_category entry $command" \
            "$help_stdout" \
            "$help_stderr"
    fi
done

if (( ${#observed_categorized_help_entries[@]} != ${#expected_categorized_help_entries[@]} )); then
    fail_journey \
        'ordinary-help' \
        'observed read/requires-write entry count differs from the canonical declarations' \
        "$help_stdout" \
        "$help_stderr"
fi

for command in "${expected_operational_commands[@]}"; do
    if [[ "${observed_operational_commands[$command]:-missing}" != 'present' ]]; then
        fail_journey \
            'ordinary-help' \
            "typed operational command $command is not exposed as a help entry" \
            "$help_stdout" \
            "$help_stderr"
    fi
done

while IFS= read -r help_line; do
    signature="$(help_signature "$help_line")"
    for removed_entry in repair --all '\trace' '\sync'; do
        removed_is_entry=false
        if signature_starts_entry "$signature" "$removed_entry"; then
            removed_is_entry=true
        elif signature_is_command_or_option_entry "$signature" \
            && signature_contains_entry "$signature" "$removed_entry"; then
            removed_is_entry=true
        else
            for registered_entry in \
                "${expected_valid_meta_commands[@]}" \
                "${expected_operational_commands[@]}"; do
                if signature_starts_entry "$signature" "$registered_entry" \
                    && signature_contains_entry "$signature" "$removed_entry"; then
                    removed_is_entry=true
                fi
            done
        fi
        if [[ "$removed_is_entry" == true ]]; then
            fail_journey \
                'ordinary-help' \
                "removed command or option $removed_entry remains a help entry" \
                "$help_stdout" \
                "$help_stderr"
        fi
    done
done <"$help_stdout"
}
RETIRED_PERMISSIVE_HELP_MATCHER

for command in "${expected_read_commands[@]}"; do
    if [[ "${exercised_store_read_commands[$command]:-missing}" != 'present' ]]; then
        fail_plain "canonical StoreRead command $command lacks a production-binary journey"
    fi
done
if (( ${#exercised_store_read_commands[@]} != ${#expected_read_commands[@]} )); then
    fail_plain 'executed StoreRead journeys do not exactly cover the canonical declarations'
fi

for command in "${expected_session_commands[@]}"; do
    if [[ "${exercised_session_commands[$command]:-missing}" != 'present' ]]; then
        fail_plain "canonical SessionOnly command $command lacks a production-binary journey"
    fi
done
if (( ${#exercised_session_commands[@]} != ${#expected_session_commands[@]} )); then
    fail_plain 'executed SessionOnly journeys do not exactly cover the canonical declarations'
fi

legacy_exact_registry_help_surface() {
    local journey="$1"
    local surface="$2"
    local content="$3"
    local signature
    local recognized
    local command
    local effect

    for declaration in "${META_COMMAND_DECLARATIONS[@]}"; do
        effect="${declaration%%|*}"
        command="${declaration#*|}"
        [[ "$effect" == Invalid ]] && continue
        if ! grep -F -- "$command" "$content" >/dev/null; then
            fail_journey "$journey" "$surface omitted canonical $effect command $command" "$content"
        fi
    done
    for alias in '\dt' '\d' '\q' '\?'; do
        recognized=false
        while IFS= read -r help_line; do
            signature="$(help_signature "$help_line")"
            if signature_starts_entry "$signature" "$alias"; then
                recognized=true
                break
            fi
        done <"$content"
        if [[ "$recognized" == false ]]; then
            fail_journey "$journey" "$surface omitted canonical alias $alias" "$content"
        fi
    done
    for command in "${expected_operational_commands[@]}"; do
        if ! grep -E -- "(^|[[:space:]])$command([[:space:]]|$)" "$content" >/dev/null; then
            fail_journey "$journey" "$surface omitted canonical operational row $command" "$content"
        fi
    done

    while IFS= read -r help_line; do
        signature="$(help_signature "$help_line")"
        [[ "$signature" == .* ]] || continue
        recognized=false
        for declaration in "${META_COMMAND_DECLARATIONS[@]}"; do
            effect="${declaration%%|*}"
            command="${declaration#*|}"
            if [[ "$effect" != Invalid ]] && signature_starts_entry "$signature" "$command"; then
                recognized=true
            fi
        done
        if [[ "$recognized" == false ]]; then
            fail_journey "$journey" "$surface exposes stale or extra dot-command entry $signature" "$content"
        fi
    done <"$content"

    for forbidden_entry in repair --all '\trace' '\sync'; do
        while IFS= read -r help_line; do
            signature="$(help_signature "$help_line")"
            if signature_starts_entry "$signature" "$forbidden_entry"; then
                fail_journey "$journey" "$surface exposes removed, invalid, or stale entry $forbidden_entry" "$content"
            fi
        done <"$content"
    done
}

# A help entry is a signature, never a prose mention.  Markdown tables expose
# only their first cell; ordinary and in-session help must start an entry with
# the command itself.  This intentionally does not scan options or positional
# arguments as commands.
help_signature_entry() {
    local line="$1"
    local signature

    signature="${line#"${line%%[![:space:]]*}"}"
    if [[ "$signature" == '|'* ]]; then
        signature="${signature#|}"
        signature="${signature%%|*}"
        signature="${signature#"${signature%%[![:space:]]*}"}"
        signature="${signature//\`/}"
    else
        case "$signature" in
            '- '*) signature="${signature#- }" ;;
            '* '*) signature="${signature#\* }" ;;
        esac
        if [[ "$signature" != .* && "$signature" != \\* && "$signature" != --all* ]]; then
            for command in "${expected_operational_commands[@]}" repair; do
                if [[ "$signature" == "$command" || "$signature" == "$command "* ]]; then
                    break
                fi
            done
            if [[ "$signature" != "$command" && "$signature" != "$command "* ]]; then
                return 0
            fi
        fi
    fi
    signature="${signature%%$'\t'*}"
    signature="${signature%%  *}"
    signature="${signature%% — *}"
    printf '%s\n' "$signature"
}

signature_starts_entry() {
    local signature="$1"
    local entry="$2"
    [[ "$signature" == "$entry" || "$signature" == "$entry "* ]]
}

assert_exact_registry_help_surface() {
    local journey="$1"
    local surface="$2"
    local content="$3"
    # Two different surfaces, two different registries. The in-session `.help` and the
    # documentation carry every meta-command; ordinary `--help` carries the invocation
    # arguments and the operational command tree, and the meta-commands belong to the
    # session, not to the command line. Either way the surface is EXACT: a removed or
    # undeclared spelling fails wherever it appears.
    local scope="${4:-meta-commands-and-operational}"
    local help_line
    local signature
    local fragment
    local candidate
    local recognized
    local command
    declare -A observed_entries=()
    declare -A expected_entries=()

    if [[ "$scope" == operational-only ]]; then
        for command in "${expected_operational_commands[@]}"; do
            expected_entries["$command"]=present
        done
    else
        for command in "${expected_valid_meta_commands[@]}" '\dt' '\d' '\q' '\?' \
            "${expected_operational_commands[@]}"; do
            expected_entries["$command"]=present
        done
    fi

    while IFS= read -r help_line; do
        signature="$(help_signature_entry "$help_line")"
        [[ -n "$signature" ]] || continue
        # Slash and pipe are signature alternatives; a later documentation
        # writer must spell multiword commands individually instead of using
        # shorthand such as `.cursor open/fetch/close`.
        signature="${signature//\//$'\n'}"
        signature="${signature//|/$'\n'}"
        while IFS= read -r fragment; do
            fragment="${fragment#"${fragment%%[![:space:]]*}"}"
            fragment="${fragment//\`/}"
            [[ -n "$fragment" ]] || continue
            recognized=''
            for candidate in "${!expected_entries[@]}"; do
                if signature_starts_entry "$fragment" "$candidate"; then
                    if [[ -z "$recognized" || ${#candidate} -gt ${#recognized} ]]; then
                        recognized="$candidate"
                    fi
                fi
            done
            if [[ -n "$recognized" ]]; then
                observed_entries["$recognized"]=present
            elif [[ "$fragment" == .* || "$fragment" == \\* || "$fragment" == repair* || "$fragment" == --all* ]]; then
                fail_journey \
                    "$journey" \
                    "$surface exposes undeclared, removed, or stale signature $fragment" \
                    "$content"
            fi
        done <<<"$signature"
    done <"$content"

    for command in "${!expected_entries[@]}"; do
        if [[ "${observed_entries[$command]:-missing}" != present ]]; then
            fail_journey \
                "$journey" \
                "$surface omitted canonical command signature $command" \
                "$content"
        fi
    done
    if (( ${#observed_entries[@]} != ${#expected_entries[@]} )); then
        fail_journey \
            "$journey" \
            "$surface's declared command signatures are not an exact registry surface" \
            "$content"
    fi
}

assert_registry_consumer_dataflow() {
    local cli_root
    local repo_root
    local repl_source
    local main_source
    local documentation
    local authorization_line
    local dispatch_line
    local dispatch_marker

    cli_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
    repo_root="$(cd "$cli_root/../.." && pwd)"
    repl_source="$cli_root/src/repl.rs"
    main_source="$cli_root/src/main.rs"
    documentation="$repo_root/docs/cli.md"

    for source in "$repl_source" "$main_source"; do
        if ! rg -F -- 'canonical_help_signatures' "$source" >/dev/null; then
            fail_plain "canonical command discovery is not consumed by ${source#$repo_root/}"
        fi
    done
    if ! grep -F -- '<!-- command-registry: canonical_help_signatures -->' "$documentation" >/dev/null; then
        fail_plain 'docs/cli.md lacks the canonical-help parity anchor'
    fi

    authorization_line="$(rg -n -F -- 'authorize_meta_command_before_dispatch' "$repl_source" \
        | head -n 1 | cut -d: -f1)"
    if [[ -z "$authorization_line" ]]; then
        fail_plain 'the REPL has no pre-dispatch StoreWrite authorization gate'
    fi
    for dispatch_marker in 'db.run_maintenance_cycle' 'db.compact_now' 'handle_sync_command'; do
        dispatch_line="$(rg -n -F -- "$dispatch_marker" "$repl_source" | head -n 1 | cut -d: -f1)"
        if [[ -z "$dispatch_line" ]] || (( authorization_line >= dispatch_line )); then
            fail_plain "the StoreWrite authorization gate does not precede $dispatch_marker"
        fi
    done
}

session_help_lines="$output_dir/session-help.lines"
if ! jq -r -s '
    if (length == 1 and (.[0] | keys) == ["help"] and
        (.[0].help | type == "array" and all(.[]; type == "string")))
    then .[0].help[]
    else error("not one help document")
    end
' "$output_dir/session-help-reads-no-store-data.stderr" >"$session_help_lines"; then
    fail_journey \
        'session-help-reads-no-store-data' \
        '.help must provide one parseable registry-discovery document' \
        "$output_dir/session-help-reads-no-store-data.stdout" \
        "$output_dir/session-help-reads-no-store-data.stderr"
fi
assert_exact_registry_help_surface \
    'session-help-reads-no-store-data' \
    'in-session .help' \
    "$session_help_lines"
assert_exact_registry_help_surface \
    'ordinary-help' \
    'ordinary --help' \
    "$help_stdout" \
    operational-only

documentation_help_surface="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)/docs/cli.md"
assert_exact_registry_help_surface \
    'documentation-help-parity' \
    'docs/cli.md' \
    "$documentation_help_surface"
assert_registry_consumer_dataflow

printf 'PASS read classification and discovery production proof\n'
