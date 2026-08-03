#!/usr/bin/env bash
# Installed-release proof for the authenticated schema vector and oversized
# dependency-unit crash boundaries. It uses real files, real ticketed Iroh,
# exact child PIDs, and state-driven pipe events; there are no sleeps or mocks.
set -euo pipefail

cli="${CONTEXTDB_CLI:?set CONTEXTDB_CLI to the installed release contextdb binary}"
driver="${CONTEXTDB_SMOKE_DRIVER:?set CONTEXTDB_SMOKE_DRIVER to the installed release contextdb-smoke-driver binary}"
server="${CONTEXTDB_SERVER:?set CONTEXTDB_SERVER to the installed release contextdb-server binary}"

if command -v timeout >/dev/null 2>&1; then
  timeout_bin="$(command -v timeout)"
elif command -v gtimeout >/dev/null 2>&1; then
  timeout_bin="$(command -v gtimeout)"
else
  printf 'FAIL installed smoke requires timeout (GNU coreutils; on macOS: brew install coreutils)\n' >&2
  exit 2
fi

[[ -x "$cli" ]] || { printf 'FAIL installed contextdb CLI is missing\n' >&2; exit 2; }
[[ -x "$driver" ]] || { printf 'FAIL installed smoke verifier is missing\n' >&2; exit 2; }
[[ -x "$server" ]] || { printf 'FAIL installed contextdb server is missing\n' >&2; exit 2; }

work="$(mktemp -d)"
hub_pid=""

cleanup() {
  local status=$?
  trap - EXIT
  if [[ -n "$hub_pid" ]] && kill -0 "$hub_pid" 2>/dev/null; then
    kill -KILL "$hub_pid" 2>/dev/null || true
    wait "$hub_pid" 2>/dev/null || true
  fi
  exec 9<&- || true
  if [[ "$status" -eq 0 && "${CONTEXTDB_SMOKE_KEEP_WORK:-0}" != "1" ]]; then
    rm -rf "$work"
  else
    printf 'ARTIFACTS retained at %s\n' "$work" >&2
  fi
  exit "$status"
}
trap cleanup EXIT

fail() {
  printf 'FAIL %s\n' "$1" >&2
  exit 1
}

pass() {
  printf 'PASS %s\n' "$1"
}

require_text() {
  local text="$1"
  local expected="$2"
  local label="$3"
  [[ "$text" == *"$expected"* ]] || fail "$label"
  pass "$label"
}

require_file_text() {
  local file="$1"
  local expected="$2"
  local label="$3"
  grep -Fq -- "$expected" "$file" || fail "$label"
  pass "$label"
}

start_hub() {
  local root="$1"
  local receiver="$2"
  local checkpoint="$3"
  mkdir -p "$root"
  local events="$root/events.fifo"
  rm -f "$events"
  mkfifo "$events"
  "$driver" hub \
    --db "$root/hub.db" \
    --identity "$root/hub.identity" \
    --ticket-file "$root/ticket" \
    --tenant-id installed-smoke \
    --receiver "$receiver" \
    --checkpoint "$checkpoint" \
    >"$events" 2>"$root/hub.stderr" &
  hub_pid=$!
  exec 9<"$events"
  local ready
  IFS= read -r -t 30 -u 9 ready || {
    wait "$hub_pid" 2>/dev/null || true
    fail "hub reached production route readiness"
  }
  require_text "$ready" '"event":"ready"' "hub reached production route readiness"
}

read_hub_event() {
  local event
  IFS= read -r -t 30 -u 9 event || fail "hub emitted the requested durable checkpoint within 30 seconds"
  printf '%s\n' "$event"
}

stop_hub() {
  kill -TERM "$hub_pid"
  wait "$hub_pid"
  exec 9<&-
  hub_pid=""
}

kill_hub_at_checkpoint() {
  kill -KILL "$hub_pid"
  wait "$hub_pid" 2>/dev/null || true
  exec 9<&-
  hub_pid=""
}

snapshot_state() {
  local db="$1"
  local artifact="$2"
  "$cli" snapshot export "$db" "$artifact" --json >/dev/null
  "$cli" inspect sync-apply-state "$artifact" --json
}

schema_json() {
  local db="$1"
  printf '.schema authored_migration\n.quit\n' | "$cli" "$db" --json \
    | grep '"table":"authored_migration"' | tail -n 1
}

json_number() {
  local json="$1"
  local field="$2"
  printf '%s\n' "$json" | sed -n "s/.*\"${field}\":\([0-9][0-9]*\).*/\1/p"
}

json_file_number() {
  local file="$1"
  local field="$2"
  local value
  value="$(sed -n "s/.*\"${field}\":\([0-9][0-9]*\).*/\1/p" "$file")"
  [[ "$value" =~ ^[0-9]+$ ]] || fail "$file has exactly one numeric $field"
  printf '%s\n' "$value"
}

json_string() {
  local json="$1"
  local field="$2"
  printf '%s\n' "$json" | sed -n "s/.*\"${field}\":\"\([^\"]*\)\".*/\1/p"
}

printf 'CHECK ordinary product help does not expose the verifier or removed policy/broker controls\n'
help="$($cli --help 2>&1)"
[[ "$help" != *"smoke-driver"* ]] || fail "ordinary CLI hides verifier controls"
[[ "$help" != *"ServerWins"* && "$help" != *"EdgeWins"* && "$help" != *"LatestWins"* && "$help" != *"InsertIfNotExists"* ]] \
  || fail "ordinary CLI hides role-mechanic policy names"
[[ "$help" != *"nats"* && "$help" != *"NATS"* ]] || fail "ordinary CLI has no broker surface"
pass "ordinary CLI hides verifier, role-mechanic, and broker controls"
for removed_command in policy direction; do
  removed_output="$(printf '.sync %s\n.quit\n' "$removed_command" \
    | "$cli" "$work/removed-$removed_command.db" --json 2>&1 || true)"
  removed_output_lower="$(printf '%s' "$removed_output" | tr '[:upper:]' '[:lower:]')"
  [[ "$removed_output_lower" == *"$removed_command"* \
     && ( "$removed_output_lower" == *"unknown"* || "$removed_output_lower" == *"unrecognized"* ) ]] \
    || fail "removed session $removed_command command fails as unknown"
  pass "removed session $removed_command command fails as unknown"
done
if "$cli" "$work/removed-cli-flag.db" --nats-url nats://127.0.0.1:4222 </dev/null \
  >"$work/removed-cli-flag.stdout" 2>"$work/removed-cli-flag.stderr"; then
  fail "removed CLI broker flag fails as unknown"
fi
require_file_text "$work/removed-cli-flag.stderr" "unexpected argument '--nats-url'" \
  "removed CLI broker flag fails as unknown"
if "$server" --db-path "$work/removed-server-flag.db" --nats-url nats://127.0.0.1:4222 \
  >"$work/removed-server-flag.stdout" 2>"$work/removed-server-flag.stderr"; then
  fail "removed server broker flag fails as unknown"
fi
require_file_text "$work/removed-server-flag.stderr" "unexpected argument '--nats-url'" \
  "removed server broker flag fails as unknown"

printf 'CHECK one machine identity survives reopen and database recreation while database life changes\n'
identity_root="$work/identity-life"
mkdir -p "$identity_root"
first_identity="$($driver identity --db "$identity_root/edge.db" \
  --identity "$identity_root/edge.identity" --tenant-id installed-smoke)"
reopened_identity="$($driver identity --db "$identity_root/edge.db" \
  --identity "$identity_root/edge.identity" --tenant-id installed-smoke)"
first_node="$(json_string "$first_identity" node_id)"
first_incarnation="$(json_string "$first_identity" database_incarnation)"
[[ -n "$first_node" && -n "$first_incarnation" ]] \
  || fail "identity verifier emitted node and database incarnation"
[[ "$first_identity" == "$reopened_identity" ]] \
  || fail "plain reopen retained machine identity and database incarnation"
pass "plain reopen retained machine identity and database incarnation"
rm -f "$identity_root/edge.db"
recreated_identity="$($driver identity --db "$identity_root/edge.db" \
  --identity "$identity_root/edge.identity" --tenant-id installed-smoke)"
recreated_node="$(json_string "$recreated_identity" node_id)"
recreated_incarnation="$(json_string "$recreated_identity" database_incarnation)"
[[ "$recreated_node" == "$first_node" && "$recreated_incarnation" != "$first_incarnation" ]] \
  || fail "database recreation retained machine identity and minted a new database life"
pass "database recreation retained machine identity and minted a new database life"

printf 'CHECK declared convergence, one-way refusals, and pull-only overwrite\n'
mkdir -p "$work/policy-journeys"
"$timeout_bin" 180 "$driver" policy --root "$work/policy-journeys" --cli "$cli" \
  >"$work/policy-journeys.log"
require_file_text "$work/policy-journeys.log" '"event":"two_way_keep_first"' \
  "two-way KEEP FIRST converged with durable winner provenance"
require_file_text "$work/policy-journeys.log" '"event":"push_only_write_refused"' \
  "push-only refused write retired with retained local history"
require_file_text "$work/policy-journeys.log" '"event":"push_only_delete_refused"' \
  "push-only refused delete survived process restart and retired"
require_file_text "$work/policy-journeys.log" '"event":"pull_only_overwrite"' \
  "pull-only local edit was replaced by the hub value"
require_file_text "$work/policy-journeys.log" '"event":"offline_delete_accepted"' \
  "offline delete survived process restart and converged across the fleet"
require_file_text "$work/policy-journeys.log" '"event":"offline_delete_refused"' \
  "conflicting offline delete retired and restored the hub winner"
require_file_text "$work/policy-journeys.log" '"event":"policy_journeys_complete"' \
  "installed policy journeys completed"

printf 'CHECK established-owner vectors reach SQL and ANN on every participant\n'
mkdir -p "$work/vector-enrichment"
"$timeout_bin" 180 "$driver" vector --root "$work/vector-enrichment" >"$work/vector-enrichment.log"
require_file_text "$work/vector-enrichment.log" '"event":"vector_enrichment_complete"' \
  "later vector enrichment reached the hub and both edges"
require_file_text "$work/vector-enrichment.log" \
  '"owner_present_before_enrichment":{"edge_a":true,"edge_b":true,"hub":true}' \
  "owner reached every participant before its vector was authored"
require_file_text "$work/vector-enrichment.log" \
  '"sql_exact":{"edge_a":true,"edge_b":true,"hub":true}' \
  "SQL returned the exact owner and vector on every participant"

printf 'CHECK authoritative PURGE destroys engine-held copies and blocks resurrection\n'
mkdir -p "$work/purge-journeys"
"$timeout_bin" 480 "$driver" purge --root "$work/purge-journeys" >"$work/purge-journeys.log"
require_file_text "$work/purge-journeys.log" '"event":"purge_copy_erasure"' \
  "standalone multi-row purge removed history, vectors, and media across the fleet"
require_file_text "$work/purge-journeys.log" '"event":"purge_edge_authority"' \
  "configured edge refused purge before connection and after restart"
require_file_text "$work/purge-journeys.log" '"event":"purge_stale_and_fresh"' \
  "stale descendants were refused while fresh same-key data was accepted"
require_file_text "$work/purge-journeys.log" '"event":"purge_recreated_generation"' \
  "removed table generations stayed refused while replacement data synchronized"
require_file_text "$work/purge-journeys.log" '"event":"purge_push_only_delivery"' \
  "authoritative purge crossed the push-only lane without ordinary pull"
require_file_text "$work/purge-journeys.log" '"event":"purge_shared_media"' \
  "shared media named its survivor and disappeared after the final referent"
require_file_text "$work/purge-journeys.log" '"event":"purge_pre_backup_restore"' \
  "peer tombstone refused a pre-purge backup until the operator reissued purge"
require_file_text "$work/purge-journeys.log" '"event":"purge_wrong_hub_refused"' \
  "a non-bound hub could not mutate pull, push, or purge authority"
require_file_text "$work/purge-journeys.log" '"event":"purge_forged_push_refused"' \
  "authenticated push-borne purge was atomically refused"
require_file_text "$work/purge-journeys.log" '"event":"purge_journeys_complete"' \
  "installed purge journeys completed"

printf 'CHECK exact authored DDL vector survives hub, restart, pull, and wipe-restore\n'
positive="$work/ddl-positive"
start_hub "$positive/hub" core observe
"$driver" ddl-source \
  --db "$positive/source.db" --identity "$positive/source.identity" \
  --ticket-file "$positive/hub/ticket" --tenant-id installed-smoke \
  --phase author-push --expect success >"$positive/source.log"
require_file_text "$positive/source.log" \
  '"order":["create_table","create_trigger","alter_table"]' \
  "source printed the exact authored DDL vector"
"$driver" ddl-source \
  --db "$positive/edge.db" --identity "$positive/edge.identity" \
  --ticket-file "$positive/hub/ticket" --tenant-id installed-smoke \
  --phase pull-inspect --expect success >"$positive/edge.log"
require_file_text "$positive/edge.log" '"event":"received_ddl_vector"' \
  "first edge received the exact DDL vector"
stop_hub
"$driver" ddl-source \
  --db "$positive/hub/hub.db" --identity "$positive/hub/hub.identity" \
  --ticket-file "$positive/hub/ticket" --tenant-id installed-smoke \
  --phase inspect-local --expect success >"$positive/hub-vector.log"
require_file_text "$positive/hub-vector.log" \
  '"order":["create_table","create_trigger","alter_table"]' \
  "hub retained the exact authored DDL vector"
source_schema="$(schema_json "$positive/source.db")"
hub_schema="$(schema_json "$positive/hub/hub.db")"
edge_schema="$(schema_json "$positive/edge.db")"
[[ "$source_schema" == "$hub_schema" && "$source_schema" == "$edge_schema" ]] \
  || fail "source, hub, and first edge render identical schema"
pass "source, hub, and first edge render identical schema"
start_hub "$positive/hub" core observe
"$driver" ddl-source \
  --db "$positive/edge.db" --identity "$positive/edge.identity" \
  --ticket-file "$positive/hub/ticket" --tenant-id installed-smoke \
  --phase pull-inspect --expect success >"$positive/restarted-edge.log"
"$driver" ddl-source \
  --db "$positive/wiped-edge.db" --identity "$positive/wiped-edge.identity" \
  --ticket-file "$positive/hub/ticket" --tenant-id installed-smoke \
  --phase pull-inspect --expect success >"$positive/wiped-edge.log"
stop_hub
require_file_text "$positive/restarted-edge.log" '"event":"received_ddl_vector"' \
  "restarted edge retained the exact DDL vector"
require_file_text "$positive/wiped-edge.log" '"event":"received_ddl_vector"' \
  "wiped edge restored the exact DDL vector"
[[ "$source_schema" == "$(schema_json "$positive/edge.db")" \
   && "$source_schema" == "$(schema_json "$positive/wiped-edge.db")" ]] \
  || fail "restart and wipe-restore render the authored schema identically"
pass "restart and wipe-restore render the authored schema identically"

printf 'CHECK every receiving-plugin DDL rewrite is refused with no durable mutation\n'
for receiver in ddl-add ddl-remove ddl-replace ddl-reorder; do
  case_root="$work/rewrite-$receiver"
  start_hub "$case_root/hub" core observe
  stop_hub
  baseline="$(snapshot_state "$case_root/hub/hub.db" "$case_root/before.snapshot")"
  start_hub "$case_root/hub" "$receiver" observe
  "$driver" ddl-source \
    --db "$case_root/source.db" --identity "$case_root/source.identity" \
    --ticket-file "$case_root/hub/ticket" --tenant-id installed-smoke \
    --phase author-push --expect immutable-ddl-refusal >"$case_root/source.log"
  stop_hub
  after="$(snapshot_state "$case_root/hub/hub.db" "$case_root/after.snapshot")"
  [[ "$baseline" == "$after" ]] || fail "$receiver refusal left every durable apply category unchanged"
  require_file_text "$case_root/source.log" \
    'authenticated received DDL is immutable after transport validation' \
    "$receiver received the typed immutable-DDL refusal"
  pass "$receiver refusal left every durable apply category unchanged"
done

printf 'CHECK oversized dependency unit resumes after fragment-persisted hub death\n'
fragment="$work/oversized-fragment"
start_hub "$fragment/hub" core observe
"$driver" oversized-source \
  --db "$fragment/edge.db" --identity "$fragment/edge.identity" \
  --ticket-file "$fragment/hub/ticket" --tenant-id installed-smoke \
  --phase bootstrap-and-seed >"$fragment/prepare.log"
stop_hub
fragment_baseline="$(snapshot_state "$fragment/hub/hub.db" "$fragment/before.snapshot")"
start_hub "$fragment/hub" core after-fragment0
"$timeout_bin" 180 "$driver" oversized-source \
  --db "$fragment/edge.db" --identity "$fragment/edge.identity" \
  --ticket-file "$fragment/hub/ticket" --tenant-id installed-smoke \
  --phase push-existing >"$fragment/interrupted.log" 2>"$fragment/interrupted.stderr" &
edge_pid=$!
checkpoint="$(read_hub_event)"
require_text "$checkpoint" '"event":"durable_request_fragment"' \
  "hub reported the durably persisted first request fragment"
require_text "$checkpoint" '"sequence":0' "checkpoint names actual fragment sequence zero"
kill_hub_at_checkpoint
wait "$edge_pid" 2>/dev/null && fail "interrupted oversized push remained unconfirmed"
require_file_text "$fragment/interrupted.log" '"event":"oversized_push_unconfirmed"' \
  "edge reported the interrupted push as unconfirmed"
fragment_watermark_before="$(json_file_number "$fragment/interrupted.log" push_watermark_before)"
fragment_watermark_after="$(json_file_number "$fragment/interrupted.log" push_watermark_after)"
[[ "$fragment_watermark_before" -eq "$fragment_watermark_after" ]] \
  || fail "fragment interruption advanced the edge push watermark without confirmation"
pass "fragment interruption left the edge push watermark unadvanced"
fragment_after="$(snapshot_state "$fragment/hub/hub.db" "$fragment/after-kill.snapshot")"
[[ "$fragment_baseline" == "$fragment_after" ]] \
  || fail "partial request changed hub rows, schema, lineage, receipt, cursor, or watermark"
pass "partial request changed no hub durable apply state"
start_hub "$fragment/hub" core observe
"$driver" oversized-source \
  --db "$fragment/edge.db" --identity "$fragment/edge.identity" \
  --ticket-file "$fragment/hub/ticket" --tenant-id installed-smoke \
  --phase push-existing >"$fragment/resumed.log"
stop_hub
require_file_text "$fragment/resumed.log" '"event":"oversized_push_confirmed"' \
  "resumed oversized unit received final confirmation"
fragment_source_lsn="$(json_file_number "$fragment/resumed.log" source_lsn)"
fragment_push_before="$(json_file_number "$fragment/resumed.log" push_watermark_before)"
fragment_push_after="$(json_file_number "$fragment/resumed.log" push_watermark_after)"
[[ "$fragment_push_after" -eq "$fragment_source_lsn" \
   && "$fragment_push_after" -gt "$fragment_push_before" ]] \
  || fail "confirmed resumed unit advanced the edge watermark to its exact source position"
pass "confirmed resumed unit advanced the edge watermark to its exact source position"
fragment_final="$(snapshot_state "$fragment/hub/hub.db" "$fragment/final.snapshot")"
fragment_before_lsn="$(json_number "$fragment_baseline" current_lsn)"
fragment_final_lsn="$(json_number "$fragment_final" current_lsn)"
[[ "$fragment_final_lsn" -eq $((fragment_before_lsn + 1)) ]] \
  || fail "resumed dependency unit committed at exactly one hub position"
pass "resumed dependency unit committed at exactly one hub position"

printf 'CHECK oversized dependency unit reconciles after committed hub dies before success reply\n'
lost_ack="$work/oversized-lost-ack"
start_hub "$lost_ack/hub" core observe
"$driver" oversized-source \
  --db "$lost_ack/edge.db" --identity "$lost_ack/edge.identity" \
  --ticket-file "$lost_ack/hub/ticket" --tenant-id installed-smoke \
  --phase bootstrap-and-seed >"$lost_ack/prepare.log"
stop_hub
lost_baseline="$(snapshot_state "$lost_ack/hub/hub.db" "$lost_ack/before.snapshot")"
start_hub "$lost_ack/hub" core after-apply
"$timeout_bin" 180 "$driver" oversized-source \
  --db "$lost_ack/edge.db" --identity "$lost_ack/edge.identity" \
  --ticket-file "$lost_ack/hub/ticket" --tenant-id installed-smoke \
  --phase push-existing >"$lost_ack/interrupted.log" 2>"$lost_ack/interrupted.stderr" &
edge_pid=$!
checkpoint="$(read_hub_event)"
require_text "$checkpoint" '"event":"completed_apply_before_reply"' \
  "hub reported committed apply before the success reply"
require_text "$checkpoint" '"dependency_complete":true' \
  "lost-ack checkpoint belongs to the dependency-complete unit"
require_text "$checkpoint" '"response_success":true' \
  "lost-ack checkpoint is success-only"
kill_hub_at_checkpoint
wait "$edge_pid" 2>/dev/null && fail "lost-final-ack push remained unconfirmed"
require_file_text "$lost_ack/interrupted.log" '"event":"oversized_push_unconfirmed"' \
  "edge did not advance on a lost final acknowledgement"
lost_watermark_before="$(json_file_number "$lost_ack/interrupted.log" push_watermark_before)"
lost_watermark_after="$(json_file_number "$lost_ack/interrupted.log" push_watermark_after)"
[[ "$lost_watermark_before" -eq "$lost_watermark_after" ]] \
  || fail "lost final acknowledgement advanced the edge push watermark without confirmation"
pass "lost final acknowledgement left the edge push watermark unadvanced"
lost_committed="$(snapshot_state "$lost_ack/hub/hub.db" "$lost_ack/committed.snapshot")"
lost_before_lsn="$(json_number "$lost_baseline" current_lsn)"
lost_committed_lsn="$(json_number "$lost_committed" current_lsn)"
[[ "$lost_committed_lsn" -eq $((lost_before_lsn + 1)) ]] \
  || fail "lost-ack unit had exactly one committed hub position"
pass "lost-ack unit had exactly one committed hub position"
start_hub "$lost_ack/hub" core observe
"$driver" oversized-source \
  --db "$lost_ack/edge.db" --identity "$lost_ack/edge.identity" \
  --ticket-file "$lost_ack/hub/ticket" --tenant-id installed-smoke \
  --phase push-existing >"$lost_ack/reconciled.log"
stop_hub
lost_final="$(snapshot_state "$lost_ack/hub/hub.db" "$lost_ack/final.snapshot")"
[[ "$lost_committed" == "$lost_final" ]] \
  || fail "lost-ack reconciliation duplicated or otherwise mutated the committed unit"
require_file_text "$lost_ack/reconciled.log" '"event":"oversized_push_confirmed"' \
  "edge reconciled the already-committed unit"
lost_source_lsn="$(json_file_number "$lost_ack/reconciled.log" source_lsn)"
lost_push_before="$(json_file_number "$lost_ack/reconciled.log" push_watermark_before)"
lost_push_after="$(json_file_number "$lost_ack/reconciled.log" push_watermark_after)"
[[ "$lost_push_after" -eq "$lost_source_lsn" && "$lost_push_after" -gt "$lost_push_before" ]] \
  || fail "reconciliation advanced the edge watermark to the confirmed source position"
pass "reconciliation advanced the edge watermark to the confirmed source position"
pass "lost-ack reconciliation made no second hub commit"

printf 'CHECK fitting dependency and unrelated ordinary work stay on one request with no staging\n'
for fixture in fitting-dependency ordinary; do
  request_root="$work/request-path-$fixture"
  start_hub "$request_root/hub" core observe
  "$driver" oversized-source \
    --db "$request_root/edge.db" --identity "$request_root/edge.identity" \
    --ticket-file "$request_root/hub/ticket" --tenant-id installed-smoke \
    --phase bootstrap-and-seed --fixture "$fixture" >"$request_root/prepare.log"
  stop_hub
  start_hub "$request_root/hub" core observe
  "$driver" oversized-source \
    --db "$request_root/edge.db" --identity "$request_root/edge.identity" \
    --ticket-file "$request_root/hub/ticket" --tenant-id installed-smoke \
    --phase push-existing --fixture "$fixture" >"$request_root/push.log"
  request_path="$(read_hub_event)"
  require_text "$request_path" '"event":"push_request_path"' \
    "$fixture emitted a production request-path observation"
  require_text "$request_path" '"chunked":false' \
    "$fixture used one ordinary request without durable staging"
  stop_hub
  fixture_source_lsn="$(json_file_number "$request_root/push.log" source_lsn)"
  fixture_push_after="$(json_file_number "$request_root/push.log" push_watermark_after)"
  [[ "$fixture_push_after" -eq "$fixture_source_lsn" ]] \
    || fail "$fixture advanced to its exact confirmed source position"
  pass "$fixture advanced to its exact confirmed source position"
done

printf 'PASS installed release authenticated schema and oversized durability smoke\n'
