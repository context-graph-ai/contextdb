//! Fixed outgoing work must not retain excluded lineage or row bodies, and
//! suppressing one owner must preserve another owner's same-commit vector.
#![expect(
    clippy::result_large_err,
    reason = "Measured sync operations return the public Error with inline refusal diagnostics."
)]
use super::outbound_declaration_walk_support::*;
use contextdb_core::{Lsn, Value};
use std::collections::HashMap;

fn assert_declared_indexed_key(db: &contextdb_engine::Database, table: &str) {
    let meta = db.table_meta(table).expect("declared keyed table");
    assert!(
        meta.primary_key_columns.iter().any(|column| column == "id")
            || meta
                .columns
                .iter()
                .any(|column| column.name == "id" && column.primary_key)
    );
    assert!(
        db.column_has_covering_index(table, "id"),
        "this regression exercises the authenticated indexed identity route, not the deferred no-exact-index case"
    );
}

// The physical allocator is process-wide. Each row runs alone in a fresh
// child, including its runtime workers, so another test cannot reset its peak.
fn child(row: &str) -> bool {
    const ARM: &str = "CONTEXTDB_OUTBOUND_SELECTION_REGRESSION";
    if std::env::var(ARM).as_deref() == Ok(row) {
        return true;
    }
    let filter = crate::child_test_filter(module_path!(), row);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            filter.as_str(),
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(ARM, row)
        .status()
        .unwrap();
    assert!(
        status.success(),
        "isolated outbound regression failed: {row}: {status}"
    );
    false
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct LineageSample {
    pending_charge: usize,
    pending_heap: usize,
    pending_held: usize,
    push_charge: usize,
    push_heap: usize,
    push_held: usize,
    pre_reply: usize,
    lsn: Lsn,
}

#[cfg(target_os = "linux")]
fn lineage_sample(records: usize) -> LineageSample {
    use super::writer_reopen_and_doubling_keep_a_fixed_resident_envelope::{
        allocation_peak_for_test, live_allocations_for_test, reset_allocation_peak_for_test,
    };
    use redb::ReadableDatabase;
    let _clock = fixed_clock();
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    let edge = Peer::new(directory.path(), "edge", 1);
    let hub = Peer::new(directory.path(), "hub", 2);
    sql(
        &edge.db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY) SYNC TWO WAY",
    );
    sql(
        &edge.db,
        "CREATE TABLE ballast (id INTEGER PRIMARY KEY) SYNC TWO WAY",
    );
    assert_declared_indexed_key(&edge.db, "ballast");
    assert_declared_indexed_key(&edge.db, "notes");
    sql(
        &edge.db,
        "CREATE TABLE local_only (n INTEGER, body TEXT) SYNC OFF",
    );
    one_commit(
        &edge.db,
        &(0..records)
            .flat_map(|id| {
                [
                    format!("INSERT INTO ballast VALUES ({id})"),
                    format!("INSERT INTO local_only VALUES ({id}, 'excluded')"),
                ]
            })
            .collect::<Vec<_>>(),
    );
    let network = Network::connect(&runtime, edge, hub);
    let initial = runtime
        .block_on(network.client.push())
        .expect("seed rows reach the hub");
    assert_eq!(initial.applied_rows, records);
    one_commit(
        &network.edge.db,
        &(0..records)
            .map(|id| format!("DELETE FROM ballast WHERE id = {id}"))
            .collect::<Vec<_>>(),
    );
    runtime
        .block_on(network.client.push())
        .expect("confirm all ballast deletions");
    assert!(ids(&network.hub.db, "ballast").is_empty());
    assert!(
        network
            .hub
            .db
            .execute("SELECT n FROM local_only", &HashMap::new())
            .unwrap()
            .rows()
            .is_empty(),
        "keyless SYNC OFF ballast is never delivered"
    );
    assert_eq!(network.client.pending_push_change_count().unwrap(), 0);
    let frontier = network.client.push_watermark();
    network.close(&runtime);
    // Inspect real durable config without rewriting it. This proves the
    // ballast is lineage records, not merely row or DDL history.
    let raw = redb::Database::open(directory.path().join("edge.redb")).unwrap();
    let read = raw.begin_read().unwrap();
    let config = read
        .open_table(redb::TableDefinition::<&str, &[u8]>::new("config"))
        .unwrap();
    let persisted = config
        .range("sync_lineage.v1.record."..)
        .unwrap()
        .map(|entry| entry.unwrap().0.value().to_owned())
        .take_while(|key| key.starts_with("sync_lineage.v1.record."))
        .count();
    assert_eq!(
        persisted, records,
        "fixture holds exactly the confirmed durable lineage records"
    );
    drop(config);
    drop(read);
    drop(raw);
    let edge = Peer::reopen(directory.path(), "edge");
    let hub = Peer::reopen(directory.path(), "hub");
    let network = Network::connect(&runtime, edge, hub);
    assert_eq!(network.client.push_watermark(), frontier);
    // Warm ordinary storage caches with zero output; decoded collectors must
    // still die on return, and their NEXT operation's peak remains observable.
    for _ in 0..2 {
        assert_eq!(network.client.pending_push_change_count().unwrap(), 0);
    }
    sql(&network.edge.db, "INSERT INTO notes VALUES (999)");
    let lsn = network.edge.db.current_lsn();
    let charged_before = network.edge.db.accountant().usage().used;
    let heap_before = live_allocations_for_test();
    reset_allocation_peak_for_test();
    let pending = measure(&network.edge.db, || {
        network.client.pending_push_change_count()
    });
    let pending_heap = allocation_peak_for_test().saturating_sub(heap_before);
    let pending_held = network
        .edge
        .db
        .accountant()
        .usage()
        .used
        .saturating_sub(charged_before);
    assert_eq!(
        pending.value.unwrap(),
        1,
        "only the fixed new row is pending"
    );
    assert_eq!(network.client.push_watermark(), frontier);
    assert_eq!(
        pending_held, 0,
        "pending selection returns its charged custody"
    );
    let charged_before = network.edge.db.accountant().usage().used;
    let heap_before = live_allocations_for_test();
    reset_allocation_peak_for_test();
    let pushed = measure(&network.edge.db, || runtime.block_on(network.client.push()));
    let push_heap = allocation_peak_for_test().saturating_sub(heap_before);
    let push_held = network
        .edge
        .db
        .accountant()
        .usage()
        .used
        .saturating_sub(charged_before);
    assert_eq!(
        pushed
            .value
            .as_ref()
            .expect("fixed delta push succeeds")
            .applied_rows,
        1
    );
    assert_eq!(ids(&network.hub.db, "notes"), vec![999]);
    assert!(ids(&network.hub.db, "ballast").is_empty());
    assert!(
        network
            .hub
            .db
            .execute("SELECT n FROM local_only", &HashMap::new())
            .unwrap()
            .rows()
            .is_empty(),
        "keyless SYNC OFF ballast is never delivered"
    );
    let sample = LineageSample {
        pending_charge: pending.peak,
        pending_heap,
        pending_held,
        push_charge: pushed.peak,
        push_heap,
        push_held,
        pre_reply: pushed
            .pre_reply_peak
            .expect("the pre-reply boundary was observed"),
        lsn,
    };
    eprintln!("LINEAGE_FIXED_OUTPUT records={records} {sample:?}");
    network.close(&runtime);
    sample
}

#[cfg(target_os = "linux")]
#[test]
fn pending_and_push_memory_stays_flat_as_confirmed_lineage_grows() {
    if !child("pending_and_push_memory_stays_flat_as_confirmed_lineage_grows") {
        return;
    }
    let small = lineage_sample(16);
    let large = lineage_sample(160);
    assert_eq!(
        small.lsn, large.lsn,
        "the fixed delta has the same source position"
    );
    // Pending reads have no reply write. Push's pre-reply charge isolates
    // selection; the full push additionally retains the existing named
    // maximum config-page allowance for the separate CDB-249 storage defect.
    const WINDOW: usize = 4096;
    const REPLY_PAGE: usize = 65_664; // CDB-249, one existing maximum config page.
    const PAGE_INVOICE: usize = 4_224; // Existing fixed page plus owner/header invoice.
    let pending_charge_ok = large.pending_charge <= small.pending_charge + WINDOW;
    let pending_heap_ok = large.pending_heap <= small.pending_heap + WINDOW;
    let push_charge_ok = large.pre_reply <= small.pre_reply + WINDOW;
    let push_heap_ok = large.push_heap <= small.push_heap + REPLY_PAGE + 2 * PAGE_INVOICE;
    let push_whole_ok = large.push_charge <= small.push_charge + REPLY_PAGE + 2 * PAGE_INVOICE;
    let held_ok = large.push_held <= small.push_held + WINDOW
        && small.pending_held == 0
        && large.pending_held == 0;
    eprintln!(
        "LINEAGE_FLAT pending_charge={pending_charge_ok} pending_heap={pending_heap_ok} push_pre_reply={push_charge_ok} push_heap={push_heap_ok} push_whole={push_whole_ok} held={held_ok} small={small:?} large={large:?}"
    );
    assert!(
        pending_charge_ok
            && pending_heap_ok
            && push_charge_ok
            && push_heap_ok
            && push_whole_ok
            && held_ok,
        "pending reads and pushes must not hold the confirmed durable lineage prefix: small={small:?}, large={large:?}"
    );
}

fn payload_sample(bytes: usize, same_key: bool, keyless: bool) -> usize {
    let _clock = fixed_clock();
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    let edge = Peer::new(directory.path(), "edge", 1);
    let hub = Peer::new(directory.path(), "hub", 2);
    sql(
        &edge.db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
    );
    sql(
        &edge.db,
        "CREATE TABLE sent (id INTEGER PRIMARY KEY) SYNC TWO WAY",
    );
    assert_declared_indexed_key(&edge.db, "notes");
    sql(
        &edge.db,
        "CREATE TABLE local_only (n INTEGER, body TEXT) SYNC OFF",
    );
    let network = Network::connect(&runtime, edge, hub);
    runtime.block_on(network.client.push()).unwrap();
    let frontier = network.client.push_watermark();
    one_commit(
        &network.edge.db,
        &[
            "INSERT INTO notes VALUES (1, 'old-one')".into(),
            "INSERT INTO notes VALUES (2, 'kept-two')".into(),
        ],
    );
    if !keyless {
        sql(&network.edge.db, "ALTER TABLE notes SET SYNC OFF");
    }
    let key = if same_key { 1 } else { 3 };
    network
        .edge
        .db
        .execute(
            if keyless {
                "INSERT INTO local_only VALUES ($id, $body)"
            } else if same_key {
                "UPDATE notes SET body = $body WHERE id = $id"
            } else {
                "INSERT INTO notes VALUES ($id, $body)"
            },
            &HashMap::from([
                ("id".into(), Value::Int64(key)),
                ("body".into(), Value::Text("x".repeat(bytes))),
            ]),
        )
        .expect("write excluded body at the setup budget");
    sql(&network.edge.db, "INSERT INTO sent VALUES (9)");
    const PUSH_BUDGET: usize = 4 * 1024 * 1024;
    if bytes > 1024 {
        assert!(
            bytes > PUSH_BUDGET,
            "unsent body is larger than the entire push budget"
        );
    }
    // Drop every source handle and its caches. The measured push is the
    // first row read on a fresh writable open under the fixed operation cap.
    network.close(&runtime);
    let source_path = directory.path().join("edge.redb");
    let edge = Peer {
        db: std::sync::Arc::new(
            contextdb_engine::Database::open_with_options(
                &source_path,
                contextdb_engine::DatabaseOpenOptions {
                    memory_limit: Some(PUSH_BUDGET),
                    maintenance_policy: contextdb_engine::MaintenancePolicy::CallerDriven,
                    ..Default::default()
                },
            )
            .expect("cold writable open at the fixed push budget"),
        ),
        path: source_path,
        identity: std::sync::Arc::new(
            contextdb_server::FabricIdentity::load_or_generate(
                &directory.path().join("edge.identity"),
            )
            .unwrap(),
        ),
    };
    let hub = Peer::reopen(directory.path(), "hub");
    let network = Network::connect(&runtime, edge, hub);
    assert_eq!(
        network.edge.db.accountant().usage().limit,
        Some(PUSH_BUDGET)
    );
    assert_eq!(network.client.push_watermark(), frontier);
    let (pushed, pages) = redb::observe_page_lifetimes_for_test(|| {
        measure(&network.edge.db, || runtime.block_on(network.client.push()))
    });
    eprintln!(
        "EXCLUDED_BODY bytes={bytes} same_key={same_key} keyless={keyless} peak={} source_pages={pages:?} result={:?}",
        pushed.peak, pushed.value
    );
    assert!(
        pages.peak_bytes <= PUSH_BUDGET as i128,
        "cold source pages must fit the fixed operation budget: {pages:?}"
    );
    let result = pushed
        .value
        .expect("an excluded non-key payload must not make a fixed-output push refuse");
    let expected = if same_key && !keyless {
        vec![2]
    } else {
        vec![1, 2]
    };
    assert_eq!(result.applied_rows, expected.len() + 1);
    assert_eq!(
        ids(&network.hub.db, "notes"),
        expected,
        "supersession removes only the excluded replacement's older key"
    );
    assert_eq!(ids(&network.hub.db, "sent"), vec![9]);
    assert!(
        network
            .hub
            .db
            .execute("SELECT n FROM local_only", &HashMap::new())
            .unwrap()
            .rows()
            .is_empty()
    );
    assert_eq!(
        network.edge.db.accountant().usage().limit,
        Some(PUSH_BUDGET)
    );
    network.close(&runtime);
    pushed.peak
}

#[test]
#[ignore = "CDB-265: post-extraction push consumers reread excluded bodies"]
fn an_excluded_body_larger_than_the_push_budget_does_not_charge_the_push() {
    if !child("an_excluded_body_larger_than_the_push_budget_does_not_charge_the_push") {
        return;
    }
    let mut all_cases_pass = true;
    for (same_key, keyless) in [(false, true), (true, false), (false, false)] {
        // Exercise both identities even if the first larger body refuses.
        // No caught panic can pass: the final assertion requires every case.
        let small = std::panic::catch_unwind(|| payload_sample(16, same_key, keyless));
        let large = std::panic::catch_unwind(|| payload_sample(5 * 1024 * 1024, same_key, keyless));
        let bounded = match (&small, &large) {
            (Ok(small), Ok(large)) => *large <= *small + 4096,
            _ => false,
        };
        eprintln!(
            "EXCLUDED_BODY_CASE same_key={same_key} keyless={keyless} small_ok={} large_ok={} bounded={bounded}",
            small.is_ok(),
            large.is_ok()
        );
        all_cases_pass &= bounded;
    }
    assert!(
        all_cases_pass,
        "both cold excluded-body identities must push at a fixed charge, including a body larger than the budget"
    );
}

fn same_commit_vectors(replace_one: bool) {
    let _clock = fixed_clock();
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    let edge = Peer::new(directory.path(), "edge", 1);
    let hub = Peer::new(directory.path(), "hub", 2);
    sql(
        &edge.db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, v VECTOR(3) SEARCH_MODE EXACT) SYNC TWO WAY",
    );
    assert_declared_indexed_key(&edge.db, "notes");
    sql(
        &edge.db,
        "CREATE TABLE local_only (n INTEGER, v VECTOR(3) SEARCH_MODE EXACT) SYNC OFF",
    );
    let network = Network::connect(&runtime, edge, hub);
    network.client.set_peer_vector_schema_support_for_test(true);
    runtime.block_on(network.client.push()).unwrap();
    let frontier = network.client.push_watermark();
    // Keep the row's F32 slot NULL and write its vector through the public
    // vector API in the SAME transaction. An inline copy would let the hub
    // reconstruct a vector omitted by the outbound cleanup and mask the bug.
    let tx = network.edge.db.begin().unwrap();
    let mut owners = Vec::new();
    for (id, vector) in [(1, vec![1.0, 0.0, 0.0]), (2, vec![0.0, 1.0, 0.0])] {
        let owner = network
            .edge
            .db
            .insert_row(
                tx,
                "notes",
                HashMap::from([("id".into(), Value::Int64(id)), ("v".into(), Value::Null)]),
            )
            .unwrap();
        network
            .edge
            .db
            .insert_vector(
                tx,
                contextdb_core::VectorIndexRef::new("notes", "v"),
                owner,
                vector,
            )
            .unwrap();
        owners.push(owner);
    }
    assert_ne!(owners[0], owners[1]);
    let local_owner = network
        .edge
        .db
        .insert_row(
            tx,
            "local_only",
            HashMap::from([("n".into(), Value::Int64(7)), ("v".into(), Value::Null)]),
        )
        .unwrap();
    network
        .edge
        .db
        .insert_vector(
            tx,
            contextdb_core::VectorIndexRef::new("local_only", "v"),
            local_owner,
            vec![0.5, 0.5, 0.0],
        )
        .unwrap();
    network.edge.db.commit(tx).unwrap();
    let shared_lsn = network.edge.db.current_lsn();
    if replace_one {
        sql(&network.edge.db, "ALTER TABLE notes SET SYNC OFF");
        network
            .edge
            .db
            .execute(
                "UPDATE notes SET v = $v WHERE id = 1",
                &HashMap::from([("v".into(), Value::Vector(vec![0.0, 0.0, 1.0]))]),
            )
            .unwrap();
        assert!(shared_lsn < network.edge.db.current_lsn());
    }
    assert!(frontier < shared_lsn);
    let before_push = network.broker.recorded_exchanges().len();
    let pushed = runtime
        .block_on(network.client.push())
        .expect("push the unaffected owner");
    assert_eq!(pushed.applied_rows, if replace_one { 1 } else { 2 });
    assert_eq!(
        ids(&network.hub.db, "notes"),
        if replace_one { vec![2] } else { vec![1, 2] }
    );
    assert!(
        network
            .hub
            .db
            .execute("SELECT n FROM local_only", &HashMap::new())
            .unwrap()
            .rows()
            .is_empty(),
        "same-commit keyless vector ballast stays local"
    );
    let mut sent_vectors = Vec::new();
    for exchange in network
        .broker
        .recorded_exchanges()
        .into_iter()
        .skip(before_push)
    {
        let envelope = contextdb_engine::protocol::decode(&exchange.request_bytes).unwrap();
        if matches!(
            envelope.message_type,
            contextdb_engine::protocol::MessageType::PushRequest
                | contextdb_engine::protocol::MessageType::DependencyCompletePushRequest
        ) {
            let request: contextdb_engine::protocol::PushRequest =
                rmp_serde::from_slice(&envelope.payload).unwrap();
            sent_vectors.extend(request.changeset.vectors);
        }
    }
    eprintln!(
        "SAME_COMMIT_SENT_VECTORS replaced={replace_one} owners={owners:?} shared_lsn={} vectors={sent_vectors:?}",
        shared_lsn.0
    );
    let wire_kept = sent_vectors.iter().any(|vector| {
        vector.index.table == "notes"
            && vector.index.column == "v"
            && vector.row_id == owners[1]
            && vector.lsn == shared_lsn
            && vector.vector == vec![0.0, 1.0, 0.0]
    });
    assert!(
        !sent_vectors
            .iter()
            .any(|vector| vector.index.table == "local_only")
    );
    let kept = network
        .hub
        .db
        .scan("notes", network.hub.db.snapshot())
        .unwrap()
        .into_iter()
        .find(|row| row.values.get("id") == Some(&Value::Int64(2)))
        .unwrap();
    let vector = network
        .hub
        .db
        .live_vector_entry(kept.row_id, network.hub.db.snapshot())
        .unwrap();
    eprintln!(
        "SAME_COMMIT_RECEIVED_VECTOR replaced={replace_one} row={} wire_kept={wire_kept} vector={vector:?}",
        kept.row_id.0
    );
    assert!(
        wire_kept,
        "the push must carry the unaffected same-commit owner's exact vector"
    );
    assert_eq!(
        vector
            .expect("the receiving hub must retain the unaffected owner's vector")
            .vector,
        vec![0.0, 1.0, 0.0]
    );
    let answer = network
        .hub
        .db
        .execute(
            "SELECT id FROM notes ORDER BY v <=> $probe USE VECTOR EXACT LIMIT 10",
            &HashMap::from([("probe".into(), Value::Vector(vec![0.0, 1.0, 0.0]))]),
        )
        .unwrap();
    eprintln!(
        "SAME_COMMIT_VECTOR replaced={replace_one} shared_lsn={} applied={} search={:?}",
        shared_lsn.0,
        pushed.applied_rows,
        answer.rows()
    );
    assert_eq!(
        answer.rows(),
        if replace_one {
            vec![vec![Value::Int64(2)]]
        } else {
            vec![vec![Value::Int64(2)], vec![Value::Int64(1)]]
        },
        "the delivered vector is searchable under its own row identity"
    );
    network.close(&runtime);
}

#[test]
fn excluding_one_owner_preserves_the_other_same_commit_vector() {
    if !child("excluding_one_owner_preserves_the_other_same_commit_vector") {
        return;
    }
    same_commit_vectors(false); // Positive transport/SQL/vector control.
    same_commit_vectors(true);
}
