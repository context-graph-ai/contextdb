use super::*;
use std::fs;
use std::sync::{Arc, Barrier, Mutex};

static LEGACY_FINISH_LOCK_GAP: Mutex<Option<(Arc<Barrier>, Arc<Barrier>)>> = Mutex::new(None);

fn install_legacy_finish_lock_gap(entered: Arc<Barrier>, child_closed_scope: Arc<Barrier>) {
    let mut gap = LEGACY_FINISH_LOCK_GAP
        .lock()
        .expect("finish lock-gap observation mutex");
    assert!(
        gap.is_none(),
        "finish lock-gap observation already installed"
    );
    *gap = Some((entered, child_closed_scope));
}

pub(super) fn observe_legacy_finish_lock_gap() {
    let gap = LEGACY_FINISH_LOCK_GAP
        .lock()
        .expect("finish lock-gap observation mutex")
        .take();
    if let Some((entered, child_closed_scope)) = gap {
        entered.wait();
        child_closed_scope.wait();
    }
}

#[test]
fn native_scopes_bind_only_the_open_record_and_keep_legacy_json_equivalent() {
    // Native notes before any sink exists must neither resolve the environment
    // nor survive into the first real record.
    let directory = tempfile::tempdir().expect("temporary attribution directory");
    let output = directory.path().join("attribution.jsonl");
    {
        let scope = native_reclaim_meter_scope();
        note_native_graph_reclaims_over_budget();
        note_native_graph_reclaim_required_bytes(91);
        let meters = scope.finish();
        assert_eq!(meters.graph_reclaims_over_budget, 1);
    }
    assert!(
        !output.exists(),
        "a recordless scope must not open a sink file"
    );

    // The module resolves this process's sink once. This single mapped libtest
    // function owns its one file; nextest's process-per-test isolation is not
    // required, though it also isolates this static when that runner is used.
    unsafe { std::env::set_var("CONTEXTDB_FIND_ATTRIBUTION", &output) };

    begin("after-recordless-native-work");
    count("legacy", 2);
    finish();
    let lines = fs::read_to_string(&output).expect("legacy record file");
    let first: serde_json::Value =
        serde_json::from_str(lines.lines().next().expect("first line")).expect("legacy JSON");
    assert_eq!(first["counts"]["legacy"], 2);
    assert!(first["counts"].get("graph_reclaims_over_budget").is_none());

    // Nested scopes and an unwind each close exactly once. The outer scope is
    // restored after the inner closes, so its work is not counted twice.
    begin("nested-and-unwind");
    let outer = native_reclaim_meter_scope();
    note_native_graph_reclaims_over_budget();
    note_native_graph_reclaim_required_bytes(10);
    {
        let inner = native_reclaim_meter_scope();
        note_native_graph_reclaims_over_budget();
        note_native_graph_reclaim_required_bytes(20);
        note_native_graph_reclaim_available_bytes(5);
        note_native_sealed_generations_evicted(2);
        assert_eq!(inner.finish().graph_reclaims_over_budget, 1);
    }
    let unwind = std::panic::catch_unwind(|| {
        let _panic_scope = native_reclaim_meter_scope();
        note_native_graph_reclaims_over_budget();
        note_native_graph_reclaim_required_bytes(30);
        panic!("exercise scope drop during unwind");
    });
    assert!(unwind.is_err());
    note_native_graph_reclaim_available_bytes(7);
    assert_eq!(outer.finish().graph_reclaims_over_budget, 1);
    finish();
    let nested: serde_json::Value = serde_json::from_str(
        fs::read_to_string(&output)
            .expect("nested record file")
            .lines()
            .nth(1)
            .expect("nested line"),
    )
    .expect("native JSON");
    assert_eq!(nested["counts"]["graph_reclaims_over_budget"], 3);
    assert_eq!(nested["counts"]["graph_reclaim_required_bytes"], 60);
    assert_eq!(nested["counts"]["graph_reclaim_available_bytes"], 12);
    assert_eq!(nested["counts"]["sealed_generations_evicted"], 2);

    // A child has its own native meter but closes it into the currently open
    // global record. The barrier establishes that close before finish, without
    // a scheduling or elapsed-time assumption.
    begin("child-record");
    let barrier = Arc::new(Barrier::new(2));
    let child_barrier = Arc::clone(&barrier);
    let child = std::thread::spawn(move || {
        let scope = native_reclaim_meter_scope();
        note_native_graph_reclaims_over_budget();
        note_native_graph_reclaim_required_bytes(44);
        child_barrier.wait();
        scope.finish()
    });
    barrier.wait();
    assert_eq!(
        child
            .join()
            .expect("child scope")
            .graph_reclaim_required_bytes,
        44
    );
    finish();
    begin("later-record");
    finish();
    let child_lines: Vec<serde_json::Value> = fs::read_to_string(&output)
        .expect("child record file")
        .lines()
        .map(|line| serde_json::from_str(line).expect("record JSON"))
        .collect();
    assert_eq!(child_lines[2]["counts"]["graph_reclaim_required_bytes"], 44);
    assert!(
        child_lines[3]["counts"]
            .get("graph_reclaim_required_bytes")
            .is_none()
    );

    // A scope that closes after finish's first native selection but before
    // its legacy take must still be written through the native path. The two
    // barriers force that real lock gap; the child is joined only after finish.
    begin("cross-thread-lock-gap");
    let entered = Arc::new(Barrier::new(2));
    let child_closed_scope = Arc::new(Barrier::new(2));
    install_legacy_finish_lock_gap(Arc::clone(&entered), Arc::clone(&child_closed_scope));
    let child = std::thread::spawn(move || {
        let scope = native_reclaim_meter_scope();
        note_native_graph_reclaims_over_budget();
        note_native_graph_reclaim_required_bytes(44);
        entered.wait();
        let meters = scope.finish();
        child_closed_scope.wait();
        meters
    });
    finish();
    let meters = child.join().expect("lock-gap child scope");
    assert_eq!(meters.graph_reclaims_over_budget, 1);
    assert_eq!(meters.graph_reclaim_required_bytes, 44);
    begin("after-cross-thread-lock-gap");
    finish();
    let lock_gap_lines: Vec<serde_json::Value> = fs::read_to_string(&output)
        .expect("cross-thread record file")
        .lines()
        .map(|line| serde_json::from_str(line).expect("record JSON"))
        .collect();
    assert_eq!(lock_gap_lines[4]["counts"]["graph_reclaims_over_budget"], 1);
    assert_eq!(
        lock_gap_lines[4]["counts"]["graph_reclaim_required_bytes"],
        44
    );
    assert!(
        lock_gap_lines[5]["counts"]
            .get("graph_reclaims_over_budget")
            .is_none()
    );
    assert!(
        lock_gap_lines[5]["counts"]
            .get("graph_reclaim_required_bytes")
            .is_none()
    );

    // A final-pass observation is present even if it gave back zero bytes.
    begin("zero-serving-pass");
    let scope = native_reclaim_meter_scope();
    note_native_serving_page_give_back_reached_final_pass();
    assert!(scope.finish().serving_page_give_back_reached_final_pass);
    finish();
    let zero: serde_json::Value = serde_json::from_str(
        fs::read_to_string(&output)
            .expect("zero record file")
            .lines()
            .nth(6)
            .expect("zero line"),
    )
    .expect("zero JSON");
    assert_eq!(zero["counts"]["serving_page_bytes_given_back"], 0);

    // Native finish uses the borrowed writer. Its output has the same legacy
    // fields as ordinary finish, including duplicate-key-last and f64 JSON
    // handling; its deterministic writer order is byte-stable for one record.
    let entry = || {
        vec![
            ("duplicate".to_owned(), Field::Count(1)),
            ("fraction".to_owned(), Field::Millis(1.25)),
            ("duplicate".to_owned(), Field::Text("last".to_owned())),
        ]
    };
    begin("mixed");
    count("legacy", 4);
    count("graph_reclaims_over_budget", 9);
    note_partition(entry());
    note_load(entry());
    let scope = native_reclaim_meter_scope();
    note_native_graph_reclaims_over_budget();
    note_native_graph_reclaim_required_bytes(8);
    scope.finish();
    let stable_a = directory.path().join("stable-a.json");
    let stable_b = directory.path().join("stable-b.json");
    {
        let sink = SINK
            .get()
            .expect("initialized sink")
            .as_ref()
            .expect("enabled sink")
            .lock()
            .expect("sink lock");
        let record = sink.record.as_ref().expect("open mixed record");
        write_native_record(
            &mut fs::File::create(&stable_a).expect("first real file"),
            record,
            0,
        )
        .expect("first borrowed write");
        write_native_record(
            &mut fs::File::create(&stable_b).expect("second real file"),
            record,
            0,
        )
        .expect("second borrowed write");
    }
    assert_eq!(
        fs::read(&stable_a).expect("first bytes"),
        fs::read(&stable_b).expect("second bytes")
    );
    finish();
    begin("mixed");
    count("legacy", 4);
    count("graph_reclaims_over_budget", 9);
    note_partition(entry());
    note_load(entry());
    finish();
    let mixed_lines: Vec<serde_json::Value> = fs::read_to_string(&output)
        .expect("mixed record file")
        .lines()
        .map(|line| serde_json::from_str(line).expect("mixed JSON"))
        .collect();
    let mut native = mixed_lines[7].clone();
    let mut legacy = mixed_lines[8].clone();
    for value in [&mut native, &mut legacy] {
        value
            .as_object_mut()
            .expect("record object")
            .remove("total_ms");
    }
    let shared_native_and_legacy = native["counts"]["graph_reclaims_over_budget"]
        .as_u64()
        .expect("shared native and legacy count");
    assert_eq!(shared_native_and_legacy, 10);
    native["counts"]["graph_reclaims_over_budget"] =
        serde_json::Value::from(shared_native_and_legacy - 1);
    for native_key in [
        "graph_reclaim_required_bytes",
        "graph_reclaim_available_bytes",
        "sealed_generations_evicted",
        "serving_page_bytes_given_back",
        "serving_page_give_back_short",
    ] {
        native["counts"]
            .as_object_mut()
            .expect("native counts")
            .remove(native_key);
    }
    assert_eq!(native, legacy);
    assert_eq!(mixed_lines[7]["counts"]["graph_reclaims_over_budget"], 10);
    assert_eq!(mixed_lines[7]["partitions"][0]["duplicate"], "last");
    assert_eq!(mixed_lines[7]["partitions"][0]["fraction"], 1.25);

    // Unlike the pre-enablement row at the start of this test, this scope
    // closes after the sink exists but while no Record is open. Its native
    // observation must still be dropped and must not pollute a later record.
    let recordless = native_reclaim_meter_scope();
    note_native_graph_reclaims_over_budget();
    note_native_graph_reclaim_required_bytes(91);
    let meters = recordless.finish();
    assert_eq!(meters.graph_reclaims_over_budget, 1);
    assert_eq!(meters.graph_reclaim_required_bytes, 91);
    begin("after-enabled-recordless-scope");
    count("legacy", 2);
    finish();
    let later: serde_json::Value = serde_json::from_str(
        fs::read_to_string(&output)
            .expect("later record file")
            .lines()
            .last()
            .expect("later record"),
    )
    .expect("later record JSON");
    assert_eq!(later["counts"]["legacy"], 2);
    assert!(later["counts"].get("graph_reclaims_over_budget").is_none());
    assert!(
        later["counts"]
            .get("graph_reclaim_required_bytes")
            .is_none()
    );
}
