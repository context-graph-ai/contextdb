//! "Vector maintenance": initial construction, replacement and eligible
//! reclamation complete as a selected partition grows, alongside admitted writes and reads.
use crate::resident_history_support as support;
use contextdb_core::{Value, Wallclock};
use contextdb_engine::database::resident_history_test_seams::observe;
use contextdb_vector::VectorPartitionRef;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use support::*;

fn all_searches(db: &contextdb_engine::Database, ids: impl IntoIterator<Item = usize>, mode: &str) {
    for id in ids {
        assert_eq!(
            query_vector(db, "building", id, 128, mode),
            vec![vec![Value::Int64(id as i64)]],
            "every vector identity is searchable, including suffix {id}"
        );
    }
}
fn bounded_cycle(
    db: &contextdb_engine::Database,
    workspace: usize,
) -> (
    contextdb_core::Result<contextdb_engine::database::MaintenanceReport>,
    contextdb_engine::database::resident_history_test_seams::Work,
) {
    #[cfg(target_os = "linux")]
    let actual_before = {
        use crate::writer_reopen_and_doubling_keep_a_fixed_resident_envelope as allocation;
        allocation::reset_allocation_peak_for_test();
        (
            allocation::live_allocations_for_test(),
            db.__writer_cache_stats_for_test().unwrap().3,
        )
    };
    #[cfg(not(target_os = "linux"))]
    let _ = workspace;
    let result = observe(|| db.run_maintenance_cycle());
    #[cfg(target_os = "linux")]
    {
        use crate::writer_reopen_and_doubling_keep_a_fixed_resident_envelope as allocation;
        let peak = allocation::allocation_peak_for_test().saturating_sub(actual_before.0);
        let cache_growth = db
            .__writer_cache_stats_for_test()
            .unwrap()
            .3
            .saturating_sub(actual_before.1);
        assert!(
            peak.saturating_sub(cache_growth) <= workspace,
            "isolated real batch allocation, excluding observed shared-cache growth, exceeds workspace: peak={peak}, cache_growth={cache_growth}, workspace={workspace}"
        );
    }
    result
}

#[test]
fn paged_initial_replacement_and_reclamation_progress_under_writes() {
    if maintenance_crash_child() {
        return;
    }
    if isolated_test(
        module_path!(),
        "paged_initial_replacement_and_reclamation_progress_under_writes",
        "CONTEXTDB_MAINTENANCE_ALLOCATION_ARM",
    ) {
        return;
    }
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    current_only_maintenance_crash_cuts();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("maintenance.db");
    let db = Arc::new(open(&path, Some(64 * 1024 * 1024)));
    exec(
        &db,
        "CREATE TABLE building (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(128) PARTITION_KEY (scope) MAX_PARTITIONS 2 SEARCH_MODE INDEXED HNSW (M = 16, EF_CONSTRUCTION = 32)) HISTORY ALL",
    );
    const ROWS: usize = 1024;
    const WORKSPACE: usize = 256 * 1024;
    assert!(
        ROWS * 128 * std::mem::size_of::<f32>() > WORKSPACE,
        "the selected partition payload alone exceeds one admitted builder workspace"
    );
    for id in 0..ROWS {
        insert_vector(&db, "building", id, Some(0), 128);
    }
    // Superseded memberships are real rejected source inputs, not graph points.
    // A count/filter pass over retained history must be metered independently
    // of the live vectors returned to the builder.
    for _ in 0..4 {
        for id in 0..128 {
            exec(
                &db,
                &format!("UPDATE building SET scope = 0 WHERE id = {id}"),
            );
        }
    }
    let frontier = db.snapshot();
    let route = VectorPartitionRef::new(index("building"), partition(0));
    let receipt = db.writer_memory_ownership_receipt_for_test();
    // Cache ownership is fixed when the file opens; a smaller declared
    // limit must still fund that owner plus one batch.
    let limit = receipt.named_owner_total + WORKSPACE;
    db.set_memory_limit(Some(limit)).unwrap();
    let (first_batch, first_work) = bounded_cycle(&db, WORKSPACE);
    eprintln!("first finite batch: {first_batch:?}; source/ownership {first_work:?}");
    assert!(
        first_batch.is_ok(),
        "one admitted finite batch must return a report: {first_batch:?}"
    );
    let committed = db
        .vector_store_for_test()
        .partition_info(&route.index, &route.partition_key)
        .unwrap()
        .maintenance_progress;
    assert!(
        committed.is_some_and(|progress| progress.vectors_done > 0 && progress.vectors_done < ROWS),
        "a partition larger than the workspace must commit an unfinished source cursor, not refuse or build everything: {committed:?}; {first_batch:?}"
    );
    assert!(
        first_work.maintenance_source_keys > 0
            && first_work.maintenance_source_keys <= 64
            && first_work.maintenance_source_visits <= 512,
        "finite source batch observes positive work without scanning the partition: {first_work:?}"
    );
    assert!(
        first_work.maintenance_source_visits > 0 && first_work.maintenance_source_visits <= 512,
        "count, rejection and body-copy source doors must all fit one finite step: {first_work:?}"
    );
    assert!(
        first_work.workspace_peak > 0 && first_work.workspace_peak <= WORKSPACE,
        "a real bounded batch has positive builder ownership: {first_work:?}"
    );
    let committed = committed.unwrap();
    assert_eq!(
        committed.vectors_done, first_work.maintenance_source_keys,
        "durably completed point count matches the actual first source batch"
    );
    db.close().unwrap();
    drop(db);
    let db = Arc::new(open(&path, Some(limit)));
    let restored = db
        .vector_store_for_test()
        .partition_info(&route.index, &route.partition_key)
        .unwrap()
        .maintenance_progress;
    assert_eq!(
        restored,
        Some(committed),
        "successful batch cursor is durable before cancellation"
    );
    let pinned = db.pin_snapshot(frontier);
    // Admit writes/read barriers between committed batches, while that batch's
    // workspace is released. The cancellation pause need not fund another
    // transaction from the deliberately small builder allowance.
    for id in ROWS..ROWS + 4 {
        insert_vector(&db, "building", id, Some(0), 128);
    }
    assert_eq!(
        vector_rows(&db, "building", frontier, Some(0)),
        vector_oracle(0..ROWS, 128),
        "pinned input excludes admitted suffix writes"
    );

    // Actual construction reaches a deterministic insertion boundary, then the
    // same stop flag used by engine-owned maintenance cancels the active batch.
    let pause = Arc::new(
        db.__arm_vector_maintenance_progress_pause_for_test(&route, committed.vectors_done + 3),
    );
    let (reached, boundary) = std::sync::mpsc::channel();
    let waiting_pause = Arc::clone(&pause);
    let waiting_notice = reached.clone();
    let waiting = std::thread::spawn(move || {
        let observed = waiting_pause.wait_until_reached_blocking();
        waiting_notice.send(observed).unwrap();
        observed
    });
    let stop = Arc::new(AtomicBool::new(false));
    let worker_db = Arc::clone(&db);
    let worker_stop = Arc::clone(&stop);
    let worker = std::thread::spawn(move || {
        // A refused or panicking worker cannot strand the barrier waiter.
        struct Finished(std::sync::mpsc::Sender<bool>);
        impl Drop for Finished {
            fn drop(&mut self) {
                let _ = self.0.send(false);
            }
        }
        let _finished = Finished(reached);
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        observe(|| worker_db.__maintenance_with_stop_for_test(worker_stop))
    });
    if !boundary.recv().unwrap() {
        pause.release();
        waiting.join().unwrap();
        let outcome = worker.join();
        panic!("construction returned before the deterministic insertion boundary: {outcome:?}");
    }
    assert!(
        waiting.join().unwrap(),
        "the real insertion boundary was reached"
    );
    let partial = db
        .vector_store_for_test()
        .partition_info(&route.index, &route.partition_key)
        .unwrap()
        .maintenance_progress
        .unwrap();
    assert!(
        partial.vectors_done > committed.vectors_done && partial.vectors_done < ROWS,
        "resume must advance beyond committed keys before cancellation"
    );
    stop.store(true, Ordering::SeqCst);
    pause.release();
    let (cancelled, cancel_work) = worker.join().unwrap();
    eprintln!(
        "cancelled initial build {cancelled:?}; in-flight ownership {cancel_work:?}; captured progress {partial:?}"
    );
    assert!(
        cancel_work.workspace_peak <= WORKSPACE && cancel_work.maintenance_source_visits <= 512,
        "even an unfinished cancelled batch has bounded ownership/source work: {cancel_work:?}"
    );
    assert!(
        matches!(cancelled, Err(contextdb_core::Error::ReadCancelled)),
        "cancellation has its typed result: {cancelled:?}"
    );
    assert!(
        !db.vector_store_for_test()
            .partition_info(&route.index, &route.partition_key)
            .unwrap()
            .graph_available,
        "cancellation publishes no partial graph"
    );
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0,
        "cancelled batch releases owners"
    );
    drop(pinned);
    db.close().unwrap();
    drop(db);
    let db = open(&path, Some(64 * 1024 * 1024));
    let resume = db
        .vector_store_for_test()
        .partition_info(&route.index, &route.partition_key)
        .unwrap()
        .maintenance_progress;
    db.set_memory_limit(Some(limit)).unwrap();
    let mut committed_cursors = vec![committed.vectors_done];
    let mut completed_sources = first_work
        .maintenance_source_identities
        .into_iter()
        .flatten()
        .collect::<std::collections::HashSet<_>>();
    let mut initial_published = false;
    let mut reports = Vec::new();
    let mut workspace_peak = 0;
    for cycle in 0..16 {
        db.accountant().reset_high_water_for_test();
        let (result, work) = bounded_cycle(&db, WORKSPACE);
        workspace_peak = workspace_peak.max(work.workspace_peak);
        assert!(
            work.maintenance_source_keys <= 64 && work.maintenance_source_visits <= 512,
            "resumed maintenance skips completed source keys in bounded windows: {work:?}"
        );
        reports.push(format!("{cycle}: {result:?}; work={work:?}"));
        if !initial_published {
            for identity in work.maintenance_source_identities.into_iter().flatten() {
                assert!(
                    completed_sources.insert(identity),
                    "resumed initial construction must exclude committed source identities; repeated {identity:?}"
                );
            }
            initial_published = db
                .vector_store_for_test()
                .partition_info(&route.index, &route.partition_key)
                .unwrap()
                .graph_available;
        }
        if let Some(progress) = db
            .vector_store_for_test()
            .partition_info(&route.index, &route.partition_key)
            .unwrap()
            .maintenance_progress
        {
            let previous = *committed_cursors.last().unwrap();
            assert!(
                progress.vectors_done <= previous + work.maintenance_source_keys,
                "a committed cursor cannot advance without corresponding source bodies: {previous} -> {progress:?}; {work:?}"
            );
            committed_cursors.push(progress.vectors_done);
        }
        assert!(
            db.accountant().high_water_for_test() <= limit,
            "peak admission, including transient construction, stays inside the writer budget"
        );
        if cycle < 4 {
            insert_vector(&db, "building", ROWS + 4 + cycle, Some(0), 128);
        }
        assert_eq!(
            vector_rows(&db, "building", frontier, Some(0)),
            vector_oracle(0..ROWS, 128)
        );
        assert_eq!(
            query_vector(&db, "building", 0, 128, "EXACT"),
            vec![vec![Value::Int64(0)]]
        );
    }
    db.close().unwrap();
    drop(db);
    let db = open(&path, Some(limit));
    for cycle in 16..64 {
        let (result, work) = bounded_cycle(&db, WORKSPACE);
        workspace_peak = workspace_peak.max(work.workspace_peak);
        assert!(
            work.maintenance_source_keys <= 64 && work.maintenance_source_visits <= 512,
            "resumed maintenance skips completed source keys in bounded windows: {work:?}"
        );
        reports.push(format!("{cycle}: {result:?}; work={work:?}"));
    }
    assert_eq!(
        vector_rows(&db, "building", db.snapshot(), Some(0)),
        vector_oracle(0..ROWS + 8, 128)
    );
    all_searches(&db, 0..ROWS + 8, "EXACT");
    let info = db
        .vector_store_for_test()
        .partition_info(&route.index, &route.partition_key)
        .unwrap();
    eprintln!(
        "restart progress={resume:?}; workspace_peak={workspace_peak}; finite cycles:\n{}",
        reports.join("\n")
    );
    assert!(
        info.graph_available,
        "finite cycles must finish a partition exceeding the frozen workspace; repeated whole-build refusal is not durable progress"
    );
    assert!(
        resume == Some(committed),
        "cancellation discards the unfinished batch and restores exactly the last committed cursor: committed={committed:?}, in_flight={partial:?}, reopened={resume:?}"
    );
    assert!(
        committed_cursors.windows(2).all(|pair| pair[1] >= pair[0])
            && committed_cursors
                .iter()
                .any(|done| *done > committed.vectors_done),
        "resumed committed source cursors advance without restart-from-zero: {committed_cursors:?}"
    );
    assert!(
        workspace_peak > 0 && workspace_peak <= WORKSPACE,
        "peak resident builder bytes are one finite batch"
    );
    all_searches(&db, 0..ROWS + 8, "INDEXED");
    let generation = info.base_generation;
    let old = db.snapshot();
    let pin = db.pin_snapshot(old);
    for id in 0..128 {
        exec(&db, &format!("DELETE FROM building WHERE id = {id}"));
    }
    for _ in 0..64 {
        let (result, work) = bounded_cycle(&db, WORKSPACE);
        result.unwrap();
        assert!(work.workspace_peak <= WORKSPACE && work.maintenance_source_visits <= 512);
    }
    let replacement = db
        .vector_store_for_test()
        .partition_info(&route.index, &route.partition_key)
        .unwrap();
    assert!(
        replacement.base_generation > generation,
        "replacement publishes a new complete generation"
    );
    assert_eq!(
        vector_rows(&db, "building", old, Some(0)),
        vector_oracle(0..ROWS + 8, 128)
    );
    assert_eq!(
        vector_rows(&db, "building", db.snapshot(), Some(0)),
        vector_oracle(128..ROWS + 8, 128)
    );
    all_searches(&db, 128..ROWS + 8, "INDEXED");
    for id in 0..128 {
        let answer = db.execute_at_snapshot(
            "SELECT id FROM building WHERE scope = 0 ORDER BY v <=> $v USE VECTOR INDEXED LIMIT 1",
            &std::collections::HashMap::from([("v".into(), Value::Vector(vector(id, 128)))]),
            old,
        );
        assert_eq!(
            answer
                .expect("the pinned complete generation remains indexed")
                .rows,
            vec![vec![Value::Int64(id as i64)]],
            "deleted identity {id} must use its pinned old indexed generation"
        );
    }
    let held = db.__debug_vector_partition_generation_retention_for_test(&route);
    assert!(
        held.durable_superseded_generations + held.resident_superseded_generations > 0,
        "a pinned reader retains replaced generations"
    );
    drop(pin);
    for _ in 0..64 {
        db.run_maintenance_cycle().unwrap();
    }
    let reclaimed = db.__debug_vector_partition_generation_retention_for_test(&route);
    assert_eq!(
        reclaimed.durable_superseded_generations + reclaimed.resident_superseded_generations,
        0,
        "eligible old generations must actually be reclaimed"
    );
    all_searches(&db, 128..ROWS + 8, "INDEXED");
    db.close().unwrap();
}

const MAINTENANCE_CHILD_PATH: &str = "CONTEXTDB_CURRENT_MAINTENANCE_CRASH_PATH";
const MAINTENANCE_CHILD_CUT: &str = "CONTEXTDB_CURRENT_MAINTENANCE_CRASH_CUT";
fn maintenance_crash_child() -> bool {
    use std::io::{Read, Write};
    let Some(path) = std::env::var_os(MAINTENANCE_CHILD_PATH) else {
        return false;
    };
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let db = Arc::new(open(std::path::Path::new(&path), Some(64 * 1024 * 1024)));
    let cut = std::env::var(MAINTENANCE_CHILD_CUT).unwrap();
    let pause = |label: &str| {
        println!("MAINTENANCE_REAL_CUT {label}");
        std::io::stdout().flush().unwrap();
        let mut byte = [0];
        std::io::stdin()
            .read_exact(&mut byte)
            .expect("parent kills paused process");
    };
    if cut == "Insertion" {
        let route = VectorPartitionRef::new(index("current_build"), partition(0));
        let boundary = db.__arm_vector_maintenance_progress_pause_for_test(&route, 3);
        let worker_db = db.clone();
        let worker = std::thread::spawn(move || {
            let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
            worker_db.run_maintenance_cycle()
        });
        if boundary.wait_until_reached_blocking() {
            pause("Insertion");
        }
        boundary.release();
        worker.join().unwrap().unwrap();
    } else {
        contextdb_engine::writer_commit_test_seams::observe(
            Arc::new(move |point| {
                if format!("{point:?}") == cut {
                    use std::io::{Read, Write};
                    println!("MAINTENANCE_REAL_CUT {point:?}");
                    std::io::stdout().flush().unwrap();
                    std::io::stdin()
                        .read_exact(&mut [0])
                        .expect("parent kills paused process");
                }
            }),
            || db.run_maintenance_cycle(),
        )
        .unwrap();
    }
    true
}

fn current_only_maintenance_crash_cuts() {
    use std::{
        io::BufRead,
        process::{Command, Stdio},
    };
    const ROWS: usize = 128;
    for cut in ["GenerationBeforeRedb", "GenerationAfterRedb", "Insertion"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("current-build.db");
        let db = open(&path, Some(64 * 1024 * 1024));
        exec(
            &db,
            "CREATE TABLE current_build (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(128) PARTITION_KEY (scope) MAX_PARTITIONS 2 SEARCH_MODE INDEXED HNSW (M = 16, EF_CONSTRUCTION = 32)) HISTORY CURRENT ONLY SYNC OFF",
        );
        for id in 0..ROWS {
            insert_vector(&db, "current_build", id, Some(0), 128);
        }
        let frontier = db.snapshot();
        db.close().unwrap();
        drop(db);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &crate::child_test_filter(
                    module_path!(),
                    "paged_initial_replacement_and_reclamation_progress_under_writes",
                ),
                "--nocapture",
            ])
            .env(MAINTENANCE_CHILD_PATH, &path)
            .env(MAINTENANCE_CHILD_CUT, cut)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
        let mut reached = false;
        let mut transcript = String::new();
        loop {
            let mut line = String::new();
            if output.read_line(&mut line).unwrap() == 0 {
                break;
            }
            transcript.push_str(&line);
            if line.contains("MAINTENANCE_REAL_CUT") {
                reached = true;
                break;
            }
        }
        if reached {
            child.kill().unwrap();
        }
        let status = child.wait().unwrap();
        assert!(
            reached && !status.success(),
            "must kill at a real production maintenance boundary: cut={cut}; {transcript}"
        );
        let db = open(&path, Some(64 * 1024 * 1024));
        let route = VectorPartitionRef::new(index("current_build"), partition(0));
        let restored = db
            .vector_store_for_test()
            .partition_info(&route.index, &route.partition_key)
            .unwrap();
        assert_eq!(
            restored.graph_available,
            cut == "GenerationAfterRedb",
            "complete graph/catalog publication is atomic across process death: {cut}; {restored:?}"
        );
        // No caller pin is created. Superseding current-only source must leave the
        // durable maintenance floor protecting the captured unfinished frontier.
        if cut == "Insertion" {
            for id in 0..ROWS {
                if id % 3 == 0 {
                    exec(&db, &format!("DELETE FROM current_build WHERE id = {id}"));
                } else {
                    db.execute(
                        "UPDATE current_build SET scope = $scope, v = $v WHERE id = $id",
                        &std::collections::HashMap::from([
                            ("scope".into(), Value::Int64((id % 3 == 1) as i64)),
                            ("v".into(), Value::Vector(vector(id + ROWS, 128))),
                            ("id".into(), Value::Int64(id as i64)),
                        ]),
                    )
                    .unwrap();
                }
            }
            db.compact_currency_versions().unwrap();
            assert_eq!(
                vector_rows(&db, "current_build", frontier, Some(0)),
                vector_oracle(0..ROWS, 128),
                "restart restores the maintenance floor before cleanup, without a caller-created pin"
            );
            // The first batch may correctly roll back to zero progress. This cut
            // establishes no preceding committed scratch batch; exact durable
            // node/source/adjacency/progress correspondence awaits that protocol.
        }
        for _ in 0..64 {
            db.run_maintenance_cycle().unwrap();
        }
        assert!(
            db.vector_store_for_test()
                .partition_info(&route.index, &route.partition_key)
                .unwrap()
                .graph_available
        );
        let changed = cut == "Insertion";
        for scope in 0..=1 {
            let expected = (0..ROWS)
                .filter(|id| {
                    if changed {
                        id % 3 != 0 && (*id % 3 == 1) as usize == scope
                    } else {
                        scope == 0
                    }
                })
                .map(|id| {
                    vec![
                        Value::Int64(id as i64),
                        Value::Vector(vector(if changed { id + ROWS } else { id }, 128)),
                    ]
                })
                .collect::<Vec<_>>();
            assert_eq!(
                vector_rows(&db, "current_build", db.snapshot(), Some(scope as i64)),
                expected,
                "resumed current graph/source preserves changed vectors, partition moves and deletes: {cut}; scope={scope}"
            );
        }
        for id in 0..ROWS {
            if changed && id % 3 == 0 {
                continue;
            }
            let scope = if changed { (id % 3 == 1) as i64 } else { 0 };
            let query = vector(if changed { id + ROWS } else { id }, 128);
            let answer = db.execute("SELECT id FROM current_build WHERE scope = $scope ORDER BY v <=> $v USE VECTOR INDEXED LIMIT 1", &std::collections::HashMap::from([
                ("scope".into(),Value::Int64(scope)), ("v".into(),Value::Vector(query))])).unwrap();
            assert_eq!(
                answer.rows,
                vec![vec![Value::Int64(id as i64)]],
                "resumed maintained search returns the verified committed suffix: {cut}; {id}"
            );
        }
        db.close().unwrap();
    }
}
