#![allow(clippy::zombie_processes)] // Existing compatibility and test seam shape.
//! The production file route publishes one authenticated physical snapshot
//! registration and keeps it for the session/cursor lifetime without holding
//! writer admission.

use contextdb_core::Error;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    ReaderRegistrationLockPhase, WriterReaderSnapshotRegistryObservation,
    observe_writer_reader_snapshots_for_test, pause_reader_registration_lock_for_test,
    pause_reader_snapshot_registry_scan_for_test,
};
use contextdb_engine::{Database, ReadSession};
use std::collections::HashMap;
use std::io::{BufRead as _, BufReader, Write as _};
use std::os::unix::fs::MetadataExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};

const CHILD_ROLE: &str = "CONTEXTDB_READER_SNAPSHOT_CHILD";
const CHILD_STORE: &str = "CONTEXTDB_READER_SNAPSHOT_STORE";

fn seed(path: &PathBuf) {
    let db = Database::open(path).expect("create registration fixture");
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)",
        &HashMap::new(),
    )
    .expect("create fixture table");
    db.execute("INSERT INTO items VALUES (1, 'original')", &HashMap::new())
        .expect("seed original row");
    db.close().expect("close fixture writer");
}

fn seed_page_fixture(path: &PathBuf) {
    let db = Database::open(path).expect("create page-retention fixture");
    db.execute(
        "CREATE TABLE page_items (id INTEGER PRIMARY KEY, value TEXT)",
        &HashMap::new(),
    )
    .expect("create page fixture table");
    for id in 0..128i64 {
        db.execute(
            "INSERT INTO page_items VALUES ($id, $value)",
            &HashMap::from([
                ("id".into(), contextdb_core::Value::Int64(id)),
                (
                    "value".into(),
                    contextdb_core::Value::Text(format!("snapshot-{id}-{}", "s".repeat(8192))),
                ),
            ]),
        )
        .expect("seed page fixture row");
    }
    db.close().expect("close page fixture writer");
}

fn captured_writer_registry(path: &PathBuf) -> (Database, WriterReaderSnapshotRegistryObservation) {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&observed);
    let db = observe_writer_reader_snapshots_for_test(
        Arc::new(move |snapshot| sink.lock().expect("capture registry").push(snapshot)),
        || Database::open(path),
    )
    .expect("writer opens after file hydration released its admission hold");
    let mut observations = observed.lock().expect("read captured registry").clone();
    assert_eq!(
        observations.len(),
        1,
        "one physical writer open captures once"
    );
    (db, observations.remove(0))
}

#[test]
fn file_session_registration_survives_alias_and_replacement_and_releases_with_its_last_handle() {
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("store.redb");
    seed(&path);
    let original_metadata = std::fs::metadata(&path).expect("stat original store");

    let session = ReadSession::open(&path).expect("open production file session");
    assert_eq!(
        session.route(),
        contextdb_core::read_contract::ReadRoute::File
    );
    let registration = session
        .file_storage_registration_for_test()
        .expect("file route owns a storage registration");
    assert_eq!(
        (registration.physical_device, registration.physical_inode),
        (original_metadata.dev(), original_metadata.ino())
    );
    assert!(registration.database_identity.is_some());
    assert!(registration.storage_transaction_id > 0);
    assert!(registration.storage_root_page.is_some());
    assert!(registration.storage_root_checksum.is_some());
    assert!(registration.storage_root_length.is_some());
    assert_eq!(registration.process_id, std::process::id());
    assert!(registration.descriptor_open);
    assert!(!registration.load_lock_held);
    let registration_path = registration
        .registration_path
        .clone()
        .expect("usable runtime publishes the authenticated registration");
    assert!(registration_path.is_file());

    let alias = root.path().join("store-alias.redb");
    std::fs::hard_link(&path, &alias).expect("create hardlink alias");
    let displaced = root.path().join("store-displaced.redb");
    std::fs::rename(&path, &displaced).expect("rename original while descriptor remains open");
    let (writer, captured) = captured_writer_registry(&alias);
    let captured = match captured {
        WriterReaderSnapshotRegistryObservation::Complete(captured) => captured,
        other => panic!("writer must authenticate the physical reader registry: {other:?}"),
    };
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].handle_identity, registration.handle_identity);
    assert_eq!(
        captured[0].physical_identity,
        (original_metadata.dev(), original_metadata.ino())
    );
    assert_eq!(
        captured[0].storage_transaction_id,
        registration.storage_transaction_id
    );
    assert_eq!(
        captured[0].storage_root_page,
        registration.storage_root_page
    );
    writer
        .execute("INSERT INTO items VALUES (2, 'later')", &HashMap::new())
        .expect("writer progresses while the session registration remains live");
    writer.close().expect("close alias writer");

    std::fs::remove_file(&alias).expect("remove old hardlink alias");
    seed(&path);
    let replacement_metadata = std::fs::metadata(&path).expect("stat replacement store");
    assert_ne!(
        (replacement_metadata.dev(), replacement_metadata.ino()),
        (original_metadata.dev(), original_metadata.ino())
    );
    let (replacement_writer, captured) = captured_writer_registry(&path);
    assert_eq!(
        captured,
        WriterReaderSnapshotRegistryObservation::Complete(Vec::new()),
        "a replacement inode must not inherit the old physical registration"
    );
    assert_eq!(
        replacement_writer
            .__external_snapshot_retention_for_test()
            .expect("replacement store has its own retention authority"),
        Default::default(),
        "a live registration for the displaced incarnation must retain no replacement pages"
    );
    replacement_writer
        .close()
        .expect("close replacement writer");

    let result = session
        .execute("SELECT value FROM items ORDER BY id", &HashMap::new())
        .expect("fixed file session still answers its original hydrated snapshot");
    assert_eq!(
        result.rows,
        vec![vec![contextdb_core::Value::Text("original".into())]]
    );
    let cursor = session
        .open_cursor("SELECT value FROM items ORDER BY id", &HashMap::new())
        .expect("open cursor on original session");
    drop(session);
    assert!(
        registration_path.is_file(),
        "cursor ownership keeps the session registration alive without a gap"
    );
    cursor.close().expect("close cursor");
    assert!(
        !registration_path.exists(),
        "the last session/cursor handle releases the registration"
    );
}

#[test]
fn exact_external_page_retention_is_shared_once_and_released_with_the_last_session() {
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("exact-retention.redb");
    seed_page_fixture(&path);
    let first = ReadSession::open(&path).expect("open first pre-writer snapshot");
    let second = ReadSession::open(&path).expect("open second pre-writer snapshot");
    let writer = Database::open(&path).expect("writer overtakes registered readers");

    writer
        .execute(
            "UPDATE page_items SET value = $value WHERE id = 0",
            &HashMap::from([(
                "value".into(),
                contextdb_core::Value::Text(format!("writer-visible-{}", "w".repeat(8192))),
            )]),
        )
        .expect("modify a page visible to the registered root");
    writer
        .execute(
            "CREATE TABLE later_churn (id INTEGER PRIMARY KEY, value TEXT)",
            &HashMap::new(),
        )
        .expect("create post-snapshot churn table");
    writer
        .execute(
            "INSERT INTO later_churn VALUES (1, 'initial')",
            &HashMap::new(),
        )
        .expect("seed post-snapshot churn row");
    for generation in 0..4i64 {
        writer
            .execute(
                "UPDATE later_churn SET value = $value WHERE id = 1",
                &HashMap::from([(
                    "value".into(),
                    contextdb_core::Value::Text(format!(
                        "generation-{generation}-{}",
                        "c".repeat(8192)
                    )),
                )]),
            )
            .expect("settle deferred frees before inventory baseline");
    }

    let baseline = writer
        .__external_snapshot_retention_for_test()
        .expect("read redb external-retention authority");
    assert_eq!(
        baseline.snapshots, 1,
        "sessions sharing one hydrated target publish one physical root owner"
    );
    assert!(baseline.reachable_pages > 0);
    assert_eq!(baseline.referenced_pages, baseline.reachable_pages);
    assert!(baseline.retained_pages > 0);
    assert_eq!(
        baseline.retained_pages,
        baseline.retained_page_allocations.len() as u64
    );
    let inventory_bytes = baseline
        .retained_page_allocations
        .iter()
        .map(|encoded| {
            let order = u64::from_le_bytes(*encoded) >> 59;
            4096u64 << order
        })
        .sum::<u64>();
    assert_eq!(baseline.retained_bytes, inventory_bytes);
    assert!(baseline.retained_bytes <= baseline.referenced_bytes);

    let original = first
        .execute("SELECT value FROM page_items WHERE id = 0", &HashMap::new())
        .expect("first session reads its original snapshot after writer commit");
    assert!(matches!(
        original.rows.as_slice(),
        [row] if matches!(row.as_slice(), [contextdb_core::Value::Text(value)] if value.starts_with("snapshot-0-"))
    ));

    for generation in 4..20i64 {
        writer
            .execute(
                "UPDATE later_churn SET value = $value WHERE id = 1",
                &HashMap::from([(
                    "value".into(),
                    contextdb_core::Value::Text(format!(
                        "generation-{generation}-{}",
                        "c".repeat(8192)
                    )),
                )]),
            )
            .expect("commit unrelated post-snapshot churn");
        let now = writer
            .__external_snapshot_retention_for_test()
            .expect("observe retained physical allocation set");
        assert_eq!(
            now.retained_page_allocations,
            baseline.retained_page_allocations
        );
        assert_eq!(now.retained_bytes, baseline.retained_bytes);
    }

    drop(first);
    writer
        .execute(
            "UPDATE later_churn SET value = 'one-reader' WHERE id = 1",
            &HashMap::new(),
        )
        .expect("refresh after first handle closes");
    let one_reader = writer
        .__external_snapshot_retention_for_test()
        .expect("one shared root remains");
    assert_eq!(one_reader.snapshots, 1);
    assert_eq!(
        one_reader.retained_page_allocations,
        baseline.retained_page_allocations
    );
    assert_eq!(one_reader.retained_bytes, baseline.retained_bytes);

    drop(second);
    for generation in 0..3i64 {
        writer
            .execute(
                "UPDATE later_churn SET value = $value WHERE id = 1",
                &HashMap::from([(
                    "value".into(),
                    contextdb_core::Value::Text(format!("released-{generation}")),
                )]),
            )
            .expect("advance reclamation after final reader closes");
    }
    assert_eq!(
        writer
            .__external_snapshot_retention_for_test()
            .expect("released registry has no retained allocation"),
        Default::default()
    );
    writer.close().expect("close retention writer");
}

#[test]
fn projected_allocator_growth_respects_disk_limit_with_a_live_retained_reader() {
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("allocator-reservation.redb");
    seed_page_fixture(&path);
    let reader = ReadSession::open(&path).expect("open retained pre-writer snapshot");
    let writer = Database::open(&path).expect("writer overtakes retained reader");

    writer
        .execute(
            "UPDATE page_items SET value = $value WHERE id = 0",
            &HashMap::from([(
                "value".into(),
                contextdb_core::Value::Text(format!("writer-{}", "w".repeat(8192))),
            )]),
        )
        .expect("retire pages reachable by the reader");
    writer
        .execute(
            "CREATE TABLE allocation_churn (id INTEGER PRIMARY KEY, value TEXT)",
            &HashMap::new(),
        )
        .expect("create post-snapshot allocation table");
    writer
        .execute(
            "INSERT INTO allocation_churn VALUES (0, 'seed')",
            &HashMap::new(),
        )
        .expect("seed a row for the nonpoisoning retry");
    for generation in 0..3i64 {
        writer
            .execute(
                "UPDATE allocation_churn SET value = $value WHERE id = 0",
                &HashMap::from([(
                    "value".into(),
                    contextdb_core::Value::Text(format!("settle-{generation}")),
                )]),
            )
            .expect("settle retained deferred frees");
    }
    let retained = writer
        .__external_snapshot_retention_for_test()
        .expect("observe exact retained allocations");
    assert!(retained.retained_bytes > 0);

    writer
        .set_disk_limit(Some(u64::MAX / 2))
        .expect("create the durable limit slot without constraining growth");
    let before_limit = std::fs::metadata(&path).expect("stat before limit").len();
    let limit = before_limit + 1024;
    writer
        .set_disk_limit(Some(limit))
        .expect("install an exact allocator boundary");
    let bounded_start = std::fs::metadata(&path).expect("stat bounded store").len();
    assert!(bounded_start <= limit);

    let payload = "x".repeat(64 * 1024);
    let mut refused = None;
    for id in 1..=256i64 {
        match writer.execute(
            "INSERT INTO allocation_churn VALUES ($id, $value)",
            &HashMap::from([
                ("id".into(), contextdb_core::Value::Int64(id)),
                ("value".into(), contextdb_core::Value::Text(payload.clone())),
            ]),
        ) {
            Ok(_) => {}
            Err(error) => {
                refused = Some((id, error));
                break;
            }
        }
    }
    let (refused_id, refusal) = refused.expect("allocator must reach projected growth boundary");
    let refused_file_bytes = std::fs::metadata(&path).unwrap().len();
    let attributed = |error: &Error| {
        matches!(error, Error::DiskBudgetExceeded {
        operation, current_bytes, budget_limit_bytes, .. }
        if operation == "allocate storage pages" && *current_bytes == refused_file_bytes
            && *current_bytes <= limit && *budget_limit_bytes == limit)
    };
    assert!(attributed(&refusal), "{refusal:?}");
    if let Error::DiskBudgetExceeded {
        current_bytes,
        budget_limit_bytes,
        hint,
        ..
    } = &refusal
    {
        assert!(!attributed(&Error::DiskBudgetExceeded {
            operation: "purge".to_owned(),
            current_bytes: *current_bytes,
            budget_limit_bytes: *budget_limit_bytes,
            hint: hint.clone()
        }));
    }
    assert!(refused_file_bytes <= limit);
    assert_eq!(
        writer
            .__projected_disk_growth_bytes_for_test()
            .expect("inspect released reservation"),
        0,
        "refusal releases its projected-growth reservation"
    );

    let original = reader
        .execute("SELECT value FROM page_items WHERE id = 0", &HashMap::new())
        .expect("retained reader remains usable after writer refusal");
    assert!(matches!(
        original.rows.as_slice(),
        [row] if matches!(row.as_slice(), [contextdb_core::Value::Text(value)] if value.starts_with("snapshot-0-"))
    ));
    writer
        .execute(
            "UPDATE allocation_churn SET value = 'retry-ok' WHERE id = 0",
            &HashMap::new(),
        )
        .expect("a smaller transaction reuses available pages after refusal");

    drop(reader);
    // Each admitted storage commit frees at most 64 native page-list entries.
    // Drain the observed backlog, including the two commit fences, before retry.
    let initial = writer.__deferred_free_backlog_for_test().unwrap();
    let passes = initial
        .iter()
        .map(|(_, pages)| *pages)
        .sum::<usize>()
        .div_ceil(64)
        + 2;
    for pass in 0..passes {
        writer
            .__commit_storage_transaction_for_test()
            .expect("finite deferred-free pass fits the unchanged disk ceiling");
    }
    assert_eq!(
        writer
            .__external_snapshot_retention_for_test()
            .expect("released reader owns no pages"),
        Default::default()
    );
    writer
        .execute(
            "INSERT INTO allocation_churn VALUES ($id, $value)",
            &HashMap::from([
                ("id".into(), contextdb_core::Value::Int64(refused_id)),
                (
                    "value".into(),
                    contextdb_core::Value::Text("reclaimed".repeat(1024)),
                ),
            ]),
        )
        .expect("released retained pages satisfy the formerly refused allocation");
    assert!(
        std::fs::metadata(&path)
            .expect("stat reclaimed store")
            .len()
            <= limit
    );
    writer.close().expect("close allocator-bound writer");
}

#[test]
fn writer_scan_after_final_reader_teardown_observes_no_released_registration() {
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("scan-after-reader-teardown.redb");
    seed(&path);

    let session = ReadSession::open(&path).expect("open authenticated production file reader");
    let registration_path = session
        .file_storage_registration_for_test()
        .and_then(|registration| registration.registration_path)
        .expect("normal file reader publishes an authenticated registration");
    assert!(registration_path.is_file());

    let scan = pause_reader_snapshot_registry_scan_for_test(&path)
        .expect("arm the physical registry scan after reader registration");
    let writer_path = path.clone();
    let (completed, completion) = mpsc::channel();
    std::thread::spawn(move || {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&observed);
        let admission = observe_writer_reader_snapshots_for_test(
            Arc::new(move |snapshot| sink.lock().expect("capture registry").push(snapshot)),
            || Database::open(&writer_path),
        );
        let outcome = admission
            .and_then(|writer| writer.close())
            .map_err(|error| error.to_string());
        completed
            .send((
                outcome,
                observed.lock().expect("read captured registry").clone(),
            ))
            .expect("report writer admission");
    });

    let inspection = scan.wait_until_inspected_for_test();
    assert_eq!(inspection.database_path, path);
    assert_eq!(inspection.physical_identity, {
        let metadata = std::fs::metadata(&path).expect("stat writer target");
        (metadata.dev(), metadata.ino())
    });

    drop(session);
    assert!(
        !registration_path.exists(),
        "normal final-reader teardown removes its authenticated registration before scan resume"
    );
    scan.resume_for_test();
    scan.finish_for_test();

    let (outcome, observations) = completion.recv().expect("writer admission completes");
    outcome.expect("writer admission does not report an indeterminate authenticated registry");
    assert_eq!(
        observations,
        vec![WriterReaderSnapshotRegistryObservation::Complete(Vec::new())],
        "writer scan contains no released registration"
    );
}

const RACE_CHILD_ROLE: &str = "CONTEXTDB_LAST_READER_CLOSE_RACE_CHILD";

/// Each race runs in a child with its own private runtime directory: the
/// shared default one can hold other stores' registry files under a reused
/// physical identity, which would keep the departing reader from being the
/// last one.
fn run_race_child(scenario: &str) {
    let scratch = tempfile::tempdir().expect("scratch directory");
    let runtime = scratch.path().join("runtime");
    std::fs::create_dir(&runtime).expect("create the private runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("secure the private runtime directory");
    let filter = crate::child_test_filter(module_path!(), "last_reader_close_race_child");
    let status = Command::new(std::env::current_exe().expect("current test binary"))
        .args(["--exact", &filter, "--nocapture"])
        .env(RACE_CHILD_ROLE, scenario)
        .env("XDG_RUNTIME_DIR", &runtime)
        .status()
        .expect("run the reader race child");
    assert!(
        status.success(),
        "reader race child {scenario} failed: {status}"
    );
}

#[test]
fn closing_the_last_reader_while_another_opens_does_not_reject_the_new_reader() {
    run_race_child("validated");
}

#[test]
fn closing_the_last_reader_before_the_new_reader_validates_its_lock_does_not_reject_it() {
    run_race_child("descriptor-opened");
}

#[test]
fn repeated_teardown_while_a_reader_opens_never_rejects_it() {
    run_race_child("repeated");
}

#[test]
fn an_untrusted_registry_lock_file_is_still_refused() {
    run_race_child("untrusted-mode");
    run_race_child("untrusted-link");
}

/// A store read with no writer companion beside it (a copied or archived
/// store) has nothing else in its reader registry, so the last reader to
/// close takes the registry and its lock away.
fn seed_companionless(path: &PathBuf) {
    seed(path);
    std::fs::remove_file(contextdb_engine::persistence::store_companion_path(path))
        .expect("read the store as one with no writer companion beside it");
}

fn registry_lock_path(path: &PathBuf) -> PathBuf {
    let metadata = std::fs::metadata(path).expect("stat store");
    PathBuf::from(std::env::var_os("XDG_RUNTIME_DIR").expect("private runtime"))
        .join("contextdb")
        .join(format!(
            "storage-{:016x}-{:016x}.snapshot-registry-lock",
            metadata.dev(),
            metadata.ino()
        ))
}

fn original_rows() -> Vec<Vec<contextdb_core::Value>> {
    vec![vec![contextdb_core::Value::Text("original".into())]]
}

fn read_rows(session: &ReadSession) -> Vec<Vec<contextdb_core::Value>> {
    session
        .execute("SELECT value FROM items ORDER BY id", &HashMap::new())
        .expect("the reader answers")
        .rows
}

#[test]
fn last_reader_close_race_child() {
    let Some(scenario) = std::env::var_os(RACE_CHILD_ROLE) else {
        return;
    };
    let scenario = scenario.to_string_lossy().into_owned();
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("open-while-last-reader-closes.redb");
    seed_companionless(&path);

    if let Some(control) = scenario.strip_prefix("untrusted-") {
        // Plant a lock file the reader must not trust, then open.
        let lock = registry_lock_path(&path);
        std::fs::create_dir_all(lock.parent().unwrap()).expect("create registry root");
        std::fs::set_permissions(
            lock.parent().unwrap(),
            std::fs::Permissions::from_mode(0o700),
        )
        .expect("secure registry root");
        std::fs::write(&lock, b"").expect("plant the lock file");
        match control {
            "mode" => std::fs::set_permissions(&lock, std::fs::Permissions::from_mode(0o644))
                .expect("make the lock group-readable"),
            "link" => {
                std::fs::set_permissions(&lock, std::fs::Permissions::from_mode(0o600))
                    .expect("make the lock private");
                std::fs::hard_link(&lock, lock.with_extension("alias"))
                    .expect("give the lock a second name");
            }
            other => panic!("unknown control {other}"),
        }
        let refused = ReadSession::open(&path)
            .err()
            .unwrap_or_else(|| panic!("a reader must refuse an untrusted {control} lock"));
        assert!(
            refused
                .to_string()
                .contains("not a trusted private regular file"),
            "refused for the lock's trust, not something else: {refused}"
        );
        return;
    }

    let departing = ReadSession::open(&path).expect("open the reader that will close");
    assert_eq!(read_rows(&departing), original_rows());
    let registry_directory = departing
        .file_storage_registration_for_test()
        .and_then(|registration| registration.registration_path)
        .expect("normal file reader publishes an authenticated registration")
        .parent()
        .expect("registration lives in its physical registry directory")
        .to_path_buf();

    let phase = match scenario.as_str() {
        "validated" => ReaderRegistrationLockPhase::Validated,
        "descriptor-opened" | "repeated" => ReaderRegistrationLockPhase::DescriptorOpened,
        other => panic!("unknown scenario {other}"),
    };
    let teardowns: u64 = if scenario == "repeated" { 12 } else { 1 };
    let pause = pause_reader_registration_lock_for_test(&path, phase)
        .expect("arm the arriving reader's registration pause");
    let arriving_path = path.clone();
    let (opened, arrival) = mpsc::channel();
    std::thread::spawn(move || {
        let outcome = ReadSession::open(&arriving_path)
            .map_err(|error| error.to_string())
            .map(|session| {
                let rows = read_rows(&session);
                let registration = session
                    .file_storage_registration_for_test()
                    .and_then(|registration| registration.registration_path);
                (session, rows, registration)
            });
        opened.send(outcome).expect("report the arriving reader");
    });

    // Tear the registry down under the paused opener, once per attempt. The
    // first teardown is the original last reader closing; each later one is
    // another reader that opens and closes in full while the opener holds a
    // lock descriptor it has not validated yet.
    let mut departing = Some(departing);
    for attempt in 1..=teardowns {
        if !pause.wait_for_attempt_for_test(attempt, std::time::Duration::from_secs(30)) {
            let outcome = arrival
                .recv_timeout(std::time::Duration::from_secs(30))
                .map(|outcome| outcome.map(|_| ()));
            panic!("the opener never made attempt {attempt}: {outcome:?}");
        }
        match departing.take() {
            Some(last) => drop(last),
            None => {
                let passing = ReadSession::open(&path).expect("a passing reader opens");
                assert_eq!(read_rows(&passing), original_rows());
                drop(passing);
            }
        }
        assert!(
            !registry_directory.exists(),
            "teardown {attempt} left no reader, so it removed the registry"
        );
        if attempt == teardowns {
            pause.stop_pausing_for_test();
        } else {
            pause.resume_for_test();
        }
    }

    let (arriving, rows, arriving_registration) = arrival
        .recv()
        .expect("the arriving reader reports")
        .expect("teardown must not reject a reader opening a healthy store");
    assert_eq!(
        pause.attempts_for_test(),
        teardowns + 1,
        "the opener retried once per finished teardown and then succeeded"
    );
    drop(pause);
    assert_eq!(rows, original_rows());
    let arriving_registration =
        arriving_registration.expect("the arriving reader publishes its own registration");
    assert!(arriving_registration.is_file());
    let arriving_handle = arriving
        .file_storage_registration_for_test()
        .expect("the arriving reader owns a storage registration")
        .handle_identity;

    let (writer, captured) = captured_writer_registry(&path);
    let captured = match captured {
        WriterReaderSnapshotRegistryObservation::Complete(captured) => captured,
        other => panic!("writer must authenticate the surviving reader registry: {other:?}"),
    };
    assert_eq!(
        captured
            .iter()
            .map(|snapshot| snapshot.handle_identity)
            .collect::<Vec<_>>(),
        vec![arriving_handle],
        "the writer sees exactly the reader that is still open"
    );
    writer
        .execute(
            "UPDATE items SET value = 'later' WHERE id = 1",
            &HashMap::new(),
        )
        .expect("the writer commits past the arriving reader's snapshot");
    writer.close().expect("close writer");
    assert_eq!(
        read_rows(&arriving),
        original_rows(),
        "the arriving reader keeps the snapshot it pinned before the teardowns"
    );

    drop(arriving);
    assert!(!arriving_registration.exists());
    let (writer, captured) = captured_writer_registry(&path);
    assert_eq!(
        captured,
        WriterReaderSnapshotRegistryObservation::Complete(Vec::new()),
        "after both readers close the writer sees no registration"
    );
    writer.close().expect("close writer");
}

#[test]
fn reader_snapshot_registration_child() {
    if std::env::var_os(CHILD_ROLE).is_none() {
        return;
    }
    let path = PathBuf::from(std::env::var_os(CHILD_STORE).expect("child store path"));
    let session = ReadSession::open(path).expect("child opens a production file session");
    let registration = session
        .file_storage_registration_for_test()
        .and_then(|registration| registration.registration_path)
        .expect("child publishes a registration");
    println!("{}", registration.display());
    std::io::stdout().flush().expect("flush child registration");
    let mut release = String::new();
    std::io::stdin()
        .read_line(&mut release)
        .expect("wait for parent release");
    drop(session);
}

#[test]
fn process_death_releases_the_registration_lock_and_writer_reaps_the_record() {
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("process-death.redb");
    seed(&path);
    let filter = crate::child_test_filter(module_path!(), "reader_snapshot_registration_child");
    let mut child = Command::new(std::env::current_exe().expect("current test binary"))
        .args(["--exact", &filter, "--nocapture"])
        .env(CHILD_ROLE, "1")
        .env(CHILD_STORE, &path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn reader child");
    let mut stdout = BufReader::new(child.stdout.take().expect("child stdout"));
    let mut line = String::new();
    loop {
        line.clear();
        assert_ne!(stdout.read_line(&mut line).expect("read child output"), 0);
        let candidate = PathBuf::from(line.trim());
        if candidate.extension().and_then(|value| value.to_str()) == Some("snapshot") {
            assert!(candidate.is_file());
            let (writer, captured) = captured_writer_registry(&path);
            assert!(matches!(
                captured,
                WriterReaderSnapshotRegistryObservation::Complete(ref snapshots)
                    if snapshots.len() == 1
            ));
            writer
                .execute(
                    "UPDATE items SET value = 'writer' WHERE id = 1",
                    &HashMap::new(),
                )
                .expect("retire pages reachable by the child snapshot");
            writer
                .execute(
                    "CREATE TABLE death_churn (id INTEGER PRIMARY KEY, value TEXT)",
                    &HashMap::new(),
                )
                .expect("create post-snapshot child churn");
            writer
                .execute(
                    "INSERT INTO death_churn VALUES (1, 'first')",
                    &HashMap::new(),
                )
                .expect("seed post-snapshot child churn");
            for generation in 0..3i64 {
                writer
                    .execute(
                        "UPDATE death_churn SET value = $value WHERE id = 1",
                        &HashMap::from([(
                            "value".into(),
                            contextdb_core::Value::Text(format!("before-death-{generation}")),
                        )]),
                    )
                    .expect("settle child-retained pages");
            }
            let live = writer
                .__external_snapshot_retention_for_test()
                .expect("observe child-retained allocation versions");
            assert_eq!(live.snapshots, 1);
            assert!(live.retained_pages > 0);
            assert!(!live.retained_page_allocations.is_empty());

            child.kill().expect("terminate reader process");
            let status = child.wait().expect("reap reader process");
            assert!(!status.success());
            for generation in 0..3i64 {
                writer
                    .execute(
                        "UPDATE death_churn SET value = $value WHERE id = 1",
                        &HashMap::from([(
                            "value".into(),
                            contextdb_core::Value::Text(format!("after-death-{generation}")),
                        )]),
                    )
                    .expect("refresh and reclaim after child death");
            }
            assert_eq!(
                writer
                    .__external_snapshot_retention_for_test()
                    .expect("dead child owns no retained allocation"),
                Default::default(),
                "process death releases the registration and its exact page holds"
            );
            assert!(!candidate.exists());
            writer.close().expect("close post-death writer");
            break;
        }
    }
}
