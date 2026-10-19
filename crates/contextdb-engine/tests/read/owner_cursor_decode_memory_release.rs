use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::fd::AsRawFd;
use std::path::Path;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use contextdb_core::read_contract::{
    ReadFailureDetail, ReadFailureKind, ReadFailureLimit, ReadLimits, ReadRoute,
};
use contextdb_core::{Error, Value};
#[path = "../owner_cursor_test_support.rs"]
#[allow(dead_code)]
mod owner_cursor_test_support;
use contextdb_engine::read_session::ReadSessionTestObserver;
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use owner_cursor_test_support::decode_owner_cursor_page as decode_cursor_page;
use owner_cursor_test_support::replace_owner_cursor_page;

const ROLE: &str = "CONTEXTDB_OWNER_DECODE_MEMORY_ROLE";
const PATH: &str = "CONTEXTDB_OWNER_DECODE_MEMORY_PATH";

#[test]
fn writer_child() {
    if std::env::var_os(ROLE).is_none() {
        return;
    }
    let path = std::env::var(PATH).expect("child store path");
    let db = Database::open(&path).expect("own the fixture store");
    println!("HOLDING");
    std::io::stdout().flush().expect("flush owner marker");
    let mut command = String::new();
    loop {
        command.clear();
        if std::io::stdin()
            .lock()
            .read_line(&mut command)
            .expect("read owner command")
            == 0
        {
            break;
        }
        match command.trim() {
            "count" => {
                let resources = db
                    .read_route_resources_for_test()
                    .expect("read owner resources");
                assert_eq!(resources.active_owner_slots, resources.active_cursors);
                println!("CURSORS {}", resources.active_cursors);
                std::io::stdout().flush().expect("flush cursor count");
            }
            "release" => break,
            other => panic!("unknown owner command: {other}"),
        }
    }
    drop(db);
    println!("RELEASED");
    std::io::stdout().flush().expect("flush release marker");
}

struct WriterChild {
    child: Child,
    output: BufReader<ChildStdout>,
}

impl WriterChild {
    fn spawn(path: &Path) -> Self {
        let mut child = Command::new(std::env::current_exe().expect("test binary"))
            .arg("--exact")
            .arg(super::child_test_filter(module_path!(), "writer_child"))
            .arg("--nocapture")
            .env(ROLE, "holding")
            .env(PATH, path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start owner child");
        let output = BufReader::new(child.stdout.take().expect("child output"));
        let mut owner = Self { child, output };
        owner.wait_for("HOLDING");
        owner
    }

    fn wait_for(&mut self, marker: &str) {
        let mut line = String::new();
        while self.output.read_line(&mut line).expect("read child marker") != 0 {
            if line.contains(marker) {
                return;
            }
            line.clear();
        }
        panic!("owner child exited before {marker}");
    }

    fn terminate(mut self) {
        self.child
            .kill()
            .expect("stop the owner after the decode refusal");
        self.child.wait().expect("reap the stopped owner");
    }

    fn cursor_count(&mut self) -> u64 {
        writeln!(self.child.stdin.as_mut().expect("child input"), "count")
            .expect("ask owner for its live cursor count");
        let mut line = String::new();
        while self.output.read_line(&mut line).expect("read cursor count") != 0 {
            if let Some(count) = line.trim().strip_prefix("CURSORS ") {
                return count.parse().expect("numeric cursor count");
            }
            line.clear();
        }
        panic!("owner exited before reporting cursor count");
    }
}

struct OversizedFetchPage;

impl ReadSessionTestObserver for OversizedFetchPage {
    fn cursor_fetch_payload_for_test(&self, payload: &[u8]) -> Option<Vec<u8>> {
        let mut page = decode_cursor_page(payload).expect("owner published a valid page");
        assert_eq!(page.rows.len(), 1);
        page.rows[0][1] = Value::Text("x".repeat(128 * 1024));
        Some(replace_owner_cursor_page(payload, &page).expect("encode one oversized valid page"))
    }
}

struct MalformedFetchPage;

impl ReadSessionTestObserver for MalformedFetchPage {
    fn cursor_fetch_payload_for_test(&self, _payload: &[u8]) -> Option<Vec<u8>> {
        Some(vec![0xff])
    }
}

#[derive(Default)]
struct OversizedFirstPageOnce(AtomicBool);

impl ReadSessionTestObserver for OversizedFirstPageOnce {
    fn cursor_open_payload_for_test(&self, payload: &[u8]) -> Option<Vec<u8>> {
        if self.0.swap(true, Ordering::SeqCst) {
            return None;
        }
        let mut page = decode_cursor_page(payload).expect("owner published a valid first page");
        assert_eq!(page.rows.len(), 1);
        page.rows[0][1] = Value::Text("x".repeat(128 * 1024));
        Some(
            replace_owner_cursor_page(payload, &page)
                .expect("encode one oversized valid first page"),
        )
    }
}

fn reader_presence(path: &Path) -> bool {
    const WIDTH: i64 = 1 << 60;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open file to inspect reader presence");
    for base in [WIDTH, 2 * WIDTH] {
        let mut lock = libc::flock {
            l_type: libc::F_WRLCK as libc::c_short,
            l_whence: libc::SEEK_SET as libc::c_short,
            l_start: base,
            l_len: WIDTH,
            l_pid: 0,
        };
        // SAFETY: lock is initialized and the file descriptor stays open.
        assert_ne!(
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_GETLK, &mut lock) },
            -1
        );
        if lock.l_type != libc::F_UNLCK as libc::c_short {
            return true;
        }
    }
    false
}

#[test]
fn oversized_owner_fetch_page_releases_cursor_slot_snapshot_and_presence() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("task-scoped store directory");
    let path = dir.path().join("decode-memory.db");
    let seed = Database::open(&path).expect("create fixture");
    seed.execute(
        "CREATE TABLE rows (id INTEGER PRIMARY KEY, payload TEXT)",
        &HashMap::new(),
    )
    .expect("create table");
    for id in 0..3 {
        seed.execute(
            "INSERT INTO rows (id, payload) VALUES ($id, 'small')",
            &HashMap::from([("id".to_owned(), Value::Int64(id))]),
        )
        .expect("seed one row");
    }
    seed.close().expect("release seed writer");

    let mut owner = WriterChild::spawn(&path);
    let memory = 64 * 1024;
    let session = ReadSession::open_with_observer_for_test(
        &path,
        ReadSessionOptions {
            memory_limit: Some(memory),
            limits: ReadLimits {
                memory: memory as u64,
                result_bytes: 1024 * 1024,
                cursor_page_rows: 1,
                cursor_page_bytes: 1024 * 1024,
                ..ReadLimits::default()
            },
            ..ReadSessionOptions::default()
        },
        Arc::new(OversizedFetchPage),
    )
    .expect("open bounded owner session");
    assert_eq!(session.route(), ReadRoute::Owner);
    let mut cursor = session
        .open_cursor("SELECT id, payload FROM rows ORDER BY id", &HashMap::new())
        .expect("open first small page");
    assert!(cursor.first_page().has_more);
    assert_eq!(
        session.route_resources_for_test().unwrap().active_cursors,
        1
    );
    assert_eq!(owner.cursor_count(), 1);
    assert!(reader_presence(&path), "live cursor holds file presence");

    let error = cursor
        .fetch(None)
        .expect_err("oversized decoded page is refused");
    let Error::ReadFailure(failure) = error else {
        panic!("memory refusal must be typed: {error:?}");
    };
    assert_eq!(failure.kind(), ReadFailureKind::OwnerLimitExceeded);
    let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail() else {
        panic!("memory refusal must name its limit: {failure:?}");
    };
    assert_eq!(detail.limit, ReadFailureLimit::Memory);
    assert_eq!(detail.value, memory as u64);
    assert!(!cursor.is_live(), "decode refusal terminates the cursor");
    assert_eq!(
        session.route_resources_for_test().unwrap().active_cursors,
        0
    );
    assert_eq!(
        owner.cursor_count(),
        0,
        "terminal refusal closes owner cursor"
    );
    assert!(
        !reader_presence(&path),
        "cursor release drops file presence"
    );

    let malformed_session = ReadSession::open_with_observer_for_test(
        &path,
        ReadSessionOptions {
            memory_limit: Some(memory),
            limits: ReadLimits {
                memory: memory as u64,
                result_bytes: 1024 * 1024,
                cursor_page_rows: 1,
                cursor_page_bytes: 1024 * 1024,
                ..ReadLimits::default()
            },
            ..ReadSessionOptions::default()
        },
        Arc::new(MalformedFetchPage),
    )
    .expect("open owner session for malformed page proof");
    let mut malformed_cursor = malformed_session
        .open_cursor("SELECT id, payload FROM rows ORDER BY id", &HashMap::new())
        .expect("open another small first page");
    assert_eq!(owner.cursor_count(), 1);
    assert!(reader_presence(&path));
    let malformed = malformed_cursor
        .fetch(None)
        .expect_err("malformed owner page is refused");
    assert!(
        matches!(malformed, Error::ReadFailure(failure) if failure.kind() == ReadFailureKind::InvalidChannelData),
        "malformed payload must keep its channel-data classification"
    );
    assert!(!malformed_cursor.is_live());
    assert_eq!(owner.cursor_count(), 0);
    assert_eq!(
        malformed_session
            .route_resources_for_test()
            .unwrap()
            .active_cursors,
        0
    );
    assert!(!reader_presence(&path));

    owner.terminate();
    let replacement = Database::open(&path).expect("open replacement writer");
    assert_eq!(
        replacement
            .__external_snapshot_retention_for_test()
            .expect("inspect held snapshots")
            .snapshots,
        0,
        "snapshot registration releases while the cursor object remains alive"
    );
    let _ = (&cursor, &malformed_cursor);
}

#[test]
fn oversized_owner_first_page_closes_the_owner_cursor_and_preserves_the_refusal() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("task-scoped store directory");
    let path = dir.path().join("decode-first-page.db");
    let owner = Database::open(&path).expect("create owner fixture");
    owner
        .execute(
            "CREATE TABLE rows (id INTEGER PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .expect("create table");
    for id in 0..3 {
        owner
            .execute(
                "INSERT INTO rows (id, payload) VALUES ($id, 'small')",
                &HashMap::from([("id".to_owned(), Value::Int64(id))]),
            )
            .expect("seed one row");
    }
    let memory = 64 * 1024;
    let session = ReadSession::open_with_observer_for_test(
        &path,
        ReadSessionOptions {
            memory_limit: Some(memory),
            limits: ReadLimits {
                memory: memory as u64,
                result_bytes: 1024 * 1024,
                cursor_page_rows: 1,
                cursor_page_bytes: 1024 * 1024,
                ..ReadLimits::default()
            },
            ..ReadSessionOptions::default()
        },
        Arc::new(OversizedFirstPageOnce::default()),
    )
    .expect("open bounded owner session");
    assert_eq!(session.route(), ReadRoute::Owner);

    let error =
        match session.open_cursor("SELECT id, payload FROM rows ORDER BY id", &HashMap::new()) {
            Ok(_) => panic!("oversized decoded first page must be refused"),
            Err(error) => error,
        };
    let Error::ReadFailure(failure) = error else {
        panic!("memory refusal must be typed: {error:?}");
    };
    assert_eq!(failure.kind(), ReadFailureKind::OwnerLimitExceeded);
    let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail() else {
        panic!("memory refusal must name its limit: {failure:?}");
    };
    assert_eq!(detail.limit, ReadFailureLimit::Memory);
    assert_eq!(detail.value, memory as u64);
    assert_eq!(
        session.route_resources_for_test().unwrap().active_cursors,
        0
    );
    assert_eq!(
        owner
            .read_route_resources_for_test()
            .unwrap()
            .active_cursors,
        0
    );
    assert_eq!(
        owner
            .read_route_resources_for_test()
            .unwrap()
            .active_owner_slots,
        0
    );
    assert!(
        !reader_presence(&path),
        "failed open drops provisional presence"
    );

    let cursor = session
        .open_cursor("SELECT id, payload FROM rows ORDER BY id", &HashMap::new())
        .expect("next cursor opens after owner-side close");
    assert!(cursor.first_page().has_more);
    cursor.close().expect("close second cursor");
}
