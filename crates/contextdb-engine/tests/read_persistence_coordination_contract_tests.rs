//! Production-path proof for side-effect-free read-image hydration and
//! durable writer/direct-reader coordination.

use contextdb_core::read_contract::{
    ChannelAddress, DatabaseIdentity, HeldByReadersDetail, LocalUserIdentity, OwnerReadStatus,
    OwnerServingReason, OwnerServingState, ProcessStartIdentity, ReadFailureDetail,
    ReadFailureKind, ReaderBreadcrumb, WriterRunNumber,
};
use contextdb_core::{Error, Value};
use contextdb_engine::local_transport::derive_channel_address;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    COMPANION_CHANNEL_ADDRESS_CODEC, CompanionRecordFields, CompanionRecordObservation,
    CompanionWriteBoundary, LoadReadImageAdapterError, ReadImageObservation, ReadImageSourceEvent,
    ReplacementIntentObservation, StoreReplacementBoundary, StoreReplacementEvent,
    TwoReadImageHydrationController, arm_companion_crash_for_test,
    arm_read_image_hydration_pause_for_test, arm_store_replacement_sequence_for_test,
    arm_two_read_image_hydrations_for_test, begin_two_read_image_hydration_attempt_for_test,
    cancel_store_replacement_sequence_for_test, canonical_reader_breadcrumb_directory_for_test,
    clear_companion_effective_user_identity_for_test, companion_effective_user_identity_for_test,
    companion_effective_user_source_calls_for_test, create_unlocked_reader_breadcrumb_for_test,
    decode_companion_record_for_test, encode_companion_record_for_test,
    finish_store_replacement_sequence_for_test, inspect_companion_record_for_test,
    inspect_replacement_intent_for_test, load_read_image_for_test_adapter,
    locked_reader_breadcrumbs_for_test, next_store_replacement_event_for_test,
    read_image_source_events_for_test, reader_breadcrumb_runtime_root_for_test,
    release_store_replacement_checkpoint_for_test, replace_reader_breadcrumb_for_test,
    reset_companion_effective_user_source_calls_for_test, reset_read_image_source_events_for_test,
    reset_writer_open_attempts_for_test, set_companion_effective_user_identity_for_test,
    store_fingerprint_for_test, writer_open_attempts_for_test,
};
use contextdb_engine::{Database, ReadSession};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read as _, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread;

#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};

const CHILD_ROLE_ENV: &str = "CONTEXTDB_READ_PERSISTENCE_CHILD_ROLE";
const CHILD_PATH_ENV: &str = "CONTEXTDB_READ_PERSISTENCE_STORE_PATH";
const CHILD_BOUNDARY_ENV: &str = "CONTEXTDB_COMPANION_CRASH_BOUNDARY";
const CHILD_REPLACEMENT_BOUNDARY_ENV: &str = "CONTEXTDB_STORE_REPLACEMENT_CRASH_BOUNDARY";
const HOLDER_OPENED: &str = "READ_PERSISTENCE_HOLDER_OPENED";
const READ_SESSION_OPENED: &str = "PRODUCTION_READ_SESSION_OPENED";
const HYDRATION_HELD: &str = "READ_IMAGE_HYDRATION_HELD=";
const HYDRATION_FINISHED: &str = "READ_IMAGE_HYDRATION_FINISHED";
const HYDRATION_NOT_IMPLEMENTED: &str = "READ_IMAGE_HYDRATION_NOT_IMPLEMENTED";
const RELEASE: &str = "release";
const SENTINEL_PERMIT_FINAL_RELEASE: &str = "permit-final-release";
const SENTINEL_ARMED: &str = "STORE_REPLACEMENT_LOCK_SENTINEL_ARMED";
const SENTINEL_GAP: &str = "STORE_REPLACEMENT_LOCK_SENTINEL_ACQUIRED_EARLY";
const SENTINEL_FINAL_PERMISSION: &str = "STORE_REPLACEMENT_LOCK_SENTINEL_FINAL_PERMISSION";
const SENTINEL_ACQUIRED: &str = "STORE_REPLACEMENT_LOCK_SENTINEL_ACQUIRED_AFTER_RELEASE";
const SENTINEL_OTHER: &str = "STORE_REPLACEMENT_LOCK_SENTINEL_OTHER=";
const REPLACEMENT_WRITER_REFUSED: &str = "STORE_REPLACEMENT_WRITER_REFUSED";
const REPLACEMENT_WRITER_ACQUIRED: &str = "STORE_REPLACEMENT_WRITER_ACQUIRED";
const REPLACEMENT_WRITER_OTHER: &str = "STORE_REPLACEMENT_WRITER_OTHER=";
const WRITER_PROBE: &str = "READ_PERSISTENCE_WRITER_PROBE=";
const WRITER_PROBE_BREADCRUMB: &str = "READ_PERSISTENCE_WRITER_BREADCRUMB=";
const RAW_REDB_WRITER_PROBE: &str = "RAW_REDB_WRITER_PROBE=";
const STORE_REPLACEMENT_CRASH_BOUNDARY: &str = "STORE_REPLACEMENT_CRASH_BOUNDARY=";
const STORE_REPLACEMENT_GENERATED_TARGET: &str = "STORE_REPLACEMENT_GENERATED_TARGET=";
const STORE_REPLACEMENT_GENERATED_IDENTITY: &str = "STORE_REPLACEMENT_GENERATED_IDENTITY=";
const STORE_REPLACEMENT_GENERATED_FINGERPRINT: &str = "STORE_REPLACEMENT_GENERATED_FINGERPRINT=";

static COORDINATION_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

fn serialise_coordination_test() -> MutexGuard<'static, ()> {
    COORDINATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct ArmedStoreReplacementSequence;

impl ArmedStoreReplacementSequence {
    fn arm() -> Self {
        arm_store_replacement_sequence_for_test();
        Self
    }
}

impl Drop for ArmedStoreReplacementSequence {
    fn drop(&mut self) {
        cancel_store_replacement_sequence_for_test();
    }
}

struct StoreReplacementTask {
    handle: Option<thread::JoinHandle<std::result::Result<(), Error>>>,
}

impl StoreReplacementTask {
    fn spawn(path: PathBuf) -> Self {
        Self {
            handle: Some(thread::spawn(move || {
                let _finished = StoreReplacementFinished;
                Database::force_reset(path)
            })),
        }
    }

    fn join(mut self) -> std::thread::Result<std::result::Result<(), Error>> {
        self.handle
            .take()
            .expect("store replacement task was already joined")
            .join()
    }
}

impl Drop for StoreReplacementTask {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        cancel_store_replacement_sequence_for_test();
        let _ = handle.join();
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_hex(encoded: &str) -> std::result::Result<Vec<u8>, String> {
    if !encoded.len().is_multiple_of(2) {
        return Err("hex value has odd length".to_owned());
    }
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let nibble = |byte: u8| match byte {
                b'0'..=b'9' => Ok(byte - b'0'),
                b'a'..=b'f' => Ok(byte - b'a' + 10),
                b'A'..=b'F' => Ok(byte - b'A' + 10),
                _ => Err(format!("invalid hex byte {byte}")),
            };
            Ok((nibble(pair[0])? << 4) | nibble(pair[1])?)
        })
        .collect()
}

fn child_store_path() -> PathBuf {
    PathBuf::from(std::env::var_os(CHILD_PATH_ENV).expect("child receives a store path"))
}

fn directory_entries(path: &Path) -> BTreeSet<PathBuf> {
    fs::read_dir(path.parent().expect("store has a parent directory"))
        .expect("read store parent directory")
        .map(|entry| entry.expect("read store parent entry").path())
        .collect()
}

fn discover_new_reset_target(
    path: &Path,
    baseline: &BTreeSet<PathBuf>,
) -> std::result::Result<PathBuf, String> {
    let source_name = path
        .file_name()
        .ok_or_else(|| "reset store has no file name".to_owned())?
        .to_string_lossy();
    let prefix = format!("{source_name}.replacement-");
    let mut candidates = directory_entries(path)
        .into_iter()
        .filter(|candidate| !baseline.contains(candidate))
        .filter(|candidate| {
            candidate.file_name().is_some_and(|name| {
                let name = name.to_string_lossy();
                name.starts_with(&prefix) && name.ends_with(".tmp")
            })
        })
        .filter(|candidate| {
            fs::symlink_metadata(candidate).is_ok_and(|metadata| metadata.file_type().is_file())
        })
        .collect::<Vec<_>>();
    candidates.sort();
    match candidates.as_slice() {
        [target] => Ok(target.clone()),
        _ => Err(format!(
            "expected one newly generated reset target, found {candidates:?}"
        )),
    }
}

fn publish_opened_and_wait_for_release() {
    println!("{HOLDER_OPENED}");
    std::io::stdout()
        .flush()
        .expect("flush holder-opened marker");
    let mut command = String::new();
    std::io::stdin()
        .read_line(&mut command)
        .expect("read holder release command");
    assert_eq!(command.trim(), RELEASE);
}

#[test]
fn writer_holder_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("writer-holder") {
        return;
    }
    let db = Database::open(child_store_path()).expect("writer child opens the real store");
    publish_opened_and_wait_for_release();
    db.close().expect("writer child closes cleanly");
}

#[test]
fn raw_readonly_holder_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("raw-readonly-holder") {
        return;
    }
    let readonly = redb::Builder::new()
        .open_read_only(child_store_path())
        .expect("raw primitive opens redb read-only");
    publish_opened_and_wait_for_release();
    drop(readonly);
}

#[test]
fn raw_redb_writer_probe_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("raw-redb-writer-probe") {
        return;
    }
    match redb::Builder::new().open(child_store_path()) {
        Ok(database) => {
            drop(database);
            println!("{RAW_REDB_WRITER_PROBE}acquired");
        }
        Err(redb::DatabaseError::DatabaseAlreadyOpen) => {
            println!("{RAW_REDB_WRITER_PROBE}database-already-open");
        }
        Err(error) => println!("{RAW_REDB_WRITER_PROBE}other:{error}"),
    }
    std::io::stdout()
        .flush()
        .expect("flush raw-redb writer result");
}

#[test]
fn read_image_hydration_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("read-image-hydration") {
        return;
    }
    arm_read_image_hydration_pause_for_test();
    match load_read_image_for_test_adapter(&child_store_path()) {
        Ok(_) => {
            println!("{HYDRATION_FINISHED}");
            std::io::stdout()
                .flush()
                .expect("flush hydration-finished marker");
        }
        Err(LoadReadImageAdapterError::NotImplemented) => {
            println!("{HYDRATION_NOT_IMPLEMENTED}");
            std::io::stdout()
                .flush()
                .expect("flush hydration-not-implemented marker");
        }
        Err(err) => panic!("production load_read_image adapter failed: {err}"),
    }
}

/// A direct reader that reaches the committed file the way a real one does:
/// `ReadSession::open` resolves the runtime directory itself, so the
/// breadcrumb this child publishes is the one production publishes. Nothing
/// here hands the session a runtime root, because a deployed reader has
/// nobody to hand it one.
#[test]
fn production_read_session_holder_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("production-read-session-holder") {
        return;
    }
    arm_read_image_hydration_pause_for_test();
    let session = ReadSession::open(child_store_path())
        .expect("production read session opens the committed store file");
    println!("{READ_SESSION_OPENED}");
    std::io::stdout()
        .flush()
        .expect("flush production read-session marker");
    drop(session);
    println!("{HYDRATION_FINISHED}");
    std::io::stdout()
        .flush()
        .expect("flush production read-session completion marker");
}

fn writer_probe_child_result(path: &Path) {
    reset_writer_open_attempts_for_test();
    match Database::open(path) {
        Ok(db) => {
            db.close().expect("close successful writer probe");
            println!("{WRITER_PROBE}acquired:{}", writer_open_attempts_for_test());
        }
        Err(error) => match &error {
            Error::ReadFailure(failure) if failure.kind() == ReadFailureKind::HeldByWriter => {
                println!(
                    "{WRITER_PROBE}writer_refusal:{}:{}",
                    writer_open_attempts_for_test(),
                    encode_hex(error.to_string().as_bytes())
                );
            }
            Error::ReadFailure(failure) if failure.kind() == ReadFailureKind::HeldByReaders => {
                let ReadFailureDetail::HeldByReaders(HeldByReadersDetail {
                    observed_direct_readers,
                    verified_readers,
                }) = failure.detail()
                else {
                    panic!("HeldByReaders must carry its specialized typed detail");
                };
                println!(
                    "{WRITER_PROBE}reader_refusal:{}:{}:{}",
                    writer_open_attempts_for_test(),
                    observed_direct_readers,
                    encode_hex(error.to_string().as_bytes())
                );
                for breadcrumb in verified_readers {
                    println!(
                        "{WRITER_PROBE_BREADCRUMB}{}:{}:{}",
                        breadcrumb.process_id,
                        breadcrumb.process_start.0,
                        encode_hex(breadcrumb.process_name.as_bytes())
                    );
                }
            }
            Error::ReadFailure(failure) => println!(
                "{WRITER_PROBE}other_typed:{}:{:?}",
                writer_open_attempts_for_test(),
                failure.kind()
            ),
            _ => println!(
                "{WRITER_PROBE}other:{}:{}",
                writer_open_attempts_for_test(),
                encode_hex(error.to_string().as_bytes())
            ),
        },
    }
    println!("{WRITER_PROBE}end");
    std::io::stdout()
        .flush()
        .expect("flush writer-probe result");
}

#[test]
fn writer_probe_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("writer-probe") {
        return;
    }
    writer_probe_child_result(&child_store_path());
}

fn replacement_writer_attempt(path: &Path) -> std::result::Result<bool, String> {
    match Database::open(path) {
        Ok(db) => {
            db.close().expect("close replacement contender");
            Ok(true)
        }
        Err(Error::ReadFailure(failure)) if failure.kind() == ReadFailureKind::HeldByWriter => {
            Ok(false)
        }
        Err(Error::ReadFailure(failure)) => Err(format!("typed:{:?}", failure.kind())),
        Err(err) => Err(err.to_string().replace(['\n', '\r'], " ")),
    }
}

#[test]
fn replacement_writer_probe_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("replacement-writer-probe") {
        return;
    }
    let path = child_store_path();
    match replacement_writer_attempt(&path) {
        Ok(false) => println!("{REPLACEMENT_WRITER_REFUSED}"),
        Ok(true) => println!("{REPLACEMENT_WRITER_ACQUIRED}"),
        Err(error) => println!("{REPLACEMENT_WRITER_OTHER}{error}"),
    }
    std::io::stdout()
        .flush()
        .expect("flush replacement writer probe result");
}

#[test]
fn replacement_lock_sentinel_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("replacement-lock-sentinel") {
        return;
    }
    let companion = appended_companion(&child_store_path());
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&companion)
        .unwrap_or_else(|error| panic!("open sentinel companion {}: {error}", companion.display()));
    match fs2::FileExt::try_lock_exclusive(&file) {
        Ok(()) => {
            println!("{SENTINEL_GAP}");
            std::io::stdout()
                .flush()
                .expect("flush initially unguarded companion result");
            return;
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
        Err(error) => {
            println!("{SENTINEL_OTHER}{error}");
            std::io::stdout()
                .flush()
                .expect("flush companion sentinel error");
            return;
        }
    }

    let final_release_permitted = Arc::new(AtomicBool::new(false));
    let command_permission = Arc::clone(&final_release_permitted);
    thread::spawn(move || {
        let mut command = String::new();
        std::io::stdin()
            .read_line(&mut command)
            .expect("read lock-sentinel release command");
        assert_eq!(command.trim(), SENTINEL_PERMIT_FINAL_RELEASE);
        command_permission.store(true, Ordering::SeqCst);
        println!("{SENTINEL_FINAL_PERMISSION}");
        std::io::stdout()
            .flush()
            .expect("flush lock-sentinel final permission");
    });

    println!("{SENTINEL_ARMED}");
    std::io::stdout()
        .flush()
        .expect("flush blocked lock-sentinel marker");
    if let Err(error) = fs2::FileExt::lock_exclusive(&file) {
        println!("{SENTINEL_OTHER}{error}");
        std::io::stdout()
            .flush()
            .expect("flush blocking companion-lock error");
        return;
    }
    if final_release_permitted.load(Ordering::SeqCst) {
        println!("{SENTINEL_ACQUIRED}");
        std::io::stdout()
            .flush()
            .expect("flush expected lock-sentinel acquisition");
    } else {
        println!("{SENTINEL_GAP}");
        std::io::stdout()
            .flush()
            .expect("flush premature lock-sentinel acquisition");
    }
}

fn commanded_replacement_writer_probe(path: &Path) -> std::result::Result<(), String> {
    let output = Command::new(std::env::current_exe().expect("current integration-test binary"))
        .arg("--exact")
        .arg("replacement_writer_probe_child")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "replacement-writer-probe")
        .env(CHILD_PATH_ENV, path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("start replacement writer probe: {error}"))?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if !output.status.success() {
        return Err(format!(
            "replacement writer probe failed: {}: {text}",
            output.status
        ));
    }
    if text.contains(REPLACEMENT_WRITER_REFUSED) {
        return Ok(());
    }
    if text.contains(REPLACEMENT_WRITER_ACQUIRED) {
        return Err("real competing writer acquired during the guarded interval".to_owned());
    }
    if let Some(index) = text.find(REPLACEMENT_WRITER_OTHER) {
        return Err(format!(
            "real competing writer returned the wrong result: {}",
            &text[index + REPLACEMENT_WRITER_OTHER.len()..]
        ));
    }
    Err(format!(
        "replacement writer probe emitted no result: {text}"
    ))
}

struct CompanionLockSentinel {
    child: Child,
    stdout: BufReader<ChildStdout>,
    finished: bool,
}

impl CompanionLockSentinel {
    fn read_marker(&mut self) -> std::result::Result<String, String> {
        let mut line = String::new();
        loop {
            line.clear();
            let read = self
                .stdout
                .read_line(&mut line)
                .map_err(|error| format!("read companion lock sentinel: {error}"))?;
            if read == 0 {
                return Err("companion lock sentinel exited before its marker".to_owned());
            }
            let line = line.trim();
            if line.contains(SENTINEL_ARMED)
                || line.contains(SENTINEL_GAP)
                || line.contains(SENTINEL_FINAL_PERMISSION)
                || line.contains(SENTINEL_ACQUIRED)
                || line.contains(SENTINEL_OTHER)
            {
                return Ok(line.to_owned());
            }
        }
    }

    fn assert_kernel_blocked(&mut self) -> std::result::Result<(), String> {
        if let Some(status) = self
            .child
            .try_wait()
            .map_err(|error| format!("inspect companion lock sentinel: {error}"))?
        {
            let mut remaining = String::new();
            self.stdout
                .read_to_string(&mut remaining)
                .map_err(|error| format!("read finished lock sentinel: {error}"))?;
            self.finished = true;
            return Err(format!(
                "companion guard was not continuous: status={status}, output={remaining}"
            ));
        }
        Ok(())
    }

    fn permit_final_release(&mut self) -> std::result::Result<(), String> {
        let stdin = self
            .child
            .stdin
            .as_mut()
            .ok_or_else(|| "companion lock sentinel lost its command pipe".to_owned())?;
        writeln!(stdin, "{SENTINEL_PERMIT_FINAL_RELEASE}")
            .map_err(|error| format!("permit final companion release: {error}"))?;
        stdin
            .flush()
            .map_err(|error| format!("flush final companion release: {error}"))?;
        loop {
            let line = self.read_marker()?;
            if line.contains(SENTINEL_FINAL_PERMISSION) {
                return Ok(());
            }
            if line.contains(SENTINEL_GAP) {
                return Err("companion lock sentinel acquired before final release".to_owned());
            }
            if let Some(index) = line.find(SENTINEL_OTHER) {
                return Err(line[index + SENTINEL_OTHER.len()..].to_owned());
            }
        }
    }

    fn complete(mut self) -> std::result::Result<(), String> {
        let marker = self.read_marker()?;
        let acquisition = if marker.contains(SENTINEL_ACQUIRED) {
            Ok(())
        } else if marker.contains(SENTINEL_GAP) {
            Err("companion lock sentinel observed a drop before final release".to_owned())
        } else {
            Err(format!("unexpected final lock-sentinel marker: {marker}"))
        };
        let status = self
            .child
            .wait()
            .map_err(|error| format!("wait for companion lock sentinel: {error}"))?;
        self.finished = true;
        if !status.success() {
            return Err(format!("companion lock sentinel failed: {status}"));
        }
        acquisition
    }
}

impl Drop for CompanionLockSentinel {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn spawn_companion_lock_sentinel(
    path: &Path,
) -> std::result::Result<CompanionLockSentinel, String> {
    let mut child = Command::new(std::env::current_exe().expect("current integration-test binary"))
        .arg("--exact")
        .arg("replacement_lock_sentinel_child")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "replacement-lock-sentinel")
        .env(CHILD_PATH_ENV, path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| format!("start companion lock sentinel: {error}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "companion lock sentinel has no stdout".to_owned())?;
    let mut sentinel = CompanionLockSentinel {
        child,
        stdout: BufReader::new(stdout),
        finished: false,
    };
    let marker = sentinel.read_marker()?;
    if marker.contains(SENTINEL_ARMED) {
        Ok(sentinel)
    } else if marker.contains(SENTINEL_GAP) {
        Err("sentinel acquired the companion at the first guard checkpoint".to_owned())
    } else {
        Err(format!("companion lock sentinel did not arm: {marker}"))
    }
}

#[test]
fn companion_crash_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("companion-crash") {
        return;
    }
    let boundary = match std::env::var(CHILD_BOUNDARY_ENV)
        .expect("crash child receives a boundary")
        .as_str()
    {
        "SlotPayloadWrite" => CompanionWriteBoundary::SlotPayloadWrite,
        "SlotPayloadSync" => CompanionWriteBoundary::SlotPayloadSync,
        "SlotChecksumWrite" => CompanionWriteBoundary::SlotChecksumWrite,
        "SlotChecksumSync" => CompanionWriteBoundary::SlotChecksumSync,
        "ActiveSlotWrite" => CompanionWriteBoundary::ActiveSlotWrite,
        "ActiveSlotSync" => CompanionWriteBoundary::ActiveSlotSync,
        other => panic!("unknown companion crash boundary: {other}"),
    };
    arm_companion_crash_for_test(boundary);
    let db = Database::open(child_store_path()).expect("writer reaches companion update");
    db.close().expect("unarmed writer close");
}

fn store_replacement_boundary_from_wire(value: &str) -> StoreReplacementBoundary {
    match value {
        "GuardAcquired" => StoreReplacementBoundary::GuardAcquired,
        "BeforeAtomicReplacement" => StoreReplacementBoundary::BeforeAtomicReplacement,
        "AfterAtomicReplacement" => StoreReplacementBoundary::AfterAtomicReplacement,
        "ReplacementPublishedBeforeGuardRelease" => {
            StoreReplacementBoundary::ReplacementPublishedBeforeGuardRelease
        }
        other => panic!("unknown store replacement boundary: {other}"),
    }
}

#[test]
#[cfg(unix)]
fn reset_replacement_crash_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("reset-replacement-crash") {
        return;
    }
    let crash_boundary = store_replacement_boundary_from_wire(
        &std::env::var(CHILD_REPLACEMENT_BOUNDARY_ENV)
            .expect("reset crash child receives a replacement boundary"),
    );
    let _sequence = ArmedStoreReplacementSequence::arm();
    let path = child_store_path();
    let baseline = directory_entries(&path);
    let reset = StoreReplacementTask::spawn(path.clone());
    for expected in StoreReplacementBoundary::ALL {
        match next_store_replacement_event_for_test() {
            StoreReplacementEvent::Checkpoint(actual) => {
                assert_eq!(actual, expected);
                if actual == StoreReplacementBoundary::BeforeAtomicReplacement {
                    let target = discover_new_reset_target(&path, &baseline)
                        .expect("discover exact generated reset target");
                    let (device, inode) = file_identity(&target);
                    let fingerprint = store_fingerprint_for_test(&target)
                        .expect("fingerprint exact generated reset target");
                    println!(
                        "{STORE_REPLACEMENT_GENERATED_TARGET}{}",
                        encode_hex(target.as_os_str().as_encoded_bytes())
                    );
                    println!("{STORE_REPLACEMENT_GENERATED_IDENTITY}{device}:{inode}");
                    println!(
                        "{STORE_REPLACEMENT_GENERATED_FINGERPRINT}{}",
                        encode_hex(&fingerprint)
                    );
                    std::io::stdout()
                        .flush()
                        .expect("flush generated reset target evidence");
                }
                if actual == crash_boundary {
                    println!("{STORE_REPLACEMENT_CRASH_BOUNDARY}{actual:?}");
                    std::io::stdout()
                        .flush()
                        .expect("flush reset replacement crash boundary");
                    std::process::abort();
                }
                release_store_replacement_checkpoint_for_test();
            }
            event => panic!("reset ended before crash boundary {crash_boundary:?}: {event:?}"),
        }
    }
    let result = reset.join().expect("join unexpectedly completed reset");
    panic!("reset completed before armed crash: {result:?}");
}

struct HeldChild {
    child: Child,
    stdout: BufReader<ChildStdout>,
    finished: bool,
}

impl HeldChild {
    fn wait_for_opened(&mut self) {
        let mut line = String::new();
        loop {
            line.clear();
            let read = self
                .stdout
                .read_line(&mut line)
                .expect("read holder output");
            assert_ne!(read, 0, "holder exited before publishing opened marker");
            if line.contains(HOLDER_OPENED) {
                return;
            }
        }
    }

    fn release(mut self) {
        let stdin = self.child.stdin.as_mut().expect("holder command pipe");
        writeln!(stdin, "{RELEASE}").expect("release holder");
        stdin.flush().expect("flush holder release");
        let status = self.child.wait().expect("wait for holder");
        self.finished = true;
        assert!(status.success(), "holder exits cleanly: {status}");
    }
}

impl Drop for HeldChild {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn spawn_holder(role: &str, test_name: &str, path: &Path) -> HeldChild {
    let mut child = Command::new(std::env::current_exe().expect("current integration-test binary"))
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, role)
        .env(CHILD_PATH_ENV, path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start holder child");
    let stdout = child.stdout.take().expect("holder stdout");
    HeldChild {
        child,
        stdout: BufReader::new(stdout),
        finished: false,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HydrationEvidence {
    breadcrumb_path: Option<PathBuf>,
    breadcrumb: Option<ReaderBreadcrumb>,
}

struct HydrationChild {
    child: Child,
    stdout: BufReader<ChildStdout>,
    finished: bool,
}

impl HydrationChild {
    fn process_id(&self) -> u32 {
        self.child.id()
    }

    fn wait_until_held(&mut self) -> std::result::Result<HydrationEvidence, String> {
        let mut line = String::new();
        loop {
            line.clear();
            let read = self
                .stdout
                .read_line(&mut line)
                .map_err(|err| format!("read hydration output: {err}"))?;
            if read == 0 {
                return Err("hydration child exited before acquiring its real read lock".to_owned());
            }
            if line.contains(HYDRATION_NOT_IMPLEMENTED) {
                return Err(
                    "load_read_image production path reports typed NotImplemented".to_owned(),
                );
            }
            let Some(index) = line.find(HYDRATION_HELD) else {
                continue;
            };
            let value = line[index + HYDRATION_HELD.len()..].trim();
            if value == "unverified" {
                return Ok(HydrationEvidence {
                    breadcrumb_path: None,
                    breadcrumb: None,
                });
            }
            let mut fields = value.splitn(4, ':');
            let breadcrumb_path = decode_hex(
                fields
                    .next()
                    .ok_or_else(|| "hydration marker lacks breadcrumb path".to_owned())?,
            )?;
            let breadcrumb_path = PathBuf::from(
                String::from_utf8(breadcrumb_path)
                    .map_err(|err| format!("breadcrumb path is not UTF-8: {err}"))?,
            );
            let process_id = fields
                .next()
                .ok_or_else(|| "hydration marker lacks process id".to_owned())?
                .parse::<u32>()
                .map_err(|err| format!("parse hydration process id: {err}"))?;
            let process_start = fields
                .next()
                .ok_or_else(|| "hydration marker lacks process start".to_owned())?
                .parse::<u64>()
                .map_err(|err| format!("parse hydration process start: {err}"))?;
            let name = decode_hex(
                fields
                    .next()
                    .ok_or_else(|| "hydration marker lacks process name".to_owned())?,
            )?;
            let process_name = String::from_utf8(name)
                .map_err(|err| format!("hydration process name is not UTF-8: {err}"))?;
            return Ok(HydrationEvidence {
                breadcrumb_path: Some(breadcrumb_path),
                breadcrumb: Some(ReaderBreadcrumb {
                    process_id,
                    process_name,
                    process_start: ProcessStartIdentity(process_start),
                }),
            });
        }
    }

    fn release(mut self) -> std::result::Result<(), String> {
        let stdin = self
            .child
            .stdin
            .as_mut()
            .ok_or_else(|| "hydration child lost release pipe".to_owned())?;
        writeln!(stdin, "{RELEASE}").map_err(|err| format!("release hydration: {err}"))?;
        stdin
            .flush()
            .map_err(|err| format!("flush hydration release: {err}"))?;
        let mut line = String::new();
        let mut finished_marker = false;
        while self
            .stdout
            .read_line(&mut line)
            .map_err(|err| format!("read hydration completion: {err}"))?
            != 0
        {
            if line.contains(HYDRATION_FINISHED) {
                finished_marker = true;
                break;
            }
            line.clear();
        }
        let status = self
            .child
            .wait()
            .map_err(|err| format!("wait for hydration child: {err}"))?;
        self.finished = true;
        if !finished_marker || !status.success() {
            return Err(format!(
                "hydration did not decode and clean up successfully: marker={finished_marker}, status={status}"
            ));
        }
        Ok(())
    }
}

impl Drop for HydrationChild {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn spawn_hydration_from(
    executable: &Path,
    path: &Path,
    runtime_directory: &Path,
) -> HydrationChild {
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg("read_image_hydration_child")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "read-image-hydration")
        .env(CHILD_PATH_ENV, path)
        .env("XDG_RUNTIME_DIR", runtime_directory)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start read-image hydration child");
    let stdout = child.stdout.take().expect("hydration stdout");
    HydrationChild {
        child,
        stdout: BufReader::new(stdout),
        finished: false,
    }
}

fn spawn_hydration(path: &Path, runtime_directory: &Path) -> HydrationChild {
    let executable = std::env::current_exe().expect("current integration-test binary");
    spawn_hydration_from(&executable, path, runtime_directory)
}

/// Park a reader inside a production `ReadSession::open` at the same
/// hydration checkpoint the estate already uses. The child receives the
/// runtime root only as `XDG_RUNTIME_DIR`, exactly as a deployed reader does,
/// and resolves it through the production resolver rather than a test adapter.
fn spawn_production_read_session(path: &Path, runtime_directory: &Path) -> HydrationChild {
    let mut child = Command::new(std::env::current_exe().expect("current integration-test binary"))
        .arg("--exact")
        .arg("production_read_session_holder_child")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "production-read-session-holder")
        .env(CHILD_PATH_ENV, path)
        .env("XDG_RUNTIME_DIR", runtime_directory)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start production read-session child");
    let stdout = child.stdout.take().expect("production read-session stdout");
    HydrationChild {
        child,
        stdout: BufReader::new(stdout),
        finished: false,
    }
}

#[cfg(target_os = "linux")]
const TERMINAL_CONTROL_READER_EXECUTABLE_BASENAME: &str = "live-\u{1b}\r\n\\reader";

#[cfg(target_os = "linux")]
fn copied_hydration_executable_with_terminal_control_name(root: &Path) -> PathBuf {
    let copied = root.join(TERMINAL_CONTROL_READER_EXECUTABLE_BASENAME);
    fs::copy(
        std::env::current_exe().expect("current integration-test binary"),
        &copied,
    )
    .unwrap_or_else(|error| panic!("copy hydration executable {}: {error}", copied.display()));
    fs::set_permissions(&copied, fs::Permissions::from_mode(0o700))
        .unwrap_or_else(|error| panic!("make copied hydration executable runnable: {error}"));
    copied
}

#[cfg(target_os = "linux")]
fn linux_live_process_name_evidence(process_id: u32) -> [String; 3] {
    let process_directory = PathBuf::from(format!("/proc/{process_id}"));
    let comm = fs::read_to_string(process_directory.join("comm"))
        .expect("read copied hydration process comm");
    let comm = comm
        .strip_suffix('\n')
        .expect("Linux process comm ends with its record newline")
        .to_owned();
    let executable = fs::read_link(process_directory.join("exe"))
        .expect("read copied hydration process executable")
        .file_name()
        .and_then(|name| name.to_str())
        .expect("copied hydration executable basename is UTF-8")
        .to_owned();
    let command_line = fs::read(process_directory.join("cmdline"))
        .expect("read copied hydration process command line");
    let argument_zero = command_line
        .split(|byte| *byte == 0)
        .next()
        .filter(|argument| !argument.is_empty())
        .expect("copied hydration process command line has argv[0]");
    let argument_zero =
        std::str::from_utf8(argument_zero).expect("copied hydration process argv[0] is UTF-8");
    let argument_zero = Path::new(argument_zero)
        .file_name()
        .and_then(|name| name.to_str())
        .expect("copied hydration process argv[0] has a UTF-8 basename")
        .to_owned();
    [comm, executable, argument_zero]
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WriterProbeResult {
    Acquired {
        attempts: usize,
    },
    HeldByWriter {
        attempts: usize,
    },
    HeldByReaders {
        attempts: usize,
        observed_direct_readers: u64,
        verified_readers: Vec<ReaderBreadcrumb>,
        message: String,
    },
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RawRedbWriterProbeResult {
    Acquired,
    DatabaseAlreadyOpen,
    Other(String),
}

fn probe_raw_redb_writer(path: &Path) -> RawRedbWriterProbeResult {
    let output = Command::new(std::env::current_exe().expect("current integration-test binary"))
        .arg("--exact")
        .arg("raw_redb_writer_probe_child")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "raw-redb-writer-probe")
        .env(CHILD_PATH_ENV, path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run raw-redb writer probe");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if !output.status.success() {
        return RawRedbWriterProbeResult::Other(format!(
            "raw-redb writer process failed with {}: {text}",
            output.status
        ));
    }
    if text.contains(&format!("{RAW_REDB_WRITER_PROBE}database-already-open")) {
        RawRedbWriterProbeResult::DatabaseAlreadyOpen
    } else if text.contains(&format!("{RAW_REDB_WRITER_PROBE}acquired")) {
        RawRedbWriterProbeResult::Acquired
    } else if let Some(index) = text.find(&format!("{RAW_REDB_WRITER_PROBE}other:")) {
        RawRedbWriterProbeResult::Other(
            text[index + RAW_REDB_WRITER_PROBE.len() + "other:".len()..]
                .trim()
                .to_owned(),
        )
    } else {
        RawRedbWriterProbeResult::Other(format!("raw-redb writer emitted no result: {text}"))
    }
}

fn probe_writer(path: &Path, runtime_directory: &Path) -> WriterProbeResult {
    let output = Command::new(std::env::current_exe().expect("current integration-test binary"))
        .arg("--exact")
        .arg("writer_probe_child")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "writer-probe")
        .env(CHILD_PATH_ENV, path)
        .env("XDG_RUNTIME_DIR", runtime_directory)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run writer probe");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let mut result = None;
    let mut verified_readers = Vec::new();
    for line in text.lines() {
        if let Some(index) = line.find(WRITER_PROBE_BREADCRUMB) {
            let mut fields = line[index + WRITER_PROBE_BREADCRUMB.len()..].splitn(3, ':');
            let process_id = fields.next().unwrap_or_default().parse::<u32>();
            let process_start = fields.next().unwrap_or_default().parse::<u64>();
            let process_name = fields
                .next()
                .ok_or_else(|| "missing writer-probe process name".to_owned())
                .and_then(decode_hex)
                .and_then(|bytes| String::from_utf8(bytes).map_err(|err| err.to_string()));
            match (process_id, process_start, process_name) {
                (Ok(process_id), Ok(process_start), Ok(process_name)) => {
                    verified_readers.push(ReaderBreadcrumb {
                        process_id,
                        process_name,
                        process_start: ProcessStartIdentity(process_start),
                    });
                }
                values => {
                    return WriterProbeResult::Other(format!("invalid breadcrumb: {values:?}"));
                }
            }
            continue;
        }
        let Some(index) = line.find(WRITER_PROBE) else {
            continue;
        };
        let value = &line[index + WRITER_PROBE.len()..];
        if value == "end" {
            continue;
        }
        let mut fields = value.split(':');
        match fields.next() {
            Some("acquired") => {
                result = fields
                    .next()
                    .and_then(|value| value.parse().ok())
                    .map(|attempts| WriterProbeResult::Acquired { attempts });
            }
            Some("writer_refusal") => {
                result = fields
                    .next()
                    .and_then(|value| value.parse().ok())
                    .map(|attempts| WriterProbeResult::HeldByWriter { attempts });
            }
            Some("reader_refusal") => {
                let attempts = fields.next().and_then(|value| value.parse::<usize>().ok());
                let observed = fields.next().and_then(|value| value.parse::<u64>().ok());
                let message = fields
                    .next()
                    .and_then(|value| decode_hex(value).ok())
                    .and_then(|bytes| String::from_utf8(bytes).ok());
                result = match (attempts, observed, message) {
                    (Some(attempts), Some(observed_direct_readers), Some(message)) => {
                        Some(WriterProbeResult::HeldByReaders {
                            attempts,
                            observed_direct_readers,
                            verified_readers: Vec::new(),
                            message,
                        })
                    }
                    _ => Some(WriterProbeResult::Other(format!(
                        "invalid held-by-readers marker: {value}"
                    ))),
                };
            }
            Some(other) => {
                result = Some(WriterProbeResult::Other(format!(
                    "{other}:{}",
                    fields.collect::<Vec<_>>().join(":")
                )));
            }
            None => {}
        }
    }
    match result {
        Some(WriterProbeResult::HeldByReaders {
            attempts,
            observed_direct_readers,
            message,
            ..
        }) => WriterProbeResult::HeldByReaders {
            attempts,
            observed_direct_readers,
            verified_readers,
            message,
        },
        Some(result) => result,
        None => WriterProbeResult::Other(format!("writer probe emitted no result: {text}")),
    }
}

struct CrashedWriter {
    process_id: u32,
    output: Output,
}

fn spawn_companion_crash(path: &Path, boundary: CompanionWriteBoundary) -> CrashedWriter {
    let child = Command::new(std::env::current_exe().expect("current integration-test binary"))
        .arg("--exact")
        .arg("companion_crash_child")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "companion-crash")
        .env(CHILD_PATH_ENV, path)
        .env(CHILD_BOUNDARY_ENV, format!("{boundary:?}"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start companion crash child");
    let process_id = child.id();
    let output = child
        .wait_with_output()
        .expect("wait for companion crash child");
    CrashedWriter { process_id, output }
}

fn spawn_reset_replacement_crash(path: &Path, boundary: StoreReplacementBoundary) -> Output {
    Command::new(std::env::current_exe().expect("current integration-test binary"))
        .arg("--exact")
        .arg("reset_replacement_crash_child")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "reset-replacement-crash")
        .env(CHILD_PATH_ENV, path)
        .env(CHILD_REPLACEMENT_BOUNDARY_ENV, format!("{boundary:?}"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap_or_else(|error| panic!("start reset crash at {boundary:?}: {error}"))
}

#[cfg(unix)]
fn reset_generated_target_from_output(output: &str) -> Option<PathBuf> {
    output.lines().find_map(|line| {
        let index = line.find(STORE_REPLACEMENT_GENERATED_TARGET)?;
        let bytes = decode_hex(&line[index + STORE_REPLACEMENT_GENERATED_TARGET.len()..]).ok()?;
        Some(PathBuf::from(OsString::from_vec(bytes)))
    })
}

fn reset_generated_identity_from_output(output: &str) -> Option<(u64, u64)> {
    output.lines().find_map(|line| {
        let index = line.find(STORE_REPLACEMENT_GENERATED_IDENTITY)?;
        let mut fields = line[index + STORE_REPLACEMENT_GENERATED_IDENTITY.len()..].split(':');
        Some((fields.next()?.parse().ok()?, fields.next()?.parse().ok()?))
    })
}

fn reset_generated_fingerprint_from_output(output: &str) -> Option<[u8; 32]> {
    output.lines().find_map(|line| {
        let index = line.find(STORE_REPLACEMENT_GENERATED_FINGERPRINT)?;
        decode_hex(&line[index + STORE_REPLACEMENT_GENERATED_FINGERPRINT.len()..])
            .ok()?
            .try_into()
            .ok()
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EntryKind {
    Directory,
    File,
    Symlink,
    BlockDevice,
    CharacterDevice,
    Fifo,
    Socket,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EntryInventory {
    kind: EntryKind,
    mode: Option<u32>,
    link_target: Option<PathBuf>,
    content_hash: Option<[u8; 32]>,
}

fn inventory_directory(root: &Path) -> BTreeMap<PathBuf, EntryInventory> {
    fn visit(root: &Path, path: &Path, entries: &mut BTreeMap<PathBuf, EntryInventory>) {
        let metadata = fs::symlink_metadata(path)
            .unwrap_or_else(|err| panic!("inspect {}: {err}", path.display()));
        let file_type = metadata.file_type();
        let kind = if file_type.is_dir() {
            EntryKind::Directory
        } else if file_type.is_file() {
            EntryKind::File
        } else if file_type.is_symlink() {
            EntryKind::Symlink
        } else {
            #[cfg(unix)]
            {
                if file_type.is_block_device() {
                    EntryKind::BlockDevice
                } else if file_type.is_char_device() {
                    EntryKind::CharacterDevice
                } else if file_type.is_fifo() {
                    EntryKind::Fifo
                } else if file_type.is_socket() {
                    EntryKind::Socket
                } else {
                    EntryKind::Other
                }
            }
            #[cfg(not(unix))]
            {
                EntryKind::Other
            }
        };
        #[cfg(unix)]
        let mode = Some(metadata.mode());
        #[cfg(not(unix))]
        let mode = None;
        let relative = path
            .strip_prefix(root)
            .expect("inventory path stays below root");
        let relative = if relative.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            relative.to_path_buf()
        };
        let link_target = file_type
            .is_symlink()
            .then(|| fs::read_link(path).expect("read inventory symlink"));
        let content_hash = file_type.is_file().then(|| {
            *blake3::hash(
                &fs::read(path).unwrap_or_else(|err| panic!("read {}: {err}", path.display())),
            )
            .as_bytes()
        });
        entries.insert(
            relative,
            EntryInventory {
                kind: kind.clone(),
                mode,
                link_target,
                content_hash,
            },
        );
        if kind == EntryKind::Directory {
            let mut children = fs::read_dir(path)
                .unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
                .map(|entry| entry.expect("directory entry").path())
                .collect::<Vec<_>>();
            children.sort();
            for child in children {
                visit(root, &child, entries);
            }
        }
    }

    let mut entries = BTreeMap::new();
    visit(root, root, &mut entries);
    entries
}

fn appended_companion(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".lock");
    PathBuf::from(name)
}

fn pre_change_replaced_companion(path: &Path) -> PathBuf {
    path.with_extension("lock")
}

fn remove_companion_candidates(path: &Path) {
    let candidates = BTreeSet::from([
        appended_companion(path),
        pre_change_replaced_companion(path),
    ]);
    for candidate in candidates {
        match fs::remove_file(&candidate) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("remove {}: {error}", candidate.display()),
        }
    }
}

fn seed_store(path: &Path) {
    let db = Database::open(path).unwrap_or_else(|err| panic!("create {}: {err}", path.display()));
    db.execute(
        "CREATE TABLE proof (id INTEGER PRIMARY KEY, body TEXT)",
        &Default::default(),
    )
    .unwrap_or_else(|err| panic!("seed {}: {err}", path.display()));
    db.close()
        .unwrap_or_else(|err| panic!("close {}: {err}", path.display()));
}

fn create_runtime_directory(path: &Path) {
    fs::create_dir_all(path).expect("create runtime directory");
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .expect("make runtime directory owner-only");
}

#[cfg(unix)]
fn file_identity(path: &Path) -> (u64, u64) {
    let metadata =
        fs::metadata(path).unwrap_or_else(|err| panic!("stat {}: {err}", path.display()));
    (metadata.dev(), metadata.ino())
}

#[cfg(unix)]
#[derive(Debug)]
struct PreservedResetDecoys {
    files: BTreeMap<PathBuf, ((u64, u64), Vec<u8>)>,
    /// The complete, openable store whose pathname follows the exact
    /// generated grammar. Nothing durably records it, so recovery must leave
    /// it and its sidecars alone.
    complete_generated_shaped_store: PathBuf,
}

#[cfg(unix)]
fn seed_preserved_reset_decoys(path: &Path) -> PreservedResetDecoys {
    let mut obsolete_name = path.as_os_str().to_os_string();
    obsolete_name.push(".replacement.tmp");
    let obsolete = PathBuf::from(obsolete_name);
    let mut matching_name = path.as_os_str().to_os_string();
    matching_name.push(".replacement-decoy-not-a-generated-id.tmp");
    let matching = PathBuf::from(matching_name);
    let mut sidecar_name = matching.as_os_str().to_os_string();
    sidecar_name.push(".journal");
    // Same shape a live reset allocates -- `.replacement-<32 lowercase
    // hex>.tmp` -- with a real current-format store behind it, so a
    // pathname-pattern or "looks generated" cleanup would take it.
    let mut complete_name = path.as_os_str().to_os_string();
    complete_name.push(".replacement-0000dead00004000800000000000002a.tmp");
    let complete = PathBuf::from(complete_name);
    seed_store(&complete);
    let mut complete_sidecar_name = complete.as_os_str().to_os_string();
    complete_sidecar_name.push(".journal");
    let complete_sidecar = PathBuf::from(complete_sidecar_name);
    fs::write(
        &complete_sidecar,
        b"complete generated-shaped sidecar decoy",
    )
    .expect("seed the complete generated-shaped reset sidecar decoy");
    let paths = [
        (
            obsolete.clone(),
            b"obsolete fixed reset target decoy".as_slice(),
        ),
        (
            appended_companion(&obsolete),
            b"obsolete fixed reset sidecar decoy".as_slice(),
        ),
        (matching.clone(), b"matching reset target decoy".as_slice()),
        (
            appended_companion(&matching),
            b"matching reset companion decoy".as_slice(),
        ),
        (
            PathBuf::from(sidecar_name),
            b"matching reset journal decoy".as_slice(),
        ),
    ];
    let mut files = BTreeMap::new();
    for (decoy, bytes) in paths {
        fs::write(&decoy, bytes)
            .unwrap_or_else(|error| panic!("seed reset decoy {}: {error}", decoy.display()));
        files.insert(decoy.clone(), (file_identity(&decoy), bytes.to_vec()));
    }
    for produced in [
        complete.clone(),
        appended_companion(&complete),
        complete_sidecar,
    ] {
        let bytes = fs::read(&produced).unwrap_or_else(|error| {
            panic!("read seeded reset decoy {}: {error}", produced.display())
        });
        files.insert(produced.clone(), (file_identity(&produced), bytes));
    }
    PreservedResetDecoys {
        files,
        complete_generated_shaped_store: complete,
    }
}

#[cfg(unix)]
fn assert_reset_decoys_preserved(decoys: &PreservedResetDecoys) {
    for (path, (identity, bytes)) in &decoys.files {
        assert_eq!(
            file_identity(path),
            *identity,
            "reset recovery replaced decoy {}",
            path.display()
        );
        assert_eq!(
            fs::read(path).unwrap_or_else(|error| panic!("read decoy {}: {error}", path.display())),
            *bytes,
            "reset recovery changed decoy {}",
            path.display()
        );
    }
}

#[cfg(unix)]
fn independently_locate_reset_generated_inode(
    path: &Path,
    baseline: &BTreeSet<PathBuf>,
    identity: (u64, u64),
) -> PathBuf {
    let mut candidates = directory_entries(path)
        .into_iter()
        .filter(|candidate| candidate == path || !baseline.contains(candidate))
        .filter(|candidate| {
            fs::metadata(candidate).is_ok_and(|metadata| {
                metadata.file_type().is_file() && (metadata.dev(), metadata.ino()) == identity
            })
        })
        .collect::<Vec<_>>();
    candidates.sort();
    assert_eq!(
        candidates.len(),
        1,
        "generated reset inode must have one independently discoverable path: {candidates:?}"
    );
    candidates.pop().expect("one generated reset inode")
}

fn new_reset_generated_residue(path: &Path, baseline: &BTreeSet<PathBuf>) -> Vec<PathBuf> {
    let source_name = path
        .file_name()
        .expect("reset store has a file name")
        .to_string_lossy();
    let prefix = format!("{source_name}.replacement-");
    let mut residue = directory_entries(path)
        .into_iter()
        .filter(|candidate| !baseline.contains(candidate))
        .filter(|candidate| {
            candidate
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(&prefix))
        })
        .collect::<Vec<_>>();
    residue.sort();
    residue
}

// These literals deliberately do not come from the production companion
// inspector. They are an independent on-disk contract parser, so a writer
// cannot return a convincing object while putting different bytes elsewhere.
#[cfg(unix)]
const RAW_COMPANION_FILE_MAGIC: &[u8; 16] = b"CTXDB-COMPANION1";
#[cfg(unix)]
const RAW_COMPANION_SLOT_PAYLOAD_CAPACITY: usize = 4 * 1024;
#[cfg(unix)]
const RAW_COMPANION_SLOT_BYTES: usize = 4 + RAW_COMPANION_SLOT_PAYLOAD_CAPACITY + 32;
#[cfg(unix)]
const RAW_COMPANION_FIRST_SLOT_OFFSET: usize = RAW_COMPANION_FILE_MAGIC.len();
#[cfg(unix)]
const RAW_COMPANION_SELECTOR_OFFSET: usize =
    RAW_COMPANION_FIRST_SLOT_OFFSET + 2 * RAW_COMPANION_SLOT_BYTES;
#[cfg(unix)]
const RAW_COMPANION_SELECTOR_BODY_BYTES: usize = 24;
#[cfg(unix)]
const RAW_COMPANION_SELECTOR_BYTES: usize = RAW_COMPANION_SELECTOR_BODY_BYTES + 32;
#[cfg(unix)]
const RAW_COMPANION_FILE_BYTES: usize =
    RAW_COMPANION_SELECTOR_OFFSET + RAW_COMPANION_SELECTOR_BYTES;
#[cfg(unix)]
const RAW_COMPANION_SELECTOR_MAGIC: &[u8; 8] = b"ACTIVEV1";

#[cfg(unix)]
const RAW_COMPANION_PENDING_MAGIC: &[u8] = b"contextdb-replacement-v1";

/// One recorded replacement intent, decoded from the durable slot bytes:
/// the fingerprint of the exact store the writer intends to install, and the
/// complete companion record it intends to publish for it.
#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct RawPendingIntent {
    store_fingerprint: [u8; 32],
    intended_record: CompanionRecordObservation,
    checksummed_payload: Vec<u8>,
}

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
enum RawCompanionSlot {
    Vacant,
    Complete(CompanionRecordObservation),
    PendingIntent(RawPendingIntent),
    Torn(String),
}

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedRawCompanion {
    active_slot: usize,
    selector_generation: u64,
    slots: [RawCompanionSlot; 2],
    active_record: CompanionRecordObservation,
}

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct RawCompanionObservation {
    identity: (u64, u64),
    bytes: Vec<u8>,
    parsed: ParsedRawCompanion,
}

#[cfg(unix)]
fn raw_companion_slot_offset(slot: usize) -> usize {
    RAW_COMPANION_FIRST_SLOT_OFFSET + slot * RAW_COMPANION_SLOT_BYTES
}

/// Decodes one recorded replacement intent out of the durable slot payload:
/// `magic || store fingerprint || record length || record || record
/// checksum`, with the slot's own checksum verified here rather than taken
/// from the writer.
#[cfg(unix)]
fn parse_raw_pending_payload(
    payload: &[u8],
    checksum: [u8; 32],
) -> std::result::Result<RawPendingIntent, String> {
    if *blake3::hash(payload).as_bytes() != checksum {
        return Err("replacement intent checksum mismatch".to_owned());
    }
    let mut offset = RAW_COMPANION_PENDING_MAGIC.len();
    let store_fingerprint: [u8; 32] = payload
        .get(offset..offset + 32)
        .ok_or_else(|| "replacement intent has no store fingerprint".to_owned())?
        .try_into()
        .map_err(|_| "replacement intent store fingerprint is truncated".to_owned())?;
    offset += 32;
    let intended_length = u32::from_le_bytes(
        payload
            .get(offset..offset + 4)
            .ok_or_else(|| "replacement intent has no record length".to_owned())?
            .try_into()
            .map_err(|_| "replacement intent record length is truncated".to_owned())?,
    ) as usize;
    offset += 4;
    let intended_payload = payload
        .get(offset..offset + intended_length)
        .ok_or_else(|| "replacement intent record is truncated".to_owned())?
        .to_vec();
    offset += intended_length;
    let intended_checksum: [u8; 32] = payload
        .get(offset..offset + 32)
        .ok_or_else(|| "replacement intent has no record checksum".to_owned())?
        .try_into()
        .map_err(|_| "replacement intent record checksum is truncated".to_owned())?;
    offset += 32;
    if offset != payload.len() {
        return Err("replacement intent carries trailing bytes".to_owned());
    }
    let intended_record = decode_companion_record_for_test(&intended_payload, intended_checksum)?;
    Ok(RawPendingIntent {
        store_fingerprint,
        intended_record,
        checksummed_payload: payload.to_vec(),
    })
}

#[cfg(unix)]
fn parse_raw_companion_slot(slot: &[u8]) -> std::result::Result<RawCompanionSlot, String> {
    if slot.len() != RAW_COMPANION_SLOT_BYTES {
        return Err(format!(
            "companion slot has {} bytes, expected {RAW_COMPANION_SLOT_BYTES}",
            slot.len()
        ));
    }
    let payload_length = u32::from_le_bytes(
        slot[..4]
            .try_into()
            .map_err(|_| "companion slot length is truncated".to_owned())?,
    ) as usize;
    let payload_region = &slot[4..4 + RAW_COMPANION_SLOT_PAYLOAD_CAPACITY];
    let checksum: [u8; 32] = slot[4 + RAW_COMPANION_SLOT_PAYLOAD_CAPACITY..]
        .try_into()
        .map_err(|_| "companion slot checksum is truncated".to_owned())?;
    if payload_length == 0 {
        if payload_region.iter().any(|byte| *byte != 0) || checksum != [0; 32] {
            return Err("vacant companion slot contains nonzero record bytes".to_owned());
        }
        return Ok(RawCompanionSlot::Vacant);
    }
    if payload_length > RAW_COMPANION_SLOT_PAYLOAD_CAPACITY {
        return Err(format!(
            "companion slot payload length {payload_length} exceeds capacity"
        ));
    }
    if payload_region[payload_length..]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err("companion slot has nonzero bytes after its payload".to_owned());
    }
    let payload = &payload_region[..payload_length];
    if payload.starts_with(RAW_COMPANION_PENDING_MAGIC) {
        return parse_raw_pending_payload(payload, checksum).map(RawCompanionSlot::PendingIntent);
    }
    decode_companion_record_for_test(payload, checksum).map(RawCompanionSlot::Complete)
}

#[cfg(unix)]
fn parse_raw_companion_bytes(bytes: &[u8]) -> std::result::Result<ParsedRawCompanion, String> {
    if bytes.len() != RAW_COMPANION_FILE_BYTES {
        return Err(format!(
            "companion inode has {} bytes, expected {RAW_COMPANION_FILE_BYTES}",
            bytes.len()
        ));
    }
    if &bytes[..RAW_COMPANION_FILE_MAGIC.len()] != RAW_COMPANION_FILE_MAGIC {
        return Err("companion file magic mismatch".to_owned());
    }

    let slots: [RawCompanionSlot; 2] = std::array::from_fn(|slot| {
        let offset = raw_companion_slot_offset(slot);
        parse_raw_companion_slot(&bytes[offset..offset + RAW_COMPANION_SLOT_BYTES])
            .unwrap_or_else(RawCompanionSlot::Torn)
    });

    let selector = &bytes[RAW_COMPANION_SELECTOR_OFFSET..];
    let selector_body = &selector[..RAW_COMPANION_SELECTOR_BODY_BYTES];
    if &selector_body[..RAW_COMPANION_SELECTOR_MAGIC.len()] != RAW_COMPANION_SELECTOR_MAGIC {
        return Err("companion selector magic mismatch".to_owned());
    }
    let active_slot = usize::from(selector_body[8]);
    if active_slot >= 2 {
        return Err(format!(
            "companion selector names invalid slot {active_slot}"
        ));
    }
    if selector_body[9..16].iter().any(|byte| *byte != 0) {
        return Err("companion selector reserved bytes are nonzero".to_owned());
    }
    let selector_generation = u64::from_le_bytes(
        selector_body[16..24]
            .try_into()
            .map_err(|_| "companion selector generation is truncated".to_owned())?,
    );
    let selector_checksum: [u8; 32] = selector[RAW_COMPANION_SELECTOR_BODY_BYTES..]
        .try_into()
        .map_err(|_| "companion selector checksum is truncated".to_owned())?;
    if *blake3::hash(selector_body).as_bytes() != selector_checksum {
        return Err("companion selector checksum mismatch".to_owned());
    }
    let active_record = match &slots[active_slot] {
        RawCompanionSlot::Complete(record) => record.clone(),
        RawCompanionSlot::Vacant => {
            return Err("companion selector points at a vacant slot".to_owned());
        }
        RawCompanionSlot::PendingIntent(_) => {
            return Err(
                "companion selector points at an unpublished replacement intent".to_owned(),
            );
        }
        RawCompanionSlot::Torn(error) => {
            return Err(format!("companion selector points at a torn slot: {error}"));
        }
    };
    if active_record.fields.generation != selector_generation {
        return Err(format!(
            "selector generation {selector_generation} does not match active record generation {}",
            active_record.fields.generation
        ));
    }
    Ok(ParsedRawCompanion {
        active_slot,
        selector_generation,
        slots,
        active_record,
    })
}

#[cfg(unix)]
fn read_raw_companion_from_exact_inode(
    path: &Path,
    expected_identity: (u64, u64),
) -> std::result::Result<RawCompanionObservation, String> {
    let mut file = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|error| format!("open raw companion {}: {error}", path.display()))?;
    let metadata_before = file
        .metadata()
        .map_err(|error| format!("stat opened companion {}: {error}", path.display()))?;
    let identity_before = (metadata_before.dev(), metadata_before.ino());
    if identity_before != expected_identity {
        return Err(format!(
            "opened companion inode {identity_before:?}, expected {expected_identity:?}"
        ));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| format!("read raw companion {}: {error}", path.display()))?;
    let metadata_after = file
        .metadata()
        .map_err(|error| format!("restat opened companion {}: {error}", path.display()))?;
    let identity_after = (metadata_after.dev(), metadata_after.ino());
    if identity_after != identity_before || file_identity(path) != identity_before {
        return Err("companion pathname or opened inode changed during raw observation".to_owned());
    }
    let parsed = parse_raw_companion_bytes(&bytes)?;
    Ok(RawCompanionObservation {
        identity: identity_before,
        bytes,
        parsed,
    })
}

#[cfg(unix)]
fn assert_raw_companion_parser_rejects_damage(raw: &RawCompanionObservation) {
    for truncated_length in 0..raw.bytes.len() {
        assert!(
            parse_raw_companion_bytes(&raw.bytes[..truncated_length]).is_err(),
            "raw parser accepted truncation at byte {truncated_length}"
        );
    }
    let mut trailing_bytes = raw.bytes.clone();
    trailing_bytes.push(0);
    assert!(
        parse_raw_companion_bytes(&trailing_bytes).is_err(),
        "raw parser accepted bytes outside the canonical dual-slot layout"
    );

    let mut damaged = raw.bytes.clone();
    damaged[0] ^= 0x01;
    assert!(parse_raw_companion_bytes(&damaged).is_err());

    for slot in 0..2 {
        let offset = raw_companion_slot_offset(slot);
        let original = &raw.bytes[offset..offset + RAW_COMPANION_SLOT_BYTES];
        let mut malformed_length = original.to_vec();
        malformed_length[..4]
            .copy_from_slice(&((RAW_COMPANION_SLOT_PAYLOAD_CAPACITY + 1) as u32).to_le_bytes());
        assert!(parse_raw_companion_slot(&malformed_length).is_err());

        match &raw.parsed.slots[slot] {
            RawCompanionSlot::Complete(record) => {
                let mut payload_damage = original.to_vec();
                payload_damage[4 + record.checksummed_payload.len() / 2] ^= 0x40;
                assert!(parse_raw_companion_slot(&payload_damage).is_err());

                let mut checksum_damage = original.to_vec();
                checksum_damage[4 + RAW_COMPANION_SLOT_PAYLOAD_CAPACITY] ^= 0x80;
                assert!(parse_raw_companion_slot(&checksum_damage).is_err());

                if record.checksummed_payload.len() < RAW_COMPANION_SLOT_PAYLOAD_CAPACITY {
                    let mut padding_damage = original.to_vec();
                    padding_damage[4 + record.checksummed_payload.len()] = 1;
                    assert!(parse_raw_companion_slot(&padding_damage).is_err());
                }
            }
            RawCompanionSlot::PendingIntent(intent) => {
                let mut payload_damage = original.to_vec();
                payload_damage[4 + intent.checksummed_payload.len() / 2] ^= 0x40;
                assert!(parse_raw_companion_slot(&payload_damage).is_err());

                let mut checksum_damage = original.to_vec();
                checksum_damage[4 + RAW_COMPANION_SLOT_PAYLOAD_CAPACITY] ^= 0x80;
                assert!(parse_raw_companion_slot(&checksum_damage).is_err());
            }
            RawCompanionSlot::Vacant => {
                let mut vacant_damage = original.to_vec();
                vacant_damage[4] = 1;
                assert!(parse_raw_companion_slot(&vacant_damage).is_err());
            }
            RawCompanionSlot::Torn(error) => {
                panic!("baseline companion contains a torn slot: {slot}: {error}");
            }
        }
    }

    let mut selector_body_damage = raw.bytes.clone();
    selector_body_damage[RAW_COMPANION_SELECTOR_OFFSET + 16] ^= 0x20;
    assert!(parse_raw_companion_bytes(&selector_body_damage).is_err());
    let mut selector_checksum_damage = raw.bytes.clone();
    selector_checksum_damage[RAW_COMPANION_SELECTOR_OFFSET + RAW_COMPANION_SELECTOR_BODY_BYTES] ^=
        0x10;
    assert!(parse_raw_companion_bytes(&selector_checksum_damage).is_err());

    let mut self_checksummed_selector_damage = raw.bytes.clone();
    self_checksummed_selector_damage[RAW_COMPANION_SELECTOR_OFFSET + 16] ^= 0x01;
    let selector_checksum = *blake3::hash(
        &self_checksummed_selector_damage[RAW_COMPANION_SELECTOR_OFFSET
            ..RAW_COMPANION_SELECTOR_OFFSET + RAW_COMPANION_SELECTOR_BODY_BYTES],
    )
    .as_bytes();
    self_checksummed_selector_damage
        [RAW_COMPANION_SELECTOR_OFFSET + RAW_COMPANION_SELECTOR_BODY_BYTES..]
        .copy_from_slice(&selector_checksum);
    assert!(
        parse_raw_companion_bytes(&self_checksummed_selector_damage).is_err(),
        "selector generation must agree with the complete selected record"
    );
}

/// The one replacement intent the durable companion records, taken from the
/// parsed slot bytes rather than from the reader recovery itself uses.
#[cfg(unix)]
fn recorded_replacement_intent(parsed: &ParsedRawCompanion) -> Option<RawPendingIntent> {
    let mut recorded = parsed
        .slots
        .iter()
        .filter_map(|slot| match slot {
            RawCompanionSlot::PendingIntent(intent) => Some(intent.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        recorded.len() <= 1,
        "the durable companion records more than one replacement intent: {recorded:?}"
    );
    recorded.pop()
}

/// `recreate` records its exact-target intent in the original companion
/// before the atomic replacement, and only resolves that record into the
/// active generation at publication. Every boundary in between must
/// therefore carry the intent durably, so recovery has a recorded target to
/// act on instead of a sibling pathname to guess at.
#[cfg(unix)]
fn boundary_requires_recorded_reset_intent(boundary: StoreReplacementBoundary) -> bool {
    matches!(
        boundary,
        StoreReplacementBoundary::BeforeAtomicReplacement
            | StoreReplacementBoundary::AfterAtomicReplacement
    )
}

#[cfg(unix)]
fn assert_only_store_and_appended_companion(path: &Path) {
    let companion = appended_companion(path);
    let directory = path.parent().expect("store has a parent directory");
    let actual = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("read {}: {error}", directory.display()))
        .map(|entry| PathBuf::from(entry.expect("adjacent entry").file_name()))
        .collect::<BTreeSet<_>>();
    let expected = BTreeSet::from([
        PathBuf::from(path.file_name().expect("store file name")),
        PathBuf::from(companion.file_name().expect("companion file name")),
    ]);
    assert_eq!(
        actual, expected,
        "companion publication must not use a sidecar or extra adjacent record artifact"
    );
    assert!(
        !pre_change_replaced_companion(path).exists(),
        "extension-replacing companion artifact must remain absent"
    );
}

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct SensitiveFileMetadata {
    device: u64,
    inode: u64,
    mode: u32,
    size: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
    content_hash: [u8; 32],
}

#[cfg(unix)]
fn sensitive_file_metadata(path: &Path) -> SensitiveFileMetadata {
    let metadata = fs::symlink_metadata(path)
        .unwrap_or_else(|error| panic!("inspect {}: {error}", path.display()));
    assert!(
        metadata.file_type().is_file(),
        "{} is not a file",
        path.display()
    );
    SensitiveFileMetadata {
        device: metadata.dev(),
        inode: metadata.ino(),
        mode: metadata.mode(),
        size: metadata.size(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
        content_hash: *blake3::hash(
            &fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
        )
        .as_bytes(),
    }
}

fn expected_channel_address(path: &Path) -> ChannelAddress {
    assert_eq!(COMPANION_CHANNEL_ADDRESS_CODEC, "blake3-canonical-path-v1");
    derive_channel_address(path)
        .unwrap_or_else(|err| panic!("derive channel address for {}: {err}", path.display()))
}

fn expected_reader_breadcrumb_directory(path: &Path, runtime_directory: &Path) -> PathBuf {
    let expected = runtime_directory
        .join("contextdb")
        .join(encode_hex(&expected_channel_address(path).0));
    let actual = canonical_reader_breadcrumb_directory_for_test(path, runtime_directory)
        .expect("production breadcrumb directory derives through the local channel address");
    let contextdb_directory = runtime_directory.join("contextdb");
    assert_eq!(actual, expected);
    assert_eq!(actual.parent(), Some(contextdb_directory.as_path()));
    assert_eq!(
        actual
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::len),
        Some(64),
        "the runtime directory uses the fixed-width local channel address"
    );
    actual
}

fn assert_runtime_breadcrumb_path(path: &Path, expected_directory: &Path) {
    assert_eq!(path.parent(), Some(expected_directory));
    assert_eq!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("reader")
    );
    assert!(
        path.is_file(),
        "{} must be the live runtime breadcrumb",
        path.display()
    );
}

#[cfg(unix)]
fn effective_user_identity() -> LocalUserIdentity {
    LocalUserIdentity(nix::unistd::Uid::effective().as_raw() as u64)
}

#[cfg(unix)]
struct EffectiveUserIdentityOverride;

#[cfg(unix)]
impl Drop for EffectiveUserIdentityOverride {
    fn drop(&mut self) {
        clear_companion_effective_user_identity_for_test();
    }
}

fn assert_complete_owner_read_status(status: &OwnerReadStatus) {
    match (&status.state, &status.reason) {
        (OwnerServingState::Serving, None)
        | (OwnerServingState::ServingDisabled, Some(OwnerServingReason::DisabledByConfiguration))
        | (OwnerServingState::NotApplicable, Some(OwnerServingReason::PlatformUnsupported))
        | (OwnerServingState::NotServing, Some(OwnerServingReason::ShutdownDraining)) => {}
        (OwnerServingState::NotApplicable, None) => {}
        (OwnerServingState::NotServing, Some(OwnerServingReason::StartupFailure(reason)))
            if !reason.is_empty() => {}
        _ => panic!("contradictory owner-read status: {status:?}"),
    }
}

fn assert_complete_companion_record_for_user(
    path: &Path,
    record: &CompanionRecordObservation,
    expected_user: LocalUserIdentity,
) {
    assert_ne!(record.fields.format_version, 0);
    assert_ne!(record.fields.database_identity, DatabaseIdentity([0; 16]));
    assert_ne!(record.fields.writer_run_number, WriterRunNumber([0; 16]));
    assert_eq!(
        record.fields.channel_address,
        expected_channel_address(path)
    );
    assert_ne!(record.fields.process_id, 0);
    assert_eq!(record.fields.owner_user, expected_user);
    assert_complete_owner_read_status(&record.fields.owner_read_status);
    assert!(record.checksummed_payload.len() > 64);
    assert_eq!(
        record.stored_checksum,
        *blake3::hash(&record.checksummed_payload).as_bytes()
    );
    assert_eq!(
        decode_companion_record_for_test(&record.checksummed_payload, record.stored_checksum,)
            .expect("complete checksummed record must decode through the production codec"),
        *record,
        "observed fields must be the fields covered by the durable payload"
    );
}

fn assert_complete_companion_record(path: &Path, record: &CompanionRecordObservation) {
    #[cfg(unix)]
    let expected_user = effective_user_identity();
    #[cfg(not(unix))]
    let expected_user = LocalUserIdentity(0);
    assert_complete_companion_record_for_user(path, record, expected_user);
}

fn sorted_breadcrumbs(mut breadcrumbs: Vec<ReaderBreadcrumb>) -> Vec<ReaderBreadcrumb> {
    breadcrumbs.sort_by(|left, right| {
        (left.process_id, left.process_start.0, &left.process_name).cmp(&(
            right.process_id,
            right.process_start.0,
            &right.process_name,
        ))
    });
    breadcrumbs
}

fn assert_unverified_reader_refusal(
    result: WriterProbeResult,
    expected_count: u64,
    unverified: &[&ReaderBreadcrumb],
) {
    match result {
        WriterProbeResult::HeldByReaders {
            attempts,
            observed_direct_readers,
            verified_readers,
            message,
        } => {
            assert_eq!(attempts, 1);
            assert_eq!(observed_direct_readers, expected_count);
            assert!(verified_readers.is_empty());
            assert_eq!(
                message,
                format!(
                    "{expected_count} direct readers are hydrating this store; retry in a moment"
                )
            );
            for breadcrumb in unverified {
                assert!(!verified_readers.contains(*breadcrumb));
                assert!(
                    !message.contains(&breadcrumb.process_name),
                    "generic refusal exposed unverified process name {:?}: {message}",
                    breadcrumb.process_name
                );
            }
        }
        other => panic!("unverified readers must use the exact generic fallback: {other:?}"),
    }
}

#[cfg(target_os = "linux")]
fn assert_breadcrumb_matches_live_linux_process(breadcrumb: &ReaderBreadcrumb) {
    let process_directory = PathBuf::from(format!("/proc/{}", breadcrumb.process_id));
    let stat = fs::read_to_string(process_directory.join("stat"))
        .unwrap_or_else(|err| panic!("read live process stat: {err}"));
    let after_name = stat
        .rfind(") ")
        .map(|index| &stat[index + 2..])
        .expect("Linux process stat contains its parenthesized command name");
    let kernel_start = after_name
        .split_whitespace()
        .nth(19)
        .expect("Linux process stat contains field 22")
        .parse::<u64>()
        .expect("Linux process start field is an integer");
    assert_eq!(
        breadcrumb.process_start,
        ProcessStartIdentity(kernel_start),
        "breadcrumb start identity must come from the live process, not its PID alone"
    );

    let mut kernel_names = BTreeSet::new();
    if let Ok(comm) = fs::read_to_string(process_directory.join("comm")) {
        kernel_names.insert(comm.trim_end().to_owned());
    }
    if let Ok(executable) = fs::read_link(process_directory.join("exe"))
        && let Some(name) = executable.file_name().and_then(|name| name.to_str())
    {
        kernel_names.insert(name.to_owned());
    }
    if let Ok(command_line) = fs::read(process_directory.join("cmdline"))
        && let Some(argument_zero) = command_line.split(|byte| *byte == 0).next()
        && let Ok(argument_zero) = std::str::from_utf8(argument_zero)
        && let Some(name) = Path::new(argument_zero)
            .file_name()
            .and_then(|name| name.to_str())
    {
        kernel_names.insert(name.to_owned());
    }
    assert!(
        kernel_names.contains(&breadcrumb.process_name),
        "breadcrumb name {:?} must identify the live process; kernel names: {kernel_names:?}",
        breadcrumb.process_name
    );
}

#[cfg(unix)]
struct PermissionRestore(Vec<(PathBuf, fs::Permissions)>);

#[cfg(unix)]
impl PermissionRestore {
    fn for_paths(paths: &[&Path]) -> Self {
        Self(
            paths
                .iter()
                .map(|path| {
                    (
                        (*path).to_path_buf(),
                        fs::metadata(path).expect("read permissions").permissions(),
                    )
                })
                .collect(),
        )
    }
}

#[cfg(unix)]
impl Drop for PermissionRestore {
    fn drop(&mut self) {
        for (path, permissions) in &self.0 {
            let _ = fs::set_permissions(path, permissions.clone());
        }
    }
}

#[test]
fn companion_appends_lock_for_db_redb_and_extensionless_store_names() {
    let _serial = serialise_coordination_test();
    let root = tempfile::tempdir().expect("scratch directory");
    for name in ["alpha.db", "alpha.redb", "alpha"] {
        let path = root.path().join(name);
        let appended = appended_companion(&path);
        let pre_change = pre_change_replaced_companion(&path);
        if pre_change != appended {
            fs::write(&pre_change, b"stale extension-replaced companion")
                .expect("seed stale companion");
        }
        seed_store(&path);
        assert!(appended.is_file(), "{} must exist", appended.display());
        if pre_change != appended {
            assert_eq!(
                fs::read(&pre_change).expect("read stale companion"),
                b"stale extension-replaced companion"
            );
        }
    }
}

#[test]
fn writer_refusal_is_typed_and_changes_nothing_after_holder_publication() {
    let _serial = serialise_coordination_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let store_directory = root.path().join("store");
    let runtime_directory = root.path().join("runtime");
    fs::create_dir(&store_directory).expect("create store directory");
    create_runtime_directory(&runtime_directory);
    let path = store_directory.join("writer.redb");
    seed_store(&path);
    let mut holder = spawn_holder("writer-holder", "writer_holder_child", &path);
    holder.wait_for_opened();
    let baseline = inventory_directory(&store_directory);

    assert!(matches!(
        probe_writer(&path, &runtime_directory),
        WriterProbeResult::HeldByWriter { .. }
    ));
    assert_eq!(inventory_directory(&store_directory), baseline);

    holder.release();
    assert!(matches!(
        probe_writer(&path, &runtime_directory),
        WriterProbeResult::Acquired { .. }
    ));
}

#[test]
#[cfg(unix)]
fn hydration_without_a_companion_leaves_every_store_adjacent_companion_absent() {
    let _serial = serialise_coordination_test();
    assert_ne!(
        nix::unistd::Uid::effective().as_raw(),
        0,
        "read-only filesystem proof requires a non-root test process"
    );
    let root = tempfile::tempdir().expect("scratch directory");
    let store_directory = root.path().join("store");
    let runtime_directory = root.path().join("runtime");
    fs::create_dir(&store_directory).expect("create store directory");
    create_runtime_directory(&runtime_directory);
    let path = store_directory.join("without-companion.db");
    seed_store(&path);
    remove_companion_candidates(&path);
    let appended = appended_companion(&path);
    let replaced = pre_change_replaced_companion(&path);
    assert!(!appended.exists());
    assert!(!replaced.exists());

    let restore = PermissionRestore::for_paths(&[&store_directory, &path]);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o444)).expect("store is read-only");
    fs::set_permissions(&store_directory, fs::Permissions::from_mode(0o555))
        .expect("store directory is read-only");
    let store_before = inventory_directory(&store_directory);
    let runtime_before = inventory_directory(&runtime_directory);

    let mut hydration = spawn_hydration(&path, &runtime_directory);
    let evidence = hydration
        .wait_until_held()
        .expect("production hydration reaches its read-lock checkpoint");
    let breadcrumb_directory = expected_reader_breadcrumb_directory(&path, &runtime_directory);
    let breadcrumb_path = evidence
        .breadcrumb_path
        .clone()
        .expect("usable runtime returns the production breadcrumb path");
    assert_runtime_breadcrumb_path(&breadcrumb_path, &breadcrumb_directory);
    assert_eq!(
        evidence
            .breadcrumb
            .as_ref()
            .expect("usable runtime publishes a breadcrumb")
            .process_id,
        hydration.process_id()
    );
    assert!(!appended.exists());
    assert!(!replaced.exists());
    assert_eq!(inventory_directory(&store_directory), store_before);

    hydration
        .release()
        .expect("production hydration decodes and releases its read lock");
    assert!(!breadcrumb_path.exists());
    assert!(!appended.exists());
    assert!(!replaced.exists());
    assert_eq!(inventory_directory(&store_directory), store_before);
    assert_eq!(inventory_directory(&runtime_directory), runtime_before);
    drop(restore);
}

#[test]
#[cfg(unix)]
fn hydration_preserves_a_nonwritable_companion_byte_and_metadata_for_byte() {
    let _serial = serialise_coordination_test();
    assert_ne!(
        nix::unistd::Uid::effective().as_raw(),
        0,
        "non-writable companion proof requires a non-root test process"
    );
    let root = tempfile::tempdir().expect("scratch directory");
    let store_directory = root.path().join("store");
    let runtime_directory = root.path().join("runtime");
    fs::create_dir(&store_directory).expect("create store directory");
    create_runtime_directory(&runtime_directory);
    let path = store_directory.join("existing-companion.redb");
    seed_store(&path);
    let companion = appended_companion(&path);
    assert!(companion.is_file());

    let restore = PermissionRestore::for_paths(&[&store_directory, &path, &companion]);
    fs::set_permissions(&companion, fs::Permissions::from_mode(0o400))
        .expect("companion is non-writable");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o444)).expect("store is read-only");
    fs::set_permissions(&store_directory, fs::Permissions::from_mode(0o555))
        .expect("store directory is read-only");
    let companion_before = sensitive_file_metadata(&companion);
    assert_eq!(companion_before.mode & 0o777, 0o400);
    let store_before = inventory_directory(&store_directory);
    let runtime_before = inventory_directory(&runtime_directory);

    let mut hydration = spawn_hydration(&path, &runtime_directory);
    let evidence = hydration
        .wait_until_held()
        .expect("production hydration reaches its read-lock checkpoint");
    let breadcrumb_directory = expected_reader_breadcrumb_directory(&path, &runtime_directory);
    let breadcrumb_path = evidence
        .breadcrumb_path
        .clone()
        .expect("usable runtime returns the production breadcrumb path");
    assert_runtime_breadcrumb_path(&breadcrumb_path, &breadcrumb_directory);
    assert_eq!(
        evidence
            .breadcrumb
            .as_ref()
            .expect("usable runtime publishes a breadcrumb")
            .process_id,
        hydration.process_id()
    );
    assert_eq!(sensitive_file_metadata(&companion), companion_before);
    assert_eq!(inventory_directory(&store_directory), store_before);

    hydration
        .release()
        .expect("production hydration decodes and releases its read lock");
    assert!(!breadcrumb_path.exists());
    assert_eq!(sensitive_file_metadata(&companion), companion_before);
    assert_eq!(inventory_directory(&store_directory), store_before);
    assert_eq!(inventory_directory(&runtime_directory), runtime_before);
    drop(restore);
}

#[test]
#[cfg(unix)]
fn production_read_image_hydration_reports_live_readers_ignores_recycled_identity_and_cleans_up() {
    let _serial = serialise_coordination_test();
    assert_ne!(
        nix::unistd::Uid::effective().as_raw(),
        0,
        "read-only filesystem proof requires a non-root test process"
    );
    let root = tempfile::tempdir().expect("scratch directory");
    let store_directory = root.path().join("store");
    let runtime_directory = root.path().join("runtime");
    fs::create_dir(&store_directory).expect("create store directory");
    create_runtime_directory(&runtime_directory);
    let path = store_directory.join("hydration.db");
    seed_store(&path);

    let restore = PermissionRestore::for_paths(&[&store_directory, &path]);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o444)).expect("store is read-only");
    fs::set_permissions(&store_directory, fs::Permissions::from_mode(0o555))
        .expect("store directory is read-only");
    let baseline = inventory_directory(&store_directory);

    let mut first = spawn_hydration(&path, &runtime_directory);
    let mut second = spawn_hydration(&path, &runtime_directory);
    let first_evidence = first
        .wait_until_held()
        .expect("first production hydration reaches its checkpoint");
    let second_evidence = second
        .wait_until_held()
        .expect("second production hydration reaches its checkpoint");
    let breadcrumb_directory = expected_reader_breadcrumb_directory(&path, &runtime_directory);
    let first_breadcrumb_path = first_evidence
        .breadcrumb_path
        .clone()
        .expect("first hydration returns its exact production breadcrumb path");
    let second_breadcrumb_path = second_evidence
        .breadcrumb_path
        .clone()
        .expect("second hydration returns its exact production breadcrumb path");
    assert_runtime_breadcrumb_path(&first_breadcrumb_path, &breadcrumb_directory);
    assert_runtime_breadcrumb_path(&second_breadcrumb_path, &breadcrumb_directory);
    assert_ne!(
        first_breadcrumb_path, second_breadcrumb_path,
        "simultaneous hydrations require independent locked breadcrumb files"
    );
    let first_breadcrumb = first_evidence
        .breadcrumb
        .expect("valid runtime publishes first breadcrumb");
    let second_breadcrumb = second_evidence
        .breadcrumb
        .expect("valid runtime publishes second breadcrumb");
    assert_eq!(first_breadcrumb.process_id, first.process_id());
    assert_eq!(second_breadcrumb.process_id, second.process_id());
    assert!(!first_breadcrumb.process_name.is_empty());
    assert!(!second_breadcrumb.process_name.is_empty());
    #[cfg(target_os = "linux")]
    {
        assert_breadcrumb_matches_live_linux_process(&first_breadcrumb);
        assert_breadcrumb_matches_live_linux_process(&second_breadcrumb);
    }
    let stale_breadcrumb = ReaderBreadcrumb {
        process_id: u32::MAX - 1,
        process_name: "stale-unlocked-reader".to_owned(),
        process_start: ProcessStartIdentity(u64::MAX - 1),
    };
    let reclaimed_stale_path =
        create_unlocked_reader_breadcrumb_for_test(&path, &runtime_directory, &stale_breadcrumb)
            .expect(
                "create an unlocked stale breadcrumb through the production codec and address path",
            );
    assert_runtime_breadcrumb_path(&reclaimed_stale_path, &breadcrumb_directory);
    let locked = locked_reader_breadcrumbs_for_test(&path, &runtime_directory)
        .expect("inspect advisory locks through the production breadcrumb reader");
    assert_eq!(
        locked.len(),
        2,
        "only locked hydration breadcrumbs establish count"
    );
    assert_eq!(
        locked
            .iter()
            .map(|observation| observation.path.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            first_breadcrumb_path.clone(),
            second_breadcrumb_path.clone(),
        ])
    );
    assert!(
        locked
            .iter()
            .all(|observation| observation.breadcrumb.as_ref() != Some(&stale_breadcrumb))
    );
    assert!(
        !reclaimed_stale_path.exists(),
        "the unlocked stale runtime file is ignored and reclaimed best effort"
    );
    let stale_breadcrumb_path =
        create_unlocked_reader_breadcrumb_for_test(&path, &runtime_directory, &stale_breadcrumb)
            .expect("place an unlocked stale breadcrumb in front of the production writer probe");
    assert_runtime_breadcrumb_path(&stale_breadcrumb_path, &breadcrumb_directory);
    assert_eq!(inventory_directory(&store_directory), baseline);

    match probe_writer(&path, &runtime_directory) {
        WriterProbeResult::HeldByReaders {
            attempts,
            observed_direct_readers,
            verified_readers,
            message,
        } => {
            assert_eq!(attempts, 1, "reader refusal bypasses writer retry windows");
            assert_eq!(observed_direct_readers, 2);
            assert_eq!(
                sorted_breadcrumbs(verified_readers),
                sorted_breadcrumbs(vec![first_breadcrumb.clone(), second_breadcrumb.clone()])
            );
            assert!(message.contains(&first_breadcrumb.process_name));
            assert!(message.contains(&second_breadcrumb.process_name));
            assert!(!message.contains(&stale_breadcrumb.process_name));
        }
        other => panic!("expected typed HeldByReaders for two live hydrations: {other:?}"),
    }
    assert!(
        !stale_breadcrumb_path.exists(),
        "the production refusal path must ignore and reclaim an unlocked stale breadcrumb"
    );
    assert_eq!(inventory_directory(&store_directory), baseline);

    let mismatched_name = format!("mismatched-{}", first_breadcrumb.process_id);
    let wrong_name = ReaderBreadcrumb {
        process_id: first_breadcrumb.process_id,
        process_name: mismatched_name.clone(),
        process_start: first_breadcrumb.process_start,
    };
    assert_eq!(
        replace_reader_breadcrumb_for_test(
            &path,
            &runtime_directory,
            &first_breadcrumb,
            &wrong_name,
        )
        .expect("replace the process name through the production breadcrumb codec"),
        first_breadcrumb_path
    );
    match probe_writer(&path, &runtime_directory) {
        WriterProbeResult::HeldByReaders {
            attempts,
            observed_direct_readers,
            verified_readers,
            message,
        } => {
            assert_eq!(attempts, 1, "reader refusal bypasses writer retry windows");
            assert_eq!(observed_direct_readers, 2);
            assert_eq!(verified_readers, vec![second_breadcrumb.clone()]);
            assert!(
                verified_readers
                    .iter()
                    .all(|reader| reader.process_name != mismatched_name)
            );
            assert!(message.contains(&second_breadcrumb.process_name));
            assert!(!message.contains(&wrong_name.process_name));
            assert!(!message.contains(&stale_breadcrumb.process_name));
        }
        other => panic!("a matching PID/start with the wrong name must be ignored: {other:?}"),
    }

    let second_uninspectable = ReaderBreadcrumb {
        process_id: u32::MAX - 2,
        process_name: "second-process-inspection-unavailable".to_owned(),
        process_start: ProcessStartIdentity(second_breadcrumb.process_start.0),
    };
    assert_eq!(
        replace_reader_breadcrumb_for_test(
            &path,
            &runtime_directory,
            &second_breadcrumb,
            &second_uninspectable,
        )
        .expect("make the second locked breadcrumb uninspectable"),
        second_breadcrumb_path
    );
    let generic_stale_path =
        create_unlocked_reader_breadcrumb_for_test(&path, &runtime_directory, &stale_breadcrumb)
            .expect("place an unlocked stale breadcrumb beside two unverified hydration holders");
    assert_runtime_breadcrumb_path(&generic_stale_path, &breadcrumb_directory);
    assert_unverified_reader_refusal(
        probe_writer(&path, &runtime_directory),
        2,
        &[&wrong_name, &second_uninspectable, &stale_breadcrumb],
    );
    assert!(
        !generic_stale_path.exists(),
        "unlocked stale breadcrumb must not inflate the two-reader fallback"
    );
    assert_eq!(
        replace_reader_breadcrumb_for_test(
            &path,
            &runtime_directory,
            &second_uninspectable,
            &second_breadcrumb,
        )
        .expect("restore the second live breadcrumb"),
        second_breadcrumb_path
    );

    assert_eq!(
        replace_reader_breadcrumb_for_test(
            &path,
            &runtime_directory,
            &wrong_name,
            &first_breadcrumb,
        )
        .expect("restore the live name through the production breadcrumb codec"),
        first_breadcrumb_path
    );
    let wrong_start = ReaderBreadcrumb {
        process_id: first_breadcrumb.process_id,
        process_name: first_breadcrumb.process_name.clone(),
        process_start: ProcessStartIdentity(first_breadcrumb.process_start.0 ^ 1),
    };
    assert_eq!(
        replace_reader_breadcrumb_for_test(
            &path,
            &runtime_directory,
            &first_breadcrumb,
            &wrong_start,
        )
        .expect("replace the process start through the production breadcrumb codec"),
        first_breadcrumb_path
    );
    match probe_writer(&path, &runtime_directory) {
        WriterProbeResult::HeldByReaders {
            attempts,
            observed_direct_readers,
            verified_readers,
            message,
        } => {
            assert_eq!(attempts, 1, "reader refusal bypasses writer retry windows");
            assert_eq!(observed_direct_readers, 2);
            assert_eq!(verified_readers, vec![second_breadcrumb.clone()]);
            assert!(!verified_readers.contains(&wrong_start));
            assert!(message.contains(&second_breadcrumb.process_name));
            assert!(!message.contains(&stale_breadcrumb.process_name));
        }
        other => panic!("a matching PID/name with the wrong start must be ignored: {other:?}"),
    }

    assert_eq!(
        replace_reader_breadcrumb_for_test(
            &path,
            &runtime_directory,
            &wrong_start,
            &first_breadcrumb,
        )
        .expect("restore the live start through the production breadcrumb codec"),
        first_breadcrumb_path
    );
    let unavailable_process = ReaderBreadcrumb {
        process_id: u32::MAX,
        process_name: "process-inspection-unavailable".to_owned(),
        process_start: ProcessStartIdentity(first_breadcrumb.process_start.0),
    };
    assert_eq!(
        replace_reader_breadcrumb_for_test(
            &path,
            &runtime_directory,
            &first_breadcrumb,
            &unavailable_process,
        )
        .expect("write an uninspectable breadcrumb through the production codec"),
        first_breadcrumb_path
    );
    match probe_writer(&path, &runtime_directory) {
        WriterProbeResult::HeldByReaders {
            attempts,
            observed_direct_readers,
            verified_readers,
            message,
        } => {
            assert_eq!(attempts, 1, "reader refusal bypasses writer retry windows");
            assert_eq!(observed_direct_readers, 2);
            assert_eq!(verified_readers, vec![second_breadcrumb.clone()]);
            assert!(!verified_readers.contains(&unavailable_process));
            assert!(message.contains(&second_breadcrumb.process_name));
            assert!(!message.contains(&unavailable_process.process_name));
            assert!(!message.contains(&stale_breadcrumb.process_name));
        }
        other => panic!("an unavailable process inspection must not name a reader: {other:?}"),
    }

    second
        .release()
        .expect("second hydration decodes and removes its breadcrumb");
    assert!(!second_breadcrumb_path.exists());
    assert_unverified_reader_refusal(
        probe_writer(&path, &runtime_directory),
        1,
        &[&unavailable_process, &stale_breadcrumb],
    );
    assert_eq!(inventory_directory(&store_directory), baseline);

    assert_eq!(
        replace_reader_breadcrumb_for_test(
            &path,
            &runtime_directory,
            &unavailable_process,
            &wrong_name,
        )
        .expect("restore the live PID/start with a mismatched process name"),
        first_breadcrumb_path
    );
    assert_unverified_reader_refusal(
        probe_writer(&path, &runtime_directory),
        1,
        &[&wrong_name, &stale_breadcrumb],
    );

    assert_eq!(
        replace_reader_breadcrumb_for_test(&path, &runtime_directory, &wrong_name, &wrong_start,)
            .expect("replace the mismatched name with a mismatched process start"),
        first_breadcrumb_path
    );
    assert_unverified_reader_refusal(
        probe_writer(&path, &runtime_directory),
        1,
        &[&wrong_start, &stale_breadcrumb],
    );

    assert_eq!(
        replace_reader_breadcrumb_for_test(
            &path,
            &runtime_directory,
            &wrong_start,
            &first_breadcrumb,
        )
        .expect("restore the live breadcrumb before hydration cleanup"),
        first_breadcrumb_path
    );

    first
        .release()
        .expect("first hydration decodes and removes its breadcrumb");
    assert_eq!(inventory_directory(&store_directory), baseline);
    assert!(!first_breadcrumb_path.exists());
    assert!(!second_breadcrumb_path.exists());
    assert!(!stale_breadcrumb_path.exists());
    assert!(
        inventory_directory(&runtime_directory)
            .values()
            .all(|entry| entry.kind == EntryKind::Directory),
        "released hydration must remove every breadcrumb file"
    );
    drop(restore);
    let orphaned_stale_path =
        create_unlocked_reader_breadcrumb_for_test(&path, &runtime_directory, &stale_breadcrumb)
            .expect("place a stale breadcrumb with no corresponding hydration holder");
    assert_runtime_breadcrumb_path(&orphaned_stale_path, &breadcrumb_directory);
    assert!(matches!(
        probe_writer(&path, &runtime_directory),
        WriterProbeResult::Acquired { .. }
    ));
    let locked = locked_reader_breadcrumbs_for_test(&path, &runtime_directory)
        .expect("reclaim orphaned stale breadcrumb after the production probe");
    assert!(locked.is_empty());
    assert!(!orphaned_stale_path.exists());
}

#[test]
#[cfg(target_os = "linux")]
fn live_reader_terminal_control_process_name_is_anonymous_and_cannot_reach_holder_output() {
    let _serial = serialise_coordination_test();
    let root = tempfile::tempdir().expect("private scratch directory");
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
        .expect("make scratch directory private");
    let store_directory = root.path().join("store");
    let runtime_directory = root.path().join("runtime");
    fs::create_dir(&store_directory).expect("create store directory");
    create_runtime_directory(&runtime_directory);
    let path = store_directory.join("terminal-control-reader.db");
    seed_store(&path);
    let executable = copied_hydration_executable_with_terminal_control_name(root.path());

    let mut hydration = spawn_hydration_from(&executable, &path, &runtime_directory);
    assert_eq!(
        hydration
            .wait_until_held()
            .expect("copied hydration reaches its real read-lock checkpoint"),
        HydrationEvidence {
            breadcrumb_path: None,
            breadcrumb: None,
        },
        "a live process whose only OS evidence has terminal-control bytes must publish anonymously"
    );
    assert_eq!(
        linux_live_process_name_evidence(hydration.process_id()),
        [
            TERMINAL_CONTROL_READER_EXECUTABLE_BASENAME.to_owned(),
            TERMINAL_CONTROL_READER_EXECUTABLE_BASENAME.to_owned(),
            TERMINAL_CONTROL_READER_EXECUTABLE_BASENAME.to_owned(),
        ],
        "the copied executable must leave no safe process-name alias that could make this proof non-causal"
    );

    let breadcrumb_directory = expected_reader_breadcrumb_directory(&path, &runtime_directory);
    let mut locked = locked_reader_breadcrumbs_for_test(&path, &runtime_directory)
        .expect("inspect the actual locked runtime breadcrumb");
    assert_eq!(
        locked.len(),
        1,
        "the hydration holds exactly one breadcrumb"
    );
    let anonymous = locked.pop().expect("the held breadcrumb is present");
    assert_runtime_breadcrumb_path(&anonymous.path, &breadcrumb_directory);
    assert_eq!(
        anonymous.breadcrumb, None,
        "the actual locked breadcrumb must decode as anonymous"
    );
    assert_eq!(
        fs::read(&anonymous.path).expect("read actual anonymous breadcrumb"),
        b"contextdb-reader-anonymous-v1"
    );

    match probe_writer(&path, &runtime_directory) {
        WriterProbeResult::HeldByReaders {
            attempts,
            observed_direct_readers,
            verified_readers,
            message,
        } => {
            assert_eq!(attempts, 1, "reader refusal bypasses writer retry windows");
            assert_eq!(observed_direct_readers, 1);
            assert!(verified_readers.is_empty());
            assert_eq!(
                message,
                "1 direct readers are hydrating this store; retry in a moment"
            );
            for malicious_byte in [b'\x1b', b'\r', b'\n', b'\\'] {
                assert!(
                    !message.as_bytes().contains(&malicious_byte),
                    "generic reader refusal exposed terminal-control byte {malicious_byte:#04x}"
                );
            }
        }
        other => panic!("one anonymous live hydration must refuse the writer: {other:?}"),
    }

    hydration
        .release()
        .expect("copied hydration decodes and releases its real read lock");
    assert!(
        !anonymous.path.exists(),
        "released anonymous breadcrumb is removed"
    );
    assert!(
        locked_reader_breadcrumbs_for_test(&path, &runtime_directory)
            .expect("inspect runtime breadcrumbs after release")
            .is_empty(),
        "no locked breadcrumb survives the hydration release"
    );
    assert!(matches!(
        probe_writer(&path, &runtime_directory),
        WriterProbeResult::Acquired { .. }
    ));
}

#[test]
fn unusable_runtime_keeps_hydration_unverified_and_uses_exact_generic_fallback() {
    let _serial = serialise_coordination_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let store_directory = root.path().join("store");
    fs::create_dir(&store_directory).expect("create store directory");
    let path = store_directory.join("unverified.db");
    seed_store(&path);
    let unusable_runtime = root.path().join("runtime-is-a-file");
    fs::write(&unusable_runtime, b"not a directory").expect("create unusable runtime path");
    let baseline = inventory_directory(&store_directory);

    let mut hydration = spawn_hydration(&path, &unusable_runtime);
    assert_eq!(
        hydration
            .wait_until_held()
            .expect("unverified hydration still reaches its production checkpoint"),
        HydrationEvidence {
            breadcrumb_path: None,
            breadcrumb: None,
        }
    );
    match probe_writer(&path, &unusable_runtime) {
        WriterProbeResult::HeldByReaders {
            attempts,
            observed_direct_readers,
            verified_readers,
            message,
        } => {
            assert_eq!(attempts, 1);
            assert_eq!(observed_direct_readers, 1);
            assert!(verified_readers.is_empty());
            assert_eq!(
                message,
                "1 direct readers are hydrating this store; retry in a moment"
            );
        }
        other => panic!("unusable runtime must retain typed reader count: {other:?}"),
    }
    assert_eq!(inventory_directory(&store_directory), baseline);
    hydration
        .release()
        .expect("unverified hydration still decodes and releases redb");
    assert_eq!(inventory_directory(&store_directory), baseline);
    assert!(matches!(
        probe_writer(&path, &unusable_runtime),
        WriterProbeResult::Acquired { .. }
    ));
}

#[test]
fn raw_redb_readonly_is_only_a_secondary_primitive_check() {
    let _serial = serialise_coordination_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("raw.redb");
    seed_store(&path);
    let baseline = inventory_directory(root.path());
    let mut holder = spawn_holder("raw-readonly-holder", "raw_readonly_holder_child", &path);
    holder.wait_for_opened();
    assert_eq!(inventory_directory(root.path()), baseline);
    holder.release();
    assert_eq!(inventory_directory(root.path()), baseline);
}

#[test]
fn production_hydration_holds_redb_process_lock_until_every_reader_releases() {
    let _serial = serialise_coordination_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let store_directory = root.path().join("store");
    let runtime_directory = root.path().join("runtime");
    fs::create_dir(&store_directory).expect("create store directory");
    create_runtime_directory(&runtime_directory);
    let path = store_directory.join("kernel-lock.redb");
    seed_store(&path);
    let baseline = inventory_directory(&store_directory);

    let mut first = spawn_hydration(&path, &runtime_directory);
    let mut second = spawn_hydration(&path, &runtime_directory);
    let first_evidence = first
        .wait_until_held()
        .expect("first production hydration holds its real read-only database handle");
    let second_evidence = second
        .wait_until_held()
        .expect("second production hydration holds its real read-only database handle");
    assert!(first_evidence.breadcrumb_path.is_some());
    assert!(second_evidence.breadcrumb_path.is_some());

    assert_eq!(
        probe_raw_redb_writer(&path),
        RawRedbWriterProbeResult::DatabaseAlreadyOpen,
        "a writer that bypasses ContextDB must be refused by Redb's process lock"
    );
    match probe_writer(&path, &runtime_directory) {
        WriterProbeResult::HeldByReaders {
            attempts,
            observed_direct_readers,
            verified_readers,
            ..
        } => {
            assert_eq!(attempts, 1);
            assert_eq!(observed_direct_readers, 2);
            assert_eq!(verified_readers.len(), 2);
        }
        other => panic!("ContextDB must separately classify the live hydrations: {other:?}"),
    }
    assert_eq!(inventory_directory(&store_directory), baseline);

    first
        .release()
        .expect("first production hydration releases cleanly");
    assert_eq!(
        probe_raw_redb_writer(&path),
        RawRedbWriterProbeResult::DatabaseAlreadyOpen,
        "one remaining hydration must retain Redb's process lock"
    );
    assert_eq!(inventory_directory(&store_directory), baseline);

    second
        .release()
        .expect("second production hydration releases cleanly");
    assert_eq!(inventory_directory(&store_directory), baseline);
    assert_eq!(
        probe_raw_redb_writer(&path),
        RawRedbWriterProbeResult::Acquired,
        "the same raw-redb writer path must open after every hydration releases"
    );
}

type SameProcessHydrationResult =
    std::result::Result<ReadImageObservation, LoadReadImageAdapterError>;

struct SameProcessHydrationRun {
    controller: Option<TwoReadImageHydrationController>,
    handles: Vec<(
        Option<u64>,
        Option<thread::JoinHandle<SameProcessHydrationResult>>,
    )>,
}

impl SameProcessHydrationRun {
    fn arm() -> Self {
        Self {
            controller: Some(arm_two_read_image_hydrations_for_test()),
            handles: Vec::new(),
        }
    }

    fn spawn_and_wait(
        &mut self,
        path: &Path,
    ) -> contextdb_engine::persistence::read_persistence_test_scaffold::ReadImageHydrationParticipantObservation
    {
        let path = path.to_path_buf();
        let handle = thread::spawn(move || {
            let _attempt = begin_two_read_image_hydration_attempt_for_test();
            load_read_image_for_test_adapter(&path)
        });
        let index = self.handles.len();
        self.handles.push((None, Some(handle)));
        let observation = self
            .controller
            .as_mut()
            .expect("same-process hydration controller is active")
            .next_participant_for_test();
        assert!(
            self.handles
                .iter()
                .filter_map(|(participant, _)| *participant)
                .all(|participant| participant != observation.participant),
            "hydration participant identity was reused"
        );
        self.handles[index].0 = Some(observation.participant);
        observation
    }

    fn release_and_join(&mut self, participant: u64) -> ReadImageObservation {
        self.controller
            .as_ref()
            .expect("same-process hydration controller is active")
            .release_participant_for_test(participant);
        let (_, handle) = self
            .handles
            .iter_mut()
            .find(|(observed, _)| *observed == Some(participant))
            .expect("released hydration participant has a thread");
        handle
            .take()
            .expect("hydration thread was already joined")
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
            .unwrap_or_else(|error| panic!("production hydration failed: {error}"))
    }

    fn finish(mut self) {
        assert!(
            self.handles.iter().all(|(_, handle)| handle.is_none()),
            "every same-process hydration thread must be joined"
        );
        self.controller
            .take()
            .expect("same-process hydration controller is active")
            .finish_for_test();
    }
}

impl Drop for SameProcessHydrationRun {
    fn drop(&mut self) {
        drop(self.controller.take());
        for (_, handle) in &mut self.handles {
            if let Some(handle) = handle.take() {
                let _ = handle.join();
            }
        }
    }
}

fn raw_redb_writer_attempt_in_this_process(path: &Path) -> RawRedbWriterProbeResult {
    match redb::Builder::new().open(path) {
        Ok(database) => {
            drop(database);
            RawRedbWriterProbeResult::Acquired
        }
        Err(redb::DatabaseError::DatabaseAlreadyOpen) => {
            RawRedbWriterProbeResult::DatabaseAlreadyOpen
        }
        Err(error) => RawRedbWriterProbeResult::Other(error.to_string()),
    }
}

fn contextdb_writer_attempt_in_this_process(path: &Path) -> std::result::Result<bool, String> {
    match Database::open(path) {
        Ok(database) => {
            database
                .close()
                .map_err(|error| format!("close same-process writer: {error}"))?;
            Ok(true)
        }
        Err(Error::ReadFailure(failure)) if failure.kind() == ReadFailureKind::HeldByReaders => {
            Ok(false)
        }
        Err(error) => Err(format!("same-process writer returned {error:?}")),
    }
}

#[test]
fn two_same_process_hydrations_release_ownership_independently_before_a_writer_opens() {
    let _serial = serialise_coordination_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let store_directory = root.path().join("store");
    let runtime_directory = root.path().join("runtime");
    fs::create_dir(&store_directory).expect("create store directory");
    create_runtime_directory(&runtime_directory);
    let path = store_directory.join("same-process-hydration.redb");
    seed_store(&path);
    let store_baseline = inventory_directory(&store_directory);
    let runtime_baseline = inventory_directory(&runtime_directory);
    reset_read_image_source_events_for_test();

    // Where readers actually write themselves down. It is the default
    // per-user runtime location, not the directory either hydration was
    // handed: the writer that a reader blocks is started by somebody else,
    // very often with no runtime flag at all, and looks there. Asking
    // production for it is how this proof and the code agree on one location.
    let breadcrumb_root = reader_breadcrumb_runtime_root_for_test()
        .expect("this machine has a usable per-user runtime location");

    let mut run = SameProcessHydrationRun::arm();
    let first = run.spawn_and_wait(&path);
    let second = run.spawn_and_wait(&path);
    let first_path = first
        .breadcrumb_path
        .clone()
        .expect("first same-process hydration has a breadcrumb");
    let second_path = second
        .breadcrumb_path
        .clone()
        .expect("second same-process hydration has a breadcrumb");
    assert_ne!(first.participant, second.participant);
    assert_ne!(first_path, second_path);
    assert_eq!(
        first
            .breadcrumb
            .as_ref()
            .expect("first hydration has process identity")
            .process_id,
        std::process::id()
    );
    assert_eq!(first.breadcrumb, second.breadcrumb);
    let mut held_paths = locked_reader_breadcrumbs_for_test(&path, &breadcrumb_root)
        .expect("inspect two same-process breadcrumbs")
        .into_iter()
        .map(|locked| locked.path)
        .collect::<Vec<_>>();
    held_paths.sort();
    let mut expected_paths = vec![first_path.clone(), second_path.clone()];
    expected_paths.sort();
    assert_eq!(held_paths, expected_paths);
    assert_eq!(
        raw_redb_writer_attempt_in_this_process(&path),
        RawRedbWriterProbeResult::DatabaseAlreadyOpen,
        "both same-process hydration handles must refuse a raw writer"
    );
    assert_eq!(
        contextdb_writer_attempt_in_this_process(&path),
        Ok(false),
        "both same-process hydration intervals must refuse a production writer"
    );

    let first_result = run.release_and_join(first.participant);
    assert_eq!(
        first_result.released_breadcrumb_path.as_deref(),
        Some(first_path.as_path())
    );
    assert!(first_result.source_accesses > 0);
    assert!(!first_path.exists());
    assert!(second_path.exists());
    let remaining = locked_reader_breadcrumbs_for_test(&path, &breadcrumb_root)
        .expect("inspect independently retained second breadcrumb");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].path, second_path);
    assert_eq!(
        raw_redb_writer_attempt_in_this_process(&path),
        RawRedbWriterProbeResult::DatabaseAlreadyOpen,
        "the independently retained second handle must still refuse the writer"
    );
    assert_eq!(
        contextdb_writer_attempt_in_this_process(&path),
        Ok(false),
        "one remaining same-process hydration must still refuse a production writer"
    );

    let second_result = run.release_and_join(second.participant);
    assert_eq!(
        second_result.released_breadcrumb_path.as_deref(),
        Some(second_path.as_path())
    );
    assert!(second_result.source_accesses > 0);
    assert!(!second_path.exists());
    assert!(
        locked_reader_breadcrumbs_for_test(&path, &breadcrumb_root)
            .expect("inspect complete same-process release")
            .is_empty()
    );
    assert_eq!(inventory_directory(&store_directory), store_baseline);
    assert_eq!(
        inventory_directory(&runtime_directory),
        runtime_baseline,
        "the runtime directory these hydrations were handed names an owner channel; no reader \
         breadcrumb is ever left in it"
    );
    assert_eq!(
        raw_redb_writer_attempt_in_this_process(&path),
        RawRedbWriterProbeResult::Acquired,
        "the released hydrations must free Redb's own process lock, not merely ContextDB's view"
    );
    assert_eq!(
        contextdb_writer_attempt_in_this_process(&path),
        Ok(true),
        "a production writer may open only after both independent hydration intervals end"
    );
    run.finish();

    let events = read_image_source_events_for_test();
    let started = events
        .iter()
        .filter_map(|event| match event {
            ReadImageSourceEvent::Started { breadcrumb_path } => breadcrumb_path.clone(),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let released = events
        .iter()
        .filter_map(|event| match event {
            ReadImageSourceEvent::Released { breadcrumb_path } => breadcrumb_path.clone(),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        started,
        BTreeSet::from([first_path.clone(), second_path.clone()])
    );
    assert_eq!(released, BTreeSet::from([first_path, second_path]));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, ReadImageSourceEvent::SourceHandlesDropped))
            .count(),
        2
    );
    assert_eq!(inventory_directory(&runtime_directory), runtime_baseline);
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn next_unique_integer_id(state: &mut u64, ids: &mut BTreeSet<i64>) -> i64 {
    loop {
        let candidate = (next_random(state) & 0x3fff_ffff_ffff_ffff) as i64 + 1;
        if ids.insert(candidate) {
            return candidate;
        }
    }
}

fn randomized_vector(state: &mut u64, dimension: usize, bias: f32) -> Vec<f32> {
    (0..dimension)
        .map(|index| {
            let value = next_random(state);
            let unit = (value >> 40) as f32 / ((1_u64 << 24) - 1) as f32;
            (unit * 2.0 - 1.0) * 0.9 + bias + index as f32 * 0.011
        })
        .collect()
}

fn insert_vector(db: &Database, table: &str, id: i64, vector: &[f32]) {
    let parameters = HashMap::from([
        ("id".to_owned(), Value::Int64(id)),
        ("vector".to_owned(), Value::Vector(vector.to_vec())),
    ]);
    db.execute(
        &format!("INSERT INTO {table} (id, vec) VALUES ($id, $vector)"),
        &parameters,
    )
    .unwrap_or_else(|err| panic!("insert {table}/{id}: {err}"));
}

fn assert_quantized_vector(table: &str, id: i64, actual: &[f32], expected: &[f32], tolerance: f32) {
    assert_eq!(actual.len(), expected.len(), "{table}/{id} dimension");
    for (component, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        assert!(
            (*actual - *expected).abs() <= tolerance,
            "{table}/{id} component {component}: got {actual}, expected approximately {expected}"
        );
    }
}

#[test]
fn production_load_read_image_reconstructs_current_quantized_state_without_mutation() {
    let _serial = serialise_coordination_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let store_directory = root.path().join("store");
    let runtime_directory = root.path().join("runtime");
    fs::create_dir(&store_directory).expect("create store directory");
    create_runtime_directory(&runtime_directory);
    let path = store_directory.join("quantized.db");
    let mut random_state = 0x9e37_79b9_7f4a_7c15_u64 ^ (u64::from(std::process::id()) << 17);
    let suffix = format!("{:010x}", next_random(&mut random_state) & 0xff_ffff_ffff);
    let cases = [
        (format!("sq8_{suffix}"), "SQ8", 8_usize, 0.02_f32),
        (format!("sq4_{suffix}"), "SQ4", 7_usize, 0.14_f32),
    ];
    let db = Database::open(&path).expect("create randomized quantized store");
    let mut expected = BTreeMap::new();
    let mut replaced_prior_values = BTreeMap::new();
    let mut deleted = BTreeSet::new();
    let mut ids = BTreeSet::new();
    for (case_index, (table, quantization, dimension, tolerance)) in cases.iter().enumerate() {
        db.execute(
            &format!(
                "CREATE TABLE {table} (id INTEGER PRIMARY KEY, vec VECTOR({dimension}) WITH (quantization = '{quantization}'))"
            ),
            &Default::default(),
        )
        .unwrap_or_else(|err| panic!("create {table}: {err}"));

        for vector_index in 0..3 {
            let id = next_unique_integer_id(&mut random_state, &mut ids);
            let vector = randomized_vector(
                &mut random_state,
                *dimension,
                case_index as f32 * 0.07 + vector_index as f32 * 0.03,
            );
            insert_vector(&db, table, id, &vector);
            expected.insert((table.as_str().to_owned(), id), (vector, *tolerance));
        }

        let constant_id = next_unique_integer_id(&mut random_state, &mut ids);
        let constant = ((next_random(&mut random_state) & 0xffff) as f32 / 65_535.0) * 1.7 - 0.85
            + case_index as f32 * 0.019;
        let constant_vector = vec![constant; *dimension];
        insert_vector(&db, table, constant_id, &constant_vector);
        expected.insert(
            (table.as_str().to_owned(), constant_id),
            (constant_vector, f32::EPSILON),
        );

        let replacement_id = next_unique_integer_id(&mut random_state, &mut ids);
        let prior = randomized_vector(&mut random_state, *dimension, -0.31);
        insert_vector(&db, table, replacement_id, &prior);
        let replacement = prior
            .iter()
            .enumerate()
            .map(|(index, value)| *value + 2.75 + index as f32 * 0.017)
            .collect::<Vec<_>>();
        db.execute(
            &format!("UPDATE {table} SET vec = $vector WHERE id = $id"),
            &HashMap::from([
                ("id".to_owned(), Value::Int64(replacement_id)),
                ("vector".to_owned(), Value::Vector(replacement.clone())),
            ]),
        )
        .unwrap_or_else(|err| panic!("replace {table}/{replacement_id}: {err}"));
        replaced_prior_values.insert((table.as_str().to_owned(), replacement_id), prior);
        expected.insert(
            (table.as_str().to_owned(), replacement_id),
            (replacement, *tolerance),
        );

        let deleted_id = next_unique_integer_id(&mut random_state, &mut ids);
        let deleted_vector = randomized_vector(&mut random_state, *dimension, 0.53);
        insert_vector(&db, table, deleted_id, &deleted_vector);
        db.execute(
            &format!("DELETE FROM {table} WHERE id = $id"),
            &HashMap::from([("id".to_owned(), Value::Int64(deleted_id))]),
        )
        .unwrap_or_else(|err| panic!("delete {table}/{deleted_id}: {err}"));
        deleted.insert((table.as_str().to_owned(), deleted_id));
    }
    db.close().expect("close randomized quantized store");
    let baseline = inventory_directory(&store_directory);
    let runtime_baseline = inventory_directory(&runtime_directory);

    let image = load_read_image_for_test_adapter(&path)
        .unwrap_or_else(|err| panic!("load complete quantized read image: {err}"));
    assert!(image.source_accesses > 0);
    let released_breadcrumb = image
        .released_breadcrumb_path
        .as_ref()
        .expect("usable runtime returns the exact released breadcrumb path");
    assert!(!released_breadcrumb.exists());
    assert_eq!(image.vectors.len(), expected.len());
    for ((table, id), (expected_vector, tolerance)) in &expected {
        let actual = image
            .vectors
            .get(&(table.as_str().to_owned(), *id))
            .unwrap_or_else(|| panic!("current vector is absent: {table}/{id}"));
        assert_quantized_vector(table, *id, actual, expected_vector, *tolerance);
        if let Some(prior) = replaced_prior_values.get(&(table.as_str().to_owned(), *id)) {
            assert!(
                actual
                    .iter()
                    .zip(prior)
                    .any(|(current, prior)| (*current - *prior).abs() > *tolerance * 2.0),
                "{table}/{id} exposed the inserted vector instead of its replacement"
            );
        }
    }
    for (table, id) in deleted {
        assert!(
            !image.vectors.contains_key(&(table.clone(), id)),
            "deleted vector remained in the committed read image: {table}/{id}"
        );
    }
    assert_eq!(inventory_directory(&store_directory), baseline);
    assert_eq!(inventory_directory(&runtime_directory), runtime_baseline);
}

fn golden_companion_fields() -> CompanionRecordFields {
    CompanionRecordFields {
        format_version: 1,
        generation: 0x0102_0304_0506_0708,
        database_identity: DatabaseIdentity(std::array::from_fn(|index| 0x10 + index as u8)),
        writer_run_number: WriterRunNumber(std::array::from_fn(|index| 0x20 + index as u8)),
        owner_user: LocalUserIdentity(0x3031_3233_3435_3637),
        channel_address: ChannelAddress(std::array::from_fn(|index| 0x40 + index as u8)),
        process_id: 0x6061_6263,
        owner_read_status: OwnerReadStatus {
            state: OwnerServingState::NotServing,
            reason: Some(OwnerServingReason::StartupFailure("offline".to_owned())),
        },
    }
}

#[test]
fn companion_codec_has_literal_golden_and_every_field_is_checksum_covered() {
    let fields = golden_companion_fields();
    let expected_payload = decode_hex(
        "636f6e7465787464622d636f6d70616e696f6e01000807060504030201101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f3736353433323130404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f636261600202070000006f66666c696e65",
    )
    .expect("literal payload is valid hex");
    let expected_checksum: [u8; 32] =
        decode_hex("01b3c085d11b5413a077c031e67c887726f6e2e2bd609b726c3de9301c16ac93")
            .expect("literal checksum is valid hex")
            .try_into()
            .expect("literal checksum is 32 bytes");
    let encoded = encode_companion_record_for_test(&fields)
        .expect("production companion codec encodes the literal record");
    assert_eq!(encoded.checksummed_payload, expected_payload);
    assert_eq!(encoded.stored_checksum, expected_checksum);
    assert_eq!(
        decode_companion_record_for_test(&expected_payload, expected_checksum)
            .expect("production companion codec decodes the literal record"),
        encoded
    );

    for index in 0..expected_payload.len() {
        let mut torn_payload = expected_payload.clone();
        torn_payload[index] ^= 1_u8 << (index % 8);
        assert!(
            decode_companion_record_for_test(&torn_payload, expected_checksum).is_err(),
            "payload mutation at byte {index} must not expose any partial record"
        );
    }
    for index in 0..expected_checksum.len() {
        let mut torn_checksum = expected_checksum;
        torn_checksum[index] ^= 1_u8 << (index % 8);
        assert!(
            decode_companion_record_for_test(&expected_payload, torn_checksum).is_err(),
            "checksum mutation at byte {index} must not expose any partial record"
        );
    }

    let mut mutations = Vec::new();
    let mut changed = fields.clone();
    changed.database_identity.0[0] ^= 1;
    mutations.push(("database identity", changed));
    let mut changed = fields.clone();
    changed.generation ^= 1;
    mutations.push(("generation", changed));
    let mut changed = fields.clone();
    changed.writer_run_number.0[0] ^= 1;
    mutations.push(("writer run", changed));
    let mut changed = fields.clone();
    changed.owner_user.0 ^= 1;
    mutations.push(("effective user", changed));
    let mut changed = fields.clone();
    changed.channel_address.0[0] ^= 1;
    mutations.push(("channel address", changed));
    let mut changed = fields.clone();
    changed.process_id ^= 1;
    mutations.push(("process id", changed));
    let mut changed = fields.clone();
    changed.owner_read_status = OwnerReadStatus {
        state: OwnerServingState::Serving,
        reason: None,
    };
    mutations.push(("serving state", changed));
    let mut changed = fields.clone();
    changed.owner_read_status = OwnerReadStatus {
        state: OwnerServingState::NotServing,
        reason: Some(OwnerServingReason::StartupFailure("different".to_owned())),
    };
    mutations.push(("serving reason", changed));

    for (field, mutation) in mutations {
        let mutated = encode_companion_record_for_test(&mutation)
            .unwrap_or_else(|err| panic!("encode {field} mutation: {err}"));
        assert_ne!(
            mutated.checksummed_payload, encoded.checksummed_payload,
            "{field}"
        );
        assert_ne!(mutated.stored_checksum, encoded.stored_checksum, "{field}");
    }
}

#[test]
fn companion_codec_round_trips_every_legal_serving_state_and_reason() {
    let legal_statuses = [
        OwnerReadStatus {
            state: OwnerServingState::Serving,
            reason: None,
        },
        OwnerReadStatus {
            state: OwnerServingState::ServingDisabled,
            reason: Some(OwnerServingReason::DisabledByConfiguration),
        },
        OwnerReadStatus {
            state: OwnerServingState::NotServing,
            reason: Some(OwnerServingReason::StartupFailure(
                "runtime directory unavailable".to_owned(),
            )),
        },
        OwnerReadStatus {
            state: OwnerServingState::NotServing,
            reason: Some(OwnerServingReason::ShutdownDraining),
        },
        OwnerReadStatus {
            state: OwnerServingState::NotApplicable,
            reason: None,
        },
        OwnerReadStatus {
            state: OwnerServingState::NotApplicable,
            reason: Some(OwnerServingReason::PlatformUnsupported),
        },
    ];
    let mut checksums = BTreeSet::new();
    for status in legal_statuses {
        let mut fields = golden_companion_fields();
        fields.owner_read_status = status.clone();
        let encoded = encode_companion_record_for_test(&fields)
            .unwrap_or_else(|err| panic!("encode {status:?}: {err}"));
        let decoded =
            decode_companion_record_for_test(&encoded.checksummed_payload, encoded.stored_checksum)
                .unwrap_or_else(|err| panic!("decode {status:?}: {err}"));
        assert_eq!(decoded.fields.owner_read_status, status);
        assert!(checksums.insert(encoded.stored_checksum));
    }
}

#[test]
#[cfg(unix)]
fn companion_owner_user_comes_from_effective_identity_not_database_metadata() {
    let _serial = serialise_coordination_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("effective-user.db");
    seed_store(&path);
    let file_owner =
        LocalUserIdentity(fs::metadata(&path).expect("inspect database owner").uid() as u64);
    let controlled_effective_user = LocalUserIdentity(file_owner.0 + 1);
    assert_ne!(controlled_effective_user, file_owner);

    set_companion_effective_user_identity_for_test(controlled_effective_user);
    let override_guard = EffectiveUserIdentityOverride;
    assert_eq!(
        companion_effective_user_identity_for_test(),
        controlled_effective_user,
        "the controlled fixture must pass through the production effective-identity source"
    );
    reset_companion_effective_user_source_calls_for_test();
    let writer = Database::open(&path).expect("reopen through the real companion writer");
    writer.close().expect("close effective-user writer run");
    assert!(
        companion_effective_user_source_calls_for_test() > 0,
        "the real companion writer must consult the production effective-identity source"
    );

    let record = inspect_companion_record_for_test(&path)
        .expect("inspect the record written through the controlled identity source");
    assert_complete_companion_record_for_user(&path, &record, controlled_effective_user);
    assert_ne!(record.fields.owner_user, file_owner);
    assert_eq!(
        LocalUserIdentity(fs::metadata(&path).expect("reinspect database owner").uid() as u64,),
        file_owner,
        "the fixture changes no ownership or permissions"
    );
    drop(override_guard);
}

#[test]
#[cfg(unix)]
fn database_identity_is_stable_runs_are_unique_and_same_path_reset_changes_identity() {
    let _serial = serialise_coordination_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("identity.db");
    seed_store(&path);
    let mut records =
        vec![inspect_companion_record_for_test(&path).expect("inspect initial companion record")];
    for _ in 0..8 {
        let db = Database::open(&path).expect("reopen same store");
        db.close().expect("close same store reopen");
        records.push(
            inspect_companion_record_for_test(&path).expect("inspect reopened companion record"),
        );
    }
    for record in &records {
        assert_complete_companion_record(&path, record);
        assert_eq!(record.fields.owner_user, effective_user_identity());
        assert_eq!(record.fields.process_id, std::process::id());
        assert_eq!(
            record.fields.database_identity,
            records[0].fields.database_identity
        );
    }
    let runs = records
        .iter()
        .map(|record| record.fields.writer_run_number)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        runs.len(),
        records.len(),
        "all same-path writer runs differ"
    );
    for pair in records.windows(2) {
        assert!(
            pair[1].fields.generation > pair[0].fields.generation,
            "complete companion generations must increase across writer runs"
        );
    }

    let identity_before_reset = records[0].fields.database_identity;
    Database::force_reset(&path).expect("reset same pathname");
    let reset = inspect_companion_record_for_test(&path).expect("inspect reset companion record");
    assert_complete_companion_record(&path, &reset);
    assert_ne!(reset.fields.database_identity, identity_before_reset);
}

fn planned_record_from_crash_output(
    output: &str,
) -> std::result::Result<CompanionRecordObservation, String> {
    let marker = "COMPANION_PLANNED_RECORD=";
    let line = output
        .lines()
        .find_map(|line| line.find(marker).map(|index| &line[index + marker.len()..]))
        .ok_or_else(|| "crash child did not expose its complete planned record".to_owned())?;
    let (payload, checksum) = line
        .split_once(':')
        .ok_or_else(|| "planned record marker lacks checksum".to_owned())?;
    let payload = decode_hex(payload)?;
    let checksum: [u8; 32] = decode_hex(checksum)?
        .try_into()
        .map_err(|_| "planned record checksum is not 32 bytes".to_owned())?;
    decode_companion_record_for_test(&payload, checksum)
}

#[test]
#[cfg(unix)]
fn every_structural_companion_io_boundary_recovers_exactly_prior_or_planned_record() {
    let _serial = serialise_coordination_test();
    let root = tempfile::tempdir().expect("scratch directory");
    for boundary in CompanionWriteBoundary::ALL {
        let case_directory = root.path().join(format!("boundary-{boundary:?}"));
        fs::create_dir(&case_directory).expect("create isolated crash-boundary directory");
        let path = case_directory.join("store.db");
        seed_store(&path);
        assert_only_store_and_appended_companion(&path);
        let companion = appended_companion(&path);
        let companion_identity = file_identity(&companion);
        let raw_prior = read_raw_companion_from_exact_inode(&companion, companion_identity)
            .unwrap_or_else(|err| panic!("{boundary:?}: parse prior raw companion: {err}"));
        assert_eq!(raw_prior.identity, companion_identity);
        assert_raw_companion_parser_rejects_damage(&raw_prior);
        let prior = raw_prior.parsed.active_record.clone();
        assert!(raw_prior.parsed.active_slot < 2);
        assert_eq!(
            raw_prior.parsed.selector_generation,
            prior.fields.generation
        );
        assert_complete_companion_record(&path, &prior);
        let crashed = spawn_companion_crash(&path, boundary);
        let output = format!(
            "{}{}",
            String::from_utf8_lossy(&crashed.output.stdout),
            String::from_utf8_lossy(&crashed.output.stderr)
        );
        assert!(
            !crashed.output.status.success()
                && output.contains(&format!("COMPANION_CRASH_AFTER={boundary:?}")),
            "{boundary:?}: writer must abort inside the actual write/sync primitive: {output}"
        );
        let planned = planned_record_from_crash_output(&output)
            .unwrap_or_else(|err| panic!("{boundary:?}: {err}"));
        assert_complete_companion_record(&path, &planned);
        assert_eq!(planned.fields.generation, prior.fields.generation + 1);
        assert_eq!(planned.fields.process_id, crashed.process_id);
        assert_eq!(planned.fields.format_version, prior.fields.format_version);
        assert_eq!(
            planned.fields.database_identity,
            prior.fields.database_identity
        );
        assert_ne!(
            planned.fields.writer_run_number,
            prior.fields.writer_run_number
        );
        assert_eq!(planned.fields.owner_user, prior.fields.owner_user);
        assert_eq!(planned.fields.channel_address, prior.fields.channel_address);
        assert_only_store_and_appended_companion(&path);
        assert_eq!(file_identity(&companion), companion_identity);
        let raw_recovered = read_raw_companion_from_exact_inode(&companion, companion_identity)
            .unwrap_or_else(|err| panic!("{boundary:?}: parse crashed raw companion: {err}"));
        assert_eq!(
            raw_recovered.parsed.selector_generation,
            raw_recovered.parsed.active_record.fields.generation
        );
        for slot in &raw_recovered.parsed.slots {
            if let RawCompanionSlot::Complete(record) = slot {
                assert!(
                    record == &prior || record == &planned,
                    "{boundary:?}: a raw slot contains neither the exact prior nor planned record"
                );
            }
        }
        match boundary {
            CompanionWriteBoundary::SlotPayloadWrite | CompanionWriteBoundary::SlotPayloadSync => {
                assert_eq!(raw_recovered.parsed.active_record, prior);
                assert!(
                    raw_recovered
                        .parsed
                        .slots
                        .iter()
                        .any(|slot| matches!(slot, RawCompanionSlot::Torn(_))),
                    "{boundary:?}: the independently decoded inactive slot must reject torn payload bytes"
                );
            }
            CompanionWriteBoundary::SlotChecksumWrite
            | CompanionWriteBoundary::SlotChecksumSync => {
                assert_eq!(raw_recovered.parsed.active_record, prior);
                assert!(
                    raw_recovered.parsed.slots.iter().any(
                        |slot| matches!(slot, RawCompanionSlot::Complete(record) if record == &planned)
                    ),
                    "{boundary:?}: the complete planned inactive slot is absent"
                );
            }
            CompanionWriteBoundary::ActiveSlotWrite => {
                assert!(
                    raw_recovered.parsed.active_record == prior
                        || raw_recovered.parsed.active_record == planned,
                    "selector write exposed neither exact complete generation"
                );
            }
            CompanionWriteBoundary::ActiveSlotSync => {
                assert_eq!(
                    raw_recovered.parsed.active_record, planned,
                    "the final selector sync must publish the exact planned record"
                );
            }
        }
        let writer = Database::open(&path)
            .unwrap_or_else(|err| panic!("{boundary:?}: reopen recovered store: {err}"));
        writer.close().expect("close recovered store");
        assert_eq!(file_identity(&companion), companion_identity);
        assert_only_store_and_appended_companion(&path);
        read_raw_companion_from_exact_inode(&companion, companion_identity)
            .unwrap_or_else(|err| panic!("{boundary:?}: parse companion after recovery: {err}"));
    }
}

struct StoreReplacementFinished;

impl Drop for StoreReplacementFinished {
    fn drop(&mut self) {
        finish_store_replacement_sequence_for_test();
    }
}

#[test]
#[cfg(unix)]
fn reset_retains_one_original_guard_through_every_replacement_boundary() {
    let _serial = serialise_coordination_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("reset.db");
    seed_store(&path);
    let companion = appended_companion(&path);
    let companion_before = file_identity(&companion);
    let identity_before = inspect_companion_record_for_test(&path)
        .expect("inspect pre-reset companion")
        .fields
        .database_identity;

    let _sequence = ArmedStoreReplacementSequence::arm();
    let reset = StoreReplacementTask::spawn(path.clone());
    let mut coordination_error = None;
    let mut checkpoints = Vec::new();
    let mut lock_sentinel = None;
    for (index, expected) in StoreReplacementBoundary::ALL.into_iter().enumerate() {
        match next_store_replacement_event_for_test() {
            StoreReplacementEvent::Checkpoint(actual) => {
                if actual != expected {
                    coordination_error = Some(format!(
                        "expected checkpoint {expected:?}, reached {actual:?}"
                    ));
                }
                if index == 0 {
                    match spawn_companion_lock_sentinel(&path) {
                        Ok(sentinel) => lock_sentinel = Some(sentinel),
                        Err(error) if coordination_error.is_none() => {
                            coordination_error = Some(format!(
                                "{actual:?}: companion lock sentinel did not arm: {error}"
                            ));
                        }
                        Err(_) => {}
                    }
                }
                if let Some(sentinel) = lock_sentinel.as_mut()
                    && let Err(error) = sentinel.assert_kernel_blocked()
                    && coordination_error.is_none()
                {
                    coordination_error = Some(format!("{actual:?}: {error}"));
                }
                if let Err(error) = commanded_replacement_writer_probe(&path)
                    && coordination_error.is_none()
                {
                    coordination_error = Some(format!(
                        "{actual:?}: checkpointed production writer probe failed: {error}"
                    ));
                }
                if index + 1 == StoreReplacementBoundary::ALL.len()
                    && let Some(sentinel) = lock_sentinel.as_mut()
                    && let Err(error) = sentinel.permit_final_release()
                    && coordination_error.is_none()
                {
                    coordination_error = Some(format!(
                        "final guard checkpoint did not safely permit release: {error}"
                    ));
                }
                release_store_replacement_checkpoint_for_test();
                checkpoints.push(actual);
            }
            event => {
                coordination_error = Some(format!(
                    "reset ended before {expected:?}; coordination event: {event:?}"
                ));
                cancel_store_replacement_sequence_for_test();
                break;
            }
        }
    }
    if coordination_error.is_none() {
        let event = next_store_replacement_event_for_test();
        if event != StoreReplacementEvent::CompletedAfterGuardRelease {
            coordination_error = Some(format!("unexpected completion event: {event:?}"));
            cancel_store_replacement_sequence_for_test();
        }
    }
    let reset_result = reset.join();
    let completion_acquisition = lock_sentinel
        .map(CompanionLockSentinel::complete)
        .unwrap_or_else(|| Err("companion lock sentinel never armed".to_owned()));

    assert!(
        coordination_error.is_none(),
        "{}",
        coordination_error.unwrap_or_default()
    );
    assert_eq!(checkpoints.as_slice(), &StoreReplacementBoundary::ALL);
    reset_result
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
        .expect("production reset succeeds");
    assert_eq!(completion_acquisition, Ok(()));
    assert_eq!(file_identity(&companion), companion_before);
    let reset_record = inspect_companion_record_for_test(&path).expect("inspect reset result");
    assert_ne!(reset_record.fields.database_identity, identity_before);
    let reset_store = Database::open(&path).expect("open reset result");
    assert!(
        reset_store
            .execute("SELECT * FROM proof", &Default::default())
            .is_err()
    );
    reset_store.close().expect("close reset result");
}

#[test]
#[cfg(unix)]
fn every_reset_replacement_crash_recovers_old_or_new_store_with_its_matching_companion() {
    let _serial = serialise_coordination_test();
    let root = tempfile::tempdir().expect("scratch directory");
    for boundary in StoreReplacementBoundary::ALL {
        let case_directory = root.path().join(format!("reset-crash-{boundary:?}"));
        fs::create_dir(&case_directory).expect("create reset crash case directory");
        let path = case_directory.join("store.db");
        seed_store(&path);
        let companion = appended_companion(&path);
        let companion_identity = file_identity(&companion);
        let prior_store_identity = file_identity(&path);
        let prior_record =
            inspect_companion_record_for_test(&path).expect("inspect pre-reset companion");
        let decoys = seed_preserved_reset_decoys(&path);
        let preserved_decoy_fingerprint =
            store_fingerprint_for_test(&decoys.complete_generated_shaped_store).unwrap_or_else(
                |error| panic!("{boundary:?}: fingerprint the complete preserved decoy: {error}"),
            );
        let baseline = directory_entries(&path);

        let output = spawn_reset_replacement_crash(&path, boundary);
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !output.status.success()
                && text.contains(&format!("{STORE_REPLACEMENT_CRASH_BOUNDARY}{boundary:?}")),
            "{boundary:?}: reset child must abort at the actual replacement checkpoint: {text}"
        );

        let generated_target = reset_generated_target_from_output(&text);
        let generated_identity = reset_generated_identity_from_output(&text);
        let generated_fingerprint = reset_generated_fingerprint_from_output(&text);
        let independently_identified_target = match (generated_target.as_ref(), generated_identity)
        {
            (Some(reported), Some(identity)) => {
                let identified =
                    independently_locate_reset_generated_inode(&path, &baseline, identity);
                if reported.exists() {
                    assert_eq!(
                        &identified, reported,
                        "{boundary:?}: reported reset target is not the independently discovered inode"
                    );
                } else {
                    assert_eq!(
                        identified, path,
                        "{boundary:?}: renamed reset target is not at the active pathname"
                    );
                }
                let fingerprint = store_fingerprint_for_test(&identified).unwrap_or_else(|error| {
                    panic!(
                        "{boundary:?}: fingerprint independently discovered reset target: {error}"
                    )
                });
                assert_eq!(
                    Some(fingerprint),
                    generated_fingerprint,
                    "{boundary:?}: child reset intent differs from the independently fingerprinted inode"
                );
                Some(identified)
            }
            (None, None) => {
                assert_eq!(boundary, StoreReplacementBoundary::GuardAcquired);
                assert!(generated_fingerprint.is_none());
                None
            }
            values => panic!("{boundary:?}: incomplete reset target evidence: {values:?}"),
        };
        let pre_recovery: ReplacementIntentObservation = inspect_replacement_intent_for_test(&path)
            .unwrap_or_else(|error| panic!("{boundary:?}: capture exact reset intent: {error}"));
        assert!(
            pre_recovery.active_record.is_some(),
            "{boundary:?}: exact active reset record was not captured before recovery"
        );
        assert_eq!(
            pre_recovery.current_store_fingerprint,
            store_fingerprint_for_test(&path)
                .unwrap_or_else(|error| panic!("{boundary:?}: recapture reset store: {error}")),
            "{boundary:?}: pre-recovery reset fingerprint was not stable"
        );
        // Read out of the durable companion bytes at every boundary, so what
        // recovery is entitled to act on never rests on the reader recovery
        // itself uses. The whole-file parse also proves one complete record
        // still stands beside any recorded intent.
        let raw_pre_recovery = read_raw_companion_from_exact_inode(&companion, companion_identity)
            .unwrap_or_else(|error| {
                panic!("{boundary:?}: parse the durable pre-recovery companion: {error}")
            });
        let recorded_intent = recorded_replacement_intent(&raw_pre_recovery.parsed);
        assert_eq!(
            recorded_intent
                .as_ref()
                .map(|intent| intent.store_fingerprint),
            pre_recovery.pending_store_fingerprint,
            "{boundary:?}: durable companion bytes and the reader disagree on the recorded fingerprint"
        );
        assert_eq!(
            recorded_intent
                .as_ref()
                .map(|intent| intent.intended_record.clone()),
            pre_recovery.pending_intended_record,
            "{boundary:?}: durable companion bytes and the reader disagree on the recorded record"
        );
        if boundary_requires_recorded_reset_intent(boundary) {
            let intent = recorded_intent.as_ref().unwrap_or_else(|| {
                panic!("{boundary:?}: the prepared replacement left no durable intent")
            });
            assert_eq!(
                Some(intent.store_fingerprint),
                generated_fingerprint,
                "{boundary:?}: the recorded intent does not name the exact generated reset target"
            );
            assert_ne!(
                intent.store_fingerprint, preserved_decoy_fingerprint,
                "{boundary:?}: the recorded intent names the complete generated-shaped decoy"
            );
            assert_eq!(
                intent.intended_record.fields.channel_address, prior_record.fields.channel_address,
                "{boundary:?}: the recorded intent names a different store pathname"
            );
            assert_ne!(
                intent.intended_record.fields.database_identity,
                prior_record.fields.database_identity,
                "{boundary:?}: the recorded intent reuses the replaced generation's identity"
            );
            assert!(
                intent.intended_record.fields.generation > prior_record.fields.generation,
                "{boundary:?}: the recorded intent does not advance the companion generation"
            );
        }
        if boundary == StoreReplacementBoundary::ReplacementPublishedBeforeGuardRelease {
            assert!(
                recorded_intent.is_none(),
                "{boundary:?}: publication left the replacement intent unresolved"
            );
            assert_ne!(
                raw_pre_recovery
                    .parsed
                    .active_record
                    .fields
                    .database_identity,
                prior_record.fields.database_identity,
                "{boundary:?}: the published replacement wears the replaced generation's identity"
            );
        }
        let active_store_identity = file_identity(&path);
        let (old_generation, expected_record) = if active_store_identity == prior_store_identity {
            let active = pre_recovery
                .active_record
                .as_ref()
                .expect("old reset generation retains an active record");
            assert_eq!(
                active.fields.database_identity, prior_record.fields.database_identity,
                "{boundary:?}: exact old store inode is paired with the wrong identity"
            );
            (true, active)
        } else {
            assert_eq!(
                Some(active_store_identity),
                generated_identity,
                "{boundary:?}: active inode is neither the exact old nor exact generated target"
            );
            let fingerprint = generated_fingerprint
                .expect("non-old reset store must match exact generated-target intent");
            assert_eq!(
                pre_recovery.current_store_fingerprint, fingerprint,
                "{boundary:?}: store matches neither exact old nor exact intended fingerprint"
            );
            let intended = if let Some(pending) = pre_recovery.pending_intended_record.as_ref() {
                assert_eq!(pre_recovery.pending_store_fingerprint, Some(fingerprint));
                pending
            } else {
                pre_recovery
                    .active_record
                    .as_ref()
                    .expect("published reset target retains its exact active record")
            };
            (false, intended)
        };

        let recovered = Database::open(&path)
            .unwrap_or_else(|error| panic!("{boundary:?}: recover reset store: {error}"));
        let observed_old_generation = recovered
            .execute("SELECT * FROM proof", &Default::default())
            .is_ok();
        assert_eq!(
            observed_old_generation, old_generation,
            "{boundary:?}: recovered data does not match the exact classified store fingerprint"
        );
        recovered
            .close()
            .unwrap_or_else(|error| panic!("{boundary:?}: close recovered reset store: {error}"));
        let recovered_record = inspect_companion_record_for_test(&path)
            .unwrap_or_else(|error| panic!("{boundary:?}: inspect recovered companion: {error}"));
        assert_eq!(
            recovered_record.fields.database_identity, expected_record.fields.database_identity,
            "{boundary:?}: recovered store did not retain the identity bound to its exact intent"
        );
        assert_eq!(file_identity(&companion), companion_identity);
        if let Some(target) = generated_target {
            assert!(
                !target.exists(),
                "{boundary:?}: exact generated reset target survived recovery"
            );
            assert!(
                !appended_companion(&target).exists(),
                "{boundary:?}: generated reset sidecar survived recovery"
            );
        }
        if let Some(identified) = independently_identified_target
            && identified != path
        {
            assert!(
                !identified.exists(),
                "{boundary:?}: independently identified reset target survived recovery"
            );
        }
        assert_reset_decoys_preserved(&decoys);
        assert!(
            new_reset_generated_residue(&path, &baseline).is_empty(),
            "{boundary:?}: reset recovery left non-baseline generated residue: {:?}",
            new_reset_generated_residue(&path, &baseline)
        );
        read_raw_companion_from_exact_inode(&companion, companion_identity)
            .unwrap_or_else(|error| panic!("{boundary:?}: parse recovered companion: {error}"));
    }
}

/// A pathname is not an identity. Between the moment a reset records the
/// fingerprint of the target it generated and the moment recovery removes that
/// target, anything at all may arrive at the pathname the target sat under --
/// an operator's own file, restored or copied in beside a store they are
/// repairing. Recovery is a DESTRUCTIVE filesystem operation, so it may remove
/// only the inode the intent actually recorded: a file it cannot prove is that
/// inode is left in place, whatever its name says.
///
/// The swap is staged while the crashed reset is gone, which is exactly the
/// window a racing rename would land in and needs no thread and no sleeping to
/// reproduce.
#[test]
#[cfg(unix)]
fn a_reset_target_pathname_swapped_before_recovery_never_removes_the_swapped_in_file() {
    const INNOCENT: &[u8] =
        b"an operator's own file, restored at the pathname the reset target left";
    let _serial = serialise_coordination_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("store.db");
    seed_store(&path);
    let decoys = seed_preserved_reset_decoys(&path);

    let output =
        spawn_reset_replacement_crash(&path, StoreReplacementBoundary::BeforeAtomicReplacement);
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !output.status.success()
            && text.contains(&format!(
                "{STORE_REPLACEMENT_CRASH_BOUNDARY}{:?}",
                StoreReplacementBoundary::BeforeAtomicReplacement
            )),
        "the reset child must abort with its intent recorded and its target still on disk: {text}"
    );
    let recorded_target = reset_generated_target_from_output(&text)
        .expect("the crashed reset names its exact target");
    let recorded_fingerprint = reset_generated_fingerprint_from_output(&text)
        .expect("the crashed reset names the exact fingerprint it recorded");
    assert!(
        recorded_target.exists(),
        "the crashed reset must leave its recorded target on disk for recovery to remove"
    );

    // The swap: the recorded inode moves off the pathname under a name the
    // recovery scan never considers, and an unrelated file arrives at the
    // pathname recovery is about to walk.
    let moved_aside = root.path().join("recorded-target-moved-aside");
    fs::rename(&recorded_target, &moved_aside).expect("move the recorded target off its pathname");
    fs::write(&recorded_target, INNOCENT).expect("an unrelated file arrives at that pathname");
    let innocent_identity = file_identity(&recorded_target);
    assert_ne!(
        store_fingerprint_for_test(&recorded_target).expect("fingerprint the swapped-in file"),
        recorded_fingerprint,
        "the swapped-in file must not be the inode the intent recorded, or this journey proves \
         nothing"
    );

    let recovered = Database::open(&path).expect("recovery opens the surviving store");
    recovered.close().expect("close the recovered store");

    assert!(
        recorded_target.exists(),
        "recovery removed the file that merely occupied the recorded target's PATHNAME: {}",
        recorded_target.display()
    );
    assert_eq!(
        file_identity(&recorded_target),
        innocent_identity,
        "recovery replaced the swapped-in file at {}",
        recorded_target.display()
    );
    assert_eq!(
        fs::read(&recorded_target).expect("read the swapped-in file"),
        INNOCENT,
        "recovery rewrote the swapped-in file at {}",
        recorded_target.display()
    );
    assert_eq!(
        file_identity(&moved_aside),
        {
            let metadata = fs::metadata(&moved_aside).expect("stat the moved-aside inode");
            (metadata.dev(), metadata.ino())
        },
        "the moved-aside recorded inode must still be identifiable"
    );
    assert_reset_decoys_preserved(&decoys);
}

/// The same destructive recovery, run where the store's own identity cannot be
/// proven: a second hard link means the pathname is not the only name for that
/// inode, so nothing can establish that the file recovery is about to act on is
/// the generation the intent was recorded against. It must refuse typed and
/// remove nothing, rather than proceed on the pathname's word.
#[test]
#[cfg(unix)]
fn reset_recovery_refuses_typed_when_the_store_identity_cannot_be_proven() {
    let _serial = serialise_coordination_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("store.db");
    seed_store(&path);
    let decoys = seed_preserved_reset_decoys(&path);

    let output =
        spawn_reset_replacement_crash(&path, StoreReplacementBoundary::BeforeAtomicReplacement);
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !output.status.success(),
        "the reset child must abort with its intent recorded: {text}"
    );
    let recorded_target = reset_generated_target_from_output(&text)
        .expect("the crashed reset names its exact target");
    let recorded_identity = file_identity(&recorded_target);

    // A second name for the store inode. Nothing about the pathname now
    // establishes which file recovery would be acting on.
    let second_name = root.path().join("store.db.second-name");
    fs::hard_link(&path, &second_name).expect("give the store inode a second name");

    // An opened database is not printable, so the refusal is taken by pattern.
    let Err(error) = Database::open(&path) else {
        panic!("recovery must refuse a store whose identity cannot be proven");
    };
    let reported = format!("{error}");
    assert!(
        reported.contains(&path.display().to_string()) && reported.contains("fingerprint"),
        "recovery must refuse by naming the store whose identity it could not prove, got \
         {reported}"
    );

    assert!(
        recorded_target.exists() && file_identity(&recorded_target) == recorded_identity,
        "a refused recovery removed the recorded target at {}",
        recorded_target.display()
    );
    assert_reset_decoys_preserved(&decoys);
}

/// Park two readers inside a production `ReadSession::open` below this
/// runtime root, then let a production writer meet them: the refusal must
/// name both live readers, and both breadcrumbs must sit in the directory the
/// one production resolver derives from that root for reader and writer
/// alike. Everything asserted here is a path or a reported identity, never a
/// timing.
#[cfg(unix)]
fn assert_production_readers_are_named_where_the_writer_scans(runtime_directory: &Path) {
    let root = tempfile::tempdir().expect("scratch directory");
    let store_directory = root.path().join("store");
    fs::create_dir(&store_directory).expect("create store directory");
    let path = store_directory.join("production-readers.db");
    seed_store(&path);

    let mut first = spawn_production_read_session(&path, runtime_directory);
    let mut second = spawn_production_read_session(&path, runtime_directory);
    let first_evidence = first
        .wait_until_held()
        .expect("the first production read session reaches its read-lock checkpoint");
    let second_evidence = second
        .wait_until_held()
        .expect("the second production read session reaches its read-lock checkpoint");
    let first_breadcrumb = first_evidence
        .breadcrumb
        .clone()
        .expect("a production read session publishes its reader identity");
    let second_breadcrumb = second_evidence
        .breadcrumb
        .clone()
        .expect("a production read session publishes its reader identity");
    assert_eq!(first_breadcrumb.process_id, first.process_id());
    assert_eq!(second_breadcrumb.process_id, second.process_id());
    assert!(!first_breadcrumb.process_name.is_empty());
    assert!(!second_breadcrumb.process_name.is_empty());
    #[cfg(target_os = "linux")]
    {
        assert_breadcrumb_matches_live_linux_process(&first_breadcrumb);
        assert_breadcrumb_matches_live_linux_process(&second_breadcrumb);
    }

    match probe_writer(&path, runtime_directory) {
        WriterProbeResult::HeldByReaders {
            attempts,
            observed_direct_readers,
            verified_readers,
            message,
        } => {
            assert_eq!(attempts, 1, "reader refusal bypasses writer retry windows");
            assert_eq!(
                sorted_breadcrumbs(verified_readers),
                sorted_breadcrumbs(vec![first_breadcrumb.clone(), second_breadcrumb.clone()]),
                "a writer refused by live production readers is told exactly who they are"
            );
            assert_eq!(
                observed_direct_readers, 2,
                "the reported count is an observation of the live readers, not a floor"
            );
            assert!(message.contains(&first_breadcrumb.process_name));
            assert!(message.contains(&format!("process {}", first_breadcrumb.process_id)));
            assert!(message.contains(&second_breadcrumb.process_name));
            assert!(message.contains(&format!("process {}", second_breadcrumb.process_id)));
        }
        other => {
            panic!("two live production read sessions must refuse a writer by name, got: {other:?}")
        }
    }

    let scanned_directory = expected_reader_breadcrumb_directory(&path, runtime_directory);
    let first_breadcrumb_path = first_evidence
        .breadcrumb_path
        .clone()
        .expect("a production read session returns the breadcrumb path it published");
    let second_breadcrumb_path = second_evidence
        .breadcrumb_path
        .clone()
        .expect("a production read session returns the breadcrumb path it published");
    assert_ne!(
        first_breadcrumb_path, second_breadcrumb_path,
        "simultaneous readers require independent locked breadcrumb files"
    );
    assert_runtime_breadcrumb_path(&first_breadcrumb_path, &scanned_directory);
    assert_runtime_breadcrumb_path(&second_breadcrumb_path, &scanned_directory);

    first
        .release()
        .expect("the first production read session finishes and releases its read lock");
    second
        .release()
        .expect("the second production read session finishes and releases its read lock");
    assert!(!first_breadcrumb_path.exists());
    assert!(!second_breadcrumb_path.exists());
}

/// A runtime root short enough that a local channel pathname below it still
/// fits a Unix socket address.
#[cfg(unix)]
fn short_runtime_root() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("cdb")
        .tempdir_in("/tmp")
        .expect("a runtime root short enough for local channel addresses")
}

/// A writer refused because direct readers hold the store is told who they
/// are, and both live readers here reached the store through production path
/// resolution -- no test adapter hands either side a runtime root.
///
/// Two readers park inside `ReadSession::open`, so the reported count cannot
/// be satisfied by the typed floor a single refusal carries: only a real
/// observation reaches two. The same run pins the agreement the report rests
/// on, as a statement about paths rather than about timing -- one runtime
/// root goes to the reader and to the writer, and the one production resolver
/// must derive one breadcrumb directory from it, the directory the readers
/// publish into and the writer scans.
#[test]
#[cfg(unix)]
fn a_writer_is_told_who_the_production_read_sessions_holding_the_store_are() {
    let _serial = serialise_coordination_test();
    let runtime_root = short_runtime_root();
    create_runtime_directory(runtime_root.path());
    assert_production_readers_are_named_where_the_writer_scans(runtime_root.path());
}

/// An operator may keep their runtime root in a directory they named
/// `contextdb` themselves. That spelling says nothing about whether the
/// pathname is a root or the runtime directory inside one, so readers and the
/// writer still meet in the root's `contextdb` child -- one level inside the
/// operator's directory, where this process owns the permissions -- exactly
/// as they do below any other root.
#[test]
#[cfg(unix)]
fn production_readers_and_a_writer_meet_below_a_runtime_root_named_contextdb() {
    let _serial = serialise_coordination_test();
    let base = short_runtime_root();
    let runtime_directory = base.path().join("contextdb");
    create_runtime_directory(&runtime_directory);
    assert_production_readers_are_named_where_the_writer_scans(&runtime_directory);
}
