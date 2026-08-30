//! Production-dispatch proof that migration holds one original appended
//! companion guard continuously through every replacement operation.

use contextdb_cli::ops::dispatch_if_subcommand;
use contextdb_cli::ops::migration_replacement_test_scaffold::{
    MigrationReplacementBoundary, MigrationReplacementEvent, arm_final_rename_failure_for_test,
    arm_migration_replacement_sequence_for_test, arm_temporary_companion_cleanup_failure_for_test,
    cancel_migration_replacement_sequence_for_test, finish_migration_replacement_sequence_for_test,
    next_migration_replacement_event_for_test, release_migration_replacement_checkpoint_for_test,
};
use contextdb_cli::{EXIT_ERROR, EXIT_OK};
use contextdb_core::read_contract::ReadFailureKind;
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use contextdb_engine::persistence::RedbPersistence;
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    CompanionRecordObservation, RawRedbWriterOpenObservation, inspect_companion_record_for_test,
    inspect_replacement_intent_for_test, replace_legacy_text_witness_for_test,
    store_fingerprint_for_test, try_raw_redb_writer_open_for_test,
};
#[cfg(unix)]
use contextdb_engine::persistence::read_persistence_test_scaffold::{
    LockedMigrationSourceObservation, decode_companion_record_for_test,
    observe_locked_migration_sources_for_test, reset_locked_migration_sources_for_test,
};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read as _, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread;

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

#[cfg(unix)]
unsafe extern "C" {
    #[link_name = "flock"]
    fn kernel_flock(
        file_descriptor: std::ffi::c_int,
        operation: std::ffi::c_int,
    ) -> std::ffi::c_int;
}

#[cfg(unix)]
const KERNEL_LOCK_EXCLUSIVE: std::ffi::c_int = 2;
#[cfg(unix)]
const KERNEL_LOCK_NONBLOCKING: std::ffi::c_int = 4;

#[cfg(unix)]
fn try_kernel_lock_exclusive(file: &fs::File) -> std::io::Result<bool> {
    let result = unsafe {
        kernel_flock(
            file.as_raw_fd(),
            KERNEL_LOCK_EXCLUSIVE | KERNEL_LOCK_NONBLOCKING,
        )
    };
    if result == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::WouldBlock {
        Ok(false)
    } else {
        Err(error)
    }
}

#[cfg(unix)]
fn kernel_lock_exclusive(file: &fs::File) -> std::io::Result<()> {
    let result = unsafe { kernel_flock(file.as_raw_fd(), KERNEL_LOCK_EXCLUSIVE) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

const CHILD_ROLE_ENV: &str = "CONTEXTDB_MIGRATION_COORDINATION_CHILD_ROLE";
const CHILD_PATH_ENV: &str = "CONTEXTDB_MIGRATION_COORDINATION_STORE_PATH";
const SENTINEL_PERMIT_FINAL_RELEASE: &str = "permit-final-release";
const SENTINEL_ARMED: &str = "MIGRATION_LOCK_SENTINEL_ARMED";
const SENTINEL_GAP: &str = "MIGRATION_LOCK_SENTINEL_ACQUIRED_EARLY";
const SENTINEL_FINAL_PERMISSION: &str = "MIGRATION_LOCK_SENTINEL_FINAL_PERMISSION";
const SENTINEL_ACQUIRED: &str = "MIGRATION_LOCK_SENTINEL_ACQUIRED_AFTER_RELEASE";
const SENTINEL_OTHER: &str = "MIGRATION_LOCK_SENTINEL_OTHER=";
const CHECKPOINT_WRITER_REFUSED: &str = "MIGRATION_CHECKPOINT_WRITER_REFUSED";
const CHECKPOINT_WRITER_ACQUIRED: &str = "MIGRATION_CHECKPOINT_WRITER_ACQUIRED";
const CHECKPOINT_WRITER_OTHER: &str = "MIGRATION_CHECKPOINT_WRITER_OTHER=";
const MIGRATION_TARGET_INTRUSION_TABLE: &str = "migration_target_intrusion";
const SCENARIO_BOUNDARY_ENV: &str = "CONTEXTDB_MIGRATION_SCENARIO_BOUNDARY";
const SCENARIO_GENERATED_TARGET: &str = "MIGRATION_SCENARIO_GENERATED_TARGET=";
const SCENARIO_GENERATED_TARGET_IDENTITY: &str = "MIGRATION_SCENARIO_GENERATED_TARGET_IDENTITY=";
const SCENARIO_GENERATED_TARGET_FINGERPRINT: &str =
    "MIGRATION_SCENARIO_GENERATED_TARGET_FINGERPRINT=";
const SCENARIO_OPERATION_EXIT: &str = "MIGRATION_SCENARIO_OPERATION_EXIT=";
const SCENARIO_CRASH_BOUNDARY: &str = "MIGRATION_SCENARIO_CRASH_BOUNDARY=";

static MIGRATION_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

fn serialise_migration_test() -> MutexGuard<'static, ()> {
    MIGRATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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

struct ArmedMigrationReplacementSequence;

impl ArmedMigrationReplacementSequence {
    fn arm() -> Self {
        arm_migration_replacement_sequence_for_test();
        Self
    }
}

impl Drop for ArmedMigrationReplacementSequence {
    fn drop(&mut self) {
        cancel_migration_replacement_sequence_for_test();
    }
}

struct MigrationTask {
    handle: Option<thread::JoinHandle<i32>>,
}

impl MigrationTask {
    fn spawn(path: PathBuf) -> Self {
        Self {
            handle: Some(thread::spawn(move || {
                let _finished = MigrationFinished;
                dispatch_migration(&path)
            })),
        }
    }

    fn join(mut self) -> std::thread::Result<i32> {
        self.handle
            .take()
            .expect("migration task was already joined")
            .join()
    }
}

impl Drop for MigrationTask {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        cancel_migration_replacement_sequence_for_test();
        let _ = handle.join();
    }
}

fn appended_companion(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".lock");
    PathBuf::from(name)
}

fn pre_change_replaced_companion(path: &Path) -> PathBuf {
    path.with_extension("lock")
}

#[cfg(unix)]
fn file_identity(path: &Path) -> (u64, u64) {
    let metadata =
        fs::metadata(path).unwrap_or_else(|err| panic!("stat {}: {err}", path.display()));
    (metadata.dev(), metadata.ino())
}

/// A witness value drawn from the kernel entropy source, so no earlier run,
/// process id, or fixture byte can supply the value the backup must carry.
#[cfg(unix)]
fn fresh_source_witness() -> String {
    let mut entropy = [0_u8; 16];
    fs::File::open("/dev/urandom")
        .and_then(|mut source| source.read_exact(&mut entropy))
        .expect("draw fresh source-witness entropy");
    format!("locked-source-witness-{}", encode_hex(&entropy))
}

/// The one live migration-source descriptor whose lock-time identity is
/// `identity`, restated by asking that exact descriptor rather than by
/// listing pathnames.
#[cfg(unix)]
fn locked_source_descriptor(identity: (u64, u64)) -> LockedMigrationSourceObservation {
    let mut matching = observe_locked_migration_sources_for_test()
        .into_iter()
        .filter(|observation| (observation.locked_device, observation.locked_inode) == identity)
        .collect::<Vec<_>>();
    assert_eq!(
        matching.len(),
        1,
        "exactly one held migration descriptor may carry the locked source identity: {matching:?}"
    );
    matching
        .pop()
        .expect("one held migration source descriptor")
}

#[cfg(unix)]
fn assert_locked_source_descriptor(identity: (u64, u64), link_count: u64, stage: &str) {
    let observation = locked_source_descriptor(identity);
    assert_eq!(
        (observation.observed_device, observation.observed_inode),
        identity,
        "{stage}: the held migration descriptor no longer names the locked source inode"
    );
    assert_eq!(
        observation.observed_link_count, link_count,
        "{stage}: the held migration descriptor reports the wrong link count"
    );
}

fn legacy_fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy-v1.0.0-store.db")
}

fn keyless_legacy_fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy-v1.0.0-keyless-store.db")
}

fn child_store_path() -> PathBuf {
    PathBuf::from(std::env::var_os(CHILD_PATH_ENV).expect("competing writer receives a store path"))
}

fn writer_attempt(path: &Path) -> std::result::Result<bool, String> {
    match Database::open(path) {
        Ok(db) => {
            db.close().expect("close competing writer");
            Ok(true)
        }
        Err(Error::ReadFailure(failure)) if failure.kind() == ReadFailureKind::HeldByWriter => {
            Ok(false)
        }
        Err(Error::ReadFailure(failure)) => Err(format!("typed:{:?}", failure.kind())),
        Err(err) => Err(err.to_string().replace(['\n', '\r'], " ")),
    }
}

fn target_mutation_attempt(path: &Path) -> std::result::Result<bool, String> {
    match Database::open(path) {
        Ok(db) => {
            db.execute(
                &format!(
                    "CREATE TABLE {MIGRATION_TARGET_INTRUSION_TABLE} (id INTEGER PRIMARY KEY)"
                ),
                &Default::default(),
            )
            .map_err(|error| format!("mutate generated migration target: {error}"))?;
            db.close()
                .map_err(|error| format!("close generated-target contender: {error}"))?;
            Ok(true)
        }
        Err(Error::ReadFailure(failure)) if failure.kind() == ReadFailureKind::HeldByWriter => {
            Ok(false)
        }
        Err(Error::ReadFailure(failure)) => Err(format!("typed:{:?}", failure.kind())),
        Err(error) => Err(error.to_string().replace(['\n', '\r'], " ")),
    }
}

fn raw_redb_target_writer_attempt(path: &Path) -> std::result::Result<bool, String> {
    match try_raw_redb_writer_open_for_test(path) {
        RawRedbWriterOpenObservation::Acquired => Ok(true),
        RawRedbWriterOpenObservation::DatabaseAlreadyOpen => Ok(false),
        RawRedbWriterOpenObservation::Other(error) => Err(error),
    }
}

#[cfg(unix)]
fn target_companion_lock_attempt(path: &Path) -> std::result::Result<bool, String> {
    let companion = appended_companion(path);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&companion)
        .map_err(|error| {
            format!(
                "open exact generated companion {}: {error}",
                companion.display()
            )
        })?;
    try_kernel_lock_exclusive(&file).map_err(|error| {
        format!(
            "probe exact generated companion {}: {error}",
            companion.display()
        )
    })
}

#[test]
fn migration_checkpoint_writer_child() {
    let role = std::env::var(CHILD_ROLE_ENV).ok();
    if !matches!(
        role.as_deref(),
        Some(
            "checkpoint-writer"
                | "generated-target-mutator"
                | "raw-redb-target-writer"
                | "generated-target-companion-lock"
        )
    ) {
        return;
    }
    let path = child_store_path();
    let attempt = match role.as_deref() {
        Some("generated-target-mutator") => target_mutation_attempt(&path),
        Some("raw-redb-target-writer") => raw_redb_target_writer_attempt(&path),
        #[cfg(unix)]
        Some("generated-target-companion-lock") => target_companion_lock_attempt(&path),
        _ => writer_attempt(&path),
    };
    match attempt {
        Ok(false) => println!("{CHECKPOINT_WRITER_REFUSED}"),
        Ok(true) => println!("{CHECKPOINT_WRITER_ACQUIRED}"),
        Err(error) => println!("{CHECKPOINT_WRITER_OTHER}{error}"),
    }
    std::io::stdout()
        .flush()
        .expect("flush migration checkpoint writer result");
}

#[test]
#[cfg(unix)]
fn migration_lock_sentinel_child() {
    if std::env::var(CHILD_ROLE_ENV).ok().as_deref() != Some("lock-sentinel") {
        return;
    }
    let companion = appended_companion(&child_store_path());
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&companion)
        .unwrap_or_else(|error| panic!("open migration sentinel {}: {error}", companion.display()));
    match try_kernel_lock_exclusive(&file) {
        Ok(true) => {
            println!("{SENTINEL_GAP}");
            std::io::stdout()
                .flush()
                .expect("flush initially unguarded migration companion");
            return;
        }
        Ok(false) => {}
        Err(error) => {
            println!("{SENTINEL_OTHER}{error}");
            std::io::stdout()
                .flush()
                .expect("flush migration lock-sentinel error");
            return;
        }
    }

    let final_release_permitted = Arc::new(AtomicBool::new(false));
    let command_permission = Arc::clone(&final_release_permitted);
    thread::spawn(move || {
        let mut command = String::new();
        std::io::stdin()
            .read_line(&mut command)
            .expect("read migration lock-sentinel release command");
        assert_eq!(command.trim(), SENTINEL_PERMIT_FINAL_RELEASE);
        command_permission.store(true, Ordering::SeqCst);
        println!("{SENTINEL_FINAL_PERMISSION}");
        std::io::stdout()
            .flush()
            .expect("flush migration final-release permission");
    });

    println!("{SENTINEL_ARMED}");
    std::io::stdout()
        .flush()
        .expect("flush blocked migration lock sentinel");
    if let Err(error) = kernel_lock_exclusive(&file) {
        println!("{SENTINEL_OTHER}{error}");
        std::io::stdout()
            .flush()
            .expect("flush blocking migration lock error");
        return;
    }
    if final_release_permitted.load(Ordering::SeqCst) {
        println!("{SENTINEL_ACQUIRED}");
        std::io::stdout()
            .flush()
            .expect("flush expected migration lock acquisition");
    } else {
        println!("{SENTINEL_GAP}");
        std::io::stdout()
            .flush()
            .expect("flush premature migration lock acquisition");
    }
}

fn commanded_writer(path: &Path, role: &str) -> std::result::Result<(), String> {
    let output = Command::new(std::env::current_exe().expect("current integration-test binary"))
        .arg("--exact")
        .arg("migration_checkpoint_writer_child")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, role)
        .env(CHILD_PATH_ENV, path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("start migration {role}: {error}"))?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if !output.status.success() {
        return Err(format!(
            "migration {role} failed: {}: {text}",
            output.status
        ));
    }
    if text.contains(CHECKPOINT_WRITER_REFUSED) {
        return Ok(());
    }
    if text.contains(CHECKPOINT_WRITER_ACQUIRED) {
        return Err(format!("real {role} acquired during migration"));
    }
    if let Some(index) = text.find(CHECKPOINT_WRITER_OTHER) {
        return Err(format!(
            "real competing writer returned the wrong result: {}",
            &text[index + CHECKPOINT_WRITER_OTHER.len()..]
        ));
    }
    Err(format!("migration {role} emitted no result: {text}"))
}

fn commanded_checkpoint_writer(path: &Path) -> std::result::Result<(), String> {
    commanded_writer(path, "checkpoint-writer")
}

fn commanded_target_mutator(path: &Path) -> std::result::Result<(), String> {
    commanded_writer(path, "generated-target-mutator")
}

fn commanded_raw_redb_target_writer(path: &Path) -> std::result::Result<(), String> {
    commanded_writer(path, "raw-redb-target-writer")
}

#[cfg(unix)]
fn commanded_target_companion_lock(path: &Path) -> std::result::Result<(), String> {
    commanded_writer(path, "generated-target-companion-lock")
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
                .map_err(|error| format!("read migration lock sentinel: {error}"))?;
            if read == 0 {
                return Err("migration lock sentinel exited before its marker".to_owned());
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
            .map_err(|error| format!("inspect migration lock sentinel: {error}"))?
        {
            let mut remaining = String::new();
            self.stdout
                .read_to_string(&mut remaining)
                .map_err(|error| format!("read finished migration sentinel: {error}"))?;
            self.finished = true;
            return Err(format!(
                "migration companion guard was not continuous: status={status}, output={remaining}"
            ));
        }
        Ok(())
    }

    fn permit_final_release(&mut self) -> std::result::Result<(), String> {
        let stdin = self
            .child
            .stdin
            .as_mut()
            .ok_or_else(|| "migration lock sentinel lost its command pipe".to_owned())?;
        writeln!(stdin, "{SENTINEL_PERMIT_FINAL_RELEASE}")
            .map_err(|error| format!("permit final migration release: {error}"))?;
        stdin
            .flush()
            .map_err(|error| format!("flush final migration release: {error}"))?;
        loop {
            let line = self.read_marker()?;
            if line.contains(SENTINEL_FINAL_PERMISSION) {
                return Ok(());
            }
            if line.contains(SENTINEL_GAP) {
                return Err("migration lock sentinel acquired before final release".to_owned());
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
            Err("migration lock sentinel observed a pre-release gap".to_owned())
        } else {
            Err(format!(
                "unexpected final migration sentinel marker: {marker}"
            ))
        };
        let status = self
            .child
            .wait()
            .map_err(|error| format!("wait for migration lock sentinel: {error}"))?;
        self.finished = true;
        if !status.success() {
            return Err(format!("migration lock sentinel failed: {status}"));
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
        .arg("migration_lock_sentinel_child")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "lock-sentinel")
        .env(CHILD_PATH_ENV, path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| format!("start migration lock sentinel: {error}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "migration lock sentinel has no stdout".to_owned())?;
    let mut sentinel = CompanionLockSentinel {
        stdout: BufReader::new(stdout),
        child,
        finished: false,
    };
    let marker = sentinel.read_marker()?;
    if marker.contains(SENTINEL_ARMED) {
        Ok(sentinel)
    } else if marker.contains(SENTINEL_GAP) {
        Err("sentinel acquired the companion at source-guard acquisition".to_owned())
    } else {
        Err(format!("migration lock sentinel did not arm: {marker}"))
    }
}

fn dispatch_migration(path: &Path) -> i32 {
    let args = vec![
        "contextdb".to_owned(),
        "migrate".to_owned(),
        path.to_string_lossy().into_owned(),
    ];
    dispatch_if_subcommand(&args).expect("migrate is a production subcommand")
}

fn generated_migration_residue(path: &Path) -> Vec<PathBuf> {
    let source_name = path
        .file_name()
        .expect("migration source has a file name")
        .to_string_lossy();
    let generated_prefix = format!("{source_name}.migrate-");
    let mut residue = fs::read_dir(path.parent().expect("migration source has a parent"))
        .expect("read migration directory")
        .map(|entry| entry.expect("read migration directory entry").path())
        .filter(|candidate| {
            candidate
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(&generated_prefix))
        })
        .collect::<Vec<_>>();
    residue.sort();
    residue
}

fn migration_directory_entries(path: &Path) -> BTreeSet<PathBuf> {
    fs::read_dir(path.parent().expect("migration source has a parent"))
        .expect("read migration directory")
        .map(|entry| entry.expect("read migration directory entry").path())
        .collect()
}

fn discover_generated_migration_target(
    path: &Path,
    baseline: &BTreeSet<PathBuf>,
) -> std::result::Result<PathBuf, String> {
    let targets = generated_migration_residue(path)
        .into_iter()
        .filter(|candidate| !baseline.contains(candidate))
        .filter(|candidate| {
            candidate
                .extension()
                .is_some_and(|extension| extension == "tmp")
        })
        .filter(|candidate| {
            fs::symlink_metadata(candidate).is_ok_and(|metadata| metadata.file_type().is_file())
        })
        .filter(|candidate| {
            let companion = appended_companion(candidate);
            !baseline.contains(&companion)
                && fs::symlink_metadata(companion)
                    .is_ok_and(|metadata| metadata.file_type().is_file())
        })
        .collect::<Vec<_>>();
    match targets.as_slice() {
        [target] => Ok(target.clone()),
        _ => Err(format!(
            "expected one actual generated migration target, found {targets:?}"
        )),
    }
}

#[cfg(unix)]
fn publish_generated_target(path: &Path) {
    println!(
        "{SCENARIO_GENERATED_TARGET}{}",
        encode_hex(path.as_os_str().as_encoded_bytes())
    );
    std::io::stdout()
        .flush()
        .expect("flush generated migration target");
    let (device, inode) = file_identity(path);
    println!("{SCENARIO_GENERATED_TARGET_IDENTITY}{device}:{inode}");
    std::io::stdout()
        .flush()
        .expect("flush generated migration target identity");
}

#[cfg(unix)]
fn generated_target_from_output(output: &str) -> Option<PathBuf> {
    output.lines().find_map(|line| {
        let index = line.find(SCENARIO_GENERATED_TARGET)?;
        let bytes = decode_hex(&line[index + SCENARIO_GENERATED_TARGET.len()..]).ok()?;
        Some(PathBuf::from(OsString::from_vec(bytes)))
    })
}

#[cfg(unix)]
fn generated_target_identity_from_output(output: &str) -> Option<(u64, u64)> {
    output.lines().find_map(|line| {
        let index = line.find(SCENARIO_GENERATED_TARGET_IDENTITY)?;
        let mut fields = line[index + SCENARIO_GENERATED_TARGET_IDENTITY.len()..].split(':');
        Some((fields.next()?.parse().ok()?, fields.next()?.parse().ok()?))
    })
}

fn generated_target_fingerprint_from_output(output: &str) -> Option<[u8; 32]> {
    output.lines().find_map(|line| {
        let index = line.find(SCENARIO_GENERATED_TARGET_FINGERPRINT)?;
        decode_hex(&line[index + SCENARIO_GENERATED_TARGET_FINGERPRINT.len()..])
            .ok()?
            .try_into()
            .ok()
    })
}

#[derive(Debug)]
struct CheckpointedMigration {
    exit_code: i32,
    checkpoints: Vec<MigrationReplacementBoundary>,
    terminal_event: MigrationReplacementEvent,
}

fn run_checkpointed_migration(
    path: &Path,
    mut checkpoint: impl FnMut(MigrationReplacementBoundary) -> std::result::Result<(), String>,
) -> CheckpointedMigration {
    let _sequence = ArmedMigrationReplacementSequence::arm();
    let migration = MigrationTask::spawn(path.to_path_buf());
    let mut checkpoints = Vec::new();
    let mut coordination_errors = Vec::new();
    let mut terminal_event = None;
    for expected in MigrationReplacementBoundary::ALL {
        match next_migration_replacement_event_for_test() {
            MigrationReplacementEvent::Checkpoint(actual) => {
                if actual != expected {
                    coordination_errors.push(format!(
                        "expected checkpoint {expected:?}, reached {actual:?}"
                    ));
                }
                if let Err(error) = checkpoint(actual) {
                    coordination_errors.push(format!("{actual:?}: {error}"));
                }
                release_migration_replacement_checkpoint_for_test();
                checkpoints.push(actual);
            }
            event => {
                terminal_event = Some(event);
                break;
            }
        }
    }
    if terminal_event.is_none() {
        terminal_event = Some(next_migration_replacement_event_for_test());
    }
    let exit_code = migration
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(
        coordination_errors.is_empty(),
        "migration checkpoint coordination failed: {}",
        coordination_errors.join("; ")
    );
    CheckpointedMigration {
        exit_code,
        checkpoints,
        terminal_event: terminal_event.expect("migration has a terminal event"),
    }
}

fn assert_migration_completed(run: &CheckpointedMigration) {
    assert_eq!(run.exit_code, EXIT_OK, "production migration must succeed");
    assert_eq!(run.checkpoints, MigrationReplacementBoundary::ALL);
    assert_eq!(
        run.terminal_event,
        MigrationReplacementEvent::CompletedAfterGuardRelease
    );
}

fn assert_primary_fixture_hydrated(path: &Path) {
    let db = Database::open(path).unwrap_or_else(|error| {
        panic!("open migrated primary fixture {}: {error}", path.display())
    });
    let mut names = db
        .execute("SELECT name FROM entities", &Default::default())
        .expect("read migrated primary entities")
        .rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [Value::Text(name)] => name.clone(),
            other => panic!("unexpected migrated entity row: {other:?}"),
        })
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, ["gadget-service", "widget-service"]);
    db.close().expect("close migrated primary fixture");
}

fn assert_keyless_fixture_witness_hydrated(path: &Path, witness: &str) {
    let db = Database::open(path).unwrap_or_else(|error| {
        panic!("open migrated witness fixture {}: {error}", path.display())
    });
    let mut payloads = db
        .execute("SELECT payload FROM events", &Default::default())
        .expect("read migrated witnessed keyless rows")
        .rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [Value::Text(payload)] => payload.clone(),
            other => panic!("unexpected migrated keyless witness row: {other:?}"),
        })
        .collect::<Vec<_>>();
    payloads.sort();
    let mut expected = vec!["event-2".to_owned(), witness.to_owned()];
    expected.sort();
    assert_eq!(payloads, expected);
    db.close().expect("close migrated keyless witness fixture");
}

#[cfg(unix)]
fn assert_inode_has_no_path(directory: &Path, identity: (u64, u64)) {
    for entry in fs::read_dir(directory).expect("read source witness directory") {
        let path = entry.expect("read source witness entry").path();
        let Ok(metadata) = fs::metadata(&path) else {
            continue;
        };
        assert_ne!(
            (metadata.dev(), metadata.ino()),
            identity,
            "locked source inode remained pathname-reachable at {}",
            path.display()
        );
    }
}

fn migration_boundary_from_wire(value: &str) -> MigrationReplacementBoundary {
    match value {
        "SourceGuardAcquired" => MigrationReplacementBoundary::SourceGuardAcquired,
        "TemporaryStoreOpened" => MigrationReplacementBoundary::TemporaryStoreOpened,
        "TemporaryStoreImported" => MigrationReplacementBoundary::TemporaryStoreImported,
        "SourceStoreSealed" => MigrationReplacementBoundary::SourceStoreSealed,
        "TemporaryStoreBuilt" => MigrationReplacementBoundary::TemporaryStoreBuilt,
        "TemporaryStoreDurablyPreparedAndOwned" => {
            MigrationReplacementBoundary::TemporaryStoreDurablyPreparedAndOwned
        }
        "BeforeAtomicSwap" => MigrationReplacementBoundary::BeforeAtomicSwap,
        "AfterAtomicSwap" => MigrationReplacementBoundary::AfterAtomicSwap,
        "TemporaryCompanionCleaned" => MigrationReplacementBoundary::TemporaryCompanionCleaned,
        "BeforeFinalGuardRelease" => MigrationReplacementBoundary::BeforeFinalGuardRelease,
        other => panic!("unknown migration replacement boundary: {other}"),
    }
}

#[test]
#[cfg(unix)]
fn migration_failure_scenario_child() {
    let Some(role) = std::env::var(CHILD_ROLE_ENV).ok() else {
        return;
    };
    if !matches!(
        role.as_str(),
        "final-rename-failure" | "temporary-companion-cleanup-failure" | "replacement-crash"
    ) {
        return;
    }
    let path = child_store_path();
    let baseline = migration_directory_entries(&path);
    let mut generated_target = None;
    let mut generated_identity = None;
    match role.as_str() {
        "final-rename-failure" => {
            let fault = arm_final_rename_failure_for_test();
            let run = run_checkpointed_migration(&path, |boundary| {
                if boundary == MigrationReplacementBoundary::TemporaryStoreOpened {
                    let target = discover_generated_migration_target(&path, &baseline)?;
                    publish_generated_target(&target);
                    generated_target = Some(target);
                }
                Ok(())
            });
            fault.verify_exact_injection_for_test(
                generated_target
                    .as_deref()
                    .expect("generated migration target was observed"),
                &path,
            );
            assert_eq!(run.exit_code, EXIT_ERROR);
            assert_eq!(
                run.terminal_event,
                MigrationReplacementEvent::FinishedBeforeExpectedCheckpoint {
                    expected: MigrationReplacementBoundary::AfterAtomicSwap,
                }
            );
            println!("{SCENARIO_OPERATION_EXIT}{}", run.exit_code);
        }
        "temporary-companion-cleanup-failure" => {
            let fault = arm_temporary_companion_cleanup_failure_for_test();
            let run = run_checkpointed_migration(&path, |boundary| {
                if boundary == MigrationReplacementBoundary::TemporaryStoreOpened {
                    let target = discover_generated_migration_target(&path, &baseline)?;
                    publish_generated_target(&target);
                    generated_target = Some(target);
                }
                Ok(())
            });
            let generated_companion = appended_companion(
                generated_target
                    .as_deref()
                    .expect("generated migration target was observed"),
            );
            let observation = fault.verify_exact_retry_for_test(&generated_companion);
            assert_eq!(observation.exact_path, generated_companion);
            assert_eq!(observation.attempted_removals, 2);
            assert_eq!(observation.injected_failures, 1);
            assert_eq!(observation.real_removals, 1);
            assert_eq!(observation.successful_real_removals, 1);
            assert_migration_completed(&run);
            println!("{SCENARIO_OPERATION_EXIT}{}", run.exit_code);
        }
        "replacement-crash" => {
            let crash_boundary = migration_boundary_from_wire(
                &std::env::var(SCENARIO_BOUNDARY_ENV)
                    .expect("migration crash child receives a replacement boundary"),
            );
            let _ = run_checkpointed_migration(&path, |boundary| {
                if boundary == MigrationReplacementBoundary::TemporaryStoreOpened {
                    let target = discover_generated_migration_target(&path, &baseline)?;
                    publish_generated_target(&target);
                    generated_identity = Some(file_identity(&target));
                    generated_target = Some(target);
                }
                if boundary == crash_boundary {
                    if let (Some(target), Some(identity)) =
                        (generated_target.as_deref(), generated_identity)
                    {
                        let fingerprint_path = if fs::metadata(target)
                            .is_ok_and(|metadata| (metadata.dev(), metadata.ino()) == identity)
                        {
                            target
                        } else {
                            let active_identity = file_identity(&path);
                            assert_eq!(
                                active_identity, identity,
                                "generated target inode is at neither its temporary nor active pathname"
                            );
                            path.as_path()
                        };
                        let fingerprint = store_fingerprint_for_test(fingerprint_path)
                            .expect("fingerprint exact generated target before crash");
                        println!(
                            "{SCENARIO_GENERATED_TARGET_FINGERPRINT}{}",
                            encode_hex(&fingerprint)
                        );
                    }
                    println!("{SCENARIO_CRASH_BOUNDARY}{boundary:?}");
                    std::io::stdout()
                        .flush()
                        .expect("flush migration replacement crash boundary");
                    std::process::abort();
                }
                Ok(())
            });
            panic!("migration crash child passed its armed boundary");
        }
        _ => unreachable!(),
    }
    std::io::stdout()
        .flush()
        .expect("flush migration failure scenario result");
}

#[cfg(unix)]
fn run_migration_scenario_child(
    role: &str,
    path: &Path,
    boundary: Option<MigrationReplacementBoundary>,
) -> Output {
    let mut command =
        Command::new(std::env::current_exe().expect("current integration-test binary"));
    command
        .arg("--exact")
        .arg("migration_failure_scenario_child")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, role)
        .env(CHILD_PATH_ENV, path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(boundary) = boundary {
        command.env(SCENARIO_BOUNDARY_ENV, format!("{boundary:?}"));
    }
    command
        .output()
        .unwrap_or_else(|error| panic!("run migration scenario {role}: {error}"))
}

fn scenario_output_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

struct MigrationFinished;

impl Drop for MigrationFinished {
    fn drop(&mut self) {
        finish_migration_replacement_sequence_for_test();
    }
}

#[test]
#[cfg(unix)]
fn migration_retains_one_appended_guard_through_every_replacement_boundary() {
    let _serial = serialise_migration_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("legacy.db");
    fs::copy(legacy_fixture(), &path).expect("stage genuine legacy fixture");
    let companion = appended_companion(&path);
    let old_style = pre_change_replaced_companion(&path);
    if old_style != companion {
        fs::write(&old_style, b"stale extension-replaced companion")
            .expect("seed stale extension-replaced companion");
    }

    let source = Database::open_legacy_for_migration(&path)
        .expect("open genuine legacy source through production migration loader");
    source.close().expect("close legacy migration source");
    let companion_before = file_identity(&companion);
    let old_style_bytes = (old_style != companion)
        .then(|| fs::read(&old_style).expect("read stale extension-replaced companion"));

    let _sequence = ArmedMigrationReplacementSequence::arm();
    let migration = MigrationTask::spawn(path.clone());

    let mut coordination_error = None;
    let mut checkpoints = Vec::new();
    let mut lock_sentinel = None;
    for (index, expected) in MigrationReplacementBoundary::ALL.into_iter().enumerate() {
        match next_migration_replacement_event_for_test() {
            MigrationReplacementEvent::Checkpoint(actual) => {
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
                                "{actual:?}: migration lock sentinel did not arm: {error}"
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
                if let Err(error) = commanded_checkpoint_writer(&path)
                    && coordination_error.is_none()
                {
                    coordination_error = Some(format!(
                        "{actual:?}: checkpointed production writer probe failed: {error}"
                    ));
                }
                if index + 1 == MigrationReplacementBoundary::ALL.len()
                    && let Some(sentinel) = lock_sentinel.as_mut()
                    && let Err(error) = sentinel.permit_final_release()
                    && coordination_error.is_none()
                {
                    coordination_error = Some(format!(
                        "final migration guard checkpoint did not safely permit release: {error}"
                    ));
                }
                release_migration_replacement_checkpoint_for_test();
                checkpoints.push(actual);
            }
            event => {
                coordination_error = Some(format!(
                    "migration ended before {expected:?}; coordination event: {event:?}"
                ));
                cancel_migration_replacement_sequence_for_test();
                break;
            }
        }
    }
    if coordination_error.is_none() {
        let event = next_migration_replacement_event_for_test();
        if event != MigrationReplacementEvent::CompletedAfterGuardRelease {
            coordination_error = Some(format!("unexpected completion event: {event:?}"));
            cancel_migration_replacement_sequence_for_test();
        }
    }

    let migration_result = migration.join();
    let completion_acquisition = lock_sentinel
        .map(CompanionLockSentinel::complete)
        .unwrap_or_else(|| Err("migration lock sentinel never armed".to_owned()));

    assert!(
        coordination_error.is_none(),
        "{}",
        coordination_error.unwrap_or_default()
    );
    assert_eq!(
        checkpoints.as_slice(),
        &MigrationReplacementBoundary::ALL,
        "every replacement checkpoint must be exercised in one migration"
    );
    assert_eq!(
        migration_result.unwrap_or_else(|panic| std::panic::resume_unwind(panic)),
        EXIT_OK,
        "production migration must complete after all checkpoints are released"
    );
    assert_eq!(
        completion_acquisition,
        Ok(()),
        "the contender may acquire only after migration reports completion and releases the guard"
    );
    assert_eq!(
        file_identity(&companion),
        companion_before,
        "migration must preserve the original appended companion inode"
    );
    if let Some(old_style_bytes) = old_style_bytes {
        assert_eq!(
            fs::read(&old_style).expect("read stale old-style companion after migration"),
            old_style_bytes,
            "migration must ignore the stale extension-replaced companion"
        );
    }

    assert!(
        generated_migration_residue(&path).is_empty(),
        "completed migration left generated residue: {:?}",
        generated_migration_residue(&path)
    );
    let migrated_store = Database::open(&path).expect("open migrated result");
    migrated_store.close().expect("close migrated result");
}

#[test]
#[cfg(unix)]
fn generated_temporary_target_remains_exclusively_owned_until_its_exact_inode_is_renamed() {
    let _serial = serialise_migration_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("generated-target.db");
    fs::copy(legacy_fixture(), &path).expect("stage genuine legacy fixture");
    let source = Database::open_legacy_for_migration(&path)
        .expect("create the source companion through the production migration opener");
    source.close().expect("close source seeding opener");
    let baseline = migration_directory_entries(&path);

    let mut generated_target = None;
    let mut generated_identity = None;
    let mut generated_companion_identity = None;
    let mut renamed_identity = None;
    let mut ownership_probes = Vec::new();
    let run = run_checkpointed_migration(&path, |boundary| {
        if boundary == MigrationReplacementBoundary::TemporaryStoreOpened {
            let target = discover_generated_migration_target(&path, &baseline)?;
            let metadata = fs::metadata(&target)
                .map_err(|error| format!("stat generated migration target: {error}"))?;
            generated_identity = Some((metadata.dev(), metadata.ino()));
            let companion_metadata = fs::metadata(appended_companion(&target))
                .map_err(|error| format!("stat generated migration companion: {error}"))?;
            generated_companion_identity =
                Some((companion_metadata.dev(), companion_metadata.ino()));
            generated_target = Some(target);
        }
        if matches!(
            boundary,
            MigrationReplacementBoundary::TemporaryStoreDurablyPreparedAndOwned
                | MigrationReplacementBoundary::BeforeAtomicSwap
        ) {
            let target = generated_target
                .as_deref()
                .ok_or_else(|| "generated target was not discovered".to_owned())?;
            let target_metadata = fs::metadata(target)
                .map_err(|error| format!("restat generated migration target: {error}"))?;
            let companion_metadata = fs::metadata(appended_companion(target))
                .map_err(|error| format!("restat generated migration companion: {error}"))?;
            if Some((target_metadata.dev(), target_metadata.ino())) != generated_identity {
                return Err("generated target pathname changed inodes before rename".to_owned());
            }
            if Some((companion_metadata.dev(), companion_metadata.ino()))
                != generated_companion_identity
            {
                return Err(
                    "generated target companion pathname changed inodes before rename".to_owned(),
                );
            }
            ownership_probes.push((
                boundary,
                commanded_raw_redb_target_writer(target),
                commanded_target_companion_lock(target),
                commanded_target_mutator(target),
            ));
        }
        if boundary == MigrationReplacementBoundary::AfterAtomicSwap {
            let metadata = fs::metadata(&path)
                .map_err(|error| format!("stat atomically swapped migration target: {error}"))?;
            renamed_identity = Some((metadata.dev(), metadata.ino()));
        }
        Ok(())
    });

    assert_eq!(ownership_probes.len(), 2);
    for (boundary, raw_redb, kernel_companion, contextdb_writer) in ownership_probes {
        assert_eq!(
            raw_redb,
            Ok(()),
            "{boundary:?}: the exact generated target inode lacks its independent raw-Redb owner"
        );
        assert_eq!(
            kernel_companion,
            Ok(()),
            "{boundary:?}: the exact generated companion inode lacks its kernel lock"
        );
        assert_eq!(
            contextdb_writer,
            Ok(()),
            "{boundary:?}: a real ContextDB writer must also be refused"
        );
    }
    assert_migration_completed(&run);
    assert_eq!(
        renamed_identity, generated_identity,
        "the final pathname must identify the exact generated target inode that stayed guarded"
    );
    let generated_target = generated_target.expect("generated target was observed");
    assert!(!generated_target.exists());
    assert!(!appended_companion(&generated_target).exists());

    let migrated = Database::open(&path).expect("open exact atomically renamed target");
    assert!(
        migrated
            .execute(
                &format!("SELECT * FROM {MIGRATION_TARGET_INTRUSION_TABLE}"),
                &Default::default(),
            )
            .is_err(),
        "the generated target contains a competing writer's mutation"
    );
    migrated.close().expect("close exact migrated target");
}

#[test]
#[cfg(unix)]
fn backup_and_hydration_use_the_unlinked_locked_source_generation_with_a_per_run_witness() {
    let _serial = serialise_migration_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("locked-source.db");
    let pathname_replacement = root.path().join("pathname-replacement.db");
    fs::copy(keyless_legacy_fixture(), &path).expect("stage locked legacy source");
    let witness = fresh_source_witness();
    replace_legacy_text_witness_for_test(&path, "events", "payload", "event-1", &witness)
        .expect("put a per-run witness in the real legacy row codec");
    let locked_source_bytes = fs::read(&path).expect("read locked source generation");
    assert!(
        locked_source_bytes
            .windows(witness.len())
            .any(|window| window == witness.as_bytes()),
        "the per-run witness must be independently visible in the exact source bytes"
    );
    let source_metadata = fs::metadata(&path).expect("stat witnessed source generation");
    assert_eq!(source_metadata.nlink(), 1);
    let source_identity = (source_metadata.dev(), source_metadata.ino());
    fs::copy(legacy_fixture(), &pathname_replacement)
        .expect("stage distinct pathname replacement generation");
    let replacement_bytes = fs::read(&pathname_replacement).expect("read replacement generation");
    assert_ne!(locked_source_bytes, replacement_bytes);
    assert!(
        !replacement_bytes
            .windows(witness.len())
            .any(|window| window == witness.as_bytes()),
        "the pathname replacement must not carry the locked source's witness"
    );

    reset_locked_migration_sources_for_test();
    let mut replaced = false;
    let run = run_checkpointed_migration(&path, |boundary| {
        if boundary == MigrationReplacementBoundary::SourceGuardAcquired {
            assert_locked_source_descriptor(
                source_identity,
                1,
                "before the source pathname is unlinked",
            );
            fs::remove_file(&path)
                .map_err(|error| format!("unlink exact locked source descriptor: {error}"))?;
            assert_locked_source_descriptor(
                source_identity,
                0,
                "after the source pathname is unlinked",
            );
            assert_inode_has_no_path(root.path(), source_identity);
            std::os::unix::fs::symlink(&pathname_replacement, &path)
                .map_err(|error| format!("replace source pathname with symlink: {error}"))?;
            assert_locked_source_descriptor(
                source_identity,
                0,
                "after the source pathname is replaced",
            );
            assert_inode_has_no_path(root.path(), source_identity);
            replaced = true;
        }
        Ok(())
    });

    assert!(replaced, "the source pathname race was not injected");
    assert_migration_completed(&run);
    let backup = PathBuf::from(format!("{}.bak", path.display()));
    assert_eq!(
        fs::read(&backup).expect("read migration backup"),
        locked_source_bytes,
        "backup bytes must come from the exact descriptor locked before pathname replacement"
    );
    assert!(
        fs::read(&backup)
            .expect("reread witnessed migration backup")
            .windows(witness.len())
            .any(|window| window == witness.as_bytes()),
        "the exact backup must visibly carry the locked source's witness"
    );
    assert_eq!(
        fs::read(&pathname_replacement).expect("read untouched replacement target"),
        replacement_bytes,
        "the symlink destination must never become migration input"
    );
    assert!(
        !fs::symlink_metadata(&path)
            .expect("inspect final migration pathname")
            .file_type()
            .is_symlink(),
        "the atomic swap must replace the raced symlink with the locked generation's migration"
    );
    assert_keyless_fixture_witness_hydrated(&path, &witness);
}

#[test]
#[cfg(unix)]
fn post_swap_temporary_companion_cleanup_failure_is_retried_without_generated_residue() {
    let _serial = serialise_migration_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("cleanup-failure.db");
    fs::copy(legacy_fixture(), &path).expect("stage genuine legacy fixture");

    let output = run_migration_scenario_child("temporary-companion-cleanup-failure", &path, None);
    let text = scenario_output_text(&output);
    assert!(
        output.status.success(),
        "cleanup-failure injector must finish its production migration: {text}"
    );
    assert!(text.contains(&format!("{SCENARIO_OPERATION_EXIT}{EXIT_OK}")));
    let generated_target = generated_target_from_output(&text)
        .expect("cleanup-failure scenario reports the actual generated target");
    assert!(
        !generated_target.exists(),
        "the generated store was swapped"
    );
    assert!(
        !appended_companion(&generated_target).exists(),
        "the one-shot failure left the actual generated temporary companion for an operator to clean: {}",
        appended_companion(&generated_target).display()
    );
    assert!(
        generated_migration_residue(&path).is_empty(),
        "successful migration left generated residue: {:?}",
        generated_migration_residue(&path)
    );
    assert!(!text.to_ascii_lowercase().contains("by hand"));
    assert!(!text.to_ascii_lowercase().contains("manually"));
    assert_primary_fixture_hydrated(&path);
}

#[test]
#[cfg(unix)]
fn final_rename_failure_keeps_the_original_usable_and_cleans_every_generated_artifact() {
    let _serial = serialise_migration_test();
    let root = tempfile::tempdir().expect("scratch directory");
    let path = root.path().join("rename-failure.db");
    fs::copy(legacy_fixture(), &path).expect("stage genuine legacy fixture");
    let source_bytes = fs::read(&path).expect("read pre-migration source bytes");
    let source =
        Database::open_legacy_for_migration(&path).expect("seed the original migration companion");
    source
        .close()
        .expect("close source companion seeding opener");
    let companion = appended_companion(&path);
    let companion_identity = file_identity(&companion);

    let output = run_migration_scenario_child("final-rename-failure", &path, None);
    let text = scenario_output_text(&output);
    assert!(
        output.status.success(),
        "final-rename fault injector must observe a handled migration refusal: {text}"
    );
    assert!(text.contains(&format!("{SCENARIO_OPERATION_EXIT}{EXIT_ERROR}")));
    let generated_target = generated_target_from_output(&text)
        .expect("rename-failure scenario reports the actual generated target");

    assert!(
        matches!(RedbPersistence::is_legacy_format_store(&path), Ok(true)),
        "the original complete legacy generation must remain at the requested pathname"
    );
    let backup = PathBuf::from(format!("{}.bak", path.display()));
    assert_eq!(
        fs::read(&backup).expect("read durable pre-migration backup"),
        source_bytes,
        "rename failure must retain the exact locked-source backup"
    );
    let recovered = Database::open_legacy_for_migration(&path)
        .expect("original legacy store remains usable without operator cleanup");
    recovered.close().expect("close recovered original source");
    assert_eq!(file_identity(&companion), companion_identity);
    assert!(!generated_target.exists());
    assert!(
        !appended_companion(&generated_target).exists(),
        "final rename failure leaked the generated companion {}",
        appended_companion(&generated_target).display()
    );
    assert!(
        generated_migration_residue(&path).is_empty(),
        "final rename failure leaked generated residue: {:?}",
        generated_migration_residue(&path)
    );
    let lower = text.to_ascii_lowercase();
    assert!(
        !lower.contains("by hand"),
        "manual recovery instruction: {text}"
    );
    assert!(
        !lower.contains("manually"),
        "manual cleanup instruction: {text}"
    );
}

#[derive(Debug)]
struct PreservedMigrationDecoys {
    files: BTreeMap<PathBuf, ((u64, u64), Vec<u8>)>,
    /// The complete, openable store whose pathname follows the exact
    /// generated grammar. Nothing durably records it, so recovery must leave
    /// it and its sidecars alone.
    complete_generated_shaped_store: PathBuf,
}

#[cfg(unix)]
fn seed_preserved_migration_decoys(path: &Path) -> PreservedMigrationDecoys {
    let mut obsolete_name = path.as_os_str().to_os_string();
    obsolete_name.push(".migrate-tmp");
    let obsolete = PathBuf::from(obsolete_name);
    let mut matching_name = path.as_os_str().to_os_string();
    matching_name.push(".migrate-decoy-not-a-generated-id.tmp");
    let matching = PathBuf::from(matching_name);
    let mut matching_sidecar_name = matching.as_os_str().to_os_string();
    matching_sidecar_name.push(".journal");
    // Same shape a live migration allocates -- `.migrate-<process>-
    // <sequence>.tmp` -- and a real current-format store behind it, so a
    // pathname-pattern or "looks generated" cleanup would take it.
    let mut complete_name = path.as_os_str().to_os_string();
    complete_name.push(".migrate-0000dead-000000000000002a.tmp");
    let complete = PathBuf::from(complete_name);
    let complete_store = Database::open(&complete).unwrap_or_else(|error| {
        panic!(
            "create complete generated-shaped decoy {}: {error}",
            complete.display()
        )
    });
    complete_store
        .execute(
            "CREATE TABLE decoy_rows (id INTEGER PRIMARY KEY, body TEXT)",
            &Default::default(),
        )
        .expect("declare the complete generated-shaped decoy table");
    complete_store
        .execute(
            "INSERT INTO decoy_rows (id, body) VALUES (1, 'preserved')",
            &Default::default(),
        )
        .expect("fill the complete generated-shaped decoy table");
    complete_store
        .close()
        .expect("close the complete generated-shaped decoy");
    let mut complete_sidecar_name = complete.as_os_str().to_os_string();
    complete_sidecar_name.push(".journal");
    let complete_sidecar = PathBuf::from(complete_sidecar_name);
    fs::write(
        &complete_sidecar,
        b"complete generated-shaped sidecar decoy",
    )
    .expect("seed the complete generated-shaped decoy sidecar");
    let paths = [
        (
            obsolete.clone(),
            b"obsolete fixed migration target decoy".as_slice(),
        ),
        (
            appended_companion(&obsolete),
            b"obsolete fixed migration companion decoy".as_slice(),
        ),
        (
            matching.clone(),
            b"matching generated target decoy".as_slice(),
        ),
        (
            appended_companion(&matching),
            b"matching generated companion decoy".as_slice(),
        ),
        (
            PathBuf::from(matching_sidecar_name),
            b"matching generated sidecar decoy".as_slice(),
        ),
    ];
    let mut files = BTreeMap::new();
    for (decoy, bytes) in paths {
        fs::write(&decoy, bytes)
            .unwrap_or_else(|error| panic!("seed migration decoy {}: {error}", decoy.display()));
        files.insert(decoy.clone(), (file_identity(&decoy), bytes.to_vec()));
    }
    for produced in [
        complete.clone(),
        appended_companion(&complete),
        complete_sidecar,
    ] {
        let bytes = fs::read(&produced)
            .unwrap_or_else(|error| panic!("read seeded decoy {}: {error}", produced.display()));
        files.insert(produced.clone(), (file_identity(&produced), bytes));
    }
    PreservedMigrationDecoys {
        files,
        complete_generated_shaped_store: complete,
    }
}

#[cfg(unix)]
fn assert_migration_decoys_preserved(decoys: &PreservedMigrationDecoys) {
    for (path, (identity, bytes)) in &decoys.files {
        assert_eq!(
            file_identity(path),
            *identity,
            "migration recovery replaced decoy {}",
            path.display()
        );
        assert_eq!(
            fs::read(path).unwrap_or_else(|error| panic!("read decoy {}: {error}", path.display())),
            *bytes,
            "migration recovery changed decoy {}",
            path.display()
        );
    }
}

/// The published companion layout: a fixed magic, two fixed-size checksummed
/// slots, then the active-slot selector. The proof parses the recorded
/// replacement intent straight out of those bytes instead of asking the same
/// engine reader recovery itself uses, so the intent is independent evidence.
#[cfg(unix)]
const COMPANION_FILE_MAGIC: &[u8; 16] = b"CTXDB-COMPANION1";
#[cfg(unix)]
const COMPANION_SLOT_PAYLOAD_CAPACITY: usize = 4 * 1024;
#[cfg(unix)]
const COMPANION_SLOT_BYTES: usize = 4 + COMPANION_SLOT_PAYLOAD_CAPACITY + 32;
#[cfg(unix)]
const COMPANION_FIRST_SLOT_OFFSET: usize = COMPANION_FILE_MAGIC.len();
#[cfg(unix)]
const COMPANION_PENDING_MAGIC: &[u8] = b"contextdb-replacement-v1";

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct DurableReplacementIntent {
    slot: usize,
    store_fingerprint: [u8; 32],
    intended_record: CompanionRecordObservation,
}

#[cfg(unix)]
fn parse_durable_replacement_intent(
    slot: usize,
    bytes: &[u8],
) -> std::result::Result<Option<DurableReplacementIntent>, String> {
    if bytes.len() != COMPANION_SLOT_BYTES {
        return Err(format!(
            "companion slot has {} bytes, expected {COMPANION_SLOT_BYTES}",
            bytes.len()
        ));
    }
    let length = u32::from_le_bytes(
        bytes[..4]
            .try_into()
            .map_err(|_| "companion slot length is truncated".to_owned())?,
    ) as usize;
    if length == 0 {
        return Ok(None);
    }
    if length > COMPANION_SLOT_PAYLOAD_CAPACITY {
        return Err(format!(
            "companion slot payload length {length} exceeds its capacity"
        ));
    }
    let payload = &bytes[4..4 + length];
    if !payload.starts_with(COMPANION_PENDING_MAGIC) {
        return Ok(None);
    }
    let mut offset = COMPANION_PENDING_MAGIC.len();
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
    Ok(Some(DurableReplacementIntent {
        slot,
        store_fingerprint,
        intended_record,
    }))
}

#[cfg(unix)]
fn parse_durable_companion_record(
    bytes: &[u8],
) -> std::result::Result<Option<CompanionRecordObservation>, String> {
    if bytes.len() != COMPANION_SLOT_BYTES {
        return Err(format!(
            "companion slot has {} bytes, expected {COMPANION_SLOT_BYTES}",
            bytes.len()
        ));
    }
    let length = u32::from_le_bytes(
        bytes[..4]
            .try_into()
            .map_err(|_| "companion slot length is truncated".to_owned())?,
    ) as usize;
    if length == 0 || length > COMPANION_SLOT_PAYLOAD_CAPACITY {
        return Ok(None);
    }
    let payload = &bytes[4..4 + length];
    if payload.starts_with(COMPANION_PENDING_MAGIC) {
        return Ok(None);
    }
    let checksum: [u8; 32] = bytes[4 + COMPANION_SLOT_PAYLOAD_CAPACITY..]
        .try_into()
        .map_err(|_| "companion slot checksum is truncated".to_owned())?;
    decode_companion_record_for_test(payload, checksum).map(Some)
}

#[cfg(unix)]
fn durable_replacement_intent(companion: &Path) -> Option<DurableReplacementIntent> {
    let bytes = fs::read(companion)
        .unwrap_or_else(|error| panic!("read durable companion {}: {error}", companion.display()));
    assert!(
        bytes.len() >= COMPANION_FIRST_SLOT_OFFSET + 2 * COMPANION_SLOT_BYTES,
        "durable companion {} is shorter than its two record slots",
        companion.display()
    );
    assert_eq!(
        &bytes[..COMPANION_FILE_MAGIC.len()],
        COMPANION_FILE_MAGIC.as_slice(),
        "durable companion {} does not carry the companion magic",
        companion.display()
    );
    let mut intents = (0..2)
        .filter_map(|slot| {
            let offset = COMPANION_FIRST_SLOT_OFFSET + slot * COMPANION_SLOT_BYTES;
            parse_durable_replacement_intent(slot, &bytes[offset..offset + COMPANION_SLOT_BYTES])
                .unwrap_or_else(|error| {
                    panic!(
                        "parse slot {slot} of durable companion {}: {error}",
                        companion.display()
                    )
                })
        })
        .collect::<Vec<_>>();
    assert!(
        intents.len() <= 1,
        "the durable companion records more than one replacement intent: {intents:?}"
    );
    let intent = intents.pop()?;
    // A recorded intent never costs the reader its complete generation: the
    // other slot still decodes as one whole checksummed record.
    let sibling_offset = COMPANION_FIRST_SLOT_OFFSET + (1 - intent.slot) * COMPANION_SLOT_BYTES;
    let sibling = parse_durable_companion_record(
        &bytes[sibling_offset..sibling_offset + COMPANION_SLOT_BYTES],
    )
    .unwrap_or_else(|error| {
        panic!(
            "parse the slot beside the recorded intent in {}: {error}",
            companion.display()
        )
    });
    assert!(
        sibling.is_some(),
        "the durable companion recorded a replacement intent without keeping a complete record beside it"
    );
    Some(intent)
}

/// Migration records its exact-target intent in the original companion
/// BEFORE it creates the target inode, and keeps that record naming the
/// exact target as the target's contents change, so no boundary ever has a
/// generated target that nothing durable names. Recovery therefore always
/// has a recorded inode to remove and never has to guess a sibling
/// pathname. Every boundary from the target's creation until the source
/// guard is released must carry the record durably.
#[cfg(unix)]
fn boundary_requires_recorded_replacement_intent(boundary: MigrationReplacementBoundary) -> bool {
    matches!(
        boundary,
        MigrationReplacementBoundary::TemporaryStoreOpened
            | MigrationReplacementBoundary::TemporaryStoreImported
            | MigrationReplacementBoundary::SourceStoreSealed
            | MigrationReplacementBoundary::TemporaryStoreBuilt
            | MigrationReplacementBoundary::TemporaryStoreDurablyPreparedAndOwned
            | MigrationReplacementBoundary::BeforeAtomicSwap
            | MigrationReplacementBoundary::AfterAtomicSwap
            | MigrationReplacementBoundary::TemporaryCompanionCleaned
            | MigrationReplacementBoundary::BeforeFinalGuardRelease
    )
}

#[cfg(unix)]
fn independently_locate_generated_inode(
    path: &Path,
    baseline: &BTreeSet<PathBuf>,
    generated_identity: (u64, u64),
) -> PathBuf {
    let mut candidates = migration_directory_entries(path)
        .into_iter()
        .filter(|candidate| candidate == path || !baseline.contains(candidate))
        .filter(|candidate| {
            fs::metadata(candidate).is_ok_and(|metadata| {
                metadata.file_type().is_file()
                    && (metadata.dev(), metadata.ino()) == generated_identity
            })
        })
        .collect::<Vec<_>>();
    candidates.sort();
    assert_eq!(
        candidates.len(),
        1,
        "the generated inode must have one independently discoverable active or temporary path: {candidates:?}"
    );
    candidates.pop().expect("one generated inode candidate")
}

#[cfg(unix)]
fn recover_migration_after_crash(
    path: &Path,
    prior_record: &CompanionRecordObservation,
    prior_store_identity: (u64, u64),
    pre_recovery: &contextdb_engine::persistence::read_persistence_test_scaffold::ReplacementIntentObservation,
    generated_target_identity: Option<(u64, u64)>,
    generated_target_fingerprint: Option<[u8; 32]>,
) {
    let legacy = RedbPersistence::is_legacy_format_store(path)
        .unwrap_or_else(|error| panic!("classify crashed migration root: {error}"));
    let active_store_identity = file_identity(path);
    let expected_record = if active_store_identity == prior_store_identity {
        assert!(legacy, "the exact old store inode must decode as legacy");
        let active = pre_recovery
            .active_record
            .as_ref()
            .expect("old generation retains an exact active companion record");
        assert_eq!(
            active.fields.database_identity, prior_record.fields.database_identity,
            "the exact old store fingerprint is not paired with the prior database identity"
        );
        active
    } else {
        assert_eq!(
            Some(active_store_identity),
            generated_target_identity,
            "the active store inode is neither the exact old nor the exact generated generation"
        );
        let generated_target_fingerprint = generated_target_fingerprint
            .expect("a non-old store must match the observed generated-target intent");
        assert_eq!(
            pre_recovery.current_store_fingerprint, generated_target_fingerprint,
            "the active store matches neither the exact old generation nor generated-target intent"
        );
        assert!(
            !legacy,
            "the exact generated target must decode as current format"
        );
        if let Some(pending) = pre_recovery.pending_intended_record.as_ref() {
            assert_eq!(
                pre_recovery.pending_store_fingerprint,
                Some(generated_target_fingerprint),
                "the pending record does not name the exact generated target fingerprint"
            );
            pending
        } else {
            pre_recovery
                .active_record
                .as_ref()
                .expect("published generated target retains its exact active record")
        }
    };
    if legacy {
        let source = Database::open_legacy_for_migration(path)
            .unwrap_or_else(|error| panic!("recover old complete migration generation: {error}"));
        source
            .close()
            .expect("close recovered old migration generation");
    } else {
        assert_primary_fixture_hydrated(path);
    }
    let recovered = inspect_companion_record_for_test(path)
        .expect("inspect companion after migration crash recovery");
    assert_eq!(
        recovered.fields.database_identity, expected_record.fields.database_identity,
        "recovery did not retain the database identity bound to the exact selected store fingerprint"
    );
}

#[test]
#[cfg(unix)]
fn every_migration_replacement_crash_recovers_one_complete_generation_without_residue() {
    let _serial = serialise_migration_test();
    let root = tempfile::tempdir().expect("scratch directory");
    for boundary in MigrationReplacementBoundary::ALL {
        let case_directory = root.path().join(format!("crash-{boundary:?}"));
        fs::create_dir(&case_directory).expect("create migration crash case directory");
        let path = case_directory.join("store.db");
        fs::copy(legacy_fixture(), &path).expect("stage genuine legacy crash fixture");
        let source_bytes = fs::read(&path).expect("read crash fixture source bytes");
        let source =
            Database::open_legacy_for_migration(&path).expect("seed migration crash companion");
        source
            .close()
            .expect("close crash companion seeding opener");
        let companion = appended_companion(&path);
        let companion_identity = file_identity(&companion);
        let prior_store_identity = file_identity(&path);
        let prior_record = inspect_companion_record_for_test(&path)
            .expect("inspect pre-crash migration companion");
        let decoys = seed_preserved_migration_decoys(&path);
        let preserved_decoy_fingerprint =
            store_fingerprint_for_test(&decoys.complete_generated_shaped_store).unwrap_or_else(
                |error| panic!("{boundary:?}: fingerprint the complete preserved decoy: {error}"),
            );
        let baseline = migration_directory_entries(&path);

        let output = run_migration_scenario_child("replacement-crash", &path, Some(boundary));
        let text = scenario_output_text(&output);
        assert!(
            !output.status.success()
                && text.contains(&format!("{SCENARIO_CRASH_BOUNDARY}{boundary:?}")),
            "{boundary:?}: migration child must abort at the real replacement boundary: {text}"
        );
        let generated_target = generated_target_from_output(&text);
        let generated_identity = generated_target_identity_from_output(&text);
        let generated_fingerprint = generated_target_fingerprint_from_output(&text);
        let independently_identified_target = match (generated_target.as_ref(), generated_identity)
        {
            (Some(reported), Some(identity)) => {
                let identified = independently_locate_generated_inode(&path, &baseline, identity);
                if reported.exists() {
                    assert_eq!(
                        &identified, reported,
                        "{boundary:?}: child-reported target does not match the independently discovered inode"
                    );
                } else {
                    assert_eq!(
                        identified, path,
                        "{boundary:?}: renamed generated inode is not at the active pathname"
                    );
                }
                let fingerprint = store_fingerprint_for_test(&identified).unwrap_or_else(|error| {
                    panic!("{boundary:?}: fingerprint independently identified target: {error}")
                });
                assert_eq!(
                    Some(fingerprint),
                    generated_fingerprint,
                    "{boundary:?}: child target intent does not match the independently fingerprinted inode"
                );
                Some(identified)
            }
            (None, None) => {
                assert_eq!(boundary, MigrationReplacementBoundary::SourceGuardAcquired);
                assert!(generated_fingerprint.is_none());
                None
            }
            values => panic!("{boundary:?}: incomplete generated-target evidence: {values:?}"),
        };
        let pre_recovery = inspect_replacement_intent_for_test(&path).unwrap_or_else(|error| {
            panic!("{boundary:?}: capture exact pre-recovery intent: {error}")
        });
        assert!(
            pre_recovery.active_record.is_some(),
            "{boundary:?}: exact active companion record was not captured before recovery"
        );
        assert_eq!(
            pre_recovery.current_store_fingerprint,
            store_fingerprint_for_test(&path)
                .unwrap_or_else(|error| panic!("{boundary:?}: recapture active store: {error}")),
            "{boundary:?}: pre-recovery store fingerprint was not stable"
        );
        // Parsed straight out of the durable companion bytes at every
        // boundary, so what recovery is entitled to remove never rests on the
        // engine reader recovery itself uses.
        let recorded_intent = durable_replacement_intent(&companion);
        assert_eq!(
            recorded_intent
                .as_ref()
                .map(|intent| intent.store_fingerprint),
            pre_recovery.pending_store_fingerprint,
            "{boundary:?}: durable companion bytes and the engine reader disagree on the recorded fingerprint"
        );
        assert_eq!(
            recorded_intent
                .as_ref()
                .map(|intent| intent.intended_record.clone()),
            pre_recovery.pending_intended_record,
            "{boundary:?}: durable companion bytes and the engine reader disagree on the recorded record"
        );
        // A generated target exists at exactly the boundaries that must carry
        // a durable record of it; the removal and residue assertions below
        // are the same shape, so neither side can drift into demanding a
        // removal that nothing recorded.
        assert_eq!(
            boundary_requires_recorded_replacement_intent(boundary),
            generated_fingerprint.is_some(),
            "{boundary:?}: the recorded-intent schedule and the observed target disagree"
        );
        if boundary_requires_recorded_replacement_intent(boundary) {
            let intent = recorded_intent.as_ref().unwrap_or_else(|| {
                panic!("{boundary:?}: the generated target has no durable replacement intent")
            });
            assert_eq!(
                Some(intent.store_fingerprint),
                generated_fingerprint,
                "{boundary:?}: the recorded intent does not name the exact generated inode"
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
                "{boundary:?}: the recorded intent reuses the replaced generation's database identity"
            );
            assert!(
                intent.intended_record.fields.generation > prior_record.fields.generation,
                "{boundary:?}: the recorded intent does not advance the companion generation"
            );
        }
        recover_migration_after_crash(
            &path,
            &prior_record,
            prior_store_identity,
            &pre_recovery,
            generated_identity,
            generated_fingerprint,
        );
        assert_eq!(file_identity(&companion), companion_identity);
        if boundary != MigrationReplacementBoundary::SourceGuardAcquired {
            let backup = PathBuf::from(format!("{}.bak", path.display()));
            assert_eq!(
                fs::read(&backup)
                    .unwrap_or_else(|error| panic!("{boundary:?}: read durable backup: {error}")),
                source_bytes,
                "{boundary:?}: crash backup differs from the locked source generation"
            );
        }
        if let Some(target) = generated_target {
            assert!(
                !target.exists(),
                "{boundary:?}: generated store survived recovery"
            );
            assert!(
                !appended_companion(&target).exists(),
                "{boundary:?}: generated companion survived recovery"
            );
        }
        if let Some(identified) = independently_identified_target
            && identified != path
        {
            assert!(
                !identified.exists(),
                "{boundary:?}: independently identified generated target survived recovery"
            );
        }
        assert_migration_decoys_preserved(&decoys);
        let unexpected_owned_residue = generated_migration_residue(&path)
            .into_iter()
            .filter(|candidate| !baseline.contains(candidate))
            .collect::<Vec<_>>();
        assert!(
            unexpected_owned_residue.is_empty(),
            "{boundary:?}: generated residue outside the preserved baseline remains: {unexpected_owned_residue:?}"
        );
    }
}
