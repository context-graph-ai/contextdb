//! Hosts opaque requests without taking ownership of the store's write decisions.
use crate::support;
use contextdb::{
    DatabaseOpenOptions, Error, OwnerReadCancellation, OwnerRequestHandler, ReadLimits,
    request_owner,
};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
pub const NAMESPACE: &str = "opaque";
pub const PAYLOAD: &[u8] = &[0, 255, 128, b'\n', 0, 1];
#[derive(Default)]
struct Handler {
    db: std::sync::Weak<contextdb::Database>,
    threads: Mutex<BTreeSet<String>>,
    released: Arc<(Mutex<bool>, Condvar)>,
    cancel: Mutex<Option<OwnerReadCancellation>>,
}
impl OwnerRequestHandler for Handler {
    fn handle(
        &self,
        namespace: &str,
        bytes: &[u8],
        cancel: &OwnerReadCancellation,
    ) -> contextdb::Result<Vec<u8>> {
        assert_eq!(namespace, NAMESPACE);
        *self.cancel.lock().unwrap() = Some(cancel.clone());
        self.threads
            .lock()
            .unwrap()
            .insert(format!("{:?}", std::thread::current().id()));
        match bytes {
            b"refuse" => Err(Error::PlanError("the host refused this request".into())),
            b"answer" => Ok(vec![255; 64]),
            b"huge" => Ok(vec![0; 512]),
            b"unicode" => Err(Error::PlanError("é".repeat(u16::MAX as usize))),
            b"hold" | b"closing" | b"commit_hold" => {
                if bytes == b"commit_hold" {
                    let db = self.db.upgrade().unwrap();
                    support::run(&*db, "CREATE TABLE committed (id INT PRIMARY KEY)");
                    support::run(&*db, "INSERT INTO committed VALUES (1)");
                    *self.released.0.lock().unwrap() = false;
                }
                let wake = self.released.clone();
                let _listener = cancel.tell_on_cancel(move || {
                    let (lock, ready) = &*wake;
                    *lock.lock().unwrap() = true;
                    ready.notify_all();
                })?;
                let (lock, ready) = &*self.released;
                let mut released = lock.lock().unwrap();
                support::say("entered");
                while !*released {
                    released = ready.wait(released).unwrap();
                }
                if bytes == b"commit_hold" {
                    Ok(b"ok".to_vec())
                } else if cancel.is_cancelled() {
                    Err(Error::OwnerNotServing)
                } else {
                    Ok(bytes.to_vec())
                }
            }
            _ => Ok(bytes.to_vec()),
        }
    }
}
pub fn writer(folder: &Path, custom: bool) {
    let mut options = DatabaseOpenOptions::default();
    if custom {
        options.channel_dir = Some(folder.join("channel"));
    }
    let start = |options: DatabaseOpenOptions| {
        let db = Arc::new(support::open_with(folder, options.clone()).unwrap());
        if options.memory_limit.is_none() && db.table_names().unwrap().is_empty() {
            support::declare(&db);
        }
        let handler = Arc::new(Handler {
            db: Arc::downgrade(&db),
            ..Handler::default()
        });
        let weak = Arc::downgrade(&handler);
        assert!(matches!(
            db.serve_requests(&"x".repeat(65536), handler.clone()),
            Err(Error::OwnerLimitExceeded)
        ));
        db.serve_requests(NAMESPACE, handler.clone()).unwrap();
        assert!(matches!(
            db.serve_requests(NAMESPACE, handler.clone()),
            Err(Error::PlanError(..))
        ));
        drop(handler);
        (db, weak)
    };
    let (mut db, mut weak) = start(options.clone());
    support::say("serving");
    let mut next = support::int(&*db, "SELECT COUNT(*) FROM t") + 1;
    let mut descriptors = Vec::new();
    while let Some(word) = support::hear() {
        match word.as_str() {
            "release" => {
                let handler = weak.upgrade().unwrap();
                let (lock, ready) = &*handler.released;
                *lock.lock().unwrap() = true;
                ready.notify_all();
                support::say("released");
            }
            "commit" => {
                support::insert(&*db, next..=next);
                next += 1;
                support::say("committed");
            }
            "listen" => {
                // The channel's budget has room for a few listeners and then none.
                let cancel = weak
                    .upgrade()
                    .unwrap()
                    .cancel
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap();
                let alive = Arc::new(());
                let register = |held: &mut Vec<_>| loop {
                    let mark = alive.clone();
                    match cancel.tell_on_cancel(move || drop(mark.clone())) {
                        Ok(listener) => held.push(listener),
                        Err(error) => return error,
                    }
                    assert!(
                        held.len() < 1 << 20,
                        "listeners were admitted without limit"
                    );
                };
                let mut held = Vec::new();
                let error = register(&mut held);
                assert!(
                    matches!(error, Error::MemoryBudgetExceeded { .. }),
                    "{error:?}"
                );
                assert!(!held.is_empty(), "no listener fit in the spare room");
                assert_eq!(
                    Arc::strong_count(&alive),
                    1 + held.len(),
                    "the refused listener was stored before it was admitted"
                );
                let (count, full) = (held.len(), support::used(&db));
                held.clear();
                assert!(
                    support::used(&db) < full,
                    "dropping {count} listeners gave back none of their storage"
                );
                let error = register(&mut held);
                assert!(
                    matches!(error, Error::MemoryBudgetExceeded { .. }),
                    "{error:?}"
                );
                assert!(
                    held.len() >= count,
                    "storage given back was not usable again"
                );
                support::say("listened");
            }
            "tight" | "roomy" | "snug" => {
                // An open at the floor reaches zero spare without a limit-setting commit.
                db.close().unwrap();
                drop(db);
                let mut next_options = options.clone();
                next_options.memory_limit = (word != "roomy").then(|| {
                    let spare = if word == "snug" { 4096 } else { 0 };
                    support::opening::opening_charge(folder, None) + NAMESPACE.len() + spare
                });
                (db, weak) = start(next_options);
                support::say("ready");
            }
            "fill" => {
                while let Ok(file) = std::fs::File::open("/dev/null") {
                    descriptors.push(file);
                }
                support::say("filled");
            }
            "free" => {
                descriptors.clear();
                support::say("freed");
            }
            "used" => support::say(support::used(&db)),
            "threads" => support::say(weak.upgrade().unwrap().threads.lock().unwrap().len()),
            "census" => support::say(support::threads()),
            "close" | "drop" => {
                let cancel = weak.upgrade().unwrap().cancel.lock().unwrap().clone();
                if word == "close" {
                    db.close().unwrap();
                } else {
                    drop(db);
                }
                if let Some(cancel) = cancel {
                    let calls = Arc::new(Mutex::new(0));
                    let noted = calls.clone();
                    let listener = cancel
                        .tell_on_cancel(move || *noted.lock().unwrap() += 1)
                        .unwrap();
                    assert_eq!(*calls.lock().unwrap(), 1);
                    drop(listener);
                    assert_eq!(*calls.lock().unwrap(), 1);
                }
                assert!(weak.upgrade().is_none(), "close released the handler");
                let reopened = support::open(folder);
                support::assert_rows_are(&reopened, 1..=next - 1);
                support::close(reopened);
                support::say("closed");
                return;
            }
            other => panic!("unexpected writer command {other}"),
        }
    }
}
pub fn request(folder: &Path, bytes: &[u8]) -> contextdb::Result<Vec<u8>> {
    request_owner(folder, NAMESPACE, bytes, &ReadLimits::default())
}
pub fn assert_kind(error: Error, kind: &str) {
    let expected = match (&error, kind) {
        (Error::OwnerNotRunning, "absent") => "owner_not_running",
        (Error::OwnerNotServing, "closing") => "owner_not_serving",
        (Error::OwnerLimitExceeded, "large") => "owner_limit_exceeded",
        (Error::OwnerDisconnected, "cut") => "owner_disconnected",
        (Error::OwnerRefused { reason }, "refused")
            if reason.contains("the host refused this request") =>
        {
            "owner_refused"
        }
        _ => panic!("{kind}: {error:?}"),
    };
    assert!(error.to_string().starts_with(expected), "{error}");
}

pub fn register(db: &contextdb::Database) -> contextdb::Result<()> {
    db.serve_requests(NAMESPACE, Arc::new(Handler::default()))
}
pub fn socket_address(folder: &Path) -> (std::fs::File, std::path::PathBuf) {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::MetadataExt;
    let data = std::fs::metadata(folder.join("data")).unwrap();
    let directory = std::fs::File::open(folder).unwrap();
    let name = format!("{}-{}.sock", data.dev(), data.ino());
    let address = format!("/proc/self/fd/{}/{name}", directory.as_raw_fd()).into();
    (directory, address)
}
