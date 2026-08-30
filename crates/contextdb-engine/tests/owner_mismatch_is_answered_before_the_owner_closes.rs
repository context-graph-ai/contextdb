//! A reader that presents the wrong owner facts is told so, every time.
//!
//! A reader whose recorded database identity or writer run no longer matches
//! the owner it dialled has to learn that its record is stale, because that
//! is the one fault it can act on: re-read the owner's facts and dial again.
//! The answer that says the owner went away sends it somewhere else entirely
//! -- to wait, to retry, to report an outage -- and if that answer arrives
//! even occasionally, the reader cannot trust the one it usually gets.
//!
//! So the identity verdict is a property of the presented handshake and of
//! nothing else: the same wrong fact answers the same way on the first
//! connection and the thousandth, alone or alongside others.
//!
//! Nothing here holds the owner still between the refusal it sends and the
//! close that follows it -- there is no seam for that -- so what stands in
//! for it is volume and simultaneity: every field is driven many times in a
//! row and again from several readers at once, and a single answer of the
//! wrong shape fails the pin.

#![cfg(all(unix, feature = "test-seams"))]

use contextdb_core::read_contract::{
    DatabaseIdentity, DeadlineClock, LocalUserIdentity, OwnerReadLimits, OwnerReadStatus,
    OwnerServiceTimeouts, OwnerServingState, ReadClientTimeouts, ReadFailureKind, ReadLimits,
    WriterRunNumber,
};
use contextdb_engine::local_transport::{
    ChannelPathFacts, LocalConfigurationSource, LocalHandshake, LocalRequest, LocalRequestEnvelope,
    ManualDeadlineClock,
};
use contextdb_engine::owner_read::{
    OwnerClient, OwnerReadScaffoldError, OwnerReadService, OwnerServiceSpec, ValidatedOwnerListener,
};
use contextdb_engine::{Database, OwnerReadConfig, OwnerReadTestHooks};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Wake, Waker};

/// How many times one wrong field is presented on its own connection.
const SEQUENTIAL_CONNECTIONS: usize = 200;
/// How many readers present the same wrong field at the same moment.
const SIMULTANEOUS_READERS: usize = 8;
/// How many connections each simultaneous reader makes.
const CONNECTIONS_PER_READER: usize = 25;

struct FutureSignal {
    ready: Mutex<bool>,
    changed: Condvar,
}

impl Wake for FutureSignal {
    fn wake(self: Arc<Self>) {
        let mut ready = self.ready.lock().expect("future signal state");
        *ready = true;
        self.changed.notify_one();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let mut ready = self.ready.lock().expect("future signal state");
        *ready = true;
        self.changed.notify_one();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let signal = Arc::new(FutureSignal {
        ready: Mutex::new(true),
        changed: Condvar::new(),
    });
    let waker = Waker::from(Arc::clone(&signal));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        if let Poll::Ready(output) = Pin::as_mut(&mut future).poll(&mut context) {
            return output;
        }
        let mut ready = signal.ready.lock().expect("future signal state");
        while !*ready {
            ready = signal.changed.wait(ready).expect("future signal wait");
        }
        *ready = false;
    }
}

fn limits() -> ReadLimits {
    ReadLimits {
        result_rows: 500,
        result_bytes: 4 * 1024 * 1024,
        work: 50_000,
        active_ms: 5_000,
        memory: 16 * 1024 * 1024,
        cursor_page_rows: 100,
        cursor_page_bytes: 1024 * 1024,
        cursor_idle_ms: 300_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

/// A live owner serving on its own channel, with the facts it expects a
/// reader to present.
struct LiveOwner {
    _directory: tempfile::TempDir,
    _database: Arc<Database>,
    service: Arc<OwnerReadService>,
    expected: LocalHandshake,
    clock: ManualDeadlineClock,
    client_timeouts: ReadClientTimeouts,
}

impl LiveOwner {
    fn start() -> Self {
        let directory = tempfile::tempdir().expect("task-scoped owner-read directory");
        let database_path = directory.path().join("owner-identity.db");
        let database = Arc::new(Database::open(&database_path).expect("open owner database"));
        database
            .set_memory_limit(Some(64 * 1024 * 1024))
            .expect("declare database memory so the owner can serve");
        let owner_user = LocalUserIdentity(nix::unistd::Uid::effective().as_raw() as u64);
        let expected = LocalHandshake::current(
            DatabaseIdentity([0x31; 16]),
            WriterRunNumber([0x42; 16]),
            owner_user,
        );
        let clock = ManualDeadlineClock::at(100);
        let (drain_started, _drain_started_at) = std::sync::mpsc::channel();
        let config = OwnerReadConfig {
            enabled: true,
            limits: OwnerReadLimits {
                limits: limits(),
                concurrency: 4,
            },
            timeouts: OwnerServiceTimeouts {
                request_ms: 10_000,
                shutdown_drain_ms: 10_000,
            },
            runtime_dir: Some(directory.path().to_path_buf()),
            handler: None,
            test_hooks: Some(OwnerReadTestHooks {
                clock: Arc::new(clock.clone()),
                drain_started,
            }),
        };
        let listener = ValidatedOwnerListener::new(ChannelPathFacts {
            path: directory.path().join("owner.sock"),
            runtime_directory: directory.path().to_path_buf(),
            is_socket: true,
            owner: owner_user,
            mode: 0o700,
        });
        let spec = OwnerServiceSpec::new(
            Arc::clone(&database),
            listener,
            expected.clone(),
            OwnerReadStatus {
                state: OwnerServingState::Serving,
                reason: None,
            },
            config,
            LocalConfigurationSource::Override,
            Arc::new(clock.clone()),
        );
        let service =
            OwnerReadService::start(spec).expect("the owner service starts and serves readers");
        Self {
            _directory: directory,
            _database: database,
            service,
            expected,
            clock,
            client_timeouts: ReadClientTimeouts {
                connect_ms: 1_000,
                routing_retry_ms: 1_000,
                response_ms: 11_000,
            },
        }
    }

    /// Dial the live owner on a fresh connection presenting `presented`, ask
    /// it one read, and report how that attempt was answered.
    fn answer_for(&self, presented: LocalHandshake) -> Answer {
        let clock: Arc<dyn DeadlineClock> = Arc::new(self.clock.clone());
        let connected = block_on(OwnerClient::connect(
            self.service.channel_path(),
            presented,
            self.client_timeouts,
            clock,
        ));
        let mut client = match connected {
            Ok(client) => client,
            Err(error) => return Answer::of(error),
        };
        match block_on(client.request(LocalRequestEnvelope {
            limits: limits(),
            request: LocalRequest::Query {
                statement: "SELECT 1".to_owned(),
                params: BTreeMap::new(),
            },
        })) {
            Ok(responses) => Answer::Served(responses.len()),
            Err(error) => Answer::of(error),
        }
    }
}

/// What one connection attempt came back with.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Answer {
    /// The owner refused, naming what it refused for.
    Refused(ReadFailureKind),
    /// The owner served the reader instead of refusing it.
    Served(usize),
    /// Something other than a typed refusal came back.
    Untyped(String),
}

impl Answer {
    fn of(error: OwnerReadScaffoldError) -> Self {
        match error {
            OwnerReadScaffoldError::Refused(failure) => Self::Refused(failure.kind()),
            other => Self::Untyped(format!("{other:?}")),
        }
    }
}

/// One handshake field presented wrongly, and the verdict that names it.
struct WrongField {
    name: &'static str,
    presented: LocalHandshake,
    verdict: ReadFailureKind,
}

fn wrong_fields(expected: &LocalHandshake) -> Vec<WrongField> {
    let mut marker = expected.clone();
    marker.marker[0] ^= 1;
    let mut version = expected.clone();
    version.version = version.version.saturating_add(1);
    let mut database = expected.clone();
    database.database_identity.0[0] ^= 1;
    let mut writer = expected.clone();
    writer.writer_run.0[0] ^= 1;
    let mut recorded_user = expected.clone();
    recorded_user.owner_user = LocalUserIdentity(expected.owner_user.0 + 1);
    vec![
        WrongField {
            name: "protocol marker",
            presented: marker,
            verdict: ReadFailureKind::LocalProtocolMismatch,
        },
        WrongField {
            name: "protocol version",
            presented: version,
            verdict: ReadFailureKind::LocalProtocolMismatch,
        },
        WrongField {
            name: "database identity",
            presented: database,
            verdict: ReadFailureKind::OwnerMismatch,
        },
        WrongField {
            name: "writer run",
            presented: writer,
            verdict: ReadFailureKind::OwnerMismatch,
        },
        WrongField {
            name: "recorded owner user",
            presented: recorded_user,
            verdict: ReadFailureKind::OwnerUserMismatch,
        },
    ]
}

/// Every attempt at one wrong field, counted by the verdict it earned.
fn tally(answers: impl IntoIterator<Item = Answer>) -> BTreeMap<String, usize> {
    let mut counted = BTreeMap::new();
    for answer in answers {
        *counted.entry(format!("{answer:?}")).or_insert(0) += 1;
    }
    counted
}

/// The verdict a wrong field earns is the same one every time. Every attempt
/// for the field is counted before anything is judged, so one run reports
/// every field and the rate at which each one answers wrongly, rather than
/// stopping at whichever field happens to slip first.
fn record_disagreement(
    field: &str,
    expected: ReadFailureKind,
    attempts: usize,
    counted: &BTreeMap<String, usize>,
    disagreements: &mut Vec<String>,
) {
    let agreed = counted
        .get(&format!("{:?}", Answer::Refused(expected)))
        .copied()
        .unwrap_or_default();
    if agreed == attempts {
        return;
    }
    let went_away = counted
        .get(&format!(
            "{:?}",
            Answer::Refused(ReadFailureKind::OwnerDisconnected)
        ))
        .copied()
        .unwrap_or_default();
    disagreements.push(format!(
        "the wrong {field} was refused for that field on {agreed} of {attempts} \
         connections, and {went_away} of them were told the owner went away instead -- an \
         answer that sends a reader to wait out an outage rather than correct its record; \
         every verdict counted: {counted:?}"
    ));
}

#[test]
fn a_wrong_handshake_field_earns_the_same_verdict_on_every_connection() {
    let owner = LiveOwner::start();
    let mut disagreements = Vec::new();
    for field in wrong_fields(&owner.expected) {
        let counted =
            tally((0..SEQUENTIAL_CONNECTIONS).map(|_| owner.answer_for(field.presented.clone())));
        record_disagreement(
            field.name,
            field.verdict,
            SEQUENTIAL_CONNECTIONS,
            &counted,
            &mut disagreements,
        );
    }
    assert!(
        disagreements.is_empty(),
        "a stale record earns the same refusal on the first connection and the \
         {SEQUENTIAL_CONNECTIONS}th:\n{}",
        disagreements.join("\n")
    );
}

#[test]
fn a_wrong_handshake_field_earns_the_same_verdict_from_readers_arriving_together() {
    let owner = Arc::new(LiveOwner::start());
    let attempts = SIMULTANEOUS_READERS * CONNECTIONS_PER_READER;
    let mut disagreements = Vec::new();
    for field in wrong_fields(&owner.expected) {
        // Every reader waits at the same gate and is let go together, so the
        // owner is answering many refusals at once rather than one at a time.
        let arrived = Arc::new((Mutex::new(0usize), Condvar::new()));
        let started = Arc::new(AtomicUsize::new(0));
        let mut readers = Vec::new();
        for _ in 0..SIMULTANEOUS_READERS {
            let owner = Arc::clone(&owner);
            let arrived = Arc::clone(&arrived);
            let started = Arc::clone(&started);
            let presented = field.presented.clone();
            readers.push(
                std::thread::Builder::new()
                    .name("contextdb-mismatched-reader".to_owned())
                    .spawn(move || {
                        let (count, released) = &*arrived;
                        {
                            let mut count = count.lock().expect("reader gate state");
                            *count += 1;
                            released.notify_all();
                            while *count < SIMULTANEOUS_READERS {
                                count = released.wait(count).expect("reader gate wait");
                            }
                        }
                        let _ = started.fetch_add(1, Ordering::SeqCst);
                        (0..CONNECTIONS_PER_READER)
                            .map(|_| owner.answer_for(presented.clone()))
                            .collect::<Vec<_>>()
                    })
                    .expect("spawn a reader presenting a stale record"),
            );
        }
        let counted = tally(
            readers
                .into_iter()
                .flat_map(|reader| reader.join().expect("a reader finishes its connections")),
        );
        assert_eq!(
            started.load(Ordering::SeqCst),
            SIMULTANEOUS_READERS,
            "every reader passed the gate before any of them connected"
        );
        assert_eq!(
            counted.values().sum::<usize>(),
            attempts,
            "every simultaneous connection is accounted for"
        );
        record_disagreement(
            field.name,
            field.verdict,
            attempts,
            &counted,
            &mut disagreements,
        );
    }
    assert!(
        disagreements.is_empty(),
        "a stale record earns the same refusal however many readers present it at once:\n{}",
        disagreements.join("\n")
    );
}

#[test]
fn the_owner_still_serves_a_reader_that_presents_its_recorded_facts() {
    let owner = LiveOwner::start();

    // The refusals above are about stale records, not about an owner that has
    // stopped serving: the facts it published still get a reader in.
    let observed = owner.answer_for(owner.expected.clone());
    match observed {
        Answer::Served(responses) => assert!(
            responses > 0,
            "a reader presenting the owner's own facts is answered"
        ),
        other => panic!("a reader presenting the owner's own facts is served, got {other:?}"),
    }
    assert!(matches!(
        owner.service.status().state,
        OwnerServingState::Serving
    ));
}
