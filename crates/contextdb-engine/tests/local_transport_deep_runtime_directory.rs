#![cfg(unix)]
#![cfg(feature = "test-seams")]

//! An operator's directory layout does not get to take the channel away.
//!
//! A Unix socket address is capped at 107 bytes on Linux, and the channel's
//! fixed basename is 69 of them. That left an operator roughly thirty
//! characters for the whole runtime directory -- less than a normally-named
//! service directory takes -- and past that line the owner could not create
//! its channel and every reader saw a live, serving store as NOT serving.
//! Nothing told the operator why; the store simply stopped answering over the
//! fast route.
//!
//! The channel is now addressed through the runtime directory held open, so
//! how deep the directory sits stops being part of the question. These
//! journeys use a runtime directory whose absolute pathname is longer than the
//! entire socket-address limit -- the case that used to be unserveable -- and
//! require that the owner serves on it and that a reader connects and reads
//! real rows back over that channel.

use contextdb_core::read_contract::{
    DeadlineClock, LocalUserIdentity, ReadClientTimeouts, ReadLimits,
};
use contextdb_engine::local_transport::{
    ChannelKernelAddress, ChannelPathViolation, LocalHandshake, LocalRequest, LocalRequestEnvelope,
    LocalResponse, LocalTransportError, MonotonicDeadlineClock, channel_socket_path,
    unix_socket_path_limit,
};
use contextdb_engine::owner_read::OwnerClient;
use contextdb_engine::persistence::read_persistence_test_scaffold::inspect_companion_record_for_test;
use contextdb_engine::read_contract::decode_cursor_page;
use contextdb_engine::{Database, DatabaseOpenOptions};
use std::collections::HashMap;
use std::future::Future;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

struct ThreadWake(std::thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match Pin::as_mut(&mut future).poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}

/// A runtime directory of the shape a packaged service actually gets: a real,
/// owner-only directory whose absolute pathname is longer than the whole
/// socket-address limit. Every component is named, not padded to a number, so
/// this reads like the deployment it stands for.
fn deep_runtime_directory(base: &Path) -> PathBuf {
    let directory = base
        .join("var-lib-contextdb-service-instance")
        .join("state-per-tenant-directory")
        .join("owner-read-channel-runtime-root")
        .join("owner-read-runtime-directory");
    std::fs::create_dir_all(&directory).expect("the operator's runtime directory");
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
        .expect("an owner-only runtime directory");
    assert!(
        directory.as_os_str().len()
            > unix_socket_path_limit().expect("a platform with a socket-address limit"),
        "this journey is only about the case the address limit used to refuse",
    );
    directory
}

fn owner_handshake(store: &Path) -> LocalHandshake {
    let record = inspect_companion_record_for_test(store).expect("published companion record");
    LocalHandshake::current(
        record.fields.database_identity,
        record.fields.writer_run_number,
        LocalUserIdentity(nix::unistd::Uid::effective().as_raw() as u64),
    )
}

fn owner_channel(store: &Path, runtime_directory: &Path) -> PathBuf {
    let record = inspect_companion_record_for_test(store).expect("published companion record");
    channel_socket_path(runtime_directory, record.fields.channel_address)
        .expect("a channel below a deep runtime directory is still addressable")
}

#[test]
fn an_owner_serves_and_a_reader_reads_below_a_runtime_directory_past_the_address_limit() {
    let base = tempfile::Builder::new()
        .prefix("cdb")
        .tempdir_in("/tmp")
        .expect("a short base so only the directory below it is deep");
    let runtime_directory = deep_runtime_directory(base.path());
    let store_directory = tempfile::tempdir().expect("store directory");
    let store = store_directory.path().join("deep.db");

    let mut options = DatabaseOpenOptions::default();
    options.owner_reads.runtime_dir = Some(runtime_directory.clone());
    let database = Database::open_with_options(&store, options).expect("the owner opens the store");
    database
        .execute(
            "CREATE TABLE deep_rows (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("create the fixture table");
    for id in 1..=3_i64 {
        database
            .execute(
                "INSERT INTO deep_rows VALUES ($id)",
                &HashMap::from([("id".to_owned(), contextdb_core::Value::Int64(id))]),
            )
            .expect("insert the fixture rows");
    }

    let status = format!("{:?}", database.owner_read_status());
    assert!(
        status.contains("Serving") && !status.contains("NotServing"),
        "an owner whose runtime directory is deep must still serve: {status}",
    );

    let channel = owner_channel(&store, &runtime_directory);
    assert!(
        channel.exists(),
        "the channel is created at its real pathname, whatever address the bind was given",
    );

    let clock: Arc<dyn DeadlineClock> = Arc::new(MonotonicDeadlineClock::new());
    let mut client = block_on(OwnerClient::connect(
        &channel,
        owner_handshake(&store),
        ReadClientTimeouts::default(),
        clock,
    ))
    .expect("a reader connects to the deep channel");

    let limits = ReadLimits::default();
    let responses = block_on(client.request(LocalRequestEnvelope {
        limits,
        request: LocalRequest::OwnerStatus,
    }))
    .expect("the owner answers for its own state");
    match responses.as_slice() {
        [LocalResponse::OwnerStatus { status }] => {
            let state = format!("{:?}", status.status);
            assert!(
                state.contains("Serving") && !state.contains("NotServing"),
                "the owner reports itself serving over the deep channel: {state}",
            );
        }
        other => panic!("owner status returns one response, got {other:?}"),
    }

    let responses = block_on(client.request(LocalRequestEnvelope {
        limits,
        request: LocalRequest::CursorOpen {
            statement: "SELECT id FROM deep_rows ORDER BY id".to_owned(),
            params: Default::default(),
        },
    }))
    .expect("the reader reads over the deep channel");
    let rows = match responses.as_slice() {
        [LocalResponse::CursorOpened { opened }] => {
            decode_cursor_page(&opened.payload)
                .expect("decode the page the owner answered with")
                .rows
        }
        other => panic!("a cursor open returns one opened response, got {other:?}"),
    };
    assert_eq!(
        rows,
        vec![
            vec![contextdb_core::Value::Int64(1)],
            vec![contextdb_core::Value::Int64(2)],
            vec![contextdb_core::Value::Int64(3)],
        ],
        "the rows a reader gets over a deep channel are the rows the store holds",
    );

    database.close().expect("the owner releases the store");
}

/// The pathname a channel lives at and the address the kernel is handed are
/// the same string until the pathname stops fitting, and only then differ.
/// An ordinary deployment therefore keeps exactly the addressing it had.
#[test]
fn the_kernel_address_is_the_pathname_until_the_pathname_stops_fitting() {
    let limit = unix_socket_path_limit().expect("a platform with a socket-address limit");

    let base = tempfile::Builder::new()
        .prefix("cdb")
        .tempdir_in("/tmp")
        .expect("a short base");
    let shallow = base.path().join("channel.sock");
    assert!(shallow.as_os_str().len() <= limit);
    let addressed = ChannelKernelAddress::resolve(&shallow).expect("a short pathname resolves");
    assert_eq!(
        addressed.as_path(),
        shallow.as_path(),
        "a pathname that fits is handed to the kernel unchanged",
    );
    assert!(
        !addressed.holds_its_directory(),
        "an ordinary deployment holds no extra descriptor",
    );

    let deep = deep_runtime_directory(base.path()).join("channel.sock");
    assert!(deep.as_os_str().len() > limit);
    let addressed = ChannelKernelAddress::resolve(&deep).expect("a deep pathname still resolves");
    assert_ne!(
        addressed.as_path(),
        deep.as_path(),
        "a pathname past the limit is addressed through the held directory instead",
    );
    assert!(
        addressed.as_path().as_os_str().len() <= limit,
        "what the kernel is handed fits the address limit: {:?}",
        addressed.as_path(),
    );
    assert_eq!(
        addressed.as_path().file_name(),
        deep.as_path().file_name(),
        "the channel keeps its name; only the directory is named differently",
    );
    assert!(
        addressed.holds_its_directory(),
        "the address names a descriptor, so the descriptor is held for as long as the address",
    );
}

/// The length refusal moved onto the kernel-facing address; it did not go away,
/// and it is still typed.
///
/// Holding the directory open bounds the DIRECTORY half of the address, not the
/// whole of it. What the kernel is shown is `/proc/self/fd/<descriptor>/` plus
/// the channel's own name, so a name that cannot fit beside that prefix is
/// still refused -- and refused as the same channel-path violation an over-long
/// pathname always produced, so a caller's handling of it does not change. On a
/// platform with no held-descriptor pathname the absolute path is the address
/// and the same refusal is reached directly.
///
/// Read this together with what the fix actually removed: with the shipped
/// 69-byte channel basename, the prefix-plus-name address is about ninety
/// bytes, so on Linux no runtime directory an operator can name reaches this
/// refusal any more. It guards the address, not the operator's directory
/// layout.
#[test]
fn a_name_the_kernel_still_cannot_hold_is_refused_as_a_channel_path_violation() {
    let limit = unix_socket_path_limit().expect("a platform with a socket-address limit");
    let base = tempfile::Builder::new()
        .prefix("cdb")
        .tempdir_in("/tmp")
        .expect("a short base");
    // Longer than the whole address limit by itself, so no directory -- however
    // it is named, however it is held -- can make room for it.
    let unholdable = deep_runtime_directory(base.path()).join("c".repeat(limit + 1));

    assert!(
        matches!(
            ChannelKernelAddress::resolve(&unholdable),
            Err(LocalTransportError::ChannelPath(
                ChannelPathViolation::PathTooLong
            ))
        ),
        "a name past the address limit is refused by class, not silently truncated or bound",
    );
}
