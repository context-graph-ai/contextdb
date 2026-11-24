//! Four listeners serve many processes, independently of the writer's commits.
pub mod channel_support;
mod support;
use channel_support::{NAMESPACE, PAYLOAD, assert_kind, request};
use contextdb::Error::QueryCanceled;
use contextdb::{DatabaseOpenOptions, ReadLimits, ReadSession, request_owner};
use support::Child;
#[test]
fn a_request_is_answered_between_two_processes() {
    const TEST: &str = "a_request_is_answered_between_two_processes";
    if let Some(role) = support::role() {
        return match role.name.as_str() {
            "writer" => channel_support::writer(&role.folder, false),
            "held" => {
                assert_eq!(request(&role.folder, b"hold").unwrap(), b"hold");
                support::say("answered");
            }
            "reader" => {
                for bytes in [b"".as_slice(), PAYLOAD] {
                    assert_eq!(request(&role.folder, bytes).unwrap(), bytes);
                }
                support::say("answered");
            }
            "hundred" => {
                request(&role.folder, PAYLOAD).unwrap();
                support::say("first");
                support::hear();
                for _ in 1..100 {
                    request(&role.folder, PAYLOAD).unwrap();
                }
                support::say("hundredth");
            }
            _ => panic!("unknown role"),
        };
    }
    let folder = support::folder("channel-processes");
    let mut writer = Child::start(TEST, "writer", &folder);
    writer.expect("serving");
    // Four occupied handlers prove all four listeners exist; later requests must reuse them.
    writer.tell("used");
    let idle: usize = writer.line().unwrap().parse().unwrap();
    let mut held = Vec::new();
    for _ in 0..4 {
        held.push(Child::start(TEST, "held", &folder));
        writer.expect("entered");
    }
    writer.tell("used");
    let receiving: usize = writer.line().unwrap().parse().unwrap();
    assert!(receiving >= idle + 4 * (NAMESPACE.len() + b"hold".len()));
    writer.ask("commit", "committed");
    let reader = ReadSession::open(&folder, ReadLimits::default()).unwrap();
    support::assert_rows_are(&reader, 1..=1);
    writer.ask("release", "released");
    for child in &mut held {
        child.expect("answered");
        child.end();
    }
    for _ in 0..12 {
        let mut child = Child::start(TEST, "reader", &folder);
        child.expect("answered");
        child.end();
    }
    writer.ask("threads", "4");
    // One process's hundred requests leave the writer's threads as its first left them
    // (intent:55-59, design 8.3).
    let mut hundred = Child::start(TEST, "hundred", &folder);
    hundred.expect("first");
    writer.tell("census");
    let first = writer.line();
    hundred.ask("on", "hundredth");
    writer.tell("census");
    assert_eq!(writer.line(), first, "after one request and a hundred");
    hundred.end();
    writer.ask("commit", "committed");
    support::assert_rows_are(&reader, 1..=2);
    writer.ask("close", "closed");
    writer.end();
}
#[test]
fn each_way_a_request_fails_has_its_kind() {
    const TEST: &str = "each_way_a_request_fails_has_its_kind";
    if let Some(role) = support::role() {
        return match role.name.as_str() {
            "writer" => channel_support::writer(&role.folder, false),
            "malformed" => {
                use std::io::{Read, Write};
                let (_directory, address) = channel_support::socket_address(&role.folder);
                let listener = std::os::unix::net::UnixListener::bind(address).unwrap();
                support::say("listening");
                let (mut socket, _) = listener.accept().unwrap();
                for _ in 0..2 {
                    let mut size = [0; 4];
                    socket.read_exact(&mut size).unwrap();
                    socket
                        .read_exact(&mut vec![0; u32::from_be_bytes(size) as usize])
                        .unwrap();
                }
                socket.write_all(&[255, 0, 0, 0, 0]).unwrap();
            }
            "deadline" => {
                let limits = ReadLimits {
                    active_ms: 10,
                    ..ReadLimits::default()
                };
                let answer = request_owner(&role.folder, NAMESPACE, b"hold", &limits);
                assert!(
                    matches!(answer, Err(QueryCanceled { statement_timeout }) if statement_timeout)
                );
                support::say("timed out");
            }
            "committed" => assert_eq!(request(&role.folder, b"commit_hold").unwrap(), b"ok"),
            "closing" | "cut" => {
                let bytes = if role.name == "closing" {
                    b"closing".as_slice()
                } else {
                    b"hold".as_slice()
                };
                support::say("requesting");
                assert_kind(request(&role.folder, bytes).unwrap_err(), &role.name);
                support::say("refused");
            }
            _ => panic!("unknown role"),
        };
    }
    let folder = support::folder("channel-failures");
    let db = support::open(&folder);
    support::close(db);
    assert_kind(request(&folder, b"no listener").unwrap_err(), "absent");
    #[cfg(target_os = "linux")]
    {
        let mut fake = Child::start(TEST, "malformed", &folder);
        fake.expect("listening");
        assert_kind(request(&folder, PAYLOAD).unwrap_err(), "cut");
        fake.end();
    }
    let mut writer = Child::start(TEST, "writer", &folder);
    writer.expect("serving");
    let small_answer = ReadLimits {
        result_bytes: 8,
        active_ms: u64::MAX,
        ..ReadLimits::default()
    };
    let small_request = ReadLimits {
        memory: 1,
        ..ReadLimits::default()
    };
    let reader = contextdb::ReadSession::open(&folder, ReadLimits::default()).unwrap();
    for (n, (bytes, limits, kind)) in [
        (b"refuse".as_slice(), ReadLimits::default(), "refused"),
        (b"answer".as_slice(), small_answer, "large"),
        ([0_u8; 64].as_slice(), small_request, "large"),
    ]
    .into_iter()
    .enumerate()
    {
        let error = request_owner(&folder, NAMESPACE, bytes, &limits).unwrap_err();
        assert!(!error.to_string().contains("unimplemented"), "{error}");
        assert_kind(error, kind);
        assert_eq!(request(&folder, b"answer").unwrap(), vec![255; 64]);
        support::assert_rows_are(&reader, 1..=n as i64);
        writer.ask("commit", "committed");
        support::assert_rows_are(&reader, 1..=n as i64 + 1);
    }
    // A deadline that has passed before the owner could serve the request is a timeout, not a hang.
    let expired = ReadLimits {
        active_ms: 0,
        ..ReadLimits::default()
    };
    let late = request_owner(&folder, NAMESPACE, b"answer", &expired);
    assert!(
        matches!(
            late,
            Err(QueryCanceled {
                statement_timeout: true
            })
        ),
        "{late:?}"
    );
    writer.ask("tight", "ready");
    for bytes in [[0; 512].as_slice(), b"huge"] {
        let limits = ReadLimits {
            result_bytes: 0,
            ..ReadLimits::default()
        };
        let error = request_owner(&folder, NAMESPACE, bytes, &limits).unwrap_err();
        assert!(
            matches!(error, contextdb::Error::MemoryBudgetExceeded { requested_bytes, available_bytes: 0, budget_limit_bytes, .. } if requested_bytes > 0 && budget_limit_bytes > 0),
            "{error:?}"
        );
    }
    // Registering a cancellation listener is refused by the channel's budget, by name, before the
    // listener is stored; dropping listeners gives their storage back.
    writer.ask("snug", "ready");
    assert_eq!(request(&folder, b"answer").unwrap(), vec![255; 64]);
    writer.ask("listen", "listened");
    writer.ask("roomy", "ready");
    assert_eq!(request(&folder, PAYLOAD).unwrap(), PAYLOAD);
    writer.ask("commit", "committed");
    #[cfg(target_os = "linux")]
    {
        let old = writer.limit("nofile", "64");
        writer.ask("fill", "filled");
        assert_kind(request(&folder, PAYLOAD).unwrap_err(), "cut");
        writer.ask("free", "freed");
        writer.limit("nofile", &old);
        assert_eq!(request(&folder, PAYLOAD).unwrap(), PAYLOAD);
        writer.ask("commit", "committed");
    }
    assert!(
        matches!(request_owner(&folder, "wrong", b"", &ReadLimits::default()), Err(contextdb::Error::OwnerRefused { reason }) if reason.contains("namespace"))
    );
    assert!(
        matches!(request(&folder, b"unicode"), Err(contextdb::Error::OwnerRefused { reason }) if reason.len() <= u16::MAX as usize && reason.ends_with('é'))
    );
    #[cfg(target_os = "linux")]
    {
        use std::io::{Read, Write};
        writer.tell("used");
        writer.line();
        writer.tell("used");
        let idle: usize = writer.line().unwrap().parse().unwrap();
        let (_directory, address) = channel_support::socket_address(&folder);
        let mut active = std::os::unix::net::UnixStream::connect(address).unwrap();
        active
            .write_all(
                &[
                    (NAMESPACE.len() as u32).to_be_bytes().as_slice(),
                    NAMESPACE.as_bytes(),
                    &4096_u32.to_be_bytes(),
                    &[0; 4095],
                ]
                .concat(),
            )
            .unwrap();
        loop {
            writer.tell("used");
            if writer.line().unwrap().parse::<usize>().unwrap() >= idle + 4096 {
                break;
            }
            std::thread::yield_now();
        }
        writer.ask("close", "closed");
        let mut reply = Vec::new();
        let _ = active.read_to_end(&mut reply);
        assert!(reply.first().is_none_or(|b| *b != 0));
        writer.end();
        writer = Child::start(TEST, "writer", &folder);
        writer.expect("serving");
    }
    let mut closing = Vec::new();
    for _ in 0..3 {
        let mut child = Child::start(TEST, "closing", &folder);
        child.expect("requesting");
        writer.expect("entered");
        closing.push(child);
    }
    let mut committed = Child::start(TEST, "committed", &folder);
    writer.expect("entered");
    // Connect with no frames while all handlers are held; write only after close returns.
    #[cfg(target_os = "linux")]
    let mut queued = {
        let (_directory, address) = channel_support::socket_address(&folder);
        std::os::unix::net::UnixStream::connect(address).unwrap()
    };
    writer.ask("close", "closed");
    #[cfg(target_os = "linux")]
    {
        assert!(std::io::Write::write_all(&mut queued, b"late frames").is_err());
        let mut reply = [0; 5];
        std::io::Read::read_exact(&mut queued, &mut reply).unwrap();
        assert_eq!(reply, [1, 0, 0, 0, 0]);
    }
    for child in &mut closing {
        child.expect("refused");
        child.end();
    }
    committed.end();
    writer.end();
    assert_eq!(support::int(&reader, "SELECT COUNT(*) FROM committed"), 1);
    assert_kind(request(&folder, b"after close").unwrap_err(), "absent");
    // A severed connection differs from both an orderly shutdown and an absent listener.
    let mut writer = Child::start(TEST, "writer", &folder);
    writer.expect("serving");
    let mut cut = Child::start(TEST, "cut", &folder);
    cut.expect("requesting");
    writer.expect("entered");
    writer.kill();
    cut.expect("refused");
    cut.end();
    // Drop, like close, releases the handler and the store lock before returning.
    let mut writer = Child::start(TEST, "writer", &folder);
    writer.expect("serving");
    let mut deadline = Child::start(TEST, "deadline", &folder);
    writer.expect("entered");
    deadline.expect("timed out");
    assert_eq!(
        request(&folder, b"answer").unwrap(),
        vec![255; 64],
        "after a timeout"
    );
    writer.ask("release", "released");
    deadline.end();
    writer.ask("drop", "closed");
    writer.end();
    // An unavailable optional channel is reported while ordinary writes still work.
    #[cfg(target_os = "linux")]
    {
        let (_directory, socket) = channel_support::socket_address(&folder);
        std::fs::create_dir(&socket).unwrap();
        let db = support::open(&folder);
        assert!(matches!(
            channel_support::register(&db),
            Err(contextdb::Error::StoreIo { .. })
        ));
        support::close(db);
        std::fs::remove_dir(socket).unwrap();
    }
    let blocked = folder.join("blocked-channel");
    std::fs::write(&blocked, b"a file, not a directory").unwrap();
    let mut options = DatabaseOpenOptions::default();
    options.channel_dir = Some(blocked);
    let db = support::open_with(&folder, options).expect("channel failure leaves the store usable");
    assert!(matches!(
        channel_support::register(&db),
        Err(contextdb::Error::StoreIo { .. })
    ));
    let before = support::int(&db, "SELECT COUNT(*) FROM t");
    support::assert_rows_are(&db, 1..=before);
    support::insert(&db, before + 1..=before + 1);
    support::assert_rows_are(&db, 1..=before + 1);
    support::close(db);
}
#[test]
fn a_two_hundred_character_store_path_reaches_the_writer() {
    const TEST: &str = "a_two_hundred_character_store_path_reaches_the_writer";
    if let Some(role) = support::role() {
        return channel_support::writer(&role.folder, role.name == "custom");
    }
    let base = support::folder("channel-long-path");
    let length = base.as_os_str().len();
    assert!(length < 199, "the fixture leaves room for the store name");
    let folder = base.join("x".repeat(199 - length));
    assert_eq!(folder.as_os_str().len(), 200);
    for role in ["writer", "custom"] {
        let mut writer = Child::start(TEST, role, &folder);
        writer.expect("serving");
        assert_eq!(request(&folder, PAYLOAD).unwrap(), PAYLOAD);
        if role == "custom" {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            let mode = |path: &std::path::Path| path.metadata().unwrap().permissions().mode();
            let data = folder.join("data").metadata().unwrap();
            let name = format!("{}-{}.sock", data.dev(), data.ino());
            assert_eq!(
                mode(&folder.join("channel")) & 0o077,
                0,
                "only the owner may enter the channel directory"
            );
            #[cfg(target_os = "linux")]
            let socket = folder.join("channel").join(&name);
            #[cfg(not(target_os = "linux"))]
            let socket = {
                use std::hash::{Hash, Hasher};
                let directory = std::fs::canonicalize(folder.join("channel")).unwrap();
                let mut bytes = [0_u8; 32];
                for seed in 0..4 {
                    let mut hash = std::collections::hash_map::DefaultHasher::new();
                    (seed, &directory, &name).hash(&mut hash);
                    bytes[seed * 8..seed * 8 + 8].copy_from_slice(&hash.finish().to_be_bytes());
                }
                let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
                let mut encoded = String::new();
                for chunk in bytes.chunks(3) {
                    let word = chunk
                        .iter()
                        .fold(0_u32, |word, byte| (word << 8) | u32::from(*byte))
                        << (8 * (3 - chunk.len()));
                    for shift in (0..=chunk.len()).map(|i| 18 - i * 6) {
                        encoded.push(alphabet[((word >> shift) & 63) as usize] as char);
                    }
                }
                std::env::temp_dir().join(encoded)
            };
            assert_eq!(mode(&socket) & 0o077, 0, "only the owner may connect");
        }
        writer.ask("close", "closed");
        writer.end();
    }
}
#[test]
fn a_reader_is_never_answered_by_the_writer_of_another_store() {
    let folders = ["one", "two"].map(support::folder);
    let mut options = DatabaseOpenOptions::default();
    options.channel_dir = Some(support::folder("shared"));
    let [first, second] = folders.each_ref().map(|folder| {
        let db = support::open_with(folder, options.clone()).unwrap();
        channel_support::register(&db).unwrap();
        db
    });
    assert!(
        folders
            .iter()
            .all(|f| request(f, PAYLOAD).unwrap() == PAYLOAD)
    );
    support::close(first); // returns while the second is open: each writer has its own name
    assert_kind(request(&folders[0], PAYLOAD).unwrap_err(), "absent");
    assert_eq!(request(&folders[1], PAYLOAD).unwrap(), PAYLOAD);
    support::close(second);
}
