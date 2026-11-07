use super::*;

/// A runtime directory an operator supplied, owner-only, outside any
/// platform default.
fn supplied_root(directory: &Path, name: &str) -> PathBuf {
    let root = directory.join(name);
    std::fs::create_dir(&root).expect("create the operator-supplied runtime root");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .expect("secure the operator-supplied runtime root");
    }
    root
}

/// The supplied root is the state directory for the channel and the
/// reader registry alike.
#[test]
fn a_supplied_runtime_root_moves_the_breadcrumb_location() {
    let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
    let root = supplied_root(directory.path(), "supplied-runtime");
    let store = directory.path().join("packaged.db");
    std::fs::write(&store, b"").expect("the store this deployment names exists");

    let where_readers_go = reader_breadcrumb_directory(&store, &root)
        .expect("a supplied root derives the reader registry directory");

    assert!(
        where_readers_go.starts_with(&root),
        "readers are written down under the supplied runtime root"
    );
    if let Some(default_runtime) = reader_breadcrumb_runtime_directory() {
        assert_ne!(
            where_readers_go,
            reader_breadcrumb_directory(&store, default_runtime.path()).unwrap()
        );
    }
}

/// The writer scans the same root its direct readers publish into.
#[test]
fn a_writer_reads_the_breadcrumbs_of_a_supplied_runtime_root() {
    let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
    let root = supplied_root(directory.path(), "supplied-runtime");
    let store = directory.path().join("packaged.db");
    std::fs::write(&store, b"").expect("the store this deployment names exists");
    let breadcrumb = current_reader_breadcrumb().expect("identify this reader process");
    let guard = ReaderBreadcrumbGuard::create(&store, &root, Some(&breadcrumb))
        .expect("publish a locked reader breadcrumb inside the supplied root");
    let (observed, verified) = live_reader_holders(&store, Some(&root));
    assert_eq!(observed, 1);
    assert_eq!(verified.len(), 1);
    drop(guard);
    assert!(root.is_dir(), "reader teardown preserves the supplied root");

    // A named holder must match the complete OS identity, not just a PID.
    // These are claimed-marker mutations, not simulated OS observations.
    let runtime = RuntimeDirectory::supplied(&root);
    assert_ne!(breadcrumb.process_start.0, 0);
    for (label, claimed) in [
        (
            "zero PID",
            ReaderBreadcrumb {
                process_id: 0,
                ..breadcrumb.clone()
            },
        ),
        (
            "invalid PID",
            ReaderBreadcrumb {
                process_id: u32::MAX,
                ..breadcrumb.clone()
            },
        ),
        (
            "zero start",
            ReaderBreadcrumb {
                process_start: contextdb_core::read_contract::ProcessStartIdentity(0),
                ..breadcrumb.clone()
            },
        ),
        (
            "changed start",
            ReaderBreadcrumb {
                process_start: contextdb_core::read_contract::ProcessStartIdentity(
                    breadcrumb.process_start.0 ^ 1,
                ),
                ..breadcrumb.clone()
            },
        ),
        (
            "empty name",
            ReaderBreadcrumb {
                process_name: String::new(),
                ..breadcrumb.clone()
            },
        ),
        (
            "control name",
            ReaderBreadcrumb {
                process_name: "reader\x1b[2J".into(),
                ..breadcrumb.clone()
            },
        ),
        (
            "invented name",
            ReaderBreadcrumb {
                process_name: format!("unobserved-{}", breadcrumb.process_name),
                ..breadcrumb.clone()
            },
        ),
    ] {
        let guard = ReaderBreadcrumbGuard::create(&store, &root, Some(&claimed))
            .expect("publish a trusted locked negative control");
        let (count, named) = observed_reader_holders(&store, &runtime)
            .expect("inspect the real locked file and OS identity");
        assert_eq!(count, 1, "{label}: an unverified holder still counts");
        assert!(
            named.is_empty(),
            "{label}: must disclose no identity: {named:?}"
        );
        drop(guard);
    }
    let anonymous = ReaderBreadcrumbGuard::create(&store, &root, None)
        .expect("publish the production anonymous marker");
    assert_eq!(
        observed_reader_holders(&store, &runtime).unwrap(),
        (1, vec![])
    );
    drop(anonymous);

    #[cfg(unix)]
    {
        use std::os::unix::fs::{PermissionsExt as _, symlink};
        let guard = ReaderBreadcrumbGuard::create(&store, &root, Some(&breadcrumb))
            .expect("publish the trusted-file positive control");
        let path = &guard.path;
        assert!(open_trusted_reader_breadcrumb(path).is_ok());
        for mode in [0o400, 0o640, 0o660, 0o644, 0o1600] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
            assert!(
                open_trusted_reader_breadcrumb(path).is_err(),
                "mode {mode:o}"
            );
            assert!(
                observed_reader_holders(&store, &runtime)
                    .unwrap()
                    .1
                    .is_empty()
            );
        }
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let alias = directory.path().join("extra-link");
        std::fs::hard_link(path, &alias).unwrap();
        assert!(open_trusted_reader_breadcrumb(path).is_err());
        assert!(
            observed_reader_holders(&store, &runtime)
                .unwrap()
                .1
                .is_empty()
        );
        std::fs::remove_file(&alias).unwrap();

        let displaced = directory.path().join("original-marker");
        std::fs::rename(path, &displaced).unwrap();
        symlink(&displaced, path).unwrap();
        assert!(open_trusted_reader_breadcrumb(path).is_err());
        assert!(
            observed_reader_holders(&store, &runtime)
                .unwrap()
                .1
                .is_empty()
        );
        std::fs::remove_file(path).unwrap();
        std::fs::rename(&displaced, path).unwrap();
        assert_eq!(
            observed_reader_holders(&store, &runtime).unwrap(),
            (1, vec![breadcrumb.clone()]),
            "restoring the same trusted locked inode restores the verified identity"
        );
        drop(guard);
    }
    assert_eq!(
        observed_reader_holders(&store, &runtime).unwrap(),
        (0, vec![])
    );

    #[cfg(target_os = "macos")]
    {
        // Decode actual libc fields; do not reproduce the Darwin ABI.
        // None bytes means a buffer with no in-bounds NUL terminator.
        let fields: &[(&str, Option<&[u8]>, Option<&str>)] = &[
            ("safe", Some(b"native-reader"), Some("native-reader")),
            ("empty", Some(b""), None),
            ("non UTF-8", Some(b"\xff"), None),
            ("non ASCII UTF-8", Some("é".as_bytes()), None),
            ("escape", Some(b"r\x1b[2J"), None),
            ("newline", Some(b"r\n"), None),
            ("DEL", Some(b"r\x7f"), None),
            ("slash", Some(b"r/x"), None),
            ("backslash", Some(b"r\\x"), None),
            ("unterminated", None, None),
            (
                "different safe",
                Some(b"other-reader"),
                Some("other-reader"),
            ),
        ];
        let fill = |field: &mut [libc::c_char], bytes: Option<&[u8]>| {
            field.fill(b'X' as libc::c_char);
            if let Some(bytes) = bytes {
                assert!(bytes.len() < field.len());
                for (dst, src) in field.iter_mut().zip(bytes) {
                    *dst = *src as libc::c_char;
                }
                field[bytes.len()] = 0;
            }
        };
        for &(name_label, name_bytes, name_expected) in fields {
            for &(comm_label, comm_bytes, comm_expected) in fields {
                // SAFETY: libc's record consists solely of integer fields.
                let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
                fill(&mut info.pbi_name, name_bytes);
                fill(&mut info.pbi_comm, comm_bytes);
                assert_eq!(
                    macos_reader_process_display_name(&info).as_deref(),
                    name_expected.or(comm_expected),
                    "name={name_label}, comm={comm_label}"
                );
            }
        }
        for pid in [0, i32::MAX as u32 + 1, u32::MAX] {
            let error = macos_process_info(pid).expect_err("invalid PID must fail");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        }
    }
    #[cfg(target_os = "macos")]
    native_observation_rows::run(&store, &root);
}

// Rows of the existing writer/reader identity proof, using only the approved
// raw-call seam. This module is absent from Linux and production builds.
#[cfg(target_os = "macos")]
mod native_observation_rows {
    use super::super::macos_process_info_observation::{Observation, counts, with_observations};
    use super::*;
    use contextdb_core::read_contract::ProcessStartIdentity;
    use std::os::unix::fs::MetadataExt as _;

    #[derive(Clone)]
    struct Raw {
        pid: u32,
        sec: u64,
        usec: u64,
        name: Option<Vec<u8>>,
        comm: Option<Vec<u8>>,
        written: libc::c_int,
        errno: libc::c_int,
    }

    fn stable(pid: u32) -> Raw {
        Raw {
            pid,
            sec: 1,
            usec: 2,
            name: Some(b"native-reader".to_vec()),
            comm: Some(b"native-reader".to_vec()),
            written: libc::c_int::try_from(std::mem::size_of::<libc::proc_bsdinfo>()).unwrap(),
            errno: 0,
        }
    }

    fn fill(field: &mut [libc::c_char], value: &Option<Vec<u8>>) {
        field.fill(b'X' as libc::c_char);
        if let Some(bytes) = value {
            assert!(bytes.len() < field.len());
            for (dst, src) in field.iter_mut().zip(bytes) {
                *dst = *src as libc::c_char;
            }
            field[bytes.len()] = 0;
        }
    }

    fn queue(requested: u32, raws: &[Raw]) -> Vec<Observation> {
        raws.iter()
            .enumerate()
            .map(|(index, raw)| {
                // SAFETY: the real libc record contains only integer fields.
                let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
                info.pbi_pid = raw.pid;
                info.pbi_start_tvsec = raw.sec;
                info.pbi_start_tvusec = raw.usec;
                fill(&mut info.pbi_name, &raw.name);
                fill(&mut info.pbi_comm, &raw.comm);
                Observation {
                    ordinal: index + 1,
                    pid: libc::c_int::try_from(requested).unwrap(),
                    info,
                    written: raw.written,
                    errno: raw.errno,
                }
            })
            .collect()
    }

    fn expected(pid: u32, packed: u64, name: &str) -> ReaderBreadcrumb {
        ReaderBreadcrumb {
            process_id: pid,
            process_start: ProcessStartIdentity(packed),
            process_name: name.into(),
        }
    }

    fn descriptor(guard: &ReaderBreadcrumbGuard) -> (u64, u64) {
        let held = guard.file.as_ref().unwrap().metadata().unwrap();
        let (opened, path) = open_trusted_reader_breadcrumb(&guard.path).unwrap();
        let opened = opened.metadata().unwrap();
        assert_eq!((opened.dev(), opened.ino()), (held.dev(), held.ino()));
        assert_eq!((path.dev(), path.ino()), (held.dev(), held.ino()));
        assert_eq!(held.nlink(), 1);
        assert_eq!(held.mode() & 0o7777, 0o600);
        (held.dev(), held.ino())
    }

    fn positive(
        store: &Path,
        root: &Path,
        guard: &ReaderBreadcrumbGuard,
        raws: &[Raw],
        want: &ReaderBreadcrumb,
        inode: (u64, u64),
    ) {
        assert_eq!(descriptor(guard), inode);
        with_observations(queue(want.process_id, raws), || {
            assert_eq!(current_reader_breadcrumb().unwrap(), *want);
            assert_eq!(counts(), (2, 2));
        });
        with_observations(queue(want.process_id, raws), || {
            assert_eq!(
                observed_reader_holders(store, &RuntimeDirectory::supplied(root)).unwrap(),
                (1, vec![want.clone()])
            );
            assert_eq!(counts(), (2, 2));
        });
        assert_eq!(descriptor(guard), inode);
    }

    // Every failure is driven through BOTH the creator and the real locked
    // verifier. The restored control uses the SAME held file descriptor.
    fn reject(
        store: &Path,
        root: &Path,
        label: &str,
        requested: u32,
        raws: &[Raw],
        errno: Option<i32>,
        invalid_input: bool,
    ) {
        let pid = std::process::id();
        let want = expected(pid, 1_000_002, "native-reader");
        let claim = ReaderBreadcrumb {
            process_id: requested,
            ..want.clone()
        };
        let mut guard = ReaderBreadcrumbGuard::create(store, root, Some(&claim)).unwrap();
        let inode = descriptor(&guard);
        let observations = || {
            if invalid_input {
                vec![]
            } else {
                queue(requested, raws)
            }
        };
        with_observations(observations(), || {
            let error = if requested == std::process::id() {
                current_reader_breadcrumb()
            } else {
                macos_reader_breadcrumb(requested)
            }
            .unwrap_err();
            if let Some(errno) = errno {
                assert_eq!(error.raw_os_error(), Some(errno), "{label}");
            } else {
                assert_eq!(
                    error.kind(),
                    if invalid_input {
                        std::io::ErrorKind::InvalidInput
                    } else {
                        std::io::ErrorKind::InvalidData
                    },
                    "{label}"
                );
            }
            assert_eq!(counts(), (raws.len(), raws.len()), "{label}: creator");
        });
        with_observations(observations(), || {
            assert_eq!(
                observed_reader_holders(store, &RuntimeDirectory::supplied(root)).unwrap(),
                (1, vec![]),
                "{label}: rejected identity must remain anonymous"
            );
            assert_eq!(counts(), (raws.len(), raws.len()), "{label}: verifier");
        });
        assert_eq!(descriptor(&guard), inode);
        // Invalid caller controls have a different claimed PID. Replace only
        // the bytes on the held inode, using the production encoder.
        if requested != pid {
            let file = guard.file.as_mut().unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.set_len(0).unwrap();
            file.write_all(&encode_reader_breadcrumb(&want).unwrap())
                .unwrap();
            file.sync_data().unwrap();
        }
        let good = stable(pid);
        positive(store, root, &guard, &[good.clone(), good], &want, inode);
        drop(guard);
        assert_eq!(
            observed_reader_holders(store, &RuntimeDirectory::supplied(root)).unwrap(),
            (0, vec![]),
            "{label}: release"
        );
    }

    fn both_reads(store: &Path, root: &Path, label: &str, bad: Raw, errno: Option<i32>) {
        let good = stable(std::process::id());
        // Validate raw-record failures directly as well: otherwise a later
        // changed-tuple rejection could mask a broken arithmetic guard.
        if !label.starts_with("invalid-name/") {
            with_observations(queue(good.pid, std::slice::from_ref(&bad)), || {
                let error = macos_process_info(good.pid).expect_err("raw record must be rejected");
                if let Some(errno) = errno {
                    assert_eq!(error.raw_os_error(), Some(errno), "{label}");
                } else {
                    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData, "{label}");
                }
                assert_eq!(counts(), (1, 1));
            });
        }
        // Second-read cases precede first-read cases so accepting an invalid
        // selected name fails an outcome assertion before queue exhaustion.
        reject(
            store,
            root,
            &format!("{label}/second"),
            good.pid,
            &[good.clone(), bad.clone()],
            errno,
            false,
        );
        reject(
            store,
            root,
            &format!("{label}/first"),
            good.pid,
            std::slice::from_ref(&bad),
            errno,
            false,
        );
    }

    pub(super) fn run(store: &Path, root: &Path) {
        let pid = std::process::id();
        assert!(pid > 1 && pid < i32::MAX as u32);
        let good = stable(pid);
        // Exact stable positives run before negatives; a skipped recheck or
        // invented constant name must fail an identity/count assertion.
        for name in ["native-reader", "other-reader"] {
            let raw = Raw {
                name: Some(name.as_bytes().to_vec()),
                comm: Some(name.as_bytes().to_vec()),
                ..good.clone()
            };
            let want = expected(pid, 1_000_002, name);
            let guard = ReaderBreadcrumbGuard::create(store, root, Some(&want)).unwrap();
            positive(
                store,
                root,
                &guard,
                &[raw.clone(), raw],
                &want,
                descriptor(&guard),
            );
        }
        // Raw failure is the Cartesian product, not representative samples.
        for written in [-1, 0] {
            for errno in [libc::EACCES, libc::ESRCH] {
                let bad = Raw {
                    written,
                    errno,
                    ..good.clone()
                };
                both_reads(
                    store,
                    root,
                    &format!("raw/{written}/errno/{errno}"),
                    bad,
                    Some(errno),
                );
            }
        }
        let size = good.written;
        for written in (1..size).chain(std::iter::once(size + 1)) {
            both_reads(
                store,
                root,
                &format!("length/{written}"),
                Raw {
                    written,
                    ..good.clone()
                },
                None,
            );
        }
        for returned in [0, pid - 1, pid + 1] {
            both_reads(
                store,
                root,
                &format!("returned-pid/{returned}"),
                Raw {
                    pid: returned,
                    ..good.clone()
                },
                None,
            );
        }
        for requested in [0, i32::MAX as u32 + 1, u32::MAX] {
            // Leave a result ready for the specific forbidden truncated call.
            // The valid case makes zero production calls; drain it only
            // after asserting that, so queue exhaustion cannot mask a failure.
            let mut pending = queue(
                pid,
                &[Raw {
                    pid: requested,
                    ..good.clone()
                }],
            );
            pending[0].pid = requested as libc::c_int;
            with_observations(pending, || {
                let error = macos_process_info(requested).expect_err("caller PID must be rejected");
                assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
                assert_eq!(counts(), (0, 0), "production must reject before the FFI");
                let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
                assert!(
                    super::super::macos_process_info_observation::observe(
                        requested as libc::c_int,
                        &mut info
                    )
                    .is_some()
                );
                assert_eq!(
                    counts(),
                    (1, 1),
                    "only the explicit test drain consumed the result"
                );
            });
            reject(
                store,
                root,
                &format!("caller-pid/{requested}"),
                requested,
                &[],
                None,
                true,
            );
        }
        for (label, sec, usec) in [
            ("zero-start", 0, 0),
            ("microsecond-limit", 1, 1_000_000),
            ("microsecond-max", 1, u64::MAX),
            ("multiply-overflow", u64::MAX / 1_000_000 + 1, 0),
            (
                "add-overflow",
                u64::MAX / 1_000_000,
                u64::MAX % 1_000_000 + 1,
            ),
            // Wrapping exactly at MAX+1 becomes zero and is also rejected
            // by the nonzero guard. MAX+2 isolates checked-add itself.
            (
                "add-overflow-nonzero",
                u64::MAX / 1_000_000,
                u64::MAX % 1_000_000 + 2,
            ),
        ] {
            both_reads(
                store,
                root,
                label,
                Raw {
                    sec,
                    usec,
                    ..good.clone()
                },
                None,
            );
        }
        for (sec, usec, packed) in [
            (0, 1, 1),
            (1, 0, 1_000_000),
            (1, 999_999, 1_999_999),
            (u64::MAX / 1_000_000, u64::MAX % 1_000_000, u64::MAX),
        ] {
            let raw = Raw {
                sec,
                usec,
                ..good.clone()
            };
            let want = expected(pid, packed, "native-reader");
            let guard = ReaderBreadcrumbGuard::create(store, root, Some(&want)).unwrap();
            positive(
                store,
                root,
                &guard,
                &[raw.clone(), raw],
                &want,
                descriptor(&guard),
            );
        }
        for (label, after) in [
            (
                "changed-start",
                Raw {
                    usec: 3,
                    ..good.clone()
                },
            ),
            (
                "changed-pid",
                Raw {
                    pid: pid + 1,
                    ..good.clone()
                },
            ),
            (
                "changed-selected-name",
                Raw {
                    name: Some(b"other-reader".to_vec()),
                    ..good.clone()
                },
            ),
        ] {
            reject(store, root, label, pid, &[good.clone(), after], None, false);
        }
        // All nine invalid fixtures from the independent name-field table.
        let invalid: &[(&str, Option<&[u8]>)] = &[
            ("empty", Some(b"")),
            ("non-utf8", Some(b"\xff")),
            ("non-ascii", Some("é".as_bytes())),
            ("escape", Some(b"r\x1b[2J")),
            ("newline", Some(b"r\n")),
            ("del", Some(b"r\x7f")),
            ("slash", Some(b"r/x")),
            ("backslash", Some(b"r\\x")),
            ("unterminated", None),
        ];
        for &(label, bytes) in invalid {
            let value = bytes.map(<[u8]>::to_vec);
            both_reads(
                store,
                root,
                &format!("invalid-name/{label}"),
                Raw {
                    name: value.clone(),
                    comm: value,
                    ..good.clone()
                },
                None,
            );
            let recovered = Raw {
                name: bytes.map(<[u8]>::to_vec),
                ..good.clone()
            };
            let want = expected(pid, 1_000_002, "native-reader");
            let guard = ReaderBreadcrumbGuard::create(store, root, Some(&want)).unwrap();
            let inode = descriptor(&guard);
            positive(
                store,
                root,
                &guard,
                &[recovered.clone(), recovered.clone()],
                &want,
                inode,
            );
            // Selected name is stable even when its source buffer changes.
            positive(
                store,
                root,
                &guard,
                &[good.clone(), recovered.clone()],
                &want,
                inode,
            );
            positive(
                store,
                root,
                &guard,
                &[recovered, good.clone()],
                &want,
                inode,
            );
        }
        // A safe primary wins over a different safe comm, observed exactly.
        let primary = Raw {
            comm: Some(b"other-reader".to_vec()),
            ..good.clone()
        };
        let want = expected(pid, 1_000_002, "native-reader");
        let guard = ReaderBreadcrumbGuard::create(store, root, Some(&want)).unwrap();
        positive(
            store,
            root,
            &guard,
            &[primary.clone(), primary],
            &want,
            descriptor(&guard),
        );
        drop(guard);
        for (label, claim) in [
            ("claimed-start", expected(pid, 1_000_003, "native-reader")),
            ("claimed-zero-start", expected(pid, 0, "native-reader")),
            ("claimed-name", expected(pid, 1_000_002, "forged-reader")),
        ] {
            let mut guard = ReaderBreadcrumbGuard::create(store, root, Some(&claim)).unwrap();
            let inode = descriptor(&guard);
            with_observations(queue(pid, &[good.clone(), good.clone()]), || {
                // Creator succeeds with the observed tuple, not the claim.
                assert_eq!(macos_reader_breadcrumb(pid).unwrap(), want, "{label}");
                assert_eq!(counts(), (2, 2));
            });
            with_observations(queue(pid, &[good.clone(), good.clone()]), || {
                assert_eq!(
                    observed_reader_holders(store, &RuntimeDirectory::supplied(root)).unwrap(),
                    (1, vec![]),
                    "{label}"
                );
                assert_eq!(counts(), (2, 2));
            });
            let file = guard.file.as_mut().unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.set_len(0).unwrap();
            file.write_all(&encode_reader_breadcrumb(&want).unwrap())
                .unwrap();
            file.sync_data().unwrap();
            positive(
                store,
                root,
                &guard,
                &[good.clone(), good.clone()],
                &want,
                inode,
            );
        }
        // Claimed PID is also the verifier's query PID; an observed different
        // PID is rejected on the first native record, before tuple equality.
        reject(
            store,
            root,
            "claimed-pid",
            pid + 1,
            std::slice::from_ref(&good),
            None,
            false,
        );
        let guard = ReaderBreadcrumbGuard::create(store, root, None).unwrap();
        with_observations(vec![], || {
            assert_eq!(
                observed_reader_holders(store, &RuntimeDirectory::supplied(root)).unwrap(),
                (1, vec![])
            );
            assert_eq!(counts(), (0, 0));
        });
        drop(guard);
        scope_controls(pid, &good);
    }

    fn panic_text(panic: Box<dyn std::any::Any + Send>) -> String {
        if let Some(text) = panic.downcast_ref::<String>() {
            text.clone()
        } else if let Some(text) = panic.downcast_ref::<&str>() {
            (*text).into()
        } else {
            panic!("unexpected panic payload")
        }
    }

    fn scope_controls(pid: u32, good: &Raw) {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        // SAFETY: Darwin's accessor returns this thread's live errno slot.
        let errno = unsafe { *libc::__error() };
        with_observations(queue(pid, &[good.clone(), good.clone()]), || {
            macos_process_info(pid).unwrap();
            assert_eq!(counts(), (1, 1));
            let before = unsafe { *libc::__error() };
            let bad = Raw {
                written: -1,
                errno: libc::EACCES,
                ..good.clone()
            };
            with_observations(queue(pid, &[bad]), || {
                assert_eq!(
                    macos_process_info(pid)
                        .expect_err("raw failure")
                        .raw_os_error(),
                    Some(libc::EACCES)
                );
                assert_eq!(counts(), (1, 1));
            });
            assert_eq!(unsafe { *libc::__error() }, before);
            assert_eq!(counts(), (1, 1));
            let panic = catch_unwind(AssertUnwindSafe(|| {
                with_observations(queue(pid, std::slice::from_ref(good)), || {
                    unsafe { *libc::__error() = libc::ESRCH };
                    panic!("native scope original panic");
                });
            }))
            .unwrap_err();
            assert_eq!(panic_text(panic), "native scope original panic");
            assert_eq!(unsafe { *libc::__error() }, before);
            assert_eq!(counts(), (1, 1));
            macos_process_info(pid).unwrap();
            assert_eq!(counts(), (2, 2));
        });
        assert_eq!(unsafe { *libc::__error() }, errno);
        for mode in ["unused", "ordinal", "pid", "exhausted"] {
            let mut observations = queue(pid, std::slice::from_ref(good));
            match mode {
                "ordinal" => observations[0].ordinal = 2,
                "pid" => observations[0].pid += 1,
                "exhausted" => observations.clear(),
                _ => {}
            }
            let panic = catch_unwind(AssertUnwindSafe(|| {
                with_observations(observations, || {
                    if mode != "unused" {
                        let rejection = catch_unwind(AssertUnwindSafe(|| {
                            let _ = macos_process_info(pid);
                        }));
                        assert!(
                            panic_text(rejection.unwrap_err())
                                .contains("native observation mismatch")
                        );
                        assert_eq!(counts(), (1, 0));
                        // Catching the call panic cannot make scope exit pass.
                    }
                });
            }))
            .unwrap_err();
            let message = panic_text(panic);
            assert!(
                message.contains(if mode == "unused" {
                    "unused results"
                } else {
                    "sequence was rejected"
                }),
                "{mode}: {message}"
            );
            assert_eq!(unsafe { *libc::__error() }, errno);
            // A failed scope must leave no stale override behind. Calling
            // observe directly avoids executing a real FFI call here.
            let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
            assert!(
                super::super::macos_process_info_observation::observe(
                    pid as libc::c_int,
                    &mut info
                )
                .is_none()
            );
            with_observations(queue(pid, std::slice::from_ref(good)), || {
                macos_process_info(pid).unwrap();
                assert_eq!(counts(), (1, 1));
            });
        }
    }
}

/// A real store nobody is holding, released at once.
fn idle_store(directory: &Path, name: &str) -> PathBuf {
    let store = directory.join(name);
    drop(RedbPersistence::create(&store).expect("claim and release a real store"));
    store
}

/// The wait answers immediately when nobody holds the store. The verdict
/// comes from the readers themselves, so a deployment's runtime directory
/// neither supplies it nor withholds it.
#[test]
fn a_store_nobody_holds_is_released_at_once() {
    let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
    let root = supplied_root(directory.path(), "supplied-runtime");
    let store = idle_store(directory.path(), "packaged.db");
    let resolved = RuntimeDirectory::supplied(&root);
    let stop = OwnerReadCancellation::default();

    assert!(
        matches!(
            wait_for_reader_release(&store, Some(&resolved), &stop),
            ReaderReleaseWait::Released
        ),
        "no reader is holding this store, so there is nothing to wait for"
    );
}

/// A pathname that is not a store has no companion beside it, so there is
/// nowhere for a reader of it to record a hold and nothing this wait can
/// observe. A caller about to take it is told that, never `Released`.
#[test]
fn a_pathname_with_no_companion_is_unobservable_not_released() {
    let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
    let root = supplied_root(directory.path(), "supplied-runtime");
    let store = directory.path().join("not-a-store.db");
    std::fs::write(&store, b"").expect("plant a pathname that is not a store");
    let resolved = RuntimeDirectory::supplied(&root);
    let stop = OwnerReadCancellation::default();

    assert!(
        matches!(
            wait_for_reader_release(&store, Some(&resolved), &stop),
            ReaderReleaseWait::Unobservable(_)
        ),
        "a pathname with no coordination artifact beside it is not a store with no readers"
    );
}

/// The door a packaged consumer can actually reach: it holds the runtime
/// ROOT its deployment named, not a directory this crate resolved, and it
/// still reaches the wait below it and answers for the store.
#[test]
fn a_supplied_root_still_reaches_the_wait_below_it() {
    let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
    let root = supplied_root(directory.path(), "supplied-runtime");
    let store = idle_store(directory.path(), "packaged.db");
    let stop = OwnerReadCancellation::default();

    assert!(
        matches!(
            wait_for_reader_release_in_runtime_dir(&store, Some(&root), &stop),
            ReaderReleaseWait::Released
        ),
        "the root-taking door resolves what it was given and answers for the store"
    );
}

/// A root this process cannot read says nothing about this store's
/// readers. Where the owner's channel lives is a different question, and a
/// caller waiting on a store nobody is reading is answered from the store.
#[test]
fn a_root_that_cannot_be_read_does_not_decide_the_answer() {
    let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
    let store = idle_store(directory.path(), "packaged.db");
    let absent = directory.path().join("no-such-runtime-directory");
    let stop = OwnerReadCancellation::default();

    assert!(
        matches!(
            wait_for_reader_release_in_runtime_dir(&store, Some(&absent), &stop),
            ReaderReleaseWait::Released
        ),
        "nobody is holding this store, and an unreadable channel root is not a reason to \
             refuse to say so"
    );
}

/// Without a supplied root the new door is the old one: the platform's own
/// runtime location, same answer.
#[test]
fn no_supplied_root_is_the_ordinary_wait() {
    let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
    let store = directory.path().join("packaged.db");
    std::fs::write(&store, b"").expect("the store this deployment names exists");
    let stop = OwnerReadCancellation::default();
    stop.cancel();

    assert!(
        matches!(
            wait_for_reader_release_in_runtime_dir(&store, None, &stop),
            ReaderReleaseWait::Stopped
        ),
        "the root-taking door carries the same stop contract as the one it delegates to"
    );
}

/// A caller that has already stopped is answered at once, with no wait.
#[test]
fn a_stopped_wait_answers_without_waiting() {
    let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
    let root = supplied_root(directory.path(), "supplied-runtime");
    let store = directory.path().join("packaged.db");
    std::fs::write(&store, b"").expect("the store this deployment names exists");
    let resolved = RuntimeDirectory::supplied(&root);
    let stop = OwnerReadCancellation::default();
    stop.cancel();

    assert!(
        matches!(
            wait_for_reader_release(&store, Some(&resolved), &stop),
            ReaderReleaseWait::Stopped
        ),
        "an interrupt is as prompt as a release"
    );
}
