//! Regression proofs for the reader registry's filesystem trust boundary.
use super::*;
use crate::runtime_directory::prepare_fallback_reader_runtime;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

#[test]
fn fallback_reader_registry_refuses_unsafe_ancestors_across_all_consumers() {
    use crate::runtime_directory::private_primary_group::ancestor_rule;
    for (label, directory_gid, primary_gid, group_name, members, expected) in [
        ("empty private group", 1000, 1000, "reader", vec![], Ok(())),
        (
            "self-only private group",
            1000,
            1000,
            "reader",
            vec!["reader"],
            Ok(()),
        ),
        (
            "another member",
            1000,
            1000,
            "reader",
            vec!["other"],
            Err("another member other"),
        ),
        (
            "self and another member",
            1000,
            1000,
            "reader",
            vec!["reader", "other"],
            Err("another member other"),
        ),
        (
            "differently named primary group",
            1000,
            1000,
            "shared",
            vec![],
            Err("expected login name reader"),
        ),
        (
            "supplementary group",
            1001,
            1000,
            "reader",
            vec![],
            Err("expected primary group 1000"),
        ),
        (
            "effective gid differs from passwd primary",
            1000,
            1001,
            "reader",
            vec!["reader"],
            Err("expected primary group 1001"),
        ),
    ] {
        let members = members.into_iter().map(str::to_owned).collect::<Vec<_>>();
        let actual = ancestor_rule(
            1000,
            directory_gid,
            primary_gid,
            "reader",
            group_name,
            &members,
        );
        match expected {
            Ok(()) => assert_eq!(actual, Ok(()), "{label}"),
            Err(reason) => {
                let error = actual.unwrap_err();
                assert!(error.contains(reason), "{label}: {error}");
                assert!(error.contains("owned by user 1000"), "{label}: {error}");
            }
        }
    }
    const DATA: &str = "CONTEXTDB_READER_ANCESTOR_DATA";
    if std::env::var_os(DATA).is_none() {
        for mask in ["0002", "0022"] {
            let fixture = tempfile::tempdir().unwrap();
            let ancestor = fixture.path().join("ancestor");
            let data = ancestor.join("data");
            std::fs::create_dir_all(&data).unwrap();
            for directory in [fixture.path(), ancestor.as_path(), data.as_path()] {
                std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))
                    .unwrap();
            }
            assert!(crate::test_process::rerun_alone(
                module_path!(),
                "fallback_reader_registry_refuses_unsafe_ancestors_across_all_consumers",
                &[
                    (DATA, data.as_os_str()),
                    ("XDG_DATA_HOME", data.as_os_str()),
                    (
                        "CONTEXTDB_READER_ANCESTOR_UMASK",
                        std::ffi::OsStr::new(mask)
                    )
                ],
            ));
        }
        return;
    }
    let mask = u32::from_str_radix(
        &std::env::var("CONTEXTDB_READER_ANCESTOR_UMASK").unwrap(),
        8,
    )
    .unwrap();
    // This child runs this exact test alone, so its process-wide umask is isolated.
    let _previous_mask = unsafe { libc::umask(mask) };
    let data = PathBuf::from(std::env::var_os(DATA).unwrap());
    let ancestor = data.parent().unwrap();
    let runtime = RuntimeDirectory::supplied(&data.join("contextdb/readers"));
    prepare_fallback_reader_runtime(&runtime).expect("a private ancestor chain is usable");
    let scratch_root = crate::scratch::user_scratch_root().unwrap();
    let scratch = crate::scratch::ScratchDirectory::create_in(&scratch_root).unwrap();
    for directory in [
        data.join("contextdb"),
        runtime.path().to_path_buf(),
        scratch_root,
        scratch.path().to_path_buf(),
    ] {
        assert_eq!(
            std::fs::metadata(&directory).unwrap().mode() & 0o777,
            0o700,
            "fresh directory {} stays private under umask {mask:04o}",
            directory.display()
        );
    }
    let leaf_mode = 0o750;
    std::fs::set_permissions(runtime.path(), std::fs::Permissions::from_mode(leaf_mode)).unwrap();
    let error = prepare_fallback_reader_runtime(&runtime).unwrap_err();
    assert!(
        error
            .to_string()
            .contains(&runtime.path().display().to_string())
    );
    assert!(error.to_string().contains("0750"));
    assert_eq!(
        std::fs::metadata(runtime.path()).unwrap().mode() & 0o777,
        leaf_mode
    );
    std::fs::set_permissions(runtime.path(), std::fs::Permissions::from_mode(0o700)).unwrap();

    let effective = nix::unistd::Uid::effective();
    let user = nix::unistd::User::from_uid(effective).unwrap().unwrap();
    let group = nix::unistd::Group::from_gid(user.gid).unwrap().unwrap();
    let metadata = std::fs::metadata(ancestor).unwrap();
    assert_eq!(metadata.uid(), effective.as_raw());
    let expected = ancestor_rule(
        metadata.uid(),
        metadata.gid(),
        user.gid.as_raw(),
        &user.name,
        &group.name,
        &group.mem,
    )
    .is_ok();
    std::fs::set_permissions(ancestor, std::fs::Permissions::from_mode(0o770)).unwrap();
    assert_eq!(
        prepare_fallback_reader_runtime(&runtime).is_ok(),
        expected,
        "the actual user's owned group-writable ancestor follows the resolved primary-group rule"
    );
    assert_eq!(std::fs::metadata(ancestor).unwrap().mode() & 0o777, 0o770);
    std::fs::set_permissions(ancestor, std::fs::Permissions::from_mode(0o700)).unwrap();
    let saved = runtime.path().with_file_name("saved-readers");
    std::fs::rename(runtime.path(), &saved).unwrap();
    std::os::unix::fs::symlink(&saved, runtime.path()).unwrap();
    let error = prepare_fallback_reader_runtime(&runtime).unwrap_err();
    assert!(
        error
            .to_string()
            .contains(&runtime.path().display().to_string())
    );
    assert!(error.to_string().contains("symbolic link"));
    assert_eq!(std::fs::metadata(&saved).unwrap().mode() & 0o777, 0o700);
    std::fs::remove_file(runtime.path()).unwrap();
    std::fs::rename(&saved, runtime.path()).unwrap();
    // Validate an actual unowned ancestor without creating anything below it.
    let unowned = if effective.as_raw() == 0 {
        let unowned = data.join("unowned-data");
        std::fs::create_dir(&unowned).unwrap();
        std::fs::set_permissions(&unowned, std::fs::Permissions::from_mode(0o700)).unwrap();
        use std::os::unix::ffi::OsStrExt as _;
        let name = std::ffi::CString::new(unowned.as_os_str().as_bytes()).unwrap();
        assert_eq!(
            unsafe { libc::chown(name.as_ptr(), 1, libc::gid_t::MAX) },
            0
        );
        unowned
    } else {
        PathBuf::from("/")
    };
    let unowned_mode = std::fs::metadata(&unowned).unwrap().mode() & 0o7777;
    assert_ne!(
        std::fs::metadata(&unowned).unwrap().uid(),
        effective.as_raw()
    );
    let error = crate::runtime_directory::validate_fallback_reader_runtime(
        &RuntimeDirectory::supplied(&unowned.join("contextdb/readers")),
    )
    .unwrap_err();
    if effective.as_raw() == 0 {
        use std::os::unix::ffi::OsStrExt as _;
        let name = std::ffi::CString::new(unowned.as_os_str().as_bytes()).unwrap();
        assert_eq!(
            unsafe { libc::chown(name.as_ptr(), effective.as_raw(), libc::gid_t::MAX) },
            0
        );
    }
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(error.to_string().contains(&unowned.display().to_string()));
    assert!(
        error
            .to_string()
            .contains(&format!("mode {unowned_mode:04o}"))
    );
    assert_eq!(
        std::fs::metadata(&unowned).unwrap().mode() & 0o7777,
        unowned_mode
    );
    let discovered = || {
        existing_reader_registry_roots(None)
            .iter()
            .any(|root| root.path() == runtime.path())
    };
    assert!(discovered());
    assert!(
        matches!(capture_reader_snapshot_registry_one((37, 41), &runtime),
        ReaderSnapshotRegistry::Complete(snapshots) if snapshots.is_empty())
    );

    // The data root and its parent both control replacement of the registry,
    // even while contextdb/readers retain owner-only modes.
    for unsafe_directory in [ancestor, data.as_path()] {
        std::fs::set_permissions(unsafe_directory, std::fs::Permissions::from_mode(0o777)).unwrap();
        let preparation = prepare_fallback_reader_runtime(&runtime);
        let discovery = discovered();
        let inventory = reader_inventory_directories(None);
        let captured = capture_reader_snapshot_registry_one((37, 41), &runtime);
        let preserved_mode = std::fs::metadata(unsafe_directory).unwrap().mode() & 0o777;
        std::fs::set_permissions(unsafe_directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let error = preparation.unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&unsafe_directory.display().to_string())
        );
        assert!(error.to_string().contains("0777"));
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(
            !discovery,
            "unsafe fallback must not be advertised to readers"
        );
        assert!(
            inventory.iter().any(|root| root.path() == runtime.path()),
            "an unsafe existing fallback must remain visible to the writer's fail-closed inventory"
        );
        assert!(
            matches!(captured, ReaderSnapshotRegistry::Indeterminate(path)
            if path == runtime.path())
        );
        assert_eq!(
            preserved_mode, 0o777,
            "validation must not chmod the consumer's data ancestors"
        );
        assert!(discovered(), "repairing the ancestor restores discovery");
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
mod linux_syscall_proofs {
    use super::*;
    use std::io::{Read as _, Write as _};
    use std::os::unix::net::{UnixListener, UnixStream};

    const TRACE_SOCKET: &str = "CONTEXTDB_READER_PRIVACY_TRACE_SOCKET";
    const FIXTURE: &str = "CONTEXTDB_READER_PRIVACY_FIXTURE";

    fn trace_this_test_thread() {
        let mut connection = UnixStream::connect(std::env::var_os(TRACE_SOCKET).unwrap()).unwrap();
        let thread = unsafe { libc::syscall(libc::SYS_gettid) } as libc::pid_t;
        // The parent traces only this test thread. No credentials are changed,
        // and no process outside the task's own child is attached.
        let result = unsafe { libc::ptrace(libc::PTRACE_TRACEME, 0, 0, 0) };
        let error = if result == -1 {
            std::io::Error::last_os_error().raw_os_error().unwrap_or(-1)
        } else {
            0
        };
        connection.write_all(&thread.to_ne_bytes()).unwrap();
        connection.write_all(&error.to_ne_bytes()).unwrap();
        assert_eq!(
            error, 0,
            "ptrace TRACEME must be supported for this deterministic regression proof"
        );
        unsafe {
            libc::raise(libc::SIGSTOP);
        }
    }

    struct TraceChild(std::process::Child);
    impl Drop for TraceChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn checked_ptrace(request: libc::c_uint, thread: libc::pid_t, data: *mut libc::c_void) {
        assert_ne!(
            unsafe { libc::ptrace(request, thread, std::ptr::null_mut::<libc::c_void>(), data) },
            -1,
            "ptrace request {request}: {}",
            std::io::Error::last_os_error()
        );
    }

    fn trace_child(
        test: &str,
        fixture: &Path,
        mut syscall_exit: impl FnMut(&mut libc::user_regs_struct),
    ) {
        let socket_directory = tempfile::tempdir().unwrap();
        let socket = socket_directory.path().join("trace.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let name = format!("{}::{test}", module_path!().split_once("::").unwrap().1);
        let mut child = TraceChild(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &name, "--nocapture", "--test-threads=1"])
                .env(TRACE_SOCKET, &socket)
                .env(FIXTURE, fixture)
                .env_remove("HOME")
                .env_remove("XDG_DATA_HOME")
                .spawn()
                .unwrap(),
        );
        let (mut connection, _) = listener.accept().unwrap();
        let mut bytes = [0; 4];
        connection.read_exact(&mut bytes).unwrap();
        let thread = libc::pid_t::from_ne_bytes(bytes);
        connection.read_exact(&mut bytes).unwrap();
        let trace_error = i32::from_ne_bytes(bytes);
        assert_eq!(
            trace_error, 0,
            "deterministic regression cannot run: ptrace TRACEME errno {trace_error}"
        );
        let mut status = 0;
        assert_eq!(
            unsafe { libc::waitpid(thread, &mut status, libc::__WALL) },
            thread
        );
        assert!(libc::WIFSTOPPED(status));
        checked_ptrace(
            libc::PTRACE_SETOPTIONS,
            thread,
            libc::PTRACE_O_TRACESYSGOOD as *mut libc::c_void,
        );
        loop {
            checked_ptrace(libc::PTRACE_SYSCALL, thread, std::ptr::null_mut());
            assert_eq!(
                unsafe { libc::waitpid(thread, &mut status, libc::__WALL) },
                thread
            );
            if libc::WIFEXITED(status) || libc::WIFSIGNALED(status) {
                break;
            }
            assert!(libc::WIFSTOPPED(status));
            assert_eq!(
                libc::WSTOPSIG(status),
                libc::SIGTRAP | 0x80,
                "unexpected tracee signal; the proof must not silently skip a syscall"
            );
            let mut registers: libc::user_regs_struct = unsafe { std::mem::zeroed() };
            checked_ptrace(
                libc::PTRACE_GETREGS,
                thread,
                (&mut registers as *mut libc::user_regs_struct).cast(),
            );
            // Linux x86_64 reports -ENOSYS in rax at syscall entry.
            if registers.rax as i64 != -(libc::ENOSYS as i64) {
                syscall_exit(&mut registers);
                checked_ptrace(
                    libc::PTRACE_SETREGS,
                    thread,
                    (&mut registers as *mut libc::user_regs_struct).cast(),
                );
            }
        }
        assert!(
            child.0.wait().unwrap().success(),
            "the traced production-call proof failed"
        );
    }

    #[test]
    fn reader_directory_creation_does_not_chmod_a_replacement_symlink_target() {
        if std::env::var_os(TRACE_SOCKET).is_some() {
            let fixture = PathBuf::from(std::env::var_os(FIXTURE).unwrap());
            trace_this_test_thread();
            let result = create_reader_breadcrumb_directory(&fixture.join("created"));
            assert_eq!(
                std::fs::metadata(fixture.join("victim")).unwrap().mode() & 0o777,
                0o755,
                "normalizing the registry must never chmod a symlink target"
            );
            assert!(
                result.is_err(),
                "the replaced directory name must be refused"
            );
            return;
        }
        let fixture = tempfile::tempdir().unwrap();
        let created = fixture.path().join("created");
        let victim = fixture.path().join("victim");
        std::fs::create_dir(&victim).unwrap();
        std::fs::set_permissions(&victim, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut replaced = false;
        trace_child(
            "reader_directory_creation_does_not_chmod_a_replacement_symlink_target",
            fixture.path(),
            |registers| {
                if !replaced
                    && registers.rax == 0
                    && matches!(
                        registers.orig_rax as i64,
                        libc::SYS_mkdir | libc::SYS_mkdirat
                    )
                {
                    assert!(
                        created.is_dir(),
                        "the intercepted mkdir must be the production call"
                    );
                    std::fs::rename(&created, fixture.path().join("parked")).unwrap();
                    std::os::unix::fs::symlink(&victim, &created).unwrap();
                    replaced = true;
                }
            },
        );
        assert!(
            replaced,
            "the proof must replace the pathname after the production mkdir completes"
        );
    }

    #[test]
    fn reader_data_home_uses_effective_user_when_home_is_unset() {
        let effective = nix::unistd::Uid::effective();
        let synthetic_real = 2_000_000_001;
        if std::env::var_os(TRACE_SOCKET).is_some() {
            let expected_home = PathBuf::from(std::env::var_os(FIXTURE).unwrap());
            trace_this_test_thread();
            assert_eq!(
                nix::unistd::getuid().as_raw(),
                synthetic_real,
                "the trace must expose a distinct real UID to the production path"
            );
            assert_eq!(nix::unistd::Uid::effective(), effective);
            assert_eq!(
                crate::scratch::user_data_root(),
                Some(expected_home.join(".local/share")),
                "passwd fallback must resolve the effective user's home"
            );
            return;
        }
        assert_ne!(effective.as_raw(), synthetic_real);
        assert!(
            nix::unistd::User::from_uid(nix::unistd::Uid::from_raw(synthetic_real))
                .unwrap()
                .is_none(),
            "the synthetic real user must have no passwd home"
        );
        let expected_home = nix::unistd::User::from_uid(effective).unwrap().unwrap().dir;
        let mut real_uid_reads = 0;
        trace_child(
            "reader_data_home_uses_effective_user_when_home_is_unset",
            &expected_home,
            |registers| {
                if registers.orig_rax as i64 == libc::SYS_getuid {
                    registers.rax = synthetic_real as u64;
                    real_uid_reads += 1;
                }
            },
        );
        assert!(
            real_uid_reads > 0,
            "the proof must observe the distinct-real-UID syscall"
        );
    }
}
