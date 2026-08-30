use contextdb_core::read_contract::{ChannelAddress, LocalUserIdentity};
#[cfg(unix)]
use contextdb_engine::local_transport::UnixLocalCarrier;
#[cfg(target_os = "linux")]
use contextdb_engine::local_transport::linux_filesystem_type_is_local;
use contextdb_engine::local_transport::{
    ChannelPathFacts, ChannelPathViolation, LocalTransportError, OWNER_ONLY_MODE,
    ProcessRuntimeDirectoryEnvironment, ResolvedRuntimeDirectory, RuntimeDirectoryEnvironment,
    RuntimeDirectoryFacts, RuntimeDirectoryInspector, RuntimeDirectoryRequest,
    RuntimeDirectorySource, RuntimeRootViolation, SystemRuntimeDirectoryInspector,
    channel_socket_path, inspect_channel_path, opaque_channel_basename, prepare_runtime_directory,
    prepare_runtime_directory_with_environment, resolve_runtime_directory_with_environment,
    unix_socket_path_limit, validate_channel_path, validate_runtime_root,
};
use std::collections::BTreeMap;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Default)]
struct FactsInspector {
    entries: BTreeMap<PathBuf, RuntimeDirectoryFacts>,
}

impl FactsInspector {
    fn with(mut self, facts: RuntimeDirectoryFacts) -> Self {
        self.entries.insert(facts.requested_root.clone(), facts);
        self
    }
}

impl RuntimeDirectoryInspector for FactsInspector {
    fn inspect(&self, root: &Path) -> Result<RuntimeDirectoryFacts, LocalTransportError> {
        self.entries
            .get(root)
            .cloned()
            .ok_or(LocalTransportError::RuntimeRoot(
                RuntimeRootViolation::Unavailable,
            ))
    }
}

struct RuntimeEnvironment {
    xdg: Option<PathBuf>,
    effective_user: LocalUserIdentity,
    macos_temporary: Option<PathBuf>,
}

impl RuntimeDirectoryEnvironment for RuntimeEnvironment {
    fn xdg_runtime_directory(&self) -> Option<PathBuf> {
        self.xdg.clone()
    }

    fn effective_user_identity(&self) -> Result<LocalUserIdentity, LocalTransportError> {
        Ok(self.effective_user)
    }

    fn macos_user_temporary_directory(&self) -> Result<PathBuf, LocalTransportError> {
        self.macos_temporary
            .clone()
            .ok_or(LocalTransportError::RuntimeRoot(
                RuntimeRootViolation::Unavailable,
            ))
    }
}

fn facts(root: PathBuf, user: LocalUserIdentity) -> RuntimeDirectoryFacts {
    RuntimeDirectoryFacts {
        requested_root: root.clone(),
        resolved_root: root,
        is_absolute: true,
        is_local: true,
        is_directory: true,
        is_symbolic_link: false,
        owner: user,
        mode: OWNER_ONLY_MODE,
        writable: true,
        available: true,
    }
}

#[test]
fn local_channel_runtime_root_requires_the_owner_only_safe_shape() {
    let owner = LocalUserIdentity(7_101);
    let root = PathBuf::from("/runtime/contextdb-test");
    let valid = facts(root.clone(), owner);

    let mut cases = Vec::new();

    let mut relative = valid.clone();
    relative.is_absolute = false;
    cases.push((relative, RuntimeRootViolation::Relative));

    let mut non_local = valid.clone();
    non_local.is_local = false;
    cases.push((non_local, RuntimeRootViolation::NonLocal));

    let mut regular_file = valid.clone();
    regular_file.is_directory = false;
    cases.push((regular_file, RuntimeRootViolation::NotDirectory));

    let mut symbolic_link = valid.clone();
    symbolic_link.is_symbolic_link = true;
    symbolic_link.resolved_root = PathBuf::from("/elsewhere/contextdb-test");
    cases.push((symbolic_link, RuntimeRootViolation::SymbolicLink));

    let mut wrong_owner = valid.clone();
    wrong_owner.owner = LocalUserIdentity(owner.0 + 1);
    cases.push((wrong_owner, RuntimeRootViolation::WrongOwner));

    let mut broad_mode = valid.clone();
    broad_mode.mode = 0o755;
    cases.push((broad_mode, RuntimeRootViolation::InsecureMode));

    let mut read_only = valid.clone();
    read_only.writable = false;
    cases.push((read_only, RuntimeRootViolation::NotWritable));

    let mut absent = valid.clone();
    absent.available = false;
    cases.push((absent, RuntimeRootViolation::Unavailable));

    for (candidate, violation) in cases {
        assert_eq!(
            validate_runtime_root(&candidate, owner),
            Err(LocalTransportError::RuntimeRoot(violation)),
            "runtime root {:?} must be refused",
            candidate.requested_root
        );
    }
    validate_runtime_root(&valid, owner).expect("valid owner-only runtime root");
}

#[test]
#[cfg(target_os = "linux")]
fn local_channel_derives_linux_roots_without_a_caller_supplied_fallback_path() {
    let owner = LocalUserIdentity(7_102);
    let xdg = PathBuf::from("/run/user/7102");
    let per_user = PathBuf::from("/run/user/7102");
    let inspector = FactsInspector::default()
        .with(facts(xdg.clone(), owner))
        .with(facts(per_user.clone(), owner));

    let xdg_request = RuntimeDirectoryRequest {
        explicit_root: None,
        current_user: owner,
    };
    assert_eq!(
        resolve_runtime_directory_with_environment(
            &xdg_request,
            &inspector,
            &RuntimeEnvironment {
                xdg: Some(xdg.clone()),
                effective_user: owner,
                macos_temporary: None,
            },
        )
        .expect("XDG runtime resolution"),
        ResolvedRuntimeDirectory {
            path: xdg.join("contextdb"),
            source: RuntimeDirectorySource::Xdg,
        }
    );

    assert_eq!(
        resolve_runtime_directory_with_environment(
            &xdg_request,
            &inspector,
            &RuntimeEnvironment {
                xdg: None,
                effective_user: owner,
                macos_temporary: None,
            },
        )
        .expect("exact /run/user/<uid> fallback resolution"),
        ResolvedRuntimeDirectory {
            path: per_user.join("contextdb"),
            source: RuntimeDirectorySource::PerUserFallback,
        }
    );
}

#[test]
#[cfg(target_os = "linux")]
fn linux_runtime_resolution_and_preparation_fail_closed_for_every_invalid_native_root() {
    let owner = LocalUserIdentity(7_106);
    let xdg = PathBuf::from("/run/user/7106-xdg");
    let per_user = PathBuf::from("/run/user/7106");
    let temporary = PathBuf::from("/tmp/contextdb-forbidden-fallback");
    let request = RuntimeDirectoryRequest {
        explicit_root: None,
        current_user: owner,
    };
    let valid_xdg = facts(xdg.clone(), owner);
    let valid_per_user = facts(per_user.clone(), owner);
    let valid_temporary = facts(temporary.clone(), owner);

    let mut invalid_roots = Vec::new();
    let mut candidate = valid_xdg.clone();
    candidate.is_absolute = false;
    invalid_roots.push((candidate, RuntimeRootViolation::Relative));
    let mut candidate = valid_xdg.clone();
    candidate.is_local = false;
    invalid_roots.push((candidate, RuntimeRootViolation::NonLocal));
    let mut candidate = valid_xdg.clone();
    candidate.is_directory = false;
    invalid_roots.push((candidate, RuntimeRootViolation::NotDirectory));
    let mut candidate = valid_xdg.clone();
    candidate.is_symbolic_link = true;
    invalid_roots.push((candidate, RuntimeRootViolation::SymbolicLink));
    let mut candidate = valid_xdg.clone();
    candidate.owner = LocalUserIdentity(owner.0 + 1);
    invalid_roots.push((candidate, RuntimeRootViolation::WrongOwner));
    let mut candidate = valid_xdg.clone();
    candidate.mode = 0o755;
    invalid_roots.push((candidate, RuntimeRootViolation::InsecureMode));
    let mut candidate = valid_xdg.clone();
    candidate.writable = false;
    invalid_roots.push((candidate, RuntimeRootViolation::NotWritable));
    let mut candidate = valid_xdg;
    candidate.available = false;
    invalid_roots.push((candidate, RuntimeRootViolation::Unavailable));

    for (invalid_xdg, violation) in invalid_roots {
        let inspector = FactsInspector::default()
            .with(invalid_xdg)
            .with(valid_per_user.clone())
            .with(valid_temporary.clone());
        let environment = RuntimeEnvironment {
            xdg: Some(xdg.clone()),
            effective_user: owner,
            macos_temporary: Some(temporary.clone()),
        };
        let expected = Err(LocalTransportError::RuntimeRoot(violation));
        assert_eq!(
            resolve_runtime_directory_with_environment(&request, &inspector, &environment),
            expected.clone()
        );
        assert_eq!(
            prepare_runtime_directory_with_environment(&request, &inspector, &environment),
            expected,
            "an invalid present XDG root cannot fall through to /run/user or /tmp"
        );
    }

    let mut invalid_fallbacks = Vec::new();
    let mut candidate = valid_per_user.clone();
    candidate.is_absolute = false;
    invalid_fallbacks.push((candidate, RuntimeRootViolation::Relative));
    let mut candidate = valid_per_user.clone();
    candidate.is_local = false;
    invalid_fallbacks.push((candidate, RuntimeRootViolation::NonLocal));
    let mut candidate = valid_per_user.clone();
    candidate.is_directory = false;
    invalid_fallbacks.push((candidate, RuntimeRootViolation::NotDirectory));
    let mut candidate = valid_per_user.clone();
    candidate.is_symbolic_link = true;
    invalid_fallbacks.push((candidate, RuntimeRootViolation::SymbolicLink));
    let mut candidate = valid_per_user.clone();
    candidate.owner = LocalUserIdentity(owner.0 + 1);
    invalid_fallbacks.push((candidate, RuntimeRootViolation::WrongOwner));
    let mut candidate = valid_per_user.clone();
    candidate.mode = 0o755;
    invalid_fallbacks.push((candidate, RuntimeRootViolation::InsecureMode));
    let mut candidate = valid_per_user.clone();
    candidate.writable = false;
    invalid_fallbacks.push((candidate, RuntimeRootViolation::NotWritable));
    let mut candidate = valid_per_user;
    candidate.available = false;
    invalid_fallbacks.push((candidate, RuntimeRootViolation::Unavailable));

    for (invalid_fallback, violation) in invalid_fallbacks {
        let inspector = FactsInspector::default()
            .with(invalid_fallback)
            .with(valid_temporary.clone());
        let environment = RuntimeEnvironment {
            xdg: None,
            effective_user: owner,
            macos_temporary: Some(temporary.clone()),
        };
        let expected = Err(LocalTransportError::RuntimeRoot(violation));
        assert_eq!(
            resolve_runtime_directory_with_environment(&request, &inspector, &environment),
            expected.clone()
        );
        assert_eq!(
            prepare_runtime_directory_with_environment(&request, &inspector, &environment),
            expected,
            "an invalid /run/user/<uid> root cannot fall through to /tmp"
        );
    }
}

#[test]
#[cfg(target_os = "linux")]
fn linux_filesystem_locality_is_an_allowlist_and_unknown_types_are_refused() {
    assert!(linux_filesystem_type_is_local(0x0000_ef53));
    assert!(linux_filesystem_type_is_local(0x0102_1994));
    assert!(linux_filesystem_type_is_local(0x794c_7630));
    assert!(!linux_filesystem_type_is_local(0x6969));
    assert!(!linux_filesystem_type_is_local(0x517b));
    assert!(!linux_filesystem_type_is_local(0x1234_5678));
}

#[test]
#[cfg(target_os = "macos")]
fn local_channel_derives_the_macos_confstr_temporary_root() {
    let owner = LocalUserIdentity(7_102);
    let temporary_root = PathBuf::from("/var/folders/contextdb-user-temp");
    let request = RuntimeDirectoryRequest {
        explicit_root: None,
        current_user: owner,
    };
    let inspector = FactsInspector::default().with(facts(temporary_root.clone(), owner));

    assert_eq!(
        resolve_runtime_directory_with_environment(
            &request,
            &inspector,
            &RuntimeEnvironment {
                xdg: Some(PathBuf::from("/ignored-on-macos")),
                effective_user: owner,
                macos_temporary: Some(temporary_root.clone()),
            },
        )
        .expect("macOS temporary runtime resolution"),
        ResolvedRuntimeDirectory {
            path: temporary_root.join("contextdb"),
            source: RuntimeDirectorySource::MacosTemporary,
        }
    );
}

#[test]
fn explicit_runtime_root_precedes_internal_platform_resolution() {
    let owner = LocalUserIdentity(7_103);
    let explicit = PathBuf::from("/service/runtime");
    let xdg = PathBuf::from("/run/user/7103");
    let request = RuntimeDirectoryRequest {
        explicit_root: Some(explicit.clone()),
        current_user: owner,
    };
    let inspector = FactsInspector::default()
        .with(facts(explicit.clone(), owner))
        .with(facts(xdg.clone(), owner));

    assert_eq!(
        resolve_runtime_directory_with_environment(
            &request,
            &inspector,
            &RuntimeEnvironment {
                xdg: Some(xdg),
                effective_user: owner,
                macos_temporary: None,
            },
        )
        .expect("explicit runtime resolution"),
        ResolvedRuntimeDirectory {
            path: explicit.join("contextdb"),
            source: RuntimeDirectorySource::Explicit,
        }
    );
}

#[test]
fn local_channel_path_is_an_owner_only_socket_below_the_validated_runtime_directory() {
    let owner = LocalUserIdentity(7_104);
    let runtime = PathBuf::from("/run/user/7104/contextdb");
    let facts = ChannelPathFacts {
        path: runtime.join("a1b2c3.sock"),
        runtime_directory: runtime,
        is_socket: true,
        owner,
        mode: OWNER_ONLY_MODE,
    };
    validate_channel_path(&facts, owner).expect("owner-only channel socket");

    let malformed = [
        (
            ChannelPathFacts {
                is_socket: false,
                ..facts.clone()
            },
            ChannelPathViolation::NotSocket,
        ),
        (
            ChannelPathFacts {
                path: PathBuf::from("/var/lib/contextdb/store.sock"),
                ..facts.clone()
            },
            ChannelPathViolation::OutsideRuntimeDirectory,
        ),
        (
            ChannelPathFacts {
                owner: LocalUserIdentity(owner.0 + 1),
                ..facts.clone()
            },
            ChannelPathViolation::WrongOwner,
        ),
        (
            ChannelPathFacts {
                mode: 0o766,
                ..facts
            },
            ChannelPathViolation::InsecureMode,
        ),
    ];
    for (candidate, violation) in malformed {
        assert_eq!(
            validate_channel_path(&candidate, owner),
            Err(LocalTransportError::ChannelPath(violation)),
            "only an owner-only channel socket may remain below the runtime directory"
        );
    }
}

/// A Unix socket pathname is capped at 107 bytes, and the channel basename
/// alone is 69, so a runtime base created under a long ambient temporary
/// directory cannot host a socket at all. These journeys therefore build
/// their bases under a deliberately short root instead of inheriting the
/// environment's temporary directory.
#[cfg(unix)]
fn short_runtime_base() -> tempfile::TempDir {
    tempfile::Builder::new()
        .tempdir_in("/tmp")
        .expect("short runtime base")
}

#[test]
#[cfg(unix)]
fn production_environment_and_carrier_create_owner_only_runtime_and_channel_artifacts() {
    use std::os::unix::fs::symlink;

    let base = short_runtime_base();
    let current = LocalUserIdentity(nix::unistd::geteuid().as_raw() as u64);
    let inspector = SystemRuntimeDirectoryInspector;
    let resolved = prepare_runtime_directory(
        &RuntimeDirectoryRequest {
            explicit_root: Some(base.path().to_path_buf()),
            current_user: current,
        },
        &inspector,
    )
    .expect("production runtime preparation");
    assert_eq!(resolved.path, base.path().join("contextdb"));
    assert_eq!(resolved.source, RuntimeDirectorySource::Explicit);

    let runtime_facts = inspector
        .inspect(&resolved.path)
        .expect("inspect production-created runtime root");
    assert!(runtime_facts.is_absolute);
    assert!(runtime_facts.is_local);
    assert!(runtime_facts.is_directory);
    assert!(!runtime_facts.is_symbolic_link);
    assert_eq!(runtime_facts.owner, current);
    assert_eq!(runtime_facts.mode, OWNER_ONLY_MODE);
    assert!(runtime_facts.writable);

    let runtime_alias = base.path().join("runtime-alias");
    symlink(&resolved.path, &runtime_alias).expect("create runtime symlink");
    assert!(
        inspector
            .inspect(&runtime_alias)
            .expect("inspect runtime symlink")
            .is_symbolic_link
    );

    let socket = channel_socket_path(&resolved.path, ChannelAddress([0x31; 32]))
        .expect("bounded production channel path");
    let listener = UnixLocalCarrier
        .listen(&socket)
        .expect("production carrier creates channel");
    let socket_facts =
        inspect_channel_path(&socket, &resolved.path).expect("inspect production channel object");
    assert!(socket_facts.is_socket);
    assert_eq!(socket_facts.owner, current);
    assert_eq!(socket_facts.mode, OWNER_ONLY_MODE);
    drop(listener);
}

#[test]
#[cfg(unix)]
fn production_preparation_refuses_every_hostile_preexisting_runtime_child() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let current = LocalUserIdentity(nix::unistd::geteuid().as_raw() as u64);
    let request_for = |base: &Path| RuntimeDirectoryRequest {
        explicit_root: Some(base.to_path_buf()),
        current_user: current,
    };

    let file_base = short_runtime_base();
    std::fs::write(file_base.path().join("contextdb"), b"hostile")
        .expect("pre-create regular-file child");
    assert_eq!(
        prepare_runtime_directory(
            &request_for(file_base.path()),
            &SystemRuntimeDirectoryInspector,
        ),
        Err(LocalTransportError::RuntimeRoot(
            RuntimeRootViolation::NotDirectory
        ))
    );

    let symlink_base = short_runtime_base();
    let symlink_target = short_runtime_base();
    symlink(symlink_target.path(), symlink_base.path().join("contextdb"))
        .expect("pre-create symlink child");
    assert_eq!(
        prepare_runtime_directory(
            &request_for(symlink_base.path()),
            &SystemRuntimeDirectoryInspector,
        ),
        Err(LocalTransportError::RuntimeRoot(
            RuntimeRootViolation::SymbolicLink
        ))
    );

    let mode_base = short_runtime_base();
    let mode_child = mode_base.path().join("contextdb");
    std::fs::create_dir(&mode_child).expect("pre-create broad-mode child");
    std::fs::set_permissions(&mode_child, std::fs::Permissions::from_mode(0o755))
        .expect("set adversarial child mode");
    assert_eq!(
        prepare_runtime_directory(
            &request_for(mode_base.path()),
            &SystemRuntimeDirectoryInspector,
        ),
        Err(LocalTransportError::RuntimeRoot(
            RuntimeRootViolation::InsecureMode
        ))
    );

    let wrong_owner_base = PathBuf::from("/runtime/wrong-owner-proof");
    let wrong_owner_child = wrong_owner_base.join("contextdb");
    let mut wrong_owner = facts(wrong_owner_child.clone(), current);
    wrong_owner.owner = LocalUserIdentity(current.0.wrapping_add(1));
    let inspector = FactsInspector::default()
        .with(facts(wrong_owner_base.clone(), current))
        .with(wrong_owner);
    assert_eq!(
        prepare_runtime_directory_with_environment(
            &RuntimeDirectoryRequest {
                explicit_root: Some(wrong_owner_base),
                current_user: current,
            },
            &inspector,
            &RuntimeEnvironment {
                xdg: None,
                effective_user: current,
                macos_temporary: None,
            },
        ),
        Err(LocalTransportError::RuntimeRoot(
            RuntimeRootViolation::WrongOwner
        ))
    );
}

#[test]
#[cfg(target_os = "macos")]
fn process_environment_uses_the_native_macos_user_temporary_directory() {
    let environment = ProcessRuntimeDirectoryEnvironment;
    let temporary = environment
        .macos_user_temporary_directory()
        .expect("read _CS_DARWIN_USER_TEMP_DIR through confstr");
    assert!(temporary.is_absolute());
    let current = LocalUserIdentity(nix::unistd::geteuid().as_raw() as u64);
    assert_eq!(
        resolve_runtime_directory(
            &RuntimeDirectoryRequest {
                explicit_root: None,
                current_user: current,
            },
            &SystemRuntimeDirectoryInspector,
        )
        .expect("resolve through the process environment"),
        ResolvedRuntimeDirectory {
            path: temporary.join("contextdb"),
            source: RuntimeDirectorySource::MacosTemporary,
        }
    );
}

#[test]
#[cfg(target_os = "linux")]
fn linux_process_fallback_is_derived_from_the_effective_uid_not_the_request() {
    #[derive(Default)]
    struct RecordingMissingInspector(std::sync::Mutex<Vec<PathBuf>>);

    impl RuntimeDirectoryInspector for RecordingMissingInspector {
        fn inspect(&self, root: &Path) -> Result<RuntimeDirectoryFacts, LocalTransportError> {
            self.0
                .lock()
                .expect("recording inspector")
                .push(root.to_path_buf());
            Err(LocalTransportError::RuntimeRoot(
                RuntimeRootViolation::Unavailable,
            ))
        }
    }

    let environment = ProcessRuntimeDirectoryEnvironment;
    let effective = LocalUserIdentity(nix::unistd::geteuid().as_raw() as u64);
    assert_eq!(environment.effective_user_identity(), Ok(effective));
    assert_eq!(
        environment.linux_per_user_runtime_directory(),
        Ok(PathBuf::from("/run/user").join(effective.0.to_string()))
    );

    let caller_selected = LocalUserIdentity(effective.0.wrapping_add(91_337));
    let fallback = PathBuf::from("/run/user").join(effective.0.to_string());
    let inspector = RecordingMissingInspector::default();
    assert_eq!(
        resolve_runtime_directory_with_environment(
            &RuntimeDirectoryRequest {
                explicit_root: None,
                current_user: caller_selected,
            },
            &inspector,
            &RuntimeEnvironment {
                xdg: None,
                effective_user: effective,
                macos_temporary: Some(PathBuf::from("/tmp/forbidden")),
            },
        ),
        Err(LocalTransportError::RuntimeRoot(
            RuntimeRootViolation::Unavailable
        ))
    );
    assert_eq!(
        *inspector.0.lock().expect("recorded fallback inspection"),
        vec![fallback.clone()]
    );
    assert!(!fallback.starts_with("/tmp"));
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn channel_names_are_distinct_opaque_and_survive_a_deep_runtime_directory() {
    let runtime = PathBuf::from("/run/user/7105/contextdb");
    let first = ChannelAddress([0x11; 32]);
    let second = ChannelAddress([0x22; 32]);
    let first_path = channel_socket_path(&runtime, first).expect("first channel path");
    let second_path = channel_socket_path(&runtime, second).expect("second channel path");
    let first_basename = opaque_channel_basename(first);
    assert_ne!(first_path, second_path);
    assert_eq!(
        first_path.file_name().and_then(|name| name.to_str()),
        Some(first_basename.as_str())
    );
    assert_eq!(first_basename.len(), 69);

    let limit = unix_socket_path_limit().expect("Unix socket pathname limit");
    let too_long_runtime = PathBuf::from("/").join("r".repeat(limit));
    #[cfg(target_os = "linux")]
    assert_eq!(
        channel_socket_path(&too_long_runtime, first)
            .expect("a deep runtime directory still names a usable channel"),
        too_long_runtime.join(first_basename.as_str())
    );
    #[cfg(not(target_os = "linux"))]
    assert_eq!(
        channel_socket_path(&too_long_runtime, first),
        Err(LocalTransportError::ChannelPath(
            ChannelPathViolation::PathTooLong,
        ))
    );
}

#[test]
fn runtime_root_refuses_a_nonwritable_root_for_the_effective_channel_owner() {
    let effective_owner = LocalUserIdentity(7_108);
    let root = PathBuf::from("/service/effective-owner-runtime");
    let mut nonwritable = facts(root.clone(), effective_owner);
    nonwritable.writable = false;
    let inspector = FactsInspector::default().with(nonwritable);
    let environment = RuntimeEnvironment {
        xdg: None,
        effective_user: effective_owner,
        macos_temporary: None,
    };

    assert_eq!(
        resolve_runtime_directory_with_environment(
            &RuntimeDirectoryRequest {
                explicit_root: Some(root),
                current_user: effective_owner,
            },
            &inspector,
            &environment,
        ),
        Err(LocalTransportError::RuntimeRoot(
            RuntimeRootViolation::NotWritable
        )),
        "a runtime root owned by the effective channel user still fails closed when that identity cannot write it"
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod effective_writability_ast {
    use std::collections::BTreeMap;
    use syn::parse::Parser;
    use syn::punctuated::Punctuated;
    use syn::visit::{self, Visit};
    use syn::{
        Attribute, Block, Expr, ExprCall, ExprStruct, File, FnArg, GenericArgument, Item, ItemFn,
        Lit, Member, Meta, Pat, PathArguments, ReturnType, Stmt, Token, Type, UseTree,
    };

    type Aliases = BTreeMap<String, Vec<String>>;
    type Bindings = BTreeMap<String, Expr>;

    pub(super) struct Audit {
        pub(super) production_definitions: usize,
        pub(super) access_calls: usize,
        pub(super) faccessat_calls: usize,
        pub(super) correctly_bound_faccessat_calls: usize,
        pub(super) returns_result_bool: bool,
        pub(super) returned_boolean_is_fail_closed: bool,
        pub(super) authorizing_control_flow: usize,
        pub(super) available_runtime_facts_assignments: usize,
        pub(super) writable_assignment_uses_inspected_root: bool,
    }

    impl Audit {
        pub(super) fn proves_effective_writability(&self) -> bool {
            self.production_definitions == 1
                && self.access_calls == 0
                && self.faccessat_calls == 1
                && self.correctly_bound_faccessat_calls == 1
                && self.returns_result_bool
                && self.returned_boolean_is_fail_closed
                && self.authorizing_control_flow == 0
                && self.available_runtime_facts_assignments == 1
                && self.writable_assignment_uses_inspected_root
        }
    }

    pub(super) fn audit(source: &str) -> Audit {
        let syntax = syn::parse_file(source).expect("parse runtime writability source as Rust");
        let definitions = function_definitions(&syntax);
        let production_writability = definitions
            .iter()
            .filter(|definition| {
                !definition.test_only && definition.function.sig.ident == "filesystem_is_writable"
            })
            .map(|definition| definition.function)
            .collect::<Vec<_>>();
        let assignment = audit_runtime_facts_assignment(&syntax, &definitions);

        let mut audit = Audit {
            production_definitions: production_writability.len(),
            access_calls: 0,
            faccessat_calls: 0,
            correctly_bound_faccessat_calls: 0,
            returns_result_bool: false,
            returned_boolean_is_fail_closed: false,
            authorizing_control_flow: 0,
            available_runtime_facts_assignments: assignment.available_assignments,
            writable_assignment_uses_inspected_root: assignment.exact,
        };
        if production_writability.len() != 1 {
            return audit;
        }

        let function = production_writability[0];
        let Some(path_parameter) = first_path_parameter(function) else {
            return audit;
        };
        let mut aliases = imported_aliases(&syntax);
        add_block_imported_aliases(&function.block, &mut aliases);
        add_local_callable_aliases(function, &mut aliases);
        let bindings = local_bindings(function);
        let mut calls = CallAudit {
            aliases: &aliases,
            bindings: &bindings,
            path_parameter: &path_parameter,
            access_calls: 0,
            faccessat_calls: 0,
            correctly_bound_faccessat_calls: 0,
        };
        calls.visit_item_fn(function);

        let mut control_flow = ControlFlowAudit { escapes: 0 };
        control_flow.visit_item_fn(function);

        audit.access_calls = calls.access_calls;
        audit.faccessat_calls = calls.faccessat_calls;
        audit.correctly_bound_faccessat_calls = calls.correctly_bound_faccessat_calls;
        audit.returns_result_bool = returns_result_bool(function);
        audit.authorizing_control_flow = control_flow.escapes;
        audit.returned_boolean_is_fail_closed = final_expression(&function.block)
            .map(|expression| {
                returned_result_is_fail_closed(
                    expression,
                    &aliases,
                    &bindings,
                    &path_parameter,
                    &mut Vec::new(),
                )
            })
            .unwrap_or(false);
        audit
    }

    #[derive(Clone, Copy)]
    struct FunctionDefinition<'ast> {
        function: &'ast ItemFn,
        test_only: bool,
    }

    fn function_definitions(file: &File) -> Vec<FunctionDefinition<'_>> {
        let mut collector = FunctionCollector {
            definitions: Vec::new(),
            test_only_context: false,
        };
        collector.visit_file(file);
        collector.definitions
    }

    struct FunctionCollector<'ast> {
        definitions: Vec<FunctionDefinition<'ast>>,
        test_only_context: bool,
    }

    impl<'ast> Visit<'ast> for FunctionCollector<'ast> {
        fn visit_item_mod(&mut self, module: &'ast syn::ItemMod) {
            let previous = self.test_only_context;
            self.test_only_context = previous || attributes_require_test(&module.attrs);
            visit::visit_item_mod(self, module);
            self.test_only_context = previous;
        }

        fn visit_item_fn(&mut self, function: &'ast ItemFn) {
            let previous = self.test_only_context;
            let test_only = previous || attributes_require_test(&function.attrs);
            self.definitions.push(FunctionDefinition {
                function,
                test_only,
            });
            self.test_only_context = test_only;
            visit::visit_item_fn(self, function);
            self.test_only_context = previous;
        }
    }

    fn attributes_require_test(attributes: &[Attribute]) -> bool {
        attributes.iter().any(|attribute| {
            if attribute.path().is_ident("test") {
                return true;
            }
            let Meta::List(cfg) = &attribute.meta else {
                return false;
            };
            if !cfg.path.is_ident("cfg") {
                return false;
            }
            let parser = Punctuated::<Meta, Token![,]>::parse_terminated;
            parser
                .parse2(cfg.tokens.clone())
                .ok()
                .is_some_and(|predicates| {
                    !predicates.is_empty() && predicates.iter().all(cfg_predicate_requires_test)
                })
        })
    }

    fn cfg_predicate_requires_test(predicate: &Meta) -> bool {
        match predicate {
            Meta::Path(path) => path.is_ident("test"),
            Meta::NameValue(name_value) => {
                name_value.path.is_ident("feature")
                    && matches!(
                        &name_value.value,
                        Expr::Lit(value)
                            if matches!(
                                &value.lit,
                                Lit::Str(feature) if feature.value() == "test-seams"
                            )
                    )
            }
            Meta::List(list) => {
                let parser = Punctuated::<Meta, Token![,]>::parse_terminated;
                let Ok(nested) = parser.parse2(list.tokens.clone()) else {
                    return false;
                };
                if list.path.is_ident("all") {
                    nested.iter().any(cfg_predicate_requires_test)
                } else if list.path.is_ident("any") {
                    !nested.is_empty() && nested.iter().all(cfg_predicate_requires_test)
                } else {
                    false
                }
            }
        }
    }

    fn imported_aliases(file: &File) -> Aliases {
        let mut aliases = Aliases::new();
        for item in &file.items {
            if let Item::Use(item_use) = item {
                collect_use_aliases(&item_use.tree, &mut Vec::new(), &mut aliases);
            }
        }
        aliases
    }

    fn collect_use_aliases(tree: &UseTree, prefix: &mut Vec<String>, aliases: &mut Aliases) {
        match tree {
            UseTree::Path(path) => {
                prefix.push(path.ident.to_string());
                collect_use_aliases(&path.tree, prefix, aliases);
                prefix.pop();
            }
            UseTree::Name(name) => {
                let mut origin = prefix.clone();
                origin.push(name.ident.to_string());
                aliases.insert(name.ident.to_string(), origin);
            }
            UseTree::Rename(rename) => {
                let mut origin = prefix.clone();
                origin.push(rename.ident.to_string());
                aliases.insert(rename.rename.to_string(), origin);
            }
            UseTree::Group(group) => {
                for item in &group.items {
                    collect_use_aliases(item, prefix, aliases);
                }
            }
            UseTree::Glob(_) => {}
        }
    }

    fn add_block_imported_aliases(block: &Block, aliases: &mut Aliases) {
        for statement in &block.stmts {
            if let Stmt::Item(Item::Use(item_use)) = statement {
                collect_use_aliases(&item_use.tree, &mut Vec::new(), aliases);
            }
        }
    }

    fn normalized_path(path: &syn::Path, aliases: &Aliases) -> Vec<String> {
        let raw = path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        let Some(first) = raw.first() else {
            return raw;
        };
        let Some(origin) = aliases.get(first) else {
            return raw;
        };
        let mut normalized = origin.clone();
        normalized.extend(raw.into_iter().skip(1));
        normalized
    }

    fn expression_path(expression: &Expr) -> Option<&syn::Path> {
        match expression {
            Expr::Path(path) => Some(&path.path),
            Expr::Group(group) => expression_path(&group.expr),
            Expr::Paren(parenthesized) => expression_path(&parenthesized.expr),
            _ => None,
        }
    }

    fn callable_path(expression: &Expr, aliases: &Aliases) -> Option<Vec<String>> {
        expression_path(expression).map(|path| normalized_path(path, aliases))
    }

    fn callable_name(expression: &Expr, aliases: &Aliases) -> Option<String> {
        callable_path(expression, aliases).and_then(|path| path.last().cloned())
    }

    fn path_is(path: &[String], expected: &[&str]) -> bool {
        path.len() == expected.len()
            && path
                .iter()
                .zip(expected)
                .all(|(actual, expected)| actual.as_str() == *expected)
    }

    fn is_nix_unistd_faccessat(expression: &Expr, aliases: &Aliases) -> bool {
        callable_path(expression, aliases)
            .is_some_and(|path| path_is(&path, &["nix", "unistd", "faccessat"]))
    }

    fn first_path_parameter(function: &ItemFn) -> Option<String> {
        function
            .sig
            .inputs
            .iter()
            .find_map(|argument| match argument {
                FnArg::Typed(typed) => simple_binding_name(&typed.pat),
                FnArg::Receiver(_) => None,
            })
    }

    fn returns_result_bool(function: &ItemFn) -> bool {
        let ReturnType::Type(_, returned) = &function.sig.output else {
            return false;
        };
        let Type::Path(result) = returned.as_ref() else {
            return false;
        };
        let Some(result_segment) = result.path.segments.last() else {
            return false;
        };
        if result_segment.ident != "Result" {
            return false;
        }
        let PathArguments::AngleBracketed(arguments) = &result_segment.arguments else {
            return false;
        };
        matches!(
            arguments.args.first(),
            Some(GenericArgument::Type(Type::Path(boolean)))
                if boolean.path.is_ident("bool")
        )
    }

    fn simple_binding_name(pattern: &Pat) -> Option<String> {
        match pattern {
            Pat::Ident(ident) => Some(ident.ident.to_string()),
            Pat::Type(typed) => simple_binding_name(&typed.pat),
            _ => None,
        }
    }

    fn add_local_callable_aliases(function: &ItemFn, aliases: &mut Aliases) {
        for statement in &function.block.stmts {
            let Stmt::Local(local) = statement else {
                continue;
            };
            let Some(binding) = simple_binding_name(&local.pat) else {
                continue;
            };
            let Some(initializer) = &local.init else {
                continue;
            };
            let Some(path) = expression_path(&initializer.expr) else {
                continue;
            };
            let origin = normalized_path(path, aliases);
            if matches!(
                origin.last().map(String::as_str),
                Some("access") | Some("faccessat")
            ) {
                aliases.insert(binding, origin);
            }
        }
    }

    fn add_block_bindings(block: &Block, bindings: &mut Bindings) {
        for statement in &block.stmts {
            let Stmt::Local(local) = statement else {
                continue;
            };
            let Some(binding) = simple_binding_name(&local.pat) else {
                continue;
            };
            let Some(initializer) = &local.init else {
                continue;
            };
            bindings.insert(binding, (*initializer.expr).clone());
        }
    }

    fn local_bindings(function: &ItemFn) -> Bindings {
        let mut bindings = Bindings::new();
        add_block_bindings(&function.block, &mut bindings);
        bindings
    }

    fn follow_binding<'a>(
        expression: &'a Expr,
        bindings: &'a Bindings,
        visited: &mut Vec<String>,
    ) -> Option<&'a Expr> {
        let Expr::Path(path) = expression else {
            return None;
        };
        if path.path.segments.len() != 1 {
            return None;
        }
        let name = path.path.segments[0].ident.to_string();
        if visited.contains(&name) {
            return None;
        }
        let bound = bindings.get(&name)?;
        visited.push(name);
        Some(bound)
    }

    fn leave_binding(expression: &Expr, visited: &mut Vec<String>) {
        if matches!(expression, Expr::Path(path) if path.path.segments.len() == 1) {
            visited.pop();
        }
    }

    fn resolves_to_exact_path(
        expression: &Expr,
        aliases: &Aliases,
        bindings: &Bindings,
        expected_path: &[&str],
        visited: &mut Vec<String>,
    ) -> bool {
        match expression {
            Expr::Path(path) => {
                if path.path.segments.len() == 1 {
                    let name = path.path.segments[0].ident.to_string();
                    if bindings.contains_key(&name) {
                        let Some(bound) = follow_binding(expression, bindings, visited) else {
                            return false;
                        };
                        let resolved = resolves_to_exact_path(
                            bound,
                            aliases,
                            bindings,
                            expected_path,
                            visited,
                        );
                        leave_binding(expression, visited);
                        return resolved;
                    }
                }
                let normalized = normalized_path(&path.path, aliases);
                normalized.len() == expected_path.len()
                    && normalized
                        .iter()
                        .zip(expected_path)
                        .all(|(actual, expected)| actual.as_str() == *expected)
            }
            Expr::Group(group) => {
                resolves_to_exact_path(&group.expr, aliases, bindings, expected_path, visited)
            }
            Expr::Paren(parenthesized) => resolves_to_exact_path(
                &parenthesized.expr,
                aliases,
                bindings,
                expected_path,
                visited,
            ),
            Expr::Reference(reference) => {
                resolves_to_exact_path(&reference.expr, aliases, bindings, expected_path, visited)
            }
            Expr::Block(block) => {
                let mut scoped_bindings = bindings.clone();
                add_block_bindings(&block.block, &mut scoped_bindings);
                final_expression(&block.block)
                    .map(|tail| {
                        resolves_to_exact_path(
                            tail,
                            aliases,
                            &scoped_bindings,
                            expected_path,
                            visited,
                        )
                    })
                    .unwrap_or(false)
            }
            _ => false,
        }
    }

    fn resolves_to_path_parameter(
        expression: &Expr,
        parameter: &str,
        bindings: &Bindings,
        visited: &mut Vec<String>,
    ) -> bool {
        match expression {
            Expr::Path(path) if path.path.segments.len() == 1 => {
                let name = path.path.segments[0].ident.to_string();
                if bindings.contains_key(&name) {
                    let Some(bound) = follow_binding(expression, bindings, visited) else {
                        return false;
                    };
                    let resolved = resolves_to_path_parameter(bound, parameter, bindings, visited);
                    leave_binding(expression, visited);
                    resolved
                } else {
                    name == parameter
                }
            }
            Expr::Group(group) => {
                resolves_to_path_parameter(&group.expr, parameter, bindings, visited)
            }
            Expr::Paren(parenthesized) => {
                resolves_to_path_parameter(&parenthesized.expr, parameter, bindings, visited)
            }
            Expr::Reference(reference) => {
                resolves_to_path_parameter(&reference.expr, parameter, bindings, visited)
            }
            Expr::Block(block) => {
                let mut scoped_bindings = bindings.clone();
                add_block_bindings(&block.block, &mut scoped_bindings);
                final_expression(&block.block)
                    .map(|tail| {
                        resolves_to_path_parameter(tail, parameter, &scoped_bindings, visited)
                    })
                    .unwrap_or(false)
            }
            _ => false,
        }
    }

    fn is_effective_faccessat_call(
        call: &ExprCall,
        aliases: &Aliases,
        bindings: &Bindings,
        path_parameter: &str,
    ) -> bool {
        if !is_nix_unistd_faccessat(&call.func, aliases) || call.args.len() != 4 {
            return false;
        }
        let directory = call.args.first().expect("four faccessat arguments");
        let path = call.args.iter().nth(1).expect("four faccessat arguments");
        let mode = call.args.iter().nth(2).expect("four faccessat arguments");
        let flags = call.args.iter().nth(3).expect("four faccessat arguments");
        resolves_to_exact_path(
            directory,
            aliases,
            bindings,
            &["nix", "fcntl", "AT_FDCWD"],
            &mut Vec::new(),
        ) && resolves_to_path_parameter(path, path_parameter, bindings, &mut Vec::new())
            && resolves_to_exact_path(
                mode,
                aliases,
                bindings,
                &["nix", "unistd", "AccessFlags", "W_OK"],
                &mut Vec::new(),
            )
            && resolves_to_exact_path(
                flags,
                aliases,
                bindings,
                &["nix", "fcntl", "AtFlags", "AT_EACCESS"],
                &mut Vec::new(),
            )
    }

    struct CallAudit<'a> {
        aliases: &'a Aliases,
        bindings: &'a Bindings,
        path_parameter: &'a str,
        access_calls: usize,
        faccessat_calls: usize,
        correctly_bound_faccessat_calls: usize,
    }

    impl<'ast> Visit<'ast> for CallAudit<'_> {
        fn visit_expr_call(&mut self, call: &'ast ExprCall) {
            match callable_name(&call.func, self.aliases).as_deref() {
                Some("access") => self.access_calls += 1,
                Some("faccessat") => {
                    self.faccessat_calls += 1;
                    if is_effective_faccessat_call(
                        call,
                        self.aliases,
                        self.bindings,
                        self.path_parameter,
                    ) {
                        self.correctly_bound_faccessat_calls += 1;
                    }
                }
                _ => {}
            }
            visit::visit_expr_call(self, call);
        }

        fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
            if call.method == "access" {
                self.access_calls += 1;
            }
            visit::visit_expr_method_call(self, call);
        }
    }

    struct ControlFlowAudit {
        escapes: usize,
    }

    impl<'ast> Visit<'ast> for ControlFlowAudit {
        fn visit_expr_if(&mut self, expression: &'ast syn::ExprIf) {
            self.escapes += 1;
            visit::visit_expr_if(self, expression);
        }

        fn visit_expr_match(&mut self, expression: &'ast syn::ExprMatch) {
            self.escapes += 1;
            visit::visit_expr_match(self, expression);
        }

        fn visit_expr_return(&mut self, expression: &'ast syn::ExprReturn) {
            self.escapes += 1;
            visit::visit_expr_return(self, expression);
        }

        fn visit_expr_loop(&mut self, expression: &'ast syn::ExprLoop) {
            self.escapes += 1;
            visit::visit_expr_loop(self, expression);
        }

        fn visit_expr_while(&mut self, expression: &'ast syn::ExprWhile) {
            self.escapes += 1;
            visit::visit_expr_while(self, expression);
        }

        fn visit_expr_for_loop(&mut self, expression: &'ast syn::ExprForLoop) {
            self.escapes += 1;
            visit::visit_expr_for_loop(self, expression);
        }

        fn visit_expr_break(&mut self, expression: &'ast syn::ExprBreak) {
            self.escapes += 1;
            visit::visit_expr_break(self, expression);
        }

        fn visit_expr_continue(&mut self, expression: &'ast syn::ExprContinue) {
            self.escapes += 1;
            visit::visit_expr_continue(self, expression);
        }
    }

    fn final_expression(block: &Block) -> Option<&Expr> {
        match block.stmts.last() {
            Some(Stmt::Expr(expression, _)) => Some(expression),
            _ => None,
        }
    }

    fn resolves_to_effective_faccessat(
        expression: &Expr,
        aliases: &Aliases,
        bindings: &Bindings,
        path_parameter: &str,
        visited: &mut Vec<String>,
    ) -> bool {
        match expression {
            Expr::Call(call) => {
                is_effective_faccessat_call(call, aliases, bindings, path_parameter)
            }
            Expr::Group(group) => resolves_to_effective_faccessat(
                &group.expr,
                aliases,
                bindings,
                path_parameter,
                visited,
            ),
            Expr::Paren(parenthesized) => resolves_to_effective_faccessat(
                &parenthesized.expr,
                aliases,
                bindings,
                path_parameter,
                visited,
            ),
            Expr::Block(block) => final_expression(&block.block)
                .map(|tail| {
                    resolves_to_effective_faccessat(
                        tail,
                        aliases,
                        bindings,
                        path_parameter,
                        visited,
                    )
                })
                .unwrap_or(false),
            Expr::Path(_) => {
                let Some(bound) = follow_binding(expression, bindings, visited) else {
                    return false;
                };
                let resolved = resolves_to_effective_faccessat(
                    bound,
                    aliases,
                    bindings,
                    path_parameter,
                    visited,
                );
                leave_binding(expression, visited);
                resolved
            }
            _ => false,
        }
    }

    fn fail_closed_writable_boolean(
        expression: &Expr,
        aliases: &Aliases,
        bindings: &Bindings,
        path_parameter: &str,
        visited: &mut Vec<String>,
    ) -> bool {
        match expression {
            Expr::MethodCall(method) if method.method == "is_ok" && method.args.is_empty() => {
                resolves_to_effective_faccessat(
                    &method.receiver,
                    aliases,
                    bindings,
                    path_parameter,
                    visited,
                )
            }
            Expr::Group(group) => fail_closed_writable_boolean(
                &group.expr,
                aliases,
                bindings,
                path_parameter,
                visited,
            ),
            Expr::Paren(parenthesized) => fail_closed_writable_boolean(
                &parenthesized.expr,
                aliases,
                bindings,
                path_parameter,
                visited,
            ),
            Expr::Block(block) => final_expression(&block.block)
                .map(|tail| {
                    fail_closed_writable_boolean(tail, aliases, bindings, path_parameter, visited)
                })
                .unwrap_or(false),
            Expr::Path(_) => {
                let Some(bound) = follow_binding(expression, bindings, visited) else {
                    return false;
                };
                let fail_closed =
                    fail_closed_writable_boolean(bound, aliases, bindings, path_parameter, visited);
                leave_binding(expression, visited);
                fail_closed
            }
            _ => false,
        }
    }

    fn returned_result_is_fail_closed(
        expression: &Expr,
        aliases: &Aliases,
        bindings: &Bindings,
        path_parameter: &str,
        visited: &mut Vec<String>,
    ) -> bool {
        match expression {
            Expr::Call(call)
                if callable_name(&call.func, aliases).as_deref() == Some("Ok")
                    && call.args.len() == 1 =>
            {
                fail_closed_writable_boolean(
                    call.args.first().expect("one Ok argument"),
                    aliases,
                    bindings,
                    path_parameter,
                    visited,
                )
            }
            Expr::Return(returned) => returned
                .expr
                .as_deref()
                .map(|value| {
                    returned_result_is_fail_closed(
                        value,
                        aliases,
                        bindings,
                        path_parameter,
                        visited,
                    )
                })
                .unwrap_or(false),
            Expr::Group(group) => returned_result_is_fail_closed(
                &group.expr,
                aliases,
                bindings,
                path_parameter,
                visited,
            ),
            Expr::Paren(parenthesized) => returned_result_is_fail_closed(
                &parenthesized.expr,
                aliases,
                bindings,
                path_parameter,
                visited,
            ),
            Expr::Block(block) => final_expression(&block.block)
                .map(|tail| {
                    returned_result_is_fail_closed(tail, aliases, bindings, path_parameter, visited)
                })
                .unwrap_or(false),
            Expr::Path(_) => {
                let Some(bound) = follow_binding(expression, bindings, visited) else {
                    return false;
                };
                let fail_closed = returned_result_is_fail_closed(
                    bound,
                    aliases,
                    bindings,
                    path_parameter,
                    visited,
                );
                leave_binding(expression, visited);
                fail_closed
            }
            _ => false,
        }
    }

    struct RuntimeAssignmentAudit {
        available_assignments: usize,
        exact: bool,
    }

    fn audit_runtime_facts_assignment(
        file: &File,
        definitions: &[FunctionDefinition<'_>],
    ) -> RuntimeAssignmentAudit {
        let aliases = imported_aliases(file);
        let mut available_assignments = 0;
        let mut exact = true;

        for definition in definitions.iter().filter(|definition| {
            !definition.test_only && definition.function.sig.ident == "inspect_system_runtime_root"
        }) {
            let function = definition.function;
            let Some(root_parameter) = first_path_parameter(function) else {
                continue;
            };
            let bindings = local_bindings(function);
            let mut facts = AvailableRuntimeFacts {
                expressions: Vec::new(),
            };
            facts.visit_item_fn(function);
            for expression in facts.expressions {
                available_assignments += 1;
                let writable = struct_field(expression, "writable");
                let writable_is_exact = writable.is_some_and(|value| {
                    resolves_to_runtime_writability_call(
                        value,
                        &aliases,
                        &bindings,
                        &root_parameter,
                        &mut Vec::new(),
                    )
                });
                let mut metadata = MetadataRootAudit {
                    aliases: &aliases,
                    bindings: &bindings,
                    root_parameter: &root_parameter,
                    exact_calls: 0,
                };
                metadata.visit_item_fn(function);
                exact &= writable_is_exact && metadata.exact_calls > 0;
            }
        }

        RuntimeAssignmentAudit {
            available_assignments,
            exact: available_assignments == 1 && exact,
        }
    }

    struct AvailableRuntimeFacts<'ast> {
        expressions: Vec<&'ast ExprStruct>,
    }

    impl<'ast> Visit<'ast> for AvailableRuntimeFacts<'ast> {
        fn visit_expr_struct(&mut self, expression: &'ast ExprStruct) {
            let is_runtime_facts = expression
                .path
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "RuntimeDirectoryFacts");
            if is_runtime_facts
                && struct_field(expression, "available").is_some_and(expression_is_true)
            {
                self.expressions.push(expression);
            }
            visit::visit_expr_struct(self, expression);
        }
    }

    fn struct_field<'a>(expression: &'a ExprStruct, name: &str) -> Option<&'a Expr> {
        expression
            .fields
            .iter()
            .find_map(|field| match &field.member {
                Member::Named(ident) if ident == name => Some(&field.expr),
                _ => None,
            })
    }

    fn expression_is_true(expression: &Expr) -> bool {
        matches!(
            expression,
            Expr::Lit(value) if matches!(&value.lit, Lit::Bool(boolean) if boolean.value)
        )
    }

    fn resolves_to_runtime_writability_call(
        expression: &Expr,
        aliases: &Aliases,
        bindings: &Bindings,
        root_parameter: &str,
        visited: &mut Vec<String>,
    ) -> bool {
        match expression {
            Expr::Try(attempt) => resolves_to_runtime_writability_call(
                &attempt.expr,
                aliases,
                bindings,
                root_parameter,
                visited,
            ),
            Expr::Call(call) if call.args.len() == 1 => {
                let exact_function = callable_path(&call.func, aliases).is_some_and(|path| {
                    path_is(&path, &["filesystem_is_writable"])
                        || path_is(&path, &["self", "filesystem_is_writable"])
                        || path_is(
                            &path,
                            &[
                                "crate",
                                "local_transport",
                                "runtime",
                                "filesystem_is_writable",
                            ],
                        )
                });
                exact_function
                    && resolves_to_path_parameter(
                        call.args.first().expect("one writability argument"),
                        root_parameter,
                        bindings,
                        &mut Vec::new(),
                    )
            }
            Expr::Group(group) => resolves_to_runtime_writability_call(
                &group.expr,
                aliases,
                bindings,
                root_parameter,
                visited,
            ),
            Expr::Paren(parenthesized) => resolves_to_runtime_writability_call(
                &parenthesized.expr,
                aliases,
                bindings,
                root_parameter,
                visited,
            ),
            Expr::Path(_) => {
                let Some(bound) = follow_binding(expression, bindings, visited) else {
                    return false;
                };
                let resolved = resolves_to_runtime_writability_call(
                    bound,
                    aliases,
                    bindings,
                    root_parameter,
                    visited,
                );
                leave_binding(expression, visited);
                resolved
            }
            _ => false,
        }
    }

    struct MetadataRootAudit<'a> {
        aliases: &'a Aliases,
        bindings: &'a Bindings,
        root_parameter: &'a str,
        exact_calls: usize,
    }

    impl<'ast> Visit<'ast> for MetadataRootAudit<'_> {
        fn visit_expr_call(&mut self, call: &'ast ExprCall) {
            let is_metadata_call = callable_path(&call.func, self.aliases)
                .is_some_and(|path| path_is(&path, &["std", "fs", "symlink_metadata"]));
            if is_metadata_call
                && call.args.len() == 1
                && resolves_to_path_parameter(
                    call.args.first().expect("one metadata argument"),
                    self.root_parameter,
                    self.bindings,
                    &mut Vec::new(),
                )
            {
                self.exact_calls += 1;
            }
            visit::visit_expr_call(self, call);
        }
    }
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
const EXACT_EFFECTIVE_WRITABILITY: &str = r#"
use nix::fcntl::{AtFlags as EffectiveFlags, AT_FDCWD as CurrentDirectory};
use nix::unistd::{AccessFlags as Modes, faccessat as effective_access};

#[cfg(feature = "test-seams")]
fn filesystem_is_writable(_path: &Path) -> Result<bool, Error> {
    Ok(true)
}

fn filesystem_is_writable(path: &Path) -> Result<bool, Error> {
    let check = effective_access;
    let directory = CurrentDirectory;
    let inspected_path = path;
    let write_mode = Modes::W_OK;
    let effective_ids = EffectiveFlags::AT_EACCESS;
    let checked = check(directory, inspected_path, write_mode, effective_ids);
    Ok(checked.is_ok())
}
"#;

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn audit_effective_writability_fixture(
    definitions: &str,
    assignment_path: &str,
    metadata_path: &str,
) -> effective_writability_ast::Audit {
    let source = format!(
        r#"
{definitions}

fn inspect_system_runtime_root(root: &Path) -> Result<RuntimeDirectoryFacts, Error> {{
    let _metadata = std::fs::symlink_metadata({metadata_path})?;
    Ok(RuntimeDirectoryFacts {{
        writable: filesystem_is_writable({assignment_path})?,
        available: true,
    }})
}}
"#
    );
    effective_writability_ast::audit(&source)
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn effective_writability_ast_rejects_every_noncausal_or_alternate_authorization_path() {
    let exact = audit_effective_writability_fixture(EXACT_EFFECTIVE_WRITABILITY, "root", "root");
    assert_eq!(exact.production_definitions, 1);
    assert_eq!(exact.access_calls, 0);
    assert_eq!(exact.faccessat_calls, 1);
    assert_eq!(exact.correctly_bound_faccessat_calls, 1);
    assert_eq!(exact.authorizing_control_flow, 0);
    assert!(exact.returns_result_bool);
    assert!(exact.returned_boolean_is_fail_closed);
    assert_eq!(exact.available_runtime_facts_assignments, 1);
    assert!(exact.writable_assignment_uses_inspected_root);
    assert!(
        exact.proves_effective_writability(),
        "harmless import, callable, flag, result, and path aliases preserve exact dataflow"
    );

    let duplicate_definitions = format!(
        r#"
{EXACT_EFFECTIVE_WRITABILITY}
fn filesystem_is_writable(path: &Path) -> Result<bool, Error> {{
    Ok(nix::unistd::access(path, nix::unistd::AccessFlags::W_OK).is_ok())
}}
"#
    );
    let duplicate = audit_effective_writability_fixture(&duplicate_definitions, "root", "root");
    assert_eq!(duplicate.production_definitions, 2);
    assert!(
        !duplicate.proves_effective_writability(),
        "a cfg(test-seams) decoy must not hide a second buggy production definition"
    );

    let early_true = audit_effective_writability_fixture(
        r#"
fn filesystem_is_writable(path: &Path) -> Result<bool, Error> {
    if bypass {
        return Ok(true);
    }
    Ok(nix::unistd::faccessat(
        nix::fcntl::AT_FDCWD,
        path,
        nix::unistd::AccessFlags::W_OK,
        nix::fcntl::AtFlags::AT_EACCESS,
    ).is_ok())
}
"#,
        "root",
        "root",
    );
    assert!(early_true.authorizing_control_flow > 0);
    assert!(
        !early_true.proves_effective_writability(),
        "an early Ok(true) path cannot coexist with the audited syscall result"
    );

    let wrong_path = audit_effective_writability_fixture(
        r#"
fn filesystem_is_writable(path: &Path) -> Result<bool, Error> {
    Ok(nix::unistd::faccessat(
        nix::fcntl::AT_FDCWD,
        other_path,
        nix::unistd::AccessFlags::W_OK,
        nix::fcntl::AtFlags::AT_EACCESS,
    ).is_ok())
}
"#,
        "root",
        "root",
    );
    assert_eq!(wrong_path.correctly_bound_faccessat_calls, 0);
    assert!(!wrong_path.proves_effective_writability());

    let wrong_directory = audit_effective_writability_fixture(
        r#"
fn filesystem_is_writable(path: &Path) -> Result<bool, Error> {
    Ok(nix::unistd::faccessat(
        other_directory,
        path,
        nix::unistd::AccessFlags::W_OK,
        nix::fcntl::AtFlags::AT_EACCESS,
    ).is_ok())
}
"#,
        "root",
        "root",
    );
    assert_eq!(wrong_directory.correctly_bound_faccessat_calls, 0);
    assert!(!wrong_directory.proves_effective_writability());

    let real_id_access = audit_effective_writability_fixture(
        r#"
use nix::unistd::access as real_access;

fn filesystem_is_writable(path: &Path) -> Result<bool, Error> {
    let check = real_access;
    let _ignored = check(path, nix::unistd::AccessFlags::W_OK);
    Ok(nix::unistd::faccessat(
        nix::fcntl::AT_FDCWD,
        path,
        nix::unistd::AccessFlags::W_OK,
        nix::fcntl::AtFlags::AT_EACCESS,
    ).is_ok())
}
"#,
        "root",
        "root",
    );
    assert_eq!(real_id_access.access_calls, 1);
    assert!(!real_id_access.proves_effective_writability());

    let ignored_flag_mentions = audit_effective_writability_fixture(
        r#"
fn filesystem_is_writable(path: &Path) -> Result<bool, Error> {
    let mode = {
        let _mentioned_write = nix::unistd::AccessFlags::W_OK;
        nix::unistd::AccessFlags::R_OK
    };
    let flags = {
        let _mentioned_effective = nix::fcntl::AtFlags::AT_EACCESS;
        nix::fcntl::AtFlags::empty()
    };
    Ok(nix::unistd::faccessat(
        nix::fcntl::AT_FDCWD,
        path,
        mode,
        flags,
    ).is_ok())
}
"#,
        "root",
        "root",
    );
    assert_eq!(ignored_flag_mentions.correctly_bound_faccessat_calls, 0);
    assert!(
        !ignored_flag_mentions.proves_effective_writability(),
        "ignored mentions outside the evaluated third and fourth arguments do not count"
    );

    let ignored_result = audit_effective_writability_fixture(
        r#"
fn filesystem_is_writable(path: &Path) -> Result<bool, Error> {
    let _ignored = nix::unistd::faccessat(
        nix::fcntl::AT_FDCWD,
        path,
        nix::unistd::AccessFlags::W_OK,
        nix::fcntl::AtFlags::AT_EACCESS,
    );
    let unrelated = Ok(());
    Ok(unrelated.is_ok())
}
"#,
        "root",
        "root",
    );
    assert_eq!(ignored_result.correctly_bound_faccessat_calls, 1);
    assert!(!ignored_result.returned_boolean_is_fail_closed);
    assert!(
        !ignored_result.proves_effective_writability(),
        "an ignored faccessat result cannot authorize writability"
    );

    let wrong_assignment =
        audit_effective_writability_fixture(EXACT_EFFECTIVE_WRITABILITY, "other_root", "root");
    assert!(!wrong_assignment.writable_assignment_uses_inspected_root);
    assert!(!wrong_assignment.proves_effective_writability());

    let wrong_inspection =
        audit_effective_writability_fixture(EXACT_EFFECTIVE_WRITABILITY, "root", "other_root");
    assert!(!wrong_inspection.writable_assignment_uses_inspected_root);
    assert!(!wrong_inspection.proves_effective_writability());
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn production_runtime_writability_uses_effective_identity_access_not_real_id_access() {
    let source_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/local_transport/runtime.rs");
    let source =
        fs::read_to_string(&source_path).expect("read production runtime inspector source");
    let audit = effective_writability_ast::audit(&source);

    assert_eq!(
        audit.production_definitions, 1,
        "there must be exactly one non-test-only filesystem_is_writable definition"
    );
    assert_eq!(
        audit.available_runtime_facts_assignments, 1,
        "there must be exactly one available RuntimeDirectoryFacts production assignment"
    );
    assert!(
        audit.writable_assignment_uses_inspected_root,
        "RuntimeDirectoryFacts::writable must call the unique function with the root inspected by symlink_metadata"
    );
    assert_eq!(
        audit.access_calls, 0,
        "the unique function must reject every access call regardless of import or qualification"
    );
    assert_eq!(
        audit.faccessat_calls, 1,
        "the returned writability decision must contain exactly one nix::unistd::faccessat call"
    );
    assert_eq!(
        audit.correctly_bound_faccessat_calls, 1,
        "the evaluated arguments must resolve to AT_FDCWD, the path parameter, W_OK, and AT_EACCESS"
    );
    assert!(audit.returns_result_bool);
    assert_eq!(
        audit.authorizing_control_flow, 0,
        "the unique function must have no branch or early return that can authorize independently"
    );
    assert!(
        audit.returned_boolean_is_fail_closed,
        "Result<bool> must be Ok(is_ok()) on that sole faccessat call or its exact binding"
    );
    assert!(audit.proves_effective_writability());
}

/// Create an owner-only directory the way an operator's runtime root already
/// exists before this process ever looks at it.
#[cfg(unix)]
fn create_owner_only_directory(path: &Path) {
    use std::os::unix::fs::DirBuilderExt;

    std::fs::DirBuilder::new()
        .mode(OWNER_ONLY_MODE)
        .create(path)
        .unwrap_or_else(|error| panic!("create {}: {error}", path.display()));
}

/// A runtime root is a root because a caller handed it over as one, never
/// because of how its last component happens to be spelled. An operator whose
/// explicit root is itself named `contextdb` gets the runtime directory one
/// level inside it, like everybody else, created and validated owner-only --
/// not the root itself, whose contents and permissions are the operator's
/// business rather than this process's.
#[test]
#[cfg(unix)]
fn an_explicit_runtime_root_named_contextdb_still_holds_its_runtime_directory_inside_itself() {
    let base = short_runtime_base();
    let root = base.path().join("contextdb");
    create_owner_only_directory(&root);
    let current = LocalUserIdentity(nix::unistd::geteuid().as_raw() as u64);
    let inspector = SystemRuntimeDirectoryInspector;

    let prepared = prepare_runtime_directory(
        &RuntimeDirectoryRequest {
            explicit_root: Some(root.clone()),
            current_user: current,
        },
        &inspector,
    )
    .expect("an owner-only root named contextdb is a usable runtime root");
    assert_eq!(
        prepared.path,
        root.join("contextdb"),
        "the runtime directory is the root's contextdb child, whatever the root is called"
    );
    assert_eq!(prepared.source, RuntimeDirectorySource::Explicit);

    let facts = inspector
        .inspect(&prepared.path)
        .expect("inspect the prepared runtime directory");
    assert!(facts.is_directory);
    assert!(!facts.is_symbolic_link);
    assert_eq!(facts.owner, current);
    assert_eq!(
        facts.mode, OWNER_ONLY_MODE,
        "the runtime directory this process creates is owner-only"
    );
    assert_eq!(
        fs::read_dir(&root)
            .expect("read the operator's runtime root")
            .map(|entry| entry.expect("read runtime root entry").path())
            .collect::<Vec<_>>(),
        vec![root.join("contextdb")],
        "nothing is placed directly in the operator's root"
    );
}

/// The same for the root an operator points `XDG_RUNTIME_DIR` at: a root
/// named `contextdb` still holds its runtime directory inside itself, so
/// channels and reader breadcrumbs stay behind the owner-only child this
/// process creates instead of landing a level above it.
#[test]
#[cfg(target_os = "linux")]
fn an_xdg_runtime_root_named_contextdb_still_holds_its_runtime_directory_inside_itself() {
    let base = short_runtime_base();
    let root = base.path().join("contextdb");
    create_owner_only_directory(&root);
    let current = LocalUserIdentity(nix::unistd::geteuid().as_raw() as u64);
    let inspector = SystemRuntimeDirectoryInspector;

    let prepared = prepare_runtime_directory_with_environment(
        &RuntimeDirectoryRequest {
            explicit_root: None,
            current_user: current,
        },
        &inspector,
        &RuntimeEnvironment {
            xdg: Some(root.clone()),
            effective_user: current,
            macos_temporary: None,
        },
    )
    .expect("an owner-only XDG root named contextdb is a usable runtime root");
    assert_eq!(
        prepared.path,
        root.join("contextdb"),
        "the runtime directory is the XDG root's contextdb child, whatever the root is called"
    );
    assert_eq!(prepared.source, RuntimeDirectorySource::Xdg);

    let facts = inspector
        .inspect(&prepared.path)
        .expect("inspect the prepared runtime directory");
    assert!(facts.is_directory);
    assert_eq!(facts.owner, current);
    assert_eq!(
        facts.mode, OWNER_ONLY_MODE,
        "the runtime directory this process creates is owner-only"
    );
    assert_eq!(
        fs::read_dir(&root)
            .expect("read the operator's XDG runtime root")
            .map(|entry| entry.expect("read XDG runtime root entry").path())
            .collect::<Vec<_>>(),
        vec![root.join("contextdb")],
        "nothing is placed directly in the operator's XDG root"
    );
}
