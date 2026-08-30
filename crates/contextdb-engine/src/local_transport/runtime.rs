use super::LocalTransportError;
use contextdb_core::read_contract::LocalUserIdentity;
use std::path::{Path, PathBuf};

/// Owner-only permissions for the channel directory and channel.
pub const OWNER_ONLY_MODE: u32 = 0o700;

/// The single child of a runtime root that every contextdb runtime file lives
/// in: the channel sockets, and the reader breadcrumb directories beside them.
/// Which ROOT that child sits in differs by purpose -- a channel follows the
/// runtime directory its deployment stated, a reader breadcrumb is always in
/// the default per-user runtime location -- but the child's name never does.
pub const RUNTIME_DIRECTORY_NAME: &str = "contextdb";

/// Where one process keeps its local runtime files: the runtime root it was
/// given, and the runtime directory it owns inside that root.
///
/// A pathname cannot say which of the two it is -- an operator is free to
/// name their runtime root `contextdb` -- so the pair is settled once, by
/// which constructor the caller uses, and every later caller takes the part
/// it needs from this value. Nothing re-reads a path's spelling to decide
/// what it is: that guess would put a channel one level away from where the
/// other side dials it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeDirectory {
    root: PathBuf,
    directory: PathBuf,
}

impl RuntimeDirectory {
    /// The runtime directory inside a runtime root: always that root's
    /// `contextdb` child, whatever the root itself is named. This process
    /// creates and owns the child; the root stays the operator's.
    pub fn within(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            directory: root.join(RUNTIME_DIRECTORY_NAME),
        }
    }

    /// A directory supplied whole, which IS the channel directory.
    ///
    /// An operator who names a runtime directory for a packaged service, a
    /// container, or a Home Assistant add-on has that exact path created and
    /// owned for them, so this process uses it as given instead of creating a
    /// `contextdb` child inside it. Writer and reader must resolve a supplied
    /// directory the same way or the two sides look in different places, so
    /// this is the one shape both use.
    pub fn supplied(directory: &Path) -> Self {
        Self {
            root: directory.to_path_buf(),
            directory: directory.to_path_buf(),
        }
    }

    /// The runtime root: the directory whose ownership and permissions this
    /// process requires but never creates.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The runtime directory: where this process's channel sockets live, and
    /// where the reader breadcrumb directories of the runtime location this
    /// value names sit beside them.
    pub fn path(&self) -> &Path {
        &self.directory
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalPlatformAvailability {
    Available,
    PlatformUnsupported,
}

pub const fn local_platform_availability() -> LocalPlatformAvailability {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        LocalPlatformAvailability::Available
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        LocalPlatformAvailability::PlatformUnsupported
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeDirectorySource {
    Explicit,
    Xdg,
    PerUserFallback,
    MacosTemporary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeDirectoryRequest {
    pub explicit_root: Option<PathBuf>,
    pub current_user: LocalUserIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRuntimeDirectory {
    pub path: PathBuf,
    pub source: RuntimeDirectorySource,
}

/// Filesystem facts are injected so validation stays deterministic and can be
/// proven without privileged ownership manipulation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeDirectoryFacts {
    pub requested_root: PathBuf,
    pub resolved_root: PathBuf,
    pub is_absolute: bool,
    pub is_local: bool,
    pub is_directory: bool,
    pub is_symbolic_link: bool,
    pub owner: LocalUserIdentity,
    pub mode: u32,
    pub writable: bool,
    pub available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelPathFacts {
    pub path: PathBuf,
    pub runtime_directory: PathBuf,
    pub is_socket: bool,
    pub owner: LocalUserIdentity,
    pub mode: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelPathViolation {
    NotSocket,
    OutsideRuntimeDirectory,
    WrongOwner,
    InsecureMode,
    PathTooLong,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeRootViolation {
    Relative,
    NonLocal,
    NotDirectory,
    SymbolicLink,
    WrongOwner,
    InsecureMode,
    NotWritable,
    Unavailable,
}

pub trait RuntimeDirectoryInspector {
    fn inspect(&self, root: &Path) -> Result<RuntimeDirectoryFacts, LocalTransportError>;
}

/// Inputs that the operating system normally provides during runtime-root
/// resolution. The request itself carries no caller-selected fallback path.
pub trait RuntimeDirectoryEnvironment {
    fn xdg_runtime_directory(&self) -> Option<PathBuf>;

    /// Platform fallback roots are derived from the process identity, never
    /// from a caller-selected request value.
    fn effective_user_identity(&self) -> Result<LocalUserIdentity, LocalTransportError>;

    fn linux_per_user_runtime_directory(&self) -> Result<PathBuf, LocalTransportError> {
        Ok(PathBuf::from("/run/user").join(self.effective_user_identity()?.0.to_string()))
    }

    fn macos_user_temporary_directory(&self) -> Result<PathBuf, LocalTransportError>;
}

#[derive(Debug, Default)]
pub struct ProcessRuntimeDirectoryEnvironment;

impl RuntimeDirectoryEnvironment for ProcessRuntimeDirectoryEnvironment {
    fn xdg_runtime_directory(&self) -> Option<PathBuf> {
        std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from)
    }

    #[cfg(unix)]
    fn effective_user_identity(&self) -> Result<LocalUserIdentity, LocalTransportError> {
        Ok(LocalUserIdentity(nix::unistd::geteuid().as_raw() as u64))
    }

    #[cfg(not(unix))]
    fn effective_user_identity(&self) -> Result<LocalUserIdentity, LocalTransportError> {
        Err(LocalTransportError::PlatformUnsupported)
    }

    #[cfg(target_os = "macos")]
    fn macos_user_temporary_directory(&self) -> Result<PathBuf, LocalTransportError> {
        use std::os::unix::ffi::OsStringExt;

        // `/usr/bin/getconf DARWIN_USER_TEMP_DIR` is the native, safe wrapper
        // around `_CS_DARWIN_USER_TEMP_DIR`. Clearing the child environment
        // keeps caller-controlled TMPDIR and locale settings out of the
        // authority path.
        let output = std::process::Command::new("/usr/bin/getconf")
            .arg("DARWIN_USER_TEMP_DIR")
            .env_clear()
            .output()
            .map_err(|_| LocalTransportError::RuntimeRoot(RuntimeRootViolation::Unavailable))?;
        if !output.status.success() || !output.stderr.is_empty() {
            return Err(LocalTransportError::RuntimeRoot(
                RuntimeRootViolation::Unavailable,
            ));
        }
        let Some(path_bytes) = output.stdout.strip_suffix(b"\n") else {
            return Err(LocalTransportError::RuntimeRoot(
                RuntimeRootViolation::Unavailable,
            ));
        };
        if path_bytes.is_empty()
            || path_bytes.contains(&0)
            || path_bytes.contains(&b'\n')
            || path_bytes.contains(&b'\r')
        {
            return Err(LocalTransportError::RuntimeRoot(
                RuntimeRootViolation::Unavailable,
            ));
        }
        let path = PathBuf::from(std::ffi::OsString::from_vec(path_bytes.to_vec()));
        if !path.is_absolute() {
            return Err(LocalTransportError::RuntimeRoot(
                RuntimeRootViolation::Unavailable,
            ));
        }
        Ok(path)
    }

    #[cfg(not(target_os = "macos"))]
    fn macos_user_temporary_directory(&self) -> Result<PathBuf, LocalTransportError> {
        Err(LocalTransportError::PlatformUnsupported)
    }
}

/// Production metadata inspection for the root and socket paths. It follows
/// no symlink for the security decision and records locality separately.
#[derive(Debug, Default)]
pub struct SystemRuntimeDirectoryInspector;

impl RuntimeDirectoryInspector for SystemRuntimeDirectoryInspector {
    fn inspect(&self, root: &Path) -> Result<RuntimeDirectoryFacts, LocalTransportError> {
        inspect_system_runtime_root(root)
    }
}

/// Shape rules that hold for every runtime directory, whether it is the
/// caller-supplied container or the owner-only channel directory below it. A
/// symbolic link is reported as a link rather than as a non-directory: it is
/// the more specific refusal and the one an operator has to act on.
fn validate_runtime_root_shape(
    facts: &RuntimeDirectoryFacts,
    current_user: LocalUserIdentity,
) -> Result<(), LocalTransportError> {
    if !facts.available {
        return Err(LocalTransportError::RuntimeRoot(
            RuntimeRootViolation::Unavailable,
        ));
    }
    if !facts.is_absolute {
        return Err(LocalTransportError::RuntimeRoot(
            RuntimeRootViolation::Relative,
        ));
    }
    if !facts.is_local {
        return Err(LocalTransportError::RuntimeRoot(
            RuntimeRootViolation::NonLocal,
        ));
    }
    if facts.is_symbolic_link {
        return Err(LocalTransportError::RuntimeRoot(
            RuntimeRootViolation::SymbolicLink,
        ));
    }
    if !facts.is_directory {
        return Err(LocalTransportError::RuntimeRoot(
            RuntimeRootViolation::NotDirectory,
        ));
    }
    if facts.owner != current_user {
        return Err(LocalTransportError::RuntimeRoot(
            RuntimeRootViolation::WrongOwner,
        ));
    }
    if !facts.writable {
        return Err(LocalTransportError::RuntimeRoot(
            RuntimeRootViolation::NotWritable,
        ));
    }
    Ok(())
}

pub fn validate_runtime_root(
    facts: &RuntimeDirectoryFacts,
    current_user: LocalUserIdentity,
) -> Result<(), LocalTransportError> {
    validate_runtime_root_shape(facts, current_user)?;
    if facts.mode != OWNER_ONLY_MODE {
        return Err(LocalTransportError::RuntimeRoot(
            RuntimeRootViolation::InsecureMode,
        ));
    }
    Ok(())
}

/// A caller-supplied explicit root is the container the channel directory is
/// created inside, not the channel directory itself: it is required to be an
/// absolute, local, non-symlink directory the effective channel owner owns and
/// can write, but its own permission bits stay the caller's choice. The
/// owner-only boundary is the `contextdb` child created below it and the
/// channel socket inside that child, both of which are created and then
/// validated against the full owner-only rule.
fn validate_runtime_container_root(
    facts: &RuntimeDirectoryFacts,
    current_user: LocalUserIdentity,
) -> Result<(), LocalTransportError> {
    validate_runtime_root_shape(facts, current_user)
}

pub fn validate_channel_path(
    facts: &ChannelPathFacts,
    current_user: LocalUserIdentity,
) -> Result<(), LocalTransportError> {
    validate_channel_addressability(&facts.path)?;
    if !facts.is_socket {
        return Err(LocalTransportError::ChannelPath(
            ChannelPathViolation::NotSocket,
        ));
    }
    if facts.path.parent() != Some(facts.runtime_directory.as_path()) {
        return Err(LocalTransportError::ChannelPath(
            ChannelPathViolation::OutsideRuntimeDirectory,
        ));
    }
    if facts.owner != current_user {
        return Err(LocalTransportError::ChannelPath(
            ChannelPathViolation::WrongOwner,
        ));
    }
    if facts.mode != OWNER_ONLY_MODE {
        return Err(LocalTransportError::ChannelPath(
            ChannelPathViolation::InsecureMode,
        ));
    }
    Ok(())
}

/// Maximum encoded pathname length excluding the trailing NUL required by the
/// platform `sockaddr_un` representation.
pub const fn unix_socket_path_limit() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        Some(107)
    }
    #[cfg(target_os = "macos")]
    {
        Some(103)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

/// The limit applies to what is handed to the kernel, so this is checked on
/// the address a bind or a connect actually receives -- never on a pathname
/// the operating system is never shown.
pub fn validate_socket_path_length(path: &Path) -> Result<(), LocalTransportError> {
    let Some(limit) = unix_socket_path_limit() else {
        return Err(LocalTransportError::PlatformUnsupported);
    };
    if path.as_os_str().as_encoded_bytes().len() > limit {
        return Err(LocalTransportError::ChannelPath(
            ChannelPathViolation::PathTooLong,
        ));
    }
    Ok(())
}

/// Whether a channel below this directory can be addressed at all.
///
/// A Unix socket address is capped at 107 bytes on Linux and 103 on macOS,
/// and the channel basename alone is 69 of them. An operator whose service
/// manager hands the owner a normally-named runtime directory -- something
/// like `/var/lib/<service>/runtime` -- is left with barely thirty characters
/// of headroom, and past that the owner could not create a channel and every
/// reader saw a live store as not serving. That is a channel the operator's
/// directory layout silently took away.
///
/// On Linux the directory is held open and the channel is addressed through
/// the held descriptor (see `ChannelKernelAddress`), so the operator's
/// directory depth stops being part of the question: what the kernel is shown
/// is a short, fixed-shape address. On platforms with no equivalent -- macOS,
/// which has no `/proc` -- the absolute pathname IS the address, so the limit
/// still applies there and is reported the same way it always was.
pub fn validate_channel_addressability(path: &Path) -> Result<(), LocalTransportError> {
    if unix_socket_path_limit().is_none() {
        return Err(LocalTransportError::PlatformUnsupported);
    }
    #[cfg(target_os = "linux")]
    {
        let _ = path;
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        validate_socket_path_length(path)
    }
}

pub fn resolve_runtime_directory(
    request: &RuntimeDirectoryRequest,
    inspector: &dyn RuntimeDirectoryInspector,
) -> Result<ResolvedRuntimeDirectory, LocalTransportError> {
    let environment = ProcessRuntimeDirectoryEnvironment;
    let mut authoritative_request = request.clone();
    authoritative_request.current_user = environment.effective_user_identity()?;
    resolve_runtime_directory_with_environment(&authoritative_request, inspector, &environment)
}

pub fn resolve_runtime_directory_with_environment(
    request: &RuntimeDirectoryRequest,
    inspector: &dyn RuntimeDirectoryInspector,
    environment: &dyn RuntimeDirectoryEnvironment,
) -> Result<ResolvedRuntimeDirectory, LocalTransportError> {
    let (runtime, source) =
        resolve_runtime_layout_with_environment(request, inspector, environment)?;
    Ok(ResolvedRuntimeDirectory {
        path: runtime.path().to_path_buf(),
        source,
    })
}

/// The same resolution, keeping the runtime root beside the directory it
/// names for a caller that needs both.
fn resolve_runtime_layout_with_environment(
    request: &RuntimeDirectoryRequest,
    inspector: &dyn RuntimeDirectoryInspector,
    environment: &dyn RuntimeDirectoryEnvironment,
) -> Result<(RuntimeDirectory, RuntimeDirectorySource), LocalTransportError> {
    if local_platform_availability() == LocalPlatformAvailability::PlatformUnsupported {
        return Err(LocalTransportError::PlatformUnsupported);
    }

    let (root, source) = if let Some(explicit) = request.explicit_root.clone() {
        (explicit, RuntimeDirectorySource::Explicit)
    } else {
        #[cfg(target_os = "linux")]
        {
            match environment.xdg_runtime_directory() {
                Some(root) => (root, RuntimeDirectorySource::Xdg),
                None => (
                    environment.linux_per_user_runtime_directory()?,
                    RuntimeDirectorySource::PerUserFallback,
                ),
            }
        }
        #[cfg(target_os = "macos")]
        {
            (
                environment.macos_user_temporary_directory()?,
                RuntimeDirectorySource::MacosTemporary,
            )
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            return Err(LocalTransportError::PlatformUnsupported);
        }
    };
    let facts = inspector.inspect(&root)?;
    match source {
        RuntimeDirectorySource::Explicit => {
            validate_runtime_container_root(&facts, request.current_user)?
        }
        RuntimeDirectorySource::Xdg
        | RuntimeDirectorySource::PerUserFallback
        | RuntimeDirectorySource::MacosTemporary => {
            validate_runtime_root(&facts, request.current_user)?
        }
    }
    let runtime = RuntimeDirectory::within(&facts.resolved_root);
    validate_channel_addressability(
        &runtime
            .path()
            .join("0000000000000000000000000000000000000000000000000000000000000000.sock"),
    )?;
    Ok((runtime, source))
}

/// The ONE runtime directory a process uses for this store's local CHANNEL:
/// where a writer binds it and a reader dials it.
///
/// There is exactly one resolution, here, because both sides of a deployment
/// have to land on the same answer. A directory an operator supplied IS the
/// directory -- a container or packaged service has that exact path created
/// and owned for it -- so it is used as given; with nothing supplied, the
/// platform base's owner-only `contextdb` child is prepared and used. Callers
/// carry the resolved value; re-deriving a directory from the root inside it
/// is how a writer and the readers dialling it end up in different places.
///
/// Reader BREADCRUMBS are a different question and this does not answer it.
/// A supplied root is one deployment's statement about its own channel, and
/// the writer that a direct reader blocks is started by somebody else -- very
/// often a supervisor with nothing but the store path. So a breadcrumb always
/// goes in the DEFAULT per-user runtime location, whatever root either side
/// was given: one location, no flag for the two of them to disagree on.
pub fn runtime_directory_for_store(
    supplied: Option<&Path>,
    current_user: LocalUserIdentity,
) -> Result<RuntimeDirectory, LocalTransportError> {
    let Some(supplied) = supplied else {
        return prepare_runtime_layout(
            &RuntimeDirectoryRequest {
                explicit_root: None,
                current_user,
            },
            &SystemRuntimeDirectoryInspector,
        );
    };
    let facts = SystemRuntimeDirectoryInspector.inspect(supplied)?;
    validate_runtime_root(&facts, current_user)?;
    Ok(RuntimeDirectory::supplied(&facts.resolved_root))
}

/// Resolve and validate the platform-native base, then create and validate
/// its owner-only `contextdb` child. Callers cannot supply any emergency or
/// temporary fallback path through this API.
pub fn prepare_runtime_directory(
    request: &RuntimeDirectoryRequest,
    inspector: &dyn RuntimeDirectoryInspector,
) -> Result<ResolvedRuntimeDirectory, LocalTransportError> {
    let environment = ProcessRuntimeDirectoryEnvironment;
    let mut authoritative_request = request.clone();
    authoritative_request.current_user = environment.effective_user_identity()?;
    prepare_runtime_directory_with_environment(&authoritative_request, inspector, &environment)
}

pub fn prepare_runtime_directory_with_environment(
    request: &RuntimeDirectoryRequest,
    inspector: &dyn RuntimeDirectoryInspector,
    environment: &dyn RuntimeDirectoryEnvironment,
) -> Result<ResolvedRuntimeDirectory, LocalTransportError> {
    let (runtime, source) =
        prepare_runtime_layout_with_environment(request, inspector, environment)?;
    Ok(ResolvedRuntimeDirectory {
        path: runtime.path().to_path_buf(),
        source,
    })
}

/// Resolve and prepare the runtime directory, and hand back the root it sits
/// in along with it, so a caller that must name both -- the directory the
/// channel is bound in, and the root it sits inside -- is never left deriving
/// one from the other.
pub fn prepare_runtime_layout(
    request: &RuntimeDirectoryRequest,
    inspector: &dyn RuntimeDirectoryInspector,
) -> Result<RuntimeDirectory, LocalTransportError> {
    let environment = ProcessRuntimeDirectoryEnvironment;
    let mut authoritative_request = request.clone();
    authoritative_request.current_user = environment.effective_user_identity()?;
    prepare_runtime_layout_with_environment(&authoritative_request, inspector, &environment)
        .map(|(runtime, _)| runtime)
}

fn prepare_runtime_layout_with_environment(
    request: &RuntimeDirectoryRequest,
    inspector: &dyn RuntimeDirectoryInspector,
    environment: &dyn RuntimeDirectoryEnvironment,
) -> Result<(RuntimeDirectory, RuntimeDirectorySource), LocalTransportError> {
    let (runtime, source) =
        resolve_runtime_layout_with_environment(request, inspector, environment)?;
    match inspector.inspect(runtime.path()) {
        Ok(facts) if facts.available => validate_runtime_root(&facts, request.current_user)?,
        Ok(_) | Err(LocalTransportError::RuntimeRoot(RuntimeRootViolation::Unavailable)) => {
            create_owner_only_runtime_child(runtime.path())?;
            let facts = inspector.inspect(runtime.path())?;
            validate_runtime_root(&facts, request.current_user)?;
        }
        Err(error) => return Err(error),
    }
    Ok((runtime, source))
}

fn create_owner_only_runtime_child(path: &Path) -> Result<(), LocalTransportError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;

        let mut builder = std::fs::DirBuilder::new();
        builder.mode(OWNER_ONLY_MODE);
        match builder.create(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(error) => Err(LocalTransportError::FilesystemInspection(error.to_string())),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(LocalTransportError::PlatformUnsupported)
    }
}

/// Inspect an existing channel object without opening, connecting, or
/// removing it. The caller validates the returned facts against its owner.
pub fn inspect_channel_path(
    path: &Path,
    runtime_directory: &Path,
) -> Result<ChannelPathFacts, LocalTransportError> {
    inspect_system_channel_path(path, runtime_directory)
}

#[cfg(unix)]
fn inspect_system_runtime_root(root: &Path) -> Result<RuntimeDirectoryFacts, LocalTransportError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = match std::fs::symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RuntimeDirectoryFacts {
                requested_root: root.to_path_buf(),
                resolved_root: root.to_path_buf(),
                is_absolute: root.is_absolute(),
                is_local: false,
                is_directory: false,
                is_symbolic_link: false,
                owner: LocalUserIdentity(0),
                mode: 0,
                writable: false,
                available: false,
            });
        }
        Err(error) => return Err(LocalTransportError::FilesystemInspection(error.to_string())),
    };
    let is_symbolic_link = metadata.file_type().is_symlink();
    let is_directory = metadata.file_type().is_dir();
    let resolved_root = std::fs::canonicalize(root)
        .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
    Ok(RuntimeDirectoryFacts {
        requested_root: root.to_path_buf(),
        resolved_root,
        is_absolute: root.is_absolute(),
        is_local: filesystem_is_local(root)?,
        is_directory,
        is_symbolic_link,
        owner: LocalUserIdentity(metadata.uid() as u64),
        mode: metadata.mode() & 0o777,
        writable: filesystem_is_writable(root)?,
        available: true,
    })
}

#[cfg(not(unix))]
fn inspect_system_runtime_root(_root: &Path) -> Result<RuntimeDirectoryFacts, LocalTransportError> {
    Err(LocalTransportError::PlatformUnsupported)
}

#[cfg(unix)]
fn inspect_system_channel_path(
    path: &Path,
    runtime_directory: &Path,
) -> Result<ChannelPathFacts, LocalTransportError> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};

    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
    Ok(ChannelPathFacts {
        path: path.to_path_buf(),
        runtime_directory: runtime_directory.to_path_buf(),
        is_socket: metadata.file_type().is_socket(),
        owner: LocalUserIdentity(metadata.uid() as u64),
        mode: metadata.mode() & 0o777,
    })
}

#[cfg(not(unix))]
fn inspect_system_channel_path(
    _path: &Path,
    _runtime_directory: &Path,
) -> Result<ChannelPathFacts, LocalTransportError> {
    Err(LocalTransportError::PlatformUnsupported)
}

/// Writability is decided for the identity the channel actually runs as. The
/// real-user `access(2)` answer is wrong for any set-user-identity or
/// privilege-dropping process, so the check evaluates the effective identity
/// and treats every failure as not writable.
#[cfg(unix)]
fn filesystem_is_writable(path: &Path) -> Result<bool, LocalTransportError> {
    let evaluated = nix::unistd::faccessat(
        nix::fcntl::AT_FDCWD,
        path,
        nix::unistd::AccessFlags::W_OK,
        nix::fcntl::AtFlags::AT_EACCESS,
    );
    Ok(evaluated.is_ok())
}

/// A filesystem kind is a fixed 32-bit magic, but the word the operating
/// system reports it through differs by target: a magic whose high bit is set
/// arrives widened on one and sign-extended on another. Both spellings name
/// the same kind, so the reported word is read back as the magic it carries
/// before the set is consulted.
#[cfg(target_os = "linux")]
#[doc(hidden)]
pub const fn linux_filesystem_type_is_local(reported: i64) -> bool {
    let kind = if reported < 0 {
        reported as u32 as i64
    } else {
        reported
    };
    kind == 0x0000_ef53 // ext2/3/4
        || kind == 0x0102_1994 // tmpfs
        || kind == 0x8584_58f6 // ramfs
        || kind == 0x5846_5342 // xfs
        || kind == 0x9123_683e // btrfs
        || kind == 0x794c_7630 // overlayfs
        || kind == 0xf2f5_2010 // f2fs
        || kind == 0x2fc1_2fc1 // zfs
}

#[cfg(target_os = "linux")]
fn filesystem_is_local(path: &Path) -> Result<bool, LocalTransportError> {
    let filesystem = nix::sys::statfs::statfs(path)
        .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
    Ok(linux_filesystem_type_is_local(
        filesystem.filesystem_type().0 as i64,
    ))
}

#[cfg(target_os = "macos")]
fn filesystem_is_local(path: &Path) -> Result<bool, LocalTransportError> {
    let filesystem = nix::sys::statfs::statfs(path)
        .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
    Ok(filesystem.flags().contains(nix::mount::MntFlags::MNT_LOCAL))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn filesystem_is_local(_path: &Path) -> Result<bool, LocalTransportError> {
    Err(LocalTransportError::PlatformUnsupported)
}
