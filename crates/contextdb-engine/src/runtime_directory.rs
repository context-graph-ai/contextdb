//! Shared private runtime directory creation and trust validation.

use std::path::Path;
#[cfg(target_os = "linux")]
use std::path::PathBuf;

use crate::local_transport::RuntimeDirectory;

pub(crate) fn reader_runtime_user() -> Option<u64> {
    #[cfg(unix)]
    {
        Some(nix::unistd::Uid::effective().as_raw() as u64)
    }
    #[cfg(not(unix))]
    {
        None
    }
}

pub(crate) fn validate_reader_runtime_directory(runtime_directory: &Path) -> std::io::Result<()> {
    if !runtime_directory.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "{} is not an absolute reader runtime directory",
                runtime_directory.display()
            ),
        ));
    }
    let metadata = std::fs::symlink_metadata(runtime_directory)?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "{} is {}",
                runtime_directory.display(),
                if metadata.file_type().is_symlink() {
                    "a symbolic link"
                } else {
                    "not a directory"
                }
            ),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;

        let Some(user) = reader_runtime_user() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "reader runtime user is unavailable",
            ));
        };
        if metadata.uid() as u64 != user
            || metadata.mode() & 0o077 != 0
            || metadata.mode() & 0o200 == 0
        {
            let reason = if metadata.uid() as u64 != user {
                format!("owned by user {}, expected user {user}", metadata.uid())
            } else if metadata.mode() & 0o020 != 0 {
                "group-writable".to_owned()
            } else if metadata.mode() & 0o002 != 0 {
                "world-writable".to_owned()
            } else if metadata.mode() & 0o077 != 0 {
                "accessible by group or others".to_owned()
            } else {
                "not writable by its owner".to_owned()
            };
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "{} is {reason} (mode {:04o})",
                    runtime_directory.display(),
                    metadata.mode() & 0o7777
                ),
            ));
        }
    }
    #[cfg(not(unix))]
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "reader runtime validation is unavailable on this platform",
        ));
    }
    Ok(())
}

/// Create a private directory without adopting an existing name.
pub(crate) fn create_private_directory(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        create_directory(path, false)
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir(path)
    }
}

/// Create missing directories privately, preserving every existing directory.
pub(crate) fn create_private_directory_all(path: &Path) -> std::io::Result<()> {
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    match create_private_directory(path) {
        Ok(()) => return Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => {
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    if let Some(parent) = path.parent() {
        create_private_directory_all(parent)?;
    }
    match create_private_directory(path) {
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => Ok(()),
        result => result,
    }
}

pub(crate) fn create_reader_breadcrumb_directory(path: &Path) -> std::io::Result<()> {
    create_directory(path, true)
}

fn create_directory(path: &Path, allow_existing: bool) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{
            DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _,
        };

        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        let created = match builder.create(path) {
            Ok(()) => true,
            Err(error) if allow_existing && error.kind() == std::io::ErrorKind::AlreadyExists => {
                false
            }
            Err(error) => return Err(error),
        };
        // Inspect both outcomes without following a replaced leaf. Only a
        // directory this call created may have its permissions changed.
        let directory = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?;
        let metadata = directory.metadata()?;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || Some(metadata.uid() as u64) != reader_runtime_user()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "{} is not a direct directory owned by the effective user (owner {}, expected {:?}, mode {:04o})",
                    path.display(),
                    metadata.uid(),
                    reader_runtime_user(),
                    metadata.mode() & 0o7777
                ),
            ));
        }
        if created && metadata.mode() & 0o777 != 0o700 {
            directory.set_permissions(std::fs::Permissions::from_mode(0o700))?;
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (path, allow_existing);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "reader breadcrumb directories are unavailable on this platform",
        ))
    }
}

pub(crate) fn fallback_reader_runtime_directory() -> Option<RuntimeDirectory> {
    #[cfg(target_os = "linux")]
    {
        crate::scratch::user_data_root()
            .map(|data| RuntimeDirectory::supplied(&data.join("contextdb").join("readers")))
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// The effective user's same-named primary group must list no other member.
#[cfg(target_os = "linux")]
pub(crate) mod private_primary_group {
    /// Decide from resolved records; the lookup wrapper authenticates the owner.
    pub(crate) fn ancestor_rule(
        directory_owner_uid: u32,
        directory_gid: u32,
        user_primary_gid: u32,
        user_login_name: &str,
        group_name: &str,
        group_member_names: &[String],
    ) -> Result<(), String> {
        if directory_gid != user_primary_gid {
            return Err(format!(
                "directory owned by user {directory_owner_uid} has group {directory_gid}, expected primary group {user_primary_gid}"
            ));
        }
        if group_name != user_login_name {
            return Err(format!(
                "directory owned by user {directory_owner_uid} has primary group {group_name}, expected login name {user_login_name}"
            ));
        }
        if let Some(member) = group_member_names
            .iter()
            .find(|member| *member != user_login_name)
        {
            return Err(format!(
                "directory owned by user {directory_owner_uid} has primary group {group_name} with another member {member}"
            ));
        }
        Ok(())
    }

    /// Lookup only the effective user's passwd record and primary group record.
    pub(super) fn owned_private_primary_group(metadata: &std::fs::Metadata) -> bool {
        use std::os::unix::fs::MetadataExt as _;

        let effective = nix::unistd::Uid::effective();
        if metadata.uid() != effective.as_raw() {
            return false;
        }
        let Ok(Some(user)) = nix::unistd::User::from_uid(effective) else {
            return false;
        };
        if metadata.gid() != user.gid.as_raw() {
            return false;
        }
        let Ok(Some(group)) = nix::unistd::Group::from_gid(user.gid) else {
            return false;
        };
        ancestor_rule(
            metadata.uid(),
            metadata.gid(),
            user.gid.as_raw(),
            &user.name,
            &group.name,
            &group.mem,
        )
        .is_ok()
    }
}

/// Validate every fallback prefix, including the parents that control rename
/// of the data root. The effective user's private primary group may write
/// owned ancestors; other shared writers require a sticky, user-owned child.
#[cfg(target_os = "linux")]
pub(crate) fn validate_fallback_reader_runtime(runtime: &RuntimeDirectory) -> std::io::Result<()> {
    if fallback_reader_runtime_exists(runtime)? {
        Ok(())
    } else {
        Err(std::io::Error::from_raw_os_error(libc::ENOENT))
    }
}

/// Inventory may omit an absent fallback only after authenticating every
/// existing ancestor. An unsafe ancestor or another I/O error must still
/// produce a candidate that the writer refuses to reclaim through.
#[cfg(target_os = "linux")]
pub(crate) fn fallback_reader_runtime_exists(runtime: &RuntimeDirectory) -> std::io::Result<bool> {
    use std::os::unix::fs::MetadataExt as _;

    let contextdb = runtime.path().parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "missing reader data parent",
        )
    })?;
    let data_root = contextdb.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "missing reader data root")
    })?;
    let user = reader_runtime_user().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "reader runtime user is unavailable",
        )
    })?;
    if !runtime.path().is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "reader data root is not absolute",
        ));
    }
    let mut prefix = PathBuf::new();
    let mut components = runtime.path().components().peekable();
    while let Some(component) = components.next() {
        prefix.push(component);
        let metadata = match std::fs::symlink_metadata(&prefix) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "{} is {}",
                    prefix.display(),
                    if metadata.file_type().is_symlink() {
                        "a symbolic link"
                    } else {
                        "not a directory"
                    }
                ),
            ));
        }
        if prefix == runtime.path() {
            validate_reader_runtime_directory(&prefix)?;
            continue;
        }
        let owned = metadata.uid() as u64 == user;
        let writable = metadata.mode() & 0o002 != 0
            || (metadata.mode() & 0o020 != 0
                && !private_primary_group::owned_private_primary_group(&metadata));
        let sticky_owned_child = metadata.mode() & 0o1000 != 0
            && components.peek().is_some_and(|next| {
                std::fs::symlink_metadata(prefix.join(next.as_os_str()))
                    .is_ok_and(|child| child.uid() as u64 == user)
            });
        let trusted_owner = owned || (!prefix.starts_with(data_root) && metadata.uid() == 0);
        let safe = trusted_owner && (!writable || sticky_owned_child);
        if !safe {
            let reason = if metadata.mode() & 0o020 != 0 {
                "group-writable".to_owned()
            } else if metadata.mode() & 0o002 != 0 {
                "world-writable".to_owned()
            } else {
                format!(
                    "owned by untrusted user {} (effective user {user})",
                    metadata.uid()
                )
            };
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "{} is {reason} (mode {:04o})",
                    prefix.display(),
                    metadata.mode() & 0o7777
                ),
            ));
        }
    }
    Ok(true)
}

/// Create missing ancestors, leaving existing ancestors through the data root
/// unchanged. Every new directory is private; existing modes are never changed.
#[cfg(target_os = "linux")]
pub(crate) fn prepare_fallback_reader_runtime(runtime: &RuntimeDirectory) -> std::io::Result<()> {
    let mut path = PathBuf::new();
    for component in runtime.path().components() {
        path.push(component);
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                if path == runtime.path() {
                    create_reader_breadcrumb_directory(&path)?;
                }
            }
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "{} is not a direct reader data directory (non-directory or symbolic link)",
                        path.display()
                    ),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                create_reader_breadcrumb_directory(&path)?;
            }
            Err(error) => return Err(error),
        }
    }
    validate_fallback_reader_runtime(runtime)
}

/// Prepare the shared private fallback for reader registration and the live
/// owner channel through the same creation and ancestor validation path.
pub(crate) fn prepare_fallback_runtime_directory() -> std::io::Result<Option<RuntimeDirectory>> {
    let Some(runtime) = fallback_reader_runtime_directory() else {
        return Ok(None);
    };
    #[cfg(target_os = "linux")]
    prepare_fallback_reader_runtime(&runtime)?;
    Ok(Some(runtime))
}
