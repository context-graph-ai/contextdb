#[cfg(unix)]
use super::validate_socket_path_length;
use super::{LocalTransportError, validate_channel_addressability};
use contextdb_core::read_contract::ChannelAddress;
use std::fmt::Write;
use std::path::{Path, PathBuf};

/// Resolve a store pathname and derive its fixed-length local-channel key.
pub fn derive_channel_address(store_path: &Path) -> Result<ChannelAddress, LocalTransportError> {
    let resolved = std::fs::canonicalize(store_path)
        .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
    Ok(ChannelAddress(
        *blake3::hash(resolved.as_os_str().as_encoded_bytes()).as_bytes(),
    ))
}

/// The opaque filesystem name for one channel address.
pub fn opaque_channel_basename(address: ChannelAddress) -> String {
    let mut basename = String::with_capacity(address.0.len() * 2 + 5);
    for byte in address.0 {
        write!(&mut basename, "{byte:02x}").expect("writing into a string cannot fail");
    }
    basename.push_str(".sock");
    basename
}

/// Place one local channel below its validated runtime directory, never below
/// the inspected store.
pub fn channel_socket_path(
    runtime_directory: &Path,
    address: ChannelAddress,
) -> Result<PathBuf, LocalTransportError> {
    let path = runtime_directory.join(opaque_channel_basename(address));
    validate_channel_addressability(&path)?;
    Ok(path)
}

/// The address the kernel is handed for one channel.
///
/// The pathname a channel LIVES at and the address a bind or connect is given
/// are not required to be the same string, and on Linux they deliberately are
/// not. A Unix socket address is capped at 107 bytes, the fixed channel
/// basename is 69 of them, and an operator does not get to choose how deep
/// their service manager puts the runtime directory -- so addressing by the
/// absolute pathname made a live owner unreachable, and every reader saw a
/// running store as not serving.
///
/// So the runtime directory is held OPEN and the channel is addressed through
/// the held descriptor: `/proc/self/fd/<descriptor>/<basename>`, which is
/// bounded by the descriptor number and the fixed basename no matter where
/// the directory itself lives. Nothing else moves. The real directory is
/// still resolved and validated -- ownership, mode, symlink refusal -- against
/// its real pathname, the created socket is still permission-set and
/// re-inspected at its real pathname, and a path that already fits is still
/// used directly, so the ordinary deployment's behaviour is untouched.
///
/// macOS has no `/proc`, and therefore no way to name a held descriptor as a
/// path; there the absolute pathname remains the address and an over-long one
/// is refused exactly as before. Windows has no local channel at all.
#[cfg(unix)]
#[derive(Debug)]
pub struct ChannelKernelAddress {
    /// Held for as long as the address is: the address names this descriptor,
    /// so closing it would unname the address.
    held_directory: Option<std::os::fd::OwnedFd>,
    address: PathBuf,
}

#[cfg(unix)]
impl ChannelKernelAddress {
    /// Resolve the address to hand the kernel for a channel at this pathname.
    pub fn resolve(path: &Path) -> Result<Self, LocalTransportError> {
        if validate_socket_path_length(path).is_ok() {
            return Ok(Self {
                held_directory: None,
                address: path.to_path_buf(),
            });
        }
        Self::through_a_held_directory(path)
    }

    /// What bind or connect is given.
    pub fn as_path(&self) -> &Path {
        &self.address
    }

    /// Whether this address names a held directory descriptor rather than the
    /// channel's own pathname. A caller cannot tell from the address alone,
    /// and the difference is the whole point: it is what an over-long
    /// pathname costs, and what an ordinary one does not.
    pub fn holds_its_directory(&self) -> bool {
        self.held_directory.is_some()
    }

    #[cfg(target_os = "linux")]
    fn through_a_held_directory(path: &Path) -> Result<Self, LocalTransportError> {
        use std::os::fd::AsRawFd;

        let (directory, basename) = split_channel_pathname(path)?;
        let held = nix::fcntl::open(
            directory,
            nix::fcntl::OFlag::O_PATH
                | nix::fcntl::OFlag::O_DIRECTORY
                | nix::fcntl::OFlag::O_CLOEXEC,
            nix::sys::stat::Mode::empty(),
        )
        .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
        let address = PathBuf::from(format!("/proc/self/fd/{}", held.as_raw_fd())).join(basename);
        // The short form has to fit too; it is what the kernel is shown.
        validate_socket_path_length(&address)?;
        Ok(Self {
            held_directory: Some(held),
            address,
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn through_a_held_directory(path: &Path) -> Result<Self, LocalTransportError> {
        let _ = path;
        // No held-descriptor pathname exists here, so the absolute pathname is
        // the address and it does not fit.
        Err(LocalTransportError::ChannelPath(
            super::ChannelPathViolation::PathTooLong,
        ))
    }
}

#[cfg(all(unix, target_os = "linux"))]
fn split_channel_pathname(path: &Path) -> Result<(&Path, &std::ffi::OsStr), LocalTransportError> {
    let directory = path.parent().ok_or_else(|| {
        LocalTransportError::FilesystemInspection(
            "a channel pathname names no directory to hold open".to_owned(),
        )
    })?;
    let basename = path.file_name().ok_or_else(|| {
        LocalTransportError::FilesystemInspection("a channel pathname names no channel".to_owned())
    })?;
    Ok((directory, basename))
}
