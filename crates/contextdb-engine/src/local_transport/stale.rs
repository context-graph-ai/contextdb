use super::LocalTransportError;
#[cfg(unix)]
use super::{LocalHandshake, UnixLocalCarrier};
#[cfg(unix)]
use contextdb_core::read_contract::DeadlineClock;
#[cfg(unix)]
use std::path::PathBuf;

/// Identity evidence from the prior owner record and a bounded probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleIdentityEvidence {
    VerifiedOwner,
    WrongResponder,
    Unverifiable,
}

/// Bounded liveness evidence after identity has been checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LivenessEvidence {
    NoResponderBeforeDeadline,
    LiveResponder,
    Unverifiable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelFilesystemIdentity {
    pub device: u64,
    pub inode: u64,
}

/// Evidence produced by the production operation's final pathname identity
/// observation. Its private field prevents callers from manufacturing the
/// token passed to the race-safe removal primitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalStaleIdentityObservation {
    identity: ChannelFilesystemIdentity,
}

impl FinalStaleIdentityObservation {
    pub const fn identity(self) -> ChannelFilesystemIdentity {
        self.identity
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleFilesystemEvidence {
    Exact(ChannelFilesystemIdentity),
    Unverifiable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaleChannelEvidence {
    pub identity: StaleIdentityEvidence,
    pub liveness: LivenessEvidence,
    pub filesystem: StaleFilesystemEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleChannelAction {
    RemoveAndRetry,
    Preserve,
}

/// The old owner identity is supplied by the coordination layer; this module
/// never reads a durable coordination record itself.
#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleChannelProbe {
    pub path: PathBuf,
    pub expected_owner: LocalHandshake,
}

#[cfg(unix)]
pub fn channel_filesystem_identity(
    path: &std::path::Path,
) -> Result<ChannelFilesystemIdentity, LocalTransportError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
    Ok(ChannelFilesystemIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

/// Take down a channel this process itself bound.
///
/// A writer that has stopped serving must leave no socket behind for the next
/// reader to dial: a pathname with no listener is indistinguishable, from the
/// outside, from a live owner until the connect fails. Removal is bound to the
/// filesystem identity observed at the moment of removal, so a socket some
/// other process has since bound at the same pathname is preserved rather than
/// taken down with ours.
#[cfg(unix)]
pub fn remove_own_bound_channel(
    path: &std::path::Path,
    identity: ChannelFilesystemIdentity,
) -> Result<StaleChannelAction, LocalTransportError> {
    remove_channel_if_identity_matches(path, FinalStaleIdentityObservation { identity })
}

/// Remove only the directory entry represented by the final observation.
/// Implementations must use an identity-bound, descriptor-relative primitive
/// and preserve a different object that appears at the pathname.
#[cfg(unix)]
pub(super) fn remove_channel_if_identity_matches(
    path: &std::path::Path,
    observation: FinalStaleIdentityObservation,
) -> Result<StaleChannelAction, LocalTransportError> {
    use nix::fcntl::{AtFlags, OFlag, open};
    use nix::sys::stat::{Mode, SFlag, fstatat};
    use nix::unistd::{UnlinkatFlags, unlinkat};

    let parent = path.parent().ok_or_else(|| {
        LocalTransportError::FilesystemInspection("channel path has no parent directory".to_owned())
    })?;
    let name = path.file_name().ok_or_else(|| {
        LocalTransportError::FilesystemInspection("channel path has no final component".to_owned())
    })?;
    let directory = open(
        parent,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    )
    .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
    let current = match fstatat(&directory, name, AtFlags::AT_SYMLINK_NOFOLLOW) {
        Ok(metadata) => metadata,
        Err(_) => return Ok(StaleChannelAction::Preserve),
    };
    let current_identity = ChannelFilesystemIdentity {
        device: current.st_dev as u64,
        inode: current.st_ino as u64,
    };
    if current_identity != observation.identity()
        || (SFlag::from_bits_truncate(current.st_mode) & SFlag::S_IFMT) != SFlag::S_IFSOCK
    {
        return Ok(StaleChannelAction::Preserve);
    }
    match unlinkat(&directory, name, UnlinkatFlags::NoRemoveDir) {
        Ok(()) => Ok(StaleChannelAction::RemoveAndRetry),
        Err(_) => Ok(StaleChannelAction::Preserve),
    }
}

#[cfg(unix)]
pub(super) fn observe_final_stale_identity(
    path: &std::path::Path,
) -> Result<FinalStaleIdentityObservation, LocalTransportError> {
    Ok(FinalStaleIdentityObservation {
        identity: channel_filesystem_identity(path)?,
    })
}

/// Convenience path for callers that do not need to interpose diagnostics.
/// The carrier still binds probe evidence to the socket's exact filesystem
/// identity and conditionally preserves any replacement seen at removal.
#[cfg(unix)]
pub fn reconcile_stale_channel(
    carrier: &UnixLocalCarrier,
    probe: &StaleChannelProbe,
    clock: &dyn DeadlineClock,
    deadline_ms: u64,
) -> Result<StaleChannelAction, LocalTransportError> {
    let evidence = carrier.probe_stale(&probe.path, &probe.expected_owner, clock, deadline_ms)?;
    carrier.remove_after_proven_stale(&probe.path, evidence)
}

/// Decide whether an existing channel is proven stale. Removal still must
/// compare the exact filesystem identity immediately before a race-safe,
/// descriptor-relative removal.
pub fn decide_stale_channel_action(
    evidence: StaleChannelEvidence,
) -> Result<StaleChannelAction, LocalTransportError> {
    if evidence.identity == StaleIdentityEvidence::VerifiedOwner
        && evidence.liveness == LivenessEvidence::NoResponderBeforeDeadline
        && matches!(evidence.filesystem, StaleFilesystemEvidence::Exact(_))
    {
        Ok(StaleChannelAction::RemoveAndRetry)
    } else {
        Ok(StaleChannelAction::Preserve)
    }
}
