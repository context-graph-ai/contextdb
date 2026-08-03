use super::{TransportError, TransportResult};
use serde::{Deserialize, Serialize};
use std::fs::File;
#[cfg(test)]
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::{
    ffi::CString,
    os::unix::ffi::OsStrExt,
    os::unix::io::{AsRawFd, FromRawFd},
};

pub(super) const CONTROL_SUBJECT: &str = "__contextdb_large_request_v1__";
pub(super) const FRAGMENT_BYTES: usize = 4 * 1024 * 1024;

const MANIFEST_VERSION: u8 = 1;
const CONTROL_MAGIC: &[u8; 4] = b"CGRQ";
const CONTROL_VERSION: u8 = 1;
const BEGIN_KIND: u8 = 1;
const FRAGMENT_KIND: u8 = 2;
const PROGRESS_MAGIC: &[u8; 4] = b"CGRP";
const BEGIN_PROGRESS_KIND: u8 = 1;
const FRAGMENT_PROGRESS_KIND: u8 = 2;
pub(super) const MAX_REQUEST_BEGIN_BYTES: usize = 16 * 1024;
// Fragments repeat the descriptor identity so the durable stage can reject a
// swapped cached descriptor. Their envelope therefore includes the bounded
// Begin subject as well as fragment bytes; this remains a framing guard, not
// a new request-payload limit.
pub(super) const MAX_REQUEST_FRAGMENT_ENVELOPE_BYTES: usize =
    FRAGMENT_BYTES + MAX_REQUEST_BEGIN_BYTES + 4096;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "body")]
pub(super) enum LargeRequestControl {
    BeginV1(LargeRequestBegin),
    FragmentV1(LargeRequestFragment),
}

impl LargeRequestControl {
    pub(super) fn encode(&self) -> TransportResult<Vec<u8>> {
        let (kind, body) = match self {
            Self::BeginV1(begin) => (BEGIN_KIND, rmp_serde::to_vec_named(begin)),
            Self::FragmentV1(fragment) => (FRAGMENT_KIND, rmp_serde::to_vec_named(fragment)),
        };
        let body = body.map_err(|err| {
            TransportError::Other(format!("cannot encode oversized request control: {err}"))
        })?;
        if kind == BEGIN_KIND && body.len() > MAX_REQUEST_BEGIN_BYTES {
            return Err(TransportError::Other(
                "oversized request descriptor exceeds its control ceiling".to_string(),
            ));
        }
        if kind == FRAGMENT_KIND && body.len() > MAX_REQUEST_FRAGMENT_ENVELOPE_BYTES {
            return Err(TransportError::Other(
                "oversized request fragment envelope exceeds its frame ceiling".to_string(),
            ));
        }
        let mut encoded = Vec::with_capacity(CONTROL_MAGIC.len() + 2 + body.len());
        encoded.extend_from_slice(CONTROL_MAGIC);
        encoded.push(CONTROL_VERSION);
        encoded.push(kind);
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }

    pub(super) fn decode(encoded: &[u8]) -> TransportResult<Self> {
        let Some((&version, rest)) = encoded
            .strip_prefix(CONTROL_MAGIC)
            .and_then(|bytes| bytes.split_first())
        else {
            return Err(TransportError::IncompleteReply(
                "invalid oversized request control tag".to_string(),
            ));
        };
        let Some((&kind, body)) = rest.split_first() else {
            return Err(TransportError::IncompleteReply(
                "invalid oversized request control header".to_string(),
            ));
        };
        if version != CONTROL_VERSION {
            return Err(TransportError::IncompleteReply(
                "unsupported oversized request control version".to_string(),
            ));
        }
        match kind {
            BEGIN_KIND => {
                if body.len() > MAX_REQUEST_BEGIN_BYTES {
                    return Err(TransportError::IncompleteReply(
                        "oversized request descriptor exceeds its control ceiling".to_string(),
                    ));
                }
                let begin: LargeRequestBegin = rmp_serde::from_slice(body).map_err(|err| {
                    TransportError::IncompleteReply(format!(
                        "invalid oversized request descriptor: {err}"
                    ))
                })?;
                begin.validate()?;
                Ok(Self::BeginV1(begin))
            }
            FRAGMENT_KIND => {
                if body.len() > MAX_REQUEST_FRAGMENT_ENVELOPE_BYTES {
                    return Err(TransportError::IncompleteReply(
                        "oversized request fragment envelope exceeds its frame ceiling".to_string(),
                    ));
                }
                let fragment: LargeRequestFragment =
                    rmp_serde::from_slice(body).map_err(|err| {
                        TransportError::IncompleteReply(format!(
                            "invalid oversized request fragment envelope: {err}"
                        ))
                    })?;
                Ok(Self::FragmentV1(fragment))
            }
            _ => Err(TransportError::IncompleteReply(
                "unsupported oversized request control kind".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct LargeRequestBegin {
    pub subject: String,
    pub unit_digest: [u8; blake3::OUT_LEN],
    pub total_bytes: u64,
    pub total_fragments: u32,
    pub transfer_digest: [u8; blake3::OUT_LEN],
}

impl LargeRequestBegin {
    pub(super) fn new(
        subject: &str,
        unit_digest: [u8; blake3::OUT_LEN],
        total_bytes: usize,
    ) -> TransportResult<Self> {
        let total_bytes = u64::try_from(total_bytes).map_err(|_| {
            TransportError::Other(
                "oversized request length cannot be represented on this platform".to_string(),
            )
        })?;
        let total_fragments = u32::try_from(fragment_count(usize::try_from(total_bytes).map_err(
            |_| {
                TransportError::Other(
                    "oversized request length cannot be represented on this platform".to_string(),
                )
            },
        )?))
        .map_err(|_| {
            TransportError::Other("oversized request has too many fragments".to_string())
        })?;
        let transfer_digest =
            request_transfer_digest(subject, unit_digest, total_bytes, total_fragments);
        Ok(Self {
            subject: subject.to_string(),
            unit_digest,
            total_bytes,
            total_fragments,
            transfer_digest,
        })
    }

    pub(super) fn validate(&self) -> TransportResult<()> {
        if self.subject.is_empty() || self.subject == CONTROL_SUBJECT || self.total_fragments < 2 {
            return Err(TransportError::Other(
                "oversized request descriptor has invalid bounds".to_string(),
            ));
        }
        let total = usize::try_from(self.total_bytes).map_err(|_| {
            TransportError::Other(
                "oversized request length cannot be represented on this platform".to_string(),
            )
        })?;
        if fragment_count(total) != self.total_fragments as usize
            || self.transfer_digest
                != request_transfer_digest(
                    &self.subject,
                    self.unit_digest,
                    self.total_bytes,
                    self.total_fragments,
                )
        {
            return Err(TransportError::Other(
                "oversized request descriptor identity does not match its bounds".to_string(),
            ));
        }
        Ok(())
    }

    pub(super) fn descriptor_digest(&self) -> TransportResult<[u8; blake3::OUT_LEN]> {
        let bytes = rmp_serde::to_vec_named(self).map_err(|err| {
            TransportError::Other(format!("cannot encode oversized request descriptor: {err}"))
        })?;
        Ok(*blake3::hash(&bytes).as_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct LargeRequestProgress {
    pub node_id: String,
    pub descriptor_digest: [u8; blake3::OUT_LEN],
    pub transfer_digest: [u8; blake3::OUT_LEN],
    pub next_missing: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum LargeRequestControlReply {
    BeginProgressV1(LargeRequestProgress),
    FragmentProgressV1(LargeRequestProgress),
}

impl LargeRequestControlReply {
    pub(super) fn encode(&self) -> TransportResult<Vec<u8>> {
        let (kind, progress) = match self {
            Self::BeginProgressV1(progress) => (BEGIN_PROGRESS_KIND, progress),
            Self::FragmentProgressV1(progress) => (FRAGMENT_PROGRESS_KIND, progress),
        };
        let body = rmp_serde::to_vec_named(progress).map_err(|err| {
            TransportError::Other(format!("cannot encode oversized request progress: {err}"))
        })?;
        let mut encoded = Vec::with_capacity(PROGRESS_MAGIC.len() + 2 + body.len());
        encoded.extend_from_slice(PROGRESS_MAGIC);
        encoded.push(CONTROL_VERSION);
        encoded.push(kind);
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }

    pub(super) fn decode(encoded: &[u8]) -> Option<Self> {
        let (&version, rest) = encoded.strip_prefix(PROGRESS_MAGIC)?.split_first()?;
        let (&kind, body) = rest.split_first()?;
        if version != CONTROL_VERSION || body.len() > MAX_REQUEST_BEGIN_BYTES {
            return None;
        }
        let progress: LargeRequestProgress = rmp_serde::from_slice(body).ok()?;
        match kind {
            BEGIN_PROGRESS_KIND => Some(Self::BeginProgressV1(progress)),
            FRAGMENT_PROGRESS_KIND => Some(Self::FragmentProgressV1(progress)),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct LargeRequestFragment {
    pub subject: String,
    pub unit_digest: [u8; blake3::OUT_LEN],
    pub transfer_digest: [u8; blake3::OUT_LEN],
    pub fragment_digest: [u8; blake3::OUT_LEN],
    pub total_bytes: u64,
    pub sequence: u32,
    pub total_fragments: u32,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

impl LargeRequestFragment {
    pub(super) fn encode(
        subject: &str,
        unit_digest: [u8; blake3::OUT_LEN],
        total_bytes: usize,
        sequence: usize,
        total_fragments: usize,
        payload: &[u8],
    ) -> TransportResult<Vec<u8>> {
        let fragment = Self {
            subject: subject.to_string(),
            unit_digest,
            transfer_digest: request_transfer_digest(
                subject,
                unit_digest,
                u64::try_from(total_bytes).map_err(|_| {
                    TransportError::Other(
                        "oversized request length cannot be represented on this platform"
                            .to_string(),
                    )
                })?,
                u32::try_from(total_fragments).map_err(|_| {
                    TransportError::Other("oversized request has too many fragments".to_string())
                })?,
            ),
            fragment_digest: *blake3::hash(payload).as_bytes(),
            total_bytes: u64::try_from(total_bytes).map_err(|_| {
                TransportError::Other(
                    "oversized request length cannot be represented on this platform".to_string(),
                )
            })?,
            sequence: u32::try_from(sequence).map_err(|_| {
                TransportError::Other("oversized request has too many fragments".to_string())
            })?,
            total_fragments: u32::try_from(total_fragments).map_err(|_| {
                TransportError::Other("oversized request has too many fragments".to_string())
            })?,
            payload: payload.to_vec(),
        };
        rmp_serde::to_vec_named(&fragment).map_err(|err| {
            TransportError::Other(format!("cannot encode oversized request fragment: {err}"))
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StageManifest {
    version: u8,
    subject: String,
    authenticated_node_id: String,
    unit_digest: [u8; blake3::OUT_LEN],
    total_bytes: u64,
    total_fragments: u32,
    transfer_digest: [u8; blake3::OUT_LEN],
}

#[derive(Debug)]
pub(super) enum StageOutcome {
    Pending {
        next_missing: u32,
    },
    Complete {
        subject: String,
        payload: Vec<u8>,
        completed_path: PathBuf,
    },
}

#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StageSnapshot {
    pub subject: String,
    pub authenticated_node_id: String,
    pub unit_digest: [u8; blake3::OUT_LEN],
    pub total_bytes: u64,
    pub total_fragments: u32,
    pub fragments: Vec<StagedFragmentSnapshot>,
}

#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StagedFragmentSnapshot {
    pub sequence: u32,
    pub bytes: u64,
    pub digest: [u8; blake3::OUT_LEN],
}

pub(super) fn fragment_count(total_bytes: usize) -> usize {
    total_bytes.div_ceil(FRAGMENT_BYTES)
}

pub(super) fn accept_descriptor_fragment(
    stage_root: &Path,
    authenticated_node_id: &str,
    begin: &LargeRequestBegin,
    fragment: &LargeRequestFragment,
) -> TransportResult<StageOutcome> {
    begin.validate()?;
    let manifest = stage_manifest(authenticated_node_id, begin);
    let completed_path = stage_path(stage_root, &manifest);
    let stage = stage_dir(
        stage_root,
        &completed_path,
        true,
        "durable oversized request stage",
    )?;
    if stage
        .read_optional(
            "manifest.msgpack",
            None,
            "durable oversized request manifest",
        )?
        .is_none()
    {
        return Err(TransportError::IncompleteReply(
            "oversized request fragment arrived before its descriptor".to_string(),
        ));
    }
    persist_manifest_at(&stage, &manifest)?;
    accept_fragment_at_stage(&stage, &completed_path, &manifest, fragment)
}

#[cfg(test)]
pub(super) fn accept_fragment(
    stage_root: &Path,
    authenticated_node_id: &str,
    encoded: &[u8],
) -> TransportResult<StageOutcome> {
    let fragment: LargeRequestFragment = rmp_serde::from_slice(encoded).map_err(|err| {
        TransportError::IncompleteReply(format!(
            "invalid oversized request fragment envelope: {err}"
        ))
    })?;
    let begin = LargeRequestBegin::new(
        &fragment.subject,
        fragment.unit_digest,
        fragment.total_bytes as usize,
    )?;
    let _ = begin_request(stage_root, authenticated_node_id, &begin)?;
    accept_descriptor_fragment(stage_root, authenticated_node_id, &begin, &fragment)
}

pub(super) fn begin_request(
    stage_root: &Path,
    authenticated_node_id: &str,
    begin: &LargeRequestBegin,
) -> TransportResult<StageOutcome> {
    begin.validate()?;
    let manifest = stage_manifest(authenticated_node_id, begin);
    let completed_path = stage_path(stage_root, &manifest);
    let stage = stage_dir(
        stage_root,
        &completed_path,
        true,
        "durable oversized request stage",
    )?;
    persist_manifest_at(&stage, &manifest)?;
    let next_missing = repair_request_fragment_prefix_at(&stage, &manifest)?;
    if next_missing != manifest.total_fragments {
        return Ok(StageOutcome::Pending { next_missing });
    }
    let payload = assemble_complete_stage_at(&stage, &manifest)?;
    Ok(StageOutcome::Complete {
        subject: begin.subject.clone(),
        payload,
        completed_path,
    })
}

fn stage_manifest(authenticated_node_id: &str, begin: &LargeRequestBegin) -> StageManifest {
    StageManifest {
        version: MANIFEST_VERSION,
        subject: begin.subject.clone(),
        authenticated_node_id: authenticated_node_id.to_string(),
        unit_digest: begin.unit_digest,
        total_bytes: begin.total_bytes,
        total_fragments: begin.total_fragments,
        transfer_digest: begin.transfer_digest,
    }
}

fn accept_fragment_at_stage(
    stage: &StageDir,
    completed_path: &Path,
    manifest: &StageManifest,
    fragment: &LargeRequestFragment,
) -> TransportResult<StageOutcome> {
    validate_fragment(manifest, fragment)?;
    let next_missing = repair_request_fragment_prefix_at(stage, manifest)?;
    if fragment.sequence > next_missing {
        return Err(TransportError::IncompleteReply(format!(
            "oversized request is missing fragment {next_missing}"
        )));
    }
    if fragment.sequence == next_missing {
        persist_fragment_at(stage, fragment)?;
    }

    let next_missing = repair_request_fragment_prefix_at(stage, manifest)?;
    if next_missing != manifest.total_fragments {
        return Ok(StageOutcome::Pending { next_missing });
    }

    let payload = assemble_complete_stage_at(stage, manifest)?;
    Ok(StageOutcome::Complete {
        subject: manifest.subject.clone(),
        payload,
        completed_path: completed_path.to_path_buf(),
    })
}

pub(super) fn remove_completed_stage(root: &Path, path: &Path) -> TransportResult<()> {
    let Some(root_dir) = stage_root_dir_optional(root)? else {
        return Ok(());
    };
    remove_completed_stage_at_root(&root_dir, root, path)
}

fn remove_completed_stage_at_root(
    root: &StageDir,
    root_path: &Path,
    path: &Path,
) -> TransportResult<()> {
    let Some((parent, leaf)) = parent_and_leaf_optional_from_root(
        root,
        root_path,
        path,
        "completed oversized request stage",
    )?
    else {
        return Ok(());
    };
    let Some(stage) = parent.child_existing_optional(&leaf, "completed oversized request stage")?
    else {
        return Ok(());
    };
    parent.remove_held_tree_child(&leaf, stage, "completed oversized request stage")
}

#[cfg(feature = "test-seams")]
pub(super) fn snapshot_stage(
    stage_root: &Path,
    authenticated_node_id: &str,
    subject: &str,
    unit_digest: [u8; blake3::OUT_LEN],
    total_bytes: u64,
) -> TransportResult<Option<StageSnapshot>> {
    let path = stage_path(
        stage_root,
        &StageManifest {
            version: MANIFEST_VERSION,
            subject: subject.to_string(),
            authenticated_node_id: authenticated_node_id.to_string(),
            unit_digest,
            total_bytes,
            total_fragments: u32::try_from(fragment_count(usize::try_from(total_bytes).map_err(
                |_| {
                    TransportError::Other(
                        "oversized request length cannot be represented on this platform"
                            .to_string(),
                    )
                },
            )?))
            .map_err(|_| {
                TransportError::Other("oversized request has too many fragments".to_string())
            })?,
            transfer_digest: request_transfer_digest(
                subject,
                unit_digest,
                total_bytes,
                u32::try_from(fragment_count(usize::try_from(total_bytes).map_err(
                    |_| {
                        TransportError::Other(
                            "oversized request length cannot be represented on this platform"
                                .to_string(),
                        )
                    },
                )?))
                .map_err(|_| {
                    TransportError::Other("oversized request has too many fragments".to_string())
                })?,
            ),
        },
    );
    let Some(root) = stage_root_dir_optional(stage_root)? else {
        return Ok(None);
    };
    let Some(stage) =
        stage_dir_optional_from_root(&root, stage_root, &path, "durable oversized request stage")?
    else {
        return Ok(None);
    };
    let Some(encoded) = stage.read_optional(
        "manifest.msgpack",
        None,
        "durable oversized request manifest",
    )?
    else {
        return Ok(None);
    };
    let manifest: StageManifest = rmp_serde::from_slice(&encoded).map_err(|err| {
        TransportError::Other(format!(
            "cannot decode durable oversized request manifest: {err}"
        ))
    })?;
    if manifest.version != MANIFEST_VERSION
        || manifest.subject != subject
        || manifest.authenticated_node_id != authenticated_node_id
        || manifest.unit_digest != unit_digest
        || manifest.total_bytes != total_bytes
    {
        return Err(TransportError::Other(
            "durable oversized request stage identity does not match".to_string(),
        ));
    }
    let mut fragments = Vec::new();
    for sequence in 0..manifest.total_fragments {
        if let Some(bytes) = stage.read_optional(
            &format!("{sequence:08}.part"),
            None,
            "durable oversized request fragment",
        )? {
            fragments.push(StagedFragmentSnapshot {
                sequence,
                bytes: bytes.len() as u64,
                digest: *blake3::hash(&bytes).as_bytes(),
            });
        }
    }
    Ok(Some(StageSnapshot {
        subject: manifest.subject,
        authenticated_node_id: manifest.authenticated_node_id,
        unit_digest: manifest.unit_digest,
        total_bytes: manifest.total_bytes,
        total_fragments: manifest.total_fragments,
        fragments,
    }))
}

/// Return read-only snapshots for every persisted oversized request under this
/// endpoint's private staging root. This is test-only observability: it never
/// exposes a stage path or changes staged media.
#[cfg(feature = "test-seams")]
pub(super) fn snapshot_all_stages(stage_root: &Path) -> TransportResult<Vec<StageSnapshot>> {
    let mut snapshots = Vec::new();
    let Some(root) = stage_root_dir_optional(stage_root)? else {
        return Ok(snapshots);
    };
    snapshot_all_stages_at(&root, 3, &mut snapshots)?;
    snapshots.sort_by(|left, right| {
        left.authenticated_node_id
            .cmp(&right.authenticated_node_id)
            .then(left.subject.cmp(&right.subject))
            .then(left.unit_digest.cmp(&right.unit_digest))
    });
    Ok(snapshots)
}

#[cfg(feature = "test-seams")]
fn snapshot_all_stages_at(
    directory: &StageDir,
    depth: usize,
    snapshots: &mut Vec<StageSnapshot>,
) -> TransportResult<()> {
    for (name, stat) in directory.entries("durable oversized request stages")? {
        if stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
            return Err(TransportError::IncompleteReply(
                "durable oversized request stages contain a symlink or unexpected file type"
                    .to_string(),
            ));
        }
        let child = directory.child_existing(&name, "durable oversized request stage")?;
        if depth > 1 {
            snapshot_all_stages_at(&child, depth - 1, snapshots)?;
            continue;
        }
        let Some(encoded) = child.read_optional(
            "manifest.msgpack",
            None,
            "durable oversized request manifest",
        )?
        else {
            continue;
        };
        let manifest: StageManifest = rmp_serde::from_slice(&encoded).map_err(|err| {
            TransportError::Other(format!(
                "cannot decode durable oversized request manifest: {err}"
            ))
        })?;
        let mut fragments = Vec::new();
        for sequence in 0..manifest.total_fragments {
            if let Some(bytes) = child.read_optional(
                &format!("{sequence:08}.part"),
                None,
                "durable oversized request fragment",
            )? {
                fragments.push(StagedFragmentSnapshot {
                    sequence,
                    bytes: bytes.len() as u64,
                    digest: *blake3::hash(&bytes).as_bytes(),
                });
            }
        }
        snapshots.push(StageSnapshot {
            subject: manifest.subject,
            authenticated_node_id: manifest.authenticated_node_id,
            unit_digest: manifest.unit_digest,
            total_bytes: manifest.total_bytes,
            total_fragments: manifest.total_fragments,
            fragments,
        });
    }
    Ok(())
}

fn validate_fragment(
    manifest: &StageManifest,
    fragment: &LargeRequestFragment,
) -> TransportResult<()> {
    if fragment.transfer_digest != manifest.transfer_digest {
        return Err(TransportError::Other(
            "oversized request fragment belongs to a different descriptor".to_string(),
        ));
    }
    if fragment.subject != manifest.subject || fragment.unit_digest != manifest.unit_digest {
        return Err(TransportError::Other(
            "oversized request fragment metadata does not match its descriptor".to_string(),
        ));
    }
    if fragment.total_fragments < 2
        || fragment.sequence >= fragment.total_fragments
        || fragment.payload.is_empty()
        || fragment.payload.len() > FRAGMENT_BYTES
    {
        return Err(TransportError::Other(
            "oversized request fragment has invalid bounds".to_string(),
        ));
    }
    if fragment.total_bytes != manifest.total_bytes
        || fragment.total_fragments != manifest.total_fragments
    {
        return Err(TransportError::Other(
            "oversized request fragment count does not match its complete length".to_string(),
        ));
    }
    let expected_fragment_bytes = if fragment.sequence + 1 == fragment.total_fragments {
        let complete_prefix = usize::try_from(fragment.sequence)
            .ok()
            .and_then(|sequence| sequence.checked_mul(FRAGMENT_BYTES))
            .ok_or_else(|| {
                TransportError::Other(
                    "oversized request fragment length cannot be represented on this platform"
                        .to_string(),
                )
            })?;
        usize::try_from(manifest.total_bytes)
            .ok()
            .and_then(|total| total.checked_sub(complete_prefix))
            .ok_or_else(|| {
                TransportError::Other(
                    "oversized request fragment length does not match its complete length"
                        .to_string(),
                )
            })?
    } else {
        FRAGMENT_BYTES
    };
    if fragment.payload.len() != expected_fragment_bytes {
        return Err(TransportError::Other(
            "oversized request fragment length does not match its sequence".to_string(),
        ));
    }
    if *blake3::hash(&fragment.payload).as_bytes() != fragment.fragment_digest {
        return Err(TransportError::Other(
            "oversized request fragment failed integrity validation".to_string(),
        ));
    }
    Ok(())
}

fn stage_path(stage_root: &Path, manifest: &StageManifest) -> PathBuf {
    let subject_digest = blake3::hash(manifest.subject.as_bytes()).to_hex();
    let scope_digest = blake3::hash(
        [
            manifest.authenticated_node_id.as_bytes(),
            manifest.subject.as_bytes(),
            &manifest.total_bytes.to_be_bytes(),
            &manifest.total_fragments.to_be_bytes(),
            &manifest.unit_digest,
            &manifest.transfer_digest,
        ]
        .concat()
        .as_slice(),
    )
    .to_hex();
    stage_root
        .join(manifest.authenticated_node_id.as_str())
        .join(subject_digest.as_str())
        .join(scope_digest.as_str())
}

fn request_transfer_digest(
    subject: &str,
    unit_digest: [u8; blake3::OUT_LEN],
    total_bytes: u64,
    total_fragments: u32,
) -> [u8; blake3::OUT_LEN] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"contextdb-large-request-transfer-v1\\0");
    hasher.update(&(subject.len() as u64).to_be_bytes());
    hasher.update(subject.as_bytes());
    hasher.update(&unit_digest);
    hasher.update(&total_bytes.to_be_bytes());
    hasher.update(&total_fragments.to_be_bytes());
    *hasher.finalize().as_bytes()
}

// Every durable staging mutation starts by opening the configured root and
// then walks *only* through directory descriptors held by this process.  A
// pathname is therefore used exactly once to acquire the root; afterwards a
// hostile rename or symlink replacement of any visible parent cannot redirect
// chmod, file publication, reads, or cleanup outside that pinned tree.
//
// `openat(..., O_NOFOLLOW|O_DIRECTORY)` pins each directory.  Leaf files use
// the same parent fd, `O_NOFOLLOW`, and `renameat2(RENAME_NOREPLACE)`.  Tree
// deletion descends by fd and unlinks with `unlinkat`; it never calls a
// path-based recursive remover after validation.
#[cfg(unix)]
#[derive(Debug)]
struct StageDir {
    file: File,
    display: PathBuf,
}

#[cfg(unix)]
impl StageDir {
    fn open_anchor(path: &Path, what: &str) -> TransportResult<Self> {
        let c_path = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| TransportError::IncompleteReply(format!("{what} has an invalid path")))?;
        let fd = unsafe {
            libc::open(
                c_path.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(stage_directory_open_error(
                path,
                &format!("cannot open {what}"),
                what,
                std::io::Error::last_os_error(),
            ));
        }
        let file = unsafe { File::from_raw_fd(fd) };
        let metadata = file
            .metadata()
            .map_err(|err| stage_io_error(path, &format!("cannot inspect {what}"), err))?;
        if !metadata.is_dir() {
            return Err(TransportError::IncompleteReply(format!(
                "{what} directory is a symlink or unexpected file type"
            )));
        }
        Ok(Self {
            file,
            display: path.to_path_buf(),
        })
    }

    fn private(file: File, display: PathBuf, what: &str) -> TransportResult<Self> {
        let metadata = file
            .metadata()
            .map_err(|err| stage_io_error(&display, &format!("cannot inspect {what}"), err))?;
        if !metadata.is_dir() {
            return Err(TransportError::IncompleteReply(format!(
                "{what} directory is a symlink or unexpected file type"
            )));
        }
        let dir = Self { file, display };
        unsafe {
            if libc::fchmod(dir.file.as_raw_fd(), 0o700) != 0 {
                return Err(stage_io_error(
                    &dir.display,
                    &format!("cannot set private {what} permissions"),
                    std::io::Error::last_os_error(),
                ));
            }
        }
        dir.sync()?;
        Ok(dir)
    }

    fn name(name: &str, what: &str) -> TransportResult<CString> {
        if name.is_empty() || name.as_bytes().contains(&b'/') || name == "." || name == ".." {
            return Err(TransportError::IncompleteReply(format!(
                "{what} has an invalid path"
            )));
        }
        CString::new(name)
            .map_err(|_| TransportError::IncompleteReply(format!("{what} has an invalid path")))
    }

    fn name_os(name: &std::ffi::OsStr, what: &str) -> TransportResult<CString> {
        let bytes = name.as_bytes();
        if bytes.is_empty() || bytes.contains(&b'/') || bytes == b"." || bytes == b".." {
            return Err(TransportError::IncompleteReply(format!(
                "{what} has an invalid path"
            )));
        }
        CString::new(bytes)
            .map_err(|_| TransportError::IncompleteReply(format!("{what} has an invalid path")))
    }

    fn child_existing(&self, name: &str, what: &str) -> TransportResult<Self> {
        let child = self.child_raw(name, what)?;
        Self::private(child.file, child.display, what)
    }

    fn child_existing_optional(&self, name: &str, what: &str) -> TransportResult<Option<Self>> {
        let name_c = Self::name(name, what)?;
        let fd = unsafe {
            libc::openat(
                self.file.as_raw_fd(),
                name_c.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOENT) {
                return Ok(None);
            }
            return Err(stage_directory_open_error(
                &self.display.join(name),
                &format!("cannot open {what}"),
                what,
                err,
            ));
        }
        Self::private(
            unsafe { File::from_raw_fd(fd) },
            self.display.join(name),
            what,
        )
        .map(Some)
    }

    fn child_raw(&self, name: &str, what: &str) -> TransportResult<Self> {
        let name_c = Self::name(name, what)?;
        let fd = unsafe {
            libc::openat(
                self.file.as_raw_fd(),
                name_c.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(stage_directory_open_error(
                &self.display.join(name),
                &format!("cannot open {what}"),
                what,
                std::io::Error::last_os_error(),
            ));
        }
        Ok(Self {
            file: unsafe { File::from_raw_fd(fd) },
            display: self.display.join(name),
        })
    }

    fn child_private(&self, name: &str, what: &str) -> TransportResult<Self> {
        let name_c = Self::name(name, what)?;
        #[cfg(test)]
        pause_after_pinned_parent_for_test(PausePoint::CreateDirectory);
        let created = unsafe { libc::mkdirat(self.file.as_raw_fd(), name_c.as_ptr(), 0o700) };
        let mut created_stat = unsafe { std::mem::zeroed::<libc::stat>() };
        if created != 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::EEXIST) {
                return Err(stage_io_error(
                    &self.display.join(name),
                    &format!("cannot create {what}"),
                    err,
                ));
            }
        } else {
            if unsafe {
                libc::fstatat(
                    self.file.as_raw_fd(),
                    name_c.as_ptr(),
                    &mut created_stat,
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            } != 0
            {
                return Err(stage_io_error(
                    &self.display.join(name),
                    &format!("cannot inspect created {what}"),
                    std::io::Error::last_os_error(),
                ));
            }
            self.sync()?;
        }
        let child = self.child_raw(name, what)?;
        if created == 0 {
            let opened = child.file.metadata().map_err(|err| {
                stage_io_error(
                    &child.display,
                    &format!("cannot inspect opened {what}"),
                    err,
                )
            })?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let created_identity =
                    canonical_raw_stat_identity(created_stat.st_dev, created_stat.st_ino, what)?;
                if (opened.dev(), opened.ino()) != created_identity {
                    return Err(TransportError::IncompleteReply(format!(
                        "{what} changed after creation"
                    )));
                }
            }
        }
        let child = Self::private(child.file, child.display, what)?;
        child.sync()?;
        Ok(child)
    }

    fn child_private_os(&self, name: &std::ffi::OsStr, what: &str) -> TransportResult<Self> {
        let name_c = Self::name_os(name, what)?;
        let display = self.display.join(name);
        #[cfg(test)]
        pause_after_pinned_parent_for_test(PausePoint::CreateDirectory);
        let created = unsafe { libc::mkdirat(self.file.as_raw_fd(), name_c.as_ptr(), 0o700) };
        let mut created_stat = unsafe { std::mem::zeroed::<libc::stat>() };
        if created != 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::EEXIST) {
                return Err(stage_io_error(
                    &display,
                    &format!("cannot create {what}"),
                    err,
                ));
            }
        } else {
            if unsafe {
                libc::fstatat(
                    self.file.as_raw_fd(),
                    name_c.as_ptr(),
                    &mut created_stat,
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            } != 0
            {
                return Err(stage_io_error(
                    &display,
                    &format!("cannot inspect created {what}"),
                    std::io::Error::last_os_error(),
                ));
            }
            self.sync()?;
        }
        let fd = unsafe {
            libc::openat(
                self.file.as_raw_fd(),
                name_c.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(stage_directory_open_error(
                &display,
                &format!("cannot open {what}"),
                what,
                std::io::Error::last_os_error(),
            ));
        }
        let child = StageDir {
            file: unsafe { File::from_raw_fd(fd) },
            display,
        };
        if created == 0 {
            let opened = child.file.metadata().map_err(|err| {
                stage_io_error(
                    &child.display,
                    &format!("cannot inspect opened {what}"),
                    err,
                )
            })?;
            use std::os::unix::fs::MetadataExt;
            let created_identity =
                canonical_raw_stat_identity(created_stat.st_dev, created_stat.st_ino, what)?;
            if (opened.dev(), opened.ino()) != created_identity {
                return Err(TransportError::IncompleteReply(format!(
                    "{what} changed after creation"
                )));
            }
        }
        let child = Self::private(child.file, child.display, what)?;
        child.sync()?;
        Ok(child)
    }

    fn child_existing_os_optional(
        &self,
        name: &std::ffi::OsStr,
        what: &str,
    ) -> TransportResult<Option<Self>> {
        let name_c = Self::name_os(name, what)?;
        let display = self.display.join(name);
        let fd = unsafe {
            libc::openat(
                self.file.as_raw_fd(),
                name_c.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOENT) {
                return Ok(None);
            }
            return Err(stage_directory_open_error(
                &display,
                &format!("cannot open {what}"),
                what,
                err,
            ));
        }
        Self::private(unsafe { File::from_raw_fd(fd) }, display, what).map(Some)
    }

    fn relative_existing(&self, relative: &Path, what: &str) -> TransportResult<Self> {
        let mut current = self.file.try_clone().map_err(|err| {
            stage_io_error(&self.display, &format!("cannot clone {what} root"), err)
        })?;
        let mut display = self.display.clone();
        for component in relative.components() {
            let std::path::Component::Normal(name) = component else {
                return Err(TransportError::IncompleteReply(format!(
                    "{what} has an invalid path"
                )));
            };
            let name = name.to_str().ok_or_else(|| {
                TransportError::IncompleteReply(format!("{what} has a non-UTF-8 path"))
            })?;
            let parent = StageDir {
                file: current,
                display,
            };
            let child = parent.child_existing(name, what)?;
            current = child.file;
            display = child.display;
        }
        Self::private(current, display, what)
    }

    fn relative_private(&self, relative: &Path, what: &str) -> TransportResult<Self> {
        let mut current = self.file.try_clone().map_err(|err| {
            stage_io_error(&self.display, &format!("cannot clone {what} root"), err)
        })?;
        let mut display = self.display.clone();
        for component in relative.components() {
            let std::path::Component::Normal(name) = component else {
                return Err(TransportError::IncompleteReply(format!(
                    "{what} has an invalid path"
                )));
            };
            let name = name.to_str().ok_or_else(|| {
                TransportError::IncompleteReply(format!("{what} has a non-UTF-8 path"))
            })?;
            let parent = StageDir {
                file: current,
                display,
            };
            let child = parent.child_private(name, what)?;
            current = child.file;
            display = child.display;
        }
        Self::private(current, display, what)
    }

    fn sync(&self) -> TransportResult<()> {
        self.file.sync_all().map_err(|err| {
            stage_io_error(&self.display, "cannot sync durable stage directory", err)
        })
    }

    fn read_file(&self, name: &str, max: Option<usize>, what: &str) -> TransportResult<Vec<u8>> {
        let name_c = Self::name(name, what)?;
        #[cfg(test)]
        pause_after_pinned_parent_for_test(PausePoint::Read);
        let fd = unsafe {
            libc::openat(
                self.file.as_raw_fd(),
                name_c.as_ptr(),
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(stage_file_open_error(
                &self.display.join(name),
                &format!("cannot open {what}"),
                what,
                std::io::Error::last_os_error(),
            ));
        }
        let mut file = unsafe { File::from_raw_fd(fd) };
        let metadata = file.metadata().map_err(|err| {
            stage_io_error(
                &self.display.join(name),
                &format!("cannot inspect {what}"),
                err,
            )
        })?;
        if !metadata.is_file() {
            return Err(TransportError::IncompleteReply(format!(
                "{what} is a symlink or unexpected file type"
            )));
        }
        let length = usize::try_from(metadata.len()).map_err(|_| {
            TransportError::IncompleteReply(format!("{what} length cannot be represented"))
        })?;
        if max.is_some_and(|max| length > max) {
            return Err(TransportError::IncompleteReply(format!(
                "{what} exceeds its durable size ceiling"
            )));
        }
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(|err| {
            TransportError::Other(format!("cannot reserve {length} bytes for {what}: {err}"))
        })?;
        file.read_to_end(&mut bytes).map_err(|err| {
            stage_io_error(
                &self.display.join(name),
                &format!("cannot read {what}"),
                err,
            )
        })?;
        if bytes.len() != length {
            return Err(TransportError::IncompleteReply(format!(
                "{what} changed while being read"
            )));
        }
        Ok(bytes)
    }

    fn read_optional(
        &self,
        name: &str,
        max: Option<usize>,
        what: &str,
    ) -> TransportResult<Option<Vec<u8>>> {
        let name_c = Self::name(name, what)?;
        let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
        if unsafe {
            libc::fstatat(
                self.file.as_raw_fd(),
                name_c.as_ptr(),
                &mut stat,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOENT) {
                return Ok(None);
            }
            return Err(stage_io_error(
                &self.display.join(name),
                &format!("cannot inspect {what}"),
                err,
            ));
        }
        if stat.st_mode & libc::S_IFMT != libc::S_IFREG {
            return Err(TransportError::IncompleteReply(format!(
                "{what} is a symlink or unexpected file type"
            )));
        }
        self.read_file(name, max, what).map(Some)
    }

    fn persist_new(&self, name: &str, bytes: &[u8], what: &str) -> TransportResult<()> {
        let target = Self::name(name, what)?;
        let temporary_name = format!(".{name}.tmp-{}", uuid::Uuid::new_v4());
        let temporary = Self::name(&temporary_name, what)?;
        #[cfg(test)]
        pause_after_pinned_parent_for_test(PausePoint::Publish);
        let fd = unsafe {
            libc::openat(
                self.file.as_raw_fd(),
                temporary.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600,
            )
        };
        if fd < 0 {
            return Err(stage_io_error(
                &self.display.join(&temporary_name),
                &format!("cannot create {what}"),
                std::io::Error::last_os_error(),
            ));
        }
        let result = (|| {
            let mut file = unsafe { File::from_raw_fd(fd) };
            if unsafe { libc::fchmod(file.as_raw_fd(), 0o600) } != 0 {
                return Err(stage_io_error(
                    &self.display.join(&temporary_name),
                    &format!("cannot set private {what} permissions"),
                    std::io::Error::last_os_error(),
                ));
            }
            file.write_all(bytes).map_err(|err| {
                stage_io_error(
                    &self.display.join(&temporary_name),
                    &format!("cannot write {what}"),
                    err,
                )
            })?;
            file.sync_all().map_err(|err| {
                stage_io_error(
                    &self.display.join(&temporary_name),
                    &format!("cannot sync {what}"),
                    err,
                )
            })?;
            let renamed = rename_no_replace(
                self.file.as_raw_fd(),
                temporary.as_ptr(),
                self.file.as_raw_fd(),
                target.as_ptr(),
            );
            if renamed != 0 {
                return Err(stage_io_error(
                    &self.display.join(name),
                    &format!("cannot publish {what}"),
                    std::io::Error::last_os_error(),
                ));
            }
            self.sync()
        })();
        if result.is_err() {
            unsafe { libc::unlinkat(self.file.as_raw_fd(), temporary.as_ptr(), 0) };
        }
        result
    }

    fn move_regular_new(
        &self,
        source: &str,
        destination: &StageDir,
        target: &str,
        what: &str,
    ) -> TransportResult<()> {
        let source_c = Self::name(source, what)?;
        let target_c = Self::name(target, what)?;
        let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
        if unsafe {
            libc::fstatat(
                self.file.as_raw_fd(),
                source_c.as_ptr(),
                &mut stat,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            return Err(stage_io_error(
                &self.display.join(source),
                &format!("cannot inspect {what}"),
                std::io::Error::last_os_error(),
            ));
        }
        if stat.st_mode & libc::S_IFMT != libc::S_IFREG {
            return Err(TransportError::IncompleteReply(format!(
                "{what} is a symlink or unexpected file type"
            )));
        }
        #[cfg(test)]
        pause_after_pinned_parent_for_test(PausePoint::Publish);
        if rename_no_replace(
            self.file.as_raw_fd(),
            source_c.as_ptr(),
            destination.file.as_raw_fd(),
            target_c.as_ptr(),
        ) != 0
        {
            return Err(stage_io_error(
                &destination.display.join(target),
                &format!("cannot publish {what}"),
                std::io::Error::last_os_error(),
            ));
        }
        // Persist the new name first. If power is lost before the source
        // directory sync, recovery may retain both names, but it cannot lose
        // both the reserve and the completion receipt.
        destination.sync()?;
        self.sync()
    }

    fn unlink_file(&self, name: &str, what: &str) -> TransportResult<()> {
        let name_c = Self::name(name, what)?;
        let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
        if unsafe {
            libc::fstatat(
                self.file.as_raw_fd(),
                name_c.as_ptr(),
                &mut stat,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOENT) {
                return Ok(());
            }
            return Err(stage_io_error(
                &self.display.join(name),
                &format!("cannot inspect {what}"),
                err,
            ));
        }
        if stat.st_mode & libc::S_IFMT != libc::S_IFREG {
            return Err(TransportError::IncompleteReply(format!(
                "{what} is a symlink or unexpected file type"
            )));
        }
        let result = unsafe { libc::unlinkat(self.file.as_raw_fd(), name_c.as_ptr(), 0) };
        if result != 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOENT) {
                return Ok(());
            }
            return Err(stage_io_error(
                &self.display.join(name),
                &format!("cannot remove {what}"),
                err,
            ));
        }
        self.sync()
    }

    fn remove_held_tree_child(
        &self,
        name: &str,
        child: StageDir,
        what: &str,
    ) -> TransportResult<()> {
        self.remove_held_tree_child_at_pause(name, child, what, PausePoint::Cleanup)
    }

    fn remove_held_tree_child_at_pause(
        &self,
        name: &str,
        child: StageDir,
        what: &str,
        pause: PausePoint,
    ) -> TransportResult<()> {
        let child_meta = child.file.metadata().map_err(|err| {
            stage_io_error(&child.display, &format!("cannot inspect {what}"), err)
        })?;
        let name_c = Self::name(name, what)?;
        #[cfg(test)]
        pause_after_pinned_parent_for_test(pause);
        #[cfg(not(test))]
        let _ = pause;
        child.remove_tree_contents(what)?;
        let mut current = unsafe { std::mem::zeroed::<libc::stat>() };
        if unsafe {
            libc::fstatat(
                self.file.as_raw_fd(),
                name_c.as_ptr(),
                &mut current,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            return Err(stage_io_error(
                &self.display.join(name),
                &format!("cannot revalidate {what}"),
                std::io::Error::last_os_error(),
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let current_identity =
                canonical_raw_stat_identity(current.st_dev, current.st_ino, what)?;
            if (child_meta.dev(), child_meta.ino()) != current_identity {
                return Err(TransportError::IncompleteReply(format!(
                    "{what} changed before cleanup could unlink it"
                )));
            }
        }
        if unsafe { libc::unlinkat(self.file.as_raw_fd(), name_c.as_ptr(), libc::AT_REMOVEDIR) }
            != 0
        {
            return Err(stage_io_error(
                &self.display.join(name),
                &format!("cannot remove {what}"),
                std::io::Error::last_os_error(),
            ));
        }
        self.sync()
    }

    fn remove_tree_contents(&self, what: &str) -> TransportResult<()> {
        let duplicate = unsafe { libc::dup(self.file.as_raw_fd()) };
        if duplicate < 0 {
            return Err(stage_io_error(
                &self.display,
                &format!("cannot enumerate {what}"),
                std::io::Error::last_os_error(),
            ));
        }
        let stream = unsafe { libc::fdopendir(duplicate) };
        if stream.is_null() {
            unsafe { libc::close(duplicate) };
            return Err(stage_io_error(
                &self.display,
                &format!("cannot enumerate {what}"),
                std::io::Error::last_os_error(),
            ));
        }
        let mut names = Vec::new();
        loop {
            reset_errno();
            let entry = unsafe { libc::readdir(stream) };
            if entry.is_null() {
                let err = std::io::Error::last_os_error();
                unsafe { libc::closedir(stream) };
                if err.raw_os_error().is_some_and(|code| code != 0) {
                    return Err(stage_io_error(
                        &self.display,
                        &format!("cannot enumerate {what}"),
                        err,
                    ));
                }
                break;
            }
            let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
            if name == b"." || name == b".." {
                continue;
            }
            let name = match std::str::from_utf8(name) {
                Ok(name) => name.to_string(),
                Err(_) => {
                    unsafe { libc::closedir(stream) };
                    return Err(TransportError::IncompleteReply(format!(
                        "{what} has a non-UTF-8 entry"
                    )));
                }
            };
            names.push(name);
        }
        for name in names {
            let name_c = Self::name(&name, what)?;
            let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
            if unsafe {
                libc::fstatat(
                    self.file.as_raw_fd(),
                    name_c.as_ptr(),
                    &mut stat,
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            } != 0
            {
                return Err(stage_io_error(
                    &self.display.join(name),
                    &format!("cannot inspect {what}"),
                    std::io::Error::last_os_error(),
                ));
            }
            let kind = stat.st_mode & libc::S_IFMT;
            if kind == libc::S_IFDIR {
                let child = self.child_existing(&name, what)?;
                self.remove_held_tree_child(&name, child, what)?;
            } else if kind == libc::S_IFREG {
                self.unlink_file(&name, what)?;
            } else {
                return Err(TransportError::IncompleteReply(format!(
                    "{what} contains a symlink or unexpected file type"
                )));
            }
        }
        self.sync()
    }

    fn entries(&self, what: &str) -> TransportResult<Vec<(String, libc::stat)>> {
        let duplicate = unsafe { libc::dup(self.file.as_raw_fd()) };
        if duplicate < 0 {
            return Err(stage_io_error(
                &self.display,
                &format!("cannot enumerate {what}"),
                std::io::Error::last_os_error(),
            ));
        }
        let stream = unsafe { libc::fdopendir(duplicate) };
        if stream.is_null() {
            unsafe { libc::close(duplicate) };
            return Err(stage_io_error(
                &self.display,
                &format!("cannot enumerate {what}"),
                std::io::Error::last_os_error(),
            ));
        }
        let mut entries = Vec::new();
        loop {
            reset_errno();
            let entry = unsafe { libc::readdir(stream) };
            if entry.is_null() {
                let err = std::io::Error::last_os_error();
                unsafe { libc::closedir(stream) };
                if err.raw_os_error().is_some_and(|code| code != 0) {
                    return Err(stage_io_error(
                        &self.display,
                        &format!("cannot enumerate {what}"),
                        err,
                    ));
                }
                break;
            }
            let raw = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
            if raw == b"." || raw == b".." {
                continue;
            }
            let name = match std::str::from_utf8(raw) {
                Ok(name) => name.to_string(),
                Err(_) => {
                    unsafe { libc::closedir(stream) };
                    return Err(TransportError::IncompleteReply(format!(
                        "{what} has a non-UTF-8 entry"
                    )));
                }
            };
            let name_c = Self::name(&name, what)?;
            let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
            if unsafe {
                libc::fstatat(
                    self.file.as_raw_fd(),
                    name_c.as_ptr(),
                    &mut stat,
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            } != 0
            {
                unsafe { libc::closedir(stream) };
                return Err(stage_io_error(
                    &self.display.join(&name),
                    &format!("cannot inspect {what}"),
                    std::io::Error::last_os_error(),
                ));
            }
            entries.push((name, stat));
        }
        Ok(entries)
    }

    fn directory_bytes(&self, what: &str) -> TransportResult<u64> {
        let mut total = 0u64;
        for (name, stat) in self.entries(what)? {
            match stat.st_mode & libc::S_IFMT {
                libc::S_IFREG => total = total.saturating_add(stat.st_size.max(0) as u64),
                libc::S_IFDIR => {
                    total = total
                        .saturating_add(self.child_existing(&name, what)?.directory_bytes(what)?)
                }
                _ => {
                    return Err(TransportError::IncompleteReply(format!(
                        "{what} contains a symlink or unexpected file type"
                    )));
                }
            }
        }
        Ok(total)
    }

    fn unlink_regular_if_same(
        &self,
        name: &str,
        expected: &libc::stat,
        what: &str,
    ) -> TransportResult<()> {
        let name_c = Self::name(name, what)?;
        let mut current = unsafe { std::mem::zeroed::<libc::stat>() };
        if unsafe {
            libc::fstatat(
                self.file.as_raw_fd(),
                name_c.as_ptr(),
                &mut current,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            return Err(stage_io_error(
                &self.display.join(name),
                &format!("cannot revalidate {what}"),
                std::io::Error::last_os_error(),
            ));
        }
        if current.st_mode & libc::S_IFMT != libc::S_IFREG {
            return Err(TransportError::IncompleteReply(format!(
                "{what} changed before cleanup could unlink it"
            )));
        }
        let current_identity = canonical_raw_stat_identity(current.st_dev, current.st_ino, what)?;
        let expected_identity =
            canonical_raw_stat_identity(expected.st_dev, expected.st_ino, what)?;
        if current_identity != expected_identity {
            return Err(TransportError::IncompleteReply(format!(
                "{what} changed before cleanup could unlink it"
            )));
        }
        if unsafe { libc::unlinkat(self.file.as_raw_fd(), name_c.as_ptr(), 0) } != 0 {
            return Err(stage_io_error(
                &self.display.join(name),
                &format!("cannot remove {what}"),
                std::io::Error::last_os_error(),
            ));
        }
        self.sync()
    }
}

#[cfg(all(unix, target_os = "linux"))]
fn rename_no_replace(
    source_parent_fd: std::os::unix::io::RawFd,
    source: *const libc::c_char,
    target_parent_fd: std::os::unix::io::RawFd,
    target: *const libc::c_char,
) -> libc::c_long {
    unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            source_parent_fd,
            source,
            target_parent_fd,
            target,
            libc::RENAME_NOREPLACE,
        )
    }
}

#[cfg(all(unix, target_os = "macos"))]
fn rename_no_replace(
    source_parent_fd: std::os::unix::io::RawFd,
    source: *const libc::c_char,
    target_parent_fd: std::os::unix::io::RawFd,
    target: *const libc::c_char,
) -> libc::c_long {
    unsafe {
        libc::renameatx_np(
            source_parent_fd,
            source,
            target_parent_fd,
            target,
            libc::RENAME_EXCL,
        ) as libc::c_long
    }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn rename_no_replace(
    _source_parent_fd: std::os::unix::io::RawFd,
    _source: *const libc::c_char,
    _target_parent_fd: std::os::unix::io::RawFd,
    _target: *const libc::c_char,
) -> libc::c_long {
    -1
}

#[cfg(all(unix, target_os = "linux"))]
fn reset_errno() {
    unsafe { *libc::__errno_location() = 0 };
}

#[cfg(all(unix, target_os = "macos"))]
fn reset_errno() {
    unsafe { *libc::__error() = 0 };
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn reset_errno() {}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PausePoint {
    CreateDirectory,
    Publish,
    Read,
    Cleanup,
    Pressure,
}

#[cfg(test)]
struct PinnedParentPause {
    point: PausePoint,
    owner: std::sync::Mutex<Option<std::thread::ThreadId>>,
    state: std::sync::Mutex<(bool, bool)>,
    reached: std::sync::Condvar,
    resume: std::sync::Condvar,
}

#[cfg(test)]
static PINNED_PARENT_PAUSE: std::sync::OnceLock<
    std::sync::Mutex<Option<std::sync::Arc<PinnedParentPause>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
fn pause_after_pinned_parent_for_test(point: PausePoint) {
    let hook = PINNED_PARENT_PAUSE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .clone();
    let Some(hook) = hook else {
        return;
    };
    if hook.point != point
        || *hook.owner.lock().unwrap_or_else(|err| err.into_inner())
            != Some(std::thread::current().id())
    {
        return;
    }
    let mut state = hook.state.lock().unwrap_or_else(|err| err.into_inner());
    if state.0 {
        return;
    }
    state.0 = true;
    hook.reached.notify_all();
    while !state.1 {
        state = hook
            .resume
            .wait(state)
            .unwrap_or_else(|err| err.into_inner());
    }
}

#[cfg(test)]
struct PinnedParentPauseGuard {
    hook: std::sync::Arc<PinnedParentPause>,
}

#[cfg(test)]
impl PinnedParentPauseGuard {
    fn hook(&self) -> std::sync::Arc<PinnedParentPause> {
        self.hook.clone()
    }

    fn wait_until_reached(&self) {
        let mut state = self
            .hook
            .state
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        while !state.0 {
            let (next, timeout) = self
                .hook
                .reached
                .wait_timeout(state, std::time::Duration::from_secs(10))
                .unwrap_or_else(|err| err.into_inner());
            assert!(
                !timeout.timed_out(),
                "production operation did not reach its pinned-parent pause"
            );
            state = next;
        }
    }

    fn resume(&self) {
        let mut state = self
            .hook
            .state
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        state.1 = true;
        self.hook.resume.notify_all();
    }
}

#[cfg(test)]
impl Drop for PinnedParentPauseGuard {
    fn drop(&mut self) {
        self.resume();
        let mut slot = PINNED_PARENT_PAUSE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if slot
            .as_ref()
            .is_some_and(|active| std::sync::Arc::ptr_eq(active, &self.hook))
        {
            *slot = None;
        }
    }
}

#[cfg(test)]
impl PinnedParentPause {
    fn claim_for_current_thread(&self) {
        *self.owner.lock().unwrap_or_else(|err| err.into_inner()) =
            Some(std::thread::current().id());
    }
}

#[cfg(test)]
fn arm_pinned_parent_pause_for_test(point: PausePoint) -> PinnedParentPauseGuard {
    let hook = std::sync::Arc::new(PinnedParentPause {
        point,
        owner: std::sync::Mutex::new(None),
        state: std::sync::Mutex::new((false, false)),
        reached: std::sync::Condvar::new(),
        resume: std::sync::Condvar::new(),
    });
    *PINNED_PARENT_PAUSE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap_or_else(|err| err.into_inner()) = Some(hook.clone());
    PinnedParentPauseGuard { hook }
}

#[cfg(unix)]
fn stage_root_parent_and_name(root: &Path) -> TransportResult<(StageDir, &std::ffi::OsStr)> {
    let parent = root.parent().unwrap_or_else(|| Path::new("."));
    let name = root.file_name().ok_or_else(|| {
        TransportError::IncompleteReply("durable staging root has an invalid path".to_string())
    })?;
    let parent = StageDir::open_anchor(parent, "durable staging root parent")?;
    Ok((parent, name))
}

#[cfg(unix)]
fn stage_root_dir_create(root: &Path) -> TransportResult<StageDir> {
    let (parent, name) = stage_root_parent_and_name(root)?;
    parent.child_private_os(name, "durable staging root")
}

#[cfg(unix)]
fn stage_root_dir_optional(root: &Path) -> TransportResult<Option<StageDir>> {
    let (parent, name) = stage_root_parent_and_name(root)?;
    parent.child_existing_os_optional(name, "durable staging root")
}

#[cfg(unix)]
fn stage_root_dir_existing(root: &Path) -> TransportResult<StageDir> {
    stage_root_dir_optional(root)?.ok_or_else(|| {
        TransportError::IncompleteReply("durable staging root is missing".to_string())
    })
}

#[cfg(unix)]
fn stage_dir(root: &Path, path: &Path, create: bool, what: &str) -> TransportResult<StageDir> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| TransportError::IncompleteReply(format!("{what} escaped its durable root")))?;
    let root = if create {
        stage_root_dir_create(root)?
    } else {
        stage_root_dir_existing(root)?
    };
    if create {
        root.relative_private(relative, what)
    } else {
        root.relative_existing(relative, what)
    }
}

#[cfg(unix)]
fn stage_dir_from_root(
    root: &StageDir,
    root_path: &Path,
    path: &Path,
    create: bool,
    what: &str,
) -> TransportResult<StageDir> {
    let relative = path
        .strip_prefix(root_path)
        .map_err(|_| TransportError::IncompleteReply(format!("{what} escaped its durable root")))?;
    if create {
        root.relative_private(relative, what)
    } else {
        root.relative_existing(relative, what)
    }
}

#[cfg(all(unix, feature = "test-seams"))]
fn stage_dir_optional_from_root(
    root: &StageDir,
    root_path: &Path,
    path: &Path,
    what: &str,
) -> TransportResult<Option<StageDir>> {
    let relative = path
        .strip_prefix(root_path)
        .map_err(|_| TransportError::IncompleteReply(format!("{what} escaped its durable root")))?;
    let mut current = StageDir {
        file: root.file.try_clone().map_err(|err| {
            stage_io_error(&root.display, &format!("cannot clone {what} root"), err)
        })?,
        display: root.display.clone(),
    };
    for component in relative.components() {
        let std::path::Component::Normal(name) = component else {
            return Err(TransportError::IncompleteReply(format!(
                "{what} has an invalid path"
            )));
        };
        let name = name.to_str().ok_or_else(|| {
            TransportError::IncompleteReply(format!("{what} has a non-UTF-8 path"))
        })?;
        let Some(child) = current.child_existing_optional(name, what)? else {
            return Ok(None);
        };
        current = child;
    }
    Ok(Some(current))
}

#[cfg(unix)]
fn parent_and_leaf_from_root(
    root: &StageDir,
    root_path: &Path,
    path: &Path,
    create_parent: bool,
    what: &str,
) -> TransportResult<(StageDir, String)> {
    let relative = path
        .strip_prefix(root_path)
        .map_err(|_| TransportError::IncompleteReply(format!("{what} escaped its durable root")))?;
    let mut components = relative.components().peekable();
    let mut parent = StageDir {
        file: root.file.try_clone().map_err(|err| {
            stage_io_error(&root.display, &format!("cannot clone {what} root"), err)
        })?,
        display: root.display.clone(),
    };
    while let Some(component) = components.next() {
        let std::path::Component::Normal(name) = component else {
            return Err(TransportError::IncompleteReply(format!(
                "{what} has an invalid path"
            )));
        };
        let name = name.to_str().ok_or_else(|| {
            TransportError::IncompleteReply(format!("{what} has a non-UTF-8 path"))
        })?;
        if components.peek().is_none() {
            return Ok((parent, name.to_string()));
        }
        parent = if create_parent {
            parent.child_private(name, what)?
        } else {
            parent.child_existing(name, what)?
        };
    }
    Err(TransportError::IncompleteReply(format!(
        "{what} has an invalid path"
    )))
}

#[cfg(unix)]
fn parent_and_leaf_optional_from_root(
    root: &StageDir,
    root_path: &Path,
    path: &Path,
    what: &str,
) -> TransportResult<Option<(StageDir, String)>> {
    let relative = path
        .strip_prefix(root_path)
        .map_err(|_| TransportError::IncompleteReply(format!("{what} escaped its durable root")))?;
    let mut components = relative.components().peekable();
    let mut parent = StageDir {
        file: root.file.try_clone().map_err(|err| {
            stage_io_error(&root.display, &format!("cannot clone {what} root"), err)
        })?,
        display: root.display.clone(),
    };
    while let Some(component) = components.next() {
        let std::path::Component::Normal(name) = component else {
            return Err(TransportError::IncompleteReply(format!(
                "{what} has an invalid path"
            )));
        };
        let name = name.to_str().ok_or_else(|| {
            TransportError::IncompleteReply(format!("{what} has a non-UTF-8 path"))
        })?;
        if components.peek().is_none() {
            return Ok(Some((parent, name.to_string())));
        }
        let Some(child) = parent.child_existing_optional(name, what)? else {
            return Ok(None);
        };
        parent = child;
    }
    Err(TransportError::IncompleteReply(format!(
        "{what} has an invalid path"
    )))
}

#[cfg(unix)]
fn open_tree_unit_from_root(
    root: &StageDir,
    root_path: &Path,
    path: &Path,
    what: &str,
) -> TransportResult<(StageDir, String, StageDir)> {
    let (parent, leaf) = parent_and_leaf_from_root(root, root_path, path, false, what)?;
    let child = parent.child_existing(&leaf, what)?;
    Ok((parent, leaf, child))
}

#[cfg(not(unix))]
compile_error!(
    "durable large-request staging requires Unix descriptor-relative filesystem operations"
);

fn persist_manifest_at(stage: &StageDir, manifest: &StageManifest) -> TransportResult<()> {
    let encoded = rmp_serde::to_vec_named(manifest).map_err(|err| {
        TransportError::Other(format!("cannot encode oversized request manifest: {err}"))
    })?;
    match stage.read_optional(
        "manifest.msgpack",
        None,
        "durable oversized request manifest",
    )? {
        None => stage.persist_new("manifest.msgpack", &encoded, "oversized request manifest"),
        Some(existing) if existing == encoded => Ok(()),
        Some(_) => Err(TransportError::Other(
            "durable oversized request stage identity does not match".to_string(),
        )),
    }
}

fn persist_fragment_at(stage: &StageDir, fragment: &LargeRequestFragment) -> TransportResult<()> {
    let stem = format!("{:08}", fragment.sequence);
    let part = format!("{stem}.part");
    let digest_name = format!("{stem}.digest");
    match stage.read_optional(&part, None, "durable oversized request fragment")? {
        Some(existing) if *blake3::hash(&existing).as_bytes() == fragment.fragment_digest => {
            let digest = stage.read_file(
                &digest_name,
                Some(blake3::OUT_LEN),
                "durable oversized request fragment digest",
            )?;
            if digest.as_slice() == fragment.fragment_digest {
                return Ok(());
            }
            discard_request_fragment_suffix_at(stage, fragment.sequence, fragment.total_fragments)?;
        }
        Some(_) => {
            return Err(TransportError::Other(
                "durable oversized request fragment differs from the staged bytes".to_string(),
            ));
        }
        None => {}
    }
    stage.persist_new(&part, &fragment.payload, "oversized request fragment")?;
    stage.persist_new(
        &digest_name,
        &fragment.fragment_digest,
        "oversized request fragment digest",
    )
}

fn repair_request_fragment_prefix_at(
    stage: &StageDir,
    manifest: &StageManifest,
) -> TransportResult<u32> {
    for sequence in 0..manifest.total_fragments {
        let stem = format!("{sequence:08}");
        let path = format!("{stem}.part");
        let digest_path = format!("{stem}.digest");
        let expected = request_fragment_bytes(manifest, sequence)?;
        let valid = stage
            .read_file(&path, Some(expected), "durable oversized request fragment")
            .and_then(|bytes| {
                let digest = stage.read_file(
                    &digest_path,
                    Some(blake3::OUT_LEN),
                    "durable oversized request fragment digest",
                )?;
                Ok(bytes.len() == expected
                    && digest.len() == blake3::OUT_LEN
                    && digest.as_slice() == blake3::hash(&bytes).as_bytes())
            })
            .unwrap_or(false);
        if !valid {
            discard_request_fragment_suffix_at(stage, sequence, manifest.total_fragments)?;
            return Ok(sequence);
        }
    }
    Ok(manifest.total_fragments)
}

fn request_fragment_bytes(manifest: &StageManifest, sequence: u32) -> TransportResult<usize> {
    let total = usize::try_from(manifest.total_bytes).map_err(|_| {
        TransportError::Other(
            "oversized request length cannot be represented on this platform".to_string(),
        )
    })?;
    Ok(if sequence + 1 == manifest.total_fragments {
        total - sequence as usize * FRAGMENT_BYTES
    } else {
        FRAGMENT_BYTES
    })
}

fn discard_request_fragment_suffix_at(
    stage: &StageDir,
    from: u32,
    total: u32,
) -> TransportResult<()> {
    for sequence in from..total {
        let stem = format!("{sequence:08}");
        for artifact in [format!("{stem}.part"), format!("{stem}.digest")] {
            stage.unlink_file(&artifact, "corrupt oversized request fragment")?;
        }
    }
    stage.sync()
}

fn assemble_complete_stage_at(
    stage: &StageDir,
    manifest: &StageManifest,
) -> TransportResult<Vec<u8>> {
    let total_bytes = usize::try_from(manifest.total_bytes).map_err(|_| {
        TransportError::Other(
            "oversized request length cannot be represented on this platform".to_string(),
        )
    })?;
    let mut fragments = Vec::new();
    for sequence in 0..manifest.total_fragments {
        let path = format!("{sequence:08}.part");
        let expected_bytes = if sequence + 1 == manifest.total_fragments {
            total_bytes - sequence as usize * FRAGMENT_BYTES
        } else {
            FRAGMENT_BYTES
        };
        let bytes = stage.read_file(
            &path,
            Some(expected_bytes),
            "durable oversized request fragment",
        )?;
        if bytes.len() != expected_bytes {
            return Err(TransportError::IncompleteReply(
                "durable oversized request fragment length does not match its manifest".to_string(),
            ));
        }
        fragments.push(bytes);
    }

    // `Vec::with_capacity` can abort the process for an authenticated peer's
    // absurd declaration.  Capacity pressure is a resumable transport error:
    // retain the verified fragments so the peer can retry after capacity is
    // available instead of losing its stage.
    let mut payload = Vec::new();
    payload.try_reserve_exact(total_bytes).map_err(|err| {
        TransportError::Other(format!(
            "cannot reserve {total_bytes} bytes for complete oversized request: {err}"
        ))
    })?;
    for fragment in fragments {
        payload.extend_from_slice(&fragment);
    }
    if *blake3::hash(&payload).as_bytes() != manifest.unit_digest {
        return Err(TransportError::Other(
            "complete oversized request failed integrity validation".to_string(),
        ));
    }
    Ok(payload)
}

fn stage_io_error(_path: &Path, action: &str, _err: std::io::Error) -> TransportError {
    TransportError::Other(action.to_string())
}

#[cfg(unix)]
fn is_nofollow_type_error(err: &std::io::Error) -> bool {
    matches!(err.raw_os_error(), Some(libc::ELOOP) | Some(libc::ENOTDIR))
}

#[cfg(unix)]
fn stage_directory_open_error(
    path: &Path,
    action: &str,
    what: &str,
    err: std::io::Error,
) -> TransportError {
    if is_nofollow_type_error(&err) {
        TransportError::IncompleteReply(format!(
            "{what} directory is a symlink or unexpected file type"
        ))
    } else {
        stage_io_error(path, action, err)
    }
}

#[cfg(unix)]
fn stage_file_open_error(
    path: &Path,
    action: &str,
    what: &str,
    err: std::io::Error,
) -> TransportError {
    if is_nofollow_type_error(&err) {
        TransportError::IncompleteReply(format!("{what} is a symlink or unexpected file type"))
    } else {
        stage_io_error(path, action, err)
    }
}

// Successful replies larger than a sync frame use the same durable-media
// discipline as incoming opaque requests.  The transport deliberately binds
// the opaque request/reply bytes, not sync fields: the normal decoded reply
// path still validates the response source/incarnation after assembly.
pub(super) const RESPONSE_CONTROL_SUBJECT: &str = "__contextdb_large_response_v1__";
pub(super) const RESPONSE_CHUNK_BYTES: usize = 4 * 1024 * 1024;
pub(super) const MAX_RESPONSE_CHUNK_ENVELOPE_BYTES: usize = RESPONSE_CHUNK_BYTES + 4096;
pub(super) const MAX_RESPONSE_CONTROL_BYTES: usize = 128 * 1024;
const RESPONSE_MANIFEST_VERSION: u8 = 2;
const RESPONSE_COMPLETION_RESERVE_FILE: &str = "completion-reserve.msgpack";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct LargeResponseManifest {
    version: u8,
    pub publication_nonce: [u8; 32],
    pub subject: String,
    pub authenticated_node_id: String,
    pub request_digest: [u8; blake3::OUT_LEN],
    pub response_digest: [u8; blake3::OUT_LEN],
    pub total_bytes: u64,
    pub total_chunks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum LargeResponseControl {
    Chunk {
        manifest: LargeResponseManifest,
        sequence: u64,
    },
    Complete {
        manifest: LargeResponseManifest,
    },
    Release {
        manifest: LargeResponseManifest,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct LargeResponseChunk {
    pub transfer_digest: [u8; blake3::OUT_LEN],
    pub sequence: u64,
    pub total_chunks: u64,
    pub digest: [u8; blake3::OUT_LEN],
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

impl LargeResponseManifest {
    pub(super) fn decode(bytes: &[u8]) -> Option<Self> {
        let manifest: Self = rmp_serde::from_slice(bytes).ok()?;
        (manifest.version == RESPONSE_MANIFEST_VERSION && manifest.total_chunks >= 2)
            .then_some(manifest)
    }

    pub(super) fn encode(&self) -> TransportResult<Vec<u8>> {
        rmp_serde::to_vec_named(self).map_err(|err| {
            TransportError::Other(format!("cannot encode oversized response manifest: {err}"))
        })
    }

    pub(super) fn transfer_digest(&self) -> TransportResult<[u8; blake3::OUT_LEN]> {
        Ok(*blake3::hash(&self.encode()?).as_bytes())
    }

    pub(super) fn validate_for(
        &self,
        node: &str,
        subject: &str,
        request: [u8; blake3::OUT_LEN],
    ) -> TransportResult<()> {
        if self.version != RESPONSE_MANIFEST_VERSION
            || self.authenticated_node_id != node
            || self.subject != subject
            || self.request_digest != request
            || self.total_chunks < 2
            || self.total_bytes < RESPONSE_CHUNK_BYTES as u64
        {
            return Err(TransportError::IncompleteReply(
                "oversized response manifest identity does not match the authenticated request"
                    .to_string(),
            ));
        }
        let bytes = usize::try_from(self.total_bytes).map_err(|_| {
            TransportError::IncompleteReply(
                "oversized response length cannot be represented".to_string(),
            )
        })?;
        if u64::try_from(bytes.div_ceil(RESPONSE_CHUNK_BYTES)).ok() != Some(self.total_chunks) {
            return Err(TransportError::IncompleteReply(
                "oversized response manifest chunk count does not match its length".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
pub(super) fn stage_large_response(
    root: &Path,
    authenticated_node_id: &str,
    subject: &str,
    request_digest: [u8; blake3::OUT_LEN],
    response: &[u8],
) -> TransportResult<LargeResponseManifest> {
    stage_large_response_with_budget(
        root,
        authenticated_node_id,
        subject,
        request_digest,
        response,
        None,
        &[],
    )
}

pub(super) fn stage_large_response_with_budget(
    root: &Path,
    authenticated_node_id: &str,
    subject: &str,
    request_digest: [u8; blake3::OUT_LEN],
    response: &[u8],
    budget: Option<u64>,
    protected: &[PathBuf],
) -> TransportResult<LargeResponseManifest> {
    let mut publication_nonce = [0u8; 32];
    getrandom::fill(&mut publication_nonce).map_err(|err| {
        TransportError::Other(format!(
            "cannot create a unique oversized response publication: {err}"
        ))
    })?;
    stage_large_response_with_nonce_and_budget(
        root,
        authenticated_node_id,
        subject,
        request_digest,
        response,
        publication_nonce,
        ResponseStageAdmission { budget, protected },
    )
}

struct ResponseStageAdmission<'a> {
    budget: Option<u64>,
    protected: &'a [PathBuf],
}

#[cfg(test)]
fn stage_large_response_with_nonce(
    root: &Path,
    authenticated_node_id: &str,
    subject: &str,
    request_digest: [u8; blake3::OUT_LEN],
    response: &[u8],
    publication_nonce: [u8; 32],
) -> TransportResult<LargeResponseManifest> {
    stage_large_response_with_nonce_and_budget(
        root,
        authenticated_node_id,
        subject,
        request_digest,
        response,
        publication_nonce,
        ResponseStageAdmission {
            budget: None,
            protected: &[],
        },
    )
}

fn stage_large_response_with_nonce_and_budget(
    root: &Path,
    authenticated_node_id: &str,
    subject: &str,
    request_digest: [u8; blake3::OUT_LEN],
    response: &[u8],
    publication_nonce: [u8; 32],
    admission: ResponseStageAdmission<'_>,
) -> TransportResult<LargeResponseManifest> {
    let total_chunks = response.len().div_ceil(RESPONSE_CHUNK_BYTES);
    if total_chunks < 2 {
        return Err(TransportError::Other(
            "oversized response stage received a fitting reply".to_string(),
        ));
    }
    let manifest = LargeResponseManifest {
        version: RESPONSE_MANIFEST_VERSION,
        publication_nonce,
        subject: subject.to_string(),
        authenticated_node_id: authenticated_node_id.to_string(),
        request_digest,
        response_digest: *blake3::hash(response).as_bytes(),
        total_bytes: response.len() as u64,
        total_chunks: total_chunks as u64,
    };
    let encoded = manifest.encode()?;
    let controls = [
        LargeResponseControl::Chunk {
            manifest: manifest.clone(),
            sequence: u64::MAX,
        },
        LargeResponseControl::Complete {
            manifest: manifest.clone(),
        },
        LargeResponseControl::Release {
            manifest: manifest.clone(),
        },
    ];
    if encoded.len() > MAX_RESPONSE_CONTROL_BYTES {
        return Err(TransportError::Other(
            "oversized response manifest cannot fit its control frame".to_string(),
        ));
    }
    for control in controls {
        let encoded_control = rmp_serde::to_vec_named(&control).map_err(|err| {
            TransportError::Other(format!(
                "cannot encode oversized response control before staging: {err}"
            ))
        })?;
        if encoded_control.len() > MAX_RESPONSE_CONTROL_BYTES {
            return Err(TransportError::Other(
                "oversized response manifest cannot fit its control frame".to_string(),
            ));
        }
    }
    if let Some(budget) = admission.budget {
        let required = u64::try_from(encoded.len())
            .ok()
            .and_then(|manifest_bytes| manifest_bytes.checked_mul(2))
            .and_then(|manifest_bytes| manifest_bytes.checked_add(manifest.total_bytes))
            .and_then(|bytes| {
                manifest
                    .total_chunks
                    .checked_mul(blake3::OUT_LEN as u64)
                    .and_then(|digest_bytes| bytes.checked_add(digest_bytes))
            })
            .ok_or_else(|| {
                TransportError::Other(
                    "oversized response durable size cannot be represented".to_string(),
                )
            })?;
        reserve_response_stage_budget(root, budget, admission.protected, required)?;
    }
    let path = response_stage_path(root, &manifest);
    let root_dir = stage_root_dir_create(root)?;
    let stage = stage_dir_from_root(
        &root_dir,
        root,
        &path,
        true,
        "durable oversized response stage",
    )?;
    persist_response_manifest_at(&stage, &manifest)?;
    stage.persist_new(
        RESPONSE_COMPLETION_RESERVE_FILE,
        &encoded,
        "oversized response completion headroom",
    )?;
    for (sequence, chunk) in response.chunks(RESPONSE_CHUNK_BYTES).enumerate() {
        persist_response_chunk_at(&stage, &manifest, sequence as u64, chunk)?;
    }
    Ok(manifest)
}

pub(super) fn response_chunk(
    root: &Path,
    authenticated_node_id: &str,
    manifest: &LargeResponseManifest,
    sequence: u64,
) -> TransportResult<LargeResponseChunk> {
    let root_dir = stage_root_dir_existing(root)?;
    let stage_path = response_stage_path(root, manifest);
    let stage = stage_dir_from_root(
        &root_dir,
        root,
        &stage_path,
        false,
        "durable oversized response stage",
    )?;
    validate_response_stage_at(&stage, authenticated_node_id, manifest)?;
    let expected_bytes = response_chunk_len(manifest, sequence)?;
    let bytes = read_exact_at(
        &stage,
        &format!("{sequence:016}.reply"),
        expected_bytes,
        "durable oversized response chunk",
    )?;
    let digest = read_exact_at(
        &stage,
        &format!("{sequence:016}.digest"),
        blake3::OUT_LEN,
        "durable oversized response chunk digest",
    )?;
    let digest: [u8; blake3::OUT_LEN] = digest.try_into().expect("exact digest length");
    if *blake3::hash(&bytes).as_bytes() != digest {
        return Err(TransportError::IncompleteReply(
            "durable oversized response chunk failed integrity validation".to_string(),
        ));
    }
    let transfer_digest = *blake3::hash(&manifest.encode()?).as_bytes();
    Ok(LargeResponseChunk {
        transfer_digest,
        sequence,
        total_chunks: manifest.total_chunks,
        digest,
        bytes,
    })
}

/// Records completion before deleting bytes.  A retry after the server has
/// accepted completion but its reply was lost finds this tombstone and returns
/// the same success without re-running the original request handler.
pub(super) fn validate_large_response_completion(
    root: &Path,
    authenticated_node_id: &str,
    manifest: &LargeResponseManifest,
) -> TransportResult<()> {
    if manifest.authenticated_node_id != authenticated_node_id {
        return Err(TransportError::IncompleteReply(
            "oversized response belongs to a different authenticated edge".to_string(),
        ));
    }
    let root_dir = stage_root_dir_existing(root)?;
    let completion_path = response_completion_path(root, manifest);
    let expected = manifest.encode()?;
    if let Some((completion_parent, completion_leaf)) = parent_and_leaf_optional_from_root(
        &root_dir,
        root,
        &completion_path,
        "oversized response completion",
    )? && let Some(bytes) = completion_parent.read_optional(
        &completion_leaf,
        Some(expected.len()),
        "oversized response completion receipt",
    )? {
        let stored = LargeResponseManifest::decode(&bytes).ok_or_else(|| {
            TransportError::IncompleteReply(
                "oversized response completion receipt is invalid".to_string(),
            )
        })?;
        return if stored.authenticated_node_id == authenticated_node_id && stored == *manifest {
            Ok(())
        } else {
            Err(TransportError::IncompleteReply(
                "oversized response completion identity does not match".to_string(),
            ))
        };
    }
    let stage = response_stage_path(root, manifest);
    let (_, _, stage_dir) =
        open_tree_unit_from_root(&root_dir, root, &stage, "oversized response stage")?;
    validate_response_stage_at(&stage_dir, authenticated_node_id, manifest)
}

/// Durably records completion before deleting response bytes. A retry after
/// the acknowledgement is lost validates and reuses the retained receipt.
pub(super) fn complete_large_response(
    root: &Path,
    authenticated_node_id: &str,
    manifest: &LargeResponseManifest,
) -> TransportResult<()> {
    if manifest.authenticated_node_id != authenticated_node_id {
        return Err(TransportError::IncompleteReply(
            "oversized response belongs to a different authenticated edge".to_string(),
        ));
    }
    let root_dir = stage_root_dir_existing(root)?;
    let completion_path = response_completion_path(root, manifest);
    let expected = manifest.encode()?;
    if let Some((completion_parent, completion_leaf)) = parent_and_leaf_optional_from_root(
        &root_dir,
        root,
        &completion_path,
        "oversized response completion",
    )? && let Some(bytes) = completion_parent.read_optional(
        &completion_leaf,
        Some(expected.len()),
        "oversized response completion receipt",
    )? {
        let stored = LargeResponseManifest::decode(&bytes).ok_or_else(|| {
            TransportError::IncompleteReply(
                "oversized response completion receipt is invalid".to_string(),
            )
        })?;
        if stored.authenticated_node_id == authenticated_node_id && stored == *manifest {
            let stage = response_stage_path(root, manifest);
            if let Some((stage_parent, stage_leaf)) = parent_and_leaf_optional_from_root(
                &root_dir,
                root,
                &stage,
                "oversized response stage",
            )? && let Some(stage_dir) =
                stage_parent.child_existing_optional(&stage_leaf, "oversized response stage")?
            {
                validate_response_stage_at(&stage_dir, authenticated_node_id, manifest)?;
                stage_parent.remove_held_tree_child(
                    &stage_leaf,
                    stage_dir,
                    "oversized response stage",
                )?;
            }
            return Ok(());
        }
        return Err(TransportError::IncompleteReply(
            "oversized response completion identity does not match".to_string(),
        ));
    }
    let stage = response_stage_path(root, manifest);
    let (stage_parent, stage_leaf, stage_dir) =
        open_tree_unit_from_root(&root_dir, root, &stage, "oversized response stage")?;
    validate_response_stage_at(&stage_dir, authenticated_node_id, manifest)?;
    let reserved = stage_dir
        .read_optional(
            RESPONSE_COMPLETION_RESERVE_FILE,
            Some(expected.len()),
            "oversized response completion headroom",
        )?
        .ok_or_else(|| {
            TransportError::IncompleteReply(
                "oversized response completion headroom is missing".to_string(),
            )
        })?;
    if reserved != expected {
        return Err(TransportError::IncompleteReply(
            "oversized response completion headroom failed integrity validation".to_string(),
        ));
    }
    let (completion_parent, completion_leaf) = parent_and_leaf_from_root(
        &root_dir,
        root,
        &completion_path,
        true,
        "oversized response completion",
    )?;
    stage_dir.move_regular_new(
        RESPONSE_COMPLETION_RESERVE_FILE,
        &completion_parent,
        &completion_leaf,
        "oversized response completion receipt",
    )?;
    stage_parent.remove_held_tree_child(&stage_leaf, stage_dir, "oversized response stage")?;
    Ok(())
}

/// Removes a staged response whose manifest could not be delivered. The
/// caller invokes this only after releasing the final in-memory logical
/// transfer reference, so an identical concurrent transfer cannot lose bytes
/// it is still reading.
pub(super) fn abandon_large_response(
    root: &Path,
    authenticated_node_id: &str,
    manifest: &LargeResponseManifest,
) -> TransportResult<()> {
    if manifest.authenticated_node_id != authenticated_node_id {
        return Err(TransportError::IncompleteReply(
            "oversized response belongs to a different authenticated edge".to_string(),
        ));
    }
    let Some(root_dir) = stage_root_dir_optional(root)? else {
        return Ok(());
    };
    let stage = response_stage_path(root, manifest);
    let Some((stage_parent, stage_leaf)) =
        parent_and_leaf_optional_from_root(&root_dir, root, &stage, "oversized response stage")?
    else {
        return Ok(());
    };
    let Some(stage_dir) =
        stage_parent.child_existing_optional(&stage_leaf, "oversized response stage")?
    else {
        return Ok(());
    };
    validate_response_stage_at(&stage_dir, authenticated_node_id, manifest)?;
    stage_parent.remove_held_tree_child(&stage_leaf, stage_dir, "oversized response stage")
}

/// The client sends this only after receiving the durable completion
/// acknowledgement. A lost release leaves a retry receipt, while a duplicate
/// release is harmless.
pub(super) fn release_large_response(
    root: &Path,
    authenticated_node_id: &str,
    manifest: &LargeResponseManifest,
) -> TransportResult<()> {
    if manifest.authenticated_node_id != authenticated_node_id {
        return Err(TransportError::IncompleteReply(
            "oversized response belongs to a different authenticated edge".to_string(),
        ));
    }
    let Some(root_dir) = stage_root_dir_optional(root)? else {
        return Ok(());
    };
    let path = response_completion_path(root, manifest);
    let Some((parent, leaf)) = parent_and_leaf_optional_from_root(
        &root_dir,
        root,
        &path,
        "oversized response completion",
    )?
    else {
        return Ok(());
    };
    let expected = manifest.encode()?;
    let Some(bytes) = parent.read_optional(
        &leaf,
        Some(expected.len()),
        "oversized response completion receipt",
    )?
    else {
        return Ok(());
    };
    if bytes.len() != expected.len() {
        return Err(TransportError::IncompleteReply(
            "oversized response completion receipt length does not match its manifest".to_string(),
        ));
    }
    let stored = LargeResponseManifest::decode(&bytes).ok_or_else(|| {
        TransportError::IncompleteReply(
            "oversized response completion receipt is invalid".to_string(),
        )
    })?;
    if stored != *manifest {
        return Err(TransportError::IncompleteReply(
            "oversized response completion identity does not match".to_string(),
        ));
    }
    parent.unlink_file(&leaf, "oversized response completion")
}

/// Test-seam-only receipt inspection, performed while the response-stage lock
/// is held by the caller so the observation has no completion race.
#[cfg(feature = "test-seams")]
pub(super) fn response_completion_receipt_exists(
    root: &Path,
    manifest: &LargeResponseManifest,
) -> TransportResult<bool> {
    let path = response_completion_path(root, manifest);
    let Some(root_dir) = stage_root_dir_optional(root)? else {
        return Ok(false);
    };
    let Some((parent, leaf)) = parent_and_leaf_optional_from_root(
        &root_dir,
        root,
        &path,
        "oversized response completion",
    )?
    else {
        return Ok(false);
    };
    Ok(parent
        .read_optional(&leaf, None, "oversized response completion receipt")?
        .is_some())
}

pub(super) fn response_stage_path(root: &Path, manifest: &LargeResponseManifest) -> PathBuf {
    let subject = blake3::hash(manifest.subject.as_bytes()).to_hex();
    let scope = blake3::hash(
        [
            manifest.authenticated_node_id.as_bytes(),
            &manifest.publication_nonce,
            manifest.subject.as_bytes(),
            &manifest.request_digest,
            &manifest.response_digest,
            &manifest.total_bytes.to_be_bytes(),
        ]
        .concat()
        .as_slice(),
    )
    .to_hex();
    root.join("responses")
        .join(&manifest.authenticated_node_id)
        .join(subject.as_str())
        .join(scope.as_str())
}

pub(super) fn response_completion_path(root: &Path, manifest: &LargeResponseManifest) -> PathBuf {
    let scope = blake3::hash(
        [
            manifest.authenticated_node_id.as_bytes(),
            &manifest.publication_nonce,
            manifest.subject.as_bytes(),
            &manifest.request_digest,
            &manifest.response_digest,
            &manifest.total_bytes.to_be_bytes(),
        ]
        .concat()
        .as_slice(),
    )
    .to_hex();
    root.join("response-completions")
        .join(&manifest.authenticated_node_id)
        .join(format!("{}.msgpack", scope))
}

fn read_exact_at(
    stage: &StageDir,
    name: &str,
    expected: usize,
    what: &str,
) -> TransportResult<Vec<u8>> {
    let bytes = stage.read_file(name, Some(expected), what)?;
    if bytes.len() != expected {
        return Err(TransportError::IncompleteReply(format!(
            "{what} length does not match its manifest"
        )));
    }
    Ok(bytes)
}

fn persist_response_manifest_at(
    stage: &StageDir,
    manifest: &LargeResponseManifest,
) -> TransportResult<()> {
    let encoded = manifest.encode()?;
    match stage.read_optional(
        "manifest.msgpack",
        Some(encoded.len()),
        "durable oversized response manifest",
    )? {
        Some(existing) if existing == encoded => Ok(()),
        Some(_) => Err(TransportError::IncompleteReply(
            "durable oversized response manifest differs from the request identity".to_string(),
        )),
        None => stage.persist_new("manifest.msgpack", &encoded, "oversized response manifest"),
    }
}

fn persist_response_chunk_at(
    stage: &StageDir,
    manifest: &LargeResponseManifest,
    sequence: u64,
    bytes: &[u8],
) -> TransportResult<()> {
    let expected_bytes = response_chunk_len(manifest, sequence)?;
    if bytes.len() != expected_bytes {
        return Err(TransportError::Other(
            "oversized response chunk length does not match its manifest sequence".to_string(),
        ));
    }
    let digest = *blake3::hash(bytes).as_bytes();
    let target = format!("{sequence:016}.reply");
    match stage.read_optional(
        &target,
        Some(expected_bytes),
        "durable oversized response chunk",
    )? {
        Some(existing)
            if existing.len() == expected_bytes
                && *blake3::hash(&existing).as_bytes() == digest =>
        {
            Ok(())
        }
        Some(_) => Err(TransportError::IncompleteReply(
            "durable oversized response chunk differs from the reply bytes".to_string(),
        )),
        None => stage.persist_new(&target, bytes, "oversized response chunk"),
    }?;
    let digest_target = format!("{sequence:016}.digest");
    match stage.read_optional(
        &digest_target,
        Some(blake3::OUT_LEN),
        "durable oversized response chunk digest",
    )? {
        Some(existing) if existing.as_slice() == digest => Ok(()),
        Some(_) => Err(TransportError::IncompleteReply(
            "durable oversized response chunk digest differs from its bytes".to_string(),
        )),
        None => stage.persist_new(&digest_target, &digest, "oversized response chunk digest"),
    }
}

fn validate_response_stage_at(
    stage: &StageDir,
    node: &str,
    manifest: &LargeResponseManifest,
) -> TransportResult<()> {
    if manifest.authenticated_node_id != node {
        return Err(TransportError::IncompleteReply(
            "oversized response belongs to a different authenticated edge".to_string(),
        ));
    }
    let expected = manifest.encode()?;
    let bytes = read_exact_at(
        stage,
        "manifest.msgpack",
        expected.len(),
        "durable oversized response manifest",
    )?;
    let stored = LargeResponseManifest::decode(&bytes).ok_or_else(|| {
        TransportError::IncompleteReply(
            "durable oversized response manifest is invalid".to_string(),
        )
    })?;
    if stored != *manifest {
        return Err(TransportError::IncompleteReply(
            "durable oversized response manifest identity does not match".to_string(),
        ));
    }
    Ok(())
}

fn response_chunk_len(manifest: &LargeResponseManifest, sequence: u64) -> TransportResult<usize> {
    let sequence = usize::try_from(sequence).map_err(|_| {
        TransportError::IncompleteReply(
            "oversized response chunk sequence cannot be represented".to_string(),
        )
    })?;
    if u64::try_from(sequence).ok() >= Some(manifest.total_chunks) {
        return Err(TransportError::IncompleteReply(
            "oversized response chunk sequence is outside the manifest".to_string(),
        ));
    }
    let total = usize::try_from(manifest.total_bytes).map_err(|_| {
        TransportError::IncompleteReply(
            "oversized response length cannot be represented".to_string(),
        )
    })?;
    let prefix = sequence.checked_mul(RESPONSE_CHUNK_BYTES).ok_or_else(|| {
        TransportError::IncompleteReply("oversized response chunk offset overflows".to_string())
    })?;
    let remaining = total.checked_sub(prefix).ok_or_else(|| {
        TransportError::IncompleteReply(
            "oversized response chunk offset exceeds its length".to_string(),
        )
    })?;
    Ok(remaining.min(RESPONSE_CHUNK_BYTES))
}

/// Applies the server-local Policy 12 storage pressure setting only to
/// abandoned oversized-response transport data. It never changes sync
/// progress, request staging, or an explicitly protected active unit.
pub(super) fn enforce_response_stage_budget(
    root: &Path,
    budget: u64,
    protected: &[PathBuf],
) -> TransportResult<()> {
    reserve_response_stage_budget(root, budget, protected, 0)
}

fn reserve_response_stage_budget(
    root: &Path,
    budget: u64,
    protected: &[PathBuf],
    required: u64,
) -> TransportResult<()> {
    let Some(root_dir) = stage_root_dir_optional(root)? else {
        return if required <= budget {
            Ok(())
        } else {
            Err(TransportError::Other(format!(
                "durable oversized response storage requires {required} bytes, exceeding the configured {budget}-byte budget while active transfers remain protected"
            )))
        };
    };
    let mut units = Vec::new();
    if let Some(responses) =
        root_dir.child_existing_optional("responses", "oversized response stages")?
    {
        collect_response_stage_units_at(&responses, PathBuf::from("responses"), 3, &mut units)?;
    }
    if let Some(receipts) =
        root_dir.child_existing_optional("response-completions", "oversized response receipts")?
    {
        collect_response_receipts_at(&receipts, PathBuf::from("response-completions"), &mut units)?;
    }
    let protected = protected
        .iter()
        .map(|path| path.strip_prefix(root).map(Path::to_path_buf))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| {
            TransportError::IncompleteReply(
                "protected oversized response stage escaped its durable root".to_string(),
            )
        })?;
    let mut total = units.iter().map(|unit| unit.bytes).sum::<u64>();
    units.sort_by_key(|unit| unit.modified);
    for unit in units {
        if total
            .checked_add(required)
            .is_some_and(|needed| needed <= budget)
        {
            break;
        }
        if protected.iter().any(|active| active == &unit.relative) {
            continue;
        }
        evict_pressure_unit(&root_dir, &unit)?;
        total = total.saturating_sub(unit.bytes);
    }
    let needed = total.checked_add(required).ok_or_else(|| {
        TransportError::Other(
            "durable oversized response storage requirement overflows".to_string(),
        )
    })?;
    if needed > budget {
        Err(TransportError::Other(format!(
            "durable oversized response storage requires {needed} bytes, exceeding the configured {budget}-byte budget while active transfers remain protected"
        )))
    } else {
        Ok(())
    }
}

/// Read-only durable-response inventory for the real transport test seam. This is
/// deliberately unavailable in production builds: it must not become a
/// transport control surface.
#[cfg(feature = "test-seams")]
pub(super) fn response_stage_counts(root: &Path) -> TransportResult<(usize, usize)> {
    let Some(root_dir) = stage_root_dir_optional(root)? else {
        return Ok((0, 0));
    };
    let mut stages = Vec::new();
    if let Some(responses) =
        root_dir.child_existing_optional("responses", "oversized response stages")?
    {
        collect_response_stage_units_at(&responses, PathBuf::from("responses"), 3, &mut stages)?;
    }
    let mut receipts = Vec::new();
    if let Some(receipt_root) =
        root_dir.child_existing_optional("response-completions", "oversized response receipts")?
    {
        collect_response_receipts_at(
            &receipt_root,
            PathBuf::from("response-completions"),
            &mut receipts,
        )?;
    }
    Ok((stages.len(), receipts.len()))
}

struct PressureUnit {
    modified: (i64, i64),
    bytes: u64,
    relative: PathBuf,
    dev: u64,
    ino: u64,
    directory: bool,
}

fn collect_response_stage_units_at(
    root: &StageDir,
    relative: PathBuf,
    depth: usize,
    units: &mut Vec<PressureUnit>,
) -> TransportResult<()> {
    for (name, stat) in root.entries("oversized response stages")? {
        if stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
            return Err(TransportError::IncompleteReply(
                "oversized response stages contain a symlink or unexpected file type".to_string(),
            ));
        }
        let child = root.child_existing(&name, "oversized response stage")?;
        let path = relative.join(&name);
        if depth == 1 {
            let (dev, ino) =
                canonical_raw_stat_identity(stat.st_dev, stat.st_ino, "oversized response stage")?;
            units.push(PressureUnit {
                modified: stat_modified(&stat),
                bytes: child.directory_bytes("oversized response stage")?,
                relative: path,
                dev,
                ino,
                directory: true,
            });
        } else {
            collect_response_stage_units_at(&child, path, depth - 1, units)?;
        }
    }
    Ok(())
}

fn collect_response_receipts_at(
    root: &StageDir,
    relative: PathBuf,
    units: &mut Vec<PressureUnit>,
) -> TransportResult<()> {
    for (node, node_stat) in root.entries("oversized response receipts")? {
        if node_stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
            return Err(TransportError::IncompleteReply(
                "oversized response receipts contain a symlink or unexpected file type".to_string(),
            ));
        }
        let node_dir = root.child_existing(&node, "oversized response receipt directory")?;
        for (name, stat) in node_dir.entries("oversized response receipt")? {
            if stat.st_mode & libc::S_IFMT != libc::S_IFREG {
                return Err(TransportError::IncompleteReply(
                    "oversized response receipts contain a symlink or unexpected file type"
                        .to_string(),
                ));
            }
            let (dev, ino) = canonical_raw_stat_identity(
                stat.st_dev,
                stat.st_ino,
                "oversized response receipt",
            )?;
            units.push(PressureUnit {
                modified: stat_modified(&stat),
                bytes: stat.st_size.max(0) as u64,
                relative: relative.join(&node).join(&name),
                dev,
                ino,
                directory: false,
            });
        }
    }
    Ok(())
}

fn evict_pressure_unit(root: &StageDir, unit: &PressureUnit) -> TransportResult<()> {
    let full = root.display.join(&unit.relative);
    let (parent, leaf) = parent_and_leaf_from_root(
        root,
        &root.display,
        &full,
        false,
        "oversized response pressure unit",
    )?;
    if unit.directory {
        let child = parent.child_existing(&leaf, "oversized response pressure stage")?;
        use std::os::unix::fs::MetadataExt;
        let metadata = child.file.metadata().map_err(|err| {
            stage_io_error(
                &child.display,
                "cannot inspect oversized response pressure stage",
                err,
            )
        })?;
        if metadata.dev() != unit.dev || metadata.ino() != unit.ino {
            return Err(TransportError::IncompleteReply(
                "oversized response pressure unit changed after inventory".to_string(),
            ));
        }
        parent.remove_held_tree_child_at_pause(
            &leaf,
            child,
            "oversized response pressure stage",
            PausePoint::Pressure,
        )
    } else {
        let name = StageDir::name(&leaf, "oversized response pressure unit")?;
        let mut current = unsafe { std::mem::zeroed::<libc::stat>() };
        if unsafe {
            libc::fstatat(
                parent.file.as_raw_fd(),
                name.as_ptr(),
                &mut current,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            return Err(stage_io_error(
                &parent.display.join(&leaf),
                "cannot revalidate oversized response pressure unit",
                std::io::Error::last_os_error(),
            ));
        }
        let current_identity = canonical_raw_stat_identity(
            current.st_dev,
            current.st_ino,
            "oversized response pressure unit",
        )?;
        if current_identity != (unit.dev, unit.ino) {
            return Err(TransportError::IncompleteReply(
                "oversized response pressure unit changed after inventory".to_string(),
            ));
        }
        #[cfg(test)]
        pause_after_pinned_parent_for_test(PausePoint::Pressure);
        parent.unlink_regular_if_same(&leaf, &current, "oversized response pressure receipt")
    }
}

fn canonical_raw_stat_identity<D, I>(dev: D, ino: I, what: &str) -> TransportResult<(u64, u64)>
where
    D: TryInto<u64>,
    I: TryInto<u64>,
{
    let dev = dev.try_into().map_err(|_| {
        TransportError::IncompleteReply(format!(
            "{what} has an unrepresentable filesystem device identity"
        ))
    })?;
    let ino = ino.try_into().map_err(|_| {
        TransportError::IncompleteReply(format!(
            "{what} has an unrepresentable filesystem inode identity"
        ))
    })?;
    Ok((dev, ino))
}

#[cfg(all(unix, target_os = "linux"))]
fn stat_modified(stat: &libc::stat) -> (i64, i64) {
    (stat.st_mtime, stat.st_mtime_nsec)
}

#[cfg(all(unix, target_os = "macos"))]
fn stat_modified(stat: &libc::stat) -> (i64, i64) {
    (stat.st_mtime, stat.st_mtime_nsec)
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn stat_modified(stat: &libc::stat) -> (i64, i64) {
    (stat.st_mtime, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_stat_identity_rejects_negative_or_unrepresentable_components() {
        let negative_device = canonical_raw_stat_identity(-1_i64, 7_i64, "test file")
            .expect_err("negative device identity must fail closed");
        assert!(matches!(
            negative_device,
            TransportError::IncompleteReply(message)
                if message == "test file has an unrepresentable filesystem device identity"
        ));

        let oversized_inode = canonical_raw_stat_identity(7_u128, u128::MAX, "test file")
            .expect_err("oversized inode identity must fail closed");
        assert!(matches!(
            oversized_inode,
            TransportError::IncompleteReply(message)
                if message == "test file has an unrepresentable filesystem inode identity"
        ));
    }

    #[cfg(unix)]
    fn outside_sentinel(outside: &Path) -> PathBuf {
        let sentinel = outside.join("must-survive");
        write_outside_sentinel(&sentinel);
        sentinel
    }

    #[cfg(unix)]
    fn write_outside_sentinel(sentinel: &Path) {
        use std::os::unix::fs::PermissionsExt;

        fs::create_dir_all(sentinel.parent().expect("outside sentinel parent"))
            .expect("create outside sentinel parent");
        fs::write(sentinel, b"outside").expect("write outside sentinel");
        fs::set_permissions(sentinel, fs::Permissions::from_mode(0o640))
            .expect("set outside sentinel mode");
    }

    #[cfg(unix)]
    fn assert_outside_sentinel(sentinel: &Path) {
        use std::os::unix::fs::PermissionsExt;

        assert_eq!(
            fs::read(sentinel).expect("read outside sentinel"),
            b"outside"
        );
        assert_eq!(
            fs::metadata(sentinel)
                .expect("outside sentinel metadata")
                .permissions()
                .mode()
                & 0o777,
            0o640,
            "outside sentinel mode must survive a pinned-parent race",
        );
    }

    #[cfg(unix)]
    fn swap_visible_parent(visible: &Path, outside: &Path, moved: &Path) {
        use std::os::unix::fs::symlink;

        fs::rename(visible, moved).expect("move original visible parent aside");
        symlink(outside, visible).expect("replace visible parent with outside symlink");
    }

    #[cfg(unix)]
    fn restore_visible_parent(visible: &Path, moved: &Path) {
        fs::remove_file(visible).expect("remove outside symlink");
        fs::rename(moved, visible).expect("restore original visible parent");
    }

    #[cfg(unix)]
    fn completed_request(root: &Path) -> (LargeRequestBegin, PathBuf) {
        let payload = vec![0xa1; FRAGMENT_BYTES + 1];
        let begin = LargeRequestBegin::new(
            "sync.push",
            *blake3::hash(&payload).as_bytes(),
            payload.len(),
        )
        .expect("build request descriptor");
        begin_request(root, "edge-a", &begin).expect("begin request");
        for (sequence, bytes) in payload.chunks(FRAGMENT_BYTES).enumerate() {
            let fragment: LargeRequestFragment = rmp_serde::from_slice(
                &LargeRequestFragment::encode(
                    &begin.subject,
                    begin.unit_digest,
                    payload.len(),
                    sequence,
                    begin.total_fragments as usize,
                    bytes,
                )
                .expect("encode request fragment"),
            )
            .expect("decode request fragment");
            accept_descriptor_fragment(root, "edge-a", &begin, &fragment)
                .expect("persist complete request");
        }
        let stage = stage_path(root, &stage_manifest("edge-a", &begin));
        (begin, stage)
    }

    #[cfg(unix)]
    #[test]
    #[serial_test::serial(safe_fs_pause)]
    fn pinned_parent_create_directory_and_chmod_stay_in_the_original_tree() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside");
        let sentinel = outside_sentinel(outside.path());
        fs::create_dir(outside.path().join("stages")).expect("create outside stage-root lookalike");
        fs::set_permissions(
            outside.path().join("stages"),
            fs::Permissions::from_mode(0o755),
        )
        .expect("set outside stage-root mode");
        let outside_root_sentinel = outside.path().join("stages").join("create-must-survive");
        write_outside_sentinel(&outside_root_sentinel);
        let visible = temp.path().join("visible");
        fs::create_dir(&visible).expect("create visible parent");
        let root = visible.join("stages");
        let request = vec![0xa0; FRAGMENT_BYTES + 1];
        let begin = LargeRequestBegin::new(
            "sync.push",
            *blake3::hash(&request).as_bytes(),
            request.len(),
        )
        .expect("build descriptor");
        let pause = arm_pinned_parent_pause_for_test(PausePoint::CreateDirectory);
        let hook = pause.hook();
        let worker_root = root.clone();
        let worker = std::thread::spawn(move || {
            hook.claim_for_current_thread();
            begin_request(&worker_root, "edge-a", &begin)
        });
        pause.wait_until_reached();
        let moved = temp.path().join("visible-real");
        swap_visible_parent(&visible, outside.path(), &moved);
        pause.resume();
        worker
            .join()
            .expect("create worker")
            .expect("pinned create succeeds in original directory");
        assert!(
            moved.join("stages").is_dir(),
            "mkdirat must use the held original parent"
        );
        assert_eq!(
            fs::metadata(moved.join("stages"))
                .expect("stage root metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(outside.path().join("stages"))
                .expect("outside stage-root metadata")
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
        assert_outside_sentinel(&outside_root_sentinel);
        assert_outside_sentinel(&sentinel);
        restore_visible_parent(&visible, &moved);
    }

    #[cfg(unix)]
    #[test]
    #[serial_test::serial(safe_fs_pause)]
    fn pinned_parent_request_response_and_receipt_publication_stay_in_the_original_tree() {
        let temp = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside");
        let sentinel = outside_sentinel(outside.path());
        let visible = temp.path().join("visible");
        fs::create_dir(&visible).expect("create visible parent");
        let root = visible.join("stages");

        let request = vec![0xa1; FRAGMENT_BYTES + 1];
        let begin = LargeRequestBegin::new(
            "sync.push",
            *blake3::hash(&request).as_bytes(),
            request.len(),
        )
        .expect("build request descriptor");
        let request_stage = stage_path(&root, &stage_manifest("edge-a", &begin));
        let outside_request_manifest = outside
            .path()
            .join("stages")
            .join(
                request_stage
                    .strip_prefix(&root)
                    .expect("request manifest suffix"),
            )
            .join("manifest.msgpack");
        write_outside_sentinel(&outside_request_manifest);
        let pause = arm_pinned_parent_pause_for_test(PausePoint::Publish);
        let hook = pause.hook();
        let worker_root = root.clone();
        let worker = std::thread::spawn(move || {
            hook.claim_for_current_thread();
            begin_request(&worker_root, "edge-a", &begin)
        });
        pause.wait_until_reached();
        let moved = temp.path().join("visible-request-real");
        swap_visible_parent(&visible, outside.path(), &moved);
        pause.resume();
        worker
            .join()
            .expect("request publish worker")
            .expect("request manifest publish");
        assert!(moved.join("stages").exists());
        assert_outside_sentinel(&outside_request_manifest);
        assert_outside_sentinel(&sentinel);
        restore_visible_parent(&visible, &moved);

        let payload = vec![0xa2; RESPONSE_CHUNK_BYTES + 1];
        let publication_nonce = [0x52; 32];
        let expected_response_manifest = LargeResponseManifest {
            version: RESPONSE_MANIFEST_VERSION,
            publication_nonce,
            subject: "sync.pull".to_string(),
            authenticated_node_id: "edge-a".to_string(),
            request_digest: *blake3::hash(b"request").as_bytes(),
            response_digest: *blake3::hash(&payload).as_bytes(),
            total_bytes: payload.len() as u64,
            total_chunks: payload.len().div_ceil(RESPONSE_CHUNK_BYTES) as u64,
        };
        let outside_response_manifest = outside
            .path()
            .join("stages")
            .join(
                response_stage_path(&root, &expected_response_manifest)
                    .strip_prefix(&root)
                    .expect("response manifest suffix"),
            )
            .join("manifest.msgpack");
        write_outside_sentinel(&outside_response_manifest);
        let pause = arm_pinned_parent_pause_for_test(PausePoint::Publish);
        let hook = pause.hook();
        let worker_root = root.clone();
        let worker_payload = payload.clone();
        let worker = std::thread::spawn(move || {
            hook.claim_for_current_thread();
            stage_large_response_with_nonce(
                &worker_root,
                "edge-a",
                "sync.pull",
                *blake3::hash(b"request").as_bytes(),
                &worker_payload,
                publication_nonce,
            )
        });
        pause.wait_until_reached();
        let moved = temp.path().join("visible-response-real");
        swap_visible_parent(&visible, outside.path(), &moved);
        pause.resume();
        let manifest = worker
            .join()
            .expect("response publish worker")
            .expect("response publish");
        assert_eq!(manifest, expected_response_manifest);
        assert!(response_stage_path(&moved.join("stages"), &manifest).exists());
        assert_outside_sentinel(&outside_response_manifest);
        assert_outside_sentinel(&sentinel);
        restore_visible_parent(&visible, &moved);

        let pause = arm_pinned_parent_pause_for_test(PausePoint::Publish);
        let hook = pause.hook();
        let worker_root = root.clone();
        let worker_manifest = manifest.clone();
        let worker = std::thread::spawn(move || {
            hook.claim_for_current_thread();
            complete_large_response(&worker_root, "edge-a", &worker_manifest)
        });
        pause.wait_until_reached();
        let outside_receipt = outside.path().join("stages").join(
            response_completion_path(&root, &manifest)
                .strip_prefix(&root)
                .expect("receipt suffix"),
        );
        write_outside_sentinel(&outside_receipt);
        let moved = temp.path().join("visible-receipt-real");
        swap_visible_parent(&visible, outside.path(), &moved);
        pause.resume();
        worker
            .join()
            .expect("receipt publish worker")
            .expect("receipt publish");
        assert!(response_completion_path(&moved.join("stages"), &manifest).exists());
        assert_outside_sentinel(&outside_receipt);
        assert_outside_sentinel(&sentinel);
        restore_visible_parent(&visible, &moved);
    }

    #[cfg(unix)]
    #[test]
    #[serial_test::serial(safe_fs_pause)]
    fn pinned_parent_response_read_uses_the_original_stage_after_a_visible_swap() {
        let temp = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside");
        let sentinel = outside_sentinel(outside.path());
        let visible = temp.path().join("visible");
        fs::create_dir(&visible).expect("create visible parent");
        let root = visible.join("stages");
        let payload = vec![0xa3; RESPONSE_CHUNK_BYTES + 1];
        let manifest = stage_large_response(
            &root,
            "edge-a",
            "sync.pull",
            *blake3::hash(b"request").as_bytes(),
            &payload,
        )
        .expect("stage response");
        let pause = arm_pinned_parent_pause_for_test(PausePoint::Read);
        let hook = pause.hook();
        let worker_root = root.clone();
        let worker_manifest = manifest.clone();
        let worker = std::thread::spawn(move || {
            hook.claim_for_current_thread();
            response_chunk(&worker_root, "edge-a", &worker_manifest, 0)
        });
        pause.wait_until_reached();
        let moved = temp.path().join("visible-read-real");
        swap_visible_parent(&visible, outside.path(), &moved);
        pause.resume();
        let chunk = worker
            .join()
            .expect("read worker")
            .expect("read from pinned stage");
        assert_eq!(chunk.bytes, payload[..RESPONSE_CHUNK_BYTES]);
        assert_outside_sentinel(&sentinel);
        restore_visible_parent(&visible, &moved);
    }

    #[cfg(unix)]
    #[test]
    #[serial_test::serial(safe_fs_pause)]
    fn pinned_parent_request_and_response_cleanup_never_delete_the_swapped_outside_tree() {
        let temp = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside");
        let sentinel = outside_sentinel(outside.path());
        let visible = temp.path().join("visible");
        fs::create_dir(&visible).expect("create visible parent");
        let root = visible.join("stages");
        let (_begin, stage) = completed_request(&root);
        let outside_stage = outside
            .path()
            .join("stages")
            .join(stage.strip_prefix(&root).expect("request suffix"));
        fs::create_dir_all(&outside_stage).expect("create outside request lookalike");
        let outside_stage_sentinel = outside_stage.join("request-cleanup-must-survive");
        fs::write(&outside_stage_sentinel, b"outside")
            .expect("write outside request stage sentinel");
        let pause = arm_pinned_parent_pause_for_test(PausePoint::Cleanup);
        let hook = pause.hook();
        let worker_root = root.clone();
        let worker_stage = stage.clone();
        let worker = std::thread::spawn(move || {
            hook.claim_for_current_thread();
            remove_completed_stage(&worker_root, &worker_stage)
        });
        pause.wait_until_reached();
        let moved = temp.path().join("visible-request-cleanup-real");
        swap_visible_parent(&visible, outside.path(), &moved);
        pause.resume();
        worker
            .join()
            .expect("request cleanup worker")
            .expect("request cleanup");
        assert!(
            !moved
                .join("stages")
                .join(stage.strip_prefix(&root).expect("request suffix"))
                .exists()
        );
        assert_eq!(
            fs::read(&outside_stage_sentinel).expect("outside request stage sentinel"),
            b"outside"
        );
        assert_outside_sentinel(&sentinel);
        restore_visible_parent(&visible, &moved);

        let payload = vec![0xa4; RESPONSE_CHUNK_BYTES + 1];
        let manifest = stage_large_response(
            &root,
            "edge-a",
            "sync.pull",
            *blake3::hash(b"request").as_bytes(),
            &payload,
        )
        .expect("stage response");
        let response_stage = response_stage_path(&root, &manifest);
        let outside_response = outside
            .path()
            .join("stages")
            .join(response_stage.strip_prefix(&root).expect("response suffix"));
        fs::create_dir_all(&outside_response).expect("create outside response lookalike");
        let outside_response_sentinel = outside_response.join("response-cleanup-must-survive");
        fs::write(&outside_response_sentinel, b"outside")
            .expect("write outside response stage sentinel");
        let pause = arm_pinned_parent_pause_for_test(PausePoint::Cleanup);
        let hook = pause.hook();
        let worker_root = root.clone();
        let worker_manifest = manifest.clone();
        let worker = std::thread::spawn(move || {
            hook.claim_for_current_thread();
            complete_large_response(&worker_root, "edge-a", &worker_manifest)
        });
        pause.wait_until_reached();
        let moved = temp.path().join("visible-response-cleanup-real");
        swap_visible_parent(&visible, outside.path(), &moved);
        pause.resume();
        worker
            .join()
            .expect("response cleanup worker")
            .expect("response cleanup");
        assert!(!response_stage_path(&moved.join("stages"), &manifest).exists());
        assert_eq!(
            fs::read(&outside_response_sentinel).expect("outside response stage sentinel"),
            b"outside"
        );
        assert_outside_sentinel(&sentinel);
        restore_visible_parent(&visible, &moved);
    }

    #[cfg(unix)]
    #[test]
    #[serial_test::serial(safe_fs_pause)]
    fn pinned_parent_pressure_eviction_cannot_follow_a_swapped_visible_ancestor() {
        let temp = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside");
        let sentinel = outside_sentinel(outside.path());
        let visible = temp.path().join("visible");
        fs::create_dir(&visible).expect("create visible parent");
        let root = visible.join("stages");
        let payload = vec![0xa5; RESPONSE_CHUNK_BYTES + 1];
        let manifest = stage_large_response(
            &root,
            "edge-a",
            "sync.pull",
            *blake3::hash(b"request").as_bytes(),
            &payload,
        )
        .expect("stage inactive response");
        let stage = response_stage_path(&root, &manifest);
        let outside_stage = outside
            .path()
            .join("stages")
            .join(stage.strip_prefix(&root).expect("pressure suffix"));
        fs::create_dir_all(&outside_stage).expect("create outside pressure lookalike");
        let outside_stage_sentinel = outside_stage.join("pressure-must-survive");
        fs::write(&outside_stage_sentinel, b"outside").expect("write outside pressure sentinel");
        let pause = arm_pinned_parent_pause_for_test(PausePoint::Pressure);
        let hook = pause.hook();
        let worker_root = root.clone();
        let worker = std::thread::spawn(move || {
            hook.claim_for_current_thread();
            enforce_response_stage_budget(&worker_root, 1, &[])
        });
        pause.wait_until_reached();
        let moved = temp.path().join("visible-pressure-real");
        swap_visible_parent(&visible, outside.path(), &moved);
        pause.resume();
        worker
            .join()
            .expect("pressure worker")
            .expect("pressure eviction");
        assert!(!response_stage_path(&moved.join("stages"), &manifest).exists());
        assert_eq!(
            fs::read(&outside_stage_sentinel).expect("outside pressure sentinel"),
            b"outside"
        );
        assert_outside_sentinel(&sentinel);
        restore_visible_parent(&visible, &moved);
    }

    #[test]
    fn request_control_is_tagged_and_bounds_only_the_descriptor() {
        let payload = vec![0x51; FRAGMENT_BYTES + 1];
        let begin = LargeRequestBegin::new(
            "contextdb.sync.push.tenant-a",
            *blake3::hash(&payload).as_bytes(),
            payload.len(),
        )
        .expect("build descriptor");
        let encoded = LargeRequestControl::BeginV1(begin.clone())
            .encode()
            .expect("encode descriptor");
        assert_eq!(
            LargeRequestControl::decode(&encoded).expect("decode tagged descriptor"),
            LargeRequestControl::BeginV1(begin.clone()),
        );
        assert!(
            LargeRequestControl::decode(
                &LargeRequestFragment::encode(
                    "contextdb.sync.push.tenant-a",
                    *blake3::hash(&payload).as_bytes(),
                    payload.len(),
                    0,
                    2,
                    &payload[..FRAGMENT_BYTES],
                )
                .expect("encode untagged fragment")
            )
            .is_err()
        );
        let mut too_large = Vec::from(CONTROL_MAGIC.as_slice());
        too_large.extend_from_slice(&[CONTROL_VERSION, BEGIN_KIND]);
        too_large.resize(too_large.len() + MAX_REQUEST_BEGIN_BYTES + 1, 0);
        assert!(LargeRequestControl::decode(&too_large).is_err());

        let long_subject = format!("sync.{}", "s".repeat(10 * 1024));
        let long_begin = LargeRequestBegin::new(&long_subject, begin.unit_digest, payload.len())
            .expect("bounded descriptor accepts a long legal subject");
        let fragment: LargeRequestFragment = rmp_serde::from_slice(
            &LargeRequestFragment::encode(
                &long_subject,
                long_begin.unit_digest,
                payload.len(),
                0,
                long_begin.total_fragments as usize,
                &payload[..FRAGMENT_BYTES],
            )
            .expect("encode full fragment"),
        )
        .expect("decode full fragment");
        assert!(LargeRequestControl::FragmentV1(fragment).encode().is_ok());
    }

    #[test]
    fn descriptor_metadata_and_authenticated_node_cannot_resume_another_stage() {
        let temp = tempfile::tempdir().expect("stage tempdir");
        let payload = vec![0x55; FRAGMENT_BYTES + 1];
        let digest = *blake3::hash(&payload).as_bytes();
        let begin = LargeRequestBegin::new("contextdb.sync.push.tenant-a", digest, payload.len())
            .expect("build descriptor");
        let first: LargeRequestFragment = rmp_serde::from_slice(
            &LargeRequestFragment::encode(
                &begin.subject,
                begin.unit_digest,
                payload.len(),
                0,
                2,
                &payload[..FRAGMENT_BYTES],
            )
            .expect("encode first fragment"),
        )
        .expect("decode first fragment");
        begin_request(temp.path(), "edge-a", &begin).expect("begin stage");
        accept_descriptor_fragment(temp.path(), "edge-a", &begin, &first)
            .expect("persist first fragment");
        for different in [
            LargeRequestBegin::new("contextdb.sync.pull.tenant-a", digest, payload.len()),
            LargeRequestBegin::new(
                "contextdb.sync.push.tenant-a",
                *blake3::hash(b"other").as_bytes(),
                payload.len(),
            ),
            LargeRequestBegin::new("contextdb.sync.push.tenant-a", digest, payload.len() + 1),
        ] {
            let different = different.expect("build isolated descriptor");
            assert!(matches!(
                begin_request(temp.path(), "edge-a", &different)
                    .expect("separate descriptor stage"),
                StageOutcome::Pending { next_missing: 0 }
            ));
        }
        assert!(matches!(
            begin_request(temp.path(), "edge-b", &begin).expect("separate edge stage"),
            StageOutcome::Pending { next_missing: 0 }
        ));
        assert!(matches!(
            begin_request(temp.path(), "edge-a", &begin).expect("resume original descriptor"),
            StageOutcome::Pending { next_missing: 1 }
        ));
    }

    #[test]
    fn durable_partial_stage_resumes_only_in_the_authenticated_scope() {
        let temp = tempfile::tempdir().expect("stage tempdir");
        let payload = vec![0x6d; FRAGMENT_BYTES + 17];
        let unit_digest = *blake3::hash(&payload).as_bytes();
        let total_fragments = fragment_count(payload.len());
        let first = LargeRequestFragment::encode(
            "contextdb.sync.push.tenant-a",
            unit_digest,
            payload.len(),
            0,
            total_fragments,
            &payload[..FRAGMENT_BYTES],
        )
        .expect("encode first fragment");
        let final_fragment = LargeRequestFragment::encode(
            "contextdb.sync.push.tenant-a",
            unit_digest,
            payload.len(),
            1,
            total_fragments,
            &payload[FRAGMENT_BYTES..],
        )
        .expect("encode final fragment");

        assert!(matches!(
            accept_fragment(temp.path(), "edge-a", &first).expect("persist first fragment"),
            StageOutcome::Pending { .. }
        ));

        let manifest = StageManifest {
            version: MANIFEST_VERSION,
            subject: "contextdb.sync.push.tenant-a".to_string(),
            authenticated_node_id: "edge-a".to_string(),
            unit_digest,
            total_bytes: payload.len() as u64,
            total_fragments: total_fragments as u32,
            transfer_digest: request_transfer_digest(
                "contextdb.sync.push.tenant-a",
                unit_digest,
                payload.len() as u64,
                total_fragments as u32,
            ),
        };
        let stage = stage_path(temp.path(), &manifest);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for directory in [
                stage.as_path(),
                stage.parent().expect("digest directory"),
                stage
                    .parent()
                    .and_then(Path::parent)
                    .expect("subject directory"),
            ] {
                assert_eq!(
                    fs::metadata(directory)
                        .expect("stage directory metadata")
                        .permissions()
                        .mode()
                        & 0o777,
                    0o700,
                    "each created stage directory is private"
                );
            }
            assert_eq!(
                fs::metadata(stage.join("00000000.part"))
                    .expect("staged fragment metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600,
                "staged fragments are owner-readable only"
            );
            assert_eq!(
                fs::metadata(stage.join("00000000.digest"))
                    .expect("staged fragment digest metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600,
                "staged fragment digests are owner-readable only"
            );
        }

        let wrong_peer = accept_fragment(temp.path(), "edge-b", &final_fragment)
            .expect_err("a different authenticated edge cannot finish the stage");
        assert!(
            wrong_peer.to_string().contains("missing fragment 0"),
            "the other edge must have an independent empty stage: {wrong_peer}"
        );

        match accept_fragment(temp.path(), "edge-a", &final_fragment)
            .expect("resume the durable stage without process-local state")
        {
            StageOutcome::Complete {
                subject,
                payload: assembled,
                ..
            } => {
                assert_eq!(subject, "contextdb.sync.push.tenant-a");
                assert_eq!(assembled, payload);
            }
            StageOutcome::Pending { .. } => {
                panic!("the matching authenticated stage must complete")
            }
        }
    }

    #[test]
    fn fragment_integrity_failure_is_refused_before_staging() {
        let temp = tempfile::tempdir().expect("stage tempdir");
        let payload = vec![0x42; FRAGMENT_BYTES + 1];
        let mut fragment: LargeRequestFragment = rmp_serde::from_slice(
            &LargeRequestFragment::encode(
                "contextdb.sync.push.tenant-a",
                *blake3::hash(&payload).as_bytes(),
                payload.len(),
                0,
                fragment_count(payload.len()),
                &payload[..FRAGMENT_BYTES],
            )
            .expect("encode fragment"),
        )
        .expect("decode fixture fragment");
        fragment.payload[0] ^= 0xff;
        let corrupted = rmp_serde::to_vec_named(&fragment).expect("encode corrupted fragment");

        let error = accept_fragment(temp.path(), "edge-a", &corrupted)
            .expect_err("corrupt fragment must be refused");
        assert!(
            error.to_string().contains("failed integrity validation"),
            "integrity refusal must be explicit: {error}"
        );
        assert!(
            !snapshot_all_stages(temp.path())
                .expect("inspect descriptor-only stage")
                .iter()
                .flat_map(|stage| stage.fragments.iter())
                .any(|stored| stored.sequence == 0),
            "an invalid fragment must not persist bytes after its descriptor"
        );
    }

    #[cfg(unix)]
    #[test]
    fn ordinary_request_staging_refuses_root_and_intermediate_symlinks() {
        use std::os::unix::fs::symlink;

        let payload = vec![0x71; FRAGMENT_BYTES + 1];
        let encoded = LargeRequestFragment::encode(
            "contextdb.sync.push.tenant-a",
            *blake3::hash(&payload).as_bytes(),
            payload.len(),
            0,
            fragment_count(payload.len()),
            &payload[..FRAGMENT_BYTES],
        )
        .expect("encode first fragment");
        for intermediate in [false, true] {
            let temp = tempfile::tempdir().expect("stage tempdir");
            let outside = tempfile::tempdir().expect("outside tempdir");
            let sentinel = outside.path().join("must-survive");
            fs::write(&sentinel, b"outside").expect("write outside sentinel");
            let root = temp.path().join("request-stage");
            if intermediate {
                fs::create_dir(&root).expect("create ordinary root");
                symlink(outside.path(), root.join("edge-a")).expect("link request intermediate");
            } else {
                symlink(outside.path(), &root).expect("link request root");
            }
            let error = accept_fragment(&root, "edge-a", &encoded)
                .expect_err("ordinary request staging must not follow a symlink");
            assert!(error.to_string().contains("symlink"));
            assert_eq!(
                fs::read(&sentinel).expect("read outside sentinel"),
                b"outside"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn ordinary_response_staging_refuses_root_and_intermediate_symlinks_without_a_budget() {
        use std::os::unix::fs::symlink;

        let response = vec![0x72; RESPONSE_CHUNK_BYTES + 1];
        for intermediate in [false, true] {
            let temp = tempfile::tempdir().expect("stage tempdir");
            let outside = tempfile::tempdir().expect("outside tempdir");
            let sentinel = outside.path().join("must-survive");
            fs::write(&sentinel, b"outside").expect("write outside sentinel");
            let root = temp.path().join("response-stage");
            if intermediate {
                fs::create_dir(&root).expect("create ordinary root");
                symlink(outside.path(), root.join("responses"))
                    .expect("link response intermediate");
            } else {
                symlink(outside.path(), &root).expect("link response root");
            }
            let error = stage_large_response(
                &root,
                "edge-a",
                "contextdb.sync.pull.tenant-a",
                *blake3::hash(b"request").as_bytes(),
                &response,
            )
            .expect_err("ordinary response staging must not follow a symlink");
            assert!(error.to_string().contains("symlink"));
            assert_eq!(
                fs::read(&sentinel).expect("read outside sentinel"),
                b"outside"
            );
        }
    }

    #[test]
    fn retained_fragment_without_its_digest_sidecar_is_discarded_and_retried() {
        let temp = tempfile::tempdir().expect("stage tempdir");
        let payload = vec![0x73; FRAGMENT_BYTES + 1];
        let digest = *blake3::hash(&payload).as_bytes();
        let first = LargeRequestFragment::encode(
            "contextdb.sync.push.tenant-a",
            digest,
            payload.len(),
            0,
            2,
            &payload[..FRAGMENT_BYTES],
        )
        .expect("encode first fragment");
        let final_fragment = LargeRequestFragment::encode(
            "contextdb.sync.push.tenant-a",
            digest,
            payload.len(),
            1,
            2,
            &payload[FRAGMENT_BYTES..],
        )
        .expect("encode final fragment");
        accept_fragment(temp.path(), "edge-a", &first).expect("persist first fragment");
        let manifest = StageManifest {
            version: MANIFEST_VERSION,
            subject: "contextdb.sync.push.tenant-a".to_string(),
            authenticated_node_id: "edge-a".to_string(),
            unit_digest: digest,
            total_bytes: payload.len() as u64,
            total_fragments: 2,
            transfer_digest: request_transfer_digest(
                "contextdb.sync.push.tenant-a",
                digest,
                payload.len() as u64,
                2,
            ),
        };
        let stage = stage_path(temp.path(), &manifest);
        fs::remove_file(stage.join("00000000.digest")).expect("simulate crash before sidecar");
        accept_fragment(temp.path(), "edge-a", &first)
            .expect("retry replaces uncommitted fragment");
        assert!(stage.join("00000000.digest").exists());
        assert!(matches!(
            accept_fragment(temp.path(), "edge-a", &final_fragment).expect("finish repaired stage"),
            StageOutcome::Complete { .. }
        ));
    }

    #[test]
    fn corrupt_or_truncated_retained_fragment_or_sidecar_is_discarded_and_retried() {
        let payload = vec![0x74; FRAGMENT_BYTES + 1];
        let digest = *blake3::hash(&payload).as_bytes();
        let first = LargeRequestFragment::encode(
            "contextdb.sync.push.tenant-a",
            digest,
            payload.len(),
            0,
            2,
            &payload[..FRAGMENT_BYTES],
        )
        .expect("encode first fragment");
        let final_fragment = LargeRequestFragment::encode(
            "contextdb.sync.push.tenant-a",
            digest,
            payload.len(),
            1,
            2,
            &payload[FRAGMENT_BYTES..],
        )
        .expect("encode final fragment");
        for sidecar in [false, true] {
            let temp = tempfile::tempdir().expect("stage tempdir");
            accept_fragment(temp.path(), "edge-a", &first).expect("persist first fragment");
            let manifest = StageManifest {
                version: MANIFEST_VERSION,
                subject: "contextdb.sync.push.tenant-a".to_string(),
                authenticated_node_id: "edge-a".to_string(),
                unit_digest: digest,
                total_bytes: payload.len() as u64,
                total_fragments: 2,
                transfer_digest: request_transfer_digest(
                    "contextdb.sync.push.tenant-a",
                    digest,
                    payload.len() as u64,
                    2,
                ),
            };
            let stage = stage_path(temp.path(), &manifest);
            let corrupt = stage.join(if sidecar {
                "00000000.digest"
            } else {
                "00000000.part"
            });
            OpenOptions::new()
                .write(true)
                .open(&corrupt)
                .expect("open retained artifact")
                .set_len(1)
                .expect("truncate retained artifact");
            accept_fragment(temp.path(), "edge-a", &first)
                .expect("correct retry repairs retained artifact");
            assert!(matches!(
                accept_fragment(temp.path(), "edge-a", &final_fragment)
                    .expect("finish repaired stage"),
                StageOutcome::Complete { .. }
            ));
        }
    }

    #[test]
    fn resume_repairs_a_corrupt_middle_fragment_by_discarding_only_its_suffix() {
        let temp = tempfile::tempdir().expect("stage tempdir");
        let payload = vec![0x75; FRAGMENT_BYTES * 3 + 11];
        let begin = LargeRequestBegin::new(
            "contextdb.sync.push.tenant-a",
            *blake3::hash(&payload).as_bytes(),
            payload.len(),
        )
        .expect("build descriptor");
        begin_request(temp.path(), "edge-a", &begin).expect("begin stage");
        let fragments = payload
            .chunks(FRAGMENT_BYTES)
            .enumerate()
            .map(|(sequence, bytes)| {
                rmp_serde::from_slice::<LargeRequestFragment>(
                    &LargeRequestFragment::encode(
                        &begin.subject,
                        begin.unit_digest,
                        payload.len(),
                        sequence,
                        begin.total_fragments as usize,
                        bytes,
                    )
                    .expect("encode fragment"),
                )
                .expect("decode fragment")
            })
            .collect::<Vec<_>>();
        for fragment in &fragments {
            accept_descriptor_fragment(temp.path(), "edge-a", &begin, fragment)
                .expect("persist fragment");
        }
        match begin_request(temp.path(), "edge-a", &begin)
            .expect("restart discovers the complete pre-dispatch stage")
        {
            StageOutcome::Complete {
                subject,
                payload: assembled,
                ..
            } => {
                assert_eq!(subject, begin.subject);
                assert_eq!(assembled, payload);
            }
            StageOutcome::Pending { .. } => panic!("complete durable stage must dispatch on Begin"),
        }
        let manifest = stage_manifest("edge-a", &begin);
        let stage = stage_path(temp.path(), &manifest);
        OpenOptions::new()
            .write(true)
            .open(stage.join("00000001.part"))
            .expect("open middle fragment")
            .set_len(1)
            .expect("truncate middle fragment");
        assert!(matches!(
            begin_request(temp.path(), "edge-a", &begin).expect("repair retained prefix"),
            StageOutcome::Pending { next_missing: 1 }
        ));
        for sequence in 1..begin.total_fragments {
            let stem = format!("{sequence:08}");
            let part = stage.join(format!("{stem}.part"));
            let digest = stage.join(format!("{stem}.digest"));
            assert!(
                !part.exists() && !digest.exists(),
                "repair removes corrupt suffix artifact {sequence}"
            );
        }
        assert!(stage.join("00000000.part").exists());
        assert!(stage.join("00000000.digest").exists());
        for fragment in fragments.iter().skip(1) {
            let outcome = accept_descriptor_fragment(temp.path(), "edge-a", &begin, fragment)
                .expect("retry suffix fragment");
            if fragment.sequence + 1 == begin.total_fragments {
                assert!(matches!(outcome, StageOutcome::Complete { .. }));
            }
        }
    }

    #[test]
    fn incomplete_stage_is_reported_before_any_complete_payload_allocation() {
        let temp = tempfile::tempdir().expect("stage tempdir");
        let payload = vec![0x24; FRAGMENT_BYTES + 19];
        let digest = *blake3::hash(&payload).as_bytes();
        let total_fragments = fragment_count(payload.len());
        let first = LargeRequestFragment::encode(
            "contextdb.sync.push.tenant-a",
            digest,
            payload.len(),
            0,
            total_fragments,
            &payload[..FRAGMENT_BYTES],
        )
        .expect("encode first fragment");
        let final_fragment = LargeRequestFragment::encode(
            "contextdb.sync.push.tenant-a",
            digest,
            payload.len(),
            1,
            total_fragments,
            &payload[FRAGMENT_BYTES..],
        )
        .expect("encode final fragment");
        assert!(matches!(
            accept_fragment(temp.path(), "edge-a", &first).expect("persist first fragment"),
            StageOutcome::Pending { .. }
        ));

        let manifest = StageManifest {
            version: MANIFEST_VERSION,
            subject: "contextdb.sync.push.tenant-a".to_string(),
            authenticated_node_id: "edge-a".to_string(),
            unit_digest: digest,
            total_bytes: payload.len() as u64,
            total_fragments: total_fragments as u32,
            transfer_digest: request_transfer_digest(
                "contextdb.sync.push.tenant-a",
                digest,
                payload.len() as u64,
                total_fragments as u32,
            ),
        };
        let first_path = stage_path(temp.path(), &manifest).join("00000000.part");
        OpenOptions::new()
            .write(true)
            .open(&first_path)
            .expect("open staged fragment")
            .set_len(1)
            .expect("truncate staged fragment");

        let error = accept_fragment(temp.path(), "edge-a", &final_fragment)
            .expect_err("a suffix cannot skip the first repaired fragment");
        assert!(
            error.to_string().contains("missing fragment 0"),
            "the first corrupt fragment becomes the durable retry point: {error}"
        );
        assert!(
            !first_path.exists(),
            "corrupt bytes are discarded before retry"
        );
        accept_fragment(temp.path(), "edge-a", &first).expect("replace repaired prefix");
        assert!(matches!(
            accept_fragment(temp.path(), "edge-a", &final_fragment).expect("finish repaired stage"),
            StageOutcome::Complete { .. }
        ));
    }

    #[test]
    fn durable_response_stage_authenticates_chunks_and_completion_receipts() {
        let temp = tempfile::tempdir().expect("response stage tempdir");
        let payload = vec![0x9a; RESPONSE_CHUNK_BYTES + 37];
        let request_digest = *blake3::hash(b"opaque sync request").as_bytes();
        let manifest = stage_large_response(
            temp.path(),
            "edge-a",
            "contextdb.sync.pull.tenant-a",
            request_digest,
            &payload,
        )
        .expect("stage response");

        let first =
            response_chunk(temp.path(), "edge-a", &manifest, 0).expect("read first response chunk");
        assert_eq!(first.bytes, payload[..RESPONSE_CHUNK_BYTES]);
        assert_eq!(first.sequence, 0);
        assert_eq!(first.total_chunks, manifest.total_chunks);
        assert!(
            response_chunk(temp.path(), "edge-b", &manifest, 0)
                .expect_err("different authenticated edge cannot read response")
                .to_string()
                .contains("different authenticated edge")
        );

        complete_large_response(temp.path(), "edge-a", &manifest)
            .expect("durably acknowledge completed response");
        assert!(
            !response_stage_path(temp.path(), &manifest).exists(),
            "completion removes staged reply bytes only after its receipt exists"
        );
        complete_large_response(temp.path(), "edge-a", &manifest)
            .expect("lost completion acknowledgement retries idempotently");
        release_large_response(temp.path(), "edge-a", &manifest)
            .expect("client release removes completion receipt");
        release_large_response(temp.path(), "edge-a", &manifest)
            .expect("lost release acknowledgement retries idempotently");

        let undelivered = stage_large_response(
            temp.path(),
            "edge-a",
            "contextdb.sync.pull.tenant-a",
            *blake3::hash(b"undelivered request").as_bytes(),
            &payload,
        )
        .expect("stage undelivered response");
        assert!(
            abandon_large_response(temp.path(), "edge-b", &undelivered).is_err(),
            "a different authenticated edge cannot abandon staged response bytes"
        );
        assert!(response_stage_path(temp.path(), &undelivered).exists());
        abandon_large_response(temp.path(), "edge-a", &undelivered)
            .expect("final undelivered manifest reference removes its stage");
        assert!(!response_stage_path(temp.path(), &undelivered).exists());
    }

    #[test]
    fn identical_response_publications_have_independent_transfer_identity_and_storage() {
        let temp = tempfile::tempdir().expect("response stage tempdir");
        let payload = vec![0x9c; RESPONSE_CHUNK_BYTES + 1];
        let request_digest = *blake3::hash(b"same request").as_bytes();
        let first = stage_large_response(
            temp.path(),
            "edge-a",
            "contextdb.sync.pull.tenant-a",
            request_digest,
            &payload,
        )
        .expect("stage first response");
        let second = stage_large_response(
            temp.path(),
            "edge-a",
            "contextdb.sync.pull.tenant-a",
            request_digest,
            &payload,
        )
        .expect("stage second response");

        assert_eq!(first.response_digest, second.response_digest);
        assert_ne!(first.publication_nonce, second.publication_nonce);
        assert_ne!(
            first.transfer_digest().expect("first transfer digest"),
            second.transfer_digest().expect("second transfer digest"),
            "identical reply publications remain distinct transfers"
        );
        assert_ne!(
            response_stage_path(temp.path(), &first),
            response_stage_path(temp.path(), &second),
            "each publication owns a distinct durable stage"
        );
        assert_ne!(
            response_completion_path(temp.path(), &first),
            response_completion_path(temp.path(), &second),
            "each publication owns a distinct completion receipt"
        );
        complete_large_response(temp.path(), "edge-a", &first)
            .expect("first publication completes independently");
        assert!(response_stage_path(temp.path(), &second).exists());
        response_chunk(temp.path(), "edge-a", &second, 0)
            .expect("second publication remains servable after the first completes");
        release_large_response(temp.path(), "edge-a", &first)
            .expect("first publication releases independently");
        assert!(response_stage_path(temp.path(), &second).exists());
        complete_large_response(temp.path(), "edge-a", &second)
            .expect("second publication completes independently");
        assert!(response_completion_path(temp.path(), &second).exists());
    }

    #[cfg(unix)]
    #[test]
    fn response_control_reads_refuse_post_stage_intermediate_and_leaf_symlinks() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("stage tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let sentinel = outside.path().join("must-survive");
        fs::write(&sentinel, b"outside").expect("write outside sentinel");
        let payload = vec![0x9b; RESPONSE_CHUNK_BYTES + 3];
        let manifest = stage_large_response(
            temp.path(),
            "edge-a",
            "sync.pull",
            *blake3::hash(b"request").as_bytes(),
            &payload,
        )
        .expect("stage response");
        let stage = response_stage_path(temp.path(), &manifest);
        let node = stage
            .parent()
            .and_then(Path::parent)
            .expect("response node")
            .to_path_buf();
        let moved = temp.path().join("edge-a-response-real");
        fs::rename(&node, &moved).expect("move staged response node");
        symlink(outside.path(), &node).expect("swap response node for link");
        let error = response_chunk(temp.path(), "edge-a", &manifest, 0)
            .expect_err("chunk control must reject swapped intermediate");
        assert!(error.to_string().contains("symlink"));
        assert_eq!(fs::read(&sentinel).expect("read sentinel"), b"outside");

        fs::remove_file(&node).expect("remove response link");
        fs::rename(&moved, &node).expect("restore response node");
        let manifest_leaf = stage.join("manifest.msgpack");
        fs::remove_file(&manifest_leaf).expect("remove manifest leaf");
        symlink(&sentinel, &manifest_leaf).expect("link manifest leaf");
        assert!(
            response_chunk(temp.path(), "edge-a", &manifest, 0)
                .expect_err("manifest leaf link must be refused")
                .to_string()
                .contains("symlink")
        );
        fs::remove_file(&manifest_leaf).expect("remove manifest link");
        let stage_dir = stage_dir(
            temp.path(),
            &stage,
            false,
            "durable oversized response stage",
        )
        .expect("open restored response stage");
        persist_response_manifest_at(&stage_dir, &manifest).expect("restore manifest");
        for leaf in [
            stage.join("0000000000000000.reply"),
            stage.join("0000000000000000.digest"),
        ] {
            fs::remove_file(&leaf).expect("remove chunk leaf");
            symlink(&sentinel, &leaf).expect("link chunk leaf");
            assert!(
                response_chunk(temp.path(), "edge-a", &manifest, 0)
                    .expect_err("chunk leaf link must be refused")
                    .to_string()
                    .contains("symlink")
            );
            fs::remove_file(&leaf).expect("remove chunk link");
            persist_response_chunk_at(&stage_dir, &manifest, 0, &payload[..RESPONSE_CHUNK_BYTES])
                .expect("restore chunk artifact");
        }

        complete_large_response(temp.path(), "edge-a", &manifest).expect("create completion");
        let receipt = response_completion_path(temp.path(), &manifest);
        let receipt_node = receipt.parent().expect("receipt node").to_path_buf();
        let receipt_moved = temp.path().join("edge-a-receipt-real");
        fs::rename(&receipt_node, &receipt_moved).expect("move receipt node");
        symlink(outside.path(), &receipt_node).expect("swap receipt node for link");
        assert!(
            release_large_response(temp.path(), "edge-a", &manifest)
                .expect_err("release must reject swapped receipt intermediate")
                .to_string()
                .contains("symlink")
        );
        assert_eq!(fs::read(&sentinel).expect("read sentinel"), b"outside");
        fs::remove_file(&receipt_node).expect("remove receipt link");
        fs::rename(&receipt_moved, &receipt_node).expect("restore receipt node");
        fs::remove_file(&receipt).expect("remove receipt leaf");
        symlink(&sentinel, &receipt).expect("link receipt leaf");
        assert!(
            release_large_response(temp.path(), "edge-a", &manifest)
                .expect_err("release must reject receipt leaf link")
                .to_string()
                .contains("symlink")
        );
        assert_eq!(fs::read(&sentinel).expect("read sentinel"), b"outside");
    }

    #[cfg(unix)]
    #[test]
    fn request_regular_file_read_refuses_a_final_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside");
        let sentinel = outside.path().join("must-survive");
        fs::write(&sentinel, b"outside").expect("write sentinel");
        let leaf = temp.path().join("request.part");
        symlink(&sentinel, &leaf).expect("link request leaf");
        let root = stage_root_dir_create(temp.path()).expect("open staging root");
        assert!(
            root.read_file("request.part", None, "request artifact")
                .is_err()
        );
        assert_eq!(fs::read(&sentinel).expect("read sentinel"), b"outside");
    }

    #[cfg(unix)]
    #[test]
    fn idempotent_response_complete_refuses_a_swapped_stage_before_cleanup() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("stage tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let sentinel = outside.path().join("must-survive");
        fs::write(&sentinel, b"outside").expect("write sentinel");
        let payload = vec![0x9c; RESPONSE_CHUNK_BYTES + 1];
        let manifest = stage_large_response(
            temp.path(),
            "edge-a",
            "sync.pull",
            *blake3::hash(b"request").as_bytes(),
            &payload,
        )
        .expect("stage response");
        complete_large_response(temp.path(), "edge-a", &manifest).expect("complete response");
        let stage = response_stage_path(temp.path(), &manifest);
        let node = stage
            .parent()
            .and_then(Path::parent)
            .expect("response node")
            .to_path_buf();
        let suffix = stage
            .strip_prefix(&node)
            .expect("stage suffix")
            .to_path_buf();
        fs::create_dir_all(outside.path().join(&suffix)).expect("create outside lookalike");
        let cleanup_sentinel = outside.path().join(&suffix).join("cleanup-must-survive");
        fs::write(&cleanup_sentinel, b"outside").expect("write cleanup sentinel");
        let moved = temp.path().join("edge-a-stage-real");
        fs::rename(&node, &moved).expect("move response node");
        symlink(outside.path(), &node).expect("swap stage node");
        assert!(
            complete_large_response(temp.path(), "edge-a", &manifest)
                .expect_err("idempotent completion must reject swapped stage")
                .to_string()
                .contains("symlink")
        );
        assert_eq!(fs::read(&sentinel).expect("read sentinel"), b"outside");
        assert_eq!(
            fs::read(&cleanup_sentinel).expect("read cleanup sentinel"),
            b"outside"
        );
    }

    #[cfg(unix)]
    #[test]
    fn completed_request_cleanup_refuses_a_swapped_intermediate() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("stage tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let sentinel = outside.path().join("must-survive");
        fs::write(&sentinel, b"outside").expect("write sentinel");
        let payload = vec![0x9d; FRAGMENT_BYTES + 1];
        let begin = LargeRequestBegin::new(
            "sync.push",
            *blake3::hash(&payload).as_bytes(),
            payload.len(),
        )
        .expect("build descriptor");
        begin_request(temp.path(), "edge-a", &begin).expect("begin request");
        for (sequence, bytes) in payload.chunks(FRAGMENT_BYTES).enumerate() {
            let fragment: LargeRequestFragment = rmp_serde::from_slice(
                &LargeRequestFragment::encode(
                    &begin.subject,
                    begin.unit_digest,
                    payload.len(),
                    sequence,
                    begin.total_fragments as usize,
                    bytes,
                )
                .expect("encode fragment"),
            )
            .expect("decode fragment");
            let _ = accept_descriptor_fragment(temp.path(), "edge-a", &begin, &fragment)
                .expect("persist fragment");
        }
        let stage = stage_path(temp.path(), &stage_manifest("edge-a", &begin));
        let node = stage
            .parent()
            .and_then(Path::parent)
            .expect("request node")
            .to_path_buf();
        let suffix = stage
            .strip_prefix(&node)
            .expect("stage suffix")
            .to_path_buf();
        fs::create_dir_all(outside.path().join(&suffix)).expect("create outside lookalike");
        let cleanup_sentinel = outside.path().join(&suffix).join("cleanup-must-survive");
        fs::write(&cleanup_sentinel, b"outside").expect("write cleanup sentinel");
        let moved = temp.path().join("edge-a-request-real");
        fs::rename(&node, &moved).expect("move request node");
        symlink(outside.path(), &node).expect("swap request node");
        assert!(
            remove_completed_stage(temp.path(), &stage)
                .expect_err("cleanup must reject swapped request stage")
                .to_string()
                .contains("symlink")
        );
        assert_eq!(fs::read(&sentinel).expect("read sentinel"), b"outside");
        assert_eq!(
            fs::read(&cleanup_sentinel).expect("read cleanup sentinel"),
            b"outside"
        );
    }

    #[test]
    fn durable_response_chunk_refuses_corruption_and_wrong_length() {
        let temp = tempfile::tempdir().expect("response stage tempdir");
        let payload = vec![0x61; RESPONSE_CHUNK_BYTES + 3];
        let manifest = stage_large_response(
            temp.path(),
            "edge-a",
            "contextdb.sync.pull.tenant-a",
            *blake3::hash(b"opaque sync request").as_bytes(),
            &payload,
        )
        .expect("stage response");
        let first = response_stage_path(temp.path(), &manifest).join("0000000000000000.reply");
        fs::write(&first, [0u8; 9]).expect("corrupt staged chunk");
        let error = response_chunk(temp.path(), "edge-a", &manifest, 0)
            .expect_err("short staged response chunk must be refused");
        assert!(error.to_string().contains("length does not match"));
    }

    #[test]
    fn configured_response_pressure_refuses_to_evict_active_stages() {
        let temp = tempfile::tempdir().expect("response stage tempdir");
        let payload = vec![0x35; RESPONSE_CHUNK_BYTES + 1];
        let manifest = stage_large_response(
            temp.path(),
            "edge-a",
            "contextdb.sync.pull.tenant-a",
            *blake3::hash(b"opaque sync request").as_bytes(),
            &payload,
        )
        .expect("stage response");
        let stage = response_stage_path(temp.path(), &manifest);
        enforce_response_stage_budget(temp.path(), 1, std::slice::from_ref(&stage))
            .expect_err("active response stage makes pressure fail loudly");
        assert!(stage.exists());
        enforce_response_stage_budget(temp.path(), 1, &[])
            .expect("inactive response stage is evicted under configured pressure");
        assert!(!stage.exists());
    }

    #[test]
    fn exact_fit_response_budget_reserves_completion_receipt_headroom() {
        let temp = tempfile::tempdir().expect("response stage tempdir");
        let payload = vec![0x4c; RESPONSE_CHUNK_BYTES + 1];
        let request_digest = *blake3::hash(b"opaque sync request").as_bytes();
        let subject = "contextdb.sync.pull.tenant-a";
        let publication_nonce = [0x2a; 32];
        let manifest = LargeResponseManifest {
            version: RESPONSE_MANIFEST_VERSION,
            publication_nonce,
            subject: subject.to_string(),
            authenticated_node_id: "edge-a".to_string(),
            request_digest,
            response_digest: *blake3::hash(&payload).as_bytes(),
            total_bytes: payload.len() as u64,
            total_chunks: payload.len().div_ceil(RESPONSE_CHUNK_BYTES) as u64,
        };
        let manifest_bytes = manifest.encode().expect("encode manifest").len() as u64;
        let exact_budget = manifest_bytes
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(payload.len() as u64))
            .and_then(|bytes| bytes.checked_add(manifest.total_chunks * blake3::OUT_LEN as u64))
            .expect("exact response budget");

        let staged = stage_large_response_with_nonce_and_budget(
            temp.path(),
            "edge-a",
            subject,
            request_digest,
            &payload,
            publication_nonce,
            ResponseStageAdmission {
                budget: Some(exact_budget),
                protected: &[],
            },
        )
        .expect("exact budget must admit response and completion headroom");
        assert_eq!(staged, manifest);
        let stage_path = response_stage_path(temp.path(), &staged);
        let root_dir = stage_root_dir_existing(temp.path()).expect("open stage root");
        let (_, _, stage_dir) = open_tree_unit_from_root(
            &root_dir,
            temp.path(),
            &stage_path,
            "oversized response stage",
        )
        .expect("open response stage");
        assert_eq!(
            stage_dir
                .directory_bytes("oversized response stage")
                .expect("count exact staged bytes"),
            exact_budget
        );

        complete_large_response(temp.path(), "edge-a", &staged)
            .expect("completion must fit the already-reserved budget");
        assert!(!stage_path.exists());
        assert_eq!(
            fs::read(response_completion_path(temp.path(), &staged))
                .expect("read durable completion receipt"),
            staged.encode().expect("encode expected receipt")
        );
        assert!(
            stage_root_dir_existing(temp.path())
                .expect("reopen stage root")
                .directory_bytes("oversized response durable state")
                .expect("count completed durable bytes")
                <= exact_budget
        );
    }

    #[test]
    fn atomic_completion_move_keeps_reserved_capacity_across_crash_retry() {
        let temp = tempfile::tempdir().expect("response stage tempdir");
        let payload = vec![0x4d; RESPONSE_CHUNK_BYTES + 1];
        let request_digest = *blake3::hash(b"opaque sync request").as_bytes();
        let subject = "contextdb.sync.pull.tenant-a";
        let manifest = stage_large_response_with_nonce(
            temp.path(),
            "edge-a",
            subject,
            request_digest,
            &payload,
            [0x31; 32],
        )
        .expect("stage first response");
        let encoded = manifest.encode().expect("encode manifest");
        let exact_budget = (encoded.len() as u64)
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(payload.len() as u64))
            .and_then(|bytes| bytes.checked_add(manifest.total_chunks * blake3::OUT_LEN as u64))
            .expect("exact response budget");
        let stage_path = response_stage_path(temp.path(), &manifest);
        let receipt_path = response_completion_path(temp.path(), &manifest);
        let protected = [stage_path.clone(), receipt_path.clone()];

        stage_large_response_with_nonce_and_budget(
            temp.path(),
            "edge-a",
            subject,
            request_digest,
            &payload,
            [0x32; 32],
            ResponseStageAdmission {
                budget: Some(exact_budget),
                protected: &protected,
            },
        )
        .expect_err("reserve-backed stage must consume the exact budget");

        let root_dir = stage_root_dir_existing(temp.path()).expect("open stage root");
        let (_, _, stage_dir) = open_tree_unit_from_root(
            &root_dir,
            temp.path(),
            &stage_path,
            "oversized response stage",
        )
        .expect("open response stage");
        assert_eq!(
            stage_dir
                .read_optional(
                    RESPONSE_COMPLETION_RESERVE_FILE,
                    Some(encoded.len()),
                    "oversized response completion headroom",
                )
                .expect("read completion reserve")
                .expect("completion reserve exists"),
            encoded
        );
        let (completion_parent, completion_leaf) = parent_and_leaf_from_root(
            &root_dir,
            temp.path(),
            &receipt_path,
            true,
            "oversized response completion",
        )
        .expect("open completion parent");
        stage_dir
            .move_regular_new(
                RESPONSE_COMPLETION_RESERVE_FILE,
                &completion_parent,
                &completion_leaf,
                "oversized response completion receipt",
            )
            .expect("atomically publish reserved receipt");

        assert_eq!(
            stage_root_dir_existing(temp.path())
                .expect("reopen stage root")
                .directory_bytes("oversized response durable state")
                .expect("count crash-visible durable bytes"),
            exact_budget
        );
        // A power loss after the destination directory is durable but before
        // the source directory is durable may recover both names for the same
        // inode. Recreate that conservative crash-visible state: it must not
        // expose capacity, and receipt-backed retry must remove the stage.
        fs::hard_link(
            &receipt_path,
            stage_path.join(RESPONSE_COMPLETION_RESERVE_FILE),
        )
        .expect("recreate both-name crash state");
        stage_large_response_with_nonce_and_budget(
            temp.path(),
            "edge-a",
            subject,
            request_digest,
            &payload,
            [0x33; 32],
            ResponseStageAdmission {
                budget: Some(exact_budget),
                protected: &protected,
            },
        )
        .expect_err("receipt-backed crash state must not expose reserved capacity");

        complete_large_response(temp.path(), "edge-a", &manifest)
            .expect("retry must finish from the durable receipt");
        assert!(!stage_path.exists());
        assert_eq!(
            fs::read(receipt_path).expect("read retained receipt"),
            encoded
        );
    }

    #[cfg(unix)]
    #[test]
    fn configured_response_pressure_refuses_a_symlinked_staging_root() {
        use std::os::unix::fs::symlink;
        let temp = tempfile::tempdir().expect("stage tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let sentinel = outside.path().join("must-survive");
        fs::write(&sentinel, b"outside").expect("write outside sentinel");
        let stage_root = temp.path().join("stage-root");
        symlink(outside.path(), &stage_root).expect("link staging root");
        let error = enforce_response_stage_budget(&stage_root, 1, &[])
            .expect_err("symlinked staging root must be refused");
        assert!(error.to_string().contains("symlink"));
        assert!(sentinel.exists());
    }

    #[cfg(unix)]
    #[test]
    fn configured_response_pressure_refuses_intermediate_and_receipt_symlinks() {
        use std::os::unix::fs::symlink;
        for (root_name, leaf) in [("responses", false), ("response-completions", true)] {
            let temp = tempfile::tempdir().expect("stage tempdir");
            let outside = tempfile::tempdir().expect("outside tempdir");
            let sentinel = outside.path().join("must-survive");
            fs::write(&sentinel, b"outside").expect("write outside sentinel");
            let root = temp.path().join(root_name);
            let node = root.join("edge-a");
            fs::create_dir_all(&node).expect("create private parent");
            if leaf {
                symlink(outside.path(), node.join("receipt.msgpack")).expect("link receipt leaf");
            } else {
                symlink(outside.path(), &node)
                    .expect_err("existing intermediate cannot be replaced");
                fs::remove_dir(&node).expect("remove empty intermediate");
                symlink(outside.path(), &node).expect("link intermediate");
            }
            assert!(enforce_response_stage_budget(temp.path(), 1, &[]).is_err());
            assert!(sentinel.exists());
        }
    }
}
