//! Per-run actual commit-frontier control, separate from immutable page basis.
//! These small records live under the shared SCOPES/Configuration authority.
use super::*;
use super::{control::Family, io::NativeIo};
use contextdb_vector::ivf::storage::{PageKey, PageKind};

pub(super) const KEY_BYTES: usize = 89;
const VALUE_BYTES: usize = 48;

fn run(key: PageKey) -> Option<u64> {
    match key.kind {
        PageKind::Directory | PageKind::DirectoryPresence => (key.run != 0).then_some(key.run),
        PageKind::Catalog => (key.list == 1 && key.ordinal != 0).then_some(key.ordinal),
        PageKind::Canonical
        | PageKind::Lists
        | PageKind::Visibility
        | PageKind::Codebook
        | PageKind::Accumulators
        | PageKind::Reservoir
        | PageKind::SlotMetadata
        | PageKind::Tail
        | PageKind::ListLinks
        | PageKind::ListAllocator
        | PageKind::ListUsed
        | PageKind::RowVersions
        | PageKind::RowCopies => None,
    }
}
fn encode_key(scope: &[u8; 64], family: &Family, run: u64, out: &mut [u8; KEY_BYTES]) {
    out[..64].copy_from_slice(scope);
    out[64] = b'D';
    out[65..73].copy_from_slice(&family.storage.to_be_bytes());
    out[73..81].copy_from_slice(&family.generation.to_be_bytes());
    out[81..89].copy_from_slice(&run.to_be_bytes());
}
fn digest(key: &[u8; KEY_BYTES], prefix: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"contextdb.clustered.run.commit.frontier.v1");
    h.update(key);
    h.update(prefix);
    *h.finalize().as_bytes()
}
fn decode(key: &[u8; KEY_BYTES], value: &[u8]) -> Result<u64> {
    if value.len() != VALUE_BYTES
        || &value[..8] != b"CDBIVFD1"
        || value[16..] != digest(key, &value[..16])
    {
        return Err(corrupt());
    }
    Ok(u64::from_le_bytes(
        value[8..16].try_into().map_err(|_| corrupt())?,
    ))
}

/// A prepared run can have directory/presence pages before its final run
/// descriptor. Every actual changed run role records the same actual outer
/// commit frontier; this is not a claim that a summary proves source coverage.
pub(super) fn stage(
    native: &NativeIo<'_>,
    scope: &[u8; 64],
    family: &Family,
    page: PageKey,
    commit_frontier: u64,
    key: &mut [u8; KEY_BYTES],
    scratch: &mut [u8; PAGE_BYTES],
) -> Result<()> {
    let Some(run) = run(page) else { return Ok(()) };
    if family.generation != page.generation || commit_frontier < family.source_frontier {
        return Err(corrupt());
    }
    encode_key(scope, family, run, key);
    let before = native.with_value(SCOPES.name(), key, |value| {
        value.map(|value| decode(key, value)).transpose()
    })?;
    if let Some(before) = before {
        if before > commit_frontier {
            return Err(corrupt());
        }
        if before == commit_frontier {
            return Ok(());
        }
    }
    scratch[..8].copy_from_slice(b"CDBIVFD1");
    scratch[8..16].copy_from_slice(&commit_frontier.to_le_bytes());
    let checksum = digest(key, &scratch[..16]);
    scratch[16..VALUE_BYTES].copy_from_slice(&checksum);
    native.put(SCOPES.name(), key, &scratch[..VALUE_BYTES])
}

/// Called only for an actual opened run payload. A missing future/sparse page
/// stays absent; an existing role without its commit-frontier record refuses.
pub(super) fn validate(
    native: &NativeIo<'_>,
    scope: &[u8; 64],
    family: &Family,
    page: PageKey,
    snapshot_lsn: u64,
) -> Result<()> {
    let Some(run) = run(page) else { return Ok(()) };
    let mut key = [0; KEY_BYTES];
    encode_key(scope, family, run, &mut key);
    native.with_value(SCOPES.name(), &key, |value| {
        let frontier = decode(&key, value.ok_or_else(corrupt)?)?;
        if frontier < family.source_frontier
            || frontier > family.committed_through_lsn
            || frontier > snapshot_lsn
        {
            return Err(corrupt());
        }
        Ok(())
    })
}
