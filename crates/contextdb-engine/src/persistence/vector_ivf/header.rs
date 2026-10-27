//! The accepted 192-byte role header around a 3,904-byte kernel payload.
//! The native factory authenticates this role's captured expected root first.
use super::control::Family;
use super::semantic::Records;
use super::*;
use contextdb_vector::ivf::storage::{PageKey, PageKind};

pub(super) const HEADER_BYTES: usize = 192;
pub(super) const VALUE_BYTES: usize = HEADER_BYTES + PAGE_BYTES;
const MAGIC: [u8; 4] = *b"CIVF";

pub(super) fn kind_tag(kind: PageKind) -> u8 {
    match kind {
        PageKind::Canonical => 1,
        PageKind::Lists => 2,
        PageKind::Directory => 3,
        PageKind::DirectoryPresence => 4,
        PageKind::Visibility => 5,
        PageKind::Codebook => 6,
        PageKind::Accumulators => 7,
        PageKind::Reservoir => 8,
        PageKind::Catalog => 9,
        PageKind::SlotMetadata => 10,
        PageKind::Tail => 11,
        PageKind::ListLinks => 12,
        PageKind::ListAllocator => 13,
        PageKind::ListUsed => 14,
        PageKind::RowVersions => 15,
        PageKind::RowCopies => 16,
    }
}
pub(super) fn kind_from_tag(tag: u8) -> Result<PageKind> {
    Ok(match tag {
        1 => PageKind::Canonical,
        2 => PageKind::Lists,
        3 => PageKind::Directory,
        4 => PageKind::DirectoryPresence,
        5 => PageKind::Visibility,
        6 => PageKind::Codebook,
        7 => PageKind::Accumulators,
        8 => PageKind::Reservoir,
        9 => PageKind::Catalog,
        10 => PageKind::SlotMetadata,
        11 => PageKind::Tail,
        12 => PageKind::ListLinks,
        13 => PageKind::ListAllocator,
        14 => PageKind::ListUsed,
        15 => PageKind::RowVersions,
        16 => PageKind::RowCopies,
        _ => return Err(corrupt()),
    })
}

pub(super) const KEY_BYTES: usize = 37;
pub(super) fn key_bytes(key: PageKey) -> [u8; KEY_BYTES] {
    let mut out = [0; KEY_BYTES];
    out[..8].copy_from_slice(&key.partition.to_be_bytes());
    out[8..16].copy_from_slice(&key.generation.to_be_bytes());
    out[16] = kind_tag(key.kind);
    out[17..21].copy_from_slice(&key.list.to_be_bytes());
    out[21..29].copy_from_slice(&key.ordinal.to_be_bytes());
    out[29..37].copy_from_slice(&key.run.to_be_bytes());
    out
}
pub(super) fn key_from_bytes(bytes: &[u8]) -> Result<PageKey> {
    if bytes.len() != KEY_BYTES {
        return Err(corrupt());
    }
    Ok(PageKey {
        partition: u64::from_be_bytes(bytes[..8].try_into().map_err(|_| corrupt())?),
        generation: u64::from_be_bytes(bytes[8..16].try_into().map_err(|_| corrupt())?),
        kind: kind_from_tag(bytes[16])?,
        list: u32::from_be_bytes(bytes[17..21].try_into().map_err(|_| corrupt())?),
        ordinal: u64::from_be_bytes(bytes[21..29].try_into().map_err(|_| corrupt())?),
        run: u64::from_be_bytes(bytes[29..37].try_into().map_err(|_| corrupt())?),
    })
}

pub(super) struct HeaderIdentity<'a> {
    pub index: &'a [u8; 32],
    pub tuple: &'a [u8; 32],
    pub family: &'a Family,
}
impl HeaderIdentity<'_> {
    fn validate(&self, key: PageKey) -> Result<()> {
        if key.partition != 0 || key.generation != self.family.generation {
            return Err(corrupt());
        }
        if matches!(key.kind, PageKind::RowVersions | PageKind::RowCopies) && key.generation != 0 {
            return Err(corrupt());
        }
        if !matches!(
            key.kind,
            PageKind::Directory | PageKind::DirectoryPresence | PageKind::RowCopies
        ) && key.run != 0
        {
            return Err(corrupt());
        }
        Ok(())
    }
}

fn write_header(
    identity: HeaderIdentity<'_>,
    key: PageKey,
    records: Records,
    digest: &[u8; 32],
    out: &mut [u8; HEADER_BYTES],
) -> Result<()> {
    identity.validate(key)?;
    out.fill(0);
    out[..4].copy_from_slice(&MAGIC);
    out[4..6].copy_from_slice(&1u16.to_le_bytes());
    out[6] = kind_tag(key.kind);
    out[8..40].copy_from_slice(identity.index);
    out[40..72].copy_from_slice(identity.tuple);
    out[72..80].copy_from_slice(&key.generation.to_le_bytes());
    out[80..88].copy_from_slice(&identity.family.policy_revision.to_le_bytes());
    out[88..96].copy_from_slice(&identity.family.source_frontier.to_le_bytes());
    out[96..104].copy_from_slice(&key.ordinal.to_le_bytes());
    out[104..112].copy_from_slice(&records.first.to_le_bytes());
    out[112..116].copy_from_slice(&records.count.to_le_bytes());
    out[116..120].copy_from_slice(&(PAGE_BYTES as u32).to_le_bytes());
    out[120..152].copy_from_slice(digest);
    out[152..184].copy_from_slice(&identity.family.source_identity);
    Ok(())
}
pub(super) fn encode(
    identity: HeaderIdentity<'_>,
    key: PageKey,
    records: Records,
    payload: &[u8; PAGE_BYTES],
    out: &mut [u8; VALUE_BYTES],
) -> Result<()> {
    let digest = *blake3::hash(payload).as_bytes();
    write_header(
        identity,
        key,
        records,
        &digest,
        (&mut out[..HEADER_BYTES])
            .try_into()
            .map_err(|_| corrupt())?,
    )?;
    out[HEADER_BYTES..].copy_from_slice(payload);
    Ok(())
}
pub(super) fn seal_in_place(
    identity: HeaderIdentity<'_>,
    key: PageKey,
    records: Records,
    value: &mut [u8; VALUE_BYTES],
) -> Result<()> {
    let digest = *blake3::hash(&value[HEADER_BYTES..]).as_bytes();
    write_header(
        identity,
        key,
        records,
        &digest,
        (&mut value[..HEADER_BYTES])
            .try_into()
            .map_err(|_| corrupt())?,
    )
}

/// This view cannot outlive the caller's admitted native value guard. No owned
/// payload, re-encoding, copied page or detached backing is created by opening it.
pub(super) struct AdmittedIvfPage<'a> {
    pub payload: &'a [u8; PAGE_BYTES],
    pub records: Records,
}
impl<'a> AdmittedIvfPage<'a> {
    /// The caller must have matched this kind's physical root against the
    /// captured SCOPES expectation under authenticated Configuration/schema.
    pub fn open(identity: HeaderIdentity<'_>, key: PageKey, value: &'a [u8]) -> Result<Self> {
        let (payload, records) = open_frame(identity, key, value)?;
        Ok(Self { payload, records })
    }
}

/// Validate a staged frame in the exclusive uncommitted write. This grants no
/// captured-root token and never makes provisional counts query-visible.
pub(super) fn check_staged(identity: HeaderIdentity<'_>, key: PageKey, value: &[u8]) -> Result<()> {
    open_frame(identity, key, value).map(|_| ())
}
fn open_frame<'a>(
    identity: HeaderIdentity<'_>,
    key: PageKey,
    value: &'a [u8],
) -> Result<(&'a [u8; PAGE_BYTES], Records)> {
    identity.validate(key)?;
    if value.len() != VALUE_BYTES
        || value[..4] != MAGIC
        || value[4..6] != 1u16.to_le_bytes()
        || value[6] != kind_tag(key.kind)
        || value[7] != 0
        || value[8..40] != *identity.index
        || value[40..72] != *identity.tuple
        || value[72..80] != key.generation.to_le_bytes()
        || value[80..88] != identity.family.policy_revision.to_le_bytes()
        || value[88..96] != identity.family.source_frontier.to_le_bytes()
        || value[96..104] != key.ordinal.to_le_bytes()
        || value[116..120] != (PAGE_BYTES as u32).to_le_bytes()
        || value[152..184] != identity.family.source_identity
        || value[184..192].iter().any(|b| *b != 0)
    {
        return Err(corrupt());
    }
    let payload: &[u8; PAGE_BYTES] = value[HEADER_BYTES..].try_into().map_err(|_| corrupt())?;
    if value[120..152] != *blake3::hash(payload).as_bytes() {
        return Err(corrupt());
    }
    let records = Records {
        first: u64::from_le_bytes(value[104..112].try_into().map_err(|_| corrupt())?),
        count: u32::from_le_bytes(value[112..116].try_into().map_err(|_| corrupt())?),
    };
    Ok((payload, records))
}
