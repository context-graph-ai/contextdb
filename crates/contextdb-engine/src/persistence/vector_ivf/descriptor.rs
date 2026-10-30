//! Compact serving metadata, authenticated by the same captured scope-table root.
use super::*;
use contextdb_vector::ivf::storage::Manifest;
pub(super) const BYTES: usize = 200;
pub(super) fn key(scope: &[u8; 64]) -> [u8; 65] {
    let mut key = [0; 65];
    key[..64].copy_from_slice(scope);
    key[64] = b'D';
    key
}
fn checksum(scope: &[u8; 64], bytes: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"clustered-serving-descriptor");
    h.update(scope);
    h.update(bytes);
    *h.finalize().as_bytes()
}
pub(super) fn encode(
    scope: &[u8; 64],
    record: &control::ScopeRecord,
    out: &mut [u8; BYTES],
) -> Result<()> {
    let manifest = record.manifest()?.ok_or_else(corrupt)?;
    let family = record.family(manifest.generation).ok_or_else(corrupt)?;
    out.fill(0);
    out[..8].copy_from_slice(b"CIVFSRV1");
    out[8..16].copy_from_slice(&manifest.partition.to_le_bytes());
    out[16..24].copy_from_slice(&manifest.generation.to_le_bytes());
    out[24..32].copy_from_slice(&(manifest.dimensions as u64).to_le_bytes());
    out[32..36].copy_from_slice(&manifest.lists.to_le_bytes());
    out[36..40].copy_from_slice(&manifest.active_lists.to_le_bytes());
    out[40..48].copy_from_slice(&manifest.slots.to_le_bytes());
    out[48..56].copy_from_slice(&manifest.sealed_slots.to_le_bytes());
    out[56..88].copy_from_slice(&manifest.codebook);
    out[88..96].copy_from_slice(&manifest.run.to_le_bytes());
    out[96..104].copy_from_slice(&family.source_frontier.to_le_bytes());
    out[104..112].copy_from_slice(&record.committed_through_lsn.to_le_bytes());
    out[112..120].copy_from_slice(&family.policy_revision.to_le_bytes());
    out[120..128].copy_from_slice(&record.schema.table_generation.to_le_bytes());
    out[128..136].copy_from_slice(&record.schema.column_incarnation.to_le_bytes());
    out[136..168].copy_from_slice(&record.tuple);
    let hash = checksum(scope, &out[..168]);
    out[168..].copy_from_slice(&hash);
    Ok(())
}
pub(super) fn decode(
    scope: &[u8; 64],
    record: &control::ScopeRecord,
    bytes: &[u8],
) -> Result<Manifest> {
    if bytes.len() != BYTES
        || &bytes[..8] != b"CIVFSRV1"
        || bytes[168..] != checksum(scope, &bytes[..168])
    {
        return Err(corrupt());
    }
    fn word(bytes: &[u8], offset: usize) -> Result<u64> {
        Ok(u64::from_le_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .map_err(|_| corrupt())?,
        ))
    }
    let manifest = Manifest {
        partition: word(bytes, 8)?,
        generation: word(bytes, 16)?,
        dimensions: usize::try_from(word(bytes, 24)?).map_err(|_| corrupt())?,
        lists: u32::from_le_bytes(bytes[32..36].try_into().map_err(|_| corrupt())?),
        active_lists: u32::from_le_bytes(bytes[36..40].try_into().map_err(|_| corrupt())?),
        slots: word(bytes, 40)?,
        sealed_slots: word(bytes, 48)?,
        codebook: bytes[56..88].try_into().map_err(|_| corrupt())?,
        run: word(bytes, 88)?,
    };
    let family = record.family(manifest.generation).ok_or_else(corrupt)?;
    if Some(manifest) != record.manifest()?
        || word(bytes, 96)? != family.source_frontier
        || word(bytes, 104)? != record.committed_through_lsn
        || word(bytes, 112)? != family.policy_revision
        || word(bytes, 120)? != record.schema.table_generation
        || word(bytes, 128)? != record.schema.column_incarnation
        || bytes[136..168] != record.tuple
    {
        return Err(corrupt());
    }
    Ok(manifest)
}
