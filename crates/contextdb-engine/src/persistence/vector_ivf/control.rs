//! Fixed, fully persisted controls for one declared partition tuple.
use super::*;
use contextdb_core::VectorPartitionComponent;
use contextdb_vector::ivf::TrainingPolicy;
use contextdb_vector::ivf::storage::{BuildPhase, BuildState, Manifest};
use serde::{Deserialize, Serialize};

/// Journal storage plus the two generation roles permitted by the switch protocol.
pub(super) const ROOTS: usize = 3;

// Fixed-int serde uses one byte for Option/bool, no tuple/array length, and
// the declared integer width. These maxima include every optional branch.
const MANIFEST_WIRE_BYTES: usize = 3 * 8 + 2 * 4 + 2 * 8 + 32 + 8;
const BUILD_WIRE_BYTES: usize =
    MANIFEST_WIRE_BYTES + 1 + 8 + 2 * 4 + 4 * 8 + 1 + MANIFEST_WIRE_BYTES + 8 + 1 + 2 * 8;
const FAMILY_WIRE_BYTES: usize =
    2 * 8 + 2 + 16 * 32 + 1 + 32 + 3 * 8 + 32 + 1 + MANIFEST_WIRE_BYTES + 8 + 1 + 8 + 2 * 8 + 1 + 8;
pub(super) const MAX_SCOPE_WIRE_BYTES: usize = 16
    + 2 * 32
    + 1
    + MANIFEST_WIRE_BYTES
    + 1
    + BUILD_WIRE_BYTES
    + ROOTS * (1 + FAMILY_WIRE_BYTES)
    + 1
    + 56
    + 32
    + 2 * 8
    + 32
    + 4 * 8
    + 3 * 8
    + 32
    + 2 * 8
    + 1
    + 2 * 8
    + 2 * 8
    + 1
    + 40;
const _: () = assert!(MAX_SCOPE_WIRE_BYTES == 2989);
const _: () = assert!(12 + MAX_SCOPE_WIRE_BYTES + 32 <= PAGE_BYTES);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(C)]
pub(super) struct KindRoots {
    pub present: u16,
    pub roots: [[u8; 32]; 16],
}
impl KindRoots {
    pub const fn empty() -> Self {
        Self {
            present: 0,
            roots: [[0; 32]; 16],
        }
    }
    pub fn validate(&self) -> Result<()> {
        for (slot, root) in self.roots.iter().enumerate() {
            if self.present & (1u16 << slot) == 0 && *root != [0; 32] {
                return Err(corrupt());
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Family {
    pub generation: u64,
    pub storage: u64,
    pub kinds: KindRoots,
    pub qualification_root: Option<[u8; 32]>,
    pub policy_revision: u64,
    pub source_frontier: u64,
    pub source_snapshot: u64,
    pub source_identity: [u8; 32],
    pub manifest: Option<ManifestRecord>,
    /// Current applied delta/run frontier; never the immutable header basis.
    pub committed_through_lsn: u64,
    pub reservoir_samples: Option<u64>,
    pub training_seed: Option<u64>,
    pub basis_entries: u64,
    pub basis_live_entries: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ManifestRecord {
    partition: u64,
    generation: u64,
    dimensions: u64,
    lists: u32,
    active_lists: u32,
    slots: u64,
    sealed_slots: u64,
    codebook: [u8; 32],
    run: u64,
}
impl From<Manifest> for ManifestRecord {
    fn from(m: Manifest) -> Self {
        Self {
            partition: m.partition,
            generation: m.generation,
            dimensions: m.dimensions as u64,
            lists: m.lists,
            active_lists: m.active_lists,
            slots: m.slots,
            sealed_slots: m.sealed_slots,
            codebook: m.codebook,
            run: m.run,
        }
    }
}
impl ManifestRecord {
    pub fn manifest(self) -> Result<Manifest> {
        let dimensions = usize::try_from(self.dimensions).map_err(|_| corrupt())?;
        if self.partition != 0
            || dimensions == 0
            || self.lists == 0
            || self.active_lists > self.lists
            || self.sealed_slots > self.slots
        {
            return Err(corrupt());
        }
        Ok(Manifest {
            partition: self.partition,
            generation: self.generation,
            dimensions,
            lists: self.lists,
            active_lists: self.active_lists,
            slots: self.slots,
            sealed_slots: self.sealed_slots,
            codebook: self.codebook,
            run: self.run,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BuildRecord {
    manifest: ManifestRecord,
    phase: u8,
    cursor: u64,
    iteration: u32,
    iterations: u32,
    reservoir_samples: u64,
    seed: u64,
    ingest_batch_keys: u64,
    phase_batch_entries: u64,
    serving_at_start: Option<ManifestRecord>,
    source_lsn: u64,
    publication_coalesce: Option<(u64, u64)>,
}
impl From<BuildState> for BuildRecord {
    fn from(s: BuildState) -> Self {
        Self {
            manifest: s.manifest.into(),
            phase: match s.phase {
                BuildPhase::Ingest => 0,
                BuildPhase::Seed => 1,
                BuildPhase::Train => 2,
                BuildPhase::Assign => 3,
                BuildPhase::Publish => 4,
                BuildPhase::Ready => 5,
            },
            cursor: s.cursor,
            iteration: s.iteration,
            iterations: s.training.iterations,
            reservoir_samples: s.training.reservoir_samples as u64,
            seed: s.training.seed,
            ingest_batch_keys: s.training.ingest_batch_keys as u64,
            phase_batch_entries: s.training.phase_batch_entries as u64,
            serving_at_start: s.serving_at_start.map(Into::into),
            source_lsn: s.source_lsn,
            publication_coalesce: s.publication_coalesce,
        }
    }
}
impl BuildRecord {
    pub fn state(self) -> Result<BuildState> {
        let phase = match self.phase {
            0 => BuildPhase::Ingest,
            1 => BuildPhase::Seed,
            2 => BuildPhase::Train,
            3 => BuildPhase::Assign,
            4 => BuildPhase::Publish,
            5 => BuildPhase::Ready,
            _ => return Err(corrupt()),
        };
        let training = TrainingPolicy {
            iterations: self.iterations,
            reservoir_samples: usize::try_from(self.reservoir_samples).map_err(|_| corrupt())?,
            seed: self.seed,
            ingest_batch_keys: usize::try_from(self.ingest_batch_keys).map_err(|_| corrupt())?,
            phase_batch_entries: usize::try_from(self.phase_batch_entries)
                .map_err(|_| corrupt())?,
        };
        if training.iterations == 0
            || training.reservoir_samples == 0
            || training.ingest_batch_keys == 0
            || training.phase_batch_entries == 0
        {
            return Err(corrupt());
        }
        Ok(BuildState {
            manifest: self.manifest.manifest()?,
            phase,
            cursor: self.cursor,
            iteration: self.iteration,
            training,
            serving_at_start: self
                .serving_at_start
                .map(ManifestRecord::manifest)
                .transpose()?,
            source_lsn: self.source_lsn,
            publication_coalesce: self.publication_coalesce,
        })
    }
}

/// No population directory, vector array or variable-length cursor is decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct SourceCursor {
    pub canonical: [u8; 32],
    pub membership: [u8; 24],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct SourceEnd {
    pub first: [u8; 32],
    pub last: [u8; 8],
}

/// Successful authoritative metadata changes applied to native deltas after
/// a held build frontier. Separate monotone counts avoid a signed range cap.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct AppliedSourceChanges {
    pub digest: [u8; 32],
    pub inserted: u64,
    pub removed: u64,
    pub live_inserted: u64,
    pub live_removed: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ScopeRecord {
    pub schema: VectorSchemaBinding,
    pub index: [u8; 32],
    pub tuple: [u8; 32],
    pub serving: Option<ManifestRecord>,
    pub build: Option<BuildRecord>,
    pub families: [Option<Family>; ROOTS],
    pub source_cursor: Option<SourceCursor>,
    pub source_end: Option<SourceEnd>,
    pub source_identity: [u8; 32],
    pub source_entries: u64,
    pub ingested_vectors: u64,
    pub build_changes: AppliedSourceChanges,
    pub policy_revision: u64,
    pub build_budget_bytes: u64,
    /// Current applied coverage is distinct from every family's frozen basis.
    pub committed_through_lsn: u64,
    pub current_membership_digest: [u8; 32],
    pub current_membership_entries: u64,
    pub current_membership_live_entries: u64,
    pub canonical_complete: bool,
    pub training_evaluations: u64,
    pub assignment_evaluations: u64,
    pub aborted_builds: u64,
    pub legacy_entries_prepared: u64,
}
impl ScopeRecord {
    pub fn empty(
        index: &VectorIndexRef,
        schema: VectorSchemaBinding,
        tuple: [u8; 32],
    ) -> Result<Self> {
        if schema == VectorSchemaBinding::UNBOUND {
            return Err(corrupt());
        }
        Ok(Self {
            schema,
            index: index_digest(index, schema),
            tuple,
            serving: None,
            build: None,
            families: [None; ROOTS],
            source_cursor: None,
            source_end: None,
            source_identity: [0; 32],
            source_entries: 0,
            ingested_vectors: 0,
            build_changes: AppliedSourceChanges::default(),
            policy_revision: 0,
            build_budget_bytes: 0,
            committed_through_lsn: 0,
            current_membership_digest: [0; 32],
            current_membership_entries: 0,
            current_membership_live_entries: 0,
            canonical_complete: false,
            training_evaluations: 0,
            assignment_evaluations: 0,
            aborted_builds: 0,
            legacy_entries_prepared: 0,
        })
    }
    pub fn family(&self, generation: u64) -> Option<&Family> {
        self.families
            .iter()
            .flatten()
            .find(|f| f.generation == generation)
    }
    pub fn manifest(&self) -> Result<Option<Manifest>> {
        self.serving.map(ManifestRecord::manifest).transpose()
    }
    pub fn build_state(&self) -> Result<Option<BuildState>> {
        self.build.map(BuildRecord::state).transpose()
    }
    pub fn validate(
        &self,
        index: &VectorIndexRef,
        schema: VectorSchemaBinding,
        tuple: [u8; 32],
    ) -> Result<()> {
        if self.schema != schema || self.index != index_digest(index, schema) || self.tuple != tuple
        {
            return Err(corrupt());
        }
        self.validate_controls()
    }
    /// The record's own constraints, for a caller that holds no schema binding.
    pub fn validate_controls(&self) -> Result<()> {
        if self.current_membership_live_entries > self.current_membership_entries {
            return Err(corrupt());
        }
        for (at, family) in self.families.iter().enumerate() {
            if let Some(f) = family {
                f.kinds.validate()?;
                if f.qualification_root == Some([0; 32]) {
                    return Err(corrupt());
                }
                if f.basis_live_entries > f.basis_entries
                    || f.source_frontier > f.committed_through_lsn
                {
                    return Err(corrupt());
                }
                if f.storage == 0
                    || self.families[..at]
                        .iter()
                        .flatten()
                        .any(|g| g.generation == f.generation || g.storage == f.storage)
                {
                    return Err(corrupt());
                }
                if let Some(m) = f.manifest {
                    if m.manifest()?.generation != f.generation {
                        return Err(corrupt());
                    }
                }
            }
        }
        let serving = self.manifest()?;
        let build = self.build_state()?;
        for m in serving.into_iter().chain(build.map(|b| b.manifest)) {
            if self.family(m.generation).is_none() {
                return Err(corrupt());
            }
        }
        if let Some(build) = build {
            if build.source_lsn
                < self
                    .family(build.manifest.generation)
                    .ok_or_else(corrupt)?
                    .source_frontier
                || (build.phase != BuildPhase::Ready
                    && build
                        .serving_at_start
                        .is_some_and(|m| self.family(m.generation).is_none()))
            {
                return Err(corrupt());
            }
        }
        Ok(())
    }

    /// The complete fixed record fits one logical control payload. No serde
    /// sequence in this record can allocate or use an input-provided length.
    pub fn encode(&self, key: &[u8; 64], out: &mut [u8; PAGE_BYTES]) -> Result<()> {
        out.fill(0);
        out[..8].copy_from_slice(b"CDBIVFS1");
        let end = PAGE_BYTES - 32;
        let n = bincode::serde::encode_into_slice(
            self,
            &mut out[12..end],
            bincode::config::standard().with_fixed_int_encoding(),
        )
        .map_err(|_| corrupt())?;
        if n > MAX_SCOPE_WIRE_BYTES {
            return Err(corrupt());
        }
        let n = u32::try_from(n).map_err(|_| corrupt())?;
        out[8..12].copy_from_slice(&n.to_le_bytes());
        let mut h = blake3::Hasher::new();
        h.update(b"contextdb.clustered.scope.control.v1");
        h.update(key);
        h.update(&out[..end]);
        out[end..].copy_from_slice(h.finalize().as_bytes());
        Ok(())
    }

    pub fn decode(
        index: &VectorIndexRef,
        key: &[u8; 64],
        input: &[u8],
        schema: VectorSchemaBinding,
        tuple: [u8; 32],
    ) -> Result<Self> {
        let record = Self::decode_envelope(key, input)?;
        record.validate(index, schema, tuple)?;
        Ok(record)
    }
    /// Size, magic, padding, exact length and the checksum bound to this key.
    /// Readers still go through decode, which adds the schema binding.
    pub fn decode_envelope(key: &[u8; 64], input: &[u8]) -> Result<Self> {
        if input.len() != PAGE_BYTES || &input[..8] != b"CDBIVFS1" {
            return Err(corrupt());
        }
        let end = PAGE_BYTES - 32;
        let mut h = blake3::Hasher::new();
        h.update(b"contextdb.clustered.scope.control.v1");
        h.update(key);
        h.update(&input[..end]);
        if input[end..] != *h.finalize().as_bytes() {
            return Err(corrupt());
        }
        let n = usize::try_from(u32::from_le_bytes(
            input[8..12].try_into().map_err(|_| corrupt())?,
        ))
        .map_err(|_| corrupt())?;
        let record_end = 12usize
            .checked_add(n)
            .filter(|record_end| n <= MAX_SCOPE_WIRE_BYTES && *record_end <= end)
            .ok_or_else(corrupt)?;
        if input[record_end..end].iter().any(|b| *b != 0) {
            return Err(corrupt());
        }
        let (record, consumed): (Self, usize) = bincode::serde::decode_from_slice(
            &input[12..record_end],
            bincode::config::standard().with_fixed_int_encoding(),
        )
        .map_err(|_| corrupt())?;
        if consumed != n {
            return Err(corrupt());
        }
        Ok(record)
    }
}

/// Retired inventory is paged under SCOPES, outside the three admitted active
/// controls. Physical reclamation still waits for state C and reader/holder
/// retention; removing an active descriptor does not delete its native tables.
pub(super) fn retired_key(scope: &[u8; 64], storage: u64) -> [u8; 73] {
    let mut key = [0; 73];
    key[..64].copy_from_slice(scope);
    key[64] = b'R';
    key[65..].copy_from_slice(&storage.to_be_bytes());
    key
}
pub(super) fn encode_retired_family(
    key: &[u8; 73],
    family: &Family,
    out: &mut [u8; PAGE_BYTES],
) -> Result<()> {
    family.kinds.validate()?;
    if family.storage == 0 || key[65..] != family.storage.to_be_bytes() {
        return Err(corrupt());
    }
    out.fill(0);
    out[..8].copy_from_slice(b"CDBIVFR1");
    let end = PAGE_BYTES - 32;
    let n = bincode::serde::encode_into_slice(
        family,
        &mut out[12..end],
        bincode::config::standard().with_fixed_int_encoding(),
    )
    .map_err(|_| corrupt())?;
    if n > FAMILY_WIRE_BYTES {
        return Err(corrupt());
    }
    out[8..12].copy_from_slice(&u32::try_from(n).map_err(|_| corrupt())?.to_le_bytes());
    let mut digest = blake3::Hasher::new();
    digest.update(b"contextdb.clustered.retired.family.v1");
    digest.update(key);
    digest.update(&out[..end]);
    out[end..].copy_from_slice(digest.finalize().as_bytes());
    Ok(())
}

pub(super) fn decode_retired_family(key: &[u8], bytes: &[u8]) -> Result<Family> {
    if key.len() != 73 || key[64] != b'R' || bytes.len() != PAGE_BYTES || &bytes[..8] != b"CDBIVFR1"
    {
        return Err(corrupt());
    }
    let end = PAGE_BYTES - 32;
    let mut hash = blake3::Hasher::new();
    hash.update(b"contextdb.clustered.retired.family.v1");
    hash.update(key);
    hash.update(&bytes[..end]);
    if bytes[end..] != *hash.finalize().as_bytes() {
        return Err(corrupt());
    }
    let length = u32::from_le_bytes(bytes[8..12].try_into().map_err(|_| corrupt())?) as usize;
    if length > FAMILY_WIRE_BYTES
        || 12 + length > end
        || bytes[12 + length..end].iter().any(|b| *b != 0)
    {
        return Err(corrupt());
    }
    let (family, consumed): (Family, usize) = bincode::serde::decode_from_slice(
        &bytes[12..12 + length],
        bincode::config::standard().with_fixed_int_encoding(),
    )
    .map_err(|_| corrupt())?;
    if consumed != length || key[65..] != family.storage.to_be_bytes() {
        return Err(corrupt());
    }
    family.kinds.validate()?;
    Ok(family)
}

pub(super) fn index_digest(index: &VectorIndexRef, schema: VectorSchemaBinding) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"contextdb.clustered.index.v1");
    h.update(&(index.table.len() as u64).to_le_bytes());
    h.update(index.table.as_bytes());
    h.update(&(index.column.len() as u64).to_le_bytes());
    h.update(index.column.as_bytes());
    h.update(&schema.table_generation.to_le_bytes());
    h.update(&schema.column_incarnation.to_le_bytes());
    *h.finalize().as_bytes()
}

/// Hash the existing canonical tuple encoding without allocating a copy.
pub(super) fn tuple_digest(tuple: &VectorPartitionKey) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(&[1]);
    h.update(&(tuple.components().len() as u64).to_be_bytes());
    for value in tuple.components() {
        match value {
            VectorPartitionComponent::Uuid(v) => {
                h.update(&[0]);
                h.update(v.as_bytes());
            }
            VectorPartitionComponent::Text(v) => {
                h.update(&[1]);
                h.update(&(v.len() as u64).to_be_bytes());
                h.update(v.as_bytes());
            }
            VectorPartitionComponent::Integer(v) => {
                h.update(&[2]);
                h.update(&v.to_be_bytes());
            }
            VectorPartitionComponent::Boolean(v) => {
                h.update(&[3, u8::from(*v)]);
            }
            VectorPartitionComponent::Timestamp(v) => {
                h.update(&[4]);
                h.update(&v.to_be_bytes());
            }
            VectorPartitionComponent::TxId(v) => {
                h.update(&[5]);
                h.update(&v.0.to_be_bytes());
            }
        }
    }
    *h.finalize().as_bytes()
}

pub(super) fn scope_key(index: [u8; 32], tuple: [u8; 32]) -> [u8; 64] {
    let mut key = [0; 64];
    key[..32].copy_from_slice(&index);
    key[32..].copy_from_slice(&tuple);
    key
}
