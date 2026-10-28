//! A generation's immutable measured qualification, authenticated by its scope root.
use super::*;
use super::{control::Family, io::NativeIo, read::PageSource};
use contextdb_vector::ivf::storage::ReadPages;
#[cfg(feature = "test-seams")]
use contextdb_vector::ivf::storage::{PageStore, WritePages};
use serde::{Deserialize, Serialize};

/// Fixed wire envelope; no decoded record contains an allocating sequence.
pub(super) const RECORD_BYTES: usize = 256;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    pub epoch: u64,
    pub complete: bool,
    pub source_digest: [u8; 32],
    pub next_chunk: u64,
    pub generation: u64,
    pub policy_revision: u64,
    pub target_hundredths: u16,
    pub query_source: bool,
    pub source_partitioned: bool,
    pub source_identity: u64,
    pub source_frontier: u64,
    pub samples: u32,
    pub planned_samples: u32,
    pub selection_samples: u32,
    pub validation_samples: u32,
    pub depths: u32,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Depth {
    pub depth: u32,
    pub probes: u32,
    pub factor_hundredths: u32,
    pub shortlist: u64,
    /// Actual per-sample re-ranks, separate from the nominal shortlist.
    pub reranks: Option<RerankSummary>,
    pub samples: u32,
    pub queries_below: u32,
    pub qualified: bool,
    /// Sums over the same samples; compare sums rather than rounded means.
    pub indexed_work: u64,
    pub exact_work: u64,
}
/// An exact integer distribution; inspection derives its mean from total/samples.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RerankSummary {
    pub minimum: u64,
    pub maximum: u64,
    pub total: u64,
    pub samples: u32,
}
impl RerankSummary {
    pub fn first(count: u64) -> Self {
        Self {
            minimum: count,
            maximum: count,
            total: count,
            samples: 1,
        }
    }
    pub fn include(&mut self, count: u64) -> Result<()> {
        let total = self.total.checked_add(count).ok_or_else(corrupt)?;
        let samples = self.samples.checked_add(1).ok_or_else(corrupt)?;
        *self = Self {
            minimum: self.minimum.min(count),
            maximum: self.maximum.max(count),
            total,
            samples,
        };
        Ok(())
    }
    pub fn mean(self) -> f64 {
        self.total as f64 / f64::from(self.samples)
    }
    fn valid(self, samples: u32) -> bool {
        self.samples != 0
            && self.samples == samples
            && self.minimum <= self.maximum
            && u128::from(self.minimum) * u128::from(samples) <= u128::from(self.total)
            && u128::from(self.total) <= u128::from(self.maximum) * u128::from(samples)
    }
}
/// A pinned identity, never a copied canonical vector.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sample {
    pub row: u64,
    pub generation: u64,
    pub slot: u64,
    pub row_created_tx: u64,
    pub row_lsn: u64,
    pub vector_created_tx: u64,
    pub vector_lsn: u64,
    pub visible_from: u64,
    pub rank: u64,
}
/// Fixed-size audit chunks are streamed under the native root; no answer map is decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowChunk {
    pub count: u32,
    pub rows: [u64; 16],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rewrite {
    pub depth_ordinal: u32,
    pub sample: u32,
    pub first_chunk: u64,
    pub answer_length: u32,
}
pub(super) fn paired_key(kind: u8, left: u32, right: u32) -> [u8; 9] {
    key(kind, (u64::from(left) << 32) | u64::from(right))
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Oracle {
    pub count: u32,
    pub work: u64,
}
/// Every measured query's own work remains independently inspectable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SampleWork {
    pub depth_ordinal: u32,
    pub sample: u32,
    pub reranked_candidates: u64,
    pub indexed_work: u64,
    pub exact_work: u64,
}
pub(super) fn sample_work_key(depth_ordinal: u32, sample: u32) -> [u8; 9] {
    key(7, (u64::from(depth_ordinal) << 32) | u64::from(sample))
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Prefix {
    pub depth: u32,
    pub k: u32,
    pub minimum_hits: u32,
    pub samples: u32,
    pub queries_below: u32,
}
impl Prefix {
    pub fn meets(self, target: u16) -> bool {
        self.samples != 0
            && u64::from(self.minimum_hits) * 10_000 >= u64::from(self.k) * u64::from(target)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Record {
    Header(Header),
    Depth(Depth),
    Prefix(Prefix),
    SampleWork(SampleWork),
    Sample(Sample),
    Rows(RowChunk),
    Rewrite(Rewrite),
    Oracle(Oracle),
}
pub(crate) struct Selection {
    pub depth: Depth,
    pub covered: bool,
    _header: Header,
    _prefix: Prefix,
    // The whole decoded control stays paid while the query uses its policy.
    _invoice: super::memory::Invoice,
}
/// Physical names derive from the existing never-reused native storage identity.
pub(super) struct TableName([u8; 22]);
impl TableName {
    pub fn new(storage: u64) -> Result<Self> {
        if storage == 0 {
            return Err(corrupt());
        }
        let mut bytes = [0; 22];
        bytes[..4].copy_from_slice(b"ivf_");
        bytes[20..].copy_from_slice(b"_q");
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for (at, digit) in bytes[4..20].iter_mut().enumerate() {
            *digit = HEX[((storage >> (60 - at * 4)) & 15) as usize];
        }
        Ok(Self(bytes))
    }
    pub fn as_str(&self) -> Result<&str> {
        std::str::from_utf8(&self.0).map_err(|_| corrupt())
    }
}
pub(super) fn key(kind: u8, ordinal: u64) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = kind;
    key[1..].copy_from_slice(&ordinal.to_be_bytes());
    key
}
pub(super) fn encode(
    scope: &[u8; 64],
    generation: u64,
    key: &[u8; 9],
    record: Record,
    bytes: &mut [u8; RECORD_BYTES],
) -> Result<()> {
    bytes.fill(0);
    bytes[..8].copy_from_slice(b"CIVFQL01");
    let n = bincode::serde::encode_into_slice(
        record,
        &mut bytes[12..RECORD_BYTES - 32],
        bincode::config::standard().with_fixed_int_encoding(),
    )
    .map_err(|_| corrupt())?;
    bytes[8..12].copy_from_slice(&u32::try_from(n).map_err(|_| corrupt())?.to_le_bytes());
    let mut h = blake3::Hasher::new();
    h.update(scope);
    h.update(&generation.to_le_bytes());
    h.update(key);
    h.update(&bytes[..RECORD_BYTES - 32]);
    bytes[RECORD_BYTES - 32..].copy_from_slice(h.finalize().as_bytes());
    Ok(())
}
fn decode(scope: &[u8; 64], generation: u64, key: &[u8; 9], bytes: &[u8]) -> Result<Record> {
    if bytes.len() != RECORD_BYTES || &bytes[..8] != b"CIVFQL01" {
        return Err(corrupt());
    }
    let mut h = blake3::Hasher::new();
    h.update(scope);
    h.update(&generation.to_le_bytes());
    h.update(key);
    h.update(&bytes[..RECORD_BYTES - 32]);
    if bytes[RECORD_BYTES - 32..] != *h.finalize().as_bytes() {
        return Err(corrupt());
    }
    let n = u32::from_le_bytes(bytes[8..12].try_into().map_err(|_| corrupt())?) as usize;
    let end = 12usize
        .checked_add(n)
        .filter(|end| *end <= RECORD_BYTES - 32)
        .ok_or_else(corrupt)?;
    if bytes[end..RECORD_BYTES - 32].iter().any(|b| *b != 0) {
        return Err(corrupt());
    }
    let (record, consumed) = bincode::serde::decode_from_slice(
        &bytes[12..end],
        bincode::config::standard().with_fixed_int_encoding(),
    )
    .map_err(|_| corrupt())?;
    if consumed != n {
        return Err(corrupt());
    }
    Ok(record)
}
pub(super) fn read(
    native: &NativeIo<'_>,
    scope: &[u8; 64],
    family: Family,
    kind: u8,
    ordinal: u64,
) -> Result<Option<Record>> {
    let Some(root) = family.qualification_root else {
        return Ok(None);
    };
    let name = TableName::new(family.storage)?;
    if native.fingerprint(name.as_str()?)? != Some(root) {
        return Err(corrupt());
    }
    let key = key(kind, ordinal);
    native.with_existing_value(name.as_str()?, &key, |value| {
        value
            .map(|bytes| decode(scope, family.generation, &key, bytes))
            .transpose()
    })
}
impl PageSource<'_> {
    /// Candidate metadata comes from this authenticated family, independently
    /// of the ordinary serving manifest. Vectors stay on the original capture.
    pub(in crate::persistence::vector_ivf) fn calibration_record(
        &self,
        generation: u64,
        kind: u8,
        ordinal: u64,
    ) -> Result<Option<Record>> {
        self.check_cancel()?;
        let family = *self
            .record
            .borrow()
            .family(generation)
            .ok_or_else(corrupt)?;
        read(&self.native(), &self.scope_key, family, kind, ordinal)
    }
    pub(super) fn qualification_header(&self) -> Result<Option<Header>> {
        let Some(manifest) = self.manifest(0)? else {
            return Ok(None);
        };
        let family = *self
            .record
            .borrow()
            .family(manifest.generation)
            .ok_or_else(corrupt)?;
        let result = read(&self.native(), &self.scope_key, family, 0, 0)?;
        match result {
            None => Ok(None),
            Some(Record::Header(header))
                if header.complete
                    && header.generation == manifest.generation
                    && header.target_hundredths > 0
                    && header.target_hundredths <= 10_000
                    && header
                        .selection_samples
                        .checked_add(header.validation_samples)
                        == Some(header.samples) =>
            {
                Ok(Some(header))
            }
            _ => Err(corrupt()),
        }
    }
    pub(super) fn qualification_depth(&self, ordinal: u32) -> Result<Depth> {
        self.check_cancel()?;
        let manifest = self.manifest(0)?.ok_or_else(corrupt)?;
        let family = *self
            .record
            .borrow()
            .family(manifest.generation)
            .ok_or_else(corrupt)?;
        match read(
            &self.native(),
            &self.scope_key,
            family,
            1,
            u64::from(ordinal),
        )? {
            Some(Record::Depth(depth))
                if depth.depth > 0
                    && depth.probes > 0
                    && depth.probes <= manifest.lists
                    && depth.factor_hundredths >= 100
                    && depth
                        .reranks
                        .is_none_or(|counts| counts.valid(depth.samples)) =>
            {
                Ok(depth)
            }
            _ => Err(corrupt()),
        }
    }
    pub(super) fn qualification_prefix(&self, k: u32) -> Result<Prefix> {
        self.check_cancel()?;
        let manifest = self.manifest(0)?.ok_or_else(corrupt)?;
        let family = *self
            .record
            .borrow()
            .family(manifest.generation)
            .ok_or_else(corrupt)?;
        match read(&self.native(), &self.scope_key, family, 2, u64::from(k))? {
            Some(Record::Prefix(prefix)) if prefix.k == k && prefix.minimum_hits <= k => Ok(prefix),
            _ => Err(corrupt()),
        }
    }
    pub(super) fn qualification_sample_work(
        &self,
        depth_ordinal: u32,
        sample: u32,
    ) -> Result<SampleWork> {
        self.check_cancel()?;
        let manifest = self.manifest(0)?.ok_or_else(corrupt)?;
        let family = *self
            .record
            .borrow()
            .family(manifest.generation)
            .ok_or_else(corrupt)?;
        let key = sample_work_key(depth_ordinal, sample);
        let ordinal = u64::from_be_bytes(key[1..].try_into().map_err(|_| corrupt())?);
        match read(&self.native(), &self.scope_key, family, 7, ordinal)? {
            Some(Record::SampleWork(work))
                if work.depth_ordinal == depth_ordinal && work.sample == sample =>
            {
                Ok(work)
            }
            _ => Err(corrupt()),
        }
    }
    pub(crate) fn select_qualification(&self, k: usize, target: u16) -> Result<Option<Selection>> {
        let Some(manifest) = self.manifest(0)? else {
            return Ok(None);
        };
        if self
            .record
            .borrow()
            .family(manifest.generation)
            .ok_or_else(corrupt)?
            .qualification_root
            .is_none()
        {
            return Ok(None);
        }
        let invoice = super::memory::Invoice::reserve(
            self.authority.vector_owner(),
            std::mem::size_of::<Selection>(),
            "clustered_qualification_selection",
        )?;
        let Some(header) = self.qualification_header()? else {
            return Ok(None);
        };
        let mut previous = 0;
        for ordinal in 0..header.depths {
            let depth = self.qualification_depth(ordinal)?;
            if depth.depth <= previous {
                return Err(corrupt());
            }
            previous = depth.depth;
            if k <= depth.depth as usize {
                let prefix = self.qualification_prefix(u32::try_from(k).map_err(|_| corrupt())?)?;
                if prefix.depth != depth.depth {
                    return Err(corrupt());
                }
                return Ok(Some(Selection {
                    depth,
                    covered: prefix.meets(target) && depth.indexed_work < depth.exact_work,
                    _header: header,
                    _prefix: prefix,
                    _invoice: invoice,
                }));
            }
        }
        Ok(None)
    }
}

#[cfg(feature = "test-seams")]
impl RedbPersistence {
    /// The fixture supplies measured qualification facts in the real receipt format.
    pub(crate) fn plant_native_ivf_qualification_for_test(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        column: &contextdb_core::ColumnDef,
        depths: &[u32],
        indexed_mean_work: u64,
        exact_mean_work: u64,
    ) -> Result<()> {
        let _decode = self.writer_decode_scope()?;
        let projection = self.require_decode_original()?;
        let original = projection.preparation_original()?;
        let owner = projection.vector_budget()?;
        let admission =
            super::super::native_storage_admission::StorageAdmissionScope::writer_original(
                &original,
            )?;
        admission.run_original_unqualified(|| {
            let root = (self.paged_root_capture()?)()?;
            let (_, snapshot, frontier) = root.authenticate()?;
            self.with_db(|database| {
                let transaction = Self::begin_history_write(database)?;
                let frozen = NativeIo::CapturedTransaction {
                    read: &root.read,
                    admission: &admission,
                };
                let mut session = write::WriteSession::borrow(
                    &transaction,
                    &admission,
                    owner,
                    snapshot,
                    frontier,
                    frontier,
                )?;
                session.with_scope(index, tuple, &|| Ok(()), &|_| Ok(()), |scope| {
                    scope.prepare_existing_roots()?;
                    let manifest = scope.manifest(0)?.ok_or_else(corrupt)?;
                    let family = *scope
                        .record
                        .borrow()
                        .family(manifest.generation)
                        .ok_or_else(corrupt)?;
                    let mut header = match read(&frozen, &scope.key, family, 0, 0)? {
                        Some(Record::Header(header)) => header,
                        None => {
                            let samples = column
                                .vector_engine_policy
                                .calibration_samples
                                .unwrap_or(200)
                                .min(family.basis_live_entries);
                            let samples = u32::try_from(samples).map_err(|_| corrupt())?;
                            Header {
                                epoch: 0,
                                complete: true,
                                source_digest: [0; 32],
                                next_chunk: 0,
                                generation: manifest.generation,
                                policy_revision: family.policy_revision,
                                target_hundredths: column
                                    .search_target_accuracy_hundredths
                                    .unwrap_or(9500),
                                query_source: false,
                                source_partitioned: false,
                                source_identity: 0,
                                source_frontier: family.source_frontier,
                                samples,
                                planned_samples: samples,
                                selection_samples: samples / 2,
                                validation_samples: samples - samples / 2,
                                depths: 0,
                            }
                        }
                        _ => return Err(corrupt()),
                    };
                    // Supplied test facts state "as if measured". A cost-only
                    // receipt has no measured queries; preserve its actual basis
                    // and use its selected sample allowance for these supplied facts.
                    if header.samples == 0 {
                        header.samples = header.planned_samples;
                        header.selection_samples = header.samples / 2;
                        header.validation_samples = header.samples - header.selection_samples;
                    }
                    let old_depths = header.depths;
                    header.depths = u32::try_from(depths.len()).map_err(|_| corrupt())?;
                    let mut previous = 0;
                    for (ordinal, &depth) in depths.iter().enumerate() {
                        if depth <= previous {
                            return Err(corrupt());
                        }
                        let mut existing = None;
                        let mut existing_ordinal = None;
                        for old_ordinal in 0..old_depths {
                            if let Some(Record::Depth(d)) =
                                read(&frozen, &scope.key, family, 1, u64::from(old_ordinal))?
                                && d.depth == depth
                            {
                                existing = Some(d);
                                existing_ordinal = Some(old_ordinal);
                                break;
                            }
                        }
                        let factor = existing
                            .map(|d| d.factor_hundredths)
                            .or(column.ivf_refine_factor_hundredths)
                            .unwrap_or(100);
                        let shortlist = u64::from(depth)
                            .checked_mul(u64::from(factor))
                            .ok_or_else(corrupt)?
                            .div_ceil(100);
                        let row = Depth {
                            depth,
                            probes: existing
                                .map(|d| d.probes)
                                .or(column.ivf_probes)
                                .unwrap_or(manifest.lists),
                            factor_hundredths: factor,
                            shortlist,
                            reranks: existing.and_then(|d| d.reranks),
                            samples: header.samples,
                            queries_below: 0,
                            qualified: true,
                            indexed_work: indexed_mean_work
                                .checked_mul(u64::from(header.samples))
                                .ok_or_else(corrupt)?,
                            exact_work: exact_mean_work
                                .checked_mul(u64::from(header.samples))
                                .ok_or_else(corrupt)?,
                        };
                        if let Some(counts) = row.reranks {
                            for sample in 0..counts.samples {
                                let old_key =
                                    sample_work_key(existing_ordinal.ok_or_else(corrupt)?, sample);
                                let old_ordinal = u64::from_be_bytes(
                                    old_key[1..].try_into().map_err(|_| corrupt())?,
                                );
                                let Some(Record::SampleWork(mut work)) =
                                    read(&frozen, &scope.key, family, 7, old_ordinal)?
                                else {
                                    return Err(corrupt());
                                };
                                work.depth_ordinal =
                                    u32::try_from(ordinal).map_err(|_| corrupt())?;
                                let new_key = sample_work_key(work.depth_ordinal, sample);
                                let new_ordinal = u64::from_be_bytes(
                                    new_key[1..].try_into().map_err(|_| corrupt())?,
                                );
                                scope.write_qualification(
                                    manifest.generation,
                                    7,
                                    new_ordinal,
                                    Record::SampleWork(work),
                                )?;
                            }
                        }
                        scope.write_qualification(
                            manifest.generation,
                            1,
                            ordinal as u64,
                            Record::Depth(row),
                        )?;
                        for k in previous + 1..=depth {
                            scope.write_qualification(
                                manifest.generation,
                                2,
                                u64::from(k),
                                Record::Prefix(Prefix {
                                    depth,
                                    k,
                                    minimum_hits: k,
                                    samples: header.samples,
                                    queries_below: 0,
                                }),
                            )?;
                        }
                        previous = depth;
                    }
                    scope.write_qualification(manifest.generation, 0, 0, Record::Header(header))?;
                    // The ordinary operation seal publishes the changed family
                    // root in the authenticated scope control before outer commit.
                    scope.begin()?.commit()?;
                    Ok(())
                })?;
                session.bind_final()?;
                drop(session);
                Self::write_writer_recovery_bounds_in_write(&transaction, None)?;
                transaction.commit().map_err(Self::storage_error)?;
                Ok(())
            })
        })
    }
}

#[cfg(test)]
#[path = "qualification_distribution_tests.rs"]
mod distribution_tests;

#[path = "qualification_dependency.rs"]
mod dependency;
pub(super) use dependency::pending;
