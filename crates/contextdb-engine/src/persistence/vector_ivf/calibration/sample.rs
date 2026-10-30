//! A bounded native-source pass selects identities; query vectors are never retained.
use super::{
    super::*,
    capture::{self, Capture, Sample, State},
};
use crate::persistence::writer_source::{SourceWindowOwner, WriterCursorFamily};
use contextdb_core::{Lsn, RowId, TxId};
use contextdb_vector::ivf::storage::{Manifest, PageKind, ReadPages};
use std::{alloc::Layout, ops::Bound, sync::atomic::Ordering};

pub(super) fn check(
    capture: &Capture,
    index: &VectorIndexRef,
    checkpoint: &dyn Fn() -> Result<()>,
) -> Result<()> {
    checkpoint()?;
    if capture.cancelled.load(Ordering::Acquire) {
        return Err(Error::ReadCancelled);
    }
    capture::writer(&capture.root)?.check_table_scope(&index.table)
}
/// The immutable history root is authenticated at the query boundary.
/// Work checkpoints keep checking the live erasure entitlement and cancellation.
pub(super) fn work_checkpoint(
    capture: &Capture,
    index: &VectorIndexRef,
    checkpoint: &dyn Fn() -> Result<()>,
) -> Result<()> {
    checkpoint()?;
    if capture.cancelled.load(Ordering::Acquire) {
        return Err(Error::ReadCancelled);
    }
    capture::writer(&capture.root)?.check_table_entitlement(&index.table)
}
pub(super) fn read_bytes(
    source: &impl ReadPages,
    manifest: Manifest,
    kind: PageKind,
    offset: u64,
    out: &mut [u8],
    page: &mut [u8; PAGE_BYTES],
) -> Result<()> {
    let mut copied = 0;
    while copied < out.len() {
        source.check_cancel()?;
        let at = offset.checked_add(copied as u64).ok_or_else(corrupt)?;
        let ordinal = at / PAGE_BYTES as u64;
        if !source.read_admitted_page(manifest.key(kind, 0, ordinal), page)? {
            return Err(corrupt());
        }
        let start = (at % PAGE_BYTES as u64) as usize;
        let take = (out.len() - copied).min(PAGE_BYTES - start);
        out[copied..copied + take].copy_from_slice(&page[start..start + take]);
        copied += take;
    }
    Ok(())
}
fn word(
    source: &impl ReadPages,
    manifest: Manifest,
    kind: PageKind,
    offset: u64,
    page: &mut [u8; PAGE_BYTES],
) -> Result<u64> {
    let mut bytes = [0; 8];
    read_bytes(source, manifest, kind, offset, &mut bytes, page)?;
    Ok(u64::from_le_bytes(bytes))
}
struct Ranges {
    lower: Vec<u8>,
    upper: Vec<u8>,
    _invoice: memory::Invoice,
}
impl Ranges {
    fn new(
        capture: &Capture,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        after: Option<[u8; 40]>,
    ) -> Result<Self> {
        let bytes = memory::route_bytes(index, tuple)?
            .checked_add(40)
            .ok_or_else(corrupt)?;
        let layout = Layout::new::<Self>()
            .extend(
                Layout::array::<u8>(bytes.checked_mul(2).ok_or_else(corrupt)?)
                    .map_err(|_| corrupt())?,
            )
            .map_err(|_| corrupt())?
            .0
            .pad_to_align();
        let invoice = memory::Invoice::reserve(
            &capture.owner,
            layout.size(),
            "clustered_calibration_source_range",
        )?;
        let mut lower = memory::route_with_suffix_capacity(index, tuple, 40)?;
        let mut upper = memory::route_with_suffix_capacity(index, tuple, 40)?;
        if let Some(after) = after {
            lower.extend_from_slice(&after);
        }
        upper.extend_from_slice(&[255; 40]);
        Ok(Self {
            lower,
            upper,
            _invoice: invoice,
        })
    }
}
impl Capture {
    /// At most the declared source-key allowance; all reads use this immutable root.
    pub(super) fn sample_batch(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        state: &mut State,
        limit: usize,
        checkpoint: &dyn Fn() -> Result<()>,
    ) -> Result<bool> {
        check(self, index, checkpoint)?;
        if let Some(source) = &self.source {
            self.sample_questions(tuple, state, limit, checkpoint, source)
        } else {
            let authority = read::NativeAuthority::open_prepared(
                &self.root,
                self.memory.clone(),
                &self.verification,
            )?;
            let selected = || check(self, index, checkpoint);
            let page_source = read::PageSource::open_prepared(
                &authority,
                index,
                tuple,
                &selected,
                &|_| Ok(()),
                &self.verification,
            )?;
            if page_source.select_build_authority_with_verification(&self.verification)?
                != self.manifest
            {
                return Err(corrupt());
            }
            let _invoice = memory::Invoice::reserve(
                &self.owner,
                PAGE_BYTES,
                "clustered_calibration_sample_page",
            )?;
            let mut page = [0; PAGE_BYTES];
            let mut examined = 0;
            while examined < limit
                && state.samples.len() < self.planned_samples
                && state.cursor.sampled < self.manifest.slots
            {
                check(self, index, checkpoint)?;
                let slot = (u128::from(self.slot_start)
                    + u128::from(state.cursor.sampled) * u128::from(self.slot_stride))
                    % u128::from(self.manifest.slots);
                let slot = slot as u64;
                state.cursor.sampled += 1;
                examined += 1;
                let mut visibility = [0; 1];
                read_bytes(
                    &page_source,
                    self.manifest,
                    PageKind::Visibility,
                    slot / 8,
                    &mut visibility,
                    &mut page,
                )?;
                if visibility[0] & (1 << (slot % 8)) == 0 {
                    continue;
                }
                let stride = (self.manifest.dimensions as u64)
                    .checked_mul(4)
                    .and_then(|n| n.checked_add(8))
                    .ok_or_else(corrupt)?;
                let row = word(
                    &page_source,
                    self.manifest,
                    PageKind::Canonical,
                    slot.checked_mul(stride).ok_or_else(corrupt)?,
                    &mut page,
                )?;
                let lsn = word(
                    &page_source,
                    self.manifest,
                    PageKind::SlotMetadata,
                    slot.checked_mul(40).ok_or_else(corrupt)?,
                    &mut page,
                )?;
                state.samples.push(Sample {
                    row,
                    generation: self.manifest.generation,
                    slot,
                    row_lsn: lsn,
                    vector_lsn: lsn,
                    ..Sample::default()
                });
            }
            Ok(state.samples.len() == self.planned_samples
                || state.cursor.sampled == self.manifest.slots)
        }
    }
    fn sample_questions(
        &self,
        tuple: &VectorPartitionKey,
        state: &mut State,
        limit: usize,
        checkpoint: &dyn Fn() -> Result<()>,
        source: &capture::Source,
    ) -> Result<bool> {
        let empty = VectorPartitionKey::unpartitioned();
        let tuple = if source.data.partitioned {
            tuple
        } else {
            &empty
        };
        let ranges = Ranges::new(self, &source.data.index, tuple, state.cursor.source_after)?;
        let root = capture::writer(&self.root)?;
        root.check_table_scope(&source.data.index.table)?;
        let bounds = if state.cursor.source_after.is_some() {
            Bound::Excluded(ranges.lower.as_slice())
        } else {
            Bound::Included(ranges.lower.as_slice())
        };
        let mut cursor = root.cursor(
            SourceWindowOwner::Read(self.memory.clone()),
            super::super::super::VECTOR_PARTITION_MEMBERSHIP_TABLE.name(),
            WriterCursorFamily::Bytes,
            bounds,
            Bound::Included(&ranges.upper),
            false,
        )?;
        let (_, snapshot, frontier) = self.root.authenticate()?;
        for _ in 0..limit {
            checkpoint()?;
            if self.cancelled.load(Ordering::Acquire) {
                return Err(Error::ReadCancelled);
            }
            let Some(window) = cursor.next_membership()? else {
                return Ok(true);
            };
            let key = window.position();
            let after = key
                .get(key.len().checked_sub(40).ok_or_else(corrupt)?..)
                .ok_or_else(corrupt)?;
            state.cursor.source_after = Some(after.try_into().map_err(|_| corrupt())?);
            state.cursor.sampled = state.cursor.sampled.checked_add(1).ok_or_else(corrupt)?;
            let record = &window.value;
            if record.index != source.data.index || record.partition_key != *tuple {
                return Err(corrupt());
            }
            if record.visible_from.0 > snapshot.0
                || record.row_lsn > frontier
                || record.deleted_tx.is_some_and(|tx| tx.0 <= snapshot.0)
            {
                continue;
            }
            if state.cursor.last_source_row == Some(record.row_id.0) {
                return Err(corrupt());
            }
            state.cursor.last_source_row = Some(record.row_id.0);
            let mut hash = blake3::Hasher::new();
            hash.update(b"contextdb.clustered.calibration.questions.v1");
            hash.update(&self.seed.to_le_bytes());
            hash.update(&self.scope);
            hash.update(&record.row_id.0.to_le_bytes());
            hash.update(&record.vector_lsn.0.to_le_bytes());
            let rank = u64::from_le_bytes(
                hash.finalize().as_bytes()[..8]
                    .try_into()
                    .map_err(|_| corrupt())?,
            );
            let sample = Sample {
                row: record.row_id.0,
                row_created_tx: record.row_created_tx.0,
                row_lsn: record.row_lsn.0,
                vector_created_tx: record.vector_created_tx.0,
                vector_lsn: record.vector_lsn.0,
                visible_from: record.visible_from.0,
                rank,
                ..Sample::default()
            };
            let at = state
                .samples
                .binary_search_by_key(&(sample.rank, sample.row), |s| (s.rank, s.row))
                .unwrap_or_else(|at| at);
            if at >= self.planned_samples {
                continue;
            }
            if state.samples.len() == self.planned_samples {
                state.samples.pop();
            }
            state.samples.insert(at, sample);
        }
        Ok(false)
    }
    pub(super) fn finish_samples(&self, state: &mut State, index: &VectorIndexRef) -> Result<()> {
        state.excluded.clear();
        let own_source = self
            .source
            .as_ref()
            .is_none_or(|source| source.data.index == *index);
        if own_source {
            state
                .excluded
                .extend(state.samples.iter().map(|sample| sample.row));
            state.excluded.sort_unstable();
            if state.excluded.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(corrupt());
            }
        }
        Ok(())
    }
    pub(super) fn query_vector(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        sample: Sample,
        output: &mut [f32],
        page: &mut [u8; PAGE_BYTES],
        checkpoint: &dyn Fn() -> Result<()>,
    ) -> Result<()> {
        check(self, index, checkpoint)?;
        if output.len() != self.manifest.dimensions {
            return Err(corrupt());
        }
        if let Some(source) = &self.source {
            let empty = VectorPartitionKey::unpartitioned();
            let tuple = if source.data.partitioned {
                tuple
            } else {
                &empty
            };
            // These exact captured membership fields were selected above; the
            // existing body exporter retains its original whole caller window.
            let names = source
                .data
                .index
                .table
                .len()
                .checked_add(source.data.index.column.len())
                .ok_or_else(corrupt)?;
            let tuple_bytes = tuple.components().iter().try_fold(0usize, |sum, c| {
                sum.checked_add(match c {
                    contextdb_core::VectorPartitionComponent::Text(s) => s.len(),
                    _ => 0,
                })
                .ok_or_else(corrupt)
            })?;
            let total = std::mem::size_of::<super::super::super::VectorPartitionMembershipRecord>()
                .checked_add(names)
                .and_then(|n| n.checked_add(tuple_bytes))
                .and_then(|n| {
                    n.checked_add(
                        tuple.components().len()
                            * std::mem::size_of::<contextdb_core::VectorPartitionComponent>(),
                    )
                })
                .ok_or_else(corrupt)?;
            let _invoice = memory::Invoice::reserve(
                &self.owner,
                total,
                "clustered_calibration_query_identity",
            )?;
            let record = super::super::super::VectorPartitionMembershipRecord {
                index: source.data.index.clone(),
                partition_key: tuple.clone(),
                row_id: RowId(sample.row),
                row_created_tx: TxId(sample.row_created_tx),
                row_lsn: Lsn(sample.row_lsn),
                vector_created_tx: TxId(sample.vector_created_tx),
                vector_lsn: Lsn(sample.vector_lsn),
                visible_from: TxId(sample.visible_from),
                deleted_tx: None,
            };
            let body = capture::writer(&self.root)?
                .export_vector_body(SourceWindowOwner::Read(self.memory.clone()), &record)?;
            if body.value.vector.len() != output.len() {
                return Err(corrupt());
            }
            output.copy_from_slice(&body.value.vector);
            if output.iter().any(|value| !value.is_finite()) {
                return Err(corrupt());
            }
            Ok(())
        } else {
            let authority = read::NativeAuthority::open_prepared(
                &self.root,
                self.memory.clone(),
                &self.verification,
            )?;
            let selected = || check(self, index, checkpoint);
            let source = read::PageSource::open_prepared(
                &authority,
                index,
                tuple,
                &selected,
                &|_| Ok(()),
                &self.verification,
            )?;
            if source.select_build_authority_with_verification(&self.verification)? != self.manifest
                || sample.generation != self.manifest.generation
            {
                return Err(corrupt());
            }
            let stride = (self.manifest.dimensions as u64)
                .checked_mul(4)
                .and_then(|n| n.checked_add(8))
                .ok_or_else(corrupt)?;
            let offset = sample.slot.checked_mul(stride).ok_or_else(corrupt)?;
            if word(&source, self.manifest, PageKind::Canonical, offset, page)? != sample.row {
                return Err(corrupt());
            }
            let mut copied = 0usize;
            while copied < output.len() {
                let at = offset
                    .checked_add(8)
                    .and_then(|n| n.checked_add((copied as u64).checked_mul(4)?))
                    .ok_or_else(corrupt)?;
                let ordinal = at / PAGE_BYTES as u64;
                if !source
                    .read_admitted_page(self.manifest.key(PageKind::Canonical, 0, ordinal), page)?
                {
                    return Err(corrupt());
                }
                let start = (at % PAGE_BYTES as u64) as usize;
                // Canonical coordinates and native payloads are four-byte aligned.
                let take = (output.len() - copied).min((PAGE_BYTES - start) / 4);
                if take == 0 {
                    return Err(corrupt());
                }
                for (value, bits) in output[copied..copied + take]
                    .iter_mut()
                    .zip(page[start..start + take * 4].chunks_exact(4))
                {
                    *value = f32::from_le_bytes(bits.try_into().map_err(|_| corrupt())?);
                    if !value.is_finite() {
                        return Err(corrupt());
                    }
                }
                copied += take;
            }
            Ok(())
        }
    }
}
