use super::{
    book::Codebook,
    codec,
    memory::{Invoice, bytes, corrupt, overflow},
    storage::{DistanceKind, Manifest, PAGE_BYTES, PageKind, ReadPages},
    types::{Candidate, Hit, Mode, Policy, Work, cosine, retain_best},
};
use crate::MemoryBudgetOwner;
use contextdb_core::Result;

/// Search into caller-admitted output. Internal scratch never escapes with an answer.
/// The returned ledger is production-readable without the test-seams feature.
pub fn search(
    source: &impl ReadPages,
    policy: Policy,
    owner: &MemoryBudgetOwner,
    query: &[f32],
    contexts: Option<&[u64]>,
    mode: Mode,
    output: &mut [Hit],
) -> Result<(usize, Work)> {
    if query.len() != policy.dimensions {
        return Err(corrupt());
    }
    source.check_cancel()?;
    let k = output.len();
    if k == 0 {
        return Ok((0, Work::default()));
    }
    let depth = policy.rerank.max(k);
    let raw_bytes = policy
        .dimensions
        .checked_mul(4)
        .and_then(|x| x.checked_add(8))
        .ok_or_else(overflow)?;
    let charge = bytes::<Candidate>(depth)?
        .checked_add(bytes::<Candidate>(k)?)
        .and_then(|x| x.checked_add(bytes::<(f64, u32)>(policy.lists as usize).ok()?))
        .and_then(|x| x.checked_add(raw_bytes))
        .and_then(|x| x.checked_add(policy.dimensions * 5))
        .and_then(|x| x.checked_add(PAGE_BYTES * 3 + std::mem::size_of::<blake3::Hasher>()))
        .ok_or_else(overflow)?;
    let _scratch = Invoice::reserve(owner, charge, "clustered_query")?;
    let mut candidates = Vec::with_capacity(depth);
    let mut merged = Vec::with_capacity(k);
    let mut scores = Vec::with_capacity(policy.lists as usize);
    let mut raw = vec![0u8; raw_bytes];
    let mut values = vec![0.0; policy.dimensions];
    let mut query_code = vec![0i8; policy.dimensions];
    let norm = query
        .iter()
        .map(|x| f64::from(*x).powi(2))
        .sum::<f64>()
        .sqrt();
    for (dst, src) in query_code.iter_mut().zip(query) {
        *dst = if norm == 0.0 {
            0
        } else {
            (f64::from(*src) / norm * 127.0)
                .round()
                .clamp(-127.0, 127.0) as i8
        };
    }
    let mut work = Work::default();
    let mut visit = |partition| -> Result<()> {
        source.check_cancel()?;
        let Some(manifest) = source.manifest(partition)? else {
            return Ok(());
        };
        if manifest.dimensions != policy.dimensions
            || manifest.lists > policy.lists
            || manifest.active_lists > manifest.lists
            || manifest.sealed_slots > manifest.slots
        {
            return Err(corrupt());
        }
        candidates.clear();
        scores.clear();
        let mut visibility = [0u8; PAGE_BYTES];
        let mut vis_loaded = None;
        let mut canonical = CanonicalWindow::new(source, manifest);
        if mode == Mode::Exact || manifest.active_lists == 0 {
            for slot in 0..manifest.slots {
                source.check_cancel()?;
                if !is_visible(
                    source,
                    manifest,
                    slot,
                    &mut visibility,
                    &mut vis_loaded,
                    &mut work,
                )? {
                    continue;
                }
                let row = canonical.vector(slot, &mut raw, &mut values)?;
                let tail = slot >= manifest.sealed_slots;
                source.before_distance(if tail {
                    DistanceKind::Buffer
                } else {
                    DistanceKind::Canonical
                })?;
                let distance = cosine(query, &values);
                if tail {
                    work.buffer_evaluations += 1;
                } else {
                    work.full_precision_evaluations += 1;
                }
                if mode == Mode::Exact {
                    work.exact_score_count += 1;
                }
                retain_best(
                    &mut candidates,
                    depth,
                    Candidate {
                        slot,
                        row,
                        distance,
                    },
                );
            }
        } else {
            let book = Codebook::load(owner, source, manifest)?;
            for list in 0..manifest.active_lists {
                source.check_cancel()?;
                source.before_distance(DistanceKind::Centroid)?;
                let distance = cosine(query, book.row(list as usize));
                work.centroid_evaluations += 1;
                scores.push((distance, list));
            }
            scores.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            let stride = 16 + policy.dimensions;
            let per_page = PAGE_BYTES / stride;
            for (rank, (_, list)) in scores.iter().enumerate() {
                if rank >= policy.probes as usize && (!policy.extend_to_k || candidates.len() >= k)
                {
                    break;
                }
                work.lists_probed += 1;
                let count = codec::count(source, manifest, *list)?;
                for ordinal in 0..count.div_ceil(per_page as u64) {
                    source.check_cancel()?;
                    let mut page = [0u8; PAGE_BYTES];
                    if !source.read_admitted_page(
                        manifest.key(PageKind::Lists, *list, ordinal),
                        &mut page,
                    )? {
                        return Err(corrupt());
                    }
                    work.list_pages_read += 1;
                    let entries = per_page.min((count - ordinal * per_page as u64) as usize);
                    for record in page.chunks_exact(stride).take(entries) {
                        let slot =
                            u64::from_le_bytes(record[..8].try_into().map_err(|_| corrupt())?);
                        let row =
                            u64::from_le_bytes(record[8..16].try_into().map_err(|_| corrupt())?);
                        if slot >= manifest.sealed_slots {
                            return Err(corrupt());
                        }
                        if !is_visible(
                            source,
                            manifest,
                            slot,
                            &mut visibility,
                            &mut vis_loaded,
                            &mut work,
                        )? {
                            continue;
                        }
                        source.before_distance(DistanceKind::Quantized)?;
                        let mut dot = 0i64;
                        let mut aa = 0i64;
                        let mut bb = 0i64;
                        for (&a, &b) in query_code.iter().zip(&record[16..]) {
                            let a = i64::from(a);
                            let b = i64::from(b as i8);
                            dot += a * b;
                            aa += a * a;
                            bb += b * b;
                        }
                        let distance = if aa == 0 || bb == 0 {
                            1.0
                        } else {
                            1.0 - dot as f64 / ((aa as f64).sqrt() * (bb as f64).sqrt())
                        };
                        work.quantized_evaluations += 1;
                        retain_best(
                            &mut candidates,
                            depth,
                            Candidate {
                                slot,
                                row,
                                distance,
                            },
                        );
                    }
                }
            }
            // Slot order coalesces canonical pages; distance order is restored only after re-score.
            candidates.sort_unstable_by_key(|c| c.slot);
            for candidate in &mut candidates {
                source.check_cancel()?;
                let row = canonical.vector(candidate.slot, &mut raw, &mut values)?;
                if row != candidate.row {
                    return Err(corrupt());
                }
                source.before_distance(DistanceKind::Canonical)?;
                candidate.distance = cosine(query, &values);
                work.full_precision_evaluations += 1;
            }
            candidates.sort_unstable_by(super::types::order);
            for slot in manifest.sealed_slots..manifest.slots {
                source.check_cancel()?;
                if !is_visible(
                    source,
                    manifest,
                    slot,
                    &mut visibility,
                    &mut vis_loaded,
                    &mut work,
                )? {
                    continue;
                }
                let row = canonical.vector(slot, &mut raw, &mut values)?;
                source.before_distance(DistanceKind::Buffer)?;
                let distance = cosine(query, &values);
                work.buffer_evaluations += 1;
                retain_best(
                    &mut candidates,
                    depth,
                    Candidate {
                        slot,
                        row,
                        distance,
                    },
                );
            }
        }
        work.canonical_pages_read += canonical.pages;
        for candidate in &candidates {
            retain_best(&mut merged, k, *candidate);
        }
        Ok(())
    };
    if let Some(partitions) = contexts {
        for (i, partition) in partitions.iter().enumerate() {
            if !partitions[..i].contains(partition) {
                visit(*partition)?;
            }
        }
    } else {
        let mut after = None;
        while let Some(partition) = source.next_partition(after)? {
            visit(partition)?;
            after = Some(partition);
        }
    }
    for (dst, src) in output.iter_mut().zip(&merged) {
        *dst = Hit {
            row: src.row,
            distance: src.distance,
        };
    }
    Ok((merged.len(), work))
}
fn is_visible(
    source: &impl ReadPages,
    manifest: Manifest,
    slot: u64,
    page: &mut [u8; PAGE_BYTES],
    loaded: &mut Option<u64>,
    work: &mut Work,
) -> Result<bool> {
    let previous = *loaded;
    let result = codec::visible(source, manifest, slot, page, loaded)?;
    if previous != *loaded {
        work.visibility_pages_read += 1;
    }
    Ok(result)
}
struct CanonicalWindow<'a, S> {
    source: &'a S,
    manifest: Manifest,
    page: [u8; PAGE_BYTES],
    loaded: Option<u64>,
    pages: u64,
}
impl<'a, S: ReadPages> CanonicalWindow<'a, S> {
    fn new(source: &'a S, manifest: Manifest) -> Self {
        Self {
            source,
            manifest,
            page: [0; PAGE_BYTES],
            loaded: None,
            pages: 0,
        }
    }
    fn vector(&mut self, slot: u64, raw: &mut [u8], values: &mut [f32]) -> Result<u64> {
        let mut offset = slot.checked_mul(raw.len() as u64).ok_or_else(overflow)?;
        let mut out = &mut *raw;
        while !out.is_empty() {
            let ordinal = offset / PAGE_BYTES as u64;
            if self.loaded != Some(ordinal) {
                self.source.check_cancel()?;
                if !self.source.read_admitted_page(
                    self.manifest.key(PageKind::Canonical, 0, ordinal),
                    &mut self.page,
                )? {
                    return Err(corrupt());
                }
                self.source.check_cancel()?;
                self.loaded = Some(ordinal);
                self.pages += 1;
            }
            let at = (offset % PAGE_BYTES as u64) as usize;
            let n = out.len().min(PAGE_BYTES - at);
            out[..n].copy_from_slice(&self.page[at..at + n]);
            out = &mut out[n..];
            offset += n as u64;
        }
        let row = u64::from_le_bytes(raw[..8].try_into().map_err(|_| corrupt())?);
        for (i, value) in values.iter_mut().enumerate() {
            *value = f32::from_le_bytes(
                raw[8 + i * 4..12 + i * 4]
                    .try_into()
                    .map_err(|_| corrupt())?,
            );
        }
        Ok(row)
    }
}
