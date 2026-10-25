use super::{
    book::{Codebook, list_page_used},
    catalog::{Catalog, Entry, Run, tail_slot},
    codec,
    memory::{Invoice, bytes, corrupt, dimension, overflow},
    storage::{DistanceKind, Manifest, PAGE_BYTES, PageKind, ReadPages},
    types::{Candidate, Hit, Mode, Policy, Work, cosine, retain_best},
};
use crate::MemoryBudgetOwner;
use contextdb_core::Result;

/// Search into caller-admitted output. Catalogs, codebooks and work are from the
/// captured native transaction; no test observer participates in this path.
pub fn search(
    source: &impl ReadPages,
    policy: Policy,
    owner: &MemoryBudgetOwner,
    query: &[f32],
    contexts: Option<&[u64]>,
    mode: Mode,
    output: &mut [Hit],
) -> Result<(usize, Work)> {
    let mut work = Work::default();
    let n = search_with_work(
        source, policy, owner, query, contexts, mode, output, &mut work,
    )?;
    Ok((n, work))
}
/// Keeps actual work readable even on refusal/cancellation and includes failed indexed attempts.
#[allow(clippy::too_many_arguments)]
pub fn search_with_work(
    source: &impl ReadPages,
    policy: Policy,
    owner: &MemoryBudgetOwner,
    query: &[f32],
    contexts: Option<&[u64]>,
    mode: Mode,
    output: &mut [Hit],
    work: &mut Work,
) -> Result<usize> {
    *work = Work::default();
    let tracked = super::page_work::Tracked::new(source);
    let attempted = search_impl(
        &tracked,
        policy,
        owner,
        query,
        contexts,
        if mode == Mode::Auto {
            Mode::Indexed
        } else {
            mode
        },
        output,
        work,
    );
    let result = match attempted {
        Err(contextdb_core::Error::StoreCorrupted { .. }) if mode == Mode::Auto => search_impl(
            &tracked,
            policy,
            owner,
            query,
            contexts,
            Mode::Exact,
            output,
            work,
        ),
        result => result,
    };
    tracked.apply(work);
    result
}
#[allow(clippy::too_many_arguments)]
fn search_impl(
    source: &impl ReadPages,
    policy: Policy,
    owner: &MemoryBudgetOwner,
    query: &[f32],
    contexts: Option<&[u64]>,
    mode: Mode,
    output: &mut [Hit],
    work: &mut Work,
) -> Result<usize> {
    if query.len() != policy.dimensions {
        return Err(dimension(source, policy.dimensions, query.len()));
    }
    source.check_cancel()?;
    let k = output.len();
    if k == 0 {
        return Ok(0);
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
        .and_then(|x| x.checked_add(policy.dimensions.checked_mul(5)?))
        .and_then(|x| {
            x.checked_add(
                PAGE_BYTES * 12
                    + std::mem::size_of::<blake3::Hasher>()
                    + std::mem::size_of::<Catalog>() * 2
                    + std::mem::size_of::<Work>(),
            )
        })
        .ok_or_else(overflow)?;
    let _scratch = Invoice::reserve(owner, charge, "clustered_query")?;
    let mut candidates = Vec::with_capacity(depth);
    let mut merged = Vec::with_capacity(k);
    let mut scores = Vec::with_capacity(policy.lists as usize);
    let mut raw = vec![0; raw_bytes];
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
    let mut visit = |partition| -> Result<()> {
        let Some(m) = source.manifest(partition)? else {
            return Ok(());
        };
        validate(source, m, policy)?;
        let c = Catalog::load(source, m)?;
        if mode == Mode::Indexed
            && let Some(key) = c.unserviceable
        {
            return Err(super::memory::corrupt_page(key));
        }
        // At most two codebooks in state B, visited sequentially under whole-object funding.
        for old in [false, true] {
            let manifest = if old {
                let Some(x) = c.legacy else { continue };
                x
            } else {
                m
            };
            validate(source, manifest, policy)?;
            let current_catalog = if old { Catalog::new(0) } else { c };
            candidates.clear();
            scores.clear();
            let mut visibility = [0; PAGE_BYTES];
            let mut vis_loaded = None;
            let mut metadata = MetadataWindow::new(source, manifest);
            let mut canonical = CanonicalWindow::new(source, manifest);
            if mode == Mode::Exact || manifest.active_lists == 0 || (old && c.legacy_unserviceable)
            {
                if old {
                    let mut id = c.legacy_first;
                    for _ in 0..c.legacy_runs {
                        let run = Run::load(source, manifest, id)?;
                        let mut slot = run.first;
                        for _ in 0..run.entries {
                            source.check_cancel()?;
                            let e = metadata.entry(slot)?;
                            if e.lsn > c.frontier
                                && e.lsn <= c.switch_lsn
                                && is_visible(
                                    source,
                                    manifest,
                                    slot,
                                    &mut visibility,
                                    &mut vis_loaded,
                                )?
                            {
                                score_exact(
                                    source,
                                    query,
                                    slot,
                                    false,
                                    mode,
                                    &mut canonical,
                                    &mut raw,
                                    &mut values,
                                    &mut candidates,
                                    depth,
                                    work,
                                )?;
                            }
                            slot = e.next;
                        }
                        id = run.next;
                    }
                    if id != 0 {
                        return Err(corrupt());
                    }
                } else {
                    for slot in 0..manifest.slots {
                        source.check_cancel()?;
                        let e = metadata.entry(slot)?;
                        if (e.prepared && e.run == c.remap_output)
                            || !is_visible(
                                source,
                                manifest,
                                slot,
                                &mut visibility,
                                &mut vis_loaded,
                            )?
                        {
                            continue;
                        }
                        let tail = e.tail >= c.tail_head && e.tail < c.tail_end;
                        score_exact(
                            source,
                            query,
                            slot,
                            tail,
                            mode,
                            &mut canonical,
                            &mut raw,
                            &mut values,
                            &mut candidates,
                            depth,
                            work,
                        )?;
                    }
                }
            } else {
                let book = Codebook::load_values(owner, source, manifest)?;
                for list in 0..manifest.active_lists {
                    source.check_cancel()?;
                    source.before_distance(DistanceKind::Centroid)?;
                    let distance = cosine(query, book.row(list as usize));
                    work.centroid_evaluations += 1;
                    scores.push((distance, list));
                }
                scores.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
                for (rank, (_, list)) in scores.iter().enumerate() {
                    if rank >= policy.probes as usize
                        && (!policy.extend_to_k || candidates.len() >= k)
                    {
                        break;
                    }
                    work.lists_probed += 1;
                    // One centroid pass per codebook, not per sealed run.
                    let mut id = if old { c.legacy_first } else { 0 };
                    let roots = if old {
                        c.legacy_runs
                    } else {
                        current_catalog.runs.checked_add(1).ok_or_else(corrupt)?
                    };
                    for root in 0..roots {
                        let mut view = manifest;
                        view.run = id;
                        let run = if id != 0 {
                            let r = Run::load(source, manifest, id)?;
                            Some(r)
                        } else {
                            None
                        };
                        let d = codec::directory(source, view, *list)?;
                        let mut remaining = d.count;
                        let mut page_id = d.head;
                        while remaining > 0 {
                            source.check_cancel()?;
                            let mut page = [0; PAGE_BYTES];
                            if page_id == 0
                                || !super::storage::read_page(
                                    source,
                                    manifest.key(PageKind::Lists, *list, page_id - 1),
                                    &mut page,
                                )?
                            {
                                return Err(corrupt());
                            }
                            let used = list_page_used(source, manifest, *list, page_id)?;
                            let stride = 16 + policy.dimensions;
                            if used == 0 || used > PAGE_BYTES / stride || used as u64 > remaining {
                                return Err(corrupt());
                            }
                            for record in page.chunks_exact(stride).take(used) {
                                let slot = u64::from_le_bytes(
                                    record[..8].try_into().map_err(|_| corrupt())?,
                                );
                                let row = u64::from_le_bytes(
                                    record[8..16].try_into().map_err(|_| corrupt())?,
                                );
                                if slot >= manifest.slots {
                                    return Err(corrupt());
                                }
                                if old {
                                    let e = metadata.entry(slot)?;
                                    if e.lsn <= c.frontier || e.lsn > c.switch_lsn {
                                        continue;
                                    }
                                }
                                if !is_visible(
                                    source,
                                    manifest,
                                    slot,
                                    &mut visibility,
                                    &mut vis_loaded,
                                )? {
                                    continue;
                                }
                                source.before_distance(DistanceKind::Quantized)?;
                                let (mut dot, mut aa, mut bb) = (0i64, 0i64, 0i64);
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
                            remaining -= used as u64;
                            if remaining > 0 {
                                page_id = codec::next_list_page(source, manifest, *list, page_id)?;
                            }
                        }
                        id = if old {
                            run.ok_or_else(corrupt)?.next
                        } else if root == 0 {
                            current_catalog.first
                        } else {
                            run.ok_or_else(corrupt)?.next
                        };
                    }
                    if id != 0 {
                        return Err(corrupt());
                    }
                }
                candidates.sort_unstable_by_key(|x| x.slot);
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
                if !old {
                    for seq in c.tail_head..c.tail_end {
                        let slot = tail_slot(source, manifest, seq)?;
                        if !is_visible(source, manifest, slot, &mut visibility, &mut vis_loaded)? {
                            continue;
                        }
                        score_exact(
                            source,
                            query,
                            slot,
                            true,
                            mode,
                            &mut canonical,
                            &mut raw,
                            &mut values,
                            &mut candidates,
                            depth,
                            work,
                        )?;
                    }
                }
            }
            for candidate in &candidates {
                retain_best(&mut merged, k, *candidate);
            }
        }
        Ok(())
    };
    if let Some(partitions) = contexts {
        for (i, p) in partitions.iter().enumerate() {
            if !partitions[..i].contains(p) {
                visit(*p)?;
            }
        }
    } else {
        let mut after = None;
        while let Some(p) = source.next_partition(after)? {
            visit(p)?;
            after = Some(p);
        }
    }
    for (dst, src) in output.iter_mut().zip(&merged) {
        *dst = Hit {
            row: src.row,
            distance: src.distance,
        };
    }
    Ok(merged.len())
}
fn validate(source: &impl ReadPages, m: Manifest, p: Policy) -> Result<()> {
    if m.dimensions != p.dimensions {
        return Err(dimension(source, m.dimensions, p.dimensions));
    }
    if m.lists > p.lists {
        return Err(super::memory::phase(
            "clustered query centroid capacity does not cover the captured generation",
        ));
    }
    if m.active_lists > m.lists || m.sealed_slots > m.slots {
        return Err(corrupt());
    }
    Ok(())
}
#[allow(clippy::too_many_arguments)]
fn score_exact<S: ReadPages>(
    s: &S,
    q: &[f32],
    slot: u64,
    tail: bool,
    mode: Mode,
    canonical: &mut CanonicalWindow<'_, S>,
    raw: &mut [u8],
    values: &mut [f32],
    out: &mut Vec<Candidate>,
    capacity: usize,
    work: &mut Work,
) -> Result<()> {
    let row = canonical.vector(slot, raw, values)?;
    s.before_distance(if tail {
        DistanceKind::Buffer
    } else {
        DistanceKind::Canonical
    })?;
    let distance = cosine(q, values);
    if tail {
        work.buffer_evaluations += 1
    } else {
        work.full_precision_evaluations += 1
    }
    if mode == Mode::Exact {
        work.exact_score_count += 1;
    }
    retain_best(
        out,
        capacity,
        Candidate {
            slot,
            row,
            distance,
        },
    );
    Ok(())
}
fn is_visible(
    s: &impl ReadPages,
    m: Manifest,
    slot: u64,
    page: &mut [u8; PAGE_BYTES],
    loaded: &mut Option<u64>,
) -> Result<bool> {
    let result = codec::visible(s, m, slot, page, loaded)?;
    Ok(result)
}
struct MetadataWindow<'a, S> {
    source: &'a S,
    manifest: Manifest,
    page: [u8; PAGE_BYTES],
    loaded: Option<u64>,
}
impl<'a, S: ReadPages> MetadataWindow<'a, S> {
    fn new(source: &'a S, manifest: Manifest) -> Self {
        Self {
            source,
            manifest,
            page: [0; PAGE_BYTES],
            loaded: None,
        }
    }
    fn entry(&mut self, slot: u64) -> Result<Entry> {
        let mut raw = [0; 40];
        let mut offset = slot.checked_mul(40).ok_or_else(corrupt)?;
        let mut out = &mut raw[..];
        while !out.is_empty() {
            let ordinal = offset / PAGE_BYTES as u64;
            if self.loaded != Some(ordinal) {
                self.source.check_cancel()?;
                if !super::storage::read_page(
                    self.source,
                    self.manifest.key(PageKind::SlotMetadata, 0, ordinal),
                    &mut self.page,
                )? {
                    return Err(corrupt());
                }
                self.loaded = Some(ordinal);
            }
            let at = (offset % PAGE_BYTES as u64) as usize;
            let n = out.len().min(PAGE_BYTES - at);
            out[..n].copy_from_slice(&self.page[at..at + n]);
            out = &mut out[n..];
            offset = offset.checked_add(n as u64).ok_or_else(corrupt)?;
        }
        let w = |i: usize| -> Result<u64> {
            Ok(u64::from_le_bytes(
                raw[i * 8..i * 8 + 8].try_into().map_err(|_| corrupt())?,
            ))
        };
        if w(4)? > 1 {
            return Err(corrupt());
        }
        Ok(Entry {
            lsn: w(0)?,
            next: w(1)?,
            run: w(2)?,
            tail: w(3)?,
            prepared: w(4)? == 1,
        })
    }
}
struct CanonicalWindow<'a, S> {
    source: &'a S,
    manifest: Manifest,
    page: [u8; PAGE_BYTES],
    loaded: Option<u64>,
}
impl<'a, S: ReadPages> CanonicalWindow<'a, S> {
    fn new(source: &'a S, manifest: Manifest) -> Self {
        Self {
            source,
            manifest,
            page: [0; PAGE_BYTES],
            loaded: None,
        }
    }
    fn vector(&mut self, slot: u64, raw: &mut [u8], values: &mut [f32]) -> Result<u64> {
        let mut offset = slot.checked_mul(raw.len() as u64).ok_or_else(corrupt)?;
        let mut out = &mut *raw;
        while !out.is_empty() {
            let ordinal = offset / PAGE_BYTES as u64;
            if self.loaded != Some(ordinal) {
                self.source.check_cancel()?;
                if !super::storage::read_page(
                    self.source,
                    self.manifest.key(PageKind::Canonical, 0, ordinal),
                    &mut self.page,
                )? {
                    return Err(corrupt());
                }
                self.source.check_cancel()?;
                self.loaded = Some(ordinal);
            }
            let at = (offset % PAGE_BYTES as u64) as usize;
            let n = out.len().min(PAGE_BYTES - at);
            out[..n].copy_from_slice(&self.page[at..at + n]);
            out = &mut out[n..];
            offset = offset.checked_add(n as u64).ok_or_else(corrupt)?;
        }
        let row = u64::from_le_bytes(raw[..8].try_into().map_err(|_| corrupt())?);
        for (i, v) in values.iter_mut().enumerate() {
            *v = f32::from_le_bytes(
                raw[8 + i * 4..12 + i * 4]
                    .try_into()
                    .map_err(|_| corrupt())?,
            );
        }
        Ok(row)
    }
}
