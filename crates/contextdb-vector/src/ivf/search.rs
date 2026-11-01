use super::{
    book::{Codebook, list_block_layout, list_page_used},
    catalog::{Catalog, Entry, Run, tail_slot},
    codec,
    memory::{Invoice, bytes, corrupt, dimension, overflow},
    storage::{DistanceKind, Manifest, PAGE_BYTES, PageKind, ReadPages},
    types::{
        Candidate, CandidateIdentity, CandidateSource, CanonicalCandidate, Hit, Mode, Policy,
        RowIdentity, Work, cosine, retain_best,
    },
};
use crate::MemoryBudgetOwner;
use contextdb_core::Result;

/// Exact existing search-constructor allowance, without allocation or admission.
/// Codebooks remain separately admitted whole objects; native frames keep their owners.
pub fn query_workspace_allocation_bytes(policy: Policy, output_slots: usize) -> Result<usize> {
    let k = output_slots;
    if k == 0 {
        return Ok(0);
    }
    let depth = policy.rerank.max(k);
    let raw_bytes = policy
        .dimensions
        .checked_mul(4)
        .and_then(|n| n.checked_add(8))
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
    Ok(charge)
}

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
    let mut accept_all = |_: CandidateIdentity<'_>| Ok::<bool, contextdb_core::Error>(true);
    search_with_eligibility(
        source,
        policy,
        owner,
        query,
        contexts,
        mode,
        &mut accept_all,
        output,
        work,
    )
}
#[allow(clippy::too_many_arguments)]
fn search_impl<E>(
    source: &impl ReadPages,
    policy: Policy,
    owner: &MemoryBudgetOwner,
    query: &[f32],
    contexts: Option<&[u64]>,
    mode: Mode,
    eligible: &mut dyn FnMut(CandidateIdentity<'_>) -> std::result::Result<bool, E>,
    output: &mut [Hit],
    work: &mut Work,
) -> QueryResult<usize, E> {
    if query.len() != policy.dimensions {
        return Err(dimension(source, policy.dimensions, query.len()).into());
    }
    source.check_cancel()?;
    let k = output.len();
    if k == 0 {
        return Ok(0);
    }
    let snapshot_lsn = contextdb_core::Lsn(source.snapshot_lsn()?);
    let depth = policy.rerank.max(k);
    let raw_bytes = policy
        .dimensions
        .checked_mul(4)
        .and_then(|x| x.checked_add(8))
        .ok_or_else(overflow)?;
    let charge = query_workspace_allocation_bytes(policy, k)?;
    let _scratch = Invoice::reserve(owner, charge, "clustered_query")?;
    let mut candidates = Vec::with_capacity(depth);
    let mut merged = Vec::with_capacity(k);
    let mut scores = Vec::with_capacity(policy.lists as usize);
    let mut raw = vec![0; raw_bytes];
    let mut values = vec![0.0; policy.dimensions];
    let mut query_code = vec![0i8; policy.dimensions];
    encode_query(query, &mut query_code);
    let mut visit = |partition| -> QueryResult<(), E> {
        let Some(m) = source.manifest(partition)? else {
            return Ok(());
        };
        validate(source, m, policy)?;
        let c = Catalog::load(source, m)?;
        if mode == Mode::Indexed
            && let Some(key) = c.unserviceable
        {
            return Err(super::memory::corrupt_page(key).into());
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
                for_each_visible_slot(
                    source,
                    manifest,
                    c,
                    old,
                    &mut metadata,
                    &mut visibility,
                    &mut vis_loaded,
                    |slot, entry, tail| {
                        score_exact(
                            source,
                            query,
                            slot,
                            manifest,
                            entry,
                            old,
                            snapshot_lsn,
                            eligible,
                            tail,
                            mode,
                            &mut canonical,
                            &mut raw,
                            &mut values,
                            &mut candidates,
                            depth,
                            work,
                        )
                    },
                )?;
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
                            if page_id == 0 {
                                return Err(corrupt().into());
                            }
                            let stride = 16 + policy.dimensions;
                            let (capacity, block_pages) =
                                list_block_layout(policy.dimensions).ok_or_else(corrupt)?;
                            let first_page = (page_id - 1)
                                .checked_mul(block_pages as u64)
                                .ok_or_else(corrupt)?;
                            // A wider record spans pages into the admitted canonical scratch,
                            // which no canonical read uses during this list scan.
                            let block: &[u8] = if stride <= PAGE_BYTES {
                                if !super::storage::read_page(
                                    source,
                                    manifest.key(PageKind::Lists, *list, first_page),
                                    &mut page,
                                )? {
                                    return Err(corrupt().into());
                                }
                                &page
                            } else {
                                codec::read(
                                    source,
                                    manifest,
                                    PageKind::Lists,
                                    *list,
                                    first_page
                                        .checked_mul(PAGE_BYTES as u64)
                                        .ok_or_else(corrupt)?,
                                    &mut raw[..stride],
                                )?;
                                &raw[..stride]
                            };
                            let used = list_page_used(source, manifest, *list, page_id)?;
                            if used == 0 || used > capacity || used as u64 > remaining {
                                return Err(corrupt().into());
                            }
                            for record in block.chunks_exact(stride).take(used) {
                                let slot = u64::from_le_bytes(
                                    record[..8].try_into().map_err(|_| corrupt())?,
                                );
                                let row = u64::from_le_bytes(
                                    record[8..16].try_into().map_err(|_| corrupt())?,
                                );
                                if slot >= manifest.slots {
                                    return Err(corrupt().into());
                                }
                                let e = metadata.entry(slot)?;
                                if old {
                                    if e.lsn <= c.frontier || e.lsn > c.switch_lsn {
                                        continue;
                                    }
                                } else if c.remap_join_target != 0 {
                                    // Joined pages stay masked until the one retirement commit.
                                    if e.prepared && e.run == c.remap_output {
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
                                let identity = candidate_identity(
                                    source,
                                    manifest,
                                    slot,
                                    e,
                                    old,
                                    false,
                                    snapshot_lsn,
                                    row,
                                );
                                source.check_cancel()?;
                                if !eligible(identity).map_err(QueryError::Consumer)? {
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
                        return Err(corrupt().into());
                    }
                }
                candidates.sort_unstable_by_key(|x| x.slot);
                for candidate in &mut candidates {
                    source.check_cancel()?;
                    let row = canonical.row(candidate.slot, raw.len())?;
                    if row != candidate.row {
                        return Err(corrupt().into());
                    }
                    canonical.vector(candidate.slot, row, &mut raw, &mut values)?;
                    source.before_distance(DistanceKind::Canonical)?;
                    candidate.distance = cosine(query, &values);
                    work.full_precision_evaluations += 1;
                    work.reranked_candidates += 1;
                }
                candidates.sort_unstable_by(super::types::order);
                if !old {
                    for seq in c.tail_head..c.tail_end {
                        let slot = tail_slot(source, manifest, seq)?;
                        let entry = metadata.entry(slot)?;
                        if !is_visible(source, manifest, slot, &mut visibility, &mut vis_loaded)? {
                            continue;
                        }
                        score_exact(
                            source,
                            query,
                            slot,
                            manifest,
                            entry,
                            old,
                            snapshot_lsn,
                            eligible,
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
fn score_exact<S: ReadPages, E>(
    s: &S,
    q: &[f32],
    slot: u64,
    manifest: Manifest,
    entry: Entry,
    old: bool,
    snapshot_lsn: contextdb_core::Lsn,
    eligible: &mut dyn FnMut(CandidateIdentity<'_>) -> std::result::Result<bool, E>,
    tail: bool,
    mode: Mode,
    canonical: &mut CanonicalWindow<'_, S>,
    raw: &mut [u8],
    values: &mut [f32],
    out: &mut Vec<Candidate>,
    capacity: usize,
    work: &mut Work,
) -> QueryResult<(), E> {
    let row = canonical.row(slot, raw.len())?;
    let identity = candidate_identity(s, manifest, slot, entry, old, tail, snapshot_lsn, row);
    if !eligible(identity).map_err(QueryError::Consumer)? {
        return Ok(());
    }
    canonical.vector(slot, row, raw, values)?;
    s.before_distance(if tail {
        DistanceKind::Buffer
    } else {
        DistanceKind::Canonical
    })?;
    let distance = cosine(q, values);
    if tail {
        work.buffer_evaluations += 1;
    } else {
        work.full_precision_evaluations += 1;
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
    fn read_range(&mut self, mut offset: u64, mut out: &mut [u8]) -> Result<()> {
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
            let count = out.len().min(PAGE_BYTES - at);
            out[..count].copy_from_slice(&self.page[at..at + count]);
            out = &mut out[count..];
            offset = offset.checked_add(count as u64).ok_or_else(corrupt)?;
        }
        Ok(())
    }
    fn row(&mut self, slot: u64, stride: usize) -> Result<u64> {
        let offset = slot.checked_mul(stride as u64).ok_or_else(corrupt)?;
        let mut prefix = [0; 8];
        self.read_range(offset, &mut prefix)?;
        Ok(u64::from_le_bytes(prefix))
    }
    fn vector(&mut self, slot: u64, row: u64, raw: &mut [u8], values: &mut [f32]) -> Result<()> {
        if values.len().checked_mul(4).and_then(|n| n.checked_add(8)) != Some(raw.len()) {
            return Err(corrupt());
        }
        let offset = slot
            .checked_mul(raw.len() as u64)
            .and_then(|n| n.checked_add(8))
            .ok_or_else(corrupt)?;
        raw[..8].copy_from_slice(&row.to_le_bytes());
        self.read_range(offset, &mut raw[8..])?;
        for (at, value) in values.iter_mut().enumerate() {
            *value = f32::from_le_bytes(
                raw[8 + at * 4..12 + at * 4]
                    .try_into()
                    .map_err(|_| corrupt())?,
            );
        }
        Ok(())
    }
}

/// Search the captured scope with the caller's fallible query callback.
#[allow(clippy::too_many_arguments)]
pub fn search_with_eligibility<E: From<contextdb_core::Error>>(
    source: &impl ReadPages,
    policy: Policy,
    owner: &MemoryBudgetOwner,
    query: &[f32],
    partitions: Option<&[u64]>,
    mode: Mode,
    eligible: &mut dyn FnMut(CandidateIdentity<'_>) -> std::result::Result<bool, E>,
    output: &mut [Hit],
    work: &mut Work,
) -> std::result::Result<usize, E> {
    *work = Work::default();
    let tracked = super::page_work::Tracked::new(source);
    let attempted = search_impl(
        &tracked,
        policy,
        owner,
        query,
        partitions,
        if mode == Mode::Auto {
            Mode::Indexed
        } else {
            mode
        },
        eligible,
        output,
        work,
    );
    let result = match attempted {
        Err(QueryError::Storage(contextdb_core::Error::StoreCorrupted { .. }))
            if mode == Mode::Auto =>
        {
            search_impl(
                &tracked,
                policy,
                owner,
                query,
                partitions,
                Mode::Exact,
                eligible,
                output,
                work,
            )
        }
        other => other,
    };
    tracked.apply(work);
    result.map_err(|error| match error {
        QueryError::Storage(error) => E::from(error),
        QueryError::Consumer(error) => error,
    })
}

enum QueryError<E> {
    Storage(contextdb_core::Error),
    Consumer(E),
}
impl<E> From<contextdb_core::Error> for QueryError<E> {
    fn from(error: contextdb_core::Error) -> Self {
        Self::Storage(error)
    }
}
type QueryResult<T, E> = std::result::Result<T, QueryError<E>>;

#[allow(clippy::too_many_arguments)]
fn candidate_identity<'a, S: ReadPages>(
    source: &'a S,
    manifest: Manifest,
    slot: u64,
    entry: Entry,
    old: bool,
    tail: bool,
    snapshot_lsn: contextdb_core::Lsn,
    row: u64,
) -> CandidateIdentity<'a> {
    CandidateIdentity {
        index: source.index_identity(),
        partition: manifest.partition,
        generation: manifest.generation,
        run: entry.run,
        slot,
        row: RowIdentity {
            row,
            commit_lsn: entry.lsn,
        },
        snapshot_lsn,
        source: if old {
            CandidateSource::LegacyRun
        } else if tail {
            CandidateSource::UnsealedTail
        } else if entry.run == 0 {
            CandidateSource::ServingBase
        } else {
            CandidateSource::CommittedRun
        },
    }
}

/// Iterate the same visible slot set used by exhaustive distance search.
#[allow(clippy::too_many_arguments)]
fn for_each_visible_slot<S: ReadPages, E: From<contextdb_core::Error>>(
    source: &S,
    manifest: Manifest,
    c: Catalog,
    old: bool,
    metadata: &mut MetadataWindow<'_, S>,
    visibility: &mut [u8; PAGE_BYTES],
    vis_loaded: &mut Option<u64>,
    mut visit: impl FnMut(u64, Entry, bool) -> std::result::Result<(), E>,
) -> std::result::Result<(), E> {
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
                    && is_visible(source, manifest, slot, visibility, vis_loaded)?
                {
                    visit(slot, e, false)?;
                }
                slot = e.next;
            }
            id = run.next;
        }
        if id != 0 {
            return Err(corrupt().into());
        }
    } else {
        for slot in 0..manifest.slots {
            source.check_cancel()?;
            let e = metadata.entry(slot)?;
            if (e.prepared && e.run == c.remap_output)
                || !is_visible(source, manifest, slot, visibility, vis_loaded)?
            {
                continue;
            }
            let tail = e.tail >= c.tail_head && e.tail < c.tail_end;
            visit(slot, e, tail)?;
        }
    }
    Ok(())
}

/// Visit every visible stored-precision vector without a distance shortlist.
pub fn visit_visible_canonical<E: From<contextdb_core::Error>>(
    source: &impl ReadPages,
    owner: &MemoryBudgetOwner,
    query: &[f32],
    partitions: Option<&[u64]>,
    eligible: &mut dyn FnMut(CandidateIdentity<'_>) -> std::result::Result<bool, E>,
    visitor: &mut dyn FnMut(CanonicalCandidate<'_>) -> std::result::Result<(), E>,
    work: &mut Work,
) -> std::result::Result<(), E> {
    *work = Work::default();
    let tracked = super::page_work::Tracked::new(source);
    let result = (|| -> std::result::Result<(), E> {
        let raw_bytes = query
            .len()
            .checked_mul(4)
            .and_then(|n| n.checked_add(8))
            .ok_or_else(overflow)?;
        let charge = canonical_visit_layout(&tracked, query.len())?;
        let _scratch = Invoice::reserve(owner, charge, "clustered_canonical_visit")?;
        let mut raw = vec![0; raw_bytes];
        let mut values = vec![0.0; query.len()];
        let snapshot_lsn = contextdb_core::Lsn(tracked.snapshot_lsn()?);
        let mut partition = |partition| -> std::result::Result<(), E> {
            let Some(m) = tracked.manifest(partition)? else {
                return Ok(());
            };
            if m.dimensions != query.len() {
                return Err(dimension(&tracked, m.dimensions, query.len()).into());
            }
            if m.active_lists > m.lists || m.sealed_slots > m.slots {
                return Err(corrupt().into());
            }
            let c = Catalog::load(&tracked, m)?;
            for old in [false, true] {
                let manifest = if old {
                    let Some(m) = c.legacy else { continue };
                    m
                } else {
                    m
                };
                if manifest.dimensions != query.len() {
                    return Err(dimension(&tracked, manifest.dimensions, query.len()).into());
                }
                let mut metadata = MetadataWindow::new(&tracked, manifest);
                let mut canonical = CanonicalWindow::new(&tracked, manifest);
                let mut visibility = [0; PAGE_BYTES];
                let mut loaded = None;
                for_each_visible_slot(
                    &tracked,
                    manifest,
                    c,
                    old,
                    &mut metadata,
                    &mut visibility,
                    &mut loaded,
                    |slot, entry, tail| {
                        let row = canonical.row(slot, raw.len())?;
                        let identity = CandidateIdentity {
                            index: tracked.index_identity(),
                            partition: manifest.partition,
                            generation: manifest.generation,
                            run: entry.run,
                            slot,
                            row: RowIdentity {
                                row,
                                commit_lsn: entry.lsn,
                            },
                            snapshot_lsn,
                            source: if old {
                                CandidateSource::LegacyRun
                            } else if tail {
                                CandidateSource::UnsealedTail
                            } else if entry.run == 0 {
                                CandidateSource::ServingBase
                            } else {
                                CandidateSource::CommittedRun
                            },
                        };
                        if !eligible(identity)? {
                            return Ok(());
                        }
                        canonical.vector(slot, row, &mut raw, &mut values)?;
                        tracked.before_distance(if tail {
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
                        work.exact_score_count += 1;
                        visitor(CanonicalCandidate {
                            identity,
                            values: &values,
                            distance,
                        })
                    },
                )?;
            }
            Ok(())
        };
        if let Some(partitions) = partitions {
            for (at, p) in partitions.iter().enumerate() {
                if !partitions[..at].contains(p) {
                    partition(*p)?;
                }
            }
        } else {
            let mut after = None;
            while let Some(p) = tracked.next_partition(after)? {
                partition(p)?;
                after = Some(p);
            }
        }
        Ok(())
    })();
    tracked.apply(work);
    result
}

fn canonical_visit_layout<S: ReadPages>(_: &S, dimensions: usize) -> Result<usize> {
    let raw = dimensions
        .checked_mul(4)
        .and_then(|n| n.checked_add(8))
        .ok_or_else(overflow)?;
    let mut layout = std::alloc::Layout::new::<(
        MetadataWindow<'_, S>,
        CanonicalWindow<'_, S>,
        [u8; PAGE_BYTES],
        Catalog,
        Manifest,
        Vec<u8>,
        Vec<f32>,
    )>();
    for part in [
        std::alloc::Layout::array::<u8>(raw),
        std::alloc::Layout::array::<f32>(dimensions),
    ] {
        layout = layout
            .extend(part.map_err(|_| overflow())?)
            .map_err(|_| overflow())?
            .0;
    }
    Ok(layout.pad_to_align().size())
}

// One production encoder; the observation writes into its caller's buffer.
fn encode_query(query: &[f32], query_code: &mut [i8]) {
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
}
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn quantized_query_codes_for_test(query: &[f32], output: &mut [i8]) {
    assert_eq!(query.len(), output.len());
    encode_query(query, output);
}
