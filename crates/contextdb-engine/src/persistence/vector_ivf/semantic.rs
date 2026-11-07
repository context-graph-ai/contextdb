//! Semantic record ownership and finite dependency expansion for native seals.
//! The supplying view reads the one actual borrowed native transaction.

#![expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
use super::*;
use contextdb_vector::ivf::storage::{BuildState, Manifest, PageKey, PageKind};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Records {
    pub first: u64,
    pub count: u32,
}

pub(super) trait SemanticView {
    fn checkpoint(&self) -> Result<()>;
    fn generation_manifest(&self, generation: u64) -> Result<Manifest>;
    fn reservoir_samples(&self, generation: u64) -> Result<u64>;
    fn payload(&self, key: PageKey, out: &mut [u8; PAGE_BYTES]) -> Result<bool>;
}

pub(super) trait SealView: SemanticView {
    /// A native pending marker, never a decoded/resident set of dirty pages.
    fn mark(&self, key: PageKey) -> Result<()>;
}

fn word(bytes: &[u8], at: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        bytes
            .get(at..at + 8)
            .ok_or_else(corrupt)?
            .try_into()
            .map_err(|_| corrupt())?,
    ))
}
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or_else(corrupt)
}
fn mul(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b).ok_or_else(corrupt)
}
fn records(first: u64, count: u64) -> Result<Records> {
    Ok(Records {
        first,
        count: u32::try_from(count).map_err(|_| corrupt())?,
    })
}
fn clipped(first: u64, capacity: u64, total: u64) -> Result<Records> {
    records(first, total.saturating_sub(first).min(capacity))
}
fn packed(ordinal: u64, width: u64, total: u64) -> Result<Records> {
    let capacity = (PAGE_BYTES as u64) / width;
    clipped(mul(ordinal, capacity)?, capacity, total)
}
fn contiguous(ordinal: u64, width: u64, total: u64) -> Result<Records> {
    let begin = mul(ordinal, PAGE_BYTES as u64)?;
    // A continuation-only page owns zero prefixes but still intersects a
    // real record. A page wholly beyond the canonical extent does not.
    if begin >= mul(total, width)? {
        return Err(corrupt());
    }
    let end = add(begin, PAGE_BYTES as u64)?;
    let first = begin.div_ceil(width);
    records(first, end.div_ceil(width).min(total).saturating_sub(first))
}
fn scalar(
    view: &impl SemanticView,
    m: Manifest,
    kind: PageKind,
    list: u32,
    offset: u64,
    window: &mut [u8; PAGE_BYTES],
) -> Result<u64> {
    let key = m.key(kind, list, offset / (PAGE_BYTES as u64));
    view.checkpoint()?;
    if !view.payload(key, window)? {
        return Err(corrupt());
    }
    word(window, (offset % (PAGE_BYTES as u64)) as usize)
}
fn allocated(
    view: &impl SemanticView,
    m: Manifest,
    list: u32,
    window: &mut [u8; PAGE_BYTES],
) -> Result<u64> {
    if list >= m.lists {
        return Err(corrupt());
    }
    scalar(
        view,
        m,
        PageKind::ListAllocator,
        0,
        mul(u64::from(list), 8)?,
        window,
    )
}

pub(super) fn derive(
    view: &impl SemanticView,
    key: PageKey,
    payload: &[u8; PAGE_BYTES],
    related: &mut [u8; PAGE_BYTES],
) -> Result<Records> {
    view.checkpoint()?;
    if key.partition != 0 {
        return Err(corrupt());
    }
    if !matches!(
        key.kind,
        PageKind::Directory | PageKind::DirectoryPresence | PageKind::RowCopies
    ) && key.run != 0
    {
        return Err(corrupt());
    }
    if !matches!(
        key.kind,
        PageKind::Lists | PageKind::ListUsed | PageKind::ListLinks | PageKind::Catalog
    ) && key.list != 0
    {
        return Err(corrupt());
    }
    // Sparse generation-neutral journal records use padded address units.
    if matches!(key.kind, PageKind::RowVersions | PageKind::RowCopies) {
        if key.generation != 0
            || key.list != 0
            || (key.kind == PageKind::RowVersions && key.run != 0)
        {
            return Err(corrupt());
        }
        let (width, flag_at) = if key.kind == PageKind::RowVersions {
            (40, 32)
        } else {
            (24, 0)
        };
        let capacity = PAGE_BYTES / width;
        if payload[capacity * width..].iter().any(|b| *b != 0) {
            return Err(corrupt());
        }
        let first = mul(key.ordinal, capacity as u64)?;
        let mut initialized = 0u64;
        for (at, record) in payload[..capacity * width].chunks_exact(width).enumerate() {
            match word(record, flag_at)? {
                0 => {
                    if record.iter().any(|b| *b != 0) {
                        return Err(corrupt());
                    }
                }
                1 | 3 => {
                    // The final sparse address page can have unused words
                    // after u64::MAX, but cannot initialize such an address.
                    add(first, at as u64)?;
                    initialized = add(initialized, 1)?;
                }
                _ => return Err(corrupt()),
            }
        }
        return records(first, initialized);
    }
    let m = view.generation_manifest(key.generation)?;
    if m.partition != key.partition || m.generation != key.generation {
        return Err(corrupt());
    }
    match key.kind {
        PageKind::Canonical => {
            contiguous(key.ordinal, add(mul(m.dimensions as u64, 4)?, 8)?, m.slots)
        }
        PageKind::Lists => {
            if key.list >= m.lists || key.run != 0 {
                return Err(corrupt());
            }
            let (capacity, block_pages) =
                contextdb_vector::ivf::list_block_layout(m.dimensions).ok_or_else(corrupt)?;
            // The key is physical; allocation, links and used counts are per logical block.
            let block = key.ordinal / block_pages as u64;
            let allocation_end = allocated(view, m, key.list, related)?;
            if block >= allocation_end {
                return Err(corrupt());
            }
            // Every list page, including its terminal page, has a real link
            // record. Authenticate that dependency even when the caller's
            // known record count does not require following the final link.
            let next = scalar(
                view,
                m,
                PageKind::ListLinks,
                key.list,
                mul(block, 8)?,
                related,
            )?;
            if next > allocation_end || next == add(block, 1)? {
                return Err(corrupt());
            }
            // Logical block l = allocated id - 1; metadata offset is l*8.
            let used = scalar(
                view,
                m,
                PageKind::ListUsed,
                key.list,
                mul(block, 8)?,
                related,
            )?;
            if used > capacity as u64 {
                return Err(corrupt());
            }
            // A continuation page owns no record prefix, as a canonical one does not.
            if !key.ordinal.is_multiple_of(block_pages as u64) {
                return records(0, 0);
            }
            let first = if used == 0 { 0 } else { word(payload, 0)? };
            if used != 0 && first >= m.slots {
                return Err(corrupt());
            }
            records(first, used)
        }
        PageKind::Directory => packed(key.ordinal, 24, u64::from(m.lists)),
        PageKind::DirectoryPresence => clipped(
            mul(key.ordinal, (PAGE_BYTES * 8) as u64)?,
            (PAGE_BYTES * 8) as u64,
            u64::from(m.lists).div_ceil((PAGE_BYTES / 24) as u64),
        ),
        PageKind::Visibility => clipped(
            mul(key.ordinal, (PAGE_BYTES * 8) as u64)?,
            (PAGE_BYTES * 8) as u64,
            m.slots,
        ),
        PageKind::Codebook => {
            if key.ordinal == 0 {
                records(0, 1)
            } else {
                packed(
                    key.ordinal.checked_sub(1).ok_or_else(corrupt)?,
                    4,
                    mul(m.dimensions as u64, u64::from(m.active_lists))?,
                )
            }
        }
        PageKind::Accumulators => packed(key.ordinal, 8, u64::from(m.active_lists)),
        PageKind::Reservoir => packed(
            key.ordinal,
            8,
            m.slots.min(view.reservoir_samples(key.generation)?),
        ),
        PageKind::Catalog => {
            if (key.list == 0 && key.ordinal == 0) || (key.list == 1 && key.ordinal != 0) {
                records(key.ordinal, 1)
            } else {
                Err(corrupt())
            }
        }
        PageKind::SlotMetadata => contiguous(key.ordinal, 40, m.slots),
        PageKind::Tail => {
            if !view.payload(m.key(PageKind::Catalog, 0, 0), related)? {
                return Err(corrupt());
            }
            if word(related, 0)? != u64::from_le_bytes(*b"IVFCAT02") {
                return Err(corrupt());
            }
            packed(key.ordinal, 8, word(related, 7 * 8)?)
        }
        PageKind::ListLinks | PageKind::ListUsed => {
            let count = allocated(view, m, key.list, related)?;
            let range = packed(key.ordinal, 8, count)?;
            records(add(range.first, 1)?, u64::from(range.count))
        }
        PageKind::ListAllocator => packed(key.ordinal, 8, u64::from(m.lists)),
        PageKind::RowVersions | PageKind::RowCopies => Err(corrupt()),
    }
}

/// Stream the changed address extent, with work admission before every marker.
/// Existing sparse/future-build pages are not fabricated by dependency expansion.
fn mark_present(view: &impl SealView, key: PageKey, window: &mut [u8; PAGE_BYTES]) -> Result<()> {
    view.checkpoint()?;
    if view.payload(key, window)? {
        view.mark(key)?;
    }
    Ok(())
}
fn extent(
    view: &impl SealView,
    m: Manifest,
    kind: PageKind,
    list: u32,
    width: u64,
    before: u64,
    after: u64,
    ordinal_bias: u64,
    window: &mut [u8; PAGE_BYTES],
) -> Result<()> {
    if before == after {
        return Ok(());
    }
    let lo = before.min(after);
    let hi = before.max(after);
    let first = mul(lo, width)? / (PAGE_BYTES as u64);
    let last = mul(hi.checked_sub(1).ok_or_else(corrupt)?, width)? / (PAGE_BYTES as u64);
    for ordinal in first..=last {
        mark_present(view, m.key(kind, list, add(ordinal, ordinal_bias)?), window)?;
    }
    Ok(())
}

/// Changes to final controls reseal existing payload boundaries even when
/// kernel put_page did not rewrite those payloads in this operation.
pub(super) fn manifest_dependencies(
    view: &impl SealView,
    before: Option<Manifest>,
    after: Manifest,
    window: &mut [u8; PAGE_BYTES],
) -> Result<()> {
    let old = before.filter(|m| m.generation == after.generation);
    if let Some(old) = old
        && (old.dimensions != after.dimensions
            || old.lists != after.lists
            || old.slots > after.slots)
    {
        return Err(corrupt());
    }
    let old_slots = old.map_or(0, |m| m.slots);
    for (kind, width) in [
        (
            PageKind::Canonical,
            add(mul(after.dimensions as u64, 4)?, 8)?,
        ),
        (PageKind::SlotMetadata, 40),
    ] {
        extent(
            view,
            after,
            kind,
            0,
            width,
            old_slots,
            after.slots,
            0,
            window,
        )?;
    }
    // A bit range is indexed in bits, not in payload-byte records.
    let old_bits = old_slots.div_ceil(8);
    let new_bits = after.slots.div_ceil(8);
    extent(
        view,
        after,
        PageKind::Visibility,
        0,
        1,
        old_bits,
        new_bits,
        0,
        window,
    )?;
    // Count can change inside the same final byte; always mark that boundary.
    if old_slots != after.slots && after.slots != 0 {
        mark_present(
            view,
            after.key(
                PageKind::Visibility,
                0,
                (after.slots - 1) / (PAGE_BYTES as u64 * 8),
            ),
            window,
        )?;
        if old_slots != 0 {
            mark_present(
                view,
                after.key(
                    PageKind::Visibility,
                    0,
                    (old_slots - 1) / (PAGE_BYTES as u64 * 8),
                ),
                window,
            )?;
        }
    }
    let old_active = old.map_or(0, |m| u64::from(m.active_lists));
    let new_active = u64::from(after.active_lists);
    extent(
        view,
        after,
        PageKind::Accumulators,
        0,
        8,
        old_active,
        new_active,
        0,
        window,
    )?;
    extent(
        view,
        after,
        PageKind::Codebook,
        0,
        4,
        mul(old_active, after.dimensions as u64)?,
        mul(new_active, after.dimensions as u64)?,
        1,
        window,
    )?;
    if old_active != new_active {
        mark_present(view, after.key(PageKind::Codebook, 0, 0), window)?;
    }
    Ok(())
}

pub(super) fn build_dependencies(
    view: &impl SealView,
    before: Option<BuildState>,
    after: BuildState,
    window: &mut [u8; PAGE_BYTES],
) -> Result<()> {
    manifest_dependencies(view, before.map(|b| b.manifest), after.manifest, window)?;
    let before = before
        .filter(|b| b.manifest.generation == after.manifest.generation)
        .map_or(0, |b| {
            b.manifest.slots.min(b.training.reservoir_samples as u64)
        });
    let after_count = after
        .manifest
        .slots
        .min(after.training.reservoir_samples as u64);
    extent(
        view,
        after.manifest,
        PageKind::Reservoir,
        0,
        8,
        before,
        after_count,
        0,
        window,
    )
}

/// Inspect only old/new fixed payload words, never a directory/list traversal.
pub(super) fn payload_dependencies(
    view: &impl SealView,
    key: PageKey,
    before: &[u8; PAGE_BYTES],
    after: &[u8; PAGE_BYTES],
    window: &mut [u8; PAGE_BYTES],
) -> Result<()> {
    match key.kind {
        PageKind::ListUsed => {
            let m = view.generation_manifest(key.generation)?;
            for (at, (old, new)) in before
                .chunks_exact(8)
                .zip(after.chunks_exact(8))
                .enumerate()
            {
                if old != new {
                    let block = add(mul(key.ordinal, (PAGE_BYTES / 8) as u64)?, at as u64)?;
                    let (_, block_pages) = contextdb_vector::ivf::list_block_layout(m.dimensions)
                        .ok_or_else(corrupt)?;
                    let first_page = mul(block, block_pages as u64)?;
                    // Every page of the block validates against this used count.
                    for page in 0..block_pages as u64 {
                        mark_present(
                            view,
                            m.key(PageKind::Lists, key.list, add(first_page, page)?),
                            window,
                        )?;
                    }
                }
            }
        }
        PageKind::ListAllocator => {
            let m = view.generation_manifest(key.generation)?;
            for at in 0..PAGE_BYTES / 8 {
                let old = word(before, at * 8)?;
                let new = word(after, at * 8)?;
                if old == new {
                    continue;
                }
                // Each allocated list page contains at least one retained
                // vector entry. Refuse a damaged extent before streaming it.
                if old > m.slots || new > m.slots {
                    return Err(corrupt());
                }
                let list = add(mul(key.ordinal, (PAGE_BYTES / 8) as u64)?, at as u64)?;
                if list >= u64::from(m.lists) {
                    return Err(corrupt());
                }
                for kind in [PageKind::ListLinks, PageKind::ListUsed] {
                    extent(
                        view,
                        m,
                        kind,
                        u32::try_from(list).map_err(|_| corrupt())?,
                        8,
                        old,
                        new,
                        0,
                        window,
                    )?;
                }
            }
        }
        PageKind::Catalog if key.list == 0 && key.ordinal == 0 => {
            let m = view.generation_manifest(key.generation)?;
            let old_end = word(before, 7 * 8)?;
            let new_end = word(after, 7 * 8)?;
            if old_end > m.slots || new_end > m.slots {
                return Err(corrupt());
            }
            extent(view, m, PageKind::Tail, 0, 8, old_end, new_end, 0, window)?;
        }
        PageKind::Canonical
        | PageKind::Lists
        | PageKind::Directory
        | PageKind::DirectoryPresence
        | PageKind::Visibility
        | PageKind::Codebook
        | PageKind::Accumulators
        | PageKind::Reservoir
        | PageKind::Catalog
        | PageKind::SlotMetadata
        | PageKind::Tail
        | PageKind::ListLinks
        | PageKind::RowVersions
        | PageKind::RowCopies => {}
    }
    Ok(())
}
