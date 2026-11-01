use super::storage::DistanceKind;
use super::{
    codec,
    memory::{Invoice, corrupt, overflow},
    storage::{Manifest, PAGE_BYTES, PageKind, ReadPages, WritePages},
    types::cosine,
};
use crate::MemoryBudgetOwner;
use contextdb_core::Result;

struct BookData {
    values: Vec<f32>,
    counts: Vec<u64>,
}
/// Complete allocation geometry used by the codebook constructor.
#[derive(Clone, Copy, Debug)]
pub struct CodebookAllocationLayouts {
    pub control: std::alloc::Layout,
    pub values: std::alloc::Layout,
    pub counts: std::alloc::Layout,
    pub charged_bytes: usize,
}

pub fn codebook_allocation_layout(
    dimensions: usize,
    active_lists: u32,
    training_counts: bool,
) -> Result<CodebookAllocationLayouts> {
    let n = dimensions
        .checked_mul(active_lists as usize)
        .ok_or_else(overflow)?;
    let control = std::alloc::Layout::array::<BookData>(usize::from(active_lists != 0))
        .map_err(|_| overflow())?;
    let values = std::alloc::Layout::array::<f32>(n).map_err(|_| overflow())?;
    let counts = std::alloc::Layout::array::<u64>(if training_counts {
        active_lists as usize
    } else {
        0
    })
    .map_err(|_| overflow())?;
    let charged_bytes = values
        .size()
        .checked_add(counts.size())
        .and_then(|n| n.checked_add(control.size()))
        .ok_or_else(overflow)?;
    Ok(CodebookAllocationLayouts {
        control,
        values,
        counts,
        charged_bytes,
    })
}

pub(crate) struct Codebook {
    data: Option<Box<BookData>>,
    dimensions: usize,
    active: usize,
    _invoice: Option<Invoice>,
}
impl Codebook {
    pub(crate) fn empty(owner: &MemoryBudgetOwner, manifest: Manifest) -> Result<Self> {
        Self::allocate(owner, manifest, true)
    }
    fn allocate(owner: &MemoryBudgetOwner, manifest: Manifest, counts: bool) -> Result<Self> {
        Self::allocate_for(owner, manifest, counts, "clustered_codebook")
    }
    fn allocate_for(
        owner: &MemoryBudgetOwner,
        manifest: Manifest,
        counts: bool,
        operation: &'static str,
    ) -> Result<Self> {
        let layouts =
            codebook_allocation_layout(manifest.dimensions, manifest.active_lists, counts)?;
        if manifest.active_lists == 0 {
            return Ok(Self {
                data: None,
                dimensions: manifest.dimensions,
                active: 0,
                _invoice: None,
            });
        }
        let n = layouts.values.size() / std::mem::size_of::<f32>();
        let invoice = Invoice::reserve(owner, layouts.charged_bytes, operation)?;
        #[cfg(feature = "test-seams")]
        super::layout_observation::admitted(super::IvfConstructorAllocationForTest::Codebook(
            layouts,
        ));
        let book = Self {
            data: Some(Box::new(BookData {
                values: vec![0.0; n],
                counts: vec![
                    0;
                    if counts {
                        manifest.active_lists as usize
                    } else {
                        0
                    }
                ],
            })),
            dimensions: manifest.dimensions,
            active: manifest.active_lists as usize,
            _invoice: Some(invoice),
        };
        #[cfg(feature = "test-seams")]
        super::layout_observation::constructed(super::IvfConstructorAllocationForTest::Codebook(
            layouts,
        ));
        Ok(book)
    }
    pub(crate) fn load(
        owner: &MemoryBudgetOwner,
        source: &impl ReadPages,
        manifest: Manifest,
    ) -> Result<Self> {
        Self::load_inner(owner, source, manifest, true)
    }
    pub(crate) fn load_values(
        owner: &MemoryBudgetOwner,
        source: &impl ReadPages,
        manifest: Manifest,
    ) -> Result<Self> {
        source.check_cancel()?;
        if let Some(retained) = source.retained_codebook(manifest) {
            if !retained.matches(manifest) {
                return Err(corrupt());
            }
            let mut book = Self::allocate(owner, manifest, false)?;
            if book.active != 0 {
                let input = &retained.book.data().values;
                for (output, input) in book
                    .data_mut()
                    .values
                    .chunks_mut(PAGE_BYTES / 4)
                    .zip(input.chunks(PAGE_BYTES / 4))
                {
                    source.check_cancel()?;
                    output.copy_from_slice(input);
                }
            }
            source.check_cancel()?;
            return Ok(book);
        }
        Self::load_inner(owner, source, manifest, false)
    }
    fn load_inner(
        owner: &MemoryBudgetOwner,
        source: &impl ReadPages,
        manifest: Manifest,
        counts: bool,
    ) -> Result<Self> {
        Self::load_inner_for(owner, source, manifest, counts, "clustered_codebook")
    }
    fn load_inner_for(
        owner: &MemoryBudgetOwner,
        source: &impl ReadPages,
        manifest: Manifest,
        counts: bool,
        operation: &'static str,
    ) -> Result<Self> {
        let mut book = Self::allocate_for(owner, manifest, counts, operation)?;
        if book.active == 0 {
            return Ok(book);
        }
        let counts_digest = read_values(source, manifest, Some(&mut book.data_mut().values))?;
        let mut page = [0; PAGE_BYTES];
        if counts {
            let mut hash = blake3::Hasher::new();
            for (ordinal, chunk) in book
                .data_mut()
                .counts
                .chunks_mut(PAGE_BYTES / 8)
                .enumerate()
            {
                if !super::storage::read_page(
                    source,
                    manifest.key(PageKind::Accumulators, 0, ordinal as u64),
                    &mut page,
                )? {
                    return Err(corrupt());
                }
                for (i, value) in chunk.iter_mut().enumerate() {
                    let bits: [u8; 8] = page[i * 8..i * 8 + 8].try_into().map_err(|_| corrupt())?;
                    hash.update(&bits);
                    *value = u64::from_le_bytes(bits);
                }
            }
            if *hash.finalize().as_bytes() != counts_digest {
                return Err(corrupt());
            }
        }
        Ok(book)
    }
    pub(crate) fn verify_values(source: &impl ReadPages, m: Manifest) -> Result<()> {
        read_values(source, m, None).map(|_| ())
    }
    pub(crate) fn nearest(&self, source: &impl ReadPages, vector: &[f32]) -> Result<usize> {
        let mut best = 0;
        let mut distance = f64::INFINITY;
        for list in 0..self.active {
            source.check_cancel()?;
            source.before_distance(DistanceKind::Centroid)?;
            let next = cosine(self.row(list), vector);
            if next < distance {
                best = list;
                distance = next;
            }
        }
        Ok(best)
    }
    fn data(&self) -> &BookData {
        self.data.as_deref().expect("active codebook has data")
    }
    fn data_mut(&mut self) -> &mut BookData {
        self.data.as_deref_mut().expect("active codebook has data")
    }
    pub(crate) fn row(&self, list: usize) -> &[f32] {
        &self.data().values[list * self.dimensions..(list + 1) * self.dimensions]
    }
    pub(crate) fn seed(&mut self, list: usize, vector: &[f32]) {
        let dimensions = self.dimensions;
        self.data_mut().values[list * dimensions..(list + 1) * dimensions].copy_from_slice(vector);
        self.normalize(list);
        self.data_mut().counts[list] = 1;
    }
    fn normalize(&mut self, list: usize) {
        let dimensions = self.dimensions;
        let row = &mut self.data_mut().values[list * dimensions..(list + 1) * dimensions];
        let norm = row
            .iter()
            .map(|x| f64::from(*x).powi(2))
            .sum::<f64>()
            .sqrt();
        if norm > 0.0 {
            for x in row {
                *x = (f64::from(*x) / norm) as f32;
            }
        }
    }
    pub(crate) fn train(&mut self, source: &impl ReadPages, vector: &[f32]) -> Result<()> {
        let list = self.nearest(source, vector)?;
        let next = self.data().counts[list]
            .checked_add(1)
            .ok_or_else(corrupt)?;
        self.data_mut().counts[list] = next;
        let count = next as f64;
        let norm = vector
            .iter()
            .map(|x| f64::from(*x).powi(2))
            .sum::<f64>()
            .sqrt();
        if norm == 0.0 {
            return Ok(());
        }
        let dimensions = self.dimensions;
        for (dst, src) in self.data_mut().values[list * dimensions..(list + 1) * dimensions]
            .iter_mut()
            .zip(vector)
        {
            *dst = (f64::from(*dst) + (f64::from(*src) / norm - f64::from(*dst)) / count) as f32;
        }
        self.normalize(list);
        Ok(())
    }
    pub(crate) fn store(
        &self,
        target: &mut impl WritePages,
        manifest: Manifest,
    ) -> Result<[u8; 32]> {
        if self.active == 0 {
            return Ok([0; 32]);
        }
        let mut values_hash = blake3::Hasher::new();
        for (ordinal, chunk) in self.data().values.chunks(PAGE_BYTES / 4).enumerate() {
            let mut page = [0u8; PAGE_BYTES];
            for (i, value) in chunk.iter().enumerate() {
                let bits = value.to_le_bytes();
                page[i * 4..i * 4 + 4].copy_from_slice(&bits);
                values_hash.update(&bits);
            }
            super::storage::put_page(
                target,
                manifest.key(PageKind::Codebook, 0, ordinal as u64 + 1),
                &page,
            )?;
        }
        let mut counts_hash = blake3::Hasher::new();
        for (ordinal, chunk) in self.data().counts.chunks(PAGE_BYTES / 8).enumerate() {
            let mut page = [0u8; PAGE_BYTES];
            for (i, value) in chunk.iter().enumerate() {
                let bits = value.to_le_bytes();
                page[i * 8..i * 8 + 8].copy_from_slice(&bits);
                counts_hash.update(&bits);
            }
            super::storage::put_page(
                target,
                manifest.key(PageKind::Accumulators, 0, ordinal as u64),
                &page,
            )?;
        }
        let values_digest = *values_hash.finalize().as_bytes();
        let counts_digest = *counts_hash.finalize().as_bytes();
        let mut header = [0u8; PAGE_BYTES];
        header[..8].copy_from_slice(b"IVFCB001");
        header[8..40].copy_from_slice(&values_digest);
        header[40..72].copy_from_slice(&counts_digest);
        super::storage::put_page(target, manifest.key(PageKind::Codebook, 0, 0), &header)?;
        Ok(identity(manifest, values_digest, counts_digest))
    }
}

fn identity(manifest: Manifest, values: [u8; 32], counts: [u8; 32]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"ivf-codebook-v1");
    hash.update(&(manifest.dimensions as u64).to_le_bytes());
    hash.update(&manifest.active_lists.to_le_bytes());
    hash.update(&values);
    hash.update(&counts);
    *hash.finalize().as_bytes()
}

pub(crate) fn read_vector(
    source: &impl ReadPages,
    manifest: Manifest,
    slot: u64,
    raw: &mut [u8],
    values: &mut [f32],
) -> Result<u64> {
    codec::read(
        source,
        manifest,
        PageKind::Canonical,
        0,
        slot.checked_mul(raw.len() as u64).ok_or_else(corrupt)?,
        raw,
    )?;
    let row = u64::from_le_bytes(raw[..8].try_into().map_err(|_| corrupt())?);
    for (i, x) in values.iter_mut().enumerate() {
        *x = f32::from_le_bytes(
            raw[8 + i * 4..12 + i * 4]
                .try_into()
                .map_err(|_| corrupt())?,
        );
    }
    Ok(row)
}
/// One logical list block for a declared dimension, as (records it holds, fixed pages it
/// spans). Records that fit a page pack into one page; a wider record is alone in its block.
/// Links, used counts and the allocator stay logical.
pub fn list_block_layout(dimensions: usize) -> Option<(usize, usize)> {
    let stride = dimensions.checked_add(16)?;
    Some(((PAGE_BYTES / stride).max(1), stride.div_ceil(PAGE_BYTES)))
}
pub(crate) fn append_list(
    target: &mut impl WritePages,
    manifest: Manifest,
    list: u32,
    slot: u64,
    row: u64,
    vector: &[f32],
    encoded: &mut [u8],
) -> Result<()> {
    let mut directory = codec::directory(target, manifest, list)?;
    let count = directory.count;
    let stride = encoded.len();
    let (per_page, block_pages) = list_block_layout(manifest.dimensions).ok_or_else(corrupt)?;
    encoded[..8].copy_from_slice(&slot.to_le_bytes());
    encoded[8..16].copy_from_slice(&row.to_le_bytes());
    let norm = vector
        .iter()
        .map(|x| f64::from(*x).powi(2))
        .sum::<f64>()
        .sqrt();
    for (dst, src) in encoded[16..].iter_mut().zip(vector) {
        *dst = if norm == 0.0 {
            0
        } else {
            ((f64::from(*src) / norm * 127.0)
                .round()
                .clamp(-127.0, 127.0) as i8) as u8
        };
    }
    // Each run owns a directory, while immutable payload pages have generation-global identities.
    // A partial terminal page is padded when runs are joined; links carry its used-entry count.
    let mut page = [0u8; PAGE_BYTES];
    let mut used = 0usize;
    if directory.tail != 0 {
        used = list_page_used(target, manifest, list, directory.tail)?;
    }
    let fresh = directory.tail == 0 || used == per_page;
    if fresh {
        let id = codec::allocate_list_page(target, manifest, list)?;
        if directory.tail != 0 {
            codec::set_next_list_page(target, manifest, list, directory.tail, id)?;
        } else {
            directory.head = id;
        }
        directory.tail = id;
        used = 0;
    }
    let first_page = (directory.tail - 1)
        .checked_mul(block_pages as u64)
        .ok_or_else(corrupt)?;
    let key = manifest.key(PageKind::Lists, list, first_page);
    if !fresh && !super::storage::read_page(target, key, &mut page)? {
        return Err(corrupt());
    }
    if stride <= PAGE_BYTES {
        page[used * stride..(used + 1) * stride].copy_from_slice(encoded);
        super::storage::put_page(target, key, &page)?;
    } else {
        let offset = first_page
            .checked_mul(PAGE_BYTES as u64)
            .ok_or_else(corrupt)?;
        codec::write(target, manifest, PageKind::Lists, list, offset, encoded)?;
    }
    set_list_page_used(target, manifest, list, directory.tail, used + 1)?;
    directory.count = count.checked_add(1).ok_or_else(corrupt)?;
    codec::set_directory(target, manifest, list, directory)
}

pub(crate) fn list_page_used(
    source: &impl ReadPages,
    m: Manifest,
    list: u32,
    id: u64,
) -> Result<usize> {
    let mut raw = [0; 8];
    codec::read(
        source,
        m,
        PageKind::ListUsed,
        list,
        id.checked_sub(1)
            .and_then(|v| v.checked_mul(8))
            .ok_or_else(corrupt)?,
        &mut raw,
    )?;
    let (capacity, _) = list_block_layout(m.dimensions).ok_or_else(corrupt)?;
    let used = usize::try_from(u64::from_le_bytes(raw)).map_err(|_| corrupt())?;
    if used > capacity {
        let ordinal = id
            .checked_sub(1)
            .and_then(|n| n.checked_mul(8))
            .ok_or_else(corrupt)?
            / PAGE_BYTES as u64;
        return Err(super::memory::corrupt_page(m.key(
            PageKind::ListUsed,
            list,
            ordinal,
        )));
    }
    Ok(used)
}
fn set_list_page_used(
    tx: &mut impl WritePages,
    m: Manifest,
    list: u32,
    id: u64,
    n: usize,
) -> Result<()> {
    codec::write(
        tx,
        m,
        PageKind::ListUsed,
        list,
        id.checked_sub(1)
            .and_then(|v| v.checked_mul(8))
            .ok_or_else(corrupt)?,
        &(n as u64).to_le_bytes(),
    )
}

fn read_values(
    source: &impl ReadPages,
    m: Manifest,
    mut decoded: Option<&mut [f32]>,
) -> Result<[u8; 32]> {
    let key = m.key(PageKind::Codebook, 0, 0);
    let mut page = [0; PAGE_BYTES];
    if !super::storage::read_page(source, key, &mut page)? || &page[..8] != b"IVFCB001" {
        return Err(super::memory::corrupt_page(key));
    }
    let values_digest: [u8; 32] = page[8..40]
        .try_into()
        .map_err(|_| super::memory::corrupt_page(key))?;
    let counts_digest: [u8; 32] = page[40..72]
        .try_into()
        .map_err(|_| super::memory::corrupt_page(key))?;
    if identity(m, values_digest, counts_digest) != m.codebook {
        return Err(super::memory::corrupt_page(key));
    }
    let count = m
        .dimensions
        .checked_mul(m.active_lists as usize)
        .ok_or_else(corrupt)?;
    if decoded.as_ref().is_some_and(|v| v.len() != count) {
        return Err(super::memory::corrupt_page(key));
    }
    let mut hash = blake3::Hasher::new();
    let per = PAGE_BYTES / 4;
    let pages = count.div_ceil(per);
    for ordinal in 0..pages {
        let key = m.key(
            PageKind::Codebook,
            0,
            (ordinal as u64).checked_add(1).ok_or_else(corrupt)?,
        );
        if !super::storage::read_page(source, key, &mut page)? {
            return Err(super::memory::corrupt_page(key));
        }
        let begin = ordinal.checked_mul(per).ok_or_else(corrupt)?;
        let n = (count - begin).min(per);
        hash.update(&page[..n * 4]);
        if let Some(values) = decoded.as_mut() {
            for (i, dst) in values[begin..begin + n].iter_mut().enumerate() {
                *dst = f32::from_le_bytes(
                    page[i * 4..i * 4 + 4]
                        .try_into()
                        .map_err(|_| super::memory::corrupt_page(key))?,
                );
            }
        }
    }
    if *hash.finalize().as_bytes() != values_digest {
        return Err(super::memory::corrupt_page(m.key(PageKind::Codebook, 0, 1)));
    }
    Ok(counts_digest)
}

/// One immutable codebook, loaded by the existing checked constructor and paid
/// by its original maintenance owner. Its holder owns the separate metadata
/// control; this object retains the actual values invoice until physical free.
/// Query constructors may consume these values while admitting their own real
/// operation object on the original reader owner.
pub struct RetainedCodebook {
    manifest: Manifest,
    book: Codebook,
}
impl RetainedCodebook {
    pub fn load(
        owner: &MemoryBudgetOwner,
        source: &impl ReadPages,
        manifest: Manifest,
    ) -> Result<Self> {
        Ok(Self {
            manifest,
            book: Codebook::load_inner_for(
                owner,
                source,
                manifest,
                false,
                "clustered_retained_codebook",
            )?,
        })
    }
    pub fn manifest(&self) -> Manifest {
        self.manifest
    }
    pub fn charged_bytes(&self) -> usize {
        self.book
            ._invoice
            .as_ref()
            .map_or(0, Invoice::charged_bytes)
    }
    #[cfg(feature = "test-seams")]
    pub fn original_owner_for_test(&self) -> usize {
        self.book
            ._invoice
            .as_ref()
            .map_or(0, Invoice::original_owner_for_test)
    }
    pub fn matches(&self, manifest: Manifest) -> bool {
        self.manifest.partition == manifest.partition
            && self.manifest.generation == manifest.generation
            && self.manifest.dimensions == manifest.dimensions
            && self.manifest.active_lists == manifest.active_lists
            && self.manifest.codebook == manifest.codebook
    }
}

/// The current tuple's recorded old codebook, without opening any centroid or
/// directory payload. An already quarantined legacy portion uses the existing
/// exhaustive route and needs no unusable centroid object.
pub fn retained_codebook_manifest(
    source: &impl ReadPages,
    partition: u64,
) -> Result<Option<Manifest>> {
    let Some(manifest) = source.manifest(partition)? else {
        return Ok(None);
    };
    let catalog = super::catalog::Catalog::load(source, manifest)?;
    Ok(if catalog.legacy_unserviceable {
        None
    } else {
        catalog.legacy.filter(|manifest| manifest.active_lists != 0)
    })
}
