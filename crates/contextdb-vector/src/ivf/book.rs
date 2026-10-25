use super::storage::DistanceKind;
use super::{
    codec,
    memory::{Invoice, bytes, corrupt, overflow},
    storage::{Manifest, PAGE_BYTES, PageKind, ReadPages, WritePages},
    types::cosine,
};
use crate::MemoryBudgetOwner;
use contextdb_core::Result;

struct BookData {
    values: Vec<f32>,
    counts: Vec<u64>,
}
pub(crate) struct Codebook {
    data: Box<BookData>,
    dimensions: usize,
    active: usize,
    pages_read: u64,
    _invoice: Invoice,
}
impl Codebook {
    pub(crate) fn empty(owner: &MemoryBudgetOwner, manifest: Manifest) -> Result<Self> {
        Self::allocate(owner, manifest, true)
    }
    fn allocate(owner: &MemoryBudgetOwner, manifest: Manifest, counts: bool) -> Result<Self> {
        let n = manifest
            .dimensions
            .checked_mul(manifest.active_lists as usize)
            .ok_or_else(overflow)?;
        let charge = bytes::<f32>(n)?
            .checked_add(bytes::<u64>(if counts {
                manifest.active_lists as usize
            } else {
                0
            })?)
            .and_then(|x| x.checked_add(std::mem::size_of::<BookData>()))
            .ok_or_else(overflow)?;
        let invoice = Invoice::reserve(owner, charge, "clustered_codebook")?;
        Ok(Self {
            data: Box::new(BookData {
                values: vec![0.0; n],
                counts: vec![
                    0;
                    if counts {
                        manifest.active_lists as usize
                    } else {
                        0
                    }
                ],
            }),
            dimensions: manifest.dimensions,
            active: manifest.active_lists as usize,
            pages_read: 0,
            _invoice: invoice,
        })
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
        Self::load_inner(owner, source, manifest, false)
    }
    pub(crate) fn pages_read(&self) -> u64 {
        self.pages_read
    }
    fn load_inner(
        owner: &MemoryBudgetOwner,
        source: &impl ReadPages,
        manifest: Manifest,
        counts: bool,
    ) -> Result<Self> {
        let mut book = Self::allocate(owner, manifest, counts)?;
        let mut page = [0u8; PAGE_BYTES];
        if !super::storage::read_page(source, manifest.key(PageKind::Codebook, 0, 0), &mut page)? {
            return Err(corrupt());
        }
        book.pages_read += 1;
        if &page[..8] != b"IVFCB001" {
            return Err(corrupt());
        }
        let values_digest: [u8; 32] = page[8..40].try_into().map_err(|_| corrupt())?;
        let counts_digest: [u8; 32] = page[40..72].try_into().map_err(|_| corrupt())?;
        if identity(manifest, values_digest, counts_digest) != manifest.codebook {
            return Err(corrupt());
        }
        let mut hash = blake3::Hasher::new();
        for (ordinal, chunk) in book.data.values.chunks_mut(PAGE_BYTES / 4).enumerate() {
            if !super::storage::read_page(
                source,
                manifest.key(PageKind::Codebook, 0, ordinal as u64 + 1),
                &mut page,
            )? {
                return Err(corrupt());
            }
            book.pages_read += 1;
            for (i, value) in chunk.iter_mut().enumerate() {
                let bits: [u8; 4] = page[i * 4..i * 4 + 4].try_into().map_err(|_| corrupt())?;
                hash.update(&bits);
                *value = f32::from_le_bytes(bits);
            }
        }
        if *hash.finalize().as_bytes() != values_digest {
            return Err(corrupt());
        }
        if counts {
            let mut hash = blake3::Hasher::new();
            for (ordinal, chunk) in book.data.counts.chunks_mut(PAGE_BYTES / 8).enumerate() {
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
    pub(crate) fn row(&self, list: usize) -> &[f32] {
        &self.data.values[list * self.dimensions..(list + 1) * self.dimensions]
    }
    pub(crate) fn seed(&mut self, list: usize, vector: &[f32]) {
        self.data.values[list * self.dimensions..(list + 1) * self.dimensions]
            .copy_from_slice(vector);
        self.normalize(list);
        self.data.counts[list] = 1;
    }
    fn normalize(&mut self, list: usize) {
        let row = &mut self.data.values[list * self.dimensions..(list + 1) * self.dimensions];
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
        self.data.counts[list] += 1;
        let count = self.data.counts[list] as f64;
        let norm = vector
            .iter()
            .map(|x| f64::from(*x).powi(2))
            .sum::<f64>()
            .sqrt();
        if norm == 0.0 {
            return Ok(());
        }
        for (dst, src) in self.data.values[list * self.dimensions..(list + 1) * self.dimensions]
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
        let mut values_hash = blake3::Hasher::new();
        for (ordinal, chunk) in self.data.values.chunks(PAGE_BYTES / 4).enumerate() {
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
        for (ordinal, chunk) in self.data.counts.chunks(PAGE_BYTES / 8).enumerate() {
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
        slot.checked_mul(raw.len() as u64).ok_or_else(overflow)?,
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
    let per_page = PAGE_BYTES / stride;
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
    if directory.tail == 0 || used == per_page {
        let id = codec::allocate_list_page(target, manifest, list)?;
        if directory.tail != 0 {
            codec::set_next_list_page(target, manifest, list, directory.tail, id)?;
        } else {
            directory.head = id;
        }
        directory.tail = id;
        used = 0;
    } else if !super::storage::read_page(
        target,
        manifest.key(PageKind::Lists, list, directory.tail - 1),
        &mut page,
    )? {
        return Err(corrupt());
    }
    page[used * stride..(used + 1) * stride].copy_from_slice(encoded);
    super::storage::put_page(
        target,
        manifest.key(PageKind::Lists, list, directory.tail - 1),
        &page,
    )?;
    set_list_page_used(target, manifest, list, directory.tail, used + 1)?;
    directory.count = count.checked_add(1).ok_or_else(overflow)?;
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
        (id - 1).checked_mul(8).ok_or_else(overflow)?,
        &mut raw,
    )?;
    usize::try_from(u64::from_le_bytes(raw)).map_err(|_| corrupt())
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
        (id - 1).checked_mul(8).ok_or_else(overflow)?,
        &(n as u64).to_le_bytes(),
    )
}
