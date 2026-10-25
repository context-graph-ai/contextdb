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
    _invoice: Invoice,
}
impl Codebook {
    pub(crate) fn empty(owner: &MemoryBudgetOwner, manifest: Manifest) -> Result<Self> {
        let n = manifest
            .dimensions
            .checked_mul(manifest.active_lists as usize)
            .ok_or_else(overflow)?;
        let charge = bytes::<f32>(n)?
            .checked_add(bytes::<u64>(manifest.active_lists as usize)?)
            .and_then(|x| x.checked_add(std::mem::size_of::<BookData>()))
            .ok_or_else(overflow)?;
        let invoice = Invoice::reserve(owner, charge, "clustered_codebook")?;
        Ok(Self {
            data: Box::new(BookData {
                values: vec![0.0; n],
                counts: vec![0; manifest.active_lists as usize],
            }),
            dimensions: manifest.dimensions,
            active: manifest.active_lists as usize,
            _invoice: invoice,
        })
    }
    pub(crate) fn load(
        owner: &MemoryBudgetOwner,
        source: &impl ReadPages,
        manifest: Manifest,
    ) -> Result<Self> {
        let mut book = Self::empty(owner, manifest)?;
        let mut page = [0u8; PAGE_BYTES];
        for (ordinal, chunk) in book.data.values.chunks_mut(PAGE_BYTES / 4).enumerate() {
            if !source.read_admitted_page(
                manifest.key(PageKind::Codebook, 0, ordinal as u64),
                &mut page,
            )? {
                return Err(corrupt());
            }
            for (i, value) in chunk.iter_mut().enumerate() {
                *value =
                    f32::from_le_bytes(page[i * 4..i * 4 + 4].try_into().map_err(|_| corrupt())?);
            }
        }
        for (ordinal, chunk) in book.data.counts.chunks_mut(PAGE_BYTES / 8).enumerate() {
            page.fill(0);
            source.read_admitted_page(
                manifest.key(PageKind::Accumulators, 0, ordinal as u64),
                &mut page,
            )?;
            for (i, value) in chunk.iter_mut().enumerate() {
                *value =
                    u64::from_le_bytes(page[i * 8..i * 8 + 8].try_into().map_err(|_| corrupt())?);
            }
        }
        let mut hash = blake3::Hasher::new();
        for value in &book.data.values {
            hash.update(&value.to_le_bytes());
        }
        if *hash.finalize().as_bytes() != manifest.codebook {
            return Err(corrupt());
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
        let mut hash = blake3::Hasher::new();
        for (ordinal, chunk) in self.data.values.chunks(PAGE_BYTES / 4).enumerate() {
            let mut page = [0u8; PAGE_BYTES];
            for (i, value) in chunk.iter().enumerate() {
                let b = value.to_le_bytes();
                page[i * 4..i * 4 + 4].copy_from_slice(&b);
                hash.update(&b);
            }
            target.put_page(manifest.key(PageKind::Codebook, 0, ordinal as u64), &page)?;
        }
        for (ordinal, chunk) in self.data.counts.chunks(PAGE_BYTES / 8).enumerate() {
            let mut page = [0u8; PAGE_BYTES];
            for (i, value) in chunk.iter().enumerate() {
                page[i * 8..i * 8 + 8].copy_from_slice(&value.to_le_bytes());
            }
            target.put_page(
                manifest.key(PageKind::Accumulators, 0, ordinal as u64),
                &page,
            )?;
        }
        Ok(*hash.finalize().as_bytes())
    }
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
    let count = codec::count(target, manifest, list)?;
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
    let offset =
        (count / per_page as u64) * PAGE_BYTES as u64 + (count % per_page as u64) * stride as u64;
    codec::write(target, manifest, PageKind::Lists, list, offset, encoded)?;
    codec::set_count(target, manifest, list, count + 1)
}
