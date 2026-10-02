use crate::memory_budget::MemoryBudget;
use contextdb_core::{Lsn, RowId, TxId, VectorEntry, VectorIndexRef, VectorQuantization};
use parking_lot::Mutex;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const HNSW_HEADER_BYTES: usize = 12;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StoredVectorEntry {
    pub row_id: RowId,
    pub vector: StoredVector,
    pub created_tx: TxId,
    pub deleted_tx: Option<TxId>,
    pub lsn: Lsn,
}

impl StoredVectorEntry {
    pub fn from_vector_entry(entry: VectorEntry, quantization: VectorQuantization) -> Self {
        Self {
            row_id: entry.row_id,
            vector: StoredVector::from_f32(&entry.vector, quantization),
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            lsn: entry.lsn,
        }
    }

    pub fn from_vector_entry_ref(entry: &VectorEntry, quantization: VectorQuantization) -> Self {
        Self {
            row_id: entry.row_id,
            vector: StoredVector::from_f32(&entry.vector, quantization),
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            lsn: entry.lsn,
        }
    }

    pub fn to_vector_entry(&self, index: VectorIndexRef) -> VectorEntry {
        VectorEntry {
            index,
            row_id: self.row_id,
            vector: self.vector.to_f32(),
            created_tx: self.created_tx,
            deleted_tx: self.deleted_tx,
            lsn: self.lsn,
        }
    }

    pub fn estimated_bytes(&self) -> usize {
        self.vector.storage_bytes()
    }

    /// Legacy void moves retain the body-only caller admission protocol even
    /// when their source was promoted by a previous fallible open.
    pub(crate) fn owned_copy(&self) -> Self {
        Self {
            row_id: self.row_id,
            vector: self.vector.owned_copy(),
            created_tx: self.created_tx,
            deleted_tx: self.deleted_tx,
            lsn: self.lsn,
        }
    }

    pub fn visible_at(&self, snapshot: contextdb_core::SnapshotId) -> bool {
        self.created_tx.0 <= snapshot.0 && self.deleted_tx.is_none_or(|tx| tx.0 > snapshot.0)
    }
}

/// Immutable raw bytes shared by distinct MVCC memberships. The reservation is
/// attached once after admission, and belongs to the allocation rather than any
/// partition. Body and shared-allocation header charges both survive registry
/// removal until the final holder drops. Declaration order drops the Vec before
/// returning its charge.
#[derive(Debug)]
pub(crate) struct RawVectorPayload<T> {
    values: Vec<T>,
    charge: Mutex<Option<RawPayloadCharge>>,
}

#[derive(Debug)]
struct RawPayloadCharge {
    body_bytes: usize,
    header_bytes: usize,
    accountant: Arc<dyn MemoryBudget>,
    /// Promotion can admit a header through a later open's accountant while
    /// the legacy body keeps the funder that already owns its reservation.
    header_accountant: Option<Arc<dyn MemoryBudget>>,
    live_body_bytes: Arc<AtomicUsize>,
    live_header_bytes: Arc<AtomicUsize>,
}

impl Drop for RawPayloadCharge {
    fn drop(&mut self) {
        let previous = self
            .live_body_bytes
            .fetch_sub(self.body_bytes, Ordering::SeqCst);
        debug_assert!(previous >= self.body_bytes);
        let previous = self
            .live_header_bytes
            .fetch_sub(self.header_bytes, Ordering::SeqCst);
        debug_assert!(previous >= self.header_bytes);
        if let Some(header_accountant) = &self.header_accountant {
            self.accountant.release(self.body_bytes);
            header_accountant.release(self.header_bytes);
        } else {
            self.accountant.release(self.body_bytes + self.header_bytes);
        }
    }
}

/// Legacy publication owns its Vec inline: its void API only admits the body.
/// Fallible admission can promote that Vec into an allocation shared by later
/// memberships. Cloning an owned value copies bytes, never its charge token.
#[derive(Debug)]
pub(crate) enum SharedVectorPayload<T> {
    Owned(RawVectorPayload<T>),
    Shared(Arc<RawVectorPayload<T>>),
}

impl<T: Clone> Clone for SharedVectorPayload<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Owned(payload) => payload.values.clone().into(),
            Self::Shared(payload) => Self::Shared(payload.clone()),
        }
    }
}

pub(crate) fn shared_payload_header_bytes() -> usize {
    std::mem::size_of::<RawVectorPayload<u8>>() + 2 * std::mem::size_of::<usize>()
}

impl<T> From<Vec<T>> for SharedVectorPayload<T> {
    fn from(values: Vec<T>) -> Self {
        Self::Owned(RawVectorPayload {
            values,
            charge: Mutex::new(None),
        })
    }
}

impl<T> Deref for SharedVectorPayload<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        &self.payload().values
    }
}

impl<T: PartialEq> PartialEq for SharedVectorPayload<T> {
    fn eq(&self, other: &Self) -> bool {
        self.payload().values == other.payload().values
    }
}

impl<T> SharedVectorPayload<T> {
    fn payload(&self) -> &RawVectorPayload<T> {
        match self {
            Self::Owned(payload) => payload,
            Self::Shared(payload) => payload,
        }
    }

    fn identity(&self) -> Option<usize> {
        match self {
            Self::Owned(_) => None,
            Self::Shared(payload) => Some(Arc::as_ptr(payload) as usize),
        }
    }

    fn header_bytes(&self) -> usize {
        match self {
            Self::Owned(_) => 0,
            Self::Shared(_) => shared_payload_header_bytes(),
        }
    }

    /// The caller has admitted the shared header before this allocation.
    fn promote_to_shared(&mut self, header_accountant: Option<Arc<dyn MemoryBudget>>) {
        if let Self::Owned(payload) = self {
            let mut charge = payload.charge.get_mut().take();
            if let Some(charge) = charge.as_mut() {
                let header_bytes = shared_payload_header_bytes();
                charge.header_bytes = header_bytes;
                charge.header_accountant = header_accountant
                    .filter(|accountant| !Arc::ptr_eq(&charge.accountant, accountant));
                charge
                    .live_header_bytes
                    .fetch_add(header_bytes, Ordering::SeqCst);
            }
            let payload = RawVectorPayload {
                values: std::mem::take(&mut payload.values),
                charge: Mutex::new(charge),
            };
            *self = Self::Shared(Arc::new(payload));
        }
    }

    /// Only newly hydrated, uncharged payloads may shed an unused shared
    /// header. `try_unwrap` proves uniqueness and moves the Vec without cloning
    /// it; charged allocations keep their existing body and header owners.
    fn move_unique_uncharged_to_owned(&mut self) {
        let Self::Shared(payload) = self else {
            return;
        };
        if payload.charge.lock().is_some() {
            return;
        }
        let Self::Shared(payload) = std::mem::replace(self, Vec::new().into()) else {
            unreachable!();
        };
        *self = match Arc::try_unwrap(payload) {
            Ok(payload) => Self::Owned(payload),
            Err(payload) => Self::Shared(payload),
        };
    }

    fn adopt_charge(
        &self,
        body_bytes: usize,
        accountant: Arc<dyn MemoryBudget>,
        live_body_bytes: Arc<AtomicUsize>,
        live_header_bytes: Arc<AtomicUsize>,
    ) {
        let mut charge = self.payload().charge.lock();
        if charge.is_none() {
            let header_bytes = self.header_bytes();
            live_body_bytes.fetch_add(body_bytes, Ordering::SeqCst);
            live_header_bytes.fetch_add(header_bytes, Ordering::SeqCst);
            *charge = Some(RawPayloadCharge {
                body_bytes,
                header_bytes,
                accountant,
                header_accountant: None,
                live_body_bytes,
                live_header_bytes,
            });
        } else if let Some(charge) = charge.as_mut() {
            // A loader may have attached before the store-wide counters exist.
            // Transfer observations without releasing its real allocation.
            if !Arc::ptr_eq(&charge.live_body_bytes, &live_body_bytes) {
                charge
                    .live_body_bytes
                    .fetch_sub(charge.body_bytes, Ordering::SeqCst);
                live_body_bytes.fetch_add(charge.body_bytes, Ordering::SeqCst);
                charge.live_body_bytes = live_body_bytes;
            }
            if !Arc::ptr_eq(&charge.live_header_bytes, &live_header_bytes) {
                charge
                    .live_header_bytes
                    .fetch_sub(charge.header_bytes, Ordering::SeqCst);
                live_header_bytes.fetch_add(charge.header_bytes, Ordering::SeqCst);
                charge.live_header_bytes = live_header_bytes;
            }
        }
    }

    fn is_charged(&self) -> bool {
        self.payload().charge.lock().is_some()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StoredVector {
    F32(SharedVectorPayload<f32>),
    SQ8 {
        min: f32,
        max: f32,
        len: usize,
        payload: SharedVectorPayload<u8>,
    },
    SQ4 {
        min: f32,
        max: f32,
        len: usize,
        payload: SharedVectorPayload<u8>,
    },
}

/// The value a quantized column actually stores.
///
/// A column declared with space-saving vector storage keeps a compressed
/// approximation, not the caller's original. Anything that answers a read of
/// such a column has to answer with what is stored, so this is the one place
/// that says what "stored" means -- used both when the value is put away and
/// when a reader asks what the store holds, so the two can never drift.
///
/// Applying it twice returns the same values it returned the first time: the
/// input is already on the grid its own bounds define.
pub fn stored_vector_value(vector: &[f32], quantization: VectorQuantization) -> Vec<f32> {
    match quantization {
        VectorQuantization::F32 => vector.to_vec(),
        VectorQuantization::SQ8 | VectorQuantization::SQ4 => {
            StoredVector::from_f32(vector, quantization).to_f32()
        }
    }
}

/// Exact retained payload bytes for one vector in the representation declared
/// by a column. Persistence-owned lazy loaders use the same calculation as
/// `StoredVector::storage_bytes` before they allocate or decode a partition.
#[doc(hidden)]
pub fn stored_vector_resident_bytes(dimension: usize, quantization: VectorQuantization) -> usize {
    match quantization {
        VectorQuantization::F32 => dimension.saturating_mul(std::mem::size_of::<f32>()),
        VectorQuantization::SQ8 => dimension.saturating_add(8),
        VectorQuantization::SQ4 => dimension.div_ceil(2).saturating_add(8),
    }
}

impl StoredVector {
    fn owned_copy(&self) -> Self {
        match self {
            Self::F32(payload) => Self::F32(payload.to_vec().into()),
            Self::SQ8 {
                min,
                max,
                len,
                payload,
            } => Self::SQ8 {
                min: *min,
                max: *max,
                len: *len,
                payload: payload.to_vec().into(),
            },
            Self::SQ4 {
                min,
                max,
                len,
                payload,
            } => Self::SQ4 {
                min: *min,
                max: *max,
                len: *len,
                payload: payload.to_vec().into(),
            },
        }
    }

    /// Sharing is a representation decision, so it must preserve every stored
    /// bit, including signed zero and quantization bounds. Numeric equality
    /// remains unchanged for existing comparisons.
    pub(crate) fn representation_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::F32(left), Self::F32(right)) => {
                left.len() == right.len()
                    && left
                        .iter()
                        .zip(right.iter())
                        .all(|(a, b)| a.to_bits() == b.to_bits())
            }
            (
                Self::SQ8 {
                    min: a_min,
                    max: a_max,
                    len: a_len,
                    payload: a,
                },
                Self::SQ8 {
                    min: b_min,
                    max: b_max,
                    len: b_len,
                    payload: b,
                },
            )
            | (
                Self::SQ4 {
                    min: a_min,
                    max: a_max,
                    len: a_len,
                    payload: a,
                },
                Self::SQ4 {
                    min: b_min,
                    max: b_max,
                    len: b_len,
                    payload: b,
                },
            ) => {
                a_min.to_bits() == b_min.to_bits()
                    && a_max.to_bits() == b_max.to_bits()
                    && a_len == b_len
                    && a == b
            }
            _ => false,
        }
    }

    pub(crate) fn payload_identity(&self) -> Option<usize> {
        match self {
            Self::F32(payload) => payload.identity(),
            Self::SQ8 { payload, .. } | Self::SQ4 { payload, .. } => payload.identity(),
        }
    }

    pub(crate) fn payload_header_bytes(&self) -> usize {
        match self {
            Self::F32(payload) => payload.header_bytes(),
            Self::SQ8 { payload, .. } | Self::SQ4 { payload, .. } => payload.header_bytes(),
        }
    }

    pub(crate) fn promote_to_shared(&mut self) {
        self.promote_to_shared_with_header_owner(None);
    }

    pub(crate) fn promote_to_shared_with_header_owner(
        &mut self,
        accountant: Option<Arc<dyn MemoryBudget>>,
    ) {
        match self {
            Self::F32(payload) => payload.promote_to_shared(accountant),
            Self::SQ8 { payload, .. } | Self::SQ4 { payload, .. } => {
                payload.promote_to_shared(accountant)
            }
        }
    }

    pub(crate) fn move_unique_uncharged_to_owned(&mut self) {
        match self {
            Self::F32(payload) => payload.move_unique_uncharged_to_owned(),
            Self::SQ8 { payload, .. } | Self::SQ4 { payload, .. } => {
                payload.move_unique_uncharged_to_owned()
            }
        }
    }

    pub(crate) fn payload_is_charged(&self) -> bool {
        match self {
            Self::F32(payload) => payload.is_charged(),
            Self::SQ8 { payload, .. } | Self::SQ4 { payload, .. } => payload.is_charged(),
        }
    }

    pub(crate) fn adopt_raw_charge(
        &self,
        accountant: Arc<dyn MemoryBudget>,
        live_bytes: Arc<AtomicUsize>,
        live_header_bytes: Arc<AtomicUsize>,
    ) {
        let bytes = self.storage_bytes();
        match self {
            Self::F32(payload) => {
                payload.adopt_charge(bytes, accountant, live_bytes, live_header_bytes)
            }
            Self::SQ8 { payload, .. } | Self::SQ4 { payload, .. } => {
                payload.adopt_charge(bytes, accountant, live_bytes, live_header_bytes)
            }
        }
    }

    pub fn from_f32(vector: &[f32], quantization: VectorQuantization) -> Self {
        match quantization {
            VectorQuantization::F32 => StoredVector::F32(vector.to_vec().into()),
            VectorQuantization::SQ8 => {
                let (min, max) = vector_min_max(vector);
                let range = max - min;
                let payload = if range <= f32::EPSILON {
                    vec![0; vector.len()]
                } else {
                    vector
                        .iter()
                        .map(|value| {
                            (((*value - min) / range) * 255.0).round().clamp(0.0, 255.0) as u8
                        })
                        .collect()
                };
                StoredVector::SQ8 {
                    min,
                    max,
                    len: vector.len(),
                    payload: payload.into(),
                }
            }
            VectorQuantization::SQ4 => {
                let (min, max) = vector_min_max(vector);
                let range = max - min;
                let quantized = if range <= f32::EPSILON {
                    vec![0; vector.len()]
                } else {
                    vector
                        .iter()
                        .map(|value| {
                            (((*value - min) / range) * 15.0).round().clamp(0.0, 15.0) as u8
                        })
                        .collect::<Vec<_>>()
                };
                let mut payload = Vec::with_capacity(vector.len().div_ceil(2));
                for pair in quantized.chunks(2) {
                    let hi = pair[0] & 0x0f;
                    let lo = pair.get(1).copied().unwrap_or(0) & 0x0f;
                    payload.push((hi << 4) | lo);
                }
                StoredVector::SQ4 {
                    min,
                    max,
                    len: vector.len(),
                    payload: payload.into(),
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            StoredVector::F32(vector) => vector.len(),
            StoredVector::SQ8 { len, .. } | StoredVector::SQ4 { len, .. } => *len,
        }
    }

    pub fn storage_bytes(&self) -> usize {
        match self {
            StoredVector::F32(vector) => {
                stored_vector_resident_bytes(vector.len(), VectorQuantization::F32)
            }
            StoredVector::SQ8 { len, .. } => {
                stored_vector_resident_bytes(*len, VectorQuantization::SQ8)
            }
            StoredVector::SQ4 { len, .. } => {
                stored_vector_resident_bytes(*len, VectorQuantization::SQ4)
            }
        }
    }

    pub fn to_f32(&self) -> Vec<f32> {
        match self {
            StoredVector::F32(vector) => vector.to_vec(),
            StoredVector::SQ8 {
                min,
                max,
                len,
                payload,
            } => payload
                .iter()
                .take(*len)
                .map(|byte| dequantize(*byte, 255.0, *min, *max))
                .collect(),
            StoredVector::SQ4 {
                min,
                max,
                len,
                payload,
            } => {
                let mut values = Vec::with_capacity(*len);
                for byte in payload.iter() {
                    for q in [byte >> 4, byte & 0x0f] {
                        if values.len() == *len {
                            break;
                        }
                        values.push(dequantize(q, 15.0, *min, *max));
                    }
                }
                values
            }
        }
    }

    pub fn cosine_similarity(&self, query: &[f32]) -> f32 {
        if query.len() != self.len() {
            return 0.0;
        }
        match self {
            StoredVector::F32(vector) => {
                cosine_from_iter(query.iter().copied(), vector.iter().copied(), vector.len())
            }
            StoredVector::SQ8 {
                min, max, payload, ..
            } => cosine_query_sq8(query, payload, *min, *max),
            StoredVector::SQ4 {
                min,
                max,
                len,
                payload,
            } => cosine_query_sq4(query, payload, *len, *min, *max),
        }
    }

    pub fn as_f32_slice(&self) -> Option<&[f32]> {
        match self {
            StoredVector::F32(vector) => Some(vector),
            _ => None,
        }
    }

    pub fn to_hnsw_u8(&self) -> Vec<u8> {
        let (len, min, max, payload) = match self {
            StoredVector::F32(_) => return Vec::new(),
            StoredVector::SQ8 {
                min,
                max,
                len,
                payload,
            }
            | StoredVector::SQ4 {
                min,
                max,
                len,
                payload,
            } => (*len, *min, *max, &payload[..]),
        };
        encode_hnsw_quantized_payload(len, min, max, payload)
    }
}

pub(crate) fn encode_hnsw_quantized_payload(
    len: usize,
    min: f32,
    max: f32,
    payload: &[u8],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HNSW_HEADER_BYTES + payload.len());
    bytes.extend_from_slice(&(len as u32).to_le_bytes());
    bytes.extend_from_slice(&min.to_le_bytes());
    bytes.extend_from_slice(&max.to_le_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HnswQuantizedHeader<'a> {
    pub len: usize,
    pub min: f32,
    pub max: f32,
    pub payload: &'a [u8],
}

pub(crate) fn decode_hnsw_quantized(bytes: &[u8]) -> Option<HnswQuantizedHeader<'_>> {
    if bytes.len() < HNSW_HEADER_BYTES {
        return None;
    }
    let len = u32::from_le_bytes(bytes[0..4].try_into().ok()?) as usize;
    let min = f32::from_le_bytes(bytes[4..8].try_into().ok()?);
    let max = f32::from_le_bytes(bytes[8..12].try_into().ok()?);
    Some(HnswQuantizedHeader {
        len,
        min,
        max,
        payload: &bytes[HNSW_HEADER_BYTES..],
    })
}

pub(crate) fn quantized_hnsw_query_score(
    bytes: &[u8],
    query: &[f32],
    quantization: VectorQuantization,
) -> Option<f32> {
    let stored = decode_hnsw_quantized(bytes)?;
    if stored.len != query.len() {
        return None;
    }
    match quantization {
        VectorQuantization::SQ8 => Some(cosine_query_sq8(
            query,
            stored.payload,
            stored.min,
            stored.max,
        )),
        VectorQuantization::SQ4 => Some(cosine_query_sq4(
            query,
            stored.payload,
            stored.len,
            stored.min,
            stored.max,
        )),
        VectorQuantization::F32 => None,
    }
}

pub(crate) fn quantized_hnsw_distance(
    left: &[u8],
    right: &[u8],
    quantization: VectorQuantization,
) -> f32 {
    let Some(left) = decode_hnsw_quantized(left) else {
        return 1.0;
    };
    let Some(right) = decode_hnsw_quantized(right) else {
        return 1.0;
    };
    let len = left.len.min(right.len);
    if len == 0 {
        return 0.0;
    }

    let similarity = match quantization {
        VectorQuantization::SQ8 => cosine_sq8(left, right, len),
        VectorQuantization::SQ4 => cosine_sq4(left, right, len),
        VectorQuantization::F32 => return 1.0,
    };

    (1.0 - similarity as f64).max(0.0) as f32
}

fn cosine_query_sq8(query: &[f32], payload: &[u8], min: f32, max: f32) -> f32 {
    let len = query.len().min(payload.len());
    let range = max - min;
    let scale = if range <= f32::EPSILON {
        0.0
    } else {
        range / 255.0
    };
    let (mut dot, mut query_norm, mut vector_norm) = (0.0_f32, 0.0_f32, 0.0_f32);
    for i in 0..len {
        let a = query[i];
        let b = if scale == 0.0 {
            min
        } else {
            min + payload[i] as f32 * scale
        };
        dot += a * b;
        query_norm += a * a;
        vector_norm += b * b;
    }
    finish_cosine(dot, query_norm, vector_norm)
}

fn cosine_query_sq4(query: &[f32], payload: &[u8], len: usize, min: f32, max: f32) -> f32 {
    let len = query.len().min(len);
    let range = max - min;
    let scale = if range <= f32::EPSILON {
        0.0
    } else {
        range / 15.0
    };
    let (mut dot, mut query_norm, mut vector_norm) = (0.0_f32, 0.0_f32, 0.0_f32);
    for (i, a) in query.iter().copied().enumerate().take(len) {
        let q = unpack_sq4(payload, i);
        let b = if scale == 0.0 {
            min
        } else {
            min + q as f32 * scale
        };
        dot += a * b;
        query_norm += a * a;
        vector_norm += b * b;
    }
    finish_cosine(dot, query_norm, vector_norm)
}

fn cosine_sq8(left: HnswQuantizedHeader<'_>, right: HnswQuantizedHeader<'_>, len: usize) -> f32 {
    let left_range = left.max - left.min;
    let right_range = right.max - right.min;
    let left_scale = if left_range <= f32::EPSILON {
        0.0
    } else {
        left_range / 255.0
    };
    let right_scale = if right_range <= f32::EPSILON {
        0.0
    } else {
        right_range / 255.0
    };
    let (mut dot, mut left_norm, mut right_norm) = (0.0_f32, 0.0_f32, 0.0_f32);
    for i in 0..len {
        let a = if left_scale == 0.0 {
            left.min
        } else {
            left.min + left.payload.get(i).copied().unwrap_or(0) as f32 * left_scale
        };
        let b = if right_scale == 0.0 {
            right.min
        } else {
            right.min + right.payload.get(i).copied().unwrap_or(0) as f32 * right_scale
        };
        dot += a * b;
        left_norm += a * a;
        right_norm += b * b;
    }
    finish_cosine(dot, left_norm, right_norm)
}

fn cosine_sq4(left: HnswQuantizedHeader<'_>, right: HnswQuantizedHeader<'_>, len: usize) -> f32 {
    let left_range = left.max - left.min;
    let right_range = right.max - right.min;
    let left_scale = if left_range <= f32::EPSILON {
        0.0
    } else {
        left_range / 15.0
    };
    let right_scale = if right_range <= f32::EPSILON {
        0.0
    } else {
        right_range / 15.0
    };
    let (mut dot, mut left_norm, mut right_norm) = (0.0_f32, 0.0_f32, 0.0_f32);
    for i in 0..len {
        let a = if left_scale == 0.0 {
            left.min
        } else {
            left.min + unpack_sq4(left.payload, i) as f32 * left_scale
        };
        let b = if right_scale == 0.0 {
            right.min
        } else {
            right.min + unpack_sq4(right.payload, i) as f32 * right_scale
        };
        dot += a * b;
        left_norm += a * a;
        right_norm += b * b;
    }
    finish_cosine(dot, left_norm, right_norm)
}

fn cosine_from_iter<A, B>(left: A, right: B, expected_len: usize) -> f32
where
    A: Iterator<Item = f32>,
    B: Iterator<Item = f32>,
{
    let (dot, left_norm, right_norm) = left.zip(right).take(expected_len).fold(
        (0.0_f32, 0.0_f32, 0.0_f32),
        |(dot, left_norm, right_norm), (a, b)| (dot + a * b, left_norm + a * a, right_norm + b * b),
    );
    if left_norm == 0.0 || right_norm == 0.0 {
        return 0.0;
    }
    finish_cosine(dot, left_norm, right_norm)
}

fn finish_cosine(dot: f32, left_norm: f32, right_norm: f32) -> f32 {
    if left_norm == 0.0 || right_norm == 0.0 {
        return 0.0;
    }
    dot / (left_norm.sqrt() * right_norm.sqrt())
}

fn dequantize(q: u8, levels: f32, min: f32, max: f32) -> f32 {
    let range = max - min;
    if range <= f32::EPSILON {
        min
    } else {
        min + ((q as f32) / levels) * range
    }
}

fn unpack_sq4(payload: &[u8], index: usize) -> u8 {
    let byte = payload.get(index / 2).copied().unwrap_or(0);
    if index.is_multiple_of(2) {
        byte >> 4
    } else {
        byte & 0x0f
    }
}

fn vector_min_max(vector: &[f32]) -> (f32, f32) {
    let Some((first, rest)) = vector.split_first() else {
        return (0.0, 0.0);
    };
    rest.iter()
        .copied()
        .fold((*first, *first), |(min, max), value| {
            (min.min(value), max.max(value))
        })
}
