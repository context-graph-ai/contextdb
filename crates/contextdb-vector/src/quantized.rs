use crate::store::VectorMembershipProvenance;
use contextdb_core::{Lsn, RowId, TxId, VectorEntry, VectorIndexRef, VectorQuantization};
use parking_lot::Mutex;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StoredVectorEntry {
    pub membership: VectorMembershipProvenance,
    pub canonical_deleted_tx: Option<TxId>,
    pub row_id: RowId,
    pub vector: StoredVector,
    pub created_tx: TxId,
    pub deleted_tx: Option<TxId>,
    pub lsn: Lsn,
}

impl StoredVectorEntry {
    pub fn from_vector_entry(entry: VectorEntry, quantization: VectorQuantization) -> Self {
        Self {
            membership: VectorMembershipProvenance::new(
                entry.created_tx,
                entry.lsn,
                entry.created_tx,
                entry.deleted_tx,
            ),
            canonical_deleted_tx: entry.deleted_tx,
            row_id: entry.row_id,
            vector: StoredVector::from_f32(&entry.vector, quantization),
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            lsn: entry.lsn,
        }
    }

    pub fn from_vector_entry_ref(entry: &VectorEntry, quantization: VectorQuantization) -> Self {
        Self {
            membership: VectorMembershipProvenance::new(
                entry.created_tx,
                entry.lsn,
                entry.created_tx,
                entry.deleted_tx,
            ),
            canonical_deleted_tx: entry.deleted_tx,
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
    #[cfg(test)]
    pub(crate) fn owned_copy(&self) -> Self {
        Self {
            membership: self.membership,
            canonical_deleted_tx: self.canonical_deleted_tx,
            row_id: self.row_id,
            vector: self.vector.owned_copy(),
            created_tx: self.created_tx,
            deleted_tx: self.deleted_tx,
            lsn: self.lsn,
        }
    }

    pub fn visible_at(&self, snapshot: contextdb_core::SnapshotId) -> bool {
        self.membership.visible_from.0 <= snapshot.0
            && self
                .membership
                .deleted_tx
                .is_none_or(|tx| tx.0 > snapshot.0)
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
    accountant: crate::memory_budget::MemoryBudgetOwner,
    /// Promotion can admit a header through a later open's accountant while
    /// the legacy body keeps the funder that already owns its reservation.
    header_accountant: Option<crate::memory_budget::MemoryBudgetOwner>,
    live_body_bytes: Arc<AtomicUsize>,
    live_header_bytes: Arc<AtomicUsize>,
    _body_control_invoice:
        Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>>,
    _header_control_invoice:
        Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>>,
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
    fn promote_to_shared(
        &mut self,
        header_accountant: Option<crate::memory_budget::MemoryBudgetOwner>,
    ) {
        if let Self::Owned(payload) = self {
            let mut charge = payload.charge.get_mut().take();
            if let Some(charge) = charge.as_mut() {
                let header_bytes = shared_payload_header_bytes();
                charge.header_bytes = header_bytes;
                charge.header_accountant = header_accountant.filter(|accountant| {
                    !crate::memory_budget::same_funder(
                        charge.accountant.as_ref(),
                        accountant.as_ref(),
                    )
                });
                charge._header_control_invoice = charge
                    .header_accountant
                    .as_ref()
                    .and_then(|owner| owner.control_invoice());
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
        accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>,
        live_body_bytes: Arc<AtomicUsize>,
        live_header_bytes: Arc<AtomicUsize>,
    ) {
        let accountant: crate::memory_budget::MemoryBudgetOwner = accountant.into();

        let mut charge = self.payload().charge.lock();
        if charge.is_none() {
            let header_bytes = self.header_bytes();
            live_body_bytes.fetch_add(body_bytes, Ordering::SeqCst);
            live_header_bytes.fetch_add(header_bytes, Ordering::SeqCst);
            *charge = Some(RawPayloadCharge {
                body_bytes,
                header_bytes,
                _body_control_invoice: accountant.control_invoice(),
                _header_control_invoice: None,
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
            // A row-value projection needs the floats, not a retained stored
            // payload. Use the same grid and dequantizer directly so this
            // copy allocates only its final float vector.
            let levels = if quantization == VectorQuantization::SQ8 {
                255.0
            } else {
                15.0
            };
            let (min, max) = vector_min_max(vector);
            let range = max - min;
            vector
                .iter()
                .map(|value| {
                    let q = if range <= f32::EPSILON {
                        0
                    } else {
                        (((*value - min) / range) * levels)
                            .round()
                            .clamp(0.0, levels) as u8
                    };
                    dequantize(q, levels, min, max)
                })
                .collect()
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
    #[cfg(test)]
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
        accountant: Option<crate::memory_budget::MemoryBudgetOwner>,
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
        accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>,
        live_bytes: Arc<AtomicUsize>,
        live_header_bytes: Arc<AtomicUsize>,
    ) {
        let accountant: crate::memory_budget::MemoryBudgetOwner = accountant.into();

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

    /// The exact route's ranking key over the coordinates this body actually
    /// stores: quantized bodies are decoded one coordinate at a time with the
    /// stored decoder, then compared through the one canonical arithmetic.
    /// A length mismatch keeps the existing similarity of 0.0.
    pub fn canonical_similarity(&self, query: &[f32]) -> f64 {
        if query.len() != self.len() {
            return 0.0;
        }
        let distance = match self {
            StoredVector::F32(vector) => crate::cosine::canonical_cosine_distance(query, vector),
            StoredVector::SQ8 {
                min,
                max,
                len,
                payload,
            } => crate::cosine::canonical_cosine_distance_pairs(
                query.iter().copied().zip(
                    payload
                        .iter()
                        .take(*len)
                        .map(|byte| dequantize(*byte, 255.0, *min, *max)),
                ),
            ),
            StoredVector::SQ4 {
                min,
                max,
                len,
                payload,
            } => crate::cosine::canonical_cosine_distance_pairs(
                query
                    .iter()
                    .copied()
                    .zip((0..*len).map(|at| dequantize(unpack_sq4(payload, at), 15.0, *min, *max))),
            ),
        };
        1.0 - distance
    }
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
