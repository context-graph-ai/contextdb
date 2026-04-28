use contextdb_core::{Lsn, RowId, TxId, VectorEntry, VectorIndexRef, VectorQuantization};

const HNSW_HEADER_BYTES: usize = 12;

#[derive(Debug, Clone)]
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

    pub fn visible_at(&self, snapshot: contextdb_core::SnapshotId) -> bool {
        self.created_tx.0 <= snapshot.0 && self.deleted_tx.is_none_or(|tx| tx.0 > snapshot.0)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum StoredVector {
    F32(Vec<f32>),
    SQ8 {
        min: f32,
        max: f32,
        len: usize,
        payload: Vec<u8>,
    },
    SQ4 {
        min: f32,
        max: f32,
        len: usize,
        payload: Vec<u8>,
    },
}

impl StoredVector {
    pub fn from_f32(vector: &[f32], quantization: VectorQuantization) -> Self {
        match quantization {
            VectorQuantization::F32 => StoredVector::F32(vector.to_vec()),
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
                    payload,
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
                    payload,
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
            StoredVector::F32(vector) => vector.len().saturating_mul(std::mem::size_of::<f32>()),
            StoredVector::SQ8 { len, .. } => len.saturating_add(8),
            StoredVector::SQ4 { len, .. } => len.div_ceil(2).saturating_add(8),
        }
    }

    pub fn to_f32(&self) -> Vec<f32> {
        match self {
            StoredVector::F32(vector) => vector.clone(),
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
                for byte in payload {
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
            } => (*len, *min, *max, payload.as_slice()),
        };
        let mut bytes = Vec::with_capacity(HNSW_HEADER_BYTES + payload.len());
        bytes.extend_from_slice(&(len as u32).to_le_bytes());
        bytes.extend_from_slice(&min.to_le_bytes());
        bytes.extend_from_slice(&max.to_le_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }
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
