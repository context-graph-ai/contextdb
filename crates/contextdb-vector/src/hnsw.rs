use crate::quantized::{StoredVector, StoredVectorEntry, quantized_hnsw_distance};
use anndists::dist::distances::{DistCosine, Distance};
use contextdb_core::{Error, Result, RowId, VectorIndexRef, VectorQuantization};
use hnsw_rs::hnsw::Hnsw;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct HnswIndex {
    hnsw: HnswInner,
    id_to_row: RwLock<HashMap<usize, RowId>>,
    exact_rows: RwLock<HashMap<Vec<u8>, Vec<RowId>>>,
    next_id: AtomicUsize,
    dimension: usize,
    quantization: VectorQuantization,
    ef_search: usize,
}

enum HnswInner {
    F32(Hnsw<'static, f32, DistCosine>),
    Quantized(Hnsw<'static, u8, DistQuantizedCosine>),
}

#[derive(Debug, Clone, Copy)]
struct DistQuantizedCosine {
    quantization: VectorQuantization,
}

impl Distance<u8> for DistQuantizedCosine {
    fn eval(&self, va: &[u8], vb: &[u8]) -> f32 {
        quantized_hnsw_distance(va, vb, self.quantization)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswGraphStats {
    pub point_count: usize,
    pub layer0_points: usize,
    pub layer0_neighbor_edges: usize,
    pub max_level_observed: u8,
    pub dimension: usize,
}

impl HnswIndex {
    pub(crate) fn new(
        entries: &[StoredVectorEntry],
        dimension: usize,
        quantization: VectorQuantization,
    ) -> Self {
        let mut sorted_entries = entries
            .iter()
            .filter(|entry| entry.deleted_tx.is_none())
            .collect::<Vec<_>>();
        sorted_entries.sort_by_key(|entry| {
            (
                entry.lsn,
                entry.created_tx,
                insertion_key(entry),
                entry.row_id,
            )
        });

        let (m, ef_construction, ef_search) = select_params(sorted_entries.len(), quantization);
        let max_elements = sorted_entries.len().max(1);
        let hnsw = match quantization {
            VectorQuantization::F32 => {
                let mut hnsw = Hnsw::new(m, max_elements, 16, ef_construction, DistCosine);
                hnsw.set_extend_candidates(true);
                hnsw.set_keeping_pruned(true);
                HnswInner::F32(hnsw)
            }
            VectorQuantization::SQ8 | VectorQuantization::SQ4 => {
                let mut hnsw = Hnsw::new(
                    m,
                    max_elements,
                    16,
                    ef_construction,
                    DistQuantizedCosine { quantization },
                );
                hnsw.set_extend_candidates(true);
                hnsw.set_keeping_pruned(true);
                HnswInner::Quantized(hnsw)
            }
        };
        let id_to_row = RwLock::new(HashMap::with_capacity(sorted_entries.len()));
        let exact_rows = RwLock::new(HashMap::<Vec<u8>, Vec<RowId>>::with_capacity(
            sorted_entries.len(),
        ));
        let mut inserted_count = 0usize;

        match &hnsw {
            HnswInner::F32(index) => {
                let data = sorted_entries
                    .iter()
                    .filter_map(|entry| {
                        entry.vector.as_f32_slice().map(|vector| {
                            let data_id = inserted_count;
                            inserted_count += 1;
                            id_to_row.write().insert(data_id, entry.row_id);
                            if let Some(key) = exact_key_for_stored_vector(&entry.vector) {
                                exact_rows
                                    .write()
                                    .entry(key)
                                    .or_default()
                                    .push(entry.row_id);
                            }
                            (vector.to_vec(), data_id)
                        })
                    })
                    .collect::<Vec<_>>();
                let refs = data
                    .iter()
                    .map(|(vector, data_id)| (vector, *data_id))
                    .collect::<Vec<_>>();
                index.parallel_insert(&refs);
            }
            HnswInner::Quantized(index) => {
                let data = sorted_entries
                    .iter()
                    .filter_map(|entry| {
                        let encoded = entry.vector.to_hnsw_u8();
                        (!encoded.is_empty()).then(|| {
                            let data_id = inserted_count;
                            inserted_count += 1;
                            id_to_row.write().insert(data_id, entry.row_id);
                            if let Some(key) = exact_key_for_stored_vector(&entry.vector) {
                                exact_rows
                                    .write()
                                    .entry(key)
                                    .or_default()
                                    .push(entry.row_id);
                            }
                            (encoded, data_id)
                        })
                    })
                    .collect::<Vec<_>>();
                let refs = data
                    .iter()
                    .map(|(vector, data_id)| (vector, *data_id))
                    .collect::<Vec<_>>();
                index.parallel_insert(&refs);
            }
        }

        Self {
            hnsw,
            id_to_row,
            exact_rows,
            next_id: AtomicUsize::new(inserted_count),
            dimension,
            quantization,
            ef_search,
        }
    }

    /// Number of vectors currently indexed in the HNSW graph.
    pub fn len(&self) -> usize {
        self.next_id.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[doc(hidden)]
    pub fn graph_stats(&self) -> HnswGraphStats {
        let (point_count, layer0_neighbor_edges, max_level_observed) = match &self.hnsw {
            HnswInner::F32(hnsw) => hnsw_stats(hnsw),
            HnswInner::Quantized(hnsw) => hnsw_stats(hnsw),
        };

        HnswGraphStats {
            point_count,
            layer0_points: point_count,
            layer0_neighbor_edges,
            max_level_observed,
            dimension: self.dimension,
        }
    }

    pub fn search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(RowId, f32)>> {
        if k == 0 {
            return Ok(Vec::new());
        }

        let got = query.len();
        if got != self.dimension {
            return Err(Error::VectorIndexDimensionMismatch {
                index: index.clone(),
                expected: self.dimension,
                actual: got,
            });
        }

        let ef = self.ef_search.max(k.saturating_mul(10)).max(1);
        let neighbors = match &self.hnsw {
            HnswInner::F32(hnsw) => hnsw.search(query, ef, ef),
            HnswInner::Quantized(hnsw) => {
                let encoded = StoredVector::from_f32(query, self.quantization).to_hnsw_u8();
                hnsw.search(&encoded, ef, ef)
            }
        };
        let id_to_row = self.id_to_row.read();
        let cap = ef.max(k);
        let mut scored = Vec::with_capacity(
            cap.saturating_add(neighbors.len())
                .min(cap.saturating_mul(2)),
        );
        if let Some(key) = exact_key_for_query(query, self.quantization)
            && let Some(row_ids) = self.exact_rows.read().get(&key)
        {
            scored.extend(row_ids.iter().take(cap).map(|row_id| (*row_id, 1.0)));
        }

        scored.extend(neighbors.into_iter().filter_map(|neighbor| {
            id_to_row
                .get(&neighbor.d_id)
                .copied()
                .map(|row_id| (row_id, 1.0 - neighbor.distance))
        }));
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let mut seen = HashSet::new();
        scored.retain(|(row_id, _)| seen.insert(*row_id));
        scored.truncate(cap);
        Ok(scored)
    }

    #[doc(hidden)]
    pub fn raw_entry_count_for_row(&self, row_id: RowId) -> usize {
        self.id_to_row
            .read()
            .values()
            .filter(|indexed_row| **indexed_row == row_id)
            .count()
    }
}

fn exact_key_for_stored_vector(vector: &StoredVector) -> Option<Vec<u8>> {
    match vector {
        StoredVector::F32(values) => Some(f32_exact_key(values)),
        StoredVector::SQ8 { .. } | StoredVector::SQ4 { .. } => {
            let encoded = vector.to_hnsw_u8();
            (!encoded.is_empty()).then_some(encoded)
        }
    }
}

fn exact_key_for_query(query: &[f32], quantization: VectorQuantization) -> Option<Vec<u8>> {
    match quantization {
        VectorQuantization::F32 => Some(f32_exact_key(query)),
        VectorQuantization::SQ8 | VectorQuantization::SQ4 => {
            let encoded = StoredVector::from_f32(query, quantization).to_hnsw_u8();
            (!encoded.is_empty()).then_some(encoded)
        }
    }
}

fn f32_exact_key(values: &[f32]) -> Vec<u8> {
    let mut key = Vec::with_capacity(std::mem::size_of_val(values));
    for value in values {
        key.extend_from_slice(&value.to_bits().to_be_bytes());
    }
    key
}

fn hnsw_stats<T, D>(hnsw: &Hnsw<'_, T, D>) -> (usize, usize, u8)
where
    T: Clone + Send + Sync,
    D: Distance<T> + Send + Sync,
{
    let indexation = hnsw.get_point_indexation();
    let layer0_neighbor_edges = indexation
        .get_layer_iterator(0)
        .map(|point| {
            point
                .get_neighborhood_id()
                .first()
                .map_or(0, |neighbors| neighbors.len())
        })
        .sum();
    (
        hnsw.get_nb_point(),
        layer0_neighbor_edges,
        hnsw.get_max_level_observed(),
    )
}

fn select_params(count: usize, quantization: VectorQuantization) -> (usize, usize, usize) {
    if !matches!(quantization, VectorQuantization::F32) {
        return match count {
            0..=5000 => (8, 32, 96.min(count.max(32))),
            5001..=50000 => (12, 64, 128),
            _ => (12, 64, 128),
        };
    }
    match count {
        0..=5000 => (16, 200, count.max(200)),
        5001..=50000 => (24, 400, 400),
        _ => (16, 200, 200),
    }
}

fn insertion_key(entry: &StoredVectorEntry) -> u64 {
    let mut x = entry.row_id.0 ^ entry.lsn.0 ^ entry.created_tx.0;
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
