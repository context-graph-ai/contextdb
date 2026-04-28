use crate::quantized::{StoredVector, StoredVectorEntry, quantized_hnsw_distance};
use anndists::dist::distances::{DistCosine, Distance};
use contextdb_core::{Error, Result, RowId, VectorIndexRef, VectorQuantization};
use hnsw_rs::hnsw::Hnsw;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct HnswIndex {
    hnsw: HnswInner,
    id_to_row: RwLock<HashMap<usize, RowId>>,
    row_to_id: RwLock<HashMap<RowId, usize>>,
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
        let (m, ef_construction, ef_search) = select_params(entries.len(), quantization);
        let max_elements = entries.len().max(1);
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
        let id_to_row = RwLock::new(HashMap::with_capacity(entries.len()));
        let row_to_id = RwLock::new(HashMap::with_capacity(entries.len()));
        let mut sorted_entries = entries.iter().collect::<Vec<_>>();
        sorted_entries.sort_by_key(|entry| {
            (
                insertion_key(entry),
                entry.lsn,
                entry.created_tx,
                entry.row_id,
            )
        });

        match &hnsw {
            HnswInner::F32(index) => {
                let data = sorted_entries
                    .iter()
                    .enumerate()
                    .filter_map(|(data_id, entry)| {
                        entry.vector.as_f32_slice().map(|vector| {
                            id_to_row.write().insert(data_id, entry.row_id);
                            row_to_id.write().insert(entry.row_id, data_id);
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
                    .enumerate()
                    .filter_map(|(data_id, entry)| {
                        let encoded = entry.vector.to_hnsw_u8();
                        (!encoded.is_empty()).then(|| {
                            id_to_row.write().insert(data_id, entry.row_id);
                            row_to_id.write().insert(entry.row_id, data_id);
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
            row_to_id,
            next_id: AtomicUsize::new(entries.len()),
            dimension,
            quantization,
            ef_search,
        }
    }

    pub(crate) fn insert(&self, row_id: RowId, vector: &StoredVector) {
        let data_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        insert_into_hnsw(&self.hnsw, vector, data_id);
        self.id_to_row.write().insert(data_id, row_id);
        self.row_to_id.write().insert(row_id, data_id);
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

        Ok(neighbors
            .into_iter()
            .filter_map(|neighbor| {
                id_to_row
                    .get(&neighbor.d_id)
                    .copied()
                    .map(|row_id| (row_id, 1.0 - neighbor.distance))
            })
            .collect())
    }
}

fn insert_into_hnsw(hnsw: &HnswInner, vector: &StoredVector, data_id: usize) {
    match hnsw {
        HnswInner::F32(hnsw) => {
            let Some(vector) = vector.as_f32_slice() else {
                return;
            };
            hnsw.insert((vector, data_id));
        }
        HnswInner::Quantized(hnsw) => {
            let encoded = vector.to_hnsw_u8();
            if !encoded.is_empty() {
                hnsw.insert((&encoded, data_id));
            }
        }
    }
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
