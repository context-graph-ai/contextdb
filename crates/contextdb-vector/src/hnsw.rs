use anndists::dist::distances::DistCosine;
use contextdb_core::{Error, Result, RowId, VectorEntry};
use hnsw_rs::hnsw::Hnsw;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct HnswIndex {
    hnsw: Hnsw<'static, f32, DistCosine>,
    id_to_row: RwLock<HashMap<usize, RowId>>,
    row_to_id: RwLock<HashMap<RowId, usize>>,
    next_id: AtomicUsize,
    dimension: usize,
    ef_search: usize,
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
    pub fn new(entries: &[VectorEntry], dimension: usize) -> Self {
        let (m, ef_construction, ef_search) = select_params(entries.len());
        let max_elements = entries.len().max(1);
        let mut hnsw = Hnsw::new(m, max_elements, 16, ef_construction, DistCosine);
        hnsw.set_extend_candidates(true);
        hnsw.set_keeping_pruned(true);
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

        for (data_id, entry) in sorted_entries.into_iter().enumerate() {
            hnsw.insert((&entry.vector, data_id));
            id_to_row.write().insert(data_id, entry.row_id);
            row_to_id.write().insert(entry.row_id, data_id);
        }

        Self {
            hnsw,
            id_to_row,
            row_to_id,
            next_id: AtomicUsize::new(entries.len()),
            dimension,
            ef_search,
        }
    }

    pub fn insert(&self, row_id: RowId, vector: &[f32]) {
        let data_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.hnsw.insert((vector, data_id));
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
        let indexation = self.hnsw.get_point_indexation();
        let layer0_neighbor_edges = indexation
            .get_layer_iterator(0)
            .map(|point| {
                point
                    .get_neighborhood_id()
                    .first()
                    .map_or(0, |neighbors| neighbors.len())
            })
            .sum();

        HnswGraphStats {
            point_count: self.hnsw.get_nb_point(),
            layer0_points: indexation.get_layer_nb_point(0),
            layer0_neighbor_edges,
            max_level_observed: self.hnsw.get_max_level_observed(),
            dimension: self.dimension,
        }
    }

    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<(RowId, f32)>> {
        if k == 0 {
            return Ok(Vec::new());
        }

        let got = query.len();
        if got != self.dimension {
            return Err(Error::VectorDimensionMismatch {
                expected: self.dimension,
                got,
            });
        }

        let ef = self.ef_search.max(k.saturating_mul(10)).max(1);
        let neighbors = self.hnsw.search(query, ef, ef);
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

fn select_params(count: usize) -> (usize, usize, usize) {
    match count {
        0..=5000 => (16, 200, count.max(200)),
        5001..=50000 => (24, 400, 400),
        _ => (16, 200, 200),
    }
}

fn insertion_key(entry: &VectorEntry) -> u64 {
    let mut x = entry.row_id.0 ^ entry.lsn.0 ^ entry.created_tx.0;
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
