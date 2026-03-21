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

impl HnswIndex {
    pub fn new(entries: &[VectorEntry], dimension: usize) -> Self {
        let (m, ef_construction, ef_search) = select_params(entries.len());
        let max_elements = entries.len().max(1);
        let hnsw = Hnsw::new(m, max_elements, 16, ef_construction, DistCosine);
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

        let knbn = k.saturating_mul(3).max(1);
        let ef = self.ef_search.max(knbn);
        let neighbors = self.hnsw.search(query, knbn, ef);
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
        0..=5000 => (16, 200, 200),
        5001..=50000 => (24, 400, 400),
        _ => (16, 200, 200),
    }
}

fn insertion_key(entry: &VectorEntry) -> u64 {
    let mut x = entry.row_id ^ entry.lsn ^ entry.created_tx;
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
