//! Diagnostic, not a proof: which built graphs leave layer-zero points that
//! the entry point cannot reach. Run with
//! `--features test-seams -- --ignored --nocapture graph_reachability`.
#![cfg(feature = "test-seams")]

use contextdb_core::{Lsn, RowId, TxId, VectorEntry, VectorIndexRef, VectorQuantization};
use contextdb_vector::hnsw::HnswIndex;
use contextdb_vector::store::ResolvedVectorPolicy;

fn mixed(seed: u64) -> f32 {
    let mut word = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    ((word ^ (word >> 31)) % 1_000_003) as f32 / 1_000_003.0 - 0.5
}

fn entries(vectors: Vec<Vec<f32>>) -> Vec<VectorEntry> {
    vectors
        .into_iter()
        .enumerate()
        .map(|(position, vector)| VectorEntry {
            index: VectorIndexRef::new("t", "v"),
            row_id: RowId(position as u64 + 1),
            vector,
            created_tx: TxId(1),
            deleted_tx: None,
            lsn: Lsn(position as u64 + 1),
        })
        .collect()
}

fn distinct(n: usize, dimension: usize) -> Vec<Vec<f32>> {
    (0..n)
        .map(|row| {
            (0..dimension)
                .map(|axis| mixed((row * 1_000 + axis) as u64))
                .collect()
        })
        .collect()
}

/// The frozen tied-frontier fixture's vectors: `[0, 1, 11 * (id % 11) / 120]`.
fn tied(n: usize) -> Vec<Vec<f32>> {
    (1..=n as i64)
        .map(|id| vec![0.0, 1.0, ((id * 11) % 121) as f32 / 120.0])
        .collect()
}

/// `n / copies` distinct vectors, each stored `copies` times.
fn duplicated(n: usize, dimension: usize, copies: usize) -> Vec<Vec<f32>> {
    let base = distinct(n.div_ceil(copies), dimension);
    (0..n).map(|row| base[row / copies].clone()).collect()
}

/// Near-duplicates: `n / copies` centres, each with `copies` points within
/// `spread` of it.
fn clustered(n: usize, dimension: usize, copies: usize, spread: f32) -> Vec<Vec<f32>> {
    let base = distinct(n.div_ceil(copies), dimension);
    (0..n)
        .map(|row| {
            base[row / copies]
                .iter()
                .enumerate()
                .map(|(axis, value)| value + spread * mixed((row * 7_919 + axis) as u64 + 77))
                .collect()
        })
        .collect()
}

fn policy() -> ResolvedVectorPolicy {
    ResolvedVectorPolicy {
        auto_index_at: 1,
        hnsw_m: 16,
        hnsw_ef_construction: 200,
        hnsw_ef_search: 10,
        policy_revision: 1,
        auto_index_at_source: "declared",
        ef_search_source: "declared",
    }
}

fn report(label: &str, vectors: Vec<Vec<f32>>) {
    let dimension = vectors[0].len();
    let entries = entries(vectors);
    for workers in [1, 8] {
        let repeats = if workers == 1 { 1 } else { 5 };
        let mut counts = Vec::new();
        for _ in 0..repeats {
            let index = HnswIndex::from_vector_entries_with_parallel_progress(
                &entries,
                dimension,
                VectorQuantization::F32,
                policy(),
                workers,
                || false,
                |_| Ok(()),
            )
            .expect("build the graph");
            counts.push(index.unreachable_layer0_points_for_test());
        }
        println!(
            "graph_reachability {label} n={} workers={workers} (unreachable, points)={counts:?}",
            entries.len()
        );
    }
}

#[test]
#[ignore = "diagnostic"]
fn graph_reachability_by_vector_shape() {
    for n in [1_000, 5_000] {
        report("distinct d=8", distinct(n, 8));
        report("distinct d=128", distinct(n, 128));
        report("duplicated x2 d=128", duplicated(n, 128, 2));
        report("duplicated x10 d=128", duplicated(n, 128, 10));
        report(
            "near-duplicate x10 spread=1e-4 d=128",
            clustered(n, 128, 10, 1e-4),
        );
        report(
            "near-duplicate x10 spread=1e-2 d=128",
            clustered(n, 128, 10, 1e-2),
        );
    }
    for n in [120, 1_200] {
        report("tied fixture", tied(n));
    }
}
