//! Rows that store the same representation share one graph point. The
//! neighbour heuristic prunes a candidate at distance zero from an already
//! chosen neighbour, so separate points for such rows leave most of them with
//! no incoming link: before this, a build of exact duplicates or signed zeros
//! left points the entry point could not reach. Each family now holds one
//! point per stored representation and every point is reachable, for one
//! worker and for several, F32 and quantized. Positive scalar multiples are
//! distinct representations: they keep a point each.
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

fn direction(seed: usize, dimension: usize) -> Vec<f32> {
    (0..dimension)
        .map(|axis| mixed((seed * 1_000 + axis) as u64))
        .collect()
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

/// Build the family with `workers` and require one point per graph identity
/// and no unreachable point.
fn assert_family(label: &str, vectors: Vec<Vec<f32>>, identities: usize) {
    let dimension = vectors[0].len();
    let entries = entries(vectors);
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        for workers in [1, 2, 4, 8] {
            let index = HnswIndex::from_vector_entries_with_parallel_progress(
                &entries,
                dimension,
                quantization,
                policy(),
                workers,
                || false,
                |_| Ok(()),
            )
            .expect("build the graph");
            assert_eq!(
                index.unreachable_layer0_points_for_test(),
                (0, identities),
                "{label}, {quantization:?}, {workers} workers: (unreachable points, graph points)"
            );
            assert_eq!(index.len(), entries.len(), "{label}: every row is indexed");
        }
    }
}

/// Graph points of a build, for families that are not grouped.
fn points(vectors: Vec<Vec<f32>>) -> usize {
    let dimension = vectors[0].len();
    HnswIndex::from_vector_entries_with_parallel_progress(
        &entries(vectors),
        dimension,
        VectorQuantization::F32,
        policy(),
        1,
        || false,
        |_| Ok(()),
    )
    .expect("build the graph")
    .unreachable_layer0_points_for_test()
    .1
}

#[test]
fn zero_distance_families_share_one_reachable_point() {
    const IDENTITIES: usize = 60;
    const COPIES: usize = 10;
    let dimension = 32;
    // Exact duplicates.
    assert_family(
        "exact duplicates",
        (0..IDENTITIES * COPIES)
            .map(|row| direction(row / COPIES, dimension))
            .collect(),
        IDENTITIES,
    );
    // Positive scalar multiples store distinct representations: a point each.
    assert_eq!(
        points(
            (0..IDENTITIES * COPIES)
                .map(|row| {
                    let scale =
                        [1.0f32, 2.0, 0.5, 4.0, 8.0, 0.25, 16.0, 0.125, 32.0, 64.0][row % COPIES];
                    direction(row / COPIES, dimension)
                        .into_iter()
                        .map(|value| value * scale)
                        .collect()
                })
                .collect(),
        ),
        IDENTITIES * COPIES,
        "scalar multiples are not grouped"
    );
    // All-zero rows store one representation.
    assert_family("all zero", vec![vec![0.0; dimension]; COPIES * 3], 1);
    // Signed zeros: one direction whose zero coordinates differ in sign.
    assert_family(
        "signed zeros",
        (0..IDENTITIES * COPIES)
            .map(|row| {
                let mut vector = direction(row / COPIES, dimension);
                for (axis, value) in vector.iter_mut().enumerate().take(COPIES) {
                    *value = if (row % COPIES) >> (axis % 4) & 1 == 1 {
                        -0.0
                    } else {
                        0.0
                    };
                }
                vector
            })
            .collect(),
        IDENTITIES,
    );
    // The frozen tied-frontier fixture's vectors: eleven directions, each
    // stored about eleven times.
    assert_family(
        "tied fixture",
        (1..=120_i64)
            .map(|id| vec![0.0, 1.0, ((id * 11) % 121) as f32 / 120.0])
            .collect(),
        11,
    );
}
