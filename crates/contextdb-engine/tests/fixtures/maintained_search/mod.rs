//! Frozen portable inputs for the maintained-search regressions.
//!
//! The generator reproduces the original raw little-endian IEEE-754 `f32`
//! fixture byte for byte. Its fingerprints keep the workload fixed without
//! shipping the previous 30 MiB binary corpus in every source package.

mod generator;

use std::sync::OnceLock;

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_a_filtered_scope_is_charged_for_its_candidates, vector_first_filt"
)]
pub const DIMENSIONS: usize = 768;
#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_a_filtered_scope_is_charged_for_its_candidates, vector_first_filt"
)]
pub const CORPUS_ROWS: usize = 10_000;
#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_a_filtered_scope_is_charged_for_its_candidates, vector_first_filt"
)]
pub const HELD_OUT_ROWS: usize = 100;
#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_graph_recall_50000_isotropic_per_context, vector_graph_recall_500"
)]
pub const PROFILE_DIMENSIONS: usize = 64;
#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_a_filtered_scope_is_charged_for_its_candidates, vector_first_filt"
)]
pub const CORPUS_SHA256: &str = "30b962cfabd4727efd55e1569a3e1a028422c244230f9cb1e88121d0167b3d4d";
#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_a_filtered_scope_is_charged_for_its_candidates, vector_first_filt"
)]
pub const HELD_OUT_SHA256: &str =
    "c5b50de92c85b23d8fbd3cdd6070d27c29f8b8315cb78e7b5f6f462f666cd2c9";

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_a_filtered_scope_is_charged_for_its_candidates, vector_first_filt"
)]
const VECTOR_BYTES: usize = DIMENSIONS * std::mem::size_of::<f32>();

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_a_filtered_scope_is_charged_for_its_candidates, vector_first_filt"
)]
static CORPUS: OnceLock<Vec<f32>> = OnceLock::new();
#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_a_filtered_scope_is_charged_for_its_candidates, vector_first_filt"
)]
static HELD_OUT: OnceLock<Vec<f32>> = OnceLock::new();

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_a_filtered_scope_is_charged_for_its_candidates, vector_first_filt"
)]
fn generate(role: &str, rows: usize, expected_sha256: &str) -> Vec<f32> {
    let values = (0..rows)
        .flat_map(|ordinal| generator::vector(role, ordinal, DIMENSIONS))
        .collect::<Vec<_>>();
    let bytes = values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect::<Vec<_>>();
    assert_eq!(bytes.len(), rows * VECTOR_BYTES);
    assert_eq!(
        generator::hex(&generator::sha256(&bytes)),
        expected_sha256,
        "the deterministic {role} generator must reproduce the published fixture"
    );
    values
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_a_filtered_scope_is_charged_for_its_candidates, vector_first_filt"
)]
fn generated_vector(
    vectors: &'static OnceLock<Vec<f32>>,
    role: &str,
    ordinal: usize,
    rows: usize,
    expected_sha256: &str,
) -> Vec<f32> {
    assert!(
        ordinal < rows,
        "frozen fixture ordinal {ordinal} is outside {rows} rows"
    );
    let vectors = vectors.get_or_init(|| generate(role, rows, expected_sha256));
    vectors[ordinal * DIMENSIONS..(ordinal + 1) * DIMENSIONS].to_vec()
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_a_filtered_scope_is_charged_for_its_candidates, vector_first_filt"
)]
pub fn corpus_vector(ordinal: usize) -> Vec<f32> {
    generated_vector(&CORPUS, "corpus", ordinal, CORPUS_ROWS, CORPUS_SHA256)
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_a_filtered_scope_is_charged_for_its_candidates, vector_first_filt"
)]
pub fn held_out_vector(ordinal: usize) -> Vec<f32> {
    generated_vector(
        &HELD_OUT,
        "held-out",
        ordinal,
        HELD_OUT_ROWS,
        HELD_OUT_SHA256,
    )
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_graph_recall_50000_isotropic_per_context, vector_graph_recall_500"
)]
pub fn profile_vector(shape: &str, seed: usize, role: &str, ordinal: usize) -> Vec<f32> {
    generator::vector_for(shape, seed, role, ordinal, PROFILE_DIMENSIONS)
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_first_filtered_search_in_a_fresh_reader_uses_the_saved_graph, vec"
)]
pub fn corpus_partition(ordinal: usize) -> i64 {
    i64::try_from(ordinal / 1_250).expect("eight frozen corpus partitions fit i64")
}

#[allow(
    dead_code,
    reason = "KEEP-SHARED: shared test helper used by vector, vector_a_filtered_scope_is_charged_for_its_candidates, vector_first_filt"
)]
pub fn corpus_id(ordinal: usize) -> i64 {
    i64::try_from(ordinal + 1).expect("frozen corpus ids fit i64")
}
