/// Already resolved engine policy. SQL validation/default resolution lives above this layer.
#[derive(Clone, Copy, Debug)]
pub struct Policy {
    pub(super) dimensions: usize,
    pub(super) lists: u32,
    pub(super) probes: u32,
    pub(super) rerank: usize,
    pub(super) buffer_cap_bytes: usize,
    pub(super) extend_to_k: bool,
    pub(super) max_delta_runs: u64,
    pub(super) coalesce_pages_per_step: Option<usize>,
}
impl Policy {
    pub fn dimensions(self) -> usize {
        self.dimensions
    }
    pub fn lists(self) -> u32 {
        self.lists
    }
    pub fn probes(self) -> u32 {
        self.probes
    }
    pub fn rerank(self) -> usize {
        self.rerank
    }
    pub fn buffer_cap_bytes(self) -> usize {
        self.buffer_cap_bytes
    }
    pub fn extend_to_k(self) -> bool {
        self.extend_to_k
    }

    /// Resolved declared MAX_DELTA_RUNS (engine default two).
    pub fn with_max_delta_runs(mut self, runs: u64) -> Option<Self> {
        if runs == 0 || runs.checked_add(1).is_none() {
            return None;
        }
        self.max_delta_runs = runs;
        Some(self)
    }
    pub fn max_delta_runs(self) -> u64 {
        self.max_delta_runs
    }
    /// The engine passes the resolved declared maintenance page allowance.
    pub fn with_coalesce_pages_per_step(mut self, pages: usize) -> Option<Self> {
        if pages == 0 {
            return None;
        }
        self.coalesce_pages_per_step = Some(pages);
        Some(self)
    }

    /// Reject a policy before any allocation; the declaring layer supplies its typed diagnostic.
    pub fn checked(
        dimensions: usize,
        lists: u32,
        probes: u32,
        rerank: usize,
        buffer_cap_bytes: usize,
        extend_to_k: bool,
    ) -> Option<Self> {
        let record = dimensions.checked_mul(4)?.checked_add(40)?;
        if dimensions == 0
            || lists == 0
            || probes == 0
            || probes > lists
            || rerank == 0
            || record > buffer_cap_bytes
        {
            return None;
        }
        dimensions.checked_add(16)?;
        dimensions.checked_mul(5)?;
        dimensions.checked_mul(lists as usize)?.checked_mul(4)?;
        Some(Self {
            dimensions,
            lists,
            probes,
            rerank,
            buffer_cap_bytes,
            extend_to_k,
            max_delta_runs: 2,
            coalesce_pages_per_step: None,
        })
    }
}

/// Inputs are finite; the engine rejects NaN/infinity with a typed InvalidVectorValue.
#[derive(Clone, Copy, Debug)]
pub struct Vector<'a> {
    pub partition: u64,
    pub row: u64,
    pub values: &'a [f32],
}
/// Canonical row version, independent of its physical generation or slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowIdentity {
    pub row: u64,
    pub commit_lsn: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Exact,
    Indexed,
    /// Planner-selected index attempt with a canonical fallback on derived corruption.
    Auto,
}
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Hit {
    pub row: u64,
    pub distance: f64,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Work {
    pub centroid_evaluations: u64,
    pub quantized_evaluations: u64,
    pub full_precision_evaluations: u64,
    /// Actual canonical re-ranks of quantized candidates; a subset of full-precision work.
    pub reranked_candidates: u64,
    pub buffer_evaluations: u64,
    pub list_pages_read: u64,
    pub codebook_pages_read: u64,
    pub accumulator_pages_read: u64,
    pub directory_pages_read: u64,
    pub directory_presence_pages_read: u64,
    pub row_version_pages_read: u64,
    pub row_copy_pages_read: u64,
    pub canonical_pages_read: u64,
    pub visibility_pages_read: u64,
    pub lists_probed: u64,
    pub catalog_pages_read: u64,
    pub slot_metadata_pages_read: u64,
    pub tail_pages_read: u64,
    pub list_link_pages_read: u64,
    /// Actual scores on selected EXACT routes, never inferred from an oracle.
    pub exact_score_count: u64,
}
impl Work {
    pub fn distance_evaluations(self) -> u64 {
        self.centroid_evaluations
            + self.quantized_evaluations
            + self.full_precision_evaluations
            + self.buffer_evaluations
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BufferState {
    pub cap_bytes: usize,
    pub high_water_bytes: usize,
    pub sealed_runs: u64,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct Candidate {
    pub slot: u64,
    pub row: u64,
    pub distance: f64,
}

pub(crate) fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let mut dot = 0.0;
    let mut aa = 0.0;
    let mut bb = 0.0;
    for (&x, &y) in a.iter().zip(b) {
        let x = f64::from(x);
        let y = f64::from(y);
        dot += x * y;
        aa += x * x;
        bb += y * y;
    }
    if aa == 0.0 || bb == 0.0 {
        1.0
    } else {
        1.0 - dot / (aa.sqrt() * bb.sqrt())
    }
}
pub(crate) fn order(a: &Candidate, b: &Candidate) -> std::cmp::Ordering {
    a.distance
        .total_cmp(&b.distance)
        .then_with(|| a.row.cmp(&b.row))
}
pub(crate) fn retain_best(candidates: &mut Vec<Candidate>, capacity: usize, item: Candidate) {
    if capacity == 0 {
        return;
    }
    let at = candidates
        .binary_search_by(|x| order(x, &item))
        .unwrap_or_else(|i| i);
    if at >= capacity {
        return;
    }
    if candidates.len() == capacity {
        candidates.pop();
    }
    candidates.insert(at, item);
}

/// Resolved declared maintenance policy. No training or phase-size default is hidden here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrainingPolicy {
    pub iterations: u32,
    pub reservoir_samples: usize,
    pub seed: u64,
    pub ingest_batch_keys: usize,
    pub phase_batch_entries: usize,
}

/// A committed candidate's physical serving role in its captured generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CandidateSource {
    ServingBase,
    CommittedRun,
    LegacyRun,
    UnsealedTail,
}

/// Identity supplied by the captured native scope, independent of current rows.
#[derive(Clone, Copy, Debug)]
pub struct CandidateIdentity<'a> {
    pub index: &'a contextdb_core::VectorIndexRef,
    pub partition: u64,
    pub generation: u64,
    pub run: u64,
    pub slot: u64,
    pub row: RowIdentity,
    pub snapshot_lsn: contextdb_core::Lsn,
    pub source: CandidateSource,
}

/// A stored-precision vector borrowed for one fallible canonical visit.
pub struct CanonicalCandidate<'a> {
    pub identity: CandidateIdentity<'a>,
    pub values: &'a [f32],
    pub distance: f64,
}
