#![cfg(feature = "test-seams")]
//! The clustered list index, below the engine: committed inserts stay inside a
//! fixed write-buffer cap and are searchable at once; a one-Context search
//! reads only that Context's list segments and fewer pages than an
//! all-Context search; EXACT compares every allowed vector at full precision
//! and returns the full-precision order where int8 records tie, while INDEXED
//! does strictly less complete distance work (centroid, int8, canonical and
//! write-buffer comparisons all count).
//!
//! The guards read the index through [`ClusteredLists`]. Each method states
//! what it must do; the index binds each one to its own surface, and a method
//! must delegate to the production surface rather than duplicate its algorithm. The
//! guard bodies do not change when a binding is made.

use std::collections::BTreeSet;

/// One stored row: its Context, its canonical row identity and its vector.
#[derive(Clone, Debug)]
struct Row {
    context: u64,
    row: u64,
    vector: Vec<f32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Exact,
    Indexed,
}

/// The declared shape of one clustered index.
#[derive(Clone, Copy, Debug)]
struct Declared {
    dimensions: usize,
    lists: usize,
    probes: usize,
    rerank: usize,
    /// Byte threshold at which the write buffer seals a list run.
    buffer_cap_bytes: usize,
}

#[derive(Clone, Debug, Default)]
struct Searched {
    /// Row identities, nearest first, ties in ascending row identity.
    rows: Vec<u64>,
    /// List pages (base lists and sealed runs) read, each read counted.
    list_pages_read: u64,
    /// Contexts whose list-segment pages were read.
    contexts_read: BTreeSet<u64>,
    /// Every distance computation is counted in exactly one of these four.
    /// Query-to-centroid comparisons, including codebook selection.
    centroid_evaluations: u64,
    /// Int8 list-record comparisons.
    quantized_evaluations: u64,
    /// Canonical-precision comparisons against list-generation vectors.
    full_precision_evaluations: u64,
    /// Exact comparisons against the capped, unsealed write buffer.
    buffer_evaluations: u64,
}

impl Searched {
    fn distance_evaluations(&self) -> u64 {
        self.centroid_evaluations
            + self.quantized_evaluations
            + self.full_precision_evaluations
            + self.buffer_evaluations
    }

    fn full_precision_comparisons(&self) -> u64 {
        self.full_precision_evaluations + self.buffer_evaluations
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Buffer {
    cap_bytes: usize,
    /// Most bytes the buffer has held since the index was built.
    high_water_bytes: usize,
    /// List runs sealed since the index was built.
    sealed_runs: u64,
}

#[path = "support/ivf_pages.rs"]
mod ivf_pages;

/// The native page fixture supplies storage only; the production clustered index
/// executes every build, commit, search and buffer observation below.
struct ClusteredLists {
    index: contextdb_vector::ivf::ClusteredIndex<ivf_pages::Pages>,
    budget: std::sync::Arc<ivf_pages::Budget>,
}
impl ClusteredLists {
    fn build(declared: Declared, rows: &[Row]) -> Self {
        use contextdb_vector::ivf::{ClusteredIndex, Policy, TrainingPolicy};
        let budget = std::sync::Arc::new(ivf_pages::Budget::default());
        let policy = Policy::checked(
            declared.dimensions,
            declared.lists as u32,
            declared.probes as u32,
            declared.rerank,
            declared.buffer_cap_bytes,
            true,
        )
        .unwrap();
        let training = TrainingPolicy {
            iterations: 10,
            reservoir_samples: 300000,
            seed: 11,
            ingest_batch_keys: 64,
            phase_batch_entries: 256,
        };
        let mut index = ClusteredIndex::new(
            ivf_pages::Pages::default(),
            policy,
            training,
            budget.clone().into(),
        )
        .unwrap();
        for context in rows.iter().map(|r| r.context).collect::<BTreeSet<_>>() {
            index
                .build_partition(
                    context,
                    rows.iter()
                        .filter(|r| r.context == context)
                        .map(|r| (r.row, r.vector.as_slice())),
                )
                .unwrap();
        }
        Self { index, budget }
    }
    fn commit_insert(&mut self, row: Row) {
        self.index
            .commit_insert(contextdb_vector::ivf::Vector {
                partition: row.context,
                row: row.row,
                values: &row.vector,
            })
            .unwrap();
    }
    fn search(&self, query: &[f32], k: usize, contexts: Option<&[u64]>, mode: Mode) -> Searched {
        use contextdb_vector::ivf;
        self.index.storage().contexts.borrow_mut().clear();
        let mut hits = vec![ivf::Hit::default(); k];
        let mode = match mode {
            Mode::Exact => ivf::Mode::Exact,
            Mode::Indexed => ivf::Mode::Indexed,
        };
        let (count, work) = ivf::search(
            self.index.storage(),
            self.index.policy(),
            &self.budget.clone().into(),
            query,
            contexts,
            mode,
            &mut hits,
        )
        .unwrap();
        Searched {
            rows: hits[..count].iter().map(|h| h.row).collect(),
            list_pages_read: work.list_pages_read,
            contexts_read: self.index.storage().contexts.borrow().clone(),
            centroid_evaluations: work.centroid_evaluations,
            quantized_evaluations: work.quantized_evaluations,
            full_precision_evaluations: work.full_precision_evaluations,
            buffer_evaluations: work.buffer_evaluations,
        }
    }
    fn buffer(&self) -> Buffer {
        let b = self.index.buffer();
        Buffer {
            cap_bytes: b.cap_bytes,
            high_water_bytes: b.high_water_bytes,
            sealed_runs: b.sealed_runs,
        }
    }
}

fn unit_vector(seed: u64, dimensions: usize) -> Vec<f32> {
    let mut state = seed
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0xD1B5_4A32_D192_ED03);
    let mut values = Vec::with_capacity(dimensions);
    for _ in 0..dimensions {
        state ^= state >> 33;
        state = state.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        state ^= state >> 33;
        values.push(((state >> 11) as f64 / (1_u64 << 53) as f64 * 2.0 - 1.0) as f32);
    }
    let norm = values
        .iter()
        .map(|v| f64::from(*v).powi(2))
        .sum::<f64>()
        .sqrt();
    values
        .into_iter()
        .map(|v| (f64::from(v) / norm) as f32)
        .collect()
}

fn cosine_distance(left: &[f32], right: &[f32]) -> f64 {
    let dot = left
        .iter()
        .zip(right)
        .map(|(l, r)| f64::from(*l) * f64::from(*r))
        .sum::<f64>();
    let norm = |v: &[f32]| v.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
    1.0 - dot / (norm(left) * norm(right))
}

fn ranked(query: &[f32], rows: &[Row], k: usize) -> Vec<u64> {
    let mut ranked = rows
        .iter()
        .map(|row| (cosine_distance(query, &row.vector), row.row))
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| left.partial_cmp(right).unwrap());
    ranked.into_iter().take(k).map(|(_, row)| row).collect()
}

fn rows(contexts: u64, per_context: u64, dimensions: usize) -> Vec<Row> {
    (0..contexts)
        .flat_map(|context| {
            (0..per_context).map(move |offset| {
                let row = context * per_context + offset;
                Row {
                    context,
                    row,
                    vector: unit_vector(row, dimensions),
                }
            })
        })
        .collect()
}

const BUFFER_DIMENSIONS: usize = 64;
const BUFFER_CAP_BYTES: usize = 16 * 1024;
/// N inserts carry four times the cap in raw vector bytes alone.
const N: u64 = (4 * BUFFER_CAP_BYTES / (BUFFER_DIMENSIONS * 4)) as u64;

fn insert_one_at_a_time(inserts: u64) -> Buffer {
    let declared = Declared {
        dimensions: BUFFER_DIMENSIONS,
        lists: 4,
        probes: 4,
        rerank: 64,
        buffer_cap_bytes: BUFFER_CAP_BYTES,
    };
    let mut index = ClusteredLists::build(declared, &rows(1, 64, BUFFER_DIMENSIONS));
    for row in 64..64 + inserts {
        let vector = unit_vector(row, BUFFER_DIMENSIONS);
        index.commit_insert(Row {
            context: 0,
            row,
            vector: vector.clone(),
        });
        assert_eq!(
            index.search(&vector, 1, None, Mode::Indexed).rows,
            vec![row],
            "row {row} is searchable when its insert returns"
        );
    }
    index.buffer()
}

#[test]
fn twice_as_many_inserts_never_raise_the_write_buffer_high_water() {
    let n = insert_one_at_a_time(N);
    let two_n = insert_one_at_a_time(2 * N);
    for buffer in [n, two_n] {
        assert_eq!(buffer.cap_bytes, BUFFER_CAP_BYTES);
        assert!(
            buffer.high_water_bytes > 0,
            "inserts went through the buffer: {buffer:?}"
        );
        assert!(
            buffer.high_water_bytes <= buffer.cap_bytes,
            "the buffer never grew past its cap: {buffer:?}"
        );
    }
    assert!(
        n.sealed_runs >= 1,
        "four caps of inserts sealed at least one run: {n:?}"
    );
    assert!(
        two_n.sealed_runs > n.sealed_runs,
        "doubling the inserts seals more runs instead of growing the buffer"
    );
    assert_eq!(
        two_n.high_water_bytes, n.high_water_bytes,
        "N and 2N reach the same high-water"
    );
}

#[test]
fn a_one_context_search_reads_only_its_segments_and_fewer_pages_than_all() {
    let declared = Declared {
        dimensions: 16,
        lists: 8,
        probes: 2,
        rerank: 32,
        buffer_cap_bytes: 1 << 20,
    };
    let index = ClusteredLists::build(declared, &rows(4, 150, 16));
    let query = unit_vector(3, 16);
    let one = index.search(&query, 10, Some(&[0]), Mode::Indexed);
    let several = index.search(&query, 10, Some(&[0, 1]), Mode::Indexed);
    let all = index.search(&query, 10, None, Mode::Indexed);
    for searched in [&one, &several, &all] {
        assert_eq!(
            searched.rows.len(),
            10,
            "a full page of answers: {searched:?}"
        );
    }
    assert_eq!(one.contexts_read, BTreeSet::from([0]), "{one:?}");
    assert_eq!(several.contexts_read, BTreeSet::from([0, 1]), "{several:?}");
    assert_eq!(all.contexts_read, BTreeSet::from([0, 1, 2, 3]), "{all:?}");
    assert!(one.list_pages_read > 0, "{one:?}");
    assert!(
        one.list_pages_read < all.list_pages_read,
        "one Context reads strictly fewer list pages than all: one {one:?}, all {all:?}"
    );
}

fn angle(radians: f64) -> Vec<f32> {
    vec![radians.cos() as f32, radians.sin() as f32]
}

#[test]
fn exact_returns_full_precision_order_where_int8_records_tie() {
    // Eight neighbours 0.001 rad apart inside one list whose coordinates span
    // the circle; interleaved identities so no identity order is distance
    // order.
    let tie_rows = [105_u64, 101, 108, 103, 106, 102, 107, 104];
    let mut stored = tie_rows
        .iter()
        .enumerate()
        .map(|(rank, row)| Row {
            context: 0,
            row: *row,
            vector: angle(0.5 + 0.001 * rank as f64),
        })
        .collect::<Vec<_>>();
    for (row, radians) in [(1, 2.0), (2, 3.0), (3, 4.0), (4, 5.0), (5, -0.9), (6, 1.6)] {
        stored.push(Row {
            context: 0,
            row,
            vector: angle(radians),
        });
    }
    let declared = Declared {
        dimensions: 2,
        lists: 1,
        probes: 1,
        rerank: 16,
        buffer_cap_bytes: 1 << 20,
    };
    let index = ClusteredLists::build(declared, &stored);
    let query = angle(0.45);
    assert_eq!(
        ranked(&query, &stored, 3),
        vec![105, 101, 108],
        "the fixture's full-precision order"
    );
    let exact = index.search(&query, 3, None, Mode::Exact);
    assert_eq!(
        exact.rows,
        vec![105, 101, 108],
        "EXACT returns the full-precision order"
    );
    assert_eq!(
        exact.full_precision_comparisons(),
        stored.len() as u64,
        "EXACT compares every stored vector exactly once at full precision: {exact:?}"
    );
    assert_eq!(
        index.search(&query, 3, None, Mode::Indexed).rows,
        vec![105, 101, 108],
        "INDEXED re-ranks its candidates at full precision"
    );
}

#[test]
fn exact_rescores_every_allowed_vector_and_indexed_does_strictly_less_work() {
    let declared = Declared {
        dimensions: 16,
        lists: 16,
        probes: 2,
        rerank: 20,
        buffer_cap_bytes: 1 << 20,
    };
    let stored = rows(2, 600, 16);
    let mut index = ClusteredLists::build(declared, &stored);
    let tail = (5_000..5_005)
        .map(|row| Row {
            context: 0,
            row,
            vector: unit_vector(row, 16),
        })
        .collect::<Vec<_>>();
    for row in &tail {
        index.commit_insert(row.clone());
    }
    let allowed = stored
        .iter()
        .filter(|row| row.context == 0)
        .cloned()
        .chain(tail)
        .collect::<Vec<_>>();
    let query = unit_vector(77_777, 16);
    let exact = index.search(&query, 10, Some(&[0]), Mode::Exact);
    assert_eq!(
        exact.rows,
        ranked(&query, &allowed, 10),
        "EXACT is the full-precision answer"
    );
    assert_eq!(
        exact.full_precision_comparisons(),
        allowed.len() as u64,
        "EXACT compares every allowed vector, built and tail, exactly once: {exact:?}"
    );
    let indexed = index.search(&query, 10, Some(&[0]), Mode::Indexed);
    assert!(
        indexed.centroid_evaluations > 0,
        "INDEXED's centroid comparisons are observed and counted: {indexed:?}"
    );
    assert!(
        exact.buffer_evaluations <= 5,
        "each buffered vector is compared at most once: {exact:?}"
    );
    assert_eq!(
        indexed.buffer_evaluations, exact.buffer_evaluations,
        "both routes compare the capped buffer exactly, once per buffered vector"
    );
    assert!(
        indexed.distance_evaluations() < exact.distance_evaluations(),
        "INDEXED does strictly less complete distance work: indexed {indexed:?}, exact {exact:?}"
    );
    assert!(indexed.full_precision_comparisons() < exact.full_precision_comparisons());
}
