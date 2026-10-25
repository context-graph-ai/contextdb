#![cfg(feature = "test-seams")]
//! The clustered list index, below the engine: committed inserts stay inside a
//! fixed write-buffer cap and are searchable at once; a one-Context search
//! reads only that Context's list segments and fewer pages than an
//! all-Context search; EXACT compares every allowed vector at full precision
//! and returns the full-precision order where int8 records tie, while INDEXED
//! does strictly less distance work.
//!
//! The guards read the index through [`ClusteredLists`]. Each method states
//! what it must do; the index binds each one to its own surface, and a method
//! that still panics with [`UNBOUND`] means the surface is not there yet. The
//! guard bodies do not change when a binding is made.

use std::collections::BTreeSet;

const UNBOUND: &str = "the vector crate does not expose this clustered list index surface";

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
    quantized_evaluations: u64,
    full_precision_evaluations: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Buffer {
    cap_bytes: usize,
    /// Most bytes the buffer has held since the index was built.
    high_water_bytes: usize,
    /// List runs sealed since the index was built.
    sealed_runs: u64,
}

/// The clustered list index under guard.
struct ClusteredLists;

impl ClusteredLists {
    /// Builds and publishes one generation per Context over `rows`, cosine
    /// distance, each with the declared list count.
    fn build(_declared: Declared, _rows: &[Row]) -> Self {
        unimplemented!("{UNBOUND}: build a published generation")
    }

    /// Commits one insert. When this returns the row is searchable.
    fn commit_insert(&mut self, _row: Row) {
        unimplemented!("{UNBOUND}: commit one insert")
    }

    /// Searches the named Contexts (`None`: every Context) for the `k`
    /// nearest rows.
    fn search(
        &self,
        _query: &[f32],
        _k: usize,
        _contexts: Option<&[u64]>,
        _mode: Mode,
    ) -> Searched {
        unimplemented!("{UNBOUND}: search")
    }

    fn buffer(&self) -> Buffer {
        unimplemented!("{UNBOUND}: write-buffer state")
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
        exact.full_precision_evaluations,
        stored.len() as u64,
        "EXACT compares every stored vector once at full precision: {exact:?}"
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
        exact.full_precision_evaluations,
        allowed.len() as u64,
        "EXACT compares every allowed vector, built and tail, once: {exact:?}"
    );
    let indexed = index.search(&query, 10, Some(&[0]), Mode::Indexed);
    assert!(
        indexed.quantized_evaluations + indexed.full_precision_evaluations
            < exact.quantized_evaluations + exact.full_precision_evaluations,
        "INDEXED does strictly fewer distance evaluations: indexed {indexed:?}, exact {exact:?}"
    );
    assert!(indexed.full_precision_evaluations < exact.full_precision_evaluations);
}
