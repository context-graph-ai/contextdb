//! Diagnostic sibling of `multi_partition_union_recovers_nearest_rows`: the
//! same uniform fixture, measured on the frozen probes or on a held-out probe
//! set drawn from the same generator with another ordinal base. For every
//! true neighbour an indexed page misses it reports the partition walk that
//! owns it, the stop that walk took, and where an observer-only continuation
//! past that stop first evaluates it (expansion index and parent, with the
//! parent's rank among the rows the walk had evaluated and in its frontier).
//!
//! Run explicitly: `cargo test --test vector --features test-seams
//! multi_partition_union_recall_beam_diagnostic -- --ignored --nocapture`.
//! `UNION_RECALL_QUERY_BASE` (default 10000000, the frozen probes) chooses the
//! probe ordinals; `UNION_RECALL_QUERIES` (default 100) how many;
//! `UNION_RECALL_RADII` (comma separated, default: the index's own) the
//! search radii to measure; `UNION_RECALL_SCOPES` (default `3,8`);
//! `UNION_RECALL_QUANTIZATION` (`F32` default, `SQ8`, `SQ4`) the column's
//! stored representation.

use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use contextdb_vector::hnsw::{
    UnionWalkTrace, with_union_search_radius, with_union_search_receipts, with_union_walk_traces,
};
use std::collections::{HashMap, HashSet};

const PARTITIONS: usize = 8;
const ROWS_PER_PARTITION: usize = 1_250;
const DIMENSIONS: usize = 768;
const LIMIT: usize = 10;
const CONTINUATION_EXPANSIONS: usize = 96;

fn embedding(ordinal: usize) -> Vec<f32> {
    let mut state = (ordinal as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15).max(1);
    (0..DIMENSIONS)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 40) as f32 / 16_777_216.0) - 0.5
        })
        .collect()
}

fn cosine_distance(left: &[f32], right: &[f32]) -> f64 {
    let (mut dot, mut l, mut r) = (0f64, 0f64, 0f64);
    for (a, b) in left.iter().zip(right) {
        dot += f64::from(*a) * f64::from(*b);
        l += f64::from(*a) * f64::from(*a);
        r += f64::from(*b) * f64::from(*b);
    }
    1.0 - dot / (l.sqrt() * r.sqrt())
}

fn ids(result: &QueryResult) -> HashSet<i64> {
    result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Int64(id)] => *id,
            other => panic!("nearest-row query must project one integer id: {other:?}"),
        })
        .collect()
}

fn seeded_uniform_store(quantization: &str) -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE union_recall (id INTEGER PRIMARY KEY, scope_id INTEGER NOT NULL, \
             embedding VECTOR(768) WITH (quantization = '{quantization}') \
             PARTITION_KEY (scope_id) MAX_PARTITIONS 8 \
             SEARCH_MODE INDEXED AUTO_INDEX_AT 1 \
             HNSW (M = 16, EF_CONSTRUCTION = 200))"
        ),
        &HashMap::new(),
    )
    .expect("declare eight maintained vector partitions");
    let tx = db.begin().expect("begin fixture ingestion");
    for partition in 0..PARTITIONS {
        for ordinal in 0..ROWS_PER_PARTITION {
            let global = partition * ROWS_PER_PARTITION + ordinal;
            db.insert_row(
                tx,
                "union_recall",
                HashMap::from([
                    ("id".into(), Value::Int64((global + 1) as i64)),
                    ("scope_id".into(), Value::Int64(partition as i64)),
                    ("embedding".into(), Value::Vector(embedding(global + 1))),
                ]),
            )
            .expect("stage one stored vector");
        }
    }
    db.commit(tx).expect("commit all stored vectors");
    for _ in 0..64 {
        db.run_maintenance_cycle()
            .expect("advance deterministic maintenance");
        let status = db.vector_store_for_test().hnsw_maintenance_status();
        if status.ready_partitions == PARTITIONS && status.remaining_partitions == 0 {
            return db;
        }
    }
    panic!("all eight partitions must publish their maintained graphs");
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn percentile(values: &[usize], fraction: f64) -> usize {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = ((sorted.len() as f64) * fraction).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn describe_miss(id: i64, distance: f64, traces: &[UnionWalkTrace]) -> String {
    let id = id as u64;
    let Some(trace) = traces.iter().find(|trace| {
        trace.continuation.iter().any(|entry| entry.row_id == id)
            || trace.evaluated.iter().any(|entry| entry.row_id == id)
            || trace.frontier_at_stop.iter().any(|entry| entry.0 == id)
    }) else {
        return format!(
            "  miss id={id} d={distance:.6}: not reached by any walk within {CONTINUATION_EXPANSIONS} continuation expansions"
        );
    };
    let stop = trace
        .stop
        .map(|stop| {
            format!(
                "stop head id={} d={:.6} vs kept[{}] worst d={:.6} [{}] ties={} evals={} frontier={}",
                stop.head_row,
                stop.head_distance,
                stop.kept,
                stop.worst_kept_distance,
                if stop.head_is_farther {
                    "Greater"
                } else {
                    "Equal"
                },
                stop.worst_ties,
                stop.evaluations,
                stop.frontier_len,
            )
        })
        .unwrap_or_else(|| "no rule stop (frontier ran empty)".into());
    if let Some(evaluated) = trace.evaluated.iter().find(|entry| entry.row_id == id) {
        return format!(
            "  miss id={id} d={distance:.6} partition={} expansions={} {stop}; EVALUATED by the walk at expansion {} yet not returned",
            trace.partition, trace.expansions, evaluated.expansion
        );
    }
    let Some(found) = trace.continuation.iter().find(|entry| entry.row_id == id) else {
        return format!(
            "  miss id={id} d={distance:.6} partition={} expansions={} {stop}; not reached within {CONTINUATION_EXPANSIONS} continuation expansions",
            trace.partition, trace.expansions
        );
    };
    let parent = found.parent.unwrap_or(u64::MAX);
    let mut by_distance: Vec<_> = trace
        .evaluated
        .iter()
        .map(|entry| (entry.distance, entry.row_id))
        .collect();
    by_distance.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
    let evalrank = by_distance
        .iter()
        .position(|entry| entry.1 == parent)
        .map_or("-".into(), |rank| (rank + 1).to_string());
    // The stop declined its head; a stop-less walk expands it first.
    let head = trace.stop.map(|stop| stop.head_row);
    let frontrank = if head == Some(parent) {
        "1".to_string()
    } else {
        trace
            .frontier_at_stop
            .iter()
            .position(|entry| entry.0 == parent)
            .map_or("-".into(), |rank| (rank + 2).to_string())
    };
    let parent_distance = by_distance
        .iter()
        .find(|entry| entry.1 == parent)
        .map_or(f32::NAN, |entry| entry.0);
    format!(
        "  miss id={id} d={distance:.6} partition={} expansions={} {stop}; continuation evaluates it at expansion #{} (short by {}) via parent id={parent} d={parent_distance:.6} evalrank={evalrank} frontrank={frontrank}",
        trace.partition,
        trace.expansions,
        found.expansion,
        found.expansion - trace.expansions,
    )
}

#[test]
#[ignore = "diagnostic: run explicitly with --ignored"]
fn multi_partition_union_recall_beam_diagnostic() {
    let base = env_usize("UNION_RECALL_QUERY_BASE", 10_000_000);
    let queries = env_usize("UNION_RECALL_QUERIES", 100);
    let radii: Vec<Option<f32>> = std::env::var("UNION_RECALL_RADII")
        .ok()
        .map(|list| {
            list.split(',')
                .map(|radius| Some(radius.trim().parse().expect("radius")))
                .collect()
        })
        .unwrap_or_else(|| vec![None]);
    let wanted: Vec<usize> = std::env::var("UNION_RECALL_SCOPES")
        .ok()
        .map(|list| {
            list.split(',')
                .map(|scope| scope.trim().parse().expect("scope"))
                .collect()
        })
        .unwrap_or_else(|| vec![3, 8]);
    let quantization =
        std::env::var("UNION_RECALL_QUANTIZATION").unwrap_or_else(|_| "F32".to_string());
    let db = seeded_uniform_store(&quantization);
    let scopes = [(3usize, " WHERE scope_id IN (0, 1, 2)"), (8, "")];
    for (radius, (scope, predicate)) in radii.iter().flat_map(|radius| {
        scopes
            .into_iter()
            .filter(|(scope, _)| wanted.contains(scope))
            .map(move |scope| (*radius, scope))
    }) {
        let mut matched_total = 0usize;
        let mut low = Vec::new();
        let mut work = Vec::new();
        let mut admission = Vec::new();
        let mut cut_beyond = Vec::new();
        let mut digest = 0xcbf29ce484222325u64;
        for probe in 0..queries {
            let query = embedding(base + probe);
            let params = HashMap::from([("query".into(), Value::Vector(query.clone()))]);
            let statement = |mode: &str| {
                format!(
                    "SELECT id FROM union_recall{predicate} ORDER BY embedding <=> $query \
                     USE VECTOR {mode} LIMIT {LIMIT}"
                )
            };
            let truth = ids(&db
                .execute(&statement("EXACT"), &params)
                .expect("exhaustive search of the stored vectors"));
            let run = || {
                with_union_walk_traces(CONTINUATION_EXPANSIONS, || {
                    with_union_search_receipts(|| db.execute(&statement("INDEXED"), &params))
                })
            };
            let ((indexed, receipts), traces) = match radius {
                Some(radius) => with_union_search_radius(radius, run),
                None => run(),
            };
            let [receipt] = receipts.as_slice() else {
                panic!("one union search must serve each query: {receipts:?}");
            };
            let found = ids(&indexed.expect("indexed page"));
            let mut ordered: Vec<_> = found.iter().copied().collect();
            ordered.sort_unstable();
            for id in ordered {
                for byte in id.to_le_bytes() {
                    digest = (digest ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                }
            }
            digest = (digest ^ 0xff).wrapping_mul(0x100000001b3);
            work.push(receipt.admission_evaluations + receipt.expansion_evaluations);
            admission.push(receipt.admission_evaluations);
            let matched = found.intersection(&truth).count();
            matched_total += matched;
            if matched < LIMIT {
                low.push((probe, matched));
                let expansions: Vec<_> = traces.iter().map(|trace| trace.expansions).collect();
                eprintln!(
                    "radius {radius:?} scope {scope} probe {probe} (ordinal {}) {matched}/{LIMIT} walk expansions {expansions:?} work {}",
                    base + probe,
                    work.last().unwrap()
                );
                let mut misses: Vec<_> = truth
                    .difference(&found)
                    .map(|id| (*id, cosine_distance(&embedding(*id as usize), &query)))
                    .collect();
                misses.sort_by(|left, right| left.1.total_cmp(&right.1));
                for (id, distance) in misses {
                    let line = describe_miss(id, distance, &traces);
                    if let Some(short) = line
                        .split("(short by ")
                        .nth(1)
                        .and_then(|rest| rest.split(')').next())
                        .and_then(|value| value.parse::<usize>().ok())
                    {
                        cut_beyond.push(short);
                    }
                    eprintln!("{line}");
                }
            }
        }
        let rows = scope * ROWS_PER_PARTITION;
        eprintln!(
            "SUMMARY quantization={quantization} radius={radius:?} base={base} queries={queries} scope={scope} recall={matched_total}/{} low={low:?} work_mean={:.1} work_p95={} work_max={} admission_mean={:.1} exact_rows={rows} shortfalls={cut_beyond:?} answer_digest={digest:016x}",
            queries * LIMIT,
            work.iter().sum::<usize>() as f64 / queries as f64,
            percentile(&work, 0.95),
            work.iter().max().unwrap(),
            admission.iter().sum::<usize>() as f64 / queries as f64,
        );
    }
}
