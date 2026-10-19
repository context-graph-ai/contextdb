//! A find over every authorized Context answers from every partition's saved
//! graph whenever their topologies fit the reader's memory together -- even
//! when earlier loads have already filled that memory with serving pages.
//!
//! A loaded partition's serving cache is a reservation for pages it may
//! re-read, not memory the answer depends on: a page it gives up is read again
//! from the store and authenticated again, and the answer does not change. A
//! partition's topology is different -- without it that partition cannot be
//! searched from its graph at all. So when a wide find's next load does not fit,
//! the serving pages already resident must give way to that load's topology,
//! and the find is refused, with the typed memory refusal, only when the
//! topologies it must hold at once do not fit.
//!
//! On the owner's laptop at 100,000 passages over eight Contexts, the first
//! all-Context find loaded five partitions and then had two loads refused
//! after earlier loads had taken their serving shares; those two Contexts
//! were answered by streaming 12,500 vectors each, on every find, cold and
//! warm.
//!
//! The proof calibrates a memory limit from the fixture itself: what a fresh
//! reader holds before its first find, plus every partition's least load
//! (topology and a one-page cache), plus the statement's own workspace, plus
//! a little slack. At that limit a find over one partition first takes that
//! partition's whole serving cache, and a find over all of them then needs
//! every topology.

use crate::a_saved_graph_load_checks_its_bytes_once::{
    Fixture, LIMIT, ROOMY, Shape, fresh_reader_within, ids, seed_shape, since, statement,
};
use contextdb_core::Error;
use contextdb_core::find_attribution::load_meters;

pub(crate) const SHAPE: Shape = Shape {
    partitions: 5,
    rows_per_partition: 600,
    // Wide enough that a partition's pages outweigh its topology, so a
    // serving cache is worth giving way.
    dimensions: 256,
};

/// Slack above the calibrated working set: two serving pages per partition.
pub(crate) const SLACK_BYTES: usize = SHAPE.partitions * 2 * 64 * 1024;

pub(crate) const ALL: [usize; SHAPE.partitions] = [0, 1, 2, 3, 4];

#[cfg(unix)]
fn used_at_open(fixture: &Fixture, memory: usize) -> usize {
    let reader = fresh_reader_within(fixture, memory);
    let (usage, ..) = reader
        .file_memory_for_test()
        .expect("a file-route reader reports its memory");
    usage.used
}

/// What a find over every partition needs at once beside what the reader
/// holds at open: every partition's least load, and the statement's own
/// workspace at its peak.
#[cfg(unix)]
pub(crate) struct WorkingSet {
    pub(crate) least_loads: usize,
    pub(crate) workspace: usize,
}

#[cfg(unix)]
pub(crate) fn calibrate(fixture: &Fixture) -> WorkingSet {
    let reader = fresh_reader_within(fixture, ROOMY);
    let (sql, params) = statement(fixture.shape, &ALL, "USE VECTOR INDEXED");
    let before = load_meters();
    reader
        .execute(&sql, &params)
        .expect("with room to spare a find over every partition answers");
    let spent = since(before, load_meters());
    assert_eq!(
        spent.loads, SHAPE.partitions as u64,
        "one load per partition"
    );
    let (usage, _, high_water, _) = reader
        .file_memory_for_test()
        .expect("a file-route reader reports its memory");
    WorkingSet {
        least_loads: spent.minimum_bytes as usize,
        workspace: high_water.saturating_sub(usage.used),
    }
}

/// The limit at which the reader's own open, every partition's least load,
/// the workspace and `slack` fit exactly. What a reader holds at open
/// depends on its limit (its store cache is a share of it), so the limit is
/// settled by re-measuring the open at each candidate.
#[cfg(unix)]
pub(crate) fn limit_for(fixture: &Fixture, working: &WorkingSet, slack: isize) -> usize {
    let beside = working.least_loads + working.workspace;
    let mut limit = used_at_open(fixture, ROOMY) + beside;
    for _ in 0..6 {
        let next = (used_at_open(fixture, limit) + beside).saturating_add_signed(slack);
        if next == limit {
            break;
        }
        limit = next;
    }
    limit
}

#[cfg(unix)]
#[test]
#[ignore = "encodes the all-resident serving-cache pricing model, which contradicts the intent's on-demand paging; superseded by the paged-pricing replacement rows"]
fn a_wide_find_answers_from_every_saved_graph_once_earlier_pages_give_way() {
    let fixture = seed_shape(SHAPE);
    let working = calibrate(&fixture);
    let limit = limit_for(&fixture, &working, SLACK_BYTES as isize);
    println!(
        "wide find within {limit} bytes: least loads {} workspace {}",
        working.least_loads, working.workspace
    );

    // The answer the same statement gives with room to spare: memory pressure
    // may change what is resident, never what is answered.
    let (sql, params) = statement(SHAPE, &ALL, "USE VECTOR INDEXED");
    let oracle = fresh_reader_within(&fixture, ROOMY)
        .execute(&sql, &params)
        .expect("with room to spare the wide find answers");
    assert_eq!(oracle.rows.len(), LIMIT);

    let reader = fresh_reader_within(&fixture, limit);
    // One partition alone first: its load takes the serving share a lone load
    // is given, which is its whole partition.
    let (one_sql, one_params) = statement(SHAPE, &[0], "USE VECTOR INDEXED");
    let before = load_meters();
    reader
        .execute(&one_sql, &one_params)
        .expect("a find over one partition answers");
    let lone = since(before, load_meters());
    println!(
        "lone find: loads={} admitted={} minimum={}",
        lone.loads, lone.admitted_bytes, lone.minimum_bytes
    );

    let before = load_meters();
    let answer = reader.execute(&sql, &params);
    let spent = since(before, load_meters());
    println!(
        "wide find: loads={} refused={} admitted={} minimum={}",
        spent.loads, spent.refused, spent.admitted_bytes, spent.minimum_bytes
    );
    let answer = answer.expect("every topology fits, so the wide find answers from its graphs");
    assert_eq!(
        spent.refused, 0,
        "no load is refused while serving pages could give way to it"
    );
    assert_eq!(
        spent.loads,
        (SHAPE.partitions - 1) as u64,
        "every partition not yet resident is loaded from its saved graph"
    );
    assert_eq!(ids(&answer), ids(&oracle), "the answer is unchanged");
    let (usage, ..) = reader
        .file_memory_for_test()
        .expect("a file-route reader reports its memory");
    assert!(
        usage.used <= limit,
        "the reader stays inside its limit: {} of {limit}",
        usage.used
    );

    // Warm: every partition stays resident and a repeat answers the same.
    let before = load_meters();
    let warm = reader
        .execute(&sql, &params)
        .expect("the warm wide find answers");
    let spent = since(before, load_meters());
    assert_eq!(
        (spent.loads, spent.refused),
        (0, 0),
        "a warm find loads nothing"
    );
    assert_eq!(ids(&warm), ids(&oracle));
}

#[cfg(unix)]
#[test]
#[ignore = "encodes the all-resident serving-cache pricing model, which contradicts the intent's on-demand paging; superseded by the paged-pricing replacement rows"]
fn a_wide_find_whose_topologies_do_not_fit_is_refused_by_type() {
    let fixture = seed_shape(SHAPE);
    let working = calibrate(&fixture);
    // Room for about half of the topologies. A least load is an admission
    // bound -- it includes the load's own workspace, returned once the graph
    // is decoded -- so the limit is set well below it rather than at its edge.
    let short = (working.least_loads / 2 + working.workspace) as isize;
    let limit = limit_for(&fixture, &working, -short);
    let reader = fresh_reader_within(&fixture, limit);
    let (sql, params) = statement(SHAPE, &ALL, "USE VECTOR INDEXED");
    let before = load_meters();
    let refused = reader.execute(&sql, &params);
    let spent = since(before, load_meters());
    println!(
        "short find within {limit}: loads={} refused={} admitted={} minimum={}",
        spent.loads, spent.refused, spent.admitted_bytes, spent.minimum_bytes
    );
    assert!(
        matches!(
            &refused,
            Err(Error::MemoryBudgetExceeded { operation, .. })
                if operation == "load_retained_vector_generation"
        ),
        "a find whose topologies cannot all be held is refused, never answered from part of \
         its scope: {refused:?}"
    );
}

/// Serving pages are a cache, and a cache never refuses the statement that
/// filled it. A wide find's loads share out what is free as serving pages; the
/// statement's own later reads -- its candidates, its walk -- must still find
/// room, by taking pages back, rather than be refused with memory full of
/// pages it could read again. On the owner's laptop the loads of a find over
/// three and over eight Contexts left 49 KB of 1 GiB free and the find was
/// refused asking for 65 KB of candidates.
#[cfg(unix)]
#[test]
#[ignore = "encodes the all-resident serving-cache pricing model, which contradicts the intent's on-demand paging; superseded by the paged-pricing replacement rows"]
fn a_wide_find_whose_loads_filled_memory_with_pages_still_answers() {
    let fixture = seed_shape(SHAPE);
    let working = calibrate(&fixture);
    let (sql, params) = statement(SHAPE, &ALL, "USE VECTOR INDEXED");
    // A second, wider read from the far side of the space: its walk reaches
    // pages the first one never read, and its own working memory is more than
    // the loads left free.
    let mut elsewhere = params.clone();
    if let Some(contextdb_core::Value::Vector(query)) = elsewhere.get_mut("query") {
        query.iter_mut().for_each(|value| *value = -*value);
    }
    let wide_sql = sql.replace(&format!("LIMIT {LIMIT}"), "LIMIT 500");
    let roomy = fresh_reader_within(&fixture, ROOMY);
    let oracle = roomy
        .execute(&sql, &params)
        .expect("with room to spare the wide find answers");
    let oracle_elsewhere = roomy
        .execute(&wide_sql, &elsewhere)
        .expect("with room to spare the second find answers");
    let mut failures = Vec::new();
    // Room for every topology and more serving pages than the partitions
    // hold between them, less the statement's own workspace in steps: at some
    // step the loads' pages take what the statement then needs.
    for step in 0..8_isize {
        let slack = SLACK_BYTES as isize * 4 - (working.workspace as isize) * step / 4;
        let limit = limit_for(&fixture, &working, slack.max(0));
        let reader = fresh_reader_within(&fixture, limit);
        let before = load_meters();
        let answer = reader.execute(&sql, &params);
        let spent = since(before, load_meters());
        println!(
            "pages filled memory within {limit}: loads={} refused={} given_back={} answer={:?}",
            spent.loads,
            spent.refused,
            spent.given_back_bytes,
            answer
                .as_ref()
                .map(|answer| answer.rows.len())
                .map_err(|error| error.to_string())
        );
        match answer {
            Ok(answer) if ids(&answer) == ids(&oracle) => {}
            Ok(answer) => failures.push(format!(
                "within {limit} the answer changed: {:?}",
                ids(&answer)
            )),
            Err(error) => failures.push(format!("within {limit} the find was refused: {error}")),
        }
        match reader.execute(&wide_sql, &elsewhere) {
            Ok(answer) if ids(&answer) == ids(&oracle_elsewhere) => {}
            Ok(answer) => failures.push(format!(
                "within {limit} the second answer changed: {:?}",
                ids(&answer)
            )),
            Err(error) => failures.push(format!(
                "within {limit} the second find was refused: {error}"
            )),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A partition the statement is not searching never keeps serving pages the
/// statement's own loads need. Before a find loads its partitions, idle ones
/// outside it give their pages back -- their topology stays, so they are not
/// read again -- and the find's loads take the caches they would have had on a
/// fresh reader. On the owner's laptop a find over one Context cached that
/// partition whole, and a find over three others then got caches a third the
/// size and re-read about 6,600 pages on every warm find instead of 450.
#[cfg(unix)]
#[test]
fn an_idle_partitions_pages_give_way_to_the_partitions_a_find_loads() {
    let fixture = seed_shape(SHAPE);
    let working = calibrate(&fixture);
    let limit = limit_for(&fixture, &working, SLACK_BYTES as isize * 4);
    let others = [1, 2, 3, 4];
    let (sql, params) = statement(SHAPE, &others, "USE VECTOR INDEXED");

    let fresh = fresh_reader_within(&fixture, limit);
    let before = load_meters();
    fresh
        .execute(&sql, &params)
        .expect("a fresh reader answers the find over four partitions");
    let unhindered = since(before, load_meters());

    let reader = fresh_reader_within(&fixture, limit);
    let (one_sql, one_params) = statement(SHAPE, &[0], "USE VECTOR INDEXED");
    let before = load_meters();
    reader
        .execute(&one_sql, &one_params)
        .expect("a find over partition zero alone answers");
    let lone = since(before, load_meters());
    // What partition zero keeps once its pages give way: its topology and
    // one serving page, so a find over it later does not read it again.
    let kept = lone.held_bytes - lone.cache_bytes + 64 * 1024;
    let before = load_meters();
    reader
        .execute(&sql, &params)
        .expect("the find over the other four answers");
    let after_idle = since(before, load_meters());
    println!(
        "idle pages within {limit}: fresh caches {} after an idle partition {} (it keeps {kept})",
        unhindered.cache_bytes, after_idle.cache_bytes
    );
    assert_eq!(after_idle.loads, unhindered.loads);
    assert!(
        // A load's cache is a whole number of pages, so each may round down
        // by less than one page.
        after_idle.cache_bytes + kept + after_idle.loads * 64 * 1024 >= unhindered.cache_bytes,
        "partition zero, idle and outside the find, kept serving pages the find's loads needed: \
         caches {} against {} on a fresh reader, when partition zero keeps only {kept}",
        after_idle.cache_bytes,
        unhindered.cache_bytes
    );
}
