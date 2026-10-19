//! A find is admitted at what its saved-graph loads will actually hold, and
//! each load leaves room for the loads still to come.
//!
//! A load asks the memory budget for what it is about to hold. That ask must
//! be what a paged load holds -- its topology, its row mappings and the least
//! serving cache -- read from the saved topology, not the whole saved graph or
//! a fixed figure per point: on the owner's laptop at 100,000 passages over
//! eight Contexts the old catalog price asked for 974 MB for five partitions
//! whose loads held about 21 MB each, and a load's own admission charged
//! 1,664 bytes per point whatever the graph held.
//!
//! Each load then sizes its serving cache from what is free. It must leave the
//! least load of every partition the statement has still to load: taking a
//! share of all that is free let early loads take the room a later one needed
//! for its topology, so a wide find refused its own last partitions.
//!
//! Measured on the file route through a real `ReadSession`, fresh readers,
//! at one, two and five partitions.

use crate::a_saved_graph_load_checks_its_bytes_once::{
    ROOMY, fresh_reader_within, ids, seed_shape, since, statement,
};
use crate::a_wide_find_admits_every_partition_when_serving_pages_give_way::{
    ALL, SHAPE, SLACK_BYTES, calibrate, limit_for,
};
use contextdb_core::find_attribution::load_meters;

/// The least serving cache a load may be given: one page.
const LEAST_CACHE_BYTES: u64 = 64 * 1024;

/// How far a load's admission at its least cache may sit above what it then
/// holds: the fixed allowances every load is admitted with -- the headers
/// around its topology and its own workspace, both returned or unused once
/// the graph is decoded. Nothing proportional to the graph.
const ADMISSION_SLACK_PER_LOAD: u64 = 256 * 1024;

#[cfg(unix)]
#[test]
#[ignore = "encodes the all-resident serving-cache pricing model, which contradicts the intent's on-demand paging; superseded by the paged-pricing replacement rows"]
fn a_wide_find_is_priced_at_what_its_loads_hold() {
    let fixture = seed_shape(SHAPE);
    let mut failures = Vec::new();
    for selected in [1_usize, 2, SHAPE.partitions] {
        let partitions = (0..selected).collect::<Vec<_>>();
        let (sql, params) = statement(SHAPE, &partitions, "USE VECTOR INDEXED");
        let reader = fresh_reader_within(&fixture, ROOMY);
        let before = load_meters();
        reader
            .execute(&sql, &params)
            .expect("the first find answers");
        let spent = since(before, load_meters());
        assert_eq!(spent.loads, selected as u64, "one load per partition");
        // What the loads hold with the least cache each: what they hold, less
        // the part of their cache above one page.
        let held_least = spent.held_bytes - spent.cache_bytes + spent.loads * LEAST_CACHE_BYTES;
        println!(
            "priced at {selected} partitions: priced={} held_least={} minimum={} saved={}",
            spent.priced_bytes, held_least, spent.minimum_bytes, spent.saved_bytes
        );
        if spent.minimum_bytes < held_least {
            failures.push(format!(
                "at {selected} partitions the loads were admitted at {} but hold {held_least}",
                spent.minimum_bytes
            ));
        }
        let ceiling = held_least + spent.loads * ADMISSION_SLACK_PER_LOAD;
        if spent.minimum_bytes > ceiling {
            failures.push(format!(
                "at {selected} partitions the loads were admitted at {}, more than {ceiling} for \
                 loads that hold {held_least}",
                spent.minimum_bytes
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[cfg(unix)]
#[test]
#[ignore = "encodes the all-resident serving-cache pricing model, which contradicts the intent's on-demand paging; superseded by the paged-pricing replacement rows"]
fn each_load_leaves_room_for_the_loads_still_to_come() {
    let fixture = seed_shape(SHAPE);
    let working = calibrate(&fixture);
    let limit = limit_for(&fixture, &working, SLACK_BYTES as isize);
    let (sql, params) = statement(SHAPE, &ALL, "USE VECTOR INDEXED");
    let oracle = fresh_reader_within(&fixture, ROOMY)
        .execute(&sql, &params)
        .expect("with room to spare the wide find answers");

    let reader = fresh_reader_within(&fixture, limit);
    let before = load_meters();
    let answer = reader
        .execute(&sql, &params)
        .expect("every topology fits, so the wide find answers");
    let spent = since(before, load_meters());
    println!(
        "shares within {limit}: loads={} refused={} given_back={} cache={}",
        spent.loads, spent.refused, spent.given_back_bytes, spent.cache_bytes
    );
    assert_eq!(ids(&answer), ids(&oracle));
    assert_eq!(spent.loads, SHAPE.partitions as u64);
    assert_eq!(
        spent.given_back_bytes, 0,
        "no load of this find had to take serving pages back from an earlier load of the same find"
    );
}
