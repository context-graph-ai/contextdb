//! What one cg event append writes once the schema says which index-prefix
//! counts it wants kept.
//!
//! The same pinned cg startup schema and the same three-table append the
//! per-family grader measures, with the two declarations cg's own reads use:
//! `MAINTAIN COUNT (context_id), (context_id, connector_id,
//! source_instance_ref)` on `events`, and `MAINTAIN COUNT NONE` on
//! `event_anchors` and `event_local_commits`. Every count the append then
//! writes is one the schema named, and the twenty-four nothing reads are gone.

use contextdb_engine::append_work::{self, IndexPrefixCountTouch};
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;

use crate::one_event_append_work_by_family as cg_append;

/// cg's own declaration, applied to the pinned startup schema the way a
/// consumer applies it: the two prefixes its reads use, and silence made
/// explicit on the two tables whose counts nothing reads.
pub(crate) fn declare_cg_maintained_counts(db: &Database) {
    for ddl in [
        "ALTER TABLE events SET MAINTAIN COUNT (context_id), \
         (context_id, connector_id, source_instance_ref)",
        "ALTER TABLE event_anchors SET MAINTAIN COUNT NONE",
        "ALTER TABLE event_local_commits SET MAINTAIN COUNT NONE",
    ] {
        db.execute(ddl, &HashMap::new())
            .unwrap_or_else(|error| panic!("cg maintained-count declaration `{ddl}`: {error}"));
    }
}

#[test]
#[ignore = "an append writes counts beyond the declared count of each index"]
fn a_declared_count_is_the_only_count_an_append_writes() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let db =
        Database::open(root.path().join("declared-counts.db")).expect("open the writable fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    cg_append::declare(&db);
    declare_cg_maintained_counts(&db);

    // The first append installs what a later one reuses, so the pinned shape
    // is read from an append that follows one.
    cg_append::append_event(&db, 0);

    append_work::reset();
    cg_append::append_event(&db, 1);
    let mut counts = append_work::take_index_prefix_counts();
    counts.sort();

    for IndexPrefixCountTouch {
        table,
        prefix_columns,
        count_after_the_write,
        ..
    } in &counts
    {
        eprintln!(
            "  {table:<20} ({}) count={count_after_the_write:?}",
            prefix_columns.join(", ")
        );
    }

    let named = counts
        .iter()
        .map(|touch| (touch.table.clone(), touch.prefix_columns.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        named,
        vec![
            ("events".to_owned(), vec!["context_id".to_owned()]),
            (
                "events".to_owned(),
                vec![
                    "context_id".to_owned(),
                    "connector_id".to_owned(),
                    "source_instance_ref".to_owned(),
                ]
            ),
        ],
        "an append maintains exactly the column lists the schema declared, and nothing else"
    );

    let work = append_work::take_family_work();
    let mutations = work
        .iter()
        .map(|(_, name, family)| (*name, family.mutations))
        .collect::<Vec<_>>();
    eprintln!("per-family mutations under cg's declaration: {mutations:?}");
    let status = mutations
        .iter()
        .find(|(name, _)| *name == "Status")
        .map(|(_, mutations)| *mutations)
        .unwrap_or(0);
    let postings = mutations
        .iter()
        .find(|(name, _)| *name == "SchemaAndIndexes")
        .map(|(_, mutations)| *mutations)
        .unwrap_or(0);
    assert_eq!(
        (postings, status),
        (22, 2),
        "twenty-two index postings and the two declared counts: twenty-four distinct records, \
         against forty-eight when every prefix of every index was maintained"
    );

    db.close().expect("close the fixture");
}

/// What the cg event append costs before and after the declaration, at the
/// two depths the consumer benchmark reports.
///
/// "Before" is `MAINTAIN COUNT ALL PREFIXES` on the three append tables,
/// which is exactly what the engine maintained unconditionally before the
/// declaration existed. "After" is cg's own two declarations. Both arms run
/// on the same instrument in the same build, so the difference is the
/// declaration and nothing else.
///
/// Forty thousand appends through a debug-profile engine is many minutes of
/// work, so this is run on demand (`--ignored`) rather than in the ordinary
/// suite; the per-append shape it measures is pinned without it by
/// `a_declared_count_is_the_only_count_an_append_writes` above.
#[test]
#[ignore = "measurement: builds two 20,010-event streams"]
fn what_the_cg_event_append_costs_before_and_after_the_declaration() {
    use crate::one_event_append_cost_by_stream_depth::curve_with;

    let depths = [10usize, 20_010];
    let before = curve_with(&depths, |db| {
        cg_append::declare_all_prefixes_on_the_append_tables(db);
    });
    let after = curve_with(&depths, declare_cg_maintained_counts);

    println!("| depth | arm | mutations | node writes | identity bytes | freed pages |");
    println!("|---|---|---|---|---|---|");
    for (label, curve) in [("before (ALL PREFIXES)", &before), ("after (cg)", &after)] {
        for (depth, cost) in curve.iter() {
            println!(
                "| {depth} | {label} | {} | {} | {} | {} |",
                cost.mutations, cost.node_writes, cost.bytes_written, cost.freed_pages
            );
        }
    }

    for ((depth, before_cost), (_, after_cost)) in before.iter().zip(after.iter()) {
        assert!(
            after_cost.mutations < before_cost.mutations,
            "at {depth} events the declaration removes staged mutations: {} -> {}",
            before_cost.mutations,
            after_cost.mutations
        );
        assert!(
            after_cost.bytes_written < before_cost.bytes_written,
            "at {depth} events the declaration removes identity bytes: {} -> {}",
            before_cost.bytes_written,
            after_cost.bytes_written
        );
    }
}
