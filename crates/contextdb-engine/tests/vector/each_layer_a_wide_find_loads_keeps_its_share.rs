//! Every saved layer a wide find loads keeps the memory share reserved for it.
//!
//! A Context whose saved state has a base graph and a change graph on top of
//! it is two loads, not one. A find over several such Contexts must open every
//! layer its snapshot needs, and each load must leave the room the layers after
//! it need and take only its share of what is free beyond that. Counting one
//! load per Context let the first half of the layers take all the headroom and
//! left every later layer the least serving cache, one page, whatever its own
//! pages -- so which Context re-read its pages over and over depended on the
//! order the find happened to load them in.
//!
//! A load a writer publishes over is thrown away and loaded again. That retry
//! is the same layer: it must not take a second share, which would erase the
//! room kept for the layers after it.
//!
//! Measured on the file route through a real `ReadSession`, fresh readers, at
//! a memory limit that holds every layer's least load and two serving pages
//! per layer besides.

use crate::a_saved_graph_load_checks_its_bytes_once::{
    Fixture, ROOMY, Shape, context_id, fresh_reader_within, generation_id, ids, index, since,
    statement,
};
use crate::a_wide_find_admits_every_partition_when_serving_pages_give_way::{
    WorkingSet, limit_for,
};
use contextdb_core::Value;
use contextdb_core::find_attribution::{LoadMeters, load_meters};
use contextdb_core::serving_cache_share::{Share, outstanding_for_test, take_claims_for_test};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadConfig};
use std::collections::HashMap;

const SHAPE: Shape = Shape {
    partitions: 4,
    rows_per_partition: 600,
    // Wide enough that each layer's pages span several serving pages.
    dimensions: 256,
};

/// Rows committed after each Context's base graph is saved, sealed into a
/// change graph on top of it: past the tenth of the base the fixture declares
/// as the point a change is sealed, and more than one serving page of vectors.
const CHANGE_ROWS_PER_PARTITION: usize = 100;

/// Every Context loads its base and its change.
const LAYERS: usize = SHAPE.partitions * 2;

const ALL: [usize; SHAPE.partitions] = [0, 1, 2, 3];

/// Slack above the calibrated working set: two serving pages per layer.
const SLACK_BYTES: usize = LAYERS * 2 * 64 * 1024;

fn embedding(ordinal: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|axis| {
            let seed = (ordinal
                .wrapping_mul(0x9e37_79b9)
                .wrapping_add(axis.wrapping_mul(0x85eb_ca6b)))
                % 10_007;
            (seed as f32 / 10_007.0) - 0.5
        })
        .collect()
}

fn insert_rows(admin: &Database, partition: usize, ordinals: std::ops::Range<usize>) {
    for ordinal in ordinals {
        admin
            .execute(
                "INSERT INTO scoped_rows VALUES ($id, $context, $generation, TRUE, $embedding)",
                &HashMap::from([
                    ("id".into(), Value::Int64(ordinal as i64 + 1)),
                    ("context".into(), Value::Uuid(context_id(partition))),
                    ("generation".into(), Value::Uuid(generation_id(partition))),
                    (
                        "embedding".into(),
                        Value::Vector(embedding(ordinal, SHAPE.dimensions)),
                    ),
                ]),
            )
            .expect("seed one partitioned row");
    }
}

/// Every Context saved as a base graph with a sealed change graph on top.
#[cfg(unix)]
fn seed_bases_with_changes() -> Fixture {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::TempDir::new().expect("task-scoped fixture directory");
    let path = directory.path().join("layered-shares.db");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir(&runtime).expect("the reader runtime directory");
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700))
        .expect("the runtime directory is private to its owner");
    let admin = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the fixture store");
    admin.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    admin
        .execute(
            &format!(
                "CREATE TABLE scoped_rows (id INT PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
                 generation_id UUID NOT NULL, is_current BOOL NOT NULL, \
                 embedding VECTOR({}) PARTITION_KEY (context_id, generation_id) \
                 MAX_PARTITIONS 8 SEARCH_MODE AUTO AUTO_INDEX_AT 1 \
                 CONSOLIDATION (CHANGE_PERCENT = 10))",
                SHAPE.dimensions
            ),
            &HashMap::new(),
        )
        .expect("declare the partitioned fixture");
    admin
        .execute(
            "CREATE INDEX scoped_rows_active_idx ON scoped_rows \
             (context_id, generation_id, is_current)",
            &HashMap::new(),
        )
        .expect("declare the index the candidate identities come from");
    let rows = SHAPE.rows_per_partition + CHANGE_ROWS_PER_PARTITION;
    for partition in 0..SHAPE.partitions {
        let first = partition * rows;
        insert_rows(&admin, partition, first..first + SHAPE.rows_per_partition);
    }
    let bases_saved = (0..4096).any(|_| {
        admin
            .run_maintenance_cycle()
            .expect("advance deterministic maintenance");
        let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
            admin
                .declared_reader_memory()
                .expect("admit the partition observation reader"),
        );

        let ready = admin
            .vector_store_for_test()
            .partition_infos(&index(), &mut partition_observation_owner_1)
            .expect("inspect partition readiness");
        ready.len() == SHAPE.partitions && ready.iter().all(|partition| partition.graph_available)
    });
    assert!(bases_saved, "every Context saves its base graph");

    for partition in 0..SHAPE.partitions {
        let first = partition * rows + SHAPE.rows_per_partition;
        insert_rows(&admin, partition, first..first + CHANGE_ROWS_PER_PARTITION);
    }
    let layered = |admin: &Database| {
        let store = admin.vector_store_for_test();
        let vector_fixture_reader_1 = admin
            .declared_reader_memory()
            .expect("admit the vector fixture reader");
        let mut vector_fixture_copy_1 =
            contextdb_engine::vector_read_memory::admission(vector_fixture_reader_1.clone());

        let partitions = store
            .partition_infos(&index(), &mut vector_fixture_copy_1)
            .expect("inspect partition readiness");
        partitions.len() == SHAPE.partitions
            && partitions.iter().all(|info| {
                store
                    .partition_graph_generation_status(&info.partition)
                    .is_some_and(|status| {
                        (status.base.is_some() || status.dormant_base.is_some())
                            && (status.change.is_some() || status.dormant_change.is_some())
                            && status.fresh_tail_entries == 0
                    })
            })
    };
    let changes_saved = (0..256).any(|_| {
        admin
            .run_maintenance_cycle()
            .expect("advance deterministic maintenance");
        layered(&admin)
    });
    assert!(
        changes_saved,
        "every Context seals its later rows into a change graph on top of its base"
    );
    admin.close().expect("seal the fixture store");
    Fixture {
        _directory: directory,
        path,
        runtime,
        shape: SHAPE,
    }
}

/// One find over every Context in a fresh reader within `limit`: what it
/// answered, what its loads were given, the shares they claimed, and what the
/// reader holds before and after it.
#[cfg(unix)]
struct Find {
    ids: Vec<Value>,
    spent: LoadMeters,
    claims: Vec<Share>,
    used_before: usize,
    used_after: usize,
    high_water: usize,
}

#[cfg(unix)]
fn find_within(fixture: &Fixture, limit: usize) -> Find {
    let (sql, params) = statement(SHAPE, &ALL, "USE VECTOR INDEXED");
    let reader = fresh_reader_within(fixture, limit);
    let (usage, ..) = reader
        .file_memory_for_test()
        .expect("a file-route reader reports its memory");
    let used_before = usage.used;
    take_claims_for_test();
    let before = load_meters();
    let answer = reader
        .execute(&sql, &params)
        .expect("every layer fits, so the wide find answers from its graphs");
    let spent = since(before, load_meters());
    let claims = take_claims_for_test();
    let (usage, _, high_water, _) = reader
        .file_memory_for_test()
        .expect("a file-route reader reports its memory");
    assert_eq!(
        outstanding_for_test(),
        (0, 0),
        "once the find returns, no share is left declared on its thread"
    );
    Find {
        ids: ids(&answer),
        spent,
        claims,
        used_before,
        used_after: usage.used,
        high_water,
    }
}

/// The limit holding every layer's least load, the statement's workspace
/// and two serving pages per layer.
#[cfg(unix)]
fn calibrated_limit(fixture: &Fixture) -> (usize, Find) {
    let roomy = find_within(fixture, ROOMY);
    assert_eq!(
        roomy.spent.loads, LAYERS as u64,
        "a fresh reader loads each Context's base and its change"
    );
    let working = WorkingSet {
        least_loads: roomy.spent.minimum_bytes as usize,
        workspace: roomy.high_water.saturating_sub(roomy.used_after),
    };
    (limit_for(fixture, &working, SLACK_BYTES as isize), roomy)
}

/// What each load claims when every layer is counted: the first load shares
/// what is free with every layer, the last takes what is left, and each keeps
/// room for the least loads of the layers still to come.
#[cfg(unix)]
fn assert_one_share_per_layer(claims: &[Share], what: &str) {
    let divisors = claims.iter().map(|share| share.divisor).collect::<Vec<_>>();
    assert_eq!(
        divisors,
        (1..=LAYERS).rev().collect::<Vec<_>>(),
        "{what}: each of the {LAYERS} layers claims one share of what is free, counted over \
         the layers still to load"
    );
    let reserved = claims
        .iter()
        .map(|share| share.reserved_for_later)
        .collect::<Vec<_>>();
    assert!(
        reserved.windows(2).all(|pair| pair[0] > pair[1]) && reserved.last() == Some(&0),
        "{what}: each load keeps room for the least loads of the layers after it, and the last \
         keeps none: {reserved:?}"
    );
}

#[cfg(unix)]
#[test]
#[ignore = "encodes the all-resident serving-cache pricing model, which contradicts the intent's on-demand paging; superseded by the paged-pricing replacement rows"]
fn every_layer_of_a_wide_find_keeps_its_share() {
    let fixture = seed_bases_with_changes();
    let (limit, oracle) = calibrated_limit(&fixture);
    let find = find_within(&fixture, limit);
    println!(
        "layered find within {limit}: loads={} refused={} least_cache={} cache={} claims={:?} \
         used {} -> {} held={}",
        find.spent.loads,
        find.spent.refused,
        find.spent.least_cache_loads,
        find.spent.cache_bytes,
        find.claims,
        find.used_before,
        find.used_after,
        find.spent.held_bytes,
    );
    assert_eq!(
        find.ids, oracle.ids,
        "memory pressure never changes the answer"
    );
    assert_eq!(find.spent.loads, LAYERS as u64, "every layer loads once");
    assert_eq!(find.spent.refused, 0, "every layer fits");
    assert_one_share_per_layer(&find.claims, "an undisturbed wide find");
    assert_eq!(
        find.spent.least_cache_loads, 0,
        "no layer is left the one-page least cache while its pages and the slack allow more"
    );
}

#[cfg(unix)]
#[test]
#[ignore = "encodes the all-resident serving-cache pricing model, which contradicts the intent's on-demand paging; superseded by the paged-pricing replacement rows"]
fn a_load_published_over_is_retried_on_the_share_it_already_claimed() {
    let fixture = seed_bases_with_changes();
    let (limit, oracle) = calibrated_limit(&fixture);
    let undisturbed = find_within(&fixture, limit);

    contextdb_vector::test_seam::arm_vector_stale_dormant_load_for_test();
    let retried = find_within(&fixture, limit);
    assert!(
        contextdb_vector::test_seam::vector_stale_dormant_load_consumed_for_test(),
        "the first load of the find was published over and thrown away"
    );
    println!(
        "retried find within {limit}: loads={} refused={} least_cache={} claims={:?} used {} -> \
         {} (undisturbed {} -> {})",
        retried.spent.loads,
        retried.spent.refused,
        retried.spent.least_cache_loads,
        retried.claims,
        retried.used_before,
        retried.used_after,
        undisturbed.used_before,
        undisturbed.used_after,
    );
    assert_eq!(
        retried.ids, oracle.ids,
        "a retried load never changes the answer"
    );
    assert_eq!(
        retried.spent.loads,
        LAYERS as u64 + 1,
        "the discarded load and its retry both loaded"
    );
    assert_eq!(retried.spent.refused, 0, "every layer fits");
    assert_eq!(
        retried.claims, undisturbed.claims,
        "the retry loads on the share its discarded load claimed; it does not claim the same \
         layer twice"
    );
    assert_one_share_per_layer(&retried.claims, "a find with one load published over");
    assert_eq!(
        retried.spent.least_cache_loads, 0,
        "no layer is left the one-page least cache because an earlier load was retried"
    );
    assert_eq!(
        retried.used_after - retried.used_before,
        undisturbed.used_after - undisturbed.used_before,
        "the discarded load returned everything it was charged: the find holds what an \
         undisturbed find holds"
    );
}
