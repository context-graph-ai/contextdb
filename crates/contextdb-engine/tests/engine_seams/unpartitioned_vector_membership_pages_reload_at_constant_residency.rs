//! "Vigil and unpartitioned vectors" and "Writable startup": populated vector
//! memberships load and release within bounded memory, including unpartitioned columns.
use crate::resident_history_support as support;
use contextdb_core::{Value, VectorPartitionKey, Wallclock};
use contextdb_engine::database::resident_history_test_seams::observe;
use contextdb_vector::VectorPartitionRef;
use support::*;

fn displace_membership_pages(db: &contextdb_engine::Database) {
    let before = db.__writer_cache_stats_for_test().unwrap();
    for id in 0..64 {
        assert_eq!(
            exec(
                db,
                &format!("SELECT body FROM membership_pressure WHERE id = {id}")
            ),
            vec![vec![Value::Text("p".repeat(262144))]]
        );
    }
    let after = db.__writer_cache_stats_for_test().unwrap();
    assert!(
        after.2 > before.2,
        "disjoint durable body pages must actually evict membership source pages"
    );
}

#[test]
fn unpartitioned_vector_membership_pages_reload_at_constant_residency() {
    if isolated_test(
        module_path!(),
        "unpartitioned_vector_membership_pages_reload_at_constant_residency",
        "CONTEXTDB_MEMBERSHIP_ALLOCATION_ARM",
    ) {
        return;
    }
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let mut directory_bytes = Vec::new();
    const DIM: usize = 256;
    const MEMBERSHIP_WORKSPACE: usize = 64 * 1024;
    for n in [512, 1024] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("membership.db");
        let db = open(&path, Some(64 * 1024 * 1024));
        exec(
            &db,
            "CREATE TABLE unpartitioned_items (id INT PRIMARY KEY, v VECTOR(256)) HISTORY ALL",
        );
        exec(
            &db,
            "CREATE TABLE partitioned_items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(256) PARTITION_KEY (scope) MAX_PARTITIONS 4) HISTORY ALL",
        );
        for id in 0..n {
            insert_vector(&db, "unpartitioned_items", id, None, DIM);
            insert_vector(&db, "partitioned_items", id, Some((id % 2) as i64), DIM);
        }
        exec(
            &db,
            "CREATE TABLE membership_pressure (id INT PRIMARY KEY, body TEXT)",
        );
        for id in 0..64 {
            db.execute(
                "INSERT INTO membership_pressure VALUES ($id,$body)",
                &std::collections::HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("body".into(), Value::Text("p".repeat(262144))),
                ]),
            )
            .unwrap();
        }
        let old = db.snapshot();
        db.close().unwrap();
        drop(db);
        let db = open(&path, Some(64 * 1024 * 1024));
        let store = db.vector_store_for_test();
        let initial = store.resident_membership_directory_bytes_for_test();
        let mut reloaded = 0;
        for (table, key, scope) in [
            (
                "unpartitioned_items",
                VectorPartitionKey::unpartitioned(),
                None,
            ),
            ("partitioned_items", partition(0), Some(0)),
            ("partitioned_items", partition(1), Some(1)),
        ] {
            let ids = (0..n)
                .filter(|id| scope.is_none_or(|scope| (*id % 2) as i64 == scope))
                .collect::<Vec<_>>();
            assert_eq!(
                vector_rows(&db, table, old, scope),
                vector_oracle(ids.iter().copied(), DIM)
            );
            assert_eq!(
                exact_partition_ids(&db, table, old, scope, DIM, n),
                ids.iter().map(|id| *id as i64).collect::<Vec<_>>()
            );
            // Drop any legacy body owner, then displace the actual shared page
            // cache. A streamed implementation need not have a partition body.
            store.evict_raw_partition_for_test(&index(table), &key);
            displace_membership_pages(&db);
            let receipt = db.writer_memory_ownership_receipt_for_test();
            // Limit transitions preserve the cache allocated at open. Admit only
            // one workspace above the actual settled owners, including that cache.
            let operation_limit = receipt.named_owner_total + MEMBERSHIP_WORKSPACE;
            assert!(
                n * DIM * std::mem::size_of::<f32>() > MEMBERSHIP_WORKSPACE,
                "active unpartitioned payload cannot fit the admitted membership workspace"
            );
            db.set_memory_limit(Some(operation_limit)).unwrap();
            let baseline = db.accountant().usage().used;
            db.accountant().reset_high_water_for_test();
            #[cfg(target_os = "linux")]
            let allocation_baseline = {
                use crate::writer_reopen_and_doubling_keep_a_fixed_resident_envelope as allocation;
                allocation::reset_allocation_peak_for_test();
                allocation::live_allocations_for_test()
            };
            let (bounded, bounded_work) = observe(|| {
                db.execute_at_snapshot(
                    &format!(
                        "SELECT id FROM {table}{} ORDER BY v <=> $v USE VECTOR EXACT LIMIT 1",
                        scope.map_or(String::new(), |scope| format!(" WHERE scope = {scope}"))
                    ),
                    &std::collections::HashMap::from([(
                        "v".into(),
                        Value::Vector(vector(ids[0], DIM)),
                    )]),
                    old,
                )
            });
            assert!(
                bounded.is_ok(),
                "active membership larger than workspace must stream through admitted source windows: {bounded:?}; {bounded_work:?}"
            );
            assert_eq!(
                bounded.unwrap().rows,
                vec![vec![Value::Int64(ids[0] as i64)]]
            );
            #[cfg(target_os = "linux")]
            {
                use crate::writer_reopen_and_doubling_keep_a_fixed_resident_envelope as allocation;
                let actual_peak =
                    allocation::allocation_peak_for_test().saturating_sub(allocation_baseline);
                assert!(
                    actual_peak <= MEMBERSHIP_WORKSPACE,
                    "real membership allocation peak must fit the workspace, including uncharged temporary directories: {actual_peak}"
                );
            }
            let peak = db
                .accountant()
                .high_water_for_test()
                .saturating_sub(baseline);
            assert!(
                peak > 0 && peak <= MEMBERSHIP_WORKSPACE,
                "membership operation has a positive bounded allocation peak: {peak}; {bounded_work:?}"
            );
            assert!(
                bounded_work.membership_keys_examined >= ids.len(),
                "no resident mirror fallback; every exact candidate comes from durable membership: {bounded_work:?}"
            );
            db.set_memory_limit(Some(64 * 1024 * 1024)).unwrap();
            store.evict_raw_partition_for_test(&index(table), &key);
            displace_membership_pages(&db);
            let before = db.__writer_cache_stats_for_test().unwrap();
            let (answer, work) = observe(|| exact_partition_ids(&db, table, old, scope, DIM, n));
            assert_eq!(answer, ids.iter().map(|id| *id as i64).collect::<Vec<_>>());
            let after = db.__writer_cache_stats_for_test().unwrap();
            assert!(
                after.1 > before.1,
                "eviction requires a real durable membership/body page reload"
            );
            assert!(
                work.membership_envelopes_verified >= ids.len()
                    && work.membership_keys_examined >= ids.len(),
                "each reloaded membership envelope is verified: {work:?}"
            );
            reloaded += 1;
        }
        assert_eq!(reloaded, 3);
        exec(&db, "UPDATE partitioned_items SET scope = 2 WHERE id = 0");
        exec(&db, "DELETE FROM unpartitioned_items WHERE id = 1");
        let current = db.snapshot();
        for (table, scope, ids) in [
            (
                "unpartitioned_items",
                None,
                (0..n).filter(|id| *id != 1).collect::<Vec<_>>(),
            ),
            ("partitioned_items", Some(0), (2..n).step_by(2).collect()),
            ("partitioned_items", Some(1), (1..n).step_by(2).collect()),
            ("partitioned_items", Some(2), vec![0]),
        ] {
            assert_eq!(
                vector_rows(&db, table, current, scope),
                vector_oracle(ids.iter().copied(), DIM),
                "every current body after moves/deletes"
            );
            assert_eq!(
                exact_partition_ids(&db, table, current, scope, DIM, n),
                ids.iter().map(|id| *id as i64).collect::<Vec<_>>(),
                "runtime membership after moves/deletes"
            );
        }
        assert_eq!(
            exact_partition_ids(&db, "unpartitioned_items", old, None, DIM, n),
            (0..n as i64).collect::<Vec<_>>()
        );
        for scope in 0..2 {
            assert_eq!(
                exact_partition_ids(&db, "partitioned_items", old, Some(scope), DIM, n),
                (0..n as i64)
                    .filter(|id| id % 2 == scope)
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                vector_rows(&db, "partitioned_items", old, Some(scope)),
                vector_oracle((0..n).filter(|id| *id % 2 == scope as usize), DIM)
            );
        }
        assert_eq!(
            vector_rows(&db, "unpartitioned_items", old, None),
            vector_oracle(0..n, DIM)
        );
        let receipt = db.writer_memory_ownership_receipt_for_test();
        assert_eq!(receipt.reconciliation_gap, 0);
        directory_bytes.push((
            initial,
            store.resident_membership_directory_bytes_for_test(),
        ));
        db.close().unwrap();
        drop(store);
        drop(db);
        let db = open(&path, Some(64 * 1024 * 1024));
        assert_eq!(
            exact_partition_ids(&db, "unpartitioned_items", current, None, DIM, n),
            (0..n as i64).filter(|id| *id != 1).collect::<Vec<_>>()
        );
        assert_eq!(
            exact_partition_ids(&db, "partitioned_items", current, Some(2), DIM, n),
            vec![0]
        );
        assert_eq!(
            vector_rows(&db, "unpartitioned_items", old, None),
            vector_oracle(0..n, DIM)
        );
        // Reopen the deliberately damaged fixture so the reader root includes
        // the changed envelope. Eager open and cold membership use may both
        // discover corruption, but either must verify this checksum.
        let route = VectorPartitionRef::new(
            index("unpartitioned_items"),
            VectorPartitionKey::unpartitioned(),
        );
        db.__damage_membership_envelope_for_test(&route).unwrap();
        db.close().unwrap();
        drop(db);
        let (damaged_open, open_work) = observe(|| {
            contextdb_engine::Database::open_with_options(
                &path,
                contextdb_engine::DatabaseOpenOptions {
                    maintenance_policy: contextdb_engine::MaintenancePolicy::CallerDriven,
                    memory_limit: Some(64 * 1024 * 1024),
                    ..Default::default()
                },
            )
        });
        let (refused, corruption_work) = match damaged_open {
            Err(error) => (Err(error), open_work),
            Ok(db) => {
                displace_membership_pages(&db);
                let (answer, work) = observe(|| {
                    db.execute(
                        "SELECT id FROM unpartitioned_items ORDER BY v <=> $v USE VECTOR EXACT LIMIT 1",
                        &std::collections::HashMap::from([(
                            "v".into(),
                            Value::Vector(vector(0, DIM)),
                        )]),
                    )
                });
                db.close().unwrap();
                (answer, work)
            }
        };
        assert!(
            matches!(&refused, Err(contextdb_core::Error::StoreCorrupted { reason, .. }) if reason.contains("checksum mismatch")),
            "checksum corruption must be a typed reload refusal: {refused:?}"
        );
        assert!(
            corruption_work.membership_envelopes_verified > 0,
            "the refusal must come from verifying a reloaded membership: {corruption_work:?}"
        );
    }
    assert!(
        directory_bytes
            .iter()
            .all(|(open, used)| *open == 0 && *used == 0),
        "whole membership directories must never remain resident: {directory_bytes:?}"
    );
}
