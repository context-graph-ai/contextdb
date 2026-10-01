//! Acceptance: "Durable rows, indexes and their metadata must load on demand rather than remain
//! wholly resident." Deleting one edge must seek its identity rather than decode unrelated edges.

use contextdb_core::{Direction, Wallclock};
use contextdb_engine::persistence::RedbPersistence;
use contextdb_engine::writer_query_test_seams::observe;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;
use uuid::Uuid;

#[test]
fn graph_delete_seeks_only_its_edge_identity() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let source = Uuid::from_u128(1);
    let target = Uuid::from_u128(2);
    let mut measured = Vec::new();
    for unrelated in [64_u128, 128] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("edges.redb");
        let database = Database::open(&path).unwrap();
        database.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let tx = database.begin_or_panic();
        database
            .insert_edge(tx, source, target, "links".to_owned(), HashMap::new())
            .unwrap();
        for ordinal in 0..unrelated {
            database
                .insert_edge(
                    tx,
                    Uuid::from_u128(1000 + ordinal * 2),
                    Uuid::from_u128(1001 + ordinal * 2),
                    "links".to_owned(),
                    HashMap::new(),
                )
                .unwrap();
        }
        database.commit(tx).unwrap();
        let insert_tx = tx;
        let old_snapshot = database.snapshot();
        let old = database.pin_snapshot(old_snapshot);
        let tx = database.begin_or_panic();
        database.delete_edge(tx, source, target, "links").unwrap();
        let (result, work) = observe(|| database.commit(tx));
        result.unwrap();
        assert_eq!(
            database
                .edge_count(source, "links", database.snapshot())
                .unwrap(),
            0
        );
        assert_eq!(
            database.edge_count(source, "links", old_snapshot).unwrap(),
            1
        );
        for (node, direction) in [(source, Direction::Outgoing), (target, Direction::Incoming)] {
            let traversal = database
                .query_bfs(node, None, direction, 1, database.snapshot())
                .unwrap();
            assert!(
                traversal.nodes.is_empty(),
                "both live adjacency directions lose the deleted edge"
            );
        }
        drop(old);
        database.close().unwrap();
        drop(database);
        let persistence = RedbPersistence::open(&path).unwrap();
        let mut forward = persistence.load_forward_edges().unwrap();
        let mut reverse = persistence.load_reverse_edges().unwrap();
        forward.sort_by_key(|edge| (edge.source, edge.target, edge.created_tx));
        reverse.sort_by_key(|edge| (edge.source, edge.target, edge.created_tx));
        assert_eq!(forward.len(), unrelated as usize + 1);
        assert_eq!(reverse.len(), forward.len());
        let mut expected_identities = vec![(source, target, "links".to_owned())];
        expected_identities.extend((0..unrelated).map(|ordinal| {
            (
                Uuid::from_u128(1000 + ordinal * 2),
                Uuid::from_u128(1001 + ordinal * 2),
                "links".to_owned(),
            )
        }));
        expected_identities.sort();
        assert_eq!(
            forward
                .iter()
                .map(|edge| (edge.source, edge.target, edge.edge_type.clone()))
                .collect::<Vec<_>>(),
            expected_identities,
            "every durable edge has exactly its inserted identity"
        );
        for (fwd, rev) in forward.iter().zip(&reverse) {
            assert_eq!(fwd.created_tx, insert_tx);
            assert!(fwd.properties.is_empty());
            assert_eq!(
                (
                    fwd.source,
                    fwd.target,
                    &fwd.edge_type,
                    fwd.created_tx,
                    fwd.deleted_tx,
                    fwd.lsn,
                    &fwd.properties
                ),
                (
                    rev.source,
                    rev.target,
                    &rev.edge_type,
                    rev.created_tx,
                    rev.deleted_tx,
                    rev.lsn,
                    &rev.properties
                ),
                "forward and reverse durable versions agree exactly"
            );
            if fwd.source == source && fwd.target == target {
                assert_eq!(
                    fwd.deleted_tx,
                    Some(tx),
                    "the selected version carries exactly this delete transaction"
                );
            } else {
                assert!(
                    fwd.deleted_tx.is_none(),
                    "unrelated edge versions are unchanged"
                );
            }
        }
        measured.push(work.graph_delete_entries_decoded);
    }
    assert_eq!(
        measured[0], measured[1],
        "deleting one edge must decode equal entries beside N and 2N unrelated edges"
    );
    assert_eq!(
        measured[0], 1,
        "the delete probe decodes only its exact live edge identity"
    );
}
