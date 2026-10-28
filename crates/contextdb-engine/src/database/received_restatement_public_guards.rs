#[cfg(feature = "test-seams")]
mod received_restatement_public_guards {
    use super::*;
    fn peer(root: &std::path::Path, name: &str, file: bool) -> JourneyPeer {
        if file {
            JourneyPeer::open(root, name)
        } else {
            JourneyPeer {
                db: Database::open_memory(),
                path: root.join(format!("{name}.db")),
                identity: Arc::new(
                    crate::identity::FabricIdentity::load_or_generate(
                        &root.join(format!("{name}.key")),
                    )
                    .unwrap(),
                ),
            }
        }
    }
    fn sql(db: &Database, statement: &str) {
        db.execute(statement, &HashMap::new()).unwrap();
    }
    fn public_pending_count(edge: &JourneyPeer, hub: &JourneyPeer) -> usize {
        let transport = crate::transport::in_process::InProcessBroker::new();
        let _server_identity = transport.server_as(&hub.node_id());
        let client =
            crate::sync_client::SyncClient::with_authenticated_transport_and_identity_for_test(
                Arc::new(edge.db.scoped_with_constraints(None, None, None)),
                transport.client_as(&edge.node_id()),
                TenantId::from(JOURNEY_TENANT),
                edge.identity.clone(),
            )
            .unwrap();
        let count = client.pending_push_change_count().unwrap();
        assert!(
            transport.recorded_exchanges().is_empty(),
            "pending inspection performs no network request"
        );
        count
    }

    fn complete_public_restatement(file: bool, orphan: bool) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let hub = peer(root.path(), "hub", file);
        let edge = peer(root.path(), "edge", file);
        for db in [&hub.db, &edge.db] {
            sql(db, "CREATE TABLE bootstrap (id INTEGER PRIMARY KEY)");
            sql(db, "ALTER TABLE bootstrap ADD COLUMN extra TEXT");
        }
        if orphan {
            sql(&edge.db, "CREATE TABLE local_only (id INTEGER PRIMARY KEY)");
        }
        assert_eq!(public_pending_count(&edge, &hub), 2 + usize::from(orphan));
        let full = served_by(&hub, Lsn(0));
        assert_eq!(full.ddl.len(), 2);
        pulled_from(&edge, &hub, full);
        let check = |edge: &JourneyPeer| {
            assert_eq!(
                public_pending_count(edge, &hub),
                usize::from(orphan),
                "full matching public pull settles only matching local declaration history"
            );
            assert!(edge
                .db
                .execute("SELECT extra FROM bootstrap", &HashMap::new())
                .unwrap()
                .rows()
                .is_empty());
            if orphan {
                assert!(edge.db.table_meta("local_only").is_some());
            }
        };
        check(&edge);
        if file {
            let edge = edge.reopen();
            check(&edge);
        }
    }
    #[test]
    fn restatement_complete_public_memory() {
        complete_public_restatement(false, false);
    }
    #[test]
    fn restatement_complete_public_file() {
        complete_public_restatement(true, false);
    }
    #[test]
    fn restatement_public_local_only_stays_pending_memory() {
        complete_public_restatement(false, true);
    }
    #[test]
    fn restatement_public_local_only_stays_pending_file() {
        complete_public_restatement(true, true);
    }
}
