// Received schema histories use the existing real-peer fixture: every migration below is authored by SQL and delivered through the
// public authenticated sync door. No received context or signature is fabricated.
mod received_schema_history_guards {
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
        db.execute(statement, &HashMap::new())
            .unwrap_or_else(|e| panic!("author SQL {statement}: {e:?}"));
    }

    fn received(receiver: &JourneyPeer, sender: &JourneyPeer, since: Lsn) -> PushedUnit {
        try_pushed_to_with_atomicity(receiver, sender, served_by(sender, since), true)
            .unwrap_or_else(|e| panic!("connected authenticated apply: {e}"))
    }

    fn offline(root: &std::path::Path, file: bool) -> (JourneyPeer, JourneyPeer) {
        let author = peer(root, "author", file);
        let hub = peer(root, "hub", file);
        sql(
            &author.db,
            "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST",
        );
        for (id, body) in [(1, "first"), (2, "second")] {
            sql(
                &author.db,
                &format!("INSERT INTO notes VALUES ('00000000-0000-0000-0000-{id:012}', '{body}')"),
            );
        }
        let authored = author.db.changes_since(Lsn(0)).unwrap();
        assert_eq!(authored.ddl.len(), 1);
        assert_eq!(authored.rows.len(), 2);
        assert!(authored.ddl_lsn[0] < authored.rows[0].lsn);
        assert!(authored.rows[0].lsn < authored.rows[1].lsn);
        (author, hub)
    }

    fn assert_two_rows(db: &Database) {
        let result = db
            .execute("SELECT id, body FROM notes ORDER BY id", &HashMap::new())
            .unwrap();
        assert_eq!(
            result.rows().len(),
            2,
            "replay has each authored row once in effect"
        );
        let bodies: Vec<_> = result.rows().iter().map(|r| r.get(1).cloned()).collect();
        assert_eq!(
            bodies,
            vec![
                Some(Value::Text("first".into())),
                Some(Value::Text("second".into()))
            ]
        );
    }

    fn position_order(file: bool) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let (author, hub) = offline(root.path(), file);
        let before = hub.db.current_lsn();
        let applied = received(&hub, &author, Lsn(0));
        let end = hub.db.current_lsn();
        assert_eq!(
            end,
            Lsn(before.0 + 3),
            "three authored commits reserve three ordered positions"
        );
        assert_eq!(
            applied.result.new_lsn, end,
            "acknowledgement names final acceptance position"
        );
        let check = |db: &Database| {
            let c = db.changes_since(before).unwrap();
            assert_eq!(c.ddl_lsn, vec![Lsn(before.0 + 1)]);
            assert_eq!(
                c.rows.iter().map(|r| r.lsn).collect::<Vec<_>>(),
                vec![Lsn(before.0 + 2), end]
            );
            assert_two_rows(db);
            for interior in [Lsn(before.0 + 1), Lsn(before.0 + 2)] {
                let rows = db.scan("notes", db.snapshot_at(interior)).unwrap();
                assert_eq!(
                    rows.len(),
                    2,
                    "an interior coordinate is not a partial committed snapshot"
                );
            }
        };
        check(&hub.db);
        if file {
            let hub = hub.reopen();
            check(&hub.db);
        }
    }

    fn position_resume(file: bool) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let (author, hub) = offline(root.path(), file);
        let before = hub.db.current_lsn();
        received(&hub, &author, Lsn(0));
        let end = hub.db.current_lsn();
        let check = |hub: &JourneyPeer| {
            for since in [before, Lsn(before.0 + 1), Lsn(before.0 + 2)] {
                let c = hub.db.changes_since(since).unwrap();
                assert_eq!(
                    (c.ddl.len(), c.rows.len()),
                    (1, 2),
                    "resume at {since:?} replays complete connected unit"
                );
                let replica = peer(
                    root.path(),
                    &format!("resume-{}-{}", since.0, hub.db.current_lsn().0),
                    file,
                );
                received(&replica, hub, since);
                assert_two_rows(&replica.db);
                let accepted = replica.db.current_lsn();
                received(&replica, hub, since);
                assert_eq!(
                    replica.db.current_lsn(),
                    accepted,
                    "duplicate resumed unit advances no position"
                );
                assert_two_rows(&replica.db);
                replica.db.close().unwrap();
            }
            let c = hub.db.changes_since(end).unwrap();
            assert!(
                c.ddl.is_empty() && c.rows.is_empty(),
                "resume at END is exhausted"
            );
        };
        check(&hub);
        if file {
            let hub = hub.reopen();
            check(&hub);
        }
    }

    fn position_retry_relay(file: bool) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let (author, hub) = offline(root.path(), file);
        let wire = served_by(&author, Lsn(0));
        let expected_rows = crate::protocol::wire_row_lineages(&wire);
        let expected_ddl = wire.ddl_provenance[0].author.clone();
        let first = received(&hub, &author, Lsn(0));
        let check = |hub: &JourneyPeer| {
            let end = hub.db.current_lsn();
            let retry = received(hub, &author, Lsn(0));
            assert_eq!(
                crate::protocol::WireApplyResult::from(retry.result),
                crate::protocol::WireApplyResult::from(first.result.clone()),
                "lost acknowledgement retry returns exact original outcome"
            );
            assert_eq!(hub.db.current_lsn(), end);
            assert_two_rows(&hub.db);
            let relay = served_by(hub, Lsn(0));
            assert_eq!(relay.ddl_provenance[0].author, expected_ddl);
            let lineages = crate::protocol::wire_row_lineages(&relay);
            assert_eq!(lineages.len(), 2);
            for (i, (_, key, _, lineage)) in lineages.iter().enumerate() {
                assert_eq!(key, &expected_rows[i].1, "relay preserves author order");
                assert_eq!(
                    lineage, &expected_rows[i].3,
                    "relay keeps exact signed creator identity"
                );
            }
            let next = peer(root.path(), "onward", file);
            received(&next, hub, Lsn(0));
            assert_two_rows(&next.db);
            let check_onward = |next: &JourneyPeer| {
                let onward = served_by(next, Lsn(0));
                assert_eq!(onward.ddl_provenance[0].author, expected_ddl);
                let identities = crate::protocol::wire_row_lineages(&onward);
                assert_eq!(identities.len(), 2);
                assert!(onward.ddl_lsn[0] < onward.rows[0].lsn);
                assert!(onward.rows[0].lsn < onward.rows[1].lsn);
                for (i, (_, key, _, lineage)) in identities.iter().enumerate() {
                    assert_eq!(key, &expected_rows[i].1);
                    assert_eq!(lineage, &expected_rows[i].3);
                }
            };
            check_onward(&next);
            if file {
                let next = next.reopen();
                check_onward(&next);
                next.db.close().unwrap();
            }
        };
        check(&hub);
        if file {
            let hub = hub.reopen();
            check(&hub);
        }
    }

    fn acceptance_diagnostic(file: bool) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let (author, hub) = offline(root.path(), file);
        let first = received(&hub, &author, Lsn(0));
        let end = hub.db.current_lsn();
        assert_eq!(first.result.new_lsn, end);
        let challenger = peer(root.path(), "challenger", file);
        sql(
            &challenger.db,
            "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST",
        );
        let since = challenger.db.current_lsn();
        sql(
            &challenger.db,
            "INSERT INTO notes VALUES ('00000000-0000-0000-0000-000000000001', 'refused')",
        );
        let check = |hub: &JourneyPeer| {
            let refused = received(hub, &challenger, since);
            assert_eq!(refused.result.conflicts.len(), 1);
            let conflict = &refused.result.conflicts[0];
            assert_eq!(
                conflict.winning_author_node_id.as_deref(),
                Some(author.node_id().as_str())
            );
            assert_eq!(
                conflict.hub_acceptance_position,
                Some(end),
                "winner diagnostic names whole-unit END, never interior row position"
            );
            assert_two_rows(&hub.db);
        };
        check(&hub);
        if file {
            let hub = hub.reopen();
            check(&hub);
        }
    }

    const EDGE_DDL: &str = "CREATE TABLE links (id INTEGER PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT, note TEXT) SYNC CONFLICT KEEP LATEST";
    fn row(db: &Database, id: i64, target: u128) {
        sql(
            db,
            &format!(
                "INSERT INTO links VALUES ({id}, '00000000-0000-0000-0000-000000000001', '{}', 'link', 'row')",
                uuid::Uuid::from_u128(target)
            ),
        );
    }
    fn edge(db: &Database, target: u128) -> Option<HashMap<String, Value>> {
        db.get_edge_properties(
            uuid::Uuid::from_u128(1),
            uuid::Uuid::from_u128(target),
            "link",
            db.snapshot(),
        )
        .unwrap()
    }
    fn standalone(db: &Database) {
        let tx = db.begin().unwrap();
        assert!(
            db.insert_edge(
                tx,
                uuid::Uuid::from_u128(1),
                uuid::Uuid::from_u128(2),
                "link".into(),
                HashMap::from([("independent".into(), Value::Text("keep me".into()))])
            )
            .unwrap()
        );
        db.commit(tx).unwrap();
    }
    fn assert_standalone(db: &Database) {
        assert_eq!(
            edge(db, 2),
            Some(HashMap::from([(
                "independent".into(),
                Value::Text("keep me".into())
            )])),
            "same-triple standalone edge and properties survive"
        );
    }

    fn drop_pending(file: bool) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let author = peer(root.path(), "author", file);
        let hub = peer(root.path(), "hub", file);
        standalone(&author.db);
        sql(&author.db, EDGE_DDL);
        row(&author.db, 1, 2);
        sql(&author.db, "DROP TABLE links");
        assert_standalone(&author.db);
        received(&hub, &author, Lsn(0));
        assert!(hub.db.table_meta("links").is_none());
        assert_standalone(&hub.db);
        if file {
            let hub = hub.reopen();
            assert_standalone(&hub.db);
        }
    }

    fn existing_then_retire(file: bool, mutation: &str, retirement: &str) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let author = peer(root.path(), "author", file);
        let hub = peer(root.path(), "hub", file);
        standalone(&author.db);
        sql(&author.db, EDGE_DDL);
        row(&author.db, 1, 3);
        sql(
            &author.db,
            "CREATE TABLE other_links (id INTEGER PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        );
        sql(
            &author.db,
            "INSERT INTO other_links VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000005', 'link')",
        );
        received(&hub, &author, Lsn(0));
        assert!(edge(&hub.db, 3).is_some());
        let since = author.db.current_lsn();
        sql(&author.db, mutation);
        sql(&author.db, retirement);
        received(&hub, &author, since);
        let check = |db: &Database| {
            assert_eq!(edge(db, 3), None, "old table edge is retired");
            assert_eq!(edge(db, 4), None, "earlier pending updated edge is retired");
            assert!(edge(db, 5).is_some(), "other table edge survives");
            assert_standalone(db);
            assert_eq!(
                db.execute("SELECT * FROM other_links", &HashMap::new())
                    .unwrap()
                    .rows()
                    .len(),
                1
            );
        };
        check(&hub.db);
        if file {
            let hub = hub.reopen();
            check(&hub.db);
        }
    }

    fn shape_cycle(recreate: bool) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let author = peer(root.path(), "author", true);
        let hub = peer(root.path(), "hub", true);
        sql(&author.db, EDGE_DDL);
        row(&author.db, 1, 3);
        received(&hub, &author, Lsn(0));
        let since = author.db.current_lsn();
        if recreate {
            sql(&author.db, "DROP TABLE links");
            sql(&author.db, EDGE_DDL);
            row(&author.db, 2, 4);
        } else {
            sql(&author.db, "ALTER TABLE links DROP COLUMN target_id");
            sql(&author.db, "ALTER TABLE links ADD COLUMN target_id UUID");
            sql(
                &author.db,
                "UPDATE links SET target_id = '00000000-0000-0000-0000-000000000004' WHERE id = 1",
            );
        }
        received(&hub, &author, since);
        let check = |db: &Database| {
            assert_eq!(
                edge(db, 3),
                None,
                "prior shape/incarnation has no ghost edge"
            );
            assert!(
                edge(db, 4).is_some(),
                "final schema and final row define one live edge"
            );
            assert_eq!(
                db.edge_count(uuid::Uuid::from_u128(1), "link", db.snapshot())
                    .unwrap(),
                1
            );
            let result = db
                .execute("SELECT target_id FROM links", &HashMap::new())
                .unwrap();
            assert_eq!(result.rows().len(), 1);
            assert_eq!(
                result.rows()[0].first(),
                Some(&Value::Uuid(uuid::Uuid::from_u128(4)))
            );
        };
        check(&hub.db);
        let hub = hub.reopen();
        check(&hub.db);
    }

    /// An already-known historical declaration is a no-op.
    /// The hub's CREATE equals the CREATE that made the edge's
    /// current `bootstrap` generation, so it is an already-known historical
    /// declaration and a literal no-op -- not a claim that the two peers' whole
    /// schemas are equal. The edge keeps its later column, its data and its
    /// policy; its own ALTER stays pending onward work under the occurrence
    /// bookkeeping, and the hub converges when it syncs that migration. Before
    /// this, the pull was refused, so two peers sharing a CREATE could not
    /// sync while either had migrated further.
    fn incomplete_restatement(file: bool) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let hub = peer(root.path(), "hub", file);
        let edge = peer(root.path(), "edge", file);
        for db in [&hub.db, &edge.db] {
            sql(db, "CREATE TABLE bootstrap (id INTEGER PRIMARY KEY)");
        }
        sql(&edge.db, "ALTER TABLE bootstrap ADD COLUMN extra TEXT");
        sql(&edge.db, "INSERT INTO bootstrap VALUES (1, 'still here')");
        let declared = edge.db.table_meta("bootstrap").unwrap();
        try_pulled_from(&edge, &hub, served_by(&hub, Lsn(0))).unwrap_or_else(|e| {
            panic!("the edge's own historical CREATE arriving from the hub is a no-op: {e}")
        });
        assert_eq!(
            edge.db.table_meta("bootstrap").unwrap(),
            declared,
            "the no-op adopts none of the old CREATE's shape or policy"
        );
        let pending = ChangeSet::try_from(served_by(&edge, Lsn(0))).unwrap().ddl;
        assert!(
            pending.iter().any(
                |ddl| matches!(ddl, DdlChange::AlterTable { name, .. } if name == "bootstrap")
            ),
            "the edge's own ALTER is still pending onward work"
        );
        try_pulled_from(&hub, &edge, served_by(&edge, Lsn(0)))
            .unwrap_or_else(|e| panic!("the hub converges on the edge's migration: {e}"));
        let check = |db: &Database| {
            assert_eq!(
                table_columns(db, "bootstrap"),
                vec!["id".to_string(), "extra".to_string()]
            );
            assert_eq!(
                db.execute("SELECT extra FROM bootstrap", &HashMap::new())
                    .unwrap()
                    .rows()[0]
                    .first(),
                Some(&Value::Text("still here".into()))
            );
        };
        check(&edge.db);
        check(&hub.db);
        if file {
            let edge = edge.reopen();
            check(&edge.db);
            let hub = hub.reopen();
            check(&hub.db);
        }
    }

    macro_rules! both {
        ($memory:ident, $file:ident, $run:ident) => {
            #[test]
            fn $memory() {
                $run(false);
            }
            #[test]
            fn $file() {
                $run(true);
            }
        };
    }
    both!(positions_order_memory, positions_order_file, position_order);
    both!(
        positions_resume_memory,
        positions_resume_file,
        position_resume
    );
    both!(
        positions_lost_ack_relay_memory,
        positions_lost_ack_relay_file,
        position_retry_relay
    );
    both!(
        positions_acceptance_diagnostic_memory,
        positions_acceptance_diagnostic_file,
        acceptance_diagnostic
    );
    both!(
        argon_standalone_pending_drop_memory,
        argon_standalone_pending_drop_file,
        drop_pending
    );
    both!(
        restatement_incomplete_memory,
        restatement_incomplete_file,
        incomplete_restatement
    );
    #[test]
    fn argon_drop_recreate_file() {
        shape_cycle(true);
    }
    #[test]
    fn argon_shape_loss_regain_file() {
        shape_cycle(false);
    }

    // An arriving historical CREATE is judged by the shape its own
    // history reaches, not by the receiver's final shape; and the receiver
    // advances a vector column's policy revision on the local ALTER door's
    // axes (executor.rs `next_vector_policy_revision`).
    const T_EDGE: &str =
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";
    const T_HUB: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";
    const V_EDGE: &str = "CREATE TABLE v (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED IVF (PROBES = 11)) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";
    const V_HUB: &str = "CREATE TABLE v (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED IVF (PROBES = 7)) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

    fn vector_row(db: &Database, id: i64) {
        let params = HashMap::from([
            ("id".to_string(), Value::Int64(id)),
            (
                "vector".to_string(),
                Value::Vector(vec![id as f32, 1.0, 0.5]),
            ),
        ]);
        db.execute(
            "INSERT INTO v (id, embedding) VALUES ($id, $vector)",
            &params,
        )
        .unwrap_or_else(|e| panic!("author vector row {id}: {e:?}"));
    }

    fn table_columns(db: &Database, table: &str) -> Vec<String> {
        db.table_meta(table)
            .expect("the table is present")
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    }

    fn embedding(db: &Database) -> contextdb_core::ColumnDef {
        db.table_meta("v")
            .expect("the vector table is present")
            .columns
            .into_iter()
            .find(|column| column.name == "embedding")
            .expect("the embedding column")
    }

    fn ids(db: &Database, table: &str) -> Vec<Option<Value>> {
        db.execute(
            &format!("SELECT id FROM {table} ORDER BY id"),
            &HashMap::new(),
        )
        .unwrap()
        .rows()
        .iter()
        .map(|row| row.first().cloned())
        .collect()
    }

    /// The revision a fresh replay of `db`'s own schema history reaches.
    fn projected_embedding_revision(db: &Database) -> u64 {
        let mut projected = HashMap::<String, contextdb_core::TableMeta>::new();
        for ddl in db.changes_since(Lsn(0)).unwrap().ddl {
            Database::apply_sync_table_ddl_to_projected_meta(&mut projected, &ddl);
        }
        projected["v"]
            .columns
            .iter()
            .find(|column| column.name == "embedding")
            .expect("the projected embedding column")
            .vector_policy_revision
    }

    fn peer_history_reaching_live_shape(file: bool) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let hub = peer(root.path(), "hub", file);
        let edge = peer(root.path(), "edge", file);
        sql(&hub.db, T_HUB);
        sql(
            &hub.db,
            "INSERT INTO t (id, a, b) VALUES (1, 'one', 'gone')",
        );
        sql(&hub.db, "ALTER TABLE t DROP COLUMN b");
        sql(&hub.db, "INSERT INTO t (id, a) VALUES (2, 'two')");
        sql(&edge.db, T_EDGE);
        try_pulled_from(&edge, &hub, served_by(&hub, Lsn(0))).unwrap_or_else(|e| {
            panic!("a peer history reaching the edge's own shape delivers its rows: {e}")
        });
        let check = |db: &Database| {
            assert_eq!(
                table_columns(db, "t"),
                vec!["id".to_string(), "a".to_string()]
            );
            let result = db
                .execute("SELECT id, a FROM t ORDER BY id", &HashMap::new())
                .unwrap();
            let rows: Vec<_> = result
                .rows()
                .iter()
                .map(|row| (row.first().cloned(), row.get(1).cloned()))
                .collect();
            assert_eq!(
                rows,
                vec![
                    (Some(Value::Int64(1)), Some(Value::Text("one".into()))),
                    (Some(Value::Int64(2)), Some(Value::Text("two".into()))),
                ]
            );
        };
        check(&edge.db);
        if file {
            let edge = edge.reopen();
            check(&edge.db);
        }
    }

    fn peer_vector_history_reaching_live_policy(file: bool) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let hub = peer(root.path(), "hub", file);
        let edge = peer(root.path(), "edge", file);
        sql(&hub.db, V_HUB);
        vector_row(&hub.db, 1);
        sql(
            &hub.db,
            "ALTER TABLE v ALTER COLUMN embedding SET IVF (PROBES = 11)",
        );
        vector_row(&hub.db, 2);
        sql(&edge.db, V_EDGE);
        assert_eq!(embedding(&hub.db).vector_policy_revision, 2);
        assert_eq!(embedding(&edge.db).vector_policy_revision, 1);
        try_pulled_from(&edge, &hub, served_by(&hub, Lsn(0))).unwrap_or_else(|e| {
            panic!("a peer vector history reaching the edge's own policy is accepted: {e}")
        });
        let check = |db: &Database| {
            assert_eq!(
                ids(db, "v"),
                vec![Some(Value::Int64(1)), Some(Value::Int64(2))]
            );
            let column = embedding(db);
            assert_eq!(column.ivf_probes, Some(11));
            assert_eq!(
                column.vector_policy_revision, 1,
                "the arriving ALTER changes nothing on the edge's column"
            );
        };
        check(&edge.db);
        if file {
            let edge = edge.reopen();
            check(&edge.db);
        }
    }

    fn refused_without_change(edge: JourneyPeer, hub: &JourneyPeer, table: &str, file: bool) {
        let lsn = edge.db.current_lsn();
        let ddl = edge.db.changes_since(Lsn(0)).unwrap().ddl;
        let rows = ids(&edge.db, table);
        let refused = try_pulled_from(&edge, hub, served_by(hub, Lsn(0)));
        assert!(
            refused.is_err(),
            "a history reaching another shape is a conflict"
        );
        let check = |db: &Database| {
            assert_eq!(db.current_lsn(), lsn);
            assert_eq!(db.changes_since(Lsn(0)).unwrap().ddl, ddl);
            assert_eq!(ids(db, table), rows);
        };
        check(&edge.db);
        if file {
            let edge = edge.reopen();
            check(&edge.db);
        }
    }

    fn peer_history_reaching_another_shape(file: bool) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let hub = peer(root.path(), "hub", file);
        let edge = peer(root.path(), "edge", file);
        sql(&hub.db, T_HUB);
        sql(
            &hub.db,
            "INSERT INTO t (id, a, b) VALUES (1, 'one', 'gone')",
        );
        sql(&hub.db, "ALTER TABLE t DROP COLUMN b");
        sql(&hub.db, "ALTER TABLE t ADD COLUMN c TEXT");
        sql(
            &hub.db,
            "INSERT INTO t (id, a, c) VALUES (2, 'two', 'kept')",
        );
        sql(&edge.db, T_EDGE);
        refused_without_change(edge, &hub, "t", file);

        let hub = peer(root.path(), "vector-hub", file);
        let edge = peer(root.path(), "vector-edge", file);
        sql(&hub.db, V_HUB);
        vector_row(&hub.db, 1);
        sql(
            &hub.db,
            "ALTER TABLE v ALTER COLUMN embedding SET IVF (PROBES = 9)",
        );
        vector_row(&hub.db, 2);
        sql(&edge.db, V_EDGE);
        refused_without_change(edge, &hub, "v", file);
    }

    /// Only the CREATE of the table's current generation counts: an equal
    /// CREATE from an earlier incarnation here is not proof.
    fn older_incarnation_create_is_not_restated(file: bool) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let hub = peer(root.path(), "hub", file);
        let edge = peer(root.path(), "edge", file);
        sql(&hub.db, T_HUB);
        sql(
            &hub.db,
            "INSERT INTO t (id, a, b) VALUES (1, 'one', 'kept')",
        );
        sql(&edge.db, T_HUB);
        sql(&edge.db, "DROP TABLE t");
        sql(&edge.db, T_EDGE);
        refused_without_change(edge, &hub, "t", file);
    }

    fn same_alter_history_equal_revision(file: bool) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let hub = peer(root.path(), "hub", file);
        let edge = peer(root.path(), "edge", file);
        // No rows: the local door refuses to re-quantize a nonempty index,
        // and the revision axes are pure policy.
        sql(&hub.db, V_HUB);
        // (statement, the local door's revision after it)
        let steps: [(&str, u64); 9] = [
            ("", 1),
            (
                "ALTER TABLE v ALTER COLUMN embedding SET IVF (PROBES = 11)",
                2,
            ),
            (
                "ALTER TABLE v ALTER COLUMN embedding SET IVF (PROBES = 11)",
                2,
            ),
            (
                "ALTER TABLE v ALTER COLUMN embedding SET WITH (quantization = 'SQ8')",
                3,
            ),
            (
                "ALTER TABLE v ALTER COLUMN embedding SET WITH (quantization = 'SQ4', training_seed = 7)",
                4,
            ),
            (
                "ALTER TABLE v ALTER COLUMN embedding RESET WITH (quantization)",
                5,
            ),
            ("ALTER TABLE v ADD COLUMN note TEXT", 5),
            (
                "ALTER TABLE v ALTER COLUMN embedding SET SEARCH_MODE AUTO",
                5,
            ),
            (
                "ALTER TABLE v ALTER COLUMN embedding SET AUTO_INDEX_AT 7",
                5,
            ),
        ];
        let mut since = Lsn(0);
        for (statement, expected) in steps {
            if !statement.is_empty() {
                sql(&hub.db, statement);
            }
            try_pulled_from(&edge, &hub, served_by(&hub, since))
                .unwrap_or_else(|e| panic!("the edge receives {statement:?}: {e}"));
            since = hub.db.current_lsn();
            let local = embedding(&hub.db).vector_policy_revision;
            assert_eq!(local, expected, "local door after {statement:?}");
            assert_eq!(
                embedding(&edge.db).vector_policy_revision,
                local,
                "receiver after {statement:?}"
            );
            assert_eq!(
                projected_embedding_revision(&hub.db),
                local,
                "projection after {statement:?}"
            );
        }
        if file {
            let edge = edge.reopen();
            assert_eq!(embedding(&edge.db).vector_policy_revision, 5);
        }
    }

    /// A certificate names one CREATE occurrence, never a table: a later
    /// independently authored CREATE of the same table is judged on its own
    /// history. Two genuine authors write the statements; an honest relay
    /// cannot hold both histories itself (it would refuse the second), so
    /// this row drives the judgement both refusal sites consult. The
    /// authenticated door row is
    /// `a_later_create_in_one_delivery_is_refused_whole_file`.
    #[test]
    fn a_later_create_is_judged_on_its_own_history() {
        let root = tempfile::tempdir().unwrap();
        let first = peer(root.path(), "first-author", false);
        let second = peer(root.path(), "second-author", false);
        let edge = peer(root.path(), "edge", false);
        sql(&first.db, T_HUB);
        sql(&first.db, "ALTER TABLE t DROP COLUMN b");
        sql(
            &second.db,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, c TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        );
        sql(&edge.db, T_EDGE);
        let mut ddl = first.db.changes_since(Lsn(0)).unwrap().ddl;
        assert_eq!(ddl.len(), 2, "CREATE (id, a, b) and its DROP COLUMN b");
        ddl.extend(second.db.changes_since(Lsn(0)).unwrap().ddl);
        assert_eq!(ddl.len(), 3, "and the second author's CREATE (id, a, c)");
        let changes = ChangeSet {
            ddl,
            ..Default::default()
        };
        let certificate = edge
            .db
            .received_creates_reaching_held_shape(&changes)
            .unwrap();
        assert!(
            certificate.covers(0, &changes.ddl[0]),
            "the first CREATE's own history reaches the held (id, a)"
        );
        assert!(
            !certificate.covers(2, &changes.ddl[2]),
            "the later CREATE's own history ends at (id, a, c)"
        );
        let reader = edge.db.declared_reader_memory().unwrap();
        assert!(
            edge.db
                .arriving_statement_refusal(&changes.ddl[2], false, true, reader.clone())
                .is_err(),
            "an authenticated later CREATE reaching another shape is refused"
        );
        assert!(
            edge.db
                .arriving_statement_refusal(&changes.ddl[0], true, true, reader)
                .unwrap()
                .is_none(),
            "the certified CREATE needs no shape judgement"
        );
    }

    /// `relay` forwards `relayed` after its own served log. Each author block
    /// (who wrote the statement, the author's LSN, ordinal, generation, unit
    /// length and signature) travels unchanged, as a forwarding peer carries
    /// it; only the forwarding sender's own positions follow its log.
    fn relayed_after(
        mut wire: crate::protocol::WireChangeSet,
        mut relayed: crate::protocol::WireChangeSet,
    ) -> crate::protocol::WireChangeSet {
        assert!(
            relayed.rows.is_empty()
                && relayed.edges.is_empty()
                && relayed.vectors.is_empty()
                && relayed.purges.is_empty()
                && relayed.manifests.is_empty(),
            "the relayed author unit is schema only"
        );
        assert_eq!(relayed.ddl.len(), relayed.ddl_provenance.len());
        let base = wire.ddl_lsn.iter().map(|lsn| lsn.0).max().unwrap_or(0);
        for ((lsn, provenance), ddl) in relayed
            .ddl_lsn
            .iter_mut()
            .zip(relayed.ddl_provenance.iter_mut())
            .zip(relayed.ddl.iter())
        {
            *lsn = Lsn(base + lsn.0);
            provenance.source_ddl_lsn = *lsn;
            // The sender's own record digest binds its position, so the
            // forwarding sender emits it for its own LSN; the author's
            // signed digest inside the author block is untouched.
            provenance.digest = crate::protocol::canonical_ddl_provenance_digest(
                ddl,
                provenance.source_ddl_lsn,
                provenance.ordinal,
                provenance.table.as_deref(),
                provenance.table_generation,
            )
            .unwrap();
            assert!(
                provenance.author.is_some(),
                "the relayed statement keeps its author's signed block"
            );
        }
        wire.ddl.append(&mut relayed.ddl);
        wire.ddl_lsn.append(&mut relayed.ddl_lsn);
        wire.ddl_provenance.append(&mut relayed.ddl_provenance);
        wire
    }

    /// The edge's durable state a refused delivery must leave untouched:
    /// schema and DDL history, acceptance position, received arrivals,
    /// progress, every durable config value (receipts and outcomes
    /// included), and the table's shape and rows.
    fn conflict_image(
        edge: &JourneyPeer,
        relay: &JourneyPeer,
    ) -> (ReceiverImage, Vec<String>, Vec<Option<Value>>) {
        (
            receiver_image(edge, relay),
            table_columns(&edge.db, "t"),
            ids(&edge.db, "t"),
        )
    }

    /// The authenticated door, not a helper: one delivery carries two
    /// genuine authors' complete signed units -- the first author's CREATE
    /// (id, a, b) and DROP COLUMN b, whose own history reaches the edge's
    /// (id, a), then the second author's CREATE (id, a, c), whose own
    /// history ends at another shape. A certificate for the first CREATE
    /// must not exempt the second: the later CREATE is refused, and with it
    /// the whole delivery, at the pull door and the receipted push door,
    /// with schema, rows, history, acceptance, receipts and progress
    /// unchanged before and after a restart. The control delivery, spliced
    /// the same way from a second author whose history does reach (id, a),
    /// installs: the refusal is the CREATE judgement, not the splice.
    #[test]
    fn a_later_create_in_one_delivery_is_refused_whole_file() {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let first = peer(root.path(), "first-author", true);
        let second = peer(root.path(), "second-author", true);
        let agreeing = peer(root.path(), "agreeing-author", true);
        let edge = peer(root.path(), "edge", true);
        let control = peer(root.path(), "control-edge", true);
        sql(&first.db, T_HUB);
        sql(&first.db, "ALTER TABLE t DROP COLUMN b");
        const T_OTHER: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, c TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST";
        sql(&second.db, T_OTHER);
        sql(&agreeing.db, T_OTHER);
        sql(&agreeing.db, "ALTER TABLE t DROP COLUMN c");
        for receiver in [&edge, &control] {
            sql(&receiver.db, T_EDGE);
            sql(&receiver.db, "INSERT INTO t (id, a) VALUES (1, 'kept')");
        }

        let conflicting = relayed_after(served_by(&first, Lsn(0)), served_by(&second, Lsn(0)));
        assert_eq!(conflicting.ddl.len(), 3, "two complete authored units");
        // The edge's own sync incarnation (its database life, minted on its
        // first sync use, accepted or refused) exists before the image, so
        // the image compares only what a delivery could change.
        let own_life = edge.incarnation();
        let before = conflict_image(&edge, &first);
        let pulled = try_pulled_from(&edge, &first, conflicting.clone());
        let error = pulled.err().unwrap_or_else(|| {
            panic!("a later CREATE reaching another shape installed through the pull door")
        });
        assert!(
            error.contains("schema mismatch for table t"),
            "the pull refusal is the CREATE judgement, not a signature or splice check: {error}"
        );
        assert_eq!(
            conflict_image(&edge, &first),
            before,
            "a refused pull changes nothing"
        );
        let pushed = try_pushed_to(&edge, &first, conflicting);
        let error = pushed.err().unwrap_or_else(|| {
            panic!("a later CREATE reaching another shape installed through the push door")
        });
        assert!(
            error.contains("schema mismatch for table t"),
            "the push refusal is the CREATE judgement: {error}"
        );
        assert_eq!(
            conflict_image(&edge, &first),
            before,
            "a refused push changes nothing"
        );
        let edge = edge.reopen();
        assert_eq!(
            conflict_image(&edge, &first),
            before,
            "after a restart the refused delivery left nothing behind"
        );
        assert_eq!(edge.incarnation(), own_life, "the edge keeps its own life");

        let agreed = relayed_after(served_by(&first, Lsn(0)), served_by(&agreeing, Lsn(0)));
        assert_eq!(agreed.ddl.len(), 4);
        try_pulled_from(&control, &first, agreed).unwrap_or_else(|e| {
            panic!("the same splice of two histories reaching (id, a) installs: {e}")
        });
        let control = control.reopen();
        assert_eq!(
            table_columns(&control.db, "t"),
            vec!["id".to_string(), "a".to_string()]
        );
        assert_eq!(ids(&control.db, "t"), vec![Some(Value::Int64(1))]);
    }

    /// A durable store whose declaration history is `prefix` unrelated
    /// tables created and dropped, then `CREATE TABLE t` (its current
    /// generation), then `suffix` more. Built under ordinary budgets; the
    /// history is confirmed to hold every declaration. Returns the store and
    /// its own CREATE of `t`, which a peer restating it would send.
    fn restating_history(prefix: usize, suffix: usize) -> (Database, DdlChange) {
        let db = Database::open_memory();
        assert!(db.persistence.is_some(), "the durable path");
        let churn = |db: &Database, stem: &str, count: usize| {
            for i in 0..count {
                sql(
                    db,
                    &format!("CREATE TABLE {stem}{i} (id INTEGER PRIMARY KEY)"),
                );
                sql(db, &format!("DROP TABLE {stem}{i}"));
            }
        };
        churn(&db, "p", prefix);
        sql(&db, T_EDGE);
        churn(&db, "s", suffix);
        let ddl = db.changes_since(Lsn(0)).unwrap().ddl;
        assert_eq!(
            ddl.len(),
            2 * prefix + 1 + 2 * suffix,
            "the durable history holds every declaration"
        );
        let create = ddl
            .into_iter()
            .find(|ddl| matches!(ddl, DdlChange::CreateTable { name, .. } if name == "t"))
            .expect("the store's own CREATE of t");
        (db, create)
    }

    /// Judge `create` with the store's reader limited to `limit` for this
    /// one call: the answer (typed error kept whole), the reader parent's
    /// before/peak/after bytes, and how many declaration-history entries the
    /// call decoded.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn judge_restatement(
        db: &mut Database,
        create: &DdlChange,
        limit: u64,
    ) -> (
        std::result::Result<bool, String>,
        crate::vector_read_memory::ReaderMemoryHighWaterForTest,
        u64,
    ) {
        db.declared_read_memory =
            crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(limit)
                .expect("the reader root admits itself");
        let db = &*db;
        let ((answer, memory), work) = crate::writer_query_test_seams::observe(|| {
            db.with_reader_memory_high_water_for_test(|| {
                db.declared_reader_memory().and_then(|reader| {
                    db.received_create_restates_current_generation(create, reader)
                })
            })
        });
        (
            answer.map_err(|error| format!("{error:?}")),
            memory,
            work.history.ddl_occurrences,
        )
    }

    /// Judging a restated CREATE walks this store's own declaration history
    /// one charged entry at a time and stops at the current-generation
    /// CREATE. The same fixed reader limit restates it after
    /// 17 or 1025 earlier declarations, within a history-independent reader
    /// bound, releasing every transient credit; a roomy reader is the
    /// control. A match followed by a long suffix never visits the suffix.
    /// Calibrated on 0335486f under a roomy reader: the collected history
    /// peaked at 10176 bytes for 17 entries and 521676 for 1025; at this
    /// limit the long history was refused once the retained output filled
    /// the reader (the next cursor window found no room).
    #[test]
    fn restating_create_reader_memory_does_not_grow_with_history() {
        const LIMIT: u64 = 64 * 1024;
        const ROOMY: u64 = 1 << 30;
        const BOUND: u64 = 16 * 1024;
        // Declared before the green run: the 17- and 1025-entry walks may
        // differ by at most one cursor window (576 or 736 bytes observed)
        // plus the longer table names.
        const FLAT_TOLERANCE: u64 = 1024;
        let mut peaks = Vec::new();
        for prefix in [8, 512] {
            let (mut db, create) = restating_history(prefix, 0);
            let (roomy, ..) = judge_restatement(&mut db, &create, ROOMY);
            assert_eq!(
                roomy,
                Ok(true),
                "control: a roomy reader restates (prefix {prefix})"
            );
            let (answer, memory, visited) = judge_restatement(&mut db, &create, LIMIT);
            eprintln!(
                "RECEIVED_SCHEMA_HISTORY_WALK prefix={prefix} entries={} answer={answer:?} memory={memory:?} visited={visited}",
                2 * prefix + 1
            );
            assert_eq!(
                answer,
                Ok(true),
                "prefix {prefix}: restated within the fixed {LIMIT}-byte reader limit ({memory:?})"
            );
            assert!(
                memory.peak - memory.before <= BOUND,
                "prefix {prefix}: reader peak within the history-independent bound ({memory:?})"
            );
            assert_eq!(
                memory.after, memory.before,
                "prefix {prefix}: transient credits released"
            );
            peaks.push(memory.peak - memory.before);
        }
        assert!(
            peaks[0].abs_diff(peaks[1]) <= FLAT_TOLERANCE,
            "the reader peak is flat from 17 to 1025 entries within {FLAT_TOLERANCE} bytes: {peaks:?}"
        );
        let (mut db, create) = restating_history(4, 512);
        let (answer, memory, visited) = judge_restatement(&mut db, &create, LIMIT);
        eprintln!(
            "RECEIVED_SCHEMA_HISTORY_WALK early-stop prefix=4 suffix=512 entries=1033 answer={answer:?} memory={memory:?} visited={visited}"
        );
        assert_eq!(answer, Ok(true), "the early match restates");
        assert!(
            visited < 1033,
            "the walk stops at the match; the 1024-entry suffix is not visited ({visited} decoded)"
        );
        assert!(
            visited <= 2 * 4 + 2,
            "only the 8-entry prefix and the match are decoded ({visited} decoded)"
        );
    }

    both!(
        peer_history_reaching_live_shape_memory,
        peer_history_reaching_live_shape_file,
        peer_history_reaching_live_shape
    );
    both!(
        peer_vector_history_reaching_live_policy_memory,
        peer_vector_history_reaching_live_policy_file,
        peer_vector_history_reaching_live_policy
    );
    both!(
        peer_history_reaching_another_shape_memory,
        peer_history_reaching_another_shape_file,
        peer_history_reaching_another_shape
    );
    both!(
        older_incarnation_create_is_not_restated_memory,
        older_incarnation_create_is_not_restated_file,
        older_incarnation_create_is_not_restated
    );
    both!(
        same_alter_history_equal_revision_memory,
        same_alter_history_equal_revision_file,
        same_alter_history_equal_revision
    );

    macro_rules! retire {
        ($name:ident, $file:expr, $mutation:expr, $ddl:expr) => {
            #[test]
            fn $name() {
                existing_then_retire($file, $mutation, $ddl);
            }
        };
    }
    retire!(
        argon_update_drop_memory,
        false,
        "UPDATE links SET target_id = '00000000-0000-0000-0000-000000000004' WHERE id = 1",
        "DROP TABLE links"
    );
    retire!(
        argon_update_drop_file,
        true,
        "UPDATE links SET target_id = '00000000-0000-0000-0000-000000000004' WHERE id = 1",
        "DROP TABLE links"
    );
    retire!(
        argon_delete_drop_memory,
        false,
        "DELETE FROM links WHERE id = 1",
        "DROP TABLE links"
    );
    retire!(
        argon_delete_drop_file,
        true,
        "DELETE FROM links WHERE id = 1",
        "DROP TABLE links"
    );
    retire!(
        argon_update_shape_loss_memory,
        false,
        "UPDATE links SET target_id = '00000000-0000-0000-0000-000000000004' WHERE id = 1",
        "ALTER TABLE links DROP COLUMN target_id"
    );
    retire!(
        argon_update_shape_loss_file,
        true,
        "UPDATE links SET target_id = '00000000-0000-0000-0000-000000000004' WHERE id = 1",
        "ALTER TABLE links DROP COLUMN target_id"
    );
    retire!(
        argon_delete_shape_loss_memory,
        false,
        "DELETE FROM links WHERE id = 1",
        "ALTER TABLE links DROP COLUMN target_id"
    );
    retire!(
        argon_delete_shape_loss_file,
        true,
        "DELETE FROM links WHERE id = 1",
        "ALTER TABLE links DROP COLUMN target_id"
    );
    retire!(
        argon_update_rename_memory,
        false,
        "UPDATE links SET target_id = '00000000-0000-0000-0000-000000000004' WHERE id = 1",
        "ALTER TABLE links RENAME COLUMN target_id TO former_target"
    );
    retire!(
        argon_update_rename_file,
        true,
        "UPDATE links SET target_id = '00000000-0000-0000-0000-000000000004' WHERE id = 1",
        "ALTER TABLE links RENAME COLUMN target_id TO former_target"
    );
    retire!(
        argon_note_update_then_shape_loss_memory,
        false,
        "UPDATE links SET note = 'changed' WHERE id = 1",
        "ALTER TABLE links DROP COLUMN target_id"
    );
    #[test]
    fn repeated_received_prepare_drop_returns_identical_held_total() {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let author = peer(root.path(), "cycle-author", false);
        sql(
            &author.db,
            "CREATE TABLE cycle_table (id INTEGER PRIMARY KEY)",
        );
        let wire = served_by(&author, Lsn(0));
        let context = crate::protocol::received_ddl_context(
            &wire,
            &TenantId::from(JOURNEY_TENANT),
            &author.node_id(),
            author.incarnation(),
        )
        .unwrap()
        .unwrap();
        let changes = ChangeSet::try_from(wire).unwrap();
        // Public opener installs the original engine-owned budget registrations.
        // The private prepare door is required here because this guard's subject
        // is dropping an unpublished stage, not committing a received migration.
        let db = Database::open_with_options(":memory:", Default::default()).unwrap();
        let owner = db.accountant.clone();
        let baseline = owner.usage().used;
        let mut after = Vec::new();
        for cycle in 0..8 {
            {
                let memory = db.declared_reader_memory().unwrap();
                let mut copy = crate::vector_read_memory::admission(memory.clone());
                let stage = db
                    .prepare_received_schema_stage(
                        Lsn(9),
                        &changes,
                        &context,
                        &[],
                        &mut copy,
                        memory,
                    )
                    .unwrap();
                assert!(
                    owner.usage().used > baseline,
                    "real stage retains admitted memory"
                );
                drop(stage);
            }
            after.push(owner.usage().used);
            assert_eq!(
                after[cycle], baseline,
                "prepare/drop cycle {cycle} must return the identical held total; samples={after:?}"
            );
            assert_eq!(db.current_lsn(), Lsn(0));
            assert!(db.table_meta("cycle_table").is_none());
        }
    }

    #[test]
    fn positions_failed_durable_commit_publishes_no_interior_file() {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let (author, hub) = offline(root.path(), true);
        let before = hub.db.current_lsn();
        hub.db.fail_next_received_schema_durable_commit_for_test();
        assert!(
            try_pushed_to_with_atomicity(&hub, &author, served_by(&author, Lsn(0)), true).is_err()
        );
        let check = |db: &Database| {
            assert_eq!(db.current_lsn(), before);
            assert!(db.table_meta("notes").is_none());
            let changes = db.changes_since(before).unwrap();
            assert!(changes.ddl.is_empty() && changes.rows.is_empty());
        };
        check(&hub.db);
        let hub = hub.reopen();
        check(&hub.db);
        let accepted = received(&hub, &author, Lsn(0));
        assert_eq!(accepted.result.new_lsn, hub.db.current_lsn());
        assert_two_rows(&hub.db);
    }

    fn standalone_control(file: bool) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let author = peer(root.path(), "author", file);
        let hub = peer(root.path(), "hub", file);
        standalone(&author.db);
        sql(&author.db, EDGE_DDL);
        row(&author.db, 1, 3);
        received(&hub, &author, Lsn(0));
        assert_standalone(&hub.db);
        assert!(edge(&hub.db, 3).is_some());
        if file {
            let hub = hub.reopen();
            assert_standalone(&hub.db);
            assert!(edge(&hub.db, 3).is_some());
        }
    }
    both!(
        argon_standalone_control_memory,
        argon_standalone_control_file,
        standalone_control
    );
    #[test]
    fn refused_history_payload_releases_new_empty_backing_every_cycle() {
        let durable = Database::open_with_options(":memory:", Default::default()).unwrap();
        let db = durable.detached_local_schema_working_database().unwrap();
        drop(
            db.prepare_memory_schema_history(Lsn(9), 0, &[], &[])
                .unwrap(),
        );
        let owner = durable.accountant.clone();
        let before = owner.usage().used;
        // Permit exactly the first history-log slot, with its actual layout.
        // The subsequent payload/scratch admission must fail after this backing
        // has physically been allocated. No pre-existing ceiling is changed.
        let slot =
            std::mem::size_of::<(Lsn, DdlChange)>() + crate::sync_types::held::ALLOCATION_BYTES;
        let change = DdlChange::DropTable {
            name: "wide_history_name".repeat(512),
        };
        for cycle in 0..4 {
            owner.set_budget(Some(before + slot)).unwrap();
            let error = match db.prepare_memory_schema_history(
                Lsn(9),
                0,
                std::slice::from_ref(&change),
                &[],
            ) {
                Ok(_) => panic!("payload admission must refuse after capacity succeeds"),
                Err(error) => error,
            };
            owner.set_budget(None).unwrap();
            assert!(
                matches!(error, Error::MemoryBudgetExceeded { operation, .. } if operation == "history_occurrences"),
                "the refusal must come after successful history capacity admission"
            );
            assert_eq!(
                owner.usage().used,
                before,
                "failed payload preparation cycle {cycle} releases the newly allocated empty backing"
            );
            assert!(db.changes_since(Lsn(0)).unwrap().ddl.is_empty());
        }
        drop(db);
        // The outer durable path must likewise release every refused transaction.
        let wide_name = "refused_history_name".repeat(512);
        let refused_sql = format!("CREATE TABLE {wide_name} (id INTEGER PRIMARY KEY)");
        let durable_before = owner.usage().used;
        for cycle in 0..4 {
            sql(&durable, "BEGIN");
            owner.set_budget(Some(owner.usage().used)).unwrap();
            let staged = owner.usage().used;
            let error = durable
                .execute(&refused_sql, &HashMap::new())
                .expect_err("oversized durable DDL must refuse");
            assert!(
                matches!(
                    error,
                    Error::MemoryBudgetExceeded { .. }
                        | Error::NativeEngineOther(
                            contextdb_core::read_memory::NativeRefusal::MemoryBudgetExceeded { .. }
                        )
                ),
                "typed durable refusal in cycle {cycle}: {error}"
            );
            drop(error);
            assert_eq!(
                owner.usage().used,
                staged,
                "refused durable statement releases its own charges in cycle {cycle}"
            );
            owner.set_budget(None).unwrap();
            sql(&durable, "ROLLBACK");
            assert_eq!(
                owner.usage().used,
                durable_before,
                "durable rollback releases empty stage backing in cycle {cycle}"
            );
            assert!(durable.changes_since(Lsn(0)).unwrap().ddl.is_empty());
        }
    }

    #[test]
    fn refused_history_payload_keeps_populated_history_and_other_claim_funded() {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let durable = Database::open_with_options(":memory:", Default::default()).unwrap();
        let db = durable.detached_local_schema_working_database().unwrap();
        sql(
            &db,
            "CREATE TABLE retained_history (id INTEGER PRIMARY KEY)",
        );
        let committed = db.changes_since(Lsn(0)).unwrap().ddl;
        let owner = durable.accountant.clone();
        let small = DdlChange::DropTable {
            name: "pending_history".into(),
        };
        let pending = db
            .prepare_memory_schema_history(db.current_lsn(), 0, std::slice::from_ref(&small), &[])
            .unwrap();
        // A second real preparation grows room for a third occurrence. Drop it
        // while the first claim and committed history still own their capacity.
        let spare = db
            .prepare_memory_schema_history(db.current_lsn(), 0, std::slice::from_ref(&small), &[])
            .unwrap();
        drop(spare);
        let before = owner.usage().used;
        let large = DdlChange::DropTable {
            name: "refused_history_name".repeat(512),
        };
        owner.set_budget(Some(before)).unwrap();
        let error = match db.prepare_memory_schema_history(
            db.current_lsn(),
            0,
            std::slice::from_ref(&large),
            &[],
        ) {
            Ok(_) => panic!("the next payload must refuse"),
            Err(error) => error,
        };
        owner.set_budget(None).unwrap();
        assert!(
            matches!(error, Error::MemoryBudgetExceeded { operation, .. } if operation == "history_occurrences")
        );
        assert_eq!(
            owner.usage().used,
            before,
            "refusal cannot refund still-owned history or another pending claim"
        );
        assert_eq!(db.changes_since(Lsn(0)).unwrap().ddl, committed);
        // Publishing the surviving ticket must still have its promised physical
        // slot and original credit. No allocation/admission is permitted here.
        db.publish_memory_schema_history(pending);
        let history = db.changes_since(Lsn(0)).unwrap().ddl;
        assert_eq!(&history[..committed.len()], committed.as_slice());
        assert_eq!(history.last(), Some(&small));
        sql(&db, "INSERT INTO retained_history VALUES (1)");
        assert_eq!(
            db.execute("SELECT id FROM retained_history", &HashMap::new())
                .unwrap()
                .rows(),
            &[vec![Value::Int64(1)]]
        );
        drop(db);
        sql(
            &durable,
            "CREATE TABLE retained_history (id INTEGER PRIMARY KEY)",
        );
        sql(&durable, "INSERT INTO retained_history VALUES (1)");
        let committed = durable.changes_since(Lsn(0)).unwrap().ddl;
        sql(&durable, "BEGIN");
        sql(
            &durable,
            "CREATE TABLE pending_history (id INTEGER PRIMARY KEY)",
        );
        owner.set_budget(Some(owner.usage().used)).unwrap();
        let staged = owner.usage().used;
        let wide_name = "refused_history_name".repeat(512);
        let refused_sql = format!("CREATE TABLE {wide_name} (id INTEGER PRIMARY KEY)");
        let error = durable
            .execute(&refused_sql, &HashMap::new())
            .expect_err("the later durable DDL payload must refuse");
        assert!(
            matches!(
                error,
                Error::MemoryBudgetExceeded { .. }
                    | Error::NativeEngineOther(
                        contextdb_core::read_memory::NativeRefusal::MemoryBudgetExceeded { .. }
                    )
            ),
            "typed durable refusal: {error}"
        );
        drop(error);
        assert_eq!(
            owner.usage().used,
            staged,
            "refusal cannot refund the earlier durable transaction claim"
        );
        owner.set_budget(None).unwrap();
        assert_eq!(durable.changes_since(Lsn(0)).unwrap().ddl, committed);
        sql(&durable, "COMMIT");
        let history = durable.changes_since(Lsn(0)).unwrap().ddl;
        assert_eq!(history.len(), committed.len() + 1);
        assert_eq!(&history[..committed.len()], committed.as_slice());
        assert!(
            matches!(history.last(), Some(DdlChange::CreateTable { name, .. }) if name == "pending_history")
        );
        assert_eq!(
            durable
                .execute("SELECT id FROM retained_history", &HashMap::new())
                .unwrap()
                .rows(),
            &[vec![Value::Int64(1)]]
        );
    }
}
