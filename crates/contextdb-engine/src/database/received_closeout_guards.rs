// Independent closeout qualification. Only the existing real-peer fixture is
// shared: every migration below is authored by SQL and delivered through the
// public authenticated sync door. No received context or signature is fabricated.
mod received_closeout_guards {
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
        sql(db, &format!("INSERT INTO links VALUES ({id}, '00000000-0000-0000-0000-000000000001', '{}', 'link', 'row')", uuid::Uuid::from_u128(target)));
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
        assert!(db
            .insert_edge(
                tx,
                uuid::Uuid::from_u128(1),
                uuid::Uuid::from_u128(2),
                "link".into(),
                HashMap::from([("independent".into(), Value::Text("keep me".into()))])
            )
            .unwrap());
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
        sql(&author.db, "CREATE TABLE other_links (id INTEGER PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)");
        sql(&author.db, "INSERT INTO other_links VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000005', 'link')");
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

    fn incomplete_restatement(file: bool) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let hub = peer(root.path(), "hub", file);
        let edge = peer(root.path(), "edge", file);
        for db in [&hub.db, &edge.db] {
            sql(db, "CREATE TABLE bootstrap (id INTEGER PRIMARY KEY)");
        }
        sql(&edge.db, "ALTER TABLE bootstrap ADD COLUMN extra TEXT");
        let before = edge.db.current_lsn();
        let before_history = edge.db.changes_since(Lsn(0)).unwrap().ddl;
        let refused = try_pulled_from(&edge, &hub, served_by(&hub, Lsn(0)));
        assert!(
            refused.is_err(),
            "authenticated old CREATE-only cannot restate richer local final schema"
        );
        assert_eq!(edge.db.current_lsn(), before);
        assert_eq!(edge.db.changes_since(Lsn(0)).unwrap().ddl, before_history);
        sql(&edge.db, "INSERT INTO bootstrap VALUES (1, 'still here')");
        let check = |db: &Database| {
            assert_eq!(
                db.execute("SELECT extra FROM bootstrap", &HashMap::new())
                    .unwrap()
                    .rows()[0]
                    .first(),
                Some(&Value::Text("still here".into()))
            );
        };
        check(&edge.db);
        if file {
            let edge = edge.reopen();
            check(&edge.db);
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
            assert_eq!(after[cycle], baseline, "prepare/drop cycle {cycle} must return the identical held total; samples={after:?}");
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
        let db = Database::open_with_options(":memory:", Default::default()).unwrap();
        drop(
            db.prepare_memory_schema_history(Lsn(9), 0, &[], &[])
                .unwrap(),
        );
        let owner = db.accountant.clone();
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
            assert_eq!(owner.usage().used, before, "failed payload preparation cycle {cycle} releases the newly allocated empty backing");
            assert!(db.changes_since(Lsn(0)).unwrap().ddl.is_empty());
        }
    }

    #[test]
    fn refused_history_payload_keeps_populated_history_and_other_claim_funded() {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let db = Database::open_with_options(":memory:", Default::default()).unwrap();
        sql(
            &db,
            "CREATE TABLE retained_history (id INTEGER PRIMARY KEY)",
        );
        let committed = db.changes_since(Lsn(0)).unwrap().ddl;
        let owner = db.accountant.clone();
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
    }
}
