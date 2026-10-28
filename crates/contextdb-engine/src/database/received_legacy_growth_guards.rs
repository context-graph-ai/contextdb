// Independent successful-apply population bound on the public production host.
// No production selection or admission is replaced by this fixture.
#[cfg(feature = "test-seams")]
mod received_legacy_growth_guards {
    use super::*;
    use crate::persistence::content_identity::proof_allocation_oracle_tests as physical;
    use serial_test::serial;

    const WRITER: usize = 4 * 1024 * 1024;
    const READER: usize = WRITER;
    const DIM: usize = 1027;
    const WIDTH: usize = DIM * std::mem::size_of::<f32>();
    // Each retained population separately exceeds both execution owners combined.
    const N: usize = (WRITER + READER) / WIDTH + 1;
    const SEED: usize = 512 * 1024 * 1024;
    const TABLE: &str = "retained";
    const KIND: &str = "selected_growth";

    fn options(limit: usize) -> crate::read_session::DatabaseOpenOptions {
        let mut options = crate::read_session::DatabaseOpenOptions::default();
        options.memory_limit = Some(limit);
        options.owner_reads.enabled = false;
        options.owner_reads.limits.limits.memory = limit as u64;
        options.maintenance_policy = MaintenancePolicy::CallerDriven;
        options
    }
    fn open(path: &std::path::Path, limit: usize) -> Database {
        let db = Database::open_with_options(path, options(limit)).unwrap();
        assert!(
            matches!(&db.tx_mgr, DatabaseTransactionHost::Legacy(_)),
            "only the actual production host qualifies this guard"
        );
        db
    }
    fn sql(db: &Database, statement: &str) {
        db.execute(statement, &HashMap::new()).unwrap();
    }
    fn ddl() -> String {
        format!(
            "CREATE TABLE retained (id INTEGER PRIMARY KEY, body TEXT, embedding VECTOR({DIM}) SEARCH_MODE EXACT) HISTORY ALL"
        )
    }
    fn row(i: usize, body: char) -> HashMap<String, Value> {
        HashMap::from([
            ("id".into(), Value::Int64(i as i64)),
            ("body".into(), Value::Text(body.to_string().repeat(WIDTH))),
            ("embedding".into(), Value::Vector(vec![(i + 1) as f32; DIM])),
        ])
    }
    fn source(i: usize) -> uuid::Uuid {
        uuid::Uuid::from_u128(0x100000 + i as u128)
    }
    fn target(i: usize) -> uuid::Uuid {
        uuid::Uuid::from_u128(0x200000 + i as u128)
    }
    fn properties(i: usize) -> HashMap<String, Value> {
        HashMap::from([
            ("ordinal".into(), Value::Int64(i as i64)),
            ("body".into(), Value::Text("g".repeat(WIDTH))),
        ])
    }
    fn reopen(peer: JourneyPeer, limit: usize) -> JourneyPeer {
        peer.db.close().unwrap();
        drop(peer.db);
        JourneyPeer {
            db: open(&peer.path, limit),
            path: peer.path,
            identity: peer.identity,
        }
    }
    fn seeded(root: &std::path::Path, count: usize) -> (JourneyPeer, JourneyPeer, Lsn, Lsn) {
        let author = JourneyPeer::open(root, "author");
        author
            .db
            .set_maintenance_policy(MaintenancePolicy::CallerDriven);
        sql(&author.db, &ddl());
        let path = root.join("receiver.db");
        let receiver = JourneyPeer {
            db: open(&path, SEED),
            path,
            identity: Arc::new(
                crate::identity::FabricIdentity::load_or_generate(&root.join("receiver.key"))
                    .unwrap(),
            ),
        };
        try_pushed_to_with_atomicity(&receiver, &author, served_by(&author, Lsn(0)), true).unwrap();
        // Seed graph through the persistence bulk writer outside the measured
        // migration. Entries use a committed seed transaction and LSN.
        let mut seed_tx = None;
        for first in (0..count).step_by(N) {
            let tx = receiver.db.begin().unwrap();
            for i in first..(first + N).min(count) {
                receiver.db.insert_row(tx, TABLE, row(i, 'a')).unwrap();
            }
            receiver.db.commit(tx).unwrap();
            seed_tx = Some(tx);
        }
        let retained_at = receiver.db.current_lsn();
        for first in (0..count).step_by(256) {
            let edges: Vec<_> = (first..(first + 256).min(count))
                .map(|i| contextdb_core::types::AdjEntry {
                    source: source(i),
                    target: target(i),
                    edge_type: KIND.into(),
                    properties: properties(i),
                    created_tx: seed_tx.unwrap(),
                    deleted_tx: None,
                    lsn: retained_at,
                })
                .collect();
            receiver.db.persistence.as_ref().unwrap()
                .append_graph_edges_batch(&edges).unwrap();
        }
        let receiver = reopen(receiver, SEED);
        for first in (0..count).step_by(N) {
            let tx = receiver.db.begin().unwrap();
            for i in first..(first + N).min(count) {
                receiver
                    .db
                    .execute_in_tx(
                        tx,
                        "UPDATE retained SET body = $body WHERE id = $id",
                        &HashMap::from([
                            ("id".into(), Value::Int64(i as i64)),
                            ("body".into(), Value::Text("b".repeat(WIDTH))),
                        ]),
                    )
                    .unwrap();
            }
            receiver.db.commit(tx).unwrap();
        }
        let before = author.db.current_lsn();
        sql(&author.db, "ALTER TABLE retained ADD COLUMN extra TEXT");
        let tx = author.db.begin().unwrap();
        author.db.insert_row(tx, TABLE, row(count, 'c')).unwrap();
        author
            .db
            .insert_edge(
                tx,
                source(count),
                target(count),
                KIND.into(),
                properties(count),
            )
            .unwrap();
        author.db.commit(tx).unwrap();
        let receiver = reopen(receiver, WRITER);
        (author, receiver, before, retained_at)
    }
    fn exact(db: &Database, count: usize, installed: bool, retained_at: Lsn) {
        assert!(matches!(&db.tx_mgr, DatabaseTransactionHost::Legacy(_)));
        assert_eq!(
            db.table_meta(TABLE)
                .unwrap()
                .columns
                .iter()
                .any(|c| c.name == "extra"),
            installed
        );
        let total = count + usize::from(installed);
        for i in 0..total {
            let found = db
                .point_lookup(TABLE, "id", &Value::Int64(i as i64), db.snapshot())
                .unwrap()
                .unwrap();
            assert_eq!(
                found.values.get("body"),
                Some(&Value::Text(
                    if i == count { "c" } else { "b" }.repeat(WIDTH)
                ))
            );
            assert_eq!(
                db.live_vector_entry(found.row_id, db.snapshot())
                    .unwrap()
                    .unwrap()
                    .vector,
                vec![(i + 1) as f32; DIM]
            );
            if i < count {
                let old = db
                    .point_lookup(
                        TABLE,
                        "id",
                        &Value::Int64(i as i64),
                        db.snapshot_at(retained_at),
                    )
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    old.values.get("body"),
                    Some(&Value::Text("a".repeat(WIDTH))),
                    "retained authored version survives migration and reopen"
                );
            }
            assert_eq!(db.edge_count(source(i), KIND, db.snapshot()).unwrap(), 1);
            assert_eq!(
                db.get_edge_properties(source(i), target(i), KIND, db.snapshot())
                    .unwrap(),
                Some(properties(i))
            );
        }
        assert!(
            db.point_lookup(TABLE, "id", &Value::Int64(total as i64), db.snapshot())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            db.edge_count(source(total), KIND, db.snapshot()).unwrap(),
            0
        );
    }
    // Freeze the actual authenticated sender before the physical window.
    // Applying this real wire unit touches only the receiver, so no unrelated
    // sender cache allocation can masquerade as a leaked receiver page.
    struct Delivery {
        wire: crate::protocol::WireChangeSet,
        node: String,
        incarnation: Incarnation,
    }
    impl Delivery {
        fn authored_by(author: &JourneyPeer, since: Lsn) -> Self {
            Self {
                wire: served_by(author, since),
                node: author.node_id(),
                incarnation: author.incarnation(),
            }
        }
        fn apply(&self, receiver: &JourneyPeer) -> Result<ApplyResult> {
            let tenant = TenantId::from(JOURNEY_TENANT);
            let request = crate::protocol::PushRequest {
                changeset: self.wire.clone(),
                incarnation: self.incarnation,
            };
            let outcome_key = Database::sync_push_outcome_key(&tenant, &self.node, &request, true)?;
            let received = crate::protocol::received_ddl_context(
                &request.changeset,
                &tenant,
                &self.node,
                self.incarnation,
            )
            .map_err(crate::protocol::received_ddl_error)?
            .expect("this actual signed unit includes its ALTER");
            let arrivals = crate::protocol::wire_row_arrivals(&request.changeset);
            let lineages = crate::protocol::wire_row_lineages(&request.changeset);
            let changes = ChangeSet::try_from(request.changeset)
                .map_err(crate::protocol::received_ddl_error)?;
            let memory = receiver.db.declared_reader_memory()?;
            receiver
                .db
                .validate_incoming_push_lineages_with_received_ddl(
                    &tenant,
                    &changes,
                    &lineages,
                    &self.node,
                    self.incarnation,
                    &received,
                    memory.clone(),
                )?;
            let changes = receiver
                .db
                .reject_accepted_lineage_replays_with_received_ddl(
                    &tenant,
                    changes,
                    &lineages,
                    &received,
                    memory.clone(),
                )?;
            let receipt = SyncApplyReceipt {
                tenant_id: tenant,
                node_id: self.node.clone(),
                incarnation: self.incarnation,
                source_lsn: changes.max_lsn().expect("source frontier"),
                dependency_complete: true,
                hub_contact_ms: None,
            };
            let mut copy_owner = contextdb_engine::vector_read_memory::admission(memory.clone());
            receiver
                .db
                .apply_authenticated_received_changes_with_outcome(
                    changes,
                    &arrivals,
                    SyncAdoption::Continuing,
                    receipt,
                    Some(&receiver.node_id()),
                    &lineages,
                    Some(&received),
                    Some(&outcome_key),
                    &mut copy_owner,
                    memory,
                )
        }
    }
    fn growth(scale: usize) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let count = if scale == 0 { 1 } else { N * scale };
        if scale > 0 {
            assert!(count * WIDTH > WRITER + READER);
        }
        let (author, receiver, before, retained_at) = seeded(root.path(), count);
        let delivery = Delivery::authored_by(&author, before);
        assert_eq!(
            (
                delivery.wire.ddl.len(),
                delivery.wire.rows.len(),
                delivery.wire.edges.len()
            ),
            (1, 1, 1)
        );
        // Prepay observer-global Vec capacity outside the product window.
        receiver.db.with_reader_memory_high_water_for_test(|| ());
        receiver.db.close().unwrap();
        drop(receiver.db);
        let path = receiver.path;
        let identity = receiver.identity;
        // The allocator is independent of admission and prices actual Layouts.
        // Oracle-private headers are reported raw but excluded from the product bound.
        let (
            ((applied, writer_peak, reader_peak), pages),
            raw_peak,
            raw_live,
            payload_peak,
            payload_live,
        ) = physical::measured_payload(|| {
            redb::observe_page_lifetimes_for_test(|| {
                let receiver = JourneyPeer {
                    db: open(&path, WRITER),
                    path: path.clone(),
                    identity: identity.clone(),
                };
                receiver.db.accountant.reset_high_water_for_test();
                let (outcome, reader_peak) = receiver
                    .db
                    .with_reader_memory_high_water_for_test(|| delivery.apply(&receiver));
                let writer_peak = receiver.db.write_memory_high_water_for_test();
                let applied = outcome.is_ok();
                if let Err(error) = &outcome {
                    eprintln!("LEGACY_GROWTH_REFUSAL scale={scale} count={count} {error:?}");
                }
                drop(outcome);
                receiver.db.close().unwrap();
                drop(receiver);
                (applied, writer_peak, reader_peak)
            })
        });
        eprintln!(
            "LEGACY_GROWTH scale={scale} rows={count} owner_limit={WRITER} reader_limit={READER} writer_peak={writer_peak} reader={reader_peak:?} raw_peak={raw_peak} raw_live={raw_live} payload_peak={payload_peak} payload_live={payload_live} pages={pages:?}"
        );
        assert!(
            applied,
            "successful bounded staging is required, not a safely refused full image"
        );
        assert!(writer_peak <= WRITER);
        assert!(reader_peak.peak <= READER as u64);
        assert!(
            payload_peak <= WRITER + READER,
            "all live operation allocations must fit their two declared funders"
        );
        assert_eq!(
            raw_live, 0,
            "all measured physical backing dies after cold-open/apply/close"
        );
        assert_eq!(payload_live, 0);
        assert!(
            pages.allocated_pages > 0,
            "the journey performs real physical page work"
        );
        assert_eq!(
            pages.freed_pages, pages.allocated_pages,
            "every created page physically frees"
        );
        assert_eq!(pages.freed_bytes, pages.allocated_bytes);
        assert_eq!((pages.current_pages, pages.current_bytes), (0, 0));
        assert!(
            pages.peak_bytes <= (WRITER + READER) as i128,
            "page backing is a subset of the same two finite physical funders"
        );
        if scale > 0 {
            assert!(
                pages.allocated_bytes > WRITER + READER,
                "cumulative physical page work exceeds live memory: pages must recycle within the journey"
            );
        }
        let path = root.path().join("receiver.db");
        let identity = Arc::new(
            crate::identity::FabricIdentity::load_or_generate(&root.path().join("receiver.key"))
                .unwrap(),
        );
        let receiver = JourneyPeer {
            db: open(&path, WRITER),
            path,
            identity,
        };
        exact(&receiver.db, count, true, retained_at);
        let end = receiver.db.current_lsn();
        delivery.apply(&receiver).unwrap();
        assert_eq!(
            receiver.db.current_lsn(),
            end,
            "lost acknowledgement retry is effect-idempotent"
        );
        exact(&receiver.db, count, true, retained_at);
        receiver.db.close().unwrap();
        author.db.close().unwrap();
    }
    #[test]
    #[serial]
    fn production_legacy_small_population_is_a_fixed_budget_positive_control() {
        growth(0);
    }
    #[test]
    #[serial]
    fn production_legacy_selected_population_n_succeeds_with_fixed_memory() {
        growth(1);
    }
    #[test]
    #[serial]
    fn production_legacy_selected_population_2n_succeeds_with_fixed_memory() {
        growth(2);
    }
    #[test]
    #[serial]
    fn production_legacy_selected_population_4n_succeeds_with_fixed_memory() {
        growth(4);
    }
    include!("received_legacy_growth_lifecycle_guards.rs");
}
