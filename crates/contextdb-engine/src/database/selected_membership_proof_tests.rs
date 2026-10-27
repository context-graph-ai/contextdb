//! A selected membership keeps the one owning-row proof it already paid for.
use super::*;
use crate::persistence::vector_membership_source::{DurableMembershipSource, MembershipOwner};
use contextdb_vector::store::{DormantRawVectorLoader, RawVectorMembershipSource};

#[test]
#[serial_test::serial]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn selected_historical_membership_and_body_reconcile_the_owning_row_once() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_700_000_000_000);
    for rows in [32, 64] {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open_with_options(
            dir.path().join("selected-proof.redb"),
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        db.execute(
            "CREATE TABLE selected_proof (id INTEGER PRIMARY KEY, v VECTOR(4)) HISTORY ALL",
            &HashMap::new(),
        )
        .unwrap();
        for id in 0..rows {
            db.execute(
                "INSERT INTO selected_proof VALUES ($id,$v)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("v".into(), Value::Vector(vec![1.0, id as f32, 0.0, 0.0])),
                ]),
            )
            .unwrap();
        }
        db.execute(
            "CREATE TABLE other_proof (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .unwrap();
        let other_dir = tempfile::tempdir().unwrap();
        let other = Database::open_with_options(
            other_dir.path().join("other.redb"),
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        other
            .execute(
                "CREATE TABLE selected_proof (id INTEGER PRIMARY KEY, v VECTOR(4)) HISTORY ALL",
                &HashMap::new(),
            )
            .unwrap();
        let other_owner = MembershipOwner::Writable(other.persistence.as_ref().unwrap().clone());
        let other_reader_memory = crate::executor::bounded::with_native_read_memory(
            db.ordinary_read_child().unwrap(),
            |memory, _trace_refund| Ok(memory.clone()),
        )
        .unwrap();
        let snapshot = db.snapshot();
        // A real later publication makes visible_count use its historical
        // cursor, while preserving exactly one version for every vector row.
        db.execute(
            "CREATE TABLE after_snapshot_marker (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .unwrap();
        db.execute(
            "INSERT INTO after_snapshot_marker VALUES (1)",
            &HashMap::new(),
        )
        .unwrap();
        let child = db.ordinary_read_child().unwrap();
        crate::executor::bounded::with_native_read_memory(child, |memory, _trace_refund| {
            let image = crate::read_image_memory::ImageMemory::new_direct_reader(None, memory.clone())?;
            image.scope(|| {
            let _intent = db.vector_historical_read_intent(snapshot, memory.clone());
            let mut copies = crate::vector_read_memory::admission(memory.clone());
            let _scope = db.vector_read_scope(snapshot, memory.clone(), &mut copies).unwrap();
            let persistence = db.persistence.as_ref().unwrap().clone();
            let partitions = persistence.durable_membership_partitions(&mut copies).unwrap();
            assert_eq!(partitions.len(), 1);
            let (partition, summary) = partitions.into_iter().next().unwrap();
            let mut workspace = copies.scratch();
            let retained = copies.scratch();
            let source = DurableMembershipSource::from_owner_admitted(
                MembershipOwner::Writable(persistence.clone()), None, &partition, summary,
                db.accountant.clone(), &mut workspace, retained,
            ).unwrap();
            let count_work = crate::read_work_probe::ReadWorkObservationScope::enter(crate::read_work_probe::ReadWorkPhase::Query);
            let visible = source.visible_count(snapshot, &mut || true, &mut copies).unwrap();
            assert_eq!(visible, rows as usize, "the actual historical count consumes the declared population");
            let counted = count_work.counters();
            let counted_history = count_work.historical_counters();
            assert_eq!(counted_history.owning_row_controls_admitted, rows as u64,
                "each counted record still reconciles its original owning row");
            assert_eq!(counted.decoded_rows, 0,
                "a membership count authenticates body and schema without constructing owned row vectors: {counted:?}, {counted_history:?}");
            drop(count_work);
            // Discover the genuine physical row id through its existing
            // admitted cursor, outside the measured select/load segment.
            let mut first_window = copies.scratch();
            let mut first = source.open(&mut |request| match request {
                DormantVectorLoadRequest::Reserve(bytes) => { first_window.reserve(bytes).unwrap(); true }
                _ => true,
            }).unwrap();
            let identity = first.next_with_body_identity(&mut |request| match request {
                DormantVectorLoadRequest::Reserve(bytes) => { first_window.reserve(bytes).unwrap(); true }
                _ => true,
            }).unwrap().unwrap().0;
            drop(first);
            drop(first_window);
            let loader = DurableRawVectorPartitionLoader {
                source: DurableRawVectorSource::Writable(persistence), accountant: db.accountant.clone(),
                partition, dimension: 4, quantization: VectorQuantization::F32, metadata_copy_owner: None,
            };
            let observed = crate::read_work_probe::ReadWorkObservationScope::enter(crate::read_work_probe::ReadWorkPhase::Query);
            let selected = source.select_visible_by_row(identity.row_id, snapshot, &mut || true, &mut copies).unwrap().unwrap();
            let mut body_window = copies.scratch();
            let body_admissions = std::cell::Cell::new(0usize);
            let loaded = loader.load_selected_candidate(selected.as_ref(), true, &mut |request| match request {
                DormantVectorLoadRequest::Reserve(bytes) => { body_admissions.set(body_admissions.get() + 1); body_window.reserve(bytes).unwrap(); true }
                _ => true,
            }).unwrap();
            assert_eq!(selected.identity(), identity, "selected record preserves the genuine cursor identity; the actual loader checks its body");
            let work = observed.counters();
            let history = observed.historical_counters();
            assert_eq!(history.owning_row_controls_admitted, 1,
                "one consumed membership interval needs one successfully admitted owning-row control: {work:?}, {history:?}");
            assert_eq!(work.decoded_rows, 1, "selection/body reuse the same authenticated owning row");
            assert_eq!(body_admissions.get(), 1, "one actual selected-body reservation");
            assert_eq!(work.vector_body_accesses, 1);
            assert_eq!(work.decoded_vectors, 1);
            assert!(history.manifest_members_visited <= 2, "one version and at most one boundary witness: {history:?}");
            selected.as_any().downcast_ref::<crate::persistence::vector_membership_source::SelectedVectorMembership>()
                .expect("actual durable selection type")
                .assert_row_evidence_bindings_for_test(&other_owner, &other_reader_memory);
            drop(loaded);
            drop(body_window);
            drop(selected);
            Ok(())
            })
        }).unwrap();
        db.close().unwrap();
        other.close().unwrap();
    }
}

#[test]
#[serial_test::serial]
fn historical_receipt_preserves_old_and_current_indexed_answers_across_overwrite_and_delete() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_700_000_000_000);
    for rows in [16, 32] {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open_with_options(
            dir.path().join("receipt-answers.redb"),
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        let params = HashMap::new();
        db.execute("CREATE TABLE receipt_answers (id INTEGER PRIMARY KEY, v VECTOR(4) SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 4, EF_CONSTRUCTION = 16, EF_SEARCH = 16)) HISTORY ALL", &params).unwrap();
        for id in 0..rows {
            db.execute(
                "INSERT INTO receipt_answers VALUES ($id,$v)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("v".into(), Value::Vector(vec![1.0, id as f32, 0.0, 0.0])),
                ]),
            )
            .unwrap();
        }
        db.run_maintenance_cycle().unwrap();
        let old = db.snapshot();
        let pin = db.pin_snapshot(old).unwrap();
        db.execute(
            "UPDATE receipt_answers SET v = [0,0,1,0] WHERE id = 0",
            &params,
        )
        .unwrap();
        db.execute("DELETE FROM receipt_answers WHERE id = 1", &params)
            .unwrap();
        let sql = "SELECT id FROM receipt_answers ORDER BY v <=> $v USE VECTOR INDEXED LIMIT 1";
        let query = HashMap::from([("v".into(), Value::Vector(vec![1.0, 0.0, 0.0, 0.0]))]);
        let observed = crate::read_work_probe::ReadWorkObservationScope::enter(
            crate::read_work_probe::ReadWorkPhase::Query,
        );
        let old_answer = db.execute_at_snapshot(sql, &query, old).unwrap();
        assert_eq!(old_answer.rows(), vec![vec![Value::Int64(0)]]);
        assert_eq!(
            old_answer.trace().vector_search.as_ref().unwrap().route,
            Some(VectorSearchRoute::Indexed)
        );
        assert_eq!(
            old_answer
                .trace()
                .vector_search
                .as_ref()
                .unwrap()
                .route_evidence,
            VectorSearchRouteEvidence::Executed
        );
        let history = observed.historical_counters();
        assert!(
            history.owning_row_controls_admitted > 0,
            "the actual old query enters historical owning-row proof: {history:?}"
        );
        eprintln!(
            "historical_receipt_whole_query counters={:?} historical={history:?}",
            observed.counters()
        );
        if rows == 16 {
            // Exact independently retained parent RED work, not an amount
            // derived from this candidate's held bytes or current counters.
            assert_eq!(history.owning_row_controls_admitted, 60);
            assert_eq!(history.manifest_members_visited, 127);
        }
        // The N32 108/223 result is retained as an observation. It is not
        // promoted to an expected formula without the count/probe derivation.

        assert_eq!(
            history.owning_row_proof_setups, 1,
            "one actual captured root/table/reader reuses fixed verifier setup while every consumed row is still authenticated: {history:?}"
        );
        drop(old_answer);
        drop(observed);
        let current = db.execute(sql, &query).unwrap();
        assert_eq!(current.rows(), vec![vec![Value::Int64(2)]]);
        assert_eq!(
            current.trace().vector_search.as_ref().unwrap().route,
            Some(VectorSearchRoute::Indexed)
        );
        assert_eq!(
            current
                .trace()
                .vector_search
                .as_ref()
                .unwrap()
                .route_evidence,
            VectorSearchRouteEvidence::Executed
        );
        drop(current);
        let deleted_query = HashMap::from([("v".into(), Value::Vector(vec![1.0, 1.0, 0.0, 0.0]))]);
        assert_eq!(
            db.execute_at_snapshot(sql, &deleted_query, old)
                .unwrap()
                .rows(),
            vec![vec![Value::Int64(1)]]
        );
        drop(pin);
        db.close().unwrap();
    }
}

/// The real count cursor must reject malformed JSON even though its complete
/// borrowed structural parser accepts the UTF-8 bytes. The fixture rewrites the
/// real persisted body AND manifest; a digest mismatch cannot satisfy this.
#[test]
#[serial_test::serial]
fn historical_count_rejects_authenticated_malformed_json() {
    authenticated_json_count_fixture(true, false);
}
#[test]
#[serial_test::serial]
fn historical_count_valid_json_uses_existing_decoder() {
    authenticated_json_count_fixture(false, false);
}
#[test]
#[serial_test::serial]
fn count_header_producer_skips_future_authenticated_malformed_json() {
    authenticated_json_count_fixture(true, true);
}
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn authenticated_json_count_fixture(malformed: bool, future: bool) {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        dir.path().join("json-count.redb"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    db.execute(
        "CREATE TABLE count_json (id INTEGER PRIMARY KEY, doc JSON, v VECTOR(4)) HISTORY ALL",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO count_json VALUES (1,$doc,$v)",
        &HashMap::from([
            ("doc".into(), Value::Json(serde_json::json!({}))),
            ("v".into(), Value::Vector(vec![1.0, 0.0, 0.0, 0.0])),
        ]),
    )
    .unwrap();
    let snapshot = db.snapshot();
    if future {
        db.execute(
            "UPDATE count_json SET doc = $doc WHERE id = 1",
            &HashMap::from([("doc".into(), Value::Json(serde_json::json!({})))]),
        )
        .unwrap();
    }
    if malformed {
        db.persistence
            .as_ref()
            .unwrap()
            .malformed_count_json_for_test(db.accountant.clone(), "count_json", future)
            .unwrap();
    }
    db.execute(
        "CREATE TABLE count_json_marker (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("INSERT INTO count_json_marker VALUES (1)", &HashMap::new())
        .unwrap();
    crate::executor::bounded::with_native_read_memory(db.ordinary_read_child().unwrap(), |memory, _trace_refund| {
        let image = crate::read_image_memory::ImageMemory::new_direct_reader(None, memory.clone())?;
        image.scope(|| {
            let _intent = db.vector_historical_read_intent(snapshot, memory.clone());
            let mut copies = crate::vector_read_memory::admission(memory.clone());
            let _scope = db.vector_read_scope(snapshot, memory.clone(), &mut copies).unwrap();
            let persistence = db.persistence.as_ref().unwrap().clone();
            let mut partitions = persistence.durable_membership_partitions(&mut copies).unwrap();
            let (partition, summary) = partitions.pop().unwrap();
            let mut workspace = copies.scratch();
            let owner = MembershipOwner::Writable(persistence);
            let source = DurableMembershipSource::from_owner_admitted(
                owner.clone(), None, &partition, summary,
                db.accountant.clone(), &mut workspace, copies.scratch()).unwrap();
            let work = crate::read_work_probe::ReadWorkObservationScope::enter(crate::read_work_probe::ReadWorkPhase::Query);
            let result = if future {
                crate::persistence::vector_membership_source::count_header_before_future_version_for_test(&owner, "count_json", snapshot)
            } else {
                source.visible_count(snapshot, &mut || true, &mut copies)
            };
            if future {
                assert_eq!(result.unwrap(), 1, "the same count producer selects the older valid row and skips decoding its authenticated future version");
                assert_eq!(work.counters().decoded_rows, 1, "only the older valid JSON row requires full decoding");
            } else if malformed {
                assert!(matches!(result, Err(Error::StoreCorrupted { .. })),
                    "the actual historical count must preserve the original JSON decode rejection: {result:?}");
                assert_eq!(work.counters().decoded_rows, 1, "malformed JSON goes through the existing full decoder");
            } else {
                assert_eq!(result.unwrap(), 1);
                assert_eq!(work.counters().decoded_rows, 1, "valid JSON uses the same original full decoder");
            }
            Ok(())
        })
    }).unwrap();
    db.close().unwrap();
}
