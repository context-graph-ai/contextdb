use super::*;

fn selection_partition() -> VectorPartitionRef {
    VectorPartitionRef::new(
        VectorIndexRef::new("selection_items", "v"),
        VectorPartitionKey::unpartitioned(),
    )
}

fn canonical_retained_selection_digest(
    db: &Database,
    partition: &VectorPartitionRef,
    snapshot: SnapshotId,
) -> [u8; 32] {
    let vector_fixture_reader_3 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_3 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_3.clone());
    let vector_fixture_count_3 = db
        .vector_store
        .partition_generation_entry_counts(
            &partition.index,
            &partition.partition_key,
            snapshot,
            None,
            &mut vector_fixture_copy_3,
        )
        .expect("count the real selected generation")
        .0;
    let vector_fixture_workspace_3 = db
        .vector_store
        .reserve_workspace(
            vector_fixture_copy_3.accountant_for_test().clone(),
            db.vector_store
                .generation_copy_bytes_for_test(
                    &partition.index,
                    &partition.partition_key,
                    vector_fixture_count_3,
                )
                .expect("price the retained generation"),
            "fixture_generation_copy",
            "Use the exact selected fixture generation.",
        )
        .expect("admit the actual generation workspace");

    let entries = db
        .vector_store
        .entries_for_partition_at_snapshot(
            &partition.index,
            &partition.partition_key,
            snapshot,
            &vector_fixture_workspace_3,
        )
        .unwrap();
    let mut live = entries
        .iter()
        .filter(|entry| entry.deleted_tx.is_none())
        .collect::<Vec<_>>();
    live.sort_by_key(|entry| (entry.row_id, entry.created_tx, entry.lsn));
    let canonical = bincode::serde::encode_to_vec(&live, bincode::config::standard()).unwrap();
    *blake3::hash(&canonical).as_bytes()
}

/// A native build keeps its workspace hold while persistence admits its own
/// complete operation holder; neither can reuse the other holder's credit.
#[cfg(all(feature = "test-seams", target_pointer_width = "64"))]
#[test]
fn native_durable_generation_build_and_seal_workspaces_keep_separate_original_invoices() {
    use crate::vector_ivf_observations::{
        NativeIvfDriveCallbacks, NativeIvfFaultBoundary, NativeIvfObjectKind,
        NativeIvfOperationPrefixStage, NativeIvfSealEvent, NativeIvfSealObserver,
        with_native_ivf_seal_observer_for_test,
    };
    use contextdb_core::Wallclock;
    use contextdb_vector::ivf::{
        IvfConstructorAllocationForTest, IvfConstructorObserverForTest,
        with_ivf_constructor_observer_for_test, workspace_allocation_layout,
    };
    use std::sync::Mutex;
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let directory = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        directory.path().join("native-workspace-owners.db"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    db.execute("CREATE TABLE receipt_items (id INT PRIMARY KEY, embedding VECTOR(64) SEARCH_MODE INDEXED SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = 50000) WITH (write_buffer_bytes = 4096, training_samples = 16, training_iterations = 1))", &HashMap::new()).unwrap();
    for id in 0..2 {
        let mut vector = vec![0.0; 64];
        vector[id as usize] = 1.0;
        db.execute(
            "INSERT INTO receipt_items VALUES ($id, $v)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(id)),
                ("v".to_owned(), Value::Vector(vector)),
            ]),
        )
        .unwrap();
    }
    let index = VectorIndexRef::new("receipt_items", "embedding");
    let tuple = VectorPartitionKey::unpartitioned();
    let resolved = db
        .native_ivf_resolved_policy_for_test(&index, &tuple)
        .unwrap();
    let build_charge = workspace_allocation_layout(resolved.policy, resolved.training)
        .unwrap()
        .charged_bytes;
    let seal_charge = crate::persistence::vector_ivf::writer_operation_control_bytes();
    let owner = db.native_ivf_original_owner_for_test().unwrap();
    let accountant = db.__accountant_handle_for_test();
    #[derive(Default)]
    struct Ledger {
        before_build: Option<usize>,
        before_seal: Option<usize>,
        before_build_release: Option<usize>,
        build: Option<(u64, usize)>,
        seal: Option<(u64, usize)>,
        builds: usize,
        seals: usize,
        seal_objects: usize,
        seal_debit_mismatch: Option<(usize, usize)>,
    }
    struct Constructor {
        ledger: Arc<Mutex<Ledger>>,
        accountant: crate::memory_accounting::AccountantHandle,
        owner: usize,
        charge: usize,
    }
    impl IvfConstructorObserverForTest for Constructor {
        fn before_reserve(&self, owner: usize, bytes: usize, operation: &'static str) {
            if operation == "clustered_writer_workspace" {
                assert_eq!((owner, bytes), (self.owner, self.charge));
                self.ledger.lock().unwrap().before_build = Some(self.accountant.usage().used);
            }
        }
        fn debited(&self, invoice: u64, owner: usize, bytes: usize, operation: &'static str) {
            if operation == "clustered_writer_workspace" {
                let mut ledger = self.ledger.lock().unwrap();
                assert_eq!((owner, bytes), (self.owner, self.charge));
                assert_eq!(
                    self.accountant.usage().used - ledger.before_build.unwrap(),
                    self.charge
                );
                assert!(ledger.build.replace((invoice, bytes)).is_none());
                ledger.builds += 1;
            }
        }
        fn admitted(&self, allocation: IvfConstructorAllocationForTest) {
            if let IvfConstructorAllocationForTest::WriterWorkspace(layout) = allocation {
                assert_eq!(layout.charged_bytes, self.charge);
            }
        }
        fn buffers_freed(&self, invoice: u64, owner: usize, bytes: usize, operation: &'static str) {
            if operation == "clustered_writer_workspace" {
                let mut ledger = self.ledger.lock().unwrap();
                assert_eq!(owner, self.owner);
                assert_eq!(ledger.build, Some((invoice, bytes)));
                assert!(
                    ledger.seal.is_none(),
                    "persistence releases its own holder first"
                );
                ledger.before_build_release = Some(self.accountant.usage().used);
            }
        }
        fn released(&self, invoice: u64, owner: usize, bytes: usize, operation: &'static str) {
            if operation == "clustered_writer_workspace" {
                let mut ledger = self.ledger.lock().unwrap();
                assert_eq!((owner, bytes), (self.owner, self.charge));
                assert_eq!(ledger.build.take(), Some((invoice, bytes)));
                assert_eq!(
                    ledger.before_build_release.unwrap() - self.accountant.usage().used,
                    self.charge
                );
            }
        }
    }
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let constructor = Arc::new(Constructor {
        ledger: ledger.clone(),
        accountant: accountant.clone(),
        owner,
        charge: build_charge,
    });
    struct NativeObserver<F>(F);
    impl<F> NativeIvfSealObserver for NativeObserver<F>
    where
        F: for<'a> Fn(NativeIvfSealEvent<'a>) + Send + Sync,
    {
        fn observe(&self, event: NativeIvfSealEvent<'_>) {
            (self.0)(event);
        }
        fn before_boundary(&self, _: NativeIvfFaultBoundary<'_>) -> Result<()> {
            Ok(())
        }
    }
    let state = ledger.clone();
    let actual_accountant = accountant.clone();
    let native = Arc::new(NativeObserver(move |event: NativeIvfSealEvent<'_>| {
        let mut ledger = state.lock().unwrap();
        match event {
            NativeIvfSealEvent::OperationPrefixChecked {
                stage: NativeIvfOperationPrefixStage::Begin,
                ..
            } => {
                if ledger.build.is_some() {
                    ledger.before_seal = Some(actual_accountant.usage().used);
                }
            }
            NativeIvfSealEvent::ReservationDebited {
                invoice,
                original_owner,
                bytes,
                operation: "clustered_native_seal",
            } if ledger.build.is_some() => {
                assert_eq!((original_owner, bytes), (owner, seal_charge));
                let actual = actual_accountant
                    .usage()
                    .used
                    .checked_sub(ledger.before_seal.unwrap())
                    .unwrap_or(usize::MAX);
                if actual != seal_charge {
                    ledger.seal_debit_mismatch = Some((actual, seal_charge));
                }
                assert!(ledger.seal.replace((invoice, bytes)).is_none());
                ledger.seals += 1;
            }
            NativeIvfSealEvent::ObjectAdmitted {
                kind: NativeIvfObjectKind::SealWorkspace,
                layout,
                original_owner,
                ..
            } if ledger.seal.is_some() => {
                assert_eq!((original_owner, layout.size()), (owner, seal_charge));
                assert!(ledger.build.is_some() && ledger.seal.is_some());
                ledger.seal_objects += 1;
            }
            NativeIvfSealEvent::ReservationReleased {
                invoice,
                original_owner,
                bytes,
                operation: "clustered_native_seal",
            } if ledger.seal == Some((invoice, bytes)) => {
                assert_eq!((original_owner, bytes), (owner, seal_charge));
                assert_eq!(ledger.seal.take(), Some((invoice, bytes)));
                assert!(
                    ledger.build.is_some(),
                    "the original build holder still owns its own credit"
                );
            }
            _ => {}
        }
    }));
    let baseline = accountant.usage().used;
    with_native_ivf_seal_observer_for_test(native, || {
        with_ivf_constructor_observer_for_test(constructor, || {
            db.with_aborting_native_ivf_write_for_test(
                &index,
                &tuple,
                NativeIvfDriveCallbacks {
                    checkpoint: &|| Ok(()),
                    before_distance: &|_| Ok(()),
                },
                |scope| {
                    scope.with_clustered_writer(resolved.policy, resolved.training, |writer| {
                        writer.begin_build(0).map(|_| ())
                    })
                },
            )
        })
    })
    .unwrap();
    let ledger = ledger.lock().unwrap();
    assert_eq!(ledger.builds, 1);
    assert!(ledger.seals > 0 && ledger.seals == ledger.seal_objects);
    assert!(ledger.build.is_none() && ledger.seal.is_none());
    assert_eq!(
        ledger.seal_debit_mismatch, None,
        "persistence has a separate actual debit; the build workspace credit cannot fund both holders"
    );
    assert_eq!(
        accountant.usage().used,
        baseline,
        "both original invoices are released after the native outer abort"
    );
    assert!(
        db.native_ivf_build_progress_for_test(&index, &tuple)
            .unwrap()
            .build
            .is_none()
    );
    db.close().unwrap();
}
