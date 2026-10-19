//! Additional initialized D1 rows in the existing mapped allocator witness.
//! No sibling tests, allocator, production field, or baseline price is added.
use super::*;
use crate::persistence::content_identity::proof_allocation_oracle_tests::{
    candidate_payload_free_observation_for_test, last_request_for_test,
    last_request_pointer_for_test, measured, watch_candidate_payload_for_test,
};
use crate::persistence::vector_membership_source::{
    DurableColumnSourceFactory, DurableLoaderBuilder, DurableMembershipSource,
    DurableReverseMembershipSource, MembershipOwner,
};
use contextdb_vector::store::{
    DurableRawPartitionFactory, RawVectorMembershipSource, RawVectorMembershipSummary,
    RawVectorReverseMembershipSource,
};

#[derive(Clone, Copy, Default)]
struct Request {
    pointer: usize,
    size: usize,
    align: usize,
    count: usize,
}
impl Request {
    fn actual() -> Self {
        let (size, align, count) = last_request_for_test(false);
        Self {
            pointer: last_request_pointer_for_test(),
            size,
            align,
            count,
        }
    }
    fn assert_arc<T: ?Sized>(&self, arc: &Arc<T>, row: &str) {
        let payload = Arc::as_ptr(arc) as *const () as usize;
        let size = std::mem::size_of_val(&**arc);
        let align = std::mem::align_of_val(&**arc);
        assert!(
            self.count > 0,
            "{row}: real production constructor was measured"
        );
        assert_ne!(
            self.pointer, 0,
            "{row}: actual allocator request has an identity"
        );
        assert_eq!(
            self.align, align,
            "{row}: observed enclosing allocation alignment"
        );
        assert!(
            payload > self.pointer,
            "{row}: Arc payload follows its real control header"
        );
        assert!(
            payload + size <= self.pointer + self.size,
            "{row}: complete initialized payload lies in actual enclosing allocation"
        );
        println!(
            "CDB127_D1_ALLOCATION row={row} allocator_pointer={} payload_pointer={payload} request_size={} request_align={} actual_payload_size={size} actual_payload_align={align} constructor_request_count={}",
            self.pointer, self.size, self.align, self.count
        );
    }
}
#[derive(Default)]
struct OpenCapture {
    callback: Option<(DurableLoaderBuilder, Request)>,
    columns: Option<(Arc<DurableColumnSourceFactory>, Request)>,
    factory_credit: Request,
    factory_uses_plain_accountant: bool,
    callback_users_before: usize,
}
thread_local! {
    static OPEN: std::cell::RefCell<Option<OpenCapture>> = const { std::cell::RefCell::new(None) };
    static PAIR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static PARTITION_CREDIT: std::cell::Cell<Request> = const { std::cell::Cell::new(Request { pointer: 0, size: 0, align: 0, count: 0 }) };
    static REVERSE_REQUEST: std::cell::Cell<Request> = const { std::cell::Cell::new(Request { pointer: 0, size: 0, align: 0, count: 0 }) };
}
// Read existing census metadata before any observer Arc clone. Observers are
// inactive for every original row and compiled out of all production builds.
pub(crate) fn observe_factory_credit_for_test(
    credit: &Arc<contextdb_vector::VectorCopyAdmission>,
    accountant: &crate::memory_accounting::AccountantHandle,
) {
    OPEN.with(|slot| {
        if let Some(capture) = slot.borrow_mut().as_mut() {
            capture.factory_credit = Request::actual();
            contextdb_vector::VectorCopyAdmission::visit_retained_copy_credit(
                credit,
                &**accountant,
                &mut |_, bytes, plain| {
                    assert!(
                        bytes > 0,
                        "actual production factory owner holds admitted metadata"
                    );
                    capture.factory_uses_plain_accountant = plain;
                },
            );
            capture.callback_users_before = accountant.writer_registration_observation_for_test().3;
        }
    });
}
pub(crate) fn observe_callback_for_test(
    callback: &DurableLoaderBuilder,
    accountant: &crate::memory_accounting::AccountantHandle,
) {
    OPEN.with(|slot| { if let Some(capture) = slot.borrow_mut().as_mut() {
        capture.callback = Some((callback.clone(), Request::actual()));
        assert_eq!(accountant.writer_registration_observation_for_test().3, capture.callback_users_before + 1, "whole production callback retains exactly one original writer handle");
        assert!(!capture.factory_uses_plain_accountant, "production callback retains its real ImageMemory factory owner rather than a plain accountant capability");
    } });
}
pub(crate) fn observe_columns_for_test(columns: &Arc<DurableColumnSourceFactory>) {
    OPEN.with(|slot| {
        if let Some(capture) = slot.borrow_mut().as_mut() {
            capture.columns = Some((columns.clone(), Request::actual()));
        }
    });
}
pub(crate) fn observe_partition_credit_for_test() {
    if PAIR.with(std::cell::Cell::get) {
        PARTITION_CREDIT.with(|slot| slot.set(Request::actual()));
    }
}
pub(crate) fn observe_reverse_for_test(_reverse: &Arc<DurableReverseMembershipSource>) {
    if PAIR.with(std::cell::Cell::get) {
        REVERSE_REQUEST.with(|slot| slot.set(Request::actual()));
    }
}
fn reader_credit() -> contextdb_vector::VectorCopyAdmission {
    let declared = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
        contextdb_core::read_contract::ReadLimits::default().memory,
    );
    crate::vector_read_memory::admission(declared.child().unwrap())
}
fn open(path: &std::path::Path, owner: &Arc<MemoryAccountant>) -> (Database, OpenCapture) {
    OPEN.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(OpenCapture::default());
    });
    let (database, _, _) =
        measured(|| Database::open_with_config(path, Arc::new(CorePlugin), owner.clone()).unwrap());
    let captured = OPEN.with(|slot| slot.borrow_mut().take().unwrap());
    assert!(
        captured.callback.is_some(),
        "actual fallible writable assembly constructed the whole callback"
    );
    assert!(
        captured.columns.is_some(),
        "actual fallible writable assembly constructed its column factory"
    );
    (database, captured)
}
fn pair(
    columns: &DurableColumnSourceFactory,
    index: &VectorIndexRef,
    layout: &VectorIndexLayout,
) -> (
    Arc<dyn RawVectorReverseMembershipSource>,
    Arc<dyn DurableRawPartitionFactory>,
    Request,
    Request,
    Request,
) {
    PAIR.with(|slot| {
        assert!(!slot.replace(true));
    });
    let ((reverse, partitions), _, _) = measured(|| {
        columns
            .column_sources(index, layout, reader_credit(), reader_credit())
            .unwrap()
    });
    PAIR.with(|slot| slot.set(false));
    let reverse_request = REVERSE_REQUEST.with(std::cell::Cell::get);
    let partition_request = Request::actual();
    let credit = PARTITION_CREDIT.with(std::cell::Cell::get);
    reverse_request.assert_arc(&reverse, "column_sources.reverse");
    partition_request.assert_arc(&partitions, "column_sources.partitions");
    (
        reverse,
        partitions,
        reverse_request,
        partition_request,
        credit,
    )
}
fn finish<T: ?Sized>(
    row: &str,
    database: Database,
    owner: &Arc<MemoryAccountant>,
    control: Arc<T>,
    request: Request,
    payload_pointer: Option<usize>,
) {
    request.assert_arc(&control, row);
    let epoch = owner.writer_registration_observation_for_test().2;
    assert_ne!(epoch, 0, "{row}: real writer selected an original epoch");
    let alias = control.clone();
    let users = owner.writer_registration_observation_for_test().3;
    drop(control);
    assert_eq!(
        owner.writer_registration_observation_for_test().3,
        users,
        "{row}: Arc alias creates no second handle allocation or use"
    );
    database.close().unwrap();
    drop(database);
    assert_eq!(
        owner.writer_registration_observation_for_test().2,
        epoch,
        "{row}: independent control outlives the store in its original epoch"
    );
    let pointer = payload_pointer.unwrap_or(request.pointer);
    watch_candidate_payload_for_test(pointer, owner);
    drop(alias);
    let (census, freed, node) = candidate_payload_free_observation_for_test();
    assert!(freed.0, "{row}: actual System deallocation was observed");
    assert!(
        node.2,
        "{row}: original registration was physically deallocated"
    );
    assert_eq!(
        node.0, node.1,
        "{row}: registration refund follows physical deallocation"
    );
    if payload_pointer.is_some() {
        assert_eq!(
            census.0, epoch,
            "{row}: source/credit payload freed while original epoch is live"
        );
        assert!(
            census.1 > 0,
            "{row}: counted handle survives payload destruction"
        );
        assert!(
            freed.1,
            "{row}: payload physical free precedes node physical free"
        );
    } else {
        assert_eq!(
            census.0, 0,
            "{row}: complete control allocation freed after final handle retirement"
        );
    }
    assert_eq!(
        owner.writer_registration_observation_for_test().2,
        0,
        "{row}: final independent holder retires registration"
    );
    assert_eq!(
        owner.usage().used,
        0,
        "{row}: host accountant survives with its original zero balance"
    );
    assert_eq!(owner.underflow_count_for_test(), 0);
    println!(
        "CDB127_D1_ROW row={row} real_initialized=true original_epoch={epoch} parent_drop=true final_physical_free=true payload_observed={} final_registration=0 final_used=0",
        payload_pointer.is_some()
    );
}
fn layout() -> VectorIndexLayout {
    VectorIndexLayout {
        dimension: 1,
        quantization: VectorQuantization::F32,
        partition_key_columns: Vec::new(),
        max_partitions: None,
        search_mode: contextdb_core::VectorSearchMode::Exact,
        auto_index_at: None,
        flat_below: None,
        auto_expansion_ceiling: None,
        hnsw_m: None,
        hnsw_ef_construction: None,
        hnsw_ef_search: None,
        hnsw_search_radius_millionths: None,
        policy_revision: 0,
        consolidation_change_percent: None,
        consolidation_tombstone_percent: None,
        consolidation_disabled: false,
    }
}

pub(crate) fn mapped_rows_for_test() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("d1-initialized-controls.db");
    let seed = Database::open(&path).unwrap();
    seed.execute(
        "CREATE TABLE d1_vectors (id INTEGER PRIMARY KEY, embedding VECTOR(1))",
        &HashMap::new(),
    )
    .unwrap();
    seed.execute(
        "INSERT INTO d1_vectors VALUES (1, '[0.25]')",
        &HashMap::new(),
    )
    .unwrap();
    seed.close().unwrap();
    drop(seed);
    let index = VectorIndexRef::new("d1_vectors", "embedding");
    let partition = VectorPartitionRef::new(index.clone(), VectorPartitionKey::unpartitioned());
    let renamed = VectorIndexRef::new("renamed_d1_vectors", "embedding");
    let layout = layout();
    // Each escaping control is tested independently; two passes observe its
    // full Arc allocation and its owned metadata where an exact payload is available.
    for row in [
        "forward.from_owner",
        "forward.rename",
        "forward.partition_create",
        "reverse.from_owner",
        "reverse.rename",
        "reverse.column_sources",
        "partition.column_sources",
        "column.production_open",
    ] {
        for payload in [false, true] {
            if payload
                && matches!(
                    row,
                    "forward.rename"
                        | "forward.partition_create"
                        | "reverse.rename"
                        | "reverse.column_sources"
                )
            {
                continue;
            }
            let owner = Arc::new(MemoryAccountant::no_limit());
            let (database, capture) = open(&path, &owner);
            let (columns, columns_request) = capture.columns.unwrap();
            let factory_credit = capture.factory_credit;
            drop(capture.callback);
            let source = database.persistence.as_ref().unwrap().clone();
            let mut workspace = reader_credit();
            match row {
                "forward.from_owner" | "forward.rename" => {
                    let before = owner.writer_registration_observation_for_test().3;
                    let (original, _, _) = measured(|| {
                        DurableMembershipSource::from_owner_admitted(
                            MembershipOwner::Writable(source.clone()),
                            None,
                            &partition,
                            RawVectorMembershipSummary::default(),
                            owner.clone().into(),
                            &mut workspace,
                            reader_credit(),
                        )
                        .unwrap()
                    });
                    let original_request = Request::actual();
                    assert_eq!(
                        owner.writer_registration_observation_for_test().3,
                        before + 1,
                        "forward constructor retains exactly one original handle"
                    );
                    let (tag, metadata) = original.d1_initialized_observation_for_test();
                    assert!(
                        tag,
                        "real initialized forward control carries its writer role"
                    );
                    if row == "forward.rename" {
                        let before = owner.writer_registration_observation_for_test().3;
                        let (child, _, _) = measured(|| {
                            original
                                .for_renamed_index(&renamed, &mut workspace, reader_credit())
                                .unwrap()
                        });
                        let request = Request::actual();
                        assert_eq!(
                            owner.writer_registration_observation_for_test().3,
                            before + 1,
                            "forward rename clones exactly one original handle"
                        );
                        drop((original, source, columns, workspace));
                        finish(row, database, &owner, child, request, None);
                    } else {
                        drop((source, columns, workspace));
                        finish(
                            row,
                            database,
                            &owner,
                            original,
                            original_request,
                            payload.then_some(metadata),
                        );
                    }
                }
                "reverse.from_owner" | "reverse.rename" => {
                    let before = owner.writer_registration_observation_for_test().3;
                    let (original, _, _) = measured(|| {
                        DurableReverseMembershipSource::from_owner_admitted(
                            MembershipOwner::Writable(source.clone()),
                            &index,
                            owner.clone().into(),
                            &mut workspace,
                            reader_credit(),
                        )
                        .unwrap()
                    });
                    let original_request = Request::actual();
                    assert_eq!(
                        owner.writer_registration_observation_for_test().3,
                        before + 1,
                        "reverse constructor retains exactly one original handle"
                    );
                    let (tag, metadata) = original.d1_initialized_observation_for_test();
                    assert!(
                        tag,
                        "real initialized reverse control carries its writer role"
                    );
                    if row == "reverse.rename" {
                        let before = owner.writer_registration_observation_for_test().3;
                        let (child, _, _) = measured(|| {
                            original
                                .for_renamed_index(&renamed, &mut workspace, reader_credit())
                                .unwrap()
                        });
                        let request = Request::actual();
                        assert_eq!(
                            owner.writer_registration_observation_for_test().3,
                            before + 1,
                            "reverse rename clones exactly one original handle"
                        );
                        drop((original, source, columns, workspace));
                        finish(row, database, &owner, child, request, None);
                    } else {
                        drop((source, columns, workspace));
                        finish(
                            row,
                            database,
                            &owner,
                            original,
                            original_request,
                            payload.then_some(metadata),
                        );
                    }
                }
                "reverse.column_sources"
                | "partition.column_sources"
                | "forward.partition_create" => {
                    let before = owner.writer_registration_observation_for_test().3;
                    let (reverse, partitions, reverse_request, partition_request, credit) =
                        pair(&columns, &index, &layout);
                    assert_eq!(
                        owner.writer_registration_observation_for_test().3,
                        before + 2,
                        "column pair clones exactly two original handles; admitted layout contains no third"
                    );
                    if row == "forward.partition_create" {
                        let before = owner.writer_registration_observation_for_test().3;
                        let ((forward, loader), _, _) = measured(|| {
                            partitions
                                .create(&partition, reader_credit(), reader_credit())
                                .unwrap()
                        });
                        let request = Request::actual();
                        assert_eq!(
                            owner.writer_registration_observation_for_test().3,
                            before + 1,
                            "partition create clones exactly one forward handle"
                        );
                        drop((loader, reverse, partitions, source, columns, workspace));
                        finish(row, database, &owner, forward, request, None);
                    } else if row == "reverse.column_sources" {
                        drop((partitions, source, columns, workspace));
                        finish(row, database, &owner, reverse, reverse_request, None);
                    } else {
                        drop((reverse, source, columns, workspace));
                        finish(
                            row,
                            database,
                            &owner,
                            partitions,
                            partition_request,
                            payload.then_some(credit.pointer),
                        );
                    }
                }
                "column.production_open" => {
                    assert!(
                        columns.accountant.is_writer(),
                        "actual production column factory retains selected writer role"
                    );
                    drop((source, workspace));
                    finish(
                        row,
                        database,
                        &owner,
                        columns,
                        columns_request,
                        payload.then_some(factory_credit.pointer),
                    );
                }
                _ => unreachable!(),
            }
        }
    }
    for (invoked, observe_credit) in [(false, false), (false, true), (true, false)] {
        let owner = Arc::new(MemoryAccountant::no_limit());
        let (database, capture) = open(&path, &owner);
        let (callback, request) = capture.callback.unwrap();
        let credit = capture.factory_credit;
        drop(capture.columns);
        request.assert_arc(&callback, "callback.production_open.whole_closure");
        assert_eq!(
            std::mem::size_of_val(&*callback),
            std::mem::size_of::<LoaderCallbackCaptures>(),
            "actual callback retains the complete production capture value"
        );
        let epoch = owner.writer_registration_observation_for_test().2;
        if invoked {
            // This distinct row proves release of a separately assembled production
            // callback. The opening callback remains owned by the accepted service
            // view; its original opening and never-invoked rows stay unchanged.
            drop(callback);
            let declared = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                contextdb_core::read_contract::ReadLimits::default().memory,
            );
            let image = crate::read_image_memory::ImageMemory::charging_reader(
                owner.clone().into(),
                declared.child().unwrap(),
            );
            let mut construction = image.vector_copy(0).unwrap();
            let task_vector = VectorStore::default();
            task_vector
                .register_index_with_layout(
                    index.clone(),
                    layout.clone(),
                    construction.accountant().clone(),
                    &mut construction.owner,
                )
                .unwrap();
            OPEN.with(|slot| {
                assert!(slot.borrow().is_none());
                *slot.borrow_mut() = Some(OpenCapture::default());
            });
            measured(|| {
                register_dormant_raw_vector_partitions(
                    &task_vector,
                    &database.tx_mgr,
                    database.persistence.as_ref().unwrap(),
                    &crate::memory_accounting::AccountantHandle::from_host(owner.clone()),
                    &mut construction.owner,
                )
                .unwrap()
            });
            let separate = OPEN.with(|slot| slot.borrow_mut().take().unwrap());
            let (callback, request) = separate.callback.unwrap();
            request.assert_arc(
                &callback,
                "callback.separately_assembled_production.invoked.whole_closure",
            );
            assert_eq!(
                std::mem::size_of_val(&*callback),
                std::mem::size_of::<LoaderCallbackCaptures>(),
                "actual callback retains the complete production capture value"
            );
            drop((separate.columns, task_vector, construction, image));
            let returned = callback(&partition, &layout, reader_credit(), reader_credit()).unwrap();
            // The real owner service registers this request and holds its database
            // view while owner Drop withdraws the channel. The original answer
            // assertions run inside that accepted request after parent/callback drop.
            use crate::read_session::{OwnerReadTestHooks, QuiescentStopPhase, ReadSessionOptions};
            use contextdb_core::read_contract::{OwnerReadCancellation, OwnerRequestHandler};
            use std::sync::mpsc;
            type AcceptedRequest = Box<dyn FnOnce() -> Result<Vec<u8>> + Send>;
            struct HeldRequest {
                run: std::sync::Mutex<Option<AcceptedRequest>>,
                _retired: mpsc::Sender<()>,
            }
            impl OwnerRequestHandler for HeldRequest {
                fn handle(
                    &self,
                    _namespace: &str,
                    _request: &[u8],
                    cancellation: &OwnerReadCancellation,
                ) -> Result<Vec<u8>> {
                    let run = self.run.lock().unwrap().take().unwrap();
                    let answer = run();
                    assert!(
                        !cancellation.is_cancelled(),
                        "accepted production callback read survives graceful owner drop"
                    );
                    answer
                }
            }
            let runtime = temporary.path().join("d1-callback-runtime");
            std::fs::create_dir(&runtime).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
            }
            let (registered, registration_seen) = mpsc::channel();
            let (entered, entered_seen) = mpsc::channel();
            let (release, released) = mpsc::channel();
            let (phase_sent, phase_seen) = mpsc::channel();
            let (drain_started, _drain_seen) = mpsc::channel();
            let (service_dropped, service_gone) = mpsc::channel();
            let (handler_lifetime, handler_gone) = mpsc::channel::<()>();
            let (returned_sent, returned_seen) = mpsc::channel();
            let handler = Arc::new(HeldRequest {
                run: std::sync::Mutex::new(None),
                _retired: handler_lifetime,
            });
            let stop_observer = Arc::new(move |phase: QuiescentStopPhase| {
                phase_sent.send(phase).unwrap();
            });
            let mut database = database;
            database.owner_read_config = Arc::new(OwnerReadConfig {
                runtime_dir: Some(runtime.clone()),
                handler: Some(handler.clone()),
                test_hooks: Some(OwnerReadTestHooks {
                    clock: Arc::new(crate::local_transport::ManualDeadlineClock::default()),
                    drain_started,
                    active_request_registered: Some(registered),
                    accept_thread_exited: None,
                    accept_thread_joined: None,
                    service_dropped: Some(service_dropped),
                    quiescent_stop: Some(stop_observer),
                }),
                ..OwnerReadConfig::default()
            });
            database.owner_read_service = database.start_owner_read_service(&path).unwrap();
            assert!(
                database.owner_read_service.is_some(),
                "new callback row uses the actual accepted owner service"
            );
            database.record_owner_read_status();
            database.close_claim_window();
            *handler.run.lock().unwrap() = Some(Box::new(move || {
                entered.send(()).unwrap();
                released.recv().unwrap();
                let mut copy = reader_credit();
                let loaded = returned.load_all(&mut copy).unwrap();
                assert_eq!(
                    loaded.entries.len(),
                    1,
                    "invoked production callback's returned loader remains usable after parent and callback drop"
                );
                assert_eq!(loaded.entries[0].entry.vector, vec![0.25]);
                drop((loaded, copy));
                returned_sent.send(returned).unwrap();
                Ok(b"d1-callback".to_vec())
            }));
            let reader = crate::ReadSession::open_owner_only_in_runtime_dir(
                &path,
                ReadSessionOptions::default(),
                Some(runtime),
            )
            .unwrap();
            let reading = std::thread::spawn(move || reader.request_owner("proof", b"d1-callback"));
            registration_seen.recv().unwrap();
            entered_seen.recv().unwrap();
            drop(database);
            assert_eq!(
                phase_seen.recv().unwrap(),
                QuiescentStopPhase::AdmissionStopped
            );
            assert!(
                matches!(service_gone.try_recv(), Err(mpsc::TryRecvError::Empty)),
                "real accepted request holds database after parent drop"
            );
            assert_eq!(owner.writer_registration_observation_for_test().2, epoch);
            watch_candidate_payload_for_test(request.pointer, &owner);
            drop(callback);
            let (census, freed, node) = candidate_payload_free_observation_for_test();
            assert!(freed.0, "actual whole callback allocation physically freed");
            assert_eq!(
                census.0, epoch,
                "returned loader retains the callback's original persistence epoch"
            );
            assert!(
                !node.2,
                "returned loader remains an independent real engine user"
            );
            release.send(()).unwrap();
            assert_eq!(reading.join().unwrap().unwrap(), b"d1-callback");
            let returned = returned_seen.recv().unwrap();
            drop(handler);
            service_gone.recv().unwrap();
            assert!(
                handler_gone.recv().is_err(),
                "service database view drops before final returned-loader census"
            );
            drop(returned);
            assert_eq!(owner.writer_registration_observation_for_test().2, 0);
            assert_eq!(owner.usage().used, 0);
            assert_eq!(owner.underflow_count_for_test(), 0);
            println!(
                "CDB127_D1_ROW row=callback.separately_assembled_production.invoked real_production_callback=true whole_allocation_free=true returned_loader_after_parent=true preserved_answer=true original_epoch=true final_zero=true"
            );
        } else {
            finish(
                "callback.never_invoked",
                database,
                &owner,
                callback,
                request,
                observe_credit.then_some(credit.pointer),
            );
        }
    }
    // Original-reader constructors remain untagged when this exact host
    // accountant later acquires a writer epoch. Their rename children retain
    // that immutable role. The persistence is the real absent-slot reader path.
    let source = Arc::new(RedbPersistence::open(&path).unwrap());
    let owner = Arc::new(MemoryAccountant::no_limit());
    let mut workspace = reader_credit();
    let forward = DurableMembershipSource::from_owner_admitted(
        MembershipOwner::Writable(source.clone()),
        None,
        &partition,
        RawVectorMembershipSummary::default(),
        owner.clone().into(),
        &mut workspace,
        reader_credit(),
    )
    .unwrap();
    let reverse = DurableReverseMembershipSource::from_owner_admitted(
        MembershipOwner::Writable(source.clone()),
        &index,
        owner.clone().into(),
        &mut workspace,
        reader_credit(),
    )
    .unwrap();
    assert!(!forward.d1_initialized_observation_for_test().0);
    assert!(!reverse.d1_initialized_observation_for_test().0);
    let later = crate::memory_accounting::WriterBudget::from_accountant(owner.clone()).unwrap();
    let users = owner.writer_registration_observation_for_test().3;
    let forward_child = forward
        .for_renamed_index(&renamed, &mut workspace, reader_credit())
        .unwrap();
    let reverse_child = reverse
        .for_renamed_index(&renamed, &mut workspace, reader_credit())
        .unwrap();
    assert_eq!(
        owner.writer_registration_observation_for_test().3,
        users,
        "original-reader rename children acquire no later writer uses"
    );
    drop(later);
    assert_eq!(
        owner.writer_registration_observation_for_test().2,
        0,
        "all four original-reader controls remain live without writer liability"
    );
    assert_eq!(owner.usage().used, 0);
    drop((
        forward,
        reverse,
        forward_child,
        reverse_child,
        workspace,
        source,
    ));
    assert_eq!(owner.usage().used, 0);
    assert_eq!(owner.underflow_count_for_test(), 0);
    println!(
        "CDB127_D1_ROW row=original_reader_controls actual_absent_slot=true later_writer_excluded=true rename_children_untagged=true final_zero=true"
    );
}
