//! Guards for currency-table version compaction (`compact_currency_versions`),
//! the fix for unbounded MVCC version accumulation in the fabric currency
//! tables (a 365MB debris ledger held 248,996 `work_capabilities` versions
//! for ~10 live rows).
//!
//!  * `compaction_bounds_versions_and_change_log` — after churning one logical
//!    capability row N times, compaction collapses it to a single physical
//!    version AND drops the N-1 change-log entries in lockstep. Without the
//!    fix, both stay at N.
//!  * `changes_since_converges_to_current_truth_after_compaction` — the hard
//!    sync constraint: a full edge pull (`changes_since(0)`), a
//!    watermark-regressed re-push (`changes_since(mid)`), and a prune during
//!    ongoing churn each still yield exactly the current row with the current
//!    value — no orphaned change-log entry replays a stale/substituted
//!    version, and nothing is lost.
use super::*;
use tempfile::TempDir;

fn tags() -> Vec<String> {
    vec!["class:example.detector".to_string()]
}

fn physical_versions(db: &Database, table: &str) -> usize {
    db.relational_store
        .tables
        .read()
        .get(table)
        .map(|rows| rows.len())
        .unwrap_or(0)
}

fn change_log_row_inserts(db: &Database, table: &str) -> usize {
    db.change_log
        .read()
        .iter()
        .filter(|entry| matches!(entry, ChangeLogEntry::RowInsert { table: t, .. } if t == table))
        .count()
}

fn durable_physical_versions(db: &Database, table: &str) -> usize {
    db.__physical_version_count_for_test(table)
}

fn durable_change_log_row_inserts(db: &Database, table: &str) -> usize {
    db.change_log_since(Lsn(0)).expect("read durable history").iter()
        .filter(|entry| matches!(entry, ChangeLogEntry::RowInsert { table: owner, .. } if owner == table))
        .count()
}

fn drain_currency(db: &Database) -> (u64, u64) {
    let mut versions = 0;
    let mut history = 0;
    for _ in 0..1024 {
        let batch = db
            .compact_currency_versions()
            .expect("one finite currency batch");
        versions += batch.pruned_versions;
        history += batch.pruned_change_log_entries;
        // A finite pass may advance a different cohort without pruning.
        // Completion is this fixture's exact durable end state.
        if durable_physical_versions(db, "work_capabilities") == 1
            && durable_change_log_row_inserts(db, "work_capabilities") == 1
            && db
                .change_log_since(Lsn(0))
                .expect("read drained history")
                .len()
                <= 2
        {
            return (versions, history);
        }
    }
    panic!("the finite ledger fixture did not drain within1024batches");
}

fn current_advertised_at(db: &Database) -> i64 {
    let result = db
        .execute(
            "SELECT advertised_at FROM work_capabilities",
            &HashMap::new(),
        )
        .expect("select advertised_at");
    assert_eq!(result.rows().len(), 1, "exactly one live capability row");
    match &result.rows()[0][0] {
        Value::Timestamp(ms) => *ms,
        other => panic!("advertised_at should be a Timestamp, got {other:?}"),
    }
}

/// changes_since row values for `work_capabilities` only.
fn capability_changes(db: &Database, since: Lsn) -> Vec<i64> {
    db.changes_since(since)
        .expect("read the declared changes")
        .rows
        .into_iter()
        .filter(|change| change.table == "work_capabilities")
        .map(|change| match change.values.get("advertised_at") {
            Some(Value::Timestamp(ms)) => *ms,
            other => panic!("advertised_at should be a Timestamp, got {other:?}"),
        })
        .collect()
}

fn open_ledger() -> (TempDir, Database) {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("fabric-ledger.db");
    let db = Database::open(&path).expect("open ledger");
    crate::work_ledger::install_work_ledger_schema(&db).expect("install work ledger schema");
    (tmp, db)
}

#[test]
#[serial_test::serial]
fn compaction_bounds_versions_and_change_log() {
    // Complete cleanup copies include selected names and nested payloads.
    // These rows extend the registered compaction proof: the frozen width
    // fixture uses short names and cannot expose an omitted selected name.
    #[cfg(feature = "test-seams")]
    {
        use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
        use contextdb_core::read_memory::ReadMemoryBudget;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug)]
        struct RefuseCopy {
            requested: AtomicUsize,
            calls: AtomicUsize,
            ordinary: contextdb_core::read_memory::ReadMemoryOwner,
        }
        impl ReadMemoryBudget for RefuseCopy {
            fn try_reserve(
                &self,
                bytes: usize,
                subsystem: &'static str,
                operation: &'static str,
                hint: &'static str,
            ) -> Result<()> {
                if operation == "cleanup_schema" {
                    self.calls.fetch_add(1, Ordering::SeqCst);
                    self.requested.store(bytes, Ordering::SeqCst);
                    return Err(Error::MemoryBudgetExceeded {
                        subsystem: subsystem.into(),
                        operation: operation.into(),
                        requested_bytes: bytes,
                        available_bytes: 0,
                        budget_limit_bytes: 0,
                        hint: hint.into(),
                    });
                }
                self.ordinary.try_reserve(bytes, subsystem, operation, hint)
            }
            fn release(&self, bytes: usize) {
                self.ordinary.release(bytes);
            }
        }

        // The neighbouring counts cross the usual small map and Vec
        // growth boundaries without importing a large history fixture.
        // Collect all rows before asserting the admission lower bound.
        let mut copies = Vec::new();
        for (count, name_bytes) in [
            (1, 16),
            (1, 4096),
            (3, 4096),
            (4, 4096),
            (7, 4096),
            (8, 4096),
        ] {
            let db = Database::open_memory();
            db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            let names: Vec<String> = (0..count)
                .map(|i| format!("selected_{i}_{}", "n".repeat(name_bytes)))
                .collect();
            for name in &names {
                db.execute(&format!("CREATE TABLE {name} (id INT PRIMARY KEY, revision INT) HISTORY CURRENT ONLY SYNC OFF"), &HashMap::new()).unwrap();
                db.execute(&format!("INSERT INTO {name} VALUES (1,0)"), &HashMap::new())
                    .unwrap();
            }
            let snapshot = db.snapshot();
            let pin = db.pin_snapshot(snapshot).unwrap();
            for name in &names {
                db.execute(&format!("UPDATE {name} SET revision = 1"), &HashMap::new())
                    .unwrap();
            }
            let tables: Vec<&str> = names.iter().map(String::as_str).collect();
            let before: Vec<_> = names
                .iter()
                .map(|name| {
                    (
                        physical_versions(&db, name),
                        change_log_row_inserts(&db, name),
                    )
                })
                .collect();
            assert!(before.iter().all(|state| *state == (2, 2)));
            let lsn = db.current_lsn();
            // Stack slots deliberately omit container backing: this is a
            // conservative physical lower bound, independent of whichever
            // private container production chooses for the selected copies.
            let catalog = db.relational_store.table_meta.read();
            let (payloads, requests, request_count) = oracle::source_requests_for_test(0, || {
                let payloads: [Option<(String, TableMeta)>; 8] = std::array::from_fn(|i| {
                    (i < count).then(|| (names[i].clone(), catalog[&names[i]].clone()))
                });
                payloads
            });
            drop(catalog);
            assert!(request_count > count && request_count <= requests.len());
            let native_payload_bytes: usize = requests[..request_count].iter().map(|r| r.1).sum();
            assert!(native_payload_bytes >= names.iter().map(String::len).sum());
            drop(payloads);
            let refusal = Arc::new(RefuseCopy {
                requested: AtomicUsize::new(0),
                calls: AtomicUsize::new(0),
                ordinary: db.declared_reader_memory().unwrap(),
            });
            let outcome =
                db.__compact_currency_versions_for_tables_for_test(&tables, refusal.clone());
            assert!(
                matches!(&outcome, Err(Error::MemoryBudgetExceeded { operation, .. }) if operation == "cleanup_schema"),
                "selected-copy admission must refuse typed: {outcome:?}"
            );
            assert_eq!(
                refusal.calls.load(Ordering::SeqCst),
                1,
                "no later allocation admission after the refused copy"
            );
            assert_eq!(db.current_lsn(), lsn);
            assert_eq!(
                names
                    .iter()
                    .map(|name| (
                        physical_versions(&db, name),
                        change_log_row_inserts(&db, name)
                    ))
                    .collect::<Vec<_>>(),
                before,
                "refusal publishes neither row erasure nor history pruning"
            );
            let deferred = db
                .__compact_currency_versions_for_tables_for_test(
                    &tables,
                    db.declared_reader_memory().unwrap(),
                )
                .unwrap();
            assert_eq!(deferred.pruned_versions, 0);
            assert_eq!(deferred.versions_deferred_for_readers, count as u64);
            for name in &names {
                assert_eq!(
                    db.execute_at_snapshot(
                        &format!("SELECT revision FROM {name}"),
                        &HashMap::new(),
                        snapshot
                    )
                    .unwrap()
                    .rows(),
                    vec![vec![Value::Int64(0)]],
                    "admitted cleanup preserves each pinned reader's original answer"
                );
            }
            drop(pin);
            let retried = db
                .__compact_currency_versions_for_tables_for_test(
                    &tables,
                    db.declared_reader_memory().unwrap(),
                )
                .unwrap();
            assert_eq!(
                retried.pruned_versions, count as u64,
                "retry must still find every refused stale version"
            );
            for name in &names {
                assert_eq!(physical_versions(&db, name), 1);
                assert_eq!(
                    db.execute(&format!("SELECT revision FROM {name}"), &HashMap::new())
                        .unwrap()
                        .rows(),
                    vec![vec![Value::Int64(1)]]
                );
            }
            copies.push((
                count,
                name_bytes,
                refusal.requested.load(Ordering::SeqCst),
                native_payload_bytes,
            ));
        }
        assert!(
            copies
                .iter()
                .all(|(_, _, admitted, native)| admitted >= native),
            "cleanup must admit every physically copied selected name and nested payload, even before container backing: {copies:?}"
        );
    }
    // The real selected-copy container and nested payload must be paid
    // before allocation and until System.dealloc returns. Empty tables
    // isolate this production copy from row/keeper workspace; the earlier
    // external-clone rows cannot see its backing or refund order.
    #[cfg(feature = "test-seams")]
    {
        use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
        use contextdb_core::read_memory::ReadMemoryBudget;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug)]
        struct ObserveCopy {
            ordinary: contextdb_core::read_memory::ReadMemoryOwner,
            writer: crate::memory_accounting::AccountantHandle,
            parent: crate::vector_read_memory::DeclaredReadRoot,
            baseline: usize,
            requested: AtomicUsize,
            calls: AtomicUsize,
            reservations: parking_lot::Mutex<([(&'static str, usize, usize, usize); 64], usize)>,
            releases: parking_lot::Mutex<([(usize, usize, usize); 64], usize)>,
            refunds: AtomicUsize,
            observation: parking_lot::Mutex<Option<oracle::DeclarationObservation>>,
            receipt: parking_lot::Mutex<Option<(usize, usize, usize)>>,
        }
        impl ReadMemoryBudget for ObserveCopy {
            fn try_reserve(
                &self,
                bytes: usize,
                subsystem: &'static str,
                operation: &'static str,
                hint: &'static str,
            ) -> Result<()> {
                let before = self.parent.retained_bytes_for_source_unit_test() as usize;
                self.ordinary
                    .try_reserve(bytes, subsystem, operation, hint)?;
                let after = self.parent.retained_bytes_for_source_unit_test() as usize;
                {
                    let mut reservations = self.reservations.lock();
                    let index = reservations.1;
                    if index < reservations.0.len() {
                        reservations.0[index] = (operation, bytes, before, after);
                    }
                    reservations.1 += 1;
                }
                if operation == "cleanup_schema" && self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.requested.store(bytes, Ordering::SeqCst);
                    let mut observation = oracle::DeclarationObservation::funded_by_reader(
                        self.parent.clone(),
                        self.baseline,
                    );
                    observation.observe_writer(self.writer.clone());
                    *self.observation.lock() = Some(observation);
                }
                Ok(())
            }
            fn release(&self, bytes: usize) {
                // Other phase invoices (including ImageMemory controls)
                // also use this budget. Match the selected invoice by its
                // unique request size, verified outside every destructor.
                let before = self.parent.retained_bytes_for_source_unit_test() as usize;
                self.ordinary.release(bytes);
                let after = self.parent.retained_bytes_for_source_unit_test() as usize;
                {
                    let mut releases = self.releases.lock();
                    let index = releases.1;
                    if index < releases.0.len() {
                        releases.0[index] = (bytes, before, after);
                    }
                    releases.1 += 1;
                }
                if bytes != 0 && bytes == self.requested.load(Ordering::SeqCst) {
                    self.refunds.fetch_add(1, Ordering::SeqCst);
                    if let Some(observation) = self.observation.lock().as_ref() {
                        *self.receipt.lock() = Some(observation.physical_receipt());
                    }
                }
            }
        }
        // Calibrate the same physical oracle before trusting the copy
        // rows: a block from before observation is outside its census,
        // while a genuinely early refund must leave a positive gap until
        // the observed block has actually passed through System.dealloc.
        {
            let db = Database::open_memory();
            let ordinary = db.declared_reader_memory().unwrap();
            let baseline = db
                .declared_read_memory
                .retained_bytes_for_source_unit_test() as usize;
            let credit = contextdb_core::read_memory::ReadCredit::try_new(
                ordinary.clone(),
                31,
                "reader",
                "oracle_control",
                "physical oracle control",
            )
            .unwrap();
            let ((before, funded, refunded, freed), _, _) = oracle::measured(|| {
                let earlier = std::hint::black_box(Vec::<u8>::with_capacity(17));
                let observation = oracle::DeclarationObservation::funded_by_reader(
                    db.declared_read_memory.clone(),
                    baseline,
                );
                drop(earlier);
                let before = observation.physical_receipt();
                let payload = std::hint::black_box(Vec::<u8>::with_capacity(31));
                let funded = observation.physical_receipt();
                drop(credit);
                let refunded = observation.physical_receipt();
                drop(payload);
                let freed = observation.physical_receipt();
                drop(observation);
                (before, funded, refunded, freed)
            });
            assert_eq!(
                before,
                (0, 0, 0),
                "pre-observation frees do not debit this epoch"
            );
            assert_eq!(
                funded,
                (31, 0, 31),
                "real parent credit precedes the physical allocation"
            );
            assert_eq!(
                refunded,
                (31, 31, 31),
                "a genuinely early refund remains a positive gap"
            );
            assert_eq!(
                freed,
                (0, 31, 31),
                "physical free retires live bytes without hiding the gap"
            );
            assert_eq!(
                db.declared_read_memory
                    .retained_bytes_for_source_unit_test() as usize,
                baseline
            );
        }
        for count in [1, 3, 4, 7, 8] {
            let db = Database::open_memory();
            db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            let names: Vec<_> = (0..count).map(|i| format!("empty_selected_{i}")).collect();
            for name in &names {
                db.execute(&format!("CREATE TABLE {name} (id INT PRIMARY KEY, revision INT) HISTORY CURRENT ONLY SYNC OFF"),
                    &HashMap::new()).unwrap();
            }
            let tables: Vec<_> = names.iter().map(String::as_str).collect();
            let ordinary = db.declared_reader_memory().unwrap();
            let baseline = db
                .declared_read_memory
                .retained_bytes_for_source_unit_test() as usize;
            let observer = Arc::new(ObserveCopy {
                ordinary,
                writer: crate::memory_accounting::AccountantHandle::clone(
                    &db.finalization.accountant,
                ),
                parent: db.declared_read_memory.clone(),
                baseline,
                requested: AtomicUsize::new(0),
                reservations: parking_lot::Mutex::new(([("", 0, 0, 0); 64], 0)),
                releases: parking_lot::Mutex::new(([(0, 0, 0); 64], 0)),
                refunds: AtomicUsize::new(0),
                calls: AtomicUsize::new(0),
                observation: parking_lot::Mutex::new(None),
                receipt: parking_lot::Mutex::new(None),
            });
            let lsn = db.current_lsn();
            let ((outcome, final_receipt, final_evidence), _, _) = oracle::measured(|| {
                let outcome =
                    db.__compact_currency_versions_for_tables_for_test(&tables, observer.clone());
                // Capture at the actual producer return, inside measured:
                // its epilogue and current_lsn's new public operation are
                // not compaction. Never close over retained producer bytes.
                let observation = observer.observation.lock().take().unwrap();
                let final_receipt = observation.physical_receipt();
                let final_evidence = observation.evidence();
                assert_eq!(
                    observer.refunds.load(Ordering::SeqCst),
                    1,
                    "operation return must follow the actual selected-copy refund"
                );
                assert_eq!(
                    final_receipt.0, 0,
                    "cannot end observation with actual producer allocations still live: {final_evidence:?}"
                );
                assert_eq!(
                    observer.parent.retained_bytes_for_source_unit_test() as usize,
                    baseline,
                    "operation return must follow all actual copy invoice releases"
                );
                drop(observation);
                (outcome, final_receipt, final_evidence)
            });
            let report = outcome.unwrap();
            assert_eq!(report.tables_in_memory_snapshot, count as u64);
            assert_eq!(report.pruned_versions, 0);
            // A separate native window names the unrelated public DB
            // inspection. Its requests never enter the compaction receipt.
            let (inspection_lsn, inspection_requests, inspection_count) =
                oracle::native_requests_for_test(|| db.current_lsn());
            assert_eq!(inspection_lsn, lsn);
            assert!(
                inspection_count <= inspection_requests.len(),
                "unrelated current_lsn allocation census must be complete"
            );
            assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
            let reservations = observer.reservations.lock();
            assert!(
                reservations.1 <= reservations.0.len(),
                "invoice census must be complete"
            );
            assert_eq!(
                reservations.0[..reservations.1]
                    .iter()
                    .filter(|bytes| bytes.1 == observer.requested.load(Ordering::SeqCst))
                    .count(),
                1,
                "selected-copy invoice must be distinguishable from every other phase"
            );
            assert_eq!(
                observer.refunds.load(Ordering::SeqCst),
                1,
                "exactly one real selected-copy refund must be observed"
            );
            let releases = observer.releases.lock();
            assert!(
                releases.1 <= releases.0.len(),
                "release census must be complete"
            );
            let evidence = final_evidence;
            assert!(
                evidence.complete,
                "declaration allocation census must be complete: {evidence:?}"
            );
            let (live_at_refund, gap, native_peak) = observer
                .receipt
                .lock()
                .take()
                .expect("the actual selected-copy invoice was released");
            assert!(
                native_peak > names.iter().map(String::len).sum::<usize>(),
                "actual copy includes storage beyond selected names"
            );
            assert_eq!(
                gap,
                0,
                "{count} selected declarations: every overlapping native request, including container backing, stays paid through physical free; first-gap/native origin={evidence:?}; baseline={baseline}; reservations={:?}; releases={:?}",
                &reservations.0[..reservations.1],
                &releases.0[..releases.1]
            );
            assert_eq!(
                live_at_refund, 0,
                "{count} selected declarations: refund follows every actual copy free"
            );

            assert!(
                final_evidence.complete,
                "final declaration census must be complete: {final_evidence:?}"
            );
            assert_eq!(
                final_receipt,
                (0, 0, native_peak),
                "observation includes the real refund and every subsequent physical free before unrelated inspection; first-gap/native origin={final_evidence:?}; baseline={baseline}; unrelated current_lsn requests={inspection_requests:?} count={inspection_count}; reservations={:?}; releases={:?}",
                &reservations.0[..reservations.1],
                &releases.0[..releases.1]
            );
            drop(reservations);
            drop(releases);
            assert_eq!(
                observer.parent.retained_bytes_for_source_unit_test() as usize,
                baseline,
                "the actual production budget returns to its initial debit"
            );
        }
    }
    // A refused proof pass publishes no cleanup or progress and can be retried
    // with its ordinary owner after resource relief.
    #[cfg(any(test, feature = "test-seams"))]
    {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        struct RefuseNextProof;
        impl RefuseNextProof {
            fn arm(
                owner: &crate::memory_accounting::AccountantHandle,
                fired: Arc<AtomicBool>,
                proof_seen: Arc<AtomicUsize>,
                target: Arc<
                    std::sync::Mutex<
                        Option<
                            crate::memory_accounting::compaction_admission_capture::GroupingRequest,
                        >,
                    >,
                >,
            ) {
                let captured_owner = owner.clone();
                owner.set_admission_race_hook(Box::new(move || {
                    let entries = crate::memory_accounting::take_budget_ledger_for_test();
                    crate::memory_accounting::watch_budget_ledger_for_test();
                    // Traffic remains diagnostic; it never chooses the cut.
                    proof_seen.fetch_add(entries.iter().filter(|entry|
                        entry.what == "content_identity/maintain_committed_proof"
                    ).count(), Ordering::SeqCst);
                    let owner = captured_owner.clone();
                    let request = crate::memory_accounting::compaction_admission_capture::grouping_request(&owner)
                        .filter(|request| request.variant == crate::memory_accounting::compaction_admission_capture::GroupingApply::Borrowed
                            && request.bytes != 0);
                    if let Some(request) = request {
                        // Scoped pruning applies posting_outputs first, then
                        // identity_mutations. Choose its first nonempty borrowed
                        // group, before any tuple slice allocation. The owned
                        // preparation path is observed but cannot select this cut.
                        assert!(!fired.swap(true, Ordering::SeqCst));
                        assert_eq!(request.payer, owner.as_ptr() as usize);
                        assert_eq!(owner.usage().used, request.held);
                        owner.set_budget(Some(request.held + request.bytes - 1)).unwrap();
                        *target.lock().unwrap() = Some(request);
                        crate::memory_accounting::compaction_admission_capture::note(&owner, "cut", 0);
                    } else if !fired.load(Ordering::SeqCst) {
                        Self::arm(&owner, fired.clone(), proof_seen.clone(), target.clone());
                    }
                }));
            }
        }
        let (_tmp, db) = open_ledger();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.__set_currency_maintenance_interval(Duration::from_secs(3600));
        for advertised_at in 1..=3 {
            crate::work_ledger::advertise_capability(
                &db,
                "node-a",
                "example-detector-cpu",
                &tags(),
                advertised_at,
            )
            .unwrap();
        }
        let capture = || {
            let root = writer_read::durable_root(db.tx_mgr.capture_published_read().handle)
                .unwrap()
                .unwrap();
            let meta = db
                .relational_store
                .table_meta
                .read()
                .get("work_capabilities")
                .unwrap()
                .clone();
            let declaration = RedbPersistence::currency_cleanup_declaration(&meta).unwrap();
            let state =
                RedbPersistence::currency_cleanup_progress(&root, "work_capabilities", declaration)
                    .unwrap();
            (
                state.declaration,
                state.through,
                state.after_group,
                state
                    .group
                    .map(|group| (group.row, group.keeper, group.prune_after)),
                RedbPersistence::currency_cleanup_rotation(&root).unwrap(),
                physical_versions(&db, "work_capabilities"),
                change_log_row_inserts(&db, "work_capabilities"),
                durable_physical_versions(&db, "work_capabilities"),
                durable_change_log_row_inserts(&db, "work_capabilities"),
                capability_changes(&db, Lsn(0)),
                db.current_lsn(),
            )
        };
        let before = capture();
        assert_eq!(before.7, 3);
        // This fixture has one scalar currency key. Select its actual stale
        // RowInsert LSNs and run the production commit-candidate selection.
        // The scoped transaction contributes one expiry key per stale row,
        // one current-row key, and distinct history/commit-index keys.
        let history = db.change_log_since(Lsn(0)).unwrap();
        let keeper = history
            .iter()
            .filter_map(|entry| match entry {
                ChangeLogEntry::RowInsert { table, lsn, .. } if table == "work_capabilities" => {
                    Some(*lsn)
                }
                _ => None,
            })
            .max()
            .unwrap();
        let removed_lsn_counts: HashMap<Lsn, u64> = history
            .iter()
            .filter_map(|entry| match entry {
                ChangeLogEntry::RowInsert { table, lsn, .. }
                    if table == "work_capabilities" && *lsn != keeper =>
                {
                    Some((*lsn, 1))
                }
                _ => None,
            })
            .collect();
        let affected_lsns = removed_lsn_counts.keys().copied().collect();
        let commit_keys = scoped_commit_index_candidates(
            &db.tx_mgr,
            &db.change_log_lsn_refcounts.read(),
            &removed_lsn_counts,
            &affected_lsns,
        )
        .unwrap();
        let stale_keys = removed_lsn_counts.len();
        let family_counts = [stale_keys, 1, stale_keys + commit_keys.len()];
        let unique_keys = family_counts.iter().sum::<usize>();
        let grouped_bytes = family_counts
            .into_iter()
            .map(|count| {
                std::alloc::Layout::array::<(Vec<u8>, Option<[u8; 32]>)>(count)
                    .unwrap()
                    .size()
            })
            .sum::<usize>();
        let original_limit = db.memory_limit();
        let fired = Arc::new(AtomicBool::new(false));
        let proof_seen = Arc::new(AtomicUsize::new(0));
        let target = Arc::new(std::sync::Mutex::new(None));
        let capture_storage = crate::memory_accounting::compaction_admission_capture::prepare();
        crate::memory_accounting::watch_budget_ledger_for_test();
        RefuseNextProof::arm(
            &db.accountant,
            fired.clone(),
            proof_seen.clone(),
            target.clone(),
        );
        crate::memory_accounting::compaction_admission_capture::start(&db.accountant);
        let refused = db.__compact_currency_versions_for_tables_for_test(
            &["work_capabilities"],
            db.declared_reader_memory().unwrap(),
        );
        db.accountant.set_admission_race_hook(Box::new(|| {}));
        let _traffic = crate::memory_accounting::take_budget_ledger_for_test();
        db.set_memory_limit(original_limit).unwrap();
        crate::memory_accounting::compaction_admission_capture::emit();
        let actual_group = target
            .lock()
            .unwrap()
            .expect("the actual borrowed grouping admission must fire");
        assert!(fired.load(Ordering::SeqCst));
        assert_eq!(
            actual_group.variant,
            crate::memory_accounting::compaction_admission_capture::GroupingApply::Borrowed
        );
        assert_eq!(actual_group.phase, "proof-grouping");
        assert_eq!(actual_group.site, "borrowed grouped tuple slices");
        let selected_family = actual_group
            .family
            .expect("ordinary pruning groups one actual family at a time");
        assert!(actual_group.family_counts[selected_family] > 0);
        assert!(
            actual_group
                .family_counts
                .iter()
                .enumerate()
                .all(|(family, count)| family == selected_family || *count == 0)
        );
        assert!(
            actual_group.apply > 0,
            "the actual apply must be identified"
        );
        assert_eq!(actual_group.payer, db.accountant.as_ptr() as usize);
        let actual_grouped_bytes = actual_group
            .family_counts
            .into_iter()
            .map(|count| {
                std::alloc::Layout::array::<(Vec<u8>, Option<[u8; 32]>)>(count)
                    .unwrap()
                    .size()
            })
            .sum::<usize>();
        assert_eq!(actual_group.bytes, actual_grouped_bytes);
        assert!(actual_group.bytes > 0);
        // The old estimate describes identity keys, but posting_outputs can
        // group first. Preserve that calculation as workload evidence; the
        // actual production Layout/counts above now set the exact G-1 cut.
        assert_eq!(unique_keys, family_counts.iter().sum::<usize>());
        assert_eq!(
            grouped_bytes,
            unique_keys * std::mem::size_of::<(Vec<u8>, Option<[u8; 32]>)>()
        );
        crate::memory_accounting::compaction_admission_capture::assert_exact_group_refusal(
            actual_group,
        );
        drop(capture_storage);
        eprintln!("COMPACTION_ADMISSION_RAW_RESULT {refused:?}");
        assert!(
            fired.load(Ordering::SeqCst) && proof_seen.load(Ordering::SeqCst) > 0,
            "the existing seam must reach actual accounted proof maintenance"
        );
        let error = refused.unwrap_err();
        assert!(
            matches!(error, Error::MemoryBudgetExceeded { ref subsystem, ref operation, .. } if subsystem == "content_identity" && operation == "maintain_committed_proof"),
            "the original proof memory refusal must escape without successful unaccounted retry: {error:?}"
        );
        if let Error::MemoryBudgetExceeded {
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            ..
        } = error
        {
            assert_eq!(requested_bytes, actual_group.bytes);
            assert_eq!(available_bytes, actual_group.bytes - 1);
            assert_eq!(
                budget_limit_bytes,
                actual_group.held + actual_group.bytes - 1
            );
        }
        assert_eq!(
            capture(),
            before,
            "a refused pass changes neither durable progress nor in-memory pruning or exported history"
        );
        let retry = db
            .__compact_currency_versions_for_tables_for_test(
                &["work_capabilities"],
                db.declared_reader_memory().unwrap(),
            )
            .unwrap();
        assert_eq!(retry.pruned_versions, 2);
        assert_eq!(durable_physical_versions(&db, "work_capabilities"), 1);
        assert_eq!(current_advertised_at(&db), 3);
    }
    let (_tmp, db) = open_ledger();
    // Hold the auto-maintenance tick (60s in production) out of this test: under machine
    // load the 200-upsert setup can outlast one tick, and a mid-setup auto-compaction
    // destroys the accumulated-versions precondition this test exists to compact manually.
    // The auto-heal path is proven by installing_the_ledger_starts_maintenance_and_it_auto_heals.
    db.__set_currency_maintenance_interval(Duration::from_secs(3600));
    const N: i64 = 200;
    for advertised_at in 1..=N {
        crate::work_ledger::advertise_capability(
            &db,
            "node-a",
            "example-detector-cpu",
            &tags(),
            advertised_at,
        )
        .expect("advertise");
    }

    // The defect: one logical row, N physical versions, N change-log entries.
    assert_eq!(
        durable_physical_versions(&db, "work_capabilities"),
        N as usize
    );
    assert_eq!(
        durable_change_log_row_inserts(&db, "work_capabilities"),
        N as usize
    );

    let (pruned_versions, pruned_history) = drain_currency(&db);
    assert_eq!(pruned_versions, (N - 1) as u64);
    // Each ON-CONFLICT update writes both a RowInsert (new version) and a
    // RowDelete (old version), so at least the N-1 superseded inserts plus
    // their deletes are dropped in lockstep.
    assert!(
        pruned_history >= (N - 1) as u64,
        "at least the superseded versions' change-log entries must be dropped in \
             lockstep; got {}",
        pruned_history
    );

    // Bounded: one live version, one surviving RowInsert, tiny total log.
    assert_eq!(
        durable_physical_versions(&db, "work_capabilities"),
        1,
        "compaction must collapse a logical row to its single current version"
    );
    assert_eq!(
        durable_change_log_row_inserts(&db, "work_capabilities"),
        1,
        "the superseded versions' change-log entries must be dropped in lockstep"
    );
    assert!(
        db.change_log_since(Lsn(0)).unwrap().len() <= 2,
        "only the keeper commit's entries survive; got {}",
        db.change_log_since(Lsn(0)).unwrap().len()
    );
    // Correctness preserved: the surviving row is the CURRENT one.
    assert_eq!(current_advertised_at(&db), N);

    // Idempotent: nothing left to compact.
    let again = db.compact_currency_versions().expect("compact again");
    assert_eq!(again.pruned_versions, 0);
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn changes_since_survives_reopen_after_compaction() {
    // Allowed graph history is complete, or extraction returns the original
    // metadata read failure after the preceding node histories were admitted.
    {
        use contextdb_core::read_memory::{ReadCredit, ReadMemoryBudget};
        use std::sync::atomic::{AtomicUsize, Ordering};
        #[derive(Debug)]
        struct MetadataOwner {
            ordinary: contextdb_core::read_memory::ReadMemoryOwner,
            refused: Arc<crate::memory_accounting::MemoryAccountant>,
            refuse: bool,
            answer_admissions: Arc<AtomicUsize>,
            metadata_calls: AtomicUsize,
            admissions_before_metadata: AtomicUsize,
            original_error: std::sync::Mutex<Option<String>>,
        }
        impl ReadMemoryBudget for MetadataOwner {
            fn try_reserve(
                &self,
                bytes: usize,
                subsystem: &'static str,
                operation: &'static str,
                hint: &'static str,
            ) -> Result<()> {
                if operation == "graph_metadata_tables" {
                    self.metadata_calls.fetch_add(1, Ordering::SeqCst);
                    self.admissions_before_metadata.fetch_max(
                        self.answer_admissions.load(Ordering::SeqCst),
                        Ordering::SeqCst,
                    );
                    if self.refuse {
                        if let Err(error) =
                            self.refused.try_reserve(bytes, subsystem, operation, hint)
                        {
                            *self.original_error.lock().unwrap() = Some(format!("{error:?}"));
                            return Err(error);
                        }
                        self.refused.release(bytes);
                    }
                }
                self.ordinary.try_reserve(bytes, subsystem, operation, hint)
            }
            fn release(&self, bytes: usize) {
                self.ordinary.release(bytes);
            }
        }
        let mut observations = Vec::new();
        let mut expected = Vec::new();
        for deleted in [false, true] {
            for fault in ["memory", "source", "denied"] {
                let mut db = Database::open_memory();
                db.execute("CREATE TABLE export_nodes (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, tick INTEGER)", &HashMap::new()).unwrap();
                db.execute("CREATE TABLE export_edge_metadata (source_id UUID, target_id UUID, edge_type TEXT, PRIMARY KEY (source_id, target_id, edge_type))", &HashMap::new()).unwrap();
                let source = uuid::Uuid::from_u128(1);
                let target = uuid::Uuid::from_u128(2);
                let context = uuid::Uuid::from_u128(10);
                for id in [source, target] {
                    db.execute(
                        "INSERT INTO export_nodes VALUES ($id, $context, 0)",
                        &HashMap::from([
                            ("id".into(), Value::Uuid(id)),
                            ("context".into(), Value::Uuid(context)),
                        ]),
                    )
                    .unwrap();
                }
                db.execute(
                    "INSERT INTO export_edge_metadata VALUES ($source, $target, 'link')",
                    &HashMap::from([
                        ("source".into(), Value::Uuid(source)),
                        ("target".into(), Value::Uuid(target)),
                    ]),
                )
                .unwrap();
                let tx = db.begin().unwrap();
                db.insert_edge(
                    tx,
                    source,
                    target,
                    "link".into(),
                    HashMap::from([("origin".into(), Value::Text("standalone".into()))]),
                )
                .unwrap();
                db.commit(tx).unwrap();
                if !deleted {
                    let tx = db.begin().unwrap();
                    db.delete_edge(tx, source, target, "link").unwrap();
                    db.commit(tx).unwrap();
                }
                let since = db.current_lsn();
                db.execute("UPDATE export_nodes SET tick = 1", &HashMap::new())
                    .unwrap();
                let tx = db.begin().unwrap();
                if deleted {
                    db.delete_edge(tx, source, target, "link").unwrap();
                } else {
                    db.insert_edge(
                        tx,
                        source,
                        target,
                        "link".into(),
                        HashMap::from([("origin".into(), Value::Text("standalone".into()))]),
                    )
                    .unwrap();
                }
                db.commit(tx).unwrap();
                db.access.contexts = Some([ContextId::new(context)].into_iter().collect());
                let control = db.changes_since(since).unwrap();
                assert_eq!(
                    control
                        .rows
                        .iter()
                        .filter(|row| row.table == "export_nodes")
                        .count(),
                    2
                );
                assert_eq!(control.edges.len(), 1);
                assert_eq!(
                    control.edges[0].properties.get("__deleted"),
                    deleted.then_some(&Value::Bool(true))
                );
                if fault == "source" {
                    // Remove the actual metadata index while preserving the
                    // committed rows and histories. Its required read now fails.
                    db.relational_store
                        .indexes
                        .write()
                        .get_mut("export_nodes")
                        .unwrap()
                        .clear();
                } else if fault == "denied" {
                    db.access.contexts = Some(
                        [ContextId::new(uuid::Uuid::from_u128(11))]
                            .into_iter()
                            .collect(),
                    );
                    assert!(
                        !db.node_read_allowed(
                            source,
                            db.snapshot(),
                            db.declared_reader_memory().unwrap()
                        )
                        .unwrap()
                    );
                }
                let source_error = if fault == "source" {
                    Some(format!(
                        "{:?}",
                        db.node_read_allowed(
                            source,
                            db.snapshot(),
                            db.declared_reader_memory().unwrap()
                        )
                        .unwrap_err()
                    ))
                } else {
                    None
                };
                let answer_admissions = Arc::new(AtomicUsize::new(0));
                let ordinary = db.declared_reader_memory().unwrap();
                let owner = Arc::new(MetadataOwner {
                    ordinary: ordinary.clone(),
                    refused: Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(1)),
                    refuse: fault == "memory",
                    answer_admissions: answer_admissions.clone(),
                    metadata_calls: AtomicUsize::new(0),
                    admissions_before_metadata: AtomicUsize::new(0),
                    original_error: std::sync::Mutex::new(None),
                });
                let credit = std::sync::Mutex::new(ReadCredit::new(ordinary));
                let admit = |bytes| {
                    answer_admissions.fetch_add(1, Ordering::SeqCst);
                    credit.lock().unwrap().try_grow_for(
                        bytes,
                        "reader",
                        "history_answer",
                        "Reduce the selected history.",
                    )
                };
                let release = |bytes| credit.lock().unwrap().try_shrink(bytes);
                let projection = ChangeProjection {
                    checkpoint: &|| Ok(()),
                    admit: &admit,
                    release: &release,
                    bounded: true,
                    ddl: false,
                    page: None,
                };
                let mut copy = crate::vector_read_memory::admission(owner.clone());
                let outcome =
                    db.changes_since_base_projected(since, &projection, &mut copy, owner.clone());
                let original =
                    source_error.or_else(|| owner.original_error.lock().unwrap().clone());
                let expected_outcome = if fault == "denied" {
                    "denied normally".to_string()
                } else {
                    original
                        .expect("the real failed metadata read records its original typed error")
                };
                let observed_outcome = match outcome {
                    Err(error) => format!("{error:?}"),
                    Ok((changes, _))
                        if fault == "denied"
                            && changes.rows.is_empty()
                            && changes.edges.is_empty() =>
                    {
                        "denied normally".to_string()
                    }
                    Ok((changes, _)) => format!(
                        "successful partial export: nodes={}, edges={}",
                        changes
                            .rows
                            .iter()
                            .filter(|row| row.table == "export_nodes")
                            .count(),
                        changes.edges.len()
                    ),
                };
                observations.push((
                    deleted,
                    fault,
                    owner.metadata_calls.load(Ordering::SeqCst) > 0,
                    fault == "denied"
                        || owner.admissions_before_metadata.load(Ordering::SeqCst) >= 2,
                    observed_outcome,
                ));
                expected.push((deleted, fault, true, true, expected_outcome));
            }
        }
        // Collect both insertions and deletions before checking the matrix so
        // one failing permission read cannot mask the other export branch.
        assert_eq!(observations, expected);
    }
    // The rewritten change log must round-trip through persistence: after a
    // compaction, a reopened ledger must reconstruct the same current truth
    // (proves rewrite_change_log wrote valid keys/ordering and commit-index
    // reconstruction still agrees).
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("fabric-ledger.db");
    {
        let db = Database::open(&path).expect("open");
        crate::work_ledger::install_work_ledger_schema(&db).expect("install");
        // Pin the background tick out of the way: the version buildup below
        // is the precondition, and an auto-compaction racing it would turn
        // this test's verdict into a scheduling artifact.
        db.__set_currency_maintenance_interval(Duration::from_secs(3600));
        for advertised_at in 1..=80 {
            crate::work_ledger::advertise_capability(
                &db,
                "node-a",
                "example-detector-cpu",
                &tags(),
                advertised_at,
            )
            .expect("advertise");
        }
        drain_currency(&db);
    }
    // Reopen from disk.
    let db = Database::open(&path).expect("reopen");
    assert_eq!(durable_physical_versions(&db, "work_capabilities"), 1);
    assert_eq!(current_advertised_at(&db), 80);
    assert_eq!(
        capability_changes(&db, Lsn(0)),
        vec![80],
        "a full pull after reopen must still converge to current truth"
    );
}

#[test]
fn changes_since_converges_to_current_truth_after_compaction() {
    let (_tmp, db) = open_ledger();
    // Pin the background tick out of the way. Without this, an
    // auto-compaction landing between the buildup and the
    // `changes_since(...).rows[N/2]` read collapses the row vector and
    // index-panics — the canonical scheduling-dependent verdict flip.
    db.__set_currency_maintenance_interval(Duration::from_secs(3600));
    const N: i64 = 120;
    // Record a mid-range watermark straight from the ledger during the
    // churn. A full pull already converges each identity to its current
    // version, so the changeset carries one row, not N — a mid-range LSN
    // comes from the clock at the midpoint, not from indexing the changeset.
    let mut mid_lsn = Lsn(0);
    for advertised_at in 1..=N {
        crate::work_ledger::advertise_capability(
            &db,
            "node-a",
            "example-detector-cpu",
            &tags(),
            advertised_at,
        )
        .expect("advertise");
        if advertised_at == N / 2 {
            mid_lsn = db.current_lsn();
        }
    }

    db.compact_currency_versions().expect("compact");

    // Full edge pull (since = 0): exactly the current row, current value.
    let full = capability_changes(&db, Lsn(0));
    assert_eq!(
        full,
        vec![N],
        "a wiped-edge full pull after a prune must transfer exactly the current \
             capability version and converge to current truth — no orphaned change-log \
             entry replaying a stale/substituted version"
    );

    // Watermark-regressed re-push (since = mid): still converges, still bounded.
    let regressed = capability_changes(&db, mid_lsn);
    assert!(
        regressed.iter().all(|value| *value == N),
        "a re-push replayed from a regressed watermark must only surface the current \
             value, never a substituted one; got {regressed:?}"
    );
    assert!(
        regressed.len() <= 1,
        "re-push transfer must be bounded to the live row count, got {}",
        regressed.len()
    );

    // Prune during ongoing traffic loses nothing: churn more, compact, pull.
    for advertised_at in (N + 1)..=(N + 40) {
        crate::work_ledger::advertise_capability(
            &db,
            "node-a",
            "example-detector-cpu",
            &tags(),
            advertised_at,
        )
        .expect("advertise");
    }
    db.compact_currency_versions().expect("compact again");
    assert_eq!(
        capability_changes(&db, Lsn(0)),
        vec![N + 40],
        "after a second prune over new traffic the pull still converges to the latest"
    );
}

/// Acceptance against a real large debris ledger. Ignored by default
/// (needs the corpus file); run with the path in `DEBRIS_LEDGER_CORPUS`:
///   DEBRIS_LEDGER_CORPUS=/path/to/debris-ledger.db \
///     cargo test -p contextdb-engine --lib debris_corpus_acceptance -- --ignored --nocapture
/// Copies the corpus to a temp file first (never mutates the original).
#[test]
#[ignore = "requires the DEBRIS_LEDGER_CORPUS debris file"]
fn debris_corpus_acceptance_shrinks_debris_ledger() {
    let Ok(src) = std::env::var("DEBRIS_LEDGER_CORPUS") else {
        panic!("set DEBRIS_LEDGER_CORPUS to a copy-able debris ledger path");
    };
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("fabric-ledger.db");
    std::fs::copy(&src, &path).expect("copy corpus ledger");

    let bytes_before = std::fs::metadata(&path).unwrap().len();
    let db = Database::open(&path).expect("open corpus ledger");
    let versions_before = physical_versions(&db, "work_capabilities");
    println!(
        "debris_corpus before: file={} MB work_capabilities_versions={}",
        bytes_before / 1_048_576,
        versions_before
    );

    let report = db.compact_currency_versions().expect("compact corpus");
    let versions_after = physical_versions(&db, "work_capabilities");
    drop(db);
    let bytes_after = std::fs::metadata(&path).unwrap().len();
    println!(
        "debris_corpus after: file={} MB work_capabilities_versions={} pruned_versions={} \
             pruned_change_log_entries={} redb_compacted={}",
        bytes_after / 1_048_576,
        versions_after,
        report.pruned_versions,
        report.pruned_change_log_entries,
        report.redb_compacted
    );

    assert!(
        versions_after <= 64,
        "version count must be bounded to the live-row order of magnitude; got {versions_after}"
    );
    assert!(
        report.redb_compacted,
        "a debris ledger this fragmented must trigger a redb compaction"
    );
    assert!(
        bytes_after < 16 * 1_048_576,
        "the compacted debris ledger must drop to a few MB; got {} MB",
        bytes_after / 1_048_576
    );
}

#[test]
fn installing_the_ledger_starts_maintenance_and_it_auto_heals() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("fabric-ledger.db");
    let db = Database::open(&path).expect("open");
    // Fresh DB with no eligible tables yet: no thread.
    assert!(
        !db.__maintenance_thread_running(),
        "a database with no currency tables must not spawn a maintenance thread"
    );

    crate::work_ledger::install_work_ledger_schema(&db).expect("install");
    // Installing the capability registry starts the engine-owned thread.
    assert!(
        db.__maintenance_thread_running(),
        "installing the currency schema must start the maintenance thread in-session"
    );

    // Preserve the accumulated-version precondition before the measured
    // background work is released at the fast test cadence below.
    db.__set_currency_maintenance_interval(Duration::from_secs(3600));
    for advertised_at in 1..=200 {
        crate::work_ledger::advertise_capability(
            &db,
            "node-a",
            "example-detector-cpu",
            &tags(),
            advertised_at,
        )
        .expect("advertise");
    }
    assert_eq!(durable_physical_versions(&db, "work_capabilities"), 200);

    // Speed the cadence up and let the thread heal the accumulated versions.
    db.__set_currency_maintenance_interval(Duration::from_millis(40));
    // State-polled wait with sleep pacing: poll the thread's wake counter
    // and the healed version count under a generous failure ceiling (this
    // test's promise IS that a real background thread does real work, so
    // a ceiling remains by necessity — but success is decided by observed
    // STATE, never by how long we happened to sleep).
    let baseline_wakes = db.__maintenance_wakes();
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        if db.__maintenance_wakes() > baseline_wakes
            && durable_physical_versions(&db, "work_capabilities") == 1
        {
            break;
        }
        std::thread::yield_now();
    }
    assert_eq!(
        durable_physical_versions(&db, "work_capabilities"),
        1,
        "the background maintenance thread must auto-compact accumulated versions"
    );
    assert_eq!(current_advertised_at(&db), 200);
}

#[test]
fn non_eligible_database_never_spawns_a_maintenance_thread() {
    // A database without currency tables must get ZERO new threads,
    // on fresh open, after unrelated schema, and on reopen.
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("plain.db");
    {
        let db = Database::open(&path).expect("open");
        assert!(
            !db.__maintenance_thread_running(),
            "fresh non-eligible open"
        );
        db.execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .expect("create");
        db.execute(
            "INSERT INTO notes (id, body) VALUES (1, 'hi')",
            &HashMap::new(),
        )
        .expect("insert");
        assert!(
            !db.__maintenance_thread_running(),
            "an unrelated schema must not start a maintenance thread"
        );
    }
    // Reopen: still no eligible tables, still no thread.
    let db = Database::open(&path).expect("reopen");
    assert!(
        !db.__maintenance_thread_running(),
        "reopening a non-eligible database must not start a maintenance thread"
    );
}

#[test]
fn close_joins_the_maintenance_thread() {
    // Shutdown-aware: an eligible database's maintenance thread must join
    // on close (no zombie thread). Event/state recast: `close()` joins
    // synchronously, so its RETURN is the completion event and the
    // thread-gone state is the assertion; a genuinely hung join hangs the
    // test and is caught by the harness/mutants timeout (the failure
    // ceiling). The old `elapsed < 2s` wall-clock bound asserted only
    // promptness and flipped verdicts under load; the production clock
    // now wakes directly on shutdown.
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("fabric-ledger.db");
    let db = Database::open(&path).expect("open");
    crate::work_ledger::install_work_ledger_schema(&db).expect("install");
    assert!(db.__maintenance_thread_running());
    db.close().expect("close");
    assert!(
        !db.__maintenance_thread_running(),
        "close must join and clear the maintenance thread"
    );
}

#[test]
fn dropping_vector_only_database_wakes_and_joins_held_maintenance_worker() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE embeddings (id INTEGER PRIMARY KEY, embedding VECTOR(2))",
        &HashMap::new(),
    )
    .expect("create vector-only table");
    assert!(
        db.__maintenance_thread_running(),
        "a vector declaration must start engine-owned maintenance"
    );

    let worker_clock = db
        .__hold_engine_owned_vector_maintenance_clock_for_test()
        .expect("hold the engine-owned vector maintenance worker");
    worker_clock.wait_until_held_asleep();
    assert!(
        !worker_clock.worker_exited(),
        "the worker must still be asleep when automatic drop begins"
    );

    drop(db);

    assert!(
        worker_clock.worker_exited(),
        "automatic drop must wake and join the held maintenance worker"
    );
}
