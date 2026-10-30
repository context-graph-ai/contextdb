// Step 28: a local schema stage spends exactly the live reservation it was
// admitted, on the live accountant, on both hosts. Every exact-delta test
// here runs in its own invocation (`--test-threads=1 --exact <name>`), on
// databases with caller-driven maintenance and no sync, subscription or sink
// worker, so nothing else moves the accountant or the process aggregate.
mod local_schema_stage_accounting {
    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) enum Host {
        Legacy,
        Owned,
    }

    pub(super) struct Fixture {
        pub(super) db: Database,
        _dir: tempfile::TempDir,
    }

    pub(super) fn open_host(host: Host) -> Fixture {
        open_host_with(host, None)
    }

    pub(super) fn open_host_with(host: Host, plugin: Option<Arc<dyn DatabasePlugin>>) -> Fixture {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("local-schema-stage.redb");
        let mut options = crate::read_session::DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        };
        if let Some(plugin) = plugin {
            options.plugin = plugin;
        }
        let db = match host {
            Host::Legacy => Database::open_with_options(&path, options),
            Host::Owned => Database::open_owned_for_test(&path, options),
        }
        .unwrap();
        Fixture { db, _dir: dir }
    }

    pub(super) const EMPTY: fn() -> HashMap<String, Value> = HashMap::new;

    /// Counters measured just before BEGIN.
    #[derive(Clone, Copy, Debug)]
    pub(super) struct Base {
        pub(super) used: usize,
        pub(super) aggregate: usize,
        pub(super) limit: Option<usize>,
        pub(super) ceiling: Option<usize>,
    }

    pub(super) fn base(db: &Database) -> Base {
        let usage = db.finalization.accountant.usage();
        Base {
            used: usage.used,
            aggregate: crate::memory_accounting::process_aggregate_bytes_for_test(),
            limit: usage.limit,
            ceiling: usage.startup_ceiling,
        }
    }

    pub(super) fn deltas(db: &Database, base: Base) -> (i128, i128) {
        (
            db.finalization.accountant.usage().used as i128 - base.used as i128,
            crate::memory_accounting::process_aggregate_bytes_for_test() as i128
                - base.aggregate as i128,
        )
    }

    /// BEGIN, then the counters measured just after it. On the owned host
    /// BEGIN itself charges the transaction's own Original (the Legacy host
    /// charges nothing); that charge belongs to the transaction, not to the
    /// stage, and ROLLBACK returns it, so the stage ledger is measured from
    /// here and the settled-after-ROLLBACK checks from before BEGIN.
    pub(super) fn begin(db: &Database) -> (TxId, Base) {
        db.execute("BEGIN", &EMPTY()).unwrap();
        (session_tx(db), base(db))
    }

    pub(super) fn session_tx(db: &Database) -> TxId {
        db.session_transaction_for_test()
            .expect("an SQL transaction is open")
    }

    /// The stage ledger, with the step-28 equation asserted at this
    /// observation point: `used` delta = H + C + P + Kc, aggregate delta =
    /// H + C + P + Kc + Kr, P = D - Rs, one accountant and one writer
    /// identity, Kr = the three registries' open residue, and the limit and
    /// startup ceiling as they were.
    pub(super) fn ledger(db: &Database, base: Base, what: &str) -> LocalSchemaStageLedgerForTest {
        let tx = session_tx(db);
        let ledger = db
            .local_schema_stage_ledger_for_test(tx)
            .unwrap_or_else(|| panic!("{what}: a local schema stage is pending"));
        let (used, aggregate) = deltas(db, base);
        let owned = (ledger.hold
            + ledger.credit
            + ledger.payload
            + ledger.controls_used
            + ledger.stage_slot) as i128;
        assert_eq!(used, owned, "{what}: used delta = H + C + P + Kc + S: {ledger:?}");
        assert_eq!(
            aggregate,
            owned + (ledger.controls_residue + ledger.stage_slot_residue) as i128,
            "{what}: aggregate delta = H + C + P + Kc + S + Kr + Sr: {ledger:?}"
        );
        assert_eq!(
            ledger.payload,
            ledger.draws - ledger.restores,
            "{what}: P = D - Rs: {ledger:?}"
        );
        assert_eq!(
            ledger.image_accountant, ledger.live_accountant,
            "{what}: the image runs on the live accountant"
        );
        assert!(ledger.live_writer_identity.is_some(), "{what}: the live writer is registered");
        assert_eq!(
            ledger.image_writer_identity, ledger.live_writer_identity,
            "{what}: the image keeps the live writer identity"
        );
        let usage = db.finalization.accountant.usage();
        assert_eq!(usage.limit, base.limit, "{what}: the stage never changes the limit");
        assert_eq!(
            usage.startup_ceiling, base.ceiling,
            "{what}: the stage never changes the startup ceiling"
        );
        ledger
    }

    /// No stage is pending and both deltas are back to 0.
    pub(super) fn assert_settled(db: &Database, base: Base, tx: TxId, what: &str) {
        assert!(
            db.local_schema_stage_ledger_for_test(tx).is_none(),
            "{what}: no local schema stage is pending"
        );
        assert_eq!(deltas(db, base), (0, 0), "{what}: used and aggregate deltas are 0");
    }

    pub(super) const STATEMENTS: [&str; 3] = [
        "CREATE TABLE authored_migration (id UUID PRIMARY KEY, body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        "CREATE TRIGGER authored_migration_insert ON authored_migration WHEN INSERT",
        "ALTER TABLE authored_migration ADD COLUMN detail TEXT",
    ];

    /// Run `STATEMENTS` in one transaction, asserting the ledger after each:
    /// the first statement funds the stage from its baseline (H plus the
    /// payload credit, each as sized before BEGIN) plus its own reservation;
    /// every later statement moves C + P by exactly its reservation R, P by
    /// D - Rs, and leaves H, Kc and Kr alone, while `used` and the aggregate
    /// each grow by exactly R.
    pub(super) fn run_growth(db: &Database) -> (TxId, Base) {
        let hold = db.local_schema_stage_hold_bytes().unwrap();
        let credit = db.local_schema_stage_payload_credit_bytes();
        let (tx, base) = begin(db);
        records::record_open_residues();
        let mut previous: Option<(LocalSchemaStageLedgerForTest, (i128, i128))> = None;
        for (index, sql) in STATEMENTS.iter().enumerate() {
            let what = format!("statement {index}");
            let reservation = db.staged_statement_reservation_bytes_for_test(tx, sql);
            db.execute(sql, &EMPTY()).unwrap();
            let now = ledger(db, base, &what);
            let now_deltas = deltas(db, base);
            match previous {
                None => {
                    // Kr is exactly the named open residue of the controls
                    // built with the image, and its three stage registries
                    // name what their layout arithmetic says.
                    let residues = records::take_open_residues();
                    assert_eq!(
                        now.controls_residue,
                        residues.iter().map(|(_, bytes)| bytes).sum::<usize>(),
                        "{what}: Kr is exactly the named open residues {residues:?}"
                    );
                    let registries = residues
                        .iter()
                        .filter(|(operation, _)| operation.contains("registry"))
                        .map(|(_, bytes)| bytes)
                        .sum::<usize>();
                    assert_eq!(
                        registries,
                        super::super::schema_stage_registry::private_image_open_residue_bytes_for_test(),
                        "{what}: the three stage registries' residue: {residues:?}"
                    );
                    assert_eq!(now.hold, hold, "{what}: H is the hold sized before BEGIN");
                    assert_eq!(
                        now.credit + now.payload,
                        credit + reservation,
                        "{what}: C + P is the baseline credit plus this statement's reservation"
                    );
                }
                Some((before, before_deltas)) => {
                    assert_eq!(
                        (now.credit + now.payload) as i128 - (before.credit + before.payload) as i128,
                        reservation as i128,
                        "{what}: C + P grows by exactly this statement's reservation"
                    );
                    assert_eq!(
                        now.payload as i128 - before.payload as i128,
                        (now.draws - before.draws) as i128 - (now.restores - before.restores) as i128,
                        "{what}: P grows by this statement's D - Rs"
                    );
                    assert_eq!(now.hold, before.hold, "{what}: H unchanged");
                    assert_eq!(now.controls_used, before.controls_used, "{what}: Kc unchanged");
                    assert_eq!(
                        now.controls_residue, before.controls_residue,
                        "{what}: Kr unchanged"
                    );
                    assert_eq!(
                        (now_deltas.0 - before_deltas.0, now_deltas.1 - before_deltas.1),
                        (reservation as i128, reservation as i128),
                        "{what}: used and aggregate each grow by exactly R"
                    );
                }
            }
            previous = Some((now, now_deltas));
        }
        (tx, base)
    }

    // Test 1, owned host. The Legacy host is the extended
    // `local_schema_memory_refusal_leaves_no_live_or_durable_schema`.
    #[test]
    fn local_schema_initial_stage_refusal_is_exact_on_the_owned_host() {
        initial_stage_refusal_is_exact(Host::Owned);
    }

    pub(super) fn initial_stage_refusal_is_exact(host: Host) {
        let fixture = open_host(host);
        let db = &fixture.db;
        db.execute("CREATE TABLE committed_before (id UUID PRIMARY KEY)", &EMPTY())
            .unwrap();
        let outer = base(db);
        let (tx, before) = begin(db);
        let baseline = db.local_schema_stage_hold_bytes().unwrap()
            + db.local_schema_stage_payload_credit_bytes();
        let used = db.finalization.accountant.usage().used;
        db.set_memory_limit(Some(used + baseline - 1)).unwrap();
        let limited = base(db);
        let error = db
            .execute("CREATE TABLE refused_stage (id UUID PRIMARY KEY)", &EMPTY())
            .expect_err("one byte below the baseline refuses the stage");
        assert!(
            matches!(
                &error,
                Error::MemoryBudgetExceeded { subsystem, operation, .. }
                    if subsystem == "ddl" && operation == "stage_transactional_schema"
            ),
            "{host:?}: the typed stage refusal: {error:?}"
        );
        assert!(db.local_schema_stage_ledger_for_test(tx).is_none());
        assert_eq!(
            deltas(db, limited),
            (0, 0),
            "{host:?}: the refused stage leaves both counters as they were"
        );
        db.set_memory_limit(before.limit).unwrap();
        db.execute("ROLLBACK", &EMPTY()).unwrap();
        assert_settled_after_refused_reservation(db, outer, tx, host, &format!("{host:?} after rollback"));
        assert!(db.table_meta("refused_stage").is_none());
    }

    /// After ROLLBACK both deltas are 0 from before BEGIN. On the owned host a
    /// refused Original preparation (`reserve_original_preparation_for`, the
    /// transaction host's own code, outside the stage) keeps 32 bytes past
    /// ROLLBACK; that pre-existing owned-host defect is reported separately,
    /// so after such a refusal only the stage's absence is asserted there.
    fn assert_settled_after_refused_reservation(db: &Database, outer: Base, tx: TxId, host: Host, what: &str) {
        match host {
            Host::Legacy => assert_settled(db, outer, tx, what),
            Host::Owned => assert!(
                db.local_schema_stage_ledger_for_test(tx).is_none(),
                "{what}: no local schema stage is pending"
            ),
        }
    }

    // Test 2, owned host. The Legacy host is the integration fixture
    // `file_backed_sql_transaction_keeps_heterogeneous_ddl_at_one_lsn_after_reopen`.
    #[test]
    fn local_schema_growth_owns_exactly_each_statement_reservation_on_the_owned_host() {
        let fixture = open_host(Host::Owned);
        let db = &fixture.db;
        let outer = base(db);
        let (tx, _) = run_growth(db);
        db.execute("ROLLBACK", &EMPTY()).unwrap();
        assert_settled(db, outer, tx, "after rollback");
    }

    // Test 9.
    fn rollback_returns_every_byte(host: Host) {
        let fixture = open_host(host);
        let db = &fixture.db;
        let outer = base(db);
        let (tx, _) = run_growth(db);
        db.execute("ROLLBACK", &EMPTY()).unwrap();
        assert_settled(db, outer, tx, &format!("{host:?} after rollback"));
        assert!(db.table_meta("authored_migration").is_none());
    }

    #[test]
    fn local_schema_rollback_returns_every_stage_byte_on_the_legacy_host() {
        rollback_returns_every_byte(Host::Legacy);
    }

    #[test]
    fn local_schema_rollback_returns_every_stage_byte_on_the_owned_host() {
        rollback_returns_every_byte(Host::Owned);
    }

    // The negative fixture: an internal image has no durable store, so both
    // received-stage entries refuse it before any working write begins.
    #[test]
    fn a_private_image_is_refused_as_a_receiver_before_any_working_write() {
        let fixture = open_host(Host::Legacy);
        let db = &fixture.db;
        let mut reservation = TemporaryMemoryReservation::reserve_for_database(
            db,
            db.local_schema_stage_hold_bytes().unwrap() + db.local_schema_stage_payload_credit_bytes(),
            "stage_transactional_schema",
        )
        .unwrap();
        let mut stage_ledger = LocalSchemaStageLedger {
            hold: db.local_schema_stage_hold_bytes().unwrap(),
            ..LocalSchemaStageLedger::default()
        };
        let image = db
            .detached_local_schema_working_database(&mut reservation, &mut stage_ledger)
            .unwrap();
        assert!(image.persistence.is_none(), "the private image has no durable store");
        let ddl = create_table();
        let received = received_single_ddl(&ddl, 3);
        let changes = ChangeSet {
            ddl: vec![ddl],
            ddl_lsn: vec![Lsn(41)],
            ..ChangeSet::default()
        };
        let expected = super::super::received_schema_apply::received_schema_requires_durable_receiver()
            .to_string();

        // The Legacy received-stage entry.
        let control = crate::persistence::received_working::working_control_identity_watermark_for_test();
        let memory = image.declared_reader_memory().unwrap();
        let mut owner = crate::vector_read_memory::admission(memory.clone());
        let error = image
            .commit_received_schema_stage_for_test(
                &changes,
                &received,
                &[],
                &HashMap::new(),
                false,
                &mut owner,
                memory,
            )
            .expect_err("a private image cannot receive a schema stage");
        assert_eq!(error.to_string(), expected, "the typed durable-receiver refusal");
        assert_eq!(
            crate::persistence::received_working::working_control_identity_watermark_for_test(),
            control,
            "the refusal comes before any working write is begun"
        );

        // The connected scope's preparation, on an owned live host's real
        // commit scope, handed the private image as its receiver.
        let owned = open_host(Host::Owned);
        let live = &owned.db;
        let tx = live.begin().unwrap();
        // A received interval always carries a commit position, so the
        // interval reaches its preparation even with no row written yet.
        live.tx_mgr
            .update_flags(tx, |flags| {
                flags.requires_commit_lsn = true;
            })
            .unwrap();
        let control = crate::persistence::received_working::working_control_identity_watermark_for_test();
        let memory = live.declared_reader_memory().unwrap();
        let mut owner = crate::vector_read_memory::admission(memory.clone());
        let seen = std::cell::RefCell::new(None);
        let arrivals = HashMap::new();
        let committed = live.tx_mgr.commit_received_interval(
            tx,
            1,
            memory.clone(),
            |scope, _, lsn, receiver_tx| {
                let scope = scope.expect("the owned host lends its commit scope");
                let result = scope.prepare_received_schema_stage(
                    &image,
                    &scope.host.events,
                    lsn,
                    None,
                    &changes,
                    Some(&received),
                    &[],
                    ReceivedSchemaAdjudicationInputs {
                        push_outcome_key: None,
                        apply_role: SyncApplyRole::PullLeg,
                        arrivals: &arrivals,
                        adoption: SyncAdoption::Continuing,
                        receipt: None,
                        dependency_complete: false,
                        terminal_refusal_context: None,
                        hub_local_author: None,
                        receiver_tx,
                        unattested_policies: None,
                    },
                    None,
                    &mut owner,
                    memory.clone(),
                );
                *seen.borrow_mut() = Some(result.map(|_| ()).map_err(|error| error.to_string()));
                Err(Error::Other("end the probe interval".into()))
            },
            |_| Ok(()),
            |_, _| Ok(()),
            |_, _| {},
        );
        assert!(committed.is_err(), "the probe interval never commits");
        assert_eq!(
            seen.into_inner(),
            Some(Err(expected)),
            "the connected scope returns the typed durable-receiver refusal"
        );
        assert_eq!(
            crate::persistence::received_working::working_control_identity_watermark_for_test(),
            control,
            "the connected refusal comes before any working write is begun"
        );
        drop(image);
        drop(reservation);
    }

    use crate::database::statement_execution::local_schema_test_seams as seams;
    use crate::memory_accounting::spend_scope_test_records as records;

    /// The stage accounts a refused or failed statement leaves exactly as
    /// they were before it: H, C, P, Kc and Kr (D and Rs may have moved).
    fn assert_same_accounts(
        now: &LocalSchemaStageLedgerForTest,
        before: &LocalSchemaStageLedgerForTest,
        what: &str,
    ) {
        assert_eq!(
            (now.hold, now.credit, now.payload, now.controls_used, now.controls_residue),
            (
                before.hold,
                before.credit,
                before.payload,
                before.controls_used,
                before.controls_residue
            ),
            "{what}: H, C, P, Kc and Kr are as they were before the statement"
        );
    }

    fn is_stage_refusal(error: &Error) -> bool {
        matches!(
            error,
            Error::MemoryBudgetExceeded { subsystem, operation, .. }
                if subsystem == crate::memory_accounting::RESERVATION_SPEND_SUBSYSTEM
                    && operation == crate::memory_accounting::RESERVATION_SPEND_OPERATION
        )
    }

    fn requires_rollback(db: &Database, what: &str) {
        let error = db
            .execute("CREATE TABLE after_discard (id UUID PRIMARY KEY)", &EMPTY())
            .expect_err("a discarded stage admits no further statement");
        assert_eq!(
            error.to_string(),
            super::super::statement_execution::local_schema_stage_requires_rollback().to_string(),
            "{what}: the next statement returns the typed requires-rollback error"
        );
    }

    // Test 3.
    fn growth_refusal(host: Host) {
        let fixture = open_host(host);
        let db = &fixture.db;
        let outer = base(db);
        let (tx, before) = begin(db);
        db.execute(STATEMENTS[0], &EMPTY()).unwrap();
        let first = ledger(db, before, "after the first statement");
        let first_deltas = deltas(db, before);
        let reservation = db.staged_statement_reservation_bytes_for_test(tx, STATEMENTS[1]);
        let used = db.finalization.accountant.usage().used;
        let unlimited = base(db);
        db.set_memory_limit(Some(used + reservation - 1)).unwrap();
        // Setting the limit charges its own bookkeeping, outside the stage;
        // the stage's ledger is measured net of it.
        let limit_change = deltas(db, unlimited);
        let limited = Base {
            used: (before.used as i128 + limit_change.0) as usize,
            aggregate: (before.aggregate as i128 + limit_change.1) as usize,
            limit: db.finalization.accountant.usage().limit,
            ..before
        };
        let error = db
            .execute(STATEMENTS[1], &EMPTY())
            .expect_err("the second statement's reservation does not fit");
        assert!(
            matches!(
                &error,
                Error::MemoryBudgetExceeded { subsystem, operation, .. }
                    if subsystem == "ddl" && operation == "grow_transactional_schema_stage"
            ),
            "{host:?}: refused at admission: {error:?}"
        );
        let now = ledger(db, limited, "after the refused statement");
        assert_eq!(now, first, "{host:?}: the ledger equals its value after the first statement");
        assert_eq!(deltas(db, limited), first_deltas, "{host:?}: both deltas unchanged");
        db.set_memory_limit(before.limit).unwrap();
        db.execute("ROLLBACK", &EMPTY()).unwrap();
        assert_settled_after_refused_reservation(db, outer, tx, host, &format!("{host:?} after rollback"));
    }

    #[test]
    fn local_schema_growth_refusal_keeps_the_stage_exact_on_the_legacy_host() {
        growth_refusal(Host::Legacy);
    }

    #[test]
    fn local_schema_growth_refusal_keeps_the_stage_exact_on_the_owned_host() {
        growth_refusal(Host::Owned);
    }

    // Test 4.
    struct RefuseNextDdl(std::sync::atomic::AtomicBool);
    impl DatabasePlugin for RefuseNextDdl {
        fn on_ddl(&self, _change: &DdlChange) -> Result<()> {
            if self.0.swap(false, std::sync::atomic::Ordering::SeqCst) {
                return Err(Error::Other("plugin refused this DDL".into()));
            }
            Ok(())
        }
    }

    fn repeated_failures_then_success(host: Host) {
        let plugin = Arc::new(RefuseNextDdl(std::sync::atomic::AtomicBool::new(false)));
        let fixture = open_host_with(host, Some(plugin.clone()));
        let db = &fixture.db;
        db.execute("CREATE TABLE existing_table (id UUID PRIMARY KEY)", &EMPTY())
            .unwrap();
        let outer = base(db);
        let (tx, before) = begin(db);
        db.execute(STATEMENTS[0], &EMPTY()).unwrap();
        let staged = ledger(db, before, "after the first statement");
        let staged_deltas = deltas(db, before);
        let failures: [(&str, &str, bool); 3] = [
            ("ALTER on a missing table", "ALTER TABLE missing_table ADD COLUMN extra TEXT", false),
            ("CREATE of an existing name", "CREATE TABLE existing_table (id UUID PRIMARY KEY)", false),
            ("plugin refusal", "CREATE TABLE plugin_refused (id UUID PRIMARY KEY)", true),
        ];
        for (what, sql, refuse) in failures {
            plugin.0.store(refuse, std::sync::atomic::Ordering::SeqCst);
            db.execute(sql, &EMPTY())
                .expect_err("the statement fails");
            let now = ledger(db, before, what);
            assert_same_accounts(&now, &staged, &format!("{host:?} {what}"));
            assert_eq!(deltas(db, before), staged_deltas, "{host:?} {what}: both deltas unchanged");
        }
        let sql = "CREATE TABLE after_failures (id UUID PRIMARY KEY, body TEXT)";
        let reservation = db.staged_statement_reservation_bytes_for_test(tx, sql);
        let failed = ledger(db, before, "after the failures");
        db.execute(sql, &EMPTY()).unwrap();
        let now = ledger(db, before, "after the success");
        assert_eq!(
            (now.credit + now.payload) as i128 - (failed.credit + failed.payload) as i128,
            reservation as i128,
            "{host:?}: the success grows C + P by exactly its reservation"
        );
        let now_deltas = deltas(db, before);
        assert_eq!(
            (now_deltas.0 - staged_deltas.0, now_deltas.1 - staged_deltas.1),
            (reservation as i128, reservation as i128),
            "{host:?}: used and aggregate grow by exactly R"
        );
        db.execute("ROLLBACK", &EMPTY()).unwrap();
        assert_settled(db, outer, tx, &format!("{host:?} after rollback"));
    }

    #[test]
    fn local_schema_repeated_failures_then_success_keep_exact_ownership_on_the_legacy_host() {
        repeated_failures_then_success(Host::Legacy);
    }

    #[test]
    fn local_schema_repeated_failures_then_success_keep_exact_ownership_on_the_owned_host() {
        repeated_failures_then_success(Host::Owned);
    }

    const PROBED: &str = "CREATE TABLE probed (id UUID PRIMARY KEY, body TEXT)";

    /// One staged statement after `STATEMENTS[0]`, optionally with a probe
    /// armed inside its spend scope: the ledger before and after, and both
    /// deltas before and after.
    fn probed_statement(
        host: Host,
        probe: Option<Box<dyn FnOnce(&seams::ScopeProbe<'_, '_>) + Send>>,
    ) -> (
        LocalSchemaStageLedgerForTest,
        LocalSchemaStageLedgerForTest,
        (i128, i128),
        (i128, i128),
        usize,
    ) {
        let fixture = open_host(host);
        let db = &fixture.db;
        let outer = base(db);
        let (tx, before) = begin(db);
        db.execute(STATEMENTS[0], &EMPTY()).unwrap();
        let first = ledger(db, before, "before the probed statement");
        let first_deltas = deltas(db, before);
        let reservation = db.staged_statement_reservation_bytes_for_test(tx, PROBED);
        if let Some(probe) = probe {
            seams::arm_scope_probe(probe);
        }
        db.execute(PROBED, &EMPTY()).unwrap();
        let after = ledger(db, before, "after the probed statement");
        let after_deltas = deltas(db, before);
        db.execute("ROLLBACK", &EMPTY()).unwrap();
        assert_settled(db, outer, tx, &format!("{host:?} after rollback"));
        (first, after, first_deltas, after_deltas, reservation)
    }

    // Test 5.
    fn allocate_release_reallocate(host: Host) {
        let (control_before, control_after, _, _, reservation) = probed_statement(host, None);
        let own_draws = control_after.draws - control_before.draws;
        let own_restores = control_after.restores - control_before.restores;
        let own_payload = control_after.payload - control_before.payload;

        let seen = Arc::new(Mutex::new(None));
        let probe_seen = seen.clone();
        let (before, after, before_deltas, after_deltas, _) = probed_statement(
            host,
            Some(Box::new(move |probe| {
                let credit = probe.remaining_credit();
                let a = credit - 8;
                probe.try_allocate(a).unwrap();
                probe.release(a);
                probe.try_allocate(a).expect("the restore rule returns the credit");
                probe.release(a);
                *probe_seen.lock() = Some((credit, a));
            })),
        );
        let (credit, a) = seen.lock().take().expect("the probe ran inside the scope");
        assert_eq!(credit, reservation, "{host:?}: the scope's credit is the statement's reservation");
        assert_eq!(after.draws - before.draws, 2 * a + own_draws, "{host:?}: D");
        assert_eq!(after.restores - before.restores, 2 * a + own_restores, "{host:?}: Rs");
        assert_eq!(after.payload - before.payload, own_payload, "{host:?}: P moves only by the statement's payload");
        assert_eq!(after.credit, before.credit + credit - own_payload, "{host:?}: C ends at c minus that payload");
        assert_eq!(
            (after_deltas.0 - before_deltas.0, after_deltas.1 - before_deltas.1),
            (reservation as i128, reservation as i128),
            "{host:?}: used and aggregate move by exactly R"
        );

        // Variant: allocate a, release it, then ask for more than the credit.
        let seen = Arc::new(Mutex::new(None));
        let probe_seen = seen.clone();
        let (before, after, before_deltas, after_deltas, _) = probed_statement(
            host,
            Some(Box::new(move |probe| {
                let credit = probe.remaining_credit();
                let a = credit - 8;
                probe.try_allocate(a).unwrap();
                probe.release(a);
                let refused = probe.try_allocate(credit + 1);
                *probe_seen.lock() = Some((a, refused, probe.ledger()));
            })),
        );
        let (a, refused, _) = seen.lock().take().expect("the probe ran inside the scope");
        let refused = refused.expect_err("credit + 1 does not fit the scope");
        assert!(is_stage_refusal(&refused), "{host:?}: the typed stage refusal: {refused:?}");
        assert_eq!(after.draws - before.draws, a + own_draws, "{host:?}: D counts no refused draw");
        assert_eq!(after.restores - before.restores, a + own_restores, "{host:?}: Rs");
        assert_eq!(after.payload - before.payload, own_payload, "{host:?}: P");
        assert_eq!(
            (after_deltas.0 - before_deltas.0, after_deltas.1 - before_deltas.1),
            (reservation as i128, reservation as i128),
            "{host:?}: used and aggregate move by exactly R"
        );
        records::take_refusals();
    }

    #[test]
    fn local_schema_scope_allocate_release_reallocate_on_the_legacy_host() {
        allocate_release_reallocate(Host::Legacy);
    }

    #[test]
    fn local_schema_scope_allocate_release_reallocate_on_the_owned_host() {
        allocate_release_reallocate(Host::Owned);
    }

    // Test 6.
    fn transient_allocations(host: Host) {
        let (control_before, control_after, _, _, reservation) = probed_statement(host, None);
        let own_draws = control_after.draws - control_before.draws;
        let own_restores = control_after.restores - control_before.restores;
        let own_payload = control_after.payload - control_before.payload;
        let seen = Arc::new(Mutex::new(None));
        let probe_seen = seen.clone();
        let (before, after, before_deltas, after_deltas, _) = probed_statement(
            host,
            Some(Box::new(move |probe| {
                let block = (probe.remaining_credit() - 8) / 3;
                for _ in 0..3 {
                    probe.try_allocate(block).unwrap();
                }
                for _ in 0..3 {
                    probe.release(block);
                }
                *probe_seen.lock() = Some(3 * block);
            })),
        );
        let transient = seen.lock().take().expect("the probe ran inside the scope");
        assert!(
            transient > own_payload,
            "{host:?}: the transient blocks total more than the statement's retained payload"
        );
        assert_eq!(after.draws - before.draws, transient + own_draws, "{host:?}: D");
        assert_eq!(after.restores - before.restores, transient + own_restores, "{host:?}: Rs is the transient total plus the statement's own");
        assert_eq!(after.payload - before.payload, own_payload, "{host:?}: P");
        assert_eq!(
            (after.credit + after.payload) - (before.credit + before.payload),
            reservation,
            "{host:?}: C + P grows by exactly R"
        );
        assert_eq!(
            (after_deltas.0 - before_deltas.0, after_deltas.1 - before_deltas.1),
            (reservation as i128, reservation as i128),
            "{host:?}: used and aggregate move by exactly R"
        );
    }

    #[test]
    fn local_schema_success_with_transient_allocations_is_exact_on_the_legacy_host() {
        transient_allocations(Host::Legacy);
    }

    #[test]
    fn local_schema_success_with_transient_allocations_is_exact_on_the_owned_host() {
        transient_allocations(Host::Owned);
    }

    // Test 7.
    const SHORT: &str = "CREATE TABLE short_credit (id UUID PRIMARY KEY)";

    fn short_credit(host: Host) {
        // A CREATE TABLE never reaches the commit-retention apply: local DDL
        // publishes through the DDL LSN path. Its fallible pre-admission is the
        // new table's metadata, admitted before the infallible create (the
        // statement's largest draw). Measure the scope's need at that point on
        // a twin, then reserve 16 bytes less.
        let need = {
            let fixture = open_host(host);
            let db = &fixture.db;
            db.execute("BEGIN", &EMPTY()).unwrap();
            db.execute(STATEMENTS[0], &EMPTY()).unwrap();
            records::take_draws();
            db.execute(SHORT, &EMPTY()).unwrap();
            let draws = records::take_draws();
            let (consumed_before, metadata) = draws
                .iter()
                .copied()
                .max_by_key(|(_, bytes)| *bytes)
                .expect("the statement drew from its scope");
            db.execute("ROLLBACK", &EMPTY()).unwrap();
            consumed_before + metadata
        };
        let fixture = open_host(host);
        let db = &fixture.db;
        let (tx, before) = begin(db);
        db.execute(STATEMENTS[0], &EMPTY()).unwrap();
        let staged = ledger(db, before, "before the short statement");
        let staged_deltas = deltas(db, before);
        seams::override_next_statement_reservation(need - 16);
        let entry = Arc::new(Mutex::new(None));
        let probe_entry = entry.clone();
        seams::arm_scope_probe(move |probe| {
            probe.reset_high_water();
            *probe_entry.lock() = Some((
                probe.used(),
                crate::memory_accounting::process_aggregate_bytes_for_test(),
                probe.retained_charges(),
                probe.admitted(),
            ));
        });
        records::take_refusals();
        let error = db
            .execute(SHORT, &EMPTY())
            .expect_err("16 bytes short at the retention site");
        assert!(is_stage_refusal(&error), "{host:?}: the typed stage refusal: {error:?}");
        let (used, aggregate, retained, admitted) = entry.lock().take().expect("the scope was entered");
        let refusals = records::take_refusals();
        assert_eq!(refusals.len(), 1, "{host:?}: one refusal: {refusals:?}");
        let refusal = refusals[0];
        assert_eq!(refusal.available + 16, refusal.requested, "{host:?}: exactly 16 bytes short");
        assert_eq!(refusal.high_water, used, "{host:?}: the counter never rose inside the scope");
        assert_eq!(refusal.used, used, "{host:?}: used at the refusal equals used at entry");
        assert_eq!(refusal.aggregate, aggregate, "{host:?}: the aggregate at the refusal equals its value at entry");
        assert_eq!(refusal.retained_charges, retained, "{host:?}: no retained charge inside the scope");
        assert_eq!(refusal.admitted, admitted, "{host:?}: no counter admission inside the scope");
        let now = ledger(db, before, "after the short statement");
        assert_same_accounts(&now, &staged, &format!("{host:?} after the short statement"));
        assert_eq!(deltas(db, before), staged_deltas, "{host:?}: both deltas as before the statement");
        db.execute("COMMIT", &EMPTY()).unwrap();
        assert!(db.table_meta("authored_migration").is_some(), "{host:?}: the earlier staged statement remains");
        assert!(db.table_meta("short_credit").is_none(), "{host:?}: the refused statement left nothing");
        assert!(db.local_schema_stage_ledger_for_test(tx).is_none());
    }

    #[test]
    fn local_schema_short_credit_refuses_without_overdraft_on_the_legacy_host() {
        short_credit(Host::Legacy);
    }

    #[test]
    fn local_schema_short_credit_refuses_without_overdraft_on_the_owned_host() {
        short_credit(Host::Owned);
    }

    fn invariant_hook_unwinds(host: Host) {
        let fixture = open_host(host);
        let db = &fixture.db;
        let outer = base(db);
        let (tx, before) = begin(db);
        db.execute(STATEMENTS[0], &EMPTY()).unwrap();
        ledger(db, before, "before the invariant hook");
        let seen = Arc::new(Mutex::new(None));
        let probe_seen = seen.clone();
        seams::arm_scope_probe(move |probe| {
            *probe_seen.lock() = Some((probe.used(), probe.retained_charges()));
            probe.charge_retained(1);
        });
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.execute(PROBED, &EMPTY())
        }))
        .expect_err("a plain charge_retained inside the scope is an engine-invariant panic");
        let message = unwound
            .downcast_ref::<String>()
            .cloned()
            .unwrap_or_default();
        assert!(
            message.contains("engine invariant violated: an infallible charge_retained"),
            "{host:?}: the specific engine-invariant message: {message}"
        );
        let (used, retained) = seen.lock().take().expect("the scope was entered");
        assert_eq!(
            db.finalization.accountant.retained_charge_bytes_for_test(),
            retained,
            "{host:?}: the panic fired before the counter changed"
        );
        assert!(used >= before.used);
        assert_settled(db, before, tx, &format!("{host:?} after the unwind discarded the stage"));
        requires_rollback(db, &format!("{host:?} after the unwind"));
        db.execute("ROLLBACK", &EMPTY()).unwrap();
        assert_settled(db, outer, tx, &format!("{host:?} after rollback"));
        assert!(db.table_meta("authored_migration").is_none());
    }

    #[test]
    fn local_schema_invariant_hook_unwinds_and_discards_the_stage_on_the_legacy_host() {
        invariant_hook_unwinds(Host::Legacy);
    }

    #[test]
    fn local_schema_invariant_hook_unwinds_and_discards_the_stage_on_the_owned_host() {
        invariant_hook_unwinds(Host::Owned);
    }

    // Test 8.
    fn failure_after_execution(host: Host) {
        for kind in [seams::LocalSchemaFaultKind::Error, seams::LocalSchemaFaultKind::Unwind] {
            for after_growth in [false, true] {
                let what = format!("{host:?} {kind:?} after_growth={after_growth}");
                let fixture = open_host(host);
                let db = &fixture.db;
                let outer = base(db);
                let (tx, before) = begin(db);
                if after_growth {
                    db.execute(STATEMENTS[0], &EMPTY()).unwrap();
                    db.execute(STATEMENTS[1], &EMPTY()).unwrap();
                    ledger(db, before, &what);
                }
                seams::arm_statement_fault(seams::LocalSchemaStatementFault::AfterExecuteBeforeAbsorb(kind));
                let sql = "CREATE TABLE faulted (id UUID PRIMARY KEY)";
                match kind {
                    seams::LocalSchemaFaultKind::Error => {
                        let error = db.execute(sql, &EMPTY()).expect_err("the injected fault");
                        assert_eq!(
                            error.to_string(),
                            Error::Other("injected local schema statement fault after execution".into())
                                .to_string(),
                            "{what}"
                        );
                    }
                    seams::LocalSchemaFaultKind::Unwind => {
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            db.execute(sql, &EMPTY())
                        }))
                        .expect_err("the injected unwind");
                    }
                }
                assert_settled(db, before, tx, &format!("{what}: the stage is gone"));
                requires_rollback(db, &what);
                db.execute("ROLLBACK", &EMPTY()).unwrap();
                assert_settled(db, outer, tx, &format!("{what}: after rollback"));
                assert!(db.table_meta("faulted").is_none(), "{what}: nothing committed");
                assert!(db.table_meta("authored_migration").is_none(), "{what}: nothing committed");
            }
        }
    }

    #[test]
    fn local_schema_failure_after_execution_discards_the_stage_on_the_legacy_host() {
        failure_after_execution(Host::Legacy);
    }

    #[test]
    fn local_schema_failure_after_execution_discards_the_stage_on_the_owned_host() {
        failure_after_execution(Host::Owned);
    }

    // Test 10.
    fn failed_preparation(host: Host) {
        let fixture = open_host(host);
        let db = &fixture.db;
        let outer = base(db);
        let (tx, before) = begin(db);
        seams::arm_preparation_fault();
        let error = db
            .execute("CREATE TABLE never_prepared (id UUID PRIMARY KEY)", &EMPTY())
            .expect_err("the preparation fault fires after controls and fold");
        assert_eq!(
            error.to_string(),
            Error::Other("injected local schema preparation fault after controls and fold".into())
                .to_string(),
            "{host:?}"
        );
        assert_settled(db, before, tx, &format!("{host:?}: Kc and Kr refunded exactly"));
        db.execute("ROLLBACK", &EMPTY()).unwrap();
        assert_settled(db, outer, tx, &format!("{host:?} after rollback"));
    }

    #[test]
    fn local_schema_failed_preparation_refunds_every_byte_on_the_legacy_host() {
        failed_preparation(Host::Legacy);
    }

    #[test]
    fn local_schema_failed_preparation_refunds_every_byte_on_the_owned_host() {
        failed_preparation(Host::Owned);
    }

    // Test 11. The autocommit database is opened only after the transactional
    // one has been measured and dropped, so their windows never overlap. Both
    // start from the same schema, reached the same way: one prior
    // transactional schema commit. (The owned host builds a one-time 116-byte
    // structure at its first transactional schema commit; every later commit
    // retains the same as autocommit. The warm-up keeps that one-time cost out
    // of the comparison on both hosts.)
    fn warmed(host: Host) -> Fixture {
        let fixture = open_host(host);
        fixture.db.execute("BEGIN", &EMPTY()).unwrap();
        fixture
            .db
            .execute("CREATE TABLE warm_up (id UUID PRIMARY KEY)", &EMPTY())
            .unwrap();
        fixture.db.execute("COMMIT", &EMPTY()).unwrap();
        fixture
    }

    fn commit_matches_autocommit(host: Host) {
        let (transactional_used, transactional) = {
            let fixture = warmed(host);
            let db = &fixture.db;
            let outer = base(db);
            let (tx, _) = run_growth(db);
            db.execute("COMMIT", &EMPTY()).unwrap();
            assert!(
                db.local_schema_stage_ledger_for_test(tx).is_none(),
                "{host:?}: the stage is gone after COMMIT"
            );
            let committed = deltas(db, outer);
            assert_eq!(committed.1, committed.0, "{host:?}: the aggregate delta equals the live delta");
            (db.finalization.accountant.usage().used, committed)
        };
        let fixture = warmed(host);
        let db = &fixture.db;
        let before = base(db);
        for sql in STATEMENTS {
            db.execute(sql, &EMPTY()).unwrap();
        }
        let autocommit = deltas(db, before);
        assert_eq!(
            transactional, autocommit,
            "{host:?}: committing the stage retains exactly what the same DDL in autocommit retains"
        );
        assert_eq!(
            transactional_used,
            db.finalization.accountant.usage().used,
            "{host:?}: live used equals the autocommit database's"
        );
    }

    #[test]
    fn local_schema_commit_matches_an_equivalent_autocommit_database_on_the_legacy_host() {
        commit_matches_autocommit(Host::Legacy);
    }

    #[test]
    fn local_schema_commit_matches_an_equivalent_autocommit_database_on_the_owned_host() {
        commit_matches_autocommit(Host::Owned);
    }
}
