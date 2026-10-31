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
        assert_settled(db, outer, tx, &format!("{host:?} after rollback"));
        assert!(db.table_meta("refused_stage").is_none());
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
        assert_settled(db, outer, tx, &format!("{host:?} after rollback"));
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

    /// A committed schema with automatic, declared and foreign-key indexes,
    /// so the image's metadata fold is nonzero.
    const POPULATED: [&str; 4] = [
        "CREATE TABLE parents (id UUID PRIMARY KEY, name TEXT UNIQUE)",
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id), body TEXT)",
        "CREATE INDEX children_body ON children (body)",
        "CREATE TABLE notes (id UUID PRIMARY KEY, a TEXT, b TEXT, UNIQUE (a, b))",
    ];

    /// The fold the image draws for the live schema: each table's
    /// `estimated_bytes`, since the image store is not disk-backed.
    fn image_fold_bytes(db: &Database) -> usize {
        db.relational_store
            .table_meta
            .read()
            .values()
            .map(|meta| meta.estimated_bytes())
            .sum()
    }

    // A refusal at the two preparation boundaries the stage crosses before
    // its image holds the fold: the image's reader setup, which now runs
    // before any stage credit moves, and the point just after the fold is
    // drawn and before the tables hold it. Each returns the reader refusal,
    // leaves no stage, and both counters exactly as they were after BEGIN;
    // the transaction then stages a statement normally.
    fn preparation_boundary_refusal(host: Host, at: seams::PreparationFault) {
        let fixture = open_host(host);
        let db = &fixture.db;
        for sql in POPULATED {
            db.execute(sql, &EMPTY()).unwrap();
        }
        let fold = image_fold_bytes(db);
        assert!(fold > 0, "{host:?}: the populated schema has a metadata fold");
        let outer = base(db);
        let (tx, before) = begin(db);
        records::take_draws();
        seams::arm_preparation_fault_at(at);
        let error = db
            .execute("CREATE TABLE never_prepared (id UUID PRIMARY KEY)", &EMPTY())
            .expect_err("the armed boundary refuses the preparation");
        assert_eq!(
            error.to_string(),
            seams::injected_reader_refusal().to_string(),
            "{host:?} {at:?}: the reader refusal"
        );
        let draws = records::take_draws();
        match at {
            seams::PreparationFault::ReaderSetup => assert!(
                draws.is_empty(),
                "{host:?}: no stage credit moved before the reader setup: {draws:?}"
            ),
            seams::PreparationFault::AfterFoldBeforeTables => assert_eq!(
                draws,
                vec![(0, fold)],
                "{host:?}: exactly the fold was drawn before the refusal"
            ),
            seams::PreparationFault::AfterRegistration => unreachable!(),
        }
        assert_settled(db, before, tx, &format!("{host:?} {at:?}: every admitted byte returned"));
        assert!(db.table_meta("never_prepared").is_none());
        // The transaction is still usable: the next statement stages a fresh
        // image with the full fold and the ledger equation holds.
        db.execute(STATEMENTS[0], &EMPTY()).unwrap();
        let staged = ledger(db, before, "after the refused preparation");
        assert!(staged.payload >= fold, "{host:?}: the new image holds the fold");
        db.execute("ROLLBACK", &EMPTY()).unwrap();
        assert_settled(db, outer, tx, &format!("{host:?} {at:?} after rollback"));
        assert!(db.table_meta("authored_migration").is_none());
    }

    #[test]
    fn local_schema_reader_setup_refusal_refunds_every_byte_on_the_legacy_host() {
        preparation_boundary_refusal(Host::Legacy, seams::PreparationFault::ReaderSetup);
    }

    #[test]
    fn local_schema_reader_setup_refusal_refunds_every_byte_on_the_owned_host() {
        preparation_boundary_refusal(Host::Owned, seams::PreparationFault::ReaderSetup);
    }

    #[test]
    fn local_schema_refusal_after_the_fold_returns_it_on_the_legacy_host() {
        preparation_boundary_refusal(Host::Legacy, seams::PreparationFault::AfterFoldBeforeTables);
    }

    #[test]
    fn local_schema_refusal_after_the_fold_returns_it_on_the_owned_host() {
        preparation_boundary_refusal(Host::Owned, seams::PreparationFault::AfterFoldBeforeTables);
    }

    /// The owned host's commit-event stage map: its live stages, capacity,
    /// held bytes and the plan price of that capacity. The Legacy host has no
    /// such map.
    fn commit_event_stage_map(db: &Database) -> (usize, usize, usize, usize) {
        match &db.tx_mgr {
            DatabaseTransactionHost::Owned(handle) => {
                handle.with_host(|host| host.events.stage_map_for_test())
            }
            DatabaseTransactionHost::Legacy(_) => (0, 0, 0, 0),
        }
    }

    // Test 11, warmed. The autocommit database is opened only after the
    // transactional one has been measured and dropped, so their windows never
    // overlap. Both start from the same schema, reached the same way: one
    // prior transactional schema commit, which builds the owned host's one
    // retained first-commit owner (the cold test below prices it). On that
    // common state a later commit retains exactly what autocommit retains.
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

    const REPEATED: [&str; 3] = [
        "CREATE TABLE repeated_0 (id UUID PRIMARY KEY, body TEXT)",
        "CREATE TABLE repeated_1 (id UUID PRIMARY KEY, body TEXT)",
        "CREATE TABLE repeated_2 (id UUID PRIMARY KEY, body TEXT)",
    ];

    // Test 11, cold. The first transactional schema commit on a fresh
    // database retains exactly what the same DDL in autocommit retains, plus
    // one named owner: on the owned host, the commit-event stage map's first
    // capacity. The map is created empty, and the first ordinary owned commit,
    // which autocommit DDL never takes, grows it to hold a stage and keeps that
    // capacity (the smallest map the plan builds). Legacy has no such map, so
    // its equation has no extra term.
    // Three more transactional commits then each retain exactly what the same
    // DDL retains in autocommit, and the map neither grows nor keeps a stage,
    // so the owner is paid once and never accumulates.
    fn cold_commit_matches_autocommit(host: Host) {
        let (transactional_used, transactional, owner, repeated) = {
            let fixture = open_host(host);
            let db = &fixture.db;
            let outer = base(db);
            assert_eq!(
                commit_event_stage_map(db),
                (0, 0, 0, 0),
                "{host:?}: the stage map holds nothing before the first ordinary commit"
            );
            let (tx, _) = run_growth(db);
            db.execute("COMMIT", &EMPTY()).unwrap();
            assert!(db.local_schema_stage_ledger_for_test(tx).is_none());
            let committed = deltas(db, outer);
            let committed_used = db.finalization.accountant.usage().used;
            assert_eq!(committed.1, committed.0, "{host:?}: the aggregate delta equals the live delta");
            let (stages, capacity, held, priced) = commit_event_stage_map(db);
            assert_eq!(stages, 0, "{host:?}: the committed stage left the map");
            assert_eq!(held, priced, "{host:?}: the map holds exactly its capacity's price");
            match host {
                Host::Legacy => assert_eq!(capacity, 0, "Legacy has no commit-event stage map"),
                Host::Owned => {
                    assert!(capacity >= 1, "the first commit grows the map to hold one stage");
                    assert!(held > 0, "that capacity is a priced owner");
                }
            }
            let mut repeated = Vec::new();
            for sql in REPEATED {
                let before = base(db);
                db.execute("BEGIN", &EMPTY()).unwrap();
                db.execute(sql, &EMPTY()).unwrap();
                db.execute("COMMIT", &EMPTY()).unwrap();
                repeated.push(deltas(db, before));
                assert_eq!(
                    commit_event_stage_map(db),
                    (0, capacity, held, priced),
                    "{host:?}: a later commit neither grows the map nor leaves a stage in it"
                );
            }
            (committed_used, committed, held, repeated)
        };
        let fixture = open_host(host);
        let db = &fixture.db;
        let before = base(db);
        for sql in STATEMENTS {
            db.execute(sql, &EMPTY()).unwrap();
        }
        let autocommit = deltas(db, before);
        assert_eq!(
            transactional,
            (autocommit.0 + owner as i128, autocommit.1 + owner as i128),
            "{host:?}: the first transactional commit = autocommit + the stage map's first capacity ({owner} bytes)"
        );
        assert_eq!(
            transactional_used,
            db.finalization.accountant.usage().used + owner,
            "{host:?}: live used = the autocommit database's + the stage map's first capacity"
        );
        for (index, sql) in REPEATED.iter().enumerate() {
            let before = base(db);
            db.execute(sql, &EMPTY()).unwrap();
            assert_eq!(
                repeated[index],
                deltas(db, before),
                "{host:?}: repeated commit {index} retains exactly what autocommit retains"
            );
        }
    }

    #[test]
    fn local_schema_first_commit_matches_autocommit_plus_its_named_owner_on_the_legacy_host() {
        cold_commit_matches_autocommit(Host::Legacy);
    }

    #[test]
    fn local_schema_first_commit_matches_autocommit_plus_its_named_owner_on_the_owned_host() {
        cold_commit_matches_autocommit(Host::Owned);
    }

    #[test]
    fn local_schema_commit_matches_an_equivalent_autocommit_database_on_the_legacy_host() {
        commit_matches_autocommit(Host::Legacy);
    }

    #[test]
    fn local_schema_commit_matches_an_equivalent_autocommit_database_on_the_owned_host() {
        commit_matches_autocommit(Host::Owned);
    }

    // Refund provenance. Inside a real statement's spend scope, a control the
    // live counter paid before the statement and a control the scope pays are
    // dropped side by side on the same counter. Each refund goes to the owner
    // that paid it: the first back to the live counter (used and the process
    // aggregate fall by its bytes; the stage credit and ledger do not move),
    // the second back to the stage (its credit rises and its payload falls;
    // used and the aggregate do not move).
    fn mixed_owner_refunds(host: Host) {
        const PRE_FUNDED: usize = 3000;
        const SCOPE_FUNDED: usize = 200;
        let fixture = open_host(host);
        let db = &fixture.db;
        let outer = base(db);
        let (tx, before) = begin(db);
        db.execute(STATEMENTS[0], &EMPTY()).unwrap();
        ledger(db, before, "before the mixed-owner statement");
        let staged = deltas(db, before);
        let pre_funded = crate::memory_accounting::ControlCredit::admit(
            &db.finalization.accountant,
            PRE_FUNDED,
            "mixed_owner_pre_funded",
        )
        .unwrap();
        let with_pre = deltas(db, before);
        assert_eq!(
            (with_pre.0 - staged.0, with_pre.1 - staged.1),
            (PRE_FUNDED as i128, PRE_FUNDED as i128),
            "{host:?}: the live counter paid the pre-funded control"
        );
        let seen = Arc::new(Mutex::new(None));
        let probe_seen = seen.clone();
        seams::arm_scope_probe(move |probe| {
            let observe = |probe: &seams::ScopeProbe<'_, '_>| {
                (
                    probe.used(),
                    crate::memory_accounting::process_aggregate_bytes_for_test(),
                    probe.remaining_credit(),
                    probe.ledger(),
                )
            };
            let entry = observe(probe);
            let scope_funded = crate::memory_accounting::ControlCredit::admit(
                probe.accountant(),
                SCOPE_FUNDED,
                "mixed_owner_scope_funded",
            )
            .expect("the scope pays the second control");
            let admitted = observe(probe);
            drop(pre_funded);
            let after_pre = observe(probe);
            drop(scope_funded);
            let after_scoped = observe(probe);
            *probe_seen.lock() = Some((entry, admitted, after_pre, after_scoped));
        });
        db.execute(PROBED, &EMPTY()).unwrap();
        let (entry, admitted, after_pre, after_scoped) =
            seen.lock().take().expect("the probe ran inside the scope");
        // The scope paid the second control out of its credit.
        assert_eq!((admitted.0, admitted.1), (entry.0, entry.1), "{host:?}: the scope's control charged neither counter");
        assert_eq!(admitted.2, entry.2 - SCOPE_FUNDED, "{host:?}: the scope's credit paid it");
        assert_eq!(admitted.3.payload, entry.3.payload + SCOPE_FUNDED, "{host:?}: it is stage payload");
        // The pre-funded control's refund goes to the live counter only.
        assert_eq!(after_pre.0, admitted.0 - PRE_FUNDED, "{host:?}: used falls by the pre-funded control");
        assert_eq!(after_pre.1, admitted.1 - PRE_FUNDED, "{host:?}: the aggregate falls by it");
        assert_eq!(after_pre.2, admitted.2, "{host:?}: the stage credit does not take it");
        assert_eq!(after_pre.3, admitted.3, "{host:?}: the stage ledger does not move");
        // The scope-funded control's refund goes to the stage only.
        assert_eq!((after_scoped.0, after_scoped.1), (after_pre.0, after_pre.1), "{host:?}: neither counter takes the stage's refund");
        assert_eq!(after_scoped.2, after_pre.2 + SCOPE_FUNDED, "{host:?}: the stage credit takes it back");
        assert_eq!(after_scoped.3.payload, after_pre.3.payload - SCOPE_FUNDED, "{host:?}: P falls by it");
        assert_eq!(after_scoped.3.restores, after_pre.3.restores + SCOPE_FUNDED, "{host:?}: Rs rises by it");
        assert_eq!(after_scoped.3.draws, after_pre.3.draws, "{host:?}: D does not move");
        // With the pre-funded control gone, the stage equation holds again.
        ledger(db, before, "after the mixed-owner statement");
        db.execute("ROLLBACK", &EMPTY()).unwrap();
        assert_settled(db, outer, tx, &format!("{host:?} after rollback"));
    }

    #[test]
    fn local_schema_refunds_return_to_the_owner_that_paid_on_the_legacy_host() {
        mixed_owner_refunds(Host::Legacy);
    }

    #[test]
    fn local_schema_refunds_return_to_the_owner_that_paid_on_the_owned_host() {
        mixed_owner_refunds(Host::Owned);
    }

    /// Statements staged before the one a late shortage refuses, the refused
    /// statement, and a later statement that must still succeed.
    const LATE_CASES: [(&str, &[&str]); 6] = [
        (
            "CREATE TABLE late_target (id UUID PRIMARY KEY, a TEXT, b TEXT, UNIQUE (a, b))",
            &["CREATE TABLE late_prior (id UUID PRIMARY KEY)"],
        ),
        (STATEMENTS[1], &[STATEMENTS[0]]),
        (STATEMENTS[2], &[STATEMENTS[0], STATEMENTS[1]]),
        ("CREATE INDEX authored_body ON authored_migration (body)", &[STATEMENTS[0]]),
        (
            "CREATE TABLE late_vectors (id UUID PRIMARY KEY, embedding VECTOR(4))",
            &["CREATE TABLE late_prior (id UUID PRIMARY KEY)"],
        ),
        ("ALTER TABLE authored_migration ADD COLUMN embedding VECTOR(4)", &[STATEMENTS[0]]),
    ];
    const LATER: &str = "CREATE TABLE later_success (id UUID PRIMARY KEY, body TEXT)";

    /// The committed schema of a database: every table's metadata and every
    /// trigger declaration.
    fn committed_schema(
        db: &Database,
    ) -> (
        std::collections::BTreeMap<String, TableMeta>,
        BTreeMap<String, TriggerDeclaration>,
    ) {
        (
            db.relational_store
                .table_meta
                .read()
                .iter()
                .map(|(name, meta)| (name.clone(), meta.clone()))
                .collect(),
            db.trigger.declarations.lock().clone(),
        )
    }

    // Statement-level atomicity under a late shortage. For each statement, a
    // twin measures every draw its scope makes; then, for every draw that
    // raises the statement's peak need after its first draw (so after its
    // metadata or first preparation was admitted), a fresh transaction stages
    // the earlier statements and runs the statement with a reservation one
    // byte short of that peak. The statement returns the typed stage refusal
    // at exactly that draw. It undoes only its own work: the stage ledger,
    // both counters and the image (payload, DDL history, tables, triggers and
    // event-bus projection) are exactly as before it, and the earlier
    // statements stay staged. A later statement then succeeds. After COMMIT
    // the durable schema equals that of a twin that never ran the refused
    // statement; after ROLLBACK nothing is durable and both counters are back
    // to their values before BEGIN.
    fn late_shortage_is_statement_atomic(host: Host) {
        for (sql, prior) in LATE_CASES {
            let draws = {
                let fixture = open_host(host);
                let db = &fixture.db;
                db.execute("BEGIN", &EMPTY()).unwrap();
                for statement in prior {
                    db.execute(statement, &EMPTY()).unwrap();
                }
                records::take_draws();
                db.execute(sql, &EMPTY()).unwrap();
                let draws = records::take_draws();
                db.execute("ROLLBACK", &EMPTY()).unwrap();
                draws
            };
            let mut peak = 0usize;
            let mut points = Vec::new();
            for (index, (consumed, bytes)) in draws.iter().copied().enumerate() {
                let need = consumed + bytes;
                if need > peak {
                    points.push((index, need, bytes));
                    peak = need;
                }
            }
            assert!(
                points.len() >= 2,
                "{host:?} {sql}: the statement draws again after its first draw: {draws:?}"
            );
            let expected = {
                let fixture = open_host(host);
                let db = &fixture.db;
                db.execute("BEGIN", &EMPTY()).unwrap();
                for statement in prior {
                    db.execute(statement, &EMPTY()).unwrap();
                }
                db.execute(LATER, &EMPTY()).unwrap();
                db.execute("COMMIT", &EMPTY()).unwrap();
                committed_schema(db)
            };
            for (index, need, bytes) in points.iter().copied().skip(1) {
                for commit in [true, false] {
                    let what = format!("{host:?} {sql} refused at draw {index} commit={commit}");
                    let fixture = open_host(host);
                    let db = &fixture.db;
                    let schema_before = committed_schema(db);
                    let outer = base(db);
                    let (tx, before) = begin(db);
                    for statement in prior {
                        db.execute(statement, &EMPTY()).unwrap();
                    }
                    let staged = ledger(db, before, &what);
                    let staged_deltas = deltas(db, before);
                    let image = db.local_schema_image_shape_for_test(tx);
                    assert!(image.is_some(), "{what}: the earlier statements are staged");
                    seams::override_next_statement_reservation(need - 1);
                    records::take_refusals();
                    let error = db
                        .execute(sql, &EMPTY())
                        .expect_err("one byte short of the peak need");
                    assert!(is_stage_refusal(&error), "{what}: the typed stage refusal: {error:?}");
                    let refusals = records::take_refusals();
                    assert_eq!(refusals.len(), 1, "{what}: one refusal: {refusals:?}");
                    assert_eq!(refusals[0].requested, bytes, "{what}: refused at that draw");
                    let now = ledger(db, before, &what);
                    assert_same_accounts(&now, &staged, &what);
                    assert_eq!(deltas(db, before), staged_deltas, "{what}: both counters as before");
                    assert_eq!(
                        db.local_schema_image_shape_for_test(tx),
                        image,
                        "{what}: the image is exactly as before the statement"
                    );
                    db.execute(LATER, &EMPTY()).unwrap();
                    ledger(db, before, &format!("{what}: after the later statement"));
                    if commit {
                        db.execute("COMMIT", &EMPTY()).unwrap();
                        assert_eq!(
                            committed_schema(db),
                            expected,
                            "{what}: the durable schema holds the earlier and later statements only"
                        );
                    } else {
                        db.execute("ROLLBACK", &EMPTY()).unwrap();
                        assert_settled(db, outer, tx, &what);
                        assert_eq!(committed_schema(db), schema_before, "{what}: nothing durable");
                    }
                }
            }
        }
    }

    #[test]
    fn local_schema_late_shortage_undoes_only_its_statement_on_the_legacy_host() {
        late_shortage_is_statement_atomic(Host::Legacy);
    }

    #[test]
    fn local_schema_late_shortage_undoes_only_its_statement_on_the_owned_host() {
        late_shortage_is_statement_atomic(Host::Owned);
    }

    /// The committed schema the estimate shapes run against.
    const ESTIMATE_BASE: [&str; 3] = [
        "CREATE TABLE estimate_parent (id UUID PRIMARY KEY, a TEXT, b TEXT, UNIQUE (a, b))",
        "CREATE TABLE wide_target (id UUID PRIMARY KEY, c0 TEXT, c1 TEXT, c2 TEXT, c3 TEXT, \
         c4 TEXT, c5 TEXT, c6 TEXT, c7 TEXT, c8 TEXT, c9 TEXT, c10 TEXT, c11 TEXT, c12 TEXT, \
         c13 TEXT, c14 TEXT, c15 TEXT, c16 TEXT, c17 TEXT, c18 TEXT, c19 TEXT, c20 TEXT, \
         c21 TEXT, c22 TEXT, c23 TEXT, c24 TEXT, c25 TEXT, c26 TEXT, c27 TEXT, c28 TEXT, \
         c29 TEXT, u0 TEXT UNIQUE, u1 TEXT UNIQUE, u2 TEXT UNIQUE, u3 TEXT UNIQUE, \
         r0 UUID REFERENCES estimate_parent(id), r1 UUID REFERENCES estimate_parent(id), \
         UNIQUE (c0, c1), UNIQUE (c2, c3, c4))",
        "CREATE INDEX wide_target_c5 ON wide_target (c5)",
    ];

    /// Declaration shapes the statement estimate does not derive exactly:
    /// added unique and reference columns, composite keys and references,
    /// duplicate declarations, a key over a column no exact filter can index,
    /// and statements over a large target table.
    const ESTIMATE_SHAPES: [&str; 11] = [
        "ALTER TABLE wide_target ADD COLUMN added_unique TEXT UNIQUE",
        "ALTER TABLE wide_target ADD COLUMN added_reference UUID REFERENCES estimate_parent(id)",
        "ALTER TABLE wide_target ADD COLUMN added_both UUID UNIQUE REFERENCES estimate_parent(id)",
        "CREATE TABLE composite_keys (a TEXT, b TEXT, c UUID, PRIMARY KEY (a, b), UNIQUE (b, c))",
        "CREATE TABLE composite_child (id UUID PRIMARY KEY, x TEXT, y TEXT, \
         FOREIGN KEY (x, y) REFERENCES estimate_parent(a, b))",
        "CREATE TABLE duplicate_unique (id UUID PRIMARY KEY UNIQUE, k TEXT UNIQUE, m TEXT, \
         UNIQUE (k, m), UNIQUE (k, m))",
        "CREATE TABLE duplicate_reference (id UUID PRIMARY KEY, p UUID UNIQUE REFERENCES estimate_parent(id), \
         q TEXT, UNIQUE (p, q), UNIQUE (p, q))",
        "CREATE TABLE non_indexable_keys (id UUID PRIMARY KEY, j JSON UNIQUE)",
        "CREATE INDEX wide_target_c6 ON wide_target (c6)",
        "CREATE INDEX wide_target_c7_c8 ON wide_target (c7, c8)",
        "ALTER TABLE wide_target ADD COLUMN added_plain TEXT",
    ];

    // The statement reservation is an admission estimate, not a proof; the
    // spend scope is the enforcement. For each shape, on a schema with a
    // large target table, the staged statement either runs within its credit
    // or refuses with the typed stage refusal: it never panics and never
    // overdraws. On success the ledger equation holds and both counters grow
    // by exactly the statement's reservation; on a refusal everything is as
    // before the statement. A shape the engine rejects for its declaration
    // fails exactly as the same statement does in autocommit on a twin. Each
    // shape's estimate and the peak its scope actually drew are printed.
    fn estimate_shapes_run_within_credit_or_refuse(host: Host) {
        for sql in ESTIMATE_SHAPES {
            let autocommit = {
                let fixture = open_host(host);
                let db = &fixture.db;
                for statement in ESTIMATE_BASE {
                    db.execute(statement, &EMPTY()).unwrap();
                }
                db.execute(sql, &EMPTY()).map(|_| ()).map_err(|error| error.to_string())
            };
            let fixture = open_host(host);
            let db = &fixture.db;
            for statement in ESTIMATE_BASE {
                db.execute(statement, &EMPTY()).unwrap();
            }
            let outer = base(db);
            let (tx, before) = begin(db);
            db.execute(STATEMENTS[0], &EMPTY()).unwrap();
            let staged = ledger(db, before, sql);
            let staged_deltas = deltas(db, before);
            let reservation = db.staged_statement_reservation_bytes_for_test(tx, sql);
            records::take_draws();
            records::take_refusals();
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                db.execute(sql, &EMPTY())
            }))
            .unwrap_or_else(|_| panic!("{host:?} {sql}: a staged statement never panics"));
            let draws = records::take_draws();
            let peak = draws
                .iter()
                .map(|(consumed, bytes)| consumed + bytes)
                .max()
                .unwrap_or(0);
            eprintln!(
                "estimate-shape {host:?} estimate={reservation} drawn_peak={peak} ok={} {sql}",
                outcome.is_ok()
            );
            let now = ledger(db, before, sql);
            match outcome {
                Ok(_) => {
                    assert!(peak <= reservation, "{host:?} {sql}: the scope never drew past its credit");
                    let now_deltas = deltas(db, before);
                    assert_eq!(
                        (now_deltas.0 - staged_deltas.0, now_deltas.1 - staged_deltas.1),
                        (reservation as i128, reservation as i128),
                        "{host:?} {sql}: both counters grow by exactly the reservation"
                    );
                    assert!(autocommit.is_ok(), "{host:?} {sql}: autocommit admits it too: {autocommit:?}");
                }
                Err(error) => {
                    assert_same_accounts(&now, &staged, sql);
                    assert_eq!(deltas(db, before), staged_deltas, "{host:?} {sql}: counters as before");
                    if is_stage_refusal(&error) {
                        assert_eq!(records::take_refusals().len(), 1, "{host:?} {sql}: one typed refusal");
                    } else {
                        assert_eq!(
                            Err(error.to_string()),
                            autocommit,
                            "{host:?} {sql}: a declaration error matches autocommit's"
                        );
                    }
                }
            }
            db.execute("ROLLBACK", &EMPTY()).unwrap();
            assert_settled(db, outer, tx, &format!("{host:?} {sql}: after rollback"));
        }
    }

    #[test]
    fn local_schema_estimate_shapes_run_within_credit_or_refuse_on_the_legacy_host() {
        estimate_shapes_run_within_credit_or_refuse(Host::Legacy);
    }

    #[test]
    fn local_schema_estimate_shapes_run_within_credit_or_refuse_on_the_owned_host() {
        estimate_shapes_run_within_credit_or_refuse(Host::Owned);
    }
}
