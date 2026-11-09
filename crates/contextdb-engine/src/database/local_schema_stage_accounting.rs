// A local schema stage is charged on the live accountant through ordinary
// admission, on both hosts. Each scenario uses caller-driven maintenance and no
// sync, subscription or sink worker; run the file's exact-byte tests one at a
// time (`--test-threads=1 --exact <name>`), since the process aggregate is shared.
mod local_schema_stage_accounting {
    use super::*;
    use crate::database::statement_execution::local_schema_test_seams as seams;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Host {
        Legacy,
        Owned,
    }

    const HOSTS: [Host; 2] = [Host::Legacy, Host::Owned];

    struct Fixture {
        db: Database,
        _dir: tempfile::TempDir,
    }

    fn open_host(host: Host) -> Fixture {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("local-schema-stage.redb");
        let options = crate::read_session::DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        };
        let db = match host {
            Host::Legacy => Database::open_with_options(&path, options),
            Host::Owned => Database::open_owned_for_test(&path, options),
        }
        .unwrap();
        Fixture { db, _dir: dir }
    }

    const EMPTY: fn() -> HashMap<String, Value> = HashMap::new;

    /// The live and process-aggregate byte counters.
    fn counters(db: &Database) -> (usize, usize) {
        (
            db.finalization.accountant.usage().used,
            crate::memory_accounting::process_aggregate_bytes_for_test(),
        )
    }

    fn session_tx(db: &Database) -> TxId {
        (*db.session_tx.lock()).expect("an SQL transaction is open")
    }

    fn stage_pending(db: &Database, tx: TxId) -> bool {
        db.pending_local_schema_stages.lock().contains_key(&tx)
    }

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

    const STATEMENTS: [&str; 3] = [
        "CREATE TABLE authored_migration (id UUID PRIMARY KEY, body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        "CREATE TRIGGER authored_migration_insert ON authored_migration WHEN INSERT",
        "ALTER TABLE authored_migration ADD COLUMN detail TEXT",
    ];
    const LATER: &str = "CREATE TABLE later_table (id UUID PRIMARY KEY)";
    const REFUSED_TABLE: &str = "CREATE TABLE refused_table (id UUID PRIMARY KEY, body TEXT)";

    // Refusal before mutation: at each headroom that refuses a statement, up to
    // the last byte it needs, the statement leaves the stage, both counters and
    // the earlier DDL exactly as they were, a later statement still runs, and
    // COMMIT holds only the earlier and later statements.
    #[test]
    fn a_refused_statement_keeps_earlier_ddl_and_both_counters() {
        for host in HOSTS {
            let expected = {
                let fixture = open_host(host);
                let db = &fixture.db;
                db.execute("BEGIN", &EMPTY()).unwrap();
                db.execute(STATEMENTS[0], &EMPTY()).unwrap();
                db.execute(LATER, &EMPTY()).unwrap();
                db.execute("COMMIT", &EMPTY()).unwrap();
                committed_schema(db)
            };
            // Runs one statement at one headroom; a refusal must leave everything
            // as it was. Returns whether it was refused.
            let refused_at = |refused_sql: &str, headroom: usize| -> bool {
                let what = format!("{host:?} {refused_sql} headroom={headroom}");
                let fixture = open_host(host);
                let db = &fixture.db;
                db.execute("BEGIN", &EMPTY()).unwrap();
                let tx = session_tx(db);
                db.execute(STATEMENTS[0], &EMPTY()).unwrap();
                let limit = db.finalization.accountant.usage().limit;
                db.set_memory_limit(Some(counters(db).0 + headroom)).unwrap();
                let staged = counters(db);
                let error = match db.execute(refused_sql, &EMPTY()) {
                    Ok(_) => {
                        db.execute("ROLLBACK", &EMPTY()).unwrap();
                        return false;
                    }
                    Err(error) => error,
                };
                assert!(
                    matches!(error, Error::MemoryBudgetExceeded { .. }),
                    "{what}: a typed refusal: {error:?}"
                );
                assert_eq!(counters(db), staged, "{what}: both counters as before");
                assert!(stage_pending(db, tx), "{what}: the earlier statement stays staged");
                db.set_memory_limit(limit).unwrap();
                db.execute(LATER, &EMPTY()).unwrap();
                db.execute("COMMIT", &EMPTY()).unwrap();
                assert_eq!(committed_schema(db), expected, "{what}: only the admitted DDL commits");
                true
            };
            for refused_sql in [STATEMENTS[2], REFUSED_TABLE] {
                // Doubling, then bisection up to the last byte the statement needs.
                assert!(refused_at(refused_sql, 0), "{host:?} {refused_sql}: no headroom refuses");
                let (mut refused, mut admitted) = (0usize, 16usize);
                while refused_at(refused_sql, admitted) {
                    refused = admitted;
                    admitted *= 2;
                    assert!(admitted < 1 << 24, "{host:?} {refused_sql}: admitted at some headroom");
                }
                while admitted - refused > 1 {
                    let middle = refused + (admitted - refused) / 2;
                    if refused_at(refused_sql, middle) {
                        refused = middle;
                    } else {
                        admitted = middle;
                    }
                }
            }
        }
    }

    // A private commit's retention is admitted before its apply: a shortage
    // refuses with nothing charged, and only the apply's consumption keeps it.
    #[test]
    fn retention_is_admitted_before_apply_and_returned_unless_consumed() {
        use crate::memory_accounting::PreadmittedRetention;
        let accountant = MemoryAccountant::with_budget(100);
        let error = PreadmittedRetention::admit(&accountant, 101)
            .err()
            .expect("a shortage refuses before the apply");
        assert!(matches!(error, Error::MemoryBudgetExceeded { .. }), "{error:?}");
        assert_eq!(accountant.usage().used, 0);
        drop(PreadmittedRetention::admit(&accountant, 60).unwrap());
        assert_eq!(accountant.usage().used, 0, "an unconsumed admission returns its bytes");
        PreadmittedRetention::admit(&accountant, 60).unwrap().consume(60);
        assert_eq!(accountant.usage().used, 60, "the retained owner now holds the bytes");
    }

    // Rollback returns every byte.
    #[test]
    fn rollback_returns_every_stage_byte() {
        for host in HOSTS {
            let fixture = open_host(host);
            let db = &fixture.db;
            let outer = counters(db);
            db.execute("BEGIN", &EMPTY()).unwrap();
            let tx = session_tx(db);
            for sql in STATEMENTS {
                db.execute(sql, &EMPTY()).unwrap();
            }
            db.execute("ROLLBACK", &EMPTY()).unwrap();
            assert!(!stage_pending(db, tx));
            assert_eq!(counters(db), outer, "{host:?}: live and aggregate bytes return");
            assert!(db.table_meta("authored_migration").is_none(), "{host:?}");
        }
    }

    // A failure or unwind after execution frees the image and blocks COMMIT.
    #[test]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn a_failure_after_execution_frees_the_image_and_requires_rollback() {
        for host in HOSTS {
            for kind in [seams::LocalSchemaFaultKind::Error, seams::LocalSchemaFaultKind::Unwind] {
                for after_growth in [false, true] {
                    let what = format!("{host:?} {kind:?} after_growth={after_growth}");
                    let fixture = open_host(host);
                    let db = &fixture.db;
                    let outer = counters(db);
                    db.execute("BEGIN", &EMPTY()).unwrap();
                    let tx = session_tx(db);
                    let begun = counters(db);
                    if after_growth {
                        db.execute(STATEMENTS[0], &EMPTY()).unwrap();
                    }
                    seams::arm_statement_fault(kind);
                    let sql = "CREATE TABLE faulted (id UUID PRIMARY KEY)";
                    match kind {
                        seams::LocalSchemaFaultKind::Error => {
                            db.execute(sql, &EMPTY()).expect_err("the injected fault");
                        }
                        seams::LocalSchemaFaultKind::Unwind => {
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                db.execute(sql, &EMPTY())
                            }))
                            .expect_err("the injected unwind");
                        }
                    }
                    assert!(!stage_pending(db, tx), "{what}: the stage is gone");
                    assert_eq!(counters(db), begun, "{what}: the image returned every byte");
                    let refused = db
                        .execute("COMMIT", &EMPTY())
                        .expect_err("a discarded stage cannot commit");
                    assert_eq!(
                        refused.to_string(),
                        crate::database::statement_execution::local_schema_stage_requires_rollback()
                            .to_string(),
                        "{what}"
                    );
                    db.execute("ROLLBACK", &EMPTY()).unwrap();
                    assert_eq!(counters(db), outer, "{what}: after rollback");
                    assert!(db.table_meta("faulted").is_none(), "{what}: nothing committed");
                    assert!(db.table_meta("authored_migration").is_none(), "{what}: nothing committed");
                }
            }
        }
    }

    // A failed preparation returns its held copies and its metadata fold.
    #[test]
    fn a_failed_preparation_returns_every_byte() {
        for host in HOSTS {
            for at in [
                seams::PreparationFault::AfterFoldBeforeTables,
                seams::PreparationFault::AfterRegistration,
            ] {
                let what = format!("{host:?} {at:?}");
                let fixture = open_host(host);
                let db = &fixture.db;
                db.execute("CREATE TABLE committed_before (id UUID PRIMARY KEY, body TEXT)", &EMPTY())
                    .unwrap();
                let outer = counters(db);
                db.execute("BEGIN", &EMPTY()).unwrap();
                let tx = session_tx(db);
                let begun = counters(db);
                seams::arm_preparation_fault(at);
                db.execute("CREATE TABLE never_prepared (id UUID PRIMARY KEY)", &EMPTY())
                    .expect_err("the preparation fault fires");
                assert!(!stage_pending(db, tx), "{what}: no stage");
                assert_eq!(counters(db), begun, "{what}: every byte returned");
                db.execute("ROLLBACK", &EMPTY()).unwrap();
                assert_eq!(counters(db), outer, "{what}: after rollback");
            }
        }
    }

    // COMMIT and reopen match the same schema run normally, with no lingering
    // stage charge.
    #[test]
    fn commit_and_reopen_match_the_same_schema_run_normally() {
        for host in HOSTS {
            let warmed = || {
                let fixture = open_host(host);
                fixture.db.execute("BEGIN", &EMPTY()).unwrap();
                fixture
                    .db
                    .execute("CREATE TABLE warm_up (id UUID PRIMARY KEY)", &EMPTY())
                    .unwrap();
                fixture.db.execute("COMMIT", &EMPTY()).unwrap();
                fixture
            };
            let transactional = warmed();
            let db = &transactional.db;
            let before = counters(db);
            db.execute("BEGIN", &EMPTY()).unwrap();
            let tx = session_tx(db);
            for sql in STATEMENTS {
                db.execute(sql, &EMPTY()).unwrap();
            }
            db.execute("COMMIT", &EMPTY()).unwrap();
            assert!(!stage_pending(db, tx), "{host:?}: the stage is gone after COMMIT");
            let after = counters(db);
            let retained = (after.0 - before.0, after.1 - before.1);
            let normal = warmed();
            let other = &normal.db;
            let before = counters(other);
            for sql in STATEMENTS {
                other.execute(sql, &EMPTY()).unwrap();
            }
            let after = counters(other);
            assert_eq!(
                retained,
                (after.0 - before.0, after.1 - before.1),
                "{host:?}: COMMIT retains what the same DDL run normally retains"
            );
            assert_eq!(committed_schema(db), committed_schema(other), "{host:?}");
            let schema = committed_schema(db);
            let path = transactional._dir.path().join("local-schema-stage.redb");
            transactional.db.close().unwrap();
            let reopened = match host {
                Host::Legacy => Database::open(&path),
                Host::Owned => Database::open_owned_for_test(&path, Default::default()),
            }
            .unwrap();
            assert_eq!(committed_schema(&reopened), schema, "{host:?}: after reopen");
            reopened.close().unwrap();
        }
    }

    // The negative fixture: an internal image has no durable store, so both
    // received-stage entries refuse it before any working write begins.
    #[test]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn a_private_image_is_refused_as_a_receiver_before_any_working_write() {
        let fixture = open_host(Host::Legacy);
        let db = &fixture.db;
        let image = db.detached_local_schema_working_database().unwrap();
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
    }
}
