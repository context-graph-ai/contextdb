//! Independent completion of the accepted endpoint-retirement design.
//! Public construction is Legacy; private access observes or authentically damages fixtures.
use super::*;
use crate::{Database, MaintenancePolicy, ReadSession};
use contextdb_core::read_contract::ReadRoute;
use contextdb_tx::OwnedAdjEntry;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

const EXPIRED: u128 = 81_001;
const SURVIVOR: u128 = 81_002;
const OTHER: u128 = 81_003;
const EDGE: u128 = 91_000;
const OTHER_EDGE: u128 = 91_001;

fn frozen_read(path: &Path) -> Result<ReadSession> {
    ReadSession::open_with_clock_for_test(
        path,
        Default::default(),
        Arc::new(crate::local_transport::ManualDeadlineClock::at(0)),
    )
}
fn params() -> HashMap<String, Value> {
    HashMap::from([(
        "anchor".into(),
        Value::Uuid(uuid::Uuid::from_u128(SURVIVOR)),
    )])
}
fn graph_sql(reverse: bool, kind: &str) -> String {
    if reverse {
        format!(
            "SELECT t FROM GRAPH_TABLE(edges MATCH (a)<-[:{kind}]-(b) WHERE a.id = $anchor COLUMNS (b.id AS t)) ORDER BY t"
        )
    } else {
        format!(
            "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:{kind}]->(b) WHERE a.id = $anchor COLUMNS (b.id AS t)) ORDER BY t"
        )
    }
}
fn nodes(ids: &[u128]) -> Vec<Vec<Value>> {
    ids.iter()
        .map(|id| vec![Value::Uuid(uuid::Uuid::from_u128(*id))])
        .collect()
}
fn writer(path: &Path) -> Database {
    let db = Database::open(path).expect("production Legacy writer open");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db
}
fn close(db: Database) {
    db.close().unwrap();
    drop(db);
}
fn seed(db: &Database, reverse: bool, due: bool, current_only: bool) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE retained (id UUID PRIMARY KEY, expires_at TIMESTAMP EXPIRES, embedding VECTOR(2)) RETAIN 1 HOURS", &HashMap::new()).unwrap();
    let history = if current_only {
        " HISTORY CURRENT ONLY SYNC TWO WAY SYNC CONFLICT KEEP LATEST"
    } else {
        ""
    };
    db.execute(&format!("CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT){history}"), &HashMap::new()).unwrap();
    for id in [EXPIRED, SURVIVOR] {
        db.execute(
            "INSERT INTO retained VALUES ($id, $expires, '[1,0]')",
            &HashMap::from([
                ("id".into(), Value::Uuid(uuid::Uuid::from_u128(id))),
                (
                    "expires".into(),
                    Value::Timestamp(if due && id == EXPIRED { -1 } else { i64::MAX }),
                ),
            ]),
        )
        .unwrap();
    }
    for (id, other) in [(EDGE, EXPIRED), (OTHER_EDGE, OTHER)] {
        let (source, target) = if reverse {
            (other, SURVIVOR)
        } else {
            (SURVIVOR, other)
        };
        db.execute(
            "INSERT INTO edges VALUES ($id, $source, $target, 'LINKS')",
            &HashMap::from([
                ("id".into(), Value::Uuid(uuid::Uuid::from_u128(id))),
                ("source".into(), Value::Uuid(uuid::Uuid::from_u128(source))),
                ("target".into(), Value::Uuid(uuid::Uuid::from_u128(target))),
            ]),
        )
        .unwrap();
    }
    assert_eq!(
        db.execute(&graph_sql(reverse, "LINKS"), &params())
            .unwrap()
            .rows(),
        nodes(&[EXPIRED, OTHER])
    );
    assert_eq!(
        db.execute("SELECT id FROM edges ORDER BY id", &HashMap::new())
            .unwrap()
            .rows(),
        nodes(&[EDGE, OTHER_EDGE])
    );
}
fn retire_other_endpoint(db: &Database) {
    db.execute(
        "INSERT INTO retained VALUES ($id, $expires, '[1,0]')",
        &HashMap::from([
            ("id".into(), Value::Uuid(uuid::Uuid::from_u128(OTHER))),
            ("expires".into(), Value::Timestamp(-1)),
        ]),
    )
    .unwrap();
    db.run_pruning_cycle_checked().unwrap();
}
fn change_owner_life(db: &Database) {
    db.execute(
        "UPDATE edges SET edge_type = 'RENEWED' WHERE id = $id",
        &HashMap::from([("id".into(), Value::Uuid(uuid::Uuid::from_u128(EDGE)))]),
    )
    .expect("a retired old owner does not suppress its new exact row life");
}
fn is_retirement_key(key: &str) -> bool {
    key.starts_with("row_graph_retired_v1")
        || key
            .strip_prefix("sync_lineage.v1.")
            .is_some_and(|key| key.starts_with("row_graph_retired_v1"))
}
fn fixture_retirement_key(owner: &contextdb_tx::RowEdgeOwner) -> String {
    use std::fmt::Write;
    let mut key = String::from("row_graph_retired_v1.");
    for byte in owner.table.as_bytes() {
        write!(&mut key, "{byte:02x}").unwrap();
    }
    write!(
        &mut key,
        ".{:020}.{:020}.{:020}.{:020}",
        owner.row_id.0, owner.created_tx.0, owner.lsn.0, owner.table_generation
    )
    .unwrap();
    key
}
fn inspect<R>(path: &Path, action: impl FnOnce(&RedbPersistence) -> R) -> R {
    let persistence = RedbPersistence::open(path).unwrap();
    let memory = crate::read_image_memory::ImageMemory::new(None).unwrap();
    let _scope = memory.enter();
    let result = action(&persistence);
    persistence.close();
    result
}
fn controls(path: &Path) -> BTreeMap<String, Vec<u8>> {
    inspect(path, |p| {
        p.dump_config_raw()
            .unwrap()
            .into_iter()
            .filter(|(key, _)| is_retirement_key(key))
            .collect()
    })
}
fn owned(path: &Path, reverse: bool) -> Vec<OwnedAdjEntry> {
    inspect(path, |p| {
        p.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let table = read
                .open_table(if reverse {
                    row_graph::REVERSE
                } else {
                    row_graph::FORWARD
                })
                .map_err(RedbPersistence::storage_error)?;
            let mut result = Vec::new();
            for entry in table.iter().map_err(RedbPersistence::storage_error)? {
                let (_, value) = entry.map_err(RedbPersistence::storage_error)?;
                let (entry, used): (OwnedAdjEntry, usize) =
                    bincode::serde::decode_from_slice(value.value(), bincode::config::standard())
                        .unwrap();
                assert_eq!(used, value.value().len());
                result.push(entry);
            }
            Ok(result)
        })
        .unwrap()
    })
}
fn assert_export(path: &Path, reverse: bool, retired: bool) {
    let expected = if retired {
        nodes(&[OTHER])
    } else {
        nodes(&[EXPIRED, OTHER])
    };
    let file = frozen_read(path).expect("a complete export is readable");
    assert_eq!(file.route(), ReadRoute::File);
    assert_eq!(
        file.execute(&graph_sql(reverse, "LINKS"), &params())
            .unwrap()
            .rows(),
        expected
    );
    assert_eq!(
        file.execute("SELECT id FROM edges ORDER BY id", &HashMap::new())
            .unwrap()
            .rows(),
        nodes(&[EDGE, OTHER_EDGE])
    );
    drop(file);
    let forward = owned(path, false);
    let reverse_entries = owned(path, true);
    let encoded = |entries: Vec<OwnedAdjEntry>| {
        let mut values: Vec<_> = entries
            .into_iter()
            .map(|entry| bincode::serde::encode_to_vec(entry, bincode::config::standard()).unwrap())
            .collect();
        values.sort();
        values
    };
    assert_eq!(
        encoded(forward.clone()),
        encoded(reverse_entries),
        "export contains reciprocal exact-owner witnesses"
    );
    assert_eq!(forward.len(), expected.len());
    assert_eq!(
        forward
            .iter()
            .filter(|entry| entry.entry.source == uuid::Uuid::from_u128(EXPIRED)
                || entry.entry.target == uuid::Uuid::from_u128(EXPIRED))
            .count(),
        usize::from(!retired),
        "the retired exact owner never exports a live witness"
    );
    assert_eq!(
        controls(path).len(),
        usize::from(retired),
        "one captured export emits live witness OR retirement for the selected owner"
    );
}
fn assert_current_routes(path: &Path, reverse: bool, old_kind: &[u128], new_kind: &[u128]) {
    let db = writer(path);
    let owner = frozen_read(path).unwrap();
    assert_eq!(owner.route(), ReadRoute::Owner);
    for (kind, expected) in [("LINKS", old_kind), ("RENEWED", new_kind)] {
        assert_eq!(
            owner
                .execute(&graph_sql(reverse, kind), &params())
                .unwrap()
                .rows(),
            nodes(expected)
        );
        assert_eq!(
            db.execute(&graph_sql(reverse, kind), &params())
                .unwrap()
                .rows(),
            nodes(expected)
        );
    }
    assert_eq!(
        owner
            .execute("SELECT id FROM edges ORDER BY id", &HashMap::new())
            .unwrap()
            .rows(),
        nodes(&[EDGE, OTHER_EDGE])
    );
    drop(owner);
    close(db);
    let file = frozen_read(path).unwrap();
    assert_eq!(file.route(), ReadRoute::File);
    assert_eq!(
        file.execute(&graph_sql(reverse, "LINKS"), &params())
            .unwrap()
            .rows(),
        nodes(old_kind)
    );
    assert_eq!(
        file.execute(&graph_sql(reverse, "RENEWED"), &params())
            .unwrap()
            .rows(),
        nodes(new_kind)
    );
    assert_eq!(
        file.execute("SELECT id FROM edges ORDER BY id", &HashMap::new())
            .unwrap()
            .rows(),
        nodes(&[EDGE, OTHER_EDGE])
    );
}

#[test]
fn memory_retirement_export_has_an_exact_retirement_and_no_live_witness_for_it() {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    for reverse in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let db = Database::open_memory();
        seed(&db, reverse, true, false);
        db.run_pruning_cycle_checked().unwrap();
        assert_eq!(
            db.execute(&graph_sql(reverse, "LINKS"), &params())
                .unwrap()
                .rows(),
            nodes(&[OTHER]),
            "memory retention retires the graph binding while preserving its SQL owner"
        );
        let path = directory.path().join("memory-retired.db");
        db.export_snapshot(&path).unwrap();
        assert_export(&path, reverse, true);
        close(db);
    }
}

#[test]
fn memory_reader_protection_exports_live_witnesses_until_the_release_event() {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let directory = tempfile::tempdir().unwrap();
    let db = Database::open_memory();
    seed(&db, false, false, false);
    let snapshot = db.snapshot();
    let pin = db.pin_snapshot(snapshot).unwrap();
    db.execute(
        "UPDATE retained SET expires_at = $expires WHERE id = $id",
        &HashMap::from([
            ("id".into(), Value::Uuid(uuid::Uuid::from_u128(EXPIRED))),
            ("expires".into(), Value::Timestamp(-1)),
        ]),
    )
    .unwrap();
    db.run_pruning_cycle_checked().unwrap();
    let before = directory.path().join("before-release.db");
    db.export_snapshot(&before).unwrap();
    assert_export(&before, false, true);
    assert_eq!(
        db.execute_at_snapshot(&graph_sql(false, "LINKS"), &params(), snapshot)
            .unwrap()
            .rows(),
        nodes(&[EXPIRED, OTHER]),
        "the older pinned snapshot retains its binding"
    );
    drop(pin);
    db.run_pruning_cycle_checked().unwrap();
    let after = directory.path().join("after-release.db");
    db.export_snapshot(&after).unwrap();
    assert_export(&after, false, true);
    close(db);
}

#[test]
fn memory_export_captured_before_a_new_owner_life_keeps_its_retired_watermark() {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let directory = tempfile::tempdir().unwrap();
    let db = Database::open_memory();
    seed(&db, false, true, false);
    db.run_pruning_cycle_checked().unwrap();
    assert_eq!(
        db.execute(&graph_sql(false, "LINKS"), &params())
            .unwrap()
            .rows(),
        nodes(&[OTHER])
    );
    let path = directory.path().join("captured-retired.db");
    std::thread::scope(|scope| {
        // Drop the release guard before scoped-thread joining on every unwind.
        let pause = db.pause_after_export_snapshot_capture_for_test();
        let export = scope.spawn(|| db.export_snapshot(&path));
        // This bound is only a named missing-event watchdog, never a latency assertion.
        assert!(
            pause.wait_until_reached(std::time::Duration::from_secs(30)),
            "memory export capture never delivered"
        );
        let update = db.execute(
            "UPDATE edges SET edge_type = 'RENEWED' WHERE id = $id",
            &HashMap::from([("id".into(), Value::Uuid(uuid::Uuid::from_u128(EDGE)))]),
        );
        pause.release();
        let exported = export.join().unwrap();
        update.unwrap();
        exported.unwrap();
    });
    assert_export(&path, false, true);
    assert_eq!(
        db.execute(&graph_sql(false, "RENEWED"), &params())
            .unwrap()
            .rows(),
        nodes(&[EXPIRED]),
        "the later exact owner is live locally but absent from the earlier export"
    );
    close(db);
}

#[test]
fn a_new_exact_row_owner_after_retirement_is_live_on_owner_and_file_routes() {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    for reverse in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("new-life.db");
        let db = writer(&path);
        seed(&db, reverse, true, false);
        db.run_pruning_cycle_checked().unwrap();
        assert_eq!(
            db.execute(&graph_sql(reverse, "LINKS"), &params())
                .unwrap()
                .rows(),
            nodes(&[OTHER])
        );
        let retired_snapshot = db.snapshot();
        let pin = db.pin_snapshot(retired_snapshot).unwrap();
        change_owner_life(&db);
        assert_eq!(
            db.execute_at_snapshot(&graph_sql(reverse, "LINKS"), &params(), retired_snapshot)
                .unwrap()
                .rows(),
            nodes(&[OTHER]),
            "the old owner remains retired at its own snapshot"
        );
        assert!(
            db.execute_at_snapshot(&graph_sql(reverse, "RENEWED"), &params(), retired_snapshot)
                .unwrap()
                .rows()
                .is_empty()
        );
        drop(pin);
        close(db);
        assert_current_routes(&path, reverse, &[OTHER], &[EXPIRED]);
    }
}

#[test]
fn a_new_table_generation_does_not_inherit_the_retired_owner_or_its_metadata() {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("new-generation.db");
    let db = writer(&path);
    seed(&db, false, true, false);
    db.run_pruning_cycle_checked().unwrap();
    close(db);
    assert_eq!(controls(&path).len(), 1);
    let db = writer(&path);
    db.execute("DROP TABLE edges", &HashMap::new())
        .expect("drop accepts authenticated retirement of its old owner");
    close(db);
    assert!(
        controls(&path).is_empty(),
        "table-generation cleanup collects its exact retirement namespace"
    );
    let db = writer(&path);
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO edges VALUES ($id, $source, $target, 'LINKS')",
        &HashMap::from([
            ("id".into(), Value::Uuid(uuid::Uuid::from_u128(EDGE))),
            (
                "source".into(),
                Value::Uuid(uuid::Uuid::from_u128(SURVIVOR)),
            ),
            ("target".into(), Value::Uuid(uuid::Uuid::from_u128(EXPIRED))),
        ]),
    )
    .unwrap();
    let owner = frozen_read(&path).unwrap();
    assert_eq!(owner.route(), ReadRoute::Owner);
    assert_eq!(
        owner
            .execute(&graph_sql(false, "LINKS"), &params())
            .unwrap()
            .rows(),
        nodes(&[EXPIRED])
    );
    assert_eq!(
        owner
            .execute("SELECT id FROM edges", &HashMap::new())
            .unwrap()
            .rows(),
        nodes(&[EDGE])
    );
    drop(owner);
    close(db);
    let file = frozen_read(&path).unwrap();
    assert_eq!(file.route(), ReadRoute::File);
    assert_eq!(
        file.execute(&graph_sql(false, "LINKS"), &params())
            .unwrap()
            .rows(),
        nodes(&[EXPIRED])
    );
    assert_eq!(
        file.execute("SELECT id FROM edges", &HashMap::new())
            .unwrap()
            .rows(),
        nodes(&[EDGE])
    );
}

fn rebuild_lineage(path: &Path) {
    inspect(path, |p| {
        p.with_db(|db| {
            let write = RedbPersistence::begin_history_write(db)?;
            RedbPersistence::rebuild_read_lineage_config_in_write(&write)?;
            RedbPersistence::apply_content_identity_mutations_in_write(
                &write,
                &[],
                &mut p.proof_admission(),
            )?;
            RedbPersistence::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap()
    });
}
fn projected_controls(path: &Path) -> BTreeMap<String, Vec<u8>> {
    inspect(path, |p| {
        p.with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let table = read
                .open_table(READ_LINEAGE_CONFIG_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let mut result = BTreeMap::new();
            for item in table.iter().map_err(RedbPersistence::storage_error)? {
                let (key, value) = item.map_err(RedbPersistence::storage_error)?;
                if is_retirement_key(key.value()) {
                    result.insert(key.value().to_owned(), value.value().to_vec());
                }
            }
            Ok(result)
        })
        .unwrap()
    })
}

#[test]
fn retirement_namespace_is_preserved_by_authenticated_lineage_projection_rebuild() {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("namespace-rebuild.db");
    let db = writer(&path);
    seed(&db, false, true, false);
    db.run_pruning_cycle_checked().unwrap();
    close(db);
    let before = controls(&path);
    assert_eq!(
        before.len(),
        1,
        "the retired exact owner has canonical lifecycle metadata"
    );
    rebuild_lineage(&path);
    assert_eq!(
        controls(&path),
        before,
        "rebuild never changes canonical retirement records"
    );
    assert_eq!(
        projected_controls(&path),
        before,
        "the complete retirement prefix is restored into the authenticated lineage projection"
    );
    assert_current_routes(&path, false, &[OTHER], &[]);
}

#[test]
fn metadata_gc_removes_only_retirement_for_the_pruned_exact_sql_owner() {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("retirement-gc.db");
    let db = writer(&path);
    seed(&db, false, true, true);
    db.run_pruning_cycle_checked().unwrap();
    close(db);
    let first = controls(&path);
    assert_eq!(first.len(), 1);
    let db = writer(&path);
    retire_other_endpoint(&db);
    close(db);
    let both = controls(&path);
    assert_eq!(both.len(), 2);
    let survivor: BTreeMap<_, _> = both
        .into_iter()
        .filter(|(key, _)| !first.contains_key(key))
        .collect();
    assert_eq!(survivor.len(), 1);
    let db = writer(&path);
    let old_owner_snapshot = db.snapshot();
    let old_owner_pin = db.pin_snapshot(old_owner_snapshot).unwrap();
    change_owner_life(&db);
    let versions = db.__physical_version_count_for_test("edges");
    assert!(
        versions > 2,
        "the old exact SQL version is still present before currency cleanup"
    );
    drop(old_owner_pin);
    for _ in 0..=versions {
        db.compact_currency_versions().unwrap();
        if db.__physical_version_count_for_test("edges") == 2 {
            break;
        }
    }
    assert_eq!(
        db.__physical_version_count_for_test("edges"),
        2,
        "the finite exact-version cleanup event must complete"
    );
    close(db);
    assert_eq!(
        controls(&path),
        survivor,
        "metadata GC removes the pruned owner's retirement and preserves the other live SQL owner's record"
    );
    assert_eq!(projected_controls(&path), survivor);
    assert_current_routes(&path, false, &[], &[EXPIRED]);
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    ForwardOnly,
    ReverseOnly,
    WitnessMismatch,
    MalformedRetirement,
    WrongOwnerRetirement,
    LiveAndRetired,
}
#[derive(Clone, Copy, Debug)]
enum Door {
    Owner,
    File,
    Update,
    Export,
}
fn rewrite_control(path: &Path, key: &str, value: Option<&[u8]>) {
    inspect(path, |p| {
        p.with_db(|db| {
            let write = RedbPersistence::begin_history_write(db)?;
            {
                let mut config = write
                    .open_table(CONFIG_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                if let Some(value) = value {
                    config
                        .insert(key, value)
                        .map_err(RedbPersistence::storage_error)?;
                } else {
                    config.remove(key).map_err(RedbPersistence::storage_error)?;
                }
            }
            // Both canonical and projected bytes have valid producer roots. The
            // public refusal must concern the damaged control, not a stale checksum.
            RedbPersistence::write_read_lineage_config_entry(&write, key, value)?;
            RedbPersistence::apply_content_identity_mutations_in_write(
                &write,
                &content_identity::config_mutations(key, value),
                &mut p.proof_admission(),
            )?;
            RedbPersistence::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap()
    });
}
fn damage_witness(path: &Path, selected: &OwnedAdjEntry) {
    inspect(path, |p| {
        p.with_db(|db| {
            let write = RedbPersistence::begin_history_write(db)?;
            let mut wrong = selected.clone();
            wrong.entry.lsn = Lsn(wrong.entry.lsn.0 + 1);
            let bytes = bincode::serde::encode_to_vec(&wrong, bincode::config::standard()).unwrap();
            for reverse in [false, true] {
                let key = RedbPersistence::row_graph_key(selected, reverse)?;
                write
                    .open_table(if reverse {
                        row_graph::REVERSE
                    } else {
                        row_graph::FORWARD
                    })
                    .map_err(RedbPersistence::storage_error)?
                    .insert(key.as_slice(), bytes.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap()
    });
}
// Fixture-only bypass: sanctioned append rejects resurrection of a retired owner.
fn inject_live_witness_pair(path: &Path, selected: &OwnedAdjEntry) {
    assert!(selected.entry.deleted_tx.is_none());
    inspect(path, |p| {
        p.with_db(|db| {
            let write = RedbPersistence::begin_history_write(db)?;
            let bytes =
                bincode::serde::encode_to_vec(selected, bincode::config::standard()).unwrap();
            for reverse in [false, true] {
                let key = RedbPersistence::row_graph_key(selected, reverse)?;
                write
                    .open_table(if reverse {
                        row_graph::REVERSE
                    } else {
                        row_graph::FORWARD
                    })
                    .map_err(RedbPersistence::storage_error)?
                    .insert(key.as_slice(), bytes.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap()
    });
}
fn assert_corruption(result: Result<()>, fault: Fault, door: Door) {
    match result {
        Err(Error::StoreCorrupted { .. }) => {}
        other => panic!(
            "authenticated control damage must be typed StoreCorrupted ({fault:?}/{door:?}): {other:?}"
        ),
    }
}
fn probe(path: &Path, destination: &Path, fault: Fault, door: Door) {
    let reverse = matches!(fault, Fault::ReverseOnly);
    let bindings = if reverse {
        HashMap::from([("anchor".into(), Value::Uuid(uuid::Uuid::from_u128(EXPIRED)))])
    } else {
        params()
    };
    if matches!(door, Door::File) {
        let result = frozen_read(path).and_then(|reader| {
            assert_eq!(reader.route(), ReadRoute::File);
            reader
                .execute(&graph_sql(reverse, "LINKS"), &bindings)
                .map(|_| ())
        });
        assert_corruption(result, fault, door);
        return;
    }
    let db = match Database::open(path) {
        Ok(db) => db,
        Err(error) => {
            assert_corruption(Err(error), fault, door);
            return;
        }
    };
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let result = match door {
        Door::Owner => frozen_read(path).and_then(|reader| {
            assert_eq!(reader.route(), ReadRoute::Owner);
            reader
                .execute(&graph_sql(reverse, "LINKS"), &bindings)
                .map(|_| ())
        }),
        Door::Update => db
            .execute(
                "UPDATE edges SET edge_type = 'RENEWED' WHERE id = $id",
                &HashMap::from([("id".into(), Value::Uuid(uuid::Uuid::from_u128(EDGE)))]),
            )
            .map(|_| ()),
        Door::Export => db.export_snapshot(destination).map(|_| ()),
        Door::File => unreachable!(),
    };
    close(db);
    assert_corruption(result, fault, door);
}
fn control_fault(fault: Fault) {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    for door in [Door::File, Door::Owner, Door::Update, Door::Export] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control-damage.db");
        let db = writer(&path);
        seed(&db, false, false, false);
        close(db);
        let selected = owned(&path, false)
            .into_iter()
            .find(|owned| owned.entry.target == uuid::Uuid::from_u128(EXPIRED))
            .unwrap();
        match fault {
            Fault::ForwardOnly => Database::remove_row_graph_witness_for_test(
                &path,
                "edges",
                selected.owner.row_id,
                true,
            )
            .unwrap(),
            Fault::ReverseOnly => Database::remove_row_graph_witness_for_test(
                &path,
                "edges",
                selected.owner.row_id,
                false,
            )
            .unwrap(),
            Fault::WitnessMismatch => damage_witness(&path, &selected),
            _ => {
                let db = writer(&path);
                let snapshot = db.snapshot();
                let pin = db.pin_snapshot(snapshot).unwrap();
                db.execute(
                    "UPDATE retained SET expires_at = $expires WHERE id = $id",
                    &HashMap::from([
                        ("id".into(), Value::Uuid(uuid::Uuid::from_u128(EXPIRED))),
                        ("expires".into(), Value::Timestamp(-1)),
                    ]),
                )
                .unwrap();
                db.run_pruning_cycle_checked().unwrap();
                if matches!(fault, Fault::WrongOwnerRetirement) {
                    retire_other_endpoint(&db);
                }
                drop(pin);
                close(db);
                for reverse in [false, true] {
                    let physical = owned(&path, reverse);
                    let tombstone = physical
                        .iter()
                        .find(|entry| entry.owner == selected.owner)
                        .expect("the selected exact owner remains physically present");
                    assert!(
                        tombstone.entry.deleted_tx.is_some(),
                        "the selected physical witness is a tombstone"
                    );
                }
                let first = controls(&path);
                assert_eq!(
                    first.len(),
                    if matches!(fault, Fault::WrongOwnerRetirement) {
                        2
                    } else {
                        1
                    },
                    "the selected exact owner really retires before control damage"
                );
                let key = fixture_retirement_key(&selected.owner);
                assert!(first.contains_key(&key));
                let records = controls(&path);
                assert_eq!(
                    records.len(),
                    if matches!(fault, Fault::WrongOwnerRetirement) {
                        2
                    } else {
                        1
                    },
                    "real retirement produces exact-owner control before deliberate damage"
                );
                match fault {
                    Fault::MalformedRetirement => rewrite_control(&path, &key, Some(&[0xff])),
                    Fault::WrongOwnerRetirement => rewrite_control(
                        &path,
                        &key,
                        Some(records.iter().find(|(other, _)| *other != &key).unwrap().1),
                    ),
                    Fault::LiveAndRetired => {
                        inject_live_witness_pair(&path, &selected);
                        for reverse in [false, true] {
                            let physical = owned(&path, reverse);
                            let live = physical
                                .iter()
                                .find(|entry| entry.owner == selected.owner)
                                .expect("the injected exact owner is physically present");
                            assert_eq!(live, &selected, "the fixture contains the live witness");
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
        probe(&path, &directory.path().join("must-refuse.db"), fault, door);
    }
}
// Manager ruling 2026-10-29: fully absent pairs with re-authenticated roots
// are outside the integrity scope of a door that never encounters them.
#[test]
fn one_forward_witness_without_its_reverse_is_typed_corruption() {
    control_fault(Fault::ForwardOnly);
}
#[test]
fn one_reverse_witness_without_its_forward_is_typed_corruption() {
    control_fault(Fault::ReverseOnly);
}
#[test]
fn mismatched_witness_owner_is_typed_corruption() {
    control_fault(Fault::WitnessMismatch);
}
#[test]
fn malformed_retirement_control_is_typed_corruption() {
    control_fault(Fault::MalformedRetirement);
}
#[test]
fn retirement_for_another_exact_owner_is_typed_corruption() {
    control_fault(Fault::WrongOwnerRetirement);
}
#[test]
fn live_witness_and_retirement_for_the_same_owner_is_typed_corruption() {
    control_fault(Fault::LiveAndRetired);
}

fn fresh_statement_hides_expiry_while_old_reader_is_pinned(reverse: bool, owner_route: bool) {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("logical-expiry.db");
    let db = writer(&path);
    seed(&db, reverse, false, false);
    close(db);
    let old = ReadSession::open_with_clock_for_test(
        &path,
        crate::ReadSessionOptions {
            limits: contextdb_core::read_contract::ReadLimits {
                cursor_page_rows: 1,
                ..Default::default()
            },
            ..Default::default()
        },
        Arc::new(crate::local_transport::ManualDeadlineClock::at(0)),
    )
    .unwrap();
    assert_eq!(old.route(), ReadRoute::File);
    assert_eq!(
        old.execute(&graph_sql(reverse, "LINKS"), &params())
            .unwrap()
            .rows(),
        nodes(&[EXPIRED, OTHER])
    );
    let mut cursor = old
        .open_cursor(&graph_sql(reverse, "LINKS"), &params())
        .unwrap();
    assert_eq!(cursor.first_page().rows(), nodes(&[EXPIRED]));
    assert!(
        cursor.first_page().has_more(),
        "cursor must retain an unread page"
    );
    let db = writer(&path);
    db.execute(
        "UPDATE retained SET expires_at = $expires WHERE id = $id",
        &HashMap::from([
            ("id".into(), Value::Uuid(uuid::Uuid::from_u128(EXPIRED))),
            ("expires".into(), Value::Timestamp(-1)),
        ]),
    )
    .unwrap();
    db.run_pruning_cycle_checked().unwrap();
    assert_eq!(
        cursor.first_page().rows(),
        nodes(&[EXPIRED]),
        "the pre-expiry cursor retains its first page"
    );
    let db = if owner_route {
        Some(db)
    } else {
        close(db);
        None
    };
    let fresh = ReadSession::open_with_clock_for_test(
        &path,
        Default::default(),
        Arc::new(crate::local_transport::ManualDeadlineClock::at(0)),
    )
    .unwrap();
    assert_eq!(
        fresh.route(),
        if owner_route {
            ReadRoute::Owner
        } else {
            ReadRoute::File
        }
    );
    assert_eq!(
        fresh
            .execute(&graph_sql(reverse, "LINKS"), &params())
            .unwrap()
            .rows(),
        nodes(&[OTHER]),
        "fresh statement hides the expired binding while old reader pins it"
    );
    assert_eq!(
        old.execute(&graph_sql(reverse, "LINKS"), &params())
            .unwrap()
            .rows(),
        nodes(&[OTHER]),
        "a new statement on the old session sees current data"
    );
    let page = cursor.fetch(None).unwrap();
    assert_eq!(
        page.rows(),
        nodes(&[OTHER]),
        "the open cursor finishes its old snapshot"
    );
    cursor.close().unwrap();
    drop(fresh);
    drop(old);
    if let Some(db) = db {
        close(db);
    }
}
#[test]
fn fresh_owner_forward_hides_expiry_while_reader_pins() {
    fresh_statement_hides_expiry_while_old_reader_is_pinned(false, true);
}
#[test]
fn fresh_owner_reverse_hides_expiry_while_reader_pins() {
    fresh_statement_hides_expiry_while_old_reader_is_pinned(true, true);
}
#[test]
fn fresh_file_forward_hides_expiry_while_reader_pins() {
    fresh_statement_hides_expiry_while_old_reader_is_pinned(false, false);
}
#[test]
fn fresh_file_reverse_hides_expiry_while_reader_pins() {
    fresh_statement_hides_expiry_while_old_reader_is_pinned(true, false);
}

fn replay_while_old_reader_pins(use_schema_rebuild: bool) {
    use crate::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange};
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("retired-replay.db");
    let db = writer(&path);
    seed(&db, false, false, false);
    let mut replay = db.changes_since(Lsn(0)).unwrap();
    replay.rows.retain(|row| {
        row.table == "edges"
            && row.values.get("id") == Some(&Value::Uuid(uuid::Uuid::from_u128(EDGE)))
    });
    assert_eq!(
        replay.rows.len(),
        1,
        "capture the original exact owner row for replay"
    );
    replay.ddl.clear();
    replay.ddl_lsn.clear();
    replay.edges.clear();
    replay.vectors.clear();
    let snapshot = db.snapshot();
    let pin = db.pin_snapshot(snapshot).unwrap();
    let persistence = db.replay_guard_persistence_for_test();
    let image_memory = crate::read_image_memory::ImageMemory::new(None).unwrap();
    let _scope = image_memory.enter();
    db.execute(
        "UPDATE retained SET expires_at = $expires WHERE id = $id",
        &HashMap::from([
            ("id".into(), Value::Uuid(uuid::Uuid::from_u128(EXPIRED))),
            ("expires".into(), Value::Timestamp(-1)),
        ]),
    )
    .unwrap();
    db.run_pruning_cycle_checked().unwrap();
    assert_eq!(
        db.execute_at_snapshot(&graph_sql(false, "LINKS"), &params(), snapshot)
            .unwrap()
            .rows(),
        nodes(&[EXPIRED, OTHER])
    );
    assert_eq!(
        db.execute(&graph_sql(false, "LINKS"), &params())
            .unwrap()
            .rows(),
        nodes(&[OTHER])
    );
    let read_controls = || {
        persistence
            .dump_config_raw()
            .unwrap()
            .into_iter()
            .filter(|(key, _)| is_retirement_key(key))
            .collect::<BTreeMap<_, _>>()
    };
    let read_owned = |reverse: bool| {
        persistence
            .with_db(|inner| {
                let read = inner.begin_read().map_err(RedbPersistence::storage_error)?;
                let table = read
                    .open_table(if reverse {
                        row_graph::REVERSE
                    } else {
                        row_graph::FORWARD
                    })
                    .map_err(RedbPersistence::storage_error)?;
                let mut result = Vec::new();
                for item in table.iter().map_err(RedbPersistence::storage_error)? {
                    let (_, value) = item.map_err(RedbPersistence::storage_error)?;
                    let (entry, _): (OwnedAdjEntry, usize) = bincode::serde::decode_from_slice(
                        value.value(),
                        bincode::config::standard(),
                    )
                    .unwrap();
                    result.push(entry);
                }
                Ok(result)
            })
            .unwrap()
    };
    let before_control = read_controls();
    assert_eq!(
        before_control.len(),
        1,
        "expiry publishes exact retirement while pinned"
    );
    let forward = read_owned(false);
    let reverse = read_owned(true);
    let select = |entries: Vec<OwnedAdjEntry>| {
        entries
            .into_iter()
            .find(|e| {
                e.entry.source == uuid::Uuid::from_u128(SURVIVOR)
                    && e.entry.target == uuid::Uuid::from_u128(EXPIRED)
            })
            .expect("pinned pair remains physically present")
    };
    let before = select(forward);
    assert_eq!(
        select(reverse),
        before,
        "paired tombstones match before replay"
    );
    assert!(
        before.entry.deleted_tx.is_some(),
        "the old binding is logically retired"
    );
    let changes = if use_schema_rebuild {
        ChangeSet {
            ddl: vec![DdlChange::AlterTable {
                name: "edges".into(),
                columns: vec![("note".into(), "TEXT".into())],
                constraints: Vec::new(),
                foreign_keys: Vec::new(),
                composite_foreign_keys: Vec::new(),
                composite_unique: Vec::new(),
            }],
            ddl_lsn: vec![Lsn(100_000)],
            ..ChangeSet::default()
        }
    } else {
        replay
    };
    db.apply_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
    )
    .expect("replay or received schema rebuild accepts the already retired exact owner");
    assert_eq!(
        db.execute_at_snapshot(&graph_sql(false, "LINKS"), &params(), snapshot)
            .unwrap()
            .rows(),
        nodes(&[EXPIRED, OTHER]),
        "pinned snapshot survives replay"
    );
    assert_eq!(
        db.execute(&graph_sql(false, "LINKS"), &params())
            .unwrap()
            .rows(),
        nodes(&[OTHER]),
        "fresh view remains retired"
    );
    assert_eq!(
        read_controls(),
        before_control,
        "replay preserves exact retirement control"
    );
    assert_eq!(
        select(read_owned(false)),
        before,
        "replay preserves the forward tombstone"
    );
    assert_eq!(
        select(read_owned(true)),
        before,
        "replay preserves the reverse tombstone"
    );
    drop(pin);
    close(db);
}

#[test]
fn replay_of_old_owner_keeps_pinned_retirement_and_paired_tombstones() {
    replay_while_old_reader_pins(false);
}

#[test]
fn received_schema_rebuild_keeps_pinned_retirement_and_paired_tombstones() {
    replay_while_old_reader_pins(true);
}
