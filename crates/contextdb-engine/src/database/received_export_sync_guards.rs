// Independent public export durability-mode boundary. No work/time ceiling.
#[cfg(feature = "test-seams")]
mod received_export_sync_guards {
    use super::*;
    use crate::persistence::read_persistence_test_scaffold::{
        WriterFileIoKindForTest as Kind, observe_writer_file_io_for_test,
    };
    use serial_test::serial;
    #[derive(Debug, Default, Clone, Copy)]
    struct Io {
        writes: u64,
        sync_begin: u64,
        sync_ok: u64,
        sync_failed: u64,
    }
    fn observe<T>(operation: impl FnOnce() -> T) -> (T, Io) {
        let receipt = Arc::new(std::sync::Mutex::new(Io::default()));
        let observed = receipt.clone();
        let value = observe_writer_file_io_for_test(
            Arc::new(move |event| {
                let mut io = observed.lock().unwrap();
                match event.kind {
                    Kind::WriteEnd if event.success == Some(true) => io.writes += 1,
                    Kind::SyncBegin => io.sync_begin += 1,
                    Kind::SyncEnd if event.success == Some(true) => io.sync_ok += 1,
                    Kind::SyncEnd => io.sync_failed += 1,
                    _ => {}
                }
            }),
            operation,
        );
        let io = *receipt.lock().unwrap();
        (value, io)
    }
    fn sql(db: &Database, statement: &str) {
        db.execute(statement, &HashMap::new()).unwrap();
    }
    fn live_commit(io: Io) {
        assert!(io.writes > 0, "real data reaches the live file");
        assert!(
            io.sync_ok > 0,
            "every ordinary live commit keeps physical data sync"
        );
        assert_eq!(io.sync_failed, 0);
        assert_eq!(io.sync_begin, io.sync_ok);
    }
    fn open(path: &std::path::Path) -> Database {
        let mut configuration = crate::read_session::DatabaseOpenOptions::default();
        configuration.maintenance_policy = MaintenancePolicy::CallerDriven;
        configuration.owner_reads.enabled = false;
        let db = Database::open_with_options(path, configuration).unwrap();
        assert!(matches!(&db.tx_mgr, DatabaseTransactionHost::Legacy(_)));
        db
    }
    fn read(db: &Database, id: i64, snapshot: SnapshotId) -> Option<String> {
        db.point_lookup("exported", "id", &Value::Int64(id), snapshot)
            .unwrap()
            .map(|row| match &row.values["body"] {
                Value::Text(body) => body.clone(),
                other => panic!("text expected: {other:?}"),
            })
    }
    #[test]
    #[serial]
    fn public_export_skips_private_intermediate_sync_and_keeps_both_live_doors_durable() {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("source.db");
        let artifact = root.path().join("snapshot.db");
        let source = open(&path);
        sql(
            &source,
            "CREATE TABLE exported (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL",
        );
        let ((), source_io) = observe(|| sql(&source, "INSERT INTO exported VALUES (1, 'old')"));
        live_commit(source_io);
        let old_at = source.current_lsn();
        sql(&source, "UPDATE exported SET body = 'current' WHERE id = 1");
        sql(&source, "INSERT INTO exported VALUES (2, 'second')");
        let captured = source.current_lsn();
        let (report, export_io) = observe(|| source.export_snapshot(&artifact));
        let report = report.unwrap();
        assert_eq!(report.snapshot_lsn, captured);
        assert!(
            export_io.writes > 0,
            "public export performs actual private file writes"
        );
        assert_eq!(
            (
                export_io.sync_begin,
                export_io.sync_ok,
                export_io.sync_failed
            ),
            (0, 0, 0),
            "unpublished artifact construction/metadata/compaction rely on the final close+sync_all publication barrier"
        );
        let before = std::fs::read(&artifact).unwrap();
        let refusal = source.export_snapshot(&artifact).unwrap_err();
        assert!(matches!(refusal, Error::ExportDestinationExists { path } if path == artifact));
        assert_eq!(
            std::fs::read(&artifact).unwrap(),
            before,
            "existing destination is never replaced"
        );
        let ((), source_after) =
            observe(|| sql(&source, "INSERT INTO exported VALUES (3, 'source-only')"));
        live_commit(source_after);
        source.close().unwrap();
        drop(source);
        let destination = open(&artifact);
        assert_eq!(
            read(&destination, 1, destination.snapshot()),
            Some("current".into())
        );
        assert_eq!(
            read(&destination, 2, destination.snapshot()),
            Some("second".into())
        );
        assert_eq!(read(&destination, 3, destination.snapshot()), None);
        assert_eq!(
            read(&destination, 1, destination.snapshot_at(old_at)),
            Some("old".into())
        );
        let ((), destination_io) = observe(|| {
            sql(
                &destination,
                "INSERT INTO exported VALUES (3, 'destination-live')",
            )
        });
        live_commit(destination_io);
        destination.close().unwrap();
        drop(destination);
        let destination = open(&artifact);
        assert_eq!(
            read(&destination, 3, destination.snapshot()),
            Some("destination-live".into())
        );
        assert_eq!(
            read(&destination, 1, destination.snapshot_at(old_at)),
            Some("old".into())
        );
        destination.close().unwrap();
        drop(destination);
        let source = open(&path);
        assert_eq!(
            read(&source, 3, source.snapshot()),
            Some("source-only".into())
        );
        source.close().unwrap();
    }
}
