// Test observations pin the published durable root and release each admitted window.
mod durable_observation {
    use super::*;
    use crate::persistence::writer_source::{WriterCursorFamily};
    use std::ops::Bound;

    pub(super) fn root(db: &Database) -> crate::persistence::WriterSnapshotHandle {
        db.capture_writer_statement()
            .unwrap()
            .1
            .expect("file-backed published root")
    }

    #[allow(dead_code, reason = "KEEP-SHARED: shared test helper; other test targets or modules use it")]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(super) fn version_count(db: &Database, table: &str) -> usize {
        let root = root(db);
        let source_fixture_reader_1 = db
            .declared_reader_memory()
            .expect("admit the source fixture reader");

        let mut cursor = root
            .cursor(
                crate::persistence::writer_source::SourceWindowOwner::Read(source_fixture_reader_1),
                &RedbPersistence::rel_table_name(table),
                WriterCursorFamily::Bytes,
                Bound::Unbounded,
                Bound::Unbounded,
                false,
            )
            .unwrap();
        let mut count = 0;
        // Count physical versions without decoding unrelated wide row bodies.
        while let Some(_window) = cursor.next(|_, _| Ok(())).unwrap() {
            count += 1;
        }
        count
    }

    #[allow(dead_code, reason = "KEEP-SHARED: shared test helper; other test targets or modules use it")]
    pub(super) fn rows(db: &Database, table: &str) -> Vec<VersionedRow> {
        let root = root(db);
        let source_fixture_reader_2 = db
            .declared_reader_memory()
            .expect("admit the source fixture reader");

        let mut cursor = root
            .cursor(
                crate::persistence::writer_source::SourceWindowOwner::Read(source_fixture_reader_2),
                &RedbPersistence::rel_table_name(table),
                WriterCursorFamily::Bytes,
                Bound::Unbounded,
                Bound::Unbounded,
                false,
            )
            .unwrap();
        let mut rows = Vec::new();
        while let Some(window) = cursor.next_row().unwrap() {
            rows.push(window.value.clone());
        }
        rows
    }

    #[allow(dead_code, reason = "KEEP-SHARED: shared test helper; other test targets or modules use it")]
    pub(super) fn changes(db: &Database) -> Vec<ChangeLogEntry> {
        let root = root(db);
        let source_fixture_reader_3 = db
            .declared_reader_memory()
            .expect("admit the source fixture reader");

        let mut cursor = root
            .change_log_cursor(
                crate::persistence::writer_source::SourceWindowOwner::Read(source_fixture_reader_3),
                Lsn(0),
                db.current_lsn(),
            )
            .unwrap();
        let mut changes = Vec::new();
        while let Some(window) = cursor.next_change().unwrap() {
            changes.push(window.value.clone());
        }
        changes
    }

    #[allow(dead_code, reason = "KEEP-SHARED: shared test helper; other test targets or modules use it")]
    pub(super) fn table_index(db: &Database, table: &str) -> Vec<(Lsn, RowId)> {
        let root = root(db);
        let source_fixture_reader_4 = db
            .declared_reader_memory()
            .expect("admit the source fixture reader");

        let mut cursor = root
            .table_change_cursor(
                crate::persistence::writer_source::SourceWindowOwner::Read(source_fixture_reader_4),
                table,
                Lsn(0),
                db.current_lsn(),
            )
            .unwrap();
        let mut index = Vec::new();
        while let Some(window) = cursor.next_table_change().unwrap() {
            if let ChangeLogEntry::RowInsert { lsn, row_id, .. }
            | ChangeLogEntry::RowDelete { lsn, row_id, .. } = window.value
            {
                index.push((lsn, row_id));
            }
        }
        index
    }
}
