//! An authenticated received table drop retires its dropped table's
//! change-log records from the committed identity in the same transaction
//! it removes them physically, so a strict committed-history read never
//! rejects an honestly installed image.
//!
//! `flush_received_schema_stage` removes a structurally dropped table's
//! `CHANGE_LOG_TABLE` records but used to leave their coordinates in the
//! identity mutation batch untouched. The reader's history walk is now a
//! lockstep proof against every committed coordinate (see
//! `tests/read/history_answer_is_complete_and_in_author_order.rs`), so a
//! committed coordinate whose physical record is gone is typed damage --
//! meaning a perfectly legitimate received drop used to leave the store
//! unable to answer a history request at all.
//!
//! `flush_received_schema_stage` only runs for an AUTHENTICATED received
//! schema unit -- `Database::apply_changes` alone (the door
//! `a_received_drop_of_an_edge_table_takes_its_links.rs` drives) takes the
//! ordinary local-DDL path instead, never reaching it. This test drives the
//! real authenticated door instead, the same way
//! `tests/read/reader_continuity_contract.rs` does: an in-memory "author"
//! database performs the DROP itself, `outbound_ddl_provenance_for_test`
//! derives the wire provenance a real source would attach, and the
//! receiving store installs it through `apply_received_schema_for_test`.

use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::{Incarnation, Lsn, TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::direct_file_reader::test_seams::{
    DirectImageMetadataKind, DirectImageState, DirectMetadataBody, DirectMetadataRequest,
    DirectReaderConfig, open_for_test,
};
use contextdb_engine::protocol::WireDdlProvenance;
use contextdb_engine::sync_types::ChangeSet;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

const TENANT: &str = "received-drop-retires-history";
const SOURCE_INCARNATION: Incarnation = Incarnation(1);

struct FixedClock;

impl DeadlineClock for FixedClock {
    fn now_ms(&self) -> u64 {
        1
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn config() -> DirectReaderConfig {
    DirectReaderConfig::new(
        ReadLimits {
            result_rows: 1_000,
            result_bytes: 64 * 1024 * 1024,
            work: 1_000_000,
            active_ms: 60_000,
            memory: 1024 * 1024 * 1024,
            cursor_page_rows: 100,
            cursor_page_bytes: 2 * 1024 * 1024,
            cursor_idle_ms: 10_000,
            cursor_lifetime_ms: 20_000,
        },
        Arc::new(FixedClock),
    )
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

/// Open the store as a strict committed-file reader and ask for its complete
/// history. A dangling identity would surface here as `StoreCorrupted`,
/// naming a committed change-log record the store no longer holds.
fn committed_row_change_count(path: &std::path::Path) -> usize {
    let reader = open_for_test(path, config()).expect("open the receiving store as a reader");
    match reader
        .metadata(DirectMetadataRequest::ImageState {
            kind: DirectImageMetadataKind::ChangeLog,
        })
        .expect(
            "a strict committed-history read must answer an honestly installed image, \
             not refuse it as damage",
        )
        .body
    {
        DirectMetadataBody::ImageState {
            state: DirectImageState::ChangeLog(changes),
        } => changes.rows.len(),
        other => panic!("change history answers as change-log state: {other:?}"),
    }
}

/// Derive the wire provenance a real source would attach to `changes` from
/// the database that actually authored it, and apply the resulting unit to
/// `receiver` through the same authenticated, dependency-complete door a
/// real sync client uses.
fn apply_received(author: &Database, receiver: &Database, changes: ChangeSet) {
    let identity = {
        static AUTHOR: std::sync::OnceLock<contextdb_engine::identity::FabricIdentity> =
            std::sync::OnceLock::new();
        AUTHOR.get_or_init(contextdb_engine::identity::FabricIdentity::generate)
    };
    let node_id = identity.node_id();
    let fixture_operation_reader_1 = author
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let ddl_provenance: Vec<WireDdlProvenance> = author
        .outbound_ddl_provenance_for_test(
            &changes,
            &TenantId::from(TENANT),
            &node_id,
            SOURCE_INCARNATION,
            &|bytes| Ok(identity.sign_for_test(bytes)),
            fixture_operation_reader_1.clone(),
        )
        .expect("the authoring source produces canonical DDL provenance for this schema unit");
    let fixture_operation_reader_2 = receiver
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_2 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_2.clone());

    receiver
        .apply_received_schema_for_test(
            changes,
            ddl_provenance,
            TenantId::from(TENANT),
            &node_id,
            SOURCE_INCARNATION,
            &mut fixture_operation_copy_2,
            fixture_operation_reader_2.clone(),
        )
        .expect("an authenticated, dependency-complete received schema unit applies");
}

#[test]
fn a_received_drop_and_recreate_leaves_readable_committed_history() {
    let directory = tempfile::TempDir::new().expect("task-scoped sync directory");
    let path = directory.path().join("received-drop-retires-history.db");

    let author = Database::open_memory();
    author
        .execute(
            "CREATE TABLE widgets (id UUID PRIMARY KEY, body TEXT)",
            &empty(),
        )
        .expect("the author declares the table it will later drop and recreate");
    let create_changes = author
        .changes_since(Lsn(0))
        .expect("read or construct the declared fixture result");
    assert_eq!(create_changes.ddl.len(), 1, "one authored CREATE TABLE");

    {
        let database = Database::open(&path).expect("claim the receiving store");
        apply_received(&author, &database, create_changes);
        for row in 0..4u128 {
            database
                .execute(
                    "INSERT INTO widgets (id, body) VALUES ($id, 'v1')",
                    &HashMap::from([(
                        "id".to_string(),
                        Value::Uuid(Uuid::from_u128(0x77_00 + row)),
                    )]),
                )
                .expect("commit an ordinary row before the drop arrives");
        }
        database
            .close()
            .expect("release the writer before reading back its pre-drop history");
    }

    // Every one of those inserts is a committed history record before the
    // drop arrives -- multi-entry history is what a naive physical-only
    // removal would leave dangling in the identity.
    assert_eq!(
        committed_row_change_count(&path),
        4,
        "the pre-drop history has to be there, or this journey proves nothing"
    );

    let since_create = author.current_lsn();
    author
        .execute("DROP TABLE widgets", &empty())
        .expect("the author drops the table its earlier CREATE declared");
    let drop_changes = author
        .changes_since(since_create)
        .expect("read or construct the declared fixture result");
    assert_eq!(drop_changes.ddl.len(), 1, "one authored DROP TABLE");

    let since_drop = author.current_lsn();
    author
        .execute(
            "CREATE TABLE widgets (id UUID PRIMARY KEY, body TEXT)",
            &empty(),
        )
        .expect("the author recreates the table under its old name");
    let recreate_changes = author
        .changes_since(since_drop)
        .expect("read or construct the declared fixture result");
    assert_eq!(
        recreate_changes.ddl.len(),
        1,
        "one authored recreate CREATE TABLE"
    );

    {
        let database = Database::open(&path).expect("reopen to receive the drop and recreate");
        apply_received(&author, &database, drop_changes);
        apply_received(&author, &database, recreate_changes);
        database
            .execute(
                "INSERT INTO widgets (id, body) VALUES ($id, 'v2')",
                &HashMap::from([("id".to_string(), Value::Uuid(Uuid::from_u128(0x77_10)))]),
            )
            .expect("commit a row into the recreated table");
        database
            .close()
            .expect("release the writer after the drop and recreate");
    }

    // The drop retired the four pre-drop rows' history identity along with
    // their physical records; the recreate's own insert is the only
    // survivor -- and the read answers instead of refusing.
    assert_eq!(
        committed_row_change_count(&path),
        1,
        "a strict history read must answer the honestly installed image, not refuse it"
    );

    // The identity mutation committed to the same Redb transaction as the
    // physical removal, so this is not an in-memory-only agreement: it
    // survives a restart exactly as it does the first read.
    assert_eq!(
        committed_row_change_count(&path),
        1,
        "the readable history survives a restart, exactly as the drop and recreate left it"
    );
}
