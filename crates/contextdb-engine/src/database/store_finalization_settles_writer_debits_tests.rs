//! Store finalization settles every writer-counter debit the store took.
//!
//! A store's writer counter carries its own fixed terminal debit until the
//! store's final native owner keeper, the finalization control refund,
//! releases it after physical free. Every other store control (pending
//! release, vector and non-vector memory, history, staged postings, the
//! vector accountant and the store accountant) must refund exactly what it
//! charged before that, whether or not a bounded reader read the store, and
//! whether or not the store was closed first. A refund below the terminal
//! debit panics in finalization; a missing refund leaves process writer
//! bytes behind.
use super::*;
use crate::local_transport::ManualDeadlineClock;

#[derive(Clone, Copy)]
enum Construction {
    /// The construction every production open uses.
    Public,
    /// The engine's own writer-owned host construction.
    Owned,
}

#[derive(Clone, Copy)]
struct Lifecycle {
    construction: Construction,
    vector_table: bool,
    bounded_reads: bool,
    close: bool,
}

/// Open a store, fill one table, optionally read it through bounded execute
/// and cursor on another thread, optionally close it, and drop its last
/// handle. Runs on a fresh thread; a panic anywhere, finalization included,
/// is returned with its message.
fn finalize(lifecycle: Lifecycle) -> std::result::Result<(), String> {
    std::thread::spawn(move || {
        let directory = tempfile::tempdir().expect("store directory");
        let path = directory.path().join("finalization.db");
        let options = DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        };
        let db = Arc::new(match lifecycle.construction {
            Construction::Public => Database::open_with_options(&path, options),
            Construction::Owned => Database::open_owned_for_test(&path, options),
        }
        .expect("open the store"));
        let (table, query) = if lifecycle.vector_table {
            db.execute(
                "CREATE TABLE vectors (id INTEGER PRIMARY KEY, embedding VECTOR(2) SEARCH_MODE EXACT) SYNC OFF",
                &HashMap::new(),
            )
            .expect("declare the vector table");
            (
                "INSERT INTO vectors VALUES (1, '[1,0]')",
                "SELECT id FROM vectors ORDER BY embedding <=> $probe LIMIT 1",
            )
        } else {
            db.execute(
                "CREATE TABLE plain (id INTEGER PRIMARY KEY) SYNC OFF",
                &HashMap::new(),
            )
            .expect("declare the plain table");
            ("INSERT INTO plain VALUES (1)", "SELECT id FROM plain")
        };
        db.execute(table, &HashMap::new()).expect("write one row");
        if lifecycle.bounded_reads {
            bounded_reads(&db, query);
        }
        if lifecycle.close {
            db.close().expect("close the store");
        }
        drop(db);
    })
    .join()
    .map_err(|payload| {
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|text| (*text).to_string()))
            .unwrap_or_default()
    })
}

#[cfg(feature = "test-seams")]
fn bounded_reads(db: &Arc<Database>, query: &'static str) {
    use crate::executor::bounded_read_test_support as bounded;
    use contextdb_core::read_contract::ReadLimits;
    let reader = Arc::clone(db);
    std::thread::spawn(move || {
        let request = bounded::BoundedReadRequest::new(
            query,
            HashMap::from([("probe".to_owned(), Value::Vector(vec![1.0, 0.0]))]),
            ReadLimits::default(),
            Arc::new(ManualDeadlineClock::at(0)),
        );
        let result = bounded::execute(&reader, &request).expect("bounded read answers");
        assert_eq!(result.result.rows(), vec![vec![Value::Int64(1)]]);
        let cursor = bounded::open_cursor(reader, &request).expect("bounded cursor answers");
        assert_eq!(cursor.first_page.rows(), vec![vec![Value::Int64(1)]]);
        assert!(!cursor.first_page.has_more());
    })
    .join()
    .expect("both bounded reads finish before the store closes");
}

#[cfg(not(feature = "test-seams"))]
fn bounded_reads(_: &Arc<Database>, _: &'static str) {
    unreachable!("bounded-read rows run only with the test seams")
}

/// The lifecycle finishes, and the process writer bytes return to exactly
/// their value before the store opened.
fn assert_finalization_settles(lifecycle: Lifecycle) {
    let before = crate::memory_accounting::process_aggregate_bytes_for_test();
    let finished = finalize(lifecycle);
    assert_eq!(
        finished,
        Ok(()),
        "store finalization must settle every writer-counter debit without panicking: \
         the finalization control refund is the store's final native owner keeper, and \
         every earlier store control must refund exactly what it charged"
    );
    assert_eq!(
        crate::memory_accounting::process_aggregate_bytes_for_test(),
        before,
        "every writer byte the store took is settled once its last handle drops"
    );
}

#[cfg(feature = "test-seams")]
#[test]
#[serial_test::serial]
fn an_owned_store_with_a_vector_table_finalizes_after_bounded_reads_and_close() {
    assert_finalization_settles(Lifecycle {
        construction: Construction::Owned,
        vector_table: true,
        bounded_reads: true,
        close: true,
    });
}

#[test]
#[serial_test::serial]
fn an_owned_store_with_a_vector_table_finalizes_when_its_last_handle_drops() {
    assert_finalization_settles(Lifecycle {
        construction: Construction::Owned,
        vector_table: true,
        bounded_reads: false,
        close: false,
    });
}

#[cfg(feature = "test-seams")]
#[test]
#[serial_test::serial]
fn a_public_store_with_a_vector_table_finalizes_after_bounded_reads_and_close() {
    assert_finalization_settles(Lifecycle {
        construction: Construction::Public,
        vector_table: true,
        bounded_reads: true,
        close: true,
    });
}

#[test]
#[serial_test::serial]
fn an_owned_store_without_a_vector_table_finalizes_after_close() {
    assert_finalization_settles(Lifecycle {
        construction: Construction::Owned,
        vector_table: false,
        bounded_reads: false,
        close: true,
    });
}
