//! A file reader with a small read limit opens and answers, whatever larger
//! session ceiling it also declares.
//!
//! Promise: a reader's page cache and the index that tracks it are paid from
//! the session ceiling that bounds its loaded state, never from the per-read
//! limit alone. A session that declares a 256 MiB ceiling while keeping the
//! shipped 16 MiB per-read limit (or 64 MiB) opens its file reader and
//! answers, exactly as it did before the cache share was derived from the
//! declared budget, and so do sessions whose ceiling and read limit are both
//! 16 MiB or both 64 MiB. A budget too small to hold the committed read image
//! at all is refused at open with the typed memory-budget refusal, never
//! opened past its limit.

use super::reader_proof_support as fixture;
use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, ReadLimits};
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

const MIB: usize = 1024 * 1024;

fn seed(path: &Path) {
    let db = Database::open(path).unwrap();
    db.execute(
        "CREATE TABLE small (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    for row in 0..200i64 {
        db.execute(
            "INSERT INTO small VALUES ($id, $payload)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(row)),
                (
                    "payload".to_owned(),
                    Value::Text(format!("{row:08x}").repeat(64)),
                ),
            ]),
        )
        .unwrap();
    }
    db.close().unwrap();
}

fn options(ceiling: usize, read_limit: usize) -> ReadSessionOptions {
    ReadSessionOptions {
        memory_limit: Some(ceiling),
        limits: ReadLimits {
            memory: read_limit as u64,
            ..fixture::limits()
        },
        ..Default::default()
    }
}

/// Sessions whose ceiling equals their read limit, and sessions whose 256 MiB
/// ceiling sits above a 16 MiB or 64 MiB read limit, all open their file
/// reader and answer.
#[test]
fn a_reader_opens_and_answers_when_its_session_ceiling_is_at_or_above_its_read_limit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("small-limits.db");
    seed(&path);
    for (ceiling, read_limit) in [
        (16 * MIB, 16 * MIB),
        (64 * MIB, 64 * MIB),
        (256 * MIB, 16 * MIB),
        (256 * MIB, 64 * MIB),
    ] {
        let clock: Arc<dyn DeadlineClock> = Arc::new(ManualDeadlineClock::default());
        let session = ReadSession::open_with_observations_for_test(
            &path,
            options(ceiling, read_limit),
            clock,
            None,
            None,
        )
        .unwrap_or_else(|refusal| {
            panic!(
                "a reader with a {ceiling}-byte session ceiling and a {read_limit}-byte read \
                 limit must open its file reader: {refusal:?}"
            )
        });
        let rows = session
            .execute(
                "SELECT id FROM small WHERE id >= 10 AND id < 20 ORDER BY id",
                &HashMap::new(),
            )
            .unwrap_or_else(|refusal| {
                panic!(
                    "a reader with a {ceiling}-byte session ceiling and a {read_limit}-byte \
                     read limit must answer: {refusal:?}"
                )
            });
        assert_eq!(
            rows.rows(),
            (10..20i64)
                .map(|id| vec![Value::Int64(id)])
                .collect::<Vec<_>>()
        );
    }
}

/// A budget too small for the read image is refused at open with the typed
/// memory-budget refusal: the direct-read subsystem, the image hydration that
/// needed the memory, the bytes it asked for against the bytes still free,
/// the declared budget, and a hint. A caller can tell a reader that is too
/// small from any other failure without reading its message.
#[test]
fn a_budget_too_small_for_the_read_image_is_still_refused_at_open() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("too-small.db");
    seed(&path);
    let fixed = ReadSessionOptions::default()
        .read_cache_share_status()
        .fixed_reservation_bytes;
    assert!(fixed > 1);
    for budget in [fixed / 2, fixed - 1] {
        let clock: Arc<dyn DeadlineClock> = Arc::new(ManualDeadlineClock::default());
        let refusal = match ReadSession::open_with_observations_for_test(
            &path,
            options(budget, budget),
            clock,
            None,
            None,
        ) {
            Ok(_) => panic!("a {budget}-byte reader cannot hold the committed read image"),
            Err(refusal) => refusal,
        };
        match refusal {
            contextdb_core::Error::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            } => {
                assert_eq!(AsRef::<str>::as_ref(&subsystem), "direct_read");
                assert_eq!(AsRef::<str>::as_ref(&operation), "hydrate_owned_image");
                assert_eq!(
                    budget_limit_bytes, budget,
                    "the refusal names the reader's declared budget"
                );
                assert!(
                    available_bytes <= budget && requested_bytes > available_bytes,
                    "the hydration asked for more than the budget still had free: requested \
                     {requested_bytes}, available {available_bytes} of {budget}"
                );
                assert!(
                    !AsRef::<str>::as_ref(&hint).is_empty(),
                    "the refusal tells the caller what to do"
                );
            }
            other => panic!(
                "a {budget}-byte reader is refused with the typed Error::MemoryBudgetExceeded, \
                 not {other:?}"
            ),
        }
    }
}
