use crate::{Database, ReadSession, ReadSessionOptions};
use std::collections::HashMap;

fn reader(budget: usize, share: Option<u8>) -> (tempfile::TempDir, ReadSession) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cache-policy.db");
    let writer = Database::open(&path).unwrap();
    writer
        .execute(
            "CREATE TABLE entries (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .unwrap();
    writer
        .execute("INSERT INTO entries VALUES (1)", &HashMap::new())
        .unwrap();
    writer.close().unwrap();
    let reader = ReadSession::open_with_options(
        &path,
        ReadSessionOptions {
            memory_limit: Some(budget),
            read_cache_share: share,
            ..Default::default()
        },
    )
    .unwrap();
    (directory, reader)
}

#[test]
fn a_zero_share_discloses_its_prepaid_index_inside_the_reader_budget() {
    let budget = 1024 * 1024 * 1024;
    let (_directory, reader) = reader(budget, Some(0));
    let status = reader.read_cache_share_status().unwrap();
    assert_eq!(status.declared, Some(0));
    assert_eq!(status.effective_bytes, 0);
    let index = status
        .prepaid_index_bytes
        .expect("file cache index inspected");
    assert!(index > 0);
    assert!(
        index < budget * 3 / 100,
        "prepaid index {index} must remain below three percent of budget {budget}"
    );
    reader.set_read_cache_share(Some(40)).unwrap();
    assert_eq!(
        reader
            .read_cache_share_status()
            .unwrap()
            .prepaid_index_bytes,
        Some(index)
    );
    reader.set_read_cache_share(Some(0)).unwrap();
    assert_eq!(
        reader
            .read_cache_share_status()
            .unwrap()
            .prepaid_index_bytes,
        Some(index)
    );
}

#[test]
fn a_shrink_keeps_a_detached_page_paid_until_its_final_cross_thread_free() {
    let (_directory, reader) = reader(64 * 1024 * 1024, Some(40));
    let held = reader.file_cached_header_for_test();
    let (payer, _, bytes) = held.payment();
    assert_eq!(payer, "cache");
    reader.set_read_cache_share(Some(0)).unwrap();
    let before = reader.charged_bytes().unwrap();
    assert!(
        reader
            .read_cache_share_status()
            .unwrap()
            .held_page_bytes
            .unwrap()
            >= bytes
    );
    std::thread::spawn(move || drop(held)).join().unwrap();
    let after = reader.charged_bytes().unwrap();
    assert_eq!(
        before - after,
        bytes as u64,
        "only the physically freed backing is refunded"
    );
}

#[test]
fn refused_cache_growth_keeps_the_old_declaration_capacity_and_charge() {
    let (_directory, reader) = reader(64 * 1024 * 1024, Some(2));
    let before = reader.read_cache_share_status().unwrap();
    let charge = reader.charged_bytes().unwrap();
    let error = reader.set_read_cache_share(Some(100)).unwrap_err();
    assert!(
        matches!(&error, contextdb_core::Error::MemoryBudgetExceeded { .. }),
        "growth must return its original typed refusal: {error:?}"
    );
    drop(error);
    assert_eq!(reader.options().read_cache_share, Some(2));
    assert_eq!(
        reader.read_cache_share_status().unwrap().effective_bytes,
        before.effective_bytes
    );
    assert_eq!(reader.charged_bytes().unwrap(), charge);
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn two_readers_share_the_declared_cache_through_capacity_changes() {
    let (_directory, reader) = reader(64 * 1024 * 1024, Some(2));
    let reader = std::sync::Arc::new(reader);
    let ready = std::sync::Arc::new(std::sync::Barrier::new(3));
    let done = std::sync::Arc::new(std::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let reader = reader.clone();
        let ready = ready.clone();
        let done = done.clone();
        workers.push(std::thread::spawn(move || {
            let mut answers = Vec::with_capacity(4);
            for _ in 0..4 {
                ready.wait();
                let answer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    reader.execute("SELECT id FROM entries ORDER BY id", &HashMap::new())
                }));
                answers.push(matches!(answer, Ok(Ok(ref rows)) if rows.rows() == [vec![contextdb_core::Value::Int64(1)]]));
                drop(answer);
                done.wait();
            }
            answers
        }));
    }
    let index = reader
        .read_cache_share_status()
        .unwrap()
        .prepaid_index_bytes;
    for percent in [Some(2), Some(40), Some(0), None] {
        reader.set_read_cache_share(percent).unwrap();
        ready.wait();
        done.wait();
        assert_eq!(
            reader
                .read_cache_share_status()
                .unwrap()
                .prepaid_index_bytes,
            index
        );
    }
    for worker in workers {
        assert!(worker.join().unwrap().into_iter().all(|correct| correct));
    }
}

#[test]
fn direct_registration_refuses_an_invalid_cache_share_before_opening_storage() {
    let directory = tempfile::tempdir().unwrap();
    for percent in [101, 255] {
        let budget = 64 * 1024 * 1024;
        let declared =
            crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(budget).unwrap();
        let context = crate::executor::bounded::ReadOpenContext::new(
            declared.child().unwrap(),
            ReadSessionOptions::default().limits,
            crate::read_session::read_clock(),
            contextdb_core::read_contract::OwnerReadCancellation::new(),
        )
        .unwrap();
        let result = crate::persistence::load_read_image_at_registration_with_cache_share(
            &directory.path().join("not-created.db"),
            Some(usize::try_from(budget).unwrap()),
            Some(percent),
            None,
            None,
            context,
        );
        let Err(crate::persistence::LoadReadImageError::Memory(
            contextdb_core::Error::InvalidSetting {
                name,
                value,
                allowed,
            },
        )) = result
        else {
            panic!(
                "direct registration must return the typed setting refusal before opening storage"
            );
        };
        assert_eq!(name, "READ_CACHE_SHARE");
        assert_eq!(value, percent.to_string());
        assert_eq!(allowed, "0..=100, an integer percent");
    }
}
