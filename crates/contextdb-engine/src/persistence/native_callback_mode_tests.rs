//! Native admission callbacks preserve the exact mode of their caller.
use super::*;
use crate::vector_read_memory::DeclaredReadMemory;
use std::panic::{AssertUnwindSafe, catch_unwind};

fn current_mode() -> bool {
    CALLBACK_NATIVE_SCALARS.with(|slot| slot.get())
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn retained_mode<T>(
    scope: &StorageAdmissionScope,
    operation: impl FnOnce() -> contextdb_core::Result<T>,
) -> contextdb_core::Result<T> {
    scope.run_scoped(true, operation).0
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn nested_native_callback_modes_restore_on_success_error_and_panic() {
    let previous = current_mode();
    let parent = DeclaredReadMemory::from_reader_limit(64 * 1024).unwrap();
    let child = parent.child().unwrap();
    let baseline = parent.retained_bytes_for_test();
    let qualified = StorageAdmissionScope::reader_operation(child.clone())
        .map_err(original_storage_error)
        .unwrap();
    let unqualified = StorageAdmissionScope::reader_operation(child.clone())
        .map_err(original_storage_error)
        .unwrap();
    assert!(
        parent.retained_bytes_for_test() > baseline,
        "the real admission cells charge their original caller"
    );
    let result = qualified.run_native(|| {
        assert!(
            current_mode(),
            "a native retained-refusal callback uses its own mode"
        );
        let success = retained_mode(&unqualified, || {
            assert!(!current_mode());
            Ok(())
        });
        assert!(success.is_ok());
        assert!(
            current_mode(),
            "nested success restores the enclosing native mode"
        );
        let error = retained_mode(&unqualified, || {
            assert!(!current_mode());
            Err::<(), _>(contextdb_core::Error::ReadCancelled)
        });
        assert!(matches!(error, Err(contextdb_core::Error::ReadCancelled)));
        assert!(
            current_mode(),
            "nested error restores the enclosing native mode"
        );
        let panic = catch_unwind(AssertUnwindSafe(|| {
            retained_mode(&unqualified, || -> contextdb_core::Result<()> {
                assert!(!current_mode());
                panic!("callback-mode proof panic");
            })
        }));
        assert!(panic.is_err());
        assert!(
            current_mode(),
            "nested unwind restores the enclosing native mode"
        );
        Ok::<(), contextdb_core::Error>(())
    });
    assert!(result.is_ok());
    assert_eq!(current_mode(), previous);
    let result = retained_mode(&unqualified, || {
        assert!(!current_mode());
        let nested = qualified.run_native(|| {
            assert!(current_mode());
            Ok::<(), contextdb_core::Error>(())
        });
        assert!(nested.is_ok());
        assert!(!current_mode(), "the previous false mode is restored too");
        Ok(())
    });
    assert!(result.is_ok());
    assert_eq!(current_mode(), previous);
    let error = retained_mode(&unqualified, || {
        Err::<(), _>(contextdb_core::Error::ReadCancelled)
    });
    assert!(matches!(error, Err(contextdb_core::Error::ReadCancelled)));
    assert_eq!(
        current_mode(),
        previous,
        "a top-level callback error restores its caller"
    );
    let panic = catch_unwind(AssertUnwindSafe(|| {
        retained_mode(&unqualified, || -> contextdb_core::Result<()> {
            panic!("top-level callback-mode proof panic");
        })
    }));
    assert!(panic.is_err());
    assert_eq!(
        current_mode(),
        previous,
        "a top-level unwind restores its caller"
    );
    drop(qualified);
    drop(unqualified);
    assert_eq!(
        parent.retained_bytes_for_test(),
        baseline,
        "real native admission credits are released"
    );
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn native_read_and_write_after_callback_unwind_refuse_on_their_own_counters() {
    use crate::memory_accounting::{MemoryAccountant, OwnedMemoryReservation};
    use crate::vector_read_memory::statement_child_proof;
    use contextdb_core::read_memory::NativeRefusal;

    const LIMIT: usize = 8 * 1024 * 1024;
    let writer = MemoryAccountant::writer_budget(LIMIT).unwrap();
    let writer_owner = writer.accountant();
    let funder = writer.prepare_source_funder().unwrap();
    let original = writer.reserve_assembly_original(&funder).unwrap();
    let writer_scope = StorageAdmissionScope::writer_original_native(&original).unwrap();
    let mut builder = redb::Database::builder();
    builder.set_cache_size(128 * 1024);
    let raw = writer_scope
        .run_native(|| {
            builder
                .create_with_backend(redb::backends::InMemoryBackend::new())
                .map_err(|error| contextdb_core::Error::Other(error.to_string().into()))
        })
        .unwrap();
    let fixed_bytes = (128 * 1024usize)
        .checked_add(raw.cache_index_bytes())
        .unwrap();
    let reserve =
        crate::persistence::cache_reserve::original(&writer_owner, fixed_bytes, false).unwrap();
    let controls = raw.retain_cache_reserve(reserve);
    let warm = raw.cached_header_for_test().unwrap();
    let parent = DeclaredReadMemory::from_reader_limit(64 * 1024).unwrap();
    let child = parent.child().unwrap();
    let reader_scope = StorageAdmissionScope::reader_operation(child.clone()).unwrap();
    let previous = current_mode();
    let unwind = catch_unwind(AssertUnwindSafe(|| {
        reader_scope.run_original_unqualified(|| -> contextdb_core::Result<()> {
            panic!("callback before native operations");
        })
    }));
    assert!(unwind.is_err());
    assert_eq!(current_mode(), previous);
    let reader_before = parent.retained_bytes_for_test();
    let writer_before = writer_owner.usage().used;
    let (read, _) = statement_child_proof::observe(&parent, reader_before, || {
        reader_scope.run_native(|| {
            raw.begin_native_read()
                .map_err(crate::persistence::RedbPersistence::storage_error)
        })
    });
    assert!(matches!(
        &read,
        Err(NativeStorageFailure::Refused(
            NativeRefusal::MemoryBudgetExceeded {
                available_bytes: 0,
                ..
            }
        ))
    ));
    drop(read);
    assert_eq!(parent.retained_bytes_for_test(), reader_before);
    assert_eq!(writer_owner.usage().used, writer_before);
    let tail = OwnedMemoryReservation::try_new_for_native(
        writer_owner.clone(),
        LIMIT - writer_before,
        "test",
        "callback writer",
        "controlled fixture reservation",
    )
    .unwrap();
    let write = writer_scope.run_native(|| {
        raw.lifetime_aliases_for_test()
            .map_err(crate::persistence::RedbPersistence::storage_error)
    });
    assert!(matches!(
        &write,
        Err(NativeStorageFailure::Refused(
            NativeRefusal::MemoryBudgetExceeded {
                available_bytes: 0,
                ..
            }
        ))
    ));
    drop(write);
    assert_eq!(writer_owner.usage().used, LIMIT);
    assert_eq!(parent.retained_bytes_for_test(), reader_before);
    drop(tail);
    drop(warm);
    drop(controls);
    drop(raw);
    drop(writer_scope);
    drop(original);
    drop(funder);
    drop(reader_scope);
    drop(child);
}
