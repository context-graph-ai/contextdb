use super::*;
use crate::memory_accounting::MemoryAccountant;

#[test]
fn an_ignored_native_refusal_cannot_be_reported_as_callback_success() {
    let writer = MemoryAccountant::writer_no_limit().unwrap();
    let owner = writer.accountant();
    let funder = writer.prepare_source_funder().unwrap();
    let original = writer.reserve_assembly_original(&funder).unwrap();
    let scope = StorageAdmissionScope::writer_original_native(&original).unwrap();
    let limit = owner.usage().used;
    assert!(limit > 0, "the actual native controls are already admitted");
    owner.set_budget(Some(limit)).unwrap();

    let result = scope.run_native(|| {
        // Invoke the genuine source callback; ignore its returned marker.
        // Only the already-admitted scope is kept, and no backing is copied.
        assert!(unsafe { admit_scope(scope.pointer.cast(), Layout::new::<u8>()) }.is_err());
        Ok(17_u8)
    });
    assert!(
        matches!(
            result,
            Err(NativeStorageFailure::Refused(NativeRefusal::MemoryBudgetExceeded {
                requested_bytes: 1,
                available_bytes: 0,
                budget_limit_bytes,
                ..
            })) if budget_limit_bytes == limit
        ),
        "an ignored native marker cannot become a successful callback value: {result:?}"
    );
    assert_eq!(
        owner.usage().used,
        limit,
        "a refused debit retains no credit"
    );
    owner.set_budget(None).unwrap();
}
