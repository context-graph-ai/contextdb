//! Additional initialized D1 rows in the existing mapped allocator witness.
//! No sibling tests, allocator, production field, or baseline price is added.
use super::*;
use crate::persistence::content_identity::proof_allocation_oracle_tests::{
    candidate_payload_free_observation_for_test, last_request_for_test,
    last_request_pointer_for_test, measured, watch_candidate_payload_for_test,
};
use crate::persistence::vector_membership_source::{
    DurableColumnSourceFactory, DurableLoaderBuilder, DurableMembershipSource,
    DurableReverseMembershipSource, MembershipOwner,
};
use contextdb_vector::store::{
    DurableRawPartitionFactory, RawVectorMembershipSource, RawVectorMembershipSummary,
    RawVectorReverseMembershipSource,
};

#[derive(Clone, Copy, Default)]
struct Request {
    pointer: usize,
    size: usize,
    align: usize,
    count: usize,
}
impl Request {
    fn actual() -> Self {
        let (size, align, count) = last_request_for_test(false);
        Self {
            pointer: last_request_pointer_for_test(),
            size,
            align,
            count,
        }
    }
}
#[derive(Default)]
struct OpenCapture {
    callback: Option<(DurableLoaderBuilder, Request)>,
    columns: Option<(Arc<DurableColumnSourceFactory>, Request)>,
    factory_credit: Request,
    factory_uses_plain_accountant: bool,
    callback_users_before: usize,
}
thread_local! {
    static OPEN: std::cell::RefCell<Option<OpenCapture>> = const { std::cell::RefCell::new(None) };
    static PAIR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static PARTITION_CREDIT: std::cell::Cell<Request> = const { std::cell::Cell::new(Request { pointer: 0, size: 0, align: 0, count: 0 }) };
    static REVERSE_REQUEST: std::cell::Cell<Request> = const { std::cell::Cell::new(Request { pointer: 0, size: 0, align: 0, count: 0 }) };
}
// Read existing census metadata before any observer Arc clone. Observers are
// inactive for every original row and compiled out of all production builds.
pub(crate) fn observe_factory_credit_for_test(
    credit: &Arc<contextdb_vector::VectorCopyAdmission>,
    accountant: &crate::memory_accounting::AccountantHandle,
) {
    OPEN.with(|slot| {
        if let Some(capture) = slot.borrow_mut().as_mut() {
            capture.factory_credit = Request::actual();
            contextdb_vector::VectorCopyAdmission::visit_retained_copy_credit(
                credit,
                &**accountant,
                &mut |_, bytes, plain| {
                    assert!(
                        bytes > 0,
                        "actual production factory owner holds admitted metadata"
                    );
                    capture.factory_uses_plain_accountant = plain;
                },
            );
            capture.callback_users_before = accountant.writer_registration_observation_for_test().3;
        }
    });
}
pub(crate) fn observe_callback_for_test(
    callback: &DurableLoaderBuilder,
    accountant: &crate::memory_accounting::AccountantHandle,
) {
    OPEN.with(|slot| { if let Some(capture) = slot.borrow_mut().as_mut() {
        capture.callback = Some((callback.clone(), Request::actual()));
        assert_eq!(accountant.writer_registration_observation_for_test().3, capture.callback_users_before + 1, "whole production callback retains exactly one original writer handle");
        assert!(!capture.factory_uses_plain_accountant, "production callback retains its real ImageMemory factory owner rather than a plain accountant capability");
    } });
}
pub(crate) fn observe_columns_for_test(columns: &Arc<DurableColumnSourceFactory>) {
    OPEN.with(|slot| {
        if let Some(capture) = slot.borrow_mut().as_mut() {
            capture.columns = Some((columns.clone(), Request::actual()));
        }
    });
}
pub(crate) fn observe_partition_credit_for_test() {
    if PAIR.with(std::cell::Cell::get) {
        PARTITION_CREDIT.with(|slot| slot.set(Request::actual()));
    }
}
pub(crate) fn observe_reverse_for_test(_reverse: &Arc<DurableReverseMembershipSource>) {
    if PAIR.with(std::cell::Cell::get) {
        REVERSE_REQUEST.with(|slot| slot.set(Request::actual()));
    }
}
