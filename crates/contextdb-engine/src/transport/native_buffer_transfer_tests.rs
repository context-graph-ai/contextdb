//! The delivery body and reply move between the transport and the custody
//! selection as the same physical bytes under the same invoice.
use super::{AdmittedWireVec, BoundedWireBody};
use crate::custody::native_selection::{Failure, NativeBuffer};
use crate::vector_read_memory::DeclaredReadMemory;
use contextdb_core::read_memory::ReadMemoryOwner;
use std::sync::Arc;

fn native_owner(limit: u64) -> (Arc<DeclaredReadMemory>, ReadMemoryOwner) {
    let parent = DeclaredReadMemory::from_reader_limit(limit);
    let owner = parent
        .native_child()
        .expect("admit one native request owner")
        .into_owner();
    (parent, owner)
}

#[test]
fn delivery_body_and_reply_move_without_a_copy_or_a_second_charge() {
    let (parent, owner) = native_owner(1 << 20);
    let start = parent.retained_bytes_for_test();
    let request = b"one authenticated delivery request";
    let body = BoundedWireBody::from_native_reply(
        AdmittedWireVec::copy_from(request, owner.clone()).expect("admit the request body"),
    );
    let charged = parent.retained_bytes_for_test();
    assert_eq!(charged - start, request.len() as u64);
    let admitted = body
        .into_native_contiguous()
        .ok()
        .expect("an unconsumed admitted body is contiguous");
    let pointer = admitted.as_ptr_for_test();
    let (bytes, credit) = admitted.into_prepaid();
    let buffer = NativeBuffer::from_prepaid(bytes, credit, &owner).expect("same request owner");
    assert_eq!(buffer.bytes().as_ptr(), pointer);
    assert_eq!(buffer.bytes(), request);
    assert_eq!(parent.retained_bytes_for_test(), charged);
    let (bytes, credit) = buffer.into_prepaid();
    let admitted =
        AdmittedWireVec::from_prepaid(bytes, credit, owner.clone()).expect("same request owner");
    assert_eq!(admitted.as_ptr_for_test(), pointer);
    let body = BoundedWireBody::from_native_reply(admitted);
    assert_eq!(
        body.native_contiguous_bytes()
            .expect("contiguous reply")
            .as_ptr(),
        pointer
    );
    assert_eq!(parent.retained_bytes_for_test(), charged);
    drop(body);
    assert_eq!(parent.retained_bytes_for_test(), start);
    drop(owner);
    assert_eq!(parent.retained_bytes_for_test(), 0);
}

#[test]
fn a_foreign_owner_cannot_adopt_paid_delivery_bytes() {
    let (parent, owner) = native_owner(1 << 20);
    let (other_parent, other) = native_owner(1 << 20);
    let start = parent.retained_bytes_for_test();
    let other_start = other_parent.retained_bytes_for_test();

    let paid = NativeBuffer::copy_from(&owner, b"edge").expect("paid edge copy");
    let (bytes, credit) = paid.into_prepaid();
    let refused = NativeBuffer::from_prepaid(bytes, credit, &other);
    assert!(matches!(refused, Err(Failure::OwnerMismatch)));
    assert_eq!(other_parent.retained_bytes_for_test(), other_start);
    assert_eq!(parent.retained_bytes_for_test(), start);

    let paid = NativeBuffer::copy_from(&owner, b"reply").expect("paid reply");
    let (bytes, credit) = paid.into_prepaid();
    assert!(AdmittedWireVec::from_prepaid(bytes, credit, other.clone()).is_err());
    assert_eq!(other_parent.retained_bytes_for_test(), other_start);
    assert_eq!(parent.retained_bytes_for_test(), start);
}
