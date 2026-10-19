//! The original native operation, created before request-owned allocations.
use crate::vector_read_memory::NativeReadChildReceipt;
use contextdb_core::read_contract::NativeReadCancellation;
use contextdb_core::read_memory::ReadMemoryOwner;

/// Only a sealed child receipt can start this operation. The cancellation
/// returned to the caller retains the same token, without another allocation.
/// No public owner/token getter or Legacy conversion exists.
#[doc(hidden)]
#[derive(Debug)]
pub struct NativeReadRequest {
    q: ReadMemoryOwner,
    cancellation: NativeReadCancellation,
}

impl NativeReadRequest {
    pub(crate) fn try_new(
        receipt: NativeReadChildReceipt,
    ) -> contextdb_core::Result<(Self, NativeReadCancellation)> {
        let q = receipt.into_owner();
        let cancellation = NativeReadCancellation::try_new(q.clone())?;
        let caller_cancellation = cancellation.clone();
        Ok((Self { q, cancellation }, caller_cancellation))
    }

    /// Native companion of `try_new`: the cancellation token's admission
    /// refuses with a `NativeRefusal`, without an owned diagnostic.
    pub(crate) fn try_new_native(
        receipt: NativeReadChildReceipt,
    ) -> std::result::Result<
        (Self, NativeReadCancellation),
        contextdb_core::read_memory::NativeRefusal,
    > {
        let q = receipt.into_owner();
        let cancellation = NativeReadCancellation::try_new_native(q.clone())?;
        let caller_cancellation = cancellation.clone();
        Ok((Self { q, cancellation }, caller_cancellation))
    }

    /// An allocation-free alias of this operation, never a new provider child
    /// or cancellation token. Used only to retain original custody while the
    /// handler consumes the authentic operation by value.
    pub(crate) fn share(&self) -> Self {
        Self {
            q: self.q.clone(),
            cancellation: self.cancellation.clone(),
        }
    }

    pub(crate) fn owner(&self) -> ReadMemoryOwner {
        self.q.clone()
    }

    pub(crate) fn cancellation(&self) -> &NativeReadCancellation {
        &self.cancellation
    }
}
