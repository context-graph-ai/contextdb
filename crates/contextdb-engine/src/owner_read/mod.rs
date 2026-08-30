//! The authenticated live-owner inspection plane: how a process that owns a
//! store serves other processes' bounded read sessions.
//!
//! Three parts, re-exported below. `admission` decides which incoming request
//! is let in and leases it against the owner's declared concurrency
//! (`OwnerAdmission`, `RequestLease`). `service` is the owner side that listens,
//! holds reader cursors, and answers (`OwnerReadService`, `OwnerServiceSpec`,
//! `ValidatedOwnerListener`, `CursorEntry`). `client` is the reading side that
//! dials the owner and consumes its answers (`OwnerClient`). The module is
//! crate-private in production and exposed only under the `test-seams`
//! feature; consumers reach this plane through `ReadSession`, never directly.
//!
//! What this module deliberately does not own: addressing, authentication,
//! framing and deadlines belong to `local_transport`; execution belongs to the
//! one bounded executor; the typed refusal vocabulary belongs to
//! `contextdb_core::read_contract`. This plane owns no database lifecycle,
//! persistence handle, companion record, or parser, and implements no second
//! execution path — it carries requests to the same kernel a direct file read
//! uses and carries the answers back.

#![allow(dead_code)]

mod admission;
mod client;
mod service;

pub use admission::{OwnerAdmission, RequestLease};
pub use client::OwnerClient;
#[cfg(unix)]
pub use client::OwnerDisconnectHandle;
pub use service::{
    ConnectionState, CursorEntry, CursorIdentifierAllocator, OwnerReadResourceSnapshot,
    OwnerReadService, OwnerServiceSpec, ValidatedOwnerListener,
};
#[cfg(feature = "test-seams")]
pub use service::{OwnerBoundedExecutionObserver, OwnerBoundedOperation};

use crate::local_transport::LocalTransportError;
use crate::local_transport::{LocalRequest, LocalResponseExpectation, PayloadViolation};
use contextdb_core::Error;
use contextdb_core::read_contract::{ReadContractViolation, ReadFailure};

/// Internal error for this RED assembly. Stable product failures remain the
/// existing `ReadFailure`; this enum only makes missing prerequisites honest.
#[derive(Debug, thiserror::Error)]
pub enum OwnerReadScaffoldError {
    #[error("owner-read prerequisite is unimplemented: {seam}")]
    Unimplemented { seam: &'static str },
    #[error("read-contract prerequisite failed: {0}")]
    ReadContract(#[from] ReadContractViolation),
    #[error("local transport prerequisite failed: {0}")]
    LocalTransport(#[from] LocalTransportError),
    #[error("owner request refused: {0:?}")]
    Refused(ReadFailure),
    #[error("database prerequisite failed: {0}")]
    Database(#[from] Error),
}

impl OwnerReadScaffoldError {
    pub(crate) const fn unimplemented(seam: &'static str) -> Self {
        Self::Unimplemented { seam }
    }

    pub(crate) fn from_local(error: LocalTransportError) -> Self {
        match error {
            LocalTransportError::Refusal(failure) => Self::Refused(failure),
            // A payload that is exactly what it claims to be and simply too
            // large to decode inside the ceiling in force is a BUDGET answer,
            // not a broken peer. Handing the caller the empty
            // invalid-channel-data document instead tells them their owner is
            // sending garbage when the ceiling they themselves declared is
            // what was reached -- so they can neither act on it nor raise it.
            LocalTransportError::Payload(PayloadViolation::MemoryCeilingExceeded { ceiling }) => {
                Self::Refused(crate::local_transport::memory_ceiling_read_failure(ceiling))
            }
            LocalTransportError::Frame(_) | LocalTransportError::Payload(_) => Self::Refused(
                ReadFailure::new(
                    contextdb_core::read_contract::ReadFailureKind::InvalidChannelData,
                    contextdb_core::read_contract::ReadFailureDetail::None,
                )
                .expect("invalid channel data accepts canonical empty detail"),
            ),
            other => Self::LocalTransport(other),
        }
    }
}

fn response_expectation(request: &LocalRequest) -> LocalResponseExpectation {
    match request {
        LocalRequest::Query { .. } => LocalResponseExpectation::OrdinaryResult,
        LocalRequest::CursorOpen { .. } => LocalResponseExpectation::CursorOpen,
        LocalRequest::CursorFetch { .. } => LocalResponseExpectation::CursorFetch,
        LocalRequest::CursorClose { .. } => LocalResponseExpectation::CursorClose,
        LocalRequest::Metadata { request } => LocalResponseExpectation::Metadata(request.clone()),
        LocalRequest::Explain { .. } => LocalResponseExpectation::Explain,
        LocalRequest::OwnerStatus => LocalResponseExpectation::OwnerStatus,
        LocalRequest::Custom { .. } => LocalResponseExpectation::Custom,
        // A cancel is answered by the refusal the request it named receives,
        // never by a reply of its own.
        LocalRequest::CancelInFlight { .. } => LocalResponseExpectation::Custom,
    }
}

pub type OwnerReadScaffoldResult<T> = std::result::Result<T, OwnerReadScaffoldError>;
