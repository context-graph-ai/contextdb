//! Custody for provisional reader registrations and interruptible coordination.

use contextdb_core::read_memory::{ReadCredit, ReadMemoryOwner, ReadShared};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static NEXT_TRANSACTION: AtomicU64 = AtomicU64::new(1);

/// Every record of one opening consults this same teardown authority.
pub(crate) struct RegistrationTransaction {
    identity: u64,
    committed: AtomicBool,
}

/// The unique capability to accept a privately constructed reader.
pub(crate) struct RegistrationCommitTicket {
    transaction: ReadShared<RegistrationTransaction>,
}

impl RegistrationCommitTicket {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating. "
    )]
    pub(crate) fn new(owner: ReadMemoryOwner) -> contextdb_core::Result<Self> {
        #[cfg(any(test, feature = "test-seams"))]
        let _payer = contextdb_core::coordination_test_observation::register_current_payer(&owner);
        let identity = NEXT_TRANSACTION
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| {
                contextdb_core::Error::NativeEngineOther(
                    contextdb_core::read_memory::NativeRefusal::Other(
                        "reader registration identity exhausted",
                    ),
                )
            })?;
        let invoice = ReadCredit::try_new_owned(
            owner,
            ReadShared::<RegistrationTransaction>::layout().size(),
            "reader",
            "registration_transaction",
            "Admit the reader's registration transaction before publication.",
        )?;
        let transaction = {
            #[cfg(any(test, feature = "test-seams"))]
            let _enrollment = contextdb_core::coordination_test_observation::allocation(
                contextdb_core::coordination_test_observation::AllocationClass::RegistrationTransaction,
                ReadShared::<RegistrationTransaction>::layout(), &invoice,
            );
            ReadShared::try_from_prepaid_native(
                RegistrationTransaction {
                    identity,
                    committed: AtomicBool::new(false),
                },
                invoice,
            )
            .map_err(contextdb_core::Error::NativeEngineOther)?
        };
        Ok(Self { transaction })
    }

    pub(crate) fn transaction(&self) -> ReadShared<RegistrationTransaction> {
        self.transaction.clone()
    }

    /// Only a retained real registration may outlive this accepting transition.
    /// No observer or other fallible operation runs between this store and return.
    pub(crate) fn commit(self) {
        self.transaction.committed.store(true, Ordering::Release);
    }
}

impl RegistrationTransaction {
    pub(crate) fn identity(&self) -> u64 {
        self.identity
    }

    pub(crate) fn is_staging(&self) -> bool {
        !self.committed.load(Ordering::Acquire)
    }
}

/// Cursor handoff retains its original absolute deadline.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating. "
)]
pub(crate) fn check_handoff_authority(
    cancellation: &contextdb_core::read_contract::OwnerReadCancellation,
    clock: &dyn contextdb_core::read_contract::DeadlineClock,
    deadline: u64,
    active_ms: u64,
) -> contextdb_core::Result<()> {
    if cancellation.is_cancelled() {
        return Err(contextdb_core::Error::ReadCancelled);
    }
    if clock.now_ms() >= deadline {
        return Err(contextdb_core::Error::ReadFailure(
            contextdb_core::read_contract::ReadFailure::owner_limit_exceeded(
                contextdb_core::read_contract::OwnerLimitExceededDetail {
                    limit: contextdb_core::read_contract::ReadFailureLimit::ActiveMs,
                    value: active_ms,
                    required: None,
                    statement: None,
                },
            ),
        ));
    }
    Ok(())
}
