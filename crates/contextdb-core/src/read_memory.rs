//! Owned memory charges shared by storage and suspendable readers.
//!
//! The engine supplies the budget capability; storage cannot change its limit.
use crate::Result;
use std::fmt::Debug;
use std::sync::Arc;

#[doc(hidden)]
pub trait ReadMemoryBudget: Debug + Send + Sync {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()>;
    fn release(&self, bytes: usize);
}

/// A charge moves with its allocation and returns exactly once. Owners place
/// this field after their payload so the payload drops before the credit.
#[derive(Debug)]
#[doc(hidden)]
pub struct ReadCredit {
    budget: Arc<dyn ReadMemoryBudget>,
    bytes: usize,
}

impl ReadCredit {
    pub fn try_new(
        budget: Arc<dyn ReadMemoryBudget>,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<Self> {
        budget.try_reserve(bytes, subsystem, operation, hint)?;
        Ok(Self { budget, bytes })
    }
}

impl Drop for ReadCredit {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}
