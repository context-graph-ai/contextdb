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
    pub fn new(budget: Arc<dyn ReadMemoryBudget>) -> Self {
        Self { budget, bytes: 0 }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn try_grow_for(
        &mut self,
        added: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        let bytes = self
            .bytes
            .checked_add(added)
            .ok_or_else(|| crate::Error::Other("reader charge overflow".into()))?;
        self.budget.try_reserve(added, subsystem, operation, hint)?;
        self.bytes = bytes;
        Ok(())
    }

    pub fn try_shrink(&mut self, returned: usize) -> Result<()> {
        let bytes = self
            .bytes
            .checked_sub(returned)
            .ok_or_else(|| crate::Error::Other("reader charge release exceeds credit".into()))?;
        self.budget.release(returned);
        self.bytes = bytes;
        Ok(())
    }

    pub fn try_split_off(&mut self, bytes: usize) -> Result<Self> {
        self.bytes = self.bytes.checked_sub(bytes)
            .ok_or_else(|| crate::Error::Other("reader split exceeds credit".into()))?;
        Ok(Self { budget: self.budget.clone(), bytes })
    }

    pub fn transfer(mut self) {
        self.bytes = 0;
    }

    /// Passive observation of the charge held by this allocation owner.
    #[doc(hidden)]
    pub fn bytes_for_test(&self) -> usize {
        self.bytes
    }

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

/// A boxed payload whose allocation is freed before its admitted credit.
/// Callers reserve the concrete layout before allocating and coerce any
/// trait-object Box before constructing this move-only owner.
#[doc(hidden)]
pub struct OwnedReadBox<T: ?Sized> {
    payload: Box<T>,
    _credit: ReadCredit,
}

impl<T: ?Sized> OwnedReadBox<T> {
    pub fn new(payload: Box<T>, credit: ReadCredit) -> Self {
        Self {
            payload,
            _credit: credit,
        }
    }
    pub fn as_ref(&self) -> &T {
        &self.payload
    }
}
impl<T: ?Sized> std::ops::Deref for OwnedReadBox<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.as_ref()
    }
}
