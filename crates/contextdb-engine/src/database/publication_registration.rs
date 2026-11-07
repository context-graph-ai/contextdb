//! Paid publication registrations borrow the immutable name carrier, never copied keys.
#![allow(clippy::result_large_err, reason = "Preserve inline budget refusals.")]
use super::*;
#[cfg(not(test))]
use contextdb_core::read_memory::NativeRefusal;
use contextdb_core::read_memory::{ReadCredit, ReadShared};

#[derive(Clone)]
pub(super) enum ReadPublicationRegistration {
    Funded(ReadShared<ReadPublicationRegistrationData>),
    #[cfg(test)]
    Unfunded(Arc<ReadPublicationRegistrationData>),
}

pub(super) struct ReadPublicationRegistrationData {
    pub(super) tables: crate::executor::PublicationTables,
    pub(super) next: Mutex<Option<ReadPublicationRegistration>>,
}
impl std::ops::Deref for ReadPublicationRegistration {
    type Target = ReadPublicationRegistrationData;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Funded(node) => node,
            #[cfg(test)]
            Self::Unfunded(node) => node,
        }
    }
}
impl ReadPublicationRegistration {
    pub(super) fn new(scope: &crate::executor::ReadPublicationScope) -> Result<Self> {
        let payload = ReadPublicationRegistrationData {
            tables: scope.tables.clone(),
            next: Mutex::new(None),
        };
        if let Some(owner) = &scope.read_memory {
            let invoice = ReadCredit::try_new_owned(
                owner.clone(),
                ReadShared::<ReadPublicationRegistrationData>::layout().size(),
                "bounded_read",
                "publication_registration",
                "Release retained publication guards or raise MEMORY_LIMIT.",
            )?;
            return ReadShared::try_from_prepaid_native(payload, invoice)
                .map(Self::Funded)
                .map_err(Error::NativeEngineOther);
        }
        #[cfg(test)]
        {
            Ok(Self::Unfunded(Arc::new(payload)))
        }
        #[cfg(not(test))]
        {
            Err(Error::NativeEngineOther(NativeRefusal::Other(
                "publication has no original memory owner",
            )))
        }
    }
    pub(super) fn same(&self, other: &Self) -> bool {
        std::ptr::eq(&**self, &**other)
    }
}

impl ReadPublicationGateState {
    pub(super) fn conflicts_with_readers(&self, tables: &BTreeSet<String>) -> bool {
        let mut next = self.readers.head.clone();
        while let Some(node) = next {
            if publication_scopes_conflict(tables, &node.tables) {
                return true;
            }
            next = node.next.lock().clone();
        }
        false
    }
    pub(super) fn unlink_reader(&mut self, target: &ReadPublicationRegistration) {
        let Some(mut node) = self.readers.head.clone() else {
            panic!("publication guard was registered");
        };
        if node.same(target) {
            self.readers.head = node.next.lock().take();
            return;
        }
        loop {
            let next = {
                let mut link = node.next.lock();
                let next = link.as_ref().expect("publication guard remains registered");
                if next.same(target) {
                    let after = next.next.lock().take();
                    *link = after;
                    return;
                }
                next.clone()
            };
            node = next;
        }
    }
}
#[derive(Default)]
pub(super) struct ReadPublicationReaders {
    pub(super) head: Option<ReadPublicationRegistration>,
}
impl ReadPublicationReaders {
    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.head.is_none()
    }
}
impl Drop for ReadPublicationReaders {
    fn drop(&mut self) {
        // Also bound teardown if construction unwinds with no external guard aliases.
        while let Some(node) = self.head.take() {
            self.head = node.next.lock().take();
            drop(node);
        }
    }
}
