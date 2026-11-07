#![expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
use super::*;

#[derive(Default)]
struct PublicationState {
    granted: bool,
    finished: bool,
    committed_lsn: Option<Lsn>,
}

#[derive(Default)]
struct DriveState {
    cancelled: bool,
    transactions: BTreeMap<TxId, PublicationState>,
}

pub(super) struct WritePublicationControlForTest {
    index: VectorIndexRef,
    state: Mutex<DriveState>,
    changed: Condvar,
}

impl WritePublicationControlForTest {
    fn cancel(&self) {
        self.state.lock().cancelled = true;
        self.changed.notify_all();
    }

    fn admit(self: &Arc<Self>, tx: TxId) -> WritePublicationLeaseForTest {
        let mut state = self.state.lock();
        state.transactions.entry(tx).or_default();
        self.changed.notify_all();
        while !state.cancelled && !state.transactions[&tx].granted {
            self.changed.wait(&mut state);
        }
        WritePublicationLeaseForTest {
            control: Arc::clone(self),
            tx,
        }
    }
}

pub(super) struct WritePublicationLeaseForTest {
    control: Arc<WritePublicationControlForTest>,
    tx: TxId,
}

impl Drop for WritePublicationLeaseForTest {
    fn drop(&mut self) {
        let mut state = self.control.state.lock();
        let transaction = state
            .transactions
            .get_mut(&self.tx)
            .expect("admitted transaction");
        transaction.finished = true;
        self.control.changed.notify_all();
    }
}

#[doc(hidden)]
pub struct WritePublicationDriveForTest {
    control: Arc<WritePublicationControlForTest>,
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WritePublicationReceipt {
    pub txid: TxId,
    pub committed_lsn: Lsn,
    pub index: VectorIndexRef,
}

impl Drop for WritePublicationDriveForTest {
    fn drop(&mut self) {
        self.control.cancel();
    }
}

impl WritePublicationDriveForTest {
    #[doc(hidden)]
    pub fn wait_for_queued_for_test(&self, count: usize) -> Vec<TxId> {
        let mut state = self.control.state.lock();
        loop {
            let queued = state
                .transactions
                .iter()
                .filter(|(_, transaction)| !transaction.granted && !transaction.finished)
                .map(|(tx, _)| *tx)
                .collect::<Vec<_>>();
            if state.cancelled || queued.len() >= count {
                return queued;
            }
            self.control.changed.wait(&mut state);
        }
    }

    #[doc(hidden)]
    pub fn grant_for_test(&self, txid: TxId) -> Result<()> {
        let mut state = self.control.state.lock();
        if state.cancelled {
            return Err(Error::Other("write publication drive is cancelled".into()));
        }
        state
            .transactions
            .get_mut(&txid)
            .ok_or_else(|| Error::Other("transaction has not reached writer admission".into()))?
            .granted = true;
        self.control.changed.notify_all();
        Ok(())
    }

    #[doc(hidden)]
    pub fn wait_for_published_and_released_for_test(
        &self,
        txid: TxId,
    ) -> Result<WritePublicationReceipt> {
        let mut state = self.control.state.lock();
        loop {
            if state.cancelled {
                return Err(Error::Other("write publication drive is cancelled".into()));
            }
            if let Some(transaction) = state.transactions.get(&txid)
                && transaction.finished
            {
                let committed_lsn = transaction.committed_lsn.ok_or_else(|| {
                    Error::Other("admitted transaction ended without publication".into())
                })?;
                return Ok(WritePublicationReceipt {
                    txid,
                    committed_lsn,
                    index: self.control.index.clone(),
                });
            }
            self.control.changed.wait(&mut state);
        }
    }
}

#[cfg(feature = "test-seams")]
impl Database {
    #[doc(hidden)]
    pub fn write_publication_drive_for_test(
        &self,
        index: VectorIndexRef,
    ) -> WritePublicationDriveForTest {
        let control = Arc::new(WritePublicationControlForTest {
            index: index.clone(),
            state: Mutex::default(),
            changed: Condvar::new(),
        });
        let previous = self
            .finalization
            .publication_drives_for_test
            .lock()
            .insert(index, Arc::downgrade(&control));
        if let Some(previous) = previous.and_then(|control| control.upgrade()) {
            previous.cancel();
        }
        WritePublicationDriveForTest { control }
    }

    pub(super) fn publication_committed_for_test(&self, tx: TxId, lsn: Lsn) {
        for control in self
            .finalization
            .publication_drives_for_test
            .lock()
            .values()
            .filter_map(std::sync::Weak::upgrade)
        {
            if let Some(transaction) = control.state.lock().transactions.get_mut(&tx) {
                transaction.committed_lsn = Some(lsn);
            }
        }
    }

    pub(super) fn publication_admission_for_test(
        &self,
        tx: TxId,
    ) -> Vec<WritePublicationLeaseForTest> {
        if !self
            .finalization
            .publication_drives_for_test
            .lock()
            .values()
            .any(|control| control.strong_count() != 0)
        {
            return Vec::new();
        }
        let indexes = self
            .tx_mgr
            .with_read(tx, |writes| {
                writes
                    .vector_inserts
                    .iter()
                    .map(|entry| entry.index.clone())
                    .chain(
                        writes
                            .vector_deletes
                            .iter()
                            .map(|(index, _, _)| index.clone()),
                    )
                    .chain(
                        writes
                            .vector_moves
                            .iter()
                            .map(|(index, _, _, _)| index.clone()),
                    )
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();
        let controls = {
            let registry = self.finalization.publication_drives_for_test.lock();
            indexes
                .iter()
                .filter_map(|index| registry.get(index).and_then(std::sync::Weak::upgrade))
                .collect::<Vec<_>>()
        };
        controls
            .into_iter()
            .map(|control| control.admit(tx))
            .collect()
    }
}

#[cfg(test)]
mod tests;
