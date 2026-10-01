//! Storage-neutral commit history. File writers supply a durable cursor;
//! only the memory backend retains a tree.
use contextdb_core::{Lsn, Result, TxId};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::ops::Bound;

pub trait CommitDirectoryCursor: Send {
    fn next_entry(&mut self) -> Result<Option<(Lsn, TxId)>>;
}

/// Implementations pin one durable root per cursor and decode one entry at
/// a time. Counts and endpoint probes must not enumerate history. Mutations
/// are bounded by their supplied candidates, not by directory length.
pub trait CommitDirectory: Send + Sync {
    fn cursor(
        &self,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
    ) -> Result<Box<dyn CommitDirectoryCursor>>;
    fn floor(&self, upper: Bound<Lsn>) -> Result<Option<(Lsn, TxId)>>;
    fn census(&self) -> Result<(usize, Lsn)>;
    fn contains(&self, lsn: Lsn) -> Result<bool>;
    fn remove(&self, lsns: &[Lsn]) -> Result<u64>;
    fn replace(&self, retained: BTreeMap<Lsn, TxId>) -> Result<u64>;
    /// File-backed apply has already written this entry in its atomic durable
    /// transaction. Its adapter does nothing here; the memory backend inserts.
    fn record_applied(&self, lsn: Lsn, tx: TxId);
}

pub(crate) struct MemoryCommitDirectory(pub Mutex<BTreeMap<Lsn, TxId>>);
struct MemoryCursor(std::collections::btree_map::IntoIter<Lsn, TxId>);
impl CommitDirectoryCursor for MemoryCursor {
    fn next_entry(&mut self) -> Result<Option<(Lsn, TxId)>> {
        Ok(self.0.next())
    }
}
impl CommitDirectory for MemoryCommitDirectory {
    fn cursor(
        &self,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
    ) -> Result<Box<dyn CommitDirectoryCursor>> {
        Ok(Box::new(MemoryCursor(
            self.0
                .lock()
                .range((lower, upper))
                .map(|(lsn, tx)| (*lsn, *tx))
                .collect::<BTreeMap<_, _>>()
                .into_iter(),
        )))
    }
    fn floor(&self, upper: Bound<Lsn>) -> Result<Option<(Lsn, TxId)>> {
        Ok(self
            .0
            .lock()
            .range((Bound::Unbounded, upper))
            .next_back()
            .map(|(lsn, tx)| (*lsn, *tx)))
    }
    fn census(&self) -> Result<(usize, Lsn)> {
        let entries = self.0.lock();
        Ok((
            entries.len(),
            entries.keys().next().copied().unwrap_or(Lsn(0)),
        ))
    }
    fn contains(&self, lsn: Lsn) -> Result<bool> {
        Ok(self.0.lock().contains_key(&lsn))
    }
    fn remove(&self, lsns: &[Lsn]) -> Result<u64> {
        let mut entries = self.0.lock();
        Ok(lsns
            .iter()
            .filter(|lsn| entries.remove(lsn).is_some())
            .count() as u64)
    }
    fn replace(&self, retained: BTreeMap<Lsn, TxId>) -> Result<u64> {
        let mut entries = self.0.lock();
        let removed = entries.len().saturating_sub(retained.len()) as u64;
        *entries = retained;
        Ok(removed)
    }
    fn record_applied(&self, lsn: Lsn, tx: TxId) {
        self.0.lock().insert(lsn, tx);
    }
}
