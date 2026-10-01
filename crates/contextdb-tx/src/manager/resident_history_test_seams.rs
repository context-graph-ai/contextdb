//! Passive ownership observation for resident commit-directory tests.
use super::{Lsn, TransactionManager, TxId, WriteSetApplicator};

impl<S: WriteSetApplicator> TransactionManager<S> {
    /// Lower-bound bytes held by the actual resident commit-directory entries.
    ///
    /// Used by `writer_reopen_and_doubling_keep_a_fixed_resident_envelope` and
    /// `history_sync_and_commit_cursors_reload_without_full_directories`.
    /// Excludes private B-tree node overhead and the fixed map/mutex header.
    /// Zero means the resident directory is empty. This observer reads the
    /// resident map in place; it does not clone or load durable inventory.
    #[doc(hidden)]
    pub fn retained_commit_directory_bytes_for_test(&self) -> usize {
        let commit_index = self.commit_index.lock();
        commit_index
            .len()
            .saturating_mul(std::mem::size_of::<Lsn>() + std::mem::size_of::<TxId>())
    }
}
