//! Passive ownership observation for resident commit-directory tests.
use super::{CommitHistoryCapability, TransactionManager, WriteSetApplicator};

impl<S: WriteSetApplicator> TransactionManager<S> {
    /// Identifies the capability installed by the real captured-image open.
    #[doc(hidden)]
    pub fn has_captured_commit_directory_for_test(&self) -> bool {
        matches!(&self.commit_index, CommitHistoryCapability::Captured { .. })
    }

    /// Visit the already installed captured child without creating an owner.
    #[doc(hidden)]
    pub fn visit_captured_read_memory_for_test(
        &self,
        visit: impl FnOnce(&std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>),
    ) {
        if let CommitHistoryCapability::Captured { image_read_memory, .. } = &self.commit_index {
            visit(image_read_memory);
        }
    }

    /// Lower-bound bytes held by the actual resident commit-directory entries.
    ///
    /// Used by `writer_reopen_and_doubling_keep_a_fixed_resident_envelope` and
    /// `history_sync_and_commit_cursors_reload_without_full_directories`.
    /// Excludes private B-tree node overhead and the fixed map/mutex header.
    /// Zero means the resident directory is empty. This observer reads the
    /// resident map in place; it does not clone or load durable inventory.
    #[doc(hidden)]
    pub fn retained_commit_directory_bytes_for_test(&self) -> usize {
        match &self.commit_index {
            CommitHistoryCapability::Writable(directory) => directory.retained_entry_bytes(),
            CommitHistoryCapability::EngineMemory(directory) => directory
                .census()
                .0
                .saturating_mul(
                    std::mem::size_of::<contextdb_core::Lsn>()
                        + std::mem::size_of::<contextdb_core::TxId>(),
                ),
            CommitHistoryCapability::Captured { directory, .. } => directory.retained_entry_bytes(),
        }
    }
}
