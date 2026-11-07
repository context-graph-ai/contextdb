//! Private-owner evidence for writer_reopen_and_doubling_keep_a_fixed_resident_envelope.

use super::{RelationalStore, RetentionExpiryKey, RetentionExpiryOwner};
use std::mem::size_of;

impl RelationalStore {
    /// Allocated element-capacity bytes of the resident retention-expiry lookup.
    ///
    /// Includes key/owner slots and each owned table-name buffer. The owner
    /// contains its source LSN and credit handle; the credit's admitted amount
    /// is not a second allocation and is not added. No row or durable page loads.
    /// HashMap's private control bytes, spare buckets beyond its public capacity,
    /// and allocator rounding are excluded: this is a capacity-based lower bound.
    /// Required by writer_reopen_and_doubling_keep_a_fixed_resident_envelope.
    #[doc(hidden)]
    pub fn resident_retention_directory_bytes_for_test(&self) -> usize {
        let expiries = self.retention_expiries.read();
        expiries.keys().fold(
            expiries
                .capacity()
                .saturating_mul(size_of::<(RetentionExpiryKey, RetentionExpiryOwner)>()),
            |bytes, (table, _, _)| bytes.saturating_add(table.capacity()),
        )
    }
}
