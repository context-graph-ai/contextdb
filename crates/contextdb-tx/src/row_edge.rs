//! Internal row-version ownership shared by transaction staging and graph storage.
use contextdb_core::{AdjEntry, Lsn, RowId, TxId};
use serde::{Deserialize, Serialize};

/// Receiver-local immutable row version; never transported as an edge identity.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RowEdgeOwner {
    pub table: String,
    pub table_generation: u64,
    pub row_id: RowId,
    pub created_tx: TxId,
    pub lsn: Lsn,
}
impl Ord for RowEdgeOwner {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.table
            .len()
            .cmp(&other.table.len())
            .then_with(|| self.table.as_bytes().cmp(other.table.as_bytes()))
            .then_with(|| self.table_generation.cmp(&other.table_generation))
            .then_with(|| self.row_id.cmp(&other.row_id))
            .then_with(|| self.created_tx.cmp(&other.created_tx))
            .then_with(|| self.lsn.cmp(&other.lsn))
    }
}
impl PartialOrd for RowEdgeOwner {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl RowEdgeOwner {
    /// One owner body including its owned table allocation.
    pub fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.table.capacity())
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OwnedAdjEntry {
    pub owner: RowEdgeOwner,
    pub entry: AdjEntry,
}
impl OwnedAdjEntry {
    /// Both adjacency copies and their owner metadata, plus the ordinary edge
    /// write allowance. Derived records do not create standalone edge logs.
    pub fn estimated_bytes(&self) -> usize {
        self.entry
            .estimated_bytes()
            .saturating_add(self.owner.estimated_bytes().saturating_mul(2))
    }

    pub fn reassign_tx(&mut self, from: TxId, to: TxId) {
        if self.owner.created_tx == from {
            self.owner.created_tx = to;
        }
        if self.entry.created_tx == from {
            self.entry.created_tx = to;
        }
        if self.entry.deleted_tx == Some(from) {
            self.entry.deleted_tx = Some(to);
        }
    }

    pub fn stamp_lsn(&mut self, lsn: Lsn) {
        self.owner.lsn = lsn;
        self.entry.lsn = lsn;
    }
}
