//! The shared durable row-owned adjacency mutation path, including received DDL.
use super::*;
use contextdb_tx::OwnedAdjEntry;

fn corruption(reason: &str) -> Error {
    Error::StoreCorrupted {
        path: "row-derived adjacency".into(),
        reason: reason.into(),
    }
}

impl RedbPersistence {
    /// Called inside the caller's existing history write and graph writer
    /// admission window. The selected dropped owners were captured before DDL;
    /// no graph population scan or transport occurrence matching is needed.
    pub(super) fn apply_received_row_graph_parts_in_write(
        write: &redb::WriteTransaction,
        inserts: &[OwnedAdjEntry],
        deletes: &[(OwnedAdjEntry, TxId)],
        dropped: &[OwnedAdjEntry],
    ) -> Result<()> {
        if inserts.is_empty() && deletes.is_empty() && dropped.is_empty() {
            return Ok(());
        }
        let slots = dropped
            .len()
            .checked_mul(std::mem::size_of::<&OwnedAdjEntry>())
            .ok_or_else(|| corruption("dropped owner lookup size overflow"))?;
        crate::read_image_memory::reserve(slots)?;
        let mut dropped_index: Vec<_> = dropped.iter().collect();
        dropped_index.sort_unstable_by(|a, b| a.owner.cmp(&b.owner));
        if dropped_index.windows(2).any(|p| p[0].owner == p[1].owner) {
            return Err(corruption("table drop repeats an exact owned witness"));
        }
        Self::erase_row_graph_in_write(write, dropped)?;
        // The producer root hook authenticates both pre-mutation roots and
        // captures the replacements when both handles close, before commit.
        let mut forward = write
            .open_table(row_graph::FORWARD)
            .map_err(Self::storage_error)?;
        let mut reverse = write
            .open_table(row_graph::REVERSE)
            .map_err(Self::storage_error)?;
        for (before, deleted_tx) in deletes {
            row_graph::validate(before)?;
            if let Ok(at) = dropped_index.binary_search_by(|e| e.owner.cmp(&before.owner)) {
                if dropped_index[at].entry != before.entry {
                    return Err(corruption(
                        "dropped owner and deletion before-image disagree",
                    ));
                }
                continue;
            }
            let fwd = Self::row_graph_key(before, false)?;
            let rev = Self::row_graph_key(before, true)?;
            let stored = forward
                .get(fwd.as_slice())
                .map_err(Self::storage_error)?
                .ok_or_else(|| corruption("row deletion has no owned forward witness"))?;
            let stored_reverse = reverse
                .get(rev.as_slice())
                .map_err(Self::storage_error)?
                .ok_or_else(|| corruption("row deletion has no owned reverse witness"))?;
            if stored.value() != stored_reverse.value() {
                return Err(corruption("owned forward and reverse witnesses disagree"));
            }
            let mut owned: OwnedAdjEntry = Self::decode(stored.value())?;
            if owned.owner != before.owner || owned.entry != before.entry {
                return Err(corruption(
                    "row deletion does not match its exact owned witness",
                ));
            }
            if owned.entry.deleted_tx.is_some_and(|tx| tx != *deleted_tx) {
                return Err(corruption(
                    "row deletion changes an existing witness interval",
                ));
            }
            owned.entry.deleted_tx = Some(*deleted_tx);
            let encoded = row_graph::encode_row_graph(&owned)?;
            drop(stored_reverse);
            drop(stored);
            forward
                .insert(fwd.as_slice(), encoded.as_slice())
                .map_err(Self::storage_error)?;
            reverse
                .insert(rev.as_slice(), encoded.as_slice())
                .map_err(Self::storage_error)?;
        }
        for owned in inserts {
            row_graph::validate(owned)?;
            if dropped_index
                .binary_search_by(|e| e.owner.cmp(&owned.owner))
                .is_ok()
            {
                return Err(corruption(
                    "row insertion reuses an erased table-generation owner",
                ));
            }
            let fwd = Self::row_graph_key(owned, false)?;
            let rev = Self::row_graph_key(owned, true)?;
            let encoded = row_graph::encode_row_graph(owned)?;
            if forward
                .get(fwd.as_slice())
                .map_err(Self::storage_error)?
                .is_some()
                || reverse
                    .get(rev.as_slice())
                    .map_err(Self::storage_error)?
                    .is_some()
            {
                return Err(corruption(
                    "owned adjacency would overwrite an existing row version",
                ));
            }
            forward
                .insert(fwd.as_slice(), encoded.as_slice())
                .map_err(Self::storage_error)?;
            reverse
                .insert(rev.as_slice(), encoded.as_slice())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }
}
