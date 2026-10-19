//! Immutable pending-delete roots, changed only by lineage publication.
use super::*;
use contextdb_core::read_memory::{ReadCredit, ReadMemoryBudget};
use contextdb_relational::membership::MembershipImage;
use contextdb_relational::store::IndexEntry;
use parking_lot::{ArcMutexGuard, RawMutex};
use std::ops::Bound;

/// Reuse the store's admitted persistent membership tree. The coordinate is
/// (delete LSN, canonical lifecycle key); its final opaque field retains the
/// record itself. Replacing a record copies a search path, while a captured
/// root keeps the original fields without copying the remaining history.
pub(super) struct ResidentPendingSource {
    pub(super) image: MembershipImage,
    _control: ReadCredit,
}

pub(super) struct PreparedPendingRoot {
    original: ArcMutexGuard<RawMutex, ResidentPendingSource>,
    image: MembershipImage,
}

pub(super) struct PreparedPendingDelta {
    source: PreparedPendingRoot,
    pub(super) state: ArcMutexGuard<RawMutex, InMemoryLineageState>,
}

impl PreparedPendingRoot {
    pub(super) fn publish(mut self) {
        std::mem::swap(&mut self.original.image, &mut self.image);
    }

    fn update(
        &mut self,
        key: &str,
        previous: Option<&DurableLineageRecord>,
        replacement: Option<&DurableLineageRecord>,
        budget: Arc<dyn ReadMemoryBudget>,
    ) -> Result<()> {
        if let Some(previous) =
            previous.filter(|record| record.delete_obligation == DurableDeleteObligation::Pending)
        {
            let (_scratch, prefix) = coordinate(previous.delete_lsn, key, budget.clone())?;
            // Borrow the old immutable root, rather than copying a record in
            // order to remove its current membership from the prepared root.
            let old = self.image.clone();
            if let Some((position, entry)) = old.next(None, Bound::Included(&prefix), false)
                && position.get(..2) == Some(prefix.as_slice())
            {
                self.image.remove(position, entry.row_id, TxId(0))?;
            }
        }
        if let Some(record) = replacement
            .filter(|record| record.delete_obligation == DurableDeleteObligation::Pending)
        {
            let (_scratch, mut position) = coordinate(record.delete_lsn, key, budget.clone())?;
            // Fund binary encoding, temporary JSON text for JSON-valued
            // identities, and the binary/text overlap before serialization.
            // Hex keeps every supported scalar bit pattern lossless.
            let record_bytes = std::mem::size_of::<DurableLineageRecord>()
                .saturating_add(record.natural_key.held_bytes())
                .saturating_add(record.table.len())
                .saturating_add(record.lineage_root.len())
                .saturating_add(record.lineage_attestation.len())
                .saturating_add(record.author_node_id.as_ref().map_or(0, String::len))
                .saturating_add(
                    record
                        .author_database_incarnation
                        .as_ref()
                        .map_or(0, String::len),
                )
                .saturating_add(record.bound_hub_node_id.as_ref().map_or(0, String::len))
                .saturating_add(record.purge_frontier.as_ref().map_or(0, String::len));
            // JSON escaping takes at most six bytes per input byte; four
            // overlapping encoding buffers cover growth and the final hex.
            let _body = ReadCredit::try_new(
                budget,
                record_bytes
                    .saturating_mul(6)
                    .saturating_mul(4)
                    .saturating_add(4 * held::ALLOCATION_BYTES),
                "lineage",
                "pending_record",
                "Raise the declared memory limit to retain this pending deletion.",
            )?;
            let bytes = RedbPersistence::encode_config_value(record)?;
            let encoded = Database::hex_component(&bytes);
            position.push(contextdb_core::DirectedValue::Asc(
                contextdb_core::TotalOrdAsc(Value::Text(encoded)),
            ));
            let inserted = self.image.insert(
                &position,
                IndexEntry {
                    row_id: RowId(0),
                    created_tx: TxId(0),
                    deleted_tx: None,
                },
            );
            // The scratch body's owner was admitted after the coordinate;
            // destroy the coordinate before releasing that owner's credit,
            // including when preparing the new path was refused.
            drop(position);
            inserted?;
        }
        Ok(())
    }
}

impl PreparedPendingDelta {
    pub(super) fn publish(self, delta: InMemoryLineageDelta) {
        let Self { source, mut state } = self;
        install(&mut state, delta);
        source.publish();
    }
}

pub(super) fn install(state: &mut InMemoryLineageState, delta: InMemoryLineageDelta) {
    if let Some(image) = delta.connected_membership { state.connected_membership = image; }
    state.unbound_creations.extend(delta.unbound_creations);
    state.row_sidecars.extend(delta.row_sidecars);
    state.records.extend(delta.records);
    state.purged_history.extend(delta.purged_history);
    state.push_outcomes.extend(delta.push_outcomes);
}

impl Database {
    fn resident_pending_source(&self) -> Result<Arc<Mutex<ResidentPendingSource>>> {
        let mut slot = self.snapshot_registry.pending_delete_source.lock();
        if let Some(source) = slot.as_ref() {
            return Ok(source.clone());
        }
        let budget = self.accountant.read_budget()?;
        let control = ReadCredit::try_new(
            budget.clone(),
            std::mem::size_of::<Mutex<ResidentPendingSource>>()
                .saturating_add(2 * std::mem::size_of::<usize>()),
            "lineage",
            "pending_source",
            "Raise the declared memory limit to retain pending deletions.",
        )?;
        let mut image = MembershipImage::default();
        image.admit(budget)?;
        let source = Arc::new(Mutex::new(ResidentPendingSource {
            image,
            _control: control,
        }));
        *slot = Some(source.clone());
        Ok(source)
    }

    pub(super) fn prepare_resident_pending_record(
        &self,
        state: &InMemoryLineageState,
        key: &str,
        record: Option<&DurableLineageRecord>,
    ) -> Result<PreparedPendingRoot> {
        self.prepare_resident_pending_changes(state, std::iter::once((key, record)))
    }

    pub(super) fn prepare_resident_pending_changes<'a>(
        &self,
        state: &InMemoryLineageState,
        records: impl IntoIterator<Item = (&'a str, Option<&'a DurableLineageRecord>)>,
    ) -> Result<PreparedPendingRoot> {
        let source = self.resident_pending_source()?;
        let original = source.lock_arc();
        let mut prepared = PreparedPendingRoot {
            image: original.image.clone(),
            original,
        };
        let budget = self.accountant.read_budget()?;
        for (key, record) in records {
            prepared.update(key, state.records.get(key), record, budget.clone())?;
        }
        Ok(prepared)
    }

    pub(super) fn prepare_resident_pending_delta(
        &self,
        records: &HashMap<String, DurableLineageRecord>,
    ) -> Result<Option<PreparedPendingDelta>> {
        if records.is_empty() {
            return Ok(None);
        }
        // Transaction preparation already owns the commit gate. Keep the
        // canonical state before the source lock, exactly as direct metadata
        // writers do, until the prepared publication consumes both guards.
        debug_assert!(self.tx_mgr.commit_boundary_held_by_current_thread());
        let state = self.lineage_state_lock.lock_arc();
        if !records.iter().any(|(key, record)| {
            record.delete_obligation == DurableDeleteObligation::Pending
                || state
                    .records
                    .get(key)
                    .is_some_and(|old| old.delete_obligation == DurableDeleteObligation::Pending)
        }) {
            return Ok(None);
        }
        let source = self.resident_pending_source()?;
        let original = source.lock_arc();
        let mut prepared = PreparedPendingRoot {
            image: original.image.clone(),
            original,
        };
        let budget = self.accountant.read_budget()?;
        for (key, record) in records {
            prepared.update(key, state.records.get(key), Some(record), budget.clone())?;
        }
        Ok(Some(PreparedPendingDelta {
            source: prepared,
            state,
        }))
    }
}

fn coordinate(
    lsn: u64,
    key: &str,
    budget: Arc<dyn ReadMemoryBudget>,
) -> Result<(ReadCredit, IndexKey)> {
    let credit = ReadCredit::try_new(
        budget,
        3 * std::mem::size_of::<contextdb_core::DirectedValue>()
            + 20
            + key.len()
            + 3 * held::ALLOCATION_BYTES,
        "lineage",
        "pending_coordinate",
        "Raise the declared memory limit to retain this pending coordinate.",
    )?;
    let mut position = Vec::with_capacity(3);
    position.push(contextdb_core::DirectedValue::Asc(
        contextdb_core::TotalOrdAsc(Value::Text(format!("{lsn:020}"))),
    ));
    position.push(contextdb_core::DirectedValue::Asc(
        contextdb_core::TotalOrdAsc(Value::Text(key.into())),
    ));
    Ok((credit, position))
}

pub(super) struct ResidentPendingCursor {
    image: MembershipImage,
    through: Lsn,
    start: IndexKey,
    position: Option<IndexKey>,
    position_credit: ReadCredit,
    peeked: Option<Lsn>,
    memory: Arc<dyn ReadMemoryBudget>,
    accountant: crate::memory_accounting::AccountantHandle,
    _start_credit: ReadCredit,
}

impl ResidentPendingCursor {
    pub(super) fn new(
        image: MembershipImage,
        since: Lsn,
        through: Lsn,
        memory: Arc<dyn ReadMemoryBudget>,
        accountant: crate::memory_accounting::AccountantHandle,
    ) -> Result<Option<Self>> {
        if since >= through {
            return Ok(None);
        }
        let start_credit = ReadCredit::try_new(
            memory.clone(),
            std::mem::size_of::<contextdb_core::DirectedValue>() + 20 + 2 * held::ALLOCATION_BYTES,
            "reader",
            "pull_source_window",
            "Raise the declared memory limit to admit this pending cursor.",
        )?;
        let start = vec![contextdb_core::DirectedValue::Asc(
            contextdb_core::TotalOrdAsc(Value::Text(format!("{:020}", since.0 + 1))),
        )];
        Ok(Some(Self {
            image,
            through,
            start,
            position: None,
            position_credit: ReadCredit::new(memory.clone()),
            peeked: None,
            memory,
            accountant,
            _start_credit: start_credit,
        }))
    }

    pub(super) fn peek_lsn(&mut self) -> Result<Option<Lsn>> {
        if self.peeked.is_some() {
            return Ok(self.peeked);
        }
        let next = match self.position.as_ref() {
            None => self.image.next(None, Bound::Included(&self.start), false),
            Some(position) => {
                let previous = self
                    .image
                    .next(None, Bound::Included(position), false)
                    .filter(|(key, _)| key.get(..2) == Some(position.as_slice()))
                    .ok_or_else(|| corrupt("captured pending coordinate is absent"))?;
                self.image.next(
                    Some((previous.0, previous.1.row_id)),
                    Bound::Unbounded,
                    false,
                )
            }
        };
        let Some((key, _)) = next else {
            return Ok(None);
        };
        let lsn = match key.first() {
            Some(contextdb_core::DirectedValue::Asc(contextdb_core::TotalOrdAsc(Value::Text(
                text,
            )))) => text.parse::<u64>().ok().map(Lsn),
            _ => None,
        }
        .ok_or_else(|| corrupt("invalid resident pending coordinate"))?;
        if lsn > self.through {
            return Ok(None);
        }
        // Only the coordinate is copied for a lookahead. The unselected
        // record remains borrowed in the immutable store root.
        let bytes = key.iter().take(2).try_fold(
            2 * std::mem::size_of::<contextdb_core::DirectedValue>() + 3 * held::ALLOCATION_BYTES,
            |bytes, value| match value {
                contextdb_core::DirectedValue::Asc(contextdb_core::TotalOrdAsc(Value::Text(
                    text,
                ))) => Ok(bytes.saturating_add(text.len())),
                _ => Err(corrupt("invalid resident pending key")),
            },
        )?;
        let credit = ReadCredit::try_new(
            self.memory.clone(),
            bytes,
            "reader",
            "pull_source_window",
            "Raise the declared memory limit to admit this pending coordinate.",
        )?;
        let position = key[..2].to_vec();
        self.position = Some(position);
        self.position_credit = credit;
        self.peeked = Some(lsn);
        #[cfg(any(test, feature = "test-seams"))]
        resident_history_test_seams::note(|work| work.pull_source_entries += 1);
        Ok(self.peeked)
    }

    pub(super) fn next_record(
        &mut self,
    ) -> Result<
        Option<(
            DurableLineageRecord,
            Arc<crate::read_image_memory::ImageMemory>,
        )>,
    > {
        let Some(lsn) = self.peek_lsn()? else {
            return Ok(None);
        };
        let position = self
            .position
            .as_ref()
            .expect("a lookahead has its coordinate");
        let (key, _) = self
            .image
            .next(None, Bound::Included(position), false)
            .filter(|(key, _)| key.get(..2) == Some(position.as_slice()))
            .ok_or_else(|| corrupt("selected pending record is absent"))?;
        let body = match key.get(2) {
            Some(contextdb_core::DirectedValue::Asc(contextdb_core::TotalOrdAsc(Value::Text(
                body,
            )))) => body,
            _ => return Err(corrupt("selected pending record has no body")),
        };
        let memory = crate::read_image_memory::ImageMemory::operation_reader(
            self.accountant.clone(),
            self.memory.clone(),
        )?;
        if body.len() % 2 != 0 {
            return Err(corrupt("invalid resident pending body encoding"));
        }
        let _encoded = ReadCredit::try_new(
            self.memory.clone(),
            body.len() / 2 + held::ALLOCATION_BYTES,
            "reader",
            "pull_source_window",
            "Raise the declared memory limit to decode this pending deletion.",
        )?;
        let mut encoded = Vec::with_capacity(body.len() / 2);
        for pair in body.as_bytes().chunks_exact(2) {
            let digit = |byte: u8| match byte {
                b'0'..=b'9' => Some(byte - b'0'),
                b'a'..=b'f' => Some(byte - b'a' + 10),
                _ => None,
            };
            let hi =
                digit(pair[0]).ok_or_else(|| corrupt("invalid resident pending body encoding"))?;
            let lo =
                digit(pair[1]).ok_or_else(|| corrupt("invalid resident pending body encoding"))?;
            encoded.push((hi << 4) | lo);
        }
        let record: DurableLineageRecord =
            memory.scope(|| RedbPersistence::decode_config_value_exact(&encoded))?;
        if record.delete_lsn != lsn.0
            || record.delete_obligation != DurableDeleteObligation::Pending
        {
            return Err(corrupt(
                "resident pending coordinate disagrees with its record",
            ));
        }
        self.peeked = None;
        Ok(Some((record, memory)))
    }
}

fn corrupt(reason: &str) -> Error {
    Error::StoreCorrupted {
        path: "pending delete history".into(),
        reason: reason.into(),
    }
}
