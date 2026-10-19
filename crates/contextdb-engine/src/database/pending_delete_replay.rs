//! The ordered pending-delete source at a pull's captured writer root.
use super::*;
use crate::persistence::writer_source::{SourceWindowOwner, WriterReadCursor, WriterWindow};

pub(super) struct PendingReplay {
    resident: Option<super::resident_pending::ResidentPendingCursor>,
    cursor: Option<WriterReadCursor>,
    root: Option<crate::persistence::WriterSnapshotHandle>,
    owner: Option<SourceWindowOwner>,
    peeked: Option<WriterWindow<Lsn>>,
}

impl PendingReplay {
    pub(super) fn empty() -> Self {
        Self {
            resident: None,
            cursor: None,
            root: None,
            owner: None,
            peeked: None,
        }
    }

    pub(super) fn new(
        root: crate::persistence::WriterSnapshotHandle,
        owner: SourceWindowOwner,
        memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        since: Lsn,
    ) -> Result<Self> {
        use std::fmt::Write;
        if since >= root.lsn {
            return Ok(Self::empty());
        }
        let prefix = "sync_lineage.v1.pending.";
        let _key = contextdb_core::read_memory::ReadCredit::try_new(
            memory,
            prefix.len() + 21 + held::ALLOCATION_BYTES,
            "reader",
            "pull_source_window",
            "Raise the declared memory limit to admit this history endpoint.",
        )?;
        let mut start = String::with_capacity(prefix.len() + 21);
        write!(&mut start, "{prefix}{:020}.", since.0 + 1).expect("string write");
        let cursor = root.lineage_cursor_from(owner.clone(), prefix, &start)?;
        Ok(Self {
            resident: None,
            cursor: Some(cursor),
            root: Some(root),
            owner: Some(owner),
            peeked: None,
        })
    }

    pub(super) fn resident(
        image: Option<contextdb_relational::membership::MembershipImage>,
        since: Lsn,
        through: Lsn,
        memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        accountant: crate::memory_accounting::AccountantHandle,
    ) -> Result<Self> {
        let mut source = Self::empty();
        if let Some(image) = image {
            source.resident = super::resident_pending::ResidentPendingCursor::new(
                image, since, through, memory, accountant,
            )?;
        }
        Ok(source)
    }

    fn fill(&mut self) -> Result<()> {
        if self.peeked.is_none()
            && let Some(cursor) = self.cursor.as_mut()
        {
            self.peeked = cursor.next(|key, _| {
                #[cfg(any(test, feature = "test-seams"))]
                if let Ok(key) = std::str::from_utf8(key) {
                    crate::writer_query_test_seams::note_config_enumeration(key);
                }
                let lsn = std::str::from_utf8(key)
                    .ok()
                    .and_then(|key| key.strip_prefix("sync_lineage.v1.pending."))
                    .and_then(|key| key.split_once('.'))
                    .filter(|(lsn, suffix)| {
                        lsn.len() == 20
                            && !suffix.is_empty()
                            && lsn.bytes().all(|byte| byte.is_ascii_digit())
                    })
                    .and_then(|(lsn, _)| lsn.parse::<u64>().ok())
                    .map(Lsn)
                    .ok_or_else(|| Self::corrupt("invalid ordered pending coordinate"))?;
                Ok(lsn)
            })?;
            #[cfg(any(test, feature = "test-seams"))]
            if self.peeked.is_some() {
                resident_history_test_seams::note(|work| work.pull_source_entries += 1);
            }
            if let (Some(root), Some(position)) = (&self.root, &self.peeked)
                && position.value > root.lsn
            {
                return Err(Self::corrupt(
                    "pending occurrence exceeds its published frontier",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn peek_lsn(&mut self) -> Result<Option<Lsn>> {
        if let Some(resident) = self.resident.as_mut() {
            return resident.peek_lsn();
        }
        self.fill()?;
        Ok(self.peeked.as_ref().map(|window| window.value))
    }

    pub(super) fn next_record(&mut self) -> Result<Option<PendingRecordWindow>> {
        if let Some(resident) = self.resident.as_mut() {
            return resident.next_record().map(|record| {
                record.map(|(value, memory)| PendingRecordWindow {
                    value,
                    owner: PendingRecordOwner::Resident { _memory: memory },
                })
            });
        }
        self.fill()?;
        let Some(position) = self.peeked.take() else {
            return Ok(None);
        };
        let root = self.root.as_ref().expect("pending source has its root");
        let owner = self.owner.as_ref().expect("pending source has its owner");
        let key = std::str::from_utf8(position.position())
            .map_err(|_| Self::corrupt("invalid pending key"))?;
        let reference = root
            .lineage_point::<String>(owner.clone(), key)?
            .ok_or_else(|| Self::corrupt("selected pending occurrence has no reference"))?;
        let suffix = reference
            .value
            .strip_prefix(Database::DURABLE_LINEAGE_CONFIG_PREFIX)
            .ok_or_else(|| Self::corrupt("pending reference is not a lifecycle key"))?;
        let selected_suffix = key
            .strip_prefix("sync_lineage.v1.pending.")
            .and_then(|key| key.split_once('.'))
            .map(|(_, suffix)| suffix)
            .ok_or_else(|| Self::corrupt("invalid pending occurrence key"))?;
        if suffix != selected_suffix {
            return Err(Self::corrupt(
                "pending coordinate disagrees with its reference",
            ));
        }
        let record = root
            .lineage_point::<DurableLineageRecord>(owner.clone(), &reference.value)?
            .ok_or_else(|| Self::corrupt("pending occurrence has no lifecycle record"))?;
        if record.value.delete_lsn != position.value.0
            || record.value.delete_obligation != DurableDeleteObligation::Pending
        {
            return Err(Self::corrupt(
                "pending occurrence disagrees with its lifecycle state",
            ));
        }
        let (value, window) = record.into_parts();
        Ok(Some(PendingRecordWindow {
            value,
            owner: PendingRecordOwner::Durable { _window: window },
        }))
    }

    fn corrupt(reason: &str) -> Error {
        Error::StoreCorrupted {
            path: "pending delete history".into(),
            reason: reason.into(),
        }
    }
}

pub(super) struct PendingRecordWindow {
    value: DurableLineageRecord,
    owner: PendingRecordOwner,
}
impl PendingRecordWindow {
    pub(super) fn into_parts(self) -> (DurableLineageRecord, PendingRecordOwner) {
        (self.value, self.owner)
    }
}
pub(super) enum PendingRecordOwner {
    Durable {
        _window: WriterWindow<()>,
    },
    Resident {
        _memory: Arc<crate::read_image_memory::ImageMemory>,
    },
}
