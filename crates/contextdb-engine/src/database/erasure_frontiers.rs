//! The memory database's canonical erasure controls. State clones share this
//! control; preparation reserves only new table entries, never a copied map.
use super::*;
use crate::memory_accounting::{
    OwnedMemoryReservation, merge_preparation_original, reserve_preparation_from_original,
};
use contextdb_core::read_memory::{ReadCredit, ReadShared};

struct Frontiers {
    tables: BTreeMap<String, Lsn>,
    global: Lsn,
    // Last: every key and native B-tree node is freed before its funding.
    memory: OwnedMemoryReservation,
}
#[derive(Clone, Default)]
pub(super) struct MemoryErasureFrontiers(Option<ReadShared<Mutex<Frontiers>>>);

pub(super) struct PreparedErasureFrontiers {
    root: ReadShared<Mutex<Frontiers>>,
    entries: Vec<(String, Lsn)>,
    retained: OwnedMemoryReservation,
    scratch: OwnedMemoryReservation,
}
// The pinned standard-library B-tree uses eleven keys and twelve children.
// Two internal-node layouts per new key bound the whole tree and its splits.
#[repr(C)]
struct Leaf {
    parent: Option<std::ptr::NonNull<()>>,
    parent_index: std::mem::MaybeUninit<u16>,
    len: u16,
    keys: [std::mem::MaybeUninit<String>; 11],
    values: [std::mem::MaybeUninit<Lsn>; 11],
}
#[repr(C)]
struct Internal {
    leaf: Leaf,
    children: [std::mem::MaybeUninit<std::ptr::NonNull<Leaf>>; 12],
}
impl MemoryErasureFrontiers {
    pub(super) fn global(&self) -> Lsn {
        self.0.as_ref().map_or(Lsn(0), |root| root.lock().global)
    }

    pub(super) fn copy_into(
        &self,
        destination: &RedbPersistence,
        memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        let Some(root) = &self.0 else {
            return Ok(());
        };
        // Keep only a funded cursor key. Release the source lock before each
        // destination write so an export never holds up ordinary publication.
        // A later erasure is detected by the export's final publication fence.
        let mut after: Option<(String, ReadCredit)> = None;
        loop {
            let next = {
                let frontiers = root.lock();
                let next = match &after {
                    Some((key, _)) => frontiers
                        .tables
                        .range::<str, _>((
                            std::ops::Bound::Excluded(key.as_str()),
                            std::ops::Bound::Unbounded,
                        ))
                        .next(),
                    None => frontiers.tables.first_key_value(),
                };
                next.map(|(table, frontier)| -> Result<_> {
                    let credit = ReadCredit::try_new(
                        memory.clone(),
                        table.len(),
                        "reader",
                        "export_erasure_frontiers",
                        "Raise the reader memory limit to export local erasure controls.",
                    )?;
                    let mut key = String::with_capacity(table.len());
                    key.push_str(table);
                    Ok((key, *frontier, credit))
                })
                .transpose()?
            };
            let Some((table, frontier, credit)) = next else {
                return Ok(());
            };
            destination.copy_erasure_frontier(&table, frontier)?;
            after = Some((table, credit));
        }
    }

    pub(super) fn validate(&self, scope: &crate::executor::ReadPublicationScope) -> Result<()> {
        scope.check_publication_budget()?;
        if let Some(root) = &self.0 {
            let frontiers = root.lock();
            if scope.tables.contains("*") {
                scope.check_erasure_frontier("*", frontiers.global)?;
            } else {
                for table in &scope.tables {
                    scope.check_publication_budget()?;
                    scope.check_erasure_frontier(
                        table,
                        frontiers.tables.get(table).copied().unwrap_or(Lsn(0)),
                    )?;
                }
            }
        }
        scope.check_publication_budget()
    }
    pub(super) fn prepare(
        &self,
        tables: &BTreeSet<String>,
        frontier: Lsn,
        accountant: crate::memory_accounting::AccountantHandle,
    ) -> Result<PreparedErasureFrontiers> {
        let root = match &self.0 {
            Some(root) => root.clone(),
            None => {
                let control = ReadCredit::try_new(
                    accountant.read_budget()?,
                    ReadShared::<Mutex<Frontiers>>::layout().size(),
                    "erasure",
                    "prepare_read_frontiers",
                    "Raise MEMORY_LIMIT to retain local erasure controls.",
                )?;
                let memory = OwnedMemoryReservation::try_new_for(
                    accountant,
                    0,
                    "erasure",
                    "prepare_read_frontiers",
                    "Raise MEMORY_LIMIT to retain local erasure controls.",
                )?;
                ReadShared::try_from_prepaid(
                    Mutex::new(Frontiers {
                        tables: BTreeMap::new(),
                        global: Lsn(0),
                        memory,
                    }),
                    control,
                )?
            }
        };
        let current = root.lock();
        let mut retained = 0usize;
        let mut scratch = tables
            .len()
            .checked_mul(std::mem::size_of::<(String, Lsn)>())
            .ok_or_else(|| Error::Other("erasure control size overflow".into()))?;
        for table in tables {
            let bytes = if current.tables.contains_key(table) {
                &mut scratch
            } else {
                retained = retained
                    .checked_add(2 * std::mem::size_of::<Internal>())
                    .ok_or_else(|| Error::Other("erasure control size overflow".into()))?;
                &mut retained
            };
            *bytes = bytes
                .checked_add(table.len())
                .ok_or_else(|| Error::Other("erasure control size overflow".into()))?;
        }
        let retained = reserve_preparation_from_original(
            &current.memory,
            retained,
            "erasure",
            "prepare_read_frontiers",
            "Raise MEMORY_LIMIT to retain local erasure controls.",
        )?;
        let scratch = reserve_preparation_from_original(
            &current.memory,
            scratch,
            "erasure",
            "prepare_read_frontiers",
            "Raise MEMORY_LIMIT to prepare local erasure controls.",
        )?;
        let mut entries = Vec::with_capacity(tables.len());
        for table in tables {
            entries.push((table.clone(), frontier));
        }
        drop(current);
        Ok(PreparedErasureFrontiers {
            root,
            entries,
            retained,
            scratch,
        })
    }
}
impl PreparedErasureFrontiers {
    pub(super) fn publish(mut self) -> MemoryErasureFrontiers {
        let mut current = self.root.lock();
        // Preparation used this very original owner. No new admission or
        // allocation can fail after the enclosing erasure has committed.
        merge_preparation_original(&mut current.memory, &mut self.retained)
            .expect("erasure preparation retains its original memory owner");
        for (table, frontier) in self.entries.drain(..) {
            current.global = current.global.max(frontier);
            let value = current.tables.entry(table).or_insert(Lsn(0));
            *value = (*value).max(frontier);
        }
        drop(current);
        drop(self.entries);
        drop(self.scratch);
        MemoryErasureFrontiers(Some(self.root))
    }
}
