//! Retained memory schema history. Capacity preparation precedes publication;
//! the original writer owns each allocation until it is physically destroyed.
use super::*;
use crate::memory_accounting::{
    grow_preparation_from_original, merge_preparation_original, release_preparation_original,
    reserve_preparation_from_original,
};

const HINT: &str = "Reduce retained schema history or raise MEMORY_LIMIT.";
const HEADER: usize = crate::sync_types::held::ALLOCATION_BYTES;
fn array<T>(capacity: usize) -> usize {
    if capacity == 0 {
        0
    } else {
        capacity
            .saturating_mul(std::mem::size_of::<T>())
            .saturating_add(HEADER)
    }
}
fn text(value: &String, copying: bool) -> usize {
    let capacity = if copying {
        value.len()
    } else {
        value.capacity()
    };
    array::<u8>(capacity)
}
fn strings(values: &Vec<String>, copying: bool) -> usize {
    array::<String>(if copying {
        values.len()
    } else {
        values.capacity()
    })
    .saturating_add(
        values
            .iter()
            .map(|value| text(value, copying))
            .sum::<usize>(),
    )
}
/// The actual owned heap, or the exact deep-clone allocation plan. The enum's
/// inline slot belongs to the log backing, never to this payload invoice.
fn payload(change: &DdlChange, copying: bool) -> usize {
    match change {
        DdlChange::CreateTable {
            name,
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        }
        | DdlChange::AlterTable {
            name,
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        } => {
            text(name, copying)
                + array::<(String, String)>(if copying {
                    columns.len()
                } else {
                    columns.capacity()
                })
                + columns
                    .iter()
                    .map(|(name, ty)| text(name, copying) + text(ty, copying))
                    .sum::<usize>()
                + strings(constraints, copying)
                + array::<contextdb_core::SingleColumnForeignKey>(if copying {
                    foreign_keys.len()
                } else {
                    foreign_keys.capacity()
                })
                + foreign_keys
                    .iter()
                    .map(|key| {
                        text(&key.child_column, copying)
                            + text(&key.parent_table, copying)
                            + text(&key.parent_column, copying)
                    })
                    .sum::<usize>()
                + array::<contextdb_core::CompositeForeignKey>(if copying {
                    composite_foreign_keys.len()
                } else {
                    composite_foreign_keys.capacity()
                })
                + composite_foreign_keys
                    .iter()
                    .map(|key| {
                        text(&key.parent_table, copying)
                            + strings(&key.child_columns, copying)
                            + strings(&key.parent_columns, copying)
                    })
                    .sum::<usize>()
                + array::<Vec<String>>(if copying {
                    composite_unique.len()
                } else {
                    composite_unique.capacity()
                })
                + composite_unique
                    .iter()
                    .map(|keys| strings(keys, copying))
                    .sum::<usize>()
        }
        DdlChange::DropTable { name } | DdlChange::DropTrigger { name } => text(name, copying),
        DdlChange::CreateIndex {
            table,
            name,
            columns,
        } => {
            text(table, copying)
                + text(name, copying)
                + array::<(String, contextdb_core::SortDirection)>(if copying {
                    columns.len()
                } else {
                    columns.capacity()
                })
                + columns
                    .iter()
                    .map(|(name, _)| text(name, copying))
                    .sum::<usize>()
        }
        DdlChange::DropIndex { table, name } | DdlChange::DropRoute { name, table } => {
            text(table, copying) + text(name, copying)
        }
        DdlChange::CreateTrigger {
            name,
            table,
            on_events,
        }
        | DdlChange::CreateTriggerIncludingSync {
            name,
            table,
            on_events,
        } => text(name, copying) + text(table, copying) + strings(on_events, copying),
        DdlChange::CreateEventType {
            name,
            trigger,
            table,
        } => text(name, copying) + text(trigger, copying) + text(table, copying),
        DdlChange::CreateSink {
            name,
            sink_type,
            url,
        } => {
            text(name, copying)
                + text(sink_type, copying)
                + url.as_ref().map_or(0, |url| text(url, copying))
        }
        DdlChange::CreateRoute {
            name,
            event_type,
            sink,
            table,
            where_in,
        } => {
            text(name, copying)
                + text(event_type, copying)
                + text(sink, copying)
                + text(table, copying)
                + where_in.as_ref().map_or(0, |(column, values)| {
                    text(column, copying) + strings(values, copying)
                })
        }
    }
}

pub(super) struct SchemaHistoryMemory {
    ddl_log: Arc<RwLock<Vec<(Lsn, DdlChange)>>>,
    generations: Arc<Mutex<InMemoryDdlGenerations>>,
    backing: OwnedMemoryReservation,
    payload: OwnedMemoryReservation,
    log_backing_bytes: usize,
    generation_backing_bytes: usize,
    pending_log: usize,
    pending_generations: usize,
    pending_bytes: usize,
}
impl SchemaHistoryMemory {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn prepare_capacity(&mut self, logs: usize, generations: usize) -> Result<()> {
        let mut log = self.ddl_log.write();
        let required = log
            .len()
            .checked_add(self.pending_log)
            .and_then(|n| n.checked_add(logs))
            .ok_or_else(EngineMetadataJournal::identity_error)?;
        if required > log.capacity() {
            let capacity = log
                .capacity()
                .checked_mul(2)
                .map(|n| n.max(required))
                .ok_or_else(EngineMetadataJournal::identity_error)?;
            let next = array::<(Lsn, DdlChange)>(capacity);
            grow_preparation_from_original(
                &mut self.backing,
                next,
                "ddl",
                "history_log_capacity",
                HINT,
            )?;
            let additional = capacity - log.len();
            if log.try_reserve_exact(additional).is_err() {
                release_preparation_original(&mut self.backing, next);
                return Err(Error::Other(
                    "DDL history capacity allocation failed".into(),
                ));
            }
            // Vec has released the displaced allocation before its credit.
            release_preparation_original(&mut self.backing, self.log_backing_bytes);
            self.log_backing_bytes = next;
            assert_eq!(log.capacity(), capacity, "prepared history Vec capacity");
        }
        drop(log);
        let mut map = self.generations.lock();
        let required = map
            .len()
            .checked_add(self.pending_generations)
            .and_then(|n| n.checked_add(generations))
            .ok_or_else(EngineMetadataJournal::identity_error)?;
        if required > map.capacity() {
            let plan = crate::memory_accounting::native_map_plan::<
                DdlGenerationKey,
                DdlGenerationValue,
            >(required)?;
            let next = plan.layout.size().saturating_add(HEADER);
            grow_preparation_from_original(
                &mut self.backing,
                next,
                "ddl",
                "history_generation_capacity",
                HINT,
            )?;
            let additional = required - map.len();
            if map.try_reserve(additional).is_err() {
                release_preparation_original(&mut self.backing, next);
                return Err(Error::Other(
                    "DDL generation capacity allocation failed".into(),
                ));
            }
            release_preparation_original(&mut self.backing, self.generation_backing_bytes);
            self.generation_backing_bytes = next;
            crate::memory_accounting::validate_native_map_capacity(&map, &plan)?;
        }
        Ok(())
    }
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature"
        )
    )]
    pub(super) fn bytes(&self) -> usize {
        self.backing
            .bytes()
            .saturating_add(self.payload.bytes())
            .saturating_add(self.pending_bytes)
    }
}
impl Drop for SchemaHistoryMemory {
    fn drop(&mut self) {
        drop(std::mem::take(&mut *self.ddl_log.write()));
        drop(std::mem::take(&mut *self.generations.lock()));
        // Reservation fields refund only after both physical buffers are gone.
    }
}

/// Slot claims include all simultaneous stages, so preparing a trigger suffix
/// cannot consume capacity already promised to its event prefix. Drop rolls
/// back only the claim; retained physical capacity remains paid.
///
/// The strong owner is the same already-funded store finalization allocation.
/// Erasing only its rollback capability seals the private recursive ownership
/// graph; Arc unsizing allocates nothing and retains the original writer use.
trait HistorySlotOwner: Send + Sync {
    fn release_history_slots(&self, logs: usize, generations: usize, bytes: usize);
}
impl HistorySlotOwner for StoreFinalization {
    fn release_history_slots(&self, logs: usize, generations: usize, bytes: usize) {
        let mut retained = self.schema_history_memory.lock();
        if let Some(owner) = retained.as_mut() {
            owner.pending_log -= logs;
            owner.pending_generations -= generations;
            owner.pending_bytes -= bytes;
        }
    }
}
struct HistorySlots {
    finalization: Arc<dyn HistorySlotOwner>,
    logs: usize,
    generations: usize,
    bytes: usize,
}
impl HistorySlots {
    fn release(&mut self, owner: &mut SchemaHistoryMemory) {
        owner.pending_log -= std::mem::take(&mut self.logs);
        owner.pending_generations -= std::mem::take(&mut self.generations);
        owner.pending_bytes -= std::mem::take(&mut self.bytes);
    }
}
impl Drop for HistorySlots {
    fn drop(&mut self) {
        if self.logs == 0 && self.generations == 0 && self.bytes == 0 {
            return;
        }
        // Clone no handles and allocate nothing on this rollback path.
        self.finalization
            .release_history_slots(self.logs, self.generations, self.bytes);
    }
}
pub(crate) struct PreparedSchemaHistory {
    ddl: Vec<(Lsn, DdlChange)>,
    generations: Vec<(DdlGenerationKey, DdlGenerationValue)>,
    credit: OwnedMemoryReservation,
    scratch: usize,
    slots: HistorySlots,
}
impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn initialize_schema_history_memory(&self) -> Result<()> {
        let mut retained = self.finalization.schema_history_memory.lock();
        if retained.is_none() {
            // Read images and selected-copy maps have independent invoices and
            // must never be adopted as previously uncharged writer history.
            assert!(
                self.ddl_log.read().is_empty() && self.in_memory_ddl_generations.lock().is_empty(),
                "writer history starts custody before its first allocation"
            );
            let payload = self.with_writer_preparation_original(|original| {
                reserve_preparation_from_original(original, 0, "ddl", "history_payload", HINT)
            })?;
            let backing = crate::memory_accounting::retain_preparation_original(&payload);
            *retained = Some(SchemaHistoryMemory {
                ddl_log: self.ddl_log.clone(),
                generations: self.in_memory_ddl_generations.clone(),
                backing,
                payload,
                log_backing_bytes: 0,
                generation_backing_bytes: 0,
                pending_log: 0,
                pending_generations: 0,
                pending_bytes: 0,
            });
        }
        Ok(())
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_staged_memory_schema_history(
        &self,
        lsn: Lsn,
        start: u32,
        ddl: &[DdlChange],
        sidecars: &[DurableDdlGenerationSidecar],
    ) -> Result<Option<PreparedSchemaHistory>> {
        // Detached capture keeps the existing authored log without installing
        // the stage publisher's generation mirror into the working image.
        let sidecars = if self.capture_detached_sync_write_set.load(Ordering::SeqCst) {
            &[][..]
        } else {
            sidecars
        };
        self.prepare_memory_schema_history(lsn, start, ddl, sidecars)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_memory_schema_history(
        &self,
        lsn: Lsn,
        start: u32,
        ddl: &[DdlChange],
        sidecars: &[DurableDdlGenerationSidecar],
    ) -> Result<Option<PreparedSchemaHistory>> {
        if self.persistence.is_some() || self.read_image_memory.is_some() {
            return Ok(None);
        }
        self.prepare_memory_history_entries(
            ddl.iter().map(|change| (lsn, change)),
            sidecars.iter().enumerate().map(|(offset, value)| {
                (
                    (lsn, start + offset as u32),
                    (&value.table, value.table_generation),
                )
            }),
        )
        .map(Some)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_memory_history_entries<'a>(
        &self,
        ddl: impl Iterator<Item = (Lsn, &'a DdlChange)> + Clone,
        generations: impl Iterator<Item = (DdlGenerationKey, (&'a Option<String>, Option<u64>))> + Clone,
    ) -> Result<PreparedSchemaHistory> {
        self.initialize_schema_history_memory()?;
        let logs = ddl.clone().count();
        let generation_count = generations.clone().count();
        let scratch = array::<(Lsn, DdlChange)>(logs)
            .saturating_add(array::<(DdlGenerationKey, DdlGenerationValue)>(
                generation_count,
            ));
        let payload_bytes = ddl
            .clone()
            .map(|(_, change)| payload(change, true))
            .sum::<usize>()
            .saturating_add(
                generations
                    .clone()
                    .map(|(_, (table, _))| table.as_ref().map_or(0, |table| text(table, true)))
                    .sum::<usize>(),
            );
        // Iterators are consumed below; pricing uses their cloned traversal.
        self.prepare_memory_history_entries_priced(
            ddl,
            generations,
            logs,
            generation_count,
            scratch,
            payload_bytes,
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_memory_history_entries_priced<'a>(
        &self,
        ddl: impl Iterator<Item = (Lsn, &'a DdlChange)> + Clone,
        generations: impl Iterator<Item = (DdlGenerationKey, (&'a Option<String>, Option<u64>))> + Clone,
        logs: usize,
        generation_count: usize,
        scratch: usize,
        payload_bytes: usize,
    ) -> Result<PreparedSchemaHistory> {
        let mut retained = self.finalization.schema_history_memory.lock();
        let owner = retained
            .as_mut()
            .expect("initialized schema history custody");
        owner.prepare_capacity(logs, generation_count)?;
        let credit = reserve_preparation_from_original(
            &owner.payload,
            scratch.saturating_add(payload_bytes),
            "ddl",
            "history_occurrences",
            HINT,
        )?;
        let mut entries = Vec::with_capacity(logs);
        entries.extend(ddl.map(|(lsn, change)| (lsn, change.clone())));
        let ddl = entries;
        let mut values = Vec::with_capacity(generation_count);
        values.extend(
            generations.map(|(key, (table, generation))| (key, (table.clone(), generation))),
        );
        let generations = values;
        owner.pending_log += logs;
        owner.pending_generations += generation_count;
        owner.pending_bytes += credit.bytes();
        let bytes = credit.bytes();
        Ok(PreparedSchemaHistory {
            ddl,
            generations,
            credit,
            scratch,
            slots: HistorySlots {
                finalization: self.finalization.clone(),
                logs,
                generations: generation_count,
                bytes,
            },
        })
    }
    pub(super) fn publish_memory_schema_history(&self, prepared: Option<PreparedSchemaHistory>) {
        let Some(mut prepared) = prepared else {
            return;
        };
        let mut retained = self.finalization.schema_history_memory.lock();
        let owner = retained.as_mut().expect("prepared schema history custody");
        let mut log = self.ddl_log.write();
        assert!(log.capacity() - log.len() >= prepared.ddl.len());
        log.extend(prepared.ddl.drain(..));
        drop(log);
        let mut generations = self.in_memory_ddl_generations.lock();
        assert!(generations.capacity() - generations.len() >= prepared.generations.len());
        for (key, value) in prepared.generations.drain(..) {
            if let Some(old) = generations.insert(key, value) {
                let bytes = old.0.as_ref().map_or(0, |table| text(table, false));
                drop(old);
                release_preparation_original(&mut owner.payload, bytes);
            }
        }
        drop(generations);
        drop(std::mem::take(&mut prepared.ddl));
        drop(std::mem::take(&mut prepared.generations));
        release_preparation_original(&mut prepared.credit, prepared.scratch);
        // Both credits came from the same held owner at preparation; this is
        // a capability/debit move, not an allocation or a new admission.
        merge_preparation_original(&mut owner.payload, &mut prepared.credit)
            .expect("prepared history credits retain their original funder");
        prepared.slots.release(owner);
        drop(retained);
    }
    /// A selected working-image map replaces its prior selection. Keep its
    /// paid backing, but destroy every displaced name before refunding it.
    /// The replacement ticket was fully prepared before this infallible step.
    pub(super) fn replace_memory_schema_generations(&self, prepared: PreparedSchemaHistory) {
        {
            let mut retained = self.finalization.schema_history_memory.lock();
            let owner = retained.as_mut().expect("prepared generation replacement");
            let mut generations = self.in_memory_ddl_generations.lock();
            let bytes = generations
                .values()
                .map(|(table, _)| table.as_ref().map_or(0, |table| text(table, false)))
                .sum();
            generations.clear();
            release_preparation_original(&mut owner.payload, bytes);
        }
        self.publish_memory_schema_history(Some(prepared));
    }
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from maintenance_and_limits.rs"
        )
    )]
    pub(super) fn schema_history_retained_bytes(&self) -> usize {
        self.finalization
            .schema_history_memory
            .lock()
            .as_ref()
            .map_or(0, SchemaHistoryMemory::bytes)
    }
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from constraint_validation.rs"
        )
    )]
    pub(super) fn clear_memory_schema_log(&self) {
        let mut retained = self.finalization.schema_history_memory.lock();
        let mut log = self.ddl_log.write();
        let bytes = log.iter().map(|(_, change)| payload(change, false)).sum();
        log.clear();
        if let Some(owner) = retained.as_mut() {
            release_preparation_original(&mut owner.payload, bytes);
        }
    }
}
