//! Final delivery projections share the ordinary row and schema commit boundary.
use super::{authority::*, canonical::*, records::*};
use crate::Database;
use crate::custody_types::*;
use crate::sync_types::{ChangeSet, NaturalKey, RowChange};
use contextdb_core::{Error, Incarnation, Result, TenantId};
use std::collections::BTreeMap;
use uuid::Uuid;
pub(crate) type LineageSigner = std::sync::Arc<dyn Fn(&[u8]) -> Result<Vec<u8>> + Send + Sync>;
#[derive(Clone)]
pub(crate) struct SourceRegistration {
    pub tenant: TenantId,
    pub edge: String,
    pub root: RowRef,
    pub members: Vec<RowRef>,
    pub signer: LineageSigner,
}
impl std::fmt::Debug for SourceRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceRegistration")
            .field("root", &self.root)
            .field("members", &self.members)
            .finish_non_exhaustive()
    }
}
impl SourceRegistration {
    pub(crate) fn prepare(
        &self,
        db: &Database,
        ws: &mut contextdb_tx::WriteSet,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Vec<Record>> {
        prepare_manifest(
            db,
            ws,
            &self.tenant,
            &self.edge,
            DeliveryManifest {
                root_table: &self.root.table,
                root_key: self.root.key.clone(),
                members: self
                    .members
                    .iter()
                    .map(|r| (r.table.as_str(), r.key.clone()))
                    .collect(),
            },
            &self.signer,
            read_memory.clone(),
        )
    }
}

pub(crate) type CommitPreparation<'a> =
    dyn FnMut(&Database, &mut contextdb_tx::WriteSet) -> Result<Vec<Record>> + 'a;

// Native payload fields precede their original reservations in every owner.
// None of these owners exports a payload or a replaceable funding provider.
pub(crate) type OwnedCommitPreparation<'a> = dyn for<'g, 'h> FnMut(
        &Database,
        &'g mut contextdb_tx::manager::EngineGuard<'h>,
    ) -> Result<OwnedPreparationRecords>
    + 'a;

pub(crate) enum DeliveryPreparation<'borrow, 'body> {
    Legacy(&'borrow mut CommitPreparation<'body>),
    Owned(&'borrow mut OwnedCommitPreparation<'body>),
}

pub(crate) struct OwnedPreparationRecords {
    records: Vec<Record>,
    debits: Vec<crate::memory_accounting::OwnedMemoryReservation>,
    buffers: crate::memory_accounting::OwnedMemoryReservation,
}

pub(crate) struct OwnedPreparationValue<T> {
    value: T,
    debit: crate::memory_accounting::OwnedMemoryReservation,
}

pub(crate) struct OwnedPreparationOutputs {
    batches: Vec<OwnedPreparationRecords>,
    debit: crate::memory_accounting::OwnedMemoryReservation,
}

pub(crate) fn preparation_array_bytes<T>(count: usize) -> Result<usize> {
    std::alloc::Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| invalid())
}

impl OwnedPreparationRecords {
    pub(crate) fn try_new(
        db: &Database,
        record_capacity: usize,
        debit_capacity: usize,
    ) -> Result<Self> {
        let bytes = preparation_array_bytes::<Record>(record_capacity)?
            .checked_add(preparation_array_bytes::<
                crate::memory_accounting::OwnedMemoryReservation,
            >(debit_capacity)?)
            .ok_or_else(invalid)?;
        let buffers = db.reserve_owned_preparation(bytes)?;
        let records = Vec::with_capacity(record_capacity);
        let debits = Vec::with_capacity(debit_capacity);
        // On capacity drift destroy both native buffers before returning credit.
        if records.capacity() != record_capacity || debits.capacity() != debit_capacity {
            drop(debits);
            drop(records);
            return Err(invalid());
        }
        Ok(Self {
            records,
            debits,
            buffers,
        })
    }

    pub(crate) fn with_records<R>(&self, f: impl for<'r> FnOnce(&'r [Record]) -> R) -> R {
        f(&self.records)
    }

    pub(crate) fn copy_record(&mut self, db: &Database, record: &Record) -> Result<()> {
        assert!(
            self.records.len() < self.records.capacity(),
            "preadmitted record slot"
        );
        assert!(
            self.debits.len() < self.debits.capacity(),
            "preadmitted original debit slot"
        );
        let copied = copy_record_owned(db, record)?;
        self.push(copied);
        Ok(())
    }

    fn push(&mut self, value: OwnedPreparationValue<Record>) {
        assert!(
            self.records.len() < self.records.capacity(),
            "preadmitted record slot"
        );
        assert!(
            self.debits.len() < self.debits.capacity(),
            "preadmitted original debit slot"
        );
        let OwnedPreparationValue { value, debit } = value;
        // An unwind after this move still retains the invoice until value dies.
        self.debits.push(debit);
        self.records.push(value);
    }

    fn extend(&mut self, mut other: Self) {
        assert!(other.records.len() <= self.records.capacity() - self.records.len());
        assert!(other.debits.len() <= self.debits.capacity() - self.debits.len());
        // Keep the other owner intact on unwind; its field order frees both
        // native backings before the original buffer debit can be refunded.
        self.debits.extend(other.debits.drain(..));
        self.records.extend(other.records.drain(..));
        drop(other);
    }

    fn grow(&mut self, db: &Database, record_capacity: usize, debit_capacity: usize) -> Result<()> {
        assert!(record_capacity >= self.records.len());
        assert!(debit_capacity >= self.debits.len());
        // Admit the whole overlapping successor, not a delta against live backing.
        let mut successor = Self::try_new(db, record_capacity, debit_capacity)?;
        std::mem::swap(&mut successor.records, &mut self.records);
        std::mem::swap(&mut successor.debits, &mut self.debits);
        // successor owns old backing and its still-live original buffer grant.
        std::mem::swap(&mut successor.buffers, &mut self.buffers);
        // Move original invoices first so every partially moved payload remains
        // funded even if a subsequent extension unwinds.
        self.debits.extend(successor.debits.drain(..));
        self.records.extend(successor.records.drain(..));
        drop(successor);
        Ok(())
    }
}

impl<T> OwnedPreparationValue<T> {
    pub(crate) fn try_produce(
        db: &Database,
        native_bytes: usize,
        produce: impl FnOnce() -> Result<T>,
    ) -> Result<Self> {
        let debit = db.reserve_owned_preparation(native_bytes)?;
        let value = produce()?;
        Ok(Self { value, debit })
    }

    pub(crate) fn with_value<R>(&self, f: impl for<'v> FnOnce(&'v T) -> R) -> R {
        f(&self.value)
    }

    fn transfer_into(
        self,
        target: &mut OwnedPreparationRecords,
        append: impl FnOnce(T, &mut OwnedPreparationRecords),
    ) {
        assert!(
            target.debits.len() < target.debits.capacity(),
            "preadmitted original debit slot"
        );
        let Self { value, debit } = self;
        target.debits.push(debit);
        append(value, target);
    }
}

impl OwnedPreparationOutputs {
    pub(crate) fn try_new(db: &Database, batch_capacity: usize) -> Result<Self> {
        let debit = db.reserve_owned_preparation(preparation_array_bytes::<
            OwnedPreparationRecords,
        >(batch_capacity)?)?;
        let batches = Vec::with_capacity(batch_capacity);
        if batches.capacity() != batch_capacity {
            drop(batches);
            return Err(invalid());
        }
        Ok(Self { batches, debit })
    }

    pub(crate) fn retain(&mut self, batch: OwnedPreparationRecords) {
        assert!(
            self.batches.len() < self.batches.capacity(),
            "preadmitted output slot"
        );
        self.batches.push(batch);
    }
}

impl SourceRegistration {
    pub(crate) fn manifest_input_owned<'a>(
        &'a self,
        db: &Database,
    ) -> Result<OwnedPreparationValue<DeliveryManifest<'a>>> {
        let mut bytes = preparation_array_bytes::<(&str, NaturalKey)>(self.members.len())?
            .checked_add(count_key_native(&self.root.key)?)
            .ok_or_else(invalid)?;
        for reference in &self.members {
            bytes = bytes
                .checked_add(count_key_native(&reference.key)?)
                .ok_or_else(invalid)?;
        }
        OwnedPreparationValue::try_produce(db, bytes, || {
            let mut members = Vec::with_capacity(self.members.len());
            if members.capacity() != self.members.len() {
                return Err(invalid());
            }
            for reference in &self.members {
                members.push((reference.table.as_str(), copy_key_native(&reference.key)?));
            }
            Ok(DeliveryManifest {
                root_table: &self.root.table,
                root_key: copy_key_native(&self.root.key)?,
                members,
            })
        })
    }
}

pub(crate) fn copy_row_owned(
    db: &Database,
    row: &contextdb_core::VersionedRow,
) -> Result<OwnedPreparationValue<contextdb_core::VersionedRow>> {
    let bytes = crate::memory_accounting::preparation_row_copy_bytes(row)?;
    OwnedPreparationValue::try_produce(db, bytes, || {
        crate::memory_accounting::preparation_copy_row(row)
    })
}

fn row_matches_reference(
    meta: &contextdb_core::TableMeta,
    values: &std::collections::HashMap<String, contextdb_core::Value>,
    key: &NaturalKey,
) -> bool {
    // Borrow the exact column precedence of natural_key_from_row_values_admitted.
    let columns: &[String] = if let Some(column) = &meta.natural_key_column {
        std::slice::from_ref(column)
    } else if !meta.primary_key_columns.is_empty() {
        &meta.primary_key_columns
    } else if let Some(column) = meta.columns.iter().find(|column| column.primary_key) {
        std::slice::from_ref(&column.name)
    } else if let Some(column) = meta.columns.iter().find(|column| column.name == "id") {
        std::slice::from_ref(&column.name)
    } else {
        return false;
    };
    columns.len() == 1 + key.rest.len()
        && columns[0] == key.column
        && columns
            .iter()
            .skip(1)
            .zip(&key.rest)
            .all(|(column, (name, _))| column == name)
        && key.matches_values(values)
}
fn stored_vector_scratch_bytes(
    dimension: usize,
    quantization: contextdb_core::VectorQuantization,
) -> Result<usize> {
    use contextdb_core::VectorQuantization;
    let packed = match quantization {
        VectorQuantization::F32 => return Ok(0),
        VectorQuantization::SQ8 => dimension,
        VectorQuantization::SQ4 => dimension.div_ceil(2),
    };
    // StoredVector::from_f32 first constructs its byte Vec, then converts that
    // exact payload into Arc<[u8]>. SQ4 also constructs dimension quantized bytes.
    let arc = std::alloc::Layout::new::<[usize; 2]>()
        .extend(std::alloc::Layout::array::<u8>(packed).map_err(|_| invalid())?)
        .map_err(|_| invalid())?
        .0
        .pad_to_align()
        .size();
    packed
        .checked_add(arc)
        .and_then(|bytes| {
            bytes.checked_add(if quantization == VectorQuantization::SQ4 {
                dimension
            } else {
                0
            })
        })
        .ok_or_else(invalid)
}

pub(crate) fn final_row_owned(
    db: &Database,
    guard: &contextdb_tx::manager::EngineGuard<'_>,
    reference: &RowRef,
    require_write: bool,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedPreparationValue<contextdb_core::VersionedRow>> {
    let staged = db.with_owned_delivery_meta(&reference.table, |meta| {
        final_row_from_meta_owned(db, guard, reference, meta)
    })?;
    if let Some(staged) = staged {
        return Ok(staged);
    }
    if require_write {
        return Err(Error::ManifestMemberOutsideTransaction {
            table: reference.table.clone(),
        });
    }
    // End the catalog borrow before the indexed read re-enters table metadata.
    let memory = crate::read_image_memory::ImageMemory::operation_reader(
        db.bounded_read_accountant(),
        read_memory.clone(),
    )?;
    let _scope = memory.enter();
    let selected = actual_row(db, reference, read_memory)?;
    copy_row_owned(db, &selected)
}
fn final_row_from_meta_owned(
    db: &Database,
    guard: &contextdb_tx::manager::EngineGuard<'_>,
    reference: &RowRef,
    meta: &contextdb_core::TableMeta,
) -> Result<Option<OwnedPreparationValue<contextdb_core::VersionedRow>>> {
    use crate::memory_accounting::{
        preparation_check_row_capacity, preparation_copy_value, preparation_row_map_bytes,
        preparation_value_copy_bytes,
    };
    let view = guard.read();
    let mut rows = view.relational_inserts.iter().filter(|(table, row)| {
        *table == reference.table && row_matches_reference(meta, &row.values, &reference.key)
    });
    let Some((_, row)) = rows.next() else {
        return Ok(None);
    };
    if rows.next().is_some() {
        return Err(invalid());
    }
    let vector = |column: &contextdb_core::ColumnDef| -> Option<&[f32]> {
        if !matches!(column.column_type, contextdb_core::ColumnType::Vector(_)) {
            return None;
        }
        view.vector_inserts
            .iter()
            .rev()
            .find(|entry| {
                entry.index.table == reference.table
                    && entry.index.column == column.name
                    && entry.row_id == row.row_id
            })
            .map(|entry| entry.vector.as_slice())
            .or_else(|| match row.values.get(&column.name) {
                Some(contextdb_core::Value::Vector(vector)) => Some(vector.as_slice()),
                _ => None,
            })
    };
    let extra = meta
        .columns
        .iter()
        .filter(|column| !row.values.contains_key(&column.name) && vector(column).is_some())
        .count();
    let members = row.values.len().checked_add(extra).ok_or_else(invalid)?;
    let mut bytes = preparation_row_map_bytes(members)?;
    let mut scratch = 0usize;
    for (name, value) in &row.values {
        let column = meta.columns.iter().find(|column| column.name == *name);
        let payload = if let Some((column, selected)) =
            column.and_then(|column| vector(column).map(|selected| (column, selected)))
        {
            scratch = scratch.max(stored_vector_scratch_bytes(
                selected.len(),
                column.quantization,
            )?);
            preparation_array_bytes::<f32>(selected.len())?
        } else {
            preparation_value_copy_bytes(value)?
        };
        bytes = bytes
            .checked_add(name.len())
            .and_then(|bytes| bytes.checked_add(payload))
            .ok_or_else(invalid)?;
    }
    for column in meta
        .columns
        .iter()
        .filter(|column| !row.values.contains_key(&column.name))
    {
        if let Some(selected) = vector(column) {
            scratch = scratch.max(stored_vector_scratch_bytes(
                selected.len(),
                column.quantization,
            )?);
            bytes = bytes
                .checked_add(column.name.len())
                .and_then(|bytes| {
                    bytes.checked_add(selected.len().checked_mul(std::mem::size_of::<f32>())?)
                })
                .ok_or_else(invalid)?;
        }
    }
    let _scratch = db.reserve_owned_preparation(scratch)?;
    OwnedPreparationValue::try_produce(db, bytes, || {
        let mut values = std::collections::HashMap::with_capacity(members);
        for (name, value) in &row.values {
            let column = meta.columns.iter().find(|column| column.name == *name);
            let value = if let Some((column, selected)) =
                column.and_then(|column| vector(column).map(|selected| (column, selected)))
            {
                contextdb_core::Value::Vector(contextdb_vector::stored_vector_value(
                    selected,
                    column.quantization,
                ))
            } else {
                preparation_copy_value(value)?
            };
            values.insert(name.as_str().to_owned(), value);
        }
        for column in meta
            .columns
            .iter()
            .filter(|column| !row.values.contains_key(&column.name))
        {
            if let Some(selected) = vector(column) {
                values.insert(
                    column.name.clone(),
                    contextdb_core::Value::Vector(contextdb_vector::stored_vector_value(
                        selected,
                        column.quantization,
                    )),
                );
            }
        }
        let copied = contextdb_core::VersionedRow {
            row_id: row.row_id,
            values,
            created_tx: row.created_tx,
            deleted_tx: row.deleted_tx,
            lsn: row.lsn,
            created_at: row.created_at,
        };
        preparation_check_row_capacity(&copied)?;
        Ok(copied)
    })
    .map(Some)
}

pub(crate) fn final_row(
    db: &Database,
    ws: &contextdb_tx::WriteSet,
    reference: &RowRef,
    require_write: bool,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<contextdb_core::VersionedRow> {
    let meta = db.table_meta(&reference.table).ok_or_else(invalid)?;
    let mut rows = ws.relational_inserts.iter().filter(|(table, row)| {
        *table == reference.table
            && crate::sync_types::natural_key_from_row_values(&meta, &row.values).as_ref()
                == Some(&reference.key)
    });
    if let Some((_, row)) = rows.next() {
        if rows.next().is_some() {
            return Err(invalid());
        }
        let mut row = row.clone();
        for column in &meta.columns {
            if !matches!(column.column_type, contextdb_core::ColumnType::Vector(_)) {
                continue;
            }
            let vector = ws
                .vector_inserts
                .iter()
                .rev()
                .find(|entry| {
                    entry.index.table == reference.table
                        && entry.index.column == column.name
                        && entry.row_id == row.row_id
                })
                .map(|entry| entry.vector.clone())
                .or_else(|| match row.values.get(&column.name) {
                    Some(contextdb_core::Value::Vector(v)) => Some(v.clone()),
                    _ => None,
                });
            if let Some(vector) = vector {
                row.values.insert(
                    column.name.clone(),
                    contextdb_core::Value::Vector(contextdb_vector::stored_vector_value(
                        &vector,
                        column.quantization,
                    )),
                );
            }
        }
        return Ok(row);
    }
    if require_write {
        return Err(Error::ManifestMemberOutsideTransaction {
            table: reference.table.clone(),
        });
    }
    actual_row(db, reference, read_memory.clone())
}

pub(crate) fn random_digest() -> [u8; 32] {
    let mut digest = [0; 32];
    getrandom::fill(&mut digest).expect("operating system entropy");
    digest
}
pub(crate) fn next_state(
    db: &Database,
    incarnation: Incarnation,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<StateRecord> {
    let mut custody_hold = crate::custody::store::SelectionHold::new(read_memory.clone());

    let records = custody_hold.retain(db.custody_authority(read_memory.clone())?)?;
    let control = records.iter().find_map(|r| {
        if let Record::Control(c) = r {
            Some(c)
        } else {
            None
        }
    });
    let previous = control
        .map(|c| {
            records
                .iter()
                .find_map(|r| match r {
                    Record::State(s) if s.state.token == c.head => Some(s),
                    _ => None,
                })
                .ok_or_else(invalid)
        })
        .transpose()?;
    if previous.is_some_and(|s| s.state.incarnation != incarnation) {
        return Err(invalid());
    }
    // One immutable identity record per incarnation, not a
    // history entry for each database write or accepted unit.
    if let Some(previous) = previous {
        return Ok(previous.clone());
    }
    Ok(StateRecord {
        state: StateRef {
            logical_store_id: previous
                .map(|s| s.state.logical_store_id)
                .unwrap_or_else(Uuid::new_v4),
            incarnation,
            birth_id: previous
                .map(|s| s.state.birth_id)
                .unwrap_or_else(Uuid::new_v4),
            token: random_digest(),
        },
        parent: previous.map(|s| s.state.token),
        sequence: previous
            .map(|s| s.sequence.checked_add(1).ok_or_else(invalid))
            .transpose()?
            .unwrap_or(0),
        retirement_root: previous
            .map(|s| s.retirement_root)
            .unwrap_or_else(empty_history_root),
    })
}
pub(crate) fn state_records(
    db: &Database,
    state: StateRecord,
    tenant: &TenantId,
    hub: &str,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Vec<Record>> {
    let mut custody_hold = crate::custody::store::SelectionHold::new(read_memory.clone());

    let existing = custody_hold.retain(db.custody_authority(read_memory.clone())?)?;
    let birth = BirthRef::from(&state.state);
    let mut states: Vec<_> = existing
        .iter()
        .filter_map(|r| match r {
            Record::State(s) if BirthRef::from(&s.state) == birth => Some(s.clone()),
            _ => None,
        })
        .collect();
    if !states.iter().any(|s| s.state == state.state) {
        states.push(state.clone());
    }
    let mut records = vec![
        Record::Control(Control {
            format: 1,
            identity: birth.clone(),
            hub_node: Some(hub.into()),
            tenant: Some(tenant.clone()),
            head: state.state.token,
            namespace_revision: existing
                .iter()
                .find_map(|r| match r {
                    Record::Control(c) => Some(c.namespace_revision),
                    _ => None,
                })
                .unwrap_or(0),
            active_history_root: history_root(&states),
            retired_history_root: state.retirement_root,
        }),
        Record::State(state),
    ];
    if !existing
        .iter()
        .any(|r| matches!(r,Record::Birth(b) if b.identity==birth))
    {
        records.push(Record::Birth(Birth {
            identity: birth,
            predecessor: None,
            selected_image: None,
            triggers: Vec::new(),
            retired: Vec::new(),
        }));
    }
    Ok(records)
}
/// StateRecord is entirely inline; selecting a prior state copies no native body.
pub(crate) fn next_state_owned(
    db: &Database,
    incarnation: Incarnation,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedPreparationValue<StateRecord>> {
    let selected = db.custody_authority(read_memory)?;
    let control = selected.iter().find_map(|record| match record {
        Record::Control(control) => Some(control),
        _ => None,
    });
    let previous = control
        .map(|control| {
            selected
                .iter()
                .find_map(|record| match record {
                    Record::State(state) if state.state.token == control.head => Some(state),
                    _ => None,
                })
                .ok_or_else(invalid)
        })
        .transpose()?;
    if previous.is_some_and(|state| state.state.incarnation != incarnation) {
        return Err(invalid());
    }
    OwnedPreparationValue::try_produce(db, 0, || {
        if let Some(previous) = previous {
            return Ok(previous.clone());
        }
        Ok(StateRecord {
            state: StateRef {
                logical_store_id: Uuid::new_v4(),
                incarnation,
                birth_id: Uuid::new_v4(),
                token: random_digest(),
            },
            parent: None,
            sequence: 0,
            retirement_root: history_root_owned(db, &[])?,
        })
    })
}

pub(crate) fn state_records_owned(
    db: &Database,
    state: OwnedPreparationValue<StateRecord>,
    tenant: &TenantId,
    hub: &str,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedPreparationRecords> {
    // Keep the actual selected read lease, rather than detaching its Vec.
    let existing = db.custody_authority(read_memory)?;
    let birth = state.with_value(|state| BirthRef::from(&state.state));
    let selected_count = existing.iter().filter(|record|
        matches!(record, Record::State(state) if BirthRef::from(&state.state) == birth)).count();
    let includes_state = state.with_value(|state| existing.iter().any(|record|
        matches!(record, Record::State(other) if BirthRef::from(&other.state) == birth && other.state == state.state)));
    let count = selected_count
        .checked_add(usize::from(!includes_state))
        .ok_or_else(invalid)?;
    let states = OwnedPreparationValue::try_produce(
        db,
        preparation_array_bytes::<&StateRecord>(count)?,
        || {
            let mut states = Vec::with_capacity(count);
            if states.capacity() != count {
                return Err(invalid());
            }
            for record in existing.iter() {
                if let Record::State(state) = record
                    && BirthRef::from(&state.state) == birth
                {
                    states.push(state);
                }
            }
            // The input owner lives through the hash; no borrowed state escapes it.
            if !includes_state {
                states.push(&state.value);
            }
            Ok(states)
        },
    )?;
    let active_history_root = states.with_value(|states| history_root_owned(db, states))?;
    drop(states);
    let needs_birth = !existing
        .iter()
        .any(|record| matches!(record, Record::Birth(existing) if existing.identity == birth));
    let capacity = 2 + usize::from(needs_birth);
    let mut records = OwnedPreparationRecords::try_new(db, capacity, capacity)?;
    let native = hub
        .len()
        .checked_add(tenant.as_str().len())
        .ok_or_else(invalid)?;
    let control = OwnedPreparationValue::try_produce(db, native, || {
        Ok(Record::Control(Control {
            format: 1,
            identity: birth.clone(),
            hub_node: Some(hub.into()),
            tenant: Some(tenant.clone()),
            head: state.value.state.token,
            namespace_revision: existing
                .iter()
                .find_map(|record| match record {
                    Record::Control(control) => Some(control.namespace_revision),
                    _ => None,
                })
                .unwrap_or(0),
            active_history_root,
            retired_history_root: state.value.retirement_root,
        }))
    })?;
    records.push(control);
    state.transfer_into(&mut records, |state, records| {
        records.records.push(Record::State(state))
    });
    if needs_birth {
        records.push(OwnedPreparationValue::try_produce(db, 0, || {
            Ok(Record::Birth(Birth {
                identity: birth,
                predecessor: None,
                selected_image: None,
                triggers: Vec::new(),
                retired: Vec::new(),
            }))
        })?);
    }
    Ok(records)
}

pub(crate) fn normalized(
    table: &str,
    mut policy: ApplicationTablePolicy,
) -> Result<ApplicationTablePolicy> {
    if let Some(tables) = &mut policy.manifest_tables {
        tables.sort();
        if tables.is_empty() || tables.windows(2).any(|w| w[0] == w[1]) {
            return Err(invalid());
        }
    }
    policy_digest(table, &policy)?;
    Ok(policy)
}
pub(crate) fn binding(
    db: &Database,
    edge: &str,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<SignedBinding> {
    let mut custody_hold = crate::custody::store::SelectionHold::new(read_memory.clone());

    let records = custody_hold.retain(db.custody_authority(read_memory.clone())?)?;
    let destination = records.iter().find_map(|r| match r {
        Record::Destination(a) => Some(&a.namespace),
        _ => None,
    });
    let local_hub = records.iter().find_map(|r| match r {
        Record::Control(c) if c.hub_node.is_some() => Some(c),
        _ => None,
    });
    records
        .iter()
        .find_map(|r| match r {
            Record::Binding(b)
                if b.namespace.edge_node == edge
                    && destination.is_none_or(|current| current == &b.namespace)
                    && local_hub
                        .is_none_or(|c| c.identity.incarnation == b.namespace.hub_incarnation) =>
            {
                Some(b.clone())
            }
            _ => None,
        })
        .ok_or_else(invalid)
}
pub(crate) fn actual_row(
    db: &Database,
    r: &RowRef,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<contextdb_core::VersionedRow> {
    // Indexed natural-key lookup, never a table scan per member.
    db.custody_row(r, read_memory.clone())?.ok_or_else(|| {
        Error::SyncError(format!(
            "delivery prerequisite row is absent in {}",
            r.table
        ))
    })
}

pub(crate) fn actual_policy(
    db: &Database,
    table: &str,
    _read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<ApplicationTablePolicy> {
    let m = db
        .table_meta(table)
        .ok_or_else(|| Error::TableNotFound(table.into()))?;
    Ok(ApplicationTablePolicy {
        sync_direction: m
            .sync_direction
            .unwrap_or(contextdb_core::DEFAULT_SYNC_DIRECTION),
        sync_conflict: m
            .conflict_policy
            .unwrap_or(contextdb_core::DEFAULT_CONFLICT_POLICY),
        immutable: m.immutable,
        retain: m.default_ttl_seconds.map(|seconds| RetentionDeclaration {
            seconds,
            declared_unit: m
                .retain_declared_unit
                .unwrap_or(contextdb_core::RetainUnit::Seconds),
            sync_safe: m.sync_safe,
        }),
        history: m
            .history_policy
            .unwrap_or(contextdb_core::DEFAULT_HISTORY_POLICY),
        manifest_tables: m.delivery_manifest_tables,
        edge_discard: m.edge_discard.unwrap_or_default(),
    })
}
// Registration validates the writing transaction immediately;
// final preparation repeats it after ordinary commit validation has run.
pub(crate) fn validate_registration(
    db: &Database,
    ws: &contextdb_tx::WriteSet,
    input: &SourceRegistration,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<()> {
    let fail = || Error::ManifestIncomplete {
        table: input.root.table.clone(),
    };
    let policy = actual_policy(db, &input.root.table, read_memory.clone())?;
    let allowed = policy
        .manifest_tables
        .as_ref()
        .ok_or_else(|| Error::ManifestRequired {
            table: input.root.table.clone(),
        })?;
    let root = final_row(db, ws, &input.root, true, read_memory.clone())?;
    let mut members = input.members.iter().collect::<Vec<_>>();
    members.sort_by_key(|r| r.order_key());
    if members.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(fail());
    }
    for reference in members {
        if reference == &input.root || !allowed.contains(&reference.table) {
            return Err(fail());
        }
        let row = final_row(db, ws, reference, true, read_memory.clone())?;
        let meta = db.table_meta(&reference.table).ok_or_else(fail)?;
        let single = meta.columns.iter().any(|c| {
            c.references.as_ref().is_some_and(|fk| {
                fk.table == input.root.table
                    && row.values.get(&c.name).is_some_and(|v| {
                        *v != contextdb_core::Value::Null && Some(v) == root.values.get(&fk.column)
                    })
            })
        });
        let composite = meta.composite_foreign_keys.iter().any(|fk| {
            fk.parent_table == input.root.table
                && fk
                    .child_columns
                    .iter()
                    .zip(&fk.parent_columns)
                    .all(|(a, b)| {
                        row.values.get(a).is_some_and(|v| {
                            *v != contextdb_core::Value::Null && Some(v) == root.values.get(b)
                        })
                    })
        });
        if !single && !composite {
            return Err(fail());
        }
    }
    Ok(())
}

pub(crate) fn prepare_manifest(
    db: &Database,
    ws: &mut contextdb_tx::WriteSet,
    tenant: &TenantId,
    edge: &str,
    input: DeliveryManifest<'_>,
    signer: &LineageSigner,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Vec<Record>> {
    let mut custody_hold = crate::custody::store::SelectionHold::new(read_memory.clone());

    let prior = custody_hold.retain(db.custody_authority(read_memory.clone())?)?;
    let incarnation = db.sync_incarnation_with_read_memory(tenant, read_memory.clone())?;
    let b = if db.retention_sync_peer().is_some() {
        let binding = binding(db, edge, read_memory.clone())?;
        binding.verify()?;
        if binding.namespace.tenant != *tenant || binding.namespace.edge_incarnation != incarnation
        {
            return Err(invalid());
        }
        Some(binding)
    } else {
        None
    };
    let authority_policy = |table: &str| -> Option<BoundPolicy> {
        if let Some(binding) = &b {
            binding.tables.iter().find(|p| p.table == table).cloned()
        } else {
            prior.iter().find_map(|record| match record {
                Record::Policy(p) if p.table == table && p.tenant.as_ref() == Some(tenant) => {
                    Some(BoundPolicy {
                        table: p.table.clone(),
                        policy: p.policy.clone(),
                        version: p.version,
                        digest: p.digest,
                    })
                }
                _ => None,
            })
        }
    };
    let root = RowRef {
        table: input.root_table.into(),
        key: input.root_key,
    };
    let mut members: Vec<_> = input
        .members
        .into_iter()
        .map(|(table, key)| RowRef {
            table: table.into(),
            key,
        })
        .collect();
    members.sort_by_key(RowRef::order_key);
    if members.windows(2).any(|w| w[0] == w[1]) || members.contains(&root) {
        return Err(invalid());
    }
    let root_policy = authority_policy(&root.table);
    let local_policy = actual_policy(db, &root.table, read_memory.clone())?;
    let policy = root_policy
        .as_ref()
        .map(|p| &p.policy)
        .or_else(|| {
            (local_policy.sync_direction == contextdb_core::SyncDirection::None)
                .then_some(&local_policy)
        })
        .ok_or_else(invalid)?;
    let allowed = policy.manifest_tables.as_ref().ok_or_else(invalid)?;
    if members.iter().any(|m| !allowed.contains(&m.table)) {
        return Err(invalid());
    }
    let references: Vec<_> = std::iter::once(root.clone()).chain(members).collect();
    let mut changes = ChangeSet::default();
    let mut local_rows = Vec::new();
    let mut policies = BTreeMap::new();
    for reference in &references {
        let row = final_row(db, ws, reference, true, read_memory.clone())?;
        let installed = actual_policy(db, &reference.table, read_memory.clone())?;
        let bound = authority_policy(&reference.table);
        if let Some(p) = &bound
            && p.digest != policy_digest(&reference.table, &installed)?
        {
            return Err(Error::SyncError(format!(
                "installed policy differs from prerequisite binding for {}",
                reference.table
            )));
        }
        policies.insert(
            reference.table.clone(),
            PolicyEvidence {
                table: reference.table.clone(),
                installed,
                authority: if bound.is_some() {
                    if b.is_some() { 2 } else { 1 }
                } else {
                    0
                },
                bound,
                agreement: true,
            },
        );
        changes.rows.push(RowChange {
            table: reference.table.clone(),
            natural_key: reference.key.clone(),
            values: row.values.clone(),
            deleted: false,
            lsn: row.lsn,
            created_at: row.created_at,
        });
        local_rows.push(row);
    }
    let root_lsn = local_rows[0].lsn;
    let prior = custody_hold.retain(db.custody_root(&root, read_memory.clone())?)?;
    if prior.iter().any(|r|matches!(r,Record::Manifest(m) if m.seal.root==root&&m.edge_node==edge&&m.seal.source.0>=root_lsn)) {
        return Err(Error::SyncError("a new prerequisite submission requires an actual later source writing transaction".into()));
    }

    if local_rows.iter().any(|r| r.lsn != root_lsn) {
        return Err(Error::SyncError(
            "manifest prerequisites must come from one actual committed writing transaction".into(),
        ));
    }
    let lineages = db.prepare_delivery_row_lineages(
        ws,
        &changes,
        tenant,
        edge,
        incarnation,
        signer.as_ref(),
        read_memory.clone(),
    )?;
    let mut evidence = Vec::new();
    let mut lives = Vec::new();
    let mut content = Vec::new();
    for ((reference, row), change) in references.iter().zip(&local_rows).zip(&changes.rows) {
        let lineage = lineages
            .get(&(
                reference.table.clone(),
                rmp_serde::to_vec(&reference.key).map_err(|_| invalid())?,
                change.lsn,
            ))
            .cloned()
            .ok_or_else(invalid)?;
        evidence.push(RowEvidence {
            reference: reference.clone(),
            creator: lineage.clone(),
            authored_source: (row.lsn, 0),
            row_digest: full_row_digest(&row.values),
        });
        lives.push(LocalLifeAnchor {
            original_table: reference.table.clone(),
            table_generation: lineage.table_generation,
            local_row_id: row.row_id.0,
            creation_lsn: lineage.author_local_mutation_position,
        });
        content.push(content_row_digest(&row.values, &[]));
    }
    // Validate the real schema relationships, rather than assuming a shared name means membership.
    for (reference, row) in references.iter().zip(&local_rows).skip(1) {
        let meta = db.table_meta(&reference.table).ok_or_else(invalid)?;
        let root_values = &local_rows[0].values;
        let single = meta.columns.iter().any(|c| {
            c.references.as_ref().is_some_and(|fk| {
                fk.table == root.table && row.values.get(&c.name) == root_values.get(&fk.column)
            })
        });
        let composite = meta.composite_foreign_keys.iter().any(|fk| {
            fk.parent_table == root.table
                && fk
                    .child_columns
                    .iter()
                    .zip(&fk.parent_columns)
                    .all(|(a, b)| row.values.get(a) == root_values.get(b))
        });
        if !single && !composite {
            return Err(Error::SyncError(
                "manifest prerequisite member does not reference its root".into(),
            ));
        }
    }
    let mut unit = Encoder::domain("delivery-content-unit.v1");
    root.encode(&mut unit);
    unit.raw(&content[0]);
    unit.u64((references.len() - 1) as u64);
    for (r, d) in references.iter().zip(content).skip(1) {
        r.encode(&mut unit);
        unit.raw(&d);
    }
    // The unit signs its current schema image at its real commit,
    // so a locally retained table needs no replay of a former peer's DDL history.
    let mut schemas = Vec::new();
    let tables = evidence
        .iter()
        .map(|r| (r.reference.table.clone(), r.creator.table_generation))
        .collect::<std::collections::BTreeSet<_>>();
    // The snapshot is one authored unit: every statement signs how many it holds.
    let unit_statements = u32::try_from(tables.len()).map_err(|_| invalid())?;
    for (ordinal, (table, generation)) in tables.into_iter().enumerate() {
        let meta = db.table_meta(&table).ok_or_else(invalid)?;
        let ddl = crate::protocol::WireDdlChange::from(crate::database::ddl_change_from_meta(
            &table, &meta,
        ));
        let source_lsn = ws.commit_lsn.ok_or_else(invalid)?;
        let ordinal = u32::try_from(ordinal).map_err(|_| invalid())?;
        let mut schema = SchemaDependency {
            tenant: tenant.clone(),
            author_node: edge.into(),
            author_incarnation: incarnation,
            source_lsn,
            ordinal,
            unit_statements,
            table: table.clone(),
            table_generation: generation,
            ddl_digest: crate::protocol::canonical_ddl_provenance_digest(
                &ddl,
                source_lsn,
                ordinal,
                Some(&table),
                Some(generation),
            )
            .map_err(|_| invalid())?,
            ddl_bytes: rmp_serde::to_vec(&ddl).map_err(|_| invalid())?,
            signature: Vec::new(),
        };
        schema.signature = signer(&schema.bytes()?)?;
        schemas.push(schema);
    }
    let mut m = ManifestRecord {
        tenant: tenant.clone(),
        edge_node: edge.into(),
        edge_incarnation: incarnation,
        id: Uuid::new_v4(),
        root_life: lives[0].clone(),
        member_lives: lives[1..].to_vec(),
        seal: Seal {
            root: root.clone(),
            root_digest: evidence[0].row_digest,
            origin_life_digest: evidence[0].origin_digest(tenant)?,
            membership_revision: Uuid::new_v4(),
            source: (root_lsn, 0),
            kind: 0,
            registered_count: (references.len() - 1) as u64,
            unit_digest: unit.digest(),
            last_complete: None,
            projection_digest: [0; 32],
            authority_digest: [0; 32],
        },
        rows: evidence,
        schemas,
        policies: policies.into_values().collect(),
        binding: b,
        signature: Vec::new(),
    };
    let mut projection = Encoder::domain("delivery-materialization-projection.v1");
    projection.raw(&m.projection_bytes()?);
    m.seal.projection_digest = projection.digest();
    m.seal.authority_digest = m.authority_digest()?;
    m.signature = signer(&m.signed_bytes()?)?;
    m.verify()?;
    {
        let mut records = vec![
            Record::Root {
                life: m.root_life.clone(),
                submission: m.id,
            },
            Record::Manifest(Box::new(m.clone())),
        ];
        for member in &m.member_lives {
            records.push(Record::MemberOwner {
                member: member.clone(),
                root: m.root_life.clone(),
                submission: m.id,
            });
        }
        records.push(Record::SourceHistory {
            root: m.root_life.clone(),
            submission: m.id,
            seal_digest: m.seal.digest(),
            source: m.seal.source,
        });
        for (row, life) in m
            .rows
            .iter()
            .zip(std::iter::once(&m.root_life).chain(&m.member_lives))
        {
            records.push(Record::PolicyProfile {
                life: life.clone(),
                submission: m.id,
                profile: m
                    .policies
                    .iter()
                    .find(|p| p.table == row.reference.table)
                    .cloned()
                    .ok_or_else(invalid)?,
            });
        }
        Ok(records)
    }
}

/// Advance the complete branch without signing or exposing a new public mutation door.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TerminalInput {
    pub tenant_id: TenantId,
    pub edge_incarnation: Incarnation,
    pub manifest: ManifestRecord,
    pub rows: Vec<RowChange>,
    pub kind: u8,
    pub cause: Option<String>,
}

#[allow(clippy::too_many_arguments)] // Keep this established input list explicit without changing its API.
pub(crate) fn prepare_terminal(
    db: &Database,
    ws: &mut contextdb_tx::WriteSet,
    tenant: TenantId,
    hub: String,
    edge: String,
    request: TerminalInput,
    signer: &LineageSigner,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Vec<Record>> {
    prepare_terminal_at(
        db,
        ws,
        tenant,
        hub,
        edge,
        request,
        signer,
        None,
        0,
        read_memory.clone(),
    )
}

#[allow(clippy::too_many_arguments)]
fn prepare_terminal_at(
    db: &Database,
    ws: &mut contextdb_tx::WriteSet,
    tenant: TenantId,
    hub: String,
    edge: String,
    request: TerminalInput,
    signer: &LineageSigner,
    reserved_state: Option<StateRecord>,
    ordinal: u32,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Vec<Record>> {
    let mut custody_hold = crate::custody::store::SelectionHold::new(read_memory.clone());

    let mut retained_vector_copy = crate::vector_read_memory::admission(read_memory.clone());
    let vector_copy_owner = &mut retained_vector_copy;

    let m = request.manifest;
    m.verify()?;
    if request.kind != 2
        && (request.rows.len() != m.rows.len()
            || m.rows.iter().any(|e| {
                request
                    .rows
                    .iter()
                    .filter(|r| {
                        r.table == e.reference.table
                            && r.natural_key == e.reference.key
                            && !r.deleted
                            && full_row_digest(&r.values) == e.row_digest
                            && r.lsn == e.authored_source.0
                    })
                    .count()
                    != 1
            }))
    {
        return Err(invalid());
    }
    if request.tenant_id != tenant
        || m.tenant != tenant
        || m.edge_node != edge
        || m.edge_incarnation != request.edge_incarnation
        || request.kind > 2
    {
        return Err(invalid());
    }
    if request.kind == 0 {
        for evidence in &m.rows {
            let actual = final_row(db, ws, &evidence.reference, true, read_memory.clone())?;
            if full_row_digest(&actual.values) != evidence.row_digest {
                return Err(invalid());
            }
        }
    }
    let b = binding(db, &edge, read_memory.clone())?;
    if b.namespace.hub_node != hub
        || b.namespace.hub_incarnation
            != db.sync_incarnation_with_read_memory(&tenant, read_memory.clone())?
    {
        return Err(invalid());
    }
    // An accepted terminal requires every source row in this exact final write set.
    // Equivalence instead validates the complete existing materialization.
    if request.kind < 2 {
        let mut unit = Encoder::domain("delivery-content-unit.v1");
        m.seal.root.encode(&mut unit);
        let root_row = final_row(db, ws, &m.seal.root, request.kind == 0, read_memory.clone())?;
        unit.raw(&content_row_digest(&root_row.values, &[]));
        unit.u64(m.seal.registered_count);
        for row in m.rows.iter().skip(1) {
            let actual = final_row(
                db,
                ws,
                &row.reference,
                request.kind == 0,
                read_memory.clone(),
            )?;
            row.reference.encode(&mut unit);
            unit.raw(&content_row_digest(&actual.values, &[]));
        }
        if unit.digest() != m.seal.unit_digest {
            return Err(Error::SyncError(
                "hub does not hold the prerequisite complete content".into(),
            ));
        }
    }
    let conflicts = if request.kind == 2 {
        match request.cause.as_deref() {
            // Diagnose the actual held rows. A
            // terminal's sender can be a relay or an equivalent writer, and a
            // winnerless member must name its conflicting sibling instead.
            Some(cause @ ("unit_digest_mismatch" | "keep_first_refused")) => {
                let mut conflicts = db.dependency_unit_refusal_conflicts(
                    &request.rows,
                    &crate::sync_types::ConflictPolicies::uniform(
                        crate::sync_types::ConflictPolicy::InsertIfNotExists,
                    ),
                    Some(&hub),
                    read_memory.clone(),
                )?;
                if conflicts.len() != request.rows.len()
                    || conflicts.iter().any(|c| {
                        if c.refusal_cause.is_some() {
                            c.winning_author_node_id.is_some()
                                || c.hub_acceptance_position.is_some()
                        } else {
                            c.winning_author_node_id.is_none()
                                || c.hub_acceptance_position.is_none()
                        }
                    })
                {
                    return Err(invalid());
                }
                for conflict in &mut conflicts {
                    conflict.reason = Some(cause.into());
                }
                conflicts
            }
            Some("manifest_incomplete" | "member_digest_mismatch" | "binding_mismatch") => {
                Vec::new()
            }
            _ => return Err(invalid()),
        }
    } else {
        if request.cause.is_some() {
            return Err(invalid());
        }
        Vec::new()
    };
    // Resolve actual incumbent row lives, including equivalent source aliases.
    let mut incumbent_lives = Vec::new();
    for (source, source_life) in m
        .rows
        .iter()
        .zip(std::iter::once(&m.root_life).chain(&m.member_lives))
    {
        if request.kind == 2 {
            incumbent_lives.push((source_life.clone(), source.row_digest));
            continue;
        }
        let row = final_row(
            db,
            ws,
            &source.reference,
            request.kind == 0,
            read_memory.clone(),
        )?;
        incumbent_lives.push((
            LocalLifeAnchor {
                original_table: source.reference.table.clone(),
                table_generation: db.durable_lineage_table_generation(
                    &source.reference.table,
                    read_memory.clone(),
                )?,
                local_row_id: row.row_id.0,
                creation_lsn: row.lsn,
            },
            full_row_digest(&row.values),
        ));
    }
    if request.kind == 1 {
        let existing = custody_hold.retain(db.custody_root(&m.seal.root, read_memory.clone())?)?;
        let complete_incumbent = existing.iter().filter_map(|r| match r { Record::Terminal { edge: false, record: t } if t.kind != 2 && t.root == m.seal.root && t.unit_digest == m.seal.unit_digest && t.namespace.hub_node == hub => Some(t), _ => None })
            .any(|t| m.rows.iter().zip(&incumbent_lives).all(|(source, (life, digest))| existing.iter().any(|r| matches!(r, Record::MaterializedOwner(o) if o.namespace == t.namespace && o.submission == t.submission && o.incumbent_reference == source.reference && o.incumbent == *life && o.incumbent_row_digest == *digest))));
        // Metadata ownership is unnecessary for a directly held
        // complete unit. Revalidate that unit inside the terminal commit gate.
        if !complete_incumbent
            && db.custody_incumbent_digest(&m.seal.root, vector_copy_owner, read_memory.clone())?
                != Some(m.seal.unit_digest)
        {
            return Err(Error::SyncError(
                "equivalence requires a complete authenticated incumbent materialization".into(),
            ));
        }
    }
    let state = match reserved_state {
        Some(state) => state,
        None => next_state(db, b.namespace.hub_incarnation, read_memory.clone())?,
    };
    let mut subjects: Vec<_> = m.rows.iter().map(|r| (r.reference.clone(), 0)).collect();
    subjects.sort_by_key(|(r, _)| r.order_key());
    let block = DiagnosticBlock {
        salt: random_digest(),
        subjects,
        conflicts,
    };
    let lsn = ws.commit_lsn.ok_or_else(invalid)?;
    {
        let mut t = TerminalRecord {
            namespace: b.namespace.clone(),
            root: m.seal.root.clone(),
            origin_life_digest: m.seal.origin_life_digest,
            submission: m.id,
            seal_digest: m.seal.digest(),
            unit_digest: m.seal.unit_digest,
            source: m.seal.source,
            kind: request.kind,
            cause: request.cause,
            position: (lsn, ordinal),
            state: state.state.clone(),
            diagnostics: DiagnosticClaim::from_block(&block)?,
            signature: Vec::new(),
        };
        super::checkpoint::append(db, ws, &t.namespace, signer, read_memory.clone())?;
        t.signature = signer(&t.bytes()?)?;
        t.verify()?;
        let mut records = state_records(
            db,
            state,
            &t.namespace.tenant,
            &t.namespace.hub_node,
            read_memory.clone(),
        )?;
        for (source, (incumbent, digest)) in m.rows.iter().zip(&incumbent_lives) {
            // A refused source owns diagnostic subjects, never the incumbent materialization.
            if t.kind != 2 {
                records.push(Record::MaterializedOwner(MaterializedOwner {
                    namespace: t.namespace.clone(),
                    submission: t.submission,
                    source: source.clone(),
                    incumbent: incumbent.clone(),
                    incumbent_reference: source.reference.clone(),
                    incumbent_row_digest: *digest,
                    root_incumbent: incumbent_lives[0].0.clone(),
                }));
            }
            records.push(Record::DiagnosticOwner {
                namespace: t.namespace.clone(),
                submission: t.submission,
                life: incumbent.clone(),
            });
        }
        records.extend([
            Record::Terminal {
                edge: false,
                record: t.clone(),
            },
            Record::Order {
                namespace: t.namespace.clone(),
                position: t.position,
                submission: t.submission,
            },
            Record::Manifest(Box::new(m)),
            Record::Diagnostic {
                namespace: t.namespace.clone(),
                submission: t.submission,
                block,
            },
        ]);
        Ok(records)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct SignedBindingPacket {
    pub binding: SignedBinding,
    pub certificate: Certificate,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct SignedOutcomePacket {
    #[serde(default)]
    pub checkpoint: Option<super::checkpoint::Checkpoint>,
    pub record: TerminalRecord,
    pub block: DiagnosticBlock,
    pub certificate: Certificate,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct BindingInput {
    pub tenant_id: TenantId,
    pub edge_incarnation: Incarnation,
    pub expectation: ApplicationTablePolicyExpectation,
}

pub(crate) fn committed_certificate(
    db: &Database,
    namespace: &Namespace,
    state: &StateRef,
    signer: &LineageSigner,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Certificate> {
    let mut custody_hold = crate::custody::store::SelectionHold::new(read_memory.clone());

    let records = custody_hold.retain(db.custody_authority(read_memory.clone())?)?;
    certificate_from_records(&records, namespace, state, signer)
}

pub(crate) fn certificate_from_records(
    records: &[Record],
    namespace: &Namespace,
    state: &StateRef,
    signer: &LineageSigner,
) -> Result<Certificate> {
    let state = records
        .iter()
        .find_map(|r| match r {
            Record::State(s) if s.state == *state => Some(s.clone()),
            _ => None,
        })
        .ok_or_else(invalid)?;
    let birth = records
        .iter()
        .find_map(|r| match r {
            Record::Birth(b) if b.identity == BirthRef::from(&state.state) => Some(b.clone()),
            _ => None,
        })
        .ok_or_else(invalid)?;
    let retired_births = retired_descriptors(records, &birth)?;
    let retirement_digest = retirement_root(&retired_births)?;
    let mut certificate = Certificate {
        format: 1,
        tenant: namespace.tenant.clone(),
        hub_node: namespace.hub_node.clone(),
        state,
        birth,
        retirement_digest,
        retired_births,
        signature: Vec::new(),
    };
    certificate.signature = signer(&certificate.bytes()?)?;
    certificate.verify()?;
    Ok(certificate)
}
fn edge_authority(
    db: &Database,
    namespace: &Namespace,
    state: &StateRef,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<EdgeAuthority> {
    let mut custody_hold = crate::custody::store::SelectionHold::new(read_memory.clone());

    let birth = BirthRef::from(state);
    let old = custody_hold
        .retain(db.custody_authority(read_memory.clone())?)?
        .into_iter()
        .find_map(|r| {
            if let Record::Destination(a) = r {
                Some(a)
            } else {
                None
            }
        });
    if let Some(old) = old {
        if old.namespace != *namespace || old.birth != birth {
            return Ok(EdgeAuthority {
                destination_epoch: old.destination_epoch.checked_add(1).ok_or_else(invalid)?,
                namespace: namespace.clone(),
                birth,
                validity_revision: old.validity_revision.checked_add(1).ok_or_else(invalid)?,
            });
        }
        Ok(old)
    } else {
        Ok(EdgeAuthority {
            destination_epoch: 0,
            namespace: namespace.clone(),
            birth,
            validity_revision: 0,
        })
    }
}
#[allow(clippy::too_many_arguments)] // Keep this established input list explicit without changing its API.
pub(crate) fn commit_binding(
    db: &Database,
    tenant_id: TenantId,
    hub_node_id: String,
    edge_node_id: String,
    request: BindingInput,
    signer: &LineageSigner,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<SignedBindingPacket> {
    let mut custody_hold = crate::custody::store::SelectionHold::new(read_memory.clone());

    if request.tenant_id != tenant_id {
        return Err(invalid());
    }
    // Compare before minting authority, and repeat without a commit.
    let records = custody_hold.retain(db.custody_authority(read_memory.clone())?)?;
    for (table, expected) in &request.expectation.tables {
        let declared = records
            .iter()
            .find_map(|r| match r {
                Record::Policy(p) if p.table == *table => Some(p),
                _ => None,
            })
            .ok_or_else(|| Error::TenantPolicyNotDeclared {
                table: table.clone(),
            })?;
        if let Some(clause) =
            super::policy::differing_clause(&declared.policy, &normalized(table, expected.clone())?)
        {
            return Err(Error::TenantPolicyMismatch {
                table: table.clone(),
                clause: clause.into(),
            });
        }
    }
    let current_hub_incarnation = db.existing_sync_incarnation(&tenant_id, read_memory.clone())?;
    if let Some(binding) = records.iter().find_map(|r| match r {
        Record::Binding(b)
            if b.namespace.tenant == tenant_id
                && b.namespace.hub_node == hub_node_id
                && b.namespace.edge_node == edge_node_id
                && b.namespace.edge_incarnation == request.edge_incarnation
                && Some(b.namespace.hub_incarnation) == current_hub_incarnation
                && b.tables.len() == request.expectation.tables.len()
                && b.tables.iter().all(|p| {
                    request
                        .expectation
                        .tables
                        .get(&p.table)
                        .is_some_and(|expected| {
                            super::policy::differing_clause(&p.policy, expected).is_none()
                        })
                }) =>
        {
            Some(b.clone())
        }
        _ => None,
    }) {
        let certificate = committed_certificate(
            db,
            &binding.namespace,
            &binding.issuance_state,
            signer,
            read_memory.clone(),
        )?;
        return Ok(SignedBindingPacket {
            binding,
            certificate,
        });
    }
    let incarnation = db.sync_incarnation_with_read_memory(&tenant_id, read_memory.clone())?;
    let mut result = None;
    db.commit_delivery_metadata(
        |lsn| {
            // Policy comparison and freeze use the same commit exclusion as issuance.
            let declared: BTreeMap<_, _> = custody_hold
                .retain(db.custody_authority(read_memory.clone())?)?
                .into_iter()
                .filter_map(|r| match r {
                    Record::Policy(p) => Some((p.table.clone(), p)),
                    _ => None,
                })
                .collect();
            let mut policies = Vec::new();
            let mut records = Vec::new();
            for (table, expected) in request.expectation.tables {
                let mut declaration = declared.get(&table).cloned().ok_or_else(|| {
                    Error::TenantPolicyNotDeclared {
                        table: table.clone(),
                    }
                })?;
                if declaration.digest != policy_digest(&table, &normalized(&table, expected)?)? {
                    return Err(Error::TenantPolicyMismatch {
                        table,
                        clause: "binding comparison".into(),
                    });
                }
                if declaration.tenant.as_ref().is_some_and(|t| t != &tenant_id) {
                    return Err(invalid());
                }
                if db.table_meta(&table).is_some()
                    && policy_digest(&table, &actual_policy(db, &table, read_memory.clone())?)?
                        != declaration.digest
                {
                    return Err(Error::SyncError(format!(
                        "hub installed policy differs from declaration for {table}"
                    )));
                }
                declaration.tenant = Some(tenant_id.clone());
                policies.push(BoundPolicy {
                    table,
                    policy: declaration.policy.clone(),
                    version: declaration.version,
                    digest: declaration.digest,
                });
                records.push(declaration);
            }
            let state = next_state(db, incarnation, read_memory.clone())?;
            let mut b = SignedBinding {
                format: 1,
                namespace: Namespace {
                    tenant: tenant_id,
                    hub_node: hub_node_id,
                    hub_incarnation: incarnation,
                    edge_node: edge_node_id,
                    edge_incarnation: request.edge_incarnation,
                },
                tables: policies,
                issuance_state: state.state.clone(),
                issuance_source: (lsn, 0),
                signature: Vec::new(),
            };
            b.signature = signer(&b.bytes()?)?;
            b.verify()?;
            result = Some(b.clone());
            let mut out = state_records(
                db,
                state,
                &b.namespace.tenant,
                &b.namespace.hub_node,
                read_memory.clone(),
            )?;
            out.push(Record::Binding(b));
            for mut p in records {
                p.first_bound_position.get_or_insert(lsn);
                p.first_bound_at
                    .get_or_insert(contextdb_core::Wallclock::now().0);
                out.push(Record::Policy(p));
            }
            Ok(out)
        },
        vector_copy_owner,
        read_memory.clone(),
    )?;
    let binding = result.ok_or_else(invalid)?;
    let certificate = committed_certificate(
        db,
        &binding.namespace,
        &binding.issuance_state,
        signer,
        read_memory.clone(),
    )?;
    Ok(SignedBindingPacket {
        binding,
        certificate,
    })
}
pub(crate) fn admit_binding(
    db: &Database,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    packet: &SignedBindingPacket,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<()> {
    let mut custody_hold = crate::custody::store::SelectionHold::new(read_memory.clone());

    let b = &packet.binding;
    let certificate = &packet.certificate;
    certificate.verify()?;
    if certificate.tenant != b.namespace.tenant
        || certificate.hub_node != b.namespace.hub_node
        || certificate.state.state != b.issuance_state
    {
        return Err(invalid());
    }
    b.verify()?;
    if &b.namespace.tenant != tenant
        || b.namespace.hub_node != hub
        || b.namespace.edge_node != edge
        || db
            .existing_sync_incarnation(tenant, read_memory.clone())?
            .is_some_and(|inc| inc != b.namespace.edge_incarnation)
    {
        return Err(invalid());
    }
    if custody_hold
        .retain(db.custody_authority(read_memory.clone())?)?
        .iter()
        .any(|r| matches!(r, Record::Binding(prior) if prior == b))
    {
        return Ok(());
    }
    let tx = db.begin()?;
    db.commit_delivery_prepared(
        tx,
        &mut |db, ws| {
            // The accepted binding and edge incarnation persist together.
            ws.config_writes.push((
                tenant.config_key("sync_incarnation"),
                crate::persistence::RedbPersistence::encode_config_value(
                    &b.namespace.edge_incarnation.to_hex(),
                )?,
            ));
            let mut existing = custody_hold.retain(db.custody_authority(read_memory.clone())?)?;
            let old_objects = existing
                .iter()
                .filter_map(|r| match r {
                    Record::Binding(prior) if prior.namespace == b.namespace => Some(prior),
                    _ => None,
                })
                .map(|prior| {
                    Ok(format!(
                        "@object:{}",
                        hex(&object_digest(0, &prior.bytes()?, &prior.signature))
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            existing.extend(
                custody_hold.retain(db.custody_records_in(&old_objects, read_memory.clone())?)?,
            );
            for prior in existing.iter().filter_map(|r| match r {
                Record::Binding(prior) if prior.namespace == b.namespace => Some(prior),
                _ => None,
            }) {
                let digest = object_digest(0, &prior.bytes()?, &prior.signature);
                if digest != object_digest(0, &b.bytes()?, &b.signature) {
                    for record in &existing {
                        if matches!(record, Record::ObjectAdmission(a) if a.object_digest == digest)
                        {
                            ws.config_deletes.push(record.key()?);
                        }
                    }
                }
            }
            let authority =
                edge_authority(db, &b.namespace, &b.issuance_state, read_memory.clone())?;
            let admission = Admission {
                object_digest: object_digest(0, &b.bytes()?, &b.signature),
                authority: authority.clone(),
                owning_state: b.issuance_state.clone(),
                certificate: certificate.id()?,
            };
            let state = next_state(db, b.namespace.edge_incarnation, read_memory.clone())?;
            let mut records = state_records(
                db,
                state,
                &b.namespace.tenant,
                &b.namespace.hub_node,
                read_memory.clone(),
            )?;
            // The edge owns a local history, not the remote hub's signing identity.
            for record in &mut records {
                if let Record::Control(control) = record {
                    control.hub_node = None;
                    control.tenant = None;
                }
            }
            records.extend([
                Record::Binding(b.clone()),
                Record::Certificate(certificate.clone()),
                Record::Destination(authority),
                Record::ObjectAdmission(admission),
            ]);
            Ok(records)
        },
        vector_copy_owner,
        read_memory.clone(),
    )
}
// Changed application rows have no current registration;
// malformed or damaged journal records still fail their canonical verification.
pub(crate) fn current_manifest(
    db: &Database,
    table: &str,
    key: &NaturalKey,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Option<ManifestRecord>> {
    let mut custody_hold = crate::custody::store::SelectionHold::new(read_memory.clone());

    let records = custody_hold.retain(db.custody_root(
        &RowRef {
            table: table.into(),
            key: key.clone(),
        },
        read_memory.clone(),
    )?)?;
    let manifest = records.iter().find_map(|r| match r {
        Record::Manifest(m)
            if m.seal.root.table == table
                && m.seal.root.key == *key
                && records.iter().any(|r| {
                    matches!(r, Record::Root { life, submission }
                if *life == m.root_life && *submission == m.id)
                }) =>
        {
            Some(m.as_ref())
        }
        _ => None,
    });
    let Some(m) = manifest else {
        return Ok(None);
    };
    m.verify()?;
    for e in &m.rows {
        let Some(row) = db.custody_row(&e.reference, read_memory.clone())? else {
            return Ok(None);
        };
        if full_row_digest(&row.values) != e.row_digest {
            return Ok(None);
        }
    }
    Ok(Some(m.clone()))
}
#[allow(clippy::too_many_arguments)] // Keep this established input list explicit without changing its API.
pub(crate) fn commit_terminal(
    db: &Database,
    tenant: TenantId,
    hub: String,
    edge: String,
    request: TerminalInput,
    signer: &LineageSigner,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<SignedOutcomePacket> {
    let decoded_memory = crate::read_image_memory::ImageMemory::charging_reader_for(
        db.bounded_read_accountant(),
        read_memory.clone(),
        "reader",
        "decode_custody_metadata",
        "Reduce selected custody metadata.",
    );

    let mut custody_hold = crate::custody::store::SelectionHold::new(read_memory.clone());

    request.manifest.verify()?;
    let submission = request.manifest.id;
    let input = request;
    let mut prepared = Some(input.clone());
    let mut prepare = |db: &Database, ws: &mut contextdb_tx::WriteSet| {
        prepare_terminal(
            db,
            ws,
            tenant.clone(),
            hub.clone(),
            edge.clone(),
            prepared.take().ok_or_else(invalid)?,
            signer,
            read_memory.clone(),
        )
    };
    if input.kind == 0 {
        let mut changes = ChangeSet {
            rows: input.rows.clone(),
            ..Default::default()
        };
        let received = if input
            .manifest
            .rows
            .iter()
            .any(|r| db.table_meta(&r.reference.table).is_none())
        {
            let mut context = crate::protocol::ReceivedDdlContext {
                tenant_id: tenant.clone(),
                source_node_id: edge.clone(),
                source_incarnation: input.edge_incarnation,
                entries: Vec::new(),
            };
            for schema in &input.manifest.schemas {
                schema.verify()?;
                if schema.author_node != edge
                    || schema.author_incarnation != input.edge_incarnation
                    || schema.tenant != tenant
                {
                    return Err(invalid());
                }
                let ddl: crate::protocol::WireDdlChange =
                    decoded_memory.decode_msgpack(&schema.ddl_bytes)?;
                changes.ddl.push(crate::sync_types::DdlChange::from(ddl));
                changes.ddl_lsn.push(schema.source_lsn);
                context.entries.push(crate::protocol::ReceivedDdlEntry {
                    source_ddl_lsn: schema.source_lsn,
                    ordinal: schema.ordinal,
                    table: Some(schema.table.clone()),
                    table_generation: Some(schema.table_generation),
                    digest: schema.ddl_digest.clone(),
                    // The manifest's schema dependency IS the author's signed
                    // provenance, just verified above. Carrying it here is what
                    // lets this node serve the schema onward under the author's
                    // name, with the author's own signature.
                    author: Some(crate::protocol::WireDdlAuthor {
                        node_id: schema.author_node.clone(),
                        incarnation: schema.author_incarnation,
                        source_ddl_lsn: schema.source_lsn,
                        ordinal: schema.ordinal,
                        table: Some(schema.table.clone()),
                        table_generation: Some(schema.table_generation),
                        digest: schema.ddl_digest.clone(),
                        attestation: schema.signature.clone(),
                        unit_statements: schema.unit_statements,
                    }),
                });
            }
            Some(context)
        } else {
            None
        };
        let lineages = input
            .manifest
            .rows
            .iter()
            .map(|r| {
                (
                    r.reference.table.clone(),
                    r.reference.key.clone(),
                    r.authored_source.0,
                    r.creator.clone(),
                )
            })
            .collect::<Vec<_>>();
        let receipt = crate::database::SyncApplyReceipt {
            tenant_id: tenant.clone(),
            node_id: edge.clone(),
            incarnation: input.edge_incarnation,
            source_lsn: input.manifest.seal.source.0,
            dependency_complete: true,
            hub_contact_ms: None,
        };
        db.apply_delivery_prepared(
            changes,
            receipt,
            &hub,
            &lineages,
            received.as_ref(),
            &mut prepare,
            vector_copy_owner,
            read_memory.clone(),
        )?;
    } else {
        let tx = db.begin()?;
        db.commit_delivery_prepared(tx, &mut prepare, vector_copy_owner, read_memory.clone())?;
    }
    let records = custody_hold.retain(db.custody_submission(submission, read_memory.clone())?)?;
    let current_hub_incarnation = db.existing_sync_incarnation(&tenant, read_memory.clone())?;
    let record = records
        .iter()
        .find_map(|r| match r {
            Record::Terminal {
                edge: false,
                record,
            } if record.submission == submission
                && record.namespace.hub_node == hub
                && record.namespace.edge_node == edge
                && Some(record.namespace.hub_incarnation) == current_hub_incarnation =>
            {
                Some(record.clone())
            }
            _ => None,
        })
        .ok_or_else(invalid)?;
    let block = records
        .iter()
        .find_map(|r| match r {
            Record::Diagnostic {
                namespace,
                submission: id,
                block,
            } if *id == submission && *namespace == record.namespace => Some(block.clone()),
            _ => None,
        })
        .ok_or_else(invalid)?;
    let certificate = committed_certificate(
        db,
        &record.namespace,
        &record.state,
        signer,
        read_memory.clone(),
    )?;
    Ok(SignedOutcomePacket {
        checkpoint: super::checkpoint::current(db, &record.namespace, read_memory.clone())?,
        record,
        block,
        certificate,
    })
}
#[cfg(feature = "test-seams")]
pub(crate) fn admit_terminal(
    db: &Database,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    packet: &SignedOutcomePacket,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<()> {
    let mut retained_vector_copy = crate::vector_read_memory::admission(read_memory.clone());
    let vector_copy_owner = &mut retained_vector_copy;

    admit_terminals(
        db,
        tenant,
        hub,
        edge,
        std::slice::from_ref(packet),
        vector_copy_owner,
        read_memory.clone(),
    )
}

// One admission transaction for the complete returned
// cohort; no row scan, no truncated acceptance position, no partial credit.
pub(crate) fn admit_terminals(
    db: &Database,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    packets: &[SignedOutcomePacket],
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<()> {
    let mut custody_hold = crate::custody::store::SelectionHold::new(read_memory.clone());

    if packets.is_empty() {
        return Ok(());
    }
    for packet in packets {
        let t = &packet.record;
        packet.certificate.verify()?;
        t.verify()?;
        if let Some(c) = &packet.checkpoint {
            c.verify()?;
            if c.namespace != t.namespace {
                return Err(invalid());
            }
        }
        if packet.certificate.state.state != t.state
            || packet.certificate.tenant != t.namespace.tenant
            || packet.certificate.hub_node != t.namespace.hub_node
            || !t.diagnostics.matches(&packet.block)?
            || t.namespace.tenant != *tenant
            || t.namespace.hub_node != hub
            || t.namespace.edge_node != edge
        {
            return Err(invalid());
        }
    }
    let selectors = std::iter::once("delivery_destination.v1".to_owned())
        .chain(
            packets
                .iter()
                .map(|p| format!("@{}", super::store::submission_group(p.record.submission))),
        )
        .collect::<Vec<_>>();
    let current = custody_hold.retain(db.custody_records_in(&selectors, read_memory.clone())?)?;
    if packets.iter().all(|p| current.iter().any(|r| matches!(r, Record::Terminal { edge: true, record: t } if t.signature == p.record.signature))) { return Ok(()); }
    db.commit_delivery_metadata(|_| {
        let current = custody_hold.retain(db.custody_records_in(&selectors, read_memory.clone())?)?;
        let destination = current.iter().find_map(|r| match r { Record::Destination(d) => Some(d), _ => None }).ok_or_else(invalid)?;
        let manifests = current.iter().filter_map(|r| match r { Record::Manifest(m) if current.iter().any(|r| matches!(r, Record::Root { submission, .. } if *submission == m.id)) => Some((m.id, m)), _ => None }).collect::<BTreeMap<_, _>>();
        let mut writes = Vec::new();
        let mut strongest: Option<&super::checkpoint::Checkpoint> = None;
        for packet in packets {
            let t = &packet.record;
            if current.iter().any(|r| matches!(r, Record::Terminal { edge: true, record: old } if old.signature == t.signature)) { continue; }
            // A purged/discarded root has no owning manifest; stale replies
            // cannot restore its credit or recreate any metadata.
            let Some(m) = manifests.get(&t.submission) else { continue; };
            if t.namespace != destination.namespace || t.namespace.edge_incarnation != m.edge_incarnation
                || t.root != m.seal.root || t.seal_digest != m.seal.digest() || t.unit_digest != m.seal.unit_digest { return Err(invalid()); }
            if let Some(checkpoint) = &packet.checkpoint {
                if strongest.is_some_and(|old| old.count == checkpoint.count && old.digest != checkpoint.digest) { return Err(invalid()); }
                if strongest.is_none_or(|old| old.count < checkpoint.count) { strongest = Some(checkpoint); }
            }
            writes.extend([
                Record::Terminal { edge: true, record: t.clone() },
                Record::Diagnostic { namespace: t.namespace.clone(), submission: t.submission, block: packet.block.clone() },
                Record::Certificate(packet.certificate.clone()),
                Record::ObjectAdmission(Admission { object_digest: object_digest(1, &t.bytes()?, &t.signature), authority: destination.clone(), owning_state: t.state.clone(), certificate: packet.certificate.id()? }),
            ]);
            for life in std::iter::once(&m.root_life).chain(&m.member_lives) {
                writes.push(Record::DiagnosticOwner { namespace: t.namespace.clone(), submission: t.submission, life: life.clone() });
            }
        }
        // Packet order cannot weaken already admitted image evidence.
        if let Some(checkpoint) = strongest
            && let Some(record) = super::checkpoint::admission(db,checkpoint, read_memory.clone())? { writes.push(record); }
        Ok(writes)
    }, vector_copy_owner, read_memory.clone())
}

/// Commit several independently signed source units in one real receiving
/// transaction. Shared subjects must carry identical authenticated row bytes.
#[cfg(feature = "test-seams")]
#[allow(clippy::too_many_arguments)] // Keep this established input list explicit without changing its API.
pub(crate) fn commit_terminal_batch(
    db: &Database,
    tenant: TenantId,
    hub: String,
    edge: String,
    requests: Vec<TerminalInput>,
    signer: &LineageSigner,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<()> {
    if requests.is_empty()
        || requests.iter().any(|r| {
            r.kind != 0
                || r.tenant_id != tenant
                || r.edge_incarnation != requests[0].edge_incarnation
                || r.manifest.seal.source.0 != requests[0].manifest.seal.source.0
        })
    {
        return Err(invalid());
    }
    let mut changes = ChangeSet::default();
    let mut lineages = Vec::new();
    let mut seen = BTreeMap::new();
    for input in &requests {
        input.manifest.verify()?;
        for row in &input.rows {
            let key = RowRef {
                table: row.table.clone(),
                key: row.natural_key.clone(),
            }
            .order_key();
            if let Some(prior) = seen.insert(key, full_row_digest(&row.values)) {
                if prior != full_row_digest(&row.values) {
                    return Err(invalid());
                }
            } else {
                changes.rows.push(row.clone());
            }
        }
        for row in &input.manifest.rows {
            lineages.push((
                row.reference.table.clone(),
                row.reference.key.clone(),
                row.authored_source.0,
                row.creator.clone(),
            ));
        }
    }
    let receipt = crate::database::SyncApplyReceipt {
        tenant_id: tenant.clone(),
        node_id: edge.clone(),
        incarnation: requests[0].edge_incarnation,
        source_lsn: requests[0].manifest.seal.source.0,
        dependency_complete: true,
        hub_contact_ms: None,
    };
    db.apply_delivery_prepared(
        changes,
        receipt,
        &hub,
        &lineages,
        None,
        &mut |db, ws| {
            let incarnation = binding(db, &edge, read_memory.clone())?
                .namespace
                .hub_incarnation;
            let state = next_state(db, incarnation, read_memory.clone())?;
            let mut records = BTreeMap::new();
            for (ordinal, request) in requests.iter().enumerate() {
                for record in prepare_terminal_at(
                    db,
                    ws,
                    tenant.clone(),
                    hub.clone(),
                    edge.clone(),
                    request.clone(),
                    signer,
                    Some(state.clone()),
                    u32::try_from(ordinal).map_err(|_| invalid())?,
                    read_memory.clone(),
                )? {
                    let key = record.key()?;
                    if let Some(prior) = records.insert(key, record.clone())
                        && encode(&prior)? != encode(&record)?
                    {
                        return Err(invalid());
                    }
                }
            }
            Ok(records.into_values().collect())
        },
        vector_copy_owner,
        read_memory.clone(),
    )?;
    Ok(())
}

// Each lineage entry keeps its independently admitted native bodies. The
// envelope itself retains the original exact entry-vector backing admission.
struct OwnedDeliveryLineageEntry {
    table: OwnedPreparationValue<String>,
    key: OwnedPreparationValue<Vec<u8>>,
    lsn: contextdb_core::Lsn,
    lineage: OwnedPreparationValue<crate::protocol::WireRowLineage>,
}
pub(crate) struct OwnedDeliveryLineages {
    entries: Vec<OwnedDeliveryLineageEntry>,
    debit: crate::memory_accounting::OwnedMemoryReservation,
}
impl OwnedDeliveryLineages {
    pub(crate) fn try_new(db: &Database, capacity: usize) -> Result<Self> {
        let debit = db.reserve_owned_preparation(preparation_array_bytes::<
            OwnedDeliveryLineageEntry,
        >(capacity)?)?;
        let entries = Vec::with_capacity(capacity);
        if entries.capacity() != capacity {
            drop(entries);
            return Err(invalid());
        }
        Ok(Self { entries, debit })
    }
    pub(crate) fn insert(
        &mut self,
        db: &Database,
        table: &str,
        key: OwnedPreparationValue<Vec<u8>>,
        lsn: contextdb_core::Lsn,
        lineage: OwnedPreparationValue<crate::protocol::WireRowLineage>,
    ) -> Result<()> {
        let table = OwnedPreparationValue::try_produce(db, table.len(), || Ok(table.to_owned()))?;
        if let Some(slot) = self.entries.iter_mut().find(|entry| {
            entry.table.value == table.value && entry.key.value == key.value && entry.lsn == lsn
        }) {
            // Assignment physically destroys every replaced body before its own credit.
            *slot = OwnedDeliveryLineageEntry {
                table,
                key,
                lsn,
                lineage,
            };
        } else {
            assert!(
                self.entries.len() < self.entries.capacity(),
                "preadmitted lineage slot"
            );
            self.entries.push(OwnedDeliveryLineageEntry {
                table,
                key,
                lsn,
                lineage,
            });
        }
        Ok(())
    }
    fn with_lineage<R>(
        &self,
        db: &Database,
        reference: &RowRef,
        lsn: contextdb_core::Lsn,
        f: impl for<'a> FnOnce(&'a crate::protocol::WireRowLineage) -> Result<R>,
    ) -> Result<R> {
        let key = prepare_positional_encoding_owned(db, &reference.key)?;
        let entry = self
            .entries
            .iter()
            .find(|entry| {
                entry.table.value == reference.table
                    && entry.key.value == key.value
                    && entry.lsn == lsn
            })
            .ok_or_else(invalid)?;
        f(&entry.lineage.value)
    }
}
pub(crate) fn row_matches_key_owned(
    meta: &contextdb_core::TableMeta,
    values: &std::collections::HashMap<String, contextdb_core::Value>,
    key: &NaturalKey,
) -> bool {
    let columns: &[String] = if let Some(column) = &meta.natural_key_column {
        std::slice::from_ref(column)
    } else if !meta.primary_key_columns.is_empty() {
        &meta.primary_key_columns
    } else if let Some(column) = meta.columns.iter().find(|column| column.primary_key) {
        std::slice::from_ref(&column.name)
    } else if let Some(column) = meta.columns.iter().find(|column| column.name == "id") {
        std::slice::from_ref(&column.name)
    } else {
        return false;
    };
    columns.len() == 1 + key.rest.len()
        && columns[0] == key.column
        && columns
            .iter()
            .skip(1)
            .zip(&key.rest)
            .all(|(column, (name, _))| column == name)
        && key.matches_values(values)
}
pub(crate) struct BorrowedHex<'a>(pub &'a [u8]);
impl std::fmt::Display for BorrowedHex<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}
pub(crate) fn sign_preparation_owned(
    db: &Database,
    signer: &dyn Fn(&[u8]) -> Result<Vec<u8>>,
    bytes: &[u8],
) -> Result<OwnedPreparationValue<Vec<u8>>> {
    // Every inventoried signer invokes FabricIdentity::sign_lineage once.
    OwnedPreparationValue::try_produce(db, 64, || {
        let signature = signer(bytes)?;
        if signature.len() != 64 || signature.capacity() != 64 {
            return Err(invalid());
        }
        Ok(signature)
    })
}
pub(crate) fn count_config_encoding<T: serde::Serialize>(value: &T) -> Result<usize> {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("config size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    bincode::serde::encode_into_std_write(value, &mut count, bincode::config::standard())
        .map_err(|_| invalid())?;
    Ok(count.0)
}
pub(crate) fn emit_config_encoding<T: serde::Serialize>(
    value: &T,
    output: &mut Vec<u8>,
) -> Result<()> {
    let planned = count_config_encoding(value)?;
    if !output.is_empty() || output.capacity() != planned {
        return Err(invalid());
    }
    struct Bounded<'a>(&'a mut Vec<u8>);
    impl std::io::Write for Bounded<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.0.capacity() - self.0.len() {
                return Err(std::io::Error::other("config capacity exceeded"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    bincode::serde::encode_into_std_write(value, &mut Bounded(output), bincode::config::standard())
        .map_err(|_| invalid())?;
    if output.len() != planned || output.capacity() != planned {
        return Err(invalid());
    }
    Ok(())
}
fn actual_policy_owned(
    db: &Database,
    table: &str,
) -> Result<OwnedPreparationValue<ApplicationTablePolicy>> {
    db.with_owned_delivery_meta(table, |m| {
        let bytes = m
            .delivery_manifest_tables
            .as_ref()
            .map_or(Ok(0), |tables| {
                tables.iter().try_fold(
                    preparation_array_bytes::<String>(tables.len())?,
                    |n, table| n.checked_add(table.len()).ok_or_else(invalid),
                )
            })?;
        OwnedPreparationValue::try_produce(db, bytes, || {
            Ok(ApplicationTablePolicy {
                sync_direction: m
                    .sync_direction
                    .unwrap_or(contextdb_core::DEFAULT_SYNC_DIRECTION),
                sync_conflict: m
                    .conflict_policy
                    .unwrap_or(contextdb_core::DEFAULT_CONFLICT_POLICY),
                immutable: m.immutable,
                retain: m.default_ttl_seconds.map(|seconds| RetentionDeclaration {
                    seconds,
                    declared_unit: m
                        .retain_declared_unit
                        .unwrap_or(contextdb_core::RetainUnit::Seconds),
                    sync_safe: m.sync_safe,
                }),
                history: m
                    .history_policy
                    .unwrap_or(contextdb_core::DEFAULT_HISTORY_POLICY),
                manifest_tables: m.delivery_manifest_tables.clone(),
                edge_discard: m.edge_discard.unwrap_or_default(),
            })
        })
    })
}
fn binding_owned(
    db: &Database,
    edge: &str,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedPreparationBody<SignedBinding>> {
    let records = db.custody_authority(read_memory)?;
    let destination = records.iter().find_map(|r| match r {
        Record::Destination(a) => Some(&a.namespace),
        _ => None,
    });
    let local_hub = records.iter().find_map(|r| match r {
        Record::Control(c) if c.hub_node.is_some() => Some(c),
        _ => None,
    });
    let binding = records
        .iter()
        .find_map(|r| match r {
            Record::Binding(b)
                if b.namespace.edge_node == edge
                    && destination.is_none_or(|current| current == &b.namespace)
                    && local_hub
                        .is_none_or(|c| c.identity.incarnation == b.namespace.hub_incarnation) =>
            {
                Some(b)
            }
            _ => None,
        })
        .ok_or_else(invalid)?;
    OwnedPreparationBody::try_produce(
        db,
        binding_native_bytes_owned(binding)?,
        signature_count(&binding.signature)?,
        0,
        || Ok(binding.clone()),
    )
}

impl<T: serde::Serialize> serde::Serialize for OwnedPreparationValue<T> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.value.serialize(serializer)
    }
}
fn text_owned(
    db: &Database,
    render: impl Fn(&mut dyn std::fmt::Write) -> std::fmt::Result,
) -> Result<OwnedPreparationValue<String>> {
    struct Count(usize);
    impl std::fmt::Write for Count {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            self.0 = self.0.checked_add(text.len()).ok_or(std::fmt::Error)?;
            Ok(())
        }
    }
    let mut count = Count(0);
    render(&mut count).map_err(|_| invalid())?;
    OwnedPreparationValue::try_produce(db, count.0, || {
        let mut output = String::with_capacity(count.0);
        render(&mut output).map_err(|_| invalid())?;
        if output.len() != count.0 || output.capacity() != count.0 {
            return Err(invalid());
        }
        Ok(output)
    })
}
struct Joined<'a>(&'a [String]);
impl std::fmt::Display for Joined<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, item) in self.0.iter().enumerate() {
            if i != 0 {
                f.write_str(", ")?;
            }
            f.write_str(item)?;
        }
        Ok(())
    }
}
struct Quoted<'a>(&'a str, char);
impl std::fmt::Display for Quoted<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for ch in self.0.chars() {
            if ch == self.1 {
                write!(f, "{ch}")?;
            }
            write!(f, "{ch}")?;
        }
        Ok(())
    }
}
fn render_column_finite(
    out: &mut dyn std::fmt::Write,
    col: &contextdb_core::ColumnDef,
    rules: &[contextdb_core::PropagationRule],
) -> std::fmt::Result {
    use contextdb_core::{
        ColumnType, PropagationRule, ScopeLabelKind, VectorQuantization, VectorSearchMode,
    };
    match col.column_type {
        ColumnType::Integer => out.write_str("INTEGER")?,
        ColumnType::Real => out.write_str("REAL")?,
        ColumnType::Text => out.write_str("TEXT")?,
        ColumnType::Boolean => out.write_str("BOOLEAN")?,
        ColumnType::Json => out.write_str("JSON")?,
        ColumnType::Uuid => out.write_str("UUID")?,
        ColumnType::Vector(dim) => write!(out, "VECTOR({dim})")?,
        ColumnType::Timestamp => out.write_str("TIMESTAMP")?,
        ColumnType::TxId => out.write_str("TXID")?,
    }
    if col.quantization != VectorQuantization::F32 {
        write!(
            out,
            " WITH (quantization = '{}')",
            col.quantization.as_str()
        )?;
    }
    if matches!(col.column_type, ColumnType::Vector(_)) {
        if let Some(columns) = &col.partition_key_columns
            && !columns.is_empty()
        {
            write!(
                out,
                " PARTITION_KEY ({}) MAX_PARTITIONS {}",
                Joined(columns),
                col.effective_max_partitions()
                    .unwrap_or(contextdb_core::DEFAULT_VECTOR_MAX_PARTITIONS)
            )?;
        }
        if col.search_mode != VectorSearchMode::Auto {
            write!(out, " SEARCH_MODE {}", col.search_mode)?;
        }
        if let Some(value) = col.auto_index_at {
            write!(out, " AUTO_INDEX_AT {value}")?;
        }
        let mut first = true;
        for (name, value) in [
            ("M", col.hnsw_m),
            ("EF_CONSTRUCTION", col.hnsw_ef_construction),
            ("EF_SEARCH", col.hnsw_ef_search),
        ] {
            if let Some(value) = value {
                out.write_str(if first { " HNSW (" } else { ", " })?;
                first = false;
                write!(out, "{name} = {value}")?;
            }
        }
        if !first {
            out.write_str(")")?;
        }
        if col.consolidation_disabled {
            out.write_str(" CONSOLIDATION NONE")?;
        } else {
            let mut first = true;
            for (name, value) in [
                ("CHANGE_PERCENT", col.consolidation_change_percent),
                ("TOMBSTONE_PERCENT", col.consolidation_tombstone_percent),
            ] {
                if let Some(value) = value {
                    out.write_str(if first { " CONSOLIDATION (" } else { ", " })?;
                    first = false;
                    write!(out, "{name} = {value}")?;
                }
            }
            if !first {
                out.write_str(")")?;
            }
        }
    }
    if col.context_id {
        out.write_str(" CONTEXT_ID")?;
    }
    let labels = |out: &mut dyn std::fmt::Write, labels: &[String]| -> std::fmt::Result {
        for (i, label) in labels.iter().enumerate() {
            if i != 0 {
                out.write_str(", ")?;
            }
            write!(out, "'{}'", Quoted(label, '\''))?;
        }
        Ok(())
    };
    if let Some(scope) = &col.scope_label {
        match scope {
            ScopeLabelKind::Simple { write_labels } => {
                out.write_str(" SCOPE_LABEL (")?;
                labels(out, write_labels)?;
                out.write_str(")")?;
            }
            ScopeLabelKind::Split {
                read_labels,
                write_labels,
            } => {
                out.write_str(" SCOPE_LABEL_READ (")?;
                labels(out, read_labels)?;
                out.write_str(") WRITE (")?;
                labels(out, write_labels)?;
                out.write_str(")")?;
            }
        }
    }
    let fk = |rule: &contextdb_core::PropagationRule| matches!(rule, PropagationRule::ForeignKey { fk_column, .. } if fk_column == &col.name);
    if let Some(reference) = &col.references {
        write!(out, " REFERENCES {}({})", reference.table, reference.column)?;
    } else if let Some(PropagationRule::ForeignKey {
        referenced_table,
        referenced_column,
        ..
    }) = rules.iter().find(|rule| fk(rule))
    {
        write!(out, " REFERENCES {referenced_table}({referenced_column})")?;
    }
    for rule in rules.iter().filter(|rule| fk(rule)) {
        if let PropagationRule::ForeignKey {
            trigger_state,
            target_state,
            max_depth,
            abort_on_failure,
            ..
        } = rule
        {
            write!(
                out,
                " ON STATE {trigger_state} PROPAGATE SET {target_state}"
            )?;
            if *max_depth != 10 {
                write!(out, " MAX DEPTH {max_depth}")?;
            }
            if *abort_on_failure {
                out.write_str(" ABORT ON FAILURE")?;
            }
        }
    }
    if let Some(acl) = &col.acl_ref {
        write!(out, " ACL REFERENCES {}({})", acl.ref_table, acl.ref_column)?;
    }
    if col.primary_key {
        out.write_str(" PRIMARY KEY")?;
    }
    if !col.nullable && !col.primary_key {
        out.write_str(" NOT NULL")?;
    }
    if let Some(default) = &col.default {
        write!(out, " DEFAULT {default}")?;
    }
    if col.unique {
        out.write_str(" UNIQUE")?;
    }
    if col.expires {
        out.write_str(" EXPIRES")?;
    }
    if col.immutable {
        out.write_str(" IMMUTABLE")?;
    }
    if let Some(policy) = &col.rank_policy {
        write!(
            out,
            " RANK_POLICY (JOIN {} ON {}, FORMULA '{}', SORT_KEY {})",
            policy.joined_table,
            policy.joined_column,
            Quoted(&policy.formula, '\''),
            policy.sort_key
        )?;
    }
    Ok(())
}
#[derive(serde::Serialize)]
struct BorrowedForeignKey<'a> {
    child_column: &'a str,
    parent_table: &'a str,
    parent_column: &'a str,
}
struct BorrowedForeignKeys<'a>(&'a contextdb_core::TableMeta);
impl serde::Serialize for BorrowedForeignKeys<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let count = self
            .0
            .columns
            .iter()
            .filter(|c| c.references.is_some())
            .count();
        let mut seq = serializer.serialize_seq(Some(count))?;
        for column in &self.0.columns {
            if let Some(reference) = &column.references {
                seq.serialize_element(&BorrowedForeignKey {
                    child_column: &column.name,
                    parent_table: &reference.table,
                    parent_column: &reference.column,
                })?;
            }
        }
        seq.end()
    }
}
// The sole variant has the exact original CreateTable name, index and field order.
#[derive(serde::Serialize)]
enum BorrowedSchemaDdl<'a> {
    CreateTable {
        name: &'a str,
        columns: &'a Vec<(&'a str, OwnedPreparationValue<String>)>,
        constraints: &'a Vec<OwnedPreparationValue<String>>,
        foreign_keys: BorrowedForeignKeys<'a>,
        composite_foreign_keys: &'a Vec<contextdb_core::CompositeForeignKey>,
        composite_unique: &'a Vec<Vec<String>>,
    },
}
fn schema_constraint_count(meta: &contextdb_core::TableMeta) -> usize {
    usize::from(meta.delivery_manifest_tables.is_some())
        + usize::from(meta.edge_discard.is_some())
        + usize::from(meta.immutable)
        + usize::from(meta.state_machine.is_some())
        + usize::from(!meta.dag_edge_types.is_empty())
        + usize::from(meta.default_ttl_seconds.is_some())
        + usize::from(meta.history_policy.is_some())
        + usize::from(meta.sync_direction.is_some())
        + usize::from(
            meta.conflict_policy
                .and_then(|p| p.declared_clause())
                .is_some(),
        )
        + usize::from(meta.maintained_counts.is_some())
        + meta.unique_constraints.len()
        + usize::from(!meta.primary_key_columns.is_empty())
        + meta.composite_foreign_keys.len()
        + meta
            .propagation_rules
            .iter()
            .filter(|r| !matches!(r, contextdb_core::PropagationRule::ForeignKey { .. }))
            .count()
}
fn schema_constraints_owned(
    db: &Database,
    meta: &contextdb_core::TableMeta,
) -> Result<OwnedPreparationValue<Vec<OwnedPreparationValue<String>>>> {
    use contextdb_core::{MaintainedCounts, PropagationRule};
    let count = schema_constraint_count(meta);
    OwnedPreparationValue::try_produce(
        db,
        preparation_array_bytes::<OwnedPreparationValue<String>>(count)?,
        || {
            let mut result = Vec::with_capacity(count);
            if result.capacity() != count {
                return Err(invalid());
            }
            let mut add =
                |render: &dyn Fn(&mut dyn std::fmt::Write) -> std::fmt::Result| -> Result<()> {
                    if result.len() == result.capacity() {
                        return Err(invalid());
                    }
                    result.push(text_owned(db, render)?);
                    Ok(())
                };
            if let Some(tables) = &meta.delivery_manifest_tables {
                add(&|out| {
                    out.write_str("DELIVERY MANIFEST OVER ")?;
                    for (i, table) in tables.iter().enumerate() {
                        if i != 0 {
                            out.write_str(", ")?;
                        }
                        write!(out, "\"{}\"", Quoted(table, '"'))?;
                    }
                    Ok(())
                })?;
            }
            if let Some(mode) = meta.edge_discard {
                add(&|out| {
                    write!(
                        out,
                        "EDGE DISCARD {}",
                        match mode {
                            contextdb_core::EdgeDiscardMode::Never => "NEVER",
                            contextdb_core::EdgeDiscardMode::AfterOutcome => "AFTER OUTCOME",
                            contextdb_core::EdgeDiscardMode::Always => "ALWAYS",
                        }
                    )
                })?;
            }
            if meta.immutable {
                add(&|out| out.write_str("IMMUTABLE"))?;
            }
            if let Some(sm) = &meta.state_machine {
                let clauses = OwnedPreparationValue::try_produce(
                    db,
                    preparation_array_bytes::<OwnedPreparationValue<String>>(sm.transitions.len())?,
                    || {
                        let mut clauses = Vec::with_capacity(sm.transitions.len());
                        if clauses.capacity() != sm.transitions.len() {
                            return Err(invalid());
                        }
                        for (from, to) in &sm.transitions {
                            clauses.push(text_owned(db, |out| {
                                write!(out, "{from} -> [{}]", Joined(to))
                            })?);
                        }
                        clauses.sort_unstable_by(|a, b| a.value.cmp(&b.value));
                        Ok(clauses)
                    },
                )?;
                add(&|out| {
                    write!(out, "STATE MACHINE ({}: ", sm.column)?;
                    for (i, clause) in clauses.value.iter().enumerate() {
                        if i != 0 {
                            out.write_str(", ")?;
                        }
                        out.write_str(&clause.value)?;
                    }
                    out.write_str(")")
                })?;
            }
            if !meta.dag_edge_types.is_empty() {
                add(&|out| {
                    out.write_str("DAG(")?;
                    for (i, ty) in meta.dag_edge_types.iter().enumerate() {
                        if i != 0 {
                            out.write_str(", ")?;
                        }
                        write!(out, "'{ty}'")?;
                    }
                    out.write_str(")")
                })?;
            }
            if let Some(seconds) = meta.default_ttl_seconds {
                add(&|out| {
                    let (window, unit) = match meta.retain_declared_unit {
                        Some(unit) if unit.seconds_multiplier() != 0 => {
                            (seconds / unit.seconds_multiplier(), unit.sql())
                        }
                        _ if seconds.is_multiple_of(86400) => (seconds / 86400, "DAYS"),
                        _ if seconds.is_multiple_of(3600) => (seconds / 3600, "HOURS"),
                        _ if seconds.is_multiple_of(60) => (seconds / 60, "MINUTES"),
                        _ => (seconds, "SECONDS"),
                    };
                    write!(out, "RETAIN {window} {unit}")?;
                    if meta.sync_safe {
                        out.write_str(" SYNC SAFE")?;
                    }
                    Ok(())
                })?;
            }
            if let Some(policy) = meta.history_policy {
                add(&|out| out.write_str(policy.declared_clause()))?;
            }
            if let Some(direction) = meta.sync_direction {
                add(&|out| out.write_str(direction.sql()))?;
            }
            if let Some(clause) = meta.conflict_policy.and_then(|p| p.declared_clause()) {
                add(&|out| out.write_str(clause))?;
            }
            if let Some(counts) = &meta.maintained_counts {
                add(&|out| match counts {
                    MaintainedCounts::None => out.write_str("MAINTAIN COUNT NONE"),
                    MaintainedCounts::AllPrefixes => out.write_str("MAINTAIN COUNT ALL PREFIXES"),
                    MaintainedCounts::Lists(lists) => {
                        out.write_str("MAINTAIN COUNT ")?;
                        for (i, columns) in lists.iter().enumerate() {
                            if i != 0 {
                                out.write_str(", ")?;
                            }
                            out.write_str("(")?;
                            for (j, (name, direction)) in columns.iter().enumerate() {
                                if j != 0 {
                                    out.write_str(", ")?;
                                }
                                let plain = !name.is_empty()
                                    && name
                                        .chars()
                                        .next()
                                        .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
                                    && name
                                        .chars()
                                        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_');
                                if plain {
                                    out.write_str(name)?;
                                } else {
                                    write!(out, "\"{}\"", Quoted(name, '"'))?;
                                }
                                if *direction == contextdb_core::SortDirection::Desc {
                                    out.write_str(" DESC")?;
                                }
                            }
                            out.write_str(")")?;
                        }
                        Ok(())
                    }
                })?;
            }
            for columns in &meta.unique_constraints {
                add(&|out| write!(out, "UNIQUE ({})", Joined(columns)))?;
            }
            if !meta.primary_key_columns.is_empty() {
                add(&|out| write!(out, "PRIMARY KEY ({})", Joined(&meta.primary_key_columns)))?;
            }
            for fk in &meta.composite_foreign_keys {
                add(&|out| {
                    write!(
                        out,
                        "FOREIGN KEY ({}) REFERENCES {}({})",
                        Joined(&fk.child_columns),
                        fk.parent_table,
                        Joined(&fk.parent_columns)
                    )
                })?;
            }
            for rule in &meta.propagation_rules {
                match rule {
                    PropagationRule::Edge {
                        edge_type,
                        direction,
                        trigger_state,
                        target_state,
                        max_depth,
                        abort_on_failure,
                    } => {
                        add(&|out| {
                            let direction = match direction {
                                contextdb_core::Direction::Incoming => "INCOMING",
                                contextdb_core::Direction::Outgoing => "OUTGOING",
                                contextdb_core::Direction::Both => "BOTH",
                            };
                            write!(
                                out,
                                "PROPAGATE ON EDGE {edge_type} {direction} STATE {trigger_state} SET {target_state}"
                            )?;
                            if *max_depth != 10 {
                                write!(out, " MAX DEPTH {max_depth}")?;
                            }
                            if *abort_on_failure {
                                out.write_str(" ABORT ON FAILURE")?;
                            }
                            Ok(())
                        })?;
                    }
                    PropagationRule::VectorExclusion { trigger_state } => add(&|out| {
                        write!(out, "PROPAGATE ON STATE {trigger_state} EXCLUDE VECTOR")
                    })?,
                    PropagationRule::ForeignKey { .. } => {}
                }
            }
            drop(add);
            if result.len() != count || result.capacity() != count {
                return Err(invalid());
            }
            Ok(result)
        },
    )
}
fn schema_dependency_owned(
    db: &Database,
    guard: &contextdb_tx::manager::EngineGuard<'_>,
    tenant: &TenantId,
    edge: &str,
    incarnation: Incarnation,
    table: &str,
    generation: u64,
    ordinal: u32,
    unit_statements: u32,
    signer: &LineageSigner,
) -> Result<OwnedPreparationBody<SchemaDependency>> {
    db.with_owned_delivery_meta(table, |meta| {
        let columns = OwnedPreparationValue::try_produce(
            db,
            preparation_array_bytes::<(&str, OwnedPreparationValue<String>)>(meta.columns.len())?,
            || {
                let mut columns = Vec::with_capacity(meta.columns.len());
                if columns.capacity() != meta.columns.len() {
                    return Err(invalid());
                }
                for column in &meta.columns {
                    columns.push((
                        column.name.as_str(),
                        text_owned(db, |out| {
                            render_column_finite(out, column, &meta.propagation_rules)
                        })?,
                    ));
                }
                Ok(columns)
            },
        )?;
        let constraints = schema_constraints_owned(db, meta)?;
        let ddl = BorrowedSchemaDdl::CreateTable {
            name: table,
            columns: &columns.value,
            constraints: &constraints.value,
            foreign_keys: BorrowedForeignKeys(meta),
            composite_foreign_keys: &meta.composite_foreign_keys,
            composite_unique: &meta.unique_constraints,
        };
        let source_lsn = guard.read().commit_lsn.ok_or_else(invalid)?;
        let ddl_bytes = prepare_positional_encoding_owned(db, &ddl)?;
        let digest = positional_digest(&(
            "contextdb.sync-ddl-provenance.v6",
            &ddl,
            source_lsn.0,
            ordinal,
            Some(table),
            Some(generation),
        ))?;
        let schema = OwnedPreparationValue::try_produce(
            db,
            tenant
                .as_str()
                .len()
                .checked_add(edge.len())
                .and_then(|n| n.checked_add(table.len()))
                .and_then(|n| n.checked_add(ddl_bytes.value.len()))
                .and_then(|n| n.checked_add(32))
                .ok_or_else(invalid)?,
            || {
                Ok(SchemaDependency {
                    tenant: tenant.clone(),
                    author_node: edge.into(),
                    author_incarnation: incarnation,
                    source_lsn,
                    ordinal,
                    unit_statements,
                    table: table.into(),
                    table_generation: generation,
                    ddl_bytes: ddl_bytes.value.clone(),
                    ddl_digest: digest.to_vec(),
                    signature: Vec::new(),
                })
            },
        )?;
        let bytes = finite_bytes_owned(db, |e| schema.value.bytes_finite(e))?;
        let schema = OwnedPreparationBody::from_unsigned(db, schema)?;
        let schema = schema.sign(db, signer.as_ref(), &bytes.value, |schema, signature| {
            schema.signature = signature
        })?;
        Ok(schema)
    })
}

impl SourceRegistration {
    pub(crate) fn prepare_owned(
        &self,
        db: &Database,
        guard: &mut contextdb_tx::manager::EngineGuard<'_>,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<OwnedPreparationRecords> {
        let input = self.manifest_input_owned(db)?;
        input.with_value(|input| {
            prepare_manifest_owned(
                db,
                guard,
                &self.tenant,
                &self.edge,
                input,
                &self.signer,
                read_memory,
            )
        })
    }
}
fn reference_owned(
    db: &Database,
    table: &str,
    key: &NaturalKey,
) -> Result<OwnedPreparationValue<RowRef>> {
    OwnedPreparationValue::try_produce(
        db,
        table
            .len()
            .checked_add(count_key_native(key)?)
            .ok_or_else(invalid)?,
        || {
            Ok(RowRef {
                table: table.into(),
                key: copy_key_native(key)?,
            })
        },
    )
}
fn authority_policy_owned(
    db: &Database,
    prior: &[Record],
    binding: Option<&SignedBinding>,
    tenant: &TenantId,
    table: &str,
) -> Result<Option<OwnedPreparationValue<BoundPolicy>>> {
    if let Some(binding) = binding {
        binding
            .tables
            .iter()
            .find(|p| p.table == table)
            .map(|p| {
                OwnedPreparationValue::try_produce(db, bound_policy_native_bytes_owned(p)?, || {
                    Ok(p.clone())
                })
            })
            .transpose()
    } else {
        prior
            .iter()
            .find_map(|record| match record {
                Record::Policy(p) if p.table == table && p.tenant.as_ref() == Some(tenant) => {
                    Some(p)
                }
                _ => None,
            })
            .map(|p| {
                OwnedPreparationValue::try_produce(
                    db,
                    p.table
                        .len()
                        .checked_add(count_policy_native(&p.policy)?)
                        .ok_or_else(invalid)?,
                    || {
                        Ok(BoundPolicy {
                            table: p.table.clone(),
                            policy: p.policy.clone(),
                            version: p.version,
                            digest: p.digest,
                        })
                    },
                )
            })
            .transpose()
    }
}
fn row_change_owned(
    db: &Database,
    reference: &RowRef,
    row: &contextdb_core::VersionedRow,
) -> Result<OwnedPreparationValue<RowChange>> {
    let bytes = count_reference_native(reference)?
        .checked_add(crate::memory_accounting::preparation_row_copy_bytes(row)?)
        .ok_or_else(invalid)?;
    OwnedPreparationValue::try_produce(db, bytes, || {
        Ok(RowChange {
            table: reference.table.clone(),
            natural_key: copy_key_native(&reference.key)?,
            values: crate::memory_accounting::preparation_copy_row(row)?.values,
            deleted: false,
            lsn: row.lsn,
            created_at: row.created_at,
        })
    })
}
pub(crate) fn prepare_manifest_owned(
    db: &Database,
    guard: &mut contextdb_tx::manager::EngineGuard<'_>,
    tenant: &TenantId,
    edge: &str,
    input: &DeliveryManifest<'_>,
    signer: &LineageSigner,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedPreparationRecords> {
    let prior = db.custody_authority(read_memory.clone())?;
    let incarnation = db.sync_incarnation_with_read_memory(tenant, read_memory.clone())?;
    let binding = if db.retention_sync_peer().is_some() {
        let binding = binding_owned(db, edge, read_memory.clone())?;
        binding.value.verify_owned(db)?;
        if binding.value.namespace.tenant != *tenant
            || binding.value.namespace.edge_incarnation != incarnation
        {
            return Err(invalid());
        }
        Some(binding)
    } else {
        None
    };
    let root = reference_owned(db, input.root_table, &input.root_key)?;
    let count = input.members.len().checked_add(1).ok_or_else(invalid)?;
    let members = OwnedPreparationValue::try_produce(
        db,
        preparation_array_bytes::<(
            OwnedPreparationValue<RowRef>,
            OwnedPreparationValue<Vec<u8>>,
            usize,
        )>(input.members.len())?,
        || {
            let mut members = Vec::with_capacity(input.members.len());
            if members.capacity() != input.members.len() {
                return Err(invalid());
            }
            for (position, (table, key)) in input.members.iter().enumerate() {
                members.push((
                    reference_owned(db, table, key)?,
                    finite_bytes_owned(db, |e| {
                        e.key(key);
                        Ok(())
                    })?,
                    position,
                ));
            }
            members.sort_unstable_by(|a, b| {
                a.0.value
                    .table
                    .cmp(&b.0.value.table)
                    .then_with(|| a.1.value.cmp(&b.1.value))
                    .then_with(|| a.2.cmp(&b.2))
            });
            if members.windows(2).any(|w| w[0].0.value == w[1].0.value)
                || members.iter().any(|m| m.0.value == root.value)
            {
                return Err(invalid());
            }
            Ok(members)
        },
    )?;
    let root_policy = authority_policy_owned(
        db,
        &prior,
        binding.as_ref().map(|b| &b.value),
        tenant,
        &root.value.table,
    )?;
    let local_policy = actual_policy_owned(db, &root.value.table)?;
    let policy = root_policy
        .as_ref()
        .map(|p| &p.value.policy)
        .or_else(|| {
            (local_policy.value.sync_direction == contextdb_core::SyncDirection::None)
                .then_some(&local_policy.value)
        })
        .ok_or_else(invalid)?;
    let allowed = policy.manifest_tables.as_ref().ok_or_else(invalid)?;
    if members
        .value
        .iter()
        .any(|m| !allowed.contains(&m.0.value.table))
    {
        return Err(invalid());
    }
    let references =
        OwnedPreparationValue::try_produce(db, preparation_array_bytes::<&RowRef>(count)?, || {
            let mut references = Vec::with_capacity(count);
            if references.capacity() != count {
                return Err(invalid());
            }
            references.push(&root.value);
            references.extend(members.value.iter().map(|m| &m.0.value));
            Ok(references)
        })?;
    let mut rows: OwnedPreparationValue<Vec<OwnedPreparationValue<contextdb_core::VersionedRow>>> =
        OwnedPreparationValue::try_produce(
            db,
            preparation_array_bytes::<OwnedPreparationValue<contextdb_core::VersionedRow>>(count)?,
            || {
                let capacity = count;
                let value = Vec::with_capacity(capacity);
                if value.capacity() != capacity {
                    return Err(invalid());
                }
                Ok(value)
            },
        )?;
    let mut changes: OwnedPreparationValue<Vec<OwnedPreparationValue<RowChange>>> =
        OwnedPreparationValue::try_produce(
            db,
            preparation_array_bytes::<OwnedPreparationValue<RowChange>>(count)?,
            || {
                let capacity = count;
                let value = Vec::with_capacity(capacity);
                if value.capacity() != capacity {
                    return Err(invalid());
                }
                Ok(value)
            },
        )?;
    let mut policies: OwnedPreparationValue<Vec<OwnedPreparationValue<PolicyEvidence>>> =
        OwnedPreparationValue::try_produce(
            db,
            preparation_array_bytes::<OwnedPreparationValue<PolicyEvidence>>(count)?,
            || {
                let capacity = count;
                let value = Vec::with_capacity(capacity);
                if value.capacity() != capacity {
                    return Err(invalid());
                }
                Ok(value)
            },
        )?;
    for reference in &references.value {
        let row = final_row_owned(db, guard, reference, true, read_memory.clone())?;
        let installed = actual_policy_owned(db, &reference.table)?;
        let bound = authority_policy_owned(
            db,
            &prior,
            binding.as_ref().map(|b| &b.value),
            tenant,
            &reference.table,
        )?;
        if let Some(bound) = &bound
            && bound.value.digest != finite_policy_digest(&reference.table, &installed.value)?
        {
            return Err(Error::SyncError(format!(
                "installed policy differs from prerequisite binding for {}",
                reference.table
            )));
        }
        let bytes = reference
            .table
            .len()
            .checked_add(count_policy_native(&installed.value)?)
            .and_then(|n| {
                n.checked_add(
                    bound
                        .as_ref()
                        .map_or(Ok(0), |b| bound_policy_native_bytes_owned(&b.value))
                        .ok()?,
                )
            })
            .ok_or_else(invalid)?;
        let profile = OwnedPreparationValue::try_produce(db, bytes, || {
            Ok(PolicyEvidence {
                table: reference.table.clone(),
                installed: installed.value.clone(),
                authority: if bound.is_some() {
                    if binding.is_some() { 2 } else { 1 }
                } else {
                    0
                },
                bound: bound.as_ref().map(|b| b.value.clone()),
                agreement: true,
            })
        })?;
        if let Some(slot) = policies
            .value
            .iter_mut()
            .find(|p| p.value.table == reference.table)
        {
            *slot = profile;
        } else {
            policies.value.push(profile);
        }
        changes
            .value
            .push(row_change_owned(db, reference, &row.value)?);
        rows.value.push(row);
    }
    policies
        .value
        .sort_unstable_by(|a, b| a.value.table.cmp(&b.value.table));
    let root_lsn = rows.value[0].value.lsn;
    let previous = db.custody_root(&root.value, read_memory.clone())?;
    if previous.iter().any(|r|matches!(r,Record::Manifest(m) if m.seal.root==root.value&&m.edge_node==edge&&m.seal.source.0>=root_lsn)) {
        return Err(Error::SyncError("a new prerequisite submission requires an actual later source writing transaction".into()));
    }
    if rows.value.iter().any(|r| r.value.lsn != root_lsn) {
        return Err(Error::SyncError(
            "manifest prerequisites must come from one actual committed writing transaction".into(),
        ));
    }
    let change_bytes =
        changes
            .value
            .iter()
            .try_fold(preparation_array_bytes::<RowChange>(count)?, |n, c| {
                n.checked_add(row_change_native_bytes_owned(&c.value)?)
                    .ok_or_else(invalid)
            })?;
    let change_set = OwnedPreparationValue::try_produce(db, change_bytes, || {
        Ok(ChangeSet {
            rows: copy_finite_vec(&changes.value, |c| copy_row_change_finite(&c.value))?,
            ..Default::default()
        })
    })?;
    let lineages = db.prepare_delivery_row_lineages_owned(
        guard,
        &change_set.value,
        tenant,
        edge,
        incarnation,
        signer.as_ref(),
        read_memory.clone(),
    )?;
    let mut evidence: OwnedPreparationValue<Vec<OwnedPreparationBody<RowEvidence>>> =
        OwnedPreparationValue::try_produce(
            db,
            preparation_array_bytes::<OwnedPreparationBody<RowEvidence>>(count)?,
            || {
                let capacity = count;
                let value = Vec::with_capacity(capacity);
                if value.capacity() != capacity {
                    return Err(invalid());
                }
                Ok(value)
            },
        )?;
    let mut lives: OwnedPreparationValue<Vec<OwnedPreparationValue<LocalLifeAnchor>>> =
        OwnedPreparationValue::try_produce(
            db,
            preparation_array_bytes::<OwnedPreparationValue<LocalLifeAnchor>>(count)?,
            || {
                let capacity = count;
                let value = Vec::with_capacity(capacity);
                if value.capacity() != capacity {
                    return Err(invalid());
                }
                Ok(value)
            },
        )?;
    for (reference, row) in references.value.iter().zip(&rows.value) {
        let entry = lineages.with_lineage(db, reference, row.value.lsn, |lineage| {
            let bytes = count_reference_native(reference)?
                .checked_add(lineage_native_bytes_owned(lineage)?)
                .ok_or_else(invalid)?;
            OwnedPreparationBody::try_produce(
                db,
                bytes,
                signature_count(&lineage.attestation)?,
                0,
                || {
                    Ok(RowEvidence {
                        reference: copy_reference_finite(reference)?,
                        creator: lineage.clone(),
                        authored_source: (row.value.lsn, 0),
                        row_digest: finite_row_digest(&row.value.values, None)?,
                    })
                },
            )
        })?;
        lives.value.push(OwnedPreparationValue::try_produce(
            db,
            reference.table.len(),
            || {
                Ok(LocalLifeAnchor {
                    original_table: reference.table.clone(),
                    table_generation: entry.value.creator.table_generation,
                    local_row_id: row.value.row_id.0,
                    creation_lsn: entry.value.creator.author_local_mutation_position,
                })
            },
        )?);
        evidence.value.push(entry);
    }
    for (reference, row) in references.value.iter().zip(&rows.value).skip(1) {
        db.with_owned_delivery_meta(&reference.table, |meta| {
            let root_values = &rows.value[0].value.values;
            let single = meta.columns.iter().any(|c| {
                c.references.as_ref().is_some_and(|fk| {
                    fk.table == root.value.table
                        && row.value.values.get(&c.name) == root_values.get(&fk.column)
                })
            });
            let composite = meta.composite_foreign_keys.iter().any(|fk| {
                fk.parent_table == root.value.table
                    && fk
                        .child_columns
                        .iter()
                        .zip(&fk.parent_columns)
                        .all(|(a, b)| row.value.values.get(a) == root_values.get(b))
            });
            if !single && !composite {
                return Err(Error::SyncError(
                    "manifest prerequisite member does not reference its root".into(),
                ));
            }
            Ok(())
        })?;
    }
    let unit_digest = finite_hash(|e| {
        e.string("delivery-content-unit.v1");
        root.value.encode_finite(e);
        e.raw(&finite_row_digest(&rows.value[0].value.values, Some(&[]))?);
        e.u64((count - 1) as u64);
        for (reference, row) in references.value.iter().zip(&rows.value).skip(1) {
            reference.encode_finite(e);
            e.raw(&finite_row_digest(&row.value.values, Some(&[]))?);
        }
        Ok(())
    })?;
    let mut tables = OwnedPreparationValue::try_produce(
        db,
        preparation_array_bytes::<(&str, u64)>(count)?,
        || {
            let mut tables = Vec::with_capacity(count);
            if tables.capacity() != count {
                return Err(invalid());
            }
            for row in &evidence.value {
                tables.push((
                    row.value.reference.table.as_str(),
                    row.value.creator.table_generation,
                ));
            }
            tables.sort_unstable();
            tables.dedup();
            Ok(tables)
        },
    )?;
    let unit_statements = u32::try_from(tables.value.len()).map_err(|_| invalid())?;
    let mut schemas: OwnedPreparationValue<Vec<OwnedPreparationBody<SchemaDependency>>> =
        OwnedPreparationValue::try_produce(
            db,
            preparation_array_bytes::<OwnedPreparationBody<SchemaDependency>>(tables.value.len())?,
            || {
                let capacity = tables.value.len();
                let value = Vec::with_capacity(capacity);
                if value.capacity() != capacity {
                    return Err(invalid());
                }
                Ok(value)
            },
        )?;
    for (ordinal, (table, generation)) in tables.value.iter().enumerate() {
        schemas.value.push(schema_dependency_owned(
            db,
            guard,
            tenant,
            edge,
            incarnation,
            table,
            *generation,
            u32::try_from(ordinal).map_err(|_| invalid())?,
            unit_statements,
            signer,
        )?);
    }
    let mut bytes = tenant
        .as_str()
        .len()
        .checked_add(edge.len())
        .and_then(|n| n.checked_add(lives.value[0].value.original_table.len()))
        .and_then(|n| n.checked_add(count_reference_native(&root.value).ok()?))
        .ok_or_else(invalid)?;
    bytes = bytes
        .checked_add(preparation_array_bytes::<LocalLifeAnchor>(count - 1)?)
        .ok_or_else(invalid)?;
    for life in lives.value.iter().skip(1) {
        bytes = bytes
            .checked_add(life.value.original_table.len())
            .ok_or_else(invalid)?;
    }
    bytes = bytes
        .checked_add(preparation_array_bytes::<RowEvidence>(count)?)
        .ok_or_else(invalid)?;
    for row in &evidence.value {
        bytes = bytes
            .checked_add(row_evidence_native_bytes_owned(&row.value)?)
            .ok_or_else(invalid)?;
    }
    bytes = bytes
        .checked_add(preparation_array_bytes::<SchemaDependency>(
            schemas.value.len(),
        )?)
        .ok_or_else(invalid)?;
    for schema in &schemas.value {
        bytes = bytes
            .checked_add(schema_native_bytes_owned(&schema.value)?)
            .ok_or_else(invalid)?;
    }
    bytes = bytes
        .checked_add(preparation_array_bytes::<PolicyEvidence>(
            policies.value.len(),
        )?)
        .ok_or_else(invalid)?;
    for policy in &policies.value {
        bytes = bytes
            .checked_add(policy_evidence_native_bytes_owned(&policy.value)?)
            .ok_or_else(invalid)?;
    }
    bytes = bytes
        .checked_add(
            binding
                .as_ref()
                .map_or(Ok(0), |b| binding_native_bytes_owned(&b.value))?,
        )
        .ok_or_else(invalid)?;
    let copied_signatures = evidence.value.iter().try_fold(0usize, |n, row| {
        n.checked_add(signature_count(&row.value.creator.attestation)?)
            .ok_or_else(invalid)
    })?;
    let copied_signatures = schemas
        .value
        .iter()
        .try_fold(copied_signatures, |n, schema| {
            n.checked_add(signature_count(&schema.value.signature)?)
                .ok_or_else(invalid)
        })?
        .checked_add(
            binding
                .as_ref()
                .map_or(Ok(0), |b| signature_count(&b.value.signature))?,
        )
        .ok_or_else(invalid)?;
    let mut manifest = OwnedPreparationBody::try_produce(db, bytes, copied_signatures, 1, || {
        Ok(ManifestRecord {
            tenant: tenant.clone(),
            edge_node: edge.into(),
            edge_incarnation: incarnation,
            id: Uuid::new_v4(),
            root_life: lives.value[0].value.clone(),
            member_lives: copy_finite_vec(&lives.value[1..], |l| Ok(l.value.clone()))?,
            seal: Seal {
                root: copy_reference_finite(&root.value)?,
                root_digest: evidence.value[0].value.row_digest,
                origin_life_digest: evidence.value[0].value.origin_digest_finite(tenant)?,
                membership_revision: Uuid::new_v4(),
                source: (root_lsn, 0),
                kind: 0,
                registered_count: (count - 1) as u64,
                unit_digest,
                last_complete: None,
                projection_digest: [0; 32],
                authority_digest: [0; 32],
            },
            rows: copy_finite_vec(&evidence.value, |e| copy_row_evidence_finite(&e.value))?,
            schemas: copy_finite_vec(&schemas.value, |s| Ok(s.value.clone()))?,
            policies: copy_finite_vec(&policies.value, |p| Ok(p.value.clone()))?,
            binding: binding.as_ref().map(|b| b.value.clone()),
            signature: Vec::new(),
        })
    })?;
    manifest.value.seal.projection_digest = manifest.value.projection_digest_owned(db)?;
    manifest.value.seal.authority_digest = manifest.value.authority_digest_owned(db)?;
    let signed = finite_bytes_owned(db, |e| manifest.value.signed_bytes_finite(e))?;
    let manifest = manifest.sign(db, signer.as_ref(), &signed.value, |manifest, signature| {
        manifest.signature = signature
    })?;
    manifest.value.verify_owned(db, read_memory)?;
    let m = &manifest.value;
    let capacity = count
        .checked_mul(2)
        .and_then(|n| n.checked_add(2))
        .ok_or_else(invalid)?;
    let mut records = OwnedPreparationRecords::try_new(
        db,
        capacity,
        capacity
            .checked_add(manifest_signature_count(m)?)
            .ok_or_else(invalid)?,
    )?;
    records.push(OwnedPreparationValue::try_produce(
        db,
        m.root_life.original_table.len(),
        || {
            Ok(Record::Root {
                life: m.root_life.clone(),
                submission: m.id,
            })
        },
    )?);
    records.push_body(OwnedPreparationBody::try_produce(
        db,
        count_manifest_native_finite(m)?
            .checked_add(std::mem::size_of::<ManifestRecord>())
            .ok_or_else(invalid)?,
        manifest_signature_count(m)?,
        0,
        || Ok(Record::Manifest(Box::new(copy_manifest_finite(m)?))),
    )?);
    for member in &m.member_lives {
        records.push(OwnedPreparationValue::try_produce(
            db,
            member
                .original_table
                .len()
                .checked_add(m.root_life.original_table.len())
                .ok_or_else(invalid)?,
            || {
                Ok(Record::MemberOwner {
                    member: member.clone(),
                    root: m.root_life.clone(),
                    submission: m.id,
                })
            },
        )?);
    }
    records.push(OwnedPreparationValue::try_produce(
        db,
        m.root_life.original_table.len(),
        || {
            Ok(Record::SourceHistory {
                root: m.root_life.clone(),
                submission: m.id,
                seal_digest: m.seal.digest_finite()?,
                source: m.seal.source,
            })
        },
    )?);
    for (row, life) in m
        .rows
        .iter()
        .zip(std::iter::once(&m.root_life).chain(&m.member_lives))
    {
        let profile = m
            .policies
            .iter()
            .find(|p| p.table == row.reference.table)
            .ok_or_else(invalid)?;
        records.push(OwnedPreparationValue::try_produce(
            db,
            life.original_table
                .len()
                .checked_add(policy_evidence_native_bytes_owned(profile)?)
                .ok_or_else(invalid)?,
            || {
                Ok(Record::PolicyProfile {
                    life: life.clone(),
                    submission: m.id,
                    profile: profile.clone(),
                })
            },
        )?);
    }
    Ok(records)
}

impl<T> OwnedPreparationValue<Vec<T>> {
    pub(crate) fn push_slot(&mut self, value: T) {
        assert!(
            self.value.len() < self.value.capacity(),
            "preadmitted native slot"
        );
        self.value.push(value);
    }
}

pub(crate) fn prepare_terminal_owned(
    db: &Database,
    guard: &mut contextdb_tx::manager::EngineGuard<'_>,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    request: &TerminalInput,
    signer: &LineageSigner,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedPreparationRecords> {
    prepare_terminal_at_owned(
        db,
        guard,
        tenant,
        hub,
        edge,
        request,
        signer,
        None,
        0,
        read_memory,
    )
}
fn prepare_terminal_at_owned(
    db: &Database,
    guard: &mut contextdb_tx::manager::EngineGuard<'_>,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    request: &TerminalInput,
    signer: &LineageSigner,
    reserved_state: Option<&StateRecord>,
    ordinal: u32,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedPreparationRecords> {
    let memory = crate::read_image_memory::ImageMemory::operation_reader(
        db.bounded_read_accountant(),
        read_memory.clone(),
    )?;
    let _scope = memory.enter();
    let mut retained_vector_copy = crate::vector_read_memory::admission(read_memory.clone());
    let vector_copy_owner = &mut retained_vector_copy;
    let m = &request.manifest;
    m.verify_owned(db, read_memory.clone())?;
    if request.kind != 2
        && (request.rows.len() != m.rows.len()
            || m.rows.iter().any(|e| {
                request
                    .rows
                    .iter()
                    .filter(|r| {
                        r.table == e.reference.table
                            && r.natural_key == e.reference.key
                            && !r.deleted
                            && finite_row_digest(&r.values, None).is_ok_and(|d| d == e.row_digest)
                            && r.lsn == e.authored_source.0
                    })
                    .count()
                    != 1
            }))
    {
        return Err(invalid());
    }
    if request.tenant_id != *tenant
        || m.tenant != *tenant
        || m.edge_node != edge
        || m.edge_incarnation != request.edge_incarnation
        || request.kind > 2
    {
        return Err(invalid());
    }
    if request.kind == 0 {
        for evidence in &m.rows {
            let actual =
                final_row_owned(db, guard, &evidence.reference, true, read_memory.clone())?;
            if finite_row_digest(&actual.value.values, None)? != evidence.row_digest {
                return Err(invalid());
            }
        }
    }
    let b = binding_owned(db, edge, read_memory.clone())?;
    if b.value.namespace.hub_node != hub
        || b.value.namespace.hub_incarnation
            != db.sync_incarnation_with_read_memory(tenant, read_memory.clone())?
    {
        return Err(invalid());
    }
    if request.kind < 2 {
        let digest = finite_hash(|e| {
            e.string("delivery-content-unit.v1");
            m.seal.root.encode_finite(e);
            let root = final_row_owned(
                db,
                guard,
                &m.seal.root,
                request.kind == 0,
                read_memory.clone(),
            )?;
            e.raw(&finite_row_digest(&root.value.values, Some(&[]))?);
            e.u64(m.seal.registered_count);
            for row in m.rows.iter().skip(1) {
                let actual = final_row_owned(
                    db,
                    guard,
                    &row.reference,
                    request.kind == 0,
                    read_memory.clone(),
                )?;
                row.reference.encode_finite(e);
                e.raw(&finite_row_digest(&actual.value.values, Some(&[]))?);
            }
            Ok(())
        })?;
        if digest != m.seal.unit_digest {
            return Err(Error::SyncError(
                "hub does not hold the prerequisite complete content".into(),
            ));
        }
    }
    let mut conflicts = if request.kind == 2 {
        match request.cause.as_deref() {
            Some(cause @ ("unit_digest_mismatch" | "keep_first_refused")) => {
                let mut conflicts = db.dependency_unit_refusal_conflicts_owned(
                    &request.rows,
                    hub,
                    read_memory.clone(),
                )?;
                if conflicts.value.len() != request.rows.len()
                    || conflicts.value.iter().any(|c| {
                        if c.value.refusal_cause.is_some() {
                            c.value.winning_author_node_id.is_some()
                                || c.value.hub_acceptance_position.is_some()
                        } else {
                            c.value.winning_author_node_id.is_none()
                                || c.value.hub_acceptance_position.is_none()
                        }
                    })
                {
                    return Err(invalid());
                }
                for conflict in &mut conflicts.value {
                    replace_conflict_reason_owned(db, conflict, cause)?;
                }
                conflicts
            }
            Some("manifest_incomplete" | "member_digest_mismatch" | "binding_mismatch") => {
                OwnedPreparationValue::try_produce(db, 0, || Ok(Vec::new()))?
            }
            _ => return Err(invalid()),
        }
    } else {
        if request.cause.is_some() {
            return Err(invalid());
        }
        OwnedPreparationValue::try_produce(db, 0, || Ok(Vec::new()))?
    };
    let mut incumbent_lives = OwnedPreparationValue::try_produce(
        db,
        preparation_array_bytes::<OwnedPreparationValue<(LocalLifeAnchor, Digest)>>(m.rows.len())?,
        || {
            let capacity = m.rows.len();
            let value = Vec::with_capacity(capacity);
            if value.capacity() != capacity {
                return Err(invalid());
            }
            Ok(value)
        },
    )?;
    for (source, source_life) in m
        .rows
        .iter()
        .zip(std::iter::once(&m.root_life).chain(&m.member_lives))
    {
        if request.kind == 2 {
            incumbent_lives
                .value
                .push(OwnedPreparationValue::try_produce(
                    db,
                    source_life.original_table.len(),
                    || Ok((source_life.clone(), source.row_digest)),
                )?);
        } else {
            let row = final_row_owned(
                db,
                guard,
                &source.reference,
                request.kind == 0,
                read_memory.clone(),
            )?;
            let generation =
                db.durable_lineage_table_generation(&source.reference.table, read_memory.clone())?;
            incumbent_lives
                .value
                .push(OwnedPreparationValue::try_produce(
                    db,
                    source.reference.table.len(),
                    || {
                        Ok((
                            LocalLifeAnchor {
                                original_table: source.reference.table.clone(),
                                table_generation: generation,
                                local_row_id: row.value.row_id.0,
                                creation_lsn: row.value.lsn,
                            },
                            finite_row_digest(&row.value.values, None)?,
                        ))
                    },
                )?);
        }
    }
    if request.kind == 1 {
        let existing = db.custody_root(&m.seal.root, read_memory.clone())?;
        let complete=existing.iter().filter_map(|r|match r {
            Record::Terminal {edge:false,record:t} if t.kind!=2&&t.root==m.seal.root&&t.unit_digest==m.seal.unit_digest&&t.namespace.hub_node==hub=>Some(t),_=>None,
        }).any(|t|m.rows.iter().zip(&incumbent_lives.value).all(|(source,entry)|{
            let (life,digest)=&entry.value;
            existing.iter().any(|r|matches!(r,Record::MaterializedOwner(o) if o.namespace==t.namespace&&o.submission==t.submission
                &&o.incumbent_reference==source.reference&&o.incumbent==*life&&o.incumbent_row_digest==*digest))
        }));
        if !complete
            && db.custody_incumbent_digest(&m.seal.root, vector_copy_owner, read_memory.clone())?
                != Some(m.seal.unit_digest)
        {
            return Err(Error::SyncError(
                "equivalence requires a complete authenticated incumbent materialization".into(),
            ));
        }
    }
    let state = match reserved_state {
        Some(state) => OwnedPreparationValue::try_produce(db, 0, || Ok(state.clone()))?,
        None => next_state_owned(db, b.value.namespace.hub_incarnation, read_memory.clone())?,
    };
    let mut subjects = OwnedPreparationValue::try_produce(
        db,
        preparation_array_bytes::<(
            OwnedPreparationValue<RowRef>,
            OwnedPreparationValue<Vec<u8>>,
            usize,
        )>(m.rows.len())?,
        || {
            let mut subjects = Vec::with_capacity(m.rows.len());
            if subjects.capacity() != m.rows.len() {
                return Err(invalid());
            }
            for (position, row) in m.rows.iter().enumerate() {
                subjects.push((
                    reference_owned(db, &row.reference.table, &row.reference.key)?,
                    finite_bytes_owned(db, |e| {
                        e.key(&row.reference.key);
                        Ok(())
                    })?,
                    position,
                ));
            }
            subjects.sort_unstable_by(|a, b| {
                a.0.value
                    .table
                    .cmp(&b.0.value.table)
                    .then_with(|| a.1.value.cmp(&b.1.value))
                    .then_with(|| a.2.cmp(&b.2))
            });
            Ok(subjects)
        },
    )?;
    let mut block_bytes = preparation_array_bytes::<(RowRef, u8)>(m.rows.len())?
        .checked_add(preparation_array_bytes::<crate::sync_types::Conflict>(
            conflicts.value.len(),
        )?)
        .ok_or_else(invalid)?;
    for (reference, _, _) in &subjects.value {
        block_bytes = block_bytes
            .checked_add(count_reference_native(&reference.value)?)
            .ok_or_else(invalid)?;
    }
    for conflict in &conflicts.value {
        block_bytes = block_bytes
            .checked_add(conflict_native_bytes_owned(&conflict.value)?)
            .ok_or_else(invalid)?;
    }
    let block = OwnedPreparationValue::try_produce(db, block_bytes, || {
        Ok(DiagnosticBlock {
            salt: random_digest(),
            subjects: copy_finite_vec(&subjects.value, |(reference, _, _)| {
                Ok((copy_reference_finite(&reference.value)?, 0))
            })?,
            conflicts: copy_finite_vec(&conflicts.value, |c| copy_conflict_finite(&c.value))?,
        })
    })?;
    let claim = DiagnosticClaim {
        subject_count: block.value.subjects.len() as u64,
        conflict_count: block.value.conflicts.len() as u64,
        canonical_bytes: finite_len(|e| block.value.bytes_finite(e))? as u64,
        commitment: finite_hash(|e| {
            e.string("delivery-diagnostics.v1");
            block.value.bytes_finite(e)
        })?,
    };
    let bytes = namespace_native_bytes_owned(&b.value.namespace)?
        .checked_add(count_reference_native(&m.seal.root)?)
        .and_then(|n| n.checked_add(request.cause.as_ref().map_or(0, String::len)))
        .ok_or_else(invalid)?;
    let terminal = OwnedPreparationValue::try_produce(db, bytes, || {
        Ok(TerminalRecord {
            namespace: b.value.namespace.clone(),
            root: copy_reference_finite(&m.seal.root)?,
            origin_life_digest: m.seal.origin_life_digest,
            submission: m.id,
            seal_digest: m.seal.digest_finite()?,
            unit_digest: m.seal.unit_digest,
            source: m.seal.source,
            kind: request.kind,
            cause: request.cause.clone(),
            position: (guard.read().commit_lsn.ok_or_else(invalid)?, ordinal),
            state: state.value.state.clone(),
            diagnostics: claim,
            signature: Vec::new(),
        })
    })?;
    super::checkpoint::append_owned(
        db,
        guard,
        &terminal.value.namespace,
        signer,
        read_memory.clone(),
    )?;
    let bytes = finite_bytes_owned(db, |e| terminal.value.bytes_finite(e))?;
    let terminal = OwnedPreparationBody::from_unsigned(db, terminal)?;
    let terminal = terminal.sign(db, signer.as_ref(), &bytes.value, |terminal, signature| {
        terminal.signature = signature
    })?;
    terminal.value.verify_owned(db)?;
    let t = &terminal.value;
    let mut records = state_records_owned(
        db,
        state,
        &t.namespace.tenant,
        &t.namespace.hub_node,
        read_memory,
    )?;
    let additional = m
        .rows
        .len()
        .checked_mul(if t.kind != 2 { 2 } else { 1 })
        .and_then(|n| n.checked_add(4))
        .ok_or_else(invalid)?;
    let capacity = records
        .records
        .len()
        .checked_add(additional)
        .ok_or_else(invalid)?;
    let materialized_signatures = if t.kind != 2 {
        m.rows.iter().try_fold(0usize, |n, row| {
            n.checked_add(signature_count(&row.creator.attestation)?)
                .ok_or_else(invalid)
        })?
    } else {
        0
    };
    let debit_capacity = records
        .debits
        .len()
        .checked_add(additional)
        .and_then(|n| n.checked_add(manifest_signature_count(m).ok()?))
        .and_then(|n| n.checked_add(signature_count(&t.signature).ok()?))
        .and_then(|n| n.checked_add(materialized_signatures))
        .ok_or_else(invalid)?;
    records.grow(db, capacity, debit_capacity)?;
    for (source, entry) in m.rows.iter().zip(&incumbent_lives.value) {
        let (incumbent, digest) = &entry.value;
        if t.kind != 2 {
            let native = namespace_native_bytes_owned(&t.namespace)?
                .checked_add(row_evidence_native_bytes_owned(source)?)
                .and_then(|n| n.checked_add(incumbent.original_table.len()))
                .and_then(|n| n.checked_add(count_reference_native(&source.reference).ok()?))
                .and_then(|n| n.checked_add(incumbent_lives.value[0].value.0.original_table.len()))
                .ok_or_else(invalid)?;
            records.push_body(OwnedPreparationBody::try_produce(
                db,
                native,
                signature_count(&source.creator.attestation)?,
                0,
                || {
                    Ok(Record::MaterializedOwner(MaterializedOwner {
                        namespace: t.namespace.clone(),
                        submission: t.submission,
                        source: copy_row_evidence_finite(source)?,
                        incumbent: incumbent.clone(),
                        incumbent_reference: copy_reference_finite(&source.reference)?,
                        incumbent_row_digest: *digest,
                        root_incumbent: incumbent_lives.value[0].value.0.clone(),
                    }))
                },
            )?);
        }
        records.push(OwnedPreparationValue::try_produce(
            db,
            namespace_native_bytes_owned(&t.namespace)?
                .checked_add(incumbent.original_table.len())
                .ok_or_else(invalid)?,
            || {
                Ok(Record::DiagnosticOwner {
                    namespace: t.namespace.clone(),
                    submission: t.submission,
                    life: incumbent.clone(),
                })
            },
        )?);
    }
    records.push_body(OwnedPreparationBody::try_produce(
        db,
        terminal_native_bytes_owned(t)?,
        signature_count(&t.signature)?,
        0,
        || {
            Ok(Record::Terminal {
                edge: false,
                record: copy_terminal_finite(t)?,
            })
        },
    )?);
    records.push(OwnedPreparationValue::try_produce(
        db,
        namespace_native_bytes_owned(&t.namespace)?,
        || {
            Ok(Record::Order {
                namespace: t.namespace.clone(),
                position: t.position,
                submission: t.submission,
            })
        },
    )?);
    records.push_body(OwnedPreparationBody::try_produce(
        db,
        count_manifest_native_finite(m)?
            .checked_add(std::mem::size_of::<ManifestRecord>())
            .ok_or_else(invalid)?,
        manifest_signature_count(m)?,
        0,
        || Ok(Record::Manifest(Box::new(copy_manifest_finite(m)?))),
    )?);
    records.push(OwnedPreparationValue::try_produce(
        db,
        namespace_native_bytes_owned(&t.namespace)?
            .checked_add(diagnostic_native_bytes_owned(&block.value)?)
            .ok_or_else(invalid)?,
        || {
            Ok(Record::Diagnostic {
                namespace: t.namespace.clone(),
                submission: t.submission,
                block: copy_diagnostic_finite(&block.value)?,
            })
        },
    )?);
    Ok(records)
}

impl OwnedPreparationValue<Vec<BirthRef>> {
    pub(crate) fn pop_slot(&mut self) -> Option<BirthRef> {
        self.value.pop()
    }
}

fn edge_authority_owned(
    db: &Database,
    namespace: &Namespace,
    state: &StateRef,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedPreparationValue<EdgeAuthority>> {
    let records = db.custody_authority(read_memory)?;
    let birth = BirthRef::from(state);
    let old = records.iter().find_map(|r| match r {
        Record::Destination(a) => Some(a),
        _ => None,
    });
    let changed = old.is_some_and(|old| old.namespace != *namespace || old.birth != birth);
    OwnedPreparationValue::try_produce(db, namespace_native_bytes_owned(namespace)?, || {
        Ok(EdgeAuthority {
            destination_epoch: if changed {
                old.expect("selected authority")
                    .destination_epoch
                    .checked_add(1)
                    .ok_or_else(invalid)?
            } else {
                old.map_or(0, |a| a.destination_epoch)
            },
            namespace: namespace.clone(),
            birth,
            validity_revision: if changed {
                old.expect("selected authority")
                    .validity_revision
                    .checked_add(1)
                    .ok_or_else(invalid)?
            } else {
                old.map_or(0, |a| a.validity_revision)
            },
        })
    })
}
pub(crate) fn validate_admitted_binding_owned(
    db: &Database,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    packet: &SignedBindingPacket,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<bool> {
    let b = &packet.binding;
    let certificate = &packet.certificate;
    certificate.verify_owned(db)?;
    if certificate.tenant != b.namespace.tenant
        || certificate.hub_node != b.namespace.hub_node
        || certificate.state.state != b.issuance_state
    {
        return Err(invalid());
    }
    b.verify_owned(db)?;
    if &b.namespace.tenant != tenant
        || b.namespace.hub_node != hub
        || b.namespace.edge_node != edge
        || db
            .existing_sync_incarnation(tenant, read_memory.clone())?
            .is_some_and(|inc| inc != b.namespace.edge_incarnation)
    {
        return Err(invalid());
    }
    let records = db.custody_authority(read_memory)?;
    Ok(records
        .iter()
        .any(|r| matches!(r,Record::Binding(prior) if prior==b)))
}
// The independent admission closure: its caller owns the original funded
// packet and retains the returned records through the eventual terminal.
pub(crate) fn prepare_admitted_binding_owned(
    db: &Database,
    guard: &mut contextdb_tx::manager::EngineGuard<'_>,
    tenant: &TenantId,
    packet: &SignedBindingPacket,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedPreparationRecords> {
    let b = &packet.binding;
    let certificate = &packet.certificate;
    let key = prepare_string_owned(db, format_args!("sync_incarnation:{}", tenant.as_str()))?;
    let incarnation =
        prepare_string_owned(db, format_args!("{:032x}", b.namespace.edge_incarnation.0))?;
    let size = count_config_encoding(&incarnation.value)?;
    db.stage_owned_delivery_aux(
        guard,
        key.value.len().checked_add(size).ok_or_else(invalid)?,
        || {
            let mut bytes = Vec::with_capacity(size);
            if bytes.capacity() != size {
                return Err(invalid());
            }
            emit_config_encoding(&incarnation.value, &mut bytes)?;
            let copied_key = key.value.clone();
            if copied_key.capacity() != copied_key.len() {
                return Err(invalid());
            }
            Ok(contextdb_tx::staged_source::AuxBody::ConfigWrite((
                copied_key, bytes,
            )))
        },
    )?;
    let existing = db.custody_authority(read_memory.clone())?;
    let old_count = existing
        .iter()
        .filter(|r| matches!(r,Record::Binding(prior) if prior.namespace==b.namespace))
        .count();
    let selectors = OwnedPreparationValue::try_produce(
        db,
        preparation_array_bytes::<OwnedPreparationValue<String>>(old_count)?,
        || {
            let mut selectors = Vec::with_capacity(old_count);
            if selectors.capacity() != old_count {
                return Err(invalid());
            }
            for prior in existing.iter().filter_map(|r| match r {
                Record::Binding(prior) if prior.namespace == b.namespace => Some(prior),
                _ => None,
            }) {
                let digest = object_digest_finite(0, |e| prior.bytes_finite(e), &prior.signature)?;
                selectors.push(prepare_string_owned(
                    db,
                    format_args!("@object:{}", BorrowedHex(&digest)),
                )?);
            }
            Ok(selectors)
        },
    )?;
    let selected_keys = OwnedPreparationValue::try_produce(
        db,
        preparation_array_bytes::<String>(old_count)?
            .checked_add(selectors.value.iter().try_fold(0usize, |n, s| {
                n.checked_add(s.value.len()).ok_or_else(invalid)
            })?)
            .ok_or_else(invalid)?,
        || Ok(copy_finite_vec(&selectors.value, |s| Ok(s.value.clone()))?),
    )?;
    let objects = db.custody_records_in(&selected_keys.value, read_memory.clone())?;
    let object_digest = object_digest_finite(0, |e| b.bytes_finite(e), &b.signature)?;
    for prior in existing
        .iter()
        .chain(objects.iter())
        .filter_map(|r| match r {
            Record::Binding(prior) if prior.namespace == b.namespace => Some(prior),
            _ => None,
        })
    {
        let digest = object_digest_finite(0, |e| prior.bytes_finite(e), &prior.signature)?;
        if digest != object_digest {
            for record in existing.iter().chain(objects.iter()) {
                if matches!(record,Record::ObjectAdmission(a) if a.object_digest==digest) {
                    let key = record_key_owned(db, record)?;
                    db.stage_owned_delivery_aux(guard, key.value.len(), || {
                        Ok(contextdb_tx::staged_source::AuxBody::ConfigDelete(
                            key.value.clone(),
                        ))
                    })?;
                }
            }
        }
    }
    let authority = edge_authority_owned(db, &b.namespace, &b.issuance_state, read_memory.clone())?;
    let state = next_state_owned(db, b.namespace.edge_incarnation, read_memory.clone())?;
    let mut records = state_records_owned(
        db,
        state,
        &b.namespace.tenant,
        &b.namespace.hub_node,
        read_memory,
    )?;
    for record in &mut records.records {
        if let Record::Control(control) = record {
            control.hub_node = None;
            control.tenant = None;
        }
    }
    let capacity = records.records.len().checked_add(4).ok_or_else(invalid)?;
    let debit_capacity = records
        .debits
        .len()
        .checked_add(4)
        .and_then(|n| n.checked_add(signature_count(&b.signature).ok()?))
        .and_then(|n| n.checked_add(signature_count(&certificate.signature).ok()?))
        .ok_or_else(invalid)?;
    records.grow(db, capacity, debit_capacity)?;
    records.push_body(OwnedPreparationBody::try_produce(
        db,
        binding_native_bytes_owned(b)?,
        signature_count(&b.signature)?,
        0,
        || Ok(Record::Binding(b.clone())),
    )?);
    records.push_body(OwnedPreparationBody::try_produce(
        db,
        certificate_native_bytes_owned(certificate)?,
        signature_count(&certificate.signature)?,
        0,
        || Ok(Record::Certificate(certificate.clone())),
    )?);
    records.push(OwnedPreparationValue::try_produce(
        db,
        namespace_native_bytes_owned(&authority.value.namespace)?,
        || Ok(Record::Destination(authority.value.clone())),
    )?);
    records.push(OwnedPreparationValue::try_produce(
        db,
        namespace_native_bytes_owned(&authority.value.namespace)?,
        || {
            Ok(Record::ObjectAdmission(Admission {
                object_digest,
                authority: authority.value.clone(),
                owning_state: b.issuance_state.clone(),
                certificate: certificate.id_finite()?,
            }))
        },
    )?);
    Ok(records)
}
pub(crate) fn prepare_terminal_batch_owned(
    db: &Database,
    guard: &mut contextdb_tx::manager::EngineGuard<'_>,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    requests: &[TerminalInput],
    signer: &LineageSigner,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedPreparationRecords> {
    if requests.is_empty()
        || requests.iter().any(|r| {
            r.kind != 0
                || r.tenant_id != *tenant
                || r.edge_incarnation != requests[0].edge_incarnation
                || r.manifest.seal.source.0 != requests[0].manifest.seal.source.0
        })
    {
        return Err(invalid());
    }
    let incarnation = binding_owned(db, edge, read_memory.clone())?
        .value
        .namespace
        .hub_incarnation;
    let state = next_state_owned(db, incarnation, read_memory.clone())?;
    let capacity = requests.iter().try_fold(0usize, |n, request| {
        request
            .manifest
            .rows
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(7))
            .and_then(|slots| n.checked_add(slots))
            .ok_or_else(invalid)
    })?;
    let debit_capacity = requests.iter().try_fold(capacity, |n, request| {
        n.checked_add(
            request
                .manifest
                .rows
                .len()
                .checked_mul(2)
                .ok_or_else(invalid)?,
        )
        .and_then(|n| n.checked_add(request.manifest.schemas.len()))
        .and_then(|n| n.checked_add(usize::from(request.manifest.binding.is_some())))
        .and_then(|n| n.checked_add(2))
        .ok_or_else(invalid)
    })?;
    let mut result = OwnedPreparationRecords::try_new(db, capacity, debit_capacity)?;
    let mut keyed: OwnedPreparationValue<Vec<(OwnedPreparationValue<String>, Record)>> =
        OwnedPreparationValue::try_produce(
            db,
            preparation_array_bytes::<(OwnedPreparationValue<String>, Record)>(capacity)?,
            || {
                let capacity = capacity;
                let value = Vec::with_capacity(capacity);
                if value.capacity() != capacity {
                    return Err(invalid());
                }
                Ok(value)
            },
        )?;
    for (ordinal, request) in requests.iter().enumerate() {
        let mut batch = prepare_terminal_at_owned(
            db,
            guard,
            tenant,
            hub,
            edge,
            request,
            signer,
            Some(&state.value),
            u32::try_from(ordinal).map_err(|_| invalid())?,
            read_memory.clone(),
        )?;
        // Move all exact original invoices before any body moves. Displaced
        // duplicate bodies die while their original credit stays in result.
        assert!(
            batch.debits.len() <= result.debits.capacity() - result.debits.len(),
            "preadmitted batch original invoices"
        );
        result.debits.extend(batch.debits.drain(..));
        for record in batch.records.drain(..) {
            let key = record_key_owned(db, &record)?;
            match keyed
                .value
                .binary_search_by(|prior| prior.0.value.cmp(&key.value))
            {
                Ok(index) => {
                    let old = prepare_named_encoding_owned(db, &keyed.value[index].1)?;
                    let next = prepare_named_encoding_owned(db, &record)?;
                    if old.value != next.value {
                        return Err(invalid());
                    }
                    keyed.value[index] = (key, record);
                }
                Err(index) => {
                    assert!(
                        keyed.value.len() < keyed.value.capacity(),
                        "preadmitted batch key slot"
                    );
                    keyed.value.insert(index, (key, record));
                }
            }
        }
        // Equality refusal occurs before preparing the next sibling, exactly
        // as in the original closure, including signer and checkpoint order.
    }
    for (key, record) in keyed.value.drain(..) {
        result.records.push(record);
        drop(key);
    }
    Ok(result)
}
impl OwnedPreparationRecords {
    pub(crate) fn stage_owned(
        &self,
        db: &Database,
        guard: &mut contextdb_tx::manager::EngineGuard<'_>,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        for record in &self.records {
            record.verify_owned(db, read_memory.clone())?;
            let key = record_key_owned(db, record)?;
            let capacity = count_named_encoding(record)?;
            db.stage_owned_delivery_aux(
                guard,
                key.value.len().checked_add(capacity).ok_or_else(invalid)?,
                || {
                    let mut encoded = Vec::with_capacity(capacity);
                    if encoded.capacity() != capacity {
                        return Err(invalid());
                    }
                    emit_named_encoding(record, &mut encoded)?;
                    if encoded.len() != capacity || encoded.capacity() != capacity {
                        return Err(invalid());
                    }
                    let copied_key = key.value.clone();
                    if copied_key.capacity() != copied_key.len() {
                        return Err(invalid());
                    }
                    Ok(contextdb_tx::staged_source::AuxBody::ConfigWrite((
                        copied_key, encoded,
                    )))
                },
            )?;
        }
        Ok(())
    }
}
// This is the common, independently completed delivery closure leg. The
// original P supplies borrowed registrations; the caller retains outputs
// outside its terminal closure across apply, post_commit and error cleanup.
pub(crate) fn prepare_delivery_owned(
    db: &Database,
    guard: &mut contextdb_tx::manager::EngineGuard<'_>,
    registrations: &[SourceRegistration],
    mut callback: Option<&mut OwnedCommitPreparation<'_>>,
    outputs: &mut OwnedPreparationOutputs,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<()> {
    for registration in registrations {
        let batch = registration.prepare_owned(db, guard, read_memory.clone())?;
        outputs.retain(batch);
        outputs
            .batches
            .last()
            .expect("retained delivery batch")
            .stage_owned(db, guard, read_memory.clone())?;
    }
    if let Some(callback) = callback.as_mut() {
        let batch = callback(db, guard)?;
        outputs.retain(batch);
        outputs
            .batches
            .last()
            .expect("retained delivery callback batch")
            .stage_owned(db, guard, read_memory)?;
    }
    Ok(())
}

fn normalized_policy_owned(
    db: &Database,
    table: &str,
    policy: &ApplicationTablePolicy,
) -> Result<OwnedPreparationValue<ApplicationTablePolicy>> {
    let mut policy =
        OwnedPreparationValue::try_produce(
            db,
            count_policy_native(policy)?,
            || Ok(policy.clone()),
        )?;
    if let Some(tables) = &mut policy.value.manifest_tables {
        tables.sort_unstable();
        if tables.is_empty() || tables.windows(2).any(|w| w[0] == w[1]) {
            return Err(invalid());
        }
    }
    finite_policy_digest(table, &policy.value)?;
    Ok(policy)
}
pub(crate) fn prepare_issued_binding_owned(
    db: &Database,
    guard: &mut contextdb_tx::manager::EngineGuard<'_>,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    incarnation: Incarnation,
    request: &BindingInput,
    signer: &LineageSigner,
    result: &mut Option<OwnedPreparationBody<SignedBinding>>,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedPreparationRecords> {
    let declared = db.custody_authority(read_memory.clone())?;
    let count = request.expectation.tables.len();
    let mut policies = OwnedPreparationValue::try_produce(
        db,
        preparation_array_bytes::<OwnedPreparationValue<BoundPolicy>>(count)?,
        || {
            let capacity = count;
            let value = Vec::with_capacity(capacity);
            if value.capacity() != capacity {
                return Err(invalid());
            }
            Ok(value)
        },
    )?;
    let mut declarations = OwnedPreparationValue::try_produce(
        db,
        preparation_array_bytes::<OwnedPreparationValue<PolicyRecord>>(count)?,
        || {
            let capacity = count;
            let value = Vec::with_capacity(capacity);
            if value.capacity() != capacity {
                return Err(invalid());
            }
            Ok(value)
        },
    )?;
    for (table, expected) in &request.expectation.tables {
        let declaration = declared
            .iter()
            .find_map(|r| match r {
                Record::Policy(p) if p.table == *table => Some(p),
                _ => None,
            })
            .ok_or_else(|| Error::TenantPolicyNotDeclared {
                table: table.clone(),
            })?;
        let expected = normalized_policy_owned(db, table, expected)?;
        if declaration.digest != finite_policy_digest(table, &expected.value)? {
            return Err(Error::TenantPolicyMismatch {
                table: table.clone(),
                clause: "binding comparison".into(),
            });
        }
        if declaration.tenant.as_ref().is_some_and(|t| t != tenant) {
            return Err(invalid());
        }
        if let Some(installed) = db.with_owned_delivery_meta_if_present(table, |meta| {
            let direction = meta
                .sync_direction
                .unwrap_or(contextdb_core::DEFAULT_SYNC_DIRECTION);
            let conflict = meta
                .conflict_policy
                .unwrap_or(contextdb_core::DEFAULT_CONFLICT_POLICY);
            // Borrow the installed manifest table list in the actual policy copier.
            let bytes = meta
                .delivery_manifest_tables
                .as_ref()
                .map_or(Ok(0), |tables| {
                    tables
                        .iter()
                        .try_fold(preparation_array_bytes::<String>(tables.len())?, |n, t| {
                            n.checked_add(t.len()).ok_or_else(invalid)
                        })
                })?;
            let installed = OwnedPreparationValue::try_produce(db, bytes, || {
                Ok(ApplicationTablePolicy {
                    sync_direction: direction,
                    sync_conflict: conflict,
                    immutable: meta.immutable,
                    retain: meta
                        .default_ttl_seconds
                        .map(|seconds| RetentionDeclaration {
                            seconds,
                            declared_unit: meta
                                .retain_declared_unit
                                .unwrap_or(contextdb_core::RetainUnit::Seconds),
                            sync_safe: meta.sync_safe,
                        }),
                    history: meta
                        .history_policy
                        .unwrap_or(contextdb_core::DEFAULT_HISTORY_POLICY),
                    manifest_tables: meta.delivery_manifest_tables.clone(),
                    edge_discard: meta.edge_discard.unwrap_or_default(),
                })
            })?;
            finite_policy_digest(table, &installed.value)
        })? && installed != declaration.digest
        {
            return Err(Error::SyncError(format!(
                "hub installed policy differs from declaration for {table}"
            )));
        }
        let bytes = declaration
            .table
            .len()
            .checked_add(count_policy_native(&declaration.policy)?)
            .and_then(|n| n.checked_add(tenant.as_str().len()))
            .ok_or_else(invalid)?;
        declarations
            .value
            .push(OwnedPreparationValue::try_produce(db, bytes, || {
                Ok(PolicyRecord {
                    table: declaration.table.clone(),
                    policy: declaration.policy.clone(),
                    version: declaration.version,
                    digest: declaration.digest,
                    tenant: Some(tenant.clone()),
                    first_bound_position: declaration.first_bound_position,
                    first_bound_at: declaration.first_bound_at,
                })
            })?);
        policies.value.push(OwnedPreparationValue::try_produce(
            db,
            table
                .len()
                .checked_add(count_policy_native(&declaration.policy)?)
                .ok_or_else(invalid)?,
            || {
                Ok(BoundPolicy {
                    table: table.clone(),
                    policy: declaration.policy.clone(),
                    version: declaration.version,
                    digest: declaration.digest,
                })
            },
        )?);
    }
    let state = next_state_owned(db, incarnation, read_memory.clone())?;
    let mut bytes = tenant
        .as_str()
        .len()
        .checked_add(hub.len())
        .and_then(|n| n.checked_add(edge.len()))
        .and_then(|n| n.checked_add(preparation_array_bytes::<BoundPolicy>(count).ok()?))
        .ok_or_else(invalid)?;
    for policy in &policies.value {
        bytes = bytes
            .checked_add(bound_policy_native_bytes_owned(&policy.value)?)
            .ok_or_else(invalid)?;
    }
    let binding = OwnedPreparationValue::try_produce(db, bytes, || {
        Ok(SignedBinding {
            format: 1,
            namespace: Namespace {
                tenant: tenant.clone(),
                hub_node: hub.into(),
                hub_incarnation: incarnation,
                edge_node: edge.into(),
                edge_incarnation: request.edge_incarnation,
            },
            tables: copy_finite_vec(&policies.value, |p| Ok(p.value.clone()))?,
            issuance_state: state.value.state.clone(),
            issuance_source: (guard.read().commit_lsn.ok_or_else(invalid)?, 0),
            signature: Vec::new(),
        })
    })?;
    let signed = finite_bytes_owned(db, |e| binding.value.bytes_finite(e))?;
    let binding = OwnedPreparationBody::from_unsigned(db, binding)?;
    let binding = binding.sign(db, signer.as_ref(), &signed.value, |binding, signature| {
        binding.signature = signature
    })?;
    binding.value.verify_owned(db)?;
    *result = Some(OwnedPreparationBody::try_produce(
        db,
        binding_native_bytes_owned(&binding.value)?,
        signature_count(&binding.value.signature)?,
        0,
        || Ok(binding.value.clone()),
    )?);
    let mut out = state_records_owned(
        db,
        state,
        &binding.value.namespace.tenant,
        &binding.value.namespace.hub_node,
        read_memory,
    )?;
    let capacity = out
        .records
        .len()
        .checked_add(1)
        .and_then(|n| n.checked_add(count))
        .ok_or_else(invalid)?;
    let debit_capacity = out
        .debits
        .len()
        .checked_add(2)
        .and_then(|n| n.checked_add(count))
        .ok_or_else(invalid)?;
    out.grow(db, capacity, debit_capacity)?;
    binding.transfer_into(&mut out, |binding, out| {
        out.records.push(Record::Binding(binding))
    });
    let lsn = guard.read().commit_lsn.ok_or_else(invalid)?;
    for mut declaration in declarations.value.drain(..) {
        declaration.value.first_bound_position.get_or_insert(lsn);
        declaration
            .value
            .first_bound_at
            .get_or_insert(contextdb_core::Wallclock::now().0);
        declaration.transfer_into(&mut out, |declaration, out| {
            out.records.push(Record::Policy(declaration))
        });
    }
    Ok(out)
}

impl OwnedPreparationRecords {
    pub(crate) fn with_record_references<R>(
        &self,
        db: &Database,
        f: impl for<'a> FnOnce(&'a [&'a Record]) -> Result<R>,
    ) -> Result<R> {
        let references = OwnedPreparationValue::try_produce(
            db,
            preparation_array_bytes::<&Record>(self.records.len())?,
            || Ok(self.records.iter().collect::<Vec<_>>()),
        )?;
        references.with_value(|references| f(references))
    }
}
pub(crate) fn reference_from_row_owned(
    db: &Database,
    table: &str,
    meta: &contextdb_core::TableMeta,
    values: &std::collections::HashMap<String, contextdb_core::Value>,
) -> Result<Option<OwnedPreparationValue<RowRef>>> {
    let columns: &[String] = if let Some(column) = &meta.natural_key_column {
        std::slice::from_ref(column)
    } else if !meta.primary_key_columns.is_empty() {
        &meta.primary_key_columns
    } else if let Some(column) = meta.columns.iter().find(|c| c.primary_key) {
        std::slice::from_ref(&column.name)
    } else if let Some(column) = meta.columns.iter().find(|c| c.name == "id") {
        std::slice::from_ref(&column.name)
    } else {
        return Ok(None);
    };
    if columns.iter().any(|column| !values.contains_key(column)) {
        return Ok(None);
    }
    let mut bytes = table
        .len()
        .checked_add(preparation_array_bytes::<(String, contextdb_core::Value)>(
            columns.len() - 1,
        )?)
        .ok_or_else(invalid)?;
    for column in columns {
        bytes = bytes
            .checked_add(column.len())
            .and_then(|n| {
                n.checked_add(
                    crate::memory_accounting::preparation_value_copy_bytes(&values[column]).ok()?,
                )
            })
            .ok_or_else(invalid)?;
    }
    OwnedPreparationValue::try_produce(db, bytes, || {
        let mut rest = Vec::with_capacity(columns.len() - 1);
        if rest.capacity() != columns.len() - 1 {
            return Err(invalid());
        }
        for column in &columns[1..] {
            rest.push((
                column.clone(),
                crate::memory_accounting::preparation_copy_value(&values[column])?,
            ));
        }
        Ok(RowRef {
            table: table.into(),
            key: NaturalKey {
                column: columns[0].clone(),
                value: crate::memory_accounting::preparation_copy_value(&values[&columns[0]])?,
                rest,
            },
        })
    })
    .map(Some)
}

pub(crate) struct OwnedTerminalBatchInput {
    changes: OwnedPreparationValue<ChangeSet>,
    lineages: OwnedPreparationBody<
        Vec<(
            String,
            NaturalKey,
            contextdb_core::Lsn,
            crate::protocol::WireRowLineage,
        )>,
    >,
    receipt: OwnedPreparationValue<crate::database::SyncApplyReceipt>,
}
impl OwnedTerminalBatchInput {
    pub(crate) fn with_inputs<R>(
        &self,
        f: impl for<'a> FnOnce(
            &'a ChangeSet,
            &'a [(
                String,
                NaturalKey,
                contextdb_core::Lsn,
                crate::protocol::WireRowLineage,
            )],
            &'a crate::database::SyncApplyReceipt,
        ) -> Result<R>,
    ) -> Result<R> {
        f(
            &self.changes.value,
            &self.lineages.value,
            &self.receipt.value,
        )
    }
}
pub(crate) fn prepare_terminal_batch_input_owned(
    db: &Database,
    tenant: &TenantId,
    edge: &str,
    requests: &[TerminalInput],
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<OwnedTerminalBatchInput> {
    if requests.is_empty()
        || requests.iter().any(|r| {
            r.kind != 0
                || r.tenant_id != *tenant
                || r.edge_incarnation != requests[0].edge_incarnation
                || r.manifest.seal.source.0 != requests[0].manifest.seal.source.0
        })
    {
        return Err(invalid());
    }
    let row_count = requests.iter().try_fold(0usize, |n, r| {
        n.checked_add(r.rows.len()).ok_or_else(invalid)
    })?;
    let lineage_count = requests.iter().try_fold(0usize, |n, r| {
        n.checked_add(r.manifest.rows.len()).ok_or_else(invalid)
    })?;
    let unique = OwnedPreparationValue::try_produce(
        db,
        preparation_array_bytes::<(&RowChange, OwnedPreparationValue<Vec<u8>>, Digest)>(row_count)?,
        || {
            let mut unique: Vec<(&RowChange, OwnedPreparationValue<Vec<u8>>, Digest)> =
                Vec::with_capacity(row_count);
            if unique.capacity() != row_count {
                return Err(invalid());
            }
            for request in requests {
                request.manifest.verify_owned(db, read_memory.clone())?;
                for row in &request.rows {
                    let key = finite_bytes_owned(db, |e| {
                        e.key(&row.natural_key);
                        Ok(())
                    })?;
                    let digest = finite_row_digest(&row.values, None)?;
                    if let Some(prior) = unique.iter_mut().find(|(prior, prior_key, _)| {
                        prior.table == row.table && prior_key.value == key.value
                    }) {
                        if prior.2 != digest {
                            return Err(invalid());
                        }
                        prior.2 = digest;
                    } else {
                        unique.push((row, key, digest));
                    }
                }
            }
            Ok(unique)
        },
    )?;
    let bytes = unique.value.iter().try_fold(
        preparation_array_bytes::<RowChange>(unique.value.len())?,
        |n, (row, _, _)| {
            n.checked_add(row_change_native_bytes_owned(row)?)
                .ok_or_else(invalid)
        },
    )?;
    let changes = OwnedPreparationValue::try_produce(db, bytes, || {
        Ok(ChangeSet {
            rows: copy_finite_vec(&unique.value, |(row, _, _)| copy_row_change_finite(row))?,
            ..Default::default()
        })
    })?;
    let mut bytes = preparation_array_bytes::<(
        String,
        NaturalKey,
        contextdb_core::Lsn,
        crate::protocol::WireRowLineage,
    )>(lineage_count)?;
    for request in requests {
        for row in &request.manifest.rows {
            bytes = bytes
                .checked_add(count_reference_native(&row.reference)?)
                .and_then(|n| n.checked_add(lineage_native_bytes_owned(&row.creator).ok()?))
                .ok_or_else(invalid)?;
        }
    }
    let signature_count = requests.iter().try_fold(0usize, |n, request| {
        request.manifest.rows.iter().try_fold(n, |n, row| {
            n.checked_add(signature_count(&row.creator.attestation)?)
                .ok_or_else(invalid)
        })
    })?;
    let lineages = OwnedPreparationBody::try_produce(db, bytes, signature_count, 0, || {
        let mut lineages = Vec::with_capacity(lineage_count);
        if lineages.capacity() != lineage_count {
            return Err(invalid());
        }
        for request in requests {
            for row in &request.manifest.rows {
                lineages.push((
                    row.reference.table.clone(),
                    copy_key_native(&row.reference.key)?,
                    row.authored_source.0,
                    row.creator.clone(),
                ));
            }
        }
        Ok(lineages)
    })?;
    let receipt = OwnedPreparationValue::try_produce(
        db,
        tenant
            .as_str()
            .len()
            .checked_add(edge.len())
            .ok_or_else(invalid)?,
        || {
            Ok(crate::database::SyncApplyReceipt {
                tenant_id: tenant.clone(),
                node_id: edge.into(),
                incarnation: requests[0].edge_incarnation,
                source_lsn: requests[0].manifest.seal.source.0,
                dependency_complete: true,
                hub_contact_ms: None,
            })
        },
    )?;
    Ok(OwnedTerminalBatchInput {
        changes,
        lineages,
        receipt,
    })
}

impl OwnedPreparationRecords {
    pub(crate) fn copy_record_finite(&mut self, db: &Database, record: &Record) -> Result<()> {
        assert!(
            self.records.len() < self.records.capacity(),
            "preadmitted record slot"
        );
        let needed = self
            .debits
            .len()
            .checked_add(1)
            .and_then(|n| n.checked_add(record_signature_count(record).ok()?))
            .ok_or_else(invalid)?;
        if needed > self.debits.capacity() {
            self.grow(db, self.records.capacity(), needed)?;
        }
        self.push_body(copy_record_body_owned(db, record)?);
        Ok(())
    }
}

pub(crate) fn copy_terminal_input_owned(
    db: &Database,
    input: &TerminalInput,
) -> Result<OwnedPreparationBody<TerminalInput>> {
    let mut bytes = input
        .tenant_id
        .as_str()
        .len()
        .checked_add(count_manifest_native_finite(&input.manifest)?)
        .and_then(|n| n.checked_add(input.cause.as_ref().map_or(0, String::len)))
        .and_then(|n| n.checked_add(preparation_array_bytes::<RowChange>(input.rows.len()).ok()?))
        .ok_or_else(invalid)?;
    for row in &input.rows {
        bytes = bytes
            .checked_add(row_change_native_bytes_owned(row)?)
            .ok_or_else(invalid)?;
    }
    OwnedPreparationBody::try_produce(
        db,
        bytes,
        manifest_signature_count(&input.manifest)?,
        0,
        || {
            Ok(TerminalInput {
                tenant_id: input.tenant_id.clone(),
                edge_incarnation: input.edge_incarnation,
                manifest: copy_manifest_finite(&input.manifest)?,
                rows: copy_finite_vec(&input.rows, copy_row_change_finite)?,
                kind: input.kind,
                cause: input.cause.clone(),
            })
        },
    )
}
pub(crate) fn terminal_callback_owned<'a>(
    tenant: &'a TenantId,
    hub: &'a str,
    edge: &'a str,
    input: OwnedPreparationBody<TerminalInput>,
    signer: &'a LineageSigner,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> impl for<'g, 'h> FnMut(
    &Database,
    &'g mut contextdb_tx::manager::EngineGuard<'h>,
) -> Result<OwnedPreparationRecords>
+ 'a {
    let mut prepared = Some(input);
    move |db, guard| {
        let input = prepared.take().ok_or_else(invalid)?;
        input.with_value(|input| {
            prepare_terminal_owned(
                db,
                guard,
                tenant,
                hub,
                edge,
                input,
                signer,
                read_memory.clone(),
            )
        })
    }
}
pub(crate) fn binding_callback_owned<'a>(
    tenant: &'a TenantId,
    packet: &'a SignedBindingPacket,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> impl for<'g, 'h> FnMut(
    &Database,
    &'g mut contextdb_tx::manager::EngineGuard<'h>,
) -> Result<OwnedPreparationRecords>
+ 'a {
    move |db, guard| prepare_admitted_binding_owned(db, guard, tenant, packet, read_memory.clone())
}
pub(crate) fn terminal_batch_callback_owned<'a>(
    tenant: &'a TenantId,
    hub: &'a str,
    edge: &'a str,
    requests: &'a [TerminalInput],
    signer: &'a LineageSigner,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> impl for<'g, 'h> FnMut(
    &Database,
    &'g mut contextdb_tx::manager::EngineGuard<'h>,
) -> Result<OwnedPreparationRecords>
+ 'a {
    move |db, guard| {
        prepare_terminal_batch_owned(
            db,
            guard,
            tenant,
            hub,
            edge,
            requests,
            signer,
            read_memory.clone(),
        )
    }
}

// Original body and signature invoices are separate; payload destruction
// precedes every invoice, including invoice-vector backing on Err or unwind.
pub(crate) struct OwnedPreparationBody<T> {
    value: T,
    debit: crate::memory_accounting::OwnedMemoryReservation,
    signatures: OwnedPreparationValue<Vec<crate::memory_accounting::OwnedMemoryReservation>>,
}
impl<T> OwnedPreparationBody<T> {
    fn try_produce(
        db: &Database,
        native_bytes: usize,
        copied_signatures: usize,
        spare_signatures: usize,
        produce: impl FnOnce() -> Result<T>,
    ) -> Result<Self>
    where
        T: PreparationSignatureLayout,
    {
        let signature_bytes = copied_signatures.checked_mul(64).ok_or_else(invalid)?;
        let debit = db.reserve_owned_preparation(
            native_bytes
                .checked_sub(signature_bytes)
                .ok_or_else(invalid)?,
        )?;
        let slots = copied_signatures
            .checked_add(spare_signatures)
            .ok_or_else(invalid)?;
        let mut signatures = OwnedPreparationValue::try_produce(
            db,
            preparation_array_bytes::<crate::memory_accounting::OwnedMemoryReservation>(slots)?,
            || {
                let invoices = Vec::with_capacity(slots);
                if invoices.capacity() != slots {
                    return Err(invalid());
                }
                Ok(invoices)
            },
        )?;
        for _ in 0..copied_signatures {
            signatures.push_slot(db.reserve_owned_preparation(64)?);
        }
        let value = produce()?;
        if value.checked_signature_layout()? != copied_signatures {
            return Err(invalid());
        }
        Ok(Self {
            value,
            debit,
            signatures,
        })
    }
    fn from_unsigned(db: &Database, unsigned: OwnedPreparationValue<T>) -> Result<Self> {
        let signatures = OwnedPreparationValue::try_produce(
            db,
            preparation_array_bytes::<crate::memory_accounting::OwnedMemoryReservation>(1)?,
            || {
                let invoices = Vec::with_capacity(1);
                if invoices.capacity() != 1 {
                    return Err(invalid());
                }
                Ok(invoices)
            },
        )?;
        let OwnedPreparationValue { value, debit } = unsigned;
        Ok(Self {
            value,
            debit,
            signatures,
        })
    }
    fn sign(
        mut self,
        db: &Database,
        signer: &dyn Fn(&[u8]) -> Result<Vec<u8>>,
        bytes: &[u8],
        install: impl FnOnce(&mut T, Vec<u8>),
    ) -> Result<Self> {
        if self.signatures.value.len() == self.signatures.value.capacity() {
            return Err(invalid());
        }
        let signature = sign_preparation_owned(db, signer, bytes)?;
        let OwnedPreparationValue { value, debit } = signature;
        self.signatures.push_slot(debit);
        install(&mut self.value, value);
        Ok(self)
    }
    pub(crate) fn with_value<R>(&self, f: impl for<'v> FnOnce(&'v T) -> R) -> R {
        f(&self.value)
    }
    fn transfer_into(
        self,
        target: &mut OwnedPreparationRecords,
        append: impl FnOnce(T, &mut OwnedPreparationRecords),
    ) {
        assert!(
            1 + self.signatures.value.len() <= target.debits.capacity() - target.debits.len(),
            "preadmitted distinct body and signature slots"
        );
        // Declare all native bodies after their credit owners. No fallible
        // operation follows an invoice move before the associated payload move.
        let Self {
            debit,
            mut signatures,
            value,
        } = self;
        target.debits.push(debit);
        target.debits.extend(signatures.value.drain(..));
        append(value, target);
        drop(signatures);
    }
}
impl OwnedPreparationRecords {
    fn push_body(&mut self, value: OwnedPreparationBody<Record>) {
        assert!(
            self.records.len() < self.records.capacity(),
            "preadmitted record slot"
        );
        value.transfer_into(self, |value, target| target.records.push(value));
    }
}
fn signature_count(signature: &Vec<u8>) -> Result<usize> {
    // This is a COPY plan: the original source keeps its own backing invoice.
    // Spare source capacity does not alter the exact target's byte content.
    if signature.is_empty() {
        return Ok(0);
    }
    if signature.len() != 64 {
        return Err(invalid());
    }
    Ok(1)
}

fn manifest_signature_count(value: &ManifestRecord) -> Result<usize> {
    let mut count = signature_count(&value.signature)?;
    for row in &value.rows {
        count = count
            .checked_add(signature_count(&row.creator.attestation)?)
            .ok_or_else(invalid)?;
    }
    for schema in &value.schemas {
        count = count
            .checked_add(signature_count(&schema.signature)?)
            .ok_or_else(invalid)?;
    }
    if let Some(binding) = &value.binding {
        count = count
            .checked_add(signature_count(&binding.signature)?)
            .ok_or_else(invalid)?;
    }
    Ok(count)
}
fn replace_conflict_reason_owned(
    db: &Database,
    conflict: &mut OwnedPreparationValue<crate::sync_types::Conflict>,
    cause: &str,
) -> Result<()> {
    let old = &conflict.value;
    let bytes = conflict_native_bytes_owned(old)?
        .checked_sub(old.reason.as_ref().map_or(0, String::len))
        .and_then(|n| n.checked_add(cause.len()))
        .ok_or_else(invalid)?;
    // The complete successor is admitted while the complete old body lives.
    // Its reason is constructed once; the displaced reason is never copied.
    let successor = OwnedPreparationValue::try_produce(db, bytes, || {
        Ok(crate::sync_types::Conflict {
            natural_key: copy_key_native(&old.natural_key)?,
            resolution: old.resolution,
            reason: Some(cause.to_owned()),
            table: old.table.clone(),
            mutation_kind: old.mutation_kind.clone(),
            winning_author_node_id: old.winning_author_node_id.clone(),
            hub_acceptance_position: old.hub_acceptance_position,
            refusal_cause: old
                .refusal_cause
                .as_ref()
                .map(|cause| -> Result<crate::sync_types::RefusalCause> {
                    Ok(crate::sync_types::RefusalCause {
                        table: cause.table.clone(),
                        natural_key: copy_key_native(&cause.natural_key)?,
                    })
                })
                .transpose()?,
        })
    })?;
    // Struct field order physically drops every old native body before debit.
    *conflict = successor;
    Ok(())
}

fn record_signature_count(record: &Record) -> Result<usize> {
    match record {
        Record::CheckpointFrontier(f) => signature_count(&f.checkpoint.signature),
        Record::EdgeCheckpoint(c) => signature_count(&c.signature),
        Record::MaterializedOwner(o) => signature_count(&o.source.creator.attestation),
        Record::Binding(b) => signature_count(&b.signature),
        Record::Certificate(c) => signature_count(&c.signature),
        Record::Manifest(m) => manifest_signature_count(m),
        Record::Terminal { record, .. } => signature_count(&record.signature),
        Record::CheckpointNode(_)
        | Record::Policy(_)
        | Record::State(_)
        | Record::Control(_)
        | Record::Birth(_)
        | Record::Destination(_)
        | Record::ObjectAdmission(_)
        | Record::Root { .. }
        | Record::MemberOwner { .. }
        | Record::Diagnostic { .. }
        | Record::DiagnosticOwner { .. }
        | Record::SourceHistory { .. }
        | Record::PolicyProfile { .. }
        | Record::Order { .. } => Ok(0),
    }
}
fn copy_record_body_owned(db: &Database, record: &Record) -> Result<OwnedPreparationBody<Record>> {
    let bytes = match record {
        Record::Manifest(m) => count_manifest_native_finite(m)?
            .checked_add(std::mem::size_of::<ManifestRecord>())
            .ok_or_else(invalid)?,
        _ => count_record_native(record)?,
    };
    OwnedPreparationBody::try_produce(db, bytes, record_signature_count(record)?, 0, || {
        Ok(match record {
            Record::CheckpointFrontier(frontier) => Record::CheckpointFrontier(frontier.clone()),
            Record::CheckpointNode(node) => Record::CheckpointNode(node.clone()),
            Record::EdgeCheckpoint(checkpoint) => Record::EdgeCheckpoint(checkpoint.clone()),
            Record::MaterializedOwner(owner) => Record::MaterializedOwner(MaterializedOwner {
                namespace: owner.namespace.clone(),
                submission: owner.submission,
                source: copy_row_evidence_finite(&owner.source)?,
                incumbent: owner.incumbent.clone(),
                incumbent_reference: copy_reference_finite(&owner.incumbent_reference)?,
                incumbent_row_digest: owner.incumbent_row_digest,
                root_incumbent: owner.root_incumbent.clone(),
            }),
            Record::Policy(policy) => Record::Policy(policy.clone()),
            Record::Binding(binding) => Record::Binding(binding.clone()),
            Record::State(state) => Record::State(state.clone()),
            Record::Control(control) => Record::Control(control.clone()),
            Record::Birth(birth) => Record::Birth(birth.clone()),
            Record::Certificate(certificate) => Record::Certificate(certificate.clone()),
            Record::Destination(authority) => Record::Destination(authority.clone()),
            Record::ObjectAdmission(admission) => Record::ObjectAdmission(admission.clone()),
            Record::Root { life, submission } => Record::Root {
                life: life.clone(),
                submission: *submission,
            },
            Record::Manifest(manifest) => {
                Record::Manifest(Box::new(copy_manifest_finite(manifest)?))
            }
            Record::MemberOwner {
                member,
                root,
                submission,
            } => Record::MemberOwner {
                member: member.clone(),
                root: root.clone(),
                submission: *submission,
            },
            Record::Terminal { edge, record } => Record::Terminal {
                edge: *edge,
                record: copy_terminal_finite(record)?,
            },
            Record::Diagnostic {
                namespace,
                submission,
                block,
            } => Record::Diagnostic {
                namespace: namespace.clone(),
                submission: *submission,
                block: copy_diagnostic_finite(block)?,
            },
            Record::DiagnosticOwner {
                namespace,
                submission,
                life,
            } => Record::DiagnosticOwner {
                namespace: namespace.clone(),
                submission: *submission,
                life: life.clone(),
            },
            Record::SourceHistory {
                root,
                submission,
                seal_digest,
                source,
            } => Record::SourceHistory {
                root: root.clone(),
                submission: *submission,
                seal_digest: *seal_digest,
                source: *source,
            },
            Record::PolicyProfile {
                life,
                submission,
                profile,
            } => Record::PolicyProfile {
                life: life.clone(),
                submission: *submission,
                profile: profile.clone(),
            },
            Record::Order {
                namespace,
                position,
                submission,
            } => Record::Order {
                namespace: namespace.clone(),
                position: *position,
                submission: *submission,
            },
        })
    })
}

// Check the actual newly built signature backings, never reprice them.
trait PreparationSignatureLayout {
    fn checked_signature_layout(&self) -> Result<usize>;
}
fn signature_layout(signature: &Vec<u8>) -> Result<usize> {
    let count = signature_count(signature)?;
    if signature.capacity() != count * 64 {
        return Err(invalid());
    }
    Ok(count)
}
fn manifest_signature_layout(value: &ManifestRecord) -> Result<usize> {
    let mut count = signature_layout(&value.signature)?;
    for row in &value.rows {
        count = count
            .checked_add(signature_layout(&row.creator.attestation)?)
            .ok_or_else(invalid)?;
    }
    for schema in &value.schemas {
        count = count
            .checked_add(signature_layout(&schema.signature)?)
            .ok_or_else(invalid)?;
    }
    if let Some(binding) = &value.binding {
        count = count
            .checked_add(signature_layout(&binding.signature)?)
            .ok_or_else(invalid)?;
    }
    Ok(count)
}
impl PreparationSignatureLayout for SignedBinding {
    fn checked_signature_layout(&self) -> Result<usize> {
        signature_layout(&self.signature)
    }
}
impl PreparationSignatureLayout for RowEvidence {
    fn checked_signature_layout(&self) -> Result<usize> {
        signature_layout(&self.creator.attestation)
    }
}
impl PreparationSignatureLayout for ManifestRecord {
    fn checked_signature_layout(&self) -> Result<usize> {
        manifest_signature_layout(self)
    }
}
impl PreparationSignatureLayout for TerminalInput {
    fn checked_signature_layout(&self) -> Result<usize> {
        manifest_signature_layout(&self.manifest)
    }
}
impl PreparationSignatureLayout
    for Vec<(
        String,
        NaturalKey,
        contextdb_core::Lsn,
        crate::protocol::WireRowLineage,
    )>
{
    fn checked_signature_layout(&self) -> Result<usize> {
        self.iter().try_fold(0usize, |n, (_, _, _, lineage)| {
            n.checked_add(signature_layout(&lineage.attestation)?)
                .ok_or_else(invalid)
        })
    }
}
impl PreparationSignatureLayout for Record {
    fn checked_signature_layout(&self) -> Result<usize> {
        match self {
            Record::CheckpointFrontier(f) => signature_layout(&f.checkpoint.signature),
            Record::EdgeCheckpoint(c) => signature_layout(&c.signature),
            Record::MaterializedOwner(o) => signature_layout(&o.source.creator.attestation),
            Record::Binding(b) => signature_layout(&b.signature),
            Record::Certificate(c) => signature_layout(&c.signature),
            Record::Manifest(m) => manifest_signature_layout(m),
            Record::Terminal { record, .. } => signature_layout(&record.signature),
            Record::CheckpointNode(_)
            | Record::Policy(_)
            | Record::State(_)
            | Record::Control(_)
            | Record::Birth(_)
            | Record::Destination(_)
            | Record::ObjectAdmission(_)
            | Record::Root { .. }
            | Record::MemberOwner { .. }
            | Record::Diagnostic { .. }
            | Record::DiagnosticOwner { .. }
            | Record::SourceHistory { .. }
            | Record::PolicyProfile { .. }
            | Record::Order { .. } => Ok(0),
        }
    }
}
