//! A row's latest physical identity is maintained beside logical publication.
use super::relational_version_manifest::position_tree::{NO_POSITION, PositionTree, Vacancy};
use super::*;

#[cfg(test)]
#[path = "held_relations_tests.rs"]
mod held_relations_tests;
const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("writer_row_heads");
const ROOT_KEY: &str = "row_heads_root";
fn damage(reason: &str) -> Error {
    Error::StoreCorrupted {
        path: "writer_row_heads".into(),
        reason: reason.into(),
    }
}
fn decode(key: &[u8], value: &[u8]) -> Result<[u8; 24]> {
    if value.len() != 56 {
        return Err(damage("invalid row head envelope"));
    }
    let mut hash = blake3::Hasher::new();
    hash.update(key);
    hash.update(&value[..24]);
    if hash.finalize().as_bytes().as_slice() != &value[24..] {
        return Err(damage("row head checksum or identity mismatch"));
    }
    Ok(value[..24].try_into().unwrap())
}
fn route(key: &[u8]) -> Result<(&str, RowId)> {
    let length = key
        .get(..8)
        .ok_or_else(|| damage("truncated row head route"))?;
    let length = usize::try_from(u64::from_le_bytes(length.try_into().unwrap()))
        .map_err(|_| damage("row head route length overflow"))?;
    let end = 8usize
        .checked_add(length)
        .ok_or_else(|| damage("row head route length overflow"))?;
    if key.len()
        != end
            .checked_add(16)
            .ok_or_else(|| damage("row head route length overflow"))?
        || key[end..end + 8] != 8u64.to_le_bytes()
    {
        return Err(damage("invalid row head route"));
    }
    let table =
        std::str::from_utf8(&key[8..end]).map_err(|_| damage("row head table is not UTF-8"))?;
    let row = RowId(u64::from_le_bytes(key[end + 8..].try_into().unwrap()));
    Ok((table, row))
}
/// The writer admission a row-head reconciliation pass charges what it holds
/// to: the general writer's or the owned transaction host's.
pub(super) trait RowHeadAdmission {
    fn admit_held_relations(&mut self, bytes: usize) -> Result<()>;
    fn release_held_relations(&mut self, bytes: usize);
}
const HOLD_RELATIONS: &str = "hold_row_head_relations";
const HOLD_RELATIONS_HINT: &str =
    "Raise MEMORY_LIMIT to admit the tables this transaction's rows are written to.";
impl RowHeadAdmission for content_identity::ProofAdmission {
    fn admit_held_relations(&mut self, bytes: usize) -> Result<()> {
        self.admit_for(bytes, HOLD_RELATIONS, HOLD_RELATIONS_HINT)
    }
    fn release_held_relations(&mut self, bytes: usize) {
        self.release(bytes);
    }
}
impl RowHeadAdmission for content_identity::original_writer::ProofAdmission {
    fn admit_held_relations(&mut self, bytes: usize) -> Result<()> {
        self.admit_for(bytes, HOLD_RELATIONS, HOLD_RELATIONS_HINT)
    }
    fn release_held_relations(&mut self, bytes: usize) {
        self.release(bytes);
    }
}

type HeldRelation<'txn> = redb::Table<'txn, &'static [u8], &'static [u8]>;
/// One table the pass has used: its open relation, or none when its family
/// does not exist.
type Slot<'name, 'txn> = (&'name str, Option<HeldRelation<'txn>>);

/// Tables other than held relations a pass can have open at once while it
/// holds them: its heads table, the content-size totals table, and the history
/// witnesses the root hook opens around each relation's open and close.
const OTHER_OPENS: usize = 3;
const RELATION_PREFIX: &str = "rel_";

/// What a pass has admitted, split into reservations whose memory does not
/// exist yet (provisional) and memory that does (installed). Each field is set
/// right after its admission succeeds and moved to installed right after the
/// allocation it funds exists, so a failure or unwind at any point leaves the
/// ledger matching what is allocated. Every field is released only after the
/// memory it funds is freed (or, for a provisional one, never existed).
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct HoldLedger {
    /// Provisional: the storage name being built for a first use. It is a
    /// local of that lookup, freed before the holder drops.
    pub(super) temp_name: usize,
    /// Provisional, then installed: the open handle's name and the registry
    /// key redb copies from the storage name.
    pub(super) names_pending: usize,
    pub(super) names: usize,
    /// Provisional, then installed: the registry array a growth allocates.
    pub(super) registry_pending: usize,
    pub(super) registry: usize,
    /// Provisional, then installed: the empty baseline-sized registry array
    /// prepared before the first growth, which cleanup moves the baseline
    /// into without allocating.
    pub(super) scratch_pending: usize,
    pub(super) scratch: usize,
    /// Provisional, then installed: the slot and index node vectors.
    pub(super) slots_pending: usize,
    pub(super) slots: usize,
    pub(super) nodes_pending: usize,
    pub(super) nodes: usize,
}

/// The relations one row-head reconciliation pass holds open, each opened
/// once on first use however the rows of different tables interleave. Every
/// open and close of a protected relation makes the write transaction finalize
/// the checksums of every page it has dirtied there, so reopening one per row
/// (or per change of table) costs the rows already written.
///
/// Slots are appended in first-use order and found through a balanced index
/// of their positions keyed by table name: a lookup and a first use each
/// descend once, and no held handle is shifted by a later insertion (a slot
/// moves only when its vector doubles).
///
/// Everything holding them allocates is admitted to the writer's budget before
/// it is allocated: the storage name, redb's two copies of it (the handle's
/// and the registry key), the slot and node vectors, and the registry array
/// growth the held relations cause, learned exactly from redb before redb
/// allocates it. Before the first growth an empty registry array sized for the
/// tables open when the pass started is allocated, so dropping the holder puts
/// those tables back into it and frees the grown array without allocating or
/// admitting anything. That replacement array then stays with the transaction
/// as the pre-existing registry did: it returns to the pre-existing baseline
/// allowance this pass did not fund (owned by the writer's funding, not here).
pub(super) struct HeldRelations<'name, 'txn, 'a, A: RowHeadAdmission> {
    write: &'txn redb::WriteTransaction,
    admission: &'a mut A,
    slots: Vec<Slot<'name, 'txn>>,
    index: PositionTree,
    ledger: HoldLedger,
    baseline_len: usize,
    replacement: Option<redb::RegistryReplacement>,
    /// Counts registry allocations outside a funded reserve for the whole
    /// pass; dropped last.
    monitor: redb::RegistryMonitor,
}

impl<'name, 'txn, 'a, A: RowHeadAdmission> HeldRelations<'name, 'txn, 'a, A> {
    /// A holder for one pass. It first makes registry room for the pass's
    /// own other tables, so opening them never grows the registry unfunded.
    pub(super) fn new(write: &'txn redb::WriteTransaction, admission: &'a mut A) -> Result<Self> {
        #[cfg(any(test, feature = "test-seams"))]
        content_identity::version_manifest::commit_row_work::note_held_pass();
        let mut held = Self {
            write,
            admission,
            slots: Vec::new(),
            index: PositionTree::new(),
            ledger: HoldLedger::default(),
            baseline_len: write.open_table_registry_len(),
            replacement: None,
            monitor: write.open_table_registry_monitor(),
        };
        held.reserve_registry(OTHER_OPENS)?;
        Ok(held)
    }

    /// The open relation of `table`, or `None` when its family does not
    /// exist, opened on the table's first use in this pass.
    pub(super) fn get(&mut self, table: &'name str) -> Result<Option<&HeldRelation<'txn>>> {
        let found = self.index.search(|position| {
            #[cfg(any(test, feature = "test-seams"))]
            content_identity::version_manifest::commit_row_work::count_held_relation_work(1);
            table.cmp(self.slots[position as usize].0)
        });
        let position = match found {
            Ok(position) => position as usize,
            Err(vacancy) => {
                #[cfg(any(test, feature = "test-seams"))]
                content_identity::version_manifest::commit_row_work::note_held_first_use(table);
                self.first_use(table, vacancy)?
            }
        };
        Ok(self.slots[position].1.as_ref())
    }

    /// What this pass has admitted and still holds.
    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn ledger(&self) -> HoldLedger {
        self.ledger
    }

    fn admit(&mut self, bytes: usize) -> Result<()> {
        self.admission.admit_held_relations(bytes)
    }

    fn release(&mut self, field: fn(&mut HoldLedger) -> &mut usize) {
        let bytes = std::mem::take(field(&mut self.ledger));
        if bytes > 0 {
            self.admission.release_held_relations(bytes);
        }
    }

    fn first_use(&mut self, table: &'name str, vacancy: Vacancy) -> Result<usize> {
        hold_fault(HoldStep::AdmitName)?;
        let name_len = RELATION_PREFIX
            .len()
            .checked_add(table.len())
            .ok_or_else(|| Error::Other("held relation name overflow".into()))?;
        self.admit(name_len)?;
        self.ledger.temp_name = name_len;
        hold_fault(HoldStep::BuildName)?;
        let mut name = String::with_capacity(name_len);
        name.push_str(RELATION_PREFIX);
        name.push_str(table);
        debug_assert_eq!(name, RedbPersistence::rel_table_name(table));
        let used = self.first_use_named(table, &name, vacancy);
        drop(name);
        self.release(|ledger| &mut ledger.temp_name);
        used
    }

    fn first_use_named(
        &mut self,
        table: &'name str,
        name: &str,
        vacancy: Vacancy,
    ) -> Result<usize> {
        let definition = TableDefinition::<&[u8], &[u8]>::new(name);
        let exists = self
            .write
            .table_exists(definition)
            .map_err(RedbPersistence::storage_error)?;
        let position = u32::try_from(self.slots.len())
            .ok()
            .filter(|position| *position != NO_POSITION)
            .ok_or_else(|| Error::Other("held relation position overflow".into()))?;
        if exists {
            self.reserve_registry(1 + OTHER_OPENS)?;
        }
        self.reserve_slot()?;
        self.reserve_node()?;
        let relation = if exists {
            let names = name
                .len()
                .checked_mul(2)
                .ok_or_else(|| Error::Other("held relation name overflow".into()))?;
            hold_fault(HoldStep::AdmitNames)?;
            self.admit(names)?;
            self.ledger.names_pending = names;
            hold_fault(HoldStep::Open)?;
            match self.write.open_table(definition) {
                Ok(relation) => {
                    self.ledger.names_pending = 0;
                    self.ledger.names += names;
                    Some(relation)
                }
                Err(error) => {
                    // redb registers nothing before a failed open returns.
                    self.release(|ledger| &mut ledger.names_pending);
                    return Err(RedbPersistence::storage_error(error));
                }
            }
        } else {
            None
        };
        hold_fault(HoldStep::Install)?;
        self.slots.push((table, relation));
        let steps = self.index.insert_at(vacancy, position);
        #[cfg(any(test, feature = "test-seams"))]
        content_identity::version_manifest::commit_row_work::count_held_relation_work(steps);
        #[cfg(not(any(test, feature = "test-seams")))]
        let _ = steps;
        Ok(position as usize)
    }

    /// Make registry room for `additional` more open tables, funding any
    /// growth it needs before redb allocates it.
    fn reserve_registry(&mut self, additional: usize) -> Result<()> {
        hold_fault(HoldStep::ProbeRegistry)?;
        let Some(growth) = self
            .write
            .open_table_registry_cost_to_reserve(additional)
            .map_err(registry_error)?
        else {
            return Ok(());
        };
        let scratch = if self.ledger.registry == 0 && self.replacement.is_none() {
            self.write
                .open_table_registry_cost_to_hold(self.baseline_len)
                .map_err(registry_error)?
        } else {
            None
        };
        hold_fault(HoldStep::AdmitRegistry)?;
        self.admit(growth.size())?;
        self.ledger.registry_pending = growth.size();
        if let Some(scratch) = scratch {
            hold_fault(HoldStep::AdmitScratch)?;
            self.admit(scratch.size())?;
            self.ledger.scratch_pending = scratch.size();
            hold_fault(HoldStep::PrepareScratch)?;
            let replacement = self
                .write
                .open_table_registry_prepare(self.baseline_len, scratch.size())
                .map_err(registry_error)?;
            debug_assert_eq!(replacement.allocation_bytes(), scratch.size());
            self.replacement = Some(replacement);
            self.ledger.scratch_pending = 0;
            self.ledger.scratch = scratch.size();
            hold_fault(HoldStep::AfterScratch)?;
        }
        hold_fault(HoldStep::Reserve)?;
        let freed = self
            .write
            .open_table_registry_reserve(additional, growth.size())
            .map_err(registry_error)?;
        self.ledger.registry_pending = 0;
        let funded = std::mem::replace(&mut self.ledger.registry, growth.size());
        // The replaced array is freed now. Release it only if this pass
        // funded it; the pass-start array was never charged here.
        if funded > 0 {
            debug_assert_eq!(freed, funded);
            self.admission.release_held_relations(funded);
        }
        hold_fault(HoldStep::AfterReserve)?;
        Ok(())
    }

    fn reserve_slot(&mut self) -> Result<()> {
        if self.slots.len() < self.slots.capacity() {
            return Ok(());
        }
        let entry = std::mem::size_of::<Slot<'name, 'txn>>();
        let (capacity, bytes) = doubled(self.slots.capacity(), entry)?;
        hold_fault(HoldStep::AdmitSlots)?;
        self.admit(bytes)?;
        self.ledger.slots_pending = bytes;
        hold_fault(HoldStep::GrowSlots)?;
        let mut next = Vec::with_capacity(capacity);
        #[cfg(any(test, feature = "test-seams"))]
        content_identity::version_manifest::commit_row_work::count_held_relation_work(
            self.slots.len(),
        );
        next.append(&mut self.slots);
        drop(std::mem::replace(&mut self.slots, next));
        self.ledger.slots_pending = 0;
        let freed = std::mem::replace(&mut self.ledger.slots, bytes);
        if freed > 0 {
            self.admission.release_held_relations(freed);
        }
        Ok(())
    }

    fn reserve_node(&mut self) -> Result<()> {
        if self.index.len() < self.index.capacity() {
            return Ok(());
        }
        let (capacity, bytes) = doubled(self.index.capacity(), PositionTree::NODE_BYTES)?;
        hold_fault(HoldStep::AdmitNodes)?;
        self.admit(bytes)?;
        self.ledger.nodes_pending = bytes;
        hold_fault(HoldStep::GrowNodes)?;
        let moved = self.index.grow(capacity);
        #[cfg(any(test, feature = "test-seams"))]
        content_identity::version_manifest::commit_row_work::count_held_relation_work(moved);
        #[cfg(not(any(test, feature = "test-seams")))]
        let _ = moved;
        self.ledger.nodes_pending = 0;
        let freed = std::mem::replace(&mut self.ledger.nodes, bytes);
        if freed > 0 {
            self.admission.release_held_relations(freed);
        }
        Ok(())
    }
}

impl<A: RowHeadAdmission> Drop for HeldRelations<'_, '_, '_, A> {
    fn drop(&mut self) {
        // Close every held relation: redb frees each handle's name and its
        // registry key. A name admission whose open never returned funds
        // nothing.
        self.slots.clear();
        self.release(|ledger| &mut ledger.names);
        self.release(|ledger| &mut ledger.names_pending);
        // Put the tables open when the pass started back into the prepared
        // array, freeing the array this pass grew, without allocating.
        if self.ledger.registry > 0 {
            match self
                .write
                .open_table_registry_install(self.replacement.take())
            {
                Ok(freed) => {
                    debug_assert_eq!(freed, self.ledger.registry);
                    self.release(|ledger| &mut ledger.registry);
                    // The prepared array now holds those tables and stays with
                    // the transaction: the pre-existing baseline allowance.
                    self.release(|ledger| &mut ledger.scratch);
                }
                Err((_, replacement)) => {
                    // The grown array is still allocated, so its charge is
                    // kept: the caller's admission returns it when it ends.
                    self.replacement = replacement;
                    self.ledger.registry = 0;
                }
            }
        }
        if let Some(replacement) = self.replacement.take() {
            drop(replacement);
            self.release(|ledger| &mut ledger.scratch);
        }
        self.release(|ledger| &mut ledger.scratch_pending);
        self.release(|ledger| &mut ledger.registry_pending);
        self.slots = Vec::new();
        self.release(|ledger| &mut ledger.slots);
        self.release(|ledger| &mut ledger.slots_pending);
        self.index.clear();
        self.release(|ledger| &mut ledger.nodes);
        self.release(|ledger| &mut ledger.nodes_pending);
        // The storage name was a local of the lookup that built it.
        self.release(|ledger| &mut ledger.temp_name);
        #[cfg(any(test, feature = "test-seams"))]
        HELD_OVERDRAFT.with(|total| total.set(total.get() + self.monitor.overdraft_bytes()));
    }
}

/// The doubled capacity of a vector of `entry`-byte elements and its bytes.
fn doubled(capacity: usize, entry: usize) -> Result<(usize, usize)> {
    let capacity = capacity
        .checked_mul(2)
        .map(|n| n.max(4))
        .ok_or_else(|| Error::Other("held relation capacity overflow".into()))?;
    let bytes = capacity
        .checked_mul(entry)
        .ok_or_else(|| Error::Other("held relation layout overflow".into()))?;
    Ok((capacity, bytes))
}

fn registry_error(error: redb::RegistryReserveError) -> Error {
    match error {
        redb::RegistryReserveError::AllocError(layout) => Error::Other(format!(
            "open-table registry allocation of {} bytes failed",
            layout.size()
        )),
        other => Error::Other(format!("open-table registry reserve failed: {other:?}")),
    }
}

/// The points between a first use's admissions and allocations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HoldStep {
    AdmitName,
    BuildName,
    ProbeRegistry,
    AdmitRegistry,
    AdmitScratch,
    PrepareScratch,
    AfterScratch,
    Reserve,
    AfterReserve,
    AdmitSlots,
    GrowSlots,
    AdmitNodes,
    GrowNodes,
    AdmitNames,
    Open,
    Install,
}

/// How an injected fault surfaces.
#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HoldFault {
    Error,
    Panic,
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    /// The step, how to fail there, and how many earlier arrivals at that
    /// step to let through first.
    static HOLD_FAULT: std::cell::Cell<Option<(HoldStep, HoldFault, usize)>> =
        const { std::cell::Cell::new(None) };
    static HELD_OVERDRAFT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Fail the `skip + 1`-th arrival at `step` on this thread.
#[cfg(any(test, feature = "test-seams"))]
pub(super) fn inject_hold_fault(step: HoldStep, fault: HoldFault, skip: usize) {
    HOLD_FAULT.with(|slot| slot.set(Some((step, fault, skip))));
}

/// Disarm the injected fault; true when it was still waiting to fire.
#[cfg(any(test, feature = "test-seams"))]
pub(super) fn clear_hold_fault() -> bool {
    HOLD_FAULT.with(|slot| slot.take().is_some())
}

/// Registry bytes holders allocated outside a funded reserve on this thread
/// since the last call.
#[cfg(any(test, feature = "test-seams"))]
pub(super) fn take_held_overdraft() -> usize {
    HELD_OVERDRAFT.with(|total| total.replace(0))
}

fn hold_fault(step: HoldStep) -> Result<()> {
    #[cfg(any(test, feature = "test-seams"))]
    if let Some((armed, fault, skip)) = HOLD_FAULT.with(std::cell::Cell::get)
        && armed == step
    {
        if skip > 0 {
            HOLD_FAULT.with(|slot| slot.set(Some((armed, fault, skip - 1))));
        } else {
            HOLD_FAULT.with(|slot| slot.set(None));
            match fault {
                HoldFault::Error => {
                    return Err(Error::Other(format!(
                        "injected held-relation fault at {step:?}"
                    )));
                }
                HoldFault::Panic => panic!("injected held-relation fault at {step:?}"),
            }
        }
    }
    #[cfg(not(any(test, feature = "test-seams")))]
    let _ = step;
    Ok(())
}

/// Look up `order`'s tables through one pass's holder, handing each lookup's
/// handle to `check`; returns how many relations the pass opened.
#[cfg(test)]
pub(super) fn hold_in_order_for_test(
    write: &redb::WriteTransaction,
    order: &[String],
    admission: &mut content_identity::ProofAdmission,
    mut check: impl FnMut(&str, &HeldRelation<'_>),
) -> Result<usize> {
    let mut held = HeldRelations::new(write, admission)?;
    for table in order {
        let relation = held.get(table)?.expect("the fixture's relations exist");
        check(table, relation);
    }
    Ok(held.slots.len())
}

fn logical_key(table: &str, row: RowId) -> Vec<u8> {
    let mut key = Vec::with_capacity(table.len() + 24);
    key.extend_from_slice(&(table.len() as u64).to_le_bytes());
    key.extend_from_slice(table.as_bytes());
    key.extend_from_slice(&8u64.to_le_bytes());
    key.extend_from_slice(&row.0.to_le_bytes());
    key
}
fn rank(key: [u8; 24]) -> (u64, u64) {
    (
        u64::from_be_bytes(key[16..].try_into().unwrap()),
        u64::from_be_bytes(key[8..16].try_into().unwrap()),
    )
}

impl RedbPersistence {
    pub(super) fn apply_writer_row_heads_in_write(
        write: &redb::WriteTransaction,
        mutations: &[content_identity::ContentIdentityMutation],
        admission: &mut impl RowHeadAdmission,
    ) -> Result<()> {
        // Only the heads table is written here; the held relations are read.
        // Dropping the holder closes them and returns what holding them was
        // admitted, on success and failure.
        let mut relations = HeldRelations::new(write, admission)?;
        Self::apply_writer_row_heads_holding(write, mutations, &mut relations)
    }

    fn apply_writer_row_heads_holding<'name, 'txn, A: RowHeadAdmission>(
        write: &'txn redb::WriteTransaction,
        mutations: &'name [content_identity::ContentIdentityMutation],
        relations: &mut HeldRelations<'name, 'txn, '_, A>,
    ) -> Result<()> {
        let mut heads = write.open_table(TABLE).map_err(Self::storage_error)?;
        for mutation in mutations {
            let Some(projection) = mutation.row_head else {
                continue;
            };
            let (table, row) = route(&mutation.logical_key)?;
            let Some(relation) = relations.get(table)? else {
                if projection.is_some() {
                    return Err(damage(
                        "row head publication names an absent physical family",
                    ));
                }
                heads
                    .remove(mutation.logical_key.as_slice())
                    .map_err(Self::storage_error)?;
                continue;
            };
            let old = heads
                .get(mutation.logical_key.as_slice())
                .map_err(Self::storage_error)?
                .map(|value| decode(&mutation.logical_key, value.value()))
                .transpose()?;
            let old = match old {
                Some(key)
                    if relation
                        .get(key.as_slice())
                        .map_err(Self::storage_error)?
                        .is_some() =>
                {
                    Some(key)
                }
                _ => None,
            };
            let next = match (old, projection) {
                (Some(old), Some(next)) if rank(old) > rank(next) => Some(old),
                (_, Some(next)) => Some(next),
                (old, None) => old,
            };
            if let Some(next) = next {
                if next[..8] != row.0.to_be_bytes()
                    || relation
                        .get(next.as_slice())
                        .map_err(Self::storage_error)?
                        .is_none()
                {
                    return Err(damage(
                        "row head names a missing or foreign physical identity",
                    ));
                }
                let mut encoded = [0; 56];
                encoded[..24].copy_from_slice(&next);
                let mut hash = blake3::Hasher::new();
                hash.update(&mutation.logical_key);
                hash.update(&next);
                encoded[24..].copy_from_slice(hash.finalize().as_bytes());
                heads
                    .insert(mutation.logical_key.as_slice(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
            } else {
                heads
                    .remove(mutation.logical_key.as_slice())
                    .map_err(Self::storage_error)?;
            }
        }
        Ok(())
    }

    pub(super) fn write_row_heads_root(write: &redb::WriteTransaction) -> Result<()> {
        {
            write.open_table(TABLE).map_err(Self::storage_error)?;
        }
        let bytes = write
            .table_root_fingerprint(TABLE)
            .map_err(Self::storage_error)?
            .unwrap_or([0; 32]);
        let mut metadata = write
            .open_table(writer_recovery::RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        if metadata
            .get(ROOT_KEY)
            .map_err(Self::storage_error)?
            .is_none_or(|old| old.value() != bytes)
        {
            metadata
                .insert(ROOT_KEY, bytes.as_slice())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }

    pub(crate) fn writer_row_head(
        root: &WriterSnapshot,
        table: &str,
        row: RowId,
    ) -> Result<[u8; 24]> {
        let read = root.owner_read()?;
        Self::verify_history_root(&read, &Self::rel_table_name(table))?;
        let heads = read.open_table(TABLE).map_err(Self::storage_error)?;
        let fingerprint = read
            .table_root_fingerprint(TABLE)
            .map_err(Self::storage_error)?
            .unwrap_or([0; 32]);
        let metadata = read
            .open_table(writer_recovery::RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let expected = metadata
            .get(ROOT_KEY)
            .map_err(Self::storage_error)?
            .ok_or_else(|| damage("missing row head root binding"))?;
        if expected.value() != fingerprint {
            return Err(damage("row head root binding mismatch"));
        }
        let key = logical_key(table, row);
        let value = heads
            .get(key.as_slice())
            .map_err(Self::storage_error)?
            .ok_or_else(|| damage("missing latest physical row identity"))?;
        let head = decode(&key, value.value())?;
        if head[..8] != row.0.to_be_bytes() {
            return Err(damage("row head belongs to another row"));
        }
        let name = Self::rel_table_name(table);
        let relation: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name.as_str());
        // The head binding is metadata; validating its physical body reads
        // relational pages and belongs to the row family's backend work.
        #[cfg(feature = "test-seams")]
        root.note_read_source(crate::writer_query_test_seams::PhysicalFamily::Row);
        #[cfg(feature = "test-seams")]
        let _body_reads = crate::writer_query_test_seams::physical_operation(
            crate::writer_query_test_seams::PhysicalFamily::Row,
            table,
        );
        if read
            .open_table(relation)
            .map_err(Self::storage_error)?
            .get(head.as_slice())
            .map_err(Self::storage_error)?
            .is_none()
        {
            return Err(damage("latest row head has no physical body"));
        }
        Ok(head)
    }
}
