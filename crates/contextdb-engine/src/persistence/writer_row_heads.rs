//! A row's latest physical identity is maintained beside logical publication.
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

/// Bytes redb retains for one held relation beyond its slot here: the open
/// handle's copy of the storage name, and the transaction's open-table entry
/// (another copy of the name beside a caller location) in a hash table that
/// can be half empty after it grows.
pub(super) fn held_relation_native_bytes(relation_name: &str) -> usize {
    2 * relation_name.len()
        + 2 * std::mem::size_of::<(String, &'static std::panic::Location<'static>)>()
}

/// The relations one row-head reconciliation pass holds open, each opened once
/// on first use however the rows of different tables interleave. Every open
/// and close of a protected relation makes the write transaction finalize the
/// checksums of every page it has dirtied there, so reopening one per row (or
/// per change of table) costs the rows already written.
///
/// What holding them retains grows with the distinct tables the transaction
/// touches, so it is admitted before it is allocated: the sorted slot vector
/// (old and new capacity while it grows) and each relation's native retention.
/// [`Self::close`] drops every handle and then returns the whole charge; a pass
/// that fails part-way closes them the same way.
pub(super) struct HeldRelations<'name, 'txn> {
    open: Vec<(&'name str, HeldRelation<'txn>)>,
    charged: usize,
}
impl<'name, 'txn> HeldRelations<'name, 'txn> {
    pub(super) fn new() -> Self {
        Self {
            open: Vec::new(),
            charged: 0,
        }
    }

    /// The relation of `table`, stored under the name `relation_name` gives on
    /// its first use. A refusal leaves that relation unopened.
    pub(super) fn get(
        &mut self,
        write: &'txn redb::WriteTransaction,
        table: &'name str,
        relation_name: impl FnOnce() -> String,
        admission: &mut impl RowHeadAdmission,
    ) -> Result<&HeldRelation<'txn>> {
        let slot = match self.open.binary_search_by(|(open, _)| (*open).cmp(table)) {
            Ok(slot) => slot,
            Err(slot) => {
                let relation_name = relation_name();
                let native = held_relation_native_bytes(&relation_name);
                admission.admit_held_relations(native)?;
                self.charged += native;
                if self.open.len() == self.open.capacity() {
                    let entry = std::mem::size_of::<(&str, HeldRelation<'txn>)>();
                    let old = self.open.capacity();
                    let capacity = old
                        .checked_mul(2)
                        .map(|n| n.max(4))
                        .ok_or_else(|| Error::Other("held relation capacity overflow".into()))?;
                    let bytes = capacity
                        .checked_mul(entry)
                        .ok_or_else(|| Error::Other("held relation layout overflow".into()))?;
                    admission.admit_held_relations(bytes)?;
                    self.charged += bytes;
                    let mut next = Vec::with_capacity(capacity);
                    next.append(&mut self.open);
                    drop(std::mem::replace(&mut self.open, next));
                    admission.release_held_relations(old * entry);
                    self.charged -= old * entry;
                }
                let relation = write
                    .open_table(TableDefinition::<&[u8], &[u8]>::new(&relation_name))
                    .map_err(RedbPersistence::storage_error)?;
                self.open.insert(slot, (table, relation));
                slot
            }
        };
        Ok(&self.open[slot].1)
    }

    /// Bytes admitted for what is held now.
    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn charged(&self) -> usize {
        self.charged
    }

    /// Close every held relation, then return what holding them was admitted.
    pub(super) fn close(self, admission: &mut impl RowHeadAdmission) {
        let Self { open, charged } = self;
        drop(open);
        admission.release_held_relations(charged);
    }
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
        let mut relations = HeldRelations::new();
        let applied = Self::apply_writer_row_heads_holding(write, mutations, admission, &mut relations);
        relations.close(admission);
        applied
    }

    fn apply_writer_row_heads_holding<'name, 'txn>(
        write: &'txn redb::WriteTransaction,
        mutations: &'name [content_identity::ContentIdentityMutation],
        admission: &mut impl RowHeadAdmission,
        relations: &mut HeldRelations<'name, 'txn>,
    ) -> Result<()> {
        let mut heads = write.open_table(TABLE).map_err(Self::storage_error)?;
        let mut family_exists = std::collections::HashMap::new();
        for mutation in mutations {
            let Some(projection) = mutation.row_head else {
                continue;
            };
            let (table, row) = route(&mutation.logical_key)?;
            let name = Self::rel_table_name(table);
            let relation: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name.as_str());
            let exists = match family_exists.get(table) {
                Some(exists) => *exists,
                None => {
                    let exists = write.table_exists(relation).map_err(Self::storage_error)?;
                    family_exists.insert(table, exists);
                    exists
                }
            };
            if !exists {
                if projection.is_some() {
                    return Err(damage(
                        "row head publication names an absent physical family",
                    ));
                }
                heads
                    .remove(mutation.logical_key.as_slice())
                    .map_err(Self::storage_error)?;
                continue;
            }
            let relation = relations.get(write, table, || name.clone(), admission)?;
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
