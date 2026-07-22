use contextdb_core::{
    DirectedValue, IndexKey, Lsn, RowId, SortDirection, TableMeta, TableName, TotalOrdAsc,
    TotalOrdDesc, TxId, Value, VersionedRow,
};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap, HashSet, hash_map::Entry};
use std::hash::BuildHasherDefault;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub row_id: RowId,
    pub created_tx: TxId,
    pub deleted_tx: Option<TxId>,
}

impl IndexEntry {
    pub fn visible_at(&self, snapshot: contextdb_core::SnapshotId) -> bool {
        self.created_tx.0 <= snapshot.0 && self.deleted_tx.is_none_or(|tx| tx.0 > snapshot.0)
    }
}

#[derive(Debug, Clone)]
struct ExactBucket {
    key: IndexKey,
    postings: Vec<IndexEntry>,
}

type ExactMap = HashMap<u64, Vec<ExactBucket>, BuildHasherDefault<IdentityHasher>>;

const EXACT_FILTER_WORDS: usize = 8192;
const EXACT_FILTER_BITS: u64 = (EXACT_FILTER_WORDS as u64) * 64;
const EXACT_FILTER_MASK: u64 = EXACT_FILTER_BITS - 1;

#[derive(Default)]
struct IndexKeyHasher {
    state: u64,
}

impl IndexKeyHasher {
    fn mix(&mut self, value: u64) {
        let mixed = value
            .wrapping_add(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(self.state << 6)
            .wrapping_add(self.state >> 2);
        self.state ^= mixed;
    }
}

impl Hasher for IndexKeyHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            self.mix(u64::from_ne_bytes(
                chunk.try_into().expect("chunk size checked"),
            ));
        }
        let remainder = chunks.remainder();
        if !remainder.is_empty() {
            let mut tail = [0_u8; 8];
            tail[..remainder.len()].copy_from_slice(remainder);
            self.mix(u64::from_ne_bytes(tail));
        }
    }

    fn write_u8(&mut self, i: u8) {
        self.mix(i as u64);
    }

    fn write_u64(&mut self, i: u64) {
        self.mix(i);
    }

    fn write_i64(&mut self, i: i64) {
        self.mix(i as u64);
    }
}

#[derive(Default)]
struct IdentityHasher {
    state: u64,
}

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut padded = [0_u8; 8];
        let len = bytes.len().min(8);
        padded[..len].copy_from_slice(&bytes[..len]);
        self.state = u64::from_ne_bytes(padded);
    }

    fn write_u64(&mut self, i: u64) {
        self.state = i;
    }
}

fn hash_index_key(key: &IndexKey) -> Option<u64> {
    let mut hasher = IndexKeyHasher::default();
    for value in key {
        match value {
            DirectedValue::Asc(value) => {
                0_u8.hash(&mut hasher);
                hash_value(&value.0, &mut hasher)?;
            }
            DirectedValue::Desc(value) => {
                1_u8.hash(&mut hasher);
                hash_value(&value.0, &mut hasher)?;
            }
        }
    }
    Some(hasher.finish())
}

fn hash_value(value: &Value, hasher: &mut impl Hasher) -> Option<()> {
    match value {
        Value::Null => 0_u8.hash(hasher),
        Value::Bool(value) => {
            1_u8.hash(hasher);
            value.hash(hasher);
        }
        Value::Int64(value) => {
            2_u8.hash(hasher);
            value.hash(hasher);
        }
        Value::Float64(value) => {
            3_u8.hash(hasher);
            value.to_bits().hash(hasher);
        }
        Value::Text(value) => {
            4_u8.hash(hasher);
            value.hash(hasher);
        }
        Value::Uuid(value) => {
            5_u8.hash(hasher);
            value.hash(hasher);
        }
        Value::Timestamp(value) => {
            6_u8.hash(hasher);
            value.hash(hasher);
        }
        Value::TxId(value) => {
            7_u8.hash(hasher);
            value.0.hash(hasher);
        }
        Value::Json(_) | Value::Vector(_) => return None,
    }
    Some(())
}

/// Storage for one declared index. User-declared indexes keep the ordered
/// BTree; auto constraint indexes may be exact-only when callers only need
/// full-key probes.
#[derive(Debug)]
pub struct IndexStorage {
    pub columns: Vec<(String, SortDirection)>,
    /// Non-unique keys map to Vec<IndexEntry>; tie-break by row_id ascending
    /// is maintained at insert time (I18).
    pub tree: BTreeMap<IndexKey, Vec<IndexEntry>>,
    exact: ExactMap,
    exact_filter: Box<[u64; EXACT_FILTER_WORDS]>,
    exact_only: bool,
}

impl Default for IndexStorage {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl IndexStorage {
    pub fn new(columns: Vec<(String, SortDirection)>) -> Self {
        Self::new_with_exact_only(columns, false)
    }

    pub fn new_exact_only(columns: Vec<(String, SortDirection)>) -> Self {
        Self::new_with_exact_only(columns, true)
    }

    fn new_with_exact_only(columns: Vec<(String, SortDirection)>, exact_only: bool) -> Self {
        Self {
            columns,
            tree: BTreeMap::new(),
            exact: ExactMap::default(),
            exact_filter: Box::new([0; EXACT_FILTER_WORDS]),
            exact_only,
        }
    }

    /// Number of live (non-tombstoned) postings, useful for leak detection.
    pub fn total_entries(&self) -> u64 {
        if self.exact_only {
            self.exact
                .values()
                .flat_map(|buckets| buckets.iter())
                .map(|bucket| {
                    bucket
                        .postings
                        .iter()
                        .filter(|e| e.deleted_tx.is_none())
                        .count() as u64
                })
                .sum()
        } else {
            self.tree
                .values()
                .map(|v| v.iter().filter(|e| e.deleted_tx.is_none()).count() as u64)
                .sum()
        }
    }

    /// Total postings including tombstones (for DROP TABLE leak detection).
    pub fn total_entries_including_tombstones(&self) -> u64 {
        if self.exact_only {
            self.exact
                .values()
                .flat_map(|buckets| buckets.iter())
                .map(|bucket| bucket.postings.len() as u64)
                .sum()
        } else {
            self.tree.values().map(|v| v.len() as u64).sum()
        }
    }

    /// Insert a posting at the given key, placing it in row_id-ascending order.
    pub fn insert_posting(&mut self, key: IndexKey, entry: IndexEntry) {
        if self.exact_only {
            self.insert_exact_posting(key, entry);
            return;
        }
        let vec = self.tree.entry(key).or_default();
        let pos = vec
            .binary_search_by(|e| e.row_id.cmp(&entry.row_id))
            .unwrap_or_else(|i| i);
        vec.insert(pos, entry);
    }

    /// Stamp `deleted_tx` on the posting matching `row_id` at `key`.
    pub fn tombstone_posting(&mut self, key: &IndexKey, row_id: RowId, deleted_tx: TxId) {
        if self.exact_only {
            self.tombstone_exact_posting(key, row_id, deleted_tx);
            return;
        }
        if let Some(vec) = self.tree.get_mut(key) {
            for entry in vec.iter_mut() {
                if entry.row_id == row_id && entry.deleted_tx.is_none() {
                    entry.deleted_tx = Some(deleted_tx);
                    return;
                }
            }
        }
    }

    fn insert_exact_posting(&mut self, key: IndexKey, entry: IndexEntry) {
        let Some(hash) = hash_index_key(&key) else {
            return;
        };
        self.mark_exact_filter(hash);
        let buckets = self.exact.entry(hash).or_default();
        let postings = if let Some(bucket) = buckets.iter_mut().find(|bucket| bucket.key == key) {
            &mut bucket.postings
        } else {
            buckets.push(ExactBucket {
                key,
                postings: Vec::new(),
            });
            &mut buckets.last_mut().expect("bucket just pushed").postings
        };
        let pos = postings
            .binary_search_by(|e| e.row_id.cmp(&entry.row_id))
            .unwrap_or_else(|i| i);
        postings.insert(pos, entry);
    }

    fn tombstone_exact_posting(&mut self, key: &IndexKey, row_id: RowId, deleted_tx: TxId) {
        if let Some(hash) = hash_index_key(key)
            && let Some(bucket) = self
                .exact
                .get_mut(&hash)
                .and_then(|buckets| buckets.iter_mut().find(|bucket| bucket.key == *key))
        {
            for entry in bucket.postings.iter_mut() {
                if entry.row_id == row_id && entry.deleted_tx.is_none() {
                    entry.deleted_tx = Some(deleted_tx);
                    break;
                }
            }
        }
    }

    pub fn exact_postings(&self, key: &IndexKey) -> Option<&Vec<IndexEntry>> {
        if self.exact_only {
            return hash_index_key(key).and_then(|hash| {
                if !self.exact_filter_may_contain(hash) {
                    return None;
                }
                self.exact
                    .get(&hash)
                    .and_then(|buckets| buckets.iter().find(|bucket| bucket.key == *key))
                    .map(|bucket| &bucket.postings)
            });
        }
        self.tree.get(key)
    }

    pub fn exact_only(&self) -> bool {
        self.exact_only
    }

    fn reserve_insert_postings(&mut self, additional: usize) {
        if self.exact_only {
            self.exact.reserve(additional);
        }
    }

    fn mark_exact_filter(&mut self, hash: u64) {
        let ((word_a, bit_a), (word_b, bit_b)) = exact_filter_slots(hash);
        self.exact_filter[word_a] |= bit_a;
        self.exact_filter[word_b] |= bit_b;
    }

    fn exact_filter_may_contain(&self, hash: u64) -> bool {
        let ((word_a, bit_a), (word_b, bit_b)) = exact_filter_slots(hash);
        (self.exact_filter[word_a] & bit_a) != 0 && (self.exact_filter[word_b] & bit_b) != 0
    }
}

fn exact_filter_slots(hash: u64) -> ((usize, u64), (usize, u64)) {
    let bit_a = hash & EXACT_FILTER_MASK;
    let mixed = hash
        .rotate_left(27)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0xBF58_476D_1CE4_E5B9);
    let bit_b = mixed & EXACT_FILTER_MASK;
    (filter_slot(bit_a), filter_slot(bit_b))
}

fn filter_slot(bit: u64) -> (usize, u64) {
    ((bit / 64) as usize, 1_u64 << (bit % 64))
}

pub struct RelationalStore {
    pub tables: RwLock<HashMap<TableName, Vec<VersionedRow>>>,
    row_positions: RwLock<HashMap<(TableName, RowId), usize>>,
    row_version_positions: RwLock<HashMap<(TableName, RowId), Vec<usize>>>,
    /// Sync provenance sidecar: table → row_id → originating source LSN.
    /// Nested keying lets the hot apply path probe with a borrowed `&str`
    /// table name, avoiding a per-row `String` allocation.
    sync_source_lsns: RwLock<HashMap<TableName, HashMap<RowId, Lsn>>>,
    pub table_meta: RwLock<HashMap<TableName, TableMeta>>,
    /// table → index_name → IndexStorage. Physical storage is table-keyed so
    /// DML can maintain only the affected table's indexes without consulting
    /// metadata that DDL mutates separately.
    pub indexes: RwLock<HashMap<TableName, HashMap<String, IndexStorage>>>,
    /// Counts how many times `apply_changes`-style batch-level index-lock
    /// acquisitions have happened. The per-row commit path does not bump this
    /// counter; only coarse batch holders (`apply_changes` wrapping N rows) do.
    pub index_write_lock_count: AtomicU64,
    pub index_maintenance_visits: AtomicU64,
    pub open_index_maintenance_visits: AtomicU64,
    pub scan_rows_touched: AtomicU64,
    next_row_id: AtomicU64,
}

impl Default for RelationalStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RelationalStore {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            row_positions: RwLock::new(HashMap::new()),
            row_version_positions: RwLock::new(HashMap::new()),
            sync_source_lsns: RwLock::new(HashMap::new()),
            table_meta: RwLock::new(HashMap::new()),
            indexes: RwLock::new(HashMap::new()),
            index_write_lock_count: AtomicU64::new(0),
            index_maintenance_visits: AtomicU64::new(0),
            open_index_maintenance_visits: AtomicU64::new(0),
            scan_rows_touched: AtomicU64::new(0),
            next_row_id: AtomicU64::new(1),
        }
    }

    pub fn new_row_id(&self) -> RowId {
        RowId(self.next_row_id.fetch_add(1, Ordering::SeqCst))
    }

    pub fn sync_source_lsn(&self, table: &str, row_id: RowId) -> Option<Lsn> {
        self.sync_source_lsns
            .read()
            .get(table)
            .and_then(|by_row| by_row.get(&row_id))
            .copied()
    }

    pub fn set_sync_source_lsns(&self, entries: impl IntoIterator<Item = (TableName, RowId, Lsn)>) {
        let mut source_lsns = self.sync_source_lsns.write();
        for (table, row_id, lsn) in entries {
            source_lsns.entry(table).or_default().insert(row_id, lsn);
        }
    }

    pub fn clear_sync_source_lsns(&self, entries: impl IntoIterator<Item = (TableName, RowId)>) {
        let mut source_lsns = self.sync_source_lsns.write();
        for (table, row_id) in entries {
            if let Some(by_row) = source_lsns.get_mut(&table) {
                by_row.remove(&row_id);
                if by_row.is_empty() {
                    source_lsns.remove(&table);
                }
            }
        }
    }

    pub fn replace_sync_source_lsns(&self, entries: HashMap<(TableName, RowId), Lsn>) {
        let mut nested: HashMap<TableName, HashMap<RowId, Lsn>> = HashMap::new();
        for ((table, row_id), lsn) in entries {
            nested.entry(table).or_default().insert(row_id, lsn);
        }
        *self.sync_source_lsns.write() = nested;
    }

    /// Apply row inserts AND maintain every index (user-declared AND auto)
    /// on the affected tables. The index update runs under the same
    /// write-lock scope that the relational insert takes (implicitly held
    /// by the caller's commit_mutex).
    pub fn apply_inserts(&self, inserts: Vec<(TableName, VersionedRow)>) {
        self.apply_inserts_ref(&inserts);
    }

    pub fn apply_inserts_ref(&self, inserts: &[(TableName, VersionedRow)]) {
        self.apply_inserts_ref_skipping_indexes(inserts, &HashSet::new());
    }

    fn apply_inserts_ref_skipping_indexes(
        &self,
        inserts: &[(TableName, VersionedRow)],
        skip_index_rows: &HashSet<(TableName, RowId)>,
    ) {
        // Stage the owned row copies BEFORE acquiring any write guard. The
        // deep VersionedRow copy is the same retained data we keep either way;
        // doing it outside the lock keeps the writer's lock-hold window tight
        // so concurrent readers don't stall on the per-row clone cost.
        let staged: Vec<(TableName, VersionedRow)> = inserts.to_vec();
        let mut row_counts = HashMap::<TableName, usize>::new();
        let mut index_counts = HashMap::<TableName, usize>::new();
        for (table_name, row) in &staged {
            *row_counts.entry(table_name.clone()).or_default() += 1;
            if !skip_index_rows.contains(&(table_name.clone(), row.row_id)) {
                *index_counts.entry(table_name.clone()).or_default() += 1;
            }
        }
        let mut indexes = self.indexes.write();
        let mut tables = self.tables.write();
        let mut row_positions = self.row_positions.write();
        let mut row_version_positions = self.row_version_positions.write();
        row_positions.reserve(staged.len());
        row_version_positions.reserve(staged.len());
        for (table_name, count) in &row_counts {
            tables
                .entry(table_name.clone())
                .or_default()
                .reserve(*count);
        }
        for (table_name, count) in &index_counts {
            if let Some(table_indexes) = indexes.get_mut(table_name) {
                for index in table_indexes.values_mut() {
                    index.reserve_insert_postings(*count);
                }
            }
        }
        let mut visits = 0u64;
        for (table_name, row) in staged {
            let entry = IndexEntry {
                row_id: row.row_id,
                created_tx: row.created_tx,
                deleted_tx: row.deleted_tx,
            };
            let skip_indexes = skip_index_rows.contains(&(table_name.clone(), row.row_id));
            if !skip_indexes && let Some(table_indexes) = indexes.get_mut(&table_name) {
                for idx in table_indexes.values_mut() {
                    visits = visits.saturating_add(1);
                    let key = index_key_for_row(&idx.columns, &row.values);
                    idx.insert_posting(key, entry.clone());
                }
            }
            let row_id = row.row_id;
            let rows = tables.entry(table_name.clone()).or_default();
            let row_position = rows.len();
            row_positions.insert((table_name.clone(), row_id), row_position);
            row_version_positions
                .entry((table_name.clone(), row_id))
                .or_default()
                .push(row_position);
            rows.push(row);
        }
        if visits > 0 {
            self.bump_index_maintenance_visits(visits);
        }
    }

    pub fn apply_deletes(&self, deletes: Vec<(TableName, RowId, TxId)>) {
        self.apply_deletes_ref(&deletes);
    }

    pub fn apply_deletes_ref(&self, deletes: &[(TableName, RowId, TxId)]) {
        self.apply_deletes_ref_skipping_indexes(deletes, &HashSet::new());
    }

    fn apply_deletes_ref_skipping_indexes(
        &self,
        deletes: &[(TableName, RowId, TxId)],
        skip_index_rows: &HashSet<(TableName, RowId)>,
    ) {
        let mut indexes = self.indexes.write();
        let mut tables = self.tables.write();
        let row_positions = self.row_positions.read();
        let mut visits = 0u64;
        for (table_name, row_id, deleted_tx) in deletes {
            let row_position = row_positions
                .get(&(table_name.clone(), *row_id))
                .copied()
                .and_then(|position| {
                    tables
                        .get(table_name)
                        .and_then(|rows| rows.get(position))
                        .filter(|row| row.row_id == *row_id && row.deleted_tx.is_none())
                        .map(|_| position)
                });
            let Some(row_position) = row_position else {
                continue;
            };

            let skip_indexes = skip_index_rows.contains(&(table_name.clone(), *row_id));
            if !skip_indexes
                && let Some(values) = tables
                    .get(table_name)
                    .and_then(|rows| rows.get(row_position))
                    .map(|row| &row.values)
                && let Some(table_indexes) = indexes.get_mut(table_name)
            {
                for idx in table_indexes.values_mut() {
                    visits = visits.saturating_add(1);
                    let key = index_key_for_row(&idx.columns, values);
                    idx.tombstone_posting(&key, *row_id, *deleted_tx);
                }
            }
            if let Some(row) = tables
                .get_mut(table_name)
                .and_then(|rows| rows.get_mut(row_position))
                && row.row_id == *row_id
                && row.deleted_tx.is_none()
            {
                row.deleted_tx = Some(*deleted_tx);
            }
        }
        if visits > 0 {
            self.bump_index_maintenance_visits(visits);
        }
    }

    pub fn apply_replacements_ref(
        &self,
        deletes: &[(TableName, RowId, TxId)],
        inserts: &[(TableName, VersionedRow)],
    ) {
        let skip_index_rows = self.same_index_key_replacements(deletes, inserts);
        self.apply_deletes_ref_skipping_indexes(deletes, &skip_index_rows);
        self.apply_inserts_ref_skipping_indexes(inserts, &skip_index_rows);
    }

    pub fn create_table(&self, name: &str, meta: TableMeta) {
        self.tables.write().entry(name.to_string()).or_default();
        self.table_meta.write().insert(name.to_string(), meta);
    }

    pub fn insert_loaded_row(&self, name: &str, row: VersionedRow) {
        // Row-load during Database::open: populate every index (user-declared
        // + auto) so the rebuild stays in lockstep. Rows arrive in row_id
        // ascending order, satisfying I18.
        let index_names = self.index_names_for_table(name);
        {
            let mut indexes = self.indexes.write();
            let entry = IndexEntry {
                row_id: row.row_id,
                created_tx: row.created_tx,
                deleted_tx: row.deleted_tx,
            };
            let mut visits = 0u64;
            if let Some(table_indexes) = indexes.get_mut(name) {
                for index_name in &index_names {
                    let Some(idx) = table_indexes.get_mut(index_name) else {
                        continue;
                    };
                    visits = visits.saturating_add(1);
                    let key = index_key_for_row(&idx.columns, &row.values);
                    idx.insert_posting(key, entry.clone());
                }
            }
            if visits > 0 {
                self.bump_open_index_maintenance_visits(visits);
            }
        }
        let mut tables = self.tables.write();
        let rows = tables.entry(name.to_string()).or_default();
        let row_position = rows.len();
        self.row_positions
            .write()
            .insert((name.to_string(), row.row_id), row_position);
        self.row_version_positions
            .write()
            .entry((name.to_string(), row.row_id))
            .or_default()
            .push(row_position);
        rows.push(row);
    }

    fn index_names_for_table(&self, table: &str) -> Vec<String> {
        self.indexes
            .read()
            .get(table)
            .map(|indexes| indexes.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
    }

    fn same_index_key_replacements(
        &self,
        deletes: &[(TableName, RowId, TxId)],
        inserts: &[(TableName, VersionedRow)],
    ) -> HashSet<(TableName, RowId)> {
        let mut inserts_by_row = HashMap::new();
        let mut duplicate_inserts = HashSet::new();
        for (table, row) in inserts {
            let key = (table.clone(), row.row_id);
            if inserts_by_row.insert(key.clone(), row).is_some() {
                duplicate_inserts.insert(key);
            }
        }
        if inserts_by_row.is_empty() {
            return HashSet::new();
        }

        let mut delete_counts = HashMap::<(TableName, RowId), usize>::new();
        for (table, row_id, _) in deletes {
            *delete_counts.entry((table.clone(), *row_id)).or_default() += 1;
        }

        let indexes = self.indexes.read();
        let tables = self.tables.read();
        let row_positions = self.row_positions.read();
        let mut skip = HashSet::new();
        for (table, row_id, _) in deletes {
            let key = (table.clone(), *row_id);
            if delete_counts.get(&key).copied() != Some(1) || duplicate_inserts.contains(&key) {
                continue;
            }
            let Some(new_row) = inserts_by_row.get(&key) else {
                continue;
            };
            let Some(table_indexes) = indexes.get(table) else {
                continue;
            };
            let old_position = row_positions.get(&key).copied().and_then(|position| {
                tables
                    .get(table)
                    .and_then(|rows| rows.get(position))
                    .filter(|row| row.row_id == *row_id && row.deleted_tx.is_none())
                    .map(|_| position)
            });
            let Some(old_row) = old_position
                .and_then(|position| tables.get(table).and_then(|rows| rows.get(position)))
            else {
                continue;
            };
            if table_indexes.values().all(|index| {
                index_key_for_row(&index.columns, &old_row.values)
                    == index_key_for_row(&index.columns, &new_row.values)
            }) {
                skip.insert(key);
            }
        }
        skip
    }

    pub fn row_by_id(
        &self,
        table: &str,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
    ) -> Option<VersionedRow> {
        let tables = self.tables.read();
        let rows = tables.get(table)?;
        let version_positions = self.row_version_positions.read();
        let positions = version_positions.get(&(table.to_string(), row_id))?;
        let candidate_count = positions.partition_point(|position| {
            rows.get(*position)
                .is_some_and(|row| row.row_id == row_id && row.created_tx.0 <= snapshot.0)
        });
        let position = *positions.get(candidate_count.checked_sub(1)?)?;
        rows.get(position)
            .filter(|row| row.row_id == row_id && row.visible_at(snapshot))
            .cloned()
    }

    pub fn live_row_by_id(&self, table: &str, row_id: RowId) -> Option<VersionedRow> {
        let tables = self.tables.read();
        let positions = self.row_positions.read();
        let position = *positions.get(&(table.to_string(), row_id))?;
        let rows = tables.get(table)?;
        rows.get(position)
            .filter(|row| row.row_id == row_id && row.deleted_tx.is_none())
            .cloned()
    }

    pub fn live_rows_by_id(
        &self,
        keys: &[(TableName, RowId)],
    ) -> HashMap<TableName, HashMap<RowId, VersionedRow>> {
        let tables = self.tables.read();
        let positions = self.row_positions.read();
        let mut by_table = HashMap::<TableName, HashMap<RowId, VersionedRow>>::new();
        for (table, row_id) in keys {
            if by_table
                .get(table)
                .is_some_and(|by_row| by_row.contains_key(row_id))
            {
                continue;
            }
            let Some(position) = positions.get(&(table.clone(), *row_id)).copied() else {
                continue;
            };
            let Some(row) = tables
                .get(table)
                .and_then(|rows| rows.get(position))
                .filter(|row| row.row_id == *row_id && row.deleted_tx.is_none())
                .cloned()
            else {
                continue;
            };
            by_table
                .entry(table.clone())
                .or_default()
                .insert(*row_id, row);
        }
        by_table
    }

    fn rebuild_position_maps_for_table(
        table: &str,
        rows: &[VersionedRow],
        positions: &mut HashMap<(TableName, RowId), usize>,
        version_positions: &mut HashMap<(TableName, RowId), Vec<usize>>,
    ) {
        positions.retain(|(position_table, _), _| position_table != table);
        version_positions.retain(|(position_table, _), _| position_table != table);

        let table_name = table.to_string();
        let mut row_keys = Vec::new();
        for (position, row) in rows.iter().enumerate() {
            let key = (table_name.clone(), row.row_id);
            match version_positions.entry(key) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().push(position);
                }
                Entry::Vacant(entry) => {
                    row_keys.push(entry.key().clone());
                    entry.insert(vec![position]);
                }
            }
        }

        for key in row_keys {
            let latest_position = {
                let Some(row_positions) = version_positions.get_mut(&key) else {
                    continue;
                };
                row_positions.sort_by_key(|position| {
                    let row = &rows[*position];
                    (row.created_tx.0, *position)
                });
                row_positions.last().copied()
            };
            if let Some(position) = latest_position {
                positions.insert(key, position);
            }
        }
    }

    pub fn remove_rows_by_id(&self, table: &str, row_ids: &HashSet<RowId>) {
        if row_ids.is_empty() {
            return;
        }
        let mut tables = self.tables.write();
        let Some(rows) = tables.get_mut(table) else {
            return;
        };
        rows.retain(|row| !row_ids.contains(&row.row_id));
        {
            let mut source_lsns = self.sync_source_lsns.write();
            if let Some(by_row) = source_lsns.get_mut(table) {
                by_row.retain(|row_id, _| !row_ids.contains(row_id));
                if by_row.is_empty() {
                    source_lsns.remove(table);
                }
            }
        }
        let mut positions = self.row_positions.write();
        let mut version_positions = self.row_version_positions.write();
        Self::rebuild_position_maps_for_table(table, rows, &mut positions, &mut version_positions);
    }

    pub fn remove_row_versions(&self, table: &str, versions: &HashSet<(RowId, TxId)>) {
        if versions.is_empty() {
            return;
        }
        let mut tables = self.tables.write();
        let Some(rows) = tables.get_mut(table) else {
            return;
        };
        rows.retain(|row| !versions.contains(&(row.row_id, row.created_tx)));
        let remaining_live_row_ids = rows
            .iter()
            .filter(|row| row.deleted_tx.is_none())
            .map(|row| row.row_id)
            .collect::<HashSet<_>>();
        {
            let mut source_lsns = self.sync_source_lsns.write();
            if let Some(by_row) = source_lsns.get_mut(table) {
                by_row.retain(|row_id, _| remaining_live_row_ids.contains(row_id));
                if by_row.is_empty() {
                    source_lsns.remove(table);
                }
            }
        }
        let mut positions = self.row_positions.write();
        let mut version_positions = self.row_version_positions.write();
        Self::rebuild_position_maps_for_table(table, rows, &mut positions, &mut version_positions);
    }

    pub fn rebuild_row_position_maps(&self) {
        let tables = self.tables.read();
        let total_rows = tables.values().map(Vec::len).sum();
        let mut positions = self.row_positions.write();
        let mut version_positions = self.row_version_positions.write();
        positions.clear();
        version_positions.clear();
        positions.reserve(total_rows);
        version_positions.reserve(total_rows);
        for (table, rows) in tables.iter() {
            Self::rebuild_position_maps_for_table(
                table,
                rows,
                &mut positions,
                &mut version_positions,
            );
        }
    }

    pub fn max_row_id(&self) -> RowId {
        self.tables
            .read()
            .values()
            .flat_map(|rows| rows.iter().map(|row| row.row_id))
            .max()
            .unwrap_or(RowId(0))
    }

    pub fn set_next_row_id(&self, next_row_id: RowId) {
        self.next_row_id.store(next_row_id.0, Ordering::SeqCst);
    }

    pub fn drop_table(&self, name: &str) {
        self.table_meta.write().remove(name);
        // Drop all indexes whose key-table matches; releases BTreeMap storage.
        let mut indexes = self.indexes.write();
        indexes.remove(name);
        self.tables.write().remove(name);
        self.row_positions
            .write()
            .retain(|(table, _), _| table != name);
        self.row_version_positions
            .write()
            .retain(|(table, _), _| table != name);
        self.sync_source_lsns.write().remove(name);
    }

    /// Register a new index storage for (table, name). The IndexDecl must
    /// already be present in the table's TableMeta (callers should
    /// `register_index_meta` before `create_index_storage`).
    pub fn create_index_storage(
        &self,
        table: &str,
        name: &str,
        columns: Vec<(String, SortDirection)>,
    ) {
        self.indexes
            .write()
            .entry(table.to_string())
            .or_default()
            .insert(name.to_string(), IndexStorage::new(columns));
    }

    pub fn create_exact_index_storage(
        &self,
        table: &str,
        name: &str,
        columns: Vec<(String, SortDirection)>,
    ) {
        let mut indexes = self.indexes.write();
        let table_indexes = indexes.entry(table.to_string()).or_default();
        if table_indexes.iter().any(|(existing_name, storage)| {
            existing_name != name && storage.exact_only() && storage.columns == columns
        }) {
            return;
        }
        if let Some(existing) = table_indexes.get(name) {
            if existing.exact_only() && existing.columns == columns {
                return;
            }
            return;
        }
        table_indexes.insert(name.to_string(), IndexStorage::new_exact_only(columns));
    }

    /// Remove an index storage. Called from DROP INDEX / CASCADE.
    pub fn drop_index_storage(&self, table: &str, name: &str) {
        self.indexes
            .write()
            .get_mut(table)
            .and_then(|indexes| indexes.remove(name));
    }

    /// Build (or rebuild) an index storage by scanning the current table rows.
    /// Used after Database::open completes rebuilding TableMeta.indexes but
    /// before the executor sees queries. Iterates in row_id-ascending order
    /// to preserve I18 tie-break stability.
    pub fn rebuild_index(&self, table: &str, name: &str) {
        let mut indexes = self.indexes.write();
        let Some(table_indexes) = indexes.get_mut(table) else {
            return;
        };
        let Some(existing) = table_indexes.get(name) else {
            return;
        };
        let columns = existing.columns.clone();
        let mut rebuilt = if existing.exact_only() {
            IndexStorage::new_exact_only(columns.clone())
        } else {
            IndexStorage::new(columns.clone())
        };
        let tables = self.tables.read();
        if let Some(rows) = tables.get(table) {
            let mut sorted: Vec<&VersionedRow> = rows.iter().collect();
            sorted.sort_by_key(|r| r.row_id);
            for row in sorted {
                let key = index_key_for_row(&columns, &row.values);
                rebuilt.insert_posting(
                    key,
                    IndexEntry {
                        row_id: row.row_id,
                        created_tx: row.created_tx,
                        deleted_tx: row.deleted_tx,
                    },
                );
            }
        }
        table_indexes.insert(name.to_string(), rebuilt);
    }

    /// Introspect total postings across all indexes (including tombstones).
    /// Tests use this to confirm DROP TABLE releases index storage.
    pub fn introspect_indexes_total_entries(&self) -> u64 {
        self.indexes
            .read()
            .values()
            .flat_map(|indexes| indexes.values())
            .map(|s| s.total_entries_including_tombstones())
            .sum()
    }

    /// Bump the batch-level index-write lock counter. Called once per
    /// `apply_changes` batch to prove I14.
    pub fn bump_index_write_lock_count(&self) {
        self.index_write_lock_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn index_write_lock_count(&self) -> u64 {
        self.index_write_lock_count.load(Ordering::SeqCst)
    }

    pub fn bump_index_maintenance_visits(&self, delta: u64) {
        self.index_maintenance_visits
            .fetch_add(delta, Ordering::SeqCst);
    }

    pub fn index_maintenance_visits(&self) -> u64 {
        self.index_maintenance_visits.load(Ordering::SeqCst)
    }

    pub fn reset_index_maintenance_visits(&self) {
        self.index_maintenance_visits.store(0, Ordering::SeqCst);
    }

    pub fn bump_open_index_maintenance_visits(&self, delta: u64) {
        self.open_index_maintenance_visits
            .fetch_add(delta, Ordering::SeqCst);
    }

    pub fn open_index_maintenance_visits(&self) -> u64 {
        self.open_index_maintenance_visits.load(Ordering::SeqCst)
    }

    pub fn bump_scan_rows_touched(&self, rows: u64) {
        // Observability-only counter. Relaxed avoids the full SeqCst barrier on
        // the hot scan path (8 concurrent readers in the contention bench touch
        // this line on every probe); the test assertions reset then read after
        // all writer/reader threads join, so no cross-thread ordering is needed.
        self.scan_rows_touched.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn scan_rows_touched(&self) -> u64 {
        self.scan_rows_touched.load(Ordering::Relaxed)
    }

    pub fn reset_scan_rows_touched(&self) {
        self.scan_rows_touched.store(0, Ordering::Relaxed);
    }

    pub fn alter_table_add_column(
        &self,
        table: &str,
        col: contextdb_core::ColumnDef,
    ) -> Result<(), String> {
        let mut meta = self.table_meta.write();
        let m = meta
            .get_mut(table)
            .ok_or_else(|| format!("table '{}' not found", table))?;
        if m.columns.iter().any(|c| c.name == col.name) {
            return Err(format!(
                "column '{}' already exists in table '{}'",
                col.name, table
            ));
        }
        m.columns.push(col);
        Ok(())
    }

    pub fn alter_table_drop_column(&self, table: &str, column: &str) -> Result<(), String> {
        {
            let mut meta = self.table_meta.write();
            let m = meta
                .get_mut(table)
                .ok_or_else(|| format!("table '{}' not found", table))?;
            let pos = m
                .columns
                .iter()
                .position(|c| c.name == column)
                .ok_or_else(|| {
                    format!("column '{}' does not exist in table '{}'", column, table)
                })?;
            if m.columns[pos].primary_key {
                return Err(format!("cannot drop primary key column '{}'", column));
            }
            m.columns.remove(pos);
        }
        {
            let mut tables = self.tables.write();
            if let Some(rows) = tables.get_mut(table) {
                for row in rows.iter_mut() {
                    row.values.remove(column);
                }
            }
        }
        Ok(())
    }

    pub fn alter_table_rename_column(
        &self,
        table: &str,
        from: &str,
        to: &str,
    ) -> Result<(), String> {
        {
            let mut meta = self.table_meta.write();
            let m = meta
                .get_mut(table)
                .ok_or_else(|| format!("table '{}' not found", table))?;
            if m.columns.iter().any(|c| c.name == to) {
                return Err(format!(
                    "column '{}' already exists in table '{}'",
                    to, table
                ));
            }
            let col = m
                .columns
                .iter_mut()
                .find(|c| c.name == from)
                .ok_or_else(|| format!("column '{}' does not exist in table '{}'", from, table))?;
            if col.primary_key {
                return Err(format!("cannot rename primary key column '{}'", from));
            }
            col.name = to.to_string();
        }
        {
            let mut tables = self.tables.write();
            if let Some(rows) = tables.get_mut(table) {
                for row in rows.iter_mut() {
                    if let Some(val) = row.values.remove(from) {
                        row.values.insert(to.to_string(), val);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn is_immutable(&self, table: &str) -> bool {
        self.table_meta
            .read()
            .get(table)
            .is_some_and(|m| m.immutable)
    }

    pub fn validate_state_transition(
        &self,
        table: &str,
        column: &str,
        from: &str,
        to: &str,
    ) -> bool {
        self.table_meta
            .read()
            .get(table)
            .and_then(|m| m.state_machine.as_ref())
            .filter(|sm| sm.column == column)
            .is_none_or(|sm| {
                sm.transitions
                    .get(from)
                    .is_some_and(|targets| targets.iter().any(|t| t == to))
            })
    }

    pub fn table_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.tables.read().keys().cloned().collect();
        names.sort();
        names
    }

    pub fn table_meta(&self, name: &str) -> Option<TableMeta> {
        self.table_meta.read().get(name).cloned()
    }

    pub fn table_has_state_machine(&self, name: &str) -> Option<bool> {
        self.table_meta
            .read()
            .get(name)
            .map(|meta| meta.state_machine.is_some())
    }
}

/// Build the directed IndexKey for a row's values given the index's column
/// declaration. Missing columns map to `Value::Null` (NULL partition).
pub fn index_key_for_row(
    columns: &[(String, SortDirection)],
    values: &HashMap<String, Value>,
) -> IndexKey {
    columns
        .iter()
        .map(|(col, dir)| {
            let v = values.get(col).cloned().unwrap_or(Value::Null);
            match dir {
                SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(v)),
                SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(v)),
            }
        })
        .collect()
}

/// Build the directed IndexKey for a Vec of Values (one per indexed column).
pub fn index_key_from_values(columns: &[(String, SortDirection)], values: &[Value]) -> IndexKey {
    columns
        .iter()
        .zip(values.iter())
        .map(|((_, dir), v)| match dir {
            SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(v.clone())),
            SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(v.clone())),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_core::{Lsn, SnapshotId};

    fn row(row_id: RowId, created_tx: TxId, deleted_tx: Option<TxId>, label: &str) -> VersionedRow {
        VersionedRow {
            row_id,
            values: HashMap::from([("label".to_string(), Value::Text(label.to_string()))]),
            created_tx,
            deleted_tx,
            lsn: Lsn(created_tx.0),
            created_at: None,
        }
    }

    #[test]
    fn remove_row_versions_preserves_sorted_same_row_positions() {
        let store = RelationalStore::new();
        store.insert_loaded_row("items", row(RowId(1), TxId(3), None, "live"));
        store.insert_loaded_row("items", row(RowId(1), TxId(2), Some(TxId(3)), "old"));
        store.insert_loaded_row("items", row(RowId(2), TxId(1), None, "other"));
        store.rebuild_row_position_maps();

        let pruned = HashSet::from([(RowId(2), TxId(1))]);
        store.remove_row_versions("items", &pruned);

        let old = store
            .row_by_id("items", RowId(1), SnapshotId(2))
            .expect("older snapshot must still find the first version");
        assert_eq!(
            old.values.get("label"),
            Some(&Value::Text("old".to_string()))
        );

        let live = store
            .row_by_id("items", RowId(1), SnapshotId(3))
            .expect("newer snapshot must find the replacement version");
        assert_eq!(
            live.values.get("label"),
            Some(&Value::Text("live".to_string()))
        );

        let current = store
            .live_row_by_id("items", RowId(1))
            .expect("latest-row map must point at the live version");
        assert_eq!(
            current.values.get("label"),
            Some(&Value::Text("live".to_string()))
        );
    }
}
