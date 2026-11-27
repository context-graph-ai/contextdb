//! schema: table declarations: checking them, storing them, printing them back.
//!
//! It never changes a row.
//!
//! A table's declaration is kept as one schema record under the table's name: its number, its
//! columns and rules as they were declared, its indexes with their numbers, and each VECTOR
//! column's number and clauses.
//!
//! A table's indexes come in one order: the index behind the primary key (`__pk_<column>`),
//! the index behind each UNIQUE column in column order (`__unique_<column>`; the key column has
//! its key's index and no other), the index behind each UNIQUE rule of the table in declared
//! order (`__unique_<column>_<column>...`), the index that serves the child side of each
//! reference no earlier index leads with (`__fk_<column>...`), the two indexes of a table of
//! edges (`__graph_edge_...`), then the indexes of CREATE INDEX in the order they were made.

mod alter;
mod check;
mod record;
mod render;
mod rules;
#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included};
use std::sync::Arc;

pub(crate) use self::alter::{Declared, named};
pub(crate) use self::rules::{Reference, Rules};
use crate::memory::{self, Budget, Owned, Reservation};
use crate::sql::ast::EventDeclaration;
use crate::sql::plan::{CreateTablePlan, InsertPlan};
use crate::sql::{self, Expr, Plan};
use crate::store::{
    Cell, ChangeSet, Entry, EntryShape, IndexId, OWED, Read, Record, Row, SCHEDULE, TableId,
    VectorId, View,
};
pub(crate) use crate::types::Quantization;
use crate::types::{
    ColumnType, Error, IndexDecl, Kind, Result, SortDirection, TableMeta, Value, VectorIndexRef,
    VectorSearchMode,
};

pub(crate) const SCHEMA: u64 = 64;
const _: () = assert!(
    size_of::<Table>() <= 64 * 83
        && size_of::<TableMeta>() <= 64 * 23
        && size_of::<crate::types::ColumnDef>() <= 64 * 13
        && size_of::<IndexDecl>() <= 64 * 12
        && size_of::<Reference>() <= 64 * 17
        && size_of::<rules::Count>() <= 64 * 12
        && size_of::<VectorColumn>() <= 64 * 17
        && size_of::<rules::RankPolicy>() <= 64 * 24
        && size_of::<crate::types::StateMachine>() <= 64 * 8
        && size_of::<crate::types::RouteDecl>() <= 64 * 13
);

/// What the name of an index the engine makes for itself begins with. An index of CREATE INDEX
/// may not take one of these.
const RESERVED_PREFIXES: [&str; 5] = ["__pk_", "__unique_", "__fk_", "__graph_", "__retain_"];

/// What [`Table::insert`] gives.
pub(crate) type Inserted<'p> = (Vec<usize>, Option<(Vec<usize>, Vec<(usize, &'p Expr)>)>);

type IndexShape = (EntryShape, Vec<(usize, SortDirection)>);

/// One declared table as the other parts use it. The rules a host reads are in `meta`; what
/// `schema` works out from the declaration beyond them is in `rules`.
#[derive(Clone)]
pub(crate) struct Table {
    pub id: TableId,
    pub(crate) stored: u32,
    pub(crate) heap: u64,
    pub name: String,
    pub meta: TableMeta,
    /// Ordinary index ids in metadata order, followed by the retained table's write-time id.
    pub index_ids: Vec<IndexId>,
    /// One entry per VECTOR column, in column order.
    pub vectors: Vec<VectorColumn>,
    /// The primary key's columns in declared order; empty for a table that declares no key.
    pub key: Vec<usize>,
    /// The key was written as an element of the table, `PRIMARY KEY (...)`, not on its column.
    pub key_is_element: bool,
    /// For each column, what a row with no cell for it reads as.
    pub missing: Vec<Value>,
    pub rules: Rules,
    /// How the entries of each index are made, in the order of `meta.indexes`.
    shapes: Arc<[IndexShape]>,
}

/// How many partitions a VECTOR column with a PARTITION_KEY may have when its declaration
/// names no MAX_PARTITIONS.
const DEFAULT_MAX_PARTITIONS: u32 = 256;

/// What a table keeps about one of its VECTOR columns: its number in the store and the clauses
/// it was declared with. The column's dimension is in its type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VectorColumn {
    pub id: VectorId,
    /// The column's name.
    pub column: String,
    /// The columns of PARTITION_KEY, in declared order; empty when none was declared.
    pub partition_key: Vec<String>,
    /// MAX_PARTITIONS as declared; `None` when the declaration names none.
    pub max_partitions: Option<u32>,
    /// SEARCH_MODE as declared; `None` when the declaration names none.
    pub search_mode: Option<VectorSearchMode>,
    /// INDEXING_THRESHOLD as declared; `None` when the declaration names none.
    pub indexing_threshold: Option<u64>,
    /// PROBES as declared: how many groups a search scans; `None` lets it measure.
    pub probes: Option<u32>,
    /// COMPACTION as declared; `None` when the declaration names none.
    pub compaction: Option<Compaction>,
    /// How the column keeps a vector: what `WITH (quantization = ...)` declared.
    pub quantization: Quantization,
}

/// What `COMPACTION` declares: when the vectors written since a partition was grouped, and
/// the slots its rows have left behind, call for IVF it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Compaction {
    /// `COMPACTION NONE`: never for those reasons.
    None,
    /// `COMPACTION (CHANGE_PERCENT = n, TOMBSTONE_PERCENT = n)`; a member that was not
    /// written is `None` and takes its default.
    Percent {
        change: Option<u8>,
        tombstone: Option<u8>,
    },
}

/// INDEXING_THRESHOLD of a column that declares none: a comparison of every vector of this many
/// takes about a hundredth of a second on one core, and past it the IVFs are worth using.
const DEFAULT_INDEX_AT: u64 = 100_000;
/// A partition is given its first IVF when it holds this share of INDEXING_THRESHOLD: one in
/// a hundred, so that the IVFs of a store spread over many partitions are in place before
/// AUTO begins to use them.
const GROUPED_SHARE: u64 = 100;
/// CHANGE_PERCENT and TOMBSTONE_PERCENT of a column that declares none.
const DEFAULT_COMPACTION: (u8, u8) = (20, 10);

impl VectorColumn {
    /// The most partitions the column may have: what was declared, 256 when a PARTITION_KEY
    /// was declared with no MAX_PARTITIONS, and `None` for a column with no PARTITION_KEY,
    /// which has its one partition and no limit to meet.
    pub(crate) fn limit(&self) -> Option<u32> {
        let partitioned = !self.partition_key.is_empty();
        partitioned.then(|| self.max_partitions.unwrap_or(DEFAULT_MAX_PARTITIONS))
    }

    /// The mode a search of the column runs in when its statement has no USE VECTOR.
    pub(crate) fn mode(&self) -> VectorSearchMode {
        self.search_mode.unwrap_or_default()
    }

    /// INDEXING_THRESHOLD in force: what was declared, else 100,000. The count of vectors in the
    /// selected partitions together from which AUTO uses their IVFs.
    pub(crate) fn index_at(&self) -> u64 {
        self.indexing_threshold.unwrap_or(DEFAULT_INDEX_AT)
    }

    /// How many slots a partition's tail holds when its first IVF is due: a hundredth of
    /// INDEXING_THRESHOLD in force and one at least, so 1,000 where nothing is declared.
    pub(crate) fn grouped_from(&self) -> u64 {
        (self.index_at() / GROUPED_SHARE).max(1)
    }

    /// CHANGE_PERCENT and TOMBSTONE_PERCENT in force, 20 and 10 where none was declared;
    /// `None` under `COMPACTION NONE`.
    pub(crate) fn compaction(&self) -> Option<(u8, u8)> {
        let (change, tombstone) = DEFAULT_COMPACTION;
        match self.compaction {
            None => Some((change, tombstone)),
            Some(Compaction::None) => None,
            Some(Compaction::Percent {
                change: c,
                tombstone: t,
            }) => Some((c.unwrap_or(change), t.unwrap_or(tombstone))),
        }
    }
}

impl Table {
    /// Where the column a statement or an index names is in a row of the table.
    pub(crate) fn column(&self, name: &str) -> Result<usize> {
        rules::position(&self.name, &self.meta, name)
    }

    /// Where each column a statement writes is in a row; one named twice is refused.
    pub(crate) fn targets<'n>(&self, of: impl Iterator<Item = &'n String>) -> Result<Vec<usize>> {
        let mut targets = Vec::new();
        for name in of {
            let at = self.column(name)?;
            if targets.contains(&at) {
                let table = &self.name;
                let refusal = format!("{name} is named twice as a column {table} is written in");
                return Err(Error::PlanError(refusal));
            }
            targets.push(at);
        }
        Ok(targets)
    }

    /// Where each assigned column is in a row, with its new value.
    pub(crate) fn set<'e>(&self, set: &'e [(String, Expr)]) -> Result<Vec<(usize, &'e Expr)>> {
        let at = self.targets(set.iter().map(|(name, _)| name))?;
        let values = set.iter().map(|(_, value)| value);
        Ok(at.into_iter().zip(values).collect())
    }

    /// Checks an INSERT's columns, its `ON CONFLICT` and the length of each row of VALUES. Gives
    /// where each column is, and the columns of `ON CONFLICT` with where each assignment writes.
    pub(crate) fn insert<'p>(&self, plan: &'p InsertPlan) -> Result<Inserted<'p>> {
        let named = match plan.columns.is_empty() {
            true => (0..self.meta.columns.len()).collect(),
            false => self.targets(plan.columns.iter())?,
        };
        let conflict = match &plan.on_conflict {
            Some(conflict) => {
                let on = conflict.columns.iter().map(|name| self.column(name));
                let mut on: Vec<usize> = on.collect::<Result<_>>()?;
                on.sort_unstable();
                let is_on = |index: &IndexDecl| {
                    let at = index.columns.iter().map(|(name, _)| self.column(name).ok());
                    let mut at: Vec<_> = at.collect::<Option<_>>().unwrap_or_default();
                    at.sort_unstable();
                    rules::is_rule(index) && at == on
                };
                if !self.meta.indexes.iter().any(is_on) {
                    return Err(Error::PlanError(format!(
                        "ON CONFLICT ({}) names neither the primary key nor a UNIQUE rule of {}",
                        conflict.columns.join(", "),
                        self.name
                    )));
                }
                Some((on, self.set(&conflict.assignments)?))
            }
            None => None,
        };
        let (table, columns) = (&self.name, named.len());
        if let Some(given) = plan.values.iter().map(Vec::len).find(|&n| n != columns) {
            let refusal = format!("INSERT INTO {table} gives {given} values for {columns} columns");
            return Err(Error::PlanError(refusal));
        }
        Ok((named, conflict))
    }

    /// The cell a stored row holds for a column. A row written before the column was added has
    /// none, and reads as the table says such a row does.
    pub(crate) fn cell<'a>(&self, row: &'a Row, column: usize) -> Cow<'a, Cell> {
        match row.cells.get(column) {
            Some(cell) => Cow::Borrowed(cell),
            None => Cow::Owned(Cell::Here(self.missing[column].clone())),
        }
    }

    /// How the entries of the index at this place in `meta.indexes` are made.
    pub(crate) fn entry_shape(&self, index: usize) -> &EntryShape {
        &self.shapes[index].0
    }

    pub(crate) fn retention_entry(&self, row: &Row) -> Option<Entry> {
        let shape = &self.shapes.get(self.meta.indexes.len())?.0;
        let null = |at| *self.cell(row, at) == Cell::Here(Value::Null);
        let s = i64::from(row.written.unwrap_or(0));
        self.rules.expires_column.is_none_or(null).then_some(())?;
        shape.entry(&[Value::Int64(s)]).ok()
    }

    /// The entry, in the index at this place in `meta.indexes`, of a row holding `values` in its
    /// columns. One over a JSON path has none for a row with no [`field_key`] at the path.
    pub(crate) fn entry(&self, index: usize, values: &[Value]) -> Result<Option<Entry>> {
        let path = &self.meta.indexes[index].json_path;
        let field = match (path.is_empty(), values) {
            (true, _) => return self.shapes[index].0.entry(values).map(Some),
            (false, [Value::Json(json)]) => path.iter().try_fold(json, |json, key| json.get(key)),
            (false, _) => None,
        };
        let key = field.and_then(field_key);
        key.map(|key| self.shapes[index].0.entry(&[key]))
            .transpose()
    }

    /// The table with the shape of each of its indexes, made once here: every table of a
    /// catalog has been through this.
    fn shaped(mut self) -> Table {
        let shape = |index: &IndexDecl| {
            let columns = index.columns.iter();
            let directions: Vec<_> = columns.clone().map(|(_, direction)| *direction).collect();
            let of = |name: &String| self.meta.columns.iter().find(|c| c.name == *name);
            let types: Vec<ColumnType> = match index.json_path.is_empty() {
                true => columns
                    .clone()
                    .filter_map(|(name, _)| Some(of(name)?.column_type))
                    .collect(),
                // An index over a JSON path is one TEXT column to its entries.
                false => vec![ColumnType::Text],
            };
            let resolved = columns
                .map(|(name, way)| (self.column(name).expect("a checked index column"), *way))
                .collect();
            (EntryShape::new(&types, &directions), resolved)
        };
        let mut shapes: Vec<_> = self.meta.indexes.iter().map(shape).collect();
        if self.meta.retain.is_some() {
            shapes.push((EntryShape::new(&[ColumnType::Integer], &[]), Vec::new()));
        }
        self.shapes = shapes.into();
        self
    }

    /// Borrows a count's prefix from its index's resolved columns.
    pub(crate) fn count_columns(&self, count: &rules::Count) -> &[(usize, SortDirection)] {
        &self.shapes[count.index].1[..count.prefix]
    }

    fn shape_bytes(&self) -> u64 {
        let columns = self
            .meta
            .indexes
            .iter()
            .map(|i| i.columns.len())
            .sum::<usize>();
        ((self.meta.indexes.len() + usize::from(self.meta.retain.is_some()))
            * size_of::<IndexShape>()
            + 2 * size_of::<usize>()
            + columns * (size_of::<SortDirection>() + size_of::<(usize, SortDirection)>()))
            as u64
    }

    /// What the table keeps about the VECTOR column of this name, `None` when it has no such
    /// column or the column is of another type.
    pub(crate) fn vector(&self, column: &str) -> Option<&VectorColumn> {
        self.vectors.iter().find(|vector| vector.column == column)
    }

    /// How many numbers a vector of this VECTOR column has.
    pub(crate) fn dimension(&self, column: &VectorColumn) -> usize {
        let columns = self.meta.columns.iter();
        let mut sizes = columns.filter_map(|declared| match declared.column_type {
            ColumnType::Vector(size) if declared.name == column.column => Some(size),
            _ => None,
        });
        sizes.next().unwrap_or_default()
    }

    /// The column as a refusal names it.
    pub(crate) fn vector_ref(&self, vector: &VectorColumn) -> VectorIndexRef {
        VectorIndexRef {
            table: self.name.clone(),
            column: vector.column.clone(),
        }
    }
}

/// What an index over a JSON path keeps for the field there: its text as JSON writes it, a
/// number as its REAL value (adding zero makes `-0` into `0`), so that fields `=` finds equal
/// keep one text; nothing for an object or an array.
pub(crate) fn field_key(field: &serde_json::Value) -> Option<Value> {
    Some(Value::Text(match field {
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => return None,
        serde_json::Value::Number(number) => (number.as_f64()? + 0.0).to_string(),
        scalar => scalar.to_string(),
    }))
}

const TABLE_NODE: u64 = memory::node::<(String, Arc<Owned<Table>>)>();
const SCHEDULE_NODE: u64 = memory::node::<(String, u64)>();
/// An event declaration, with what it is charged.
type Event = (EventDeclaration, u64);

const EVENT_NODE: u64 = memory::node::<((Kind, String), Arc<Owned<Event>>)>();
const ID_NODE: u64 = memory::node::<(TableId, Arc<Owned<On>>)>();
const VECTOR_NODE: u64 = memory::node::<(VectorId, TableId)>();

/// What a commit looks up for a table by number: its name, its events' keys, its children.
#[derive(Clone, Default)]
pub(crate) struct On {
    name: String,
    events: Vec<(Kind, String)>,
    children: Vec<String>,
}

/// Every table declared in a store at one moment; each entry is shared, with its own charge.
pub(crate) struct Catalog {
    pub(crate) events: BTreeMap<(Kind, String), Arc<Owned<Event>>>,
    pub(crate) schedules: BTreeMap<String, u64>,
    tables: BTreeMap<String, Arc<Owned<Table>>>,
    ids: BTreeMap<TableId, Arc<Owned<On>>>,
    vectors: BTreeMap<VectorId, TableId>,
    edges: Arc<Owned<Vec<TableId>>>,
    /// One past the highest VECTOR column, index and kept count number given since the open.
    numbers: [u32; 3],
    /// What the entries are charged, together.
    entries: u64,
    held: Reservation,
}

/// Puts `item` in its place in a sorted list, or takes it out.
fn toggle<T: Ord>(list: &mut Vec<T>, item: T, put: bool) {
    match (list.binary_search(&item), put) {
        (Err(at), true) => list.insert(at, item),
        (Ok(at), false) => drop(list.remove(at)),
        _ => {}
    }
}

impl Catalog {
    /// Stages runtime progress without changing the declaration or its schema watermark.
    pub(crate) fn advance_schedule(
        &mut self,
        name: &str,
        due: u64,
        changes: &mut ChangeSet,
    ) -> Result<()> {
        changes.put_schedule(name, Some(due))?;
        if let Some(current) = self.schedules.get_mut(name) {
            *current = due;
            return Ok(());
        }
        self.schedules.insert(name.to_owned(), due);
        self.fit()
    }

    pub(crate) fn event_values(&self) -> impl Iterator<Item = &Event> {
        self.events.values().map(|e| &***e)
    }

    pub(crate) fn event(&self, key: &(Kind, String)) -> Option<&Event> {
        self.events.get(key).map(|e| &***e)
    }

    /// The table with this number.
    pub(crate) fn by_id(&self, id: TableId) -> Option<&Table> {
        self.table(&self.ids.get(&id)?.name)
    }

    /// The declarations of this kind ON the table with this number.
    pub(crate) fn on(&self, id: TableId, kind: Kind) -> impl Iterator<Item = &Event> {
        let keys = self.ids.get(&id).into_iter().flat_map(|on| &on.events);
        keys.filter(move |key| key.0 == kind)
            .filter_map(|key| self.event(key))
    }

    /// Every table with a reference that names `table`, in name order.
    pub(crate) fn children(&self, table: &Table) -> impl Iterator<Item = &Table> {
        let names = self.ids.get(&table.id).map_or(&[][..], |on| &on.children);
        names.iter().filter_map(|name| self.table(name))
    }

    /// Every table of edges.
    pub(crate) fn edges(&self) -> impl Iterator<Item = &Table> {
        self.edges.iter().filter_map(|id| self.by_id(*id))
    }

    fn empty(budget: &Budget) -> Result<Self> {
        let none = || budget.reserve(0, "loading declarations");
        Ok(Self {
            tables: BTreeMap::new(),
            events: BTreeMap::new(),
            schedules: BTreeMap::new(),
            ids: BTreeMap::new(),
            vectors: BTreeMap::new(),
            edges: Arc::new(Owned::charged(Vec::new(), none()?)),
            numbers: [0; 3],
            entries: 0,
            held: none()?,
        })
    }

    /// Loads each declaration under a temporary bound; keeps its counted allocation charge.
    pub(crate) fn load(view: &View, budget: &Budget) -> Result<Catalog> {
        let mut catalog = Self::empty(budget)?;
        let mut cover = budget.passing("decoding declaration values")?;
        // Every table comes before every event declaration, and an event type before a route.
        view.each_schema(|bytes| {
            let mut charge = budget.reserve(0, "loading declarations")?;
            if bytes.first() != Some(&(Kind::Table as u8)) {
                let (event, heap) = record::decode_event(bytes, view, &mut charge, &mut cover)?;
                if let EventDeclaration::Schedule(d) = &event
                    && d.every_ms == 0
                {
                    return Err(view.damaged("inconsistent schedule runtime state"));
                }
                catalog.put_event((event, heap), charge, budget)
            } else {
                let (mut table, _) = record::decode(bytes, view, &mut charge, &mut cover)?;
                table.stored = bytes.len() as u32;
                let shapes = table.shape_bytes();
                charge.grow(3 * shapes)?;
                let table = table.shaped();
                charge.shrink(2 * shapes);
                catalog.put(table, charge, budget)
            }
        })?;
        view.deliveries(
            (Included(&[SCHEDULE][..]), Excluded(&[SCHEDULE + 1][..])),
            &mut |key, value| {
                let Record::Schedule(name, due) = view.runtime_record(key, value)? else {
                    unreachable!()
                };
                catalog.schedules.insert(name.to_owned(), due);
                Ok(true)
            },
        )?;
        // A number an owed range names is not given again while it owes.
        for (source, n) in catalog.numbers.iter_mut().enumerate() {
            let range = [source as u8, source as u8 + 1].map(|at| [OWED, at]);
            view.deliveries(
                (Included(&range[0][..]), Excluded(&range[1][..])),
                &mut |key, _| {
                    let id = key
                        .get(2..6)
                        .map_or(0, |id| u32::from_be_bytes(id.try_into().unwrap()));
                    *n = (*n).max(id + 1);
                    Ok(true)
                },
            )?;
        }
        // A reference may name a table loaded after its own.
        let _copy = budget.reserve(catalog.pointers(), "loading declarations")?;
        for table in catalog.tables.clone().into_values() {
            catalog.relate(&table, true, budget)?;
        }
        catalog.fit()?;
        Ok(catalog)
    }

    /// A version that shares every entry with this one.
    pub(crate) fn next(&self, budget: &Budget) -> Result<Self> {
        Ok(Self {
            held: budget.reserve(self.pointers(), "copying declarations")?,
            tables: self.tables.clone(),
            events: self.events.clone(),
            schedules: self.schedules.clone(),
            ids: self.ids.clone(),
            vectors: self.vectors.clone(),
            edges: self.edges.clone(),
            numbers: self.numbers,
            entries: self.entries,
        })
    }

    /// A version's own bytes: its maps' nodes, a map's first leaf as four, and their keys.
    fn pointers(&self) -> u64 {
        let nodes = |len: usize, node: u64| (len + 3 * len.min(1)) as u64 * node;
        let events = self.events.keys().map(|(_, name)| name);
        let keys = self.tables.keys().chain(self.schedules.keys());
        keys.chain(events).map(|key| key.len() as u64).sum::<u64>()
            + nodes(self.tables.len(), TABLE_NODE)
            + nodes(self.ids.len(), ID_NODE)
            + nodes(self.events.len(), EVENT_NODE)
            + nodes(self.schedules.len(), SCHEDULE_NODE)
            + nodes(self.vectors.len(), VECTOR_NODE)
    }

    /// Totals the entries and holds what [`Self::pointers`] counts, after a change.
    pub(crate) fn fit(&mut self) -> Result<()> {
        let tables = self.tables.values().map(|table| table.bytes());
        let events = self.events.values().map(|event| event.bytes());
        let ons = self.ids.values().map(|on| on.bytes());
        self.entries = tables.chain(events).chain(ons).sum::<u64>() + self.edges.bytes();
        let (held, wanted) = (self.held.bytes(), self.pointers());
        self.held.grow(wanted.saturating_sub(held))?;
        self.held.shrink(held.saturating_sub(wanted));
        Ok(())
    }

    /// Puts a table, charged `held`, in place of the one of its name.
    fn put(&mut self, table: Table, held: Reservation, budget: &Budget) -> Result<()> {
        let table = Owned::shared(table, held)?;
        if let Some(old) = self.tables.insert(table.name.clone(), table.clone()) {
            self.relate(&old, false, budget)?;
        }
        if !self.ids.contains_key(&table.id) {
            let name = table.name.clone();
            self.edit(table.id, budget, |on| on.name = name)?;
        }
        self.relate(&table, true, budget)
    }

    /// Takes a table out, with what a commit looks up for it.
    fn take(&mut self, name: &str, budget: &Budget) -> Result<()> {
        let old = self.tables.remove(name).expect("a declared table");
        self.ids.remove(&old.id);
        self.relate(&old, false, budget)
    }

    /// Puts or takes out a table's VECTOR columns, edges and references; numbers count past it.
    fn relate(&mut self, table: &Table, put: bool, budget: &Budget) -> Result<()> {
        for vector in &table.vectors {
            match put {
                true => self.vectors.insert(vector.id, table.id),
                false => self.vectors.remove(&vector.id),
            };
        }
        let mut indexes = table.meta.indexes.iter();
        if indexes.any(|index| index.name.starts_with("__graph_edge_")) {
            let mut edges = Vec::clone(&self.edges);
            toggle(&mut edges, table.id, put);
            let held = budget.reserve(size_of_val(&edges[..]) as u64, "a declaration")?;
            self.edges = Arc::new(Owned::charged(edges, held));
        }
        for reference in &table.rules.references {
            if let Some(parent) = self.table(&reference.parent).map(|parent| parent.id) {
                let child = table.name.clone();
                self.edit(parent, budget, |on| toggle(&mut on.children, child, put))?;
            }
        }
        let [vectors, indexes, counts] = &mut self.numbers;
        let past = |n: &mut u32, given: Option<u32>| *n = (*n).max(given.map_or(0, |g| g + 1));
        past(vectors, table.vectors.iter().map(|v| v.id.0).max());
        past(indexes, table.index_ids.iter().map(|i| i.0).max());
        past(counts, table.rules.counts.iter().map(|c| c.id.0).max());
        Ok(())
    }

    /// Puts an event declaration, charged `held`, ON its table.
    fn put_event(&mut self, event: Event, held: Reservation, budget: &Budget) -> Result<()> {
        self.mention(&event.0, true, budget)?;
        let (kind, name) = event.0.identity();
        let key = (kind, name.to_owned());
        self.events.insert(key, Owned::shared(event, held)?);
        Ok(())
    }

    /// Takes an event declaration out.
    fn take_event(&mut self, key: &(Kind, String), budget: &Budget) -> Result<()> {
        let old = self.events.remove(key).expect("a declared event");
        self.mention(&old.0, false, budget)
    }

    /// Names a trigger, an event type or a route ON the table it is of, or no longer.
    fn mention(&mut self, event: &EventDeclaration, put: bool, budget: &Budget) -> Result<()> {
        use EventDeclaration::{EventType, Route, Trigger};
        let table = match event {
            Trigger(d) => &d.table,
            EventType(d) => &d.table,
            Route(d) => match self.event(&(Kind::EventType, d.event_type.clone())) {
                Some((EventType(d), _)) => &d.table,
                _ => return Ok(()),
            },
            _ => return Ok(()),
        };
        let Some(id) = self.table(table).map(|table| table.id) else {
            return Ok(());
        };
        let (kind, name) = event.identity();
        let key = (kind, name.to_owned());
        self.edit(id, budget, |on| toggle(&mut on.events, key, put))
    }

    /// Puts what a commit looks up for a table, as `change` leaves it, as a new entry.
    fn edit(&mut self, id: TableId, budget: &Budget, change: impl FnOnce(&mut On)) -> Result<()> {
        let on = self.ids.get(&id);
        let mut on = on.map_or_else(On::default, |on| On::clone(on));
        change(&mut on);
        let slot = size_of::<(Kind, String)>() as u64;
        let names = on.events.iter().map(|(_, n)| n).chain(&on.children);
        let bytes = names.chain([&on.name]).map(|n| n.len() as u64 + slot);
        let held = budget.reserve(bytes.sum(), "a declaration")?;
        self.ids.insert(id, Owned::shared(on, held)?);
        Ok(())
    }

    /// A table by name, as SQL names it.
    pub(crate) fn table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name).map(|t| &***t)
    }

    /// Starts retention now on each table the `plans` declared that keeps rows for a time.
    pub(crate) fn stamp_retention<'p>(
        &mut self,
        plans: impl Iterator<Item = &'p Plan>,
        changes: &mut ChangeSet,
        now: u32,
        budget: &Budget,
    ) -> Result<()> {
        for name in plans.filter_map(subject) {
            let stamped = |t: &&Table| t.meta.retain.is_some() && t.rules.retain_since.is_none();
            let Some(mut table) = self.table(name).filter(stamped).cloned() else {
                continue;
            };
            table.rules.retain_since = Some(now);
            changes.put_schema(&Kind::Table.key(name), record::encode(&mut table))?;
            let held = budget.reserve(table.heap, "a declaration")?;
            self.put(table, held, budget)?;
        }
        self.fit()
    }

    /// Every table, in name order.
    pub(crate) fn tables(&self) -> Vec<&Table> {
        self.tables.values().map(|t| &***t).collect()
    }

    pub(crate) fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// The name of every table, in name order.
    pub(crate) fn table_names(
        &self,
        budget: &Budget,
        allowed: Option<&std::collections::BTreeSet<String>>,
    ) -> Result<Owned<Vec<String>>> {
        let held = self.answering(budget, 1)?;
        let names = self
            .tables
            .keys()
            .filter(|n| allowed.is_none_or(|a| a.contains(*n)))
            .cloned()
            .collect();
        Ok(Owned::charged(names, held))
    }

    /// Holds `copies` of the whole catalog: this version's nodes and keys, and every entry.
    pub(crate) fn answering(&self, budget: &Budget, copies: u64) -> Result<Reservation> {
        let whole = self.held.bytes() + self.entries;
        budget.reserve(copies * whole, "catalog answer")
    }

    /// A table's declaration as `.schema` prints it, or `TableNotFound`.
    /// The text, run again, declares the same table.
    pub(crate) fn declaration(&self, name: &str, budget: &Budget) -> Result<Owned<String>> {
        let name = crate::sql::identifier(name);
        let table = self
            .table(name)
            .ok_or_else(|| Error::TableNotFound(name.to_owned()))?;
        let mut held =
            budget.reserve(SCHEMA * u64::from(table.stored), "rendering a declaration")?;
        let text = render::declaration(table);
        held.shrink(held.bytes().saturating_sub(text.capacity() as u64));
        Ok(Owned::charged(text, held))
    }

    /// Projects a supplied declaration through the same metadata as stored tables.
    pub(crate) fn declared_table(create: &CreateTablePlan) -> Result<(String, TableMeta)> {
        let budget = Budget::new(None)?;
        let mut catalog = Self::empty(&budget)?;
        let table = check::new_table(create, None, &budget)?;
        catalog.put(table, budget.reserve(0, "a declaration")?, &budget)?;
        Ok((create.name.clone(), catalog.table_meta(&create.name)?))
    }

    /// [`Self::table_meta`] as an answer charged to `budget`, which holds twice the table's heap.
    pub(crate) fn meta_answer(&self, name: &str, budget: &Budget) -> Result<Owned<TableMeta>> {
        let name = crate::sql::identifier(name);
        let heap = self.table(name).map_or(0, |t| t.heap);
        let held = budget.reserve(2 * heap, "table metadata answer")?;
        Ok(Owned::charged(self.table_meta(name)?, held))
    }

    /// The public typed projection of a stored table declaration.
    pub(crate) fn table_meta(&self, name: &str) -> Result<TableMeta> {
        let table = self
            .table(name)
            .ok_or_else(|| Error::TableNotFound(name.to_owned()))?;
        let mut meta = table.meta.clone();
        meta.delivery_manifest = table.rules.delivery_manifest.clone();
        meta.delivery_manifest.sort();
        meta.delivery_manifest.dedup();
        meta.edge_discard = table.rules.edge_discard;
        let name = |&at: &usize| meta.columns[at].name.clone();
        meta.immutable_columns = table.rules.immutable_columns.iter().map(name).collect();
        meta.expires_column = table.rules.expires_column.as_ref().map(name);
        meta.propagation_rules = table
            .rules
            .references
            .iter()
            .filter(|r| r.on_state.is_some() || r.follows_parent_purge)
            .map(|r| {
                let columns = r
                    .columns
                    .iter()
                    .map(|&at| meta.columns[at].name.clone())
                    .collect();
                let rule = sql::ast::Reference {
                    table: r.parent.clone(),
                    columns: r.parent_columns.clone(),
                    on_state: r.on_state.clone(),
                    follows_parent_purge: r.follows_parent_purge,
                };
                (columns, rule)
            })
            .collect();
        meta.propagate_edges = table.rules.propagate_edges.clone();
        meta.exclude_vector_states = table.rules.exclude_vector_states.clone();
        meta.vectors = table
            .vectors
            .iter()
            .map(|v| crate::types::VectorOptions {
                column: v.column.clone(),
                partition_key_columns: v.partition_key.clone(),
                max_partitions: v.max_partitions,
                search_mode: v.search_mode,
                quantization: v.quantization,
            })
            .collect();
        meta.scope_label = table
            .rules
            .scope_labels
            .as_ref()
            .map(|(at, labels)| (meta.columns[*at].name.clone(), labels.clone()));
        let key = meta.columns.iter_mut().filter(|column| column.primary_key);
        key.for_each(|column| column.nullable = false);
        Ok(meta)
    }

    /// The table that has the VECTOR column with this number, and what it keeps about it.
    pub(crate) fn vector(&self, id: VectorId) -> Option<(&Table, &VectorColumn)> {
        let table = self.by_id(*self.vectors.get(&id)?)?;
        Some((table, table.vectors.iter().find(|vector| vector.id == id)?))
    }
}

/// The table a table or index declaration is of.
pub(crate) fn subject(plan: &Plan) -> Option<&String> {
    match plan {
        Plan::CreateTable(create) => Some(&create.name),
        Plan::CreateIndex(create) => Some(&create.table),
        Plan::AlterTable(alter) => Some(&alter.table),
        Plan::DropTable(drop) => Some(&drop.name),
        Plan::DropIndex(drop) => drop.table.as_ref(),
        _ => None,
    }
}

/// Applies one table, index, event or schedule declaration to a change set.
///
/// Checks the declaration against the catalog, stages its schema record and returns the
/// catalog as it will be once the change set commits. Stages no row and no index entry; after
/// a CREATE INDEX on a table that has rows, `database` asks `write` to fill the index. `now`
/// is the time of the statement, which a row written before an added column reads where the
/// column's default is `NOW()`.
pub(crate) fn declare(
    plan: &Plan,
    catalog: &Catalog,
    changes: &mut ChangeSet,
    now: i64,
    budget: &crate::memory::Budget,
) -> Result<Declared> {
    use EventDeclaration::*;
    let copied = match plan {
        Plan::CreateIndex(c) => catalog.table(&c.table).map_or(0, |t| t.stored),
        _ => 0,
    };
    let _copying = budget.reserve(
        SCHEMA * u64::from(copied),
        "assembling an index declaration",
    )?;
    if matches!(
        plan,
        Plan::AlterTable(_) | Plan::DropTable(_) | Plan::DropIndex(_)
    ) {
        return alter::declare(plan, catalog, changes, now, budget);
    }
    let mut next = catalog.next(budget)?;
    let mut table = match plan {
        Plan::DeclareEvent(event) => {
            let (kind, name) = event.identity();
            let noun = kind.words().0;
            let key = (kind, name.to_owned());
            let table = match event {
                Trigger(d) => Some(&d.table),
                EventType(d) => Some(&d.table),
                _ => None,
            };
            if let Some(table) = table {
                catalog
                    .table(table)
                    .ok_or_else(|| Error::TableNotFound(table.clone()))?;
            }
            if let Route(d) = event {
                for (kind, name) in [(Kind::EventType, &d.event_type), (Kind::Sink, &d.sink)] {
                    if !catalog.events.contains_key(&(kind, name.clone())) {
                        let noun = kind.words().0;
                        return Err(check::invalid(format!("{noun} '{name}' does not exist")));
                    }
                }
                let of = &catalog.events[&(Kind::EventType, d.event_type.clone())].0;
                if let (Some(f), EventType(e)) = (&d.where_in, of) {
                    catalog.table(&e.table).unwrap().column(&f.column)?;
                }
            }
            if catalog.events.contains_key(&key) && kind != Kind::Schedule {
                return Err(check::invalid(format!("{noun} '{name}' already exists")));
            }
            let (record, heap) = record::encode_event(event);
            changes.put_schema(&kind.key(name), record)?;
            let held = budget.reserve(heap, "a declaration")?;
            next.put_event((event.clone(), heap), held, budget)?;
            if let Schedule(d) = event {
                next.advance_schedule(
                    name,
                    (now.max(0) as u64).saturating_add(d.every_ms),
                    changes,
                )?;
            }
            return Ok(next.into());
        }
        Plan::DropEvent {
            kind,
            name,
            if_exists,
            table,
        } => {
            let key = (*kind, name.clone());
            let found = catalog
                .event(&key)
                .is_some_and(|(event, _)| match (event, table) {
                    (Trigger(d), Some(table)) => d.table == *table,
                    _ => true,
                });
            if !found {
                if *if_exists {
                    return Ok(next.into());
                }
                let noun = kind.words().0;
                let on = table
                    .as_ref()
                    .map(|t| format!(" for table '{t}'"))
                    .unwrap_or_default();
                return Err(check::invalid(format!(
                    "{noun} '{name}'{on} does not exist"
                )));
            }
            next.take_event(&key, budget)?;
            if *kind == Kind::Schedule {
                next.schedules.remove(name);
                changes.put_schedule(name, None)?;
            }
            changes.delete_schema(&kind.key(name))?;
            return Ok(next.into());
        }
        Plan::CreateTable(create) if !next.tables.contains_key(&create.name) => {
            let mut table = check::new_table(create, Some(catalog), budget)?;
            table.id = changes.take_table_number()?;
            table
        }
        // A taken name is settled before the rest of the declaration is looked at.
        Plan::CreateTable(create) if create.if_not_exists => {
            return Ok(next.into());
        }
        Plan::CreateTable(create) => {
            return Err(Error::SchemaInvalid {
                reason: format!(
                    "table '{}' already exists: CREATE TABLE never redefines a table, so nothing \
                     was changed; CREATE TABLE IF NOT EXISTS leaves an existing table as it is",
                    create.name
                ),
            });
        }
        Plan::CreateIndex(create) => check::with_index(create, catalog)?,
        _ => unreachable!("a declaration is dispatched above"),
    };
    // The counts a CREATE INDEX made on its table hold no record yet.
    let old = catalog.table(&table.name);
    let before = |id| old.is_some_and(|old| old.rules.counts.iter().any(|count| count.id == id));
    let made = table.rules.counts.iter().map(|count| count.id);
    let counts = made.filter(|id| !before(*id)).collect();
    changes.put_schema(&Kind::Table.key(&table.name), record::encode(&mut table))?;
    let held = budget.reserve(table.heap, "a declaration")?;
    next.put(table.shaped(), held, budget)?;
    let mut declared = Declared::from(next);
    declared.counts = counts;
    Ok(declared)
}
