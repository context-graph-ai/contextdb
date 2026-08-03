//! Sync contract types for contextDB change-tracking and replication.
//!
//! These types define the public API for sync operations and the durable wire
//! shape used by file-backed history, full snapshots, and server replication.

use contextdb_core::{
    CompositeForeignKey, Lsn, RowId, SingleColumnForeignKey, TableMeta, Value, VectorIndexRef,
};
use serde::de::VariantAccess;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

/// The durable direction declarations that governed each table generation.
///
/// A table name can be dropped and reused. Resolving only its current value
/// would make a former `SYNC OFF` generation's rows/vectors eligible once the
/// live metadata disappears, so each outbound data entry is resolved at its
/// own LSN instead. Schema declarations themselves always travel intact.
#[derive(Debug, Clone, Default)]
pub(crate) struct SyncDirectionHistory {
    events: HashMap<String, Vec<SyncDirectionHistoryEvent>>,
}

#[derive(Debug, Clone, Copy)]
enum SyncDirectionHistoryEvent {
    Set { lsn: Lsn, direction: SyncDirection },
    Drop { lsn: Lsn },
}

impl SyncDirectionHistory {
    pub(crate) fn record_create(&mut self, table: String, lsn: Lsn, direction: SyncDirection) {
        self.events
            .entry(table)
            .or_default()
            .push(SyncDirectionHistoryEvent::Set { lsn, direction });
    }

    pub(crate) fn record_alter(&mut self, table: String, lsn: Lsn, direction: SyncDirection) {
        self.events
            .entry(table)
            .or_default()
            .push(SyncDirectionHistoryEvent::Set { lsn, direction });
    }

    pub(crate) fn record_drop(&mut self, table: String, lsn: Lsn) {
        self.events
            .entry(table)
            .or_default()
            .push(SyncDirectionHistoryEvent::Drop { lsn });
    }

    pub(crate) fn includes(&self, table: &str, lsn: Lsn, include: &[SyncDirection]) -> bool {
        include.contains(&self.direction_at(table, lsn))
    }

    fn direction_at(&self, table: &str, lsn: Lsn) -> SyncDirection {
        let Some(events) = self.events.get(table) else {
            return SyncDirection::Both;
        };
        let mut direction = SyncDirection::Both;
        for event in events {
            match *event {
                SyncDirectionHistoryEvent::Set {
                    lsn: event_lsn,
                    direction: declared,
                } if event_lsn <= lsn => direction = declared,
                // The drop itself belongs to the generation it removes. A
                // later LSN starts with the default until another CREATE.
                SyncDirectionHistoryEvent::Drop { lsn: event_lsn } if event_lsn < lsn => {
                    direction = SyncDirection::Both;
                }
                _ => {}
            }
        }
        direction
    }
}

/// A set of changes extracted from a database since a given LSN.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChangeSet {
    pub rows: Vec<RowChange>,
    pub edges: Vec<EdgeChange>,
    pub vectors: Vec<VectorChange>,
    pub ddl: Vec<DdlChange>,
    #[serde(default)]
    pub ddl_lsn: Vec<Lsn>,
}

impl ChangeSet {
    pub fn validate_ddl_lsn_cardinality(&self) -> std::result::Result<(), String> {
        if self.ddl_lsn.len() == self.ddl.len() {
            return Ok(());
        }
        Err(format!(
            "invalid ChangeSet ddl_lsn length: got {}, expected {} for {} DDL entries",
            self.ddl_lsn.len(),
            self.ddl.len(),
            self.ddl.len()
        ))
    }

    pub fn max_lsn(&self) -> Option<Lsn> {
        self.max_data_lsn()
            .into_iter()
            .chain(self.ddl_lsn.iter().copied())
            .max()
    }

    pub fn max_data_lsn(&self) -> Option<Lsn> {
        self.rows
            .iter()
            .map(|row| row.lsn)
            .chain(self.edges.iter().map(|edge| edge.lsn))
            .chain(self.vectors.iter().map(|vector| vector.lsn))
            .max()
    }

    pub fn data_entry_count(&self) -> usize {
        self.rows.len() + self.edges.len() + self.vectors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data_entry_count() == 0 && self.ddl.is_empty()
    }

    pub fn has_create_trigger_ddl(&self) -> bool {
        self.ddl
            .iter()
            .any(|ddl| matches!(ddl, DdlChange::CreateTrigger { .. }))
    }

    pub fn split_at_trigger_bootstrap_barriers(self) -> Vec<ChangeSet> {
        let mut batches = Vec::new();
        let mut current = ChangeSet::default();

        for group in self.split_by_data_lsn() {
            if group.has_create_trigger_ddl() && group.data_entry_count() == 0 {
                if !current.is_empty() {
                    batches.push(std::mem::take(&mut current));
                }
                batches.push(group);
                continue;
            }
            current.rows.extend(group.rows);
            current.edges.extend(group.edges);
            current.vectors.extend(group.vectors);
            current.ddl.extend(group.ddl);
            current.ddl_lsn.extend(group.ddl_lsn);
        }

        if !current.is_empty() || batches.is_empty() {
            batches.push(current);
        }
        batches
    }

    pub fn split_by_data_lsn(self) -> Vec<ChangeSet> {
        let mut groups = BTreeMap::<Lsn, ChangeSet>::new();
        for row in self.rows {
            groups.entry(row.lsn).or_default().rows.push(row);
        }
        for edge in self.edges {
            groups.entry(edge.lsn).or_default().edges.push(edge);
        }
        for vector in self.vectors {
            groups.entry(vector.lsn).or_default().vectors.push(vector);
        }
        let fallback_ddl_lsn = groups.keys().next().copied();
        for (index, ddl) in self.ddl.into_iter().enumerate() {
            let Some(lsn) = self.ddl_lsn.get(index).copied().or(fallback_ddl_lsn) else {
                groups.entry(Lsn(0)).or_default().ddl.push(ddl);
                continue;
            };
            let group = groups.entry(lsn).or_default();
            group.ddl.push(ddl);
            group.ddl_lsn.push(lsn);
        }

        if groups.is_empty() {
            return vec![ChangeSet {
                rows: Vec::new(),
                edges: Vec::new(),
                vectors: Vec::new(),
                ddl: Vec::new(),
                ddl_lsn: Vec::new(),
            }];
        }
        groups.into_values().collect::<Vec<_>>()
    }

    /// Filters this changeset to only include tables matching the given directions.
    pub fn filter_by_direction(
        &self,
        directions: &HashMap<String, SyncDirection>,
        include: &[SyncDirection],
    ) -> ChangeSet {
        ChangeSet {
            rows: self
                .rows
                .iter()
                .filter(|row| {
                    let direction = directions
                        .get(&row.table)
                        .copied()
                        .unwrap_or(SyncDirection::Both);
                    include.contains(&direction)
                })
                .cloned()
                .collect(),
            edges: self.edges.clone(),
            vectors: self
                .vectors
                .iter()
                .filter(|vector| {
                    let direction = directions
                        .get(&vector.index.table)
                        .copied()
                        .unwrap_or(SyncDirection::Both);
                    include.contains(&direction)
                })
                .cloned()
                .collect(),
            // A receiver-side row filter cannot slice an authenticated schema
            // commit into a different vector. Source-side eligibility uses the
            // durable-history path below before any schema reaches the wire.
            ddl: self.ddl.clone(),
            ddl_lsn: self.ddl_lsn.clone(),
        }
    }

    /// Filters internal outbound work against the declaration that governed
    /// the entry's own table generation. This is intentionally crate-private:
    /// the wire and public `ChangeSet` API remain unchanged.
    pub(crate) fn filter_by_direction_history(
        &self,
        history: &SyncDirectionHistory,
        include: &[SyncDirection],
    ) -> ChangeSet {
        ChangeSet {
            rows: self
                .rows
                .iter()
                .filter(|row| history.includes(&row.table, row.lsn, include))
                .cloned()
                .collect(),
            edges: self.edges.clone(),
            vectors: self
                .vectors
                .iter()
                .filter(|vector| history.includes(&vector.index.table, vector.lsn, include))
                .cloned()
                .collect(),
            // Direction is a row-placement policy. The complete authenticated
            // schema vector still travels so every peer can enforce the
            // author's exact declaration, including SYNC OFF/PULL ONLY and a
            // transition that closes a formerly delivering table.
            ddl: self.ddl.clone(),
            ddl_lsn: self.ddl_lsn.clone(),
        }
    }
}

#[cfg(test)]
mod direction_history_tests {
    use super::*;

    fn create_table(name: &str) -> DdlChange {
        DdlChange::CreateTable {
            name: name.to_string(),
            columns: Vec::new(),
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        }
    }

    #[test]
    fn historical_direction_preserves_local_and_shared_schema_commits() {
        let mut history = SyncDirectionHistory::default();
        history.record_create("local".to_string(), Lsn(1), SyncDirection::None);
        history.record_drop("local".to_string(), Lsn(2));
        history.record_create("shared".to_string(), Lsn(2), SyncDirection::Both);
        let changes = ChangeSet {
            ddl: vec![
                DdlChange::CreateTable {
                    name: "local".to_string(),
                    columns: Vec::new(),
                    constraints: vec!["SYNC OFF".to_string()],
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
                DdlChange::DropTable {
                    name: "local".to_string(),
                },
                create_table("shared"),
            ],
            ddl_lsn: vec![Lsn(1), Lsn(2), Lsn(2)],
            ..ChangeSet::default()
        };

        let filtered = changes
            .filter_by_direction_history(&history, &[SyncDirection::Push, SyncDirection::Both]);

        assert_eq!(filtered.ddl, changes.ddl);
        assert_eq!(filtered.ddl_lsn, changes.ddl_lsn);
    }

    #[test]
    fn historical_direction_keeps_a_global_sink_for_a_later_shared_route() {
        let mut history = SyncDirectionHistory::default();
        history.record_create("shared".to_string(), Lsn(1), SyncDirection::Both);
        let changes = ChangeSet {
            ddl: vec![
                DdlChange::CreateSink {
                    name: "archive".to_string(),
                    sink_type: "CALLBACK".to_string(),
                    url: None,
                },
                DdlChange::CreateRoute {
                    name: "shared_archive".to_string(),
                    event_type: "shared_change".to_string(),
                    sink: "archive".to_string(),
                    table: "shared".to_string(),
                    where_in: None,
                },
            ],
            ddl_lsn: vec![Lsn(1), Lsn(2)],
            ..ChangeSet::default()
        };

        let filtered = changes
            .filter_by_direction_history(&history, &[SyncDirection::Push, SyncDirection::Both]);

        assert_eq!(filtered.ddl, changes.ddl);
        assert_eq!(filtered.ddl_lsn, changes.ddl_lsn);
    }

    #[test]
    fn historical_direction_preserves_closing_alter_and_new_sync_off_schema() {
        let mut history = SyncDirectionHistory::default();
        history.record_create("existing".to_string(), Lsn(1), SyncDirection::Both);
        history.record_alter("existing".to_string(), Lsn(2), SyncDirection::None);
        history.record_create("local".to_string(), Lsn(3), SyncDirection::None);
        let changes = ChangeSet {
            ddl: vec![
                DdlChange::AlterTable {
                    name: "existing".to_string(),
                    columns: Vec::new(),
                    constraints: vec!["SYNC OFF".to_string()],
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
                DdlChange::CreateTable {
                    name: "local".to_string(),
                    columns: Vec::new(),
                    constraints: vec!["SYNC OFF".to_string()],
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
            ],
            ddl_lsn: vec![Lsn(2), Lsn(3)],
            ..ChangeSet::default()
        };

        let filtered = changes.filter_by_direction_history(
            &history,
            &[
                SyncDirection::Push,
                SyncDirection::Pull,
                SyncDirection::Both,
            ],
        );

        assert_eq!(filtered.ddl, changes.ddl);
        assert_eq!(filtered.ddl_lsn, changes.ddl_lsn);
    }

    #[test]
    fn historical_direction_preserves_mixed_closing_alter_and_new_local_table() {
        let mut history = SyncDirectionHistory::default();
        history.record_create("existing".to_string(), Lsn(1), SyncDirection::Both);
        history.record_alter("existing".to_string(), Lsn(2), SyncDirection::None);
        history.record_create("local".to_string(), Lsn(2), SyncDirection::None);
        let changes = ChangeSet {
            ddl: vec![
                DdlChange::AlterTable {
                    name: "existing".to_string(),
                    columns: Vec::new(),
                    constraints: vec!["SYNC OFF".to_string()],
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
                DdlChange::CreateTable {
                    name: "local".to_string(),
                    columns: Vec::new(),
                    constraints: vec!["SYNC OFF".to_string()],
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                },
            ],
            ddl_lsn: vec![Lsn(2), Lsn(2)],
            ..ChangeSet::default()
        };

        let filtered = changes.filter_by_direction_history(
            &history,
            &[
                SyncDirection::Push,
                SyncDirection::Pull,
                SyncDirection::Both,
            ],
        );
        assert_eq!(filtered.ddl, changes.ddl);
        assert_eq!(filtered.ddl_lsn, changes.ddl_lsn);
    }

    #[test]
    fn historical_direction_preserves_drop_recreate_across_direction_boundary() {
        for (before, after) in [
            (SyncDirection::Both, SyncDirection::None),
            (SyncDirection::None, SyncDirection::Both),
        ] {
            let mut history = SyncDirectionHistory::default();
            history.record_create("reused".to_string(), Lsn(1), before);
            history.record_drop("reused".to_string(), Lsn(2));
            history.record_create("reused".to_string(), Lsn(2), after);
            let changes = ChangeSet {
                ddl: vec![
                    DdlChange::DropTable {
                        name: "reused".to_string(),
                    },
                    DdlChange::CreateTable {
                        name: "reused".to_string(),
                        columns: Vec::new(),
                        constraints: vec![after.sql().to_string()],
                        foreign_keys: Vec::new(),
                        composite_foreign_keys: Vec::new(),
                        composite_unique: Vec::new(),
                    },
                ],
                ddl_lsn: vec![Lsn(2), Lsn(2)],
                ..ChangeSet::default()
            };

            let filtered = changes.filter_by_direction_history(
                &history,
                &[
                    SyncDirection::Push,
                    SyncDirection::Pull,
                    SyncDirection::Both,
                ],
            );
            assert_eq!(filtered.ddl, changes.ddl);
            assert_eq!(filtered.ddl_lsn, changes.ddl_lsn);
        }
    }
}

/// Every column that makes up a table's sync identity, in the order the
/// identity is declared. A row is told apart from another by ALL of these
/// columns together — for a table-level `PRIMARY KEY (a, b, ...)` that is every
/// listed column, in declaration order. Precedence matches the single-column
/// resolver: an explicit override, then a multi-column primary key, then a
/// single-column primary key, then a literal `id` column. `None` means the
/// table has no identity and its rows are keyless-skipped from sync.
///
/// Public (not `pub(crate)`): a sync-eligibility caller outside this crate
/// (e.g. `contextdb-server`'s push-time refusal for a keyless table with no
/// usable identity) consumes this SAME resolver rather than re-deriving the
/// precedence rule on its own.
pub fn natural_key_columns_for_meta(meta: &TableMeta) -> Option<Vec<String>> {
    if let Some(column) = &meta.natural_key_column {
        return Some(vec![column.clone()]);
    }
    if !meta.primary_key_columns.is_empty() {
        return Some(meta.primary_key_columns.clone());
    }
    if let Some(column) = meta.columns.iter().find(|column| column.primary_key) {
        return Some(vec![column.name.clone()]);
    }
    if meta.columns.iter().any(|column| column.name == "id") {
        return Some(vec!["id".to_string()]);
    }
    None
}

/// The leading identity column — the first of [`natural_key_columns_for_meta`].
/// Callers that only need to know a table HAS an identity (the `SYNC SAFE`
/// gate) or that name the leading column keep using this; the full identity is
/// resolved through the columns variant above.
pub(crate) fn natural_key_column_for_meta(meta: &TableMeta) -> Option<String> {
    natural_key_columns_for_meta(meta).map(|mut columns| columns.remove(0))
}

/// Build the whole identity of a row from a table's key columns and the row's
/// values. `None` when the table is keyless or the row is missing a key column.
pub(crate) fn natural_key_from_row_values(
    meta: &TableMeta,
    values: &HashMap<String, Value>,
) -> Option<NaturalKey> {
    let columns = natural_key_columns_for_meta(meta)?;
    let mut pairs = Vec::with_capacity(columns.len());
    for column in columns {
        let value = values.get(&column)?.clone();
        pairs.push((column, value));
    }
    NaturalKey::from_pairs(pairs)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowChange {
    pub table: String,
    pub natural_key: NaturalKey,
    pub values: HashMap<String, Value>,
    pub deleted: bool,
    pub lsn: Lsn,
    /// When the row was WRITTEN, on the clock of the node that wrote it.
    /// Retention judges a row by this carried stamp against the holder's own
    /// clock, so a row keeps its age when it moves: a hub does not hand a
    /// day-old backlog a fresh retention window just because it arrived
    /// today, and apply-queue latency never extends a row's life. `None` for
    /// a row whose origin recorded no stamp (a delete tombstone, or a peer
    /// older than this field), in which case the receiver stamps its own.
    #[serde(default)]
    pub created_at: Option<contextdb_core::Wallclock>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeChange {
    pub source: Uuid,
    pub target: Uuid,
    pub edge_type: String,
    pub properties: HashMap<String, Value>,
    pub lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorChange {
    pub index: VectorIndexRef,
    pub row_id: RowId,
    pub vector: Vec<f32>,
    pub lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum DdlChange {
    CreateTable {
        name: String,
        columns: Vec<(String, String)>,
        constraints: Vec<String>,
        foreign_keys: Vec<SingleColumnForeignKey>,
        composite_foreign_keys: Vec<CompositeForeignKey>,
        composite_unique: Vec<Vec<String>>,
    },
    DropTable {
        name: String,
    },
    AlterTable {
        name: String,
        columns: Vec<(String, String)>,
        constraints: Vec<String>,
        foreign_keys: Vec<SingleColumnForeignKey>,
        composite_foreign_keys: Vec<CompositeForeignKey>,
        composite_unique: Vec<Vec<String>>,
    },
    CreateIndex {
        table: String,
        name: String,
        columns: Vec<(String, contextdb_core::SortDirection)>,
    },
    DropIndex {
        table: String,
        name: String,
    },
    CreateTrigger {
        name: String,
        table: String,
        on_events: Vec<String>,
    },
    DropTrigger {
        name: String,
    },
    CreateEventType {
        name: String,
        trigger: String,
        table: String,
    },
    CreateSink {
        name: String,
        sink_type: String,
        url: Option<String>,
    },
    CreateRoute {
        name: String,
        event_type: String,
        sink: String,
        table: String,
        where_in: Option<(String, Vec<String>)>,
    },
    DropRoute {
        name: String,
        table: String,
    },
}

impl<'de> Deserialize<'de> for DdlChange {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        enum Variant {
            CreateTable,
            DropTable,
            AlterTable,
            CreateIndex,
            DropIndex,
            CreateTrigger,
            DropTrigger,
            CreateEventType,
            CreateSink,
            CreateRoute,
            DropRoute,
        }

        struct DdlChangeVisitor;

        impl<'de> serde::de::Visitor<'de> for DdlChangeVisitor {
            type Value = DdlChange;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a DdlChange")
            }

            fn visit_enum<A>(self, data: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::EnumAccess<'de>,
            {
                let (variant, access) = data.variant::<Variant>()?;
                match variant {
                    Variant::CreateTable => {
                        let fields = access.newtype_variant::<TableDdlFields>()?;
                        Ok(DdlChange::CreateTable {
                            name: fields.name,
                            columns: fields.columns,
                            constraints: fields.constraints,
                            foreign_keys: fields.foreign_keys,
                            composite_foreign_keys: fields.composite_foreign_keys,
                            composite_unique: fields.composite_unique,
                        })
                    }
                    Variant::DropTable => {
                        let fields = access.newtype_variant::<DropTableFields>()?;
                        Ok(DdlChange::DropTable { name: fields.name })
                    }
                    Variant::AlterTable => {
                        let fields = access.newtype_variant::<TableDdlFields>()?;
                        Ok(DdlChange::AlterTable {
                            name: fields.name,
                            columns: fields.columns,
                            constraints: fields.constraints,
                            foreign_keys: fields.foreign_keys,
                            composite_foreign_keys: fields.composite_foreign_keys,
                            composite_unique: fields.composite_unique,
                        })
                    }
                    Variant::CreateIndex => {
                        let fields = access.newtype_variant::<CreateIndexFields>()?;
                        Ok(DdlChange::CreateIndex {
                            table: fields.table,
                            name: fields.name,
                            columns: fields.columns,
                        })
                    }
                    Variant::DropIndex => {
                        let fields = access.newtype_variant::<DropIndexFields>()?;
                        Ok(DdlChange::DropIndex {
                            table: fields.table,
                            name: fields.name,
                        })
                    }
                    Variant::CreateTrigger => {
                        let fields = access.newtype_variant::<CreateTriggerFields>()?;
                        Ok(DdlChange::CreateTrigger {
                            name: fields.name,
                            table: fields.table,
                            on_events: fields.on_events,
                        })
                    }
                    Variant::DropTrigger => {
                        let fields = access.newtype_variant::<DropTriggerFields>()?;
                        Ok(DdlChange::DropTrigger { name: fields.name })
                    }
                    Variant::CreateEventType => {
                        let fields = access.newtype_variant::<CreateEventTypeFields>()?;
                        Ok(DdlChange::CreateEventType {
                            name: fields.name,
                            trigger: fields.trigger,
                            table: fields.table,
                        })
                    }
                    Variant::CreateSink => {
                        let fields = access.newtype_variant::<CreateSinkFields>()?;
                        Ok(DdlChange::CreateSink {
                            name: fields.name,
                            sink_type: fields.sink_type,
                            url: fields.url,
                        })
                    }
                    Variant::CreateRoute => {
                        let fields = access.newtype_variant::<CreateRouteFields>()?;
                        Ok(DdlChange::CreateRoute {
                            name: fields.name,
                            event_type: fields.event_type,
                            sink: fields.sink,
                            table: fields.table,
                            where_in: fields.where_in,
                        })
                    }
                    Variant::DropRoute => {
                        let fields = access.newtype_variant::<DropRouteFields>()?;
                        Ok(DdlChange::DropRoute {
                            name: fields.name,
                            table: fields.table,
                        })
                    }
                }
            }
        }

        const VARIANTS: &[&str] = &[
            "CreateTable",
            "DropTable",
            "AlterTable",
            "CreateIndex",
            "DropIndex",
            "CreateTrigger",
            "DropTrigger",
            "CreateEventType",
            "CreateSink",
            "CreateRoute",
            "DropRoute",
        ];
        deserializer.deserialize_enum("DdlChange", VARIANTS, DdlChangeVisitor)
    }
}

#[derive(Debug)]
struct TableDdlFields {
    name: String,
    columns: Vec<(String, String)>,
    constraints: Vec<String>,
    foreign_keys: Vec<SingleColumnForeignKey>,
    composite_foreign_keys: Vec<CompositeForeignKey>,
    composite_unique: Vec<Vec<String>>,
}

impl<'de> Deserialize<'de> for TableDdlFields {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct TableDdlFieldsVisitor;

        impl<'de> serde::de::Visitor<'de> for TableDdlFieldsVisitor {
            type Value = TableDdlFields;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("table DDL fields")
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let name = seq
                    .next_element::<String>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let columns = seq
                    .next_element::<Vec<(String, String)>>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let constraints = seq
                    .next_element::<Vec<String>>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;
                // Bincode's serde representation for struct variants is not
                // self-describing, so old three-field table DDL records cannot
                // be distinguished from new records with trailing fields
                // without risking an EOF error. Treat sequence-form records as
                // legacy and let the engine enrich structured fields from
                // current TableMeta when serving DDL logs.
                Ok(TableDdlFields {
                    name,
                    columns,
                    constraints,
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                })
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut name = None;
                let mut columns = None;
                let mut constraints = None;
                let mut foreign_keys = None;
                let mut composite_foreign_keys = None;
                let mut composite_unique = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "name" => name = Some(map.next_value()?),
                        "columns" => columns = Some(map.next_value()?),
                        "constraints" => constraints = Some(map.next_value()?),
                        "foreign_keys" => foreign_keys = Some(map.next_value()?),
                        "composite_foreign_keys" => {
                            composite_foreign_keys = Some(map.next_value()?)
                        }
                        "composite_unique" => composite_unique = Some(map.next_value()?),
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }

                Ok(TableDdlFields {
                    name: name.ok_or_else(|| serde::de::Error::missing_field("name"))?,
                    columns: columns.ok_or_else(|| serde::de::Error::missing_field("columns"))?,
                    constraints: constraints
                        .ok_or_else(|| serde::de::Error::missing_field("constraints"))?,
                    foreign_keys: foreign_keys.unwrap_or_default(),
                    composite_foreign_keys: composite_foreign_keys.unwrap_or_default(),
                    composite_unique: composite_unique.unwrap_or_default(),
                })
            }
        }

        const FIELDS: &[&str] = &[
            "name",
            "columns",
            "constraints",
            "foreign_keys",
            "composite_foreign_keys",
            "composite_unique",
        ];
        deserializer.deserialize_struct("TableDdlFields", FIELDS, TableDdlFieldsVisitor)
    }
}

#[derive(Deserialize)]
struct DropTableFields {
    name: String,
}

#[derive(Deserialize)]
struct CreateIndexFields {
    table: String,
    name: String,
    columns: Vec<(String, contextdb_core::SortDirection)>,
}

#[derive(Deserialize)]
struct DropIndexFields {
    table: String,
    name: String,
}

#[derive(Deserialize)]
struct CreateTriggerFields {
    name: String,
    table: String,
    on_events: Vec<String>,
}

#[derive(Deserialize)]
struct DropTriggerFields {
    name: String,
}

#[derive(Deserialize)]
struct CreateEventTypeFields {
    name: String,
    trigger: String,
    table: String,
}

#[derive(Deserialize)]
struct CreateSinkFields {
    name: String,
    sink_type: String,
    url: Option<String>,
}

#[derive(Deserialize)]
struct CreateRouteFields {
    name: String,
    event_type: String,
    sink: String,
    #[serde(default)]
    table: String,
    where_in: Option<(String, Vec<String>)>,
}

#[derive(Deserialize)]
struct DropRouteFields {
    name: String,
    #[serde(default)]
    table: String,
}

/// A row's sync identity. `column`/`value` are the LEADING key column and its
/// value; `rest` carries every further key column, in declared order, each
/// paired with its own value. `rest` is a required field — msgpack encodes this
/// struct as a sequence, so a single-column key encodes as a three-element
/// sequence with an EMPTY `rest`, which is the one current shape. A multi-column
/// `PRIMARY KEY (a, b, c)` puts `a` in the flat fields and `b, c` in `rest` —
/// the whole tuple is what tells one row from another across machines. The
/// obsolete two-element encoding is rejected at decode.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NaturalKey {
    pub column: String,
    pub value: Value,
    pub rest: Vec<(String, Value)>,
}

impl NaturalKey {
    /// A single-column identity — the shape every keyless-`id` and
    /// single-`PRIMARY KEY` table carries.
    pub fn single(column: String, value: Value) -> Self {
        Self {
            column,
            value,
            rest: Vec::new(),
        }
    }

    /// Build an identity from its ordered `(column, value)` components. The
    /// first pair is the leading key; the remainder are `rest`. `None` when the
    /// component list is empty (a keyless row carries no identity).
    pub fn from_pairs(mut pairs: Vec<(String, Value)>) -> Option<Self> {
        if pairs.is_empty() {
            return None;
        }
        let (column, value) = pairs.remove(0);
        Some(Self {
            column,
            value,
            rest: pairs,
        })
    }

    /// The identity's column names in declared order (leading first).
    pub fn key_columns(&self) -> Vec<String> {
        let mut columns = Vec::with_capacity(1 + self.rest.len());
        columns.push(self.column.clone());
        columns.extend(self.rest.iter().map(|(column, _)| column.clone()));
        columns
    }

    /// The identity's `(column, value)` components in declared order (leading
    /// first) — a stable, whole-identity view for keying and diagnostics.
    pub fn pairs(&self) -> Vec<(String, Value)> {
        let mut pairs = Vec::with_capacity(1 + self.rest.len());
        pairs.push((self.column.clone(), self.value.clone()));
        pairs.extend(self.rest.iter().cloned());
        pairs
    }

    /// The identity's values in the same order as [`Self::key_columns`].
    pub fn key_values(&self) -> Vec<Value> {
        let mut values = Vec::with_capacity(1 + self.rest.len());
        values.push(self.value.clone());
        values.extend(self.rest.iter().map(|(_, value)| value.clone()));
        values
    }

    /// Whether a row's values carry this whole identity — every key column
    /// present and equal. The composite-aware replacement for a single
    /// `values.get(col) == Some(value)` check.
    pub fn matches_values(&self, values: &HashMap<String, Value>) -> bool {
        if values.get(&self.column) != Some(&self.value) {
            return false;
        }
        self.rest
            .iter()
            .all(|(column, value)| values.get(column) == Some(value))
    }
}

// Ordinary tables persist only the public KEEP FIRST / KEEP LATEST
// declaration in `TableMeta`. This engine-private type also carries the two
// role-relative mechanics used by ContextDB-owned system tables.
macro_rules! define_conflict_policy {
    ($visibility:vis) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        $visibility enum ConflictPolicy {
            InsertIfNotExists,
            ServerWins,
            EdgeWins,
            LatestWins,
        }
    };
}

#[cfg(feature = "test-seams")]
define_conflict_policy!(pub);
#[cfg(not(feature = "test-seams"))]
define_conflict_policy!(pub(crate));

#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictPolicies {
    pub per_table: HashMap<String, ConflictPolicy>,
    pub default: ConflictPolicy,
}

#[cfg(not(feature = "test-seams"))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ConflictPolicies {
    pub per_table: HashMap<String, ConflictPolicy>,
    pub default: ConflictPolicy,
}

#[cfg(feature = "test-seams")]
impl ConflictPolicies {
    pub fn uniform(policy: ConflictPolicy) -> Self {
        Self {
            per_table: HashMap::new(),
            default: policy,
        }
    }
}

#[cfg(not(feature = "test-seams"))]
impl ConflictPolicies {
    pub(crate) fn uniform(policy: ConflictPolicy) -> Self {
        Self {
            per_table: HashMap::new(),
            default: policy,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyResult {
    pub applied_rows: usize,
    pub skipped_rows: usize,
    pub conflicts: Vec<Conflict>,
    pub new_lsn: Lsn,
}

/// Whether a changeset being applied is a continuing pull against a stable,
/// already-adopted source, or a full re-fetch from LSN 0 that a client issued
/// right after detecting that its cursor's source (the serving store's
/// incarnation) changed — a hub wiped and rebuilt under the same transport
/// identity, the standard recovery flow.
///
/// The two cases arbitrate differently under `ConflictPolicy::LatestWins`.
/// On the stable path, a row's carried arrival is compared against this
/// store's existing sidecar — both minted by the SAME source, so the
/// comparison is meaningful. After a source change that comparison is
/// meaningless: the two incarnations mint arrival numbers from unrelated,
/// independently-reset counters, so a low arrival freshly stamped by a
/// rebuilt hub is not "older" than a high arrival recorded under the extinct
/// one — treating it as a stale echo would silently drop a row that reached
/// the rebuilt hub by sync from another edge (the standard rebuild-recovery
/// flow), breaking every-pushed-row-reaches-every-puller.
///
/// `ReadoptingSource` trusts the served value over any row whose CURRENT
/// committed value is itself unmodified since its last sync (a live
/// sidecar) — never over a row a local write has since diverged, which
/// clears its sidecar regardless of this mode (see
/// `Database::sync_source_lsn_updates`). A locally-authored value that was
/// never pushed anywhere therefore never reaches the served-always-wins
/// arm: it falls through to the same comparison the stable path uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncAdoption {
    /// The ordinary path: this store's cursor already addresses the source
    /// serving this changeset.
    Continuing,
    /// This changeset is the full re-fetch from LSN 0 issued because the
    /// cursor's source just changed. Every row in it belongs to the newly
    /// adopted source, for the whole duration of this apply.
    ReadoptingSource,
    /// A status probe proved this edge's previously re-sent work landed, but
    /// its acknowledgement was lost. The pull is the one safe place to
    /// resolve `AcceptedLocalPending` against that hub's current history.
    ConfirmedPendingReconciliation,
}

pub(crate) const PURGED_LINEAGE_CONFLICT_REASON: &str = "purged_lineage";
pub(crate) const REMOVED_GENERATION_CONFLICT_REASON: &str = "removed_generation";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conflict {
    pub natural_key: NaturalKey,
    #[cfg(feature = "test-seams")]
    pub resolution: ConflictPolicy,
    #[cfg(not(feature = "test-seams"))]
    pub(crate) resolution: ConflictPolicy,
    pub reason: Option<String>,
    /// Present for a terminal authenticated dependency-unit refusal. Ordinary
    /// conflict accounting keeps these unset so its established wire shape is
    /// unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutation_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub winning_author_node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hub_acceptance_position: Option<Lsn>,
}

// The direction vocabulary lives in `contextdb-core`, next to the `TableMeta`
// that persists it, so the DDL clause, the stored declaration and this filter
// can never drift into two different enums.
pub use contextdb_core::SyncDirection;
