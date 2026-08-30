//! The committed image a reading route owns after its source is released.
//!
//! Hydration hands over decoded state; this module turns that state into the
//! handle a bounded read runs against and into the owned projection a reader
//! answers image questions from. Both come from one place so the reading
//! routes cannot drift apart: the execution target is an ordinary engine
//! handle over the same rows, so it plans and decides exactly as the writer
//! does, and the projection is read back out of that same handle rather than
//! re-derived beside it.

use crate::cli_render::{render_column_type, render_table_meta};
use crate::database::{Database, MaintenancePolicy};
use crate::direct_file_reader::{
    DirectChangeState, DirectColumnReference, DirectConfigurationState, DirectEventTypeStatus,
    DirectEventsStatus, DirectFileReaderError, DirectImageMetadataKind, DirectImageState,
    DirectIndexColumn, DirectIndexDirection, DirectMaintenanceStatus, DirectMetadataBody,
    DirectMetadataRequest, DirectMetadataResponse, DirectOwnedImage, DirectPropagationRule,
    DirectRankPolicy, DirectReferencePropagation, DirectRetainPolicy, DirectRouteStatus,
    DirectScheduleStatus, DirectSchema, DirectSchemaColumn, DirectSchemaIndex,
    DirectScopeLabelKind, DirectSinkStatus, DirectSnapshot, DirectStateMachine, DirectStoredEdge,
    DirectStoredRow, DirectStoredVector, DirectSyncSource, DirectSyncState, DirectTableSyncPolicy,
    DirectTypedImageDigest, DirectVectorQuantization,
};
use crate::executor::ReadExecutionTarget;
use crate::persistence::ReadPersistenceImageParts;
use contextdb_core::read_contract::{
    DeadlineClock, MetadataPageVocabulary, OwnerReadCancellation, ReadFailure, ReadLimits,
};
use contextdb_core::{
    ColumnType, ContextId, Direction, Error, HistoryPolicy, IndexKind, Lsn, Principal,
    PropagationRule, Result, RetainUnit, ScopeLabel, ScopeLabelKind, SortDirection, SyncDirection,
    TxId, Value, VectorIndexRef, VectorQuantization,
};
use contextdb_planner::PhysicalPlan;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// Everything a direct session needs once its source is gone: the shared
/// execution target, the owned projection, and the identity of both.
pub(crate) struct CommittedImage {
    pub(crate) target: Arc<dyn ReadExecutionTarget>,
    pub(crate) image: Arc<DirectOwnedImage>,
    pub(crate) digest: DirectTypedImageDigest,
}

/// What a read session declared about who it reads as and which part of the
/// store it is looking at.
///
/// It is the same three declarations a writable open carries, because the
/// committed image enforces them the same way: the handle below is told, and
/// the engine's own row gate decides every row. Nothing here inspects a
/// statement.
#[derive(Debug, Clone, Default)]
struct DeclaredReadVisibility {
    contexts: Option<BTreeSet<ContextId>>,
    scope_labels: Option<BTreeSet<ScopeLabel>>,
    principal: Option<Principal>,
}

impl DeclaredReadVisibility {
    fn declares_anything(&self) -> bool {
        self.contexts.is_some() || self.scope_labels.is_some() || self.principal.is_some()
    }
}

impl crate::persistence::ReadPersistenceImage {
    /// Turn a hydrated image into the target a bounded read runs against,
    /// with the owned projection and identity that ride alongside it.
    pub(crate) fn into_committed_image(
        self,
        limits: ReadLimits,
        contexts: Option<BTreeSet<ContextId>>,
        scope_labels: Option<BTreeSet<ScopeLabel>>,
        principal: Option<Principal>,
    ) -> Result<CommittedImage> {
        build_committed_image(
            self.into_runtime_parts(),
            limits,
            DeclaredReadVisibility {
                contexts,
                scope_labels,
                principal,
            },
        )
    }
}

fn build_committed_image(
    parts: ReadPersistenceImageParts,
    _limits: ReadLimits,
    declared: DeclaredReadVisibility,
) -> Result<CommittedImage> {
    let graph_edges = project_edges(&parts);
    let vectors = project_vectors(&parts);
    let commit_index = parts
        .commit_index
        .iter()
        .map(|(lsn, tx)| (*lsn, *tx))
        .collect::<Vec<_>>();

    // The reader's own ceiling bounds each read, not the handle: seeding the
    // accountant with it would let a caller's request limit stand in for the
    // store's declared memory limit, and the image would then report the
    // reader's number as the store's own.
    let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
    let mut database =
        Database::open_committed_image(parts, Arc::new(crate::plugin::CorePlugin), accountant)?;

    // The image projection is read back before any narrowing is applied. It
    // answers what tables and columns EXIST, which is a declaration-level
    // question the store answers the same way to everyone; deciding which of
    // them a consumer should be shown is the consumer's own filtering, and
    // hiding a table here would make a narrowed session report a different
    // store rather than a narrowed view of the same one.
    let image = project_owned_image(&database, graph_edges, vectors, commit_index)?;
    let digest = typed_digest(&image)?;
    let image = DirectOwnedImage {
        snapshot: DirectSnapshot {
            image_id: digest.complete,
        },
        ..image
    };

    // ROWS are what the declaration governs, and the handle this target is
    // built on is told before it is published -- so every statement, of every
    // shape, and every cursor page taken from it runs through the same row
    // gate a writable handle opened with the same three declarations runs
    // through. A session that declared nothing changes nothing here.
    if declared.declares_anything() {
        database.declare_read_access(declared.contexts, declared.scope_labels, declared.principal);
    }

    Ok(CommittedImage {
        target: Arc::new(database) as Arc<dyn ReadExecutionTarget>,
        image: Arc::new(image),
        digest,
    })
}

fn project_edges(parts: &ReadPersistenceImageParts) -> Vec<DirectStoredEdge> {
    // One edge identity can carry several stored occurrences -- a later write
    // that adds properties does not erase the one that introduced the edge --
    // and an image holds current state, so the latest occurrence is the one
    // an edge reads as.
    let mut current = BTreeMap::new();
    for edge in &parts.forward_edges {
        if edge.deleted_tx.is_some() {
            continue;
        }
        let identity = (edge.edge_type.clone(), edge.source, edge.target);
        let order = (edge.lsn, edge.created_tx);
        match current.get(&identity) {
            Some((seen, _)) if *seen >= order => {}
            _ => {
                current.insert(identity, (order, edge));
            }
        }
    }
    let mut edges = current
        .into_values()
        .map(|(_, edge)| DirectStoredEdge {
            source: edge.source,
            target: edge.target,
            edge_type: edge.edge_type.clone(),
            properties: edge
                .properties
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        })
        .collect::<Vec<_>>();
    edges.sort_by(|left, right| {
        (&left.edge_type, left.source, left.target).cmp(&(
            &right.edge_type,
            right.source,
            right.target,
        ))
    });
    edges
}

fn project_vectors(parts: &ReadPersistenceImageParts) -> Vec<DirectStoredVector> {
    let mut vectors = parts
        .current_vectors
        .iter()
        .map(|((index, row_id), values)| DirectStoredVector {
            index: index.clone(),
            row_id: *row_id,
            quantization: quantization_for_index(parts, index),
            values: values.clone(),
        })
        .collect::<Vec<_>>();
    vectors.sort_by(|left, right| {
        (&left.index.table, &left.index.column, left.row_id).cmp(&(
            &right.index.table,
            &right.index.column,
            right.row_id,
        ))
    });
    vectors
}

fn quantization_for_index(
    parts: &ReadPersistenceImageParts,
    index: &VectorIndexRef,
) -> DirectVectorQuantization {
    parts
        .table_meta
        .get(&index.table)
        .and_then(|meta| {
            meta.columns
                .iter()
                .find(|column| column.name == index.column)
                .map(|column| direct_quantization(column.quantization))
        })
        .unwrap_or(DirectVectorQuantization::F32)
}

fn direct_quantization(quantization: VectorQuantization) -> DirectVectorQuantization {
    match quantization {
        VectorQuantization::F32 => DirectVectorQuantization::F32,
        VectorQuantization::SQ8 => DirectVectorQuantization::Sq8,
        VectorQuantization::SQ4 => DirectVectorQuantization::Sq4,
    }
}

fn project_owned_image(
    database: &Database,
    graph_edges: Vec<DirectStoredEdge>,
    vectors: Vec<DirectStoredVector>,
    commit_index: Vec<(Lsn, TxIdAlias)>,
) -> Result<DirectOwnedImage> {
    let snapshot_id = database.snapshot();
    let mut table_names = database.table_names();
    table_names.sort();

    let mut relational_rows = Vec::new();
    for table in &table_names {
        for row in database.scan(table, snapshot_id)? {
            relational_rows.push(DirectStoredRow {
                table: table.clone(),
                row_id: row.row_id,
                values: row.values.into_iter().collect(),
            });
        }
    }
    relational_rows
        .sort_by(|left, right| (&left.table, left.row_id).cmp(&(&right.table, right.row_id)));

    let schemas = table_names
        .iter()
        .filter_map(|table| {
            database
                .table_meta(table)
                .map(|meta| project_schema(table, &meta))
        })
        .collect::<Vec<_>>();

    let (changes, arrivals) = database.changes_since_with_arrivals(Lsn(0));
    let changes_state = DirectChangeState {
        current_lsn: database.current_lsn(),
        committed_watermark: database.committed_watermark(),
        next_tx: database.next_tx(),
        commit_index,
        rows: changes.rows.clone(),
        edges: changes.edges.clone(),
        vectors: changes.vectors.clone(),
        ddl: changes.ddl.clone(),
        ddl_lsn: changes.ddl_lsn.clone(),
    };

    let mut sources = arrivals
        .into_iter()
        .map(|(row_lsn, source_lsn)| DirectSyncSource {
            row_lsn,
            source_lsn,
        })
        .collect::<Vec<_>>();
    sources.sort_by_key(|source| source.row_lsn);
    let mut tables = schemas
        .iter()
        .map(|schema| DirectTableSyncPolicy {
            table: schema.table.clone(),
            direction: schema
                .sync_direction
                .clone()
                .unwrap_or_else(|| "two_way".to_owned()),
            conflict_policy: schema
                .conflict_policy
                .clone()
                .unwrap_or_else(|| "keep_first".to_owned()),
        })
        .collect::<Vec<_>>();
    tables.sort_by(|left, right| left.table.cmp(&right.table));
    let sync = DirectSyncState {
        watermark: database.sync_watermark(),
        sources,
        tables,
    };

    let configuration = DirectConfigurationState {
        memory_limit_bytes: database.memory_limit(),
        disk_limit_bytes: database.disk_limit(),
        schemas,
    };

    let events = project_events(database);

    // Runtime fields are the fresh handle's own: this session started no
    // maintenance and inherits no caller's policy. Retention and currency are
    // declarations, so they come from the image's own schemas.
    let status = database.maintenance_status();
    let maintenance = DirectMaintenanceStatus {
        policy: match status.policy {
            MaintenancePolicy::EngineOwned => "engine_owned".to_owned(),
            MaintenancePolicy::CallerDriven => "caller_driven".to_owned(),
        },
        running: false,
        retention_enabled: status.retention_enabled,
        currency_compaction_enabled: status.currency_compaction_enabled,
        active_maintenance_loops: 0,
    };

    Ok(DirectOwnedImage {
        snapshot: DirectSnapshot { image_id: [0; 32] },
        relational_rows,
        graph_edges,
        vectors,
        changes: changes_state,
        sync,
        configuration,
        events,
        maintenance,
    })
}

/// The event surface as the engine reports it right now.
///
/// One projection serves both the committed image and a live database,
/// because it is the same question: what event types, sinks, routes and
/// schedules does this store declare.
pub(crate) fn project_events(database: &Database) -> DirectEventsStatus {
    let bus = database.event_bus_status();
    DirectEventsStatus {
        event_types: bus
            .event_types
            .into_iter()
            .map(|event| DirectEventTypeStatus {
                name: event.name,
                trigger: event.trigger,
                table: event.table,
            })
            .collect(),
        sinks: bus
            .sinks
            .into_iter()
            .map(|sink| DirectSinkStatus {
                name: sink.name,
                sink_type: sink.sink_type,
                callback_registered: sink.callback_registered,
                delivered: sink.metrics.delivered,
                queued: sink.metrics.queued,
                retried: sink.metrics.retried,
                permanent_failures: sink.metrics.permanent_failures,
                examined: sink.metrics.examined,
            })
            .collect(),
        routes: bus
            .routes
            .into_iter()
            .map(|route| DirectRouteStatus {
                name: route.name,
                event_type: route.event_type,
                sink: route.sink,
            })
            .collect(),
        schedules: database
            .cron_status()
            .into_iter()
            .map(|schedule| DirectScheduleStatus {
                name: schedule.name,
                every: schedule.every_text,
                callback: schedule.callback,
                callback_registered: schedule.callback_registered,
                next_fire_at_ms: schedule.next_fire_at_ms,
                last_fire_at_ms: schedule.last_fire_at_ms,
                fire_count: schedule.fire_count,
            })
            .collect(),
    }
}

/// The persisted scope-label constraint as the read schema answer reports it.
/// The two forms stay apart: a Split declaration's read set is not its write
/// set, and a consumer verifying its own declaration must see which is which.
fn project_scope_label(kind: &ScopeLabelKind) -> DirectScopeLabelKind {
    match kind {
        ScopeLabelKind::Simple { write_labels } => DirectScopeLabelKind::Simple {
            write_labels: write_labels.clone(),
        },
        ScopeLabelKind::Split {
            read_labels,
            write_labels,
        } => DirectScopeLabelKind::Split {
            read_labels: read_labels.clone(),
            write_labels: write_labels.clone(),
        },
    }
}

pub(crate) fn project_schema(table: &str, meta: &contextdb_core::TableMeta) -> DirectSchema {
    let columns = meta
        .columns
        .iter()
        .map(|column| {
            let propagation = meta.propagation_rules.iter().find_map(|rule| match rule {
                PropagationRule::ForeignKey {
                    fk_column,
                    trigger_state,
                    target_state,
                    max_depth,
                    abort_on_failure,
                    ..
                } if fk_column == &column.name => Some(DirectReferencePropagation {
                    on_state: trigger_state.clone(),
                    set_state: target_state.clone(),
                    max_depth: *max_depth,
                    abort_on_failure: *abort_on_failure,
                }),
                _ => None,
            });
            DirectSchemaColumn {
                name: column.name.clone(),
                data_type: render_column_type(&column.column_type),
                nullable: column.nullable,
                primary_key: column.primary_key,
                unique: column.unique,
                immutable: column.immutable,
                expires: column.expires,
                default: column.default.clone(),
                references: column
                    .references
                    .as_ref()
                    .map(|reference| DirectColumnReference {
                        table: reference.table.clone(),
                        column: reference.column.clone(),
                        propagation,
                    }),
                quantization: matches!(column.column_type, ColumnType::Vector(_))
                    .then(|| direct_quantization(column.quantization)),
                rank: column.rank_policy.as_ref().map(|rank| DirectRankPolicy {
                    sort_key: rank.sort_key.clone(),
                    formula: rank.formula.clone(),
                    joined_table: rank.joined_table.clone(),
                    joined_column: rank.joined_column.clone(),
                }),
                scope_label: column.scope_label.as_ref().map(project_scope_label),
                acl_ref: column.acl_ref.as_ref().map(|acl| DirectColumnReference {
                    table: acl.ref_table.clone(),
                    column: acl.ref_column.clone(),
                    propagation: None,
                }),
            }
        })
        .collect::<Vec<_>>();
    let primary_key = if meta.primary_key_columns.is_empty() {
        meta.columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|column| column.name.clone())
            .collect()
    } else {
        meta.primary_key_columns.clone()
    };
    let indexes = meta
        .indexes
        .iter()
        .filter(|index| index.kind != IndexKind::Auto)
        .map(|index| DirectSchemaIndex {
            name: index.name.clone(),
            columns: index
                .columns
                .iter()
                .map(|(column, direction)| DirectIndexColumn {
                    column: column.clone(),
                    direction: match direction {
                        SortDirection::Asc => DirectIndexDirection::Asc,
                        SortDirection::Desc => DirectIndexDirection::Desc,
                    },
                })
                .collect(),
        })
        .collect();
    let state_machine = meta.state_machine.as_ref().map(|state| DirectStateMachine {
        column: state.column.clone(),
        transitions: state
            .transitions
            .iter()
            .map(|(from, to)| (from.clone(), to.clone()))
            .collect(),
    });
    let retain = meta.default_ttl_seconds.map(|seconds| {
        let (window, unit) = match meta.retain_declared_unit {
            Some(unit) => (seconds / unit.seconds_multiplier(), unit.sql().to_owned()),
            None => (seconds, RetainUnit::Seconds.sql().to_owned()),
        };
        DirectRetainPolicy {
            window,
            unit,
            seconds,
            sync_safe: meta.sync_safe,
        }
    });
    let history = meta.history_policy.map(|policy| match policy {
        HistoryPolicy::All => "ALL".to_owned(),
        HistoryPolicy::CurrentOnly => "CURRENT_ONLY".to_owned(),
    });
    let mut propagate = meta
        .propagation_rules
        .iter()
        .map(|rule| match rule {
            PropagationRule::Edge {
                edge_type,
                direction,
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } => DirectPropagationRule::Edge {
                edge_type: edge_type.clone(),
                direction: match direction {
                    Direction::Incoming => "INCOMING",
                    Direction::Outgoing => "OUTGOING",
                    Direction::Both => "BOTH",
                }
                .to_owned(),
                on_state: trigger_state.clone(),
                set_state: target_state.clone(),
                max_depth: *max_depth,
                abort_on_failure: *abort_on_failure,
            },
            PropagationRule::VectorExclusion { trigger_state } => {
                DirectPropagationRule::VectorExclusion {
                    on_state: trigger_state.clone(),
                }
            }
            PropagationRule::ForeignKey {
                fk_column,
                referenced_table,
                referenced_column,
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } => DirectPropagationRule::ForeignKey {
                column: fk_column.clone(),
                references_table: referenced_table.clone(),
                references_column: referenced_column.clone(),
                on_state: trigger_state.clone(),
                set_state: target_state.clone(),
                max_depth: *max_depth,
                abort_on_failure: *abort_on_failure,
            },
        })
        .collect::<Vec<_>>();
    propagate.sort_by_key(propagation_sort_key);
    DirectSchema {
        table: table.to_owned(),
        immutable: meta.immutable,
        columns,
        primary_key,
        indexes,
        state_machine,
        retain,
        history,
        sync_direction: meta.sync_direction.map(sync_direction_word),
        conflict_policy: conflict_policy_word(meta),
        dag_edge_types: meta.dag_edge_types.clone(),
        propagate,
        ddl: render_table_meta(table, meta),
    }
}

fn sync_direction_word(direction: SyncDirection) -> String {
    match direction {
        SyncDirection::None => "sync_off",
        SyncDirection::Push => "push_only",
        SyncDirection::Pull => "pull_only",
        SyncDirection::Both => "two_way",
    }
    .to_owned()
}

fn conflict_policy_word(meta: &contextdb_core::TableMeta) -> Option<String> {
    meta.conflict_policy
        .and_then(|policy| policy.declared_clause())
        .map(|clause| match clause {
            "SYNC CONFLICT KEEP FIRST" => "keep_first".to_owned(),
            "SYNC CONFLICT KEEP LATEST" => "keep_latest".to_owned(),
            other => other.to_owned(),
        })
}

fn propagation_sort_key(rule: &DirectPropagationRule) -> (u8, String, String) {
    match rule {
        DirectPropagationRule::Edge {
            edge_type,
            on_state,
            ..
        } => (0, on_state.clone(), edge_type.clone()),
        DirectPropagationRule::ForeignKey {
            column, on_state, ..
        } => (1, on_state.clone(), column.clone()),
        DirectPropagationRule::VectorExclusion { on_state } => (2, on_state.clone(), String::new()),
    }
}

/// Length-prefixed absorption, so two different shapes cannot hash alike.
#[derive(Default)]
struct FamilyDigest {
    hasher: blake3::Hasher,
}

impl FamilyDigest {
    fn new() -> Self {
        Self::default()
    }

    fn bytes(&mut self, bytes: &[u8]) -> &mut Self {
        self.hasher.update(&(bytes.len() as u64).to_le_bytes());
        self.hasher.update(bytes);
        self
    }

    fn text(&mut self, text: &str) -> &mut Self {
        self.bytes(text.as_bytes())
    }

    fn number(&mut self, value: u64) -> &mut Self {
        self.hasher.update(&value.to_le_bytes());
        self
    }

    fn flag(&mut self, value: bool) -> &mut Self {
        self.number(u64::from(value))
    }

    /// Absorb a collection by content rather than by the order it arrived
    /// in, and by each item's content rather than by the order its own maps
    /// happen to iterate. The durable encoder preserves map iteration order,
    /// which is per-process, so identity is taken over a key-sorted rendering
    /// instead.
    fn canonical_set<T: serde::Serialize>(&mut self, values: &[T]) -> Result<&mut Self> {
        let mut rendered = values
            .iter()
            .map(|value| {
                // Going through the generic document sorts every map by key;
                // serializing the typed value directly would keep whatever
                // order this process's maps happen to iterate in.
                serde_json::to_value(value)
                    .and_then(|document| serde_json::to_vec(&document))
                    .map_err(|error| {
                        Error::Other(format!("render committed image identity: {error}"))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        rendered.sort();
        self.number(rendered.len() as u64);
        for bytes in &rendered {
            self.bytes(bytes);
        }
        Ok(self)
    }

    fn finish(&self) -> [u8; 32] {
        *self.hasher.finalize().as_bytes()
    }
}

fn typed_digest(image: &DirectOwnedImage) -> Result<DirectTypedImageDigest> {
    let mut relational = FamilyDigest::new();
    for row in &image.relational_rows {
        relational.text(&row.table).number(row.row_id.0);
        for (column, value) in &row.values {
            relational.text(column);
            relational.canonical_set(std::slice::from_ref(value))?;
        }
    }

    let mut graph = FamilyDigest::new();
    for edge in &image.graph_edges {
        graph
            .text(&edge.edge_type)
            .bytes(edge.source.as_bytes())
            .bytes(edge.target.as_bytes());
        for (name, value) in &edge.properties {
            graph.text(name);
            graph.canonical_set(std::slice::from_ref(value))?;
        }
    }

    let mut families = [
        FamilyDigest::new(),
        FamilyDigest::new(),
        FamilyDigest::new(),
    ];
    for vector in &image.vectors {
        let family = match vector.quantization {
            DirectVectorQuantization::F32 => &mut families[0],
            DirectVectorQuantization::Sq8 => &mut families[1],
            DirectVectorQuantization::Sq4 => &mut families[2],
        };
        family
            .text(&vector.index.table)
            .text(&vector.index.column)
            .number(vector.row_id.0)
            .number(vector.values.len() as u64);
        for component in &vector.values {
            family.number(u64::from(component.to_bits()));
        }
    }

    let mut schema_and_indexes = FamilyDigest::new();
    let mut policy = FamilyDigest::new();
    for schema in &image.configuration.schemas {
        schema_and_indexes
            .text(&schema.table)
            .flag(schema.immutable);
        for column in &schema.columns {
            schema_and_indexes
                .text(&column.name)
                .text(&column.data_type)
                .flag(column.nullable)
                .flag(column.primary_key)
                .flag(column.unique)
                .flag(column.immutable)
                .flag(column.expires)
                .text(column.default.as_deref().unwrap_or_default());
        }
        for column in &schema.primary_key {
            schema_and_indexes.text(column);
        }
        for index in &schema.indexes {
            schema_and_indexes.text(&index.name);
            for column in &index.columns {
                schema_and_indexes
                    .text(&column.column)
                    .number(match column.direction {
                        DirectIndexDirection::Asc => 0,
                        DirectIndexDirection::Desc => 1,
                    });
            }
        }
        policy
            .text(&schema.table)
            .text(&schema.ddl)
            .text(schema.history.as_deref().unwrap_or_default())
            .text(schema.sync_direction.as_deref().unwrap_or_default())
            .text(schema.conflict_policy.as_deref().unwrap_or_default())
            .number(schema.propagate.len() as u64);
        for edge_type in &schema.dag_edge_types {
            policy.text(edge_type);
        }
    }

    let mut sync = FamilyDigest::new();
    sync.number(image.sync.watermark.0);
    for source in &image.sync.sources {
        sync.number(source.row_lsn.0)
            .number(source.source_lsn.map(|lsn| lsn.0).unwrap_or_default());
    }
    for table in &image.sync.tables {
        sync.text(&table.table)
            .text(&table.direction)
            .text(&table.conflict_policy);
    }

    let mut change_and_ddl = FamilyDigest::new();
    change_and_ddl
        .number(image.changes.current_lsn.0)
        .number(image.changes.committed_watermark.0)
        .number(image.changes.next_tx.0);
    for (lsn, tx) in &image.changes.commit_index {
        change_and_ddl.number(lsn.0).number(tx.0);
    }
    // The change view is assembled from maps, so two readers of the same
    // committed bytes can list the same changes in different orders. Identity
    // is about what the image contains, not about the order a particular
    // process happened to walk it, so each change is absorbed in sorted
    // encoded order.
    change_and_ddl.canonical_set(&image.changes.rows)?;
    change_and_ddl.canonical_set(&image.changes.edges)?;
    change_and_ddl.canonical_set(&image.changes.vectors)?;
    change_and_ddl.canonical_set(&image.changes.ddl)?;
    let mut ddl_lsn = image.changes.ddl_lsn.clone();
    ddl_lsn.sort();
    for lsn in ddl_lsn {
        change_and_ddl.number(lsn.0);
    }

    let mut configuration = FamilyDigest::new();
    configuration
        .number(image.configuration.memory_limit_bytes.unwrap_or_default() as u64)
        .number(image.configuration.disk_limit_bytes.unwrap_or_default());

    let mut status = FamilyDigest::new();
    for event in &image.events.event_types {
        status
            .text(&event.name)
            .text(&event.trigger)
            .text(&event.table);
    }
    for sink in &image.events.sinks {
        status
            .text(&sink.name)
            .text(&sink.sink_type)
            .flag(sink.callback_registered)
            .number(sink.delivered)
            .number(sink.queued)
            .number(sink.retried)
            .number(sink.permanent_failures)
            .number(sink.examined);
    }
    for route in &image.events.routes {
        status
            .text(&route.name)
            .text(&route.event_type)
            .text(&route.sink);
    }
    for schedule in &image.events.schedules {
        status
            .text(&schedule.name)
            .text(&schedule.every)
            .text(&schedule.callback)
            .flag(schedule.callback_registered)
            .number(schedule.fire_count);
    }
    status
        .text(&image.maintenance.policy)
        .flag(image.maintenance.running)
        .flag(image.maintenance.retention_enabled)
        .flag(image.maintenance.currency_compaction_enabled)
        .number(image.maintenance.active_maintenance_loops as u64);

    let relational = relational.finish();
    let graph = graph.finish();
    let f32_vectors = families[0].finish();
    let sq8_vectors = families[1].finish();
    let sq4_vectors = families[2].finish();
    let schema_and_indexes = schema_and_indexes.finish();
    let policy = policy.finish();
    let sync = sync.finish();
    let change_and_ddl = change_and_ddl.finish();
    let configuration = configuration.finish();
    let status = status.finish();

    let mut complete = FamilyDigest::new();
    for family in [
        &relational,
        &graph,
        &f32_vectors,
        &sq8_vectors,
        &sq4_vectors,
        &schema_and_indexes,
        &policy,
        &sync,
        &change_and_ddl,
        &configuration,
        &status,
    ] {
        complete.bytes(family);
    }

    Ok(DirectTypedImageDigest {
        relational,
        graph,
        f32_vectors,
        sq8_vectors,
        sq4_vectors,
        schema_and_indexes,
        policy,
        sync,
        change_and_ddl,
        configuration,
        status,
        complete: complete.finish(),
    })
}

// The commit index rides through as ordinary pairs; naming the transaction
// type once here keeps the projection signature readable.
type TxIdAlias = contextdb_core::TxId;

impl std::fmt::Debug for CommittedImage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommittedImage")
            .field("image_id", &self.image.snapshot.image_id)
            .finish_non_exhaustive()
    }
}

#[allow(dead_code)]
fn unused_error_shape(error: Error) -> Error {
    error
}

/// Write one metadata answer into canonical bytes.
///
/// Ordering here is the document's own: the shape a reader gets back is the
/// shape that was hashed and published, so a field added later changes the
/// bytes rather than hiding inside them.
pub(crate) fn write_metadata_body(
    bytes: &mut crate::read_contract::CanonicalWriter,
    body: &DirectMetadataBody,
) {
    match body {
        DirectMetadataBody::Tables { items, has_more } => {
            bytes.tag(0).count(items.len() as u64);
            for item in items {
                bytes.text(item);
            }
            bytes.flag(*has_more);
        }
        DirectMetadataBody::Schema { schema } => {
            bytes.tag(1);
            write_schema(bytes, schema);
        }
        DirectMetadataBody::Explain {
            sql,
            physical_plan,
            index,
        } => {
            bytes
                .tag(2)
                .text(sql)
                .text(physical_plan)
                .optional_text(index.as_deref());
        }
        DirectMetadataBody::EventsStatus {
            status,
            has_more,
            continuation,
        } => {
            bytes.tag(3);
            write_events(bytes, status);
            bytes.flag(*has_more).optional_text(continuation.as_deref());
        }
        DirectMetadataBody::MaintenanceStatus { status } => {
            bytes
                .tag(4)
                .text(&status.policy)
                .flag(status.running)
                .flag(status.retention_enabled)
                .flag(status.currency_compaction_enabled)
                .count(status.active_maintenance_loops as u64);
        }
        DirectMetadataBody::ImageState { state } => {
            bytes.tag(5);
            match state {
                DirectImageState::Sync(sync) => {
                    bytes.tag(0).count(sync.watermark.0);
                    bytes.count(sync.sources.len() as u64);
                    for source in &sync.sources {
                        bytes.count(source.row_lsn.0);
                        match source.source_lsn {
                            Some(lsn) => {
                                bytes.tag(1).count(lsn.0);
                            }
                            None => {
                                bytes.tag(0);
                            }
                        }
                    }
                    bytes.count(sync.tables.len() as u64);
                    for table in &sync.tables {
                        bytes
                            .text(&table.table)
                            .text(&table.direction)
                            .text(&table.conflict_policy);
                    }
                }
                DirectImageState::ChangeLog(changes) => {
                    bytes
                        .tag(1)
                        .count(changes.current_lsn.0)
                        .count(changes.committed_watermark.0)
                        .count(changes.next_tx.0)
                        .count(changes.commit_index.len() as u64);
                    for (lsn, tx) in &changes.commit_index {
                        bytes.count(lsn.0).count(tx.0);
                    }
                    write_rendered(bytes, &changes.rows);
                    write_rendered(bytes, &changes.edges);
                    write_rendered(bytes, &changes.vectors);
                    write_rendered(bytes, &changes.ddl);
                    bytes.count(changes.ddl_lsn.len() as u64);
                    for lsn in &changes.ddl_lsn {
                        bytes.count(lsn.0);
                    }
                }
                DirectImageState::Configuration(configuration) => {
                    bytes.tag(2);
                    match configuration.memory_limit_bytes {
                        Some(limit) => {
                            bytes.tag(1).count(limit as u64);
                        }
                        None => {
                            bytes.tag(0);
                        }
                    }
                    match configuration.disk_limit_bytes {
                        Some(limit) => {
                            bytes.tag(1).count(limit);
                        }
                        None => {
                            bytes.tag(0);
                        }
                    }
                    bytes.count(configuration.schemas.len() as u64);
                    for schema in &configuration.schemas {
                        write_schema(bytes, schema);
                    }
                }
            }
        }
    }
}

/// Read one metadata answer back out of canonical bytes.
///
/// The exact inverse of [`write_metadata_body`]: same order, same tags, same
/// prefixes. Kept beside the writer so the two are read together and a field
/// added to one is visibly missing from the other.
pub(crate) fn read_metadata_body(
    bytes: &mut crate::read_contract::CanonicalReader<'_>,
) -> std::result::Result<DirectMetadataBody, crate::read_contract::ReadEncodingError> {
    match bytes.tag()? {
        0 => {
            let count = bytes.element_count()?;
            let mut items = Vec::new();
            for _ in 0..count {
                items.push(bytes.text()?);
            }
            let has_more = bytes.flag()?;
            Ok(DirectMetadataBody::Tables { items, has_more })
        }
        1 => Ok(DirectMetadataBody::Schema {
            schema: read_schema(bytes)?,
        }),
        2 => Ok(DirectMetadataBody::Explain {
            sql: bytes.text()?,
            physical_plan: bytes.text()?,
            index: bytes.optional_text()?,
        }),
        3 => {
            let status = read_events(bytes)?;
            let has_more = bytes.flag()?;
            let continuation = bytes.optional_text()?;
            Ok(DirectMetadataBody::EventsStatus {
                status,
                has_more,
                continuation,
            })
        }
        4 => Ok(DirectMetadataBody::MaintenanceStatus {
            status: DirectMaintenanceStatus {
                policy: bytes.text()?,
                running: bytes.flag()?,
                retention_enabled: bytes.flag()?,
                currency_compaction_enabled: bytes.flag()?,
                active_maintenance_loops: usize::try_from(bytes.count()?)
                    .map_err(|_| invalid_metadata_payload())?,
            },
        }),
        5 => Ok(DirectMetadataBody::ImageState {
            state: read_image_state(bytes)?,
        }),
        _ => Err(invalid_metadata_payload()),
    }
}

fn invalid_metadata_payload() -> crate::read_contract::ReadEncodingError {
    crate::read_contract::ReadEncodingError::InvalidPayload
}

fn read_image_state(
    bytes: &mut crate::read_contract::CanonicalReader<'_>,
) -> std::result::Result<DirectImageState, crate::read_contract::ReadEncodingError> {
    match bytes.tag()? {
        0 => {
            let watermark = Lsn(bytes.count()?);
            let source_count = bytes.element_count()?;
            let mut sources = Vec::new();
            for _ in 0..source_count {
                let row_lsn = Lsn(bytes.count()?);
                let source_lsn = match bytes.tag()? {
                    0 => None,
                    1 => Some(Lsn(bytes.count()?)),
                    _ => return Err(invalid_metadata_payload()),
                };
                sources.push(DirectSyncSource {
                    row_lsn,
                    source_lsn,
                });
            }
            let table_count = bytes.element_count()?;
            let mut tables = Vec::new();
            for _ in 0..table_count {
                tables.push(DirectTableSyncPolicy {
                    table: bytes.text()?,
                    direction: bytes.text()?,
                    conflict_policy: bytes.text()?,
                });
            }
            Ok(DirectImageState::Sync(DirectSyncState {
                watermark,
                sources,
                tables,
            }))
        }
        1 => {
            let current_lsn = Lsn(bytes.count()?);
            let committed_watermark = TxId(bytes.count()?);
            let next_tx = TxId(bytes.count()?);
            let index_count = bytes.element_count()?;
            let mut commit_index = Vec::new();
            for _ in 0..index_count {
                commit_index.push((Lsn(bytes.count()?), TxId(bytes.count()?)));
            }
            let rows = read_rendered(bytes)?;
            let edges = read_rendered(bytes)?;
            let vectors = read_rendered(bytes)?;
            let ddl = read_rendered(bytes)?;
            let lsn_count = bytes.element_count()?;
            let mut ddl_lsn = Vec::new();
            for _ in 0..lsn_count {
                ddl_lsn.push(Lsn(bytes.count()?));
            }
            Ok(DirectImageState::ChangeLog(DirectChangeState {
                current_lsn,
                committed_watermark,
                next_tx,
                commit_index,
                rows,
                edges,
                vectors,
                ddl,
                ddl_lsn,
            }))
        }
        2 => {
            let memory_limit_bytes = match bytes.tag()? {
                0 => None,
                1 => Some(usize::try_from(bytes.count()?).map_err(|_| invalid_metadata_payload())?),
                _ => return Err(invalid_metadata_payload()),
            };
            let disk_limit_bytes = match bytes.tag()? {
                0 => None,
                1 => Some(bytes.count()?),
                _ => return Err(invalid_metadata_payload()),
            };
            let schema_count = bytes.element_count()?;
            let mut schemas = Vec::new();
            for _ in 0..schema_count {
                schemas.push(read_schema(bytes)?);
            }
            Ok(DirectImageState::Configuration(DirectConfigurationState {
                memory_limit_bytes,
                disk_limit_bytes,
                schemas,
            }))
        }
        _ => Err(invalid_metadata_payload()),
    }
}

/// The change log's own entries are written as their serde rendering, so they
/// are read back the same way. A rendering that is not the type it sits in
/// front of is an invalid payload, not an empty entry.
fn read_rendered<T: serde::de::DeserializeOwned>(
    bytes: &mut crate::read_contract::CanonicalReader<'_>,
) -> std::result::Result<Vec<T>, crate::read_contract::ReadEncodingError> {
    let count = bytes.element_count()?;
    let mut values = Vec::new();
    for _ in 0..count {
        let rendered = bytes.raw()?;
        values.push(serde_json::from_slice(rendered).map_err(|_| invalid_metadata_payload())?);
    }
    Ok(values)
}

fn read_labels_list(
    bytes: &mut crate::read_contract::CanonicalReader<'_>,
) -> std::result::Result<Vec<String>, crate::read_contract::ReadEncodingError> {
    let count = bytes.element_count()?;
    let mut labels = Vec::with_capacity(count);
    for _ in 0..count {
        labels.push(bytes.text()?);
    }
    Ok(labels)
}

fn read_schema(
    bytes: &mut crate::read_contract::CanonicalReader<'_>,
) -> std::result::Result<DirectSchema, crate::read_contract::ReadEncodingError> {
    let table = bytes.text()?;
    let immutable = bytes.flag()?;
    let column_count = bytes.element_count()?;
    let mut columns = Vec::new();
    for _ in 0..column_count {
        let name = bytes.text()?;
        let data_type = bytes.text()?;
        let nullable = bytes.flag()?;
        let primary_key = bytes.flag()?;
        let unique = bytes.flag()?;
        let column_immutable = bytes.flag()?;
        let expires = bytes.flag()?;
        let default = bytes.optional_text()?;
        let references = match bytes.tag()? {
            0 => None,
            1 => {
                let reference_table = bytes.text()?;
                let reference_column = bytes.text()?;
                let propagation = match bytes.tag()? {
                    0 => None,
                    1 => Some(DirectReferencePropagation {
                        on_state: bytes.text()?,
                        set_state: bytes.text()?,
                        max_depth: u32::try_from(bytes.count()?)
                            .map_err(|_| invalid_metadata_payload())?,
                        abort_on_failure: bytes.flag()?,
                    }),
                    _ => return Err(invalid_metadata_payload()),
                };
                Some(DirectColumnReference {
                    table: reference_table,
                    column: reference_column,
                    propagation,
                })
            }
            _ => return Err(invalid_metadata_payload()),
        };
        let quantization = match bytes.tag()? {
            0 => None,
            1 => Some(match bytes.tag()? {
                0 => DirectVectorQuantization::F32,
                1 => DirectVectorQuantization::Sq8,
                2 => DirectVectorQuantization::Sq4,
                _ => return Err(invalid_metadata_payload()),
            }),
            _ => return Err(invalid_metadata_payload()),
        };
        let rank = match bytes.tag()? {
            0 => None,
            1 => Some(DirectRankPolicy {
                sort_key: bytes.text()?,
                formula: bytes.text()?,
                joined_table: bytes.text()?,
                joined_column: bytes.text()?,
            }),
            _ => return Err(invalid_metadata_payload()),
        };
        let scope_label = match bytes.tag()? {
            0 => None,
            1 => Some(match bytes.tag()? {
                0 => DirectScopeLabelKind::Simple {
                    write_labels: read_labels_list(bytes)?,
                },
                1 => DirectScopeLabelKind::Split {
                    read_labels: read_labels_list(bytes)?,
                    write_labels: read_labels_list(bytes)?,
                },
                _ => return Err(invalid_metadata_payload()),
            }),
            _ => return Err(invalid_metadata_payload()),
        };
        let acl_ref = match bytes.tag()? {
            0 => None,
            1 => {
                let table = bytes.text()?;
                let column = bytes.text()?;
                let propagation = match bytes.tag()? {
                    0 => None,
                    1 => Some(DirectReferencePropagation {
                        on_state: bytes.text()?,
                        set_state: bytes.text()?,
                        max_depth: u32::try_from(bytes.count()?)
                            .map_err(|_| invalid_metadata_payload())?,
                        abort_on_failure: bytes.flag()?,
                    }),
                    _ => return Err(invalid_metadata_payload()),
                };
                Some(DirectColumnReference {
                    table,
                    column,
                    propagation,
                })
            }
            _ => return Err(invalid_metadata_payload()),
        };
        columns.push(DirectSchemaColumn {
            name,
            data_type,
            nullable,
            primary_key,
            unique,
            immutable: column_immutable,
            expires,
            default,
            references,
            quantization,
            rank,
            scope_label,
            acl_ref,
        });
    }
    let primary_key_count = bytes.element_count()?;
    let mut primary_key = Vec::new();
    for _ in 0..primary_key_count {
        primary_key.push(bytes.text()?);
    }
    let index_count = bytes.element_count()?;
    let mut indexes = Vec::new();
    for _ in 0..index_count {
        let name = bytes.text()?;
        let column_count = bytes.element_count()?;
        let mut index_columns = Vec::new();
        for _ in 0..column_count {
            let column = bytes.text()?;
            let direction = match bytes.tag()? {
                0 => DirectIndexDirection::Asc,
                1 => DirectIndexDirection::Desc,
                _ => return Err(invalid_metadata_payload()),
            };
            index_columns.push(DirectIndexColumn { column, direction });
        }
        indexes.push(DirectSchemaIndex {
            name,
            columns: index_columns,
        });
    }
    let state_machine = match bytes.tag()? {
        0 => None,
        1 => {
            let column = bytes.text()?;
            let transition_count = bytes.element_count()?;
            let mut transitions = BTreeMap::new();
            for _ in 0..transition_count {
                let from = bytes.text()?;
                let target_count = bytes.element_count()?;
                let mut targets = Vec::new();
                for _ in 0..target_count {
                    targets.push(bytes.text()?);
                }
                transitions.insert(from, targets);
            }
            Some(DirectStateMachine {
                column,
                transitions,
            })
        }
        _ => return Err(invalid_metadata_payload()),
    };
    let retain = match bytes.tag()? {
        0 => None,
        1 => Some(DirectRetainPolicy {
            window: bytes.count()?,
            unit: bytes.text()?,
            seconds: bytes.count()?,
            sync_safe: bytes.flag()?,
        }),
        _ => return Err(invalid_metadata_payload()),
    };
    let history = bytes.optional_text()?;
    let sync_direction = bytes.optional_text()?;
    let conflict_policy = bytes.optional_text()?;
    let edge_type_count = bytes.element_count()?;
    let mut dag_edge_types = Vec::new();
    for _ in 0..edge_type_count {
        dag_edge_types.push(bytes.text()?);
    }
    let rule_count = bytes.element_count()?;
    let mut propagate = Vec::new();
    for _ in 0..rule_count {
        propagate.push(match bytes.tag()? {
            0 => DirectPropagationRule::Edge {
                edge_type: bytes.text()?,
                direction: bytes.text()?,
                on_state: bytes.text()?,
                set_state: bytes.text()?,
                max_depth: u32::try_from(bytes.count()?).map_err(|_| invalid_metadata_payload())?,
                abort_on_failure: bytes.flag()?,
            },
            1 => DirectPropagationRule::VectorExclusion {
                on_state: bytes.text()?,
            },
            2 => DirectPropagationRule::ForeignKey {
                column: bytes.text()?,
                references_table: bytes.text()?,
                references_column: bytes.text()?,
                on_state: bytes.text()?,
                set_state: bytes.text()?,
                max_depth: u32::try_from(bytes.count()?).map_err(|_| invalid_metadata_payload())?,
                abort_on_failure: bytes.flag()?,
            },
            _ => return Err(invalid_metadata_payload()),
        });
    }
    let ddl = bytes.text()?;
    Ok(DirectSchema {
        table,
        immutable,
        columns,
        primary_key,
        indexes,
        state_machine,
        retain,
        history,
        sync_direction,
        conflict_policy,
        dag_edge_types,
        propagate,
        ddl,
    })
}

fn read_events(
    bytes: &mut crate::read_contract::CanonicalReader<'_>,
) -> std::result::Result<DirectEventsStatus, crate::read_contract::ReadEncodingError> {
    let type_count = bytes.element_count()?;
    let mut event_types = Vec::new();
    for _ in 0..type_count {
        event_types.push(DirectEventTypeStatus {
            name: bytes.text()?,
            trigger: bytes.text()?,
            table: bytes.text()?,
        });
    }
    let sink_count = bytes.element_count()?;
    let mut sinks = Vec::new();
    for _ in 0..sink_count {
        sinks.push(DirectSinkStatus {
            name: bytes.text()?,
            sink_type: bytes.text()?,
            callback_registered: bytes.flag()?,
            delivered: bytes.count()?,
            queued: bytes.count()?,
            retried: bytes.count()?,
            permanent_failures: bytes.count()?,
            examined: bytes.count()?,
        });
    }
    let route_count = bytes.element_count()?;
    let mut routes = Vec::new();
    for _ in 0..route_count {
        routes.push(DirectRouteStatus {
            name: bytes.text()?,
            event_type: bytes.text()?,
            sink: bytes.text()?,
        });
    }
    let schedule_count = bytes.element_count()?;
    let mut schedules = Vec::new();
    for _ in 0..schedule_count {
        let name = bytes.text()?;
        let every = bytes.text()?;
        let callback = bytes.text()?;
        let callback_registered = bytes.flag()?;
        let next_fire_at_ms = bytes.count()?;
        // The writer renders "never fired" as zero, so zero is what comes
        // back: the absent form is not on the wire and cannot be invented
        // here without claiming a fire that did not happen.
        let last_fire_at_ms = match bytes.count()? {
            0 => None,
            fired => Some(fired),
        };
        let fire_count = bytes.count()?;
        schedules.push(DirectScheduleStatus {
            name,
            every,
            callback,
            callback_registered,
            next_fire_at_ms,
            last_fire_at_ms,
            fire_count,
        });
    }
    Ok(DirectEventsStatus {
        event_types,
        sinks,
        routes,
        schedules,
    })
}

fn write_rendered<T: serde::Serialize>(
    bytes: &mut crate::read_contract::CanonicalWriter,
    values: &[T],
) {
    bytes.count(values.len() as u64);
    for value in values {
        let rendered = serde_json::to_value(value)
            .and_then(|document| serde_json::to_vec(&document))
            .unwrap_or_default();
        bytes.raw(&rendered);
    }
}

fn write_labels_list(bytes: &mut crate::read_contract::CanonicalWriter, labels: &[String]) {
    bytes.count(labels.len() as u64);
    for label in labels {
        bytes.text(label);
    }
}

fn write_schema(bytes: &mut crate::read_contract::CanonicalWriter, schema: &DirectSchema) {
    bytes
        .text(&schema.table)
        .flag(schema.immutable)
        .count(schema.columns.len() as u64);
    for column in &schema.columns {
        bytes
            .text(&column.name)
            .text(&column.data_type)
            .flag(column.nullable)
            .flag(column.primary_key)
            .flag(column.unique)
            .flag(column.immutable)
            .flag(column.expires)
            .optional_text(column.default.as_deref());
        match &column.references {
            Some(reference) => {
                bytes.tag(1).text(&reference.table).text(&reference.column);
                match &reference.propagation {
                    Some(propagation) => {
                        bytes
                            .tag(1)
                            .text(&propagation.on_state)
                            .text(&propagation.set_state)
                            .count(u64::from(propagation.max_depth))
                            .flag(propagation.abort_on_failure);
                    }
                    None => {
                        bytes.tag(0);
                    }
                }
            }
            None => {
                bytes.tag(0);
            }
        }
        match column.quantization {
            Some(quantization) => {
                bytes.tag(1).tag(match quantization {
                    DirectVectorQuantization::F32 => 0,
                    DirectVectorQuantization::Sq8 => 1,
                    DirectVectorQuantization::Sq4 => 2,
                });
            }
            None => {
                bytes.tag(0);
            }
        }
        match &column.rank {
            Some(rank) => {
                bytes
                    .tag(1)
                    .text(&rank.sort_key)
                    .text(&rank.formula)
                    .text(&rank.joined_table)
                    .text(&rank.joined_column);
            }
            None => {
                bytes.tag(0);
            }
        }
        match &column.scope_label {
            Some(DirectScopeLabelKind::Simple { write_labels }) => {
                bytes.tag(1).tag(0);
                write_labels_list(bytes, write_labels);
            }
            Some(DirectScopeLabelKind::Split {
                read_labels,
                write_labels,
            }) => {
                bytes.tag(1).tag(1);
                write_labels_list(bytes, read_labels);
                write_labels_list(bytes, write_labels);
            }
            None => {
                bytes.tag(0);
            }
        }
        match &column.acl_ref {
            Some(reference) => {
                bytes.tag(1).text(&reference.table).text(&reference.column);
                match &reference.propagation {
                    Some(propagation) => {
                        bytes
                            .tag(1)
                            .text(&propagation.on_state)
                            .text(&propagation.set_state)
                            .count(u64::from(propagation.max_depth))
                            .flag(propagation.abort_on_failure);
                    }
                    None => {
                        bytes.tag(0);
                    }
                }
            }
            None => {
                bytes.tag(0);
            }
        }
    }
    bytes.count(schema.primary_key.len() as u64);
    for column in &schema.primary_key {
        bytes.text(column);
    }
    bytes.count(schema.indexes.len() as u64);
    for index in &schema.indexes {
        bytes.text(&index.name).count(index.columns.len() as u64);
        for column in &index.columns {
            bytes.text(&column.column).tag(match column.direction {
                DirectIndexDirection::Asc => 0,
                DirectIndexDirection::Desc => 1,
            });
        }
    }
    match &schema.state_machine {
        Some(state) => {
            bytes
                .tag(1)
                .text(&state.column)
                .count(state.transitions.len() as u64);
            for (from, to) in &state.transitions {
                bytes.text(from).count(to.len() as u64);
                for target in to {
                    bytes.text(target);
                }
            }
        }
        None => {
            bytes.tag(0);
        }
    }
    match &schema.retain {
        Some(retain) => {
            bytes
                .tag(1)
                .count(retain.window)
                .text(&retain.unit)
                .count(retain.seconds)
                .flag(retain.sync_safe);
        }
        None => {
            bytes.tag(0);
        }
    }
    bytes
        .optional_text(schema.history.as_deref())
        .optional_text(schema.sync_direction.as_deref())
        .optional_text(schema.conflict_policy.as_deref())
        .count(schema.dag_edge_types.len() as u64);
    for edge_type in &schema.dag_edge_types {
        bytes.text(edge_type);
    }
    bytes.count(schema.propagate.len() as u64);
    for rule in &schema.propagate {
        match rule {
            DirectPropagationRule::Edge {
                edge_type,
                direction,
                on_state,
                set_state,
                max_depth,
                abort_on_failure,
            } => {
                bytes
                    .tag(0)
                    .text(edge_type)
                    .text(direction)
                    .text(on_state)
                    .text(set_state)
                    .count(u64::from(*max_depth))
                    .flag(*abort_on_failure);
            }
            DirectPropagationRule::VectorExclusion { on_state } => {
                bytes.tag(1).text(on_state);
            }
            DirectPropagationRule::ForeignKey {
                column,
                references_table,
                references_column,
                on_state,
                set_state,
                max_depth,
                abort_on_failure,
            } => {
                bytes
                    .tag(2)
                    .text(column)
                    .text(references_table)
                    .text(references_column)
                    .text(on_state)
                    .text(set_state)
                    .count(u64::from(*max_depth))
                    .flag(*abort_on_failure);
            }
        }
    }
    bytes.text(&schema.ddl);
}

fn write_events(bytes: &mut crate::read_contract::CanonicalWriter, events: &DirectEventsStatus) {
    bytes.count(events.event_types.len() as u64);
    for event in &events.event_types {
        bytes
            .text(&event.name)
            .text(&event.trigger)
            .text(&event.table);
    }
    bytes.count(events.sinks.len() as u64);
    for sink in &events.sinks {
        bytes
            .text(&sink.name)
            .text(&sink.sink_type)
            .flag(sink.callback_registered)
            .count(sink.delivered)
            .count(sink.queued)
            .count(sink.retried)
            .count(sink.permanent_failures)
            .count(sink.examined);
    }
    bytes.count(events.routes.len() as u64);
    for route in &events.routes {
        bytes
            .text(&route.name)
            .text(&route.event_type)
            .text(&route.sink);
    }
    bytes.count(events.schedules.len() as u64);
    for schedule in &events.schedules {
        bytes
            .text(&schedule.name)
            .text(&schedule.every)
            .text(&schedule.callback)
            .flag(schedule.callback_registered)
            .count(schedule.next_fire_at_ms)
            .count(schedule.last_fire_at_ms.unwrap_or_default())
            .count(schedule.fire_count);
    }
}

/// Answer one metadata request from the owned image, with the plan question
/// answered by the same planner an ordinary read uses.
/// Answer one metadata question from a live database.
///
/// The in-process reader is looking at the writer's own state rather than at
/// a committed file, so the answer is projected from the database directly.
/// Every kind a live owner can answer is answered here in the same words the
/// file route uses; the one kind it cannot is refused by name rather than
/// answered emptily -- see [`project_metadata`] for the image's own version.
pub(crate) fn project_metadata_from_database(
    database: &Database,
    request: DirectMetadataRequest,
    limits: ReadLimits,
    clock: Arc<dyn DeadlineClock>,
    continuation: Option<&str>,
) -> std::result::Result<(DirectMetadataBody, Option<String>), DirectFileReaderError> {
    // An inventory is bounded a page at a time by the pager below. Everything
    // else arrives whole, so whether it fits is decided once it is built.
    let arrives_whole = !matches!(
        request,
        DirectMetadataRequest::Tables | DirectMetadataRequest::EventsStatus
    );
    let mut resume_at = None;
    let body = match request {
        DirectMetadataRequest::Tables => {
            let page = paged(
                MetadataPageVocabulary::Tables,
                crate::metadata_page::table_items(database.table_names()),
                continuation,
                limits.result_bytes,
                crate::metadata_page::metadata_table_key,
            )?;
            resume_at = page.continuation.clone();
            DirectMetadataBody::Tables {
                items: crate::metadata_page::table_names_of(&page),
                has_more: page.has_more,
            }
        }
        DirectMetadataRequest::Schema { table } => {
            let meta = database.table_meta(&table).ok_or_else(|| {
                DirectFileReaderError::Engine(format!("table not found: {table}"))
            })?;
            DirectMetadataBody::Schema {
                schema: project_schema(&table, &meta),
            }
        }
        DirectMetadataRequest::Explain { sql } => {
            explained_statement(database, sql, limits, Arc::clone(&clock))?
        }
        DirectMetadataRequest::EventsStatus => {
            let page = paged(
                MetadataPageVocabulary::EventsStatus,
                crate::metadata_page::event_status_items(
                    &database.event_bus_status(),
                    &database.cron_status(),
                ),
                continuation,
                limits.result_bytes,
                crate::metadata_page::metadata_event_key,
            )?;
            resume_at = page.continuation.clone();
            DirectMetadataBody::EventsStatus {
                status: crate::metadata_page::events_of(&page),
                has_more: page.has_more,
                continuation: page.continuation.clone(),
            }
        }
        DirectMetadataRequest::MaintenanceStatus => {
            // A live owner reports the maintenance that is actually running,
            // where the committed image deliberately reports none: an image
            // is a file, and a file runs nothing.
            let status = database.maintenance_status();
            DirectMetadataBody::MaintenanceStatus {
                status: DirectMaintenanceStatus {
                    policy: match status.policy {
                        MaintenancePolicy::EngineOwned => "engine_owned".to_owned(),
                        MaintenancePolicy::CallerDriven => "caller_driven".to_owned(),
                    },
                    running: status.running,
                    retention_enabled: status.retention_enabled,
                    currency_compaction_enabled: status.currency_compaction_enabled,
                    active_maintenance_loops: status.active_maintenance_loops,
                },
            }
        }
        // The route decides this one: an owner -- here or over a channel --
        // is not a committed file. The session refuses it before reaching
        // this projection; this arm keeps the match honest if it ever does.
        DirectMetadataRequest::ImageState { .. } => {
            return Err(DirectFileReaderError::Engine(
                "the state of a committed image is a question about a file".to_owned(),
            ));
        }
    };
    if arrives_whole {
        admit_complete(&body, limits.result_bytes)?;
    }
    Ok((body, resume_at))
}

/// The stable word a caller branches on for the inspection asked for here.
pub(crate) const IMAGE_STATE_INSPECTION: &str = "image_state";

/// The state of a committed image is a question about a file.
///
/// A reader talking to an owner -- in this process or over a channel -- is not
/// looking at one: the owner's state is still moving, and the local protocol
/// carries no request for it. The question is refused by name rather than
/// answered with an empty state a caller would read as "this store has none".
///
/// The refusal names the inspection it could not answer and the route that
/// can, so a caller learns which question failed and where to ask it instead
/// of being handed a bare "not implemented" it cannot act on.
pub(crate) fn image_state_is_a_file_question() -> Error {
    Error::ReadFailure(ReadFailure::owner_route_unsupported(
        IMAGE_STATE_INSPECTION.to_owned(),
    ))
}

pub(crate) fn project_metadata(
    target: &dyn ReadExecutionTarget,
    request: DirectMetadataRequest,
    image: &DirectOwnedImage,
    limits: ReadLimits,
    clock: Arc<dyn DeadlineClock>,
    continuation: Option<&str>,
) -> std::result::Result<DirectMetadataResponse, DirectFileReaderError> {
    let arrives_whole = !matches!(
        request,
        DirectMetadataRequest::Tables | DirectMetadataRequest::EventsStatus
    );
    let mut resume_at = None;
    let body = match request {
        DirectMetadataRequest::Tables => {
            // Cut by the same pager the owner uses, so an inventory read from
            // the file breaks at exactly the places it breaks over a channel.
            let page = paged(
                MetadataPageVocabulary::Tables,
                crate::metadata_page::table_items(
                    image
                        .configuration
                        .schemas
                        .iter()
                        .map(|schema| schema.table.clone())
                        .collect(),
                ),
                continuation,
                limits.result_bytes,
                crate::metadata_page::metadata_table_key,
            )?;
            resume_at = page.continuation.clone();
            DirectMetadataBody::Tables {
                items: crate::metadata_page::table_names_of(&page),
                has_more: page.has_more,
            }
        }
        DirectMetadataRequest::Schema { table } => {
            let schema = image
                .configuration
                .schemas
                .iter()
                .find(|schema| schema.table == table)
                .cloned()
                .ok_or_else(|| {
                    DirectFileReaderError::Engine(format!("table not found: {table}"))
                })?;
            DirectMetadataBody::Schema { schema }
        }
        DirectMetadataRequest::Explain { sql } => {
            explained_statement(target, sql, limits, Arc::clone(&clock))?
        }
        DirectMetadataRequest::EventsStatus => {
            let page = paged(
                MetadataPageVocabulary::EventsStatus,
                crate::metadata_page::event_status_items_of(&image.events),
                continuation,
                limits.result_bytes,
                crate::metadata_page::metadata_event_key,
            )?;
            resume_at = page.continuation.clone();
            DirectMetadataBody::EventsStatus {
                status: crate::metadata_page::events_of(&page),
                has_more: page.has_more,
                continuation: page.continuation.clone(),
            }
        }
        DirectMetadataRequest::MaintenanceStatus => DirectMetadataBody::MaintenanceStatus {
            status: image.maintenance.clone(),
        },
        DirectMetadataRequest::ImageState { kind } => DirectMetadataBody::ImageState {
            state: match kind {
                DirectImageMetadataKind::Sync => DirectImageState::Sync(image.sync.clone()),
                DirectImageMetadataKind::ChangeLog => {
                    DirectImageState::ChangeLog(image.changes.clone())
                }
                DirectImageMetadataKind::Configuration => {
                    DirectImageState::Configuration(image.configuration.clone())
                }
            },
        },
    };
    let canonical_bytes = crate::read_contract::encode_metadata_body(&body)
        .map_err(|error| DirectFileReaderError::Engine(error.to_string()))?;
    if arrives_whole
        && let Some(failure) = crate::metadata_page::admit_complete_metadata(
            canonical_bytes.len(),
            limits.result_bytes,
        )
    {
        return Err(DirectFileReaderError::ReadFailure(failure));
    }
    Ok(DirectMetadataResponse {
        body,
        canonical_bytes,
        snapshot: image.snapshot,
        continuation: resume_at,
    })
}

/// What the engine would do with this statement, answered the only honest way
/// for the kind of statement it is.
///
/// A READ is run: the route a read really takes -- which index it picks, what
/// it pushes down, what it rejects -- is only known once it has taken it, and
/// running it changes nothing. A WRITE is planned and NOT run: `.explain
/// DELETE FROM t` has to leave the rows alone, so what it answers is the plan
/// the engine chose, which reads schema and decides a strategy and touches no
/// data. Refusing to explain a write at all -- which is what asking the
/// bounded read door for a write plan used to produce -- turns "here is what
/// this WOULD do" into "no", which is not what explaining is for.
///
/// No index is reported for a planned write, because nothing ran to pick one;
/// reporting one would be a claim about an execution that never happened.
pub(crate) fn explained_statement(
    target: &dyn ReadExecutionTarget,
    sql: String,
    limits: ReadLimits,
    clock: Arc<dyn DeadlineClock>,
) -> std::result::Result<DirectMetadataBody, DirectFileReaderError> {
    let statement = contextdb_parser::parse(&sql)
        .map_err(|error| DirectFileReaderError::Engine(error.to_string()))?;
    if contextdb_parser::statement_effect(&statement) != contextdb_parser::StatementEffect::Read {
        let physical_plan = target
            .explain_plan_without_running_it(&sql)
            .map_err(|error| DirectFileReaderError::Engine(error.to_string()))?;
        return Ok(DirectMetadataBody::Explain {
            physical_plan,
            index: None,
            sql,
        });
    }
    let cancellation = OwnerReadCancellation::new();
    // An explain request carries a statement, not the values a caller would
    // run it with, and the engine's plan can depend on those values -- an
    // index seek needs something to seek to. Every parameter the statement
    // names is therefore bound to nothing, which is the honest reading of
    // "explain this statement, with no values supplied", and keeps the answer
    // the engine's own rather than a second description of what it might have
    // chosen.
    let params = explain_bindings(target, &sql);
    let answered = target.read_query(&sql, &params, limits, clock, &cancellation)?;
    Ok(DirectMetadataBody::Explain {
        physical_plan: answered.0.trace.physical_plan.to_owned(),
        index: answered.0.trace.index_used.clone(),
        sql,
    })
}

/// One page of an inventory, cut by the shared pager.
fn paged(
    vocabulary: MetadataPageVocabulary,
    items: Vec<contextdb_core::read_contract::MetadataItem>,
    continuation: Option<&str>,
    page_bytes: u64,
    key: fn(&contextdb_core::read_contract::MetadataItem) -> String,
) -> std::result::Result<contextdb_core::read_contract::MetadataPage, DirectFileReaderError> {
    crate::metadata_page::continuation_page(vocabulary, items, continuation, page_bytes, key)
        .map_err(|error| match error {
            // A misused continuation and a page that cannot fit are both
            // typed refusals a caller can act on -- the same ones the owner
            // gives for the same store -- not engine prose.
            crate::metadata_page::MetadataPagingError::Continuation(failure)
            | crate::metadata_page::MetadataPagingError::Oversized(failure) => {
                DirectFileReaderError::ReadFailure(failure)
            }
            crate::metadata_page::MetadataPagingError::Encoding(encoding) => {
                DirectFileReaderError::Engine(encoding.to_string())
            }
        })
}

/// Refuse a complete answer that does not fit the byte ceiling in force.
///
/// The owner refuses this before it puts the answer on the wire. Reading the
/// same store as a file has to refuse it too, or a ceiling means one thing
/// over a channel and nothing at all against the file.
fn admit_complete(
    body: &DirectMetadataBody,
    result_bytes: u64,
) -> std::result::Result<(), DirectFileReaderError> {
    let payload = crate::read_contract::encode_metadata_body(body)
        .map_err(|error| DirectFileReaderError::Engine(error.to_string()))?;
    match crate::metadata_page::admit_complete_metadata(payload.len(), result_bytes) {
        Some(failure) => Err(DirectFileReaderError::ReadFailure(failure)),
        None => Ok(()),
    }
}

/// Stand-in values for the parameters an explain request does not carry.
///
/// A plan can depend on the values a statement is run with -- an index seek
/// needs something of the column's own type to seek on -- so explaining a
/// parameterized statement with nothing bound would answer for a query nobody
/// asked about. Each parameter compared against a column is therefore bound
/// to an empty value OF THAT COLUMN'S TYPE, which is what makes the answer
/// the plan the engine really chooses for this statement rather than the
/// fallback it chooses when it has nothing to seek on.
fn explain_bindings(target: &dyn ReadExecutionTarget, sql: &str) -> HashMap<String, Value> {
    let mut bound = HashMap::new();
    let Ok(statement) = contextdb_parser::parse(sql) else {
        return bound;
    };
    let Ok(plan) = contextdb_planner::plan(&statement) else {
        return bound;
    };
    let mut predicates = Vec::new();
    collect_plan_predicates(&plan, &mut predicates);
    for (table, predicate) in predicates {
        let Some(meta) = target.table_meta(&table) else {
            continue;
        };
        let mut compared = Vec::new();
        collect_compared_parameters(predicate, &mut compared);
        for (column, request) in compared {
            if bound.contains_key(&request) {
                continue;
            }
            if let Some(declaration) = meta.columns.iter().find(|entry| entry.name == column) {
                bound.insert(request, empty_value_of_type(&declaration.column_type));
            }
        }
    }
    bound
}

/// Every filter a plan applies, with the table it applies to.
fn collect_plan_predicates<'a>(
    plan: &'a PhysicalPlan,
    found: &mut Vec<(String, &'a contextdb_parser::ast::Expr)>,
) {
    match plan {
        PhysicalPlan::Scan {
            table,
            filter: Some(filter),
            ..
        } => found.push((table.clone(), filter)),
        PhysicalPlan::Filter { input, predicate } => {
            if let Some(table) = plan_table(input) {
                found.push((table, predicate));
            }
            collect_plan_predicates(input, found);
        }
        PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Distinct { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. } => collect_plan_predicates(input, found),
        _ => {}
    }
}

fn plan_table(plan: &PhysicalPlan) -> Option<String> {
    match plan {
        PhysicalPlan::Scan { table, .. } | PhysicalPlan::IndexScan { table, .. } => {
            Some(table.clone())
        }
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Distinct { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. } => plan_table(input),
        _ => None,
    }
}

/// Every `column <op> $parameter` pairing a predicate makes, either way round.
fn collect_compared_parameters(
    predicate: &contextdb_parser::ast::Expr,
    found: &mut Vec<(String, String)>,
) {
    use contextdb_parser::ast::Expr;

    match predicate {
        Expr::BinaryOp { left, right, .. } => {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(column), Expr::Parameter(request))
                | (Expr::Parameter(request), Expr::Column(column)) => {
                    found.push((column.column.clone(), request.clone()));
                }
                _ => {}
            }
            collect_compared_parameters(left, found);
            collect_compared_parameters(right, found);
        }
        Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => {
            collect_compared_parameters(operand, found);
        }
        Expr::InList { expr, list, .. } => {
            if let Expr::Column(column) = expr.as_ref() {
                for entry in list {
                    if let Expr::Parameter(request) = entry {
                        found.push((column.column.clone(), request.clone()));
                    }
                }
            }
            collect_compared_parameters(expr, found);
        }
        Expr::Like { expr, pattern, .. } => {
            if let (Expr::Column(column), Expr::Parameter(request)) =
                (expr.as_ref(), pattern.as_ref())
            {
                found.push((column.column.clone(), request.clone()));
            }
        }
        _ => {}
    }
}

fn empty_value_of_type(column_type: &ColumnType) -> Value {
    match column_type {
        ColumnType::Integer | ColumnType::TxId => Value::Int64(0),
        ColumnType::Real => Value::Float64(0.0),
        ColumnType::Text | ColumnType::Json => Value::Text(String::new()),
        ColumnType::Boolean => Value::Bool(false),
        ColumnType::Uuid => Value::Uuid(uuid::Uuid::nil()),
        ColumnType::Timestamp => Value::Timestamp(0),
        ColumnType::Vector(dimension) => Value::Vector(vec![0.0; *dimension]),
    }
}
