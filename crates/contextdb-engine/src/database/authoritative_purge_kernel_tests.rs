#![cfg(all(feature = "sync-orchestration", feature = "test-seams"))]

use super::*;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    path::PathBuf,
    time::Duration,
};
use uuid::Uuid;

const NOTES: &str = "notes";
const EDGES: &str = "edges";
const SINK: &str = "purge_archive";
const EDGE_TYPE: &str = "PURGE_TEST";

#[derive(Debug, Clone, PartialEq)]
struct GraphProbe {
    selected_forward: BTreeSet<Uuid>,
    selected_reverse: BTreeSet<Uuid>,
    survivor_forward: BTreeSet<Uuid>,
    survivor_reverse: BTreeSet<Uuid>,
}

#[derive(Debug, Clone, PartialEq)]
struct SecondaryIndexProbe {
    rows: Vec<Vec<Value>>,
    index_used: Option<String>,
}

#[derive(Clone, PartialEq)]
struct OwnedCopies {
    selected_live: Vec<Vec<Value>>,
    selected_secondary_index: SecondaryIndexProbe,
    survivor_live: Vec<Vec<Value>>,
    survivor_secondary_index: SecondaryIndexProbe,
    selected_history: Vec<RowChange>,
    survivor_history: Vec<RowChange>,
    selected_history_count: usize,
    survivor_history_count: usize,
    selected_edge_live: Vec<Vec<Value>>,
    survivor_edge_live: Vec<Vec<Value>>,
    selected_edge_history: Vec<RowChange>,
    survivor_edge_history: Vec<RowChange>,
    selected_edge_history_count: usize,
    survivor_edge_history_count: usize,
    durable_change_log: Vec<ChangeLogEntry>,
    selected_vector_present: bool,
    survivor_vector_present: bool,
    hnsw_len: usize,
    graph: GraphProbe,
    durable_sink_payloads: BTreeMap<RowId, HashMap<String, Value>>,
    memory_sink_payloads: BTreeMap<RowId, HashMap<String, Value>>,
    selected_note_disk_sync_source: Option<(Lsn, u8)>,
    survivor_note_disk_sync_source: Option<(Lsn, u8)>,
    selected_edge_disk_sync_source: Option<(Lsn, u8)>,
    survivor_edge_disk_sync_source: Option<(Lsn, u8)>,
    selected_note_memory_sync_source: Option<(Lsn, u8)>,
    survivor_note_memory_sync_source: Option<(Lsn, u8)>,
    selected_edge_memory_sync_source: Option<(Lsn, u8)>,
    survivor_edge_memory_sync_source: Option<(Lsn, u8)>,
    selected_note_live_sidecar: Option<AuthoritativePurgeLiveRowSidecarSnapshot>,
    survivor_note_live_sidecar: Option<AuthoritativePurgeLiveRowSidecarSnapshot>,
    selected_edge_live_sidecar: Option<AuthoritativePurgeLiveRowSidecarSnapshot>,
    survivor_edge_live_sidecar: Option<AuthoritativePurgeLiveRowSidecarSnapshot>,
    selected_deletion_state: DurableDeletionStateSnapshot,
    survivor_deletion_state: DurableDeletionStateSnapshot,
    selected_edge_deletion_state: DurableDeletionStateSnapshot,
    survivor_edge_deletion_state: DurableDeletionStateSnapshot,
}

#[derive(Clone)]
struct CapturedLineage {
    row_id: RowId,
    table_generation: u64,
    natural_key: NaturalKey,
    lineage_root: String,
}

struct Fixture {
    path: PathBuf,
    note_selection: AuthoritativePurgeSelection,
    edge_selection: AuthoritativePurgeSelection,
    survivor_note_selection: AuthoritativePurgeSelection,
    survivor_edge_selection: AuthoritativePurgeSelection,
    selected: Uuid,
    survivor: Uuid,
    selected_edge: Uuid,
    survivor_edge: Uuid,
    selected_graph_target: Uuid,
    survivor_graph_target: Uuid,
    pinned_snapshot: SnapshotId,
    selected_note: CapturedLineage,
    survivor_note: CapturedLineage,
    selected_edge_lineage: CapturedLineage,
    survivor_edge_lineage: CapturedLineage,
}

struct FixtureSeed {
    root: tempfile::TempDir,
    path: PathBuf,
    db: Database,
    selected: Uuid,
}

fn params(id: Uuid, body: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_string(), Value::Uuid(id)),
        ("body".to_string(), Value::Text(body.to_string())),
    ])
}

fn exact_rows(
    db: &Database,
    table: &str,
    id: Uuid,
    snapshot: Option<SnapshotId>,
) -> Vec<Vec<Value>> {
    let sql = format!("SELECT id, body FROM {table} WHERE id = $id");
    let arguments = HashMap::from([("id".to_string(), Value::Uuid(id))]);
    match snapshot {
        Some(snapshot) => db.execute_at_snapshot(&sql, &arguments, snapshot),
        None => db.execute(&sql, &arguments),
    }
    .expect("read exact fixture row")
    .rows
}

fn secondary_index_rows(db: &Database, body: &str) -> SecondaryIndexProbe {
    let result = db
        .execute(
            "SELECT id FROM notes WHERE body = $body",
            &HashMap::from([("body".to_string(), Value::Text(body.to_string()))]),
        )
        .expect("probe secondary index");
    SecondaryIndexProbe {
        rows: result.rows,
        index_used: result.trace.index_used,
    }
}

fn row_id(db: &Database, table: &str, id: Uuid) -> RowId {
    db.scan(table, db.snapshot())
        .expect("scan fixture table")
        .into_iter()
        .find(|row| row.values.get("id") == Some(&Value::Uuid(id)))
        .expect("fixture row exists")
        .row_id
}

fn vector_row_ids(db: &Database) -> Vec<RowId> {
    db.query_vector(
        VectorIndexRef::new(NOTES, "embedding"),
        &[1.0, 0.0, 0.0],
        16,
        None,
        db.snapshot(),
    )
    .expect("query fixture vector index")
    .into_iter()
    .map(|(row_id, _)| row_id)
    .collect()
}

fn graph_neighbors(db: &Database, node: Uuid, direction: Direction) -> BTreeSet<Uuid> {
    db.query_bfs(
        node,
        Some(&[EDGE_TYPE.to_string()]),
        direction,
        1,
        db.snapshot(),
    )
    .expect("traverse durable production graph")
    .nodes
    .into_iter()
    .map(|node| node.id)
    .collect()
}

fn graph_probe(db: &Database, fixture: &Fixture) -> GraphProbe {
    GraphProbe {
        selected_forward: graph_neighbors(db, fixture.selected, Direction::Outgoing),
        selected_reverse: graph_neighbors(db, fixture.selected_graph_target, Direction::Incoming),
        survivor_forward: graph_neighbors(db, fixture.survivor, Direction::Outgoing),
        survivor_reverse: graph_neighbors(db, fixture.survivor_graph_target, Direction::Incoming),
    }
}

fn durable_sink_payloads(db: &Database) -> BTreeMap<RowId, HashMap<String, Value>> {
    db.persistence
        .as_ref()
        .expect("file-backed fixture")
        .load_sink_queue::<event_bus::SinkQueueEntry>(SINK)
        .expect("read durable fixture queue")
        .into_iter()
        .map(|entry| (entry.row_id, entry.event.row_values))
        .collect()
}

fn memory_sink_payloads(db: &Database) -> BTreeMap<RowId, HashMap<String, Value>> {
    db.authoritative_purge_memory_sink_queue_for_test(SINK)
}

fn durable_change_log(db: &Database) -> Vec<ChangeLogEntry> {
    db.persistence
        .as_ref()
        .expect("file-backed fixture")
        .load_change_log()
        .expect("read durable fixture change log")
}

fn disk_sync_source(db: &Database, table: &str, row_id: RowId) -> Option<(Lsn, u8)> {
    let persistence = db.persistence.as_ref().expect("file-backed fixture");
    let lsns = persistence
        .load_sync_source_lsns()
        .expect("read durable sync-source LSNs");
    let kinds = persistence
        .load_sync_source_kinds()
        .expect("read durable sync-source kinds");
    Some((
        *lsns.get(&(table.to_string(), row_id))?,
        *kinds.get(&(table.to_string(), row_id))?,
    ))
}

fn memory_sync_source(db: &Database, table: &str, row_id: RowId) -> Option<(Lsn, u8)> {
    let lsn = db.relational_store.sync_source_lsn(table, row_id)?;
    let kind = match db.relational_store.sync_source_kind(table, row_id)? {
        contextdb_relational::store::SyncSourceKind::Pulled => 0,
        contextdb_relational::store::SyncSourceKind::AcceptedLocal => 1,
        contextdb_relational::store::SyncSourceKind::AcceptedLocalPending => 2,
    };
    Some((lsn, kind))
}

fn exact_edge_rows(db: &Database, id: Uuid) -> Vec<Vec<Value>> {
    db.execute(
        "SELECT id, source_id, target_id, edge_type FROM edges WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .expect("read exact durable edge owner row")
    .rows
}

fn captured(selection: &AuthoritativePurgeSelection, row_id: RowId) -> CapturedLineage {
    CapturedLineage {
        row_id,
        table_generation: selection.table_generation,
        natural_key: selection.natural_key.clone(),
        lineage_root: selection.lineage_root.clone(),
    }
}

fn current_row_change(db: &Database, table: &str, key: &NaturalKey) -> RowChange {
    let snapshot = db.snapshot();
    let current_lsn = db
        .visible_row_by_natural_key(table, key, snapshot, &std::collections::HashSet::new())
        .expect("find fixture row by its exact natural key")
        .expect("fixture row remains live while recording acknowledgement")
        .lsn;
    db.changes_since(Lsn(0))
        .rows
        .into_iter()
        .filter(|row| {
            row.table == table && !row.deleted && row.natural_key == *key && row.lsn == current_lsn
        })
        // `changes_since` appends synthetic vector-owner rows after the
        // ordinary current-row replay.  An UPDATE that did not replace its
        // vector therefore has an older synthetic owner LSN at the tail.
        // Acknowledgement must prove the visible, current row instead.
        .max_by_key(|row| row.lsn)
        .expect("find current row for durable source provenance")
}

fn assert_owned_copies_equal_after_reopen(before: &OwnedCopies, after: &OwnedCopies) {
    // HNSW itself is nondurable. `owned_copies` deliberately runs the vector
    // query first, so this proves reopen can rematerialize the same survivor
    // graph without pretending the pre-close allocation identity persisted.
    assert_eq!(after.hnsw_len, before.hnsw_len);
    assert_eq!(
        after.durable_change_log, before.durable_change_log,
        "the injected failure must preserve the complete ordered decoded durable change-log occurrence sequence across reopen"
    );

    let mut expected = before.clone();
    let mut observed = after.clone();
    for copies in [&mut expected, &mut observed] {
        copies.selected_history.clear();
        copies.survivor_history.clear();
        copies.selected_edge_history.clear();
        copies.survivor_edge_history.clear();
        copies.selected_history_count = 0;
        copies.survivor_history_count = 0;
        copies.selected_edge_history_count = 0;
        copies.survivor_edge_history_count = 0;
    }
    assert!(
        observed == expected,
        "failed point-removes must preserve every durable and mirrored owner class across reopen"
    );
}

fn owned_copies(db: &Database, fixture: &Fixture) -> OwnedCopies {
    let vectors = vector_row_ids(db);
    let changes = db.changes_since(Lsn(0));
    let selected_history = changes
        .rows
        .iter()
        .filter(|row| row.table == NOTES && row.natural_key == fixture.selected_note.natural_key)
        .cloned()
        .collect::<Vec<_>>();
    let survivor_history = changes
        .rows
        .iter()
        .filter(|row| row.table == NOTES && row.natural_key == fixture.survivor_note.natural_key)
        .cloned()
        .collect::<Vec<_>>();
    let selected_edge_history = changes
        .rows
        .iter()
        .filter(|row| {
            row.table == EDGES && row.natural_key == fixture.selected_edge_lineage.natural_key
        })
        .cloned()
        .collect::<Vec<_>>();
    let survivor_edge_history = changes
        .rows
        .iter()
        .filter(|row| {
            row.table == EDGES && row.natural_key == fixture.survivor_edge_lineage.natural_key
        })
        .cloned()
        .collect::<Vec<_>>();
    OwnedCopies {
        selected_live: exact_rows(db, NOTES, fixture.selected, None),
        selected_secondary_index: secondary_index_rows(db, "selected-after-update"),
        survivor_live: exact_rows(db, NOTES, fixture.survivor, None),
        survivor_secondary_index: secondary_index_rows(db, "survivor"),
        selected_history_count: selected_history.len(),
        survivor_history_count: survivor_history.len(),
        selected_history,
        survivor_history,
        selected_edge_live: exact_edge_rows(db, fixture.selected_edge),
        survivor_edge_live: exact_edge_rows(db, fixture.survivor_edge),
        selected_edge_history_count: selected_edge_history.len(),
        survivor_edge_history_count: survivor_edge_history.len(),
        selected_edge_history,
        survivor_edge_history,
        durable_change_log: durable_change_log(db),
        selected_vector_present: vectors.contains(&fixture.selected_note.row_id),
        survivor_vector_present: vectors.contains(&fixture.survivor_note.row_id),
        hnsw_len: db
            .__debug_vector_hnsw_len(VectorIndexRef::new(NOTES, "embedding"))
            .expect("materialized fixture HNSW"),
        graph: graph_probe(db, fixture),
        durable_sink_payloads: durable_sink_payloads(db),
        memory_sink_payloads: memory_sink_payloads(db),
        selected_note_disk_sync_source: disk_sync_source(db, NOTES, fixture.selected_note.row_id),
        survivor_note_disk_sync_source: disk_sync_source(db, NOTES, fixture.survivor_note.row_id),
        selected_edge_disk_sync_source: disk_sync_source(
            db,
            EDGES,
            fixture.selected_edge_lineage.row_id,
        ),
        survivor_edge_disk_sync_source: disk_sync_source(
            db,
            EDGES,
            fixture.survivor_edge_lineage.row_id,
        ),
        selected_note_memory_sync_source: memory_sync_source(
            db,
            NOTES,
            fixture.selected_note.row_id,
        ),
        survivor_note_memory_sync_source: memory_sync_source(
            db,
            NOTES,
            fixture.survivor_note.row_id,
        ),
        selected_edge_memory_sync_source: memory_sync_source(
            db,
            EDGES,
            fixture.selected_edge_lineage.row_id,
        ),
        survivor_edge_memory_sync_source: memory_sync_source(
            db,
            EDGES,
            fixture.survivor_edge_lineage.row_id,
        ),
        selected_note_live_sidecar: db
            .authoritative_purge_live_row_sidecar_for_test(&fixture.note_selection),
        survivor_note_live_sidecar: db
            .authoritative_purge_live_row_sidecar_for_test(&fixture.survivor_note_selection),
        selected_edge_live_sidecar: db
            .authoritative_purge_live_row_sidecar_for_test(&fixture.edge_selection),
        survivor_edge_live_sidecar: db
            .authoritative_purge_live_row_sidecar_for_test(&fixture.survivor_edge_selection),
        selected_deletion_state: db
            .durable_deletion_state_for_test(NOTES, &Value::Uuid(fixture.selected)),
        survivor_deletion_state: db
            .durable_deletion_state_for_test(NOTES, &Value::Uuid(fixture.survivor)),
        selected_edge_deletion_state: db
            .durable_deletion_state_for_test(EDGES, &Value::Uuid(fixture.selected_edge)),
        survivor_edge_deletion_state: db
            .durable_deletion_state_for_test(EDGES, &Value::Uuid(fixture.survivor_edge)),
    }
}

fn fixture_seed() -> FixtureSeed {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("authoritative-purge.db");
    let db = Database::open(&path).expect("open file db");
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT, embedding VECTOR(3)) SYNC CONFLICT KEEP LATEST",
        &HashMap::new(),
    )
    .expect("create notes");
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) SYNC CONFLICT KEEP LATEST",
        &HashMap::new(),
    )
    .expect("create durable edge owner table");
    db.execute(
        "CREATE INDEX notes_body_idx ON notes (body)",
        &HashMap::new(),
    )
    .expect("create secondary index");
    db.execute(
        "CREATE EVENT TYPE purge_note_insert WHEN INSERT ON notes",
        &HashMap::new(),
    )
    .expect("create event type");
    db.execute("CREATE SINK purge_archive TYPE callback", &HashMap::new())
        .expect("create sink");
    db.execute(
        "CREATE ROUTE purge_notes EVENT purge_note_insert TO purge_archive",
        &HashMap::new(),
    )
    .expect("create route");

    let selected = Uuid::new_v4();
    db.execute(
        "INSERT INTO notes (id, body, embedding) VALUES ($id, $body, '[1,0,0]')",
        &params(selected, "selected-before-update"),
    )
    .expect("insert selected note");
    FixtureSeed {
        root,
        path,
        db,
        selected,
    }
}

fn finish_fixture(seed: &FixtureSeed, pinned_snapshot: SnapshotId) -> Fixture {
    let db = &seed.db;
    let selected = seed.selected;
    let survivor = Uuid::new_v4();
    db.execute(
        "UPDATE notes SET body = $body WHERE id = $id",
        &params(selected, "selected-after-update"),
    )
    .expect("create selected superseded version");
    db.execute(
        "INSERT INTO notes (id, body, embedding) VALUES ($id, $body, '[1,0,0]')",
        &params(survivor, "survivor"),
    )
    .expect("insert survivor note");
    for ordinal in 0..998 {
        db.execute(
            "INSERT INTO notes (id, body, embedding) VALUES ($id, $body, '[-1,0,0]')",
            &params(Uuid::new_v4(), &format!("threshold-corpus-{ordinal}")),
        )
        .expect("seed real HNSW threshold corpus");
    }
    let hnsw_query = db
        .execute(
            "SELECT id FROM notes ORDER BY embedding <=> [1,0,0] LIMIT 10",
            &HashMap::new(),
        )
        .expect("materialize HNSW");
    assert!(hnsw_query.trace.physical_plan.contains("HNSWSearch"));

    let selected_edge = Uuid::new_v4();
    let survivor_edge = Uuid::new_v4();
    let selected_graph_target = Uuid::new_v4();
    let survivor_graph_target = Uuid::new_v4();
    for (id, source_id, target_id) in [
        (selected_edge, selected, selected_graph_target),
        (survivor_edge, survivor, survivor_graph_target),
    ] {
        db.execute(
            "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source_id, $target_id, $edge_type)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(id)),
                ("source_id".to_string(), Value::Uuid(source_id)),
                ("target_id".to_string(), Value::Uuid(target_id)),
                ("edge_type".to_string(), Value::Text(EDGE_TYPE.to_string())),
            ]),
        )
        .expect("insert durable edge owner row");
    }

    let selected_note_key = NaturalKey::single("id".to_string(), Value::Uuid(selected));
    let survivor_note_key = NaturalKey::single("id".to_string(), Value::Uuid(survivor));
    let selected_edge_key = NaturalKey::single("id".to_string(), Value::Uuid(selected_edge));
    let survivor_edge_key = NaturalKey::single("id".to_string(), Value::Uuid(survivor_edge));

    let selected_current = current_row_change(db, NOTES, &selected_note_key);
    let survivor_current = current_row_change(db, NOTES, &survivor_note_key);
    let selected_edge_current = current_row_change(db, EDGES, &selected_edge_key);
    let survivor_edge_current = current_row_change(db, EDGES, &survivor_edge_key);
    let current_rows = vec![
        selected_current.clone(),
        survivor_current.clone(),
        selected_edge_current.clone(),
        survivor_edge_current.clone(),
    ];
    let fixture_tenant = TenantId::from("authoritative-purge-kernel-fixture");
    let fixture_identity = crate::identity::FabricIdentity::generate();
    let fixture_node_id = fixture_identity.node_id();
    let fixture_incarnation = Incarnation(0xb2);
    let bound_lineages = db
        .outbound_row_lineages(
            &ChangeSet {
                rows: current_rows.clone(),
                ..ChangeSet::default()
            },
            &fixture_tenant,
            &fixture_node_id,
            fixture_incarnation,
            &|bytes| Ok(fixture_identity.sign_lineage(bytes)),
        )
        .expect("bind exact current fixture rows through outbound lineage");
    let lineage_key = |row: &RowChange| {
        (
            row.table.clone(),
            rmp_serde::to_vec(&row.natural_key).expect("fixture natural key serializes"),
            row.lsn,
        )
    };
    let expected_lineage_keys = current_rows.iter().map(lineage_key).collect::<HashSet<_>>();
    assert_eq!(
        bound_lineages.len(),
        4,
        "one lineage per current fixture row"
    );
    assert_eq!(
        bound_lineages.keys().cloned().collect::<HashSet<_>>(),
        expected_lineage_keys,
        "outbound binding covers the exact current fixture rows"
    );
    for row in &current_rows {
        let lineage = bound_lineages
            .get(&lineage_key(row))
            .expect("each exact current fixture row has a bound lineage");
        assert_eq!(lineage.author_node_id, fixture_node_id);
        assert_eq!(lineage.author_database_incarnation, fixture_incarnation);
        assert_eq!(
            lineage.lineage_root,
            format!(
                "author:{}:{}:{}",
                fixture_node_id,
                fixture_incarnation.to_hex(),
                lineage.author_local_mutation_position.0
            )
        );
        assert!(
            !lineage.attestation.is_empty(),
            "every bound fixture lineage carries the identity signature"
        );
    }
    let selected_lineage = bound_lineages
        .get(&lineage_key(&selected_current))
        .expect("selected current note has a bound lineage");
    assert!(
        selected_lineage.author_local_mutation_position < selected_current.lsn,
        "the body-only update retains its original creation root"
    );

    let note_selection = db
        .resolve_authoritative_purge_selection(NOTES, &selected_note_key)
        .expect("resolve selected note lineage");
    let edge_selection = db
        .resolve_authoritative_purge_selection(EDGES, &selected_edge_key)
        .expect("resolve selected edge lineage separately from notes");
    let survivor_note_selection = db
        .resolve_authoritative_purge_selection(NOTES, &survivor_note_key)
        .expect("resolve survivor note lineage");
    let survivor_edge_selection = db
        .resolve_authoritative_purge_selection(EDGES, &survivor_edge_key)
        .expect("resolve survivor edge lineage");
    assert_eq!(
        note_selection.lineage_root,
        bound_lineages
            .get(&lineage_key(&selected_current))
            .expect("selected current note lineage remains bound")
            .lineage_root
    );
    assert_eq!(
        edge_selection.lineage_root,
        bound_lineages
            .get(&lineage_key(&selected_edge_current))
            .expect("selected current edge lineage remains bound")
            .lineage_root
    );
    assert_eq!(
        survivor_note_selection.lineage_root,
        bound_lineages
            .get(&lineage_key(&survivor_current))
            .expect("survivor current note lineage remains bound")
            .lineage_root
    );
    assert_eq!(
        survivor_edge_selection.lineage_root,
        bound_lineages
            .get(&lineage_key(&survivor_edge_current))
            .expect("survivor current edge lineage remains bound")
            .lineage_root
    );

    db.record_hub_accepted_rows(&[selected_current], Lsn(701), Some("source-proof"))
        .expect("persist selected source provenance through the acknowledgement path");
    db.record_hub_accepted_rows(&[survivor_current], Lsn(702), Some("source-proof"))
        .expect("persist survivor source provenance through the acknowledgement path");
    db.record_hub_accepted_rows(&[selected_edge_current], Lsn(703), Some("source-proof"))
        .expect("persist selected edge source provenance through the acknowledgement path");
    db.record_hub_accepted_rows(&[survivor_edge_current], Lsn(704), Some("source-proof"))
        .expect("persist survivor edge source provenance through the acknowledgement path");

    Fixture {
        selected_note: captured(&note_selection, row_id(db, NOTES, selected)),
        survivor_note: captured(&survivor_note_selection, row_id(db, NOTES, survivor)),
        selected_edge_lineage: captured(&edge_selection, row_id(db, EDGES, selected_edge)),
        survivor_edge_lineage: captured(&survivor_edge_selection, row_id(db, EDGES, survivor_edge)),
        path: seed.path.clone(),
        note_selection,
        edge_selection,
        survivor_note_selection,
        survivor_edge_selection,
        selected,
        survivor,
        selected_edge,
        survivor_edge,
        selected_graph_target,
        survivor_graph_target,
        pinned_snapshot,
    }
}

fn assert_exact_purge_fence(error: Error, selected: &CapturedLineage, frontier: Lsn) {
    match error {
        Error::PurgeCausalityFence {
            table,
            key,
            lineage_root,
            frontier: observed_frontier,
        } => {
            assert_eq!(table, NOTES);
            assert_eq!(key, selected.natural_key.pairs());
            assert_eq!(lineage_root, selected.lineage_root);
            assert_eq!(observed_frontier, frontier);
        }
        _ => panic!("selected descendant must receive the purge-causality fence"),
    }
}

fn assert_exact_export_fence(error: Error, selected: &CapturedLineage, frontier: Lsn) {
    match error {
        Error::PurgeExportSnapshotFence {
            table,
            key,
            lineage_root,
            frontier: observed_frontier,
            snapshot_lsn: observed_snapshot_lsn,
        } => {
            assert_eq!(table, NOTES);
            assert_eq!(key, selected.natural_key.pairs());
            assert_eq!(lineage_root, selected.lineage_root);
            assert_eq!(observed_frontier, frontier);
            assert!(observed_snapshot_lsn < frontier);
        }
        _ => panic!("export must receive the purge/export snapshot fence"),
    }
}

fn assert_no_export_attempt_files(root: &std::path::Path) {
    for entry in std::fs::read_dir(root).expect("read export fixture directory") {
        let entry = entry.expect("read export fixture entry");
        let name = entry.file_name();
        let name = name.to_string_lossy();
        assert!(
            !name.starts_with("purged-export.redb."),
            "export must remove its unpublished temporary artifact and lock"
        );
    }
}

fn assert_snapshot_is_fenced_or_empty(db: &Database, fixture: &Fixture, frontier: Lsn) {
    match db.execute_at_snapshot(
        "SELECT id, body FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(fixture.selected))]),
        fixture.pinned_snapshot,
    ) {
        Ok(result) => assert!(
            result.rows.is_empty(),
            "pinned read cannot expose purged bytes"
        ),
        Err(error) => assert_exact_purge_fence(error, &fixture.selected_note, frontier),
    }
}

fn assert_durable_purge_record(
    db: &Database,
    captured: &CapturedLineage,
    table: &str,
    id: Uuid,
    frontier: Lsn,
) {
    let state = db.durable_deletion_state_for_test(table, &Value::Uuid(id));
    assert_eq!(state.table_generation, Some(captured.table_generation));
    assert_eq!(state.lineage_root, Some(captured.lineage_root.clone()));
    assert_eq!(state.purge_frontier, Some(frontier.to_string()));
    assert_eq!(state.delete_obligation, None);
    assert_eq!(state.accepted_delete_marker, None);
}

fn assert_full_selected_absence_after_reopen(
    db: &Database,
    fixture: &Fixture,
    note_frontier: Lsn,
    edge_frontier: Lsn,
    survivor_before: &OwnedCopies,
) {
    assert!(exact_rows(db, NOTES, fixture.selected, None).is_empty());
    assert!(exact_edge_rows(db, fixture.selected_edge).is_empty());
    let selected_secondary_index = secondary_index_rows(db, "selected-after-update");
    let survivor_secondary_index = secondary_index_rows(db, "survivor");
    assert!(selected_secondary_index.rows.is_empty());
    assert_eq!(
        selected_secondary_index.index_used.as_deref(),
        Some("notes_body_idx")
    );
    assert_eq!(
        survivor_secondary_index,
        survivor_before.survivor_secondary_index
    );
    assert_eq!(
        survivor_secondary_index.index_used.as_deref(),
        Some("notes_body_idx")
    );
    let reopened_changes = db.changes_since(Lsn(0));
    assert!(
        reopened_changes.rows.iter().all(|row| {
            row.table != NOTES || row.natural_key != fixture.selected_note.natural_key
        })
    );
    assert!(reopened_changes.rows.iter().all(|row| {
        row.table != EDGES || row.natural_key != fixture.selected_edge_lineage.natural_key
    }));
    assert_eq!(
        reopened_changes
            .rows
            .iter()
            .filter(|row| row.table == NOTES && row.natural_key == fixture.survivor_note.natural_key)
            .cloned()
            .collect::<Vec<_>>(),
        survivor_before.survivor_history,
    );
    assert_eq!(
        reopened_changes
            .rows
            .iter()
            .filter(|row| {
                row.table == EDGES && row.natural_key == fixture.survivor_edge_lineage.natural_key
            })
            .cloned()
            .collect::<Vec<_>>(),
        survivor_before.survivor_edge_history,
    );
    assert_eq!(
        exact_edge_rows(db, fixture.survivor_edge),
        survivor_before.survivor_edge_live
    );
    let reopened_vectors = vector_row_ids(db);
    assert!(!reopened_vectors.contains(&fixture.selected_note.row_id));
    assert!(reopened_vectors.contains(&fixture.survivor_note.row_id));
    assert_eq!(
        db.__debug_vector_hnsw_len(VectorIndexRef::new(NOTES, "embedding")),
        None,
        "after reopen the 999-survivor query follows the ordinary below-threshold lazy path"
    );
    assert!(graph_neighbors(db, fixture.selected, Direction::Outgoing).is_empty());
    assert!(graph_neighbors(db, fixture.selected_graph_target, Direction::Incoming).is_empty());
    assert_eq!(
        graph_neighbors(db, fixture.survivor, Direction::Outgoing),
        survivor_before.graph.survivor_forward
    );
    assert_eq!(
        graph_neighbors(db, fixture.survivor_graph_target, Direction::Incoming),
        survivor_before.graph.survivor_reverse
    );
    let durable_sink_payloads = durable_sink_payloads(db);
    let memory_sink_payloads = memory_sink_payloads(db);
    assert_eq!(durable_sink_payloads, memory_sink_payloads);
    assert!(!durable_sink_payloads.contains_key(&fixture.selected_note.row_id));
    assert!(!memory_sink_payloads.contains_key(&fixture.selected_note.row_id));
    assert_eq!(
        durable_sink_payloads.get(&fixture.survivor_note.row_id),
        survivor_before
            .durable_sink_payloads
            .get(&fixture.survivor_note.row_id),
    );
    assert_eq!(
        memory_sink_payloads.get(&fixture.survivor_note.row_id),
        survivor_before
            .memory_sink_payloads
            .get(&fixture.survivor_note.row_id),
    );
    assert_eq!(
        disk_sync_source(db, NOTES, fixture.selected_note.row_id),
        None
    );
    assert_eq!(
        memory_sync_source(db, NOTES, fixture.selected_note.row_id),
        None
    );
    assert_eq!(
        disk_sync_source(db, EDGES, fixture.selected_edge_lineage.row_id),
        None
    );
    assert_eq!(
        memory_sync_source(db, EDGES, fixture.selected_edge_lineage.row_id),
        None
    );
    assert_eq!(
        disk_sync_source(db, NOTES, fixture.survivor_note.row_id),
        survivor_before.survivor_note_disk_sync_source
    );
    assert_eq!(
        memory_sync_source(db, NOTES, fixture.survivor_note.row_id),
        survivor_before.survivor_note_memory_sync_source
    );
    assert_eq!(
        disk_sync_source(db, EDGES, fixture.survivor_edge_lineage.row_id),
        survivor_before.survivor_edge_disk_sync_source
    );
    assert_eq!(
        memory_sync_source(db, EDGES, fixture.survivor_edge_lineage.row_id),
        survivor_before.survivor_edge_memory_sync_source
    );
    assert_eq!(
        db.authoritative_purge_live_row_sidecar_for_test(&fixture.note_selection),
        None
    );
    assert_eq!(
        db.authoritative_purge_live_row_sidecar_for_test(&fixture.edge_selection),
        None
    );
    assert_eq!(
        db.authoritative_purge_live_row_sidecar_for_test(&fixture.survivor_note_selection),
        survivor_before.survivor_note_live_sidecar
    );
    assert_eq!(
        db.authoritative_purge_live_row_sidecar_for_test(&fixture.survivor_edge_selection),
        survivor_before.survivor_edge_live_sidecar
    );
    assert_durable_purge_record(
        db,
        &fixture.selected_note,
        NOTES,
        fixture.selected,
        note_frontier,
    );
    assert_durable_purge_record(
        db,
        &fixture.selected_edge_lineage,
        EDGES,
        fixture.selected_edge,
        edge_frontier,
    );
    assert_eq!(
        db.durable_deletion_state_for_test(NOTES, &Value::Uuid(fixture.survivor)),
        survivor_before.survivor_deletion_state
    );
    assert_eq!(
        db.durable_deletion_state_for_test(EDGES, &Value::Uuid(fixture.survivor_edge)),
        survivor_before.survivor_edge_deletion_state
    );
}

#[test]
fn authoritative_file_purge_kernel_removes_only_the_immutable_selected_lineage() {
    let seed = fixture_seed();
    let pinned_snapshot = seed.db.snapshot();
    let pinned = seed.db.pin_snapshot(pinned_snapshot);
    let fixture = finish_fixture(&seed, pinned_snapshot);
    let before = owned_copies(&seed.db, &fixture);
    assert_eq!(before.selected_note_disk_sync_source, Some((Lsn(701), 1)));
    assert_eq!(before.survivor_note_disk_sync_source, Some((Lsn(702), 1)));
    assert_eq!(before.selected_edge_disk_sync_source, Some((Lsn(703), 1)));
    assert_eq!(before.survivor_edge_disk_sync_source, Some((Lsn(704), 1)));
    assert_eq!(before.selected_note_memory_sync_source, Some((Lsn(701), 1)));
    assert_eq!(before.survivor_note_memory_sync_source, Some((Lsn(702), 1)));
    assert_eq!(before.selected_edge_memory_sync_source, Some((Lsn(703), 1)));
    assert_eq!(before.survivor_edge_memory_sync_source, Some((Lsn(704), 1)));
    assert!(!before.selected_live.is_empty());
    assert!(!before.selected_secondary_index.rows.is_empty());
    assert_eq!(
        before.selected_secondary_index.index_used.as_deref(),
        Some("notes_body_idx")
    );
    assert_eq!(
        before.survivor_secondary_index.index_used.as_deref(),
        Some("notes_body_idx")
    );
    assert!(before.selected_history_count >= 2);
    assert!(before.survivor_history_count >= 1);
    assert!(before.selected_vector_present);
    assert!(
        before
            .durable_sink_payloads
            .contains_key(&fixture.selected_note.row_id)
    );
    assert!(
        before
            .memory_sink_payloads
            .contains_key(&fixture.selected_note.row_id)
    );
    assert_eq!(before.durable_sink_payloads, before.memory_sink_payloads);
    assert!(before.selected_note_live_sidecar.is_some());
    assert!(before.survivor_note_live_sidecar.is_some());
    assert!(before.selected_edge_live_sidecar.is_some());
    assert!(before.survivor_edge_live_sidecar.is_some());
    assert_eq!(
        before.graph.selected_forward,
        BTreeSet::from([fixture.selected_graph_target])
    );
    assert_eq!(
        before.graph.selected_reverse,
        BTreeSet::from([fixture.selected])
    );
    assert_eq!(
        before.graph.survivor_forward,
        BTreeSet::from([fixture.survivor_graph_target])
    );
    assert_eq!(
        before.graph.survivor_reverse,
        BTreeSet::from([fixture.survivor])
    );

    let note_frontier = seed
        .db
        .commit_authoritative_purge_kernel(&fixture.note_selection)
        .expect("commit selected note lineage purge");
    let after_notes = owned_copies(&seed.db, &fixture);
    assert!(after_notes.selected_live.is_empty());
    assert!(after_notes.selected_secondary_index.rows.is_empty());
    assert_eq!(
        after_notes.selected_secondary_index.index_used.as_deref(),
        Some("notes_body_idx")
    );
    assert!(after_notes.selected_history.is_empty());
    assert_eq!(after_notes.selected_history_count, 0);
    assert!(!after_notes.selected_vector_present);
    assert!(after_notes.survivor_vector_present);
    assert_eq!(after_notes.hnsw_len, before.hnsw_len - 1);
    assert!(
        !after_notes
            .durable_sink_payloads
            .contains_key(&fixture.selected_note.row_id)
    );
    assert!(
        !after_notes
            .memory_sink_payloads
            .contains_key(&fixture.selected_note.row_id)
    );
    assert_eq!(after_notes.selected_note_disk_sync_source, None);
    assert_eq!(after_notes.selected_note_memory_sync_source, None);
    assert_eq!(after_notes.selected_note_live_sidecar, None);
    assert_eq!(
        after_notes
            .durable_sink_payloads
            .get(&fixture.survivor_note.row_id),
        before
            .durable_sink_payloads
            .get(&fixture.survivor_note.row_id),
    );
    assert_eq!(
        after_notes
            .memory_sink_payloads
            .get(&fixture.survivor_note.row_id),
        before
            .memory_sink_payloads
            .get(&fixture.survivor_note.row_id),
    );
    assert_eq!(
        after_notes.durable_sink_payloads,
        after_notes.memory_sink_payloads
    );
    assert_eq!(after_notes.survivor_live, before.survivor_live);
    assert_eq!(
        after_notes.survivor_secondary_index,
        before.survivor_secondary_index
    );
    assert_eq!(
        after_notes.survivor_secondary_index.index_used.as_deref(),
        Some("notes_body_idx")
    );
    assert_eq!(after_notes.survivor_history, before.survivor_history);
    assert_eq!(
        after_notes.survivor_history_count,
        before.survivor_history_count
    );
    assert_eq!(
        after_notes.survivor_note_disk_sync_source,
        before.survivor_note_disk_sync_source
    );
    assert_eq!(
        after_notes.survivor_note_memory_sync_source,
        before.survivor_note_memory_sync_source
    );
    assert_eq!(after_notes.selected_edge_live, before.selected_edge_live);
    assert_eq!(after_notes.survivor_edge_live, before.survivor_edge_live);
    assert_eq!(
        after_notes.selected_edge_history,
        before.selected_edge_history
    );
    assert_eq!(
        after_notes.survivor_edge_history,
        before.survivor_edge_history
    );
    assert_eq!(
        after_notes.selected_edge_disk_sync_source,
        before.selected_edge_disk_sync_source
    );
    assert_eq!(
        after_notes.survivor_edge_disk_sync_source,
        before.survivor_edge_disk_sync_source
    );
    assert_eq!(
        after_notes.selected_edge_memory_sync_source,
        before.selected_edge_memory_sync_source
    );
    assert_eq!(
        after_notes.survivor_edge_memory_sync_source,
        before.survivor_edge_memory_sync_source
    );
    assert_eq!(after_notes.graph, before.graph);
    assert_eq!(
        after_notes.selected_edge_live_sidecar,
        before.selected_edge_live_sidecar
    );
    assert_eq!(
        after_notes.survivor_edge_live_sidecar,
        before.survivor_edge_live_sidecar
    );
    assert_snapshot_is_fenced_or_empty(&seed.db, &fixture, note_frontier);
    assert_eq!(
        after_notes.survivor_note_live_sidecar,
        before.survivor_note_live_sidecar
    );

    let edge_frontier = seed
        .db
        .commit_authoritative_purge_kernel(&fixture.edge_selection)
        .expect("commit selected edge lineage purge separately from notes");
    let after_edges = owned_copies(&seed.db, &fixture);
    assert!(after_edges.selected_edge_live.is_empty());
    assert!(after_edges.selected_edge_history.is_empty());
    assert_eq!(after_edges.selected_edge_history_count, 0);
    assert_eq!(after_edges.selected_edge_disk_sync_source, None);
    assert_eq!(after_edges.selected_edge_memory_sync_source, None);
    assert_eq!(after_edges.selected_edge_live_sidecar, None);
    assert_eq!(after_edges.survivor_edge_live, before.survivor_edge_live);
    assert_eq!(
        after_edges.survivor_edge_history,
        before.survivor_edge_history
    );
    assert_eq!(
        after_edges.survivor_edge_history_count,
        before.survivor_edge_history_count
    );
    assert_eq!(
        after_edges.survivor_edge_disk_sync_source,
        before.survivor_edge_disk_sync_source
    );
    assert_eq!(
        after_edges.survivor_edge_memory_sync_source,
        before.survivor_edge_memory_sync_source
    );
    assert_eq!(
        after_edges.survivor_edge_live_sidecar,
        before.survivor_edge_live_sidecar
    );
    assert!(after_edges.graph.selected_forward.is_empty());
    assert!(after_edges.graph.selected_reverse.is_empty());
    assert_eq!(
        after_edges.graph.survivor_forward,
        before.graph.survivor_forward
    );
    assert_eq!(
        after_edges.graph.survivor_reverse,
        before.graph.survivor_reverse
    );
    assert_eq!(
        after_edges.survivor_deletion_state,
        before.survivor_deletion_state
    );
    assert_durable_purge_record(
        &seed.db,
        &fixture.selected_note,
        NOTES,
        fixture.selected,
        note_frontier,
    );
    assert_durable_purge_record(
        &seed.db,
        &fixture.selected_edge_lineage,
        EDGES,
        fixture.selected_edge,
        edge_frontier,
    );

    drop(pinned);
    seed.db.close().expect("close after purge");
    let reopened = Database::open(&fixture.path).expect("reopen purged database");
    assert_full_selected_absence_after_reopen(
        &reopened,
        &fixture,
        note_frontier,
        edge_frontier,
        &before,
    );
    assert_eq!(
        reopened.classify_authoritative_purge_root_for_test(
            NOTES,
            fixture.selected_note.table_generation,
            &fixture.selected_note.natural_key,
            &fixture.selected_note.lineage_root,
        ),
        AuthoritativePurgeRootClassification::Purged {
            permanent_frontier: note_frontier,
        }
    );
    assert_eq!(
        reopened.classify_authoritative_purge_root_for_test(
            NOTES,
            fixture.selected_note.table_generation,
            &fixture.selected_note.natural_key,
            &format!("{}:different-life", fixture.selected_note.lineage_root),
        ),
        AuthoritativePurgeRootClassification::NotPurged
    );
}

#[test]
fn authoritative_file_purge_kernel_fences_stale_transactions_and_exports() {
    let seed = fixture_seed();
    let pinned_snapshot = seed.db.snapshot();
    let pinned = seed.db.pin_snapshot(pinned_snapshot);
    let fixture = finish_fixture(&seed, pinned_snapshot);
    let edge_frontier = seed
        .db
        .commit_authoritative_purge_kernel(&fixture.edge_selection)
        .expect("commit edge purge before export captures its snapshot");
    let edge_purged_baseline = owned_copies(&seed.db, &fixture);
    assert!(edge_purged_baseline.selected_edge_live.is_empty());
    assert!(edge_purged_baseline.graph.selected_forward.is_empty());
    assert!(edge_purged_baseline.graph.selected_reverse.is_empty());
    assert!(
        edge_purged_baseline
            .durable_sink_payloads
            .contains_key(&fixture.selected_note.row_id)
    );
    assert!(
        edge_purged_baseline
            .memory_sink_payloads
            .contains_key(&fixture.selected_note.row_id)
    );
    assert_eq!(
        edge_purged_baseline.durable_sink_payloads,
        edge_purged_baseline.memory_sink_payloads
    );
    let stale_tx = seed
        .db
        .begin()
        .expect("begin selected descendant transaction before purge");
    seed.db
        .execute_in_tx(
            stale_tx,
            "UPDATE notes SET body = 'stale-descendant' WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(fixture.selected))]),
        )
        .expect("stage selected descendant write");
    let unrelated_tx = seed
        .db
        .begin()
        .expect("begin unrelated transaction before purge");
    seed.db
        .execute_in_tx(
            unrelated_tx,
            "UPDATE notes SET body = 'survivor-tx' WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(fixture.survivor))]),
        )
        .expect("stage unrelated survivor write");

    let export_path = seed.root.path().join("purged-export.redb");
    let pause = seed.db.pause_after_export_snapshot_capture_for_test();
    std::thread::scope(|scope| {
        let export = scope.spawn(|| seed.db.export_snapshot(&export_path));
        assert!(
            pause.wait_until_reached(Duration::from_secs(5)),
            "export must capture its snapshot before the private kernel commits"
        );
        let note_frontier = seed
            .db
            .commit_authoritative_purge_kernel(&fixture.note_selection)
            .expect("commit note purge while export is paused");
        // The export has observed the required ordering. Release it before
        // any assertion that can panic so scoped-thread cleanup cannot strand
        // the exporter at this deterministic test pause.
        pause.release();
        let export_result = export.join().expect("export thread did not panic");
        let note_purged = owned_copies(&seed.db, &fixture);
        assert!(note_purged.selected_live.is_empty());
        assert_eq!(
            note_purged.selected_edge_live,
            edge_purged_baseline.selected_edge_live
        );
        assert_eq!(
            note_purged.selected_edge_history,
            edge_purged_baseline.selected_edge_history
        );
        assert_eq!(note_purged.graph, edge_purged_baseline.graph);
        assert_exact_purge_fence(
            seed.db
                .commit(stale_tx)
                .expect_err("selected descendant cannot resurrect purged lineage"),
            &fixture.selected_note,
            note_frontier,
        );
        seed.db
            .commit(unrelated_tx)
            .expect("unrelated pre-purge transaction remains committable");
        assert_eq!(
            exact_rows(&seed.db, NOTES, fixture.survivor, None),
            vec![vec![
                Value::Uuid(fixture.survivor),
                Value::Text("survivor-tx".to_string())
            ]],
        );
        assert_snapshot_is_fenced_or_empty(&seed.db, &fixture, note_frontier);
        match export_result {
            Ok(_) => {
                let artifact =
                    Database::open(&export_path).expect("open published export artifact");
                assert_full_selected_absence_after_reopen(
                    &artifact,
                    &fixture,
                    note_frontier,
                    edge_frontier,
                    &edge_purged_baseline,
                );
            }
            Err(error) => {
                assert_exact_export_fence(error, &fixture.selected_note, note_frontier);
                assert!(
                    !export_path.exists(),
                    "a fenced export cannot leave an artifact containing selected bytes"
                );
            }
        }
        assert_no_export_attempt_files(seed.root.path());
    });
    drop(pinned);
}

#[test]
fn authoritative_file_purge_kernel_rolls_back_every_owned_copy_on_persistence_failure() {
    let seed = fixture_seed();
    let pinned_snapshot = seed.db.snapshot();
    let pinned = seed.db.pin_snapshot(pinned_snapshot);
    let fixture = finish_fixture(&seed, pinned_snapshot);
    let before = owned_copies(&seed.db, &fixture);
    seed.db
        .arm_authoritative_purge_point_remove_persistence_failure_for_test();
    assert!(
        seed.db
            .commit_authoritative_purge_kernel(&fixture.note_selection)
            .is_err(),
        "the staged point-removes must fail before their one Redb commit"
    );
    drop(pinned);
    seed.db
        .close()
        .expect("close after injected persistence failure");
    let reopened_notes = Database::open(&fixture.path).expect("reopen failed-note-purge database");
    let after_notes = owned_copies(&reopened_notes, &fixture);
    assert_owned_copies_equal_after_reopen(&before, &after_notes);
    reopened_notes.arm_authoritative_purge_point_remove_persistence_failure_for_test();
    assert!(
        reopened_notes
            .commit_authoritative_purge_kernel(&fixture.edge_selection)
            .is_err(),
        "the graph-owning edge selection must fail before its one Redb commit"
    );
    reopened_notes
        .close()
        .expect("close after injected edge persistence failure");
    let reopened = Database::open(&fixture.path).expect("reopen failed-edge-purge database");
    let after = owned_copies(&reopened, &fixture);
    assert_owned_copies_equal_after_reopen(&before, &after);
    assert_eq!(
        exact_rows(
            &reopened,
            NOTES,
            fixture.selected,
            Some(fixture.pinned_snapshot)
        ),
        vec![vec![
            Value::Uuid(fixture.selected),
            Value::Text("selected-before-update".to_string()),
        ]],
    );
}
