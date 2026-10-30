//! Guards for `commit_index_across_all`, the change-log → `BTreeMap<Lsn, TxId>`
//! reconstruction that `Database::open` runs unconditionally on every open.
//!
//! Two contracts are covered:
//!   * `commit_index_matches_naive_across_shapes` — the OPTIMIZED implementation
//!     must produce a byte-identical index to the original O(deletes × rows)
//!     algorithm (frozen here as `naive_commit_index`) for every representative
//!     change-log shape, including delete-of-nonexistent-row and
//!     delete-after-multiple-inserts-of-the-same-id.
//!   * `commit_index_reconstruction_is_not_quadratic` — the reconstruction must
//!     complete in near-linear time. This is the regression that took down the
//!     owner's node: the original code linearly rescanned every row in a table
//!     for EACH `RowDelete` entry, so the work ledger's per-input retention
//!     deletes made `open` O(deletes × rows) and pinned the boot thread at 100%
//!     CPU for minutes until the Supervisor watchdog killed the add-on.
use super::*;

fn row(row_id: RowId, created_tx: u64, deleted_tx: Option<u64>, lsn: Lsn) -> VersionedRow {
    VersionedRow {
        row_id,
        values: HashMap::new(),
        created_tx: TxId(created_tx),
        deleted_tx: deleted_tx.map(TxId),
        lsn,
        created_at: None,
    }
}

fn edge(
    source: NodeId,
    target: NodeId,
    created_tx: u64,
    deleted_tx: Option<u64>,
    lsn: Lsn,
) -> AdjEntry {
    AdjEntry {
        source,
        target,
        edge_type: "REL".to_string(),
        properties: HashMap::new(),
        created_tx: TxId(created_tx),
        deleted_tx: deleted_tx.map(TxId),
        lsn,
    }
}

fn relational_with(tables: Vec<(&str, Vec<VersionedRow>)>) -> RelationalStore {
    let store = RelationalStore::new();
    {
        let mut guard = store.tables.write();
        for (name, rows) in tables {
            guard.insert(name.to_string(), rows);
        }
    }
    store
}

fn graph_with(edges: Vec<AdjEntry>) -> GraphStore {
    let store = GraphStore::new();
    {
        let mut fwd = store.forward_adj.write();
        for e in edges {
            fwd.entry(e.source).or_default().push(e.into());
        }
    }
    store
}

fn empty_vector() -> VectorStore {
    VectorStore::default()
}

fn row_delete(table: &str, row_id: RowId, lsn: Lsn) -> ChangeLogEntry {
    ChangeLogEntry::RowDelete {
        table: table.to_string(),
        row_id,
        natural_key: NaturalKey::single("id".to_string(), Value::Null),
        lsn,
    }
}

/// The ORIGINAL O(deletes × rows) reconstruction, frozen verbatim so the
/// optimized production function can be proven output-identical. Any drift
/// here would defeat the equivalence guard, so it must not be "improved."
fn naive_commit_index(
    relational: &RelationalStore,
    graph: &GraphStore,
    vector: &VectorStore,
    change_log: &[ChangeLogEntry],
) -> BTreeMap<Lsn, TxId> {
    let mut index = BTreeMap::new();
    let mut add_entry = |lsn: Lsn, tx: TxId| {
        if lsn != Lsn(0) {
            index
                .entry(lsn)
                .and_modify(|current: &mut TxId| *current = (*current).max(tx))
                .or_insert(tx);
        }
    };
    let relational_rows = relational
        .tables
        .read()
        .iter()
        .map(|(table, rows)| (table.clone(), rows.clone()))
        .collect::<Vec<_>>();
    for row in relational
        .tables
        .read()
        .values()
        .flat_map(|rows| rows.iter().cloned())
    {
        add_entry(row.lsn, row.created_tx);
    }
    let graph_entries = graph
        .forward_adj
        .read()
        .values()
        .flat_map(|entries| entries.iter().cloned())
        .collect::<Vec<_>>();
    let vector_fixture_parent_12 =
        contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
            contextdb_engine::OwnerReadConfig::default()
                .limits
                .limits
                .memory,
        )
        .expect("admit the declared fixture root");
    let vector_fixture_reader_12 = vector_fixture_parent_12
        .child()
        .expect("admit the declared fixture reader child");
    let mut vector_fixture_copy_12 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_12.clone());

    for entry in graph
        .forward_adj
        .read()
        .values()
        .flat_map(|entries| entries.iter().cloned())
    {
        add_entry(entry.lsn, entry.created_tx);
    }
    let vector_entries = vector
        .all_entries(&mut vector_fixture_copy_12)
        .expect("read or publish the admitted vector fixture");
    for entry in &vector_entries {
        add_entry(entry.lsn, entry.created_tx);
    }

    for entry in change_log {
        match entry {
            ChangeLogEntry::RowInsert { .. }
            | ChangeLogEntry::EdgeInsert { .. }
            | ChangeLogEntry::VectorInsert { .. } => {}
            ChangeLogEntry::RowDelete {
                table, row_id, lsn, ..
            } => {
                if let Some(deleted_tx) = relational_rows
                    .iter()
                    .find(|(candidate, _)| candidate == table)
                    .and_then(|(_, rows)| {
                        rows.iter()
                            .filter(|row| {
                                row.row_id == *row_id && row.lsn < *lsn && row.deleted_tx.is_some()
                            })
                            .max_by_key(|row| row.lsn)
                            .and_then(|row| row.deleted_tx)
                    })
                {
                    add_entry(*lsn, deleted_tx);
                }
            }
            ChangeLogEntry::EdgeDelete {
                source,
                target,
                edge_type,
                lsn,
            } => {
                if let Some(deleted_tx) = graph_entries
                    .iter()
                    .filter(|edge| {
                        edge.source == *source
                            && edge.target == *target
                            && edge.edge_type == *edge_type
                            && edge.lsn < *lsn
                            && edge.deleted_tx.is_some()
                    })
                    .max_by_key(|edge| edge.lsn)
                    .and_then(|edge| edge.deleted_tx)
                {
                    add_entry(*lsn, deleted_tx);
                }
            }
            ChangeLogEntry::VectorDelete {
                index: vector_index,
                row_id,
                lsn,
            } => {
                if let Some(deleted_tx) = vector_entries
                    .iter()
                    .filter(|vector| {
                        vector.index == *vector_index
                            && vector.row_id == *row_id
                            && vector.lsn < *lsn
                            && vector.deleted_tx.is_some()
                    })
                    .max_by_key(|vector| vector.lsn)
                    .and_then(|vector| vector.deleted_tx)
                {
                    add_entry(*lsn, deleted_tx);
                }
            }
        }
    }
    index
}

#[test]
fn commit_index_matches_naive_across_shapes() {
    let n1 = uuid::Uuid::from_u128(1);
    let n2 = uuid::Uuid::from_u128(2);
    let n3 = uuid::Uuid::from_u128(3);

    // Each case: (relational tables, edges, change_log). Every shape the
    // reconstruction must handle identically to the frozen naive algorithm.
    type Case<'a> = (
        Vec<(&'a str, Vec<VersionedRow>)>,
        Vec<AdjEntry>,
        Vec<ChangeLogEntry>,
    );
    let cases: Vec<Case> = vec![
        // Empty store, no change log.
        (vec![], vec![], vec![]),
        // Inserts only — created_tx entries, no deletes.
        (
            vec![(
                "t",
                vec![
                    row(RowId(1), 5, None, Lsn(10)),
                    row(RowId(2), 6, None, Lsn(12)),
                ],
            )],
            vec![],
            vec![],
        ),
        // Delete of a nonexistent row (no matching tombstone) — must add nothing.
        (
            vec![("t", vec![row(RowId(1), 5, None, Lsn(10))])],
            vec![],
            vec![row_delete("t", RowId(999), Lsn(50))],
        ),
        // Delete against a nonexistent table — must add nothing.
        (
            vec![("t", vec![row(RowId(1), 5, None, Lsn(10))])],
            vec![],
            vec![row_delete("other", RowId(1), Lsn(50))],
        ),
        // Single tombstone resolved by its delete entry.
        (
            vec![("t", vec![row(RowId(1), 5, Some(9), Lsn(10))])],
            vec![],
            vec![row_delete("t", RowId(1), Lsn(20))],
        ),
        // Delete-after-multiple-inserts-of-the-same-id: several tombstoned
        // versions of row_id 1; the delete at lsn 40 must pick the max-lsn
        // tombstone with lsn < 40 (the one at lsn 30 → deleted_tx 31), not the
        // later lsn-45 tombstone, and not the earlier lsn-15 one.
        (
            vec![(
                "t",
                vec![
                    row(RowId(1), 3, Some(4), Lsn(15)),
                    row(RowId(1), 20, Some(21), Lsn(30)),
                    row(RowId(1), 44, Some(46), Lsn(45)),
                ],
            )],
            vec![],
            vec![row_delete("t", RowId(1), Lsn(40))],
        ),
        // Two delete entries against the same id at different thresholds pick
        // different tombstones (proves the resolution is per-entry, not global).
        (
            vec![(
                "t",
                vec![
                    row(RowId(7), 3, Some(4), Lsn(15)),
                    row(RowId(7), 20, Some(21), Lsn(30)),
                ],
            )],
            vec![],
            vec![
                row_delete("t", RowId(7), Lsn(20)),
                row_delete("t", RowId(7), Lsn(40)),
            ],
        ),
        // Edges: inserts + a resolvable edge delete + a nonexistent edge delete.
        (
            vec![],
            vec![
                edge(n1, n2, 5, Some(8), Lsn(10)),
                edge(n1, n3, 6, None, Lsn(12)),
            ],
            vec![
                ChangeLogEntry::EdgeDelete {
                    source: n1,
                    target: n2,
                    edge_type: "REL".to_string(),
                    lsn: Lsn(30),
                },
                ChangeLogEntry::EdgeDelete {
                    source: n2,
                    target: n3,
                    edge_type: "REL".to_string(),
                    lsn: Lsn(31),
                },
            ],
        ),
        // Mixed: rows across two tables + edges + interleaved deletes, with an
        // lsn collision (two entries at the same lsn must resolve to max TxId).
        (
            vec![
                (
                    "a",
                    vec![
                        row(RowId(1), 5, Some(50), Lsn(10)),
                        row(RowId(2), 7, None, Lsn(14)),
                    ],
                ),
                ("b", vec![row(RowId(1), 6, Some(9), Lsn(11))]),
            ],
            vec![edge(n1, n2, 8, Some(60), Lsn(13))],
            vec![
                row_delete("a", RowId(1), Lsn(100)),
                row_delete("b", RowId(1), Lsn(100)),
                ChangeLogEntry::EdgeDelete {
                    source: n1,
                    target: n2,
                    edge_type: "REL".to_string(),
                    lsn: Lsn(100),
                },
            ],
        ),
        // Lsn(0) tombstone: a version at lsn 0 (skipped by add_entry) that a
        // later RowDelete still resolves against. Exercises the tombstone
        // map's Lsn(0) boundary in partition_point / the naive max_by_key.
        (
            vec![(
                "t",
                vec![
                    row(RowId(1), 5, Some(9), Lsn(0)),
                    row(RowId(1), 10, None, Lsn(12)),
                ],
            )],
            vec![],
            vec![row_delete("t", RowId(1), Lsn(8))],
        ),
    ];

    for (idx, (tables, edges, change_log)) in cases.into_iter().enumerate() {
        let relational = relational_with(tables);
        let graph = graph_with(edges);
        let vector = empty_vector();
        let expected = naive_commit_index(&relational, &graph, &vector, &change_log);
        let actual = commit_index_across_all(&relational, &graph, &vector, &change_log);
        assert_eq!(
            actual, expected,
            "case {idx}: optimized commit index must match the frozen naive reconstruction"
        );
    }
}

#[test]
fn commit_index_matches_naive_for_vector_deletes() {
    let standalone_retained_writer_1 =
        std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());

    use contextdb_core::VectorQuantization;
    // Build a vector store with two tombstoned versions of one (index,row_id)
    // plus a live one, so a VectorDelete must resolve to the max-lsn tombstone
    // below its threshold — exactly as the naive scan does.
    let index = VectorIndexRef::new("t", "vec");
    let vector = VectorStore::default();
    vector
        .register_index(
            index.clone(),
            2,
            VectorQuantization::F32,
            standalone_retained_writer_1.clone(),
        )
        .expect("read or publish the admitted vector fixture");
    let entry = |created_tx: u64, deleted_tx: Option<u64>, lsn: Lsn| VectorEntry {
        index: index.clone(),
        row_id: RowId(1),
        vector: vec![0.0, 0.0],
        created_tx: TxId(created_tx),
        deleted_tx: deleted_tx.map(TxId),
        lsn,
    };
    vector
        .insert_loaded_vector(
            entry(3, Some(4), Lsn(15)),
            standalone_retained_writer_1.clone(),
        )
        .expect("read or publish the admitted vector fixture");
    vector
        .insert_loaded_vector(
            entry(20, Some(21), Lsn(30)),
            standalone_retained_writer_1.clone(),
        )
        .expect("read or publish the admitted vector fixture");
    vector
        .insert_loaded_vector(
            entry(44, None, Lsn(45)),
            standalone_retained_writer_1.clone(),
        )
        .expect("read or publish the admitted vector fixture");

    let relational = relational_with(vec![]);
    let graph = graph_with(vec![]);
    let change_log = vec![
        // Resolves against the lsn-30 tombstone (max lsn < 40).
        ChangeLogEntry::VectorDelete {
            index: index.clone(),
            row_id: RowId(1),
            lsn: Lsn(40),
        },
        // Delete of a nonexistent (index,row_id) — must add nothing.
        ChangeLogEntry::VectorDelete {
            index: VectorIndexRef::new("t", "other"),
            row_id: RowId(1),
            lsn: Lsn(40),
        },
    ];

    let expected = naive_commit_index(&relational, &graph, &vector, &change_log);
    let actual = commit_index_across_all(&relational, &graph, &vector, &change_log);
    assert_eq!(
        actual, expected,
        "optimized vector-delete resolution must match the frozen naive reconstruction"
    );
}

#[test]
#[cfg(feature = "test-seams")]
fn commit_index_reconstruction_is_not_quadratic() {
    fn work(
        n: u64,
    ) -> (
        usize,
        super::compaction_and_ddl_rendering::ReconstructionWorkForTest,
    ) {
        let mut rows = Vec::with_capacity(n as usize);
        let mut change_log = Vec::with_capacity(n as usize);
        for i in 0..n {
            rows.push(row(RowId(i), i + 1, Some(n + i + 1), Lsn(2 * i + 2)));
            change_log.push(row_delete("t", RowId(i), Lsn(2 * i + 3)));
        }
        let relational = relational_with(vec![("t", rows)]);
        let graph = graph_with(vec![]);
        let vector = empty_vector();
        super::compaction_and_ddl_rendering::reconstruction_work_reset_for_test();
        let index = commit_index_across_all(&relational, &graph, &vector, &change_log);
        (
            index.len(),
            super::compaction_and_ddl_rendering::reconstruction_work_for_test(),
        )
    }

    fn assert_stages(n: u64, work: super::compaction_and_ddl_rendering::ReconstructionWorkForTest) {
        let size = n as usize;
        assert_eq!(work.row_examinations, size, "each row is examined once");
        assert_eq!(
            work.tombstone_insertions, size,
            "each row contributes one tombstone"
        );
        assert_eq!(
            work.delete_version_probes, size,
            "each delete probes one matching version"
        );
        assert_eq!(
            work.index_insertions,
            2 * size,
            "each row and delete contributes an index insertion"
        );
    }

    const N0: u64 = 10;
    const N: u64 = 60_000;
    let (_, w0) = work(0);
    let (_, wn0) = work(N0);
    let (_, w2n0) = work(2 * N0);
    let (len_n, wn) = work(N);
    assert_eq!(
        len_n,
        2 * N as usize,
        "each row contributes a created_tx entry and each delete a distinct-lsn entry"
    );
    assert_stages(N, wn);
    let (len_2n, w2n) = work(2 * N);
    assert_eq!(
        len_2n,
        4 * N as usize,
        "each row contributes a created_tx entry and each delete a distinct-lsn entry"
    );
    assert_stages(2 * N, w2n);
    assert_eq!(
        w2n.total(),
        2 * wn.total(),
        "doubling the input must double reconstruction work"
    );
    assert!(
        w2n.total() - w0.total() <= 2 * (wn.total() - w0.total()),
        "doubling the input must at most double reconstruction work"
    );
    assert!(
        (wn.total() - wn0.total()) * N0 as usize
            <= (N - N0) as usize * (w2n0.total() - wn0.total()),
        "the marginal work must not exceed the measured small-fixture slope"
    );
}
