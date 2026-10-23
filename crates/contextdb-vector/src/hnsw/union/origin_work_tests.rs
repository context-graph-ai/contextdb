//! The durable expansion consumes an edge's existing origin without reopening
//! that point merely to rediscover it. This drives the real expansion with a
//! finite admitted reader; graph bytes below are an explicit test provider.
use super::admitted_navigation_body_tests::budget;
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
const DIMENSION: usize = 2048;
const WINDOW: usize = 16 * 1024;
#[derive(Clone)]
struct Fixture {
    point_reads: Arc<AtomicUsize>,
    bridge: bool,
    wrong_identity: bool,
    wrong_body: bool,
}
struct OriginReader {
    fixture: Fixture,
}
impl paged_builder::PagedVectorReader for OriginReader {
    fn read_with_copy_admission(
        &mut self,
        request: paged_builder::PagedVectorReadRequest<'_>,
        admit: &mut dyn FnMut(usize) -> Result<Option<crate::VectorCopyAdmission>>,
    ) -> Result<paged_builder::PagedVectorReadValue> {
        use paged_builder::{PagedVectorReadRequest as Request, PagedVectorReadValue as Value};
        match request {
            Request::Point { id } => {
                let token = admit(WINDOW)?;
                self.fixture.point_reads.fetch_add(1, Ordering::SeqCst);
                let rank = usize::try_from(id.1).unwrap();
                let mut values = vec![0.0; DIMENSION];
                values[rank] = 1.0;
                let next = if rank == 0 || (rank == 1 && self.fixture.bridge) {
                    vec![paged_builder::PagedEdge {
                        point_id: PointId(0, id.1 + 1),
                        origin_id: 8 + rank,
                        distance: 0.0,
                    }]
                } else {
                    Vec::new()
                };
                Ok(Value::Point(crate::ScopedVectorCopy::from_admitted(
                    paged_builder::PagedVectorPoint::F32(paged_builder::PagedPoint {
                        id,
                        origin_id: 7 + rank + usize::from(rank == 1 && self.fixture.wrong_body),
                        values,
                        neighbours: vec![next],
                    }),
                    token,
                )))
            }
            Request::Identity { data_id } => {
                let token = admit(256)?;
                Ok(Value::Identity(crate::ScopedVectorCopy::from_admitted(
                    paged_builder::PagedVectorIdentity {
                        point: PointId(
                            0,
                            if self.fixture.wrong_identity {
                                0
                            } else {
                                (data_id - 7) as i32
                            },
                        ),
                        data_id,
                        graph_origin: data_id,
                        row_id: RowId(data_id as u64),
                        lsn: Lsn(1),
                        created_tx: contextdb_core::TxId(1),
                        deleted_tx: None,
                        exact_key: Vec::new(),
                    },
                    token,
                )))
            }
            _ => panic!("this expansion uses only point and identity reads"),
        }
    }
    fn load_point(&mut self, _: PointId) -> Result<paged_builder::PagedVectorPoint> {
        panic!("must use copy admission")
    }
    fn identity(&mut self, _: usize) -> Result<paged_builder::PagedVectorIdentity> {
        unreachable!()
    }
    fn point_for_row(
        &mut self,
        _: contextdb_core::RowId,
        _: contextdb_core::SnapshotId,
    ) -> Result<Option<PointId>> {
        unreachable!()
    }
    fn exact_rows(
        &mut self,
        _: &[u8],
        _: contextdb_core::SnapshotId,
        _: Option<contextdb_core::RowId>,
        _: usize,
    ) -> Result<Vec<contextdb_core::RowId>> {
        unreachable!()
    }
    fn checkpoint(&mut self) -> Result<()> {
        unreachable!()
    }
    fn before_distance(&mut self) -> Result<()> {
        unreachable!()
    }
    fn before_visit(&mut self) -> Result<()> {
        unreachable!()
    }
    fn visible_count(&mut self, _: contextdb_core::SnapshotId) -> Result<usize> {
        unreachable!()
    }
    fn row_window(
        &mut self,
        _: contextdb_core::SnapshotId,
        _: Option<contextdb_core::RowId>,
        _: usize,
    ) -> Result<Vec<contextdb_core::RowId>> {
        unreachable!()
    }
    fn row_bounds(
        &mut self,
    ) -> Result<Option<(contextdb_core::RowId, contextdb_core::RowId, usize)>> {
        unreachable!()
    }
    fn point_at_or_after(
        &mut self,
        _: contextdb_core::RowId,
    ) -> Result<Option<(contextdb_core::RowId, PointId)>> {
        unreachable!()
    }
    fn exact_graph_row_after(
        &mut self,
        _: &[u8],
        _: Option<contextdb_core::RowId>,
    ) -> Result<Option<contextdb_core::RowId>> {
        unreachable!()
    }
    fn member_after(
        &mut self,
        _: usize,
        _: Option<(contextdb_core::RowId, usize)>,
    ) -> Result<Option<(contextdb_core::RowId, usize, contextdb_core::Lsn)>> {
        unreachable!()
    }
    fn group_after(&mut self, _: u64, _: Option<usize>) -> Result<Option<usize>> {
        unreachable!()
    }
}
struct UnusedPersistence;
impl paged_builder::PagedVectorPersistence for UnusedPersistence {
    fn commit_batch(
        &self,
        _: &paged_builder::PagedVectorProgress,
        _: paged_builder::PagedVectorBatchPlan,
        _: &mut dyn FnMut(
            &mut dyn paged_builder::PagedVectorTransaction,
        ) -> Result<paged_builder::PagedVectorProgress>,
    ) -> Result<paged_builder::PagedVectorCommittedBatch> {
        panic!("read-only fixture")
    }
}
struct UnusedSource;
impl paged_builder::PagedVectorGraphSource for UnusedSource {
    fn reader(&self) -> Result<Box<dyn paged_builder::PagedVectorReader>> {
        panic!("reader is supplied directly to the actual open layer")
    }
    fn retained_bytes(&self) -> usize {
        0
    }
}

#[test]
fn durable_expansion_reads_one_point_body_per_distance_plus_required_adjacency() {
    for (bridge, allow_bridges, admitted, wrong_identity, wrong_body, cached_fault) in [
        (false, false, true, false, false, 0),
        (true, false, true, false, false, 0),
        (false, true, false, false, false, 0),
        (false, false, true, true, false, 0),
        // Fresh body checks: distance, then excluded bridge adjacency.
        (false, false, true, false, true, 0),
        (true, false, true, false, true, 0),
        // Existing identity-cache binding checks, then cached distance and
        // bridge adjacency body-origin checks. These retain normal funding.
        (false, false, true, false, false, 1),
        (false, false, true, false, false, 2),
        (false, false, true, false, false, 3),
        (true, false, true, false, false, 3),
    ] {
        let payer = budget();
        let reads = Arc::new(AtomicUsize::new(0));
        let fixture = Fixture {
            point_reads: reads.clone(),
            bridge,
            wrong_identity,
            wrong_body,
        };
        let mut progress =
            paged_builder::PagedVectorProgress::new(1, 1, contextdb_core::TxId(1), Lsn(1), 3);
        progress.graph.points = 3;
        progress.graph.layer_counts[0] = 3;
        progress.graph.entry_point = Some(PointId(0, 0));
        let tail = Arc::new(
            paged_builder::PagedVectorTail::new(
                crate::store::VectorPartitionRef::new(
                    contextdb_core::VectorIndexRef::new("origin_work", "v"),
                    contextdb_core::VectorPartitionKey::unpartitioned(),
                ),
                progress,
                crate::store::ResolvedVectorPolicy {
                    auto_index_at: 1,
                    hnsw_m: 16,
                    hnsw_ef_construction: 32,
                    hnsw_ef_search: 4,
                    policy_revision: 0,
                    auto_index_at_source: "declared",
                    ef_search_source: "declared",
                },
                DIMENSION,
                VectorQuantization::F32,
                Arc::new(UnusedPersistence),
                Arc::new(UnusedSource),
                payer.clone(),
                Arc::new(AtomicUsize::new(0)),
            )
            .unwrap(),
        );
        // A mandatory native-style window fits alongside bounded metadata.
        // Retaining another complete F32 body while that window is alive
        // cannot fit. This prevents the optional cache hiding an origin read.
        let standing = payer.used.load(Ordering::SeqCst);
        payer
            .limit
            .store(standing + WINDOW + 4096, Ordering::SeqCst);
        let navigation = NavigationSession::new(Some(payer.clone().into()));
        if cached_fault != 0 {
            // Cache-hit negatives need room for both an optional body and the
            // independent mandatory window. Primary exact-fit cases above
            // keep their original limit and body-miss assertion unchanged.
            payer.limit.store(standing + 4 * WINDOW, Ordering::SeqCst);
            if cached_fault <= 2 {
                navigation.lock().remember_identity(
                    0,
                    8,
                    &paged_builder::PagedVectorIdentity {
                        point: PointId(0, if cached_fault == 1 { 0 } else { 1 }),
                        data_id: 8,
                        graph_origin: if cached_fault == 2 { 9 } else { 8 },
                        row_id: RowId(8),
                        lsn: Lsn(1),
                        created_tx: contextdb_core::TxId(1),
                        deleted_tx: None,
                        exact_key: Vec::new(),
                    },
                );
            } else {
                navigation.lock().remember_point(
                    0,
                    PointId(0, 1),
                    &paged_builder::PagedVectorPoint::F32(paged_builder::PagedPoint {
                        id: PointId(0, 1),
                        origin_id: 9,
                        values: vec![0.0; DIMENSION],
                        neighbours: vec![Vec::new()],
                    }),
                );
                assert!(
                    navigation
                        .lock()
                        .point(0, PointId(0, 1))
                        .unwrap()
                        .values
                        .is_some()
                );
            }
        }

        let rows = parking_lot::RwLock::new(HashMap::new());
        let members = parking_lot::RwLock::new(super::super::GraphMembers::default());
        let versions = parking_lot::RwLock::new(HashMap::new());
        let mut open = [OpenLayer {
            graph: LayerGraph::Durable {
                tail: tail.clone(),
                reader: RefCell::new(Box::new(OriginReader { fixture })),
            },
            id_to_row: rows.read(),
            members: members.read(),
            id_lsn: versions.read(),
            covered_lsn: None,
            has_members: false,
            partition: 0,
            source_layer: 0,
            navigation: &navigation,
            first_bit: 0,
            level_ends: vec![3],
            seeds: Vec::new(),
            supplied: None,
        }];
        let mut walk = Walk {
            reached: HashMap::new(),
            visit_bytes: 0,
            frontier: BinaryHeap::new(),
            frontier_capacity: 3,
            frontier_charged: 0,
            kept: BinaryHeap::new(),
            kept_capacity: 1,
            kept_rows: HashMap::new(),
            kept_rows_bytes: 0,
            worst_ties: 0,
            failed_resolved_stop_distance: None,
            stop_distance_comparisons: std::cell::Cell::new(0),
            evaluations: 0,
            visited: 0,
            admissions: 0,
            evaluation_cap: 3,
            rescores: 0,
            rescore_cap: 3,
            visit_cap: 3,
            hops: HopScratch::default(),
            bridges: HopScratch::default(),
            visible_lsn: None,
        };
        // One already-admitted entry supplies a real edge for expansion;
        // entry/seed admission is not included in this source-work segment.
        walk.frontier.push(Nearest(Reached {
            distance: 0.0,
            row_id: RowId(7),
            layer: 0,
            point: PointId(0, 0),
            origin: 7,
        }));
        let mut values = vec![0.0; DIMENSION];
        values[..3].fill(1.0);
        let query = UnionQuery {
            values: &values,
            right_norm: 3f64.sqrt(),
            encoded: None,
        };
        let wanted = if bridge { RowId(9) } else { RowId(8) };
        let result = expand(
            &mut open,
            &mut walk,
            &query,
            1,
            1,
            true,
            false,
            0.02,
            &mut [],
            allow_bridges,
            &mut |_, row| admitted && row == wanted,
            &mut |_, _, _, _| Ok::<_, Error>(None),
            &mut || Ok::<_, Error>(()),
            &mut || Ok::<_, Error>(()),
            &mut |_| Ok::<_, Error>(()),
            &mut |_| {},
        );
        if wrong_identity || wrong_body || cached_fault != 0 {
            assert!(
                result.is_err(),
                "an edge must keep its authenticated identity and body origin on fresh and cached reads: bridge={bridge} wrong_identity={wrong_identity} wrong_body={wrong_body} cached_fault={cached_fault}"
            );
        } else {
            result.unwrap();
            assert_eq!(
                walk.evaluations, 1,
                "excluded navigation is still scored when bridges are permitted"
            );
            let found = walk.kept.iter().map(|row| row.0.row_id).collect::<Vec<_>>();
            assert_eq!(found, if admitted { vec![wanted] } else { Vec::new() });
            assert!(
                navigation
                    .lock()
                    .point(0, PointId(0, if bridge { 2 } else { 1 }))
                    .unwrap()
                    .values
                    .is_none(),
                "this actual finite window must refuse optional complete bodies"
            );
            let required_adjacency_reads = 1 + usize::from(bridge);
            assert_eq!(
                reads.load(Ordering::SeqCst),
                required_adjacency_reads + walk.evaluations,
                "an edge already carries its origin; reading that point solely for origin duplicates its admitted distance read"
            );
        }
        drop(walk);
        drop(open);
        drop(navigation);
        drop(tail);
        assert_eq!(
            payer.used.load(Ordering::SeqCst),
            0,
            "actual source windows and optional metadata refund the same original payer"
        );
    }
}
