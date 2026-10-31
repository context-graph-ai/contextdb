//! Native candidates enter the same bounded executor and finalist row path.
use super::super::*;
use contextdb_vector::ivf::{Hit, Work, storage::DistanceKind};
use std::cell::RefCell;
impl Database {
    pub(crate) fn has_native_ivf_source(&self) -> bool {
        self.persistence.is_some() || self.retained_graph_source.is_some()
    }

    pub(crate) fn native_ivf_root(
        &self,
    ) -> Result<Option<crate::persistence::vector_paged::CapturedPagedRoot>> {
        if SNAPSHOT_OVERRIDE.with(|cell| cell.borrow().is_some())
            || self
                .vector_membership_owner()
                .is_some_and(|owner| owner.historical_operation())
        {
            let requested = self.snapshot_for_read();
            let pinned = self
                .snapshot_registry
                .state
                .lock()
                .active
                .values()
                .filter(|registered| {
                    registered.snapshot == requested && registered.holder_still_entitled()
                })
                .filter_map(|registered| {
                    registered
                        .native_root
                        .as_ref()
                        .and_then(|root| root.upgrade())
                })
                .find(|root| root.snapshot == requested);
            if let Some(root) = pinned {
                return Ok(Some(
                    crate::persistence::vector_paged::CapturedPagedRoot::from_writer(root)?,
                ));
            }
        }
        if let Some(persistence) = &self.persistence {
            return Ok(Some((persistence.paged_root_capture()?)()?));
        }
        if let Some(source) = &self.retained_graph_source {
            let read = Arc::new(source.begin_read()?);
            return Ok(Some(
                crate::persistence::vector_paged::CapturedPagedRoot::from_image(
                    read,
                    source.clone(),
                )?,
            ));
        }
        Ok(None)
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn native_ivf_bounded_search<E: From<Error>>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        groups: &contextdb_vector::mem::BoundedVectorCandidateGroups,
        mode: VectorSearchMode,
        query_breadth: crate::database::vector_ivf::policy::QueryBreadth,
        before_source: &mut dyn FnMut() -> std::result::Result<(), E>,
        before_exact: &mut dyn FnMut() -> std::result::Result<(), E>,
        before_distance: &mut dyn FnMut() -> std::result::Result<(), E>,
        before_retain: &mut dyn FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut dyn FnMut(usize),
        memory: contextdb_core::read_memory::ReadMemoryOwner,
        mut row_check: Option<contextdb_vector::mem::RowCheck<'_, E>>,
    ) -> std::result::Result<Option<crate::executor::BoundedVectorSearchOutcome>, E> {
        #[cfg(feature = "test-seams")]
        let _native_activity = self.vector_store.native_index_activity_scope_for_test();
        let Some(root) = self.native_ivf_root().map_err(E::from)? else {
            return Ok(None);
        };
        let (_, native_snapshot, _) = root.authenticate().map_err(E::from)?;
        if native_snapshot != groups.native_snapshot() {
            return if mode == VectorSearchMode::Indexed {
                Err(E::from(Error::VectorIndexedRouteUnavailable {
                    index: index.clone(),
                }))
            } else {
                Ok(None)
            };
        }
        let meta = self
            .table_meta(&index.table)
            .ok_or_else(|| E::from(Error::TableNotFound(index.table.clone())))?;
        let column = meta
            .columns
            .iter()
            .find(|c| c.name == index.column)
            .ok_or_else(|| {
                E::from(Error::UnknownVectorIndex {
                    index: index.clone(),
                })
            })?;
        if k == 0 || groups.aggregate_allowed_count() == 0 {
            return Ok(Some(
                crate::executor::BoundedVectorSearchOutcome::CompleteEmpty,
            ));
        }
        // Both output arrays are bounded by the caller's LIMIT/candidate
        // allowance, independent of source population and selected tuples.
        let scratch = k
            .checked_mul(std::mem::size_of::<Hit>())
            .ok_or_else(|| E::from(Error::Other("native candidate workspace overflow".into())))?;
        let _scratch = contextdb_core::read_memory::ReadCredit::try_new(
            memory.clone(),
            scratch,
            "vector_search",
            "native_partition_hits",
            "Reduce LIMIT or raise reader memory.",
        )
        .map_err(E::from)?;
        #[cfg(feature = "test-seams")]
        let _scratch = crate::vector_ivf_observations::observe_existing_read_credit(
            _scratch,
            "native_partition_hits",
        )
        .map_err(E::from)?;
        let mut hits = vec![Hit::default(); k];
        // A ranked indexed read keeps the whole refined candidate pool of the
        // route selected for `k`; the pool's extra score and merge space is
        // admitted when a partition reports it.
        let policy_limit = contextdb_vector::mem::search_policy_limit();
        let rank_pool = mode != VectorSearchMode::Exact && policy_limit.is_some();
        let mut capacity = k;
        let mut pool_growth = contextdb_core::read_memory::ReadCredit::new(memory.clone());
        let score_bytes = k
            .checked_mul(std::mem::size_of::<(RowId, f32)>())
            .ok_or_else(|| E::from(Error::Other("native score workspace overflow".into())))?;
        let merge_bytes = k
            .checked_mul(std::mem::size_of::<(RowId, f64)>())
            .ok_or_else(|| E::from(Error::Other("native merge workspace overflow".into())))?;
        let _merge = contextdb_core::read_memory::ReadCredit::try_new(
            memory.clone(),
            merge_bytes,
            "vector_search",
            "native_canonical_merge",
            "Reduce LIMIT or raise reader memory.",
        )
        .map_err(E::from)?;
        #[cfg(feature = "test-seams")]
        let _merge = crate::vector_ivf_observations::observe_existing_read_credit(
            _merge,
            "native_canonical_merge",
        )
        .map_err(E::from)?;
        before_retain(score_bytes)?;
        let mut rows = Vec::with_capacity(k);
        let mut retained = score_bytes;
        let mut trace_routes = Vec::new();
        let mut trace_bytes = 0usize;
        let error = RefCell::new(None::<E>);
        let source = RefCell::new(before_source);
        let exact = RefCell::new(before_exact);
        let distance = RefCell::new(before_distance);
        let checkpoint = || match (source.borrow_mut())() {
            Ok(()) => Ok(()),
            Err(e) => {
                *error.borrow_mut() = Some(e);
                Err(Error::ReadCancelled)
            }
        };
        let charge = |kind: DistanceKind| {
            let result = if mode != VectorSearchMode::Indexed
                && matches!(kind, DistanceKind::Canonical | DistanceKind::Buffer)
            {
                (exact.borrow_mut())()
            } else {
                (distance.borrow_mut())()
            };
            match result {
                Ok(()) => Ok(()),
                Err(e) => {
                    *error.borrow_mut() = Some(e);
                    Err(Error::ReadCancelled)
                }
            }
        };
        let original_maintenance = || self.finalization.accountant.vector_budget();
        let historical = super::read_only_selection()
            || self
                .vector_membership_owner()
                .is_some_and(|owner| owner.historical_operation());
        let retained_boundary = |run: &mut dyn FnMut() -> Result<()>| {
            self.with_native_retained_selection(&root, &checkpoint, run)
        };
        let maintenance = self.persistence.as_ref().filter(|_| !historical).map(|p| {
            crate::persistence::vector_ivf::retained::Selection {
                cache: &p.native_ivf_cache,
                owner: &original_maintenance,
                boundary: &retained_boundary,
            }
        });
        let mut unavailable = false;
        let mut total = Work::default();
        let result=groups.visit_native_partitions(|tuple,allowed,_count| {
            if unavailable {return Ok(());}
            let mut eligible=|candidate:contextdb_vector::ivf::CandidateIdentity<'_>|->std::result::Result<bool,E>{
                if allowed.is_some_and(|ids|ids.binary_search(&candidate.row.row).is_err()) {return Ok(false);}
                if let Some(check)=&mut row_check {return (check.check)(RowId(candidate.row.row));}
                Ok(true)
            };
            let record_damage=|catalog|self.record_selected_native_ivf_damage(&root,index,tuple,column.vector_engine_policy.max_delta_runs.unwrap_or(2),catalog,&checkpoint).map(|_|());
            let pooled = std::cell::Cell::new(false);
            let merge_hit = |rows: &mut Vec<(RowId, f64)>, capacity: usize, hit: &Hit| {
                let score = (RowId(hit.row), hit.distance);
                let at = rows
                    .binary_search_by(|(r, d): &(RowId, f64)| {
                        d.total_cmp(&score.1).then_with(|| r.cmp(&score.0))
                    })
                    .unwrap_or_else(|at| at);
                if at >= capacity {
                    return;
                }
                if rows.len() == capacity {
                    rows.pop();
                }
                rows.insert(at, score);
            };
            let mut on_pool = |pool: &[Hit]| -> std::result::Result<(), E> {
                if pool.len() > capacity {
                    let extra = pool.len() - capacity;
                    let score_extra = extra
                        .checked_mul(std::mem::size_of::<(RowId, f32)>())
                        .ok_or_else(|| E::from(Error::Other("native pool workspace overflow".into())))?;
                    let merge_extra = extra
                        .checked_mul(std::mem::size_of::<(RowId, f64)>())
                        .ok_or_else(|| E::from(Error::Other("native pool workspace overflow".into())))?;
                    before_retain(score_extra)?;
                    retained += score_extra;
                    pool_growth
                        .try_grow_for(
                            merge_extra,
                            "vector_search",
                            "native_canonical_merge",
                            "Reduce LIMIT or raise reader memory.",
                        )
                        .map_err(E::from)?;
                    rows.reserve_exact(extra);
                    capacity = pool.len();
                }
                for hit in pool {
                    merge_hit(&mut rows, capacity, hit);
                }
                pooled.set(true);
                Ok(())
            };
            let found=crate::persistence::vector_ivf::query::search(&root,memory.clone(),index,tuple,column,maintenance,Some(&record_damage),query,mode,query_breadth,&checkpoint,&charge,&mut eligible,&mut hits,rank_pool.then_some(&mut on_pool as &mut dyn FnMut(&[Hit]) -> std::result::Result<(), E>));
            if let Some(error)=error.borrow_mut().take() {return Err(error);}
            let Some((count,work))=found? else {unavailable=true;return Ok(());};
            #[cfg(feature="test-seams")]
            crate::vector_ivf_observations::observe_query(index,tuple,work.manifest.generation,work.indexed,work.served_depth,work.objective_met,&work.work);
            add_work(&mut total,work.work);
            let bytes=std::mem::size_of::<VectorPartitionSearchTrace>();before_retain(bytes)?;trace_bytes+=bytes;
            trace_routes.reserve_exact(1);trace_routes.push(VectorPartitionSearchTrace {route:if work.indexed {VectorPartitionSearchRoute::Indexed} else {VectorPartitionSearchRoute::Exact},reason:if work.indexed {"clustered_index"} else {"clustered_exact"}});
            if !pooled.get() {
                for hit in &hits[..count] {
                    merge_hit(&mut rows, capacity, hit);
                }
            }
            Ok(())
        });
        if let Err(error) = result {
            drop(rows);
            drop(trace_routes);
            release_retained(retained + trace_bytes);
            return Err(error);
        }
        if unavailable {
            drop(rows);
            drop(trace_routes);
            release_retained(retained + trace_bytes);
            return if mode == VectorSearchMode::Indexed {
                Err(E::from(Error::VectorIndexedRouteUnavailable {
                    index: index.clone(),
                }))
            } else {
                Ok(None)
            };
        }
        let identity = index
            .table
            .len()
            .checked_add(index.column.len())
            .ok_or_else(|| E::from(Error::Other("native trace identity overflow".into())))?;
        if let Err(error) = before_retain(identity) {
            drop(rows);
            drop(trace_routes);
            release_retained(retained + trace_bytes);
            return Err(error);
        }
        trace_bytes += identity;
        let trace = VectorSearchDebugTrace {
            index: index.clone(),
            final_row_ids: Vec::new(),
            supplemented_row_count: 0,
            fallback_reason: None,
            partition_routes: trace_routes,
            budget_receipt: None,
        };
        // Keep canonical precision through every tuple merge and LIMIT cut.
        // The public score representation is converted only after ranking.
        let mut scores = Vec::with_capacity(capacity);
        for (row, distance) in &rows {
            scores.push((*row, (1.0 - distance) as f32));
        }
        drop(rows);
        let _ = (&mut retained, total);
        Ok(Some(crate::executor::BoundedVectorSearchOutcome::Complete(
            (scores, retained),
            contextdb_vector::mem::BoundedVectorSearchTrace::from_prepaid(trace, trace_bytes),
        )))
    }
}
fn add_work(total: &mut Work, work: Work) {
    total.centroid_evaluations += work.centroid_evaluations;
    total.quantized_evaluations += work.quantized_evaluations;
    total.full_precision_evaluations += work.full_precision_evaluations;
    total.reranked_candidates += work.reranked_candidates;
    total.buffer_evaluations += work.buffer_evaluations;
    total.exact_score_count += work.exact_score_count;
}

impl Database {
    pub(crate) fn with_native_retained_selection(
        &self,
        captured: &crate::persistence::vector_paged::CapturedPagedRoot,
        checkpoint: &dyn Fn() -> Result<()>,
        run: &mut dyn FnMut() -> Result<()>,
    ) -> Result<bool> {
        let Some(persistence) = &self.persistence else {
            return Ok(false);
        };
        let mut attempt = || {
            // This is a cache-publication check only. The query continues
            // reading its original registered capture, never this newer root.
            // A capture superseded by a commit uses ordinary reader objects.
            let latest = (persistence.paged_root_capture()?)()?;
            if latest.read.storage_snapshot_identity() != captured.read.storage_snapshot_identity()
            {
                return Ok(false);
            }
            run()?;
            Ok(true)
        };
        if self.tx_mgr.commit_boundary_held_by_current_thread() {
            return attempt();
        }
        loop {
            checkpoint()?;
            if let Some(result) = self
                .tx_mgr
                .try_with_commit_lock_for(std::time::Duration::ZERO, || attempt())
            {
                return result;
            }
            std::thread::yield_now();
        }
    }
}
