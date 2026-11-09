//! The public Rust doors consume the same native executor as SQL.

#![expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
use super::super::*;
use contextdb_core::read_memory::{ReadCredit, ReadMemoryOwner};
use contextdb_vector::{
    MemoryBudgetOwner,
    mem::{BoundedVectorSearchTrace, SearchScores},
};
use std::cell::RefCell;

// This token retains the actual read capability that paid the native trace.
// The existing vector bridge checks its stable funder before adopting it.
struct NativeTraceCredit {
    credit: ReadCredit,
    owner: MemoryBudgetOwner,
}
impl contextdb_vector::PrepaidVectorCopyCharge for NativeTraceCredit {
    fn accountant(&self) -> MemoryBudgetOwner {
        self.owner.clone()
    }
    fn retained_bytes(&self) -> usize {
        self.credit.bytes()
    }
    fn transfer(self: Box<Self>) {
        self.credit.transfer();
    }
}
impl Database {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn native_ivf_owner_search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        mode: VectorSearchMode,
        executing_owner: MemoryBudgetOwner,
        memory: ReadMemoryOwner,
        copies: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Option<(SearchScores, RetainedVectorSearchTrace)>> {
        if !super::CLUSTERED_INDEX_SHIPS || !self.has_native_ivf_source() {
            return Ok(None);
        }
        let credit = RefCell::new(ReadCredit::new(memory.clone()));
        let retain = |bytes| {
            credit.borrow_mut().try_grow_for(
                bytes,
                "vector_search",
                "native_public_results",
                "Reduce the query scope or raise reader memory.",
            )
        };
        let release = |bytes| {
            credit
                .borrow_mut()
                .try_shrink(bytes)
                .expect("native result refunds only its admitted bytes");
        };
        // A supplied filter is the caller's existing finite set. The captured
        // membership producer partitions these exact ids using its read window.
        let ids = if let Some(candidates) = candidates {
            let count = usize::try_from(candidates.len())
                .map_err(|_| Error::Other("native filter length overflow".into()))?;
            retain(
                count
                    .checked_mul(std::mem::size_of::<u64>())
                    .ok_or_else(|| Error::Other("native filter bytes overflow".into()))?,
            )?;
            Some(candidates.iter().collect::<Vec<_>>())
        } else {
            None
        };
        let groups = self.bounded_vector_candidate_groups(
            index,
            ids.as_deref(),
            snapshot,
            || Ok(()),
            || Ok(()),
            retain,
            release,
            copies,
            memory.clone(),
        )?;
        let result = self.native_ivf_bounded_search(
            index,
            query,
            k,
            &groups,
            mode,
            super::policy::QueryBreadth::default(),
            &mut || Ok(()),
            &mut || Ok(()),
            &mut || Ok(()),
            &mut retain.clone(),
            &mut release.clone(),
            memory.clone(),
            None,
        )?;
        drop(groups);
        drop(ids);
        let Some(result) = result else {
            return Ok(None);
        };
        let (raw, trace) = match result {
            crate::executor::BoundedVectorSearchOutcome::Complete((rows, _), trace) => {
                (rows, trace)
            }
            crate::executor::BoundedVectorSearchOutcome::CompleteEmpty => {
                let bytes = index
                    .table
                    .len()
                    .checked_add(index.column.len())
                    .ok_or_else(|| Error::Other("native trace identity overflow".into()))?;
                retain(bytes)?;
                (
                    Vec::new(),
                    BoundedVectorSearchTrace::from_prepaid(
                        VectorSearchDebugTrace {
                            index: index.clone(),
                            final_row_ids: Vec::new(),
                            supplemented_row_count: 0,
                            fallback_reason: None,
                            partition_routes: Vec::new(),
                            budget_receipt: None,
                        },
                        bytes,
                    ),
                )
            }
            _ => {
                return Err(Error::VectorIndexedRouteUnavailable {
                    index: index.clone(),
                });
            }
        };
        let mut rows = SearchScores::new(index, executing_owner.clone())?;
        if mode == VectorSearchMode::Exact {
            rows = rows.for_exact();
        }
        rows.reserve(raw.len())?;
        // A finished native answer: narrow each canonical key once.
        rows.try_extend(raw.iter().map(|&(row_id, key)| (row_id, key as f32)))?;
        drop(raw);
        let trace_credit = credit.borrow_mut().try_split_off(trace.retained_bytes())?;
        let owner = crate::vector_read_memory::budget(memory);
        let trace = RetainedVectorSearchTrace::from_owned(
            trace.into_owned(
                executing_owner.clone(),
                NativeTraceCredit {
                    credit: trace_credit,
                    owner,
                },
            )?,
            executing_owner,
        )?;
        Ok(Some((rows, trace)))
    }
}

impl Database {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn native_ivf_predict_routes(
        &self,
        index: &VectorIndexRef,
        k: usize,
        mode: VectorSearchMode,
        breadth: super::policy::QueryBreadth,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        memory: ReadMemoryOwner,
        copies: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Vec<VectorPartitionSearchTrace>> {
        let _writer = self.writer_read_scope(memory.clone())?;
        let _capture = self.vector_read_scope(snapshot, memory.clone(), copies)?;
        let Some(root) = self.native_ivf_root()? else {
            return Ok(Vec::new());
        };
        let meta = self
            .table_meta(&index.table)
            .ok_or_else(|| Error::TableNotFound(index.table.clone()))?;
        let column = meta
            .columns
            .iter()
            .find(|column| column.name == index.column)
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })?;
        let credit = RefCell::new(ReadCredit::new(memory.clone()));
        let retain = |bytes| {
            credit.borrow_mut().try_grow_for(
                bytes,
                "vector_search",
                "native_explain_scope",
                "Reduce the inspected scope or raise reader memory.",
            )
        };
        let release = |bytes| {
            credit
                .borrow_mut()
                .try_shrink(bytes)
                .expect("native scope refunds its admitted bytes");
        };
        let ids = if let Some(candidates) = candidates {
            let count = usize::try_from(candidates.len())
                .map_err(|_| Error::Other("native filter length overflow".into()))?;
            retain(
                count
                    .checked_mul(std::mem::size_of::<u64>())
                    .ok_or_else(|| Error::Other("native filter bytes overflow".into()))?,
            )?;
            Some(candidates.iter().collect::<Vec<_>>())
        } else {
            None
        };
        let groups = self.bounded_vector_candidate_groups(
            index,
            ids.as_deref(),
            snapshot,
            || Ok(()),
            || Ok(()),
            retain,
            release,
            copies,
            memory.clone(),
        )?;
        let mut routes = Vec::new();
        groups.visit_native_partitions(|tuple, _, _| -> Result<()> {
            let indexed = crate::persistence::vector_ivf::query::predicts_indexed(
                &root,
                memory.clone(),
                index,
                tuple,
                column,
                mode,
                breadth,
                k,
            )?;
            copies.reserve(std::mem::size_of::<VectorPartitionSearchTrace>())?;
            routes.reserve_exact(1);
            routes.push(VectorPartitionSearchTrace {
                route: if indexed {
                    VectorPartitionSearchRoute::Indexed
                } else {
                    VectorPartitionSearchRoute::Exact
                },
                reason: if indexed {
                    "clustered_index"
                } else {
                    "clustered_exact"
                },
                traversal_complete: !indexed,
            });
            Ok(())
        })?;
        Ok(routes)
    }
}
