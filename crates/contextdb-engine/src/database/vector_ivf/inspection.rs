//! Lazy selected-tuple verification and truthful persisted state.
use super::super::*;
use crate::persistence::vector_ivf::{
    inspection::{self, Inspection},
    retained::Selection,
};
impl Database {
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn native_ivf_inspection(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        memory: contextdb_core::read_memory::ReadMemoryOwner,
        checkpoint: &dyn Fn() -> Result<()>,
    ) -> Result<Option<Inspection>> {
        self.native_ivf_inspect(index, tuple, memory, checkpoint, false, None)
    }
    /// SHOW's inspection: uncached, selects nothing and records no damage.
    pub(crate) fn native_ivf_pure_inspection(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        memory: contextdb_core::read_memory::ReadMemoryOwner,
        checkpoint: &dyn Fn() -> Result<()>,
        live_rows: usize,
    ) -> Result<Option<Inspection>> {
        self.native_ivf_inspect(
            index,
            tuple,
            memory,
            checkpoint,
            true,
            u64::try_from(live_rows).ok(),
        )
    }
    fn native_ivf_inspect(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        memory: contextdb_core::read_memory::ReadMemoryOwner,
        checkpoint: &dyn Fn() -> Result<()>,
        pure: bool,
        live: Option<u64>,
    ) -> Result<Option<Inspection>> {
        if !super::CLUSTERED_INDEX_SHIPS {
            return Ok(None);
        }
        let Some(root) = self.native_ivf_root()? else {
            return Ok(None);
        };
        let metadata = self
            .table_meta(&index.table)
            .ok_or_else(|| Error::TableNotFound(index.table.clone()))?;
        let column = metadata
            .columns
            .iter()
            .find(|column| column.name == index.column)
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })?;
        let max_runs = column.vector_engine_policy.max_delta_runs.unwrap_or(2);
        let owner = || self.finalization.accountant.vector_budget();
        let boundary = |run: &mut dyn FnMut() -> Result<()>| {
            self.with_native_retained_selection(&root, checkpoint, run)
        };
        let historical = read_only_selection()
            || self
                .vector_membership_owner()
                .is_some_and(|owner| owner.historical_operation());
        let selection = self
            .persistence
            .as_ref()
            .filter(|_| !historical && !pure)
            .map(|persistence| Selection {
                cache: &persistence.native_ivf_cache,
                owner: &owner,
                boundary: &boundary,
            });
        let Some(mut result) = inspection::inspect(
            &root, memory, index, tuple, column, max_runs, selection, live, checkpoint,
        )?
        else {
            return Ok(None);
        };
        if let Some(catalog) = result.catalog.filter(|_| !pure) {
            result.catalog = Some(self.record_selected_native_ivf_damage(
                &root, index, tuple, max_runs, catalog, checkpoint,
            )?);
        }
        Ok(Some(result))
    }
}

thread_local! {static READ_ONLY_SELECTION:std::cell::Cell<bool>=const{std::cell::Cell::new(false)};}
pub(crate) struct ReadOnlySelection(bool);
impl ReadOnlySelection {
    pub(crate) fn enter() -> Self {
        Self(READ_ONLY_SELECTION.with(|slot| slot.replace(true)))
    }
}
impl Drop for ReadOnlySelection {
    fn drop(&mut self) {
        READ_ONLY_SELECTION.with(|slot| slot.set(self.0));
    }
}
pub(crate) fn read_only_selection() -> bool {
    READ_ONLY_SELECTION.with(std::cell::Cell::get)
}

impl Database {
    pub(crate) fn record_selected_native_ivf_damage(
        &self,
        root: &crate::persistence::vector_paged::CapturedPagedRoot,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        max_runs: u64,
        mut selected: contextdb_vector::ivf::CatalogState,
        checkpoint: &dyn Fn() -> Result<()>,
    ) -> Result<contextdb_vector::ivf::CatalogState> {
        if selected.unserviceable.is_none()
            || selected.verdict_recorded
            || read_only_selection()
            || self
                .vector_membership_owner()
                .is_some_and(|owner| owner.historical_operation())
        {
            return Ok(selected);
        }
        let Some(persistence) = &self.persistence else {
            return Ok(selected);
        };
        let mut committed = None;
        self.with_native_retained_selection(root, checkpoint, &mut || {
            committed = Some(self.publish_direct_metadata(|| {
                persistence.record_native_ivf_inspection(index, tuple, max_runs, checkpoint)
            })?);
            Ok(())
        })?;
        if let Some(Some(stored)) = committed
            && stored.unserviceable == selected.unserviceable
        {
            // This flag describes our completed metadata write. Counts and
            // membership remain those of the original query capture.
            selected.verdict_recorded = stored.verdict_recorded;
        }
        Ok(selected)
    }
}
