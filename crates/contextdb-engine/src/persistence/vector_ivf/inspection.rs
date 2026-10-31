//! Inspection uses the same authenticated native source and catalog verifier.
use super::*;
use contextdb_vector::ivf::{
    CatalogState,
    storage::{Manifest, ReadPages},
};
/// What a partition's matching qualification evidence says about its declared
/// target accuracy. Absent without a declared objective.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Objective {
    Unmeasured,
    Met,
    Unmet,
}
impl Objective {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unmeasured => "unmeasured",
            Self::Met => "met",
            Self::Unmet => "unmet",
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct Inspection {
    pub manifest: Option<Manifest>,
    pub catalog: Option<CatalogState>,
    pub objective: Option<Objective>,
    /// Bytes of the selected generation's persisted index tables; absent
    /// while nothing is published.
    pub durable_index_bytes: Option<u64>,
    /// The source snapshot of the selected published generation; absent
    /// while nothing is published.
    pub base_tx: Option<u64>,
    /// Why an idle published index is due for a replacement, by the same
    /// declared-policy decision maintenance makes; absent without live rows
    /// to judge or when nothing is due.
    pub pending_trigger: Option<&'static str>,
}
impl Inspection {
    /// A published index that can serve this partition.
    pub fn available(self) -> bool {
        self.recovery_action() != "rebuild"
            && self
                .manifest
                .is_some_and(|manifest| manifest.active_lists != 0)
    }
    /// The published codebook's identity, when an index is published.
    pub fn codebook(self) -> Option<[u8; 32]> {
        self.manifest
            .filter(|manifest| manifest.active_lists != 0)
            .map(|manifest| manifest.codebook)
    }
    pub fn recovery_action(self) -> &'static str {
        if self
            .catalog
            .is_some_and(|catalog| catalog.unserviceable.is_some())
        {
            "rebuild"
        } else {
            "none"
        }
    }
    pub fn state(self) -> Option<&'static str> {
        self.catalog.filter(|catalog|catalog.unserviceable.is_some()).map(|catalog|
            if catalog.verdict_recorded {"recorded"} else {"damage seen by a reader; not yet recorded (a writer records it when it next touches this partition)"})
    }
}
pub(crate) fn inspect(
    root: &super::super::vector_paged::CapturedPagedRoot,
    memory: contextdb_core::read_memory::ReadMemoryOwner,
    index: &VectorIndexRef,
    tuple: &VectorPartitionKey,
    column: &contextdb_core::ColumnDef,
    max_delta_runs: u64,
    selection: Option<retained::Selection<'_>>,
    live: Option<u64>,
    checkpoint: &dyn Fn() -> Result<()>,
) -> Result<Option<Inspection>> {
    let authority = read::NativeAuthority::open_uncached(root, memory)?;
    let Some(mut source) =
        read::PageSource::open(&authority, index, tuple, checkpoint, &|_| Ok(()))?
    else {
        return Ok(None);
    };
    if let Some(selection) = selection {
        source.activate_retained(tuple, selection)?;
    }
    let manifest = source.manifest(0)?;
    let catalog = contextdb_vector::ivf::inspect_catalog(&source, 0, max_delta_runs)?;
    let objective = objective(&source, column, tuple, manifest)?;
    let durable_index_bytes = manifest
        .filter(|manifest| manifest.active_lists != 0)
        .map(|manifest| source.durable_index_bytes(manifest.generation))
        .transpose()?;
    let base_tx = manifest
        .filter(|manifest| manifest.active_lists != 0)
        .map(|manifest| source.base_tx(manifest.generation))
        .transpose()?;
    let pending_trigger = match (live, manifest.filter(|manifest| manifest.active_lists != 0)) {
        (Some(live), Some(serving)) => source.replacement_trigger(
            column,
            serving,
            live,
            catalog.is_some_and(|catalog| catalog.unserviceable.is_some()),
        ),
        _ => None,
    };
    Ok(Some(Inspection {
        manifest,
        catalog,
        objective,
        durable_index_bytes,
        base_tx,
        pending_trigger,
    }))
}
/// The declared objective judged only by evidence for this generation and
/// policy: stale or missing evidence is unmeasured, any measured depth that
/// missed the target is unmet.
fn objective(
    source: &read::PageSource<'_>,
    column: &contextdb_core::ColumnDef,
    tuple: &VectorPartitionKey,
    manifest: Option<Manifest>,
) -> Result<Option<Objective>> {
    let Some(target) = column.search_target_accuracy_hundredths else {
        return Ok(None);
    };
    if manifest.is_none() || source.requalification_pending(column, tuple)? {
        return Ok(Some(Objective::Unmeasured));
    }
    let Some(header) = source.qualification_header()? else {
        return Ok(Some(Objective::Unmeasured));
    };
    if header.target_hundredths != target {
        return Ok(Some(Objective::Unmeasured));
    }
    for ordinal in 0..header.depths {
        if !source.qualification_depth(ordinal)?.qualified {
            return Ok(Some(Objective::Unmet));
        }
    }
    Ok(Some(Objective::Met))
}
impl RedbPersistence {
    /// A metadata-only native write under the writer's existing owner.
    pub(crate) fn record_native_ivf_inspection(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        max_delta_runs: u64,
        checkpoint: &dyn Fn() -> Result<()>,
    ) -> Result<Option<CatalogState>> {
        let _decode = self.writer_decode_scope()?;
        let projection = self.require_decode_original()?;
        let original = projection.preparation_original()?;
        let owner = projection.vector_budget()?;
        let admission =
            super::super::native_storage_admission::StorageAdmissionScope::writer_original(
                &original,
            )?;
        admission.run_original_unqualified(|| {
            checkpoint()?;
            let root = (self.paged_root_capture()?)()?;
            let (_, snapshot, frontier) = root.authenticate()?;
            self.with_db(|database| {
                let transaction = Self::begin_history_write(database)?;
                let mut session = write::WriteSession::borrow(
                    &transaction,
                    &admission,
                    owner,
                    snapshot,
                    frontier,
                    frontier,
                )?;
                let catalog =
                    session.with_scope(index, tuple, checkpoint, &|_| Ok(()), |scope| {
                        scope.prepare_existing_roots()?;
                        contextdb_vector::ivf::inspect_and_record_catalog(scope, 0, max_delta_runs)
                    })?;
                session.bind_final()?;
                drop(session);
                Self::write_writer_recovery_bounds_in_write(&transaction, None)?;
                checkpoint()?;
                transaction.commit().map_err(Self::storage_error)?;
                Ok(catalog)
            })
        })
    }
}

/// Whether the captured native root holds a usable published index that
/// covers every committed vector of this tuple, including its committed write
/// buffer. A tuple with no committed vector is covered.
pub(crate) fn covered(
    root: &super::super::vector_paged::CapturedPagedRoot,
    memory: contextdb_core::read_memory::ReadMemoryOwner,
    index: &VectorIndexRef,
    tuple: &VectorPartitionKey,
    checkpoint: &dyn Fn() -> Result<()>,
) -> Result<bool> {
    let authority = read::NativeAuthority::open_uncached(root, memory)?;
    let Some(source) = read::PageSource::open(&authority, index, tuple, checkpoint, &|_| Ok(()))?
    else {
        let route = RedbPersistence::vector_partition_prefix(index, tuple);
        return Ok(authority.source_summary(&route)?.live_entries == 0);
    };
    if source.source_summary.live_entries == 0 {
        return Ok(true);
    }
    Ok(source.canonical_ready() && source.manifest(0)?.is_some_and(|m| m.active_lists != 0))
}
