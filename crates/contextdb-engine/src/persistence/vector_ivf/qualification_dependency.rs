//! Source dependency inspection borrows the selected root and its original reader owner.

#![expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
use super::*;
use contextdb_core::ColumnDef;
impl read::PageSource<'_> {
    pub(in crate::persistence::vector_ivf) fn requalification_pending(
        &self,
        column: &ColumnDef,
        tuple: &VectorPartitionKey,
    ) -> Result<bool> {
        let Some(header) = self.qualification_header()? else {
            return Ok(false);
        };
        if header.policy_revision != column.vector_policy_revision {
            return Ok(true);
        }
        let Some(reference) = &column.vector_engine_policy.calibration_queries else {
            return Ok(header.source_identity != 0);
        };
        let empty = VectorPartitionKey::unpartitioned();
        let source_tuple = if header.source_partitioned {
            tuple
        } else {
            &empty
        };
        let names = reference
            .table
            .len()
            .checked_add(reference.column.len())
            .ok_or_else(corrupt)?;
        // The current reference is already borrowed. Charge the exact cloned
        // names and lookup key before constructing either owning value.
        let route_bytes =
            memory::route_bytes_for_names(&reference.table, &reference.column, source_tuple)?;
        let layout = std::alloc::Layout::new::<(VectorIndexRef, Vec<u8>)>()
            .extend(
                std::alloc::Layout::array::<u8>(
                    names.checked_add(route_bytes).ok_or_else(corrupt)?,
                )
                .map_err(|_| corrupt())?,
            )
            .map_err(|_| corrupt())?
            .0
            .pad_to_align();
        let _invoice = memory::Invoice::reserve(
            self.authority.vector_owner(),
            layout.size(),
            "clustered_calibration_dependency",
        )?;
        let source = VectorIndexRef::new(reference.table.clone(), reference.column.clone());
        let schema = self.authority.admission.run_original_unqualified(|| {
            RedbPersistence::vector_schema_binding_in_read(&self.authority.root.read, &source)
        })?;
        let digest = control::index_digest(&source, schema);
        let identity = u64::from_le_bytes(digest[..8].try_into().map_err(|_| corrupt())?);
        if identity != header.source_identity {
            return Ok(true);
        }
        let route = memory::route_with_suffix_capacity(&source, source_tuple, 0)?;
        let summary = self.authority.source_summary(&route)?;
        Ok(summary.source_identity() != header.source_digest)
    }
}
pub(in crate::persistence::vector_ivf) fn pending(
    root: &crate::persistence::vector_paged::CapturedPagedRoot,
    memory: contextdb_core::read_memory::ReadMemoryOwner,
    index: &VectorIndexRef,
    tuple: &VectorPartitionKey,
    column: &ColumnDef,
    checkpoint: &dyn Fn() -> Result<()>,
) -> Result<bool> {
    let authority = read::NativeAuthority::open_uncached(root, memory)?;
    let Some(source) = read::PageSource::open(&authority, index, tuple, checkpoint, &|_| Ok(()))?
    else {
        return Ok(false);
    };
    source.requalification_pending(column, tuple)
}
