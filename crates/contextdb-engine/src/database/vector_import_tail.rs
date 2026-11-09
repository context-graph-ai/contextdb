//! Import completion: whether the published native index covers every
//! committed passage vector.
use super::*;

impl Database {
    /// Import completion seam: `true` when every committed vector is covered
    /// by the published native index, including its committed write buffer.
    /// It builds nothing and chases no later commit; the existing maintenance
    /// cycle advances the index, and the caller alternates the two.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn persist_import_vector_tails(&self) -> Result<bool> {
        let read_memory = self.declared_reader_memory()?;
        let mut operation = self.assert_open_operation();
        operation.release_schema_publication_for_online_maintenance();
        let _guard = MaintenanceReceiptGuard::lock(&self.pruning_guard);
        self.maintenance_context()?
            .persist_import_vector_tails(read_memory)
    }
}

impl MaintenanceContext {
    /// Every route is judged on one captured native root, funded by this
    /// writer's declared memory limit.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn persist_import_vector_tails(
        &self,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<bool> {
        let _decode = crate::read_image_memory::ImageMemory::enter_writer_operation(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
        )?;
        let mut copy_owner = contextdb_vector::VectorCopyAdmission::new(
            self.finalization.accountant.vector_budget()?,
        );
        if !super::vector_ivf::CLUSTERED_INDEX_SHIPS {
            // No index covers a populated partition.
            for index in self.vector.index_layout_infos(&mut copy_owner)? {
                let partitions = self.vector.partition_infos(&index.index, &mut copy_owner)?;
                if partitions.iter().any(|partition| partition.live_rows != 0) {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        let root = (self.persistence.paged_root_capture()?)()?;
        let checkpoint = || self.build_checkpoint();
        for partition in vector_partition_routes(&self.vector, &mut copy_owner)? {
            if !crate::persistence::vector_ivf::inspection::covered(
                &root,
                read_memory.clone(),
                &partition.index,
                &partition.partition_key,
                &checkpoint,
            )? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
