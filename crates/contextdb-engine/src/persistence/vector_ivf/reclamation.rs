//! Current-tree cleanup depends on the generation each retained root selects.
use super::*;
use contextdb_vector::ivf::storage::ReadPages;

pub(crate) fn selected_generation(
    root: &super::super::vector_paged::CapturedPagedRoot,
    memory: contextdb_core::read_memory::ReadMemoryOwner,
    index: &VectorIndexRef,
    tuple: &VectorPartitionKey,
) -> Result<Option<(u64, contextdb_core::SnapshotId)>> {
    let authority = read::NativeAuthority::open_uncached(root, memory)?;
    let Some(source) = read::PageSource::open(&authority, index, tuple, &|| Ok(()), &|_| Ok(()))?
    else {
        return Ok(None);
    };
    let Some(manifest) = source.manifest(0)? else {
        return Ok(None);
    };
    let basis = source
        .record
        .borrow()
        .family(manifest.generation)
        .ok_or_else(corrupt)?
        .source_snapshot;
    Ok(Some((
        manifest.generation,
        contextdb_core::SnapshotId(basis),
    )))
}
impl RedbPersistence {
    pub(crate) fn native_ivf_cleanup_basis(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<Option<(u64, contextdb_core::SnapshotId)>> {
        let root = (self.paged_root_capture()?)()?;
        selected_generation(&root, memory, index, tuple)
    }
}
