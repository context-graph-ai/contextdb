//! Authenticated clustered pages under the engine's native transaction owner.
//! Configuration authenticates fixed scope controls, which authenticate each
//! separately addressable generation/kind family at the captured source root.
use super::{RedbPersistence, VectorSchemaBinding};
use contextdb_core::{Error, Result, VectorIndexRef, VectorPartitionKey};
use contextdb_vector::ivf::storage::PAGE_BYTES;
use redb::{TableDefinition, TableHandle};
pub(super) const SCOPES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ivf_scopes");

mod cache;
mod calibration;
mod control;
mod descriptor;
pub(crate) mod inspection;
mod maintenance;
mod mutations;
pub(crate) mod qualification;
pub(crate) mod query;
pub(crate) mod retained;
pub(crate) use cache::NativeIvfCache;
mod frontier;
mod header;
mod io;
mod memory;
mod read;
mod seal;
mod semantic;
pub(super) mod write;

fn corrupt() -> Error {
    Error::StoreCorrupted {
        path: "clustered vector native pages".into(),
        reason: "a captured vector scope, role, semantic count or native root is invalid".into(),
    }
}

#[cfg(feature = "test-seams")]
mod test_driver;
#[cfg(feature = "test-seams")]
pub use write::WriteScope as NativeIvfWriteForTest;

#[cfg(feature = "test-seams")]
mod observe;

#[cfg(feature = "test-seams")]
pub use test_driver::NativeIvfReadForTest;
#[cfg(feature = "test-seams")]
pub(crate) use test_driver::with_captured_read;

/// Fixed native control, seal and caller-owned source/output space which must
/// coexist with the producer workspace. This is measured from real holders.
pub(crate) fn writer_control_bytes(
    index: &VectorIndexRef,
    tuple: &VectorPartitionKey,
) -> Result<usize> {
    write::control_bytes(index, tuple)
}

#[cfg(feature = "test-seams")]
pub(crate) fn writer_operation_control_bytes() -> usize {
    std::mem::size_of::<write::WriteOperation<'_, '_, '_>>()
}

#[cfg(feature = "test-seams")]
impl RedbPersistence {
    pub(crate) fn native_ivf_source_summary(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<(u64, [u8; 32])> {
        let root = (self.paged_root_capture()?)()?;
        let authority = read::NativeAuthority::open(&root, memory)?;
        let construction =
            memory::ScopeConstruction::reserve::<()>(authority.vector_owner(), index, tuple)?;
        let route = construction.route(index, tuple)?;
        let summary = authority.source_summary(&route)?;
        Ok((summary.live_entries, summary.source_identity()))
    }
}

#[cfg(feature = "test-seams")]
impl RedbPersistence {
    pub(super) fn native_ivf_test_column(
        &self,
        column: &contextdb_core::ColumnDef,
    ) -> Result<contextdb_core::ColumnDef> {
        let options = self
            .load_config_value::<crate::vector_ivf_observations::NativeIvfIndexTestOptions>(
                "test.native_ivf_options",
            )?
            .unwrap_or_default();
        let mut column = column.clone();
        if let Some(cap) = options.write_buffer_cap_bytes {
            column.vector_engine_policy.write_buffer_bytes = Some(cap);
        }
        if let Some(batch) = options.build_batch_rows {
            column.vector_engine_policy.ingest_batch_keys = Some(batch);
            column.vector_engine_policy.build_batch_entries = Some(batch);
            column.vector_engine_policy.remap_batch_entries = Some(batch);
        }
        Ok(column)
    }
}

/// The same partition/source basis feeds maintenance, row preparation and
/// passive resolution. An undeclared seed never depends on the caller door.
pub(crate) fn training_identity(tuple: &VectorPartitionKey, source: [u8; 32]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(&control::tuple_digest(tuple));
    hash.update(&source);
    *hash.finalize().as_bytes()
}

pub(crate) use memory::native_receipt_reserve;

#[cfg(feature = "test-seams")]
pub use super::writer_file::NativeIvfPhysicalDiskOwnerForTest;
#[cfg(feature = "test-seams")]
impl RedbPersistence {
    pub(crate) fn native_ivf_physical_disk_owner_for_test(
        &self,
    ) -> Result<NativeIvfPhysicalDiskOwnerForTest> {
        self.writer_file
            .read()
            .map_err(|_| Error::Other("physical writer owner lock was poisoned".into()))?
            .as_ref()
            .cloned()
            .map(NativeIvfPhysicalDiskOwnerForTest::new)
            .ok_or_else(|| {
                Error::Other("native disk observation requires a live physical writer".into())
            })
    }
}
