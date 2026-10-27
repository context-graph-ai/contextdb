//! Authenticated clustered pages under the engine's native transaction owner.
//! Configuration authenticates fixed scope controls, which authenticate each
//! separately addressable generation/kind family at the captured source root.
use super::{RedbPersistence, VectorSchemaBinding};
use contextdb_core::{Error, Result, VectorIndexRef, VectorPartitionKey};
use contextdb_vector::ivf::storage::PAGE_BYTES;
use redb::{TableDefinition, TableHandle};
pub(super) const SCOPES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ivf_scopes");

mod control;
mod frontier;
mod header;
mod io;
mod memory;
mod read;
mod seal;
mod semantic;
mod write;

fn corrupt() -> Error {
    Error::StoreCorrupted {
        path: "clustered vector native pages".into(),
        reason: "a captured vector scope, role, semantic count or native root is invalid".into(),
    }
}
