use crate::composite_store::CompositeStore;
use crate::persistence::RedbPersistence;
use contextdb_core::{Result, RowId};
use contextdb_tx::{WriteSet, WriteSetApplicator};
use std::sync::Arc;

pub struct PersistentCompositeStore {
    inner: CompositeStore,
    persistence: Arc<RedbPersistence>,
}

impl PersistentCompositeStore {
    pub fn new(inner: CompositeStore, persistence: Arc<RedbPersistence>) -> Self {
        Self { inner, persistence }
    }
}

impl WriteSetApplicator for PersistentCompositeStore {
    fn apply(&self, ws: WriteSet) -> Result<()> {
        self.persistence.flush_data(&ws)?;
        self.inner.apply(ws)
    }

    fn new_row_id(&self) -> RowId {
        self.inner.new_row_id()
    }
}
