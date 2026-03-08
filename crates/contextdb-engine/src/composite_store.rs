use contextdb_core::{Result, RowId};
use contextdb_graph::GraphStore;
use contextdb_relational::RelationalStore;
use contextdb_tx::{WriteSet, WriteSetApplicator};
use contextdb_vector::VectorStore;
use std::sync::Arc;

pub struct CompositeStore {
    pub relational: Arc<RelationalStore>,
    pub graph: Arc<GraphStore>,
    pub vector: Arc<VectorStore>,
}

impl CompositeStore {
    pub fn new(
        relational: Arc<RelationalStore>,
        graph: Arc<GraphStore>,
        vector: Arc<VectorStore>,
    ) -> Self {
        Self {
            relational,
            graph,
            vector,
        }
    }
}

impl WriteSetApplicator for CompositeStore {
    fn apply(&self, ws: WriteSet) -> Result<()> {
        self.relational.apply_inserts(ws.relational_inserts);
        self.relational.apply_deletes(ws.relational_deletes);
        self.graph.apply_inserts(ws.adj_inserts);
        self.graph.apply_deletes(ws.adj_deletes);
        self.vector.apply_inserts(ws.vector_inserts);
        self.vector.apply_deletes(ws.vector_deletes);
        Ok(())
    }

    fn new_row_id(&self) -> RowId {
        self.relational.new_row_id()
    }
}
