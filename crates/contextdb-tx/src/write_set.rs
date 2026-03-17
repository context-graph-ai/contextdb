use contextdb_core::*;

#[derive(Debug, Default)]
pub struct WriteSet {
    pub relational_inserts: Vec<(TableName, VersionedRow)>,
    pub relational_deletes: Vec<(TableName, RowId, TxId)>,
    pub adj_inserts: Vec<AdjEntry>,
    pub adj_deletes: Vec<(NodeId, EdgeType, NodeId, TxId)>,
    pub vector_inserts: Vec<VectorEntry>,
    pub vector_deletes: Vec<(RowId, TxId)>,
    pub commit_lsn: Option<u64>,
    pub propagation_in_progress: bool,
}

impl WriteSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.relational_inserts.is_empty()
            && self.relational_deletes.is_empty()
            && self.adj_inserts.is_empty()
            && self.adj_deletes.is_empty()
            && self.vector_inserts.is_empty()
            && self.vector_deletes.is_empty()
    }

    pub fn stamp_lsn(&mut self, lsn: u64) {
        self.commit_lsn = Some(lsn);

        for (_, row) in &mut self.relational_inserts {
            row.lsn = lsn;
        }

        for entry in &mut self.adj_inserts {
            entry.lsn = lsn;
        }

        for entry in &mut self.vector_inserts {
            entry.lsn = lsn;
        }
    }
}

pub trait WriteSetApplicator: Send + Sync {
    fn apply(&self, ws: WriteSet) -> Result<()>;
    fn new_row_id(&self) -> RowId;
}
