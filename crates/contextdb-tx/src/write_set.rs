use contextdb_core::*;
use std::collections::HashSet;

#[derive(Debug, Default, Clone)]
pub struct WriteSet {
    pub relational_inserts: Vec<(TableName, VersionedRow)>,
    pub relational_deletes: Vec<(TableName, RowId, TxId)>,
    pub adj_inserts: Vec<AdjEntry>,
    pub adj_deletes: Vec<(NodeId, EdgeType, NodeId, TxId)>,
    pub vector_inserts: Vec<VectorEntry>,
    pub vector_deletes: Vec<(VectorIndexRef, RowId, TxId)>,
    pub vector_moves: Vec<(VectorIndexRef, RowId, RowId, TxId)>,
    pub commit_lsn: Option<Lsn>,
    pub visibility_floor: Option<TxId>,
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
            && self.vector_moves.is_empty()
    }

    pub fn stamp_lsn(&mut self, lsn: Lsn) {
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

    pub fn canonicalize_final_state(&mut self) {
        if self.relational_inserts.len() > 1 {
            let mut seen = HashSet::new();
            let mut inserts = self
                .relational_inserts
                .drain(..)
                .rev()
                .filter(|(table, row)| seen.insert((table.clone(), row.row_id)))
                .collect::<Vec<_>>();
            inserts.reverse();
            self.relational_inserts = inserts;
        }
    }

    pub fn reassign_tx(&mut self, from: TxId, to: TxId) {
        if from == to {
            return;
        }

        for (_, row) in &mut self.relational_inserts {
            if row.created_tx == from {
                row.created_tx = to;
            }
            if row.deleted_tx == Some(from) {
                row.deleted_tx = Some(to);
            }
        }
        for (_, _, deleted_tx) in &mut self.relational_deletes {
            if *deleted_tx == from {
                *deleted_tx = to;
            }
        }
        for entry in &mut self.adj_inserts {
            if entry.created_tx == from {
                entry.created_tx = to;
            }
            if entry.deleted_tx == Some(from) {
                entry.deleted_tx = Some(to);
            }
        }
        for (_, _, _, deleted_tx) in &mut self.adj_deletes {
            if *deleted_tx == from {
                *deleted_tx = to;
            }
        }
        for entry in &mut self.vector_inserts {
            if entry.created_tx == from {
                entry.created_tx = to;
            }
            if entry.deleted_tx == Some(from) {
                entry.deleted_tx = Some(to);
            }
        }
        for (_, _, deleted_tx) in &mut self.vector_deletes {
            if *deleted_tx == from {
                *deleted_tx = to;
            }
        }
        for (_, _, _, tx) in &mut self.vector_moves {
            if *tx == from {
                *tx = to;
            }
        }
    }
}

pub trait WriteSetApplicator: Send + Sync {
    fn apply(&self, ws: WriteSet) -> Result<()>;
    fn new_row_id(&self) -> RowId;
}

impl WriteSetApplicator for Box<dyn WriteSetApplicator> {
    fn apply(&self, ws: WriteSet) -> Result<()> {
        (**self).apply(ws)
    }

    fn new_row_id(&self) -> RowId {
        (**self).new_row_id()
    }
}
