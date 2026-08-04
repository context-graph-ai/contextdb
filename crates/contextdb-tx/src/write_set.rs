use contextdb_core::*;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq)]
pub struct RelationalDeletePredicate {
    pub table: TableName,
    pub predicates: Vec<(ColName, Value)>,
}

#[derive(Debug, Default, Clone)]
pub struct WriteSet {
    pub relational_inserts: Vec<(TableName, VersionedRow)>,
    pub relational_deletes: Vec<(TableName, RowId, TxId)>,
    pub relational_delete_predicates: Vec<RelationalDeletePredicate>,
    pub adj_inserts: Vec<AdjEntry>,
    pub adj_deletes: Vec<(NodeId, EdgeType, NodeId, TxId)>,
    pub vector_inserts: Vec<VectorEntry>,
    pub vector_deletes: Vec<(VectorIndexRef, RowId, TxId)>,
    pub vector_moves: Vec<(VectorIndexRef, RowId, RowId, TxId)>,
    pub commit_lsn: Option<Lsn>,
    pub relational_insert_source_lsns: HashMap<TableName, HashMap<RowId, Lsn>>,
    pub relational_insert_source_kinds: HashMap<TableName, HashMap<RowId, u8>>,
    /// Sync provenance for rows this transaction does NOT rewrite. The sidecars
    /// above ride a staged row version, so they cannot describe a row whose
    /// stored values are already correct — the case where an incoming row
    /// restates what is held, and an immutable table forbids rewriting it at
    /// all. Carrying the mark here keeps that record in the same commit as the
    /// apply that learned it.
    pub sync_source_provenance_marks: Vec<(TableName, RowId, Lsn, u8)>,
    /// Engine-owned durable metadata written in the same storage transaction
    /// as this write set. Sync uses this for authenticated applied receipts.
    pub config_writes: Vec<(String, Vec<u8>)>,
    /// Config keys whose `u64` payload is a monotonic frontier. Persistence
    /// compares and raises these inside the write transaction so concurrent
    /// authenticated sync applies cannot publish an older receipt last.
    pub config_max_u64_keys: Vec<String>,
    /// A transaction-owned engine sidecar needs the normal commit boundary
    /// even when it has no row/edge/vector writes.  The sidecar itself stays
    /// in the engine; this bit only prevents the transaction manager's empty
    /// write-set fast path from skipping LSN allocation and `store.apply`.
    pub requires_commit_lsn: bool,
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
            && self.config_writes.is_empty()
            && self.sync_source_provenance_marks.is_empty()
            && !self.requires_commit_lsn
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

    pub fn set_relational_insert_source_lsn(
        &mut self,
        table: impl Into<TableName>,
        row_id: RowId,
        lsn: Lsn,
    ) {
        let table = table.into();
        self.relational_insert_source_lsns
            .entry(table.clone())
            .or_default()
            .insert(row_id, lsn);
        self.relational_insert_source_kinds
            .entry(table)
            .or_default()
            .insert(row_id, 0);
    }

    pub fn set_relational_insert_source_lsn_kind(
        &mut self,
        table: impl Into<TableName>,
        row_id: RowId,
        lsn: Lsn,
        kind: u8,
    ) {
        let table = table.into();
        self.relational_insert_source_lsns
            .entry(table.clone())
            .or_default()
            .insert(row_id, lsn);
        self.relational_insert_source_kinds
            .entry(table)
            .or_default()
            .insert(row_id, kind);
    }

    pub fn set_sync_source_provenance_mark(
        &mut self,
        table: impl Into<TableName>,
        row_id: RowId,
        lsn: Lsn,
        kind: u8,
    ) {
        self.sync_source_provenance_marks
            .push((table.into(), row_id, lsn, kind));
    }
}

pub fn row_matches_delete_predicates(
    predicates: &[RelationalDeletePredicate],
    table: &str,
    row: &VersionedRow,
) -> bool {
    predicates.iter().any(|predicate| {
        predicate.table == table
            && !predicate.predicates.is_empty()
            && predicate
                .predicates
                .iter()
                .all(|(column, value)| row.values.get(column) == Some(value))
    })
}

pub trait WriteSetApplicator: Send + Sync {
    fn apply(&self, ws: &WriteSet) -> Result<()>;
    fn new_row_id(&self) -> RowId;
}

impl WriteSetApplicator for Box<dyn WriteSetApplicator> {
    fn apply(&self, ws: &WriteSet) -> Result<()> {
        (**self).apply(ws)
    }

    fn new_row_id(&self) -> RowId {
        (**self).new_row_id()
    }
}
