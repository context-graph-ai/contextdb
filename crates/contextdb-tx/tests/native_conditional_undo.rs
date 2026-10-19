//! Conditional removal pre-admits every inverse it publishes. A refusal at
//! any journal-node boundary leaves row-owned bodies and before-owners intact.
use contextdb_core::{AdjEntry, Error, Lsn, NodeId, Result, RowId, TxId};
use contextdb_tx::manager::{EnginePreparation, EngineRead, EngineTransactions, WriteSetCounts};
use contextdb_tx::staged_source::*;
use contextdb_tx::{OwnedAdjEntry, RowEdgeOwner, WriteSet, WriteSetApplicator};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct Funding {
    used: AtomicUsize,
    limit: AtomicUsize,
    requests: Mutex<Vec<usize>>,
}
impl Funding {
    fn new() -> Self {
        Self {
            used: AtomicUsize::new(0),
            limit: AtomicUsize::new(usize::MAX),
            requests: Mutex::new(Vec::new()),
        }
    }
}
impl SourceFunding for Funding {
    fn admit_source(&self, bytes: usize) -> Result<SourceLease> {
        self.used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.limit.load(Ordering::SeqCst))
            })
            .map_err(|_| Error::Other("injected native byte limit".into()))?;
        self.requests.lock().push(bytes);
        Ok(SourceLease {
            identity: 1,
            epoch: 1,
        })
    }
    fn retain_source_lease(&self, _: SourceLease) {}
    fn release_source(&self, bytes: usize, _: SourceLease) {
        self.used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                used.checked_sub(bytes)
            })
            .expect("every native release has a live invoice");
    }
    fn copy_row(self: Arc<Self>, row: &contextdb_core::VersionedRow) -> Result<AdmittedSourceRow> {
        let invoice = OriginalRowInvoice::from_charge(SourceCharge::admit(self, row.estimated_bytes())?);
        Ok(AdmittedSourceRow::new(row.clone(), invoice))
    }
    fn copy_eager_row(
        self: Arc<Self>,
        _: &contextdb_core::VersionedRow,
    ) -> Result<AdmittedEagerSourceRow> {
        unreachable!("fixture stages auxiliary bodies only")
    }
    fn copy_vector(
        self: Arc<Self>,
        _: &contextdb_core::VectorEntry,
    ) -> Result<AdmittedSourceVector> {
        unreachable!("fixture stages auxiliary bodies only")
    }
    fn copy_key(self: Arc<Self>, _: &contextdb_core::IndexKey) -> Result<AdmittedSourceKey> {
        unreachable!("fixture stages auxiliary bodies only")
    }
    fn copy_aux(self: Arc<Self>, bodies: AuxBorrow<'_>) -> Result<AdmittedAuxBatch> {
        let len = match bodies {
            AuxBorrow::RowAdjInserts(v) => v.len(),
            AuxBorrow::RowAdjDeletes(v) => v.len(),
            _ => unreachable!("only row-owned lanes are seeded"),
        };
        let body_size = match bodies {
            AuxBorrow::RowAdjInserts(_) => std::mem::size_of::<OwnedAdjEntry>(),
            _ => std::mem::size_of::<(OwnedAdjEntry, TxId)>(),
        };
        let buffer = SourceCharge::admit(
            self.clone(),
            len * (body_size + std::mem::size_of::<OriginalAuxInvoice>()),
        )?;
        let mut invoices = Vec::with_capacity(len);
        match bodies {
            AuxBorrow::RowAdjInserts(values) => {
                let mut copies = Vec::with_capacity(len);
                for value in values {
                    invoices.push(OriginalAuxInvoice::from_charge(SourceCharge::admit(
                        self.clone(),
                        value.estimated_bytes(),
                    )?));
                    copies.push(value.clone());
                }
                Ok(AdmittedAuxBatch::from_parts(
                    AuxBodies::RowAdjInserts(copies),
                    invoices,
                    buffer,
                ))
            }
            AuxBorrow::RowAdjDeletes(values) => {
                let mut copies = Vec::with_capacity(len);
                for value in values {
                    invoices.push(OriginalAuxInvoice::from_charge(SourceCharge::admit(
                        self.clone(),
                        value.0.estimated_bytes() + std::mem::size_of::<TxId>(),
                    )?));
                    copies.push(value.clone());
                }
                Ok(AdmittedAuxBatch::from_parts(
                    AuxBodies::RowAdjDeletes(copies),
                    invoices,
                    buffer,
                ))
            }
            _ => unreachable!(),
        }
    }
}
struct Store;
impl WriteSetApplicator for Store {
    fn apply(
        &self,
        _: &WriteSet,
        _: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        unreachable!("no durable commit is requested")
    }
    fn new_row_id(&self) -> RowId {
        unreachable!("explicit row identities")
    }
}
fn edge() -> OwnedAdjEntry {
    OwnedAdjEntry {
        owner: RowEdgeOwner {
            table: "edges".into(),
            table_generation: 4,
            row_id: RowId(7),
            created_tx: TxId(3),
            lsn: Lsn(9),
        },
        entry: AdjEntry {
            source: NodeId::from_u128(1),
            target: NodeId::from_u128(2),
            edge_type: "LINK".into(),
            properties: Default::default(),
            created_tx: TxId(3),
            deleted_tx: None,
            lsn: Lsn(9),
        },
    }
}
fn counts(read: EngineRead<'_>) -> WriteSetCounts {
    WriteSetCounts {
        relational_inserts: read.relational_inserts.len(),
        relational_deletes: read.relational_deletes.len(),
        row_adj_inserts: read.row_adj_inserts.len(),
        row_adj_deletes: read.row_adj_deletes.len(),
        adj_inserts: read.adj_inserts.len(),
        adj_deletes: read.adj_deletes.len(),
        vector_inserts: read.vector_inserts.len(),
        vector_deletes: read.vector_deletes.len(),
        vector_moves: read.vector_moves.len(),
    }
}
#[test]
fn conditional_removal_funds_seven_inverses_and_rolls_back_at_every_byte_limit() {
    for mode in [EnginePreparation::Ordinary, EnginePreparation::Eager] {
        let funding = Arc::new(Funding::new());
        let txs = EngineTransactions::new_funded_with_preparation(
            Store,
            SourceFunder::from_host(funding.clone()),
            mode,
        );
        let tx = txs.try_begin().unwrap();
        let insert = edge();
        let before_delete = (edge(), TxId(11));
        txs.with_engine(tx, |guard| {
            let batch = guard.copy_aux(AuxBorrow::RowAdjInserts(std::slice::from_ref(&insert)))?;
            let prepared = guard.prepare_aux_splice(0, &[], batch)?;
            guard.publish_tx(prepared);
            let batch = guard.copy_aux(AuxBorrow::RowAdjDeletes(std::slice::from_ref(
                &before_delete,
            )))?;
            let prepared = guard.prepare_aux_splice(0, &[], batch)?;
            guard.publish_tx(prepared);
            Ok(())
        })
        .unwrap();
        let stable_balance = funding.used.load(Ordering::SeqCst);
        let mut node_bytes = 0;
        let error=txs.with_engine(tx,|guard|guard.with_tx_checkpoint(|guard|{
            let before=counts(guard.read());
            funding.requests.lock().clear();
            let prepared=guard.prepare_remove_conditional_slice(before,before)?;
            let requests=funding.requests.lock().clone();
            // Ordinary preparation isolates the shared seven-node journal.
            // Eager preparation additionally retains original capture grants.
            if mode == EnginePreparation::Ordinary {
                assert_eq!(requests.len(),7,"every one of the seven published Aux inverses needs a journal node, including empty slices");
                assert!(requests.iter().all(|bytes|*bytes==requests[0]));
                node_bytes=requests[0];
            }
            guard.publish_tx(prepared);
            Err::<(),_>(Error::Other("rollback after conditional publication".into()))
        })).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("rollback after conditional publication")
        );
        let removed = txs.with_engine(tx, |guard| guard.with_tx_checkpoint(|guard| {
            let after = counts(guard.read());
            let mut before = after;
            before.row_adj_inserts = 0;
            let prepared = guard.prepare_remove_conditional_slice(before, after)?;
            guard.publish_tx(prepared);
            assert!(guard.read().row_adj_inserts.is_empty(), "the selected witness is actually retired before rollback");
            assert_eq!(guard.read().row_adj_deletes, std::slice::from_ref(&before_delete), "a surviving final deletion keeps its before-owner outside the discarded insert range");
            Err::<(), _>(Error::Other("rollback after witness retirement".into()))
        }));
        assert!(
            removed
                .unwrap_err()
                .to_string()
                .contains("rollback after witness retirement")
        );
        txs.with_read(tx, |read| {
            assert_eq!(read.row_adj_inserts, std::slice::from_ref(&insert));
            assert_eq!(read.row_adj_deletes, std::slice::from_ref(&before_delete));
        })
        .unwrap();
        assert_eq!(
            funding.used.load(Ordering::SeqCst),
            stable_balance,
            "rollback retains exactly the original payer balance"
        );
        let boundaries = if mode == EnginePreparation::Ordinary {
            0..8
        } else {
            0..0
        };
        for admitted_nodes in boundaries {
            let result = txs.with_engine(tx, |guard| {
                guard.with_tx_checkpoint(|guard| {
                    let before = counts(guard.read());
                    let used = funding.used.load(Ordering::SeqCst);
                    funding
                        .limit
                        .store(used + admitted_nodes * node_bytes, Ordering::SeqCst);
                    let prepared = guard.prepare_remove_conditional_slice(before, before);
                    funding.limit.store(usize::MAX, Ordering::SeqCst);
                    if admitted_nodes < 7 {
                        assert!(
                            prepared.is_err(),
                            "six funded records cannot publish seven inverses"
                        );
                        assert_eq!(
                            funding.used.load(Ordering::SeqCst),
                            used,
                            "failed preparation returns every partial node invoice"
                        );
                    } else {
                        assert_eq!(funding.used.load(Ordering::SeqCst), used + 7 * node_bytes);
                        guard.publish_tx(prepared?);
                    }
                    Err::<(), _>(Error::Other("rollback after budget boundary".into()))
                })
            });
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("rollback after budget boundary")
            );
            txs.with_read(tx, |read| {
                assert_eq!(read.row_adj_inserts, std::slice::from_ref(&insert));
                assert_eq!(
                    read.row_adj_deletes,
                    std::slice::from_ref(&before_delete),
                    "a final deletion retains its exact before-owner and deleting transaction"
                );
            })
            .unwrap();
        }
        assert_eq!(
            funding.used.load(Ordering::SeqCst),
            stable_balance,
            "every failed or rolled-back splice preserves original invoices"
        );
        drop(txs);
        assert_eq!(
            funding.used.load(Ordering::SeqCst),
            0,
            "all native body/control/journal invoices settle"
        );
    }
}

// Same existing receipt for reader descriptors and original writer invoices;
// separate instances preserve payer attribution without another oracle.
impl contextdb_core::read_memory::ReadMemoryBudget for Funding {
    fn try_reserve(&self, bytes: usize, subsystem: &'static str, operation: &'static str, hint: &'static str) -> Result<()> {
        self.admit_source(bytes).map(|_| ()).map_err(|_| Error::MemoryBudgetExceeded {
            subsystem: subsystem.into(), operation: operation.into(), requested_bytes: bytes,
            available_bytes: self.limit.load(Ordering::SeqCst).saturating_sub(self.used.load(Ordering::SeqCst)),
            budget_limit_bytes: self.limit.load(Ordering::SeqCst), hint: hint.into(),
        })
    }
    fn release(&self, bytes: usize) { self.release_source(bytes, SourceLease { identity: 1, epoch: 1 }); }
}
fn custody_row(tx: TxId, text: &str) -> contextdb_core::VersionedRow {
    contextdb_core::VersionedRow { row_id: RowId(7), created_tx: tx, deleted_tx: None, lsn: Lsn(0), created_at: None,
        values: std::collections::HashMap::from([("body".into(), contextdb_core::Value::Text(text.into()))]) }
}
fn publish_custody_row(guard: &mut contextdb_tx::manager::EngineGuard<'_>, funding: &Arc<Funding>, tx: TxId, text: &str) -> Result<()> {
    let row = custody_row(tx, text);
    let invoice = OriginalRowInvoice::from_charge(SourceCharge::admit(funding.clone(), row.estimated_bytes())?);
    let prepared = guard.prepare_row_from_meta("edges", row, &contextdb_core::TableMeta::default(), invoice)?;
    guard.publish(prepared);
    Ok(())
}
fn assert_custody_body(rows: &CanonicalStatementRows, text: &str) {
    rows.with_row(RowId(7), |row| { assert_eq!(row.unwrap().values["body"], contextdb_core::Value::Text(text.into())); Ok(()) }).unwrap();
}
// Captured rows must keep their real original invoice through terminal rollback.
// A dropped-root mutant passes the pre-existing Aux-only rollback test.
fn sealed_custody_survives_mutation(operation: &str, directory_first: bool) {
    let funding = Arc::new(Funding::new());
    let reader = Arc::new(Funding::new());
    let txs = EngineTransactions::new_funded_with_preparation(Store, SourceFunder::from_host(funding.clone()), EnginePreparation::Ordinary);
    let idle = funding.used.load(Ordering::SeqCst);
    let tx = txs.try_begin().unwrap();
    txs.with_engine(tx, |g| publish_custody_row(g, &funding, tx, "original")).unwrap();
    let (directory, rows) = txs.with_engine(tx, |g| {
        let directory = g.capture_sealed_directory(reader.clone())?;
        let rows = directory.table_rows("edges", reader.clone())?;
        Ok((directory, rows))
    }).unwrap();
    let clone = rows.clone();
    let captured_writer = funding.used.load(Ordering::SeqCst);
    let captured_reader = reader.used.load(Ordering::SeqCst);
    assert!(captured_reader > 0);
    if operation == "checkpoint" {
        let result = txs.with_engine(tx, |g| g.with_tx_checkpoint(|g| {
            publish_custody_row(g, &funding, tx, "replacement")?;
            let fresh = g.capture_sealed_directory(reader.clone())?.table_rows("edges", reader.clone())?;
            assert_custody_body(&fresh, "replacement");
            assert_custody_body(&rows, "original");
            Err::<(), _>(Error::Other("rollback replacement".into()))
        }));
        assert!(result.unwrap_err().to_string().contains("rollback replacement"));
        assert_eq!(funding.used.load(Ordering::SeqCst), captured_writer);
    } else if operation == "replace" {
        txs.with_engine(tx, |g| publish_custody_row(g, &funding, tx, "replacement")).unwrap();
        let fresh = txs.with_engine(tx, |g| g.capture_sealed_directory(reader.clone())?.table_rows("edges", reader.clone())).unwrap();
        assert_custody_body(&fresh, "replacement");
    }
    assert_custody_body(&rows, "original");
    drop(txs.rollback_owned(tx).unwrap());
    assert_custody_body(&rows, "original");
    let held = funding.used.load(Ordering::SeqCst);
    assert!(held > idle, "rollback cannot refund the still-live original canonical directory");
    assert_eq!(reader.used.load(Ordering::SeqCst), captured_reader, "cloning shares one descriptor credit");
    if directory_first {
        drop(directory);
        let table_held = funding.used.load(Ordering::SeqCst);
        assert!(table_held > idle, "the original table and body remain funded after directory metadata is released");
        drop(rows);
        assert_eq!(funding.used.load(Ordering::SeqCst), table_held, "the last table clone keeps the original funded body");
        assert_custody_body(&clone, "original");
        drop(clone);
    } else {
        drop(rows); drop(clone);
        assert_eq!(funding.used.load(Ordering::SeqCst), held, "the directory alone keeps the original funded body");
        let fresh = directory.table_rows("edges", reader.clone()).unwrap();
        assert_custody_body(&fresh, "original"); drop(fresh); drop(directory);
    }
    assert_eq!(funding.used.load(Ordering::SeqCst), idle, "last descriptor releases to the live manager's exact baseline");
    assert_eq!(reader.used.load(Ordering::SeqCst), 0);
    drop(txs);
    assert_eq!(funding.used.load(Ordering::SeqCst), 0, "original provider remains alive to observe final refund");
}
#[test]
fn sealed_directory_retains_original_payer_after_replacement() { sealed_custody_survives_mutation("replace", true); }
#[test]
fn sealed_directory_retains_original_payer_after_checkpoint_rollback() { sealed_custody_survives_mutation("checkpoint", true); }
#[test]
fn sealed_directory_retains_original_payer_after_transaction_rollback() { sealed_custody_survives_mutation("rollback", true); }
#[test]
fn sealed_directory_outlives_table_descriptors() { sealed_custody_survives_mutation("replace", false); }
#[test]
fn sealed_capture_admission_refusal_preserves_canonical_source_and_payers() {
    let funding = Arc::new(Funding::new()); let reader = Arc::new(Funding::new());
    let txs = EngineTransactions::new_funded_with_preparation(Store, SourceFunder::from_host(funding.clone()), EnginePreparation::Ordinary);
    let tx = txs.try_begin().unwrap();
    txs.with_engine(tx, |g| publish_custody_row(g, &funding, tx, "original")).unwrap();
    let stable = funding.used.load(Ordering::SeqCst);
    reader.limit.store(0, Ordering::SeqCst);
    let error = txs.with_engine(tx, |g| g.capture_sealed_directory(reader.clone()).map(|_| ())).unwrap_err();
    assert!(matches!(error, Error::MemoryBudgetExceeded { .. }));
    assert_eq!(funding.used.load(Ordering::SeqCst), stable); assert_eq!(reader.used.load(Ordering::SeqCst), 0);
    reader.limit.store(usize::MAX, Ordering::SeqCst);
    let directory = txs.with_engine(tx, |g| g.capture_sealed_directory(reader.clone())).unwrap();
    let descriptor_bytes = reader.used.load(Ordering::SeqCst);
    reader.limit.store(descriptor_bytes, Ordering::SeqCst);
    assert!(matches!(directory.table_rows("edges", reader.clone()), Err(Error::MemoryBudgetExceeded { .. })));
    assert_eq!(reader.used.load(Ordering::SeqCst), descriptor_bytes); assert_eq!(funding.used.load(Ordering::SeqCst), stable);
    reader.limit.store(usize::MAX, Ordering::SeqCst);
    let rows = directory.table_rows("edges", reader.clone()).unwrap(); assert_custody_body(&rows, "original");
    drop(rows); drop(directory); drop(txs.rollback_owned(tx).unwrap()); drop(txs);
    assert_eq!(reader.used.load(Ordering::SeqCst), 0); assert_eq!(funding.used.load(Ordering::SeqCst), 0);
}

// Directory/table capture metadata must stay fixed as selected row bodies grow.
#[test]
fn sealed_directory_descriptor_charge_is_fixed_across_row_doublings() {
    let mut charges = Vec::new();
    for n in [1usize, 2, 4] {
        let funding = Arc::new(Funding::new());
        let reader = Arc::new(Funding::new());
        let txs = EngineTransactions::new_funded_with_preparation(Store, SourceFunder::from_host(funding.clone()), EnginePreparation::Ordinary);
        let idle = funding.used.load(Ordering::SeqCst);
        let tx = txs.try_begin().unwrap();
        txs.with_engine(tx, |g| {
            for id in 0..n {
                let mut row = custody_row(tx, &"body".repeat(1024)); row.row_id = RowId(id as u64);
                let invoice = OriginalRowInvoice::from_charge(SourceCharge::admit(funding.clone(), row.estimated_bytes())?);
                let p = g.prepare_row_from_meta("edges", row, &contextdb_core::TableMeta::default(), invoice)?; g.publish(p);
            }
            Ok(())
        }).unwrap();
        let writer_before = funding.used.load(Ordering::SeqCst);
        let directory = txs.with_engine(tx, |g| g.capture_sealed_directory(reader.clone())).unwrap();
        let rows = directory.table_rows("edges", reader.clone()).unwrap();
        assert_eq!(funding.used.load(Ordering::SeqCst), writer_before, "capture shares canonical custody without another writer debit");
        charges.push(reader.used.load(Ordering::SeqCst));
        assert_eq!(rows.len(), n);
        drop(txs.rollback_owned(tx).unwrap());
        let mut position = None; let mut seen = 0;
        while let Some((sequence, id)) = rows.next_unordered(position) {
            rows.with_row(id, |row| { assert_eq!(row.unwrap().values["body"], contextdb_core::Value::Text("body".repeat(1024))); Ok(()) }).unwrap();
            seen += 1; position = Some(sequence);
        }
        assert_eq!(seen, n); assert!(funding.used.load(Ordering::SeqCst) > idle);
        drop(rows); drop(directory);
        assert_eq!(funding.used.load(Ordering::SeqCst), idle); assert_eq!(reader.used.load(Ordering::SeqCst), 0);
    }
    assert!(charges[0] > 0); assert_eq!(charges, vec![charges[0]; 3]);
}
