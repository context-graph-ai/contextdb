//! Synchronous query access to the one unpublished received working write.
//! No table, range, queue or transaction-state guard crosses a caller callback.
use super::*;
use contextdb_core::Direction;
use super::received_working::ReceivedWorkingWrite;
use contextdb_core::read_memory::{OwnedReadBox, ReadCredit, ReadMemoryBudget};
use contextdb_relational::source::{RelationalCursor, RelationalCursorOwner,
    RelationalReadSource, RelationalWindow, RelationalIndexRange, RelationalPosting,
    RelationalPositionKey};
use contextdb_relational::store::{IndexEntry,SyncSourceKind};
use std::ops::Bound;

enum ReadCopy<T> {End,Need(usize),Ready(T)}

struct Context<'o> {
    token: crate::database::DatabaseId,
    host: usize,
    source: WorkingReadSource,
    frontier:std::cell::Cell<(SnapshotId,Lsn)>,
    write: &'o ReceivedWorkingWrite,
    original: &'o crate::memory_accounting::OwnedMemoryReservation,
    store: &'o RedbPersistence,
    metadata: &'o contextdb_relational::store::RelationalStore,
    graph_schema_scope: &'o HashSet<String>,
    failure: std::cell::RefCell<Option<Error>>,
    failed: std::cell::Cell<bool>,
}
thread_local! {
    static ACTIVE: std::cell::Cell<*const Context<'static>> = const { std::cell::Cell::new(std::ptr::null()) };
}
struct Scope {
    previous: *const Context<'static>,
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl Drop for Scope {
    fn drop(&mut self) { ACTIVE.with(|slot|slot.set(self.previous)); }
}
#[derive(Clone)]
pub(crate) struct WorkingReadSource {
    token: crate::database::DatabaseId,
    control_identity: u64,
    budget: Arc<dyn ReadMemoryBudget>,
    root: WriterSnapshotHandle,
    snapshot: SnapshotId,
    lsn: Lsn,
}
impl ReceivedWorkingWrite {
    pub(crate) fn with_source<R>(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        store: &RedbPersistence,
        metadata: &contextdb_relational::store::RelationalStore,
        graph_schema_scope: &HashSet<String>,
        host: usize,
        database:crate::database::DatabaseId,
        root: WriterSnapshotHandle, snapshot: SnapshotId, lsn: Lsn,
        use_source: impl FnOnce(WorkingReadSource) -> Result<R>,
    ) -> Result<R> {
        self.require_original(original)?;
        self.require_store(store)?;
        root.require_working_store(store)?;
        self.claim_database_loan(database)?;
        for (table,meta) in metadata.table_meta.read().iter() {
            self.remember_schema(original,table,meta)?;
        }
        let token=database;
        // This projection clones precisely the lent writer use, not a current
        // registration discovered from a database accountant.
        let budget = crate::memory_accounting::persistence_original_projection(original)?.read_budget()?;
        let source=WorkingReadSource {token,control_identity:super::received_working::next_working_control_identity()?,budget,root,snapshot,lsn};
        let context=Context {token,host,source:source.clone(),frontier:std::cell::Cell::new((snapshot,lsn)),write:self,original,store,metadata,graph_schema_scope,failure:std::cell::RefCell::new(None),failed:std::cell::Cell::new(false)};
        let previous = ACTIVE.with(|slot|slot.replace((&context as *const Context<'_>).cast()));
        let _scope = Scope { previous,_thread:std::marker::PhantomData };
        let result=use_source(source);
        let failure=context.failure.borrow_mut().take();
        if let Some(error)=failure {
            drop(result);
            return Err(error);
        }
        result
    }
}
/// Direct terminal work has no source loan. A live source must belong to the
/// exact write and authentic Original; an unrelated nested context cannot lend
/// its identity to a witness. No context reference escapes this call.
pub(super) fn graph_witness_loan(write: &ReceivedWorkingWrite,
    original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<Option<u64>> {
    write.require_original(original)?;
    ACTIVE.with(|slot| {
        let pointer = slot.get();
        if pointer.is_null() { return Ok(None); }
        // SAFETY: identical synchronous stack lifetime to context().
        let context = unsafe { &*pointer };
        if !std::ptr::eq(context.write, write) || !std::ptr::eq(context.original, original) {
            return Err(Error::Other("working graph witness has another source loan".into()));
        }
        if context.failed.get() { return Err(Error::ReadCancelled); }
        Ok(Some(context.source.control_identity))
    })
}
impl WorkingReadSource {
    pub(crate) fn current_for(host:usize)->Result<Option<Self>> {
        ACTIVE.with(|slot| {
            let pointer=slot.get();
            if pointer.is_null() {return Ok(None);}
            // Same scoped stack loan as context(); only the exact private
            // transaction host can select this already-lent source.
            let context=unsafe {&*pointer};
            Ok((context.host==host).then(|| {
                let mut source=context.source.clone();
                (source.snapshot,source.lsn)=context.frontier.get();
                source
            }))
        })
    }
    pub(crate) fn current_for_relational(store: &contextdb_relational::store::RelationalStore) -> Result<Option<Self>> {
        ACTIVE.with(|slot| {
            let pointer = slot.get();
            if pointer.is_null() { return Ok(None); }
            // Same synchronous loan as current_for; an unrelated receiver
            // store cannot accidentally select this private working image.
            let context = unsafe { &*pointer };
            Ok(std::ptr::eq(context.metadata, store).then(|| context.source.clone()))
        })
    }
    pub(crate) fn apply_private_graph(&self, ws: &contextdb_tx::WriteSet) -> Result<()> {
        self.context(|context| {
            if context.write.graph_capturing(context.original)? { return Ok(()); }
            context.write.apply_graph_replacements(context.original, &ws.adj_deletes, &ws.adj_inserts)
        })
    }
    pub(crate) fn remove_private_graph_identity(&self, source: NodeId, kind: &str, target: NodeId) -> Result<()> {
        self.context(|context| context.write.remove_graph_identity(context.original, source, kind, target))
    }
    pub(crate) fn rewrite_private_graph(&self,
        transform: impl FnMut(&mut AdjEntry) -> Result<super::received_working_graph::GraphRewrite>,
    ) -> Result<()> {
        self.context(|context| context.write.rewrite_graph(context.original, transform))
    }
    pub(crate) fn apply_private_vector_plan(&self, plan: &PreparedVectorPartitionMutationBatch, ws: &contextdb_tx::WriteSet) -> Result<()> {
        self.context(|context| {
            context.write.apply_vector_partition_plan(context.original, plan, ws)?;
            context.write.refresh_vector_catalog(context.original)
        })
    }
    pub(crate) fn same_working_source(&self, other: &Self) -> bool {
        self.token == other.token && self.control_identity == other.control_identity && self.root.ptr_eq(&other.root)
    }
    pub(crate) fn reader_budget(&self) -> Arc<dyn ReadMemoryBudget> { self.budget.clone() }
    pub(crate) fn rewrite_private_vectors(
        &self,
        body: impl FnMut(&OriginalPersistenceFrame<'_>, &mut super::received_working_vector::WorkingVectorIdentity<'_>) -> Result<bool>,
        member: impl FnMut(&OriginalPersistenceFrame<'_>, &mut VectorPartitionMembershipRecord) -> Result<bool>,
    ) -> Result<()> {
        self.context(|context| {
            context.write.rewrite_vector_image(context.original, body, member)?;
            context.write.refresh_vector_catalog(context.original)
        })
    }
    pub(crate) fn rewrite_private_row_identities(&self, table: &str,
        stamp: impl FnMut(&mut super::received_working::WorkingRowIdentity<'_>) -> Result<()>) -> Result<()> {
        self.context(|context| {
            self.root.check_working_scope(table)?;
            let metadata = context.metadata.table_meta.read();
            context.write.rewrite_row_identities(context.store, context.original, table, metadata.get(table), stamp)
        })
    }
    pub(crate) fn root(&self)->WriterSnapshotHandle {self.root.clone()}
    pub(crate) fn vector_reader_memory(&self) -> Result<Arc<crate::read_image_memory::ImageMemory>> {
        self.context(|context| crate::read_image_memory::ImageMemory::operation_reader(
            context.original.accountant().clone(), self.budget.clone()))
    }


    fn context<R>(&self, use_context: impl for<'o> FnOnce(&Context<'o>) -> Result<R>) -> Result<R> {
        ACTIVE.with(|slot| {
            let pointer = slot.get();
            if pointer.is_null() { return Err(Error::Other("received working source is outside its original loan".into())); }
            // SAFETY: with_source installs this synchronous stack context and
            // Scope restores its predecessor before any borrowed value dies.
            // An escaped/cross-thread cursor contains only a token and fails
            // before dereference/use; no reference can escape this HRTB call.
            let context = unsafe { &*pointer };
            if context.token != self.token || context.source.control_identity != self.control_identity { return Err(Error::Other("received working source loan does not match".into())); }
            if context.failed.get() {return Err(Error::ReadCancelled);}
            match use_context(context) {
                Ok(value)=>Ok(value),
                Err(error @ (Error::TableNotFound(_)|Error::IndexNotFound {..}))=>Err(error),
                Err(error)=>{
                    // Like the core JSON decoder's failure sideband, preserve
                    // the actual typed refusal for the unit boundary even if
                    // user code catches the private read's fixed sentinel.
                    context.write.graph_became_unready();
                    if !context.failed.replace(true) {*context.failure.borrow_mut()=Some(error);}
                    Err(Error::ReadCancelled)
                }
            }
        })
    }
    fn credit(&self, bytes:usize) -> Result<ReadCredit> {
        self.context(|_|ReadCredit::try_new(self.budget.clone(),bytes,"persistence","received_working_read",
            "Raise MEMORY_LIMIT to inspect this received unit."))
    }
    fn cursor(&self,table:&str,lower:Bound<&[u8]>,upper:Bound<&[u8]>,reverse:bool)
        -> Result<RelationalCursorOwner<VersionedRow>> {
        self.validate_table(table)?;
        let length = |bound:&Bound<&[u8]>| match bound { Bound::Included(v)|Bound::Excluded(v)=>v.len(),Bound::Unbounded=>0 };
        let payload_credit = self.credit(table.len().checked_add(length(&lower))
            .and_then(|n|n.checked_add(length(&upper))).ok_or_else(||Error::Other("working cursor endpoint overflow".into()))?)?;
        let control = self.credit(std::mem::size_of::<RowCursor>())?;
        let cursor = RowCursor { source:self.clone(),table:table.to_owned(),
            lower:lower.map(|v|v.to_vec()),upper:upper.map(|v|v.to_vec()),
            after:None,reverse,examined:0,_payload_credit:payload_credit };
        Ok(RelationalCursorOwner::new(Box::new(cursor),control))
    }
    fn box_row(&self,row:writer_source::WriterWindow<VersionedRow>)
        ->Result<OwnedReadBox<dyn RelationalWindow<VersionedRow>>> {
        row.boxed(|boxed|boxed as Box<dyn RelationalWindow<VersionedRow>>)
    }
}
struct RowCursor {
    source:WorkingReadSource,
    table:String,
    lower:Bound<Vec<u8>>,upper:Bound<Vec<u8>>,
    after:Option<[u8;24]>,
    reverse:bool,
    examined:u64,
    _payload_credit:ReadCredit,
}
fn borrowed(bound:&Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound { Bound::Included(v)=>Bound::Included(v.as_slice()),
        Bound::Excluded(v)=>Bound::Excluded(v.as_slice()),Bound::Unbounded=>Bound::Unbounded }
}
impl RelationalCursor<VersionedRow> for RowCursor {
    fn next_window(&mut self)->Result<Option<OwnedReadBox<dyn RelationalWindow<VersionedRow>>>> {
        self.next_window_before(&mut ||Ok(()))
    }
    fn next_window_before(&mut self,before:&mut dyn FnMut()->Result<()>)
        ->Result<Option<OwnedReadBox<dyn RelationalWindow<VersionedRow>>>> {
        self.source.context(|_|before())?;
        let lower = if !self.reverse { self.after.as_ref().map_or_else(||borrowed(&self.lower),|key|Bound::Excluded(key.as_slice())) }
            else { borrowed(&self.lower) };
        let upper = if self.reverse { self.after.as_ref().map_or_else(||borrowed(&self.upper),|key|Bound::Excluded(key.as_slice())) }
            else { borrowed(&self.upper) };
        let row = self.source.read_row(&self.table,lower,upper,self.reverse)?;
        if let Some(row) = row {
            self.examined=self.examined.checked_add(1).ok_or_else(||Error::Other("working cursor count overflow".into()))?;
            self.after=Some(row.position.as_ref().try_into().map_err(|_|Error::Other("working row cursor position width changed".into()))?);
            self.source.box_row(row).map(Some)
        } else { Ok(None) }
    }
    fn examined_entries(&self)->u64 { self.examined }
}
impl RelationalReadSource for WorkingReadSource {
    fn snapshot(&self)->SnapshotId { self.snapshot }
    fn lsn(&self)->Lsn { self.lsn }
    fn validate_table(&self,table:&str)->Result<()> {
        self.context(|context| {
            self.root.check_working_scope(table)?;
            if !context.metadata.table_meta.read().contains_key(table) {
                return Err(Error::TableNotFound(table.to_owned()));
            }
            context.write.prepare_rows(context.store,context.original,table)
        })
    }
    fn row_cursor(&self,table:&str,lower:Bound<&[u8]>,upper:Bound<&[u8]>,reverse:bool)
        ->Result<RelationalCursorOwner<VersionedRow>> { self.cursor(table,lower,upper,reverse) }
    fn row_id_cursor(&self,table:&str,row:RowId,snapshot:SnapshotId)
        ->Result<RelationalCursorOwner<VersionedRow>> {
        let mut lower=[0u8;24];lower[..8].copy_from_slice(&row.0.to_be_bytes());
        let mut upper=[255u8;24];upper[..8].copy_from_slice(&row.0.to_be_bytes());
        upper[8..16].copy_from_slice(&snapshot.0.to_be_bytes());
        self.cursor(table,Bound::Included(&lower),Bound::Included(&upper),true)
    }
    fn exact_row(&self,table:&str,key:&[u8;24])
        ->Result<Option<OwnedReadBox<dyn RelationalWindow<VersionedRow>>>> {
        self.validate_table(table)?;
        let row=self.read_row(table,Bound::Included(key),Bound::Included(key),false)?;
        row.map(|row|self.box_row(row)).transpose()
    }
    fn index_cursor(&self,table:&str,index:&str,range:RelationalIndexRange<'_>)
        ->Result<RelationalCursorOwner<RelationalPosting>> {
        self.validate_table(table)?;
        self.context(|context| {
            let frame=OriginalPersistenceFrame::new(context.original)?;
            let metadata=self.metadata(table)?;
            let meta=&metadata.value;
            frame.reserve(meta.estimated_bytes().saturating_mul(8).saturating_add(1024))?;
            let declarations=RedbPersistence::relational_index_declarations(meta);
            let declaration=declarations.iter().find(|decl|decl.name==index)
                .ok_or_else(||Error::IndexNotFound {table:table.to_owned(),index:index.to_owned()})?;
            context.write.prepare_index(context.store,context.original,table,declaration)
        })?;
        let length=|bound:&Bound<&[u8]>|match bound {Bound::Included(v)|Bound::Excluded(v)=>v.len(),Bound::Unbounded=>0};
        let payload_credit=self.credit(table.len().saturating_add(index.len())
            .saturating_add(length(&range.lower)).saturating_add(length(&range.upper)))?;
        let control=self.credit(std::mem::size_of::<PostingCursor>())?;
        let cursor=PostingCursor { source:self.clone(),table:table.to_owned(),index:index.to_owned(),
            lower:range.lower.map(|key|key.to_vec()),upper:range.upper.map(|key|key.to_vec()),
            after:None,endpoint_credit:None,current:range.current,reverse:range.reverse,examined:0,_payload_credit:payload_credit };
        Ok(RelationalCursorOwner::new(Box::new(cursor),control))
    }
    fn exact_index_cursor(&self,table:&str,index:&str,current:bool,key:&IndexKey)
        ->Result<RelationalCursorOwner<RelationalPosting>> {
        let lower=self.index_position_key(current,key,RowId(0),TxId(0))?;
        let upper=self.index_position_key(current,key,RowId(u64::MAX),TxId(u64::MAX))?;
        self.index_cursor(table,index,RelationalIndexRange {current,
            lower:Bound::Included(lower.as_bytes()),upper:Bound::Included(upper.as_bytes()),reverse:false})
    }
    fn index_position_key(&self,current:bool,key:&IndexKey,row:RowId,created:TxId)
        ->Result<OwnedReadBox<dyn RelationalPositionKey>> {
        // Existing endpoint codec only; it reads no row/index from this root.
        self.context(|_|self.root.index_position_key(writer_source::SourceWindowOwner::Read(self.budget.clone()),
            key,row,if current {TxId(0)}else{created})?
            .boxed(|boxed|boxed as Box<dyn RelationalPositionKey>))
    }
    fn sync_source(&self,table:&str,row:RowId)->Result<Option<(Lsn,SyncSourceKind)>> {
        self.validate_table(table)?;
        self.context(|context|context.write.sync_source(context.original,table,row))
    }
}

struct PostingCursor {
    source:WorkingReadSource,table:String,index:String,
    lower:Bound<Vec<u8>>,upper:Bound<Vec<u8>>,after:Option<Vec<u8>>,
    current:bool,reverse:bool,examined:u64,endpoint_credit:Option<ReadCredit>,_payload_credit:ReadCredit,
}
impl RelationalCursor<RelationalPosting> for PostingCursor {
    fn next_window(&mut self)->Result<Option<OwnedReadBox<dyn RelationalWindow<RelationalPosting>>>> {
        self.next_window_before(&mut ||Ok(()))
    }
    fn next_window_before(&mut self,before:&mut dyn FnMut()->Result<()>)
        ->Result<Option<OwnedReadBox<dyn RelationalWindow<RelationalPosting>>>> {
        self.source.context(|_|before())?;
        let lower=if !self.reverse {self.after.as_ref().map_or_else(||borrowed(&self.lower),|key|Bound::Excluded(key.as_slice()))}
            else {borrowed(&self.lower)};
        let upper=if self.reverse {self.after.as_ref().map_or_else(||borrowed(&self.upper),|key|Bound::Excluded(key.as_slice()))}
            else {borrowed(&self.upper)};
        let posting=self.source.read_posting(&self.table,&self.index,self.current,lower,upper,self.reverse)?;
        let Some(posting)=posting else {return Ok(None);};
        self.examined=self.examined.checked_add(1).ok_or_else(||Error::Other("working posting count overflow".into()))?;
        // Admit the successor while the prior endpoint is still owned. Drop
        // the prior allocation before returning its endpoint credit.
        let next=self.source.credit(posting.position.len())?;
        let successor=posting.position.to_vec();
        let prior=self.after.replace(successor);
        drop(prior);
        // Keep the exact one cursor endpoint bill across the next seek.
        // Separate from the immutable bounds/table bill.
        self.endpoint_credit=Some(next);
        posting.boxed(|boxed|boxed as Box<dyn RelationalWindow<RelationalPosting>>).map(Some)
    }
    fn examined_entries(&self)->u64 {self.examined}
}

impl WorkingReadSource {
    pub(crate) fn at_snapshot(mut self,snapshot:SnapshotId)->Result<Self> {
        if snapshot.0>self.snapshot.0 {
            return Err(Error::Other("working row snapshot exceeds its source frontier".into()));
        }
        self.snapshot=snapshot;
        Ok(self)
    }
    pub(crate) fn for_read(mut self,budget:Arc<dyn ReadMemoryBudget>)->Self {
        self.budget=budget;
        self
    }
    fn read_row(&self,table:&str,lower:Bound<&[u8]>,upper:Bound<&[u8]>,reverse:bool)
        ->Result<Option<writer_source::WriterWindow<VersionedRow>>> {
        self.context(|context| {
            self.root.check_working_scope(table)?;
            let memory=crate::read_image_memory::ImageMemory::operation_reader(
                context.original.accountant().clone(),self.budget.clone())?;
            let _read=memory.enter();
            memory.reserve(table.len().saturating_add(96))?;
            let name=super::received_working::working_row_name(table);
            let result=(|| {
                let mut paid=0usize;
                let (position,encoded)=loop {
                    let copied=context.write.with_write(|write| {
                        let source=write.open_table(TableDefinition::<&[u8],&[u8]>::new(name.as_str()))
                            .map_err(RedbPersistence::storage_error)?;
                        let mut range=source.range::<&[u8]>((lower,upper)).map_err(RedbPersistence::storage_error)?;
                        let entry=if reverse {range.next_back()}else{range.next()};
                        let Some((key,body))=entry.transpose().map_err(RedbPersistence::storage_error)? else {return Ok(ReadCopy::End);};
                        let needed=body.value().len();
                        if needed>paid {return Ok(ReadCopy::Need(needed));}
                        let position:[u8;24]=key.value().try_into().map_err(|_|Error::StoreCorrupted {
                            path:table.to_owned(),reason:"working read row key has invalid width".into()})?;
                        Ok(ReadCopy::Ready((position,body.value().to_vec())))
                    })?;
                    match copied {
                        ReadCopy::End=>return Ok(None),
                        ReadCopy::Need(needed)=>{memory.reserve(needed-paid)?;paid=needed;},
                        ReadCopy::Ready(value)=>break value,
                    }
                };
                // Every provider call, serde allocation and output-box
                // admission below runs after all native guards have closed.
                let row=RedbPersistence::decode_versioned_row(&encoded,None)?;
                if row.row_id.0!=u64::from_be_bytes(position[..8].try_into().unwrap())
                    ||row.created_tx.0!=u64::from_be_bytes(position[8..16].try_into().unwrap())
                    ||row.lsn.0!=u64::from_be_bytes(position[16..].try_into().unwrap()) {
                    return Err(Error::StoreCorrupted {path:table.to_owned(),reason:"working read row key and body disagree".into()});
                }
                let mut window=writer_source::WriterWindow::from_owned(&self.root,memory.clone(),row);
                window.position=Box::new(position);
                Ok(Some(window))
            })();
            if let Some(error)=memory.take_failure() {drop(result);return Err(error);}
            result
        })
    }
    fn read_posting(&self,table:&str,index:&str,current:bool,
        lower:Bound<&[u8]>,upper:Bound<&[u8]>,reverse:bool)
        ->Result<Option<writer_source::WriterWindow<RelationalPosting>>> {
        self.context(|context| {
            self.root.check_working_scope(table)?;
            let memory=crate::read_image_memory::ImageMemory::operation_reader(
                context.original.accountant().clone(),self.budget.clone())?;
            let _read=memory.enter();
            memory.reserve(table.len().saturating_add(index.len()).saturating_add(128)
                .saturating_add(match lower {Bound::Included(v)|Bound::Excluded(v)=>v.len(),Bound::Unbounded=>0})
                .saturating_add(match upper {Bound::Included(v)|Bound::Excluded(v)=>v.len(),Bound::Unbounded=>0}))?;
            let name=super::received_working::working_index_name(table,index,current);
            let owned=|bound:Bound<&[u8]>|bound.map(|key|DurableRelationalIndexKey(key.to_vec()));
            let lower=owned(lower);let upper=owned(upper);
            let result=(|| {
                let mut paid=0usize;
                let (key,value)=loop {
                    let copied=context.write.with_write(|write| {
                        let source=write.open_table(TableDefinition::<DurableRelationalIndexKey,&[u8]>::new(name.as_str()))
                            .map_err(RedbPersistence::storage_error)?;
                        let mut range=source.range::<&DurableRelationalIndexKey>((lower.as_ref(),upper.as_ref()))
                            .map_err(RedbPersistence::storage_error)?;
                        let entry=if reverse {range.next_back()}else{range.next()};
                        let Some((key,value))=entry.transpose().map_err(RedbPersistence::storage_error)? else {return Ok(ReadCopy::End);};
                        let needed=key.retained_page_bytes().checked_add(value.value().len())
                            .ok_or_else(||Error::Other("working posting copy size overflow".into()))?;
                        if needed>paid {return Ok(ReadCopy::Need(needed));}
                        Ok(ReadCopy::Ready((key.value(),value.value().to_vec())))
                    })?;
                    match copied {
                        ReadCopy::End=>return Ok(None),
                        ReadCopy::Need(needed)=>{memory.reserve(needed-paid)?;paid=needed;},
                        ReadCopy::Ready(value)=>break value,
                    }
                };
                let corrupt=|reason:String|Error::StoreCorrupted {path:"received_working".into(),reason};
                let mut position=key.decode_canonical().map_err(|error|match error {
                    Error::Other(reason)=>corrupt(reason.into_owned()),other=>other,
                })?;
                let deleted_tx=if current {
                    if position.created_tx!=TxId(0) {return Err(Error::StoreCorrupted {
                        path:table.to_owned(),reason:"working current index has a historical physical tie".into()});}
                    position.created_tx=RedbPersistence::decode_exact::<TxId>(&value)?;
                    None
                }else{
                    RedbPersistence::decode_exact::<Option<TxId>>(&value).map_err(|error|match error {
                        Error::Other(reason)=>corrupt(format!("durable index visibility record is damaged: {reason}")),
                        other=>other,
                    })?
                };
                let posting=RelationalPosting {key:position.key,entry:IndexEntry {
                    row_id:position.row_id,created_tx:position.created_tx,deleted_tx}};
                let mut window=writer_source::WriterWindow::from_owned(&self.root,memory.clone(),posting);
                window.position=key.0.into_boxed_slice();
                Ok(Some(window))
            })();
            if let Some(error)=memory.take_failure() {drop(result);return Err(error);}
            result
        })
    }
}

pub(crate) struct WorkingMetadata {
    value:TableMeta,
    _credit:ReadCredit,
}
impl WorkingMetadata {pub(crate) fn value(&self)->&TableMeta {&self.value}}
impl Drop for WorkingMetadata {fn drop(&mut self) {}}
impl WorkingReadSource {
    pub(crate) fn metadata(&self,table:&str)->Result<WorkingMetadata> {
        self.context(|context| {
            let mut credit=ReadCredit::new(self.budget.clone());
            loop {
                let needed={
                    let tables=context.metadata.table_meta.read();
                    let meta=tables.get(table).ok_or_else(||Error::TableNotFound(table.to_owned()))?;
                    meta.estimated_bytes().saturating_mul(4).saturating_add(1024)
                };
                // A provider may reenter. No metadata/storage guard crosses
                // admission; recheck the concrete clone quote afterward.
                self.context(|_|credit.try_grow_for(needed.saturating_sub(credit.bytes()),
                    "persistence","received_working_read",
                    "Raise MEMORY_LIMIT to inspect this received unit."))?;
                let tables=context.metadata.table_meta.read();
                let meta=tables.get(table).ok_or_else(||Error::TableNotFound(table.to_owned()))?;
                if meta.estimated_bytes().saturating_mul(4).saturating_add(1024)>credit.bytes() {
                    drop(tables);continue;
                }
                return Ok(WorkingMetadata {value:meta.clone(),_credit:credit});
            }
        })
    }
    pub(crate) fn preflight(&self,table:&str,index:Option<&str>,snapshot:SnapshotId,
        before:&mut dyn FnMut()->Result<()>)->Result<()> {
        self.context(|_|before())?;
        if snapshot.0>self.snapshot.0 {return Err(Error::Other("working read exceeds its source frontier".into()));}
        self.validate_table(table)?;
        if let Some(index)=index {
            self.context(|context| {
                let frame=OriginalPersistenceFrame::new(context.original)?;
                let metadata=self.metadata(table)?;
                let meta=&metadata.value;
                frame.reserve(meta.estimated_bytes().saturating_mul(8).saturating_add(1024))?;
                let declarations=RedbPersistence::relational_index_declarations(meta);
                let declaration=declarations.iter().find(|decl|decl.name==index)
                    .ok_or_else(||Error::IndexNotFound {table:table.to_owned(),index:index.to_owned()})?;
                context.write.prepare_index(context.store,context.original,table,declaration)
            })?;
        }
        self.context(|_|before())
    }
    pub(crate) fn row_head(&self,table:&str,row:RowId)->Result<(Lsn,[u8;24])> {
        let mut cursor=self.row_id_cursor(table,row,self.snapshot)?;
        let Some(window)=cursor.next_window()? else {
            return Err(Error::StoreCorrupted {path:table.to_owned(),reason:"working row head is missing".into()});
        };
        let key=window.position_key().try_into().map_err(|_|Error::StoreCorrupted {
            path:table.to_owned(),reason:"working row head has an invalid key".into()})?;
        Ok((window.value().lsn,key))
    }
}

impl WorkingReadSource {
    pub(crate) fn project_schema(&self,table:&str,arriving:Option<&TableMeta>,ddl:&[DdlChange])->Result<()> {
        self.context(|context| {
            self.root.check_working_scope(table)?;
            context.write.project_schema(context.store,context.original,table,arriving,ddl)?;
            context.write.refresh_vector_catalog(context.original)
        })
    }
    pub(crate) fn apply_ddl(&self,ddl:&DdlChange)->Result<()> {
        let table=match ddl {
            DdlChange::CreateTable {name,..}|DdlChange::AlterTable {name,..}=>name,
            DdlChange::CreateIndex {table,..}|DdlChange::DropIndex {table,..}=>table,
            DdlChange::DropTable {name}=>return self.project_schema(name,None,std::slice::from_ref(ddl)),
            _=>return Ok(()),
        };
        let meta=self.metadata(table)?;
        self.project_schema(table,Some(meta.value()),std::slice::from_ref(ddl))
    }
}

impl WorkingReadSource {
    pub(crate) fn check(&self)->Result<()> {
        self.context(|context| {
            self.root.check_working_scope("")?;
            for table in context.metadata.table_meta.read().keys() {
                self.root.check_working_scope(table)?;
            }
            Ok(())
        })
    }
    pub(crate) fn advance_frontier(&self,snapshot:SnapshotId,lsn:Lsn)->Result<()> {
        self.context(|context| {
            let previous=context.frontier.get();
            if snapshot.0<previous.0.0 || lsn.0<previous.1.0 {
                return Err(Error::Other("private working frontier moved backward".into()));
            }
            context.frontier.set((snapshot,lsn));
            Ok(())
        })
    }
}

impl WorkingReadSource {
    /// Lend only the immutable original already held by this exact synchronous
    /// source. The result cannot borrow it; no registration or replacement seed
    /// is discovered. Owning constructors keep their body and invoice together.
    pub(crate) fn with_original<R>(&self,
        prepare:impl for<'o> FnOnce(&'o crate::memory_accounting::OwnedMemoryReservation)->Result<R>)->Result<R> {
        self.context(|context|prepare(context.original))
    }
}

struct WorkingGraphCursor {
    source: WorkingReadSource,
    reverse: bool,
    exact: bool,
    lower: Vec<u8>,
    upper: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
    _bounds: ReadCredit,
    endpoint: Option<ReadCredit>,
}
impl contextdb_graph::source::GraphCursor for WorkingGraphCursor {
    fn next_edge(&mut self) -> Result<Option<OwnedReadBox<dyn contextdb_graph::source::GraphEdgeWindow>>> {
        loop {
            let lower = self.after.as_deref().map_or(Bound::Included(self.lower.as_slice()), Bound::Excluded);
            let upper = self.upper.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
            let Some(window) = self.source.read_graph(self.reverse, lower, upper)? else { return Ok(None); };
            let credit = self.source.credit(window.position.len())?;
            let next = window.position.to_vec();
            drop(self.after.replace(next));
            self.endpoint = Some(credit);
            // The on-disk delimiter does not make a string containing NUL a
            // different prefix. Preserve the resident exact-identity test.
            if self.exact && window.value.edge_type.as_bytes() != &self.lower[32..self.lower.len()-1] { continue; }
            return window.boxed(|boxed| boxed as Box<dyn contextdb_graph::source::GraphEdgeWindow>).map(Some);
        }
    }
}
impl WorkingReadSource {
    pub(crate) fn graph_cursor(&self, query: contextdb_graph::source::GraphQuery<'_>)
        -> Result<contextdb_graph::source::GraphCursorOwner> {
        let reverse = match query.direction {
            Direction::Outgoing => false, Direction::Incoming => true,
            Direction::Both => return Err(Error::Other("graph source requires a directional cursor".into())),
        };
        let length = match query.identity {
            Some((_, kind)) => kind.len().checked_add(33).ok_or_else(|| Error::Other("working graph endpoint overflow".into()))?,
            None => 16,
        };
        let bounds = self.credit(length.checked_mul(2).ok_or_else(|| Error::Other("working graph endpoint overflow".into()))?)?;
        let control = self.credit(std::mem::size_of::<WorkingGraphCursor>())?;
        let mut lower = Vec::with_capacity(length);
        lower.extend_from_slice(query.node.as_bytes());
        if let Some((other, kind)) = query.identity {
            lower.extend_from_slice(other.as_bytes());
            lower.extend_from_slice(kind.as_bytes());
            lower.push(0);
        }
        if lower.len() != length || lower.capacity() != length {
            return Err(Error::Other("working graph endpoint capacity differs from admission".into()));
        }
        let upper = writer_source::prefix_end(&lower);
        let payload = Box::new(WorkingGraphCursor { source: self.clone(), reverse, exact: query.identity.is_some(), lower, upper,
            after: None, _bounds: bounds, endpoint: None });
        Ok(contextdb_graph::source::GraphCursorOwner::new(payload, control))
    }

    fn read_graph(&self, reverse: bool, lower: Bound<&[u8]>, upper: Bound<&[u8]>)
        -> Result<Option<writer_source::WriterWindow<AdjEntry>>> {
        self.context(|context| {
            if !context.write.graph_prepared()? {
                return Err(Error::Other("received graph source has no completed private image".into()));
            }
            let memory = crate::read_image_memory::ImageMemory::operation_reader(
                context.original.accountant().clone(), self.budget.clone())?;
            let _read = memory.enter();
            let result = (|| {
                let mut paid = 0usize;
                let (key, encoded) = loop {
                    let copied = context.write.with_write(|write| {
                        let source = write.open_table(if reverse {
                            super::received_working_graph::WORKING_GRAPH_REV
                        } else { super::received_working_graph::WORKING_GRAPH_FWD }).map_err(RedbPersistence::storage_error)?;
                        let mut range = source.range::<&[u8]>((lower, upper)).map_err(RedbPersistence::storage_error)?;
                        let Some((key, body)) = range.next().transpose().map_err(RedbPersistence::storage_error)? else { return Ok(ReadCopy::End); };
                        let needed = key.value().len().checked_add(body.value().len())
                            .ok_or_else(|| Error::Other("working graph window overflow".into()))?;
                        if needed > paid { return Ok(ReadCopy::Need(needed)); }
                        Ok(ReadCopy::Ready((key.value().to_vec(), body.value().to_vec())))
                    })?;
                    match copied {
                        ReadCopy::End => return Ok(None),
                        ReadCopy::Need(needed) => { memory.reserve(needed-paid)?; paid=needed; },
                        ReadCopy::Ready(value) => break value,
                    }
                };
                // Provider calls, decode and output-box admission happen only
                // after the write table/range guards have physically closed.
                self.root.check_working_graph_key(&key, reverse)?;
                let edge = RedbPersistence::decode_exact::<AdjEntry>(&encoded)?;
                if !super::received_working_graph::graph_key_matches(&key, &edge, reverse) {
                    return Err(Error::StoreCorrupted { path: "received working graph".into(),
                        reason: "private edge key and body disagree".into() });
                }
                let mut window = writer_source::WriterWindow::from_owned(&self.root, memory.clone(), edge);
                window.position = key.into_boxed_slice();
                Ok(Some(window))
            })();
            if let Some(error) = memory.take_failure() { drop(result); return Err(error); }
            result
        })
    }
}

/// Existing frontier plus physical private table roots. This is a capture
/// witness, not a new mutable epoch or a token-only approximation.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkingVectorCapture {
    roots: [Option<Option<[u8; 32]>>; 8],
    frontier: (SnapshotId, Lsn),
}
impl WorkingReadSource {
    pub(crate) fn capture_vector_catalog(&self) -> Result<WorkingVectorCapture> {
        use super::received_working_vector::*;
        self.context(|context| {
            if !context.write.vectors_prepared()? { return Err(Error::Other("received vectors have no completed private image".into())); }
            let roots = context.write.with_write(|write| {
                let mut roots = [None; 8];
                for (slot, definition) in roots.iter_mut().zip([WORKING_VECTOR_BODIES,
                    WORKING_VECTOR_MEMBERSHIPS, WORKING_VECTOR_REVERSE, WORKING_VECTOR_SUMMARIES,
                    WORKING_VECTOR_COUNTS, WORKING_VECTOR_SUMMARY_TX, WORKING_VECTOR_EMPTY, WORKING_VECTOR_LINEAGE]) {
                    *slot = write.normal_table_root(definition.name()).map_err(RedbPersistence::storage_error)?;
                    if slot.is_none() { return Err(Error::Other("working vector capture lost a scratch table".into())); }
                }
                Ok(roots)
            })?;
            Ok(WorkingVectorCapture { roots, frontier: context.frontier.get() })
        })
    }
    pub(crate) fn check_vector_capture(&self, captured: &WorkingVectorCapture) -> Result<()> {
        if self.capture_vector_catalog()? != *captured {
            return self.context(|_| Err(Error::Other("working vector selection changed after its capture".into())));
        }
        Ok(())
    }
    pub(crate) fn read_vector_lineage(&self, captured: &WorkingVectorCapture,
        key: &[u8]) -> Result<Option<writer_source::WriterWindow<VectorIndexRef>>> {
        self.check_vector_capture(captured)?;
        let result = self.read_vector_record(super::received_working_vector::WORKING_VECTOR_LINEAGE,
            Bound::Included(key), Bound::Included(key), false, |_key, encoded|
                RedbPersistence::decode::<VectorIndexRef>(encoded))?;
        self.check_vector_capture(captured)?;
        if result.as_ref().is_some_and(|window| window.position.as_ref() != key) {
            return Err(Error::StoreCorrupted { path: "received working vector lineage".into(),
                reason: "lineage point read escaped its key".into() });
        }
        Ok(result)
    }

    pub(crate) fn read_vector_summary(&self, captured: &WorkingVectorCapture,
        lower: Bound<&[u8]>, upper: Bound<&[u8]>)
        -> Result<Option<writer_source::WriterWindow<(VectorPartitionKey, contextdb_vector::store::RawVectorMembershipSummary, bool)>>> {
        self.read_vector_summary_from(captured, super::received_working_vector::WORKING_VECTOR_SUMMARIES, lower, upper)
    }
    pub(crate) fn read_vector_empty_summary(&self, captured: &WorkingVectorCapture,
        lower: Bound<&[u8]>, upper: Bound<&[u8]>)
        -> Result<Option<writer_source::WriterWindow<(VectorPartitionKey, contextdb_vector::store::RawVectorMembershipSummary, bool)>>> {
        self.read_vector_summary_from(captured, super::received_working_vector::WORKING_VECTOR_EMPTY, lower, upper)
    }
    fn read_vector_summary_from(&self, captured: &WorkingVectorCapture, definition: TableDefinition<&[u8], &[u8]>,
        lower: Bound<&[u8]>, upper: Bound<&[u8]>)
        -> Result<Option<writer_source::WriterWindow<(VectorPartitionKey, contextdb_vector::store::RawVectorMembershipSummary, bool)>>> {
        self.check_vector_capture(captured)?;
        let result = self.read_vector_record(definition,
            lower, upper, false, |key, bytes| Ok((super::received_working_vector::decode_summary_partition(key)?,
                super::received_working_vector::summary_decode(bytes)?, bytes[24] != 0)))?;
        self.check_vector_capture(captured)?;
        Ok(result)
    }
    pub(crate) fn refresh_private_vector_catalog(&self) -> Result<()> {
        self.context(|context| context.write.refresh_vector_catalog(context.original))
    }
    pub(crate) fn vector_summary_scalars(&self, captured: &WorkingVectorCapture, key: &[u8])
        -> Result<Option<(contextdb_vector::store::RawVectorMembershipSummary, bool)>> {
        self.check_vector_capture(captured)?;
        let result = self.context(|context| context.write.with_write(|write| {
            let summaries = write.open_table(super::received_working_vector::WORKING_VECTOR_SUMMARIES)
                .map_err(RedbPersistence::storage_error)?;
            summaries.get(key).map_err(RedbPersistence::storage_error)?
                .map(|value| Ok((super::received_working_vector::summary_decode(value.value())?, value.value()[24] != 0))).transpose()
        }))?;
        self.check_vector_capture(captured)?;
        Ok(result)
    }
    pub(crate) fn vector_partition_count(&self, captured: &WorkingVectorCapture, index_prefix: &[u8]) -> Result<usize> {
        Ok(self.vector_partition_counts(captured, index_prefix)?[0])
    }
    pub(crate) fn vector_partition_counts(&self, captured: &WorkingVectorCapture, index_prefix: &[u8]) -> Result<[usize; 6]> {
        self.check_vector_capture(captured)?;
        let count = self.context(|context| context.write.with_write(|write| {
            let counts = write.open_table(super::received_working_vector::WORKING_VECTOR_COUNTS)
                .map_err(RedbPersistence::storage_error)?;
            let count = counts.get(index_prefix).map_err(RedbPersistence::storage_error)?;
            match count {
                None => Ok([0; 6]),
                Some(count) => {
                    let bytes = count.value();
                    if bytes.len() != 48 { return Err(Error::StoreCorrupted {
                        path: "received working vector counts".into(), reason: "invalid partition counts".into() }); }
                    let total = u64::from_be_bytes(bytes[..8].try_into().expect("checked count width"));
                    let live = u64::from_be_bytes(bytes[8..16].try_into().expect("checked count width"));
                    let retained = u64::from_be_bytes(bytes[16..24].try_into().expect("checked count width"));
                    let active = u64::from_be_bytes(bytes[24..32].try_into().expect("checked count width"));
                    let live_vectors = u64::from_be_bytes(bytes[32..40].try_into().expect("checked count width"));
                    let entries = u64::from_be_bytes(bytes[40..].try_into().expect("checked count width"));
                    if live > active || retained > active || active > total || live_vectors > entries { return Err(Error::StoreCorrupted {
                        path: "received working vector counts".into(), reason: "partition counts disagree".into() }); }
                    let mut counts = [0; 6];
                    for (output, value) in counts.iter_mut().zip([total, live, retained, active, live_vectors, entries]) {
                        *output = usize::try_from(value).map_err(|_| Error::Other("working vector partition count overflow".into()))?;
                    }
                    Ok(counts)
                },
            }
        }))?;
        self.check_vector_capture(captured)?;
        Ok(count)
    }
}

impl WorkingReadSource {
    /// Copy one physical working record with this executing reader. Reserve
    /// outside native guards; the original remains the write's custody, never
    /// relabeled into the reader's debit.
    fn read_vector_record<T>(
        &self, definition: TableDefinition<&[u8], &[u8]>,
        lower: Bound<&[u8]>, upper: Bound<&[u8]>, reverse: bool,
        decode: impl FnOnce(&[u8], &[u8]) -> Result<T>,
    ) -> Result<Option<writer_source::WriterWindow<T>>> {
        self.context(|context| {
            if !context.write.vectors_prepared()? {
                return Err(Error::Other("received vectors have no completed private image".into()));
            }
            let memory = crate::read_image_memory::ImageMemory::operation_reader(
                context.original.accountant().clone(), self.budget.clone())?;
            let _read = memory.enter();
            let result = (|| {
                let mut paid = 0usize;
                let (key, encoded) = loop {
                    let copied = context.write.with_write(|write| {
                        let table = write.open_table(definition).map_err(RedbPersistence::storage_error)?;
                        let mut range = table.range::<&[u8]>((lower, upper)).map_err(RedbPersistence::storage_error)?;
                        let Some((key, body)) = (if reverse { range.next_back() } else { range.next() }).transpose().map_err(RedbPersistence::storage_error)? else { return Ok(ReadCopy::End); };
                        let needed = key.value().len().checked_add(body.value().len())
                            .ok_or_else(|| Error::Other("working vector window overflow".into()))?;
                        if needed > paid { return Ok(ReadCopy::Need(needed)); }
                        Ok(ReadCopy::Ready((key.value().to_vec(), body.value().to_vec())))
                    })?;
                    match copied {
                        ReadCopy::End => return Ok(None),
                        ReadCopy::Need(needed) => { memory.reserve(needed-paid)?; paid=needed; },
                        ReadCopy::Ready(value) => break value,
                    }
                };
                let value = decode(&key, &encoded)?;
                let mut window = writer_source::WriterWindow::from_owned(&self.root, memory.clone(), value);
                window.position = key.into_boxed_slice();
                Ok(Some(window))
            })();
            if let Some(error) = memory.take_failure() { drop(result); return Err(error); }
            result
        })
    }

    pub(crate) fn read_vector_membership(
        &self, lower: Bound<&[u8]>, upper: Bound<&[u8]>,
    ) -> Result<Option<writer_source::WriterWindow<VectorPartitionMembershipRecord>>> {
        self.read_vector_record(super::received_working_vector::WORKING_VECTOR_MEMBERSHIPS, lower, upper, false,
            |key, encoded| {
                let member: VectorPartitionMembershipRecord = decode_vector_partition_record_payload_checked(
                    VectorPartitionRecordKind::Membership, encoded).map_err(|failure| failure.into_error(|reason|
                        Error::StoreCorrupted { path: "received working memberships".into(), reason }))?;
                self.root.check_working_scope(&member.index.table)?;
                if !super::received_working_vector::membership_key_matches(key, &member) {
                    return Err(Error::StoreCorrupted { path: "received working memberships".into(), reason: "placement key and body disagree".into() });
                }
                Ok(member)
            })
    }

    pub(crate) fn read_vector_reverse_membership(
        &self, lower: Bound<&[u8]>, upper: Bound<&[u8]>,
    ) -> Result<Option<writer_source::WriterWindow<VectorPartitionMembershipRecord>>> {
        self.read_vector_record(super::received_working_vector::WORKING_VECTOR_REVERSE, lower, upper, true,
            |key, encoded| {
                let member: VectorPartitionMembershipRecord = decode_vector_partition_record_payload_checked(
                    VectorPartitionRecordKind::Membership, encoded).map_err(|failure| failure.into_error(|reason|
                        Error::StoreCorrupted { path: "received working vector reverse".into(), reason }))?;
                self.root.check_working_scope(&member.index.table)?;
                if !super::received_working_vector::reverse_membership_key_matches(key, &member) {
                    return Err(Error::StoreCorrupted { path: "received working vector reverse".into(), reason: "reverse key and placement disagree".into() });
                }
                Ok(member)
            })
    }

    pub(crate) fn read_vector_body(
        &self, member: &VectorPartitionMembershipRecord,
    ) -> Result<writer_source::WriterWindow<VectorEntry>> {
        self.root.check_working_scope(&member.index.table)?;
        let _key_credit = ReadCredit::try_new(self.budget.clone(), member.index.table.len()
            .checked_add(member.index.column.len()).and_then(|n| n.checked_add(34))
            .ok_or_else(|| Error::Other("working vector identity overflow".into()))?,
            "reader", "working_vector_identity", "Raise the declared reader memory limit.")?;
        let key = RedbPersistence::vector_identity_key(&member.index, member.row_id, member.vector_created_tx, member.vector_lsn);
        self.read_vector_record(super::received_working_vector::WORKING_VECTOR_BODIES,
            Bound::Included(key.as_slice()), Bound::Included(key.as_slice()), false, |key, encoded| {
                let body = RedbPersistence::decode_vector_entry(encoded)?;
                if body.index != member.index || body.row_id != member.row_id
                    || body.created_tx != member.vector_created_tx || body.lsn != member.vector_lsn
                    || !super::received_working_vector::body_key_matches(key, &body) {
                    return Err(Error::StoreCorrupted { path: "received working vectors".into(), reason: "placement and canonical body disagree".into() });
                }
                Ok(body)
            })?.ok_or_else(|| Error::StoreCorrupted { path: "received working vectors".into(), reason: "placement lost its canonical body".into() })
    }
}

/// Retains the same scoped source, never a resident directory of graph types.
/// Escaping the source loan makes every operation fail through context().
pub(crate) struct WorkingGraphSelection {
    source: WorkingReadSource,
    captured: super::received_working_graph::WorkingGraphCapture,
}
impl WorkingGraphSelection {
    pub(crate) fn contains_original(&self, original: &crate::memory_accounting::OwnedMemoryReservation, kind: &str) -> Result<bool> {
        self.source.context(|context| {
            context.write.require_original(original)?;
            context.write.check_graph_capture(context.original, &self.captured)?;
            let found = context.write.graph_type_contains_original(context.original, kind)?;
            context.write.check_graph_capture(context.original, &self.captured)?;
            Ok(found)
        })
    }
    pub(crate) fn visit_original(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        mut visit: impl FnMut(&str) -> Result<()>) -> Result<()> {
        self.source.context(|context| {
            context.write.require_original(original)?;
            context.write.check_graph_capture(context.original, &self.captured)?;
            context.write.visit_graph_types_original(context.original, |kind| {
                visit(kind)?;
                context.write.check_graph_capture(context.original, &self.captured)
            })?;
            context.write.check_graph_capture(context.original, &self.captured)
        })
    }
}
impl WorkingReadSource {
    pub(crate) fn select_private_graph_type(&self, kind: &str) -> Result<()> {
        self.context(|context| context.write.select_graph_type(context.original, kind))
    }
    pub(crate) fn allocate_private_graph_occurrence(&self, origin: super::received_working_graph::WorkingGraphOrigin)
        -> Result<super::received_working_graph::WorkingGraphOccurrence> {
        self.context(|context| context.write.allocate_graph_occurrence(context.original, origin))
    }
    pub(crate) fn record_private_graph_coordinate(&self, edge: &AdjEntry, deletion: bool,
        coordinate: super::received_working_graph::WorkingGraphCoordinate) -> Result<()> {
        self.context(|context| context.write.record_graph_coordinate(context.original, edge, deletion, coordinate))
    }
    pub(crate) fn private_graph_coordinate(&self, edge: &AdjEntry, deletion: bool)
        -> Result<Option<super::received_working_graph::WorkingGraphCoordinate>> {
        self.context(|context| context.write.graph_coordinate_original(context.original, edge, deletion))
    }
    pub(crate) fn capture_private_graph(&self) -> Result<super::received_working_graph::WorkingGraphCapture> {
        self.context(|context| context.write.capture_graph(context.original))
    }
    pub(crate) fn check_private_graph_capture(&self, captured: &super::received_working_graph::WorkingGraphCapture) -> Result<()> {
        self.context(|context| context.write.check_graph_capture(context.original, captured))
    }
    /// Call only after restamp and complete validation. Terminal consumers must
    /// keep this source loan live; no graph publication is activated here.
    pub(crate) fn freeze_private_graph_selection(&self) -> Result<WorkingGraphSelection> {
        self.context(|context| Ok(WorkingGraphSelection { source: self.clone(),
            captured: context.write.freeze_graph_selection(context.original)? }))
    }
}
impl WorkingReadSource {
    pub(crate) fn visit_private_graph_coordinates(&self, deletion: bool,
        visit: impl FnMut(&[u8], super::received_working_graph::WorkingGraphCoordinate) -> Result<()>) -> Result<()> {
        self.context(|context| context.write.visit_graph_coordinates_original(context.original, deletion, visit))
    }
}

impl WorkingReadSource {
    pub(crate) fn begin_private_graph_capture(&self) -> Result<()> {
        self.context(|context| context.write.begin_graph_capture(context.original))
    }
    pub(crate) fn retain_private_graph_mutation(&self,
        origin: super::received_working_graph::WorkingGraphOrigin,
        source: NodeId, kind: &str, target: NodeId, after: Option<&AdjEntry>, deleted: TxId) -> Result<()> {
        self.context(|context| context.write.retain_graph_mutation(context.original, origin, source, kind, target, after, deleted))
    }
    pub(crate) fn pending_private_graph_coordinate(&self, edge: &AdjEntry)
        -> Result<Option<super::received_working_graph::WorkingGraphCoordinate>> {
        self.context(|context| context.write.pending_graph_coordinate(context.original, edge))
    }
    pub(crate) fn private_graph_capturing(&self) -> Result<bool> {
        self.context(|context| context.write.graph_capturing(context.original))
    }
    pub(crate) fn restamp_private_graph_occurrences(&self, receiver: TxId,
        position: impl FnMut(super::received_working_graph::WorkingGraphOrigin) -> Result<Lsn>) -> Result<()> {
        self.context(|context| context.write.restamp_graph_occurrences(context.original, receiver, position))
    }
    pub(crate) fn checkpoint_private_graph(&self) -> Result<super::received_working_graph::WorkingGraphCheckpoint> {
        self.context(|context| context.write.checkpoint_graph(context.original))
    }
    pub(crate) fn settle_private_graph_checkpoint(&self,
        checkpoint: super::received_working_graph::WorkingGraphCheckpoint, restore: bool) -> Result<()> {
        self.context(|context| context.write.settle_graph_checkpoint(context.original, checkpoint, restore))
    }
}

impl WorkingReadSource {
    pub(crate) fn retire_private_graph_occurrences(&self, source: NodeId, kind: &str, target: NodeId) -> Result<()> {
        self.context(|context| context.write.retire_graph_occurrences(context.original, source, kind, target))
    }
}

pub(crate) struct OriginalGraphDeclarations {
    pub(crate) types: Vec<String>,
    pub(crate) owner: Option<crate::memory_accounting::OwnedMemoryReservation>,
}
impl WorkingReadSource {
    pub(crate) fn current_private_graph_declarations(&self) -> Result<OriginalGraphDeclarations> {
        self.context(|context| {
            let frame = OriginalPersistenceFrame::new(context.original)?;
            let (count, bytes) = {
                let current = context.metadata.table_meta.read();
                context.source.root.captured_table_metadata()
                    .filter(|(table, _)| !context.graph_schema_scope.contains(*table))
                    .map(|(_, meta)| meta).chain(current.values())
                    .flat_map(|meta| meta.dag_edge_types.iter())
                    .fold((0usize, 0usize), |(count, bytes), kind|
                        (count.saturating_add(1), bytes.saturating_add(kind.len()).saturating_add(256)))
            };
            frame.reserve(bytes.saturating_mul(2).saturating_add(256))?;
            let mut types = Vec::with_capacity(count);
            for (_, meta) in context.source.root.captured_table_metadata()
                .filter(|(table, _)| !context.graph_schema_scope.contains(*table)) {
                types.extend(meta.dag_edge_types.iter().cloned());
            }
            {
                let current = context.metadata.table_meta.read();
                for meta in current.values() { types.extend(meta.dag_edge_types.iter().cloned()); }
            }
            if types.len() != count { return Err(Error::Other("private graph declarations changed during preparation".into())); }
            Ok(OriginalGraphDeclarations { types, owner: Some(frame.into_reservation()) })
        })
    }
}

impl WorkingReadSource {
    pub(crate) fn last_private_graph_coordinate(&self, source: NodeId, kind: &str, target: NodeId)
        -> Result<Option<super::received_working_graph::WorkingGraphCoordinate>> {
        self.context(|context| context.write.last_graph_coordinate_original(context.original, source, kind, target))
    }
}

impl WorkingReadSource {
    pub(crate) fn replace_private_graph_declarations_original(&self,
        owner: &mut Option<crate::memory_accounting::OwnedMemoryReservation>)
        -> Result<Option<crate::memory_accounting::OwnedMemoryReservation>> {
        self.context(|context| context.write.replace_graph_declarations_original(context.original, owner))
    }
}

use crate::persistence::writer_source::{WriterSnapshotHandle};
