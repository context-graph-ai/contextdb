//! Custody of one received unit's unpublished public write. Dropping it aborts
//! every private phase; only the existing terminal flush may commit it.
use super::*;

// Private, process-local control identities never wrap or recycle. They are
// neither durable epochs nor a replacement for authentic Original admission.
static NEXT_WORKING_CONTROL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
pub(super) fn next_working_control_identity() -> Result<u64> {
    NEXT_WORKING_CONTROL.fetch_update(std::sync::atomic::Ordering::Relaxed,
        std::sync::atomic::Ordering::Relaxed, |next| next.checked_add(1))
        .map_err(|_| Error::Other("received working control identity exhausted".into()))
}

pub(crate) struct ReceivedWorkingWrite {
    // Physical transaction and its pages die before the exact original lease.
    write: Option<redb::WriteTransaction>,
    pub(super) control_identity: u64,
    original_address: usize,
    store_address: usize,
    scratch: parking_lot::Mutex<Vec<ScratchName>>,
    lent_databases: parking_lot::Mutex<Vec<crate::database::DatabaseId>>,
    schemas:parking_lot::Mutex<HashMap<String,WorkingSchema>>,
    graph_ready: std::sync::atomic::AtomicBool,
    pub(super) graph_state: parking_lot::Mutex<super::received_working_graph::WorkingGraphState>,
    vector_ready: std::sync::atomic::AtomicBool,
    vector_catalog: parking_lot::Mutex<Option<super::received_working_vector::WorkingVectorCatalogOwner>>,
    owner: parking_lot::Mutex<crate::memory_accounting::OwnedMemoryReservation>,
    original_owner: Option<crate::database::event_bus::OwnedEventBusHandle>,
}
impl ReceivedWorkingWrite {
    pub(crate) fn begin(store: &RedbPersistence,
        original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<Self> {
        let storage_scope = super::native_storage_admission::StorageAdmissionScope::writer_original(original)?;
        storage_scope.run_original_unqualified(|| store.with_db(|db|Self::begin_in(store,db,original)))
    }
    pub(crate) fn begin_funded(store: &RedbPersistence,
        original: &crate::memory_accounting::OwnedMemoryReservation,
    ) -> crate::funded_error::OriginalResult<Self> {
        let scope = super::native_storage_admission::StorageAdmissionScope::writer_original(original)?;
        scope.run_original(crate::funded_error::OriginalStoragePhase::ReceivedOuter, || store.with_db_original_funded(original, |db|
            Self::begin_in_funded(store, db, original)))
    }
    pub(super) fn begin_in(store: &RedbPersistence, db: &redb::Database,
        original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<Self> {
        Self::begin_in_route(store, original, || {
            let scope = super::native_storage_admission::StorageAdmissionScope::writer_original(original)?;
            scope.run_original_unqualified(|| RedbPersistence::begin_history_write(db))
        })
    }
    pub(super) fn begin_in_funded(store: &RedbPersistence, db: &redb::Database,
        original: &crate::memory_accounting::OwnedMemoryReservation,
    ) -> crate::funded_error::OriginalResult<Self> {
        Self::begin_in_route(store, original, || {
            let scope = super::native_storage_admission::StorageAdmissionScope::writer_original(original)?;
            scope.run_original(crate::funded_error::OriginalStoragePhase::ReceivedBegin, || RedbPersistence::begin_history_write_funded(original, db))
        })
    }
    fn begin_in_route<E: From<Error>>(store:&RedbPersistence,
        original:&crate::memory_accounting::OwnedMemoryReservation,
        write: impl FnOnce() -> std::result::Result<redb::WriteTransaction, E>,
    ) -> std::result::Result<Self, E> {
        let owner=crate::memory_accounting::reserve_preparation_from_original(original,
            std::mem::size_of::<Self>(),"persistence","received_working_write",
            "Raise MEMORY_LIMIT to prepare this received unit.")?;
        let control_identity = next_working_control_identity()?;
        let write = write()?;
        Ok(Self {write:Some(write),control_identity,original_address:original as *const _ as usize,
            store_address:store as *const _ as usize,scratch:parking_lot::Mutex::new(Vec::new()),
            lent_databases:parking_lot::Mutex::new(Vec::new()),schemas:parking_lot::Mutex::new(HashMap::new()),graph_state:parking_lot::Mutex::new(super::received_working_graph::WorkingGraphState::new()),graph_ready:std::sync::atomic::AtomicBool::new(false),vector_ready:std::sync::atomic::AtomicBool::new(false),vector_catalog:parking_lot::Mutex::new(None),owner:parking_lot::Mutex::new(owner),original_owner:None})
    }
    pub(crate) fn retain_original_owner(
        &mut self, events: &crate::database::event_bus::OwnedEventBusHandle,
    ) -> Result<()> {
        events.with_owned_persistence_original(|original| self.require_original(original))?;
        if let Some(existing) = &self.original_owner {
            existing.with_owned_persistence_original(|original| self.require_original(original))?;
        } else {
            self.original_owner = Some(events.clone());
        }
        Ok(())
    }

    fn transaction(&self)->&redb::WriteTransaction {
        self.write.as_ref().expect("live unpublished received write")
    }
    pub(super) fn claim_database_loan(&self,database:crate::database::DatabaseId)->Result<()> {
        let mut lent=self.lent_databases.lock();
        if lent.contains(&database) {
            return Err(Error::Other("this private database already consumed its working-source loan".into()));
        }
        if lent.len()==lent.capacity() {
            let capacity=lent.capacity().checked_mul(2).map(|n|n.max(4))
                .ok_or_else(||Error::Other("working source loan directory overflow".into()))?;
            let bytes=std::alloc::Layout::array::<crate::database::DatabaseId>(capacity)
                .map_err(|_|Error::Other("working source loan directory overflow".into()))?.size();
            crate::memory_accounting::grow_preparation_from_original(&mut self.owner.lock(),bytes,
                "persistence","received_working_loan_directory","Raise MEMORY_LIMIT to prepare this received unit.")?;
            let additional=capacity-lent.len();lent.reserve_exact(additional);
        }
        lent.push(database);
        Ok(())
    }
    pub(super) fn with_write<R>(&self,
        f: impl FnOnce(&redb::WriteTransaction) -> Result<R>) -> Result<R> {
        f(self.transaction())
    }
    pub(crate) fn require_original(&self,
        original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<()> {
        if self.original_address != original as *const _ as usize {
            return Err(Error::Other("received working write requires its authentic original loan".into()));
        }
        Ok(())
    }
    pub(crate) fn install_vector_catalog(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        catalog: super::received_working_vector::WorkingVectorCatalogOwner) -> Result<()> {
        self.require_original(original)?;
        let mut slot = self.vector_catalog.lock();
        if slot.is_some() { return Err(Error::Other("working vector catalog is already installed".into())); }
        *slot = Some(catalog);
        Ok(())
    }
    pub(crate) fn refresh_vector_catalog(&self, original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<()> {
        self.require_original(original)?;
        let catalog = self.vector_catalog.lock().clone();
        if let Some(catalog) = catalog { catalog.refresh(self, original)?; }
        Ok(())
    }
    pub(super) fn vector_is_ready(&self) -> bool {
        self.vector_ready.load(std::sync::atomic::Ordering::Acquire)
    }
    pub(super) fn vector_became_unready(&self) {
        self.vector_ready.store(false, std::sync::atomic::Ordering::Release);
    }
    pub(super) fn vector_became_ready(&self) {
        self.vector_ready.store(true, std::sync::atomic::Ordering::Release);
    }
    pub(super) fn graph_is_ready(&self) -> bool {
        self.graph_ready.load(std::sync::atomic::Ordering::Acquire)
    }
    pub(super) fn graph_became_unready(&self) {
        self.graph_ready.store(false, std::sync::atomic::Ordering::Release);
    }
    pub(super) fn graph_became_ready(&self) {
        self.graph_ready.store(true, std::sync::atomic::Ordering::Release);
    }

    pub(super) fn require_store(&self, store: &RedbPersistence) -> Result<()> {
        if self.store_address != store as *const _ as usize {
            return Err(Error::Other("received working write belongs to another persistence owner".into()));
        }
        Ok(())
    }

    pub(super) fn commit(mut self)->std::result::Result<(),redb::CommitError> {
        // Keep the complete carrier alive across consuming commit. A failed
        // commit destroys its transaction before this carrier's invoice.
        self.write.take().expect("one received terminal commit").commit()
    }
}

/// One mutable-source answer. No table/range guard crosses into the callback;
/// its original decode invoice follows the body until physical destruction.
pub(crate) struct WorkingRow {
    row: VersionedRow,
    key: [u8;24],
    _owner: crate::memory_accounting::OwnedMemoryReservation,
}
impl ReceivedWorkingWrite {
    pub(crate) fn next_row(
        &self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        table: &str,
        lower: std::ops::Bound<&[u8]>,
        upper: std::ops::Bound<&[u8]>,
        reverse: bool,
    ) -> Result<Option<WorkingRow>> {
        self.require_original(original)?;
        let frame = OriginalPersistenceFrame::new(original)?;
        frame.reserve(table.len().saturating_add(64))?;
        let name = working_row_name(table);
        let definition = TableDefinition::<&[u8],&[u8]>::new(name.as_str());
        if !self.transaction().table_exists(definition).map_err(RedbPersistence::storage_error)? {
            return Ok(None);
        }
        let source = self.transaction().open_table(definition).map_err(RedbPersistence::storage_error)?;
        let mut range = source.range::<&[u8]>((lower,upper)).map_err(RedbPersistence::storage_error)?;
        let next = if reverse { range.next_back() } else { range.next() };
        let Some((physical_key,body)) = next.transpose().map_err(RedbPersistence::storage_error)? else {
            return Ok(None);
        };
        let key: [u8;24] = physical_key.value().try_into().map_err(|_| Error::StoreCorrupted {
            path: table.to_owned(), reason: "private working row has an invalid physical key".into(),
        })?;
        let row = RedbPersistence::decode_versioned_row_original(&frame,body.value(),None)?;
        if u64::from_be_bytes(key[..8].try_into().unwrap()) != row.row_id.0
            || u64::from_be_bytes(key[8..16].try_into().unwrap()) != row.created_tx.0
            || u64::from_be_bytes(key[16..].try_into().unwrap()) != row.lsn.0 {
            return Err(Error::StoreCorrupted { path: table.to_owned(),
                reason: "private working row key and body disagree".into() });
        }
        drop(body);
        drop(physical_key);
        drop(range);
        drop(source);
        Ok(Some(WorkingRow { row,key,_owner:frame.into_reservation() }))
    }
}

pub(crate) struct WorkingPosting {
    posting: contextdb_relational::source::RelationalPosting,
    key: Vec<u8>,
    _owner: crate::memory_accounting::OwnedMemoryReservation,
}
fn index_bound(frame:&OriginalPersistenceFrame<'_>,bound:std::ops::Bound<&[u8]>)
    ->Result<std::ops::Bound<DurableRelationalIndexKey>> {
    match bound {
        std::ops::Bound::Included(key)=>Ok(std::ops::Bound::Included(DurableRelationalIndexKey(frame.copy_bytes(key)?))),
        std::ops::Bound::Excluded(key)=>Ok(std::ops::Bound::Excluded(DurableRelationalIndexKey(frame.copy_bytes(key)?))),
        std::ops::Bound::Unbounded=>Ok(std::ops::Bound::Unbounded),
    }
}
impl ReceivedWorkingWrite {
    pub(crate) fn next_posting(
        &self,original:&crate::memory_accounting::OwnedMemoryReservation,
        table:&str,index:&str,current:bool,
        lower:std::ops::Bound<&[u8]>,upper:std::ops::Bound<&[u8]>,reverse:bool,
    )->Result<Option<WorkingPosting>> {
        self.require_original(original)?;
        let frame=OriginalPersistenceFrame::new(original)?;
        let image=crate::memory_accounting::image_memory_for_original_record(&frame)?;
        let _image_scope=image.enter();
        frame.reserve(table.len().saturating_add(index.len()).saturating_add(128))?;
        let name=working_index_name(table,index,current);
        let definition=TableDefinition::<DurableRelationalIndexKey,&[u8]>::new(name.as_str());
        if !self.transaction().table_exists(definition).map_err(RedbPersistence::storage_error)? {
            return Err(Error::IndexNotFound { table:table.to_owned(),index:index.to_owned() });
        }
        let lower=index_bound(&frame,lower)?;
        let upper=index_bound(&frame,upper)?;
        let source=self.transaction().open_table(definition).map_err(RedbPersistence::storage_error)?;
        let mut range=source.range::<&DurableRelationalIndexKey>((lower.as_ref(),upper.as_ref())).map_err(RedbPersistence::storage_error)?;
        let decoded={
        let entry=if reverse { range.next_back() }else{ range.next() };
        let entry=entry.transpose().map_err(RedbPersistence::storage_error)?;
        if current {
            if let Some((key,value))=entry {
                frame.reserve(key.retained_page_bytes())?;
                let key=key.value();
                let encoded:EncodedRelationalIndexPosition=frame.decode(&key.0)?;
                let _json_scratch=frame.json_scratch(encoded.key.iter().map(|(_,value)|value))?;
                if frame.encode(&encoded)?!=key.0 || encoded.created_tx!=TxId(0) {
                    return Err(Error::StoreCorrupted {path:table.to_owned(),reason:"noncanonical working current index key".into()});
                }
                frame.array::<DirectedValue>(encoded.key.len())?;
                let mut position:RelationalIndexPosition=encoded.into();
                position.created_tx=frame.decode::<TxId>(value.value())?;
                Some((key.0,position,None))
            } else {None}
        } else { decode_relational_index_entry_original(&frame,Path::new("received_working"),entry)? }
        };
        drop(range);drop(source);
        let Some((key,position,deleted_tx))=decoded else { return Ok(None); };
        let posting=contextdb_relational::source::RelationalPosting { key:position.key,
            entry:contextdb_relational::store::IndexEntry { row_id:position.row_id,
                created_tx:position.created_tx,deleted_tx } };
        Ok(Some(WorkingPosting { posting,key,_owner:frame.into_reservation() }))
    }
    pub(crate) fn sync_source(&self,original:&crate::memory_accounting::OwnedMemoryReservation,
        table:&str,row:RowId)->Result<Option<(Lsn,contextdb_relational::store::SyncSourceKind)>> {
        use contextdb_relational::store::SyncSourceKind;
        self.require_original(original)?;
        let frame=OriginalPersistenceFrame::new(original)?;
        frame.reserve(table.len().saturating_mul(2).saturating_add(128))?;
        let private_name=format!("__received_working_sources_{table}");
        let definition=TableDefinition::<&[u8],&[u8]>::new(private_name.as_str());
        if self.transaction().table_exists(definition).map_err(RedbPersistence::storage_error)? {
            let source=self.transaction().open_table(definition).map_err(RedbPersistence::storage_error)?;
            let key=row.0.to_be_bytes();
            if let Some(value)=source.get(key.as_slice()).map_err(RedbPersistence::storage_error)? {
                let raw=value.value();
                return match raw {
                    [0]=>Ok(None),
                    [1,rest @ ..] if rest.len()==9=>{
                        let lsn=Lsn(u64::from_be_bytes(rest[..8].try_into().unwrap()));
                        let kind=match rest[8] {0=>SyncSourceKind::Pulled,1=>SyncSourceKind::AcceptedLocal,
                            2=>SyncSourceKind::AcceptedLocalPending,
                            _=>return Err(Error::StoreCorrupted {path:table.to_owned(),reason:"invalid private provenance kind".into()})};
                        Ok(Some((lsn,kind)))
                    },
                    _=>Err(Error::StoreCorrupted {path:table.to_owned(),reason:"invalid private provenance record".into()}),
                };
            }
            if source.get(&[] as &[u8]).map_err(RedbPersistence::storage_error)?.is_some() {
                return Ok(None);
            }
        }
        let key=RedbPersistence::sync_row_source_lsn_key(table,row);
        let lsn=if self.transaction().table_exists(SYNC_ROW_SOURCE_LSN_TABLE).map_err(RedbPersistence::storage_error)? {
            self.transaction().open_table(SYNC_ROW_SOURCE_LSN_TABLE).map_err(RedbPersistence::storage_error)?
                .get(key.as_slice()).map_err(RedbPersistence::storage_error)?.map(|value|Lsn(value.value()))
        }else{None};
        let kind=if self.transaction().table_exists(SYNC_ROW_SOURCE_KIND_TABLE).map_err(RedbPersistence::storage_error)? {
            self.transaction().open_table(SYNC_ROW_SOURCE_KIND_TABLE).map_err(RedbPersistence::storage_error)?
                .get(key.as_slice()).map_err(RedbPersistence::storage_error)?.map(|value|value.value())
        }else{None};
        match (lsn,kind) {
            (None,None)=>Ok(None),
            (Some(lsn),Some(kind))=>Ok(Some((lsn,match kind {0=>SyncSourceKind::Pulled,
                1=>SyncSourceKind::AcceptedLocal,2=>SyncSourceKind::AcceptedLocalPending,
                _=>return Err(Error::StoreCorrupted {path:table.to_owned(),reason:"invalid working source provenance kind".into()}) }))),
            _=>Err(Error::StoreCorrupted {path:table.to_owned(),reason:"incomplete working source provenance".into()}),
        }
    }
}

struct ScratchName {
    name:String,
    index:bool,
    _owner:crate::memory_accounting::OwnedMemoryReservation,
}
pub(super) fn working_row_name(table:&str)->String {format!("__received_working_rows_{table}")}
struct ManifestWindow {
    key:Vec<u8>,
    _owner:content_identity::original_writer::ProofAdmission,
}
impl ReceivedWorkingWrite {
    pub(super) fn keep_scratch_name(&self,name:String,
        owner:crate::memory_accounting::OwnedMemoryReservation)->Result<()> {
        self.keep_scratch_name_kind(name,owner,false)
    }
    fn keep_scratch_name_kind(&self,name:String,
        owner:crate::memory_accounting::OwnedMemoryReservation,index:bool)->Result<()> {
        // Pair before the first guard/early return/fallible growth, including
        // duplicate-name cleanup. The physical String dies before its debit.
        let entry = ScratchName { name, index, _owner: owner };
        let mut names=self.scratch.lock();
        if names.iter().any(|prior|prior.name==entry.name) {return Ok(());}
        if names.len()==names.capacity() {
            let capacity=names.capacity().checked_mul(2).map(|n|n.max(4))
                .ok_or_else(||Error::Other("working schema directory overflow".into()))?;
            let bytes=std::alloc::Layout::array::<ScratchName>(capacity)
                .map_err(|_|Error::Other("working schema directory overflow".into()))?.size();
            crate::memory_accounting::grow_preparation_from_original(&mut self.owner.lock(),bytes,
                "persistence","received_working_directory","Raise MEMORY_LIMIT to prepare this received unit.")?;
            let additional=capacity-names.len();
            names.reserve_exact(additional);
        }
        names.push(entry);
        Ok(())
    }
    /// The public table stays untouched. The same transaction owns a private
    /// copy on disk, authenticated against the outgoing version manifest.
    /// Each proof window is destroyed only after its carried key, including
    /// the successor retained across the next seek.
    pub(crate) fn prepare_rows(&self,store:&RedbPersistence,
        original:&crate::memory_accounting::OwnedMemoryReservation,table:&str)->Result<()> {
        use content_identity::{original_version_manifest as manifest,original_writer::ProofAdmission};
        self.require_original(original)?;
        if self.store_address!=store as *const _ as usize {
            return Err(Error::Other("received working write belongs to another persistence owner".into()));
        }
        let frame=OriginalPersistenceFrame::new(original)?;
        frame.reserve(table.len().saturating_mul(3).saturating_add(256))?;
        let name=working_row_name(table);
        let destination_def=TableDefinition::<&[u8],&[u8]>::new(name.as_str());
        if self.transaction().table_exists(destination_def).map_err(RedbPersistence::storage_error)? {return Ok(());}
        let public_name=RedbPersistence::rel_table_name(table);
        let public_def=TableDefinition::<&[u8],&[u8]>::new(public_name.as_str());
        let prefix=manifest::manifest_prefix(table)?;
        if !self.transaction().table_exists(public_def).map_err(RedbPersistence::storage_error)? {
            let mut admission=ProofAdmission::new(original,store.proof_scratch_owner())?;
            let next=manifest::writer_manifest_next(self.transaction(),&prefix,false,&mut admission)?;
            if next.as_ref().is_some_and(|key|key.starts_with(&prefix)) {
                return Err(Error::StoreCorrupted {path:table.to_owned(),reason:"working source has rooted rows but no physical table".into()});
            }
            self.transaction().open_table(destination_def).map_err(RedbPersistence::storage_error)?;
            return self.keep_scratch_name(name,frame.into_reservation());
        }
        {
            let source=self.transaction().open_table(public_def).map_err(RedbPersistence::storage_error)?;
            let mut rows=source.iter().map_err(RedbPersistence::storage_error)?;
            let mut destination=self.transaction().open_table(destination_def).map_err(RedbPersistence::storage_error)?;
            let mut after=None::<ManifestWindow>;
            loop {
                let mut admission=ProofAdmission::new(original,store.proof_scratch_owner())?;
                let next=manifest::writer_manifest_next(self.transaction(),
                    after.as_ref().map_or(prefix.as_slice(),|window|window.key.as_slice()),
                    after.is_some(),&mut admission)?;
                let Some(coordinate)=next.filter(|key|key.starts_with(&prefix)) else {
                    if rows.next().transpose().map_err(RedbPersistence::storage_error)?.is_some() {
                        return Err(Error::StoreCorrupted {path:table.to_owned(),reason:"working source has an unrooted physical suffix".into()});
                    }
                    break;
                };
                let Some((key,body))=rows.next().transpose().map_err(RedbPersistence::storage_error)? else {
                    return Err(Error::StoreCorrupted {path:table.to_owned(),reason:"working source lost a rooted physical row".into()});
                };
                let expected=manifest::manifest_for_body(table,key.value(),body.value(),&mut admission)?;
                if coordinate!=expected {
                    return Err(Error::StoreCorrupted {path:table.to_owned(),reason:"working source physical order/body disagrees with its rooted row".into()});
                }
                destination.insert(key.value(),body.value()).map_err(RedbPersistence::storage_error)?;
                drop(expected);drop(body);drop(key);
                after=Some(ManifestWindow {key:coordinate,_owner:admission});
            }
        }
        self.keep_scratch_name(name,frame.into_reservation())
    }
    /// All source cursors/loans end before terminal apply calls this. Scratch
    /// relations are never committed or visible to published readers.
    pub(crate) fn reset_scratch(&self)->Result<()> {
        self.discard_scratch()?;
        self.scratch.lock().clear();
        self.schemas.lock().clear();
        self.vector_catalog.lock().take();
        Ok(())
    }
    pub(super) fn discard_scratch(&self)->Result<()> {
        self.vector_ready.store(false, std::sync::atomic::Ordering::Release);
        self.graph_ready.store(false, std::sync::atomic::Ordering::Release);
        self.graph_state.lock().reset()?;
        let names=self.scratch.lock();
        for entry in names.iter() {
            if entry.index {
                self.transaction().delete_table(TableDefinition::<DurableRelationalIndexKey,&[u8]>::new(entry.name.as_str()))
                    .map_err(RedbPersistence::storage_error)?;
            }else{
                self.transaction().delete_table(TableDefinition::<&[u8],&[u8]>::new(entry.name.as_str()))
                    .map_err(RedbPersistence::storage_error)?;
            }
        }
        Ok(())
    }
}

struct WorkingIndexHead {
    key:IndexKey,row:RowId,created:TxId,end:Option<TxId>,
    _owner:crate::memory_accounting::OwnedMemoryReservation,
}
pub(super) fn working_index_name(table:&str,index:&str,current:bool)->String {
    let public=if current {RedbPersistence::current_relational_index_table_name(table,index)}
        else {RedbPersistence::relational_index_table_name(table,index)};
    format!("__received_working_{public}")
}
impl ReceivedWorkingWrite {
    fn finish_index_head(&self,original:&crate::memory_accounting::OwnedMemoryReservation,
        name:&str,head:WorkingIndexHead)->Result<()> {
        if head.end.is_some() {return Ok(());}
        let frame=OriginalPersistenceFrame::new(original)?;
        frame.reserve(head.key.iter().map(|part|match part {
            DirectedValue::Asc(TotalOrdAsc(value))|DirectedValue::Desc(TotalOrdDesc(value))=>value.estimated_bytes(),
        }).sum::<usize>().saturating_mul(8).saturating_add(1024))?;
        let _json_scratch=frame.json_scratch(head.key.iter().map(|part|match part {
            DirectedValue::Asc(TotalOrdAsc(value))|DirectedValue::Desc(TotalOrdDesc(value))=>value,
        }))?;
        let key=DurableRelationalIndexKey(frame.encode(&EncodedRelationalIndexPosition::from(
            RelationalIndexPosition {key:head.key.clone(),row_id:head.row,created_tx:TxId(0)}))?);
        let value=frame.encode(&head.created)?;
        self.transaction().open_table(TableDefinition::<DurableRelationalIndexKey,&[u8]>::new(name))
            .map_err(RedbPersistence::storage_error)?.insert(&key,value.as_slice()).map_err(RedbPersistence::storage_error)?;
        Ok(())
    }
    /// Private indexes are derived only from authenticated working rows. They
    /// never replace or publish the outgoing public index/commitment records.
    pub(crate) fn prepare_index(&self,store:&RedbPersistence,
        original:&crate::memory_accounting::OwnedMemoryReservation,
        table:&str,declaration:&IndexDecl)->Result<()> {
        self.prepare_rows(store,original,table)?;
        let frame=OriginalPersistenceFrame::new(original)?;
        frame.reserve(table.len().saturating_add(declaration.name.len()).saturating_mul(4).saturating_add(256))?;
        let historical_name=working_index_name(table,&declaration.name,false);
        let current_name=working_index_name(table,&declaration.name,true);
        let historical_def=TableDefinition::<DurableRelationalIndexKey,&[u8]>::new(historical_name.as_str());
        let current_def=TableDefinition::<DurableRelationalIndexKey,&[u8]>::new(current_name.as_str());
        if self.transaction().table_exists(historical_def).map_err(RedbPersistence::storage_error)? {return Ok(());}
        drop(self.transaction().open_table(historical_def).map_err(RedbPersistence::storage_error)?);
        drop(self.transaction().open_table(current_def).map_err(RedbPersistence::storage_error)?);
        let mut after=None::<[u8;24]>;
        let mut head=None::<WorkingIndexHead>;
        loop {
            let row=self.next_row(original,table,after.as_ref().map_or(std::ops::Bound::Unbounded,
                |key|std::ops::Bound::Excluded(key.as_slice())),std::ops::Bound::Unbounded,false)?;
            let Some(row)=row else {break;};
            if head.as_ref().is_some_and(|head|head.row!=row.row.row_id) {
                self.finish_index_head(original,&current_name,head.take().unwrap())?;
            }
            let window=OriginalPersistenceFrame::new(original)?;
            window.reserve(row.row.estimated_bytes().saturating_mul(8).saturating_add(1024))?;
            let _json_scratch=window.json_scratch(row.row.values.values())?;
            let key=contextdb_relational::index_key_for_row(&declaration.columns,&row.row.values);
            let created=head.as_ref().filter(|prior|prior.row==row.row.row_id
                &&prior.key==key&&prior.end==Some(row.row.created_tx)).map_or(row.row.created_tx,|prior|prior.created);
            let encoded=DurableRelationalIndexKey(window.encode(&EncodedRelationalIndexPosition::from(
                RelationalIndexPosition {key:key.clone(),row_id:row.row.row_id,created_tx:created}))?);
            let end=window.encode(&row.row.deleted_tx)?;
            self.transaction().open_table(historical_def).map_err(RedbPersistence::storage_error)?
                .insert(&encoded,end.as_slice()).map_err(RedbPersistence::storage_error)?;
            after=Some(row.key);
            head=Some(WorkingIndexHead {key,row:row.row.row_id,created,end:row.row.deleted_tx,
                _owner:window.into_reservation()});
        }
        if let Some(head)=head {self.finish_index_head(original,&current_name,head)?;}
        // Names have their OWN transferred source invoice, ahead of future
        // cleanup. The directory distinguishes index keys from row keys.
        self.keep_index_scratch_name(original,historical_name)?;
        self.keep_index_scratch_name(original,current_name)?;
        Ok(())
    }
    fn keep_index_scratch_name(&self,original:&crate::memory_accounting::OwnedMemoryReservation,
        name:String)->Result<()> {
        // The enclosing preparation frame admitted construction. Transfer the
        // name by a move into a separately admitted standing schema record.
        let owner=crate::memory_accounting::reserve_preparation_from_original(original,name.capacity(),
            "persistence","received_working_index_name","Raise MEMORY_LIMIT to prepare this received unit.")?;
        self.keep_scratch_name_kind(name,owner,true)
    }
}

impl std::ops::Deref for ReceivedWorkingWrite {
    type Target=redb::WriteTransaction;
    fn deref(&self)->&Self::Target {self.transaction()}
}
impl Drop for ReceivedWorkingWrite {fn drop(&mut self) {}}
impl WorkingRow {
    pub(crate) fn value(&self)->&VersionedRow {&self.row}
    pub(crate) fn position(&self)->&[u8;24] {&self.key}
}
impl Drop for WorkingRow {fn drop(&mut self) {}}
impl WorkingPosting {
    pub(crate) fn value(&self)->&contextdb_relational::source::RelationalPosting {&self.posting}
    pub(crate) fn position(&self)->&[u8] {&self.key}
}
impl Drop for WorkingPosting {fn drop(&mut self) {}}

impl ReceivedWorkingWrite {
    pub(super) fn take_or_begin(store:&RedbPersistence,db:&redb::Database,
        original:&crate::memory_accounting::OwnedMemoryReservation,
        stage:&ReceivedSchemaPersistenceProjection)->Result<Self> {
        Self::take_or_begin_in(store, db, original, stage, Self::begin_in)
    }
    pub(super) fn take_or_begin_funded(store:&RedbPersistence,db:&redb::Database,
        original:&crate::memory_accounting::OwnedMemoryReservation,
        stage:&ReceivedSchemaPersistenceProjection)->crate::funded_error::OriginalResult<Self> {
        Self::take_or_begin_in(store, db, original, stage, Self::begin_in_funded)
    }
    pub(super) fn take_or_begin_in<E: From<Error>>(store:&RedbPersistence,db:&redb::Database,
        original:&crate::memory_accounting::OwnedMemoryReservation,
        stage:&ReceivedSchemaPersistenceProjection,
        begin: impl FnOnce(&RedbPersistence, &redb::Database,
            &crate::memory_accounting::OwnedMemoryReservation) -> std::result::Result<Self, E>,
    )->std::result::Result<Self,E> {
        let prepared={
            let mut slot=stage.working_write.lock();
            if let Some(write)=slot.as_ref() {write.require_original(original)?;}
            slot.take()
        };
        if let Some(write)=prepared {
            // The terminal borrows the same source through graph projection
            // and fingerprints. It discards every private table after those
            // final consumers, before applying identities and committing.
            Ok(write)
        }else{begin(store,db,original)}
    }
}

impl ReceivedWorkingWrite {
    /// Final projection walks the private physical order without retaining a
    /// table-sized row directory. next_row closes all Redb guards before the
    /// consumer runs; its body invoice remains alive throughout the call.
    pub(crate) fn visit_rows(&self,
        original:&crate::memory_accounting::OwnedMemoryReservation,
        table:&str,mut visit:impl FnMut(&VersionedRow)->Result<()>) -> Result<()> {
        self.require_original(original)?;
        let mut after:Option<[u8;24]>=None;
        loop {
            let lower=after.as_ref().map_or(std::ops::Bound::Unbounded,
                |key|std::ops::Bound::Excluded(key.as_slice()));
            let Some(row)=self.next_row(original,table,lower,std::ops::Bound::Unbounded,false)? else {break;};
            visit(row.value())?;
            after=Some(*row.position());
        }
        Ok(())
    }
    pub(crate) fn spool_rows(&self,
        original:&crate::memory_accounting::OwnedMemoryReservation,
        table:&str,records:&mut super::schema_projection::ReceivedRowRecords)->Result<()> {
        records.push_table_with(table,|push|self.visit_rows(original,table,push))
    }
}

impl ReceivedWorkingWrite {
    fn invalidate_indexes(&self)->Result<()> {
        let mut names=self.scratch.lock();
        for entry in names.iter().filter(|entry|entry.index) {
            self.transaction().delete_table(TableDefinition::<DurableRelationalIndexKey,&[u8]>::new(entry.name.as_str()))
                .map_err(RedbPersistence::storage_error)?;
        }
        // The relation disappears before its retained name invoice. Row
        // relations survive, so subsequent queries rebuild against new rows.
        names.retain(|entry|!entry.index);
        Ok(())
    }
    /// Apply the existing adjudicated replacement sequence, never wire rows
    /// inferred to be accepted. The caller supplies final receiver identities.
    pub(crate) fn apply_row_replacements(&self,store:&RedbPersistence,
        original:&crate::memory_accounting::OwnedMemoryReservation,
        metadata:&HashMap<String,TableMeta>,
        deletes:&[(String,RowId,TxId)],inserts:&[(String,VersionedRow)]) -> Result<()> {
        self.require_original(original)?;
        if deletes.is_empty() && inserts.is_empty() {return Ok(());}
        self.invalidate_indexes()?;
        for (table,row_id,deleted_tx) in deletes {
            self.prepare_rows(store,original,table)?;
            let mut lower=[0u8;24];lower[..8].copy_from_slice(&row_id.0.to_be_bytes());
            let mut upper=[u8::MAX;24];upper[..8].copy_from_slice(&row_id.0.to_be_bytes());
            let Some(row)=self.next_row(original,table,std::ops::Bound::Included(&lower),
                std::ops::Bound::Included(&upper),true)? else {continue;};
            if row.value().deleted_tx.is_some() {continue;}
            let frame=OriginalPersistenceFrame::new(original)?;
            frame.reserve(table.len().saturating_add(64))?;
            let mut encoded=Vec::new();
            RedbPersistence::encode_versioned_row_with_deleted_tx_into_original(&frame,row.value(),
                Some(*deleted_tx),metadata.get(table),&mut encoded)?;
            let name=working_row_name(table);
            let mut destination=self.transaction().open_table(TableDefinition::<&[u8],&[u8]>::new(name.as_str()))
                .map_err(RedbPersistence::storage_error)?;
            destination.insert(row.position().as_slice(),encoded.as_slice())
                .map_err(RedbPersistence::storage_error)?;
        }
        for (table,row) in inserts {
            self.prepare_rows(store,original,table)?;
            let frame=OriginalPersistenceFrame::new(original)?;
            frame.reserve(table.len().saturating_add(64))?;
            let encoded=RedbPersistence::encode_versioned_row_original(&frame,row,metadata.get(table))?;
            let mut key=[0u8;24];key[..8].copy_from_slice(&row.row_id.0.to_be_bytes());
            key[8..16].copy_from_slice(&row.created_tx.0.to_be_bytes());
            key[16..].copy_from_slice(&row.lsn.0.to_be_bytes());
            let name=working_row_name(table);
            let mut destination=self.transaction().open_table(TableDefinition::<&[u8],&[u8]>::new(name.as_str()))
                .map_err(RedbPersistence::storage_error)?;
            destination.insert(key.as_slice(),encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
        }
        Ok(())
    }
}

impl ReceivedWorkingWrite {
    fn prepare_source_overlay(&self,
        original:&crate::memory_accounting::OwnedMemoryReservation,table:&str)->Result<()> {
        self.require_original(original)?;
        let frame=OriginalPersistenceFrame::new(original)?;
        frame.reserve(table.len().saturating_add(64))?;
        let name=format!("__received_working_sources_{table}");
        let definition=TableDefinition::<&[u8],&[u8]>::new(name.as_str());
        if self.transaction().table_exists(definition).map_err(RedbPersistence::storage_error)? {return Ok(());}
        drop(self.transaction().open_table(definition).map_err(RedbPersistence::storage_error)?);
        self.keep_scratch_name(name,frame.into_reservation())
    }
    pub(crate) fn mark_sync_source(&self,
        original:&crate::memory_accounting::OwnedMemoryReservation,table:&str,row:RowId,
        value:Option<(Lsn,contextdb_relational::store::SyncSourceKind)>)->Result<()> {
        self.prepare_source_overlay(original,table)?;
        let frame=OriginalPersistenceFrame::new(original)?;
        frame.reserve(table.len().saturating_add(64))?;
        let name=format!("__received_working_sources_{table}");
        let mut destination=self.transaction().open_table(TableDefinition::<&[u8],&[u8]>::new(name.as_str()))
            .map_err(RedbPersistence::storage_error)?;
        let key=row.0.to_be_bytes();
        let mut encoded=[0u8;10];
        let raw=if let Some((lsn,kind))=value {
            encoded[0]=1;encoded[1..9].copy_from_slice(&lsn.0.to_be_bytes());
            encoded[9]=match kind {contextdb_relational::store::SyncSourceKind::Pulled=>0,
                contextdb_relational::store::SyncSourceKind::AcceptedLocal=>1,
                contextdb_relational::store::SyncSourceKind::AcceptedLocalPending=>2};
            encoded.as_slice()
        }else{&encoded[..1]};
        destination.insert(key.as_slice(),raw).map_err(RedbPersistence::storage_error)?;
        Ok(())
    }
    /// DROP followed by CREATE must preserve an empty private incarnation.
    /// Keeping the owned scratch name prevents lazy initialization from
    /// copying the old public table or its provenance back into the new one.
    pub(crate) fn clear_rows(&self,store:&RedbPersistence,
        original:&crate::memory_accounting::OwnedMemoryReservation,table:&str)->Result<()> {
        self.prepare_rows(store,original,table)?;
        self.invalidate_indexes()?;
        self.prepare_source_overlay(original,table)?;
        let frame=OriginalPersistenceFrame::new(original)?;
        frame.reserve(table.len().saturating_mul(2).saturating_add(128))?;
        let row_name=working_row_name(table);
        let definition=TableDefinition::<&[u8],&[u8]>::new(row_name.as_str());
        self.transaction().delete_table(definition).map_err(RedbPersistence::storage_error)?;
        drop(self.transaction().open_table(definition).map_err(RedbPersistence::storage_error)?);
        let source_name=format!("__received_working_sources_{table}");
        let definition=TableDefinition::<&[u8],&[u8]>::new(source_name.as_str());
        self.transaction().delete_table(definition).map_err(RedbPersistence::storage_error)?;
        let mut source=self.transaction().open_table(definition).map_err(RedbPersistence::storage_error)?;
        source.insert(&[] as &[u8],&[0u8] as &[u8]).map_err(RedbPersistence::storage_error)?;
        Ok(())
    }
}

impl ReceivedWorkingWrite {
    /// Rewrite the retained private projection with the same rename/drop and
    /// vector-lifetime rules as the existing schema stream. Public authored
    /// history remains untouched for its separate terminal custody pass.
    pub(crate) fn project_rows(&self,store:&RedbPersistence,
        original:&crate::memory_accounting::OwnedMemoryReservation,table:&str,
        previous:&TableMeta,arriving:&TableMeta,ddl:&[DdlChange])->Result<()> {
        self.prepare_rows(store,original,table)?;
        self.invalidate_indexes()?;
        let declaration=OriginalPersistenceFrame::new(original)?;
        declaration.reserve(previous.estimated_bytes().saturating_add(arriving.estimated_bytes())
            .saturating_add(ddl.iter().map(DdlChange::estimated_bytes).fold(0usize,usize::saturating_add))
            .saturating_mul(8).saturating_add(table.len()).saturating_add(4096))?;
        let renames=ddl.iter().filter_map(|change|match change {
            DdlChange::AlterTable {name,constraints,..} if name==table=>Some(constraints),_=>None,
        }).flat_map(|constraints|crate::database::sync_column_renames_from_constraints(constraints)
            .into_iter().chain(crate::database::sync_vector_rename_from_constraints(constraints))).collect::<Vec<_>>();
        let mut after:Option<[u8;24]>=None;
        loop {
            let lower=after.as_ref().map_or(std::ops::Bound::Unbounded,
                |key|std::ops::Bound::Excluded(key.as_slice()));
            let Some(source)=self.next_row(original,table,lower,std::ops::Bound::Unbounded,false)? else {break;};
            let window=OriginalPersistenceFrame::new(original)?;
            window.reserve(crate::memory_accounting::preparation_row_copy_bytes(source.value())?
                .saturating_add(arriving.estimated_bytes().saturating_mul(8)).saturating_add(4096))?;
            let mut row=crate::memory_accounting::preparation_copy_row(source.value())?;
            for (from,to) in &renames {
                if let Some(value)=row.values.remove(from) {row.values.insert(to.clone(),value);}
            }
            row.values.retain(|column,_|arriving.columns.iter().any(|declared|declared.name==*column));
            for old in &previous.columns {
                if matches!(old.column_type,ColumnType::Vector(_))
                    && !renames.iter().any(|(from,_)|from==&old.name)
                    && arriving.columns.iter().find(|new|new.name==old.name).is_some_and(|new|
                        old.column_type!=new.column_type || old.quantization!=new.quantization) {
                    row.values.insert(old.name.clone(),Value::Null);
                }
            }
            let encoded=RedbPersistence::encode_versioned_row_original(&window,&row,Some(arriving))?;
            let name=working_row_name(table);
            let mut destination=self.transaction().open_table(TableDefinition::<&[u8],&[u8]>::new(name.as_str()))
                .map_err(RedbPersistence::storage_error)?;
            destination.insert(source.position().as_slice(),encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
            after=Some(*source.position());
        }
        Ok(())
    }
}

impl ReceivedWorkingWrite {
    /// Install the adjudicator's complete final table into private scratch.
    /// This transitional caller accepts borrowed resident rows; the paged
    /// adjudicator uses apply_row_replacements instead of building this slice.
    pub(crate) fn replace_projected_rows(&self,store:&RedbPersistence,
        original:&crate::memory_accounting::OwnedMemoryReservation,table:&str,
        meta:&TableMeta,rows:&[VersionedRow])->Result<()> {
        self.prepare_rows(store,original,table)?;
        self.invalidate_indexes()?;
        let declaration=OriginalPersistenceFrame::new(original)?;
        declaration.reserve(table.len().saturating_add(64))?;
        let name=working_row_name(table);
        let definition=TableDefinition::<&[u8],&[u8]>::new(name.as_str());
        self.transaction().delete_table(definition).map_err(RedbPersistence::storage_error)?;
        drop(self.transaction().open_table(definition).map_err(RedbPersistence::storage_error)?);
        let mut previous=None;
        for row in rows {
            let mut key=[0u8;24];key[..8].copy_from_slice(&row.row_id.0.to_be_bytes());
            key[8..16].copy_from_slice(&row.created_tx.0.to_be_bytes());
            key[16..].copy_from_slice(&row.lsn.0.to_be_bytes());
            if previous.is_some_and(|old|old>=key) {
                return Err(Error::StoreCorrupted {path:table.to_owned(),
                    reason:"private projection contains repeated or unordered row identities".into()});
            }
            let window=OriginalPersistenceFrame::new(original)?;
            let encoded=RedbPersistence::encode_versioned_row_original(&window,row,Some(meta))?;
            let mut destination=self.transaction().open_table(definition).map_err(RedbPersistence::storage_error)?;
            destination.insert(key.as_slice(),encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
            previous=Some(key);
        }
        Ok(())
    }
}

struct WorkingSchema {
    meta:TableMeta,
    _owner:crate::memory_accounting::OwnedMemoryReservation,
}
impl WorkingSchema {
    fn copy(original:&crate::memory_accounting::OwnedMemoryReservation,
        table:&str,meta:&TableMeta)->Result<Self> {
        let frame=OriginalPersistenceFrame::new(original)?;
        frame.reserve(meta.estimated_bytes().saturating_mul(8).saturating_add(table.len())
            .saturating_add(std::mem::size_of::<Self>()).saturating_add(1024))?;
        Ok(Self {meta:meta.clone(),_owner:frame.into_reservation()})
    }
}
impl ReceivedWorkingWrite {
    fn admit_schema_slot(&self,schemas:&mut HashMap<String,WorkingSchema>)->Result<()> {
        if schemas.len()==schemas.capacity() {
            let requested=schemas.capacity().checked_mul(2).map(|value|value.max(4))
                .ok_or_else(||Error::Other("working schema directory overflow".into()))?;
            let bytes=std::alloc::Layout::array::<(String,WorkingSchema)>(requested)
                .map_err(|_|Error::Other("working schema directory overflow".into()))?.size()
                .checked_mul(4).ok_or_else(||Error::Other("working schema directory overflow".into()))?;
            crate::memory_accounting::grow_preparation_from_original(&mut self.owner.lock(),bytes,
                "persistence","received_working_schema_directory","Raise MEMORY_LIMIT to prepare this received unit.")?;
            schemas.reserve(requested-schemas.len());
        }
        Ok(())
    }
    pub(super) fn vector_declaration_at(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        position: usize) -> Result<Option<super::OriginalDecoded<VectorIndexRef>>> {
        self.require_original(original)?;
        let length = {
            let schemas = self.schemas.lock();
            schemas.iter().flat_map(|(table, schema)| schema.meta.columns.iter()
                .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
                .map(move |column| (table, column)))
                .nth(position).map(|(table, column)| table.len().checked_add(column.name.len())
                    .ok_or_else(|| Error::Other("working vector declaration key overflow".into()))).transpose()?
        };
        let Some(length) = length else { return Ok(None); };
        let frame = OriginalPersistenceFrame::new(original)?;
        frame.reserve(length)?;
        let value = {
            let schemas = self.schemas.lock();
            let (table, column) = schemas.iter().flat_map(|(table, schema)| schema.meta.columns.iter()
                .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
                .map(move |column| (table, column))).nth(position)
                .ok_or_else(|| Error::Other("working vector declaration changed during preparation".into()))?;
            if table.len().checked_add(column.name.len()) != Some(length) {
                return Err(Error::Other("working vector declaration changed during preparation".into()));
            }
            VectorIndexRef { table: crate::memory_accounting::preparation_copy_string(table)?,
                column: crate::memory_accounting::preparation_copy_string(&column.name)? }
        };
        Ok(Some(super::OriginalDecoded { value, allocation: frame.into_reservation() }))
    }

    pub(crate) fn remember_schema(&self,
        original:&crate::memory_accounting::OwnedMemoryReservation,table:&str,meta:&TableMeta)->Result<()> {
        self.require_original(original)?;
        if self.schemas.lock().contains_key(table) {return Ok(());}
        let prepared=WorkingSchema::copy(original,table,meta)?;
        let mut schemas=self.schemas.lock();
        self.admit_schema_slot(&mut schemas)?;
        schemas.insert(table.to_owned(),prepared);
        Ok(())
    }
    pub(crate) fn project_schema(&self,store:&RedbPersistence,
        original:&crate::memory_accounting::OwnedMemoryReservation,table:&str,
        arriving:Option<&TableMeta>,ddl:&[DdlChange])->Result<()> {
        self.require_original(original)?;
        let prepared=arriving.map(|meta|WorkingSchema::copy(original,table,meta)).transpose()?;
        let mut schemas=self.schemas.lock();
        if let Some(prepared)=prepared {
            if let Some(previous)=schemas.get(table) {
                if previous.meta.columns!=prepared.meta.columns {
                    self.project_rows(store,original,table,&previous.meta,&prepared.meta,ddl)?;
                    self.project_vectors_schema(original,table,Some(&previous.meta),Some(&prepared.meta),ddl)?;
                }
            }else{
                // A prior DROP keeps its empty relation, while a genuinely
                // new table initializes from the authenticated absent source.
                self.prepare_rows(store,original,table)?;
                self.project_vectors_schema(original,table,None,Some(&prepared.meta),ddl)?;
            }
            self.invalidate_indexes()?;
            if !schemas.contains_key(table) {self.admit_schema_slot(&mut schemas)?;}
            schemas.insert(table.to_owned(),prepared);
        }else{
            self.project_vectors_schema(original,table,schemas.get(table).map(|schema| &schema.meta),None,ddl)?;
            self.clear_rows(store,original,table)?;
            schemas.remove(table);
        }
        Ok(())
    }
}

impl ReceivedWorkingWrite {
    pub(crate) fn apply_provenance(&self,
        original:&crate::memory_accounting::OwnedMemoryReservation,ws:&contextdb_tx::WriteSet)->Result<()> {
        self.require_original(original)?;
        crate::composite_store::visit_sync_source_lsn_updates(ws,
            |table,row|self.mark_sync_source(original,table,row,None),
            |table,row,lsn,kind| {
                use contextdb_relational::store::SyncSourceKind;
                let kind=match kind {0=>SyncSourceKind::Pulled,1=>SyncSourceKind::AcceptedLocal,
                    2=>SyncSourceKind::AcceptedLocalPending,
                    _=>return Err(Error::StoreCorrupted {path:table.to_owned(),reason:"invalid adjudicated provenance kind".into()})};
                self.mark_sync_source(original,table,row,Some((lsn,kind)))
            })
    }
}

impl ReceivedWorkingWrite {
    pub(crate) fn latest_row(&self,
        original:&crate::memory_accounting::OwnedMemoryReservation,table:&str,row:RowId)->Result<Option<WorkingRow>> {
        let mut lower=[0u8;24];lower[..8].copy_from_slice(&row.0.to_be_bytes());
        let mut upper=[u8::MAX;24];upper[..8].copy_from_slice(&row.0.to_be_bytes());
        self.next_row(original,table,std::ops::Bound::Included(&lower),std::ops::Bound::Included(&upper),true)
    }
    pub(crate) fn exact_version(&self,
        original:&crate::memory_accounting::OwnedMemoryReservation,table:&str,
        row:RowId,created:TxId,lsn:Lsn)->Result<Option<WorkingRow>> {
        let mut key=[0u8;24];key[..8].copy_from_slice(&row.0.to_be_bytes());
        key[8..16].copy_from_slice(&created.0.to_be_bytes());key[16..].copy_from_slice(&lsn.0.to_be_bytes());
        self.next_row(original,table,std::ops::Bound::Included(&key),std::ops::Bound::Included(&key),false)
    }
}

pub(crate) struct WorkingRowIdentity<'a> {
    pub(crate) row_id: RowId,
    pub(crate) created_tx: &'a mut TxId,
    pub(crate) deleted_tx: &'a mut Option<TxId>,
    pub(crate) lsn: &'a mut Lsn,
    pub(crate) created_at: &'a mut Option<contextdb_core::Wallclock>,
}
impl ReceivedWorkingWrite {
    /// Replace private-coordinate keys with receiver-coordinate keys before
    /// final projection. Unchanged rows retain their exact physical bytes.
    pub(crate) fn rewrite_row_identities(
        &self, store: &RedbPersistence,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        table: &str, meta: Option<&TableMeta>,
        mut stamp: impl FnMut(&mut WorkingRowIdentity<'_>) -> Result<()>,
    ) -> Result<()> {
        self.require_original(original)?;
        self.prepare_rows(store, original, table)?;
        self.invalidate_indexes()?;
        let names = OriginalPersistenceFrame::new(original)?;
        names.reserve(table.len().saturating_mul(3).saturating_add(256))?;
        let row_name = working_row_name(table);
        let temporary = format!("__received_working_restamp_{table}");
        let tracked = crate::memory_accounting::preparation_copy_string(&temporary)?;
        self.keep_scratch_name(tracked, names.into_reservation())?;
        let source = TableDefinition::<&[u8], &[u8]>::new(row_name.as_str());
        let destination = TableDefinition::<&[u8], &[u8]>::new(temporary.as_str());
        self.with_write(|write| {
            match write.delete_table(destination) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {},
                Err(error) => return Err(RedbPersistence::storage_error(error)),
            }
            write.open_table(destination).map(|_| ()).map_err(RedbPersistence::storage_error)
        })?;
        let mut after = None::<[u8; 24]>;
        loop {
            let frame = OriginalPersistenceFrame::new(original)?;
            let lower = after.as_ref().map_or(std::ops::Bound::Unbounded, |key| std::ops::Bound::Excluded(key.as_slice()));
            let Some(mut window) = self.next_row(original, table, lower, std::ops::Bound::Unbounded, false)? else { break; };
            after = Some(*window.position());
            let before = (window.row.created_tx, window.row.deleted_tx, window.row.lsn, window.row.created_at);
            let VersionedRow { row_id, created_tx, deleted_tx, lsn, created_at, .. } = &mut window.row;
            stamp(&mut WorkingRowIdentity { row_id: *row_id, created_tx, deleted_tx, lsn, created_at })?;
            let changed = before != (window.row.created_tx, window.row.deleted_tx, window.row.lsn, window.row.created_at);
            let encoded = if changed {
                RedbPersistence::encode_versioned_row_original(&frame, &window.row, meta)?
            } else {
                self.with_write(|write| {
                    let rows = write.open_table(source).map_err(RedbPersistence::storage_error)?;
                    let body = rows.get(window.position().as_slice()).map_err(RedbPersistence::storage_error)?
                        .ok_or_else(|| Error::Other("working row disappeared during coordinate projection".into()))?;
                    frame.copy_bytes(body.value())
                })?
            };
            let key = RedbPersistence::rel_row_key(&window.row);
            self.with_write(|write| {
                write.open_table(destination).map_err(RedbPersistence::storage_error)?
                    .insert(key.as_slice(), encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
                Ok(())
            })?;
        }
        // No old key is removed until the complete replacement exists.
        self.with_write(|write| {
            write.delete_table(source).map_err(RedbPersistence::storage_error)?;
            write.open_table(source).map_err(RedbPersistence::storage_error)?;
            Ok(())
        })?;
        after = None;
        loop {
            let frame = OriginalPersistenceFrame::new(original)?;
            let copied = self.with_write(|write| {
                let rows = write.open_table(destination).map_err(RedbPersistence::storage_error)?;
                let lower = after.as_ref().map_or(std::ops::Bound::Unbounded, |key| std::ops::Bound::Excluded(key.as_slice()));
                let mut range = rows.range::<&[u8]>((lower, std::ops::Bound::Unbounded)).map_err(RedbPersistence::storage_error)?;
                let Some((key, body)) = range.next().transpose().map_err(RedbPersistence::storage_error)? else { return Ok(None); };
                let key: [u8; 24] = key.value().try_into().map_err(|_| Error::Other("restamped row key has invalid width".into()))?;
                Ok(Some((key, frame.copy_bytes(body.value())?)))
            })?;
            let Some((key, body)) = copied else { break; };
            self.with_write(|write| {
                write.open_table(source).map_err(RedbPersistence::storage_error)?
                    .insert(key.as_slice(), body.as_slice()).map_err(RedbPersistence::storage_error)?;
                Ok(())
            })?;
            after = Some(key);
        }
        Ok(())
    }
}
