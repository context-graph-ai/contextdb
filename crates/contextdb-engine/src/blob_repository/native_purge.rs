//! Native witnesses stay in the exact erasure write. Each family is visited
//! one record at a time, including many tags/partials/staging objects per hash.
use super::*;
use redb::ReadableTableMetadata;
#[path = "native_metadata.rs"]
mod metadata;
use crate::persistence::{OriginalPersistenceFrame,discard_history::OriginalDiscardHistory};
use crate::memory_accounting::OwnedMemoryReservation;

pub(crate) const WITNESSES:TableDefinition<&[u8],&[u8]>=TableDefinition::new("__discard_blob_witnesses");
pub(crate) const NEXT:TableDefinition<&[u8],&[u8]>=TableDefinition::new("__discard_blob_next");
pub(crate) const OBJECTS:TableDefinition<&[u8],u8>=TableDefinition::new("__discard_blob_objects");
pub(crate) const CONTROL:TableDefinition<u64,u64>=TableDefinition::new("__discard_blob_control");
const FAMILIES:[TableDefinition<&[u8],&[u8]>;8]=[
    HASH_GENERATIONS_TABLE,MANIFESTS_TABLE,PARTIAL_BITFIELDS_TABLE,TAGS_BY_NAME_TABLE,
    TAGS_BY_HASH_TABLE,IMPORT_STAGING_TABLE,PAYLOAD_CHUNKS_TABLE,OUTBOARD_CHUNKS_TABLE,
];
fn invalid() -> Error {Error::Other("blob durable erasure witness disagrees with its owner".into())}
fn window(original:&OwnedMemoryReservation) -> Result<OriginalPersistenceFrame<'_>> {
    OriginalPersistenceFrame::new(original)
}
fn get(write:&redb::WriteTransaction,original:&OwnedMemoryReservation,family:usize,key:&[u8]) -> Result<Option<metadata::Bytes>> {
    metadata::get(write,original,FAMILIES[family],key)
}
// A tag's format and role belong to that tag, not to its manifest. Both
// writers preserve these fields independently. Require the opposite index to
// witness the complete typed owner (including its actual format/role) and the
// exact bytes that apply will recheck before removing either index.
fn require_tag_counterpart(write:&redb::WriteTransaction,original:&OwnedMemoryReservation,
    family:usize,key:&[u8],encoded:&[u8],tag:&BlobTagRecord) -> Result<()> {
    let bytes=get(write,original,family,key)?.ok_or_else(invalid)?;
    let frame=window(original)?;
    let counterpart:BlobTagRecord=frame.decode_blob(&bytes)?;
    if &counterpart!=tag || &*bytes!=encoded {return Err(invalid());}
    Ok(())
}
// Whole record/key widths are NOT an accepted-input limit. Every owned copy
// has a real Original invoice; raw witnesses use fixed-width value chunks.
pub(crate) const BYTES:TableDefinition<&[u8],&[u8]>=TableDefinition::new("__discard_blob_witness_bytes");
const WITNESS_CHUNK:usize=4096;
fn chunk_key(record:u64,offset:usize)->Result<[u8;16]> {
    let mut key=[0u8;16];key[..8].copy_from_slice(&record.to_be_bytes());
    key[8..].copy_from_slice(&u64::try_from(offset).map_err(|_|invalid())?.to_be_bytes());Ok(key)
}
fn header(value:&[u8])->Result<(u64,usize)> {
    if value.len()!=16 {return Err(invalid());}
    let ordinal=u64::from_be_bytes(value[..8].try_into().map_err(|_|invalid())?);
    let len=usize::try_from(u64::from_be_bytes(value[8..].try_into().map_err(|_|invalid())?)).map_err(|_|invalid())?;
    Ok((ordinal,len))
}
fn witness(write:&redb::WriteTransaction,original:&OwnedMemoryReservation,family:usize,key:&[u8],value:&[u8]) -> Result<()> {
    witness_stream(write,original,family,key,value.len(),|range,consume|consume(&value[range]))
}
fn witness_source(write:&redb::WriteTransaction,original:&OwnedMemoryReservation,family:usize,source:&metadata::Source<'_, '_>)->Result<()> {
    source.with_key(|key|witness_stream(write,original,family,key,source.value_len(),|range,consume|source.value_window(range,consume)))
}
fn witness_stream(write:&redb::WriteTransaction,original:&OwnedMemoryReservation,family:usize,key:&[u8],len:usize,
    mut chunk:impl FnMut(std::ops::Range<usize>,&mut dyn FnMut(&[u8])->Result<()>)->Result<()>) -> Result<()> {
    let frame=window(original)?;
    frame.reserve(key.len().checked_add(1).ok_or_else(invalid)?)?;
    let mut address=Vec::with_capacity(key.len().checked_add(1).ok_or_else(invalid)?);
    address.push(family as u8);address.extend_from_slice(key);
    let mut witnesses=write.open_table(WITNESSES).map_err(RedbPersistence::storage_error)?;
    if let Some(previous)=witnesses.get(address.as_slice()).map_err(RedbPersistence::storage_error)? {
        let (ordinal,expected_len)=header(previous.value())?;
        if expected_len!=len {return Err(invalid());}
        let backing=write.open_table(BYTES).map_err(RedbPersistence::storage_error)?;
        for offset in (0..len).step_by(WITNESS_CHUNK) {
            let key=chunk_key(ordinal,offset)?;
            let expected=backing.get(key.as_slice()).map_err(RedbPersistence::storage_error)?.ok_or_else(invalid)?;
            chunk(offset..(offset+WITNESS_CHUNK).min(len),&mut |bytes| {
                if bytes!=expected.value() {return Err(invalid());} Ok(())
            })?;
        }
        return Ok(());
    }
    let ordinal=witnesses.len().map_err(RedbPersistence::storage_error)?;
    let mut descriptor=[0u8;16];descriptor[..8].copy_from_slice(&ordinal.to_be_bytes());
    descriptor[8..].copy_from_slice(&u64::try_from(len).map_err(|_|invalid())?.to_be_bytes());
    {
        let mut backing=write.open_table(BYTES).map_err(RedbPersistence::storage_error)?;
        for offset in (0..len).step_by(WITNESS_CHUNK) {
            let key=chunk_key(ordinal,offset)?;
            chunk(offset..(offset+WITNESS_CHUNK).min(len),&mut |bytes| {
                if backing.insert(key.as_slice(),bytes).map_err(RedbPersistence::storage_error)?.is_some() {return Err(invalid());} Ok(())
            })?;
        }
    }
    witnesses.insert(address.as_slice(),descriptor.as_slice()).map_err(RedbPersistence::storage_error)?;
    Ok(())
}
fn visit(write:&redb::WriteTransaction,original:&OwnedMemoryReservation,family:usize,
    consume:impl FnMut(&metadata::Source<'_, '_>)->Result<()>) -> Result<()> {
    metadata::visit(write,original,FAMILIES[family],consume)
}
fn object(write:&redb::WriteTransaction,id:&[u8;16]) -> Result<()> {
    write.open_table(OBJECTS).map_err(RedbPersistence::storage_error)?.insert(id.as_slice(),0).map_err(RedbPersistence::storage_error)?;Ok(())
}

impl BlobRepository {
    pub(crate) fn prepare_original_authoritative_purge(&self,original:&OwnedMemoryReservation,
        history:&OriginalDiscardHistory,guard:&BlobExclusiveSelectorGuard,
        selected:&FrozenBlobSelector,survivors:&FrozenBlobSelector,frontier:Lsn) -> Result<BlobAuthoritativePurgeProjection> {
        guard.require(self)?;let store=self.store()?;
        guard.selector().require(store,original)?;
        selected.require(store,original)?;survivors.require(store,original)?;
        if !selected.is_subset_of(guard.selector())? {return Err(blob_fence_invalid());}
        let (write,control)=history.blob_write(original)?;
        RedbPersistence::with_original_blob_pages(original,|| {
        for definition in [WITNESSES,NEXT,BYTES] {
            if !write.open_table(definition).map_err(RedbPersistence::storage_error)?.is_empty().map_err(RedbPersistence::storage_error)? {return Err(invalid());}
        }
        if !write.open_table(OBJECTS).map_err(RedbPersistence::storage_error)?.is_empty().map_err(RedbPersistence::storage_error)? {return Err(invalid());}
        if !write.open_table(CONTROL).map_err(RedbPersistence::storage_error)?.is_empty().map_err(RedbPersistence::storage_error)? {return Err(invalid());}
        write.open_table(CONTROL).map_err(RedbPersistence::storage_error)?.insert(0,control).map_err(RedbPersistence::storage_error)?;
        for ordinal in 0..selected.len() {
            let hash=selected.at(ordinal)?;
            if survivors.contains(&hash)? {continue;}
            let _window=window(original)?;
            let expected=get(write,original,0,&hash)?;
            let mut generation=expected.as_deref().map(|bytes|_window.decode_blob::<BlobHashGenerationState>(bytes)).transpose()?.unwrap_or_default();
            if generation.version!=RECORD_VERSION {return Err(invalid());}
            witness(write,original,0,&hash,expected.as_deref().unwrap_or(&[]))?;
            if let Some(active)=generation.active {
                let manifest_key=BlobRepository::manifest_key(&hash,active.generation);
                let bytes=get(write,original,1,&manifest_key)?.ok_or_else(invalid)?;
                let manifest:BlobManifest=_window.decode_blob(&bytes)?;
                if manifest.version!=RECORD_VERSION || manifest.object_id!=active.object_id {return Err(invalid());}
                let (payload,outboard)=match manifest.state {
                    BlobManifestState::Complete=>{
                        validate_complete_block_count(manifest.total_size,manifest.payload_chunk_count,"payload")?;
                        validate_complete_block_count(manifest.outboard_size,manifest.outboard_chunk_count,"outboard")?;
                        (BlobChunkSetWitness::Dense(manifest.payload_chunk_count),BlobChunkSetWitness::Dense(manifest.outboard_chunk_count))
                    }
                    BlobManifestState::Partial=>{
                        let bytes=get(write,original,2,&manifest_key)?.ok_or_else(invalid)?;
                        let partial_frame=window(original)?;
                        let partial:BlobPartialState=partial_frame.decode_blob(&bytes)?;
                        if partial.version!=RECORD_VERSION {return Err(invalid());}
                        validate_partial_chunks(write,original,&manifest,&partial)?;
                        witness(write,original,2,&manifest_key,&bytes)?;
                        // Validation above streams one 32-bit roaring partition
                        // at a time; no complete per-hash treemap survives it.
                        (BlobChunkSetWitness::Dense(0),BlobChunkSetWitness::Dense(0))
                    }
                };
                validate_canonical_chunk_witnesses_in_write(write,PAYLOAD_CHUNKS_TABLE,&manifest.object_id,&payload,manifest.total_size,manifest.state,"payload")?;
                validate_canonical_chunk_witnesses_in_write(write,OUTBOARD_CHUNKS_TABLE,&manifest.object_id,&outboard,manifest.outboard_size,manifest.state,"outboard")?;
                witness(write,original,1,&manifest_key,&bytes)?;
                object(write,&manifest.object_id)?;
                // Cross-check each direction by exact lookup, using on-disk
                // uniqueness instead of retaining a tag-name population.
                let has_names=write.table_exists(TAGS_BY_NAME_TABLE).map_err(RedbPersistence::storage_error)?;
                let has_reverse=write.table_exists(TAGS_BY_HASH_TABLE).map_err(RedbPersistence::storage_error)?;
                if has_names!=has_reverse {return Err(invalid());}
                visit(write,original,4,|source| {
                    if !source.key_prefix(&manifest_key)? {return Ok(());}
                    source.with_record(|key,encoded| {
                        let frame=window(original)?;
                        let tag:BlobTagRecord=frame.decode_blob(encoded)?;
                        frame.reserve(manifest_key.len().checked_add(tag.name.len()).ok_or_else(invalid)?)?;
                        let mut reverse=Vec::with_capacity(manifest_key.len().checked_add(tag.name.len()).ok_or_else(invalid)?);
                        reverse.extend_from_slice(&manifest_key);reverse.extend_from_slice(&tag.name);
                        if tag.version!=RECORD_VERSION || tag.hash!=hash || tag.generation!=active.generation
                            || key!=reverse.as_slice() {return Err(invalid());}
                        require_tag_counterpart(write,original,3,&tag.name,encoded,&tag)?;
                        witness(write,original,4,key,encoded)?;witness(write,original,3,&tag.name,encoded)
                    })
                })?;
                visit(write,original,3,|source| {
                    let tag=source.tag(original)?;
                    if tag.hash!=hash || tag.generation!=active.generation {return Ok(());}
                    source.with_record(|key,encoded| {
                        let frame=window(original)?;
                        let tag:BlobTagRecord=frame.decode_blob(encoded)?;
                        frame.reserve(manifest_key.len().checked_add(tag.name.len()).ok_or_else(invalid)?)?;
                        let mut reverse=Vec::with_capacity(manifest_key.len().checked_add(tag.name.len()).ok_or_else(invalid)?);
                        reverse.extend_from_slice(&manifest_key);reverse.extend_from_slice(&tag.name);
                        if tag.version!=RECORD_VERSION || tag.hash!=hash || tag.generation!=active.generation
                            || tag.name.as_slice()!=key {return Err(invalid());}
                        require_tag_counterpart(write,original,4,&reverse,encoded,&tag)
                    })
                })?;
                generation.last_retired_generation=generation.last_retired_generation.max(active.generation);
                generation.next_generation=generation.next_generation.max(active.generation.saturating_add(1));
                generation.active=None;
            }
            visit(write,original,5,|source| {
                let staging=source.staging(original)?;
                if staging.version!=RECORD_VERSION || !source.key_is_object(&staging.object_id)? {return Err(invalid());}
                if let Some((staged_hash,staged_generation))=staging.provisional_generation {
                    if staged_hash==hash {
                        let partial_key=BlobRepository::manifest_key(&hash,staged_generation);
                        if let Some(bytes)=get(write,original,2,&partial_key)? {witness(write,original,2,&partial_key,&bytes)?;}
                        witness_source(write,original,5,source)?;
                        object(write,&staging.object_id)?;
                    }
                } Ok(())
            })?;
            generation.last_purge_lsn=generation.last_purge_lsn.max(frontier.0);
            let encoded=_window.encode_blob(&generation)?;
            write.open_table(NEXT).map_err(RedbPersistence::storage_error)?.insert(hash.as_slice(),encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
        }
        // Payload, outboard, staging, tags and partials all use on-disk
        // uniqueness. No per-hash vector is assumed to be small.
        for family in [6,7] {
            visit(write,original,family,|source| {
                // Object ownership is checked from the borrowed key before any
                // payload/outboard value is materialized or copied.
                let mut id=[0u8;16];
                if !source.object_prefix(&mut id)? {return Ok(());}
                let objects=write.open_table(OBJECTS).map_err(RedbPersistence::storage_error)?;
                let selected=objects.get(id.as_slice()).map_err(RedbPersistence::storage_error)?.is_some();
                drop(objects);
                if selected {source.with_key(|key|witness(write,original,family,key,&(source.value_len() as u64).to_be_bytes()))?;}
                Ok(())
            })?;
        }
        Ok(BlobAuthoritativePurgeProjection {hashes:Vec::new(),native_control:Some(control)})
        })
    }
}

pub(super) fn apply(write:&redb::WriteTransaction,projection:&BlobAuthoritativePurgeProjection,
    original:&OriginalPersistenceFrame<'_>) -> Result<()> {
    RedbPersistence::with_original_blob_pages(original.original,||apply_with_pages(write,projection,original))
}
fn apply_with_pages(write:&redb::WriteTransaction,projection:&BlobAuthoritativePurgeProjection,
    original:&OriginalPersistenceFrame<'_>) -> Result<()> {
    let control=write.open_table(CONTROL).map_err(RedbPersistence::storage_error)?;
    if control.get(0).map_err(RedbPersistence::storage_error)?.map(|v|v.value())!=projection.native_control {return Err(invalid());}
    drop(control);
    let mut any=false;
    loop {
        let frame=window(original.original)?;
        let entry={
            let table=write.open_table(WITNESSES).map_err(RedbPersistence::storage_error)?;
            let mut cursor=table.cursor_authenticated();
            match cursor.next().transpose().map_err(RedbPersistence::storage_error)? {
                None=>None,
                Some(handle)=>{
                    let (key,value)=handle.materialize().map_err(RedbPersistence::storage_error)?;
                    Some((frame.copy_bytes(key.value())?,frame.copy_bytes(value.value())?))
                }
            }
        };
        let Some((address,descriptor))=entry else {break;};
        let (&family,key)=address.split_first().ok_or_else(invalid)?;
        let family=usize::from(family);
        if family>=FAMILIES.len() {return Err(invalid());}
        let (ordinal,len)=header(&descriptor)?;
        let mut found=false;
        metadata::visit(write,original.original,FAMILIES[family],|source| {
            if !source.key_matches(key)? {return Ok(());}
            found=true;
            if family>=6 {
                let backing=write.open_table(BYTES).map_err(RedbPersistence::storage_error)?;
                let first=chunk_key(ordinal,0)?;
                let expected=backing.get(first.as_slice()).map_err(RedbPersistence::storage_error)?.ok_or_else(invalid)?;
                if len!=8 || expected.value()!=&(source.value_len() as u64).to_be_bytes() {return Err(invalid());}
                Ok(())
            } else {
                if source.value_len()!=len {return Err(invalid());}
                let backing=write.open_table(BYTES).map_err(RedbPersistence::storage_error)?;
                for offset in (0..len).step_by(WITNESS_CHUNK) {
                    let key=chunk_key(ordinal,offset)?;
                    let expected=backing.get(key.as_slice()).map_err(RedbPersistence::storage_error)?.ok_or_else(invalid)?;
                    source.value_window(offset..(offset+WITNESS_CHUNK).min(len),&mut |bytes| {
                        if bytes!=expected.value() {return Err(invalid());} Ok(())
                    })?;
                }
                Ok(())
            }
        })?;
        if !found && !(family==0 && len==0) {return Err(invalid());}
        if found && family==0 && len==0 {return Err(invalid());}
        if family==0 {
            let table=write.open_table(NEXT).map_err(RedbPersistence::storage_error)?;
            let next=table.get(key).map_err(RedbPersistence::storage_error)?.ok_or_else(invalid)?;
            write.open_table(HASH_GENERATIONS_TABLE).map_err(RedbPersistence::storage_error)?
                .insert(key,next.value()).map_err(RedbPersistence::storage_error)?;
            any=true;
        } else {
            write.open_table(FAMILIES[family]).map_err(RedbPersistence::storage_error)?.remove(key).map_err(RedbPersistence::storage_error)?;
        }
        {
            let mut backing=write.open_table(BYTES).map_err(RedbPersistence::storage_error)?;
            for offset in (0..len).step_by(WITNESS_CHUNK) {
                let key=chunk_key(ordinal,offset)?;
                if backing.remove(key.as_slice()).map_err(RedbPersistence::storage_error)?.is_none() {return Err(invalid());}
            }
        }
        write.open_table(WITNESSES).map_err(RedbPersistence::storage_error)?.remove(address.as_slice()).map_err(RedbPersistence::storage_error)?;
    }
    #[cfg(test)]
    if any && AUTHORITATIVE_PURGE_BLOB_COMMIT_FAULT.with(|fault|fault.replace(false)) {
        return Err(Error::Other("blob authoritative purge commit failure injected".into()));
    }
    for table in [WITNESSES,NEXT,BYTES] {write.delete_table(table).map_err(RedbPersistence::storage_error)?;}
    write.delete_table(OBJECTS).map_err(RedbPersistence::storage_error)?;
    write.delete_table(CONTROL).map_err(RedbPersistence::storage_error)?;
    Ok(())
}

// The serialized treemap is already an admitted selected metadata field. Decode
// one 32-bit bitmap at a time, paying its real container/payload layouts before
// roaring allocates them. Never retain a treemap proportional to chunk history.
fn le<const N:usize>(bytes:&mut &[u8])->Result<[u8;N]> {
    let value=bytes.get(..N).ok_or_else(invalid)?.try_into().map_err(|_|invalid())?;
    *bytes=&bytes[N..];Ok(value)
}
fn take<'a>(bytes:&mut &'a [u8],len:usize)->Result<&'a [u8]> {
    let value=bytes.get(..len).ok_or_else(invalid)?;*bytes=&bytes[len..];Ok(value)
}
fn bitmap_plan(bytes:&[u8])->Result<(usize,usize)> {
    let mut rest=bytes;
    let cookie=u32::from_le_bytes(le::<4>(&mut rest)?);
    let (count,run)=if cookie==12346 {(u32::from_le_bytes(le::<4>(&mut rest)?) as usize,false)}
        else if cookie as u16==12347 {(((cookie>>16)+1) as usize,true)} else {return Err(invalid());};
    // This is Roaring's format width (u16 container keys), not an engine limit.
    if count>usize::from(u16::MAX)+1 {return Err(invalid());}
    let flags=take(&mut rest,if run {count.div_ceil(8)} else {0})?;
    let descriptions=take(&mut rest,count.checked_mul(4).ok_or_else(invalid)?)?;
    let offsets=if !run || count>=4 {count.checked_mul(4).ok_or_else(invalid)?} else {0};
    take(&mut rest,offsets)?;
    // Container is a u16 key plus a Vec/boxed bitmap enum. The tuple includes
    // both enum discriminant and key alignment; run decode has two interval
    // arrays plus a possible output reallocation live during checked conversion. Headers/offsets are temporary Vecs.
    let mut allocation=flags.len().checked_add(descriptions.len()).and_then(|n|n.checked_add(offsets))
        .and_then(|n|count.checked_mul(std::mem::size_of::<(u16,Vec<u16>,u64)>()).and_then(|m|n.checked_add(m)))
        .ok_or_else(invalid)?;
    for index in 0..count {
        let cardinality=usize::from(u16::from_le_bytes(descriptions[index*4+2..index*4+4].try_into().map_err(|_|invalid())?))+1;
        let is_run=run && flags[index/8]&(1<<(index%8))!=0;
        let (encoded,backing)=if is_run {
            let runs=usize::from(u16::from_le_bytes(le::<2>(&mut rest)?));
            let bytes=runs.checked_mul(std::mem::size_of::<[u16;2]>()).ok_or_else(invalid)?;
            // Checked conversion may retain the input while Vec::collect
            // grows its output. Pay input plus both output allocations at a
            // growth boundary, including Vec's four-element minimum.
            let capacity=runs.max(4).checked_next_power_of_two().ok_or_else(invalid)?;
            let output=capacity.checked_add(capacity/2).and_then(|n|n.checked_mul(std::mem::size_of::<[u16;2]>())).ok_or_else(invalid)?;
            (bytes,bytes.checked_add(output).ok_or_else(invalid)?)
        } else if cardinality<=4096 {
            let bytes=cardinality.checked_mul(std::mem::size_of::<u16>()).ok_or_else(invalid)?;(bytes,bytes)
        } else {(std::mem::size_of::<[u64;1024]>(),std::mem::size_of::<[u64;1024]>())};
        take(&mut rest,encoded)?;
        allocation=allocation.checked_add(backing).ok_or_else(invalid)?;
    }
    Ok((bytes.len()-rest.len(),allocation))
}
fn later_partition(mut bytes:&[u8],remaining:u64,key:u32)->Result<bool> {
    for _ in 0..remaining {
        let candidate=u32::from_le_bytes(le::<4>(&mut bytes)?);
        if candidate==key {return Ok(true);}
        let (length,_)=bitmap_plan(bytes)?;take(&mut bytes,length)?;
    }
    Ok(false)
}
fn validate_partial_chunks(write:&redb::WriteTransaction,original:&OwnedMemoryReservation,
    manifest:&BlobManifest,partial:&BlobPartialState)->Result<()> {
    for (bytes,count,size,definition) in [
        (partial.payload_indices.as_slice(),manifest.payload_chunk_count,manifest.total_size,PAYLOAD_CHUNKS_TABLE),
        (partial.outboard_indices.as_slice(),manifest.outboard_chunk_count,manifest.outboard_size,OUTBOARD_CHUNKS_TABLE),
    ] {
        let mut rest=bytes;
        let partitions=u64::from_le_bytes(le::<8>(&mut rest)?);
        let mut cardinality=0u64;
        for ordinal in 0..partitions {
            let high=u32::from_le_bytes(le::<4>(&mut rest)?);
            let (length,allocation)=bitmap_plan(rest)?;
            let frame=window(original)?;frame.reserve(allocation)?;
            let bitmap=roaring::RoaringBitmap::deserialize_from(take(&mut rest,length)?)
                .map_err(|err|Error::Other((format!("decode exact blob chunk set: {err}")).into()))?;
            // Match RoaringTreemap's last value for duplicate high keys without
            // an in-memory directory. Every bitmap still undergoes its full
            // checked decoder, even when a later partition supersedes it.
            if later_partition(rest,partitions-ordinal-1,high)? {continue;}
            cardinality=cardinality.checked_add(bitmap.len()).ok_or_else(invalid)?;
            let table=write.open_table(definition).map_err(RedbPersistence::storage_error)?;
            for low in bitmap.iter() {
                let index=(u64::from(high)<<32)|u64::from(low);
                if index>=logical_block_count(size) {return Err(invalid());}
                let key=BlobRepository::chunk_key(&manifest.object_id,index);
                let value=table.get(key.as_slice()).map_err(RedbPersistence::storage_error)?.ok_or_else(invalid)?;
                let expected=size.saturating_sub(index.saturating_mul(BLOB_LOGICAL_BLOCK_BYTES as u64))
                    .min(BLOB_LOGICAL_BLOCK_BYTES as u64) as usize;
                if value.value().is_empty() || value.value().len()>expected {return Err(invalid());}
            }
        }
        if cardinality!=count {return Err(invalid());}
    }
    Ok(())
}
