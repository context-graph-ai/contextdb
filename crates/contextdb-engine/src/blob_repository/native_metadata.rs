//! Same-write authenticated metadata loans. Filter with a streamed projection;
//! complete records are borrowed only after selection. No key/value continuation
//! or decoded family population is retained by the cursor.
use super::*;
use crate::persistence::OriginalPersistenceFrame;
use crate::memory_accounting::OwnedMemoryReservation;
use std::io::Read;

pub(super) struct Bytes {
    bytes: Vec<u8>,
    _invoice: OwnedMemoryReservation,
}
impl std::ops::Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] { &self.bytes }
}
impl Bytes {
    pub(super) fn copy(original: &OwnedMemoryReservation, bytes: &[u8]) -> Result<Self> {
        let frame = OriginalPersistenceFrame::new(original)?;
        let bytes = frame.copy_bytes(bytes)?;
        Ok(Self {bytes, _invoice: frame.into_reservation()})
    }
}
pub(super) struct Source<'a, 't> {
    handle: &'a redb::BoundedByteHandle<'t>,
}
impl Source<'_, '_> {
    pub(super) fn key_prefix(&self, prefix: &[u8]) -> Result<bool> {
        if self.handle.key_len() < prefix.len() { return Ok(false); }
        self.handle.with_bytes(true,0..prefix.len(),|key| key==prefix)
            .map_err(RedbPersistence::storage_error)
    }
    pub(super) fn key_matches(&self, key: &[u8]) -> Result<bool> {
        self.handle.key_matches(key).map_err(RedbPersistence::storage_error)
    }
    pub(super) fn object_prefix(&self, id: &mut [u8;16]) -> Result<bool> {
        if self.handle.key_len()<id.len() {return Ok(false);}
        self.handle.with_bytes(true,0..16,|bytes|id.copy_from_slice(bytes)).map_err(RedbPersistence::storage_error)?;
        Ok(true)
    }

    pub(super) fn value_len(&self) -> usize {self.handle.value_len()}
    pub(super) fn with_key<R>(&self, consume:impl FnOnce(&[u8])->Result<R>)->Result<R> {
        self.handle.with_bytes(true,0..self.handle.key_len(),consume).map_err(RedbPersistence::storage_error)?
    }
    pub(super) fn value_window(&self,range:std::ops::Range<usize>,consume:&mut dyn FnMut(&[u8])->Result<()>)->Result<()> {
        self.handle.with_bytes(false,range,consume).map_err(RedbPersistence::storage_error)?
    }
    pub(super) fn with_record<R>(&self, consume: impl FnOnce(&[u8], &[u8])->Result<R>) -> Result<R> {
        let (key,value) = self.handle.materialize().map_err(RedbPersistence::storage_error)?;
        consume(key.value(),value.value())
    }
    pub(super) fn key_is_object(&self, id: &[u8;16]) -> Result<bool> { self.key_matches(id) }
    /// The projection uses the actual record schema. CheckedBytes consumes and
    /// validates every u8 without constructing the Vec that it replaces.
    fn probe<T: serde::de::DeserializeOwned>(&self, original: &OwnedMemoryReservation) -> Result<T> {
        let frame = OriginalPersistenceFrame::new(original)?;
        let mut reader = Reader::new(self.handle);
        // rmp-serde's ReadReader owns scratch for str/bin/ext tokens. Discover
        // its real maximum before constructing it; ordinary Vec<u8> arrays do
        // not cause a record-sized scratch charge or copy.
        let scratch = scan(&mut reader, 0)?;
        frame.reserve(scratch.checked_mul(3).and_then(|n|n.checked_add(128)).ok_or_else(invalid)?)?;
        let mut reader = Reader::new(self.handle);
        let result = T::deserialize(&mut rmp_serde::Deserializer::new(&mut reader));
        if let Some(error) = reader.failure.take() { drop(result); return Err(error); }
        result.map_err(|error| Error::Other((format!("decode blob repository record: {error}")).into()))
    }
    pub(super) fn tag(&self, original: &OwnedMemoryReservation) -> Result<Tag> { self.probe(original) }
    pub(super) fn staging(&self, original: &OwnedMemoryReservation) -> Result<Staging> { self.probe(original) }
}
fn invalid() -> Error { Error::Other("blob metadata source has invalid encoded boundaries".into()) }

pub(super) fn visit(write: &redb::WriteTransaction, original: &OwnedMemoryReservation,
    definition: TableDefinition<&[u8],&[u8]>, mut consume: impl FnMut(&Source<'_, '_>)->Result<()>) -> Result<()> {
    RedbPersistence::with_original_blob_pages(original, || {
        if !write.table_exists(definition).map_err(RedbPersistence::storage_error)? { return Ok(()); }
        let _control = OriginalPersistenceFrame::new(original)?;
        // The cursor's parents are paid by Redb page admissions; it keeps page
        // owners and scalar positions, never a copied wide seek key.
        let table = write.open_table(definition).map_err(RedbPersistence::storage_error)?;
        let mut cursor = table.cursor_authenticated();
        while let Some(handle) = cursor.next().transpose().map_err(RedbPersistence::storage_error)? {
            consume(&Source {handle:&handle})?;
        }
        Ok(())
    })
}
pub(super) fn get(write: &redb::WriteTransaction, original: &OwnedMemoryReservation,
    definition: TableDefinition<&[u8],&[u8]>, key: &[u8]) -> Result<Option<Bytes>> {
    let mut found = None;
    visit(write,original,definition,|source| {
        if source.key_matches(key)? {
            found=Some(source.with_record(|_,value| Bytes::copy(original,value))?);
        }
        Ok(())
    })?;
    Ok(found)
}

struct Reader<'a,'t> {
    handle: &'a redb::BoundedByteHandle<'t>,
    position: usize,
    start: usize,
    used: usize,
    buffer: [u8;4096],
    failure: Option<Error>,
}
impl<'a,'t> Reader<'a,'t> {
    fn new(handle: &'a redb::BoundedByteHandle<'t>) -> Self {
        Self {handle,position:0,start:0,used:0,buffer:[0;4096],failure:None}
    }
    fn number(&mut self, width: usize) -> Result<usize> {
        let mut bytes = [0u8;8];
        self.read_exact(&mut bytes[8-width..]).map_err(|_| self.failure.take().unwrap_or_else(invalid))?;
        usize::try_from(u64::from_be_bytes(bytes)).map_err(|_|invalid())
    }
    fn skip(&mut self, count: usize) -> Result<()> {
        self.position=self.position.checked_add(count).filter(|end|*end<=self.handle.value_len()).ok_or_else(invalid)?;
        Ok(())
    }
}
impl Read for Reader<'_, '_> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if self.position==self.handle.value_len() || output.is_empty() { return Ok(0); }
        if self.position<self.start || self.position>=self.start+self.used {
            self.start=self.position;
            self.used=(self.handle.value_len()-self.position).min(self.buffer.len());
            if let Err(error)=self.handle.with_bytes(false,self.start..self.start+self.used,
                |bytes| self.buffer[..self.used].copy_from_slice(bytes)) {
                self.failure=Some(RedbPersistence::storage_error(error));
                return Err(std::io::ErrorKind::Other.into());
            }
        }
        let offset=self.position-self.start;
        let count=output.len().min(self.used-offset);
        output[..count].copy_from_slice(&self.buffer[offset..offset+count]);
        self.position+=count;
        Ok(count)
    }
}
// Structural preflight only: the unmodified serde schemas still perform all
// type, missing/duplicate-field, enum, byte-element and integer validation.
fn scan(reader: &mut Reader<'_, '_>, depth: usize) -> Result<usize> {
    if depth>1024 { return Err(invalid()); }
    let marker=reader.number(1)?;
    let (children,raw,scratch)=match marker {
        0x00..=0x7f | 0xc0 | 0xc2..=0xc3 | 0xe0..=0xff => (0,0,0),
        0x80..=0x8f => ((marker&15)*2,0,0),
        0x90..=0x9f => (marker&15,0,0),
        0xa0..=0xbf => (0,marker&31,marker&31),
        0xc4 | 0xd9 => {let n=reader.number(1)?;(0,n,n)},
        0xc5 | 0xda => {let n=reader.number(2)?;(0,n,n)},
        0xc6 | 0xdb => {let n=reader.number(4)?;(0,n,n)},
        0xc7..=0xc9 => {let n=reader.number(1usize<<(marker-0xc7))?;(0,n.checked_add(1).ok_or_else(invalid)?,n)},
        0xca | 0xce | 0xd2 => (0,4,0),
        0xcb | 0xcf | 0xd3 => (0,8,0),
        0xcc | 0xd0 => (0,1,0),
        0xcd | 0xd1 => (0,2,0),
        0xd4..=0xd8 => {let n=1usize<<(marker-0xd4);(0,n+1,n)},
        0xdc => (reader.number(2)?,0,0),
        0xdd => (reader.number(4)?,0,0),
        0xde => (reader.number(2)?.checked_mul(2).ok_or_else(invalid)?,0,0),
        0xdf => (reader.number(4)?.checked_mul(2).ok_or_else(invalid)?,0,0),
        _ => return Err(invalid()),
    };
    reader.skip(raw)?;
    let mut maximum=scratch;
    for _ in 0..children { maximum=maximum.max(scan(reader,depth+1)?); }
    Ok(maximum)
}

#[derive(Default)]
struct CheckedBytes;
impl<'de> serde::Deserialize<'de> for CheckedBytes {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> std::result::Result<Self,D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value=CheckedBytes;
            fn expecting(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result { f.write_str("a sequence") }
            fn visit_seq<A:serde::de::SeqAccess<'de>>(self,mut values:A)->std::result::Result<CheckedBytes,A::Error> {
                while values.next_element::<u8>()?.is_some() {} Ok(CheckedBytes)
            }
        }
        decoder.deserialize_seq(V)
    }
}
#[allow(dead_code)]
#[derive(serde::Deserialize)]
pub(super) struct Tag {
    pub version:u8, name:CheckedBytes, pub hash:[u8;32], pub generation:u64,
    pub format:u8, role:BlobTagRole,
}
#[allow(dead_code)]
#[derive(serde::Deserialize)]
pub(super) struct Staging {
    pub version:u8, pub object_id:[u8;16], start_lsn:u64, expected_size:Option<u64>,
    received_size:u64,payload_chunk_count:u64,outboard_chunk_count:u64,
    payload_indices:CheckedBytes,outboard_indices:CheckedBytes,
    pub provisional_generation:Option<([u8;32],u64)>,
    #[serde(default)] adapter_state:CheckedBytes,
}
