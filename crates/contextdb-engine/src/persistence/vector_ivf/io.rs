//! Actual native views and borrowed writes; no owned decoded page directory.
use super::*;
use contextdb_vector::ivf::storage::PageKind;
use redb::{ReadableTable, TableDefinition};

pub(super) const PENDING: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ivf_pending_seals");
// The allocator is shared authority and therefore lives under SCOPES and its
// final Configuration binding, rather than in an unauthenticated side table.
const NEXT_STORAGE: &[u8] = b"\0ivf_storage_id";

/// This is a physical table name, never a row identity, Context or clock.
pub(super) struct KindTableName([u8; 23]);
impl KindTableName {
    pub fn new(storage: u64, kind: PageKind) -> Result<Self> {
        if storage == 0 {
            return Err(corrupt());
        }
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut bytes = [0; 23];
        bytes[..4].copy_from_slice(b"ivf_");
        bytes[20] = b'_';
        for (at, digit) in bytes[4..20].iter_mut().enumerate() {
            *digit = HEX[((storage >> (60 - at * 4)) & 15) as usize];
        }
        let tag = header::kind_tag(kind);
        bytes[21] = HEX[usize::from(tag >> 4)];
        bytes[22] = HEX[usize::from(tag & 15)];
        Ok(Self(bytes))
    }
    pub fn as_str(&self) -> Result<&str> {
        std::str::from_utf8(&self.0).map_err(|_| corrupt())
    }
}

pub(super) enum NativeIo<'a> {
    CapturedTransaction {
        read: &'a redb::ReadTransaction,
        admission: &'a super::super::native_storage_admission::StorageAdmissionScope,
    },
    Borrowed(&'a redb::WriteTransaction),
}
impl NativeIo<'_> {
    pub fn fingerprint(&self, name: &str) -> Result<Option<[u8; 32]>> {
        let definition = TableDefinition::<&[u8], &[u8]>::new(name);
        match self {
            Self::CapturedTransaction { read, admission } => {
                admission.run_original_unqualified(|| {
                    read.table_root_fingerprint(definition)
                        .map_err(RedbPersistence::storage_error)
                })
            }
            Self::Borrowed(write) => match write.table_root_fingerprint(definition) {
                Ok(root) => Ok(root),
                Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
                Err(error) => Err(RedbPersistence::storage_error(error)),
            },
        }
    }
    /// Borrow the original native guard. No Vec/frame alias escapes this call.
    pub fn with_value<T>(
        &self,
        name: &str,
        key: &[u8],
        read: impl FnOnce(Option<&[u8]>) -> Result<T>,
    ) -> Result<T> {
        if self.fingerprint(name)?.is_none() {
            return read(None);
        }
        self.with_existing_value(name, key, read)
    }
    /// The scope factory has already authenticated this captured role root.
    /// A native missing-table error is retained; this method never creates it.
    pub fn with_existing_value<T>(
        &self,
        name: &str,
        key: &[u8],
        read: impl FnOnce(Option<&[u8]>) -> Result<T>,
    ) -> Result<T> {
        match self {
            Self::CapturedTransaction {
                read: transaction,
                admission,
            } => admission.run_original_unqualified(|| {
                let value = transaction
                    .get_native_byte_value(name, key)
                    .map_err(|error| RedbPersistence::storage_error(redb::Error::Native(error)))?;
                read(value.as_ref().map(|v| v.value()))
            }),
            Self::Borrowed(write) => {
                // Do not open/create a missing table just to answer a read.
                let table = write
                    .open_table(TableDefinition::<&[u8], &[u8]>::new(name))
                    .map_err(RedbPersistence::storage_error)?;
                let value = table.get(key).map_err(RedbPersistence::storage_error)?;
                read(value.as_ref().map(|v| v.value()))
            }
        }
    }
    pub fn write(&self) -> Result<&redb::WriteTransaction> {
        match self {
            Self::Borrowed(write) => Ok(write),
            Self::CapturedTransaction { .. } => Err(corrupt()),
        }
    }
    pub fn put(&self, name: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_using(name, key, value, |insert| insert())
    }
    pub fn put_using(
        &self,
        name: &str,
        key: &[u8],
        value: &[u8],
        wrap: impl FnOnce(&mut dyn FnMut() -> Result<()>) -> Result<()>,
    ) -> Result<()> {
        let mut table = self
            .write()?
            .open_table(TableDefinition::<&[u8], &[u8]>::new(name))
            .map_err(RedbPersistence::storage_error)?;
        let mut insert = || {
            table
                .insert(key, value)
                .map_err(RedbPersistence::storage_error)?;
            Ok(())
        };
        wrap(&mut insert)
    }
}

/// The same actual outer write owns allocation and its inventory record.
/// A damaged allocator cannot silently reuse another physical family.
pub(super) fn allocate_storage(
    write: &redb::WriteTransaction,
    native_value: &mut [u8; header::VALUE_BYTES],
) -> Result<u64> {
    let native = NativeIo::Borrowed(write);
    let next = native.with_value(SCOPES.name(), NEXT_STORAGE, |value| {
        let Some(value) = value else { return Ok(1) };
        if value.len() != 48 || &value[..8] != b"CDBIVFA1" {
            return Err(corrupt());
        }
        let mut digest = blake3::Hasher::new();
        digest.update(NEXT_STORAGE);
        digest.update(&value[..16]);
        if value[16..] != *digest.finalize().as_bytes() {
            return Err(corrupt());
        }
        let next = u64::from_le_bytes(value[8..16].try_into().map_err(|_| corrupt())?);
        if next == 0 {
            return Err(corrupt());
        }
        Ok(next)
    })?;
    for tag in 1..=16 {
        let name = KindTableName::new(next, header::kind_from_tag(tag)?)?;
        if write
            .table_exists(TableDefinition::<&[u8], &[u8]>::new(name.as_str()?))
            .map_err(RedbPersistence::storage_error)?
        {
            return Err(corrupt());
        }
    }
    let qualification = super::qualification::TableName::new(next)?;
    if write
        .table_exists(TableDefinition::<&[u8], &[u8]>::new(
            qualification.as_str()?,
        ))
        .map_err(RedbPersistence::storage_error)?
    {
        return Err(corrupt());
    }
    let following = next.checked_add(1).ok_or_else(corrupt)?;
    native_value[..8].copy_from_slice(b"CDBIVFA1");
    native_value[8..16].copy_from_slice(&following.to_le_bytes());
    let mut digest = blake3::Hasher::new();
    digest.update(NEXT_STORAGE);
    digest.update(&native_value[..16]);
    native_value[16..48].copy_from_slice(digest.finalize().as_bytes());
    native.put(SCOPES.name(), NEXT_STORAGE, &native_value[..48])?;
    Ok(next)
}
