//! Immutable encoded output retains its original buffer and control invoices.
use super::ReadEncodingError;
use crate::local_transport::query_funding::Funded;
use contextdb_core::read_memory::{ReadCredit, ReadShared};

/// Canonical engine-created bytes. Clones share the same admitted allocation;
/// byte borrows cannot outlive the carrier. There is no mutable or owning Vec
/// projection that could separate the allocation from its original invoice.
#[derive(Debug, Clone)]
pub struct EncodedReadBytes {
    bytes: ReadShared<Funded<Vec<u8>>>,
}

impl EncodedReadBytes {
    pub(crate) fn from_funded(
        bytes: Funded<Vec<u8>>,
        memory_ceiling: u64,
    ) -> Result<Self, ReadEncodingError> {
        let owner = bytes.credit.as_ref()
            .ok_or(ReadEncodingError::InvalidPayload)?.owner();
        // The payload keeps its buffer credit. This separate exact invoice
        // covers the native shared cell until after its physical deallocation.
        let invoice = ReadCredit::try_new_owned(
            owner, ReadShared::<Funded<Vec<u8>>>::layout().size(),
            "reader", "query_buffer",
            "Reduce simultaneous query working memory or raise the declared reader limit.",
        ).map_err(|error| super::encoding_error_from_core(error, memory_ceiling))?;
        let bytes = ReadShared::try_from_prepaid_native(bytes, invoice)
            .map_err(|error| super::encoding_error_from_core(contextdb_core::Error::NativeEngineOther(error), memory_ceiling))?;
        Ok(Self { bytes })
    }

    pub fn as_slice(&self) -> &[u8] { &self.bytes.value }
}
impl std::ops::Deref for EncodedReadBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] { self.as_slice() }
}
impl AsRef<[u8]> for EncodedReadBytes {
    fn as_ref(&self) -> &[u8] { self.as_slice() }
}
impl PartialEq for EncodedReadBytes {
    fn eq(&self, other: &Self) -> bool { self.as_slice() == other.as_slice() }
}
impl Eq for EncodedReadBytes {}
impl PartialEq<Vec<u8>> for EncodedReadBytes {
    fn eq(&self, other: &Vec<u8>) -> bool { self.as_slice() == other.as_slice() }
}
impl PartialEq<EncodedReadBytes> for Vec<u8> {
    fn eq(&self, other: &EncodedReadBytes) -> bool { self.as_slice() == other.as_slice() }
}

impl serde::Serialize for EncodedReadBytes {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(self.as_slice(), serializer)
    }
}
impl<'de> serde::Deserialize<'de> for EncodedReadBytes {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let memory = crate::read_image_memory::active_owner().map_err(D::Error::custom)?;
        let before = memory.held();
        let bytes = crate::read_image_memory::deserialize_exact_vec(deserializer, false)?;
        let retained = memory.held().checked_sub(before)
            .ok_or_else(|| D::Error::custom("encoded output admission underflow"))?;
        let credit = memory.take_retained_credit(retained).map_err(D::Error::custom)?;
        Self::from_funded(Funded { value: bytes, credit: Some(credit) }, u64::MAX)
            .map_err(D::Error::custom)
    }
}
