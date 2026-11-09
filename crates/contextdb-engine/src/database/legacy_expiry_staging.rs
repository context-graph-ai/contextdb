//! Original funding for Legacy expiry bodies until their WriteSet is freed.
use super::*;

pub(super) struct LegacyExpiryStaging<'a> {
    host: &'a DatabaseTransactionHost,
    tx: TxId,
    active: bool,
    memory: std::cell::RefCell<OwnedMemoryReservation>,
}
impl<'a> LegacyExpiryStaging<'a> {
    pub(super) fn new(
        host: &'a DatabaseTransactionHost,
        tx: TxId,
        accountant: crate::memory_accounting::AccountantHandle,
    ) -> Self {
        Self {
            host,
            tx,
            active: true,
            memory: std::cell::RefCell::new(OwnedMemoryReservation::new(accountant)),
        }
    }
    // A successful returned WriteSet drops before this earlier-declared guard.
    pub(super) fn committed(&mut self) {
        self.active = false;
    }
    pub(super) fn rollback(&mut self) {
        if self.active {
            let _ = self.host.rollback(self.tx);
            self.active = false;
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Preserve inline typed admission errors."
    )]
    pub(super) fn push<T>(
        &self,
        values: &mut Vec<T>,
        payload: usize,
        produce: impl FnOnce() -> Result<T>,
    ) -> Result<()> {
        let mut memory = self.memory.borrow_mut();
        let old_bytes = std::alloc::Layout::array::<T>(values.capacity())
            .map_err(|_| Error::Other("expiry staged backing size overflow".into()))?
            .size();
        if values.len() == values.capacity() {
            let capacity = values
                .len()
                .checked_add(1)
                .ok_or_else(|| Error::Other("expiry staged capacity overflow".into()))?;
            let replacement = std::alloc::Layout::array::<T>(capacity)
                .map_err(|_| Error::Other("expiry staged backing size overflow".into()))?
                .size();
            let bytes = replacement
                .checked_add(payload)
                .ok_or_else(|| Error::Other("expiry staged payload size overflow".into()))?;
            memory.try_grow_for(
                bytes,
                "retention",
                "prepare_expiry",
                "Raise MEMORY_LIMIT to admit the expiry transaction workspace.",
            )?;
            // Both backings are admitted during movement. Payloads move without
            // cloning; only the empty old allocation is returned after dealloc.
            let mut next = Vec::with_capacity(capacity);
            next.append(values);
            let old = std::mem::replace(values, next);
            drop(old);
            memory.try_shrink(old_bytes)?;
        } else {
            memory.try_grow_for(
                payload,
                "retention",
                "prepare_expiry",
                "Raise MEMORY_LIMIT to admit the expiry transaction workspace.",
            )?;
        }
        values.push(produce()?);
        Ok(())
    }
    #[expect(
        clippy::result_large_err,
        reason = "Preserve inline typed admission errors."
    )]
    pub(super) fn copy_config(
        &self,
        values: &mut Vec<(String, Vec<u8>)>,
        key: &str,
        value: &[u8],
    ) -> Result<()> {
        let bytes = key
            .len()
            .checked_add(value.len())
            .ok_or_else(|| Error::Other("expiry config payload size overflow".into()))?;
        self.push(values, bytes, || Ok((key.to_owned(), value.to_vec())))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Preserve inline typed admission errors."
    )]
    pub(super) fn expiry_config(
        &self,
        values: &mut Vec<(String, Vec<u8>)>,
        table: &str,
        row: RowId,
        created: TxId,
        lsn: Lsn,
    ) -> Result<()> {
        let prefix = crate::composite_store::RETENTION_EXPIRY_PREFIX;
        let mut key_count = ByteCount(0);
        std::fmt::Write::write_fmt(
            &mut key_count,
            format_args!("{prefix}{}:{table}:{}:{}", table.len(), row.0, created.0),
        )
        .map_err(|_| Error::Other("expiry config key size overflow".into()))?;
        let identity = (table, row, created, lsn);
        let mut encoded_count = ByteCount(0);
        bincode::serde::encode_into_std_write(
            identity,
            &mut encoded_count,
            bincode::config::standard(),
        )
        .map_err(|_| Error::Other("expiry config value size overflow".into()))?;
        let bytes = key_count
            .0
            .checked_add(encoded_count.0)
            .ok_or_else(|| Error::Other("expiry config payload size overflow".into()))?;
        self.push(values, bytes, || {
            let mut key = String::with_capacity(key_count.0);
            std::fmt::Write::write_fmt(
                &mut key,
                format_args!("{prefix}{}:{table}:{}:{}", table.len(), row.0, created.0),
            )
            .map_err(|_| Error::Other("expiry config key formatting failed".into()))?;
            let mut encoded = Vec::with_capacity(encoded_count.0);
            bincode::serde::encode_into_std_write(
                identity,
                &mut encoded,
                bincode::config::standard(),
            )
            .map_err(|_| Error::Other("expiry config value encoding failed".into()))?;
            Ok((key, encoded))
        })
    }
}
impl Drop for LegacyExpiryStaging<'_> {
    fn drop(&mut self) {
        self.rollback();
    }
}
struct ByteCount(usize);
impl std::fmt::Write for ByteCount {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.0 = self.0.checked_add(value.len()).ok_or(std::fmt::Error)?;
        Ok(())
    }
}
impl std::io::Write for ByteCount {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("expiry config size overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
