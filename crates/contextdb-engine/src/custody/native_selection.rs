//! Fixed native custody slots and scratch on the original request capability.
use super::native_decode::Invalid;
#[cfg(feature = "sync-orchestration")]
use crate::persistence::{native_storage_admission::NativeStorageFailure, writer_source::NativeConfigFailure};
use contextdb_core::read_memory::{NativeRefusal, ReadCredit, ReadMemoryOwner};
#[derive(Debug)]
pub(crate) enum Failure {
    Invalid(Invalid),
    Refused(NativeRefusal),
    // Backend, cache/bootstrap and checkpoint faults remain honest unqualified
    // faults; none is converted into absence or a protocol Error payload.
    Unqualified(contextdb_core::Error),
    ColdUnqualified(redb::NativeColdReadError),
    OwnerMismatch,
}
pub(crate) type Selected<T> = std::result::Result<T, Failure>;
impl From<Invalid> for Failure {
    fn from(e: Invalid) -> Self {
        Self::Invalid(e)
    }
}
#[cfg(feature = "sync-orchestration")]
impl From<NativeConfigFailure> for Failure {
    fn from(e: NativeConfigFailure) -> Self {
        match e {
            NativeConfigFailure::Storage(NativeStorageFailure::Refused(r)) => Self::Refused(r),
            NativeConfigFailure::Storage(NativeStorageFailure::Unqualified(e)) => {
                Self::Unqualified(e)
            }
            NativeConfigFailure::ColdUnqualified(e) => Self::ColdUnqualified(e),
            NativeConfigFailure::InvalidStoredText | NativeConfigFailure::InvalidIncarnation => {
                Self::Invalid(Invalid::Codec)
            }
            NativeConfigFailure::InvalidCanonicalIndex => Self::Invalid(Invalid::Canonical),
            NativeConfigFailure::OwnerMismatch => Self::OwnerMismatch,
        }
    }
}
/// This native constructor uses the scalar reserve route. The older image
/// helper's allocating Error route cannot be used by this producer.
pub(crate) struct NativeSelectionHold<T> {
    values: Vec<T>,
    invoice: ReadCredit,
    slots: usize,
}
impl<T> NativeSelectionHold<T> {
    pub(crate) fn try_new(owner: &ReadMemoryOwner, slots: usize) -> Selected<Self> {
        let layout = std::alloc::Layout::array::<T>(slots).map_err(|_| {
            Failure::Refused(NativeRefusal::Other("custody selection layout overflow"))
        })?;
        let invoice = ReadCredit::try_new_owned_native(
            owner.clone(),
            layout.size(),
            "reader",
            "retain_custody_selection",
            "Reduce custody metadata selected by this operation.",
        )
        .map_err(Failure::Refused)?;
        let values = if layout.size() == 0 {
            Vec::new()
        } else {
            let p = std::ptr::NonNull::new(unsafe { std::alloc::alloc(layout) }.cast::<T>())
                .ok_or(Failure::Refused(NativeRefusal::Other(
                    "custody selection allocation failed",
                )))?;
            // The exact array is prepaid; only the empty initialized prefix is
            // adopted. retain never grows or reallocates the fixed capacity.
            unsafe { Vec::from_raw_parts(p.as_ptr(), 0, slots) }
        };
        Ok(Self {
            values,
            invoice,
            slots,
        })
    }
    pub(crate) fn retain(&mut self, value: T) -> Selected<()> {
        if self.values.len() == self.slots {
            return Err(Failure::Refused(NativeRefusal::Other(
                "custody selection exceeds counted slots",
            )));
        }
        self.values.push(value);
        Ok(())
    }
    pub(crate) fn as_slice(&self) -> &[T] {
        &self.values
    }
    pub(crate) fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.values
    }
    pub(crate) fn is_funded_by(&self, owner: &ReadMemoryOwner) -> bool {
        self.invoice.is_funded_by(owner)
    }
    pub(crate) fn truncate(&mut self, len: usize) {
        self.values.truncate(len)
    }
}
pub(crate) struct NativeBuffer {
    bytes: Vec<u8>,
    invoice: ReadCredit,
}
impl NativeBuffer {
    pub(crate) fn allocate(owner: &ReadMemoryOwner, len: usize) -> Selected<Self> {
        let layout = std::alloc::Layout::array::<u8>(len).map_err(|_| {
            Failure::Refused(NativeRefusal::Other("custody buffer layout overflow"))
        })?;
        let invoice = ReadCredit::try_new_owned_native(
            owner.clone(),
            len,
            "reader",
            "custody_decode_scratch",
            "Reduce custody metadata selected by this operation.",
        )
        .map_err(Failure::Refused)?;
        let bytes = if len == 0 {
            Vec::new()
        } else {
            let p = std::ptr::NonNull::new(unsafe { std::alloc::alloc(layout) }).ok_or(
                Failure::Refused(NativeRefusal::Other("custody buffer allocation failed")),
            )?;
            unsafe {
                std::ptr::write_bytes(p.as_ptr(), 0, len);
                Vec::from_raw_parts(p.as_ptr(), len, len)
            }
        };
        Ok(Self { bytes, invoice })
    }
    pub(crate) fn is_funded_by(&self, owner: &ReadMemoryOwner) -> bool {
        self.invoice.is_funded_by(owner)
    }
    // Transfer the same already-paid exact physical bytes; no text copy occurs.
    pub(crate) fn into_text_parts(self) -> Selected<(String, ReadCredit)> {
        let Self { bytes, invoice } = self;
        let text = String::from_utf8(bytes).map_err(|_| Failure::Invalid(Invalid::Codec))?;
        Ok((text, invoice))
    }
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(crate) fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
    pub(crate) fn text(&self) -> Selected<&str> {
        std::str::from_utf8(&self.bytes).map_err(|_| Failure::Invalid(Invalid::Codec))
    }
}
impl NativeBuffer {
    pub(crate) fn format(owner: &ReadMemoryOwner, args: std::fmt::Arguments<'_>) -> Selected<Self> {
        use std::fmt::Write;
        struct Count(usize);
        impl Write for Count {
            fn write_str(&mut self, s: &str) -> std::fmt::Result {
                self.0 = self.0.checked_add(s.len()).ok_or(std::fmt::Error)?;
                Ok(())
            }
        }
        let mut count = Count(0);
        count
            .write_fmt(args)
            .map_err(|_| Failure::Invalid(Invalid::Canonical))?;
        let mut value = Self::allocate(owner, count.0)?;
        struct Emit<'a> {
            bytes: &'a mut [u8],
            pos: usize,
        };
        impl Write for Emit<'_> {
            fn write_str(&mut self, s: &str) -> std::fmt::Result {
                let end = self
                    .pos
                    .checked_add(s.len())
                    .filter(|n| *n <= self.bytes.len())
                    .ok_or(std::fmt::Error)?;
                self.bytes[self.pos..end].copy_from_slice(s.as_bytes());
                self.pos = end;
                Ok(())
            }
        }
        let mut emit = Emit {
            bytes: value.bytes_mut(),
            pos: 0,
        };
        emit.write_fmt(args)
            .map_err(|_| Failure::Invalid(Invalid::Canonical))?;
        if emit.pos != count.0 {
            return Err(Invalid::Canonical.into());
        }
        Ok(value)
    }
}
