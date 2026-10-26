use crate::types::{ContextId, Principal, RowId, ScopeLabel, TxId, Value, VectorIndexRef};
use std::borrow::Cow;
use std::collections::BTreeSet;

/// Text that keeps its actual producer's native storage alive. Legacy text
/// moves without copying; the native arm retains the same original invoice.
/// This carrier does not qualify allocations nested inside a foreign producer.
pub struct DiagnosticText(DiagnosticTextStorage);
enum DiagnosticTextStorage {
    StaticParts {
        prefix: &'static str,
        text: &'static str,
        suffix: &'static str,
    },
    Unsigned {
        prefix: &'static str,
        value: u64,
        suffix: &'static str,
    },
    Text(Cow<'static, str>),
    Native(FundedEngineError),
    Refusal(crate::read_memory::NativeRefusal),
}
impl DiagnosticText {
    /// Compose immutable text without creating engine-owned storage.
    #[doc(hidden)]
    pub fn from_static_parts(
        prefix: &'static str,
        text: &'static str,
        suffix: &'static str,
    ) -> Self {
        Self(DiagnosticTextStorage::StaticParts {
            prefix,
            text,
            suffix,
        })
    }
    /// A scalar diagnosis owns no heap allocation and borrows only constants.
    #[doc(hidden)]
    pub fn from_unsigned(prefix: &'static str, value: u64, suffix: &'static str) -> Self {
        Self(DiagnosticTextStorage::Unsigned {
            prefix,
            value,
            suffix,
        })
    }

    /// Move an already admitted native diagnostic into an outer error field.
    /// Construction allocates nothing and preserves the original cell.
    #[doc(hidden)]
    pub fn from_shared<T, I>(value: crate::read_memory::ReadShared<T, I>) -> Self
    where
        T: std::fmt::Display + Send + Sync + 'static,
        I: Send + Sync + 'static,
    {
        use crate::read_memory::ReadShared;
        unsafe fn inspect(_: std::ptr::NonNull<()>) -> (Option<std::ptr::NonNull<Error>>, bool) {
            (None, false)
        }
        unsafe fn display<T: std::fmt::Display, I>(
            pointer: std::ptr::NonNull<()>,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            let owner = std::mem::ManuallyDrop::new(unsafe {
                ReadShared::<T, I>::from_raw(pointer.as_ptr().cast())
            });
            std::fmt::Display::fmt(&**owner, f)
        }
        unsafe fn release<T, I>(pointer: std::ptr::NonNull<()>) {
            drop(unsafe { ReadShared::<T, I>::from_raw(pointer.as_ptr().cast()) });
        }
        let pointer =
            std::ptr::NonNull::new(value.into_raw().cast_mut().cast()).expect("native diagnostic");
        // SAFETY: ReadShared supplies the live immutable Send+Sync payload and
        // finalizer. Its existing NativeFreeGuard frees the block before invoices.
        let value = unsafe {
            FundedEngineError::from_owned(pointer, inspect, display::<T, I>, release::<T, I>)
        };
        Self(DiagnosticTextStorage::Native(value))
    }
    /// Borrow existing text only; deferred diagnostics are rendered by Display.
    pub fn as_text(&self) -> Option<&str> {
        match &self.0 {
            DiagnosticTextStorage::Text(text) => Some(text),
            _ => None,
        }
    }
    pub fn static_text(&self) -> Option<&'static str> {
        match &self.0 {
            DiagnosticTextStorage::Text(Cow::Borrowed(text)) => Some(*text),
            _ => None,
        }
    }
    /// Inspect complete deferred text without allocating or detaching its owner.
    pub fn contains(&self, needle: &str) -> bool {
        if let Some(text) = self.as_text() {
            return text.contains(needle);
        }
        if needle.is_empty() {
            return true;
        }
        struct Search<'a> {
            needle: &'a [u8],
            matched: usize,
            found: bool,
        }
        impl std::fmt::Write for Search<'_> {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                for byte in text.bytes() {
                    if self.found {
                        break;
                    }
                    while self.matched != 0 && self.needle[self.matched] != byte {
                        // Find the longest proper prefix that is also a suffix
                        // of the matched prefix. Recompute from borrowed bytes;
                        // no pattern table or diagnostic copy is allocated.
                        self.matched = (1..self.matched)
                            .rev()
                            .find(|&length| {
                                self.needle[..self.matched].ends_with(&self.needle[..length])
                            })
                            .unwrap_or(0);
                    }
                    if self.needle[self.matched] == byte {
                        self.matched += 1;
                    }
                    if self.matched == self.needle.len() {
                        self.found = true;
                    }
                }
                Ok(())
            }
        }
        let mut search = Search {
            needle: needle.as_bytes(),
            matched: 0,
            found: false,
        };
        std::fmt::write(&mut search, format_args!("{self}")).is_ok() && search.found
    }
    /// Inspect the prefix of the complete Display text without allocating or
    /// detaching its owner, including prefixes split across formatting chunks.
    pub fn starts_with(&self, prefix: &str) -> bool {
        if let Some(text) = self.as_text() {
            return text.starts_with(prefix);
        }
        if prefix.is_empty() {
            return true;
        }
        struct Prefix<'a> {
            prefix: &'a [u8],
            matched: usize,
            matches: bool,
        }
        impl std::fmt::Write for Prefix<'_> {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                let length = text.len().min(self.prefix.len() - self.matched);
                let end = self.matched + length;
                self.matches &= text.as_bytes()[..length] == self.prefix[self.matched..end];
                self.matched = end;
                Ok(())
            }
        }
        let mut inspect = Prefix {
            prefix: prefix.as_bytes(),
            matched: 0,
            matches: true,
        };
        std::fmt::write(&mut inspect, format_args!("{self}")).is_ok()
            && inspect.matches
            && inspect.matched == inspect.prefix.len()
    }
    fn equals_text(&self, expected: &str) -> bool {
        struct Equal<'a> {
            expected: &'a [u8],
            offset: usize,
            equal: bool,
        }
        impl std::fmt::Write for Equal<'_> {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                let end = self.offset.checked_add(text.len()).ok_or(std::fmt::Error)?;
                self.equal &= self.expected.get(self.offset..end) == Some(text.as_bytes());
                self.offset = end;
                Ok(())
            }
        }
        let mut equal = Equal {
            expected: expected.as_bytes(),
            offset: 0,
            equal: true,
        };
        std::fmt::write(&mut equal, format_args!("{self}")).is_ok()
            && equal.equal
            && equal.offset == equal.expected.len()
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating."
    )]
    fn admit(
        owner: crate::read_memory::ReadMemoryOwner,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> crate::Result<crate::read_memory::ReadCredit> {
        use crate::read_memory::{NativeRefusal, ReadCredit, ReadMemoryBudget};
        // Engine suppliers with scalar admission do not format on refusal.
        // A genuine application provider keeps its ordinary contract and its
        // original error, unchanged; there is no unsupported fallback or skip.
        if owner.native_reserve_supported() {
            ReadCredit::try_new_owned_native(owner, bytes, subsystem, operation, hint).map_err(
                |error| match error {
                    NativeRefusal::MemoryBudgetExceeded {
                        subsystem,
                        operation,
                        requested_bytes,
                        available_bytes,
                        budget_limit_bytes,
                        hint,
                    } => Error::MemoryBudgetExceeded {
                        subsystem: subsystem.into(),
                        operation: operation.into(),
                        requested_bytes,
                        available_bytes,
                        budget_limit_bytes,
                        hint: hint.into(),
                    },
                    NativeRefusal::Other(reason) => Error::Other(reason.into()),
                    other => Error::NativeEngineOther(other),
                },
            )
        } else {
            ReadCredit::try_new_owned(owner, bytes, subsystem, operation, hint)
        }
    }
    /// Prepare native diagnostic storage on its actual existing payer before
    /// invoking the producer. Failure has no engine-owned formatted backing.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating."
    )]
    pub fn try_prepare_shared<T>(
        owner: crate::read_memory::ReadMemoryOwner,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> crate::Result<crate::read_memory::ReadPrepared<T>> {
        use crate::read_memory::ReadShared;
        let invoice = Self::admit(
            owner,
            ReadShared::<T>::layout().size(),
            subsystem,
            operation,
            hint,
        )?;
        ReadShared::try_prepare_native(invoice).map_err(Error::NativeEngineOther)
    }
    /// Admit both the exact output buffer and native cell before allocating.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating."
    )]
    pub fn try_format(
        owner: crate::read_memory::ReadMemoryOwner,
        value: impl std::fmt::Display,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> crate::Result<Self> {
        use crate::read_memory::{NativeProduce, NativeRefusal, ReadShared};
        struct Count(usize);
        impl std::fmt::Write for Count {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                self.0 = self.0.checked_add(text.len()).ok_or(std::fmt::Error)?;
                Ok(())
            }
        }
        let mut count = Count(0);
        std::fmt::write(&mut count, format_args!("{value}")).map_err(|_| {
            Error::NativeEngineOther(NativeRefusal::Other("diagnostic text size overflow"))
        })?;
        let text_layout = std::alloc::Layout::array::<u8>(count.0).map_err(|_| {
            Error::NativeEngineOther(NativeRefusal::Other("diagnostic text layout overflow"))
        })?;
        let bytes = ReadShared::<String>::layout()
            .size()
            .checked_add(count.0)
            .ok_or(Error::NativeEngineOther(NativeRefusal::Other(
                "diagnostic text layout overflow",
            )))?;
        let invoice = Self::admit(owner, bytes, subsystem, operation, hint)?;
        let shared = ReadShared::try_from_prepaid_payload_with_native(invoice, || {
            // Do not let an inconsistent Display grow past its admitted buffer.
            struct Output(String);
            impl std::fmt::Write for Output {
                fn write_str(&mut self, text: &str) -> std::fmt::Result {
                    if text.len() > self.0.capacity() - self.0.len() {
                        return Err(std::fmt::Error);
                    }
                    self.0.push_str(text);
                    Ok(())
                }
            }
            let text = if count.0 == 0 {
                String::new()
            } else {
                // The original invoice is already outside the native cell. Use
                // this exact raw layout, never an allocator-selected growth cap.
                let raw = unsafe { std::alloc::alloc(text_layout) };
                let pointer = std::ptr::NonNull::new(raw).ok_or(Error::NativeEngineOther(
                    NativeRefusal::AllocatorFailed {
                        site: "diagnostic text",
                        layout: text_layout,
                    },
                ))?;
                // SAFETY: the global allocator supplied count.0 uninitialized
                // bytes with byte alignment; a zero-length String owns that
                // exact capacity and will free it through the same allocator.
                unsafe { String::from_raw_parts(pointer.as_ptr(), 0, count.0) }
            };
            let mut output = Output(text);
            std::fmt::write(&mut output, format_args!("{value}")).map_err(|_| {
                Error::NativeEngineOther(NativeRefusal::Other(
                    "diagnostic display exceeded its admitted size",
                ))
            })?;
            Ok::<_, Error>(output.0)
        })
        .map_err(|error| match error {
            NativeProduce::Refused(error) => Error::NativeEngineOther(error),
            NativeProduce::Producer(error) => error,
        })?;
        Ok(Self::from_shared(shared))
    }
}
impl From<String> for DiagnosticText {
    fn from(text: String) -> Self {
        Self(DiagnosticTextStorage::Text(Cow::Owned(text)))
    }
}
impl From<&'static str> for DiagnosticText {
    fn from(text: &'static str) -> Self {
        Self(DiagnosticTextStorage::Text(Cow::Borrowed(text)))
    }
}
impl From<Cow<'static, str>> for DiagnosticText {
    fn from(text: Cow<'static, str>) -> Self {
        Self(DiagnosticTextStorage::Text(text))
    }
}
impl From<crate::read_memory::NativeRefusal> for DiagnosticText {
    fn from(refusal: crate::read_memory::NativeRefusal) -> Self {
        Self(DiagnosticTextStorage::Refusal(refusal))
    }
}
impl From<FundedEngineError> for DiagnosticText {
    fn from(owner: FundedEngineError) -> Self {
        Self(DiagnosticTextStorage::Native(owner))
    }
}
impl std::fmt::Display for DiagnosticText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            DiagnosticTextStorage::StaticParts {
                prefix,
                text,
                suffix,
            } => write!(f, "{prefix}{text}{suffix}"),
            DiagnosticTextStorage::Unsigned {
                prefix,
                value,
                suffix,
            } => write!(f, "{prefix}{value}{suffix}"),
            DiagnosticTextStorage::Text(text) => std::fmt::Display::fmt(text, f),
            DiagnosticTextStorage::Native(value) => std::fmt::Display::fmt(value, f),
            DiagnosticTextStorage::Refusal(refusal) => std::fmt::Display::fmt(refusal, f),
        }
    }
}
impl PartialEq<str> for DiagnosticText {
    fn eq(&self, other: &str) -> bool {
        self.equals_text(other)
    }
}
impl PartialEq<&str> for DiagnosticText {
    fn eq(&self, other: &&str) -> bool {
        self.equals_text(other)
    }
}
impl std::fmt::Debug for DiagnosticText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Preserve String's quoted Debug envelope without materializing text.
        struct Escape<'a, 'b>(&'a mut std::fmt::Formatter<'b>);
        impl std::fmt::Write for Escape<'_, '_> {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                for ch in text.chars() {
                    if ch == '\'' {
                        self.0.write_str("'")?;
                    } else {
                        for escaped in ch.escape_debug() {
                            std::fmt::Write::write_char(self.0, escaped)?;
                        }
                    }
                }
                Ok(())
            }
        }
        f.write_str("\"")?;
        std::fmt::write(&mut Escape(f), format_args!("{self}"))?;
        f.write_str("\"")
    }
}

/// A contiguous operation label. Native labels retain the original invoice;
/// ordinary externally supplied Strings move unchanged. There is deliberately
/// no allocating Clone implementation.
pub struct DiagnosticLabel(DiagnosticLabelStorage);
enum DiagnosticLabelStorage {
    Text(Cow<'static, str>),
    Native {
        pointer: std::ptr::NonNull<()>,
        text: unsafe fn(std::ptr::NonNull<()>) -> *const str,
        release: unsafe fn(std::ptr::NonNull<()>),
    },
}
// SAFETY: native constructors accept only immutable Send+Sync shared storage.
unsafe impl Send for DiagnosticLabel {}
unsafe impl Sync for DiagnosticLabel {}
impl DiagnosticLabel {
    #[doc(hidden)]
    pub fn from_shared<I: Send + Sync + 'static>(
        value: crate::read_memory::ReadShared<String, I>,
    ) -> Self {
        unsafe fn text<I>(pointer: std::ptr::NonNull<()>) -> *const str {
            let owner = std::mem::ManuallyDrop::new(unsafe {
                crate::read_memory::ReadShared::<String, I>::from_raw(pointer.as_ptr().cast())
            });
            owner.as_str() as *const str
        }
        unsafe fn release<I>(pointer: std::ptr::NonNull<()>) {
            drop(unsafe {
                crate::read_memory::ReadShared::<String, I>::from_raw(pointer.as_ptr().cast())
            });
        }
        Self(DiagnosticLabelStorage::Native {
            pointer: std::ptr::NonNull::new(value.into_raw().cast_mut().cast())
                .expect("native label"),
            text: text::<I>,
            release: release::<I>,
        })
    }
    /// Count without materializing, then admit the exact buffer and control
    /// layout on the supplier's existing payer before either allocation.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating."
    )]
    pub fn try_format_with_invoice<I: Send + Sync + 'static>(
        value: impl std::fmt::Display,
        admit: impl FnOnce(usize) -> crate::Result<I>,
    ) -> crate::Result<Self> {
        use crate::read_memory::{NativeRefusal, ReadShared};
        struct Count(usize);
        impl std::fmt::Write for Count {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                self.0 = self.0.checked_add(text.len()).ok_or(std::fmt::Error)?;
                Ok(())
            }
        }
        let mut count = Count(0);
        std::fmt::write(&mut count, format_args!("{value}")).map_err(|_| {
            Error::NativeEngineOther(NativeRefusal::Other("diagnostic label size overflow"))
        })?;
        let layout = std::alloc::Layout::array::<u8>(count.0).map_err(|_| {
            Error::NativeEngineOther(NativeRefusal::Other("diagnostic label layout overflow"))
        })?;
        let bytes = ReadShared::<String, I>::layout()
            .size()
            .checked_add(count.0)
            .ok_or(Error::NativeEngineOther(NativeRefusal::Other(
                "diagnostic label layout overflow",
            )))?;
        let invoice = admit(bytes)?;
        let text = if count.0 == 0 {
            String::new()
        } else {
            let pointer = std::ptr::NonNull::new(unsafe { std::alloc::alloc(layout) }).ok_or(
                Error::NativeEngineOther(NativeRefusal::AllocatorFailed {
                    site: "diagnostic label",
                    layout,
                }),
            )?;
            // SAFETY: exact byte-aligned global allocation, initially empty.
            unsafe { String::from_raw_parts(pointer.as_ptr(), 0, count.0) }
        };
        struct Output(String);
        impl std::fmt::Write for Output {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                if text.len() > self.0.capacity() - self.0.len() {
                    return Err(std::fmt::Error);
                }
                self.0.push_str(text);
                Ok(())
            }
        }
        let mut output = Output(text);
        std::fmt::write(&mut output, format_args!("{value}")).map_err(|_| {
            Error::NativeEngineOther(NativeRefusal::Other(
                "diagnostic label exceeded its admitted size",
            ))
        })?;
        let shared = ReadShared::try_from_invoice_native(output.0, invoice)
            .map_err(Error::NativeEngineOther)?;
        Ok(Self::from_shared(shared))
    }
    pub fn static_text(&self) -> Option<&'static str> {
        match &self.0 {
            DiagnosticLabelStorage::Text(Cow::Borrowed(text)) => Some(*text),
            _ => None,
        }
    }
    /// Explicit terminal materialization, matching the old Cow receiver.
    /// Native producer paths move the label instead of calling this method.
    pub fn into_owned(mut self) -> String {
        match &mut self.0 {
            DiagnosticLabelStorage::Text(text) => {
                std::mem::replace(text, Cow::Borrowed("")).into_owned()
            }
            DiagnosticLabelStorage::Native { pointer, text, .. } => {
                unsafe { &*text(*pointer) }.to_owned()
            }
        }
    }
}
impl Drop for DiagnosticLabelStorage {
    fn drop(&mut self) {
        if let Self::Native {
            pointer, release, ..
        } = self
        {
            unsafe { release(*pointer) };
        }
    }
}
impl From<&'static str> for DiagnosticLabel {
    fn from(text: &'static str) -> Self {
        Self(DiagnosticLabelStorage::Text(Cow::Borrowed(text)))
    }
}
impl From<String> for DiagnosticLabel {
    fn from(text: String) -> Self {
        Self(DiagnosticLabelStorage::Text(Cow::Owned(text)))
    }
}
impl From<Cow<'static, str>> for DiagnosticLabel {
    fn from(text: Cow<'static, str>) -> Self {
        Self(DiagnosticLabelStorage::Text(text))
    }
}
impl AsRef<str> for DiagnosticLabel {
    fn as_ref(&self) -> &str {
        match &self.0 {
            DiagnosticLabelStorage::Text(text) => text,
            // SAFETY: this handle retains the immutable native cell and its
            // original invoice for the entire returned borrow.
            DiagnosticLabelStorage::Native { pointer, text, .. } => unsafe { &*text(*pointer) },
        }
    }
}
impl serde::Serialize for DiagnosticLabel {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_ref())
    }
}
impl std::ops::Deref for DiagnosticLabel {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_ref()
    }
}
impl std::fmt::Display for DiagnosticLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}
impl std::fmt::Debug for DiagnosticLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.as_ref(), f)
    }
}
impl PartialEq<str> for DiagnosticLabel {
    fn eq(&self, other: &str) -> bool {
        self.as_ref() == other
    }
}
impl PartialEq<&str> for DiagnosticLabel {
    fn eq(&self, other: &&str) -> bool {
        self.as_ref() == *other
    }
}
impl PartialEq<String> for DiagnosticLabel {
    fn eq(&self, other: &String) -> bool {
        self.as_ref() == other.as_str()
    }
}

/// The helper's complete exit diagnosis needs no owned text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReaderWaitHelperExit {
    Exited(i32),
    Killed,
    Unknown,
}
impl std::fmt::Display for ReaderWaitHelperExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the writer's acquisition helper ended without the store: {self:?}"
        )
    }
}

/// Distinguishes which callback context produced a callback-active error.
///
/// Contract: final `Display` for callback-active errors must be alloc-free
/// (inline `&'static str` templates, never `kind.to_string()`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CallbackKind {
    Trigger,
    Cron,
}

impl std::fmt::Display for CallbackKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            CallbackKind::Trigger => "trigger",
            CallbackKind::Cron => "cron",
        })
    }
}

struct ContextSetDisplay<'a>(&'a BTreeSet<ContextId>);
impl std::fmt::Display for ContextSetDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("{")?;
        for (index, context) in self.0.iter().enumerate() {
            if index != 0 {
                f.write_str(", ")?;
            }
            write!(f, "{}", context.0)?;
        }
        f.write_str("}")
    }
}

struct ScopeLabelSetDisplay<'a>(&'a BTreeSet<ScopeLabel>);
impl std::fmt::Display for ScopeLabelSetDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("{")?;
        for (index, label) in self.0.iter().enumerate() {
            if index != 0 {
                f.write_str(", ")?;
            }
            f.write_str(label.0.as_str())?;
        }
        f.write_str("}")
    }
}

struct PrincipalDisplay<'a>(&'a Principal);
impl std::fmt::Display for PrincipalDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Principal::System => f.write_str("system"),
            Principal::Agent(id) => write!(f, "agent:{id}"),
            Principal::Human(id) => write!(f, "human:{id}"),
        }
    }
}

struct ContextScopeViolationDisplay<'a> {
    requested: &'a ContextId,
    allowed: &'a BTreeSet<ContextId>,
}
impl std::fmt::Display for ContextScopeViolationDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("row hidden by context scope: requested ")?;
        if self.requested.0 == uuid::Uuid::from_u128(u128::MAX) {
            f.write_str("row has no context")?;
        } else {
            write!(f, "context {}", self.requested.0)?;
        }
        write!(f, ", allowed {}", ContextSetDisplay(self.allowed))
    }
}
fn context_scope_violation_display<'a>(
    requested: &'a ContextId,
    allowed: &'a BTreeSet<ContextId>,
) -> ContextScopeViolationDisplay<'a> {
    ContextScopeViolationDisplay { requested, allowed }
}

struct ScopeLabelViolationDisplay<'a> {
    requested: &'a ScopeLabel,
    allowed: &'a BTreeSet<ScopeLabel>,
}
impl std::fmt::Display for ScopeLabelViolationDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "row hidden by scope label: requested {}, allowed {}",
            self.requested.0,
            ScopeLabelSetDisplay(self.allowed)
        )
    }
}
fn scope_label_violation_display<'a>(
    requested: &'a ScopeLabel,
    allowed: &'a BTreeSet<ScopeLabel>,
) -> ScopeLabelViolationDisplay<'a> {
    ScopeLabelViolationDisplay { requested, allowed }
}

struct AclDeniedDisplay<'a> {
    table: &'a str,
    row_id: &'a RowId,
    principal: &'a Principal,
}
impl std::fmt::Display for AclDeniedDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "row hidden by ACL on table `{}` row {} for principal {}",
            self.table,
            self.row_id,
            PrincipalDisplay(self.principal)
        )
    }
}
fn acl_denied_display<'a>(
    table: &'a str,
    row_id: &'a RowId,
    principal: &'a Principal,
) -> AclDeniedDisplay<'a> {
    AclDeniedDisplay {
        table,
        row_id,
        principal,
    }
}

/// Safe structural reasons a vector partition declaration can be refused.
///
/// These cases deliberately carry no column value or partition-key value.
/// The enclosing error identifies only the vector index; callers can render
/// the fixed reason without accidentally echoing data from a bound parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum VectorPartitionDeclarationIssue {
    #[error("vector partition clauses require a VECTOR column")]
    RequiresVectorColumn,
    #[error("PARTITION_KEY must contain at least one column")]
    EmptyPartitionKey,
    #[error("PARTITION_KEY references a column that does not exist in the same table")]
    UnknownPartitionKeyColumn,
    #[error("every PARTITION_KEY column must be NOT NULL")]
    NullablePartitionKeyColumn,
    #[error("PARTITION_KEY columns must use UUID, TEXT, INTEGER, BOOLEAN, TIMESTAMP, or TXID")]
    UnsupportedPartitionKeyColumnType,
    #[error("a PARTITION_KEY column may appear only once")]
    DuplicatePartitionKeyColumn,
    #[error("PARTITION_KEY may not contain the vector column itself")]
    VectorColumnInPartitionKey,
    #[error("MAX_PARTITIONS requires PARTITION_KEY")]
    MaxPartitionsWithoutPartitionKey,
    #[error("MAX_PARTITIONS must be a positive integer")]
    MaxPartitionsNotPositive,
    #[error("MAX_PARTITIONS is outside the persisted numeric range")]
    MaxPartitionsOutOfRange,
}

/// Safe structural reasons a per-column vector workload policy can be
/// refused. The enclosing error identifies the column; values are omitted so
/// bound or synced input is never echoed through the diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum VectorPolicyDeclarationIssue {
    #[error("vector policy clauses require a VECTOR column")]
    RequiresVectorColumn,
    #[error("AUTO_INDEX_AT must be a positive integer")]
    AutoIndexAtNotPositive,
    #[error("AUTO_INDEX_AT is outside the persisted numeric range")]
    AutoIndexAtOutOfRange,
    #[error("FLAT_BELOW must be a non-negative integer")]
    FlatBelowNegative,
    #[error("FLAT_BELOW is outside the persisted numeric range")]
    FlatBelowOutOfRange,
    #[error("AUTO_EXPANSION_CEILING must be an integer percent from 1 through 100")]
    AutoExpansionCeilingOutOfRange,
    #[error(
        "HNSW SEARCH_RADIUS must be a decimal from 0 through 2 with at most six decimal places"
    )]
    SearchRadiusOutOfRange,
    #[error("HNSW policy values must be positive integers")]
    HnswValueNotPositive,
    #[error("an HNSW policy value is outside the persisted numeric range")]
    HnswValueOutOfRange,
    #[error("HNSW EF_CONSTRUCTION must be at least M")]
    EfConstructionBelowM,
    #[error("vector consolidation percentages must be integers from 1 through 100")]
    ConsolidationPercentOutOfRange,
}

/// Why a `SET STORAGE_COMPACTION` declaration was refused. The SQL statement
/// and the Rust setter refuse the same declarations with the same issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StorageCompactionDeclarationIssue {
    #[error("START_RATIO must be greater than 0 and at most 1")]
    StartRatioOutOfRange,
    #[error("STOP_RATIO must be greater than 0 and at most 1")]
    StopRatioOutOfRange,
    #[error("MAX_SWEEPS must be at least 1")]
    MaxSweepsNotPositive,
    #[error("MAX_SWEEPS is outside the persisted numeric range")]
    MaxSweepsOutOfRange,
    #[error("MIN_INTERVAL must be positive")]
    MinIntervalNotPositive,
    #[error("MIN_INTERVAL is longer than 9223372036854775807 milliseconds")]
    MinIntervalOutOfRange,
    #[error("a clause may appear only once in one declaration")]
    DuplicateClause,
}

/// The engine's own answers.
///
/// This vocabulary is the engine's, and it is free to grow and be reordered:
/// no wire carries it. A caller reading over the local owner channel is still
/// entitled to the same answer an in-process call would have produced, but
/// that entitlement is met by the read channel's own error document, which
/// names each class a read can return under a tag written down in its own
/// source. Adding, reordering, or reshaping a variant here cannot move a byte
/// a reader parses.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A setting was understood but its value is outside the allowed range.
    #[error("invalid {name} value {value}; allowed: {allowed}")]
    InvalidSetting {
        name: &'static str,
        value: String,
        allowed: String,
    },
    /// Engine-produced diagnostic whose admitted backing survives until the
    /// caller destroys it. Application/plugin errors keep their original form.
    #[doc(hidden)]
    #[error(transparent)]
    FundedEngine(FundedEngineError),
    /// Allocation-free native diagnostics with the existing Other wire class.
    #[doc(hidden)]
    #[error("{0}")]
    NativeEngineOther(crate::read_memory::NativeRefusal),
    /// Native cancellation identities are never reused. This is a local,
    /// typed construction refusal rather than a channel or wire condition.
    #[doc(hidden)]
    #[error("native cancellation identity space exhausted")]
    NativeCancellationIdentityExhausted,
    #[error("bounded read session is not implemented")]
    ReadSessionNotImplemented,
    #[error("owner reads did not drain before shutdown deadline")]
    OwnerReadDrainTimeout,
    #[error(
        "event-bus queue write refused after owner quiescence; queued event remains available after reopen"
    )]
    EventBusQueueQuiescent,
    #[error("read operation was cancelled")]
    ReadCancelled,
    /// A writable open asked to bound its wait for starting readers by a
    /// cancellation or a deadline, and this process cannot give it a wait it
    /// can end early. An open that asks for neither still waits.
    #[error("a cancellable or bounded wait for starting readers is not available in this process")]
    ReaderWaitUnsupported,
    /// The helper a writable open's cancellable wait started ended without
    /// handing over the store; the open is refused and holds nothing.
    #[error("the wait for starting readers failed: {0}")]
    ReaderWaitHelperFailed(ReaderWaitHelperExit),
    /// A writable open's acquisition helper could not be confirmed dead after
    /// its wait was ended. This process keeps the store held, so nothing the
    /// helper might still be granted can reach anyone else; a later open or
    /// close of the store re-checks, and process exit releases it.
    #[error(
        "the store at {path} is held by this process until an ended reader wait is confirmed; restart the process if it persists"
    )]
    ReaderWaitQuarantined { path: DiagnosticText },
    #[error("{0}")]
    ReadFailure(crate::read_contract::ReadFailure),
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("{0} are immutable")]
    ImmutableTable(String),
    #[error("column `{column}` on table `{table}` is immutable")]
    ImmutableColumn { table: String, column: String },
    #[error("invalid state transition: {0}")]
    InvalidStateTransition(String),
    #[error("conditional update conflict: {count} staged update(s) no longer matched at commit")]
    ConditionalUpdateConflict { count: u64 },
    #[error("propagation aborted: {table}.{column} transition {from} -> {to} is invalid")]
    PropagationAborted {
        table: String,
        column: String,
        from: String,
        to: String,
    },
    #[error("BFS depth exceeds maximum allowed ({0})")]
    BfsDepthExceeded(u32),
    #[error("BFS visited set exceeded limit ({0})")]
    BfsVisitedExceeded(usize),
    #[error("vector index dimension mismatch on {index}: expected {expected}, got {actual}")]
    VectorIndexDimensionMismatch {
        index: VectorIndexRef,
        expected: usize,
        actual: usize,
    },
    #[error(
        "vector source identity mismatch on {index} row {row_id}: expected created {expected_created_tx}/LSN {expected_lsn}, got created {actual_created_tx:?}/LSN {actual_lsn:?}"
    )]
    VectorSourceIdentityMismatch {
        index: VectorIndexRef,
        row_id: RowId,
        expected_created_tx: TxId,
        expected_lsn: crate::types::Lsn,
        actual_created_tx: Option<TxId>,
        actual_lsn: Option<crate::types::Lsn>,
    },
    #[error("unknown vector index {index}")]
    UnknownVectorIndex { index: VectorIndexRef },
    #[error("invalid vector partition declaration on {index}: {issue}")]
    InvalidVectorPartitionDeclaration {
        index: VectorIndexRef,
        issue: VectorPartitionDeclarationIssue,
    },
    #[error("invalid vector policy declaration on {index}: {issue}")]
    InvalidVectorPolicyDeclaration {
        index: VectorIndexRef,
        issue: VectorPolicyDeclarationIssue,
    },
    #[error("invalid storage compaction declaration: {issue}")]
    InvalidStorageCompactionDeclaration {
        issue: StorageCompactionDeclarationIssue,
    },
    /// A write's PARTITION_KEY value is not of the type its column declares.
    ///
    /// The declaration is sound; the VALUE the caller wrote is not, so this
    /// is deliberately not an `InvalidVectorPartitionDeclaration`, which
    /// would send a developer to inspect a `CREATE TABLE` that has nothing
    /// wrong with it. Only the key column is named: the offending value is
    /// bound or synced input and is never echoed through the diagnostic.
    #[error(
        "vector partition key value refused on {index}: the value written to PARTITION_KEY column `{column}` is not of the column's declared type"
    )]
    VectorPartitionKeyValueTypeMismatch {
        index: VectorIndexRef,
        column: String,
    },
    #[error(
        "vector partition limit reached on {index}: MAX_PARTITIONS is {max_partitions}; raise MAX_PARTITIONS and retry"
    )]
    VectorPartitionLimitExceeded {
        index: VectorIndexRef,
        max_partitions: u32,
    },
    #[error(
        "EXACT vector search on {index} requires {required_bytes} bytes but the active budget has {available_bytes} bytes available; narrow the authorized scope or raise its active limit"
    )]
    VectorExactSearchBudgetExceeded {
        index: VectorIndexRef,
        required_bytes: u64,
        available_bytes: u64,
    },
    #[error(
        "INDEXED vector search on {index} has no complete maintained route; wait for or repair index maintenance"
    )]
    VectorIndexedRouteUnavailable { index: VectorIndexRef },
    #[error(
        "INDEXED vector search on {index} refused: partition {partition} (in scope order) holds live rows its maintained graph does not connect (graph_frontier_disconnected); repair index maintenance, or use AUTO or EXACT"
    )]
    VectorIndexedPartitionDisconnected {
        index: VectorIndexRef,
        partition: u64,
    },
    #[error(
        "filtered vector search on {index} has no bounded relational candidate route for columns {predicate_columns:?}; add a supporting relational index or use EXACT"
    )]
    VectorFilteredRouteUnavailable {
        index: VectorIndexRef,
        predicate_columns: Vec<String>,
    },
    #[error("whole-vector-index inspection requires an unrestricted database handle")]
    VectorWholeIndexInspectionDenied,
    #[error("persisted row vector source row missing on {index}: key {key}")]
    PersistedRowVectorRowMissing { index: VectorIndexRef, key: String },
    #[error("persisted row vector source cell is NULL on {index}: key {key}")]
    PersistedRowVectorCellNull { index: VectorIndexRef, key: String },
    #[error("rank policy on index `{index}` references unknown column `{column}`")]
    RankPolicyColumnUnknown { index: String, column: String },
    #[error(
        "rank policy on index `{index}` references ambiguous column `{column}` (present on both anchor and joined table); rename one of the columns -- the rank-formula grammar does not support table-qualified column references"
    )]
    RankPolicyColumnAmbiguous { index: String, column: String },
    #[error(
        "rank policy on index `{index}` references column `{column}` of type {actual}; expected {expected}"
    )]
    RankPolicyColumnType {
        index: String,
        column: String,
        expected: String,
        actual: String,
    },
    #[error("rank policy on index `{index}` joins unknown table `{table}`")]
    RankPolicyJoinTableUnknown { index: String, table: String },
    #[error(
        "rank policy on index `{index}` joins column `{column}` not present on table `{table}`"
    )]
    RankPolicyJoinColumnUnknown {
        index: String,
        table: String,
        column: String,
    },
    #[error(
        "rank policy on index `{index}` joins column `{joined_table}.{column}` which has no index; add: CREATE INDEX {joined_table}_{column}_idx ON {joined_table}({column});"
    )]
    RankPolicyJoinColumnUnindexed {
        index: String,
        joined_table: String,
        column: String,
    },
    #[error("rank policy sort key `{sort_key}` not found on index `{index}`")]
    RankPolicyNotFound { index: String, sort_key: String },
    #[error(
        "rank policy formula on index `{index}` failed to parse at position {position}: {reason}"
    )]
    RankPolicyFormulaParse {
        index: String,
        position: usize,
        reason: String,
    },
    #[error("USE RANK requires ORDER BY embedding <=> $param in the same query")]
    UseRankRequiresVectorOrder,
    #[error("USE VECTOR requires ORDER BY embedding <=> $param in the same query")]
    UseVectorRequiresVectorOrder,
    #[error("USE RANK requires LIMIT in the same query")]
    UseRankRequiresLimit,
    #[error(
        "{}",
        drop_blocked_rank_policy_display(
            table,
            column,
            dropped_index,
            policy_table,
            policy_column,
            sort_key
        )
    )]
    DropBlockedByRankPolicy {
        table: Box<str>,
        column: Option<Box<str>>,
        dropped_index: Option<Box<str>>,
        policy_table: Box<str>,
        policy_column: Box<str>,
        sort_key: Box<str>,
    },
    #[error(
        "This store was written by an earlier build and is not read. Move it aside and create the store again."
    )]
    LegacyVectorStoreDetected {
        found_format_marker: String,
        expected_release: String,
    },
    #[error("corrupt store at {path}: {reason}")]
    StoreCorrupted {
        path: DiagnosticText,
        reason: DiagnosticText,
    },
    #[error("vector metadata I/O failed during {operation}: {reason}")]
    VectorMetadataIo {
        operation: DiagnosticLabel,
        reason: DiagnosticText,
    },
    #[error(
        "no store at {path}: this open was asked to change a store that already exists, so it created nothing — point it at an existing store, or open it with the creating disposition if a new one is wanted"
    )]
    StoreMissing { path: DiagnosticText },
    #[error(
        "the store's identity could not be proven, so its fingerprint identifies nothing: more than one name resolves to the file at {path}"
    )]
    StoreIdentityUnprovable { path: String },
    #[error("{operation} cannot safely coordinate readers through {path}: {reason}")]
    ReaderCoordinationUnavailable {
        operation: String,
        path: String,
        reason: String,
    },
    #[error(
        "{operation} cannot take a holder-presence lock through {path}: {reason}; this filesystem cannot prove whether a background-work holder is alive"
    )]
    HolderPresenceUnavailable {
        operation: String,
        path: String,
        reason: String,
    },
    #[error("not found: {0}")]
    NotFound(String),
    #[error("transaction not found: {0}")]
    TxNotFound(TxId),
    #[error("unique constraint violation: {table}.{column}")]
    UniqueViolation { table: String, column: String },
    #[error(
        "{}",
        foreign_key_violation_display(child_table, child_columns, parent_table, parent_columns)
    )]
    ForeignKeyViolation {
        child_table: String,
        child_columns: Vec<String>,
        parent_table: String,
        parent_columns: Vec<String>,
    },
    #[error("recursive CTEs are not supported")]
    RecursiveCteNotSupported,
    #[error("window form is not supported: {form}")]
    WindowFunctionNotSupported { form: String },
    #[error("stored procedures/functions are not supported")]
    StoredProcNotSupported,
    #[error("graph traversal requires explicit depth bound")]
    UnboundedTraversal,
    #[error("vector search requires LIMIT clause")]
    UnboundedVectorSearch,
    #[error("subqueries not supported; use CTE chaining")]
    SubqueryNotSupported,
    #[error("full-text search (WHERE column MATCH) is not supported")]
    FullTextSearchNotSupported,
    #[error(
        "ORDER BY requires a column reference, a SELECT-list alias naming one, COALESCE(...), or arithmetic (+, -, *, /); this sort key expression is not supported"
    )]
    OrderByExpressionNotSupported,
    #[error("parse error: {0}")]
    ParseError(String),
    #[error("plan error: {0}")]
    PlanError(String),
    #[error("sync error: {0}")]
    SyncError(Cow<'static, str>),
    #[error("tenant table policy is not declared for {table}")]
    TenantPolicyNotDeclared { table: String },
    #[error("tenant table policy for {table} differs at {clause}")]
    TenantPolicyMismatch { table: String, clause: String },
    #[error("tenant table policy for {table} is already bound")]
    TenantPolicyBound { table: String },
    #[error("table {table} differs from its authenticated binding at {clause}")]
    TableBindingMismatch { table: String, clause: String },
    /// An authenticated authored transaction restates a table this node
    /// declared for itself, with a policy that differs from the declaration.
    /// The declaration cannot change from here and the transaction cannot be
    /// installed in part, so the whole transaction is refused; only its
    /// author can correct it.
    #[error(
        "the explicit local policy for '{table}' conflicts with the arriving declaration at \
         {clause}: local policy {local_policy}; arriving policy {arriving_policy}; the local \
         declaration is immutable once bound, so the whole authored transaction is refused and \
         must be corrected by its author, not retried"
    )]
    DeclaredPolicyPreserved {
        table: String,
        clause: String,
        local_policy: String,
        arriving_policy: String,
    },
    /// A later unit touches a table that an earlier authored transaction in
    /// the same stream was refused for, so it waits behind that refusal
    /// instead of installing out of the author's order or failing the rest
    /// of the stream.
    #[error("table '{table}' is held back behind a refused authored transaction: {refusal}")]
    TableHeldBehindRefusedUnit { table: String, refusal: String },
    #[error("delivery manifest required for root table {table}")]
    ManifestRequired { table: String },
    #[error("delivery manifest is incomplete for root table {table}")]
    ManifestIncomplete { table: String },
    #[error("delivery manifest member from {table} was not written in this transaction")]
    ManifestMemberOutsideTransaction { table: String },
    #[error("tenant table policy declarations must originate at authoritative hub {hub_node_id}")]
    DeclareRequiresAuthoritativeHub { hub_node_id: String },
    #[error("DISCARD is not eligible for {table} with sync direction {direction}")]
    DiscardNotEligible { table: String, direction: String },
    #[error("DISCARD is not permitted on a hub; use PURGE")]
    DiscardNotOnHub,
    #[error(
        "edge discard denied by hub {hub_node_id} for {table} in mode {mode:?}: {pending_count} pending unit(s)"
    )]
    EdgeDiscardDenied {
        hub_node_id: String,
        table: String,
        mode: crate::EdgeDiscardMode,
        pending_count: u64,
    },
    #[error("PURGE must originate at authoritative hub {hub_node_id}; run PURGE there")]
    PurgeRequiresAuthoritativeHub { hub_node_id: String },
    /// A push re-offered a row the hub already knows was deleted -- the SAME
    /// terminated lineage, not a fresh write. The store already agrees (both
    /// sides converged on the delete); refusing the replay is correct, but
    /// it is NOT a definitive push failure the way a constraint violation or
    /// an unreachable endpoint is. A caller driving an unconditional
    /// exit-time push (the CLI's final-flush-on-quit) should treat this as
    /// benign convergence -- exit clean, optional notice -- never as a hard
    /// failure. See [`Error::SyncPushUnconfirmed`] for the sibling
    /// "indeterminate, not failed" contract this mirrors.
    #[error(
        "push replays a lineage the hub already terminated by an accepted delete: {table} key {key:?} (store already agrees; no action needed)"
    )]
    SyncReplayOfAcceptedDelete {
        table: String,
        key: Vec<(String, Value)>,
    },
    /// A peer was asked to serve schema it received from another author, but
    /// it no longer holds that author's exact signed declarations for `table`.
    /// Received schema only ever leaves a peer as its author wrote it -- the
    /// author's statements, order and signatures -- so the peer refuses
    /// rather than restating the migration under its own name.
    #[error("received schema for table {table} cannot be served as its author wrote it: {reason}")]
    ReceivedSchemaAuthorshipUnavailable { table: String, reason: String },
    #[error("PURGE must run as a standalone authoritative statement")]
    PurgeRequiresStandaloneExecution,
    #[error(
        "purge causality fence rejects {table} key {key:?}: lineage root {lineage_root} was permanently purged at frontier {frontier}"
    )]
    PurgeCausalityFence {
        table: String,
        key: Vec<(String, Value)>,
        lineage_root: String,
        frontier: crate::types::Lsn,
    },
    #[error(
        "export snapshot fence rejects {table} key {key:?}: lineage root {lineage_root} was permanently purged at frontier {frontier} after snapshot {snapshot_lsn}"
    )]
    PurgeExportSnapshotFence {
        table: String,
        key: Vec<(String, Value)>,
        lineage_root: String,
        frontier: crate::types::Lsn,
        snapshot_lsn: crate::types::Lsn,
    },
    #[error("table {0} is not sync-eligible (no natural key)")]
    NotSyncEligible(String),
    #[error(
        "cycle detected: inserting {edge_type} edge from {source_node} to {target_node} would create a cycle"
    )]
    CycleDetected {
        edge_type: String,
        source_node: uuid::Uuid,
        target_node: uuid::Uuid,
    },
    #[error("plugin rejected at {hook}: {reason}")]
    PluginRejected { hook: String, reason: String },
    #[error(
        "memory budget exceeded: {subsystem}/{operation} requested {requested_bytes} bytes, {available_bytes} available of {budget_limit_bytes} budget. Hint: {hint}"
    )]
    MemoryBudgetExceeded {
        subsystem: DiagnosticLabel,
        operation: DiagnosticLabel,
        requested_bytes: usize,
        available_bytes: usize,
        budget_limit_bytes: usize,
        hint: DiagnosticLabel,
    },
    #[error(
        "disk budget exceeded: {operation} - file is {current_bytes} bytes, limit is {budget_limit_bytes} bytes. Hint: {hint}"
    )]
    DiskBudgetExceeded {
        operation: String,
        current_bytes: u64,
        budget_limit_bytes: u64,
        hint: String,
    },
    /// Scratch working storage could not complete `create`, `read`, or `write`.
    /// This is an I/O failure, not a refusal of the store's configured disk
    /// budget. Retain the kind without exposing a scratch path or error source.
    #[error("scratch working storage unavailable during {operation}: {io_kind}")]
    ScratchWorkingStorageUnavailable {
        operation: String,
        io_kind: std::io::ErrorKind,
    },
    #[error("column type mismatch: {table}.{column} expected {expected}, got {actual}")]
    ColumnTypeMismatch {
        table: String,
        column: String,
        expected: String,
        actual: String,
    },
    #[error("tx id out of range: {table}.{column} value {value} exceeds max {max}")]
    TxIdOutOfRange {
        table: String,
        column: String,
        value: u64,
        max: u64,
    },
    #[error("tx id overflow: {table} incoming {incoming}")]
    TxIdOverflow { table: String, incoming: u64 },
    #[error("column {table}.{column} is NOT NULL")]
    ColumnNotNullable { table: String, column: String },
    #[error("index not found: {table}.{index}")]
    IndexNotFound { table: String, index: String },
    #[error("duplicate index: {table}.{index}")]
    DuplicateIndex { table: String, index: String },
    #[error("reserved index name: {table}.{name} uses reserved prefix `{prefix}`")]
    ReservedIndexName {
        table: String,
        name: String,
        prefix: String,
    },
    #[error("column not indexable: {table}.{column} has type {column_type:?}")]
    ColumnNotIndexable {
        table: String,
        column: String,
        column_type: crate::table_meta::ColumnType,
    },
    #[error("column in index: {table}.{column} referenced by index {index}")]
    ColumnInIndex {
        table: String,
        column: String,
        index: String,
    },

    #[error(
        "cannot alter {table}.{column}: it is a partition key for vector index {index}; create a replacement table with the desired vector partition key and copy the wanted rows"
    )]
    VectorPartitionKeyInUse {
        table: String,
        column: String,
        index: String,
    },
    #[error("column not found: {table}.{column}")]
    ColumnNotFound { table: String, column: String },
    #[error("database is locked: another process (pid {holder_pid}) holds {path:?}")]
    DatabaseLocked {
        holder_pid: u32,
        path: std::path::PathBuf,
    },
    #[error("{}", context_scope_violation_display(requested, allowed))]
    ContextScopeViolation {
        requested: crate::types::ContextId,
        allowed: std::collections::BTreeSet<crate::types::ContextId>,
    },
    #[error("{}", scope_label_violation_display(requested, allowed))]
    ScopeLabelViolation {
        requested: crate::types::ScopeLabel,
        allowed: std::collections::BTreeSet<crate::types::ScopeLabel>,
    },
    #[error("principal required for read on table `{table}`")]
    PrincipalRequired { table: String },
    #[error("{}", acl_denied_display(table, row_id, principal))]
    AclDenied {
        table: String,
        row_id: RowId,
        principal: Principal,
    },
    #[error("missed cron ticks exceeded policy: {ticks} ticks missed under '{policy}'")]
    MissedTicksExceeded { ticks: u32, policy: String },
    #[error("database is not initialized: trigger callbacks must be registered before {operation}")]
    EngineNotInitialized { operation: String },
    #[error("trigger callback missing: {trigger_name}")]
    TriggerCallbackMissing { trigger_name: String },
    #[error("trigger not declared: {trigger_name}")]
    TriggerNotDeclared { trigger_name: String },
    #[error("trigger callback already registered: {trigger_name}")]
    TriggerAlreadyRegistered { trigger_name: String },
    #[error("trigger cascade depth exceeded for {trigger_name}: depth {depth}")]
    TriggerCascadeDepthExceeded { trigger_name: String, depth: u32 },
    #[error("trigger event is not supported: {event}")]
    TriggerEventUnsupported { event: String },
    #[error("trigger callback failed for {trigger_name}: {reason}")]
    TriggerCallbackFailed {
        trigger_name: String,
        reason: String,
    },
    #[error(
        "sync trigger `{trigger_name}` attempted forbidden effect `{effect}`; INCLUDING SYNC callbacks may only insert relational rows"
    )]
    SyncTriggerEffectNotAllowed {
        trigger_name: String,
        effect: String,
    },
    #[error("trigger operation requires an admin database handle: {operation}")]
    TriggerRequiresAdmin { operation: String },
    #[error("schema invalid: {reason}")]
    SchemaInvalid { reason: String },
    /// Cross-thread retry-safe contention. Post-TriggerActiveSameDBProgress,
    /// ordinary same-DB cross-thread trigger contention waits inside the engine
    /// and proceeds as `Ok`; unrelated cross-DB writers proceed independently.
    /// This typed error remains for cron same-DB contention, callback tx-bound
    /// handles used from the wrong thread, and bounded trigger deadlock-guard
    /// timeouts.
    ///
    /// Canonical callback contract: same-DB trigger Class B waits and
    /// proceeds; unrelated DBs are independent; Class A callback-thread reentry
    /// returns [`Error::CallbackReentry`]; cron same-DB Class B returns this
    /// typed error immediately; if a same-DB trigger wait exceeds the engine's
    /// deadlock-guard deadline of 60 seconds -- a fixed deadline no
    /// environment name can move -- the engine returns this typed error and
    /// emits exactly one `tracing::warn!` with `trigger_name`, `waited_ms`,
    /// and `surface`.
    ///
    /// Greppable cross-process substring (sync-wire, log scrapers): the Display
    /// string contains `"callback active on another thread"` for both kinds.
    // MAINTAINER: there are no em-dashes in the CallbackActiveCrossThread template,
    // but see the CallbackReentry template below -- the em-dash character `—` is
    // U+2014 (NOT ASCII hyphen `-`). The public-API string contract pins exact
    // bytes (cross-process consumers may grep for these strings); preserve every
    // em-dash on any future edit and never substitute `--` or `-`.
    #[error("{kind} callback active on another thread; retry once the callback completes")]
    CallbackActiveCrossThread { kind: CallbackKind },

    /// API misuse. The caller is inside its own callback body. Tx-control from
    /// any `Database` handle is forbidden, and write-control is allowed only
    /// through the supplied tx-bound callback handle. Retrying cannot recover.
    /// See [`Error::CallbackActiveCrossThread`] for the complementary Class B
    /// contract: same-DB trigger contention waits, unrelated DBs proceed
    /// independently, cron B1 remains immediate, and timeouts emit an operator
    /// `tracing::warn!`.
    ///
    /// Greppable substring: the Display string contains `"operation not allowed inside"`
    /// for both kinds.
    // MAINTAINER: the em-dash character `—` below is U+2014 (NOT ASCII hyphen `-`).
    // Public-API string contract pins exact bytes -- preserve the em-dash on any
    // future edit and never substitute `--` or `-`.
    #[error("operation not allowed inside a {kind} callback (API misuse — fix the call site)")]
    CallbackReentry { kind: CallbackKind },
    #[error("export destination already exists: {path:?}")]
    ExportDestinationExists { path: std::path::PathBuf },
    #[error("export I/O failure at {path:?}: {reason}")]
    ExportIo {
        path: std::path::PathBuf,
        reason: String,
    },
    #[error("work ledger: input of job {job_id} is a blob_ref; route it to the blob resolver")]
    InputRequiresBlobResolver { job_id: String },
    /// A job's ledger-carried input copies are gone: they aged out under
    /// `work_inputs`' declared retention window before the job was executed.
    /// Distinct from a job with no ledger inputs at all -- that job's input
    /// reference names a different kind. A worker reaching this must see a
    /// typed refusal and record a failure; it must never execute on
    /// silently-empty input.
    #[error("work ledger: input of job {job_id} expired under work_inputs' retention window")]
    WorkInputExpired { job_id: String },
    /// A push was interrupted after its batch left the edge, and the hub could
    /// not be reached to confirm whether the batch landed. The outcome is
    /// INDETERMINATE — the data may or may not have committed on the hub — so
    /// this is distinct from a definitive [`Error::SyncError`] failure. The
    /// push watermark is left unadvanced; a later push reconciles idempotently.
    #[error("sync push unconfirmed: {detail}")]
    SyncPushUnconfirmed { detail: String },
    /// A compaction's internal handle recycle (close then reopen the same
    /// on-disk file, to release in-process allocator state a file-level
    /// compact does not) closed the old handle successfully but could not
    /// reopen it. The on-disk file itself is untouched by the recycle step
    /// (compaction already finished before this ran), so it stays intact and
    /// openable; this store instance is left closed and must be reopened
    /// fresh by the caller.
    #[error(
        "store handle recycle failed for {path}: {reason} (on-disk file is intact; reopen fresh)"
    )]
    StoreHandleRecycleFailed { path: String, reason: String },
    /// A commit reached durable storage, but moving its prepared image into
    /// memory was interrupted by an unwind. The in-memory state of this
    /// handle no longer matches what is durable, and there is no correct
    /// partial publication to patch it back to, so the handle refuses every
    /// later read and write. Reopening the database recovers the complete
    /// committed state from the durable image.
    #[error(
        "a commit reached durable storage but its in-memory publication was interrupted; \
         this database refuses service until it is reopened, which recovers the committed state"
    )]
    PublicationInterrupted,
    #[error("{0}")]
    Other(DiagnosticText),
}

impl Error {
    /// Borrow the logical diagnosis without releasing its physical owner.
    #[doc(hidden)]
    pub fn engine_diagnostic(&self) -> &Self {
        let mut error = self;
        while let Self::FundedEngine(owner) = error {
            let Some(diagnostic) = owner.diagnostic() else {
                break;
            };
            error = diagnostic;
        }
        error
    }

    pub const fn owner_read_drain_timeout() -> Self {
        Self::OwnerReadDrainTimeout
    }
}

type DiagnosticInspector =
    unsafe fn(std::ptr::NonNull<()>) -> (Option<std::ptr::NonNull<Error>>, bool);

/// An opaque bridge to the engine's already admitted diagnostic storage, or
/// an inline native OS diagnosis. Construction neither allocates nor admits bytes.
#[doc(hidden)]
pub struct FundedEngineError {
    pointer: std::ptr::NonNull<()>,
    inspect: DiagnosticInspector,
    display: unsafe fn(std::ptr::NonNull<()>, &mut std::fmt::Formatter<'_>) -> std::fmt::Result,
    release: unsafe fn(std::ptr::NonNull<()>),
}
impl FundedEngineError {
    /// An allocation-free native OS diagnosis of the existing Other class.
    /// The nonzero errno is scalar data, never a pointer to owned storage.
    /// This companion does not change the owned producer bridge below.
    #[doc(hidden)]
    pub fn from_os_error(errno: std::num::NonZeroI32) -> Self {
        unsafe fn inspect(_: std::ptr::NonNull<()>) -> (Option<std::ptr::NonNull<Error>>, bool) {
            (None, true)
        }
        unsafe fn display(
            code: std::ptr::NonNull<()>,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            let errno = code.addr().get() as i32;
            std::fmt::Display::fmt(&std::io::Error::from_raw_os_error(errno), f)
        }
        unsafe fn release(_: std::ptr::NonNull<()>) {}
        // Strict-provenance scalar encoding. None of these callbacks ever
        // dereferences this address, and signed nonzero values round-trip.
        let scalar = std::num::NonZeroUsize::new(errno.get() as isize as usize)
            .expect("nonzero errno remains nonzero at pointer width");
        Self {
            pointer: std::ptr::NonNull::without_provenance(scalar),
            inspect,
            display,
            release,
        }
    }
    /// Transfer one strong owner of producer-prepared diagnostic storage.
    ///
    /// # Safety
    /// The pointer must own initialized, Send + Sync diagnostic storage whose
    /// complete carrier allocation and control invoices were admitted before
    /// allocation. It must retain the payload's actual producer invoices; this
    /// bridge does not certify a foreign diagnostic's allocation provenance.
    /// `inspect` must return the same live logical Error on every call, or None
    /// only for a deferred diagnostic of the existing Other class. Its boolean
    /// may be true only if the original source owns no unpaid allocation and
    /// its actual producer invoices remain retained through physical free.
    /// A foreign or inherited diagnostic is not qualified by this carrier.
    /// `display`
    /// must render the original diagnosis without copying it. Neither may refer
    /// back to this carrier. `release` must destroy the payload, then free its
    /// storage, then release its invoices exactly once, including on unwind.
    /// No borrowed pointer may outlive this owner. The owner must not contain
    /// this carrier.
    pub unsafe fn from_owned(
        pointer: std::ptr::NonNull<()>,
        inspect: DiagnosticInspector,
        display: unsafe fn(std::ptr::NonNull<()>, &mut std::fmt::Formatter<'_>) -> std::fmt::Result,
        release: unsafe fn(std::ptr::NonNull<()>),
    ) -> Self {
        Self {
            pointer,
            inspect,
            display,
            release,
        }
    }
    fn diagnostic(&self) -> Option<&Error> {
        unsafe {
            (self.inspect)(self.pointer)
                .0
                .map(|pointer| pointer.as_ref())
        }
    }
    /// Source qualification supplied by the actual opaque producer, never
    /// inferred from the carrier's existence or from a later reservation.
    #[doc(hidden)]
    pub fn source_is_qualified(&self) -> bool {
        unsafe { (self.inspect)(self.pointer).1 }
    }
}
// Owned storage requires Send + Sync and shared-only access; native errno is scalar.
unsafe impl Send for FundedEngineError {}
unsafe impl Sync for FundedEngineError {}
impl std::fmt::Debug for FundedEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.diagnostic() {
            Some(error) => std::fmt::Debug::fmt(error, f),
            None => std::fmt::Display::fmt(self, f),
        }
    }
}
impl std::fmt::Display for FundedEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe { (self.display)(self.pointer, f) }
    }
}
impl std::error::Error for FundedEngineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.diagnostic().and_then(std::error::Error::source)
    }
}
impl Drop for FundedEngineError {
    fn drop(&mut self) {
        unsafe { (self.release)(self.pointer) }
    }
}

impl From<crate::read_contract::ReadFailure> for Error {
    fn from(failure: crate::read_contract::ReadFailure) -> Self {
        Self::ReadFailure(failure)
    }
}

struct DropBlockedRankPolicyDisplay<'a> {
    table: &'a str,
    column: &'a Option<Box<str>>,
    dropped_index: &'a Option<Box<str>>,
    policy_table: &'a str,
    policy_column: &'a str,
    sort_key: &'a str,
}
impl std::fmt::Display for DropBlockedRankPolicyDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            table,
            column,
            dropped_index,
            policy_table,
            policy_column,
            sort_key,
        } = self;
        if let Some(column) = column {
            return write!(
                f,
                "cannot drop or rename column `{table}.{column}`: rank policy `{sort_key}` on `{policy_table}.{policy_column}` depends on it"
            );
        }
        if let Some(index) = dropped_index {
            return write!(
                f,
                "cannot drop index `{index}` on `{table}`: rank policy `{sort_key}` on `{policy_table}.{policy_column}` depends on it"
            );
        }
        write!(
            f,
            "cannot drop table `{table}`: rank policy `{sort_key}` on `{policy_table}.{policy_column}` depends on it"
        )
    }
}
fn drop_blocked_rank_policy_display<'a>(
    table: &'a str,
    column: &'a Option<Box<str>>,
    dropped_index: &'a Option<Box<str>>,
    policy_table: &'a str,
    policy_column: &'a str,
    sort_key: &'a str,
) -> DropBlockedRankPolicyDisplay<'a> {
    DropBlockedRankPolicyDisplay {
        table,
        column,
        dropped_index,
        policy_table,
        policy_column,
        sort_key,
    }
}

struct ForeignKeyViolationDisplay<'a> {
    child_table: &'a str,
    child_columns: &'a [String],
    parent_table: &'a str,
    parent_columns: &'a [String],
}
impl std::fmt::Display for ForeignKeyViolationDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "foreign key violation: {}(", self.child_table)?;
        for (index, column) in self.child_columns.iter().enumerate() {
            if index != 0 {
                f.write_str(", ")?;
            }
            f.write_str(column)?;
        }
        write!(f, ") references {}(", self.parent_table)?;
        for (index, column) in self.parent_columns.iter().enumerate() {
            if index != 0 {
                f.write_str(", ")?;
            }
            f.write_str(column)?;
        }
        f.write_str(")")
    }
}
fn foreign_key_violation_display<'a>(
    child_table: &'a str,
    child_columns: &'a [String],
    parent_table: &'a str,
    parent_columns: &'a [String],
) -> ForeignKeyViolationDisplay<'a> {
    ForeignKeyViolationDisplay {
        child_table,
        child_columns,
        parent_table,
        parent_columns,
    }
}

pub type Result<T> = std::result::Result<T, Error>;

// Inline refusal diagnostics must stay allocation-free without further Error growth.
const _: () = assert!(std::mem::size_of::<Error>() <= 160);
