//! Terminal diagnostics retain the original open payer, without std custom-I/O
//! boxes or formatting after a failed acquisition. The public outer class stays
//! at the receiver; this module only owns its existing text fields.
use super::{AbsentStoreAnswer, RedbPersistence, live_reader_holders, physical_admission::WaitIo};
use contextdb_core::error::DiagnosticText;
use contextdb_core::read_contract::{HeldByReadersDetail, ReadFailure};
use contextdb_core::read_memory::{ReadMemoryOwner, ReadPrepared};
use contextdb_core::{Error, Result};
use std::path::Path;

pub(super) struct PreparedOpenDiagnostic {
    pub(super) path: DiagnosticText,
    source: ReadPrepared<OpenIoDiagnostic>,
}
struct OpenIoDiagnostic {
    source: OpenDiagnosticSource,
    corruption: bool,
}
enum OpenDiagnosticSource {
    Wait(WaitIo),
    Database(redb::DatabaseError),
    Panic,
}
impl std::fmt::Display for OpenDiagnosticSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wait(source) => write!(f, "I/O error: {source}"),
            Self::Database(source) => std::fmt::Display::fmt(source, f),
            Self::Panic => f.write_str("metadata/format read panicked; store may be truncated or corrupt"),
        }
    }
}
impl std::fmt::Display for OpenIoDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if matches!(&self.source, OpenDiagnosticSource::Panic) {
            write!(f, "{} — {}", self.source, RedbPersistence::CORRUPT_STORE_NEXT_STEP)
        } else if self.corruption {
            write!(f, "metadata/format could not be read: {} — {}",
                self.source, RedbPersistence::CORRUPT_STORE_NEXT_STEP)
        } else {
            write!(f, "{}{}", super::STORAGE_IO_ERROR_PREFIX, self.source)
        }
    }
}
impl PreparedOpenDiagnostic {
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub(super) fn new(original: ReadMemoryOwner, path: &Path) -> Result<Self> {
        let path = DiagnosticText::try_format(original.clone(), path.display(),
            "reader_wait", "diagnostic_path",
            "Release retained open diagnostics or raise the declared memory limit.")?;
        let source = DiagnosticText::try_prepare_shared(original,
            "reader_wait", "diagnostic_source",
            "Release retained open diagnostics or raise the declared memory limit.")?;
        Ok(Self { path, source })
    }
    pub(super) fn into_storage_error(self, source: WaitIo) -> Error {
        Error::Other(DiagnosticText::from_shared(self.source.publish(
            OpenIoDiagnostic { source: OpenDiagnosticSource::Wait(source), corruption: false })))
    }
    /// The cell and path predate the backend open. Backend-owned allocations
    /// inside `source` remain subject to their actual producer's admission.
    pub(super) fn into_database_error(
        self, source: redb::DatabaseError, path: &Path,
        absent: AbsentStoreAnswer, runtime_dir: Option<&Path>,
    ) -> Error {
        if matches!(&source, redb::DatabaseError::DatabaseAlreadyOpen) {
            return RedbPersistence::held_store_error_in_runtime_dir(path, runtime_dir);
        }
        if absent == AbsentStoreAnswer::Typed && super::open_found_no_store(&source) {
            return Error::StoreMissing { path: self.path };
        }
        let permission = super::open_was_refused_by_permissions(&source);
        if permission {
            let (observed_direct_readers, verified_readers) = live_reader_holders(path, runtime_dir);
            if observed_direct_readers != 0 {
                return Error::ReadFailure(ReadFailure::held_by_readers(HeldByReadersDetail {
                    observed_direct_readers, verified_readers,
                }).expect("held reader observation constructs a valid typed refusal"));
            }
        }
        let registry = matches!(&source,
            redb::DatabaseError::Storage(redb::StorageError::Io(error))
                if error.kind() == std::io::ErrorKind::WouldBlock
                    && display_contains_registry(error));
        let corruption = !permission && !registry;
        // The legacy storage receiver also consumes recorded coordination
        // and disk-growth refusals. Keep its exact precedence and classes;
        // their producer-owned text remains a separate admission gap.
        if !corruption {
            return RedbPersistence::storage_error(source);
        }
        let reason = DiagnosticText::from_shared(self.source.publish(OpenIoDiagnostic {
            source: OpenDiagnosticSource::Database(source), corruption,
        }));
        Error::StoreCorrupted { path: self.path, reason }
    }
    pub(super) fn into_panic_error(self) -> Error {
        Error::StoreCorrupted {
            path: self.path,
            reason: DiagnosticText::from_shared(self.source.publish(OpenIoDiagnostic {
                source: OpenDiagnosticSource::Panic, corruption: true,
            })),
        }
    }
    pub(super) fn into_error(
        self, source: WaitIo, path: &Path, absent: AbsentStoreAnswer, runtime_dir: Option<&Path>,
    ) -> Error {
        let kind = source.kind();
        if absent == AbsentStoreAnswer::Typed && kind == std::io::ErrorKind::NotFound {
            return Error::StoreMissing { path: self.path };
        }
        let permission = kind == std::io::ErrorKind::PermissionDenied;
        if permission {
            let (observed_direct_readers, verified_readers) = live_reader_holders(path, runtime_dir);
            if observed_direct_readers != 0 {
                return Error::ReadFailure(ReadFailure::held_by_readers(HeldByReadersDetail {
                    observed_direct_readers, verified_readers,
                }).expect("held reader observation constructs a valid typed refusal"));
            }
        }
        // Preserve the existing registry uncertainty exception, including a
        // custom source whose text arrives in more than one Display fragment.
        let registry = kind == std::io::ErrorKind::WouldBlock && display_contains_registry(&source);
        let corruption = !permission && !registry;
        let reason = DiagnosticText::from_shared(self.source.publish(OpenIoDiagnostic {
            source: OpenDiagnosticSource::Wait(source), corruption,
        }));
        if corruption {
            Error::StoreCorrupted { path: self.path, reason }
        } else {
            Error::Other(reason)
        }
    }
}

fn display_contains_registry(value: &impl std::fmt::Display) -> bool {
    const NEEDLE: &[u8] = b"authenticated reader snapshot registry is indeterminate";
    struct Search {
        window: [u8; NEEDLE.len()],
        seen: usize,
        found: bool,
    }
    impl std::fmt::Write for Search {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            for byte in text.bytes() {
                self.window.rotate_left(1);
                self.window[NEEDLE.len() - 1] = byte;
                self.seen = self.seen.saturating_add(1);
                self.found |= self.seen >= NEEDLE.len() && self.window.as_slice() == NEEDLE;
            }
            Ok(())
        }
    }
    let mut search = Search { window: [0; NEEDLE.len()], seen: 0, found: false };
    let _ = std::fmt::write(&mut search, format_args!("{value}"));
    search.found
}
