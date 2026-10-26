//! Private raw observation at one authentic committed root. This is an
//! explicit table visit, not a certificate that every retained source was
//! enumerated. In particular it cannot inventory resident graph/page holders.
use super::ColdCommittedProvider;
use crate::executor::bounded::NativeReadOpenContext;
use crate::owner_read::NativeReadRequest;
use crate::persistence::native_storage_admission::{NativeStorageFailure, StorageAdmissionScope};
use crate::persistence::writer_source::{NativeConfigBytes, NativeConfigFailure};
use contextdb_core::{Error, Lsn, SnapshotId, read_memory::ReadMemoryOwner};

impl ColdCommittedProvider<'_> {}

impl ColdCommittedProvider<'_> {}
