//! Independent controls for the real CheckedBackend read/write fault boundary.
#![cfg(feature = "test-seams")]
use redb::{
    CheckedWriterBackendForTest, NativeBackendError, NativeStorageBackend, StorageBackend,
    StorageError,
};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

#[derive(Debug, Default)]
struct State {
    bytes: redb::backends::InMemoryBackend,
    fail_read: AtomicBool,
    fail_write: AtomicBool,
    fail_sync: AtomicBool,
    reads: AtomicUsize,
    writes: AtomicUsize,
    syncs: AtomicUsize,
}
#[derive(Clone, Debug)]
struct FaultBackend(Arc<State>);
impl StorageBackend for FaultBackend {
    fn len(&self) -> io::Result<u64> {
        self.0.bytes.len()
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.bytes.set_len(len)
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.0.reads.fetch_add(1, Ordering::SeqCst);
        if self.0.fail_read.swap(false, Ordering::SeqCst) {
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        self.0.bytes.read(offset, out)
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.0.writes.fetch_add(1, Ordering::SeqCst);
        if self.0.fail_write.swap(false, Ordering::SeqCst) {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe));
        }
        self.0.bytes.write(offset, data)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.0.syncs.fetch_add(1, Ordering::SeqCst);
        if self.0.fail_sync.swap(false, Ordering::SeqCst) {
            return Err(io::Error::from(io::ErrorKind::Other));
        }
        self.0.bytes.sync_data()
    }
}
impl NativeStorageBackend for FaultBackend {
    fn native_len(&self) -> Result<u64, NativeBackendError> {
        self.len().map_err(NativeBackendError::Io)
    }
    fn native_set_len(&self, len: u64) -> Result<(), NativeBackendError> {
        self.set_len(len).map_err(NativeBackendError::Io)
    }
    fn native_read(&self, offset: u64, out: &mut [u8]) -> Result<(), NativeBackendError> {
        self.read(offset, out).map_err(NativeBackendError::Io)
    }
    fn native_write(&self, offset: u64, data: &[u8]) -> Result<(), NativeBackendError> {
        self.write(offset, data).map_err(NativeBackendError::Io)
    }
    fn native_sync_data(&self) -> Result<(), NativeBackendError> {
        self.sync_data().map_err(NativeBackendError::Io)
    }
}
fn fixture(native: bool) -> (CheckedWriterBackendForTest, Arc<State>, Vec<u8>) {
    let state = Arc::new(State::default());
    let expected = vec![9; contextdb_vector::ivf::storage::PAGE_BYTES];
    state.bytes.set_len(expected.len() as u64).unwrap();
    state.bytes.write(0, &expected).unwrap();
    let backend = FaultBackend(state.clone());
    let checked = if native {
        CheckedWriterBackendForTest::native(backend)
    } else {
        CheckedWriterBackendForTest::legacy(backend)
    };
    (checked, state, expected)
}

fn transient_read_case(native: bool) {
    let (checked, state, expected) = fixture(native);
    let mut out = vec![0; expected.len()];
    state.fail_read.store(true, Ordering::SeqCst);
    assert!(checked.read(0, &mut out).is_err());
    checked
        .read(0, &mut out)
        .expect("one failed read must leave the same backend readable");
    assert_eq!(out, expected);
    assert_eq!(state.reads.load(Ordering::SeqCst), 2);
    checked
        .write(0, &expected)
        .expect("a read fault does not poison writes");
    checked
        .sync_data()
        .expect("a read fault does not poison durability");
}

#[test]
fn transient_legacy_checked_read_retries_on_same_backend() {
    transient_read_case(false);
}

#[test]
fn transient_native_checked_read_retries_on_same_backend() {
    transient_read_case(true);
}

#[test]
fn checked_write_and_durability_faults_remain_sticky() {
    for native in [false, true] {
        for fail_sync in [false, true] {
            let (checked, state, expected) = fixture(native);
            if fail_sync {
                state.fail_sync.store(true, Ordering::SeqCst);
                assert!(checked.sync_data().is_err());
            } else {
                state.fail_write.store(true, Ordering::SeqCst);
                assert!(checked.write(0, &expected).is_err());
            }
            let calls = (
                state.reads.load(Ordering::SeqCst),
                state.writes.load(Ordering::SeqCst),
                state.syncs.load(Ordering::SeqCst),
            );
            let mut out = vec![0; expected.len()];
            assert!(matches!(
                checked.read(0, &mut out),
                Err(StorageError::PreviousIo)
            ));
            assert!(matches!(
                checked.write(0, &expected),
                Err(StorageError::PreviousIo)
            ));
            assert!(matches!(checked.sync_data(), Err(StorageError::PreviousIo)));
            assert_eq!(
                (
                    state.reads.load(Ordering::SeqCst),
                    state.writes.load(Ordering::SeqCst),
                    state.syncs.load(Ordering::SeqCst)
                ),
                calls,
                "a poisoned writer never dispatches further backend I/O"
            );
        }
    }
}
