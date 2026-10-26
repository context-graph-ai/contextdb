//! Same-write authenticated metadata loans. Filter with a streamed projection;
//! complete records are borrowed only after selection. No key/value continuation
//! or decoded family population is retained by the cursor.
use super::*;
use crate::memory_accounting::OwnedMemoryReservation;
use crate::persistence::OriginalPersistenceFrame;
use std::io::Read;

pub(super) struct Bytes {
    bytes: Vec<u8>,
    _invoice: OwnedMemoryReservation,
}
impl std::ops::Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Bytes {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn copy(original: &OwnedMemoryReservation, bytes: &[u8]) -> Result<Self> {
        let frame = OriginalPersistenceFrame::new(original)?;
        let bytes = frame.copy_bytes(bytes)?;
        Ok(Self {
            bytes,
            _invoice: frame.into_reservation(),
        })
    }
}
pub(super) struct Source<'a, 't> {
    handle: &'a redb::BoundedByteHandle<'t>,
}
impl Source<'_, '_> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn key_prefix(&self, prefix: &[u8]) -> Result<bool> {
        if self.handle.key_len() < prefix.len() {
            return Ok(false);
        }
        self.handle
            .with_bytes(true, 0..prefix.len(), |key| key == prefix)
            .map_err(RedbPersistence::storage_error)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn key_matches(&self, key: &[u8]) -> Result<bool> {
        self.handle
            .key_matches(key)
            .map_err(RedbPersistence::storage_error)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn object_prefix(&self, id: &mut [u8; 16]) -> Result<bool> {
        if self.handle.key_len() < id.len() {
            return Ok(false);
        }
        self.handle
            .with_bytes(true, 0..16, |bytes| id.copy_from_slice(bytes))
            .map_err(RedbPersistence::storage_error)?;
        Ok(true)
    }

    pub(super) fn value_len(&self) -> usize {
        self.handle.value_len()
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_key<R>(&self, consume: impl FnOnce(&[u8]) -> Result<R>) -> Result<R> {
        self.handle
            .with_bytes(true, 0..self.handle.key_len(), consume)
            .map_err(RedbPersistence::storage_error)?
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn value_window(
        &self,
        range: std::ops::Range<usize>,
        consume: &mut dyn FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        self.handle
            .with_bytes(false, range, consume)
            .map_err(RedbPersistence::storage_error)?
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_record<R>(
        &self,
        consume: impl FnOnce(&[u8], &[u8]) -> Result<R>,
    ) -> Result<R> {
        let (key, value) = self
            .handle
            .materialize()
            .map_err(RedbPersistence::storage_error)?;
        consume(key.value(), value.value())
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn key_is_object(&self, id: &[u8; 16]) -> Result<bool> {
        self.key_matches(id)
    }
    /// The projection uses the actual record schema. CheckedBytes consumes and
    /// validates every u8 without constructing the Vec that it replaces.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn probe<T: crate::persistence::StreamedBlobProjection>(
        &self,
        original: &OwnedMemoryReservation,
    ) -> Result<T> {
        let frame = OriginalPersistenceFrame::new(original)?;
        let mut reader = Reader::new(self.handle);
        // rmp-serde's ReadReader owns scratch for str/bin/ext tokens. Discover
        // its real maximum before constructing it; ordinary Vec<u8> arrays do
        // not cause a record-sized scratch charge or copy.
        let scratch = scan(&mut reader, 0)?;
        frame.reserve(
            scratch
                .checked_mul(3)
                .and_then(|n| n.checked_add(128))
                .ok_or_else(invalid)?,
        )?;
        let mut reader = Reader::new(self.handle);
        let result = RedbPersistence::decode_owned_blob_reader::<T, _>(original, &mut reader);
        if let Some(error) = reader.failure.take() {
            drop(result);
            return Err(error);
        }
        let mut decoded = result?;
        frame.retain(&mut decoded.allocation)?;
        Ok(decoded.value)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn tag(&self, original: &OwnedMemoryReservation) -> Result<Tag> {
        self.probe(original)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn staging(&self, original: &OwnedMemoryReservation) -> Result<Staging> {
        self.probe(original)
    }
}
fn invalid() -> Error {
    Error::Other("blob metadata source has invalid encoded boundaries".into())
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn visit(
    write: &redb::WriteTransaction,
    original: &OwnedMemoryReservation,
    definition: TableDefinition<&[u8], &[u8]>,
    mut consume: impl FnMut(&Source<'_, '_>) -> Result<()>,
) -> Result<()> {
    RedbPersistence::with_original_blob_pages(original, || {
        if !write
            .table_exists(definition)
            .map_err(RedbPersistence::storage_error)?
        {
            return Ok(());
        }
        let _control = OriginalPersistenceFrame::new(original)?;
        // The cursor's parents are paid by Redb page admissions; it keeps page
        // owners and scalar positions, never a copied wide seek key.
        let table = write
            .open_table(definition)
            .map_err(RedbPersistence::storage_error)?;
        let mut cursor = table.cursor_authenticated();
        while let Some(handle) = cursor
            .next()
            .transpose()
            .map_err(RedbPersistence::storage_error)?
        {
            consume(&Source { handle: &handle })?;
        }
        Ok(())
    })
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn get(
    write: &redb::WriteTransaction,
    original: &OwnedMemoryReservation,
    definition: TableDefinition<&[u8], &[u8]>,
    key: &[u8],
) -> Result<Option<Bytes>> {
    let mut found = None;
    visit(write, original, definition, |source| {
        if source.key_matches(key)? {
            found = Some(source.with_record(|_, value| Bytes::copy(original, value))?);
        }
        Ok(())
    })?;
    Ok(found)
}

struct Reader<'a, 't> {
    handle: &'a redb::BoundedByteHandle<'t>,
    position: usize,
    start: usize,
    used: usize,
    buffer: [u8; 4096],
    failure: Option<Error>,
}
impl<'a, 't> Reader<'a, 't> {
    fn new(handle: &'a redb::BoundedByteHandle<'t>) -> Self {
        Self {
            handle,
            position: 0,
            start: 0,
            used: 0,
            buffer: [0; 4096],
            failure: None,
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn number(&mut self, width: usize) -> Result<usize> {
        let mut bytes = [0u8; 8];
        self.read_exact(&mut bytes[8 - width..])
            .map_err(|_| self.failure.take().unwrap_or_else(invalid))?;
        usize::try_from(u64::from_be_bytes(bytes)).map_err(|_| invalid())
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn skip(&mut self, count: usize) -> Result<()> {
        self.position = self
            .position
            .checked_add(count)
            .filter(|end| *end <= self.handle.value_len())
            .ok_or_else(invalid)?;
        Ok(())
    }
}
impl Read for Reader<'_, '_> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if self.position == self.handle.value_len() || output.is_empty() {
            return Ok(0);
        }
        if self.position < self.start || self.position >= self.start + self.used {
            self.start = self.position;
            self.used = (self.handle.value_len() - self.position).min(self.buffer.len());
            if let Err(error) =
                self.handle
                    .with_bytes(false, self.start..self.start + self.used, |bytes| {
                        self.buffer[..self.used].copy_from_slice(bytes)
                    })
            {
                self.failure = Some(RedbPersistence::storage_error(error));
                return Err(std::io::ErrorKind::Other.into());
            }
        }
        let offset = self.position - self.start;
        let count = output.len().min(self.used - offset);
        output[..count].copy_from_slice(&self.buffer[offset..offset + count]);
        self.position += count;
        Ok(count)
    }
}
// Structural preflight only: the unmodified serde schemas still perform all
// type, missing/duplicate-field, enum, byte-element and integer validation.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn scan(reader: &mut Reader<'_, '_>, depth: usize) -> Result<usize> {
    if depth > 1024 {
        return Err(invalid());
    }
    let marker = reader.number(1)?;
    let (children, raw, scratch) = match marker {
        0x00..=0x7f | 0xc0 | 0xc2..=0xc3 | 0xe0..=0xff => (0, 0, 0),
        0x80..=0x8f => ((marker & 15) * 2, 0, 0),
        0x90..=0x9f => (marker & 15, 0, 0),
        0xa0..=0xbf => (0, marker & 31, marker & 31),
        0xc4 | 0xd9 => {
            let n = reader.number(1)?;
            (0, n, n)
        }
        0xc5 | 0xda => {
            let n = reader.number(2)?;
            (0, n, n)
        }
        0xc6 | 0xdb => {
            let n = reader.number(4)?;
            (0, n, n)
        }
        0xc7..=0xc9 => {
            let n = reader.number(1usize << (marker - 0xc7))?;
            (0, n.checked_add(1).ok_or_else(invalid)?, n)
        }
        0xca | 0xce | 0xd2 => (0, 4, 0),
        0xcb | 0xcf | 0xd3 => (0, 8, 0),
        0xcc | 0xd0 => (0, 1, 0),
        0xcd | 0xd1 => (0, 2, 0),
        0xd4..=0xd8 => {
            let n = 1usize << (marker - 0xd4);
            (0, n + 1, n)
        }
        0xdc => (reader.number(2)?, 0, 0),
        0xdd => (reader.number(4)?, 0, 0),
        0xde => (reader.number(2)?.checked_mul(2).ok_or_else(invalid)?, 0, 0),
        0xdf => (reader.number(4)?.checked_mul(2).ok_or_else(invalid)?, 0, 0),
        _ => return Err(invalid()),
    };
    reader.skip(raw)?;
    let mut maximum = scratch;
    for _ in 0..children {
        maximum = maximum.max(scan(reader, depth + 1)?);
    }
    Ok(maximum)
}

#[derive(serde::Deserialize)]
pub(super) struct Tag {
    pub version: u8,
    pub hash: [u8; 32],
    pub generation: u64,
}
#[derive(serde::Deserialize)]
pub(super) struct Staging {
    pub version: u8,
    pub object_id: [u8; 16],
    pub provisional_generation: Option<([u8; 32], u64)>,
}

// Both projections contain only scalars, fixed arrays/tuples and CheckedBytes.
// CheckedBytes visits every u8 without creating or retaining a sequence buffer.
impl crate::persistence::StreamedBlobProjection for Tag {}
impl crate::persistence::StreamedBlobProjection for Staging {}

// The projection types are private to this module. Keep these independent
// rows here so the mapped decoder test reaches their real selected entry
// without broadening a production API or adding an unaudited marker root.
#[cfg(test)]
pub(crate) fn codec_projection_contract_rows() {
    use crate::memory_accounting::MemoryAccountant;
    use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
    use std::io::Write;

    fn with_source<R>(
        original: &OwnedMemoryReservation,
        bytes: &[u8],
        consume: impl FnOnce(&Source<'_, '_>) -> R,
    ) -> R {
        let directory = tempfile::tempdir().unwrap();
        let raw = redb::Database::create(directory.path().join("projection.db")).unwrap();
        let write = raw.begin_write().unwrap();
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new("projection_fixture");
        {
            let mut table = write.open_table(definition).unwrap();
            table.insert(b"record".as_slice(), bytes).unwrap();
        }
        RedbPersistence::with_original_blob_pages(original, || {
            let table = write.open_table(definition).unwrap();
            let mut cursor = table.cursor_authenticated();
            let handle = cursor.next().transpose().unwrap().unwrap();
            Ok(consume(&Source { handle: &handle }))
        })
        .unwrap()
    }

    fn errors<T: crate::persistence::StreamedBlobProjection>(
        original: &OwnedMemoryReservation,
        owner: &crate::memory_accounting::AccountantHandle,
    ) {
        let counter = owner.writer_registration_observation_for_test().4;
        // Native derive visitors, actual missing/duplicate version producers.
        let duplicate = [
            0x82, 0xa7, b'v', b'e', b'r', b's', b'i', b'o', b'n', 1, 0xa7, b'v', b'e', b'r', b's',
            b'i', b'o', b'n', 1,
        ];
        for bytes in [&[0x80][..], &duplicate[..]] {
            let expected = with_source(original, bytes, |source| {
                // Warm only the fixture's authenticated page before measuring the
                // actual probe; its reader scratch and error are recreated below.
                drop(source.probe::<T>(original));
                let baseline = owner.usage().used;
                let native = match rmp_serde::from_read::<_, T>(std::io::Cursor::new(bytes)) {
                    Err(error) => error,
                    Ok(_) => panic!("missing/duplicate version must fail"),
                };
                let native_text_len = native.to_string().len();
                let expected = format!("decode blob repository record: {native}");
                // Discover the actual producer's admission floor, not a guessed
                // cell size or a number copied from a different error variant.
                let (result, producer_requests, count) =
                    oracle::source_requests_for_test(counter, || source.probe::<T>(original));
                let credits = oracle::source_request_credit_for_test();
                assert!(count > 0 && count <= credits.len());
                let floor = *credits[..count].iter().max().unwrap();
                assert_eq!(
                    producer_requests[count - 1].1,
                    native_text_len,
                    "the final native request is the real closed-field text, not a surrogate carrier"
                );
                assert_eq!(
                    credits[count - 1],
                    floor,
                    "one-byte-short calibration must target that actual text producer"
                );
                drop(result);
                assert_eq!(owner.usage().used, baseline);
                for cap in [None, Some(floor)] {
                    owner.set_budget(cap).unwrap();
                    let ((), gap) = oracle::funded_requests(owner, || {
                        let observation = oracle::declaration_observation();
                        let error = match source.probe::<T>(original) {
                            Err(error) => error,
                            Ok(_) => panic!("real derive error must escape"),
                        };
                        assert!(
                            matches!(&error, Error::FundedEngine(source) if source.source_is_qualified())
                        );
                        assert!(!crate::read_image_memory::is_memory_refusal(&error));
                        let retained = observation.physical_receipt();
                        assert!(
                            retained.0 >= native_text_len,
                            "the error's actual text and control remain physically live"
                        );
                        assert!(owner.usage().used >= baseline + retained.0);
                        // Exhaust the remaining headroom after the primary exists.
                        // This secondary refusal must never replace its diagnosis.
                        owner.set_budget(Some(owner.usage().used)).unwrap();
                        assert!(
                            crate::memory_accounting::reserve_preparation_original_native(
                                original,
                                1,
                                "events",
                                "decode_sink_queue_entry",
                                "test secondary refusal"
                            )
                            .is_err()
                        );
                        let used = owner.usage().used;
                        let mut output = std::io::Cursor::new([0u8; 256]);
                        write!(&mut output, "{error}").unwrap();
                        assert_eq!(
                            &output.get_ref()[..output.position() as usize],
                            expected.as_bytes()
                        );
                        let public = Some(error);
                        assert_eq!(
                            owner.usage().used,
                            used,
                            "moving the public error retains its original invoice"
                        );
                        assert_eq!(observation.physical_receipt(), retained);
                        drop(public);
                        assert_eq!(
                            observation.physical_receipt().0,
                            0,
                            "every actual diagnostic allocation must physically free"
                        );
                        assert_eq!(owner.usage().used, baseline);
                        assert!(observation.evidence().complete);
                        drop(observation);
                    });
                    assert_eq!(
                        gap, 0,
                        "text/control admission and last physical free use the same payer"
                    );
                    owner.set_budget(None).unwrap();
                }
                // With one byte less than the authentic producer floor, the
                // denied owning allocation must never enter System.alloc.
                owner.set_budget(Some(floor - 1)).unwrap();
                let ((receipt, complete), gap) = oracle::funded_requests(owner, || {
                    let observation = oracle::declaration_observation();
                    // Existing native request tracing cannot nest its measured
                    // window; declaration evidence observes this exact entry.
                    let result = source.probe::<T>(original);
                    let error = match result {
                        Err(error) => error,
                        Ok(_) => panic!("producer floor must refuse"),
                    };
                    assert!(crate::read_image_memory::is_memory_refusal(&error));
                    drop(error);
                    let receipt = observation.physical_receipt();
                    let complete = observation.evidence().complete;
                    drop(observation);
                    (receipt, complete)
                });
                assert!(complete);
                assert_eq!(receipt.0, 0);
                assert_eq!(
                    gap, 0,
                    "no owning refusal marker is allocated after producer denial"
                );
                assert_eq!(owner.usage().used, baseline);
                // A separate traced decode proves that the denied final text
                // allocation never entered the allocator. The final successful
                // request is the actual closed producer text, admitted at floor.
                let (denied, denied_requests, denied_count) =
                    oracle::source_requests_for_test(counter, || source.probe::<T>(original));
                assert!(
                    matches!(denied, Err(ref error) if crate::read_image_memory::is_memory_refusal(error))
                );
                drop(denied);
                assert_eq!(
                    denied_count + 1,
                    count,
                    "one byte short prevents the actual final producer allocation; no replacement marker"
                );
                for i in 0..denied_count {
                    assert_eq!(
                        (denied_requests[i].1, denied_requests[i].2),
                        (producer_requests[i].1, producer_requests[i].2),
                        "denial preserves only the actual pre-producer requests, with no substitute marker"
                    );
                }
                assert_eq!(owner.usage().used, baseline);
                owner.set_budget(None).unwrap();
                expected
            });
            let baseline = owner.usage().used;
            // Drive the actual public plugin callback/return handoff with a
            // freshly produced closed-field error and the same original payer.
            struct Plugin {
                pending: Mutex<Option<Error>>,
                owner: crate::memory_accounting::AccountantHandle,
                retained: std::sync::atomic::AtomicUsize,
                callbacks: std::sync::atomic::AtomicUsize,
                expected: String,
            }
            impl crate::plugin::DatabasePlugin for Plugin {
                fn pre_commit(
                    &self,
                    _: &contextdb_tx::WriteSet,
                    _: crate::plugin::CommitSource,
                ) -> Result<()> {
                    match self.pending.lock().take() {
                        Some(error) => Err(error),
                        None => Ok(()),
                    }
                }
                fn commit_failed(
                    &self,
                    _: &contextdb_tx::WriteSet,
                    _: crate::plugin::CommitSource,
                    error: &Error,
                ) {
                    assert_eq!(
                        self.owner.usage().used,
                        self.retained.load(Ordering::SeqCst)
                    );
                    assert!(
                        matches!(error, Error::FundedEngine(source) if source.source_is_qualified())
                    );
                    let mut output = std::io::Cursor::new([0u8; 256]);
                    write!(&mut output, "{error}").unwrap();
                    assert_eq!(
                        &output.get_ref()[..output.position() as usize],
                        self.expected.as_bytes()
                    );
                    self.callbacks.fetch_add(1, Ordering::SeqCst);
                }
            }
            let plugin = Arc::new(Plugin {
                pending: Mutex::new(None),
                owner: owner.clone(),
                retained: std::sync::atomic::AtomicUsize::new(0),
                callbacks: std::sync::atomic::AtomicUsize::new(0),
                expected,
            });
            let db = crate::Database::open_memory_with_plugin(plugin.clone()).unwrap();
            db.set_maintenance_policy(crate::MaintenancePolicy::CallerDriven);
            db.execute(
                "CREATE TABLE callback_error (id INT PRIMARY KEY)",
                &HashMap::new(),
            )
            .unwrap();
            db.execute("BEGIN", &HashMap::new()).unwrap();
            db.execute("INSERT INTO callback_error VALUES (1)", &HashMap::new())
                .unwrap();
            let error = with_source(original, bytes, |source| {
                match source.probe::<T>(original) {
                    Err(error) => error,
                    Ok(_) => panic!("derive diagnosis must reach callback"),
                }
            });
            let retained = owner.usage().used;
            assert!(retained > baseline);
            plugin.retained.store(retained, Ordering::SeqCst);
            *plugin.pending.lock() = Some(error);
            let error = db.execute("COMMIT", &HashMap::new()).unwrap_err();
            assert_eq!(plugin.callbacks.load(Ordering::SeqCst), 1);
            assert_eq!(
                owner.usage().used,
                retained,
                "public return keeps the producer invoice after callback"
            );
            assert_eq!(error.to_string(), plugin.expected);
            drop(error);
            assert_eq!(
                owner.usage().used,
                baseline,
                "last caller release refunds the original producer"
            );
            db.close().unwrap();
        }
    }

    fn projection<T: crate::persistence::StreamedBlobProjection>(
        original: &OwnedMemoryReservation,
        bytes: &[u8],
        check: impl FnOnce(&T),
    ) -> (usize, usize) {
        with_source(original, bytes, |source| {
            drop(source.probe::<T>(original).unwrap());
            let ((), peak, live) = oracle::measured(|| {
                let value = source.probe::<T>(original).unwrap();
                check(&value);
                drop(value);
            });
            assert_eq!(live, 0);
            (peak, live)
        })
    }

    let writer = MemoryAccountant::writer_no_limit().unwrap();
    let owner = writer.accountant();
    let funder = writer.prepare_source_funder().unwrap();
    let original = writer.reserve_assembly_original(&funder).unwrap();
    errors::<Tag>(&original, &owner);
    errors::<Staging>(&original, &owner);
    let mut tag_peaks = [0; 2];
    let mut staging_peaks = [0; 2];
    for (i, length) in [4096, 8192].into_iter().enumerate() {
        let tag = BlobTagRecord {
            version: 1,
            name: vec![7; length],
            hash: [3; 32],
            generation: 9,
            format: 0,
            role: BlobTagRole::Servable,
        };
        let mut bytes = rmp_serde::to_vec_named(&tag).unwrap();
        // The actual root must also ignore a large, unknown nested field.
        assert_eq!(bytes[0], 0x86);
        bytes[0] = 0x87;
        bytes.extend(rmp_serde::to_vec("ignored").unwrap());
        bytes.extend(rmp_serde::to_vec(&vec![vec![13u8; length]]).unwrap());
        let native: BlobTagRecord = rmp_serde::from_slice(&bytes).unwrap();
        tag_peaks[i] = projection::<Tag>(&original, &bytes, |value| {
            assert_eq!(value.version, native.version);
            assert_eq!(value.hash, native.hash);
            assert_eq!(value.generation, native.generation);
        })
        .0;
        let staging = BlobImportStaging {
            version: 1,
            object_id: [5; 16],
            start_lsn: 3,
            expected_size: Some(100),
            received_size: 2,
            payload_chunk_count: 1,
            outboard_chunk_count: 1,
            payload_indices: vec![7; length],
            outboard_indices: vec![9; length],
            provisional_generation: Some(([3; 32], 9)),
            adapter_state: vec![11; length],
        };
        let mut bytes = rmp_serde::to_vec_named(&staging).unwrap();
        assert_eq!(bytes[0], 0x8b);
        bytes[0] = 0x8c;
        bytes.extend(rmp_serde::to_vec("ignored").unwrap());
        bytes.extend(rmp_serde::to_vec(&vec![vec![13u8; length]]).unwrap());
        let native: BlobImportStaging = rmp_serde::from_slice(&bytes).unwrap();
        staging_peaks[i] = projection::<Staging>(&original, &bytes, |value| {
            assert_eq!(value.version, native.version);
            assert_eq!(value.object_id, native.object_id);
            assert_eq!(value.provisional_generation, native.provisional_generation);
        })
        .0;
    }
    assert_eq!(
        tag_peaks[0], tag_peaks[1],
        "Tag validates large bytes without a payload copy"
    );
    assert_eq!(
        staging_peaks[0], staging_peaks[1],
        "Staging validates large bytes without a payload copy"
    );
}
