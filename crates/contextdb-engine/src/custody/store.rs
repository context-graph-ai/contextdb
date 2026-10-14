//! Key-indexed committed metadata and touched-record validation.
//! The index owns each decoded record once. A unit reads/clones only its selected keys;
//! admitting a delta never clones or revalidates the historical journal.
use super::{authority::object_digest, canonical::*, records::*};
use contextdb_core::Result;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// A selected metadata answer and the actual requesting read's allocation lease.
/// Keep the lease through use; consuming the answer transfers it explicitly.
pub(crate) struct AdmittedRecords {
    records: Vec<Record>,
    memory: Arc<crate::read_image_memory::ImageMemory>,
}
impl std::ops::Deref for AdmittedRecords {
    type Target = [Record];
    fn deref(&self) -> &Self::Target {
        &self.records
    }
}
impl AdmittedRecords {
    pub(crate) fn memory(&self) -> Arc<crate::read_image_memory::ImageMemory> {
        self.memory.clone()
    }
    pub(crate) fn into_parts(self) -> (Vec<Record>, Arc<crate::read_image_memory::ImageMemory>) {
        (self.records, self.memory)
    }
}
/// One executing custody operation retains every admitted selection until use ends.
pub(crate) struct SelectionHold {
    images: Vec<Arc<crate::read_image_memory::ImageMemory>>,
    credit: contextdb_core::read_memory::ReadCredit,
}
impl SelectionHold {
    pub(crate) fn new(read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Self {
        Self {
            images: Vec::new(),
            credit: contextdb_core::read_memory::ReadCredit::new(read_memory),
        }
    }
    pub(crate) fn retain(&mut self, selected: AdmittedRecords) -> Result<Vec<Record>> {
        if self.images.len() == self.images.capacity() {
            self.credit.try_grow_for(
                std::mem::size_of::<Arc<crate::read_image_memory::ImageMemory>>()
                    + if self.images.is_empty() {
                        crate::sync_types::held::ALLOCATION_BYTES
                    } else {
                        0
                    },
                "reader",
                "retain_custody_selection",
                "Reduce custody metadata selected by this operation.",
            )?;
            self.images.reserve_exact(1);
        }
        let image = selected.memory();
        let (records, _previous_image) = selected.into_parts();
        self.images.push(image);
        Ok(records)
    }
}

struct KeyScratch {
    memory: Arc<crate::read_image_memory::ImageMemory>,
    bytes: usize,
}
impl KeyScratch {
    fn new(memory: Arc<crate::read_image_memory::ImageMemory>, count: usize) -> Result<Self> {
        let bytes = if count == 0 {
            0
        } else {
            count
                .checked_mul(std::mem::size_of::<&String>())
                .and_then(|bytes| bytes.checked_add(crate::sync_types::held::ALLOCATION_BYTES))
                .ok_or_else(invalid)?
        };
        memory.reserve(bytes)?;
        Ok(Self { memory, bytes })
    }
}
impl Drop for KeyScratch {
    fn drop(&mut self) {
        self.memory.release(self.bytes);
    }
}

/// Counts encoded bytes without allocating.
struct EncodedLen(usize);
impl std::io::Write for EncodedLen {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("custody record encoding size overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
/// One answer record, copied under `memory` exactly as every custody answer
/// is: its wire form is admitted, then decoded into the answer's image.
fn copy_record(
    record: &Record,
    memory: &Arc<crate::read_image_memory::ImageMemory>,
) -> Result<Record> {
    let mut count = EncodedLen(0);
    serde::Serialize::serialize(
        record,
        &mut rmp_serde::Serializer::new(&mut count).with_struct_map(),
    )
    .map_err(|_| invalid())?;
    let wire_bytes = count.0;
    let credit = wire_bytes
        .checked_add(crate::sync_types::held::ALLOCATION_BYTES)
        .ok_or_else(invalid)?;
    memory.reserve(credit)?;
    let decoded = (|| {
        let mut bytes = Vec::with_capacity(wire_bytes);
        serde::Serialize::serialize(
            record,
            &mut rmp_serde::Serializer::new(&mut bytes).with_struct_map(),
        )
        .map_err(|_| invalid())?;
        work::decoded(1);
        memory.decode_msgpack::<Record>(&bytes)
    })();
    memory.release(credit);
    decoded
}
fn copy_records<'a>(
    source: impl Iterator<Item = &'a Record> + Clone,
    memory: Arc<crate::read_image_memory::ImageMemory>,
) -> Result<AdmittedRecords> {
    let count = source.clone().count();
    if count != 0 {
        memory.reserve(
            count
                .checked_mul(std::mem::size_of::<Record>())
                .and_then(|bytes| bytes.checked_add(crate::sync_types::held::ALLOCATION_BYTES))
                .ok_or_else(invalid)?,
        )?;
    }
    let mut records = Vec::with_capacity(count);
    for record in source {
        records.push(copy_record(record, &memory)?);
    }
    Ok(AdmittedRecords { records, memory })
}

/// Admitted bytes held by one custody operation, released on drop.
pub(crate) use durable::{DeltaRecords, Scratch as CustodyCharge};
pub(crate) use verified::CompanionMemo;

/// Test-seam measurement of custody-journal work (process-wide).
/// `HYDRATED` counts delivery-journal records loaded into a resident store;
/// `DECODED` counts every custody record decode the store layer performs.
#[cfg(feature = "test-seams")]
pub(crate) mod work {
    use std::sync::atomic::{AtomicU64, Ordering};
    pub(crate) static HYDRATED: AtomicU64 = AtomicU64::new(0);
    pub(crate) static DECODED: AtomicU64 = AtomicU64::new(0);
    pub(crate) static PENDING_EXAMINED: AtomicU64 = AtomicU64::new(0);
    pub(crate) fn pending_examined(n: u64) {
        PENDING_EXAMINED.fetch_add(n, Ordering::Relaxed);
        THREAD_PENDING_EXAMINED.with(|count| count.set(count.get() + n));
    }
    thread_local! {
        static THREAD_PENDING_EXAMINED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }
    /// Pending entries examined on the calling thread only, so concurrent
    /// tests never inflate one test's count.
    pub(crate) fn pending_examined_total() -> u64 {
        THREAD_PENDING_EXAMINED.with(std::cell::Cell::get)
    }
    pub(crate) fn hydrated(n: u64) {
        HYDRATED.fetch_add(n, Ordering::Relaxed);
    }
    pub(crate) fn decoded(n: u64) {
        DECODED.fetch_add(n, Ordering::Relaxed);
        THREAD_DECODED.with(|count| count.set(count.get() + n));
    }
    thread_local! {
        static THREAD_DECODED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }
    /// Decodes performed on the calling thread only.
    pub(crate) fn thread_decoded() -> u64 {
        THREAD_DECODED.with(std::cell::Cell::get)
    }
    thread_local! {
        static THREAD_VALIDATION_DECODED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }
    /// A decode performed by commit-time delta validation on this thread.
    pub(crate) fn validation_decoded(n: u64) {
        decoded(n);
        THREAD_VALIDATION_DECODED.with(|count| count.set(count.get() + n));
    }
    pub(crate) fn thread_validation_decoded() -> u64 {
        THREAD_VALIDATION_DECODED.with(std::cell::Cell::get)
    }
    pub(crate) fn read() -> (u64, u64) {
        (
            HYDRATED.load(Ordering::Relaxed),
            DECODED.load(Ordering::Relaxed),
        )
    }
}
#[cfg(not(feature = "test-seams"))]
pub(crate) mod work {
    #[inline(always)]
    pub(crate) fn hydrated(_: u64) {}
    #[inline(always)]
    pub(crate) fn decoded(_: u64) {}
    #[inline(always)]
    pub(crate) fn pending_examined(_: u64) {}
    #[inline(always)]
    pub(crate) fn validation_decoded(_: u64) {}
}

/// Where a durable store reads: the database's own captured roots, or one root
/// pinned for its whole life (a received unit's detached working image).
#[derive(Clone)]
pub(crate) struct DurableSource {
    pinned: Option<Arc<crate::persistence::writer_source::WriterSnapshot>>,
    /// The writer that funds the companion memo once the first durable read
    /// needs it.
    writer: crate::memory_accounting::AccountantHandle,
}

#[derive(Default)]
pub(crate) struct Store {
    pub records: BTreeMap<String, Record>,
    groups: BTreeMap<String, BTreeSet<String>>,
    local_roots: BTreeSet<uuid::Uuid>,
    pending: BTreeSet<uuid::Uuid>,
    #[cfg(feature = "sync-orchestration")]
    pub hidden_pulls: BTreeMap<String, crate::database::custody_pull::HiddenPullScan>,
    pub publications: Arc<AtomicUsize>,
    /// A file-backed store whose derived group index is complete reads the
    /// delivery journal durably. The maps above then mirror only the bounded
    /// tenant policy and binding records; no delivery record stays resident.
    durable: Option<DurableSource>,
    /// Fixed-size memo of companion checks already passed at a captured root.
    checked: Option<Arc<CompanionMemo>>,
    /// The writer admission for `checked`'s box and shared storage, taken
    /// before they are allocated. Declared after `checked`, so it is released
    /// only once both are freed.
    checked_charge: Option<crate::memory_accounting::OwnedMemoryReservation>,
    /// The captured writer root every durable read of the current call uses.
    read_root: Option<Arc<crate::persistence::writer_source::WriterSnapshot>>,
    pub selected_records: u64,
    pub read_visits: u64,
    pub validation_visits: u64,
}

// A reader may return the last admitted immutable snapshot while its successor
// is durable but not yet published. Compare persisted bytes only outside that
// interval; otherwise a concurrent valid commit would look like corruption.
pub(crate) struct Publication(pub Arc<AtomicUsize>);
impl Publication {
    pub fn begin(counter: &Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter.clone())
    }
}
impl Drop for Publication {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

pub(crate) fn root_group(root: &RowRef) -> String {
    format!("root:{}", hex(&root.bytes()))
}
pub(crate) fn submission_group(id: uuid::Uuid) -> String {
    format!("submission:{id}")
}
fn object_group(digest: &Digest) -> String {
    format!("object:{}", hex(digest))
}
fn certificate_group(digest: &Digest) -> String {
    format!("certificate:{}", hex(digest))
}

fn groups(r: &Record) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let submission = match r {
        Record::Manifest(m) => {
            out.push(root_group(&m.seal.root));
            for row in &m.rows {
                out.push(format!("row:{}", hex(&row.reference.bytes())));
            }
            Some(m.id)
        }
        Record::Terminal { record: t, .. } => {
            out.push(root_group(&t.root));
            out.push(object_group(&super::authority::object_digest_finite(
                1,
                |e| t.bytes_finite(e),
                &t.signature,
            )?));
            Some(t.submission)
        }
        Record::MaterializedOwner(o) => {
            out.push(root_group(&o.source.reference));
            Some(o.submission)
        }
        Record::Root { submission, .. }
        | Record::MemberOwner { submission, .. }
        | Record::Diagnostic { submission, .. }
        | Record::DiagnosticOwner { submission, .. }
        | Record::SourceHistory { submission, .. }
        | Record::PolicyProfile { submission, .. }
        | Record::Order { submission, .. } => Some(*submission),
        Record::Binding(b) => {
            out.push("authority".into());
            out.push(object_group(&super::authority::object_digest_finite(
                0,
                |e| b.bytes_finite(e),
                &b.signature,
            )?));
            None
        }
        Record::ObjectAdmission(a) => {
            out.push(object_group(&a.object_digest));
            out.push(certificate_group(&a.certificate));
            None
        }
        Record::CheckpointFrontier(_) | Record::CheckpointNode(_) | Record::EdgeCheckpoint(_) => {
            None
        }
        Record::Policy(_) => {
            out.push("policies".into());
            None
        }
        Record::Control(_)
        | Record::State(_)
        | Record::Birth(_)
        | Record::Certificate(_)
        | Record::Destination(_) => {
            out.push("authority".into());
            None
        }
    };
    if let Some(id) = submission {
        out.push(submission_group(id));
    }
    Ok(out)
}

/// Measurement only: the refusal one stored record produces on the durable
/// decode path (under an optional image limit) and on the resident load path.
#[cfg(feature = "test-seams")]
#[cfg(feature = "test-seams")]
thread_local! {
    static ADMISSION_PROBE: std::cell::Cell<Option<fn(usize)>> = const { std::cell::Cell::new(None) };
}
/// Measurement only: call `probe` with the operation memory's admitted bytes
/// at every custody admission point on this thread (and at a refusal), so a
/// test can compare them with what the thread has actually allocated.
#[cfg(feature = "test-seams")]
pub(crate) fn set_admission_probe_for_test(probe: Option<fn(usize)>) {
    ADMISSION_PROBE.with(|cell| cell.set(probe));
}
/// The start of commit validation: production-dead outside the seam. The
/// probe receives `usize::MAX` so a test can take its baseline here.
#[inline]
pub(crate) fn admission_probe_start() {
    #[cfg(feature = "test-seams")]
    if let Some(probe) = ADMISSION_PROBE.with(std::cell::Cell::get) {
        probe(usize::MAX);
    }
}
/// Bracket phases reported to the measurement probe: immediately before an
/// admission (`0`), immediately after the allocation it funds (`1`), and at a
/// refusal (`2`).
pub(crate) const PROBE_PRE: u8 = 0;
pub(crate) const PROBE_POST: u8 = 1;
pub(crate) const PROBE_REFUSED: u8 = 2;
#[cfg(feature = "test-seams")]
thread_local! {
    static BRACKET_PROBE: std::cell::Cell<Option<fn(&'static str, u8, usize, usize)>> =
        const { std::cell::Cell::new(None) };
    static REFUSE_AT: std::cell::Cell<Option<(&'static str, usize)>> =
        const { std::cell::Cell::new(None) };
}
/// Measurement only: report every labelled custody admission bracket on this
/// thread as `(label, phase, invoice bytes, operation bytes held)`.
#[cfg(feature = "test-seams")]
pub(crate) fn set_bracket_probe_for_test(probe: Option<fn(&'static str, u8, usize, usize)>) {
    BRACKET_PROBE.with(|cell| cell.set(probe));
}
/// Measurement only: refuse the `occurrence`-th (1-based) admission labelled
/// `label` on this thread, before it reserves anything.
#[cfg(feature = "test-seams")]
pub(crate) fn refuse_at_for_test(at: Option<(&'static str, usize)>) {
    REFUSE_AT.with(|cell| cell.set(at));
}
/// One bracket edge: production-dead outside the measurement seam.
#[inline]
pub(crate) fn bracket(_label: &'static str, _phase: u8, _invoice: usize, _held: usize) {
    #[cfg(feature = "test-seams")]
    if let Some(probe) = BRACKET_PROBE.with(std::cell::Cell::get) {
        probe(_label, _phase, _invoice, _held);
    }
}
/// Whether the measurement seam refuses this occurrence of `label`.
#[inline]
pub(crate) fn refusal_injected(_label: &'static str) -> bool {
    #[cfg(feature = "test-seams")]
    {
        return REFUSE_AT.with(|cell| match cell.get() {
            Some((label, count)) if label == _label => {
                if count <= 1 {
                    cell.set(None);
                    true
                } else {
                    cell.set(Some((label, count - 1)));
                    false
                }
            }
            _ => false,
        });
    }
    #[allow(unreachable_code)]
    false
}
/// The typed refusal of one labelled admission, built from static texts
/// inside the operation's error reserve.
pub(crate) fn injected_refusal(label: &'static str, bytes: usize) -> contextdb_core::Error {
    contextdb_core::Error::MemoryBudgetExceeded {
        subsystem: "custody".into(),
        operation: label.into(),
        requested_bytes: bytes,
        available_bytes: 0,
        budget_limit_bytes: 0,
        hint: "Reduce the custody records this operation reads or raise the memory limit.".into(),
    }
}
/// One admission point: production-dead outside the measurement seam.
#[inline]
pub(crate) fn admission_probe(_held: usize) {
    #[cfg(feature = "test-seams")]
    if let Some(probe) = ADMISSION_PROBE.with(std::cell::Cell::get) {
        probe(_held);
    }
}

/// Measurement only: one transient durable probe of `bytes` stored at `key`
/// under `memory`. Answers the outcome; the probe is dropped before return.
#[cfg(feature = "test-seams")]
pub(crate) fn probe_for_test(
    memory: &Arc<crate::read_image_memory::ImageMemory>,
    key: &str,
    bytes: &[u8],
) -> Result<()> {
    durable::probe_for_test(memory, key, bytes)
}

pub(crate) fn decode_refusals_for_test(
    key: &str,
    bytes: &[u8],
    durable_limit: Option<usize>,
) -> (Option<contextdb_core::Error>, Option<contextdb_core::Error>) {
    let memory = crate::read_image_memory::ImageMemory::new(durable_limit);
    let mut owner = durable::Scratch::new(memory.clone());
    let durable = durable::decode_stored(&memory, &mut owner, key, bytes).err();
    drop(owner);
    let resident = Store::from_entries(BTreeMap::from([(key.to_owned(), bytes.to_vec())])).err();
    (durable, resident)
}

impl Store {
    /// Resident delivery-journal records and an estimate of their owned bytes
    /// (keys, decoded records, encoded bodies and group membership).
    #[cfg(feature = "test-seams")]
    pub(crate) fn resident_delivery_for_test(&self) -> (u64, u64) {
        let mut records = 0u64;
        let mut bytes = 0usize;
        for (key, record) in &self.records {
            if key.starts_with("delivery_") {
                records += 1;
                bytes += key.capacity()
                    + std::mem::size_of::<Record>()
                    + encode(record).map_or(0, |e| e.len());
            }
        }
        for (group, keys) in &self.groups {
            let members = keys.iter().filter(|k| k.starts_with("delivery_"));
            let mut any = false;
            for key in members {
                any = true;
                bytes += key.capacity() + std::mem::size_of::<String>();
            }
            if any {
                bytes += group.capacity() + std::mem::size_of::<String>();
            }
        }
        bytes += (self.local_roots.len() + self.pending.len()) * std::mem::size_of::<uuid::Uuid>();
        (records, bytes as u64)
    }
    pub fn from_entries(entries: BTreeMap<String, Vec<u8>>) -> Result<Self> {
        let mut store = Self::default();
        for (key, bytes) in entries {
            work::decoded(1);
            if key.starts_with("delivery_") {
                work::hydrated(1);
            }
            let record = decode(&bytes)?;
            if record.key()? != key {
                return Err(invalid());
            }
            record.verify()?;
            store.insert(key, record)?;
        }
        for id in store.local_roots.clone() {
            store.refresh_pending(id);
        }
        let empty = BTreeMap::new();
        for record in store.records.values() {
            let view = store.companions(record, &empty)?;
            super::records::validate_record(record, &view)?;
        }
        Ok(store)
    }
    /// A durable view: `tenant_entries` are the tenant policy and binding
    /// records only; every delivery record is read at a captured writer root.
    /// Its companion memo is created after the first durable read that decodes
    /// delivery records, admitted to `writer` before it is allocated, so a
    /// store that never reads its delivery journal holds nothing for it.
    pub fn durable(
        tenant_entries: BTreeMap<String, Vec<u8>>,
        writer: crate::memory_accounting::AccountantHandle,
    ) -> Result<Self> {
        if tenant_entries.keys().any(|key| !key.starts_with("tenant_")) {
            return Err(invalid());
        }
        let mut store = Self::from_entries(tenant_entries)?;
        store.durable = Some(DurableSource {
            pinned: None,
            writer,
        });
        Ok(store)
    }
    /// The companion memo, admitted to the store's writer before it exists.
    pub(crate) fn ensure_companion_memo(&mut self) -> Result<()> {
        if self.checked.is_some() {
            return Ok(());
        }
        let Some(source) = &self.durable else {
            return Ok(());
        };
        let charge = crate::memory_accounting::OwnedMemoryReservation::try_new_for(
            source.writer.clone(),
            CompanionMemo::BYTES,
            "custody",
            "companion_memo",
            "Raise MEMORY_LIMIT to open durable custody reads.",
        )?;
        self.checked_charge = Some(charge);
        self.checked = Some(Arc::new(CompanionMemo::new()));
        Ok(())
    }
    /// A durable view that reads only `root`, whoever asks.
    pub fn durable_pinned(
        root: Arc<crate::persistence::writer_source::WriterSnapshot>,
        writer: crate::memory_accounting::AccountantHandle,
    ) -> Result<Self> {
        let mut store = Self::durable(BTreeMap::new(), writer)?;
        if let Some(source) = store.durable.as_mut() {
            source.pinned = Some(root);
        }
        Ok(store)
    }
    /// The writer bytes this store's companion memo holds admitted.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn companion_memo_charge(&self) -> usize {
        self.checked_charge
            .as_ref()
            .map_or(0, |charge| charge.bytes())
    }
    pub fn is_durable(&self) -> bool {
        self.durable.is_some()
    }
    pub fn pinned_root(&self) -> Option<&Arc<crate::persistence::writer_source::WriterSnapshot>> {
        self.durable
            .as_ref()
            .and_then(|source| source.pinned.as_ref())
    }
    /// Set (or clear) the captured root for the reads of one call.
    pub fn set_read_root(
        &mut self,
        root: Option<Arc<crate::persistence::writer_source::WriterSnapshot>>,
    ) {
        self.read_root = root;
    }
    /// Run one durable read at this call's captured root. A root whose group
    /// index is incomplete refuses with the typed incomplete-index error; the
    /// journal is never loaded whole in its place.
    fn durable_read<T>(
        &mut self,
        read: impl FnOnce(&crate::persistence::custody_source::CustodyReader) -> Result<T>,
    ) -> Result<Option<T>> {
        if self.durable.is_none() {
            return Ok(None);
        }
        let root = self.read_root.clone().ok_or_else(invalid)?;
        let mut found_delivery = false;
        let answer = root
            .with_custody_read(self.checked.clone(), |r| {
                let answer = read(r);
                found_delivery = r.found_delivery();
                answer
            })?
            .map(Some)
            .ok_or_else(crate::persistence::custody_source::incomplete_custody_index);
        // A store that has read delivery records keeps a companion memo for
        // the reads after this one. It is only a cache of passed checks: if
        // the writer refuses its admission, reads simply repeat the checks.
        if found_delivery {
            let _ = self.ensure_companion_memo();
        }
        answer
    }
    fn mirrors(&self, key: &str) -> bool {
        self.durable.is_none() || !key.starts_with("delivery_")
    }
    pub fn insert(&mut self, key: String, record: Record) -> Result<()> {
        if !self.mirrors(&key) {
            return Ok(());
        }
        self.remove(&key)?;
        for group in groups(&record)? {
            self.groups.entry(group).or_default().insert(key.clone());
        }
        let source = match &record {
            Record::Root { submission, .. } => Some(*submission),
            _ => None,
        };
        let terminal = match &record {
            Record::Terminal { edge: true, record } => Some(record.submission),
            _ => None,
        };
        let destination = matches!(&record, Record::Destination(_));
        self.records.insert(key, record);
        if let Some(id) = source {
            self.local_roots.insert(id);
            self.refresh_pending(id);
        }
        if let Some(id) = terminal {
            self.refresh_pending(id);
        }
        if destination {
            for id in self.local_roots.clone() {
                self.refresh_pending(id);
            }
        }
        Ok(())
    }
    pub fn remove(&mut self, key: &str) -> Result<()> {
        if let Some(record) = self.records.remove(key) {
            if let Record::Root { submission, .. } = &record {
                self.local_roots.remove(submission);
                self.pending.remove(submission);
            }
            for group in groups(&record)? {
                if let Some(keys) = self.groups.get_mut(&group) {
                    keys.remove(key);
                    if keys.is_empty() {
                        self.groups.remove(&group);
                    }
                }
            }
        }
        Ok(())
    }
    fn refresh_pending(&mut self, id: uuid::Uuid) {
        if !self.local_roots.contains(&id) {
            return;
        }
        let destination = self
            .records
            .get("delivery_destination.v1")
            .and_then(|r| match r {
                Record::Destination(d) => Some(&d.namespace),
                _ => None,
            });
        let held = self.groups.get(&submission_group(id)).is_some_and(|keys| keys.iter().any(|k| matches!(self.records.get(k), Some(Record::Terminal { edge: true, record: t }) if Some(&t.namespace) == destination)));
        if held {
            self.pending.remove(&id);
        } else {
            self.pending.insert(id);
        }
    }
    pub fn pending(
        &mut self,
        memory: Arc<crate::read_image_memory::ImageMemory>,
        db: &crate::Database,
    ) -> Result<AdmittedRecords> {
        if self.durable.is_some() {
            let durable_memory = memory.clone();
            if let Some(answer) = self.durable_read(|r| durable::pending(r, durable_memory, db))? {
                return Ok(answer);
            }
        }
        copy_records(
            self.pending.iter().filter_map(|id| {
                self.groups.get(&submission_group(*id)).and_then(|keys| {
                    keys.iter()
                        .filter_map(|key| self.records.get(key))
                        .find(|record| matches!(record, Record::Manifest(_)))
                })
            }),
            memory,
        )
    }
    pub fn select(
        &mut self,
        selectors: &[&str],
        memory: Arc<crate::read_image_memory::ImageMemory>,
        db: &crate::Database,
    ) -> Result<AdmittedRecords> {
        if self.durable.is_some() {
            let durable_memory = memory.clone();
            if let Some(answer) =
                self.durable_read(|r| durable::select(r, selectors, durable_memory, db))?
            {
                self.selected_records += answer.len() as u64;
                self.read_visits += answer.len() as u64;
                return Ok(answer);
            }
        }
        // A borrowed key directory avoids copying every selected key body.
        let (count, answer) = {
            let mut slots = 0usize;
            for selector in selectors {
                slots = slots
                    .checked_add(if let Some(group) = selector.strip_prefix('@') {
                        self.groups.get(group).map_or(0, BTreeSet::len)
                    } else {
                        self.records
                            .range::<str, _>((
                                std::ops::Bound::Included(selector.as_str()),
                                std::ops::Bound::Unbounded,
                            ))
                            .take_while(|(key, _)| key.starts_with(selector))
                            .count()
                    })
                    .ok_or_else(invalid)?;
            }
            let scratch = KeyScratch::new(memory.clone(), slots)?;
            let mut keys = Vec::with_capacity(slots);
            for selector in selectors {
                if let Some(group) = selector.strip_prefix('@') {
                    if let Some(found) = self.groups.get(group) {
                        keys.extend(found.iter());
                    }
                } else {
                    keys.extend(
                        self.records
                            .range::<str, _>((
                                std::ops::Bound::Included(selector.as_str()),
                                std::ops::Bound::Unbounded,
                            ))
                            .take_while(|(key, _)| key.starts_with(selector))
                            .map(|(key, _)| key),
                    );
                }
            }
            keys.sort_unstable();
            keys.dedup();
            let count = keys.len();
            let answer = copy_records(
                keys.iter().filter_map(|key| self.records.get(*key)),
                memory.clone(),
            );
            drop(keys);
            drop(scratch);
            (count, answer)
        };
        self.selected_records += count as u64;
        self.read_visits += count as u64;
        answer
    }
    pub fn select_table(
        &mut self,
        table: &str,
        memory: Arc<crate::read_image_memory::ImageMemory>,
        db: &crate::Database,
    ) -> Result<AdmittedRecords> {
        if self.durable.is_some() {
            let durable_memory = memory.clone();
            if let Some((answer, visits)) =
                self.durable_read(|r| durable::select_table(r, table, durable_memory, db))?
            {
                self.read_visits += visits;
                self.selected_records += answer.len() as u64;
                return Ok(answer);
            }
        }
        let relevant = |members: &BTreeSet<String>| {
            members.iter().any(|key| {
            matches!(self.records.get(key), Some(Record::Manifest(m)) if m.seal.root.table == table)
                || matches!(self.records.get(key), Some(Record::Root { life, .. }) if life.original_table == table)
                || matches!(self.records.get(key), Some(Record::Terminal { record, .. }) if record.root.table == table)
        })
        };
        let groups = || {
            self.groups
                .range::<str, _>((
                    std::ops::Bound::Included("submission:"),
                    std::ops::Bound::Unbounded,
                ))
                .take_while(|(group, _)| group.starts_with("submission:"))
        };
        let mut slots = self.groups.get("authority").map_or(0, BTreeSet::len);
        let mut visits = 0u64;
        for (_, members) in groups() {
            visits += members.len() as u64;
            if relevant(members) {
                slots = slots.checked_add(members.len()).ok_or_else(invalid)?;
            }
        }
        let scratch = KeyScratch::new(memory.clone(), slots)?;
        let mut keys = Vec::with_capacity(slots);
        for (_, members) in groups() {
            if relevant(members) {
                keys.extend(members.iter());
            }
        }
        if let Some(authority) = self.groups.get("authority") {
            keys.extend(authority.iter());
        }
        keys.sort_unstable();
        keys.dedup();
        let count = keys.len();
        let answer = copy_records(
            keys.iter().filter_map(|key| self.records.get(*key)),
            memory.clone(),
        );
        drop(keys);
        drop(scratch);
        self.read_visits += visits;
        self.selected_records += count as u64;
        answer
    }
    pub fn select_rows(
        &mut self,
        rows: &[RowRef],
        memory: Arc<crate::read_image_memory::ImageMemory>,
        db: &crate::Database,
    ) -> Result<AdmittedRecords> {
        if self.durable.is_some() {
            let durable_memory = memory.clone();
            if let Some((answer, visits)) =
                self.durable_read(|r| durable::select_rows(r, rows, durable_memory, db))?
            {
                self.read_visits += visits;
                self.selected_records += answer.len() as u64;
                return Ok(answer);
            }
        }
        let mut keys = BTreeSet::new();
        for row in rows {
            for group in [root_group(row), format!("row:{}", hex(&row.bytes()))] {
                if let Some(found) = self.groups.get(&group) {
                    self.read_visits += found.len() as u64;
                    keys.extend(found.iter().cloned());
                }
            }
        }
        let submissions = keys
            .iter()
            .filter_map(|key| match self.records.get(key) {
                Some(Record::Manifest(m)) => Some(m.id),
                Some(Record::Terminal { record, .. }) => Some(record.submission),
                Some(Record::MaterializedOwner(owner)) => Some(owner.submission),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        for submission in submissions {
            if let Some(found) = self.groups.get(&submission_group(submission)) {
                self.read_visits += found.len() as u64;
                keys.extend(found.iter().cloned());
            }
        }
        let mut objects = BTreeSet::new();
        for key in &keys {
            if let Some(Record::Terminal { record, .. }) = self.records.get(key) {
                objects.insert(object_digest(1, &record.bytes()?, &record.signature));
            }
        }
        for object in objects {
            if let Some(found) = self.groups.get(&object_group(&object)) {
                self.read_visits += found.len() as u64;
                keys.extend(found.iter().cloned());
            }
        }
        self.selected_records += keys.len() as u64;
        copy_records(keys.iter().filter_map(|key| self.records.get(key)), memory)
    }
    fn selected_keys(&self, selectors: &[String]) -> BTreeSet<String> {
        let mut keys = BTreeSet::new();
        for selector in selectors {
            if let Some(group) = selector.strip_prefix('@') {
                if let Some(found) = self.groups.get(group) {
                    keys.extend(found.iter().cloned());
                }
            } else {
                keys.extend(
                    self.records
                        .range(selector.clone()..)
                        .take_while(|(key, _)| key.starts_with(selector))
                        .map(|(key, _)| key.clone()),
                );
            }
        }
        keys
    }
    fn companions<'a>(
        &'a self,
        record: &'a Record,
        delta: &'a BTreeMap<String, Option<Record>>,
    ) -> Result<Vec<&'a Record>> {
        let selectors = match record {
            Record::Control(_) => vec!["@authority".into()],
            Record::ObjectAdmission(a) => vec![
                format!("@{}", object_group(&a.object_digest)),
                format!("delivery_peer_evidence.v1.{}", hex(&a.certificate)),
            ],
            Record::Terminal { record: t, .. } => vec![
                format!("@{}", submission_group(t.submission)),
                format!(
                    "@{}",
                    object_group(&object_digest(1, &t.bytes()?, &t.signature))
                ),
            ],
            Record::Root { submission, .. } => vec![format!("@{}", submission_group(*submission))],
            Record::MaterializedOwner(o) => vec![format!("@{}", submission_group(o.submission))],
            _ => Vec::new(),
        };
        let mut keys = self.selected_keys(&selectors);
        for (key, candidate) in delta {
            if let Some(candidate) = candidate {
                let candidate_groups = groups(candidate)?;
                if selectors.iter().any(|s| {
                    if let Some(group) = s.strip_prefix('@') {
                        candidate_groups.iter().any(|g| g == group)
                    } else {
                        key.starts_with(s)
                    }
                }) {
                    keys.insert(key.clone());
                }
            }
        }
        Ok(keys
            .into_iter()
            .filter_map(|key| match delta.get(&key) {
                Some(r) => r.as_ref(),
                None => self.records.get(&key),
            })
            .collect())
    }
    /// Measurement only: visit `group`'s members at the captured root,
    /// funding a long group head from `memory`.
    #[cfg(feature = "test-seams")]
    pub(crate) fn visit_group_for_test(
        &mut self,
        group: &str,
        memory: &crate::read_image_memory::ImageMemory,
    ) -> Result<usize> {
        let mut count = 0usize;
        self.durable_read(|r| {
            r.visit_group(group, memory, |_| {
                count += 1;
                Ok(())
            })
        })?
        .ok_or_else(invalid)?;
        Ok(count)
    }
    pub fn validate_delta(
        &mut self,
        writes: &[(String, Vec<u8>)],
        deletes: &[String],
        memory: Arc<crate::read_image_memory::ImageMemory>,
        db: &crate::Database,
    ) -> Result<(DeltaRecords, CustodyCharge)> {
        if self.durable.is_some() {
            let durable_memory = memory.clone();
            if let Some((delta, visits, retained)) = self
                .durable_read(|r| durable::validate_delta(r, writes, deletes, durable_memory, db))?
            {
                self.validation_visits += visits;
                return Ok((delta, retained));
            }
        }
        let retained = CustodyCharge::new(memory.clone());
        let mut delta = BTreeMap::new();
        for key in deletes.iter().filter(|k| owned_key(k)) {
            delta.insert(key.clone(), None);
        }
        for (key, bytes) in writes.iter().filter(|(k, _)| owned_key(k)) {
            // An unchanged authority certificate is common to every unit. Its dependents
            // are already verified; admitting it again cannot change their validity.
            if self
                .records
                .get(key)
                .is_some_and(|old| encode(old).ok().as_ref() == Some(bytes))
                && !delta.contains_key(key)
            {
                continue;
            }
            work::decoded(1);
            let record = decode(bytes)?;
            if record.key()? != *key {
                return Err(invalid());
            }
            record.verify()?;
            delta.insert(key.clone(), Some(record));
        }
        let mut affected = BTreeSet::new();
        for (key, next) in &delta {
            affected.insert(key.clone());
            for record in self.records.get(key).into_iter().chain(next.as_ref()) {
                for group in groups(record)?.into_iter().filter(|g| {
                    !matches!(record, Record::ObjectAdmission(_)) || !g.starts_with("certificate:")
                }) {
                    if let Some(keys) = self.groups.get(&group) {
                        affected.extend(keys.iter().cloned());
                    }
                }
                if let Record::Certificate(c) = record
                    && let Some(keys) = self.groups.get(&certificate_group(&c.id()?))
                {
                    affected.extend(keys.iter().cloned());
                }
            }
        }
        self.validation_visits += affected.len() as u64;
        for key in affected {
            let record = match delta.get(&key) {
                Some(next) => next.as_ref(),
                None => self.records.get(&key),
            };
            if let Some(record) = record {
                super::records::validate_record(record, &self.companions(record, &delta)?)?;
            }
        }
        // The resident journal answers the same admitted delta type.
        let mut admitted: DeltaRecords = durable::AdmittedKeys::new(memory, "custody_delta");
        for (key, record) in delta {
            admitted.insert_with(key.as_str(), key.len(), || key.clone(), record)?;
        }
        Ok((admitted, retained))
    }
}

/// Results of pure custody checks, memoized in fixed tables that never
/// allocate: a signature check depends only on a record's stored bytes, and a
/// companion check only on the record's key at one immutable captured root.
/// A hit skips repeating the check; a miss (including a slot reused by another
/// entry) repeats it. Nothing here grows with the journal.
mod verified {
    const SLOTS: usize = 1024;
    type Table = parking_lot::Mutex<[[u8; 32]; SLOTS]>;
    static SIGNATURES: Table = parking_lot::const_mutex([[0; 32]; SLOTS]);

    /// One store's companion checks, keyed by captured root and record key.
    /// Allocated once per durable store at a fixed size.
    pub(crate) struct CompanionMemo(parking_lot::Mutex<Box<[[u8; 32]; SLOTS]>>);
    impl CompanionMemo {
        /// The table box plus the shared allocation that holds the memo: what
        /// its owner admits before either exists.
        pub(crate) const BYTES: usize = std::mem::size_of::<[[u8; 32]; SLOTS]>()
            + std::mem::size_of::<Self>()
            + 2 * std::mem::size_of::<usize>()
            + 2 * crate::sync_types::held::ALLOCATION_BYTES;
        pub(crate) fn new() -> Self {
            Self(parking_lot::Mutex::new(Box::new([[0; 32]; SLOTS])))
        }
        pub(crate) fn known(&self, digest: &[u8; 32]) -> bool {
            self.0.lock()[slot(digest)] == *digest
        }
        pub(crate) fn remember(&self, digest: [u8; 32]) {
            self.0.lock()[slot(&digest)] = digest;
        }
    }

    pub(super) fn digest(parts: &[&[u8]]) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        for part in parts {
            hasher.update(&(part.len() as u64).to_be_bytes());
            hasher.update(part);
        }
        *hasher.finalize().as_bytes()
    }
    fn slot(digest: &[u8; 32]) -> usize {
        usize::from(u16::from_le_bytes([digest[0], digest[1]])) % SLOTS
    }
    fn known(table: &Table, digest: &[u8; 32]) -> bool {
        table.lock()[slot(digest)] == *digest
    }
    fn remember(table: &Table, digest: [u8; 32]) {
        table.lock()[slot(&digest)] = digest;
    }
    pub(super) fn signature_known(digest: &[u8; 32]) -> bool {
        known(&SIGNATURES, digest)
    }
    pub(super) fn remember_signature(digest: [u8; 32]) {
        remember(&SIGNATURES, digest)
    }
}

/// The operation's memory as the funder of native signature inputs: each
/// native buffer is admitted to it before the buffer exists, and a refusal
/// keeps the operation's own typed error for the caller.
#[derive(Debug)]
struct VerificationFunds {
    memory: Arc<crate::read_image_memory::ImageMemory>,
    refusal: parking_lot::Mutex<Option<contextdb_core::Error>>,
}
impl contextdb_core::read_memory::ReadMemoryBudget for VerificationFunds {
    fn try_reserve(
        &self,
        bytes: usize,
        _subsystem: &'static str,
        _operation: &'static str,
        _hint: &'static str,
    ) -> Result<()> {
        self.memory.reserve(bytes)
    }
    fn try_reserve_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), contextdb_core::read_memory::NativeRefusal> {
        use contextdb_core::read_memory::NativeRefusal;
        self.memory.reserve(bytes).map_err(|error| {
            let refusal = match &error {
                contextdb_core::Error::MemoryBudgetExceeded {
                    requested_bytes,
                    available_bytes,
                    budget_limit_bytes,
                    ..
                } => NativeRefusal::MemoryBudgetExceeded {
                    subsystem,
                    operation,
                    requested_bytes: *requested_bytes,
                    available_bytes: *available_bytes,
                    budget_limit_bytes: *budget_limit_bytes,
                    hint,
                },
                _ => NativeRefusal::Other("custody verification admission refused"),
            };
            *self.refusal.lock() = Some(error);
            refusal
        })
    }
    fn native_reserve_supported(&self) -> bool {
        true
    }
    fn release(&self, bytes: usize) {
        self.memory.release(bytes);
    }
}

/// The native checks of one stored record, after its charged decode: exact
/// canonical form (a borrowed comparing re-serialization, no buffer), the key
/// the record names, and its signature unless these exact stored bytes were
/// verified before. The key and signature inputs are rendered into native
/// buffers `memory` admits first through the admitted `VerificationFunds`.
fn native_check(
    memory: &Arc<crate::read_image_memory::ImageMemory>,
    key: &str,
    bytes: &[u8],
) -> Result<()> {
    use super::native_selection::Failure;
    let view = super::native_record_codec::decode_record(bytes).map_err(|_| invalid())?;
    // The funder's shared allocation, admitted before it exists.
    let funder_bytes = std::mem::size_of::<VerificationFunds>()
        + 2 * std::mem::size_of::<usize>()
        + crate::sync_types::held::ALLOCATION_BYTES;
    bracket(
        "custody_native_funder",
        PROBE_PRE,
        funder_bytes,
        memory.held(),
    );
    if refusal_injected("custody_native_funder") {
        return Err(injected_refusal("custody_native_funder", funder_bytes));
    }
    memory.reserve(funder_bytes)?;
    let funds = Arc::new(VerificationFunds {
        memory: memory.clone(),
        refusal: parking_lot::Mutex::new(None),
    });
    bracket("custody_native_funder", PROBE_POST, 0, memory.held());
    let owner = contextdb_core::read_memory::ReadMemoryOwner::from_legacy(funds.clone());
    let checked = (|| -> std::result::Result<(), Failure> {
        bracket("custody_native_key", PROBE_PRE, 0, memory.held());
        if refusal_injected("custody_native_key") {
            return Err(Failure::Unqualified(injected_refusal(
                "custody_native_key",
                0,
            )));
        }
        let rendered = view.key_native(&owner)?;
        let matches = rendered.text()? == key;
        drop(rendered);
        bracket("custody_native_key", PROBE_POST, 0, memory.held());
        if !matches {
            return Err(Failure::Invalid(super::native_decode::Invalid::Key));
        }
        let digest = verified::digest(&[key.as_bytes(), bytes]);
        if verified::signature_known(&digest) {
            return Ok(());
        }
        bracket("custody_native_signature", PROBE_PRE, 0, memory.held());
        if refusal_injected("custody_native_signature") {
            return Err(Failure::Unqualified(injected_refusal(
                "custody_native_signature",
                0,
            )));
        }
        view.verify_native(&owner)?;
        bracket("custody_native_signature", PROBE_POST, 0, memory.held());
        verified::remember_signature(digest);
        Ok(())
    })();
    drop(owner);
    let refusal = funds.refusal.lock().take();
    drop(funds);
    memory.release(funder_bytes);
    match checked {
        Ok(()) => Ok(()),
        Err(Failure::Invalid(_)) => Err(invalid()),
        Err(Failure::Unqualified(error)) => Err(error),
        Err(other) => Err(refusal.unwrap_or_else(|| {
            contextdb_core::Error::Other(match other {
                Failure::Refused(refusal) => refusal.to_string(),
                _ => "custody signature verification failed".into(),
            })
        })),
    }
}

/// Durable custody reads for a file-backed store with a complete group index.
/// Every key list, decoded record and scratch copy is admitted to the
/// requesting operation's memory before it is allocated, and released when
/// the operation's scratch drops. Nothing outlives the call except the
/// returned, already-charged answer.
mod durable {
    use super::*;
    use super::{native_check, verified};
    use crate::custody::records::validate_record_owned;
    use crate::persistence::custody_source::CustodyReader;
    use crate::read_image_memory::ImageMemory;
    use crate::sync_types::held::ALLOCATION_BYTES;

    /// Bytes admitted to one owner on an operation's memory, released on drop.
    pub(crate) struct Scratch {
        memory: Arc<ImageMemory>,
        bytes: usize,
    }
    /// A debit detached from one owner, owed to the next: the memory still
    /// holds it. Dropping it un-adopted releases it (the abort path).
    pub(crate) struct Debit {
        memory: Arc<ImageMemory>,
        bytes: usize,
    }
    impl Drop for Debit {
        fn drop(&mut self) {
            self.memory.release(self.bytes);
        }
    }
    impl Scratch {
        pub(super) fn new(memory: Arc<ImageMemory>) -> Self {
            Self { memory, bytes: 0 }
        }
        /// Bytes this owner holds admitted now.
        pub(crate) fn bytes(&self) -> usize {
            self.bytes
        }
        pub(super) fn memory(&self) -> &Arc<ImageMemory> {
            &self.memory
        }
        /// Admit `bytes` under `label` before the allocation they fund. The
        /// caller reports the allocation's completion with [`Self::post`].
        pub(super) fn admit_at(&mut self, label: &'static str, bytes: usize) -> Result<()> {
            bracket(label, PROBE_PRE, bytes, self.memory.held());
            if refusal_injected(label) {
                bracket(label, PROBE_REFUSED, bytes, self.memory.held());
                return Err(injected_refusal(label, bytes));
            }
            if let Err(refused) = self.memory.reserve(bytes) {
                bracket(label, PROBE_REFUSED, bytes, self.memory.held());
                admission_probe(self.memory.held());
                return Err(refused);
            }
            self.bytes = self.bytes.checked_add(bytes).ok_or_else(invalid)?;
            Ok(())
        }
        /// The allocation funded by the last `admit_at(label, ..)` exists.
        pub(super) fn post(&self, label: &'static str) {
            bracket(label, PROBE_POST, 0, self.memory.held());
            admission_probe(self.memory.held());
        }
        /// An admission whose funded allocation follows immediately.
        pub(super) fn admit(&mut self, bytes: usize) -> Result<()> {
            self.admit_at("custody_scratch", bytes)?;
            self.post("custody_scratch");
            Ok(())
        }
        pub(super) fn release(&mut self, bytes: usize) {
            let bytes = bytes.min(self.bytes);
            self.memory.release(bytes);
            self.bytes -= bytes;
        }
        fn reset(&mut self) {
            self.memory.release(self.bytes);
            self.bytes = 0;
        }
        /// Take over `bytes` this owner's memory already holds charged (a
        /// charged decode's result), to release with the rest on drop.
        pub(super) fn adopt(&mut self, bytes: usize) -> Result<()> {
            self.bytes = self.bytes.checked_add(bytes).ok_or_else(invalid)?;
            Ok(())
        }
        /// Detach `bytes` of this owner's charge without a refund.
        pub(super) fn detach(&mut self, bytes: usize) -> Result<Debit> {
            self.bytes = self.bytes.checked_sub(bytes).ok_or_else(invalid)?;
            Ok(Debit {
                memory: self.memory.clone(),
                bytes,
            })
        }
        /// Adopt a detached debit on the same memory, without a reservation.
        pub(super) fn adopt_debit(&mut self, debit: Debit) -> Result<()> {
            if !Arc::ptr_eq(&self.memory, &debit.memory) {
                return Err(invalid());
            }
            self.bytes = self.bytes.checked_add(debit.bytes).ok_or_else(invalid)?;
            std::mem::forget(debit);
            Ok(())
        }
        /// A copy of `key` admitted at exactly its length.
        pub(super) fn own_key(&mut self, label: &'static str, key: &str) -> Result<String> {
            self.admit_at(label, key.len())?;
            let owned = key.to_owned();
            if owned.capacity() > key.len() {
                drop(owned);
                self.release(key.len());
                return Err(invalid());
            }
            self.post(label);
            Ok(owned)
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            self.memory.release(self.bytes);
        }
    }

    /// Bytes of `n` slots of `T` plus one allocation header, checked.
    fn slot_bytes<T>(n: usize) -> Result<usize> {
        n.checked_mul(std::mem::size_of::<T>().max(1))
            .and_then(|bytes| bytes.checked_add(ALLOCATION_BYTES))
            .ok_or_else(invalid)
    }

    /// A vector whose capacity is admitted before it is allocated: exactly
    /// (`with_exact`) or by doubling (`push`), the reallocation overlap
    /// included. The actual capacity is checked against the admitted bound;
    /// a larger capacity is dropped at once and refused. The capacity charge
    /// is kept across `clear` and released only after the buffer is freed.
    pub(crate) struct AdmittedVec<T> {
        items: Vec<T>,
        owner: Scratch,
        label: &'static str,
    }
    impl<T> AdmittedVec<T> {
        pub(super) fn new(memory: Arc<ImageMemory>, label: &'static str) -> Self {
            Self {
                items: Vec::new(),
                owner: Scratch::new(memory),
                label,
            }
        }
        pub(super) fn with_exact(
            memory: Arc<ImageMemory>,
            label: &'static str,
            n: usize,
        ) -> Result<Self> {
            let mut this = Self::new(memory, label);
            if n == 0 {
                return Ok(this);
            }
            this.owner.admit_at(label, slot_bytes::<T>(n)?)?;
            this.items = Vec::with_capacity(n);
            if this.items.capacity() > n {
                this.items = Vec::new();
                return Err(invalid());
            }
            this.owner.post(label);
            Ok(this)
        }
        /// Grow to twice the capacity (at least four), admitting the new
        /// buffer while the old one still exists, then releasing the old one.
        fn grow(&mut self) -> Result<()> {
            let old = self.items.capacity();
            let new = old.checked_mul(2).ok_or_else(invalid)?.max(4);
            let added = slot_bytes::<T>(new)?;
            self.owner.admit_at(self.label, added)?;
            self.items.reserve_exact(new - self.items.len());
            if self.items.capacity() > new {
                // Unfunded surplus: drop the buffer and refuse.
                self.items = Vec::new();
                self.owner.reset();
                return Err(invalid());
            }
            if old != 0 {
                self.owner.release(slot_bytes::<T>(old)?);
            }
            self.owner.post(self.label);
            Ok(())
        }
        pub(super) fn reserve_one(&mut self) -> Result<()> {
            if self.items.len() == self.items.capacity() {
                self.grow()?;
            }
            Ok(())
        }
        /// Push after capacity is admitted; never reallocates unfunded.
        pub(super) fn push(&mut self, value: T) -> Result<()> {
            self.reserve_one()?;
            self.items.push(value);
            Ok(())
        }
        /// Push into already admitted capacity only.
        pub(super) fn push_within(&mut self, value: T) -> Result<()> {
            if self.items.len() == self.items.capacity() {
                return Err(invalid());
            }
            self.items.push(value);
            Ok(())
        }
        pub(super) fn as_slice(&self) -> &[T] {
            &self.items
        }
        pub(super) fn as_mut_slice(&mut self) -> &mut [T] {
            &mut self.items
        }
        pub(super) fn len(&self) -> usize {
            self.items.len()
        }
        pub(super) fn pop(&mut self) -> Option<T> {
            self.items.pop()
        }
        pub(super) fn clear(&mut self) {
            self.items.clear();
        }
        pub(super) fn insert(&mut self, index: usize, value: T) -> Result<()> {
            self.reserve_one()?;
            self.items.insert(index, value);
            Ok(())
        }
        pub(super) fn retain_first_of_runs(&mut self, same: impl FnMut(&mut T, &mut T) -> bool) {
            self.items.dedup_by(same);
        }
        pub(super) fn memory(&self) -> &Arc<ImageMemory> {
            self.owner.memory()
        }
        pub(super) fn into_parts(self) -> (Vec<T>, Scratch) {
            let this = std::mem::ManuallyDrop::new(self);
            // SAFETY: `this` is never dropped; each field is moved out once.
            unsafe { (std::ptr::read(&this.items), std::ptr::read(&this.owner)) }
        }
    }

    /// Owned key strings in an admitted vector, each admitted at exactly its
    /// length to `keys` and released only after the string is dropped.
    pub(crate) struct KeyList {
        items: AdmittedVec<String>,
        keys: Scratch,
    }
    impl KeyList {
        pub(super) fn new(memory: Arc<ImageMemory>, label: &'static str) -> Self {
            Self {
                items: AdmittedVec::new(memory.clone(), label),
                keys: Scratch::new(memory),
            }
        }
        pub(super) fn with_exact(
            memory: Arc<ImageMemory>,
            label: &'static str,
            n: usize,
        ) -> Result<Self> {
            Ok(Self {
                items: AdmittedVec::with_exact(memory.clone(), label, n)?,
                keys: Scratch::new(memory),
            })
        }
        pub(super) fn push(&mut self, key: &str) -> Result<()> {
            self.items.reserve_one()?;
            let owned = self.keys.own_key(self.items.label, key)?;
            self.items.push_within(owned)
        }
        /// Move a key owned by another list's key owner on the same memory.
        pub(super) fn push_owned(&mut self, key: String, debit: Debit) -> Result<()> {
            self.items.reserve_one()?;
            self.keys.adopt_debit(debit)?;
            self.items.push_within(key)
        }
        pub(super) fn pop(&mut self) -> Option<(String, Debit)> {
            let key = self.items.pop()?;
            let debit = self.keys.detach(key.len()).ok()?;
            Some((key, debit))
        }
        pub(super) fn clear(&mut self) {
            let bytes: usize = self.items.as_slice().iter().map(String::len).sum();
            self.items.clear();
            self.keys.release(bytes);
        }
        pub(super) fn sort_dedup(&mut self) {
            self.items.as_mut_slice().sort_unstable();
            let mut removed = 0usize;
            self.items.retain_first_of_runs(|a, b| {
                if a == b {
                    removed += a.len();
                    true
                } else {
                    false
                }
            });
            self.keys.release(removed);
        }
        pub(super) fn as_slice(&self) -> &[String] {
            self.items.as_slice()
        }
        pub(super) fn len(&self) -> usize {
            self.items.len()
        }
        /// Move every key out with its debit into `into`.
        pub(super) fn drain_into(&mut self, into: &mut KeyList) -> Result<()> {
            while let Some((key, debit)) = self.pop() {
                into.push_owned(key, debit)?;
            }
            Ok(())
        }
    }

    /// A key a sorted admitted map can own: its heap bytes, if any.
    pub(crate) trait KeyBytes: Ord {
        fn heap(&self) -> usize;
    }
    impl KeyBytes for String {
        fn heap(&self) -> usize {
            self.len()
        }
    }
    impl KeyBytes for [u8; 32] {
        fn heap(&self) -> usize {
            0
        }
    }

    /// A sorted map without tree nodes: append-only admitted entries and an
    /// admitted index sorted by key. Every admission precedes every move; if
    /// the index growth is refused after the entries grew, the entries' new
    /// capacity stays funded by their capacity charge.
    pub(crate) struct AdmittedKeys<K: KeyBytes, V> {
        entries: AdmittedVec<(K, V)>,
        order: AdmittedVec<u32>,
        keys: Scratch,
    }
    impl<K: KeyBytes, V> AdmittedKeys<K, V> {
        pub(super) fn new(memory: Arc<ImageMemory>, label: &'static str) -> Self {
            Self {
                entries: AdmittedVec::new(memory.clone(), label),
                order: AdmittedVec::new(memory.clone(), label),
                keys: Scratch::new(memory),
            }
        }
        fn search<Q: Ord + ?Sized>(&self, key: &Q) -> std::result::Result<usize, usize>
        where
            K: std::borrow::Borrow<Q>,
        {
            let entries = self.entries.as_slice();
            self.order
                .as_slice()
                .binary_search_by(|index| entries[*index as usize].0.borrow().cmp(key))
        }
        pub(super) fn contains<Q: Ord + ?Sized>(&self, key: &Q) -> bool
        where
            K: std::borrow::Borrow<Q>,
        {
            self.search(key).is_ok()
        }
        pub(super) fn get<Q: Ord + ?Sized>(&self, key: &Q) -> Option<&V>
        where
            K: std::borrow::Borrow<Q>,
        {
            let at = self.search(key).ok()?;
            Some(&self.entries.as_slice()[self.order.as_slice()[at] as usize].1)
        }
        /// Insert `key` (built by `make` after its `heap` bytes are admitted)
        /// unless present. Answers whether it was inserted.
        pub(super) fn insert_with<Q: Ord + ?Sized>(
            &mut self,
            probe: &Q,
            heap: usize,
            make: impl FnOnce() -> K,
            value: V,
        ) -> Result<bool>
        where
            K: std::borrow::Borrow<Q>,
        {
            let at = match self.search(probe) {
                Ok(_) => return Ok(false),
                Err(at) => at,
            };
            let label = self.entries.label;
            if heap != 0 {
                self.keys.admit_at(label, heap)?;
            }
            if let Err(refused) = self.entries.reserve_one() {
                self.keys.release(heap);
                return Err(refused);
            }
            if let Err(refused) = self.order.reserve_one() {
                // The entries' grown capacity stays, funded by its charge.
                self.keys.release(heap);
                return Err(refused);
            }
            let index = u32::try_from(self.entries.len()).map_err(|_| invalid())?;
            let key = make();
            if key.heap() != heap {
                drop(key);
                self.keys.release(heap);
                return Err(invalid());
            }
            if heap != 0 {
                self.keys.post(label);
            }
            self.entries.push_within((key, value))?;
            self.order.insert(at, index)?;
            Ok(true)
        }
        pub(super) fn len(&self) -> usize {
            self.entries.len()
        }
        pub(super) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
            let entries = self.entries.as_slice();
            self.order.as_slice().iter().map(move |index| {
                let (k, v) = &entries[*index as usize];
                (k, v)
            })
        }
        pub(super) fn keys(&self) -> impl Iterator<Item = &K> {
            self.iter().map(|(k, _)| k)
        }
        /// Entries in key order, consumed; every charge is held by the
        /// iterator until it drops.
        pub(super) fn into_sorted(self) -> SortedEntries<K, V> {
            let Self {
                mut entries,
                order,
                keys,
            } = self;
            drop(order);
            entries
                .as_mut_slice()
                .sort_unstable_by(|a, b| a.0.cmp(&b.0));
            let (items, capacity) = entries.into_parts();
            SortedEntries {
                items: items.into_iter(),
                _capacity: capacity,
                _keys: keys,
            }
        }
    }
    impl<K: KeyBytes> AdmittedKeys<K, ()> {
        pub(super) fn insert_key(&mut self, key: &str) -> Result<bool>
        where
            K: std::borrow::Borrow<str> + From<String>,
        {
            self.insert_with(key, key.len(), || K::from(key.to_owned()), ())
        }
    }
    /// Consumed entries in key order, holding their charges until dropped.
    pub(crate) struct SortedEntries<K, V> {
        items: std::vec::IntoIter<(K, V)>,
        _capacity: Scratch,
        _keys: Scratch,
    }
    impl<K, V> Iterator for SortedEntries<K, V> {
        type Item = (K, V);
        fn next(&mut self) -> Option<(K, V)> {
            self.items.next()
        }
    }

    /// The validated records of one commit, keyed and sorted, with their
    /// retained charge.
    pub(crate) type DeltaRecords = AdmittedKeys<String, Option<Record>>;

    /// The bound of one public error built during an operation: a static text
    /// or at most 16 KiB of decoded detail, twice (the decoder's message and
    /// its established rendering are alive together).
    pub(crate) const ERROR_RESERVE: usize = 2 * (16 * 1024 + 64);
    /// The longest decoded detail a returned error carries.
    pub(crate) const ERROR_DETAIL_MAX: usize = 16 * 1024;

    /// The operation's error reserve, admitted at entry by its outermost
    /// owner and released at the public return, where a returned error's
    /// ownership passes to the caller.
    pub(crate) fn error_reserve(memory: &Arc<ImageMemory>) -> Result<Scratch> {
        let mut reserve = Scratch::new(memory.clone());
        reserve.admit_at("custody_error_reserve", ERROR_RESERVE)?;
        reserve.post("custody_error_reserve");
        Ok(reserve)
    }

    /// Decode one stored record into an answer charged to `owner`, once:
    /// the charged decode first (malformed input keeps the established codec
    /// diagnostic), then the native canonical, key and signature checks.
    pub(super) fn decode_stored(
        memory: &Arc<ImageMemory>,
        owner: &mut Scratch,
        key: &str,
        bytes: &[u8],
    ) -> Result<Record> {
        work::decoded(1);
        let (record, charged) = decode_charged(memory, bytes)?;
        owner.adopt(charged)?;
        native_check(memory, key, bytes)?;
        Ok(record)
    }

    /// Decode stored bytes with `memory`'s charged decoder: every string,
    /// list, map and JSON value the decode builds is admitted as it is
    /// allocated. Answers the record and the bytes it holds charged. A
    /// malformed record refuses with the custody codec's established
    /// diagnostic; an admission refusal passes through unchanged.
    pub(super) fn decode_charged(
        memory: &Arc<ImageMemory>,
        bytes: &[u8],
    ) -> Result<(Record, usize)> {
        let before = memory.held();
        bracket("custody_decode", PROBE_PRE, 0, before);
        if refusal_injected("custody_decode") {
            bracket("custody_decode", PROBE_REFUSED, 0, before);
            return Err(injected_refusal("custody_decode", bytes.len()));
        }
        let record = memory
            .decode_msgpack::<Record>(bytes)
            .map_err(established_codec_error)?;
        bracket(
            "custody_decode",
            PROBE_POST,
            memory.held() - before,
            memory.held(),
        );
        admission_probe(memory.held());
        Ok((record, memory.held().saturating_sub(before)))
    }

    /// The custody codec's established diagnostic for a malformed record,
    /// rendered from the charged decoder's own error without decoding again.
    /// The detail is bounded at [`ERROR_DETAIL_MAX`] before it is copied.
    fn established_codec_error(error: contextdb_core::Error) -> contextdb_core::Error {
        match error {
            contextdb_core::Error::SyncError(message) => {
                match message.strip_prefix("MessagePack decode error: ") {
                    Some(detail) => {
                        let mut end = detail.len().min(ERROR_DETAIL_MAX);
                        while !detail.is_char_boundary(end) {
                            end -= 1;
                        }
                        contextdb_core::Error::SyncError(format!(
                            "delivery metadata decode: {}",
                            &detail[..end]
                        ))
                    }
                    None => contextdb_core::Error::SyncError(message),
                }
            }
            other => other,
        }
    }

    /// A transient check of one stored record (canonical, keyed, signed):
    /// charged as it decodes and released when the caller drops it.
    fn probe_stored(memory: &Arc<ImageMemory>, key: &str, bytes: &[u8]) -> Result<Probed> {
        let mut probed = Probed {
            owner: Scratch::new(memory.clone()),
            record: None,
        };
        probed
            .owner
            .admit_at("custody_probe_slot", std::mem::size_of::<Record>())?;
        probed.owner.post("custody_probe_slot");
        let record = decode_stored(memory, &mut probed.owner, key, bytes)?;
        probed.record = Some(record);
        Ok(probed)
    }
    /// One transiently decoded record and its admission. The record drops
    /// before its charge (field order).
    pub(super) struct Probed {
        record: Option<Record>,
        owner: Scratch,
    }
    impl Probed {
        fn record(&self) -> &Record {
            self.record.as_ref().expect("probed record")
        }
    }

    #[cfg(feature = "test-seams")]
    pub(super) fn probe_for_test(memory: &Arc<ImageMemory>, key: &str, bytes: &[u8]) -> Result<()> {
        let _errors = error_reserve(memory)?;
        probe_stored(memory, key, bytes).map(drop)
    }

    /// The committed record at `key`, transiently.
    fn load(r: &CustodyReader, memory: &Arc<ImageMemory>, key: &str) -> Result<Option<Probed>> {
        r.get(key, |value| {
            value
                .map(|bytes| probe_stored(memory, key, bytes))
                .transpose()
        })
    }

    /// The longest companion or caller selector: `delivery_peer_evidence.v1.`
    /// and a 32-byte hex digest.
    const SELECTOR_MAX: usize = 96;

    /// One selector rendered on the stack: `@group` or a key prefix.
    pub(crate) struct Selector {
        bytes: [u8; SELECTOR_MAX],
        len: usize,
    }
    impl Selector {
        pub(crate) fn text(text: &str) -> Result<Self> {
            let mut this = Self {
                bytes: [0; SELECTOR_MAX],
                len: 0,
            };
            this.push(text.as_bytes())?;
            Ok(this)
        }
        fn push(&mut self, part: &[u8]) -> Result<()> {
            let end = self.len.checked_add(part.len()).ok_or_else(invalid)?;
            if end > SELECTOR_MAX {
                return Err(invalid());
            }
            self.bytes[self.len..end].copy_from_slice(part);
            self.len = end;
            Ok(())
        }
        fn push_hex(&mut self, digest: &[u8]) -> Result<()> {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            for byte in digest {
                self.push(&[HEX[usize::from(byte >> 4)], HEX[usize::from(byte & 15)]])?;
            }
            Ok(())
        }
        pub(crate) fn submission(id: uuid::Uuid) -> Result<Self> {
            let mut this = Self::text("@submission:")?;
            let mut buffer = uuid::Uuid::encode_buffer();
            this.push(id.hyphenated().encode_lower(&mut buffer).as_bytes())?;
            Ok(this)
        }
        pub(crate) fn object(digest: &[u8; 32]) -> Result<Self> {
            let mut this = Self::text("@object:")?;
            this.push_hex(digest)?;
            Ok(this)
        }
        pub(crate) fn certificate_key(digest: &[u8; 32]) -> Result<Self> {
            let mut this = Self::text("delivery_peer_evidence.v1.")?;
            this.push_hex(digest)?;
            Ok(this)
        }
        pub(crate) fn as_str(&self) -> &str {
            // Only ASCII is ever pushed except whole `&str` parts.
            std::str::from_utf8(&self.bytes[..self.len]).unwrap_or("")
        }
    }

    /// The companion selectors the resident index answers, on the stack.
    fn companion_selectors(record: &Record) -> Result<([Option<Selector>; 2], usize)> {
        let (first, second) = match record {
            Record::Control(_) => (Some(Selector::text("@authority")?), None),
            Record::ObjectAdmission(a) => (
                Some(Selector::object(&a.object_digest)?),
                Some(Selector::certificate_key(&a.certificate)?),
            ),
            Record::Terminal { record: t, .. } => (
                Some(Selector::submission(t.submission)?),
                Some(Selector::object(
                    &super::super::authority::object_digest_finite(
                        1,
                        |e| t.bytes_finite(e),
                        &t.signature,
                    )?,
                )?),
            ),
            Record::Root { submission, .. } => (Some(Selector::submission(*submission)?), None),
            Record::MaterializedOwner(o) => (Some(Selector::submission(o.submission)?), None),
            _ => (None, None),
        };
        let count = usize::from(first.is_some()) + usize::from(second.is_some());
        Ok(([first, second], count))
    }

    /// Whether `record` (stored at `key`) is a member of `selector`, decided
    /// from its fields without building its group list. Mirrors `groups()`
    /// for the selector families companions use; checked against it by
    /// `joins_parity_for_test`.
    pub(super) fn joins(key: &str, record: &Record, selector: &str) -> Result<bool> {
        let Some(group) = selector.strip_prefix('@') else {
            return Ok(key.starts_with(selector));
        };
        if group == "authority" {
            return Ok(matches!(
                record,
                Record::Binding(_)
                    | Record::Control(_)
                    | Record::State(_)
                    | Record::Birth(_)
                    | Record::Certificate(_)
                    | Record::Destination(_)
            ));
        }
        if let Some(id) = group.strip_prefix("submission:") {
            let submission = match record {
                Record::Manifest(m) => Some(m.id),
                Record::Terminal { record: t, .. } => Some(t.submission),
                Record::MaterializedOwner(o) => Some(o.submission),
                Record::Root { submission, .. }
                | Record::MemberOwner { submission, .. }
                | Record::Diagnostic { submission, .. }
                | Record::DiagnosticOwner { submission, .. }
                | Record::SourceHistory { submission, .. }
                | Record::PolicyProfile { submission, .. }
                | Record::Order { submission, .. } => Some(*submission),
                _ => None,
            };
            let mut buffer = uuid::Uuid::encode_buffer();
            return Ok(submission.is_some_and(|s| s.hyphenated().encode_lower(&mut buffer) == id));
        }
        if let Some(hex_digest) = group.strip_prefix("object:") {
            let digest = match record {
                Record::Binding(b) => Some(super::super::authority::object_digest_finite(
                    0,
                    |e| b.bytes_finite(e),
                    &b.signature,
                )?),
                Record::Terminal { record: t, .. } => {
                    Some(super::super::authority::object_digest_finite(
                        1,
                        |e| t.bytes_finite(e),
                        &t.signature,
                    )?)
                }
                Record::ObjectAdmission(a) => Some(a.object_digest),
                _ => None,
            };
            return Ok(digest.is_some_and(|d| {
                Selector::object(&d).is_ok_and(|s| &s.as_str()[8..] == hex_digest)
            }));
        }
        Ok(false)
    }

    /// Visit the keys a selector names.
    fn visit_selector(
        r: &CustodyReader,
        selector: &str,
        memory: &ImageMemory,
        visit: &mut dyn FnMut(&str) -> Result<()>,
    ) -> Result<()> {
        match selector.strip_prefix('@') {
            Some(group) => r.visit_group(group, memory, |key| visit(key)),
            None => r.visit_prefix(selector, |key, _| visit(key)),
        }
    }

    /// Admitted, sorted and deduplicated key list for `selectors`.
    fn selected_keys(
        r: &CustodyReader,
        selectors: &[&str],
        memory: &Arc<ImageMemory>,
    ) -> Result<KeyList> {
        let mut count = 0usize;
        for selector in selectors {
            visit_selector(r, selector, memory, &mut |_| {
                count = count.checked_add(1).ok_or_else(invalid)?;
                Ok(())
            })?;
        }
        let mut keys = KeyList::with_exact(memory.clone(), "custody_selected_keys", count)?;
        for selector in selectors {
            visit_selector(r, selector, memory, &mut |key| {
                if keys.len() == count {
                    return Err(invalid());
                }
                keys.push(key)
            })?;
        }
        keys.sort_dedup();
        Ok(keys)
    }

    /// The answer under construction: its slots and decoded bodies are
    /// charged to `owner`, which detaches into the answer on success and
    /// releases everything (records first) on any failure.
    struct AnswerBuild {
        records: Vec<Record>,
        owner: Scratch,
    }

    /// Decode `keys` into one answer charged to `memory`, in key order, and
    /// validate its whole evidence closure.
    fn answer(
        r: &CustodyReader,
        keys: &[String],
        memory: Arc<ImageMemory>,
        db: &crate::Database,
    ) -> Result<AdmittedRecords> {
        let mut build = AnswerBuild {
            records: Vec::new(),
            owner: Scratch::new(memory.clone()),
        };
        if !keys.is_empty() {
            build
                .owner
                .admit_at("custody_answer_slots", slot_bytes::<Record>(keys.len())?)?;
            build.records = Vec::with_capacity(keys.len());
            if build.records.capacity() > keys.len() {
                return Err(invalid());
            }
            build.owner.post("custody_answer_slots");
        }
        for key in keys {
            // A derived member whose committed row is absent is damage.
            let record = r.get(key, |value| {
                decode_stored(&memory, &mut build.owner, key, value.ok_or_else(invalid)?)
            })?;
            build.records.push(record);
        }
        validate_answer(r, keys, &build.records, &memory, db)?;
        let AnswerBuild { records, mut owner } = build;
        let bytes = owner.bytes();
        // The answer owns these bytes now; the memory keeps holding them.
        std::mem::forget(owner.detach(bytes)?);
        Ok(AdmittedRecords { records, memory })
    }

    /// Companions kept from earlier answer records, so one group's members are
    /// not re-read for each sibling.
    const COMPANION_MEMO: usize = 64;

    /// Every answer record and, transitively, every companion its validation
    /// relies on is validated at this same root, exactly as the resident
    /// journal checked each record when it loaded: a terminal's object
    /// admission is itself validated against its certificate, and so on. The
    /// walk is an iterative worklist over keys with an admitted visited set
    /// of digests, so a cycle of companions ends and no recursion grows the
    /// stack. A check that passed at this root before is remembered in the
    /// store's fixed memo; a memo entry certifies a key's whole closure, so
    /// entries are published only after the walk completes and a refusal part
    /// way through leaves none behind. A missing or damaged record anywhere in
    /// the closure refuses with the custody corruption error. The verifier
    /// itself is the admitted one.
    fn validate_answer(
        r: &CustodyReader,
        keys: &[String],
        records: &[Record],
        memory: &Arc<ImageMemory>,
        db: &crate::Database,
    ) -> Result<()> {
        // Positions of the answer by key, for companions the answer holds.
        let mut by_key =
            AdmittedVec::<usize>::with_exact(memory.clone(), "custody_closure_by_key", keys.len())?;
        for index in 0..keys.len() {
            by_key.push_within(index)?;
        }
        by_key
            .as_mut_slice()
            .sort_unstable_by(|a, b| keys[*a].cmp(&keys[*b]));
        let by_key = by_key;
        let in_answer = |key: &str| {
            by_key
                .as_slice()
                .binary_search_by(|i| keys[*i].as_str().cmp(key))
                .ok()
                .map(|i| &records[by_key.as_slice()[i]])
        };
        // The memo of loaded companions: fixed slots, keys owned by
        // `memo_keys` until eviction.
        let mut memo_slots = Scratch::new(memory.clone());
        memo_slots.admit_at(
            "custody_closure_memo",
            slot_bytes::<(String, Probed)>(COMPANION_MEMO)?,
        )?;
        let mut memo: std::collections::VecDeque<(String, Probed)> =
            std::collections::VecDeque::with_capacity(COMPANION_MEMO);
        memo_slots.post("custody_closure_memo");
        let mut memo_keys = Scratch::new(memory.clone());
        // Keys reached so far, as digests, and the closure keys to validate.
        let mut seen: AdmittedKeys<[u8; 32], ()> =
            AdmittedKeys::new(memory.clone(), "custody_closure_seen");
        let mut queue = KeyList::new(memory.clone(), "custody_closure_queue");
        // Certifications of this walk, published only once it completes.
        let mut staged: AdmittedVec<[u8; 32]> =
            AdmittedVec::new(memory.clone(), "custody_closure_staged");
        let digest_of = |key: &str| verified::digest(&[key.as_bytes()]);
        for key in keys {
            let d = digest_of(key);
            seen.insert_with(&d, 0, || d, ())?;
        }
        let mut next_answer = 0usize;
        loop {
            // The answer's own records first, then the closure they reach.
            let (key, owned_key, loaded_self) = if next_answer < keys.len() {
                next_answer += 1;
                (keys[next_answer - 1].as_str(), None, None)
            } else if let Some((owned, debit)) = queue.pop() {
                let found = if in_answer(&owned).is_some() || memo.iter().any(|(k, _)| *k == owned)
                {
                    None
                } else {
                    Some(load(r, memory, &owned)?.ok_or_else(invalid)?)
                };
                ("", Some((owned, debit)), found)
            } else {
                break;
            };
            let key: &str = match &owned_key {
                Some((owned, _)) => owned.as_str(),
                None => key,
            };
            // At one immutable root a key names one record and one companion
            // set, so a closure certified there before need not repeat.
            let checked = verified::digest(&[r.root_fingerprint(), key.as_bytes()]);
            if r.companion_memo().is_some_and(|memo| memo.known(&checked)) {
                continue;
            }
            let record = match &loaded_self {
                Some(probed) => probed.record(),
                None => in_answer(key)
                    .or_else(|| memo.iter().find(|(k, _)| k == key).map(|(_, p)| p.record()))
                    .ok_or_else(invalid)?,
            };
            let mut call = Scratch::new(memory.clone());
            let companions = companion_keys(r, key, record, None, memory)?;
            let mut loaded = AdmittedVec::<(String, Probed)>::with_exact(
                memory.clone(),
                "custody_closure_loaded",
                companions.len(),
            )?;
            for companion in companions.keys() {
                if in_answer(companion).is_some() || memo.iter().any(|(k, _)| k == companion) {
                    continue;
                }
                // An absent companion is simply not in the view; a required
                // one then fails validation below.
                if let Some(probed) = load(r, memory, companion)? {
                    let owned = call.own_key("custody_closure_loaded_key", companion)?;
                    loaded.push_within((owned, probed))?;
                }
            }
            {
                let mut view = AdmittedVec::<&Record>::with_exact(
                    memory.clone(),
                    "custody_closure_view",
                    companions.len(),
                )?;
                for companion in companions.keys() {
                    if let Some(found) = in_answer(companion)
                        .or_else(|| {
                            memo.iter()
                                .find(|(k, _)| k == companion)
                                .map(|(_, p)| p.record())
                        })
                        .or_else(|| {
                            loaded
                                .as_slice()
                                .iter()
                                .find(|(k, _)| k == companion)
                                .map(|(_, p)| p.record())
                        })
                    {
                        view.push_within(found)?;
                    }
                }
                validate_record_owned(db, record, view.as_slice())?;
            }
            if r.companion_memo().is_some() {
                staged.push(checked)?;
            }
            // Every present companion, loaded or memoized, joins the closure
            // once.
            for companion in companions.keys() {
                if in_answer(companion).is_some() {
                    continue;
                }
                let present = loaded.as_slice().iter().any(|(k, _)| k == companion)
                    || memo.iter().any(|(k, _)| k == companion);
                if !present {
                    continue;
                }
                let d = digest_of(companion);
                if seen.insert_with(&d, 0, || d, ())? {
                    queue.push(companion)?;
                }
            }
            drop(loaded_self);
            drop(owned_key);
            // Move each loaded companion into the memo: its key's debit
            // moves from `call` to `memo_keys`; an evicted entry drops
            // before its key's bytes are released.
            let (loaded, loaded_capacity) = loaded.into_parts();
            for (owned, probed) in loaded {
                if memo.len() == COMPANION_MEMO {
                    if let Some((evicted, evicted_probe)) = memo.pop_front() {
                        let bytes = evicted.len();
                        drop(evicted_probe);
                        drop(evicted);
                        memo_keys.release(bytes);
                    }
                }
                let debit = call.detach(owned.len())?;
                memo_keys.adopt_debit(debit)?;
                memo.push_back((owned, probed));
            }
            drop(loaded_capacity);
            drop(companions);
            drop(call);
        }
        // The whole closure validated: publish its certifications. Nothing
        // here can fail or allocate.
        if let Some(certified) = r.companion_memo() {
            for checked in staged.as_slice() {
                certified.remember(*checked);
            }
        }
        drop(memo);
        drop(memo_keys);
        drop(memo_slots);
        Ok(())
    }

    pub(super) fn select(
        r: &CustodyReader,
        selectors: &[&str],
        memory: Arc<ImageMemory>,
        db: &crate::Database,
    ) -> Result<AdmittedRecords> {
        let _errors = error_reserve(&memory)?;
        let keys = selected_keys(r, selectors, &memory)?;
        answer(r, keys.as_slice(), memory, db)
    }

    /// "submission:" + hyphenated UUID.
    const SUBMISSION_GROUP_LEN: usize = 11 + 36;

    /// Every submission group with a record of `table`, plus authority.
    /// This administrative count is proportional to the table's units by
    /// definition; it holds only the relevant keys, never the journal.
    pub(super) fn select_table(
        r: &CustodyReader,
        table: &str,
        memory: Arc<ImageMemory>,
        db: &crate::Database,
    ) -> Result<(AdmittedRecords, u64)> {
        let _errors = error_reserve(&memory)?;
        let mut keys = KeyList::new(memory.clone(), "custody_table_keys");
        let mut members = KeyList::new(memory.clone(), "custody_table_members");
        let mut visits = 0u64;
        let mut group = [0u8; SUBMISSION_GROUP_LEN];
        let mut started = false;
        let mut relevant = false;
        let finish = |members: &mut KeyList, relevant: bool, keys: &mut KeyList| -> Result<()> {
            if relevant {
                members.drain_into(keys)?;
            } else {
                // Keeps the members' capacity charge; releases key bytes.
                members.clear();
            }
            Ok(())
        };
        r.visit_groups_with_prefix("submission:", SUBMISSION_GROUP_LEN, |next, key| {
            if !started || next.as_bytes() != group.as_slice() {
                finish(&mut members, relevant, &mut keys)?;
                group.copy_from_slice(
                    next.as_bytes()
                        .get(..SUBMISSION_GROUP_LEN)
                        .ok_or_else(invalid)?,
                );
                started = true;
                relevant = false;
            }
            visits += 1;
            members.push(key)?;
            if !relevant {
                relevant = match load(r, &memory, key)?.ok_or_else(invalid)?.record() {
                    Record::Manifest(m) => m.seal.root.table == table,
                    Record::Root { life, .. } => life.original_table == table,
                    Record::Terminal { record, .. } => record.root.table == table,
                    _ => false,
                };
            }
            Ok(())
        })?;
        finish(&mut members, relevant, &mut keys)?;
        drop(members);
        r.visit_group("authority", &memory, |key| keys.push(key))?;
        keys.sort_dedup();
        let answer = answer(r, keys.as_slice(), memory, db)?;
        Ok((answer, visits))
    }

    /// The group name `prefix` + hex of `row`'s canonical reference bytes,
    /// rendered into `buffer` (admitted, grown by doubling) from the finite
    /// encoding: no intermediate `RowRef::bytes()` or hex string.
    pub(super) fn render_row_group<'b>(
        buffer: &'b mut AdmittedVec<u8>,
        prefix: &str,
        row: &RowRef,
    ) -> Result<&'b str> {
        let len = finite_len(|e| {
            row.encode_finite(e);
            Ok(())
        })?;
        let total = len
            .checked_mul(2)
            .and_then(|n| n.checked_add(prefix.len()))
            .ok_or_else(invalid)?;
        buffer.clear();
        while buffer.items.capacity() < total {
            buffer.grow()?;
        }
        buffer.items.resize(total, 0);
        buffer.items[..prefix.len()].copy_from_slice(prefix.as_bytes());
        // Emit the raw bytes into the tail, then expand to hex in place from
        // the back, so no second buffer exists.
        let tail = total - len;
        super::super::canonical::finite_emit_with_error(
            &mut buffer.items[tail..],
            |e| {
                row.encode_finite(e);
                Ok(())
            },
            invalid,
        )?;
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for i in (0..len).rev() {
            let byte = buffer.items[tail + i];
            buffer.items[prefix.len() + 2 * i] = HEX[usize::from(byte >> 4)];
            buffer.items[prefix.len() + 2 * i + 1] = HEX[usize::from(byte & 15)];
        }
        std::str::from_utf8(&buffer.items[..total]).map_err(|_| invalid())
    }

    pub(super) fn select_rows(
        r: &CustodyReader,
        rows: &[RowRef],
        memory: Arc<ImageMemory>,
        db: &crate::Database,
    ) -> Result<(AdmittedRecords, u64)> {
        let _errors = error_reserve(&memory)?;
        let mut keys: AdmittedKeys<String, ()> =
            AdmittedKeys::new(memory.clone(), "custody_rows_keys");
        let mut visits = 0u64;
        let mut group = AdmittedVec::<u8>::new(memory.clone(), "custody_rows_group");
        for row in rows {
            for prefix in ["root:", "row:"] {
                let name = render_row_group(&mut group, prefix, row)?;
                r.visit_group(name, &memory, |key| {
                    visits += 1;
                    keys.insert_key(key).map(drop)
                })?;
            }
        }
        let mut submissions =
            AdmittedVec::<uuid::Uuid>::new(memory.clone(), "custody_rows_submissions");
        let mut objects = AdmittedVec::<[u8; 32]>::new(memory.clone(), "custody_rows_objects");
        for key in keys.keys() {
            match load(r, &memory, key)?.ok_or_else(invalid)?.record() {
                Record::Manifest(m) => submissions.push(m.id)?,
                Record::Terminal { record, .. } => submissions.push(record.submission)?,
                Record::MaterializedOwner(owner) => submissions.push(owner.submission)?,
                _ => {}
            }
        }
        submissions.as_mut_slice().sort_unstable();
        submissions.retain_first_of_runs(|a, b| a == b);
        for submission in submissions.as_slice() {
            let selector = Selector::submission(*submission)?;
            r.visit_group(&selector.as_str()[1..], &memory, |key| {
                visits += 1;
                keys.insert_key(key).map(drop)
            })?;
        }
        for key in keys.keys() {
            if let Record::Terminal { record, .. } =
                load(r, &memory, key)?.ok_or_else(invalid)?.record()
            {
                objects.push(super::super::authority::object_digest_finite(
                    1,
                    |e| record.bytes_finite(e),
                    &record.signature,
                )?)?;
            }
        }
        objects.as_mut_slice().sort_unstable();
        objects.retain_first_of_runs(|a, b| a == b);
        for object in objects.as_slice() {
            let selector = Selector::object(object)?;
            r.visit_group(&selector.as_str()[1..], &memory, |key| {
                visits += 1;
                keys.insert_key(key).map(drop)
            })?;
        }
        let mut list = KeyList::with_exact(memory.clone(), "custody_rows_answer_keys", keys.len())?;
        for key in keys.keys() {
            list.push(key)?;
        }
        drop(keys);
        let answer = answer(r, list.as_slice(), memory, db)?;
        Ok((answer, visits))
    }

    /// Manifests of local roots that hold no edge terminal at the registered
    /// destination, in submission order. Reads the current root ownership
    /// rows and one exact terminal key per root; never the signed history.
    /// Pending entries one cursor page examines.
    const PENDING_PAGE: usize = 64;

    const MANIFEST_PREFIX: &str = "delivery_manifest.v1.";
    const MANIFEST_KEY_LEN: usize = MANIFEST_PREFIX.len() + 32;

    /// Manifests of local roots still pending delivery, in submission order,
    /// read from the durable pending index page by page: the work is
    /// proportional to the pending roots, never to delivered history. Two
    /// bounds differ here. The visit is bounded: each page examines at most
    /// `PENDING_PAGE` index entries and resumes strictly after the last one.
    /// The answer is not: it materializes every pending manifest, each key
    /// and decoded record admitted to the reader's memory, so a large pending
    /// set is refused by the reader limit rather than held unadmitted.
    pub(super) fn pending(
        r: &CustodyReader,
        memory: Arc<ImageMemory>,
        db: &crate::Database,
    ) -> Result<AdmittedRecords> {
        let _errors = error_reserve(&memory)?;
        let mut keys = KeyList::new(memory.clone(), "custody_pending_keys");
        let mut after = None;
        loop {
            let mut page = 0usize;
            let next = r.visit_pending(after, PENDING_PAGE, |submission| {
                page += 1;
                work::pending_examined(1);
                // The manifest key is rendered on the stack; only a present
                // manifest's key is copied, after its bytes and the list's
                // capacity growth are admitted.
                let mut rendered = [0_u8; MANIFEST_KEY_LEN];
                rendered[..MANIFEST_PREFIX.len()].copy_from_slice(MANIFEST_PREFIX.as_bytes());
                submission
                    .simple()
                    .encode_lower(&mut rendered[MANIFEST_PREFIX.len()..]);
                let key = std::str::from_utf8(&rendered).map_err(|_| invalid())?;
                if r.get(key, |value| Ok(value.is_some()))? {
                    keys.push(key)?;
                }
                Ok(())
            })?;
            if page < PENDING_PAGE {
                break;
            }
            after = next;
        }
        answer(r, keys.as_slice(), memory, db)
    }

    /// The fixed part of one delta or committed entry's map slot. The key
    /// copy is admitted at its length by the map; the decoded body is
    /// charged by the decoder as it allocates.
    fn entry_slot_bytes() -> usize {
        std::mem::size_of::<(String, Option<Record>)>()
            + std::mem::size_of::<(Option<Record>, usize)>()
    }

    /// Decode, check and verify one stored body under `memory`, its decoded
    /// allocations taken over by `owner`.
    fn decode_checked(
        memory: &Arc<ImageMemory>,
        owner: &mut Scratch,
        key: &str,
        bytes: &[u8],
    ) -> Result<Record> {
        work::validation_decoded(1);
        let (record, charged) = decode_charged(memory, bytes)?;
        owner.adopt(charged)?;
        native_check(memory, key, bytes)?;
        Ok(record)
    }

    /// Visit, one at a time, the group names `record` joins (as `groups()`
    /// lists them), rendering row groups into the admitted `buffer` and
    /// every other group on the stack. No group list is built.
    fn for_each_group(
        record: &Record,
        buffer: &mut AdmittedVec<u8>,
        visit: &mut dyn FnMut(&str) -> Result<()>,
    ) -> Result<()> {
        let mut stack = |selector: Result<Selector>, visit: &mut dyn FnMut(&str) -> Result<()>| {
            let selector = selector?;
            visit(&selector.as_str()[1..])
        };
        let submission = match record {
            Record::Manifest(m) => {
                visit(render_row_group(buffer, "root:", &m.seal.root)?)?;
                for row in &m.rows {
                    visit(render_row_group(buffer, "row:", &row.reference)?)?;
                }
                Some(m.id)
            }
            Record::Terminal { record: t, .. } => {
                visit(render_row_group(buffer, "root:", &t.root)?)?;
                stack(
                    Selector::object(&super::super::authority::object_digest_finite(
                        1,
                        |e| t.bytes_finite(e),
                        &t.signature,
                    )?),
                    visit,
                )?;
                Some(t.submission)
            }
            Record::MaterializedOwner(o) => {
                visit(render_row_group(buffer, "root:", &o.source.reference)?)?;
                Some(o.submission)
            }
            Record::Root { submission, .. }
            | Record::MemberOwner { submission, .. }
            | Record::Diagnostic { submission, .. }
            | Record::DiagnosticOwner { submission, .. }
            | Record::SourceHistory { submission, .. }
            | Record::PolicyProfile { submission, .. }
            | Record::Order { submission, .. } => Some(*submission),
            Record::Binding(b) => {
                visit("authority")?;
                stack(
                    Selector::object(&super::super::authority::object_digest_finite(
                        0,
                        |e| b.bytes_finite(e),
                        &b.signature,
                    )?),
                    visit,
                )?;
                None
            }
            Record::ObjectAdmission(a) => {
                // A certificate's group is joined by the certificate itself
                // below; an admission's certificate group is not followed
                // here, exactly as the old filter did.
                stack(Selector::object(&a.object_digest), visit)?;
                None
            }
            Record::CheckpointFrontier(_)
            | Record::CheckpointNode(_)
            | Record::EdgeCheckpoint(_) => None,
            Record::Policy(_) => {
                visit("policies")?;
                None
            }
            Record::Certificate(c) => {
                visit("authority")?;
                let mut selector = Selector::text("@certificate:")?;
                selector.push_hex(&c.id_finite()?)?;
                visit(&selector.as_str()[1..])?;
                None
            }
            Record::Control(_) | Record::State(_) | Record::Birth(_) | Record::Destination(_) => {
                visit("authority")?;
                None
            }
        };
        if let Some(id) = submission {
            stack(Selector::submission(id), visit)?;
        }
        Ok(())
    }

    /// Touched-record validation over the committed journal plus `delta`.
    /// Affected keys are the delta and the durable members of every group a
    /// touched record joins or leaves; companions are point reads. Every key,
    /// map entry, decoded body, group name, plan and verification workspace
    /// is admitted before it is allocated. The returned delta's own bytes
    /// stay admitted in its own charges until the caller publishes or aborts.
    pub(super) fn validate_delta(
        r: &CustodyReader,
        writes: &[(String, Vec<u8>)],
        deletes: &[String],
        memory: Arc<ImageMemory>,
        db: &crate::Database,
    ) -> Result<(DeltaRecords, u64, Scratch)> {
        admission_probe_start();
        let errors = error_reserve(&memory)?;
        let mut retained = Scratch::new(memory.clone());
        let mut scratch = Scratch::new(memory.clone());
        let mut delta: DeltaRecords = AdmittedKeys::new(memory.clone(), "custody_delta");
        for key in deletes.iter().filter(|k| owned_key(k)) {
            retained.admit_at("custody_delta_slot", entry_slot_bytes())?;
            retained.post("custody_delta_slot");
            delta.insert_with(key.as_str(), key.len(), || key.clone(), None)?;
        }
        for (key, bytes) in writes.iter().filter(|(k, _)| owned_key(k)) {
            // An unchanged committed record (for example the authority
            // certificate every unit restates) cannot change its dependents.
            if !delta.contains(key.as_str())
                && r.get(key, |old| Ok(old == Some(bytes.as_slice())))?
            {
                continue;
            }
            retained.admit_at("custody_delta_slot", entry_slot_bytes())?;
            retained.post("custody_delta_slot");
            let record = decode_checked(&memory, &mut retained, key, bytes)?;
            delta.insert_with(key.as_str(), key.len(), || key.clone(), Some(record))?;
        }
        // Committed records this validation reads, each decoded once.
        let mut committed: AdmittedKeys<String, (Option<Record>, usize)> =
            AdmittedKeys::new(memory.clone(), "custody_committed");
        let fetch = |committed: &mut AdmittedKeys<String, (Option<Record>, usize)>,
                     scratch: &mut Scratch,
                     key: &str|
         -> Result<()> {
            if committed.contains(key) {
                return Ok(());
            }
            let entry = r.get(key, |value| {
                let encoded = value.map_or(0, <[u8]>::len);
                scratch.admit_at("custody_committed_slot", entry_slot_bytes())?;
                scratch.post("custody_committed_slot");
                let record = value
                    .map(|bytes| decode_checked(&memory, scratch, key, bytes))
                    .transpose()?;
                Ok((record, encoded))
            })?;
            committed.insert_with(key, key.len(), || key.to_owned(), entry)?;
            Ok(())
        };
        let mut affected: AdmittedKeys<String, ()> =
            AdmittedKeys::new(memory.clone(), "custody_affected");
        let mut group = AdmittedVec::<u8>::new(memory.clone(), "custody_delta_group");
        for (key, next) in delta.iter() {
            affected.insert_key(key)?;
            fetch(&mut committed, &mut scratch, key)?;
            let old = committed
                .get(key.as_str())
                .and_then(|(record, _)| record.as_ref());
            for record in old.into_iter().chain(next.as_ref()) {
                for_each_group(record, &mut group, &mut |name| {
                    r.visit_group(name, &memory, |member| {
                        affected.insert_key(member).map(drop)
                    })
                })?;
            }
        }
        let visits = affected.len() as u64;
        for key in affected.keys() {
            if !delta.contains(key.as_str()) {
                fetch(&mut committed, &mut scratch, key)?;
            }
        }
        let current = |key: &str| -> Option<&Record> {
            match delta.get(key) {
                Some(next) => next.as_ref(),
                None => committed.get(key).and_then(|(record, _)| record.as_ref()),
            }
        };
        let mut plans = AdmittedVec::<(&String, AdmittedKeys<String, ()>)>::with_exact(
            memory.clone(),
            "custody_delta_plans",
            affected.len(),
        )?;
        for key in affected.keys() {
            if let Some(record) = current(key) {
                plans.push_within((key, companion_keys(r, key, record, Some(&delta), &memory)?))?;
            }
        }
        for (_, companions) in plans.as_slice() {
            for key in companions.keys() {
                if !delta.contains(key.as_str()) {
                    fetch(&mut committed, &mut scratch, key)?;
                }
            }
        }
        // `committed` grew; borrow it afresh for the verification pass.
        let current = |key: &str| -> Option<&Record> {
            match delta.get(key) {
                Some(next) => next.as_ref(),
                None => committed.get(key).and_then(|(record, _)| record.as_ref()),
            }
        };
        for (key, companions) in plans.as_slice() {
            let record = current(key).ok_or_else(invalid)?;
            // The companion view; the admitted verifier funds its own work.
            let mut view = AdmittedVec::<&Record>::with_exact(
                memory.clone(),
                "custody_delta_view",
                companions.len(),
            )?;
            for companion in companions.keys() {
                if let Some(found) = current(companion) {
                    view.push_within(found)?;
                }
            }
            validate_record_owned(db, record, view.as_slice())?;
        }
        drop(plans);
        drop(group);
        drop(affected);
        drop(committed);
        drop(scratch);
        drop(errors);
        Ok((delta, visits, retained))
    }

    /// The companion keys of `record` (stored at `key`): the durable members
    /// of its stack-rendered selectors, overlaid with the in-commit delta's
    /// members decided by the allocation-free `joins` predicate.
    fn companion_keys(
        r: &CustodyReader,
        _key: &str,
        record: &Record,
        delta: Option<&DeltaRecords>,
        memory: &Arc<ImageMemory>,
    ) -> Result<AdmittedKeys<String, ()>> {
        let (selectors, count) = companion_selectors(record)?;
        let mut keys: AdmittedKeys<String, ()> =
            AdmittedKeys::new(memory.clone(), "custody_companion_keys");
        for selector in selectors.iter().take(count).flatten() {
            visit_selector(r, selector.as_str(), memory, &mut |member| {
                keys.insert_key(member).map(drop)
            })?;
        }
        if let Some(delta) = delta {
            for (candidate_key, candidate) in delta.iter() {
                let Some(candidate) = candidate else {
                    continue;
                };
                let mut joins_any = false;
                for selector in selectors.iter().take(count).flatten() {
                    if joins(candidate_key, candidate, selector.as_str())? {
                        joins_any = true;
                        break;
                    }
                }
                if joins_any {
                    keys.insert_key(candidate_key)?;
                }
            }
        }
        Ok(keys)
    }

    /// Measurement only: `joins` agrees with `groups()` for every record in
    /// `records`, over every selector-family group any of them joins.
    /// Answers how many (record, group) pairs were compared.
    #[cfg(feature = "test-seams")]
    pub(super) fn joins_parity_for_test(records: &[(String, Record)]) -> Result<usize> {
        let mut families: Vec<String> = Vec::new();
        for (_, record) in records {
            for group in groups(record)? {
                if group == "authority"
                    || group.starts_with("submission:")
                    || group.starts_with("object:")
                {
                    families.push(group);
                }
            }
        }
        families.sort();
        families.dedup();
        let mut compared = 0usize;
        for (key, record) in records {
            let own = groups(record)?;
            for group in &families {
                let expected = own.iter().any(|g| g == group);
                let selector = format!("@{group}");
                if joins(key, record, &selector)? != expected {
                    return Err(contextdb_core::Error::Other(format!(
                        "joins disagrees with groups() for {key} in {group}"
                    )));
                }
                compared += 1;
            }
        }
        Ok(compared)
    }

    /// Measurement only: `render_row_group` equals `root_group` for every
    /// row reference the records name.
    #[cfg(feature = "test-seams")]
    pub(super) fn row_group_parity_for_test(
        records: &[(String, Record)],
        memory: Arc<ImageMemory>,
    ) -> Result<usize> {
        let mut buffer = AdmittedVec::<u8>::new(memory, "custody_row_group_parity");
        let mut compared = 0usize;
        for (_, record) in records {
            let rows: Vec<&RowRef> = match record {
                Record::Manifest(m) => std::iter::once(&m.seal.root)
                    .chain(m.rows.iter().map(|row| &row.reference))
                    .collect(),
                Record::Terminal { record: t, .. } => vec![&t.root],
                Record::MaterializedOwner(o) => vec![&o.source.reference],
                _ => Vec::new(),
            };
            for row in rows {
                let rendered = render_row_group(&mut buffer, "root:", row)?.to_owned();
                if rendered != root_group(row) {
                    return Err(contextdb_core::Error::Other(format!(
                        "row group rendering differs: {rendered} vs {}",
                        root_group(row)
                    )));
                }
                compared += 1;
            }
        }
        Ok(compared)
    }
}

/// A borrowed record cannot outlive the checked raw view's erasure fence.
/// Retained windows themselves hold pages, metadata and the original weak
/// checkpoint context; they do not keep an erasure read lock between calls.
#[cfg(feature = "sync-orchestration")]
pub(crate) struct NativeCustodyView<'a> {
    raw: crate::persistence::writer_source::NativeRawConfigView<'a>,
}
#[cfg(feature = "sync-orchestration")]
impl NativeCustodyView<'_> {
    pub(crate) fn key(&self) -> &str {
        self.raw.key()
    }
    pub(crate) fn stored_bytes(&self) -> &[u8] {
        self.raw.stored_bytes()
    }
    pub(crate) fn record(
        &self,
    ) -> super::native_decode::Decoded<super::native_records::RecordView<'_>> {
        super::native_record_codec::decode_record(self.raw.stored_bytes())
    }
}
#[cfg(feature = "sync-orchestration")]
pub(crate) struct NativeCustodyWindow {
    // Physical guards and key storage precede counted owners on drop.
    raw: crate::persistence::writer_source::NativeRawConfigWindow,
    key: super::native_selection::NativeBuffer,
}
#[cfg(feature = "sync-orchestration")]
impl NativeCustodyWindow {
    pub(crate) fn admit(
        raw: crate::persistence::writer_source::NativeRawConfigWindow,
        owner: &contextdb_core::read_memory::ReadMemoryOwner,
    ) -> super::native_selection::Selected<Self> {
        let key = {
            let view = raw.view()?;
            let record = super::native_record_codec::decode_record(view.stored_bytes())?;
            let key = record.key_native(owner)?;
            if key.text()? != view.key() {
                return Err(super::native_decode::Invalid::Key.into());
            }
            record.verify_native(owner)?;
            key
        };
        Ok(Self { raw, key })
    }
    pub(crate) fn key(&self) -> &str {
        self.key
            .text()
            .expect("admitted UTF-8 canonical custody key")
    }
    pub(crate) fn view(&self) -> super::native_selection::Selected<NativeCustodyView<'_>> {
        Ok(NativeCustodyView {
            raw: self.raw.view()?,
        })
    }
    pub(crate) fn committed_view(
        &self,
    ) -> (
        redb::StorageSnapshotIdentity,
        contextdb_core::SnapshotId,
        contextdb_core::Lsn,
    ) {
        self.raw.committed_view()
    }
}
#[cfg(feature = "sync-orchestration")]
pub(crate) struct NativeCustodySelection {
    windows: super::native_selection::NativeSelectionHold<NativeCustodyWindow>,
}
#[cfg(feature = "sync-orchestration")]
impl NativeCustodySelection {
    pub(crate) fn try_new(
        owner: &contextdb_core::read_memory::ReadMemoryOwner,
        slots: usize,
    ) -> super::native_selection::Selected<Self> {
        Ok(Self {
            windows: super::native_selection::NativeSelectionHold::try_new(owner, slots)?,
        })
    }
    pub(crate) fn retain(
        &mut self,
        window: NativeCustodyWindow,
    ) -> super::native_selection::Selected<()> {
        self.windows.retain(window)
    }
    pub(crate) fn windows(&self) -> &[NativeCustodyWindow] {
        self.windows.as_slice()
    }
    pub(crate) fn sort_dedup(&mut self) {
        let windows = self.windows.as_mut_slice();
        windows.sort_unstable_by(|a, b| a.key().cmp(b.key()));
        let mut used = 0;
        for i in 0..windows.len() {
            if used == 0 || windows[i].key() != windows[used - 1].key() {
                windows.swap(used, i);
                used += 1;
            }
        }
        self.windows.truncate(used);
    }
    pub(crate) fn is_funded_by(
        &self,
        owner: &contextdb_core::read_memory::ReadMemoryOwner,
    ) -> bool {
        self.windows.is_funded_by(owner)
    }
}
