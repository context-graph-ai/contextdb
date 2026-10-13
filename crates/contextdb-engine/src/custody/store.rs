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
pub(crate) use durable::Scratch as CustodyCharge;
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
    }
    pub(crate) fn pending_examined_total() -> u64 {
        PENDING_EXAMINED.load(Ordering::Relaxed)
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
    let durable = durable::decode_stored(&memory, key, bytes).err();
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
    /// Its companion memo is admitted to `writer` before it is allocated.
    pub fn durable(
        tenant_entries: BTreeMap<String, Vec<u8>>,
        writer: crate::memory_accounting::AccountantHandle,
    ) -> Result<Self> {
        if tenant_entries.keys().any(|key| !key.starts_with("tenant_")) {
            return Err(invalid());
        }
        let charge = crate::memory_accounting::OwnedMemoryReservation::try_new_for(
            writer,
            CompanionMemo::BYTES,
            "custody",
            "companion_memo",
            "Raise MEMORY_LIMIT to open durable custody reads.",
        )?;
        let mut store = Self::from_entries(tenant_entries)?;
        store.durable = Some(DurableSource { pinned: None });
        store.checked_charge = Some(charge);
        store.checked = Some(Arc::new(CompanionMemo::new()));
        Ok(store)
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
        root.with_custody_read(self.checked.clone(), read)?
            .map(Some)
            .ok_or_else(crate::persistence::custody_source::incomplete_custody_index)
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
        selectors: &[String],
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
    ) -> Result<(BTreeMap<String, Option<Record>>, CustodyCharge)> {
        if self.durable.is_some() {
            let durable_memory = memory.clone();
            if let Some((delta, visits, retained)) = self
                .durable_read(|r| durable::validate_delta(r, writes, deletes, durable_memory, db))?
            {
                self.validation_visits += visits;
                return Ok((delta, retained));
            }
        }
        let retained = CustodyCharge::new(memory);
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
        Ok((delta, retained))
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

/// Verify a stored record's signature unless these exact stored bytes were
/// verified before. The native verifier renders each signature input into a
/// buffer `memory` admits first, so no verification allocation goes unfunded.
fn verify_stored(
    memory: &Arc<crate::read_image_memory::ImageMemory>,
    key: &str,
    bytes: &[u8],
) -> Result<()> {
    use super::native_selection::Failure;
    let digest = verified::digest(&[key.as_bytes(), bytes]);
    if verified::signature_known(&digest) {
        return Ok(());
    }
    let view = super::native_record_codec::decode_record(bytes).map_err(|_| invalid())?;
    let owner = contextdb_core::read_memory::ReadMemoryOwner::from_legacy(memory.clone());
    match view.verify_native(&owner) {
        Ok(()) => {}
        Err(Failure::Invalid(_)) => return Err(invalid()),
        Err(Failure::Unqualified(error)) => return Err(error),
        Err(other) => {
            return Err(memory.take_failure().unwrap_or_else(|| {
                contextdb_core::Error::Other(match other {
                    Failure::Refused(refusal) => refusal.to_string(),
                    _ => "custody signature verification failed".into(),
                })
            }));
        }
    }
    verified::remember_signature(digest);
    Ok(())
}

/// Durable custody reads for a file-backed store with a complete group index.
/// Every key list, decoded record and scratch copy is admitted to the
/// requesting operation's memory before it is allocated, and released when
/// the operation's scratch drops. Nothing outlives the call except the
/// returned, already-charged answer.
mod durable {
    use super::*;
    use super::{verified, verify_stored};
    use crate::custody::records::validate_record_owned;
    use crate::persistence::custody_source::CustodyReader;
    use crate::read_image_memory::ImageMemory;
    use crate::sync_types::held::ALLOCATION_BYTES;

    /// Bytes admitted for scratch that this operation releases on drop.
    pub(crate) struct Scratch {
        memory: Arc<ImageMemory>,
        bytes: usize,
    }
    impl Scratch {
        pub(super) fn new(memory: Arc<ImageMemory>) -> Self {
            Self { memory, bytes: 0 }
        }
        /// Bytes this owner holds admitted now.
        pub(crate) fn bytes(&self) -> usize {
            self.bytes
        }
        pub(super) fn admit(&mut self, bytes: usize) -> Result<()> {
            if let Err(refused) = self.memory.reserve(bytes) {
                admission_probe(self.memory.held());
                return Err(refused);
            }
            self.bytes = self.bytes.checked_add(bytes).ok_or_else(invalid)?;
            admission_probe(self.memory.held());
            Ok(())
        }
        fn reset(&mut self) {
            self.memory.release(self.bytes);
            self.bytes = 0;
        }
        /// Take over `bytes` this owner's memory already holds charged (a
        /// charged decode's result), to release with the rest on drop.
        fn adopt(&mut self, bytes: usize) -> Result<()> {
            self.bytes = self.bytes.checked_add(bytes).ok_or_else(invalid)?;
            Ok(())
        }
        fn admit_key(&mut self, key: &str) -> Result<()> {
            self.admit(
                key.len()
                    .checked_add(std::mem::size_of::<String>() + ALLOCATION_BYTES)
                    .ok_or_else(invalid)?,
            )
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            self.memory.release(self.bytes);
        }
    }

    /// Decode one stored record into an answer charged to `memory`, once:
    /// canonical bytes, matching key, valid signature. The decode is charged
    /// as it allocates; the transient canonical re-encode is admitted first.
    pub(super) fn decode_stored(
        memory: &Arc<ImageMemory>,
        key: &str,
        bytes: &[u8],
    ) -> Result<Record> {
        let credit = bytes
            .len()
            .checked_add(key.len())
            .and_then(|n| n.checked_add(2 * ALLOCATION_BYTES))
            .ok_or_else(invalid)?;
        memory.reserve(credit)?;
        let decoded = (|| {
            work::decoded(1);
            let (record, _) = decode_charged(memory, bytes)?;
            if encode(&record)? != bytes || record.key()? != key {
                return Err(invalid());
            }
            verify_stored(memory, key, bytes)?;
            Ok(record)
        })();
        memory.release(credit);
        decoded
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
        let record = memory
            .decode_msgpack::<Record>(bytes)
            .map_err(established_codec_error)?;
        admission_probe(memory.held());
        Ok((record, memory.held().saturating_sub(before)))
    }

    /// The custody codec's established diagnostic for a malformed record,
    /// rendered from the charged decoder's own error without decoding again.
    fn established_codec_error(error: contextdb_core::Error) -> contextdb_core::Error {
        match error {
            contextdb_core::Error::SyncError(message) => {
                match message.strip_prefix("MessagePack decode error: ") {
                    Some(detail) => contextdb_core::Error::SyncError(format!(
                        "delivery metadata decode: {detail}"
                    )),
                    None => contextdb_core::Error::SyncError(message),
                }
            }
            other => other,
        }
    }

    /// A transient check of one stored record (canonical, keyed, signed):
    /// charged as it decodes and released when the caller drops it.
    fn probe_stored(memory: &Arc<ImageMemory>, key: &str, bytes: &[u8]) -> Result<Probed> {
        // The record's own slot and key, and its transient canonical re-encode.
        let credit = key
            .len()
            .checked_add(bytes.len())
            .and_then(|n| n.checked_add(std::mem::size_of::<Record>() + 3 * ALLOCATION_BYTES))
            .ok_or_else(invalid)?;
        memory.reserve(credit)?;
        let mut probed = Probed {
            memory: memory.clone(),
            credit,
            record: None,
        };
        work::decoded(1);
        let (record, charged) = decode_charged(memory, bytes)?;
        probed.credit += charged;
        probed.record = Some(record);
        if encode(probed.record())? != bytes || probed.record().key()? != key {
            return Err(invalid());
        }
        verify_stored(memory, key, bytes)?;
        Ok(probed)
    }
    /// One transiently decoded record and its admission.
    pub(super) struct Probed {
        memory: Arc<ImageMemory>,
        credit: usize,
        record: Option<Record>,
    }
    impl Probed {
        fn record(&self) -> &Record {
            self.record.as_ref().expect("probed record")
        }
    }
    impl Drop for Probed {
        fn drop(&mut self) {
            drop(self.record.take());
            self.memory.release(self.credit);
        }
    }

    #[cfg(feature = "test-seams")]
    pub(super) fn probe_for_test(memory: &Arc<ImageMemory>, key: &str, bytes: &[u8]) -> Result<()> {
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

    /// Admitted, sorted and deduplicated key list for `selectors`.
    fn selected_keys(
        r: &CustodyReader,
        selectors: &[String],
        scratch: &mut Scratch,
    ) -> Result<Vec<String>> {
        let memory = scratch.memory.clone();
        let mut count = 0usize;
        for selector in selectors {
            visit_selector(r, selector, &memory, &mut |_| {
                count = count.checked_add(1).ok_or_else(invalid)?;
                Ok(())
            })?;
        }
        scratch.admit(
            count
                .checked_mul(std::mem::size_of::<String>())
                .and_then(|n| n.checked_add(ALLOCATION_BYTES))
                .ok_or_else(invalid)?,
        )?;
        let mut keys = Vec::with_capacity(count);
        for selector in selectors {
            visit_selector(r, selector, &memory, &mut |key| {
                if keys.len() == count {
                    return Err(invalid());
                }
                scratch.admit(
                    key.len()
                        .checked_add(ALLOCATION_BYTES)
                        .ok_or_else(invalid)?,
                )?;
                keys.push(key.to_owned());
                Ok(())
            })?;
        }
        keys.sort_unstable();
        keys.dedup();
        Ok(keys)
    }
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

    /// Decode `keys` into one answer charged to `memory`, in key order.
    fn answer(
        r: &CustodyReader,
        keys: &[String],
        memory: Arc<ImageMemory>,
        db: &crate::Database,
    ) -> Result<AdmittedRecords> {
        if !keys.is_empty() {
            memory.reserve(
                keys.len()
                    .checked_mul(std::mem::size_of::<Record>())
                    .and_then(|n| n.checked_add(ALLOCATION_BYTES))
                    .ok_or_else(invalid)?,
            )?;
        }
        let mut records = Vec::with_capacity(keys.len());
        for key in keys {
            // A derived member whose committed row is absent is damage.
            records.push(r.get(key, |value| {
                decode_stored(&memory, key, value.ok_or_else(invalid)?)
            })?);
        }
        validate_answer(r, keys, &records, &memory, db)?;
        Ok(AdmittedRecords { records, memory })
    }

    /// Companions kept from earlier answer records, so one group's members are
    /// not re-read for each sibling.
    const COMPANION_MEMO: usize = 64;

    /// Every answer record and, transitively, every companion its validation
    /// relies on is validated at this same root, exactly as the resident
    /// journal checked each record when it loaded: a terminal's object
    /// admission is itself validated against its certificate, and so on. The
    /// walk is an iterative worklist over keys with a visited set, so a cycle
    /// of companions ends and no recursion grows the stack; every key it holds
    /// and every record it loads is admitted to `memory`. A check that passed
    /// at this root before is remembered in the store's fixed memo. A missing
    /// or damaged record anywhere in the closure refuses with the custody
    /// corruption error. The verifier itself is the admitted one.
    fn validate_answer(
        r: &CustodyReader,
        keys: &[String],
        records: &[Record],
        memory: &Arc<ImageMemory>,
        db: &crate::Database,
    ) -> Result<()> {
        let empty = BTreeMap::new();
        let mut scratch = Scratch::new(memory.clone());
        // Positions of the answer by key, for companions the answer already holds.
        scratch.admit(
            keys.len()
                .checked_mul(std::mem::size_of::<usize>())
                .and_then(|n| n.checked_add(ALLOCATION_BYTES))
                .ok_or_else(invalid)?,
        )?;
        let mut by_key = (0..keys.len()).collect::<Vec<_>>();
        by_key.sort_unstable_by(|a, b| keys[*a].cmp(&keys[*b]));
        let in_answer = |key: &str| {
            by_key
                .binary_search_by(|i| keys[*i].as_str().cmp(key))
                .ok()
                .map(|i| &records[by_key[i]])
        };
        scratch.admit(
            COMPANION_MEMO
                .checked_mul(std::mem::size_of::<(String, Probed)>())
                .and_then(|n| n.checked_add(ALLOCATION_BYTES))
                .ok_or_else(invalid)?,
        )?;
        let mut memo: std::collections::VecDeque<(String, Probed)> =
            std::collections::VecDeque::with_capacity(COMPANION_MEMO);
        // Keys reached so far and the closure keys still to validate.
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut queue: Vec<String> = Vec::new();
        for key in keys {
            if !seen.contains(key) {
                scratch.admit_key(key)?;
                seen.insert(key.clone());
            }
        }
        let mut next_answer = 0usize;
        loop {
            // The answer's own records first, then the closure they reach.
            let (key, loaded_self) = if next_answer < keys.len() {
                next_answer += 1;
                (keys[next_answer - 1].clone(), None)
            } else if let Some(key) = queue.pop() {
                let found = match in_answer(&key) {
                    Some(_) => None,
                    None => match memo.iter().position(|(k, _)| *k == key) {
                        Some(_) => None,
                        None => Some(load(r, memory, &key)?.ok_or_else(invalid)?),
                    },
                };
                (key, found)
            } else {
                break;
            };
            // At one immutable root a key names one record and one companion
            // set, so a check that passed there before need not repeat.
            let checked = verified::digest(&[r.root_fingerprint(), key.as_bytes()]);
            if r.companion_memo().is_some_and(|memo| memo.known(&checked)) {
                continue;
            }
            let record = match &loaded_self {
                Some(probed) => probed.record(),
                None => in_answer(&key)
                    .or_else(|| {
                        memo.iter()
                            .find(|(k, _)| *k == key)
                            .map(|(_, p)| p.record())
                    })
                    .ok_or_else(invalid)?,
            };
            let mut call = Scratch::new(memory.clone());
            let companions = companion_keys(r, record, &empty, &|_| 0, &mut call)?;
            call.admit(
                companions
                    .len()
                    .checked_mul(
                        std::mem::size_of::<(String, Probed)>() + std::mem::size_of::<&Record>(),
                    )
                    .and_then(|n| n.checked_add(2 * ALLOCATION_BYTES))
                    .ok_or_else(invalid)?,
            )?;
            let mut loaded: Vec<(String, Probed)> = Vec::with_capacity(companions.len());
            for companion in &companions {
                if in_answer(companion).is_some() || memo.iter().any(|(k, _)| k == companion) {
                    continue;
                }
                // An absent companion is simply not in the view; a required
                // one then fails validation below.
                if let Some(probed) = load(r, memory, companion)? {
                    call.admit_key(companion)?;
                    loaded.push((companion.clone(), probed));
                }
            }
            {
                let view = companions
                    .iter()
                    .filter_map(|key| {
                        in_answer(key)
                            .or_else(|| {
                                memo.iter().find(|(k, _)| k == key).map(|(_, p)| p.record())
                            })
                            .or_else(|| {
                                loaded
                                    .iter()
                                    .find(|(k, _)| k == key)
                                    .map(|(_, p)| p.record())
                            })
                    })
                    .collect::<Vec<_>>();
                validate_record_owned(db, record, &view)?;
            }
            if let Some(memo) = r.companion_memo() {
                memo.remember(checked);
            }
            // Every present companion joins the closure once.
            for (companion, _) in &loaded {
                if !seen.contains(companion) {
                    scratch.admit_key(companion)?;
                    seen.insert(companion.clone());
                    reserve_admitted(&mut queue, &mut scratch)?;
                    scratch.admit_key(companion)?;
                    queue.push(companion.clone());
                }
            }
            for companion in &companions {
                if !seen.contains(companion) && in_answer(companion).is_none() {
                    // Present in the memo but not loaded by this record.
                    if memo.iter().any(|(k, _)| k == companion) {
                        scratch.admit_key(companion)?;
                        seen.insert(companion.clone());
                        reserve_admitted(&mut queue, &mut scratch)?;
                        scratch.admit_key(companion)?;
                        queue.push(companion.clone());
                    }
                }
            }
            drop(loaded_self);
            for entry in loaded {
                if memo.len() == COMPANION_MEMO {
                    memo.pop_front();
                }
                memo.push_back(entry);
            }
        }
        Ok(())
    }

    pub(super) fn select(
        r: &CustodyReader,
        selectors: &[String],
        memory: Arc<ImageMemory>,
        db: &crate::Database,
    ) -> Result<AdmittedRecords> {
        let mut scratch = Scratch::new(memory.clone());
        let keys = selected_keys(r, selectors, &mut scratch)?;
        let answer = answer(r, &keys, memory, db);
        drop(keys);
        drop(scratch);
        answer
    }

    /// Every submission group with a record of `table`, plus authority.
    /// This administrative count is proportional to the table's units by
    /// definition; it holds only the relevant keys, never the journal.
    pub(super) fn select_table(
        r: &CustodyReader,
        table: &str,
        memory: Arc<ImageMemory>,
        db: &crate::Database,
    ) -> Result<(AdmittedRecords, u64)> {
        // "submission:" + hyphenated UUID.
        const SUBMISSION_GROUP_LEN: usize = 11 + 36;
        let mut scratch = Scratch::new(memory.clone());
        let mut keys = Vec::new();
        let mut visits = 0u64;
        let mut group = String::new();
        let mut members: Vec<String> = Vec::new();
        let mut relevant = false;
        let finish = |members: &mut Vec<String>,
                      relevant: bool,
                      keys: &mut Vec<String>,
                      scratch: &mut Scratch,
                      probe: &mut Scratch|
         -> Result<()> {
            if relevant {
                for key in members.drain(..) {
                    scratch.admit_key(&key)?;
                    keys.push(key);
                }
            } else {
                members.clear();
            }
            probe.reset();
            Ok(())
        };
        // One submission group's member keys at a time.
        let mut probe = Scratch::new(memory.clone());
        r.visit_groups_with_prefix("submission:", SUBMISSION_GROUP_LEN, |next, key| {
            if next != group {
                finish(&mut members, relevant, &mut keys, &mut scratch, &mut probe)?;
                group.clear();
                group.push_str(next);
                relevant = false;
            }
            visits += 1;
            probe.admit_key(key)?;
            members.push(key.to_owned());
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
        finish(&mut members, relevant, &mut keys, &mut scratch, &mut probe)?;
        drop(members);
        drop(probe);
        r.visit_group("authority", &memory, |key| {
            scratch.admit_key(key)?;
            keys.push(key.to_owned());
            Ok(())
        })?;
        keys.sort_unstable();
        keys.dedup();
        let answer = answer(r, &keys, memory, db)?;
        drop(keys);
        drop(scratch);
        Ok((answer, visits))
    }

    pub(super) fn select_rows(
        r: &CustodyReader,
        rows: &[RowRef],
        memory: Arc<ImageMemory>,
        db: &crate::Database,
    ) -> Result<(AdmittedRecords, u64)> {
        let mut scratch = Scratch::new(memory.clone());
        let mut keys = BTreeSet::new();
        let mut visits = 0u64;
        let mut add =
            |keys: &mut BTreeSet<String>, scratch: &mut Scratch, key: &str| -> Result<()> {
                visits += 1;
                if !keys.contains(key) {
                    scratch.admit_key(key)?;
                    keys.insert(key.to_owned());
                }
                Ok(())
            };
        for row in rows {
            for group in [root_group(row), format!("row:{}", hex(&row.bytes()))] {
                r.visit_group(&group, &memory, |key| add(&mut keys, &mut scratch, key))?;
            }
        }
        let mut submissions = BTreeSet::new();
        for key in &keys {
            match load(r, &memory, key)?.ok_or_else(invalid)?.record() {
                Record::Manifest(m) => submissions.insert(m.id),
                Record::Terminal { record, .. } => submissions.insert(record.submission),
                Record::MaterializedOwner(owner) => submissions.insert(owner.submission),
                _ => false,
            };
        }
        for submission in submissions {
            r.visit_group(&submission_group(submission), &memory, |key| {
                add(&mut keys, &mut scratch, key)
            })?;
        }
        let mut objects = BTreeSet::new();
        for key in &keys {
            if let Record::Terminal { record, .. } =
                load(r, &memory, key)?.ok_or_else(invalid)?.record()
            {
                objects.insert(object_digest(1, &record.bytes()?, &record.signature));
            }
        }
        for object in objects {
            r.visit_group(&object_group(&object), &memory, |key| {
                add(&mut keys, &mut scratch, key)
            })?;
        }
        let keys = keys.into_iter().collect::<Vec<_>>();
        let answer = answer(r, &keys, memory, db)?;
        drop(keys);
        drop(scratch);
        Ok((answer, visits))
    }

    /// Manifests of local roots that hold no edge terminal at the registered
    /// destination, in submission order. Reads the current root ownership
    /// rows and one exact terminal key per root; never the signed history.
    /// Pending entries one cursor page examines.
    const PENDING_PAGE: usize = 64;

    const MANIFEST_PREFIX: &str = "delivery_manifest.v1.";
    const MANIFEST_KEY_LEN: usize = MANIFEST_PREFIX.len() + 32;

    /// Grow `list` by doubling once it is full, admitting the added capacity
    /// before the reallocation exists.
    fn reserve_admitted<T>(list: &mut Vec<T>, scratch: &mut Scratch) -> Result<()> {
        if list.len() < list.capacity() {
            return Ok(());
        }
        let additional = list.capacity().max(4);
        scratch.admit(
            additional
                .checked_mul(std::mem::size_of::<T>())
                .and_then(|n| n.checked_add(ALLOCATION_BYTES))
                .ok_or_else(invalid)?,
        )?;
        list.reserve_exact(additional);
        Ok(())
    }

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
        let mut scratch = Scratch::new(memory.clone());
        let mut keys: Vec<String> = Vec::new();
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
                    reserve_admitted(&mut keys, &mut scratch)?;
                    scratch.admit_key(key)?;
                    keys.push(key.to_owned());
                }
                Ok(())
            })?;
            if page < PENDING_PAGE {
                break;
            }
            after = next;
        }
        let answer = answer(r, &keys, memory, db)?;
        drop(keys);
        drop(scratch);
        Ok(answer)
    }

    /// The fixed part of one delta or committed entry: its key copy and its map
    /// slot. The decoded body is charged by the decoder as it allocates.
    fn entry_bytes(key: &str) -> Result<usize> {
        key.len()
            .checked_add(std::mem::size_of::<(String, Option<Record>)>())
            .and_then(|n| n.checked_add(std::mem::size_of::<(Option<Record>, usize)>()))
            .and_then(|n| n.checked_add(2 * ALLOCATION_BYTES))
            .ok_or_else(invalid)
    }
    /// Decode, check and verify one stored body under `memory`, its decoded
    /// allocations taken over by `owner`. The canonical re-encode and the
    /// rendered key are admitted transiently first.
    fn decode_checked(
        memory: &Arc<ImageMemory>,
        owner: &mut Scratch,
        key: &str,
        bytes: &[u8],
    ) -> Result<Record> {
        let mut workspace = Scratch::new(memory.clone());
        workspace.admit(
            bytes
                .len()
                .checked_add(key.len())
                .and_then(|n| n.checked_add(2 * ALLOCATION_BYTES))
                .ok_or_else(invalid)?,
        )?;
        work::validation_decoded(1);
        let (record, charged) = decode_charged(memory, bytes)?;
        owner.adopt(charged)?;
        if encode(&record)? != bytes || record.key()? != key {
            return Err(invalid());
        }
        drop(workspace);
        verify_stored(memory, key, bytes)?;
        Ok(record)
    }
    /// Upper bound of the group strings one record of `encoded` stored bytes
    /// joins: each is a short prefix and a hex rendering of the record's own
    /// content, so all together stay within four times its encoding.
    fn group_bytes(record: &Record, encoded: usize) -> Result<usize> {
        let count = match record {
            Record::Manifest(m) => m.rows.len().checked_add(2).ok_or_else(invalid)?,
            Record::Terminal { .. } => 3,
            Record::Certificate(_) => 2,
            _ => 3,
        };
        count
            .checked_mul(std::mem::size_of::<String>() + 2 * ALLOCATION_BYTES + 96)
            .and_then(|n| n.checked_add(encoded.checked_mul(4)?))
            .ok_or_else(invalid)
    }

    /// Touched-record validation over the committed journal plus `delta`.
    /// Affected keys are the delta and the durable members of every group a
    /// touched record joins or leaves; companions are point reads. Every key,
    /// map entry, decoded body, group list, plan and verification workspace is
    /// admitted before it is allocated. The returned delta's own bytes stay
    /// admitted in the returned charge until the caller publishes or aborts.
    pub(super) fn validate_delta(
        r: &CustodyReader,
        writes: &[(String, Vec<u8>)],
        deletes: &[String],
        memory: Arc<ImageMemory>,
        db: &crate::Database,
    ) -> Result<(BTreeMap<String, Option<Record>>, u64, Scratch)> {
        let mut retained = Scratch::new(memory.clone());
        let mut scratch = Scratch::new(memory.clone());
        let mut delta = BTreeMap::new();
        for key in deletes.iter().filter(|k| owned_key(k)) {
            retained.admit(entry_bytes(key)?)?;
            delta.insert(key.clone(), None);
        }
        for (key, bytes) in writes.iter().filter(|(k, _)| owned_key(k)) {
            // An unchanged committed record (for example the authority
            // certificate every unit restates) cannot change its dependents.
            if !delta.contains_key(key) && r.get(key, |old| Ok(old == Some(bytes.as_slice())))? {
                continue;
            }
            retained.admit(entry_bytes(key)?)?;
            let record = decode_checked(&memory, &mut retained, key, bytes)?;
            delta.insert(key.clone(), Some(record));
        }
        // Committed records this validation reads, each decoded once.
        let mut committed: BTreeMap<String, (Option<Record>, usize)> = BTreeMap::new();
        let fetch = |committed: &mut BTreeMap<String, (Option<Record>, usize)>,
                     scratch: &mut Scratch,
                     key: &str|
         -> Result<()> {
            if committed.contains_key(key) {
                return Ok(());
            }
            let entry = r.get(key, |value| {
                let encoded = value.map_or(0, <[u8]>::len);
                scratch.admit(entry_bytes(key)?)?;
                let record = value
                    .map(|bytes| decode_checked(&memory, scratch, key, bytes))
                    .transpose()?;
                Ok((record, encoded))
            })?;
            committed.insert(key.to_owned(), entry);
            Ok(())
        };
        let mut affected = BTreeSet::new();
        for (key, next) in &delta {
            if !affected.contains(key) {
                scratch.admit_key(key)?;
                affected.insert(key.clone());
            }
            fetch(&mut committed, &mut scratch, key)?;
            let (old, old_encoded) = committed
                .get(key)
                .map(|(record, encoded)| (record.as_ref(), *encoded))
                .unwrap_or((None, 0));
            let next_encoded = writes
                .iter()
                .find(|(k, _)| k == key)
                .map_or(0, |(_, bytes)| bytes.len());
            for (record, encoded) in old
                .map(|r| (r, old_encoded))
                .into_iter()
                .chain(next.as_ref().map(|r| (r, next_encoded)))
            {
                let mut lists = Scratch::new(memory.clone());
                lists.admit(group_bytes(record, encoded)?)?;
                let mut joined = groups(record)?
                    .into_iter()
                    .filter(|g| {
                        !matches!(record, Record::ObjectAdmission(_))
                            || !g.starts_with("certificate:")
                    })
                    .collect::<Vec<_>>();
                if let Record::Certificate(c) = record {
                    joined.push(certificate_group(&c.id_finite()?));
                }
                for group in &joined {
                    r.visit_group(group, &memory, |member| {
                        if !affected.contains(member) {
                            scratch.admit_key(member)?;
                            affected.insert(member.to_owned());
                        }
                        Ok(())
                    })?;
                }
                drop(joined);
                drop(lists);
            }
        }
        let visits = affected.len() as u64;
        for key in &affected {
            if !delta.contains_key(key) {
                fetch(&mut committed, &mut scratch, key)?;
            }
        }
        scratch.admit(
            affected
                .len()
                .checked_mul(std::mem::size_of::<(&String, BTreeSet<String>)>())
                .and_then(|n| n.checked_add(ALLOCATION_BYTES))
                .ok_or_else(invalid)?,
        )?;
        let encoded = |key: &str| {
            writes
                .iter()
                .find(|(k, _)| k == key)
                .map_or(0, |(_, bytes)| bytes.len())
        };
        let mut plans = Vec::with_capacity(affected.len());
        for key in &affected {
            if let Some(record) = current(&delta, &committed, key) {
                plans.push((
                    key,
                    companion_keys(r, record, &delta, &encoded, &mut scratch)?,
                ));
            }
        }
        for (_, companions) in &plans {
            for key in companions {
                if !delta.contains_key(key) {
                    fetch(&mut committed, &mut scratch, key)?;
                }
            }
        }
        for (key, companions) in &plans {
            let record = current(&delta, &committed, key).ok_or_else(invalid)?;
            // The companion view; the admitted verifier funds its own work.
            let mut view_memory = Scratch::new(memory.clone());
            view_memory.admit(
                companions
                    .len()
                    .checked_mul(std::mem::size_of::<&Record>())
                    .and_then(|n| n.checked_add(ALLOCATION_BYTES))
                    .ok_or_else(invalid)?,
            )?;
            let view = companions
                .iter()
                .filter_map(|key| current(&delta, &committed, key))
                .collect::<Vec<_>>();
            validate_record_owned(db, record, &view)?;
            drop(view);
            drop(view_memory);
        }
        drop(plans);
        drop(committed);
        drop(affected);
        drop(scratch);
        Ok((delta, visits, retained))
    }

    /// The in-commit record at `key`, else the committed one.
    fn current<'a>(
        delta: &'a BTreeMap<String, Option<Record>>,
        committed: &'a BTreeMap<String, (Option<Record>, usize)>,
        key: &str,
    ) -> Option<&'a Record> {
        match delta.get(key) {
            Some(next) => next.as_ref(),
            None => committed.get(key).and_then(|(record, _)| record.as_ref()),
        }
    }

    /// The longest companion selector: `delivery_peer_evidence.v1.` and a
    /// 32-byte hex digest. Every selector is a short prefix and one digest or
    /// identifier, so this bounds each one before it is formatted.
    const SELECTOR_MAX: usize = 96;

    /// The same companion selectors the resident index answers, read durably
    /// and overlaid with the in-commit delta. Selector strings, candidate group
    /// lists and the answered keys are admitted to `scratch` before they are
    /// allocated; the terminal's object digest is computed without an encoding
    /// buffer. `encoded` names each delta record's stored length.
    fn companion_keys(
        r: &CustodyReader,
        record: &Record,
        delta: &BTreeMap<String, Option<Record>>,
        encoded: &dyn Fn(&str) -> usize,
        scratch: &mut Scratch,
    ) -> Result<BTreeSet<String>> {
        // Two selectors, each with its one intermediate digest rendering.
        scratch.admit(
            4usize
                .checked_mul(std::mem::size_of::<String>() + SELECTOR_MAX + ALLOCATION_BYTES)
                .and_then(|n| n.checked_add(ALLOCATION_BYTES))
                .ok_or_else(invalid)?,
        )?;
        let selectors: Vec<String> = match record {
            Record::Control(_) => vec!["@authority".into()],
            Record::ObjectAdmission(a) => vec![
                format!("@{}", object_group(&a.object_digest)),
                format!("delivery_peer_evidence.v1.{}", hex(&a.certificate)),
            ],
            Record::Terminal { record: t, .. } => vec![
                format!("@{}", submission_group(t.submission)),
                format!(
                    "@{}",
                    object_group(&super::super::authority::object_digest_finite(
                        1,
                        |e| t.bytes_finite(e),
                        &t.signature,
                    )?)
                ),
            ],
            Record::Root { submission, .. } => vec![format!("@{}", submission_group(*submission))],
            Record::MaterializedOwner(o) => vec![format!("@{}", submission_group(o.submission))],
            _ => Vec::new(),
        };
        if selectors.iter().any(|s| s.len() > SELECTOR_MAX) {
            return Err(invalid());
        }
        let memory = scratch.memory.clone();
        let mut keys = BTreeSet::new();
        for selector in &selectors {
            visit_selector(r, selector, &memory, &mut |key| {
                if !keys.contains(key) {
                    scratch.admit_key(key)?;
                    keys.insert(key.to_owned());
                }
                Ok(())
            })?;
        }
        for (key, candidate) in delta {
            if let Some(candidate) = candidate {
                // The candidate's group list, admitted for its own lifetime.
                let mut lists = Scratch::new(memory.clone());
                lists.admit(group_bytes(candidate, encoded(key))?)?;
                let candidate_groups = groups(candidate)?;
                let joins = selectors.iter().any(|s| match s.strip_prefix('@') {
                    Some(group) => candidate_groups.iter().any(|g| g == group),
                    None => key.starts_with(s.as_str()),
                });
                drop(candidate_groups);
                drop(lists);
                if joins && !keys.contains(key) {
                    scratch.admit_key(key)?;
                    keys.insert(key.clone());
                }
            }
        }
        Ok(keys)
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
