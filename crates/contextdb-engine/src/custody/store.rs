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

fn copy_records<'a>(
    source: impl Iterator<Item = &'a Record> + Clone,
    memory: Arc<crate::read_image_memory::ImageMemory>,
) -> Result<AdmittedRecords> {
    struct Count(usize);
    impl std::io::Write for Count {
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
        let mut count = Count(0);
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
            memory.decode_msgpack::<Record>(&bytes)
        })();
        memory.release(credit);
        records.push(decoded?);
    }
    Ok(AdmittedRecords { records, memory })
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
            out.push(object_group(&object_digest(1, &t.bytes()?, &t.signature)));
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
            out.push(object_group(&object_digest(0, &b.bytes()?, &b.signature)));
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

impl Store {
    pub fn from_entries(entries: BTreeMap<String, Vec<u8>>) -> Result<Self> {
        let mut store = Self::default();
        for (key, bytes) in entries {
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
    pub fn insert(&mut self, key: String, record: Record) -> Result<()> {
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
        &self,
        memory: Arc<crate::read_image_memory::ImageMemory>,
    ) -> Result<AdmittedRecords> {
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
    ) -> Result<AdmittedRecords> {
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
    ) -> Result<AdmittedRecords> {
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
    ) -> Result<AdmittedRecords> {
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
    pub fn validate_delta(
        &mut self,
        writes: &[(String, Vec<u8>)],
        deletes: &[String],
    ) -> Result<BTreeMap<String, Option<Record>>> {
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
        Ok(delta)
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
