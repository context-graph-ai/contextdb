//! Statements 7/11/13/15/17: key-indexed committed metadata and touched-record validation.
//! The index owns each decoded record once. A unit reads/clones only its selected keys;
//! admitting a delta never clones or revalidates the historical journal.
use super::{authority::object_digest, canonical::*, records::*};
use contextdb_core::Result;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

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
    pub fn pending(&self) -> Vec<Record> {
        self.pending
            .iter()
            .filter_map(|id| {
                self.records
                    .get(&format!("delivery_manifest.v1.{}", id.simple()))
                    .cloned()
            })
            .collect()
    }
    pub fn select(&mut self, selectors: &[String]) -> Vec<Record> {
        let keys = self.selected_keys(selectors);
        self.selected_records += keys.len() as u64;
        self.read_visits += keys.len() as u64;
        keys.into_iter()
            .filter_map(|k| self.records.get(&k).cloned())
            .collect()
    }
    pub fn select_table(&mut self, table: &str) -> Vec<Record> {
        let mut keys = BTreeSet::new();
        for (group, members) in self
            .groups
            .range("submission:".to_owned()..)
            .take_while(|(group, _)| group.starts_with("submission:"))
        {
            let _ = group;
            self.read_visits += members.len() as u64;
            if members.iter().any(|key| {
                matches!(self.records.get(key),
                    Some(Record::Manifest(m)) if m.seal.root.table == table)
                    || matches!(self.records.get(key),
                        Some(Record::Root { life, .. }) if life.original_table == table)
                    || matches!(self.records.get(key),
                        Some(Record::Terminal { record, .. }) if record.root.table == table)
            }) {
                keys.extend(members.iter().cloned());
            }
        }
        if let Some(authority) = self.groups.get("authority") {
            keys.extend(authority.iter().cloned());
        }
        self.selected_records += keys.len() as u64;
        keys.into_iter()
            .filter_map(|key| self.records.get(&key).cloned())
            .collect()
    }
    pub fn select_rows(&mut self, rows: &[RowRef]) -> Result<Vec<Record>> {
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
        let objects = keys
            .iter()
            .filter_map(|key| match self.records.get(key) {
                Some(Record::Terminal { record, .. }) => {
                    Some(object_digest(1, &record.bytes().ok()?, &record.signature))
                }
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        for object in objects {
            if let Some(found) = self.groups.get(&object_group(&object)) {
                self.read_visits += found.len() as u64;
                keys.extend(found.iter().cloned());
            }
        }
        self.selected_records += keys.len() as u64;
        Ok(keys
            .into_iter()
            .filter_map(|key| self.records.get(&key).cloned())
            .collect())
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
