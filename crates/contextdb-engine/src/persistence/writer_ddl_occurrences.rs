//! Exact declaration selectors reconciled at authenticated publication.
use super::*;
use std::ops::Bound;

const SELECTORS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ddl_eligible_occurrences");
const COUNTS: TableDefinition<u64, u64> = TableDefinition::new("ddl_unmarked_lsn_count");
const GENERATION: &str = "sync_ddl_generation.v1.";
const ARRIVAL: &str = "sync_ddl_received_arrival.v1.";
const SUPERSEDED: &str = "sync_ddl_superseded.v1.";

fn damage(reason: &str) -> Error {
    Error::StoreCorrupted {
        path: "ddl_eligible_occurrences".into(),
        reason: reason.into(),
    }
}

fn coordinate(key: &str) -> Result<(Lsn, u32)> {
    let (lsn, ordinal) = key.split_once(':').unwrap_or((key, "000000"));
    let lsn = Lsn(lsn.parse().map_err(|_| damage("invalid occurrence LSN"))?);
    let ordinal: u32 = ordinal
        .parse()
        .map_err(|_| damage("invalid occurrence ordinal"))?;
    validate_ordinal(ordinal)?;
    let canonical = if key.contains(':') {
        format!("{:020}:{ordinal:06}", lsn.0)
    } else {
        format!("{:020}", lsn.0)
    };
    if canonical != key {
        return Err(damage("noncanonical occurrence coordinate"));
    }
    Ok((lsn, ordinal))
}

fn validate_ordinal(ordinal: u32) -> Result<()> {
    if ordinal > 999_999 {
        return Err(damage(
            "occurrence ordinal exceeds canonical six-digit width",
        ));
    }
    Ok(())
}

fn suffix(at: (Lsn, u32)) -> [u8; 12] {
    let mut bytes = [0; 12];
    bytes[..8].copy_from_slice(&at.0.0.to_be_bytes());
    bytes[8..].copy_from_slice(&at.1.to_be_bytes());
    bytes
}

fn state_key(at: (Lsn, u32)) -> [u8; 13] {
    let mut bytes = [0; 13];
    bytes[0] = 0;
    bytes[1..].copy_from_slice(&suffix(at));
    bytes
}

fn prefix(change: &DdlChange) -> Result<Vec<u8>> {
    let (kind, name) = match change {
        DdlChange::CreateTable { name, .. } => (0, name),
        DdlChange::DropTable { name } => (1, name),
        DdlChange::AlterTable { name, .. } => (2, name),
        DdlChange::CreateIndex { name, .. } => (3, name),
        DdlChange::DropIndex { name, .. } => (4, name),
        DdlChange::CreateTrigger { name, .. } => (5, name),
        DdlChange::DropTrigger { name } => (6, name),
        DdlChange::CreateEventType { name, .. } => (7, name),
        DdlChange::CreateSink { name, .. } => (8, name),
        DdlChange::CreateRoute { name, .. } => (9, name),
        DdlChange::DropRoute { name, .. } => (10, name),
        DdlChange::CreateTriggerIncludingSync { name, .. } => (11, name),
    };
    let mut key = Vec::with_capacity(name.len() + 42);
    key.extend_from_slice(&[1, kind]);
    key.extend_from_slice(&(name.len() as u64).to_be_bytes());
    key.extend_from_slice(name.as_bytes());
    let mut hash = blake3::Hasher::new();
    hash.update(b"contextdb-exact-ddl-selector\0");
    struct HashWriter<'a>(&'a mut blake3::Hasher);
    impl std::io::Write for HashWriter<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    bincode::serde::encode_into_std_write(
        change,
        &mut HashWriter(&mut hash),
        bincode::config::standard(),
    )
    .map_err(|_| damage("declaration selector encoding failed"))?;
    key.extend_from_slice(hash.finalize().as_bytes());
    Ok(key)
}

#[derive(Serialize, Deserialize)]
struct State {
    primary: String,
    selector: Vec<u8>,
    eligible: bool,
    unmarked: bool,
}

fn config_occurrence(key: &str) -> Result<Option<(Lsn, u32)>> {
    for prefix in [GENERATION, ARRIVAL, SUPERSEDED] {
        if let Some(rest) = key.strip_prefix(prefix) {
            let (lsn, ordinal) = rest
                .split_once('.')
                .ok_or_else(|| damage("invalid occurrence configuration key"))?;
            return coordinate(&format!("{lsn}:{ordinal}")).map(Some);
        }
    }
    Ok(None)
}

impl RedbPersistence {
    pub(crate) fn validate_ddl_occurrence_ordinal(ordinal: u32) -> Result<()> {
        validate_ordinal(ordinal)
    }

    pub(crate) fn ddl_config_occurrence_coordinate(key: &str) -> Result<(Lsn, u32)> {
        config_occurrence(key)?.ok_or_else(|| damage("invalid occurrence configuration namespace"))
    }

    pub(super) fn initialize_ddl_selectors_in_write(write: &redb::WriteTransaction) -> Result<()> {
        write.delete_table(SELECTORS).map_err(Self::storage_error)?;
        write.delete_table(COUNTS).map_err(Self::storage_error)?;
        write.open_table(SELECTORS).map_err(Self::storage_error)?;
        write.open_table(COUNTS).map_err(Self::storage_error)?;
        Ok(())
    }

    pub(super) fn ddl_selector_fingerprints_in_write(
        write: &redb::WriteTransaction,
    ) -> Result<[[u8; 32]; 2]> {
        if !write.table_exists(SELECTORS).map_err(Self::storage_error)?
            || !write.table_exists(COUNTS).map_err(Self::storage_error)?
        {
            return Err(damage("missing mandatory declaration selector table"));
        }
        Ok([
            write
                .table_root_fingerprint(SELECTORS)
                .map_err(Self::storage_error)?
                .unwrap_or([0; 32]),
            write
                .table_root_fingerprint(COUNTS)
                .map_err(Self::storage_error)?
                .unwrap_or([0; 32]),
        ])
    }

    pub(super) fn reconcile_ddl_selectors_in_write(
        write: &redb::WriteTransaction,
        mutations: &[content_identity::ContentIdentityMutation],
        admission: &mut content_identity::ProofAdmission,
    ) -> Result<()> {
        Self::validate_ddl_selectors_in_write(write)?;
        let mut affected = BTreeMap::<(Lsn, u32), Option<&str>>::new();
        let mut charged = 0;
        for mutation in mutations {
            let primary = content_identity::ddl_mutation_coordinate(mutation);
            let at = if let Some(key) = primary {
                Some(coordinate(key)?)
            } else if mutation.family == content_identity::LogicalFamily::Configuration {
                config_occurrence(
                    std::str::from_utf8(&mutation.logical_key)
                        .map_err(|_| damage("configuration key is not UTF-8"))?,
                )?
            } else {
                None
            };
            if let Some(at) = at {
                if !affected.contains_key(&at) {
                    admission.admit(128)?;
                    charged += 128;
                }
                let slot = affected.entry(at).or_default();
                if let Some(key) = primary {
                    if slot.is_some_and(|old| old != key) {
                        return Err(damage("two primary keys name one occurrence"));
                    }
                    *slot = Some(key);
                }
            }
        }
        if affected.is_empty() {
            return Ok(());
        }
        let mut selectors = write.open_table(SELECTORS).map_err(Self::storage_error)?;
        let mut counts = write.open_table(COUNTS).map_err(Self::storage_error)?;
        let primary = write
            .open_table(DDL_LOG_TABLE)
            .map_err(Self::storage_error)?;
        let config = write
            .open_table(CONFIG_TABLE)
            .map_err(Self::storage_error)?;
        for (at, changed_key) in affected {
            let state_key = state_key(at);
            let old_guard = selectors
                .get(state_key.as_slice())
                .map_err(Self::storage_error)?;
            let old_bytes = old_guard.as_ref().map_or(0, |value| {
                value.value().len().saturating_mul(4).saturating_add(256)
            });
            admission.admit(old_bytes)?;
            let old: Option<State> = old_guard
                .as_ref()
                .map(|value| Self::decode_config_value_exact(value.value()))
                .transpose()?;
            drop(old_guard);
            if let Some(old) = &old {
                if coordinate(&old.primary)? != at
                    || (old.eligible && !old.unmarked)
                    || old.selector.len() < 54
                    || old.selector[0] != 1
                    || old.selector[1] > 11
                    || old.selector[old.selector.len() - 12..] != suffix(at)
                {
                    return Err(damage("invalid occurrence reconciliation state"));
                }
                let name_len = u64::from_be_bytes(old.selector[2..10].try_into().unwrap());
                if usize::try_from(name_len)
                    .ok()
                    .and_then(|length| length.checked_add(54))
                    != Some(old.selector.len())
                {
                    return Err(damage("invalid occurrence selector name framing"));
                }
                if old.eligible {
                    let selected = selectors
                        .get(old.selector.as_slice())
                        .map_err(Self::storage_error)?
                        .ok_or_else(|| damage("eligible occurrence lacks its selector"))?;
                    if selected.value() != old.primary.as_bytes() {
                        return Err(damage(
                            "eligible selector disagrees with reconciliation state",
                        ));
                    }
                }
            }
            let key_bytes = changed_key
                .map_or_else(
                    || old.as_ref().map_or(32, |state| state.primary.len()),
                    str::len,
                )
                .saturating_mul(4)
                .saturating_add(256);
            admission.admit(key_bytes)?;
            let primary_key = changed_key
                .map(str::to_owned)
                .or_else(|| old.as_ref().map(|state| state.primary.clone()))
                .unwrap_or_else(|| format!("{:020}:{:06}", at.0.0, at.1));
            // A singleton and an indexed zero must never coexist.
            let bare = format!("{:020}", at.0.0);
            let indexed = format!("{:020}:{:06}", at.0.0, at.1);
            let bare_value = if at.1 == 0 {
                primary.get(bare.as_str()).map_err(Self::storage_error)?
            } else {
                None
            };
            let indexed_value = primary.get(indexed.as_str()).map_err(Self::storage_error)?;
            if bare_value.is_some() && indexed_value.is_some() {
                return Err(damage("ambiguous singleton occurrence"));
            }
            let found = bare_value.as_ref().or(indexed_value.as_ref());
            let chosen_key = if bare_value.is_some() {
                bare.as_str()
            } else {
                indexed.as_str()
            };
            if found.is_some()
                && chosen_key != primary_key
                && (changed_key.is_some() || old.is_some())
            {
                return Err(damage("occurrence primary key changed spelling"));
            }
            let body_bytes = found.map_or(0, |value| {
                value.value().len().saturating_mul(64).saturating_add(512)
            });
            admission.admit(body_bytes)?;
            let next = if let Some(value) = found {
                let change: DdlChange = Self::decode_config_value_exact(value.value())?;
                let mut selector = prefix(&change)?;
                selector.extend_from_slice(&suffix(at));
                let mut marker = |namespace: &str| -> Result<bool> {
                    let key = format!("{namespace}{:020}.{:06}", at.0.0, at.1);
                    let value = config.get(key.as_str()).map_err(Self::storage_error)?;
                    let Some(value) = value else {
                        return Ok(false);
                    };
                    let bytes = value.value().len().saturating_mul(64).saturating_add(256);
                    admission.admit(bytes)?;
                    crate::database::Database::validate_ddl_occurrence_config(
                        &key,
                        at,
                        value.value(),
                    )
                    .map_err(|_| damage("invalid exact occurrence configuration record"))?;
                    admission.release(bytes);
                    Ok(true)
                };
                let unmarked = !marker(ARRIVAL)? && !marker(SUPERSEDED)?;
                Some(State {
                    primary: chosen_key.to_owned(),
                    selector,
                    eligible: unmarked && marker(GENERATION)?,
                    unmarked,
                })
            } else {
                None
            };
            let count = counts
                .get(at.0.0)
                .map_err(Self::storage_error)?
                .map_or(0, |value| value.value());
            let count = count
                .checked_sub(u64::from(old.as_ref().is_some_and(|state| state.unmarked)))
                .ok_or_else(|| damage("unmarked count underflow"))?
                .checked_add(u64::from(next.as_ref().is_some_and(|state| state.unmarked)))
                .ok_or_else(|| damage("unmarked count overflow"))?;
            if let Some(old) = &old
                && old.eligible
            {
                selectors
                    .remove(old.selector.as_slice())
                    .map_err(Self::storage_error)?;
            }
            if let Some(next) = next {
                if next.eligible {
                    selectors
                        .insert(next.selector.as_slice(), next.primary.as_bytes())
                        .map_err(Self::storage_error)?;
                }
                let encoded = Self::encode(&next)?;
                selectors
                    .insert(state_key.as_slice(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
            } else {
                selectors
                    .remove(state_key.as_slice())
                    .map_err(Self::storage_error)?;
            }
            if count == 0 {
                counts.remove(at.0.0).map_err(Self::storage_error)?;
            } else {
                counts.insert(at.0.0, count).map_err(Self::storage_error)?;
            }
            admission.release(body_bytes + old_bytes + key_bytes);
        }
        admission.release(charged);
        Ok(())
    }

    fn validate_ddl_selectors_in_write(write: &redb::WriteTransaction) -> Result<()> {
        let actual = Self::ddl_selector_fingerprints_in_write(write)?;
        let expected = content_identity::ddl_selector_roots_in_write(write)?;
        if actual != expected {
            return Err(damage("declaration selector root binding mismatch"));
        }
        Ok(())
    }

    pub(super) fn validate_ddl_selectors_in_read(read: &redb::ReadTransaction) -> Result<()> {
        match read.open_table(SELECTORS) {
            Ok(_) => {}
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Err(damage("missing mandatory declaration selector table"));
            }
            Err(error) => return Err(Self::storage_error(error)),
        }
        match read.open_table(COUNTS) {
            Ok(_) => {}
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Err(damage("missing mandatory declaration count table"));
            }
            Err(error) => return Err(Self::storage_error(error)),
        }
        let actual = [
            read.table_root_fingerprint(SELECTORS)
                .map_err(Self::storage_error)?
                .unwrap_or([0; 32]),
            read.table_root_fingerprint(COUNTS)
                .map_err(Self::storage_error)?
                .unwrap_or([0; 32]),
        ];
        if actual != content_identity::ddl_selector_roots_in_read(read)? {
            return Err(damage("declaration selector root binding mismatch"));
        }
        Ok(())
    }

    pub(crate) fn first_eligible_ddl_occurrence(
        root: &WriterSnapshotHandle,
        owner: writer_source::SourceWindowOwner,
        change: &DdlChange,
        after: Option<(Lsn, u32)>,
    ) -> Result<Option<(Lsn, u32)>> {
        if let Some((_, ordinal)) = after {
            validate_ordinal(ordinal)?;
        }
        // The borrowed declaration is already owned by the received vector.
        // Only key/control allocations are new; streaming serialization retains no body copy.
        let name = match change {
            DdlChange::CreateTable { name, .. }
            | DdlChange::DropTable { name }
            | DdlChange::AlterTable { name, .. }
            | DdlChange::CreateIndex { name, .. }
            | DdlChange::DropIndex { name, .. }
            | DdlChange::CreateTrigger { name, .. }
            | DdlChange::DropTrigger { name }
            | DdlChange::CreateEventType { name, .. }
            | DdlChange::CreateSink { name, .. }
            | DdlChange::CreateRoute { name, .. }
            | DdlChange::DropRoute { name, .. }
            | DdlChange::CreateTriggerIncludingSync { name, .. } => name,
        };
        let _charge =
            root.adapter_control(&owner, name.len().saturating_mul(8).saturating_add(2048))?;
        let prefix = prefix(change)?;
        let mut lower = prefix.clone();
        lower.extend_from_slice(&suffix(after.unwrap_or((Lsn(0), 0))));
        let mut upper = prefix.clone();
        upper.extend_from_slice(&[255; 12]);
        let read = root.owner_read()?;
        Self::validate_ddl_selectors_in_read(&read)?;
        let selectors = read.open_table(SELECTORS).map_err(Self::storage_error)?;
        let entry = selectors
            .range::<&[u8]>((
                if after.is_some() {
                    Bound::Excluded(lower.as_slice())
                } else {
                    Bound::Included(lower.as_slice())
                },
                Bound::Included(upper.as_slice()),
            ))
            .map_err(Self::storage_error)?
            .next()
            .transpose()
            .map_err(Self::storage_error)?;
        let Some((key, value)) = entry else {
            return Ok(None);
        };
        if key.value().len() != prefix.len() + 12 || !key.value().starts_with(&prefix) {
            return Err(damage("invalid declaration selector key"));
        }
        let key_bytes = &key.value()[prefix.len()..];
        let at = (
            Lsn(u64::from_be_bytes(key_bytes[..8].try_into().unwrap())),
            u32::from_be_bytes(key_bytes[8..].try_into().unwrap()),
        );
        let primary = std::str::from_utf8(value.value())
            .map_err(|_| damage("invalid declaration primary key"))?;
        if coordinate(primary)? != at {
            return Err(damage("selector coordinate does not bind primary key"));
        }
        let state = selectors
            .get(state_key(at).as_slice())
            .map_err(Self::storage_error)?
            .ok_or_else(|| damage("selector occurrence state is absent"))?;
        let state: State = Self::decode_config_value_exact(state.value())
            .map_err(|_| damage("invalid selector occurrence state"))?;
        if !state.eligible
            || !state.unmarked
            || state.selector != key.value()
            || state.primary.as_bytes() != value.value()
        {
            return Err(damage("selector disagrees with exact occurrence state"));
        }
        let mut cursor = root.cursor(
            owner,
            "ddl_log",
            writer_source::WriterCursorFamily::Text,
            Bound::Included(primary.as_bytes()),
            Bound::Included(primary.as_bytes()),
            false,
        )?;
        let local = cursor
            .next(|_, bytes| {
                crate::read_image_memory::reserve(
                    bytes.len().saturating_mul(64).saturating_add(4096),
                )?;
                Self::decode_config_value_exact::<DdlChange>(bytes)
                    .map_err(|_| damage("invalid exact declaration primary encoding"))
            })?
            .ok_or_else(|| damage("selector primary is absent"))?;
        if &local.value != change {
            return Err(damage("exact declaration selector disagrees with primary"));
        }
        Ok(Some(at))
    }

    pub(crate) fn unmarked_ddl_occurrences(
        root: &WriterSnapshot,
        owner: writer_source::SourceWindowOwner,
        lsn: Lsn,
    ) -> Result<u64> {
        let _charge = root.adapter_control(&owner, 256)?;
        let read = root.owner_read()?;
        Self::validate_ddl_selectors_in_read(&read)?;
        let counts = read.open_table(COUNTS).map_err(Self::storage_error)?;
        Ok(counts
            .get(lsn.0)
            .map_err(Self::storage_error)?
            .map_or(0, |value| value.value()))
    }
}

use crate::persistence::writer_source::{WriterSnapshotHandle};
