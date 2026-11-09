//! Compaction moves native tables. Role and qualification proofs are physical
//! root fingerprints, so the proof of a table moves in the same commit as the
//! table. A proof that did not match before the move is left as it was: moving
//! a table must never turn a mismatch into a fresh proof.

#![expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
use super::io::NativeIo;
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Kind(u8),
    Qualification,
}

/// The exact physical names of role and qualification tables, and nothing else:
/// `ivf_scopes` and `ivf_pending_seals` are authority, not rebuildable index data.
fn parse_native_table(name: &str) -> Option<(u64, Role)> {
    let rest = name.strip_prefix("ivf_")?;
    let (hex, suffix) = rest.split_at_checked(16)?;
    let lower_hex = |text: &str| {
        text.bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    };
    if !lower_hex(hex) {
        return None;
    }
    let storage = u64::from_str_radix(hex, 16).ok().filter(|id| *id != 0)?;
    let suffix = suffix.strip_prefix('_')?;
    if suffix == "q" {
        return Some((storage, Role::Qualification));
    }
    if suffix.len() != 2 || !lower_hex(suffix) {
        return None;
    }
    let tag = u8::from_str_radix(suffix, 16).ok()?;
    (1..=16)
        .contains(&tag)
        .then_some((storage, Role::Kind(tag)))
}

pub(in super::super) fn is_native_table(name: &str) -> bool {
    parse_native_table(name).is_some()
}

const OWNER_PREFIX: u8 = b'O';
fn owner_key(storage: u64) -> [u8; 9] {
    let mut key = [OWNER_PREFIX; 9];
    key[1..].copy_from_slice(&storage.to_be_bytes());
    key
}

/// A physical table name carries only its storage id. This one entry per live
/// id finds the owning scope without enumerating scopes.
pub(super) fn publish_owner(
    write: &redb::WriteTransaction,
    storage: u64,
    scope: &[u8; 64],
) -> Result<()> {
    let native = NativeIo::Borrowed(write);
    if native.with_value(SCOPES.name(), &owner_key(storage), |value| {
        Ok(value.is_some())
    })? {
        return Err(corrupt());
    }
    native.put(SCOPES.name(), &owner_key(storage), scope)
}

pub(super) fn remove_owner(write: &redb::WriteTransaction, storage: u64) -> Result<()> {
    let removed = write
        .open_table(SCOPES)
        .map_err(RedbPersistence::storage_error)?
        .remove(owner_key(storage).as_slice())
        .map_err(RedbPersistence::storage_error)?
        .is_some();
    if removed { Ok(()) } else { Err(corrupt()) }
}

pub(in super::super) fn event(
    write: &redb::WriteTransaction,
    name: &str,
    event: redb::TableRootEvent,
) -> std::result::Result<(), redb::StorageError> {
    if !matches!(event, redb::TableRootEvent::AfterRelocate) {
        return Ok(());
    }
    let Some((storage, role)) = parse_native_table(name) else {
        return Ok(());
    };
    carry_proof(write, name, storage, role)
        .map_err(|error| redb::StorageError::AdmissionRefused(Box::new(error)))
}

/// `stored` is rewritten only if it proved the table before this batch moved it.
fn carry(stored: &mut [u8; 32], source: [u8; 32], actual: [u8; 32]) -> bool {
    if *stored != source {
        return false;
    }
    *stored = actual;
    true
}

fn carry_family(
    family: &mut control::Family,
    role: Role,
    source: [u8; 32],
    actual: [u8; 32],
) -> bool {
    match role {
        Role::Kind(tag) => {
            let bit = 1u16 << (tag - 1);
            family.kinds.present & bit != 0
                && carry(
                    &mut family.kinds.roots[usize::from(tag - 1)],
                    source,
                    actual,
                )
        }
        Role::Qualification => family
            .qualification_root
            .as_mut()
            .is_some_and(|root| carry(root, source, actual)),
    }
}

/// One scope key, one encoded page and one decoded record, for one moved table.
const WORKING_BYTES: usize = 64 + PAGE_BYTES + std::mem::size_of::<control::ScopeRecord>();

fn carry_proof(write: &redb::WriteTransaction, name: &str, storage: u64, role: Role) -> Result<()> {
    redb::admit_active_read_memory(WORKING_BYTES).map_err(RedbPersistence::storage_error)?;
    // The scope directory is authority: before one of its records is read or
    // rewritten, it must still match Configuration, and every page the update
    // copies must match its checksum. Otherwise the later rebind would bless it.
    super::super::content_identity::validate_clustered_scope_root_in_write(write)?;
    let path_intact = |key: &[u8]| -> Result<()> {
        if write
            .table_path_checksums_hold(SCOPES, &key)
            .map_err(RedbPersistence::storage_error)?
        {
            Ok(())
        } else {
            Err(Error::StoreCorrupted {
                path: "clustered vector native pages".into(),
                reason: "a scope directory page on the path to this record failed its checksum"
                    .into(),
            })
        }
    };
    let native = NativeIo::Borrowed(write);
    let source = write.relocation_source_root().ok_or_else(corrupt)?;
    let actual = native.fingerprint(name)?.ok_or_else(corrupt)?;
    let mut scope = [0; 64];
    path_intact(&owner_key(storage))?;
    native.with_value(SCOPES.name(), &owner_key(storage), |value| {
        let value = value.ok_or_else(corrupt)?;
        if value.len() != scope.len() {
            return Err(corrupt());
        }
        scope.copy_from_slice(value);
        Ok(())
    })?;
    path_intact(&scope)?;
    let mut record = native.with_value(SCOPES.name(), &scope, |value| {
        control::ScopeRecord::decode_envelope(&scope, value.ok_or_else(corrupt)?)
    })?;
    if record.index[..] != scope[..32] || record.tuple[..] != scope[32..] {
        return Err(corrupt());
    }
    record.validate_controls()?;
    #[cfg(feature = "test-seams")]
    note(|seen| {
        seen.owner_entries_read += 1;
        seen.scopes_read += 1;
    });
    let mut payload = [0; PAGE_BYTES];
    let active = record
        .families
        .iter()
        .position(|family| family.is_some_and(|family| family.storage == storage));
    let carried = if let Some(slot) = active {
        let family = record.families[slot].as_mut().ok_or_else(corrupt)?;
        let carried = carry_family(family, role, source, actual);
        if carried {
            record.encode(&scope, &mut payload)?;
            native.put(SCOPES.name(), &scope, &payload)?;
        }
        carried
    } else {
        let key = control::retired_key(&scope, storage);
        path_intact(&key)?;
        let mut family = native.with_value(SCOPES.name(), &key, |value| {
            control::decode_retired_family(&key, value.ok_or_else(corrupt)?)
        })?;
        if family.storage != storage {
            return Err(corrupt());
        }
        #[cfg(feature = "test-seams")]
        note(|seen| seen.retired_records_read += 1);
        let carried = carry_family(&mut family, role, source, actual);
        if carried {
            control::encode_retired_family(&key, &family, &mut payload)?;
            native.put(SCOPES.name(), &key, &payload)?;
            #[cfg(feature = "test-seams")]
            note(|seen| seen.refreshed_retired += 1);
        }
        carried
    };
    #[cfg(feature = "test-seams")]
    note(|seen| match (carried, role) {
        (false, _) => seen.left_stale += 1,
        (true, Role::Kind(tag)) => seen.refreshed_kinds[usize::from(tag - 1)] += 1,
        (true, Role::Qualification) => seen.refreshed_qualification += 1,
    });
    #[cfg(not(feature = "test-seams"))]
    let _ = carried;
    #[cfg(feature = "test-seams")]
    if FAIL_THIS_CHECKPOINT.with(|armed| armed.replace(false)) {
        // Boundary 28 sits in this same step's checkpoint, after this proof
        // was written: the step must roll back table and proof together.
        super::super::arm_currency_reclaim_fault_for_test(28, false);
    }
    Ok(())
}

#[cfg(feature = "test-seams")]
thread_local! {
    static FAIL_THIS_CHECKPOINT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The next compaction step that carries a native proof fails before it commits.
#[cfg(feature = "test-seams")]
pub(crate) fn arm_checkpoint_failure_after_next_carry_for_test() {
    FAIL_THIS_CHECKPOINT.with(|armed| armed.set(true));
}

#[cfg(feature = "test-seams")]
fn note(update: impl FnOnce(&mut crate::vector_ivf_observations::NativeIvfRootRelocationsForTest)) {
    crate::vector_ivf_observations::NATIVE_ROOT_RELOCATIONS.with(|cell| {
        let mut seen = cell.get();
        update(&mut seen);
        cell.set(seen);
    });
}

#[cfg(feature = "test-seams")]
impl RedbPersistence {
    pub(crate) fn native_ivf_owned_storage_for_test(&self) -> Result<Vec<u64>> {
        use redb::{ReadableDatabase, ReadableTable};
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table = match read.open_table(SCOPES) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut owned = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, _) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                if key.len() == 9 && key[0] == OWNER_PREFIX {
                    owned.push(u64::from_be_bytes(
                        key[1..].try_into().map_err(|_| corrupt())?,
                    ));
                }
            }
            Ok(owned)
        })
    }

    /// Changes the scope directory without rebinding Configuration.
    pub(crate) fn plant_native_ivf_unbound_scope_entry_for_test(&self) -> Result<()> {
        self.with_db(|db| {
            let write = Self::begin_history_write(db)?;
            NativeIo::Borrowed(&write).put(SCOPES.name(), b"Z unbound test entry", b"x")?;
            write.commit().map_err(Self::storage_error)
        })
    }

    pub(crate) fn plant_native_ivf_stale_root_for_test(
        &self,
        index_digest: [u8; 32],
        tuple_digest: [u8; 32],
        storage: u64,
        role: crate::vector_ivf_observations::NativeIvfRootRoleForTest,
    ) -> Result<()> {
        use crate::vector_ivf_observations::NativeIvfRootRoleForTest as Planted;
        self.with_db(|db| {
            let write = Self::begin_history_write(db)?;
            {
                let scope = control::scope_key(index_digest, tuple_digest);
                let native = NativeIo::Borrowed(&write);
                let mut record = native.with_value(SCOPES.name(), &scope, |value| {
                    control::ScopeRecord::decode_envelope(&scope, value.ok_or_else(corrupt)?)
                })?;
                let family = record
                    .families
                    .iter_mut()
                    .flatten()
                    .find(|family| family.storage == storage)
                    .ok_or_else(corrupt)?;
                let root = match role {
                    Planted::Kind(tag) => &mut family.kinds.roots[usize::from(tag - 1)],
                    Planted::Qualification => {
                        family.qualification_root.as_mut().ok_or_else(corrupt)?
                    }
                };
                root[0] ^= 0xFF;
                let mut payload = [0; PAGE_BYTES];
                record.encode(&scope, &mut payload)?;
                native.put(SCOPES.name(), &scope, &payload)?;
            }
            super::super::content_identity::bind_vector_pages_root_in_write(&write)?;
            write.commit().map_err(Self::storage_error)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::io::KindTableName;
    use super::*;

    #[test]
    fn only_the_exact_role_and_qualification_names_are_native_tables() {
        assert_eq!(
            parse_native_table("ivf_0000000000000001_0f"),
            Some((1, Role::Kind(15)))
        );
        assert_eq!(
            parse_native_table("ivf_00000000000000ab_10"),
            Some((0xab, Role::Kind(16)))
        );
        assert_eq!(
            parse_native_table("ivf_0000000000000002_q"),
            Some((2, Role::Qualification))
        );
        for name in [
            "ivf_scopes",
            "ivf_pending_seals",
            "ivf_",
            "ivf_0000000000000000_01",
            "ivf_0000000000000001_00",
            "ivf_0000000000000001_11",
            "ivf_0000000000000001_0F",
            "ivf_000000000000000A_01",
            "ivf_0000000000000001_0",
            "ivf_0000000000000001_011",
            "ivf_0000000000000001_qq",
            "ivf_0000000000000001",
            "rel_items",
        ] {
            assert_eq!(parse_native_table(name), None, "{name}");
        }
    }

    #[test]
    fn every_table_the_engine_names_is_classified_as_it_should_be() {
        for tag in 1..=16u8 {
            let kind = header::kind_from_tag(tag).unwrap();
            let name = KindTableName::new(7, kind).unwrap();
            assert_eq!(
                parse_native_table(name.as_str().unwrap()),
                Some((7, Role::Kind(tag)))
            );
        }
        let name = qualification::TableName::new(7).unwrap();
        assert_eq!(
            parse_native_table(name.as_str().unwrap()),
            Some((7, Role::Qualification))
        );
        assert!(!is_native_table(SCOPES.name()));
        assert!(!is_native_table(io::PENDING.name()));
    }

    #[test]
    fn a_proof_carries_only_if_it_matched_before_the_move() {
        let (source, actual) = ([1; 32], [2; 32]);
        let mut matching = source;
        assert!(carry(&mut matching, source, actual));
        assert_eq!(matching, actual);
        let mut stale = [3; 32];
        assert!(!carry(&mut stale, source, actual));
        assert_eq!(stale, [3; 32]);
    }
}
