//! Private derived pending-delivery membership, maintained in the same write
//! transaction as each custody CONFIG entry. Never CONFIG, content identity or
//! export data. It exists exactly when the custody group index is complete.
//!
//! A local root is pending until an edge terminal for its submission, at the
//! registered destination, is committed. A terminal excludes its root only
//! once this maintenance has verified it here, at the transaction's own
//! committed view: its signature, its diagnostic block and its object
//! admission with that admission's certificate. The only allocation is each
//! signature's canonical input, funded by the transaction's proof admission.
//! A terminal that fails verification never excludes its root: its
//! submission is recorded as damaged, and every pending read refuses as
//! corruption while any damage is recorded, without reading delivered
//! history. An evidence record that changes re-verifies the terminals that
//! depend on it.
use super::*;
use crate::custody::native_record_codec::decode_record;
use crate::custody::native_records::{Namespace, RecordView};

/// Root ownership key -> its 16-byte submission.
const ROOT_KEYS: TableDefinition<&str, &[u8]> = TableDefinition::new("read_custody_pending_roots");
/// Submission (16 bytes) -> empty: a current local root exists.
const SUBMISSIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("read_custody_pending_submissions");
/// Submission (16 bytes) -> empty: that root is still pending delivery.
pub(super) const PENDING: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("read_custody_pending");
/// Submission (16 bytes) -> empty: its edge terminal at the destination failed
/// verification. Any entry makes pending reads refuse as corruption.
pub(super) const DAMAGED: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("read_custody_pending_damaged");

const ROOT_PREFIX: &str = "delivery_root.v1.";
const EDGE_OUTCOME_PREFIX: &str = "delivery_edge_outcome.v1.";
const DESTINATION_KEY: &str = "delivery_destination.v1";
const DIAGNOSTIC_PREFIX: &str = "delivery_diagnostic.v1.";
const ADMISSION_PREFIX: &str = "delivery_object_admission.v1.";
const CERTIFICATE_PREFIX: &str = "delivery_peer_evidence.v1.";

/// The proof admission's owner for the signature inputs verification renders.
pub(super) type Owner = contextdb_core::read_memory::ReadMemoryOwner;

fn damage<T>(_: T) -> Error {
    crate::custody::canonical::invalid()
}

/// Create the empty tables alongside a newly complete group index.
pub(super) fn create(write: &redb::WriteTransaction) -> Result<()> {
    for result in [
        write.delete_table(ROOT_KEYS).map(|_| ()),
        write.delete_table(SUBMISSIONS).map(|_| ()),
        write.delete_table(PENDING).map(|_| ()),
        write.delete_table(DAMAGED).map(|_| ()),
    ] {
        match result {
            Ok(()) | Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(RedbPersistence::storage_error(error)),
        }
    }
    write
        .open_table(ROOT_KEYS)
        .map_err(RedbPersistence::storage_error)?;
    write
        .open_table(SUBMISSIONS)
        .map_err(RedbPersistence::storage_error)?;
    write
        .open_table(PENDING)
        .map_err(RedbPersistence::storage_error)?;
    write
        .open_table(DAMAGED)
        .map_err(RedbPersistence::storage_error)?;
    Ok(())
}

fn index_complete(write: &redb::WriteTransaction) -> Result<bool> {
    if write
        .table_root_fingerprint(custody_groups::GROUP_TABLE)
        .map_err(RedbPersistence::storage_error)?
        .is_none()
    {
        return Ok(false);
    }
    use redb::ReadableTable;
    Ok(write
        .open_table(custody_groups::GROUP_TABLE)
        .map_err(RedbPersistence::storage_error)?
        .get(custody_groups::COMPLETE)
        .map_err(RedbPersistence::storage_error)?
        .is_some_and(|v| v.value() == custody_groups::COMPLETE_VALUE))
}

/// Parse the trailing 32-hex-digit submission of an edge outcome key.
fn outcome_submission(key: &str) -> Result<[u8; 16]> {
    let hex = key
        .rsplit('.')
        .next()
        .ok_or_else(crate::custody::canonical::invalid)?;
    let mut out = [0u8; 16];
    if hex.len() != 32 {
        return Err(crate::custody::canonical::invalid());
    }
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).map_err(damage)?;
    }
    Ok(out)
}

/// Parse `N` bytes of lowercase hex from the start of `hex`.
fn parse_hex<const N: usize>(hex: &str) -> Result<[u8; N]> {
    let hex = hex
        .get(..2 * N)
        .ok_or_else(crate::custody::canonical::invalid)?;
    let mut out = [0u8; N];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).map_err(damage)?;
    }
    Ok(out)
}

/// A group head or key `[u64 BE len][prefix][lowercase hex of digest]`, on
/// the stack; prefixes are at most 32 bytes.
struct DigestHead {
    bytes: [u8; 8 + 32 + 64],
    len: usize,
}
impl DigestHead {
    fn new(prefix: &str, digest: &[u8; 32]) -> Self {
        debug_assert!(prefix.len() <= 32);
        let name = prefix.len() + 64;
        let mut bytes = [0u8; 8 + 32 + 64];
        bytes[..8].copy_from_slice(&(name as u64).to_be_bytes());
        bytes[8..8 + prefix.len()].copy_from_slice(prefix.as_bytes());
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for (i, byte) in digest.iter().enumerate() {
            bytes[8 + prefix.len() + 2 * i] = HEX[usize::from(byte >> 4)];
            bytes[8 + prefix.len() + 2 * i + 1] = HEX[usize::from(byte & 0x0f)];
        }
        Self {
            bytes,
            len: 8 + name,
        }
    }
    fn head(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

/// Visit the member keys of the group whose head is `head`.
fn visit_members(
    groups: &impl redb::ReadableTable<&'static [u8], &'static [u8]>,
    head: &[u8],
    mut visit: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    for entry in groups
        .range::<&[u8]>(head..)
        .map_err(RedbPersistence::storage_error)?
    {
        let (member, _) = entry.map_err(RedbPersistence::storage_error)?;
        let Some(member) = member.value().strip_prefix(head) else {
            break;
        };
        visit(std::str::from_utf8(member).map_err(damage)?)?;
    }
    Ok(())
}

/// Submissions whose edge terminals depend on the evidence record `key`.
fn dependent_submissions(
    groups: &impl redb::ReadableTable<&'static [u8], &'static [u8]>,
    key: &str,
    mut found: impl FnMut([u8; 16]) -> Result<()>,
) -> Result<()> {
    let terminals_of_object = |object: &[u8; 32], found: &mut dyn FnMut([u8; 16]) -> Result<()>| {
        visit_members(
            groups,
            DigestHead::new("object:", object).head(),
            |member| {
                if member.starts_with(EDGE_OUTCOME_PREFIX) {
                    found(outcome_submission(member)?)?;
                }
                Ok(())
            },
        )
    };
    if let Some(rest) = key.strip_prefix(DIAGNOSTIC_PREFIX) {
        let hex = rest
            .rsplit('.')
            .next()
            .ok_or_else(crate::custody::canonical::invalid)?;
        if hex.len() != 32 {
            return Err(crate::custody::canonical::invalid());
        }
        found(parse_hex::<16>(hex)?)?;
    } else if let Some(rest) = key.strip_prefix(ADMISSION_PREFIX) {
        terminals_of_object(&parse_hex::<32>(rest)?, &mut found)?;
    } else if let Some(rest) = key.strip_prefix(CERTIFICATE_PREFIX) {
        let certificate = parse_hex::<32>(rest)?;
        visit_members(
            groups,
            DigestHead::new("certificate:", &certificate).head(),
            |member| {
                if let Some(rest) = member.strip_prefix(ADMISSION_PREFIX) {
                    terminals_of_object(&parse_hex::<32>(rest)?, &mut found)?;
                }
                Ok(())
            },
        )?;
    } else if key.starts_with(EDGE_OUTCOME_PREFIX) {
        found(outcome_submission(key)?)?;
    }
    Ok(())
}

/// Maintain pending membership for one committed custody CONFIG entry. When
/// `defer` is set the caller recomputes every submission once at the end.
pub(super) fn maintain(
    write: &redb::WriteTransaction,
    key: &str,
    value: Option<&[u8]>,
    defer: bool,
    owner: &Owner,
) -> Result<()> {
    use redb::ReadableTable;
    if !index_complete(write)? {
        return Ok(());
    }
    if key.starts_with(ROOT_PREFIX) {
        let mut roots = write
            .open_table(ROOT_KEYS)
            .map_err(RedbPersistence::storage_error)?;
        let mut submissions = write
            .open_table(SUBMISSIONS)
            .map_err(RedbPersistence::storage_error)?;
        let mut pending = write
            .open_table(PENDING)
            .map_err(RedbPersistence::storage_error)?;
        let mut damaged = write
            .open_table(DAMAGED)
            .map_err(RedbPersistence::storage_error)?;
        let old = roots
            .remove(key)
            .map_err(RedbPersistence::storage_error)?
            .map(|old| <[u8; 16]>::try_from(old.value()).map_err(damage))
            .transpose()?;
        if let Some(old) = old {
            submissions
                .remove(old.as_slice())
                .map_err(RedbPersistence::storage_error)?;
            pending
                .remove(old.as_slice())
                .map_err(RedbPersistence::storage_error)?;
            damaged
                .remove(old.as_slice())
                .map_err(RedbPersistence::storage_error)?;
        }
        let Some(value) = value else {
            return Ok(());
        };
        let RecordView::Root { submission, .. } = decode_record(value).map_err(damage)? else {
            return Err(crate::custody::canonical::invalid());
        };
        let submission = *submission.as_bytes();
        roots
            .insert(key, submission.as_slice())
            .map_err(RedbPersistence::storage_error)?;
        submissions
            .insert(submission.as_slice(), b"".as_slice())
            .map_err(RedbPersistence::storage_error)?;
        drop((roots, submissions, pending, damaged));
        if !defer {
            recompute(write, &[submission], owner)?;
        }
        return Ok(());
    }
    if defer {
        return Ok(());
    }
    if key == DESTINATION_KEY {
        return recompute_all(write, owner);
    }
    // An edge terminal, or evidence an edge terminal's exclusion rests on:
    // re-verify every current submission that depends on it.
    let mut affected = [[0u8; 16]; RECHECK_BATCH];
    let mut count = 0usize;
    let mut overflow = false;
    {
        let groups = write
            .open_table(custody_groups::GROUP_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        let submissions = write
            .open_table(SUBMISSIONS)
            .map_err(RedbPersistence::storage_error)?;
        dependent_submissions(&groups, key, |submission| {
            if submissions
                .get(submission.as_slice())
                .map_err(RedbPersistence::storage_error)?
                .is_none()
                || affected[..count].contains(&submission)
            {
                return Ok(());
            }
            if count == RECHECK_BATCH {
                overflow = true;
            } else {
                affected[count] = submission;
                count += 1;
            }
            Ok(())
        })?;
    }
    if overflow {
        // Evidence shared by more terminals than one fixed batch: recompute
        // every current submission, still proportional to current roots.
        return recompute_all(write, owner);
    }
    recompute(write, &affected[..count], owner)
}

/// Submissions one evidence change re-verifies before falling back to all.
const RECHECK_BATCH: usize = 64;

/// Recompute every current submission: a destination change, or the end of a
/// rebuild. Proportional to current local roots, and only at those events.
pub(super) fn recompute_all(write: &redb::WriteTransaction, owner: &Owner) -> Result<()> {
    use redb::ReadableTable;
    if !index_complete(write)? {
        return Ok(());
    }
    let submissions = write
        .open_table(SUBMISSIONS)
        .map_err(RedbPersistence::storage_error)?;
    let config = write
        .open_table(CONFIG_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let groups = write
        .open_table(custody_groups::GROUP_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let mut pending = write
        .open_table(PENDING)
        .map_err(RedbPersistence::storage_error)?;
    let mut damaged = write
        .open_table(DAMAGED)
        .map_err(RedbPersistence::storage_error)?;
    let destination = config
        .get(DESTINATION_KEY)
        .map_err(RedbPersistence::storage_error)?;
    let destination = destination
        .as_ref()
        .map(
            |value| match decode_record(value.value()).map_err(damage)? {
                RecordView::Destination(d) => Ok(d.namespace),
                _ => Err(crate::custody::canonical::invalid()),
            },
        )
        .transpose()?;
    for entry in submissions.iter().map_err(RedbPersistence::storage_error)? {
        let (submission, _) = entry.map_err(RedbPersistence::storage_error)?;
        let submission = <[u8; 16]>::try_from(submission.value()).map_err(damage)?;
        set_pending(
            &groups,
            &config,
            &mut pending,
            &mut damaged,
            destination.as_ref(),
            submission,
            owner,
        )?;
    }
    Ok(())
}

fn recompute(write: &redb::WriteTransaction, which: &[[u8; 16]], owner: &Owner) -> Result<()> {
    use redb::ReadableTable;
    if which.is_empty() {
        return Ok(());
    }
    let config = write
        .open_table(CONFIG_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let groups = write
        .open_table(custody_groups::GROUP_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let mut pending = write
        .open_table(PENDING)
        .map_err(RedbPersistence::storage_error)?;
    let mut damaged = write
        .open_table(DAMAGED)
        .map_err(RedbPersistence::storage_error)?;
    let destination = config
        .get(DESTINATION_KEY)
        .map_err(RedbPersistence::storage_error)?;
    let destination = destination
        .as_ref()
        .map(
            |value| match decode_record(value.value()).map_err(damage)? {
                RecordView::Destination(d) => Ok(d.namespace),
                _ => Err(crate::custody::canonical::invalid()),
            },
        )
        .transpose()?;
    for submission in which {
        set_pending(
            &groups,
            &config,
            &mut pending,
            &mut damaged,
            destination.as_ref(),
            *submission,
            owner,
        )?;
    }
    Ok(())
}

fn same_namespace(a: &Namespace<'_>, b: &Namespace<'_>) -> bool {
    a.tenant == b.tenant
        && a.hub_node == b.hub_node
        && a.hub_incarnation == b.hub_incarnation
        && a.edge_node == b.edge_node
        && a.edge_incarnation == b.edge_incarnation
}

/// A verification fault: damaged evidence (`Ok(false)`) or a real refusal.
fn verdict(result: crate::custody::native_selection::Selected<()>) -> Result<bool> {
    use crate::custody::native_selection::Failure;
    match result {
        Ok(()) => Ok(true),
        Err(Failure::Invalid(_)) => Ok(false),
        Err(Failure::Refused(refusal)) => Err(Error::Other(refusal.to_string())),
        Err(Failure::Unqualified(error)) => Err(error),
        Err(Failure::ColdUnqualified(error)) => Err(Error::Other(format!("{error:?}"))),
        Err(Failure::OwnerMismatch) => Err(Error::Other(
            "custody pending verification owner mismatch".into(),
        )),
    }
}

/// Whether the edge terminal `record` (from submission group `head`) carries
/// its complete evidence at this view: a valid signature, the matching
/// diagnostic block, and an object admission whose certificate is present,
/// valid and agrees with it. The same checks commit validation applies.
fn terminal_verified(
    groups: &impl redb::ReadableTable<&'static [u8], &'static [u8]>,
    config: &impl redb::ReadableTable<&'static str, &'static [u8]>,
    head: &[u8],
    terminal: &RecordView<'_>,
    owner: &Owner,
) -> Result<bool> {
    use crate::custody::native_record_codec::{finite_hash, finite_len, state_birth_eq, state_eq};
    let RecordView::Terminal { record: t, .. } = terminal else {
        return Ok(false);
    };
    if !verdict(terminal.verify_native(owner))? {
        return Ok(false);
    }
    // The diagnostic block shares the submission group.
    let mut diagnostic = false;
    visit_members(groups, head, |member| {
        if diagnostic || !member.starts_with(DIAGNOSTIC_PREFIX) {
            return Ok(());
        }
        // Mid-transaction, an entry this commit already removed can still
        // be a group member until its own maintenance runs: it is absent.
        let Some(value) = config.get(member).map_err(RedbPersistence::storage_error)? else {
            return Ok(());
        };
        if let Ok(RecordView::Diagnostic {
            namespace,
            submission,
            block,
        }) = decode_record(value.value())
            && same_namespace(&namespace, &t.namespace)
            && submission == t.submission
        {
            let matches = (|| -> crate::custody::native_decode::Decoded<bool> {
                Ok(t.diagnostics.subject_count == block.subjects.len() as u64
                    && t.diagnostics.conflict_count == block.conflicts.len() as u64
                    && t.diagnostics.canonical_bytes
                        == finite_len(|e| block.bytes_finite(e))? as u64
                    && t.diagnostics.commitment
                        == finite_hash(|e| {
                            e.string("delivery-diagnostics.v1");
                            block.bytes_finite(e)
                        })?)
            })();
            diagnostic = matches.unwrap_or(false);
        }
        Ok(())
    })?;
    if !diagnostic {
        return Ok(false);
    }
    // The object admission shares the terminal's object group.
    let object = match (|| {
        finite_hash(|e| {
            e.string("delivery-authority-object.v1");
            e.u8(1);
            e.u64(finite_len(|e| t.bytes_finite(e))? as u64);
            t.bytes_finite(e)?;
            e.bytes(&t.signature);
            Ok(())
        })
    })() {
        Ok(object) => object,
        Err(_) => return Ok(false),
    };
    let mut admitted = false;
    visit_members(
        groups,
        DigestHead::new("object:", &object).head(),
        |member| {
            if admitted || !member.starts_with(ADMISSION_PREFIX) {
                return Ok(());
            }
            // Mid-transaction, an entry this commit already removed can still
            // be a group member until its own maintenance runs: it is absent.
            let Some(value) = config.get(member).map_err(RedbPersistence::storage_error)? else {
                return Ok(());
            };
            let Ok(RecordView::ObjectAdmission(a)) = decode_record(value.value()) else {
                return Ok(());
            };
            if a.object_digest != object {
                return Ok(());
            }
            let certificate_key = DigestHead::new(CERTIFICATE_PREFIX, &a.certificate);
            let certificate_key =
                std::str::from_utf8(&certificate_key.head()[8..]).map_err(damage)?;
            let Some(stored) = config
                .get(certificate_key)
                .map_err(RedbPersistence::storage_error)?
            else {
                return Ok(());
            };
            let Ok(certificate) = decode_record(stored.value()) else {
                return Ok(());
            };
            let RecordView::Certificate(c) = &certificate else {
                return Ok(());
            };
            if c.id_finite().ok() != Some(a.certificate)
                || !state_eq(&c.state.state, &a.owning_state)
                || c.tenant != a.authority.namespace.tenant
                || c.hub_node != a.authority.namespace.hub_node
                || !state_birth_eq(&a.owning_state, &a.authority.birth)
            {
                return Ok(());
            }
            admitted = verdict(certificate.verify_native(owner))?;
            Ok(())
        },
    )?;
    Ok(admitted)
}

/// Pending exactly when no verified edge terminal for `submission` at the
/// registered destination exists in its submission group. A terminal there
/// that fails verification records the submission as damaged instead of
/// excluding it.
fn set_pending(
    groups: &impl redb::ReadableTable<&'static [u8], &'static [u8]>,
    config: &impl redb::ReadableTable<&'static str, &'static [u8]>,
    pending: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
    damaged: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
    destination: Option<&Namespace<'_>>,
    submission: [u8; 16],
    owner: &Owner,
) -> Result<()> {
    // `[u64 BE 47]["submission:" + hyphenated uuid]`, built on the stack.
    const GROUP_LEN: usize = 11 + 36;
    let mut head = [0u8; 8 + GROUP_LEN];
    head[..8].copy_from_slice(&(GROUP_LEN as u64).to_be_bytes());
    head[8..19].copy_from_slice(b"submission:");
    uuid::Uuid::from_bytes(submission)
        .hyphenated()
        .encode_lower(&mut head[19..]);
    let mut held = false;
    let mut broken = false;
    if let Some(destination) = destination {
        visit_members(groups, &head, |member| {
            if held || !member.starts_with(EDGE_OUTCOME_PREFIX) {
                return Ok(());
            }
            // Mid-transaction, an entry this commit already removed can still
            // be a group member until its own maintenance runs: it is absent.
            let Some(value) = config.get(member).map_err(RedbPersistence::storage_error)? else {
                return Ok(());
            };
            let view = match decode_record(value.value()) {
                Ok(view) => view,
                Err(_) => {
                    broken = true;
                    return Ok(());
                }
            };
            if let RecordView::Terminal { edge: true, record } = &view
                && *record.submission.as_bytes() == submission
                && same_namespace(&record.namespace, destination)
            {
                if terminal_verified(groups, config, &head, &view, owner)? {
                    held = true;
                } else {
                    broken = true;
                }
            }
            Ok(())
        })?;
    }
    if held {
        pending
            .remove(submission.as_slice())
            .map_err(RedbPersistence::storage_error)?;
    } else {
        pending
            .insert(submission.as_slice(), b"".as_slice())
            .map_err(RedbPersistence::storage_error)?;
    }
    if broken && !held {
        damaged
            .insert(submission.as_slice(), b"".as_slice())
            .map_err(RedbPersistence::storage_error)?;
    } else {
        damaged
            .remove(submission.as_slice())
            .map_err(RedbPersistence::storage_error)?;
    }
    Ok(())
}
