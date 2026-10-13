//! Private derived pending-delivery membership, maintained in the same write
//! transaction as each custody CONFIG entry. Never CONFIG, content identity or
//! export data. It exists exactly when the custody group index is complete.
//!
//! A local root is pending until an edge terminal for its submission, at the
//! registered destination, is committed. Every engine path that commits an
//! edge terminal validates it first (decode, key, signature and its required
//! admission/diagnostic companions, `custody::store` `validate_delta`), so
//! this index derives membership from committed records with native views
//! only: maintenance allocates nothing inside the transaction.
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

const ROOT_PREFIX: &str = "delivery_root.v1.";
const EDGE_OUTCOME_PREFIX: &str = "delivery_edge_outcome.v1.";
const DESTINATION_KEY: &str = "delivery_destination.v1";

fn damage<T>(_: T) -> Error {
    crate::custody::canonical::invalid()
}

/// Create the empty tables alongside a newly complete group index.
pub(super) fn create(write: &redb::WriteTransaction) -> Result<()> {
    for result in [
        write.delete_table(ROOT_KEYS).map(|_| ()),
        write.delete_table(SUBMISSIONS).map(|_| ()),
        write.delete_table(PENDING).map(|_| ()),
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

/// Maintain pending membership for one committed custody CONFIG entry. When
/// `defer` is set the caller recomputes every submission once at the end.
pub(super) fn maintain(
    write: &redb::WriteTransaction,
    key: &str,
    value: Option<&[u8]>,
    defer: bool,
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
        drop((roots, submissions, pending));
        if !defer {
            recompute(write, &[submission])?;
        }
        return Ok(());
    }
    if defer {
        return Ok(());
    }
    if key.starts_with(EDGE_OUTCOME_PREFIX) {
        let submission = outcome_submission(key)?;
        let exists = write
            .open_table(SUBMISSIONS)
            .map_err(RedbPersistence::storage_error)?
            .get(submission.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .is_some();
        if exists {
            recompute(write, &[submission])?;
        }
    } else if key == DESTINATION_KEY {
        recompute_all(write)?;
    }
    Ok(())
}

/// Recompute every current submission: a destination change, or the end of a
/// rebuild. Proportional to current local roots, and only at those events.
pub(super) fn recompute_all(write: &redb::WriteTransaction) -> Result<()> {
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
            destination.as_ref(),
            submission,
        )?;
    }
    Ok(())
}

fn recompute(write: &redb::WriteTransaction, which: &[[u8; 16]]) -> Result<()> {
    use redb::ReadableTable;
    let config = write
        .open_table(CONFIG_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let groups = write
        .open_table(custody_groups::GROUP_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let mut pending = write
        .open_table(PENDING)
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
            destination.as_ref(),
            *submission,
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

/// Pending exactly when no committed edge terminal for `submission` at the
/// registered destination exists in its submission group.
fn set_pending(
    groups: &impl redb::ReadableTable<&'static [u8], &'static [u8]>,
    config: &impl redb::ReadableTable<&'static str, &'static [u8]>,
    pending: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
    destination: Option<&Namespace<'_>>,
    submission: [u8; 16],
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
    if let Some(destination) = destination {
        for entry in groups
            .range::<&[u8]>(head.as_slice()..)
            .map_err(RedbPersistence::storage_error)?
        {
            let (member, _) = entry.map_err(RedbPersistence::storage_error)?;
            let Some(member) = member.value().strip_prefix(head.as_slice()) else {
                break;
            };
            let member = std::str::from_utf8(member).map_err(damage)?;
            if !member.starts_with(EDGE_OUTCOME_PREFIX) {
                continue;
            }
            let Some(value) = config.get(member).map_err(RedbPersistence::storage_error)? else {
                return Err(crate::custody::canonical::invalid());
            };
            if let RecordView::Terminal { edge: true, record } =
                decode_record(value.value()).map_err(damage)?
                && *record.submission.as_bytes() == submission
                && same_namespace(&record.namespace, destination)
            {
                held = true;
                break;
            }
        }
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
    Ok(())
}
