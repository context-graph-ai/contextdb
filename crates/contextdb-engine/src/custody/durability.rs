//! Statement 11: a scoped observation/fault at the actual row/outcome commit.
#[cfg(feature = "test-seams")]
use super::records::*;
#[cfg(feature = "test-seams")]
use contextdb_core::Result;
#[cfg(feature = "test-seams")]
use redb::ReadableTable;

// Statement 11: ordinary push applies on a blocking worker. Scope the test fault to the
// intended unit, across threads, and never let a contact or status write consume it.
#[cfg(feature = "test-seams")]
struct CommitFault {
    root: RowRef,
    digest: [u8; 32],
    reached: bool,
}
#[cfg(feature = "test-seams")]
static BEFORE_COMMIT_FAULT: std::sync::Mutex<Option<CommitFault>> = std::sync::Mutex::new(None);

#[cfg(feature = "test-seams")]
pub(crate) fn arm_commit_fault(root: RowRef, digest: [u8; 32]) {
    *BEFORE_COMMIT_FAULT.lock().unwrap() = Some(CommitFault {
        root,
        digest,
        reached: false,
    });
}

#[cfg(feature = "test-seams")]
pub(crate) fn commit_fault_reached() -> bool {
    BEFORE_COMMIT_FAULT
        .lock()
        .unwrap()
        .as_ref()
        .is_some_and(|fault| fault.reached)
}

#[cfg(feature = "test-seams")]
fn take_commit_fault(transaction: &redb::WriteTransaction) -> Result<bool> {
    let mut fault = BEFORE_COMMIT_FAULT.lock().unwrap();
    let Some(fault) = fault.as_mut().filter(|fault| !fault.reached) else {
        return Ok(false);
    };
    let table = transaction
        .open_table(crate::persistence::CONFIG_TABLE)
        .map_err(crate::persistence::RedbPersistence::storage_error)?;
    let prefix = "delivery_hub_outcome.v1.";
    for entry in table
        .range(prefix..)
        .map_err(crate::persistence::RedbPersistence::storage_error)?
    {
        let (key, value) = entry.map_err(crate::persistence::RedbPersistence::storage_error)?;
        if !key.value().starts_with(prefix) {
            break;
        }
        if let Record::Terminal {
            edge: false,
            record,
        } = decode(value.value())?
            && record.root == fault.root
            && record.unit_digest == fault.digest
        {
            fault.reached = true;
            return Ok(true);
        }
    }
    Ok(false)
}

// Statement 11: called only at the actual shared row/outcome commit boundary.
#[cfg(feature = "test-seams")]
pub(crate) fn before_custody_commit(transaction: &redb::WriteTransaction) -> Result<()> {
    if take_commit_fault(transaction)? {
        return Err(contextdb_core::Error::Other(
            "storage error: injected delivery commit failure".into(),
        ));
    }
    Ok(())
}
