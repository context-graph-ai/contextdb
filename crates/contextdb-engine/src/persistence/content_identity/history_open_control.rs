//! Fixed startup facts; these are never authenticated proof-node bodies.
use super::super::{COMMIT_INDEX_TABLE, DDL_LOG_TABLE};
use super::*;
use redb::{ReadableTableMetadata, TableHandle};

// Exact fixed layouts admitted before acquisition. Borrowed table payloads and
// their redb pages keep the existing page-admission owner; no payload is copied.
// Both hash states can coexist with the returned controls and fixed bindings.
pub(in super::super) const OPEN_CONTROL_SCRATCH_BYTES: usize =
    std::mem::size_of::<ValidatedHistoryOpenControl>()
        + std::mem::size_of::<PublishedRoot>()
        + std::mem::size_of::<super::super::writer_recovery::AuthenticatedReadBounds<'_>>()
        + std::mem::size_of::<[u8; 130]>()
        + std::mem::size_of::<[u8; 234]>()
        + std::mem::size_of::<blake3::Hasher>()
        + std::mem::size_of::<redb::ReadOnlyTable<u8, &'static [u8]>>()
        + 2 * std::mem::size_of::<redb::ReadOnlyTable<&'static str, &'static [u8]>>()
        + std::mem::size_of::<redb::ReadOnlyTable<u64, u64>>()
        + std::mem::size_of::<redb::ReadOnlyUntypedTable>()
        + 3 * std::mem::size_of::<redb::AccessGuard<'static, &'static [u8]>>()
        + std::mem::size_of::<redb::AccessGuard<'static, &'static str>>()
        + 2 * std::mem::size_of::<Option<(Lsn, TxId)>>();

pub(in super::super) const DIRECTORY_CHECK_SCRATCH_BYTES: usize =
    2 * std::mem::size_of::<PhysicalHistoryTable>()
        + std::mem::size_of::<redb::StorageSnapshotIdentity>()
        + std::mem::size_of::<redb::ReadOnlyUntypedTable>()
        + std::mem::size_of::<redb::ReadOnlyTable<u64, u64>>();

pub(super) fn history_control_damage(reason: &str) -> Error {
    Error::StoreCorrupted {
        path: "committed history controls".into(),
        reason: reason.into(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super) struct PhysicalHistoryTable {
    pub(in super::super) present: bool,
    pub(in super::super) root: [u8; 32],
}
impl PhysicalHistoryTable {
    fn empty() -> Self {
        Self {
            present: false,
            root: [0; 32],
        }
    }
    pub(super) fn encode_into(self, out: &mut Vec<u8>) {
        out.push(u8::from(self.present));
        out.extend_from_slice(&self.root);
    }
    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 33 || bytes[0] > 1 || (bytes[0] == 0 && bytes[1..] != [0; 32]) {
            return Err(history_control_damage(
                "invalid physical history table descriptor",
            ));
        }
        Ok(Self {
            present: bytes[0] == 1,
            root: bytes[1..].try_into().expect("fixed descriptor"),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super) struct HistoryOpenControl {
    pub(in super::super) tree: Option<[u8; 32]>,
    pub(in super::super) tree_hash: [u8; 32],
    pub(in super::super) commit_count: u64,
    pub(in super::super) first: Option<(Lsn, [u8; 32])>,
    pub(in super::super) last: Option<(Lsn, [u8; 32])>,
    // Exact coordinate length, parsed LSN, coordinate digest and value digest.
    pub(in super::super) ddl: Option<(u64, Lsn, [u8; 32], [u8; 32])>,
}
impl HistoryOpenControl {
    fn empty() -> Self {
        Self {
            tree: None,
            tree_hash: empty_hash(LogicalFamily::ChangeAndDdl),
            commit_count: 0,
            first: None,
            last: None,
            ddl: None,
        }
    }
    pub(super) fn validate(&self) -> Result<()> {
        let bad = || history_control_damage("noncanonical fixed history control");
        if self.tree == Some([0; 32]) {
            return Err(bad());
        }
        if self.tree.is_none()
            && (self.tree_hash != empty_hash(LogicalFamily::ChangeAndDdl)
                || self.commit_count != 0
                || self.ddl.is_some())
        {
            return Err(bad());
        }
        match (self.commit_count, self.first, self.last) {
            (0, None, None) => {}
            (1, Some(first), Some(last)) if first == last => {}
            (count, Some(first), Some(last)) if count > 1 && first.0 < last.0 => {}
            _ => return Err(bad()),
        }
        if self.ddl.is_some_and(|(len, _, _, _)| len == 0) {
            return Err(bad());
        }
        usize::try_from(self.commit_count).map_err(|_| bad())?;
        Ok(())
    }
    pub(super) fn encode(self) -> [u8; 234] {
        let mut out = [0; 234];
        out[0] = LogicalFamily::ChangeAndDdl as u8;
        out[1] = u8::from(self.tree.is_some())
            | (u8::from(self.first.is_some()) << 1)
            | (u8::from(self.ddl.is_some()) << 2);
        if let Some(id) = self.tree {
            out[2..34].copy_from_slice(&id);
        }
        out[34..66].copy_from_slice(&self.tree_hash);
        out[66..74].copy_from_slice(&self.commit_count.to_be_bytes());
        if let Some((lsn, hash)) = self.first {
            out[74..82].copy_from_slice(&lsn.0.to_be_bytes());
            out[82..114].copy_from_slice(&hash);
        }
        if let Some((lsn, hash)) = self.last {
            out[114..122].copy_from_slice(&lsn.0.to_be_bytes());
            out[122..154].copy_from_slice(&hash);
        }
        if let Some((len, lsn, key, value)) = self.ddl {
            out[154..162].copy_from_slice(&len.to_be_bytes());
            out[162..170].copy_from_slice(&lsn.0.to_be_bytes());
            out[170..202].copy_from_slice(&key);
            out[202..234].copy_from_slice(&value);
        }
        out
    }
    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        let bad = || history_control_damage("invalid fixed history control encoding");
        if bytes.len() != 234 || bytes[0] != LogicalFamily::ChangeAndDdl as u8 || bytes[1] & !7 != 0
        {
            return Err(bad());
        }
        let number =
            |a, b| u64::from_be_bytes(bytes[a..b].try_into().expect("fixed control integer"));
        let hash = |a, b| <[u8; 32]>::try_from(&bytes[a..b]).expect("fixed control digest");
        let value = Self {
            tree: (bytes[1] & 1 != 0).then(|| hash(2, 34)),
            tree_hash: hash(34, 66),
            commit_count: number(66, 74),
            first: (bytes[1] & 2 != 0).then(|| (Lsn(number(74, 82)), hash(82, 114))),
            last: (bytes[1] & 2 != 0).then(|| (Lsn(number(114, 122)), hash(122, 154))),
            ddl: (bytes[1] & 4 != 0).then(|| {
                (
                    number(154, 162),
                    Lsn(number(162, 170)),
                    hash(170, 202),
                    hash(202, 234),
                )
            }),
        };
        value.validate()?;
        if value.encode() != bytes {
            return Err(bad());
        }
        Ok(value)
    }
    pub(super) fn digest(self) -> [u8; 32] {
        const DOMAIN: &[u8] = b"contextdb-history-open-control\0";
        let bytes = self.encode();
        let mut digest = blake3::Hasher::new();
        digest.update(DOMAIN);
        digest.update(&bytes);
        #[cfg(feature = "test-seams")]
        crate::read_work_probe::note_hashed_bytes(DOMAIN.len() + bytes.len());
        *digest.finalize().as_bytes()
    }
}
pub(in super::super) fn ddl_coordinate_digest(
    coordinate: &[u8],
    checkpoint: Option<&dyn Fn() -> Result<()>>,
) -> Result<[u8; 32]> {
    const DOMAIN: &[u8] = b"contextdb-history-open-ddl-coordinate\0";
    let len = u64::try_from(coordinate.len())
        .map_err(|_| history_control_damage("DDL coordinate length overflow"))?;
    let mut hash = blake3::Hasher::new();
    hash.update(DOMAIN);
    hash.update(&len.to_be_bytes());
    #[cfg(feature = "test-seams")]
    crate::read_work_probe::note_hashed_bytes(DOMAIN.len() + 8);
    for chunk in coordinate.chunks(4096) {
        if let Some(checkpoint) = checkpoint {
            checkpoint()?;
        }
        hash.update(chunk);
        #[cfg(feature = "test-seams")]
        crate::read_work_probe::note_hashed_bytes(chunk.len());
    }
    Ok(*hash.finalize().as_bytes())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super) struct HistoryOpenPublication {
    pub(in super::super) control: HistoryOpenControl,
    pub(in super::super) control_hash: [u8; 32],
    pub(in super::super) commit: PhysicalHistoryTable,
    pub(in super::super) ddl: PhysicalHistoryTable,
}
impl HistoryOpenPublication {
    pub(super) fn empty() -> Self {
        let control = HistoryOpenControl::empty();
        Self {
            control,
            control_hash: control.digest(),
            commit: PhysicalHistoryTable::empty(),
            ddl: PhysicalHistoryTable::empty(),
        }
    }
}

/// Minted only by authenticated fixed-root acquisition. No body proof authority.
#[derive(Debug, Clone, Copy)]
pub(in super::super) struct ValidatedHistoryOpenControl {
    publication: HistoryOpenPublication,
    snapshot: redb::StorageSnapshotIdentity,
}
impl ValidatedHistoryOpenControl {
    pub(in super::super) fn control(&self) -> &HistoryOpenControl {
        &self.publication.control
    }
    pub(in super::super) fn check_snapshot(
        &self,
        snapshot: redb::StorageSnapshotIdentity,
    ) -> Result<()> {
        if self.snapshot != snapshot {
            return Err(history_control_damage(
                "history controls escaped captured snapshot",
            ));
        }
        Ok(())
    }
    pub(in super::super) fn check_endpoint(
        &self,
        lsn: Lsn,
        tx: Option<TxId>,
        _mutation: &ContentIdentityMutation,
    ) -> Result<()> {
        let c = &self.publication.control;
        let expected = [c.first, c.last]
            .into_iter()
            .flatten()
            .find(|pair| pair.0 == lsn)
            .ok_or_else(|| {
                history_control_damage("open endpoint capability used for another coordinate")
            })?;
        let tx = tx.ok_or_else(|| history_control_damage("captured commit endpoint is absent"))?;
        #[cfg(feature = "test-seams")]
        crate::read_work_probe::note_hashed_bytes(
            _mutation.logical_key.len() + _mutation.logical_value.as_ref().map_or(0, Vec::len),
        );
        if commit_index_value_hash(tx) != expected.1 {
            return Err(history_control_damage(
                "captured commit endpoint value differs from control",
            ));
        }
        Ok(())
    }
    pub(in super::super) fn check_directory(&self, read: &redb::ReadTransaction) -> Result<()> {
        self.check_snapshot(read.storage_snapshot_identity())?;
        let actual = physical_in_read(read, COMMIT_INDEX_TABLE)?;
        if actual != self.publication.commit {
            return Err(history_control_damage(
                "commit directory differs from its committed root",
            ));
        }
        let table = read
            .open_table(COMMIT_INDEX_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        if table.len().map_err(RedbPersistence::storage_error)?
            != self.publication.control.commit_count
        {
            return Err(history_control_damage(
                "commit directory cardinality differs from recovery bounds",
            ));
        }
        Ok(())
    }
    pub(in super::super) fn check_complete_commits(
        &self,
        count: u64,
        first: Option<(Lsn, TxId)>,
        last: Option<(Lsn, TxId)>,
    ) -> Result<()> {
        let pair = |pair: Option<(Lsn, TxId)>| pair.map(|(lsn, tx)| {
            let hash = commit_index_value_hash(tx);
            // This new reconciliation hashes the actual value-hash inputs;
            // the existing membership observation convention is unchanged.
            #[cfg(feature = "test-seams")]
            crate::read_work_probe::note_hashed_bytes(
                b"contextdb-logical-record-value-v1".len() + 2 * 8 + 1 + 8,
            );
            (lsn, hash)
        });
        if count != self.publication.control.commit_count
            || pair(first) != self.publication.control.first
            || pair(last) != self.publication.control.last
        {
            return Err(history_control_damage(
                "complete commit history differs from its fixed controls",
            ));
        }
        Ok(())
    }
    pub(in super::super) fn check_complete_ddl(
        &self,
        last: Option<(u64, Lsn, [u8; 32], [u8; 32])>,
    ) -> Result<()> {
        if last != self.publication.control.ddl {
            return Err(history_control_damage(
                "complete DDL history differs from its fixed controls",
            ));
        }
        Ok(())
    }
    pub(in super::super) fn check_ddl(&self, read: &redb::ReadTransaction) -> Result<()> {
        self.check_snapshot(read.storage_snapshot_identity())?;
        if physical_in_read(read, DDL_LOG_TABLE)? != self.publication.ddl {
            return Err(history_control_damage(
                "DDL directory differs from its committed root",
            ));
        }
        Ok(())
    }
}

fn table_present_in_read(
    read: &redb::ReadTransaction,
    definition: impl redb::TableHandle,
) -> Result<bool> {
    match read.open_untyped_table(definition) {
        Ok(_) => Ok(true),
        Err(redb::TableError::TableDoesNotExist(_)) => Ok(false),
        Err(error) => Err(RedbPersistence::storage_error(error)),
    }
}

fn physical_in_read<K: redb::Key + 'static, V: redb::Value + 'static>(
    read: &redb::ReadTransaction,
    definition: redb::TableDefinition<K, V>,
) -> Result<PhysicalHistoryTable> {
    let present = table_present_in_read(read, definition)?;
    let acquired = read.table_root_fingerprint(definition);
    #[cfg(any(test, feature = "test-seams"))]
    crate::read_work_probe::note_graph_root(redb::TableHandle::name(&definition), &[], &acquired);
    let root = acquired
        .map_err(RedbPersistence::storage_error)?
        .unwrap_or([0; 32]);
    Ok(PhysicalHistoryTable { present, root })
}

pub(in super::super) fn acquire(
    read: &redb::ReadTransaction,
    count: u64,
) -> Result<ValidatedHistoryOpenControl> {
    // Standalone acquisitions retain the full separate metadata proof.
    let authenticated = RedbPersistence::authenticate_read_bounds(read)?;
    acquire_authenticated(&authenticated, count)
}

pub(in super::super) fn acquire_authenticated(
    authenticated: &super::super::writer_recovery::AuthenticatedReadBounds<'_>,
    count: u64,
) -> Result<ValidatedHistoryOpenControl> {
    // The evidence supplies its own exact read capture, never decoded bounds.
    let read = authenticated.read();
    let table = read
        .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let encoded = table
        .get(LogicalFamily::ChangeAndDdl as u8)
        .map_err(RedbPersistence::storage_error)?
        .ok_or_else(|| history_control_damage("missing fixed history root"))?;
    let publication = decode_published_root(LogicalFamily::ChangeAndDdl, encoded.value())?
        .history_open
        .ok_or_else(|| history_control_damage("missing fixed history controls"))?;
    if publication.control.digest() != publication.control_hash
        || publication.control.commit_count != count
    {
        return Err(history_control_damage(
            "fixed history control commitment disagrees",
        ));
    }
    for (expected, actual, nonempty) in [
        (
            publication.commit,
            table_present_in_read(read, COMMIT_INDEX_TABLE)?,
            publication.control.commit_count != 0,
        ),
        (
            publication.ddl,
            table_present_in_read(read, DDL_LOG_TABLE)?,
            publication.control.ddl.is_some(),
        ),
    ] {
        if !actual || expected.present != actual || (nonempty && !expected.present) {
            return Err(history_control_damage(
                "required history table presence disagrees",
            ));
        }
    }
    Ok(ValidatedHistoryOpenControl {
        publication,
        snapshot: read.storage_snapshot_identity(),
    })
}

pub(in super::super) fn finalize(write: &redb::WriteTransaction) -> Result<()> {
    // Inspect existence before any write open_table could manufacture it.
    let mut present = [false; 4];
    for handle in write
        .list_tables()
        .map_err(RedbPersistence::storage_error)?
    {
        for (index, name) in [
            redb::TableHandle::name(&CONTENT_IDENTITY_ROOTS_TABLE),
            redb::TableHandle::name(&CONTENT_IDENTITY_NODES_TABLE),
            redb::TableHandle::name(&COMMIT_INDEX_TABLE),
            redb::TableHandle::name(&DDL_LOG_TABLE),
        ]
        .into_iter()
        .enumerate()
        {
            present[index] |= handle.name() == name;
        }
    }
    if !present.into_iter().all(|p| p) {
        return Err(history_control_damage("missing required publication table"));
    }
    let commit = PhysicalHistoryTable {
        present: present[2],
        root: write
            .table_root_fingerprint(COMMIT_INDEX_TABLE)
            .map_err(RedbPersistence::storage_error)?
            .unwrap_or([0; 32]),
    };
    let ddl = PhysicalHistoryTable {
        present: present[3],
        root: write
            .table_root_fingerprint(DDL_LOG_TABLE)
            .map_err(RedbPersistence::storage_error)?
            .unwrap_or([0; 32]),
    };
    let mut roots = write
        .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let mut published = {
        let encoded = roots
            .get(LogicalFamily::ChangeAndDdl as u8)
            .map_err(RedbPersistence::storage_error)?
            .ok_or_else(|| history_control_damage("missing history root during publication"))?;
        decode_published_root(LogicalFamily::ChangeAndDdl, encoded.value())?
    };
    let publication = published
        .history_open
        .as_mut()
        .ok_or_else(|| history_control_damage("missing history publication controls"))?;
    let commits = write
        .open_table(COMMIT_INDEX_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    if commits.len().map_err(RedbPersistence::storage_error)? != publication.control.commit_count {
        return Err(history_control_damage(
            "logical history count differs from finalized commit table",
        ));
    }
    for (logical, physical) in [
        (
            publication.control.first,
            commits.first().map_err(RedbPersistence::storage_error)?,
        ),
        (
            publication.control.last,
            commits.last().map_err(RedbPersistence::storage_error)?,
        ),
    ] {
        match (logical, physical) {
            (None, None) => {}
            (Some((lsn, hash)), Some((key, value)))
                if lsn.0 == key.value() && commit_index_value_hash(TxId(value.value())) == hash => {
            }
            _ => {
                return Err(history_control_damage(
                    "logical history endpoint differs from finalized commit table",
                ));
            }
        }
    }
    drop(commits);
    publication.commit = commit;
    publication.ddl = ddl;
    let encoded = encode_published_root(&published);
    if roots
        .get(LogicalFamily::ChangeAndDdl as u8)
        .map_err(RedbPersistence::storage_error)?
        .is_none_or(|old| old.value() != encoded.as_slice())
    {
        roots
            .insert(LogicalFamily::ChangeAndDdl as u8, encoded.as_slice())
            .map_err(RedbPersistence::storage_error)?;
    }
    Ok(())
}
