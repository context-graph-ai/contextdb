//! Storage-neutral commit history. File writers supply a durable cursor;
//! only the memory backend retains a tree.
use crate::staged_source::{FundedArc, MutableSourceMap, PreparedMapEntry, SourceFunder};
use contextdb_core::read_memory::{ReadCredit, ReadMemoryBudget};
use contextdb_core::{Lsn, Result, TxId};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

/// Borrowed operation context. The supplied budget is the existing reader child;
/// this seam never creates or substitutes a memory owner.
#[doc(hidden)]
pub struct ReadCommitContext<'a> {
    pub read_memory: Arc<dyn ReadMemoryBudget>,
    pub checkpoint: &'a dyn Fn() -> Result<()>,
}

impl<'a> ReadCommitContext<'a> {
    pub fn new(
        read_memory: Arc<dyn ReadMemoryBudget>,
        checkpoint: &'a dyn Fn() -> Result<()>,
    ) -> Self {
        Self {
            read_memory,
            checkpoint,
        }
    }

    pub fn checkpoint(&self) -> Result<()> {
        (self.checkpoint)()
    }
}

/// One captured pair at a time. The engine admits and owns any output copy.
#[doc(hidden)]
pub trait ReadCommitCursor: Send {
    fn next_entry(&mut self, context: &ReadCommitContext<'_>) -> Result<Option<(Lsn, TxId)>>;
}

/// Immutable commit history at one captured source root. Storage implementations
/// authenticate membership, absence, range completion, and durable census.
#[doc(hidden)]
pub trait ReadCommitDirectory: Send + Sync {
    fn get(&self, lsn: Lsn, context: &ReadCommitContext<'_>) -> Result<Option<TxId>>;
    fn contains(&self, lsn: Lsn, context: &ReadCommitContext<'_>) -> Result<bool> {
        self.get(lsn, context).map(|entry| entry.is_some())
    }
    fn floor(
        &self,
        upper: Bound<Lsn>,
        context: &ReadCommitContext<'_>,
    ) -> Result<Option<(Lsn, TxId)>>;
    fn census(&self, context: &ReadCommitContext<'_>) -> Result<(usize, Lsn)>;
    fn cursor(
        &self,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
        context: &ReadCommitContext<'_>,
    ) -> Result<Box<dyn ReadCommitCursor>>;
    #[cfg(feature = "test-seams")]
    fn retained_entry_bytes(&self) -> usize {
        0
    }
}

pub(crate) struct WritableReadCursor {
    cursor: Box<dyn CommitDirectoryCursor>,
    _control: ReadCredit,
}

impl WritableReadCursor {
    pub(crate) fn open<D: CommitDirectory + ?Sized>(
        directory: &D,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
        context: &ReadCommitContext<'_>,
    ) -> Result<Box<dyn ReadCommitCursor>> {
        context.checkpoint()?;
        let control = ReadCredit::try_new(
            context.read_memory.clone(),
            std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>(),
            "commit_directory",
            "read_cursor",
            "Raise the reader memory limit to admit the cursor.",
        )?;
        let cursor = directory.cursor(lower, upper)?;
        Ok(Box::new(Self {
            cursor,
            _control: control,
        }))
    }
}

impl ReadCommitCursor for WritableReadCursor {
    fn next_entry(&mut self, context: &ReadCommitContext<'_>) -> Result<Option<(Lsn, TxId)>> {
        context.checkpoint()?;
        self.cursor.next_entry()
    }
}

/// Adapter only for legacy fallible callers, using the image's existing child.
/// Bounded callers pass their active context directly to ReadCommitCursor.
pub(crate) struct CapturedLegacyCursor {
    cursor: Box<dyn ReadCommitCursor>,
    read_memory: Arc<dyn ReadMemoryBudget>,
    _control: ReadCredit,
}

impl CapturedLegacyCursor {
    pub(crate) fn open(
        directory: &dyn ReadCommitDirectory,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
        read_memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<Box<dyn CommitDirectoryCursor>> {
        let context = ReadCommitContext::new(read_memory.clone(), &|| Ok(()));
        let control = ReadCredit::try_new(
            read_memory.clone(),
            std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>(),
            "commit_directory",
            "read_cursor",
            "Raise the reader memory limit to admit the cursor.",
        )?;
        let cursor = directory.cursor(lower, upper, &context)?;
        Ok(Box::new(Self {
            cursor,
            read_memory,
            _control: control,
        }))
    }
}

impl CommitDirectoryCursor for CapturedLegacyCursor {
    fn next_entry(&mut self) -> Result<Option<(Lsn, TxId)>> {
        self.cursor.next_entry(&ReadCommitContext::new(
            self.read_memory.clone(),
            &|| Ok(()),
        ))
    }
}

pub trait CommitDirectoryCursor: Send {
    fn next_entry(&mut self) -> Result<Option<(Lsn, TxId)>>;
}

/// Implementations pin one durable root per cursor and decode one entry at
/// a time. Counts and endpoint probes must not enumerate history. Mutations
/// are bounded by their supplied candidates, not by directory length.
pub trait CommitDirectory: Send + Sync {
    fn cursor(
        &self,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
    ) -> Result<Box<dyn CommitDirectoryCursor>>;
    #[doc(hidden)]
    fn cursor_with_read_context(
        &self,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
        context: &ReadCommitContext<'_>,
    ) -> Result<Box<dyn ReadCommitCursor>> {
        WritableReadCursor::open(self, lower, upper, context)
    }
    fn floor(&self, upper: Bound<Lsn>) -> Result<Option<(Lsn, TxId)>>;
    fn census(&self) -> Result<(usize, Lsn)>;
    fn contains(&self, lsn: Lsn) -> Result<bool>;
    fn remove(&self, lsns: &[Lsn]) -> Result<u64>;
    fn replace(&self, retained: BTreeMap<Lsn, TxId>) -> Result<u64>;
    /// File-backed apply has already written this entry in its atomic durable
    /// transaction. Its adapter does nothing here; the memory backend inserts.
    fn record_applied(&self, lsn: Lsn, tx: TxId);
    #[cfg(feature = "test-seams")]
    fn retained_entry_bytes(&self) -> usize {
        0
    }
}

/// Engine-owned in-memory history.  Its map cells are funded before they are
/// linked, and each cursor retains the same funded control instead of copying
/// the directory into an unpriced collection.
pub(crate) struct EngineMemoryCommitDirectory {
    state: FundedArc<Mutex<MutableSourceMap<Lsn, TxId>>>,
    funder: SourceFunder,
}

pub(crate) struct PreparedMemoryCommitRecord {
    directory_identity: usize,
    entry: PreparedMapEntry<Lsn, TxId>,
}

struct EngineMemoryCursor {
    state: FundedArc<Mutex<MutableSourceMap<Lsn, TxId>>>,
    lower: Bound<Lsn>,
    upper: Bound<Lsn>,
    last: Option<Lsn>,
}

impl CommitDirectoryCursor for EngineMemoryCursor {
    fn next_entry(&mut self) -> Result<Option<(Lsn, TxId)>> {
        let next = self
            .state
            .lock()
            .next_in_range(self.lower, self.upper, self.last);
        if let Some((lsn, _)) = next {
            self.last = Some(lsn);
        }
        Ok(next)
    }
}

struct EngineMemoryReadCursor {
    cursor: EngineMemoryCursor,
    _credit: ReadCredit,
}

impl ReadCommitCursor for EngineMemoryReadCursor {
    fn next_entry(&mut self, context: &ReadCommitContext<'_>) -> Result<Option<(Lsn, TxId)>> {
        context.checkpoint()?;
        self.cursor.next_entry()
    }
}

impl EngineMemoryCommitDirectory {
    pub(crate) fn try_new(funder: SourceFunder) -> Result<Self> {
        let charge = FundedArc::<Mutex<MutableSourceMap<Lsn, TxId>>>::prepare(funder.clone())?;
        Ok(Self {
            state: FundedArc::from_prepared(Mutex::new(MutableSourceMap::default()), charge),
            funder,
        })
    }

    pub(crate) fn identity(&self) -> usize {
        &*self.state as *const Mutex<MutableSourceMap<Lsn, TxId>> as usize
    }

    pub(crate) fn prepare_record(&self, lsn: Lsn, tx: TxId) -> Result<PreparedMemoryCommitRecord> {
        Ok(PreparedMemoryCommitRecord {
            directory_identity: self.identity(),
            entry: MutableSourceMap::prepare(lsn, tx, &self.funder)?,
        })
    }

    pub(crate) fn record_prepared(&self, record: PreparedMemoryCommitRecord) {
        assert_eq!(record.directory_identity, self.identity());
        self.state.lock().insert_prepared(record.entry);
    }

    pub(crate) fn cursor(
        &self,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
    ) -> Box<dyn CommitDirectoryCursor> {
        Box::new(EngineMemoryCursor {
            state: self.state.clone(),
            lower,
            upper,
            last: None,
        })
    }

    pub(crate) fn cursor_with_read_context(
        &self,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
        context: &ReadCommitContext<'_>,
    ) -> Result<Box<dyn ReadCommitCursor>> {
        context.checkpoint()?;
        let credit = ReadCredit::try_new(
            context.read_memory.clone(),
            std::mem::size_of::<EngineMemoryReadCursor>() + 2 * std::mem::size_of::<usize>(),
            "commit_directory",
            "read_cursor",
            "Raise the reader memory limit to admit the cursor.",
        )?;
        Ok(Box::new(EngineMemoryReadCursor {
            cursor: EngineMemoryCursor {
                state: self.state.clone(),
                lower,
                upper,
                last: None,
            },
            _credit: credit,
        }))
    }

    pub(crate) fn census(&self) -> (usize, Lsn) {
        let state = self.state.lock();
        (state.len(), state.first_key().copied().unwrap_or(Lsn(0)))
    }

    pub(crate) fn contains(&self, lsn: Lsn) -> bool {
        self.state.lock().get(&lsn).is_some()
    }

    pub(crate) fn floor(&self, upper: Bound<Lsn>) -> Option<(Lsn, TxId)> {
        self.state.lock().floor(upper)
    }

    pub(crate) fn remove(&self, lsns: &[Lsn]) -> u64 {
        let mut state = self.state.lock();
        lsns.iter()
            .filter(|lsn| state.remove(lsn).is_some())
            .count() as u64
    }

    pub(crate) fn replace(&self, retained: BTreeMap<Lsn, TxId>) -> Result<u64> {
        // Admit every replacement node before mutating the live directory.
        let mut replacement = MutableSourceMap::default();
        for (lsn, tx) in retained {
            replacement.insert_prepared(MutableSourceMap::prepare(lsn, tx, &self.funder)?);
        }
        let mut state = self.state.lock();
        let removed = state.len().saturating_sub(replacement.len()) as u64;
        *state = replacement;
        Ok(removed)
    }
}

pub(crate) struct MemoryCommitDirectory(pub Mutex<BTreeMap<Lsn, TxId>>);
struct MemoryCursor(std::collections::btree_map::IntoIter<Lsn, TxId>);
struct MemoryReadCursor {
    entries: std::vec::IntoIter<(Lsn, TxId)>,
    _credit: ReadCredit,
}
impl ReadCommitCursor for MemoryReadCursor {
    fn next_entry(&mut self, context: &ReadCommitContext<'_>) -> Result<Option<(Lsn, TxId)>> {
        context.checkpoint()?;
        Ok(self.entries.next())
    }
}

fn empty_bounds(lower: Bound<Lsn>, upper: Bound<Lsn>) -> bool {
    let first = match lower {
        Bound::Unbounded => Some(0),
        Bound::Included(lsn) => Some(lsn.0),
        Bound::Excluded(lsn) => lsn.0.checked_add(1),
    };
    let last = match upper {
        Bound::Unbounded => Some(u64::MAX),
        Bound::Included(lsn) => Some(lsn.0),
        Bound::Excluded(lsn) => lsn.0.checked_sub(1),
    };
    match (first, last) {
        (Some(first), Some(last)) => first > last,
        _ => true,
    }
}
impl CommitDirectoryCursor for MemoryCursor {
    fn next_entry(&mut self) -> Result<Option<(Lsn, TxId)>> {
        Ok(self.0.next())
    }
}
impl CommitDirectory for MemoryCommitDirectory {
    fn cursor(
        &self,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
    ) -> Result<Box<dyn CommitDirectoryCursor>> {
        Ok(Box::new(MemoryCursor(
            self.0
                .lock()
                .range((lower, upper))
                .map(|(lsn, tx)| (*lsn, *tx))
                .collect::<BTreeMap<_, _>>()
                .into_iter(),
        )))
    }
    fn cursor_with_read_context(
        &self,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
        context: &ReadCommitContext<'_>,
    ) -> Result<Box<dyn ReadCommitCursor>> {
        context.checkpoint()?;
        let entries = self.0.lock();
        let empty = empty_bounds(lower, upper);
        let count = if empty {
            0
        } else {
            entries.range((lower, upper)).count()
        };
        let bytes = count
            .checked_mul(std::mem::size_of::<(Lsn, TxId)>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<MemoryReadCursor>()))
            .and_then(|bytes| bytes.checked_add(4 * std::mem::size_of::<usize>()))
            .ok_or_else(|| contextdb_core::Error::Other("reader cursor charge overflow".into()))?;
        let credit = ReadCredit::try_new(
            context.read_memory.clone(),
            bytes,
            "commit_directory",
            "read_cursor",
            "Raise the reader memory limit to admit the cursor.",
        )?;
        context.checkpoint()?;
        let mut copied = Vec::with_capacity(count);
        if !empty {
            for (lsn, tx) in entries.range((lower, upper)) {
                context.checkpoint()?;
                copied.push((*lsn, *tx));
            }
        }
        Ok(Box::new(MemoryReadCursor {
            entries: copied.into_iter(),
            _credit: credit,
        }))
    }
    fn floor(&self, upper: Bound<Lsn>) -> Result<Option<(Lsn, TxId)>> {
        Ok(self
            .0
            .lock()
            .range((Bound::Unbounded, upper))
            .next_back()
            .map(|(lsn, tx)| (*lsn, *tx)))
    }
    fn census(&self) -> Result<(usize, Lsn)> {
        let entries = self.0.lock();
        Ok((
            entries.len(),
            entries.keys().next().copied().unwrap_or(Lsn(0)),
        ))
    }
    fn contains(&self, lsn: Lsn) -> Result<bool> {
        Ok(self.0.lock().contains_key(&lsn))
    }
    fn remove(&self, lsns: &[Lsn]) -> Result<u64> {
        let mut entries = self.0.lock();
        Ok(lsns
            .iter()
            .filter(|lsn| entries.remove(lsn).is_some())
            .count() as u64)
    }
    fn replace(&self, retained: BTreeMap<Lsn, TxId>) -> Result<u64> {
        let mut entries = self.0.lock();
        let removed = entries.len().saturating_sub(retained.len()) as u64;
        *entries = retained;
        Ok(removed)
    }
    #[cfg(feature = "test-seams")]
    fn retained_entry_bytes(&self) -> usize {
        self.0
            .lock()
            .len()
            .saturating_mul(std::mem::size_of::<Lsn>() + std::mem::size_of::<TxId>())
    }
    fn record_applied(&self, lsn: Lsn, tx: TxId) {
        self.0.lock().insert(lsn, tx);
    }
}
