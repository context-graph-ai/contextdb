//! Native storage ports. The engine owns pages, catalogs, transactions and disk admission.
use contextdb_core::{Result, VectorIndexRef};

/// Native page payload. Authentication headers/proofs belong to the supplying page factory.
pub const PAGE_BYTES: usize = 3904;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PageKind {
    Canonical,
    Lists,
    Directory,
    DirectoryPresence,
    Visibility,
    Codebook,
    Accumulators,
    Reservoir,
    Catalog,
    SlotMetadata,
    Tail,
    ListLinks,
    ListAllocator,
    ListUsed,
    RowVersions,
    RowCopies,
}

/// A page's role inside the captured generation. The native reader must authenticate
/// this complete identity against its pinned root before copying a payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageKey {
    pub partition: u64,
    pub generation: u64,
    pub kind: PageKind,
    pub list: u32,
    pub ordinal: u64,
    /// Directory root; payload/list identities remain generation-global.
    pub run: u64,
}

/// A verified inspection result scoped to one admitted manifest window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Healthy,
    Corrupt(PageKey),
}
impl Verdict {
    pub(crate) fn damage(self) -> Option<PageKey> {
        match self {
            Self::Healthy => None,
            Self::Corrupt(key) => Some(key),
        }
    }
}

/// Fixed metadata; directories and vectors remain pages, never vectors in this object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub partition: u64,
    pub generation: u64,
    pub dimensions: usize,
    pub lists: u32,
    pub active_lists: u32,
    pub slots: u64,
    pub sealed_slots: u64,
    pub codebook: [u8; 32],
    pub run: u64,
}
impl Manifest {
    pub fn key(self, kind: PageKind, list: u32, ordinal: u64) -> PageKey {
        PageKey {
            partition: self.partition,
            generation: if matches!(kind, PageKind::RowVersions | PageKind::RowCopies) {
                0
            } else {
                self.generation
            },
            kind,
            list,
            ordinal,
            run: if matches!(
                kind,
                PageKind::Directory | PageKind::DirectoryPresence | PageKind::RowCopies
            ) {
                self.run
            } else {
                0
            },
        }
    }
}

/// The caller supplies a snapshot-pinned native reader. A missing payload returns
/// false; a bad proof, wrong root/identity, cancellation or refusal returns its
/// existing typed engine error. This port never grants an uncharged page alias.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildPhase {
    Ingest,
    Seed,
    Train,
    Assign,
    Publish,
    Ready,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BuildState {
    pub manifest: Manifest,
    pub phase: BuildPhase,
    pub cursor: u64,
    pub iteration: u32,
    pub training: super::TrainingPolicy,
    pub serving_at_start: Option<Manifest>,
    pub source_lsn: u64,
    /// Only this coalesce may delay publication; later seals never extend the fence.
    pub publication_coalesce: Option<(u64, u64)>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DistanceKind {
    Centroid,
    Quantized,
    Canonical,
    Buffer,
}

pub trait ReadPages {
    /// Optional immutable source retained by maintenance through a replacement
    /// switch. The reader still admits and owns its ordinary constructor object;
    /// this loan replaces only where that constructor obtains the same values.
    fn retained_codebook(&self, _manifest: Manifest) -> Option<&super::RetainedCodebook> {
        None
    }
    /// Snapshot identity is implicit in the supplying pinned view. The verdict
    /// lives inline in its already admitted manifest/control holder; eviction is
    /// permitted and re-admission then requires verification again.
    fn inspection_verdict(&self, partition: u64, generation: u64) -> Option<Verdict>;
    fn record_inspection_verdict(&self, partition: u64, generation: u64, verdict: Verdict);
    /// Captured native source frontier, never a slot-derived substitute.
    fn snapshot_lsn(&self) -> Result<u64>;
    /// The captured native index identity; copied into a typed caller diagnostic only on failure.
    fn index_identity(&self) -> &VectorIndexRef;
    fn check_cancel(&self) -> Result<()>;
    /// Existing request work/deadline admission, before the actual comparison.
    fn before_distance(&self, kind: DistanceKind) -> Result<()>;
    fn manifest(&self, partition: u64) -> Result<Option<Manifest>>;
    fn build_state(&self, partition: u64) -> Result<Option<BuildState>>;
    fn next_partition(&self, after: Option<u64>) -> Result<Option<u64>>;
    fn read_admitted_page(&self, key: PageKey, output: &mut [u8; PAGE_BYTES]) -> Result<bool>;
}

/// Each write obtains the native allocator's disk admission BEFORE execution,
/// including copy-on-write overlap. Commit installs payloads and manifest roots
/// atomically. Abort/drop leaves the previous root complete. A backend must not
/// retain a population-sized decoded write set: use the native paged transaction.
pub trait WritePages: ReadPages {
    /// Native LSN assigned to the row/delta/tombstone commit.
    fn commit_lsn(&self) -> Result<u64>;
    fn put_page(&mut self, key: PageKey, payload: &[u8; PAGE_BYTES]) -> Result<()>;
    /// Register/update unpublished roots in the existing build/erasure inventory.
    fn stage_build(&mut self, state: BuildState) -> Result<()>;
    fn publish_manifest(&mut self, manifest: Manifest) -> Result<()>;
    fn commit(self) -> Result<()>
    where
        Self: Sized;
}

/// The native adapter uses one writer handle per partition for inserts, updates,
/// deletes, build, remap and coalescing. Separate handles may not allocate slots
/// concurrently in one partition. After an outer native transaction aborts, its
/// staged writer window is discarded/reopened before that handle is reused.
/// Maintenance may call remap and coalescing in either order, with writes between
/// calls; no adjacency is required. It invokes ClusteredIndex::inspect_catalog to
/// persist the damage verdict before driving partition work, so read-only SHOW
/// observes that verdict after the maintenance commit.
pub trait PageStore: ReadPages {
    type Transaction<'a>: WritePages
    where
        Self: 'a;
    fn begin(&mut self) -> Result<Self::Transaction<'_>>;
}

/// Cancellation remains typed even for staged native payloads already in a write transaction.
pub(crate) fn read_page(
    source: &impl ReadPages,
    key: PageKey,
    out: &mut [u8; PAGE_BYTES],
) -> Result<bool> {
    source.check_cancel()?;
    let present = source.read_admitted_page(key, out)?;
    source.check_cancel()?;
    Ok(present)
}
pub(crate) fn put_page(
    target: &mut impl WritePages,
    key: PageKey,
    payload: &[u8; PAGE_BYTES],
) -> Result<()> {
    target.check_cancel()?;
    target.put_page(key, payload)?;
    target.check_cancel()
}
