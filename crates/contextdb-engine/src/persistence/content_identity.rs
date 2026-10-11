#![allow(clippy::too_many_arguments)] // Existing compatibility and test seam shape.
#![allow(dead_code)] // Existing compatibility and test seam shape.
//! Incrementally maintained identity of committed logical content.
//!
//! Each typed family is split at deterministic key-hash boundaries into
//! bounded logical pages. Separators and child hashes form high-fanout internal
//! chunks, so grouping and identity depend only on the current logical key/value
//! set rather than insertion order, compaction, or Redb page layout. Point
//! mutations rewrite one page and its bounded proof path.

use super::{
    CONTENT_IDENTITY_METADATA_TABLE, CONTENT_IDENTITY_NODES_TABLE, CONTENT_IDENTITY_ROOTS_TABLE,
    RedbPersistence,
};
use crate::direct_file_reader::DirectTypedImageDigest;
#[cfg(all(test, feature = "test-seams"))]
use crate::memory_accounting::MemoryAccountant;
use contextdb_core::{
    AdjEntry, Error, Lsn, Result, RowId, TableMeta, TxId, Value, VectorEntry, VectorIndexRef,
    VectorQuantization, VersionedRow,
};
use redb::{ReadOnlyTable, ReadableDatabase, ReadableTable};
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

#[cfg(feature = "test-seams")]
thread_local! {
    static IDENTITY_WORK: std::cell::Cell<(u64, u64, u64)> = const {
        std::cell::Cell::new((0, 0, 0))
    };
    static IDENTITY_BYTES: std::cell::Cell<(u64, u64)> = const {
        std::cell::Cell::new((0, 0))
    };
}

/// What one logical family cost in a measured window: the coalesced mutations
/// handed to its tree, the underlying node reads and writes its walk performed,
/// and the bytes those moved. Measurement-only; nothing in production reads it.
#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FamilyWork {
    pub mutations: u64,
    pub node_reads: u64,
    pub node_writes: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    /// Chunks the walk found already resident, which cost no read. A miss is a
    /// `node_reads`, so hits plus reads is every chunk the walk resolved.
    pub cache_hits: u64,
    /// The level of the family's root when the walk started: how many chunks
    /// deep a root-to-leaf path is. Zero for a family whose tree is empty.
    pub tree_height: u64,
    /// Writes that landed on a storage key an earlier write of the same
    /// measured window had already written. Zero says every rewrite in the
    /// window is a distinct chunk, so no upper-level chunk on a shared path is
    /// being rewritten once per edit instead of once per commit.
    pub repeated_writes: u64,
    /// Writes that store a wide separator's key bytes rather than a chunk.
    pub key_blob_writes: u64,
}

#[cfg(feature = "test-seams")]
thread_local! {
    static FAMILY_WORK: std::cell::RefCell<BTreeMap<u8, FamilyWork>> =
        const { std::cell::RefCell::new(BTreeMap::new()) };
    /// The family whose tree the walk is inside, so the untagged node seams can
    /// attribute what they already count.
    static CURRENT_FAMILY: std::cell::Cell<Option<u8>> = const { std::cell::Cell::new(None) };
    /// Coalesced `Status` mutations by the namespace literal their key opens
    /// with, and the copies the fan-out constructors produced.
    static STATUS_NAMESPACES: std::cell::RefCell<BTreeMap<Vec<u8>, u64>> =
        const { std::cell::RefCell::new(BTreeMap::new()) };
}

#[cfg(feature = "test-seams")]
fn with_current_family(note: impl FnOnce(&mut FamilyWork)) {
    let Some(family) = CURRENT_FAMILY.with(|slot| slot.get()) else {
        return;
    };
    FAMILY_WORK.with(|work| note(work.borrow_mut().entry(family).or_default()));
}

#[cfg(feature = "test-seams")]
fn enter_family(family: LogicalFamily, mutations: usize) {
    CURRENT_FAMILY.with(|slot| slot.set(Some(family as u8)));
    redb::commit_work::set_tag(family as u8);
    with_current_family(|work| work.mutations = work.mutations.saturating_add(mutations as u64));
}

#[cfg(feature = "test-seams")]
fn leave_family() {
    CURRENT_FAMILY.with(|slot| slot.set(None));
    redb::commit_work::set_tag(redb::commit_work::TAG_UNTAGGED);
}

/// Where the content-identity stage's own time goes, in nanoseconds. The four
/// regions partition the proof walk: reading a stored chunk and decoding it;
/// hashing the chunks the walk builds; encoding a finished chunk and handing
/// its bytes to the storage engine; and the coalescing and grouping that turns
/// a commit's mutations into one sorted edit list per family. Test-seams only.
#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IdentitySpans {
    pub read_and_decode_nanos: u64,
    pub build_and_hash_nanos: u64,
    pub encode_and_store_nanos: u64,
    pub coalesce_and_group_nanos: u64,
}

#[cfg(feature = "test-seams")]
pub(crate) enum IdentitySpan {
    ReadAndDecode,
    BuildAndHash,
    EncodeAndStore,
    CoalesceAndGroup,
}

#[cfg(feature = "test-seams")]
thread_local! {
    static IDENTITY_SPANS: std::cell::Cell<IdentitySpans> =
        const { std::cell::Cell::new(IdentitySpans {
            read_and_decode_nanos: 0,
            build_and_hash_nanos: 0,
            encode_and_store_nanos: 0,
            coalesce_and_group_nanos: 0,
        }) };
}

#[cfg(feature = "test-seams")]
pub(crate) fn observe_identity_span(span: IdentitySpan, started: std::time::Instant) {
    let nanos = started.elapsed().as_nanos() as u64;
    IDENTITY_SPANS.with(|spans| {
        let mut current = spans.get();
        let field = match span {
            IdentitySpan::ReadAndDecode => &mut current.read_and_decode_nanos,
            IdentitySpan::BuildAndHash => &mut current.build_and_hash_nanos,
            IdentitySpan::EncodeAndStore => &mut current.encode_and_store_nanos,
            IdentitySpan::CoalesceAndGroup => &mut current.coalesce_and_group_nanos,
        };
        *field = field.saturating_add(nanos);
        spans.set(current);
    });
}

#[cfg(feature = "test-seams")]
pub(crate) fn take_identity_spans_for_test() -> IdentitySpans {
    IDENTITY_SPANS.with(|spans| spans.replace(IdentitySpans::default()))
}

/// How many copied records a commit hashed to bind the compact copies to the
/// committed family roots: vector partition memberships for the vector
/// directory, and custody policy and delivery records for the custody index.
/// Test-seams only.
#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DirectoryBindingWork {
    pub vector_memberships: u64,
    pub custody_records: u64,
}

#[cfg(feature = "test-seams")]
thread_local! {
    /// Records hashed since the last root publication on this thread.
    static DIRECTORY_BINDING_PENDING: std::cell::Cell<DirectoryBindingWork> =
        const { std::cell::Cell::new(DirectoryBindingWork {
            vector_memberships: 0,
            custody_records: 0,
        }) };
    /// Records the most recent root publication on this thread hashed.
    static DIRECTORY_BINDING_LAST: std::cell::Cell<DirectoryBindingWork> =
        const { std::cell::Cell::new(DirectoryBindingWork {
            vector_memberships: 0,
            custody_records: 0,
        }) };
}

#[cfg(feature = "test-seams")]
pub(crate) fn observe_vector_directory_binding_records(records: u64) {
    DIRECTORY_BINDING_PENDING.with(|work| {
        let mut current = work.get();
        current.vector_memberships = current.vector_memberships.saturating_add(records);
        work.set(current);
    });
}

#[cfg(feature = "test-seams")]
pub(crate) fn observe_custody_binding_records(records: u64) {
    DIRECTORY_BINDING_PENDING.with(|work| {
        let mut current = work.get();
        current.custody_records = current.custody_records.saturating_add(records);
        work.set(current);
    });
}

#[cfg(feature = "test-seams")]
fn publish_directory_binding_work() {
    let pending =
        DIRECTORY_BINDING_PENDING.with(|work| work.replace(DirectoryBindingWork::default()));
    DIRECTORY_BINDING_LAST.with(|work| work.set(pending));
}

#[cfg(feature = "test-seams")]
pub(crate) fn last_directory_binding_work_for_test() -> DirectoryBindingWork {
    DIRECTORY_BINDING_LAST.with(|work| work.get())
}

#[cfg(feature = "test-seams")]
thread_local! {
    /// The walk found a chunk it had already read.
    ///
    /// Storage keys written in this window, so a key written twice is named
    /// rather than counted once.
    static WRITTEN_KEYS: std::cell::RefCell<std::collections::BTreeSet<[u8; 33]>> =
        const { std::cell::RefCell::new(std::collections::BTreeSet::new()) };
    // Exact rewrite identities belong only to an explicitly reset/taken
    // measurement window, never to the lifetime of an ordinary writer thread.
    static WRITTEN_KEYS_ACTIVE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Chunk rewrites by the level of the chunk rewritten, and the pages the
    /// storage engine copied while each level was being written. A chunk's id
    /// opens with its level, so the level is read from the key rather than
    /// threaded through the walk.
    static WRITES_BY_LEVEL: std::cell::RefCell<BTreeMap<u8, (u64, u64)>> =
        const { std::cell::RefCell::new(BTreeMap::new()) };
}

/// Chunk rewrites and copied pages by the level of the chunk, since the last
/// reset.
#[cfg(feature = "test-seams")]
pub(crate) fn take_writes_by_level_for_test() -> Vec<(u8, u64, u64)> {
    WRITES_BY_LEVEL.with(|levels| {
        std::mem::take(&mut *levels.borrow_mut())
            .into_iter()
            .map(|(level, (writes, pages))| (level, writes, pages))
            .collect()
    })
}

/// One write landing on a storage key. A key this window has already written is
/// a repeat, which is what a per-edit rewrite of a shared upper-level chunk
/// would look like.
#[cfg(feature = "test-seams")]
fn observe_written_key(key: &[u8; 33], is_key_blob: bool) {
    let repeated = WRITTEN_KEYS_ACTIVE.with(|active| {
        active.get() && WRITTEN_KEYS.with(|written| !written.borrow_mut().insert(*key))
    });
    if !is_key_blob {
        // Byte 1 of a stored key is the chunk's level; byte 0 is its family.
        WRITES_BY_LEVEL.with(|levels| levels.borrow_mut().entry(key[1]).or_default().0 += 1);
    }
    with_current_family(|work| {
        if repeated {
            work.repeated_writes = work.repeated_writes.saturating_add(1);
        }
        if is_key_blob {
            work.key_blob_writes = work.key_blob_writes.saturating_add(1);
        }
    });
}

#[cfg(feature = "test-seams")]
fn observe_cache_hit() {
    with_current_family(|work| work.cache_hits = work.cache_hits.saturating_add(1));
}

/// How tall the family's tree was when its walk started.
#[cfg(feature = "test-seams")]
fn observe_tree_height(height: u32) {
    with_current_family(|work| work.tree_height = work.tree_height.max(height as u64));
}

/// The namespace a `Status` logical key opens with. `status_mutation` builds its
/// key with `key_parts`, so the first component is the namespace literal. A
/// `Status` member written by `config_mutations` or `schema_mutations` carries
/// the bare config key or table name instead; that key is reported verbatim
/// behind a `raw:` prefix, so a duplicate of a fact another family already
/// holds is named rather than merely counted.
#[cfg(feature = "test-seams")]
fn status_namespace(logical_key: &[u8]) -> Vec<u8> {
    fn raw(logical_key: &[u8]) -> Vec<u8> {
        let mut named = b"raw:".to_vec();
        named.extend_from_slice(&logical_key[..logical_key.len().min(96)]);
        named
    }
    let mut rest = logical_key;
    let mut first: Option<&[u8]> = None;
    while !rest.is_empty() {
        if rest.len() < 8 {
            return raw(logical_key);
        }
        let len = u64::from_le_bytes(rest[..8].try_into().expect("eight bytes")) as usize;
        if rest.len() - 8 < len {
            return raw(logical_key);
        }
        let (part, tail) = rest[8..].split_at(len);
        first.get_or_insert(part);
        rest = tail;
    }
    match first {
        Some(namespace) => namespace.to_vec(),
        None => raw(logical_key),
    }
}

#[cfg(feature = "test-seams")]
fn observe_status_namespace(logical_key: &[u8]) {
    let namespace = status_namespace(logical_key);
    STATUS_NAMESPACES.with(|counts| {
        *counts.borrow_mut().entry(namespace).or_default() += 1;
    });
}

#[cfg(feature = "test-seams")]
fn forget_status_namespace(logical_key: &[u8]) {
    let namespace = status_namespace(logical_key);
    STATUS_NAMESPACES.with(|counts| {
        let mut counts = counts.borrow_mut();
        if let Some(count) = counts.get_mut(&namespace) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(&namespace);
            }
        }
    });
}

#[cfg(feature = "test-seams")]
pub(crate) fn reset_family_work_for_test() {
    WRITTEN_KEYS_ACTIVE.with(|active| active.set(true));
    FAMILY_WORK.with(|work| work.borrow_mut().clear());
    WRITTEN_KEYS.with(|written| written.borrow_mut().clear());
    WRITES_BY_LEVEL.with(|levels| levels.borrow_mut().clear());
    IDENTITY_SPANS.with(|spans| spans.set(IdentitySpans::default()));
    CURRENT_FAMILY.with(|slot| slot.set(None));
    STATUS_NAMESPACES.with(|counts| counts.borrow_mut().clear());
    redb::commit_work::reset_page_tags_and_spans_for_test();
}

/// Per-family work since the last reset, as `(tag, family name, counts)`.
#[cfg(feature = "test-seams")]
pub(crate) fn take_family_work_for_test() -> Vec<(u8, &'static str, FamilyWork)> {
    WRITTEN_KEYS_ACTIVE.with(|active| active.set(false));
    WRITTEN_KEYS.with(|written| written.borrow_mut().clear());
    FAMILY_WORK.with(|work| {
        std::mem::take(&mut *work.borrow_mut())
            .into_iter()
            .map(|(tag, counts)| {
                let name = LogicalFamily::from_tag(tag)
                    .map(LogicalFamily::name_for_test)
                    .unwrap_or("unknown");
                (tag, name, counts)
            })
            .collect()
    })
}

/// Coalesced `Status` mutations by namespace literal since the last reset.
/// The family a page tag names, for the storage engine's per-tag page counts.
#[cfg(feature = "test-seams")]
pub(crate) fn family_name_for_tag(tag: u8) -> &'static str {
    LogicalFamily::from_tag(tag)
        .map(LogicalFamily::name_for_test)
        .unwrap_or("unknown")
}

#[cfg(feature = "test-seams")]
pub(crate) fn take_status_namespaces_for_test() -> Vec<(String, u64)> {
    STATUS_NAMESPACES.with(|counts| {
        std::mem::take(&mut *counts.borrow_mut())
            .into_iter()
            .map(|(namespace, count)| (String::from_utf8_lossy(&namespace).into_owned(), count))
            .collect()
    })
}

#[cfg(feature = "test-seams")]
pub(crate) fn reset_work_for_test() {
    IDENTITY_WORK.with(|work| work.set((0, 0, 0)));
    IDENTITY_BYTES.with(|bytes| bytes.set((0, 0)));
}

#[cfg(feature = "test-seams")]
pub(crate) fn take_work_for_test() -> (u64, u64, u64) {
    IDENTITY_WORK.with(|work| work.replace((0, 0, 0)))
}

#[cfg(feature = "test-seams")]
pub(crate) fn take_work_bytes_for_test() -> (u64, u64) {
    IDENTITY_BYTES.with(|bytes| bytes.replace((0, 0)))
}

#[cfg(feature = "test-seams")]
thread_local! {
    /// Peak proof bytes charged: the mutation path's chunk bodies, the chunk
    /// record it must keep for the flush, and one read walk's whole charge.
    static PROOF_MEMORY_PEAK: std::cell::Cell<(usize, usize, usize)> = const {
        std::cell::Cell::new((0, 0, 0))
    };
}

#[cfg(feature = "test-seams")]
fn observe_proof_memory(bodies: usize, records: usize) {
    PROOF_MEMORY_PEAK.with(|peak| {
        let (cached, kept, read) = peak.get();
        peak.set((cached.max(bodies), kept.max(records), read));
    });
}

#[cfg(feature = "test-seams")]
fn observe_read_proof_memory(bytes: usize) {
    PROOF_MEMORY_PEAK.with(|peak| {
        let (cached, kept, read) = peak.get();
        peak.set((cached, kept, read.max(bytes)));
    });
}

/// Since the last call: the peak chunk bodies a mutation transaction held,
/// the chunk record it kept for its flush, and the peak a read walk charged.
#[cfg(feature = "test-seams")]
#[allow(dead_code)] // Read by the proof-memory contracts beside this module.
pub(crate) fn take_proof_memory_peak_for_test() -> (usize, usize, usize) {
    PROOF_MEMORY_PEAK.with(|peak| peak.replace((0, 0, 0)))
}

#[cfg(feature = "test-seams")]
thread_local! {
    /// Ordered (`"charge"` / `"copy"`) observations of a referenced-key admission.
    /// A copy that precedes its charge is the defect: the allocation exists before
    /// the accountant has admitted it.
    static RESOLVED_KEY_EVENTS: std::cell::RefCell<Vec<(&'static str, usize)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static RESOLVED_KEY_WATCH_ENABLED: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[cfg(feature = "test-seams")]
#[allow(dead_code)] // Read by the proof-memory contracts beside this module.
#[must_use = "keep the watch alive while observing referenced-key copies"]
pub(crate) struct ResolvedKeyCopyWatchForTest(std::marker::PhantomData<std::rc::Rc<()>>);

#[cfg(feature = "test-seams")]
impl Drop for ResolvedKeyCopyWatchForTest {
    fn drop(&mut self) {
        RESOLVED_KEY_WATCH_ENABLED.with(|enabled| enabled.set(false));
        RESOLVED_KEY_EVENTS.with(|obs| drop(std::mem::take(&mut *obs.borrow_mut())));
    }
}

#[cfg(feature = "test-seams")]
#[allow(dead_code)] // Read by the proof-memory contracts beside this module.
pub(crate) fn watch_resolved_key_copies_for_test() -> ResolvedKeyCopyWatchForTest {
    RESOLVED_KEY_WATCH_ENABLED.with(|enabled| {
        assert!(!enabled.get(), "referenced-key watches must not overlap");
    });
    RESOLVED_KEY_EVENTS.with(|obs| drop(std::mem::take(&mut *obs.borrow_mut())));
    RESOLVED_KEY_WATCH_ENABLED.with(|enabled| enabled.set(true));
    ResolvedKeyCopyWatchForTest(std::marker::PhantomData)
}

#[cfg(feature = "test-seams")]
#[allow(dead_code)] // Read by the proof-memory contracts beside this module.
pub(crate) fn take_resolved_key_copy_observations_for_test() -> Vec<(&'static str, usize)> {
    RESOLVED_KEY_EVENTS.with(|obs| std::mem::take(&mut *obs.borrow_mut()))
}

#[cfg(feature = "test-seams")]
fn observe_resolved_key_event(kind: &'static str, bytes: usize) {
    if RESOLVED_KEY_WATCH_ENABLED.with(|enabled| enabled.get()) {
        RESOLVED_KEY_EVENTS.with(|obs| obs.borrow_mut().push((kind, bytes)));
    }
}

#[cfg(feature = "test-seams")]
thread_local! {
    /// Entries inspected or moved by one resident-chunk `touch`.
    static TOUCH_WORK: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(feature = "test-seams")]
pub(crate) fn reset_touch_work_for_test() {
    TOUCH_WORK.with(|work| work.set(0));
}

#[cfg(feature = "test-seams")]
pub(crate) fn take_touch_work_for_test() -> u64 {
    TOUCH_WORK.with(|work| work.replace(0))
}

#[cfg(feature = "test-seams")]
fn observe_touch_work(entries: u64) {
    TOUCH_WORK.with(|work| work.set(work.get().saturating_add(entries)));
}

#[cfg(feature = "test-seams")]
fn observe_node_read(bytes: usize) {
    // Count only the underlying Redb read. Transaction-local cache hits do
    // not pass through this seam.
    IDENTITY_WORK.with(|work| {
        let (reads, writes, removes) = work.get();
        work.set((reads + 1, writes, removes));
    });
    IDENTITY_BYTES.with(|work| {
        let (read, written) = work.get();
        work.set((read.saturating_add(bytes as u64), written));
    });
    with_current_family(|work| {
        work.node_reads = work.node_reads.saturating_add(1);
        work.bytes_read = work.bytes_read.saturating_add(bytes as u64);
    });
}

#[cfg(feature = "test-seams")]
fn observe_node_write(bytes: usize) {
    // Count only the underlying Redb insert performed by the final flush.
    IDENTITY_WORK.with(|work| {
        let (reads, writes, removes) = work.get();
        work.set((reads, writes + 1, removes));
    });
    IDENTITY_BYTES.with(|work| {
        let (read, written) = work.get();
        work.set((read, written.saturating_add(bytes as u64)));
    });
    with_current_family(|work| {
        work.node_writes = work.node_writes.saturating_add(1);
        work.bytes_written = work.bytes_written.saturating_add(bytes as u64);
    });
}

#[cfg(feature = "test-seams")]
fn observe_node_remove() {
    // Count only the underlying Redb removal performed by the final flush.
    IDENTITY_WORK.with(|work| {
        let (reads, writes, removes) = work.get();
        work.set((reads, writes, removes + 1));
    });
}

const IDENTITY_VERSION_KEY: &str = "logical_content_identity_version";
const IDENTITY_VERSION: &[u8] = b"canonical-key-pages";
const NODE_VERSION: u8 = 1;
const ROOT_VERSION: u8 = 2;
const ORDERED_POSTING_MAGIC: &[u8] = b"\xffcontextdb-index-posting\0";
const HASHED_KEY_TAG: u8 = 0;
const ORDERED_POSTING_KEY_TAG: u8 = 1;
// Tag 2 is the in-node key reference (`KEY_REFERENCE_TAG`), never a resolved
// key, so the ordered history key space takes 3.
const ORDERED_HISTORY_MAGIC: &[u8] = b"\xffcontextdb-history\0";
const ORDERED_HISTORY_KEY_TAG: u8 = 3;
const POSTING_LOWER_BOUND_TAG: u8 = 0;
const POSTING_ACTUAL_TAG: u8 = 1;
const POSTING_UPPER_BOUND_TAG: u8 = 2;
// Redb's base page is 4 KiB. A 2 KiB target leaves room for the
// upper CDF tail, table key and page headers without a large-value page.
const TARGET_NODE_ENTRY_BYTES: usize = 2 * 1024;
const MAX_NODE_BYTES: usize = 64 * 1024;
// A canonical hierarchy contracts, so this level ceiling is a decode bound,
// not a shape a real tree reaches.
const MAX_CHUNK_LEVEL: u32 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub(crate) enum LogicalFamily {
    Relational = 0,
    Graph = 1,
    F32Vectors = 2,
    Sq8Vectors = 3,
    Sq4Vectors = 4,
    SchemaAndIndexes = 5,
    Policy = 6,
    Sync = 7,
    ChangeAndDdl = 8,
    Configuration = 9,
    Status = 10,
}

impl LogicalFamily {
    /// Measurement-only display name.
    #[cfg(feature = "test-seams")]
    pub(crate) const fn name_for_test(self) -> &'static str {
        match self {
            Self::Relational => "Relational",
            Self::Graph => "Graph",
            Self::F32Vectors => "F32Vectors",
            Self::Sq8Vectors => "Sq8Vectors",
            Self::Sq4Vectors => "Sq4Vectors",
            Self::SchemaAndIndexes => "SchemaAndIndexes",
            Self::Policy => "Policy",
            Self::Sync => "Sync",
            Self::ChangeAndDdl => "ChangeAndDdl",
            Self::Configuration => "Configuration",
            Self::Status => "Status",
        }
    }

    pub(crate) const ALL: [Self; 11] = [
        Self::Relational,
        Self::Graph,
        Self::F32Vectors,
        Self::Sq8Vectors,
        Self::Sq4Vectors,
        Self::SchemaAndIndexes,
        Self::Policy,
        Self::Sync,
        Self::ChangeAndDdl,
        Self::Configuration,
        Self::Status,
    ];

    fn from_tag(tag: u8) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|family| *family as u8 == tag)
            .ok_or_else(|| Error::Other(format!("unknown logical content family tag {tag}")))
    }

    fn carries_vector_directory_projection(self) -> bool {
        matches!(self, Self::F32Vectors | Self::Sq8Vectors | Self::Sq4Vectors)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ContentIdentityMutation {
    pub(crate) family: LogicalFamily,
    pub(crate) logical_key: Vec<u8>,
    pub(crate) logical_value: Option<Vec<u8>>,
    pub(crate) row_head: Option<Option<[u8; 24]>>,
}

impl ContentIdentityMutation {
    pub(crate) fn put(
        family: LogicalFamily,
        logical_key: impl Into<Vec<u8>>,
        logical_value: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            family,
            logical_key: logical_key.into(),
            logical_value: Some(logical_value.into()),
            row_head: None,
        }
    }

    pub(crate) fn remove(family: LogicalFamily, logical_key: impl Into<Vec<u8>>) -> Self {
        Self {
            family,
            logical_key: logical_key.into(),
            logical_value: None,
            row_head: None,
        }
    }
}

fn ordered_posting_logical_key(table: &str, index: &str, order_key: &[u8]) -> Vec<u8> {
    let mut key = ORDERED_POSTING_MAGIC.to_vec();
    key.extend_from_slice(&(table.len() as u32).to_be_bytes());
    key.extend_from_slice(table.as_bytes());
    key.extend_from_slice(&(index.len() as u32).to_be_bytes());
    key.extend_from_slice(index.as_bytes());
    key.push(POSTING_ACTUAL_TAG);
    key.extend_from_slice(order_key);
    key
}

pub(crate) fn relational_index_posting_mutation(
    table: &str,
    index: &str,
    order_key: &[u8],
) -> ContentIdentityMutation {
    ContentIdentityMutation::put(
        LogicalFamily::SchemaAndIndexes,
        ordered_posting_logical_key(table, index, order_key),
        [],
    )
}

pub(crate) fn remove_relational_index_posting_mutation(
    table: &str,
    index: &str,
    order_key: &[u8],
) -> ContentIdentityMutation {
    ContentIdentityMutation::remove(
        LogicalFamily::SchemaAndIndexes,
        ordered_posting_logical_key(table, index, order_key),
    )
}

fn key_parts(parts: &[&[u8]]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for part in parts {
        encoded.extend_from_slice(&(part.len() as u64).to_le_bytes());
        encoded.extend_from_slice(part);
    }
    encoded
}

fn canonical_json<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    let encoding_failure = |error: serde_json::Error| {
        Error::Other(format!("cannot encode logical identity leaf: {error}"))
    };
    let value = canonicalize_json_value(serde_json::to_value(value).map_err(encoding_failure)?);
    // A leaf can hold one very large value. Letting the output grow by
    // doubling would reserve up to twice the leaf and briefly hold the old
    // and new buffers together while it copies, so the leaf is measured
    // first and written once into a buffer of exactly its length.
    let mut measured = EncodedLength(0);
    serde_json::to_writer(&mut measured, &value).map_err(encoding_failure)?;
    let mut encoded = Vec::with_capacity(measured.0);
    serde_json::to_writer(&mut encoded, &value).map_err(encoding_failure)?;
    Ok(encoded)
}

/// Counts the bytes an encoder would write without keeping any of them.
struct EncodedLength(usize);

impl std::io::Write for EncodedLength {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn canonicalize_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonicalize_json_value).collect())
        }
        serde_json::Value::Object(values) => {
            let mut entries = values.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonicalize_json_value(value)))
                    .collect(),
            )
        }
        scalar => scalar,
    }
}

pub(crate) fn row_mutation(
    table: &str,
    row: &VersionedRow,
    meta: Option<&TableMeta>,
) -> Result<ContentIdentityMutation> {
    let row_id = row.row_id.0.to_le_bytes();
    let key = key_parts(&[table.as_bytes(), &row_id]);
    if row.deleted_tx.is_some() {
        let mut mutation = ContentIdentityMutation::remove(LogicalFamily::Relational, key);
        mutation.row_head = Some(Some(RedbPersistence::rel_row_key(row).try_into().unwrap()));
        return Ok(mutation);
    }
    let values = row
        .values
        .iter()
        .filter(|(name, value)| {
            !matches!(value, Value::Vector(_))
                && !meta.is_some_and(|meta| {
                    meta.columns.iter().any(|column| {
                        column.name.as_str() == name.as_str()
                            && matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
                    })
                })
        })
        // Encoded by reference: the leaf is the canonical encoding of the
        // row's values, and copying every value first would hold the row
        // twice while it is written.
        .collect::<BTreeMap<&String, &Value>>();
    let mut mutation =
        ContentIdentityMutation::put(LogicalFamily::Relational, key, canonical_json(&values)?);
    mutation.row_head = Some(Some(RedbPersistence::rel_row_key(row).try_into().unwrap()));
    Ok(mutation)
}

pub(crate) fn remove_row_mutation(table: &str, row_id: RowId) -> ContentIdentityMutation {
    let mut mutation = ContentIdentityMutation::remove(
        LogicalFamily::Relational,
        key_parts(&[table.as_bytes(), &row_id.0.to_le_bytes()]),
    );
    mutation.row_head = Some(None);
    mutation
}

pub(crate) fn edge_mutation(edge: &AdjEntry) -> Result<ContentIdentityMutation> {
    let key = key_parts(&[
        edge.edge_type.as_bytes(),
        edge.source.as_bytes(),
        edge.target.as_bytes(),
    ]);
    if edge.deleted_tx.is_some() {
        return Ok(ContentIdentityMutation::remove(LogicalFamily::Graph, key));
    }
    let properties = edge
        .properties
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    Ok(ContentIdentityMutation::put(
        LogicalFamily::Graph,
        key,
        canonical_json(&properties)?,
    ))
}

pub(crate) fn remove_edge_mutation(
    source: uuid::Uuid,
    edge_type: &str,
    target: uuid::Uuid,
) -> ContentIdentityMutation {
    ContentIdentityMutation::remove(
        LogicalFamily::Graph,
        key_parts(&[edge_type.as_bytes(), source.as_bytes(), target.as_bytes()]),
    )
}

fn vector_family(quantization: VectorQuantization) -> LogicalFamily {
    match quantization {
        VectorQuantization::F32 => LogicalFamily::F32Vectors,
        VectorQuantization::SQ8 => LogicalFamily::Sq8Vectors,
        VectorQuantization::SQ4 => LogicalFamily::Sq4Vectors,
    }
}

fn vector_key(index: &VectorIndexRef, row_id: RowId) -> Vec<u8> {
    key_parts(&[
        index.table.as_bytes(),
        index.column.as_bytes(),
        &row_id.0.to_le_bytes(),
    ])
}

pub(crate) fn vector_mutation(
    entry: &VectorEntry,
    quantization: VectorQuantization,
) -> ContentIdentityMutation {
    vector_value_mutation(&entry.index, entry.row_id, &entry.vector, quantization)
}

pub(crate) fn decoded_vector_mutation(
    entry: &VectorEntry,
    quantization: VectorQuantization,
) -> ContentIdentityMutation {
    decoded_vector_value_mutation(&entry.index, entry.row_id, &entry.vector, quantization)
}

pub(crate) fn decoded_vector_value_mutation(
    index: &VectorIndexRef,
    row_id: RowId,
    stored: &[f32],
    quantization: VectorQuantization,
) -> ContentIdentityMutation {
    encoded_vector_value_mutation(index, row_id, stored, quantization)
}

pub(crate) fn vector_value_mutation(
    index: &VectorIndexRef,
    row_id: RowId,
    vector: &[f32],
    quantization: VectorQuantization,
) -> ContentIdentityMutation {
    let stored = contextdb_vector::stored_vector_value(vector, quantization);
    encoded_vector_value_mutation(index, row_id, &stored, quantization)
}

fn encoded_vector_value_mutation(
    index: &VectorIndexRef,
    row_id: RowId,
    stored: &[f32],
    quantization: VectorQuantization,
) -> ContentIdentityMutation {
    let mut value = Vec::with_capacity(8 + stored.len() * 4);
    value.extend_from_slice(&(stored.len() as u64).to_le_bytes());
    for component in stored {
        value.extend_from_slice(&component.to_bits().to_le_bytes());
    }
    ContentIdentityMutation::put(
        vector_family(quantization),
        vector_key(index, row_id),
        value,
    )
}

fn vector_version_key(
    index: &VectorIndexRef,
    row_id: RowId,
    created_tx: TxId,
    lsn: Lsn,
    quantization: VectorQuantization,
) -> Vec<u8> {
    hash_parts(
        b"contextdb-vector-version-key-v1",
        &[
            index.table.as_bytes(),
            index.column.as_bytes(),
            &row_id.0.to_le_bytes(),
            &created_tx.0.to_le_bytes(),
            &lsn.0.to_le_bytes(),
            &[match quantization {
                VectorQuantization::F32 => 0,
                VectorQuantization::SQ8 => 1,
                VectorQuantization::SQ4 => 2,
            }],
        ],
    )
    .to_vec()
}

fn encoded_vector_version_value_mutation(
    index: &VectorIndexRef,
    row_id: RowId,
    created_tx: TxId,
    lsn: Lsn,
    stored: &[f32],
    quantization: VectorQuantization,
) -> ContentIdentityMutation {
    let mut value = Vec::with_capacity(8 + stored.len() * 4);
    value.extend_from_slice(&(stored.len() as u64).to_le_bytes());
    for component in stored {
        value.extend_from_slice(&component.to_bits().to_le_bytes());
    }
    ContentIdentityMutation::put(
        LogicalFamily::ChangeAndDdl,
        vector_version_key(index, row_id, created_tx, lsn, quantization),
        value,
    )
}

pub(crate) fn vector_version_mutation(
    entry: &VectorEntry,
    quantization: VectorQuantization,
) -> ContentIdentityMutation {
    let stored = contextdb_vector::stored_vector_value(&entry.vector, quantization);
    encoded_vector_version_value_mutation(
        &entry.index,
        entry.row_id,
        entry.created_tx,
        entry.lsn,
        &stored,
        quantization,
    )
}

pub(crate) fn decoded_vector_version_mutation(
    entry: &VectorEntry,
    quantization: VectorQuantization,
) -> ContentIdentityMutation {
    decoded_vector_version_value_mutation(
        &entry.index,
        entry.row_id,
        entry.created_tx,
        entry.lsn,
        &entry.vector,
        quantization,
    )
}

pub(crate) fn decoded_vector_version_value_mutation(
    index: &VectorIndexRef,
    row_id: RowId,
    created_tx: TxId,
    lsn: Lsn,
    stored: &[f32],
    quantization: VectorQuantization,
) -> ContentIdentityMutation {
    encoded_vector_version_value_mutation(index, row_id, created_tx, lsn, stored, quantization)
}

pub(crate) fn remove_vector_version_mutation(
    index: &VectorIndexRef,
    row_id: RowId,
    created_tx: TxId,
    lsn: Lsn,
    quantization: VectorQuantization,
) -> ContentIdentityMutation {
    ContentIdentityMutation::remove(
        LogicalFamily::ChangeAndDdl,
        vector_version_key(index, row_id, created_tx, lsn, quantization),
    )
}

pub(crate) fn remove_vector_mutation(
    index: &VectorIndexRef,
    row_id: RowId,
    quantization: VectorQuantization,
) -> ContentIdentityMutation {
    ContentIdentityMutation::remove(vector_family(quantization), vector_key(index, row_id))
}

pub(crate) fn schema_mutations(
    name: &str,
    meta: &TableMeta,
) -> Result<Vec<ContentIdentityMutation>> {
    let value = canonical_json(meta)?;
    // One logical fact, one tree member. The `schema_and_indexes`, `sync` and
    // `status` digests keep covering a table's schema by deriving from this
    // family's root in `digest_from_roots`, not by holding a copy of it.
    Ok(vec![ContentIdentityMutation::put(
        SCHEMA_RECORD_FAMILY,
        name.as_bytes(),
        value,
    )])
}

pub(crate) fn remove_schema_mutations(name: &str) -> Vec<ContentIdentityMutation> {
    vec![ContentIdentityMutation::remove(
        SCHEMA_RECORD_FAMILY,
        name.as_bytes(),
    )]
}

pub(crate) fn config_mutations(key: &str, value: Option<&[u8]>) -> Vec<ContentIdentityMutation> {
    // One logical fact, one tree member. The `sync` and `status` digests keep
    // covering declared configuration by deriving from the Configuration root
    // in `digest_from_roots`, not by holding a copy of each config record.
    vec![match value {
        Some(value) => {
            ContentIdentityMutation::put(LogicalFamily::Configuration, key.as_bytes(), value)
        }
        None => ContentIdentityMutation::remove(LogicalFamily::Configuration, key.as_bytes()),
    }]
}

pub(crate) fn sync_source_mutation(
    table: &str,
    row_id: RowId,
    source_lsn: u64,
    kind: u8,
) -> ContentIdentityMutation {
    let mut key = table.as_bytes().to_vec();
    key.extend_from_slice(&row_id.0.to_be_bytes());
    let mut value = source_lsn.to_le_bytes().to_vec();
    value.push(kind);
    ContentIdentityMutation::put(LogicalFamily::Sync, key, value)
}

pub(crate) fn remove_sync_source_mutation(table: &str, row_id: RowId) -> ContentIdentityMutation {
    let mut key = table.as_bytes().to_vec();
    key.extend_from_slice(&row_id.0.to_be_bytes());
    ContentIdentityMutation::remove(LogicalFamily::Sync, key)
}

/// The three stored histories. Each lives in its own ordered range of the
/// `ChangeAndDdl` family, keyed by the exact coordinate its table stores it
/// under, so the committed tree enumerates every member in the order the
/// table -- and so the author -- holds them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum HistoryKind {
    CommitIndex = 0,
    ChangeLog = 1,
    DdlLog = 2,
    RelationalVersion = 3,
}

/// A stored history record type and the range it is committed in.
pub(crate) trait HistoryRecord: Serialize {
    const KIND: HistoryKind;

    /// Public history keeps its canonical JSON commitment. Private wrappers
    /// override this with their exact durable manual encoding.
    fn commitment_bytes(&self) -> Result<Vec<u8>> {
        canonical_json(self)
    }
}

impl HistoryRecord for crate::composite_store::ChangeLogEntry {
    const KIND: HistoryKind = HistoryKind::ChangeLog;
}

impl HistoryRecord for crate::sync_types::DdlChange {
    const KIND: HistoryKind = HistoryKind::DdlLog;
}

impl<T: HistoryRecord> HistoryRecord for &T {
    const KIND: HistoryKind = T::KIND;

    fn commitment_bytes(&self) -> Result<Vec<u8>> {
        T::commitment_bytes(*self)
    }
}

fn history_logical_key(kind: HistoryKind, coordinate: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(ORDERED_HISTORY_MAGIC.len() + 1 + coordinate.len());
    key.extend_from_slice(ORDERED_HISTORY_MAGIC);
    key.push(kind as u8);
    key.extend_from_slice(coordinate);
    key
}

pub(super) fn history_tree_key(kind: HistoryKind, coordinate: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + coordinate.len());
    key.push(ORDERED_HISTORY_KEY_TAG);
    key.push(kind as u8);
    key.extend_from_slice(coordinate);
    key
}

/// A stored change or schema key is a zero-padded LSN, optionally followed by
/// `:` and a zero-padded ordinal. Anything else is not a key a writer stores.
fn validate_history_key(physical_key: &str) -> Result<()> {
    let (lsn, ordinal) = match physical_key.split_once(':') {
        Some((lsn, ordinal)) => (lsn, Some(ordinal)),
        None => (physical_key, None),
    };
    let digits = |part: &str, width: usize| {
        part.len() >= width && part.bytes().all(|byte| byte.is_ascii_digit())
    };
    if lsn.len() == 20 && digits(lsn, 20) && ordinal.is_none_or(|ordinal| digits(ordinal, 6)) {
        Ok(())
    } else {
        Err(Error::Other(format!(
            "stored history key {physical_key:?} is not an LSN and ordinal coordinate"
        )))
    }
}

pub(super) fn ddl_mutation_coordinate(mutation: &ContentIdentityMutation) -> Option<&str> {
    if mutation.family != LogicalFamily::ChangeAndDdl {
        return None;
    }
    let raw = mutation.logical_key.strip_prefix(ORDERED_HISTORY_MAGIC)?;
    if raw.first().copied() != Some(HistoryKind::DdlLog as u8) {
        return None;
    }
    std::str::from_utf8(&raw[1..]).ok()
}

pub(super) fn ddl_selector_roots_in_read(read: &redb::ReadTransaction) -> Result<[[u8; 32]; 2]> {
    let roots = read
        .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let encoded = roots
        .get(LogicalFamily::ChangeAndDdl as u8)
        .map_err(RedbPersistence::storage_error)?
        .ok_or_else(|| Error::StoreCorrupted {
            path: "ddl_eligible_occurrences".into(),
            reason: "missing history selector binding".into(),
        })?;
    decode_published_root(LogicalFamily::ChangeAndDdl, encoded.value())
        .map_err(|_| Error::StoreCorrupted {
            path: "ddl_eligible_occurrences".into(),
            reason: "invalid mandatory history selector root tail".into(),
        })?
        .ddl_selector_roots
        .ok_or_else(|| Error::StoreCorrupted {
            path: "ddl_eligible_occurrences".into(),
            reason: "missing declaration selector root tail".into(),
        })
}

pub(super) fn ddl_selector_roots_in_write(write: &redb::WriteTransaction) -> Result<[[u8; 32]; 2]> {
    let roots = write
        .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let encoded = roots
        .get(LogicalFamily::ChangeAndDdl as u8)
        .map_err(RedbPersistence::storage_error)?
        .ok_or_else(|| Error::StoreCorrupted {
            path: "ddl_eligible_occurrences".into(),
            reason: "missing history selector binding".into(),
        })?;
    decode_published_root(LogicalFamily::ChangeAndDdl, encoded.value())
        .map_err(|_| Error::StoreCorrupted {
            path: "ddl_eligible_occurrences".into(),
            reason: "invalid mandatory history selector root tail".into(),
        })?
        .ddl_selector_roots
        .ok_or_else(|| Error::StoreCorrupted {
            path: "ddl_eligible_occurrences".into(),
            reason: "missing declaration selector root tail".into(),
        })
}

impl RedbPersistence {
    pub(super) fn rebind_ddl_selector_roots_in_write(write: &redb::WriteTransaction) -> Result<()> {
        let fingerprints = Self::ddl_selector_fingerprints_in_write(write)?;
        let mut roots = write
            .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
            .map_err(Self::storage_error)?;
        let mut root = {
            let encoded = roots
                .get(LogicalFamily::ChangeAndDdl as u8)
                .map_err(Self::storage_error)?
                .ok_or_else(|| Error::StoreCorrupted {
                    path: "ddl_eligible_occurrences".into(),
                    reason: "missing history selector binding".into(),
                })?;
            decode_published_root(LogicalFamily::ChangeAndDdl, encoded.value())?
        };
        root.ddl_selector_roots = Some(fingerprints);
        let content_fingerprint = write
            .table_root_fingerprint(super::content_size::TOTALS)
            .map_err(Self::storage_error)?
            .unwrap_or([0; 32]);
        let mut config = {
            let value = roots
                .get(LogicalFamily::Configuration as u8)
                .map_err(Self::storage_error)?
                .ok_or_else(|| {
                    Error::Other("missing configuration root during compaction".into())
                })?;
            decode_published_root(LogicalFamily::Configuration, value.value())?
        };
        config.content_size_root = Some(content_fingerprint);
        config.vector_pages_root = Some(vector_schema_pages_root_in_write(write)?);
        bind_vector_schema_meta_root_in_write(write)?;
        roots
            .insert(
                LogicalFamily::Configuration as u8,
                encode_published_root(&config).as_slice(),
            )
            .map_err(Self::storage_error)?;
        let encoded = encode_published_root(&root);
        roots
            .insert(LogicalFamily::ChangeAndDdl as u8, encoded.as_slice())
            .map_err(Self::storage_error)?;
        Ok(())
    }
}

pub(crate) fn encoded_history_mutation<T: HistoryRecord>(
    physical_key: &str,
    value: &T,
) -> Result<ContentIdentityMutation> {
    validate_history_key(physical_key)?;
    Ok(ContentIdentityMutation::put(
        LogicalFamily::ChangeAndDdl,
        history_logical_key(T::KIND, physical_key.as_bytes()),
        value.commitment_bytes()?,
    ))
}

/// Caller validates the private codec before passing the complete stored
/// bytes. Never reconstruct these bytes from public equality or JSON.
pub(super) fn private_history_mutation(
    physical_key: &str,
    bytes: &[u8],
) -> Result<ContentIdentityMutation> {
    validate_history_key(physical_key)?;
    let owned_bytes = ORDERED_HISTORY_MAGIC
        .len()
        .checked_add(1)
        .and_then(|n| n.checked_add(physical_key.len()))
        .and_then(|n| n.checked_add(bytes.len()))
        .ok_or_else(|| Error::StoreCorrupted {
            path: "change_log".into(),
            reason: "private history commitment length overflow".into(),
        })?;
    crate::read_image_memory::reserve(owned_bytes)?;
    Ok(ContentIdentityMutation::put(
        LogicalFamily::ChangeAndDdl,
        history_logical_key(HistoryKind::ChangeLog, physical_key.as_bytes()),
        bytes,
    ))
}

pub(super) fn private_history_value_hash(bytes: &[u8]) -> [u8; 32] {
    value_hash(LogicalFamily::ChangeAndDdl, bytes)
}

pub(crate) fn remove_encoded_history_mutation<T: HistoryRecord>(
    physical_key: &str,
    _value: &T,
) -> Result<ContentIdentityMutation> {
    validate_history_key(physical_key)?;
    Ok(ContentIdentityMutation::remove(
        LogicalFamily::ChangeAndDdl,
        history_logical_key(T::KIND, physical_key.as_bytes()),
    ))
}

/// The committed value hash of one stored change or schema record, as the
/// ordered history walk compares it.
pub(crate) fn history_record_value_hash<T: HistoryRecord>(value: &T) -> Result<[u8; 32]> {
    Ok(value_hash(
        LogicalFamily::ChangeAndDdl,
        &value.commitment_bytes()?,
    ))
}

/// The committed value hash of one commit-index entry.
pub(crate) fn commit_index_value_hash(tx: TxId) -> [u8; 32] {
    value_hash(LogicalFamily::ChangeAndDdl, &tx.0.to_le_bytes())
}

pub(crate) fn commit_index_mutation(lsn: Lsn, tx: TxId) -> ContentIdentityMutation {
    ContentIdentityMutation::put(
        LogicalFamily::ChangeAndDdl,
        history_logical_key(HistoryKind::CommitIndex, &lsn.0.to_be_bytes()),
        tx.0.to_le_bytes(),
    )
}

pub(crate) fn remove_commit_index_mutation(lsn: Lsn) -> ContentIdentityMutation {
    ContentIdentityMutation::remove(
        LogicalFamily::ChangeAndDdl,
        history_logical_key(HistoryKind::CommitIndex, &lsn.0.to_be_bytes()),
    )
}

pub(crate) fn status_mutation(
    namespace: &[u8],
    key: &[u8],
    value: &[u8],
) -> ContentIdentityMutation {
    ContentIdentityMutation::put(LogicalFamily::Status, key_parts(&[namespace, key]), value)
}

pub(crate) fn encoded_status_mutation<T: Serialize>(
    namespace: &[u8],
    key: &[u8],
    value: &T,
) -> Result<ContentIdentityMutation> {
    Ok(status_mutation(namespace, key, &canonical_json(value)?))
}

pub(crate) fn remove_status_mutation(namespace: &[u8], key: &[u8]) -> ContentIdentityMutation {
    ContentIdentityMutation::remove(LogicalFamily::Status, key_parts(&[namespace, key]))
}

fn hash_parts(domain: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(domain);
    for part in parts {
        hash.update(&(part.len() as u64).to_le_bytes());
        hash.update(part);
    }
    *hash.finalize().as_bytes()
}

// Private readers preserve the existing framing while checkpointing actual input.
fn hash_parts_checkpoint(domain: &[u8], parts: &[&[u8]], checkpoint: &mut dyn FnMut() -> Result<()>) -> Result<[u8; 32]> {
    let mut hash = blake3::Hasher::new();
    checkpoint()?;
    hash.update(domain);
    for part in parts {
        hash.update(&(part.len() as u64).to_le_bytes());
        for window in part.chunks(64 * 1024) { checkpoint()?; hash.update(window); }
    }
    Ok(*hash.finalize().as_bytes())
}
fn key_hash_checkpoint(key: &[u8], checkpoint: &mut dyn FnMut() -> Result<()>) -> Result<[u8; 32]> {
    if key.first() == Some(&KEY_REFERENCE_TAG) && key.len() == KEY_REFERENCE_BYTES {
        checkpoint()?;
        Ok(key[5..].try_into().expect("key reference hash"))
    } else { hash_parts_checkpoint(b"contextdb-prolly-key", &[key], checkpoint) }
}
fn bytes_cmp_checkpoint(left: &[u8], right: &[u8], checkpoint: &mut dyn FnMut() -> Result<()>) -> Result<Ordering> {
    for (left, right) in left.chunks(64 * 1024).zip(right.chunks(64 * 1024)) {
        checkpoint()?;
        let ordering = left.cmp(right);
        if ordering != Ordering::Equal { return Ok(ordering); }
    }
    Ok(left.len().cmp(&right.len()))
}
fn identity_key_cmp_checkpoint(left: &[u8], right: &[u8], checkpoint: &mut dyn FnMut() -> Result<()>) -> Result<Ordering> {
    match (left.first(), right.first()) {
        (Some(&HASHED_KEY_TAG), Some(&HASHED_KEY_TAG)) |
        (Some(&ORDERED_HISTORY_KEY_TAG), Some(&ORDERED_HISTORY_KEY_TAG)) => bytes_cmp_checkpoint(left, right, checkpoint),
        _ => { checkpoint()?; identity_key_cmp(left, right) }
    }
}
pub(super) fn checkpoint_operation<E: From<Error>, T>(checkpoint: &mut dyn FnMut() -> std::result::Result<(), E>, operation: impl FnOnce(&mut dyn FnMut() -> Result<()>) -> Result<T>) -> std::result::Result<T, E> {
    let mut failure = None;
    let result = operation(&mut || {
        if let Err(error) = checkpoint() {
            failure = Some(error);
            Err(Error::Other("reader checkpoint refused".into()))
        } else { Ok(()) }
    });
    if let Some(error) = failure { return Err(error); }
    result.map_err(E::from)
}
fn chunk_id_checkpoint(level: u32, last: &[u8], checkpoint: &mut dyn FnMut() -> Result<()>) -> Result<[u8; 32]> {
    let key_hash = key_hash_checkpoint(last, checkpoint)?;
    let mut id = [0; 32];
    id[0] = level.min(MAX_CHUNK_LEVEL) as u8;
    id[1..].copy_from_slice(&hash_parts_checkpoint(b"contextdb-prolly-chunk-id", &[&level.to_be_bytes(), &key_hash], checkpoint)?[..31]);
    Ok(id)
}
fn resolve_key_checkpoint<'a>(table: &impl ReadableTable<&'static [u8], &'static [u8]>, family: LogicalFamily, key: &'a [u8], checkpoint: &mut dyn FnMut() -> Result<()>) -> Result<std::borrow::Cow<'a, [u8]>> {
    checkpoint()?;
    if key.first() != Some(&KEY_REFERENCE_TAG) { return Ok(key.into()); }
    if key.len() != KEY_REFERENCE_BYTES { return Err(Error::Other("invalid authenticated key reference".into())); }
    let len = u32::from_be_bytes(key[1..5].try_into().expect("length")) as usize;
    if len <= INLINE_SEPARATOR_BYTES { return Err(Error::Other("invalid authenticated wide key length".into())); }
    let id = key_hash_checkpoint(key, checkpoint)?;
    let stored_key = node_storage_key(family, &id);
    let blob = table.get(stored_key.as_slice()).map_err(RedbPersistence::storage_error)?
        .ok_or_else(|| Error::Other("authenticated separator key is missing".into()))?;
    let bytes = blob.value();
    #[cfg(feature = "test-seams")] observe_node_read(bytes.len());
    if bytes.len() != len.checked_add(1).ok_or_else(|| Error::Other("wide key length overflow".into()))? || bytes[0] != 0 || key_hash_checkpoint(&bytes[1..], checkpoint)? != id {
        return Err(Error::Other("authenticated separator key is damaged".into()));
    }
    #[cfg(feature = "test-seams")] observe_resolved_key_event("copy", len);
    let mut resolved = Vec::with_capacity(len);
    for window in bytes[1..].chunks(64 * 1024) { checkpoint()?; resolved.extend_from_slice(window); }
    Ok(resolved.into())
}
fn resolve_key_charged_checkpoint<'a>(table: &impl ReadableTable<&'static [u8], &'static [u8]>, family: LogicalFamily, key: &'a [u8], admission: &mut ProofAdmission, checkpoint: &mut dyn FnMut() -> Result<()>) -> Result<(std::borrow::Cow<'a, [u8]>, usize)> {
    if key.first() != Some(&KEY_REFERENCE_TAG) { checkpoint()?; return Ok((key.into(), 0)); }
    if key.len() != KEY_REFERENCE_BYTES { return Err(Error::Other("invalid authenticated key reference".into())); }
    let charge = u32::from_be_bytes(key[1..5].try_into().expect("length")) as usize;
    admission.admit(charge)?;
    match resolve_key_checkpoint(table, family, key, checkpoint) {
        Ok(key) => Ok((key, charge)),
        Err(error) => { admission.release(charge); Err(error) }
    }
}
fn record_key(family: LogicalFamily, logical_key: &[u8]) -> [u8; 32] {
    hash_parts(
        b"contextdb-logical-record-key-v1",
        &[&[family as u8], logical_key],
    )
}

fn value_hash(family: LogicalFamily, logical_value: &[u8]) -> [u8; 32] {
    hash_parts(
        b"contextdb-logical-record-value-v1",
        &[&[family as u8], logical_value],
    )
}

#[cfg(feature = "test-seams")]
fn observe_read_authentication_hash(mutation: &ContentIdentityMutation, bytes: usize) {
    match mutation.family {
        LogicalFamily::Relational => crate::read_work_probe::note_hashed_row(),
        LogicalFamily::F32Vectors | LogicalFamily::Sq8Vectors | LogicalFamily::Sq4Vectors => {
            crate::read_work_probe::note_hashed_vector();
        }
        _ => {}
    }
    crate::read_work_probe::note_hashed_bytes(bytes);
}

#[derive(Clone, PartialEq, Eq)]
struct PageEntry {
    key: Vec<u8>,
    value_hash: [u8; 32],
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct Node {
    level: u32,
    entries: Vec<PageEntry>,
    subtree_hash: [u8; 32],
}

#[derive(Clone, PartialEq, Eq)]
struct FamilyRoot {
    root: Option<[u8; 32]>,
    hash: [u8; 32],
    // Only Configuration carries this committed projection. Its tree hash
    // remains the public image identity; this field binds the compact startup
    // values to the same canonical root record without opening unrelated
    // Configuration proof pages.
    open_config_root: Option<[u8; 32]>,
}

type EntryEdits = Vec<(Vec<u8>, Option<[u8; 32]>)>;

fn take_u32(bytes: &[u8], cursor: &mut usize, label: &str) -> Result<usize> {
    let end = cursor.saturating_add(4);
    let raw = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Other(format!("logical identity {label} is truncated")))?;
    *cursor = end;
    Ok(u32::from_be_bytes(raw.try_into().expect("checked u32")) as usize)
}

fn take_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
    label: &str,
) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| Error::Other(format!("logical identity {label} length overflow")))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::Other(format!("logical identity {label} is truncated")))?;
    *cursor = end;
    Ok(value)
}

fn posting_tree_key(table: &str, index: &str, bound: u8, order_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(10 + table.len() + index.len() + order_key.len());
    key.push(ORDERED_POSTING_KEY_TAG);
    key.extend_from_slice(&(table.len() as u32).to_be_bytes());
    key.extend_from_slice(table.as_bytes());
    key.extend_from_slice(&(index.len() as u32).to_be_bytes());
    key.extend_from_slice(index.as_bytes());
    key.push(bound);
    key.extend_from_slice(order_key);
    key
}

type PostingTreeKeyParts<'a> = (&'a [u8], &'a [u8], u8, &'a [u8]);

fn parse_posting_tree_key(key: &[u8]) -> Result<PostingTreeKeyParts<'_>> {
    if key.first().copied() != Some(ORDERED_POSTING_KEY_TAG) {
        return Err(Error::Other(
            "logical identity ordered key has an invalid tag".to_owned(),
        ));
    }
    let mut cursor = 1;
    let table_len = take_u32(key, &mut cursor, "posting table length")?;
    let table = take_bytes(key, &mut cursor, table_len, "posting table")?;
    let index_len = take_u32(key, &mut cursor, "posting index length")?;
    let index = take_bytes(key, &mut cursor, index_len, "posting index")?;
    let bound = *key
        .get(cursor)
        .ok_or_else(|| Error::Other("logical identity posting bound is absent".to_owned()))?;
    cursor += 1;
    if !matches!(
        bound,
        POSTING_LOWER_BOUND_TAG | POSTING_ACTUAL_TAG | POSTING_UPPER_BOUND_TAG
    ) {
        return Err(Error::Other(
            "logical identity posting bound is invalid".to_owned(),
        ));
    }
    Ok((table, index, bound, &key[cursor..]))
}

/// Ordering for one decoded index-key component.
///
/// The proof boundary compares bytes read back from disk, so it validates
/// before it orders. `RelationalIndexPosition`'s derived ordering is the SQL
/// ordering for keys this engine itself wrote: it panics on mismatched `Value`
/// variants and returns `Equal` for an Asc/Desc mismatch. A panic is not a
/// diagnosis and a tie silently breaks the strict-order invariant the tree
/// rests on, so stored bytes are refused with the identity corruption error
/// instead.
fn validated_directed_order(left: (bool, Value), right: (bool, Value)) -> Result<Ordering> {
    let (left_descending, left_value) = left;
    let (right_descending, right_value) = right;
    if left_descending != right_descending {
        return Err(Error::Other(
            "logical identity posting keys disagree about their sort direction".into(),
        ));
    }
    for value in [&left_value, &right_value] {
        if !matches!(
            value,
            Value::Null
                | Value::Bool(_)
                | Value::Int64(_)
                | Value::Float64(_)
                | Value::Text(_)
                | Value::Uuid(_)
                | Value::Timestamp(_)
                | Value::TxId(_)
        ) {
            return Err(Error::Other(
                "logical identity posting key holds a value no index may order".into(),
            ));
        }
    }
    if !left_value.is_null()
        && !right_value.is_null()
        && std::mem::discriminant(&left_value) != std::mem::discriminant(&right_value)
    {
        return Err(Error::Other(
            "logical identity posting keys compare unlike value types".into(),
        ));
    }
    Ok(if left_descending {
        contextdb_core::TotalOrdDesc(left_value).cmp(&contextdb_core::TotalOrdDesc(right_value))
    } else {
        contextdb_core::TotalOrdAsc(left_value).cmp(&contextdb_core::TotalOrdAsc(right_value))
    })
}

fn decode_posting_position(bytes: &[u8]) -> Result<super::EncodedRelationalIndexPosition> {
    let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
    RedbPersistence::decode_exact::<super::EncodedRelationalIndexPosition>(bytes).map_err(|error| {
        Error::Other(format!(
            "logical identity ordered posting key is not a decodable index position: {error}"
        ))
    })
}

/// The same order as `RelationalIndexPosition`'s derived `Ord` for every valid
/// key — component-wise, then component count, then row and transaction — with
/// every step fallible.
#[derive(serde::Deserialize)]
enum BorrowedPostingValue<'a> {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(#[serde(borrow)] &'a str),
    Uuid(uuid::Uuid),
    Timestamp(i64),
    Json(#[serde(borrow)] &'a str),
    Vector(DiscardPostingVector),
    TxId(TxId),
}

struct DiscardPostingVector;
impl<'de> serde::Deserialize<'de> for DiscardPostingVector {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = DiscardPostingVector;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a vector of f32 components")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut values: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                while values.next_element::<f32>()?.is_some() {}
                Ok(DiscardPostingVector)
            }
        }
        decoder.deserialize_seq(Visitor)
    }
}

fn posting_decode_error(error: impl std::fmt::Display) -> Error {
    Error::Other(format!(
        "logical identity ordered posting key is not a decodable index position: {error}"
    ))
}

struct BorrowedPostingCursor<'a> {
    bytes: &'a [u8],
    cursor: usize,
    count: usize,
}
impl<'a> BorrowedPostingCursor<'a> {
    fn open(bytes: &'a [u8]) -> Result<Self> {
        let (count, cursor) =
            bincode::serde::decode_from_slice::<usize, _>(bytes, bincode::config::standard())
                .map_err(posting_decode_error)?;
        // Each component has at least a bool and an enum tag. Refuse a
        // hostile vector count without walking beyond the borrowed payload.
        if count > bytes.len() / 2 {
            return Err(posting_decode_error("truncated component vector"));
        }
        Ok(Self {
            bytes,
            cursor,
            count,
        })
    }
    fn next(&mut self) -> Result<(bool, BorrowedPostingValue<'a>)> {
        let (value, read) = bincode::serde::borrow_decode_from_slice::<
            (bool, BorrowedPostingValue<'a>),
            _,
        >(&self.bytes[self.cursor..], bincode::config::standard())
        .map_err(posting_decode_error)?;
        self.cursor += read;
        Ok(value)
    }
    fn tail(&mut self) -> Result<(RowId, TxId)> {
        let (tail, read) = bincode::serde::decode_from_slice::<(RowId, TxId), _>(
            &self.bytes[self.cursor..],
            bincode::config::standard(),
        )
        .map_err(posting_decode_error)?;
        self.cursor += read;
        if self.cursor != self.bytes.len() {
            return Err(posting_decode_error("trailing index-position bytes"));
        }
        Ok(tail)
    }
    fn validate(&mut self) -> Result<(RowId, TxId)> {
        for _ in 0..self.count {
            let (_, value) = self.next()?;
            if let BorrowedPostingValue::Json(json) = value {
                validate_posting_json(json)?;
            }
        }
        self.tail()
    }
}

/// Validate an unsupported stored JSON component without constructing its
/// values. Full validation still precedes comparison, including components
/// after the first unequal one. The recursion ceiling is serde_json's default.
fn validate_posting_json(json: &str) -> Result<()> {
    let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
    struct Parser<'a> {
        bytes: &'a [u8],
        cursor: usize,
    }
    impl Parser<'_> {
        fn fail<T>(&self) -> Result<T> {
            Err(posting_decode_error("invalid JSON value"))
        }
        fn space(&mut self) {
            while self
                .bytes
                .get(self.cursor)
                .is_some_and(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
            {
                self.cursor += 1;
            }
        }
        fn eat(&mut self, byte: u8) -> bool {
            if self.bytes.get(self.cursor) == Some(&byte) {
                self.cursor += 1;
                true
            } else {
                false
            }
        }
        fn hex(&mut self) -> Result<u16> {
            let mut value = 0;
            for _ in 0..4 {
                let Some(byte) = self.bytes.get(self.cursor).copied() else {
                    return self.fail();
                };
                let digit = match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'f' => byte - b'a' + 10,
                    b'A'..=b'F' => byte - b'A' + 10,
                    _ => return self.fail(),
                };
                self.cursor += 1;
                value = value * 16 + digit as u16;
            }
            Ok(value)
        }
        fn string(&mut self) -> Result<()> {
            if !self.eat(b'"') {
                return self.fail();
            }
            loop {
                let Some(byte) = self.bytes.get(self.cursor).copied() else {
                    return self.fail();
                };
                self.cursor += 1;
                match byte {
                    b'"' => return Ok(()),
                    0..=31 => return self.fail(),
                    b'\\' => {
                        let Some(escape) = self.bytes.get(self.cursor).copied() else {
                            return self.fail();
                        };
                        self.cursor += 1;
                        match escape {
                            b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {}
                            b'u' => {
                                let high = self.hex()?;
                                if (0xd800..=0xdbff).contains(&high) {
                                    if !self.eat(b'\\') || !self.eat(b'u') {
                                        return self.fail();
                                    }
                                    if !(0xdc00..=0xdfff).contains(&self.hex()?) {
                                        return self.fail();
                                    }
                                } else if (0xdc00..=0xdfff).contains(&high) {
                                    return self.fail();
                                }
                            }
                            _ => return self.fail(),
                        }
                    }
                    _ => {}
                }
            }
        }
        fn number(&mut self) -> Result<()> {
            let start = self.cursor;
            self.eat(b'-');
            if !self.eat(b'0') {
                if !self
                    .bytes
                    .get(self.cursor)
                    .is_some_and(|byte| matches!(byte, b'1'..=b'9'))
                {
                    return self.fail();
                }
                while self.bytes.get(self.cursor).is_some_and(u8::is_ascii_digit) {
                    self.cursor += 1;
                }
            }
            if self.eat(b'.') {
                let start = self.cursor;
                while self.bytes.get(self.cursor).is_some_and(u8::is_ascii_digit) {
                    self.cursor += 1;
                }
                if self.cursor == start {
                    return self.fail();
                }
            }
            if self.eat(b'e') || self.eat(b'E') {
                if !self.eat(b'+') {
                    self.eat(b'-');
                }
                let start = self.cursor;
                while self.bytes.get(self.cursor).is_some_and(u8::is_ascii_digit) {
                    self.cursor += 1;
                }
                if self.cursor == start {
                    return self.fail();
                }
            }
            let number =
                std::str::from_utf8(&self.bytes[start..self.cursor]).expect("JSON is UTF-8");
            // Use the existing JSON numeric parser, whose overflow and rounding
            // acceptance differ from Rust's f64 parser. Number owns no heap
            // storage with this workspace's pinned default serde_json features.
            let owner = crate::read_image_memory::active_owner()?;
            let bytes = number
                .len()
                .checked_mul(8)
                .and_then(|n| n.checked_add(7200))
                .ok_or_else(|| posting_decode_error("number admission size overflow"))?;
            let credit = contextdb_core::read_memory::ReadCredit::try_new(
                owner,
                bytes,
                "json_decode",
                "validate_posting_number",
                "Raise the operation memory limit to validate this number.",
            )?;
            let parsed =
                serde_json::from_str::<serde_json::Number>(number).map_err(posting_decode_error);
            // Arbitrary precision may retain token text: drop it before refund.
            drop(parsed?);
            drop(credit);
            Ok(())
        }
        fn value(&mut self, remaining: usize) -> Result<()> {
            self.space();
            match self.bytes.get(self.cursor).copied() {
                Some(b'"') => self.string(),
                Some(b'-' | b'0'..=b'9') => self.number(),
                Some(b'n' | b't' | b'f') => {
                    let literal: &[u8] = match self.bytes[self.cursor] {
                        b'n' => b"null",
                        b't' => b"true",
                        _ => b"false",
                    };
                    if !self.bytes[self.cursor..].starts_with(literal) {
                        return self.fail();
                    }
                    self.cursor += literal.len();
                    Ok(())
                }
                Some(open @ (b'[' | b'{')) => {
                    if remaining <= 1 {
                        return self.fail();
                    }
                    self.cursor += 1;
                    self.space();
                    let close = if open == b'[' { b']' } else { b'}' };
                    if self.eat(close) {
                        return Ok(());
                    }
                    loop {
                        if open == b'{' {
                            self.space();
                            self.string()?;
                            self.space();
                            if !self.eat(b':') {
                                return self.fail();
                            }
                        }
                        self.value(remaining - 1)?;
                        self.space();
                        if self.eat(close) {
                            return Ok(());
                        }
                        if !self.eat(b',') {
                            return self.fail();
                        }
                    }
                }
                _ => self.fail(),
            }
        }
    }
    let mut parser = Parser {
        bytes: json.as_bytes(),
        cursor: 0,
    };
    parser.value(128)?;
    parser.space();
    if parser.cursor != parser.bytes.len() {
        return parser.fail();
    }
    Ok(())
}

fn borrowed_directed_order(
    left: (bool, BorrowedPostingValue<'_>),
    right: (bool, BorrowedPostingValue<'_>),
) -> Result<Ordering> {
    use BorrowedPostingValue as V;
    let (left_descending, left) = left;
    let (right_descending, right) = right;
    if left_descending != right_descending {
        return Err(Error::Other(
            "logical identity posting keys disagree about their sort direction".into(),
        ));
    }
    if matches!(left, V::Json(_) | V::Vector(_)) || matches!(right, V::Json(_) | V::Vector(_)) {
        return Err(Error::Other(
            "logical identity posting key holds a value no index may order".into(),
        ));
    }
    let ascending = match (left, right) {
        (V::Null, V::Null) => Ordering::Equal,
        (V::Null, _) => Ordering::Greater,
        (_, V::Null) => Ordering::Less,
        (V::Bool(left), V::Bool(right)) => left.cmp(&right),
        (V::Int64(left), V::Int64(right)) | (V::Timestamp(left), V::Timestamp(right)) => {
            left.cmp(&right)
        }
        (V::Float64(left), V::Float64(right)) => left.total_cmp(&right),
        (V::Text(left), V::Text(right)) => left.cmp(right),
        (V::Uuid(left), V::Uuid(right)) => left.cmp(&right),
        (V::TxId(left), V::TxId(right)) => left.0.cmp(&right.0),
        _ => {
            return Err(Error::Other(
                "logical identity posting keys compare unlike value types".into(),
            ));
        }
    };
    Ok(if left_descending {
        ascending.reverse()
    } else {
        ascending
    })
}

pub(super) fn validated_posting_order(left: &[u8], right: &[u8]) -> Result<Ordering> {
    // Validate each complete payload before ordering, just as the owned
    // decoder did, without retaining component vectors or string copies.
    let left_tail = BorrowedPostingCursor::open(left)?.validate()?;
    let right_tail = BorrowedPostingCursor::open(right)?.validate()?;
    let mut left = BorrowedPostingCursor::open(left)?;
    let mut right = BorrowedPostingCursor::open(right)?;
    for _ in 0..left.count.min(right.count) {
        let ordering = borrowed_directed_order(left.next()?, right.next()?)?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(left
        .count
        .cmp(&right.count)
        .then_with(|| left_tail.0.cmp(&right_tail.0))
        .then_with(|| left_tail.1.0.cmp(&right_tail.1.0)))
}

/// Preflight callers already validated both complete keys against their declaration.
/// Borrow their scalar payloads; wide text comparison checks each 64 KiB window.
pub(super) fn validated_preflight_posting_order(
    left_bytes: &[u8], right_bytes: &[u8], checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<Ordering> {
    fn tail(bytes: &[u8], checkpoint: &mut dyn FnMut() -> Result<()>) -> Result<(RowId, TxId)> {
        let mut cursor = BorrowedPostingCursor::open(bytes)?;
        for _ in 0..cursor.count { checkpoint()?; cursor.next()?; }
        checkpoint()?;
        cursor.tail()
    }
    let left_tail = tail(left_bytes, checkpoint)?;
    let right_tail = tail(right_bytes, checkpoint)?;
    let mut left = BorrowedPostingCursor::open(left_bytes)?;
    let mut right = BorrowedPostingCursor::open(right_bytes)?;
    for _ in 0..left.count.min(right.count) {
        checkpoint()?;
        let left_part = left.next()?;
        let right_part = right.next()?;
        let ordering = match (&left_part, &right_part) {
            ((ld, BorrowedPostingValue::Text(l)), (rd, BorrowedPostingValue::Text(r))) if ld == rd => {
                let result = bytes_cmp_checkpoint(l.as_bytes(), r.as_bytes(), checkpoint)?;
                if *ld { result.reverse() } else { result }
            }
            _ => borrowed_directed_order(left_part, right_part)?,
        };
        if ordering != Ordering::Equal { return Ok(ordering); }
    }
    checkpoint()?;
    Ok(left.count.cmp(&right.count)
        .then_with(|| left_tail.0.cmp(&right_tail.0))
        .then_with(|| left_tail.1.0.cmp(&right_tail.1.0)))
}

/// A stable rank for every `Value` shape a stored key can carry. NULL ranks
/// last, which is exactly where an ascending index puts it, so ranking never
/// moves a valid key.
fn value_rank(value: &Value) -> u8 {
    match value {
        Value::Bool(_) => 0,
        Value::Int64(_) => 1,
        Value::Float64(_) => 2,
        Value::Timestamp(_) => 3,
        Value::TxId(_) => 4,
        Value::Uuid(_) => 5,
        Value::Text(_) => 6,
        Value::Json(_) => 7,
        Value::Vector(_) => 8,
        Value::Null => 9,
    }
}

/// One index-key component, ordered totally and without failing. For the pairs
/// a declaration can produce - same direction, same type, or a NULL beside a
/// value - this is exactly the declared order. For pairs no declaration can
/// produce it still answers, so the order stays total.
fn total_directed_order(left: &(bool, Value), right: &(bool, Value)) -> Ordering {
    let (left_descending, left_value) = left;
    let (right_descending, right_value) = right;
    if left_descending != right_descending {
        return left_descending.cmp(right_descending);
    }
    let ascending = if value_rank(left_value) == value_rank(right_value)
        && !matches!(left_value, Value::Json(_) | Value::Vector(_))
    {
        contextdb_core::TotalOrdAsc(left_value.clone())
            .cmp(&contextdb_core::TotalOrdAsc(right_value.clone()))
    } else {
        value_rank(left_value)
            .cmp(&value_rank(right_value))
            .then_with(|| match (left_value, right_value) {
                (Value::Json(left), Value::Json(right)) => left.to_string().cmp(&right.to_string()),
                (Value::Vector(left), Value::Vector(right)) => {
                    left.len().cmp(&right.len()).then_with(|| {
                        left.iter()
                            .map(|component| component.to_bits())
                            .cmp(right.iter().map(|component| component.to_bits()))
                    })
                }
                _ => Ordering::Equal,
            })
    };
    if *left_descending {
        ascending.reverse()
    } else {
        ascending
    }
}

/// The one order Redb navigates its index with: total and transitive over every
/// byte string, and exactly the declared SQL order for the keys a declaration
/// produces.
///
/// It is a lexicographic composition, which is what makes it transitive: keys
/// that decode come before keys that do not; decoding keys are ordered by their
/// typed components, then component count, row and transaction; anything still
/// equal, and any pair that does not decode, falls back to the raw bytes. A
/// pair-dependent choice between typed and raw order cannot do this — valid
/// `aa` and `z` with a trailing-byte copy of `z` form a cycle under it.
/// A comparator's decoded position lives only for one comparison and is
/// bounded by its own key bytes, so it is a per-item transient: decoded under
/// a fixed allocation limit, never charged to (and left held by) whichever
/// read owner happens to be active while redb compares keys.
fn decode_posting_position_for_compare(bytes: &[u8]) -> Option<super::EncodedRelationalIndexPosition> {
    const COMPARE_DECODE_LIMIT: usize = 1 << 20;
    let (position, consumed) = bincode::serde::decode_from_slice::<super::EncodedRelationalIndexPosition, _>(
        bytes,
        bincode::config::standard().with_limit::<COMPARE_DECODE_LIMIT>(),
    )
    .ok()?;
    (consumed == bytes.len()).then_some(position)
}

pub(super) fn total_posting_order(left: &[u8], right: &[u8]) -> Ordering {
    match (
        decode_posting_position_for_compare(left),
        decode_posting_position_for_compare(right),
    ) {
        (Some(left_position), Some(right_position)) => {
            let mut ordering = Ordering::Equal;
            for (left_part, right_part) in left_position.key.iter().zip(right_position.key.iter()) {
                ordering = total_directed_order(left_part, right_part);
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            let _ = ordering;
            left_position
                .key
                .len()
                .cmp(&right_position.key.len())
                .then_with(|| left_position.row_id.cmp(&right_position.row_id))
                .then_with(|| left_position.created_tx.0.cmp(&right_position.created_tx.0))
                .then_with(|| left.cmp(right))
        }
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => left.cmp(right),
    }
}

fn identity_key_cmp(left: &[u8], right: &[u8]) -> Result<Ordering> {
    match (left.first().copied(), right.first().copied()) {
        (Some(HASHED_KEY_TAG), Some(HASHED_KEY_TAG)) => Ok(left.cmp(right)),
        (Some(ORDERED_POSTING_KEY_TAG), Some(ORDERED_POSTING_KEY_TAG)) => {
            let (left_table, left_index, left_bound, left_order) = parse_posting_tree_key(left)?;
            let (right_table, right_index, right_bound, right_order) =
                parse_posting_tree_key(right)?;
            let namespace = left_table
                .cmp(right_table)
                .then_with(|| left_index.cmp(right_index))
                .then_with(|| left_bound.cmp(&right_bound));
            if namespace != Ordering::Equal {
                return Ok(namespace);
            }
            if left_bound == POSTING_ACTUAL_TAG && right_bound == POSTING_ACTUAL_TAG {
                validated_posting_order(left_order, right_order)
            } else {
                Ok(left_order.cmp(right_order))
            }
        }
        (Some(ORDERED_HISTORY_KEY_TAG), Some(ORDERED_HISTORY_KEY_TAG)) => Ok(left.cmp(right)),
        (Some(left), Some(right))
            if matches!(left, 0 | 1 | ORDERED_HISTORY_KEY_TAG)
                && matches!(right, 0 | 1 | ORDERED_HISTORY_KEY_TAG) =>
        {
            Ok(left.cmp(&right))
        }
        _ => Err(Error::Other(
            "logical identity key has an invalid key-space tag".to_owned(),
        )),
    }
}

fn mutation_tree_key(mutation: &ContentIdentityMutation) -> Result<Vec<u8>> {
    if mutation.family == LogicalFamily::SchemaAndIndexes
        && mutation.logical_key.starts_with(ORDERED_POSTING_MAGIC)
    {
        let raw = &mutation.logical_key[ORDERED_POSTING_MAGIC.len()..];
        let mut key = Vec::with_capacity(1 + raw.len());
        key.push(ORDERED_POSTING_KEY_TAG);
        key.extend_from_slice(raw);
        let (_, _, bound, order_key) = parse_posting_tree_key(&key)?;
        if bound != POSTING_ACTUAL_TAG || order_key.is_empty() {
            return Err(Error::Other(
                "logical identity posting key is not an actual ordered posting".to_owned(),
            ));
        }
        Ok(key)
    } else if mutation.family == LogicalFamily::ChangeAndDdl
        && mutation.logical_key.starts_with(ORDERED_HISTORY_MAGIC)
    {
        let raw = &mutation.logical_key[ORDERED_HISTORY_MAGIC.len()..];
        match raw.split_first() {
            Some((&kind, coordinate))
                if kind <= HistoryKind::RelationalVersion as u8 && !coordinate.is_empty() =>
            {
                if kind == HistoryKind::RelationalVersion as u8 { version_manifest::parse_manifest(coordinate)?; }
                let mut key = Vec::with_capacity(1 + raw.len());
                key.push(ORDERED_HISTORY_KEY_TAG);
                key.extend_from_slice(raw);
                Ok(key)
            }
            _ => Err(Error::Other(
                "logical identity history key has no kind and coordinate".to_owned(),
            )),
        }
    } else {
        let mut key = Vec::with_capacity(33);
        key.push(HASHED_KEY_TAG);
        key.extend_from_slice(&record_key(mutation.family, &mutation.logical_key));
        Ok(key)
    }
}

fn empty_hash(family: LogicalFamily) -> [u8; 32] {
    hash_parts(b"contextdb-canonical-page-empty", &[&[family as u8]])
}

fn empty_root(family: LogicalFamily) -> FamilyRoot {
    FamilyRoot {
        root: None,
        hash: empty_hash(family),
        open_config_root: (family == LogicalFamily::Configuration)
            .then(|| open_config_digest(std::iter::empty())),
    }
}

/// Digests carried in the same family-root record as the tree hash, after the
/// identity bytes. They are not part of `FamilyRoot`: the tree hash stays the
/// public image identity, and an independent rebuild of that identity does not
/// recompute these copies. Writers publish them in the same transaction as the
/// tree root.
struct PublishedRoot {
    history_open: Option<HistoryOpenPublication>,
    identity: FamilyRoot,
    custody_config_root: Option<[u8; 32]>,
    vector_directory_root: Option<[u8; 32]>,
    ddl_selector_roots: Option<[[u8; 32]; 2]>,
    content_size_root: Option<[u8; 32]>,
    vector_pages_root: Option<[u8; 32]>,
}

fn empty_published_root(family: LogicalFamily) -> PublishedRoot {
    PublishedRoot {
        identity: empty_root(family),
        history_open: (family == LogicalFamily::ChangeAndDdl).then(HistoryOpenPublication::empty),
        content_size_root: (family == LogicalFamily::Configuration).then_some([0; 32]),
        vector_pages_root: (family == LogicalFamily::Configuration).then_some([0; 32]),
        ddl_selector_roots: (family == LogicalFamily::ChangeAndDdl).then_some([[0; 32]; 2]),
        custody_config_root: (family == LogicalFamily::Configuration)
            .then(|| SetCommitment::empty(CompactIndexSet::CustodyIndex).root()),
        vector_directory_root: family.carries_vector_directory_projection().then(|| {
            SetCommitment::empty(CompactIndexSet::VectorDirectory).root_with_vector_body_count(0)
        }),
    }
}

/// Domain-separated, ordered and length-framed projection of exactly the
/// startup settings. The count is included after the entries so a missing or
/// repeated entry cannot cancel as it could in an XOR accumulator.
pub(super) fn open_config_digest<'a>(
    entries: impl IntoIterator<Item = (&'a str, &'a [u8])>,
) -> [u8; 32] {
    let mut digest = OrderedProjection::open_config();
    for (key, value) in entries {
        digest.push(key.as_bytes(), value);
    }
    digest.finish()
}

/// Lanes of the order-independent set commitment that binds a compact copied
/// index to its committed family root. Each record contributes a 2048-byte
/// BLAKE3 XOF expansion read as 1024 little-endian 16-bit lanes; the set is
/// the lane-wise sum modulo 2^16 (the lattice hash LtHash16). Adding and
/// removing one record costs one expansion, independent of how many records
/// the set holds, and the sum depends only on the set, never on write order.
/// Finding a different set with the same sum is a short-vector lattice
/// problem, so a record cannot be dropped, altered or added and cancelled out
/// the way an XOR aggregate allows.
const SET_COMMITMENT_LANES: usize = 1024;
const SET_COMMITMENT_STATE_BYTES: usize = 8 + 2 * SET_COMMITMENT_LANES;

/// A compact copied index whose complete record set is bound to a committed
/// family root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CompactIndexSet {
    /// The vector partition membership records: every (key, record) pair,
    /// including each record's deletion and visibility markers.
    VectorDirectory,
    /// The copied custody policy and delivery records.
    CustodyIndex,
}

impl CompactIndexSet {
    fn element_domain(self) -> &'static [u8] {
        match self {
            Self::VectorDirectory => b"contextdb-vector-directory-set-element-v1",
            Self::CustodyIndex => b"contextdb-custody-index-set-element-v1",
        }
    }

    fn root_domain(self) -> &'static [u8] {
        match self {
            Self::VectorDirectory => b"contextdb-vector-directory-set-root-v1",
            Self::CustodyIndex => b"contextdb-custody-index-set-root-v1",
        }
    }

    /// Where the writer keeps the running sum in the set-state table.
    fn state_key(self) -> &'static str {
        match self {
            Self::VectorDirectory => "vector_directory",
            Self::CustodyIndex => "custody_index",
        }
    }
}

/// The running set commitment of one compact copied index. The writer keeps it
/// in the same transaction as the copied records it covers and publishes
/// [`SetCommitment::root`] on the family root; a reader recomputes it from the
/// copy it scans and compares the two.
pub(super) struct SetCommitment {
    set: CompactIndexSet,
    count: u64,
    lanes: Box<[u16; SET_COMMITMENT_LANES]>,
}

impl SetCommitment {
    pub(super) fn empty(set: CompactIndexSet) -> Self {
        Self {
            set,
            count: 0,
            lanes: Box::new([0; SET_COMMITMENT_LANES]),
        }
    }

    fn decode(set: CompactIndexSet, bytes: &[u8]) -> Result<Self> {
        if bytes.len() != SET_COMMITMENT_STATE_BYTES {
            return Err(Error::Other(
                "compact index set commitment has invalid size".to_owned(),
            ));
        }
        let mut commitment = Self::empty(set);
        commitment.count = u64::from_be_bytes(bytes[..8].try_into().expect("checked count"));
        for (lane, pair) in commitment.lanes.iter_mut().zip(bytes[8..].chunks_exact(2)) {
            *lane = u16::from_le_bytes([pair[0], pair[1]]);
        }
        Ok(commitment)
    }

    fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(SET_COMMITMENT_STATE_BYTES);
        encoded.extend_from_slice(&self.count.to_be_bytes());
        for lane in self.lanes.iter() {
            encoded.extend_from_slice(&lane.to_le_bytes());
        }
        encoded
    }

    /// One record's lanes. `witness` is the BLAKE3 digest of the record's key
    /// followed by its value, which for a fixed key binds the complete value;
    /// the key is length-framed here so no two (key, witness) pairs share an
    /// input.
    fn element(&self, key: &[u8], witness: &[u8; 32]) -> [u16; SET_COMMITMENT_LANES] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.set.element_domain());
        hasher.update(&(key.len() as u64).to_be_bytes());
        hasher.update(key);
        hasher.update(witness);
        let mut expanded = [0_u8; 2 * SET_COMMITMENT_LANES];
        hasher.finalize_xof().fill(&mut expanded);
        let mut lanes = [0_u16; SET_COMMITMENT_LANES];
        for (lane, pair) in lanes.iter_mut().zip(expanded.chunks_exact(2)) {
            *lane = u16::from_le_bytes([pair[0], pair[1]]);
        }
        lanes
    }

    pub(super) fn insert(&mut self, key: &[u8], witness: &[u8; 32]) -> Result<()> {
        let element = self.element(key, witness);
        for (lane, add) in self.lanes.iter_mut().zip(element) {
            *lane = lane.wrapping_add(add);
        }
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| Error::Other("compact index set count overflow".to_owned()))?;
        Ok(())
    }

    pub(super) fn remove(&mut self, key: &[u8], witness: &[u8; 32]) -> Result<()> {
        let element = self.element(key, witness);
        for (lane, sub) in self.lanes.iter_mut().zip(element) {
            *lane = lane.wrapping_sub(sub);
        }
        self.count = self
            .count
            .checked_sub(1)
            .ok_or_else(|| Error::Other("compact index set count underflow".to_owned()))?;
        Ok(())
    }

    /// The 32-byte commitment published on the family root: the set's domain,
    /// its record count and every lane.
    pub(super) fn root(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.set.root_domain());
        hasher.update(&self.encode());
        *hasher.finalize().as_bytes()
    }

    /// Bind producer-maintained distinct body coverage to the exact placement
    /// set. This is private open metadata; the canonical image identity stays
    /// unchanged. Physical root relocation does not change this relation.
    pub(super) fn root_with_vector_body_count(&self, bodies: u64) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"contextdb-vector-directory-body-coverage");
        hasher.update(&self.root());
        hasher.update(&bodies.to_be_bytes());
        *hasher.finalize().as_bytes()
    }

    /// The writer's running commitment for `set`. An absent record is the
    /// empty set: a store that never wrote the copy, or a rebuild that has just
    /// cleared it.
    pub(super) fn load_in_write(
        write_txn: &redb::WriteTransaction,
        set: CompactIndexSet,
    ) -> Result<Self> {
        let table = match write_txn.open_table(super::COMPACT_INDEX_SET_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Self::empty(set)),
            Err(error) => return Err(RedbPersistence::storage_error(error)),
        };
        match table
            .get(set.state_key())
            .map_err(RedbPersistence::storage_error)?
        {
            Some(stored) => Self::decode(set, stored.value()),
            None => Ok(Self::empty(set)),
        }
    }

    pub(super) fn store_in_write(&self, write_txn: &redb::WriteTransaction) -> Result<()> {
        write_txn
            .open_table(super::COMPACT_INDEX_SET_TABLE)
            .map_err(RedbPersistence::storage_error)?
            .insert(self.set.state_key(), self.encode().as_slice())
            .map_err(RedbPersistence::storage_error)?;
        Ok(())
    }

    /// Forget the running commitment before a rebuild re-adds every record.
    pub(super) fn clear_in_write(
        write_txn: &redb::WriteTransaction,
        set: CompactIndexSet,
    ) -> Result<()> {
        match write_txn.open_table(super::COMPACT_INDEX_SET_TABLE) {
            Ok(mut table) => {
                table
                    .remove(set.state_key())
                    .map_err(RedbPersistence::storage_error)?;
                Ok(())
            }
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(()),
            Err(error) => Err(RedbPersistence::storage_error(error)),
        }
    }
}

pub(super) struct OrderedProjection {
    hasher: blake3::Hasher,
    count: u64,
}

impl OrderedProjection {
    fn with_domain(domain: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(domain);
        Self { hasher, count: 0 }
    }

    pub(super) fn open_config() -> Self {
        Self::with_domain(b"contextdb-configuration-open-projection-v1")
    }

    pub(super) fn push(&mut self, key: &[u8], value: &[u8]) {
        self.hasher.update(&(key.len() as u64).to_be_bytes());
        self.hasher.update(key);
        self.hasher.update(&(value.len() as u64).to_be_bytes());
        self.hasher.update(value);
        self.count += 1;
    }

    pub(super) fn finish(mut self) -> [u8; 32] {
        self.hasher.update(&self.count.to_be_bytes());
        *self.hasher.finalize().as_bytes()
    }
}

struct OpenConfigDigest {
    inner: OrderedProjection,
}

impl OpenConfigDigest {
    fn new() -> Self {
        Self {
            inner: OrderedProjection::open_config(),
        }
    }

    fn push(&mut self, key: &str, value: &[u8]) {
        self.inner.push(key.as_bytes(), value);
    }

    fn finish(self) -> [u8; 32] {
        self.inner.finish()
    }
}

fn node_id(key: &[u8]) -> [u8; 32] {
    hash_parts(b"contextdb-canonical-page-id", &[key])
}

// Dolt's key-only, size-aware chunker: the conditional CDF probability is
// (CDF(end) - CDF(start)) / (1 - CDF(start)). An integer uniform CDF on
// [target/2, target*3/2] avoids platform floating-point differences. Keys,
// level and actual encoded bytes determine every boundary, never payloads.
const NODE_HEADER_BYTES: usize = 1 + 4 + 4 + 32;

// Wide separators are stored by authenticated key reference; a blob holds the
// key once and is shared by every chunk that names it. Readers resolve only the
// key being compared, not every key in a node. A leaf keeps the actual ordered
// key while it fits its chunk, so ordinary index keys are compared without a
// second read; a key too large for any chunk is stored by the same reference,
// which is what keeps a valid large index key committable at all.
const INLINE_SEPARATOR_BYTES: usize = 512;
const KEY_REFERENCE_TAG: u8 = 2;
const KEY_REFERENCE_BYTES: usize = 37;
// The largest key a leaf entry can hold inline: one chunk, minus its header,
// minus this entry's length prefix and value hash.
const MAX_INLINE_LEAF_KEY_BYTES: usize = MAX_NODE_BYTES - NODE_HEADER_BYTES - 36;

fn key_hash(key: &[u8]) -> [u8; 32] {
    if key.first() == Some(&KEY_REFERENCE_TAG) && key.len() == KEY_REFERENCE_BYTES {
        key[5..].try_into().expect("key reference hash")
    } else {
        hash_parts(b"contextdb-prolly-key", &[key])
    }
}

fn encoded_key_len(level: u32, key: &[u8]) -> usize {
    let maximum = if level == 0 {
        MAX_INLINE_LEAF_KEY_BYTES
    } else {
        INLINE_SEPARATOR_BYTES
    };
    if key.len() <= maximum {
        key.len()
    } else {
        KEY_REFERENCE_BYTES
    }
}

/// Encode a separator into caller-owned bounded storage. Authentication uses
/// this stack buffer even for malformed oversized internal separators.
fn encoded_key_in<'a>(
    level: u32,
    key: &'a [u8],
    reference: &'a mut [u8; KEY_REFERENCE_BYTES],
) -> &'a [u8] {
    if encoded_key_len(level, key) == key.len() {
        return key;
    }
    reference[0] = KEY_REFERENCE_TAG;
    reference[1..5].copy_from_slice(&(key.len() as u32).to_be_bytes());
    reference[5..].copy_from_slice(&key_hash(key));
    reference
}

fn encoded_key(level: u32, key: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    if encoded_key_len(level, key) == key.len() {
        return key.into();
    }
    let mut reference = [0; KEY_REFERENCE_BYTES];
    encoded_key_in(level, key, &mut reference).to_vec().into()
}

fn resolve_key<'a>(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    family: LogicalFamily,
    key: &'a [u8],
) -> Result<std::borrow::Cow<'a, [u8]>> {
    if key.first() != Some(&KEY_REFERENCE_TAG) {
        return Ok(key.into());
    }
    if key.len() != KEY_REFERENCE_BYTES {
        return Err(Error::Other("invalid authenticated key reference".into()));
    }
    // A reference is never made for a key that fits a separator inline. There
    // is deliberately no upper bound here: the stored blob is authenticated by
    // its key hash and charged as live bytes, so a key the user may legitimately
    // index is never refused by a size limit invented at this boundary.
    let len = u32::from_be_bytes(key[1..5].try_into().expect("length")) as usize;
    if len <= INLINE_SEPARATOR_BYTES {
        return Err(Error::Other("invalid authenticated wide key length".into()));
    }
    let id = key_hash(key);
    let stored_key = node_storage_key(family, &id);
    let blob = table
        .get(stored_key.as_slice())
        .map_err(RedbPersistence::storage_error)?
        .ok_or_else(|| Error::Other("authenticated separator key is missing".into()))?;
    let bytes = blob.value();
    #[cfg(feature = "test-seams")]
    observe_node_read(bytes.len());
    if bytes.len() != len + 1 || bytes[0] != 0 || key_hash(&bytes[1..]) != id {
        return Err(Error::Other(
            "authenticated separator key is damaged".into(),
        ));
    }
    #[cfg(feature = "test-seams")]
    observe_resolved_key_event("copy", len);
    Ok(bytes[1..].to_vec().into())
}

/// The referenced key, with the copy it makes charged before it is made. The
/// reference states the length, so the charge is exact and is taken before
/// the store is even asked for the blob.
fn resolve_key_charged<'a>(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    family: LogicalFamily,
    key: &'a [u8],
    admission: &mut ProofAdmission,
) -> Result<(std::borrow::Cow<'a, [u8]>, usize)> {
    if key.first() != Some(&KEY_REFERENCE_TAG) {
        return Ok((key.into(), 0));
    }
    if key.len() != KEY_REFERENCE_BYTES {
        return Err(Error::Other("invalid authenticated key reference".into()));
    }
    let charge = u32::from_be_bytes(key[1..5].try_into().expect("length")) as usize;
    admission.admit(charge)?;
    match resolve_key(table, family, key) {
        Ok(resolved) => Ok((resolved, charge)),
        Err(error) => {
            admission.release(charge);
            Err(error)
        }
    }
}

/// Bytes a resolved key actually holds. An inline key is borrowed from the
/// chunk that is already charged; only a resolved wide key is new memory.
/// Bytes a referenced key will occupy once copied. Inline keys are borrowed
/// from the already-charged chunk and cost nothing extra. The length is read
/// from the reference so the charge can precede the copy.
fn referenced_key_live_bytes(key: &[u8]) -> Result<usize> {
    if key.first() != Some(&KEY_REFERENCE_TAG) {
        return Ok(0);
    }
    if key.len() != KEY_REFERENCE_BYTES {
        return Err(Error::Other("invalid authenticated key reference".into()));
    }
    let len = u32::from_be_bytes(key[1..5].try_into().expect("length")) as usize;
    if len <= INLINE_SEPARATOR_BYTES {
        return Err(Error::Other("invalid authenticated wide key length".into()));
    }
    Ok(len
        .saturating_add(std::mem::size_of::<Vec<u8>>())
        .saturating_add(4096))
}

fn is_key_reference(entry: &PageEntry) -> bool {
    entry.key.first() == Some(&KEY_REFERENCE_TAG)
}

/// Where a chunk is STORED, which is not what authenticates it. A parent entry
/// carries its child's subtree hash and `decode_node` refuses a body that does
/// not hash to it, so an id only has to name a chunk uniquely — it is a
/// locator, not a commitment.
///
/// Naming it by level first puts every chunk of one level of one family in one
/// contiguous stretch of the node table. A single edit rewrites one chunk per
/// level on its way to the root, and the upper levels hold a small fraction of
/// the store's chunks, so those rewrites land beside each other and share the
/// pages the storage engine has to copy instead of each landing at a random
/// point of a table that holds one entry per chunk in the whole store.
fn chunk_id(level: u32, last: &[u8]) -> [u8; 32] {
    let mut id = [0; 32];
    // Levels above `MAX_CHUNK_LEVEL` are refused when a chunk is decoded, so
    // every level a stored chunk can carry has its own leading byte.
    id[0] = level.min(MAX_CHUNK_LEVEL) as u8;
    id[1..].copy_from_slice(
        &hash_parts(
            b"contextdb-prolly-chunk-id",
            &[&level.to_be_bytes(), &key_hash(last)],
        )[..31],
    );
    id
}

fn entry_bytes(level: u32, entry: &PageEntry) -> usize {
    4 + encoded_key(level, &entry.key).len() + 32
}

fn chunk_cut(level: u32, key: &[u8], start: usize, end: usize, count: usize) -> bool {
    // Internal chunks must make progress even when a separator exceeds target.
    if level > 0 && count < 2 {
        return false;
    }
    let low = TARGET_NODE_ENTRY_BYTES / 2;
    let high = TARGET_NODE_ENTRY_BYTES * 3 / 2;
    if end >= high {
        return true;
    }
    if end <= low {
        return false;
    }
    let start = start.max(low);
    let sample = u64::from_le_bytes(
        hash_parts(
            b"contextdb-prolly-boundary",
            &[&level.to_be_bytes(), &key_hash(key)],
        )[..8]
            .try_into()
            .expect("hash sample"),
    );
    (sample as u128) * ((high - start) as u128) < ((end - start) as u128) * (u64::MAX as u128)
}

fn node_storage_key(family: LogicalFamily, id: &[u8; 32]) -> [u8; 33] {
    let mut stored = [0; 33];
    stored[0] = family as u8;
    stored[1..].copy_from_slice(id);
    stored
}

fn node_hash(family: LogicalFamily, level: u32, entries: &[PageEntry]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"contextdb-prolly-chunk");
    hash.update(&[family as u8]);
    hash.update(&level.to_be_bytes());
    for entry in entries {
        let key = encoded_key(level, &entry.key);
        hash.update(&(key.len() as u64).to_be_bytes());
        hash.update(&key);
        hash.update(&entry.value_hash);
    }
    *hash.finalize().as_bytes()
}

fn encode_node(node: &Node) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(
        NODE_HEADER_BYTES
            + node
                .entries
                .iter()
                .map(|e| entry_bytes(node.level, e))
                .sum::<usize>(),
    );
    encoded.push(NODE_VERSION);
    encoded.extend_from_slice(&node.level.to_be_bytes());
    encoded.extend_from_slice(&(node.entries.len() as u32).to_be_bytes());
    for entry in &node.entries {
        let key = encoded_key(node.level, &entry.key);
        encoded.extend_from_slice(&(key.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&key);
        encoded.extend_from_slice(&entry.value_hash);
    }
    encoded.extend_from_slice(&node.subtree_hash);
    encoded
}

/// An authenticated borrowed chunk. Readers and writer scratch use the same
/// parser, hash and canonical-boundary checks; decoding only changes ownership.
struct EncodedNode<'a> {
    bytes: &'a [u8],
    level: u32,
    count: usize,
    subtree_hash: [u8; 32],
}

#[derive(Clone, Copy)]
struct EncodedEntry<'a> {
    key: &'a [u8],
    value_hash: [u8; 32],
}

impl<'a> EncodedNode<'a> {
    fn entry_at(&self, cursor: &mut usize) -> Result<EncodedEntry<'a>> {
        let len = take_u32(self.bytes, cursor, "key length")?;
        let key = take_bytes(self.bytes, cursor, len, "key")?;
        let value_hash = take_bytes(self.bytes, cursor, 32, "value hash")?
            .try_into()
            .expect("hash");
        if key.first() == Some(&KEY_REFERENCE_TAG) && key.len() != KEY_REFERENCE_BYTES {
            return Err(Error::Other("invalid authenticated key reference".into()));
        }
        Ok(EncodedEntry { key, value_hash })
    }

    fn authenticate(family: LogicalFamily, bytes: &'a [u8]) -> Result<Self> {
        if bytes.len() > MAX_NODE_BYTES || bytes.first().copied() != Some(NODE_VERSION) {
            return Err(Error::Other(
                "logical identity chunk size or representation is invalid".into(),
            ));
        }
        let mut cursor = 1;
        let level = take_u32(bytes, &mut cursor, "level")? as u32;
        let count = take_u32(bytes, &mut cursor, "entry count")?;
        if level > MAX_CHUNK_LEVEL || count == 0 || count > bytes.len() / 36 {
            return Err(Error::Other(
                "logical identity chunk level or count is invalid".into(),
            ));
        }
        let mut node = Self {
            bytes,
            level,
            count,
            subtree_hash: [0; 32],
        };
        let mut hash = blake3::Hasher::new();
        hash.update(b"contextdb-prolly-chunk");
        hash.update(&[family as u8]);
        hash.update(&level.to_be_bytes());
        for _ in 0..count {
            let entry = node.entry_at(&mut cursor)?;
            let mut reference = [0; KEY_REFERENCE_BYTES];
            let key = encoded_key_in(level, entry.key, &mut reference);
            hash.update(&(key.len() as u64).to_be_bytes());
            hash.update(key);
            hash.update(&entry.value_hash);
        }
        node.subtree_hash = take_bytes(bytes, &mut cursor, 32, "chunk hash")?
            .try_into()
            .expect("hash");
        #[cfg(feature = "test-seams")]
        crate::read_work_probe::note_proof_node_hashed();
        if cursor != bytes.len() || *hash.finalize().as_bytes() != node.subtree_hash {
            return Err(Error::Other(
                "logical identity chunk does not authenticate its contents".into(),
            ));
        }
        // Authenticate before a typed comparator interprets stored keys.
        let mut cursor = 9;
        let mut prior: Option<&[u8]> = None;
        let mut size = NODE_HEADER_BYTES;
        for position in 0..count {
            let entry = node.entry_at(&mut cursor)?;
            if let Some(prior) = prior
                && entry.key.first() != Some(&KEY_REFERENCE_TAG)
                && prior.first() != Some(&KEY_REFERENCE_TAG)
                && identity_key_cmp(prior, entry.key)? != Ordering::Less
            {
                return Err(Error::Other(
                    "logical identity chunk keys are not strictly ordered".into(),
                ));
            }
            let end = size + 36 + encoded_key_len(level, entry.key);
            if position + 1 < count && chunk_cut(level, entry.key, size, end, position + 1) {
                return Err(Error::Other(
                    "logical identity chunk crosses a canonical boundary".into(),
                ));
            }
            prior = Some(entry.key);
            size = end;
        }
        Ok(node)
    }
}

fn decode_node(family: LogicalFamily, bytes: &[u8]) -> Result<Node> {
    let encoded = EncodedNode::authenticate(family, bytes)?;
    let mut entries = Vec::with_capacity(encoded.count);
    let mut cursor = 9;
    for _ in 0..encoded.count {
        let entry = encoded.entry_at(&mut cursor)?;
        entries.push(PageEntry {
            key: entry.key.to_vec(),
            value_hash: entry.value_hash,
        });
    }
    Ok(Node {
        level: encoded.level,
        entries,
        subtree_hash: encoded.subtree_hash,
    })
}

fn read_node(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    family: LogicalFamily,
    id: [u8; 32],
) -> Result<Node> {
    read_node_charged(table, family, id, &mut ProofAdmission::unaccounted()).map(|(node, _)| node)
}

/// Decode one stored chunk, charging what the decode will hold before it
/// allocates. Redb hands the stored bytes back as a borrowed page, so only the
/// decoded chunk is this code's to charge.
/// Decode one stored chunk and hand the caller both the chunk and the bytes
/// that were admitted for it. The charge is NOT released here: it follows the
/// decoded chunk, and whoever ends up owning that chunk gives it back when the
/// chunk is dropped.
fn read_node_charged(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    family: LogicalFamily,
    id: [u8; 32],
    admission: &mut ProofAdmission,
) -> Result<(Node, usize)> {
    #[cfg(feature = "test-seams")]
    let read_started = std::time::Instant::now();
    let stored_key = node_storage_key(family, &id);
    let encoded = table
        .get(stored_key.as_slice())
        .map_err(RedbPersistence::storage_error)?
        .ok_or_else(|| Error::Other("logical identity tree references a missing chunk".into()))?;
    #[cfg(feature = "test-seams")]
    crate::read_work_probe::note_proof_node_read();
    #[cfg(feature = "test-seams")]
    observe_node_read(encoded.value().len());
    // The decoded chunk owns a copy of every key it holds plus its entry
    // vector; charge that before `decode_node` allocates any of it. The entry
    // count is in the stored header, which Redb hands back borrowed, so the
    // charge is the exact decoded size rather than a worst case.
    let decoded_bytes = precharged_node_bytes(encoded.value());
    admission.admit(decoded_bytes)?;
    let node = match decode_node(family, encoded.value()) {
        Ok(node) => node,
        Err(error) => {
            admission.release(decoded_bytes);
            return Err(error);
        }
    };
    if chunk_id(node.level, &node.entries.last().expect("chunk").key) != id {
        admission.release(decoded_bytes);
        return Err(Error::Other(
            "logical identity chunk id disagrees with its separator".into(),
        ));
    }
    // The charge above was read from the stored header before anything was
    // allocated, and it rounds up so the allocation can never outrun it. The
    // chunk now exists and says exactly what it occupies, so the transaction
    // keeps that and hands the rounding back: a write is never refused for
    // headroom no allocation ever took.
    let live_bytes = node_live_bytes(&node);
    if live_bytes > decoded_bytes {
        if let Err(error) = admission.admit(live_bytes - decoded_bytes) {
            admission.release(decoded_bytes);
            return Err(error);
        }
    } else {
        admission.release(decoded_bytes - live_bytes);
    }
    #[cfg(feature = "test-seams")]
    observe_identity_span(IdentitySpan::ReadAndDecode, read_started);
    Ok((node, live_bytes))
}

/// What decoding this exact stored chunk will hold: its entry vector and one
/// owned copy of every key in it. The entry count comes from the stored header,
/// which is borrowed from the page and costs nothing to read; a header that
/// does not say is charged the minimum and refused by `decode_node` moments
/// later.
fn precharged_node_bytes(encoded: &[u8]) -> usize {
    let count = encoded
        .get(5..9)
        .and_then(|raw| <[u8; 4]>::try_from(raw).ok())
        .map_or(0, |raw| u32::from_be_bytes(raw) as usize)
        .min(encoded.len() / 36);
    let framed = NODE_HEADER_BYTES.saturating_add(count.saturating_mul(36));
    std::mem::size_of::<Node>()
        .saturating_add(count.saturating_mul(std::mem::size_of::<PageEntry>()))
        .saturating_add(encoded.len().saturating_sub(framed))
        .saturating_add(64)
}

/// The decode charge for a stored chunk, read from the borrowed page before
/// anything is allocated. Missing or unreadable chunks fail here the same
/// way `read_node_charged` would a moment later.
fn stored_node_precharge_bytes(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    family: LogicalFamily,
    id: [u8; 32],
) -> Result<usize> {
    let stored_key = node_storage_key(family, &id);
    let encoded = table
        .get(stored_key.as_slice())
        .map_err(RedbPersistence::storage_error)?
        .ok_or_else(|| Error::Other("logical identity tree references a missing chunk".into()))?;
    Ok(precharged_node_bytes(encoded.value()))
}

/// What holding one decoded chunk actually costs. Wide keys are held by
/// reference, so this is bounded by the chunk's own byte bound.
fn node_live_bytes(node: &Node) -> usize {
    std::mem::size_of::<Node>()
        + node.entries.capacity() * std::mem::size_of::<PageEntry>()
        + node
            .entries
            .iter()
            .map(|entry| entry.key.capacity())
            .sum::<usize>()
}

/// Proof maintenance retains an authenticated cursor path and a height-bounded
/// original cache for one family. One admitted resident buffer builds a chunk;
/// generated parents and physical write operations stream to the admitted,
/// unlinked scratch backend. No generated-record directory grows on the heap.
/// Original page guards and exact descriptor arrays keep their charges through
/// their final borrow, and all original guards drop before physical replay.
///
/// Memory this proof work has had admitted by the database's own accountant.
///
/// Everything the proof maintenance allocates and keeps — the coalesced and
/// grouped mutation maps, the per-level edit lists, decoded chunk bodies, the
/// staged chunks awaiting the flush, the encode buffer and the retired wide-key
/// ids — is charged here *before* it is allocated, and released when it is
/// dropped. Buffers this code does not own are deliberately not charged twice:
/// the caller's mutation slice is the caller's, and Redb owns its own write
/// buffers and its page cache, which the storage layer already accounts for.
///
/// With no accountant attached the charge is still measured, so the same code
/// runs in engine-internal contracts; production always attaches one.
pub(crate) struct ProofAdmission {
    reader_budget: Option<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>>,
    reader_refusal: Option<Arc<Mutex<Option<Error>>>>,
    accountant: Option<crate::memory_accounting::AccountantHandle>,
    /// A cache this admission may reclaim from before it refuses. A proved
    /// chunk is a cache; a read that needs its bytes gets them.
    reclaim: Option<Arc<ResidentProofChunkSet>>,
    charge_fixed_refusal_control: bool,
    scratch_owner: Option<std::sync::Weak<super::writer_file::LockedWriterFile>>,
    scratch_accountant: Option<crate::memory_accounting::AccountantHandle>,
    held: usize,
    peak: std::cell::Cell<usize>,
}

/// A returned writer admission retains its epoch independently of the caller.
/// The payload drops before the use; no extra allocation is introduced.
pub(crate) struct OwnedProofAdmission {
    admission: ProofAdmission,
    _writer_use: Option<crate::memory_accounting::WriterUse>,
}
impl OwnedProofAdmission {
    fn new(admission: ProofAdmission) -> Self {
        let writer_use = admission.accountant.as_ref().or(admission.scratch_accountant.as_ref())
            .and_then(crate::memory_accounting::WriterUse::retain_if_writer);
        Self { admission, _writer_use: writer_use }
    }
}
impl std::ops::Deref for OwnedProofAdmission {
    type Target = ProofAdmission;
    fn deref(&self) -> &ProofAdmission { &self.admission }
}
impl std::ops::DerefMut for OwnedProofAdmission {
    fn deref_mut(&mut self) -> &mut ProofAdmission { &mut self.admission }
}

impl ProofAdmission {
    pub(super) fn take_reader_refusal(&self) -> Option<Error> {
        self.reader_refusal.as_ref().and_then(|slot| slot.lock().unwrap_or_else(|e| e.into_inner()).take())
    }

    pub(super) fn for_reader(memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Self {
        let mut owner = Self::new(None);
        owner.reader_budget = Some(memory);
        owner
    }

    pub(crate) fn new(accountant: Option<crate::memory_accounting::AccountantHandle>) -> Self {
        Self {
            reader_budget: None,
            reader_refusal: None,
            accountant,
            reclaim: None,
            charge_fixed_refusal_control: false,
            scratch_owner: None,
            scratch_accountant: None,
            held: 0,
            peak: std::cell::Cell::new(0),
        }
    }

    pub(super) fn with_scratch_owner(
        mut self,
        owner: Option<std::sync::Weak<super::writer_file::LockedWriterFile>>,
    ) -> Self {
        self.scratch_owner = owner;
        self
    }

    fn open_scratch(&mut self) -> Result<(Box<dyn redb::ScratchBackend>, AdmittedBytes)> {
        // The concrete factory exposes its exact Box payload layout without
        // allocating. Admission therefore precedes both file and Box creation.
        let bytes = redb::scratch_backend_allocation_size();
        let charge = if self.accountant.is_none() && self.scratch_accountant.is_some() {
            let mut admission = Self::new(self.scratch_accountant.clone());
            admission.admit(bytes)?;
            self.peak.set(
                self.peak.get().max(
                    self.held
                        .checked_add(bytes)
                        .ok_or_else(|| Error::Other("proof scratch admission overflow".into()))?,
                ),
            );
            admission.detach(bytes)
        } else {
            self.admit(bytes)?;
            self.detach(bytes)
        };
        let owner = self
            .scratch_owner
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| Error::ScratchWorkingStorageUnavailable {
                operation: "create_content_identity_scratch".into(),
                io_kind: std::io::ErrorKind::Unsupported,
            })?;
        let backend =
            owner
                .scratch_backend()
                .map_err(|error| Error::ScratchWorkingStorageUnavailable {
                    operation: "create_content_identity_scratch".into(),
                    io_kind: error.kind(),
                })?;
        Ok((backend, charge))
    }

    /// The reader's own admission: before it refuses, it gives back the proved
    /// chunks this verifier is holding. Retained proofs can therefore never be
    /// the reason a read is refused.
    fn reclaiming(
        accountant: Option<crate::memory_accounting::AccountantHandle>,
        reclaim: Arc<ResidentProofChunkSet>,
    ) -> Self {
        Self {
            reader_budget: None,
            reader_refusal: None,
            accountant,
            reclaim: Some(reclaim),
            charge_fixed_refusal_control: false,
            scratch_owner: None,
            scratch_accountant: None,
            held: 0,
            peak: std::cell::Cell::new(0),
        }
    }

    /// A fixed refusal control must land even when the refused operation has
    /// exhausted its limit. Charge its proof before allocation; ordinary proof
    /// mutations continue to use admission.
    pub(super) fn fixed_refusal_control(
        accountant: Option<crate::memory_accounting::AccountantHandle>,
    ) -> Self {
        let mut admission = Self::new(accountant);
        admission.charge_fixed_refusal_control = true;
        admission
    }

    /// For engine-internal contracts that hold no database accountant. The
    /// charge is measured but nothing is admitted.
    pub(crate) fn unaccounted() -> Self {
        Self::new(None)
    }

    /// Admit `bytes` before they are allocated. A refusal is the existing typed
    /// budget error and leaves this transaction to abort with nothing written.
    pub(super) fn admit(&mut self, bytes: usize) -> Result<()> {
        if let Some(reader) = &self.reader_budget {
            if let Err(error) = reader.try_reserve(bytes, "content_identity", "read_committed_proof", "Reduce the requested proof work or raise the declared reader memory limit.") {
                if let Some(refusal) = &self.reader_refusal {
                    *refusal.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
                    return Err(Error::Other("read proof admission refused".into()));
                }
                return Err(error);
            }
        } else if let Some(accountant) = &self.accountant {
            if self.charge_fixed_refusal_control {
                accountant.charge_retained(bytes);
            } else {
                loop {
                    match accountant.try_allocate_for(
                        bytes,
                        "content_identity",
                        "maintain_committed_proof",
                        "Raise MEMORY_LIMIT to admit the proof this transaction maintains.",
                    ) {
                        Ok(()) => break,
                        Err(refusal) => {
                            // Evict before refusing. The work asking for these
                            // bytes is a read; what is holding them is a cache of
                            // chunks this reader can prove again.
                            match &self.reclaim {
                                Some(chunks) if chunks.reclaim() => continue,
                                _ => return Err(refusal),
                            }
                        }
                    }
                }
            }
        }
        self.held = self.held.saturating_add(bytes);
        self.peak.set(self.peak.get().max(self.held));
        Ok(())
    }

    pub(super) fn release(&mut self, bytes: usize) {
        let bytes = bytes.min(self.held);
        if let Some(reader) = &self.reader_budget { reader.release(bytes); } else if let Some(accountant) = &self.accountant { accountant.release(bytes); }
        self.held -= bytes;
    }

    /// Hand `bytes` of this admission to something whose own lifetime decides
    /// when they go back. The bytes stay admitted; only their owner changes.
    fn detach(&mut self, bytes: usize) -> AdmittedBytes {
        let bytes = bytes.min(self.held);
        self.held -= bytes;
        AdmittedBytes {
            reader_budget: self.reader_budget.clone(),
            accountant: self.accountant.clone().map(crate::memory_accounting::StoreAccountantHandle::new),
            bytes,
        }
    }

    pub(crate) fn held(&self) -> usize {
        self.held
    }

    /// Whether what this admission admits is charged to a database accountant.
    /// An unaccounted admission measures but admits nothing, so an allocation
    /// it makes must not outlive the call that made it.
    fn is_accounted(&self) -> bool {
        self.accountant.is_some() || self.reader_budget.is_some()
    }

    pub(crate) fn peak(&self) -> usize {
        self.peak.get()
    }
}

/// Admitted bytes owned by one allocation, returned when that allocation is
/// dropped — including when it is dropped by an unwind.
struct AdmittedBytes {
    reader_budget: Option<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>>,
    accountant: Option<crate::memory_accounting::StoreAccountantHandle>,
    bytes: usize,

}

impl AdmittedBytes {
    fn source_custody(&self) -> Option<crate::memory_accounting::StoreAccountantHandle> {
        self.accountant.as_ref().filter(|owner| owner.is_writer()).cloned()
    }
    fn with_source(
        source: Option<crate::memory_accounting::StoreAccountantHandle>,
        mut debit: Option<Self>,
    ) -> Option<Self> {
        if let Some(source) = source {
            if let Some(charge) = debit.as_mut() {
                // These constructor routes establish identical payer/source
                // allocations. Replacing only the handle leaves the debit
                // armed exactly once and never returns its bytes early.
                debug_assert!(charge.accountant.as_ref().is_some_and(|payer| payer.same_allocation(&source)));
                charge.accountant = Some(source);
            } else {
                debit = Some(Self { reader_budget: None, accountant: Some(source), bytes: 0 });
            }
        }
        debit
    }
}

impl Drop for AdmittedBytes {
    fn drop(&mut self) {
        if let Some(reader) = &self.reader_budget { reader.release(self.bytes); } else if let Some(accountant) = &self.accountant { accountant.release(self.bytes); }
    }
}

impl Drop for ProofAdmission {
    fn drop(&mut self) {
        let held = self.held;
        self.release(held);
    }
}

// Existing fixture comparison vocabulary only; production admission never
// uses this predecessor estimate. TEST-B owns adaptation of those assertions.
#[cfg(test)]
fn mutation_entry_bytes(key_len: usize) -> usize {
    key_len + std::mem::size_of::<Vec<u8>>() + std::mem::size_of::<Option<[u8; 32]>>() + 64
}

/// Allocation overflow uses the same typed vocabulary as ordinary admission.
fn proof_allocation_overflow(admission: &ProofAdmission) -> Error {
    let usage = admission.accountant.as_ref().map(|owner| owner.usage());
    Error::MemoryBudgetExceeded {
        subsystem: "content_identity".into(),
        operation: "maintain_committed_proof".into(),
        requested_bytes: usize::MAX,
        available_bytes: usage
            .as_ref()
            .and_then(|usage| usage.available)
            .unwrap_or(0),
        budget_limit_bytes: usage.and_then(|usage| usage.limit).unwrap_or(usize::MAX),
        hint: "Proof allocation capacity overflowed.".into(),
    }
}

/// Compare the encoded bytes without allocating their owned tree keys.
/// Ordered keys borrow the same suffix that mutation_tree_key copies; ordinary
/// keys use its identical record hash, held on the stack for this comparison.
fn mutation_encoded_key_cmp(
    left: &ContentIdentityMutation,
    right: &ContentIdentityMutation,
) -> Ordering {
    fn parts(mutation: &ContentIdentityMutation) -> (u8, Option<&[u8]>, [u8; 32]) {
        if mutation.family == LogicalFamily::SchemaAndIndexes
            && mutation.logical_key.starts_with(ORDERED_POSTING_MAGIC)
        {
            (
                ORDERED_POSTING_KEY_TAG,
                Some(&mutation.logical_key[ORDERED_POSTING_MAGIC.len()..]),
                [0; 32],
            )
        } else if mutation.family == LogicalFamily::ChangeAndDdl
            && mutation.logical_key.starts_with(ORDERED_HISTORY_MAGIC)
        {
            (
                ORDERED_HISTORY_KEY_TAG,
                Some(&mutation.logical_key[ORDERED_HISTORY_MAGIC.len()..]),
                [0; 32],
            )
        } else {
            (
                HASHED_KEY_TAG,
                None,
                record_key(mutation.family, &mutation.logical_key),
            )
        }
    }
    let (left_tag, left_raw, left_hash) = parts(left);
    let (right_tag, right_raw, right_hash) = parts(right);
    left_tag.cmp(&right_tag).then_with(|| {
        left_raw
            .unwrap_or(&left_hash)
            .cmp(right_raw.unwrap_or(&right_hash))
    })
}

/// One exact allocation per unique encoded key, borrowing caller-owned records.
/// AVL balance bounds insertion, traversal and ordinary Box drop recursion by
/// logarithmic depth even for monotonically ordered incoming keys. The AVL
/// minimum-node recurrence bounds height below 128 for a usize-addressable
/// node count on supported targets, so the stored u8 height cannot overflow.
struct MutationKeyNode {
    first: usize,
    last: usize,
    left: Option<Box<MutationKeyNode>>,
    right: Option<Box<MutationKeyNode>>,
    height: u8,
}

impl MutationKeyNode {
    fn height(link: &Option<Box<Self>>) -> u8 {
        link.as_ref().map_or(0, |node| node.height)
    }

    fn refresh(&mut self) {
        self.height = 1 + Self::height(&self.left).max(Self::height(&self.right));
    }

    fn rotate_left(mut node: Box<Self>) -> Box<Self> {
        let mut root = node.right.take().expect("AVL right child");
        node.right = root.left.take();
        node.refresh();
        root.left = Some(node);
        root.refresh();
        root
    }

    fn rotate_right(mut node: Box<Self>) -> Box<Self> {
        let mut root = node.left.take().expect("AVL left child");
        node.left = root.right.take();
        node.refresh();
        root.right = Some(node);
        root.refresh();
        root
    }

    fn rebalance(mut node: Box<Self>) -> Box<Self> {
        node.refresh();
        let balance = i16::from(Self::height(&node.left)) - i16::from(Self::height(&node.right));
        if balance > 1 {
            let left = node.left.as_ref().expect("AVL left child");
            if Self::height(&left.left) < Self::height(&left.right) {
                node.left = node.left.take().map(Self::rotate_left);
            }
            Self::rotate_right(node)
        } else if balance < -1 {
            let right = node.right.as_ref().expect("AVL right child");
            if Self::height(&right.right) < Self::height(&right.left) {
                node.right = node.right.take().map(Self::rotate_right);
            }
            Self::rotate_left(node)
        } else {
            node
        }
    }

    fn insert(
        link: &mut Option<Box<Self>>,
        ordinal: usize,
        mutations: &[ContentIdentityMutation],
        admission: &mut ProofAdmission,
        node_bytes: &mut usize,
    ) -> Result<bool> {
        let Some(node) = link.as_mut() else {
            let bytes = std::alloc::Layout::new::<Self>().size();
            let total = node_bytes
                .checked_add(bytes)
                .ok_or_else(|| proof_allocation_overflow(admission))?;
            admission.admit(bytes)?;
            *link = Some(Box::new(Self {
                first: ordinal,
                last: ordinal,
                left: None,
                right: None,
                height: 1,
            }));
            *node_bytes = total;
            return Ok(false);
        };
        let incoming = &mutations[ordinal];
        let stored = &mutations[node.first];
        let replaced = match incoming
            .family
            .cmp(&stored.family)
            .then_with(|| mutation_encoded_key_cmp(incoming, stored))
        {
            Ordering::Less => {
                Self::insert(&mut node.left, ordinal, mutations, admission, node_bytes)?
            }
            Ordering::Greater => {
                Self::insert(&mut node.right, ordinal, mutations, admission, node_bytes)?
            }
            Ordering::Equal => {
                // The former map retained the first key, including its namespace,
                // while replacing its value. Duplicate updates allocate nothing.
                node.last = ordinal;
                return Ok(true);
            }
        };
        // No fallible work occurs while the owning link is detached. A refused
        // child admission leaves the entire previously balanced tree attached.
        *link = link.take().map(Self::rebalance);
        Ok(replaced)
    }

    fn visit(&self, visitor: &mut impl FnMut(&Self) -> Result<()>) -> Result<()> {
        if let Some(left) = &self.left {
            left.visit(visitor)?;
        }
        visitor(self)?;
        if let Some(right) = &self.right {
            right.visit(visitor)?;
        }
        Ok(())
    }
}

/// The exact payload capacity mutation_tree_key requests for this encoding.
fn mutation_key_capacity(
    mutation: &ContentIdentityMutation,
    admission: &ProofAdmission,
) -> Result<usize> {
    let raw_len = if mutation.family == LogicalFamily::SchemaAndIndexes
        && mutation.logical_key.starts_with(ORDERED_POSTING_MAGIC)
    {
        Some(mutation.logical_key.len() - ORDERED_POSTING_MAGIC.len())
    } else if mutation.family == LogicalFamily::ChangeAndDdl
        && mutation.logical_key.starts_with(ORDERED_HISTORY_MAGIC)
    {
        Some(mutation.logical_key.len() - ORDERED_HISTORY_MAGIC.len())
    } else {
        None
    };
    raw_len.map_or(Ok(1 + std::mem::size_of::<[u8; 32]>()), |len| {
        len.checked_add(1)
            .ok_or_else(|| proof_allocation_overflow(admission))
    })
}

#[path = "content_identity/writer_window.rs"]
mod writer_window;
#[cfg(test)]
use writer_window::update_family;

/// Reader-test ceiling retained for comparisons with the writer's resident
/// original path and bounded builder; no storage is allocated from it.
const fn proof_path_bytes(height: u32) -> usize {
    (height as usize + 3) * (MAX_NODE_BYTES + 4096)
}

/// Compare two keys that may each be stored inline or by reference, resolving
/// at most one key at a time and never both when their hashes already decide
/// the answer.
fn resolving_key_cmp(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    family: LogicalFamily,
    left: &[u8],
    right: &[u8],
    admission: &mut ProofAdmission,
) -> Result<Ordering> {
    let left_reference = left.first() == Some(&KEY_REFERENCE_TAG);
    let right_reference = right.first() == Some(&KEY_REFERENCE_TAG);
    if !left_reference && !right_reference {
        return identity_key_cmp(left, right);
    }
    // Equal keys hash equally, and a key reference carries that hash, so the
    // common case of an edit landing exactly on a separator costs no read.
    if key_hash(left) == key_hash(right) {
        return Ok(Ordering::Equal);
    }
    // Both keys are live at once for this one comparison, so both are charged
    // before either is copied and both are given back before the next.
    let (left, left_charge) = resolve_key_charged(table, family, left, admission)?;
    let (right, right_charge) = match resolve_key_charged(table, family, right, admission) {
        Ok(resolved) => resolved,
        Err(error) => {
            admission.release(left_charge);
            return Err(error);
        }
    };
    let ordering = identity_key_cmp(&left, &right);
    drop(left);
    drop(right);
    admission.release(left_charge);
    admission.release(right_charge);
    ordering
}

/// Binary search over stored entries, resolving only the key being compared.
/// A leaf that holds a key too large to inline is searched without ever
/// materializing more than one key at a time.
fn resolving_entry_search(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    family: LogicalFamily,
    entries: &[PageEntry],
    key: &[u8],
    admission: &mut ProofAdmission,
) -> Result<std::result::Result<usize, usize>> {
    let mut left = 0;
    let mut right = entries.len();
    while left < right {
        let mid = (left + right) / 2;
        match resolving_key_cmp(table, family, &entries[mid].key, key, admission)? {
            Ordering::Less => left = mid + 1,
            Ordering::Greater => right = mid,
            Ordering::Equal => return Ok(Ok(mid)),
        }
    }
    Ok(Err(left))
}

fn entry_search(entries: &[PageEntry], key: &[u8]) -> Result<std::result::Result<usize, usize>> {
    let mut left = 0;
    let mut right = entries.len();
    while left < right {
        let mid = (left + right) / 2;
        match identity_key_cmp(&entries[mid].key, key)? {
            Ordering::Less => left = mid + 1,
            Ordering::Greater => right = mid,
            Ordering::Equal => return Ok(Ok(mid)),
        }
    }
    Ok(Err(left))
}

/// Sort by canonical key order without a panic on a key that cannot be
/// ordered: the first refusal is kept and returned.
fn sort_canonical(edits: &mut EntryEdits) -> Result<()> {
    let mut failure = None;
    edits.sort_by(|left, right| match identity_key_cmp(&left.0, &right.0) {
        Ok(ordering) => ordering,
        Err(error) => {
            failure.get_or_insert(error);
            Ordering::Equal
        }
    });
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Coalescing yields distinct encoded keys in raw-byte order. The raw-byte
/// tiebreak therefore reproduces stable canonical sorting when distinct
/// accepted encodings compare semantically equal, with no auxiliary allocation.
fn sort_canonical_from_coalesced(edits: &mut EntryEdits) -> Result<()> {
    let mut failure = None;
    edits.sort_unstable_by(|left, right| match identity_key_cmp(&left.0, &right.0) {
        Ok(ordering) => ordering.then_with(|| left.0.cmp(&right.0)),
        Err(error) => {
            failure.get_or_insert(error);
            left.0.cmp(&right.0)
        }
    });
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// What one encoded family root costs: the buffer `encode_published_root`
/// reserves. Configuration carries the startup projection and the custody
/// projection; a vector family carries the membership projection.
const ENCODED_ROOT_BYTES: usize = 462;

/// What one proof-reset batch may hold at once: the working set of a family
/// deletion, not a copy of the family. Whichever bound is reached first ends
/// the batch, and a batch always takes at least one key so a single wide key
/// still makes progress (subject to its own admission).
const PROOF_RESET_BATCH_KEYS: usize = 64;
const PROOF_RESET_BATCH_BYTES: usize = 64 * 1024;

#[derive(Clone, PartialEq, Eq)]
struct RootTail {
    history_open: Option<HistoryOpenPublication>,
    custody_config_root: Option<[u8; 32]>,
    vector_directory_root: Option<[u8; 32]>,
    ddl_selector_roots: Option<[[u8; 32]; 2]>,
    content_size_root: Option<[u8; 32]>,
    vector_pages_root: Option<[u8; 32]>,
}

fn assign_open_projections(
    roots: &mut [FamilyRoot; 11],
    tails: &mut [RootTail; 11],
    open_config_root: Option<[u8; 32]>,
    custody_config_root: Option<[u8; 32]>,
    vector_directory_root: Option<[u8; 32]>,
) {
    if let Some(open_config_root) = open_config_root {
        roots[LogicalFamily::Configuration as usize].open_config_root = Some(open_config_root);
    }
    if let Some(custody_config_root) = custody_config_root {
        tails[LogicalFamily::Configuration as usize].custody_config_root =
            Some(custody_config_root);
    }
    if let Some(vector_directory_root) = vector_directory_root {
        for family in [
            LogicalFamily::F32Vectors,
            LogicalFamily::Sq8Vectors,
            LogicalFamily::Sq4Vectors,
        ] {
            tails[family as usize].vector_directory_root = Some(vector_directory_root);
        }
    }
}

fn persist_changed_family_roots(
    write_txn: &redb::WriteTransaction,
    roots: &[FamilyRoot; 11],
    original_roots: &[FamilyRoot; 11],
    tails: &[RootTail; 11],
    original_tails: &[RootTail; 11],
) -> Result<()> {
    let mut roots_table = write_txn
        .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    for family in LogicalFamily::ALL {
        if roots[family as usize] == original_roots[family as usize]
            && tails[family as usize] == original_tails[family as usize]
        {
            continue;
        }
        let encoded = encode_published_root(&PublishedRoot {
            identity: roots[family as usize].clone(),
            history_open: tails[family as usize].history_open,
            custody_config_root: tails[family as usize].custody_config_root,
            vector_directory_root: tails[family as usize].vector_directory_root,
            ddl_selector_roots: tails[family as usize].ddl_selector_roots,
            content_size_root: tails[family as usize].content_size_root,
            vector_pages_root: tails[family as usize].vector_pages_root,
        });
        roots_table
            .insert(family as u8, encoded.as_slice())
            .map_err(RedbPersistence::storage_error)?;
    }
    Ok(())
}

pub(super) fn vector_directory_projection_in_read(
    read_txn: &redb::ReadTransaction,
) -> Result<[u8; 32]> {
    let roots_table = read_txn
        .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let mut expected = None;
    for family in [
        LogicalFamily::F32Vectors,
        LogicalFamily::Sq8Vectors,
        LogicalFamily::Sq4Vectors,
    ] {
        let encoded = roots_table
            .get(family as u8)
            .map_err(RedbPersistence::storage_error)?
            .ok_or_else(|| Error::Other("logical content family root is absent".to_owned()))?;
        let root = decode_published_root(family, encoded.value())?;
        let digest = root.vector_directory_root.ok_or_else(|| {
            Error::Other("vector directory projection is absent from the committed root".to_owned())
        })?;
        match expected {
            None => expected = Some(digest),
            Some(previous) if previous != digest => {
                return Err(Error::Other(
                    "vector directory projection disagrees across committed vector roots"
                        .to_owned(),
                ));
            }
            Some(_) => {}
        }
    }
    expected.ok_or_else(|| Error::Other("vector directory projection is absent".to_owned()))
}

fn encode_root(root: &FamilyRoot) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(ENCODED_ROOT_BYTES);
    encoded.push(ROOT_VERSION);
    match root.root {
        Some(id) => {
            encoded.push(1);
            encoded.extend_from_slice(&id);
        }
        None => encoded.push(0),
    }
    encoded.extend_from_slice(&root.hash);
    if let Some(open_config_root) = root.open_config_root {
        encoded.extend_from_slice(&open_config_root);
    }
    encoded
}

fn encode_published_root(root: &PublishedRoot) -> Vec<u8> {
    let mut encoded = if root.history_open.is_some() {
        let mut encoded = Vec::with_capacity(ENCODED_ROOT_BYTES);
        encoded.push(ROOT_VERSION);
        encoded.push(u8::from(root.identity.root.is_some()));
        encoded.extend_from_slice(&root.identity.root.unwrap_or([0;32]));
        encoded.extend_from_slice(&root.identity.hash);
        encoded
    } else { encode_root(&root.identity) };
    if let Some(custody_config_root) = root.custody_config_root {
        encoded.extend_from_slice(&custody_config_root);
    }
    if let Some(vector_directory_root) = root.vector_directory_root {
        encoded.extend_from_slice(&vector_directory_root);
    }
    if let Some(roots) = root.ddl_selector_roots {
        for fingerprint in roots {
            encoded.extend_from_slice(&fingerprint);
        }
    }
    if let Some(root) = root.content_size_root {
        encoded.extend_from_slice(&root);
    }
    if let Some(root) = root.vector_pages_root { encoded.extend_from_slice(&root); }
    if let Some(history) = root.history_open {
        encoded.extend_from_slice(&history.control.encode());
        encoded.extend_from_slice(&history.control_hash);
        history.commit.encode_into(&mut encoded);
        history.ddl.encode_into(&mut encoded);
    }
    encoded
}

fn take_root_hash(bytes: &[u8], cursor: &mut usize, label: &str) -> Result<[u8; 32]> {
    Ok(take_bytes(bytes, cursor, 32, label)?
        .try_into()
        .expect("checked 32-byte root field"))
}

fn decode_root(family: LogicalFamily, bytes: &[u8]) -> Result<FamilyRoot> {
    Ok(decode_published_root(family, bytes)?.identity)
}

fn decode_published_root(family: LogicalFamily, bytes: &[u8]) -> Result<PublishedRoot> {
    if bytes.first().copied() != Some(ROOT_VERSION) {
        return Err(Error::Other(
            "logical identity root has an unsupported representation".to_owned(),
        ));
    }
    let mut cursor = 1;
    let root = match bytes.get(cursor).copied() {
        Some(0) => {
            cursor += 1;
            if family == LogicalFamily::ChangeAndDdl
                && take_bytes(bytes, &mut cursor, 32, "empty history root id")? != [0; 32] {
                return Err(history_control_damage("nonzero absent history root address"));
            }
            None
        }
        Some(1) => {
            cursor += 1;
            Some(
                take_bytes(bytes, &mut cursor, 32, "root page id")?
                    .try_into()
                    .expect("checked root id"),
            )
        }
        _ => {
            return Err(Error::Other(
                "logical identity root tag is invalid".to_owned(),
            ));
        }
    };
    let hash = take_bytes(bytes, &mut cursor, 32, "root hash")?
        .try_into()
        .expect("checked root hash");
    let open_config_root = (family == LogicalFamily::Configuration)
        .then(|| take_root_hash(bytes, &mut cursor, "open config root"))
        .transpose()?;
    let custody_config_root = (family == LogicalFamily::Configuration)
        .then(|| take_root_hash(bytes, &mut cursor, "custody config root"))
        .transpose()?;
    let vector_directory_root = family
        .carries_vector_directory_projection()
        .then(|| take_root_hash(bytes, &mut cursor, "vector directory root"))
        .transpose()?;
    let ddl_selector_roots = (family == LogicalFamily::ChangeAndDdl)
        .then(|| {
            Ok::<_, Error>([
                take_root_hash(bytes, &mut cursor, "DDL selector root")?,
                take_root_hash(bytes, &mut cursor, "DDL count root")?,
            ])
        })
        .transpose()?;
    let content_size_root = (family == LogicalFamily::Configuration)
        .then(|| take_root_hash(bytes, &mut cursor, "content-size root"))
        .transpose()?;
    let vector_pages_root = (family == LogicalFamily::Configuration)
        .then(|| take_root_hash(bytes, &mut cursor, "vector page root")).transpose()?;
    let history_open = if family == LogicalFamily::ChangeAndDdl {
        let control = HistoryOpenControl::decode(take_bytes(bytes, &mut cursor, 234, "history open control")?)?;
        if control.tree != root || control.tree_hash != hash {
            return Err(history_control_damage("history control differs from canonical root"));
        }
        Some(HistoryOpenPublication {
            control,
            control_hash: take_root_hash(bytes, &mut cursor, "history control digest")?,
            commit: PhysicalHistoryTable::decode(take_bytes(bytes, &mut cursor, 33, "commit descriptor")?)?,
            ddl: PhysicalHistoryTable::decode(take_bytes(bytes, &mut cursor, 33, "DDL descriptor")?)?,
        })
    } else { None };
    if cursor != bytes.len() || (root.is_none() && hash != empty_hash(family)) {
        return Err(Error::Other(
            "logical identity root is malformed".to_owned(),
        ));
    }
    Ok(PublishedRoot {
        history_open,
        identity: FamilyRoot {
            root,
            hash,
            open_config_root,
        },
        custody_config_root,
        vector_directory_root,
        ddl_selector_roots,
        content_size_root,
        vector_pages_root,
    })
}

/// The family whose tree holds one record per declared table. Every published
/// field that covers a table's schema derives from this family's root.
pub(crate) const SCHEMA_RECORD_FAMILY: LogicalFamily = LogicalFamily::Policy;

// Each published field that covers more than one family composes those families'
// roots under its own domain, so no two compositions can produce the same bytes
// and no field is another field's root by accident.
const DIGEST_DOMAIN_SCHEMA_AND_INDEXES: &[u8] = b"contextdb-digest-schema-and-indexes-v1";
const DIGEST_DOMAIN_POLICY: &[u8] = b"contextdb-digest-policy-v1";
const DIGEST_DOMAIN_SYNC: &[u8] = b"contextdb-digest-sync-v1";
const DIGEST_DOMAIN_CONFIGURATION: &[u8] = b"contextdb-digest-configuration-v1";
const DIGEST_DOMAIN_STATUS: &[u8] = b"contextdb-digest-status-v1";
const DIGEST_DOMAIN_COMPLETE: &[u8] = b"contextdb-digest-complete-v1";

fn complete_hash(published: &[[u8; 32]; 11]) -> [u8; 32] {
    hash_parts(
        DIGEST_DOMAIN_COMPLETE,
        &published.iter().map(|field| &field[..]).collect::<Vec<_>>(),
    )
}

const VERIFIED_RECORD_LIMIT: usize = 512;
/// One remembered result-record witness: its storage key, the value hash and
/// the map node that holds them.
const RECORD_WITNESS_BYTES: usize = 33 + 33 + 64;
pub(crate) const READ_VERIFIER_MEMORY_BYTES: usize = VERIFIED_RECORD_LIMIT * 256;

/// The FLOOR of the resident chunk set: what the reader's fixed verifier
/// reservation has left over once the 512 record witnesses have taken their
/// share. A reader gets at least this much residency for free.
///
/// `READ_VERIFIER_MEMORY_BYTES` is admitted once, at source open, for the
/// verifier's own retained state and for the whole life of the source handle.
/// The witnesses spend `VERIFIED_RECORD_LIMIT * RECORD_WITNESS_BYTES` of it;
/// the rest is where the first resident chunks live, admitting nothing further.
pub(crate) const RESIDENT_PROOF_CHUNK_BYTES: usize =
    READ_VERIFIER_MEMORY_BYTES - VERIFIED_RECORD_LIMIT * RECORD_WITNESS_BYTES;

/// The share of a session's DECLARED read memory the resident chunk set may
/// grow into past that floor. Half the ordered-run cache's `limit / 16`,
/// because these chunks are what the run cache's own walks descend through.
///
/// A consumer that declares a gigabyte is entitled to hold the proof of the
/// range it is searching; a compile-time ceiling derived from the witness cap
/// would hand it 63 KiB and make it re-walk the committed root for every
/// statement. These bytes are the consumer's own budget: admitted through the
/// accountant the session already carries, and given back before any proof
/// admission is refused.
const RESIDENT_PROOF_CHUNK_SHARE: usize = 32;
/// The cap on that share, so a very large declaration does not turn into
/// unbounded residency.
const RESIDENT_PROOF_CHUNK_MAX_BYTES: usize = 16 * 1024 * 1024;
/// The share of the declaration that must stay FREE for this set to keep
/// holding borrowed budget at all.
///
/// A cache borrows the session's budget only while the session has room to
/// spare. Below this watermark the set gives its charged chunks back and keeps
/// only the floor its owner reserved, so a reader whose declared memory is
/// genuinely full is never competing with its own proof cache. It is checked
/// on every lookup and every retention — which is every step of every descent
/// — so a session that tightens is noticed inside the statement that tightens
/// it, and `ProofAdmission` reclaims outright before it refuses.
const RESIDENT_PROOF_CHUNK_HEADROOM_SHARE: usize = 8;

/// One page's share of the reader's fixed verifier reservation, held for as
/// long as the page it covers is.
///
/// The floor is the only accounting those bytes have: nothing else is charged
/// for a page kept inside the reservation. So the share cannot end when the
/// cache entry is evicted -- a query that borrowed the page still has it in
/// memory, and handing the same room to a replacement would put two pages in
/// a reservation sized for one. It ends when the last handle to the shared
/// page drops, exactly as the session admission on a charged page does.
struct FloorCharge {
    floor: Arc<AtomicUsize>,
    bytes: usize,
}

impl Drop for FloorCharge {
    fn drop(&mut self) {
        let bytes = self.bytes;
        let _ = self
            .floor
            .fetch_update(AtomicOrdering::SeqCst, AtomicOrdering::SeqCst, |held| {
                Some(held.saturating_sub(bytes))
            });
    }
}

/// One retained chunk. The session admission, if any, lives on the shared
/// `VerifiedReadNode` so a borrower that still holds the page keeps the
/// charge after this entry is evicted.
struct ResidentChunk {
    key: [u8; 33],
    node: Arc<VerifiedReadNode>,
    bytes: usize,
    charged: bool,
    prev: Option<usize>,
    next: Option<usize>,
}

#[derive(Default)]
struct ResidentProofChunks {
    slots: Vec<Option<ResidentChunk>>,
    by_key: BTreeMap<[u8; 33], usize>,
    free: Vec<usize>,
    /// Least recently used first, so the levels every descent crosses outlive
    /// the last leaf that happened to be read.
    oldest: Option<usize>,
    newest: Option<usize>,
    /// Bytes held inside the fixed reservation, admitting nothing. Shared
    /// with every page kept inside it, so a page a query still holds keeps
    /// its share of the reservation after its cache entry is gone.
    free_bytes: Arc<AtomicUsize>,
    /// Bytes held against the session's declared budget.
    charged_bytes: usize,
}

impl ResidentProofChunks {
    fn bytes(&self) -> usize {
        self.free_held().saturating_add(self.charged_bytes)
    }

    /// What the fixed reservation is holding right now, counting pages whose
    /// cache entry is gone but whose borrower has not let go.
    fn free_held(&self) -> usize {
        self.free_bytes.load(AtomicOrdering::SeqCst)
    }

    fn len(&self) -> usize {
        self.by_key.len()
    }

    fn insert(&mut self, chunk: ResidentChunk) -> usize {
        let idx = if let Some(idx) = self.free.pop() {
            self.slots[idx] = Some(chunk);
            idx
        } else {
            let idx = self.slots.len();
            self.slots.push(Some(chunk));
            idx
        };
        let key = self.slots[idx].as_ref().expect("just inserted").key;
        self.by_key.insert(key, idx);
        idx
    }

    fn unlink(&mut self, idx: usize) {
        let (prev, next) = match self.slots.get(idx).and_then(Option::as_ref) {
            Some(chunk) => (chunk.prev, chunk.next),
            None => return,
        };
        match prev {
            Some(prev) => {
                if let Some(chunk) = self.slots.get_mut(prev).and_then(Option::as_mut) {
                    chunk.next = next;
                }
            }
            None => self.oldest = next,
        }
        match next {
            Some(next) => {
                if let Some(chunk) = self.slots.get_mut(next).and_then(Option::as_mut) {
                    chunk.prev = prev;
                }
            }
            None => self.newest = prev,
        }
        if let Some(chunk) = self.slots.get_mut(idx).and_then(Option::as_mut) {
            chunk.prev = None;
            chunk.next = None;
        }
    }

    fn link_newest(&mut self, idx: usize) {
        if let Some(chunk) = self.slots.get_mut(idx).and_then(Option::as_mut) {
            chunk.prev = self.newest;
            chunk.next = None;
        }
        match self.newest {
            Some(newest) => {
                if let Some(chunk) = self.slots.get_mut(newest).and_then(Option::as_mut) {
                    chunk.next = Some(idx);
                }
            }
            None => self.oldest = Some(idx),
        }
        self.newest = Some(idx);
    }

    fn touch_index(&mut self, idx: usize) {
        #[cfg(feature = "test-seams")]
        observe_touch_work(1);
        if self.newest == Some(idx) {
            return;
        }
        self.unlink(idx);
        self.link_newest(idx);
    }

    /// Give back the oldest chunk. Returns the session bytes this cache
    /// no longer counts, which is zero for a chunk that was living inside
    /// the fixed reservation. Neither the accountant charge nor the
    /// reservation share ends here: both follow the shared node and are
    /// released when the last handle to it drops.
    fn evict_oldest(&mut self) -> Option<usize> {
        let idx = self.oldest?;
        self.unlink(idx);
        let Some(evicted) = self.slots[idx].take() else {
            self.free.push(idx);
            return Some(0);
        };
        self.free.push(idx);
        self.by_key.remove(&evicted.key);
        if evicted.charged {
            self.charged_bytes = self.charged_bytes.saturating_sub(evicted.bytes);
            Some(evicted.bytes)
        } else {
            // The reservation share rides on the shared page and comes back
            // when its last holder drops it, which may be after this entry.
            Some(0)
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn lru_keys(&self) -> Vec<[u8; 33]> {
        let mut keys = Vec::new();
        let mut cur = self.oldest;
        while let Some(idx) = cur {
            let Some(chunk) = self.slots.get(idx).and_then(Option::as_ref) else {
                break;
            };
            keys.push(chunk.key);
            cur = chunk.next;
        }
        keys
    }
}

/// The authenticated chunks one pinned snapshot has already proved against its
/// committed root, kept so the next statement over the same immutable pages
/// does not read, decode or hash them a second time.
///
/// The set is bound to one `ContentIdentityReadVerifier`, which is bound to
/// one pinned storage snapshot, and every chunk in it is still checked against
/// the hash its parent demands on every walk. It therefore cannot hand an old
/// proof to another snapshot, another root, or content that changed.
///
/// Its bytes come from two places and are accounted separately. Up to
/// `free_floor` they live in the reservation the owner already took for the
/// verifier's retained state. Past it, each chunk is admitted through the
/// session's own accountant before it is kept, up to `ceiling`, and **given
/// back before a proof admission is refused** — the set is a cache, and a
/// cache never refuses a read while it is still holding something.
pub(super) struct ResidentProofChunkSet {
    state: Mutex<ResidentProofChunks>,
    accountant: Option<crate::memory_accounting::StoreAccountantHandle>,
    free_floor: usize,
    ceiling: usize,
    /// Declared bytes that must stay free for the charged part of this set to
    /// be held at all. Zero when the session declared no ceiling.
    headroom: usize,

}

// Declaration order is the cleanup protocol: state (all chunk/index/Vec
// children) drops before its original counted accountant handle. The enclosing
// Arc has PRE2's unchanged layout and adds no fixed-control liability.
impl ResidentProofChunkSet {
    fn idle() -> Self {
        Self {
            state: Mutex::new(ResidentProofChunks::default()),
accountant: None,
            free_floor: 0,
            ceiling: 0,
            headroom: 0,
        }
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, ResidentProofChunks> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// A chunk this snapshot has already proved, if it is still resident. A
    /// hit is also a use.
    fn get(&self, storage_key: &[u8; 33]) -> Option<Arc<VerifiedReadNode>> {
        if self.ceiling == 0 {
            return None;
        }
        let mut state = self.locked();
        let idx = *state.by_key.get(storage_key)?;
        let node = Arc::clone(&state.slots[idx].as_ref()?.node);
        state.touch_index(idx);
        Some(node)
    }

    /// Keep a chunk that has just been read, decoded and proved against the
    /// committed root, and hand back the one handle this set keeps for it.
    ///
    /// The retained handle shares the caller's decoded chunk and never carries
    /// the caller's admission: inside the free floor it is covered by the
    /// reservation the owner already holds, and past it this takes an
    /// admission of its own so the bytes are charged exactly once and released
    /// when the last handle to this shared node is dropped. A caller that goes
    /// on holding the page holds THAT handle -- never a second, uncharged
    /// wrapper around the same decode -- so evicting this entry cannot end
    /// the charge while the caller still has the page. `None` when the set
    /// declined to keep the chunk; the caller then owns the decode's charge.
    fn retain(
        &self,
        storage_key: [u8; 33],
        chunk: &Arc<VerifiedReadNode>,
    ) -> Option<Arc<VerifiedReadNode>> {
        if self.ceiling == 0 || chunk.bytes > self.ceiling {
            return None;
        }
        let tight = self.accountant.as_ref().is_some_and(|accountant| {
            accountant
                .usage()
                .available
                .is_some_and(|available| available < self.headroom)
        });
        let mut state = self.locked();
        if let Some(&idx) = state.by_key.get(&storage_key) {
            return state.slots[idx].as_ref().map(|kept| Arc::clone(&kept.node));
        }
        while state.bytes().saturating_add(chunk.bytes) > self.ceiling {
            state.evict_oldest()?;
        }
        let free = state.free_held().saturating_add(chunk.bytes) <= self.free_floor;
        if tight && !free {
            // A session this close to its declaration gets a floor-sized
            // cache and nothing more. Borrowing a byte back now would only be
            // given up again on the next descent step, which costs the read
            // two evictions instead of saving it one page.
            return None;
        }
        let charge = if free {
            None
        } else {
            // Evict before admitting, then admit before keeping. A chunk this
            // session has no room for is simply not kept; no read is refused
            // to make space for a cache.
            loop {
                match self.admit(chunk.bytes) {
                    Some(charge) => break Some(charge),
                    None => match state.evict_oldest() {
                        Some(0) | None => return None,
                        Some(_) => continue,
                    },
                }
            }
        };
        let floor = if free {
            state
                .free_bytes
                .fetch_add(chunk.bytes, AtomicOrdering::SeqCst);
            Some(FloorCharge {
                floor: Arc::clone(&state.free_bytes),
                bytes: chunk.bytes,
            })
        } else {
            state.charged_bytes = state.charged_bytes.saturating_add(chunk.bytes);
            None
        };
        let kept = Arc::new(VerifiedReadNode {
            node: Arc::clone(&chunk.node),
            bytes: chunk.bytes,
            _charge: AdmittedBytes::with_source(chunk._charge.as_ref().and_then(AdmittedBytes::source_custody), charge),
            _floor: floor,
        });
        let idx = state.insert(ResidentChunk {
            key: storage_key,
            node: Arc::clone(&kept),
            bytes: chunk.bytes,
            charged: !free,
            prev: None,
            next: None,
        });
        state.link_newest(idx);
        Some(kept)
    }

    fn admit(&self, bytes: usize) -> Option<AdmittedBytes> {
        let accountant = self.accountant.as_ref()?;
        accountant
            .try_allocate_for_original(
                bytes,
                "content_identity",
                "resident_proof_chunk",
                "Nothing to raise: a proved chunk is a cache and gives its bytes back on demand.",
            )
            .ok()?;
        Some(AdmittedBytes {
            reader_budget: None,
            accountant: Some(accountant.clone()),
            bytes,
        })
    }

    /// Give back the oldest chunk holding session bytes, so an admission that
    /// was about to be refused can have them. `true` when bytes were freed.
    fn reclaim(&self) -> bool {
        let mut state = self.locked();
        while state.charged_bytes > 0 {
            match state.evict_oldest() {
                Some(released) if released > 0 => return true,
                Some(_) => continue,
                None => return false,
            }
        }
        false
    }

    #[cfg(feature = "test-seams")]
    fn observed(&self) -> (usize, usize, usize) {
        let state = self.locked();
        (state.len(), state.bytes(), state.charged_bytes)
    }

    fn clear(&self) -> usize {
        let mut state = self.locked();
        let evicted = state.len();
        state.slots.clear();
        state.by_key.clear();
        state.free.clear();
        state.oldest = None;
        state.newest = None;
        state.charged_bytes = 0;
        evicted
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn charged_for_test(
        accountant: Arc<crate::memory_accounting::MemoryAccountant>,
        ceiling: usize,
    ) -> Self {
        Self {
            state: Mutex::new(ResidentProofChunks::default()),
accountant: Some(crate::memory_accounting::StoreAccountantHandle::new(accountant.into())),
            free_floor: 0,
            ceiling,
            headroom: 0,
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn free_for_test(floor: usize) -> Self {
        Self {
            state: Mutex::new(ResidentProofChunks::default()),
accountant: None,
            free_floor: floor,
            ceiling: floor,
            headroom: 0,
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn lru_keys_for_test(&self) -> Vec<[u8; 33]> {
        self.locked().lru_keys()
    }

    /// Whether this set is still charging or floor-covering the exact
    /// decoded page `target` points to, by allocation identity rather than
    /// storage key -- a borrower can hold a page under a key this set no
    /// longer maps (or maps to a different, later chunk), so only pointer
    /// identity of the shared decode tells whether THIS allocation is still
    /// this set's responsibility.
    #[cfg(feature = "test-seams")]
    fn still_holds_for_test(&self, target: &Arc<Node>) -> bool {
        self.locked()
            .slots
            .iter()
            .flatten()
            .any(|chunk| Arc::ptr_eq(&chunk.node.node, target))
    }
}

/// The bytes one read walk actually holds, charged against the memory the
/// caller has already had admitted.
///
/// A range proof does not hold a fixed number of pages: it holds one chunk per
/// level of the tree it descends, the one key it is comparing, whatever chunks
/// it decided to keep for reuse, and the payload the caller is accumulating.
/// All four are charged here against one pool, so the depth of the tree and the
/// width of its keys are accounted for instead of assumed.
/// How a walk's pool asks its OWNER for more room. The pool's ceiling is a
/// bound to grow into, not an entry fee: a walk admits the page it is about to
/// read and the payload it is about to keep, and grows only when it genuinely
/// needs the next one. The owner evicts its own caches before admitting, so
/// growth never inflates a budget.
pub(super) trait ProofPoolGrowth: Send + Sync {
    /// Enlarge the owner's reservation by `bytes`. `true` only when the
    /// reservation really grew.
    fn grow(&self, bytes: usize) -> bool;

    /// The most this pool may ever hold.
    fn ceiling(&self) -> usize;
}

/// The step a growable pool takes when it needs more than it holds. One step
/// covers an ordinary page and its scratch, so a deep descent grows a few
/// times rather than once per allocation.
const PROOF_POOL_GROWTH_BYTES: usize = 64 * 1024;

pub(super) struct ProofMemory {
    limit: std::cell::Cell<usize>,
    /// Present for a pool whose owner can enlarge it. Absent for a pool that is
    /// exactly the reservation its caller already holds.
    growth: Option<Arc<dyn ProofPoolGrowth>>,
    live: std::cell::Cell<usize>,
    cached: std::cell::Cell<usize>,
    payload: std::cell::Cell<usize>,
    peak: std::cell::Cell<usize>,
    live_peak: std::cell::Cell<usize>,
    exhausted: std::cell::Cell<bool>,
    /// Admission for a walk whose caller holds no reservation of its own. When
    /// the caller has already had this pool admitted — the cached-run build
    /// reserves it before the walk starts — this stays unaccounted so the same
    /// bytes are never charged to the accountant twice.
    admission: std::cell::RefCell<ProofAdmission>,
    /// Set when `admission` refuses a charge. Windowing (the pool's own
    /// ceiling) leaves this empty; an accountant refusal is the typed error.
    refusal: std::cell::RefCell<Option<Error>>,
    _writer_use: Option<crate::memory_accounting::WriterUse>,
}

/// One live charge, returned when the walk drops what it was holding —
/// including when it unwinds on an error.
pub(super) struct ProofCharge<'a> {
    memory: &'a ProofMemory,
    bytes: usize,
}

impl Drop for ProofCharge<'_> {
    fn drop(&mut self) {
        self.memory
            .live
            .set(self.memory.live.get().saturating_sub(self.bytes));
        self.memory.admission().release(self.bytes);
    }
}

impl ProofMemory {
    /// A walk inside memory the caller has already had admitted. The pool is
    /// the caller's reservation and is not charged again here.
    pub(super) fn with_limit(limit: usize) -> Self {
        Self::new(limit, ProofAdmission::unaccounted())
    }

    /// A walk that admits what it holds itself, for a caller that reserved
    /// nothing up front.
    pub(super) fn admitted(
        accountant: Option<crate::memory_accounting::AccountantHandle>,
        limit: usize,
    ) -> Self {
        Self::new(limit, ProofAdmission::new(accountant))
    }

    /// A walk inside a reservation its owner can enlarge on demand. The pool
    /// starts at what has already been admitted and grows, page by page, only
    /// as far as the owner's ceiling.
    pub(super) fn growable(limit: usize, growth: Arc<dyn ProofPoolGrowth>) -> Self {
        let memory = Self::new(limit, ProofAdmission::unaccounted());
        Self {
            growth: Some(growth),
            ..memory
        }
    }

    fn new(limit: usize, admission: ProofAdmission) -> Self {
        let writer_use = admission.accountant.as_ref().or(admission.scratch_accountant.as_ref())
            .and_then(crate::memory_accounting::WriterUse::retain_if_writer);
        Self {
            limit: std::cell::Cell::new(limit),
            growth: None,
            live: std::cell::Cell::new(0),
            cached: std::cell::Cell::new(0),
            payload: std::cell::Cell::new(0),
            peak: std::cell::Cell::new(0),
            live_peak: std::cell::Cell::new(0),
            exhausted: std::cell::Cell::new(false),
            admission: std::cell::RefCell::new(admission),
            refusal: std::cell::RefCell::new(None),
            _writer_use: writer_use,
        }
    }

    fn take_refusal(&self) -> Option<Error> {
        self.refusal.borrow_mut().take()
    }

    /// The admission this walk charges its own allocations to.
    fn admission(&self) -> std::cell::RefMut<'_, ProofAdmission> {
        self.admission.borrow_mut()
    }

    /// What this walk has admitted from the accountant right now.
    pub(super) fn admitted_bytes(&self) -> usize {
        self.admission.borrow().held()
    }

    /// For callers that hold no reservation of their own, such as the
    /// engine-internal contracts. The charge is still measured.
    pub(super) fn unlimited() -> Self {
        Self::with_limit(usize::MAX)
    }

    fn held(&self) -> usize {
        self.live
            .get()
            .saturating_add(self.cached.get())
            .saturating_add(self.payload.get())
    }

    fn note_peak(&self) {
        self.peak.set(self.peak.get().max(self.held()));
    }

    /// Make room for `bytes` beside what this pool already holds, growing the
    /// owner's reservation when the pool is a growable one. Growth is the
    /// actual shortfall, partitioned into steps of at most
    /// `PROOF_POOL_GROWTH_BYTES`, with a final partial increment. The 64 KiB
    /// figure is a maximum partition, never a floor.
    fn ensure_capacity(&self, bytes: usize) -> bool {
        let needed = self.held().saturating_add(bytes);
        if needed <= self.limit.get() {
            return true;
        }
        let Some(growth) = self.growth.as_ref() else {
            return false;
        };
        while needed > self.limit.get() {
            let ceiling = growth.ceiling();
            let Some(room) = ceiling.checked_sub(self.limit.get()) else {
                return false;
            };
            let step = needed
                .saturating_sub(self.limit.get())
                .min(PROOF_POOL_GROWTH_BYTES)
                .min(room);
            if step == 0 || !growth.grow(step) {
                return false;
            }
            self.limit.set(self.limit.get().saturating_add(step));
        }
        true
    }

    fn charge_live(&self, bytes: usize) -> Option<ProofCharge<'_>> {
        if !self.ensure_capacity(bytes) {
            self.exhausted.set(true);
            return None;
        }
        if let Err(error) = self.admission().admit(bytes) {
            self.exhausted.set(true);
            *self.refusal.borrow_mut() = Some(error);
            return None;
        }
        self.live.set(self.live.get().saturating_add(bytes));
        self.live_peak
            .set(self.live_peak.get().max(self.live.get()));
        self.note_peak();
        #[cfg(feature = "test-seams")]
        observe_resolved_key_event("charge", bytes);
        Some(ProofCharge {
            memory: self,
            bytes,
        })
    }

    /// Admit a transient allocation the caller is about to make, returned when
    /// the charge is dropped.
    pub(super) fn charge_scratch(&self, bytes: usize) -> Option<ProofCharge<'_>> {
        self.charge_live(bytes)
    }

    /// Charge `bytes`, returning `Ok(None)` when the pool's own ceiling stops
    /// the walk and `Err` when the accountant refused the allocation.
    fn charge_or_exhaust(&self, bytes: usize) -> Result<Option<ProofCharge<'_>>> {
        match self.charge_live(bytes) {
            Some(charge) => Ok(Some(charge)),
            None => match self.take_refusal() {
                Some(error) => Err(error),
                None => Ok(None),
            },
        }
    }

    pub(super) fn charge_payload(&self, bytes: usize) -> bool {
        if !self.ensure_capacity(bytes) {
            self.exhausted.set(true);
            return false;
        }
        self.payload.set(self.payload.get().saturating_add(bytes));
        self.note_peak();
        true
    }

    /// Retained proof pages are a cache, so the pool is never GROWN to hold
    /// one: they claim at most half of what this walk has already admitted,
    /// and the walk's own live set and the caller's payload keep the rest.
    fn cache_limit(&self) -> usize {
        let limit = self.limit.get();
        (limit / 2).min(
            limit
                .saturating_sub(self.live.get())
                .saturating_sub(self.payload.get()),
        )
    }

    fn set_cached(&self, bytes: usize) {
        let held = self.cached.replace(bytes);
        if bytes > held {
            // Retained pages are admitted as they are kept; a refusal here only
            // means the walk stops caching, never that it reports failure.
            if self.admission().admit(bytes - held).is_err() {
                self.cached.set(held);
                self.exhausted.set(true);
                return;
            }
        } else {
            self.admission().release(held - bytes);
        }
        self.note_peak();
    }

    pub(super) fn exhausted(&self) -> bool {
        self.exhausted.get()
    }

    pub(super) fn retry_after_eviction(&self) {
        self.exhausted.set(false);
    }

    pub(super) fn peak(&self) -> usize {
        self.peak.get()
    }

    /// What the descent itself held at its deepest, without the pages it chose
    /// to keep or the caller's payload.
    pub(super) fn live_peak(&self) -> usize {
        self.live_peak.get()
    }

    pub(super) fn live_bytes(&self) -> usize {
        self.live.get()
    }
}

/// Fixed-snapshot verifier for key-only content-defined pages. Page boundaries,
/// internal separators and node ids derive only from canonical logical keys, so
/// insertion order, compaction and Redb allocation cannot affect identity.
pub(crate) struct ContentIdentityReadVerifier {
    roots: [FamilyRoot; 11],
    /// Ordered digest of the complete custody copy, read from the Configuration
    /// family-root record when this verifier opened.
    custody_config_root: [u8; 32],
    nodes: ReadOnlyTable<&'static [u8], &'static [u8]>,
    verified_records: Mutex<BTreeMap<[u8; 33], VerifiedRecordWitness>>,
    /// The store this verifier authenticates against, so every refusal it
    /// returns is the existing typed corruption error rather than an untyped
    /// diagnostic the caller has to re-label.
    source: String,
    /// The database accountant that admits what this verifier's own walks
    /// allocate. Callers that reserved a pool up front pass their reservation
    /// instead, so those bytes are not charged twice.
    admission: Option<crate::memory_accounting::AccountantHandle>,
    /// Chunks already proved against this verifier's committed root, reused
    /// while they are resident. Bound to this verifier, which is bound to one
    /// pinned storage snapshot, so a chunk can never be reused under another
    /// root or another snapshot. Idle until an owner declares its reservation.
    resident: Arc<ResidentProofChunkSet>,

    _writer_use: Option<crate::memory_accounting::WriterUse>,
}

/// What one authenticated page's shared handle costs beside the decoded
/// chunk it holds: the handle, its bookkeeping and the map node that keeps it.
const VERIFIED_READ_NODE_HANDLE_BYTES: usize = std::mem::size_of::<VerifiedReadNode>()
    + 4 * std::mem::size_of::<usize>();

/// One authenticated page and the one charge that pays for it.
///
/// A decoded page is held through exactly one `Arc<VerifiedReadNode>`, shared
/// by every holder -- the resident set, a statement's reuse set, a leaf
/// locator, the walk in progress -- and never re-wrapped. The charge on it
/// (`_charge` or `_floor`) is therefore released when the last holder drops
/// it: no holder can outlive the charge, and no second holder adds a second
/// one.
struct VerifiedReadNode {
    node: Arc<Node>,
    bytes: usize,
    /// The decode's own admission, held for as long as this authenticated page
    /// is: the charge follows the allocation instead of ending at the call that
    /// made it.
    _charge: Option<AdmittedBytes>,
    /// The share of the reader's fixed verifier reservation this page occupies
    /// when it is kept inside that reservation rather than charged to the
    /// session. It follows the page for the same reason the admission does.
    _floor: Option<FloorCharge>,
}

#[derive(Clone, Copy)]
struct VerifiedRecordWitness {
    value_hash: Option<[u8; 32]>,
}

/// Authenticated index-tree pages one statement's ordered descents share.
/// Owned by the caller that owns the proof pool they are charged to.
#[derive(Default)]
pub(super) struct OrderedProofBatch {
    nodes: BTreeMap<[u8; 33], Arc<VerifiedReadNode>>,
    bytes: usize,
}

impl OrderedProofBatch {
    pub(super) fn evict(&mut self, proof: &ProofMemory) {
        self.nodes.clear();
        self.bytes = 0;
        proof.set_cached(0);
        proof.retry_after_eviction();
    }
}

/// Authenticated logical pages reused within one immutable query batch.
pub(super) struct ReadAuthenticationBatch<'a> {
    verifier: &'a ContentIdentityReadVerifier,
    nodes: BTreeMap<[u8; 33], Arc<VerifiedReadNode>>,
    bytes: usize,
    byte_limit: usize,
}

/// Authenticated logical pages and leaf locators kept under one pinned root.
/// An open body read can carry the set between candidates, and a retained
/// source can share it with its row walk.
///
/// Who pays for what this set holds:
///
/// - A page is one shared `Arc<VerifiedReadNode>`, and that handle carries
///   the page's one charge: the resident set's own session admission or its
///   floor share when the resident set keeps the page, or the decode's own
///   admission, handed to the page before it is kept, when only this set
///   keeps it. Every holder shares that one handle, so the charge ends when
///   the LAST holder lets go -- a resident eviction while this statement
///   still holds the page leaves the charge in place, and the same page is
///   never charged twice.
/// - A leaf locator's own bookkeeping is admitted to `charge` before it is
///   inserted, and given back when the set is dropped or reset.
///
/// Everything here is reuse, never the proof working set: it grows only while
/// the declared session keeps the resident set's headroom free, a page or
/// locator that does not fit is simply not kept, and a verification refused
/// for memory drops this whole set and tries once more before the refusal
/// stands (`verify_with_nodes`).
pub(crate) struct ReadAuthenticationNodes {
    nodes: BTreeMap<[u8; 33], Arc<VerifiedReadNode>>,
    bytes: usize,
    byte_limit: usize,
    leaves: BTreeMap<[u8; 34], CachedAuthenticatedLeaf>,
    leaf_bytes: usize,
    charge: ProofAdmission,
    _writer_use: Option<crate::memory_accounting::WriterUse>,
}

struct CachedAuthenticatedLeaf {
    node: Arc<VerifiedReadNode>,
}

const CACHED_AUTHENTICATED_LEAF_BYTES: usize =
    34 + std::mem::size_of::<Arc<VerifiedReadNode>>() + std::mem::size_of::<AdmittedBytes>() + 64;

impl ReadAuthenticationNodes {
    pub(crate) fn new(
        byte_limit: usize,
        admission: Option<crate::memory_accounting::AccountantHandle>,
    ) -> Self {
        Self {
            nodes: BTreeMap::new(),
            bytes: 0,
            byte_limit,
            leaves: BTreeMap::new(),
            leaf_bytes: 0,
            _writer_use: admission.as_ref().and_then(crate::memory_accounting::WriterUse::retain_if_writer),
            charge: ProofAdmission::new(admission),
        }
    }

    fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.leaves.is_empty()
    }

    /// Let go of every page and locator this set holds. Each page's charge
    /// goes back with the last handle to it, and the locators' admission is
    /// returned here, so the set and what it is charged for reach zero
    /// together.
    fn reset(&mut self) {
        self.nodes.clear();
        self.bytes = 0;
        self.leaves.clear();
        self.leaf_bytes = 0;
        let held = self.charge.held();
        self.charge.release(held);
    }

    /// The local byte count this statement's retained node and leaf pages
    /// are tracked at (first), and how many of those bytes are covered right
    /// now by either this set's own charge (`charge`, for pages the resident
    /// set does not cover) or by the resident set still holding and charging
    /// (or floor-covering) that same decoded page (second), rather than
    /// nothing at all. `still_resident` answers, for one page's shared
    /// decoded allocation, whether the resident set is currently charging or
    /// floor-covering it right now -- the caller (which alone holds the
    /// resident set) supplies it by pointer identity, since a page resident
    /// at retain time is not guaranteed to still be resident now, and a
    /// node's storage key is not reconstructable from its leaf-locator entry.
    #[cfg(feature = "test-seams")]
    pub(super) fn retained_bytes_and_accounted(
        &self,
        mut still_resident: impl FnMut(&Arc<Node>) -> bool,
    ) -> (usize, usize) {
        let node_accounted: usize = self
            .nodes
            .values()
            .filter(|node| {
                node._charge.as_ref().is_some_and(|charge| charge.bytes != 0) || node._floor.is_some() || still_resident(&node.node)
            })
            .map(|node| node.bytes)
            .sum();
        let leaf_accounted: usize = self
            .leaves
            .values()
            .filter(|leaf| {
                leaf.node._charge.as_ref().is_some_and(|charge| charge.bytes != 0)
                    || leaf.node._floor.is_some()
                    || still_resident(&leaf.node.node)
            })
            // `leaf_bytes` tracks the fixed locator overhead per entry, not
            // the underlying page's own bytes (already counted once through
            // `nodes`, or held only by the resident set); match that unit
            // here so the two sides of this sum agree with `self.leaf_bytes`.
            .map(|_leaf| CACHED_AUTHENTICATED_LEAF_BYTES)
            .sum();
        (
            self.bytes + self.leaf_bytes,
            node_accounted + leaf_accounted,
        )
    }

    /// The bytes this statement's own admission (`charge`) is holding right
    /// now: leaf-locator overhead plus any page only this statement kept,
    /// admitted before allocation (`ProofAdmission::detach`). Distinct from
    /// whatever the resident set is separately charging or floor-covering
    /// for the same decoded pages, and released with this whole set when it
    /// is reset (`reset`): one live charge per shared page until its last
    /// holder drops it.
    #[cfg(feature = "test-seams")]
    pub(super) fn own_charge_for_test(&self) -> usize {
        self.charge.held()
    }
}

impl ReadAuthenticationBatch<'_> {
    pub(super) fn verify(&mut self, mutation: &ContentIdentityMutation) -> Result<()> {
        self.verifier
            .verify_with_batch(
                mutation,
                &mut self.nodes,
                &mut self.bytes,
                self.byte_limit,
                false,
            )
            .map(|_| ())
            .map_err(|error| self.verifier.typed(error))
    }
}

impl ContentIdentityReadVerifier {
    pub(crate) fn verify_open_config_projection(&self, copied: &[(String, Vec<u8>)]) -> Result<()> {
        let digest = open_config_digest(
            copied
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_slice())),
        );
        if self.roots[LogicalFamily::Configuration as usize].open_config_root != Some(digest) {
            return Err(self.corrupt(
                "startup configuration projection disagrees with committed Configuration root",
            ));
        }
        Ok(())
    }

    pub(super) fn verify_custody_config_projection(&self, digest: [u8; 32]) -> Result<()> {
        if self.custody_config_root != digest {
            return Err(self.corrupt(
                "custody configuration projection disagrees with committed Configuration root",
            ));
        }
        Ok(())
    }

    /// Report a proof failure as the existing typed corruption error. An error
    /// that already carries that type passes through unchanged.
    pub(super) fn corrupt(&self, reason: impl std::fmt::Display) -> Error {
        Error::StoreCorrupted {
            path: self.source.clone(),
            reason: format!(
                "loaded content does not authenticate against the committed image root: {reason}"
            ),
        }
    }

    fn typed(&self, error: Error) -> Error {
        match error {
            Error::StoreCorrupted { .. } => error,
            // A proof this reader had no room to walk says nothing about
            // whether the store is sound. Calling it corruption sends an
            // operator looking for damage that is not there, when what they
            // have is a budget to raise.
            Error::MemoryBudgetExceeded { .. } => error,
            other => self.corrupt(other),
        }
    }

    pub(crate) fn open(
        read_txn: &redb::ReadTransaction,
        source: &std::path::Path,
        admission: Option<crate::memory_accounting::AccountantHandle>,
    ) -> Result<Self> {
        let roots_table = read_txn
            .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        let mut roots: [FamilyRoot; 11] =
            std::array::from_fn(|family| empty_root(LogicalFamily::ALL[family]));
        let mut custody_config_root = None;
        for family in LogicalFamily::ALL {
            let encoded = roots_table
                .get(family as u8)
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(|| Error::Other("logical content family root is absent".to_owned()))?;
            let published = decode_published_root(family, encoded.value())?;
            if family == LogicalFamily::Configuration {
                custody_config_root = published.custody_config_root;
            }
            roots[family as usize] = published.identity;
        }
        let custody_config_root = custody_config_root.ok_or_else(|| {
            Error::Other(
                "custody configuration projection is absent from the committed root".to_owned(),
            )
        })?;
        drop(roots_table);
        let nodes = read_txn
            .open_table(CONTENT_IDENTITY_NODES_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        let mut resident = ResidentProofChunkSet::idle();
        resident.accountant = admission.as_ref().map(|accountant|
            crate::memory_accounting::StoreAccountantHandle::new(accountant.clone()));
        Ok(Self {
            roots,
            custody_config_root,
            nodes,
            verified_records: Mutex::new(BTreeMap::new()),
            source: source.display().to_string(),
            _writer_use: (admission).as_ref().and_then(crate::memory_accounting::WriterUse::retain_if_writer),
admission,
            // Nothing is kept until an owner that holds the reservation says
            // so; see `retain_proved_chunks_within`.
            resident: Arc::new(resident),
        })
    }

    /// The database accountant this verifier admits its own walks against, for
    /// a caller that needs to admit retained bytes the same way -- a new
    /// [`ReadAuthenticationNodes`] set, in particular.
    pub(crate) fn admission_accountant(
        &self,
    ) -> Option<crate::memory_accounting::AccountantHandle> {
        self.admission.clone()
    }

    /// Let this verifier keep proved chunks. `reserved_bytes` is the floor its
    /// owner has ALREADY admitted for exactly this state; past that the set
    /// grows into the session's own declared read memory, up to
    /// `RESIDENT_PROOF_CHUNK_SHARE` of it capped by
    /// `RESIDENT_PROOF_CHUNK_MAX_BYTES`, and gives those bytes back before any
    /// proof admission is refused. A verifier whose owner took no reservation
    /// — the checkpoint export source, and the engine-internal contracts —
    /// keeps nothing.
    pub(crate) fn retain_proved_chunks_within(&mut self, reserved_bytes: usize) {
        let declared = self
            .admission
            .as_ref()
            .and_then(|accountant| accountant.usage().limit);
        let ceiling = declared.map_or(reserved_bytes, |limit| {
            (limit / RESIDENT_PROOF_CHUNK_SHARE)
                .min(RESIDENT_PROOF_CHUNK_MAX_BYTES)
                .max(reserved_bytes)
        });
        // Every caller configures the fresh verifier before any walk can
        // clone its resident handle. Reuse the control admitted at open.
        let resident = Arc::get_mut(&mut self.resident).expect("fresh verifier owns its resident control");
        resident.free_floor = reserved_bytes;
        resident.ceiling = ceiling;
        resident.headroom = declared.map_or(0, |limit| limit / RESIDENT_PROOF_CHUNK_HEADROOM_SHARE);
    }

    /// Open may prove ordered history before any statement begins. Release
    /// those pages and witnesses so the first statement starts with the same
    /// bounded proof cache as a cold query.
    pub(crate) fn clear_startup_proofs(&self) {
        self.resident.clear();
        self.verified_records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }

    /// How many authenticated chunks this reader is holding, their bytes, and
    /// how many of those bytes are charged to the declared session.
    #[cfg(feature = "test-seams")]
    pub(super) fn resident_proof_chunks_for_test(&self) -> (usize, usize, usize) {
        self.resident.observed()
    }

    /// Whether this reader's resident set is, right now, still charging or
    /// floor-covering the exact decoded page `target` points to. A page a
    /// statement copied out of residency keeps the same decoded allocation
    /// after the resident set evicts it, so this answers whether that
    /// allocation is still the resident set's charge to carry -- never
    /// whether some OTHER, later chunk now occupies its old storage key.
    #[cfg(feature = "test-seams")]
    pub(super) fn resident_still_holds_for_test(&self, target: &Arc<Node>) -> bool {
        self.resident.still_holds_for_test(target)
    }

    /// The most this reader may hold resident, floor included.
    #[cfg(feature = "test-seams")]
    pub(super) fn resident_proof_chunk_ceiling_for_test(&self) -> usize {
        self.resident.ceiling
    }

    /// Give back the remembered result-record witnesses only, leaving the
    /// resident chunks alone. Clearing one without the other is what lets a
    /// contract separate "this record was already answered" from "the chunks
    /// that prove it are already authenticated".
    #[cfg(feature = "test-seams")]
    pub(super) fn evict_record_witnesses_for_test(&self) -> usize {
        let mut records = self
            .verified_records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let evicted = records.len();
        records.clear();
        evicted
    }

    /// Give back every resident chunk, as a genuine eviction does. The next
    /// statement has to read, decode and prove them against the committed root
    /// again.
    #[cfg(feature = "test-seams")]
    pub(super) fn evict_resident_proof_chunks_for_test(&self) -> usize {
        self.resident.clear()
    }

    #[allow(dead_code)] // Exercised by the bounded proof-cache contract.
    pub(super) fn batch(&self, reserved_bytes: usize) -> ReadAuthenticationBatch<'_> {
        ReadAuthenticationBatch {
            verifier: self,
            nodes: BTreeMap::new(),
            bytes: 0,
            byte_limit: reserved_bytes,
        }
    }

    /// Point proof scratch belongs to this operation; any cache already held
    /// by the source keeps its separately admitted owner.
    pub(crate) fn verify_with_read_memory(
        &self,
        mutation: &ContentIdentityMutation,
        memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        let _controls = contextdb_core::read_memory::ReadCredit::try_new(
            memory.clone(),
            std::mem::size_of::<Mutex<Option<Error>>>() + 2 * std::mem::size_of::<usize>(),
            "content_identity", "read_proof_controls",
            "Reduce the requested proof work or raise the declared reader memory limit.",
        )?;
        let refusal = Arc::new(Mutex::new(None));
        let mut admission = ProofAdmission::new(None);
        admission.reader_budget = Some(memory);
        admission.reader_refusal = Some(refusal.clone());
        let result = self.verify_with_batch_admission(mutation, &mut BTreeMap::new(), &mut 0, 0, self.admission.is_some(), &mut admission, None).map(|_| ());
        let original = refusal.lock().unwrap_or_else(|e| e.into_inner()).take();
        match original { Some(error) => Err(error), None => result.map_err(|error| self.typed(error)) }
    }

    /// Authenticate a hashed reader projection with operation-owned scratch
    /// and the caller's checkpoint at every input window and proof descent.
    pub(crate) fn verify_with_read_memory_before(
        &self,
        mutation: &ContentIdentityMutation,
        memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        before()?;
        let _controls = contextdb_core::read_memory::ReadCredit::try_new(
            memory.clone(),
            std::mem::size_of::<Mutex<Option<Error>>>() + 2 * std::mem::size_of::<usize>(),
            "content_identity", "read_proof_controls",
            "Reduce the requested proof work or raise the declared reader memory limit.",
        )?;
        let refusal = Arc::new(Mutex::new(None));
        let mut admission = ProofAdmission::new(None);
        admission.reader_budget = Some(memory);
        admission.reader_refusal = Some(refusal.clone());
        let result = self.verify_reader_projection(mutation, &mut admission, None, before);
        let original = refusal.lock().unwrap_or_else(|error| error.into_inner()).take();
        match original {
            Some(error) => Err(error),
            None => result.map_err(|error| match error {
                Error::ReadCancelled | Error::ReadFailure(_) => error,
                other => self.typed(other),
            }),
        }
    }

    pub(crate) fn verify(&self, mutation: &ContentIdentityMutation) -> Result<()> {
        if let Some(memory) = crate::read_image_memory::current_reader_budget() { return self.verify_with_read_memory(mutation, memory); }
        self.verify_with_batch(mutation, &mut BTreeMap::new(), &mut 0, 0, true)
            .map(|_| ())
            .map_err(|error| self.typed(error))
    }

    /// `verify`, reusing the pages a statement has already authenticated.
    ///
    /// The statement's reused pages are an optimization, not the proof's
    /// working set, so they give way before a refusal does: a verification
    /// refused for memory while `nodes` holds anything drops the whole set --
    /// every page charge it pinned goes back with it -- and runs once more
    /// from the committed root. Only a refusal of that second run, whose
    /// memory is this record's own proof and nothing kept for later, is the
    /// typed answer; the set is left empty either way, so a refused statement
    /// holds nothing afterwards.
    pub(crate) fn verify_with_nodes(
        &self,
        nodes: &mut ReadAuthenticationNodes,
        mutation: &ContentIdentityMutation,
    ) -> Result<()> {
        let first = self.verify_with_nodes_once(nodes, mutation);
        let Err(Error::MemoryBudgetExceeded { .. }) = &first else {
            return first;
        };
        let was_holding = !nodes.is_empty();
        nodes.reset();
        if !was_holding {
            return first;
        }
        let second = self.verify_with_nodes_once(nodes, mutation);
        if matches!(second, Err(Error::MemoryBudgetExceeded { .. })) {
            nodes.reset();
        }
        second
    }

    /// Whether a statement's reuse set may keep one more page or locator.
    ///
    /// What that set holds stays held for the whole statement, and no later
    /// allocation can make it let go, so it grows only while the declared
    /// session still has the same share free that the resident set demands
    /// before it borrows (`RESIDENT_PROOF_CHUNK_HEADROOM_SHARE`). A session
    /// with no declared ceiling always has room.
    fn statement_reuse_has_room(&self) -> bool {
        self.admission.as_ref().is_none_or(|accountant| {
            let usage = accountant.usage();
            match (usage.limit, usage.available) {
                (Some(limit), Some(available)) => {
                    available >= limit / RESIDENT_PROOF_CHUNK_HEADROOM_SHARE
                }
                _ => true,
            }
        })
    }

    fn verify_with_nodes_once(
        &self,
        nodes: &mut ReadAuthenticationNodes,
        mutation: &ContentIdentityMutation,
    ) -> Result<()> {
        let tree_key = mutation_tree_key(mutation).map_err(|error| self.typed(error))?;
        if let Ok(key) = <[u8; 33]>::try_from(tree_key.as_slice()) {
            let mut record_key = [0; 34];
            record_key[0] = mutation.family as u8;
            record_key[1..].copy_from_slice(&key);
            if let Some((leaf_key, leaf)) = nodes.leaves.range(record_key..).next()
                && leaf_key[0] == mutation.family as u8
            {
                let found = resolving_entry_search(
                    &self.nodes,
                    mutation.family,
                    &leaf.node.node.entries,
                    &tree_key,
                    &mut ProofAdmission::reclaiming(
                        self.admission.clone(),
                        Arc::clone(&self.resident),
                    ),
                )
                .map_err(|error| self.typed(error))?;
                if let Ok(position) = found {
                    #[cfg(feature = "test-seams")]
                    observe_read_authentication_hash(
                        mutation,
                        mutation.logical_key.len()
                            + mutation.logical_value.as_ref().map_or(0, Vec::len),
                    );
                    if self
                        .admission
                        .as_ref()
                        .is_some_and(|accountant| accountant.usage().available == Some(0))
                    {
                        let _ = self.resident.reclaim();
                    }
                    let wanted = mutation
                        .logical_value
                        .as_deref()
                        .map(|value| value_hash(mutation.family, value));
                    return verify_record_witness(
                        mutation.family,
                        wanted,
                        Some(leaf.node.node.entries[position].value_hash),
                    )
                    .map_err(|error| self.typed(error));
                }
            }
        }
        let leaf = self
            .verify_with_batch(
                mutation,
                &mut nodes.nodes,
                &mut nodes.bytes,
                nodes.byte_limit.saturating_sub(nodes.leaf_bytes),
                true,
            )
            .map_err(|error| self.typed(error))?;
        if let Some((storage_key, leaf)) = leaf
            && nodes.nodes.contains_key(&storage_key)
            && let Some(last) = leaf.node.entries.last()
            && let Ok(key) = <[u8; 33]>::try_from(last.key.as_slice())
            && key[0] == HASHED_KEY_TAG
        {
            let mut leaf_key = [0; 34];
            leaf_key[0] = mutation.family as u8;
            leaf_key[1..].copy_from_slice(&key);
            // The page itself already carries its charge; the locator is this
            // set's own allocation, admitted before it exists and simply not
            // kept when the session has no room for it.
            if !nodes.leaves.contains_key(&leaf_key)
                && nodes
                    .bytes
                    .saturating_add(nodes.leaf_bytes)
                    .saturating_add(CACHED_AUTHENTICATED_LEAF_BYTES)
                    <= nodes.byte_limit
                && self.statement_reuse_has_room()
                && nodes.charge.admit(CACHED_AUTHENTICATED_LEAF_BYTES).is_ok()
            {
                nodes.leaf_bytes += CACHED_AUTHENTICATED_LEAF_BYTES;
                nodes
                    .leaves
                    .insert(leaf_key, CachedAuthenticatedLeaf { node: leaf });
            }
        }
        Ok(())
    }

    /// Verify `mutation`, growing `batch` with whatever pages the walk needs
    /// and did not already hold, and answer the proved leaf it ended on. Every
    /// page a charged walk puts into `batch` already carries its own charge
    /// (see `verified_node`), so the caller has nothing left to bill for it.
    fn verify_with_batch(
        &self,
        mutation: &ContentIdentityMutation,
        batch: &mut BTreeMap<[u8; 33], Arc<VerifiedReadNode>>,
        batch_bytes: &mut usize,
        byte_limit: usize,
        remember_record: bool,
    ) -> Result<Option<([u8; 33], Arc<VerifiedReadNode>)>> {
        // One admission for this whole verification: the tree key it builds,
        // each chunk it decodes and each separator it resolves stay admitted
        // until this call returns and drops it.
        let mut admission =
            ProofAdmission::reclaiming(self.admission.clone(), Arc::clone(&self.resident));
        self.verify_with_batch_admission(mutation, batch, batch_bytes, byte_limit, remember_record, &mut admission, None)
    }

    #[allow(clippy::too_many_arguments, reason = "One proof walk retains its existing batch and supplied admission")]
    fn verify_with_batch_admission(
        &self,
        mutation: &ContentIdentityMutation,
        batch: &mut BTreeMap<[u8; 33], Arc<VerifiedReadNode>>,
        batch_bytes: &mut usize,
        byte_limit: usize,
        remember_record: bool,
        admission: &mut ProofAdmission,
        expected_override: Option<Option<[u8;32]>>,
    ) -> Result<Option<([u8; 33], Arc<VerifiedReadNode>)>> {
        self.verify_with_batch_admission_and_checkpoint(
            mutation, batch, batch_bytes, byte_limit, remember_record,
            admission, expected_override, &mut || Ok(()),
        )
    }

    fn verify_with_batch_admission_and_checkpoint(
        &self,
        mutation: &ContentIdentityMutation,
        batch: &mut BTreeMap<[u8; 33], Arc<VerifiedReadNode>>,
        batch_bytes: &mut usize,
        byte_limit: usize,
        remember_record: bool,
        admission: &mut ProofAdmission,
        expected_override: Option<Option<[u8;32]>>,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Option<([u8; 33], Arc<VerifiedReadNode>)>> {
        checkpoint()?;
        admission.admit(mutation_key_capacity(mutation, admission)?)?;
        let wanted_key = mutation_tree_key(mutation)?;
        let wanted_value = expected_override.unwrap_or_else(|| mutation
            .logical_value
            .as_deref()
            .map(|value| value_hash(mutation.family, value)));
        let record_id = node_id(&wanted_key);
        let mut record_storage_key = [0; 33];
        record_storage_key[0] = mutation.family as u8;
        record_storage_key[1..].copy_from_slice(&record_id);

        #[cfg(feature = "test-seams")]
        observe_read_authentication_hash(
            mutation,
            mutation.logical_key.len() + mutation.logical_value.as_ref().map_or(0, Vec::len),
        );

        if let Some(witness) = self
            .verified_records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&record_storage_key)
            .copied()
        {
            verify_record_witness(mutation.family, wanted_value, witness.value_hash)?;
            return Ok(None);
        }
        let mut current = self.roots[mutation.family as usize].root;
        let mut expected_hash = self.roots[mutation.family as usize].hash;
        let mut actual_value = None;
        let mut verified_leaf = None;
        while let Some(id) = current {
            checkpoint()?;
            let verified = self.verified_node(
                mutation.family,
                id,
                expected_hash,
                batch,
                batch_bytes,
                byte_limit,
                admission,
            )?;
            let node = &verified.node;
            if node.level == 0 {
                actual_value = resolving_entry_search(
                    &self.nodes,
                    mutation.family,
                    &node.entries,
                    &wanted_key,
                    admission,
                )?
                .ok()
                .map(|index| node.entries[index].value_hash);
                verified_leaf = Some((node_storage_key(mutation.family, &id), verified.clone()));
                break;
            }
            let mut low = 0;
            let mut high = node.entries.len();
            while low < high {
                let mid = (low + high) / 2;
                checkpoint()?;
                let (key, charge) = resolve_key_charged(
                    &self.nodes,
                    mutation.family,
                    &node.entries[mid].key,
                    admission,
                )?;
                let ordering = identity_key_cmp(&key, &wanted_key)?;
                drop(key);
                admission.release(charge);
                if ordering == Ordering::Less {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            let position = low;
            let Some(child) = node.entries.get(position) else {
                break;
            };
            current = Some(chunk_id(node.level - 1, &child.key));
            expected_hash = child.value_hash;
        }
        if remember_record {
            let mut records = self
                .verified_records
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // READ_VERIFIER_MEMORY_BYTES is reserved for exactly this set for
            // the source handle's lifetime, so the witnesses are already
            // admitted; charging them again would take the same bytes twice.
            if records.len() < VERIFIED_RECORD_LIMIT {
                records.insert(
                    record_storage_key,
                    VerifiedRecordWitness {
                        value_hash: actual_value,
                    },
                );
            }
        }
        verify_record_witness(mutation.family, wanted_value, actual_value)?;
        Ok(verified_leaf)
    }

    fn verify_reader_projection(&self, mutation: &ContentIdentityMutation, admission: &mut ProofAdmission, expected: Option<Option<[u8; 32]>>, checkpoint: &mut dyn FnMut() -> Result<()>) -> Result<()> {
        checkpoint()?;
        admission.admit(mutation_key_capacity(mutation, admission)?)?;
        // Current projection keys are hashed. Hash the borrowed logical key in
        // windows rather than constructing another full logical-key buffer.
        let mut wanted = Vec::with_capacity(33);
        wanted.push(HASHED_KEY_TAG);
        wanted.extend_from_slice(&hash_parts_checkpoint(b"contextdb-logical-record-key-v1", &[&[mutation.family as u8], &mutation.logical_key], checkpoint)?);
        let wanted_value = match expected {
            Some(value) => value,
            None => mutation.logical_value.as_deref().map(|value| hash_parts_checkpoint(b"contextdb-logical-record-value-v1", &[&[mutation.family as u8], value], checkpoint)).transpose()?,
        };
        // Preserve the existing record-work observation, once per proof.
        #[cfg(feature = "test-seams")]
        observe_read_authentication_hash(mutation, mutation.logical_key.len() + mutation.logical_value.as_ref().map_or(0, Vec::len));
        let mut current = self.roots[mutation.family as usize].root;
        let mut expected_hash = self.roots[mutation.family as usize].hash;
        let mut actual = None;
        while let Some(id) = current {
            checkpoint()?;
            let (node, charged) = read_node_charged(&self.nodes, mutation.family, id, admission)?;
            let result = (|| {
                if node.subtree_hash != expected_hash { return Err(Error::Other("logical content page disagrees with its committed proof path".into())); }
                let mut low = 0;
                let mut high = node.entries.len();
                while low < high {
                    let mid = (low + high) / 2;
                    let (key, charge) = resolve_key_charged_checkpoint(&self.nodes, mutation.family, &node.entries[mid].key, admission, checkpoint)?;
                    let ordering = identity_key_cmp_checkpoint(&key, &wanted, checkpoint);
                    drop(key);
                    admission.release(charge);
                    match ordering? {
                        Ordering::Less => low = mid + 1,
                        _ => high = mid,
                    }
                }
                if node.level == 0 {
                    if let Some(entry) = node.entries.get(low) {
                        let (key, charge) = resolve_key_charged_checkpoint(&self.nodes, mutation.family, &entry.key, admission, checkpoint)?;
                        let ordering = identity_key_cmp_checkpoint(&key, &wanted, checkpoint);
                        drop(key);
                        admission.release(charge);
                        if ordering? == Ordering::Equal { actual = Some(entry.value_hash); }
                    }
                    current = None;
                } else if let Some(entry) = node.entries.get(low) {
                    current = Some(chunk_id_checkpoint(node.level - 1, &entry.key, checkpoint)?);
                    expected_hash = entry.value_hash;
                } else { current = None; }
                Ok(())
            })();
            drop(node);
            admission.release(charged);
            result?;
        }
        verify_record_witness(mutation.family, wanted_value, actual)
    }

    fn verified_node(
        &self,
        family: LogicalFamily,
        id: [u8; 32],
        expected_hash: [u8; 32],
        batch: &mut BTreeMap<[u8; 33], Arc<VerifiedReadNode>>,
        batch_bytes: &mut usize,
        byte_limit: usize,
        decode_admission: &mut ProofAdmission,
    ) -> Result<Arc<VerifiedReadNode>> {
        let storage_key = node_storage_key(family, &id);
        // The reuse belongs to the per-record authentication path, which
        // admits through the session's own accountant. The ordered-range walk
        // charges a working pool its caller reserved and measures its own
        // physical work under forced eviction of that pool; it keeps its
        // existing bounded-window accounting untouched by taking neither a
        // chunk from this set nor a place in it.
        let reusable = decode_admission.is_accounted();
        let (verified, from_store) = if let Some(verified) = batch.get(&storage_key) {
            (verified.clone(), false)
        } else if let Some(verified) = reusable.then(|| self.resident.get(&storage_key)).flatten() {
            // One decoded page, one handle, one charge: the statement holds
            // the resident set's own handle, so the resident admission or
            // floor share on it lasts until the last holder -- this batch
            // included -- lets go, however soon the resident entry is evicted.
            (verified, false)
        } else {
            let (node, _charged) = read_node_charged(&self.nodes, family, id, decode_admission)?;
            if decode_admission.reader_budget.is_some() { decode_admission.admit(VERIFIED_READ_NODE_HANDLE_BYTES)?; }
            #[cfg(feature = "test-seams")]
            crate::read_work_probe::note_hashed_bytes(encode_node(&node).len());
            (
                Arc::new(VerifiedReadNode {
                    bytes: node_live_bytes(&node) + VERIFIED_READ_NODE_HANDLE_BYTES,
                    node: Arc::new(node),
                    _charge: AdmittedBytes::with_source(
                        self.admission.as_ref().and_then(crate::memory_accounting::StoreAccountantHandle::retain_if_writer), None),
                    _floor: None,
                }),
                true,
            )
        };
        // The committed root still decides. A resident chunk is accepted only
        // under the hash its parent — and through it the committed root —
        // demands of it on THIS walk, exactly as a chunk just read is.
        if verified.node.subtree_hash != expected_hash {
            return Err(Error::Other(format!(
                "logical content family {} page disagrees with its committed proof path",
                family as u8
            )));
        }
        // A charged walk's batch is statement reuse: it pins whatever it holds
        // until the statement ends, so it grows only while the session keeps
        // its headroom. An unaccounted walk's batch is the ordered walk's own
        // pool, which charges it as cache and keeps its existing bound.
        let mut keep = !batch.contains_key(&storage_key)
            && batch_bytes.saturating_add(verified.bytes) <= byte_limit
            && (!reusable || self.statement_reuse_has_room());
        let verified = if from_store && reusable {
            let decoded = verified.bytes - VERIFIED_READ_NODE_HANDLE_BYTES;
            match self.admission.as_ref().and_then(|_| self.resident.retain(storage_key, &verified)) {
                Some(kept) => {
                    // The resident set's own admission or floor share now
                    // covers this decode, and every holder shares its handle.
                    decode_admission.release(decoded);
                    kept
                }
                // Only this statement keeps the page. The decode was admitted
                // before it was allocated; the handle overhead is admitted
                // now, before the page is kept, and the whole charge moves
                // onto the page so it ends with the page's last holder.
                None if keep
                    && decode_admission
                        .admit(VERIFIED_READ_NODE_HANDLE_BYTES)
                        .is_ok() =>
                {
                    Arc::new(VerifiedReadNode {
                        node: Arc::clone(&verified.node),
                        bytes: verified.bytes,
                        _charge: AdmittedBytes::with_source(
                            self.admission.as_ref().and_then(crate::memory_accounting::StoreAccountantHandle::retain_if_writer),
                            Some(decode_admission.detach(verified.bytes))),
                        _floor: None,
                    })
                }
                None => {
                    // Kept by nobody: the decode's charge stays with this
                    // call and goes back when the call returns.
                    keep = false;
                    verified
                }
            }
        } else {
            verified
        };
        if keep {
            *batch_bytes = batch_bytes.saturating_add(verified.bytes);
            batch.insert(storage_key, verified.clone());
        }
        Ok(verified)
    }

    pub(super) fn visit_relational_index_postings<E>(
        &self,
        table: &str,
        index: &str,
        start: std::ops::Bound<Vec<u8>>,
        reverse: bool,
        proof: &ProofMemory,
        before_page: &mut dyn FnMut() -> std::result::Result<(), E>,
        visit: &mut dyn FnMut(&[u8]) -> std::result::Result<bool, E>,
    ) -> std::result::Result<(), E>
    where
        E: From<Error>,
    {
        let mut batch = OrderedProofBatch::default();
        self.visit_relational_index_postings_in(
            table,
            index,
            start,
            reverse,
            proof,
            &mut batch,
            before_page,
            visit,
        )
    }

    /// `visit_relational_index_postings`, keeping the authenticated pages it
    /// decodes in a caller-owned batch, so several descents of one statement
    /// under the same pool decode the shared upper levels once. The batch's
    /// pages are held under `proof`'s cache limit and charged to that pool,
    /// so the batch must not outlive it.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn visit_relational_index_postings_in<E>(
        &self,
        table: &str,
        index: &str,
        start: std::ops::Bound<Vec<u8>>,
        reverse: bool,
        proof: &ProofMemory,
        shared: &mut OrderedProofBatch,
        before_page: &mut dyn FnMut() -> std::result::Result<(), E>,
        visit: &mut dyn FnMut(&[u8]) -> std::result::Result<bool, E>,
    ) -> std::result::Result<(), E>
    where
        E: From<Error>,
    {
        let namespace_low = posting_tree_key(table, index, POSTING_LOWER_BOUND_TAG, &[]);
        let namespace_high = posting_tree_key(table, index, POSTING_UPPER_BOUND_TAG, &[]);
        let start = match start {
            std::ops::Bound::Included(key) => {
                std::ops::Bound::Included(posting_tree_key(table, index, POSTING_ACTUAL_TAG, &key))
            }
            std::ops::Bound::Excluded(key) => {
                std::ops::Bound::Excluded(posting_tree_key(table, index, POSTING_ACTUAL_TAG, &key))
            }
            std::ops::Bound::Unbounded if reverse => {
                std::ops::Bound::Included(namespace_high.clone())
            }
            std::ops::Bound::Unbounded => std::ops::Bound::Included(namespace_low.clone()),
        };
        let mut stopped = false;
        let bounds = OrderedVisitBounds {
            low: &namespace_low,
            high: &namespace_high,
            start: &start,
            reverse,
            prefix_successor: false,
        };
        let mut leaf = |key: &[u8], _: [u8; 32]| -> std::result::Result<bool, E> {
            let (_, _, tag, order_key) =
                parse_posting_tree_key(key).map_err(|error| E::from(self.typed(error)))?;
            if tag != POSTING_ACTUAL_TAG {
                return Ok(false);
            }
            visit(order_key)
        };
        let visited = self.visit_ordered_node(
            LogicalFamily::SchemaAndIndexes,
            self.roots[LogicalFamily::SchemaAndIndexes as usize].root,
            self.roots[LogicalFamily::SchemaAndIndexes as usize].hash,
            &bounds,
            &mut shared.nodes,
            &mut shared.bytes,
            &mut stopped,
            proof,
            before_page,
            &mut leaf,
        );
        #[cfg(feature = "test-seams")]
        observe_read_proof_memory(proof.peak());
        visited
    }

    /// Seek the first authenticated posting at a leading prefix, or strictly
    /// beyond that prefix's upper edge. The upper edge compares only the
    /// encoded leading components, so trailing index columns are skipped too.
    /// This is a transient seek boundary; no stored key or format changes.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn visit_relational_index_group_first_in<E>(
        &self,
        table: &str,
        index: &str,
        leading: &[u8],
        after_group: bool,
        proof: &ProofMemory,
        shared: &mut OrderedProofBatch,
        visit: &mut dyn FnMut(&[u8]) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E>
    where
        E: From<Error>,
    {
        let bytes = (10usize
            .saturating_add(table.len())
            .saturating_add(index.len()))
        .saturating_mul(3)
        .saturating_add(leading.len());
        let Some(_bounds_charge) = proof.charge_scratch(bytes) else {
            return Ok(());
        };
        let low = posting_tree_key(table, index, POSTING_LOWER_BOUND_TAG, &[]);
        let high = posting_tree_key(table, index, POSTING_UPPER_BOUND_TAG, &[]);
        let key = posting_tree_key(table, index, POSTING_ACTUAL_TAG, leading);
        let start = if after_group {
            std::ops::Bound::Excluded(key)
        } else {
            std::ops::Bound::Included(key)
        };
        let bounds = OrderedVisitBounds {
            low: &low,
            high: &high,
            start: &start,
            reverse: false,
            prefix_successor: after_group,
        };
        let mut stopped = false;
        let mut leaf = |key: &[u8], _: [u8; 32]| {
            let (_, _, tag, order) =
                parse_posting_tree_key(key).map_err(|error| E::from(self.typed(error)))?;
            if tag == POSTING_ACTUAL_TAG {
                visit(order)?;
            }
            Ok(false)
        };
        self.visit_ordered_node(
            LogicalFamily::SchemaAndIndexes,
            self.roots[LogicalFamily::SchemaAndIndexes as usize].root,
            self.roots[LogicalFamily::SchemaAndIndexes as usize].hash,
            &bounds,
            &mut shared.nodes,
            &mut shared.bytes,
            &mut stopped,
            proof,
            &mut || Ok(()),
            &mut leaf,
        )
    }

    /// Walk one stored history's committed range in its authored order,
    /// proving every chunk against the pinned root. `visit` receives each
    /// member's stored coordinate and committed value hash and returns
    /// whether to continue. Returns `true` only when the walk covered the
    /// whole range or `visit` itself asked to stop; `false` means the proof
    /// pool could not hold the walk, which says nothing about the history.
    pub(super) fn visit_history<E>(
        &self,
        kind: HistoryKind,
        reverse: bool,
        proof: &ProofMemory,
        before_page: &mut dyn FnMut() -> std::result::Result<(), E>,
        visit: &mut OrderedLeafVisit<'_, E>,
    ) -> std::result::Result<bool, E>
    where
        E: From<Error>,
    {
        self.visit_history_from(
            kind,
            std::ops::Bound::Unbounded,
            reverse,
            proof,
            before_page,
            visit,
        )
    }

    /// Seek within one authenticated history range at an addressed coordinate.
    /// The proof covers the first selected member (or its absence), so callers
    /// can stop there without reading unrelated primary history records.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn visit_history_from<E>(
        &self,
        kind: HistoryKind,
        start: std::ops::Bound<Vec<u8>>,
        reverse: bool,
        proof: &ProofMemory,
        before_page: &mut dyn FnMut() -> std::result::Result<(), E>,
        visit: &mut OrderedLeafVisit<'_, E>,
    ) -> std::result::Result<bool, E>
    where
        E: From<Error>,
    {
        let coordinate_bytes = match &start {
            std::ops::Bound::Included(key) | std::ops::Bound::Excluded(key) => key.len(),
            std::ops::Bound::Unbounded => 0,
        };
        let bounds_bytes = coordinate_bytes.checked_add(8)
            .and_then(|bytes| bytes.checked_add(3 * std::mem::size_of::<Vec<u8>>()))
            .ok_or_else(|| E::from(self.corrupt("history bound layout overflow")))?;
        let Some(_bounds_charge) = proof.charge_scratch(bounds_bytes) else { return Ok(false); };
        let low = history_tree_key(kind, &[]);
        let mut high = low.clone();
        *high.last_mut().expect("history kind byte") += 1;
        let mut tree_key = |coordinate: &[u8]| -> std::result::Result<Vec<u8>, E> {
            let capacity = coordinate.len().checked_add(2).ok_or_else(|| E::from(self.corrupt("history key overflow")))?;
            let mut key = Vec::with_capacity(capacity);
            key.extend_from_slice(&[ORDERED_HISTORY_KEY_TAG, kind as u8]);
            for window in coordinate.chunks(64 * 1024) { before_page()?; key.extend_from_slice(window); }
            Ok(key)
        };
        let start = match start {
            std::ops::Bound::Included(coordinate) => {
                std::ops::Bound::Included(tree_key(&coordinate)?)
            }
            std::ops::Bound::Excluded(coordinate) => {
                std::ops::Bound::Excluded(tree_key(&coordinate)?)
            }
            std::ops::Bound::Unbounded if reverse => {
                std::ops::Bound::Included(high.clone())
            }
            std::ops::Bound::Unbounded => std::ops::Bound::Included(low.clone()),
        };
        let bounds = OrderedVisitBounds {
            low: &low,
            high: &high,
            start: &start,
            reverse,
            prefix_successor: false,
        };
        let mut asked_to_stop = false;
        let mut leaf = |key: &[u8], value_hash: [u8; 32]| -> std::result::Result<bool, E> {
            let continued = visit(&key[2..], value_hash)?;
            asked_to_stop = !continued;
            Ok(continued)
        };
        let mut batch = BTreeMap::new();
        let mut bytes = 0;
        let mut stopped = false;
        let family = LogicalFamily::ChangeAndDdl;
        self.visit_ordered_node(
            family,
            self.roots[family as usize].root,
            self.roots[family as usize].hash,
            &bounds,
            &mut batch,
            &mut bytes,
            &mut stopped,
            proof,
            before_page,
            &mut leaf,
        )?;
        drop(batch);
        if let Some(error) = proof.take_refusal() {
            return Err(E::from(error));
        }
        Ok(!stopped || asked_to_stop)
    }

    #[allow(clippy::too_many_arguments)]
    fn visit_ordered_node<E>(
        &self,
        family: LogicalFamily,
        id: Option<[u8; 32]>,
        expected_hash: [u8; 32],
        bounds: &OrderedVisitBounds<'_>,
        batch: &mut BTreeMap<[u8; 33], Arc<VerifiedReadNode>>,
        batch_bytes: &mut usize,
        stopped: &mut bool,
        proof: &ProofMemory,
        before_page: &mut dyn FnMut() -> std::result::Result<(), E>,
        visit: &mut OrderedLeafVisit<'_, E>,
    ) -> std::result::Result<(), E>
    where
        E: From<Error>,
    {
        let Some(id) = id else { return Ok(()) };
        if proof.exhausted() {
            *stopped = true;
            return Ok(());
        }
        before_page()?;
        // Admit the actual stored page before decoding it. The encoded bytes
        // are borrowed from Redb; only the decode is this walk's allocation,
        // and its size is in the stored header.
        let cached = batch.contains_key(&node_storage_key(family, &id));
        let page_scratch = if cached {
            None
        } else {
            let scratch = stored_node_precharge_bytes(&self.nodes, family, id)
                .map_err(|error| E::from(self.typed(error)))?;
            let Some(charge) = proof
                .charge_or_exhaust(scratch)
                .map_err(|error| E::from(error))?
            else {
                *stopped = true;
                return Ok(());
            };
            Some(charge)
        };
        #[cfg(feature = "test-seams")]
        if !cached {
            crate::read_work_probe::note_ordered_proof_node_read();
        }
        let verified = self
            .verified_node(
                family,
                id,
                expected_hash,
                batch,
                batch_bytes,
                proof.cache_limit(),
                &mut ProofAdmission::unaccounted(),
            )
            .map_err(|error| E::from(self.typed(error)))?;
        drop(page_scratch);
        proof.set_cached(*batch_bytes);
        // This frame holds the chunk while it descends, so the depth of the
        // walk is charged, not assumed. A retained page is already charged as
        // cache and is not counted twice.
        let _frame = if batch.contains_key(&node_storage_key(family, &id)) {
            None
        } else {
            match proof
                .charge_or_exhaust(verified.bytes)
                .map_err(|error| E::from(error))?
            {
                Some(charge) => Some(charge),
                None => {
                    *stopped = true;
                    return Ok(());
                }
            }
        };
        let node = &verified.node;
        if node.level > 0 {
            for ordinal in 0..node.entries.len() {
                let position = if bounds.reverse { node.entries.len() - 1 - ordinal } else { ordinal };
                let child = &node.entries[position];
                if child.key.first() == Some(&KEY_REFERENCE_TAG) {
                    before_page()?;
                }
                let upper_live = referenced_key_live_bytes(&child.key)
                    .map_err(|error| E::from(self.typed(error)))?;
                let Some(upper_charge) = proof
                    .charge_or_exhaust(upper_live)
                    .map_err(|error| E::from(error))?
                else {
                    *stopped = true;
                    break;
                };
                let upper = checkpoint_operation(before_page, |checkpoint| resolve_key_checkpoint(&self.nodes, family, &child.key, checkpoint).map_err(|error| self.typed(error)))?;
                let before_start = checkpoint_operation(before_page, |checkpoint| bounds.page_before_start_checkpoint(&upper, checkpoint).map_err(|error| self.typed(error)))?;
                drop(upper);
                drop(upper_charge);
                if before_start {
                    continue;
                }
                if position > 0 {
                    let prior = &node.entries[position - 1].key;
                    if prior.first() == Some(&KEY_REFERENCE_TAG) {
                        before_page()?;
                    }
                    let lower_live = referenced_key_live_bytes(prior)
                        .map_err(|error| E::from(self.typed(error)))?;
                    let Some(lower_charge) = proof
                        .charge_or_exhaust(lower_live)
                        .map_err(|error| E::from(error))?
                    else {
                        *stopped = true;
                        break;
                    };
                    let lower = checkpoint_operation(before_page, |checkpoint| resolve_key_checkpoint(&self.nodes, family, prior, checkpoint).map_err(|error| self.typed(error)))?;
                    let after_start = checkpoint_operation(before_page, |checkpoint| bounds.page_after_start_checkpoint(&lower, checkpoint).map_err(|error| self.typed(error)))?;
                    drop(lower);
                    drop(lower_charge);
                    if after_start {
                        continue;
                    }
                }
                self.visit_ordered_node(
                    family,
                    Some(checkpoint_operation(before_page, |checkpoint| chunk_id_checkpoint(node.level - 1, &child.key, checkpoint))?),
                    child.value_hash,
                    bounds,
                    batch,
                    batch_bytes,
                    stopped,
                    proof,
                    before_page,
                    visit,
                )?;
                if *stopped {
                    break;
                }
            }
        } else {
            for ordinal in 0..node.entries.len() {
                let position = if bounds.reverse { node.entries.len() - 1 - ordinal } else { ordinal };
                let entry = &node.entries[position];
                if is_key_reference(entry) {
                    before_page()?;
                }
                let key_live = referenced_key_live_bytes(&entry.key)
                    .map_err(|error| E::from(self.typed(error)))?;
                let Some(_charge) = proof
                    .charge_or_exhaust(key_live)
                    .map_err(|error| E::from(error))?
                else {
                    *stopped = true;
                    break;
                };
                let key = checkpoint_operation(before_page, |checkpoint| resolve_key_checkpoint(&self.nodes, family, &entry.key, checkpoint).map_err(|error| self.typed(error)))?;
                if !checkpoint_operation(before_page, |checkpoint| bounds.includes_checkpoint(&key, checkpoint).map_err(|error| self.typed(error)))?
                {
                    continue;
                }
                before_page()?;
                if !visit(&key, entry.value_hash)? {
                    *stopped = true;
                    break;
                }
            }
        }
        Ok(())
    }
}

/// Called for each leaf entry an ordered walk reaches, with the entry's
/// resolved tree key and committed value hash; returns whether to continue.
type OrderedLeafVisit<'a, E> = dyn FnMut(&[u8], [u8; 32]) -> std::result::Result<bool, E> + 'a;

struct OrderedVisitBounds<'a> {
    low: &'a [u8],
    high: &'a [u8],
    start: &'a std::ops::Bound<Vec<u8>>,
    reverse: bool,
    prefix_successor: bool,
}

impl OrderedVisitBounds<'_> {
    fn includes_checkpoint(&self, key: &[u8], checkpoint: &mut dyn FnMut() -> Result<()>) -> Result<bool> {
        if key.first() != Some(&ORDERED_HISTORY_KEY_TAG) { checkpoint()?; return self.includes(key); }
        if identity_key_cmp_checkpoint(key, self.low, checkpoint)? != Ordering::Greater || identity_key_cmp_checkpoint(key, self.high, checkpoint)? != Ordering::Less { return Ok(false); }
        let compare = match self.start {
            std::ops::Bound::Included(start) | std::ops::Bound::Excluded(start) => identity_key_cmp_checkpoint(key, start, checkpoint)?,
            std::ops::Bound::Unbounded => return Ok(true),
        };
        Ok(match (self.reverse, self.start) {
            (true, std::ops::Bound::Included(_)) => compare.is_le(),
            (true, _) => compare.is_lt(),
            (false, std::ops::Bound::Included(_)) => compare.is_ge(),
            (false, _) => compare.is_gt(),
        })
    }
    fn page_before_start_checkpoint(&self, key: &[u8], checkpoint: &mut dyn FnMut() -> Result<()>) -> Result<bool> {
        if key.first() != Some(&ORDERED_HISTORY_KEY_TAG) { checkpoint()?; return self.page_before_start(key); }
        if self.reverse { return Ok(identity_key_cmp_checkpoint(key, self.low, checkpoint)? != Ordering::Greater); }
        Ok(match self.start {
            std::ops::Bound::Included(start) => identity_key_cmp_checkpoint(key, start, checkpoint)?.is_lt(),
            std::ops::Bound::Excluded(start) => identity_key_cmp_checkpoint(key, start, checkpoint)?.is_le(),
            std::ops::Bound::Unbounded => false,
        })
    }
    fn page_after_start_checkpoint(&self, key: &[u8], checkpoint: &mut dyn FnMut() -> Result<()>) -> Result<bool> {
        if key.first() != Some(&ORDERED_HISTORY_KEY_TAG) { checkpoint()?; return self.page_after_start(key); }
        if !self.reverse { return Ok(identity_key_cmp_checkpoint(key, self.high, checkpoint)? != Ordering::Less); }
        Ok(match self.start {
            std::ops::Bound::Included(start) => identity_key_cmp_checkpoint(key, start, checkpoint)?.is_gt(),
            std::ops::Bound::Excluded(start) => identity_key_cmp_checkpoint(key, start, checkpoint)?.is_ge(),
            std::ops::Bound::Unbounded => false,
        })
    }
    fn compare_start(&self, key: &[u8], start: &[u8]) -> Result<Ordering> {
        if !self.prefix_successor {
            return identity_key_cmp(key, start);
        }
        if key.first() != Some(&ORDERED_POSTING_KEY_TAG) {
            return identity_key_cmp(key, start);
        }
        let (table, index, tag, order) = parse_posting_tree_key(key)?;
        let (start_table, start_index, start_tag, prefix) = parse_posting_tree_key(start)?;
        let namespace = table
            .cmp(start_table)
            .then_with(|| index.cmp(start_index))
            .then_with(|| tag.cmp(&start_tag));
        if namespace != Ordering::Equal {
            return Ok(namespace);
        }
        BorrowedPostingCursor::open(order)?.validate()?;
        BorrowedPostingCursor::open(prefix)?.validate()?;
        let mut actual = BorrowedPostingCursor::open(order)?;
        let mut bound = BorrowedPostingCursor::open(prefix)?;
        for _ in 0..actual.count.min(bound.count) {
            let ordering = borrowed_directed_order(actual.next()?, bound.next()?)?;
            if ordering != Ordering::Equal {
                return Ok(ordering);
            }
        }
        // Every longer key sharing the prefix lies before its successor.
        Ok(if actual.count < bound.count {
            Ordering::Less
        } else {
            Ordering::Equal
        })
    }

    fn includes(&self, key: &[u8]) -> Result<bool> {
        if identity_key_cmp(key, self.low)? != Ordering::Greater
            || identity_key_cmp(key, self.high)? != Ordering::Less
        {
            return Ok(false);
        }
        let compare = match self.start {
            std::ops::Bound::Included(start) => self.compare_start(key, start)?,
            std::ops::Bound::Excluded(start) => self.compare_start(key, start)?,
            std::ops::Bound::Unbounded => Ordering::Equal,
        };
        Ok(if self.reverse {
            match self.start {
                std::ops::Bound::Included(_) => compare.is_le(),
                std::ops::Bound::Excluded(_) => compare.is_lt(),
                std::ops::Bound::Unbounded => true,
            }
        } else {
            match self.start {
                std::ops::Bound::Included(_) => compare.is_ge(),
                std::ops::Bound::Excluded(_) => compare.is_gt(),
                std::ops::Bound::Unbounded => true,
            }
        })
    }

    fn page_before_start(&self, last: &[u8]) -> Result<bool> {
        if self.reverse {
            return Ok(identity_key_cmp(last, self.low)? != Ordering::Greater);
        }
        Ok(match self.start {
            std::ops::Bound::Included(start) => self.compare_start(last, start)?.is_lt(),
            std::ops::Bound::Excluded(start) => self.compare_start(last, start)?.is_le(),
            std::ops::Bound::Unbounded => false,
        })
    }

    fn page_after_start(&self, first: &[u8]) -> Result<bool> {
        if !self.reverse {
            return Ok(identity_key_cmp(first, self.high)? != Ordering::Less);
        }
        Ok(match self.start {
            std::ops::Bound::Included(start) => self.compare_start(first, start)?.is_gt(),
            std::ops::Bound::Excluded(start) => self.compare_start(first, start)?.is_ge(),
            std::ops::Bound::Unbounded => false,
        })
    }
}

fn verify_record_witness(
    family: LogicalFamily,
    wanted: Option<[u8; 32]>,
    actual: Option<[u8; 32]>,
) -> Result<()> {
    match (wanted, actual) {
        (Some(wanted), Some(actual)) if wanted == actual => Ok(()),
        (Some(_), Some(_)) => Err(Error::Other(format!(
            "logical content family {} record value does not match its authenticated page",
            family as u8
        ))),
        (None, Some(_)) => Err(Error::Other(format!(
            "logical content family {} contains a record expected to be absent",
            family as u8
        ))),
        (None, None) => Ok(()),
        (Some(_), None) => Err(Error::Other(format!(
            "logical content family {} is missing the requested committed record",
            family as u8
        ))),
    }
}

/// Compose the eleven published fields from the family roots.
///
/// Each logical record lives in exactly one family, so a field that covers more
/// than one kind of content is DERIVED from the roots of the families that hold
/// it rather than from duplicate members. What each field covers is unchanged:
///
/// * `schema_and_indexes` covers table schema records and ordered index
///   postings, so it composes the schema root with the posting root;
/// * `policy` covers table schema records only;
/// * `sync` covers table schema records, declared configuration and per-row
///   sync-source records;
/// * `configuration` covers declared configuration only;
/// * `status` covers table schema records, declared configuration and the
///   operational records (index prefix counts, sink queue, audits);
/// * `complete` covers the whole image, and is now taken over the eleven
///   PUBLISHED fields, which is what its documented meaning requires once those
///   fields are derived.
///
/// The postings root appears in `schema_and_indexes` and nowhere else, so an
/// ordinary indexed-row append cannot move `policy`.
fn digest_from_roots(roots: [[u8; 32]; 11]) -> DirectTypedImageDigest {
    let schema = &roots[SCHEMA_RECORD_FAMILY as usize];
    let postings = &roots[LogicalFamily::SchemaAndIndexes as usize];
    let configuration = &roots[LogicalFamily::Configuration as usize];
    let sync_sources = &roots[LogicalFamily::Sync as usize];
    let operational = &roots[LogicalFamily::Status as usize];
    let published = [
        roots[LogicalFamily::Relational as usize],
        roots[LogicalFamily::Graph as usize],
        roots[LogicalFamily::F32Vectors as usize],
        roots[LogicalFamily::Sq8Vectors as usize],
        roots[LogicalFamily::Sq4Vectors as usize],
        hash_parts(DIGEST_DOMAIN_SCHEMA_AND_INDEXES, &[schema, postings]),
        hash_parts(DIGEST_DOMAIN_POLICY, &[schema]),
        hash_parts(DIGEST_DOMAIN_SYNC, &[schema, configuration, sync_sources]),
        roots[LogicalFamily::ChangeAndDdl as usize],
        hash_parts(DIGEST_DOMAIN_CONFIGURATION, &[configuration]),
        hash_parts(DIGEST_DOMAIN_STATUS, &[schema, configuration, operational]),
    ];
    DirectTypedImageDigest {
        relational: published[0],
        graph: published[1],
        f32_vectors: published[2],
        sq8_vectors: published[3],
        sq4_vectors: published[4],
        schema_and_indexes: published[5],
        policy: published[6],
        sync: published[7],
        change_and_ddl: published[8],
        configuration: published[9],
        status: published[10],
        complete: complete_hash(&published),
    }
}

pub(super) fn content_size_root_in_read(read: &redb::ReadTransaction) -> Result<[u8; 32]> {
    let roots = read
        .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let value = roots
        .get(LogicalFamily::Configuration as u8)
        .map_err(RedbPersistence::storage_error)?
        .ok_or_else(|| Error::Other("committed configuration root is missing".into()))?;
    decode_published_root(LogicalFamily::Configuration, value.value())?
        .content_size_root
        .ok_or_else(|| Error::Other("committed content-size root is missing".into()))
}

/// The existing vector-root field authenticates both native records and
/// the schema-owned incarnations that select their meaning.
fn vector_schema_pages_hash(pages: [u8; 32], schema: [u8; 32]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"contextdb-vector-pages-schema-root-v1");
    hash.update(&pages);
    hash.update(&schema);
    *hash.finalize().as_bytes()
}

pub(super) fn vector_schema_pages_root_in_write(
    write: &redb::WriteTransaction,
) -> Result<[u8; 32]> {
    let pages = write
        .table_root_fingerprint(super::vector_paged::PAGES)
        .map_err(RedbPersistence::storage_error)?
        .unwrap_or([0; 32]);
    let schema = write
        .table_root_fingerprint(super::META_TABLE)
        .map_err(RedbPersistence::storage_error)?
        .unwrap_or([0; 32]);
    Ok(vector_schema_pages_hash(pages, schema))
}

pub(super) fn vector_schema_pages_root_in_read(read: &redb::ReadTransaction) -> Result<[u8; 32]> {
    let pages = read
        .table_root_fingerprint(super::vector_paged::PAGES)
        .map_err(RedbPersistence::storage_error)?
        .unwrap_or([0; 32]);
    let schema = read
        .table_root_fingerprint(super::META_TABLE)
        .map_err(RedbPersistence::storage_error)?
        .unwrap_or([0; 32]);
    Ok(vector_schema_pages_hash(pages, schema))
}

/// Whether a paged bind ever recorded the schema half of its binding.
pub(super) fn vector_schema_meta_root_bound_in_read(read: &redb::ReadTransaction) -> Result<bool> {
    let recovery = match read.open_table(super::writer_recovery::RECOVERY_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    Ok(recovery
        .get(VECTOR_SCHEMA_META_ROOT_KEY)
        .map_err(RedbPersistence::storage_error)?
        .is_some())
}

/// Whether the committed schema half of the paged binding still matches.
pub(super) fn vector_schema_meta_root_matches_in_read(read: &redb::ReadTransaction) -> Result<bool> {
    let schema = read
        .table_root_fingerprint(super::META_TABLE)
        .map_err(RedbPersistence::storage_error)?
        .unwrap_or([0; 32]);
    let recovery = read
        .open_table(super::writer_recovery::RECOVERY_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    Ok(recovery
        .get(VECTOR_SCHEMA_META_ROOT_KEY)
        .map_err(RedbPersistence::storage_error)?
        .is_some_and(|bound| bound.value() == schema))
}

pub(super) fn vector_pages_root_in_read(read: &redb::ReadTransaction) -> Result<[u8; 32]> {
    let roots = read
        .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let value = roots
        .get(LogicalFamily::Configuration as u8)
        .map_err(RedbPersistence::storage_error)?
        .ok_or_else(|| Error::Other("committed configuration root is missing".into()))?;
    decode_published_root(LogicalFamily::Configuration, value.value())?
        .vector_pages_root
        .ok_or_else(|| Error::Other("committed vector page root is missing".into()))
}
/// The schema half of the paged binding, kept on its own so a damaged page
/// table can be told apart from damaged schema metadata.
pub(super) const VECTOR_SCHEMA_META_ROOT_KEY: &str = "vector_schema_meta_root_v1";

pub(super) fn bind_vector_pages_root_in_write(write: &redb::WriteTransaction) -> Result<()> {
    let fingerprint = vector_schema_pages_root_in_write(write)?;
    bind_vector_schema_meta_root_in_write(write)?;
    let mut roots = write
        .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let mut root = {
        let value = roots
            .get(LogicalFamily::Configuration as u8)
            .map_err(RedbPersistence::storage_error)?;
        match value {
            Some(value) => decode_published_root(LogicalFamily::Configuration, value.value())?,
            None => empty_published_root(LogicalFamily::Configuration),
        }
    };
    root.vector_pages_root = Some(fingerprint);
    roots
        .insert(
            LogicalFamily::Configuration as u8,
            encode_published_root(&root).as_slice(),
        )
        .map_err(RedbPersistence::storage_error)?;
    Ok(())
}

/// Carry an existing schema-half binding forward to this commit's metadata.
/// A store that never bound a paged generation is left without one.
pub(super) fn rebind_vector_schema_meta_root_if_bound_in_write(
    write: &redb::WriteTransaction,
) -> Result<()> {
    let bound = {
        let recovery = write
            .open_table(super::writer_recovery::RECOVERY_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        recovery
            .get(VECTOR_SCHEMA_META_ROOT_KEY)
            .map_err(RedbPersistence::storage_error)?
            .is_some()
    };
    if bound {
        bind_vector_schema_meta_root_in_write(write)?;
    }
    Ok(())
}

/// Record the schema half of the paged binding beside every combined bind.
pub(super) fn bind_vector_schema_meta_root_in_write(write: &redb::WriteTransaction) -> Result<()> {
    let schema = write
        .table_root_fingerprint(super::META_TABLE)
        .map_err(RedbPersistence::storage_error)?
        .unwrap_or([0; 32]);
    {
        let mut recovery = write
            .open_table(super::writer_recovery::RECOVERY_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        let unchanged = recovery
            .get(VECTOR_SCHEMA_META_ROOT_KEY)
            .map_err(RedbPersistence::storage_error)?
            .is_some_and(|existing| existing.value() == schema);
        if !unchanged {
            recovery
                .insert(VECTOR_SCHEMA_META_ROOT_KEY, schema.as_slice())
                .map_err(RedbPersistence::storage_error)?;
        }
    }
    Ok(())
}

impl RedbPersistence {
    #[cfg(feature = "test-seams")]
    pub(crate) fn reset_content_identity_work_for_test(&self) {
        reset_work_for_test();
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn take_content_identity_work_for_test(&self) -> (u64, u64, u64) {
        take_work_for_test()
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn take_content_identity_work_bytes_for_test(&self) -> (u64, u64) {
        take_work_bytes_for_test()
    }

    /// Attach the owning database's accountant. Called once, while the
    /// database is being built, before any write is served.
    pub(crate) fn attach_memory_accountant(
        &self,
        accountant: crate::memory_accounting::AccountantHandle,
    ) {
        let _ = self.proof_accountant.set(super::ProofAccountantSlot::attached(accountant));
    }

    pub(crate) fn proof_accountant(
        &self,
    ) -> Option<crate::memory_accounting::AccountantHandle> {
        self.proof_accountant.get().map(super::ProofAccountantSlot::accountant)
    }

    // Private returned-loader provenance. Matching retains only the immutable
    // attach-time role; a mismatched supplied reader remains its own funder.
    pub(crate) fn proof_accountant_handle_for(
        &self,
        accountant: &crate::memory_accounting::AccountantHandle,
    ) -> Option<crate::memory_accounting::StoreAccountantHandle> {
        self.proof_accountant.get().and_then(|slot| slot.original_handle_for(accountant))
    }

    pub(crate) fn prepare_proof_owner(&self) -> Result<()> {
        if let Some(slot) = self.proof_accountant.get() { slot.prepare()?; }
        Ok(())
    }

    pub(super) fn proof_scratch_owner(
        &self,
    ) -> Option<std::sync::Weak<super::writer_file::LockedWriterFile>> {
        self.writer_file
            .read()
            .ok()
            .and_then(|owner| owner.as_ref().map(Arc::downgrade))
    }

    /// One transaction's proof admission, charged to the database accountant.
    pub(crate) fn proof_admission(&self) -> OwnedProofAdmission {
        OwnedProofAdmission::new(ProofAdmission::new(self.proof_accountant()).with_scratch_owner(self.proof_scratch_owner()))
    }

    pub(crate) fn require_current_content_identity(&self) -> Result<()> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let metadata = read
                .open_table(CONTENT_IDENTITY_METADATA_TABLE)
                .map_err(Self::storage_error)?;
            let version = metadata
                .get(IDENTITY_VERSION_KEY)
                .map_err(Self::storage_error)?
                .ok_or_else(|| Error::Other("logical content identity is absent".to_owned()))?;
            if version.value() != IDENTITY_VERSION {
                return Err(Error::Other(
                    "logical content identity has an unsupported internal representation"
                        .to_owned(),
                ));
            }
            Ok(())
        })
    }

    pub(super) fn initialize_content_identity_in_write(
        write_txn: &redb::WriteTransaction,
    ) -> Result<()> {
        Self::initialize_content_identity_in_write_admitted(
            write_txn,
            &mut ProofAdmission::unaccounted(),
        )
    }

    /// The same initialisation, with the empty proof it writes admitted through
    /// the owning accountant. An export destination is created with one of
    /// these before its first artifact byte, so the destination's own proof is
    /// inside `MEMORY_LIMIT` from the format marker onwards.
    pub(super) fn initialize_content_identity_in_write_admitted(
        write_txn: &redb::WriteTransaction,
        admission: &mut ProofAdmission,
    ) -> Result<()> {
        Self::initialize_ddl_selectors_in_write(write_txn)?;
        super::content_size::initialize(write_txn)?;
        let mut nodes = write_txn
            .open_table(CONTENT_IDENTITY_NODES_TABLE)
            .map_err(Self::storage_error)?;
        Self::delete_proof_nodes_in_admitted_batches(&mut nodes, None, admission)?;
        drop(nodes);
        let mut metadata = write_txn
            .open_table(CONTENT_IDENTITY_METADATA_TABLE)
            .map_err(Self::storage_error)?;
        metadata
            .insert(IDENTITY_VERSION_KEY, IDENTITY_VERSION)
            .map_err(Self::storage_error)?;
        drop(metadata);
        let mut roots = write_txn
            .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
            .map_err(Self::storage_error)?;
        for family in LogicalFamily::ALL {
            admission.admit(ENCODED_ROOT_BYTES)?;
            let encoded = encode_published_root(&empty_published_root(family));
            roots
                .insert(family as u8, encoded.as_slice())
                .map_err(Self::storage_error)?;
            admission.release(ENCODED_ROOT_BYTES);
        }
        Ok(())
    }

    /// Delete the selected families' proof nodes and publish their empty roots,
    /// inside the write transaction the caller is already committing.
    ///
    /// The deletion is a bounded, admitted WALK, not a materialisation: at most
    /// one batch of selected keys exists at a time, every one of them admitted
    /// through the caller's `ProofAdmission` before it is cloned, so a large
    /// restore or maintenance holds a batch rather than a copy of every proof
    /// chunk of the selected families. A refused batch aborts the whole
    /// transaction with the existing typed budget error and nothing reset.
    pub(super) fn reset_content_identity_families_in_write(
        write_txn: &redb::WriteTransaction,
        families: &[LogicalFamily],
        admission: &mut ProofAdmission,
    ) -> Result<()> {
        let selected = families
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let mut nodes = write_txn
            .open_table(CONTENT_IDENTITY_NODES_TABLE)
            .map_err(Self::storage_error)?;
        for family in &selected {
            Self::delete_proof_nodes_in_admitted_batches(&mut nodes, Some(*family), admission)?;
        }
        drop(nodes);
        let mut roots = write_txn
            .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
            .map_err(Self::storage_error)?;
        for family in families {
            admission.admit(ENCODED_ROOT_BYTES)?;
            let encoded = encode_published_root(&empty_published_root(*family));
            roots
                .insert(*family as u8, encoded.as_slice())
                .map_err(Self::storage_error)?;
            admission.release(ENCODED_ROOT_BYTES);
        }
        Ok(())
    }

    /// One bounded batch of proof-node keys, deleted through the table the
    /// caller already has open. `family` selects one family's contiguous key
    /// range -- a node key begins with its family tag -- and `None` walks the
    /// whole table, which is what a fresh initialisation clears. Each key is
    /// admitted before it is copied out of storage and released as soon as the
    /// batch that held it is removed, so the peak is one batch however many
    /// nodes the store holds.
    fn delete_proof_nodes_in_admitted_batches(
        nodes: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
        family: Option<LogicalFamily>,
        admission: &mut ProofAdmission,
    ) -> Result<()> {
        use std::ops::Bound;
        let lower = family.map(|family| [family as u8]);
        let upper = family
            .and_then(|family| (family as u8).checked_add(1))
            .map(|tag| [tag]);
        loop {
            let mut batch: Vec<Vec<u8>> = Vec::new();
            let mut charged = 0usize;
            let mut batch_bytes = 0usize;
            {
                let bounds = (
                    match lower.as_ref() {
                        Some(low) => Bound::Included(low.as_slice()),
                        None => Bound::Unbounded,
                    },
                    match upper.as_ref() {
                        Some(high) => Bound::Excluded(high.as_slice()),
                        None => Bound::Unbounded,
                    },
                );
                for entry in nodes.range::<&[u8]>(bounds).map_err(Self::storage_error)? {
                    let (key, _) = entry.map_err(Self::storage_error)?;
                    let key = key.value();
                    // Admitted BEFORE the copy exists, so a refusal leaves the
                    // batch exactly as large as what was already admitted.
                    let charge = key.len().saturating_add(std::mem::size_of::<Vec<u8>>());
                    if let Err(error) = admission.admit(charge) {
                        admission.release(charged);
                        return Err(error);
                    }
                    charged = charged.saturating_add(charge);
                    batch_bytes = batch_bytes.saturating_add(key.len());
                    batch.push(key.to_vec());
                    if batch.len() >= PROOF_RESET_BATCH_KEYS
                        || batch_bytes >= PROOF_RESET_BATCH_BYTES
                    {
                        break;
                    }
                }
            }
            if batch.is_empty() {
                admission.release(charged);
                return Ok(());
            }
            for key in &batch {
                nodes.remove(key.as_slice()).map_err(Self::storage_error)?;
            }
            // The batch's keys are gone from the table, so the next batch
            // starts from the same family bound and finds what follows them.
            drop(batch);
            admission.release(charged);
        }
    }

    pub(crate) fn apply_content_identity_mutations_in_write(
        write_txn: &redb::WriteTransaction,
        mutations: &[ContentIdentityMutation],
        admission: &mut ProofAdmission,
    ) -> Result<DirectTypedImageDigest> {
        Self::reconcile_ddl_selectors_in_write(write_txn, mutations, admission)?;
        super::content_size::reconcile_row_heads(write_txn, mutations)?;
        Self::apply_writer_row_heads_in_write(write_txn, mutations)?;
        for (ordinal, mutation) in mutations.iter().enumerate() {
            if mutation.family == SCHEMA_RECORD_FAMILY {
                if mutations[ordinal + 1..].iter().any(|later| {
                    later.family == SCHEMA_RECORD_FAMILY
                        && later.logical_key == mutation.logical_key
                }) {
                    continue;
                }
                let name = std::str::from_utf8(&mutation.logical_key)
                    .map_err(|_| Error::Other("invalid content-size schema name".into()))?;
                if mutation.logical_value.is_some() {
                    super::content_size::ensure_table(write_txn, name)?;
                } else {
                    write_txn
                        .open_table(super::content_size::TOTALS)
                        .map_err(Self::storage_error)?
                        .remove(name)
                        .map_err(Self::storage_error)?;
                }
            }
        }
        let metadata = write_txn
            .open_table(CONTENT_IDENTITY_METADATA_TABLE)
            .map_err(Self::storage_error)?;
        let version = metadata
            .get(IDENTITY_VERSION_KEY)
            .map_err(Self::storage_error)?
            .ok_or_else(|| {
                Error::Other("logical content identity is not initialized".to_owned())
            })?;
        if version.value() != IDENTITY_VERSION {
            return Err(Error::Other(
                "logical content identity has an unsupported version".to_owned(),
            ));
        }
        drop(version);
        drop(metadata);

        if mutations.iter().any(|mutation| {
            matches!(
                mutation.family,
                LogicalFamily::F32Vectors | LogicalFamily::Sq8Vectors | LogicalFamily::Sq4Vectors
            )
        }) {
            Self::write_vector_body_root_fingerprint(write_txn)?;
        }

        for mutation in mutations {
            if mutation.family != LogicalFamily::Configuration {
                continue;
            }
            if let Ok(key) = std::str::from_utf8(&mutation.logical_key) {
                if super::read_image_open_config_key(key) {
                    Self::write_read_open_config_entry(
                        write_txn,
                        key,
                        mutation.logical_value.as_deref(),
                    )?;
                }
                if crate::custody::records::owned_key(key) {
                    Self::write_read_custody_config_entry(
                        write_txn,
                        key,
                        mutation.logical_value.as_deref(),
                    )?;
                }
            }
        }

        let roots_table = write_txn
            .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
            .map_err(Self::storage_error)?;
        let mut roots: [FamilyRoot; 11] =
            std::array::from_fn(|family| empty_root(LogicalFamily::ALL[family]));
        let mut tails: [RootTail; 11] = std::array::from_fn(|_| RootTail {
            history_open: None,
            custody_config_root: None,
            vector_directory_root: None,
            ddl_selector_roots: None,
            content_size_root: None,
            vector_pages_root: None,
        });
        for family in LogicalFamily::ALL {
            let encoded = roots_table
                .get(family as u8)
                .map_err(Self::storage_error)?
                .ok_or_else(|| Error::Other("logical content family root is absent".to_owned()))?;
            let published = decode_published_root(family, encoded.value())?;
            roots[family as usize] = published.identity;
            tails[family as usize] = RootTail {
                history_open: published.history_open,
                custody_config_root: published.custody_config_root,
                vector_directory_root: published.vector_directory_root,
                ddl_selector_roots: published.ddl_selector_roots,
                content_size_root: published.content_size_root,
                vector_pages_root: published.vector_pages_root,
            };
        }
        drop(roots_table);
        let original_roots = roots.clone();
        let original_tails = tails.clone();
        tails[LogicalFamily::ChangeAndDdl as usize].ddl_selector_roots =
            Some(Self::ddl_selector_fingerprints_in_write(write_txn)?);
        // This is the small writer-maintained projection, never the full
        // Configuration table. Read it after all copy updates in this same
        // write transaction and publish its digest with the canonical root.
        let open_config_root = (mutations.is_empty()
            || mutations.iter().any(|mutation| {
                mutation.family == LogicalFamily::Configuration
                    && std::str::from_utf8(&mutation.logical_key)
                        .is_ok_and(super::read_image_open_config_key)
            }))
        .then(|| {
            let table = write_txn
                .open_table(super::READ_OPEN_CONFIG_TABLE)
                .map_err(Self::storage_error)?;
            let mut digest = OpenConfigDigest::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                if key.value() != super::READ_OPEN_CONFIG_ROOT_KEY {
                    digest.push(key.value(), value.value());
                }
            }
            Ok::<_, Error>(digest.finish())
        })
        .transpose()?;
        // Every membership and custody-copy write in this transaction has
        // already folded its changed records into the running set commitments
        // (`write_open_directory_entry`, `write_read_custody_config_entry`,
        // and the rebuilds that clear and re-add them). Publishing reads one
        // fixed-size state per copy, so the cost does not grow with the number
        // of memberships or custody records. It runs for every transaction, so
        // no write path that changes a copy can leave its root behind.
        // Membership-only image-export commits also publish companion roots.
        super::vector_membership_pages::write_fingerprints(write_txn)?;
        let vector_directory_root = Some(
            SetCommitment::load_in_write(write_txn, CompactIndexSet::VectorDirectory)?
                .root_with_vector_body_count(
                    super::vector_membership_pages::referenced_body_count_in_write(write_txn)?,
                ),
        );
        let custody_config_root =
            Some(SetCommitment::load_in_write(write_txn, CompactIndexSet::CustodyIndex)?.root());

        // One durable transaction can stage several physical versions of the
        // same logical record (an UPDATE commonly contributes a remove and a
        // replacement). The Merkle representation is canonical for the final
        // key/value set, so collapse those entries before walking the tree.
        let coalescing_admission_start = admission.held();
        let coalescing_result = (|| -> Result<()> {
            // Retain only unique-key boxes. Every input still receives the
            // original parser validation, including entries later overwritten.
            let mut unique = None;
            let mut node_bytes = 0usize;
            for (ordinal, mutation) in mutations.iter().enumerate() {
                let key_bytes = mutation_key_capacity(mutation, admission)?;
                admission.admit(key_bytes)?;
                let key = mutation_tree_key(mutation)?;
                debug_assert_eq!(key.capacity(), key_bytes);
                drop(key);
                admission.release(key_bytes);
                let replaced = MutationKeyNode::insert(
                    &mut unique,
                    ordinal,
                    mutations,
                    admission,
                    &mut node_bytes,
                )?;
                #[cfg(feature = "test-seams")]
                if mutation.family == LogicalFamily::Status {
                    observe_status_namespace(&mutation.logical_key);
                    if replaced {
                        forget_status_namespace(&mutation.logical_key);
                    }
                }
                #[cfg(not(feature = "test-seams"))]
                let _ = replaced;
            }
            let mut coalesced_bytes = 0usize;
            let Some(unique) = unique else {
                return Ok(());
            };

            // The coalesced directory borrows source records; grouping allocates
            // only exact tuple slices and the selected keys' actual payloads.
            #[cfg(feature = "test-seams")]
            let group_started = std::time::Instant::now();
            type GroupEntry = (Vec<u8>, Option<[u8; 32]>);
            let mut family_counts = [0usize; LogicalFamily::ALL.len()];
            unique.visit(&mut |node| {
                let family = mutations[node.first].family as usize;
                family_counts[family] = family_counts[family]
                    .checked_add(1)
                    .ok_or_else(|| proof_allocation_overflow(admission))?;
                Ok(())
            })?;
            // The fixed family directory lives on the stack. Each nonempty list
            // gets one exact slice allocation, including the tuple's alignment;
            // neither map nodes nor geometric Vec growth are needed for grouping.
            let mut grouped_bytes = 0usize;
            for count in family_counts {
                let layout = std::alloc::Layout::array::<GroupEntry>(count)
                    .map_err(|_| proof_allocation_overflow(admission))?;
                grouped_bytes = grouped_bytes
                    .checked_add(layout.size())
                    .ok_or_else(|| proof_allocation_overflow(admission))?;
            }
            admission.admit(grouped_bytes)?;
            let mut grouped: [EntryEdits; LogicalFamily::ALL.len()] =
                std::array::from_fn(|family| {
                    let count = family_counts[family];
                    let mut entries = Box::<[GroupEntry]>::new_uninit_slice(count);
                    for entry in &mut entries {
                        entry.write((Vec::new(), None));
                    }
                    // SAFETY: every slot was initialized above, with no fallible
                    // work between initialization and conversion. A boxed slice
                    // converts without reallocating and gives Vec capacity equal
                    // to its length, exactly the layout admitted before allocation.
                    let entries = unsafe { entries.assume_init() }.into_vec();
                    debug_assert_eq!(entries.capacity(), count);
                    entries
                });
            let mut family_positions = [0usize; LogicalFamily::ALL.len()];
            unique.visit(&mut |node| {
                // First-key ownership and final-value replacement match the map.
                let mutation = &mutations[node.first];
                let final_mutation = &mutations[node.last];
                let key_bytes = mutation_key_capacity(mutation, admission)?;
                let next_bytes = coalesced_bytes
                    .checked_add(key_bytes)
                    .ok_or_else(|| proof_allocation_overflow(admission))?;
                admission.admit(key_bytes)?;
                let key = mutation_tree_key(mutation)?;
                debug_assert_eq!(key.capacity(), key_bytes);
                coalesced_bytes = next_bytes;
                let value = final_mutation
                    .logical_value
                    .as_deref()
                    .map(|value| value_hash(mutation.family, value));
                let family = mutation.family as usize;
                grouped[family][family_positions[family]] = (key, value);
                family_positions[family] += 1;
                Ok(())
            })?;
            drop(unique);
            admission.release(node_bytes);

            #[cfg(feature = "test-seams")]
            observe_identity_span(IdentitySpan::CoalesceAndGroup, group_started);
            let mut nodes = write_txn
                .open_table(CONTENT_IDENTITY_NODES_TABLE)
                .map_err(Self::storage_error)?;
            for family in LogicalFamily::ALL {
                let mut mutations = std::mem::take(&mut grouped[family as usize]);
                if mutations.is_empty() {
                    continue;
                }
                // One family's frames stop being held the moment its update has
                // consumed them, so the next family starts from the lower peak.
                // The walk itself returns them, because it is what consumes the
                // list: holding them until it came back reserved the list twice
                // over for the whole of the deepest walk.
                let frames = mutations.capacity() * std::mem::size_of::<GroupEntry>();
                grouped_bytes = grouped_bytes.saturating_sub(frames);
                #[cfg(feature = "test-seams")]
                let sort_started = std::time::Instant::now();
                sort_canonical_from_coalesced(&mut mutations)?;
                #[cfg(feature = "test-seams")]
                {
                    observe_identity_span(IdentitySpan::CoalesceAndGroup, sort_started);
                    enter_family(family, mutations.len());
                }
                let mut history = tails[family as usize].history_open;
                let mut refresh = [false;3];
                if let Some(publication) = history.as_mut() {
                    for (key, value) in &mutations {
                        if key.starts_with(&[ORDERED_HISTORY_KEY_TAG, HistoryKind::CommitIndex as u8]) {
                            let raw: [u8;8] = key[2..].try_into().map_err(|_|history_control_damage("invalid changed commit coordinate"))?;
                            let lsn = Lsn(u64::from_be_bytes(raw));
                            let old = writer_window::history_member(&nodes,admission,&roots[family as usize],key)?;
                            publication.control.commit_count = match (old.is_some(), value.is_some()) {
                                (false, true) => publication.control.commit_count.checked_add(1),
                                (true, false) => publication.control.commit_count.checked_sub(1),
                                _ => Some(publication.control.commit_count),
                            }.ok_or_else(|| history_control_damage("history member count overflow"))?;
                            if let Some(hash) = value {
                                if publication.control.first.is_none_or(|pair|lsn <= pair.0) { publication.control.first = Some((lsn,*hash)); }
                                if publication.control.last.is_none_or(|pair|lsn >= pair.0) { publication.control.last = Some((lsn,*hash)); }
                            } else {
                                refresh[0] |= publication.control.first.is_some_and(|pair|pair.0==lsn);
                                refresh[1] |= publication.control.last.is_some_and(|pair|pair.0==lsn);
                            }
                        } else if key.starts_with(&[ORDERED_HISTORY_KEY_TAG, HistoryKind::DdlLog as u8]) {
                            // DDL's variable coordinate stays in the proof window; retain only fixed facts.
                            writer_window::history_member(&nodes,admission,&roots[family as usize],key)?;
                            refresh[2] = true;
                        }
                    }
                }
                let updated = writer_window::update_family_consuming_keys(
                    &mut nodes,
                    admission,
                    family,
                    &roots[family as usize],
                    mutations,
                    frames,
                    &mut coalesced_bytes,
                );
                #[cfg(feature = "test-seams")]
                leave_family();
                roots[family as usize] = updated?;
                if let Some(publication) = history.as_mut() {
                    let root = &roots[family as usize];
                    publication.control.tree = root.root;
                    publication.control.tree_hash = root.hash;
                    if refresh[0] { publication.control.first = writer_window::history_commit_extreme(&nodes, admission, root, false)?; }
                    if refresh[1] { publication.control.last = writer_window::history_commit_extreme(&nodes, admission, root, true)?; }
                    if refresh[2] { publication.control.ddl = writer_window::history_ddl_extreme(&nodes, admission, root)?; }
                    if publication.control.commit_count == 0 { publication.control.first = None; publication.control.last = None; }
                    publication.control.validate()?;
                    publication.control_hash = publication.control.digest();
                    tails[family as usize].history_open = history;
                }
            }
            drop(nodes);
            admission.release(grouped_bytes);
            admission.release(coalesced_bytes);

            Ok(())
        })();
        // Every local allocation has dropped before a refusal escapes this
        // scope. Settle only this scope's remaining admission; caller-owned
        // charges present on entry stay with their original lifetime.
        if coalescing_result.is_err() {
            admission.release(admission.held() - coalescing_admission_start);
        }
        coalescing_result?;

        assign_open_projections(
            &mut roots,
            &mut tails,
            open_config_root,
            custody_config_root,
            vector_directory_root,
        );
        tails[LogicalFamily::Configuration as usize].content_size_root = Some(
            write_txn
                .table_root_fingerprint(super::content_size::TOTALS)
                .map_err(Self::storage_error)?
                .unwrap_or([0; 32]),
        );
        tails[LogicalFamily::Configuration as usize].vector_pages_root =
            Some(vector_schema_pages_root_in_write(write_txn)?);
        persist_changed_family_roots(write_txn, &roots, &original_roots, &tails, &original_tails)?;
        #[cfg(feature = "test-seams")]
        publish_directory_binding_work();
        Ok(digest_from_roots(std::array::from_fn(|family| {
            roots[family].hash
        })))
    }

    pub(crate) fn load_content_identity_from_read(
        read_txn: &redb::ReadTransaction,
    ) -> Result<DirectTypedImageDigest> {
        let metadata = read_txn
            .open_table(CONTENT_IDENTITY_METADATA_TABLE)
            .map_err(Self::storage_error)?;
        let version = metadata
            .get(IDENTITY_VERSION_KEY)
            .map_err(Self::storage_error)?
            .ok_or_else(|| Error::Other("logical content identity is absent".to_owned()))?;
        if version.value() != IDENTITY_VERSION {
            return Err(Error::Other(
                "logical content identity has an unsupported version".to_owned(),
            ));
        }
        drop(version);
        drop(metadata);
        let roots_table = read_txn
            .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
            .map_err(Self::storage_error)?;
        let mut root_hashes = [[0; 32]; 11];
        for family in LogicalFamily::ALL {
            let encoded = roots_table
                .get(family as u8)
                .map_err(Self::storage_error)?
                .ok_or_else(|| Error::Other("logical content family root is absent".to_owned()))?;
            root_hashes[family as usize] = decode_root(family, encoded.value())?.hash;
        }
        Ok(digest_from_roots(root_hashes))
    }

    #[allow(dead_code)]
    fn validate_family_tag(tag: u8) -> Result<LogicalFamily> {
        LogicalFamily::from_tag(tag)
    }
}

/// Raw proof fixtures retain the exact writer descriptor supplying disposable
/// working storage. The caller supplies a task-created database path.
#[cfg(test)]
struct ProofFixtureDatabase {
    // Drop Redb before its descriptor owner.
    database: redb::Database,
    owner: Arc<super::writer_file::LockedWriterFile>,
}

#[cfg(test)]
impl ProofFixtureDatabase {
    fn create(
        path: impl AsRef<std::path::Path>,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let path = path.as_ref();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let owner = super::writer_file::LockedWriterFile::lock(file, path.parent())?;
        let database = redb::Database::builder().create_with_backend(owner.view())?;
        Ok(Self { database, owner })
    }

    fn admission(
        &self,
        accountant: Option<crate::memory_accounting::AccountantHandle>,
    ) -> ProofAdmission {
        ProofAdmission::new(accountant).with_scratch_owner(Some(Arc::downgrade(&self.owner)))
    }
}

#[cfg(test)]
impl std::ops::Deref for ProofFixtureDatabase {
    type Target = redb::Database;

    fn deref(&self) -> &Self::Target {
        &self.database
    }
}

#[cfg(test)]
impl std::ops::DerefMut for ProofFixtureDatabase {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.database
    }
}

#[cfg(all(test, feature = "test-seams"))]
mod read_authentication_batch_tests {
    use super::*;

    #[test]
    fn bounded_batch_reuses_proofs_without_trusting_changed_values() {
        let root = tempfile::tempdir().unwrap();
        let database = ProofFixtureDatabase::create(root.path().join("proofs.db")).unwrap();
        let mutations = (0..2048_u64)
            .map(|id| {
                ContentIdentityMutation::put(
                    LogicalFamily::Relational,
                    id.to_le_bytes().to_vec(),
                    (id + 1).to_le_bytes().to_vec(),
                )
            })
            .collect::<Vec<_>>();
        let write = database.begin_write().unwrap();
        RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
        RedbPersistence::apply_content_identity_mutations_in_write(
            &write,
            &mutations,
            &mut database.admission(None),
        )
        .unwrap();
        write.commit().unwrap();
        let read = database.begin_read().unwrap();
        let ordinary = ContentIdentityReadVerifier::open(&read, root.path(), None).unwrap();
        reset_work_for_test();
        for mutation in &mutations {
            ordinary.verify(mutation).unwrap();
        }
        let ordinary_reads = take_work_for_test().0;
        reset_work_for_test();
        for mutation in &mutations[..80] {
            ordinary.verify(mutation).unwrap();
        }
        assert_eq!(
            take_work_for_test().0,
            0,
            "authenticated record witnesses avoid repeated result-row page reads"
        );
        let changed_witness = ContentIdentityMutation::put(
            LogicalFamily::Relational,
            0_u64.to_le_bytes().to_vec(),
            b"changed-through-witness".to_vec(),
        );
        assert!(
            ordinary.verify(&changed_witness).is_err(),
            "a compact witness must still reject changed loaded content"
        );
        let verifier = ContentIdentityReadVerifier::open(&read, root.path(), None).unwrap();
        let mut batch = verifier.batch(4 * 1024 * 1024);
        reset_work_for_test();
        for mutation in &mutations {
            batch.verify(mutation).unwrap();
        }
        let batch_reads = take_work_for_test().0;
        assert!(
            batch_reads < ordinary_reads,
            "batch={batch_reads} ordinary={ordinary_reads}"
        );
        assert!(batch.nodes.len() < mutations.len());
        assert!(
            batch.nodes.len() > 1,
            "the batch retains the bounded proof pages"
        );
        assert!(
            verifier
                .verified_records
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty(),
            "range batches must not consume the bounded result-record witness set"
        );
        reset_work_for_test();
        for mutation in &mutations {
            batch.verify(mutation).unwrap();
        }
        assert_eq!(
            take_work_for_test().0,
            0,
            "authenticated pages need no repeated physical reads"
        );
        let changed = ContentIdentityMutation::put(
            LogicalFamily::Relational,
            0_u64.to_le_bytes().to_vec(),
            b"changed".to_vec(),
        );
        assert!(batch.verify(&changed).is_err());
        let missing = ContentIdentityMutation::put(
            LogicalFamily::Relational,
            u64::MAX.to_le_bytes().to_vec(),
            b"absent".to_vec(),
        );
        assert!(batch.verify(&missing).is_err());
        assert!(
            batch
                .verify(&ContentIdentityMutation::remove(
                    LogicalFamily::Relational,
                    0_u64.to_le_bytes().to_vec()
                ))
                .is_err()
        );
        let mut tiny = verifier.batch(512);
        for mutation in &mutations {
            tiny.verify(mutation).unwrap();
        }
        assert!(tiny.bytes <= 512);
        eprintln!(
            "authentication page reads: ordinary={ordinary_reads} batch={batch_reads}; repeat=0; bounded_pages={}",
            batch.nodes.len()
        );
    }

    #[test]
    fn logical_pages_are_canonical_across_insertion_order_and_detect_changed_bytes() {
        let root = tempfile::tempdir().unwrap();
        let first_path = root.path().join("first.db");
        let second_path = root.path().join("second.db");
        let mutations = (0..4096_u64)
            .map(|id| {
                ContentIdentityMutation::put(
                    LogicalFamily::Relational,
                    id.to_le_bytes().to_vec(),
                    (id.wrapping_mul(17)).to_le_bytes().to_vec(),
                )
            })
            .collect::<Vec<_>>();
        let build = |path: &std::path::Path, ordered: &[ContentIdentityMutation]| {
            let database = ProofFixtureDatabase::create(path).unwrap();
            let write = database.begin_write().unwrap();
            RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
            let digest = RedbPersistence::apply_content_identity_mutations_in_write(
                &write,
                ordered,
                &mut database.admission(None),
            )
            .unwrap();
            write.commit().unwrap();
            (database, digest)
        };
        let (first, first_digest) = build(&first_path, &mutations);
        let mut reversed = mutations.clone();
        reversed.reverse();
        let (second, second_digest) = build(&second_path, &reversed);
        assert_eq!(
            first_digest, second_digest,
            "logical key grouping and page order are independent of insertion order"
        );

        let write = first.begin_write().unwrap();
        {
            let roots = write.open_table(CONTENT_IDENTITY_ROOTS_TABLE).unwrap();
            let root = decode_root(
                LogicalFamily::Relational,
                roots
                    .get(LogicalFamily::Relational as u8)
                    .unwrap()
                    .unwrap()
                    .value(),
            )
            .unwrap()
            .root
            .unwrap();
            drop(roots);
            let stored_key = node_storage_key(LogicalFamily::Relational, &root);
            let mut pages = write.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
            let mut encoded = pages
                .get(stored_key.as_slice())
                .unwrap()
                .unwrap()
                .value()
                .to_vec();
            let last = encoded.last_mut().unwrap();
            *last ^= 0x01;
            pages
                .insert(stored_key.as_slice(), encoded.as_slice())
                .unwrap();
        }
        write.commit().unwrap();
        let read = first.begin_read().unwrap();
        let verifier = ContentIdentityReadVerifier::open(&read, root.path(), None).unwrap();
        assert!(
            verifier.verify(&mutations[0]).is_err(),
            "a changed byte in the loaded logical page must fail its committed page proof"
        );

        let write = second.begin_write().unwrap();
        {
            let roots = write.open_table(CONTENT_IDENTITY_ROOTS_TABLE).unwrap();
            let root = decode_root(
                LogicalFamily::Relational,
                roots
                    .get(LogicalFamily::Relational as u8)
                    .unwrap()
                    .unwrap()
                    .value(),
            )
            .unwrap()
            .root
            .unwrap();
            drop(roots);
            write
                .open_table(CONTENT_IDENTITY_NODES_TABLE)
                .unwrap()
                .remove(node_storage_key(LogicalFamily::Relational, &root).as_slice())
                .unwrap();
        }
        write.commit().unwrap();
        let read = second.begin_read().unwrap();
        let verifier = ContentIdentityReadVerifier::open(&read, root.path(), None).unwrap();
        assert!(
            verifier.verify(&mutations[0]).is_err(),
            "a missing authenticated page cannot contribute a posting or row"
        );
    }

    #[test]
    fn point_update_work_stays_bounded_from_ten_to_one_hundred_thousand_records() {
        let measure = |records: u64| {
            let root = tempfile::tempdir().unwrap();
            let database = ProofFixtureDatabase::create(root.path().join("scale.db")).unwrap();
            let mutations = (0..records)
                .map(|id| {
                    ContentIdentityMutation::put(
                        LogicalFamily::Relational,
                        id.to_le_bytes().to_vec(),
                        id.wrapping_mul(31).to_le_bytes().to_vec(),
                    )
                })
                .collect::<Vec<_>>();
            let write = database.begin_write().unwrap();
            RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
            RedbPersistence::apply_content_identity_mutations_in_write(
                &write,
                &mutations,
                &mut database.admission(None),
            )
            .unwrap();
            write.commit().unwrap();

            reset_work_for_test();
            let changed = ContentIdentityMutation::put(
                LogicalFamily::Relational,
                (records / 2).to_le_bytes().to_vec(),
                b"changed-at-scale".to_vec(),
            );
            let write = database.begin_write().unwrap();
            RedbPersistence::apply_content_identity_mutations_in_write(
                &write,
                &[changed],
                &mut database.admission(None),
            )
            .unwrap();
            write.commit().unwrap();
            let operations = take_work_for_test();
            let bytes = take_work_bytes_for_test();
            (operations, bytes)
        };

        let (ten_k_operations, ten_k_bytes) = measure(10_000);
        let (hundred_k_operations, hundred_k_bytes) = measure(100_000);
        eprintln!(
            "point update scaling: 10k ops={ten_k_operations:?} bytes={ten_k_bytes:?}; 100k ops={hundred_k_operations:?} bytes={hundred_k_bytes:?}"
        );
        for (label, operations, bytes) in [
            ("10k", ten_k_operations, ten_k_bytes),
            ("100k", hundred_k_operations, hundred_k_bytes),
        ] {
            assert!(
                operations.0 <= 64,
                "{label} bounded page reads: {operations:?}"
            );
            assert!(
                operations.1 <= 64,
                "{label} bounded page writes: {operations:?}"
            );
            assert_eq!(operations.2, 0, "{label} point update removes no page");
            assert!(
                bytes.0 <= 64 * MAX_NODE_BYTES as u64,
                "{label} bounded bytes read: {bytes:?}"
            );
            assert!(
                bytes.1 <= 64 * MAX_NODE_BYTES as u64,
                "{label} bounded bytes written: {bytes:?}"
            );
        }
        assert!(
            hundred_k_bytes.0 <= ten_k_bytes.0.saturating_mul(4),
            "10x population must not cause linear point-read bytes: 10k={ten_k_bytes:?} 100k={hundred_k_bytes:?}"
        );
        assert!(
            hundred_k_bytes.1 <= ten_k_bytes.1.saturating_mul(4),
            "10x population must not cause linear point-write bytes: 10k={ten_k_bytes:?} 100k={hundred_k_bytes:?}"
        );
    }

    #[test]
    fn authenticated_posting_range_is_complete_and_ordered_in_both_directions() {
        let root = tempfile::tempdir().unwrap();
        let database = ProofFixtureDatabase::create(root.path().join("ordered.db")).unwrap();
        let order_key = |id: u64| {
            crate::persistence::DurableRelationalIndexKey::new(
                vec![contextdb_core::DirectedValue::Asc(
                    contextdb_core::TotalOrdAsc(Value::Int64(id as i64)),
                )],
                RowId(id),
                TxId(0),
            )
            .unwrap()
            .0
        };
        let mut mutations = (0..1_000_u64)
            .rev()
            .map(|id| relational_index_posting_mutation("items", "by_value", &order_key(id)))
            .collect::<Vec<_>>();
        mutations.extend(
            (0..10_000_u64).map(|id| {
                relational_index_posting_mutation("items", "zz_unrelated", &order_key(id))
            }),
        );
        let write = database.begin_write().unwrap();
        RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
        RedbPersistence::apply_content_identity_mutations_in_write(
            &write,
            &mutations,
            &mut database.admission(None),
        )
        .unwrap();
        write.commit().unwrap();

        let collect = |reverse| {
            let read = database.begin_read().unwrap();
            let verifier = ContentIdentityReadVerifier::open(&read, root.path(), None).unwrap();
            let mut rows = Vec::new();
            verifier
                .visit_relational_index_postings::<Error>(
                    "items",
                    "by_value",
                    std::ops::Bound::Unbounded,
                    reverse,
                    &ProofMemory::unlimited(),
                    &mut || Ok(()),
                    &mut |key| {
                        rows.push(
                            crate::persistence::DurableRelationalIndexKey(key.to_vec())
                                .decode()?
                                .row_id
                                .0,
                        );
                        Ok(true)
                    },
                )
                .unwrap();
            rows
        };
        reset_work_for_test();
        assert_eq!(collect(false), (0..1_000).collect::<Vec<_>>());
        let target_reads = take_work_for_test().0;
        assert!(
            target_reads < 100,
            "one index namespace must prune the larger sibling namespace: reads={target_reads}"
        );
        assert_eq!(collect(true), (0..1_000).rev().collect::<Vec<_>>());

        let read = database.begin_read().unwrap();
        let verifier = ContentIdentityReadVerifier::open(&read, root.path(), None).unwrap();
        let mut suffix = Vec::new();
        verifier
            .visit_relational_index_postings::<Error>(
                "items",
                "by_value",
                std::ops::Bound::Included(order_key(500)),
                false,
                &ProofMemory::unlimited(),
                &mut || Ok(()),
                &mut |key| {
                    suffix.push(
                        crate::persistence::DurableRelationalIndexKey(key.to_vec())
                            .decode()?
                            .row_id
                            .0,
                    );
                    Ok(true)
                },
            )
            .unwrap();
        assert_eq!(suffix, (500..1_000).collect::<Vec<_>>());
    }
}

#[cfg(all(test, feature = "test-seams"))]
#[path = "high_fanout_tests.rs"]
mod high_fanout_tests;

#[cfg(all(test, feature = "test-seams"))]
#[path = "proof_authentication_tests.rs"]
mod proof_authentication_tests;

#[cfg(all(test, feature = "test-seams"))]
#[path = "proof_memory_tests.rs"]
mod proof_memory_tests;

#[cfg(all(test, feature = "test-seams"))]
#[path = "vector_occupancy_interval_tests.rs"]
mod vector_occupancy_interval_tests;

#[cfg(all(test, feature = "test-seams"))]
#[path = "physical_key_order_tests.rs"]
mod physical_key_order_tests;

#[cfg(all(test, feature = "test-seams"))]
#[path = "correction_counterexample_tests.rs"]
mod correction_counterexample_tests;

#[cfg(all(test, feature = "test-seams"))]
#[path = "proof_allocation_oracle_tests.rs"]
pub(crate) mod proof_allocation_oracle_tests;

/// Damage helpers and the reuse gate, reachable from the focused contract
/// module beside this one. Test-only.
#[cfg(all(test, feature = "test-seams"))]
pub(super) mod test_access {
    use super::*;

    /// Rewrite one stored `Relational` leaf chunk so that it is internally
    /// self-consistent but is no longer the chunk its parent names: the last
    /// entry's value hash changes, and the chunk is re-hashed to match itself.
    /// A walk that reaches it must refuse, resident siblings or not.
    pub(crate) fn damage_one_leaf_chunk_in_write(write: &redb::WriteTransaction) -> bool {
        let family = LogicalFamily::Relational;
        let mut nodes = write
            .open_table(CONTENT_IDENTITY_NODES_TABLE)
            .expect("the proof node table exists");
        let mut target = None;
        for row in nodes.iter().expect("iterate stored chunks") {
            let (key, value) = row.expect("read one stored chunk");
            if key.value().first() != Some(&(family as u8))
                || value.value().first() != Some(&NODE_VERSION)
            {
                continue;
            }
            let node = decode_node(family, value.value()).expect("stored chunk decodes");
            if node.level == 0 && !node.entries.is_empty() {
                target = Some((key.value().to_vec(), node));
                break;
            }
        }
        let Some((storage_key, mut node)) = target else {
            return false;
        };
        let last = node.entries.len() - 1;
        node.entries[last].value_hash[0] ^= 0xff;
        node.subtree_hash = node_hash(family, node.level, &node.entries);
        let encoded = encode_node(&node);
        nodes
            .insert(storage_key.as_slice(), encoded.as_slice())
            .expect("store the damaged chunk");
        true
    }
}
/// The reuse gate: a chunk this verifier has already proved is offered back
/// only under the hash its parent demands, so residency can never widen what
/// the committed root admits.
#[cfg(all(test, feature = "test-seams"))]
mod resident_chunk_gate_tests {
    use super::*;

    fn record(row: u64) -> ContentIdentityMutation {
        ContentIdentityMutation::put(
            LogicalFamily::Relational,
            row.to_be_bytes().to_vec(),
            vec![row as u8; 64],
        )
    }

    /// The ceiling is the session's own declaration, floored by the
    /// reservation the owner already holds and capped by a declared constant.
    /// A reader that declares more keeps more of its own proof resident.
    #[test]
    fn the_resident_ceiling_follows_the_declared_session() {
        let root = tempfile::tempdir().expect("fixture directory");
        let database = ProofFixtureDatabase::create(root.path().join("ceiling.redb"))
            .expect("create the fixture");
        let write = database.begin_write().unwrap();
        RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
        write.commit().unwrap();
        let read = database.begin_read().unwrap();

        let ceiling = |limit: Option<usize>| {
            let accountant = Arc::new(match limit {
                Some(bytes) => crate::memory_accounting::MemoryAccountant::with_budget(bytes),
                None => crate::memory_accounting::MemoryAccountant::no_limit(),
            });
            let mut verifier =
                ContentIdentityReadVerifier::open(&read, root.path(), Some(accountant)).unwrap();
            verifier.retain_proved_chunks_within(RESIDENT_PROOF_CHUNK_BYTES);
            verifier.resident_proof_chunk_ceiling_for_test()
        };

        assert_eq!(
            ceiling(Some(1024 * 1024)),
            RESIDENT_PROOF_CHUNK_BYTES,
            "a declaration whose share is under the floor keeps the floor it already reserved"
        );
        assert_eq!(ceiling(Some(64 * 1024 * 1024)), 64 * 1024 * 1024 / 32);
        assert_eq!(
            ceiling(Some(1024 * 1024 * 1024)),
            RESIDENT_PROOF_CHUNK_MAX_BYTES,
            "and the share of a large declaration is still capped: cg's 1 GiB default \
             keeps at most {RESIDENT_PROOF_CHUNK_MAX_BYTES} bytes resident"
        );

        // No declared ceiling means no share to take: the floor the owner
        // reserved is all this reader keeps.
        let unlimited =
            ContentIdentityReadVerifier::open(&read, root.path(), None).expect("no accountant");
        assert_eq!(
            unlimited.resident_proof_chunk_ceiling_for_test(),
            0,
            "a verifier whose owner reserved nothing keeps nothing"
        );
    }

    /// Retain a proved chunk, then ask for that exact chunk under a hash its
    /// parent does not demand. A resident hit is refused exactly as a chunk
    /// just read out of the store is.
    #[test]
    fn a_resident_chunk_offered_under_the_wrong_hash_is_refused() {
        let root = tempfile::tempdir().expect("fixture directory");
        let database = ProofFixtureDatabase::create(root.path().join("gate.redb"))
            .expect("create the fixture");
        let records = (0..2_000).map(record).collect::<Vec<_>>();
        let write = database.begin_write().unwrap();
        RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
        RedbPersistence::apply_content_identity_mutations_in_write(
            &write,
            &records,
            &mut database.admission(None),
        )
        .unwrap();
        write.commit().unwrap();

        let read = database.begin_read().unwrap();
        let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
            64 * 1024 * 1024,
        ));
        let mut verifier =
            ContentIdentityReadVerifier::open(&read, root.path(), Some(accountant)).unwrap();
        verifier.retain_proved_chunks_within(RESIDENT_PROOF_CHUNK_BYTES);

        for mutation in &records {
            verifier
                .verify(mutation)
                .expect("the committed image verifies");
        }
        let (chunks, _, _) = verifier.resident_proof_chunks_for_test();
        assert!(chunks > 0, "the walks retained what they proved");

        // The family root, which every one of those walks descended from and
        // which is therefore certainly resident.
        let family = LogicalFamily::Relational;
        let id = verifier.roots[family as usize].root.expect("a root chunk");
        let honest = verifier.roots[family as usize].hash;
        let mut forged = honest;
        forged[0] ^= 0xff;

        reset_work_for_test();
        verifier
            .verified_node(
                family,
                id,
                honest,
                &mut BTreeMap::new(),
                &mut 0,
                0,
                &mut ProofAdmission::reclaiming(
                    verifier.admission.clone(),
                    Arc::clone(&verifier.resident),
                ),
            )
            .map(|_| ())
            .expect("the resident chunk is accepted under the hash its parent demands");
        assert_eq!(
            take_work_for_test().0,
            0,
            "and it was answered from residency, not from the store"
        );

        let refused = verifier
            .verified_node(
                family,
                id,
                forged,
                &mut BTreeMap::new(),
                &mut 0,
                0,
                &mut ProofAdmission::reclaiming(
                    verifier.admission.clone(),
                    Arc::clone(&verifier.resident),
                ),
            )
            .map(|_| ())
            .expect_err("a resident chunk under the wrong hash is refused");
        assert!(
            refused
                .to_string()
                .contains("disagrees with its committed proof path"),
            "the reuse gate is the committed proof path: {refused:?}"
        );
    }
}

#[cfg(all(test, feature = "test-seams"))]
mod resident_proof_chunk_set_tests {
    use super::*;

    fn dummy_chunk(bytes: usize, accountant: &Arc<MemoryAccountant>) -> Arc<VerifiedReadNode> {
        Arc::new(VerifiedReadNode {
            node: Arc::new(Node {
                level: 0,
                entries: vec![PageEntry {
                    key: vec![0],
                    value_hash: [0; 32],
                }],
                subtree_hash: [0; 32],
            }),
            bytes,
            _charge: AdmittedBytes::with_source(
                crate::memory_accounting::StoreAccountantHandle::retain_if_writer(accountant), None),
            _floor: None,
        })
    }

    fn storage_key(tag: u8) -> [u8; 33] {
        let mut key = [0u8; 33];
        key[32] = tag;
        key
    }

    #[test]
    fn borrowed_proof_pages_stay_charged_until_released() {
        const PAGE: usize = 64;
        let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
            PAGE,
        ));
        let set = ResidentProofChunkSet::charged_for_test(Arc::clone(&accountant), PAGE);
        let key = storage_key(1);
        set.retain(key, &dummy_chunk(PAGE, &accountant));
        assert_eq!(accountant.usage().used, PAGE);

        let first = set.get(&key).expect("retained page is borrowable");
        let second = Arc::clone(&first);
        assert!(set.reclaim(), "pressure evicts the cache entry");
        assert_eq!(set.observed(), (0, 0, 0));
        assert_eq!(
            accountant.usage().used,
            PAGE,
            "the borrower still holds the charge"
        );
        assert!(
            accountant.try_allocate(1).is_err(),
            "a competing allocation is refused while the page is held"
        );

        drop(first);
        assert_eq!(accountant.usage().used, PAGE);
        assert!(accountant.try_allocate(1).is_err());
        drop(second);
        assert_eq!(
            accountant.usage().used,
            0,
            "the charge returns exactly once"
        );
        assert_eq!(accountant.underflow_count_for_test(), 0);
        accountant
            .try_allocate(PAGE)
            .expect("the budget is free after the last handle drops");
        accountant.release(PAGE);
    }

    /// A warm hit must relink the entry it found, not walk the use-order list
    /// looking for it: the entries inspected or moved per hit stay the same
    /// whether the list holds four pages or sixty-four. The hit that costs a
    /// scan the most is the newest entry, which a walk from the old end reaches
    /// last, so that is the one both sizes ask for.
    #[test]
    fn warm_cache_hits_do_not_scan_the_resident_list() {
        const PAGE: usize = 8;
        const SMALL: u8 = 4;
        const LARGE: u8 = 64;
        const CEILING: usize = PAGE * LARGE as usize;

        let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
            CEILING,
        ));
        let small = ResidentProofChunkSet::free_for_test(CEILING);
        for tag in 0..SMALL {
            small.retain(storage_key(tag), &dummy_chunk(PAGE, &accountant));
        }
        reset_touch_work_for_test();
        assert!(small.get(&storage_key(SMALL - 1)).is_some());
        let small_work = take_touch_work_for_test();

        let large = ResidentProofChunkSet::free_for_test(CEILING);
        for tag in 0..LARGE {
            large.retain(storage_key(tag), &dummy_chunk(PAGE, &accountant));
        }
        reset_touch_work_for_test();
        assert!(large.get(&storage_key(LARGE - 1)).is_some());
        let large_work = take_touch_work_for_test();

        assert_eq!(
            small_work, large_work,
            "entries inspected or moved per hit must not grow with list size: \
             small={small_work} large={large_work}"
        );
        assert_eq!(small_work, 1);

        // Eviction order is unchanged: oldest first, and a hit moves its entry
        // to the young end.
        assert_eq!(
            (
                large.lru_keys_for_test().first().copied(),
                large.lru_keys_for_test().last().copied()
            ),
            (Some(storage_key(0)), Some(storage_key(LARGE - 1)))
        );
        assert!(large.get(&storage_key(0)).is_some());
        assert_eq!(
            (
                large.lru_keys_for_test().first().copied(),
                large.lru_keys_for_test().last().copied()
            ),
            (Some(storage_key(1)), Some(storage_key(0))),
            "the touched entry becomes the youngest and the next-oldest leads"
        );

        let mut state = large.locked();
        assert_eq!(
            state.evict_oldest(),
            Some(0),
            "untouched free-floor bytes report as zero"
        );
        drop(state);
        assert_eq!(
            large.lru_keys_for_test().first().copied(),
            Some(storage_key(2)),
            "eviction takes the oldest, never the touched entry"
        );
    }

    /// A page kept inside the reader's fixed verifier reservation is the only
    /// thing accounted for those bytes, so evicting its cache entry while a
    /// query still holds the page must not hand the same room to a
    /// replacement: that would put two pages in a reservation sized for one.
    /// The room comes back when the last holder lets go, and only then.
    #[test]
    fn a_borrowed_floor_page_keeps_its_share_of_the_reservation_until_released() {
        const PAGE: usize = 64;
        let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
            PAGE,
        ));
        let set = ResidentProofChunkSet::free_for_test(PAGE);
        let held = storage_key(1);
        let replacement = storage_key(2);

        set.retain(held, &dummy_chunk(PAGE, &accountant));
        assert_eq!(
            set.observed(),
            (1, PAGE, 0),
            "the first page fills the one-page reservation"
        );

        let borrowed = set.get(&held).expect("the retained page is borrowable");
        set.retain(replacement, &dummy_chunk(PAGE, &accountant));
        assert_eq!(
            set.observed(),
            (0, PAGE, 0),
            "a reservation a borrower still holds has no room for a replacement"
        );
        assert!(
            set.get(&replacement).is_none(),
            "a page that could not be admitted is not retained"
        );

        drop(borrowed);
        assert_eq!(
            set.observed(),
            (0, 0, 0),
            "the share returns when the last holder lets go"
        );

        set.retain(replacement, &dummy_chunk(PAGE, &accountant));
        assert_eq!(
            set.observed(),
            (1, PAGE, 0),
            "and the freed room is reusable exactly once"
        );
    }
}

pub(super) mod history_open_control;
pub(super) use history_open_control::{HistoryOpenControl, HistoryOpenPublication, PhysicalHistoryTable, ValidatedHistoryOpenControl};
use history_open_control::history_control_damage;

#[path = "content_identity/version_manifest.rs"]
pub(super) mod version_manifest;
