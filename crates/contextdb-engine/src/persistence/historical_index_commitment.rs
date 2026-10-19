//! Completeness of a historical relational index without a walk.
//!
//! A historical posting `(key, row, start) -> end` covers one contiguous run
//! of a row's versions that share that index key. Per (table, index) the
//! writer keeps two additive lattice states in the same transaction as the
//! change:
//!
//! * `Postings`, folded inside [`HistoricalPostings`] from the actual old and
//!   new posting values: each interval `[s, e)` contributes `+B(s) - B(e)`;
//! * `Versions`, folded from row-version mutations: each version `[c, d)`
//!   contributes `+B(c) - B(d)` under that version's own key.
//!
//! Both sides use one boundary map `B(table, index identity, key and row,
//! tagged tx)`, where the open end is its own tag. Inside a run of equal keys
//! consecutive versions share boundaries, so their terms telescope to the
//! run's: equal states mean the postings **cover** exactly what the versions
//! cover. Coverage alone does not fix the decomposition (`[a,c) + [b,d)` and
//! `[a,d) + [b,c)` share boundaries), so every posting write also enforces,
//! against its predecessor and successor in the same (key, row): `s < e`,
//! unique starts, disjoint intervals, and no two intervals that touch (they
//! would have to be one). Under that invariant each boundary carries a
//! coefficient of -1, 0 or +1 in each state, so no modular alias of the
//! 16-bit lanes is reachable by legal mutation; detecting corruption beyond
//! that rests on the short-vector hardness of the lattice map.
//!
//! The writer refuses to commit unequal states and publishes both digests and
//! the index identity as one authenticated content-identity entry per
//! (table, index); a reader authenticates that entry and requires equality.
use super::*;

pub(super) const HISTORICAL_INDEX_COMMITMENT_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("__historical_index_commitments");

const LANES: usize = 1024;
const STATE_BYTES: usize = 2 * LANES;
const BOUNDARY_DOMAIN: &[u8] = b"contextdb-historical-index-boundary-v1";
const STATE_DIGEST_DOMAIN: &[u8] = b"contextdb-historical-index-state-digest-v1";
/// The Status namespace of the per-(table, index) publications.
const PUBLICATION_NAMESPACE: &[u8] = b"historical_index_commitment";
const OPEN_END: TxId = TxId(u64::MAX);

/// Which of a (table, index)'s two states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Side {
    Postings,
    Versions,
}

/// One additive lattice state: 1024 lanes of u16, lane-wise sum modulo 2^16.
#[derive(Clone, PartialEq, Eq)]
struct BoundaryState {
    lanes: Box<[u16; LANES]>,
}

impl BoundaryState {
    fn empty() -> Self {
        Self {
            lanes: Box::new([0; LANES]),
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != STATE_BYTES {
            return Err(Error::Other((
                "historical index commitment has invalid size".to_owned()).into()));
        }
        let mut state = Self::empty();
        for (lane, pair) in state.lanes.iter_mut().zip(bytes.chunks_exact(2)) {
            *lane = u16::from_le_bytes([pair[0], pair[1]]);
        }
        Ok(state)
    }

    fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(STATE_BYTES);
        for lane in self.lanes.iter() {
            encoded.extend_from_slice(&lane.to_le_bytes());
        }
        encoded
    }

    fn apply(&mut self, element: &[u16; LANES], positive: bool) {
        for (lane, term) in self.lanes.iter_mut().zip(element) {
            *lane = if positive {
                lane.wrapping_add(*term)
            } else {
                lane.wrapping_sub(*term)
            };
        }
    }

    /// One digest encoding for both sides: equal states have equal digests.
    fn digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(STATE_DIGEST_DOMAIN);
        hasher.update(&self.encode());
        *hasher.finalize().as_bytes()
    }
}

fn framed(parts: &[&[u8]]) -> Vec<u8> {
    let mut key = Vec::new();
    for part in parts {
        key.extend_from_slice(&(part.len() as u64).to_be_bytes());
        key.extend_from_slice(part);
    }
    key
}

/// The identity a re-declared index changes: its name and its encoded
/// declaration.
pub(super) fn index_identity(declaration: &IndexDecl) -> Result<Vec<u8>> {
    Ok(framed(&[
        declaration.name.as_bytes(),
        &RedbPersistence::encode(declaration)?,
    ]))
}

/// `B(table, index identity, key and row, tx)`. `key_row` is the canonical
/// posting position of `(key, row)` at transaction zero; `tx` is tagged so the
/// open end is never a finite value.
/// Heap-allocated: a commit folds these inside deep writer stacks.
/// The framing is streamed into the hasher, so the only allocations are the
/// two fixed-size buffers [`CommitmentBatch`] funds as fold scratch.
fn element(table: &str, identity: &[u8], key_row: &[u8], tx: TxId) -> Box<[u16; LANES]> {
    element_checked(table, identity, key_row, tx, &mut || Ok(()))
        .expect("an unchecked element never refuses")
}

/// [`element`] with a checkpoint between every 64 KiB of the key and row it
/// hashes, so a wide key never runs a request past its cancellation.
fn element_checked(
    table: &str,
    identity: &[u8],
    key_row: &[u8],
    tx: TxId,
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<Box<[u16; LANES]>> {
    note_verify_phase("hash");
    let mut hasher = element_prefix(table, identity, checkpoint)?;
    hash_part(&mut hasher, key_row, checkpoint)?;
    element_finish(hasher, tx, checkpoint)
}

/// The element hash up to its key part: the domain, the table and the index
/// identity, each length-prefixed. The key part follows, then a boundary.
fn element_prefix(
    table: &str,
    identity: &[u8],
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<blake3::Hasher> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(BOUNDARY_DOMAIN);
    hash_part(&mut hasher, table.as_bytes(), checkpoint)?;
    hash_part(&mut hasher, identity, checkpoint)?;
    Ok(hasher)
}

fn hash_part(
    hasher: &mut blake3::Hasher,
    part: &[u8],
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<()> {
    hasher.update(&(part.len() as u64).to_be_bytes());
    for chunk in part.chunks(64 * 1024) {
        checkpoint()?;
        hasher.update(chunk);
    }
    Ok(())
}

/// Close an element hash that holds its key part with the boundary `tx`.
/// One version's two elements share everything up to here, so the caller
/// clones the hasher once its key is streamed and finishes each copy.
fn element_finish(
    mut hasher: blake3::Hasher,
    tx: TxId,
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<Box<[u16; LANES]>> {
    let mut tagged = [0_u8; 9];
    let tagged: &[u8] = if tx == OPEN_END {
        tagged[0] = 1;
        &tagged[..1]
    } else {
        tagged[1..].copy_from_slice(&tx.0.to_be_bytes());
        &tagged[..]
    };
    hash_part(&mut hasher, tagged, checkpoint)?;
    let mut expanded = vec![0_u8; STATE_BYTES];
    hasher.finalize_xof().fill(&mut expanded);
    let mut lanes = Box::new([0_u16; LANES]);
    for (lane, pair) in lanes.iter_mut().zip(expanded.chunks_exact(2)) {
        *lane = u16::from_le_bytes([pair[0], pair[1]]);
    }
    Ok(lanes)
}

/// The canonical posting position of `(key, row)` at transaction zero: the
/// one byte string both sides frame for a version or posting of that row.
pub(super) fn key_row(key: &IndexKey, row: RowId) -> Result<Vec<u8>> {
    Ok(DurableRelationalIndexKey::new(key.clone(), row, TxId(0))?.0)
}

/// [`key_row`] of a stored row body under one index, projecting only the
/// indexed columns out of the body: a scalar value is copied as stored and a
/// stored vector is projected in place, never decoding the rest of the row.
/// The writer's fold keeps the key; verification streams the same bytes into
/// its element hash instead.
pub(super) fn key_row_from_body(body: &[u8], declaration: &IndexDecl) -> Result<Vec<u8>> {
    let mut key = content_identity::version_manifest::VecKey::default();
    let mut scratch = Vec::new();
    content_identity::version_manifest::project_posting_key(
        body,
        declaration,
        &mut || Ok(()),
        &mut |bytes| crate::read_image_memory::reserve(bytes),
        Some(&mut scratch),
        content_identity::version_manifest::KeySuffix::Zero,
        &mut key,
    )?
    .ok_or_else(|| Error::Other("a posting key could not be built".into()))?;
    Ok(key.0)
}

/// [`key_row_from_body`] with the caller's checkpoint, run at least once per
/// 64 KiB of the body and every 4096 projected vector components, and its
/// own `reserve`, called before each allocation.
pub(super) fn key_row_from_body_with(
    body: &[u8],
    declaration: &IndexDecl,
    meta: Option<&TableMeta>,
    checkpoint: &mut dyn FnMut() -> Result<()>,
    reserve: &mut dyn FnMut(usize) -> Result<()>,
) -> Result<Vec<u8>> {
    let _ = meta;
    let mut key = content_identity::version_manifest::VecKey::default();
    let mut scratch = Vec::new();
    content_identity::version_manifest::project_posting_key(
        body,
        declaration,
        checkpoint,
        reserve,
        Some(&mut scratch),
        content_identity::version_manifest::KeySuffix::Zero,
        &mut key,
    )?
    .ok_or_else(|| Error::Other("a posting key could not be built".into()))?;
    Ok(key.0)
}

fn state_key(table: &str, index: &str, side: Side) -> Vec<u8> {
    framed(&[
        table.as_bytes(),
        index.as_bytes(),
        match side {
            Side::Postings => b"P",
            Side::Versions => b"V",
        },
    ])
}

/// The key, inside [`PUBLICATION_NAMESPACE`], of a (table, index)'s
/// authenticated publication.
fn publication_key(table: &str, index: &str) -> Vec<u8> {
    framed(&[table.as_bytes(), index.as_bytes()])
}

fn publication(
    table: &str,
    index: &str,
    postings: &BoundaryState,
    versions: &BoundaryState,
    identity: &[u8],
) -> content_identity::ContentIdentityMutation {
    content_identity::status_mutation(
        PUBLICATION_NAMESPACE,
        &publication_key(table, index),
        &publication_value(&postings.digest(), &versions.digest(), identity),
    )
}

/// The postings digest, the versions digest and the index identity digest.
fn publication_value(postings: &[u8; 32], versions: &[u8; 32], identity: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(96);
    value.extend_from_slice(postings);
    value.extend_from_slice(versions);
    value.extend_from_slice(blake3::hash(identity).as_bytes());
    value
}

struct TouchedIndex {
    identity: Vec<u8>,
    postings: BoundaryState,
    versions: BoundaryState,
}

const ADMISSION_SUBSYSTEM: &str = "historical_index_commitment";
const ADMISSION_HINT: &str =
    "Raise MEMORY_LIMIT to commit this change's historical index commitments.";
/// One fold's temporaries: two boundary elements and their expansion buffer.
const FOLD_SCRATCH_BYTES: usize = 3 * STATE_BYTES + 256;
/// A touched pair's map node, owned names and two boxed states, beyond the
/// variable name and identity bytes.
const TOUCHED_FIXED_BYTES: usize = 2 * STATE_BYTES + 256;

/// Who pays for a batch: the writer operation's memory owner, or on the
/// owned-writer path a reservation drawn from that path's original.
enum Funding {
    Writer {
        owner: Arc<crate::read_image_memory::ImageMemory>,
        held: usize,
    },
    Original(crate::memory_accounting::OwnedMemoryReservation),
}

impl Funding {
    fn admit(&mut self, bytes: usize, operation: &str) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        if take_admission_refusal_for_test() {
            return Err(Error::MemoryBudgetExceeded {
                subsystem: (ADMISSION_SUBSYSTEM.to_owned()).into(),
                operation: (operation.to_owned()).into(),
                requested_bytes: bytes,
                available_bytes: 0,
                budget_limit_bytes: 0,
                hint: (ADMISSION_HINT.to_owned()).into(),
            });
        }
        match self {
            Funding::Writer { owner, held } => {
                owner.reserve(bytes)?;
                *held += bytes;
            }
            Funding::Original(reservation) => {
                reservation.try_grow_for(bytes, ADMISSION_SUBSYSTEM, operation, ADMISSION_HINT)?
            }
        }
        #[cfg(any(test, feature = "test-seams"))]
        note_admitted_for_test(bytes);
        Ok(())
    }
}

impl Drop for Funding {
    fn drop(&mut self) {
        if let Funding::Writer { owner, held } = self {
            owner.release(std::mem::take(held));
        }
    }
}

/// (table, index) pairs held without allocating to look one up: the outer
/// map is keyed by table and the inner by index, both found by `&str`, so a
/// pair's owned names are allocated only when it is first inserted, after
/// they are admitted.
struct PairMap<V> {
    tables: BTreeMap<String, BTreeMap<String, V>>,
    len: usize,
}

impl<V> PairMap<V> {
    fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
            len: 0,
        }
    }
    fn get(&self, table: &str, index: &str) -> Option<&V> {
        self.tables
            .get(table)
            .and_then(|indexes| indexes.get(index))
    }
    fn get_mut(&mut self, table: &str, index: &str) -> Option<&mut V> {
        self.tables
            .get_mut(table)
            .and_then(|indexes| indexes.get_mut(index))
    }
    fn contains(&self, table: &str, index: &str) -> bool {
        self.get(table, index).is_some()
    }
    /// Insert a pair known to be absent.
    fn insert(&mut self, table: &str, index: &str, value: V) {
        match self.tables.get_mut(table) {
            Some(indexes) => {
                indexes.insert(index.to_owned(), value);
            }
            None => {
                self.tables.insert(
                    table.to_owned(),
                    BTreeMap::from([(index.to_owned(), value)]),
                );
            }
        }
        self.len += 1;
    }
    fn remove(&mut self, table: &str, index: &str) -> Option<V> {
        let indexes = self.tables.get_mut(table)?;
        let removed = indexes.remove(index)?;
        if indexes.is_empty() {
            self.tables.remove(table);
        }
        self.len -= 1;
        Some(removed)
    }
    fn len(&self) -> usize {
        self.len
    }
    fn iter(&self) -> impl Iterator<Item = (&str, &str, &V)> {
        self.tables.iter().flat_map(|(table, indexes)| {
            indexes
                .iter()
                .map(move |(index, value)| (table.as_str(), index.as_str(), value))
        })
    }
}

/// What a new pair retains beyond its states: two owned names, map nodes and
/// slack for the outer map entry.
fn pair_bytes(table: &str, index: &str) -> usize {
    table
        .len()
        .saturating_mul(2)
        .saturating_add(index.len())
        .saturating_add(256)
}

/// The exact length of [`index_identity`], measured without building it.
fn identity_len(declaration: &IndexDecl) -> Result<usize> {
    let mut size = bincode::enc::write::SizeWriter::default();
    bincode::serde::encode_into_writer(declaration, &mut size, bincode::config::standard())
        .map_err(|error| Error::Other((format!("bincode size error: {error}")).into()))?;
    Ok(16usize
        .saturating_add(declaration.name.len())
        .saturating_add(size.bytes_written))
}

/// The changes one write transaction makes to the states of every
/// (table, index) it touches: each touched pair is loaded once, folded in
/// memory, and stored, compared and published once at [`Self::finish`].
///
/// Everything it retains, and the high-water of its temporaries, is admitted
/// to the writer's budget before allocation; the funding then travels with
/// the publications it paid for (see [`Publications`]) until they are
/// destroyed. A refusal is a typed `MemoryBudgetExceeded` that rolls the
/// write back.
pub(crate) struct CommitmentBatch {
    transient: usize,
    touched: PairMap<TouchedIndex>,
    dropped: PairMap<()>,
    deferred: PairMap<()>,
    funding: Funding,
}

impl CommitmentBatch {
    fn with_funding(mut funding: Funding) -> Result<Self> {
        funding.admit(FOLD_SCRATCH_BYTES, "fold_scratch")?;
        Ok(Self {
            funding,
            transient: 0,
            touched: PairMap::new(),
            dropped: PairMap::new(),
            deferred: PairMap::new(),
        })
    }

    /// A batch paid for by the active writer operation.
    pub(super) fn for_writer() -> Result<Self> {
        Self::with_funding(Funding::Writer {
            owner: crate::read_image_memory::active_owner()?,
            held: 0,
        })
    }

    /// A batch paid for from the owned-writer path's original reservation.
    pub(super) fn for_original(
        original: &crate::memory_accounting::OwnedMemoryReservation,
    ) -> Result<Self> {
        Self::with_funding(Funding::Original(
            crate::memory_accounting::reserve_preparation_from_original(
                original,
                0,
                ADMISSION_SUBSYSTEM,
                "fold_scratch",
                ADMISSION_HINT,
            )?,
        ))
    }

    /// Hold room for a temporary of `bytes`: only growth past the high-water
    /// mark is admitted.
    pub(super) fn admit_transient(&mut self, bytes: usize) -> Result<()> {
        if bytes > self.transient {
            self.funding
                .admit(bytes - self.transient, "transient_keys")?;
            self.transient = bytes;
        }
        Ok(())
    }

    fn index(
        &mut self,
        write_txn: &redb::WriteTransaction,
        table: &str,
        declaration: &IndexDecl,
    ) -> Result<&mut TouchedIndex> {
        if !self.touched.contains(table, &declaration.name) {
            // Names, identity and both states, before any is allocated.
            self.funding.admit(
                TOUCHED_FIXED_BYTES
                    .saturating_add(pair_bytes(table, &declaration.name))
                    .saturating_add(identity_len(declaration)?),
                "touched_index",
            )?;
            let (postings, versions) = if self.dropped.contains(table, &declaration.name) {
                (BoundaryState::empty(), BoundaryState::empty())
            } else {
                (
                    load(write_txn, table, &declaration.name, Side::Postings)?,
                    load(write_txn, table, &declaration.name, Side::Versions)?,
                )
            };
            self.touched.insert(
                table,
                &declaration.name,
                TouchedIndex {
                    identity: index_identity(declaration)?,
                    postings,
                    versions,
                },
            );
        }
        Ok(self
            .touched
            .get_mut(table, &declaration.name)
            .expect("just inserted"))
    }

    /// Add (`positive`) or remove the interval `[start, end)` of one side:
    /// `+B(start) - B(end)`, with `end` `None` the open end.
    #[allow(
        clippy::too_many_arguments,
        reason = "one interval names its table, index, side, key and row, both ends and its sign"
    )]
    pub(super) fn fold_interval(
        &mut self,
        write_txn: &redb::WriteTransaction,
        table: &str,
        declaration: &IndexDecl,
        side: Side,
        key_row: &[u8],
        start: TxId,
        end: Option<TxId>,
        positive: bool,
    ) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        if side == Side::Postings && take_postings_fold_skip_for_test() {
            return Ok(());
        }
        let touched = self.index(write_txn, table, declaration)?;
        let start = element(table, &touched.identity, key_row, start);
        let end = element(table, &touched.identity, key_row, end.unwrap_or(OPEN_END));
        let state = match side {
            Side::Postings => &mut touched.postings,
            Side::Versions => &mut touched.versions,
        };
        state.apply(&start, positive);
        state.apply(&end, !positive);
        Ok(())
    }

    /// Empty one side of a (table, index) whose content is about to be
    /// written again from scratch in this transaction: the postings of an
    /// index being rebuilt, or the versions of a row table being replaced
    /// wholesale. The other side is kept, so a replacement that leaves the
    /// postings alone must reproduce exactly what they cover.
    pub(super) fn reset(
        &mut self,
        write_txn: &redb::WriteTransaction,
        table: &str,
        declaration: &IndexDecl,
        side: Side,
    ) -> Result<()> {
        let touched = self.index(write_txn, table, declaration)?;
        match side {
            Side::Postings => touched.postings = BoundaryState::empty(),
            Side::Versions => touched.versions = BoundaryState::empty(),
        }
        Ok(())
    }

    /// Write rows of a table whose indexes are rebuilt later, in another
    /// transaction (a snapshot artifact is filled row batch by row batch and
    /// indexed once at the end). Its states are neither kept nor published;
    /// [`Self::finish`] requires the index to hold no posting yet and
    /// withdraws any publication, so a reader sees an index with no postings
    /// and nothing to trust until the rebuild publishes it.
    pub(super) fn defer_until_rebuild(&mut self, table: &str, indexes: &[IndexDecl]) -> Result<()> {
        for declaration in indexes {
            if !self.deferred.contains(table, &declaration.name) {
                self.funding
                    .admit(pair_bytes(table, &declaration.name), "deferred_index")?;
                self.deferred.insert(table, &declaration.name, ());
            }
        }
        Ok(())
    }

    /// Forget both states of a (table, index) whose index or table is
    /// dropped. Unless the pair is written again in this transaction, its
    /// states and publication are removed at [`Self::finish`], which also
    /// requires its posting table to be gone or empty.
    pub(super) fn drop_index(&mut self, table: &str, index: &str) -> Result<()> {
        self.touched.remove(table, index);
        if !self.dropped.contains(table, index) {
            self.funding
                .admit(pair_bytes(table, index), "dropped_index")?;
            self.dropped.insert(table, index, ());
        }
        Ok(())
    }

    /// Store every touched pair, refuse unequal ones, and return the
    /// authenticated publications and the removals of cleared pairs, with the
    /// funding that pays for them.
    pub(super) fn finish(mut self, write_txn: &redb::WriteTransaction) -> Result<Publications> {
        let pairs = self
            .touched
            .len()
            .saturating_add(self.dropped.len())
            .saturating_add(self.deferred.len());
        if pairs == 0 {
            return Ok(Publications {
                mutations: Vec::new(),
                funding: self.funding,
            });
        }
        // Each publication's key, value and mutation, every state key, and
        // the states encoded back, before any of them is built.
        let names: usize = self
            .touched
            .iter()
            .map(|(table, index, _)| (table, index))
            .chain(self.dropped.iter().map(|(table, index, _)| (table, index)))
            .chain(self.deferred.iter().map(|(table, index, _)| (table, index)))
            .map(|(table, index)| table.len() + index.len())
            .sum();
        self.funding.admit(
            pairs
                .saturating_mul(
                    std::mem::size_of::<content_identity::ContentIdentityMutation>()
                        .saturating_add(PUBLICATION_NAMESPACE.len())
                        .saturating_add(256),
                )
                .saturating_add(names.saturating_mul(4))
                .saturating_add(STATE_BYTES),
            "publications",
        )?;
        let mut mutations = Vec::with_capacity(pairs);
        let mut states = write_txn
            .open_table(HISTORICAL_INDEX_COMMITMENT_TABLE)
            .map_err(RedbPersistence::storage_error)?;
        for (table, index, ()) in self.dropped.iter() {
            if self.touched.contains(table, index) {
                continue;
            }
            let name = RedbPersistence::relational_index_table_name(table, index);
            let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                TableDefinition::new(name.as_str());
            // Opening creates a missing table; deleting it again leaves the
            // transaction as it was.
            let left = !write_txn
                .open_table(definition)
                .map_err(RedbPersistence::storage_error)?
                .is_empty()
                .map_err(RedbPersistence::storage_error)?;
            if !left {
                write_txn
                    .delete_table(definition)
                    .map_err(RedbPersistence::storage_error)?;
            } else {
                return Err(Error::StoreCorrupted {
                    path: name,
                    reason: "a dropped historical index still holds postings".to_owned(),
                });
            }
            for side in [Side::Postings, Side::Versions] {
                states
                    .remove(state_key(table, index, side).as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            mutations.push(content_identity::remove_status_mutation(
                PUBLICATION_NAMESPACE,
                &publication_key(table, index),
            ));
        }
        for (table, index, touched) in self.touched.iter() {
            if self.deferred.contains(table, index) {
                let name = RedbPersistence::relational_index_table_name(table, index);
                let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                    TableDefinition::new(name.as_str());
                if !write_txn
                    .open_table(definition)
                    .map_err(RedbPersistence::storage_error)?
                    .is_empty()
                    .map_err(RedbPersistence::storage_error)?
                {
                    return Err(Error::StoreCorrupted {
                        path: name,
                        reason: "rows were written past a historical index that already holds \
                                 postings"
                            .to_owned(),
                    });
                }
                // The rebuild derives both states afresh; nothing is kept that
                // a reader could mistake for a publication.
                for side in [Side::Postings, Side::Versions] {
                    states
                        .remove(state_key(table, index, side).as_slice())
                        .map_err(RedbPersistence::storage_error)?;
                }
                mutations.push(content_identity::remove_status_mutation(
                    PUBLICATION_NAMESPACE,
                    &publication_key(table, index),
                ));
                continue;
            }
            if touched.postings != touched.versions {
                return Err(Error::StoreCorrupted {
                    path: table.to_owned(),
                    reason: format!(
                        "historical index {index} postings do not cover what its row versions \
                         cover"
                    ),
                });
            }
            for (side, state) in [
                (Side::Postings, &touched.postings),
                (Side::Versions, &touched.versions),
            ] {
                states
                    .insert(
                        state_key(table, index, side).as_slice(),
                        state.encode().as_slice(),
                    )
                    .map_err(RedbPersistence::storage_error)?;
            }
            mutations.push(publication(
                table,
                index,
                &touched.postings,
                &touched.versions,
                &touched.identity,
            ));
        }
        Ok(Publications {
            mutations,
            funding: self.funding,
        })
    }
}

/// A batch's publications and the funding that paid for them. The caller
/// moves the mutations into its own list with [`Self::into_parts`] and keeps
/// the returned [`CommitmentCredit`] alive for as long as that list holds
/// them, so their bytes stay charged until they are destroyed.
pub(super) struct Publications {
    mutations: Vec<content_identity::ContentIdentityMutation>,
    funding: Funding,
}

/// The writer-budget charge for a batch's publications; dropping it refunds
/// them.
pub(crate) struct CommitmentCredit {
    _funding: Option<Funding>,
}

impl Publications {
    pub(super) fn into_parts(
        self,
    ) -> (
        Vec<content_identity::ContentIdentityMutation>,
        CommitmentCredit,
    ) {
        (
            self.mutations,
            CommitmentCredit {
                _funding: Some(self.funding),
            },
        )
    }
}

fn load(
    write_txn: &redb::WriteTransaction,
    table: &str,
    index: &str,
    side: Side,
) -> Result<BoundaryState> {
    let states = match write_txn.open_table(HISTORICAL_INDEX_COMMITMENT_TABLE) {
        Ok(states) => states,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(BoundaryState::empty()),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    match states
        .get(state_key(table, index, side).as_slice())
        .map_err(RedbPersistence::storage_error)?
    {
        Some(stored) => BoundaryState::decode(stored.value()),
        None => Ok(BoundaryState::empty()),
    }
}

/// What a reader authenticates for one (table, index): the content-identity
/// entry its stored states and declaration imply, and whether those states
/// are equal. `None` when nothing is published for the pair.
pub(super) fn published_in_read(
    read: &redb::ReadTransaction,
    table: &str,
    declaration: &IndexDecl,
) -> Result<Option<(content_identity::ContentIdentityMutation, bool)>> {
    let states = match read.open_table(HISTORICAL_INDEX_COMMITMENT_TABLE) {
        Ok(states) => states,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let side = |side| -> Result<Option<BoundaryState>> {
        states
            .get(state_key(table, &declaration.name, side).as_slice())
            .map_err(RedbPersistence::storage_error)?
            .map(|stored| BoundaryState::decode(stored.value()))
            .transpose()
    };
    let (Some(postings), Some(versions)) = (side(Side::Postings)?, side(Side::Versions)?) else {
        return Ok(None);
    };
    let identity = index_identity(declaration)?;
    let entry = publication(table, &declaration.name, &postings, &versions, &identity);
    Ok(Some((entry, postings == versions)))
}

/// The historical posting table of one (table, index), writable only through
/// methods that fold the postings state from the actual old and new posting
/// values and enforce the interval invariant. Reads go through `Deref`; there
/// is deliberately no `DerefMut`.
pub(super) struct HistoricalPostings<'a> {
    inner: redb::Table<'a, DurableRelationalIndexKey, &'static [u8]>,
    table: String,
    declaration: IndexDecl,
}

impl<'a> std::ops::Deref for HistoricalPostings<'a> {
    type Target = redb::Table<'a, DurableRelationalIndexKey, &'static [u8]>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a> HistoricalPostings<'a> {
    pub(super) fn open(
        write_txn: &'a redb::WriteTransaction,
        table: &str,
        declaration: &IndexDecl,
    ) -> std::result::Result<Self, redb::TableError> {
        let name = RedbPersistence::relational_index_table_name(table, &declaration.name);
        let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
            TableDefinition::new(name.as_str());
        Ok(Self {
            inner: write_txn.open_table(definition)?,
            table: table.to_owned(),
            declaration: declaration.clone(),
        })
    }

    fn corrupt(&self, reason: &str) -> Error {
        Error::StoreCorrupted {
            path: RedbPersistence::relational_index_table_name(&self.table, &self.declaration.name),
            reason: reason.to_owned(),
        }
    }

    fn end_at(&self, key: &DurableRelationalIndexKey) -> Result<Option<Option<TxId>>> {
        self.inner
            .get(key)
            .map_err(RedbPersistence::storage_error)?
            .map(|value| RedbPersistence::decode_exact::<Option<TxId>>(value.value()))
            .transpose()
    }

    /// The postings of `(key, row)` immediately before and after `start`.
    #[allow(
        clippy::type_complexity,
        reason = "a neighbour is its start and its end, each side optional"
    )]
    fn neighbours(
        &self,
        key: &IndexKey,
        row: RowId,
        start: TxId,
    ) -> Result<(Option<(TxId, Option<TxId>)>, Option<(TxId, Option<TxId>)>)> {
        let at = DurableRelationalIndexKey::new(key.clone(), row, start)?;
        let low = DurableRelationalIndexKey::new(key.clone(), row, TxId(0))?;
        let high = DurableRelationalIndexKey::new(key.clone(), row, OPEN_END)?;
        let decode = |entry: Option<
            std::result::Result<
                (
                    redb::AccessGuard<'_, DurableRelationalIndexKey>,
                    redb::AccessGuard<'_, &'static [u8]>,
                ),
                redb::StorageError,
            >,
        >|
         -> Result<Option<(TxId, Option<TxId>)>> {
            let Some(entry) = entry else { return Ok(None) };
            let (position, end) = entry.map_err(RedbPersistence::storage_error)?;
            let start = position.value().decode()?.created_tx;
            let end = RedbPersistence::decode_exact::<Option<TxId>>(end.value())?;
            Ok(Some((start, end)))
        };
        let before = decode(
            self.inner
                .range(low..at.clone())
                .map_err(RedbPersistence::storage_error)?
                .next_back(),
        )?;
        let after = decode(
            self.inner
                .range((
                    std::ops::Bound::Excluded(at),
                    std::ops::Bound::Included(high),
                ))
                .map_err(RedbPersistence::storage_error)?
                .next(),
        )?;
        Ok((before, after))
    }

    /// The interval invariant for `[start, end)` among its neighbours.
    fn check_interval(
        &self,
        key: &IndexKey,
        row: RowId,
        start: TxId,
        end: Option<TxId>,
    ) -> Result<()> {
        if end.is_some_and(|end| end <= start) || start == OPEN_END {
            return Err(self.corrupt("historical posting interval is empty or inverted"));
        }
        let (before, after) = self.neighbours(key, row, start)?;
        if let Some((_, previous_end)) = before
            && previous_end.is_none_or(|previous_end| previous_end >= start)
        {
            return Err(self.corrupt(
                "historical posting overlaps or touches the previous interval of its row",
            ));
        }
        if let Some((next_start, _)) = after
            && end.is_none_or(|end| end >= next_start)
        {
            return Err(
                self.corrupt("historical posting overlaps or touches the next interval of its row")
            );
        }
        Ok(())
    }

    /// Write the interval starting at `position` with `end`, replacing
    /// whatever interval started there.
    pub(super) fn put(
        &mut self,
        batch: &mut CommitmentBatch,
        write_txn: &redb::WriteTransaction,
        position: DurableRelationalIndexKey,
        end: Option<TxId>,
    ) -> Result<()> {
        batch.admit_transient(position.0.len().saturating_mul(4).saturating_add(256))?;
        let decoded = position.decode()?;
        self.check_interval(&decoded.key, decoded.row_id, decoded.created_tx, end)?;
        if let Some(previous) = self.end_at(&position)? {
            batch.fold_interval(
                write_txn,
                &self.table,
                &self.declaration,
                Side::Postings,
                &key_row(&decoded.key, decoded.row_id)?,
                decoded.created_tx,
                previous,
                false,
            )?;
        }
        batch.fold_interval(
            write_txn,
            &self.table,
            &self.declaration,
            Side::Postings,
            &key_row(&decoded.key, decoded.row_id)?,
            decoded.created_tx,
            end,
            true,
        )?;
        self.inner
            .insert(position, RedbPersistence::encode(&end)?.as_slice())
            .map_err(RedbPersistence::storage_error)?;
        Ok(())
    }

    /// Remove the interval starting at `position`, if any.
    pub(super) fn remove(
        &mut self,
        batch: &mut CommitmentBatch,
        write_txn: &redb::WriteTransaction,
        position: &DurableRelationalIndexKey,
    ) -> Result<()> {
        let Some(previous) = self.end_at(position)? else {
            return Ok(());
        };
        batch.admit_transient(position.0.len().saturating_mul(4).saturating_add(256))?;
        let decoded = position.decode()?;
        batch.fold_interval(
            write_txn,
            &self.table,
            &self.declaration,
            Side::Postings,
            &key_row(&decoded.key, decoded.row_id)?,
            decoded.created_tx,
            previous,
            false,
        )?;
        self.inner
            .remove(position)
            .map_err(RedbPersistence::storage_error)?;
        Ok(())
    }
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    /// Production-dead: the next postings fold on this thread is skipped, as a
    /// writer defect that maintains a posting without its commitment would.
    static SKIP_NEXT_POSTINGS_FOLD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(any(test, feature = "test-seams"))]
fn take_postings_fold_skip_for_test() -> bool {
    SKIP_NEXT_POSTINGS_FOLD.with(|skip| skip.replace(false))
}

/// Production-dead: make the next postings fold on this thread a no-op.
#[cfg(any(test, feature = "test-seams"))]
pub fn skip_next_postings_fold_for_test() {
    SKIP_NEXT_POSTINGS_FOLD.with(|skip| skip.set(true));
}

/// Production-dead: change one lane of a (table, index)'s stored postings
/// state, as storage damage would, leaving its publication as it was.
#[cfg(any(test, feature = "test-seams"))]
pub(super) fn damage_postings_state_for_test(
    write_txn: &redb::WriteTransaction,
    table: &str,
    index: &str,
) -> Result<()> {
    let mut states = write_txn
        .open_table(HISTORICAL_INDEX_COMMITMENT_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let key = state_key(table, index, Side::Postings);
    let mut state = match states
        .get(key.as_slice())
        .map_err(RedbPersistence::storage_error)?
    {
        Some(stored) => BoundaryState::decode(stored.value())?,
        None => {
            return Err(Error::Other(
                "no historical index commitment to damage".into()));
        }
    };
    state.lanes[0] = state.lanes[0].wrapping_add(1);
    states
        .insert(key.as_slice(), state.encode().as_slice())
        .map_err(RedbPersistence::storage_error)?;
    Ok(())
}

/// What [`verify_in_read`] established for one historical index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoricalIndexVerification {
    pub index: String,
    pub postings: u64,
    pub versions: u64,
}

/// Recompute both commitments of every historical index of `table` from the
/// stored postings and row versions, check the interval and lifetime
/// invariants the writer enforces at each mutation, and require the stored
/// states to equal the recomputed ones and to authenticate. The walk visits
/// every posting and row version once and holds two states per index.
///
/// The walk is a read like any other: `checkpoint` runs before every posting
/// and row version, so the request's cancellation and active-time limit stop
/// it mid-walk, and its scratch, keys and answer are charged to the
/// requesting reader's `memory` before they are allocated.
pub(super) fn verify_in_read(
    read: &redb::ReadTransaction,
    verifier: &content_identity::ContentIdentityReadVerifier,
    table: &str,
    meta: &TableMeta,
    memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<Vec<HistoricalIndexVerification>> {
    const OPERATION: &str = "historical_index_verification";
    const HINT: &str = "Raise MEMORY_LIMIT to verify this table's historical indexes.";
    let declaration_bytes = meta
        .indexes
        .iter()
        .map(|index| {
            index
                .name
                .len()
                .saturating_add(
                    index
                        .columns
                        .iter()
                        .map(|(column, _)| column.len() + 16)
                        .sum(),
                )
                .saturating_add(96)
        })
        .sum::<usize>()
        .saturating_mul(4);
    let credit_budget = memory.clone();
    // Two states, two elements and their expansion, and the declarations.
    let mut credit = contextdb_core::read_memory::ReadCredit::try_new(
        memory,
        (6 * STATE_BYTES).saturating_add(declaration_bytes),
        "read",
        OPERATION,
        HINT,
    )?;
    // Per-record key custody: the bytes one record's keys hold at once,
    // grown to its high-water mark before each allocation and reused.
    let mut keys = KeyCredit {
        credit: contextdb_core::read_memory::ReadCredit::new(credit_budget.clone()),
        held: 0,
        record: 0,
        operation: OPERATION,
        hint: HINT,
    };
    checkpoint()?;
    let declarations = RedbPersistence::relational_index_declarations(meta);
    credit.try_grow_for(
        declarations
            .iter()
            .map(|declaration| declaration.name.len().saturating_add(64))
            .sum(),
        "read",
        OPERATION,
        HINT,
    )?;
    let mut verified = Vec::with_capacity(declarations.len());
    // One fixed scratch every vector projection of this verification reuses,
    // admitted once: no per-version charge scales with a vector's width.
    credit.try_grow_for(
        content_identity::version_manifest::VECTOR_SCRATCH_BYTES,
        "read",
        OPERATION,
        HINT,
    )?;
    let mut vector_scratch = vec![0_u8; content_identity::version_manifest::VECTOR_SCRATCH_BYTES];
    let rows_name = RedbPersistence::rel_table_name(table);
    for declaration in &declarations {
        let index_name = RedbPersistence::relational_index_table_name(table, &declaration.name);
        let corrupt = |path: &str, reason: String| Error::StoreCorrupted {
            path: path.to_owned(),
            reason,
        };
        let identity = index_identity(declaration)?;
        let mut postings = BoundaryState::empty();
        let mut posting_count = 0_u64;
        match read.open_table(TableDefinition::<
            super::writer_source::BorrowedRelationalIndexKey,
            &[u8],
        >::new(index_name.as_str()))
        {
            Ok(index) => {
                let mut previous: Option<(Vec<u8>, TxId, Option<TxId>)> = None;
                for entry in index.iter().map_err(RedbPersistence::storage_error)? {
                    checkpoint()?;
                    let (position, end) = entry.map_err(RedbPersistence::storage_error)?;
                    // The posting is read in place; only its (key, row) group
                    // is copied, after the copy is admitted beside the
                    // previous group it is compared with.
                    let raw = position.value();
                    note_verify_phase("posting_walk");
                    keys.start_record(previous.as_ref().map_or(0, |(group, _, _)| group.len()));
                    keys.admit(raw.len().saturating_add(32))?;
                    let (suffix, row_id, start) =
                        content_identity::version_manifest::scalar_posting_identity_with_checkpoint(
                            raw, checkpoint,
                        )?;
                    let mut group = Vec::with_capacity(suffix.saturating_add(32));
                    note_verify_phase("group_copy");
                    content_identity::version_manifest::append_with_checkpoint(
                        &mut group,
                        &raw[..suffix],
                        checkpoint,
                    )?;
                    bincode::serde::encode_into_std_write(
                        (row_id, TxId(0)),
                        &mut group,
                        bincode::config::standard(),
                    )
                    .map_err(|error| Error::Other((format!("bincode encode error: {error}")).into()))?;
                    let end = RedbPersistence::decode_exact::<Option<TxId>>(end.value())?;
                    if end.is_some_and(|end| end <= start) || start == OPEN_END {
                        return Err(corrupt(
                            &index_name,
                            "historical posting interval is empty or inverted".into(),
                        ));
                    }
                    note_verify_phase("group_compare");
                    if let Some((previous_group, _, previous_end)) = &previous
                        && previous_end.is_none_or(|previous_end| previous_end >= start)
                        && content_identity::version_manifest::equal_with_checkpoint(
                            previous_group,
                            &group,
                            checkpoint,
                        )?
                    {
                        return Err(corrupt(
                            &index_name,
                            "historical postings of one row overlap or touch".into(),
                        ));
                    }
                    postings.apply(
                        &*element_checked(table, &identity, &group, start, checkpoint)?,
                        true,
                    );
                    postings.apply(
                        &*element_checked(
                            table,
                            &identity,
                            &group,
                            end.unwrap_or(OPEN_END),
                            checkpoint,
                        )?,
                        false,
                    );
                    posting_count += 1;
                    previous = Some((group, start, end));
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(RedbPersistence::storage_error(error)),
        }
        let mut versions = BoundaryState::empty();
        let mut version_count = 0_u64;
        match read.open_table(TableDefinition::<&[u8], &[u8]>::new(rows_name.as_str())) {
            Ok(rows) => {
                let mut previous: Option<(RowId, Option<TxId>)> = None;
                for entry in rows.iter().map_err(RedbPersistence::storage_error)? {
                    checkpoint()?;
                    let (key, body) = entry.map_err(RedbPersistence::storage_error)?;
                    let row = key
                        .value()
                        .get(..8)
                        .and_then(|bytes| bytes.try_into().ok())
                        .map(|bytes| RowId(u64::from_be_bytes(bytes)))
                        .ok_or_else(|| corrupt(&rows_name, "row key is too short".into()))?;
                    note_verify_phase("body_walk");
                    let (created, deleted) =
                        content_identity::version_manifest::body_lifetime_with_checkpoint(
                            body.value(),
                            checkpoint,
                        )?;
                    if let Some((previous_row, previous_deleted)) = previous
                        && previous_row == row
                        && previous_deleted
                            .is_none_or(|previous_deleted| previous_deleted > created)
                    {
                        return Err(corrupt(
                            &rows_name,
                            "row versions of one row overlap in lifetime".into(),
                        ));
                    }
                    keys.start_record(0);
                    // The version's key is streamed straight into its element
                    // hash, once; its two boundaries finish clones of it.
                    note_verify_phase("hash");
                    let mut hasher = element_prefix(table, &identity, checkpoint)?;
                    content_identity::version_manifest::project_posting_key(
                        body.value(),
                        declaration,
                        checkpoint,
                        &mut |bytes| keys.admit(bytes),
                        Some(&mut vector_scratch),
                        content_identity::version_manifest::KeySuffix::Zero,
                        &mut content_identity::version_manifest::HashKey(&mut hasher),
                    )?
                    .ok_or_else(|| Error::Other("a posting key could not be built".into()))?;
                    versions.apply(&*element_finish(hasher.clone(), created, checkpoint)?, true);
                    versions.apply(
                        &*element_finish(hasher, deleted.unwrap_or(OPEN_END), checkpoint)?,
                        false,
                    );
                    version_count += 1;
                    previous = Some((row, deleted));
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(RedbPersistence::storage_error(error)),
        }
        if postings != versions {
            return Err(corrupt(
                &index_name,
                "historical index postings do not cover what its row versions cover".into(),
            ));
        }
        match published_in_read(read, table, declaration)? {
            Some((entry, equal)) => {
                verifier.verify(&entry)?;
                let stored_postings = BoundaryState::decode(&entry_state_bytes(
                    read,
                    table,
                    &declaration.name,
                    Side::Postings,
                )?)?;
                if !equal || stored_postings != postings {
                    return Err(corrupt(
                        &index_name,
                        "stored historical index commitment disagrees with its postings".into(),
                    ));
                }
            }
            None if posting_count == 0 => {}
            None => {
                return Err(corrupt(
                    &index_name,
                    "historical index postings carry no commitment".into(),
                ));
            }
        }
        verified.push(HistoricalIndexVerification {
            index: declaration.name.clone(),
            postings: posting_count,
            versions: version_count,
        });
    }
    Ok(verified)
}

/// A reader's charge for the keys one record holds at once: `start_record`
/// begins a record already holding `retained` bytes, and `admit` grows the
/// charge to the record's running total before each allocation. The charge
/// only grows, so it is reused by every later, smaller record.
struct KeyCredit {
    credit: contextdb_core::read_memory::ReadCredit,
    held: usize,
    record: usize,
    operation: &'static str,
    hint: &'static str,
}

impl KeyCredit {
    fn start_record(&mut self, retained: usize) {
        self.record = retained;
    }
    fn admit(&mut self, bytes: usize) -> Result<()> {
        self.record = self.record.saturating_add(bytes);
        if self.record > self.held {
            self.credit
                .try_grow_for(self.record - self.held, "read", self.operation, self.hint)?;
            self.held = self.record;
        }
        Ok(())
    }
}

fn entry_state_bytes(
    read: &redb::ReadTransaction,
    table: &str,
    index: &str,
    side: Side,
) -> Result<Vec<u8>> {
    let states = read
        .open_table(HISTORICAL_INDEX_COMMITMENT_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let stored = states
        .get(state_key(table, index, side).as_slice())
        .map_err(RedbPersistence::storage_error)?
        .ok_or_else(|| Error::Other("historical index commitment state is absent".into()))?;
    Ok(stored.value().to_vec())
}

impl RedbPersistence {
    /// [`verify_in_read`] against the committed image.
    pub(crate) fn verify_historical_indexes(
        &self,
        table: &str,
        meta: &TableMeta,
        memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Vec<HistoricalIndexVerification>> {
        // Funded by the requesting reader, never by the writer.
        let owner = crate::read_image_memory::ImageMemory::operation_reader(
            self.require_decode_accountant()?,
            memory.clone(),
        )?;
        let _decode_scope = owner.enter();
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let verifier = content_identity::ContentIdentityReadVerifier::open(
                &read,
                &self.path,
                self.proof_accountant(),
            )?;
            verify_in_read(&read, &verifier, table, meta, memory, checkpoint)
        })
    }
}

impl RetainedReadSource {
    /// [`verify_in_read`] against this source's pinned committed root.
    pub(crate) fn verify_historical_indexes(
        &self,
        table: &str,
        meta: &TableMeta,
        memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Vec<HistoricalIndexVerification>> {
        // Funded by the requesting reader.
        let owner = crate::read_image_memory::ImageMemory::operation_reader(
            self._source_memory.memory.accountant.clone(),
            memory.clone(),
        )?;
        let _decode_scope = owner.enter();
        self.with_read(|read| {
            verify_in_read(
                read,
                &self.content_identity,
                table,
                meta,
                memory,
                checkpoint,
            )
        })
    }
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    /// Production-dead: refuse the next commitment admission on this thread.
    static REFUSE_NEXT_ADMISSION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Production-dead: bytes commitment batches admitted on this thread.
    static ADMITTED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(any(test, feature = "test-seams"))]
fn take_admission_refusal_for_test() -> bool {
    REFUSE_NEXT_ADMISSION.with(|refuse| refuse.replace(false))
}

#[cfg(any(test, feature = "test-seams"))]
fn note_admitted_for_test(bytes: usize) {
    ADMITTED.with(|admitted| admitted.set(admitted.get().saturating_add(bytes)));
}

/// Production-dead: make the next commitment admission on this thread refuse
/// as the writer budget would.
#[cfg(any(test, feature = "test-seams"))]
pub fn refuse_next_commitment_admission_for_test() {
    REFUSE_NEXT_ADMISSION.with(|refuse| refuse.set(true));
}

/// Production-dead: bytes commitment batches have admitted on this thread
/// since the last call, which resets it.
#[cfg(any(test, feature = "test-seams"))]
pub fn take_commitment_admitted_bytes_for_test() -> usize {
    ADMITTED.with(|admitted| admitted.replace(0))
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    /// Production-dead: the next index retirement on this thread leaves its
    /// historical posting table in place, as a writer defect would.
    static SKIP_NEXT_RETIREMENT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn take_retirement_skip_for_test() -> bool {
    SKIP_NEXT_RETIREMENT.with(|skip| skip.replace(false))
}

/// Production-dead: make the next index retirement on this thread skip
/// deleting its historical posting table.
#[cfg(any(test, feature = "test-seams"))]
pub fn skip_next_index_retirement_for_test() {
    SKIP_NEXT_RETIREMENT.with(|skip| skip.set(true));
}

/// Production-dead: whether either commitment state of a (table, index) is
/// stored.
#[cfg(any(test, feature = "test-seams"))]
pub(super) fn states_stored_for_test(
    read: &redb::ReadTransaction,
    table: &str,
    index: &str,
) -> Result<bool> {
    let states = match read.open_table(HISTORICAL_INDEX_COMMITMENT_TABLE) {
        Ok(states) => states,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    for side in [Side::Postings, Side::Versions] {
        if states
            .get(state_key(table, index, side).as_slice())
            .map_err(RedbPersistence::storage_error)?
            .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod verification_tests {
    //! `.verify history` is a read like any other: it stops mid-walk at its
    //! request's cancellation or active time, and it is charged to the reader
    //! that asked, refusing a budget too small for it.
    use crate::Database;
    use contextdb_core::read_contract::{
        DeadlineClock, DeadlineWait, OwnerReadCancellation, ReadFailureDetail, ReadFailureLimit,
        ReadLimits,
    };
    use contextdb_core::{Error, Value};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn history(path: &std::path::Path) -> Database {
        let db = Database::open(path).unwrap();
        let run = |sql: &str| {
            db.execute(sql, &HashMap::new()).unwrap();
        };
        run("CREATE TABLE tagged (id INT PRIMARY KEY, tag TEXT) HISTORY ALL");
        run("CREATE INDEX tagged_by_tag ON tagged (tag)");
        for id in 0..32 {
            run(&format!("INSERT INTO tagged VALUES ({id}, 'a')"));
            run(&format!("UPDATE tagged SET tag = 'b' WHERE id = {id}"));
        }
        db
    }

    #[test]
    fn verification_stops_mid_walk_at_its_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let db = history(&directory.path().join("checkpoint.db"));
        let reader = db.declared_reader_memory().unwrap();
        let mut steps = 0_u64;
        db.verify_historical_indexes_within("tagged", reader.clone(), &mut || {
            steps += 1;
            Ok(())
        })
        .expect("an uninterrupted walk verifies");
        assert!(steps > 64, "every posting and version is a step: {steps}");
        let stop_at = steps / 2;
        let mut taken = 0_u64;
        let refused = db.verify_historical_indexes_within("tagged", reader, &mut || {
            taken += 1;
            if taken == stop_at {
                return Err(Error::ReadCancelled);
            }
            Ok(())
        });
        assert!(matches!(refused, Err(Error::ReadCancelled)), "{refused:?}");
        assert_eq!(
            taken, stop_at,
            "the walk stopped at the step it was cancelled"
        );
        db.close().unwrap();
    }

    #[derive(Debug)]
    struct Budget(Arc<crate::memory_accounting::MemoryAccountant>);
    impl contextdb_core::read_memory::ReadMemoryBudget for Budget {
        fn try_reserve(
            &self,
            bytes: usize,
            subsystem: &'static str,
            operation: &'static str,
            hint: &'static str,
        ) -> contextdb_core::Result<()> {
            self.0.try_allocate_for(bytes, subsystem, operation, hint)
        }
        fn release(&self, bytes: usize) {
            self.0.release(bytes);
        }
    }

    #[test]
    fn verification_is_charged_to_its_reader_and_refuses_a_tiny_budget() {
        use crate::memory_accounting::MemoryAccountant;
        let directory = tempfile::tempdir().unwrap();
        let db = history(&directory.path().join("budget.db"));
        let tiny = Arc::new(MemoryAccountant::with_budget(1024));
        let refused = db.verify_historical_indexes_within(
            "tagged",
            Arc::new(Budget(tiny.clone())),
            &mut || Ok(()),
        );
        assert!(
            matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
            "a reader budget too small for the walk refuses it: {refused:?}"
        );
        assert_eq!(tiny.usage().used, 0, "the refusal keeps nothing charged");
        let roomy = Arc::new(MemoryAccountant::with_budget(64 * 1024 * 1024));
        let verified = db
            .verify_historical_indexes_within(
                "tagged",
                Arc::new(Budget(roomy.clone())),
                &mut || Ok(()),
            )
            .expect("a roomy reader verifies on retry");
        assert!(verified.iter().any(|index| index.index == "tagged_by_tag"));
        assert_eq!(
            roomy.usage().used,
            0,
            "the answer's walk released every charge"
        );
        db.close().unwrap();
    }

    /// A clock that moves one millisecond per reading and cancels at a chosen
    /// reading.
    struct SteppingClock {
        reads: AtomicU64,
        cancel_at: Option<(u64, OwnerReadCancellation)>,
    }
    impl DeadlineClock for SteppingClock {
        fn now_ms(&self) -> u64 {
            let read = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
            if let Some((at, cancellation)) = &self.cancel_at
                && read == *at
            {
                cancellation.cancel();
            }
            read
        }
        fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
            Box::pin(async {})
        }
    }

    fn live_answer(
        db: &Database,
        cancel_at: Option<u64>,
        active_ms: u64,
    ) -> (
        std::result::Result<
            crate::direct_file_reader::DirectMetadataBody,
            crate::direct_file_reader::DirectFileReaderError,
        >,
        u64,
    ) {
        let cancellation = OwnerReadCancellation::new();
        let clock = Arc::new(SteppingClock {
            reads: AtomicU64::new(0),
            cancel_at: cancel_at.map(|at| (at, cancellation.clone())),
        });
        let limits = ReadLimits {
            active_ms,
            ..ReadLimits::default()
        };
        let answer = crate::read_image::project_metadata_from_database(
            db,
            db.declared_reader_memory().unwrap(),
            crate::direct_file_reader::DirectMetadataRequest::VerifyHistory {
                table: "tagged".into(),
            },
            limits,
            clock.clone(),
            None,
            &cancellation,
        )
        .map(|(body, _)| body);
        (answer, clock.reads.load(Ordering::SeqCst))
    }

    #[test]
    fn the_in_process_route_stops_verification_at_cancellation_and_active_time() {
        let directory = tempfile::tempdir().unwrap();
        let db = history(&directory.path().join("live.db"));
        let (complete, steps) = live_answer(&db, None, u64::MAX / 4);
        assert!(
            matches!(
                complete,
                Ok(crate::direct_file_reader::DirectMetadataBody::VerifyHistory { .. })
            ),
            "{complete:?}"
        );
        let (cancelled, cancelled_steps) = live_answer(&db, Some(steps / 2), u64::MAX / 4);
        assert!(
            matches!(
                cancelled,
                Err(crate::direct_file_reader::DirectFileReaderError::Cancelled)
            ),
            "a request cancelled mid-walk is answered as cancelled: {cancelled:?}"
        );
        assert!(cancelled_steps < steps, "{cancelled_steps} of {steps}");
        let (expired, expired_steps) = live_answer(&db, None, steps / 2);
        match expired {
            Err(crate::direct_file_reader::DirectFileReaderError::ReadFailure(failure)) => {
                match failure.detail() {
                    ReadFailureDetail::OwnerLimitExceeded(detail) => {
                        assert_eq!(detail.limit, ReadFailureLimit::ActiveMs)
                    }
                    other => panic!("the refusal names the active-time limit: {other:?}"),
                }
            }
            other => panic!("a walk out of active time refuses: {other:?}"),
        }
        assert!(expired_steps < steps, "{expired_steps} of {steps}");
        db.close().unwrap();
        let _ = Value::Null;
    }
}

#[cfg(test)]
mod interval_invariant_tests {
    //! Boundary sums prove coverage, not decomposition, so every posting
    //! write enforces the decomposition against its neighbours, and the
    //! commitment refuses whatever changes coverage: an endpoint moved, an
    //! interval stretched across a gap, overlapping or touching intervals.
    use super::*;
    use contextdb_core::{DirectedValue, SortDirection, TotalOrdAsc};

    fn declaration() -> IndexDecl {
        IndexDecl {
            name: "by_tag".into(),
            columns: vec![("tag".into(), SortDirection::Asc)],
            kind: IndexKind::UserDeclared,
        }
    }

    fn key(tag: &str) -> IndexKey {
        vec![DirectedValue::Asc(TotalOrdAsc(Value::Text(tag.into())))]
    }

    fn at(tag: &str, start: u64) -> DurableRelationalIndexKey {
        DurableRelationalIndexKey::new(key(tag), RowId(1), TxId(start)).unwrap()
    }

    /// Run `body` against a fresh posting table and batch, then finish.
    fn with_postings(
        body: impl FnOnce(
            &mut HistoricalPostings<'_>,
            &mut CommitmentBatch,
            &redb::WriteTransaction,
        ) -> Result<()>,
    ) -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let database = redb_builder()
            .create(directory.path().join("intervals.redb"))
            .unwrap();
        let _scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
            crate::read_image_memory::ImageMemory::default_decode_accountant(),
        )?;
        let write = database.begin_write().unwrap();
        let mut batch = CommitmentBatch::for_writer()?;
        {
            let mut postings = HistoricalPostings::open(&write, "t", &declaration()).unwrap();
            body(&mut postings, &mut batch, &write)?;
        }
        batch.finish(&write).map(|_| ())
    }

    /// Fold one row version's lifetime under `tag` into the versions side.
    fn version(
        batch: &mut CommitmentBatch,
        write: &redb::WriteTransaction,
        tag: &str,
        start: u64,
        end: Option<u64>,
    ) -> Result<()> {
        batch.fold_interval(
            write,
            "t",
            &declaration(),
            Side::Versions,
            &key_row(&key(tag), RowId(1))?,
            TxId(start),
            end.map(TxId),
            true,
        )
    }

    fn corrupt(result: Result<()>, case: &str) {
        assert!(
            matches!(result, Err(Error::StoreCorrupted { .. })),
            "{case}: refused as corruption, got {result:?}"
        );
    }

    #[test]
    fn a_coalesced_run_over_its_versions_commits() {
        with_postings(|postings, batch, write| {
            version(batch, write, "a", 1, Some(4))?;
            version(batch, write, "a", 4, Some(8))?;
            postings.put(batch, write, at("a", 1), Some(TxId(8)))
        })
        .expect("one coalesced posting covers two contiguous versions of one key");
    }

    #[test]
    fn overlapping_touching_empty_and_inverted_postings_are_refused_at_write() {
        corrupt(
            with_postings(|postings, batch, write| {
                postings.put(batch, write, at("a", 5), Some(TxId(10)))?;
                postings.put(batch, write, at("a", 7), Some(TxId(12)))
            }),
            "overlap with the previous interval",
        );
        corrupt(
            with_postings(|postings, batch, write| {
                postings.put(batch, write, at("a", 5), Some(TxId(10)))?;
                postings.put(batch, write, at("a", 3), Some(TxId(6)))
            }),
            "overlap with the next interval",
        );
        corrupt(
            with_postings(|postings, batch, write| {
                postings.put(batch, write, at("a", 5), Some(TxId(10)))?;
                postings.put(batch, write, at("a", 10), Some(TxId(12)))
            }),
            "split: two touching intervals of one key must be one",
        );
        corrupt(
            with_postings(|postings, batch, write| {
                postings.put(batch, write, at("a", 5), None)?;
                postings.put(batch, write, at("a", 9), None)
            }),
            "a second open interval overlaps the first",
        );
        corrupt(
            with_postings(|postings, batch, write| {
                postings.put(batch, write, at("a", 5), Some(TxId(5)))
            }),
            "empty interval",
        );
        corrupt(
            with_postings(|postings, batch, write| {
                postings.put(batch, write, at("a", 6), Some(TxId(5)))
            }),
            "inverted interval",
        );
    }

    #[test]
    fn a_moved_endpoint_is_refused_at_commit() {
        corrupt(
            with_postings(|postings, batch, write| {
                version(batch, write, "a", 1, Some(8))?;
                postings.put(batch, write, at("a", 1), Some(TxId(9)))
            }),
            "posting ends after its versions",
        );
        corrupt(
            with_postings(|postings, batch, write| {
                version(batch, write, "a", 1, Some(8))?;
                postings.put(batch, write, at("a", 2), Some(TxId(8)))
            }),
            "posting starts after its versions",
        );
    }

    #[test]
    fn a_run_stretched_across_another_keys_versions_is_refused_at_commit() {
        // a for [1,4), b for [4,6), a again for [6,8): two runs of a.
        let versions = |batch: &mut CommitmentBatch, write: &redb::WriteTransaction| {
            version(batch, write, "a", 1, Some(4))?;
            version(batch, write, "b", 4, Some(6))?;
            version(batch, write, "a", 6, Some(8))
        };
        with_postings(|postings, batch, write| {
            versions(batch, write)?;
            postings.put(batch, write, at("a", 1), Some(TxId(4)))?;
            postings.put(batch, write, at("b", 4), Some(TxId(6)))?;
            postings.put(batch, write, at("a", 6), Some(TxId(8)))
        })
        .expect("the true decomposition commits");
        corrupt(
            with_postings(|postings, batch, write| {
                versions(batch, write)?;
                postings.put(batch, write, at("a", 1), Some(TxId(8)))?;
                postings.put(batch, write, at("b", 4), Some(TxId(6)))
            }),
            "one a-interval concealing the gap where b held",
        );
        corrupt(
            with_postings(|postings, batch, write| {
                versions(batch, write)?;
                postings.put(batch, write, at("a", 1), Some(TxId(4)))?;
                postings.put(batch, write, at("a", 6), Some(TxId(8)))
            }),
            "the interior b-interval is missing",
        );
    }
}

#[cfg(test)]
mod funding_lifetime_tests {
    //! A batch's charge travels with its publications until they are
    //! destroyed, and nothing a pair retains is allocated before it is
    //! admitted.
    use super::*;
    use crate::memory_accounting::MemoryAccountant;
    use contextdb_core::{DirectedValue, SortDirection, TotalOrdAsc};
    use serial_test::serial;

    fn declaration(name: &str) -> IndexDecl {
        IndexDecl {
            name: name.into(),
            columns: vec![("tag".into(), SortDirection::Asc)],
            kind: IndexKind::UserDeclared,
        }
    }

    fn fold_many(
        batch: &mut CommitmentBatch,
        write: &redb::WriteTransaction,
        indexes: usize,
        name_len: usize,
    ) -> Result<()> {
        let key = vec![DirectedValue::Asc(TotalOrdAsc(Value::Text("a".into())))];
        let key_row = key_row(&key, RowId(1))?;
        for index in 0..indexes {
            let name = format!("{index:04}{}", "n".repeat(name_len));
            batch.fold_interval(
                write,
                "t",
                &declaration(&name),
                Side::Versions,
                &key_row,
                TxId(1),
                None,
                true,
            )?;
            batch.fold_interval(
                write,
                "t",
                &declaration(&name),
                Side::Postings,
                &key_row,
                TxId(1),
                None,
                true,
            )?;
        }
        Ok(())
    }

    #[test]
    fn publications_stay_charged_until_their_credit_is_dropped() {
        let directory = tempfile::tempdir().unwrap();
        let database = redb_builder()
            .create(directory.path().join("funding.redb"))
            .unwrap();
        let accountant = Arc::new(MemoryAccountant::with_budget(64 * 1024 * 1024));
        let _scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
            accountant.clone().into(),
        )
        .unwrap();
        let baseline = accountant.usage().used;
        let write = database.begin_write().unwrap();
        let mut batch = CommitmentBatch::for_writer().unwrap();
        fold_many(&mut batch, &write, 16, 4096).unwrap();
        let (published, credit) = batch.finish(&write).unwrap().into_parts();
        let mut destination = Vec::new();
        destination.extend(published);
        assert_eq!(destination.len(), 16);
        let held = accountant.usage().used - baseline;
        assert!(
            held >= 16 * 4096,
            "the moved publications are still charged: {held} B held for 16 4 KiB names"
        );
        drop(destination);
        drop(credit);
        assert_eq!(
            accountant.usage().used,
            baseline,
            "dropping the credit refunds every admitted byte"
        );
    }

    #[test]
    fn a_name_too_large_for_the_budget_is_refused_before_it_is_kept() {
        let directory = tempfile::tempdir().unwrap();
        let database = redb_builder()
            .create(directory.path().join("refused.redb"))
            .unwrap();
        let accountant = Arc::new(MemoryAccountant::with_budget(256 * 1024));
        let _scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
            accountant.clone().into(),
        )
        .unwrap();
        let baseline = accountant.usage().used;
        let write = database.begin_write().unwrap();
        let mut batch = CommitmentBatch::for_writer().unwrap();
        let refused = fold_many(&mut batch, &write, 1, 1024 * 1024);
        assert!(
            matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
            "a megabyte index name is refused by a 256 KiB writer budget: {refused:?}"
        );
        assert!(
            batch.touched.len() == 0,
            "nothing was kept for the refused pair"
        );
        drop(batch);
        assert_eq!(accountant.usage().used, baseline);
    }

    /// A failed retention write keeps its touched commitment states charged
    /// until their physical boxes have been freed. This exercises the same
    /// batch destruction path used after a prune error, rather than merely
    /// checking that the final accountant total returns to zero.
    #[test]
    #[serial]
    fn a_failed_touched_batch_keeps_its_boxed_states_funded_through_each_free() {
        let directory = tempfile::tempdir().unwrap();
        let database = redb_builder()
            .create(directory.path().join("failed-batch.redb"))
            .unwrap();
        let accountant = Arc::new(MemoryAccountant::with_budget(64 * 1024 * 1024));
        let _scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
            accountant.clone().into(),
        )
        .unwrap();
        let baseline = accountant.usage().used;
        let counter = accountant.writer_registration_observation_for_test().4;
        let write = database.begin_write().unwrap();
        let mut batch = CommitmentBatch::for_writer().unwrap();
        let key = vec![DirectedValue::Asc(TotalOrdAsc(Value::Text("a".into())))];
        let key_row = key_row(&key, RowId(1)).unwrap();
        let (folded, requests, count) =
            crate::persistence::content_identity::proof_allocation_oracle_tests::source_requests_for_test(
                0,
                || {
                    batch.fold_interval(
                        &write,
                        "retained",
                        &declaration("by_tag"),
                        Side::Versions,
                        &key_row,
                        TxId(1),
                        None,
                        true,
                    )
                },
            );
        folded.unwrap();
        let touched = batch.touched.get("retained", "by_tag").unwrap();
        let live_pointers = [
            touched.postings.lanes.as_ptr() as usize,
            touched.versions.lanes.as_ptr() as usize,
        ];
        assert!(
            count <= requests.len(),
            "the fixed observation storage covers the fold"
        );
        let boxed_states = requests[..count]
            .iter()
            .copied()
            .filter(|request| request.1 == STATE_BYTES && live_pointers.contains(&request.0))
            .collect::<Vec<_>>();
        assert_eq!(
            boxed_states.len(),
            2,
            "one touched index owns exactly its postings and versions state"
        );
        crate::persistence::content_identity::proof_allocation_oracle_tests::watch_source_free_for_test(
            &boxed_states,
            counter,
        );
        let funded = accountant.usage().used;
        accountant.set_budget(Some(funded)).unwrap();
        let refused = fold_many(&mut batch, &write, 1, 1024 * 1024);
        assert!(
            matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
            "a failure leaves the already touched state for batch destruction: {refused:?}"
        );
        drop(batch);
        let frees =
            crate::persistence::content_identity::proof_allocation_oracle_tests::source_free_for_test(
                boxed_states.len(),
            );
        crate::persistence::content_identity::proof_allocation_oracle_tests::disarm_source_free_watch();
        assert!(
            frees[..boxed_states.len()]
                .iter()
                .all(|(before, after, count)| *count == 1 && *before >= *after && *before > 0),
            "each touched state stays funded at its physical free: {frees:?}"
        );
        let mut remaining = boxed_states.iter().map(|request| request.1).sum::<usize>();
        for (request, (before, after, count)) in boxed_states.iter().zip(frees) {
            assert_eq!(count, 1);
            assert!(
                before >= baseline + remaining && after >= baseline + remaining,
                "the original writer funds every still-live touched box at physical free: before={before}, after={after}, remaining={remaining}"
            );
            remaining -= request.1;
        }
        assert_eq!(
            accountant.usage().used,
            baseline,
            "failed batch refunds fully after freeing payloads"
        );
    }
}

#[cfg(test)]
mod wide_record_verification_tests {
    //! Verification stops inside one wide row body or one wide key, and a key
    //! too large for the requesting reader is refused by that reader before
    //! it is copied, without drawing on the writer.
    use crate::Database;
    use crate::memory_accounting::MemoryAccountant;
    use contextdb_core::{Error, Value};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn one_wide_row(path: &std::path::Path, tag_bytes: usize, body_bytes: usize) -> Database {
        let db = Database::open(path).unwrap();
        db.set_maintenance_policy(crate::MaintenancePolicy::CallerDriven);
        db.execute(
            "CREATE TABLE wide (id INT PRIMARY KEY, tag TEXT, body TEXT) HISTORY ALL",
            &HashMap::new(),
        )
        .unwrap();
        db.execute("CREATE INDEX wide_by_tag ON wide (tag)", &HashMap::new())
            .unwrap();
        db.execute(
            "INSERT INTO wide VALUES (1, $tag, $body)",
            &HashMap::from([
                ("tag".into(), Value::Text("t".repeat(tag_bytes))),
                ("body".into(), Value::Text("b".repeat(body_bytes))),
            ]),
        )
        .unwrap();
        db
    }

    fn steps(db: &Database) -> u64 {
        let mut steps = 0_u64;
        db.verify_historical_indexes_within(
            "wide",
            db.declared_reader_memory().unwrap(),
            &mut || {
                steps += 1;
                Ok(())
            },
        )
        .unwrap();
        steps
    }

    fn cancelled_inside(db: &Database, at: u64) {
        let mut taken = 0_u64;
        let refused = db.verify_historical_indexes_within(
            "wide",
            db.declared_reader_memory().unwrap(),
            &mut || {
                taken += 1;
                if taken == at {
                    return Err(Error::ReadCancelled);
                }
                Ok(())
            },
        );
        assert!(matches!(refused, Err(Error::ReadCancelled)), "{refused:?}");
        assert_eq!(taken, at);
    }

    #[test]
    fn verification_checks_its_request_inside_one_wide_body() {
        let directory = tempfile::tempdir().unwrap();
        let narrow = one_wide_row(&directory.path().join("narrow.db"), 8, 8);
        let wide = one_wide_row(&directory.path().join("wide-body.db"), 8, 4 * 1024 * 1024);
        let (narrow_steps, wide_steps) = (steps(&narrow), steps(&wide));
        assert!(
            wide_steps >= narrow_steps + 32,
            "a 4 MiB body is walked with a checkpoint every 64 KiB: {narrow_steps} vs \
             {wide_steps}"
        );
        cancelled_inside(&wide, narrow_steps + 16);
        narrow.close().unwrap();
        wide.close().unwrap();
    }

    #[test]
    fn verification_checks_its_request_inside_one_wide_key() {
        let directory = tempfile::tempdir().unwrap();
        let narrow = one_wide_row(&directory.path().join("narrow-key.db"), 8, 8);
        let wide = one_wide_row(&directory.path().join("wide-key.db"), 1024 * 1024, 8);
        let (narrow_steps, wide_steps) = (steps(&narrow), steps(&wide));
        assert!(
            wide_steps >= narrow_steps + 32,
            "a 1 MiB key is projected and hashed with a checkpoint every 64 KiB: \
             {narrow_steps} vs {wide_steps}"
        );
        cancelled_inside(&wide, narrow_steps + 16);
        narrow.close().unwrap();
        wide.close().unwrap();
    }

    #[derive(Debug)]
    struct Budget(Arc<MemoryAccountant>);
    impl contextdb_core::read_memory::ReadMemoryBudget for Budget {
        fn try_reserve(
            &self,
            bytes: usize,
            subsystem: &'static str,
            operation: &'static str,
            hint: &'static str,
        ) -> contextdb_core::Result<()> {
            self.0.try_allocate_for(bytes, subsystem, operation, hint)
        }
        fn release(&self, bytes: usize) {
            self.0.release(bytes);
        }
    }

    #[test]
    fn a_wide_key_is_refused_by_its_reader_without_drawing_on_the_writer() {
        let directory = tempfile::tempdir().unwrap();
        let db = one_wide_row(&directory.path().join("reader-key.db"), 1024 * 1024, 8);
        let writer = db.__accountant_handle_for_test();
        let writer_before = writer.usage().used;
        let reader = Arc::new(MemoryAccountant::with_budget(256 * 1024));
        let refused = db.verify_historical_indexes_within(
            "wide",
            Arc::new(Budget(reader.clone())),
            &mut || Ok(()),
        );
        match &refused {
            Err(Error::MemoryBudgetExceeded { operation, .. }) => assert_eq!(
                operation, "historical_index_verification",
                "the reader's own key admission refused, before the key was copied"
            ),
            other => panic!("a 1 MiB key does not fit a 256 KiB reader: {other:?}"),
        }
        assert_eq!(reader.usage().used, 0, "the refused reader keeps nothing");
        assert!(
            writer.usage().used < writer_before + 512 * 1024,
            "the writer was never charged for the reader's key: {} -> {}",
            writer_before,
            writer.usage().used
        );
        db.close().unwrap();
    }
}

#[cfg(test)]
thread_local! {
    /// Test-only: which phase of the verification walk the next checkpoint
    /// belongs to.
    static VERIFY_PHASE: std::cell::Cell<&'static str> = const { std::cell::Cell::new("") };
}

/// Mark the phase of the verification walk that the following checkpoints
/// belong to; production-dead outside tests.
#[inline]
pub(crate) fn note_verify_phase(phase: &'static str) {
    #[cfg(test)]
    VERIFY_PHASE.with(|current| current.set(phase));
    #[cfg(not(test))]
    let _ = phase;
}

#[cfg(test)]
fn verify_phase() -> &'static str {
    VERIFY_PHASE.with(std::cell::Cell::get)
}

#[cfg(test)]
mod wide_key_phase_tests {
    //! Every phase of verifying one wide key checks its request: walking the
    //! posting, copying its (key, row) group, comparing it with the previous
    //! group, hashing, walking the row body, projecting the indexed value and
    //! copying it. A cancellation or an expired active time that arrives in
    //! any one of those phases stops the walk there.
    use super::verify_phase;
    use crate::Database;
    use contextdb_core::TxId;
    use contextdb_core::read_contract::{OwnerLimitExceededDetail, ReadFailure, ReadFailureLimit};
    use contextdb_core::{Error, Value};
    use std::collections::{BTreeMap, HashMap};

    /// The phases a well-formed store runs. The group comparison runs only
    /// when a posting would overlap its predecessor, so it is driven on a
    /// damaged store below.
    const PHASES: [&str; 6] = [
        "posting_walk",
        "group_copy",
        "hash",
        "body_walk",
        "projection_walk",
        "projection_copy",
    ];

    /// Reopen the wide group's store with its first posting stretched open,
    /// so the next posting of the same wide group overlaps it and the
    /// comparison of the two 1 MiB groups runs.
    fn overlapping_wide_group(path: &std::path::Path) -> Database {
        wide_group(path).close().unwrap();
        let database = crate::persistence::redb_builder().open(path).unwrap();
        let write = database.begin_write().unwrap();
        {
            let name = crate::persistence::RedbPersistence::relational_index_table_name(
                "wide",
                "wide_by_tag",
            );
            let definition: redb::TableDefinition<
                crate::persistence::DurableRelationalIndexKey,
                &[u8],
            > = redb::TableDefinition::new(name.as_str());
            let mut postings = write.open_table(definition).unwrap();
            let first = {
                use redb::ReadableTable;
                // The first posting of the wide group, wherever the index
                // order places it among the short ones.
                let mut first = None;
                for entry in postings.iter().unwrap() {
                    let (key, _) = entry.unwrap();
                    let key: crate::persistence::DurableRelationalIndexKey = key.value();
                    if key.0.len() > 1024 * 1024 {
                        first = Some(key);
                        break;
                    }
                }
                first.expect("the wide group has a posting")
            };
            let open = crate::persistence::RedbPersistence::encode(&Option::<TxId>::None).unwrap();
            postings.insert(first, open.as_slice()).unwrap();
        }
        write.commit().unwrap();
        drop(database);
        let db = Database::open(path).unwrap();
        db.set_maintenance_policy(crate::MaintenancePolicy::CallerDriven);
        db
    }

    /// One row whose 1 MiB indexed value is set, moved away and set again:
    /// two intervals in one wide (key, row) group, so the group comparison
    /// runs over the whole key.
    fn wide_group(path: &std::path::Path) -> Database {
        let db = Database::open(path).unwrap();
        db.set_maintenance_policy(crate::MaintenancePolicy::CallerDriven);
        let run = |sql: &str, params: HashMap<String, Value>| {
            db.execute(sql, &params).unwrap();
        };
        let wide = || HashMap::from([("tag".to_owned(), Value::Text("w".repeat(1024 * 1024)))]);
        run(
            "CREATE TABLE wide (id INT PRIMARY KEY, tag TEXT) HISTORY ALL",
            HashMap::new(),
        );
        run("CREATE INDEX wide_by_tag ON wide (tag)", HashMap::new());
        run("INSERT INTO wide VALUES (1, $tag)", wide());
        run("UPDATE wide SET tag = 'x' WHERE id = 1", HashMap::new());
        run("UPDATE wide SET tag = $tag WHERE id = 1", wide());
        db
    }

    #[test]
    fn every_phase_of_one_wide_key_checks_its_request() {
        let directory = tempfile::tempdir().unwrap();
        let db = wide_group(&directory.path().join("phases.db"));
        let mut counts = BTreeMap::<&'static str, u64>::new();
        db.verify_historical_indexes_within(
            "wide",
            db.declared_reader_memory().unwrap(),
            &mut || {
                *counts.entry(verify_phase()).or_default() += 1;
                Ok(())
            },
        )
        .expect("an uninterrupted verification succeeds");
        for phase in PHASES {
            let seen = counts.get(phase).copied().unwrap_or(0);
            assert!(
                seen >= 8,
                "phase {phase} checks its request across a 1 MiB key at least every 64 KiB: \
                 {seen} checks, all phases {counts:?}"
            );
        }
        db.close().unwrap();
        let overlapping = overlapping_wide_group(&directory.path().join("compare.db"));
        let mut compared = 0_u64;
        let answer = overlapping.verify_historical_indexes_within(
            "wide",
            overlapping.declared_reader_memory().unwrap(),
            &mut || {
                if verify_phase() == "group_compare" {
                    compared += 1;
                }
                Ok(())
            },
        );
        assert!(
            matches!(answer, Err(Error::StoreCorrupted { .. })),
            "the overlapping postings are refused: {answer:?}"
        );
        assert!(
            compared >= 8,
            "comparing two 1 MiB groups checks the request at least every 64 KiB: {compared}"
        );
        overlapping.close().unwrap();
    }

    fn expired() -> Error {
        Error::ReadFailure(ReadFailure::owner_limit_exceeded(
            OwnerLimitExceededDetail {
                limit: ReadFailureLimit::ActiveMs,
                value: 1,
                required: None,
                statement: None,
            },
        ))
    }

    #[test]
    fn cancellation_and_expiry_stop_the_walk_inside_each_phase() {
        let directory = tempfile::tempdir().unwrap();
        let db = wide_group(&directory.path().join("stops.db"));
        let overlapping = overlapping_wide_group(&directory.path().join("stops-compare.db"));
        for phase in PHASES.into_iter().chain(["group_compare"]) {
            let db = if phase == "group_compare" {
                &overlapping
            } else {
                &db
            };
            for kind in ["cancelled", "expired"] {
                let refusal = || match kind {
                    "cancelled" => Error::ReadCancelled,
                    _ => expired(),
                };
                let mut seen = 0_u64;
                let mut stopped_in = None;
                let answer = db.verify_historical_indexes_within(
                    "wide",
                    db.declared_reader_memory().unwrap(),
                    &mut || {
                        if verify_phase() == phase {
                            seen += 1;
                            if seen == 3 {
                                stopped_in = Some(verify_phase());
                                return Err(refusal());
                            }
                        }
                        Ok(())
                    },
                );
                assert_eq!(
                    stopped_in,
                    Some(phase),
                    "{kind}: the request was checked a third time inside {phase}"
                );
                match (&answer, kind) {
                    (Err(Error::ReadCancelled), "cancelled") => {}
                    (Err(Error::ReadFailure(_)), "expired") => {}
                    other => panic!("{kind} inside {phase} stops the walk: {other:?}"),
                }
            }
        }
        db.close().unwrap();
        overlapping.close().unwrap();
    }
}

#[cfg(test)]
mod vector_projection_tests {
    //! Verification recomputes each stored version's index key from the
    //! body's own bytes. A body that holds a stored vector (the row writer
    //! keeps vectors out of bodies, so such a body is damaged, crafted or
    //! older-format) is projected component by component: that work checks
    //! its caller's request at least every 4096 components, and a refusal
    //! there stops it.
    use super::{key_row_from_body_with, verify_phase};
    use crate::persistence::{
        PersistedValue, PersistedVector, PersistedVersionedRow, RedbPersistence,
    };
    use contextdb_core::{
        Error, IndexDecl, IndexKind, Lsn, RowId, SortDirection, TxId, Value, VectorQuantization,
        Wallclock,
    };
    use std::collections::HashMap;

    pub(super) const REPRESENTATIONS: [(&str, VectorQuantization); 3] = [
        ("F32", VectorQuantization::F32),
        ("SQ8", VectorQuantization::SQ8),
        ("SQ4", VectorQuantization::SQ4),
    ];

    pub(super) fn declaration() -> IndexDecl {
        IndexDecl {
            name: "by_v".into(),
            columns: vec![("v".into(), SortDirection::Asc)],
            kind: IndexKind::UserDeclared,
        }
    }

    /// A stored version body holding `vector` in `representation`.
    pub(super) fn stored_body(vector: PersistedVector) -> Vec<u8> {
        RedbPersistence::encode(&PersistedVersionedRow {
            row_id: RowId(7),
            values: HashMap::from([
                ("id".to_owned(), PersistedValue::Plain(Value::Int64(7))),
                ("v".to_owned(), PersistedValue::Vector(vector)),
            ]),
            created_tx: TxId(10),
            deleted_tx: Some(TxId(20)),
            lsn: Lsn(1),
            created_at: Some(Wallclock(1)),
        })
        .unwrap()
    }

    pub(super) fn wide(dimension: usize, representation: VectorQuantization) -> PersistedVector {
        let components: Vec<f32> = (0..dimension)
            .map(|component| (component % 97) as f32 / 7.0 - 3.0)
            .collect();
        PersistedVector::from_f32(&components, representation)
    }

    #[test]
    fn projecting_a_stored_vector_checks_its_request_every_4096_components() {
        for (name, representation) in REPRESENTATIONS {
            let body = stored_body(wide(1 << 18, representation));
            let mut inside = 0_u64;
            key_row_from_body_with(
                &body,
                &declaration(),
                None,
                &mut || {
                    if verify_phase() == "vector_project" {
                        inside += 1;
                    }
                    Ok(())
                },
                &mut |_| Ok(()),
            )
            .unwrap();
            assert!(
                inside >= 64,
                "{name}: projecting 2^18 components checks the request at least every 4096: \
                 {inside}"
            );
            let mut seen = 0_u64;
            let answer = key_row_from_body_with(
                &body,
                &declaration(),
                None,
                &mut || {
                    if verify_phase() == "vector_project" {
                        seen += 1;
                        if seen == 3 {
                            return Err(Error::ReadCancelled);
                        }
                    }
                    Ok(())
                },
                &mut |_| Ok(()),
            );
            assert!(
                matches!(answer, Err(Error::ReadCancelled)) && seen == 3,
                "{name}: a cancellation inside the projection stops it there: {answer:?}"
            );
        }
    }

    #[test]
    fn a_malformed_stored_vector_is_refused_before_any_component_work() {
        let dimension = 4096_u32;
        let malformed = [
            (
                "SQ8 payload shorter than its dimension",
                stored_body(PersistedVector::SQ8 {
                    min: 0.0,
                    max: 1.0,
                    len: dimension,
                    payload: vec![0; dimension as usize - 1],
                }),
            ),
            (
                "SQ4 payload longer than its dimension",
                stored_body(PersistedVector::SQ4 {
                    min: 0.0,
                    max: 1.0,
                    len: dimension,
                    payload: vec![0; dimension as usize / 2 + 1],
                }),
            ),
            ("unknown representation", {
                let mut body = stored_body(wide(dimension as usize, VectorQuantization::SQ8));
                let at = body
                    .windows(2)
                    .position(|pair| pair == [1, 1])
                    .expect("the SQ8 value's tags");
                body[at + 1] = 9;
                body
            }),
            ("trailing bytes", {
                let mut body = stored_body(wide(dimension as usize, VectorQuantization::F32));
                body.push(0);
                body
            }),
        ];
        for (case, body) in malformed {
            let mut inside = 0_u64;
            let answer = key_row_from_body_with(
                &body,
                &declaration(),
                None,
                &mut || {
                    if verify_phase() == "vector_project" {
                        inside += 1;
                    }
                    Ok(())
                },
                &mut |_| Ok(()),
            );
            assert!(
                matches!(answer, Err(Error::StoreCorrupted { .. })),
                "{case}: refused as corruption: {answer:?}"
            );
            assert_eq!(
                inside, 0,
                "{case}: refused before any component was projected"
            );
        }
        // A torn quantized header is typed corruption, not a generic error.
        let torn = stored_body(PersistedVector::SQ8 {
            min: 0.0,
            max: 1.0,
            len: dimension,
            payload: vec![0; dimension as usize - 1],
        });
        assert!(matches!(
            key_row_from_body_with(&torn, &declaration(), None, &mut || Ok(()), &mut |_| Ok(())),
            Err(Error::StoreCorrupted { .. })
        ));
    }

    /// A stored vector cut short, or carrying a byte more than its header
    /// says, is the same typed corruption as any other malformed vector.
    #[test]
    fn a_truncated_or_overlong_stored_vector_is_refused_as_corruption() {
        // Every persisted representation uses the same parser and must keep
        // typed corruption for missing, displaced, and trailing stored bytes.
        for (representation, quantization) in REPRESENTATIONS {
            let whole = stored_body(wide(4096, quantization));
            let payload_end = whole.len() - 16;
            let truncated = whole[..payload_end].to_vec();
            let mut overlong = whole.clone();
            overlong.insert(payload_end - 8, 0x7f);
            let mut trailing = whole;
            trailing.push(0);
            for (case, body) in [
                ("truncated", truncated),
                ("over-long", overlong),
                ("trailing", trailing),
            ] {
                let answer = key_row_from_body_with(
                    &body,
                    &declaration(),
                    None,
                    &mut || Ok(()),
                    &mut |_| Ok(()),
                );
                assert!(
                    matches!(answer, Err(Error::StoreCorrupted { .. })),
                    "{representation} {case}: refused as corruption, not a generic error: {answer:?}"
                );
            }
        }
    }

    /// Streamed into its element hash, a stored vector's projection charges
    /// only the fixed descriptor controls, whatever its dimension and however
    /// many versions are projected with the one admitted scratch.
    #[test]
    fn the_hashed_projection_charges_the_same_whatever_the_width_or_count() {
        use crate::persistence::content_identity::version_manifest as manifest;
        for (name, representation) in REPRESENTATIONS {
            let mut scratch = vec![0_u8; manifest::VECTOR_SCRATCH_BYTES];
            let mut charges = Vec::new();
            for (dimension, versions) in [(4096, 1), (1 << 16, 1), (1 << 18, 4)] {
                let body = stored_body(wide(dimension, representation));
                let mut charged = 0_usize;
                for _ in 0..versions {
                    let mut hasher = blake3::Hasher::new();
                    manifest::project_posting_key(
                        &body,
                        &declaration(),
                        &mut || Ok(()),
                        &mut |bytes| {
                            charged += bytes;
                            Ok(())
                        },
                        Some(&mut scratch),
                        manifest::KeySuffix::Zero,
                        &mut manifest::HashKey(&mut hasher),
                    )
                    .unwrap()
                    .unwrap();
                }
                charges.push(charged / versions);
            }
            assert!(
                charges.windows(2).all(|pair| pair[0] == pair[1]),
                "{name}: each hashed projection charges the same fixed controls: {charges:?}"
            );
            assert_eq!(
                scratch.len(),
                manifest::VECTOR_SCRATCH_BYTES,
                "{name}: the scratch never grows"
            );
        }
    }

    /// The streamed key and both of its elements equal what the legacy
    /// encoder gives: decode the version, expand each vector with `to_f32`,
    /// build the key from the decoded values, and hash it whole.
    #[test]
    fn the_streamed_key_and_its_elements_equal_the_legacy_encoder() {
        use super::{
            OPEN_END, element_checked, element_finish, element_prefix, index_identity, key_row,
            key_row_from_body,
        };
        use crate::persistence::RedbPersistence;
        use crate::persistence::content_identity::version_manifest as manifest;
        let directory = tempfile::tempdir().unwrap();
        let persistence = RedbPersistence::create(&directory.path().join("codec.db")).unwrap();
        let writer = persistence.proof_accountant().unwrap_or_else(|| {
            std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit()).into()
        });
        persistence.attach_memory_accountant(writer.clone());
        let declaration = IndexDecl {
            name: "mixed".into(),
            columns: vec![
                ("tag".into(), SortDirection::Asc),
                ("v".into(), SortDirection::Desc),
                ("id".into(), SortDirection::Asc),
                ("missing".into(), SortDirection::Desc),
            ],
            kind: IndexKind::UserDeclared,
        };
        let identity = index_identity(&declaration).unwrap();
        for (quantization, representation) in REPRESENTATIONS {
            for dimension in [1_usize, 2, 3, 4095, 4096, 4097, 1 << 20] {
                let ramp = (0..dimension)
                    .map(|i| (i % 251) as f32 * 0.37 - 41.0)
                    .collect();
                let flat = vec![1.5_f32; dimension];
                let signed = (0..dimension)
                    .map(|i| match i % 4 {
                        0 => -0.0,
                        1 => f32::from_bits(0x7fc0_1234),
                        2 => 0.0,
                        _ => -(i as f32),
                    })
                    .collect();
                for (case, components) in [("ramp", ramp), ("flat", flat), ("signed", signed)] {
                    // The stored body holds the vector in this representation,
                    // as retained history can, whatever the row writer emits now.
                    let stored = crate::persistence::PersistedVersionedRow {
                        row_id: RowId(70_000 + dimension as u64),
                        values: HashMap::from([
                            (
                                "id".to_owned(),
                                crate::persistence::PersistedValue::Plain(Value::Int64(-5)),
                            ),
                            (
                                "tag".to_owned(),
                                crate::persistence::PersistedValue::Plain(Value::Text(
                                    "t\u{03bb}".into(),
                                )),
                            ),
                            (
                                "v".to_owned(),
                                crate::persistence::PersistedValue::Vector(
                                    crate::persistence::PersistedVector::from_f32(
                                        &components,
                                        representation,
                                    ),
                                ),
                            ),
                        ]),
                        created_tx: TxId(300),
                        deleted_tx: Some(TxId(70_000)),
                        lsn: Lsn(1),
                        created_at: Some(Wallclock(1)),
                    };
                    let row_created = stored.created_tx;
                    let _scope = persistence.writer_decode_scope().unwrap();
                    let body = RedbPersistence::encode(&stored).unwrap();
                    let decoded = RedbPersistence::decode_versioned_row(&body, None).unwrap();
                    assert!(
                        matches!(decoded.values.get("v"), Some(Value::Vector(v)) if v.len() == dimension),
                        "{quantization} d={dimension} {case}: the legacy decoder expands the vector"
                    );
                    let legacy = key_row(
                        &contextdb_relational::index_key_for_row(
                            &declaration.columns,
                            &decoded.values,
                        ),
                        decoded.row_id,
                    )
                    .unwrap();
                    assert!(
                        key_row_from_body(&body, &declaration).unwrap() == legacy,
                        "{quantization} d={dimension} {case}: the kept key equals the legacy key"
                    );
                    for tx in [row_created, OPEN_END] {
                        let mut hasher = element_prefix("eq", &identity, &mut || Ok(())).unwrap();
                        manifest::project_posting_key(
                            &body,
                            &declaration,
                            &mut || Ok(()),
                            &mut |_| Ok(()),
                            Some(&mut Vec::new()),
                            manifest::KeySuffix::Zero,
                            &mut manifest::HashKey(&mut hasher),
                        )
                        .unwrap()
                        .unwrap();
                        assert!(
                            element_finish(hasher, tx, &mut || Ok(())).unwrap()
                                == element_checked("eq", &identity, &legacy, tx, &mut || Ok(()))
                                    .unwrap(),
                            "{quantization} d={dimension} {case}: the streamed element equals \
                             the legacy element at {tx:?}"
                        );
                    }
                }
            }
        }
        drop(persistence);
        assert_eq!(writer.underflow_count_for_test(), 0);
    }
}
