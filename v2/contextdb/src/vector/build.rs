//! Building an IVF for a partition, in steps: when one is due, the record of how far a
//! build has come, and the steps that find its centroids and say which group each slot goes to.
//!
//! A build reads the partition as it stood when the build began (the IVF in force and the
//! tail blocks below `closed`) and writes beside it, under a number of its own. Nothing a
//! search reads changes until the last step, which makes the new IVF the one in force in
//! one commit. A step is worked out from one committed moment with no lock held, and staged
//! under the commit lock only if the build has not moved since.

use super::binary::{code_len, codes_of};
use super::block::BlockReader;
use super::directory::{self, Grouped, Partition, kept};
use super::ivf::{self, Place, centroids_per_record};
use super::pack;
use super::train::{
    DRAWN, Mean, ROUNDS, Round, SAMPLE_TO_A_CENTROID, first_centroids, groups_for, nearest_among,
    sampled,
};
use super::unindexed::Totals;
use crate::memory::{Budget, Reservation};
use crate::schema::{Catalog, VectorColumn};
use crate::sql::not_supported;
use crate::store::{ChangeSet, Key, Read, VectorId, VectorNumber, VectorRecord};
use crate::types::{Result, VectorSearchMode};

/// The most slots one step gives a group or packs, unless one group is larger.
pub(super) const STEP_SLOTS: u64 = 4096;

/// The most comparisons of a slot with a centroid in one step that gives slots their groups or
/// measures: 4,194,304. A round over the sample is one step whatever it compares.
pub(super) const STEP_COMPARISONS: u64 = 1 << 22;

/// How many questions a build asks of its new IVF to measure how far a search scans
/// and how wide it takes a binary code's estimate to be. Question `j` is the middle of two
/// stored vectors: the direction of the sum of the vectors that the slots at `j * slots /
/// MEASURED` and at half the IVF further on keep.
pub(super) const MEASURED: u32 = 128;

/// Bytes of a build's record before its lists.
const HEAD: usize = 174;

/// What a build does next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Phase {
    /// The next round over the sample, counted from zero; the centroids so far are stored.
    Train { round: u32 },
    /// Giving slots their groups: the next block to read, by its place among those the build
    /// reads.
    Assign { next: u32 },
    /// Packing: the next group to write, and the block of the new IVF it starts in.
    Pack { group: u32 },
    /// Measuring the gap: how many questions have been asked, and the largest gap any of them
    /// needed for its ten nearest, in ten-thousandths.
    Measure { done: u32, most: u32 },
    /// Measuring the width, with the gap settled: how many questions have been asked again,
    /// the most groups any of them had inside the gap, and for each width from 2 to 16
    /// quarters how many of their neighbours the binary codes of those groups kept, of `asked`
    /// neighbours in all.
    Width {
        gap: u32,
        probes: u32,
        done: u32,
        asked: u32,
        kept: [u32; 15],
    },
    /// Everything is written: the next step makes the IVF the one in force.
    Switch { gap: u32, probes: u32, width: u8 },
    /// The IVF is in force, and the build was of slots some of which were stale. The
    /// next steps remove the `Left` records the build applied, and the last removes the
    /// build's record.
    Sweep,
}

/// How far the build of a partition's next IVF has come.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Build {
    /// The number the new IVF's records are kept under.
    pub number: u32,
    /// How many steps of this build have been staged. A step that was worked out for another
    /// count is not staged.
    pub steps: u64,
    /// The IVF the build reads, the one in force when it began; `None` when there was
    /// none.
    pub from: Option<Grouped>,
    /// The tail blocks the build reads: from this number up to, not including, the next.
    pub tail: (u32, u32),
    /// How many slots those blocks held when the build began.
    pub slots: u64,
    /// The partition's `sealed` when the build began: the tail slots it reads are the last
    /// before that count.
    pub upto: u64,
    /// Whether any of them had been left behind by its row, so that every slot's key is
    /// looked up among the partition's `Left` records. No row is read.
    pub stale: bool,
    /// Kept centres, live outside assignments, and the packed members' sum.
    pub reusing: bool,
    pub outside: u64,
    pub sum: Vec<f64>,
    /// How many groups the new IVF has: one in a column that is not at its size, and
    /// then the build finds no centroid, writes no binary code and asks no question.
    pub groups: u32,
    pub phase: Phase,
    /// How many slots each group has been given so far, by group.
    pub counts: Vec<u32>,
    /// Where each group packed so far begins in the new blocks, and where the next begins.
    pub starts: Vec<Place>,
    /// The ten slots nearest each question asked so far, in the order asked: what measuring
    /// the gap found and measuring the width reads. Empty until the first question.
    pub tens: Vec<Place>,
    /// The bytes the step that was last refused for memory needed; zero when the last step
    /// was staged.
    pub needing: u64,
}

impl Build {
    /// The record of the build, every number low byte first. Eight numbers of eight bytes:
    /// `steps`, `needing`, `slots`, `upto`, the `slots` and `upto` of `from`, outside assignments
    /// and sum length. Flags (1: stale; 2: has `from`; 4: kept centres) and one phase byte,
    /// counted from zero in the order of [`Phase`]. Then numbers of four bytes: `number`, the
    /// two of `tail`, `groups`, the `number`, `blocks` and `groups` of `from`, nineteen for the
    /// fields of the phase in their order, the count of `starts`, `counts`, and the two
    /// numbers of every place of `starts` and `tens`, then the f64 sum. Unfilled fields are zero.
    pub(super) fn encode(&self) -> Vec<u8> {
        let from = self.from.map(|from| {
            let short = [from.number, from.blocks, from.groups];
            ([from.slots, from.upto], short)
        });
        let (long, short) = from.unwrap_or_default();
        let (phase, fields) = match self.phase {
            Phase::Train { round } => (0, vec![round]),
            Phase::Assign { next } => (1, vec![next]),
            Phase::Pack { group } => (2, vec![group]),
            Phase::Measure { done, most } => (3, vec![done, most]),
            Phase::Width {
                gap,
                probes,
                done,
                asked,
                kept,
            } => (4, [&[gap, probes, done, asked], &kept[..]].concat()),
            Phase::Switch { gap, probes, width } => (5, vec![gap, probes, width.into()]),
            Phase::Sweep => (6, Vec::new()),
        };
        let mut carried = [0; 19];
        carried[..fields.len()].copy_from_slice(&fields);
        let flags = u8::from(self.stale)
            | (u8::from(self.from.is_some()) << 1)
            | (u8::from(self.reusing) << 2);
        let places = self.starts.iter().chain(&self.tens);
        let places = places.flat_map(|place| [place.block, place.slot]);
        let head = [self.number, self.tail.0, self.tail.1, self.groups];
        let short = head.into_iter().chain(short).chain(carried);
        let short = short.chain([self.starts.len() as u32]);
        let short = short.chain(self.counts.iter().copied()).chain(places);
        let mut record = Vec::new();
        let first = [self.steps, self.needing, self.slots, self.upto];
        let rest = [self.outside, self.sum.len() as u64];
        let long = first.into_iter().chain(long).chain(rest);
        record.extend(long.flat_map(u64::to_le_bytes));
        record.extend([flags, phase]);
        record.extend(short.flat_map(u32::to_le_bytes));
        record.extend(self.sum.iter().flat_map(|n| n.to_le_bytes()));
        record
    }

    /// The build a record was written from, `None` for a record that disagrees with itself:
    /// a length that does not follow from its `groups` and its count of `starts`, an unknown
    /// flag or phase, no group or more groups than slots, a round, block or group past those it
    /// has, places that do not begin at the first slot or are out of order, places for groups
    /// it has not packed, or nearest slots that are not as many a question as its packed
    /// slots give (ten, or all of fewer) for the questions its phase has asked.
    fn decode(record: &[u8]) -> Option<Build> {
        let (head, lists) = record.split_at_checked(HEAD)?;
        let long = |at: usize| u64::from_le_bytes(std::array::from_fn(|byte| head[8 * at + byte]));
        let size = usize::try_from(long(7)).ok()?.checked_mul(8)?;
        let (lists, sum) = lists.split_at_checked(lists.len().checked_sub(size)?)?;
        let sum: Vec<f64> = sum
            .chunks_exact(8)
            .map(|n| f64::from_le_bytes(std::array::from_fn(|at| n[at])))
            .collect();
        let (lists, []) = lists.as_chunks::<4>() else {
            return None;
        };
        let short =
            |at: usize| u32::from_le_bytes(std::array::from_fn(|byte| head[66 + 4 * at + byte]));
        let listed = |at: usize| u32::from_le_bytes(lists[at]);
        let (flags, groups, begun) = (head[64], short(3) as usize, short(26) as usize);
        if flags > 7 || lists.len() < groups + 2 * begun || (lists.len() - groups) % 2 != 0 {
            return None;
        }
        let phase = match head[65] {
            0 => Phase::Train { round: short(7) },
            1 => Phase::Assign { next: short(7) },
            2 => Phase::Pack { group: short(7) },
            3 => Phase::Measure {
                done: short(7),
                most: short(8),
            },
            4 => Phase::Width {
                gap: short(7),
                probes: short(8),
                done: short(9),
                asked: short(10),
                kept: std::array::from_fn(|at| short(11 + at)),
            },
            5 => Phase::Switch {
                gap: short(7),
                probes: short(8),
                width: u8::try_from(short(9)).ok()?,
            },
            6 => Phase::Sweep,
            _ => return None,
        };
        let mut places = (groups..lists.len()).step_by(2).map(|at| Place {
            block: listed(at),
            slot: listed(at + 1),
        });
        let build = Build {
            number: short(0),
            steps: long(0),
            from: (flags & 2 != 0).then(|| Grouped {
                number: short(4),
                blocks: short(5),
                groups: short(6),
                slots: long(4),
                upto: long(5),
            }),
            tail: (short(1), short(2)),
            slots: long(2),
            upto: long(3),
            stale: flags & 1 != 0,
            reusing: flags & 4 != 0,
            outside: long(6),
            sum,
            groups: short(3),
            phase,
            counts: (0..groups).map(listed).collect(),
            starts: places.by_ref().take(begun).collect(),
            tens: places.collect(),
            needing: long(1),
        };
        let (inside, grouped) = build.from.map_or((0, 0), |from| (from.blocks, from.slots));
        let blocks = (build.tail.1.checked_sub(build.tail.0)?).checked_add(inside)?;
        let tail = build.slots.checked_sub(grouped)?;
        let packed: u64 = build.counts.iter().map(|count| u64::from(*count)).sum();
        let each = packed.min(10) as usize;
        let questions = build.tens.len().checked_div(each).unwrap_or(0);
        let whole = build.tens.len() == each * questions && questions <= MEASURED as usize;
        let ended = begun == groups + 1;
        let agrees = match phase {
            Phase::Train { round } => round < ROUNDS && begun == 0 && questions == 0,
            Phase::Assign { next } => next < blocks && begun == 0 && questions == 0,
            Phase::Pack { group } => (group as usize) < groups && begun == group as usize + 1,
            Phase::Measure { done, .. } => ended && done as usize == questions,
            Phase::Width {
                done, asked, kept, ..
            } => ended && done as usize <= questions && kept.iter().all(|kept| *kept <= asked),
            Phase::Switch { .. } => ended,
            Phase::Sweep => ended && questions == 0,
        };
        let placed = build.starts.is_sorted()
            && build
                .starts
                .first()
                .is_none_or(|first| *first == Place::default());
        let sound = 0 < groups && groups as u64 <= build.slots && build.upto >= tail && placed;
        (sound && whole && agrees && build.sum.iter().all(|n| n.is_finite())).then_some(build)
    }

    /// The build of the partition of this number, `None` when it has none. A record that
    /// disagrees with itself is damage.
    pub(super) fn read(
        read: &dyn Read,
        id: VectorId,
        at: u32,
        n: usize,
        held: &mut Reservation,
    ) -> Result<Option<Build>> {
        let ok = |b: &Build| [0, n].contains(&b.sum.len());
        let found = kept(read, VectorRecord::Build(id, at), |v| -> Result<_> {
            held.grow(v.len() as u64)?;
            Ok(Build::decode(v).filter(ok))
        })?
        .transpose()?;
        let damage = "the record of a build of the vector part disagrees with itself";
        found
            .map(|b| b.ok_or_else(|| read.damaged(damage)))
            .transpose()
    }

    /// Every record the build has written so far, for the commit that removes them: its own
    /// record, the groups it gave, its centroids and the blocks it packed with their binary codes.
    /// Once its IVF is in force those are the IVF's and it has its record alone.
    pub(super) fn written(
        &self,
        vector: VectorId,
        partition: u32,
        dimension: usize,
    ) -> impl Iterator<Item = VectorRecord<'static>> + use<> {
        let number = self.number;
        let many = u32::from(self.groups > 1);
        let centroids = self.groups.div_ceil(centroids_per_record(dimension) as u32);
        let end = self.starts.last().copied().unwrap_or_default();
        let packed = end.block + u32::from(end.slot > 0);
        let (given, centroids, packed) = match self.phase {
            Phase::Sweep => (0, 0, 0),
            Phase::Train { round } => (0, centroids * u32::from(round > 0), packed),
            Phase::Assign { next } => (next, centroids * many, packed),
            _ => (self.blocks(), centroids * many, packed),
        };
        let mean =
            many > 0 && self.starts.len() > self.groups as usize && self.phase != Phase::Sweep;
        std::iter::once(VectorRecord::Build(vector, partition))
            .chain(mean.then_some(VectorRecord::Mean(vector, number)))
            .chain((0..given).map(move |place| VectorRecord::Assigned(vector, number, place)))
            .chain((0..centroids).map(move |place| VectorRecord::Centroids(vector, number, place)))
            .chain((0..packed).map(move |block| VectorRecord::Block(vector, number, block)))
            .chain((0..packed * many).map(move |block| VectorRecord::Binary(vector, number, block)))
    }

    /// How many slots the build has still to pack: every slot it reads until each has its
    /// group, then those of the groups it has not written.
    pub(super) fn unpacked(&self) -> u64 {
        let counts = match self.phase {
            Phase::Train { .. } | Phase::Assign { .. } => return self.slots,
            Phase::Pack { group } => &self.counts[group as usize..],
            _ => &[],
        };
        counts.iter().map(|count| u64::from(*count)).sum()
    }

    /// How many blocks the build reads: those of `from`, then its tail blocks.
    pub(super) fn blocks(&self) -> u32 {
        self.from.map_or(0, |from| from.blocks) + (self.tail.1 - self.tail.0)
    }

    /// The block the build reads at this place, counted from zero: `from`'s blocks in order,
    /// then the tail blocks of the partition.
    pub(super) fn block(
        &self,
        vector: VectorId,
        partition: u32,
        place: u32,
    ) -> VectorRecord<'static> {
        match self.from {
            Some(from) if place < from.blocks => VectorRecord::Block(vector, from.number, place),
            from => {
                let after = place - from.map_or(0, |from| from.blocks);
                VectorRecord::Block(vector, partition, self.tail.0 + after)
            }
        }
    }
}

/// One partition with work for a build: one that is due, or one under way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Work {
    pub vector: VectorId,
    /// The partition's key tuple.
    pub tuple: Key,
}

/// What one step read and worked out, to be staged under the commit lock.
pub(crate) struct Step {
    pub(super) work: Work,
    /// The column as it was declared when the step was worked out: what the step that begins
    /// a build asks [`due`] of again under the lock.
    pub(super) column: VectorColumn,
    pub(super) dimension: usize,
    /// The partition's number when the step was worked out, and the build's number and count
    /// of steps then; `None` for the step that begins a build.
    pub(super) at: (u32, Option<(u32, u64)>),
    /// The records the step writes and removes. A build's records hold no key, so they
    /// outlive what they were read from.
    pub(super) puts: Vec<(VectorRecord<'static>, Vec<u8>)>,
    pub(super) removes: Vec<VectorRecord<'static>>,
    /// The keys of the partition's `Left` records the step removes.
    pub(super) left: Vec<(Key, u64)>,
    /// What the partition's record takes from the step: nothing, or the IVF the step
    /// puts in force.
    pub(super) switch: Option<Grouped>,
    /// The centroids a round made, in the order of the groups; empty for any other step.
    pub(super) centroids: Vec<f32>,
    /// The finished packed mean, staged under this build's number.
    pub(super) mean: Option<Vec<f32>>,
    /// How many vectors the step finished: those it packed.
    pub(super) done: u64,
    /// Slots read while packing, and (`compared`) the comparisons assigning made, apart from `done`.
    pub(crate) rows_read: u64,
    pub(crate) compared: u64,
    /// Never read: holds what the records above take until they are staged.
    pub(super) held: Reservation,
}

/// What a build is given to work on: one partition at one committed moment.
pub(super) struct Ground<'a> {
    pub read: &'a dyn Read,
    pub column: &'a VectorColumn,
    pub dimension: usize,
    pub partition: Partition,
    pub build: Build,
}

/// True when an IVF is due for a partition that is building none. `vectors` is how many
/// rows have a vector in the column, every partition together; the column is at its size when
/// that is its `index_at` or more, or when it declares `SEARCH_MODE INDEXED`.
///
/// In a column at its size: the partition has no IVF and its tail holds at least the
/// column's `grouped_from` slots (a hundredth of `index_at`, and one at least); or it has a
/// IVF of one group, holds that many slots, and a build now would give it more than one
/// group, the square root of its slots rounding to two or more, so that a partition whose
/// IVF already has the groups a new build would give it is never due on its size alone;
/// or it has an IVF and the vectors outside it (in tail blocks and pending) together with
/// the slots left behind come to `CHANGE_PERCENT` of its slots, or the slots left behind alone
/// to `TOMBSTONE_PERCENT`. In any other column only the
/// slots left behind count, against every slot the partition has: the build then has one
/// group and writes no centroid and no binary code. Under `COMPACTION NONE` only a first IVF
/// of a column at its size is ever due. The slots left behind are the slots of the IVF
/// and the tail less the rows with a vector that are not pending.
pub(super) fn due(
    partition: &Partition,
    pending: u64,
    column: &VectorColumn,
    vectors: u64,
) -> bool {
    let grouped = partition.ivf.map_or(0, |ivf| ivf.slots);
    let slots = grouped + partition.slots;
    let out = partition.slots + pending;
    let stale = (slots + pending).saturating_sub(partition.live);
    let behind = left_behind(partition, pending, column, vectors);
    if !sized(column, vectors) {
        return behind;
    }
    let Some(ivf) = partition.ivf else {
        return partition.slots >= column.grouped_from();
    };
    let grows = ivf.groups == 1 && slots >= column.grouped_from() && groups_for(slots) > 1;
    let reached = |(change, _): (u8, u8)| {
        grows || behind || (out + stale) * 100 >= u64::from(change) * grouped
    };
    // A partition with no slot has nothing to group: its pending rows are sealed first.
    slots > 0 && pending < partition.live && column.compaction().is_some_and(reached)
}

/// True when the slots rows have left behind are alone enough for an IVF to be due: they
/// come to the column's `TOMBSTONE_PERCENT` of the slots of the IVF in force in a column
/// at its size, and of every slot of the partition in any other. [`due`] asks it, and
/// inspection gives it as the reason.
pub(super) fn left_behind(
    partition: &Partition,
    pending: u64,
    column: &VectorColumn,
    vectors: u64,
) -> bool {
    let grouped = partition.ivf.map_or(0, |ivf| ivf.slots);
    let slots = grouped + partition.slots;
    let stale = (slots + pending).saturating_sub(partition.live);
    let of = if sized(column, vectors) {
        grouped
    } else {
        slots
    };
    let reached = |(_, share): (u8, u8)| stale * 100 >= u64::from(share) * of;
    stale > 0 && column.compaction().is_some_and(reached)
}

/// True when the column is at its size, `vectors` being its rows with a vector.
fn sized(column: &VectorColumn, vectors: u64) -> bool {
    vectors >= column.index_at() || column.mode() == VectorSearchMode::Indexed
}

/// True while a search of the nearest groups may be given this partition: the vectors
/// outside its IVF, in tail blocks and pending, are fewer than twice what makes a
/// IVF due. A partition with no IVF, or with one of one group, is searched whole,
/// and so while the vectors outside its IVF are fewer than twice the column's
/// `index_at`. Under `COMPACTION NONE` an IVF of more groups is usable unless it is marked damaged.
pub(super) fn usable(read: &dyn Read, p: &Partition, pending: u64, column: &VectorColumn) -> bool {
    let out = p.slots + pending;
    match p.ivf.filter(|ivf| ivf.groups > 1) {
        Some(ivf) if read.committed().marked(column.id, ivf.number) => false,
        None => out < column.index_at().saturating_mul(2),
        Some(ivf) => column.compaction().is_none_or(|(change, _)| {
            let due = (100 * column.grouped_from()).max(u64::from(change) * ivf.slots);
            out.saturating_mul(100) < due.saturating_mul(2)
        }),
    }
}

/// Calls `each` with every partition after `after` and its key tuple until `each` answers false.
pub(super) fn entries(
    read: &dyn Read,
    column: &VectorColumn,
    after: Option<&Key>,
    each: &mut dyn FnMut(&Key, Partition) -> Result<bool>,
) -> Result<()> {
    let first = Key::from_values(&[], &[])?;
    let from = VectorRecord::Partition(column.id, after.unwrap_or(&first));
    read.vectors(from, &mut |record, entry| match record {
        VectorRecord::Partition(vector, tuple) if vector == column.id => {
            if after == Some(tuple) {
                return Ok(true);
            }
            each(tuple, Partition::read(read, column, tuple, entry)?)
        }
        _ => Ok(false),
    })
}

/// Whether the IVF record requires a rebuild. Its temporary bytes are always budgeted.
fn repair(
    read: &dyn Read,
    vector: VectorId,
    partition: &Partition,
    held: &mut Reservation,
) -> Result<bool> {
    let Some(grouped) = partition.ivf else {
        return Ok(false);
    };
    let before = held.bytes();
    let found = ivf::Ivf::read(read, vector, grouped, held);
    held.shrink(held.bytes() - before);
    match found {
        Err(crate::types::Error::StoreCorrupted { .. }) => Ok(true),
        found => found.map(|_| read.committed().marked(vector, grouped.number)),
    }
}

/// The cycle's one work list: builds, due partitions, damaged IVFs and pending tails.
pub(crate) fn unindexed_work(
    read: &dyn Read,
    catalog: &Catalog,
    held: &mut Reservation,
    totals: &Totals,
) -> Result<Vec<Work>> {
    let mut found = Vec::new();
    for table in catalog.tables() {
        for column in &table.vectors {
            let vectors = totals.live(column.id);
            entries(read, column, None, &mut |tuple, partition| {
                if partition.building.is_some()
                    || partition.unindexed > 0
                    || due(&partition, u64::from(partition.unindexed), column, vectors)
                    || repair(read, column.id, &partition, held)?
                {
                    held.room(&mut found, usize::MAX)?;
                    held.grow(tuple.as_bytes().len() as u64)?;
                    found.push((
                        partition.ivf.is_some(),
                        column.id,
                        partition.number,
                        tuple.clone(),
                    ));
                }
                Ok(true)
            })?;
        }
    }
    found.sort();
    held.grow((found.len() * size_of::<Work>()) as u64)?;
    Ok(found
        .into_iter()
        .map(|(_, vector, _, tuple)| Work { vector, tuple })
        .collect())
}

/// Works out the next step for one partition from this moment: the beginning of a build that
/// is due, or the next step of the one under way. `None` when the partition has no work left.
/// It reads and works out only, and holds what it made through a reservation named
/// `maintenance`; a refusal for memory is the step's answer, with nothing changed.
pub(crate) fn prepare(
    work: &Work,
    read: &dyn Read,
    catalog: &Catalog,
    budget: &Budget,
    vectors: u64,
) -> Result<Option<Step>> {
    let vector = work.vector;
    let Some((table, column)) = catalog.vector(vector) else {
        return Ok(None);
    };
    let Some(partition) = directory::partition(read, column, &work.tuple)? else {
        return Ok(None);
    };
    let mut step = Step {
        work: work.clone(),
        column: column.clone(),
        dimension: table.dimension(column),
        at: (partition.number, None),
        puts: Vec::new(),
        removes: Vec::new(),
        left: Vec::new(),
        switch: None,
        centroids: Vec::new(),
        mean: None,
        done: 0,
        rows_read: 0,
        compared: 0,
        held: budget.reserve(0, "maintenance")?,
    };
    let Some(build) = Build::read(
        read,
        vector,
        partition.number,
        table.dimension(column),
        &mut step.held,
    )?
    else {
        let pending = u64::from(partition.unindexed);
        let due = due(&partition, pending, column, vectors)
            || repair(read, vector, &partition, &mut step.held)?;
        return Ok(due.then_some(step));
    };
    step.at.1 = Some((build.number, build.steps));
    let dimension = table.dimension(column);
    let ground = Ground {
        read,
        column,
        dimension,
        partition,
        build,
    };
    match ground.build.phase {
        Phase::Train { round } => train(&ground, &mut step, round)?,
        Phase::Assign { next } => assign(&ground, &mut step, next)?,
        Phase::Pack { .. } => pack::pack(&ground, &mut step)?,
        Phase::Measure { .. } => pack::measure(&ground, &mut step)?,
        Phase::Width { .. } => pack::width(&ground, &mut step)?,
        Phase::Switch { .. } => pack::switch(&ground, &mut step)?,
        Phase::Sweep => pack::sweep(&ground, &mut step)?,
    }
    Ok(Some(step))
}

/// Stages a step, under the commit lock, over the newest moment. Stages nothing and answers
/// zero when the partition is gone or has another number, or is not building the IVF,
/// at the count of steps, the step was worked out for: the one place that is checked. A step
/// that begins a build stages nothing either when no IVF is due any more. Otherwise
/// answers how many vectors the step finished.
///
/// The step that begins a build takes the build's number from the column's run of partition
/// numbers, writes the build's record, and closes the partition's tail blocks. Any other
/// stages its records, and writes the partition's record again, from what it says now, when
/// it stages the mean, puts an IVF in force or removes the build's record.
pub(crate) fn stage_step(step: Step, changes: &mut ChangeSet, vectors: u64) -> Result<u64> {
    let (vector, tuple, dimension) = (step.work.vector, &step.work.tuple, step.dimension);
    let found = directory::partition(&*changes, &step.column, tuple)?;
    let Some(mut partition) = found.filter(|partition| partition.number == step.at.0) else {
        return Ok(0);
    };
    let entry = VectorRecord::Partition(vector, tuple);
    let own = VectorRecord::Build(vector, partition.number);
    let mut held = step.held;
    let build = Build::read(&*changes, vector, partition.number, dimension, &mut held)?;
    let Some((number, steps)) = step.at.1 else {
        let pending = u64::from(partition.unindexed);
        if build.is_some()
            || !(due(&partition, pending, &step.column, vectors)
                || repair(&*changes, vector, &partition, &mut held)?)
        {
            return Ok(0);
        }
        let next = VectorNumber::NextPartition(vector);
        let number = changes.vector_number(next)?;
        changes.put_vector_number(next, number + 1)?;
        let number = u32::try_from(number).map_err(|_| {
            not_supported("more than 4,294,967,296 partitions over the life of a VECTOR column")
        })?;
        let slots = partition.ivf.map_or(0, |ivf| ivf.slots) + partition.slots;
        let stale = slots + pending > partition.live;
        let grouped = sized(&step.column, vectors) && slots >= step.column.grouped_from();
        let mut groups = if grouped { groups_for(slots) } else { 1 };
        let mut reusing = false;
        let keep = partition.ivf.filter(|g| {
            grouped
                && g.groups > 1
                && partition.assigned + if stale { 0 } else { partition.slots }
                    < u64::from(g.groups).pow(2)
        });
        if let Some(g) = keep
            && !repair(&*changes, vector, &partition, &mut held)?
            && kept(&*changes, VectorRecord::Ivf(vector, g.number), |v| {
                v[v.len() - 8..].iter().any(|byte| *byte != 0)
            })? == Some(true)
        {
            let before = held.bytes();
            match ivf::centroids(&*changes, vector, g.number, g.groups, dimension, &mut held) {
                Ok(centroids) => {
                    groups = g.groups;
                    reusing = true;
                    ivf::put_centroids(changes, vector, number, dimension, &centroids)?;
                }
                Err(crate::types::Error::StoreCorrupted { .. }) => (),
                Err(other) => return Err(other),
            }
            held.shrink(held.bytes() - before);
        }
        held.grow(HEAD as u64 + 8 * u64::from(groups))?;
        let build = Build {
            number,
            steps: 0,
            from: partition.ivf,
            tail: (partition.first, partition.blocks),
            slots,
            upto: partition.sealed,
            stale,
            reusing,
            outside: 0,
            sum: Vec::new(),
            groups,
            phase: if groups == 1 || reusing {
                Phase::Assign { next: 0 }
            } else {
                Phase::Train { round: 0 }
            },
            counts: vec![0; groups as usize],
            starts: Vec::new(),
            tens: Vec::new(),
            needing: 0,
        };
        changes.put_vector(own, build.encode())?;
        partition.building = Some((number, changes.base().commit().0));
        partition.closed = partition.blocks;
        changes.put_vector(entry, partition.encode())?;
        return Ok(0);
    };
    let build = build.filter(|build| (build.number, build.steps) == (number, steps));
    let Some(build) = build.filter(|_| partition.building.map(|(n, _)| n) == Some(number)) else {
        return Ok(0);
    };
    let was = partition;
    for (record, value) in step.puts {
        held.shrink(value.len() as u64);
        changes.put_vector(record, value)?;
    }
    for record in &step.removes {
        changes.delete_vector(*record)?;
    }
    for (key, fence) in &step.left {
        let record = VectorRecord::Left(vector, partition.number, key);
        if kept(&*changes, record, |v| left_at(&*changes, &partition, v))?
            .transpose()?
            .is_some_and(|[(_, now), _]| now == *fence)
        {
            changes.delete_vector(record)?;
        }
    }
    if !step.centroids.is_empty() {
        ivf::put_centroids(changes, vector, number, dimension, &step.centroids)?;
    }
    if let Some(mean) = &step.mean {
        ivf::put_mean(changes, vector, number, mean)?;
    }
    if step.switch.is_some() {
        let mean = (build.groups > 1)
            .then(|| ivf::mean(&*changes, vector, number, dimension, &mut held))
            .transpose()?;
        for at in partition.closed..partition.blocks {
            let code = VectorRecord::Binary(vector, partition.number, at);
            if let Some(mean) = &mean {
                let block = VectorRecord::Block(vector, partition.number, at);
                let codes = pack::with_block(&*changes, block, dimension, &mut |slots| {
                    held.grow((4 + slots.len() * code_len(dimension)) as u64)?;
                    Ok(codes_of(slots, dimension, &mut |_| mean))
                })?;
                held.shrink(codes.len() as u64);
                changes.put_vector(code, codes)?;
            } else {
                changes.delete_vector(code)?;
            }
        }
    }
    if let Some(grouped) = step.switch {
        // The tail blocks the build read go with the switch, and their slots with them.
        let read = build.slots - build.from.map_or(0, |from| from.slots);
        let slots = partition.slots.checked_sub(read);
        partition.slots = slots.ok_or_else(|| changes.damaged(directory::SHORT))?;
        partition.assigned = if build.reusing {
            partition.assigned + build.outside
        } else {
            0
        };
        (partition.ivf, partition.first) = (Some(grouped), build.tail.1);
    }
    if step.removes.contains(&own) {
        partition.building = None;
    }
    if partition != was {
        changes.put_vector(entry, partition.encode())?;
    }
    Ok(step.done)
}

/// Records, in a commit of its own, that the next step of a partition's build was refused
/// for memory and how many bytes it needed, so that every reader's inspection can say so.
/// Stages nothing when the build's record says so already.
pub(crate) fn stage_refusal(
    work: &Work,
    needed: u64,
    changes: &mut ChangeSet,
    catalog: &Catalog,
    held: &mut Reservation,
) -> Result<()> {
    if let Some((t, c)) = catalog.vector(work.vector)
        && let Some(p) = directory::partition(&*changes, c, &work.tuple)?
        && let Some(mut b) = Build::read(&*changes, work.vector, p.number, t.dimension(c), held)?
        && b.needing != needed
    {
        b.needing = needed;
        changes.put_vector(VectorRecord::Build(work.vector, p.number), b.encode())?;
    }
    Ok(())
}

/// Work left for one member of the cycle's gathered list, from its kept records.
pub(crate) fn progress(
    work: &Work,
    read: &dyn Read,
    catalog: &Catalog,
    held: &mut Reservation,
    vectors: u64,
) -> Result<(u64, u64, u64)> {
    let Some((table, column)) = catalog.vector(work.vector) else {
        return Ok((0, 0, 0));
    };
    let Some(partition) = directory::partition(read, column, &work.tuple)? else {
        return Ok((0, 0, 0));
    };
    match Build::read(
        read,
        work.vector,
        partition.number,
        table.dimension(column),
        held,
    )? {
        Some(build) if build.needing > 0 => Ok((0, 1, 1)),
        Some(build) => Ok((build.unpacked(), 1, 0)),
        None if due(&partition, u64::from(partition.unindexed), column, vectors)
            || repair(read, work.vector, &partition, held)? =>
        {
            Ok((partition.slots + partition.ivf.map_or(0, |g| g.slots), 1, 0))
        }
        None => Ok((0, 0, 0)),
    }
}

/// Gives `each` the block the build reads at this place, read in place, and answers what it
/// answers. A block that is missing or is not whole is damage.
fn input<T>(
    ground: &Ground<'_>,
    place: u32,
    each: &mut dyn FnMut(&BlockReader<'_>) -> Result<T>,
) -> Result<T> {
    let record = (ground.build).block(ground.column.id, ground.partition.number, place);
    pack::with_block(ground.read, record, ground.dimension, each)
}

/// Whether a slot was already departed at the build's source fence.
fn stale(ground: &Ground<'_>, place: u32, sealed: u64, key: &Key) -> Result<bool> {
    let build = &ground.build;
    let (inside, upto) = build.from.map_or((0, 0), |from| (from.blocks, from.upto));
    Ok(key.as_bytes().is_empty()
        || (build.stale
            && left(ground, key)?.is_some_and(|left| {
                (place >= inside && sealed < left) || (place < inside && left >= upto)
            })))
}

/// Walks only live slots, numbered consecutively, without holding their keys or bytes.
fn live_slots(ground: &Ground<'_>, each: &mut dyn FnMut(u64, &[u8]) -> Result<()>) -> Result<u64> {
    let build = &ground.build;
    let inside = build.from.map_or(0, |from| from.blocks);
    let mut sealed = build.upto - (build.slots - build.from.map_or(0, |from| from.slots));
    let mut live = 0;
    for place in 0..build.blocks() {
        input(ground, place, &mut |block| {
            for at in 0..block.len() {
                if !stale(ground, place, sealed + at as u64, &block.key(at))? {
                    each(live, block.slot(at))?;
                    live += 1;
                }
            }
            if place >= inside {
                sealed += block.len() as u64;
            }
            Ok(())
        })?;
    }
    Ok(live)
}

/// Samples evenly over live ordinals, using the same liveness test as assignment.
fn sample(
    ground: &Ground<'_>,
    live: u64,
    each: &mut dyn FnMut(u64, &[u8]) -> Result<()>,
) -> Result<()> {
    let mut wanted = sampled(live, ground.build.groups).zip(0..).peekable();
    live_slots(ground, &mut |slot, bytes| {
        if let Some((_, at)) = wanted.next_if(|(wanted, _)| *wanted == slot) {
            each(at, bytes)?;
        }
        Ok(())
    })?;
    Ok(())
}

/// Adds the build's record as a step leaves it to the step's records: one more step staged,
/// and none refused.
pub(super) fn moved(ground: &Ground<'_>, step: &mut Step, mut build: Build) {
    (build.steps, build.needing) = (ground.build.steps + 1, 0);
    let own = VectorRecord::Build(ground.column.id, ground.partition.number);
    step.puts.push((own, build.encode()));
}

/// The step that finds centroids: one round over the sample. The first round of a build first
/// takes its centroids from the sample. After the last round the build gives slots their groups.
///
fn train(ground: &Ground<'_>, step: &mut Step, round: u32) -> Result<()> {
    let (build, dimension) = (&ground.build, ground.dimension);
    let (groups, held) = (build.groups, &mut step.held);
    let live = live_slots(ground, &mut |_, _| Ok(()))?;
    held.grow(HEAD as u64 + 8 * u64::from(groups))?;
    let mut centroids = if round == 0 {
        let sample_of = live.min(SAMPLE_TO_A_CENTROID * u64::from(groups));
        let seeds = sample_of.min(u64::from(groups)) as u32;
        held.grow((4 * dimension as u64 + 8 + DRAWN) * u64::from(groups))?;
        let first = first_centroids(sample_of, seeds);
        held.shrink(DRAWN * u64::from(groups));
        let mut first = first.into_iter().peekable();
        let mut centroids = Vec::with_capacity(dimension * groups as usize);
        sample(ground, live, &mut |at, slot| {
            // The vector a slot keeps is the mean of that slot alone.
            if first.next_if_eq(&at).is_some() {
                let mut centroid = Mean::new(dimension, held)?;
                centroid.take(slot);
                held.grow(4 * dimension as u64)?;
                centroids.extend(centroid.finish());
                held.shrink(12 * dimension as u64);
            }
            Ok(())
        })?;
        centroids.resize(dimension * groups as usize, 0.0);
        for group in seeds..groups {
            centroids.copy_within(..dimension, group as usize * dimension);
        }
        centroids
    } else {
        let (read, vector) = (ground.read, ground.column.id);
        ivf::centroids(read, vector, build.number, groups, dimension, held)?
    };
    let mut sums = Round::new(groups, dimension, held)?;
    sample(ground, live, &mut |_, slot| {
        sums.take(&centroids, slot);
        Ok(())
    })?;
    sums.finish(&mut centroids);
    step.centroids = centroids;
    let phase = match round + 1 {
        ROUNDS => Phase::Assign { next: 0 },
        round => Phase::Train { round },
    };
    moved(
        ground,
        step,
        Build {
            phase,
            ..build.clone()
        },
    );
    Ok(())
}

/// When the row of this key last left a sealed slot of the partition through the build's
/// source view: how many slots had been sealed into the partition's tail by then, `None` when
/// it never did. A record that disagrees with its partition or view is damage.
fn left(ground: &Ground<'_>, key: &Key) -> Result<Option<u64>> {
    let record = VectorRecord::Left(ground.column.id, ground.partition.number, key);
    let at = |v: &[u8]| left_at(ground.read, &ground.partition, v);
    let found = kept(ground.read, record, at)?.transpose()?;
    let source = ground.partition.building.map_or(0, |(_, source)| source);
    let through = |p: [(u64, u64); 2]| p.into_iter().find(|(_, f)| (1..=source).contains(f));
    Ok(found.and_then(through).map(|(s, _)| s))
}

/// A row's last departure and, while a build runs, the last through its source view: each a
/// sealed position and commit fence, the second zero when unset. A record that is not 32
/// bytes, or is not bounded by its partition and view, is damage.
pub(super) fn left_at(read: &dyn Read, p: &Partition, value: &[u8]) -> Result<[(u64, u64); 2]> {
    let pairs = <&[u8; 32]>::try_from(value).ok().map(|v| {
        let long = |at: usize| u64::from_le_bytes(std::array::from_fn(|byte| v[8 * at + byte]));
        [(long(0), long(1)), (long(2), long(3))]
    });
    let sound = |&(s, f): &(u64, u64)| s <= p.sealed && f <= read.commit().0;
    let sound = |pairs: &[(u64, u64); 2]| pairs[0].1 > 0 && pairs.iter().all(sound);
    let damage = "a record of a row that left its slot disagrees with its owner";
    pairs.filter(sound).ok_or_else(|| read.damaged(damage))
}

/// The step that gives the slots of the next blocks their groups: whole blocks while their
/// slots times the build's groups come to at most `STEP_COMPARISONS`, and one block at least.
/// A block's groups are one record, two bytes a slot, `0xFFFF` for a stale slot. After the
/// last block the build packs, from the first place.
///
/// A slot goes to the group whose centroid is nearest; a build of one group reads no centroid.
/// A build with stale slots first looks up when the row of the slot's key last left a slot,
/// and the slot is stale when it was sealed before then: a slot of the IVF the build
/// reads, all sealed before that IVF's `upto`, when the row left at that count or
/// later; a tail slot when its own place among the slots ever sealed is below the count.
///
/// The step's reservation is grown first by the build's record and its copy, the centroids,
/// and two bytes for each slot of a block.
fn assign(ground: &Ground<'_>, step: &mut Step, next: u32) -> Result<()> {
    let (build, read, dimension) = (&ground.build, ground.read, ground.dimension);
    let (vector, groups, held) = (ground.column.id, build.groups, &mut step.held);
    held.grow(HEAD as u64 + 8 * u64::from(groups))?;
    let mut after = build.clone();
    let mut centroids = Vec::new();
    if groups > 1 {
        centroids = ivf::centroids(read, vector, build.number, groups, dimension, held)?;
    }
    let starts = if build.reusing {
        ivf::Ivf::read(read, vector, build.from.expect("kept IVF"), held)?.starts
    } else {
        Vec::new()
    };
    let from = build.from.map(|from| (from.blocks, from.slots, from.upto));
    let (inside, grouped, _) = from.unwrap_or_default();
    // The place of the next tail slot among the slots ever sealed. The build's tail slots are
    // the last before its `upto`, and the records of the tail blocks whose slots have their
    // groups say how many came before this step's.
    let mut sealed = build.upto - (build.slots - grouped);
    if build.stale {
        let given = VectorRecord::Assigned(vector, build.number, inside);
        read.vectors(given, &mut |record, given| {
            let earlier = matches!(record, VectorRecord::Assigned(column, number, place)
                if (column, number) == (vector, build.number) && place < next);
            if earlier && given.len() % 2 != 0 {
                return Err(read.damaged(pack::ASSIGNED));
            }
            sealed += u64::from(earlier) * given.len() as u64 / 2;
            Ok(earlier)
        })?;
    }
    let (mut place, mut taken) = (next, 0);
    while place < build.blocks() {
        let more = input(ground, place, &mut |block| {
            let slots = block.len() as u64;
            if taken > 0 && (taken + slots) * u64::from(groups) > STEP_COMPARISONS {
                return Ok(false);
            }
            held.grow(2 * slots)?;
            let mut given = Vec::with_capacity(2 * block.len());
            for at in 0..block.len() {
                let stale = stale(ground, place, sealed + at as u64, &block.key(at))?;
                let here = Place {
                    block: place,
                    slot: at as u32,
                };
                let group = match (stale, groups) {
                    (true, _) => u16::MAX,
                    (false, _) if build.reusing && place < inside => {
                        (starts.partition_point(|start| *start <= here) - 1) as u16
                    }
                    (false, 1) => 0,
                    (false, _) => {
                        let live = |group: usize| starts[group] != starts[group + 1];
                        let eligible = build.reusing.then_some(&live as &dyn Fn(usize) -> bool);
                        let fine = (0..centroids.len() / dimension)
                            .filter(|at| eligible.is_none_or(|e| e(*at)));
                        step.compared += fine.count() as u64;
                        nearest_among(&centroids, dimension, block.slot(at), eligible).0 as u16
                    }
                };
                if !stale {
                    after.counts[usize::from(group)] += 1;
                    after.outside += u64::from(place >= inside);
                }
                given.extend(group.to_le_bytes());
            }
            let record = VectorRecord::Assigned(vector, build.number, place);
            step.puts.push((record, given));
            sealed += if place < inside { 0 } else { slots };
            taken += slots;
            Ok(true)
        })?;
        if !more {
            break;
        }
        place += 1;
    }
    after.phase = Phase::Assign { next: place };
    if place == build.blocks() {
        after.phase = Phase::Pack { group: 0 };
        after.starts = vec![Place::default()];
        if build.reusing && ground.partition.assigned + after.outside >= u64::from(groups).pow(2) {
            held.grow((place as u64 + u64::from(groups)) * size_of::<VectorRecord<'_>>() as u64)?;
            step.puts.clear();
            step.removes.extend(
                build
                    .written(vector, ground.partition.number, dimension)
                    .skip(1),
            );
            after.groups = groups_for(build.slots);
            held.grow(4 * u64::from(after.groups))?;
            after.counts = vec![0; after.groups as usize];
            after.starts.clear();
            after.reusing = false;
            after.outside = 0;
            after.phase = Phase::Train { round: 0 };
        }
    }
    moved(ground, step, after);
    Ok(())
}
