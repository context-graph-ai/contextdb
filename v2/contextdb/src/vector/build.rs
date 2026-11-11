//! Building a grouping for a partition, in steps: when one is due, the record of how far a
//! build has come, and the steps that find its centres and say which group each slot goes to.
//!
//! A build reads the partition as it stood when the build began (the grouping in force and the
//! tail blocks below `closed`) and writes beside it, under a number of its own. Nothing a
//! search reads changes until the last step, which makes the new grouping the one in force in
//! one commit. A step is worked out from one committed moment with no lock held, and staged
//! under the commit lock only if the build has not moved since.

use super::block::{BROKEN, BlockReader};
use super::directory::{self, Grouped, Partition, kept};
use super::grouping::{self, Place, centres_to_a_record};
use super::pack;
use super::seal::room;
use super::sketch::{sketch_len, sketches_of};
use super::train::{
    DRAWN, Mean, ROUNDS, Round, SAMPLE_TO_A_CENTRE, first_centres, groups_for, nearest, sampled,
};
use crate::memory::{Budget, Reservation};
use crate::schema::{Catalog, VectorColumn};
use crate::sql::not_supported;
use crate::store::{ChangeSet, Key, Read, VectorId, VectorNumber, VectorRecord};
use crate::types::{Result, VectorSearchMode};

/// The most slots one step gives a group or packs, unless one group is larger.
pub(super) const STEP_SLOTS: u64 = 4096;

/// The most comparisons of a slot with a centre in one step that gives slots their groups or
/// measures: 4,194,304. A round over the sample is one step whatever it compares.
pub(super) const STEP_COMPARISONS: u64 = 1 << 22;

/// How many questions a build asks of its new grouping to measure how far a search scans
/// and how wide it takes a sketch's estimate to be. Question `j` is the middle of two
/// stored vectors: the direction of the sum of the vectors that the slots at `j * slots /
/// MEASURED` and at half the grouping further on keep.
pub(super) const MEASURED: u32 = 128;

/// Bytes of a build's record before its lists.
const HEAD: usize = 158;

/// What a build does next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Phase {
    /// The next round over the sample, counted from zero; the centres so far are stored.
    Train { round: u32 },
    /// Giving slots their groups: the next block to read, by its place among those the build
    /// reads.
    Assign { next: u32 },
    /// Packing: the next group to write, and the block of the new grouping it starts in.
    Pack { group: u32 },
    /// Measuring the gap: how many questions have been asked, and the largest gap any of them
    /// needed for its ten nearest, in ten-thousandths.
    Measure { done: u32, most: u32 },
    /// Measuring the width, with the gap settled: how many questions have been asked again,
    /// the most groups any of them had inside the gap, and for each width from 2 to 16
    /// quarters how many of their neighbours the sketches of those groups kept, of `asked`
    /// neighbours in all.
    Width {
        gap: u32,
        probes: u32,
        done: u32,
        asked: u32,
        kept: [u32; 15],
    },
    /// Everything is written: the next step makes the grouping the one in force.
    Switch { gap: u32, probes: u32, width: u8 },
    /// The grouping is in force, and the build was of slots some of which were stale. The
    /// next steps remove the `Left` records the build applied, and the last removes the
    /// build's record.
    Sweep,
}

/// How far the build of a partition's next grouping has come.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Build {
    /// The number the new grouping's records are kept under.
    pub number: u32,
    /// How many steps of this build have been staged. A step that was worked out for another
    /// count is not staged.
    pub steps: u64,
    /// The grouping the build reads, the one in force when it began; `None` when there was
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
    /// How many groups the new grouping has: one in a column that is not at its size, and
    /// then the build finds no centre, writes no sketch and asks no question.
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
    /// The record of the build, every number low byte first. Six numbers of eight bytes:
    /// `steps`, `needing`, `slots`, `upto`, and the `slots` and `upto` of `from`. One byte of
    /// flags (1: some slot it reads is stale; 2: it has a `from`) and one for the phase,
    /// counted from zero in the order of [`Phase`]. Then numbers of four bytes: `number`, the
    /// two of `tail`, `groups`, the `number`, `blocks` and `groups` of `from`, nineteen for the
    /// fields of the phase in their order, the count of `starts`, `counts`, and the two
    /// numbers of every place of `starts` and then of `tens`. A number no field fills is zero.
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
        let flags = u8::from(self.stale) | (u8::from(self.from.is_some()) << 1);
        let places = self.starts.iter().chain(&self.tens);
        let places = places.flat_map(|place| [place.block, place.slot]);
        let head = [self.number, self.tail.0, self.tail.1, self.groups];
        let short = head.into_iter().chain(short).chain(carried);
        let short = short.chain([self.starts.len() as u32]);
        let short = short.chain(self.counts.iter().copied()).chain(places);
        let mut record = Vec::new();
        let first = [self.steps, self.needing, self.slots, self.upto];
        record.extend(first.into_iter().chain(long).flat_map(u64::to_le_bytes));
        record.extend([flags, phase]);
        record.extend(short.flat_map(u32::to_le_bytes));
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
        let (lists, []) = lists.as_chunks::<4>() else {
            return None;
        };
        let long = |at: usize| u64::from_le_bytes(std::array::from_fn(|byte| head[8 * at + byte]));
        let short =
            |at: usize| u32::from_le_bytes(std::array::from_fn(|byte| head[50 + 4 * at + byte]));
        let listed = |at: usize| u32::from_le_bytes(lists[at]);
        let (flags, groups, begun) = (head[48], short(3) as usize, short(26) as usize);
        if flags > 3 || lists.len() < groups + 2 * begun || (lists.len() - groups) % 2 != 0 {
            return None;
        }
        let phase = match head[49] {
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
        (sound && whole && agrees).then_some(build)
    }

    /// The build of the partition of this number, `None` when it has none. A record that
    /// disagrees with itself is damage.
    pub(super) fn read(read: &dyn Read, vector: VectorId, partition: u32) -> Result<Option<Build>> {
        let found = kept(read, VectorRecord::Build(vector, partition), Build::decode)?;
        let damage = "the record of a build of the vector part disagrees with itself";
        let build = found.map(|build| build.ok_or_else(|| read.damaged(damage)));
        build.transpose()
    }

    /// Every record the build has written so far, for the commit that removes them: its own
    /// record, the groups it gave, its centres and the blocks it packed with their sketches.
    /// Once its grouping is in force those are the grouping's and it has its record alone.
    pub(super) fn written(
        &self,
        vector: VectorId,
        partition: u32,
        dimension: usize,
    ) -> impl Iterator<Item = VectorRecord<'static>> + use<> {
        let number = self.number;
        let many = u32::from(self.groups > 1);
        let centres = self.groups.div_ceil(centres_to_a_record(dimension) as u32);
        let end = self.starts.last().copied().unwrap_or_default();
        let packed = end.block + u32::from(end.slot > 0);
        let (given, centres, packed) = match self.phase {
            Phase::Sweep => (0, 0, 0),
            Phase::Train { round } => (0, centres * u32::from(round > 0), packed),
            Phase::Assign { next } => (next, centres * many, packed),
            _ => (self.blocks(), centres * many, packed),
        };
        std::iter::once(VectorRecord::Build(vector, partition))
            .chain((0..given).map(move |place| VectorRecord::Assigned(vector, number, place)))
            .chain((0..centres).map(move |place| VectorRecord::Centres(vector, number, place)))
            .chain((0..packed).map(move |block| VectorRecord::Block(vector, number, block)))
            .chain((0..packed * many).map(move |block| VectorRecord::Sketch(vector, number, block)))
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
    /// The partition's number when the step was worked out, and the build's number and count
    /// of steps then; `None` for the step that begins a build.
    pub(super) at: (u32, Option<(u32, u64)>),
    /// The records the step writes and removes. A build's records hold no key, so they
    /// outlive what they were read from.
    pub(super) puts: Vec<(VectorRecord<'static>, Vec<u8>)>,
    pub(super) removes: Vec<VectorRecord<'static>>,
    /// The keys of the partition's `Left` records the step removes.
    pub(super) left: Vec<Key>,
    /// What the partition's record takes from the step: nothing, or the grouping the step
    /// puts in force.
    pub(super) switch: Option<Grouped>,
    /// The centres a round made, in the order of the groups; empty for any other step.
    pub(super) centres: Vec<f32>,
    /// The partition's mean, from the step that stages it: its record says so from then on.
    pub(super) mean: Option<Vec<f32>>,
    /// How many vectors the step finished: those it packed.
    pub(super) done: u64,
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

/// True when a grouping is due for a partition that is building none. `vectors` is how many
/// rows have a vector in the column, every partition together; the column is at its size when
/// that is its `index_at` or more, or when it declares `SEARCH_MODE INDEXED`.
///
/// In a column at its size: the partition has no grouping and its tail holds at least the
/// column's `grouped_from` slots (a hundredth of `index_at`, and one at least); or it has a
/// grouping of one group, holds that many slots, and a build now would give it more than one
/// group, the square root of its slots rounding to two or more, so that a partition whose
/// grouping already has the groups a new build would give it is never due on its size alone;
/// or it has a grouping and the vectors outside it (in tail blocks and pending) together with
/// the slots left behind come to `CHANGE_PERCENT` of its slots, or the slots left behind alone
/// to `TOMBSTONE_PERCENT`. In any other column only the
/// slots left behind count, against every slot the partition has: the build then has one
/// group and writes no centre and no sketch. Under `CONSOLIDATION NONE` only a first grouping
/// of a column at its size is ever due. The slots left behind are the slots of the grouping
/// and the tail less the rows with a vector that are not pending.
pub(super) fn due(
    partition: &Partition,
    pending: u64,
    column: &VectorColumn,
    vectors: u64,
) -> bool {
    let grouped = partition.grouping.map_or(0, |grouping| grouping.slots);
    let slots = grouped + partition.slots;
    let out = partition.slots + pending;
    let stale = (slots + pending).saturating_sub(partition.live);
    let behind = left_behind(partition, pending, column, vectors);
    if !sized(column, vectors) {
        return behind;
    }
    let Some(grouping) = partition.grouping else {
        return partition.slots >= column.grouped_from();
    };
    let grows = grouping.groups == 1 && slots >= column.grouped_from() && groups_for(slots) > 1;
    let reached = |(change, _): (u8, u8)| {
        grows || behind || (out + stale) * 100 >= u64::from(change) * grouped
    };
    // A partition with no slot has nothing to group: its pending rows are sealed first.
    slots > 0 && column.consolidation().is_some_and(reached)
}

/// True when the slots rows have left behind are alone enough for a grouping to be due: they
/// come to the column's `TOMBSTONE_PERCENT` of the slots of the grouping in force in a column
/// at its size, and of every slot of the partition in any other. [`due`] asks it, and
/// inspection gives it as the reason.
pub(super) fn left_behind(
    partition: &Partition,
    pending: u64,
    column: &VectorColumn,
    vectors: u64,
) -> bool {
    let grouped = partition.grouping.map_or(0, |grouping| grouping.slots);
    let slots = grouped + partition.slots;
    let stale = (slots + pending).saturating_sub(partition.live);
    let of = if sized(column, vectors) {
        grouped
    } else {
        slots
    };
    let reached = |(_, share): (u8, u8)| stale * 100 >= u64::from(share) * of;
    stale > 0 && column.consolidation().is_some_and(reached)
}

/// True when the column is at its size, `vectors` being its rows with a vector.
fn sized(column: &VectorColumn, vectors: u64) -> bool {
    vectors >= column.index_at() || column.mode() == VectorSearchMode::Indexed
}

/// True while a search of the nearest groups may be given this partition: the vectors
/// outside its grouping, in tail blocks and pending, are fewer than twice what makes a
/// grouping due. A partition with no grouping, or with one of one group, is searched whole,
/// and so while the vectors outside its grouping are fewer than twice the column's
/// `index_at`. Under `CONSOLIDATION NONE` a grouping of more groups is always usable.
pub(super) fn usable(partition: &Partition, pending: u64, column: &VectorColumn) -> bool {
    let out = partition.slots + pending;
    match partition.grouping.filter(|grouping| grouping.groups > 1) {
        None => out < column.index_at().saturating_mul(2),
        Some(grouping) => column.consolidation().is_none_or(|(change, _)| {
            let due = (100 * column.grouped_from()).max(u64::from(change) * grouping.slots);
            out.saturating_mul(100) < due.saturating_mul(2)
        }),
    }
}

/// Calls `each` with every partition of a column and its key tuple.
fn entries(
    read: &dyn Read,
    vector: VectorId,
    each: &mut dyn FnMut(&Key, Partition) -> Result<()>,
) -> Result<()> {
    let first = Key::from_values(&[], &[])?;
    let from = VectorRecord::Partition(vector, &first);
    read.vectors(from, &mut |record, entry| match record {
        VectorRecord::Partition(column, tuple) if column == vector => {
            each(tuple, Partition::decode(read, entry)?)?;
            Ok(true)
        }
        _ => Ok(false),
    })
}

/// How many rows have a vector in a column, every partition together.
fn live(read: &dyn Read, vector: VectorId) -> Result<u64> {
    let mut live = 0;
    entries(read, vector, &mut |_, partition| {
        live += partition.live;
        Ok(())
    })?;
    Ok(live)
}

/// How many rows of the partition of this number are pending.
pub(super) fn pending(read: &dyn Read, vector: VectorId, number: u32) -> Result<u64> {
    let first = Key::from_values(&[], &[])?;
    let mut pending = 0;
    let from = VectorRecord::Pending(vector, number, &first);
    read.vectors(from, &mut |record, _| {
        let more = matches!(record, VectorRecord::Pending(column, partition, _)
            if (column, partition) == (vector, number));
        pending += u64::from(more);
        Ok(more)
    })?;
    Ok(pending)
}

/// Calls `each` with every partition that is building a grouping or has one due, and its
/// column and key tuple: the one walk [`waiting`] and [`remaining`] make. A column's rows
/// with a vector are added up before any of its partitions is asked.
fn working(
    read: &dyn Read,
    catalog: &Catalog,
    each: &mut dyn FnMut(&VectorColumn, &Key, &Partition) -> Result<()>,
) -> Result<()> {
    for table in catalog.tables() {
        for column in &table.vectors {
            let vectors = live(read, column.id)?;
            entries(read, column.id, &mut |tuple, partition| {
                let pending = pending(read, column.id, partition.number)?;
                if partition.building.is_some() || due(&partition, pending, column, vectors) {
                    each(column, tuple, &partition)?;
                }
                Ok(())
            })?;
        }
    }
    Ok(())
}

/// The partitions with work for a build at this moment: those with none in force first, then
/// the others, each in the order of column and partition number. The list and each key tuple
/// are taken from `held` before they are made.
pub(crate) fn waiting(
    read: &dyn Read,
    catalog: &Catalog,
    held: &mut Reservation,
) -> Result<Vec<Work>> {
    let mut found = Vec::new();
    working(read, catalog, &mut |column, tuple, partition| {
        let grouped = partition.grouping.is_some();
        room(&mut found, usize::MAX, held)?;
        held.grow(tuple.as_bytes().len() as u64)?;
        found.push((grouped, column.id, partition.number, tuple.clone()));
        Ok(())
    })?;
    found.sort();
    held.grow((found.len() * size_of::<Work>()) as u64)?;
    let work = |(_, vector, _, tuple)| Work { vector, tuple };
    Ok(found.into_iter().map(work).collect())
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
) -> Result<Option<Step>> {
    let vector = work.vector;
    let found = directory::partition(read, vector, &work.tuple)?;
    let Some(((table, column), partition)) = catalog.vector(vector).zip(found) else {
        return Ok(None);
    };
    let mut step = Step {
        work: work.clone(),
        column: column.clone(),
        at: (partition.number, None),
        puts: Vec::new(),
        removes: Vec::new(),
        left: Vec::new(),
        switch: None,
        centres: Vec::new(),
        mean: None,
        done: 0,
        held: budget.reserve(0, "maintenance")?,
    };
    // A build's record is read into lists as long as itself: its bytes are taken first.
    let own = VectorRecord::Build(vector, partition.number);
    step.held
        .grow(kept(read, own, <[u8]>::len)?.unwrap_or(0) as u64)?;
    let Some(build) = Build::read(read, vector, partition.number)? else {
        let pending = pending(read, vector, partition.number)?;
        let due = due(&partition, pending, column, live(read, vector)?);
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
        Phase::Train { .. } => train(&ground, &mut step)?,
        Phase::Assign { .. } => assign(&ground, &mut step)?,
        Phase::Pack { .. } => pack::pack(&ground, &mut step)?,
        Phase::Measure { .. } => pack::measure(&ground, &mut step)?,
        Phase::Width { .. } => pack::width(&ground, &mut step)?,
        Phase::Switch { .. } => pack::switch(&ground, &mut step)?,
        Phase::Sweep => pack::sweep(&ground, &mut step)?,
    }
    Ok(Some(step))
}

/// Stages a step, under the commit lock, over the newest moment. Stages nothing and answers
/// zero when the partition is gone or has another number, or is not building the grouping,
/// at the count of steps, the step was worked out for: the one place that is checked. A step
/// that begins a build stages nothing either when no grouping is due any more. Otherwise
/// answers how many vectors the step finished.
///
/// The step that begins a build takes the build's number from the column's run of partition
/// numbers, writes the build's record, and closes the partition's tail blocks. Any other
/// stages its records, and writes the partition's record again, from what it says now, when
/// it stages the mean, puts a grouping in force or removes the build's record.
pub(crate) fn stage_step(step: Step, changes: &mut ChangeSet) -> Result<u64> {
    let (vector, tuple) = (step.work.vector, &step.work.tuple);
    let found = directory::partition(&*changes, vector, tuple)?;
    let Some(mut partition) = found.filter(|partition| partition.number == step.at.0) else {
        return Ok(0);
    };
    let entry = VectorRecord::Partition(vector, tuple);
    let own = VectorRecord::Build(vector, partition.number);
    let build = Build::read(&*changes, vector, partition.number)?;
    let mut held = step.held;
    let Some((number, steps)) = step.at.1 else {
        let vectors = live(&*changes, vector)?;
        let pending = pending(&*changes, vector, partition.number)?;
        if build.is_some() || !due(&partition, pending, &step.column, vectors) {
            return Ok(0);
        }
        let next = VectorNumber::NextPartition(vector);
        let number = changes.vector_number(next)?;
        changes.put_vector_number(next, number + 1)?;
        let number = u32::try_from(number).map_err(|_| {
            not_supported("more than 4,294,967,296 partitions over the life of a VECTOR column")
        })?;
        let slots = partition.grouping.map_or(0, |grouping| grouping.slots) + partition.slots;
        let grouped = sized(&step.column, vectors) && slots >= step.column.grouped_from();
        let groups = if grouped { groups_for(slots) } else { 1 };
        held.grow(HEAD as u64 + 8 * u64::from(groups))?;
        let build = Build {
            number,
            steps: 0,
            from: partition.grouping,
            tail: (partition.first, partition.blocks),
            slots,
            upto: partition.sealed,
            stale: slots + pending > partition.live,
            groups,
            phase: match groups {
                1 => Phase::Assign { next: 0 },
                _ => Phase::Train { round: 0 },
            },
            counts: vec![0; groups as usize],
            starts: Vec::new(),
            tens: Vec::new(),
            needing: 0,
        };
        changes.put_vector(own, build.encode())?;
        (partition.building, partition.closed) = (Some(number), partition.blocks);
        changes.put_vector(entry, partition.encode())?;
        return Ok(0);
    };
    let build = build.filter(|build| (build.number, build.steps) == (number, steps));
    let Some(build) = build.filter(|_| partition.building == Some(number)) else {
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
    for key in &step.left {
        changes.delete_vector(VectorRecord::Left(vector, partition.number, key))?;
    }
    if !step.centres.is_empty() {
        let dimension = step.centres.len() / build.groups as usize;
        grouping::put_centres(changes, vector, number, dimension, &step.centres)?;
    }
    if let Some(mean) = &step.mean {
        grouping::put_mean(changes, vector, partition.number, mean)?;
        partition.mean = true;
        // The tail blocks sealed since the build began were sealed with no mean to take a
        // sketch from: each is given its record of sketches here, from this mean.
        for at in partition.closed..partition.blocks {
            let block = VectorRecord::Block(vector, partition.number, at);
            let mut sketches = None;
            changes.vectors(block, &mut |found, bytes| {
                let slots = BlockReader::new(bytes, mean.len());
                if found == block && slots.is_whole() {
                    held.grow((4 + slots.len() * sketch_len(mean.len())) as u64)?;
                    sketches = Some(sketches_of(bytes, mean.len(), &mut |_| mean));
                }
                Ok(false)
            })?;
            let sketches = sketches.ok_or_else(|| changes.damaged(BROKEN))?;
            held.shrink(sketches.len() as u64);
            changes.put_vector(VectorRecord::Sketch(vector, partition.number, at), sketches)?;
        }
    }
    if let Some(grouped) = step.switch {
        // The tail blocks the build read go with the switch, and their slots with them.
        let read = build.slots - build.from.map_or(0, |from| from.slots);
        let slots = partition.slots.checked_sub(read);
        partition.slots = slots.ok_or_else(|| changes.damaged(directory::SHORT))?;
        (partition.grouping, partition.first) = (Some(grouped), build.tail.1);
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
pub(crate) fn stage_refusal(work: &Work, needed: u64, changes: &mut ChangeSet) -> Result<()> {
    let Some(partition) = directory::partition(&*changes, work.vector, &work.tuple)? else {
        return Ok(());
    };
    if let Some(mut build) = Build::read(&*changes, work.vector, partition.number)?
        && build.needing != needed
    {
        build.needing = needed;
        let own = VectorRecord::Build(work.vector, partition.number);
        changes.put_vector(own, build.encode())?;
    }
    Ok(())
}

/// What is left to do, over every VECTOR column, at one moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Remaining {
    /// Slots of builds under way not yet packed, and the slots of partitions whose grouping
    /// is due and not begun. Those of a stalled build are left out: no further cycle packs
    /// them until memory is found.
    pub vectors: u64,
    /// Partitions with a build due or under way.
    pub partitions: u64,
    /// Those of them whose last step was refused for memory.
    pub stalled: u64,
}

/// What is left to do at this moment.
pub(crate) fn remaining(read: &dyn Read, catalog: &Catalog) -> Result<Remaining> {
    let mut left = Remaining::default();
    working(read, catalog, &mut |column, _, partition| {
        let build = Build::read(read, column.id, partition.number)?;
        let grouped = partition.grouping.map_or(0, |grouping| grouping.slots);
        left.partitions += 1;
        match build {
            Some(build) if build.needing > 0 => left.stalled += 1,
            Some(build) => left.vectors += build.unpacked(),
            None => left.vectors += grouped + partition.slots,
        }
        Ok(())
    })?;
    Ok(left)
}

/// Gives `each` the block the build reads at this place, read in place, and answers what it
/// answers. A block that is missing or is not whole is damage.
fn input<T>(
    ground: &Ground<'_>,
    place: u32,
    each: &mut dyn FnMut(&BlockReader<'_>) -> Result<T>,
) -> Result<T> {
    let record = (ground.build).block(ground.column.id, ground.partition.number, place);
    let mut answer = None;
    ground.read.vectors(record, &mut |found, bytes| {
        let block = BlockReader::new(bytes, ground.dimension);
        if found == record && block.is_whole() {
            answer = Some(each(&block)?);
        }
        Ok(false)
    })?;
    answer.ok_or_else(|| ground.read.damaged(BROKEN))
}

/// Gives `each` the slots the build samples, each with its number among them, in one walk
/// over the blocks it reads.
fn sample(ground: &Ground<'_>, each: &mut dyn FnMut(u64, &[u8]) -> Result<()>) -> Result<()> {
    let build = &ground.build;
    let mut wanted = sampled(build.slots, build.groups).zip(0..).peekable();
    let mut before = 0;
    for place in 0..build.blocks() {
        input(ground, place, &mut |block| {
            let end = before + block.len() as u64;
            while let Some((slot, at)) = wanted.next_if(|(slot, _)| *slot < end) {
                each(at, block.slot((slot - before) as usize))?;
            }
            before = end;
            Ok(())
        })?;
    }
    Ok(())
}

/// Adds the build's record as a step leaves it to the step's records: one more step staged,
/// and none refused.
pub(super) fn moved(ground: &Ground<'_>, step: &mut Step, mut build: Build) {
    (build.steps, build.needing) = (ground.build.steps + 1, 0);
    let own = VectorRecord::Build(ground.column.id, ground.partition.number);
    step.puts.push((own, build.encode()));
}

/// The step that finds centres: one round over the sample. The first round of a build first
/// takes its centres from the sample, and that of a partition's first build also takes the
/// sample's mean, which the step stages as the partition's. After the last round the build
/// gives slots their groups.
///
/// The step's reservation is grown first by what it takes: the build's record and its copy,
/// four bytes a number of every centre, the sums, and in the first round eight bytes a group
/// for the places of the first centres and eight a number for the mean and for one centre.
pub(super) fn train(ground: &Ground<'_>, step: &mut Step) -> Result<()> {
    let Phase::Train { round } = ground.build.phase else {
        unreachable!("a round is the step of a build that is finding its centres")
    };
    let (build, dimension) = (&ground.build, ground.dimension);
    let (groups, held) = (build.groups, &mut step.held);
    held.grow(HEAD as u64 + 8 * u64::from(groups))?;
    let mut centres = if round == 0 {
        let sample_of = build.slots.min(SAMPLE_TO_A_CENTRE * u64::from(groups));
        held.grow((4 * dimension as u64 + 8 + DRAWN) * u64::from(groups))?;
        let first = first_centres(sample_of, groups);
        held.shrink(DRAWN * u64::from(groups));
        let mut first = first.into_iter().peekable();
        let mut centres = Vec::with_capacity(dimension * groups as usize);
        let mut mean = None;
        if !ground.partition.mean {
            mean = Some(Mean::new(dimension, held)?);
        }
        sample(ground, &mut |at, slot| {
            // The vector a slot keeps is the mean of that slot alone.
            if first.next_if_eq(&at).is_some() {
                let mut centre = Mean::new(dimension, held)?;
                centre.take(slot);
                held.grow(4 * dimension as u64)?;
                centres.extend(centre.finish());
                held.shrink(12 * dimension as u64);
            }
            mean.iter_mut().for_each(|mean| mean.take(slot));
            Ok(())
        })?;
        if let Some(mean) = mean {
            // The mean's bytes are taken before it is made; its sum's are given back after.
            held.grow(4 * dimension as u64)?;
            step.mean = Some(mean.finish());
            held.shrink(8 * dimension as u64);
        }
        centres
    } else {
        let (read, vector) = (ground.read, ground.column.id);
        grouping::centres(read, vector, build.number, groups, dimension, held)?
    };
    let mut sums = Round::new(groups, dimension, held)?;
    sample(ground, &mut |_, slot| {
        sums.take(&centres, slot);
        Ok(())
    })?;
    sums.finish(&mut centres);
    step.centres = centres;
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

/// When the row of this key last left a sealed slot of the partition: how many slots had
/// been sealed into the partition's tail by then, `None` when it never did. A record that is
/// not eight bytes, or says more than the partition has sealed, is damage.
fn left(ground: &Ground<'_>, key: &Key) -> Result<Option<u64>> {
    let record = VectorRecord::Left(ground.column.id, ground.partition.number, key);
    kept(ground.read, record, |value| left_at(ground, value))?.transpose()
}

/// The count a `Left` record of the partition holds. A value that is not eight bytes, or
/// says more than the partition has sealed, is damage.
pub(super) fn left_at(ground: &Ground<'_>, value: &[u8]) -> Result<u64> {
    let sound = |left: &u64| *left <= ground.partition.sealed;
    let left = value.try_into().ok().map(u64::from_le_bytes).filter(sound);
    let damage = "a record of a row that left its slot is not what its partition says";
    left.ok_or_else(|| ground.read.damaged(damage))
}

/// The step that gives the slots of the next blocks their groups: whole blocks while their
/// slots times the build's groups come to at most `STEP_COMPARISONS`, and one block at least.
/// A block's groups are one record, two bytes a slot, `0xFFFF` for a stale slot. After the
/// last block the build packs, from the first place.
///
/// A slot goes to the group whose centre is nearest; a build of one group reads no centre.
/// A build with stale slots first looks up when the row of the slot's key last left a slot,
/// and the slot is stale when it was sealed before then: a slot of the grouping the build
/// reads, all sealed before that grouping's `upto`, when the row left at that count or
/// later; a tail slot when its own place among the slots ever sealed is below the count.
///
/// The step's reservation is grown first by the build's record and its copy, the centres,
/// and two bytes for each slot of a block.
pub(super) fn assign(ground: &Ground<'_>, step: &mut Step) -> Result<()> {
    let Phase::Assign { next } = ground.build.phase else {
        unreachable!("slots are given their groups by a build that has its centres")
    };
    let (build, read, dimension) = (&ground.build, ground.read, ground.dimension);
    let (vector, groups, held) = (ground.column.id, build.groups, &mut step.held);
    held.grow(HEAD as u64 + 8 * u64::from(groups))?;
    let mut after = build.clone();
    let mut centres = Vec::new();
    if groups > 1 {
        centres = grouping::centres(read, vector, build.number, groups, dimension, held)?;
    }
    let from = build.from.map(|from| (from.blocks, from.slots, from.upto));
    let (inside, grouped, applied) = from.unwrap_or_default();
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
                let left = build.stale.then(|| left(ground, &block.key(at)));
                let stale = left.transpose()?.flatten().is_some_and(|left| {
                    let tail = place >= inside && sealed + (at as u64) < left;
                    tail || (place < inside && left >= applied)
                });
                let group = match (stale, groups) {
                    (true, _) => u16::MAX,
                    (false, 1) => 0,
                    (false, _) => nearest(&centres, dimension, block.slot(at)).0 as u16,
                };
                if !stale {
                    after.counts[usize::from(group)] += 1;
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
    }
    moved(ground, step, after);
    Ok(())
}
