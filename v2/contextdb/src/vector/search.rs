//! The one search: score the rows that are in no block exactly, screen the slots of the
//! selected partitions (every slot of a partition searched whole; of one searched by its
//! nearest groups, the slots its binary codes keep), ask for the true distance of the rows whose
//! range can reach the answer, and prove the answer before returning it.

use std::cell::Cell;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeSet, BinaryHeap};
use std::ops::Range;

use super::binary::{Codes, Estimate, WIDEST, code_len, write_code};
use super::block::{BLOCK_BYTES, BlockReader};
use super::build::usable;
use super::directory::{self, Grouped, Partition};
use super::ivf::{self, Ivf, LEVELS, Place, level};
use super::pack;
use super::slot::{Screen, slot_len, summed};
use super::train;
use crate::memory::Reservation;
use crate::schema::VectorColumn;
use crate::store::{Key, READING, Read, TableId, VectorId, VectorRecord};
use crate::types::{Error, Result, VectorSearchMode};

/// What a search is asked for.
pub(crate) struct Nearest<'a> {
    pub count: Option<&'a dyn Fn(u64) -> Result<()>>,
    /// The table of the column, for the rows a transaction has staged.
    pub table: TableId,
    pub vector: VectorId,
    /// What the column declares: `INDEXING_THRESHOLD` and `COMPACTION` decide what AUTO runs
    /// and when INDEXED may.
    pub column: &'a VectorColumn,
    /// The mode in force: the statement's, else the column's.
    pub mode: VectorSearchMode,
    /// The partitions to search, as key tuples or the first values of key tuples; empty for
    /// every partition of the column.
    pub scope: &'a [Key],
    /// Whether a partition's key tuple can pass the filter; one that cannot is not selected.
    pub matching: Option<&'a dyn Fn(&Key) -> bool>,
    /// The vector to be near: of the column's dimension, every number finite.
    pub query: &'a [f32],
    /// How many rows to return at most.
    pub k: usize,
    /// Whether a rank orders the answer: a partition searched exactly then asks the distance of
    /// every live vector, and screens none.
    pub ranked: bool,
    /// The least likeness, `1 - distance`, a row may have: a slot or a binary code whose high end
    /// is below it is not kept, and short of `k` rows it stands in for the last row.
    pub bound: Option<f64>,
    /// Whether a filter on the rows, or a state the table leaves out of every search, may pass
    /// a row by: the build's reach then ends no walk.
    pub filtered: bool,
}

/// One row of an answer.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Hit {
    pub key: Key,
    /// The row's distance, as `distance_of` gave it.
    pub distance: f64,
}

/// What a search gives.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Outcome {
    /// The rows, nearest first.
    pub hits: Vec<Hit>,
    pub stopped: bool,
}

/// How one partition of a search is searched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Route {
    /// Every slot of it.
    Exact,
    /// The binary codes of its nearest groups and of its tail, and the slots of those whose
    /// estimate can still reach the answer.
    Indexed,
}

/// What a search does with one of the partitions it selects, settled from the partition's
/// small records before a block is read.
pub(crate) struct Planned {
    /// The partition's key tuple.
    pub tuple: Key,
    /// Rows with a vector in it, and those of them that are pending.
    pub live: u64,
    pub pending: u64,
    /// Slots in its tail blocks, outside its IVF.
    pub tail: u64,
    /// The groups of its IVF and the most of them a search scanned for its build's own
    /// questions; zero and zero when it has none.
    pub groups: u32,
    pub probes: u32,
    pub route: Route,
    /// Whether INDEXED may be given this partition. A search under INDEXED with a partition
    /// that is not usable is refused whole.
    pub usable: bool,
    partition: Partition,
    /// The record of its IVF, read when its nearest groups are searched and it has more
    /// than one group.
    ivf: Option<Ivf>,
}

impl Planned {
    /// How many binary codes a search reads of it at most: one for each slot, and none of a
    /// partition whose slots are screened with no binary code read.
    fn codes(&self) -> u64 {
        let grouped = self.partition.ivf.map_or(0, |grouped| grouped.slots);
        match &self.ivf {
            Some(ivf) if ivf.width != 0 => self.partition.slots + grouped,
            _ => 0,
        }
    }
}

/// What a search would do with each partition it selects, in the order of their numbers:
/// the one plan [`search`] runs and `.explain` prints. EXACT searches every slot of every
/// one. INDEXED searches the nearest groups of every one, which for a partition with no
/// IVF or an IVF of one group is every slot, through the slot screen alone. AUTO
/// searches every slot of every one while the rows with a vector in all of them together are
/// fewer than the column's `INDEXING_THRESHOLD`, and from there on the nearest groups of each
/// partition that has an IVF and is usable, and every slot of the others. A filter on
/// rows changes no route: it is asked of each row as the search visits it.
///
/// Only partition, pending and IVF records are read; EXACT reads no IVF record and
/// answers no `probes`. What the list takes is taken from `reservation` first and stays taken
/// until the caller lets it go: [`directory::HELD`], its key tuple and the bytes of a `Planned`
/// for each.
pub(crate) fn plan(
    nearest: &Nearest<'_>,
    read: &dyn Read,
    reservation: &mut Reservation,
) -> Result<Vec<Planned>> {
    let mut partitions = directory::selected(read, nearest.column, nearest.scope, reservation)?;
    partitions.retain(|(tuple, _)| nearest.matching.is_none_or(|can| can(tuple)));
    reservation.grow((partitions.len() * size_of::<Planned>()) as u64)?;
    let live: u64 = partitions.iter().map(|(_, partition)| partition.live).sum();
    let mode = match nearest.mode {
        VectorSearchMode::Auto if live < nearest.column.index_at() => VectorSearchMode::Exact,
        mode => mode,
    };
    let mut targets = Vec::with_capacity(partitions.len());
    // The places of an IVF's record, eight bytes a group and one more, are taken first.
    let mut record_of = |grouped: Grouped| Ivf::read(read, nearest.vector, grouped, reservation);
    for (tuple, partition) in partitions {
        let pending = u64::from(partition.unindexed);
        let usable = usable(read, &partition, pending, nearest.column);
        let grouped = partition.ivf.filter(|grouped| grouped.groups > 1);
        // AUTO searches a partition whose IVF record is damaged exactly.
        let ivf = match (mode, grouped) {
            (VectorSearchMode::Indexed, Some(grouped)) => Some(record_of(grouped)?),
            (VectorSearchMode::Auto, Some(grouped)) if usable => match record_of(grouped) {
                Err(Error::StoreCorrupted { .. }) => None,
                found => Some(found?),
            },
            _ => None,
        };
        let indexed = mode == VectorSearchMode::Indexed || ivf.is_some();
        let route = if indexed {
            Route::Indexed
        } else {
            Route::Exact
        };
        targets.push(Planned {
            tuple,
            live: partition.live,
            pending,
            tail: partition.slots,
            groups: partition.ivf.map_or(0, |grouped| grouped.groups),
            probes: ivf
                .as_ref()
                .map_or(0, |ivf| nearest.column.probes.unwrap_or(ivf.probes)),
            route,
            usable,
            partition,
            ivf,
        });
    }
    Ok(targets)
}

/// What a search reads blocks and binary codes with.
struct Asked<'a> {
    count: Option<&'a dyn Fn(u64) -> Result<()>>,
    read: &'a dyn Read,
    vector: VectorId,
    dimension: usize,
    screen: &'a Screen,
}

/// What a store is told when the binary codes of a block are missing or are not one for each of
/// the block's slots.
pub(super) const BINARY_CODES: &str =
    "the codes of a block of the vector part are not one for each slot";

/// Hands `each` the records of binary codes of the blocks kept under `number` from block `from`
/// on, each with its block's number and how many binary codes it holds, while it answers true.
/// A record it asks for that is not there, or is not whole, is damage.
fn codes(
    asked: &Asked<'_>,
    number: u32,
    from: u32,
    each: &mut dyn FnMut(u32, &Codes<'_>, usize) -> Result<bool>,
) -> Result<()> {
    let (mut at, mut more) = (from, true);
    let first = VectorRecord::Binary(asked.vector, number, from);
    asked.read.vectors(first, &mut |record, bytes| {
        if record != VectorRecord::Binary(asked.vector, number, at) {
            return Err(asked.read.damaged(BINARY_CODES));
        }
        let (codes, count) = pack::codes(asked.read, bytes, asked.dimension)?;
        more = each(at, &codes, count)?;
        at += 1;
        Ok(more)
    })?;
    if more {
        return Err(asked.read.damaged(BINARY_CODES));
    }
    Ok(())
}

/// How far in ten-thousandths of a product a group lies from the nearest group, by the
/// products of their centroids with the vector of a search; held to 20,000.
fn apart(nearest: f32, product: f32) -> f64 {
    ((f64::from(nearest) - f64::from(product)) * 10_000.0).min(20_000.0)
}

/// How many of the groups, nearest first, lie no farther from the nearest than a gap in
/// ten-thousandths.
fn within(ranked: &[(u32, f32)], gap: f64) -> usize {
    let past = |(_, product): &&(u32, f32)| apart(ranked[0].1, *product) - gap;
    ranked.iter().take_while(|group| past(group) <= 0.0).count()
}

/// The first place of a block.
fn first_of(block: u32) -> Place {
    Place { block, slot: 0 }
}

/// The slots of block `at`, which has `count` of them, that lie between two places.
fn part(places: &Range<Place>, at: u32, count: usize) -> Range<usize> {
    let begin = if at == places.start.block {
        places.start.slot as usize
    } else {
        0
    };
    let end = if at == places.end.block {
        places.end.slot as usize
    } else {
        count
    };
    begin.min(end.min(count))..end.min(count)
}

/// What binary codes are estimated with: what the vector of the search gives once, and room for
/// the ranges of one block's binary codes.
struct Wide {
    estimate: Estimate,
    ranges: Vec<(f32, f32)>,
}

impl Wide {
    /// Takes the planes of the vector, 32 bytes for every 64 numbers begun, and eight bytes
    /// for each slot the largest block can have, before either is made.
    fn new(query: &[f32], reservation: &mut Reservation) -> Result<Wide> {
        let slots = (BLOCK_BYTES / slot_len(query.len())).max(1);
        reservation.grow((32 * query.len().div_ceil(64) + 8 * slots) as u64)?;
        Ok(Wide {
            estimate: Estimate::new(query),
            ranges: Vec::with_capacity(slots),
        })
    }

    /// Hands `each` the range of every binary code between two places of the blocks kept under
    /// `number`, with its place: `width` quarters of the binary code's factor wide around `base`,
    /// the product of the binary codes' reference with the vector of the search.
    fn span(
        &mut self,
        asked: &Asked<'_>,
        number: u32,
        places: Range<Place>,
        (base, width): (f32, u8),
        each: &mut dyn FnMut(Place, f32, f32) -> Result<()>,
        work: Option<&Work<'_>>,
    ) -> Result<()> {
        if places.start >= places.end {
            return Ok(());
        }
        let each = &mut |block, record: &Codes<'_>, count| {
            let slots = part(&places, block, count);
            if work.is_some_and(|work| !work.fits(slots.len() as u64)) {
                return Ok(false);
            }
            asked.count.map(|c| c(slots.len() as u64)).transpose()?;
            self.ranges.clear();
            let estimate = &self.estimate;
            estimate.ranges(record, slots.clone(), base, width, &mut self.ranges);
            for (slot, (low, high)) in slots.zip(&self.ranges) {
                let slot = slot as u32;
                each(Place { block, slot }, *low, *high)?;
            }
            Ok(first_of(block + 1) < places.end)
        };
        codes(asked, number, places.start.block, each)
    }
}

/// How far from its nearest group an IVF has to be scanned for the ten slots nearest
/// `query`, and where those slots are. Nearest is by the product of the vector a slot keeps,
/// its codes times its scale, with `query` at length one, and equal products go by place: no
/// row is read. Every group is scanned. The gap is the best centroid's product with `query`
/// less the least such product among the groups those slots are in, in ten-thousandths,
/// rounded up and held to 20,000; zero for an IVF of no slot, which has no place to give.
/// In the same pass one histogram counts the slots by their group and the highest level their
/// high end reaches. Then, nearest group first, each level of `reach` is raised to the rank,
/// from one, that holds 999 in 1,000 of the slots at least that alike, and to the rank of each
/// such slot of the ten; and no level is left below a higher one.
pub(super) fn gap_needed(
    read: &dyn Read,
    vector: VectorId,
    grouped: Grouped,
    ivf: &Ivf,
    query: &[f32],
    reservation: &mut Reservation,
    reach: &mut [u32; LEVELS],
) -> Result<(u32, Vec<Place>)> {
    let before = reservation.bytes();
    reservation.grow(4 * (query.len() + (LEVELS + 1) * grouped.groups as usize) as u64)?;
    let screen = Screen::new(query);
    // Each group's row counts its slots by the highest level they reach, none at the row's start.
    let mut counts = vec![0_u32; (LEVELS + 1) * grouped.groups as usize];
    let mut ten: Vec<(f32, Place, usize, usize)> = Vec::with_capacity(11);
    let (mut all, mut seen) = ([0_u64; LEVELS], [0_u64; LEVELS]);
    for block in 0..grouped.blocks {
        let record = VectorRecord::Block(vector, grouped.number, block);
        pack::with_block(read, record, query.len(), &mut |slots| {
            for slot in 0..slots.len() {
                let (_, high, near) = screen.range(slots.slot(slot));
                let slot = slot as u32;
                let place = Place { block, slot };
                let group = ivf.starts.partition_point(|start| *start <= place) - 1;
                let high = f64::from(high);
                let levels = (0..LEVELS).take_while(|at| level(*at) <= high).count();
                counts[(LEVELS + 1) * group + levels] += 1;
                all[..levels].iter_mut().for_each(|count| *count += 1);
                if ten.len() < 10 || near > ten[9].0 {
                    ten.push((near, place, group, levels));
                    // The sort keeps slots of one product in the order of their places.
                    ten.sort_by(|one, other| other.0.total_cmp(&one.0));
                    ten.truncate(10);
                }
            }
            Ok(())
        })?;
    }
    let mut ranked = ivf::nearest(read, vector, grouped, screen.unit(), reservation, None)?;
    ranked.retain(|(group, _)| ivf.nonempty(*group));
    for (rank, (group, _)) in (1..).zip(&ranked) {
        let (row, mut above) = (&counts[(LEVELS + 1) * *group as usize..], 0);
        for at in (0..LEVELS).rev() {
            above += u64::from(row[at + 1]);
            let of_ten = ten.iter().any(|one| one.2 == *group as usize && at < one.3);
            if 1000 * seen[at] < 999 * all[at] || of_ten {
                reach[at] = reach[at].max(rank);
            }
            seen[at] += above;
        }
    }
    for at in (1..LEVELS).rev() {
        reach[at - 1] = reach[at - 1].max(reach[at]);
    }
    let nearest = ranked[0].1;
    let of = |place: &Place| {
        let group = ivf.starts.partition_point(|start| start <= place) - 1;
        let found = ranked.iter().find(|(number, _)| *number as usize == group);
        found.map_or(nearest, |(_, product)| *product)
    };
    let least = ten.iter().map(|(_, place, ..)| of(place));
    let least = least.fold(nearest, f32::min);
    reservation.shrink(reservation.bytes() - before);
    let places = ten.into_iter().map(|(_, place, ..)| place);
    Ok((apart(nearest, least).ceil() as u32, places.collect()))
}

/// For each width from 2 to 16 quarters, how many of `wanted` (slots of the IVF's blocks,
/// the places [`gap_needed`] gave) a search keeps by its binary codes, wherever the slot is kept:
/// the binary codes of the groups inside `IVF.gap` are estimated, and a slot is kept at a
/// width when the range of its binary code at that width reaches the tenth best low end of those
/// ranges at that width, and so does the range of the binary code a tail block would keep of it,
/// which is made here from the slot's vector and `mean`, the partition's. A search estimates
/// a tail's binary codes after the groups', against that same low end. With the counts, how many
/// groups were inside the gap: one at least, the nearest.
pub(super) fn kept_by_width(
    read: &dyn Read,
    vector: VectorId,
    grouped: Grouped,
    ivf: &Ivf,
    (query, mean): (&[f32], &[f32]),
    wanted: &[Place],
    reservation: &mut Reservation,
) -> Result<([u32; 15], u32)> {
    let before = reservation.bytes();
    let dimension = query.len();
    let found = wanted.len() * size_of::<(Place, f32, f32)>();
    reservation.grow((4 * dimension + found + 4 + code_len(dimension)) as u64)?;
    let screen = Screen::new(query);
    let asked = Asked {
        count: None,
        read,
        vector,
        dimension: query.len(),
        screen: &screen,
    };
    let mut wide = Wide::new(query, reservation)?;
    let mut ranked = ivf::nearest(read, vector, grouped, screen.unit(), reservation, None)?;
    ranked.retain(|(group, _)| ivf.nonempty(*group));
    let inside = within(&ranked, f64::from(ivf.gap));
    ranked.truncate(inside);
    ranked.sort_unstable_by_key(|(group, _)| *group);
    // Each binary code is estimated once, at the widest: its middle and a quarter of its factor
    // give its range at every width. The ten best low ends at each width, the least first.
    let mut lows = [[f32::NEG_INFINITY; 10]; 15];
    let halves =
        |(low, high): (f32, f32)| ((low + high) / 2.0, (high - low) / f32::from(2 * WIDEST));
    let mut found: Vec<(Place, f32, f32)> = Vec::with_capacity(wanted.len());
    let each = &mut |place, low: f32, high: f32| {
        let (middle, quarter) = halves((low, high));
        if wanted.contains(&place) {
            found.push((place, middle, quarter));
        }
        for (width, lows) in (2..=WIDEST).zip(&mut lows) {
            let low = middle - f32::from(width) * quarter;
            if low > lows[0] {
                lows[0] = low;
                lows.sort_unstable_by(f32::total_cmp);
            }
        }
        Ok(())
    };
    for (group, base) in &ranked {
        let places = ivf.starts[*group as usize]..ivf.starts[*group as usize + 1];
        wide.span(&asked, grouped.number, places, (*base, WIDEST), each, None)?;
    }
    // The slots found, block by block: each one's binary code from the mean, and whether both of
    // its binary codes reach the low end at each width.
    let base = summed(mean, screen.unit(), |number| number);
    let mut code = vec![0; 4 + code_len(dimension)];
    let mut kept = [0; 15];
    for places in found.chunk_by(|one, next| one.0.block == next.0.block) {
        let record = VectorRecord::Block(vector, grouped.number, places[0].0.block);
        pack::with_block(read, record, dimension, &mut |block| {
            if places.iter().any(|slot| slot.0.slot >= block.len() as u32) {
                return Err(read.damaged("a vector slot is outside its block"));
            }
            for (place, middle, quarter) in places {
                let slot = block.slot(place.slot as usize);
                let own = train::product(screen.unit(), slot);
                write_code(slot, mean, &mut code[4..]);
                wide.ranges.clear();
                let one = Codes::new(&code, dimension);
                wide.estimate
                    .ranges(&one, 0..1, base, WIDEST, &mut wide.ranges);
                let from_mean = halves(wide.ranges[0]);
                for (width, (kept, lows)) in (2..=WIDEST).zip(kept.iter_mut().zip(&lows)) {
                    let reaches = |(middle, quarter): (f32, f32)| {
                        middle + f32::from(width) * quarter >= lows[0].max(own)
                    };
                    *kept += u32::from(reaches((*middle, *quarter)) && reaches(from_mean));
                }
            }
            Ok(())
        })?;
    }
    reservation.shrink(reservation.bytes() - before);
    Ok((kept, inside as u32))
}

/// Bytes reserved for a row of the answer besides its key.
const PAIR: u64 = 96;
const _: () = assert!(size_of::<(Order, Key)>() <= 32);

/// Bytes reserved for a lower end. A search holds at most as many as the answer has rows.
const LOW: u64 = 8;

/// The most binary codes a walk holds at once, twelve bytes each.
const HELD: usize = 1_024;

/// The `k` nearest rows, nearest first; rows at one distance come in the order of their keys.
///
/// `distance_of` is `query`'s: given a row's key it reads the row and answers with its distance
/// from the vector of the search, or with `None` when the row is not there, does not pass the
/// statement's filter or has no vector. This function calls it once for every row the reader
/// has staged, and for the slots, pending or in blocks, it could not rule out, best range
/// first. It never returns a row `distance_of` did not give a distance for.
///
/// A row is in an answer once. Several entries can lead to one row: a row the reader has staged
/// and the pending record or the slot of the row it replaces, or the slots of two vectors the
/// row has had. `distance_of` answers from the row's stored vector, so every one of them gives
/// the same distance and key. The answer holds each key once: a pair whose key it already holds
/// is dropped when it arrives, and a pair whose row was pushed out earlier cannot come back,
/// because it is greater than every pair the answer holds by then. An entry left by an earlier
/// vector therefore costs one row read and changes nothing.
///
/// What rules a slot or a binary code out is only ever a row read and given a distance: the
/// similarity of the last row of a full answer, or the search's bound. A slot or a binary code
/// whose high end is below it cannot enter, and nothing is screened, estimated or asked twice.
///
/// Over partitions searched whole the answer is exactly the `k` least of `(distance, key)` over
/// the rows `distance_of` gives a distance for. A partition searched by its nearest groups is
/// walked outward from its centroids, ranked once: its pending slots, its tail and its nearest
/// group first, then one group at a time, each group's kept slots asked for before the next is
/// taken. The walk takes the groups inside the IVF's gap, or the declared `PROBES`, and goes on
/// past them only while the answer is short of `k` rows and its bound cut nothing; it ends at
/// the last group. Blocks and binary codes are read in place from the page cache; everything
/// that grows is taken from `reservation` first. When the answer is returned, `reservation`
/// holds 32 bytes and the key's for each of its rows and nothing else of the search but what
/// [`plan`] took. `targets` are what [`plan`] gave for `nearest`.
///
/// The caller may retry index damage as EXACT with its candidate set restored.
pub(crate) fn search(
    nearest: &Nearest<'_>,
    read: &dyn Read,
    reservation: &mut Reservation,
    targets: &[Planned],
    distance_of: &mut dyn FnMut(&Key) -> Result<Option<f64>>,
    (limit, evaluations): (&Cell<u64>, &Cell<u64>),
) -> Result<Outcome> {
    let work = Work { evaluations, limit };
    answer(nearest, read, reservation, targets, distance_of, &work)
        .inspect_err(|_| READING.set(None))
}

/// [`search`], in the mode it is given.
fn answer(
    nearest: &Nearest<'_>,
    read: &dyn Read,
    reservation: &mut Reservation,
    targets: &[Planned],
    distance_of: &mut dyn FnMut(&Key) -> Result<Option<f64>>,
    work: &Work<'_>,
) -> Result<Outcome> {
    if nearest.k == 0 {
        return Ok(Outcome {
            hits: Vec::new(),
            stopped: false,
        });
    }
    let before = reservation.bytes();
    // The vector at length one that the screen keeps: four bytes a number.
    reservation.grow(4 * nearest.query.len() as u64)?;
    let screen = Screen::new(nearest.query);
    let asked = Asked {
        count: nearest.count,
        read,
        vector: nearest.vector,
        dimension: nearest.query.len(),
        screen: &screen,
    };
    let mut wide = None;
    if targets.iter().any(|target| target.codes() > 0) {
        wide = Some(Wide::new(nearest.query, reservation)?);
    }
    let mut found = Found {
        k: nearest.k,
        declared: nearest.column.probes,
        answer: BTreeSet::new(),
        lows: Default::default(),
        reservation,
        distance_of,
        kept: Kept::default(),
        ranked: nearest.ranked,
        reaching: !nearest.ranked && !nearest.filtered && nearest.column.probes.is_none(),
        every: false,
        work,
        bound: nearest.bound.unwrap_or(f64::NEG_INFINITY),
        cut: false,
    };

    // The rows that are in no block, exactly: those the reader has staged.
    read.staged(nearest.table, &mut |key| found.score(key).map(|()| true))?;
    let mut held = Vec::new();
    for target in targets {
        if !work.fits(1) {
            break;
        }
        found.walk(&asked, target, &mut wide, &mut held)?;
    }
    READING.set(None);
    drop(held);

    let pairs = found.answer.iter();
    let pairs = pairs.map(|(_, key)| PAIR + key.as_bytes().len() as u64);
    let held = before + pairs.sum::<u64>();
    found.reservation.shrink(found.reservation.bytes() - held);
    let hits = found.answer.into_iter();
    let hits = hits.map(|(Order(distance), key)| Hit { key, distance });
    Ok(Outcome {
        hits: hits.collect(),
        stopped: !work.fits(1),
    })
}

/// A distance or a similarity where numbers are put in order. None is NaN: a distance is
/// between vectors of finite numbers.
#[derive(Clone, Copy)]
struct Order(f64);

impl Ord for Order {
    fn cmp(&self, other: &Order) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl PartialOrd for Order {
    fn partial_cmp(&self, other: &Order) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Order {
    fn eq(&self, other: &Order) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Order {}

/// The units a search has spent and the most it may: the caller may lower the most while the
/// search runs, and the search then stops at its next unit.
struct Work<'a> {
    evaluations: &'a Cell<u64>,
    limit: &'a Cell<u64>,
}
impl Work<'_> {
    fn fits(&self, count: u64) -> bool {
        let fits = count <= self.limit.get().saturating_sub(self.evaluations.get());
        if !fits {
            self.limit.set(self.evaluations.get());
        }
        fits
    }
}

/// What a search has found so far.
struct Found<'a> {
    k: usize,
    declared: Option<u32>,
    /// The answer so far: the `k` least pairs of distance and key among the rows scored.
    answer: BTreeSet<(Order, Key)>,
    reservation: &'a mut Reservation,
    distance_of: &'a mut dyn FnMut(&Key) -> Result<Option<f64>>,
    /// The `k` greatest lower ends of the rows read and the slots screened, whose fall to the
    /// answer's last row widens the gap in the proof; and of the binary codes estimated, below
    /// which, and below the last row, a code is dropped, as its ends are calibrated, not proven.
    lows: [BinaryHeap<Reverse<Order>>; 2],
    /// The slots screened and not yet asked for.
    kept: Kept,
    work: &'a Work<'a>,
    /// Whether a rank orders the answer, and whether every slot of the partition in hand
    /// has its row's distance asked for without screening.
    ranked: bool,
    every: bool,
    /// Whether the build's reach may end the walk: no rank, no filter on the rows and no
    /// declared PROBES.
    reaching: bool,
    /// The least likeness a row may have, and whether it has cut a row, a slot or a binary code.
    bound: f64,
    cut: bool,
}

impl Found<'_> {
    /// Asks for the distance of one row and offers it to the answer.
    fn score(&mut self, key: &Key) -> Result<()> {
        if self.work.fits(1)
            && let Some(distance) = (self.distance_of)(key)?
            && self.offer(distance, key)?
        {
            self.low(0, 1.0 - distance)?;
        }
        Ok(())
    }

    /// Offers a lower end to one list, keeping the `k` greatest.
    fn low(&mut self, list: usize, low: f64) -> Result<()> {
        let lows = &mut self.lows[list];
        if lows.len() < self.k {
            self.reservation.grow(LOW)?;
            lows.push(Reverse(Order(low)));
        } else if let Some(mut least) = lows.peek_mut()
            && Order(low) > least.0
        {
            *least = Reverse(Order(low));
        }
        Ok(())
    }

    /// The least of one list's `k` lower ends, and nothing while there are fewer.
    fn least(&self, list: usize) -> Option<f64> {
        let lows = &self.lows[list];
        let least = lows.peek().filter(|_| lows.len() == self.k);
        least.map(|Reverse(Order(low))| *low)
    }

    /// How far below the `k`-th greatest lower end the last row of the answer, or the bound
    /// that stands for it, lies: four times that, as a product, is how far behind the nearest
    /// group a group can lie and hold a nearer row.
    fn fall(&self, last: f64) -> f64 {
        4.0 * (self.least(0).unwrap_or(last) - last).max(0.0)
    }

    /// Offers a row to the answer and says whether it went in. This is the one way in, and
    /// its one rule: a pair whose key the answer holds is dropped; any other goes in when the
    /// answer has room, or when it is less than the greatest pair held, which it replaces.
    ///
    /// A key always comes with the same distance, so the pair is what the answer is asked for.
    fn offer(&mut self, distance: f64, key: &Key) -> Result<bool> {
        if 1.0 - distance < self.bound {
            self.cut = true;
            return Ok(false);
        }
        // The pair's bytes are asked for before the key is copied.
        let bytes = PAIR + key.as_bytes().len() as u64;
        self.reservation.grow(bytes)?;
        let pair = (Order(distance), key.clone());
        let full = self.answer.len() == self.k;
        let worse = full && self.answer.last().is_some_and(|last| pair > *last);
        if worse || self.answer.contains(&pair) {
            self.reservation.shrink(bytes);
            return Ok(false);
        }
        if full && let Some((_, out)) = self.answer.pop_last() {
            self.reservation.shrink(PAIR + out.as_bytes().len() as u64);
        }
        self.answer.insert(pair);
        Ok(true)
    }

    /// The similarity of the last row of the answer once it has `k` rows.
    fn last(&self) -> Option<f64> {
        let last = self.answer.last().filter(|_| self.answer.len() == self.k);
        last.map(|(Order(distance), _)| 1.0 - distance)
    }

    /// What the high end of a slot or a binary code must reach: proven by rows read.
    fn floor(&self) -> f64 {
        self.last().unwrap_or(f64::NEG_INFINITY).max(self.bound)
    }

    /// Whether a high end is below the floor, noting a cut by the bound.
    fn below(&mut self, high: f32) -> bool {
        self.cut |= f64::from(high) < self.bound;
        f64::from(high) < self.floor()
    }

    /// Screens one slot, of a block or of a pending record, and keeps it with a copy of its
    /// key unless its `high` is below the floor.
    fn look(&mut self, asked: &Asked<'_>, slot: &[u8], key: &Key) -> Result<()> {
        if key.as_bytes().is_empty() {
            return Ok(());
        }
        if self.every {
            return self.score(key);
        }
        if !self.work.fits(1) {
            return Ok(());
        }
        asked.count.map(|c| c(1)).transpose()?;
        let (low, high, _) = asked.screen.range(slot);
        if self.below(high) {
            return Ok(());
        }
        self.low(0, f64::from(low))?;
        // The list never holds more than one batch: a full one is asked for first.
        if self.kept.slots.len() == HELD {
            self.verify()?;
        }
        self.kept.keep(high, key, self.reservation)
    }

    /// Asks for the rows of the kept slots, best `high` first, while a `high` reaches the
    /// floor, and empties the list.
    fn verify(&mut self) -> Result<()> {
        self.ask(usize::MAX)
    }

    /// Asks for the rows of at most `most` kept slots, best `high` first, while a `high` reaches
    /// the floor; keeps the rest of a list cut short by `most`, else empties it.
    fn ask(&mut self, most: usize) -> Result<()> {
        let mut kept = std::mem::take(&mut self.kept);
        let by_high = |a: &Slot, b: &Slot| b.high.total_cmp(&a.high).then(a.start.cmp(&b.start));
        kept.slots.sort_unstable_by(by_high);
        let mut asked = 0;
        for slot in &kept.slots {
            if f64::from(slot.high) < self.floor() || !self.work.fits(1) {
                asked = kept.slots.len();
                break;
            }
            if asked == most {
                break;
            }
            self.score(&Key::from_bytes(
                &kept.keys[slot.start..][..slot.len as usize],
            ))?;
            asked += 1;
        }
        match asked < kept.slots.len() {
            true => kept.forget(asked),
            false => kept.clear(),
        }
        self.kept = kept;
        Ok(())
    }

    /// Screens every slot from one place up to another of the blocks kept under `number`, and
    /// asks for the rows of each block's kept slots.
    fn between(&mut self, asked: &Asked<'_>, number: u32, places: Range<Place>) -> Result<()> {
        if places.start >= places.end {
            return Ok(());
        }
        let span = places.start.block
            ..places
                .end
                .block
                .saturating_add(u32::from(places.end.slot > 0));
        for at in span {
            let record = VectorRecord::Block(asked.vector, number, at);
            pack::with_block(asked.read, record, asked.dimension, &mut |block| {
                for slot in part(&places, at, block.len()) {
                    self.look(asked, block.slot(slot), &block.key(slot))?;
                }
                Ok(())
            })?;
            // A block's kept slots are asked for before the next block is read.
            self.verify()?;
            if !self.work.fits(1) {
                break;
            }
        }
        Ok(())
    }

    /// Holds a binary code unless its high end is below the floor; a full list is screened
    /// first.
    fn code(
        &mut self,
        asked: &Asked<'_>,
        at: (&Partition, u32),
        held: &mut Vec<(f32, Place)>,
        place: Place,
        (low, high): (f32, f32),
    ) -> Result<()> {
        let floor = self.last().unwrap_or(f64::NEG_INFINITY);
        let floor = floor
            .min(self.least(1).unwrap_or(f64::NEG_INFINITY))
            .max(self.bound);
        self.cut |= f64::from(high) < self.bound;
        if f64::from(high) < floor {
            return Ok(());
        }
        self.low(1, f64::from(low))?;
        if held.len() == HELD {
            self.screen_held(asked, at, held)?;
        }
        self.reservation.room(held, HELD)?;
        held.push((high, place));
        Ok(())
    }

    /// Screens the slots of the binary codes held among the blocks under `number`, and those
    /// only, and empties the list. A block is read here for the first time, so here its
    /// binary codes are found to be one for each of its slots, or damage.
    fn screen_held(
        &mut self,
        asked: &Asked<'_>,
        (p, n): (&Partition, u32),
        held: &mut Vec<(f32, Place)>,
    ) -> Result<()> {
        for places in held.chunk_by(|one, next| one.1.block == next.1.block) {
            let record = VectorRecord::Block(asked.vector, n, places[0].1.block);
            let mut look = |b: &BlockReader<'_>| {
                let mut each = |(_, at): &(f32, Place)| {
                    let slot = at.slot as usize;
                    self.look(asked, b.slot(slot), &b.key(slot))
                };
                places.iter().try_for_each(&mut each)
            };
            pack::companions(
                asked.read,
                record,
                asked.dimension,
                (p, None),
                None,
                &mut look,
            )?;
        }
        held.clear();
        Ok(())
    }

    /// The slots between two places of the blocks under `number`: screened by their binary
    /// codes around `base` where the IVF has a width, else each one.
    fn span(
        &mut self,
        asked: &Asked<'_>,
        (p, number): (&Partition, u32),
        places: Range<Place>,
        (base, width): (f32, u8),
        (wide, held): (&mut Option<Wide>, &mut Vec<(f32, Place)>),
    ) -> Result<()> {
        let Some(wide) = wide.as_mut().filter(|_| width != 0) else {
            return self.between(asked, number, places);
        };
        let work = self.work;
        let each = &mut |place, low, high| self.code(asked, (p, number), held, place, (low, high));
        wide.span(asked, number, places, (base, width), each, Some(work))?;
        self.screen_held(asked, (p, number), held)
    }

    /// Walks one partition: its pending slots, then every slot of a partition searched whole,
    /// or, outward from its nearest centroid, its tail and its groups.
    fn walk(
        &mut self,
        asked: &Asked<'_>,
        target: &Planned,
        wide: &mut Option<Wide>,
        held: &mut Vec<(f32, Place)>,
    ) -> Result<()> {
        let p = &target.partition;
        READING.set(p.ivf.map(|grouped| (asked.vector, grouped.number)));
        self.every = (self.ranked && target.route == Route::Exact)
            || (p.ivf.is_none() && target.live <= self.k as u64);
        let (first, run) = (Key::from_values(&[], &[])?, (asked.vector, p.number));
        let from = VectorRecord::Pending(run.0, run.1, &first);
        asked.read.vectors(from, &mut |record, slot| match record {
            VectorRecord::Pending(vector, number, key) if (vector, number) == run => {
                if slot.len() != slot_len(asked.dimension) {
                    return Err(asked.read.damaged("a pending record is not a slot"));
                }
                self.look(asked, slot, key).map(|()| true)
            }
            _ => Ok(false),
        })?;
        let tail = first_of(p.first)..first_of(p.blocks);
        let (Some(ivf), Some(grouped)) = (&target.ivf, p.ivf) else {
            if let Some(grouped) = p.ivf {
                self.between(
                    asked,
                    grouped.number,
                    Place::default()..first_of(grouped.blocks),
                )?;
            }
            self.between(asked, p.number, tail)?;
            return self.verify();
        };
        if !self.work.fits(u64::from(grouped.groups)) {
            return Ok(());
        }
        let (unit, n) = (asked.screen.unit(), asked.dimension);
        let mut ranked = ivf::nearest(
            asked.read,
            asked.vector,
            grouped,
            unit,
            self.reservation,
            asked.count,
        )?;
        ranked.retain(|(group, _)| ivf.nonempty(*group));
        let width = if wide.is_some() { ivf.width } else { 0 };
        let mean = if width == 0 {
            0.0
        } else {
            let mean = ivf::mean(
                asked.read,
                asked.vector,
                grouped.number,
                n,
                self.reservation,
            )?;
            asked.count.map(|c| c(1)).transpose()?;
            self.reservation.shrink(4 * n as u64);
            summed(&mean, unit, |number| number)
        };
        // The tail's binary codes are held against the mean, as the build measured them.
        self.span(asked, (p, p.number), tail, (mean, width), (wide, held))?;
        let cover = self
            .declared
            .map_or_else(|| within(&ranked, f64::from(ivf.gap)), |n| n as usize);
        let nearest = ranked.first().map_or(0.0, |(_, product)| *product);
        for (at, (group, base)) in ranked.iter().enumerate() {
            // The proof: the answer is full, or short with a bound that cut, and no group left
            // out lies within the gap widened by the fall; a declared PROBES is not widened.
            let last = self.last().or(Some(self.bound).filter(|_| self.cut));
            let past = |last| apart(nearest, *base) - f64::from(ivf.gap) > self.fall(last) * 1e4;
            let proven = last.is_some_and(|last| self.declared.is_some() || past(last));
            // Or, short of the cover, by the build's reach at the cutoff's level; never under a
            // bound that has cut nothing.
            let open = self.reaching && at < cover && (self.cut || self.bound == f64::NEG_INFINITY);
            let reach = last.filter(|_| open).and_then(|last| ivf.reach_at(last));
            if !self.work.fits(1) || at >= cover && proven || reach.is_some_and(|r| at >= r) {
                break;
            }
            let group = *group as usize;
            let places = ivf.starts[group]..ivf.starts[group + 1];
            self.span(
                asked,
                (p, grouped.number),
                places,
                (*base, width),
                (wide, held),
            )?;
            // The best `k` set the answer's last row for the next group's proof and reach; the
            // rest wait, so a row a nearer group would push out is not read now.
            self.ask(self.k)?;
        }
        self.reservation.shrink(8 * u64::from(grouped.groups));
        self.verify()
    }
}

/// A slot still in the running: the top of its range and where its key is in [`Kept::keys`].
struct Slot {
    high: f32,
    len: u32,
    start: usize,
}
/// The slots still in the running.
#[derive(Default)]
struct Kept {
    slots: Vec<Slot>,
    /// The keys of the slots, one after another.
    keys: Vec<u8>,
    /// How many bytes of keys the reservation holds room for.
    key_room: usize,
}

impl Kept {
    /// Empties the list and keeps its room.
    fn clear(&mut self) {
        self.slots.clear();
        self.keys.clear();
    }

    /// Drops the first `n` slots and their keys, moving the other keys down in place.
    fn forget(&mut self, n: usize) {
        self.slots.drain(..n);
        self.slots.sort_unstable_by_key(|slot| slot.start);
        let mut end = 0;
        for slot in &mut self.slots {
            let len = slot.len as usize;
            self.keys.copy_within(slot.start..slot.start + len, end);
            (slot.start, end) = (end, end + len);
        }
        self.keys.truncate(end);
    }

    /// Adds a slot to a list that holds fewer than `HELD`.
    fn keep(&mut self, high: f32, key: &Key, reservation: &mut Reservation) -> Result<()> {
        reservation.room(&mut self.slots, HELD)?;
        let key = key.as_bytes();
        let start = self.keys.len();
        if start + key.len() > self.key_room {
            let more = (start + key.len() - self.key_room).max(self.key_room);
            reservation.grow((self.key_room + more) as u64)?;
            self.keys.reserve_exact(self.key_room + more - start);
            reservation.shrink(self.key_room as u64);
            self.key_room += more;
        }
        self.keys.extend_from_slice(key);
        // The key is out of a block, where four bytes say where it ends: its length fits.
        let len = key.len() as u32;
        self.slots.push(Slot { high, len, start });
        Ok(())
    }
}
