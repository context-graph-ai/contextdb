//! The one search: score the rows that are in no block exactly, screen the slots of the
//! selected partitions (every slot of a partition searched whole; of one searched by its
//! nearest groups, the slots its binary codes keep), ask for the true distance of the rows whose
//! range can reach the answer, and prove the answer before returning it.

use std::cell::Cell;
use std::cmp::{Ordering, Reverse};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::ops::Range;

use super::binary::{Codes, Estimate, WIDEST, code_len, write_code};
use super::block::{BLOCK_BYTES, BlockReader};
use super::build::usable;
use super::directory::{self, Grouped, Partition};
use super::ivf::{self, Ivf, Place};
use super::pack;
use super::slot::{SLOT_HEADER, Screen, product, scale, slot_len, summed};
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
    /// The vector to be near: of the column's dimension, every number finite.
    pub query: &'a [f32],
    /// How many rows to return at most.
    pub k: usize,
    /// Whether the statement has a filter on rows: one that does more than select partitions.
    /// The filter itself is asked through `distance_of`. This says one thing more: a search
    /// under INDEXED that runs out of binary codes to choose among is refused.
    pub filtered: bool,
    /// Whether a rank orders the answer: a partition searched exactly then asks the distance of
    /// every live vector, and screens none.
    pub ranked: bool,
    /// The least likeness, `1 - distance`, a row may have: a slot or a binary code whose high end
    /// is below it is not kept, and short of `k` rows it stands in for the last row.
    pub bound: Option<f64>,
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
/// small records before a block is read, and how far it has gone over it.
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
    /// How many of its groups the last pass took, nearest first, and how far in
    /// ten-thousandths the next one lies past its gap: infinitely when it took them all.
    taken: usize,
    next: f64,
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
    let partitions = directory::selected(read, nearest.column, nearest.scope, reservation)?;
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
            taken: 0,
            next: f64::INFINITY,
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
/// ten-thousandths widened by `fall`, a difference of products.
fn within(ranked: &[(u32, f32)], gap: f64, fall: f64) -> usize {
    let past = |(_, product): &&(u32, f32)| apart(ranked[0].1, *product) - gap;
    ranked
        .iter()
        .take_while(|group| past(group) <= fall * 10_000.0)
        .count()
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
pub(super) fn gap_needed(
    read: &dyn Read,
    vector: VectorId,
    grouped: Grouped,
    ivf: &Ivf,
    query: &[f32],
    reservation: &mut Reservation,
) -> Result<(u32, Vec<Place>)> {
    let before = reservation.bytes();
    reservation.grow(4 * query.len() as u64)?;
    let screen = Screen::new(query);
    let mut ten: Vec<(f32, Place)> = Vec::with_capacity(11);
    for block in 0..grouped.blocks {
        let record = VectorRecord::Block(vector, grouped.number, block);
        pack::with_block(read, record, query.len(), &mut |slots| {
            for slot in 0..slots.len() {
                let kept = slots.slot(slot);
                let scale = scale(kept);
                let near = scale * product(&kept[SLOT_HEADER..], screen.unit());
                if ten.len() < 10 || near > ten[9].0 {
                    let slot = slot as u32;
                    ten.push((near, Place { block, slot }));
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
    let nearest = ranked[0].1;
    let of = |place: &Place| {
        let group = ivf.starts.partition_point(|start| start <= place) - 1;
        let found = ranked.iter().find(|(number, _)| *number as usize == group);
        found.map_or(nearest, |(_, product)| *product)
    };
    let least = ten.iter().map(|(_, place)| of(place));
    let least = least.fold(nearest, f32::min);
    reservation.shrink(reservation.bytes() - before);
    let places = ten.into_iter().map(|(_, place)| place);
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
    let inside = within(&ranked, f64::from(ivf.gap), 0.0);
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

/// Bytes reserved for a lower bound. A search holds at most as many as the answer has rows.
const LOW: u64 = 8;

/// The `k` nearest rows, nearest first; rows at one distance come in the order of their keys.
///
/// `distance_of` is `query`'s: given a row's key it reads the row and answers with its distance
/// from the vector of the search, or with `None` when the row is not there, does not pass the
/// statement's filter or has no vector. This function calls it once for every row the reader
/// has staged, once for every pending row of the selected partitions, and for the slots it
/// could not rule out, best range first. It never returns a row `distance_of` did not give a
/// distance for.
///
/// A row is in an answer once. Several entries can lead to one row: a row the reader has staged
/// and the pending record or the slot of the row it replaces, or the slots of two vectors the
/// row has had. `distance_of` answers from the row's stored vector, so every one of them gives
/// the same distance and key. The answer holds each key once: a pair whose key it already holds
/// is dropped when it arrives, and a pair whose row was pushed out earlier cannot come back,
/// because it is greater than every pair the answer holds by then. An entry left by an earlier
/// vector therefore costs one row read and changes nothing.
///
/// It runs [`plan`]. Over partitions searched whole the answer is exactly the `k` least of
/// `(distance, key)` over the rows `distance_of` gives a distance for. Of a partition searched
/// by its nearest groups it screens the slots that the binary codes of the groups inside the
/// IVF's gap and of the tail keep, or all of them where the IVF's width is 0.
/// Blocks and binary codes are read in place from the page cache; everything that grows is taken
/// from `reservation` first. When the answer is returned, `reservation` holds 32 bytes and
/// the key's for each of its rows and nothing else of the search but what [`plan`] took.
///
/// A row is turned away when `distance_of` gives no distance for it. While the answer is short
/// of `k` rows that were not turned away, the search takes four times as many binary codes from
/// the groups it has, then twice as many groups. When it holds `k`, it widens each gap by how
/// far its last row lies below the `k`-th best slot screened, and keeps every binary code that
/// reaches that row. Having kept every binary code of every group it has done the work of EXACT and
/// returns what there is; under INDEXED with `nearest.filtered` it answers `None` at the step
/// that would keep them all. With `None` the caller lets go of what `reservation` took.
/// `targets` are what [`plan`] gave for `nearest`, and `reservation` holds what it took and
/// nothing else.
///
/// The caller may retry index damage as EXACT with its candidate set restored.
pub(crate) fn search(
    nearest: &Nearest<'_>,
    read: &dyn Read,
    reservation: &mut Reservation,
    targets: &mut [Planned],
    distance_of: &mut dyn FnMut(&Key) -> Result<Option<f64>>,
    (limit, evaluations): (Option<u64>, &Cell<u64>),
) -> Result<Option<Outcome>> {
    let work = Work {
        evaluations,
        limit: Cell::new(limit.unwrap_or(u64::MAX)),
    };
    answer(nearest, read, reservation, targets, distance_of, &work)
        .inspect_err(|_| READING.set(None))
}

/// [`search`], in the mode it is given.
fn answer(
    nearest: &Nearest<'_>,
    read: &dyn Read,
    reservation: &mut Reservation,
    targets: &mut [Planned],
    distance_of: &mut dyn FnMut(&Key) -> Result<Option<f64>>,
    work: &Work<'_>,
) -> Result<Option<Outcome>> {
    if nearest.k == 0 {
        return Ok(Some(Outcome {
            hits: Vec::new(),
            stopped: false,
        }));
    }
    let before = reservation.bytes();
    let indexed = nearest.mode == VectorSearchMode::Indexed;
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
        lows: Best::new(nearest.k),
        reservation,
        distance_of,
        threshold: f64::NEG_INFINITY,
        rises: true,
        kept: Kept::default(),
        marks: BTreeMap::new(),

        refused: indexed && nearest.filtered,
        ranked: nearest.ranked,
        every: false,
        work,
        bound: nearest.bound.unwrap_or(f64::NEG_INFINITY),
        cut: false,
    };

    // The rows that are in no block, exactly: those the reader has staged, then the pending
    // rows of each partition, which end where the records of the next partition begin.
    read.staged(nearest.table, &mut |key| found.score(key).map(|()| true))?;
    // With fewer than `k` rows left to ask, the answer stays short: a search that is refused
    // then is refused here, before any of them is asked.
    let live: u64 = targets.iter().map(|target| target.partition.live).sum();
    let possible = (found.answer.len() as u64).saturating_add(live);
    if found.refused && nearest.bound.is_none() && possible < nearest.k as u64 {
        return Ok(None);
    }
    let first = Key::from_values(&[], &[])?;
    for target in &*targets {
        let run = (nearest.vector, target.partition.number);
        let from = VectorRecord::Pending(run.0, run.1, &first);
        read.vectors(from, &mut |record, _| match record {
            VectorRecord::Pending(vector, number, key) if (vector, number) == run => {
                found.score(key).map(|()| true)
            }
            _ => Ok(false),
        })?;
    }

    // The slots. A pass leaves out every slot whose `high` is below the threshold its screen
    // ended with, and every binary code whose high end is below the one its binary codes ended with.
    // So when the last row of a full answer is at least as similar as both, and no group left
    // out lies within the gap widened by the fall, nothing left out could have entered.
    // Otherwise rows were turned away or counted twice, and a further pass goes on from there.
    let mut most = nearest.k;
    let mut ended = found.pass(&asked, targets, &mut wide, None, None, 0.0)?;
    loop {
        if !work.fits(1) {
            break;
        }
        // Short of `k` rows, a bound that cut something stands in for the last row.
        let (floor, codes, fall) = match found.last().or(nearest.bound.filter(|_| found.cut)) {
            // Short of `k` rows: four times the binary codes, and twice the groups once every
            // binary code of the groups taken was kept.
            None => {
                most = most.max(ended.kept).saturating_mul(4);
                let mut all = most as u64 >= targets.iter().map(Planned::codes).sum::<u64>();
                for target in &mut *targets {
                    target.taken = target.taken.saturating_mul(if ended.all { 2 } else { 1 });
                    let ivf = target.ivf.as_ref();
                    all &= ivf.is_none_or(|ivf| target.taken >= ivf.starts.len() - 1);
                }
                // A filter on rows left the index nothing to choose among: the pass in hand
                // kept everything there is, or the next one would.
                if found.refused && (ended.whole || all) {
                    return Ok(None);
                }
                if ended.whole {
                    break;
                }
                (f64::NEG_INFINITY, Some(most), 0.0)
            }
            Some(last) => {
                // A row a fall below the nearest can lie in a group whose centroid is four times
                // as far below the best centroid's: twice the angle from the vector of the
                // search. The cover takes the groups that far out.
                let fall = 4.0 * (found.lows.least() - last).max(0.0);
                let nearer = |t: &Planned| found.declared.is_none() && t.next <= fall * 10_000.0;
                let proven = last >= ended.threshold && last >= ended.codes;
                if proven && !targets.iter().any(nearer) {
                    break;
                }
                (last, None, fall)
            }
        };
        ended = found.pass(&asked, targets, &mut wide, Some(floor), codes, fall)?;
    }

    let pairs = found.answer.iter();
    let pairs = pairs.map(|(_, key)| PAIR + key.as_bytes().len() as u64);
    let held = before + pairs.sum::<u64>();
    found.reservation.shrink(found.reservation.bytes() - held);
    let hits = found.answer.into_iter();
    let hits = hits.map(|(Order(distance), key)| Hit { key, distance });
    Ok(Some(Outcome {
        hits: hits.collect(),
        stopped: !work.fits(1),
    }))
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

/// The greatest `most` of the numbers offered, the least of them on top.
struct Best {
    most: usize,
    held: BinaryHeap<Reverse<Order>>,
}

impl Best {
    fn new(most: usize) -> Best {
        let held = BinaryHeap::new();
        Best { most, held }
    }

    /// Offers a number. Eight bytes are taken for each place the list has, before it is made
    /// again with as many more.
    fn offer(&mut self, number: f64, reservation: &mut Reservation) -> Result<()> {
        if self.held.len() < self.most {
            if self.held.len() == self.held.capacity() {
                let more = self.held.capacity().max(4).min(self.most - self.held.len());
                reservation.grow(LOW * (self.held.len() + more) as u64)?;
                self.held.reserve_exact(more);
                reservation.shrink(LOW * self.held.len() as u64);
            }
            self.held.push(Reverse(Order(number)));
        } else if let Some(mut least) = self.held.peek_mut()
            && Order(number) > least.0
        {
            *least = Reverse(Order(number));
        }
        Ok(())
    }

    /// The least of `most` numbers, and nothing while there are fewer.
    fn least(&self) -> f64 {
        let least = self.held.peek().filter(|_| self.held.len() == self.most);
        least.map_or(f64::NEG_INFINITY, |Reverse(Order(number))| *number)
    }
}

/// How a pass ended: what the `high` of a slot had to reach when the screen ended and the
/// high end of a binary code when the binary codes did; how many binary codes were kept and whether all
/// it estimated were; and whether every binary code of every group and every slot screened was.
struct Ended {
    threshold: f64,
    codes: f64,
    kept: usize,
    all: bool,
    whole: bool,
}

/// What a search has found so far.
struct Work<'a> {
    evaluations: &'a Cell<u64>,
    limit: Cell<u64>,
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

struct Found<'a> {
    k: usize,
    declared: Option<u32>,
    /// The answer so far: the `k` least pairs of distance and key among the rows scored.
    answer: BTreeSet<(Order, Key)>,
    /// The `k` greatest similarities proven so far to be reached by a row or by a slot's
    /// vector. With `k` of them, a slot whose `high` is below the least cannot be among the
    /// `k` nearest of the vectors seen.
    lows: Best,
    reservation: &'a mut Reservation,
    distance_of: &'a mut dyn FnMut(&Key) -> Result<Option<f64>>,
    /// What the `high` of a slot must reach in the pass in hand, and whether that rises with
    /// the lower bounds, as it does in the first pass alone.
    threshold: f64,
    rises: bool,
    /// The slots the pass in hand keeps.
    kept: Kept,
    /// One bit a slot of each block an earlier pass kept a slot of, by the number the block
    /// is kept under and its own: set for the slots those passes kept, which are not screened
    /// and whose rows are not asked for again.
    marks: BTreeMap<(u32, u32), Vec<u64>>,
    work: &'a Work<'a>,

    /// Whether the search is refused when it runs out of binary codes to choose among: the mode
    /// is INDEXED and the statement has a filter on rows.
    refused: bool,
    /// Whether a rank orders the answer, and whether every slot of the partition in hand
    /// has its row's distance asked for without screening.
    ranked: bool,
    every: bool,
    /// The least likeness a row may have, and whether it has cut a row, a slot or a binary code.
    bound: f64,
    cut: bool,
}

impl Found<'_> {
    /// Asks for the distance of one row and offers it to the answer, and its similarity to
    /// the lower bounds when the answer took it.
    fn score(&mut self, key: &Key) -> Result<()> {
        if self.work.fits(1)
            && let Some(distance) = (self.distance_of)(key)?
            && self.offer(distance, key)?
        {
            self.lows.offer(1.0 - distance, self.reservation)?;
        }
        Ok(())
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

    /// Screens one slot of a block kept under `number`, unless an earlier pass kept it. The
    /// slot is kept, with its `high`, its place and a copy of its key, unless its `high` is
    /// below the threshold, and then offers its `low` to the lower bounds.
    fn look(
        &mut self,
        asked: &Asked<'_>,
        number: u32,
        at: Place,
        block: &BlockReader<'_>,
    ) -> Result<()> {
        let slot = at.slot as usize;
        if block.key(slot).as_bytes().is_empty() {
            return Ok(());
        }
        let bits = self.marks.get(&(number, at.block));
        let word = bits.and_then(|bits| bits.get(slot / 64));
        if word.is_some_and(|word| word >> (slot % 64) & 1 == 1) {
            return Ok(());
        }
        if self.every {
            self.score(&block.key(slot))?;
            return self.mark(number, at, asked.dimension);
        }
        if !self.work.fits(1) {
            return Ok(());
        }
        asked.count.map(|c| c(1)).transpose()?;
        let (low, high) = asked.screen.range(block.slot(slot));
        if f64::from(high) < self.threshold.max(self.bound) {
            self.cut |= f64::from(high) < self.bound;
            return Ok(());
        }
        let key = block.key(slot);
        self.kept
            .keep(high, (number, at), &key, self.threshold, self.reservation)?;
        self.lows.offer(f64::from(low), self.reservation)?;
        if self.rises {
            self.threshold = self.lows.least();
        }
        Ok(())
    }

    /// Screens every slot from one place up to another of the blocks kept under `number`.
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
                    let slot = slot as u32;
                    self.look(asked, number, Place { block: at, slot }, block)?;
                }
                Ok(())
            })?;
            if !self.work.fits(1) {
                break;
            }
        }
        Ok(())
    }

    /// Screens the slots of the binary codes kept among the blocks under `number`, and those
    /// only, and empties the list. A block is read here for the first time, so here its
    /// binary codes are found to be one for each of its slots, or damage.
    fn screen_kept(
        &mut self,
        asked: &Asked<'_>,
        p: &Partition,
        n: u32,
        chosen: &mut Chosen,
    ) -> Result<()> {
        let threshold = chosen.threshold();
        chosen
            .held
            .retain(|(high, _)| f64::from(*high) >= threshold);
        chosen.kept += chosen.held.len();
        for places in chosen
            .held
            .chunk_by(|one, next| one.1.block == next.1.block)
        {
            let record = VectorRecord::Block(asked.vector, n, places[0].1.block);
            let mut look = |b: &BlockReader<'_>| {
                places
                    .iter()
                    .try_for_each(|(_, at)| self.look(asked, n, *at, b))
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
        chosen.held.clear();
        Ok(())
    }

    /// Marks a slot already visited, charging its block's bits before allocation.
    fn mark(&mut self, number: u32, at: Place, dimension: usize) -> Result<()> {
        let words = (BLOCK_BYTES / slot_len(dimension)).div_ceil(64).max(1);
        let bits = match self.marks.entry((number, at.block)) {
            Entry::Occupied(bits) => bits.into_mut(),
            Entry::Vacant(none) => {
                // The bits of the block, and 64 bytes for its place in the list.
                self.reservation.grow(8 * words as u64 + 64)?;
                none.insert(vec![0_u64; words])
            }
        };
        if let Some(word) = bits.get_mut(at.slot as usize / 64) {
            *word |= 1 << (at.slot % 64);
        }
        Ok(())
    }

    /// One pass over the slots of the partitions, and the visit of the slots it keeps.
    ///
    /// A slot is kept unless its `high` is below the threshold. With no `floor` the threshold
    /// is the least of `k` lower bounds and rises as kept slots offer their `low`; with one
    /// it is the floor throughout. Kept slots are visited by `high`, greatest first, and the
    /// visit ends at the first whose `high` is below the similarity of the last row of a full
    /// answer: neither it nor any after it can enter, while one that equals it may, on its key.
    ///
    /// Every slot of a partition searched whole is screened. Of one searched by its nearest
    /// groups: the slots of its tail and of the groups within its gap and `fall` of the
    /// nearest, and of as many groups as it was told to take. Where the IVF has a width
    /// those slots are first chosen by their binary codes: with no `floor`, every binary code whose
    /// high end reaches the `k`-th best low end so far; with `most`, the `most` with the best
    /// high ends; else every one whose high end reaches the floor. The slots the pass before
    /// kept are marked first, and none of them is screened, or its row asked for, again.
    fn pass(
        &mut self,
        asked: &Asked<'_>,
        targets: &mut [Planned],
        wide: &mut Option<Wide>,
        floor: Option<f64>,
        most: Option<usize>,
        fall: f64,
    ) -> Result<Ended> {
        for at in 0..self.kept.slots.len() {
            let slot = &self.kept.slots[at];
            self.mark(slot.number, slot.at, asked.dimension)?;
        }
        self.kept.clear();
        self.threshold = floor.unwrap_or_else(|| self.lows.least());
        self.rises = floor.is_none();
        let mut chosen = Chosen {
            ends: Best::new(most.unwrap_or(self.k)),
            by_high: most.is_some(),
            fixed: floor.filter(|_| most.is_none()),
            held: Vec::new(),
            room: 0,
            estimated: 0,
            kept: 0,
            bound: self.bound,
            cut: false,
        };
        for target in targets.iter_mut() {
            if !self.work.fits(1) {
                break;
            }
            let partition = target.partition;
            READING.set(partition.ivf.map(|grouped| (asked.vector, grouped.number)));
            self.every = (self.ranked && target.route == Route::Exact)
                || (!self.refused && partition.ivf.is_none() && target.live <= self.k as u64);
            let tail = first_of(partition.first)..first_of(partition.blocks);
            let (Some(ivf), Some(grouped)) = (&target.ivf, partition.ivf) else {
                if let Some(grouped) = partition.ivf {
                    let every = Place::default()..first_of(grouped.blocks);
                    self.between(asked, grouped.number, every)?;
                }
                self.between(asked, partition.number, tail)?;
                continue;
            };
            let unit = asked.screen.unit();
            if !self.work.fits(u64::from(grouped.groups)) {
                break;
            }
            let mut ranked = ivf::nearest(
                asked.read,
                asked.vector,
                grouped,
                unit,
                self.reservation,
                asked.count,
            )?;
            ranked.retain(|(group, _)| ivf.nonempty(*group));
            if ranked.is_empty() {
                self.between(asked, partition.number, tail)?;
                target.next = f64::INFINITY;
                continue;
            }
            let (nearest, gap) = (ranked[0].1, f64::from(ivf.gap));
            let take = self
                .declared
                .map_or_else(|| within(&ranked, gap, fall), |n| n as usize)
                .max(target.taken)
                .min(ranked.len());
            let next = ranked
                .get(take)
                .map(|(_, product)| apart(nearest, *product) - gap);
            (target.taken, target.next) = (take, next.unwrap_or(f64::INFINITY));
            ranked.truncate(take);
            ranked.sort_unstable_by_key(|(group, _)| *group);
            let spans = ranked.iter().map(|(group, base)| {
                let group = *group as usize;
                (ivf.starts[group]..ivf.starts[group + 1], *base)
            });
            match (ivf.width, wide.as_mut()) {
                (0, _) | (_, None) => {
                    for (places, _) in spans {
                        self.between(asked, grouped.number, places)?;
                    }
                    self.between(asked, partition.number, tail)?;
                }
                (width, Some(wide)) => {
                    if !self.work.fits(1) {
                        self.reservation.shrink(8 * u64::from(grouped.groups));
                        break;
                    }
                    let (read, vector, n) = (asked.read, asked.vector, asked.dimension);
                    let mean = {
                        let mean = ivf::mean(read, vector, grouped.number, n, self.reservation)?;
                        asked.count.map(|c| c(1)).transpose()?;
                        summed(&mean, unit, |number| number)
                    };
                    self.reservation.shrink(4 * n as u64);

                    let each =
                        &mut |place, low, high| chosen.offer(place, low, high, self.reservation);
                    for (places, base) in spans {
                        wide.span(
                            asked,
                            grouped.number,
                            places,
                            (base, width),
                            each,
                            Some(self.work),
                        )?;
                    }
                    self.screen_kept(asked, &partition, grouped.number, &mut chosen)?;
                    // A tail's binary codes are held against the low end the groups left, as the
                    // build measured them: none of them raises it for the next.
                    let rising = chosen.fixed.is_none() && !chosen.by_high;
                    if rising {
                        chosen.fixed = Some(chosen.threshold());
                    }
                    let each =
                        &mut |place, low, high| chosen.offer(place, low, high, self.reservation);
                    wide.span(
                        asked,
                        partition.number,
                        tail,
                        (mean, width),
                        each,
                        Some(self.work),
                    )?;
                    self.screen_kept(asked, &partition, partition.number, &mut chosen)?;
                    if rising {
                        chosen.fixed = None;
                    }
                }
            }
            self.reservation.shrink(8 * u64::from(grouped.groups));
        }
        READING.set(None);
        let every = targets.iter().all(|target| target.next == f64::INFINITY);
        let all = chosen.kept == chosen.estimated;
        self.cut |= chosen.cut;
        let ended = Ended {
            threshold: self.threshold,
            codes: chosen.threshold(),
            kept: chosen.kept,
            all,
            whole: all && every && self.threshold == f64::NEG_INFINITY && !self.cut,
        };
        // The lists of the binary codes are let go before their bytes are given back.
        let places = chosen.ends.held.capacity() as u64;
        let chosen_bytes = (chosen.room * size_of::<(f32, Place)>()) as u64;
        drop(chosen);
        self.reservation.shrink(LOW * places + chosen_bytes);
        // The pass kept every slot there is and the answer cannot reach `k` rows: a search with
        // no bound that is refused then is refused here, before the filter is asked of every row.
        if !self.work.fits(1) || (ended.whole && self.refused && self.bound == f64::NEG_INFINITY) {
            return Ok(ended);
        }

        self.kept.drop_below(self.threshold);
        let by_high = |a: &Slot, b: &Slot| b.high.total_cmp(&a.high).then(a.start.cmp(&b.start));
        self.kept.slots.sort_unstable_by(by_high);
        let kept = std::mem::take(&mut self.kept);
        for slot in &kept.slots {
            if self.last().is_some_and(|last| f64::from(slot.high) < last) {
                break;
            }
            // The slot's low end is among the lower bounds already: its row's similarity is
            // not offered beside it, or one row would count twice and the fall never be zero.
            let key = Key::from_bytes(&kept.keys[slot.start..][..slot.len as usize]);
            if self.work.fits(1)
                && let Some(distance) = (self.distance_of)(&key)?
            {
                self.offer(distance, &key)?;
            }
        }
        self.kept = kept;
        Ok(ended)
    }
}

/// The binary codes one pass keeps: the high end of each and its place, in the order of the
/// places, twelve bytes each.
struct Chosen {
    /// With no fixed threshold: the `k` best low ends so far, or the best high ends so far
    /// of a pass that keeps the binary codes with the best high ends.
    ends: Best,
    by_high: bool,
    /// What a high end must reach in a pass that keeps every binary code reaching one similarity.
    fixed: Option<f64>,
    held: Vec<(f32, Place)>,
    /// How many binary codes the reservation holds room for.
    room: usize,
    /// How many binary codes were offered, and how many of them were kept in the end.
    estimated: usize,
    kept: usize,
    /// The search's bound, and whether it cut a binary code.
    bound: f64,
    cut: bool,
}

impl Chosen {
    /// What the high end of a binary code must reach to stay in the running.
    fn threshold(&self) -> f64 {
        self.fixed.unwrap_or(self.ends.least()).max(self.bound)
    }

    /// Offers the range of one binary code. It is kept unless its high end is below the
    /// threshold; when the list is full, those now below the threshold are dropped first.
    fn offer(
        &mut self,
        place: Place,
        low: f32,
        high: f32,
        reservation: &mut Reservation,
    ) -> Result<()> {
        self.estimated += 1;
        let threshold = self.threshold();
        if f64::from(high) < threshold {
            self.cut |= f64::from(high) < self.bound;
            return Ok(());
        }
        if self.held.len() == self.room {
            self.held.retain(|(high, _)| f64::from(*high) >= threshold);
            widen(&mut self.held, &mut self.room, reservation)?;
        }
        self.held.push((high, place));
        if self.fixed.is_none() {
            let end = if self.by_high { high } else { low };
            self.ends.offer(f64::from(end), reservation)?;
        }
        Ok(())
    }
}

/// Makes room in a list that is full after it dropped what it could: for 1,024 entries at
/// first, and for as many again only if more than half are left.
fn widen<T>(list: &mut Vec<T>, room: &mut usize, reservation: &mut Reservation) -> Result<()> {
    reservation.room(list, usize::MAX)?;
    *room = list.capacity();
    Ok(())
}

/// A slot still in the running: the top of its range, the number its block is kept under,
/// its place, and where its key is in [`Kept::keys`].
struct Slot {
    high: f32,
    number: u32,
    at: Place,
    len: u32,
    start: usize,
}

/// The slots still in the running.
#[derive(Default)]
struct Kept {
    slots: Vec<Slot>,
    /// The keys of the slots, one after another.
    keys: Vec<u8>,
    /// How many slots, and how many bytes of keys, the reservation holds room for.
    room: usize,
    key_room: usize,
}

impl Kept {
    /// Empties the list and keeps its room.
    fn clear(&mut self) {
        self.slots.clear();
        self.keys.clear();
    }

    /// Adds a slot. When the list is full, the slots now below the threshold are dropped
    /// first.
    fn keep(
        &mut self,
        high: f32,
        (number, at): (u32, Place),
        key: &Key,
        threshold: f64,
        reservation: &mut Reservation,
    ) -> Result<()> {
        if self.slots.len() == self.room {
            self.drop_below(threshold);
            widen(&mut self.slots, &mut self.room, reservation)?;
        }
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
        self.slots.push(Slot {
            high,
            number,
            at,
            len,
            start,
        });
        Ok(())
    }

    /// Drops the slots whose `high` is below the threshold and closes the gaps their keys
    /// leave. The slots are in the order they were kept in, the order their keys lie in, so
    /// a key that stays only ever moves towards the start.
    fn drop_below(&mut self, threshold: f64) {
        let (keys, mut end) = (&mut self.keys, 0);
        self.slots.retain_mut(|slot| {
            let stays = f64::from(slot.high) >= threshold;
            if stays {
                keys.copy_within(slot.start..slot.start + slot.len as usize, end);
                slot.start = end;
                end += slot.len as usize;
            }
            stays
        });
        keys.truncate(end);
    }
}
