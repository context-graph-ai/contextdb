//! The one search: score the rows that are in no block exactly, screen the slots of the
//! selected partitions (every slot of a partition searched whole; of one searched by its
//! nearest groups, the slots its sketches keep), ask for the true distance of the rows whose
//! range can reach the answer, and prove the answer before returning it.

use std::cmp::{Ordering, Reverse};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::ops::Range;

use super::block::{BLOCK_BYTES, BROKEN, BlockReader};
use super::build::{pending, usable};
use super::directory::{self, Grouped, Partition, kept};
use super::grouping::{self, Grouping, Place};
use super::sketch::{Estimate, Sketches, WIDEST, sketch_len, write_sketch};
use super::slot::{SLOT_HEADER, Screen, product, slot_len, summed};
use super::train;
use crate::memory::Reservation;
use crate::schema::VectorColumn;
use crate::store::{Key, Read, TableId, VectorId, VectorRecord};
use crate::types::{Error, Result, VectorSearchMode};

/// What a search is asked for.
pub(crate) struct Nearest<'a> {
    /// The table of the column, for the rows a transaction has staged.
    pub table: TableId,
    pub vector: VectorId,
    /// What the column declares: `AUTO_INDEX_AT` and `CONSOLIDATION` decide what AUTO runs
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
    /// under INDEXED that runs out of sketches to choose among is refused.
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
    /// How many centres it compared with the vector of the search, how many sketches it
    /// estimated and how many slots it screened, together. The rows whose distance it asked
    /// for are `query`'s to count.
    pub evaluations: u64,
}

/// How one partition of a search is searched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Route {
    /// Every slot of it.
    Exact,
    /// The sketches of its nearest groups and of its tail, and the slots of those whose
    /// estimate can still reach the answer.
    Indexed,
}

/// What a search does with one of the partitions it selects, settled from the partition's
/// small records before a block is read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Planned {
    /// The partition's key tuple.
    pub tuple: Key,
    /// Rows with a vector in it, and those of them that are pending.
    pub live: u64,
    pub pending: u64,
    /// Slots in its tail blocks, outside its grouping.
    pub tail: u64,
    /// The groups of its grouping and the most of them a search scanned for its build's own
    /// questions; zero and zero when it has none.
    pub groups: u32,
    pub probes: u32,
    pub route: Route,
    /// Whether INDEXED may be given this partition. A search under INDEXED with a partition
    /// that is not usable is refused whole.
    pub usable: bool,
}

/// One selected partition as a search goes over it.
struct Target {
    partition: Partition,
    /// What the plan says of it.
    planned: Planned,
    /// The record of its grouping, read when its nearest groups are searched and it has more
    /// than one group.
    grouping: Option<Grouping>,
    /// How many of its groups the last pass took, nearest first, and how far in
    /// ten-thousandths the next one lies past its gap: infinitely when it took them all.
    taken: usize,
    next: f64,
}

impl Target {
    /// How many sketches a search reads of it at most: one for each slot, and none of a
    /// partition whose slots are screened with no sketch read.
    fn sketches(&self) -> u64 {
        let grouped = self.partition.grouping.map_or(0, |grouped| grouped.slots);
        match &self.grouping {
            Some(grouping) if grouping.width != 0 => self.partition.slots + grouped,
            _ => 0,
        }
    }
}

/// The selected partitions in the order of their numbers, each with its pending rows counted
/// and its route settled. [`directory::HELD`], its key tuple and the bytes of a `Target` are
/// taken from `reservation` for each.
fn targets(
    nearest: &Nearest<'_>,
    read: &dyn Read,
    reservation: &mut Reservation,
) -> Result<Vec<Target>> {
    let partitions = directory::selected(read, nearest.vector, nearest.scope, reservation)?;
    reservation.grow((partitions.len() * size_of::<Target>()) as u64)?;
    let live: u64 = partitions.iter().map(|(_, partition)| partition.live).sum();
    let mode = match nearest.mode {
        VectorSearchMode::Auto if live < nearest.column.index_at() => VectorSearchMode::Exact,
        mode => mode,
    };
    let mut targets = Vec::with_capacity(partitions.len());
    // The places of a grouping's record, eight bytes a group and one more, are taken first.
    let mut record_of = |grouped: Grouped| {
        reservation.grow(8 * (u64::from(grouped.groups) + 1))?;
        Grouping::read(read, nearest.vector, grouped)
    };
    for (tuple, partition) in partitions {
        let pending = pending(read, nearest.vector, partition.number)?;
        let usable = usable(&partition, pending, nearest.column);
        let grouped = partition.grouping.filter(|grouped| grouped.groups > 1);
        // AUTO searches a partition whose grouping record is damaged exactly.
        let grouping = match (mode, grouped) {
            (VectorSearchMode::Indexed, Some(grouped)) => Some(record_of(grouped)?),
            (VectorSearchMode::Auto, Some(grouped)) if usable => match record_of(grouped) {
                Err(Error::StoreCorrupted { .. }) => None,
                found => Some(found?),
            },
            _ => None,
        };
        let indexed = mode == VectorSearchMode::Indexed || grouping.is_some();
        let route = if indexed {
            Route::Indexed
        } else {
            Route::Exact
        };
        let planned = Planned {
            tuple,
            live: partition.live,
            pending,
            tail: partition.slots,
            groups: partition.grouping.map_or(0, |grouped| grouped.groups),
            probes: grouping.as_ref().map_or(0, |grouping| grouping.probes),
            route,
            usable,
        };
        targets.push(Target {
            partition,
            planned,
            grouping,
            taken: 0,
            next: f64::INFINITY,
        });
    }
    Ok(targets)
}

/// What a search would do with each partition it selects, in the order of their numbers:
/// the one plan [`search`] runs and `.explain` prints. EXACT searches every slot of every
/// one. INDEXED searches the nearest groups of every one, which for a partition with no
/// grouping or a grouping of one group is every slot, through the slot screen alone. AUTO
/// searches every slot of every one while the rows with a vector in all of them together are
/// fewer than the column's `AUTO_INDEX_AT`, and from there on the nearest groups of each
/// partition that has a grouping and is usable, and every slot of the others. A filter on
/// rows changes no route: it is asked of each row as the search visits it.
///
/// Only partition, pending and grouping records are read; EXACT reads no grouping record and
/// answers no `probes`. What the list takes is taken from `reservation` first and stays taken
/// until the caller lets it go.
pub(crate) fn plan(
    nearest: &Nearest<'_>,
    read: &dyn Read,
    reservation: &mut Reservation,
) -> Result<Vec<Planned>> {
    let targets = targets(nearest, read, reservation)?;
    Ok(targets.into_iter().map(|target| target.planned).collect())
}

/// What a search reads blocks and sketches with.
struct Asked<'a> {
    read: &'a dyn Read,
    vector: VectorId,
    dimension: usize,
    screen: &'a Screen,
}

/// What a store is told when the sketches of a block are missing or are not one for each of
/// the block's slots.
const SKETCHES: &str = "the sketches of a block of the vector part are not one for each slot";

/// Hands `each` the blocks kept under `number` from block `from` on, each with its own
/// number, while it answers true. A block that is not whole is damage.
fn blocks(
    asked: &Asked<'_>,
    number: u32,
    from: u32,
    each: &mut dyn FnMut(u32, &BlockReader<'_>) -> Result<bool>,
) -> Result<()> {
    let run = (asked.vector, number);
    let from = VectorRecord::Block(run.0, run.1, from);
    asked.read.vectors(from, &mut |record, bytes| match record {
        VectorRecord::Block(vector, number, at) if (vector, number) == run => {
            let block = BlockReader::new(bytes, asked.dimension);
            if !block.is_whole() {
                return Err(asked.read.damaged(BROKEN));
            }
            each(at, &block)
        }
        _ => Ok(false),
    })
}

/// Hands `each` the records of sketches of the blocks kept under `number` from block `from`
/// on, each with its block's number and how many sketches it holds, while it answers true.
/// A record it asks for that is not there, or is not whole, is damage.
fn sketches(
    asked: &Asked<'_>,
    number: u32,
    from: u32,
    each: &mut dyn FnMut(u32, &Sketches<'_>, usize) -> Result<bool>,
) -> Result<()> {
    let (mut at, mut more) = (from, true);
    let first = VectorRecord::Sketch(asked.vector, number, from);
    asked.read.vectors(first, &mut |record, bytes| {
        let sketches = Sketches::new(bytes, asked.dimension);
        let count = bytes.len().saturating_sub(4) / sketch_len(asked.dimension);
        if record != VectorRecord::Sketch(asked.vector, number, at) || !sketches.is_whole(count) {
            return Err(asked.read.damaged(SKETCHES));
        }
        more = each(at, &sketches, count)?;
        at += 1;
        Ok(more)
    })?;
    if more {
        return Err(asked.read.damaged(SKETCHES));
    }
    Ok(())
}

/// How far in ten-thousandths of a product a group lies from the nearest group, by the
/// products of their centres with the vector of a search; held to 20,000.
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

/// What sketches are estimated with: what the vector of the search gives once, and room for
/// the ranges of one block's sketches.
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

    /// Hands `each` the range of every sketch between two places of the blocks kept under
    /// `number`, with its place: `width` quarters of the sketch's factor wide around `base`,
    /// the product of the sketches' reference with the vector of the search.
    fn span(
        &mut self,
        asked: &Asked<'_>,
        number: u32,
        places: Range<Place>,
        (base, width): (f32, u8),
        each: &mut dyn FnMut(Place, f32, f32) -> Result<()>,
    ) -> Result<()> {
        if places.start >= places.end {
            return Ok(());
        }
        let each = &mut |block, record: &Sketches<'_>, count| {
            self.ranges.clear();
            let slots = part(&places, block, count);
            let estimate = &self.estimate;
            estimate.ranges(record, slots.clone(), base, width, &mut self.ranges);
            for (slot, (low, high)) in slots.zip(&self.ranges) {
                let slot = slot as u32;
                each(Place { block, slot }, *low, *high)?;
            }
            Ok(first_of(block + 1) < places.end)
        };
        sketches(asked, number, places.start.block, each)
    }
}

/// How far from its nearest group a grouping has to be scanned for the ten slots nearest
/// `query`, and where those slots are. Nearest is by the product of the vector a slot keeps,
/// its codes times its scale, with `query` at length one, and equal products go by place: no
/// row is read. Every group is scanned. The gap is the best centre's product with `query`
/// less the least such product among the groups those slots are in, in ten-thousandths,
/// rounded up and held to 20,000; zero for a grouping of no slot, which has no place to give.
pub(super) fn gap_needed(
    read: &dyn Read,
    vector: VectorId,
    grouped: Grouped,
    grouping: &Grouping,
    query: &[f32],
    reservation: &mut Reservation,
) -> Result<(u32, Vec<Place>)> {
    let before = reservation.bytes();
    reservation.grow(4 * query.len() as u64)?;
    let screen = Screen::new(query);
    let asked = Asked {
        read,
        vector,
        dimension: query.len(),
        screen: &screen,
    };
    let mut ten: Vec<(f32, Place)> = Vec::with_capacity(11);
    blocks(&asked, grouped.number, 0, &mut |block, slots| {
        for slot in 0..slots.len() {
            let kept = slots.slot(slot);
            let scale = f32::from_le_bytes([kept[0], kept[1], kept[2], kept[3]]);
            let near = scale * product(&kept[SLOT_HEADER..], screen.unit());
            if ten.len() < 10 || near > ten[9].0 {
                let slot = slot as u32;
                ten.push((near, Place { block, slot }));
                // The sort keeps slots of one product in the order of their places.
                ten.sort_by(|one, other| other.0.total_cmp(&one.0));
                ten.truncate(10);
            }
        }
        Ok(true)
    })?;
    let ranked = grouping::nearest(read, vector, grouped, screen.unit(), reservation)?;
    let nearest = ranked[0].1;
    let of = |place: &Place| {
        let group = grouping.starts.partition_point(|start| start <= place) - 1;
        let found = ranked.iter().find(|(number, _)| *number as usize == group);
        found.map_or(nearest, |(_, product)| *product)
    };
    let least = ten.iter().map(|(_, place)| of(place));
    let least = least.fold(nearest, f32::min);
    reservation.shrink(reservation.bytes() - before);
    let places = ten.into_iter().map(|(_, place)| place);
    Ok((apart(nearest, least).ceil() as u32, places.collect()))
}

/// For each width from 2 to 16 quarters, how many of `wanted` (slots of the grouping's blocks,
/// the places [`gap_needed`] gave) a search keeps by its sketches, wherever the slot is kept:
/// the sketches of the groups inside `grouping.gap` are estimated, and a slot is kept at a
/// width when the range of its sketch at that width reaches the tenth best low end of those
/// ranges at that width, and so does the range of the sketch a tail block would keep of it,
/// which is made here from the slot's vector and `mean`, the partition's. A search estimates
/// a tail's sketches after the groups', against that same low end. With the counts, how many
/// groups were inside the gap: one at least, the nearest.
pub(super) fn kept_by_width(
    read: &dyn Read,
    vector: VectorId,
    grouped: Grouped,
    grouping: &Grouping,
    (query, mean): (&[f32], &[f32]),
    wanted: &[Place],
    reservation: &mut Reservation,
) -> Result<([u32; 15], u32)> {
    let before = reservation.bytes();
    let dimension = query.len();
    let found = wanted.len() * size_of::<(Place, f32, f32)>();
    reservation.grow((4 * dimension + found + 4 + sketch_len(dimension)) as u64)?;
    let screen = Screen::new(query);
    let asked = Asked {
        read,
        vector,
        dimension: query.len(),
        screen: &screen,
    };
    let mut wide = Wide::new(query, reservation)?;
    let mut ranked = grouping::nearest(read, vector, grouped, screen.unit(), reservation)?;
    let inside = within(&ranked, f64::from(grouping.gap), 0.0);
    ranked.truncate(inside);
    ranked.sort_unstable_by_key(|(group, _)| *group);
    // Each sketch is estimated once, at the widest: its middle and a quarter of its factor
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
        let places = grouping.starts[*group as usize]..grouping.starts[*group as usize + 1];
        wide.span(&asked, grouped.number, places, (*base, WIDEST), each)?;
    }
    // The slots found, block by block: each one's sketch from the mean, and whether both of
    // its sketches reach the low end at each width.
    let base = summed(mean, screen.unit(), |number| number);
    let mut sketch = vec![0; 4 + sketch_len(dimension)];
    let mut kept = [0; 15];
    for places in found.chunk_by(|one, next| one.0.block == next.0.block) {
        let mut whole = false;
        blocks(
            &asked,
            grouped.number,
            places[0].0.block,
            &mut |at, block| {
                whole = at == places[0].0.block
                    && places.iter().all(|slot| slot.0.slot < block.len() as u32);
                for (place, middle, quarter) in places.iter().filter(|_| whole) {
                    let slot = block.slot(place.slot as usize);
                    let own = train::product(screen.unit(), slot);
                    write_sketch(slot, mean, &mut sketch[4..]);
                    wide.ranges.clear();
                    let one = Sketches::new(&sketch, dimension);
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
                Ok(false)
            },
        )?;
        if !whole {
            return Err(read.damaged(BROKEN));
        }
    }
    reservation.shrink(reservation.bytes() - before);
    Ok((kept, inside as u32))
}

/// Bytes reserved for a row of the answer besides its key.
const PAIR: u64 = 32;

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
/// by its nearest groups it screens the slots that the sketches of the groups inside the
/// grouping's gap and of the tail keep, or all of them where the grouping's width is 0.
/// Blocks and sketches are read in place from the page cache; everything that grows is taken
/// from `reservation` first. When the answer is returned, `reservation` holds 32 bytes and
/// the key's for each of its rows and nothing else of the search.
///
/// A row is turned away when `distance_of` gives no distance for it. While the answer is short
/// of `k` rows that were not turned away, the search takes four times as many sketches from
/// the groups it has, then twice as many groups. When it holds `k`, it widens each gap by how
/// far its last row lies below the `k`-th best slot screened, and keeps every sketch that
/// reaches that row. Having kept every sketch of every group it has done the work of EXACT and
/// returns what there is; under INDEXED with `nearest.filtered` it answers `None` at the step
/// that would keep them all. It answers `None` too, having read only small records, when the
/// mode is INDEXED and a selected partition is not usable. With `None` the caller lets go of
/// what `reservation` took.
///
/// A record of the index that is damaged leaves the vectors whole. Under AUTO the search is
/// then run again as EXACT, which reads no such record and answers in full, or tells the
/// damage of a block; under INDEXED the damage is the answer. Nothing is rebuilt.
pub(crate) fn search(
    nearest: &Nearest<'_>,
    read: &dyn Read,
    reservation: &mut Reservation,
    distance_of: &mut dyn FnMut(&Key) -> Result<Option<f64>>,
) -> Result<Option<Outcome>> {
    let before = reservation.bytes();
    match answer(nearest, read, reservation, distance_of) {
        Err(Error::StoreCorrupted { .. }) if nearest.mode == VectorSearchMode::Auto => {
            reservation.shrink(reservation.bytes() - before);
            let mode = VectorSearchMode::Exact;
            answer(
                &Nearest { mode, ..*nearest },
                read,
                reservation,
                distance_of,
            )
        }
        other => other,
    }
}

/// [`search`], in the mode it is given.
fn answer(
    nearest: &Nearest<'_>,
    read: &dyn Read,
    reservation: &mut Reservation,
    distance_of: &mut dyn FnMut(&Key) -> Result<Option<f64>>,
) -> Result<Option<Outcome>> {
    if nearest.k == 0 {
        return Ok(Some(Outcome {
            hits: Vec::new(),
            evaluations: 0,
        }));
    }
    let before = reservation.bytes();
    let mut targets = targets(nearest, read, reservation)?;
    let indexed = nearest.mode == VectorSearchMode::Indexed;
    if indexed && targets.iter().any(|target| !target.planned.usable) {
        return Ok(None);
    }
    // The vector at length one that the screen keeps: four bytes a number.
    reservation.grow(4 * nearest.query.len() as u64)?;
    let screen = Screen::new(nearest.query);
    let asked = Asked {
        read,
        vector: nearest.vector,
        dimension: nearest.query.len(),
        screen: &screen,
    };
    let mut wide = None;
    if targets.iter().any(|target| target.sketches() > 0) {
        wide = Some(Wide::new(nearest.query, reservation)?);
    }
    let mut found = Found {
        k: nearest.k,
        answer: BTreeSet::new(),
        lows: Best::new(nearest.k),
        reservation,
        distance_of,
        threshold: f64::NEG_INFINITY,
        rises: true,
        kept: Kept::default(),
        marks: BTreeMap::new(),
        evaluations: 0,
        refused: indexed && nearest.filtered,
    };

    // The rows that are in no block, exactly: those the reader has staged, then the pending
    // rows of each partition, which end where the records of the next partition begin.
    read.staged(nearest.table, &mut |key| found.score(key).map(|()| true))?;
    // With fewer than `k` rows left to ask, the answer stays short: a search that is refused
    // then is refused here, before any of them is asked.
    let live: u64 = targets.iter().map(|target| target.partition.live).sum();
    if found.refused && (found.answer.len() as u64).saturating_add(live) < nearest.k as u64 {
        return Ok(None);
    }
    let first = Key::from_values(&[], &[])?;
    for target in &targets {
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
    // ended with, and every sketch whose high end is below the one its sketches ended with.
    // So when the last row of a full answer is at least as similar as both, and no group left
    // out lies within the gap widened by the fall, nothing left out could have entered.
    // Otherwise rows were turned away or counted twice, and a further pass goes on from there.
    let mut most = nearest.k;
    let mut ended = found.pass(&asked, &mut targets, &mut wide, None, None, 0.0)?;
    loop {
        let (floor, sketches, fall) = match found.last() {
            // Short of `k` rows: four times the sketches, and twice the groups once every
            // sketch of the groups taken was kept.
            None => {
                most = most.max(ended.kept).saturating_mul(4);
                let mut all = most as u64 >= targets.iter().map(Target::sketches).sum::<u64>();
                for target in &mut targets {
                    target.taken = target.taken.saturating_mul(if ended.all { 2 } else { 1 });
                    let grouping = target.grouping.as_ref();
                    all &=
                        grouping.is_none_or(|grouping| target.taken >= grouping.starts.len() - 1);
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
                // A row a fall below the nearest can lie in a group whose centre is four times
                // as far below the best centre's: twice the angle from the vector of the
                // search. The cover takes the groups that far out.
                let fall = 4.0 * (found.lows.least() - last).max(0.0);
                let nearer = |target: &Target| target.next <= fall * 10_000.0;
                let proven = last >= ended.threshold && last >= ended.sketches;
                if proven && !targets.iter().any(nearer) {
                    break;
                }
                (last, None, fall)
            }
        };
        ended = found.pass(&asked, &mut targets, &mut wide, Some(floor), sketches, fall)?;
    }

    let pairs = found.answer.iter();
    let pairs = pairs.map(|(_, key)| PAIR + key.as_bytes().len() as u64);
    let held = before + pairs.sum::<u64>();
    found.reservation.shrink(found.reservation.bytes() - held);
    let evaluations = found.evaluations;
    let hits = found.answer.into_iter();
    let hits = hits.map(|(Order(distance), key)| Hit { key, distance });
    Ok(Some(Outcome {
        hits: hits.collect(),
        evaluations,
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
/// high end of a sketch when the sketches did; how many sketches were kept and whether all
/// it estimated were; and whether every sketch of every group and every slot screened was.
struct Ended {
    threshold: f64,
    sketches: f64,
    kept: usize,
    all: bool,
    whole: bool,
}

/// What a search has found so far.
struct Found<'a> {
    k: usize,
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
    /// Centres compared, sketches estimated and slots screened.
    evaluations: u64,
    /// Whether the search is refused when it runs out of sketches to choose among: the mode
    /// is INDEXED and the statement has a filter on rows.
    refused: bool,
}

impl Found<'_> {
    /// Asks for the distance of one row and offers it to the answer, and its similarity to
    /// the lower bounds when the answer took it.
    fn score(&mut self, key: &Key) -> Result<()> {
        if let Some(distance) = (self.distance_of)(key)?
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
        let bits = self.marks.get(&(number, at.block));
        let word = bits.and_then(|bits| bits.get(slot / 64));
        if word.is_some_and(|word| word >> (slot % 64) & 1 == 1) {
            return Ok(());
        }
        self.evaluations += 1;
        let (low, high) = asked.screen.range(block.slot(slot));
        if f64::from(high) < self.threshold {
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
        blocks(asked, number, places.start.block, &mut |at, block| {
            for slot in part(&places, at, block.len()) {
                let slot = slot as u32;
                self.look(asked, number, Place { block: at, slot }, block)?;
            }
            Ok(first_of(at + 1) < places.end)
        })
    }

    /// Screens the slots of the sketches kept among the blocks under `number`, and those
    /// only, and empties the list. A block is read here for the first time, so here its
    /// sketches are found to be one for each of its slots, or damage.
    fn screen_kept(&mut self, asked: &Asked<'_>, number: u32, chosen: &mut Chosen) -> Result<()> {
        let threshold = chosen.threshold();
        chosen
            .held
            .retain(|(high, _)| f64::from(*high) >= threshold);
        chosen.kept += chosen.held.len();
        for places in chosen
            .held
            .chunk_by(|one, next| one.1.block == next.1.block)
        {
            let at = places[0].1.block;
            let record = VectorRecord::Sketch(asked.vector, number, at);
            let sketches = kept(asked.read, record, <[u8]>::len)?;
            let mut whole = false;
            blocks(asked, number, at, &mut |found, block| {
                let wanted = 4 + block.len() * sketch_len(asked.dimension);
                whole = found == at && sketches == Some(wanted);
                if whole {
                    for (_, place) in places {
                        self.look(asked, number, *place, block)?;
                    }
                }
                Ok(false)
            })?;
            if !whole {
                return Err(asked.read.damaged(SKETCHES));
            }
        }
        chosen.held.clear();
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
    /// nearest, and of as many groups as it was told to take. Where the grouping has a width
    /// those slots are first chosen by their sketches: with no `floor`, every sketch whose
    /// high end reaches the `k`-th best low end so far; with `most`, the `most` with the best
    /// high ends; else every one whose high end reaches the floor. The slots the pass before
    /// kept are marked first, and none of them is screened, or its row asked for, again.
    fn pass(
        &mut self,
        asked: &Asked<'_>,
        targets: &mut [Target],
        wide: &mut Option<Wide>,
        floor: Option<f64>,
        most: Option<usize>,
        fall: f64,
    ) -> Result<Ended> {
        let words = (BLOCK_BYTES / slot_len(asked.dimension))
            .div_ceil(64)
            .max(1);
        for slot in &self.kept.slots {
            let bits = match self.marks.entry((slot.number, slot.at.block)) {
                Entry::Occupied(bits) => bits.into_mut(),
                Entry::Vacant(none) => {
                    // The bits of the block, and 64 bytes for its place in the list.
                    self.reservation.grow(8 * words as u64 + 64)?;
                    none.insert(vec![0_u64; words])
                }
            };
            if let Some(word) = bits.get_mut(slot.at.slot as usize / 64) {
                *word |= 1 << (slot.at.slot % 64);
            }
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
        };
        for target in targets.iter_mut() {
            let partition = target.partition;
            let tail = first_of(partition.first)..first_of(partition.blocks);
            let (Some(grouping), Some(grouped)) = (&target.grouping, partition.grouping) else {
                if let Some(grouped) = partition.grouping {
                    let every = Place::default()..first_of(grouped.blocks);
                    self.between(asked, grouped.number, every)?;
                }
                self.between(asked, partition.number, tail)?;
                continue;
            };
            let unit = asked.screen.unit();
            let mut ranked =
                grouping::nearest(asked.read, asked.vector, grouped, unit, self.reservation)?;
            self.evaluations += ranked.len() as u64;
            let (nearest, gap) = (ranked[0].1, f64::from(grouping.gap));
            let take = within(&ranked, gap, fall).max(target.taken);
            let take = take.min(ranked.len());
            let next = ranked
                .get(take)
                .map(|(_, product)| apart(nearest, *product) - gap);
            (target.taken, target.next) = (take, next.unwrap_or(f64::INFINITY));
            ranked.truncate(take);
            ranked.sort_unstable_by_key(|(group, _)| *group);
            let spans = ranked.iter().map(|(group, base)| {
                let group = *group as usize;
                (grouping.starts[group]..grouping.starts[group + 1], *base)
            });
            match (grouping.width, wide.as_mut()) {
                (0, _) | (_, None) => {
                    for (places, _) in spans {
                        self.between(asked, grouped.number, places)?;
                    }
                    self.between(asked, partition.number, tail)?;
                }
                (width, Some(wide)) => {
                    let dimension = asked.dimension;
                    let mean = (asked.vector, partition.number);
                    let mean = {
                        let reservation = &mut *self.reservation;
                        let mean =
                            grouping::mean(asked.read, mean.0, mean.1, dimension, reservation)?;
                        summed(mean.as_slice(), unit, |number| number)
                    };
                    self.reservation.shrink(4 * dimension as u64);
                    self.evaluations += 1;
                    let each =
                        &mut |place, low, high| chosen.offer(place, low, high, self.reservation);
                    for (places, base) in spans {
                        wide.span(asked, grouped.number, places, (base, width), each)?;
                    }
                    self.screen_kept(asked, grouped.number, &mut chosen)?;
                    // A tail's sketches are held against the low end the groups left, as the
                    // build measured them: none of them raises it for the next.
                    let rising = chosen.fixed.is_none() && !chosen.by_high;
                    if rising {
                        chosen.fixed = Some(chosen.threshold());
                    }
                    let each =
                        &mut |place, low, high| chosen.offer(place, low, high, self.reservation);
                    wide.span(asked, partition.number, tail, (mean, width), each)?;
                    self.screen_kept(asked, partition.number, &mut chosen)?;
                    if rising {
                        chosen.fixed = None;
                    }
                }
            }
            self.reservation.shrink(8 * u64::from(grouped.groups));
        }
        self.evaluations += chosen.estimated as u64;
        let every = targets.iter().all(|target| target.next == f64::INFINITY);
        let all = chosen.kept == chosen.estimated;
        let ended = Ended {
            threshold: self.threshold,
            sketches: chosen.threshold(),
            kept: chosen.kept,
            all,
            whole: all && every && self.threshold == f64::NEG_INFINITY,
        };
        // The lists of the sketches are let go before their bytes are given back.
        let places = chosen.ends.held.capacity() as u64;
        let chosen_bytes = (chosen.room * size_of::<(f32, Place)>()) as u64;
        drop(chosen);
        self.reservation.shrink(LOW * places + chosen_bytes);
        // The pass kept every slot there is and the answer cannot reach `k` rows: a search
        // that is refused then is refused here, before the filter is asked of every row.
        if ended.whole && self.refused {
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
            if let Some(distance) = (self.distance_of)(&key)? {
                self.offer(distance, &key)?;
            }
        }
        self.kept = kept;
        Ok(ended)
    }
}

/// The sketches one pass keeps: the high end of each and its place, in the order of the
/// places, twelve bytes each.
struct Chosen {
    /// With no fixed threshold: the `k` best low ends so far, or the best high ends so far
    /// of a pass that keeps the sketches with the best high ends.
    ends: Best,
    by_high: bool,
    /// What a high end must reach in a pass that keeps every sketch reaching one similarity.
    fixed: Option<f64>,
    held: Vec<(f32, Place)>,
    /// How many sketches the reservation holds room for.
    room: usize,
    /// How many sketches were offered, and how many of them were kept in the end.
    estimated: usize,
    kept: usize,
}

impl Chosen {
    /// What the high end of a sketch must reach to stay in the running.
    fn threshold(&self) -> f64 {
        self.fixed.unwrap_or_else(|| self.ends.least())
    }

    /// Offers the range of one sketch. It is kept unless its high end is below the
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
    let more = match *room {
        0 => 1024,
        room if list.len() > room / 2 => room,
        _ => 0,
    };
    reservation.grow((more * size_of::<T>()) as u64)?;
    list.reserve_exact(*room + more - list.len());
    *room += more;
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
            reservation.grow(more as u64)?;
            self.keys.reserve_exact(self.key_room + more - start);
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
