//! The one search: score the rows that are in no block exactly, screen every slot of the
//! selected partitions, ask for the true distance of the rows whose range can reach the
//! answer, and prove the answer before returning it.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeSet, BinaryHeap};

use super::block::{BROKEN, BlockReader};
use super::directory::{self, Partition};
use super::slot::Screen;
use crate::memory::Reservation;
use crate::store::{Key, Read, TableId, VectorId, VectorRecord};
use crate::types::Result;

/// What a search is asked for.
pub(crate) struct Nearest<'a> {
    /// The table of the column, for the rows a transaction has staged.
    pub table: TableId,
    pub vector: VectorId,
    /// The partitions to search, as key tuples or the first values of key tuples; empty for
    /// every partition of the column.
    pub scope: &'a [Key],
    /// The vector to be near: of the column's dimension, every number finite.
    pub query: &'a [f32],
    /// How many rows to return at most.
    pub k: usize,
}

/// One row of an answer.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Hit {
    pub key: Key,
    /// The row's distance, as `distance_of` gave it.
    pub distance: f64,
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
/// The answer is exactly the `k` least of `(distance, key)` over the rows of the selected
/// partitions that `distance_of` gives a distance for. Blocks are read in place from the page
/// cache; the list of slots still in the running and the answer grow through `reservation`.
/// When the answer is returned, `reservation` holds 32 bytes and the key's for each of its
/// rows and nothing else of the search.
pub(crate) fn search(
    nearest: &Nearest<'_>,
    read: &dyn Read,
    reservation: &mut Reservation,
    distance_of: &mut dyn FnMut(&Key) -> Result<Option<f64>>,
) -> Result<Vec<Hit>> {
    if nearest.k == 0 {
        return Ok(Vec::new());
    }
    let partitions = directory::selected(read, nearest.vector, nearest.scope)?;
    // The vector at length one that the screen keeps: four bytes a number.
    let unit = 4 * nearest.query.len() as u64;
    reservation.grow(unit)?;
    let screen = Screen::new(nearest.query);
    let mut found = Found {
        k: nearest.k,
        answer: BTreeSet::new(),
        lows: BinaryHeap::new(),
        reservation,
        distance_of,
    };

    // The rows that are in no block, exactly: those the reader has staged, then the pending
    // rows of each partition, which end where the records of the next partition begin.
    read.staged(nearest.table, &mut |key| found.score(key).map(|()| true))?;
    let first = Key::from_values(&[], &[])?;
    for partition in &partitions {
        let run = (nearest.vector, partition.number);
        let from = VectorRecord::Pending(run.0, run.1, &first);
        read.vectors(from, &mut |record, _| match record {
            VectorRecord::Pending(vector, number, key) if (vector, number) == run => {
                found.score(key).map(|()| true)
            }
            _ => Ok(false),
        })?;
    }

    // The slots. Every slot the pass left out had a `high` below the threshold it ended with,
    // so when the last row of a full answer is at least that similar, no slot left out could
    // have entered, and when there never was a threshold none was left out. Otherwise a row
    // that is gone, filtered out or counted twice raised the threshold too far: one more pass
    // keeps every slot that reaches the last row found, or every slot when rows are missing.
    let mut kept = Kept::default();
    let threshold = found.pass(nearest, &partitions, read, &screen, &mut kept, None)?;
    let last = found.last();
    if threshold > f64::NEG_INFINITY && last.is_none_or(|last| last < threshold) {
        let floor = last.unwrap_or(f64::NEG_INFINITY);
        found.pass(nearest, &partitions, read, &screen, &mut kept, Some(floor))?;
    }

    let lows = LOW * found.lows.len() as u64;
    found.reservation.shrink(unit + kept.reserved() + lows);
    let hits = found.answer.into_iter();
    Ok(hits
        .map(|(Order(distance), key)| Hit { key, distance })
        .collect())
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

/// What a search has found so far.
struct Found<'a> {
    k: usize,
    /// The answer so far: the `k` least pairs of distance and key among the rows scored.
    answer: BTreeSet<(Order, Key)>,
    /// The `k` greatest similarities proven so far to be reached by a row or by a slot's
    /// vector, the least of them on top. With `k` of them, a slot whose `high` is below the
    /// least cannot be among the `k` nearest of the vectors seen.
    lows: BinaryHeap<Reverse<Order>>,
    reservation: &'a mut Reservation,
    distance_of: &'a mut dyn FnMut(&Key) -> Result<Option<f64>>,
}

impl Found<'_> {
    /// Asks for the distance of one row and offers it to the answer, and its similarity to
    /// the lower bounds when the answer took it.
    fn score(&mut self, key: &Key) -> Result<()> {
        if let Some(distance) = (self.distance_of)(key)?
            && self.offer(distance, key)?
        {
            self.offer_low(1.0 - distance)?;
        }
        Ok(())
    }

    /// Offers a row to the answer and says whether it went in. This is the one way in, and
    /// its one rule: a pair whose key the answer holds is dropped; any other goes in when the
    /// answer has room, or when it is less than the greatest pair held, which it replaces.
    ///
    /// A key always comes with the same distance, so the pair is what the answer is asked for.
    fn offer(&mut self, distance: f64, key: &Key) -> Result<bool> {
        let pair = (Order(distance), key.clone());
        if self.answer.contains(&pair) {
            return Ok(false);
        }
        let full = self.answer.len() == self.k;
        if full && self.answer.last().is_some_and(|last| pair > *last) {
            return Ok(false);
        }
        self.reservation.grow(PAIR + key.as_bytes().len() as u64)?;
        if full && let Some((_, out)) = self.answer.pop_last() {
            self.reservation.shrink(PAIR + out.as_bytes().len() as u64);
        }
        self.answer.insert(pair);
        Ok(true)
    }

    /// Offers a similarity that a row or a slot's vector is proven to reach.
    fn offer_low(&mut self, low: f64) -> Result<()> {
        if self.lows.len() < self.k {
            self.reservation.grow(LOW)?;
            self.lows.push(Reverse(Order(low)));
        } else if let Some(mut least) = self.lows.peek_mut()
            && Order(low) > least.0
        {
            *least = Reverse(Order(low));
        }
        Ok(())
    }

    /// What a slot's `high` must reach to stay in the running: the least of `k` lower bounds,
    /// and nothing while there are fewer.
    fn threshold(&self) -> f64 {
        let least = self.lows.peek().filter(|_| self.lows.len() == self.k);
        least.map_or(f64::NEG_INFINITY, |Reverse(Order(low))| *low)
    }

    /// The similarity of the last row of the answer once it has `k` rows.
    fn last(&self) -> Option<f64> {
        let last = self.answer.last().filter(|_| self.answer.len() == self.k);
        last.map(|(Order(distance), _)| 1.0 - distance)
    }

    /// Screens every slot of the partitions and visits the ones kept; returns the threshold
    /// the screen ended with.
    ///
    /// A slot is kept, with its `high` and a copy of its key, unless its `high` is below the
    /// threshold. With no `floor` the threshold is [`Found::threshold`] and rises as kept
    /// slots offer their `low`; with one it is the floor throughout. Kept slots are visited
    /// by `high`, greatest first, and the visit ends at the first whose `high` is below the
    /// similarity of the last row of a full answer: neither it nor any after it can enter,
    /// while one that equals it may, on its key.
    fn pass(
        &mut self,
        nearest: &Nearest<'_>,
        partitions: &[Partition],
        read: &dyn Read,
        screen: &Screen,
        kept: &mut Kept,
        floor: Option<f64>,
    ) -> Result<f64> {
        kept.clear();
        let mut threshold = floor.unwrap_or_else(|| self.threshold());
        for partition in partitions {
            let run = (nearest.vector, partition.number);
            let from = VectorRecord::Block(run.0, run.1, 0);
            read.vectors(from, &mut |record, bytes| match record {
                VectorRecord::Block(vector, number, _) if (vector, number) == run => {
                    let block = BlockReader::new(bytes, nearest.query.len());
                    if !block.is_whole() {
                        return Err(read.damaged(BROKEN));
                    }
                    for at in 0..block.len() {
                        let (low, high) = screen.range(block.slot(at));
                        if f64::from(high) < threshold {
                            continue;
                        }
                        kept.keep(high, &block.key(at), threshold, self.reservation)?;
                        if floor.is_none() {
                            self.offer_low(f64::from(low))?;
                            threshold = self.threshold();
                        }
                    }
                    Ok(true)
                }
                _ => Ok(false),
            })?;
        }
        kept.drop_below(threshold);
        let by_high = |a: &Slot, b: &Slot| b.high.total_cmp(&a.high).then(a.start.cmp(&b.start));
        kept.slots.sort_unstable_by(by_high);
        for slot in &kept.slots {
            if self.last().is_some_and(|last| f64::from(slot.high) < last) {
                break;
            }
            let key = &kept.keys[slot.start..][..slot.len as usize];
            self.score(&Key::from_bytes(key))?;
        }
        Ok(threshold)
    }
}

/// A slot still in the running: the top of its range, and where its key is in [`Kept::keys`].
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
    /// How many slots, and how many bytes of keys, the reservation holds room for.
    room: usize,
    key_room: usize,
}

impl Kept {
    /// Slots there is room for at first.
    const FIRST: usize = 1024;

    /// Bytes the reservation holds for the list.
    fn reserved(&self) -> u64 {
        (self.room * size_of::<Slot>() + self.key_room) as u64
    }

    /// Empties the list and keeps its room.
    fn clear(&mut self) {
        self.slots.clear();
        self.keys.clear();
    }

    /// Adds a slot. When the list is full, the slots now below the threshold are dropped
    /// first, and the room is doubled only if more than half are left.
    fn keep(
        &mut self,
        high: f32,
        key: &Key,
        threshold: f64,
        reservation: &mut Reservation,
    ) -> Result<()> {
        if self.slots.len() == self.room {
            self.drop_below(threshold);
            let more = match self.room {
                0 => Kept::FIRST,
                room if self.slots.len() > room / 2 => room,
                _ => 0,
            };
            reservation.grow((more * size_of::<Slot>()) as u64)?;
            self.slots
                .reserve_exact(self.room + more - self.slots.len());
            self.room += more;
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
        self.slots.push(Slot { high, len, start });
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
