//! The last steps of a build: packing the slots of each group into the blocks of the new
//! grouping with a sketch of each, measuring how far a search scans and how wide it
//! takes a sketch's estimate to be, and putting the grouping in force.

use super::block::{BLOCK_BYTES, BROKEN, BlockReader, BlockWriter};
use super::build::{
    Build, Ground, MEASURED, Phase, STEP_COMPARISONS, STEP_SLOTS, Step, left_at, moved,
};
use super::directory::{Grouped, kept};
use super::grouping::{Grouping, Place, centres, centres_to_a_record, mean, nearest};
use super::seal::room;
use super::search::{gap_needed, kept_by_width};
use super::sketch::{WIDEST, sketch_len, sketches_of};
use super::slot::{SLOT_HEADER, slot_len};
use crate::store::{Key, Read, VectorId, VectorRecord};
use crate::types::Result;

/// More bytes than the fields of a build's record take besides its lists.
const FIELDS: u64 = 256;

/// The most `Left` records one step removes.
const SWEPT: usize = 4096;

/// What a store is told when a build's record of the groups of a block's slots is not two
/// bytes for each slot of the block.
pub(super) const ASSIGNED: &str =
    "the groups a build of the vector part gave a block are not one for each slot";

/// What a store is told when a build's `Assigned` records give the groups of a step more
/// slots than the build counted for them.
const COUNTED: &str = "a build of the vector part was given more slots than it counted";

/// A copy of the build for a step to move on. What its lists can come to (a count and a
/// place a group, ten places a question) is taken twice first: for the copy, and for the
/// record the step puts.
fn copy(ground: &Ground<'_>, step: &mut Step) -> Result<Build> {
    let lists = 12 * u64::from(ground.build.groups) + 8 + 80 * u64::from(MEASURED);
    step.held.grow(2 * lists + FIELDS)?;
    Ok(ground.build.clone())
}

/// The grouping a build has packed, as the partition's record names it once it is in force.
fn packed(build: &Build) -> Grouped {
    let end = build.starts.last().copied().unwrap_or_default();
    Grouped {
        number: build.number,
        blocks: end.block + u32::from(end.slot > 0),
        groups: build.groups,
        slots: build.counts.iter().map(|count| u64::from(*count)).sum(),
        upto: build.upto,
    }
}

/// The grouping of the build's places with these three numbers, its places reserved first.
fn grouping_of(
    build: &Build,
    step: &mut Step,
    (gap, probes, width): (u32, u32, u8),
) -> Result<Grouping> {
    step.held.grow(8 * build.starts.len() as u64)?;
    Ok(Grouping {
        gap,
        probes,
        width,
        starts: build.starts.clone(),
    })
}

/// Hands `each` the block kept under `record`, read in place. A block that is missing or not
/// whole is damage.
fn with_block(
    read: &dyn Read,
    record: VectorRecord<'_>,
    dimension: usize,
    each: &mut dyn FnMut(&BlockReader<'_>) -> Result<()>,
) -> Result<()> {
    let mut whole = false;
    read.vectors(record, &mut |first, bytes| {
        let block = BlockReader::new(bytes, dimension);
        whole = first == record && block.is_whole();
        if whole {
            each(&block)?;
        }
        Ok(false)
    })?;
    if whole {
        Ok(())
    } else {
        Err(read.damaged(BROKEN))
    }
}

/// Finishes a block of the build and puts it, its bytes reserved first. With centres, the
/// record of its sketches is put with it, made from the whole block: the reference of a slot
/// is the centre of the last group that begins at or before the slot's place.
fn put_block(
    step: &mut Step,
    build: &Build,
    block: u32,
    writer: BlockWriter,
    centres: &[f32],
    dimension: usize,
) -> Result<()> {
    let vector = step.work.vector;
    step.held.grow(writer.bytes() as u64)?;
    let value = writer.finish();
    if !centres.is_empty() {
        let count = BlockReader::new(&value, dimension).len();
        step.held.grow((4 + count * sketch_len(dimension)) as u64)?;
        let mut centre = |slot: usize| {
            let place = Place {
                block,
                slot: slot as u32,
            };
            let group = build.starts.partition_point(|start| *start <= place) - 1;
            &centres[group * dimension..][..dimension]
        };
        let sketches = sketches_of(&value, dimension, &mut centre);
        let record = VectorRecord::Sketch(vector, build.number, block);
        step.puts.push((record, sketches));
    }
    let record = VectorRecord::Block(vector, build.number, block);
    step.puts.push((record, value));
    Ok(())
}

/// The step that packs the next groups: whole groups while they come to at most
/// `STEP_SLOTS` slots, and one group at least. Within a group the slots are in the order of
/// their rows' keys and a key comes once. Every block it writes comes with the record of its
/// sketches, each taken from the centre of its slot's group.
///
/// Reserved before it is taken: for each slot of the step's groups its bytes, its key, and
/// the entries of the list the slots are sorted in and of the two that list is parted into;
/// the lists of the block being written, and every block and record of sketches the step
/// puts.
pub(super) fn pack(ground: &Ground<'_>, step: &mut Step) -> Result<()> {
    let Phase::Pack { group } = ground.build.phase else {
        unreachable!("a build that packs is in its packing phase")
    };
    let (read, vector, dimension) = (ground.read, step.work.vector, ground.dimension);
    let number = ground.partition.number;
    let mut build = copy(ground, step)?;
    let count = |group: u32| u64::from(ground.build.counts[group as usize]);
    let (mut end, mut total) = (group + 1, count(group));
    while end < build.groups && total + count(end) <= STEP_SLOTS {
        total += count(end);
        end += 1;
    }
    let len = slot_len(dimension);
    let each = len + size_of::<(u32, Key, u32)>() + size_of::<(Key, u32, u32)>();
    step.held.grow(total * each as u64 + BLOCK_BYTES as u64)?;
    let centres = match build.groups {
        1 => Vec::new(),
        groups => centres(
            read,
            vector,
            build.number,
            groups,
            dimension,
            &mut step.held,
        )?,
    };

    // Every slot of the step's groups: its group, its key and where its bytes were copied.
    let total = total as usize;
    let mut found: Vec<(u32, Key, u32)> = Vec::with_capacity(total);
    let mut slots: Vec<u8> = Vec::with_capacity(total * len);
    let held = &mut step.held;
    let from = VectorRecord::Assigned(vector, build.number, 0);
    read.vectors(from, &mut |record, given| {
        let VectorRecord::Assigned(column, grouping, place) = record else {
            return Ok(false);
        };
        if (column, grouping) != (vector, build.number) {
            return Ok(false);
        }
        let (groups, []) = given.as_chunks::<2>() else {
            return Err(read.damaged(ASSIGNED));
        };
        let to = |at: usize| u32::from(u16::from_le_bytes(groups[at]));
        if !(0..groups.len()).any(|at| (group..end).contains(&to(at))) {
            return Ok(true);
        }
        let block = build.block(vector, number, place);
        with_block(read, block, dimension, &mut |block| {
            if given.len() != 2 * block.len() {
                return Err(read.damaged(ASSIGNED));
            }
            for at in (0..groups.len()).filter(|at| (group..end).contains(&to(*at))) {
                if found.len() == total {
                    return Err(read.damaged(COUNTED));
                }
                let key = block.key(at);
                held.grow(key.as_bytes().len() as u64)?;
                slots.extend_from_slice(block.slot(at));
                found.push((to(at), key, found.len() as u32));
            }
            Ok(())
        })?;
        Ok(true)
    })?;
    found.sort_unstable();
    let parted = found.into_iter().map(|(to, key, at)| (key, (to, at)));
    let (keys, order): (Vec<Key>, Vec<(u32, u32)>) = parted.unzip();

    // The slots go on from the last block the build wrote while it has room. `place` is where
    // the next slot goes if its block takes it.
    let mut place = build.starts.pop().unwrap_or_default();
    let (mut writer, mut left) = if place.slot == 0 {
        BlockWriter::new(dimension, None, &keys)
    } else {
        let last = VectorRecord::Block(vector, build.number, place.block);
        let carried = kept(read, last, |last| {
            let whole = BlockReader::new(last, dimension).is_whole();
            whole.then(|| BlockWriter::new(dimension, Some(last), &keys))
        })?;
        carried.flatten().ok_or_else(|| read.damaged(BROKEN))?
    };
    build.counts[group as usize..end as usize].fill(0);
    for (done, (key, (to, at))) in keys.iter().zip(&order).enumerate() {
        if left == 0 {
            // A block that was full before this step is left as it is.
            if !writer.is_empty() {
                put_block(step, &build, place.block, writer, &centres, dimension)?;
            }
            place = Place {
                block: place.block + 1,
                slot: 0,
            };
            (writer, left) = BlockWriter::new(dimension, None, &keys[done..]);
        }
        // The slot's group begins here if this is its first, and so does every group before
        // it that has no slot.
        build.starts.resize(*to as usize + 1, place);
        writer.push_slot(key, &slots[*at as usize * len..][..len]);
        build.counts[*to as usize] += 1;
        left -= 1;
        place.slot += 1;
    }
    if !writer.is_empty() {
        put_block(step, &build, place.block, writer, &centres, dimension)?;
    }
    build.starts.resize(end as usize + 1, place);

    step.done = keys.len() as u64;
    build.phase = if end < build.groups {
        Phase::Pack { group: end }
    } else if build.groups == 1 || packed(&build).slots < 2 {
        let (gap, probes, width) = (0, 1, 0);
        Phase::Switch { gap, probes, width }
    } else {
        Phase::Measure { done: 0, most: 0 }
    };
    moved(ground, step, build);
    Ok(())
}

/// Makes question `number` of a build over `slots` slots in `query`, from the vectors that
/// slots of the new grouping keep (scale times codes), brought to length one. An even
/// question is the middle of two: the slot at `number * slots / M`, with `M` the lesser of
/// `MEASURED` and `slots`, and the one half the grouping further on, added. An odd question is
/// the first of those alone, a stored vector, whose nearest slot is itself: a search asks
/// both kinds. A slot is found from the running sum of the groups' counts and the place
/// where its group begins; no row is read.
fn question(
    ground: &Ground<'_>,
    vector: VectorId,
    slots: u64,
    number: u32,
    query: &mut Vec<f32>,
) -> Result<()> {
    let build = &ground.build;
    query.clear();
    query.resize(ground.dimension, 0.0);
    let first = u64::from(number) * slots / slots.min(u64::from(MEASURED));
    let taken = 2 - number as usize % 2;
    for mut ordinal in [first, (first + slots / 2) % slots].into_iter().take(taken) {
        let mut group = 0;
        while ordinal >= u64::from(build.counts[group]) {
            ordinal -= u64::from(build.counts[group]);
            group += 1;
        }
        let Place { mut block, slot } = build.starts[group];
        let mut at = Some(slot as usize + ordinal as usize);
        while let Some(place) = at {
            let record = VectorRecord::Block(vector, build.number, block);
            with_block(ground.read, record, ground.dimension, &mut |block| {
                at = place.checked_sub(block.len());
                if at.is_none() {
                    let slot = block.slot(place);
                    let scale = f32::from_le_bytes([slot[0], slot[1], slot[2], slot[3]]);
                    for (sum, code) in query.iter_mut().zip(&slot[SLOT_HEADER..]) {
                        *sum += scale * f32::from(code.cast_signed());
                    }
                }
                Ok(())
            })?;
            block += 1;
        }
    }
    let length = query
        .iter()
        .map(|number| number * number)
        .sum::<f32>()
        .sqrt();
    if length > 0.0 {
        query.iter_mut().for_each(|number| *number /= length);
    }
    Ok(())
}

/// The step that asks the new grouping the next of its questions (`build::MEASURED`) and
/// keeps the largest gap any of them needed for its ten nearest: how far the centre of the
/// furthest group holding one of them falls short of the best centre's product.
pub(super) fn measure(ground: &Ground<'_>, step: &mut Step) -> Result<()> {
    let Phase::Measure { mut done, mut most } = ground.build.phase else {
        unreachable!("a build that measures its gap is in that phase")
    };
    let (read, vector, dimension) = (ground.read, step.work.vector, ground.dimension);
    let mut build = copy(ground, step)?;
    let grouped = packed(&build);
    let grouping = grouping_of(&build, step, (0, 1, 0))?;
    step.held.grow(4 * dimension as u64)?;
    let mut query = Vec::with_capacity(dimension);
    let questions = grouped.slots.min(u64::from(MEASURED));
    let fitting = (STEP_COMPARISONS / grouped.slots).max(1);
    let until = (u64::from(done) + fitting).min(questions) as u32;
    while done < until {
        question(ground, vector, grouped.slots, done, &mut query)?;
        let taken = step.held.bytes();
        step.held.grow(80)?;
        let held = &mut step.held;
        let (gap, places) = gap_needed(read, vector, grouped, &grouping, &query, held)?;
        most = most.max(gap);
        build.tens.extend(places);
        step.held.shrink(step.held.bytes() - taken);
        done += 1;
    }
    build.phase = if u64::from(done) < questions {
        Phase::Measure { done, most }
    } else {
        Phase::Width {
            gap: most,
            probes: 1,
            done: 0,
            asked: 0,
            kept: [0; 15],
        }
    };
    moved(ground, step, build);
    Ok(())
}

/// The step that asks the next of the same questions again, of the groups inside the gap
/// alone, keeps the most groups any of them had inside, and counts for each width how many of
/// the question's ten the sketches kept: both the sketch a slot has from its group's centre
/// and the one a tail block would keep of it, from the partition's mean, so that the one
/// width serves a search of the grouping and of its tail. When the last is asked, the width
/// is the least that kept 99 in 100 of all the neighbours asked for, and 0 when none did.
pub(super) fn width(ground: &Ground<'_>, step: &mut Step) -> Result<()> {
    let Phase::Width {
        gap,
        mut probes,
        mut done,
        mut asked,
        kept: mut keeps,
    } = ground.build.phase
    else {
        unreachable!("a build that measures its width is in that phase")
    };
    let (read, vector, dimension) = (ground.read, step.work.vector, ground.dimension);
    let build = copy(ground, step)?;
    let grouped = packed(&build);
    let grouping = grouping_of(&build, step, (gap, 1, 0))?;
    step.held.grow(4 * dimension as u64)?;
    let mut query = Vec::with_capacity(dimension);
    let mean = mean(
        read,
        vector,
        ground.partition.number,
        dimension,
        &mut step.held,
    )?;
    let questions = grouped.slots.min(u64::from(MEASURED)) as u32;
    let each = build.tens.len() / questions as usize;
    let (first, mut estimated) = (done, 0);
    while done < questions {
        question(ground, vector, grouped.slots, done, &mut query)?;
        // The sketches the question has estimated: the slots of the groups inside the gap.
        let taken = step.held.bytes();
        let inside: u64 = {
            let groups = nearest(read, vector, grouped, &query, &mut step.held)?;
            let short = |product: f32| f64::from(groups[0].1 - product) * 10_000.0;
            let inside = groups
                .iter()
                .take_while(|(_, product)| short(*product) <= f64::from(gap));
            inside
                .map(|(group, _)| u64::from(build.counts[*group as usize]))
                .sum()
        };
        step.held.shrink(step.held.bytes() - taken);
        if done > first && estimated + inside > STEP_COMPARISONS {
            break;
        }
        estimated += inside;
        let wanted = &build.tens[done as usize * each..][..each];
        let held = &mut step.held;
        let both = (query.as_slice(), mean.as_slice());
        let (more, inside) = kept_by_width(read, vector, grouped, &grouping, both, wanted, held)?;
        step.held.shrink(step.held.bytes() - taken);
        asked += each as u32;
        keeps
            .iter_mut()
            .zip(more)
            .for_each(|(kept, more)| *kept += more);
        probes = probes.max(inside);
        done += 1;
    }
    let mut build = build;
    build.phase = if done < questions {
        Phase::Width {
            gap,
            probes,
            done,
            asked,
            kept: keeps,
        }
    } else {
        let enough =
            |width: &u8| u64::from(keeps[usize::from(*width) - 2]) * 100 >= u64::from(asked) * 99;
        let width = (2..=WIDEST).find(enough).unwrap_or(0);
        Phase::Switch { gap, probes, width }
    };
    moved(ground, step, build);
    Ok(())
}

/// The last step: writes the grouping's record and has the partition's record name it, and
/// removes what it replaces: the grouping the build read, the tail blocks it read with their
/// sketches, and the records of the build: all of them, or all but its own when some slot
/// it read was stale, which then stays for [`sweep`].
pub(super) fn switch(ground: &Ground<'_>, step: &mut Step) -> Result<()> {
    let Phase::Switch { gap, probes, width } = ground.build.phase else {
        unreachable!("a build that switches is in that phase")
    };
    let (vector, dimension) = (step.work.vector, ground.dimension);
    let number = ground.partition.number;
    let mut build = copy(ground, step)?;
    let grouping = grouping_of(&build, step, (gap, probes, width))?;
    // The grouping's record, and the list of what is removed: at most a record, the centres
    // and two records a block of the grouping the build read, two a tail block, and the
    // build's own with one for each block it read.
    let removed = 3 * u64::from(build.blocks()) + build.from.map_or(0, |from| from.groups.into());
    let listed = (removed + 2) * size_of::<VectorRecord<'_>>() as u64;
    step.held
        .grow(12 + 8 * build.starts.len() as u64 + listed)?;
    let record = VectorRecord::Grouping(vector, build.number);
    step.puts.push((record, grouping.encode()));
    step.switch = Some(packed(&build));
    if let Some(from) = build.from {
        step.removes.extend(records_of(vector, from, dimension));
    }
    for block in build.tail.0..build.tail.1 {
        let sketch = VectorRecord::Sketch(vector, number, block);
        step.removes
            .extend([VectorRecord::Block(vector, number, block), sketch]);
    }
    // The centres, blocks and sketches the build wrote are the grouping's now.
    let written = build.written(vector, number, dimension);
    step.removes.extend(written.filter(|record| match record {
        VectorRecord::Assigned(..) => true,
        VectorRecord::Build(..) => !build.stale,
        _ => false,
    }));
    if build.stale {
        build.tens.clear();
        build.phase = Phase::Sweep;
        moved(ground, step, build);
    }
    Ok(())
}

/// A step after the switch: removes up to 4,096 of the partition's `Left` records whose value
/// is below the build's `upto`. The build applied each of them to every slot it read, and no
/// slot left can be stale by one; a record of `upto` itself may have been written after the
/// build read its slot, and stays for the next build. When none is left the step removes the
/// build's record.
pub(super) fn sweep(ground: &Ground<'_>, step: &mut Step) -> Result<()> {
    let (vector, number) = (step.work.vector, ground.partition.number);
    let upto = ground.build.upto;
    let first = Key::from_values(&[], &[])?;
    let (held, left) = (&mut step.held, &mut step.left);
    let from = VectorRecord::Left(vector, number, &first);
    ground.read.vectors(from, &mut |record, value| {
        let VectorRecord::Left(column, partition, key) = record else {
            return Ok(false);
        };
        if (column, partition) != (vector, number) {
            return Ok(false);
        }
        if left_at(ground, value)? < upto {
            room(left, SWEPT, held)?;
            held.grow(key.as_bytes().len() as u64)?;
            left.push(key.clone());
        }
        Ok(left.len() < SWEPT)
    })?;
    if step.left.is_empty() {
        step.removes.push(VectorRecord::Build(vector, number));
    } else {
        let build = copy(ground, step)?;
        moved(ground, step, build);
    }
    Ok(())
}

/// The records of a grouping, for a commit that removes it: its record, its centres, its
/// blocks and their sketches.
pub(super) fn records_of(
    vector: VectorId,
    grouped: Grouped,
    dimension: usize,
) -> impl Iterator<Item = VectorRecord<'static>> {
    let Grouped { number, blocks, .. } = grouped;
    let centres = grouped
        .groups
        .div_ceil(centres_to_a_record(dimension) as u32);
    std::iter::once(VectorRecord::Grouping(vector, number))
        .chain((0..centres).map(move |place| VectorRecord::Centres(vector, number, place)))
        .chain((0..blocks).flat_map(move |block| {
            let sketch = VectorRecord::Sketch(vector, number, block);
            [VectorRecord::Block(vector, number, block), sketch]
        }))
}
