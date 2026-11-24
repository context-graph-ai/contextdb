//! The last steps of a build: packing the slots of each group into the blocks of the new
//! IVF with a binary code of each, measuring how far a search scans and how wide it
//! takes a binary code's estimate to be, and putting the IVF in force.

use super::binary::{Codes, WIDEST, code_len};
use super::block::{BLOCK_BYTES, BROKEN, BlockReader, BlockWriter};
use super::build::{
    Build, Ground, MEASURED, Phase, STEP_COMPARISONS, STEP_SLOTS, Step, left_at, moved,
};
use super::directory::{Grouped, Partition, kept};
use super::ivf::{Ivf, Place, centroids, centroids_per_record, mean, nearest};
use super::seal::put_block;
use super::search::{BINARY_CODES, gap_needed, kept_by_width};
use super::slot::slot_len;
use super::train;
use crate::memory::Reservation;
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
    let sums = 8 * ground.build.sum.len() as u64;
    step.held.grow(2 * (lists + sums) + FIELDS)?;
    Ok(ground.build.clone())
}

/// The IVF a build has packed, as the partition's record names it once it is in force.
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

/// The IVF of the build's places with these three numbers, its places reserved first.
fn ivf_of(build: &Build, step: &mut Step, (gap, probes, width): (u32, u32, u8)) -> Result<Ivf> {
    step.held.grow(8 * build.starts.len() as u64)?;
    Ok(Ivf {
        gap,
        probes,
        width,
        starts: build.starts.clone(),
    })
}

/// Validates a borrowed block and returns its reader.
pub(super) fn block<'a>(read: &dyn Read, v: &'a [u8], dim: usize) -> Result<BlockReader<'a>> {
    let (r, err) = (BlockReader::new(v, dim), || read.damaged(BROKEN));
    r.is_whole().then_some(r).ok_or_else(err)
}

/// Validates a borrowed binary-code record and returns it with its slot count.
pub(super) fn codes<'a>(read: &dyn Read, v: &'a [u8], dim: usize) -> Result<(Codes<'a>, usize)> {
    let (n, c) = (
        v.len().saturating_sub(4) / code_len(dim),
        Codes::new(v, dim),
    );
    let err = || read.damaged(BINARY_CODES);
    c.is_whole(n).then_some((c, n)).ok_or_else(err)
}

/// Hands `each` the block kept under `record`, read in place.
pub(super) fn with_block<T>(
    read: &dyn Read,
    record: VectorRecord<'_>,
    dim: usize,
    each: &mut dyn FnMut(&BlockReader<'_>) -> Result<T>,
) -> Result<T> {
    kept(read, record, |v| each(&block(read, v, dim)?))?.ok_or_else(|| read.damaged(BROKEN))?
}

/// Validates a block and its companion codes against their partition or build owner.
pub(super) fn companions<T>(
    read: &dyn Read,
    record: VectorRecord<'_>,
    dim: usize,
    (p, b): (&Partition, Option<&Build>),
    lent: Option<&[u8]>,
    each: &mut dyn FnMut(&BlockReader<'_>) -> Result<T>,
) -> Result<T> {
    let (VectorRecord::Block(id, n, at) | VectorRecord::Binary(id, n, at)) = record else {
        unreachable!()
    };
    let groups = [
        p.ivf,
        b.and_then(|b| b.from),
        b.filter(|b| b.phase != Phase::Sweep).map(packed),
    ];
    let (span, many) = if n == p.number {
        (p.first..p.blocks, p.ivf.is_some_and(|g| g.groups > 1))
    } else if let Some(g) = groups.into_iter().flatten().find(|g| g.number == n) {
        (0..g.blocks, g.groups > 1)
    } else {
        return Err(read.damaged("a vector block has no owner"));
    };
    if !span.contains(&at) {
        return Err(read.damaged("a vector block is outside its owner"));
    }
    let mut pair = |r: &BlockReader<'_>| {
        let whole = |v: &[u8]| codes(read, v, dim).map(|(_, len)| len == r.len());
        let ok = match lent.filter(|_| matches!(record, VectorRecord::Binary(..))) {
            Some(v) => whole(v)?,
            None => kept(read, VectorRecord::Binary(id, n, at), whole)?
                .transpose()?
                .unwrap_or(!many),
        };
        ok.then_some(()).ok_or_else(|| read.damaged(BINARY_CODES))?;
        each(r)
    };
    match lent.filter(|_| matches!(record, VectorRecord::Block(..))) {
        Some(v) => pair(&block(read, v, dim)?),
        None => with_block(read, VectorRecord::Block(id, n, at), dim, &mut pair),
    }
}

/// The step that packs the next groups: whole groups while they come to at most
/// `STEP_SLOTS` slots, and one group at least. Within a group the slots are in the order of
/// their rows' keys and a key comes once. Every block it writes comes with the record of its
/// binary codes, each taken from the centroid of its slot's group.
///
/// Reserved before it is taken: for each slot of the step's groups its bytes, its key, and
/// the entries of the list the slots are sorted in and of the two that list is parted into;
/// the lists of the block being written, and every block and record of binary codes the step
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
    let mut centroids = match build.groups {
        1 => Vec::new(),
        groups => centroids(
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
        let VectorRecord::Assigned(column, ivf, place) = record else {
            return Ok(false);
        };
        if (column, ivf) != (vector, build.number) {
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
                step.rows_read += 1;
                found.push((to(at), key, found.len() as u32));
            }
            Ok(())
        })?;
        Ok(true)
    })?;
    if build.groups > 1 {
        step.held.grow(
            8 * dimension as u64 * (u64::from(end - group) + 2 * u64::from(build.sum.is_empty())),
        )?;
        build.sum.resize(dimension, 0.0);
        let mut sums = vec![0.0; (end - group) as usize * dimension];
        for (to, _, at) in &found {
            let slot = &slots[*at as usize * len..][..len];
            let sum = &mut sums[(*to - group) as usize * dimension..][..dimension];
            train::add(sum, slot);
            train::add(&mut build.sum, slot);
        }
        for (sum, centroid) in sums.chunks(dimension).zip(
            centroids[group as usize * dimension..end as usize * dimension].chunks_mut(dimension),
        ) {
            centroid.fill(0.0);
            train::normalize(sum, centroid);
        }
        if end == build.groups {
            step.held.grow(12 * dimension as u64)?;
            step.mean = Some(train::mean(&build.sum, packed(&build).slots));
        }
    }
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
        let open = |b: &[u8]| {
            block(read, b, dimension).map(|_| BlockWriter::new(dimension, Some(b), &keys))
        };
        kept(read, last, open)?.ok_or_else(|| read.damaged(BROKEN))??
    };
    let put = |step: &mut Step, build: &Build, block, writer| {
        let centroid = |(block, slot): (u32, u32)| {
            let place = Place { block, slot };
            let group = build.starts.partition_point(|start| *start <= place) - 1;
            &centroids[group * dimension..][..dimension]
        };
        let reference = (!centroids.is_empty()).then_some(centroid);
        let sink = |record, value| {
            step.puts.push((record, value));
            Ok(())
        };
        let held = (&mut step.held, |_: &mut Reservation, _| {});
        let reference = (dimension, reference);
        put_block((vector, build.number, block), writer, held, reference, sink)
    };
    build.counts[group as usize..end as usize].fill(0);
    for (done, (key, (to, at))) in keys.iter().zip(&order).enumerate() {
        if left == 0 {
            // A block that was full before this step is left as it is.
            if !writer.is_empty() {
                put(step, &build, place.block, writer)?;
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
        put(step, &build, place.block, writer)?;
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
    step.centroids = centroids;
    moved(ground, step, build);
    Ok(())
}

/// Makes question `number` of a build over `slots` slots in `query`, from the vectors that
/// slots of the new IVF keep (scale times codes), brought to length one. An even
/// question is the middle of two: the slot at `number * slots / M`, with `M` the lesser of
/// `MEASURED` and `slots`, and the one half the IVF further on, added. An odd question is
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
    let mut sum = vec![0.0; ground.dimension];
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
                    train::add(&mut sum, block.slot(place));
                }
                Ok(())
            })?;
            block += 1;
        }
    }
    train::normalize(&sum, query);
    Ok(())
}

/// The step that asks the new IVF the next of its questions (`build::MEASURED`) and
/// keeps the largest gap any of them needed for its ten nearest: how far the centroid of the
/// furthest group holding one of them falls short of the best centroid's product.
pub(super) fn measure(ground: &Ground<'_>, step: &mut Step) -> Result<()> {
    let Phase::Measure { mut done, mut most } = ground.build.phase else {
        unreachable!("a build that measures its gap is in that phase")
    };
    let (read, vector, dimension) = (ground.read, step.work.vector, ground.dimension);
    let mut build = copy(ground, step)?;
    let grouped = packed(&build);
    let ivf = ivf_of(&build, step, (0, 1, 0))?;
    step.held.grow(12 * dimension as u64)?;
    let mut query = Vec::with_capacity(dimension);
    let questions = grouped.slots.min(u64::from(MEASURED));
    let fitting = (STEP_COMPARISONS / grouped.slots).max(1);
    let until = (u64::from(done) + fitting).min(questions) as u32;
    while done < until {
        question(ground, vector, grouped.slots, done, &mut query)?;
        let taken = step.held.bytes();
        step.held.grow(80)?;
        let held = &mut step.held;
        let (gap, places) = gap_needed(read, vector, grouped, &ivf, &query, held)?;
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
/// the question's ten the binary codes kept: both the binary code a slot has from its group's centroid
/// and the one a tail block would keep of it, from the partition's mean, so that the one
/// width serves a search of the IVF and of its tail. When the last is asked, the width
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
    let ivf = ivf_of(&build, step, (gap, 1, 0))?;
    step.held.grow(12 * dimension as u64)?;
    let mut query = Vec::with_capacity(dimension);
    let mean = mean(read, vector, build.number, dimension, &mut step.held)?;
    let questions = grouped.slots.min(u64::from(MEASURED)) as u32;
    let each = build.tens.len() / questions as usize;
    let (first, mut estimated) = (done, 0);
    while done < questions {
        question(ground, vector, grouped.slots, done, &mut query)?;
        // The binary codes the question has estimated: the slots of the groups inside the gap.
        let taken = step.held.bytes();
        let inside: u64 = {
            let mut groups = nearest(read, vector, grouped, &query, &mut step.held, None)?;
            groups.retain(|(group, _)| ivf.nonempty(*group));
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
        let (more, inside) = kept_by_width(read, vector, grouped, &ivf, both, wanted, held)?;
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

/// The last step: writes the IVF's record and has the partition's record name it, and
/// removes what it replaces: the IVF the build read, the tail blocks it read with their
/// binary codes, and the records of the build: all of them, or all but its own when some slot
/// it read was stale, which then stays for [`sweep`].
pub(super) fn switch(ground: &Ground<'_>, step: &mut Step) -> Result<()> {
    let Phase::Switch { gap, probes, width } = ground.build.phase else {
        unreachable!("a build that switches is in that phase")
    };
    let (vector, dimension) = (step.work.vector, ground.dimension);
    let number = ground.partition.number;
    let mut build = copy(ground, step)?;
    let ivf = ivf_of(&build, step, (gap, probes, width))?;
    // The IVF's record, and the list of what is removed: at most a record, the centroids
    // and two records a block of the IVF the build read, two a tail block, and the
    // build's own with one for each block it read.
    let removed = 3 * u64::from(build.blocks()) + build.from.map_or(0, |from| from.groups.into());
    let listed = (removed + 2) * size_of::<VectorRecord<'_>>() as u64;
    step.held
        .grow(12 + 8 * build.starts.len() as u64 + listed)?;
    let record = VectorRecord::Ivf(vector, build.number);
    step.puts.push((record, ivf.encode()));
    step.switch = Some(packed(&build));
    if let Some(from) = build.from {
        step.removes.extend(records_of(vector, from, dimension));
    }
    for block in build.tail.0..build.tail.1 {
        let code = VectorRecord::Binary(vector, number, block);
        step.removes
            .extend([VectorRecord::Block(vector, number, block), code]);
    }
    // The centroids, blocks and binary codes the build wrote are the IVF's now.
    let written = build.written(vector, number, dimension);
    step.removes.extend(written.filter(|record| match record {
        VectorRecord::Assigned(..) => true,
        VectorRecord::Build(..) => !build.stale,
        _ => false,
    }));
    if build.stale {
        build.from = None;
        build.tens.clear();
        build.phase = Phase::Sweep;
        moved(ground, step, build);
    }
    Ok(())
}

/// After the switch, removes up to 4,096 deletions through the source commit, inclusive.
/// Newer deletions stay for the next build. When none is left, removes the build record.
pub(super) fn sweep(ground: &Ground<'_>, step: &mut Step) -> Result<()> {
    let (vector, number) = (step.work.vector, ground.partition.number);
    let cutoff = ground.partition.building.map_or(0, |(_, source)| source);
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
        let [(_, fence), _] = left_at(ground.read, &ground.partition, value)?;
        if fence <= cutoff {
            held.room(left, SWEPT)?;
            held.grow(key.as_bytes().len() as u64)?;
            left.push((key.clone(), fence));
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

/// Every record owned by an IVF.
pub(super) fn records_of(
    vector: VectorId,
    grouped: Grouped,
    dimension: usize,
) -> impl Iterator<Item = VectorRecord<'static>> {
    let Grouped { number, blocks, .. } = grouped;
    let centroids = grouped
        .groups
        .div_ceil(centroids_per_record(dimension) as u32)
        * u32::from(grouped.groups > 1);
    std::iter::once(VectorRecord::Ivf(vector, number))
        .chain((grouped.groups > 1).then_some(VectorRecord::Mean(vector, number)))
        .chain((0..centroids).map(move |place| VectorRecord::Centroids(vector, number, place)))
        .chain((0..blocks).flat_map(move |block| {
            let code = VectorRecord::Binary(vector, number, block);
            [VectorRecord::Block(vector, number, block), code]
        }))
}
