use std::collections::BTreeMap;

type Records = BTreeMap<Vec<u8>, Vec<u8>>;
pub type Member = (Vec<u8>, Vec<u8>, usize);
pub type Grouped = (Vec<Vec<u8>>, Vec<Vec<f32>>, Vec<Member>);
pub const DIM: usize = 16;

pub fn number(bytes: &[u8], at: usize) -> usize {
    u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize
}

fn floats(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks_exact(4)
        .map(|n| f32::from_le_bytes(n.try_into().unwrap()))
        .collect()
}

pub fn under(key: &[u8]) -> u32 {
    u32::from_be_bytes(key[5..9].try_into().unwrap())
}

pub fn key_of(id: i64) -> Vec<u8> {
    [vec![2], ((id as u64) ^ (1 << 63)).to_be_bytes().to_vec()].concat()
}

pub fn id_of(key: &[u8]) -> i64 {
    (u64::from_be_bytes(key[1..].try_into().unwrap()) ^ (1 << 63)) as i64
}

pub fn slots(block: &[u8]) -> Vec<(&[u8], &[u8])> {
    let count = number(block, 0);
    let ends = 4 + count * (8 + DIM);
    let keys = ends + count * 4;
    (0..count)
        .map(|at| {
            let start = at
                .checked_sub(1)
                .map_or(0, |at| number(block, ends + 4 * at));
            let end = number(block, ends + 4 * at);
            (
                &block[keys + start..keys + end],
                &block[4 + at * (8 + DIM)..][..8 + DIM],
            )
        })
        .collect()
}

// Independent reading of the documented block/IVF formats, never an engine-private call.
pub fn grouped(records: &Records) -> Grouped {
    let (key, ivf) = records
        .iter()
        .find(|(k, _)| k[0] == 3)
        .expect("an IVF exists");
    let starts: Vec<_> = ivf[12..]
        .chunks_exact(8)
        .map(|p| (number(p, 0), number(p, 4)))
        .collect();
    let mut centres = Vec::new();
    for (_, v) in records
        .iter()
        .filter(|(k, _)| k[0] == 4 && under(k) == under(key))
    {
        centres.extend(floats(v).chunks(DIM).map(<[f32]>::to_vec));
    }
    assert_eq!(centres.len() + 1, starts.len());
    let blocks: Vec<_> = records
        .iter()
        .filter(|(k, _)| k[0] == 0 && under(k) == under(key))
        .collect();
    let mut members = Vec::new();
    let mut block_keys = Vec::new();
    for (k, block) in blocks {
        let b = u32::from_be_bytes(k[9..13].try_into().unwrap()) as usize;
        block_keys.push(k.clone());
        for (at, (row, slot)) in slots(block).into_iter().enumerate() {
            let group = starts.partition_point(|start| *start <= (b, at)) - 1;
            members.push((row.to_vec(), slot.to_vec(), group));
        }
    }
    (block_keys, centres, members)
}

pub fn assert_derived(all: &Records) {
    for (_, p) in all
        .iter()
        .filter(|(k, p)| k[0] == 1 && p[40] & 1 != 0 && number(p, 49) > 1)
    {
        let owners = [number(p, 0) as u32, number(p, 41) as u32];
        let records: Records = all
            .iter()
            .filter(|(k, _)| [0, 3, 4, 7, 8].contains(&k[0]) && owners.contains(&under(k)))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_partition_derived(&records);
    }
}

pub fn assert_partition_derived(records: &Records) {
    let (group_blocks, centres, members) = grouped(records);
    let mut sums = vec![vec![0.0_f64; DIM]; centres.len()];
    let (mut total, mut count) = (vec![0.0_f64; DIM], 0);
    for (row, slot, group) in &members {
        if row.is_empty() {
            assert!(slot.iter().all(|b| *b == 0), "nonzero blank slot");
            continue;
        }
        count += 1;
        let scale = f32::from_le_bytes(slot[..4].try_into().unwrap());
        for (at, byte) in slot[8..].iter().enumerate() {
            let x = f64::from(scale * f32::from(*byte as i8));
            sums[*group][at] += x;
            total[at] += x;
        }
    }
    for (sum, centre) in sums.iter().zip(&centres) {
        let length = sum.iter().map(|x| x * x).sum::<f64>().sqrt();
        for (x, actual) in sum.iter().zip(centre) {
            let expected = if length == 0.0 { 0.0 } else { x / length };
            assert!((f64::from(*actual) - expected).abs() < 1e-6, "wrong centre");
        }
    }
    let mean = floats(records.iter().find(|(k, _)| k[0] == 8).unwrap().1);
    for (sum, actual) in total.iter().zip(&mean) {
        let expected = sum / count.max(1) as f64;
        assert!((f64::from(*actual) - expected).abs() < 1e-6, "wrong mean");
    }
    for (key, block) in records.iter().filter(|(k, _)| k[0] == 0) {
        let mut binary_key = key.clone();
        binary_key[0] = 7;
        let binary = &records[&binary_key];
        let block_slots = slots(block);
        assert_eq!(number(binary, 0), block_slots.len());
        assert_eq!(binary.len(), 4 + block_slots.len() * 4);
        for (at, (_, slot)) in block_slots.iter().enumerate() {
            let reference = if group_blocks.contains(key) {
                let offset: usize = group_blocks
                    .iter()
                    .take_while(|k| *k != key)
                    .map(|k| slots(&records[k]).len())
                    .sum();
                &centres[members[offset + at].2]
            } else {
                &mean
            };
            let scale = f32::from_le_bytes(slot[..4].try_into().unwrap());
            let mut expected = [0_u8; 4];
            let (mut squares, mut sizes) = (0.0, 0.0);
            for (i, (code, from)) in slot[8..].iter().zip(reference).enumerate() {
                let delta = scale * f32::from(*code as i8) - from;
                expected[i / 8] |= u8::from(delta >= 0.0) << (i % 8);
                squares += f64::from(delta).powi(2);
                sizes += f64::from(delta).abs();
            }
            expected[2..]
                .copy_from_slice(&((squares / sizes * 32768.0).round() as u16).to_le_bytes());
            assert_eq!(
                &binary[4 + at * 4..][..4],
                expected,
                "binary code uses the wrong reference"
            );
        }
    }
}
