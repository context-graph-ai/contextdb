//! One vector as a binary code keeps it, and the range a search gets from a binary code.
//!
//! A binary code is one bit for each number of the vector, then two bytes. The vector is the one its
//! slot keeps (the slot's codes times its scale), less a reference: the centroid of its group for
//! a slot of an IVF, the partition's mean for a slot of a tail block. Bit `i` is set when
//! number `i` of that difference is zero or more. The two bytes are a factor, low byte first:
//! the squared length of the difference over the sum of the sizes of its numbers, times 32,768
//! and rounded, held to 65,535; zero for a difference of all zeros.
//!
//! The product of the slot's vector with the unit vector `w` of a search is estimated as the
//! product of the reference with `w`, plus the factor times the sum of `w`'s numbers, each
//! taken with the sign its bit says. The estimate is not proven: how far it may be off is
//! measured when an IVF is built, as a width in quarters of the factor, and a search keeps
//! every slot whose estimate, widened by it, can still reach the answer.

use super::block::BlockReader;
use super::slot::{SLOT_HEADER, length, scale};

/// Bytes of a binary code after its bits.
pub(super) const BINARY_TAIL: usize = 2;

/// Bytes the binary code of a vector of this dimension takes.
pub(super) fn code_len(dimension: usize) -> usize {
    dimension.div_ceil(8) + BINARY_TAIL
}

/// The widest a binary code's estimate is ever taken to be, in quarters of its factor.
pub(super) const WIDEST: u8 = 16;

/// Writes the binary code of the vector a slot keeps, less `reference`, into `out`, which is
/// `code_len` bytes long. `reference` is of the slot's dimension.
pub(super) fn write_code(slot: &[u8], reference: &[f32], out: &mut [u8]) {
    let scale = scale(slot);
    let (bits, factor) = out.split_at_mut(reference.len().div_ceil(8));
    bits.fill(0);
    let (mut squares, mut sizes) = (0.0_f64, 0.0_f64);
    for (at, (code, from)) in slot[SLOT_HEADER..].iter().zip(reference).enumerate() {
        let off = scale * f32::from(code.cast_signed()) - from;
        bits[at / 8] |= u8::from(off >= 0.0) << (at % 8);
        squares += f64::from(off) * f64::from(off);
        sizes += f64::from(off).abs();
    }
    // The cast holds the factor to 65,535, and gives zero for the nothing over nothing of a
    // difference of all zeros.
    let kept = (squares / sizes * 32_768.0).round() as u16;
    factor.copy_from_slice(&kept.to_le_bytes());
}

/// The record of the binary codes of a block: the block's count of slots as four bytes, low byte
/// first, then one binary code for each of its slots in their order. `reference` answers the
/// reference of the slot at a place of the block. The caller has reserved the record's bytes,
/// four and `code_len` a slot, before it calls.
pub(super) fn codes_of<'a>(
    block: &BlockReader<'_>,
    dimension: usize,
    reference: &mut dyn FnMut(usize) -> &'a [f32],
) -> Vec<u8> {
    let mut record = vec![0; 4 + block.len() * code_len(dimension)];
    let (count, codes) = record.split_at_mut(4);
    count.copy_from_slice(&(block.len() as u32).to_le_bytes());
    let codes = codes.chunks_exact_mut(code_len(dimension));
    for (place, code) in codes.enumerate() {
        write_code(block.slot(place), reference(place), code);
    }
    record
}

/// The binary code at a place of a record of binary codes. A record whose length is not four bytes and
/// `count` binary codes of this dimension is not whole: the store is told so by the caller.
pub(super) struct Codes<'a> {
    bytes: &'a [u8],
    dimension: usize,
}

impl<'a> Codes<'a> {
    pub(super) fn new(bytes: &'a [u8], dimension: usize) -> Codes<'a> {
        Codes { bytes, dimension }
    }

    /// True when the record holds `count` binary codes and nothing else.
    pub(super) fn is_whole(&self, count: usize) -> bool {
        self.bytes.len() == 4 + count * code_len(self.dimension)
            && self.bytes[..4] == (count as u32).to_le_bytes()
    }

    /// The binary code at this place, counted from zero.
    pub(super) fn at(&self, place: usize) -> &'a [u8] {
        let len = code_len(self.dimension);
        &self.bytes[4 + place * len..][..len]
    }
}

/// What a search works out once from its vector and then uses for every binary code it estimates:
/// the vector at length one with each number brought to one of sixteen whole steps from -8 to
/// 7, as four planes of bits, the size of a step, and the sum of the numbers.
pub(super) struct Estimate {
    /// The four planes of each 64 numbers, one group of four after another.
    planes: Vec<u64>,
    step: f32,
    sum: f32,
}

impl Estimate {
    /// `query` is of the column's dimension and every number of it is finite.
    /// The caller has reserved the planes, 32 bytes for every 64 numbers begun, before it
    /// calls.
    pub(super) fn new(query: &[f32]) -> Estimate {
        let length = length(query);
        // A vector of zeros has no direction: its numbers at length one are zeros too.
        let unit = |x: &f32| {
            if length == 0.0 {
                0.0
            } else {
                (f64::from(*x) / length) as f32
            }
        };
        let largest = query.iter().map(|x| unit(x).abs()).fold(0.0, f32::max);
        let step = (largest / 7.0).max(f32::MIN_POSITIVE);
        let mut planes = vec![0_u64; 4 * query.len().div_ceil(64)];
        let mut sum = 0_i32;
        for (at, x) in query.iter().enumerate() {
            let steps = (unit(x) / step).round().clamp(-7.0, 7.0) as i32;
            sum += steps;
            for (plane, word) in planes[at / 64 * 4..][..4].iter_mut().enumerate() {
                *word |= u64::from(steps >> plane & 1 == 1) << (at % 64);
            }
        }
        Estimate {
            planes,
            step,
            sum: sum as f32,
        }
    }

    /// The ranges the binary codes at `places` of one record give for the similarity of their
    /// slots' vectors, pushed onto `out` in order: each `(low, high)` around `base`, the
    /// product of the binary codes' reference with the search's unit vector, plus the factor
    /// times the signed sum, and `width` quarters of the factor wide on each side. The sum is
    /// taken by counting the bits a binary code shares with each plane: the planes count one, two
    /// and four steps, and the fourth takes eight away.
    ///
    /// This is the loop a search spends its time in, and it has two arms over one body. On an
    /// x86-64 processor that says it has the bit-count instruction
    /// (`std::arch::is_x86_feature_detected!("popcnt")`, asked once a call) the body compiled
    /// for that instruction runs; otherwise, and on every other kind of processor, the body
    /// as the compiler builds it for the target. Calling the first arm is the one `unsafe`
    /// call of the library, sound because the processor was asked first. Both arms give the
    /// same ranges to the bit, and nothing outside this function knows which ran. `out` never
    /// grows past the slots of one block: the search has reserved eight bytes for each slot
    /// of the largest block before its first binary code.
    pub(super) fn ranges(
        &self,
        codes: &Codes<'_>,
        places: std::ops::Range<usize>,
        base: f32,
        width: u8,
        out: &mut Vec<(f32, f32)>,
    ) {
        #[cfg(target_arch = "x86_64")]
        {
            if std::arch::is_x86_feature_detected!("popcnt") {
                // SAFETY: the processor was asked on the line before and has the instruction
                // this body is compiled for.
                unsafe { self.counted(codes, places, base, width, out) };
                return;
            }
        }
        self.each(codes, places, base, width, out);
    }

    /// The body of [`Estimate::ranges`] compiled for the bit-count instruction.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "popcnt")]
    fn counted(
        &self,
        codes: &Codes<'_>,
        places: std::ops::Range<usize>,
        base: f32,
        width: u8,
        out: &mut Vec<(f32, f32)>,
    ) {
        self.each(codes, places, base, width, out);
    }

    /// The body of [`Estimate::ranges`]. A binary code's bits are read as words, low byte first,
    /// and the last word is completed with zeros.
    #[inline(always)]
    fn each(
        &self,
        codes: &Codes<'_>,
        places: std::ops::Range<usize>,
        base: f32,
        width: u8,
        out: &mut Vec<(f32, f32)>,
    ) {
        let (planes, _) = self.planes.as_chunks::<4>();
        for place in places {
            let (bits, factor) = codes.at(place).split_at(codes.dimension.div_ceil(8));
            let (words, rest) = bits.as_chunks::<8>();
            let mut last = [0_u8; 8];
            last[..rest.len()].copy_from_slice(rest);
            let words = words.iter().chain((!rest.is_empty()).then_some(&last));
            let mut shared = [0_u32; 4];
            for (word, planes) in words.zip(planes) {
                let word = u64::from_le_bytes(*word);
                for (count, plane) in shared.iter_mut().zip(planes) {
                    *count += (word & plane).count_ones();
                }
            }
            let under = (shared[0] + 2 * shared[1] + 4 * shared[2]) as i32 - 8 * shared[3] as i32;
            let factor = f32::from(u16::from_le_bytes([factor[0], factor[1]])) / 32_768.0;
            let estimate = base + factor * self.step * ((2 * under) as f32 - self.sum);
            let wide = f32::from(width) / 4.0 * factor;
            out.push((estimate - wide, estimate + wide));
        }
    }
}
