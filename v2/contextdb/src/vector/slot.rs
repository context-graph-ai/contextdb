//! One vector as a block keeps it, the range a search gets from it, and the distance that
//! every answer is ordered by.
//!
//! A slot is eight bytes and then one signed byte for each number of the vector: the scale as
//! an `f32`, the rounding error as an `f32`, then the codes. The codes are of the vector
//! brought to length one, `u = x / |x|`: `scale = max |u_i| / 127` and `code_i = round(u_i /
//! scale)`. The rounding error is the length of `u - scale * code`, worked out in 64 bits and
//! rounded up, so it is never smaller than the truth. A vector of all zeros has scale and
//! error zero.

/// Bytes of a slot before its codes.
pub(super) const SLOT_HEADER: usize = 8;

/// What a figure worked out in 64 bits is multiplied by so that it is not below the truth: the
/// roundings of that arithmetic come to less than a thousand millionth of the figure for any
/// vector of fewer than ten million numbers.
const UP: f64 = 1.0 + 1e-9;

/// Bytes the slot of a vector of this dimension takes.
pub(super) fn slot_len(dimension: usize) -> usize {
    SLOT_HEADER + dimension
}

/// The cosine distance between a stored vector and the vector of a search. Every answer is
/// ordered by this number and by nothing else, so a search and a comparison of every stored
/// vector agree to the last bit.
///
/// Each number is widened to 64 bits before it is multiplied, and the three sums (the dot
/// product and the two squared lengths) are added in the order of the numbers. The distance is
/// `1 - dot / (|stored| * |query|)`, and 1 when either vector is all zeros.
pub(crate) fn distance(stored: &[f32], query: &[f32]) -> f64 {
    let (mut dot, mut aa, mut bb) = (0.0_f64, 0.0_f64, 0.0_f64);
    for (x, y) in stored.iter().zip(query) {
        let (x, y) = (f64::from(*x), f64::from(*y));
        dot += x * y;
        aa += x * x;
        bb += y * y;
    }
    if aa == 0.0 || bb == 0.0 {
        1.0
    } else {
        1.0 - dot / (aa.sqrt() * bb.sqrt())
    }
}

/// The length of a vector, in 64 bits: the square of the largest `f32` does not fit in 32.
fn length(vector: &[f32]) -> f64 {
    let squares = vector.iter().map(|x| f64::from(*x) * f64::from(*x));
    squares.sum::<f64>().sqrt()
}

/// Writes the slot of `vector` into `out`, which is `slot_len(vector.len())` bytes long. Every
/// number of the vector is finite: `write` refuses any other before it is stored.
///
/// The codes are worked out with the scale as it is stored, and so is the error, which is
/// then rounded up twice: by [`UP`] for the arithmetic, and to the next `f32`.
pub(super) fn write_slot(vector: &[f32], out: &mut [u8]) {
    let length = length(vector);
    if length == 0.0 {
        out.fill(0);
        return;
    }
    let unit = |x: &f32| f64::from(*x) / length;
    let largest = vector.iter().map(|x| unit(x).abs()).fold(0.0, f64::max);
    let scale = (largest / 127.0) as f32;
    let mut off = 0.0_f64;
    for (x, code) in vector.iter().zip(&mut out[SLOT_HEADER..]) {
        let exact = unit(x);
        let rounded = (exact / f64::from(scale)).round().clamp(-127.0, 127.0);
        *code = (rounded as i8).cast_unsigned();
        let by = exact - rounded * f64::from(scale);
        off += by * by;
    }
    let error = ((off.sqrt() * UP) as f32).next_up();
    out[..4].copy_from_slice(&scale.to_le_bytes());
    out[4..SLOT_HEADER].copy_from_slice(&error.to_le_bytes());
}

/// The codes of a slot multiplied by the numbers of `unit` and added up, in 32 bits. Sixteen
/// sums run side by side, so the compiler turns the loop into the widest instructions of the
/// machine the library is built for.
fn product(codes: &[u8], unit: &[f32]) -> f32 {
    let mut lanes = [0.0_f32; 16];
    let (code_runs, code_rest) = codes.as_chunks::<16>();
    let (unit_runs, unit_rest) = unit.as_chunks::<16>();
    for (codes, unit) in code_runs.iter().zip(unit_runs) {
        for ((lane, code), number) in lanes.iter_mut().zip(codes).zip(unit) {
            *lane += f32::from(code.cast_signed()) * number;
        }
    }
    for ((lane, code), number) in lanes.iter_mut().zip(code_rest).zip(unit_rest) {
        *lane += f32::from(code.cast_signed()) * number;
    }
    lanes.iter().sum()
}

/// What a search works out once from its vector and then uses for every slot it screens.
pub(super) struct Screen {
    /// The vector of the search brought to length one, in 32 bits.
    unit: Vec<f32>,
    /// The length of `unit`, rounded up.
    length: f64,
    /// The most that the 32-bit arithmetic of one slot's product can be off by, for a slot of
    /// scale one.
    slack: f64,
    /// The most a similarity can be off by for reasons that do not depend on the slot: `unit`
    /// is not exactly of length one, and [`distance`] rounds too.
    drift: f64,
}

impl Screen {
    /// `query` is of the column's dimension and every number of it is finite.
    pub(super) fn new(query: &[f32]) -> Screen {
        let n = query.len();
        let length = length(query);
        let mut unit = Vec::with_capacity(n);
        let (mut squares, mut sum, mut off) = (0.0_f64, 0.0_f64, 0.0_f64);
        for x in query {
            // A vector of zeros has no direction: its `unit` is zeros too.
            let exact = if length == 0.0 {
                0.0
            } else {
                f64::from(*x) / length
            };
            let kept = exact as f32;
            unit.push(kept);
            let kept = f64::from(kept);
            squares += kept * kept;
            sum += kept.abs();
            off += (kept - exact) * (kept - exact);
        }
        // One slot's product is rounded this many times, one after another: a multiplication,
        // the additions of its lane, the sixteen lanes added up, the multiplication by the scale.
        let roundings = (n.div_ceil(16) + 18) as f64 * f64::from(f32::EPSILON) / 2.0;
        let gamma = roundings / (1.0 - roundings);
        Screen {
            unit,
            length: squares.sqrt() * UP,
            slack: 2.0 * 128.0 * gamma * sum * UP,
            drift: off.sqrt() * UP + 8.0 * (n + 4) as f64 * f64::EPSILON / 2.0,
        }
    }

    /// The range that the similarity of a slot's vector is proven to lie in: `(low, high)`
    /// with `low <= 1 - distance(x, query) <= high` for the vector `x` the slot was written
    /// from.
    ///
    /// Proof. Write `u = x / |x|` and `w = query / |query|`, `s`, `c` and `r` for the slot's
    /// scale, codes and error, `t` for `unit`, which is `w` rounded to 32 bits, and `n` for
    /// the dimension. In exact arithmetic `u.w - s (c.t) = u.(w - t) + (u - s c).t`, so by
    /// Cauchy-Schwarz, with `|u| = 1`,
    ///
    /// `|u.w - s (c.t)| <= |w - t| + |u - s c| |t|`.
    ///
    /// What is compared is not `u.w` and `s (c.t)` but numbers worked out from them, and
    /// each rounding between has its term:
    ///
    /// - `|u - s c| |t|`. [`write_slot`] measured `r` against `u` as 64-bit arithmetic gives
    ///   it, which is within `(n + 3) 2^-54` of `u`, and rounded `r` up; `length` is `|t|`
    ///   rounded up. The term is `r * length`.
    /// - `s (c.t)`. [`product`] and the multiplication by `s` round at most `ceil(n / 16) + 18`
    ///   times in 32 bits, on terms no larger than `127 s |t_i|`. With `g` for that count
    ///   times `2^-24` and `gamma = g / (1 - g)`, what is worked out is within
    ///   `127 gamma s |t|_1` of `s (c.t)`. The term is `s * slack`, twice that and more.
    /// - `|w - t|`. [`Screen::new`] measured `t` against `w` as 64-bit arithmetic gives it,
    ///   rounded up, and that `w` is within `(n + 3) 2^-54` of `w`.
    /// - `u.w`. [`distance`] multiplies exactly, two numbers of 24 bits in 53, and rounds
    ///   `n - 1` times in each of its three sums and four times after them; taking it from
    ///   one and the result from one again rounds twice more. So `1 - distance` is within
    ///   `(2n + 6) 2^-53` of `u.w`.
    ///
    /// The last two, and the `(n + 3) 2^-54` of the first, depend on no slot and are `drift`:
    /// the measured part of `|w - t|`, and `8 (n + 4) 2^-53` for the rest, which comes to
    /// `(3n + 9) 2^-53` and so leaves room for a product too small for 32 bits to round in
    /// proportion. The half width is therefore `r * length + s * slack + drift`,
    /// widened by [`UP`] for its own arithmetic, and both ends are rounded outward to 32
    /// bits.
    ///
    /// A vector of zeros, stored or searched for, is at distance 1. Its codes or its `unit`
    /// are zeros, so the middle of the range is 0, and `drift` is above zero.
    pub(super) fn range(&self, slot: &[u8]) -> (f32, f32) {
        let scale = f32::from_le_bytes([slot[0], slot[1], slot[2], slot[3]]);
        let error = f32::from_le_bytes([slot[4], slot[5], slot[6], slot[7]]);
        let middle = f64::from(scale * product(&slot[SLOT_HEADER..], &self.unit));
        let half = f64::from(error) * self.length + f64::from(scale) * self.slack + self.drift;
        let half = half * UP;
        (
            ((middle - half) as f32).next_down(),
            ((middle + half) as f32).next_up(),
        )
    }
}
