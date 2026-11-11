//! A block: the slots of as many vectors of one partition as fit in the block's bytes
//! (328 at 768 numbers a vector), each with the key of its row.
//!
//! A block is `count` as four bytes, then `count` slots of one length, then `count` numbers of
//! four bytes, each the end of a key within the bytes that follow, then the keys one after
//! another. Every number is low byte first. The slots come first and are of one length, so a
//! search strides over them without reading a key.

use super::slot::{slot_len, write_slot};
use crate::store::Key;

/// The most bytes a block is written with: 64 less than 256 KiB. The storage library keeps a
/// record in a run of pages whose length is a power of two, so a block of this size fills its
/// run and one byte more would take a run twice as long.
pub(super) const BLOCK_BYTES: usize = (256 << 10) - 64;

/// Bytes of a block's count, and of each key end.
const NUMBER: usize = 4;

/// What a store is told when a stored block is not [`BlockReader::is_whole`].
pub(super) const BROKEN: &str = "a block of the vector part does not hold what it counts";

/// How many of `keys`, in their order, a block of `bytes` bytes takes within [`BLOCK_BYTES`],
/// and the bytes of those keys together. A block with no slot takes one key of any length.
pub(super) fn takes(dimension: usize, bytes: usize, keys: &[Key]) -> (usize, usize) {
    let (mut count, mut key_bytes) = (0, 0);
    for key in keys {
        let more = key_bytes + key.as_bytes().len();
        let with = bytes + (count + 1) * (slot_len(dimension) + NUMBER) + more;
        if with > BLOCK_BYTES && (bytes > NUMBER || count > 0) {
            break;
        }
        (count, key_bytes) = (count + 1, more);
    }
    (count, key_bytes)
}

/// A block being put together.
pub(super) struct BlockWriter {
    dimension: usize,
    slots: Vec<u8>,
    ends: Vec<u32>,
    keys: Vec<u8>,
}

impl BlockWriter {
    /// A writer that starts with the slots and keys of the block `from`, or with none for
    /// `None`, and how many of `keys` it [`takes`]. Its lists are made here with room for those
    /// and never grow. A block that takes none is not copied: its writer has no slot.
    pub(super) fn new(dimension: usize, from: Option<&[u8]>, keys: &[Key]) -> (BlockWriter, usize) {
        let (count, key_bytes) = takes(dimension, from.map_or(NUMBER, <[u8]>::len), keys);
        let block = from.filter(|_| count > 0).unwrap_or(&[0; NUMBER]);
        let reader = BlockReader::new(block, dimension);
        let mut slots = Vec::with_capacity(reader.ends_at() - NUMBER + count * slot_len(dimension));
        let mut ends = Vec::with_capacity(reader.len() + count);
        let mut keys = Vec::with_capacity(block.len() - reader.keys_at() + key_bytes);
        slots.extend_from_slice(&block[NUMBER..reader.ends_at()]);
        ends.extend((0..reader.len()).map(|at| reader.end(at) as u32));
        keys.extend_from_slice(&block[reader.keys_at()..]);
        let writer = BlockWriter {
            dimension,
            slots,
            ends,
            keys,
        };
        (writer, count)
    }

    /// Bytes the block has now.
    pub(super) fn bytes(&self) -> usize {
        NUMBER + self.slots.len() + NUMBER * self.ends.len() + self.keys.len()
    }

    /// Adds the slot of `vector` and the key of its row.
    pub(super) fn push(&mut self, key: &Key, vector: &[f32]) {
        let at = self.slots.len();
        self.slots.resize(at + slot_len(self.dimension), 0);
        write_slot(vector, &mut self.slots[at..]);
        self.keys.extend_from_slice(key.as_bytes());
        self.ends.push(self.keys.len() as u32);
    }

    /// True when the writer has no slot.
    pub(super) fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    /// The block's bytes.
    pub(super) fn finish(self) -> Vec<u8> {
        let mut block = Vec::with_capacity(self.bytes());
        block.extend_from_slice(&(self.ends.len() as u32).to_le_bytes());
        block.extend_from_slice(&self.slots);
        block.extend(self.ends.iter().flat_map(|end| end.to_le_bytes()));
        block.extend_from_slice(&self.keys);
        block
    }
}

/// A block as it was written, read in place.
pub(super) struct BlockReader<'a> {
    bytes: &'a [u8],
    dimension: usize,
}

impl<'a> BlockReader<'a> {
    /// Reads a block [`BlockWriter::finish`] gave for vectors of this dimension.
    pub(super) fn new(bytes: &'a [u8], dimension: usize) -> BlockReader<'a> {
        BlockReader { bytes, dimension }
    }

    /// True when the bytes are a block's: its count, that many slots and key ends, no end
    /// before the one ahead of it, and the last where the block ends. Anything else is damage,
    /// and is asked before a slot or a key is read, which would read past such bytes.
    pub(super) fn is_whole(&self) -> bool {
        let Some(count) = self.bytes.first_chunk::<NUMBER>() else {
            return false;
        };
        let each = slot_len(self.dimension) + NUMBER;
        let keys_at = (u32::from_le_bytes(*count) as usize).checked_mul(each);
        let keys_at = keys_at.and_then(|slots| slots.checked_add(NUMBER));
        let Some(keys_at) = keys_at.filter(|at| *at <= self.bytes.len()) else {
            return false;
        };
        let (ends, _) = self.bytes[self.ends_at()..keys_at].as_chunks::<NUMBER>();
        let mut ends = ends.iter().map(|end| u32::from_le_bytes(*end) as usize);
        ends.clone().is_sorted() && ends.next_back().unwrap_or(0) == self.bytes.len() - keys_at
    }

    /// The number of four bytes that begins at this byte of the block.
    fn number(&self, at: usize) -> usize {
        let bytes = self.bytes;
        u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]]) as usize
    }

    /// Where the key ends begin: after the count and the slots.
    fn ends_at(&self) -> usize {
        NUMBER + self.len() * slot_len(self.dimension)
    }

    /// Where the keys begin: after the key ends.
    fn keys_at(&self) -> usize {
        self.ends_at() + NUMBER * self.len()
    }

    /// Where the key at this place ends, counted from where the keys begin.
    fn end(&self, at: usize) -> usize {
        self.number(self.ends_at() + NUMBER * at)
    }

    /// How many slots the block has.
    pub(super) fn len(&self) -> usize {
        self.number(0)
    }

    /// The slot at this place, counted from zero.
    pub(super) fn slot(&self, at: usize) -> &'a [u8] {
        let len = slot_len(self.dimension);
        &self.bytes[NUMBER + at * len..][..len]
    }

    /// The key of the row the slot at this place was written for.
    pub(super) fn key(&self, at: usize) -> Key {
        let start = if at == 0 { 0 } else { self.end(at - 1) };
        Key::from_bytes(&self.bytes[self.keys_at()..][start..self.end(at)])
    }
}
