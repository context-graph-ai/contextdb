//! vector: the blocks of a VECTOR column, exact search over them, and its partitions.
//!
//! It never decides which rows a reader may see: `query` tells it, row by row, and a row is in
//! an answer only with the distance `query` worked out from the row's stored vector.
//!
//! Every vector is stored once, at full precision, with its row. For search this part keeps a
//! second, smaller copy: per column and partition, one signed byte for each number of a vector,
//! packed into blocks. A vector that is in no block yet is on its partition's pending list.
//! Blocks and lists are derived from rows: a row that is gone, or whose vector has changed,
//! leaves a slot behind that no answer can come from.
//!
//! A search screens every block of the partitions it was given with the one-byte copies. That
//! gives each vector a range its true similarity is proven to lie in, and the search asks
//! `query` for the true distance of only the rows whose range can still reach the answer. The
//! rows it returns are the ones a comparison of every stored vector returns.

mod block;
mod directory;
mod seal;
mod search;
mod slot;
mod stage;
mod tally;
#[cfg(test)]
mod tests;

pub(crate) use seal::{Sealed, seal};
pub(crate) use search::{Nearest, search};
pub(crate) use slot::distance;
pub(crate) use stage::stage;
pub(crate) use tally::Tally;
