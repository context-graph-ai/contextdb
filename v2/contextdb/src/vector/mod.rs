//! vector: the blocks of a VECTOR column, their IVFs and binary codes, search over them,
//! and its partitions.
//!
//! It never decides which rows a reader may see: `query` tells it, row by row, and a row is in
//! an answer only with the distance `query` worked out from the row's stored vector.
//!
//! Every vector is stored once with its row, at full precision unless the column is declared
//! SQ8. For search this part keeps a second, smaller copy: per column and partition, one
//! signed byte for each number of a vector, packed into blocks. A vector that is in no block
//! yet is on its partition's pending list.
//! Blocks and lists are derived from rows: a row that is gone, or whose vector has changed,
//! leaves a slot behind that no answer can come from.
//!
//! An exact search, and any search of a partition searched whole, screens every block of the
//! partitions it was given with the one-byte copies. That gives each vector a range its true
//! similarity is proven to lie in, and the search asks `query` for the true distance of only
//! the rows whose range can still reach the answer. The rows it returns are the ones a
//! comparison of every stored vector returns.
//!
//! A partition that has grown is given an IVF: its slots sorted around centroids, and for
//! every slot a binary code of one bit a number. A search that may leave vectors out ranks the
//! centroids, estimates the binary codes of the nearest groups and of the tail, and screens the
//! one-byte copies of only the slots whose estimate can still reach the answer.

mod binary;
mod block;
mod build;
mod directory;
mod inspect;
mod ivf;
mod pack;
mod seal;
mod search;
mod slot;
mod tally;
#[cfg(test)]
pub(crate) mod tests;
mod train;
mod unindexed;

pub(crate) use build::{prepare, progress, stage_refusal, stage_step, unindexed_work};
pub(crate) use inspect::{Inspected, State, owners, partitions, verify_record};
pub(crate) use seal::seal;
pub(crate) use search::{Nearest, Planned, Route, plan, search};
pub(crate) use slot::{Reduced, distance};
pub(crate) use tally::Tally;
pub(crate) use unindexed::{Committed, Totals, erase, track_unindexed};
