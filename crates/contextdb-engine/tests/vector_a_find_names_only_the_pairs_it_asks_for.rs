#![allow(clippy::duplicate_mod)] // Shared unchanged fixtures retain their original module paths.

#[cfg(unix)]
#[path = "vector/a_find_names_only_the_pairs_it_asks_for.rs"]
mod a_find_names_only_the_pairs_it_asks_for;
