#![allow(clippy::duplicate_mod)] // Shared unchanged fixtures retain their original module paths.

#[path = "vector/indexed_route_does_less_work_than_exact.rs"]
mod indexed_route_does_less_work_than_exact;
#[path = "fixtures/maintained_search/mod.rs"]
mod maintained_search;
