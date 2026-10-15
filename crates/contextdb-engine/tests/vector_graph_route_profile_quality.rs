#![allow(clippy::duplicate_mod)] // Shared unchanged fixtures retain their original module paths.

#[allow(dead_code, unused_imports)] // Each target uses a subset of the canonical helpers.
#[path = "vector/graph_route_profile_quality.rs"]
mod graph_route_profile_quality;
#[path = "fixtures/maintained_search/mod.rs"]
mod maintained_search;
