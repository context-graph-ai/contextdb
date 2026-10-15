#![allow(clippy::duplicate_mod)] // Shared unchanged fixtures retain their original module paths.

#[cfg(unix)]
#[path = "vector/a_saved_graph_search_reports_its_distance_work.rs"]
mod a_saved_graph_search_reports_its_distance_work;
