#![allow(clippy::duplicate_mod)] // Shared unchanged fixtures retain their original module paths.

#[cfg(unix)]
#[path = "vector/a_saved_graph_load_checks_its_bytes_once.rs"]
mod a_saved_graph_load_checks_its_bytes_once;
#[cfg(unix)]
#[path = "vector/a_wide_find_admits_every_partition_when_serving_pages_give_way.rs"]
mod a_wide_find_admits_every_partition_when_serving_pages_give_way;
#[cfg(unix)]
#[path = "vector/a_wide_find_is_admitted_at_what_its_loads_hold.rs"]
mod a_wide_find_is_admitted_at_what_its_loads_hold;
#[cfg(unix)]
#[path = "vector/a_wide_find_keeps_its_entry_pages_warm.rs"]
mod a_wide_find_keeps_its_entry_pages_warm;
#[cfg(unix)]
#[path = "vector/each_layer_a_wide_find_loads_keeps_its_share.rs"]
mod each_layer_a_wide_find_loads_keeps_its_share;
