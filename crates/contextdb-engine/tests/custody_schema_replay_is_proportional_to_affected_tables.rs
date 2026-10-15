#![allow(clippy::duplicate_mod)] // Shared unchanged fixtures retain their original module paths.

const UNRELATED_RESIDENT_ROWS: i64 = 10_000;

#[allow(dead_code)]
#[path = "schema/schema_replay_is_proportional_to_affected_tables.rs"]
mod schema_replay_is_proportional_to_affected_tables;
