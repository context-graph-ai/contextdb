#![allow(clippy::duplicate_mod)] // Shared unchanged fixtures retain their original module paths.

const UNRELATED_RESIDENT_ROWS: i64 = 10_000;

// Each binary uses only part of the canonical test-only helper set.
#[allow(dead_code)]
#[path = "schema/schema_replay_identical_schema_does_no_artifact_work.rs"]
mod schema_replay_is_proportional_to_affected_tables;
