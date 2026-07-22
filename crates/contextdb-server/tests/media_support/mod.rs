//! Shared scaffolding for the media-transfer test binaries.
//!
//! These helpers are byte-identical across `media_transfer_tests.rs`,
//! `media_transfer_memory.rs`, and `media_transfer_fetch_retention.rs`; they are
//! hoisted here so the blob_ref-resolver bring-up lives once. Consumers include
//! this module with `#[path = "media_support/mod.rs"] mod media_support;` and
//! `use media_support::*;`. (The reclaim-driver test deliberately builds its
//! `blob_job` WITHOUT `submitted_at_ms`, so it keeps its own local variant.)
#![allow(dead_code)]

use contextdb_engine::Database;
use contextdb_engine::work_ledger::{
    BlobHash, ClaimInsert, InputRef, JobSpec, insert_claim, submit_job,
};
use contextdb_server::FabricIdentity;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const T0: i64 = 1_700_000_000_000;
pub const LEASE: i64 = 5 * 60_000;

pub async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .expect("bounded media-transfer operation exceeded 60s")
}

pub fn identity_file(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("fabric-identity.key")
}

pub fn bind_spec(identity: &Path) -> String {
    format!("iroh:?identity={}", identity.display())
}

pub fn node_id_of(key: &Path) -> String {
    FabricIdentity::load_or_generate(key)
        .expect("identity")
        .node_id()
}

pub fn blob_job(job_id: &str, submitter: &str, hash: &BlobHash) -> JobSpec {
    JobSpec::builder(job_id, "media.demo", "batch", submitter)
        .input_refs(vec![InputRef::blob_ref(hash.clone())])
        .submitted_at_ms(T0)
        .build()
}

pub fn seed_entitlement(
    db: &Database,
    job_id: &str,
    submitter: &str,
    claimant: &str,
    hash: &BlobHash,
) {
    submit_job(db, &blob_job(job_id, submitter, hash), &[] as &[&[u8]]).expect("submit blob job");
    match insert_claim(db, job_id, 1, claimant, T0 + LEASE, T0).expect("insert claim") {
        ClaimInsert::Inserted => {}
        other => panic!("claim seed must insert, got {other:?}"),
    }
}
