// Permanent fast CI: contextdb-engine-test-seams-7 runs this engine_seams
// module in the normal PR tier, with the existing test-seams feature tuple.
//! Cheap public SQL guard for simultaneous reader-parent and child accounting.
use crate::resident_history_support::{insert_vector, open, vector};
use contextdb_core::{Value, Wallclock};
use contextdb_engine::database::resident_history_test_seams::observe;
use contextdb_engine::vector_read_memory::statement_child_proof as statement;
use std::collections::HashMap;

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn public_membership_pages_keep_the_original_parent_within_its_live_limit() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    const DIM: usize = 256;
    const LIMIT: u64 = 64 * 1024;
    let mut saw_sibling_release = false;
    let mut saw_control_refund = false;
    for n in [32, 64] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("membership-peak.db");
        let db = open(&path, Some(64 * 1024 * 1024));
        db.execute(
            "CREATE TABLE membership_peak (id INT PRIMARY KEY, v VECTOR(256)) HISTORY ALL",
            &HashMap::new(),
        )
        .unwrap();
        for id in 0..n {
            insert_vector(&db, "membership_peak", id, None, DIM);
        }
        let old = db.snapshot();
        insert_vector(&db, "membership_peak", n, None, DIM);
        let current = db.snapshot();
        db.close().unwrap();
        drop(db);
        let db = open(&path, Some(64 * 1024 * 1024));
        let parent = db.declared_read_memory_for_test();
        for (snapshot, count) in [(old, n), (current, n + 1)] {
            let sql = format!(
                "SELECT id FROM membership_peak ORDER BY v <=> $v USE VECTOR EXACT LIMIT 1"
            );
            let params = HashMap::from([("v".into(), Value::Vector(vector(0, DIM)))]);
            // Prime a real retained answer so the next public scan can retire
            // its prior owner while the new statement remains live.
            db.execute_at_snapshot(&sql, &params, snapshot).unwrap();
            let ((mut ids, live), finished) = statement::observe(&parent, LIMIT, || {
                let (answer, work) = observe(|| db.execute_at_snapshot(&sql, &params, snapshot));
                let answer = answer.unwrap();
                assert!(work.membership_keys_examined >= count);
                let ids = answer
                    .rows()
                    .iter()
                    .map(|row| match row[0] {
                        Value::Int64(id) => id,
                        _ => panic!("integer fixture id"),
                    })
                    .collect::<Vec<_>>();
                drop(answer);
                (ids, statement::current().unwrap())
            });
            ids.sort_unstable();
            assert_eq!(ids, vec![0]);
            assert_eq!(live.children, 1);
            assert_eq!(live.parent_funding_mismatches, 0);
            assert!(live.peak > live.control_bytes as u64);
            let peak = live
                .peak_completed
                .expect("actual completed native child peak");
            assert_eq!(peak.relation, "target");
            assert_eq!(peak.child_after + live.control_bytes as u64, live.peak);
            assert_eq!(
                peak.parent_after as i128,
                live.parent_before as i128
                    + peak.child_after as i128
                    + peak.sibling_parent_net
                    + peak.direct_parent_net
                    + peak.target_control_parent_net,
                "original parent equals actual child plus actual standing-owner movements"
            );
            assert!(
                peak.parent_after <= LIMIT,
                "simultaneous original parent ceiling: {peak:?}"
            );
            assert!(live.parent_high_water <= LIMIT);
            assert!(live.parent_high_water >= live.parent_before);
            assert!(live.parent_high_water >= peak.parent_after);
            assert!(live.parent_high_water >= live.parent_after);
            assert!(live.parent_successful_admissions > 0);
            assert!(finished.parent_high_water <= LIMIT);
            assert!(finished.parent_high_water >= live.parent_high_water);
            assert!(finished.parent_high_water >= finished.parent_after);
            assert!(finished.parent_after <= LIMIT);
            assert_eq!(finished.parent_funding_mismatches, 0);
            assert_eq!(
                finished.complete_parent_expected,
                finished.parent_after as i128
            );
            if let Some(refund) = finished.first_control_refund {
                saw_control_refund = true;
                assert_eq!(refund.site, "NativeChildInvoice::drop");
                assert_eq!(
                    refund.parent_before - refund.parent_after,
                    refund.bytes as u64
                );
                assert_eq!(
                    refund.live_controls_before - refund.live_controls_after,
                    refund.bytes as u64
                );
            }
            if let Some(release) = finished.first_sibling_release {
                saw_sibling_release = true;
                assert_eq!(release.relation, "sibling");
                assert_eq!(release.kind, "ParentRelease");
                assert_eq!(
                    release.parent_before - release.parent_after,
                    release.bytes as u64
                );
            }
        }
    }
    assert!(
        saw_sibling_release,
        "prior statement must actually release its child invoice"
    );
    assert!(
        saw_control_refund,
        "prior native child control must physically refund"
    );
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn inactive_observation_copies_no_full_receipts_for_public_sql_n_and_2n() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let mut counts = Vec::new();
    for n in [32, 64] {
        let dir = tempfile::tempdir().unwrap();
        let db = open(&dir.path().join("inactive.db"), Some(64 * 1024 * 1024));
        db.execute(
            "CREATE TABLE inactive_copy (id INT PRIMARY KEY, v VECTOR(256)) HISTORY ALL",
            &HashMap::new(),
        )
        .unwrap();
        for id in 0..n {
            insert_vector(&db, "inactive_copy", id, None, 256);
        }
        let snapshot = db.snapshot();
        let params = HashMap::from([("v".into(), Value::Vector(vector(0, 256)))]);
        let ((answer, work), copies) = statement::count_observation_copies(|| {
            observe(|| {
                db.execute_at_snapshot(
                    "SELECT id FROM inactive_copy ORDER BY v <=> $v USE VECTOR EXACT LIMIT 1",
                    &params,
                    snapshot,
                )
            })
        });
        assert_eq!(answer.unwrap().rows()[0][0], Value::Int64(0));
        assert!(
            work.membership_keys_examined >= n,
            "actual native membership budget path"
        );
        counts.push(copies);
    }
    eprintln!("inactive full Observation copies N/2N: {counts:?}");
    assert_eq!(
        counts,
        [0, 0],
        "inactive SQL must not copy a full Observation"
    );
}
