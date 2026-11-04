//! A push refuses, with the same typed error as before, when the evidence of a
//! received group it would otherwise drop is corrupt or incomplete. Reaching
//! that refusal walks the history only as far as the planted fault, lets no
//! droppable received group reach the final filter, and holds no validation
//! records for dropped history; nightly, it costs the same memory however
//! much received history comes before it. After a move, a declaration
//! authored locally above the move's frontier still refuses for lack of
//! origin evidence.
#![expect(
    clippy::result_large_err,
    reason = "The measured operations return the public Error, which keeps inline NativeRefusal diagnostics."
)]
use super::outbound_declaration_walk_support::*;
use contextdb_core::{Error, Lsn};
use contextdb_engine::database::DdlEvidenceEditForTest as Edit;

#[derive(Debug, Clone, Copy)]
enum Fault {
    ArrivalDigest,
    GenerationSidecarAltered,
    GenerationSidecarRemoved,
    PartlyReceivedAtTheLastOccurrence,
    PartlyReceivedAtTheFirstOccurrence,
    SupersededDigest,
    PartlySupersededAtTheLastOccurrence,
    PartlySupersededAtTheFirstOccurrence,
}

const FAULTS: [Fault; 8] = [
    Fault::ArrivalDigest,
    Fault::GenerationSidecarAltered,
    Fault::GenerationSidecarRemoved,
    Fault::PartlyReceivedAtTheLastOccurrence,
    Fault::PartlyReceivedAtTheFirstOccurrence,
    Fault::SupersededDigest,
    Fault::PartlySupersededAtTheLastOccurrence,
    Fault::PartlySupersededAtTheFirstOccurrence,
];

/// Plant `fault` on the received group at `lsn` of `members` occurrences and
/// return the parent's refusal text.
fn plant(db: &contextdb_engine::Database, (lsn, members): (Lsn, u32), fault: Fault) -> String {
    let last = members - 1;
    let edit = |ordinal, edit| {
        db.edit_ddl_occurrence_evidence_for_test(lsn, ordinal, edit)
            .unwrap_or_else(|error| panic!("{fault:?}: plant {edit:?}: {error:?}"))
    };
    match fault {
        Fault::ArrivalDigest => {
            edit(last, Edit::CorruptArrivalDigest);
            "received DDL arrival digest does not bind its local schema occurrence".into()
        }
        Fault::GenerationSidecarAltered => {
            edit(last, Edit::AlterGenerationSidecar);
            "received DDL arrival digest does not bind its local schema occurrence".into()
        }
        Fault::GenerationSidecarRemoved => {
            edit(last, Edit::RemoveGenerationSidecar);
            format!(
                "DDL at source LSN {} ordinal {last} lacks immutable generation evidence",
                lsn.0
            )
        }
        Fault::PartlyReceivedAtTheLastOccurrence => {
            edit(last, Edit::RemoveArrival);
            "received DDL arrival evidence marks only part of one local schema commit".into()
        }
        Fault::PartlyReceivedAtTheFirstOccurrence => {
            edit(0, Edit::RemoveArrival);
            "received DDL arrival evidence marks only part of one local schema commit".into()
        }
        Fault::SupersededDigest => {
            for ordinal in 0..members {
                edit(ordinal, Edit::MarkSuperseded);
            }
            edit(last, Edit::CorruptSupersededDigest);
            "superseded DDL evidence does not bind its local schema occurrence".into()
        }
        Fault::PartlySupersededAtTheLastOccurrence => {
            edit(last, Edit::MarkSuperseded);
            "superseded DDL evidence marks only part of one local schema commit".into()
        }
        Fault::PartlySupersededAtTheFirstOccurrence => {
            edit(0, Edit::MarkSuperseded);
            "superseded DDL evidence marks only part of one local schema commit".into()
        }
    }
}

fn assert_refused<T: std::fmt::Debug>(
    what: &str,
    size: usize,
    result: &Result<T, Error>,
    expected: &str,
) {
    match result {
        Err(Error::SyncError(text)) => assert!(
            text.to_string().contains(expected),
            "{what} at {size}: refused with the parent's text {expected:?}, got {text:?}"
        ),
        other => panic!("{what} at {size}: expected the typed refusal {expected:?}, got {other:?}"),
    }
}

/// Every fault at `size`; returns (size, LSN, peak) per fault.
fn corrupt_received_group(size: usize, nightly: bool) -> Vec<(usize, Lsn, usize)> {
    let _clock = fixed_clock();
    let runtime = runtime();
    let base = tempfile::tempdir().unwrap();
    pulled_edge(
        &runtime,
        base.path(),
        size,
        nightly_pad(size, nightly),
        &|_| {},
    )
    .close(&runtime);
    let mut samples = Vec::new();
    for fault in FAULTS {
        let directory = tempfile::tempdir().unwrap();
        copy_setup(base.path(), directory.path());
        let network = Network::connect(
            &runtime,
            Peer::reopen(directory.path(), "edge"),
            Peer::reopen(directory.path(), "hub"),
        );
        let group = last_multi_occurrence_received_group(&network.edge.db);
        let refusal = plant(&network.edge.db, group, fault);
        sql(&network.edge.db, "INSERT INTO notes VALUES (2, 'second')");
        let expected = expected_walk(
            &occurrences(&network.edge.db),
            network.client.push_watermark(),
            None,
        );
        let hub_schema = occurrences(&network.hub.db);
        let pushed = measure(&network.edge.db, || runtime.block_on(network.client.push()));
        let what = format!("{fault:?}");
        receipt(&what, size, &pushed);
        assert_refused(&what, size, &pushed.value, &refusal);
        assert_eq!(
            occurrences(&network.hub.db),
            hub_schema,
            "{what}: a refused push sends nothing"
        );
        assert_eq!(ids(&network.hub.db, "notes"), vec![1]);
        if !nightly {
            assert_one_epoch(&what, &pushed.work, None, false);
        }
        assert_sent(&what, &pushed.work, &[]);
        if !nightly {
            assert_refused_walk(&what, &pushed.work, &expected, (group.0, group.1 - 1));
        }
        samples.push((size, pushed.lsn, pushed.peak));
        network.close(&runtime);
    }
    samples
}

#[test]
fn a_corrupt_received_declaration_is_refused_without_holding_the_received_history() {
    corrupt_received_group(PR_SIZE, false);
}

#[cfg(feature = "nightly-tests")]
#[test]
fn a_corrupt_received_declaration_is_refused_without_holding_the_received_history_as_received_history_grows()
 {
    let [small, large] = NIGHTLY_SIZES.map(|size| corrupt_received_group(size, true));
    let [small_control, large_control] =
        NIGHTLY_SIZES.map(|size| control_sample(size, || corrupt_received_group(PR_SIZE, true)));
    for (case, fault) in FAULTS.into_iter().enumerate() {
        assert_bounded_by_control(
            &format!("{fault:?} refusal"),
            &[small[case], large[case]],
            &[small_control[case], large_control[case]],
            BallastIn::NoWalk,
        );
    }
}

fn authored_after_move(size: usize, nightly: bool) -> (usize, Lsn, usize) {
    let _clock = fixed_clock();
    let runtime = runtime();
    let directory = tempfile::tempdir().unwrap();
    let moved = moved_edge(
        &runtime,
        directory.path(),
        size,
        Prefix::AuthoredByTheEdge,
        false,
        &|edge| {
            sql(
                edge,
                "CREATE TABLE after_move (id INTEGER PRIMARY KEY) SYNC TWO WAY",
            );
        },
    );
    let network = &moved.network;
    let history = occurrences(&network.edge.db);
    let authored = history
        .iter()
        .rev()
        .find(|occurrence| names(&occurrence.ddl, "after_move"))
        .expect("the edge authored after_move")
        .clone();
    assert!(!authored.received && authored.lsn > moved.frontier);
    let expected = expected_walk(&history, REUPLOAD_PUSH_SINCE, Some(moved.frontier));
    assert_eq!(
        expected.dropped.len(),
        size,
        "exactly the second hub's declarations are droppable: {expected:?}"
    );
    assert!(
        expected
            .collected
            .contains(&(authored.lsn, authored.ordinal)),
        "an unmarked declaration above F reaches the final filter, which refuses it"
    );
    let hub_schema = occurrences(&network.hub.db);
    let pushed = measure(&network.edge.db, || runtime.block_on(network.client.push()));
    receipt("authored_after_move", size, &pushed);
    assert_refused(
        "authored after move",
        size,
        &pushed.value,
        &format!(
            "schema published after the move at source LSN {} has no durable origin evidence",
            authored.lsn.0
        ),
    );
    assert_eq!(occurrences(&network.hub.db), hub_schema);
    if !nightly {
        assert_one_epoch(
            "authored after move",
            &pushed.work,
            Some(moved.frontier),
            false,
        );
    }
    assert_sent("authored after move", &pushed.work, &[]);
    if !nightly {
        assert_walk("authored after move", &pushed.work, &expected);
    }
    let sample = (size, pushed.lsn, pushed.peak);
    moved.network.close(&runtime);
    sample
}

#[test]
fn a_declaration_authored_after_a_move_is_refused_without_holding_the_received_history() {
    authored_after_move(PR_SIZE, false);
}

#[cfg(feature = "nightly-tests")]
#[test]
fn a_declaration_authored_after_a_move_is_refused_without_holding_the_received_history_as_received_history_grows()
 {
    let measured = NIGHTLY_SIZES.map(|size| authored_after_move(size, true));
    let control =
        NIGHTLY_SIZES.map(|size| control_sample(size, || authored_after_move(PR_SIZE, true)));
    assert_bounded_by_control(
        "refusal of a declaration authored after a move",
        &measured,
        &control,
        BallastIn::TheWalk,
    );
}
