//! A push refuses, with the same typed error as before, when the evidence of a
//! received group it would otherwise suppress is corrupt or incomplete, and
//! reaching that refusal costs the same memory however much received history
//! comes before it. After a move, a declaration authored locally above the
//! move's frontier still refuses for lack of origin evidence.
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
}

const FAULTS: [Fault; 7] = [
    Fault::ArrivalDigest,
    Fault::GenerationSidecarAltered,
    Fault::GenerationSidecarRemoved,
    Fault::PartlyReceivedAtTheLastOccurrence,
    Fault::PartlyReceivedAtTheFirstOccurrence,
    Fault::SupersededDigest,
    Fault::PartlySupersededAtTheLastOccurrence,
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

#[test]
fn a_corrupt_received_declaration_is_refused_without_holding_the_received_history() {
    let _clock = fixed_clock();
    let runtime = runtime();
    let mut samples = vec![Vec::new(); FAULTS.len()];
    let mut works = Vec::new();
    for size in SIZES {
        let base = tempfile::tempdir().unwrap();
        pulled_edge(&runtime, base.path(), size, &|_| {}).close(&runtime);
        for (case, fault) in FAULTS.into_iter().enumerate() {
            let directory = tempfile::tempdir().unwrap();
            copy_setup(base.path(), directory.path());
            let network = Network::connect(
                &runtime,
                Peer::reopen(directory.path(), "edge"),
                Peer::reopen(directory.path(), "hub"),
            );
            let group = last_multi_occurrence_received_group(&network.edge.db);
            let expected = plant(&network.edge.db, group, fault);
            sql(&network.edge.db, "INSERT INTO notes VALUES (2, 'second')");
            let hub_schema = occurrences(&network.hub.db);
            let pushed = measure(&network.edge.db, || runtime.block_on(network.client.push()));
            let what = format!("{fault:?}");
            receipt(&what, size, &pushed);
            assert_refused(&what, size, &pushed.value, &expected);
            assert_eq!(
                occurrences(&network.hub.db),
                hub_schema,
                "{what}: a refused push sends nothing"
            );
            assert_eq!(ids(&network.hub.db, "notes"), vec![1]);
            samples[case].push(comparable(&pushed));
            works.push((what, size, pushed.work));
            network.close(&runtime);
        }
    }
    for (case, fault) in FAULTS.into_iter().enumerate() {
        assert_same_across_sizes(&format!("{fault:?} refusal"), &samples[case]);
    }
    for (what, size, work) in works {
        assert_eq!(
            work.suppressed_output_peak_bytes, 0,
            "{what} at {size}: no received declaration sits in outbound output before the refusal: {work:?}"
        );
        assert!(work.suppressed_held_peak <= 1, "{what} at {size}: {work:?}");
        assert_no_fallback(&what, size, &work);
    }
}

#[test]
fn a_declaration_authored_after_a_move_is_refused_without_holding_the_received_history() {
    let _clock = fixed_clock();
    let runtime = runtime();
    let mut samples = Vec::new();
    let mut works = Vec::new();
    for size in SIZES {
        let directory = tempfile::tempdir().unwrap();
        let moved = moved_edge(
            &runtime,
            directory.path(),
            size,
            Prefix::AuthoredByTheEdge,
            &|edge| {
                sql(
                    edge,
                    "CREATE TABLE after_move (id INTEGER PRIMARY KEY) SYNC TWO WAY",
                );
            },
        );
        let network = &moved.network;
        let authored = occurrences(&network.edge.db)
            .into_iter()
            .rev()
            .find(|occurrence| names(&occurrence.ddl, "after_move"))
            .expect("the edge authored after_move");
        assert!(!authored.received && authored.lsn > moved.frontier);
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
        samples.push(comparable(&pushed));
        works.push((size, pushed.work));
        moved.network.close(&runtime);
    }
    assert_same_across_sizes("refusal of a declaration authored after a move", &samples);
    for (size, work) in works {
        assert_eq!(work.suppressed_output_peak_bytes, 0, "at {size}: {work:?}");
        assert!(work.suppressed_held_peak <= 1, "at {size}: {work:?}");
        assert_no_fallback("authored after move", size, &work);
    }
}
