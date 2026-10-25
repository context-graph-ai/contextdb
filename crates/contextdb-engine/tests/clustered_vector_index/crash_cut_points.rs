//! Publishing a rebuilt clustered generation is atomic across a crash. A
//! crash before the new generation is published leaves the old complete
//! generation serving, together with every vector committed after it was
//! built; a crash after publication leaves the new complete generation. On
//! reopen every committed row is found through the index, so no
//! half-published state is ever read, and recovery then finishes the work.
//!
//! The child process stops at a real publication boundary and is killed
//! there; nothing in it is allowed to run past the boundary.

use super::support::{
    PublicationCut, count, drive_until_serving, empty, int_ids, ivf, observe_publication_cuts,
    observe_search, open_caller_driven, params, serving, unit_vector,
};
use contextdb_core::{Value, Wallclock};
use contextdb_engine::Database;
use std::io::{BufRead, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;

const CHILD_PATH: &str = "CONTEXTDB_CLUSTERED_PUBLICATION_CHILD_PATH";
const CHILD_CUT: &str = "CONTEXTDB_CLUSTERED_PUBLICATION_CHILD_CUT";
const DIMENSIONS: usize = 8;
const BUILT_ROWS: i64 = 40;
const DELTA_ROWS: i64 = 8;
const MARKER: &str = "CLUSTERED_PUBLICATION_CUT";
const FINISHED: &str = "CLUSTERED_PUBLICATION_FINISHED";

fn insert(db: &Database, id: i64) {
    db.execute(
        "INSERT INTO published_items (id, embedding) VALUES ($id, $embedding)",
        &params([
            ("id", Value::Int64(id)),
            (
                "embedding",
                Value::Vector(unit_vector(id as u64, DIMENSIONS)),
            ),
        ]),
    )
    .expect("commit one vector");
}

/// Every committed row is found by an indexed search for its own vector, on
/// the clustered route.
fn assert_every_row_is_served_by_the_index(db: &Database, case: &str) {
    assert_eq!(
        count(db, "published_items"),
        BUILT_ROWS + DELTA_ROWS,
        "{case}: every committed row survives"
    );
    for id in 0..BUILT_ROWS + DELTA_ROWS {
        let (result, work) = observe_search(db, || {
            db.execute(
                "SELECT id FROM published_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
                &params([("query", Value::Vector(unit_vector(id as u64, DIMENSIONS)))]),
            )
            .expect("indexed search after reopen")
        });
        assert!(
            work.clustered_route,
            "{case}: row {id} was searched on the clustered index"
        );
        assert_eq!(
            int_ids(&result),
            vec![id],
            "{case}: row {id} is served by the index"
        );
    }
}

fn child_filter() -> String {
    let module = module_path!()
        .split_once("::")
        .map(|(_, rest)| rest)
        .unwrap_or(module_path!());
    format!("{module}::a_crash_at_either_publication_boundary_leaves_one_complete_generation")
}

fn run_child(path: &Path, selected: &str) {
    let db = open_caller_driven(path);
    let selected = selected.to_owned();
    observe_publication_cuts(
        Arc::new(move |cut| {
            if format!("{cut:?}") == selected {
                println!("{MARKER} {cut:?}");
                std::io::stdout().flush().unwrap();
                let mut byte = [0];
                std::io::stdin()
                    .read_exact(&mut byte)
                    .expect("the parent kills the paused child before releasing stdin");
            }
        }),
        || drive_until_serving(&db),
    );
    db.close().unwrap();
    println!("{FINISHED}");
}

/// Builds generation one over `BUILT_ROWS`, then commits `DELTA_ROWS` more:
/// enough change for the declared consolidation to schedule a replacement.
fn prepare(path: &Path) -> u64 {
    let db = open_caller_driven(path);
    db.execute(
        &format!(
            "CREATE TABLE published_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE INDEXED {} CONSOLIDATION (CHANGE_PERCENT = 10))",
            ivf(4, 4, 64)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
    for id in 0..BUILT_ROWS {
        insert(&db, id);
    }
    drive_until_serving(&db);
    let first = serving(&db, "published_items", "embedding", &[])
        .expect("the first generation is serving")
        .generation;
    for id in BUILT_ROWS..BUILT_ROWS + DELTA_ROWS {
        insert(&db, id);
    }
    db.close().expect("close the prepared store");
    first
}

#[test]
fn a_crash_at_either_publication_boundary_leaves_one_complete_generation() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        run_child(Path::new(&path), &std::env::var(CHILD_CUT).unwrap());
        return;
    }
    for selected in [
        PublicationCut::BeforeGenerationPublication,
        PublicationCut::AfterGenerationPublication,
    ]
    .map(|cut| format!("{cut:?}"))
    .into_iter()
    .chain(["FinishedControl".to_owned()])
    {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("published.db");
        let first = prepare(&path);

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &child_filter(),
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_PATH, &path)
            .env(CHILD_CUT, &selected)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdout = std::io::BufReader::new(child.stdout.take().unwrap());
        let mut transcript = String::new();
        let mut reached = false;
        loop {
            let mut line = String::new();
            if stdout.read_line(&mut line).unwrap() == 0 {
                break;
            }
            transcript.push_str(&line);
            if line.trim() == format!("{MARKER} {selected}") {
                reached = true;
                break;
            }
        }
        if selected == "FinishedControl" {
            let status = child.wait().unwrap();
            assert!(
                status.success() && transcript.contains(FINISHED),
                "the uninterrupted child finishes the replacement: {transcript}"
            );
        } else {
            assert!(
                reached,
                "the replacement reached the {selected} boundary: {transcript}"
            );
            child.kill().unwrap();
            child.wait().unwrap();
        }

        let db = open_caller_driven(&path);
        let after_crash = serving(&db, "published_items", "embedding", &[])
            .expect("a complete generation is serving after reopen");
        match selected.as_str() {
            "BeforeGenerationPublication" => assert_eq!(
                after_crash.generation, first,
                "a crash before publication leaves the old generation serving"
            ),
            _ => assert_ne!(
                after_crash.generation, first,
                "{selected}: the published generation is the one serving"
            ),
        }
        assert_every_row_is_served_by_the_index(&db, &selected);

        drive_until_serving(&db);
        assert_ne!(
            serving(&db, "published_items", "embedding", &[])
                .expect("recovery leaves a serving generation")
                .generation,
            first,
            "{selected}: recovery finishes the scheduled replacement"
        );
        assert_every_row_is_served_by_the_index(&db, &format!("{selected} after recovery"));
        db.close().unwrap();
    }
}
