//! A query's examined-row count is that query's, whoever else is reading.
//!
//! `rows_examined` is the figure an operator reads to judge a query: did this
//! statement read three rows or three million. It is only worth reading if it
//! describes the statement it is attached to and nothing else. A database is
//! read by many callers at once, so the count has to be kept per statement --
//! a database-wide counter cannot answer "how much did THIS query read",
//! because it is added to by every statement running against the handle and
//! reset by whichever one most recently started.
//!
//! This holds one statement still INSIDE its own drain, after its sources have
//! begun to read, and runs a second statement over a different table start to
//! finish while it waits. The paused statement then finishes. Each must report
//! its own table's row count: neither may fold in the other's reading, and
//! neither may lose its own.

#![cfg(feature = "test-seams")]

use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::database::{ReadExecutionConvergenceEvent, ReadExecutionConvergenceObserver};
use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// The larger table: the statement that is held still reads this one.
const HELD_ROWS: u64 = 7;
/// The smaller table: the statement that runs to completion meanwhile.
const OVERLAPPING_ROWS: u64 = 3;

/// Hold the first statement once its sources have already read, not on the
/// very first event -- a source touch is announced before the touch it
/// describes, so waiting for the second announcement means work has genuinely
/// been charged to the held statement before the other one starts.
const TOUCHES_BEFORE_HOLDING: u64 = 2;

/// Long enough that a loaded machine still reaches the hold, short enough that
/// a statement which never reaches it fails instead of hanging.
const PATIENCE: Duration = Duration::from_secs(30);

#[derive(Default)]
struct HoldState {
    touches: u64,
    held: bool,
    released: bool,
}

/// Stops a statement in the middle of its drain and keeps it there until it is
/// let go.
#[derive(Default)]
struct HoldInsideTheDrain {
    state: Mutex<HoldState>,
    changed: Condvar,
}

impl HoldInsideTheDrain {
    fn wait_until_held(&self) -> bool {
        let mut state = self.state.lock().expect("hold state");
        while !state.held {
            let (next, timeout) = self
                .changed
                .wait_timeout(state, PATIENCE)
                .expect("hold state");
            state = next;
            if timeout.timed_out() {
                return state.held;
            }
        }
        true
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("hold state");
        state.released = true;
        self.changed.notify_all();
    }
}

impl ReadExecutionConvergenceObserver for HoldInsideTheDrain {
    fn observe(&self, event: ReadExecutionConvergenceEvent) {
        if !matches!(
            event,
            ReadExecutionConvergenceEvent::PullKernelSourceTouch { .. }
        ) {
            return;
        }
        let mut state = self.state.lock().expect("hold state");
        state.touches += 1;
        if state.held || state.touches < TOUCHES_BEFORE_HOLDING {
            return;
        }
        state.held = true;
        self.changed.notify_all();
        while !state.released {
            let (next, timeout) = self
                .changed
                .wait_timeout(state, PATIENCE)
                .expect("hold state");
            state = next;
            if timeout.timed_out() && !state.released {
                return;
            }
        }
    }
}

fn params() -> HashMap<String, Value> {
    HashMap::new()
}

fn execute(database: &Database, sql: &str) -> contextdb_engine::QueryResult {
    database
        .execute(sql, &params())
        .unwrap_or_else(|error| panic!("{sql} must succeed: {error}"))
}

fn create_and_seed(database: &Database, table: &str, rows: u64) {
    execute(
        database,
        &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, body TEXT)"),
    );
    for id in 0..rows {
        execute(
            database,
            &format!("INSERT INTO {table} (id, body) VALUES ({id}, 'row-{id}')"),
        );
    }
}

#[test]
fn a_statement_held_inside_its_drain_reports_only_the_rows_it_read_itself() {
    let database = Arc::new(Database::open_memory());
    create_and_seed(&database, "held_scan", HELD_ROWS);
    create_and_seed(&database, "overlapping_scan", OVERLAPPING_ROWS);

    let hold = Arc::new(HoldInsideTheDrain::default());
    let held_statement = {
        let database = Arc::clone(&database);
        let hold = Arc::clone(&hold);
        std::thread::spawn(move || {
            database.with_read_execution_convergence_observer_for_test(
                Arc::clone(&hold) as Arc<dyn ReadExecutionConvergenceObserver>,
                || {
                    execute(&database, "SELECT id FROM held_scan")
                        .trace
                        .rows_examined
                },
            )
        })
    };

    let reached = hold.wait_until_held();
    // The second statement runs start to finish while the first is stopped
    // partway through reading its own table.
    let overlapping = execute(&database, "SELECT id FROM overlapping_scan");
    hold.release();
    let held = held_statement.join().expect("held statement thread");

    assert!(
        reached,
        "the first statement must stop inside its drain, after its sources have \
         read, so the second statement genuinely overlaps it"
    );
    assert_eq!(
        held, HELD_ROWS,
        "the statement over the {HELD_ROWS}-row table must report the rows it \
         read itself, not a figure another statement moved while it waited"
    );
    assert_eq!(
        overlapping.trace.rows_examined, OVERLAPPING_ROWS,
        "the statement over the {OVERLAPPING_ROWS}-row table must report the \
         rows it read itself, with none of the held statement's reading in it"
    );
}
