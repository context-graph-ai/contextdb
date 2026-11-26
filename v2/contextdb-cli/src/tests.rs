use contextdb::{Database, DatabaseOpenOptions, Error, Maintenance, ReadLimits, Value, params};
use contextdb::{StorageBackend, WrapStorage};
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Mutex, mpsc};

use crate::input::{Scan, scan};
use crate::output::{Output, table};
use crate::shell::{Shell, Store};

mod report {
    use super::*;
    pub enum Call {
        Read,
        Sync,
        Other,
    }
    struct Watched {
        file: std::fs::File,
        watch: Arc<dyn Fn(Call) -> std::io::Result<()> + Send + Sync>,
    }
    impl std::fmt::Debug for Watched {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "watched {:?}", self.file)
        }
    }
    impl StorageBackend for Watched {
        fn len(&self) -> std::io::Result<u64> {
            Ok(self.file.metadata()?.len())
        }
        fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
            self.file.read_exact_at(out, offset)?;
            (self.watch)(Call::Read)
        }
        fn set_len(&self, len: u64) -> std::io::Result<()> {
            (self.watch)(Call::Other)?;
            self.file.set_len(len)
        }
        fn sync_data(&self) -> std::io::Result<()> {
            (self.watch)(Call::Sync)?;
            self.file.sync_data()
        }
        fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
            (self.watch)(Call::Other)?;
            self.file.write_all_at(data, offset)
        }
        fn close(&self) -> std::io::Result<()> {
            (self.watch)(Call::Other)
        }
    }
    pub fn storage(watch: impl Fn(Call) + Send + Sync + 'static) -> WrapStorage {
        failing_storage(move |call| {
            watch(call);
            Ok(())
        })
    }
    pub fn failing_storage(
        watch: impl Fn(Call) -> std::io::Result<()> + Send + Sync + 'static,
    ) -> WrapStorage {
        let watch: Arc<dyn Fn(Call) -> std::io::Result<()> + Send + Sync> = Arc::new(watch);
        Arc::new(move |file| {
            Ok(Box::new(Watched {
                file,
                watch: watch.clone(),
            }) as Box<dyn StorageBackend>)
        })
    }
}
const OUTPUT: Output = Output {
    json: false,
    read: None,
    result: u64::MAX,
};

#[test]
fn a_statement_ends_at_the_first_semicolon_outside_quotes_and_comments() {
    assert_eq!(scan("SELECT 1; SELECT 2;"), Scan::Ends(9));
    assert_eq!(scan("SELECT ';' -- ;\n, \"a;b\" /* ; */ ;"), Scan::Ends(33));
    assert_eq!(scan("SELECT 'it''s; here'"), Scan::Open);
    assert_eq!(scan("SELECT 1\n"), Scan::Open);
    assert_eq!(scan("/* not closed ;"), Scan::Open);
    assert_eq!(scan("  -- done\n/* and */\n"), Scan::Blank);
    assert_eq!(scan(""), Scan::Blank);
}

#[test]
fn a_table_is_as_wide_as_its_widest_cell() {
    let columns = ["id".to_owned(), "body".to_owned()];
    let rows = [
        vec![Value::Int64(1), Value::Text("première".to_owned())],
        vec![Value::Int64(20), Value::Null],
    ];
    assert_eq!(
        table(&columns, &rows),
        "\
+----+----------+
| id | body     |
+----+----------+
| 1  | première |
| 20 | NULL     |
+----+----------+
(2 rows)
"
    );
    assert_eq!(table(&columns, &[]), "(0 rows)\n");
}

#[test]
fn the_read_options_default_to_the_documented_values() {
    use clap::Parser;
    let args = crate::Args::parse_from(["contextdb", "store"]);
    let mut lifted = ReadLimits::default();
    (lifted.result_rows, lifted.result_bytes) = (u64::MAX, u64::MAX);
    lifted.caller_checks_metadata = true;
    let (shell, lifted) = (format!("{:?}", args.limits()), format!("{lifted:?}"));
    assert_eq!(shell, lifted, "M6 lifts only the row and byte ceilings");
    assert_eq!(args.read_hydration_notice_ms, 1000, "notice threshold");
}

#[test]
fn a_terminal_session_says_what_it_is_and_exits_zero_after_a_refusal() {
    use crate::shell::{banner, prompt, succeeded};
    let said = |w: bool| format!("{:?}", [banner(w), prompt(w, false), prompt(w, true)]);
    let rw = r#"["write session — mutations are enabled", "contextdb> ", "      ...> "]"#;
    let ro = r#"["read-only session — pass --write to mutate", "contextdb(ro)> ", "      ...> "]"#;
    assert_eq!(said(true), rw, "the write session's texts");
    assert_eq!(said(false), ro, "the read session's texts");
    // (terminal, refused, failed): only a pipe fails after a refusal.
    let rows = [(1, 1, 0), (1, 0, 1), (0, 1, 0), (0, 0, 1), (0, 0, 0)];
    for ((t, r, f), ok) in rows.into_iter().zip([true, false, false, false, true]) {
        assert_eq!(succeeded(t > 0, r > 0, f > 0), ok, "{t}{r}{f}");
    }
}

#[test]
fn a_refusal_is_put_in_the_class_of_what_the_caller_can_do() {
    use contextdb::Error;
    // The owner channel's refusals are io, as the store's are.
    for (error, wanted) in [
        (Error::OwnerNotRunning, "io"),
        (Error::OwnerNotServing, "io"),
        (
            Error::OwnerRefused {
                reason: "busy".into(),
            },
            "io",
        ),
        (Error::OwnerLimitExceeded, "io"),
        (Error::OwnerDisconnected, "io"),
    ] {
        assert_eq!(crate::output::class(&error), wanted, "{error:?}");
    }
}

/// A writing store of more than one page of rows, reopened cold on `door`.
fn store(name: &str, door: WrapStorage) -> Store {
    let path = std::env::temp_dir().join(format!("contextdb-cli-{name}-{}", std::process::id()));
    let db = Database::open(&path).unwrap();
    let rows: Vec<String> = (0..300)
        .map(|id| format!("({id}, '{:x<100}')", ""))
        .collect();
    let insert = format!("INSERT INTO t (id, v) VALUES {}", rows.join(", "));
    for sql in ["CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &insert] {
        db.execute(sql, &params! {}).unwrap();
    }
    db.close().unwrap();
    drop(db);
    let mut options = DatabaseOpenOptions::default();
    (options.maintenance, options.storage) = (Maintenance::Caller, Some(door));
    let writer = Database::open_with_options(&path, options).unwrap();
    let reader = Arc::new(writer.read_session(ReadLimits::default()).unwrap());
    let mode = Maintenance::Caller;
    Store {
        reader,
        writer: Some(writer),
        mode,
    }
}

fn sql(store: &Store, sql: &str) -> contextdb::QueryResult {
    store
        .writer
        .as_ref()
        .unwrap()
        .execute(sql, &params! {})
        .unwrap()
}

#[test]
fn a_final_sync_the_drive_refuses_is_the_runs_failure() {
    let armed = Arc::new(AtomicBool::new(false));
    let refusing = armed.clone();
    let door = report::failing_storage(move |call| match (refusing.load(SeqCst), call) {
        (true, report::Call::Sync) => Err(std::io::Error::other("refused the final sync")),
        _ => Ok(()),
    });
    let store = store("final-sync", door);
    sql(&store, "INSERT INTO t (id, v) VALUES (300, 'y')");
    armed.store(true, SeqCst);
    let finished = crate::finish(store, true, OUTPUT);
    assert!(
        matches!(&finished, Err(Error::StoreIo { reason, .. }) if reason.contains("final sync")),
        "finish returns the refused final sync, on which main exits 1: {finished:?}"
    );
}

/// Runs `before`, then `held` with its first read held until the interrupt has been fired, then
/// `after`, through one shell. Gives whether the held line ended the run, whether the run is
/// marked interrupted, and the rows that hold 'y' after.
fn interrupted(name: &str, terminal: bool, lines: [&[&str]; 3]) -> (bool, bool, Value) {
    let armed = Arc::new(AtomicBool::new(false));
    let (reached, at) = mpsc::channel::<bool>();
    let (go, gate) = mpsc::channel(); // lets the held read go once the interrupt was fired
    let (hold, gate, ended) = (armed.clone(), Mutex::new(gate), reached.clone());
    let door = report::storage(move |call| {
        if matches!(call, report::Call::Read) && hold.swap(false, SeqCst) {
            reached.send(true).unwrap();
            gate.lock().unwrap().recv().unwrap();
        }
    });
    let store = store(name, door);
    let mut shell = Shell::new(&store, OUTPUT, false, terminal);
    let [before, [held], after] = lines else {
        unreachable!()
    };
    let goes_on =
        |shell: &mut Shell, line: &&str| assert!(shell.line(line).is_continue(), "{line}");
    before.iter().for_each(|line| goes_on(&mut shell, line));
    armed.store(true, SeqCst);
    let (flow, fired) = std::thread::scope(|scope| {
        let firing = scope.spawn(move || {
            at.recv().unwrap() && {
                crate::interrupt::fire();
                go.send(()).is_ok()
            }
        });
        let flow = shell.line(held);
        let _ = ended.send(false); // nothing waits on it once the read was held
        (flow, firing.join().unwrap())
    });
    assert!(
        fired,
        "{name}: the statement read the table, and was held there"
    );
    after.iter().for_each(|line| goes_on(&mut shell, line));
    let marked = shell.interrupted;
    drop(shell);
    (
        flow.is_break(),
        marked,
        sql(&store, "SELECT COUNT(*) FROM t WHERE v = 'y'").rows()[0][0].clone(),
    )
}

#[test]
fn an_interrupt_cancels_the_running_statement_and_a_pipe_ends_the_run() {
    let select = "SELECT COUNT(*) FROM t WHERE v <> 'y';";
    let (insert, update) = (
        "INSERT INTO t (id, v) VALUES (300, 'y');",
        "UPDATE t SET v = 'y';",
    );
    let answers = [
        interrupted(
            "prompt",
            true,
            [&["BEGIN;"], &[select], &[insert, "COMMIT;"]],
        ),
        interrupted("piped-select", false, [&["BEGIN;"], &[select], &[]]),
        interrupted("piped-update", false, [&[], &[update], &[]]),
    ];
    // (ended the run, marked interrupted, rows with 'y'): at a prompt the shell goes on and the
    // transaction's COMMIT keeps its row; piped, the run ends marked; the cancelled UPDATE changes
    // no row.
    let (one, none) = (Value::Int64(1), Value::Int64(0));
    let wanted = [
        (false, false, one),
        (true, true, none.clone()),
        (true, true, none),
    ];
    assert_eq!(
        answers, wanted,
        "(ended, marked, rows with 'y') for prompt, piped SELECT, UPDATE"
    );
}

#[test]
fn a_progress_notice_is_throttled_by_the_hydration_notice_threshold() {
    use crate::progress::Progress;
    use contextdb::{ReadPhase, ReadProgress, ReadProgressObserver};
    let output = Output {
        json: false,
        read: None,
        result: 0,
    };
    let at = |active_ms| ReadProgress {
        phase: ReadPhase::Executing,
        rows: 0,
        bytes: 0,
        work: 0,
        active_ms,
    };
    let progress = Progress {
        output,
        threshold: 3,
        next: std::sync::atomic::AtomicU64::new(3),
    };
    let next = || progress.next.load(SeqCst);
    progress.progress(at(2));
    assert_eq!(next(), 3, "below the threshold no notice is sent");
    progress.progress(at(5));
    assert_eq!(
        next(),
        8,
        "at or after the threshold a notice is sent, the next a threshold on"
    );
    progress.progress(at(6));
    assert_eq!(
        next(),
        8,
        "no second notice before another threshold has passed"
    );
    progress.progress(at(8));
    assert_eq!(
        next(),
        11,
        "a notice again once another threshold has passed"
    );
    progress.completed(at(9));
    assert_eq!(
        next(),
        3,
        "a finished statement re-arms the first threshold"
    );
}
