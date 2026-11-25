//! The observer and cancellation cannot be expressed by a SQL script; extends readers.rs.
#[path = "support/readmem.rs"]
mod readmem;
mod support;
use contextdb::{
    Database, DatabaseOpenOptions, DatabasePlugin, Error, QueryOutcome, ReadLimits, ReadProgress,
    ReadProgressObserver, params,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Barrier, Mutex, Weak};
use support::Observer;
#[test]
fn progress_reports_the_read_and_time_or_cancel_ends_only_its_statement() {
    let (db, now) = readmem::clocked(&support::folder("read-progress"), 8192);
    support::declare_docs(&db, 2, "");
    support::load_docs(&db, 1..=8192, "c", |n| support::vector(n, 2));
    let search = support::by_mode(None, "EXACT", 4);
    let scan = ("SELECT id FROM t", params! {}, 8192);
    let nearest = (search.as_str(), params! { "q" => vec![1.0_f32, 1.0] }, 4);
    for ((sql, q, rows), (advance, cancel)) in [scan, nearest].into_iter().flat_map(|statement| {
        [(7, false), (8, false), (0, true)].map(|round| (statement.clone(), round))
    }) {
        now.store(0, SeqCst);
        let observer = Observer::new(now.clone(), advance, cancel);
        let limits = ReadLimits {
            result_rows: 10_000,
            active_ms: 7,
            ..ReadLimits::default()
        };
        let read = db.read_session_with_progress(limits, observer.clone());
        let session = Arc::new(read.unwrap());
        *observer.session.lock().unwrap() = Arc::downgrade(&session);
        let result = session.execute(sql, &q);
        if cancel || advance > 7 {
            let error = result.unwrap_err();
            assert!(matches!(error, Error::QueryCanceled { .. }));
            let text = error.to_string();
            assert!(text.contains("57014"), "{text}");
            let reason = if cancel {
                "due to user request"
            } else {
                "due to statement timeout"
            };
            assert!(text.contains(reason), "{text}");
            if !cancel {
                assert!(text.contains("statement_timeout"), "{text}");
            }
        } else {
            assert_eq!(result.unwrap().rows().len(), rows);
        }
        let samples = observer.samples.lock().unwrap();
        assert!(!samples.is_empty(), "no inline progress");
        assert!(samples.iter().any(|p| p[2] >= 1024));
        if !cancel && advance == 7 {
            assert!(samples.iter().any(|p| p[0] > 0 && p[1] > 0 && p[3] == 7));
        }
        drop(samples);
        let count = support::int(session.as_ref(), "SELECT COUNT(*) FROM t");
        assert_eq!(count, 8192);
    }
}

/// A host callback that erases the table once, after a read has built its answer. The firing
/// thread is inside a callback, where PURGE is refused, so another thread asks and is waited for.
#[derive(Default)]
struct Purger {
    db: Mutex<Weak<Database>>,
    armed: AtomicBool,
    purged: AtomicU64,
}
impl Purger {
    fn purge(&self) {
        if self.armed.swap(false, SeqCst) {
            let db = self.db.lock().unwrap().upgrade().unwrap();
            std::thread::scope(|scope| {
                scope
                    .spawn(|| db.execute("PURGE FROM t", &params! {}))
                    .join()
                    .unwrap()
                    .unwrap()
            });
            self.purged.fetch_add(1, SeqCst);
        }
    }
}
impl DatabasePlugin for Purger {
    fn post_query(&self, sql: &str, _: std::time::Duration, _: &QueryOutcome) {
        if sql.starts_with("SELECT") {
            self.purge();
        }
    }
}
impl ReadProgressObserver for Purger {
    fn progress(&self, _: ReadProgress) {}
    fn completed(&self, _: ReadProgress) {
        self.purge();
    }
}

#[test]
fn a_purge_between_building_an_answer_and_returning_it_cuts_the_read_off() {
    let purger = Arc::new(Purger::default());
    let mut options = DatabaseOpenOptions::default();
    options.plugin = Some(purger.clone());
    let db = Arc::new(support::open_with(&support::folder("final-cutoff"), options).unwrap());
    *purger.db.lock().unwrap() = Arc::downgrade(&db);
    support::declare(&db);
    let session = db
        .read_session_with_progress(ReadLimits::default(), purger.clone())
        .unwrap();
    let q = params! {};
    for sql in ["SELECT id FROM t", "SELECT id / (id - id) FROM t"] {
        for door in ["database", "transaction", "session", "cursor"] {
            support::insert_together(&db, 1..=3);
            let tx = (door == "transaction").then(|| db.transaction().unwrap());
            let mut cursor = (door == "cursor").then(|| session.open_cursor(sql, &q).unwrap());
            purger.armed.store(true, SeqCst);
            let result = match door {
                "database" => db.execute(sql, &q).map(drop),
                "transaction" => tx.as_ref().unwrap().execute(sql, &q).map(drop),
                "session" => session.execute(sql, &q).map(drop),
                _ => cursor.as_mut().unwrap().fetch(None).map(drop),
            };
            assert_eq!(
                purger.purged.swap(0, SeqCst),
                1,
                "{door}: the observer did not run"
            );
            assert_eq!(
                format!("{:?}", result.unwrap_err()),
                "AuthoritativeErasure",
                "{door} read of `{sql}` returned, after a PURGE, what it had read before it: the \
                 last validity check must follow the work and the host's observation of it"
            );
        }
    }
    // A write is not cut off by its own PURGE.
    support::insert_together(&db, 1..=3);
    let write = db
        .execute("PURGE FROM t", &q)
        .expect("a PURGE returns its own result");
    assert_eq!(write.rows_affected(), 3);
}

#[test]
fn a_purge_completes_while_eight_readers_keep_reading() {
    let db = support::open(&support::folder("purge-eight-readers"));
    support::declare(&db);
    support::insert_together(&db, 1..=3);
    let sessions: Vec<_> = (0..8)
        .map(|_| db.read_session(ReadLimits::default()).unwrap())
        .collect();
    let (start, done) = (Barrier::new(9), AtomicBool::new(false));
    let reads = std::sync::atomic::AtomicUsize::new(0);
    let counts = std::thread::scope(|scope| {
        let readers: Vec<_> = sessions
            .iter()
            .map(|session| {
                let (start, done, reads) = (&start, &done, &reads);
                scope.spawn(move || {
                    let count = "SELECT COUNT(*) FROM t";
                    assert_eq!(support::int(session, count), 3);
                    start.wait();
                    while !done.load(SeqCst) {
                        match session.execute(count, &params! {}) {
                            Ok(_) | Err(Error::AuthoritativeErasure) => (),
                            Err(other) => panic!("a read during a PURGE was refused as {other:?}"),
                        }
                        reads.fetch_add(1, SeqCst);
                    }
                    support::int(session, count)
                })
            })
            .collect();
        start.wait();
        // The PURGE starts after eight reads were counted and the readers stop after eight more.
        let until = |n: usize| {
            while reads.load(SeqCst) < n {
                std::thread::yield_now()
            }
        };
        until(8);
        db.execute("PURGE FROM t", &params! {})
            .expect("the PURGE completes among readers");
        until(reads.load(SeqCst) + 8);
        done.store(true, SeqCst);
        readers
            .into_iter()
            .map(|reader| reader.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(
        counts, [0; 8],
        "a read begun after the PURGE saw an erased row"
    );
}
