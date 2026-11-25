//! Extends readers_limits.rs and readers_progress.rs; isolated until the cursor API exists.
#[path = "support/readmem.rs"]
mod readmem;
mod support;
use contextdb::{Error, ReadLimits, ReadSession, params};
use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use support::{Bytes, Millis, Observer};
#[test]
fn a_cursor_holds_one_reader_file_until_it_ends() {
    let folder = support::folder("cursor-limits");
    let (db, now) = readmem::clocked(&folder, 3);
    support::run(&db, "UPDATE t SET name = 'row-00000003' WHERE id = 3");
    let bytes = Arc::new(Bytes::default());
    let measured = db
        .read_session_with_progress(ReadLimits::default(), bytes.clone())
        .unwrap();
    let last = "SELECT name FROM t WHERE id = 3";
    let wanted = support::rows(&measured, last);
    let charge = *bytes.0.lock().unwrap();
    macro_rules! cases {
        ($base:expr; $($field:ident: $cap:expr),*) => {
            [$( (stringify!($field), ReadLimits { $field: $cap, ..$base.clone() }) ),*]
        };
    }
    for file in [false, true] {
        for over in [0, 1] {
            let base = ReadLimits {
                cursor_page_rows: 1,
                ..ReadLimits::default()
            };
            for (field, mut limits) in cases! { base;
                result_bytes: charge - over, cursor_page_bytes: charge - over,
                work: 2 - over, active_ms: 7, cursor_idle_ms: 7, cursor_lifetime_ms: 7
            }
            .into_iter()
            .chain([("cancel", base)])
            {
                now.store(0, SeqCst);
                limits.cursor_page_rows += u64::from(field == "cursor_page_bytes" && over == 0);
                let observer = Observer::new(now.clone(), 0, false);
                let mut options = support::reader_options(None);
                options.limits = limits.clone();
                options.clock = Some(observer.clone());
                let read = if file {
                    ReadSession::open_with_options(&folder, options)
                } else {
                    db.read_session_with_progress(limits, observer.clone())
                };
                let session = Arc::new(read.unwrap());
                *observer.session.lock().unwrap() = Arc::downgrade(&session);
                let invoice = (field == "cursor_page_bytes").then(|| {
                    let empty = "SELECT name FROM t WHERE id = 0";
                    // The record's line for the form opened below is added once, here.
                    drop(
                        session
                            .open_cursor("SELECT name FROM t WHERE id <> 2", &params! {})
                            .unwrap(),
                    );
                    drop(session.open_cursor(empty, &params! {}).unwrap());
                    let _row = session.execute(last, &params! {}).unwrap();
                    support::memory_used(session.as_ref())
                });
                let sql = "SELECT name FROM t WHERE id <> 2";
                let mut cursor = session.open_cursor(sql, &params! {}).unwrap();
                assert_eq!(support::listing(&folder.join("readers")).len(), 1);
                // The first page is the first fetch; the limits below are met by the second.
                drop(cursor.fetch(None).unwrap());
                let ticking = field == "active_ms" && (!file || over == 1);
                observer
                    .advance
                    .store(if ticking { 7 + over } else { 0 }, SeqCst);
                observer
                    .cancel
                    .store(field == "cancel" && over == 1, SeqCst);
                if field == "cursor_idle_ms" || field == "cursor_lifetime_ms" {
                    now.store(7 + over, SeqCst);
                }
                let page = cursor.fetch(None);
                observer.cancel.store(false, SeqCst);
                observer.advance.store(0, SeqCst);
                if over == 1 {
                    let canceled = field == "active_ms" || field == "cancel";
                    match page.unwrap_err() {
                        Error::ReadLimitExceeded { field: actual, .. } if !canceled => {
                            assert_eq!(actual, field)
                        }
                        Error::QueryCanceled { statement_timeout } if canceled => {
                            assert_eq!(statement_timeout, field == "active_ms")
                        }
                        other => panic!("{field}: {other}"),
                    }
                    assert!(support::listing(&folder.join("readers")).is_empty());
                } else {
                    let page = page.unwrap();
                    assert_eq!(page.rows(), wanted);
                    assert_eq!(cursor.has_more(), field != "cursor_page_bytes");
                    if let Some(invoice) = invoice {
                        assert_eq!(support::memory_used(session.as_ref()), invoice);
                    }
                }
                assert_eq!(support::int(session.as_ref(), "SELECT COUNT(*) FROM t"), 3);
            }
        }
    }
}

#[test]
fn a_cursor_nobody_fetches_again_lets_go_of_its_commit_at_its_limit_or_a_purge() {
    let folder = support::folder("cursor-lapse");
    let (db, now) = readmem::clocked(&folder, 3);
    let held = || {
        support::run(&db, "UPDATE t SET tag = tag + 1");
        support::listing(&folder.join("readers")).len()
    };
    let open = |limits: ReadLimits| {
        let mut options = support::reader_options(None);
        options.limits = ReadLimits {
            cursor_page_rows: 1,
            ..limits
        };
        options.clock = Some(Arc::new(Millis(now.clone())));
        let session = ReadSession::open_with_options(&folder, options).unwrap();
        let mut cursor = session
            .open_cursor("SELECT name FROM t", &params! {})
            .unwrap();
        cursor.fetch(None).unwrap();
        (session, cursor)
    };
    for field in ["cursor_idle_ms", "cursor_lifetime_ms"] {
        let mut limits = ReadLimits::default();
        *[&mut limits.cursor_idle_ms, &mut limits.cursor_lifetime_ms]
            [(field.len() > 14) as usize] = 7;
        now.store(0, SeqCst);
        let (_session, mut cursor) = open(limits);
        let at_deadline = (now.store(7, SeqCst), held()).1;
        let past = (now.store(8, SeqCst), held()).1;
        let kept = (at_deadline, past);
        assert_eq!(
            kept,
            (1, 0),
            "{field}: held at its deadline, let go a ms later"
        );
        for _ in 0..2 {
            let refused = cursor.fetch(None).unwrap_err();
            assert!(
                matches!(&refused, Error::ReadLimitExceeded { field: f, .. } if *f == field),
                "{field}: {refused:?}"
            );
        }
    }
    let (_a, mut before) = open(ReadLimits::default());
    support::run(&db, "PURGE FROM t WHERE id = 1");
    let (_b, mut after) = open(ReadLimits::default());
    let left = support::listing(&folder.join("readers")).len();
    assert_eq!(left, 1, "a purge frees only the earlier cursor");
    let erased = format!("{:?}", before.fetch(None).unwrap_err());
    assert_eq!(erased, "AuthoritativeErasure");
    after.fetch(None).unwrap();
}
