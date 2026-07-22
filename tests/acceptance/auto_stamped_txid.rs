use contextdb_core::types::TxId;
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn assert_error<T>(result: contextdb_core::Result<T>, message: &str) -> Error {
    let err = result.err();
    assert!(err.is_some(), "{message}: expected Err, got Ok");
    err.unwrap()
}

/// RED — t26_01
#[test]
fn t26_01_insert_omitting_txid_auto_stamps_with_active_tx() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE audit_log (id UUID PRIMARY KEY, actor TEXT, commit_marker TXID NOT NULL)",
        &empty(),
    )
    .unwrap();

    let id = Uuid::from_u128(1);
    let pre = db.committed_watermark();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(id));
    p.insert("actor".into(), Value::Text("agent-1".into()));
    // Note: no `commit_marker` parameter, no `commit_marker` in column list.
    let result = db.execute("INSERT INTO audit_log (id, actor) VALUES ($id, $actor)", &p);
    assert!(
        result.is_ok(),
        "auto-stamp must allow INSERT to succeed; got {result:?}"
    );

    let scan = db.scan("audit_log", db.snapshot()).unwrap();
    assert_eq!(scan.len(), 1);
    let txid_val = scan[0]
        .values
        .get("commit_marker")
        .cloned()
        .expect("commit_marker must be set");
    match txid_val {
        Value::TxId(t) => {
            // Strict: this INSERT's tx is exactly one newer than the pre-watermark.
            assert_eq!(
                t.0,
                pre.0 + 1,
                "auto-stamped TxId must equal pre-watermark + 1; got {}, pre={}",
                t.0,
                pre.0
            );
        }
        other => panic!("expected Value::TxId for commit_marker column, got: {other:?}"),
    }

    let explicit_tx_pre = db.committed_watermark();
    db.execute("BEGIN", &empty()).unwrap();
    for i in 2..=3u128 {
        let mut p = HashMap::new();
        p.insert("id".into(), Value::Uuid(Uuid::from_u128(i)));
        p.insert("actor".into(), Value::Text(format!("agent-{i}")));
        db.execute("INSERT INTO audit_log (id, actor) VALUES ($id, $actor)", &p)
            .unwrap();
    }
    db.execute("COMMIT", &empty()).unwrap();
    let scan = db.scan("audit_log", db.snapshot()).unwrap();
    let mut txids = Vec::new();
    for id in [Uuid::from_u128(2), Uuid::from_u128(3)] {
        let row = scan
            .iter()
            .find(|r| matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == id))
            .expect("explicit transaction row must exist");
        match row.values.get("commit_marker") {
            Some(Value::TxId(t)) => txids.push(*t),
            other => panic!("expected explicit transaction row to carry TxId, got {other:?}"),
        }
    }
    assert_eq!(
        txids,
        vec![TxId(explicit_tx_pre.0 + 1), TxId(explicit_tx_pre.0 + 1)],
        "every omitted TXID in one explicit transaction must stamp the same active tx id"
    );
}

/// REGRESSION GUARD — t26_02
#[test]
fn t26_02_insert_with_explicit_txid_uses_supplied_value() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE audit_log (id UUID PRIMARY KEY, explicit_clock TXID NOT NULL)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE watermark_bump (id UUID PRIMARY KEY)",
        &empty(),
    )
    .unwrap();

    let mut bump = HashMap::new();
    bump.insert("id".into(), Value::Uuid(Uuid::from_u128(99)));
    db.execute("INSERT INTO watermark_bump (id) VALUES ($id)", &bump)
        .unwrap();

    let watermark = db.committed_watermark();
    assert!(watermark.0 >= 1);

    let id2 = Uuid::from_u128(2);
    let explicit_tx = TxId(1);
    let mut p2 = HashMap::new();
    p2.insert("id".into(), Value::Uuid(id2));
    p2.insert("tx".into(), Value::TxId(explicit_tx));
    db.execute(
        "INSERT INTO audit_log (id, explicit_clock) VALUES ($id, $tx)",
        &p2,
    )
    .unwrap();

    let scan = db.scan("audit_log", db.snapshot()).unwrap();
    let row = scan
        .iter()
        .find(|r| matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == id2))
        .unwrap();
    assert_eq!(
        row.values.get("explicit_clock"),
        Some(&Value::TxId(explicit_tx))
    );
}

/// REGRESSION GUARD — t26_03
#[test]
fn t26_03_explicit_txid_above_watermark_rejected() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, causal_marker TXID NOT NULL)",
        &empty(),
    )
    .unwrap();

    // Watermark is 0; an explicit TxId(999_999) must be out of range.
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(Uuid::from_u128(1)));
    p.insert("tx".into(), Value::TxId(TxId(999_999)));
    let err = assert_error(
        db.execute("INSERT INTO t (id, causal_marker) VALUES ($id, $tx)", &p),
        "out-of-range explicit TxId must be rejected",
    );
    assert!(
        matches!(err, Error::TxIdOutOfRange { .. }),
        "expected TxIdOutOfRange, got: {err}"
    );
}

/// RED — t26_04
#[test]
fn t26_04_insert_with_null_txid_auto_stamps() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE audit_log (id UUID PRIMARY KEY, applied_at TXID NOT NULL)",
        &empty(),
    )
    .unwrap();

    let id = Uuid::from_u128(1);
    let pre = db.committed_watermark();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(id));
    p.insert("tx".into(), Value::Null);
    let result = db.execute(
        "INSERT INTO audit_log (id, applied_at) VALUES ($id, $tx)",
        &p,
    );
    assert!(
        result.is_ok(),
        "NULL must be auto-filled, not rejected; got {result:?}"
    );

    let scan = db.scan("audit_log", db.snapshot()).unwrap();
    let row = scan
        .iter()
        .find(|r| matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == id))
        .unwrap();
    match row.values.get("applied_at") {
        Some(Value::TxId(t)) => {
            assert_eq!(t.0, pre.0 + 1, "NULL must auto-fill with pre-watermark+1")
        }
        other => panic!("expected TxId, got {other:?}"),
    }
}

/// RED — t26_05
#[test]
fn t26_05_upsert_insert_path_auto_stamps() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT, created_tx TXID NOT NULL)",
        &empty(),
    )
    .unwrap();

    let id = Uuid::from_u128(1);
    let pre = db.committed_watermark();
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(id));
    p.insert("name".into(), Value::Text("first".into()));
    let result = db.execute(
        "INSERT INTO t (id, name) VALUES ($id, $name) \
         ON CONFLICT (id) DO UPDATE SET name = $name",
        &p,
    );
    assert!(
        result.is_ok(),
        "UPSERT INSERT path must auto-stamp TXID column; got {result:?}"
    );

    let scan = db.scan("t", db.snapshot()).unwrap();
    let row = scan
        .iter()
        .find(|r| matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == id))
        .unwrap();
    let inserted_tx = match row.values.get("created_tx") {
        Some(Value::TxId(t)) => {
            assert_eq!(
                t.0,
                pre.0 + 1,
                "UPSERT INSERT path must stamp pre-watermark+1"
            );
            *t
        }
        other => panic!("expected TxId, got {other:?}"),
    };

    let mut conflict = HashMap::new();
    conflict.insert("id".into(), Value::Uuid(id));
    conflict.insert("name".into(), Value::Text("second".into()));
    let result = db.execute(
        "INSERT INTO t (id, name) VALUES ($id, $name) \
         ON CONFLICT (id) DO UPDATE SET name = $name",
        &conflict,
    );
    assert!(
        result.is_ok(),
        "UPSERT conflict-update path must not reject omitted TXID; got {result:?}"
    );
    let scan = db.scan("t", db.snapshot()).unwrap();
    let row = scan
        .iter()
        .find(|r| matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == id))
        .unwrap();
    assert_eq!(
        row.values.get("name"),
        Some(&Value::Text("second".into())),
        "UPSERT conflict-update branch must update non-TXID columns"
    );
    assert_eq!(
        row.values.get("created_tx"),
        Some(&Value::TxId(inserted_tx)),
        "UPSERT conflict-update branch must leave TXID NOT NULL columns unchanged unless explicitly SET"
    );

    let explicit_tx = db.committed_watermark();
    assert!(
        explicit_tx.0 > inserted_tx.0,
        "preserving conflict update must still advance the commit watermark"
    );
    let mut explicit = HashMap::new();
    explicit.insert("id".into(), Value::Uuid(id));
    explicit.insert("name".into(), Value::Text("third".into()));
    explicit.insert("tx".into(), Value::TxId(explicit_tx));
    let result = db.execute(
        "INSERT INTO t (id, name) VALUES ($id, $name) \
         ON CONFLICT (id) DO UPDATE SET name = $name, created_tx = $tx",
        &explicit,
    );
    assert!(
        result.is_ok(),
        "UPSERT conflict-update path must honor an explicit TXID SET; got {result:?}"
    );
    let scan = db.scan("t", db.snapshot()).unwrap();
    let row = scan
        .iter()
        .find(|r| matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == id))
        .unwrap();
    assert_eq!(
        row.values.get("name"),
        Some(&Value::Text("third".into())),
        "UPSERT explicit conflict-update branch must update ordinary columns"
    );
    assert_eq!(
        row.values.get("created_tx"),
        Some(&Value::TxId(explicit_tx)),
        "UPSERT conflict-update branch must apply an explicit TXID SET instead of always preserving"
    );
}

/// REGRESSION GUARD — t26_06
#[test]
fn t26_06_update_omitting_txid_does_not_auto_overwrite() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT, write_tx TXID NOT NULL)",
        &empty(),
    )
    .unwrap();

    let id = Uuid::from_u128(1);
    let mut p = HashMap::new();
    p.insert("id".into(), Value::Uuid(id));
    p.insert("name".into(), Value::Text("v1".into()));
    p.insert("tx".into(), Value::TxId(TxId(1)));
    db.execute(
        "INSERT INTO t (id, name, write_tx) VALUES ($id, $name, $tx)",
        &p,
    )
    .unwrap();
    let pre_scan = db.scan("t", db.snapshot()).unwrap();
    let pre_tx = pre_scan[0].values.get("write_tx").cloned().unwrap();

    let mut p2 = HashMap::new();
    p2.insert("id".into(), Value::Uuid(id));
    p2.insert("name".into(), Value::Text("v2".into()));
    db.execute("UPDATE t SET name = $name WHERE id = $id", &p2)
        .unwrap();

    // The update should leave the TXID column untouched (no auto-overwrite on UPDATE).
    let post_scan = db.scan("t", db.snapshot()).unwrap();
    let post = post_scan
        .iter()
        .find(|r| matches!(r.values.get("id"), Some(Value::Uuid(u)) if *u == id))
        .unwrap();
    assert_eq!(post.values.get("write_tx"), Some(&pre_tx));
}
