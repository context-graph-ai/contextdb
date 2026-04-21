// ======== TU3 ========

#[test]
fn value_to_string_txid_renders_bare_integer() {
    use contextdb_core::{TxId, Value};
    use contextdb_engine::cli_render::value_to_string;

    let rendered = value_to_string(&Value::TxId(TxId(42)));
    assert_eq!(
        rendered, "42",
        "Value::TxId(TxId(42)) must render as bare integer '42' in CLI output, got {rendered:?}"
    );

    // Regression: same function must still render Int64 as its bare integer.
    let rendered_i = value_to_string(&Value::Int64(42));
    assert_eq!(
        rendered_i, "42",
        "Value::Int64(42) rendering must be unchanged by TxId arm addition, got {rendered_i:?}"
    );
}

// ======== TU4 ========

#[test]
fn sync_status_emits_committed_txid_line() {
    use contextdb_engine::Database;
    use contextdb_engine::cli_render::render_sync_status;

    let db = Database::open_memory();
    db.execute("CREATE TABLE drive (x INTEGER)", &Default::default())
        .expect("CREATE TABLE must succeed");

    // Drive 42 commits so committed_watermark == TxId(42) (or whatever the allocator yields).
    for i in 0..42 {
        db.execute(
            &format!("INSERT INTO drive (x) VALUES ({i})"),
            &Default::default(),
        )
        .expect("INSERT must succeed");
    }

    let live_watermark = db.committed_watermark();
    let expected_line = format!("Committed TxId: {}", live_watermark.0);

    let output = render_sync_status(&db);

    assert!(
        output.contains(&expected_line),
        ".sync status output must contain {expected_line:?}, got:\n{output}"
    );
}
