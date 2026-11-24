//! Counted write work and eight fixed-clock seasons, through the consumer's real declarations.
mod support;
use contextdb::{Database, DatabaseOpenOptions, ReadSession, TxId, Value, params};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering::SeqCst},
};

fn consumer(db: &Database) {
    let schema = include_str!("fixtures/consumer_schema.sql")
        .split("-- Second texts")
        .next()
        .unwrap();
    for sql in schema.lines().filter(|s| s.starts_with("CREATE ")) {
        support::run(db, sql);
    }
    let tx = db.transaction().unwrap();
    let p = params! {"c" => support::context(1), "g" => support::context(2), "t" => Value::TxId(tx.id())};
    for sql in [
        "INSERT INTO contexts(id,name,created_at) VALUES($c,'context',$t)",
        "INSERT INTO embedding_spaces(embedding_space_id,dimension,metric,created_at) VALUES('space',768,'cosine',$t)",
        "INSERT INTO event_search_generations(id,context_id,interpretation_snapshot,interpretation_snapshot_sha256,event_count,ordered_source_set_sha256,renderer_id,renderer_version,renderer_profile,embedding_space_id,encoding_profile,state,started_at,last_attempt_token) VALUES($g,$c,'{}','',0,'','r','1','{}','space','e','active',$t,$g)",
    ] {
        tx.execute(sql, &p).unwrap();
    }
    tx.commit().unwrap();
}
fn passage(on: &impl support::Run, id: i64) -> contextdb::QueryResult {
    on.run("INSERT INTO event_search_passages(id,context_id,generation_id,connector_id,session_ref,source_instance_ref,passage_ordinal,first_event_sequence,last_event_sequence,rendered_text,content_sha256,vector_text,dimension,is_current,is_searchable,created_at) VALUES($id,$c,$g,'c','s','s',$n,$n,$n,$body,'hash',$v,768,true,true,$t)",
        &params! {"id" => support::context(id as u128 + 100), "c" => support::context(1), "g" => support::context(2), "n" => id, "body" => support::letters(id % 1000, 512), "v" => support::vector(id % 1000,768), "t" => Value::TxId(TxId(1))}).unwrap()
}
#[test]
fn imports_and_one_row_commits_keep_their_counted_write_bounds() {
    let folder = support::folder("consumer-write-pages");
    let counts = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]);
    let mut options = DatabaseOpenOptions::default();
    options.clock = Some(Arc::new(support::At(Arc::default())));
    options.storage = Some(support::storage({
        let counts = counts.clone();
        move |call| match call {
            support::Call::Write(at, bytes) => {
                counts[0].fetch_add((at % 4096 + bytes.len() as u64).div_ceil(4096), SeqCst);
            }
            support::Call::Sync => {
                counts[1].fetch_add(1, SeqCst);
            }
            _ => (),
        }
    }));
    let db = support::open_with(&folder, options).unwrap();
    consumer(&db);
    let (mut work, mut imported, mut level) = (Vec::new(), None, Vec::new());
    // consumer_schema.sql: 87 tables, 9 passage indexes, 512-byte text and VECTOR(768); bounds count 4 KiB units written, including repeats.
    // Refill grows a compacted file: 1,829 with the 0.05/0.02 defaults on bbfc7f0ec. Lowest-address recovery bookkeeping lets small stores shrink after deletes. Refill rose from 1,830 to 1,835 writes (+3 beside writes, +2 row writes, metadata still 7). The old ceiling had 1 write of slack, so the ceiling rises by 4.
    // Each statement form runs once before the first measure, so the capped statement record gains no line later.
    support::run(
        &db,
        "DELETE FROM event_search_passages WHERE passage_ordinal < 1001",
    );
    let find = |at: i64| {
        let sql = "SELECT id FROM event_search_passages ORDER BY vector_text <=> $q USE VECTOR EXACT LIMIT 1";
        db.execute(sql, &params! {"q" => support::vector(at, 768)})
            .unwrap()
    };
    find(1);
    // With each sealed row's slot record kept in the vector part, a one-row commit writes 29
    // pages, one more than before.
    for (first, rows, pages) in [(1, 1000, 1815), (1001, 1, 29), (1002, 1000, 1835)] {
        if first == 1002 {
            support::run(
                &db,
                "DELETE FROM event_search_passages WHERE passage_ordinal < 1001",
            );
            support::maintenance(&db);
        }
        counts.iter().for_each(|c| c.store(0, SeqCst));
        let tx = db.transaction().unwrap();
        for id in first..first + rows {
            passage(&tx, id);
        }
        tx.commit().unwrap();
        imported.get_or_insert_with(|| support::data_file(&folder));
        let written = counts[0].load(SeqCst);
        work.push((rows, written, pages, counts[1].load(SeqCst)));
        let found = find(1); // the same question at every point, so peaks compare like for like
        assert_eq!(found.rows().len(), 1);
        level.push((support::memory_used(&db), found.peak_bytes()));
        for part in support::shown(
            &db,
            "SHOW VECTOR_PARTITIONS FOR event_search_passages.vector_text",
        ) {
            assert!(
                support::whole(&part, "pending_tuples") <= 328,
                "a commit left more than one block of unindexed vectors: 328 for a UUID key and 768 dimensions"
            );
        }
    }
    // One event row: a commit of no more pages than a passage's, with no large value or vector.
    let events = |db: &Database| {
        let rows = support::rows(db, "SHOW CONTENT_SIZE");
        rows.into_iter()
            .find(|r| r[0] == support::text("events"))
            .unwrap()
    };
    let before = events(&db);
    counts.iter().for_each(|c| c.store(0, SeqCst));
    db.execute("INSERT INTO events(id,context_id,connector_id,adapter_version,anchor_declaration_version,source_instance_ref,session_ref,source_event_key,source_position,seq,content_hash,anchor_count,anchor_set_hash,author,kind,render_role,coverage) VALUES($c,$c,'c','1','1','s','s','k','{}',1,'h',0,'h','a','k','r','full')", &params! {"c" => support::context(1)}).unwrap();
    let (pages, syncs) = (counts[0].load(SeqCst), counts[1].load(SeqCst));
    assert!(
        pages <= work[1].2 && syncs == 1,
        "pages {pages}, syncs {syncs}"
    );
    let after = events(&db);
    assert_eq!(
        [&after[2], &after[4], &after[5]],
        [&before[2], &before[4], &before[5]],
        "no large value, vector or block"
    );
    // A table of two vector columns: no one-row commit leaves either column a block of 336 unindexed or more (vectors_maintenance.rs:204-210).
    support::run(
        &db,
        "CREATE TABLE twin (id UUID PRIMARY KEY, a VECTOR(768), b VECTOR(768))",
    );
    for n in 1..=700 {
        db.execute(
            "INSERT INTO twin VALUES ($n, $v, $v)",
            &params! {"n" => support::context(n as u128), "v" => support::vector(n, 768)},
        )
        .unwrap();
        for part in support::shown(&db, "SHOW VECTOR_PARTITIONS") {
            let pending = support::whole(&part, "pending_tuples");
            assert!(pending <= 328, "after commit {n}: {pending} unindexed");
        }
    }
    println!("import-size {} bytes", imported.unwrap());
    // The second thousand, appended after the first, holds no more memory than the first did.
    assert!(
        level[2].0 <= level[0].0 && level[2].1 <= level[0].1,
        "memory and peak: {level:?}"
    );
    for (rows, written, pages, syncs) in &work {
        assert_eq!(*syncs, 1, "one durable commit: {work:?}");
        assert!(
            written <= pages,
            "rows, pages, bound, syncs: {work:?}; {rows} rows exceeded bound"
        );
    }
    support::close(db);
}
#[test]
fn a_season_stays_level() {
    const LIVE: i64 = 1000;
    let folder = support::folder("consumer-season");
    let clock = Arc::new(AtomicU64::new(100));
    let mut options = DatabaseOpenOptions::default();
    options.clock = Some(Arc::new(support::At(clock.clone())));
    let db = support::open_with(&folder, options).unwrap();
    consumer(&db);
    support::run(
        &db,
        "ALTER TABLE event_search_passages SET RETAIN 10 SECONDS",
    );
    support::run(
        &db,
        "ALTER TABLE event_search_passages ALTER COLUMN vector_text SET INDEXING_THRESHOLD 1000",
    );
    support::run(
        &db,
        "CREATE TABLE twelve(id INTEGER PRIMARY KEY, host TEXT NOT NULL, path TEXT NOT NULL, kind TEXT NOT NULL, seq INTEGER NOT NULL, at INTEGER NOT NULL, note TEXT NOT NULL) RETAIN 10 SECONDS",
    );
    let indexes = "host;path;kind;host,path;host,kind;path,kind;kind,host;host,path,kind;path,host;kind,path;host,kind,path;path,host,kind";
    for (i, columns) in indexes.split(';').enumerate() {
        support::run(
            &db,
            &format!("CREATE INDEX twelve_{i} ON twelve({columns})"),
        );
    }
    let firsts = [
        "SELECT MIN(passage_ordinal) FROM event_search_passages",
        "SELECT MIN(id) FROM twelve",
    ];
    let find = "SELECT id FROM event_search_passages ORDER BY vector_text <=> $q USE VECTOR INDEXED LIMIT 10";
    let q = params! {"q"=>support::vector(17,768)};
    // Each statement form runs once before the first measure, so the capped statement record
    // gains no line later (+960 bytes of peak from the second turnover on, without this).
    let counts = ["event_search_passages", "twelve"].map(|t| format!("SELECT COUNT(*) FROM {t}"));
    for sql in counts.iter().map(String::as_str).chain(firsts) {
        support::run(&db, sql);
    }
    db.execute(find, &q).unwrap();
    let mut states = Vec::new();
    for cycle in 1..=8 {
        clock.store(100 + cycle * 10, SeqCst);
        let tx = db.transaction().unwrap();
        let (mut peak, mut input) = (0, 220_u64); // The three parent rows, including defaults.
        for offset in 0..LIVE {
            let n = (offset * 37) % LIVE;
            let id = cycle as i64 * LIVE + n;
            peak = peak.max(passage(&tx, id).peak_bytes().unwrap());
            let host = format!("host-{}.build.cluster.example.internal.corp", n % 7);
            let path = format!("/srv/data/projects/{}/releases/{}", n % 13, n % 101);
            let kind = ["created", "changed", "removed", "moved", "read"][n as usize % 5];
            input += 3681 + (24 + host.len() + path.len() + kind.len() + 800) as u64;
            let row = tx.execute("INSERT INTO twelve VALUES($id,$host,$path,$kind,$seq,$at,$note)", &params!{"id"=>id,"host"=>host,"path"=>path,"kind"=>kind,"seq"=>n*3,"at"=>1_700_000_000+n*61,"note"=>support::letters(n,800)}).unwrap();
            peak = peak.max(row.peak_bytes().unwrap());
        }
        tx.commit().unwrap();
        support::maintenance(&db);
        for sql in &counts {
            assert_eq!(support::int(&db, sql), LIVE);
        }
        let firsts = firsts.map(|sql| support::int(&db, sql));
        assert_eq!(firsts, [cycle as i64 * LIVE; 2], "old rows remain due");
        let result = db.execute(find, &q).unwrap();
        states.push((
            support::data_file(&folder),
            peak.max(result.peak_bytes().unwrap()),
            result.distance_evaluations(),
            input,
        ));
    }
    println!("churn-size {} bytes", support::data_file(&folder));
    // The eighth turnover against the store as first filled and drained (design 4, the guard).
    let (old, state) = (states[0], states[7]);
    // Size, statement peak and fixed-find drift, in that order. The file's drift from the first
    // turnover (its compaction step and a tenth) is Space's to hold, and returns with PR 5C.
    let checks = [
        states.iter().all(|s| s.0 <= 2 * s.3),
        state.1 == old.1,
        state.2.abs_diff(old.2) * 10 <= old.2,
    ];
    assert_eq!(
        checks, [true; 3],
        "season (file, statement peak, compared, input): {states:?}"
    );
    support::close(db);
}
#[test]
fn a_readers_second_run_reuses_its_pages() {
    let folder = support::folder("reader-keeps-pages");
    let db = support::open(&folder);
    support::declare(&db);
    support::insert_together(&db, 1..=1000);
    let (storage, bytes) = support::counting_bytes_read();
    let mut options = support::reader_options(None);
    options.storage = Some(storage);
    let reader = ReadSession::open_with_options(&folder, options).unwrap();
    bytes.store(0, SeqCst);
    let sql = "SELECT id,name,tag FROM t ORDER BY id LIMIT 100";
    let cold = support::run(&reader, sql);
    let first = bytes.swap(0, SeqCst);
    let warm = support::run(&reader, sql);
    let second = bytes.load(SeqCst);
    assert_eq!(cold.rows(), warm.rows());
    assert!(
        first > second.saturating_mul(2),
        "cold {first} bytes, warm {second}: no page reuse"
    );
    support::close(db);
}
#[test]
fn a_reader_under_16_mib_answers_a_find_on_ten_thousand_passages() {
    let folder = support::folder("reader-10000-passages");
    let db = support::open(&folder);
    consumer(&db);
    let tx = db.transaction().unwrap();
    (1..=10_000).for_each(|id| _ = passage(&tx, id));
    tx.commit().unwrap();
    support::close(db);
    let reader = support::open_reader(&folder, Some(16 << 20));
    let near = |n: usize| vec!["0.5"; n].join(",");
    let (c, g) = (support::context(1), support::context(2));
    let find = format!(
        "SELECT id FROM event_search_passages WHERE context_id = '{c}' AND generation_id = '{g}' ORDER BY vector_text <=> '[{}]' LIMIT 5",
        near(768)
    );
    assert_eq!(
        support::rows(&reader, &find).len(),
        5,
        "the find under 16 MiB"
    );
    // The same reader reads the evidence table's other vector column, and nothing refuses it.
    let sight = format!(
        "SELECT id FROM evidence ORDER BY vector_vision <=> '[{}]' LIMIT 5",
        near(512)
    );
    assert!(support::rows(&reader, &sight).is_empty());
}
