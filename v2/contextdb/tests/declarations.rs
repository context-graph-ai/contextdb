//! Declarations: every table and index text of a program built on this library is accepted, is
//! printed back with every clause its author wrote and no other, and the printed text declares
//! the same table again, in an empty store and after the store is closed and opened. And what a
//! declaration refuses: by its kind, saying why, and declaring nothing.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use contextdb::{Database, Error, ReadSession};

/// The declarations of a program built on this library, as that program writes them.
const CONSUMER_SCHEMA: &str = include_str!("fixtures/consumer_schema.sql");

/// The statements of a text. A statement ends with `;` at the end of a line; a line that begins
/// with `--` between two statements is a comment.
fn statements(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut statement = String::new();
    for line in text.lines() {
        if statement.is_empty() && (line.trim().is_empty() || line.starts_with("--")) {
            continue;
        }
        statement.push_str(line);
        statement.push('\n');
        if let Some(whole) = statement.trim_end().strip_suffix(';') {
            found.push(whole.to_owned());
            statement.clear();
        }
    }
    assert!(statement.is_empty(), "a statement with no `;`: {statement}");
    found
}

/// The fixture's statements: the ones a single store takes in the order written, and the
/// second texts for tables among them.
fn consumer_schema() -> (Vec<String>, Vec<String>) {
    let (first, second) = CONSUMER_SCHEMA
        .split_once("\n-- Second texts")
        .expect("the fixture's second part");
    // What is left of the line that begins the second part is the rest of its comment.
    let (_, second) = second.split_once('\n').expect("the second texts");
    (statements(first), statements(second))
}

/// A name as the fixture writes one: its letters up to the first character that is none.
fn name(text: &str) -> String {
    text.chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect()
}

/// The table a `CREATE TABLE name (...)` or a `CREATE INDEX name ON table (...)` of the fixture
/// is for, and whether the statement is the table's own.
fn table_of(sql: &str) -> (String, bool) {
    if let Some(rest) = sql.strip_prefix("CREATE TABLE ") {
        return (name(rest), true);
    }
    let (_, rest) = sql
        .split_once(" ON ")
        .unwrap_or_else(|| panic!("neither a CREATE TABLE nor a CREATE INDEX: {sql}"));
    (name(rest), false)
}

/// The tables a statement of the fixture references, its own left out.
fn referenced(sql: &str) -> BTreeSet<String> {
    let own = table_of(sql).0;
    let parents = sql.split("REFERENCES ").skip(1).map(name);
    parents.filter(|parent| *parent != own).collect()
}

/// The words of a declaration in lower case and its quoted texts as they are written: what the
/// declaration says, whatever the order, the spacing and the punctuation it says it in. A type
/// is given the name it is printed under, and a quoted name that needs no quotes loses them.
fn words(sql: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut rest = sql.chars().peekable();
    while let Some(first) = rest.next() {
        if first == '\'' || first == '"' {
            let mut text = String::from(first);
            loop {
                let next = rest.next().expect("a quoted text ends with its quote");
                text.push(next);
                // The quote written twice is one quote of the text.
                if next == first && rest.next_if_eq(&first).is_none() {
                    break;
                }
            }
            let inner = &text[1..text.len() - 1];
            let plain = first == '"'
                && inner.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
                && inner
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            found.push(if plain { inner.to_owned() } else { text });
        } else if first.is_ascii_alphanumeric() || first == '_' {
            let mut word = String::from(first.to_ascii_lowercase());
            while let Some(next) =
                rest.next_if(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '.')
            {
                word.push(next.to_ascii_lowercase());
            }
            match word.as_str() {
                "int" => found.push("integer".to_owned()),
                "bool" => found.push("boolean".to_owned()),
                "float" => found.push("real".to_owned()),
                "state_machine" => found.extend(["state".to_owned(), "machine".to_owned()]),
                _ => found.push(word),
            }
        }
    }
    found
}

/// Asserts that the text a table is printed back as says everything its author wrote and
/// nothing else. `written` is the table's `CREATE TABLE` and each `CREATE INDEX` on it.
fn assert_prints_what_was_written(written: &[&str], printed: &str, table: &str) {
    let written_words: Vec<String> = written.iter().flat_map(|sql| words(sql)).collect();
    let mut counts: BTreeMap<String, i64> = BTreeMap::new();
    for word in &written_words {
        *counts.entry(word.clone()).or_default() += 1;
    }
    for word in words(printed) {
        *counts.entry(word).or_default() -= 1;
    }
    // The limit on partitions is printed for every column with a PARTITION_KEY, whether or not
    // its author wrote one: 256 when none was written.
    let count = |word: &str| written_words.iter().filter(|w| *w == word).count() as i64;
    let defaulted = count("partition_key") - count("max_partitions");
    for word in ["max_partitions", "256"] {
        *counts.entry(word.to_owned()).or_default() += defaulted;
    }
    let lost: Vec<&String> = counts
        .iter()
        .filter(|(_, n)| **n > 0)
        .map(|(w, _)| w)
        .collect();
    let added: Vec<&String> = counts
        .iter()
        .filter(|(_, n)| **n < 0)
        .map(|(w, _)| w)
        .collect();
    assert!(
        lost.is_empty() && added.is_empty(),
        "the declaration of {table} is printed back without {lost:?} and with {added:?}, which \
         its author did not write.\nwritten:\n{}\nprinted:\n{printed}",
        written.join(";\n")
    );
}

/// Declares statements that must be accepted.
fn declare<S: AsRef<str>>(db: &Database, statements: impl IntoIterator<Item = S>) {
    for sql in statements {
        support::run(db, sql.as_ref());
    }
}

/// What a table is printed back as.
fn printed(db: &Database, table: &str) -> String {
    db.declaration(table)
        .unwrap_or_else(|error| panic!("the declaration of {table} was refused: {error:?}"))
}

/// The statements for each table, the table's own first, with the tables in the order they
/// were declared.
fn by_table(statements: &[String]) -> Vec<(String, Vec<&str>)> {
    let mut tables: Vec<(String, Vec<&str>)> = Vec::new();
    for sql in statements {
        let (table, own) = table_of(sql);
        if own {
            tables.push((table, vec![sql.as_str()]));
        } else {
            let of_table = tables.iter_mut().find(|(name, _)| *name == table);
            let (_, texts) = of_table.unwrap_or_else(|| panic!("an index before its table: {sql}"));
            texts.push(sql.as_str());
        }
    }
    tables
}

/// Declares `written` in a fresh store and asserts, of every table: that it is printed back
/// with what was written and nothing else; that what was printed, declared in an empty store,
/// is printed the same; and that it is printed the same after the store is closed and opened,
/// by the writer and by a reader. Gives back the printed texts by table.
fn assert_declared_and_printed_back(test: &str, written: &[String]) -> BTreeMap<String, String> {
    let folder = support::folder(test);
    let db = support::open(&folder);
    declare(&db, written);
    let tables = by_table(written);
    let mut names: Vec<&str> = tables.iter().map(|(table, _)| table.as_str()).collect();
    names.sort_unstable();
    assert_eq!(db.table_names(), names, "the tables of the store");

    let mut texts = BTreeMap::new();
    for (table, written) in &tables {
        let text = printed(&db, table);
        assert_prints_what_was_written(written, &text, table);
        texts.insert(table.clone(), text);
    }

    let again = support::open(&support::folder(&format!("{test}-again")));
    for (table, _) in &tables {
        declare(&again, statements(&texts[table]));
    }
    for (table, text) in &texts {
        assert_eq!(
            &printed(&again, table),
            text,
            "{table}, declared from its printed text in an empty store, is printed differently"
        );
    }

    support::close(db);
    let db = support::open(&folder);
    let reader = ReadSession::open(&folder).expect("open the store for reading");
    for (table, text) in &texts {
        assert_eq!(&printed(&db, table), text, "{table} after close and open");
        let read = reader.declaration(table).expect("a reader's declaration");
        assert_eq!(&read, text, "{table} as a reader prints it");
    }
    texts
}

#[test]
fn every_declaration_of_a_consumer_is_accepted_and_printed_back_as_written() {
    let (first, second) = consumer_schema();
    let own = |statements: &[String]| statements.iter().filter(|sql| table_of(sql).1).count();
    // The fixture holds 93 table texts, 6 of them second texts, and 25 index texts.
    assert_eq!((own(&first), first.len() - own(&first)), (87, 25));
    assert_eq!((own(&second), second.len()), (6, 6));
    let texts = assert_declared_and_printed_back("consumer-schema", &first);
    assert_eq!(texts.len(), 87);
}

#[test]
fn a_second_text_for_a_consumers_table_is_accepted_in_a_store_of_its_own() {
    let (first, second) = consumer_schema();
    for (number, sql) in second.iter().enumerate() {
        // The tables the text references, and the ones those reference, as first declared.
        let mut wanted = referenced(sql);
        loop {
            let before = wanted.len();
            for parent in &first {
                let (table, own) = table_of(parent);
                if own && wanted.contains(&table) {
                    wanted.extend(referenced(parent));
                }
            }
            if wanted.len() == before {
                break;
            }
        }
        let mut store: Vec<String> = first
            .iter()
            .filter(|parent| {
                let (table, own) = table_of(parent);
                own && wanted.contains(&table)
            })
            .cloned()
            .collect();
        store.push(sql.clone());
        let test = format!("consumer-schema-second-{number}");
        let texts = assert_declared_and_printed_back(&test, &store);
        assert!(texts.contains_key(&table_of(sql).0));
    }
}

/// Every clause a table and an index may declare that the consumer's schema does not hold, and
/// some it does, each on a table that says little else.
const EVERY_CLAUSE: &[&str] = &[
    "CREATE TABLE contexts (id UUID PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, \
     acl_id UUID)",
    "CREATE TABLE projects (id UUID PRIMARY KEY, \
     context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), \
     context_name TEXT REFERENCES contexts(name), name TEXT NOT NULL IMMUTABLE, \
     scope TEXT SCOPE_LABEL ('public', 'internal'), \
     acl_id UUID ACL REFERENCES acl_grants(acl_id), made_at TXID, \
     doc JSON NOT NULL DEFAULT '{}', tags JSON DEFAULT '[]', note TEXT DEFAULT '', \
     open BOOLEAN NOT NULL DEFAULT true, shut BOOLEAN DEFAULT false, UNIQUE (context_id, id)) \
     RETAIN 48 HOURS SYNC SAFE HISTORY ALL SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
    "CREATE INDEX projects_by_name ON projects (name DESC, id)",
    "CREATE INDEX projects_by_kind ON projects ((doc -> 'kind'))",
    "CREATE INDEX projects_by_owner ON projects ((doc -> 'owner' -> 'name'))",
    "CREATE TABLE memos (id INTEGER PRIMARY KEY, \
     scope TEXT SCOPE_LABEL_READ ('reader', 'writer') WRITE ('writer'), body TEXT) \
     SYNC PULL ONLY SYNC CONFLICT KEEP LATEST HISTORY CURRENT ONLY RETAIN 90 SECONDS",
    "CREATE TABLE plans (id UUID PRIMARY KEY, name TEXT NOT NULL)",
    "CREATE TABLE steps (id UUID PRIMARY KEY, description TEXT NOT NULL, status TEXT NOT NULL, \
     confidence REAL, created_at TIMESTAMP DEFAULT NOW(), \
     plan_id UUID REFERENCES plans(id) ON STATE archived PROPAGATE SET invalidated, \
     tenant TEXT NOT NULL, embedding VECTOR(4) PARTITION_KEY (tenant) SEARCH_MODE INDEXED) \
     STATE MACHINE (status: active -> [invalidated, superseded]) \
     PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated \
     PROPAGATE ON STATE invalidated EXCLUDE VECTOR \
     PROPAGATE ON STATE superseded EXCLUDE VECTOR",
    "CREATE TABLE reviews (id UUID PRIMARY KEY, status TEXT NOT NULL, \
     plan_id UUID REFERENCES plans(id) ON STATE archived PROPAGATE SET stale MAX DEPTH 2 \
     ABORT ON FAILURE) STATE MACHINE (status: open -> [stale], stale -> [closed]) \
     PROPAGATE ON EDGE CITES OUTGOING STATE stale SET stale MAX DEPTH 3 ABORT ON FAILURE \
     PROPAGATE ON EDGE BLOCKS BOTH STATE stale SET stale",
    "CREATE TABLE tasks (id UUID PRIMARY KEY, context_id UUID NOT NULL, \
     project_id UUID NOT NULL, \
     FOREIGN KEY (context_id, project_id) REFERENCES projects(context_id, id))",
    "CREATE TABLE task_results (id UUID PRIMARY KEY, task_id UUID NOT NULL, \
     success BOOLEAN NOT NULL)",
    "CREATE INDEX task_results_task_id_idx ON task_results (task_id)",
    "CREATE TABLE ranked (id UUID PRIMARY KEY, description TEXT NOT NULL, confidence REAL, \
     embedding VECTOR(4) RANK_POLICY (JOIN task_results ON task_id, \
     FORMULA 'coalesce({confidence}, 1.0) * coalesce({success}, 1.0)', \
     SORT_KEY effective_confidence))",
    "CREATE TABLE defaults (id INTEGER PRIMARY KEY, a TEXT DEFAULT NULL, b REAL DEFAULT 0.0, \
     c INTEGER DEFAULT -1, d REAL DEFAULT -2.5, e TEXT DEFAULT 'it''s', f BOOLEAN DEFAULT TRUE)",
    "CREATE TABLE one_key (a INTEGER, b TEXT, PRIMARY KEY (a))",
    "CREATE TABLE two_keys (a INTEGER NOT NULL, b TEXT NOT NULL, c TEXT, PRIMARY KEY (a, b))",
    "CREATE TABLE no_key (line TEXT NOT NULL) SYNC OFF",
    "CREATE TABLE links (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, \
     edge_type TEXT NOT NULL) DAG('DEPENDS_ON', 'BASED_ON')",
    "CREATE TABLE counted (id UUID PRIMARY KEY, context_id UUID NOT NULL, \
     connector_id TEXT NOT NULL, UNIQUE (context_id, id), \
     UNIQUE (context_id, connector_id, id)) \
     MAINTAIN COUNT (context_id), (context_id, connector_id)",
    "CREATE TABLE counted_all (id INTEGER PRIMARY KEY, kind TEXT, UNIQUE (kind, id)) \
     MAINTAIN COUNT ALL PREFIXES",
    "CREATE TABLE counted_none (id INTEGER PRIMARY KEY, kind TEXT) MAINTAIN COUNT NONE",
    "CREATE TABLE delivered (id INTEGER PRIMARY KEY) SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST \
     IMMUTABLE DELIVERY MANIFEST OVER delivered_parts, delivered_marks \
     EDGE DISCARD AFTER OUTCOME",
    "CREATE TABLE discard_never (id INTEGER PRIMARY KEY) SYNC PUSH ONLY \
     SYNC CONFLICT KEEP FIRST IMMUTABLE DELIVERY MANIFEST OVER discard_parts EDGE DISCARD NEVER",
    "CREATE TABLE discard_always (id INTEGER PRIMARY KEY) SYNC PUSH ONLY \
     SYNC CONFLICT KEEP FIRST IMMUTABLE EDGE DISCARD ALWAYS",
    "CREATE TABLE kept_minutes (id INTEGER PRIMARY KEY) RETAIN 30 MINUTES",
    "CREATE TABLE kept_days (id INTEGER PRIMARY KEY) RETAIN 7 DAYS SYNC OFF",
    "CREATE TABLE current_local (id TEXT PRIMARY KEY, state TEXT NOT NULL) \
     RETAIN 2 DAYS HISTORY CURRENT ONLY SYNC OFF",
    "CREATE TABLE order_a (id INTEGER PRIMARY KEY, body TEXT) \
     IMMUTABLE SYNC OFF SYNC CONFLICT KEEP FIRST MAINTAIN COUNT NONE",
    "CREATE TABLE order_b (id INTEGER PRIMARY KEY, body TEXT) \
     MAINTAIN COUNT NONE SYNC CONFLICT KEEP FIRST SYNC OFF IMMUTABLE",
    "CREATE TABLE \"Mixed Case\" (\"Id\" INTEGER PRIMARY KEY, \"two words\" TEXT)",
    "CREATE TABLE Folded (Id INTEGER PRIMARY KEY, Name TEXT)",
    "CREATE INDEX Folded_By_Name ON Folded (Name)",
];

#[test]
fn every_clause_a_table_may_declare_is_printed_back_and_declares_the_same_table_again() {
    // The fixture's reader of names knows plain lower-case names, so the two tables that are
    // written in other letters are declared here under the names they are stored by.
    let statements: Vec<String> = EVERY_CLAUSE
        .iter()
        .map(|sql| {
            sql.replace("\"Mixed Case\"", "mixed_case")
                .replace("Folded_By_Name ON Folded", "folded_by_name ON folded")
                .replace("TABLE Folded", "TABLE folded")
        })
        .collect();
    let texts = assert_declared_and_printed_back("every-clause", &statements);

    // Options written in two orders are printed in one.
    assert_eq!(
        texts["order_a"].replace("order_a", "order"),
        texts["order_b"].replace("order_b", "order")
    );
    // A default is printed as its value, in the engine's spelling.
    let projects = &texts["projects"];
    for default in [
        "doc JSON NOT NULL DEFAULT '{}'",
        "tags JSON DEFAULT '[]'",
        "note TEXT DEFAULT ''",
        "open BOOLEAN NOT NULL DEFAULT TRUE",
        "shut BOOLEAN DEFAULT FALSE",
    ] {
        assert!(projects.contains(default), "no `{default}` in:\n{projects}");
    }
    // How long rows are kept is printed in the unit its author wrote.
    assert!(projects.contains("RETAIN 48 HOURS SYNC SAFE"), "{projects}");
    assert!(
        texts["memos"].contains("RETAIN 90 SECONDS"),
        "{}",
        texts["memos"]
    );
    // A VECTOR column's clauses come in one order, the limit on partitions with its number
    // though its author wrote none.
    let steps = &texts["steps"];
    let clauses =
        "embedding VECTOR(4) PARTITION_KEY (tenant) MAX_PARTITIONS 256 SEARCH_MODE INDEXED";
    assert!(steps.contains(clauses), "no `{clauses}` in:\n{steps}");
    // A table that declares none of these says none of them, whatever the table beside it
    // declares.
    assert_eq!(
        texts["plans"],
        "CREATE TABLE plans (\n  id UUID PRIMARY KEY,\n  name TEXT NOT NULL\n);\n"
    );
    // The engine's own indexes are never printed: the ones behind a key, a UNIQUE rule and a
    // reference, and the two of a table of edges.
    for (table, text) in &texts {
        assert!(
            !text.contains("__"),
            "{table} prints an index of the engine:\n{text}"
        );
    }
}

#[test]
fn a_name_without_quotes_is_stored_in_lower_case_and_a_quoted_name_as_written() {
    let folder = support::folder("declared-names");
    let db = support::open(&folder);
    declare(&db, EVERY_CLAUSE.iter().rev().take(3).rev());
    assert_eq!(db.table_names(), ["Mixed Case", "folded"]);
    assert_eq!(
        printed(&db, "folded"),
        "CREATE TABLE folded (\n  id INTEGER PRIMARY KEY,\n  name TEXT\n);\n\
         CREATE INDEX folded_by_name ON folded (name);\n"
    );
    assert_eq!(
        printed(&db, "Mixed Case"),
        "CREATE TABLE \"Mixed Case\" (\n  \"Id\" INTEGER PRIMARY KEY,\n  \"two words\" TEXT\n);\n"
    );
    // The printed texts declare the same two tables in an empty store.
    let again = support::open(&support::folder("declared-names-again"));
    for table in ["Mixed Case", "folded"] {
        declare(&again, statements(&printed(&db, table)));
        assert_eq!(printed(&again, table), printed(&db, table));
    }
    // The table is found by its stored name alone.
    for missing in ["Folded", "FOLDED", "mixed case"] {
        assert!(
            matches!(db.declaration(missing), Err(Error::TableNotFound(name)) if name == missing),
            "{missing}"
        );
    }
    // Every spelling without quotes is the one table, and a refusal names it as it is stored.
    match support::refusal(&db, "CREATE TABLE FOLDED (id INTEGER PRIMARY KEY)") {
        Error::SchemaInvalid { reason } => assert!(reason.contains("'folded'"), "{reason}"),
        other => panic!("a second declaration of folded was refused as {other:?}"),
    }
    match support::refusal(&db, "SELECT Nope FROM Folded") {
        Error::ColumnNotFound { table, column } => {
            assert_eq!((table.as_str(), column.as_str()), ("folded", "nope"));
        }
        other => panic!("a column folded does not have was refused as {other:?}"),
    }
    match support::refusal(&db, "CREATE INDEX FOLDED_BY_NAME ON folded (id)") {
        Error::DuplicateIndex { table, index } => {
            assert_eq!(
                (table.as_str(), index.as_str()),
                ("folded", "folded_by_name")
            );
        }
        other => panic!("a second index of the name was refused as {other:?}"),
    }
}

#[test]
fn a_store_of_earlier_declarations_is_printed_as_it_was() {
    let folder = support::folder("earlier-declarations");
    let db = support::open(&folder);
    declare(
        &db,
        [
            "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, \
             title TEXT NOT NULL DEFAULT 'it''s', body TEXT DEFAULT NULL, \
             stars INTEGER DEFAULT 3, slug TEXT UNIQUE, \
             \"made at\" TIMESTAMP NOT NULL DEFAULT NOW(), score REAL DEFAULT -2.0, \
             kept BOOLEAN DEFAULT TRUE, tenant TEXT NOT NULL, \
             embedding VECTOR(384) PARTITION_KEY (tenant) SEARCH_MODE EXACT, \
             plain VECTOR(3), UNIQUE (title, stars))",
            "CREATE INDEX by_stars ON notes (stars DESC, \"made at\" ASC)",
            "CREATE INDEX by_body ON notes (body)",
            // A taken name with IF NOT EXISTS declares nothing.
            "CREATE TABLE IF NOT EXISTS notes (id UUID PRIMARY KEY)",
        ],
    );
    let text = "\
CREATE TABLE notes (
  id INTEGER PRIMARY KEY,
  title TEXT NOT NULL DEFAULT 'it''s',
  body TEXT DEFAULT NULL,
  stars INTEGER DEFAULT 3,
  slug TEXT UNIQUE,
  \"made at\" TIMESTAMP NOT NULL DEFAULT NOW(),
  score REAL DEFAULT -2.0,
  kept BOOLEAN DEFAULT TRUE,
  tenant TEXT NOT NULL,
  embedding VECTOR(384) PARTITION_KEY (tenant) MAX_PARTITIONS 256 SEARCH_MODE EXACT,
  plain VECTOR(3),
  UNIQUE (title, stars)
);
CREATE INDEX by_stars ON notes (stars DESC, \"made at\");
CREATE INDEX by_body ON notes (body);
";
    assert_eq!(printed(&db, "notes"), text);
    support::close(db);
    let db = support::open(&folder);
    assert_eq!(printed(&db, "notes"), text, "after close and open");
}

/// What a refused declaration must come as.
enum Refused {
    /// `SchemaInvalid`, with a sentence that holds each of these.
    Schema(&'static [&'static str]),
    /// `ParseError`, with a sentence that holds each of these.
    Parse(&'static [&'static str]),
    /// `ParseError` or `SchemaInvalid`: no document says which of the two.
    ParseOrSchema,
    /// `StoredProcNotSupported`.
    StoredProcedure,
}

#[test]
fn a_refused_declaration_says_why_and_declares_nothing() {
    let folder = support::folder("refused-declarations");
    let db = support::open(&folder);
    declare(
        &db,
        [
            "CREATE TABLE plans (id UUID PRIMARY KEY, name TEXT NOT NULL, context_id UUID, \
             UNIQUE (context_id, id))",
        ],
    );
    let columns: Vec<String> = (1..=33).map(|n| format!("c{n:02}")).collect();
    let typed: Vec<String> = columns.iter().map(|c| format!("{c} INTEGER")).collect();
    let wide = |rule: &str| {
        format!(
            "CREATE TABLE gauges ({}, {rule} ({}))",
            typed.join(", "),
            columns.join(", ")
        )
    };
    let key = "id INTEGER PRIMARY KEY";
    let cases: Vec<(String, Refused)> = vec![
        // Keeping only current versions on a table that sends its rows out and keeps the first
        // write.
        (
            format!("CREATE TABLE gauges ({key}) HISTORY CURRENT ONLY"),
            Refused::Schema(&["gauges", "HISTORY CURRENT ONLY"]),
        ),
        (
            format!("CREATE TABLE gauges ({key}) HISTORY CURRENT ONLY SYNC PUSH ONLY"),
            Refused::Schema(&["gauges", "HISTORY CURRENT ONLY"]),
        ),
        (
            format!("CREATE TABLE gauges ({key}) IMMUTABLE RETAIN 7 DAYS"),
            Refused::Schema(&["gauges", "IMMUTABLE", "RETAIN"]),
        ),
        (
            format!("CREATE TABLE gauges ({key}) IMMUTABLE HISTORY CURRENT ONLY SYNC OFF"),
            Refused::Schema(&["gauges", "IMMUTABLE", "HISTORY CURRENT ONLY"]),
        ),
        (
            format!("CREATE TABLE gauges ({key}) RETAIN 7 DAYS SYNC SAFE SYNC OFF"),
            Refused::Schema(&["gauges", "SYNC SAFE"]),
        ),
        (
            format!("CREATE TABLE gauges ({key}) RETAIN 7 DAYS SYNC SAFE SYNC PULL ONLY"),
            Refused::Schema(&["gauges", "SYNC SAFE"]),
        ),
        // A reference to columns no key or UNIQUE rule of the parent covers.
        (
            format!("CREATE TABLE gauges ({key}, plan TEXT REFERENCES plans(name))"),
            Refused::Schema(&["gauges"]),
        ),
        (
            format!(
                "CREATE TABLE gauges ({key}, context_id UUID, name TEXT, \
                 FOREIGN KEY (context_id, name) REFERENCES plans(context_id, name))"
            ),
            Refused::Schema(&["gauges"]),
        ),
        (
            format!("CREATE TABLE gauges ({key}, kind TEXT) MAINTAIN COUNT (kind)"),
            Refused::Schema(&["gauges", "MAINTAIN COUNT"]),
        ),
        (
            format!("CREATE TABLE gauges ({key}, kind TEXT) MAINTAIN COUNT (id)"),
            Refused::Schema(&["gauges", "MAINTAIN COUNT"]),
        ),
        (
            format!("CREATE TABLE gauges ({key}, source_id UUID, target_id UUID) DAG('NEEDS')"),
            Refused::Schema(&["gauges", "DAG"]),
        ),
        // EXCLUDE VECTOR needs a state machine, and a state of it: else the rows stay searchable unsaid.
        (
            format!(
                "CREATE TABLE gauges ({key}, status TEXT, v VECTOR(4)) PROPAGATE ON STATE gone EXCLUDE VECTOR"
            ),
            Refused::Schema(&["gauges", "gone"]),
        ),
        (
            format!(
                "CREATE TABLE gauges ({key}, status TEXT, v VECTOR(4)) STATE MACHINE (status: on -> [off]) PROPAGATE ON STATE gone EXCLUDE VECTOR"
            ),
            Refused::Schema(&["gauges", "gone"]),
        ),
        // Anything that is not a literal or NOW() is no default this version keeps.
        (
            format!("CREATE TABLE gauges ({key}, c INTEGER DEFAULT $p)"),
            Refused::Schema(&[]),
        ),
        (
            format!("CREATE TABLE gauges ({key}, c INTEGER DEFAULT -'x')"),
            Refused::Schema(&[]),
        ),
        // More than 32 columns under a key, a UNIQUE rule and an index.
        (wide("PRIMARY KEY"), Refused::Schema(&["gauges", "32"])),
        (wide("UNIQUE"), Refused::Schema(&["gauges", "32"])),
        // A direction written inside RETAIN names the clause to write instead.
        (
            format!("CREATE TABLE gauges ({key}) RETAIN 24 HOURS SYNC SAFE PUSH ONLY"),
            Refused::Parse(&["SYNC PUSH ONLY"]),
        ),
        (
            format!("CREATE TABLE gauges ({key}) RETAIN 24 HOURS SYNC SAFE TWO WAY"),
            Refused::Parse(&["SYNC TWO WAY"]),
        ),
        // One key to a table.
        (
            "CREATE TABLE gauges (a INTEGER PRIMARY KEY, b TEXT, PRIMARY KEY (a, b))".to_owned(),
            Refused::ParseOrSchema,
        ),
        (
            "CREATE TABLE gauges (a INTEGER, b TEXT, PRIMARY KEY (a, b), PRIMARY KEY (a))"
                .to_owned(),
            Refused::ParseOrSchema,
        ),
        (
            "CREATE PROCEDURE tidy () AS 'DELETE FROM plans'".to_owned(),
            Refused::StoredProcedure,
        ),
        (
            "create function one () RETURNS INTEGER AS 'SELECT 1'".to_owned(),
            Refused::StoredProcedure,
        ),
    ];
    let declared = printed(&db, "plans");
    for (sql, refused) in &cases {
        let error = support::refusal(&db, sql);
        let says = |text: &str, holds: &[&str]| holds.iter().all(|part| text.contains(part));
        let as_expected = match (refused, &error) {
            (Refused::Schema(holds), Error::SchemaInvalid { reason }) => says(reason, holds),
            (Refused::Parse(holds), Error::ParseError(text)) => says(text, holds),
            (Refused::ParseOrSchema, Error::ParseError(_) | Error::SchemaInvalid { .. }) => true,
            (Refused::StoredProcedure, Error::StoredProcNotSupported) => true,
            _ => false,
        };
        assert!(as_expected, "{sql}\nwas refused as {error:?}");
        assert_eq!(db.table_names(), ["plans"], "after {sql}");
    }
    assert_eq!(printed(&db, "plans"), declared);

    // An index over 33 columns of a table that has them is refused the same way, and one over
    // 32 is made.
    let keyed = typed
        .join(", ")
        .replacen("INTEGER", "INTEGER PRIMARY KEY", 1);
    support::run(&db, &format!("CREATE TABLE broad ({keyed})"));
    let index = |count: usize| {
        let columns = columns[..count].join(", ");
        format!("CREATE INDEX broad_by ON broad ({columns})")
    };
    match support::refusal(&db, &index(33)) {
        Error::SchemaInvalid { reason } => assert!(reason.contains("32"), "{reason}"),
        other => panic!("an index over 33 columns was refused as {other:?}"),
    }
    assert!(!printed(&db, "broad").contains("broad_by"));
    support::run(&db, &index(32));
    assert!(printed(&db, "broad").contains("broad_by"));
}

#[test]
fn a_declaration_that_says_a_thing_twice_or_two_ways_is_a_parse_error_that_names_it() {
    let db = support::open(&support::folder("declared-twice"));
    let key = "CREATE TABLE t (id UUID PRIMARY KEY)";
    let cases = [
        // A direction inside RETAIN names the clause to write instead.
        (
            format!("{key} RETAIN 24 HOURS SYNC SAFE PUSH ONLY"),
            "SYNC PUSH ONLY",
        ),
        (
            format!("{key} RETAIN 24 HOURS SYNC SAFE TWO WAY"),
            "SYNC TWO WAY",
        ),
        (
            "ALTER TABLE t SET RETAIN 24 HOURS SYNC SAFE PUSH ONLY".to_owned(),
            "SYNC PUSH ONLY",
        ),
        // An option written twice.
        (format!("{key} SYNC OFF SYNC TWO WAY"), "SYNC"),
        (format!("{key} RETAIN 1 DAYS RETAIN 2 DAYS"), "RETAIN"),
        (format!("{key} HISTORY ALL HISTORY CURRENT ONLY"), "HISTORY"),
        (
            format!("{key} SYNC CONFLICT KEEP FIRST SYNC CONFLICT KEEP LATEST"),
            "SYNC CONFLICT",
        ),
        (
            format!("{key} MAINTAIN COUNT NONE MAINTAIN COUNT ALL PREFIXES"),
            "MAINTAIN COUNT",
        ),
        // A column that says a thing twice, in a new table and in an added column.
        (
            "CREATE TABLE t (id UUID PRIMARY KEY, o UUID REFERENCES p(id) REFERENCES q(id))"
                .to_owned(),
            "REFERENCES",
        ),
        (
            "ALTER TABLE t ADD COLUMN c TEXT NOT NULL NOT NULL".to_owned(),
            "NOT NULL",
        ),
        // A list that names a column twice, or a column the table does not have.
        (
            "CREATE TABLE t (a INTEGER, b TEXT, PRIMARY KEY (a, a))".to_owned(),
            "'a'",
        ),
        (
            "CREATE TABLE t (a INTEGER, b TEXT, PRIMARY KEY (a, nope))".to_owned(),
            "'nope'",
        ),
        (
            "CREATE TABLE t (a INTEGER, b TEXT, FOREIGN KEY (a, nope) REFERENCES p(a, b))"
                .to_owned(),
            "'nope'",
        ),
        // A reference of as many columns on one side as on the other.
        (
            "CREATE TABLE t (a INTEGER, b TEXT, FOREIGN KEY (a, b) REFERENCES p(a))".to_owned(),
            "FOREIGN KEY",
        ),
        // DEFAULT takes a declared list away, and a new table has none.
        (
            format!("{key} MAINTAIN COUNT DEFAULT"),
            "MAINTAIN COUNT DEFAULT",
        ),
        // What is neither this version's nor a later one's is a syntax error at the word.
        (
            "ALTER TABLE t DROP COLUMN c RESTRICT CASCADE".to_owned(),
            "'CASCADE'",
        ),
        ("ALTER TABLE t SET HISTORY NONE".to_owned(), "'NONE'"),
        (
            "ALTER TABLE t ALTER COLUMN v SET PARTITION_KEY (a)".to_owned(),
            "'PARTITION_KEY'",
        ),
        ("ALTER TABLE t RENAME a TO b".to_owned(), "'a'"),
        ("DROP INDEX i".to_owned(), "ends too soon"),
        ("DROP TABLE".to_owned(), "ends too soon"),
        (format!("{key} SYNC SIDEWAYS"), "'SIDEWAYS'"),
        (format!("{key} EDGE DISCARD SOMETIMES"), "'SOMETIMES'"),
        (format!("{key} RETAIN 7 WEEKS"), "'WEEKS'"),
    ];
    for (sql, names) in &cases {
        match support::refusal(&db, sql) {
            Error::ParseError(text) => {
                assert!(text.contains(names), "{sql}\nwas refused with: {text}");
            }
            other => panic!("{sql}\nwas not refused as a parse error: {other:?}"),
        }
    }
    // A column that expires is refused by name.
    let sql = "ALTER TABLE t ADD COLUMN until TIMESTAMP EXPIRES";
    match support::refusal(&db, sql) {
        Error::PlanError(text) => assert_eq!(
            text, "EXPIRES is not supported in this version of the library",
            "{sql}"
        ),
        other => panic!("{sql}\nwas not refused by name: {other:?}"),
    }
    // The tuning clauses of an index no version reads are refused by name, and the sentence says
    // what to write instead.
    for (sql, name) in [
        ("ALTER TABLE t ALTER COLUMN v SET SEARCH DEFAULT", "SEARCH"),
        (
            "ALTER TABLE t ALTER COLUMN v SET SEARCH (TARGET_ACCURACY = 95)",
            "SEARCH",
        ),
        ("ALTER TABLE t ALTER COLUMN v SET IVF DEFAULT", "IVF"),
        (
            "ALTER TABLE t ALTER COLUMN v SET IVF (LISTS = 64, PROBES = 8, REFINE_FACTOR = 2)",
            "IVF",
        ),
    ] {
        match support::refusal(&db, sql) {
            Error::PlanError(text) => assert_eq!(
                text,
                format!(
                    "{name} (...) is not part of this library: the index sizes its own groups and \
                     measures how far a search scans; for the true nearest rows write USE VECTOR \
                     EXACT or SEARCH_MODE EXACT, and to move the size from which AUTO uses the \
                     index write AUTO_INDEX_AT"
                ),
                "{sql}"
            ),
            other => panic!("{sql}\nwas not refused by name: {other:?}"),
        }
    }
    for sql in [
        "create procedure tidy",
        "CREATE FUNCTION one () RETURNS INTEGER AS 'SELECT 1'",
        "Create Function one",
    ] {
        let error = support::refusal(&db, sql);
        assert!(
            matches!(error, Error::StoredProcNotSupported),
            "{sql}: {error:?}"
        );
    }
    // A table may still be named for either word.
    support::run(
        &db,
        "CREATE TABLE procedure (id INTEGER PRIMARY KEY, function TEXT)",
    );
}

#[test]
fn a_rank_policy_finds_the_column_it_joins_by_one_rule_or_is_refused() {
    let folder = support::folder("rank-policy-join");
    let db = support::open(&folder);
    declare(
        &db,
        [
            "CREATE TABLE results (id UUID PRIMARY KEY, score REAL, label TEXT, task_id UUID)",
            "CREATE INDEX results_by_task ON results (task_id)",
        ],
    );
    let policy = |join: &str, formula: &str| {
        format!(
            "embedding VECTOR(4) RANK_POLICY (JOIN {join}, FORMULA '{formula}', SORT_KEY by_score)"
        )
    };
    let by_key = policy("results ON id", "coalesce({score}, 1.0)");
    let by_task = policy("results ON task_id", "coalesce({score}, 1.0)");

    // Joined by the joined table's key: the anchor's column named for the joined table, in the
    // singular or as it is; then the anchor's column of the same name. Joined by another
    // column: the anchor's column of the same name. The column found is the one the policy
    // reads, so it is the one that may not be dropped; the column beside it may.
    let found = [
        (
            "singular",
            format!("id UUID PRIMARY KEY, result_id UUID, other UUID, {by_key}"),
            "result_id",
        ),
        (
            "whole",
            format!("id UUID PRIMARY KEY, results_id UUID, other UUID, {by_key}"),
            "results_id",
        ),
        (
            "named",
            format!("id UUID PRIMARY KEY, task_id UUID, other UUID, {by_task}"),
            "task_id",
        ),
    ];
    for (table, columns, joined) in &found {
        support::run(&db, &format!("CREATE TABLE {table} ({columns})"));
        match support::refusal(&db, &format!("ALTER TABLE {table} DROP COLUMN {joined}")) {
            Error::DropBlockedByRankPolicy {
                table: of,
                column,
                policy,
            } => {
                assert_eq!(
                    (of.as_str(), column.as_str(), policy.as_str()),
                    (*table, *joined, "by_score")
                );
            }
            other => panic!("dropping {table}.{joined} was refused as {other:?}"),
        }
        support::run(&db, &format!("ALTER TABLE {table} DROP COLUMN other"));
    }

    // An `id` on both sides of the join is ambiguous in a formula, in a pair of tables whose
    // ids are numbers.
    support::run(&db, "CREATE TABLE counted (id INTEGER PRIMARY KEY, n REAL)");
    let sql = format!(
        "CREATE TABLE ambiguous (id INTEGER PRIMARY KEY, counted_id INTEGER, {})",
        policy("counted ON id", "{id} * 2.0")
    );
    assert!(
        matches!(support::refusal(&db, &sql), Error::RankPolicyInvalid { .. }),
        "{sql}\nnames an `id` that both tables have"
    );

    // What cannot stand is refused as the policy's own kind, naming the VECTOR column, and
    // declares nothing.
    let tables = db.table_names();
    let refused = [
        // Two columns the first rule finds.
        format!("id UUID PRIMARY KEY, result_id UUID, results_id UUID, {by_key}"),
    ];
    let formulas = [
        // A table, and a column, that are not there; a column no index leads with.
        ("nowhere ON id", "1.0"),
        ("results ON nope", "1.0"),
        ("results ON label", "1.0"),
        // A formula over a column that is no number, over one that is not there, and one that
        // does not read.
        ("results ON id", "coalesce({label}, 1.0)"),
        ("results ON id", "coalesce({nope}, 1.0)"),
        ("results ON id", "coalesce({score}, "),
        // A formula outside the documented grammar (`{column}`, `{vector_score}`, numbers,
        // `+`, `*`, parentheses, `coalesce(expr, literal)`): subtraction, division, a function
        // call, a name without braces; numbers the SQL grammar has no form for; a trailing
        // operator; a brace never closed; a dotted reference; a column as the second value of
        // `coalesce`; a minus sign; two values and no operator; a subquery.
        ("results ON id", "{score} - 1.0"),
        ("results ON id", "{score} / 2.0"),
        ("results ON id", "{score} * NOW()"),
        ("results ON id", "{score} * score"),
        ("results ON id", "{score} * 1e1"),
        ("results ON id", "({score} + )"),
        ("results ON id", "{score + {score}"),
        ("results ON id", "{results.score}"),
        ("results ON id", "coalesce({score}, {score})"),
        ("results ON id", "{score} {score}"),
        ("results ON id", "{score} * (SELECT 1)"),
    ];
    let refused = refused.into_iter().chain(
        formulas.map(|(join, formula)| format!("id UUID PRIMARY KEY, {}", policy(join, formula))),
    );
    for columns in refused {
        let sql = format!("CREATE TABLE ranked ({columns})");
        match support::refusal(&db, &sql) {
            Error::RankPolicyInvalid { index, reason } => {
                assert_eq!(
                    (index.table.as_str(), index.column.as_str()),
                    ("ranked", "embedding")
                );
                assert!(!reason.is_empty(), "{sql}");
            }
            other => panic!("{sql}\nwas refused as {other:?}"),
        }
        assert_eq!(db.table_names(), tables, "after {sql}");
    }

    // Formulas that stand: `{vector_score}` beside `*` and a joined column; `coalesce` is a
    // function, so a column that is named coalesce is not read by the policy and may be dropped;
    // a column read through parentheses and `coalesce` is still read; capitals and a whole
    // number are read.
    support::run(&db, "CREATE TABLE lone (id UUID PRIMARY KEY, score REAL)");
    for (table, columns, join, formula) in [
        (
            "scored",
            "result_id UUID, coalesce REAL",
            "results",
            "coalesce({score}, 1.0)",
        ),
        (
            "nested",
            "lone_id UUID",
            "lone",
            "({vector_score} + 1) * coalesce({score}, 0.5)",
        ),
        ("capitals", "lone_id UUID", "lone", "COALESCE({score}, 1)"),
    ] {
        let sql = format!(
            "CREATE TABLE {table} (id UUID PRIMARY KEY, {columns}, {})",
            policy(&format!("{join} ON id"), formula)
        );
        support::run(&db, &sql);
        assert!(
            printed(&db, table).contains(formula),
            "{table} prints its formula as written"
        );
        if table == "nested" {
            // Read through the parentheses and the `coalesce`, so `score` may not be dropped
            // from the table it is joined from.
            match support::refusal(&db, "ALTER TABLE lone DROP COLUMN score") {
                Error::DropBlockedByRankPolicy { table, column, .. } => {
                    assert_eq!((table.as_str(), column.as_str()), ("lone", "score"));
                }
                other => panic!("dropping lone.score was refused as {other:?}"),
            }
        }
    }
    support::run(&db, "ALTER TABLE scored DROP COLUMN coalesce");
}
