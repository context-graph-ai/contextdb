# What a table and an index may declare, and what a declaration refuses. A refused declaration
# declares nothing. What a rule then does to a write is the subject of the script named after
# the rule; how a declaration is printed back is checked by `tests/declarations.rs`.

# What earlier versions read is still read, with its meaning. IF NOT EXISTS declares a free name
# and leaves a taken one as it is, whatever the second declaration says.
CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, title TEXT NOT NULL DEFAULT 'none', body TEXT NULL, UNIQUE (title, body));
INSERT INTO notes (id, body) VALUES (1, 'first');
CREATE TABLE IF NOT EXISTS notes (id UUID PRIMARY KEY, other REAL) IMMUTABLE;
----
rows affected: 0

SELECT other FROM notes;
----
error: ColumnNotFound

UPDATE notes SET body = 'again' WHERE id = 1;
----
rows affected: 1

SELECT id, title, body FROM notes ORDER BY id;
----
id | title | body
1 | none | again

# The clauses of a VECTOR column: its partition key, its limit and its search mode.
CREATE TABLE docs (
  id INTEGER PRIMARY KEY,
  tenant TEXT NOT NULL,
  v VECTOR(3) PARTITION_KEY (tenant) MAX_PARTITIONS 2 SEARCH_MODE EXACT,
  w VECTOR(2) SEARCH_MODE AUTO,
  plain VECTOR(2)
);
INSERT INTO docs (id, tenant, v) VALUES (1, 'north', [1.0, 0.0, 0.0]), (2, 'south', [0.0, 1.0, 0.0]);
INSERT INTO docs (id, tenant, v) VALUES (3, 'east', [0.0, 0.0, 1.0]);
----
error: VectorPartitionLimitExceeded

SELECT id FROM docs ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 1;
----
id
1

# A plain index with a direction for each column.
CREATE INDEX notes_by_title ON notes (title DESC, id ASC);
INSERT INTO notes (id, title, body) VALUES (2, 'b', 'x'), (3, 'a', 'y');
SELECT id, title FROM notes ORDER BY title DESC, id;
----
id | title
1 | none
2 | b
3 | a

# A name written without quotes is its lower-case letters, and a name in quotes is the letters
# as written: `Items`, `ITEMS` and `items` are one table, and "Items" is another.
CREATE TABLE Items (Id INTEGER PRIMARY KEY, Name TEXT);
INSERT INTO ITEMS (ID, NAME) VALUES (1, 'a');
SELECT Id, NAME FROM items;
----
id | name
1 | a

CREATE TABLE ITEMS (id INTEGER PRIMARY KEY);
----
error: SchemaInvalid

SELECT id FROM "Items";
----
error: TableNotFound

SELECT "Id" FROM items;
----
error: ColumnNotFound

CREATE TABLE "Items" ("Id" INTEGER PRIMARY KEY, "Name" TEXT);
INSERT INTO "Items" ("Id", "Name") VALUES (7, 'quoted');
SELECT "Id", "Name" FROM "Items";
----
Id | Name
7 | quoted

SELECT id, name FROM items;
----
id | name
1 | a

# An index's name folds the same way, and belongs to its table: a second index of the name on
# the same table is refused, and another table may use the name.
CREATE INDEX Items_By_Name ON Items (Name);
CREATE INDEX items_by_name ON items (id);
----
error: DuplicateIndex

CREATE INDEX items_by_name ON notes (body);

# What a column may declare: a reference to a parent's key or unique column, IMMUTABLE, the
# column that holds the row's Context, the labels a reader needs, and the type TXID.
CREATE TABLE contexts (id UUID PRIMARY KEY, name TEXT NOT NULL UNIQUE);
CREATE TABLE projects (
  id UUID PRIMARY KEY,
  context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id),
  context_name TEXT REFERENCES contexts(name),
  name TEXT NOT NULL IMMUTABLE,
  scope TEXT SCOPE_LABEL ('public', 'internal'),
  made_at TXID,
  UNIQUE (context_id, id)
);
CREATE TABLE memos (
  id INTEGER PRIMARY KEY,
  scope TEXT SCOPE_LABEL_READ ('reader', 'writer') WRITE ('writer'),
  body TEXT
);

# The column that holds a row's access id, checked against a table of grants.
CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID);
CREATE TABLE guarded (id INTEGER PRIMARY KEY, acl_id UUID ACL REFERENCES acl_grants(acl_id), payload TEXT);

# A reference of several columns is an element of the table, to a parent's key or unique rule.
CREATE TABLE tasks (
  id UUID PRIMARY KEY,
  context_id UUID NOT NULL,
  project_id UUID NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  FOREIGN KEY (context_id, project_id) REFERENCES projects(context_id, id)
) STATE MACHINE (status: active -> [done, dropped], done -> [archived]);

# A reference may move its row's state when the parent's state changes.
CREATE TABLE plans (id UUID PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE steps (
  id UUID PRIMARY KEY,
  description TEXT NOT NULL,
  status TEXT NOT NULL,
  confidence REAL,
  created_at TIMESTAMP DEFAULT NOW(),
  plan_id UUID REFERENCES plans(id)
    ON STATE archived PROPAGATE SET invalidated,
  embedding VECTOR(4)
) STATE MACHINE (status: active -> [invalidated, superseded])
  PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated
  PROPAGATE ON STATE invalidated EXCLUDE VECTOR
  PROPAGATE ON STATE superseded EXCLUDE VECTOR;
CREATE TABLE reviews (
  id UUID PRIMARY KEY,
  status TEXT NOT NULL,
  plan_id UUID REFERENCES plans(id) ON STATE archived PROPAGATE SET stale MAX DEPTH 2 ABORT ON FAILURE
) STATE MACHINE (status: open -> [stale])
  PROPAGATE ON EDGE CITES OUTGOING STATE stale SET stale MAX DEPTH 3 ABORT ON FAILURE
  PROPAGATE ON EDGE BLOCKS BOTH STATE stale SET stale;

# The primary key as an element of the table, over one column or several, and a table with no
# primary key, whose rows are kept as they arrive: two that hold the same values are two rows.
CREATE TABLE one_key (a INTEGER, b TEXT, PRIMARY KEY (a));
CREATE TABLE two_keys (a INTEGER NOT NULL, b TEXT NOT NULL, c TEXT, PRIMARY KEY (a, b));
CREATE TABLE lines (line TEXT NOT NULL);
INSERT INTO lines (line) VALUES ('same'), ('same');
SELECT COUNT(*) AS kept FROM lines;
----
kept
2

# Every option of a table, alone.
CREATE TABLE o_immutable (id INTEGER PRIMARY KEY, body TEXT) IMMUTABLE;
CREATE TABLE o_dag (
  id UUID PRIMARY KEY,
  source_id UUID NOT NULL,
  target_id UUID NOT NULL,
  edge_type TEXT NOT NULL
) DAG('DEPENDS_ON', 'BASED_ON');
CREATE TABLE o_retain_seconds (id INTEGER PRIMARY KEY) RETAIN 90 SECONDS;
CREATE TABLE o_retain_minutes (id INTEGER PRIMARY KEY) RETAIN 30 MINUTES;
CREATE TABLE o_retain_hours (id INTEGER PRIMARY KEY) RETAIN 24 HOURS SYNC SAFE;
CREATE TABLE o_retain_days (id INTEGER PRIMARY KEY) RETAIN 7 DAYS;
CREATE TABLE o_history_all (id INTEGER PRIMARY KEY) HISTORY ALL;
CREATE TABLE o_sync_off (id INTEGER PRIMARY KEY) SYNC OFF;
CREATE TABLE o_sync_push (id INTEGER PRIMARY KEY) SYNC PUSH ONLY;
CREATE TABLE o_sync_pull (id INTEGER PRIMARY KEY) SYNC PULL ONLY;
CREATE TABLE o_sync_two_way (id INTEGER PRIMARY KEY) SYNC TWO WAY;
CREATE TABLE o_keep_first (id INTEGER PRIMARY KEY) SYNC CONFLICT KEEP FIRST;
CREATE TABLE o_keep_latest (id INTEGER PRIMARY KEY) SYNC CONFLICT KEEP LATEST;
CREATE TABLE o_counts (
  id UUID PRIMARY KEY,
  context_id UUID NOT NULL,
  connector_id TEXT NOT NULL,
  source_instance_ref TEXT NOT NULL,
  UNIQUE (context_id, id),
  UNIQUE (context_id, connector_id, source_instance_ref, id)
) MAINTAIN COUNT (context_id), (context_id, connector_id, source_instance_ref);
CREATE TABLE o_counts_all (id INTEGER PRIMARY KEY, kind TEXT, UNIQUE (kind, id)) MAINTAIN COUNT ALL PREFIXES;
CREATE TABLE o_counts_none (id INTEGER PRIMARY KEY, kind TEXT) MAINTAIN COUNT NONE;
CREATE TABLE o_manifest (id INTEGER PRIMARY KEY) SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE
  DELIVERY MANIFEST OVER o_manifest_parts, o_manifest_marks
  EDGE DISCARD AFTER OUTCOME;
CREATE TABLE o_discard_never (id INTEGER PRIMARY KEY) SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE
  DELIVERY MANIFEST OVER o_discard_parts EDGE DISCARD NEVER;
CREATE TABLE o_discard_always (id INTEGER PRIMARY KEY) SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE
  EDGE DISCARD ALWAYS;

# How long rows are kept, how many versions, where rows travel and which write wins are four
# separate clauses, and compose on one table. Keeping only the current version of each row is
# its own clause, with the two declarations that let it stand.
CREATE TABLE windows (id UUID PRIMARY KEY, body TEXT)
  RETAIN 48 HOURS SYNC SAFE HISTORY ALL SYNC TWO WAY SYNC CONFLICT KEEP FIRST;
CREATE TABLE current_latest (id TEXT PRIMARY KEY, state TEXT NOT NULL) HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST;
CREATE TABLE current_local (id TEXT PRIMARY KEY, state TEXT NOT NULL) RETAIN 2 DAYS HISTORY CURRENT ONLY SYNC OFF;

# A partitioned VECTOR column stands under every direction.
CREATE TABLE p_off (id INTEGER PRIMARY KEY, t TEXT NOT NULL, v VECTOR(2) PARTITION_KEY (t)) SYNC OFF;
CREATE TABLE p_push (id INTEGER PRIMARY KEY, t TEXT NOT NULL, v VECTOR(2) PARTITION_KEY (t)) SYNC PUSH ONLY;
CREATE TABLE p_pull (id INTEGER PRIMARY KEY, t TEXT NOT NULL, v VECTOR(2) PARTITION_KEY (t)) SYNC PULL ONLY;
CREATE TABLE p_two_way (id INTEGER PRIMARY KEY, t TEXT NOT NULL, v VECTOR(2) PARTITION_KEY (t)) SYNC TWO WAY;

# A table that keeps only current versions, sends its rows out and keeps the first write would
# hand another machine a newest value as the first: refused. KEEP LATEST or SYNC OFF is the way
# out, as above.
CREATE TABLE refused (id TEXT PRIMARY KEY, state TEXT NOT NULL) HISTORY CURRENT ONLY;
----
error: SchemaInvalid

CREATE TABLE refused (id TEXT PRIMARY KEY, state TEXT NOT NULL) HISTORY CURRENT ONLY SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST;
----
error: SchemaInvalid

# An IMMUTABLE table has no row to expire and no older version to drop.
CREATE TABLE refused (id INTEGER PRIMARY KEY) IMMUTABLE RETAIN 7 DAYS;
----
error: SchemaInvalid

CREATE TABLE refused (id INTEGER PRIMARY KEY) IMMUTABLE HISTORY CURRENT ONLY SYNC OFF;
----
error: SchemaInvalid

# SYNC SAFE waits for delivery, so it needs a direction that delivers. Plain RETAIN takes any.
CREATE TABLE refused (id INTEGER PRIMARY KEY) RETAIN 7 DAYS SYNC SAFE SYNC OFF;
----
error: SchemaInvalid

CREATE TABLE refused (id INTEGER PRIMARY KEY) RETAIN 7 DAYS SYNC SAFE SYNC PULL ONLY;
----
error: SchemaInvalid

CREATE TABLE retain_local (id INTEGER PRIMARY KEY) RETAIN 7 DAYS SYNC OFF;
CREATE TABLE retain_pull (id INTEGER PRIMARY KEY) RETAIN 7 DAYS SYNC PULL ONLY;

# RETAIN names no direction: one written inside it is a syntax error.
CREATE TABLE refused (id INTEGER PRIMARY KEY) RETAIN 24 HOURS SYNC SAFE PUSH ONLY;
----
error: ParseError

# A reference names columns that a key or a UNIQUE rule of the parent covers.
CREATE TABLE refused (id INTEGER PRIMARY KEY, plan_name TEXT REFERENCES plans(name));
----
error: SchemaInvalid

CREATE TABLE refused (
  id INTEGER PRIMARY KEY,
  context_id UUID,
  name TEXT,
  FOREIGN KEY (context_id, name) REFERENCES projects(context_id, name)
);
----
error: SchemaInvalid

# A kept count needs an index that leads with its list, and is not kept where a key already
# says the count is at most one.
CREATE TABLE refused (id INTEGER PRIMARY KEY, kind TEXT) MAINTAIN COUNT (kind);
----
error: SchemaInvalid

CREATE TABLE refused (id INTEGER PRIMARY KEY, kind TEXT) MAINTAIN COUNT (id);
----
error: SchemaInvalid

# DAG is for a table of edges: source_id, target_id and edge_type.
CREATE TABLE refused (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL) DAG('DEPENDS_ON');
----
error: SchemaInvalid

SELECT id FROM refused;
----
error: TableNotFound

# A key, a UNIQUE rule and an index name at most 32 columns.
CREATE TABLE refused (c01 INTEGER, c02 INTEGER, c03 INTEGER, c04 INTEGER, c05 INTEGER, c06 INTEGER, c07 INTEGER, c08 INTEGER, c09 INTEGER, c10 INTEGER, c11 INTEGER, c12 INTEGER, c13 INTEGER, c14 INTEGER, c15 INTEGER, c16 INTEGER, c17 INTEGER, c18 INTEGER, c19 INTEGER, c20 INTEGER, c21 INTEGER, c22 INTEGER, c23 INTEGER, c24 INTEGER, c25 INTEGER, c26 INTEGER, c27 INTEGER, c28 INTEGER, c29 INTEGER, c30 INTEGER, c31 INTEGER, c32 INTEGER, c33 INTEGER, PRIMARY KEY (c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33));
----
error: SchemaInvalid

CREATE TABLE refused (c01 INTEGER, c02 INTEGER, c03 INTEGER, c04 INTEGER, c05 INTEGER, c06 INTEGER, c07 INTEGER, c08 INTEGER, c09 INTEGER, c10 INTEGER, c11 INTEGER, c12 INTEGER, c13 INTEGER, c14 INTEGER, c15 INTEGER, c16 INTEGER, c17 INTEGER, c18 INTEGER, c19 INTEGER, c20 INTEGER, c21 INTEGER, c22 INTEGER, c23 INTEGER, c24 INTEGER, c25 INTEGER, c26 INTEGER, c27 INTEGER, c28 INTEGER, c29 INTEGER, c30 INTEGER, c31 INTEGER, c32 INTEGER, c33 INTEGER, PRIMARY KEY (c01), UNIQUE (c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33));
----
error: SchemaInvalid

SELECT c01 FROM refused;
----
error: TableNotFound

CREATE TABLE wide (c01 INTEGER, c02 INTEGER, c03 INTEGER, c04 INTEGER, c05 INTEGER, c06 INTEGER, c07 INTEGER, c08 INTEGER, c09 INTEGER, c10 INTEGER, c11 INTEGER, c12 INTEGER, c13 INTEGER, c14 INTEGER, c15 INTEGER, c16 INTEGER, c17 INTEGER, c18 INTEGER, c19 INTEGER, c20 INTEGER, c21 INTEGER, c22 INTEGER, c23 INTEGER, c24 INTEGER, c25 INTEGER, c26 INTEGER, c27 INTEGER, c28 INTEGER, c29 INTEGER, c30 INTEGER, c31 INTEGER, c32 INTEGER, c33 INTEGER, PRIMARY KEY (c01), UNIQUE (c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32));
CREATE INDEX wide_33 ON wide (c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33);
----
error: SchemaInvalid

CREATE INDEX wide_32 ON wide (c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32);
CREATE TABLE wide_key (c01 INTEGER, c02 INTEGER, c03 INTEGER, c04 INTEGER, c05 INTEGER, c06 INTEGER, c07 INTEGER, c08 INTEGER, c09 INTEGER, c10 INTEGER, c11 INTEGER, c12 INTEGER, c13 INTEGER, c14 INTEGER, c15 INTEGER, c16 INTEGER, c17 INTEGER, c18 INTEGER, c19 INTEGER, c20 INTEGER, c21 INTEGER, c22 INTEGER, c23 INTEGER, c24 INTEGER, c25 INTEGER, c26 INTEGER, c27 INTEGER, c28 INTEGER, c29 INTEGER, c30 INTEGER, c31 INTEGER, c32 INTEGER, c33 INTEGER, PRIMARY KEY (c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32));

# Clauses of a later version are still refused by their names, and stored procedures and
# functions by their own kind.
CREATE TABLE refused (id INTEGER PRIMARY KEY, until TIMESTAMP EXPIRES);
----
error: PlanError

CREATE PROCEDURE tidy () AS 'DELETE FROM notes';
----
error: StoredProcNotSupported

CREATE FUNCTION one () RETURNS INTEGER AS 'SELECT 1';
----
error: StoredProcNotSupported

SELECT id FROM refused;
----
error: TableNotFound

# An index names columns the table has and can index: a JSON column is not indexable, and a REAL
# may be indexed but is no key and under no UNIQUE rule.
CREATE TABLE people (id INTEGER PRIMARY KEY, score REAL, born INTEGER, doc JSON);
CREATE INDEX by_age ON people (age DESC);
----
error: ColumnNotFound

CREATE INDEX by_doc ON people (doc DESC);
----
error: ColumnNotIndexable

CREATE INDEX by_score ON people (score DESC);
CREATE TABLE ratios (id INTEGER PRIMARY KEY, ratio REAL UNIQUE);
----
error: ColumnNotIndexable

CREATE TABLE ratios (id INTEGER PRIMARY KEY, ratio REAL, UNIQUE (id, ratio));
----
error: ColumnNotIndexable

# A JSON-path index is declared and stored but filled by a later step: a table with one is
# written to like any other, and the index is made on a table that already has rows.
CREATE TABLE kinds (id INTEGER PRIMARY KEY, doc JSON);
CREATE INDEX kinds_by_kind ON kinds ((doc -> 'kind'));
INSERT INTO kinds (id, doc) VALUES (1, '{"kind": "a"}'), (2, '{"kind": "b"}');
UPDATE kinds SET doc = '{"kind": "c"}' WHERE id = 2;
SELECT id FROM kinds ORDER BY id;
----
id
1
2

CREATE TABLE filled (id INTEGER PRIMARY KEY, doc JSON);
INSERT INTO filled (id, doc) VALUES (1, '{"kind": "a"}');
CREATE INDEX filled_by_kind ON filled ((doc -> 'kind'));
SELECT id FROM filled;
----
id
1
