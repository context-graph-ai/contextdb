# What a table and an index may declare, and what a declaration refuses. A refused declaration
# declares nothing. What a rule then does to a write is the subject of the script named after
# the rule; how a declaration is printed back is checked by `tests/declarations.rs`.

# What earlier versions read is still read, with its meaning. IF NOT EXISTS declares a free name
# and leaves a taken one as it is, whatever the second declaration says.
CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, title TEXT NOT NULL DEFAULT 'none', body TEXT NULL, UNIQUE (title, body));
INSERT INTO notes (id, body) VALUES (1, 'first');
CREATE TABLE IF NOT EXISTS notes (id UUID PRIMARY KEY, other REAL) IMMUTABLE;
----
rows affected: 0  (read 0, compared 0)

SELECT other FROM notes;
----
error: ColumnNotFound

UPDATE notes SET body = 'again' WHERE id = 1;
----
rows affected: 1  (read 1, compared 0)

SELECT id, title, body FROM notes ORDER BY id;
----
id | title | body  (read 1, compared 0)
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
id  (read 3, compared 2)
1

# A plain index with a direction for each column.
CREATE INDEX notes_by_title ON notes (title DESC, id ASC);
INSERT INTO notes (id, title, body) VALUES (2, 'b', 'x'), (3, 'a', 'y');
SELECT id, title FROM notes ORDER BY title DESC, id;
----
id | title  (read 3, compared 0)
1 | none
2 | b
3 | a

# The indexes behind a key and a UNIQUE rule are __pk_<column> and __unique_<column>; a user's
# index may not take either prefix, and may sit beside them.
CREATE INDEX __pk_id ON notes (id);
----
error: ReservedIndexName

CREATE INDEX __unique_title ON notes (title);
----
error: ReservedIndexName

CREATE INDEX notes_by_body ON notes (body);
SELECT id FROM notes WHERE body = 'x';
----
id  (read 1, compared 0)
2

# A name written without quotes is its lower-case letters, and a name in quotes is the letters
# as written: `Items`, `ITEMS` and `items` are one table, and "Items" is another.
CREATE TABLE Items (Id INTEGER PRIMARY KEY, Name TEXT);
INSERT INTO ITEMS (ID, NAME) VALUES (1, 'a');
SELECT Id, NAME FROM items;
----
id | name  (read 1, compared 0)
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
Id | Name  (read 1, compared 0)
7 | quoted

SELECT id, name FROM items;
----
id | name  (read 1, compared 0)
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
kept  (read 0, compared 0)
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

# A STATE MACHINE over a table that is IMMUTABLE could never move.
CREATE TABLE refused (id INTEGER PRIMARY KEY, status TEXT NOT NULL) STATE MACHINE (status: a -> [b]) IMMUTABLE;
----
error: SchemaInvalid

# Plain RETAIN takes any direction.
CREATE TABLE retain_local (id INTEGER PRIMARY KEY) RETAIN 7 DAYS SYNC OFF;
CREATE TABLE retain_pull (id INTEGER PRIMARY KEY) RETAIN 7 DAYS SYNC PULL ONLY;

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
id  (read 2, compared 0)
1
2

CREATE TABLE filled (id INTEGER PRIMARY KEY, doc JSON);
INSERT INTO filled (id, doc) VALUES (1, '{"kind": "a"}');
CREATE INDEX filled_by_kind ON filled ((doc -> 'kind'));
SELECT id FROM filled;
----
id  (read 1, compared 0)
1

# A JSON-path index may name a path of two keys.
CREATE TABLE chained (id INTEGER PRIMARY KEY, doc JSON);
CREATE INDEX chained_by_path ON chained ((doc -> 'k' -> 'j'));
INSERT INTO chained (id, doc) VALUES (1, '{"k": {"j": 1}}');
SELECT id FROM chained;
----
id  (read 1, compared 0)
1

# A refused declaration declares nothing, whatever refuses it. Keeping only current versions
# beside sending rows out or keeping the first write; a reference no key or UNIQUE rule of the
# parent covers; counts without an index led by them; a DAG without its columns; EXCLUDE VECTOR
# without the state it names; a default that is not a literal or NOW(); and more than 32 columns
# under a key or a UNIQUE rule.
CREATE TABLE parents (id UUID PRIMARY KEY, name TEXT NOT NULL, context_id UUID, UNIQUE (context_id, id));
CREATE TABLE gauges (id INTEGER PRIMARY KEY) HISTORY CURRENT ONLY;
----
error: SchemaInvalid

CREATE TABLE gauges (id INTEGER PRIMARY KEY) HISTORY CURRENT ONLY SYNC PUSH ONLY;
----
error: SchemaInvalid

CREATE TABLE gauges (id INTEGER PRIMARY KEY) IMMUTABLE RETAIN 7 DAYS;
----
error: SchemaInvalid

CREATE TABLE gauges (id INTEGER PRIMARY KEY) IMMUTABLE HISTORY CURRENT ONLY SYNC OFF;
----
error: SchemaInvalid

CREATE TABLE gauges (id INTEGER PRIMARY KEY) RETAIN 7 DAYS SYNC SAFE SYNC OFF;
----
error: SchemaInvalid

CREATE TABLE gauges (id INTEGER PRIMARY KEY) RETAIN 7 DAYS SYNC SAFE SYNC PULL ONLY;
----
error: SchemaInvalid

CREATE TABLE gauges (id INTEGER PRIMARY KEY, plan TEXT REFERENCES parents(name));
----
error: SchemaInvalid

CREATE TABLE gauges (id INTEGER PRIMARY KEY, context_id UUID, name TEXT, FOREIGN KEY (context_id, name) REFERENCES parents(context_id, name));
----
error: SchemaInvalid

CREATE TABLE gauges (id INTEGER PRIMARY KEY, kind TEXT) MAINTAIN COUNT (kind);
----
error: SchemaInvalid

CREATE TABLE gauges (id INTEGER PRIMARY KEY, kind TEXT) MAINTAIN COUNT (id);
----
error: SchemaInvalid

CREATE TABLE gauges (id INTEGER PRIMARY KEY, source_id UUID, target_id UUID) DAG('NEEDS');
----
error: SchemaInvalid

CREATE TABLE gauges (id INTEGER PRIMARY KEY, status TEXT, v VECTOR(4)) PROPAGATE ON STATE gone EXCLUDE VECTOR;
----
error: SchemaInvalid

CREATE TABLE gauges (id INTEGER PRIMARY KEY, status TEXT, v VECTOR(4)) STATE MACHINE (status: on -> [off]) PROPAGATE ON STATE gone EXCLUDE VECTOR;
----
error: SchemaInvalid

CREATE TABLE gauges (id INTEGER PRIMARY KEY, c INTEGER DEFAULT $p);
----
error: SchemaInvalid

CREATE TABLE gauges (id INTEGER PRIMARY KEY, c INTEGER DEFAULT -'x');
----
error: SchemaInvalid

CREATE TABLE gauges (c01 INTEGER, c02 INTEGER, c03 INTEGER, c04 INTEGER, c05 INTEGER, c06 INTEGER, c07 INTEGER, c08 INTEGER, c09 INTEGER, c10 INTEGER, c11 INTEGER, c12 INTEGER, c13 INTEGER, c14 INTEGER, c15 INTEGER, c16 INTEGER, c17 INTEGER, c18 INTEGER, c19 INTEGER, c20 INTEGER, c21 INTEGER, c22 INTEGER, c23 INTEGER, c24 INTEGER, c25 INTEGER, c26 INTEGER, c27 INTEGER, c28 INTEGER, c29 INTEGER, c30 INTEGER, c31 INTEGER, c32 INTEGER, c33 INTEGER, PRIMARY KEY (c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33));
----
error: SchemaInvalid

CREATE TABLE gauges (c01 INTEGER, c02 INTEGER, c03 INTEGER, c04 INTEGER, c05 INTEGER, c06 INTEGER, c07 INTEGER, c08 INTEGER, c09 INTEGER, c10 INTEGER, c11 INTEGER, c12 INTEGER, c13 INTEGER, c14 INTEGER, c15 INTEGER, c16 INTEGER, c17 INTEGER, c18 INTEGER, c19 INTEGER, c20 INTEGER, c21 INTEGER, c22 INTEGER, c23 INTEGER, c24 INTEGER, c25 INTEGER, c26 INTEGER, c27 INTEGER, c28 INTEGER, c29 INTEGER, c30 INTEGER, c31 INTEGER, c32 INTEGER, c33 INTEGER, UNIQUE (c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33));
----
error: SchemaInvalid

CREATE TABLE gauges (a INTEGER PRIMARY KEY, b TEXT, PRIMARY KEY (a, b));
----
error: SchemaInvalid

SELECT id FROM gauges;
----
error: TableNotFound

# An index over 33 columns is refused the same way, and one over 32 is made.
CREATE TABLE broad (c01 INTEGER PRIMARY KEY, c02 INTEGER, c03 INTEGER, c04 INTEGER, c05 INTEGER, c06 INTEGER, c07 INTEGER, c08 INTEGER, c09 INTEGER, c10 INTEGER, c11 INTEGER, c12 INTEGER, c13 INTEGER, c14 INTEGER, c15 INTEGER, c16 INTEGER, c17 INTEGER, c18 INTEGER, c19 INTEGER, c20 INTEGER, c21 INTEGER, c22 INTEGER, c23 INTEGER, c24 INTEGER, c25 INTEGER, c26 INTEGER, c27 INTEGER, c28 INTEGER, c29 INTEGER, c30 INTEGER, c31 INTEGER, c32 INTEGER, c33 INTEGER);
CREATE INDEX broad_by ON broad (c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33);
----
error: SchemaInvalid

CREATE INDEX broad_by ON broad (c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32);

# Saying a thing twice or two ways is a parse error: a direction inside RETAIN, an option or a
# column's clause written twice, a list that names a column twice or one the table lacks, a
# reference of unequal sides, DEFAULT for a new table's counts, and words no version reads.
CREATE TABLE gauges (id INTEGER PRIMARY KEY) RETAIN 24 HOURS SYNC SAFE PUSH ONLY;
----
error: ParseError

CREATE TABLE gauges (id INTEGER PRIMARY KEY) RETAIN 24 HOURS SYNC SAFE TWO WAY;
----
error: ParseError

ALTER TABLE t SET RETAIN 24 HOURS SYNC SAFE PUSH ONLY;
----
error: ParseError

CREATE TABLE gauges (a INTEGER, b TEXT, PRIMARY KEY (a, b), PRIMARY KEY (a));
----
error: ParseError

CREATE TABLE t (id UUID PRIMARY KEY) SYNC OFF SYNC TWO WAY;
----
error: ParseError

CREATE TABLE t (id UUID PRIMARY KEY) RETAIN 1 DAYS RETAIN 2 DAYS;
----
error: ParseError

CREATE TABLE t (id UUID PRIMARY KEY) HISTORY ALL HISTORY CURRENT ONLY;
----
error: ParseError

CREATE TABLE t (id UUID PRIMARY KEY) SYNC CONFLICT KEEP FIRST SYNC CONFLICT KEEP LATEST;
----
error: ParseError

CREATE TABLE t (id UUID PRIMARY KEY) MAINTAIN COUNT NONE MAINTAIN COUNT ALL PREFIXES;
----
error: ParseError

CREATE TABLE t (id UUID PRIMARY KEY, o UUID REFERENCES p(id) REFERENCES q(id));
----
error: ParseError

ALTER TABLE t ADD COLUMN c TEXT NOT NULL NOT NULL;
----
error: ParseError

CREATE TABLE t (a INTEGER, b TEXT, PRIMARY KEY (a, a));
----
error: ParseError

CREATE TABLE t (a INTEGER, b TEXT, PRIMARY KEY (a, nope));
----
error: ParseError

CREATE TABLE t (a INTEGER, b TEXT, FOREIGN KEY (a, nope) REFERENCES p(a, b));
----
error: ParseError

CREATE TABLE t (a INTEGER, b TEXT, FOREIGN KEY (a, b) REFERENCES p(a));
----
error: ParseError

CREATE TABLE t (id UUID PRIMARY KEY) MAINTAIN COUNT DEFAULT;
----
error: ParseError

ALTER TABLE t DROP COLUMN c RESTRICT CASCADE;
----
error: ParseError

ALTER TABLE t SET HISTORY NONE;
----
error: ParseError

ALTER TABLE t ALTER COLUMN v SET PARTITION_KEY (a);
----
error: ParseError

ALTER TABLE t RENAME a TO b;
----
error: ParseError

DROP INDEX i;
----
error: IndexNotFound | index not found: i

DROP TABLE;
----
error: ParseError

CREATE TABLE t (id UUID PRIMARY KEY) SYNC SIDEWAYS;
----
error: ParseError

CREATE TABLE t (id UUID PRIMARY KEY) EDGE DISCARD SOMETIMES;
----
error: ParseError

CREATE TABLE t (id UUID PRIMARY KEY) RETAIN 7 WEEKS;
----
error: ParseError

# What a later version reads, and the tuning clauses no version reads, are refused by name;
# a procedure or function is refused as one, and either word may still name a table.
ALTER TABLE t ALTER COLUMN v SET SEARCH DEFAULT;
----
error: PlanError

ALTER TABLE t ALTER COLUMN v SET SEARCH (TARGET_ACCURACY = 95);
----
error: PlanError

ALTER TABLE t ALTER COLUMN v SET IVF DEFAULT;
----
error: PlanError

ALTER TABLE t ALTER COLUMN v SET IVF (LISTS = 64, PROBES = 8, REFINE_FACTOR = 2);
----
error: PlanError

create procedure tidy;
----
error: StoredProcNotSupported

CREATE FUNCTION one () RETURNS INTEGER AS 'SELECT 1';
----
error: StoredProcNotSupported

Create Function one;
----
error: StoredProcNotSupported

CREATE TABLE procedure (id INTEGER PRIMARY KEY, function TEXT);

CREATE TABLE expiry (id INTEGER PRIMARY KEY, expires TIMESTAMP EXPIRES) RETAIN 10 SECONDS;
INSERT INTO expiry VALUES (1, NULL), (2, '2099-01-01T00:00:00Z'), (3, NULL);
SELECT id FROM expiry WHERE expires IS NULL ORDER BY id;
----
id  (read 2, compared 0)
1
3

UPDATE expiry SET expires = '2099-01-02T00:00:00Z' WHERE id = 1;
SELECT id FROM expiry WHERE expires IS NULL ORDER BY id;
----
id  (read 1, compared 0)
3

UPDATE expiry SET expires = NULL WHERE id = 2;
SELECT id FROM expiry WHERE expires IS NULL ORDER BY id;
----
id  (read 2, compared 0)
2
3

# CONTEXT_ID and ACL are declared on a UUID column, SCOPE_LABEL on a TEXT column.
CREATE TABLE ctx_bad (id INTEGER PRIMARY KEY, ctx TEXT CONTEXT_ID);
----
error: SchemaInvalid | schema invalid: table 'ctx_bad': declare CONTEXT_ID on a UUID column

CREATE TABLE label_bad (id INTEGER PRIMARY KEY, scope INTEGER SCOPE_LABEL ('public'));
----
error: SchemaInvalid

CREATE TABLE acl_bad (id INTEGER PRIMARY KEY, acl_id TEXT ACL REFERENCES acl_grants(acl_id));
----
error: SchemaInvalid

# A vector DEFAULT written as quoted text is checked for length; a sync-safe table needs a multi-column key, not a UNIQUE.
CREATE TABLE vdef_bad (id INTEGER PRIMARY KEY, v VECTOR(2) DEFAULT '[1.0, 2.0, 3.0]');
----
error: ColumnTypeMismatch | column type mismatch: vdef_bad.v expected VECTOR(2), got Text

CREATE TABLE vdef_ok (id INTEGER PRIMARY KEY, v VECTOR(2) DEFAULT '[1.0, 2.0]');
CREATE TABLE sy_multi (a INTEGER, b INTEGER, PRIMARY KEY (a, b)) RETAIN 1 DAYS SYNC SAFE;
CREATE TABLE sy_uniq (a INTEGER, b INTEGER, UNIQUE (a, b)) RETAIN 1 DAYS SYNC SAFE;
----
error: SchemaInvalid | schema invalid: table 'sy_uniq' declares RETAIN ... SYNC SAFE without a PRIMARY KEY: declare a PRIMARY KEY so delivered rows have an identity

CREATE TABLE rp_missing (id INTEGER PRIMARY KEY, v VECTOR(2) RANK_POLICY (JOIN nowhere ON id, FORMULA '{vector_score}', SORT_KEY s));
----
error: RankPolicyInvalid | invalid rank policy on rp_missing.v: it joins the unknown table 'nowhere'
