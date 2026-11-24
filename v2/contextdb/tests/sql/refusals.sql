# What is refused by name, and what is an empty answer and not a refusal.

CREATE TABLE notes (id INTEGER PRIMARY KEY, tag TEXT);
INSERT INTO notes (id, tag) VALUES (1, 'a');

# Text that is not a statement is refused before anything runs.
SELEC id FROM notes;
----
error: ParseError

SELECT id FROM;
----
error: ParseError

CREATE USER someone;
----
error: ParseError

CREATE ROLE someone;
----
error: ParseError

GRANT SELECT ON notes TO someone;
----
error: ParseError

# A column the table does not have is never answered with a missing or made-up column.
SELECT nope FROM notes;
----
error: ColumnNotFound | column not found: notes.nope

SELECT id, nope FROM notes;
----
error: ColumnNotFound | column not found: notes.nope

CREATE INDEX on_nope ON notes (nope);
----
error: ColumnNotFound

# A sort key the engine cannot work out is refused, never answered unsorted: this version has
# no function of that name.
SELECT tag FROM notes ORDER BY UPPER(tag);
----
error: PlanError

# A key that names no row is an empty answer.
SELECT id, tag FROM notes WHERE id = 404;
----
id | tag  (read 0, compared 0)

SELECT id, tag FROM notes ORDER BY id;
----
id | tag  (read 1, compared 0)
1 | a

# A parameter with no value is refused with PlanError when the statement is planned, as Postgres
# refuses an undefined parameter: before a row is read, whatever the statement, and nothing of
# the statement is written.
SELECT id FROM notes WHERE id = $id;
----
error: PlanError

SELECT id FROM notes WHERE id = 404 AND tag = $tag;
----
error: PlanError

SELECT id FROM notes ORDER BY id LIMIT $n;
----
error: PlanError

INSERT INTO notes (id, tag) VALUES (2, $tag);
----
error: PlanError

UPDATE notes SET tag = $tag WHERE id = 404;
----
error: PlanError

DELETE FROM notes WHERE id = $id;
----
error: PlanError

SELECT id, tag FROM notes ORDER BY id;
----
id | tag  (read 1, compared 0)
1 | a

# The reader's cache share is declared when a session opens, so setting it later is answered by
# name and showing it is a row; compaction is paced by its ratios, and vectors are read
# from their blocks.
SET READ_CACHE_SHARE 50;
----
error: PlanError

SHOW READ_CACHE_SHARE;
----
declared | default_percent | effective_percent | effective_bytes | budget_bytes  (read 0, compared 0)
NULL | NULL | NULL | NULL | NULL

SET STORAGE_COMPACTION MIN_INTERVAL 10 MINUTES;

SET STORAGE_COMPACTION MAX_SWEEPS 4;
----
error: PlanError

SET VECTOR_SERVING_CACHE '64M';
----
error: PlanError

SHOW VECTOR_SERVING_CACHE;
----
error: PlanError

# A duplicate under a UNIQUE rule of three columns is refused; one column of the three differing
# is no duplicate.
CREATE TABLE u (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER, UNIQUE (a, b, c));
INSERT INTO u (id, a, b, c) VALUES (1, 1, 'x', 1);
INSERT INTO u (id, a, b, c) VALUES (2, 1, 'x', 2);
INSERT INTO u (id, a, b, c) VALUES (3, 1, 'x', 1);
----
error: UniqueViolation

SELECT id FROM u ORDER BY id;
----
id  (read 2, compared 0)
1
2

# A second CREATE TABLE of a name is refused whether it differs from the first or repeats it,
# and the table keeps its declaration and its rows.
CREATE TABLE ledger (id INTEGER PRIMARY KEY, entry TEXT NOT NULL);
INSERT INTO ledger (id, entry) VALUES (1, 'first'), (2, 'second');
CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER);
----
error: SchemaInvalid

CREATE TABLE ledger (id INTEGER PRIMARY KEY, entry TEXT NOT NULL);
----
error: SchemaInvalid

SELECT amount FROM ledger;
----
error: ColumnNotFound

SELECT id, entry FROM ledger ORDER BY id;
----
id | entry  (read 2, compared 0)
1 | first
2 | second

CREATE TABLE wrong_expiry (id INT EXPIRES);
----
error: SchemaInvalid

CREATE TABLE two_expiries (a TIMESTAMP EXPIRES, b TIMESTAMP EXPIRES);
----
error: SchemaInvalid
