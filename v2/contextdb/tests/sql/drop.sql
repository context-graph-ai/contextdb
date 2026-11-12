# DROP INDEX and DROP TABLE, and what a drop is refused for. A refused drop changes nothing.
# That a drop leaves none of the dropped thing's bytes in the store is checked by
# `tests/alter.rs`.

CREATE TABLE notes (id INTEGER PRIMARY KEY, tag TEXT, body TEXT);
CREATE TABLE tags (id INTEGER PRIMARY KEY, tag TEXT);
INSERT INTO notes (id, tag, body) VALUES (1, 'a', 'first'), (2, 'b', 'second'), (3, 'a', 'third');
INSERT INTO tags (id, tag) VALUES (1, 'a');

# An index is dropped by its name and its table. The rows are as they were, and a filter the
# index served is still answered.
CREATE INDEX by_tag ON notes (tag);
CREATE INDEX by_tag ON tags (tag);
DROP INDEX by_tag ON notes;
SELECT id FROM notes WHERE tag = 'a' ORDER BY id;
----
id
1
3

# An index that is not there is refused by name, and with IF EXISTS is nothing to do.
DROP INDEX by_tag ON notes;
----
error: IndexNotFound

DROP INDEX IF EXISTS by_tag ON notes;
----
rows affected: 0

DROP INDEX IF EXISTS by_tag ON notes;
----
rows affected: 0

DROP INDEX never_made ON notes;
----
error: IndexNotFound

DROP INDEX by_tag ON nope;
----
error: TableNotFound

# The index of the same name on another table was not touched, and the name is free again on
# this one.
DROP INDEX by_tag ON tags;
DROP INDEX by_tag ON tags;
----
error: IndexNotFound

CREATE INDEX by_tag ON notes (tag DESC);
DROP INDEX IF EXISTS By_Tag ON Notes;
DROP INDEX by_tag ON notes;
----
error: IndexNotFound

# A table is dropped with its rows and its indexes.
CREATE INDEX by_body ON notes (body);
DROP TABLE notes;
SELECT id FROM notes;
----
error: TableNotFound

INSERT INTO notes (id, tag, body) VALUES (9, 'z', 'late');
----
error: TableNotFound

DROP INDEX by_body ON notes;
----
error: TableNotFound

# A table that is not there is refused by name, and with IF EXISTS is nothing to do.
DROP TABLE notes;
----
error: TableNotFound

DROP TABLE IF EXISTS notes;
----
rows affected: 0

DROP TABLE IF EXISTS never_made;
----
rows affected: 0

# The other table is as it was.
SELECT id, tag FROM tags ORDER BY id;
----
id | tag
1 | a

# The name is free: a table declared under it has none of the old columns, rows or indexes.
CREATE TABLE notes (id INTEGER PRIMARY KEY, other TEXT);
SELECT tag FROM notes;
----
error: ColumnNotFound

SELECT COUNT(*) AS n FROM notes;
----
n
0

INSERT INTO notes (id, other) VALUES (1, 'new');
CREATE INDEX by_body ON notes (other);
SELECT id, other FROM notes ORDER BY id;
----
id | other
1 | new

# A name folds in DROP as it does everywhere, and a quoted name is the letters as written.
CREATE TABLE "Mixed Case" (id INTEGER PRIMARY KEY);
DROP TABLE Notes;
DROP TABLE "mixed case";
----
error: TableNotFound

DROP TABLE "Mixed Case";
SELECT id FROM "Mixed Case";
----
error: TableNotFound

# A table another table references is not dropped while the reference stands.
CREATE TABLE parents (id INTEGER PRIMARY KEY);
CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id));
INSERT INTO parents (id) VALUES (1);
DROP TABLE parents;
----
error: SchemaInvalid

DROP TABLE IF EXISTS parents;
----
error: SchemaInvalid

SELECT id FROM parents;
----
id
1

DROP TABLE children;
DROP TABLE parents;

# The three columns that make a table a table of edges are not dropped or renamed while the
# table declares DAG. On a table that declares none, a drop of one is a drop like any other.
CREATE TABLE edges (
  id INTEGER PRIMARY KEY,
  source_id UUID NOT NULL,
  target_id UUID NOT NULL,
  edge_type TEXT NOT NULL,
  note TEXT
) DAG('DEPENDS_ON');
ALTER TABLE edges DROP COLUMN edge_type;
----
error: SchemaInvalid

ALTER TABLE edges DROP COLUMN source_id CASCADE;
----
error: SchemaInvalid

ALTER TABLE edges RENAME COLUMN target_id TO goal;
----
error: SchemaInvalid

ALTER TABLE edges DROP COLUMN note;
CREATE TABLE links (
  id INTEGER PRIMARY KEY,
  source_id UUID NOT NULL,
  target_id UUID NOT NULL,
  edge_type TEXT NOT NULL
);
INSERT INTO links (id, source_id, target_id, edge_type)
VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'CITES');
ALTER TABLE links DROP COLUMN edge_type;
SELECT id, source_id, target_id FROM links;
----
id | source_id | target_id
1 | 00000000-0000-0000-0000-000000000001 | 00000000-0000-0000-0000-000000000002

# What a rank policy reads is not dropped while the policy stands: the table it joins, the
# index it joins through, and the columns its formula and its join name.
CREATE TABLE task_results (id UUID PRIMARY KEY, task_id UUID NOT NULL, success BOOLEAN NOT NULL, note TEXT);
CREATE INDEX task_results_task_id_idx ON task_results (task_id);
CREATE TABLE ranked (
  id UUID PRIMARY KEY,
  description TEXT NOT NULL,
  confidence REAL,
  embedding VECTOR(4) RANK_POLICY (
    JOIN task_results ON task_id,
    FORMULA 'coalesce({confidence}, 1.0) * coalesce({success}, 1.0)',
    SORT_KEY effective_confidence
  )
);
DROP TABLE task_results;
----
error: DropBlockedByRankPolicy

DROP INDEX task_results_task_id_idx ON task_results;
----
error: DropBlockedByRankPolicy

ALTER TABLE task_results DROP COLUMN success;
----
error: DropBlockedByRankPolicy

ALTER TABLE task_results DROP COLUMN task_id CASCADE;
----
error: DropBlockedByRankPolicy

ALTER TABLE ranked DROP COLUMN confidence;
----
error: DropBlockedByRankPolicy

ALTER TABLE task_results DROP COLUMN note;
DROP TABLE ranked;
DROP INDEX task_results_task_id_idx ON task_results;
DROP TABLE task_results;

# A dropped VECTOR column, and a dropped table with one, leave no vector behind: the column
# declared again has no partition, so a limit of one takes a row under another key, and a
# search finds only what was written since.
CREATE TABLE docs (id INTEGER PRIMARY KEY, tenant TEXT NOT NULL, v VECTOR(2) PARTITION_KEY (tenant) MAX_PARTITIONS 1);
INSERT INTO docs (id, tenant, v) VALUES (1, 'north', [1.0, 0.0]);
INSERT INTO docs (id, tenant, v) VALUES (2, 'south', [0.0, 1.0]);
----
error: VectorPartitionLimitExceeded

ALTER TABLE docs DROP COLUMN v;
ALTER TABLE docs ADD COLUMN v VECTOR(2) PARTITION_KEY (tenant) MAX_PARTITIONS 1;
INSERT INTO docs (id, tenant, v) VALUES (2, 'south', [0.0, 1.0]);
SELECT id FROM docs ORDER BY v <=> [1.0, 0.0] LIMIT 5;
----
id
2

DROP TABLE docs;
CREATE TABLE docs (id INTEGER PRIMARY KEY, tenant TEXT NOT NULL, v VECTOR(2) PARTITION_KEY (tenant) MAX_PARTITIONS 1);
INSERT INTO docs (id, tenant, v) VALUES (7, 'east', [1.0, 0.0]);
SELECT id FROM docs ORDER BY v <=> [1.0, 0.0] LIMIT 5;
----
id
7
