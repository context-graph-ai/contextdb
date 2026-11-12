# IMMUTABLE on a table: its rows are inserted once and never updated or deleted. IMMUTABLE on a
# column: the value is set when the row is inserted and never changed, while the row's other
# columns stay as free as before.

CREATE TABLE documents (
  id UUID PRIMARY KEY,
  data JSON,
  embedding VECTOR(3),
  title TEXT,
  created_at TIMESTAMP DEFAULT NOW()
) IMMUTABLE;
INSERT INTO documents (id, data, embedding, title)
VALUES ('00000000-0000-0000-0000-000000000001', '{"k": 1}', [1.0, 0.0, 0.0], 'first');
----
rows affected: 1

# Refused whether or not the statement would reach a row: the table says so, not the data.
UPDATE documents SET title = 'changed' WHERE id = '00000000-0000-0000-0000-000000000001';
----
error: ImmutableTable

UPDATE documents SET title = 'changed' WHERE id = '00000000-0000-0000-0000-000000000404';
----
error: ImmutableTable

DELETE FROM documents WHERE id = '00000000-0000-0000-0000-000000000001';
----
error: ImmutableTable

DELETE FROM documents WHERE title = 'no such title';
----
error: ImmutableTable

DELETE FROM documents;
----
error: ImmutableTable

# An insert that may turn into an update is refused as well, on a key that is taken and on one
# that is free.
INSERT INTO documents (id, title) VALUES ('00000000-0000-0000-0000-000000000001', 'again')
ON CONFLICT (id) DO UPDATE SET title = 'again';
----
error: ImmutableTable

INSERT INTO documents (id, title) VALUES ('00000000-0000-0000-0000-000000000009', 'ninth')
ON CONFLICT (id) DO UPDATE SET title = 'ninth';
----
error: ImmutableTable

# A plain insert is what the table takes, and a taken key is refused as on any table.
INSERT INTO documents (id, data, title) VALUES ('00000000-0000-0000-0000-000000000002', '[2]', 'second');
INSERT INTO documents (id, title) VALUES ('00000000-0000-0000-0000-000000000002', 'twice');
----
error: UniqueViolation

SELECT id, data, title FROM documents ORDER BY id;
----
id | data | title
00000000-0000-0000-0000-000000000001 | {"k":1} | first
00000000-0000-0000-0000-000000000002 | [2] | second

# A table that is never changed has no rows to expire and no older versions to drop, so IMMUTABLE
# stands with neither clause. Each clause stands without it.
CREATE TABLE expiring_log (id INTEGER PRIMARY KEY, note TEXT) IMMUTABLE RETAIN 1 DAYS;
----
error: SchemaInvalid

CREATE TABLE current_log (id INTEGER PRIMARY KEY, note TEXT) IMMUTABLE HISTORY CURRENT ONLY SYNC OFF;
----
error: SchemaInvalid

CREATE TABLE expiring (id INTEGER PRIMARY KEY, note TEXT) RETAIN 1 DAYS;
CREATE TABLE current (id INTEGER PRIMARY KEY, note TEXT) HISTORY CURRENT ONLY SYNC OFF;
SELECT id FROM expiring_log;
----
error: TableNotFound

SELECT id FROM current_log;
----
error: TableNotFound

# IMMUTABLE on two columns of a table whose rows may otherwise change.
CREATE TABLE tasks (
  id INTEGER PRIMARY KEY,
  task_type TEXT NOT NULL IMMUTABLE,
  description TEXT NOT NULL IMMUTABLE,
  confidence REAL,
  status TEXT NOT NULL DEFAULT 'active'
);
INSERT INTO tasks (id, task_type, description) VALUES (1, 'sql-migration', 'migrate the store');

UPDATE tasks SET task_type = 'other' WHERE id = 1;
----
error: ImmutableColumn

UPDATE tasks SET description = 'migrate the store' WHERE id = 1;
----
error: ImmutableColumn

UPDATE tasks SET task_type = 'other' WHERE id = 404;
----
error: ImmutableColumn

# One assignment to such a column refuses the whole statement: status is not changed either.
UPDATE tasks SET status = 'archived', description = 'rewritten' WHERE id = 1;
----
error: ImmutableColumn

INSERT INTO tasks (id, task_type, description) VALUES (1, 'sql-migration', 'second try')
ON CONFLICT (id) DO UPDATE SET description = 'second try';
----
error: ImmutableColumn

SELECT id, task_type, description, confidence, status FROM tasks ORDER BY id;
----
id | task_type | description | confidence | status
1 | sql-migration | migrate the store | NULL | active

# The other columns change, by an UPDATE and by an insert that meets the row.
UPDATE tasks SET confidence = 0.5 WHERE id = 1;
----
rows affected: 1

INSERT INTO tasks (id, task_type, description) VALUES (1, 'sql-migration', 'migrate the store')
ON CONFLICT (id) DO UPDATE SET confidence = 0.75;
----
rows affected: 1

# A record that turned out wrong is corrected by a second row, and the first is marked: both
# stay, and both are found.
INSERT INTO tasks (id, task_type, description) VALUES (2, 'sql-migration', 'migrate the store (rev 2)');
UPDATE tasks SET status = 'superseded' WHERE id = 1;
----
rows affected: 1

SELECT id, task_type, description, confidence, status FROM tasks ORDER BY id;
----
id | task_type | description | confidence | status
1 | sql-migration | migrate the store | 0.75 | superseded
2 | sql-migration | migrate the store (rev 2) | NULL | active

SELECT id FROM tasks WHERE status = 'superseded';
----
id
1

# The column cannot be taken out of the table or given another name, with CASCADE or without.
ALTER TABLE tasks DROP COLUMN task_type;
----
error: ImmutableColumn

ALTER TABLE tasks DROP COLUMN task_type CASCADE;
----
error: ImmutableColumn

ALTER TABLE tasks RENAME COLUMN description TO summary;
----
error: ImmutableColumn

SELECT task_type, description FROM tasks WHERE id = 2;
----
task_type | description
sql-migration | migrate the store (rev 2)

# A column that is not IMMUTABLE goes or is renamed, and a row of the table may be deleted.
ALTER TABLE tasks RENAME COLUMN confidence TO certainty;
ALTER TABLE tasks DROP COLUMN certainty;
DELETE FROM tasks WHERE id = 1;
----
rows affected: 1

SELECT id, task_type, description, status FROM tasks ORDER BY id;
----
id | task_type | description | status
2 | sql-migration | migrate the store (rev 2) | active
