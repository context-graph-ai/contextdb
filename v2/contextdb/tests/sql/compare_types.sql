# A comparison between a column and a value of another type is refused when the statement is
# read, whatever the table holds: on an empty table, in a SELECT, an UPDATE and a DELETE. It is
# never answered by leaving rows out. The conversions a write has still apply: quoted text is
# read as a UUID or a TIMESTAMP, and an INTEGER as a REAL.

CREATE TABLE things (
  id INTEGER PRIMARY KEY,
  n INTEGER,
  ratio REAL,
  label TEXT,
  flag BOOLEAN,
  ref UUID,
  at TIMESTAMP
);

# The table is empty, and each of these is refused all the same.
SELECT id FROM things WHERE n = 'seven';
----
error: PlanError

SELECT id FROM things WHERE label = 7;
----
error: PlanError

SELECT id FROM things WHERE label < 7;
----
error: PlanError

SELECT id FROM things WHERE flag = 1;
----
error: PlanError

SELECT id FROM things WHERE n = TRUE;
----
error: PlanError

SELECT id FROM things WHERE ref = 7;
----
error: PlanError

SELECT id FROM things WHERE at = 7;
----
error: PlanError

SELECT id FROM things WHERE id = 'one';
----
error: PlanError

SELECT id FROM things WHERE n IN (1, 'two');
----
error: PlanError

SELECT id FROM things WHERE n = label;
----
error: PlanError

SELECT id FROM things WHERE label = 'a' AND n > 'b';
----
error: PlanError

UPDATE things SET n = 1 WHERE label = 7;
----
error: PlanError

UPDATE things SET label = 'x' WHERE n >= 'seven';
----
error: PlanError

DELETE FROM things WHERE n = 'seven';
----
error: PlanError

DELETE FROM things WHERE flag = 'yes';
----
error: PlanError

# A table that is not there is said first.
SELECT id FROM nowhere WHERE id = 'one';
----
error: TableNotFound

# What does compare, on the empty table: an answer of no rows and no refusal.
SELECT id FROM things WHERE n = 7 AND ratio > 1 AND label = 'seven' AND flag = TRUE;
----
id

SELECT id FROM things WHERE n = NULL;
----
id

INSERT INTO things (id, n, ratio, label, flag, ref, at)
VALUES (1, 7, 1.5, 'seven', TRUE, '550e8400-e29b-41d4-a716-446655440001', '2026-01-01T00:00:00Z'),
       (2, 8, 2.0, 'eight', FALSE, '550e8400-e29b-41d4-a716-446655440002', '2026-01-02T00:00:00Z'),
       (3, NULL, NULL, NULL, NULL, NULL, NULL);

# With rows in the table the refusals are the same, and no row is changed or removed.
SELECT id FROM things WHERE n = 'seven';
----
error: PlanError

SELECT id FROM things WHERE label = 7;
----
error: PlanError

UPDATE things SET n = 0 WHERE label = 7;
----
error: PlanError

DELETE FROM things WHERE n = 'seven';
----
error: PlanError

# Quoted text is read as a UUID, in either case, and as a TIMESTAMP.
SELECT id FROM things WHERE ref = '550E8400-E29B-41D4-A716-446655440002';
----
id
2

SELECT id FROM things WHERE at = '2026-01-01T00:00:00Z';
----
id
1

SELECT id FROM things WHERE at > '2026-01-01T12:00:00Z' ORDER BY id;
----
id
2

# An INTEGER is read as a REAL.
SELECT id FROM things WHERE ratio = 2;
----
id
2

SELECT id FROM things WHERE ratio > 1 ORDER BY id;
----
id
1
2

# The same conversions in an UPDATE and a DELETE.
UPDATE things SET label = 'two' WHERE ratio = 2;
----
rows affected: 1

UPDATE things SET n = 9 WHERE ref = '550e8400-e29b-41d4-a716-446655440001';
----
rows affected: 1

DELETE FROM things WHERE at = '2026-01-02T00:00:00Z';
----
rows affected: 1

SELECT id, n, label FROM things ORDER BY id;
----
id | n | label
1 | 9 | seven
3 | NULL | NULL
