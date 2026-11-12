# UPDATE changes the rows its WHERE names and DELETE removes them. Each says how many.

CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL, status TEXT, points INTEGER);
INSERT INTO tasks (id, title, status, points)
VALUES (1, 'draft', 'open', 1), (2, 'review', 'open', 20), (3, 'ship', 'done', 30), (4, 'rest', NULL, 40);

UPDATE tasks SET status = 'closed' WHERE id = 2;
----
rows affected: 1

# Several columns at once, a new value worked out from the old one, more than one row.
UPDATE tasks SET points = points + 1, status = 'big' WHERE points >= 30;
----
rows affected: 2

UPDATE tasks SET title = 'nobody' WHERE id = 1000;
----
rows affected: 0

SELECT id, title, status, points FROM tasks ORDER BY id;
----
id | title | status | points
1 | draft | open | 1
2 | review | closed | 20
3 | ship | big | 31
4 | rest | big | 41

DELETE FROM tasks WHERE id = 3;
----
rows affected: 1

DELETE FROM tasks WHERE status = 'big' OR points < 5;
----
rows affected: 2

DELETE FROM tasks WHERE id = 1000;
----
rows affected: 0

SELECT id, title, status, points FROM tasks ORDER BY id;
----
id | title | status | points
2 | review | closed | 20

# A deleted key is free again.
INSERT INTO tasks (id, title) VALUES (3, 'ship again');

# With no WHERE, every row.
UPDATE tasks SET points = 0;
----
rows affected: 2

SELECT id, points FROM tasks ORDER BY id;
----
id | points
2 | 0
3 | 0

DELETE FROM tasks;
----
rows affected: 2

SELECT id FROM tasks ORDER BY id;
----
id

# A column named twice in the SET list is refused, as Postgres refuses it, whatever it matches.
INSERT INTO tasks (id, title) VALUES (1, 'again');
UPDATE tasks SET title = 'first', title = 'second' WHERE id = 1;
----
error: PlanError

# An UPDATE that swaps two keys: each key is taken while the statement runs and free when it
# ends, so each row is left under the other's key and is found by it.
CREATE TABLE swapped (k INTEGER PRIMARY KEY, v TEXT NOT NULL);
INSERT INTO swapped (k, v) VALUES (1, 'one'), (2, 'two'), (3, 'three');
UPDATE swapped SET k = 3 - k WHERE k IN (1, 2);
----
rows affected: 2

SELECT k, v FROM swapped ORDER BY k;
----
k | v
1 | two
2 | one
3 | three

SELECT v FROM swapped WHERE k = 1;
----
v
two

# A row moved to a key is not met again at that key: every row of a shift is changed once,
# although the key it moved to still passes the WHERE.
CREATE TABLE shifted (k INTEGER PRIMARY KEY, v TEXT NOT NULL);
INSERT INTO shifted (k, v) VALUES (98, 'one'), (99, 'two'), (100, 'three');
UPDATE shifted SET k = k + 1 WHERE k >= 1 AND k <= 100;
----
rows affected: 3

SELECT k, v FROM shifted ORDER BY k;
----
k | v
99 | one
100 | two
101 | three
