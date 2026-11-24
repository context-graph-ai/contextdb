# UPDATE changes the rows its WHERE names and DELETE removes them. Each says how many.

CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL, status TEXT, points INTEGER);
INSERT INTO tasks (id, title, status, points)
VALUES (1, 'draft', 'open', 1), (2, 'review', 'open', 20), (3, 'ship', 'done', 30), (4, 'rest', NULL, 40);

UPDATE tasks SET status = 'closed' WHERE id = 2;
----
rows affected: 1  (read 1, compared 0)

# Several columns at once, a new value worked out from the old one, more than one row.
UPDATE tasks SET points = points + 1, status = 'big' WHERE points >= 30;
----
rows affected: 2  (read 4, compared 0)

UPDATE tasks SET title = 'nobody' WHERE id = 1000;
----
rows affected: 0  (read 0, compared 0)

SELECT id, title, status, points FROM tasks ORDER BY id;
----
id | title | status | points  (read 4, compared 0)
1 | draft | open | 1
2 | review | closed | 20
3 | ship | big | 31
4 | rest | big | 41

DELETE FROM tasks WHERE id = 3;
----
rows affected: 1  (read 1, compared 0)

DELETE FROM tasks WHERE status = 'big' OR points < 5;
----
rows affected: 2  (read 3, compared 0)

DELETE FROM tasks WHERE id = 1000;
----
rows affected: 0  (read 0, compared 0)

SELECT id, title, status, points FROM tasks ORDER BY id;
----
id | title | status | points  (read 1, compared 0)
2 | review | closed | 20

# A deleted key is free again.
INSERT INTO tasks (id, title) VALUES (3, 'ship again');

# With no WHERE, every row.
UPDATE tasks SET points = 0;
----
rows affected: 2  (read 2, compared 0)

SELECT id, points FROM tasks ORDER BY id;
----
id | points  (read 2, compared 0)
2 | 0
3 | 0

DELETE FROM tasks;
----
rows affected: 2  (read 2, compared 0)

SELECT id FROM tasks ORDER BY id;
----
id  (read 0, compared 0)

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
rows affected: 2  (read 2, compared 0)

SELECT k, v FROM swapped ORDER BY k;
----
k | v  (read 3, compared 0)
1 | two
2 | one
3 | three

SELECT v FROM swapped WHERE k = 1;
----
v  (read 1, compared 0)
two

# A row moved to a key is not met again at that key: every row of a shift is changed once,
# although the key it moved to still passes the WHERE.
CREATE TABLE shifted (k INTEGER PRIMARY KEY, v TEXT NOT NULL);
INSERT INTO shifted (k, v) VALUES (98, 'one'), (99, 'two'), (100, 'three');
UPDATE shifted SET k = k + 1 WHERE k >= 1 AND k <= 100;
----
rows affected: 3  (read 3, compared 0)

SELECT k, v FROM shifted ORDER BY k;
----
k | v  (read 3, compared 0)
99 | one
100 | two
101 | three

# An UPDATE finds its rows with LIKE and BETWEEN as a SELECT does.
CREATE TABLE stock (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER);
INSERT INTO stock (id, name, qty) VALUES (1, 'bolt-1', 5), (2, 'bolt-2', 15), (3, 'nut-1', 7), (4, 'bolt-3', 9);
UPDATE stock SET qty = 0 WHERE name LIKE 'bolt%' AND qty BETWEEN 1 AND 10;
----
rows affected: 2  (read 4, compared 0)

SELECT id, qty FROM stock ORDER BY id;
----
id | qty  (read 4, compared 0)
1 | 0
2 | 15
3 | 7
4 | 0

# A SELECT inside a write is read once, before the write changes a row.
CREATE TABLE jobs (id INTEGER PRIMARY KEY, status TEXT NOT NULL, points INTEGER NOT NULL);
INSERT INTO jobs (id, status, points) VALUES (1, 'open', 1), (2, 'open', 20), (3, 'done', 30), (4, 'done', 5);
UPDATE jobs SET points = (SELECT MAX(points) FROM jobs) WHERE id IN (SELECT id FROM jobs WHERE status = 'open');
----
rows affected: 2  (read 10, compared 0)

SELECT id, points FROM jobs ORDER BY id;
----
id | points  (read 4, compared 0)
1 | 30
2 | 30
3 | 30
4 | 5

DELETE FROM jobs WHERE id IN (SELECT id FROM jobs WHERE status = 'done');
----
rows affected: 2  (read 6, compared 0)

SELECT id FROM jobs ORDER BY id;
----
id  (read 2, compared 0)
1
2

# A row is changed once in a statement, though its new key is met again further along the keys
# the statement reads: 1, 2 and 3 move to 11, 12 and 13 and stay there, though 11, 12 and 13 are
# still below 100.
CREATE TABLE steps (id INTEGER PRIMARY KEY, n INTEGER NOT NULL);
INSERT INTO steps (id, n) VALUES (1, 10), (2, 20), (3, 30);
UPDATE steps SET id = id + 10 WHERE id < 100;
----
rows affected: 3  (read 3, compared 0)

SELECT id, n FROM steps ORDER BY id;
----
id | n  (read 3, compared 0)
11 | 10
12 | 20
13 | 30
