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
