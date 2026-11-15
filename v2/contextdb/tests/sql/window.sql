# ROW_NUMBER() OVER: the rows of each partition numbered 1, 2, 3 in the window's order.

CREATE TABLE sessions (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, at INTEGER NOT NULL);
INSERT INTO sessions (id, ctx, at)
VALUES (1, 'a', 10), (2, 'a', 30), (3, 'a', 30), (4, 'a', 20), (5, 'b', 5), (6, 'b', 7), (7, 'c', 1);

# The newest rows of each partition, each partition with its own bound. Rows 2 and 3 are at
# the same time and get different numbers, in the order the window goes on to give.
WITH ranked AS (
  SELECT ctx, id, ROW_NUMBER() OVER (PARTITION BY ctx ORDER BY at DESC, id ASC) AS r
  FROM sessions
)
SELECT ctx, id, r FROM ranked WHERE (ctx = 'a' AND r <= 3) OR (ctx = 'b' AND r <= 1)
ORDER BY ctx, r;
----
ctx | id | r
a | 2 | 1
a | 3 | 2
a | 4 | 3
b | 6 | 1

# An index in the window's order changes the work and not the answer.
CREATE INDEX sessions_newest ON sessions (ctx ASC, at DESC, id ASC);
WITH ranked AS (
  SELECT ctx, id, ROW_NUMBER() OVER (PARTITION BY ctx ORDER BY at DESC, id ASC) AS r
  FROM sessions
)
SELECT ctx, id, r FROM ranked WHERE (ctx = 'a' AND r <= 3) OR (ctx = 'b' AND r <= 1)
ORDER BY ctx, r;
----
ctx | id | r
a | 2 | 1
a | 3 | 2
a | 4 | 3
b | 6 | 1

# That index is newest first and does not serve oldest first: the rows are sorted, and the
# answer is the same as an index would give.
WITH ranked AS (
  SELECT ctx, id, ROW_NUMBER() OVER (PARTITION BY ctx ORDER BY at ASC, id ASC) AS r FROM sessions
)
SELECT ctx, id, r FROM ranked WHERE ctx = 'a' AND r <= 2 ORDER BY r;
----
ctx | id | r
a | 1 | 1
a | 4 | 2

# Without PARTITION BY the whole input is one partition.
SELECT id, ROW_NUMBER() OVER (ORDER BY at DESC, id ASC) AS r FROM sessions ORDER BY r LIMIT 3;
----
id | r
2 | 1
3 | 2
4 | 3

# Every other window form is refused by name: no ORDER BY, another function, an aggregate.
SELECT id, ROW_NUMBER() OVER (PARTITION BY ctx) AS r FROM sessions;
----
error: WindowFunctionNotSupported

SELECT id, RANK() OVER (ORDER BY at) AS r FROM sessions;
----
error: WindowFunctionNotSupported

SELECT id, SUM(at) OVER (PARTITION BY ctx ORDER BY at) AS total FROM sessions;
----
error: WindowFunctionNotSupported

# A bound written < stops one number sooner than <=.
WITH ranked AS (
  SELECT ctx, id, ROW_NUMBER() OVER (PARTITION BY ctx ORDER BY at DESC, id ASC) AS r
  FROM sessions
)
SELECT id, r FROM ranked WHERE ctx = 'a' AND r < 3 ORDER BY r;
----
id | r
2 | 1
3 | 2

# Two alternatives that name one partition: the larger bound is the one that holds.
WITH ranked AS (
  SELECT ctx, id, ROW_NUMBER() OVER (PARTITION BY ctx ORDER BY at DESC, id ASC) AS r
  FROM sessions
)
SELECT id, r FROM ranked WHERE (ctx = 'a' AND r <= 1) OR (ctx = 'a' AND r <= 2) ORDER BY r;
----
id | r
2 | 1
3 | 2

# One window to a SELECT, and a partition or an order that is a column and nothing computed.
SELECT ROW_NUMBER() OVER (ORDER BY at) AS x, ROW_NUMBER() OVER (ORDER BY id) AS y FROM sessions;
----
error: WindowFunctionNotSupported

SELECT ROW_NUMBER() OVER (PARTITION BY ctx ORDER BY at + 1) FROM sessions;
----
error: WindowFunctionNotSupported

# Two OR branches that name one partition in two spellings are one partition.
CREATE TABLE spots (id INTEGER PRIMARY KEY, grp REAL, who UUID);
CREATE INDEX spots_grp ON spots (grp, id);
CREATE INDEX spots_who ON spots (who, id);
INSERT INTO spots (id, grp, who) VALUES (1, 1.0, '0000000a-0000-0000-0000-00000000000a'), (2, 1.0, '0000000a-0000-0000-0000-00000000000a');
WITH w AS (SELECT id, grp, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn FROM spots)
SELECT id, rn FROM w WHERE (grp = 1 AND rn <= 1) OR (grp = 1.0 AND rn <= 2) ORDER BY id;
----
id | rn
1 | 1
2 | 2

WITH w AS (SELECT id, who, ROW_NUMBER() OVER (PARTITION BY who ORDER BY id) AS rn FROM spots)
SELECT id, rn FROM w WHERE (who = '0000000A-0000-0000-0000-00000000000A' AND rn <= 1) OR (who = '0000000a-0000-0000-0000-00000000000a' AND rn <= 2) ORDER BY id;
----
id | rn
1 | 1
2 | 2
