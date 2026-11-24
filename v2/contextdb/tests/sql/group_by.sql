# GROUP BY: one row for each set of grouping values, in the order of those values, NULL last.
# `plain` and `kept` hold the same rows; `kept` keeps the number of its rows for each ctx and
# for each ctx and conn, and every statement gives the same answer over either.

CREATE TABLE plain (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, conn TEXT, bytes INTEGER);
CREATE TABLE kept (
  id INTEGER PRIMARY KEY,
  ctx TEXT NOT NULL,
  conn TEXT,
  bytes INTEGER,
  UNIQUE (ctx, conn, id)
) MAINTAIN COUNT (ctx), (ctx, conn);
INSERT INTO plain (id, ctx, conn, bytes)
VALUES (1, 'a', 'x', 10), (2, 'a', 'y', 20), (3, 'a', 'x', 30), (4, 'b', 'x', 40),
       (5, 'b', NULL, NULL), (6, 'c', 'y', 60);
INSERT INTO kept (id, ctx, conn, bytes)
VALUES (1, 'a', 'x', 10), (2, 'a', 'y', 20), (3, 'a', 'x', 30), (4, 'b', 'x', 40),
       (5, 'b', NULL, NULL), (6, 'c', 'y', 60);

# A grouped COUNT(*) with and without a kept count: the same groups in the same order.
SELECT ctx, COUNT(*) FROM plain GROUP BY ctx;
----
ctx | count  (read 6, compared 0)
a | 3
b | 2
c | 1

SELECT ctx, COUNT(*) FROM kept GROUP BY ctx;
----
ctx | count  (read 0, compared 0)
a | 3
b | 2
c | 1

# Grouped by the rest of a kept list, with its leading column given.
SELECT conn, COUNT(*) AS n FROM plain WHERE ctx = 'b' GROUP BY conn;
----
conn | n  (read 6, compared 0)
x | 1
NULL | 1

SELECT conn, COUNT(*) AS n FROM kept WHERE ctx = 'b' GROUP BY conn;
----
conn | n  (read 0, compared 0)
x | 1
NULL | 1

# A leading column that is NULL pins the list too: the groups come from its counts alone.
CREATE TABLE nulls (id INTEGER PRIMARY KEY, ctx TEXT, conn TEXT, UNIQUE (ctx, conn, id)) MAINTAIN COUNT (ctx, conn);
INSERT INTO nulls (id, ctx, conn) VALUES (1, NULL, 'x'), (2, NULL, 'x'), (3, NULL, 'y'), (4, 'a', 'x');
SELECT conn, COUNT(*) AS n FROM nulls WHERE ctx IS NULL GROUP BY conn;
----
conn | n  (read 0, compared 0)
x | 2
y | 1

# Grouped by columns no list of `kept` holds a count for: answered from the rows.
SELECT conn, COUNT(*) AS n FROM kept GROUP BY conn;
----
conn | n  (read 6, compared 0)
x | 3
y | 2
NULL | 1

CREATE TABLE nul (id INTEGER PRIMARY KEY, ctx TEXT, conn TEXT, UNIQUE (ctx, conn, id)) MAINTAIN COUNT (ctx, conn);
INSERT INTO nul (id, ctx, conn) VALUES (1, NULL, 'x'), (2, NULL, 'x'), (3, NULL, 'y'), (4, 'a', 'x');
SELECT conn, COUNT(*) AS n FROM nul WHERE ctx IS NULL GROUP BY conn;
----
conn | n  (read 0, compared 0)
x | 2
y | 1

# Every aggregate, each skipping NULL. AVG is a REAL.
SELECT ctx, COUNT(bytes), SUM(bytes), MIN(bytes), MAX(bytes), AVG(bytes) FROM plain GROUP BY ctx;
----
ctx | count | sum | min | max | avg  (read 6, compared 0)
a | 3 | 60 | 10 | 30 | 20.0
b | 1 | 40 | 40 | 40 | 40.0
c | 1 | 60 | 60 | 60 | 60.0

# Any expression groups.
SELECT bytes / 30 AS thirds, COUNT(*) AS n FROM plain GROUP BY bytes / 30;
----
thirds | n  (read 6, compared 0)
0 | 2
1 | 2
2 | 1
NULL | 1

# HAVING keeps the groups it holds for.
SELECT ctx, COUNT(*) AS n FROM plain GROUP BY ctx HAVING COUNT(*) > 1;
----
ctx | n  (read 6, compared 0)
a | 3
b | 2

# DISTINCT, ORDER BY and LIMIT stand around a grouped read, and it reads a WITH name or a join.
SELECT ctx, COUNT(*) AS n FROM plain GROUP BY ctx ORDER BY n DESC, ctx LIMIT 1;
----
ctx | n  (read 6, compared 0)
a | 3

SELECT DISTINCT COUNT(*) AS n FROM plain GROUP BY bytes / 30 ORDER BY n;
----
n  (read 6, compared 0)
1
2

WITH named AS (SELECT ctx FROM plain WHERE conn IS NOT NULL)
SELECT ctx, COUNT(*) AS n FROM named GROUP BY ctx;
----
ctx | n  (read 6, compared 0)
a | 3
b | 1
c | 1

SELECT p.ctx, COUNT(*) AS n FROM plain p JOIN kept k ON p.id = k.id WHERE k.conn = 'x'
GROUP BY p.ctx;
----
ctx | n  (read 12, compared 0)
a | 2
b | 1

# Without GROUP BY an aggregate is over every row the statement reaches. AVG of no value is NULL.
SELECT AVG(bytes) FROM plain;
----
avg  (read 6, compared 0)
32.0

SELECT AVG(bytes) FROM plain WHERE id = 5;
----
avg  (read 1, compared 0)
NULL

# A select item that is neither grouped nor an aggregate is refused when the statement is
# planned.
SELECT ctx, conn, COUNT(*) FROM plain GROUP BY ctx;
----
error: PlanError

# A count list declared DESC leaves its groups greatest first, NULL last.
CREATE TABLE days (id INTEGER PRIMARY KEY, day INTEGER NOT NULL);
CREATE INDEX days_day ON days (day DESC);
ALTER TABLE days SET MAINTAIN COUNT (day DESC);
INSERT INTO days (id, day) VALUES (1, 1), (2, 2), (3, 2), (4, 3);
SELECT day, COUNT(*) AS n FROM days GROUP BY day;
----
day | n  (read 4, compared 0)
3 | 1
2 | 2
1 | 1

# Any expression is grouped, and LIMIT and OFFSET are over the groups.
SELECT id / 2 AS half, COUNT(*) AS n FROM days GROUP BY id / 2 ORDER BY half LIMIT 1 OFFSET 1;
----
half | n  (read 4, compared 0)
1 | 2

# A window function is not a condition on groups.
SELECT day FROM days GROUP BY day HAVING ROW_NUMBER() OVER (ORDER BY day) = 1;
----
error: PlanError

# A column named twice groups once.
CREATE TABLE pair (id INTEGER PRIMARY KEY, a TEXT, b TEXT, UNIQUE (a, b, id)) MAINTAIN COUNT (a, b);
INSERT INTO pair (id, a, b) VALUES (1, 'x', 'p'), (2, 'x', 'q');
SELECT a, COUNT(*) AS n FROM pair GROUP BY a, a;
----
a | n  (read 2, compared 0)
x | 2

# A grouping expression of unlike types keeps its values apart: the text 'never' is a group of
# its own, not part of a timestamp's.
CREATE TABLE stamps (id INTEGER PRIMARY KEY, ts TIMESTAMP);
INSERT INTO stamps (id, ts) VALUES (1, '2026-01-01T00:00:00Z'), (2, NULL), (3, NULL);
SELECT COUNT(*) AS n FROM stamps GROUP BY COALESCE(ts, 'never') ORDER BY n;
----
n  (read 3, compared 0)
1
2

# One name stands for one column, whichever qualifier a statement writes it with.
SELECT plain.ctx, COUNT(*) AS n FROM plain GROUP BY ctx ORDER BY ctx;
----
ctx | n  (read 6, compared 0)
a | 3
b | 2
c | 1

# A number in GROUP BY is not a column's place: it is refused (row 55 of the differences).
SELECT COUNT(*) AS n FROM plain GROUP BY 1;
----
error: PlanError

# AVG is a REAL from its first value, so a sum past 64 bits is answered.
CREATE TABLE big (id INTEGER PRIMARY KEY, x INTEGER);
INSERT INTO big (id, x) VALUES (1, 5000000000000000000), (2, 5000000000000000000);
SELECT AVG(x) FROM big;
----
avg  (read 2, compared 0)
5e18

# The same SELECT written twice in a grouped statement is one expression.
SELECT bytes + (SELECT 1) AS y FROM plain WHERE id <= 2 GROUP BY bytes + (SELECT 1) ORDER BY y;
----
y  (read 2, compared 0)
11
21

# An unaliased function is named after the function, and ORDER BY may use that name.
SELECT ctx, COUNT(*) FROM plain GROUP BY ctx ORDER BY count DESC, ctx;
----
ctx | count  (read 6, compared 0)
a | 3
b | 2
c | 1
