# A filter no row can pass: a crossed range, a range that ends before it starts, a constant FALSE
# or NULL. Nothing is read: a read answers no rows, a count answers 0, and a write changes none.
# A filter that only looks like one still answers, as the last statement shows.

CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, tag INTEGER NOT NULL);
CREATE INDEX t_a ON t (a);
INSERT INTO t (id, a, tag) VALUES (1, 1, 10), (2, 3, 10), (3, 5, 20), (4, 7, 20), (5, 9, 30);

SELECT id FROM t WHERE a > 5 AND a < 3;
----
id  (read 0, compared 0)

SELECT id FROM t WHERE a BETWEEN 10 AND 5;
----
id  (read 0, compared 0)

SELECT id FROM t WHERE FALSE;
----
id  (read 0, compared 0)

SELECT id FROM t WHERE NULL;
----
id  (read 0, compared 0)

# Over a join too, inner or LEFT, a filter no row can pass reads nothing.
CREATE TABLE u (id INTEGER PRIMARY KEY, t_id INTEGER NOT NULL);
INSERT INTO u (id, t_id) VALUES (1, 1), (2, 3);
SELECT t.id AS id FROM t JOIN u ON u.t_id = t.id WHERE FALSE;
----
id  (read 0, compared 0)

SELECT t.id AS id FROM t LEFT JOIN u ON u.t_id = t.id WHERE FALSE;
----
id  (read 0, compared 0)

# A count of what cannot pass is 0 and reads nothing.
SELECT COUNT(*) FROM t WHERE a > 5 AND a < 3;
----
count  (read 0, compared 0)
0

SELECT COUNT(*) FROM t WHERE FALSE;
----
count  (read 0, compared 0)
0

# A write whose filter can pass no row changes none and reads nothing.
UPDATE t SET tag = 99 WHERE a BETWEEN 10 AND 5;
----
rows affected: 0  (read 0, compared 0)

DELETE FROM t WHERE FALSE;
----
rows affected: 0  (read 0, compared 0)

SELECT COUNT(*) FROM t WHERE tag = 99;
----
count  (read 5, compared 0)
0

# The plan says so in one line of its own, and names no walk of the key or an index.
EXPLAIN SELECT id FROM t WHERE FALSE;
----
QUERY PLAN  (read 0, compared 0)
NoScan { filter: false }

EXPLAIN SELECT id FROM t WHERE a > 5 AND a < 3;
----
QUERY PLAN  (read 0, compared 0)
NoScan { filter: false }

# A filter that two conditions make one value is not one that passes nothing.
SELECT id FROM t WHERE a = 5 AND a >= 5;
----
id  (read 1, compared 0)
3
