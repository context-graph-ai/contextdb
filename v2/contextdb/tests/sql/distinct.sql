# DISTINCT gives each row of the answer once.

CREATE TABLE hits (id INTEGER PRIMARY KEY, ctx TEXT, conn TEXT);
INSERT INTO hits (id, ctx, conn)
VALUES (1, 'a', 'x'), (2, 'a', 'x'), (3, 'a', 'y'), (4, 'b', 'x'), (5, NULL, NULL), (6, NULL, NULL);

SELECT DISTINCT ctx FROM hits ORDER BY ctx;
----
ctx  (read 6, compared 0)
a
b
NULL

# Rows are the same when every column is, and two NULLs are the same here.
SELECT DISTINCT ctx, conn FROM hits ORDER BY ctx, conn;
----
ctx | conn  (read 6, compared 0)
a | x
a | y
b | x
NULL | NULL

# LIMIT counts the rows that are left.
SELECT DISTINCT conn FROM hits ORDER BY conn LIMIT 2;
----
conn  (read 6, compared 0)
x
y

# A vector column, and a number column with a NULL: each different value once, NULL one value.
CREATE TABLE marks (id INTEGER PRIMARY KEY, v VECTOR(3), x INTEGER);
INSERT INTO marks (id, v, x)
VALUES (0, [1.0, 0.0, 0.0], 1), (1, [0.0, 1.0, 0.0], 2), (2, NULL, NULL),
       (3, [1.0, 0.0, 0.0], 1), (4, [0.0, 1.0, 0.0], 2), (5, NULL, NULL);

SELECT DISTINCT v FROM marks;
----
v  (read 6, compared 0)
*
*
*

SELECT DISTINCT x FROM marks ORDER BY x;
----
x  (read 6, compared 0)
1
2
NULL

# Vectors are values: equal ones group and partition together, and sort by their numbers.
SELECT COUNT(*) AS n FROM marks GROUP BY v ORDER BY n;
----
n  (read 6, compared 0)
2
2
2

SELECT id, ROW_NUMBER() OVER (PARTITION BY v ORDER BY id) AS rn FROM marks ORDER BY id;
----
id | rn  (read 6, compared 0)
0 | 1
1 | 1
2 | 1
3 | 2
4 | 2
5 | 2

SELECT id FROM marks WHERE v IS NOT NULL ORDER BY v, id;
----
id  (read 6, compared 0)
1
4
0
3

# DISTINCT keeps one row for each value shown, so a row it drops cannot be put in order.
SELECT DISTINCT x FROM marks ORDER BY id DESC;
----
error: PlanError

# Equal vectors are equal: = matches every row that holds them.
SELECT id FROM marks WHERE v = [1.0, 0.0, 0.0] ORDER BY id;
----
id  (read 6, compared 0)
0
3

# A name the select list gives under a qualifier is that item: it may be sorted by.
SELECT DISTINCT marks.x FROM marks ORDER BY x;
----
x  (read 6, compared 0)
1
2
NULL

# The same SELECT written twice is one expression: it is in the select list, so it may be sorted by.
# (Postgres matches the two by structure; not shown in its pages, believed from its source.)
SELECT DISTINCT (SELECT 1) AS k FROM marks ORDER BY (SELECT 1);
----
k  (read 6, compared 0)
1
