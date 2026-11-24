# UNION ALL puts the rows of two SELECTs together; UNION also drops the rows that repeat.

CREATE TABLE a (id INTEGER PRIMARY KEY, v TEXT);
CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO a (id, v) VALUES (1, 'x'), (2, 'y'), (3, 'y');
INSERT INTO b (id, v) VALUES (1, 'y'), (2, 'z');

# UNION ALL keeps every row, and the result's columns are named by the first SELECT.
SELECT v FROM a UNION ALL SELECT v FROM b ORDER BY v;
----
v  (read 5, compared 0)
x
y
y
y
z

# UNION keeps one of each, whichever SELECT it came from.
SELECT v FROM a UNION SELECT v FROM b ORDER BY v;
----
v  (read 5, compared 0)
x
y
z

# ORDER BY names a column as the first SELECT names it, even when the others call it by another name.
SELECT 2 AS n UNION SELECT 1 ORDER BY n;
----
n  (read 0, compared 0)
1
2

SELECT v AS w FROM a UNION SELECT v FROM b ORDER BY w;
----
w  (read 5, compared 0)
x
y
z

# ORDER BY, LIMIT and OFFSET are over the whole.
SELECT id, v FROM a UNION ALL SELECT id, v FROM b ORDER BY v DESC, id LIMIT 2;
----
id | v  (read 5, compared 0)
2 | z
1 | y

SELECT v FROM a UNION SELECT v FROM b ORDER BY v LIMIT 1 OFFSET 1;
----
v  (read 5, compared 0)
y

# A UNION stands wherever a SELECT may.
WITH whole AS (SELECT v FROM a UNION ALL SELECT v FROM b)
SELECT COUNT(*) AS n FROM whole;
----
n  (read 5, compared 0)
5

SELECT id FROM a WHERE v IN (SELECT v FROM b UNION SELECT v FROM a WHERE id = 1) ORDER BY id;
----
id  (read 6, compared 0)
1
2
3

# The SELECTs give the same number of columns, of types that match. Both are refused when the
# statement is planned: these tables hold no row.
CREATE TABLE e1 (id INTEGER PRIMARY KEY, n INTEGER);
CREATE TABLE e2 (id INTEGER PRIMARY KEY, s TEXT);
SELECT id, n FROM e1 UNION SELECT id FROM e2;
----
error: PlanError

SELECT n FROM e1 UNION ALL SELECT s FROM e2;
----
error: PlanError

# A chain of UNION and UNION ALL is read from the left: the first two drop repeats, the third
# keeps all it adds. ORDER BY, LIMIT and OFFSET are over the whole.
SELECT v FROM a UNION SELECT v FROM b UNION ALL SELECT v FROM a ORDER BY v LIMIT 3 OFFSET 1;
----
v  (read 8, compared 0)
x
y
y

# A NULL beside a number is one column of the number's type: NULL goes last going up.
SELECT NULL AS n UNION SELECT 1 ORDER BY n;
----
n  (read 0, compared 0)
1
NULL

# ORDER BY a whole number is not a column's place: it is refused, as the list of differences
# from Postgres says (row 55), never taken for a constant.
SELECT 2 AS x UNION ALL SELECT 1 AS x ORDER BY 1 ASC LIMIT 1;
----
error: PlanError

# A number and text do not match, nor do a timestamp and text.
SELECT (SELECT 2) AS x UNION ALL SELECT 'abc' AS x ORDER BY x ASC LIMIT 2;
----
error: PlanError

CREATE TABLE ts_tbl (id INTEGER PRIMARY KEY, ts TIMESTAMP);
CREATE TABLE txt_tbl (id INTEGER PRIMARY KEY, txt TEXT);
SELECT ts FROM ts_tbl UNION ALL SELECT txt FROM txt_tbl ORDER BY ts;
----
error: PlanError

# The ORDER BY of a UNION names columns of its result, never an expression.
SELECT 2 AS x UNION ALL SELECT 1 AS x ORDER BY (SELECT 1);
----
error: PlanError

SELECT 2 AS x UNION ALL SELECT 1 AS x ORDER BY x + 1;
----
error: PlanError

# A SELECT that stands for a value has the type of its column: a whole number here.
SELECT (SELECT 2) AS x UNION ALL SELECT 1 AS x ORDER BY x LIMIT 1;
----
x  (read 0, compared 0)
1

# A quoted literal beside a TIMESTAMP column is text, and text does not meet a timestamp.
SELECT ts FROM ts_tbl UNION ALL SELECT '2026-01-01T00:00:00Z';
----
error: PlanError

# An aggregate's type is its own: COUNT(*) + 1 is a whole number and meets one.
SELECT COUNT(*) + 1 AS x FROM a UNION ALL SELECT 1 ORDER BY x;
----
x  (read 0, compared 0)
1
4

# A column of a whole number and a REAL is a REAL, and each arm's rows are REALs before
# duplicates are removed: 2^53 + 1 is 2^53 as a REAL, so the two arms give one row.
SELECT 9007199254740993 AS x UNION SELECT 9007199254740992.0 AS x;
----
x  (read 0, compared 0)
9007199254740992.0

# Every row of such a column is a REAL, the whole number's included.
WITH u AS (SELECT 1 AS a UNION ALL SELECT 1.5) SELECT a / 2 AS h FROM u ORDER BY h;
----
h  (read 0, compared 0)
0.5
0.75

SELECT 1 AS one WHERE 9007199254740993 = 9007199254740992.0;
----
one  (read 0, compared 0)
