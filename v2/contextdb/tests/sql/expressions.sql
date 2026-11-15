# What a select list may work out, and what its columns are called.

CREATE TABLE sales (id INTEGER PRIMARY KEY, qty INTEGER, price REAL, region TEXT);
INSERT INTO sales (id, qty, price, region)
VALUES (1, 7, 0.5, 'north'), (2, 6, 1.5, NULL), (3, NULL, NULL, 'east');

# A SELECT needs no table. An expression with no name is called ?column?.
SELECT 1 + 1;
----
?column?
2

SELECT id, qty * 2 FROM sales WHERE id = 1;
----
id | ?column?
1 | 14

# An aggregate or a function with no name is called by its own, in lower case.
SELECT COUNT(*), SUM(qty), MIN(qty), MAX(qty), AVG(qty) FROM sales;
----
count | sum | min | max | avg
3 | 13 | 6 | 7 | 6.5

SELECT COALESCE(region, 'none') FROM sales ORDER BY id;
----
coalesce
north
none
east

# SUM of an INTEGER column is an INTEGER, and of a REAL column a REAL.
SELECT SUM(qty) AS whole, SUM(price) AS part FROM sales;
----
whole | part
13 | 2.0

# A name after an expression or a table is its alias, with or without AS.
SELECT x.qty total FROM sales x WHERE x.id = 1;
----
total
7

SELECT qty AS total FROM sales AS x WHERE x.id = 1;
----
total
7

# Rows of values compare with = and <>, column by column: two rows differ when any column
# does, whatever the others hold.
SELECT id FROM sales WHERE (qty, region) = (7, 'north');
----
id
1

SELECT id FROM sales WHERE (id, qty) <> (1, 7) ORDER BY id;
----
id
2
3

# A qualifier that names no table or alias of the statement is refused, and so is a function
# this version does not have, before any row is read.
SELECT y.qty FROM sales x;
----
error: ColumnNotFound

SELECT UPPER(region) FROM sales WHERE id > 100;
----
error: PlanError

# The engine gives a row no number: row_id is a name like any other, and an unknown one is
# refused (CDB-228), and a column of that name keeps what it is given.
SELECT row_id FROM sales;
----
error: ColumnNotFound

CREATE TABLE own_ids (row_id TEXT NOT NULL);
INSERT INTO own_ids VALUES ('k-99');
SELECT * FROM own_ids;
----
row_id
k-99

# Whole numbers stay whole, and a NULL operand gives NULL.
SELECT 7 / 2 AS whole, 7 * 0.5 AS mixed, 1.5 - 1 AS real, NULL + 1 AS unknown;
----
whole | mixed | real | unknown
3 | 3.5 | 0.5 | NULL

# What does not fit a column, divides by zero or adds a number to text is refused, never wrapped.
SELECT 9223372036854775807 + 1;
----
error: PlanError

SELECT (-9223372036854775807 - 1) * -1;
----
error: PlanError

SELECT 7 / 0;
----
error: PlanError

SELECT 7.0 / 0.0;
----
error: PlanError

SELECT 7 + 'x';
----
error: PlanError

# The high bound of a BETWEEN and the items of an IN list may be scalar subqueries.
SELECT id FROM sales WHERE id BETWEEN 1 AND (SELECT MAX(qty) FROM sales) ORDER BY id;
----
id
1
2
3

SELECT id FROM sales WHERE id IN (1, (SELECT MAX(id) FROM sales)) ORDER BY id;
----
id
1
3
