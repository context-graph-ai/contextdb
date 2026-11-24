# COUNT, SUM, MIN and MAX over the rows a statement reaches. Row 3 holds NULL in every column
# but its key.

CREATE TABLE sales (id INTEGER PRIMARY KEY, qty INTEGER, price REAL, region TEXT);
INSERT INTO sales (id, qty, price, region)
VALUES (1, 7, 0.5, 'north'), (2, 6, 1.5, 'south'), (3, NULL, NULL, NULL), (4, 9, 2.0, 'east');

# COUNT(*) counts rows and COUNT of a column counts its values that are not NULL.
SELECT COUNT(*) FROM sales;
----
count  (read 0, compared 0)
4

SELECT COUNT(qty) FROM sales;
----
count  (read 4, compared 0)
3

SELECT COUNT(*) FROM sales WHERE qty > 6;
----
count  (read 4, compared 0)
2

SELECT COUNT(*) FROM sales WHERE id > 100;
----
count  (read 0, compared 0)
0

# SUM skips NULL, adds whole numbers as whole numbers and REAL as REAL, and is named after the
# function unless the statement gives it a name.
SELECT SUM(qty) FROM sales;
----
sum  (read 4, compared 0)
22

SELECT SUM(price) FROM sales;
----
sum  (read 4, compared 0)
4.0

SELECT SUM(qty) AS total FROM sales;
----
total  (read 4, compared 0)
22

# Over no rows SUM is NULL, not zero.
SELECT SUM(qty) FROM sales WHERE id > 100;
----
sum  (read 0, compared 0)
NULL

# A column that is not a number cannot be added.
SELECT SUM(region) FROM sales;
----
error: PlanError

# A total past the largest whole number is refused, never wrapped.
CREATE TABLE huge (id INTEGER PRIMARY KEY, n INTEGER);
INSERT INTO huge (id, n) VALUES (1, 9223372036854775807), (2, 1);
SELECT SUM(n) FROM huge;
----
error: PlanError

# MIN and MAX skip NULL, and over no rows each is NULL.
SELECT MIN(qty) FROM sales;
----
min  (read 4, compared 0)
6

SELECT MAX(qty) FROM sales;
----
max  (read 4, compared 0)
9

SELECT MIN(price), MAX(price) FROM sales;
----
min | max  (read 4, compared 0)
0.5 | 2.0

SELECT MIN(region), MAX(region) FROM sales;
----
min | max  (read 4, compared 0)
east | south

SELECT MIN(qty), MAX(qty) FROM sales WHERE id > 100;
----
min | max  (read 0, compared 0)
NULL | NULL

# MIN and MAX of a VECTOR are refused, as pgvector has neither.
CREATE TABLE vecs (id INTEGER PRIMARY KEY, v VECTOR(2));
INSERT INTO vecs (id, v) VALUES (1, [1.0, 0.0]), (2, [0.0, 1.0]);
SELECT MIN(v) FROM vecs;
----
error: PlanError

SELECT MAX(v) FROM vecs;
----
error: PlanError
