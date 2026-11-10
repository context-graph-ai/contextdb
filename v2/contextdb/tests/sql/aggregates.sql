# COUNT, SUM, MIN and MAX over the rows a statement reaches. Row 3 holds NULL in every column
# but its key.

CREATE TABLE sales (id INTEGER PRIMARY KEY, qty INTEGER, price REAL, region TEXT);
INSERT INTO sales (id, qty, price, region)
VALUES (1, 7, 0.5, 'north'), (2, 6, 1.5, 'south'), (3, NULL, NULL, NULL), (4, 9, 2.0, 'east');

# COUNT(*) counts rows and COUNT of a column counts its values that are not NULL.
SELECT COUNT(*) FROM sales;
----
COUNT
4

SELECT COUNT(qty) FROM sales;
----
COUNT
3

SELECT COUNT(*) FROM sales WHERE qty > 6;
----
COUNT
2

SELECT COUNT(*) FROM sales WHERE id > 100;
----
COUNT
0

# SUM skips NULL, adds whole numbers as whole numbers and REAL as REAL, and is named after the
# function unless the statement gives it a name.
SELECT SUM(qty) FROM sales;
----
SUM
22

SELECT SUM(price) FROM sales;
----
SUM
4.0

SELECT SUM(qty) AS total FROM sales;
----
total
22

# Over no rows SUM is NULL, not zero.
SELECT SUM(qty) FROM sales WHERE id > 100;
----
SUM
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
MIN
6

SELECT MAX(qty) FROM sales;
----
MAX
9

SELECT MIN(price), MAX(price) FROM sales;
----
MIN | MAX
0.5 | 2.0

SELECT MIN(region), MAX(region) FROM sales;
----
MIN | MAX
east | south

SELECT MIN(qty), MAX(qty) FROM sales WHERE id > 100;
----
MIN | MAX
NULL | NULL
