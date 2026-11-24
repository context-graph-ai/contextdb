# A SELECT inside a statement: after IN, and as one value.

CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE tags (id INTEGER PRIMARY KEY, item_id INTEGER, label TEXT);
INSERT INTO items (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO tags (id, item_id, label) VALUES (10, 1, 'x'), (11, 1, 'y'), (12, 2, 'x');

SELECT name FROM items WHERE id IN (SELECT item_id FROM tags WHERE label = 'x') ORDER BY id;
----
name  (read 5, compared 0)
a
b

SELECT name FROM items WHERE id NOT IN (SELECT item_id FROM tags) ORDER BY id;
----
name  (read 6, compared 0)
c

# The SELECT after IN gives one column.
SELECT name FROM items WHERE id IN (SELECT item_id, label FROM tags);
----
error: PlanError

# A SELECT in parentheses that gives one row of one column is that value, wherever a value may
# stand; one that gives no row is NULL.
SELECT name FROM items WHERE id = (SELECT MAX(item_id) FROM tags);
----
name  (read 4, compared 0)
b

SELECT name, (SELECT COUNT(*) FROM tags) AS n FROM items WHERE id = 3;
----
name | n  (read 1, compared 0)
c | 3

SELECT name FROM items WHERE id = (SELECT item_id FROM tags WHERE label = 'none');
----
name  (read 6, compared 0)

# More than one row or column is not one value.
SELECT name FROM items WHERE id = (SELECT item_id FROM tags);
----
error: PlanError

SELECT name FROM items WHERE id = (SELECT item_id, label FROM tags WHERE id = 10);
----
error: PlanError

# A SELECT that reads the row of the statement around it, and EXISTS, are refused by name.
SELECT name FROM items WHERE id IN (SELECT item_id FROM tags WHERE tags.label = items.name);
----
error: SubqueryNotSupported

SELECT name FROM items WHERE EXISTS (SELECT id FROM tags);
----
error: SubqueryNotSupported

# A SELECT as a value has that value wherever the expression is worked out: beside an aggregate,
# in HAVING, and beside a window function.
SELECT COUNT(*) AS n, (SELECT 10) AS k FROM items;
----
n | k  (read 0, compared 0)
3 | 10

SELECT id FROM items GROUP BY id HAVING COUNT(*) > (SELECT 0) ORDER BY id;
----
id  (read 3, compared 0)
1
2
3

SELECT id, (SELECT 10) AS k, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM items ORDER BY id;
----
id | k | rn  (read 3, compared 0)
1 | 10 | 1
2 | 10 | 2
3 | 10 | 3

# An unaliased window function is named row_number and an unaliased one-column SELECT by its column.
SELECT (SELECT label FROM tags WHERE id = 10), ROW_NUMBER() OVER (ORDER BY id DESC) FROM items ORDER BY label, row_number LIMIT 2;
----
label | row_number  (read 4, compared 0)
x | 1
x | 2

# A subquery in FROM is a table of the statement under its alias, as Postgres reads it: its
# columns are the names its SELECT gives, a column it does not give is missing, and its own
# filter reads its own rows (the 2 items of id <= 2). Joined to a table, each of its rows reaches
# the table by key, as a WITH does: the 3 tags and the 2 items they name.
SELECT x.label FROM (SELECT id, name AS label FROM items WHERE id <= 2) AS x ORDER BY x.id DESC;
----
label  (read 2, compared 0)
b
a

SELECT i.name, c.n FROM (SELECT item_id, COUNT(*) AS n FROM tags GROUP BY item_id) c
JOIN items i ON i.id = c.item_id ORDER BY i.id;
----
name | n  (read 5, compared 0)
a | 2
b | 1

SELECT x.name FROM (SELECT id FROM items) AS x;
----
error: ColumnNotFound

SELECT renamed FROM (SELECT id FROM items WHERE id = 1) AS d(renamed);
----
renamed  (read 1, compared 0)
1

SELECT * FROM (SELECT id FROM items) AS d(a,b);
----
error: PlanError

SELECT * FROM LATERAL (SELECT id FROM items) d;
----
error: PlanError

DELETE FROM items WHERE id IN (SELECT item_id FROM tags ORDER BY item_id OFFSET 2);
----
rows affected: 1  (read 4, compared 0)

# A subquery in FROM is planned wherever its SELECT stands: in a later arm of a UNION, and inside another.
SELECT 1 AS x UNION ALL SELECT x FROM (SELECT 2 AS x) d ORDER BY x;
----
x  (read 0, compared 0)
1
2

SELECT x FROM (SELECT x FROM (SELECT 1 AS x) d1) d2;
----
x  (read 0, compared 0)
1
