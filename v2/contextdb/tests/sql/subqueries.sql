# A SELECT inside a statement: after IN, and as one value.

CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE tags (id INTEGER PRIMARY KEY, item_id INTEGER, label TEXT);
INSERT INTO items (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO tags (id, item_id, label) VALUES (10, 1, 'x'), (11, 1, 'y'), (12, 2, 'x');

SELECT name FROM items WHERE id IN (SELECT item_id FROM tags WHERE label = 'x') ORDER BY id;
----
name
a
b

SELECT name FROM items WHERE id NOT IN (SELECT item_id FROM tags) ORDER BY id;
----
name
c

# The SELECT after IN gives one column.
SELECT name FROM items WHERE id IN (SELECT item_id, label FROM tags);
----
error: PlanError

# A SELECT in parentheses that gives one row of one column is that value, wherever a value may
# stand; one that gives no row is NULL.
SELECT name FROM items WHERE id = (SELECT MAX(item_id) FROM tags);
----
name
b

SELECT name, (SELECT COUNT(*) FROM tags) AS n FROM items WHERE id = 3;
----
name | n
c | 3

SELECT name FROM items WHERE id = (SELECT item_id FROM tags WHERE label = 'none');
----
name

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
n | k
3 | 10

SELECT id FROM items GROUP BY id HAVING COUNT(*) > (SELECT 0) ORDER BY id;
----
id
1
2
3

SELECT id, (SELECT 10) AS k, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM items ORDER BY id;
----
id | k | rn
1 | 10 | 1
2 | 10 | 2
3 | 10 | 3

# An unaliased window function is named row_number and an unaliased one-column SELECT by its column.
SELECT (SELECT label FROM tags WHERE id = 10), ROW_NUMBER() OVER (ORDER BY id DESC) FROM items ORDER BY label, row_number LIMIT 2;
----
label | row_number
x | 1
x | 2
