# WITH: a named SELECT that the statement after it reads as a table.

CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, status TEXT);
INSERT INTO items (id, name, status)
VALUES (1, 'sensor-a', 'active'), (2, 'sensor-b', 'retired'), (3, 'meter', 'active');

WITH active AS (SELECT id, name FROM items WHERE status = 'active')
SELECT * FROM active WHERE name LIKE 'sensor%';
----
id | name
1 | sensor-a

# Several are separated by commas, a later one may read an earlier one, and one may be read
# more than once.
WITH active AS (SELECT id, name FROM items WHERE status = 'active'),
     meters AS (SELECT id FROM active WHERE name = 'meter')
SELECT a.name FROM active a JOIN meters m ON a.id = m.id;
----
name
meter

# Its columns are the names its SELECT gives them.
WITH n AS (SELECT COUNT(*) AS total FROM items)
SELECT total FROM n;
----
total
3

# A name that is neither a table nor a WITH name of the statement is a missing table.
WITH active AS (SELECT id FROM items)
SELECT id FROM retired;
----
error: TableNotFound

# A WITH that reads itself is refused by name.
WITH RECURSIVE up AS (SELECT id FROM items)
SELECT id FROM up;
----
error: RecursiveCteNotSupported

# A name that two columns of a WITH answer to is refused where it is read.
WITH w AS (SELECT id AS x, name AS x FROM items) SELECT x FROM w;
----
error: PlanError
