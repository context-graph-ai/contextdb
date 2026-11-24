# WITH: a named SELECT that the statement after it reads as a table.

CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, status TEXT);
INSERT INTO items (id, name, status)
VALUES (1, 'sensor-a', 'active'), (2, 'sensor-b', 'retired'), (3, 'meter', 'active');

WITH active AS (SELECT id, name FROM items WHERE status = 'active')
SELECT * FROM active WHERE name LIKE 'sensor%';
----
id | name  (read 3, compared 0)
1 | sensor-a

# Several are separated by commas, a later one may read an earlier one, and one may be read
# more than once.
WITH active AS (SELECT id, name FROM items WHERE status = 'active'),
     meters AS (SELECT id FROM active WHERE name = 'meter')
SELECT a.name FROM active a JOIN meters m ON a.id = m.id;
----
name  (read 3, compared 0)
meter

# Its columns are the names its SELECT gives them.
WITH n AS (SELECT COUNT(*) AS total FROM items)
SELECT total FROM n;
----
total  (read 0, compared 0)
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

# A WITH name joined to an indexed table reaches the table through its index: the 3 items the
# WITH reads and the 2 links of the active items, never the 8 links.
CREATE TABLE links (id INTEGER PRIMARY KEY, item_id INTEGER, note TEXT);
CREATE INDEX links_item ON links (item_id);
INSERT INTO links (id, item_id, note)
VALUES (1, 1, 'p'), (2, 2, 'q'), (3, 2, 'r'), (4, 2, 's'), (5, 3, 't'), (6, 4, 'u'), (7, 4, 'v'), (8, 5, 'w');
WITH active AS (SELECT id FROM items WHERE status = 'active')
SELECT l.note FROM active a JOIN links l ON l.item_id = a.id ORDER BY l.id;
----
note  (read 5, compared 0)
p
t

# In the other order, the stored table written first, the join still reaches it through its
# index, as Postgres chooses the side to drive from: the same 3 items and 2 links.
WITH active AS (SELECT id FROM items WHERE status = 'active')
SELECT l.note FROM links l INNER JOIN active a ON l.item_id = a.id ORDER BY l.id;
----
note  (read 5, compared 0)
p
t
