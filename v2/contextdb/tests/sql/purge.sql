# No neighbouring script erases. Selections use DELETE's predicate surface; every listed
# table is erased together, and an invalid later selection leaves earlier selections intact.
CREATE TABLE notes (id INTEGER PRIMARY KEY, kind TEXT, body TEXT, v VECTOR(3));
CREATE INDEX notes_kind ON notes (kind);
ALTER TABLE notes SET MAINTAIN COUNT (kind);
INSERT INTO notes VALUES (1, 'erase', 'private', [1.0, 0.0, 0.0]), (2, 'keep', 'public', [0.0, 1.0, 0.0]), (3, 'erase', 'secret', [0.0, 0.0, 1.0]);
CREATE TABLE other (id INTEGER PRIMARY KEY, note INTEGER);
INSERT INTO other VALUES (1, 1), (2, 2), (3, 3);

PURGE FROM notes WHERE kind = 'erase', absent WHERE id = 1;
----
error: TableNotFound

SELECT COUNT(*) FROM notes;
----
count  (read 0, compared 0)
3

PURGE FROM notes WHERE id IN (SELECT note FROM other WHERE id = 1), other WHERE id = 3;
SELECT id FROM notes ORDER BY id;
----
id  (read 2, compared 0)
2
3

SELECT id FROM other ORDER BY id;
----
id  (read 2, compared 0)
1
2

SELECT id FROM notes WHERE kind = 'erase';
----
id  (read 1, compared 0)
3

PURGE FROM notes WHERE id > 2 AND kind = 'erase';
SELECT id FROM notes ORDER BY v <=> [0.0, 0.0, 1.0] USE VECTOR EXACT LIMIT 10;
----
id  (read 2, compared 1)
2

# An empty selection changes no row. A key erased earlier can be used for fresh data.
PURGE FROM notes WHERE id = 99;
INSERT INTO notes VALUES (1, 'fresh', 'new', [1.0, 0.0, 0.0]);
SELECT id, body FROM notes ORDER BY id;
----
id | body  (read 2, compared 0)
1 | new
2 | public

# A child follows only its declared reference. Following is transitive and preserves siblings.
CREATE TABLE parents (id INTEGER PRIMARY KEY);
CREATE TABLE children (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parents(id) FOLLOWS PARENT PURGE);
CREATE TABLE grandchildren (id INTEGER PRIMARY KEY, cid INTEGER REFERENCES children(id) FOLLOWS PARENT PURGE);
INSERT INTO parents VALUES (1), (2);
INSERT INTO children VALUES (10, 1), (11, 1), (20, 2);
INSERT INTO grandchildren VALUES (100, 10), (110, 11), (200, 20);
PURGE FROM parents WHERE id = 1;
SELECT id FROM parents ORDER BY id;
----
id  (read 1, compared 0)
2

DELETE FROM parents WHERE id = 2;
----
error: ForeignKeyViolation | foreign key violation: parents is still referenced by children (pid) = (2)

SELECT id FROM children ORDER BY id;
----
id  (read 1, compared 0)
20

CREATE TABLE orphans (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES nowhere(id) FOLLOWS PARENT PURGE);
----
error: TableNotFound | table not found: nowhere

SELECT id FROM grandchildren ORDER BY id;
----
id  (read 1, compared 0)
200

# Listing a referenced parent and its child is one boundary, independent of authored order.
CREATE TABLE roots (id INTEGER PRIMARY KEY);
CREATE TABLE leaves (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES roots(id));
INSERT INTO roots VALUES (1), (2);
INSERT INTO leaves VALUES (10, 1), (20, 2);
PURGE FROM roots WHERE id = 1, leaves WHERE pid = 1;
SELECT id FROM roots ORDER BY id;
----
id  (read 1, compared 0)
2

PURGE FROM leaves, roots;
PURGE FROM notes, other, parents;
SELECT (SELECT COUNT(*) FROM notes) AS notes, (SELECT COUNT(*) FROM children) AS children, (SELECT COUNT(*) FROM grandchildren) AS grandchildren;
----
notes | children | grandchildren  (read 0, compared 0)
0 | 0 | 0

CREATE TABLE pd (id INTEGER PRIMARY KEY, k TEXT, n INTEGER);
CREATE TABLE pp (id INTEGER PRIMARY KEY, k TEXT, n INTEGER);
INSERT INTO pd VALUES (1, 'a', 10), (2, 'a', 20), (3, 'a', 30), (4, 'a', 40), (5, 'a', 50), (6, 'zx', 60), (7, 'zy', 70), (8, NULL, 80), (9, 'a', 90), (10, 'a', 100), (11, 'a', 110), (12, 'a', 120);
INSERT INTO pp VALUES (1, 'a', 10), (2, 'a', 20), (3, 'a', 30), (4, 'a', 40), (5, 'a', 50), (6, 'zx', 60), (7, 'zy', 70), (8, NULL, 80), (9, 'a', 90), (10, 'a', 100), (11, 'a', 110), (12, 'a', 120);
DELETE FROM pd WHERE id IN (1, 2);
PURGE FROM pp WHERE id IN (1, 2);
DELETE FROM pd WHERE id IN (SELECT id FROM pd WHERE n = 30);
PURGE FROM pp WHERE id IN (SELECT id FROM pp WHERE n = 30);
DELETE FROM pd WHERE id BETWEEN 4 AND 5;
PURGE FROM pp WHERE id BETWEEN 4 AND 5;
DELETE FROM pd WHERE k LIKE 'z%';
PURGE FROM pp WHERE k LIKE 'z%';
DELETE FROM pd WHERE k IS NULL;
PURGE FROM pp WHERE k IS NULL;
DELETE FROM pd WHERE id = 9 OR (id = 10 AND n = 100);
PURGE FROM pp WHERE id = 9 OR (id = 10 AND n = 100);
SELECT (SELECT COUNT(*) FROM pd) AS d, (SELECT COUNT(*) FROM pp) AS p, (SELECT COUNT(*) FROM pd JOIN pp ON pd.id = pp.id) AS both;
----
d | p | *
2 | 2 | 2
