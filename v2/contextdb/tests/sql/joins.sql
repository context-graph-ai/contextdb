# Joins: INNER, LEFT and CROSS, with the names a statement may give its tables.

CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE tags (id INTEGER PRIMARY KEY, item_id INTEGER, label TEXT);
CREATE INDEX tags_item ON tags (item_id);
INSERT INTO items (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO tags (id, item_id, label) VALUES (10, 1, 'x'), (11, 1, 'y'), (12, 2, 'x'), (13, NULL, 'z');

# An inner join gives one row for each pair the condition holds for. A qualifier may be the
# table's own name, and a result column is named by its column alone.
SELECT items.id, tags.label FROM items INNER JOIN tags ON items.id = tags.item_id
ORDER BY items.id, tags.label;
----
id | label
1 | x
1 | y
2 | x

# A bare JOIN is an inner join, and a name after a table is its alias with or without AS.
SELECT x.name, y.label FROM items x JOIN tags y ON x.id = y.item_id WHERE y.label = 'x'
ORDER BY x.id;
----
name | label
a | x
b | x

# A left join keeps a left row that nothing matches, with NULL for the right side.
SELECT i.id, t.label FROM items AS i LEFT JOIN tags AS t ON i.id = t.item_id
ORDER BY i.id, t.label;
----
id | label
1 | x
1 | y
2 | x
3 | NULL

# CROSS JOIN gives the rows of an inner join whose condition always holds.
SELECT i.id, t.id FROM items i CROSS JOIN tags t WHERE t.id >= 12 ORDER BY i.id, t.id;
----
id | id
1 | 12
1 | 13
2 | 12
2 | 13
3 | 12
3 | 13

SELECT i.id, t.id FROM items i INNER JOIN tags t ON 1 = 1 WHERE t.id >= 12 ORDER BY i.id, t.id;
----
id | id
1 | 12
1 | 13
2 | 12
2 | 13
3 | 12
3 | 13

# A qualifier that names no table or alias of the statement is refused, though another table
# has the column.
SELECT id FROM items WHERE x.name = 'a';
----
error: ColumnNotFound

# A join condition that cannot be worked out is a refusal, never fewer rows.
SELECT i.id FROM items i JOIN tags t ON -t.label > 0;
----
error: PlanError

# Three tables: an inner join and a left join after it, each table under an alias. A tag with
# no note is kept with NULL.
CREATE TABLE notes (id INTEGER PRIMARY KEY, tag_id INTEGER, note TEXT);
INSERT INTO notes (id, tag_id, note) VALUES (100, 10, 'n10'), (101, 12, 'n12');
SELECT i.id, t.label, n.note FROM items AS i INNER JOIN tags AS t ON i.id = t.item_id
LEFT JOIN notes n ON n.tag_id = t.id ORDER BY i.id, t.label;
----
id | label | note
1 | x | n10
1 | y | NULL
2 | x | n12

# Keywords in lower case say the same as the first statement of this script.
select items.id, tags.label from items inner join tags on items.id = tags.item_id
order by items.id, tags.label;
----
id | label
1 | x
1 | y
2 | x

# A name two relations give is refused, and so is a table named twice.
SELECT id FROM items LEFT JOIN tags ON items.id = tags.item_id;
----
error: PlanError

SELECT * FROM items JOIN items ON items.id = items.id;
----
error: PlanError

# A WHERE with a SELECT in it filters a join.
SELECT i.id, t.label FROM items i JOIN tags t ON i.id = t.item_id
WHERE i.id IN (SELECT item_id FROM tags WHERE label = 'y') ORDER BY t.label;
----
id | label
1 | x
1 | y

# A bare ORDER BY name that the select list gives is that column, not the first of that name.
SELECT tags.id FROM items JOIN tags ON items.id = tags.item_id ORDER BY id DESC;
----
id
12
11
10

# The name of an unaliased function is an output column, read before any input column of the
# same name (Postgres).
CREATE TABLE ca (id INTEGER PRIMARY KEY, count INTEGER);
CREATE TABLE cb (id INTEGER PRIMARY KEY, count INTEGER);
INSERT INTO ca (id, count) VALUES (1, 10);
INSERT INTO cb (id, count) VALUES (1, 20);
SELECT COUNT(*) FROM ca JOIN cb ON ca.id = cb.id ORDER BY count;
----
count
1
