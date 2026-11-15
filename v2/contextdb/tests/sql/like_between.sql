# LIKE matches text against a pattern; BETWEEN is a range with both ends in it.

CREATE TABLE parts (id INTEGER PRIMARY KEY, name TEXT, share REAL);
INSERT INTO parts (id, name, share)
VALUES (1, 'sensor-1', 0.5), (2, 'sensor-22', 1.0), (3, 'item_3', 0.05), (4, 'itemX3', 0.1),
       (5, '50% off', NULL), (6, NULL, 0.75);

# % stands for any run of characters, none included.
SELECT id FROM parts WHERE name LIKE 'sensor%' ORDER BY id;
----
id
1
2

# _ stands for exactly one character.
SELECT id FROM parts WHERE name LIKE 'sensor-_' ORDER BY id;
----
id
1

SELECT id FROM parts WHERE name LIKE 'item_3' ORDER BY id;
----
id
3
4

# After a backslash, % and _ are the characters themselves.
SELECT id FROM parts WHERE name LIKE 'item\_3' ORDER BY id;
----
id
3

SELECT id FROM parts WHERE name LIKE '50\%%' ORDER BY id;
----
id
5

# A pattern with neither matches the whole text only, and upper and lower case differ.
SELECT id FROM parts WHERE name LIKE 'sensor' OR name LIKE 'ITEM_3' ORDER BY id;
----
id

# NOT LIKE is every row with a name the pattern does not match; a NULL name is in neither.
SELECT id FROM parts WHERE name NOT LIKE '%3%' ORDER BY id;
----
id
1
2
5

# BETWEEN includes both of its ends.
SELECT id FROM parts WHERE share BETWEEN 0.5 AND 1.0 ORDER BY id;
----
id
1
2
6

SELECT id FROM parts WHERE id BETWEEN 2 AND 3 ORDER BY id;
----
id
2
3

# NOT BETWEEN is every row with a value outside the range; a NULL value is in neither.
SELECT id FROM parts WHERE share NOT BETWEEN 0 AND 0.1 ORDER BY id;
----
id
1
2
6

# AND after a range's second end is the next condition.
SELECT id FROM parts WHERE id BETWEEN 1 AND 4 AND name LIKE 'item%' ORDER BY id;
----
id
3
4
