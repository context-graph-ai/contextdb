# A field of a JSON value: -> gives it as JSON and ->> as text.

CREATE TABLE docs (id INTEGER PRIMARY KEY, doc JSON);
INSERT INTO docs (id, doc)
VALUES (1, '{"kind":"note","n":5,"tags":["a","b"],"o":{"a":1,"b":2}}'),
       (2, '{"kind":"note","n":"5","o":{"b":2,"a":1}}'),
       (3, '{"kind":"task","n":5.5,"done":true}'),
       (4, NULL);

SELECT id, doc -> 'kind' AS kind, doc ->> 'kind' AS word FROM docs ORDER BY id;
----
id | kind | word  (read 4, compared 0)
1 | "note" | note
2 | "note" | note
3 | "task" | task
4 | NULL | NULL

# A number after -> is a place in an array, from 0, and a field of a field is reached in turn.
SELECT doc -> 'tags' -> 1 AS second, doc -> 'o' ->> 'b' AS b FROM docs WHERE id = 1;
----
second | b  (read 1, compared 0)
"b" | 2

# A field that is not there, or a field of what is not an object, is NULL.
SELECT id, doc -> 'done' AS done, doc -> 'kind' -> 'x' AS x FROM docs ORDER BY id;
----
id | done | x  (read 4, compared 0)
1 | NULL | NULL
2 | NULL | NULL
3 | true | NULL
4 | NULL | NULL

# JSON values are equal as values of their own type, a number by its value however it is written: the number 5 is 5.0 and is not the text "5".
SELECT id FROM docs WHERE doc -> 'n' = '5.0' ORDER BY id;
----
id  (read 4, compared 0)
1

SELECT id FROM docs WHERE doc -> 'n' = '"5"' ORDER BY id;
----
id  (read 4, compared 0)
2

# An object is equal whatever order its keys were written in.
SELECT id FROM docs WHERE doc -> 'o' = '{"b":2,"a":1}' ORDER BY id;
----
id  (read 4, compared 0)
1
2

# Conditions on two fields are joined with AND as any others are.
SELECT id FROM docs WHERE doc -> 'kind' = '"note"' AND doc -> 'n' = '"5"' ORDER BY id;
----
id  (read 4, compared 0)
2

# An index over a field is declared with the field in parentheses, and answers the same.
CREATE INDEX docs_kind ON docs ((doc -> 'kind'));
SELECT id FROM docs WHERE doc -> 'kind' = '"note"' ORDER BY id;
----
id  (read 2, compared 0)
1
2

SELECT id FROM docs WHERE doc -> 'kind' = '"task"' AND doc -> 'n' = '5.5' ORDER BY id;
----
id  (read 1, compared 0)
3

# The index answers the field written on the right of = too, and each value of an IN list.
SELECT id FROM docs WHERE '"note"' = doc -> 'kind' ORDER BY id;
----
id  (read 2, compared 0)
1
2

SELECT id FROM docs WHERE doc -> 'kind' IN ('"note"', '"task"') ORDER BY id;
----
id  (read 3, compared 0)
1
2
3

# A row written after the index was made is found through it.
INSERT INTO docs (id, doc) VALUES (5, '{"kind":"task"}');
SELECT id FROM docs WHERE doc -> 'kind' = '"task"' ORDER BY id;
----
id  (read 2, compared 0)
3
5

# A quoted literal beside a JSON value is read as JSON: 'true' is the boolean, a bracketed
# list that array, in its order; a boolean is not a number's equal.
SELECT id FROM docs WHERE doc -> 'done' = 'true' ORDER BY id;
----
id  (read 5, compared 0)
3

SELECT id FROM docs WHERE doc -> 'done' = '1';
----
id  (read 5, compared 0)

SELECT id FROM docs WHERE doc -> 'tags' = '["b","a"]';
----
id  (read 5, compared 0)

SELECT id FROM docs WHERE doc -> 'tags' = '["a","b"]';
----
id  (read 5, compared 0)
1

# Arrays or objects of different sizes are never equal, though one begins as the other does.
SELECT id FROM docs WHERE doc -> 'tags' = '["a"]' OR doc -> 'o' = '{"a":1}';
----
id  (read 5, compared 0)

# A word that is not JSON is refused by name, not matched against nothing.
SELECT id FROM docs WHERE doc -> 'kind' = 'active';
----
error: PlanError

# JSON values are not ordered, so BETWEEN is refused as < and > are.
SELECT id FROM docs WHERE doc BETWEEN '1' AND '2';
----
error: PlanError

# A field's index follows its rows: found by what a row holds now, not by what it held.
CREATE INDEX docs_n ON docs ((doc -> 'n'));
UPDATE docs SET doc = '{"kind":"note","n":7}' WHERE id = 1;
DELETE FROM docs WHERE id = 3;
INSERT INTO docs (id, doc) VALUES (6, '{"n":5}');
SELECT id FROM docs WHERE doc -> 'n' = '7';
----
id  (read 1, compared 0)
1

SELECT id FROM docs WHERE doc -> 'n' = '5.0';
----
id  (read 1, compared 0)
6

SELECT id FROM docs WHERE doc -> 'n' = '5.5';
----
id  (read 0, compared 0)

# Objects sort as Postgres jsonb sorts them: fewer pairs first, then key by key as text, so {"a":2} < {"aa":1} < {"b":1}; and since keys compare in storage order, shorter first, {"b":1,"d":1} < {"aa":1,"c":1}.
INSERT INTO docs (id, doc) VALUES (7, '{"b":1}'), (8, '{"aa":1}'), (9, '{"a":2}'), (10, '{"aa":1,"c":1}'), (11, '{"b":1,"d":1}');
SELECT id FROM docs WHERE id > 6 ORDER BY doc;
----
id  (read 5, compared 0)
9
8
7
11
10

# Whole numbers past the largest signed 64-bit one stay different numbers.
INSERT INTO docs (id, doc) VALUES (12, '{"n":9223372036854775808}');
SELECT id FROM docs WHERE doc -> 'n' = '9223372036854775809';
----
id  (read 1, compared 0)

# Values of different kinds sort as Postgres sorts jsonb: null, string, number, boolean, array, object.
INSERT INTO docs (id, doc) VALUES (13, '"s"'), (14, '{"a":1}'), (15, 'null'), (16, 'true'), (17, '[1]'), (18, '5');
SELECT id FROM docs WHERE id > 12 ORDER BY doc;
----
id  (read 6, compared 0)
15
13
18
16
17
14
