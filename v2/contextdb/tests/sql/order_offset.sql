# ORDER BY any expressions, and OFFSET: the rows left out before the first one returned.

CREATE TABLE readings (id INTEGER PRIMARY KEY, tag TEXT, pref TEXT, fallback TEXT, score INTEGER);
INSERT INTO readings (id, tag, pref, fallback, score)
VALUES (1, 'a', NULL, 'm', 3), (2, 'b', 'k', 'z', 1), (3, 'c', NULL, 'b', 2), (4, 'd', 'x', NULL, 2);

SELECT tag FROM readings ORDER BY COALESCE(pref, fallback);
----
tag
c
b
a
d

SELECT tag FROM readings ORDER BY score + 1 DESC, tag DESC;
----
tag
a
d
c
b

# Any expression is a sort key: false sorts before true.
SELECT tag FROM readings ORDER BY score = 2 DESC, tag;
----
tag
c
d
a
b

SELECT tag FROM readings ORDER BY tag LIKE '%d' DESC, score DESC, id;
----
tag
d
a
c
b

# Without OFFSET the answer starts at the first row.
SELECT id FROM readings ORDER BY id LIMIT 2;
----
id
1
2

SELECT id FROM readings ORDER BY id LIMIT 2 OFFSET 1;
----
id
2
3

SELECT id FROM readings ORDER BY id DESC OFFSET 3;
----
id
1

# An OFFSET past the last row is an empty answer.
SELECT id FROM readings ORDER BY id LIMIT 2 OFFSET 4;
----
id

# A COUNT is one row, so an OFFSET past it leaves no row.
SELECT COUNT(*) FROM readings OFFSET 1;
----
count
