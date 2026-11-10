# ORDER BY and LIMIT. NULL sorts last going up and first going down, with or without an index.

CREATE TABLE scores (id INTEGER PRIMARY KEY, team TEXT, points INTEGER, pace REAL);
INSERT INTO scores (id, team, points, pace)
VALUES (1, 'b', 30, 1.5), (2, 'a', NULL, 0.5), (3, 'a', 10, NULL), (4, 'c', 20, -2.0), (5, NULL, 20, 9.0);

SELECT id, points FROM scores ORDER BY points, id;
----
id | points
3 | 10
4 | 20
5 | 20
1 | 30
2 | NULL

SELECT id, points FROM scores ORDER BY points DESC, id;
----
id | points
2 | NULL
1 | 30
4 | 20
5 | 20
3 | 10

SELECT id, team FROM scores ORDER BY team DESC, id DESC;
----
id | team
5 | NULL
4 | c
1 | b
3 | a
2 | a

SELECT id, pace FROM scores ORDER BY pace ASC;
----
id | pace
4 | -2.0
2 | 0.5
1 | 1.5
5 | 9.0
3 | NULL

# A sort key may be the first of several values that is not NULL: row 2, whose points are
# NULL, sorts where the value given for it belongs and not where a NULL does.
SELECT id, points FROM scores ORDER BY COALESCE(points, 0), id;
----
id | points
2 | NULL
3 | 10
4 | 20
5 | 20
1 | 30

SELECT id, points FROM scores ORDER BY COALESCE(points, 25) DESC, id;
----
id | points
1 | 30
2 | NULL
4 | 20
5 | 20
3 | 10

# LIMIT stands on its own and keeps the first rows of the order asked for.
SELECT id FROM scores ORDER BY id LIMIT 2;
----
id
1
2

SELECT id FROM scores ORDER BY id DESC LIMIT 1;
----
id
5

SELECT id FROM scores ORDER BY id LIMIT 0;
----
id

SELECT id FROM scores ORDER BY id LIMIT 100;
----
id
1
2
3
4
5

# An index keeps each of its columns in the direction it was declared with, and the answers
# through it are the answers above.
CREATE INDEX scores_by_points ON scores (points DESC, id ASC);

SELECT id, points FROM scores ORDER BY points DESC, id ASC;
----
id | points
2 | NULL
1 | 30
4 | 20
5 | 20
3 | 10

SELECT id, points FROM scores ORDER BY points DESC, id ASC LIMIT 3;
----
id | points
2 | NULL
1 | 30
4 | 20

SELECT id, points FROM scores WHERE points >= 20 ORDER BY points DESC, id ASC;
----
id | points
1 | 30
4 | 20
5 | 20

SELECT id, points FROM scores ORDER BY points ASC, id ASC;
----
id | points
3 | 10
4 | 20
5 | 20
1 | 30
2 | NULL
