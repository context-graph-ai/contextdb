# WHERE: the comparison, logical and arithmetic operators, NULL checks, and what a NULL does to
# each of them. Row 3 holds NULL in every column but its key and `b`.

CREATE TABLE readings (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, ratio REAL, tag TEXT, active BOOLEAN);
INSERT INTO readings (id, a, b, ratio, tag, active)
VALUES (1, 7, 2, 0.5, 'p', TRUE),
       (2, 6, 3, 1.5, 'q', FALSE),
       (3, NULL, 4, NULL, NULL, NULL),
       (4, 9, 0, 2.0, 'r', TRUE);

# Each comparison operator. A NULL compared with anything leaves its row out and is no error,
# so row 3 is in none of these answers.
SELECT id FROM readings WHERE a = 7 ORDER BY id;
----
id
1

SELECT id FROM readings WHERE a != 7 ORDER BY id;
----
id
2
4

SELECT id FROM readings WHERE a <> 7 ORDER BY id;
----
id
2
4

SELECT id FROM readings WHERE a < 7 ORDER BY id;
----
id
2

SELECT id FROM readings WHERE a <= 7 ORDER BY id;
----
id
1
2

SELECT id FROM readings WHERE a > 7 ORDER BY id;
----
id
4

SELECT id FROM readings WHERE a >= 7 ORDER BY id;
----
id
1
4

SELECT id FROM readings WHERE tag >= 'q' ORDER BY id;
----
id
2
4

SELECT id FROM readings WHERE ratio < 100.0 ORDER BY id;
----
id
1
2
4

# A boolean column is a predicate on its own, and so is its negation. NULL is neither.
SELECT id FROM readings WHERE active ORDER BY id;
----
id
1
4

SELECT id FROM readings WHERE NOT active ORDER BY id;
----
id
2

# A literal NULL matches nothing and is no error.
SELECT id FROM readings WHERE NULL ORDER BY id;
----
id

# AND, OR and NOT. AND binds tighter than OR.
SELECT id FROM readings WHERE a > 6 AND b > 0 ORDER BY id;
----
id
1

SELECT id FROM readings WHERE a > 8 OR b = 3 ORDER BY id;
----
id
2
4

SELECT id FROM readings WHERE NOT (a > 6) ORDER BY id;
----
id
2

SELECT id FROM readings WHERE a = 6 OR a = 7 AND b = 0 ORDER BY id;
----
id
2

# IS NULL and IS NOT NULL are how a NULL is found.
SELECT id FROM readings WHERE a IS NULL ORDER BY id;
----
id
3

SELECT id FROM readings WHERE tag IS NOT NULL ORDER BY id;
----
id
1
2
4

SELECT id FROM readings WHERE active IS NULL OR a IS NOT NULL AND b = 0 ORDER BY id;
----
id
3
4

# The four arithmetic operators, in a select list and in a predicate. A NULL operand gives NULL.
SELECT id, a + b AS plus, a - b AS minus, a * b AS times FROM readings ORDER BY id;
----
id | plus | minus | times
1 | 9 | 5 | 14
2 | 9 | 3 | 18
3 | NULL | NULL | NULL
4 | 9 | 9 | 0

SELECT a / b AS divided, ratio / 0.5 AS doubled FROM readings WHERE id = 2;
----
divided | doubled
2 | 3.0

SELECT id, a * ratio AS scaled FROM readings ORDER BY id;
----
id | scaled
1 | 3.5
2 | 9.0
3 | NULL
4 | 18.0

SELECT id FROM readings WHERE a + b > 8 ORDER BY id;
----
id
1
2
4

SELECT id FROM readings WHERE a * 2 - 1 = 13 ORDER BY id;
----
id
1
