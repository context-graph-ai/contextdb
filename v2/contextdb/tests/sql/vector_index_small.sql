# INDEXED over rows that no maintenance cycle has grouped: it searches every one of them, so
# INDEXED, AUTO and EXACT give the same rows at the same distances. From [1, 0]: rows 1 and 3
# are at distance 0, row 5 between, rows 2 and 6 at 1, row 4 at 2, and row 7 has no vector.

CREATE TABLE passages (id INTEGER PRIMARY KEY, a TEXT NOT NULL, v VECTOR(2) PARTITION_KEY (a));
INSERT INTO passages (id, a, v)
VALUES (6, 'y', [0.0, 2.0]),
       (3, 'y', [1.0, 0.0]),
       (1, 'x', [1.0, 0.0]),
       (5, 'y', [1.0, 1.0]),
       (2, 'x', [0.0, 1.0]),
       (7, 'x', NULL),
       (4, 'x', [-1.0, 0.0]);

SELECT id, v <=> [1.0, 0.0] AS distance FROM passages ORDER BY distance USE VECTOR INDEXED LIMIT 5;
----
id | distance
1 | 0.0
3 | 0.0
5 | *
2 | 1.0
6 | 1.0

SELECT id, v <=> [1.0, 0.0] AS distance FROM passages ORDER BY distance USE VECTOR AUTO LIMIT 5;
----
id | distance
1 | 0.0
3 | 0.0
5 | *
2 | 1.0
6 | 1.0

SELECT id, v <=> [1.0, 0.0] AS distance FROM passages ORDER BY distance USE VECTOR EXACT LIMIT 5;
----
id | distance
1 | 0.0
3 | 0.0
5 | *
2 | 1.0
6 | 1.0

# One partition named, and a LIMIT above what it holds: every row of it that has a vector.
SELECT id, v <=> [1.0, 0.0] AS distance FROM passages WHERE a = 'x' ORDER BY distance USE VECTOR INDEXED LIMIT 5;
----
id | distance
1 | 0.0
2 | 1.0
4 | 2.0

# Key values no row has select nothing and refuse nothing.
SELECT id FROM passages WHERE a = 'z' ORDER BY v <=> [1.0, 0.0] USE VECTOR INDEXED LIMIT 5;
----
id

# A column declared SEARCH_MODE INDEXED answers with no USE VECTOR, and as EXACT does.
CREATE TABLE declared (id INTEGER PRIMARY KEY, v VECTOR(2) SEARCH_MODE INDEXED);
INSERT INTO declared (id, v) VALUES (1, [0.0, 1.0]), (2, [1.0, 0.0]), (3, [-1.0, 0.0]);
SELECT id, v <=> [1.0, 0.0] AS distance FROM declared ORDER BY distance LIMIT 3;
----
id | distance
2 | 0.0
1 | 1.0
3 | 2.0

SELECT id, v <=> [1.0, 0.0] AS distance FROM declared ORDER BY distance USE VECTOR EXACT LIMIT 3;
----
id | distance
2 | 0.0
1 | 1.0
3 | 2.0
