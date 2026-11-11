# What a similarity search refuses, each by the name of its kind. No refusal changes a row: the
# last statement reads the table back as it was written.

CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, closeness REAL, v VECTOR(3), w VECTOR(3));
INSERT INTO docs (id, title, v, w)
VALUES (1, 'one', [1.0, 0.0, 0.0], [0.0, 1.0, 0.0]), (2, 'two', [0.0, 1.0, 0.0], [1.0, 0.0, 0.0]);

# A search needs a LIMIT: without one it is not supported in this version. The refusal has no
# kind of its own: it is the one every statement of a later version gets, and its sentence
# names a vector search without LIMIT.
SELECT id FROM docs ORDER BY v <=> [1.0, 0.0, 0.0];
----
error: PlanError

SELECT id, v <=> [1.0, 0.0, 0.0] AS distance FROM docs ORDER BY distance;
----
error: PlanError

# USE VECTOR belongs to a search, not to a plain ORDER BY.
SELECT id FROM docs ORDER BY id USE VECTOR EXACT LIMIT 2;
----
error: UseVectorRequiresVectorOrder

# The column left of <=> is a VECTOR column, and the vector has that column's dimension.
SELECT id FROM docs ORDER BY title <=> [1.0, 0.0, 0.0] LIMIT 2;
----
error: UnknownVectorIndex

SELECT id FROM docs ORDER BY v <=> [1.0, 0.0] LIMIT 2;
----
error: VectorIndexDimensionMismatch

SELECT id FROM docs ORDER BY v <=> '[1,0,0,0]' LIMIT 2;
----
error: VectorIndexDimensionMismatch

# INDEXED is not refused for rows that no maintenance cycle has grouped yet: asked for in the
# statement or declared on the column, it searches every one of them, as EXACT and AUTO do.
SELECT id FROM docs ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR INDEXED LIMIT 2;
----
id
1
2

SELECT id FROM docs ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR EXACT LIMIT 2;
----
id
1
2

CREATE TABLE indexed (id INTEGER PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED);
INSERT INTO indexed (id, v) VALUES (1, [0.0, 1.0, 0.0]), (2, [1.0, 0.0, 0.0]);
SELECT id FROM indexed ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 2;
----
id
2
1

SELECT id FROM indexed ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR EXACT LIMIT 2;
----
id
2
1

SELECT id FROM indexed ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR AUTO LIMIT 2;
----
id
2
1

# <=> in ORDER BY is the whole of vector search: there is no search function, no vector taken
# from another row, and no ranking by a policy in this version.
SELECT vector_search(v, [1.0, 0.0, 0.0], 2) FROM docs;
----
error: PlanError

SELECT id FROM docs ORDER BY v <=> ROW_VECTOR('docs', 'v', 1) LIMIT 2;
----
error: PlanError

SELECT id FROM docs ORDER BY v <=> [1.0, 0.0, 0.0] USE RANK effective_confidence LIMIT 2;
----
error: PlanError

# A distance has a value only for the rows of a search, as that search's one ascending sort key.
# Refused by name: a distance in a WHERE, with a search beside it or without;
SELECT id FROM docs WHERE v <=> [1.0, 0.0, 0.0] < 0.5;
----
error: PlanError

SELECT id FROM docs WHERE v <=> [1.0, 0.0, 0.0] < 0.5 ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 2;
----
error: PlanError

# in a select list whose statement does not order by it;
SELECT id, v <=> [1.0, 0.0, 0.0] AS distance FROM docs;
----
error: PlanError

SELECT id, v <=> [1.0, 0.0, 0.0] AS distance FROM docs ORDER BY id LIMIT 2;
----
error: PlanError

# farthest first, and beside a second sort key;
SELECT id FROM docs ORDER BY v <=> [1.0, 0.0, 0.0] DESC LIMIT 2;
----
error: PlanError

SELECT id FROM docs ORDER BY v <=> [1.0, 0.0, 0.0], id LIMIT 2;
----
error: PlanError

# in the select list of a search, to another vector or from another column than the ORDER BY's;
SELECT id, v <=> [0.0, 1.0, 0.0] AS other FROM docs ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 2;
----
error: PlanError

SELECT id, w <=> [1.0, 0.0, 0.0] AS other FROM docs ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 2;
----
error: PlanError

# between two columns, and from something that is not a column;
SELECT id FROM docs ORDER BY v <=> w LIMIT 2;
----
error: PlanError

SELECT id FROM docs ORDER BY [0.0, 1.0, 0.0] <=> [1.0, 0.0, 0.0] LIMIT 2;
----
error: PlanError

# and in a statement that writes.
UPDATE docs SET closeness = v <=> [1.0, 0.0, 0.0];
----
error: PlanError

DELETE FROM docs WHERE v <=> [1.0, 0.0, 0.0] < 0.5;
----
error: PlanError

# A search adds no score column. On a table without one the name is a column that is not there;
# on a table with one it is that column's own values.
SELECT id, score FROM docs ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 2;
----
error: ColumnNotFound

CREATE TABLE graded (id INTEGER PRIMARY KEY, score REAL, v VECTOR(3));
INSERT INTO graded (id, score, v) VALUES (1, 0.25, [0.0, 1.0, 0.0]), (2, 9.5, [1.0, 0.0, 0.0]);
SELECT id, score, v <=> [1.0, 0.0, 0.0] AS distance FROM graded ORDER BY distance LIMIT 2;
----
id | score | distance
2 | 9.5 | 0.0
1 | 0.25 | 1.0

# Every row of the first table is as it was written.
SELECT id, title, closeness, v, w FROM docs ORDER BY id;
----
id | title | closeness | v | w
1 | one | NULL | [1.0, 0.0, 0.0] | [0.0, 1.0, 0.0]
2 | two | NULL | [0.0, 1.0, 0.0] | [1.0, 0.0, 0.0]
