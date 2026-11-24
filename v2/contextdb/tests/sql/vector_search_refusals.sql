# What a similarity search refuses, each by the name of its kind. No refusal changes a row: the
# last statement reads the table back as it was written.

CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, closeness REAL, v VECTOR(3), w VECTOR(3));
INSERT INTO docs (id, title, v, w)
VALUES (1, 'one', [1.0, 0.0, 0.0], [0.0, 1.0, 0.0]), (2, 'two', [0.0, 1.0, 0.0], [1.0, 0.0, 0.0]);

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

INSERT INTO docs (id, v) VALUES (3, [1.0, 0.0]);
----
error: ColumnTypeMismatch

UPDATE docs SET v = [1.0] WHERE id = 1;
----
error: ColumnTypeMismatch

# There is no search function, and a distance is not carried into a write: an UPDATE's value, a
# DELETE's filter. (A vector taken from another row is refused in rank_policy.sql, and every
# other use of <=> is an answer in vector_search.sql.)
SELECT vector_search(v, [1.0, 0.0, 0.0], 2) FROM docs;
----
error: PlanError

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
id | score | distance  (read 4, compared 2)
2 | 9.5 | 0.0
1 | 0.25 | 1.0

# Every row of the first table is as it was written.
SELECT id, title, closeness, v, w FROM docs ORDER BY id;
----
id | title | closeness | v | w  (read 2, compared 0)
1 | one | NULL | [1.0, 0.0, 0.0] | [0.0, 1.0, 0.0]
2 | two | NULL | [0.0, 1.0, 0.0] | [1.0, 0.0, 0.0]

INSERT INTO docs (id, v) VALUES (30, [1.0, 0.0]);
----
error: ColumnTypeMismatch

UPDATE docs SET v = [1.0] WHERE id = 1;
----
error: ColumnTypeMismatch
