# A VECTOR column with PARTITION_KEY keeps its vectors in one partition for each set of key
# values. A filter on the key columns chooses the partitions a search reads; the answer is still
# the nearest rows the whole filter passes, cut to the LIMIT after every chosen partition is
# compared. From [1, 0]: rows 1 and 3 are at distance 0, row 5 between, rows 2 and 6 at 1, row 4
# at 2, and row 7 has no vector. The rows are written out of key order.

CREATE TABLE passages (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b INTEGER NOT NULL, v VECTOR(2) PARTITION_KEY (a, b));
INSERT INTO passages (id, a, b, v)
VALUES (6, 'y', 1, [0.0, 2.0]),
       (3, 'x', 2, [1.0, 0.0]),
       (1, 'x', 1, [1.0, 0.0]),
       (5, 'y', 1, [1.0, 1.0]),
       (2, 'x', 1, [0.0, 1.0]),
       (7, 'x', 1, NULL),
       (4, 'x', 2, [-1.0, 0.0]);

# Every key column given: one partition.
SELECT id, v <=> [1.0, 0.0] AS distance FROM passages WHERE a = 'x' AND b = 1 ORDER BY distance LIMIT 5;
----
id | distance
1 | 0.0
2 | 1.0

# Several partitions, named by a list of row values or by an OR: one answer, nearest first.
SELECT id, v <=> [1.0, 0.0] AS distance FROM passages WHERE (a, b) IN (('x', 1), ('y', 1)) ORDER BY distance LIMIT 5;
----
id | distance
1 | 0.0
5 | *
2 | 1.0
6 | 1.0

SELECT id, v <=> [1.0, 0.0] AS distance FROM passages WHERE (a = 'x' AND b = 1) OR (a = 'y' AND b = 1) ORDER BY distance LIMIT 5;
----
id | distance
1 | 0.0
5 | *
2 | 1.0
6 | 1.0

# Only the first key column given: every partition under it. Rows 1 and 3 hold the same vector
# in two partitions and come in key order, though row 3 was written first.
SELECT id, v <=> [1.0, 0.0] AS distance FROM passages WHERE a = 'x' ORDER BY distance LIMIT 5;
----
id | distance
1 | 0.0
3 | 0.0
2 | 1.0
4 | 2.0

# No key column given: every partition.
SELECT id, v <=> [1.0, 0.0] AS distance FROM passages ORDER BY distance LIMIT 10;
----
id | distance
1 | 0.0
3 | 0.0
5 | *
2 | 1.0
6 | 1.0
4 | 2.0

# LIMIT cuts the one answer, not each partition: from [0, 1] the two nearest rows of these two
# partitions are both in ('y', 1).
SELECT id, v <=> [0.0, 1.0] AS distance FROM passages WHERE (a, b) IN (('x', 2), ('y', 1)) ORDER BY distance LIMIT 2;
----
id | distance
6 | 0.0
5 | *

# Key values no row has give no rows, and never the rows of another partition.
SELECT id FROM passages WHERE a = 'z' AND b = 1 ORDER BY v <=> [1.0, 0.0] LIMIT 5;
----
id

SELECT id FROM passages WHERE a = 'x' AND b = 3 ORDER BY v <=> [1.0, 0.0] LIMIT 5;
----
id

SELECT id FROM passages WHERE (a, b) IN (('z', 9), ('y', 1)) ORDER BY v <=> [1.0, 0.0] LIMIT 5;
----
id
5
6

# An UPDATE of a key column moves the row to its new partition.
UPDATE passages SET b = 2 WHERE id = 2;
----
rows affected: 1

SELECT id FROM passages WHERE a = 'x' AND b = 1 ORDER BY v <=> [1.0, 0.0] LIMIT 5;
----
id
1

SELECT id FROM passages WHERE a = 'x' AND b = 2 ORDER BY v <=> [1.0, 0.0] LIMIT 5;
----
id
3
2
4

# MAX_PARTITIONS is the most partitions that may hold a vector at once. At 2, a row that would
# start a third is refused, by INSERT or by UPDATE, and nothing of it is kept.
CREATE TABLE capped (id INTEGER PRIMARY KEY, a TEXT NOT NULL, v VECTOR(2) PARTITION_KEY (a) MAX_PARTITIONS 2);
INSERT INTO capped (id, a, v) VALUES (1, 'p', [1.0, 0.0]), (2, 'q', [0.0, 1.0]);
INSERT INTO capped (id, a, v) VALUES (3, 'r', [1.0, 0.0]);
----
error: VectorPartitionLimitExceeded

SELECT id, a FROM capped ORDER BY id;
----
id | a
1 | p
2 | q

# The next row under key values that already hold a vector is taken.
INSERT INTO capped (id, a, v) VALUES (3, 'q', [1.0, 0.0]);
----
rows affected: 1

UPDATE capped SET a = 'r' WHERE id = 3;
----
error: VectorPartitionLimitExceeded

# A row with no vector starts no partition; giving it a vector would.
INSERT INTO capped (id, a, v) VALUES (4, 'r', NULL);
----
rows affected: 1

UPDATE capped SET v = [1.0, 0.0] WHERE id = 4;
----
error: VectorPartitionLimitExceeded

SELECT id, a, v FROM capped ORDER BY id;
----
id | a | v
1 | p | [1.0, 0.0]
2 | q | [0.0, 1.0]
3 | q | [1.0, 0.0]
4 | r | NULL

# Deleting the rows of a partition frees its place, and the third set of key values is taken.
DELETE FROM capped WHERE a = 'p';
----
rows affected: 1

INSERT INTO capped (id, a, v) VALUES (5, 'r', [0.0, 1.0]);
----
rows affected: 1

UPDATE capped SET v = [1.0, 0.0] WHERE id = 4;
----
rows affected: 1

SELECT id, v <=> [1.0, 0.0] AS distance FROM capped WHERE a = 'r' ORDER BY distance LIMIT 5;
----
id | distance
4 | 0.0
5 | 1.0

# At the limit, one UPDATE that empties a partition and starts another is taken.
UPDATE capped SET a = 's' WHERE a = 'q';
----
rows affected: 2

SELECT id FROM capped WHERE a = 's' ORDER BY v <=> [1.0, 0.0] LIMIT 5;
----
id
3
2

# TEXT key values that differ only in the case of a letter are two partitions: with room for
# two, 'a' and 'A' fill it.
CREATE TABLE cased (id INTEGER PRIMARY KEY, tag TEXT NOT NULL, v VECTOR(2) PARTITION_KEY (tag) MAX_PARTITIONS 2);
INSERT INTO cased (id, tag, v) VALUES (1, 'a', [1.0, 0.0]), (2, 'A', [1.0, 0.0]);
INSERT INTO cased (id, tag, v) VALUES (3, 'b', [1.0, 0.0]);
----
error: VectorPartitionLimitExceeded

SELECT id FROM cased WHERE tag = 'A' ORDER BY v <=> [1.0, 0.0] LIMIT 5;
----
id
2

SELECT id FROM cased ORDER BY v <=> [1.0, 0.0] LIMIT 5;
----
id
1
2
