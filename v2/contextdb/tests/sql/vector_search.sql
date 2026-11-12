# A similarity search: ORDER BY the cosine distance (`<=>`) from a VECTOR column to a vector, with
# a LIMIT. The nearest rows come first, and rows at one distance come in primary-key order.
# From [1, 0, 0, 0]: rows 1 and 2 point the same way (distance 0), row 5 lies between, rows 3 and
# 7 are at a right angle (1), row 8 is all zeros (1, by rule), row 4 points the opposite way (2)
# and row 6 has no vector. The rows are written out of key order.

CREATE TABLE points (id INTEGER PRIMARY KEY, tag TEXT, v VECTOR(4));
INSERT INTO points (id, tag, v)
VALUES (7, 'blue', '[0, 0, 3, 0]'),
       (4, 'blue', [-1.0, 0.0, 0.0, 0.0]),
       (2, 'red', [2.0, 0.0, 0.0, 0.0]),
       (8, 'red', [0.0, 0.0, 0.0, 0.0]),
       (5, 'red', [0.1, 0.2, 0.0, 0.0]),
       (1, 'red', [1.0, 0.0, 0.0, 0.0]),
       (6, 'blue', NULL),
       (3, 'blue', [0.0, 1.0, 0.0, 0.0]);

# A vector prints as its numbers in brackets, whichever way it was written, and no vector as NULL.
SELECT id, v FROM points WHERE id >= 5 ORDER BY id;
----
id | v
5 | [0.1, 0.2, 0.0, 0.0]
6 | NULL
7 | [0.0, 0.0, 3.0, 0.0]
8 | [0.0, 0.0, 0.0, 0.0]

# The vector searched from is a literal or its quoted text.
SELECT id FROM points ORDER BY v <=> [1.0, 0.0, 0.0, 0.0] LIMIT 3;
----
id
1
2
5

SELECT id FROM points ORDER BY v <=> '[1,0,0,0]' LIMIT 3;
----
id
1
2
5

# The distance is the same expression in the select list, under the name the statement gives it:
# a REAL, lower for a nearer row. ORDER BY takes that name or the expression, with ASC or without.
SELECT id, v <=> '[1,0,0,0]' AS distance FROM points ORDER BY distance LIMIT 7;
----
id | distance
1 | 0.0
2 | 0.0
5 | *
3 | 1.0
7 | 1.0
8 | 1.0
4 | 2.0

SELECT id, v <=> '[1,0,0,0]' AS distance FROM points ORDER BY v <=> '[1,0,0,0]' LIMIT 4;
----
id | distance
1 | 0.0
2 | 0.0
5 | *
3 | 1.0

SELECT id, v <=> '[1,0,0,0]' AS distance FROM points ORDER BY v <=> '[1,0,0,0]' ASC LIMIT 4;
----
id | distance
1 | 0.0
2 | 0.0
5 | *
3 | 1.0

# One minus the distance is the similarity, worked out like any other arithmetic.
SELECT id, 1 - (v <=> '[1,0,0,0]') AS similarity FROM points ORDER BY v <=> '[1,0,0,0]' LIMIT 7;
----
id | similarity
1 | 1.0
2 | 1.0
5 | *
3 | 0.0
7 | 0.0
8 | 0.0
4 | -1.0

# A distance with no name is called what any unnamed expression is called.
SELECT id, v <=> '[1,0,0,0]' FROM points ORDER BY v <=> '[1,0,0,0]' LIMIT 2;
----
id | expr
1 | 0.0
2 | 0.0

# A search returns the columns the statement asks for and nothing more: no distance from
# SELECT * and none from a list that does not write one.
SELECT * FROM points ORDER BY v <=> '[1,0,0,0]' LIMIT 2;
----
id | tag | v
1 | red | [1.0, 0.0, 0.0, 0.0]
2 | red | [2.0, 0.0, 0.0, 0.0]

SELECT id, tag FROM points ORDER BY v <=> '[1,0,0,0]' LIMIT 2;
----
id | tag
1 | red
2 | red

# Every row with a vector is at distance 1 from this one, so the answer is in key order. The
# distance from or to a vector of all zeros is 1 as well.
SELECT id, v <=> [0.0, 0.0, 0.0, 1.0] AS distance FROM points ORDER BY distance LIMIT 4;
----
id | distance
1 | 1.0
2 | 1.0
3 | 1.0
4 | 1.0

SELECT id, v <=> [0.0, 0.0, 0.0, 0.0] AS distance FROM points ORDER BY distance LIMIT 2;
----
id | distance
1 | 1.0
2 | 1.0

# A row with no vector is never returned, so a LIMIT above the table gives the seven rows that
# have one. LIMIT 0 gives none.
SELECT id FROM points ORDER BY v <=> '[1,0,0,0]' LIMIT 100;
----
id
1
2
5
3
7
8
4

SELECT id FROM points ORDER BY v <=> '[1,0,0,0]' LIMIT 0;
----
id

# WHERE is applied first: the nearest of the rows it passes, not the rows it passes among the
# nearest. The two nearest rows of the table are red.
SELECT id, v <=> '[1,0,0,0]' AS distance FROM points WHERE tag = 'blue' ORDER BY distance LIMIT 2;
----
id | distance
3 | 1.0
7 | 1.0

SELECT id, v <=> '[1,0,0,0]' AS distance FROM points WHERE tag = 'blue' AND id > 3 ORDER BY distance LIMIT 5;
----
id | distance
7 | 1.0
4 | 2.0

SELECT id FROM points WHERE tag = 'green' ORDER BY v <=> '[1,0,0,0]' LIMIT 5;
----
id

# Two VECTOR columns of one table, each of its own dimension, are searched each by its own name.
# Row 4 has a vector in one of them only.
CREATE TABLE media (id INTEGER PRIMARY KEY, vector_text VECTOR(2), vector_vision VECTOR(3));
INSERT INTO media (id, vector_text, vector_vision)
VALUES (1, [1.0, 0.0], [0.0, 0.0, 1.0]),
       (2, [0.0, 1.0], [0.0, 1.0, 0.0]),
       (3, [-1.0, 0.0], [1.0, 0.0, 0.0]),
       (4, NULL, [2.0, 0.0, 0.0]);

SELECT id, vector_text <=> [1.0, 0.0] AS distance FROM media ORDER BY distance LIMIT 4;
----
id | distance
1 | 0.0
2 | 1.0
3 | 2.0

SELECT id, vector_vision <=> [1.0, 0.0, 0.0] AS distance FROM media ORDER BY distance LIMIT 4;
----
id | distance
3 | 0.0
4 | 0.0
1 | 1.0
2 | 1.0

# A table may leave the rows in some states out of every search: `PROPAGATE ON STATE x EXCLUDE
# VECTOR`. Such a row stays a row, and a plain SELECT still returns it; a similarity search does
# not. A row moved into an excluded state by an UPDATE, or by propagation from the row it
# references, is out of the next search, EXACT or INDEXED, and out of what SHOW counts; moved
# back to a state the table does not exclude, it is found again. From [1, 0] the order is 1, 2,
# 5, 3, 4: the rows left out are the nearest, so a search that cut its list before leaving
# them out would come back short.
CREATE TABLE sources (id INTEGER PRIMARY KEY, status TEXT NOT NULL)
STATE MACHINE (status: active -> [archived]);
CREATE TABLE claims (
  id INTEGER PRIMARY KEY,
  status TEXT NOT NULL,
  source_id INTEGER REFERENCES sources(id) ON STATE archived PROPAGATE SET superseded,
  v VECTOR(2)
) STATE MACHINE (status: active -> [invalidated, superseded], invalidated -> [active], superseded -> [active])
  PROPAGATE ON STATE invalidated EXCLUDE VECTOR
  PROPAGATE ON STATE superseded EXCLUDE VECTOR;
INSERT INTO sources (id, status) VALUES (1, 'active'), (2, 'active');
INSERT INTO claims (id, status, source_id, v)
VALUES (3, 'active', 2, [0.0, 1.0]),
       (1, 'active', 1, [1.0, 0.0]),
       (4, 'active', 2, [-1.0, 0.0]),
       (2, 'active', 1, [0.9, 0.1]),
       (5, 'active', 2, [0.5, 0.5]);

SELECT id FROM claims ORDER BY v <=> [1.0, 0.0] LIMIT 10;
----
id
1
2
5
3
4

# Row 3 by an UPDATE; rows 1 and 2 by propagation, as source 1 is archived.
UPDATE claims SET status = 'invalidated' WHERE id = 3;
----
rows affected: 1

UPDATE sources SET status = 'archived' WHERE id = 1;
----
rows affected: 1

SELECT id FROM claims ORDER BY v <=> [1.0, 0.0] LIMIT 10;
----
id
5
4

SELECT id FROM claims ORDER BY v <=> [1.0, 0.0] USE VECTOR INDEXED LIMIT 2;
----
id
5
4

SELECT id, status FROM claims ORDER BY id;
----
id | status
1 | superseded
2 | superseded
3 | invalidated
4 | active
5 | active

SHOW VECTOR_PARTITIONS FOR claims.v;
----
table | column | partition_key | live_rows | pending_inserts | tombstones | durable_vector_bytes | durable_index_bytes | indexed_available | availability_reason | maintenance_state | maintenance_reason | maintenance_vectors_total | maintenance_vectors_done | maintenance_vectors_remaining | lists | probes | recovery_action
claims | v | {} | 2 | 2 | 0 | 0 | 0 | true | none | idle | none | 0 | 0 | 0 | NULL | NULL | none

UPDATE claims SET status = 'active' WHERE id IN (1, 2, 3);
----
rows affected: 3

SELECT id FROM claims ORDER BY v <=> [1.0, 0.0] LIMIT 10;
----
id
1
2
5
3
4
