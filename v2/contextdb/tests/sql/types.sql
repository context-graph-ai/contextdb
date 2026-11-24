# Each column type takes its literal and gives the same value back, under either spelling of its
# name. The runner prints a REAL with its decimal point, a TIMESTAMP as Timestamp(n) and JSON in
# its compact form, so a value that was kept as text or as a whole number would show here.

CREATE TABLE samples (
  id INTEGER PRIMARY KEY,
  whole INT,
  ratio REAL,
  weight FLOAT,
  words TEXT,
  done BOOLEAN,
  seen BOOL,
  ref UUID,
  at TIMESTAMP,
  doc JSON
);

INSERT INTO samples (id, whole, ratio, weight, words, done, seen, ref, at, doc)
VALUES (1, 9223372036854775807, 3.25, -0.5, 'héllo wörld ✓ 日本語', TRUE, FALSE,
        '550e8400-e29b-41d4-a716-446655440000', '1970-01-01T00:00:00Z',
        '{"key": "value", "list": [1, 2.5, null, true]}');
INSERT INTO samples (id, whole, ratio, words, done, seen, ref, doc)
VALUES (2, -9223372036854775807, 1234567.891, 'it''s', FALSE, TRUE,
        '550E8400-E29B-41D4-A716-446655440000', '[1, "two"]');
INSERT INTO samples (id) VALUES (3);

# INTEGER is 64 bits wide and exact; REAL is 64 bits wide.
SELECT id, whole, ratio, weight FROM samples ORDER BY id;
----
id | whole | ratio | weight  (read 3, compared 0)
1 | 9223372036854775807 | 3.25 | -0.5
2 | -9223372036854775807 | 1234567.891 | NULL
3 | NULL | NULL | NULL

SELECT whole - 1 AS below FROM samples WHERE id = 1;
----
below  (read 1, compared 0)
9223372036854775806

# TEXT is UTF-8 and keeps a doubled quote as one; BOOLEAN takes TRUE and FALSE.
SELECT id, words, done, seen FROM samples ORDER BY id;
----
id | words | done | seen  (read 3, compared 0)
1 | héllo wörld ✓ 日本語 | true | false
2 | it's | false | true
3 | NULL | NULL | NULL

# UUID takes quoted text in either case and is one value whichever way it was written.
SELECT id, ref FROM samples ORDER BY id;
----
id | ref  (read 3, compared 0)
1 | 550e8400-e29b-41d4-a716-446655440000
2 | 550e8400-e29b-41d4-a716-446655440000
3 | NULL

SELECT id FROM samples WHERE ref = '550e8400-e29b-41d4-a716-446655440000' ORDER BY id;
----
id  (read 3, compared 0)
1
2

# JSON takes quoted text and stores the JSON value, not the text it was written as.
SELECT id, doc FROM samples ORDER BY id;
----
id | doc  (read 3, compared 0)
1 | {"key":"value","list":[1,2.5,null,true]}
2 | [1,"two"]
3 | NULL

# TIMESTAMP takes ISO 8601 text and stores the milliseconds since the first moment of 1970. One
# moment written with two offsets is one value, and a later moment is a greater one.
SELECT at FROM samples WHERE id = 1;
----
at  (read 1, compared 0)
Timestamp(0)

INSERT INTO samples (id, at) VALUES (4, '2026-01-01T00:00:00Z');
INSERT INTO samples (id, at) VALUES (5, '2026-01-01T02:00:00+02:00');
INSERT INTO samples (id, at) VALUES (6, '2026-01-01T00:00:01Z');
SELECT id FROM samples WHERE at IS NOT NULL ORDER BY at DESC, id;
----
id  (read 6, compared 0)
6
4
5
1

INSERT INTO samples (id, at) VALUES (7, '1970-01-01T00:00:01Z');
SELECT at FROM samples WHERE id = 7;
----
at  (read 1, compared 0)
Timestamp(1000)

# A value that is not of its column's type is refused.
INSERT INTO samples (id, doc) VALUES (8, '{"key": not json');
----
error: ColumnTypeMismatch

INSERT INTO samples (id, whole) VALUES (8, 'x');
----
error: ColumnTypeMismatch

CREATE TABLE vecs (id INTEGER PRIMARY KEY, v VECTOR(3));
INSERT INTO vecs (id, v) VALUES (1, 7);
----
error: ColumnTypeMismatch | column type mismatch: vecs.v expected VECTOR(3), got Int64

INSERT INTO vecs (id, v) VALUES (2, [1.0, 2.0]);
----
error: ColumnTypeMismatch

SELECT COUNT(*) FROM vecs;
----
count  (read 0, compared 0)
0

# Text compared with a UUID, TIMESTAMP or VECTOR column is read as one, or refused; never NULL.
SELECT id FROM samples WHERE ref = 'not-a-uuid';
----
error: ColumnTypeMismatch | column type mismatch: ref expected UUID, got Text

SELECT id FROM samples WHERE at = 'not a moment';
----
error: ColumnTypeMismatch

SELECT NOW() = 'not-a-timestamp';
----
error: ColumnTypeMismatch | column type mismatch: a computed value expected TIMESTAMP, got Text

SELECT id FROM vecs WHERE v = 'not a vector';
----
error: ColumnTypeMismatch

# A TEXT column is not compared with a UUID or a TIMESTAMP column.
SELECT id FROM samples WHERE words = ref;
----
error: PlanError

SELECT id FROM samples WHERE words = at;
----
error: PlanError

# A number is read as Postgres reads a decimal constant, and a vector's component is the same rule.
SELECT 1e5 = 100000.0 AS a, 2.5e-3 = 0.0025 AS b, 1E+2 = 100.0 AS c, .5 = 0.5 AS d, 5. = 5.0 AS e;
----
a | b | c | d | e  (read 0, compared 0)
true | true | true | true | true

INSERT INTO vecs (id, v) VALUES (3, [1e-5, .5, 5.]);

INSERT INTO samples (id, at) VALUES (20, 'infinity'), (21, '-infinity');
SELECT at FROM samples WHERE id >= 20 ORDER BY id;
----
at  (read 2, compared 0)
Timestamp(9223372036854775807)
Timestamp(-9223372036854775808)
