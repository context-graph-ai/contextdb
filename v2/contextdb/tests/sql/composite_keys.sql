# A primary key of several columns: the whole tuple names the row. And the other things a key
# may be declared with: a key written as an element of the table over one column, the types a
# partition key takes, the TXID type, and the index names the engine keeps for itself.

CREATE TABLE metric_windows (
  machine_id TEXT NOT NULL,
  sensor_id TEXT NOT NULL,
  metric TEXT NOT NULL,
  window_start INTEGER NOT NULL,
  value INTEGER NOT NULL,
  PRIMARY KEY (machine_id, sensor_id, metric, window_start)
);
INSERT INTO metric_windows (machine_id, sensor_id, metric, window_start, value) VALUES
  ('m1', 's1', 'temp', 10, 1),
  ('m1', 's1', 'temp', 20, 2),
  ('m1', 's2', 'temp', 10, 3),
  ('m2', 's1', 'temp', 10, 4);

# The same tuple again is the same row and is refused; a tuple that differs in one column,
# the last one too, is another row.
INSERT INTO metric_windows (machine_id, sensor_id, metric, window_start, value) VALUES ('m1', 's1', 'temp', 10, 9);
----
error: UniqueViolation

INSERT INTO metric_windows (machine_id, sensor_id, metric, window_start, value) VALUES ('m1', 's1', 'temp', 30, 5);
INSERT INTO metric_windows (machine_id, sensor_id, metric, window_start, value) VALUES ('m1', 's1', 'load', 10, 6);
SELECT COUNT(*) AS n FROM metric_windows;
----
n
6

# Every column of the key is a column of every row.
INSERT INTO metric_windows (machine_id, sensor_id, metric, value) VALUES ('m1', 's1', 'temp', 7);
----
error: ColumnNotNullable

# The whole key names one row; its leading columns name the rows that share them, in key order.
SELECT value FROM metric_windows WHERE machine_id = 'm1' AND sensor_id = 's1' AND metric = 'temp' AND window_start = 20;
----
value
2

SELECT sensor_id, metric, window_start, value FROM metric_windows WHERE machine_id = 'm1' ORDER BY sensor_id, metric, window_start;
----
sensor_id | metric | window_start | value
s1 | load | 10 | 6
s1 | temp | 10 | 1
s1 | temp | 20 | 2
s1 | temp | 30 | 5
s2 | temp | 10 | 3

SELECT machine_id, value FROM metric_windows WHERE sensor_id = 's1' AND window_start = 10 ORDER BY machine_id, value;
----
machine_id | value
m1 | 1
m1 | 6
m2 | 4

# An UPDATE and a DELETE reach a row by its whole key, and change that row alone.
UPDATE metric_windows SET value = 20 WHERE machine_id = 'm1' AND sensor_id = 's1' AND metric = 'temp' AND window_start = 20;
----
rows affected: 1

DELETE FROM metric_windows WHERE machine_id = 'm1' AND sensor_id = 's1' AND metric = 'load' AND window_start = 10;
----
rows affected: 1

# A column of the key may be assigned: the row moves to the new tuple, unless a row is there.
UPDATE metric_windows SET window_start = 20 WHERE machine_id = 'm1' AND sensor_id = 's1' AND metric = 'temp' AND window_start = 10;
----
error: UniqueViolation

UPDATE metric_windows SET window_start = 40 WHERE machine_id = 'm1' AND sensor_id = 's1' AND metric = 'temp' AND window_start = 10;
----
rows affected: 1

SELECT machine_id, sensor_id, metric, window_start, value FROM metric_windows ORDER BY machine_id, sensor_id, metric, window_start;
----
machine_id | sensor_id | metric | window_start | value
m1 | s1 | temp | 20 | 20
m1 | s1 | temp | 30 | 5
m1 | s1 | temp | 40 | 1
m1 | s2 | temp | 10 | 3
m2 | s1 | temp | 10 | 4

# Rows of one stream after a position, in order, and never a row of the stream beside it.
CREATE TABLE stream_events (stream_id TEXT NOT NULL, ordinal INTEGER NOT NULL, body TEXT, PRIMARY KEY (stream_id, ordinal));
INSERT INTO stream_events (stream_id, ordinal, body) VALUES
  ('a', 1, 'a1'), ('a', 2, 'a2'), ('a', 3, 'a3'), ('a', 4, 'a4'),
  ('b', 1, 'b1'), ('b', 2, 'b2'), ('b', 3, 'b3'), ('b', 4, 'b4'), ('b', 5, 'b5'),
  ('c', 3, 'c3'), ('c', 4, 'c4');
SELECT ordinal, body FROM stream_events WHERE stream_id = 'b' AND ordinal > 2 ORDER BY ordinal ASC LIMIT 2;
----
ordinal | body
3 | b3
4 | b4

SELECT ordinal, body FROM stream_events WHERE stream_id = 'b' AND ordinal > 4 ORDER BY ordinal ASC LIMIT 2;
----
ordinal | body
5 | b5

SELECT ordinal, body FROM stream_events WHERE stream_id = 'b' AND ordinal > 5 ORDER BY ordinal ASC LIMIT 2;
----
ordinal | body

# A write that meets its own key changes that row and adds none.
INSERT INTO stream_events (stream_id, ordinal, body) VALUES ('b', 5, 'again')
ON CONFLICT (stream_id, ordinal) DO UPDATE SET body = 'b5 again';
SELECT COUNT(*) AS n FROM stream_events WHERE stream_id = 'b';
----
n
5

SELECT body FROM stream_events WHERE stream_id = 'b' AND ordinal = 5;
----
body
b5 again

# A key written as an element of the table may name one column.
CREATE TABLE one_key (a INTEGER, b TEXT, PRIMARY KEY (a));
INSERT INTO one_key (a, b) VALUES (1, 'x');
INSERT INTO one_key (a, b) VALUES (1, 'y');
----
error: UniqueViolation

SELECT a, b FROM one_key WHERE a = 1;
----
a | b
1 | x

# A key names columns the table has, each once, of a type whose values are equal or not and
# nothing between.
CREATE TABLE refused (a INTEGER, b TEXT, PRIMARY KEY (a, nope));
----
error: ParseError

CREATE TABLE refused (a INTEGER, b TEXT, PRIMARY KEY (a, a));
----
error: ParseError

CREATE TABLE refused (a INTEGER, r REAL, PRIMARY KEY (a, r));
----
error: ColumnNotIndexable

CREATE TABLE refused (a INTEGER, doc JSON, PRIMARY KEY (a, doc));
----
error: ColumnNotIndexable

CREATE TABLE refused (a INTEGER, v VECTOR(2), PRIMARY KEY (a, v));
----
error: ColumnNotIndexable

SELECT a FROM refused;
----
error: TableNotFound

# A table with a key of several columns takes UNIQUE rules, indexes and references like any
# other, and a reference may name the whole key of its parent.
CREATE TABLE readings (
  id INTEGER PRIMARY KEY,
  machine_id TEXT,
  sensor_id TEXT,
  metric TEXT,
  window_start INTEGER,
  note TEXT,
  UNIQUE (machine_id, note),
  FOREIGN KEY (machine_id, sensor_id, metric, window_start)
    REFERENCES metric_windows(machine_id, sensor_id, metric, window_start)
);
CREATE INDEX windows_by_value ON metric_windows (value DESC, machine_id);
SELECT machine_id, value FROM metric_windows WHERE value = 20;
----
machine_id | value
m1 | 20

# The names the engine gives the indexes it makes are kept for it.
CREATE INDEX __fk_readings ON readings (machine_id);
----
error: ReservedIndexName

CREATE INDEX __pk_machine_id ON metric_windows (machine_id);
----
error: ReservedIndexName

CREATE INDEX __unique_note ON readings (note);
----
error: ReservedIndexName

# A partition key takes a column of any type whose values are equal or not and nothing
# between: UUID, TEXT, INTEGER, BOOLEAN, TIMESTAMP and TXID.
CREATE TABLE partitioned (
  id INTEGER PRIMARY KEY,
  u UUID NOT NULL,
  t TEXT NOT NULL,
  i INTEGER NOT NULL,
  b BOOLEAN NOT NULL,
  at TIMESTAMP NOT NULL,
  v VECTOR(2) PARTITION_KEY (u, t, i, b, at) MAX_PARTITIONS 2
);
INSERT INTO partitioned (id, u, t, i, b, at, v) VALUES
  (1, '00000000-0000-0000-0000-000000000001', 'x', 1, TRUE, '2026-01-01T00:00:00Z', [1.0, 0.0]),
  (2, '00000000-0000-0000-0000-000000000001', 'x', 1, TRUE, '2026-01-01T00:00:00Z', [0.0, 1.0]),
  (3, '00000000-0000-0000-0000-000000000001', 'x', 1, FALSE, '2026-01-01T00:00:00Z', [1.0, 0.0]);
INSERT INTO partitioned (id, u, t, i, b, at, v) VALUES
  (4, '00000000-0000-0000-0000-000000000001', 'x', 2, TRUE, '2026-01-01T00:00:00Z', [1.0, 0.0]);
----
error: VectorPartitionLimitExceeded

SELECT id FROM partitioned WHERE b = TRUE ORDER BY v <=> [0.0, 1.0] LIMIT 5;
----
id
2
1

CREATE TABLE partitioned_by_commit (id INTEGER PRIMARY KEY, made_at TXID NOT NULL, v VECTOR(2) PARTITION_KEY (made_at));

# A TXID is given by the engine and bound by the program that calls the library: no SQL text
# writes one.
CREATE TABLE stamped (id INTEGER PRIMARY KEY, made_at TXID);
INSERT INTO stamped (id, made_at) VALUES (1, 5);
----
error: ColumnTypeMismatch

INSERT INTO stamped (id, made_at) VALUES (1, '5');
----
error: ColumnTypeMismatch

INSERT INTO stamped (id) VALUES (1);
SELECT id, made_at FROM stamped;
----
id | made_at
1 | NULL
