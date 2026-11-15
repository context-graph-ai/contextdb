# INSERT stores one row or many. A declared default fills a column the statement leaves out,
# before the rules are checked; a value the statement gives is stored as given.

CREATE TABLE items (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  label TEXT NOT NULL DEFAULT 'unnamed',
  qty INTEGER DEFAULT 1,
  note TEXT DEFAULT NULL,
  ok BOOLEAN DEFAULT TRUE,
  score REAL DEFAULT 0.5
);

INSERT INTO items (id, name) VALUES (1, 'one');
----
rows affected: 1

INSERT INTO items (id, name, label, qty, note, ok, score)
VALUES (2, 'two', 'b', 20, 'second', FALSE, 2.5),
       (3, 'three', 'c', 30, NULL, TRUE, 3.5),
       (4, 'four', 'd', 40, 'fourth', FALSE, 4.5);
----
rows affected: 3

# A NULL the statement gives stays NULL: the default is for a column that was left out.
INSERT INTO items (id, name, qty) VALUES (5, 'five', NULL);
----
rows affected: 1

SELECT id, name, label, qty, note, ok, score FROM items ORDER BY id;
----
id | name | label | qty | note | ok | score
1 | one | unnamed | 1 | NULL | true | 0.5
2 | two | b | 20 | second | false | 2.5
3 | three | c | 30 | NULL | true | 3.5
4 | four | d | 40 | fourth | false | 4.5
5 | five | unnamed | NULL | NULL | true | 0.5

# The default comes first and the rule second: `label` is filled and passes, `name` has no
# default and is refused, and a NULL given for `label` is refused although it has a default.
INSERT INTO items (id) VALUES (6);
----
error: ColumnNotNullable

INSERT INTO items (id, name, label) VALUES (6, 'six', NULL);
----
error: ColumnNotNullable

# One refused row refuses every row of its statement.
INSERT INTO items (id, name) VALUES (6, 'six'), (1, 'one again');
----
error: UniqueViolation

SELECT id FROM items ORDER BY id;
----
id
1
2
3
4
5

# A column named twice as a target is refused, as Postgres refuses it, and nothing is written.
INSERT INTO items (id, name, name) VALUES (7, 'seven', 'again');
----
error: PlanError

SELECT COUNT(*) FROM items WHERE id = 7;
----
count
0

# A SELECT among the VALUES is worked out before the row is stored.
CREATE TABLE counted (id INTEGER PRIMARY KEY, seen INTEGER);
INSERT INTO counted (id, seen) VALUES (1, 0), (2, 0);
INSERT INTO counted (id, seen) VALUES (3, (SELECT COUNT(*) FROM counted));
----
rows affected: 1

SELECT seen FROM counted WHERE id = 3;
----
seen
2

# A comparison of unlike types with the value a SELECT gives is refused in VALUES, and nothing is stored.
CREATE TABLE words (id INTEGER PRIMARY KEY, label TEXT);
INSERT INTO words (id, label) VALUES (1, 'seven');
CREATE TABLE flags (id INTEGER PRIMARY KEY, ok BOOLEAN);
INSERT INTO flags (id, ok) VALUES (1, 10 = (SELECT label FROM words WHERE id = 1));
----
error: PlanError

INSERT INTO flags (id, ok) VALUES (2, 10 NOT IN (SELECT label FROM words WHERE id = 99));
----
error: PlanError

SELECT COUNT(*) FROM flags;
----
count
0
