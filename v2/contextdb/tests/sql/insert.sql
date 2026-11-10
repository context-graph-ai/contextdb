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
