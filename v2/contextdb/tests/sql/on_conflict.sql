# INSERT ... ON CONFLICT (columns) DO UPDATE SET ...: a row whose key or unique values are free
# is inserted as written; a row that meets one already there changes that row by the SET list
# and inserts nothing. Either way the statement counts the row once.

CREATE TABLE items (
  id INTEGER PRIMARY KEY,
  code TEXT UNIQUE,
  name TEXT NOT NULL,
  qty INTEGER,
  shelf TEXT,
  slot INTEGER,
  UNIQUE (shelf, slot)
);

# A free key: the row goes in as written and the SET list is not used.
INSERT INTO items (id, code, name, qty, shelf, slot) VALUES (1, 'k1', 'bolt', 5, 'a', 1)
ON CONFLICT (id) DO UPDATE SET name = 'changed';
----
rows affected: 1

SELECT id, code, name, qty, shelf, slot FROM items ORDER BY id;
----
id | code | name | qty | shelf | slot
1 | k1 | bolt | 5 | a | 1

# A taken key: only the columns of the SET list change, and nothing else of the new row is kept.
INSERT INTO items (id, code, name, qty, shelf, slot) VALUES (1, 'k9', 'nut', 99, 'z', 9)
ON CONFLICT (id) DO UPDATE SET name = 'bolt m4';
----
rows affected: 1

SELECT id, code, name, qty, shelf, slot FROM items ORDER BY id;
----
id | code | name | qty | shelf | slot
1 | k1 | bolt m4 | 5 | a | 1

# A column in the SET list reads the row that is there.
INSERT INTO items (id, code, name, qty) VALUES (1, 'k1', 'bolt', 0)
ON CONFLICT (id) DO UPDATE SET qty = qty + 10, name = 'bolt m5';
----
rows affected: 1

SELECT id, name, qty FROM items ORDER BY id;
----
id | name | qty
1 | bolt m5 | 15

# By a UNIQUE column: the row with that code is changed, whatever key the new row carried.
INSERT INTO items (id, code, name, qty) VALUES (2, 'k1', 'other', 0)
ON CONFLICT (code) DO UPDATE SET qty = 20;
----
rows affected: 1

# By a UNIQUE rule of two columns.
INSERT INTO items (id, code, name, qty, shelf, slot) VALUES (3, 'k3', 'washer', 0, 'a', 1)
ON CONFLICT (shelf, slot) DO UPDATE SET qty = 30;
----
rows affected: 1

SELECT id, code, name, qty, shelf, slot FROM items ORDER BY id;
----
id | code | name | qty | shelf | slot
1 | k1 | bolt m5 | 30 | a | 1

# Several rows in one statement: each is inserted or changes the row it meets.
INSERT INTO items (id, code, name, qty, shelf, slot)
VALUES (2, 'k2', 'nut', 1, 'a', 2), (1, 'k7', 'gone', 7, 'b', 7), (3, 'k3', 'washer', 3, 'b', 1)
ON CONFLICT (id) DO UPDATE SET qty = 0;
----
rows affected: 3

SELECT id, code, name, qty, shelf, slot FROM items ORDER BY id;
----
id | code | name | qty | shelf | slot
1 | k1 | bolt m5 | 0 | a | 1
2 | k2 | nut | 1 | a | 2
3 | k3 | washer | 3 | b | 1

# The clause answers a meeting on the columns it names. A row that is free there and meets
# another rule is refused as any insert is.
INSERT INTO items (id, code, name) VALUES (4, 'k2', 'pin')
ON CONFLICT (id) DO UPDATE SET name = 'pin';
----
error: UniqueViolation

# The change it makes is held to the table's rules as an UPDATE is: NOT NULL, and the other
# UNIQUE rules.
INSERT INTO items (id, code, name) VALUES (2, 'k2', 'nut')
ON CONFLICT (id) DO UPDATE SET name = NULL;
----
error: ColumnNotNullable

INSERT INTO items (id, code, name) VALUES (2, 'k2', 'nut')
ON CONFLICT (id) DO UPDATE SET code = 'k3';
----
error: UniqueViolation

SELECT id, code, name, qty, shelf, slot FROM items ORDER BY id;
----
id | code | name | qty | shelf | slot
1 | k1 | bolt m5 | 0 | a | 1
2 | k2 | nut | 1 | a | 2
3 | k3 | washer | 3 | b | 1

# A NULL in a UNIQUE column meets nothing, so the row is inserted each time.
INSERT INTO items (id, code, name) VALUES (5, NULL, 'loose')
ON CONFLICT (code) DO UPDATE SET name = 'met';
INSERT INTO items (id, code, name) VALUES (6, NULL, 'loose')
ON CONFLICT (code) DO UPDATE SET name = 'met';
SELECT id, name FROM items WHERE id >= 5 ORDER BY id;
----
id | name
5 | loose
6 | loose

# A key of two columns.
CREATE TABLE windows (
  machine TEXT NOT NULL,
  slot INTEGER NOT NULL,
  value INTEGER NOT NULL,
  PRIMARY KEY (machine, slot)
);
INSERT INTO windows (machine, slot, value) VALUES ('m1', 1, 10), ('m1', 2, 20);
INSERT INTO windows (machine, slot, value) VALUES ('m1', 2, 99), ('m2', 2, 22)
ON CONFLICT (machine, slot) DO UPDATE SET value = value + 1;
----
rows affected: 2

SELECT machine, slot, value FROM windows ORDER BY machine, slot;
----
machine | slot | value
m1 | 1 | 10
m1 | 2 | 21
m2 | 2 | 22

# Without the clause, a taken key is refused.
INSERT INTO windows (machine, slot, value) VALUES ('m1', 2, 99);
----
error: UniqueViolation

# A column named twice in the SET list is refused as well.
INSERT INTO items (id, code, name, qty) VALUES (1, 'k1', 'bolt', 0)
ON CONFLICT (id) DO UPDATE SET qty = 1, qty = 2;
----
error: PlanError

# The columns must be the key or a whole UNIQUE rule: an index is not a rule, a part of a rule
# is not the rule, and neither is a rule with a column more. The refusal comes when the
# statement is planned, though the row to insert meets nothing.
CREATE TABLE stock (id INTEGER PRIMARY KEY, code TEXT UNIQUE, name TEXT NOT NULL, shelf TEXT, slot INTEGER, UNIQUE (shelf, slot));
CREATE INDEX stock_name ON stock (name);
INSERT INTO stock (id, code, name, shelf, slot) VALUES (1, 'k1', 'bolt', 'a', 1);
INSERT INTO stock (id, code, name, shelf, slot) VALUES (2, 'k2', 'nut', 'b', 2) ON CONFLICT (name) DO UPDATE SET name = 'changed';
----
error: PlanError

INSERT INTO stock (id, code, name, shelf, slot) VALUES (2, 'k2', 'nut', 'b', 2) ON CONFLICT (shelf) DO UPDATE SET name = 'changed';
----
error: PlanError

INSERT INTO stock (id, code, name, shelf, slot) VALUES (2, 'k2', 'nut', 'b', 2) ON CONFLICT (slot) DO UPDATE SET name = 'changed';
----
error: PlanError

INSERT INTO stock (id, code, name, shelf, slot) VALUES (2, 'k2', 'nut', 'b', 2) ON CONFLICT (id, name) DO UPDATE SET name = 'changed';
----
error: PlanError

INSERT INTO stock (id, code, name, shelf, slot) VALUES (2, 'k2', 'nut', 'b', 2) ON CONFLICT (shelf, slot, name) DO UPDATE SET name = 'changed';
----
error: PlanError

SELECT id, name FROM stock ORDER BY id;
----
id | name
1 | bolt

# The key and each UNIQUE rule are accepted.
INSERT INTO stock (id, code, name, shelf, slot) VALUES (2, 'k2', 'nut', 'b', 2) ON CONFLICT (id) DO UPDATE SET name = 'changed';
----
rows affected: 1

INSERT INTO stock (id, code, name, shelf, slot) VALUES (3, 'k3', 'nut', 'b', 3) ON CONFLICT (code) DO UPDATE SET name = 'changed';
----
rows affected: 1

INSERT INTO stock (id, code, name, shelf, slot) VALUES (4, 'k4', 'nut', 'b', 4) ON CONFLICT (shelf, slot) DO UPDATE SET name = 'changed';
----
rows affected: 1

SELECT COUNT(*) AS n FROM stock WHERE name = 'changed';
----
n
0

SELECT COUNT(*) AS n FROM stock;
----
n
4
