# MAINTAIN COUNT: the lists of columns for which a table keeps the number of its rows under
# each set of values, so that COUNT(*) with those columns given is one stored number. A count
# is always the number of rows the same filter returns, kept or not.

CREATE TABLE events (
  id INTEGER PRIMARY KEY,
  ctx INTEGER NOT NULL,
  conn TEXT NOT NULL,
  note TEXT,
  UNIQUE (ctx, id),
  UNIQUE (ctx, conn, id)
) MAINTAIN COUNT (ctx), (ctx, conn);
INSERT INTO events (id, ctx, conn)
VALUES (1, 1, 'a'), (2, 1, 'a'), (3, 1, 'b'), (4, 2, 'a'), (5, 2, 'b'), (6, 2, 'b');

SELECT COUNT(*) AS n FROM events WHERE ctx = 1;
----
n
3

SELECT COUNT(*) AS n FROM events WHERE ctx = 2 AND conn = 'b';
----
n
2

# Values no row has are a count of nothing.
SELECT COUNT(*) AS n FROM events WHERE ctx = 3;
----
n
0

SELECT COUNT(*) AS n FROM events WHERE ctx = 1 AND conn = 'z';
----
n
0

# The counts follow an insert, an UPDATE that moves a row from one set of values to another,
# an UPDATE that touches no counted column, and a DELETE.
INSERT INTO events (id, ctx, conn) VALUES (7, 1, 'b');
UPDATE events SET ctx = 1 WHERE id = 4;
UPDATE events SET note = 'seen' WHERE ctx = 2;
DELETE FROM events WHERE id = 1;
SELECT COUNT(*) AS n FROM events WHERE ctx = 1;
----
n
4

SELECT COUNT(*) AS n FROM events WHERE ctx = 2;
----
n
2

SELECT COUNT(*) AS n FROM events WHERE ctx = 1 AND conn = 'a';
----
n
2

SELECT COUNT(*) AS n FROM events WHERE ctx = 1 AND conn = 'b';
----
n
2

SELECT COUNT(*) AS n FROM events WHERE ctx = 2 AND conn = 'a';
----
n
0

# A refused statement counts nothing: the second row of this one has a key that is taken.
INSERT INTO events (id, ctx, conn) VALUES (8, 2, 'a'), (2, 2, 'a');
----
error: UniqueViolation

SELECT COUNT(*) AS n FROM events WHERE ctx = 2 AND conn = 'a';
----
n
0

SELECT COUNT(*) AS n FROM events;
----
n
6

# A table that declares no count answers the same statements with the same numbers.
CREATE TABLE plain (id INTEGER PRIMARY KEY, ctx INTEGER NOT NULL, conn TEXT NOT NULL);
CREATE INDEX plain_ctx_conn ON plain (ctx, conn);
INSERT INTO plain (id, ctx, conn)
VALUES (2, 1, 'a'), (3, 1, 'b'), (4, 1, 'a'), (5, 2, 'b'), (6, 2, 'b'), (7, 1, 'b');
SELECT COUNT(*) AS n FROM plain WHERE ctx = 1;
----
n
4

SELECT COUNT(*) AS n FROM plain WHERE ctx = 1 AND conn = 'b';
----
n
2

SELECT COUNT(*) AS n FROM plain WHERE ctx = 3;
----
n
0

# ALTER TABLE ... SET MAINTAIN COUNT replaces the whole declared set, on a table that has rows:
# the counts it names are right at once.
ALTER TABLE plain SET MAINTAIN COUNT (ctx), (ctx, conn);
SELECT COUNT(*) AS n FROM plain WHERE ctx = 1;
----
n
4

SELECT COUNT(*) AS n FROM plain WHERE ctx = 2 AND conn = 'b';
----
n
2

INSERT INTO plain (id, ctx, conn) VALUES (8, 2, 'b');
DELETE FROM plain WHERE id = 2;
SELECT COUNT(*) AS n FROM plain WHERE ctx = 1;
----
n
3

SELECT COUNT(*) AS n FROM plain WHERE ctx = 2 AND conn = 'b';
----
n
3

# ALL PREFIXES keeps a count for every beginning of every index, NONE says in words what a
# table that declares nothing does, and DEFAULT takes the declaration away. Under each, the
# same statements give the same numbers.
ALTER TABLE plain SET MAINTAIN COUNT ALL PREFIXES;
INSERT INTO plain (id, ctx, conn) VALUES (9, 1, 'a');
SELECT COUNT(*) AS n FROM plain WHERE ctx = 1;
----
n
4

SELECT COUNT(*) AS n FROM plain WHERE ctx = 1 AND conn = 'a';
----
n
2

ALTER TABLE plain SET MAINTAIN COUNT NONE;
INSERT INTO plain (id, ctx, conn) VALUES (10, 1, 'a');
SELECT COUNT(*) AS n FROM plain WHERE ctx = 1 AND conn = 'a';
----
n
3

ALTER TABLE plain SET MAINTAIN COUNT (ctx);
ALTER TABLE plain SET MAINTAIN COUNT DEFAULT;
DELETE FROM plain WHERE id = 10;
SELECT COUNT(*) AS n FROM plain WHERE ctx = 1;
----
n
4

# CREATE TABLE takes ALL PREFIXES and NONE as well.
CREATE TABLE every (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, UNIQUE (a, b, id)) MAINTAIN COUNT ALL PREFIXES;
CREATE TABLE silent (id INTEGER PRIMARY KEY, a INTEGER, UNIQUE (a, id)) MAINTAIN COUNT NONE;
INSERT INTO every (id, a, b) VALUES (1, 1, 1), (2, 1, 1), (3, 1, 2);
SELECT COUNT(*) AS n FROM every WHERE a = 1 AND b = 1;
----
n
2

# A list is written with ASC or DESC on each column, as an index is, and is led by an index
# that runs the same way.
CREATE TABLE marks (id INTEGER PRIMARY KEY, day INTEGER NOT NULL, kind TEXT NOT NULL);
CREATE INDEX marks_day_kind ON marks (day DESC, kind ASC);
ALTER TABLE marks SET MAINTAIN COUNT (day DESC), (day DESC, kind ASC);
INSERT INTO marks (id, day, kind) VALUES (1, 5, 'x'), (2, 5, 'y'), (3, 6, 'x');
SELECT COUNT(*) AS n FROM marks WHERE day = 5;
----
n
2

ALTER TABLE marks SET MAINTAIN COUNT (day ASC);
----
error: SchemaInvalid

# What is refused when it is declared. A list no index begins with: nothing could read it.
CREATE TABLE no_index (id INTEGER PRIMARY KEY, a INTEGER) MAINTAIN COUNT (a);
----
error: SchemaInvalid

ALTER TABLE events SET MAINTAIN COUNT (conn);
----
error: SchemaInvalid

# A list a key or a UNIQUE rule already holds to one row at most.
CREATE TABLE by_key (id INTEGER PRIMARY KEY, a INTEGER) MAINTAIN COUNT (id);
----
error: SchemaInvalid

ALTER TABLE events SET MAINTAIN COUNT (ctx, id);
----
error: SchemaInvalid

# The same list twice, a column twice in one list, and a column the table does not have.
CREATE TABLE twice (id INTEGER PRIMARY KEY, a INTEGER, UNIQUE (a, id)) MAINTAIN COUNT (a), (a);
----
error: SchemaInvalid

CREATE TABLE repeated (id INTEGER PRIMARY KEY, a INTEGER, UNIQUE (a, id)) MAINTAIN COUNT (a, a);
----
error: SchemaInvalid

CREATE TABLE unknown (id INTEGER PRIMARY KEY, a INTEGER, UNIQUE (a, id)) MAINTAIN COUNT (nope);
----
error: ColumnNotFound

SELECT id FROM no_index;
----
error: TableNotFound

SELECT id FROM twice;
----
error: TableNotFound

# A refused ALTER left the table's counts as they were declared, and right.
INSERT INTO events (id, ctx, conn) VALUES (9, 2, 'a');
SELECT COUNT(*) AS n FROM events WHERE ctx = 2 AND conn = 'a';
----
n
1

SELECT COUNT(*) AS n FROM events WHERE ctx = 2;
----
n
3

# A column a kept count is declared by cannot be dropped, even with CASCADE, which drops the
# indexes that name it but not the rule that counts by it.
ALTER TABLE events DROP COLUMN conn CASCADE;
----
error: SchemaInvalid

SELECT COUNT(*) AS n FROM events WHERE ctx = 2 AND conn = 'a';
----
n
1
