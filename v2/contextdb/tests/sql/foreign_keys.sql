# REFERENCES and FOREIGN KEY: a row that names a parent row is only committed when that row is
# there, and a parent row is not removed or given another key while a row still names it. Each
# statement here is a transaction of its own, so each refusal comes from the statement.

CREATE TABLE projects (id INTEGER PRIMARY KEY, code TEXT UNIQUE, title TEXT);
CREATE TABLE tasks (
  id INTEGER PRIMARY KEY,
  project_id INTEGER REFERENCES projects(id),
  project_code TEXT REFERENCES projects(code),
  note TEXT
);
INSERT INTO projects (id, code, title) VALUES (1, 'alpha', 'first'), (2, 'beta', 'second');

# A row whose parents are there, by the key and by the unique column.
INSERT INTO tasks (id, project_id, project_code) VALUES (1, 1, 'alpha'), (2, 2, NULL);
----
rows affected: 2

# A reference that is NULL names no parent and is not checked.
INSERT INTO tasks (id, project_id, project_code) VALUES (3, NULL, NULL);
----
rows affected: 1

# No such parent, by either reference.
INSERT INTO tasks (id, project_id) VALUES (4, 404);
----
error: ForeignKeyViolation

INSERT INTO tasks (id, project_code) VALUES (4, 'gamma');
----
error: ForeignKeyViolation

# One row without its parent refuses the statement for every row.
INSERT INTO tasks (id, project_id) VALUES (5, 1), (6, 404);
----
error: ForeignKeyViolation

# A change that points a row at a parent that is not there, and one that takes the reference away.
UPDATE tasks SET project_id = 404 WHERE id = 1;
----
error: ForeignKeyViolation

UPDATE tasks SET project_code = NULL WHERE id = 1;
----
rows affected: 1

SELECT id, project_id, project_code FROM tasks ORDER BY id;
----
id | project_id | project_code
1 | 1 | NULL
2 | 2 | NULL
3 | NULL | NULL

# A parent that a row still names is not deleted and does not change its key; another column
# of it changes freely.
DELETE FROM projects WHERE id = 1;
----
error: ForeignKeyViolation

UPDATE projects SET id = 10 WHERE id = 1;
----
error: ForeignKeyViolation

UPDATE projects SET title = 'renamed' WHERE id = 1;
----
rows affected: 1

# No row names 'alpha' or 'beta' any more, so the unique column they were named by is free to
# change.
UPDATE projects SET code = 'alpha-2' WHERE id = 1;
----
rows affected: 1

# Once nothing names it, the parent goes.
DELETE FROM tasks WHERE id = 1;
DELETE FROM projects WHERE id = 1;
----
rows affected: 1

SELECT id, code, title FROM projects ORDER BY id;
----
id | code | title
2 | beta | second

# A reference of two columns to a parent whose key is those two columns in that order.
CREATE TABLE windows (
  machine TEXT NOT NULL,
  slot INTEGER NOT NULL,
  label TEXT,
  PRIMARY KEY (machine, slot)
);
CREATE TABLE samples (
  id INTEGER PRIMARY KEY,
  machine TEXT,
  slot INTEGER,
  FOREIGN KEY (machine, slot) REFERENCES windows(machine, slot)
);
INSERT INTO windows (machine, slot, label) VALUES ('m1', 1, 'a'), ('m1', 2, 'b'), ('m2', 1, 'c');
INSERT INTO samples (id, machine, slot) VALUES (1, 'm1', 1), (2, 'm2', 1);
----
rows affected: 2

# Each column has a parent of its own, and the pair has none.
INSERT INTO samples (id, machine, slot) VALUES (3, 'm2', 2);
----
error: ForeignKeyViolation

# A reference with a NULL in any of its columns is not whole and is not checked.
INSERT INTO samples (id, machine, slot) VALUES (4, 'm9', NULL), (5, NULL, 9);
----
rows affected: 2

DELETE FROM windows WHERE machine = 'm1' AND slot = 1;
----
error: ForeignKeyViolation

DELETE FROM windows WHERE machine = 'm1' AND slot = 2;
----
rows affected: 1

UPDATE windows SET slot = 7 WHERE machine = 'm2';
----
error: ForeignKeyViolation

SELECT machine, slot FROM windows ORDER BY machine, slot;
----
machine | slot
m1 | 1
m2 | 1

# A reference to two columns that a UNIQUE rule covers.
CREATE TABLE seats (id INTEGER PRIMARY KEY, room TEXT, seat INTEGER, UNIQUE (room, seat));
CREATE TABLE bookings (
  id INTEGER PRIMARY KEY,
  room TEXT,
  seat INTEGER,
  FOREIGN KEY (room, seat) REFERENCES seats(room, seat)
);
INSERT INTO seats (id, room, seat) VALUES (1, 'east', 1);
INSERT INTO bookings (id, room, seat) VALUES (1, 'east', 1);
INSERT INTO bookings (id, room, seat) VALUES (2, 'east', 2);
----
error: ForeignKeyViolation

# A reference must name what a key or a UNIQUE rule covers, in the rule's order, in a table that
# is there. Nothing is declared by a refused statement.
CREATE TABLE bad_column (id INTEGER PRIMARY KEY, title TEXT REFERENCES projects(title));
----
error: SchemaInvalid

CREATE TABLE bad_order (
  id INTEGER PRIMARY KEY, machine TEXT, slot INTEGER,
  FOREIGN KEY (slot, machine) REFERENCES windows(slot, machine)
);
----
error: SchemaInvalid

CREATE TABLE bad_part (id INTEGER PRIMARY KEY, machine TEXT, FOREIGN KEY (machine) REFERENCES windows(machine));
----
error: SchemaInvalid

CREATE TABLE bad_parent (id INTEGER PRIMARY KEY, owner INTEGER REFERENCES nowhere(id));
----
error: TableNotFound

SELECT id FROM bad_column;
----
error: TableNotFound

SELECT id FROM bad_parent;
----
error: TableNotFound

# The engine names the index behind a reference itself, and keeps that beginning of a name.
CREATE INDEX __fk_tasks_note ON tasks (note);
----
error: ReservedIndexName

# A column an index names is not dropped unless the statement says CASCADE, and the refusal
# leaves the column and its indexes in place.
CREATE TABLE notes (id INTEGER PRIMARY KEY, tag TEXT, body TEXT);
CREATE INDEX notes_tag ON notes (tag);
CREATE INDEX notes_tag_body ON notes (tag, body);
INSERT INTO notes (id, tag, body) VALUES (1, 'a', 'x'), (2, 'b', 'y');
ALTER TABLE notes DROP COLUMN tag;
----
error: ColumnInIndex

ALTER TABLE notes DROP COLUMN tag RESTRICT;
----
error: ColumnInIndex

SELECT id, tag, body FROM notes WHERE tag = 'b';
----
id | tag | body
2 | b | y

ALTER TABLE notes DROP COLUMN tag CASCADE;
SELECT tag FROM notes;
----
error: ColumnNotFound

SELECT id, body FROM notes ORDER BY id;
----
id | body
1 | x
2 | y

# A reference joins columns of one type: an INTEGER never matches a UUID key, so the table is
# refused when it is declared.
CREATE TABLE typed_parents (id UUID PRIMARY KEY);
CREATE TABLE typed_children (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES typed_parents(id));
----
error: SchemaInvalid

SELECT id FROM typed_children;
----
error: TableNotFound

# A table that references itself keeps the reference when the column is renamed.
CREATE TABLE tree (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES tree(id));
INSERT INTO tree (id, parent) VALUES (1, NULL), (2, 1);
ALTER TABLE tree RENAME COLUMN parent TO up;
INSERT INTO tree (id, up) VALUES (3, 2);
----
rows affected: 1

INSERT INTO tree (id, up) VALUES (4, 404);
----
error: ForeignKeyViolation

DELETE FROM tree WHERE id = 1;
----
error: ForeignKeyViolation

# Renaming the referenced UNIQUE column (not the key) leaves the reference in force, as in
# Postgres: a missing parent is still refused and a present one taken.
CREATE TABLE n (id INTEGER PRIMARY KEY, code TEXT UNIQUE, parent TEXT REFERENCES n(code));
INSERT INTO n (id, code) VALUES (1, 'a');
ALTER TABLE n RENAME COLUMN code TO tag;
INSERT INTO n (id, tag, parent) VALUES (2, 'b', 'a');
----
rows affected: 1

INSERT INTO n (id, tag, parent) VALUES (3, 'c', 'zz');
----
error: ForeignKeyViolation

# Propagation along an edge needs the table to have the `id` column the edges name, so a table
# without one is refused when it is declared, not when a row first changes state.
CREATE TABLE no_id (k UUID PRIMARY KEY, s TEXT) STATE MACHINE (s: a -> [b]) PROPAGATE ON EDGE E INCOMING STATE b SET b;
----
error: SchemaInvalid

# The child's key is the reference: a profile is the profile of the person with its key.
CREATE TABLE people (id INTEGER PRIMARY KEY);
CREATE TABLE profiles (
  person_id INTEGER PRIMARY KEY,
  bio TEXT,
  FOREIGN KEY (person_id) REFERENCES people(id)
);
INSERT INTO people (id) VALUES (1), (2);
INSERT INTO profiles (person_id, bio) VALUES (404, 'nobody');
----
error: ForeignKeyViolation

INSERT INTO profiles (person_id, bio) VALUES (1, 'first');
----
rows affected: 1

SELECT person_id, bio FROM profiles;
----
person_id | bio
1 | first

# A table that references itself: a part names the part it belongs to.
CREATE TABLE parts (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parts(id));
INSERT INTO parts (id, parent_id) VALUES (1, NULL);
INSERT INTO parts (id, parent_id) VALUES (3, 404);
----
error: ForeignKeyViolation

INSERT INTO parts (id, parent_id) VALUES (2, 1);
----
rows affected: 1

SELECT id, parent_id FROM parts ORDER BY id;
----
id | parent_id
1 | NULL
2 | 1

# A reference onto a TEXT column whose values are longer than an index entry keeps (256 bytes):
# the parent named differs from another only after byte 256, and the reference tells them apart.
CREATE TABLE long_parents (id INTEGER PRIMARY KEY, name TEXT UNIQUE);
CREATE TABLE long_children (id INTEGER PRIMARY KEY, parent_name TEXT REFERENCES long_parents(name));
INSERT INTO long_parents (id, name) VALUES (1, 'ppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppaxxxxxxxxxxxxxxxxxxx'), (2, 'ppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppbxxxxxxxxxxxxxxxxxxx');
INSERT INTO long_children (id, parent_name) VALUES (1, 'ppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppaxxxxxxxxxxxxxxxxxxx');
----
rows affected: 1

INSERT INTO long_children (id, parent_name) VALUES (2, 'ppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppcxxxxxxxxxxxxxxxxxxx');
----
error: ForeignKeyViolation

DELETE FROM long_parents WHERE id = 1;
----
error: ForeignKeyViolation

DELETE FROM long_parents WHERE id = 2;
----
rows affected: 1
