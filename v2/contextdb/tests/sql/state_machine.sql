# STATE MACHINE: a column whose value may only move along the transitions its table declares.
# An insert sets the first state; every UPDATE of the column is held to the list.

CREATE TABLE tasks (
  id INTEGER PRIMARY KEY,
  title TEXT,
  status TEXT NOT NULL
) STATE MACHINE (status: draft -> [active, rejected], active -> [superseded]);
INSERT INTO tasks (id, title, status)
VALUES (1, 'one', 'draft'), (2, 'two', 'draft'), (3, 'three', 'active'), (4, 'four', 'draft');

# A listed transition.
UPDATE tasks SET status = 'active' WHERE id = 1;
----
rows affected: 1

UPDATE tasks SET status = 'rejected' WHERE id = 2;
----
rows affected: 1

# Not listed: backwards, past a state, out of a state that lists nothing, and to a state the
# declaration never names.
UPDATE tasks SET status = 'draft' WHERE id = 1;
----
error: InvalidStateTransition

UPDATE tasks SET status = 'superseded' WHERE id = 4;
----
error: InvalidStateTransition

UPDATE tasks SET status = 'active' WHERE id = 2;
----
error: InvalidStateTransition

UPDATE tasks SET status = 'paused' WHERE id = 4;
----
error: InvalidStateTransition

# A state does not list itself, so writing the state a row is already in is no transition.
UPDATE tasks SET status = 'active' WHERE id = 3;
----
error: InvalidStateTransition

# One row that may not move refuses the statement for every row: row 4 could have moved.
UPDATE tasks SET status = 'active' WHERE id IN (2, 4);
----
error: InvalidStateTransition

SELECT id, status FROM tasks ORDER BY id;
----
id | status
1 | active
2 | rejected
3 | active
4 | draft

# The column the table's rule is declared by cannot be dropped, with or without CASCADE, and a
# refused drop leaves the column and its rows.
ALTER TABLE tasks DROP COLUMN status;
----
error: SchemaInvalid

ALTER TABLE tasks DROP COLUMN status CASCADE;
----
error: SchemaInvalid

SELECT id, status FROM tasks ORDER BY id;
----
id | status
1 | active
2 | rejected
3 | active
4 | draft

# Every row the statement reaches may move: both do.
UPDATE tasks SET status = 'superseded' WHERE status = 'active';
----
rows affected: 2

# Another column of the row changes whatever state the row is in, and a statement that reaches
# no row is refused for nothing.
UPDATE tasks SET title = 'renamed' WHERE id = 2;
----
rows affected: 1

UPDATE tasks SET status = 'draft' WHERE id = 404;
----
rows affected: 0

SELECT id, title, status FROM tasks ORDER BY id;
----
id | title | status
1 | one | superseded
2 | renamed | rejected
3 | three | superseded
4 | four | draft

# An insert that meets a row moves it by the same list.
INSERT INTO tasks (id, title, status) VALUES (4, 'four', 'draft')
ON CONFLICT (id) DO UPDATE SET status = 'superseded';
----
error: InvalidStateTransition

INSERT INTO tasks (id, title, status) VALUES (4, 'four', 'draft')
ON CONFLICT (id) DO UPDATE SET status = 'active';
----
rows affected: 1

SELECT id, status FROM tasks WHERE id = 4;
----
id | status
4 | active

# One next state may be written without brackets.
CREATE TABLE switches (id INTEGER PRIMARY KEY, state TEXT NOT NULL)
STATE MACHINE (state: idle -> busy, busy -> idle);
INSERT INTO switches (id, state) VALUES (1, 'idle');
UPDATE switches SET state = 'busy' WHERE id = 1;
UPDATE switches SET state = 'idle' WHERE id = 1;
UPDATE switches SET state = 'broken' WHERE id = 1;
----
error: InvalidStateTransition

SELECT id, state FROM switches;
----
id | state
1 | idle

# A table that keeps only the current version of a row keeps its state and holds every change
# to the same list.
CREATE TABLE devices (id INTEGER PRIMARY KEY, state TEXT NOT NULL)
STATE MACHINE (state: starting -> [running], running -> [stopped])
HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST;
INSERT INTO devices (id, state) VALUES (1, 'starting');
UPDATE devices SET state = 'stopped' WHERE id = 1;
----
error: InvalidStateTransition

UPDATE devices SET state = 'running' WHERE id = 1;
UPDATE devices SET state = 'starting' WHERE id = 1;
----
error: InvalidStateTransition

UPDATE devices SET state = 'stopped' WHERE id = 1;
SELECT id, state FROM devices;
----
id | state
1 | stopped
