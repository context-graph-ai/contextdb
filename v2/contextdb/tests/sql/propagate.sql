# PROPAGATE: a row follows the transition of a row it references, or of a row an edge joins it
# to, inside the statement that makes the transition. A row that cannot make the move is left
# as it is, unless the declaration says ABORT ON FAILURE: then the statement is refused whole.

CREATE TABLE projects (id INTEGER PRIMARY KEY, status TEXT NOT NULL)
STATE MACHINE (status: active -> [archived]);
CREATE TABLE tasks (
  id INTEGER PRIMARY KEY,
  status TEXT NOT NULL,
  project_id INTEGER REFERENCES projects(id) ON STATE archived PROPAGATE SET invalidated
) STATE MACHINE (status: active -> [invalidated, superseded]);
CREATE TABLE steps (
  id INTEGER PRIMARY KEY,
  status TEXT NOT NULL,
  task_id INTEGER REFERENCES tasks(id) ON STATE invalidated PROPAGATE SET invalidated
) STATE MACHINE (status: active -> [invalidated]);
INSERT INTO projects (id, status) VALUES (1, 'active'), (2, 'active');
INSERT INTO tasks (id, status, project_id)
VALUES (1, 'active', 1), (2, 'superseded', 1), (3, 'active', 2), (4, 'active', NULL);
INSERT INTO steps (id, status, task_id)
VALUES (1, 'active', 1), (2, 'active', 1), (3, 'active', 2), (4, 'active', 3);

# The statement says how many rows it named, not how many followed.
UPDATE projects SET status = 'archived' WHERE id = 1;
----
rows affected: 1  (read 5, compared 0)

# Task 1 followed its project. Task 2 was superseded, a state that lists no next state, and is
# left as it is. Tasks 3 and 4 name another project and none.
SELECT id, status FROM tasks ORDER BY id;
----
id | status  (read 4, compared 0)
1 | invalidated
2 | superseded
3 | active
4 | active

# The steps of task 1 followed task 1: a row that moved by following is followed in turn. The
# step of task 2 did not move, because task 2 did not.
SELECT id, status FROM steps ORDER BY id;
----
id | status  (read 4, compared 0)
1 | invalidated
2 | invalidated
3 | active
4 | active

# A task that is moved by its own statement is followed the same way.
UPDATE tasks SET status = 'invalidated' WHERE id = 3;
----
rows affected: 1  (read 2, compared 0)

SELECT id, status FROM steps ORDER BY id;
----
id | status  (read 4, compared 0)
1 | invalidated
2 | invalidated
3 | active
4 | invalidated

# A transition to a state the reference does not name moves nothing.
UPDATE tasks SET status = 'superseded' WHERE id = 4;
INSERT INTO steps (id, status, task_id) VALUES (5, 'active', 4);
SELECT id, status FROM steps WHERE id = 5;
----
id | status  (read 1, compared 0)
5 | active

# ABORT ON FAILURE: one row that cannot follow refuses the statement, and the row that was to
# make the transition has not made it.
CREATE TABLE strict_tasks (
  id INTEGER PRIMARY KEY,
  status TEXT NOT NULL,
  project_id INTEGER REFERENCES projects(id) ON STATE archived PROPAGATE SET invalidated ABORT ON FAILURE
) STATE MACHINE (status: active -> [invalidated, superseded]);
INSERT INTO projects (id, status) VALUES (3, 'active'), (4, 'active');
INSERT INTO strict_tasks (id, status, project_id)
VALUES (1, 'active', 3), (2, 'superseded', 3), (3, 'active', 4);
UPDATE projects SET status = 'archived' WHERE id = 3;
----
error: PropagationAborted

SELECT id, status FROM projects WHERE id >= 3 ORDER BY id;
----
id | status  (read 2, compared 0)
3 | active
4 | active

SELECT id, status FROM strict_tasks ORDER BY id;
----
id | status  (read 3, compared 0)
1 | active
2 | superseded
3 | active

# Where every row can follow, the statement stands.
UPDATE projects SET status = 'archived' WHERE id = 4;
----
rows affected: 1  (read 2, compared 0)

SELECT id, status FROM strict_tasks ORDER BY id;
----
id | status  (read 3, compared 0)
1 | active
2 | superseded
3 | invalidated

# PROPAGATE ON EDGE: the rows of a table follow each other along the edges of one type.
# INCOMING follows an edge from its target to its source: here, a claim that cites an
# invalidated claim becomes invalidated, and so does a claim that cites that one.
CREATE TABLE edges (
  id INTEGER PRIMARY KEY,
  source_id UUID NOT NULL,
  target_id UUID NOT NULL,
  edge_type TEXT NOT NULL
);
CREATE TABLE claims (id UUID PRIMARY KEY, status TEXT NOT NULL)
STATE MACHINE (status: active -> [invalidated, superseded])
PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated;
INSERT INTO claims (id, status)
VALUES ('00000000-0000-0000-0000-000000000001', 'active'),
       ('00000000-0000-0000-0000-000000000002', 'active'),
       ('00000000-0000-0000-0000-000000000003', 'active'),
       ('00000000-0000-0000-0000-000000000004', 'active'),
       ('00000000-0000-0000-0000-000000000005', 'active');
# 2 cites 1, 3 cites 2, 1 cites 4, and 5 is joined to 1 by an edge of another type.
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (1, '00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001', 'CITES'),
       (2, '00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000002', 'CITES'),
       (3, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000004', 'CITES'),
       (4, '00000000-0000-0000-0000-000000000005', '00000000-0000-0000-0000-000000000001', 'MENTIONS');
UPDATE claims SET status = 'invalidated' WHERE id = '00000000-0000-0000-0000-000000000001';
----
rows affected: 1  (read 6, compared 0)

# Claim 4 is cited by claim 1, which is the other way along the edge, and stays.
SELECT id, status FROM claims ORDER BY id;
----
id | status  (read 5, compared 0)
00000000-0000-0000-0000-000000000001 | invalidated
00000000-0000-0000-0000-000000000002 | invalidated
00000000-0000-0000-0000-000000000003 | invalidated
00000000-0000-0000-0000-000000000004 | active
00000000-0000-0000-0000-000000000005 | active

# OUTGOING follows an edge from its source to its target, and MAX DEPTH ends the walk: 11
# feeds 12, 12 feeds 13 and 13 feeds 14. With MAX DEPTH 1 the pump one edge away stops and the
# pump three edges away runs on.
CREATE TABLE pumps (id UUID PRIMARY KEY, status TEXT NOT NULL)
STATE MACHINE (status: running -> [stopped])
PROPAGATE ON EDGE FEEDS OUTGOING STATE stopped SET stopped MAX DEPTH 1;
INSERT INTO pumps (id, status)
VALUES ('00000000-0000-0000-0000-000000000011', 'running'),
       ('00000000-0000-0000-0000-000000000012', 'running'),
       ('00000000-0000-0000-0000-000000000013', 'running'),
       ('00000000-0000-0000-0000-000000000014', 'running');
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (11, '00000000-0000-0000-0000-000000000011', '00000000-0000-0000-0000-000000000012', 'FEEDS'),
       (12, '00000000-0000-0000-0000-000000000012', '00000000-0000-0000-0000-000000000013', 'FEEDS'),
       (13, '00000000-0000-0000-0000-000000000013', '00000000-0000-0000-0000-000000000014', 'FEEDS');
UPDATE pumps SET status = 'stopped' WHERE id = '00000000-0000-0000-0000-000000000011';
SELECT id, status FROM pumps WHERE id <> '00000000-0000-0000-0000-000000000013' ORDER BY id;
----
id | status  (read 4, compared 0)
00000000-0000-0000-0000-000000000011 | stopped
00000000-0000-0000-0000-000000000012 | stopped
00000000-0000-0000-0000-000000000014 | running

# BOTH follows an edge either way: 21 is tied to 22, and 23 is tied to 21.
CREATE TABLE peers (id UUID PRIMARY KEY, status TEXT NOT NULL)
STATE MACHINE (status: up -> [down])
PROPAGATE ON EDGE TIED BOTH STATE down SET down;
INSERT INTO peers (id, status)
VALUES ('00000000-0000-0000-0000-000000000021', 'up'),
       ('00000000-0000-0000-0000-000000000022', 'up'),
       ('00000000-0000-0000-0000-000000000023', 'up'),
       ('00000000-0000-0000-0000-000000000024', 'up');
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (21, '00000000-0000-0000-0000-000000000021', '00000000-0000-0000-0000-000000000022', 'TIED'),
       (22, '00000000-0000-0000-0000-000000000023', '00000000-0000-0000-0000-000000000021', 'TIED');
UPDATE peers SET status = 'down' WHERE id = '00000000-0000-0000-0000-000000000021';
SELECT id, status FROM peers ORDER BY id;
----
id | status  (read 4, compared 0)
00000000-0000-0000-0000-000000000021 | down
00000000-0000-0000-0000-000000000022 | down
00000000-0000-0000-0000-000000000023 | down
00000000-0000-0000-0000-000000000024 | up

# ABORT ON FAILURE along an edge: 32 holds 31 and is already released, a state that lists no
# next state, so 31 cannot be revoked. 33 holds nothing and can.
CREATE TABLE grants (id UUID PRIMARY KEY, status TEXT NOT NULL)
STATE MACHINE (status: live -> [revoked, released])
PROPAGATE ON EDGE HOLDS INCOMING STATE revoked SET revoked ABORT ON FAILURE;
INSERT INTO grants (id, status)
VALUES ('00000000-0000-0000-0000-000000000031', 'live'),
       ('00000000-0000-0000-0000-000000000032', 'released'),
       ('00000000-0000-0000-0000-000000000033', 'live');
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (31, '00000000-0000-0000-0000-000000000032', '00000000-0000-0000-0000-000000000031', 'HOLDS');
UPDATE grants SET status = 'revoked' WHERE id = '00000000-0000-0000-0000-000000000031';
----
error: PropagationAborted

UPDATE grants SET status = 'revoked' WHERE id = '00000000-0000-0000-0000-000000000033';
----
rows affected: 1  (read 1, compared 0)

SELECT id, status FROM grants ORDER BY id;
----
id | status  (read 3, compared 0)
00000000-0000-0000-0000-000000000031 | live
00000000-0000-0000-0000-000000000032 | released
00000000-0000-0000-0000-000000000033 | revoked

# A ring of edges ends: 111 cites 112, 112 cites 113, 113 cites 111, and 114 is joined to nothing.
# Each claim follows once and the statement ends, under ABORT ON FAILURE too: the row the ring
# comes back to is already in the state asked for, so it is left alone, and not refused for
# lacking a move out of it. An edge whose source is no claim is passed over.
CREATE TABLE ring_firm (id UUID PRIMARY KEY, status TEXT NOT NULL)
STATE MACHINE (status: active -> [invalidated])
PROPAGATE ON EDGE CITES_FIRM INCOMING STATE invalidated SET invalidated ABORT ON FAILURE;
INSERT INTO ring_firm (id, status) VALUES ('00000000-0000-0000-0000-000000000111', 'active'), ('00000000-0000-0000-0000-000000000112', 'active'), ('00000000-0000-0000-0000-000000000113', 'active'), ('00000000-0000-0000-0000-000000000114', 'active');
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (111, '00000000-0000-0000-0000-000000000111', '00000000-0000-0000-0000-000000000112', 'CITES_FIRM'), (112, '00000000-0000-0000-0000-000000000112', '00000000-0000-0000-0000-000000000113', 'CITES_FIRM'), (113, '00000000-0000-0000-0000-000000000113', '00000000-0000-0000-0000-000000000111', 'CITES_FIRM'), (114, '00000000-0000-0000-0000-000000000999', '00000000-0000-0000-0000-000000000111', 'CITES_FIRM');
UPDATE ring_firm SET status = 'invalidated' WHERE id = '00000000-0000-0000-0000-000000000111';
----
rows affected: 1  (read 8, compared 0)

SELECT id, status FROM ring_firm ORDER BY id;
----
id | status  (read 4, compared 0)
00000000-0000-0000-0000-000000000111 | invalidated
00000000-0000-0000-0000-000000000112 | invalidated
00000000-0000-0000-0000-000000000113 | invalidated
00000000-0000-0000-0000-000000000114 | active

# With no MAX DEPTH a change is followed ten edges and the eleventh row stays: 201 feeds 202,
# and so on to 20c, twelve nodes in a chain.
CREATE TABLE chain_nodes (id UUID PRIMARY KEY, status TEXT NOT NULL)
STATE MACHINE (status: running -> [stopped])
PROPAGATE ON EDGE FEEDS OUTGOING STATE stopped SET stopped;
INSERT INTO chain_nodes (id, status)
VALUES ('00000000-0000-0000-0000-000000000201', 'running'), ('00000000-0000-0000-0000-000000000202', 'running'), ('00000000-0000-0000-0000-000000000203', 'running'), ('00000000-0000-0000-0000-000000000204', 'running'), ('00000000-0000-0000-0000-000000000205', 'running'), ('00000000-0000-0000-0000-000000000206', 'running'), ('00000000-0000-0000-0000-000000000207', 'running'), ('00000000-0000-0000-0000-000000000208', 'running'), ('00000000-0000-0000-0000-000000000209', 'running'), ('00000000-0000-0000-0000-00000000020a', 'running'), ('00000000-0000-0000-0000-00000000020b', 'running'), ('00000000-0000-0000-0000-00000000020c', 'running');
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (301, '00000000-0000-0000-0000-000000000201', '00000000-0000-0000-0000-000000000202', 'FEEDS'), (302, '00000000-0000-0000-0000-000000000202', '00000000-0000-0000-0000-000000000203', 'FEEDS'), (303, '00000000-0000-0000-0000-000000000203', '00000000-0000-0000-0000-000000000204', 'FEEDS'), (304, '00000000-0000-0000-0000-000000000204', '00000000-0000-0000-0000-000000000205', 'FEEDS'), (305, '00000000-0000-0000-0000-000000000205', '00000000-0000-0000-0000-000000000206', 'FEEDS'), (306, '00000000-0000-0000-0000-000000000206', '00000000-0000-0000-0000-000000000207', 'FEEDS'), (307, '00000000-0000-0000-0000-000000000207', '00000000-0000-0000-0000-000000000208', 'FEEDS'), (308, '00000000-0000-0000-0000-000000000208', '00000000-0000-0000-0000-000000000209', 'FEEDS'), (309, '00000000-0000-0000-0000-000000000209', '00000000-0000-0000-0000-00000000020a', 'FEEDS'), (310, '00000000-0000-0000-0000-00000000020a', '00000000-0000-0000-0000-00000000020b', 'FEEDS'), (311, '00000000-0000-0000-0000-00000000020b', '00000000-0000-0000-0000-00000000020c', 'FEEDS');
UPDATE chain_nodes SET status = 'stopped' WHERE id = '00000000-0000-0000-0000-000000000201';
----
rows affected: 1  (read 21, compared 0)

SELECT id, status FROM chain_nodes ORDER BY id;
----
id | status  (read 12, compared 0)
00000000-0000-0000-0000-000000000201 | stopped
00000000-0000-0000-0000-000000000202 | stopped
00000000-0000-0000-0000-000000000203 | stopped
00000000-0000-0000-0000-000000000204 | stopped
00000000-0000-0000-0000-000000000205 | stopped
00000000-0000-0000-0000-000000000206 | stopped
00000000-0000-0000-0000-000000000207 | stopped
00000000-0000-0000-0000-000000000208 | stopped
00000000-0000-0000-0000-000000000209 | stopped
00000000-0000-0000-0000-00000000020a | stopped
00000000-0000-0000-0000-00000000020b | stopped
00000000-0000-0000-0000-00000000020c | running

# A row that references its own table is followed along the chain: four rows on, and with no MAX DEPTH ten rows on and not the eleventh.
CREATE TABLE fk_chain (id INTEGER PRIMARY KEY, status TEXT NOT NULL, prev INTEGER REFERENCES fk_chain(id) ON STATE closed PROPAGATE SET closed) STATE MACHINE (status: open -> [closed]);
INSERT INTO fk_chain (id, status, prev) VALUES (1, 'open', NULL), (2, 'open', 1), (3, 'open', 2), (4, 'open', 3), (5, 'open', 4), (6, 'open', 5), (7, 'open', 6), (8, 'open', 7), (9, 'open', 8), (10, 'open', 9), (11, 'open', 10), (12, 'open', 11);
UPDATE fk_chain SET status = 'closed' WHERE id = 1;
SELECT id, status FROM fk_chain WHERE id IN (5, 11, 12) ORDER BY id;
----
id | status  (read 3, compared 0)
5 | closed
11 | closed
12 | open

# An upsert that changes a state is followed as an UPDATE is, for the rows of that row only.
INSERT INTO tasks (id, status, project_id) VALUES (5, 'active', 2), (6, 'active', 4);
INSERT INTO projects (id, status) VALUES (2, 'active') ON CONFLICT (id) DO UPDATE SET status = 'archived';
SELECT id, status FROM tasks WHERE id >= 5 ORDER BY id;
----
id | status  (read 2, compared 0)
5 | invalidated
6 | active

# A state that is IMMUTABLE takes no state machine: a state never changed has no next state.
CREATE TABLE gate_projects (id INTEGER PRIMARY KEY, status TEXT NOT NULL)
STATE MACHINE (status: active -> [archived]);
CREATE TABLE lax_tasks (
  id INTEGER PRIMARY KEY,
  status TEXT NOT NULL IMMUTABLE,
  project_id INTEGER REFERENCES gate_projects(id) ON STATE archived PROPAGATE SET invalidated
) STATE MACHINE (status: active -> [invalidated]);
----
error: SchemaInvalid
