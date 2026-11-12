# DAG('TYPE', ...): the edges of each listed type never form a cycle. An edge that would close
# one is refused when it is written, by an INSERT or by an UPDATE.

CREATE TABLE edges (
  id INTEGER PRIMARY KEY,
  source_id UUID NOT NULL,
  target_id UUID NOT NULL,
  edge_type TEXT NOT NULL
) DAG('DEPENDS_ON', 'BASED_ON');

# 1 -> 2 -> 3, and 1 -> 3 beside it: two ways to one node are no cycle.
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'DEPENDS_ON'),
       (2, '00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000003', 'DEPENDS_ON'),
       (3, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000003', 'DEPENDS_ON');
----
rows affected: 3

# Back to the start of the chain, over two edges and over one.
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (4, '00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000001', 'DEPENDS_ON');
----
error: CycleDetected

INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (4, '00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001', 'DEPENDS_ON');
----
error: CycleDetected

# An edge from a node to itself is the shortest cycle.
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (4, '00000000-0000-0000-0000-000000000009', '00000000-0000-0000-0000-000000000009', 'DEPENDS_ON');
----
error: CycleDetected

# Two edges of one statement that close a cycle between them: neither is written.
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (4, '00000000-0000-0000-0000-000000000005', '00000000-0000-0000-0000-000000000006', 'DEPENDS_ON'),
       (5, '00000000-0000-0000-0000-000000000006', '00000000-0000-0000-0000-000000000005', 'DEPENDS_ON');
----
error: CycleDetected

SELECT id FROM edges ORDER BY id;
----
id
1
2
3

# Each listed type is held acyclic by itself: an edge of the second type may run against the
# edges of the first, and may not run against its own.
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (10, '00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000001', 'BASED_ON');
----
rows affected: 1

INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (11, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000003', 'BASED_ON');
----
error: CycleDetected

# A type the table does not list may form any cycle.
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (20, '00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000001', 'CITES'),
       (21, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000003', 'CITES'),
       (22, '00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000004', 'CITES');
----
rows affected: 3

# An UPDATE is held to the same rule: turning an edge round, and giving an edge a listed type.
UPDATE edges SET source_id = '00000000-0000-0000-0000-000000000003', target_id = '00000000-0000-0000-0000-000000000001'
WHERE id = 3;
----
error: CycleDetected

UPDATE edges SET edge_type = 'DEPENDS_ON' WHERE id = 20;
----
error: CycleDetected

UPDATE edges SET edge_type = 'DEPENDS_ON' WHERE id = 22;
----
error: CycleDetected

SELECT id, edge_type FROM edges ORDER BY id;
----
id | edge_type
1 | DEPENDS_ON
2 | DEPENDS_ON
3 | DEPENDS_ON
10 | BASED_ON
20 | CITES
21 | CITES
22 | CITES

# Once the edge that made the way round is gone, the edge back is no cycle.
DELETE FROM edges WHERE id IN (2, 3);
----
rows affected: 2

INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (4, '00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000001', 'DEPENDS_ON');
----
rows affected: 1

# 3 -> 1 -> 2 now: the edge 2 -> 3 that was deleted would close it.
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (2, '00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000003', 'DEPENDS_ON');
----
error: CycleDetected

# The table declares no UNIQUE rule over an edge's three values, so a second row with the same
# three is a second row. It closes no cycle either.
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (30, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'DEPENDS_ON');
----
rows affected: 1

SELECT id FROM edges WHERE edge_type = 'DEPENDS_ON' ORDER BY id;
----
id
1
4
30

# DAG is a rule about edges: a table without the three edge columns cannot declare it.
CREATE TABLE plain (id INTEGER PRIMARY KEY, note TEXT) DAG('DEPENDS_ON');
----
error: SchemaInvalid

CREATE TABLE almost (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL) DAG('DEPENDS_ON');
----
error: SchemaInvalid

SELECT id FROM plain;
----
error: TableNotFound

# An edge that is the only one in the way is not held against its own replacement: 2 -> 1 would
# close a cycle with 1 -> 2, but 1 -> 2 is the edge this update turns round.
CREATE TABLE one_edge (
  id INTEGER PRIMARY KEY,
  source_id UUID NOT NULL,
  target_id UUID NOT NULL,
  edge_type TEXT NOT NULL
) DAG('DEPENDS_ON');
INSERT INTO one_edge (id, source_id, target_id, edge_type)
VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'DEPENDS_ON');
UPDATE one_edge SET source_id = '00000000-0000-0000-0000-000000000002', target_id = '00000000-0000-0000-0000-000000000001'
WHERE id = 1;
----
rows affected: 1

# The edge is now 2 -> 1, and the edge it was would close a cycle with it.
INSERT INTO one_edge (id, source_id, target_id, edge_type)
VALUES (2, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'DEPENDS_ON');
----
error: CycleDetected

SELECT id, source_id, target_id FROM one_edge;
----
id | source_id | target_id
1 | 00000000-0000-0000-0000-000000000002 | 00000000-0000-0000-0000-000000000001
