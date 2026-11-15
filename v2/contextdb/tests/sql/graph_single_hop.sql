# GRAPH_TABLE over an edge table, one hop: one row for each edge that matches, never one row for
# each target. An edge table is a table with source_id, target_id and edge_type.

CREATE TABLE links (
  id INTEGER PRIMARY KEY,
  source_id UUID NOT NULL,
  target_id UUID NOT NULL,
  edge_type TEXT NOT NULL
);

# 1 -> 3 twice, under two types; 2 -> 3; and 4 -> 4, an edge from a node to itself.
INSERT INTO links (id, source_id, target_id, edge_type)
VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000003', 'LINKS'),
       (2, '00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000003', 'LINKS'),
       (3, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000003', 'CITES'),
       (4, '00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000004', 'LINKS');
----
rows affected: 4

# With no node named, every edge of the type is a row: two edges into node 3 are two rows.
SELECT a_id, b_id FROM GRAPH_TABLE(
  links MATCH (a)-[:LINKS]->(b) COLUMNS (a.id AS a_id, b.id AS b_id)
) ORDER BY a_id, b_id;
----
a_id | b_id
00000000-0000-0000-0000-000000000001 | 00000000-0000-0000-0000-000000000003
00000000-0000-0000-0000-000000000002 | 00000000-0000-0000-0000-000000000003
00000000-0000-0000-0000-000000000004 | 00000000-0000-0000-0000-000000000004

# Any type: two edges of two types between the same two nodes are two rows.
SELECT a_id, b_id FROM GRAPH_TABLE(
  links MATCH (a)-[]->(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000001'
  COLUMNS (a.id AS a_id, b.id AS b_id)
);
----
a_id | b_id
00000000-0000-0000-0000-000000000001 | 00000000-0000-0000-0000-000000000003
00000000-0000-0000-0000-000000000001 | 00000000-0000-0000-0000-000000000003

# Against the arrow: the nodes with an edge into node 3.
SELECT b_id FROM GRAPH_TABLE(
  links MATCH (a)<-[:LINKS]-(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000003'
  COLUMNS (b.id AS b_id)
) ORDER BY b_id;
----
b_id
00000000-0000-0000-0000-000000000001
00000000-0000-0000-0000-000000000002

# The same edges, named by the node they arrive at.
SELECT a_id, depth FROM GRAPH_TABLE(
  links MATCH (a)-[:LINKS]->(b)
  WHERE b.id = '00000000-0000-0000-0000-000000000003'
  COLUMNS (a.id AS a_id, depth AS depth)
) ORDER BY a_id;
----
a_id | depth
00000000-0000-0000-0000-000000000001 | 1
00000000-0000-0000-0000-000000000002 | 1

# Either way round: an edge is followed from whichever end the node is at.
SELECT a_id, b_id FROM GRAPH_TABLE(
  links MATCH (a)-[:LINKS]-(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000003'
  COLUMNS (a.id AS a_id, b.id AS b_id)
) ORDER BY b_id;
----
a_id | b_id
00000000-0000-0000-0000-000000000003 | 00000000-0000-0000-0000-000000000001
00000000-0000-0000-0000-000000000003 | 00000000-0000-0000-0000-000000000002

# An edge from a node to itself is one edge, and either way round it is one row.
SELECT a_id, b_id FROM GRAPH_TABLE(
  links MATCH (a)-[:LINKS]-(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000004'
  COLUMNS (a.id AS a_id, b.id AS b_id)
);
----
a_id | b_id
00000000-0000-0000-0000-000000000004 | 00000000-0000-0000-0000-000000000004

# A node with no edge has no row, and that is an answer.
SELECT b_id FROM GRAPH_TABLE(
  links MATCH (a)-[:LINKS]->(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000009'
  COLUMNS (b.id AS b_id)
);
----
b_id

# This table declares no UNIQUE rule over an edge's three values, so a second row with the same
# three is a second edge, and one hop returns both.
INSERT INTO links (id, source_id, target_id, edge_type)
VALUES (5, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000003', 'LINKS');
----
rows affected: 1

SELECT a_id, b_id FROM GRAPH_TABLE(
  links MATCH (a)-[:LINKS]->(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000001'
  COLUMNS (a.id AS a_id, b.id AS b_id)
);
----
a_id | b_id
00000000-0000-0000-0000-000000000001 | 00000000-0000-0000-0000-000000000003
00000000-0000-0000-0000-000000000001 | 00000000-0000-0000-0000-000000000003

# A table that declares a UNIQUE rule has one edge for each value of it. A column beside the
# three is part of what makes an edge when the rule names it.
CREATE TABLE keyed (
  id INTEGER PRIMARY KEY,
  source_id UUID NOT NULL,
  target_id UUID NOT NULL,
  edge_type TEXT NOT NULL,
  role TEXT NOT NULL,
  UNIQUE (source_id, target_id, edge_type, role)
);

INSERT INTO keyed (id, source_id, target_id, edge_type, role)
VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'LINKS', 'author'),
       (2, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'LINKS', 'editor');
----
rows affected: 2

INSERT INTO keyed (id, source_id, target_id, edge_type, role)
VALUES (3, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'LINKS', 'author');
----
error: UniqueViolation

SELECT b_id FROM GRAPH_TABLE(
  keyed MATCH (a)-[:LINKS]->(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000001'
  COLUMNS (b.id AS b_id)
);
----
b_id
00000000-0000-0000-0000-000000000002
00000000-0000-0000-0000-000000000002

# COLUMNS gives the ids of the nodes a pattern names and the depth, and nothing else of a node.
SELECT x FROM GRAPH_TABLE(links MATCH (a)-[:LINKS]->(b) COLUMNS (b.edge_type AS x));
----
error: PlanError

# A table that is not there is reported before anything else.
SELECT b_id FROM GRAPH_TABLE(nowhere MATCH (a)-[]->(b) COLUMNS (b.id AS b_id));
----
error: TableNotFound

# A table without the three edge columns holds no edges: a pattern over it is refused, whatever
# another table holds.
CREATE TABLE pairs (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL);

SELECT b_id FROM GRAPH_TABLE(pairs MATCH (a)-[]->(b) COLUMNS (b.id AS b_id));
----
error: PlanError

# With no node named an undirected hop gives each edge both ways round and a self-loop once:
# 3 edges between two nodes and 1 loop.
SELECT COUNT(*) FROM GRAPH_TABLE(links MATCH (a)-[:LINKS]-(b) COLUMNS (a.id AS a_id));
----
count
7

# A step is followed at least once, and its lower bound is not above its upper.
SELECT b_id FROM GRAPH_TABLE(links MATCH (a)-[:LINKS]->{0,2}(b) COLUMNS (b.id AS b_id));
----
error: PlanError

SELECT b_id FROM GRAPH_TABLE(links MATCH (a)-[:LINKS]->{3,2}(b) COLUMNS (b.id AS b_id));
----
error: PlanError

# A label on a node would select nothing, so it is refused.
SELECT b_id FROM GRAPH_TABLE(links MATCH (a:Doc)-[:LINKS]->(b) COLUMNS (b.id AS b_id));
----
error: PlanError
