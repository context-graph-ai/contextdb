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
rows affected: 4  (read 0, compared 0)

# With no node named, every edge of the type is a row: two edges into node 3 are two rows.
SELECT a_id, b_id FROM GRAPH_TABLE(
  links MATCH (a)-[:LINKS]->(b) COLUMNS (a.id AS a_id, b.id AS b_id)
) ORDER BY a_id, b_id;
----
a_id | b_id  (read 4, compared 0)
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
a_id | b_id  (read 2, compared 0)
00000000-0000-0000-0000-000000000001 | 00000000-0000-0000-0000-000000000003
00000000-0000-0000-0000-000000000001 | 00000000-0000-0000-0000-000000000003

# Against the arrow: the nodes with an edge into node 3.
SELECT b_id FROM GRAPH_TABLE(
  links MATCH (a)<-[:LINKS]-(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000003'
  COLUMNS (b.id AS b_id)
) ORDER BY b_id;
----
b_id  (read 3, compared 0)
00000000-0000-0000-0000-000000000001
00000000-0000-0000-0000-000000000002

# Decisions are found by their edges, whatever the entity row does: one edge is written before its
# entity's row and one after, the row then changes, and both decisions still answer.
CREATE TABLE ents (id UUID PRIMARY KEY, props JSON);
CREATE TABLE basis (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL);
INSERT INTO basis (id, source_id, target_id, edge_type) VALUES (1, '00000000-0000-0000-0000-0000000000d1', '00000000-0000-0000-0000-0000000000e1', 'BASED_ON');
INSERT INTO ents (id, props) VALUES ('00000000-0000-0000-0000-0000000000e1', '{"a":1}');
INSERT INTO basis (id, source_id, target_id, edge_type) VALUES (2, '00000000-0000-0000-0000-0000000000d2', '00000000-0000-0000-0000-0000000000e1', 'BASED_ON');
UPDATE ents SET props = '{"brand_new":2}' WHERE id = '00000000-0000-0000-0000-0000000000e1';
SELECT b_id FROM GRAPH_TABLE(basis MATCH (a)<-[:BASED_ON]-(b) WHERE a.id = '00000000-0000-0000-0000-0000000000e1' COLUMNS (b.id AS b_id)) ORDER BY b_id;
----
*
00000000-0000-0000-0000-0000000000d1
00000000-0000-0000-0000-0000000000d2

# A chain written leaf first: from the root one to three steps reach the middle at 1 and the leaf at 2.
INSERT INTO basis (id, source_id, target_id, edge_type) VALUES (3, '00000000-0000-0000-0000-0000000000b2', '00000000-0000-0000-0000-0000000000b3', 'DEPENDS_ON'), (4, '00000000-0000-0000-0000-0000000000b1', '00000000-0000-0000-0000-0000000000b2', 'DEPENDS_ON');
SELECT b_id, depth FROM GRAPH_TABLE(basis MATCH (a)-[:DEPENDS_ON]->{1,3}(b) WHERE a.id = '00000000-0000-0000-0000-0000000000b1' COLUMNS (b.id AS b_id, depth AS depth)) ORDER BY depth;
----
b_id | *
00000000-0000-0000-0000-0000000000b2 | 1
00000000-0000-0000-0000-0000000000b3 | 2

# The same edges, named by the node they arrive at.
SELECT a_id, depth FROM GRAPH_TABLE(
  links MATCH (a)-[:LINKS]->(b)
  WHERE b.id = '00000000-0000-0000-0000-000000000003'
  COLUMNS (a.id AS a_id, depth AS depth)
) ORDER BY a_id;
----
a_id | depth  (read 3, compared 0)
00000000-0000-0000-0000-000000000001 | 1
00000000-0000-0000-0000-000000000002 | 1

# Either way round: an edge is followed from whichever end the node is at.
SELECT a_id, b_id FROM GRAPH_TABLE(
  links MATCH (a)-[:LINKS]-(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000003'
  COLUMNS (a.id AS a_id, b.id AS b_id)
) ORDER BY b_id;
----
a_id | b_id  (read 3, compared 0)
00000000-0000-0000-0000-000000000003 | 00000000-0000-0000-0000-000000000001
00000000-0000-0000-0000-000000000003 | 00000000-0000-0000-0000-000000000002

# An edge from a node to itself is one edge, and either way round it is one row.
SELECT a_id, b_id FROM GRAPH_TABLE(
  links MATCH (a)-[:LINKS]-(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000004'
  COLUMNS (a.id AS a_id, b.id AS b_id)
);
----
a_id | b_id  (read 2, compared 0)
00000000-0000-0000-0000-000000000004 | 00000000-0000-0000-0000-000000000004

# A node with no edge has no row, and that is an answer.
SELECT b_id FROM GRAPH_TABLE(
  links MATCH (a)-[:LINKS]->(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000009'
  COLUMNS (b.id AS b_id)
);
----
b_id  (read 0, compared 0)

# This table declares no UNIQUE rule over an edge's three values, so a second row with the same
# three is a second edge, and one hop returns both.
INSERT INTO links (id, source_id, target_id, edge_type)
VALUES (5, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000003', 'LINKS');
----
rows affected: 1  (read 0, compared 0)

SELECT a_id, b_id FROM GRAPH_TABLE(
  links MATCH (a)-[:LINKS]->(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000001'
  COLUMNS (a.id AS a_id, b.id AS b_id)
);
----
a_id | b_id  (read 3, compared 0)
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
rows affected: 2  (read 0, compared 0)

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
b_id  (read 2, compared 0)
00000000-0000-0000-0000-000000000002
00000000-0000-0000-0000-000000000002

ALTER TABLE links ADD COLUMN note TEXT;
ALTER TABLE links RENAME COLUMN note TO remark;
ALTER TABLE links DROP COLUMN remark;
SELECT COUNT(*) AS n FROM GRAPH_TABLE(links MATCH (a)-[:LINKS]->(b) COLUMNS (a.id AS a_id));
----
n  (read 5, compared 0)
4

# A property no node table carries is refused in full; a node is an id, and the edge table's INTEGER id is no node table.
SELECT x FROM GRAPH_TABLE(links MATCH (a)-[:LINKS]->(b) COLUMNS (b.edge_type AS x));
----
error: PlanError | plan error: project column not found: b.edge_type

# A table whose UNIQUE rule is exactly the three values holds one edge for each.
CREATE TABLE uniq (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL, UNIQUE (source_id, target_id, edge_type));
INSERT INTO uniq (id, source_id, target_id, edge_type) VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'LINKS');
INSERT INTO uniq (id, source_id, target_id, edge_type) VALUES (2, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'LINKS');
----
error: UniqueViolation

SELECT COUNT(*) AS n FROM GRAPH_TABLE(uniq MATCH (a)-[:LINKS]->(b) COLUMNS (a.id AS a_id));
----
n  (read 1, compared 0)
1

# A node's property is read from the row that carries the node, with no REFERENCES; a node with no row gives NULL (two edges 1 to 3 are two rows).
CREATE TABLE docs (id UUID PRIMARY KEY, title TEXT);
INSERT INTO docs (id, title) VALUES ('00000000-0000-0000-0000-000000000003', 'third');
----
rows affected: 1  (read 0, compared 0)

SELECT a_id, t FROM GRAPH_TABLE(links MATCH (a)-[:LINKS]->(b) COLUMNS (a.id AS a_id, b.title AS t)) ORDER BY a_id;
----
a_id | *
00000000-0000-0000-0000-000000000001 | third
00000000-0000-0000-0000-000000000001 | third
00000000-0000-0000-0000-000000000002 | third
00000000-0000-0000-0000-000000000004 | NULL

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

# A table of exactly the three columns, with no key, holds edges; the wrong type for one holds none.
CREATE TABLE bare (source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL);
INSERT INTO bare (source_id, target_id, edge_type) VALUES ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'E');
SELECT b_id FROM GRAPH_TABLE(bare MATCH (a)-[:E]->(b) COLUMNS (b.id AS b_id));
----
*
00000000-0000-0000-0000-000000000002

CREATE TABLE badtype (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type INTEGER NOT NULL);
SELECT b_id FROM GRAPH_TABLE(badtype MATCH (a)-[]->(b) COLUMNS (b.id AS b_id));
----
error: PlanError

# With no node named an undirected hop gives each edge both ways round and a self-loop once:
# 3 edges between two nodes and 1 loop.
SELECT COUNT(*) FROM GRAPH_TABLE(links MATCH (a)-[:LINKS]-(b) COLUMNS (a.id AS a_id));
----
count  (read 5, compared 0)
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

# Two node tables that disagree on the property's type: refused, a join gives it.
CREATE TABLE notes (id UUID PRIMARY KEY, title INTEGER);

SELECT t FROM GRAPH_TABLE(links MATCH (a)-[:LINKS]->(b) COLUMNS (b.title AS t));
----
error: PlanError
