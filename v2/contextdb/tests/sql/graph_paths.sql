# GRAPH_TABLE with a variable-length step or several steps: a walk along every matching edge out
# from each start, to the upper bound. A node is reported once, at the shallowest depth it is
# reached. An edge back to a node already reached still counts.

CREATE TABLE edges (
  id INTEGER PRIMARY KEY,
  source_id UUID NOT NULL,
  target_id UUID NOT NULL,
  edge_type TEXT NOT NULL
);

# A cycle 1 -> 2 -> 3 -> 1; node 5 with an edge to itself; 6 -> 7 -> 8 with 6 -> 8 beside it;
# and two starts, a and b, that both reach e in one step, while b reaches d in one and a in two.
INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'E'),
       (2, '00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000003', 'E'),
       (3, '00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000001', 'E'),
       (4, '00000000-0000-0000-0000-000000000005', '00000000-0000-0000-0000-000000000005', 'E'),
       (5, '00000000-0000-0000-0000-000000000006', '00000000-0000-0000-0000-000000000007', 'E'),
       (6, '00000000-0000-0000-0000-000000000007', '00000000-0000-0000-0000-000000000008', 'E'),
       (7, '00000000-0000-0000-0000-000000000006', '00000000-0000-0000-0000-000000000008', 'E'),
       (8, '00000000-0000-0000-0000-00000000000b', '00000000-0000-0000-0000-00000000000d', 'E'),
       (9, '00000000-0000-0000-0000-00000000000a', '00000000-0000-0000-0000-00000000000c', 'E'),
       (10, '00000000-0000-0000-0000-00000000000c', '00000000-0000-0000-0000-00000000000d', 'E'),
       (11, '00000000-0000-0000-0000-00000000000b', '00000000-0000-0000-0000-00000000000e', 'E'),
       (12, '00000000-0000-0000-0000-00000000000a', '00000000-0000-0000-0000-00000000000e', 'E');
----
rows affected: 12

# A cycle: the walk goes round it, and the start is reported when the cycle comes back to it
# within the range.
SELECT b_id, depth FROM GRAPH_TABLE(
  edges MATCH (a)-[:E]->{1,3}(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000001' OR a.id = '00000000-0000-0000-0000-000000000007'
  COLUMNS (b.id AS b_id, depth AS depth)
) ORDER BY depth, b_id;
----
b_id | depth
00000000-0000-0000-0000-000000000002 | 1
00000000-0000-0000-0000-000000000008 | 1
00000000-0000-0000-0000-000000000003 | 2
00000000-0000-0000-0000-000000000001 | 3

# The same cycle, one step short of coming back: the start is not reported.
SELECT b_id, depth FROM GRAPH_TABLE(
  edges MATCH (a)-[:E]->{1,2}(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000001'
  COLUMNS (b.id AS b_id, depth AS depth)
) ORDER BY depth;
----
b_id | depth
00000000-0000-0000-0000-000000000002 | 1
00000000-0000-0000-0000-000000000003 | 2

# The lower bound leaves out what is nearer.
SELECT b_id, depth FROM GRAPH_TABLE(
  edges MATCH (a)-[:E]->{2,3}(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000001'
  COLUMNS (b.id AS b_id, depth AS depth)
) ORDER BY depth;
----
b_id | depth
00000000-0000-0000-0000-000000000003 | 2
00000000-0000-0000-0000-000000000001 | 3

# With no start named, every node is a start.
SELECT COUNT(*) AS n FROM GRAPH_TABLE(
  edges MATCH (a)-[:E]->{2,3}(b)
  COLUMNS (b.id AS b_id)
);
----
n
4

# A start named twice is one start: one edge leaves it, so one row.
SELECT COUNT(*) AS n FROM GRAPH_TABLE(edges MATCH (a)-[:E]->(b) WHERE a.id IN ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000001') COLUMNS (b.id AS b_id));
----
n
1

# An edge from a node to itself reaches that node, once.
SELECT b_id, depth FROM GRAPH_TABLE(
  edges MATCH (a)-[:E]->{1,2}(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000005'
  COLUMNS (b.id AS b_id, depth AS depth)
);
----
b_id | depth
00000000-0000-0000-0000-000000000005 | 1

# Node 8 is one step and two steps from node 6: it is reported once, at one.
SELECT b_id, depth FROM GRAPH_TABLE(
  edges MATCH (a)-[:E]->{1,3}(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000006'
  COLUMNS (b.id AS b_id, depth AS depth)
) ORDER BY b_id;
----
b_id | depth
00000000-0000-0000-0000-000000000007 | 1
00000000-0000-0000-0000-000000000008 | 1

# Two starts. d is reported once, from b, which is nearer. e is as near to both, and goes to
# the start that the list names first.
SELECT a_id, b_id, depth FROM GRAPH_TABLE(
  edges MATCH (a)-[:E]->{1,2}(b)
  WHERE a.id IN ('00000000-0000-0000-0000-00000000000b', '00000000-0000-0000-0000-00000000000a')
  COLUMNS (a.id AS a_id, b.id AS b_id, depth AS depth)
) ORDER BY b_id;
----
a_id | b_id | depth
00000000-0000-0000-0000-00000000000a | 00000000-0000-0000-0000-00000000000c | 1
00000000-0000-0000-0000-00000000000b | 00000000-0000-0000-0000-00000000000d | 1
00000000-0000-0000-0000-00000000000b | 00000000-0000-0000-0000-00000000000e | 1

SELECT a_id, b_id, depth FROM GRAPH_TABLE(
  edges MATCH (a)-[:E]->{1,2}(b)
  WHERE a.id IN ('00000000-0000-0000-0000-00000000000a', '00000000-0000-0000-0000-00000000000b')
  COLUMNS (a.id AS a_id, b.id AS b_id, depth AS depth)
) ORDER BY b_id;
----
a_id | b_id | depth
00000000-0000-0000-0000-00000000000a | 00000000-0000-0000-0000-00000000000c | 1
00000000-0000-0000-0000-00000000000b | 00000000-0000-0000-0000-00000000000d | 1
00000000-0000-0000-0000-00000000000a | 00000000-0000-0000-0000-00000000000e | 1

# With no start named every node is one, and between two that are as near the smaller id wins.
SELECT a_id, b_id, depth FROM GRAPH_TABLE(
  edges MATCH (a)-[:E]->{1,2}(b)
  COLUMNS (a.id AS a_id, b.id AS b_id, depth AS depth)
) ORDER BY b_id;
----
a_id | b_id | depth
00000000-0000-0000-0000-000000000003 | 00000000-0000-0000-0000-000000000001 | 1
00000000-0000-0000-0000-000000000001 | 00000000-0000-0000-0000-000000000002 | 1
00000000-0000-0000-0000-000000000002 | 00000000-0000-0000-0000-000000000003 | 1
00000000-0000-0000-0000-000000000005 | 00000000-0000-0000-0000-000000000005 | 1
00000000-0000-0000-0000-000000000006 | 00000000-0000-0000-0000-000000000007 | 1
00000000-0000-0000-0000-000000000006 | 00000000-0000-0000-0000-000000000008 | 1
00000000-0000-0000-0000-00000000000a | 00000000-0000-0000-0000-00000000000c | 1
00000000-0000-0000-0000-00000000000b | 00000000-0000-0000-0000-00000000000d | 1
00000000-0000-0000-0000-00000000000a | 00000000-0000-0000-0000-00000000000e | 1

# Several steps written out, each node named.
SELECT a_id, b_id, c_id FROM GRAPH_TABLE(
  edges MATCH (a)-[:E]->(b)-[:E]->(c)
  WHERE a.id = '00000000-0000-0000-0000-000000000001'
  COLUMNS (a.id AS a_id, b.id AS b_id, c.id AS c_id)
);
----
a_id | b_id | c_id
00000000-0000-0000-0000-000000000001 | 00000000-0000-0000-0000-000000000002 | 00000000-0000-0000-0000-000000000003

# WHERE is applied to what the walk reached.
SELECT b_id FROM GRAPH_TABLE(
  edges MATCH (a)-[:E]->{1,3}(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000001' AND b.id <> '00000000-0000-0000-0000-000000000002'
  COLUMNS (b.id AS b_id)
) ORDER BY b_id;
----
b_id
00000000-0000-0000-0000-000000000001
00000000-0000-0000-0000-000000000003

# With no start named, a written-out pattern starts from every node that has an edge: six paths.
SELECT COUNT(*) FROM GRAPH_TABLE(edges MATCH (a)-[:E]->(b)-[:E]->(c) COLUMNS (c.id AS c_id));
----
count
6

# A pattern names its columns: with no COLUMNS it is refused.
SELECT id, depth FROM GRAPH_TABLE(
  edges MATCH (a)-[:E]->(b) WHERE a.id = '00000000-0000-0000-0000-000000000001'
);
----
error: PlanError

# A walk needs an upper bound, and the bound is at most 10.
SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:E]->{1,10}(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000007' COLUMNS (b.id AS b_id));
----
b_id
00000000-0000-0000-0000-000000000008

SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:E]->{1,11}(b) COLUMNS (b.id AS b_id));
----
error: BfsDepthExceeded

SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:E]->{1,}(b) COLUMNS (b.id AS b_id));
----
error: UnboundedTraversal

# A deleted edge is no longer walked, and an edge whose end is changed leads to the new end.
DELETE FROM edges WHERE id = 2;
----
rows affected: 1

UPDATE edges SET target_id = '00000000-0000-0000-0000-000000000009' WHERE id = 1;
----
rows affected: 1

SELECT b_id, depth FROM GRAPH_TABLE(
  edges MATCH (a)-[:E]->{1,3}(b)
  WHERE a.id = '00000000-0000-0000-0000-000000000001'
  COLUMNS (b.id AS b_id, depth AS depth)
);
----
b_id | depth
00000000-0000-0000-0000-000000000009 | 1
