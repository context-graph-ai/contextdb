# What a graph pattern returns is rows like any other: a CTE holds them, and they are joined,
# filtered and searched from there.

CREATE TABLE edges (
  id INTEGER PRIMARY KEY,
  source_id UUID NOT NULL,
  target_id UUID NOT NULL,
  edge_type TEXT NOT NULL
);

CREATE TABLE tasks (id UUID PRIMARY KEY, status TEXT NOT NULL);

CREATE TABLE items (id UUID PRIMARY KEY, name TEXT NOT NULL, is_deprecated BOOLEAN NOT NULL, embedding VECTOR(2));

INSERT INTO edges (id, source_id, target_id, edge_type)
VALUES (1, '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'DEPENDS_ON'),
       (2, '00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000003', 'DEPENDS_ON'),
       (3, '00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000005', 'DEPENDS_ON'),
       (4, '00000000-0000-0000-0000-000000000011', '00000000-0000-0000-0000-000000000012', 'RELATES_TO'),
       (5, '00000000-0000-0000-0000-000000000012', '00000000-0000-0000-0000-000000000013', 'RELATES_TO'),
       (6, '00000000-0000-0000-0000-000000000011', '00000000-0000-0000-0000-000000000014', 'RELATES_TO'),
       (7, '00000000-0000-0000-0000-000000000012', '00000000-0000-0000-0000-000000000001', 'EXTRACTED_FROM'),
       (8, '00000000-0000-0000-0000-000000000015', '00000000-0000-0000-0000-000000000002', 'EXTRACTED_FROM');
----
rows affected: 8

INSERT INTO tasks (id, status)
VALUES ('00000000-0000-0000-0000-000000000001', 'active'),
       ('00000000-0000-0000-0000-000000000002', 'active'),
       ('00000000-0000-0000-0000-000000000003', 'done'),
       ('00000000-0000-0000-0000-000000000005', 'active');
----
rows affected: 4

# Items 12, 14 and 15 hold the vector that is searched for; 13 holds another. Item 15 is on no
# RELATES_TO edge, and item 14 is deprecated.
INSERT INTO items (id, name, is_deprecated, embedding)
VALUES ('00000000-0000-0000-0000-000000000012', 'near', FALSE, [1.0, 0.0]),
       ('00000000-0000-0000-0000-000000000013', 'far', FALSE, [0.0, 1.0]),
       ('00000000-0000-0000-0000-000000000014', 'retired', TRUE, [1.0, 0.0]),
       ('00000000-0000-0000-0000-000000000015', 'outside', FALSE, [1.0, 0.0]);
----
rows affected: 4

# The tasks that task 1 depends on, joined to their rows: task 3 is not active, and task 5 is
# active but no dependency of task 1.
WITH deps AS (
  SELECT b_id FROM GRAPH_TABLE(
    edges MATCH (a)-[:DEPENDS_ON]->{1,3}(b)
    WHERE a.id = '00000000-0000-0000-0000-000000000001'
    COLUMNS (b.id AS b_id)
  )
)
SELECT t.id, t.status FROM tasks t
INNER JOIN deps ON t.id = deps.b_id
WHERE t.status = 'active';
----
id | status
00000000-0000-0000-0000-000000000002 | active

# The graph first, the search after: the nearest items among those a RELATES_TO walk reaches.
# Item 15 is as near as item 12 and is in no answer, because no walk reaches it.
WITH neighborhood AS (
  SELECT b_id FROM GRAPH_TABLE(
    edges MATCH (a)-[:RELATES_TO]->{1,2}(b)
    COLUMNS (b.id AS b_id)
  )
),
candidates AS (
  SELECT id, name, embedding FROM items i
  INNER JOIN neighborhood n ON i.id = n.b_id
  WHERE i.is_deprecated = FALSE
)
SELECT id, name FROM candidates
ORDER BY embedding <=> [1.0, 0.0]
LIMIT 5;
----
id | name
00000000-0000-0000-0000-000000000012 | near
00000000-0000-0000-0000-000000000013 | far

# The search first, the graph after: the three nearest items, and what each was extracted from.
WITH nearest AS (
  SELECT id FROM items ORDER BY embedding <=> [1.0, 0.0] LIMIT 3
)
SELECT a_id, b_id FROM GRAPH_TABLE(
  edges MATCH (a)-[:EXTRACTED_FROM]->(b)
  WHERE a.id IN (SELECT id FROM nearest)
  COLUMNS (a.id AS a_id, b.id AS b_id)
) ORDER BY a_id;
----
a_id | b_id
00000000-0000-0000-0000-000000000012 | 00000000-0000-0000-0000-000000000001
00000000-0000-0000-0000-000000000015 | 00000000-0000-0000-0000-000000000002

# A graph pattern is joined through a CTE. Written straight into a join it is not read.
SELECT r.b_id, t.status FROM GRAPH_TABLE(
  edges MATCH (a)-[:DEPENDS_ON]->(b) COLUMNS (b.id AS b_id)
) AS r JOIN tasks t ON t.id = r.b_id;
----
error: ParseError
