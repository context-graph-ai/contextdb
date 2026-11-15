# EXPLAIN <statement>: the route it would take, a line of the TEXT column QUERY PLAN for each
# line of the explanation. The statement is planned and not run.

CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL, tag INTEGER NOT NULL);
CREATE INDEX t_tag ON t (tag);
INSERT INTO t (id, name, tag) VALUES (1, 'one', 1), (2, 'two', 2), (7, 'seven', 3);

# A lookup by the key goes through the index behind the key.
EXPLAIN SELECT name FROM t WHERE id = 7;
----
QUERY PLAN
IndexScan { index: __pk_id }
  predicates_pushed: [id]
  indexes_considered: [t_tag: first column not in WHERE]

# A range of an index is read through it, and the key's index says why it lost.
EXPLAIN SELECT id FROM t WHERE tag = 3;
----
QUERY PLAN
IndexScan { index: t_tag }
  predicates_pushed: [tag]
  indexes_considered: [__pk_id: first column not in WHERE]

# Each reason an index is passed over is said, in the documented words.
CREATE TABLE e (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, s TEXT);
CREATE INDEX e_ab ON e (a, b);
CREATE INDEX e_a ON e (a);
CREATE INDEX e_s ON e (s);
EXPLAIN SELECT id FROM e WHERE COALESCE(s, 'x') = 'a';
----
QUERY PLAN
Scan
  indexes_considered: [__pk_id: first column not in WHERE, e_ab: first column not in WHERE, e_a: first column not in WHERE, e_s: function call in predicate]

EXPLAIN SELECT id FROM e WHERE a + 1 = 2;
----
QUERY PLAN
Scan
  indexes_considered: [__pk_id: first column not in WHERE, e_ab: arithmetic in predicate, e_a: arithmetic in predicate, e_s: first column not in WHERE]

EXPLAIN SELECT id FROM e WHERE a = b;
----
QUERY PLAN
Scan
  indexes_considered: [__pk_id: first column not in WHERE, e_ab: non-literal rhs, e_a: non-literal rhs, e_s: first column not in WHERE]

EXPLAIN SELECT id FROM e WHERE s LIKE 'a%';
----
QUERY PLAN
Scan
  indexes_considered: [__pk_id: first column not in WHERE, e_ab: first column not in WHERE, e_a: first column not in WHERE, e_s: LIKE is residual-only]

EXPLAIN SELECT id FROM e WHERE a = 1 AND b = 2;
----
QUERY PLAN
IndexScan { index: e_ab }
  predicates_pushed: [a, b]
  indexes_considered: [__pk_id: first column not in WHERE, e_a: fewer predicate columns matched than chosen index, e_s: first column not in WHERE]

EXPLAIN SELECT id FROM e WHERE a = 1;
----
QUERY PLAN
IndexScan { index: e_ab }
  predicates_pushed: [a]
  indexes_considered: [__pk_id: first column not in WHERE, e_a: tied with chosen index; lost by creation order, e_s: first column not in WHERE]

# A statement that writes is explained as it would run, and writes nothing.
EXPLAIN DELETE FROM t WHERE id = 7;
SELECT id, name, tag FROM t ORDER BY id;
----
id | name | tag
1 | one | 1
2 | two | 2
7 | seven | 3

# A join goes through the index of its right table; a grouped count says whether a kept count
# serves it, and the statement that would keep one when none is declared.
CREATE TABLE owners (id INTEGER PRIMARY KEY, t_id INTEGER NOT NULL, note TEXT);
CREATE INDEX owners_t ON owners (t_id);
CREATE TABLE kept (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, conn TEXT NOT NULL, UNIQUE (ctx, conn, id)) MAINTAIN COUNT (ctx), (ctx, conn);
CREATE TABLE links (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL);

EXPLAIN SELECT x.name, o.note FROM t x JOIN owners o ON o.t_id = x.id WHERE x.id = 7;
----
QUERY PLAN
IndexScan { index: __pk_id }
  predicates_pushed: [id]
  indexes_considered: [t_tag: first column not in WHERE]
IndexScan { index: owners_t }
  predicates_pushed: [t_id]
  indexes_considered: [__pk_id: first column not in WHERE]

EXPLAIN SELECT conn, COUNT(*) FROM kept WHERE ctx = 'a' GROUP BY conn;
----
QUERY PLAN
maintained count: used (ctx, conn)

EXPLAIN SELECT t_id, COUNT(*) FROM owners GROUP BY t_id;
----
QUERY PLAN
maintained count: not declared — ALTER TABLE owners SET MAINTAIN COUNT (t_id) makes this O(1)
Scan
  indexes_considered: [__pk_id: first column not in WHERE, owners_t: first column not in WHERE]

# A pattern with no start node named says why no adjacency was chosen.
EXPLAIN SELECT name FROM t WHERE id IN (SELECT depth FROM GRAPH_TABLE(links MATCH (a)-[:LINKS]->(b) COLUMNS (depth AS depth)));
----
QUERY PLAN
Scan
  indexes_considered: [__pk_id: first column not in WHERE, __graph_edge_source_target_type: first column not in WHERE, __graph_edge_target_source_type: first column not in WHERE]
  links: no pinned vertex
Scan
  indexes_considered: [__pk_id: first column not in WHERE, t_tag: first column not in WHERE]

# An index that loses to the table's whole key says why.
CREATE INDEX e_id ON e (id);
EXPLAIN SELECT id FROM e WHERE id = 1;
----
QUERY PLAN
IndexScan { index: __pk_id }
  predicates_pushed: [id]
  indexes_considered: [e_ab: first column not in WHERE, e_a: first column not in WHERE, e_s: first column not in WHERE, e_id: lower selectivity than chosen index]

# Explaining a write refuses what reading the statement refuses, never what only its rows would:
# a SELECT that stands for one value is explained, though running it over these rows is refused.
EXPLAIN UPDATE t SET tag = (SELECT tag FROM t) WHERE id = 1;
SELECT id, name, tag FROM t ORDER BY id;
----
id | name | tag
1 | one | 1
2 | two | 2
7 | seven | 3

EXPLAIN DELETE FROM t WHERE id IN (SELECT name FROM t);
----
error: PlanError

EXPLAIN INSERT INTO t (id, name) VALUES (9, name);
----
error: ColumnNotFound

EXPLAIN UPDATE t SET name = nope WHERE id = 1;
----
error: ColumnNotFound

EXPLAIN UPDATE t SET nope = 1 WHERE id = 1;
----
error: ColumnNotFound

EXPLAIN INSERT INTO t (id, name) VALUES (1);
----
error: PlanError

EXPLAIN INSERT INTO t (id, name, tag) VALUES (9, 'n', 1) ON CONFLICT (tag) DO UPDATE SET name = 'x';
----
error: PlanError
