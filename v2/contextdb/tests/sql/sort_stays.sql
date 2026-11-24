# A sort is dropped only when the rows arrive in that order. Over a list of values, a list of
# alternatives or a sort column the filter pins to alternatives, the rows of the runs are not
# in the order asked, and the sort stays.

CREATE TABLE ab (a INTEGER, b INTEGER, PRIMARY KEY (a, b));
INSERT INTO ab (a, b) VALUES (1, 5), (2, 3);

SELECT a, b FROM ab WHERE a IN (1, 2) ORDER BY b;
----
a | b  (read 2, compared 0)
2 | 3
1 | 5

SELECT a, b FROM ab WHERE a = 1 OR a = 2 ORDER BY b;
----
a | b  (read 2, compared 0)
2 | 3
1 | 5

CREATE TABLE ab2 (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL);
CREATE INDEX ab2_ab ON ab2 (a, b);
INSERT INTO ab2 (id, a, b) VALUES (1, 1, 5), (2, 2, 3);

SELECT a, b FROM ab2 WHERE a IN (1, 2) ORDER BY b;
----
a | b  (read 2, compared 0)
2 | 3
1 | 5

CREATE TABLE fb (id INTEGER PRIMARY KEY, flag BOOLEAN NOT NULL, at INTEGER NOT NULL);
CREATE INDEX fb_flag_at ON fb (flag, at);
INSERT INTO fb (id, flag, at) VALUES (1, FALSE, 9), (2, TRUE, 1);

SELECT id, at FROM fb WHERE flag IN (FALSE, TRUE) ORDER BY at;
----
id | at  (read 2, compared 0)
2 | 1
1 | 9

CREATE TABLE abc (a INTEGER, b INTEGER, c INTEGER, PRIMARY KEY (a, b, c));
INSERT INTO abc (a, b, c) VALUES (1, 5, 9), (1, 6, 2);

SELECT b, c FROM abc WHERE a = 1 AND b IN (5, 6) ORDER BY c;
----
b | c  (read 2, compared 0)
6 | 2
5 | 9

CREATE TABLE oa (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL);
INSERT INTO oa (id, a, b) VALUES (1, 2, 0), (2, 1, 6);

SELECT id, a FROM oa WHERE a = 2 OR (a = 1 AND b = 6) ORDER BY a;
----
id | a  (read 2, compared 0)
2 | 1
1 | 2

CREATE INDEX oa_a ON oa (a);

SELECT id, a FROM oa WHERE a = 2 OR (a = 1 AND b = 6) ORDER BY a DESC;
----
id | a  (read 2, compared 0)
1 | 2
2 | 1

SELECT id, a FROM oa WHERE a = 2 OR (a = 1 AND b = 6) ORDER BY a, id;
----
id | a  (read 2, compared 0)
2 | 1
1 | 2

# An index on (source, context, type) reaches its rows in the key's order after its pinned
# values: one value of each is no sort, in either direction, and two values of one are.
CREATE TABLE links (id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL, target_id INTEGER NOT NULL, edge_type TEXT NOT NULL, context_id INTEGER NOT NULL);
CREATE INDEX links_sce ON links (source_id, context_id, edge_type);
INSERT INTO links (id, source_id, target_id, edge_type, context_id)
VALUES (4, 1, 40, 'x', 7), (2, 1, 20, 'x', 7), (3, 1, 30, 'y', 7), (1, 1, 10, 'x', 7), (5, 1, 50, 'x', 8);

SELECT target_id FROM links WHERE source_id = 1 AND edge_type = 'x' AND context_id = 7 ORDER BY id;
----
target_id  (read 3, compared 0)
10
20
40

SELECT target_id FROM links WHERE source_id = 1 AND edge_type = 'x' AND context_id = 7 ORDER BY id DESC;
----
target_id  (read 3, compared 0)
40
20
10

EXPLAIN SELECT target_id FROM links WHERE source_id = 1 AND edge_type = 'x' AND context_id = 7 ORDER BY id;
----
QUERY PLAN  (read 0, compared 0)
IndexScan { index: links_sce }
  predicates_pushed: [source_id, context_id, edge_type]
  indexes_considered: [__pk_id: first column not in WHERE]

EXPLAIN SELECT target_id FROM links WHERE source_id = 1 AND edge_type = 'x' AND context_id = 7 ORDER BY id DESC;
----
QUERY PLAN  (read 0, compared 0)
IndexScan Backward { index: links_sce }
  predicates_pushed: [source_id, context_id, edge_type]
  indexes_considered: [__pk_id: first column not in WHERE]

SELECT target_id FROM links WHERE source_id = 1 AND edge_type IN ('x', 'y') AND context_id = 7 ORDER BY id;
----
target_id  (read 4, compared 0)
10
20
30
40

# The first row of an order the index gives is one row read, from either end, with no sort.
SELECT target_id FROM links WHERE source_id = 1 AND edge_type = 'x' AND context_id = 7 ORDER BY id LIMIT 1;
----
target_id  (read 1, compared 0)
10

SELECT target_id FROM links WHERE source_id = 1 AND edge_type = 'x' AND context_id = 7 ORDER BY id DESC LIMIT 1;
----
target_id  (read 1, compared 0)
40
