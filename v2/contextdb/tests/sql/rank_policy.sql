# USE RANK orders a search by the column's formula, before the LIMIT. From [1, 0]: task 1 ranks 1
# (result 10), task 2 ranks 5 (no result), task 3 ranks 3 (result 11, failure), task 4 ranks 1.5
# (results 12 and 13: the lowest key, 12, is used), task 5 ranks 1 (no result), task 6 has none.
CREATE TABLE results (id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL, success BOOLEAN NOT NULL);
CREATE INDEX results_task_id_idx ON results (task_id);
CREATE TABLE tasks (
  id INTEGER PRIMARY KEY,
  confidence REAL,
  v VECTOR(2) RANK_POLICY (
    JOIN results ON task_id,
    FORMULA '{confidence} + {vector_score} * coalesce({success}, 1.0)',
    SORT_KEY effective_confidence
  )
);
INSERT INTO tasks (id, confidence, v)
VALUES (6, NULL, [0.0, 1.0]), (3, 3.0, [-1.0, 0.0]), (5, 1.0, [0.0, 1.0]),
       (1, 0.0, [1.0, 0.0]), (4, 0.5, [2.0, 0.0]), (2, 5.0, [0.0, 1.0]);
INSERT INTO results (id, task_id, success)
VALUES (13, 4, FALSE), (12, 4, TRUE), (11, 3, FALSE), (10, 1, TRUE);

SELECT id FROM tasks ORDER BY v <=> [1.0, 0.0] LIMIT 6;
----
id
1
4
2
5
6
3

# Highest rank first, equal ranks (1 and 5) in key order, the task with no rank last.
SELECT id FROM tasks ORDER BY v <=> [1.0, 0.0] USE RANK effective_confidence LIMIT 6;
----
id
2
3
4
1
5
6

# The best are taken before the LIMIT and OFFSET, not from the nearest.
SELECT id FROM tasks ORDER BY v <=> [1.0, 0.0] USE RANK effective_confidence LIMIT 2 OFFSET 2;
----
id
4
1

# No rank or score column is added.
SELECT * FROM tasks ORDER BY v <=> [1.0, 0.0] USE RANK effective_confidence LIMIT 1;
----
id | confidence | v
2 | 5.0 | [0.0, 1.0]

SELECT id FROM tasks ORDER BY id USE RANK effective_confidence LIMIT 2;
----
error: UseRankRequiresVectorOrder

SELECT id FROM tasks ORDER BY v <=> [1.0, 0.0] USE RANK effective_confidence;
----
error: UseRankRequiresLimit

SELECT id FROM tasks ORDER BY v <=> [1.0, 0.0] USE RANK nothing LIMIT 2;
----
error: RankPolicyNotFound

SELECT id FROM tasks ORDER BY v <=> ROW_VECTOR('tasks', 'v', 1) LIMIT 2;
----
error: PlanError

# Task 1's lowest-key result (20, success) ranks it, though the index also holds success.
CREATE TABLE runs (id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL, success BOOLEAN NOT NULL);
CREATE INDEX runs_task_idx ON runs (task_id, success);
CREATE TABLE jobs (id INTEGER PRIMARY KEY, v VECTOR(2) RANK_POLICY (
  JOIN runs ON task_id, FORMULA '{vector_score} * coalesce({success}, 1.0)', SORT_KEY s));
INSERT INTO jobs (id, v) VALUES (1, [1.0, 0.0]), (2, [1.0, 1.0]);
INSERT INTO runs (id, task_id, success) VALUES (21, 1, FALSE), (20, 1, TRUE);

SELECT id FROM jobs ORDER BY v <=> [1.0, 0.0] USE RANK s LIMIT 2;
----
id
1
2
