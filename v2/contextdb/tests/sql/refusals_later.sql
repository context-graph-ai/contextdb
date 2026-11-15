# What this version does not read is refused by name when the statement is planned: no table
# named here exists, and none is looked up.

# The clauses that describe or steer a vector index.
CREATE TABLE q (id INTEGER PRIMARY KEY, v VECTOR(3) SEARCH (candidates = 64));
----
error: PlanError

CREATE TABLE q (id INTEGER PRIMARY KEY, v VECTOR(3) IVF (lists = 16));
----
error: PlanError

# After the clauses this version reads, they are still refused by name and not as a syntax error.
CREATE TABLE q (id INTEGER PRIMARY KEY, tenant TEXT NOT NULL, v VECTOR(3) PARTITION_KEY (tenant) MAX_PARTITIONS 4 SEARCH_MODE AUTO IVF (LISTS = 64, PROBES = 8));
----
error: PlanError

SELECT id FROM q ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR INDEXED (candidates = 64) LIMIT 1;
----
error: PlanError

# Triggers, events and their delivery, removal by age, and the store's own settings.
CREATE TRIGGER on_note ON notes WHEN INSERT;
----
error: PlanError

DROP TRIGGER on_note;
----
error: PlanError

CREATE EVENT TYPE note_added WHEN INSERT ON notes;
----
error: PlanError

CREATE SINK audit;
----
error: PlanError

CREATE ROUTE note_added TO audit;
----
error: PlanError

DROP ROUTE note_added TO audit;
----
error: PlanError

CREATE SCHEDULE nightly;
----
error: PlanError

DROP SCHEDULE nightly;
----
error: PlanError

PURGE FROM notes WHERE id = 1;
----
error: PlanError

DISCARD FROM notes WHERE id = 1;
----
error: PlanError

SET DISK_LIMIT '1G';
----
error: PlanError

SHOW DISK_LIMIT;
----
error: PlanError

SET MAINTENANCE_POLL_INTERVAL 5;
----
error: PlanError

SET READ_CACHE_SHARE 50;
----
error: PlanError

SET STORAGE_COMPACTION ON;
----
error: PlanError

CREATE TABLE q (id INTEGER PRIMARY KEY, until TIMESTAMP EXPIRES);
----
error: PlanError

CREATE TABLE q (id INTEGER PRIMARY KEY, parent INTEGER) FOLLOWS PARENT PURGE;
----
error: PlanError

# What belongs to several tenants and to sync.
DECLARE TENANT TABLE POLICY notes;
----
error: PlanError

SHOW TENANT TABLE POLICY;
----
error: PlanError

SHOW SYNC BINDINGS;
----
error: PlanError

SHOW SYNC_CONFLICT_POLICY;
----
error: PlanError

SHOW DELIVERY OUTCOMES;
----
error: PlanError

# What no version reads, each by its own kind where it has one.
CREATE PROCEDURE tidy() AS 'DELETE FROM notes';
----
error: StoredProcNotSupported

CREATE FUNCTION twice(n INTEGER) AS 'n * 2';
----
error: StoredProcNotSupported

SELECT id FROM notes WHERE body MATCH 'word';
----
error: FullTextSearchNotSupported

WITH RECURSIVE up AS (SELECT id FROM notes) SELECT id FROM up;
----
error: RecursiveCteNotSupported

SELECT id FROM notes WHERE EXISTS (SELECT id FROM tags);
----
error: SubqueryNotSupported

SELECT id, RANK() OVER (ORDER BY id) AS r FROM notes;
----
error: WindowFunctionNotSupported

# A row value stands beside =, <> and IN, and nowhere else.
SELECT (id, id) FROM notes;
----
error: PlanError

SELECT id FROM notes INTERSECT SELECT id FROM tags;
----
error: PlanError

SELECT id FROM notes EXCEPT SELECT id FROM tags;
----
error: PlanError

INSERT INTO notes (id) SELECT id FROM tags;
----
error: PlanError

SELECT n.id FROM notes n RIGHT JOIN tags t ON n.id = t.id;
----
error: PlanError

SELECT n.id FROM notes n FULL JOIN tags t ON n.id = t.id;
----
error: PlanError

SELECT n.id FROM notes n, tags t;
----
error: PlanError

BEGIN;
----
error: PlanError

COMMIT;
----
error: PlanError

ROLLBACK;
----
error: PlanError
