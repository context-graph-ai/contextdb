# ALTER TABLE: a column added, dropped and renamed on a table that has rows, the options of a
# table changed after it was declared, and the two clauses of a VECTOR column that may change.
# A refused ALTER changes nothing. What is left in the store after a drop, and how an altered
# table is printed back, is checked by `tests/alter.rs`.

CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER);
INSERT INTO t (id, a, b) VALUES (1, 'one', 10), (2, 'two', 20);

# A column is added with or without the word COLUMN. A row written before reads NULL in it, or
# the column's default when it declares one, and every other column is as it was.
ALTER TABLE t ADD COLUMN c TEXT;
ALTER TABLE t ADD d INTEGER DEFAULT 7;
ALTER TABLE t ADD COLUMN e TEXT NOT NULL DEFAULT 'none';
SELECT id, a, b, c, d, e FROM t ORDER BY id;
----
id | a | b | c | d | e
1 | one | 10 | NULL | 7 | none
2 | two | 20 | NULL | 7 | none

# A row written after is written like any row, and a filter reads an older row's default.
INSERT INTO t (id, a) VALUES (3, 'three');
UPDATE t SET c = 'set' WHERE id = 1;
SELECT id, c, d, e FROM t ORDER BY id;
----
id | c | d | e
1 | set | 7 | none
2 | NULL | 7 | none
3 | NULL | 7 | none

SELECT id FROM t WHERE d = 7 AND c IS NULL ORDER BY id;
----
id
2
3

# A column that may hold no NULL needs a default when the table has rows.
ALTER TABLE t ADD COLUMN f TEXT NOT NULL;
----
error: SchemaInvalid

# A name the table has, a second key, and a table that is not there.
ALTER TABLE t ADD COLUMN a TEXT;
----
error: SchemaInvalid

ALTER TABLE t ADD COLUMN k INTEGER PRIMARY KEY;
----
error: SchemaInvalid

ALTER TABLE nope ADD COLUMN a TEXT;
----
error: TableNotFound

SELECT f FROM t;
----
error: ColumnNotFound

# An added column may carry a rule, and the rule holds from then on.
ALTER TABLE t ADD COLUMN slug TEXT UNIQUE;
UPDATE t SET slug = 'first' WHERE id = 1;
UPDATE t SET slug = 'first' WHERE id = 2;
----
error: UniqueViolation

# A column is dropped with or without the word COLUMN, and the others keep their values.
ALTER TABLE t DROP b;
ALTER TABLE t DROP COLUMN d RESTRICT;
SELECT b FROM t;
----
error: ColumnNotFound

SELECT id, a, c, e FROM t ORDER BY id;
----
id | a | c | e
1 | one | set | none
2 | two | NULL | none
3 | three | NULL | none

# A column an index names is not dropped unless the statement says CASCADE; RESTRICT is the
# default. CASCADE drops the index with the column.
CREATE INDEX t_by_c ON t (c, e);
ALTER TABLE t DROP COLUMN c;
----
error: ColumnInIndex

ALTER TABLE t DROP COLUMN c RESTRICT;
----
error: ColumnInIndex

SELECT id, c FROM t WHERE c = 'set';
----
id | c
1 | set

CREATE INDEX t_by_c_late ON t (a, c);
ALTER TABLE t DROP COLUMN c CASCADE;
----
dropped_indexes
t_by_c
t_by_c_late

DROP INDEX t_by_c ON t;
----
error: IndexNotFound

ALTER TABLE t ADD COLUMN spare TEXT;
ALTER TABLE t DROP COLUMN spare CASCADE;
----
rows affected: 0

SELECT c FROM t;
----
error: ColumnNotFound

# The name is free again, and no row reads what the dropped column held.
ALTER TABLE t ADD COLUMN c TEXT;
SELECT id, c FROM t ORDER BY id;
----
id | c
1 | NULL
2 | NULL
3 | NULL

# A column is renamed, and an index over it follows the name.
CREATE INDEX t_by_a ON t (a);
ALTER TABLE t RENAME COLUMN a TO title;
SELECT a FROM t;
----
error: ColumnNotFound

SELECT id, title FROM t WHERE title = 'two';
----
id | title
2 | two

ALTER TABLE t DROP COLUMN title;
----
error: ColumnInIndex

ALTER TABLE t RENAME COLUMN title TO e;
----
error: SchemaInvalid

ALTER TABLE t RENAME COLUMN nope TO other;
----
error: ColumnNotFound

# A column of the primary key is neither dropped nor renamed.
ALTER TABLE t DROP COLUMN id;
----
error: SchemaInvalid

ALTER TABLE t DROP COLUMN id CASCADE;
----
error: SchemaInvalid

ALTER TABLE t RENAME COLUMN id TO key;
----
error: SchemaInvalid

SELECT id, title, c, e, slug FROM t ORDER BY id;
----
id | title | c | e | slug
1 | one | NULL | none | first
2 | two | NULL | none | NULL
3 | three | NULL | none | NULL

# An IMMUTABLE column is neither dropped nor renamed; the column beside it is.
CREATE TABLE frozen (id INTEGER PRIMARY KEY, kind TEXT NOT NULL IMMUTABLE, note TEXT);
ALTER TABLE frozen DROP COLUMN kind;
----
error: ImmutableColumn

ALTER TABLE frozen DROP COLUMN kind CASCADE;
----
error: ImmutableColumn

ALTER TABLE frozen RENAME COLUMN kind TO sort;
----
error: ImmutableColumn

ALTER TABLE frozen DROP COLUMN note;

# A column of a VECTOR column's PARTITION_KEY is neither dropped nor renamed, with CASCADE or
# without, and no ALTER changes a partition key in place.
CREATE TABLE docs (
  id INTEGER PRIMARY KEY,
  tenant TEXT NOT NULL,
  v VECTOR(3) PARTITION_KEY (tenant) MAX_PARTITIONS 1 SEARCH_MODE INDEXED,
  loose VECTOR(3)
);
ALTER TABLE docs DROP COLUMN tenant;
----
error: VectorPartitionKeyInUse

ALTER TABLE docs DROP COLUMN tenant CASCADE;
----
error: VectorPartitionKeyInUse

ALTER TABLE docs RENAME COLUMN tenant TO owner;
----
error: VectorPartitionKeyInUse

ALTER TABLE docs ALTER COLUMN v SET PARTITION_KEY (id);
----
error: ParseError

ALTER TABLE docs ALTER COLUMN loose SET PARTITION_KEY (tenant);
----
error: ParseError

# The limit on partitions is raised, and the next partition is then accepted.
INSERT INTO docs (id, tenant, v) VALUES (1, 'north', [1.0, 0.0, 0.0]);
INSERT INTO docs (id, tenant, v) VALUES (2, 'south', [0.0, 1.0, 0.0]);
----
error: VectorPartitionLimitExceeded

ALTER TABLE docs ALTER COLUMN v SET MAX_PARTITIONS 2;
INSERT INTO docs (id, tenant, v) VALUES (2, 'south', [0.0, 1.0, 0.0]);
ALTER TABLE docs ALTER COLUMN v SET MAX_PARTITIONS 0;
----
error: InvalidVectorPartitionDeclaration

ALTER TABLE docs ALTER COLUMN v SET MAX_PARTITIONS -1;
----
error: InvalidVectorPartitionDeclaration

ALTER TABLE docs ALTER COLUMN loose SET MAX_PARTITIONS 4;
----
error: InvalidVectorPartitionDeclaration

# The column's search mode is changed, and the next search runs in it. A column too small to
# have a grouping has nothing for INDEXED to choose among, and INDEXED searches it as EXACT does.
SELECT id FROM docs ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 1;
----
id
1

ALTER TABLE docs ALTER COLUMN v SET SEARCH_MODE EXACT;
SELECT id FROM docs ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 1;
----
id
1

ALTER TABLE docs ALTER COLUMN v SET SEARCH_MODE INDEXED;
SELECT id FROM docs ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 1;
----
id
1

ALTER TABLE docs ALTER COLUMN v SET SEARCH_MODE AUTO;
SELECT id FROM docs ORDER BY v <=> [0.0, 1.0, 0.0] LIMIT 1;
----
id
2

ALTER TABLE docs ALTER COLUMN nope SET SEARCH_MODE EXACT;
----
error: ColumnNotFound

# The thresholds of the search index are changed online, and the next search and upkeep cycle
# read them: the declaration says what was written and what is in force, and DEFAULT takes the
# declaration away.
ALTER TABLE docs ALTER COLUMN v SET AUTO_INDEX_AT 5000;
ALTER TABLE docs ALTER COLUMN v SET CONSOLIDATION (CHANGE_PERCENT = 30, TOMBSTONE_PERCENT = 5);
SHOW VECTOR_INDEXES;
----
table | column | dimension | quantization | vector_count | bytes | partition_key_columns | max_partitions | live_partitions | search_mode | declared_auto_index_at | effective_auto_index_at | declared_consolidation_mode | declared_consolidation_change_percent | declared_consolidation_tombstone_percent | effective_consolidation_mode | effective_consolidation_change_percent | effective_consolidation_tombstone_percent | pending_inserts | tombstones | durable_vector_bytes | durable_index_bytes | indexed_available | maintenance_state | maintenance_vectors_total | maintenance_vectors_done | maintenance_vectors_remaining | unavailable_partitions | stalled_partitions | broad_route | recovery_action
docs | loose | * | * | * | * | * | * | * | * | NULL | 100000 | NULL | NULL | NULL | PERCENT | 20 | 10 | * | * | * | * | * | * | * | * | * | * | * | * | *
docs | v | * | * | * | * | * | * | * | * | 5000 | 5000 | PERCENT | 30 | 5 | PERCENT | 30 | 5 | * | * | * | * | * | * | * | * | * | * | * | * | *

ALTER TABLE docs ALTER COLUMN v SET AUTO_INDEX_AT DEFAULT;
ALTER TABLE docs ALTER COLUMN v SET CONSOLIDATION NONE;
SHOW VECTOR_INDEXES;
----
table | column | dimension | quantization | vector_count | bytes | partition_key_columns | max_partitions | live_partitions | search_mode | declared_auto_index_at | effective_auto_index_at | declared_consolidation_mode | declared_consolidation_change_percent | declared_consolidation_tombstone_percent | effective_consolidation_mode | effective_consolidation_change_percent | effective_consolidation_tombstone_percent | pending_inserts | tombstones | durable_vector_bytes | durable_index_bytes | indexed_available | maintenance_state | maintenance_vectors_total | maintenance_vectors_done | maintenance_vectors_remaining | unavailable_partitions | stalled_partitions | broad_route | recovery_action
docs | loose | * | * | * | * | * | * | * | * | NULL | 100000 | NULL | NULL | NULL | PERCENT | 20 | 10 | * | * | * | * | * | * | * | * | * | * | * | * | *
docs | v | * | * | * | * | * | * | * | * | NULL | 100000 | NONE | NULL | NULL | NONE | NULL | NULL | * | * | * | * | * | * | * | * | * | * | * | * | *

ALTER TABLE docs ALTER COLUMN v SET CONSOLIDATION DEFAULT;
SHOW VECTOR_INDEXES;
----
table | column | dimension | quantization | vector_count | bytes | partition_key_columns | max_partitions | live_partitions | search_mode | declared_auto_index_at | effective_auto_index_at | declared_consolidation_mode | declared_consolidation_change_percent | declared_consolidation_tombstone_percent | effective_consolidation_mode | effective_consolidation_change_percent | effective_consolidation_tombstone_percent | pending_inserts | tombstones | durable_vector_bytes | durable_index_bytes | indexed_available | maintenance_state | maintenance_vectors_total | maintenance_vectors_done | maintenance_vectors_remaining | unavailable_partitions | stalled_partitions | broad_route | recovery_action
docs | loose | * | * | * | * | * | * | * | * | NULL | 100000 | NULL | NULL | NULL | PERCENT | 20 | 10 | * | * | * | * | * | * | * | * | * | * | * | * | *
docs | v | * | * | * | * | * | * | * | * | NULL | 100000 | NULL | NULL | NULL | PERCENT | 20 | 10 | * | * | * | * | * | * | * | * | * | * | * | * | *

# A value CREATE TABLE refuses is refused here, and the declaration stays as it was.
ALTER TABLE docs ALTER COLUMN v SET AUTO_INDEX_AT 0;
----
error: InvalidVectorPolicyDeclaration

ALTER TABLE docs ALTER COLUMN v SET CONSOLIDATION (CHANGE_PERCENT = 101);
----
error: InvalidVectorPolicyDeclaration

# SEARCH and IVF tune what the lean index measures for itself: no version reads them.
ALTER TABLE docs ALTER COLUMN v SET SEARCH DEFAULT;
----
error: PlanError

ALTER TABLE docs ALTER COLUMN v SET IVF (LISTS = 64, PROBES = 8);
----
error: PlanError

# A VECTOR column is added with its whole declaration. A row written before has no vector and
# is in no search until it is given one.
CREATE TABLE passages (id INTEGER PRIMARY KEY, tenant TEXT NOT NULL, body TEXT);
INSERT INTO passages (id, tenant, body) VALUES (1, 'north', 'old');
ALTER TABLE passages ADD COLUMN v VECTOR(2) PARTITION_KEY (tenant) MAX_PARTITIONS 2 SEARCH_MODE EXACT;
SELECT id, v FROM passages ORDER BY id;
----
id | v
1 | NULL

SELECT id FROM passages ORDER BY v <=> [1.0, 0.0] LIMIT 5;
----
id

INSERT INTO passages (id, tenant, body, v) VALUES (2, 'north', 'new', [1.0, 0.0]);
SELECT id FROM passages ORDER BY v <=> [1.0, 0.0] LIMIT 5;
----
id
2

UPDATE passages SET v = [0.0, 1.0] WHERE id = 1;
SELECT id FROM passages ORDER BY v <=> [0.0, 1.0] LIMIT 5;
----
id
1
2

INSERT INTO passages (id, tenant, body, v) VALUES (3, 'south', 'new', [1.0, 0.0]);
INSERT INTO passages (id, tenant, body, v) VALUES (4, 'east', 'new', [1.0, 0.0]);
----
error: VectorPartitionLimitExceeded

# An added VECTOR column is refused what a declared one is.
ALTER TABLE passages ADD COLUMN w VECTOR(2) PARTITION_KEY (body);
----
error: InvalidVectorPartitionDeclaration

ALTER TABLE passages ADD COLUMN w VECTOR(0);
----
error: InvalidVectorPolicyDeclaration

SELECT w FROM passages;
----
error: ColumnNotFound

# How long rows are kept is set and dropped. SYNC SAFE needs a direction that delivers, when
# either of the two is set.
CREATE TABLE scratch (id INTEGER PRIMARY KEY, body TEXT);
INSERT INTO scratch (id, body) VALUES (1, 'kept');
ALTER TABLE scratch SET RETAIN 7 DAYS;
ALTER TABLE scratch SET RETAIN 24 HOURS SYNC SAFE;
ALTER TABLE scratch SET SYNC PULL ONLY;
----
error: SchemaInvalid

ALTER TABLE scratch SET SYNC OFF;
----
error: SchemaInvalid

ALTER TABLE scratch SET SYNC PUSH ONLY;
ALTER TABLE scratch DROP RETAIN;
ALTER TABLE scratch SET SYNC PULL ONLY;
ALTER TABLE scratch SET RETAIN 1 DAYS SYNC SAFE;
----
error: SchemaInvalid

ALTER TABLE scratch SET RETAIN 1 DAYS;
ALTER TABLE scratch SET SYNC OFF;
ALTER TABLE scratch SET SYNC TWO WAY;
ALTER TABLE scratch SET RETAIN 24 HOURS SYNC SAFE PUSH ONLY;
----
error: ParseError

# Which write wins, and how many versions are kept. Keeping only the current version is
# refused, by every statement that could bring it about, on a table that sends its rows out
# and keeps the first write.
ALTER TABLE scratch SET HISTORY CURRENT ONLY;
----
error: SchemaInvalid

ALTER TABLE scratch SET SYNC CONFLICT KEEP LATEST;
ALTER TABLE scratch SET HISTORY CURRENT ONLY;
ALTER TABLE scratch SET SYNC CONFLICT KEEP FIRST;
----
error: SchemaInvalid

ALTER TABLE scratch SET HISTORY ALL;
ALTER TABLE scratch SET SYNC CONFLICT KEEP FIRST;
ALTER TABLE scratch SET SYNC OFF;
ALTER TABLE scratch SET HISTORY CURRENT ONLY;
ALTER TABLE scratch SET SYNC TWO WAY;
----
error: SchemaInvalid

ALTER TABLE scratch SET SYNC PUSH ONLY;
----
error: SchemaInvalid

SELECT id, body FROM scratch;
----
id | body
1 | kept

# An IMMUTABLE table takes neither RETAIN nor HISTORY CURRENT ONLY later.
CREATE TABLE ledger (id INTEGER PRIMARY KEY, body TEXT) IMMUTABLE SYNC OFF;
ALTER TABLE ledger SET RETAIN 7 DAYS;
----
error: SchemaInvalid

ALTER TABLE ledger SET HISTORY CURRENT ONLY;
----
error: SchemaInvalid

ALTER TABLE ledger SET HISTORY ALL;

# The counts a table keeps are replaced whole by one statement, on a table that has rows.
CREATE TABLE events (
  id INTEGER PRIMARY KEY,
  context_id INTEGER NOT NULL,
  connector_id TEXT NOT NULL,
  UNIQUE (context_id, id),
  UNIQUE (context_id, connector_id, id)
);
INSERT INTO events (id, context_id, connector_id) VALUES (1, 1, 'a'), (2, 1, 'a'), (3, 1, 'b'), (4, 2, 'a');
ALTER TABLE events SET MAINTAIN COUNT (context_id);
SELECT COUNT(*) AS n FROM events WHERE context_id = 1;
----
n
3

ALTER TABLE events SET MAINTAIN COUNT (context_id), (context_id, connector_id);
SELECT COUNT(*) AS n FROM events WHERE context_id = 1 AND connector_id = 'a';
----
n
2

ALTER TABLE events SET MAINTAIN COUNT ALL PREFIXES;
ALTER TABLE events SET MAINTAIN COUNT NONE;
ALTER TABLE events SET MAINTAIN COUNT DEFAULT;
SELECT COUNT(*) AS n FROM events WHERE context_id = 1;
----
n
3

ALTER TABLE events SET MAINTAIN COUNT (connector_id);
----
error: SchemaInvalid

ALTER TABLE events SET MAINTAIN COUNT (id);
----
error: SchemaInvalid

# A column named like a clause is still a column: it is added, renamed and dropped by its name.
CREATE TABLE words (id INTEGER PRIMARY KEY, state TEXT, retain TEXT);
ALTER TABLE words ADD COLUMN history TEXT;
ALTER TABLE words RENAME COLUMN state TO sync;
ALTER TABLE words DROP COLUMN retain;
INSERT INTO words (id, sync, history) VALUES (1, 'a', 'b');
SELECT id, sync, history FROM words;
----
id | sync | history
1 | a | b
