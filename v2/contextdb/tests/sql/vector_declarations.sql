# What the declaration of a VECTOR column refuses, each by the name of its kind. A refused
# CREATE TABLE declares nothing: after each group the table is still not there.

# A PARTITION_KEY column is a column of the same table,
CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) PARTITION_KEY (nope));
----
error: InvalidVectorPartitionDeclaration

# declared NOT NULL,
CREATE TABLE docs (id INTEGER PRIMARY KEY, tenant TEXT, v VECTOR(3) PARTITION_KEY (tenant));
----
error: InvalidVectorPartitionDeclaration

# of a type whose values are equal or not and nothing between: not REAL, JSON or VECTOR,
CREATE TABLE docs (id INTEGER PRIMARY KEY, weight REAL NOT NULL, v VECTOR(3) PARTITION_KEY (weight));
----
error: InvalidVectorPartitionDeclaration

CREATE TABLE docs (id INTEGER PRIMARY KEY, meta JSON NOT NULL, v VECTOR(3) PARTITION_KEY (meta));
----
error: InvalidVectorPartitionDeclaration

CREATE TABLE docs (id INTEGER PRIMARY KEY, other VECTOR(2) NOT NULL, v VECTOR(3) PARTITION_KEY (other));
----
error: InvalidVectorPartitionDeclaration

# named once,
CREATE TABLE docs (id INTEGER PRIMARY KEY, tenant TEXT NOT NULL, v VECTOR(3) PARTITION_KEY (tenant, tenant));
----
error: InvalidVectorPartitionDeclaration

# and not the vector column itself.
CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) PARTITION_KEY (v) NOT NULL);
----
error: InvalidVectorPartitionDeclaration

SELECT id FROM docs;
----
error: TableNotFound

# MAX_PARTITIONS needs a PARTITION_KEY, and is a whole number from 1 to the largest the engine
# keeps.
CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) MAX_PARTITIONS 4);
----
error: InvalidVectorPartitionDeclaration

CREATE TABLE docs (id INTEGER PRIMARY KEY, tenant TEXT NOT NULL, v VECTOR(3) PARTITION_KEY (tenant) MAX_PARTITIONS 0);
----
error: InvalidVectorPartitionDeclaration

CREATE TABLE docs (id INTEGER PRIMARY KEY, tenant TEXT NOT NULL, v VECTOR(3) PARTITION_KEY (tenant) MAX_PARTITIONS -1);
----
error: InvalidVectorPartitionDeclaration

CREATE TABLE docs (id INTEGER PRIMARY KEY, tenant TEXT NOT NULL, v VECTOR(3) PARTITION_KEY (tenant) MAX_PARTITIONS 99999999999);
----
error: InvalidVectorPartitionDeclaration

# A vector has at least one number.
CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(0));
----
error: InvalidVectorPolicyDeclaration

SELECT id FROM docs;
----
error: TableNotFound

# The clauses that describe or steer an index are read by a later version. Each is refused by
# its name, alone or after the clauses this version reads, and never as a syntax error.
CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = 1000000));
----
error: PlanError

CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) IVF (LISTS = 64, PROBES = 8));
----
error: PlanError

CREATE TABLE docs (
  id INTEGER PRIMARY KEY,
  tenant TEXT NOT NULL,
  v VECTOR(3) PARTITION_KEY (tenant) MAX_PARTITIONS 4 SEARCH_MODE AUTO IVF (LISTS = 64, PROBES = 8)
);
----
error: PlanError

SELECT id FROM docs;
----
error: TableNotFound

# The name was free all along. The column is declared with room for one partition and searched
# INDEXED; ALTER changes each: searched EXACT, an ordinary search answers, and with the limit
# raised a second partition is accepted.
CREATE TABLE docs (
  id INTEGER PRIMARY KEY,
  tenant TEXT NOT NULL,
  v VECTOR(3) PARTITION_KEY (tenant) MAX_PARTITIONS 1 SEARCH_MODE INDEXED
);
ALTER TABLE docs ALTER COLUMN v SET SEARCH_MODE EXACT;
----
rows affected: 0

ALTER TABLE docs ALTER COLUMN v SET MAX_PARTITIONS 512;
----
rows affected: 0

INSERT INTO docs (id, tenant, v) VALUES (1, 'north', [1.0, 0.0, 0.0]);
INSERT INTO docs (id, tenant, v) VALUES (2, 'south', [0.0, 1.0, 0.0]);
----
rows affected: 1

SELECT id FROM docs ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 1;
----
id
1

SELECT id FROM docs ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR EXACT LIMIT 1;
----
id
1
