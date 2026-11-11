# The clauses of a VECTOR column that say how its vectors are kept and when they are grouped for
# search: WITH (quantization), AUTO_INDEX_AT and CONSOLIDATION. Each is declared alone and all
# together in the one order they are written in, after which SHOW VECTOR_INDEXES gives back what
# was declared and what is in force. A clause not written is NULL as declared and its default
# in force: 100000 vectors, and regrouping at 20 percent changed or 10 percent removed.

CREATE TABLE a_reduced (id INTEGER PRIMARY KEY, v VECTOR(3) WITH (quantization = 'SQ8'));
CREATE TABLE b_sized (id INTEGER PRIMARY KEY, v VECTOR(3) AUTO_INDEX_AT 5000);
CREATE TABLE c_both (id INTEGER PRIMARY KEY, v VECTOR(3) CONSOLIDATION (CHANGE_PERCENT = 25, TOMBSTONE_PERCENT = 5));
CREATE TABLE d_change (id INTEGER PRIMARY KEY, v VECTOR(3) CONSOLIDATION (CHANGE_PERCENT = 25));
CREATE TABLE e_never (id INTEGER PRIMARY KEY, v VECTOR(3) CONSOLIDATION NONE);
CREATE TABLE f_whole (
  id INTEGER PRIMARY KEY,
  tenant TEXT NOT NULL,
  v VECTOR(3) WITH (quantization = 'SQ8') PARTITION_KEY (tenant) MAX_PARTITIONS 4 SEARCH_MODE EXACT
    AUTO_INDEX_AT 5000 CONSOLIDATION (CHANGE_PERCENT = 25, TOMBSTONE_PERCENT = 5)
);

SHOW VECTOR_INDEXES;
----
table | column | dimension | quantization | vector_count | bytes | partition_key_columns | max_partitions | live_partitions | search_mode | declared_auto_index_at | effective_auto_index_at | declared_consolidation_mode | declared_consolidation_change_percent | declared_consolidation_tombstone_percent | effective_consolidation_mode | effective_consolidation_change_percent | effective_consolidation_tombstone_percent | pending_inserts | tombstones | durable_vector_bytes | durable_index_bytes | indexed_available | maintenance_state | maintenance_vectors_total | maintenance_vectors_done | maintenance_vectors_remaining | unavailable_partitions | stalled_partitions | broad_route | recovery_action
a_reduced | v | 3 | SQ8 | 0 | 0 | [] | NULL | 0 | AUTO | NULL | 100000 | NULL | NULL | NULL | PERCENT | 20 | 10 | 0 | 0 | 0 | 0 | true | idle | 0 | 0 | 0 | 0 | 0 | fan_out | none
b_sized | v | 3 | F32 | 0 | 0 | [] | NULL | 0 | AUTO | 5000 | 5000 | NULL | NULL | NULL | PERCENT | 20 | 10 | 0 | 0 | 0 | 0 | true | idle | 0 | 0 | 0 | 0 | 0 | fan_out | none
c_both | v | 3 | F32 | 0 | 0 | [] | NULL | 0 | AUTO | NULL | 100000 | PERCENT | 25 | 5 | PERCENT | 25 | 5 | 0 | 0 | 0 | 0 | true | idle | 0 | 0 | 0 | 0 | 0 | fan_out | none
d_change | v | 3 | F32 | 0 | 0 | [] | NULL | 0 | AUTO | NULL | 100000 | PERCENT | 25 | NULL | PERCENT | 25 | 10 | 0 | 0 | 0 | 0 | true | idle | 0 | 0 | 0 | 0 | 0 | fan_out | none
e_never | v | 3 | F32 | 0 | 0 | [] | NULL | 0 | AUTO | NULL | 100000 | NONE | NULL | NULL | NONE | NULL | NULL | 0 | 0 | 0 | 0 | true | idle | 0 | 0 | 0 | 0 | 0 | fan_out | none
f_whole | v | 3 | SQ8 | 0 | 0 | ["tenant"] | 4 | 0 | EXACT | 5000 | 5000 | PERCENT | 25 | 5 | PERCENT | 25 | 5 | 0 | 0 | 0 | 0 | true | idle | 0 | 0 | 0 | 0 | 0 | fan_out | none

# What a declaration refuses, each by the name of its kind. A way of keeping a vector that is
# neither F32 nor SQ8,
CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) WITH (quantization = 'PQ'));
----
error: InvalidVectorPolicyDeclaration

# an AUTO_INDEX_AT that is not a positive number, or is past the largest the engine keeps,
CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) AUTO_INDEX_AT 0);
----
error: InvalidVectorPolicyDeclaration

CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) AUTO_INDEX_AT -1);
----
error: InvalidVectorPolicyDeclaration

CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) AUTO_INDEX_AT 99999999999999999999);
----
error: InvalidVectorPolicyDeclaration

# and a percentage that is not a whole number from 1 to 100.
CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) CONSOLIDATION (CHANGE_PERCENT = 0));
----
error: InvalidVectorPolicyDeclaration

CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) CONSOLIDATION (TOMBSTONE_PERCENT = 101));
----
error: InvalidVectorPolicyDeclaration

# SQ4, IVF (...) and SEARCH (...) are read by a later version: refused by name, not as a syntax
# error.
CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) WITH (quantization = 'SQ4'));
----
error: PlanError

CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) IVF (LISTS = 64, PROBES = 8));
----
error: PlanError

CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = 1000000));
----
error: PlanError

# A syntax error: a clause out of its order, and a CONSOLIDATION list that is empty, names a
# member twice or names another member.
CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) AUTO_INDEX_AT 5000 SEARCH_MODE EXACT);
----
error: ParseError

CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) CONSOLIDATION NONE AUTO_INDEX_AT 5000);
----
error: ParseError

CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) AUTO_INDEX_AT 5000 WITH (quantization = 'SQ8'));
----
error: ParseError

CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) CONSOLIDATION ());
----
error: ParseError

CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) CONSOLIDATION (CHANGE_PERCENT = 25, CHANGE_PERCENT = 30));
----
error: ParseError

CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) CONSOLIDATION (LISTS = 4));
----
error: ParseError

# A refused declaration declares nothing.
SELECT id FROM docs;
----
error: TableNotFound
