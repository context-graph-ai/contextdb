# The clauses of a VECTOR column that say how its vectors are kept and when they are grouped for
# search: WITH (quantization), INDEXING_THRESHOLD and COMPACTION. Each is declared alone and all
# together in the one order they are written in, after which SHOW VECTOR_INDEXES gives back what
# was declared and what is in force. A clause not written is NULL as declared and its default
# in force: 100000 vectors, and regrouping at 20 percent changed or 10 percent removed.

CREATE TABLE a_reduced (id INTEGER PRIMARY KEY, v VECTOR(3) WITH (quantization = 'SQ8'));
CREATE TABLE b_sized (id INTEGER PRIMARY KEY, v VECTOR(3) INDEXING_THRESHOLD 5000);
CREATE TABLE c_both (id INTEGER PRIMARY KEY, v VECTOR(3) COMPACTION (CHANGE_PERCENT = 25, TOMBSTONE_PERCENT = 5));
CREATE TABLE d_change (id INTEGER PRIMARY KEY, v VECTOR(3) COMPACTION (CHANGE_PERCENT = 25));
CREATE TABLE e_never (id INTEGER PRIMARY KEY, v VECTOR(3) COMPACTION NONE);
CREATE TABLE f_whole (
  id INTEGER PRIMARY KEY,
  tenant TEXT NOT NULL,
  v VECTOR(3) WITH (quantization = 'SQ8') PARTITION_KEY (tenant) MAX_PARTITIONS 4 SEARCH_MODE EXACT
    INDEXING_THRESHOLD 5000 COMPACTION (CHANGE_PERCENT = 25, TOMBSTONE_PERCENT = 5)
);

SHOW VECTOR_INDEXES;
----
table | column | dimension | quantization | vector_count | bytes | partition_key_columns | max_partitions | live_partitions | search_mode | declared_indexing_threshold | effective_indexing_threshold | declared_probes | declared_compaction_mode | declared_compaction_change_percent | declared_compaction_tombstone_percent | effective_compaction_mode | effective_compaction_change_percent | effective_compaction_tombstone_percent | pending_tuples | tombstones | durable_vector_bytes | durable_index_bytes | indexed_available | maintenance_state | maintenance_vectors_total | maintenance_vectors_done | maintenance_vectors_remaining | unavailable_partitions | stalled_partitions | broad_route | recovery_action  (read 0, compared 0)
a_reduced | v | 3 | SQ8 | 0 | 0 | [] | NULL | 0 | AUTO | NULL | 100000 | NULL | NULL | NULL | NULL | PERCENT | 20 | 10 | 0 | 0 | 0 | 0 | true | idle | 0 | 0 | 0 | 0 | 0 | fan_out | none
b_sized | v | 3 | F32 | 0 | 0 | [] | NULL | 0 | AUTO | 5000 | 5000 | NULL | NULL | NULL | NULL | PERCENT | 20 | 10 | 0 | 0 | 0 | 0 | true | idle | 0 | 0 | 0 | 0 | 0 | fan_out | none
c_both | v | 3 | F32 | 0 | 0 | [] | NULL | 0 | AUTO | NULL | 100000 | NULL | PERCENT | 25 | 5 | PERCENT | 25 | 5 | 0 | 0 | 0 | 0 | true | idle | 0 | 0 | 0 | 0 | 0 | fan_out | none
d_change | v | 3 | F32 | 0 | 0 | [] | NULL | 0 | AUTO | NULL | 100000 | NULL | PERCENT | 25 | NULL | PERCENT | 25 | 10 | 0 | 0 | 0 | 0 | true | idle | 0 | 0 | 0 | 0 | 0 | fan_out | none
e_never | v | 3 | F32 | 0 | 0 | [] | NULL | 0 | AUTO | NULL | 100000 | NULL | NONE | NULL | NULL | NONE | NULL | NULL | 0 | 0 | 0 | 0 | true | idle | 0 | 0 | 0 | 0 | 0 | fan_out | none
f_whole | v | 3 | SQ8 | 0 | 0 | ["tenant"] | 4 | 0 | EXACT | 5000 | 5000 | NULL | PERCENT | 25 | 5 | PERCENT | 25 | 5 | 0 | 0 | 0 | 0 | true | idle | 0 | 0 | 0 | 0 | 0 | fan_out | none

# SQ4 is read by a later version: refused by name, not as a syntax error.
CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) WITH (quantization = 'SQ4'));
----
error: PlanError

# A syntax error: a clause out of its order, and a COMPACTION list that is empty, names a
# member twice or names another member.
CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) INDEXING_THRESHOLD 5000 SEARCH_MODE EXACT);
----
error: ParseError

CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) COMPACTION NONE INDEXING_THRESHOLD 5000);
----
error: ParseError

CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) INDEXING_THRESHOLD 5000 WITH (quantization = 'SQ8'));
----
error: ParseError

CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) COMPACTION ());
----
error: ParseError

CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) COMPACTION (CHANGE_PERCENT = 25, CHANGE_PERCENT = 30));
----
error: ParseError

CREATE TABLE docs (id INTEGER PRIMARY KEY, v VECTOR(3) COMPACTION (LISTS = 4));
----
error: ParseError

# A refused declaration declares nothing.
SELECT id FROM docs;
----
error: TableNotFound
