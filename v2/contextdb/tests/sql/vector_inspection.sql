# SHOW VECTOR_INDEXES gives one row for each VECTOR column, in the order of table and column;
# SHOW VECTOR_PARTITIONS one row for each partition, in the order of table, column and key
# values. No maintenance cycle runs here, so every vector is pending, no block or grouping holds
# a byte, nothing is due, and INDEXED may be given every partition. Three columns: one with no
# key and no vector, one with a key and three partitions (row 5 has no vector), and one that
# keeps its vectors reduced to a byte a number.

CREATE TABLE a_plain (id INTEGER PRIMARY KEY, v VECTOR(3));
CREATE TABLE b_parts (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b INTEGER NOT NULL, v VECTOR(2) PARTITION_KEY (a, b));
INSERT INTO b_parts (id, a, b, v)
VALUES (4, 'y', 1, [0.0, 2.0]),
       (3, 'x', 2, [1.0, 0.0]),
       (1, 'x', 1, [1.0, 0.0]),
       (2, 'x', 1, [0.0, 1.0]),
       (5, 'x', 1, NULL);
CREATE TABLE c_small (id INTEGER PRIMARY KEY, v VECTOR(4) WITH (quantization = 'SQ8'));
INSERT INTO c_small (id, v) VALUES (1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0]);

SHOW VECTOR_INDEXES;
----
table | column | dimension | quantization | vector_count | bytes | partition_key_columns | max_partitions | live_partitions | search_mode | declared_auto_index_at | effective_auto_index_at | declared_consolidation_mode | declared_consolidation_change_percent | declared_consolidation_tombstone_percent | effective_consolidation_mode | effective_consolidation_change_percent | effective_consolidation_tombstone_percent | pending_inserts | tombstones | durable_vector_bytes | durable_index_bytes | indexed_available | maintenance_state | maintenance_vectors_total | maintenance_vectors_done | maintenance_vectors_remaining | unavailable_partitions | stalled_partitions | broad_route | recovery_action
a_plain | v | 3 | F32 | 0 | 0 | [] | NULL | 0 | AUTO | NULL | 100000 | NULL | NULL | NULL | PERCENT | 20 | 10 | 0 | 0 | 0 | 0 | true | idle | 0 | 0 | 0 | 0 | 0 | fan_out | none
b_parts | v | 2 | F32 | 4 | 0 | ["a","b"] | 256 | 3 | AUTO | NULL | 100000 | NULL | NULL | NULL | PERCENT | 20 | 10 | 4 | 0 | 0 | 0 | true | idle | 0 | 0 | 0 | 0 | 0 | fan_out | none
c_small | v | 4 | SQ8 | 2 | 0 | [] | NULL | 1 | AUTO | NULL | 100000 | NULL | NULL | NULL | PERCENT | 20 | 10 | 2 | 0 | 0 | 0 | true | idle | 0 | 0 | 0 | 0 | 0 | fan_out | none

# A column with no key has one partition, there before its first vector.
SHOW VECTOR_PARTITIONS;
----
table | column | partition_key | live_rows | pending_inserts | tombstones | durable_vector_bytes | durable_index_bytes | indexed_available | availability_reason | maintenance_state | maintenance_reason | maintenance_vectors_total | maintenance_vectors_done | maintenance_vectors_remaining | lists | probes | recovery_action
a_plain | v | {} | 0 | 0 | 0 | 0 | 0 | true | none | idle | none | 0 | 0 | 0 | NULL | NULL | none
b_parts | v | {"a":"x","b":1} | 2 | 2 | 0 | 0 | 0 | true | none | idle | none | 0 | 0 | 0 | NULL | NULL | none
b_parts | v | {"a":"x","b":2} | 1 | 1 | 0 | 0 | 0 | true | none | idle | none | 0 | 0 | 0 | NULL | NULL | none
b_parts | v | {"a":"y","b":1} | 1 | 1 | 0 | 0 | 0 | true | none | idle | none | 0 | 0 | 0 | NULL | NULL | none
c_small | v | {} | 2 | 2 | 0 | 0 | 0 | true | none | idle | none | 0 | 0 | 0 | NULL | NULL | none

SHOW VECTOR_PARTITIONS FOR b_parts.v;
----
table | column | partition_key | live_rows | pending_inserts | tombstones | durable_vector_bytes | durable_index_bytes | indexed_available | availability_reason | maintenance_state | maintenance_reason | maintenance_vectors_total | maintenance_vectors_done | maintenance_vectors_remaining | lists | probes | recovery_action
b_parts | v | {"a":"x","b":1} | 2 | 2 | 0 | 0 | 0 | true | none | idle | none | 0 | 0 | 0 | NULL | NULL | none
b_parts | v | {"a":"x","b":2} | 1 | 1 | 0 | 0 | 0 | true | none | idle | none | 0 | 0 | 0 | NULL | NULL | none
b_parts | v | {"a":"y","b":1} | 1 | 1 | 0 | 0 | 0 | true | none | idle | none | 0 | 0 | 0 | NULL | NULL | none

# LIMIT cuts the list with FOR or without; OFFSET, after LIMIT only, skips rows first.
SHOW VECTOR_PARTITIONS LIMIT 2;
----
table | column | partition_key | live_rows | pending_inserts | tombstones | durable_vector_bytes | durable_index_bytes | indexed_available | availability_reason | maintenance_state | maintenance_reason | maintenance_vectors_total | maintenance_vectors_done | maintenance_vectors_remaining | lists | probes | recovery_action
a_plain | v | {} | 0 | 0 | 0 | 0 | 0 | true | none | idle | none | 0 | 0 | 0 | NULL | NULL | none
b_parts | v | {"a":"x","b":1} | 2 | 2 | 0 | 0 | 0 | true | none | idle | none | 0 | 0 | 0 | NULL | NULL | none

SHOW VECTOR_PARTITIONS FOR b_parts.v LIMIT 2 OFFSET 1;
----
table | column | partition_key | live_rows | pending_inserts | tombstones | durable_vector_bytes | durable_index_bytes | indexed_available | availability_reason | maintenance_state | maintenance_reason | maintenance_vectors_total | maintenance_vectors_done | maintenance_vectors_remaining | lists | probes | recovery_action
b_parts | v | {"a":"x","b":2} | 1 | 1 | 0 | 0 | 0 | true | none | idle | none | 0 | 0 | 0 | NULL | NULL | none
b_parts | v | {"a":"y","b":1} | 1 | 1 | 0 | 0 | 0 | true | none | idle | none | 0 | 0 | 0 | NULL | NULL | none

# Refused by name: a table that is not there, and a column that is not a VECTOR column.
SHOW VECTOR_PARTITIONS FOR nope.v;
----
error: TableNotFound

SHOW VECTOR_PARTITIONS FOR b_parts.a;
----
error: UnknownVectorIndex
