# Defaults and declarations retain the old setting/value/source form.
SHOW STORAGE_COMPACTION;
----
setting | value | source  (read 0, compared 0)
start_ratio | 0.05 | default
stop_ratio | 0.02 | default
min_interval | 60000 | default

SET STORAGE_COMPACTION START_RATIO 0.2;
SET STORAGE_COMPACTION STOP_RATIO 0.1;
SET STORAGE_COMPACTION START_RATIO 1.5;
----
error: PlanError

SHOW STORAGE_COMPACTION START_RATIO;
----
setting | value | source  (read 0, compared 0)
start_ratio | 0.2 | declared

SHOW STORAGE_COMPACTION STOP_RATIO;
----
setting | value | source  (read 0, compared 0)
stop_ratio | 0.1 | declared

SET STORAGE_COMPACTION MIN_INTERVAL 10 MINUTES;
SHOW STORAGE_COMPACTION MIN_INTERVAL;
----
milliseconds  (read 0, compared 0)
600000

# Bytes are physical record bytes; a CURRENT ONLY table keeps no versions; totals add the six parts.
CREATE TABLE empty (id INTEGER PRIMARY KEY);
CREATE TABLE sized (id INTEGER PRIMARY KEY, n INTEGER) HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST;
CREATE INDEX by_n ON sized (n);
INSERT INTO sized VALUES (1, 2);
SHOW CONTENT_SIZE;
----
table | rows | large_values | indexes | vectors | blocks | versions | total  (read 0, compared 0)
empty | 0 | 0 | 0 | 0 | 0 | 0 | 0
sized | 31 | 0 | 26 | 0 | 0 | 0 | 57

DELETE FROM sized;
SHOW CONTENT_SIZE;
----
table | rows | large_values | indexes | vectors | blocks | versions | total  (read 0, compared 0)
empty | 0 | 0 | 0 | 0 | 0 | 0 | 0
sized | 0 | 0 | 0 | 0 | 0 | 0 | 0

SHOW MAINTENANCE_POLL_INTERVAL;
----
milliseconds  (read 0, compared 0)
60000

SET MAINTENANCE_POLL_INTERVAL '5 Seconds';
SHOW MAINTENANCE_POLL_INTERVAL;
----
milliseconds  (read 0, compared 0)
5000

SET MAINTENANCE_POLL_INTERVAL 250 MILLISECONDS;
SHOW MAINTENANCE_POLL_INTERVAL;
----
milliseconds  (read 0, compared 0)
250

SET MAINTENANCE_POLL_INTERVAL 5 SECONDS;
SHOW MAINTENANCE_POLL_INTERVAL;
----
milliseconds  (read 0, compared 0)
5000

SET MAINTENANCE_POLL_INTERVAL 0 SECONDS;
----
error: PlanError

SET MAINTENANCE_POLL_INTERVAL '0 seconds';
----
error: PlanError

SET MAINTENANCE_POLL_INTERVAL 0 SECONDS;
----
error: PlanError

SET DISK_LIMIT '1G';
SHOW DISK_LIMIT;
----
limit | used | available | startup_ceiling  (read 0, compared 0)
1073741824 | * | * | none

SET STORAGE_COMPACTION STOP_RATIO DEFAULT;
SHOW STORAGE_COMPACTION STOP_RATIO;
----
setting | value | source  (read 0, compared 0)
stop_ratio | 0.02 | default

SET DISK_LIMIT NONE;
SHOW DISK_LIMIT;
----
limit | used | available | startup_ceiling  (read 0, compared 0)
none | * | * | none
