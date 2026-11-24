# SET MEMORY_LIMIT declares the ceiling in bytes, or none, and SHOW MEMORY_LIMIT reports it with
# what is in use. The cells written * depend on the build and on the machine.

# A store opened with no limit declared is under the default, a quarter of the machine's memory
# (tests/memory.rs checks the figure); 64 MiB is settable under it.
SHOW MEMORY_LIMIT;
----
limit | used | available | startup_ceiling | ceiling_refresh_failures  (read 0, compared 0)
* | * | * | * | *

SET MEMORY_LIMIT 64M;
SHOW MEMORY_LIMIT;
----
limit | used | available | startup_ceiling | ceiling_refresh_failures  (read 0, compared 0)
67108864 | * | * | * | *

SET MEMORY_LIMIT 512M;
SHOW MEMORY_LIMIT;
----
limit | used | available | startup_ceiling | ceiling_refresh_failures  (read 0, compared 0)
536870912 | * | * | * | *

SET MEMORY_LIMIT 1G;
SHOW MEMORY_LIMIT;
----
limit | used | available | startup_ceiling | ceiling_refresh_failures  (read 0, compared 0)
1073741824 | * | * | * | *

# A ceiling of 8 EiB is one byte more than the largest whole number a column holds. It is shown
# as that largest number, never as a negative one.
SET MEMORY_LIMIT 8589934592G;
SHOW MEMORY_LIMIT;
----
limit | used | available | startup_ceiling | ceiling_refresh_failures  (read 0, compared 0)
9223372036854775807 | * | * | * | *

# A ceiling no number holds is an invalid value.
SET MEMORY_LIMIT 99999999999999999999G;
----
error: PlanError | plan error: SET MEMORY_LIMIT has an invalid value

SET MEMORY_LIMIT 64MiB;
----
error: PlanError | plan error: SET MEMORY_LIMIT has an invalid value
