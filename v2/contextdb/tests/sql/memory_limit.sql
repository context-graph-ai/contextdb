# SET MEMORY_LIMIT declares the ceiling in bytes, or none, and SHOW MEMORY_LIMIT reports it with
# what is in use. The cells written * depend on the build and on the machine.

SHOW MEMORY_LIMIT;
----
limit | used | available | startup_ceiling | ceiling_refresh_failures
none | * | * | * | *

SET MEMORY_LIMIT 512M;
SHOW MEMORY_LIMIT;
----
limit | used | available | startup_ceiling | ceiling_refresh_failures
536870912 | * | * | * | *

SET MEMORY_LIMIT 1G;
SHOW MEMORY_LIMIT;
----
limit | used | available | startup_ceiling | ceiling_refresh_failures
1073741824 | * | * | * | *

# A ceiling of 8 EiB is one byte more than the largest whole number a column holds. It is shown
# as that largest number, never as a negative one.
SET MEMORY_LIMIT 8589934592G;
SHOW MEMORY_LIMIT;
----
limit | used | available | startup_ceiling | ceiling_refresh_failures
9223372036854775807 | * | * | * | *

SET MEMORY_LIMIT NONE;
SHOW MEMORY_LIMIT;
----
limit | used | available | startup_ceiling | ceiling_refresh_failures
none | * | * | * | *
