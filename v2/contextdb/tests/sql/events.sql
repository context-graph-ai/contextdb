# TRIGGER, EVENT TYPE, SINK, ROUTE and SCHEDULE, each a schema record kept with the tables. A name
# declared again otherwise is refused, so a second definition that succeeds shows the first gone.

CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);
CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT);

# A trigger names its table and change; INCLUDING SYNC is kept for the rows sync applies.
CREATE TRIGGER on_note ON notes WHEN INSERT;
CREATE TRIGGER on_note_edit ON notes WHEN UPDATE INCLUDING SYNC;

# An event type names a change of a table. A sink is a callback, or a webhook kept with its URL.
# A route sends an event type to a sink, for every row or the rows whose column holds a value.
CREATE EVENT TYPE note_added WHEN INSERT ON notes;
CREATE EVENT TYPE note_removed WHEN DELETE ON notes;
CREATE SINK audit TYPE callback;
CREATE SINK hook TYPE webhook URL 'https://example.invalid/hook';
CREATE ROUTE added_to_audit EVENT note_added TO audit;
CREATE ROUTE added_to_hook EVENT note_added TO hook WHERE body IN ('urgent', 'late');

# A schedule names its interval, its callback, and what is done with ticks missed while closed.
CREATE SCHEDULE hourly EVERY '1 hour' TX (tidy);
CREATE SCHEDULE often EVERY '30 seconds' TX (sweep) MISSED_TICK_POLICY 'catch-up' WITHIN 120 SECONDS;
CREATE SCHEDULE strict EVERY '5 minutes' TX (check) MISSED_TICK_POLICY 'fail-loud';
# A schedule declared again under its name replaces the first, with no drop before it.
CREATE SCHEDULE often EVERY '1 minute' TX (sweep);

# Names fold as a table's do: unquoted is lower-cased, quoted is as written.
CREATE ROUTE folded EVENT NOTE_ADDED TO Audit;
CREATE ROUTE quoted EVENT "Note_Added" TO audit;
----
error: SchemaInvalid

# Another definition under a declared name is refused, and the first stays.
CREATE TRIGGER on_note ON tags WHEN INSERT;
----
error: SchemaInvalid

CREATE EVENT TYPE note_added WHEN UPDATE ON notes;
----
error: SchemaInvalid

CREATE SINK hook TYPE webhook URL 'https://example.invalid/other';
----
error: SchemaInvalid

CREATE ROUTE added_to_audit EVENT note_removed TO audit;
----
error: SchemaInvalid

# What a declaration names must be declared.
CREATE TRIGGER on_nothing ON nope WHEN INSERT;
----
error: TableNotFound

CREATE EVENT TYPE nothing_added WHEN INSERT ON nope;
----
error: TableNotFound

CREATE ROUTE to_nowhere EVENT note_added TO nowhere;
----
error: SchemaInvalid

CREATE ROUTE from_nothing EVENT nothing_added TO audit;
----
error: SchemaInvalid

CREATE ROUTE bad_column EVENT note_added TO audit WHERE missing IN ('urgent');
----
error: ColumnNotFound

# A trigger fires on an insert or an update, and one on a delete is refused by its kind.
CREATE TRIGGER on_delete ON notes WHEN DELETE;
----
error: TriggerEventUnsupported

CREATE TRIGGER on_delete_sync ON notes WHEN DELETE INCLUDING SYNC;
----
error: TriggerEventUnsupported

# An interval is a positive whole number of milliseconds, seconds, minutes or hours, and the
# policy for missed ticks is skip-and-audit, catch-up or fail-loud.
CREATE SCHEDULE fortnightly EVERY '1 fortnight' TX (tidy);
----
error: PlanError

CREATE SCHEDULE never EVERY '0 seconds' TX (tidy);
----
error: PlanError

CREATE SCHEDULE sometimes EVERY '1 hour' TX (tidy) MISSED_TICK_POLICY 'sometimes';
----
error: PlanError

# A trigger, a route and a schedule are dropped by name; the trigger's and route's names take
# another definition. An event type goes with its table, and a sink stays, as in the old engine.
ALTER TABLE notes DROP COLUMN body CASCADE;
----
error: SchemaInvalid

ALTER TABLE notes RENAME COLUMN body TO content;
ALTER TABLE notes DROP COLUMN content;
----
error: SchemaInvalid

ALTER TABLE notes RENAME COLUMN content TO body;
DROP ROUTE added_to_hook;
DROP TRIGGER on_note;
DROP SCHEDULE hourly;
CREATE TRIGGER on_note ON tags WHEN INSERT;
CREATE ROUTE added_to_hook EVENT note_removed TO hook;

# As in Postgres, a name that is not declared is refused unless IF EXISTS is written, a trigger
# is dropped ON its own table only, and a trigger declared again exactly is refused too.
DROP SCHEDULE hourly;
----
error: SchemaInvalid

DROP ROUTE nowhere;
----
error: SchemaInvalid

DROP TRIGGER nothing;
----
error: SchemaInvalid

DROP TRIGGER on_note ON notes;
----
error: SchemaInvalid

DROP SCHEDULE IF EXISTS hourly;
DROP ROUTE IF EXISTS nowhere;
DROP TRIGGER IF EXISTS nothing;
DROP TRIGGER IF EXISTS on_note ON notes;
CREATE TRIGGER on_note ON tags WHEN INSERT;
----
error: SchemaInvalid

DROP TRIGGER on_note ON tags;
CREATE TRIGGER on_note ON tags WHEN INSERT;

# A changed column leaves the table's declarations in place.
ALTER TABLE notes ADD COLUMN extra TEXT;
CREATE TRIGGER on_note_edit ON notes WHEN INSERT;
----
error: SchemaInvalid

# Dropping a table drops its triggers and event types, and the routes from those event types;
# another table's trigger and the sinks stay.
DROP TABLE notes;
CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);
CREATE TRIGGER on_note_edit ON notes WHEN INSERT;
CREATE EVENT TYPE note_added WHEN UPDATE ON notes;
CREATE EVENT TYPE note_removed WHEN UPDATE ON notes;
CREATE ROUTE added_to_audit EVENT note_added TO hook;
CREATE ROUTE added_to_hook EVENT note_removed TO audit;
CREATE TRIGGER on_note ON notes WHEN INSERT;
----
error: SchemaInvalid

CREATE SINK hook TYPE callback;
----
error: SchemaInvalid
