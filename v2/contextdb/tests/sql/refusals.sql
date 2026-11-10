# What is refused by name, and what is an empty answer and not a refusal.

CREATE TABLE notes (id INTEGER PRIMARY KEY, tag TEXT);
INSERT INTO notes (id, tag) VALUES (1, 'a');

# Text that is not a statement is refused before anything runs.
SELEC id FROM notes;
----
error: ParseError

SELECT id FROM;
----
error: ParseError

# A column the table does not have is never answered with a missing or made-up column.
SELECT nope FROM notes;
----
error: ColumnNotFound

SELECT id, nope FROM notes;
----
error: ColumnNotFound

# A sort key the engine cannot work out is refused, never answered unsorted.
SELECT tag FROM notes ORDER BY UPPER(tag);
----
error: OrderByExpressionNotSupported

# A key that names no row is an empty answer.
SELECT id, tag FROM notes WHERE id = 404;
----
id | tag

SELECT id, tag FROM notes ORDER BY id;
----
id | tag
1 | a
