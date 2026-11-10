# CREATE TABLE never redeclares a table, IF NOT EXISTS makes a second declaration do nothing, and
# the indexes behind a key and a unique rule have names the engine keeps for itself.

CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT UNIQUE, body TEXT);
INSERT INTO notes (id, title, body) VALUES (1, 'kept', 'first');

# A taken name is refused, and the table and its rows stay as they were.
CREATE TABLE notes (id UUID PRIMARY KEY, other REAL);
----
error: SchemaInvalid

SELECT id, title, body FROM notes ORDER BY id;
----
id | title | body
1 | kept | first

# With IF NOT EXISTS a taken name is left alone, whatever shape the second declaration has.
CREATE TABLE IF NOT EXISTS notes (id UUID PRIMARY KEY, other REAL);
----
rows affected: 0

SELECT other FROM notes;
----
error: ColumnNotFound

INSERT INTO notes (id, title, body) VALUES (2, 'second', 'more');
SELECT id, title, body FROM notes ORDER BY id;
----
id | title | body
1 | kept | first
2 | second | more

# With IF NOT EXISTS a free name is declared.
CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY, word TEXT);
INSERT INTO tags (id, word) VALUES (1, 'a');
SELECT id, word FROM tags ORDER BY id;
----
id | word
1 | a

# The key's index is __pk_<column> and a unique column's is __unique_<column>. A user's index
# may not take either prefix, and may sit beside them.
CREATE INDEX __pk_id ON notes (id);
----
error: ReservedIndexName

CREATE INDEX __unique_title ON notes (title);
----
error: ReservedIndexName

CREATE INDEX notes_by_body ON notes (body);
SELECT id FROM notes WHERE id = 2;
----
id
2

SELECT id FROM notes WHERE title = 'kept';
----
id
1
