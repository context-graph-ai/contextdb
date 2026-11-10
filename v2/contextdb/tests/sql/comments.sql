# SQL comments are taken out before a statement is read: `--` to the end of its line and
# `/* */` over any number of lines. Inside quoted text they are text.

CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);
INSERT INTO notes (id, body) VALUES (1, 'kept'), (2, '-- inside a text');

-- a comment on the line before a statement
SELECT id FROM notes ORDER BY id;
----
id
1
2

SELECT id -- the key
FROM notes -- every row
ORDER BY id DESC -- newest first
;
----
id
2
1

SELECT id /* between two words */ FROM notes WHERE /* and here */ id = 1;
----
id
1

SELECT id FROM notes /* a block
over two lines */ WHERE id = 2;
----
id
2

/* before the statement */ SELECT body FROM notes WHERE id = 1;
----
body
kept

SELECT body FROM notes WHERE id = 2;
----
body
-- inside a text

INSERT INTO notes (id, body) VALUES (3, '/* also text */');
SELECT id FROM notes WHERE body = '/* also text */';
----
id
3
