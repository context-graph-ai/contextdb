# PRIMARY KEY, UNIQUE on one column, UNIQUE on several, and NOT NULL.

CREATE TABLE people (
  id INTEGER PRIMARY KEY,
  email TEXT UNIQUE,
  name TEXT NOT NULL,
  family TEXT,
  given TEXT,
  born INTEGER,
  UNIQUE (family, given, born)
);
INSERT INTO people (id, email, name, family, given, born)
VALUES (1, 'ada@example.com', 'Ada', 'Lovelace', 'Ada', 1815);

# A second row with the same key, the same value of a unique column, or the same values of all
# the columns of a unique rule is refused.
INSERT INTO people (id, email, name, family, given, born)
VALUES (1, 'other@example.com', 'Other', 'Other', 'Other', 1900);
----
error: UniqueViolation

INSERT INTO people (id, email, name, family, given, born)
VALUES (2, 'ada@example.com', 'Second', 'Second', 'Second', 1900);
----
error: UniqueViolation

INSERT INTO people (id, email, name, family, given, born)
VALUES (3, 'third@example.com', 'Third', 'Lovelace', 'Ada', 1815);
----
error: UniqueViolation

# A row that differs in one column of the rule is another row.
INSERT INTO people (id, email, name, family, given, born)
VALUES (4, 'byron@example.com', 'Byron', 'Lovelace', 'Ada', 1816);
----
rows affected: 1

# A NULL in any column of a rule takes the row out of that rule: these two are alike in every
# column a rule names.
INSERT INTO people (id, email, name, family, given, born) VALUES (5, NULL, 'Five', 'Lovelace', NULL, 1815);
INSERT INTO people (id, email, name, family, given, born) VALUES (6, NULL, 'Six', 'Lovelace', NULL, 1815);

# NOT NULL needs a value, when the row is written and whenever it is changed. A key is never NULL.
INSERT INTO people (id, email) VALUES (7, 'seven@example.com');
----
error: ColumnNotNullable

INSERT INTO people (id, name) VALUES (NULL, 'Nobody');
----
error: ColumnNotNullable

UPDATE people SET name = NULL WHERE id = 1;
----
error: ColumnNotNullable

# A change that would make two rows alike is refused like an insert.
UPDATE people SET email = 'ada@example.com' WHERE id = 4;
----
error: UniqueViolation

UPDATE people SET born = 1815 WHERE id = 4;
----
error: UniqueViolation

UPDATE people SET id = 1 WHERE id = 4;
----
error: UniqueViolation

# Every refusal left the rows as they were, and the key finds its one row.
SELECT id, email, name, family, given, born FROM people ORDER BY id;
----
id | email | name | family | given | born
1 | ada@example.com | Ada | Lovelace | Ada | 1815
4 | byron@example.com | Byron | Lovelace | Ada | 1816
5 | NULL | Five | Lovelace | NULL | 1815
6 | NULL | Six | Lovelace | NULL | 1815

SELECT id, name FROM people WHERE id = 4;
----
id | name
4 | Byron

# UNIQUE written as a rule of the table may name one column.
CREATE TABLE codes (id INTEGER PRIMARY KEY, code TEXT, UNIQUE (code));
INSERT INTO codes (id, code) VALUES (1, 'a');
INSERT INTO codes (id, code) VALUES (2, 'a');
----
error: UniqueViolation

SELECT id, code FROM codes ORDER BY id;
----
id | code
1 | a

# Two rules of one table stay two rules whatever their columns are called: here one is on the
# column a_b and the other on the columns a and b, and each refuses its own duplicates only.
CREATE TABLE pairs (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, a_b INTEGER, UNIQUE (a_b), UNIQUE (a, b));
INSERT INTO pairs (id, a, b, a_b) VALUES (1, 1, 2, 10);
INSERT INTO pairs (id, a, b, a_b) VALUES (2, 1, 2, 20);
----
error: UniqueViolation

INSERT INTO pairs (id, a, b, a_b) VALUES (3, 1, 3, 10);
----
error: UniqueViolation

INSERT INTO pairs (id, a, b, a_b) VALUES (4, 1, 3, 20);
SELECT id, a, b, a_b FROM pairs ORDER BY id;
----
id | a | b | a_b
1 | 1 | 2 | 10
4 | 1 | 3 | 20
