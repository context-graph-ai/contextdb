# IN and NOT IN with a list of values, in the WHERE of a SELECT, an UPDATE and a DELETE, as
# standard SQL has them. Row 3 holds NULL in every column but its key and `bin`.

CREATE TABLE stock (id INTEGER PRIMARY KEY, kind TEXT, qty INTEGER, bin INTEGER, ref UUID);
INSERT INTO stock (id, kind, qty, bin, ref)
VALUES (1, 'bolt', 7, 1, '550e8400-e29b-41d4-a716-446655440001'),
       (2, 'nut', 6, 1, '550e8400-e29b-41d4-a716-446655440002'),
       (3, NULL, NULL, 2, NULL),
       (4, 'bolt', 9, 2, '550e8400-e29b-41d4-a716-446655440004'),
       (5, 'washer', 6, 3, NULL);

# IN is TRUE when the value equals one of the list; NOT IN when it equals none of them.
SELECT id FROM stock WHERE kind IN ('bolt', 'nut') ORDER BY id;
----
id
1
2
4

SELECT id FROM stock WHERE kind NOT IN ('bolt', 'nut') ORDER BY id;
----
id
5

SELECT id FROM stock WHERE qty IN (6) ORDER BY id;
----
id
2
5

SELECT id FROM stock WHERE id IN (4, 1, 404) ORDER BY id;
----
id
1
4

# It stands wherever a comparison does, beside AND and under NOT.
SELECT id FROM stock WHERE kind IN ('bolt') AND qty NOT IN (7) ORDER BY id;
----
id
4

SELECT id FROM stock WHERE NOT kind IN ('bolt', 'nut') ORDER BY id;
----
id
5

# A NULL on the left equals nothing and differs from nothing, so row 3 is in neither answer.
SELECT id FROM stock WHERE qty IN (6) OR qty NOT IN (6) ORDER BY id;
----
id
1
2
4
5

# A NULL in the list is never equal to anything: IN is TRUE only through another item, and
# NOT IN is never TRUE.
SELECT id FROM stock WHERE qty IN (7, NULL) ORDER BY id;
----
id
1

SELECT id FROM stock WHERE qty NOT IN (7, NULL) ORDER BY id;
----
id

# A row value on the left is compared with row values of the same length, column by column.
SELECT id FROM stock WHERE (kind, bin) IN (('bolt', 1), ('nut', 1)) ORDER BY id;
----
id
1
2

SELECT id FROM stock WHERE (kind, bin) NOT IN (('bolt', 1), ('nut', 2)) ORDER BY id;
----
id
2
4
5

SELECT id FROM stock WHERE (kind, bin) IN (('bolt', NULL)) ORDER BY id;
----
id

# Two row values are equal when every column is, so one column that differs settles it whatever
# a NULL beside it holds: row 3, (NULL, 2), is not ('bolt', 1).
SELECT id FROM stock WHERE (kind, bin) NOT IN (('bolt', 1)) ORDER BY id;
----
id
2
3
4
5

# A UUID column is compared with the text that spells a UUID, in either case, alone and inside
# a row value.
SELECT id FROM stock WHERE ref IN ('550e8400-e29b-41d4-a716-446655440001', '550E8400-E29B-41D4-A716-446655440004') ORDER BY id;
----
id
1
4

SELECT id FROM stock WHERE ref NOT IN ('550e8400-e29b-41d4-a716-446655440001') ORDER BY id;
----
id
2
4

SELECT id FROM stock WHERE (ref, bin) IN (('550e8400-e29b-41d4-a716-446655440002', 1)) ORDER BY id;
----
id
2

# Refused by name: a row value anywhere but beside IN, one of another length than its partner,
# and a list that is a SELECT.
SELECT id FROM stock WHERE (kind, bin) = ('bolt', 1);
----
error: PlanError

SELECT id FROM stock WHERE (kind, bin) IN (('bolt', 1, 2));
----
error: PlanError

SELECT id FROM stock WHERE id IN (SELECT id FROM stock);
----
error: PlanError

# UPDATE and DELETE find their rows the same way.
UPDATE stock SET qty = 0 WHERE id IN (1, 2);
----
rows affected: 2

UPDATE stock SET bin = 9 WHERE (kind, bin) IN (('bolt', 2), ('washer', 3));
----
rows affected: 2

UPDATE stock SET qty = 1 WHERE kind NOT IN ('bolt');
----
rows affected: 2

SELECT id, kind, qty, bin FROM stock ORDER BY id;
----
id | kind | qty | bin
1 | bolt | 0 | 1
2 | nut | 1 | 1
3 | NULL | NULL | 2
4 | bolt | 9 | 9
5 | washer | 1 | 9

DELETE FROM stock WHERE id IN (2, 404);
----
rows affected: 1

DELETE FROM stock WHERE kind NOT IN ('bolt');
----
rows affected: 1

SELECT id FROM stock ORDER BY id;
----
id
1
3
4
