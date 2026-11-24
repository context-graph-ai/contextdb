# The world of `a_narrowed_reader_is_told_the_same_whatever_is_done_to_rows_it_may_not_see`
# (`visibility.rs`), one statement to a line. `{reach}` is the column of the rule under test,
# `{own}` its value in a row the handle may see and `{hid}` in one it may not. Rows 1 to 99
# are the handle's own; rows 100 and up are hidden (H0); rows 300 and up are the hidden rows
# the steps add and take away (H1), nearer every search than any own row, and never named.
## world
CREATE TABLE notes (id INTEGER PRIMARY KEY, {reach}, kind TEXT, slug TEXT UNIQUE, payload TEXT)
CREATE INDEX notes_kind ON notes (kind)
ALTER TABLE notes SET MAINTAIN COUNT (kind)
CREATE TABLE plain (id INTEGER PRIMARY KEY, {reach}, payload TEXT)
CREATE INDEX plain_payload ON plain (payload)
CREATE INDEX plain_reach ON plain (reach)
ALTER TABLE plain SET MAINTAIN COUNT (reach)
CREATE TABLE cites (id INTEGER PRIMARY KEY, {reach}, note_id INTEGER REFERENCES notes(id))
CREATE TABLE vm (id INTEGER PRIMARY KEY, {reach}, v VECTOR(3) INDEXING_THRESHOLD 1)
CREATE TABLE vd (id INTEGER PRIMARY KEY, {reach} NOT NULL, v VECTOR(3) PARTITION_KEY (bucket, reach) INDEXING_THRESHOLD 4, bucket INTEGER NOT NULL DEFAULT 0)
CREATE TABLE vo (id INTEGER PRIMARY KEY, {reach} NOT NULL, v VECTOR(3) PARTITION_KEY (reach))
CREATE TABLE vl (id INTEGER PRIMARY KEY, {reach} NOT NULL, bucket INTEGER NOT NULL, v VECTOR(3) PARTITION_KEY (bucket, reach) MAX_PARTITIONS 2)
CREATE TABLE results (id INTEGER PRIMARY KEY, {reach}, task_id INTEGER UNIQUE, weight REAL)
CREATE TABLE tasks (id INTEGER PRIMARY KEY, {reach}, v VECTOR(3) RANK_POLICY (JOIN results ON task_id, FORMULA '{vector_score} * coalesce({weight}, 1.0)', SORT_KEY ranked))
CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL, {reach}) DAG('DEPENDS_ON')
CREATE TABLE jl (id INTEGER PRIMARY KEY, a REAL, b INTEGER)
CREATE TABLE jr (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, {reach})
CREATE INDEX jr_ab ON jr (a, b)
CREATE INDEX jr_b ON jr (b)
INSERT INTO notes VALUES (1, {own}, 'k', 'open-one', 'seen-one'), (2, {own}, 'j', 'open-two', 'seen-two'), (100, {hid}, 'k', 'shut', 'secret-one'), (101, {hid}, 'j', 'shut-two', 'secret-two')
INSERT INTO plain VALUES (1, {own}, 'seen'), (2, {own}, 'seen'), (3, {own}, 'seen'), (100, {hid}, 'secret')
INSERT INTO cites VALUES (1, {own}, 1), (2, {own}, 100), (100, {hid}, 1)
INSERT INTO vm VALUES (1, {own}, [1.0, 0.5, 0.0]), (2, {own}, [1.0, 0.0, 0.5]), (3, {own}, [0.0, 1.0, 0.0]), (100, {hid}, [1.0, 0.2, 0.0])
INSERT INTO vd (id, reach, v) VALUES (1, {own}, [1.0, 0.5, 0.0]), (2, {own}, [1.0, 0.0, 0.5]), (3, {own}, [0.0, 1.0, 0.0])
INSERT INTO vd (id, reach, v) VALUES (100, {hid}, [0.0, 1.0, 0.0]), (101, {hid}, [0.0, 1.0, 0.1]), (102, {hid}, [0.0, 1.0, 0.2]), (103, {hid}, [0.0, 1.0, 0.3]), (104, {hid}, [0.0, 1.0, 0.4]), (105, {hid}, [0.0, 1.0, 0.5]), (106, {hid}, [0.0, 1.0, 0.6]), (107, {hid}, [0.0, 1.0, 0.7]), (108, {hid}, [0.0, 1.0, 0.8]), (109, {hid}, [0.0, 1.0, 0.9]), (110, {hid}, [0.0, 1.0, 0.0]), (111, {hid}, [0.0, 1.0, 0.1]), (112, {hid}, [0.0, 1.0, 0.2]), (113, {hid}, [0.0, 1.0, 0.3]), (114, {hid}, [0.0, 1.0, 0.4]), (115, {hid}, [0.0, 1.0, 0.5]), (116, {hid}, [0.0, 1.0, 0.6]), (117, {hid}, [0.0, 1.0, 0.7]), (118, {hid}, [0.0, 1.0, 0.8]), (119, {hid}, [0.0, 1.0, 0.9]), (120, {hid}, [0.0, 1.0, 0.0]), (121, {hid}, [0.0, 1.0, 0.1]), (122, {hid}, [0.0, 1.0, 0.2]), (123, {hid}, [0.0, 1.0, 0.3]), (124, {hid}, [0.0, 1.0, 0.4]), (125, {hid}, [0.0, 1.0, 0.5]), (126, {hid}, [0.0, 1.0, 0.6]), (127, {hid}, [0.0, 1.0, 0.7]), (128, {hid}, [0.0, 1.0, 0.8]), (129, {hid}, [0.0, 1.0, 0.9]), (130, {hid}, [0.0, 1.0, 0.0]), (131, {hid}, [0.0, 1.0, 0.1]), (132, {hid}, [0.0, 1.0, 0.2]), (133, {hid}, [0.0, 1.0, 0.3]), (134, {hid}, [0.0, 1.0, 0.4]), (135, {hid}, [0.0, 1.0, 0.5]), (136, {hid}, [0.0, 1.0, 0.6]), (137, {hid}, [0.0, 1.0, 0.7]), (138, {hid}, [0.0, 1.0, 0.8]), (139, {hid}, [0.0, 1.0, 0.9]), (140, {hid}, [0.0, 1.0, 0.0]), (141, {hid}, [0.0, 1.0, 0.1]), (142, {hid}, [0.0, 1.0, 0.2]), (143, {hid}, [0.0, 1.0, 0.3]), (144, {hid}, [0.0, 1.0, 0.4]), (145, {hid}, [0.0, 1.0, 0.5]), (146, {hid}, [0.0, 1.0, 0.6]), (147, {hid}, [0.0, 1.0, 0.7]), (148, {hid}, [0.0, 1.0, 0.8]), (149, {hid}, [0.0, 1.0, 0.9]), (150, {hid}, [0.0, 1.0, 0.0]), (151, {hid}, [0.0, 1.0, 0.1]), (152, {hid}, [0.0, 1.0, 0.2]), (153, {hid}, [0.0, 1.0, 0.3]), (154, {hid}, [0.0, 1.0, 0.4]), (155, {hid}, [0.0, 1.0, 0.5]), (156, {hid}, [0.0, 1.0, 0.6]), (157, {hid}, [0.0, 1.0, 0.7]), (158, {hid}, [0.0, 1.0, 0.8]), (159, {hid}, [0.0, 1.0, 0.9]), (160, {hid}, [0.0, 1.0, 0.0]), (161, {hid}, [0.0, 1.0, 0.1]), (162, {hid}, [0.0, 1.0, 0.2]), (163, {hid}, [0.0, 1.0, 0.3]), (164, {hid}, [0.0, 1.0, 0.4]), (165, {hid}, [0.0, 1.0, 0.5]), (166, {hid}, [0.0, 1.0, 0.6]), (167, {hid}, [0.0, 1.0, 0.7]), (168, {hid}, [0.0, 1.0, 0.8]), (169, {hid}, [0.0, 1.0, 0.9]), (170, {hid}, [0.0, 1.0, 0.0]), (171, {hid}, [0.0, 1.0, 0.1]), (172, {hid}, [0.0, 1.0, 0.2]), (173, {hid}, [0.0, 1.0, 0.3]), (174, {hid}, [0.0, 1.0, 0.4]), (175, {hid}, [0.0, 1.0, 0.5]), (176, {hid}, [0.0, 1.0, 0.6]), (177, {hid}, [0.0, 1.0, 0.7]), (178, {hid}, [0.0, 1.0, 0.8]), (179, {hid}, [0.0, 1.0, 0.9]), (180, {hid}, [0.0, 1.0, 0.0]), (181, {hid}, [0.0, 1.0, 0.1]), (182, {hid}, [0.0, 1.0, 0.2]), (183, {hid}, [0.0, 1.0, 0.3]), (184, {hid}, [0.0, 1.0, 0.4]), (185, {hid}, [0.0, 1.0, 0.5]), (186, {hid}, [0.0, 1.0, 0.6]), (187, {hid}, [0.0, 1.0, 0.7]), (188, {hid}, [0.0, 1.0, 0.8]), (189, {hid}, [0.0, 1.0, 0.9]), (190, {hid}, [0.0, 1.0, 0.0]), (191, {hid}, [0.0, 1.0, 0.1]), (192, {hid}, [0.0, 1.0, 0.2]), (193, {hid}, [0.0, 1.0, 0.3]), (194, {hid}, [0.0, 1.0, 0.4]), (195, {hid}, [0.0, 1.0, 0.5]), (196, {hid}, [0.0, 1.0, 0.6]), (197, {hid}, [0.0, 1.0, 0.7]), (198, {hid}, [0.0, 1.0, 0.8]), (199, {hid}, [0.0, 1.0, 0.9]), (200, {hid}, [0.0, 1.0, 0.0]), (201, {hid}, [0.0, 1.0, 0.1]), (202, {hid}, [0.0, 1.0, 0.2]), (203, {hid}, [0.0, 1.0, 0.3]), (204, {hid}, [0.0, 1.0, 0.4]), (205, {hid}, [0.0, 1.0, 0.5]), (206, {hid}, [0.0, 1.0, 0.6]), (207, {hid}, [0.0, 1.0, 0.7]), (208, {hid}, [0.0, 1.0, 0.8]), (209, {hid}, [0.0, 1.0, 0.9]), (210, {hid}, [0.0, 1.0, 0.0]), (211, {hid}, [0.0, 1.0, 0.1]), (212, {hid}, [0.0, 1.0, 0.2]), (213, {hid}, [0.0, 1.0, 0.3]), (214, {hid}, [0.0, 1.0, 0.4]), (215, {hid}, [0.0, 1.0, 0.5]), (216, {hid}, [0.0, 1.0, 0.6]), (217, {hid}, [0.0, 1.0, 0.7]), (218, {hid}, [0.0, 1.0, 0.8]), (219, {hid}, [0.0, 1.0, 0.9]), (220, {hid}, [0.0, 1.0, 0.0]), (221, {hid}, [0.0, 1.0, 0.1]), (222, {hid}, [0.0, 1.0, 0.2]), (223, {hid}, [0.0, 1.0, 0.3]), (224, {hid}, [0.0, 1.0, 0.4]), (225, {hid}, [0.0, 1.0, 0.5]), (226, {hid}, [0.0, 1.0, 0.6]), (227, {hid}, [0.0, 1.0, 0.7]), (228, {hid}, [0.0, 1.0, 0.8]), (229, {hid}, [0.0, 1.0, 0.9]), (230, {hid}, [0.0, 1.0, 0.0]), (231, {hid}, [0.0, 1.0, 0.1]), (232, {hid}, [0.0, 1.0, 0.2]), (233, {hid}, [0.0, 1.0, 0.3]), (234, {hid}, [0.0, 1.0, 0.4]), (235, {hid}, [0.0, 1.0, 0.5]), (236, {hid}, [0.0, 1.0, 0.6])
INSERT INTO vo VALUES (1, {own}, [1.0, 0.5, 0.0]), (2, {own}, [1.0, 0.0, 0.5]), (100, {hid}, [1.0, 0.0, 0.0])
INSERT INTO vl VALUES (100, {hid}, 0, [1.0, 0.0, 0.0]), (101, {hid}, 1, [1.0, 0.0, 0.0])
INSERT INTO tasks VALUES (1, {own}, [1.0, 0.1, 0.0]), (2, {own}, [1.0, 1.0, 0.0]), (100, {hid}, [1.0, 0.0, 0.0])
INSERT INTO results VALUES (1, {own}, 2, 0.5), (100, {hid}, 1, 0.1)
INSERT INTO edges VALUES (1, '00000000-0000-0000-0000-00000000000a', '00000000-0000-0000-0000-00000000000b', 'DEPENDS_ON', {own}), (100, '00000000-0000-0000-0000-00000000000b', '00000000-0000-0000-0000-00000000000e', 'DEPENDS_ON', {hid})
INSERT INTO jl VALUES (1, 7.0, 3)
INSERT INTO jr VALUES (7, 7, 3, {own}), (100, 7, 3, {hid})
# Every read is asked plainly and through EXPLAIN.
## reads
SELECT id, kind, payload FROM notes ORDER BY id
SELECT COUNT(*) FROM notes
SELECT COUNT(*) FROM notes WHERE kind = 'k'
SELECT COUNT(*) FROM notes WHERE id > 1
SELECT COUNT(*) FROM plain WHERE payload = 'seen'
SELECT COUNT(*) FROM plain WHERE reach = {own}
SELECT COUNT(*) FROM plain WHERE reach = {hid}
SELECT kind, COUNT(*) FROM notes GROUP BY kind ORDER BY kind
SELECT id, payload FROM plain WHERE payload = 'seen' ORDER BY id
SELECT n.id, c.id FROM notes n JOIN cites c ON c.note_id = n.id ORDER BY n.id, c.id
SELECT id FROM notes WHERE id IN (SELECT note_id FROM cites) ORDER BY id
SELECT jr.id FROM jl JOIN jr ON jr.a = jl.a AND jr.b = jl.b
WITH x AS (SELECT id, v FROM vd) SELECT id FROM x ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 2
SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:DEPENDS_ON]->{1,3}(b) WHERE a.id = '00000000-0000-0000-0000-00000000000a' COLUMNS (b.id AS b_id))
SELECT id FROM vm ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR EXACT LIMIT 2
SELECT id FROM vm ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR INDEXED LIMIT 2
SELECT id FROM vm ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 2
SELECT id FROM vd ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR EXACT LIMIT 2
SELECT id FROM vd ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR INDEXED LIMIT 2
SELECT id FROM vm WHERE id <= 3 ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 2
SELECT id FROM vo ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR INDEXED LIMIT 2
SELECT id FROM tasks ORDER BY v <=> [1.0, 0.0, 0.0] USE RANK ranked LIMIT 2
SELECT payload FROM notes WHERE id = 100
SELECT payload FROM notes WHERE slug = 'shut'
# Every write is made in a transaction that is rolled back once its answer is read.
## writes
UPDATE notes SET payload = 'changed' WHERE kind = 'k'
DELETE FROM plain WHERE id <= 2
INSERT INTO notes VALUES (50, {own}, 'k', 'open-fifty', 'seen-fifty')
INSERT INTO cites VALUES (50, {own}, 2)
INSERT INTO edges VALUES (50, '00000000-0000-0000-0000-00000000000d', '00000000-0000-0000-0000-00000000000b', 'DEPENDS_ON', {own})
INSERT INTO notes VALUES (51, {hid}, 'k', 'open-other', 'seen-other')
INSERT INTO cites VALUES (51, {own}, 100)
UPDATE notes SET payload = 'changed' WHERE id = 101
## hidden rows added
INSERT INTO notes VALUES (300, {hid}, 'k', 'shut-new', 'secret-new')
INSERT INTO plain VALUES (300, {hid}, 'seen')
INSERT INTO cites VALUES (300, {hid}, 1)
INSERT INTO vm VALUES (300, {hid}, [1.0, 0.0, 0.0]), (301, {hid}, [1.0, 0.0, 0.0])
INSERT INTO vd (id, reach, v) VALUES (300, {hid}, [1.0, 0.0, 0.0]), (301, {hid}, [1.0, 0.0, 0.0])
INSERT INTO tasks VALUES (300, {hid}, [1.0, 0.0, 0.0])
INSERT INTO edges VALUES (300, '00000000-0000-0000-0000-00000000000e', '00000000-0000-0000-0000-00000000000f', 'DEPENDS_ON', {hid})
INSERT INTO jr VALUES (300, 7, 3, {hid})
## hidden rows changed
UPDATE notes SET kind = 'j', payload = 'secret-changed' WHERE id = 100
UPDATE plain SET payload = 'seen' WHERE id = 100
UPDATE vm SET v = [1.0, 0.0, 0.0] WHERE id = 100
UPDATE vd SET v = [1.0, 0.0, 0.0] WHERE id >= 100 AND id <= 110
UPDATE results SET weight = 9.0 WHERE id = 100
## hidden rows taken away
DELETE FROM notes WHERE id >= 300
DELETE FROM plain WHERE id >= 300
DELETE FROM cites WHERE id >= 300
DELETE FROM vm WHERE id >= 300
DELETE FROM vd WHERE id >= 300
DELETE FROM tasks WHERE id >= 300
DELETE FROM edges WHERE id >= 300
DELETE FROM jr WHERE id >= 300
## limit
INSERT INTO vl VALUES (1, {own}, 2, [1.0, 0.0, 0.0])
