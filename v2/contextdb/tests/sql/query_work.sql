# Counted work of statements that read more than one table or more than one row for an answer:
# each reads the stored rows its answer needs (`# reads N`), whatever the size of the tables.
# `t` row n is (n, 'row-<n>', n % 10) and owner n is (5000 + n, n, 'note-<n>'), reached from `t`
# through the index on `t_id`.
CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL, tag INTEGER NOT NULL);
CREATE TABLE owners (id INTEGER PRIMARY KEY, t_id INTEGER NOT NULL, note TEXT);
CREATE INDEX owners_t ON owners (t_id);
INSERT INTO t (id, name, tag) VALUES
(1, 'row-0000001', 1), (2, 'row-0000002', 2), (3, 'row-0000003', 3), (4, 'row-0000004', 4), (5, 'row-0000005', 5), (6, 'row-0000006', 6), (7, 'row-0000007', 7), (8, 'row-0000008', 8), (9, 'row-0000009', 9), (10, 'row-0000010', 0), (11, 'row-0000011', 1), (12, 'row-0000012', 2), (13, 'row-0000013', 3), (14, 'row-0000014', 4), (15, 'row-0000015', 5), (16, 'row-0000016', 6), (17, 'row-0000017', 7), (18, 'row-0000018', 8), (19, 'row-0000019', 9), (20, 'row-0000020', 0);
INSERT INTO owners (id, t_id, note) VALUES
(5001, 1, 'note-1'), (5002, 2, 'note-2'), (5003, 3, 'note-3'), (5004, 4, 'note-4'), (5005, 5, 'note-5'), (5006, 6, 'note-6'), (5007, 7, 'note-7'), (5008, 8, 'note-8'), (5009, 9, 'note-9'), (5010, 10, 'note-10'), (5011, 11, 'note-11'), (5012, 12, 'note-12'), (5013, 13, 'note-13'), (5014, 14, 'note-14'), (5015, 15, 'note-15'), (5016, 16, 'note-16'), (5017, 17, 'note-17'), (5018, 18, 'note-18'), (5019, 19, 'note-19'), (5020, 20, 'note-20');

# A join reads one right row for each left row through an index: 3 rows of `t` and the owner of
# each (reads 6); with a list on the right, the list once (reads 4 + 3 + 3); with a list on another
# indexed column, the join stays on its index: the 3 names, the row of `t`, its owner (reads 5).
SELECT x.id, o.note FROM t x JOIN owners o ON o.t_id = x.id WHERE x.id <= 3;
----
id | note  (read 6, compared 0)
1 | note-1
2 | note-2
3 | note-3

SELECT x.id, o.note FROM t x JOIN owners o ON o.t_id = x.id WHERE x.id <= 4
AND o.id IN (SELECT id FROM owners WHERE id <= 5003);
----
id | note  (read 10, compared 0)
1 | note-1
2 | note-2
3 | note-3

CREATE INDEX owners_note ON owners (note);
SELECT x.id FROM t x JOIN owners o ON o.t_id = x.id WHERE x.id = 1
AND o.note IN (SELECT note FROM owners WHERE id <= 5003);
----
id  (read 5, compared 0)
1

# A left 7.0 is no key of an INTEGER column and a left TEXT of 300 bytes cuts an index: the join
# still reads the other index (`jr_b`, or `jr_btag` past tag 6), the left row and the rows of
# b = 3 (reads 1 + 4), or only the entries above tag 6 (reads 1 + 3).
CREATE TABLE jl (id INTEGER PRIMARY KEY, a REAL, b INTEGER, txt TEXT);
CREATE TABLE jr (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, txt TEXT, tag INTEGER, context_id UUID NOT NULL CONTEXT_ID);
CREATE INDEX jr_ab ON jr (a, b);
CREATE INDEX jr_b ON jr (b);
CREATE INDEX jr_txt ON jr (txt, tag);
CREATE INDEX jr_btag ON jr (b, tag);
INSERT INTO jl VALUES (1, 7.0, 3, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');
INSERT INTO jr VALUES
(1, 1, 1, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 1, '00000000-0000-0000-0000-000000000001'), (2, 2, 2, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 2, '00000000-0000-0000-0000-000000000001'), (3, 3, 3, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 3, '00000000-0000-0000-0000-000000000001'), (4, 4, 0, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 4, '00000000-0000-0000-0000-000000000001'), (5, 5, 1, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 5, '00000000-0000-0000-0000-000000000001'), (6, 6, 2, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 6, '00000000-0000-0000-0000-000000000001'), (7, 7, 3, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 7, '00000000-0000-0000-0000-000000000001'), (8, 8, 0, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 8, '00000000-0000-0000-0000-000000000001'), (9, 9, 1, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 9, '00000000-0000-0000-0000-000000000001'), (10, 10, 2, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 10, '00000000-0000-0000-0000-000000000001'), (11, 11, 3, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 11, '00000000-0000-0000-0000-000000000001'), (12, 12, 0, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 12, '00000000-0000-0000-0000-000000000001'), (100, 7, 3, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 100, '00000000-0000-0000-0000-000000000002');
SELECT jr.id FROM jl JOIN jr ON jr.a = jl.a AND jr.b = jl.b;
----
id  (read 5, compared 0)
7
100

SELECT jr.id FROM jl JOIN jr ON jr.txt = jl.txt AND jr.b = jl.b WHERE jr.tag > 6;
----
id  (read 4, compared 0)
7
11
100

# By the whole key of `jr`, the rows of b = 1 that pass the filter are not read (reads 1 + 1).
SELECT jr.id FROM jl JOIN jr ON jr.id = jl.id WHERE jr.b = 1;
----
id  (read 2, compared 0)
1

# The reader's shape: 7.0 is no key of INTEGER, so the join walks `r_ab`, not `r_b`: the left row
# and the row of (3, 5) of 501 (reads 1 + 1).
CREATE TABLE rr (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);
CREATE INDEX r_ab ON rr (a, b);
CREATE INDEX r_b ON rr (b);
CREATE TABLE ll (id REAL, a INTEGER, b INTEGER);
INSERT INTO ll VALUES (7.0, 3, 5);
INSERT INTO rr VALUES
(1, 0, 5), (2, 0, 5), (3, 0, 5), (4, 0, 5), (5, 0, 5), (6, 0, 5), (7, 3, 5), (8, 0, 5), (9, 0, 5), (10, 0, 5), (11, 0, 5), (12, 0, 5), (13, 0, 5), (14, 0, 5), (15, 0, 5), (16, 0, 5), (17, 0, 5), (18, 0, 5), (19, 0, 5), (20, 0, 5), (21, 0, 5), (22, 0, 5), (23, 0, 5), (24, 0, 5), (25, 0, 5), (26, 0, 5), (27, 0, 5), (28, 0, 5), (29, 0, 5), (30, 0, 5), (31, 0, 5), (32, 0, 5), (33, 0, 5), (34, 0, 5), (35, 0, 5), (36, 0, 5), (37, 0, 5), (38, 0, 5), (39, 0, 5), (40, 0, 5), (41, 0, 5), (42, 0, 5), (43, 0, 5), (44, 0, 5), (45, 0, 5), (46, 0, 5), (47, 0, 5), (48, 0, 5), (49, 0, 5), (50, 0, 5), (51, 0, 5), (52, 0, 5), (53, 0, 5), (54, 0, 5), (55, 0, 5), (56, 0, 5), (57, 0, 5), (58, 0, 5), (59, 0, 5), (60, 0, 5), (61, 0, 5), (62, 0, 5), (63, 0, 5), (64, 0, 5), (65, 0, 5), (66, 0, 5), (67, 0, 5), (68, 0, 5), (69, 0, 5), (70, 0, 5), (71, 0, 5), (72, 0, 5), (73, 0, 5), (74, 0, 5), (75, 0, 5), (76, 0, 5), (77, 0, 5), (78, 0, 5), (79, 0, 5), (80, 0, 5), (81, 0, 5), (82, 0, 5), (83, 0, 5), (84, 0, 5), (85, 0, 5), (86, 0, 5), (87, 0, 5), (88, 0, 5), (89, 0, 5), (90, 0, 5), (91, 0, 5), (92, 0, 5), (93, 0, 5), (94, 0, 5), (95, 0, 5), (96, 0, 5), (97, 0, 5), (98, 0, 5), (99, 0, 5), (100, 0, 5), (101, 0, 5), (102, 0, 5), (103, 0, 5), (104, 0, 5), (105, 0, 5), (106, 0, 5), (107, 0, 5), (108, 0, 5), (109, 0, 5), (110, 0, 5), (111, 0, 5), (112, 0, 5), (113, 0, 5), (114, 0, 5), (115, 0, 5), (116, 0, 5), (117, 0, 5), (118, 0, 5), (119, 0, 5), (120, 0, 5), (121, 0, 5), (122, 0, 5), (123, 0, 5), (124, 0, 5), (125, 0, 5), (126, 0, 5), (127, 0, 5), (128, 0, 5), (129, 0, 5), (130, 0, 5), (131, 0, 5), (132, 0, 5), (133, 0, 5), (134, 0, 5), (135, 0, 5), (136, 0, 5), (137, 0, 5), (138, 0, 5), (139, 0, 5), (140, 0, 5), (141, 0, 5), (142, 0, 5), (143, 0, 5), (144, 0, 5), (145, 0, 5), (146, 0, 5), (147, 0, 5), (148, 0, 5), (149, 0, 5), (150, 0, 5), (151, 0, 5), (152, 0, 5), (153, 0, 5), (154, 0, 5), (155, 0, 5), (156, 0, 5), (157, 0, 5), (158, 0, 5), (159, 0, 5), (160, 0, 5), (161, 0, 5), (162, 0, 5), (163, 0, 5), (164, 0, 5), (165, 0, 5), (166, 0, 5), (167, 0, 5), (168, 0, 5), (169, 0, 5), (170, 0, 5), (171, 0, 5), (172, 0, 5), (173, 0, 5), (174, 0, 5), (175, 0, 5), (176, 0, 5), (177, 0, 5), (178, 0, 5), (179, 0, 5), (180, 0, 5), (181, 0, 5), (182, 0, 5), (183, 0, 5), (184, 0, 5), (185, 0, 5), (186, 0, 5), (187, 0, 5), (188, 0, 5), (189, 0, 5), (190, 0, 5), (191, 0, 5), (192, 0, 5), (193, 0, 5), (194, 0, 5), (195, 0, 5), (196, 0, 5), (197, 0, 5), (198, 0, 5), (199, 0, 5), (200, 0, 5),
(201, 0, 5), (202, 0, 5), (203, 0, 5), (204, 0, 5), (205, 0, 5), (206, 0, 5), (207, 0, 5), (208, 0, 5), (209, 0, 5), (210, 0, 5), (211, 0, 5), (212, 0, 5), (213, 0, 5), (214, 0, 5), (215, 0, 5), (216, 0, 5), (217, 0, 5), (218, 0, 5), (219, 0, 5), (220, 0, 5), (221, 0, 5), (222, 0, 5), (223, 0, 5), (224, 0, 5), (225, 0, 5), (226, 0, 5), (227, 0, 5), (228, 0, 5), (229, 0, 5), (230, 0, 5), (231, 0, 5), (232, 0, 5), (233, 0, 5), (234, 0, 5), (235, 0, 5), (236, 0, 5), (237, 0, 5), (238, 0, 5), (239, 0, 5), (240, 0, 5), (241, 0, 5), (242, 0, 5), (243, 0, 5), (244, 0, 5), (245, 0, 5), (246, 0, 5), (247, 0, 5), (248, 0, 5), (249, 0, 5), (250, 0, 5), (251, 0, 5), (252, 0, 5), (253, 0, 5), (254, 0, 5), (255, 0, 5), (256, 0, 5), (257, 0, 5), (258, 0, 5), (259, 0, 5), (260, 0, 5), (261, 0, 5), (262, 0, 5), (263, 0, 5), (264, 0, 5), (265, 0, 5), (266, 0, 5), (267, 0, 5), (268, 0, 5), (269, 0, 5), (270, 0, 5), (271, 0, 5), (272, 0, 5), (273, 0, 5), (274, 0, 5), (275, 0, 5), (276, 0, 5), (277, 0, 5), (278, 0, 5), (279, 0, 5), (280, 0, 5), (281, 0, 5), (282, 0, 5), (283, 0, 5), (284, 0, 5), (285, 0, 5), (286, 0, 5), (287, 0, 5), (288, 0, 5), (289, 0, 5), (290, 0, 5), (291, 0, 5), (292, 0, 5), (293, 0, 5), (294, 0, 5), (295, 0, 5), (296, 0, 5), (297, 0, 5), (298, 0, 5), (299, 0, 5), (300, 0, 5), (301, 0, 5), (302, 0, 5), (303, 0, 5), (304, 0, 5), (305, 0, 5), (306, 0, 5), (307, 0, 5), (308, 0, 5), (309, 0, 5), (310, 0, 5), (311, 0, 5), (312, 0, 5), (313, 0, 5), (314, 0, 5), (315, 0, 5), (316, 0, 5), (317, 0, 5), (318, 0, 5), (319, 0, 5), (320, 0, 5), (321, 0, 5), (322, 0, 5), (323, 0, 5), (324, 0, 5), (325, 0, 5), (326, 0, 5), (327, 0, 5), (328, 0, 5), (329, 0, 5), (330, 0, 5), (331, 0, 5), (332, 0, 5), (333, 0, 5), (334, 0, 5), (335, 0, 5), (336, 0, 5), (337, 0, 5), (338, 0, 5), (339, 0, 5), (340, 0, 5), (341, 0, 5), (342, 0, 5), (343, 0, 5), (344, 0, 5), (345, 0, 5), (346, 0, 5), (347, 0, 5), (348, 0, 5), (349, 0, 5), (350, 0, 5), (351, 0, 5), (352, 0, 5), (353, 0, 5), (354, 0, 5), (355, 0, 5), (356, 0, 5), (357, 0, 5), (358, 0, 5), (359, 0, 5), (360, 0, 5), (361, 0, 5), (362, 0, 5), (363, 0, 5), (364, 0, 5), (365, 0, 5), (366, 0, 5), (367, 0, 5), (368, 0, 5), (369, 0, 5), (370, 0, 5), (371, 0, 5), (372, 0, 5), (373, 0, 5), (374, 0, 5), (375, 0, 5), (376, 0, 5), (377, 0, 5), (378, 0, 5), (379, 0, 5), (380, 0, 5), (381, 0, 5), (382, 0, 5), (383, 0, 5), (384, 0, 5), (385, 0, 5), (386, 0, 5), (387, 0, 5), (388, 0, 5), (389, 0, 5), (390, 0, 5), (391, 0, 5), (392, 0, 5), (393, 0, 5), (394, 0, 5), (395, 0, 5), (396, 0, 5), (397, 0, 5), (398, 0, 5), (399, 0, 5), (400, 0, 5),
(401, 0, 5), (402, 0, 5), (403, 0, 5), (404, 0, 5), (405, 0, 5), (406, 0, 5), (407, 0, 5), (408, 0, 5), (409, 0, 5), (410, 0, 5), (411, 0, 5), (412, 0, 5), (413, 0, 5), (414, 0, 5), (415, 0, 5), (416, 0, 5), (417, 0, 5), (418, 0, 5), (419, 0, 5), (420, 0, 5), (421, 0, 5), (422, 0, 5), (423, 0, 5), (424, 0, 5), (425, 0, 5), (426, 0, 5), (427, 0, 5), (428, 0, 5), (429, 0, 5), (430, 0, 5), (431, 0, 5), (432, 0, 5), (433, 0, 5), (434, 0, 5), (435, 0, 5), (436, 0, 5), (437, 0, 5), (438, 0, 5), (439, 0, 5), (440, 0, 5), (441, 0, 5), (442, 0, 5), (443, 0, 5), (444, 0, 5), (445, 0, 5), (446, 0, 5), (447, 0, 5), (448, 0, 5), (449, 0, 5), (450, 0, 5), (451, 0, 5), (452, 0, 5), (453, 0, 5), (454, 0, 5), (455, 0, 5), (456, 0, 5), (457, 0, 5), (458, 0, 5), (459, 0, 5), (460, 0, 5), (461, 0, 5), (462, 0, 5), (463, 0, 5), (464, 0, 5), (465, 0, 5), (466, 0, 5), (467, 0, 5), (468, 0, 5), (469, 0, 5), (470, 0, 5), (471, 0, 5), (472, 0, 5), (473, 0, 5), (474, 0, 5), (475, 0, 5), (476, 0, 5), (477, 0, 5), (478, 0, 5), (479, 0, 5), (480, 0, 5), (481, 0, 5), (482, 0, 5), (483, 0, 5), (484, 0, 5), (485, 0, 5), (486, 0, 5), (487, 0, 5), (488, 0, 5), (489, 0, 5), (490, 0, 5), (491, 0, 5), (492, 0, 5), (493, 0, 5), (494, 0, 5), (495, 0, 5), (496, 0, 5), (497, 0, 5), (498, 0, 5), (499, 0, 5), (500, 0, 5), (501, 0, 5);
SELECT ll.a, rr.id FROM ll JOIN rr ON rr.id = ll.id AND rr.a = ll.a AND rr.b = ll.b WHERE rr.b = 5;
----
a | id  (read 2, compared 0)
3 | 7

# A cross join reads its right side once, as an inner join that always holds does: 3 rows of
# `t` and the 20 owners, not 20 for each of the 3 (reads 23).
SELECT COUNT(*) AS n FROM t x CROSS JOIN owners o WHERE x.id <= 3;
----
n  (read 23, compared 0)
60

SELECT COUNT(*) AS n FROM t x INNER JOIN owners o ON 1 = 1 WHERE x.id <= 3;
----
n  (read 23, compared 0)
60

# An OR of key ranges reads the rows of those ranges, each row once, as Postgres reads each range
# through the index and joins them: 2 + 1 + 2 rows (reads 5), and ranges that overlap the 6 rows
# from 1 to 6 (reads 6). An OR with a column no index has reads every row (reads 20).
SELECT id FROM t WHERE id <= 2 OR id = 10 OR id >= 19 ORDER BY id;
----
id  (read 5, compared 0)
1
2
10
19
20

SELECT COUNT(*) AS n FROM t WHERE id <= 5 OR (id >= 3 AND id <= 6);
----
n  (read 6, compared 0)
6

SELECT id FROM t WHERE id <= 2 OR tag = 9 ORDER BY id;
----
id  (read 20, compared 0)
1
2
9
19

# ROW_NUMBER reads each partition to its bound and one more: the 3 rows returned and at most one
# more in each of the two partitions (reads 3 to 5, where sorting reads the 14 rows of both).
CREATE TABLE sessions (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, at INTEGER NOT NULL);
CREATE INDEX sessions_newest ON sessions (ctx ASC, at DESC, id ASC);
INSERT INTO sessions (id, ctx, at) VALUES
(1, 'c1', 1), (2, 'c2', 2), (3, 'c0', 3), (4, 'c1', 4), (5, 'c2', 5), (6, 'c0', 6), (7, 'c1', 7), (8, 'c2', 8), (9, 'c0', 9), (10, 'c1', 10), (11, 'c2', 11), (12, 'c0', 12), (13, 'c1', 13), (14, 'c2', 14), (15, 'c0', 15), (16, 'c1', 16), (17, 'c2', 17), (18, 'c0', 18), (19, 'c1', 19), (20, 'c2', 20), (21, 'c0', 21);
WITH ranked AS (
  SELECT ctx, id, ROW_NUMBER() OVER (PARTITION BY ctx ORDER BY at DESC, id ASC) AS r
  FROM sessions WHERE ctx IN ('c0', 'c1'))
SELECT ctx, id, r FROM ranked
WHERE (ctx = 'c0' AND r <= 2) OR (ctx = 'c1' AND r <= 1) ORDER BY ctx ASC, r ASC;
----
ctx | id | r  (read 3, compared 0)
c0 | 21 | 1
c0 | 18 | 2
c1 | 19 | 1

# LIMIT with OFFSET over an ordered index reads the rows it skips and returns (reads 5).
CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, seq INTEGER NOT NULL);
CREATE INDEX events_kind_seq ON events (kind ASC, seq DESC);
INSERT INTO events (id, kind, seq) VALUES
(1, 'k1', 1), (2, 'k0', 2), (3, 'k1', 3), (4, 'k0', 4), (5, 'k1', 5), (6, 'k0', 6), (7, 'k1', 7), (8, 'k0', 8), (9, 'k1', 9), (10, 'k0', 10), (11, 'k1', 11), (12, 'k0', 12), (13, 'k1', 13), (14, 'k0', 14), (15, 'k1', 15), (16, 'k0', 16), (17, 'k1', 17), (18, 'k0', 18), (19, 'k1', 19), (20, 'k0', 20);
SELECT id FROM events WHERE kind = 'k1' ORDER BY seq DESC LIMIT 2 OFFSET 3;
----
id  (read 5, compared 0)
13
11

# A lookup by an indexed JSON path reads the rows it returns (reads 4 of 20).
CREATE TABLE docs (id INTEGER PRIMARY KEY, doc JSON);
CREATE INDEX docs_kind ON docs ((doc -> 'kind'));
INSERT INTO docs (id, doc) VALUES
(1, '{"kind":"k1","n":1}'), (2, '{"kind":"k2","n":2}'), (3, '{"kind":"k3","n":3}'), (4, '{"kind":"k4","n":4}'), (5, '{"kind":"k0","n":5}'), (6, '{"kind":"k1","n":6}'), (7, '{"kind":"k2","n":7}'), (8, '{"kind":"k3","n":8}'), (9, '{"kind":"k4","n":9}'), (10, '{"kind":"k0","n":10}'), (11, '{"kind":"k1","n":11}'), (12, '{"kind":"k2","n":12}'), (13, '{"kind":"k3","n":13}'), (14, '{"kind":"k4","n":14}'), (15, '{"kind":"k0","n":15}'), (16, '{"kind":"k1","n":16}'), (17, '{"kind":"k2","n":17}'), (18, '{"kind":"k3","n":18}'), (19, '{"kind":"k4","n":19}'), (20, '{"kind":"k0","n":20}');
SELECT id FROM docs WHERE doc -> 'kind' = '"k2"';
----
id  (read 4, compared 0)
2
7
12
17

# A grouped count over a kept count reads no row (reads 0) and gives the groups the rows give
# (reads 24). Row n of each is (n, 'c<n % 3>', 'k<n % 4>'): two rows for each pair.
CREATE TABLE plain (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, conn TEXT NOT NULL);
CREATE TABLE kept (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, conn TEXT NOT NULL,
  UNIQUE (ctx, conn, id)) MAINTAIN COUNT (ctx), (ctx, conn);
INSERT INTO plain (id, ctx, conn) VALUES
(1, 'c1', 'k1'), (2, 'c2', 'k2'), (3, 'c0', 'k3'), (4, 'c1', 'k0'), (5, 'c2', 'k1'), (6, 'c0', 'k2'), (7, 'c1', 'k3'), (8, 'c2', 'k0'), (9, 'c0', 'k1'), (10, 'c1', 'k2'), (11, 'c2', 'k3'), (12, 'c0', 'k0'), (13, 'c1', 'k1'), (14, 'c2', 'k2'), (15, 'c0', 'k3'), (16, 'c1', 'k0'), (17, 'c2', 'k1'), (18, 'c0', 'k2'), (19, 'c1', 'k3'), (20, 'c2', 'k0'), (21, 'c0', 'k1'), (22, 'c1', 'k2'), (23, 'c2', 'k3'), (24, 'c0', 'k0');
INSERT INTO kept (id, ctx, conn) VALUES
(1, 'c1', 'k1'), (2, 'c2', 'k2'), (3, 'c0', 'k3'), (4, 'c1', 'k0'), (5, 'c2', 'k1'), (6, 'c0', 'k2'), (7, 'c1', 'k3'), (8, 'c2', 'k0'), (9, 'c0', 'k1'), (10, 'c1', 'k2'), (11, 'c2', 'k3'), (12, 'c0', 'k0'), (13, 'c1', 'k1'), (14, 'c2', 'k2'), (15, 'c0', 'k3'), (16, 'c1', 'k0'), (17, 'c2', 'k1'), (18, 'c0', 'k2'), (19, 'c1', 'k3'), (20, 'c2', 'k0'), (21, 'c0', 'k1'), (22, 'c1', 'k2'), (23, 'c2', 'k3'), (24, 'c0', 'k0');
SELECT ctx, COUNT(*) FROM kept GROUP BY ctx;
----
ctx | count  (read 0, compared 0)
c0 | 8
c1 | 8
c2 | 8

SELECT ctx, COUNT(*) FROM plain GROUP BY ctx;
----
ctx | count  (read 24, compared 0)
c0 | 8
c1 | 8
c2 | 8

SELECT conn, COUNT(*) AS n FROM kept WHERE ctx = 'c1' GROUP BY conn;
----
conn | n  (read 0, compared 0)
k0 | 2
k1 | 2
k2 | 2
k3 | 2

SELECT conn, COUNT(*) AS n FROM plain WHERE ctx = 'c1' GROUP BY conn;
----
conn | n  (read 24, compared 0)
k0 | 2
k1 | 2
k2 | 2
k3 | 2

CREATE INDEX t_tag ON t (tag);
SELECT COUNT(*) FROM t WHERE tag >= 3 AND tag < 5 OR tag > 3 AND tag <= 6;
----
count  (read 8, compared 0)
8

SELECT COUNT(*) FROM t WHERE id <= 2 OR tag = 9;
----
count  (read 20, compared 0)
4
