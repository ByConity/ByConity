DROP TABLE IF EXISTS pi;

CREATE TABLE pi
(
  `id` Int,
  `name` String
)
ENGINE = CnchMergeTree
ORDER BY id;

INSERT INTO pi VALUES (1, 'a'), (2, 'b');

SELECT table, total_rows_count
FROM system.cnch_parts_info
WHERE database = currentDatabase() AND table = 'pi'
;

DROP TABLE IF EXISTS pi;

CREATE TABLE pi
(
  `id` Int,
  `name` String
)
ENGINE = CnchMergeTree
ORDER BY id;

INSERT INTO pi VALUES (1, 'a'), (2, 'b');

SELECT table, total_rows_count
FROM system.cnch_parts_info
WHERE database = currentDatabase() AND table = 'pi'
;

DROP TABLE IF EXISTS pi;

CREATE TABLE pi
(
  `id` Int,
  `name` String
)
ENGINE = CnchMergeTree
ORDER BY id;

INSERT INTO pi VALUES (1, 'a'), (2, 'b');

SELECT table, total_rows_count
FROM system.cnch_parts_info
WHERE database = currentDatabase() AND table = 'pi'
;
