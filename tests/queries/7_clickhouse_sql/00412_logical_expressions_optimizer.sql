DROP TABLE IF EXISTS merge_tree;
CREATE TABLE merge_tree (x UInt64, date Date) ENGINE = CnchMergeTree() PARTITION BY toYYYYMM(date) ORDER BY x SETTINGS index_granularity=1;

INSERT INTO merge_tree VALUES (1, '2000-01-01');
SELECT x AS y, y FROM merge_tree;

DROP TABLE IF EXISTS merge_tree;
