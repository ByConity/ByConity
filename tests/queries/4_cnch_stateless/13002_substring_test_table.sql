CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS substr_table;
CREATE TABLE substr_table
(
    id Int64,
    a Nullable(String),
    b String
)
ENGINE = CnchMergeTree()
ORDER BY id;

INSERT INTO substr_table (id, a, b) VALUES (0, 'example', 'example'), (1, 'apple', 'banana'), (2, 'zebra', 'ant'), (3, '', ''), (4, '', 'nonempty'), (5, 'nonempty', ''), (6, 'CaseTest', 'Chance'), (7, 'EmPtYsTrInG', 'empty?'), (8, 'test123', 'test@#'), (9, 'long_string_1', 'long_string_2'), (10, NULL, 'long_string_2');
INSERT INTO substr_table (id, a, b) VALUES (11, NULL, 'exam'), (12, 'apple', 'bana'), (13, 'zebra', 'ant0'), (14, '888', '888'), (15, '', 'none'), (16, 'test123', 'test'), (17, 'long_string_1', 'long');

SELECT id, substring(b, 2) FROM substr_table ORDER BY id;
SELECT id, substr(b, 2) FROM substr_table ORDER BY id;
SELECT id, substring(a, 2, 1) FROM substr_table ORDER BY id;
SELECT id, substr(a, 2, 1) FROM substr_table ORDER BY id;
SELECT id, substring(b FROM 3) FROM substr_table ORDER BY id;
SELECT id, substr(b FROM 3) FROM substr_table ORDER BY id;
SELECT id, substring(b FROM 6 FOR 1) FROM substr_table ORDER BY id;
SELECT id, substr(b FROM 6 FOR 1) FROM substr_table ORDER BY id;
DROP TABLE IF EXISTS substr_table;