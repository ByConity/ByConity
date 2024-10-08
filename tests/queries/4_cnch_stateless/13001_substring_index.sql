SELECT substring_index('example.domain.com', '.', 1);
SELECT substring_index('example.domain.com', '.', 2);
SELECT substring_index('example.domain.com', '.', 3);
SELECT substring_index('example.domain.com', '.', 4);
SELECT substring_index('example.domain.com', '.', 0);
SELECT substring_index('example.domain.com', '#', 1);
SELECT substring_index('example.domain.com', '#', -1);
SELECT substring_index('example.domain.com', '.', -1);
SELECT substring_index('example.domain.com', '.', -2);
SELECT substring_index('example.domain.com', '.', -3);
SELECT substring_index('example.domain.com', '.', -4);
SELECT substring_index('example.domain.com', '.', -8);
SELECT substring_index('example.domain.com', '.', -0);
SELECT substring_index('', '.', 1);
SELECT substring_index('', '.', -1);
SELECT substring_index(NULL, '.', 1);
SELECT substring_index(NULL, '.', -1);
SELECT substring_index('example.domain.com', NULL, 1);
SELECT substring_index('example.domain.com', NULL, -1);

DROP TABLE IF EXISTS substring_index_table;
CREATE TABLE substring_index_table
(
    a Nullable(String),
    b String,
    c Int64,
    d Int64
)
ENGINE = CnchMergeTree()
ORDER BY b;

INSERT INTO substring_index_table (a, b, c, d) VALUES ('A#B#C', '#', 2, 1), ('', '#', 1, 2), ('A#B#C', '#', 4, 3), ('A##B#C', '#', -2, 4), ('A,B,C', '#', -1, 5), ('A##B##C', '##', -2, 6), ('###', '#', 3, 7), ('A#B#C', '#', -1, 8), (null, '#', 5, 9);
SELECT substring_index(a, b, c), d FROM substring_index_table ORDER BY d;

DROP TABLE IF EXISTS substring_index_table;
