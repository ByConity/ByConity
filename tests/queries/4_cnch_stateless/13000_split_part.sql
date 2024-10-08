SELECT split_part('A3B3C', '3', 2);
SELECT split_part('A#B#C', '#', 2);
SELECT split_part('', '#', 1);
SELECT split_part('A#B#C', '#', 4);
SELECT split_part('A##B#C', '#', 2);
SELECT split_part('A,B,C', '#', 1);
SELECT split_part('A##B##C', '##', 2);
SELECT split_part('###', '#', 3);
SELECT split_part('A#B#C', '#', -1);

DROP TABLE IF EXISTS split_part_table;
CREATE TABLE split_part_table
(
    a Nullable(String),
    b String,
    c Nullable(Int64),
    d Int64
)
ENGINE = CnchMergeTree()
ORDER BY b;

INSERT INTO split_part_table (a, b, c, d) VALUES ('A#B#C', '#', 1, 1), ('', '#', 1, 2), ('A#B#C', '#', 4, 3), ('A##B#C', '#', 2, 4), ('A,B,C', '#', 1, 5), ('A##B##C', '##', 2, 6), ('###', '#', 3, 7), ('A#B#C', '#', -1, 8), (null, '#', 5, 9);
SELECT split_part(a, b, c), d FROM split_part_table ORDER BY d;

DROP TABLE IF EXISTS split_part_table;
