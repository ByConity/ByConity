SET disable_optimize_final = 0;

DROP TABLE IF EXISTS 02981_unique_table_vertical_merges;

CREATE TABLE 02981_unique_table_vertical_merges (a UInt64, b UInt64, c Array(String))
ENGINE = CnchMergeTree
ORDER BY a
UNIQUE KEY b
SETTINGS
    min_bytes_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 8192,
    index_granularity_bytes = 0,
    merge_max_block_size = 8192,
    merge_max_block_size_bytes = '10M';

SYSTEM START MERGES 02981_unique_table_vertical_merges;

-- Writing 2 parts
INSERT INTO 02981_unique_table_vertical_merges SELECT modulo(number, 20000), number, arrayMap(x -> repeat('a', 500), range(100)) FROM numbers(40000) settings max_block_size = 20000;

-- Replace some rows to a smaller array
INSERT INTO 02981_unique_table_vertical_merges SELECT modulo(number * 8, 20000), number * 8, arrayMap(x -> repeat('b', 500), range(99)) FROM numbers(5000);

OPTIMIZE TABLE 02981_unique_table_vertical_merges FINAL SETTINGS mutations_sync = 1;

SELECT sum(a), sum(b), sum(length(c)) FROM 02981_unique_table_vertical_merges;

DROP TABLE IF EXISTS 02981_unique_table_vertical_merges;
