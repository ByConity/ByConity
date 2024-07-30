SET disable_optimize_final = 0;

DROP TABLE IF EXISTS 02981_vertical_merges_lc_memory_usage;

CREATE TABLE 02981_vertical_merges_lc_memory_usage (id UInt64, lc LowCardinality(String))
ENGINE = CnchMergeTree ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 8192,
    index_granularity_bytes = 0,
    merge_max_block_size = 8192,
    merge_max_block_size_bytes = '10M';

SYSTEM START MERGES 02981_vertical_merges_lc_memory_usage;

-- Writing 4 parts
INSERT INTO 02981_vertical_merges_lc_memory_usage SELECT number, rightPad(toString(number), 10000, '*') from numbers(120000) SETTINGS max_block_size = 20000;

-- lc column will fallback, and need about 4.7 GiB for peak_memory_usage
OPTIMIZE TABLE 02981_vertical_merges_lc_memory_usage FINAL SETTINGS mutations_sync = 1;

INSERT INTO 02981_vertical_merges_lc_memory_usage SELECT number, rightPad(toString(number), 10000, '*') from numbers(10000);
INSERT INTO 02981_vertical_merges_lc_memory_usage SELECT number, rightPad(toString(number), 10000, '*') from numbers(10000);
INSERT INTO 02981_vertical_merges_lc_memory_usage SELECT number, rightPad(toString(number), 10000, '*') from numbers(10000);

-- merge in full state
OPTIMIZE TABLE 02981_vertical_merges_lc_memory_usage FINAL SETTINGS mutations_sync = 1;

SELECT sum(id) FROM 02981_vertical_merges_lc_memory_usage;

SYSTEM FLUSH LOGS;

-- Need about 650 MiB for peak_memory_usage if merge_max_block_size_bytes = '10M', 1.4 GiB if merge_max_block_size_bytes = 0
SELECT
    peak_memory_usage < 800 * 1024 * 1024
        ? 'OK'
        : format('FAIL: memory usage: {}', formatReadableSize(peak_memory_usage))
FROM system.server_part_log
WHERE
    table = '02981_vertical_merges_lc_memory_usage'
    AND event_type = 'MergeParts'
    AND length(source_part_names) = 4
ORDER BY event_time DESC LIMIT 1;

DROP TABLE 02981_vertical_merges_lc_memory_usage;