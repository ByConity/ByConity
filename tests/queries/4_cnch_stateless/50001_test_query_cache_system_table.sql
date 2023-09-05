CREATE TABLE test_query_cache_system_table (d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    PRIMARY KEY `id`
    ORDER BY `id`
    SAMPLE BY `id`;

--- miss, hit, hit again, miss, hit, hit again ---
INSERT INTO test_query_cache_system_table values ('2019-01-01', 1, 'a');

SELECT * FROM test_query_cache_system_table SETTINGS use_query_cache = 1; 
SELECT count() > 0 FROM system.query_cache;
SYSTEM DROP QUERY CACHE;
SELECT count() FROM system.query_cache;
DROP TABLE test_query_cache_system_table;
