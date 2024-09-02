SELECT count() FROM system.cnch_tables WHERE database = currentDatabase(1);

CREATE TABLE test (d Date, id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    PRIMARY KEY `id`
    ORDER BY `id`
    SAMPLE BY `id`;

INSERT INTO test values ('2019-01-01', 1, 'a')
SELECT count() FROM system.cnch_tables WHERE database = currentDatabase(1);
SELECT name, is_detached, partition_key, sorting_key, primary_key, sampling_key, cluster_key, split_number, with_range FROM system.cnch_tables WHERE database = currentDatabase(1);

SELECT partition_key FROM system.cnch_tables WHERE database = currentDatabase(1);
SELECT '-- test system.cnch_parts --'; 
SELECT count() from system.cnch_parts where database = currentDatabase(1) and table = 'test'; 
SELECT count() from system.cnch_parts where database = currentDatabase(1) and table = 'test' and partition_id = '20190101'; 
SELECT '-- test system.cnch_table_host --'; 
SELECT count() from system.cnch_table_host where database = currentDatabase(0) and name = 'test'; 
SELECT count() from system.cnch_table_host where database = currentDatabase(0); 

DROP TABLE test;

SELECT count() FROM system.cnch_tables WHERE database = currentDatabase(1);

SELECT '-- test system.cnch_staged_parts --'; 
CREATE TABLE test_unique (d Date, id UInt64, uniq_id UInt64, a String)
    ENGINE = CnchMergeTree()
    PARTITION BY `d`
    PRIMARY KEY `id`
    ORDER BY `id`
    SAMPLE BY `id`
    UNIQUE KEY `uniq_id`;
SELECT count() from system.cnch_staged_parts where database = currentDatabase(0) and table = 'test_unique'; 
SELECT count() from system.cnch_staged_parts where database = currentDatabase(0);  -- { serverError 80 }
DROP TABLE test_unique;
