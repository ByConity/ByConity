DROP DATABASE IF EXISTS test_system_table;
CREATE DATABASE test_system_table ENGINE=Cnch;

SELECT count() FROM system.cnch_tables;

CREATE TABLE test_system_table.test (d Date, id UInt64, a String)
    ENGINE = CnchMergeTree() 
    PARTITION BY `d` 
    PRIMARY KEY `id`
    ORDER BY `id` 
    SAMPLE BY `id`;

select count() FROM system.cnch_tables;
select database, name, is_detached, partition_key, sorting_key, primary_key, sampling_key, cluster_key, split_number, with_range FROM system.cnch_tables;

select database, partition_key FROM system.cnch_tables;

DROP TABLE test_system_table.test;

SELECT count() FROM system.cnch_tables;
