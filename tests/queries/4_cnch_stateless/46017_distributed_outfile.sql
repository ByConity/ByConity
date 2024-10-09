DROP TABLE IF EXISTS people;

CREATE TABLE people (`name` String, `country` String) ENGINE = CnchMergeTree() PARTITION BY `country` PRIMARY KEY `name` ORDER BY `name` SETTINGS index_granularity = 8192;

INSERT INTO people values ('Jack', 'sg');

-- distributed outfile into directory
SELECT * from people
INTO OUTFILE 'hdfs:///user/clickhouse/' FORMAT Parquet 
SETTINGS enable_optimizer=1, enable_distributed_output=1, overwrite_current_file=1;

-- distributed outfile into single file, different worker will use its own name
SELECT * from people
INTO OUTFILE 'hdfs:///user/clickhouse/clickhouse_outfile_1.csv' FORMAT Parquet 
SETTINGS enable_optimizer=1, enable_distributed_output=1, overwrite_current_file=1;

-- specify both enable_distributed_output and outfile_in_server_with_tcp
SELECT * from people
INTO OUTFILE 'hdfs:///user/clickhouse/' FORMAT Parquet COMPRESSION 'none'
SETTINGS enable_optimizer=1, overwrite_current_file=1, enable_distributed_output = 1, outfile_in_server_with_tcp = 1;

INSERT INTO people values ('Jack', 'sg');

-- agg query
SELECT count(distinct name) from people
INTO OUTFILE 'hdfs:///user/clickhouse/' FORMAT Parquet COMPRESSION 'none'
SETTINGS enable_optimizer=1, overwrite_current_file=1, enable_distributed_output = 1;

SELECT 'Hello, World! From client.';

DROP TABLE IF EXISTS people;
