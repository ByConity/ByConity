select * from system.cnch_kafka_tables where (database = 'wrong_database' AND name = 'wrong_name');
select * from system.cnch_kafka_tables where (database = 'wrong_database1' AND name = 'wrong_name1') OR (database = 'wrong_database2' AND name = 'wrong_name2');
select database from system.cnch_kafka_tables where (database = 'wrong_database' AND name = 'wrong_name');
select database from system.cnch_kafka_tables where (database = 'wrong_database1' AND name = 'wrong_name1') OR (database = 'wrong_database2' AND name = 'wrong_name2');
