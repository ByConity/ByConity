DROP DATABASE IF EXISTS segprofile;
CREATE DATABASE segprofile;
USE segprofile;

set enum_replicate_no_stats=0;

SELECT 1 FORMAT Null SETTINGS enable_optimizer=1, log_segment_profiles=0, log_queries=1, log_queries_min_type='QUERY_FINISH';
SYSTEM FLUSH LOGS;
WITH ( SELECT query_id FROM system.query_log WHERE current_database = currentDatabase(0) AND Settings['enable_optimizer']='1' AND Settings['log_segment_profiles']='0' AND Settings['log_queries']='1' AND query like 'SELECT 1%' ORDER BY event_time desc LIMIT 1) AS query_id_ SELECT length(segment_profiles) > 0 FROM system.query_log WHERE query_id = query_id_ UNION SELECT length(segment_profiles) > 0 FROM cnch_system.cnch_query_log WHERE query_id = query_id_;


SELECT 1 FORMAT Null SETTINGS enable_optimizer=1, log_segment_profiles=1, log_queries=1, log_queries_min_type='QUERY_FINISH';
SYSTEM FLUSH LOGS;
WITH ( SELECT query_id FROM system.query_log WHERE current_database = currentDatabase(0) AND Settings['enable_optimizer']='1' AND Settings['log_segment_profiles']='1' AND Settings['log_queries']='1' AND query like 'SELECT 1%' ORDER BY event_time desc LIMIT 1) AS query_id_ SELECT length(segment_profiles) > 0 FROM system.query_log WHERE query_id = query_id_ UNION DISTINCT SELECT length(segment_profiles) > 0 FROM cnch_system.cnch_query_log WHERE query_id = query_id_;


CREATE TABLE my_first_table ( user_id UInt32, message String, timestamp DateTime) ENGINE = CnchMergeTree() PARTITION BY timestamp ORDER BY (user_id, timestamp);
INSERT INTO my_first_table (user_id, message, timestamp) VALUES (101, 'Hello, ByConity!', now()), (102, 'Insert a lot of rows per batch', yesterday()), (102, 'Sort your data based on your commonly-used queries', today()), (101, 'Granules are the smallest chunks of data read', now() + 5);

SELECT * FROM my_first_table WHERE user_id < 102 ORDER BY timestamp ASC SETTINGS enable_optimizer = 1, log_segment_profiles = 1, log_queries = 1, log_queries_min_type = 'QUERY_FINISH' FORMAT `Null`;
SYSTEM FLUSH LOGS;
WITH ( SELECT query_id FROM system.query_log WHERE current_database = currentDatabase(0) AND Settings['enable_optimizer']='1' AND Settings['log_segment_profiles']='1' AND Settings['log_queries']='1' AND query like '%user_id < 102%' ORDER BY event_time desc LIMIT 1) AS query_id_ SELECT length(segment_profiles) > 0 FROM system.query_log WHERE query_id = query_id_ UNION DISTINCT SELECT length(segment_profiles) > 0 FROM cnch('vw_default', system.query_log) WHERE startsWith(query_id, query_id_);
