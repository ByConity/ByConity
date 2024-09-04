DROP TABLE IF EXISTS non_unique_table_upgrade_with_version_column_t1;

-- For compatibility, if version is used without unique key specified, do not throw exception
CREATE TABLE non_unique_table_upgrade_with_version_column_t1 (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64, seq UInt32)
ENGINE = CnchMergeTree(event_time) PARTITION BY toDate(event_time) ORDER BY (s, id) PRIMARY KEY s;

INSERT INTO non_unique_table_upgrade_with_version_column_t1 VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500, 0), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200, 1), ('2020-10-29 23:50:00', 10001, '10001B', 8, 800, 2), ('2020-10-29 23:50:00', 10002, '10002B', 5, 500, 3);

SELECT event_time, id, s, m1, m2 FROM non_unique_table_upgrade_with_version_column_t1 ORDER BY event_time, id;

DROP TABLE IF EXISTS non_unique_table_upgrade_with_version_column_t1;
