DROP TABLE IF EXISTS unique_with_version_bad1;
DROP TABLE IF EXISTS unique_with_version_bad2;

CREATE TABLE unique_with_version_bad1 (event_time DateTime, id UInt64, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree(m1) PARTITION BY toDate(event_time) ORDER BY id UNIQUE KEY id;
-- Test alter version column for a table
ALTER TABLE unique_with_version_bad1 DROP COLUMN m1; -- { serverError 524 }
ALTER TABLE unique_with_version_bad1 RENAME COLUMN m1 to a1; -- { serverError 524 }
ALTER TABLE unique_with_version_bad1 MODIFY COLUMN m1 String; -- { serverError 524 }

-- Test use not exist as version column
CREATE TABLE unique_with_version_bad2 (event_time DateTime, id UInt64, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree(unknown) PARTITION BY toDate(event_time) ORDER BY id UNIQUE KEY id; -- { serverError 16 }
-- Test expression as version column
CREATE TABLE unique_with_version_bad2 (event_time DateTime, id UInt64, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree(sipHash64(id)) PARTITION BY toDate(event_time) ORDER BY id UNIQUE KEY id; -- { serverError 36 }

DROP TABLE IF EXISTS unique_with_version_bad1;
DROP TABLE IF EXISTS unique_with_version_bad2;

-- Test use version column
DROP TABLE IF EXISTS unique_with_version;
CREATE TABLE unique_with_version (event_time DateTime, id UInt64, s String, m1 UInt32, m2 UInt64) ENGINE = CnchMergeTree(event_time) PARTITION BY toDate(event_time) ORDER BY (s, id) PRIMARY KEY s UNIQUE KEY id;

INSERT INTO unique_with_version VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200), ('2020-10-29 23:50:00', 10001, '10001B', 8, 800), ('2020-10-29 23:50:00', 10002, '10002B', 5, 500);
INSERT INTO unique_with_version VALUES ('2020-10-30 00:05:00', 10001, '10001A', 1, 100), ('2020-10-30 00:05:00', 10002, '10002A', 2, 200);
INSERT INTO unique_with_version VALUES ('2020-10-29 23:40:00', 10001, '10001A', 5, 500), ('2020-10-29 23:40:00', 10002, '10002A', 2, 200);

SELECT event_time, id, s, m1, m2 FROM unique_with_version ORDER BY event_time, id;
INSERT INTO unique_with_version VALUES ('2020-10-29 23:50:00', 10001, '10001B', 8, 800), ('2020-10-29 23:50:00', 10002, '10002B', 5, 500), ('2020-10-29 23:55:00', 10001, '10001C', 10, 1000), ('2020-10-29 23:55:00', 10002, '10002C', 7, 700);
SELECT event_time, id, s, m1, m2 FROM unique_with_version ORDER BY event_time, id;

DROP TABLE IF EXISTS unique_with_version;

drop table if exists unique_with_partition_version_r1;
drop table if exists unique_with_partition_version_r2;
drop table if exists unique_with_partition_version_r3;
drop table if exists unique_with_partition_version_r4;
drop table if exists unique_with_partition_version_r5;
drop table if exists unique_with_partition_version_r6;
drop table if exists unique_with_partition_version_r7;

CREATE TABLE unique_with_partition_version_r1
(
    id Int64,
    version UInt8
)
ENGINE=CnchMergeTree(version)
partition by version
order by (id, version)
unique key (id, version);

insert into unique_with_partition_version_r1 values (3, 3);
select 'r1', * from unique_with_partition_version_r1;

CREATE TABLE unique_with_partition_version_r2
(
    id Int64,
    version UInt64
)
ENGINE=CnchMergeTree(version)
partition by version
order by (id, version)
unique key (id, version);

insert into unique_with_partition_version_r2 values (4, 4);
select 'r2', * from unique_with_partition_version_r2;

CREATE TABLE unique_with_partition_version_r3
(
    id Int64,
    version Int64
)
ENGINE=CnchMergeTree(version)
partition by version
order by (id, version)
unique key (id, version); -- { serverError 36 }

CREATE TABLE unique_with_partition_version_r4
(
    id Int64,
    version Int256
)
ENGINE=CnchMergeTree(version)
partition by version
order by (id, version)
unique key id; -- { serverError 36 }

CREATE TABLE unique_with_partition_version_r5
(
    id Int64,
    version Int64
)
ENGINE=CnchMergeTree(toInt64(version))
partition by toInt64(version)
order by (id, version)
unique key id; -- { serverError 36 }

CREATE TABLE unique_with_partition_version_r6
(
    id Int64,
    version Int64
)
ENGINE=CnchMergeTree(toUInt32(version))
partition by toUInt32(version)
order by (id, version)
unique key id;

CREATE TABLE unique_with_partition_version_r7
(
    id Int64,
    version Int64
)
ENGINE=CnchMergeTree((version, id))
partition by (version, id)
order by (id, version)
unique key id; -- { serverError 36 }

drop table if exists unique_with_partition_version_r1;
drop table if exists unique_with_partition_version_r2;
drop table if exists unique_with_partition_version_r3;
drop table if exists unique_with_partition_version_r4;
drop table if exists unique_with_partition_version_r5;
drop table if exists unique_with_partition_version_r6;
drop table if exists unique_with_partition_version_r7;
