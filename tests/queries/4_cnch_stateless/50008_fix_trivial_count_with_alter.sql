DROP TABLE IF EXISTS test_trivial_count_with_alter;

CREATE TABLE test_trivial_count_with_alter(id UInt64, num UInt64, dt Date)engine=CnchMergeTree order by id;

SET optimize_trivial_count_query = 1;

insert into test_trivial_count_with_alter values(1,1,today());
insert into test_trivial_count_with_alter values(1,1,today());
insert into test_trivial_count_with_alter values(2,3,today());
insert into test_trivial_count_with_alter values(2,4,today());

SELECT count() FROM test_trivial_count_with_alter;

ALTER TABLE test_trivial_count_with_alter DROP COLUMN num;

SELECT count() FROM test_trivial_count_with_alter;

DROP TABLE IF EXISTS test_trivial_count_with_alter;