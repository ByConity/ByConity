DROP TABLE IF EXISTS null_test;
CREATE TABLE null_test (p_date Date, name Nullable(String), age Nullable(Int32), gender String) ENGINE = CnchMergeTree() PARTITION BY p_date ORDER BY (name, age) SETTINGS allow_nullable_key = 1;

INSERT INTO null_test (p_date, name, age, gender) values('2019-01-01', NULL, NULL, 'male');
select * from null_test;
select max(name), max(age) from null_test;

INSERT INTO null_test (p_date, name, age, gender) values('2019-01-01', 'qmm', 1, 'male');
select * from null_test where isNull(name);
select * from null_test where isNotNull(name);
select * from null_test where name < 'xyz' and age > 0;
select max(name), max(age) from null_test;

DROP TABLE null_test;

DROP TABLE IF EXISTS null_ttl_key;
CREATE TABLE null_ttl_key (p Nullable(DateTime), id Int32) ENGINE = CnchMergeTree() ORDER BY id;
ALTER TABLE null_ttl_key MODIFY TTL p + INTERVAL 30 DAY; -- { serverError 450}
ALTER TABLE null_ttl_key MODIFY SETTING allow_nullable_key = 1;
ALTER TABLE null_ttl_key MODIFY TTL p + INTERVAL 30 DAY;
DROP TABLE null_ttl_key;
