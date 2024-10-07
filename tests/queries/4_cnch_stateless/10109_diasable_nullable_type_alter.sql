DROP TABLE IF EXISTS u10109_disable_nullable_alter;
CREATE TABLE u10109_disable_nullable_alter (action String, labels Nullable(Array(String)), p_date Date) ENGINE = CnchMergeTree partition by p_date order by tuple();

INSERT INTO TABLE u10109_disable_nullable_alter VALUES ('test', ['test'], toDate('2024-07-11'));

SELECT * FROM u10109_disable_nullable_alter;

ALTER TABLE u10109_disable_nullable_alter MODIFY COLUMN labels Array(String); -- { serverError 517 }

ALTER TABLE u10109_disable_nullable_alter MODIFY COLUMN labels Array(String) SETTINGS mutation_allow_modify_remove_nullable = 1;

ALTER TABLE u10109_disable_nullable_alter MODIFY COLUMN action Nullable(String);

SELECT * FROM u10109_disable_nullable_alter;

DROP TABLE IF EXISTS u10109_disable_nullable_alter;

DROP TABLE IF EXISTS u10109_disable_lc_nullable_alter;
CREATE TABLE u10109_disable_lc_nullable_alter (action Nullable(String), labels LowCardinality(Nullable(String)), p_date Date) ENGINE = CnchMergeTree partition by p_date order by tuple();

INSERT INTO TABLE u10109_disable_lc_nullable_alter VALUES ('test', 'test', toDate('2024-07-11'));

SELECT * FROM u10109_disable_lc_nullable_alter;

ALTER TABLE u10109_disable_lc_nullable_alter MODIFY COLUMN action LowCardinality(Nullable(String));

ALTER TABLE u10109_disable_lc_nullable_alter MODIFY COLUMN labels LowCardinality(String) SETTINGS mutation_allow_modify_remove_nullable = 1;

SELECT * FROM u10109_disable_lc_nullable_alter;

DROP TABLE IF EXISTS u10109_disable_lc_nullable_alter;

