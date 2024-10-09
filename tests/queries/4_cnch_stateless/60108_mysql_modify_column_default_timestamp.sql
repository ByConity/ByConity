set dialect_type='MYSQL';

DROP TABLE IF EXISTS t_test_alter_column_default_timestamp;

CREATE TABLE t_test_alter_column_default_timestamp
(
    `id` Int,
    `dt` DateTime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY(id)
);

INSERT INTO t_test_alter_column_default_timestamp(id) VALUES (1);

SELECT dt > '2024-01-01' FROM t_test_alter_column_default_timestamp WHERE id = 1;

SELECT 'NO ERROR IN MYSQL DIALECT';
ALTER TABLE t_test_alter_column_default_timestamp MODIFY COLUMN dt DateTime64(3) NULL DEFAULT CURRENT_TIMESTAMP;


set dialect_type='CLICKHOUSE';
ALTER TABLE t_test_alter_column_default_timestamp MODIFY COLUMN dt DateTime64(3) NULL DEFAULT CURRENT_TIMESTAMP();

SELECT 'ERROR IN CLICKHOUSE DIALECT: Missing columns';
ALTER TABLE t_test_alter_column_default_timestamp MODIFY COLUMN dt DateTime64(3) NULL DEFAULT CURRENT_TIMESTAMP; -- { serverError 47 }

DROP TABLE t_test_alter_column_default_timestamp;
