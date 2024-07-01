DROP DATABASE IF EXISTS db_test_show;

CREATE DATABASE db_test_show ENGINE=Cnch;
SELECT name, engine FROM system.databases WHERE name = '${TENANT_DB_PREFIX}db_test_show';

DROP TABLE IF EXISTS db_test_show.A;
DROP TABLE IF EXISTS db_test_show.B;

CREATE TABLE db_test_show.A (A UInt8) ENGINE=CnchMergeTree() ORDER BY A;
CREATE TABLE db_test_show.B (A UInt8) ENGINE=CnchMergeTree() ORDER BY A;

SHOW TABLES from db_test_show;

DROP TABLE db_test_show.B;

SHOW TABLES from db_test_show;
DROP DATABASE db_test_show;
SELECT count() FROM system.databases WHERE name = '${TENANT_DB_PREFIX}db_test_show';
