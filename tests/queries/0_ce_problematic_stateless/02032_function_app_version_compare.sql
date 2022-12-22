SELECT 1;
SELECT AppVersionCompare('10.0.0.1', '10.0.0.1', '=');
SELECT AppVersionCompare('10.0.0.1', '10.0.0.1', '!=');
SELECT AppVersionCompare('10.0.0.1', '10.0.0.1', '>=');
SELECT AppVersionCompare('10.0.0.1', '10.0.0.1', '<=');
SELECT AppVersionCompare('10.0.0.1', '10.0.0.1', '>');
SELECT AppVersionCompare('10.0.0.1', '10.0.0.1', '<');
SELECT AppVersionCompare('10.0.0.1', '10.0.0.1', '');

SELECT 2;
SELECT AppVersionCompare('10.0.0.1', '10.0.0', '=');
SELECT AppVersionCompare('10.0.0.1', '10.0.0', '!=');
SELECT AppVersionCompare('10.0.0.1', '10.0.0', '>=');
SELECT AppVersionCompare('10.0.0.1', '10.0.0', '<=');
SELECT AppVersionCompare('10.0.0.1', '10.0.0', '>');
SELECT AppVersionCompare('10.0.0.1', '10.0.0', '<');
SELECT AppVersionCompare('10.0.0.1', '10.0.0', '');

SELECT 3;
SELECT AppVersionCompare('10.0.0.1', '10.0.', '=');
SELECT AppVersionCompare('10.0.0.1', '10.0.', '!=');
SELECT AppVersionCompare('10.0.0.1', '10.0.', '>=');
SELECT AppVersionCompare('10.0.0.1', '10.0.', '<=');
SELECT AppVersionCompare('10.0.0.1', '10.0.', '>');
SELECT AppVersionCompare('10.0.0.1', '10.0.', '<');
SELECT AppVersionCompare('10.0.0.1', '10.0.', '');

SELECT 4;
SELECT AppVersionCompare('10.0.0', '10.0.0.1', '=');
SELECT AppVersionCompare('10.0.0', '10.0.0.1', '!=');
SELECT AppVersionCompare('10.0.0', '10.0.0.1', '>=');
SELECT AppVersionCompare('10.0.0', '10.0.0.1', '<=');
SELECT AppVersionCompare('10.0.0', '10.0.0.1', '>');
SELECT AppVersionCompare('10.0.0', '10.0.0.1', '<');
SELECT AppVersionCompare('10.0.0', '10.0.0.1', '');

SELECT 5;
SELECT AppVersionCompare('10.0.', '10.0.0.1', '=');
SELECT AppVersionCompare('10.0.', '10.0.0.1', '!=');
SELECT AppVersionCompare('10.0.', '10.0.0.1', '>=');
SELECT AppVersionCompare('10.0.', '10.0.0.1', '<=');
SELECT AppVersionCompare('10.0.', '10.0.0.1', '>');
SELECT AppVersionCompare('10.0.', '10.0.0.1', '<');
SELECT AppVersionCompare('10.0.', '10.0.0.1', '');

SELECT 6;
SELECT AppVersionCompare('9.0.1', '10.0.0.1', '=');
SELECT AppVersionCompare('9.0.1', '10.0.0.1', '!=');
SELECT AppVersionCompare('9.0.1', '10.0.0.1', '>=');
SELECT AppVersionCompare('9.0.1', '10.0.0.1', '<=');
SELECT AppVersionCompare('9.0.1', '10.0.0.1', '>');
SELECT AppVersionCompare('9.0.1', '10.0.0.1', '<');
SELECT AppVersionCompare('9.0.1', '10.0.0.1', '');

SELECT 7;
SELECT AppVersionCompare('9.0.1', '', '=');
SELECT AppVersionCompare('9.0.1', '', '!=');
SELECT AppVersionCompare('9.0.1', '', '>=');
SELECT AppVersionCompare('9.0.1', '', '<=');
SELECT AppVersionCompare('9.0.1', '', '>');
SELECT AppVersionCompare('9.0.1', '', '<');
SELECT AppVersionCompare('9.0.1', '', '');

SELECT 8;
SELECT AppVersionCompare('', '9.0.1', '=');
SELECT AppVersionCompare('', '9.0.1', '!=');
SELECT AppVersionCompare('', '9.0.1', '>=');
SELECT AppVersionCompare('', '9.0.1', '<=');
SELECT AppVersionCompare('', '9.0.1', '>');
SELECT AppVersionCompare('', '9.0.1', '<');
SELECT AppVersionCompare('', '9.0.1', '');

SELECT 9;
SELECT AppVersionCompare('', '', '=');
SELECT AppVersionCompare('', '', '!=');
SELECT AppVersionCompare('', '', '>=');
SELECT AppVersionCompare('', '', '<=');
SELECT AppVersionCompare('', '', '>');
SELECT AppVersionCompare('', '', '<');
SELECT AppVersionCompare('', '', '');

SELECT 10;
SELECT AppVersionCompare('9.0.1', '123', '=');
SELECT AppVersionCompare('9.0.1', '123', '!=');
SELECT AppVersionCompare('9.0.1', '123', '>=');
SELECT AppVersionCompare('9.0.1', '123', '<=');
SELECT AppVersionCompare('9.0.1', '123', '>');
SELECT AppVersionCompare('9.0.1', '123', '<');
SELECT AppVersionCompare('9.0.1', '123', '');

SELECT 11;
SELECT AppVersionCompare('123', '123', '=');
SELECT AppVersionCompare('123', '123', '!=');
SELECT AppVersionCompare('123', '123', '>=');
SELECT AppVersionCompare('123', '123', '<=');
SELECT AppVersionCompare('123', '123', '>');
SELECT AppVersionCompare('123', '123', '<');
SELECT AppVersionCompare('123', '123', '');

SELECT 12;
SELECT AppVersionCompare('1234', '123', '=');
SELECT AppVersionCompare('1234', '123', '!=');
SELECT AppVersionCompare('1234', '123', '>=');
SELECT AppVersionCompare('1234', '123', '<=');
SELECT AppVersionCompare('1234', '123', '>');
SELECT AppVersionCompare('1234', '123', '<');
SELECT AppVersionCompare('1234', '123', '');

SELECT 13;
SELECT AppVersionCompare('0', '0', '=');
SELECT AppVersionCompare('0', '0', '!=');
SELECT AppVersionCompare('0', '0', '>=');
SELECT AppVersionCompare('0', '0', '<=');
SELECT AppVersionCompare('0', '0', '>');
SELECT AppVersionCompare('0', '0', '<');
SELECT AppVersionCompare('0', '0', '');

SELECT 14;
SELECT AppVersionCompare('0.7.1', '0.6.0', '=');
SELECT AppVersionCompare('0.7.1', '0.6.0', '!=');
SELECT AppVersionCompare('0.7.1', '0.6.0', '>=');
SELECT AppVersionCompare('0.7.1', '0.6.0', '<=');
SELECT AppVersionCompare('0.7.1', '0.6.0', '>');
SELECT AppVersionCompare('0.7.1', '0.6.0', '<');
SELECT AppVersionCompare('0.7.1', '0.6.0', '');

SELECT 15;
SELECT AppVersionCompare('6.4.0', '6.4', '=');
SELECT AppVersionCompare('6.4.0', '6.4', '!=');
SELECT AppVersionCompare('6.4.0', '6.4', '>=');
SELECT AppVersionCompare('6.4.0', '6.4', '<=');
SELECT AppVersionCompare('6.4.0', '6.4', '>');
SELECT AppVersionCompare('6.4.0', '6.4', '<');
SELECT AppVersionCompare('6.4.0', '6.4', '');

SELECT 16;
SELECT AppVersionCompare('.', '6.4', '=');
SELECT AppVersionCompare('.', '6.4', '!=');
SELECT AppVersionCompare('.', '6.4', '>=');
SELECT AppVersionCompare('.', '6.4', '<=');
SELECT AppVersionCompare('.', '6.4', '>');
SELECT AppVersionCompare('.', '6.4', '<');
SELECT AppVersionCompare('.', '6.4', '');

SELECT 17;
SELECT AppVersionCompare('1.', '1.4', '=');
SELECT AppVersionCompare('1.', '1.4', '!=');
SELECT AppVersionCompare('1.', '1.4', '>=');
SELECT AppVersionCompare('1.', '1.4', '<=');
SELECT AppVersionCompare('1.', '1.4', '>');
SELECT AppVersionCompare('1.', '1.4', '<');
SELECT AppVersionCompare('1.', '1.4', '');

SELECT 18;
SELECT AppVersionCompare('1.', '1.', '=');
SELECT AppVersionCompare('1.', '1.', '!=');
SELECT AppVersionCompare('1.', '1.', '>=');
SELECT AppVersionCompare('1.', '1.', '<=');
SELECT AppVersionCompare('1.', '1.', '>');
SELECT AppVersionCompare('1.', '1.', '<');
SELECT AppVersionCompare('1.', '1.', '');

SELECT 19;
SELECT AppVersionCompare('1.', '1..', '=');
SELECT AppVersionCompare('1.', '1..', '!=');
SELECT AppVersionCompare('1.', '1..', '>=');
SELECT AppVersionCompare('1.', '1..', '<=');
SELECT AppVersionCompare('1.', '1..', '>');
SELECT AppVersionCompare('1.', '1..', '<');
SELECT AppVersionCompare('1.', '1..', '');

SELECT 20;
SELECT AppVersionCompare('.1.', '1..', '=');
SELECT AppVersionCompare('.1.', '1..', '!=');
SELECT AppVersionCompare('.1.', '1..', '>=');
SELECT AppVersionCompare('.1.', '1..', '<=');
SELECT AppVersionCompare('.1.', '1..', '>');
SELECT AppVersionCompare('.1.', '1..', '<');
SELECT AppVersionCompare('.1.', '1..', '');

USE test;
DROP TABLE IF EXISTS test_app_version;
CREATE TABLE test_app_version (app_version String DEFAULT '') Engine = MergeTree ORDER BY app_version;

INSERT INTO table test_app_version VALUES ('1.0.0.1');
INSERT INTO table test_app_version VALUES ('1.0.0.');
INSERT INTO table test_app_version VALUES ('1.0.0');
INSERT INTO table test_app_version VALUES ('1.0.');
INSERT INTO table test_app_version VALUES ('1.0');
INSERT INTO table test_app_version VALUES ('1.');
INSERT INTO table test_app_version VALUES ('1');
INSERT INTO table test_app_version VALUES ('');

INSERT INTO table test_app_version VALUES ('1.0.0.1');
INSERT INTO table test_app_version VALUES ('1.0.0.');
INSERT INTO table test_app_version VALUES ('1.0.0');
INSERT INTO table test_app_version VALUES ('1.0.');
INSERT INTO table test_app_version VALUES ('1.0');
INSERT INTO table test_app_version VALUES ('1.');
INSERT INTO table test_app_version VALUES ('1');
INSERT INTO table test_app_version VALUES ('');

INSERT INTO table test_app_version VALUES ('.');
INSERT INTO table test_app_version VALUES ('.1');
INSERT INTO table test_app_version VALUES ('.1.');
INSERT INTO table test_app_version VALUES ('.1.1');
INSERT INTO table test_app_version VALUES ('1..');
INSERT INTO table test_app_version VALUES ('1..1');
INSERT INTO table test_app_version VALUES ('1.1.1');

INSERT INTO table test_app_version VALUES ('2');
INSERT INTO table test_app_version VALUES ('2.');
INSERT INTO table test_app_version VALUES ('2.0');
INSERT INTO table test_app_version VALUES ('2.0.1');
INSERT INTO table test_app_version VALUES ('2.0.1.');
INSERT INTO table test_app_version VALUES ('2.0.1.1');

INSERT INTO table test_app_version VALUES ('1.1');
INSERT INTO table test_app_version VALUES ('1.0.1');
INSERT INTO table test_app_version VALUES ('1.0.0.2');
INSERT INTO table test_app_version VALUES ('1.1.0.256');
INSERT INTO table test_app_version VALUES ('1.1.1.256');

SELECT 21;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0.1', '=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0.1', '!=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0.1', '>') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0.1', '>=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0.1', '<') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0.1', '<=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0.1', '') ORDER BY app_version;

SELECT 22;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0.', '=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0.', '!=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0.', '>') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0.', '>=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0.', '<') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0.', '<=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0.', '') ORDER BY app_version;

SELECT 23;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0', '=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0', '!=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0', '>') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0', '>=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0', '<') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0', '<=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.0', '') ORDER BY app_version;

SELECT 24;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.', '=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.', '!=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.', '>') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.', '>=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.', '<') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.', '<=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0.', '') ORDER BY app_version;

SELECT 25;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0', '=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0', '!=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0', '>') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0', '>=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0', '<') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0', '<=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.0', '') ORDER BY app_version;

SELECT 26;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.', '=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.', '!=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.', '>') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.', '>=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.', '<') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.', '<=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1.', '') ORDER BY app_version;

SELECT 27;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1', '=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1', '!=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1', '>') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1', '>=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1', '<') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1', '<=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '1', '') ORDER BY app_version;

SELECT 28;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '.', '=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '.', '!=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '.', '>') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '.', '>=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '.', '<') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '.', '<=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '.', '') ORDER BY app_version;

SELECT 29;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '', '=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '', '!=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '', '>') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '', '>=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '', '<') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '', '<=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare(app_version, '', '') ORDER BY app_version;

SELECT 30;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0.1', app_version, '=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0.1', app_version, '!=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0.1', app_version, '>') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0.1', app_version, '>=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0.1', app_version, '<') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0.1', app_version, '<') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0.1', app_version, '') ORDER BY app_version;

SELECT 36;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0.', app_version, '=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0.', app_version, '!=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0.', app_version, '>') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0.', app_version, '>=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0.', app_version, '<') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0.', app_version, '<') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0.', app_version, '') ORDER BY app_version;

SELECT 31;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0', app_version, '=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0', app_version, '!=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0', app_version, '>') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0', app_version, '>=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0', app_version, '<') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0', app_version, '<=') ORDER BY app_version;
SELECT * FROM test_app_version WHERE AppVersionCompare('1.0.0', app_version, '') ORDER BY app_version;

DROP TABLE test_app_version;
CREATE TABLE test_app_version (version_1 String, version_2 String) Engine = MergeTree ORDER BY tuple();

INSERT INTO test_app_version VALUES ('', '');
INSERT INTO test_app_version VALUES ('0', '0');
INSERT INTO test_app_version VALUES ('.', '6.4');
INSERT INTO test_app_version VALUES ('1.', '1.');
INSERT INTO test_app_version VALUES ('1.', '1.4');
INSERT INTO test_app_version VALUES ('1.', '1..');
INSERT INTO test_app_version VALUES ('9.0.1', '');
INSERT INTO test_app_version VALUES ('.1.', '1..');
INSERT INTO test_app_version VALUES ('123', '123');
INSERT INTO test_app_version VALUES ('1234', '123');
INSERT INTO test_app_version VALUES ('6.4', '6.4.');
INSERT INTO test_app_version VALUES ('9.0.1', '123');
INSERT INTO test_app_version VALUES ('6.4', '6.4.0');
INSERT INTO test_app_version VALUES ('6.4.0', '6.4');
INSERT INTO test_app_version VALUES ('0.7.1', '0.6.0');
INSERT INTO test_app_version VALUES ('10.0.0.1', '10.0.');
INSERT INTO test_app_version VALUES ('10.0.', '10.0.0.1');
INSERT INTO test_app_version VALUES ('9.0.1', '10.0.0.1');
INSERT INTO test_app_version VALUES ('10.0.0.1', '10.0.0');
INSERT INTO test_app_version VALUES ('10.0.0.1', '10.0.0.1');

SELECT 32;
SELECT version_1, '=',  version_2, AppVersionCompare(version_1, version_2,  '=') FROM test_app_version ORDER BY version_1, version_2;
SELECT version_1, '!=', version_2, AppVersionCompare(version_1, version_2, '!=') FROM test_app_version ORDER BY version_1, version_2;
SELECT 33;
SELECT version_1, '<',  version_2, AppVersionCompare(version_1, version_2,  '<') FROM test_app_version ORDER BY version_1, version_2;
SELECT version_1, '>',  version_2, AppVersionCompare(version_1, version_2,  '>') FROM test_app_version ORDER BY version_1, version_2;
SELECT 34;
SELECT version_1, '<=', version_2, AppVersionCompare(version_1, version_2, '<=') FROM test_app_version ORDER BY version_1, version_2;
SELECT version_1, '>=', version_2, AppVersionCompare(version_1, version_2, '>=') FROM test_app_version ORDER BY version_1, version_2;

SELECT 35, 'set max_length=2';
SELECT version_1, '=',  version_2, AppVersionCompare(version_1, version_2,  '=', 2) FROM test_app_version ORDER BY version_1, version_2;
SELECT version_1, '!=', version_2, AppVersionCompare(version_1, version_2, '!=', 2) FROM test_app_version ORDER BY version_1, version_2;
SELECT 36, 'set max_length=2';
SELECT version_1, '<',  version_2, AppVersionCompare(version_1, version_2,  '<', 2) FROM test_app_version ORDER BY version_1, version_2;
SELECT version_1, '>',  version_2, AppVersionCompare(version_1, version_2,  '>', 2) FROM test_app_version ORDER BY version_1, version_2;
SELECT 37, 'set max_length=2';
SELECT version_1, '<=', version_2, AppVersionCompare(version_1, version_2, '<=', 2) FROM test_app_version ORDER BY version_1, version_2;
SELECT version_1, '>=', version_2, AppVersionCompare(version_1, version_2, '>=', 2) FROM test_app_version ORDER BY version_1, version_2;

SELECT 38;
SELECT version_1, '=', version_2, AppVersionCompare(version_1, version_2, '=', -1)  FROM test_app_version ORDER BY version_1, version_2;  -- { serverError 43 }
SELECT version_1, '=', version_2, AppVersionCompare(version_1, version_2, '=', 256) FROM test_app_version ORDER BY version_1, version_2;  --{ serverError 43 }
SELECT version_1, '=', version_2, AppVersionCompare(version_1, version_2, '=', 0)   FROM test_app_version ORDER BY version_1, version_2;
SELECT version_1, '=', version_2, AppVersionCompare(version_1, version_2, '=', 255) FROM test_app_version ORDER BY version_1, version_2;

DROP TABLE test_app_version;
