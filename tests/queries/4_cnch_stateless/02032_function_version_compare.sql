SELECT 1;
SELECT versionCompare('10.0.0.1', '10.0.0.1', '=');
SELECT versionCompare('10.0.0.1', '10.0.0.1', '!=');
SELECT versionCompare('10.0.0.1', '10.0.0.1', '>=');
SELECT versionCompare('10.0.0.1', '10.0.0.1', '<=');
SELECT versionCompare('10.0.0.1', '10.0.0.1', '>');
SELECT versionCompare('10.0.0.1', '10.0.0.1', '<');
SELECT versionCompare('10.0.0.1', '10.0.0.1', '');

SELECT 2;
SELECT versionCompare('10.0.0.1', '10.0.0', '=');
SELECT versionCompare('10.0.0.1', '10.0.0', '!=');
SELECT versionCompare('10.0.0.1', '10.0.0', '>=');
SELECT versionCompare('10.0.0.1', '10.0.0', '<=');
SELECT versionCompare('10.0.0.1', '10.0.0', '>');
SELECT versionCompare('10.0.0.1', '10.0.0', '<');
SELECT versionCompare('10.0.0.1', '10.0.0', '');

SELECT 3;
SELECT versionCompare('10.0.0.1', '10.0.', '=');
SELECT versionCompare('10.0.0.1', '10.0.', '!=');
SELECT versionCompare('10.0.0.1', '10.0.', '>=');
SELECT versionCompare('10.0.0.1', '10.0.', '<=');
SELECT versionCompare('10.0.0.1', '10.0.', '>');
SELECT versionCompare('10.0.0.1', '10.0.', '<');
SELECT versionCompare('10.0.0.1', '10.0.', '');

SELECT 4;
SELECT versionCompare('10.0.0', '10.0.0.1', '=');
SELECT versionCompare('10.0.0', '10.0.0.1', '!=');
SELECT versionCompare('10.0.0', '10.0.0.1', '>=');
SELECT versionCompare('10.0.0', '10.0.0.1', '<=');
SELECT versionCompare('10.0.0', '10.0.0.1', '>');
SELECT versionCompare('10.0.0', '10.0.0.1', '<');
SELECT versionCompare('10.0.0', '10.0.0.1', '');

SELECT 5;
SELECT versionCompare('10.0.', '10.0.0.1', '=');
SELECT versionCompare('10.0.', '10.0.0.1', '!=');
SELECT versionCompare('10.0.', '10.0.0.1', '>=');
SELECT versionCompare('10.0.', '10.0.0.1', '<=');
SELECT versionCompare('10.0.', '10.0.0.1', '>');
SELECT versionCompare('10.0.', '10.0.0.1', '<');
SELECT versionCompare('10.0.', '10.0.0.1', '');

SELECT 6;
SELECT versionCompare('9.0.1', '10.0.0.1', '=');
SELECT versionCompare('9.0.1', '10.0.0.1', '!=');
SELECT versionCompare('9.0.1', '10.0.0.1', '>=');
SELECT versionCompare('9.0.1', '10.0.0.1', '<=');
SELECT versionCompare('9.0.1', '10.0.0.1', '>');
SELECT versionCompare('9.0.1', '10.0.0.1', '<');
SELECT versionCompare('9.0.1', '10.0.0.1', '');

SELECT 7;
SELECT versionCompare('9.0.1', '', '=');
SELECT versionCompare('9.0.1', '', '!=');
SELECT versionCompare('9.0.1', '', '>=');
SELECT versionCompare('9.0.1', '', '<=');
SELECT versionCompare('9.0.1', '', '>');
SELECT versionCompare('9.0.1', '', '<');
SELECT versionCompare('9.0.1', '', '');

SELECT 8;
SELECT versionCompare('', '9.0.1', '=');
SELECT versionCompare('', '9.0.1', '!=');
SELECT versionCompare('', '9.0.1', '>=');
SELECT versionCompare('', '9.0.1', '<=');
SELECT versionCompare('', '9.0.1', '>');
SELECT versionCompare('', '9.0.1', '<');
SELECT versionCompare('', '9.0.1', '');

SELECT 9;
SELECT versionCompare('', '', '=');
SELECT versionCompare('', '', '!=');
SELECT versionCompare('', '', '>=');
SELECT versionCompare('', '', '<=');
SELECT versionCompare('', '', '>');
SELECT versionCompare('', '', '<');
SELECT versionCompare('', '', '');

SELECT 10;
SELECT versionCompare('9.0.1', '123', '=');
SELECT versionCompare('9.0.1', '123', '!=');
SELECT versionCompare('9.0.1', '123', '>=');
SELECT versionCompare('9.0.1', '123', '<=');
SELECT versionCompare('9.0.1', '123', '>');
SELECT versionCompare('9.0.1', '123', '<');
SELECT versionCompare('9.0.1', '123', '');

SELECT 11;
SELECT versionCompare('123', '123', '=');
SELECT versionCompare('123', '123', '!=');
SELECT versionCompare('123', '123', '>=');
SELECT versionCompare('123', '123', '<=');
SELECT versionCompare('123', '123', '>');
SELECT versionCompare('123', '123', '<');
SELECT versionCompare('123', '123', '');

SELECT 12;
SELECT versionCompare('1234', '123', '=');
SELECT versionCompare('1234', '123', '!=');
SELECT versionCompare('1234', '123', '>=');
SELECT versionCompare('1234', '123', '<=');
SELECT versionCompare('1234', '123', '>');
SELECT versionCompare('1234', '123', '<');
SELECT versionCompare('1234', '123', '');

SELECT 13;
SELECT versionCompare('0', '0', '=');
SELECT versionCompare('0', '0', '!=');
SELECT versionCompare('0', '0', '>=');
SELECT versionCompare('0', '0', '<=');
SELECT versionCompare('0', '0', '>');
SELECT versionCompare('0', '0', '<');
SELECT versionCompare('0', '0', '');

SELECT 14;
SELECT versionCompare('0.7.1', '0.6.0', '=');
SELECT versionCompare('0.7.1', '0.6.0', '!=');
SELECT versionCompare('0.7.1', '0.6.0', '>=');
SELECT versionCompare('0.7.1', '0.6.0', '<=');
SELECT versionCompare('0.7.1', '0.6.0', '>');
SELECT versionCompare('0.7.1', '0.6.0', '<');
SELECT versionCompare('0.7.1', '0.6.0', '');

SELECT 15;
SELECT versionCompare('6.4.0', '6.4', '=');
SELECT versionCompare('6.4.0', '6.4', '!=');
SELECT versionCompare('6.4.0', '6.4', '>=');
SELECT versionCompare('6.4.0', '6.4', '<=');
SELECT versionCompare('6.4.0', '6.4', '>');
SELECT versionCompare('6.4.0', '6.4', '<');
SELECT versionCompare('6.4.0', '6.4', '');

SELECT 16;
SELECT versionCompare('.', '6.4', '=');
SELECT versionCompare('.', '6.4', '!=');
SELECT versionCompare('.', '6.4', '>=');
SELECT versionCompare('.', '6.4', '<=');
SELECT versionCompare('.', '6.4', '>');
SELECT versionCompare('.', '6.4', '<');
SELECT versionCompare('.', '6.4', '');

SELECT 17;
SELECT versionCompare('1.', '1.4', '=');
SELECT versionCompare('1.', '1.4', '!=');
SELECT versionCompare('1.', '1.4', '>=');
SELECT versionCompare('1.', '1.4', '<=');
SELECT versionCompare('1.', '1.4', '>');
SELECT versionCompare('1.', '1.4', '<');
SELECT versionCompare('1.', '1.4', '');

SELECT 18;
SELECT versionCompare('1.', '1.', '=');
SELECT versionCompare('1.', '1.', '!=');
SELECT versionCompare('1.', '1.', '>=');
SELECT versionCompare('1.', '1.', '<=');
SELECT versionCompare('1.', '1.', '>');
SELECT versionCompare('1.', '1.', '<');
SELECT versionCompare('1.', '1.', '');

SELECT 19;
SELECT versionCompare('1.', '1..', '=');
SELECT versionCompare('1.', '1..', '!=');
SELECT versionCompare('1.', '1..', '>=');
SELECT versionCompare('1.', '1..', '<=');
SELECT versionCompare('1.', '1..', '>');
SELECT versionCompare('1.', '1..', '<');
SELECT versionCompare('1.', '1..', '');

SELECT 20;
SELECT versionCompare('.1.', '1..', '=');
SELECT versionCompare('.1.', '1..', '!=');
SELECT versionCompare('.1.', '1..', '>=');
SELECT versionCompare('.1.', '1..', '<=');
SELECT versionCompare('.1.', '1..', '>');
SELECT versionCompare('.1.', '1..', '<');
SELECT versionCompare('.1.', '1..', '');

DROP TABLE IF EXISTS test_app_version;
CREATE TABLE test_app_version (version_1 String, version_2 String) Engine = CnchMergeTree ORDER BY tuple();
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

SELECT 38;
SELECT version_1, '=',  version_2, versionCompare(version_1, version_2,  '=') FROM test_app_version ORDER BY version_1, version_2;
SELECT version_1, '!=', version_2, versionCompare(version_1, version_2, '!=') FROM test_app_version ORDER BY version_1, version_2;
SELECT 39;
SELECT version_1, '<',  version_2, versionCompare(version_1, version_2,  '<') FROM test_app_version ORDER BY version_1, version_2;
SELECT version_1, '>',  version_2, versionCompare(version_1, version_2,  '>') FROM test_app_version ORDER BY version_1, version_2;
SELECT 40;
SELECT version_1, '<=', version_2, versionCompare(version_1, version_2, '<=') FROM test_app_version ORDER BY version_1, version_2;
SELECT version_1, '>=', version_2, versionCompare(version_1, version_2, '>=') FROM test_app_version ORDER BY version_1, version_2;

SELECT 41, 'set max_length=2';
SELECT version_1, '=',  version_2, versionCompare(version_1, version_2,  '=', 2) FROM test_app_version ORDER BY version_1, version_2;
SELECT version_1, '!=', version_2, versionCompare(version_1, version_2, '!=', 2) FROM test_app_version ORDER BY version_1, version_2;
SELECT 42, 'set max_length=2';
SELECT version_1, '<',  version_2, versionCompare(version_1, version_2,  '<', 2) FROM test_app_version ORDER BY version_1, version_2;
SELECT version_1, '>',  version_2, versionCompare(version_1, version_2,  '>', 2) FROM test_app_version ORDER BY version_1, version_2;
SELECT 43, 'set max_length=2';
SELECT version_1, '<=', version_2, versionCompare(version_1, version_2, '<=', 2) FROM test_app_version ORDER BY version_1, version_2;
SELECT version_1, '>=', version_2, versionCompare(version_1, version_2, '>=', 2) FROM test_app_version ORDER BY version_1, version_2;

SELECT 44;
SELECT version_1, '=', version_2, versionCompare(version_1, version_2, '=', -1)  FROM test_app_version ORDER BY version_1, version_2;  -- { serverError 43 }
SELECT version_1, '=', version_2, versionCompare(version_1, version_2, '=', 256) FROM test_app_version ORDER BY version_1, version_2;  --{ serverError 43 }
SELECT version_1, '=', version_2, versionCompare(version_1, version_2, '=', 0)   FROM test_app_version ORDER BY version_1, version_2;
SELECT version_1, '=', version_2, versionCompare(version_1, version_2, '=', 255) FROM test_app_version ORDER BY version_1, version_2;

DROP TABLE test_app_version;
