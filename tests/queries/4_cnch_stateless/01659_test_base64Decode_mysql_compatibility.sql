SELECT FROM_BASE64('dGVzdCBzdHJpbmc=');
SELECT FROM_BASE64('???'); --{serverError 117}

CREATE TABLE 01659_test_base64Decode_mysql_compatibility (base64_str String) ENGINE = CnchMergeTree ORDER BY tuple();
INSERT INTO 01659_test_base64Decode_mysql_compatibility VALUES ('dGVzdCBzdHJpbmc='), ('???'), ('dGVzdCBzdHJpbmc='), ('???');

SELECT FROM_BASE64(base64_str) FROM 01659_test_base64Decode_mysql_compatibility; --{serverError 117}

DROP TABLE 01659_test_base64Decode_mysql_compatibility;


SET dialect_type='MYSQL';
SELECT FROM_BASE64('dGVzdCBzdHJpbmc=');
SELECT FROM_BASE64('???');

CREATE TABLE 01659_test_base64Decode_mysql_compatibility (base64_str String) ENGINE = CnchMergeTree ORDER BY tuple();
INSERT INTO 01659_test_base64Decode_mysql_compatibility VALUES ('dGVzdCBzdHJpbmc='), ('???'), ('dGVzdCBzdHJpbmc='), ('???');

SELECT FROM_BASE64(base64_str) FROM 01659_test_base64Decode_mysql_compatibility;

DROP TABLE 01659_test_base64Decode_mysql_compatibility;
