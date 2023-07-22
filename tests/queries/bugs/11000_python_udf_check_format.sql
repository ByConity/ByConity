
DROP TABLE IF EXISTS "test".python_udf_14;

CREATE TABLE "test".python_udf_14(id Int, val Int)
ENGINE = CnchMergeTree()
PRIMARY KEY id
ORDER BY id;

INSERT INTO "test".python_udf_14 VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);

use "test";
DROP FUNCTION IF EXISTS test_python_limit_by_14;

CREATE FUNCTION test_python_limit_by_14 RETURNS Int64 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_limit_by_14(IUDF):
    @overload
    def process(a, b):
        return a+b
$code$;

SELECT test_python_limit_by_14(id, 0), id, val FROM python_udf_14 ORDER BY id, val LIMIT 2 BY test_python_limit_by_14(id, 0);

DROP FUNCTION IF EXISTS "test".test_python_limit_by_14;

CREATE DATABASE IF NOT EXISTS "db_test1_check_format";
DROP FUNCTION IF EXISTS "db_test1_check_format".test_python_limit_by_14;

CREATE FUNCTION "db_test1_check_format".test_python_limit_by_14 RETURNS Int64 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_limit_by_14(IUDF):
    @overload
    def process(a, b):
        return a+b+10
$code$;

SELECT "db_test1_check_format".test_python_limit_by_14(id, 0), id, val FROM "test".python_udf_14 ORDER BY id, val LIMIT 2 BY "db_test1_check_format".test_python_limit_by_14(id, 0);
DROP FUNCTION IF EXISTS "db_test1_check_format".test_python_limit_by_14;

CREATE DATABASE IF NOT EXISTS "db_test2_check_format";
DROP FUNCTION IF EXISTS "db_test2_check_format".test_python_limit_by_14;

CREATE FUNCTION "db_test2_check_format".test_python_limit_by_14 RETURNS Int64 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_limit_by_14(IUDF):
    @overload
    def process(a, b):
        return a+b+20
$code$;

SELECT "db_test2_check_format".test_python_limit_by_14(id, 0), id, val FROM "test".python_udf_14 ORDER BY id, val LIMIT 2 BY "db_test2_check_format".test_python_limit_by_14(id, 0);

DROP FUNCTION IF EXISTS "db_test2_check_format".test_python_limit_by_14;

DROP TABLE IF EXISTS "test".python_udf_14;
DROP DATABASE IF EXISTS "db_test1_check_format";
DROP DATABASE IF EXISTS "db_test2_check_format";