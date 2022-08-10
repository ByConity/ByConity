CREATE DATABASE IF NOT EXISTS "test.sdcsd";

DROP TABLE IF EXISTS "test.sdcsd".python_udf;

CREATE TABLE "test.sdcsd".python_udf(id Int, val Int)
ENGINE = CnchMergeTree()
PRIMARY KEY id
ORDER BY id;

INSERT INTO "test.sdcsd".python_udf VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);

use "test.sdcsd";
DROP FUNCTION IF EXISTS test_python_limit_by;

CREATE FUNCTION test_python_limit_by RETURNS Int64 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_limit_by(IUDF):
    @overload
    def process(a, b):
        return a+b
$code$;

SELECT test_python_limit_by(id, 0), id, val FROM python_udf ORDER BY id, val LIMIT 2 BY test_python_limit_by(id, 0);

DROP FUNCTION IF EXISTS "test.sdcsd".test_python_limit_by;

CREATE DATABASE IF NOT EXISTS "`\`\``";
DROP FUNCTION IF EXISTS "`\`\``".test_python_limit_by;

CREATE FUNCTION "`\`\``".test_python_limit_by RETURNS Int64 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_limit_by(IUDF):
    @overload
    def process(a, b):
        return a+b+10
$code$;

SELECT "`\`\``".test_python_limit_by(id, 0), id, val FROM "test.sdcsd".python_udf ORDER BY id, val LIMIT 2 BY "`\`\``".test_python_limit_by(id, 0);
DROP FUNCTION IF EXISTS "`\`\``".test_python_limit_by;

CREATE DATABASE IF NOT EXISTS "`\``";
DROP FUNCTION IF EXISTS "`\``".test_python_limit_by;

CREATE FUNCTION "`\``".test_python_limit_by RETURNS Int64 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_limit_by(IUDF):
    @overload
    def process(a, b):
        return a+b+20
$code$;

SELECT "`\``".test_python_limit_by(id, 0), id, val FROM "test.sdcsd".python_udf ORDER BY id, val LIMIT 2 BY "`\``".test_python_limit_by(id, 0);

DROP FUNCTION IF EXISTS "`\``".test_python_limit_by;

DROP TABLE IF EXISTS "test.sdcsd".python_udf;