CREATE DATABASE IF NOT EXISTS db_lambda_udf_3;

DROP FUNCTION IF EXISTS test.test_lambda_array_3;
DROP FUNCTION IF EXISTS db_lambda_udf_3.test_lambda_array_3;

CREATE FUNCTION test.test_lambda_array_3 AS (x) -> arrayCumSum(x);
CREATE FUNCTION db_lambda_udf_3.test_lambda_array_3 AS (x) -> arrayCumSum(x);

DROP FUNCTION test.test_lambda_array_3;
DROP FUNCTION db_lambda_udf_3.test_lambda_array_3;

DROP FUNCTION IF EXISTS db_lambda_udf_3.test_lambda_array_3;

CREATE FUNCTION test.test_lambda_array_3_2 AS (x) -> arrayCumSum(x);
DROP FUNCTION IF EXISTS db_lambda_udf_3.test_lambda_array_3_2;
DROP FUNCTION test.test_lambda_array_3_2;

DROP FUNCTION IF EXISTS test.test_py_udf_3_1;
CREATE FUNCTION test.test_py_udf_3_1 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_py_udf_3_1(IUDF):
    @overload
    def process(a):
        return a + a + 2

    @overload
    def process(a, b):
        return a + b
$code$;

DROP FUNCTION IF EXISTS test.test_py_udf_3_2;
CREATE FUNCTION test.test_py_udf_3_2
RETURNS String
LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_py_udf_3_2(IUDF):
    @overload
    def process(a):
        return str(a) + "_successful"

    @overload
    def process(a, b):
        return str(a + b)
$code$;

drop function if EXISTS db_lambda_udf_3.test_py_udf_3_1;

CREATE FUNCTION db_lambda_udf_3.test_py_udf_3_1 LANGUAGE PYTHON AS
$code$
import numpy
def f()
    print("hi test2")
$code$;

DROP FUNCTION IF EXISTS test.test_py_udf_3_3;
CREATE FUNCTION test.test_py_udf_3_3
RETURNS String
LANGUAGE PYTHON AS
$pikachu$
from iudf import IUDF
from overload import overload

class test_py_udf_3_3(IUDF):
    @overload
    def process(a):
        return str(a) + "_successful"

    @overload
    def process(a, b):
        return str(a + b)
$pikachu$;

DROP FUNCTION IF EXISTS test.test_py_udf_3_4;
CREATE FUNCTION test.test_py_udf_3_4
RETURNS String
LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_py_udf_3_4(IUDF):
    @overload
    def process(a):
        return str(a) + "_successful"

    @overload
    def process(a, b):
        return str(a + b)
$code$;

DROP FUNCTION IF EXISTS test.test_py_udf_3_3;
DROP FUNCTION IF EXISTS test.test_py_udf_3_4;
DROP FUNCTION IF EXISTS test.test_py_udf_3_1;
DROP FUNCTION IF EXISTS db_lambda_udf_3.test_py_udf_3_1;
DROP FUNCTION IF EXISTS test.test_py_udf_3_2;
DROP DATABASE db_lambda_udf_3;