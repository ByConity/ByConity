CREATE DATABASE IF NOT EXISTS test_2;

DROP FUNCTION IF EXISTS test_lambda_array;
DROP FUNCTION IF EXISTS test_2.test_lambda_array;

CREATE FUNCTION test_lambda_array AS (x) -> arrayCumSum(x);
CREATE FUNCTION test_2.test_lambda_array AS (x) -> arrayCumSum(x);

DROP FUNCTION test_lambda_array;
DROP FUNCTION test_2.test_lambda_array;

DROP FUNCTION IF EXISTS test_2.test_lambda_array;

CREATE FUNCTION test_lambda_array_2 LANGUAGE SQL AS (x) -> arrayCumSum(x);
DROP FUNCTION IF EXISTS test_2.test_lambda_array_2;
DROP FUNCTION test_lambda_array_2;

DROP FUNCTION IF EXISTS test_py_udf_1;
CREATE FUNCTION test_py_udf_1 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_py_udf_1(IUDF):
    @overload
    def process(a):
        return a + a + 2

    @overload
    def process(a, b):
        return a + b
$code$;

DROP FUNCTION IF EXISTS test2_py_udf_2;
CREATE FUNCTION test2_py_udf_2
RETURNS String
LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test2_py_udf_2(IUDF):
    @overload
    def process(a):
        return str(a) + "_successful"

    @overload
    def process(a, b):
        return str(a + b)
$code$;

drop function if EXISTS test_2.test_py_udf_1;

CREATE FUNCTION test_2.test_py_udf_1 LANGUAGE PYTHON AS
$code$
import numpy
def f()
    print("hi test2")
$code$;

DROP FUNCTION IF EXISTS test2_py_udf_3;
CREATE FUNCTION test2_py_udf_3
RETURNS String
LANGUAGE PYTHON AS
$pikachu$
from iudf import IUDF
from overload import overload

class test2_py_udf_3(IUDF):
    @overload
    def process(a):
        return str(a) + "_successful"

    @overload
    def process(a, b):
        return str(a + b)
$pikachu$;

DROP FUNCTION IF EXISTS test2_py_udf_4;
CREATE FUNCTION test2_py_udf_4
RETURNS String
LANGUAGE PYTHON AS
$$
from iudf import IUDF
from overload import overload

class test2_py_udf_4(IUDF):
    @overload
    def process(a):
        return str(a) + "_successful"

    @overload
    def process(a, b):
        return str(a + b)
$$;

DROP FUNCTION IF EXISTS test2_py_udf_3;
DROP FUNCTION IF EXISTS test2_py_udf_4;
DROP FUNCTION IF EXISTS test_py_udf_1;
DROP FUNCTION IF EXISTS test_2.test_py_udf_1;
DROP FUNCTION IF EXISTS test2_py_udf_2;
DROP DATABASE test_2;