CREATE DATABASE IF NOT EXISTS test;

DROP FUNCTION IF EXISTS test.py_script_2;

use test;
DROP FUNCTION IF EXISTS lambda_udf_sum;
DROP FUNCTION IF EXISTS lambda_inner_udf_sum;
DROP FUNCTION IF EXISTS lambda_recursive_udf_sum;

CREATE FUNCTION test.py_script_2
RETURNS Float64
LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class py_script_2(IUDF):
    @overload
    def process(a):
        return a + 65.987

    @overload
    def process(a, b):
        return a * b
$code$;

CREATE FUNCTION lambda_udf_sum AS (x, y) -> x+y;
CREATE FUNCTION lambda_inner_udf_sum AS (x, y) -> lambda_udf_sum(x,y) + y;


select test.py_script_2(number) as col_alias_test from numbers(10) FORMAT TabSeparatedWithNames;

select lambda_inner_udf_sum(number, number) as inner_lambda_test from numbers(10) FORMAT TabSeparatedWithNames;
CREATE FUNCTION lambda_recursive_udf_sum AS (x, y) -> lambda_recursive_udf_sum(x,y) + y;  -- { serverError 612 }

DROP FUNCTION IF EXISTS test.py_script_2;
DROP FUNCTION IF EXISTS lambda_udf_sum;
DROP FUNCTION IF EXISTS lambda_inner_udf_sum;
DROP FUNCTION IF EXISTS lambda_recursive_udf_sum;