CREATE DATABASE IF NOT EXISTS test;

DROP FUNCTION IF EXISTS test.py_script_3;

CREATE FUNCTION test.py_script_3
RETURNS String
LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class py_script_3(IUDF):
    @overload
    def process(a):
        return str(a) + "_successful"

    @overload
    def process(a, b):
        return str(a) + str(b)
$code$;

select test.py_script_3(number) from numbers(10);
select test.py_script_3(number, 2) from numbers(10);

DROP FUNCTION IF EXISTS test.py_script_3;
-- DROP FUNCTION IF EXISTS test.py_script_2;