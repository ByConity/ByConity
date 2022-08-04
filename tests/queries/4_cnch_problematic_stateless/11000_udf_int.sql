CREATE DATABASE IF NOT EXISTS test;

DROP FUNCTION IF EXISTS test.py_script_20;

CREATE FUNCTION test.py_script_20 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class py_script_20(IUDF):
    @overload
    def process(a):
        return a + a + 65

    @overload
    def process(a, b):
        return a + b + 1
$code$;

select test.py_script_20(number) from numbers(10);

DROP FUNCTION IF EXISTS test.py_script_20;

CREATE FUNCTION test.py_script_20 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class py_script_20(IUDF):
    @overload
    def process(a):
        return a + a + 100

    @overload
    def process(a, b):
        return a + b + 1
$code$;

select test.py_script_20(number) from numbers(10);

DROP FUNCTION IF EXISTS test.py_script_20;