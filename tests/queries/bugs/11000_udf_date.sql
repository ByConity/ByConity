DROP FUNCTION IF EXISTS test.py_script_15;

CREATE FUNCTION test.py_script_15
RETURNS Date
LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class py_script_15(IUDF):
    @overload
    def process(a):
        return a + 1

    @overload
    def process(a, b):
        return a + b
$code$;

select test.py_script_15(number) from numbers(10);
select test.py_script_15(number, 100) from numbers(10);

DROP FUNCTION IF EXISTS test.py_script_15;