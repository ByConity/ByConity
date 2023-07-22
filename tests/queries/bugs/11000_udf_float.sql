DROP FUNCTION IF EXISTS test.py_script_18;

CREATE FUNCTION test.py_script_18
RETURNS Float64
LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class py_script_18(IUDF):
    @overload
    def process(a):
        return a + 65.987

    @overload
    def process(a, b):
        return a * b
$code$;

select test.py_script_18(number) from numbers(10);
select test.py_script_18(number, 2.675) from numbers(10);

DROP FUNCTION IF EXISTS test.py_script_18;