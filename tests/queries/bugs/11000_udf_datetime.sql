DROP FUNCTION IF EXISTS test.py_script_16;

CREATE FUNCTION test.py_script_16
RETURNS DateTime
LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class py_script_16(IUDF):
    @overload
    def process(a):
        return a + 1

    @overload
    def process(a, b):
        return a + b
$code$;

select test.py_script_16(number) from numbers(10);
select test.py_script_16(number, 100) from numbers(10);

DROP FUNCTION IF EXISTS test.py_script_16;