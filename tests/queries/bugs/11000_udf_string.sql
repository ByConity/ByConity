DROP FUNCTION IF EXISTS test.py_script_21;

CREATE FUNCTION test.py_script_21
RETURNS String
LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class py_script_21(IUDF):
    @overload
    def process(a):
        return str(a) + "_successful"

    @overload
    def process(a, b):
        return str(a) + str(b)
$code$;

select test.py_script_21(number) from numbers(10);
select test.py_script_21(number, 2) from numbers(10);

DROP FUNCTION IF EXISTS test.py_script_21;
