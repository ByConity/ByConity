CREATE DATABASE IF NOT EXISTS test;

DROP FUNCTION IF EXISTS py_script_33;

CREATE FUNCTION py_script_33
RETURNS FixedString(12)
LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class py_script_33(IUDF):
    @overload
    def process(a):
        return str(a) + "_successful"

$code$;


select py_script_33(number) from numbers(10);

DROP FUNCTION IF EXISTS py_script_33;

DROP TABLE IF EXISTS py_fixedstring;

CREATE TABLE py_fixedstring
(
    a FixedString(2),
    b FixedString(3)
)
ENGINE = CnchMergeTree()
ORDER BY b;


INSERT INTO py_fixedstring(a, b)
VALUES ('aa', 'aaa') ('bb', 'bbb') ('cc', 'ccc') ('dd', 'ddd') ('ee', 'eee') ('ff', 'fff') ('gg', 'ggg') ('ab', 'abc') ('cb', 'cba') ('tt', 'zzz');

CREATE FUNCTION py_script_33
RETURNS FixedString(5)
LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class py_script_33(IUDF):
    @overload
    def process(a, b):
        return a + b

$code$;

select py_script_33(a, b) from py_fixedstring;

DROP FUNCTION IF EXISTS py_script_33;
DROP TABLE IF EXISTS py_fixedstring;