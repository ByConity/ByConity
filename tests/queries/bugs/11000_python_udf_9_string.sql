DROP TABLE IF EXISTS test.python_udf_9;
CREATE TABLE test.python_udf_9
(
    a Nullable(String),
    b String
)
ENGINE = CnchMergeTree()
ORDER BY b;

INSERT INTO test.python_udf_9 (a, b) VALUES ('my name is bahubali', 'age:110'), ('why, is, this, format, stupid', 'look:normal'), ('let-me-do-it', 'country:Singapore'), (NULL, 'country:India'), (NULL, 'country:India'), ('sing', 'city:Mumbai'), ('singh,is,king', 'city:Mumbai'), ('singh--are--king', 'city:Mumbai');

use test;
DROP FUNCTION IF EXISTS test_python_string_sanitise_9;

CREATE FUNCTION test_python_string_sanitise_9 RETURNS String LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

dictvar = {'is': 'are', 'this': 'that'}

class test_python_string_sanitise_9(IUDF):
    @overload
    def process(a):
        ans = ""
        word = ""
        for i in a:
            if (i == ' ' or i == ',' or i == '-'):
                if word != "":
                    if word in dictvar:
                        word = dictvar[word]
                    if ans == "":
                        ans = word
                    else:
                        ans = ans + '::' + word
                    word = ""
            else:
                word = word + i
        if word != "":
            if ans == "":
                ans = word
            else:
                ans = ans + '::' + word
        return ans
$code$;

select a, test_python_string_sanitise_9(a) from python_udf_9 order by a;

select a, test_python_string_sanitise_9(a) from python_udf_9 where isNotNull(test_python_string_sanitise_9(a)) order by a;

select count(*), test_python_string_sanitise_9(a) from python_udf_9 group by test_python_string_sanitise_9(a) order by test_python_string_sanitise_9(a);

select count(*), test_python_string_sanitise_9(a) from python_udf_9 where isNotNull(test_python_string_sanitise_9(a)) group by test_python_string_sanitise_9(a) having count(*) > 1 order by test_python_string_sanitise_9(a);

select a, b, z from
(
    select a, b, test_python_string_sanitise_9(a) as z
    from 
    python_udf_9
    order by test_python_string_sanitise_9(b) desc
) t1;

DROP FUNCTION IF EXISTS test_python_string_sanitise_9;
DROP TABLE test.python_udf_9;