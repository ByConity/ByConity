CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS python_udf;
CREATE TABLE python_udf
(
    a Nullable(String),
    b String
)
ENGINE = CnchMergeTree()
ORDER BY b;

INSERT INTO python_udf (a, b) VALUES ('my name is bahubali', 'age:110'), ('why, is, this, format, stupid', 'look:normal'), ('let-me-do-it', 'country:Singapore'), (NULL, 'country:India'), (NULL, 'country:India'), ('sing', 'city:Mumbai'), ('singh,is,king', 'city:Mumbai'), ('singh--are--king', 'city:Mumbai');


DROP FUNCTION IF EXISTS test_python_string_sanitise;

CREATE FUNCTION test_python_string_sanitise RETURNS String LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

dictvar = {'is': 'are', 'this': 'that'}

class test_python_string_sanitise(IUDF):
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

select a, test_python_string_sanitise(a) from python_udf order by a;

select a, test_python_string_sanitise(a) from python_udf where isNotNull(test_python_string_sanitise(a)) order by a;

select count(*), test_python_string_sanitise(a) from python_udf group by test_python_string_sanitise(a) order by test_python_string_sanitise(a);

select count(*), test_python_string_sanitise(a) from python_udf where isNotNull(test_python_string_sanitise(a)) group by test_python_string_sanitise(a) having count(*) > 1 order by test_python_string_sanitise(a);

select a, b, z from
(
    select a, b, test_python_string_sanitise(a) as z
    from 
    python_udf
    order by test_python_string_sanitise(b) desc
) t1;

DROP FUNCTION IF EXISTS test_python_string_sanitise;
DROP TABLE python_udf;