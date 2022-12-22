CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS python_join_1;
DROP TABLE IF EXISTS python_join_2;

CREATE TABLE python_join_1(Id Int, name String)
ENGINE = CnchMergeTree()
PRIMARY KEY Id
ORDER BY Id;

CREATE TABLE python_join_2(Id Int, text String, scores Int)
ENGINE = CnchMergeTree()
PRIMARY KEY Id
ORDER BY Id;

INSERT INTO python_join_1 VALUES (1, 'A'), (2, 'B'), (3, 'A');

INSERT INTO python_join_2 VALUES (1, 'Text A', 10), (1, 'Another text A', 20), (2, 'Text B', 30);


DROP FUNCTION IF EXISTS test_python_join_starts_with;

CREATE FUNCTION test_python_join_starts_with RETURNS UInt8 LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_join_starts_with(IUDF):
    @overload
    def process(a, b):
        if (len(b) > len(a)):
            return 0
        for i in range(0, len(b)):
            if (a[i] != b[i]):
                return 0
        return 1
$code$;

select distinct(test_python_join_starts_with(text, 'Text')) as res from python_join_2 order by res;

-- this function is changed to test_python_join_starts_withDistinct and this is not yet supported.
-- select test_python_join_starts_with(distinct(name), 'Text') from python_join_1;

SELECT name, text, test_python_join_starts_with(python_join_2.text, 'Text') FROM python_join_1 LEFT OUTER JOIN python_join_2
    ON python_join_1.Id = python_join_2.Id and test_python_join_starts_with(python_join_2.text, 'Text') = 1 order by name, text SETTINGS join_use_nulls=0;

SELECT name, text, startsWith(python_join_2.text, 'Text') FROM python_join_1 LEFT OUTER JOIN python_join_2
    ON python_join_1.Id = python_join_2.Id and startsWith(python_join_2.text, 'Text') = 1 order by name, text SETTINGS join_use_nulls=0;

DROP FUNCTION IF EXISTS test_python_join_starts_with;
DROP TABLE IF EXISTS python_join_1;
DROP TABLE IF EXISTS python_join_2;
