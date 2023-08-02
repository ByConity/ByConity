DROP TABLE IF EXISTS test.python_udf_8;
CREATE TABLE test.python_udf_8
(
    `event_id` UInt8,
    `timestamp` Date,
    `timestamp_d` DateTime('Europe/Moscow')
)
ENGINE = CnchMergeTree()
ORDER BY timestamp;

INSERT INTO test.python_udf_8 (timestamp, event_id, timestamp_d) VALUES (1546300800, 1, 1546300800), ('2019-01-01', 2, '2019-01-01 00:00:00'), ('2019-01-02', 3, '2019-01-02 00:00:00'), (1546300900, 1, 1546300900);

use test;
DROP FUNCTION IF EXISTS test_python_date_8;
DROP FUNCTION IF EXISTS test_python_date_8time;

CREATE FUNCTION test_python_date_8 RETURNS Date LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_date_8(IUDF):
    @overload
    def process(a):
        return a + 6500
$code$;

CREATE FUNCTION test_python_date_8time RETURNS DateTime LANGUAGE PYTHON AS
$code$
from iudf import IUDF
from overload import overload

class test_python_date_8time(IUDF):
    @overload
    def process(a):
        return a+1000
$code$;

select timestamp_d+1000, timestamp+6500, test_python_date_8time(timestamp_d), test_python_date_8(timestamp) from python_udf_8;

DROP FUNCTION IF EXISTS test_python_date_8;
DROP FUNCTION IF EXISTS test_python_date_8time;

DROP TABLE test.python_udf_8;
