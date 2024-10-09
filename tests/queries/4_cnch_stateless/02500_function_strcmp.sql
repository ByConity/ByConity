SELECT strcmp('example', 'example');
SELECT strcmp('apple', 'banana');
SELECT strcmp('zebra', 'ant');
SELECT strcmp('', '');
SELECT strcmp('', 'nonempty');
SELECT strcmp('nonempty', '');
SELECT strcmp('CaseTest', 'casetest');
SELECT strcmp('こんにちは', '你好');
SELECT strcmp('test123', 'test@#');
SELECT strcmp('long_string_1', 'long_string_2');
SELECT strcmp(1, '1');
SELECT strcmp(1, '2');
SELECT strcmp(123, '123');
SELECT strcmp(123, '321');
SELECT strcmp(1.23, '1.23');
SELECT strcmp(1.23, '1.24');
SELECT strcmp(1.23456, '1.23456');
SELECT strcmp(1.23456, '1.23457');
SELECT strcmp(1, 1.0);
SELECT strcmp(1, 1.23);
SELECT strcmp('1', 1.0);
SELECT strcmp('1', 1.23);
SELECT strcmp(CAST('1.23' AS DECIMAL(10, 2)), 1.23);
SELECT strcmp(CAST('1.23' AS DECIMAL(10, 2)), '1.23');
SELECT strcmp(CAST('1' AS DECIMAL(10, 0)), 1);
SELECT strcmp(CAST('1' AS DECIMAL(10, 2)), 1);

DROP TABLE IF EXISTS strcmp_table;
CREATE TABLE strcmp_table
(
    id Int64,
    a Nullable(String),
    b String,
    c FixedString(4),
    d Int64 
)
ENGINE = CnchMergeTree()
ORDER BY id;

INSERT INTO strcmp_table (id, a, b) VALUES (0, 'example', 'example'), (1, 'apple', 'banana'),(2, 'zebra', 'ant'),(3, '', ''),(4, '', 'nonempty'),(5, 'nonempty', ''),(6, 'CaseTest', 'casetest'),(7, 'こんにちは', '你好'),(8, 'test123', 'test@#'),(9, 'long_string_1', 'long_string_2'),(10, NULL, 'long_string_2');
INSERT INTO strcmp_table (id, a, b, c, d) VALUES (11, NULL, 'exam', 'exam', 444), (12, 'apple', 'bana', 'bana', 0), (13, 'zebra', 'ant0', 'anof', 123), (14, '888', '888', '888', 888), (15, '', 'none', 'none', 983), (16, 'test123', 'test', 'test', 21), (17, 'long_string_1', 'long', 'losg', -3);

SELECT id, strcmp(a, b) FROM strcmp_table ORDER BY id;
SELECT id, strcmp(a, c) FROM strcmp_table ORDER BY id;
SELECT id, strcmp(b, c) FROM strcmp_table ORDER BY id;
SELECT id, strcmp(b, d) FROM strcmp_table ORDER BY id;
DROP TABLE IF EXISTS strcmp_table;


