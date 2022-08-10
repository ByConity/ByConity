USE test;
DROP TABLE IF EXISTS test_conv;

CREATE TABLE test_conv (a Int64, str String) ENGINE = Memory;
INSERT INTO test_conv values (1, '1'), (2, '2'), (9223372036854775807, '9223372036854775807'), (-9223372036854775808, '9223372036854775808');

SELECT conv(a, 10, 2) AS a, conv(str, 10, 2) AS b FROM test_conv ORDER BY a;
SELECT conv(conv(a, 10, 2), 2, 10) AS a, conv(conv(str, 10, 2), 2, 10) AS b FROM test_conv ORDER BY a;
SELECT conv(conv(a, 10, 2), 2, 36) AS a, conv(conv(str, 10, 2), 2, 36) AS b FROM test_conv ORDER BY a;

SELECT conv(a, materialize(2), 10) FROM test_conv; -- { serverError 44 }

SELECT conv(11101, 2, 10);
SELECT conv(-1123, 2, 10);
SELECT conv(-0, 2, 10);
SELECT conv(0, 2, 10);

SELECT conv('ZZZ', 36, 10);
SELECT conv('-46655', 10, 36);
SELECT conv('1101A', 10, 10);
SELECT conv('##', 10, 10);

DROP TABLE test_conv;