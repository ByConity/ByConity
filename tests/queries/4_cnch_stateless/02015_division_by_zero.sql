set handle_division_by_zero = true;
-- simple decimal
SELECT 1 / CAST(0, 'Nullable(Decimal(7, 2))');
SELECT materialize(1) / CAST(0, 'Nullable(Decimal(7, 2))');
SELECT 1 / CAST(materialize(0), 'Nullable(Decimal(7, 2))');
SELECT materialize(1) / CAST(materialize(0), 'Nullable(Decimal(7, 2))');


SELECT 1 / CAST(1, 'Nullable(Decimal(7, 2))');
SELECT materialize(1) / CAST(1, 'Nullable(Decimal(7, 2))');
SELECT 1 / CAST(materialize(1), 'Nullable(Decimal(7, 2))');
SELECT materialize(1) / CAST(materialize(1), 'Nullable(Decimal(7, 2))');

SELECT 1 / CAST(0, 'Decimal(7, 2)');
SELECT materialize(1) / CAST(0, 'Decimal(7, 2)');
SELECT 1 / CAST(materialize(0), 'Decimal(7, 2)');
SELECT materialize(1) / CAST(materialize(0), 'Decimal(7, 2)');


SELECT 1 / CAST(1, 'Decimal(7, 2)');
SELECT materialize(1) / CAST(1, 'Decimal(7, 2)');
SELECT 1 / CAST(materialize(1), 'Decimal(7, 2)');
SELECT materialize(1) / CAST(materialize(1), 'Decimal(7, 2)');

-- simple int
SELECT 1 / CAST(0, 'Nullable(Int32)');
SELECT materialize(1) / CAST(0, 'Nullable(Int32)');
SELECT 1 / CAST(materialize(0), 'Nullable(Int32)');
SELECT materialize(1) / CAST(materialize(0), 'Nullable(Int32)');


SELECT 1 / CAST(1, 'Nullable(Int32)');
SELECT materialize(1) / CAST(1, 'Nullable(Int32)');
SELECT 1 / CAST(materialize(1), 'Nullable(Int32)');
SELECT materialize(1) / CAST(materialize(1), 'Nullable(Int32)');

SELECT 1 / 0;
-- SELECT materialize(1) / 0;
SELECT 1 / CAST(materialize(0), 'Int32');
SELECT materialize(1) / CAST(materialize(0), 'Int32');


SELECT 1 / 1;
SELECT materialize(1) / 1;
SELECT 1 / CAST(materialize(1), 'Int32');
SELECT materialize(1) / CAST(materialize(1), 'Int32');

-- intDiv Nullable(Decimal)
SELECT intDiv(1, CAST(0, 'Nullable(Decimal(7, 2))'));
SELECT intDiv(materialize(1), CAST(0, 'Nullable(Decimal(7, 2))'));
SELECT intDiv(1, CAST(materialize(0), 'Nullable(Decimal(7, 2))'));
SELECT intDiv(materialize(1), CAST(materialize(0), 'Nullable(Decimal(7, 2))'));


SELECT intDiv(1, CAST(1, 'Nullable(Decimal(7, 2))'));
SELECT intDiv(materialize(1), CAST(1, 'Nullable(Decimal(7, 2))'));
SELECT intDiv(1, CAST(materialize(1), 'Nullable(Decimal(7, 2))'));
SELECT intDiv(materialize(1), CAST(materialize(1), 'Nullable(Decimal(7, 2))'));

-- intDiv Decimal
SELECT intDiv(1, CAST(0, 'Decimal(7, 2)'));
SELECT intDiv(materialize(1), CAST(0, 'Decimal(7, 2)'));
SELECT intDiv(1, CAST(materialize(0), 'Decimal(7, 2)'));
SELECT intDiv(materialize(1), CAST(materialize(0), 'Decimal(7, 2)'));


SELECT intDiv(1, CAST(1, 'Decimal(7, 2)'));
SELECT intDiv(materialize(1), CAST(1, 'Decimal(7, 2)'));
SELECT intDiv(1, CAST(materialize(1), 'Decimal(7, 2)'));
SELECT intDiv(materialize(1), CAST(materialize(1), 'Decimal(7, 2)'));

SELECT intDiv(1, CAST(0, 'Decimal(7, 2)'));
SELECT intDiv(materialize(1), CAST(0, 'Decimal(7, 2)'));
SELECT intDiv(1, CAST(materialize(0), 'Decimal(7, 2)'));
SELECT intDiv(materialize(1), CAST(materialize(0), 'Decimal(7, 2)'));


SELECT intDiv(1, CAST(1, 'Decimal(7, 2)'));
SELECT intDiv(materialize(1), CAST(1, 'Decimal(7, 2)'));
SELECT intDiv(1, CAST(materialize(1), 'Decimal(7, 2)'));
SELECT intDiv(materialize(1), CAST(materialize(1), 'Decimal(7, 2)'));

-- left is decimal
SELECT toDecimal32(1, 2) / CAST(0, 'Nullable(UInt32)');
SELECT materialize(toDecimal32(1, 2)) / CAST(0, 'Nullable(UInt32)');
SELECT toDecimal32(1, 2) / CAST(materialize(0), 'Nullable(UInt32)');
SELECT materialize(toDecimal32(1, 2)) / CAST(materialize(0), 'Nullable(UInt32)');


SELECT toDecimal32(1, 2) / CAST(1, 'Nullable(UInt32)');
SELECT materialize(toDecimal32(1, 2)) / CAST(1, 'Nullable(UInt32)');
SELECT toDecimal32(1, 2) / CAST(materialize(1), 'Nullable(UInt32)');
SELECT materialize(toDecimal32(1, 2)) / CAST(materialize(1), 'Nullable(UInt32)');

SELECT toDecimal32(1, 2) / CAST(0, 'UInt32');
SELECT materialize(toDecimal32(1, 2)) / CAST(0, 'UInt32');
SELECT toDecimal32(1, 2) / CAST(materialize(0), 'UInt32');
SELECT materialize(toDecimal32(1, 2)) / CAST(materialize(0), 'UInt32');


SELECT toDecimal32(1, 2) / CAST(1, 'UInt32');
SELECT materialize(toDecimal32(1, 2)) / CAST(1, 'UInt32');
SELECT toDecimal32(1, 2) / CAST(materialize(1), 'UInt32');
SELECT materialize(toDecimal32(1, 2)) / CAST(materialize(1), 'UInt32');

-- int div
SELECT intDiv(1, CAST(0, 'Nullable(UInt32)'));
SELECT intDiv(materialize(1), CAST(0, 'Nullable(UInt32)'));
SELECT intDiv(1, CAST(materialize(0), 'Nullable(UInt32)'));
SELECT intDiv(materialize(1), CAST(materialize(0), 'Nullable(UInt32)'));


SELECT intDiv(1, CAST(1, 'Nullable(UInt32)'));
SELECT intDiv(materialize(1), CAST(1, 'Nullable(UInt32)'));
SELECT intDiv(1, CAST(materialize(1), 'Nullable(UInt32)'));
SELECT intDiv(materialize(1), CAST(materialize(1), 'Nullable(UInt32)'));

SELECT intDiv(1, CAST(0, 'UInt32'));
SELECT intDiv(materialize(1), CAST(0, 'UInt32'));
SELECT intDiv(1, CAST(materialize(0), 'UInt32'));
SELECT intDiv(materialize(1), CAST(materialize(0), 'UInt32'));


SELECT intDiv(1, CAST(1, 'UInt32'));
SELECT intDiv(materialize(1), CAST(1, 'UInt32'));
SELECT intDiv(1, CAST(materialize(1), 'UInt32'));
SELECT intDiv(materialize(1), CAST(materialize(1), 'UInt32'));

-- mod
SELECT 1 % CAST(0, 'UInt32');
SELECT materialize(1) % CAST(0, 'UInt32');
SELECT 1 % CAST(materialize(0), 'UInt32');
SELECT materialize(1) % CAST(materialize(0), 'UInt32');


SELECT 1 % CAST(1, 'UInt32');
SELECT materialize(1) % CAST(1, 'UInt32');
SELECT 1 % CAST(materialize(1), 'UInt32');
SELECT materialize(1) % CAST(materialize(1), 'UInt32');


SELECT intDiv(1, CAST(0, 'Float32'));
SELECT intDiv(materialize(1), CAST(0, 'Float32'));
SELECT intDiv(1, CAST(materialize(0), 'Float32'));
SELECT intDiv(materialize(1), CAST(materialize(0), 'Float32'));


SELECT intDiv(1, CAST(1, 'Float32'));
SELECT intDiv(materialize(1), CAST(1, 'Float32'));
SELECT intDiv(1, CAST(materialize(1), 'Float32'));
SELECT intDiv(materialize(1), CAST(materialize(1), 'Float32'));


SELECT 1 % CAST(0, 'Float32');
SELECT materialize(1) % CAST(0, 'Float32');
SELECT 1 % CAST(materialize(0), 'Float32');
SELECT materialize(1) % CAST(materialize(0), 'Float32');


SELECT 1 % CAST(1, 'Float32');
SELECT materialize(1) % CAST(1, 'Float32');
SELECT 1 % CAST(materialize(1), 'Float32');
SELECT materialize(1) % CAST(materialize(1), 'Float32');

-- mod with nullable
SELECT 1 % CAST(0, 'Nullable(UInt32)');
SELECT materialize(1) % CAST(0, 'Nullable(UInt32)');
SELECT 1 % CAST(materialize(0), 'Nullable(UInt32)');
SELECT materialize(1) % CAST(materialize(0), 'Nullable(UInt32)');


SELECT 1 % CAST(1, 'Nullable(UInt32)');
SELECT materialize(1) % CAST(1, 'Nullable(UInt32)');
SELECT 1 % CAST(materialize(1), 'Nullable(UInt32)');
SELECT materialize(1) % CAST(materialize(1), 'Nullable(UInt32)');


SELECT intDiv(1, CAST(0, 'Nullable(Float32)'));
SELECT intDiv(materialize(1), CAST(0, 'Nullable(Float32)'));
SELECT intDiv(1, CAST(materialize(0), 'Nullable(Float32)'));
SELECT intDiv(materialize(1), CAST(materialize(0), 'Nullable(Float32)'));


SELECT intDiv(1, CAST(1, 'Nullable(Float32)'));
SELECT intDiv(materialize(1), CAST(1, 'Nullable(Float32)'));
SELECT intDiv(1, CAST(materialize(1), 'Nullable(Float32)'));
SELECT intDiv(materialize(1), CAST(materialize(1), 'Nullable(Float32)'));


SELECT 1 % CAST(0, 'Nullable(Float32)');
SELECT materialize(1) % CAST(0, 'Nullable(Float32)');
SELECT 1 % CAST(materialize(0), 'Nullable(Float32)');
SELECT materialize(1) % CAST(materialize(0), 'Nullable(Float32)');


SELECT 1 % CAST(1, 'Nullable(Float32)');
SELECT materialize(1) % CAST(1, 'Nullable(Float32)');
SELECT 1 % CAST(materialize(1), 'Nullable(Float32)');
SELECT materialize(1) % CAST(materialize(1), 'Nullable(Float32)');


DROP TABLE IF EXISTS zero_division;
CREATE TABLE zero_division (x UInt32, y Nullable(UInt32), a Decimal(7, 2), b Nullable(Decimal(7, 2))) ENGINE=CnchMergeTree() order by x;
INSERT INTO zero_division VALUES (0, 1, 0, 1), (1, 0, 1, 0), (NULL, 1, NULL, 1), (1, NULL, 1, NULL);

SELECT intDiv(x, y) from zero_division;
SELECT intDiv(y, x) from zero_division;
SELECT x % y from zero_division;
SELECT y % x from zero_division;
SELECT x / y from zero_division;
SELECT y / x from zero_division;

SELECT intDiv(x, a) from zero_division;
SELECT intDiv(a, x) from zero_division;
-- SELECT x % a from zero_division;
-- SELECT a % x from zero_division;
SELECT x / a from zero_division;
SELECT a / x from zero_division;

SELECT intDiv(x, b) from zero_division;
SELECT intDiv(b, x) from zero_division;
-- SELECT x % b from zero_division;
-- SELECT b % x from zero_division;
SELECT x / b from zero_division;
SELECT b / x from zero_division;

SELECT intDiv(y, a) from zero_division;
SELECT intDiv(a, y) from zero_division;
-- SELECT y % a from zero_division;
-- SELECT a % y from zero_division;
SELECT y / a from zero_division;
SELECT a / y from zero_division;

SELECT intDiv(y, b) from zero_division;
SELECT intDiv(b, y) from zero_division;
-- SELECT y % b from zero_division;
-- SELECT b % y from zero_division;
SELECT y / b from zero_division;
SELECT b / y from zero_division;

SELECT intDiv(a, b) from zero_division;
SELECT intDiv(b, a) from zero_division;
-- SELECT a % b from zero_division;
-- SELECT b % a from zero_division;
SELECT a / b from zero_division;
SELECT b / a from zero_division;


DROP TABLE zero_division;
