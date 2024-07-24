SET dialect_type='MYSQL';
SELECT (1 + 2) | 1;
SELECT 1 + (2 | 1);
SELECT 1 + 2 | 1;
SELECT (10 * 2) | 8;
SELECT 10 * (2 | 8);
SELECT 10 * 2 | 8;
SELECT 1 | 2 & 4;
SELECT (1 | 2) & 4;

SELECT 1::Int32 << 2 << 2;
SELECT (1::Int32 << 2) << 2;
SELECT 1::Int32 << (2 << 2);

SELECT 8 >> 2 >> 1;
SELECT (8 >> 2) >> 1;
SELECT 8 >> (2 >> 1);

SELECT 1 + 0 ^ 31 + 42;
SELECT 1 + (0 ^ 31) + 42;
SELECT (1 + 0) ^ (31 + 42);