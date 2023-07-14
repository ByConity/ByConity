DROP TABLE IF EXISTS GREGORIAN_DAYS;

CREATE TABLE GREGORIAN_DAYS (
    u16 UInt16,
    i32 Int32, u32 UInt32, i64 Int64,
    null_128 Nullable(Int128)
) ENGINE = CnchMergeTree()
ORDER BY u16;

INSERT INTO GREGORIAN_DAYS (u16, i32, u32, i64, null_128)
SELECT
    number * 20 + 70,
    number * 20000 + 700000,
    number * 20000 + 700000,
    number * 20000 + 700000,
    if(number % 2 == 0, null, number * 20000 + 700000)
FROM numbers(10);

SELECT from_days(u16)
     , from_days(i32), from_days(u32), from_days(i64), from_days(null_128)
FROM GREGORIAN_DAYS;

SELECT from_days(0), from_days(2147483647), from_days(719528)
     , from_days(CAST(1000000000 AS Int64)), from_days(NULL);

SELECT to_days(from_days(u16))
     , to_days(from_days(i32))
     , to_days(from_days(u32))
     , to_days(from_days(i64))
     , to_days(from_days(null_128))
FROM GREGORIAN_DAYS;

SELECT to_days(CAST('1900-01-01' AS Date32))
     , to_days(CAST('1970-03-13' AS Date))
     , to_days(CAST('2100-01-01 19:02:34' AS DateTime))
     , to_days(CAST('2299-12-31 12:32:34.234125' AS DateTime64))
     , to_days(NULL);

