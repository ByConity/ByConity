set dialect_type='ANSI';
set enable_optimizer=1;
set enum_replicate=0;
explain
SELECT number IN (
        SELECT number
        FROM system.numbers
        LIMIT 1, 3
    ) AS res
FROM system.numbers
LIMIT 5;
