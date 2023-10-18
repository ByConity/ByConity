SELECT '-----Add Date-----';
SELECT addYears('2000-12-31 19:24:45', 1);
SELECT addYears('2000-12-31 19:24:45', -1);
SELECT addMonths('2000-12-31 19:24:45', 1);
SELECT addMonths('2000-12-31 19:24:45', -1);
SELECT addDays('2000-12-31 19:24:45', 5);
SELECT addDays('2000-12-31 19:24:45', -5);
SELECT addHours('2000-12-31 19:24:45', 10);
SELECT addHours('2000-12-31 19:24:45', -10);
SELECT addMinutes('2000-12-31 19:24:45', 100);
SELECT addMinutes('2000-12-31 19:24:45', -100);
SELECT addSeconds('2000-12-31 19:24:45', 1000);
SELECT addSeconds('2000-12-31 19:24:45', -1000);

SELECT '-----Minus Date-----';
SELECT subtractYears('2000-12-31 19:24:45', 1);
SELECT subtractYears('2000-12-31 19:24:45', -1);
SELECT subtractMonths('2000-12-31 19:24:45', 1);
SELECT subtractMonths('2000-12-31 19:24:45', -1);
SELECT subtractDays('2000-12-31 19:24:45', 5);
SELECT subtractDays('2000-12-31 19:24:45', -5);
SELECT subtractHours('2000-12-31 19:24:45', 10);
SELECT subtractHours('2000-12-31 19:24:45', -10);
SELECT subtractMinutes('2000-12-31 19:24:45', 100);
SELECT subtractMinutes('2000-12-31 19:24:45', -100);
SELECT subtractSeconds('2000-12-31 19:24:45', 1000);
SELECT subtractSeconds('2000-12-31 19:24:45', -1000);

SELECT '-----Test with FixedString and vector-----';
SELECT
    addYears(concat('2079-04-28 10:', toString(10 + number), ':45'), 5),
    addMonths(concat('2079-04-28 10:', toString(10 + number), ':45'), 5),
    addDays(toFixedString(concat('2079-04-28 10:', toString(10 + number), ':45'), 19), 5),
    addHours(concat('2079-04-28 10:', toString(10 + number), ':45'), -5),
    addMinutes(concat('2079-04-28 10:', toString(10 + number), ':45'), 40),
    addSeconds(toFixedString(concat('2079-04-28 10:', toString(10 + number), ':45'), 19), 50)
FROM numbers(20);
