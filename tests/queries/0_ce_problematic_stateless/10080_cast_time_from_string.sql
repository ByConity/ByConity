SELECT '05:20:30.55555'::Time;
SELECT '05:20:30.55555'::Time(6);
SELECT '05:20:30.55555'::Time(9);
SELECT '05:20:30'::Time;
SELECT '05-20-30'::Time; -- { serverError 6 }
SELECT '05:20:30.55555'::Time(12); -- { serverError 69 }
SELECT '05:20:90'::Time; -- { serverError 6 }
SELECT '25:20:50'::Time; -- { serverError 6 }
SELECT '133443'::Time; -- { serverError 6 }

SELECT toTimeType('05:20:30.55555', 3);
SELECT toTimeType('23:20:30.55', 3);
SELECT toTimeType('05:20:30', 3);