SET session_timezone = 'Абырвалг'; -- { serverError BAD_ARGUMENTS}

-- Pacific/Pitcairn = UTC-8
SELECT timezone(), timezoneOf(now()) SETTINGS session_timezone = 'Pacific/Pitcairn';

-- Asia/Novosibirsk = UTC+7
SET session_timezone = 'Asia/Novosibirsk';
SELECT timezone(), timezoneOf(now());

-- test time functions
-- Asia/Novosibirsk will work
SELECT toDateTime('2022-12-12 23:23:23');
-- Pacific/Pitcairn will work
SELECT toDateTime('2022-12-12 23:23:23') SETTINGS session_timezone = 'Pacific/Pitcairn';
-- Europe/Zurich = UTC+1, Asia/Novosibirsk will work first, then Europe/Zurich
SELECT toDateTime(toDateTime('2022-12-12 23:23:23'), 'Europe/Zurich');
-- America/Denver = UTC-7, America/Denver will work first, then Europe/Zurich
SELECT toDateTime64(toDateTime64('2022-12-12 23:23:23.123', 3), 3, 'Europe/Zurich') SETTINGS session_timezone = 'America/Denver';

-- subquery shall use main query's session_timezone
SELECT toDateTime(toDateTime('2022-12-12 23:23:23'), 'Europe/Zurich'), (SELECT toDateTime(toDateTime('2022-12-12 23:23:23'), 'Europe/Zurich') SETTINGS session_timezone = 'Europe/Helsinki') SETTINGS session_timezone = 'America/Denver';

-- test proper serialization
-- Asia/Phnom_Penh=UTC+7
SELECT toDateTime('2002-12-12 23:23:23') AS dt, toString(dt) SETTINGS session_timezone = 'Asia/Phnom_Penh';
SELECT toDateTime64('2002-12-12 23:23:23.123', 3) AS dt64, toString(dt64) SETTINGS session_timezone = 'Asia/Phnom_Penh';
