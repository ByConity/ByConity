SET send_logs_level = 'fatal';

SELECT formatDateTime(); -- { serverError 42 }
SELECT formatDateTime('not a datetime', 'IGNORED'); -- { serverError 6 }
SELECT formatDateTime(now(), now()); -- { serverError 43 }
SELECT formatDateTime(now(), 'good format pattern', now()); -- { serverError 43 }
SELECT formatDateTime(now(), 'unescaped %'); -- { serverError 36 }

SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%a'), formatDateTime(toDate32('2018-01-02'), '%a');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%b'), formatDateTime(toDate32('2018-01-02'), '%b');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%c'), formatDateTime(toDate32('2018-01-02'), '%c');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%C'), formatDateTime(toDate32('2018-01-02'), '%C');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%d'), formatDateTime(toDate32('2018-01-02'), '%d');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%D'), formatDateTime(toDate32('2018-01-02'), '%D');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%e'), formatDateTime(toDate32('2018-01-02'), '%e');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%f'), formatDateTime(toDate32('2018-01-02'), '%f');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%F'), formatDateTime(toDate32('2018-01-02'), '%F');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%h'), formatDateTime(toDate32('2018-01-02'), '%h');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%H'), formatDateTime(toDate32('2018-01-02'), '%H');
SELECT formatDateTime(toDateTime('2018-01-02 02:33:44'), '%H');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%i'), formatDateTime(toDate32('2018-01-02'), '%i');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%I'), formatDateTime(toDate32('2018-01-02'), '%I');
SELECT formatDateTime(toDateTime('2018-01-02 11:33:44'), '%I');
SELECT formatDateTime(toDateTime('2018-01-02 00:33:44'), '%I');
SELECT formatDateTime(toDateTime('2018-01-01 00:33:44'), '%j'), formatDateTime(toDate32('2018-01-01'), '%j');
SELECT formatDateTime(toDateTime('2000-12-31 00:33:44'), '%j'), formatDateTime(toDate32('2000-12-31'), '%j');
SELECT formatDateTime(toDateTime('2000-12-31 00:33:44'), '%k'), formatDateTime(toDate32('2000-12-31'), '%k');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%m'), formatDateTime(toDate32('2018-01-02'), '%m');
set formatdatetime_parsedatetime_m_is_month_name = 0;
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%M'), formatDateTime(toDate32('2018-01-02'), '%M');
set formatdatetime_parsedatetime_m_is_month_name = 1;
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%M'), formatDateTime(toDate32('2018-01-02'), '%M');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%n'), formatDateTime(toDate32('2018-01-02'), '%n');
SELECT formatDateTime(toDateTime('2018-01-02 00:33:44'), '%p'), formatDateTime(toDateTime('2018-01-02'), '%p');
SELECT formatDateTime(toDateTime('2018-01-02 11:33:44'), '%p');
SELECT formatDateTime(toDateTime('2018-01-02 12:33:44'), '%p');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%r'), formatDateTime(toDate32('2018-01-02'), '%r');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%R'), formatDateTime(toDate32('2018-01-02'), '%R');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%S'), formatDateTime(toDate32('2018-01-02'), '%S');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%t'), formatDateTime(toDate32('2018-01-02'), '%t');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%T'), formatDateTime(toDate32('2018-01-02'), '%T');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%W'), formatDateTime(toDate32('2018-01-02'), '%W');
SELECT formatDateTime(toDateTime('2018-01-01 22:33:44'), '%u'), formatDateTime(toDateTime('2018-01-07 22:33:44'), '%u'),
       formatDateTime(toDate32('2018-01-01'), '%u'), formatDateTime(toDate32('2018-01-07'), '%u');
SELECT formatDateTime(toDateTime('1996-01-01 22:33:44'), '%V'), formatDateTime(toDateTime('1996-12-31 22:33:44'), '%V'),
       formatDateTime(toDateTime('1999-01-01 22:33:44'), '%V'), formatDateTime(toDateTime('1999-12-31 22:33:44'), '%V'),
       formatDateTime(toDate32('1996-01-01'), '%V'), formatDateTime(toDate32('1996-12-31'), '%V'),
       formatDateTime(toDate32('1999-01-01'), '%V'), formatDateTime(toDate32('1999-12-31'), '%V');
SELECT formatDateTime(toDateTime('2018-01-01 22:33:44'), '%w'), formatDateTime(toDateTime('2018-01-07 22:33:44'), '%w'),
       formatDateTime(toDate32('2018-01-01'), '%w'), formatDateTime(toDate32('2018-01-07'), '%w');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%y'), formatDateTime(toDate32('2018-01-02'), '%y');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%Y'), formatDateTime(toDate32('2018-01-02'), '%Y');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%%'), formatDateTime(toDate32('2018-01-02'), '%%');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), 'no formatting pattern'), formatDateTime(toDate32('2018-01-02'), 'no formatting pattern');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%U');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%v');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%x');
SELECT formatDateTime(toDateTime('2018-01-02 22:33:44'), '%X');
SELECT formatDateTime(toDate('2018-01-01'), '%F %T');
SELECT formatDateTime(toDate32('1927-01-01'), '%F %T');

SELECT
    formatDateTime(toDateTime('2018-01-01 01:00:00', 'UTC'), '%F %T', 'UTC'),
    formatDateTime(toDateTime('2018-01-01 01:00:00', 'UTC'), '%F %T', 'Asia/Istanbul');

SELECT formatDateTime(toDateTime('2020-01-01 01:00:00', 'UTC'), '%z');
SELECT formatDateTime(toDateTime('2020-01-01 01:00:00', 'US/Samoa'), '%z');
SELECT formatDateTime(toDateTime('2020-01-01 01:00:00', 'Europe/Moscow'), '%z');
SELECT formatDateTime(toDateTime('1970-01-01 00:00:00', 'Asia/Kolkata'), '%z');

-- %f (default settings)
set formatdatetime_f_prints_single_zero = 0;
select formatDateTime(toDate('2010-01-04'), '%f');
select formatDateTime(toDate32('2010-01-04'), '%f');
select formatDateTime(toDateTime('2010-01-04 12:34:56'), '%f');
select formatDateTime(toDateTime64('2010-01-04 12:34:56', 0), '%f');
select formatDateTime(toDateTime64('2010-01-04 12:34:56.123', 3), '%f');
select formatDateTime(toDateTime64('2010-01-04 12:34:56.123456', 6), '%f');
select formatDateTime(toDateTime64('2010-01-04 12:34:56.123456789', 9), '%f');
-- %f (legacy settings)
set formatdatetime_f_prints_single_zero = 1;
select formatDateTime(toDate('2010-01-04'), '%f');
select formatDateTime(toDate32('2010-01-04'), '%f');
select formatDateTime(toDateTime('2010-01-04 12:34:56'), '%f');
select formatDateTime(toDateTime64('2010-01-04 12:34:56', 0), '%f');
select formatDateTime(toDateTime64('2010-01-04 12:34:56.123', 3), '%f');
select formatDateTime(toDateTime64('2010-01-04 12:34:56.123456', 6), '%f');
select formatDateTime(toDateTime64('2010-01-04 12:34:56.123456789', 9), '%f');

select formatDateTime(toDateTime64('2022-12-08 18:11:29.1234', 9, 'UTC'), '%F %T.%f');
select formatDateTime(toDateTime64('2022-12-08 18:11:29.1234', 1, 'UTC'), '%F %T.%f');
select formatDateTime(toDateTime64('2022-12-08 18:11:29.1234', 0, 'UTC'), '%F %T.%f');
select formatDateTime(toDateTime('2022-12-08 18:11:29', 'UTC'), '%F %T.%f');
select formatDateTime(toDate('2022-12-08 18:11:29', 'UTC'), '%F %T.%f');

set formatdatetime_f_prints_single_zero = 0;
select formatDateTime(toDateTime64('2022-12-08 18:11:29.1234', 9, 'UTC'), '%F %T.%f');
select formatDateTime(toDateTime64('2022-12-08 18:11:29.1234', 1, 'UTC'), '%F %T.%f');
select formatDateTime(toDateTime64('2022-12-08 18:11:29.1234', 0, 'UTC'), '%F %T.%f');
select formatDateTime(toDateTime('2022-12-08 18:11:29', 'UTC'), '%F %T.%f');
select formatDateTime(toDate('2022-12-08 18:11:29', 'UTC'), '%F %T.%f');

-- date_format
SELECT date_format(); -- { serverError 42 }
SELECT date_format('not a datetime', 'IGNORED'); -- { serverError 41 }
SELECT date_format(now(), now()); -- { serverError 43 }
SELECT date_format(now(), 'good format pattern', now()); -- { serverError 43 }
SELECT date_format(now(), 'unescaped %'); -- { serverError 2 }
SELECT date_format(toDateTime('2018-01-02 22:33:44'), 'A'); -- { serverError 2 }
SELECT date_format(toDateTime('2018-01-02 22:33:44'), 'P'); -- { serverError 2 }

SELECT date_format(toDateTime('2018-01-02 22:33:44'), 'y');
SELECT date_format(toDateTime('2018-01-02 22:33:44'), 'M');
SELECT date_format(toDateTime('2018-01-02 22:33:44'), 'd');
SELECT date_format(toDateTime('2018-01-02 22:33:44'), 'H');
SELECT date_format(toDateTime('2018-01-02 02:33:44'), 'm');
SELECT date_format(toDateTime('2018-01-02 22:33:44'), 's');
SELECT date_format('2018-01-02 00:33:44', 'D');
SELECT date_format('2018-01-02 22:33:44', 'a');
SELECT date_format('2018-01-02 00:33:44', 'K');
SELECT date_format('2018-01-02 22:33:44', 'Y');

SELECT date_format(toDate('2018-01-02'), 'y');
SELECT date_format(toDate('2018-01-02'), 'M');
SELECT date_format(toDate('2018-01-02'), 'd');
SELECT date_format(toDate('2018-01-02'), 'H');
SELECT date_format(toDate('2018-01-02'), 'm');
SELECT date_format(toDate('2018-01-02'), 's');
SELECT date_format('2018-01-02', 'D');
SELECT date_format('2018-01-02', 'a');
SELECT date_format('2018-01-02', 'K');
SELECT date_format('2018-01-02', 'Y');

SELECT date_format(toDateTime('2018-01-02 22:33:44'), 'G'); -- { serverError 48 }
SELECT date_format(toDateTime('2018-01-02 22:33:44'), 'k'); -- { serverError 48 }
SELECT date_format(toDateTime('2018-01-02 11:33:44'), 'S'); -- { serverError 48 }
SELECT date_format(toDateTime('2018-01-01 00:33:44'), 'F'); -- { serverError 48 }
SELECT date_format(toDateTime('2000-12-31 00:33:44'), 'w'); -- { serverError 48 }
SELECT date_format(toDateTime('2018-01-02 22:33:44'), 'W'); -- { serverError 48 }
SELECT date_format(toDateTime('2018-01-02 22:33:44'), 'h'); -- { serverError 48 }
SELECT date_format(toDateTime('2018-01-02 11:33:44'), 'z'); -- { serverError 48 }
SELECT date_format(toDateTime('2018-01-02 12:33:44'), 'Z'); -- { serverError 48 }
SELECT date_format(toDateTime('2018-01-02 22:33:44'), 'u'); -- { serverError 48 }
SELECT date_format(toDateTime('2018-01-02 22:33:44'), 'X'); -- { serverError 48 }
SELECT date_format(toDateTime('2018-01-02 22:33:44'), 'L'); -- { serverError 48 }

SELECT date_format(toDateTime('2018-01-02 22:33:44'), '%%');

SELECT date_format(toDateTime('2018-01-01 13:00:00', 'UTC'), 'yyyy-MM-dd HH:mm:ss', 'UTC');
SELECT date_format(toDateTime('2018-01-01 13:00:00', 'UTC'), 'yyyy-MM-dd KK:mm:ss a', 'UTC');
SELECT date_format(toDateTime('2018-01-01 23:00:00', 'UTC'), 'yyyy-MM-dd HH:mm:ss', 'Asia/Shanghai');
SELECT date_format(toDateTime('2018-01-01 23:00:00', 'UTC'), 'yyyy-MM-dd KK:mm:ss a', 'Asia/Shanghai');
