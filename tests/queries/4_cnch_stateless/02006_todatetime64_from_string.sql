SELECT toDateTime64('2021-03-22', 3, 'Asia/Tehran');
SELECT toDateTime64('1921-03-22', 3, 'Asia/Tehran');
SELECT toDateTime64('2021-03-22 19:20:17.123-02:30', 3, 'Asia/Tehran');
SELECT toDateTime64('1900-03-22 19:20:17.123-02:30', 3, 'Asia/Tehran');
SELECT toDateTime64('2299-03-22 19:20:17.123-02:30', 3, 'Asia/Tehran');
SELECT toDateTime64('2021-03-22 19:20:17-02:30', 3, 'Asia/Tehran');
SELECT toDateTime64('1921-03-22 19:20:17-02:30', 3, 'Asia/Tehran');
SELECT toDateTime64('2021-03-22 19:20:17.123455-02:30', 3, 'Asia/Tehran');
SELECT toDateTime64('1901-03-22 19:20:17.123455-02:30', 3, 'Asia/Tehran');
SELECT toDateTime64('2299-03-22 19:20:17.123455-02:30', 3, 'Asia/Tehran');
SELECT toDateTime64('2021-03-22 19:20:17.123455-0230', 3, 'Asia/Tehran'); --{serverError 6}
SELECT toDateTime64('2021-03-22 19:20:17.123455-02.30', 3, 'Asia/Tehran'); --{serverError 6}
SELECT toDateTime64('1921-03-22 19:20:17.123455-02.30', 3, 'Asia/Tehran'); --{serverError 6}