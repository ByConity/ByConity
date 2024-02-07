set dialect_type='MYSQL';
SELECT '----compatible patterns----';
SELECT date_format_mysql(toDateTime('2018-01-02 22:33:44', 'UTC'), '%d');
SELECT date_format_mysql(toDateTime('2018-01-02 02:33:44', 'UTC'), '%H');
SELECT date_format_mysql(toDateTime('2018-01-02 22:33:44', 'UTC'), '%m');
SELECT date_format_mysql(toDateTime('2018-01-02 22:33:44', 'UTC'), '%p');
SELECT date_format_mysql(toDateTime('2018-01-02 22:33:44', 'UTC'), '%S');
SELECT date_format_mysql(toDateTime('2018-01-02 22:33:44', 'UTC'), '%T');
SELECT date_format_mysql(toDateTime('2018-01-02 22:33:44', 'UTC'), '%Y');
SELECT date_format_mysql(toDateTime('2018-01-02 22:33:44', 'UTC'), '%y');
SELECT date_format_mysql(toDateTime('2018-01-01 22:33:44', 'UTC'), '%w'),
       date_format_mysql(toDateTime('2018-01-07 22:33:44', 'UTC'), '%w');

SELECT '----converted/specific patterns----';
SELECT date_format_mysql(toDateTime('2018-01-02 22:33:44', 'UTC'), '%a');
SELECT date_format_mysql(toDateTime('2018-01-02 22:33:44', 'UTC'), '%b');
SELECT date_format_mysql(toDateTime('2018-01-02 22:33:44', 'UTC'), '%r');
SELECT date_format_mysql(toDate('2018-01-02'), '%r');
SELECT date_format_mysql(toDateTime('2018-01-02 22:33:44', 'UTC'), '%h'),
       date_format_mysql(toDateTime('2018-01-02 11:33:44', 'UTC'), '%h'),
       date_format_mysql(toDateTime('2018-01-02 00:33:44', 'UTC'), '%h');
SELECT date_format_mysql(toDateTime('2018-01-02 22:33:44', 'UTC'), '%i');
SELECT date_format_mysql(toDateTime('2018-01-02 22:33:44', 'UTC'), '%s');

SELECT '----week patterns----';
SELECT date_format_mysql(toDateTime('2018-01-01 22:33:44', 'UTC'), '%v'), date_format_mysql(toDateTime('2018-01-07 22:33:44', 'UTC'), '%v'),
       date_format_mysql(toDateTime('1996-01-01 22:33:44', 'UTC'), '%v'), date_format_mysql(toDateTime('1996-12-31 22:33:44', 'UTC'), '%v'),
       date_format_mysql(toDateTime('1999-01-01 22:33:44', 'UTC'), '%v'), date_format_mysql(toDateTime('1999-12-31 22:33:44', 'UTC'), '%v'),
       date_format_mysql(toDateTime('1970-01-01 22:33:44', 'UTC'), '%v'), date_format_mysql(toDateTime('1970-12-30 22:33:44', 'UTC'), '%v');
SELECT date_format_mysql(toDateTime('2018-01-01 22:33:44', 'UTC'), '%V'), date_format_mysql(toDateTime('2018-01-07 22:33:44', 'UTC'), '%V'),
       date_format_mysql(toDateTime('1996-01-01 22:33:44', 'UTC'), '%V'), date_format_mysql(toDateTime('1996-12-31 22:33:44', 'UTC'), '%V'),
       date_format_mysql(toDateTime('1999-01-01 22:33:44', 'UTC'), '%V'), date_format_mysql(toDateTime('1999-12-31 22:33:44', 'UTC'), '%V'),
       date_format_mysql(toDateTime('1970-01-01 22:33:44', 'UTC'), '%V'), date_format_mysql(toDateTime('1970-12-30 22:33:44', 'UTC'), '%V');
SELECT date_format_mysql(toDateTime('2018-01-01 22:33:44', 'UTC'), '%u'), date_format_mysql(toDateTime('2018-01-07 22:33:44', 'UTC'), '%u'),
       date_format_mysql(toDateTime('1996-01-01 22:33:44', 'UTC'), '%u'), date_format_mysql(toDateTime('1996-12-31 22:33:44', 'UTC'), '%u'),
       date_format_mysql(toDateTime('1999-01-01 22:33:44', 'UTC'), '%u'), date_format_mysql(toDateTime('1999-12-31 22:33:44', 'UTC'), '%u'),
       date_format_mysql(toDateTime('1970-01-01 22:33:44', 'UTC'), '%u'), date_format_mysql(toDateTime('1970-12-30 22:33:44', 'UTC'), '%u');
SELECT date_format_mysql(toDateTime('2018-01-01 22:33:44', 'UTC'), '%U'), date_format_mysql(toDateTime('2018-01-07 22:33:44', 'UTC'), '%U'),
       date_format_mysql(toDateTime('1996-01-01 22:33:44', 'UTC'), '%U'), date_format_mysql(toDateTime('1996-12-31 22:33:44', 'UTC'), '%U'),
       date_format_mysql(toDateTime('1999-01-01 22:33:44', 'UTC'), '%U'), date_format_mysql(toDateTime('1999-12-31 22:33:44', 'UTC'), '%U'),
       date_format_mysql(toDateTime('1970-01-01 22:33:44', 'UTC'), '%U'), date_format_mysql(toDateTime('1970-12-30 22:33:44', 'UTC'), '%U');
SELECT date_format_mysql(toDateTime('2018-01-01 22:33:44', 'UTC'), '%x'), date_format_mysql(toDateTime('2018-01-07 22:33:44', 'UTC'), '%x'),
       date_format_mysql(toDateTime('1996-01-01 22:33:44', 'UTC'), '%x'), date_format_mysql(toDateTime('1996-12-31 22:33:44', 'UTC'), '%x'),
       date_format_mysql(toDateTime('1999-01-01 22:33:44', 'UTC'), '%x'), date_format_mysql(toDateTime('1999-12-31 22:33:44', 'UTC'), '%x'),
       date_format_mysql(toDateTime('1970-01-01 22:33:44', 'UTC'), '%x'), date_format_mysql(toDateTime('1970-12-30 22:33:44', 'UTC'), '%x');
SELECT date_format_mysql(toDateTime('2018-01-01 22:33:44', 'UTC'), '%X'), date_format_mysql(toDateTime('2018-01-07 22:33:44', 'UTC'), '%X'),
       date_format_mysql(toDateTime('1996-01-01 22:33:44', 'UTC'), '%X'), date_format_mysql(toDateTime('1996-12-31 22:33:44', 'UTC'), '%X'),
       date_format_mysql(toDateTime('1999-01-01 22:33:44', 'UTC'), '%X'), date_format_mysql(toDateTime('1999-12-31 22:33:44', 'UTC'), '%X'),
       date_format_mysql(toDateTime('1970-01-01 22:33:44', 'UTC'), '%X'), date_format_mysql(toDateTime('1970-12-30 22:33:44', 'UTC'), '%X');

SELECT '----random patterns----';
SELECT date_format_mysql(toDateTime('1990-01-01 12:31:44', 'UTC'), '%r %%s %%%o %%%%q');

select date_format('2022-01-27 13:23:45', '%W %M %Y %r') as result;
