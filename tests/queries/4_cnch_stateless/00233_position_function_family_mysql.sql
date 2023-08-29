SET dialect_type='MYSQL';
SET send_logs_level = 'fatal';
select 1 = position('', '');
select 1 = position('abc', '');
select 0 = position('', 'abc');
select 1 = position('abc', 'abc');
select 2 = position('abc', 'bc');
select 3 = position('abc', 'c');

select 1 = position('', '', 0);
select 1 = position('', '', 1);
select 0 = position('', '', 2);
select 1 = position('a', '', 1);
select 2 = position('a', '', 2);
select 0 = position('a', '', 3);