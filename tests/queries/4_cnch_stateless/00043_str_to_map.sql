select str_to_map('ABC_ABC, AC_AC', ',', '_');
drop table if exists test_str_map;
create table test_str_map (a String, b String, c String, d String) ENGINE = CnchMergeTree ORDER BY a;
insert into test_str_map values('a', 'b', 'c', 'd');
insert into test_str_map values('a.b', 'b.c', 'c.d', 'd.f');

select str_to_map(concat(a, '$', b, '-', c, '$', d), '-', '$') from test_str_map order by a;
drop table test_str_map;

select str_to_map('', 'a', 'b');
select str_to_map('ab', 'b', 'a');

select str_to_map('a=2&b=3', '&', '=');
select str_to_map('a=&b=', '&', '=');
