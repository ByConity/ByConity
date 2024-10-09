drop table if exists tuple_test;

set enable_optimizer = 1;

select tuple(1,'2','2022-10-12',(1,2)) as x, untuple(x);
select argMax(untuple(x)) from (select (number, number + 1) as x from numbers(10));
select argMax(untuple(x)), min(x) from (select (number, number + 1) as x from numbers(10)) having tuple(untuple(min(x))).1 != 42;
SELECT (0.5, '92233720368547758.07', NULL), '', '1.00', untuple(('256', NULL)), NULL FROM (SELECT untuple(((NULL, untuple((('0.0000000100', (65536, NULL, (65535, 9223372036854775807), '25.7', (0.00009999999747378752, '10.25', 1048577), 65536)), '0.0000001024', '65537', NULL))), untuple((9223372036854775807, -inf, 0.5)), NULL, -9223372036854775808)), 257, 7, ('0.0001048575', (1024, NULL, (7, 3), (untuple(tuple(-NULL)), NULL, '0.0001048577', NULL), 0)), 0, (0, 0.9998999834060669, '65537'), untuple(tuple('10.25')));
SELECT NULL FROM (SELECT untuple((NULL, dummy)));

create table tuple_test(a Int32,b Tuple(Int32,String)) ENGINE = CnchMergeTree() order by a;
insert into tuple_test values(1,(2,'3')),(2,(4,'6')),(4,(8,'12'));

select a, b, tuple(a, b) as x, untuple(b), tuple(x) from tuple_test;
select untuple((a,untuple(b))) from tuple_test;
select argMax(untuple(b)) from tuple_test;
select a, rank() over(partition by untuple(b) order by a) from tuple_test;

drop table if exists tuple_test;
