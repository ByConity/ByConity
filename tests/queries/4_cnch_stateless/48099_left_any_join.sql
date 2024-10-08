set enable_optimizer=1;
create table x(a Int32, b Int32) Engine=CnchMergeTree() order by a;
create table y(c Int32, d Int32) Engine=CnchMergeTree() order by c;

explain verbose=0, stats=0
select * from x global inner join y on x.a = y.c where d != 0; 
explain verbose=0, stats=0
select * from x global left join y on x.a = y.c where d != 0; 

explain verbose=0, stats=0
select * from x global any inner join y on x.a = y.c where d != 0; 
explain verbose=0, stats=0
select * from x global any left join y on x.a = y.c where d != 0; 
