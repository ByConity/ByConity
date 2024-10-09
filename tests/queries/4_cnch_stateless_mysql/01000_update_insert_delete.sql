set insert_select_with_profiles=1;
drop table if exists tt1;
create table tt1(x int, y text, primary key (x));
insert into tt1 values (1, 'aa'), (2, 'bb'), (3, 'cc');

drop table if exists tt2;
create table tt2 like tt1;
insert into tt2 select * from tt1;

update tt2 set y = 'xxx' where x = 1;
delete from tt2 where x = 2;

drop table tt1;
drop table tt2;
