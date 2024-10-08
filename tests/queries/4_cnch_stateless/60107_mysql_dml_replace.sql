drop table if exists 60107_mysql_dml_replace_non_unique_table;
drop table if exists 60107_mysql_dml_replace_unique_table;

select '60107_mysql_dml_replace_non_unique_table';
create table 60107_mysql_dml_replace_non_unique_table(x Int32, y Int32) ENGINE=CnchMergeTree ORDER BY tuple();
replace into 60107_mysql_dml_replace_non_unique_table values (1, 1); -- { serverError 48 }
select * from 60107_mysql_dml_replace_non_unique_table;
insert into 60107_mysql_dml_replace_non_unique_table values (1, 1);
select * from 60107_mysql_dml_replace_non_unique_table;
replace into 60107_mysql_dml_replace_non_unique_table values (1, 2); -- { serverError 48 }
select * from 60107_mysql_dml_replace_non_unique_table;

select '60107_mysql_dml_replace_unique_table';
create table 60107_mysql_dml_replace_unique_table(x Int32, y Int32) ENGINE=CnchMergeTree ORDER BY tuple() UNIQUE KEY x;
replace into 60107_mysql_dml_replace_unique_table values (1, 1);
select * from 60107_mysql_dml_replace_unique_table;
insert into 60107_mysql_dml_replace_unique_table values (1, 1);
select * from 60107_mysql_dml_replace_unique_table;
replace into 60107_mysql_dml_replace_unique_table values (1, 2);
select * from 60107_mysql_dml_replace_unique_table;

drop table 60107_mysql_dml_replace_non_unique_table;
drop table 60107_mysql_dml_replace_unique_table;
