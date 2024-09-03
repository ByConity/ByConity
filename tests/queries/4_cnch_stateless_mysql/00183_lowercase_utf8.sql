
--
-- Bug#25830 SHOW TABLE STATUS behaves differently depending on table name
--
-- set names utf8;
set text_case_option = 'LOWERCASE';
use test;
create table `Ö` (id int);
show tables from test like 'Ö';
show tables from test like 'ö';
drop table `Ö`;

