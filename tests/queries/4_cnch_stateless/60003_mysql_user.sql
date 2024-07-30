create database if not exists userpriv;
use userpriv;
create table xx(x int, y int) engine=CnchMergeTree() order by x;
insert into xx values (0, 1), (1, 2), (2, 3);

create database if not exists dbpriv;
use dbpriv;
create table yy(x int, y int) engine=CnchMergeTree() order by x;
insert into yy values (0, 1), (1, 2), (2, 3);

DROP USER IF EXISTS 'hello';
CREATE USER 'hello' IDENTIFIED WITH plaintext_password BY 'password';

-- global level
grant create on *.* to 'hello';

-- db level
grant alter table on userpriv.* to 'hello';
grant show tables on dbpriv.* to 'hello';

create role 'r1';
create role 'r2';

grant select on *.* to 'r1';
grant 'r1' to 'r2';
grant 'r2' to 'hello';

select * from system.cnch_user_priv where user = 'hello';
select * from mysql.user where user = 'hello';

select * from system.cnch_db_priv where user = 'hello';
select * from mysql.db where user = 'hello';

-- mysql IDE SQLs
SELECT CONCAT(Db,'.',Table_name) as scope,user,host FROM mysql.tables_priv;
SELECT CONCAT(Db,'.',Table_name,'.',Column_name) as scope,user,host FROM mysql.columns_priv;
SELECT Db,Host,User,Select_priv,Insert_priv,Update_priv,Delete_priv,Create_priv,Drop_priv,Grant_priv,References_priv,Index_priv,Alter_priv,Create_tmp_table_priv,Lock_tables_priv,Create_view_priv,Create_routine_priv,Alter_routine_priv,Execute_priv,Event_priv,Trigger_priv FROM mysql.db where user = 'hello';
SELECT Db as scope,user,host FROM mysql.db where user = 'hello';
SELECT '<global>' as Db,Host,User,Select_priv,Insert_priv,Update_priv,Delete_priv,Create_priv,Drop_priv,Grant_priv,References_priv,Index_priv,Alter_priv,Create_tmp_table_priv,Lock_tables_priv,Create_view_priv,Create_routine_priv,Alter_routine_priv,Execute_priv,Event_priv,Trigger_priv FROM mysql.user where user = 'hello';
SELECT '<global>' as scope,user,host FROM mysql.user where user = 'hello';
SELECT user, host, ssl_type, ssl_cipher, x509_issuer, x509_subject, max_questions, max_updates, max_connections, super_priv, max_user_connections FROM mysql.user where user = 'hello';

drop role 'r1';
drop role 'r2';
drop user hello;
drop table userpriv.xx;
drop database userpriv;
drop table dbpriv.yy;
drop database dbpriv;
