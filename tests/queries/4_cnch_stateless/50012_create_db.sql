drop database if exists db_comment;
create database db_comment comment 'the temporary database';
show create database db_comment;
drop database if exists db_comment;

drop database if exists db_engine;
create database db_engine engine=Memory comment 'the temporary database';
show create database db_engine;
drop database if exists db_engine;

drop database if exists db_nothing;
create database db_nothing;
show create database db_nothing;
CREATE TABLE db_nothing.check_query_comment_column
(
first_column UInt8 COMMENT 'comment 1',
second_column UInt8 COMMENT 'comment 2',
third_column UInt8 COMMENT 'comment 3'
) ENGINE = CnchMergeTree()
ORDER BY first_column;
show create table db_nothing.check_query_comment_column;
drop database if exists db_nothing;
