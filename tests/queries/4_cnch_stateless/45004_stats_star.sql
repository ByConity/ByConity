set create_stats_time_output=0;
drop table if exists 45004_stats_star.test;
drop table if exists 45004_stats_star.test2;
drop table if exists 45004_stats_star_fake.fake;
drop database if exists 45004_stats_star;
drop database if exists 45004_stats_star_fake;

create database 45004_stats_star;
create database 45004_stats_star_fake;
create table 45004_stats_star.test(x UInt32) ENGINE=CnchMergeTree() order by x;
create table 45004_stats_star.test2(x UInt32) ENGINE=CnchMergeTree() order by x;
create table 45004_stats_star_fake.fake(x UInt32) ENGINE=CnchMergeTree() order by x;





insert into 45004_stats_star.test values(1)(2);
insert into 45004_stats_star.test2 values(10)(20);

use 45004_stats_star_fake;
select '--- create stats real';
create stats 45004_stats_star.*;
select '--- show stats real';
show stats 45004_stats_star.*;
show column_stats 45004_stats_star.*;
select '--- show stats fake';
show stats all;
show stats 45004_stats_star_fake.*;
show column_stats all;
show column_stats 45004_stats_star_fake.*;
select '--- drop stats fake';
drop stats all;
drop stats 45004_stats_star_fake.*;
select '--- show stats real';
show stats 45004_stats_star.*;
show column_stats 45004_stats_star.*;
select '--- drop stats real';
drop stats 45004_stats_star.*;
select '--- show stats real, empty';
show stats 45004_stats_star.*;
show column_stats 45004_stats_star.*;
select '--- create stats fake';
create stats all; 
create stats 45004_stats_star_fake.*; 
select '--- show stats real, empty';
show stats 45004_stats_star.*;
show column_stats 45004_stats_star.*;

drop stats 45004_stats_star.*;
drop stats 45004_stats_star_fake.*;
drop table 45004_stats_star.test;
drop table 45004_stats_star.test2;
drop table 45004_stats_star_fake.fake;
drop database 45004_stats_star;
drop database 45004_stats_star_fake;
