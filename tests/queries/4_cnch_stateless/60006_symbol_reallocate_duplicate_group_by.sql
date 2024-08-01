drop table if exists 60006_t1;
create table 60006_t1 (p_date Date, app_id Int32, id Int32) engine = CnchMergeTree
partition by (p_date, app_id) order by id;
select t1.app_id, t2.app_id from 60006_t1 t1, 60006_t1 t2 where t1.app_id = t2.app_id group by t1.app_id, t2.app_id;
