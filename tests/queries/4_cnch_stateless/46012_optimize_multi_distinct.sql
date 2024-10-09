DROP TABLE IF EXISTS multi_dist;

CREATE TABLE multi_dist (a UInt64, b UInt64, c UInt64) ENGINE = CnchMergeTree() PARTITION BY a ORDER BY a UNIQUE KEY a;
insert into multi_dist values(1,2,3)(1,3,4)(2,3,4)(2,4,5)(3,4,5);

set enable_mark_distinct_optimzation=1;
select a,sum(distinct b),sum(distinct c),count() from multi_dist group by a order by a;
SELECT count(distinct(a)), sum(b), c FROM multi_dist GROUP BY c order by count(distinct(a));

set enable_optimizer=1;

explain select a,sum(distinct b),sum(distinct c),count() from multi_dist group by a;
explain SELECT count(distinct(a)), sum(b), c FROM multi_dist GROUP BY c;


DROP TABLE IF EXISTS multi_dist;

DROP database if exists aeolus_data_db_cnch_gamma_yg_202305;
create database aeolus_data_db_cnch_gamma_yg_202305;
use aeolus_data_db_cnch_gamma_yg_202305;
    
CREATE TABLE aeolus_data_table_35_1537237_prod
(
    `uid` String,
    `is_bind_final` Nullable(String),
    `customer_id_bk` String
)
    ENGINE = CnchMergeTree() 
ORDER BY uid SETTINGS index_granularity = 8192;

insert into aeolus_data_table_35_1537237_prod values('abc',null,'cba')('cbd',null,'acb')('qwe','0','asd')('fds','0','qws')('zxc','1','sad');

set enable_optimizer=1;
set enable_mark_distinct_optimzation=1;
    
SELECT
    is_bind_final,
    count(),
    countDistinct(uid),
    countDistinct(customer_id_bk) / 1000,
    countDistinct(customer_id_bk) / countDistinct(uid)
FROM aeolus_data_db_cnch_gamma_yg_202305.aeolus_data_table_35_1537237_prod
GROUP BY is_bind_final order by is_bind_final;

explain
SELECT
    distinct (uid, customer_id_bk)
FROM aeolus_data_db_cnch_gamma_yg_202305.aeolus_data_table_35_1537237_prod;

DROP TABLE IF EXISTS aeolus_data_db_cnch_gamma_yg_202305.aeolus_data_table_35_1537237_prod;
