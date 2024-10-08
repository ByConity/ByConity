drop table if exists t40029;

create table t40029 (uid String, platform Int64 )
engine = CnchMergeTree()
partition by substring (uid, 1, 1) order by uid unique key uid
settings partition_level_unique_keys = 1, index_granularity = 8192;

-- system stop merges t40029;

select sleep(3);

insert into t40029 select  cast(number, 'String'), number from system.numbers limit 100;
insert into t40029 select  cast(number, 'String'), number + 20 from system.numbers limit 50;

select count(1) from t40029 prewhere uid != '' where platform >=0 and uid !='' settings enable_optimizer = 1;
select count(1) from t40029 prewhere uid != '' where platform >=0 and uid !='' settings enable_optimizer = 0;
