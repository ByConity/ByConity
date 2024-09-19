drop table if exists plp;
create table if not exists plp (id Int64, name String) engine=CnchMergeTree() order by id cluster by id into 3 buckets;
insert into plp select number, 's' || toString(number) from system.numbers limit 10;

set enable_point_lookup_profile = 1; -- NOTE: use_sync_pipeline_executor=1 doesn't support insert select
select 'default profile',   * from plp where id = 1 settings point_lookup_profile = '';
select 'non-exist profile', * from plp where id = 2 settings point_lookup_profile = 'fake_profile';
select 'exist profile',     * from plp where id = 3 settings point_lookup_profile = 'point_lookup';
drop table if exists plp;
