set database_atomic_wait_for_drop_and_detach_synchronously = 1;

drop table if exists test_routing_ha_4567_view_local;
drop table if exists test_routing_ha_4567_view_source_local;

create table test_routing_ha_4567_view_source_local (p_date Date, id Int32) engine = CnchMergeTree partition by p_date order by id;

create view test_routing_ha_4567_view_local as select * from test_routing_ha_4567_view_source_local;

insert into test_routing_ha_4567_view_source_local select '2023-01-01', number from numbers(10);

select count() from test_routing_ha_4567_view_local settings prefer_localhost_replica = 0;
select count() from test_routing_ha_4567_view_local settings prefer_localhost_replica = 1;

drop table if exists test_routing_ha_4567_view_local;
drop table if exists test_routing_ha_4567_view_source_local;