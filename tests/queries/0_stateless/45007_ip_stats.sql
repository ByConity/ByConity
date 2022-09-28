set enable_optimizer=1;
set dialect_type='ANSI';
set create_stats_time_output=0;
drop table if exists test_ip_opt;
drop table if exists test_ip_opt_local;
create table test_ip_opt_local(
    id UInt64,
    ipv4 IPv4,
    ipv6 IPv6,
    fxstr FixedString(3),
    b Bool
) Engine = MergeTree() order by id;

create table test_ip_opt as test_ip_opt_local Engine=Distributed(test_shard_localhost, currentDatabase(), test_ip_opt_local, rand());

insert into test_ip_opt values (0, '192.168.1.1', '1111:2222:3333:4444:5555:6666:7777:8888', '123', 'T');
insert into test_ip_opt values (1, '192.168.1.1', '1111:2222:3333:4444:5555:6666:7777:8888', '123', 'T');
insert into test_ip_opt values (2, '192.168.1.2', '2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D', '456', 'F');

select * from test_ip_opt order by id;
select '*** show stats all';
create stats test_ip_opt;
select '*** test id';
explain select * from test_ip_opt where id > 1;
explain select * from test_ip_opt where id >= 1;
select '*** test ipv4';
explain select * from test_ip_opt where ipv4 == toIPv4('192.168.1.1');
explain select * from test_ip_opt where ipv4 == toIPv4('192.168.1.2');
select '*** test ipv6';
explain select * from test_ip_opt where ipv6 == toIPv6('1111:2222:3333:4444:5555:6666:7777:8888');
explain select * from test_ip_opt where ipv6 == toIPv6('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D');
select '*** test fixed string';
explain select * from test_ip_opt where fxstr == '123';
explain select * from test_ip_opt where fxstr == '456';
select '*** test bool';
explain select * from test_ip_opt where b == toBool('T');
explain select * from test_ip_opt where b == toBool('F');

drop stats test_ip_opt;
drop table if exists test_ip_opt;
drop table if exists test_ip_opt_local;
