set enable_optimizer=1;
set dialect_type='ANSI';
set data_type_default_nullable=0;
set create_stats_time_output=0;
drop table if exists test_ip_opt;
create table test_ip_opt(
    id UInt64,
    ipv4 IPv4,
    ipv6 IPv6,
    fxstr FixedString(3),
    b Bool
) Engine = CnchMergeTree() order by id;

insert into test_ip_opt values (0, '192.168.1.1', '0000:0000:0000:0000:0000:0000:0000:0000', '123', 'T');
insert into test_ip_opt values (1, '192.168.1.1', '0000:0000:0000:0000:0000:0000:0000:0000', '123', 'T');
insert into test_ip_opt values (2, '192.168.1.2', '2222:2222:2222:2222:2222:2222:2222:2222', '456', 'F');

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
explain select * from test_ip_opt where ipv6 == toIPv6('0000:0000:0000:0000:0000:0000:0000:0000');
explain select * from test_ip_opt where ipv6 == toIPv6('2222:2222:2222:2222:2222:2222:2222:2222');
select '*** test fixed string';
explain select * from test_ip_opt where fxstr == '123';
explain select * from test_ip_opt where fxstr == '456';
select '*** test bool';
explain select * from test_ip_opt where b == toBool('T');
explain select * from test_ip_opt where b == toBool('F');

drop stats test_ip_opt;
drop table if exists test_ip_opt;

set enable_optimizer=1;
set dialect_type='MYSQL';
set create_stats_time_output=0;

drop table if exists test_ip_opt;
create table test_ip_opt(
    id UInt64,
    ipv4 IPv4,
    ipv6 IPv6,
    fxstr FixedString(3),
    b Bool
) Engine = CnchMergeTree() order by id;



insert into test_ip_opt values (0, '192.168.1.1', '0000:0000:0000:0000:0000:0000:0000:0000', '123', 'T');
insert into test_ip_opt values (1, '192.168.1.1', '0000:0000:0000:0000:0000:0000:0000:0000', '123', 'T');
insert into test_ip_opt values (2, '192.168.1.2', '2222:2222:2222:2222:2222:2222:2222:2222', '456', 'F');

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
explain select * from test_ip_opt where ipv6 == toIPv6('0000:0000:0000:0000:0000:0000:0000:0000');
explain select * from test_ip_opt where ipv6 == toIPv6('2222:2222:2222:2222:2222:2222:2222:2222');
select '*** test fixed string';
explain select * from test_ip_opt where fxstr == '123';
explain select * from test_ip_opt where fxstr == '456';
select '*** test bool';
explain select * from test_ip_opt where b == toBool('T');
explain select * from test_ip_opt where b == toBool('F');

drop stats test_ip_opt;
drop table if exists test_ip_opt;
