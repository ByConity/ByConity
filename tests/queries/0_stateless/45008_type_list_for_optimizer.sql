drop table if exists optimizer_supported;
drop table if exists optimizer_unsupported;
-- data types that are known to be supported by optimizer
create table optimizer_supported
(
    datetype String
) Engine=Memory;

-- data types that are known to be unsupported by optimizer
create table optimizer_unsupported
(
    datetype String
) Engine=Memory;

insert into optimizer_supported values ('Date')('Date32');
insert into optimizer_supported values ('DateTime')('DateTime32')('DateTime64')('DateTime64')('Time')('DateTimeWithoutTz');
insert into optimizer_supported values ('Bool')('UUID');
insert into optimizer_supported values ('Int8')('Int16')('Int32')('Int64')('Int128')('Int256');
insert into optimizer_supported values ('UInt8')('UInt16')('UInt32')('UInt64')('UInt128')('UInt256');
insert into optimizer_supported values ('String')('FixedString')
insert into optimizer_supported values ('LowCardinality')('Nullable');
insert into optimizer_supported values ('Decimal')('Decimal32')('Decimal64')('Decimal128')('Decimal256');
insert into optimizer_supported values ('Enum')('Enum8')('Enum16');
insert into optimizer_supported values ('IPv4')('IPv6');
insert into optimizer_supported values ('Float32')('Float64');

-- interval type is designed for calculation, thus not supported
insert into optimizer_unsupported select name from system.data_type_families where alias_to='' and name like 'Interval%';
-- agg type is not supported
insert into optimizer_unsupported values ('AggregateFunction')('SimpleAggregateFunction');
-- high order type is not supported
insert into optimizer_unsupported values ('Map')('Set')('Nested')('Nothing')('Array')('BitMap64')('Tuple');
-- graph type is not supported
insert into optimizer_unsupported values ('MultiPolygon')('Point')('Polygon')('Ring');

select '*** the following types are newly added, please contact optimizer team to determine if optimizer should support them';
select name from system.data_type_families
            where alias_to = ''
                and name not in optimizer_supported
                and name not in optimizer_unsupported
            order by name;
drop table if exists optimizer_supported;
drop table if exists optimizer_unsupported;
