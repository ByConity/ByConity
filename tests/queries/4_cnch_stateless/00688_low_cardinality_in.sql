set allow_suspicious_low_cardinality_types = 1;
set mutations_sync = 1;
drop table if exists lc_00688;
create table lc_00688 (str StringWithDictionary, val UInt8WithDictionary) engine = CnchMergeTree order by tuple();
insert into lc_00688 values ('a', 1), ('b', 2);
select str, str in ('a', 'd') from lc_00688;
select val, val in (1, 3) from lc_00688;
select str, str in (select arrayJoin(['a', 'd'])) from lc_00688;
select val, val in (select arrayJoin([1, 3])) from lc_00688;
select str, str in (select str from lc_00688) from lc_00688;
select val, val in (select val from lc_00688) from lc_00688;
drop table if exists lc_00688;

drop table if exists ary_lc_null;
CREATE TABLE ary_lc_null (i int, v Array(LowCardinality(Nullable(String)))) ENGINE = CnchMergeTree() ORDER BY i ;
INSERT INTO ary_lc_null VALUES (1, ['1']);
SELECT v FROM ary_lc_null WHERE v IN (SELECT v FROM ary_lc_null);
drop table if exists ary_lc_null;

drop table if exists full_lc;
CREATE TABLE full_lc(c1 int, c2 LowCardinality(String)) ENGINE = CnchMergeTree() ORDER BY c1 SETTINGS enable_ingest_wide_part = 1;
system start merges full_lc;
system stop merges full_lc;
INSERT INTO full_lc(c1) values(1);
INSERT INTO full_lc select number, number from system.numbers limit 100001;
OPTIMIZE TABLE full_lc;
SELECT count(*) FROM full_lc WHERE isNull(c2) OR c2 GLOBAL NOT IN ('a');
drop table if exists full_lc;

drop table if exists full_lc_null;
CREATE TABLE full_lc_null(c1 int, c2 LowCardinality(Nullable(String))) ENGINE = CnchMergeTree() ORDER BY c1 SETTINGS enable_ingest_wide_part = 1;
system start merges full_lc_null;
system stop merges full_lc_null;
INSERT INTO full_lc_null(c1) values(1);
INSERT INTO full_lc_null select number, number from system.numbers limit 100001;
OPTIMIZE TABLE full_lc_null;
SELECT count(*) FROM full_lc_null WHERE isNull(c2) OR c2 GLOBAL NOT IN ('a');
drop table if exists full_lc_null;
