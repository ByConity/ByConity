use test;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id UInt64, m Map(String, UInt64)) ENGINE = CnchMergeTree ORDER BY (id);
INSERT INTO tab VALUES (1, {'key1':1, 'key2':10}), (2, {'key1':2,'key2':20});

set allow_map_access_without_key=1;
select * from (select m from tab) format Null;
select * from (select m{'key1'} from tab) format Null;
select * from (select m['key1'] from tab) format Null;

set allow_map_access_without_key=0;
select * from (select m from tab) format Null; -- { serverError 48 }
select * from (select m['key1'] from tab) format Null; -- { serverError 48 }
select * from (select m{'key1'} from tab) format Null;
