drop table if exists test_bitmap_parse;

create table if not exists test_bitmap_parse (tag String, uids BitMap64, p_date Date) engine = CnchMergeTree() order by tag;

---- format Value
insert into test_bitmap_parse values ('v0', [], '2021-01-01');
insert into test_bitmap_parse values ('v1', [,], '2021-01-01');
insert into test_bitmap_parse values ('v2', [,,], '2021-01-01');
insert into test_bitmap_parse values ('v3', [0,,], '2021-01-01');
insert into test_bitmap_parse values ('v4', [,0,], '2021-01-01');
insert into test_bitmap_parse values ('v5', [,,0], '2021-01-01');
insert into test_bitmap_parse values ('v6', [, 0 ,0 ], '2021-01-01');
insert into test_bitmap_parse values ('v7', [0,1,2], '2021-01-01');
insert into test_bitmap_parse values ('v8', '[0,1,2]', '2021-01-01');
insert into test_bitmap_parse values ('v9', "[0,1,2]", '2021-01-01');
insert into test_bitmap_parse values ('v10', {1,2,3}, '2021-01-01');
insert into test_bitmap_parse values ('v11', '{1,2,3}', '2021-01-01');
insert into test_bitmap_parse values ('v12', "{1,2,3}", '2021-01-01');

insert into test_bitmap_parse values ('v13', <1,2,3>, '2021-01-01'); -- { clientError 62 }
insert into test_bitmap_parse values ('v13', [1,2,3a], '2021-01-01'); -- { clientError 49 }
insert into test_bitmap_parse values ('v13', '[1,2,3}', '2021-01-01'); -- { clientError 27 }
insert into test_bitmap_parse values ('v13', "{1,2,3]", '2021-01-01'); -- { clientError 47 }

select * from test_bitmap_parse order by tag;

---- format CSV
truncate table test_bitmap_parse;
insert into test_bitmap_parse format CSV 'c0', [], '2021-01-01'
insert into test_bitmap_parse format CSV 'c1', [,], '2021-01-01'
insert into test_bitmap_parse format CSV 'c2', [,,], '2021-01-01'
insert into test_bitmap_parse format CSV 'c3', [0,,], '2021-01-01'
insert into test_bitmap_parse format CSV 'c4', [,0,], '2021-01-01'
insert into test_bitmap_parse format CSV 'c5', [,,0], '2021-01-01'
insert into test_bitmap_parse format CSV 'c6', [, 0 ,0 ], '2021-01-01'
insert into test_bitmap_parse format CSV 'c7', [0,1,2], '2021-01-01'
insert into test_bitmap_parse format CSV 'c8', '[0,1,2]', '2021-01-01'
insert into test_bitmap_parse format CSV 'c9', "[0,1,2]", '2021-01-01'
insert into test_bitmap_parse format CSV 'c10', {1,2,3}, '2021-01-01'
insert into test_bitmap_parse format CSV 'c11', '{1,2,3}', '2021-01-01'
insert into test_bitmap_parse format CSV 'c12', "{1,2,3}", '2021-01-01'

-- insert into test_bitmap_parse format CSV 'c13', <1,2,3>, '2021-01-01'  -- { clientError 27 }
-- insert into test_bitmap_parse format CSV 'c13', [1,2,3a], '2021-01-01'  -- { clientError 49 }
-- insert into test_bitmap_parse format CSV 'c13', '[1,2,3}', '2021-01-01'  -- { clientError 27 }
-- insert into test_bitmap_parse format CSV 'c13', "{1,2,3]", '2021-01-01' -- { clientError 27 }

select * from test_bitmap_parse order by tag;

---- format JSONEachRow
truncate table test_bitmap_parse;
insert into test_bitmap_parse format JSONEachRow {"tag":"j0","uids":[],"p_date":"2021-01-01"};
insert into test_bitmap_parse format JSONEachRow {"tag":"j1","uids":[,],"p_date":"2021-01-01"};
insert into test_bitmap_parse format JSONEachRow {"tag":"j2","uids":[,,],"p_date":"2021-01-01"};
insert into test_bitmap_parse format JSONEachRow {"tag":"j3","uids":[0,,],"p_date":"2021-01-01"};
insert into test_bitmap_parse format JSONEachRow {"tag":"j4","uids":[,0,],"p_date":"2021-01-01"};
insert into test_bitmap_parse format JSONEachRow {"tag":"j5","uids":[,,0],"p_date":"2021-01-01"};
insert into test_bitmap_parse format JSONEachRow {"tag":"j6","uids":[, 0 ,0 ],"p_date":"2021-01-01"};
insert into test_bitmap_parse format JSONEachRow {"tag":"j7","uids":[0,1,2],"p_date":"2021-01-01"};
insert into test_bitmap_parse format JSONEachRow {"tag":"j8","uids":'[0,1,2]',"p_date":"2021-01-01"};
insert into test_bitmap_parse format JSONEachRow {"tag":"j9","uids":"[0,1,2]","p_date":"2021-01-01"};
insert into test_bitmap_parse format JSONEachRow {"tag":"j10", "uids":{1,2,3}, "p_date":"2021-01-01"};
insert into test_bitmap_parse format JSONEachRow {"tag":"j11", "uids":'{1,2,3}', "p_date":"2021-01-01"};
insert into test_bitmap_parse format JSONEachRow {"tag":"j12", "uids":"{1,2,3}", "p_date":"2021-01-01"};

-- insert into test_bitmap_parse format JSONEachRow {"tag":"j13", "uids":<1,2,3>, "p_date":"2021-01-01"};  -- { clientError 27 }
-- insert into test_bitmap_parse format JSONEachRow {"tag":"j13", "uids":[1,2,3a], "p_date":"2021-01-01"};  -- { clientError 49 }
-- insert into test_bitmap_parse format JSONEachRow {"tag":"j13", "uids":'[1,2,3}', "p_date":"2021-01-01"};  -- { clientError 27 }
-- insert into test_bitmap_parse format JSONEachRow {"tag":"j13", "uids":"{1,2,3]", "p_date":"2021-01-01"}; -- { clientError 27 }

select * from test_bitmap_parse order by tag;

drop table if exists test_bitmap_parse;

drop table if exists parse_bitmap;
create table parse_bitmap (tag Int32, uids BitMap64) engine=CnchMergeTree() ORDER BY tag;
insert into parse_bitmap values (1, []), (2, [0]), (3, [-]), (4, [-0]), (5, [-1]);
select * from parse_bitmap order by tag;

drop table parse_bitmap;
