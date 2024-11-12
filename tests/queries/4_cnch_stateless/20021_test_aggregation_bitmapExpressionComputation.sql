drop table if exists test_bitmap64_expression_calc_20021;

create table if not exists test_bitmap64_expression_calc_20021 (p_date Date, tag_id Int64, uids BitMap64, shard_id Int64) engine = CnchMergeTree partition by (p_date, shard_id) order by tag_id settings index_granularity = 128;

insert into table test_bitmap64_expression_calc_20021 values ('2019-01-01', 1, [1], 1);
insert into table test_bitmap64_expression_calc_20021 values ('2019-01-01', 2, [1, 2], 1);
insert into table test_bitmap64_expression_calc_20021 values ('2019-01-01', 3, [1, 2, 3], 2);
insert into table test_bitmap64_expression_calc_20021 values ('2019-01-01', 4, [1, 2, 3, 4], 2);
insert into table test_bitmap64_expression_calc_20021 values ('2019-01-01', 5, [1], 3);
insert into table test_bitmap64_expression_calc_20021 values ('2019-01-01', 6, [2], 3);
insert into table test_bitmap64_expression_calc_20021 values ('2019-01-01', 7, [3], 4);
insert into table test_bitmap64_expression_calc_20021 values ('2019-01-01', 8, [4], 4);
insert into table test_bitmap64_expression_calc_20021 values ('2019-01-01', 9, [1, 2, 3, 4], 5);
insert into table test_bitmap64_expression_calc_20021 values ('2019-01-01', 10, [1, 2, 3, 4, 5], 5);

select bitmapCount('1 | 2')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapCount('1 & 2')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapCount('1 | 2 | 3')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapCount('1 | 2 & 3')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapCount('1 | (2 & 3)')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapCount('1 | (2 & 3) | 4')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapCount('1 | 2 | 3 | 4 & 5')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapCount('1 & ( 2 | 3 | 4 ) & 5')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapCount('1 | 2 | 3 | 4  | 5 & 6')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapCount('1 & ( 2 | 3 | 4  | 5) & 6')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapCount('1 & ( 2 | 3) & (4 | 5) & (6 | 7)')(tag_id, uids) from test_bitmap64_expression_calc_20021;

select bitmapCount('1')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapCount('2 ~ 1')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapCount('1 | 2 | 3 ~ 2')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapCount('1 | 2 | ( 3 ~ 2 )')(tag_id, uids) from test_bitmap64_expression_calc_20021;

select bitmapExtract('1 | 2')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapExtract('1 & 2')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapExtract('1 | 2 | 3')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapExtract('1 | 2 & 3')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapExtract('1 | (2 & 3)')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapExtract('1 | (2 & 3) | 4')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapExtract('1 | 2 | 3 | 4 & 5')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapExtract('1 & ( 2 | 3 | 4 ) & 5')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapExtract('1 | 2 | 3 | 4  | 5 & 6')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapExtract('1 & ( 2 | 3 | 4  | 5) & 6')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapExtract('1 & ( 2 | 3) & (4 | 5) & (6 | 7)')(tag_id, uids) from test_bitmap64_expression_calc_20021;

select bitmapExtract('1')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapExtract('2 ~ 1')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapExtract('1 | 2 | 3 ~ 2')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapExtract('1 | 2 | ( 3 ~ 2 )')(tag_id, uids) from test_bitmap64_expression_calc_20021;

select bitmapMultiCount('1', '_1|2', '_2&3')(tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapMultiCount('2', '3', '2&3', '2|3', '_3|_4')(tag_id, uids) from test_bitmap64_expression_calc_20021;

-- for wrong type of parameters
select bitmapCount(1)(tag_id, uids) from test_bitmap64_expression_calc_20021;  -- { serverError 169 }
select bitmapExtract(1)(tag_id, uids) from test_bitmap64_expression_calc_20021;  -- { serverError 169 }

select bitmapMultiExtract('1|2',3,'2')(tag_id, uids) from test_bitmap64_expression_calc_20021;  -- { serverError 169 }

-- for invaild paremeters when the type is integer
-- for bitmapExtract
select bitmapExtract('1 - 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapExtract('1 ～ 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapExtract('1 ｜ 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapExtract('1 ， 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapExtract('1 * 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
-- for bitmapMultiExtract
select bitmapMultiExtract('1 ~ 2', '1 - 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapMultiExtract('1 ~ 2', '1 ～ 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapMultiExtract('1 ~ 2', '1 ｜ 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapMultiExtract('1 ~ 2', '1 ， 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapMultiExtract('1 ~ 2', '1 * 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
-- for bitmapMultiExtractWithDate
select bitmapMultiExtractWithDate('20190101_1 ～ 20190101_2','20190101_c')(cast(toYYYYMMDD(p_date) as Int64), idx, rbm) from ( select toDate('20190101') AS p_date, toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toDate('20190101') AS p_date, toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapMultiExtractWithDate('20190101_1 ～ 20190101_2','20190101_c')(cast(toYYYYMMDD(p_date) as Int64), idx, rbm) from ( select toDate('20190101') AS p_date, toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toDate('20190101') AS p_date, toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapMultiExtractWithDate('20190101_1 ｜ 20190101_2','20190101_c')(cast(toYYYYMMDD(p_date) as Int64), idx, rbm) from ( select toDate('20190101') AS p_date, toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toDate('20190101') AS p_date, toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapMultiExtractWithDate('20190101_1 ， 20190101_2','20190101_c')(cast(toYYYYMMDD(p_date) as Int64), idx, rbm) from ( select toDate('20190101') AS p_date, toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toDate('20190101') AS p_date, toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
-- for bitmapCount
select bitmapCount('1 - 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapCount('1 ～ 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapCount('1 ｜ 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapCount('1 ， 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapCount('1 * 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
-- for bitmapMultiCount
select bitmapMultiCount('1 ~ 2', '1 - 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapMultiCount('1 ~ 2', '1 ～ 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapMultiCount('1 ~ 2', '1 ｜ 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapMultiCount('1 ~ 2', '1 ， 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapMultiCount('1 ~ 2', '1 * 2')(idx, rbm) from ( select toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
-- for bitmapMultiCountWithDate
select bitmapMultiCountWithDate('20190101_1 - 20190101_2','20190101_c')(cast(toYYYYMMDD(p_date) as Int64), idx, rbm) from ( select toDate('20190101') AS p_date, toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toDate('20190101') AS p_date, toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapMultiCountWithDate('20190101_1 ～ 20190101_2','20190101_c')(cast(toYYYYMMDD(p_date) as Int64), idx, rbm) from ( select toDate('20190101') AS p_date, toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toDate('20190101') AS p_date, toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapMultiCountWithDate('20190101_1 ｜ 20190101_2','20190101_c')(cast(toYYYYMMDD(p_date) as Int64), idx, rbm) from ( select toDate('20190101') AS p_date, toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toDate('20190101') AS p_date, toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }
select bitmapMultiCountWithDate('20190101_1 ， 20190101_2','20190101_c')(cast(toYYYYMMDD(p_date) as Int64), idx, rbm) from ( select toDate('20190101') AS p_date, toInt32(1) AS idx, arrayToBitmap([1, 2, 3, 4, 5]) AS rbm UNION ALL SELECT toDate('20190101') AS p_date, toInt32(2) AS idx, arrayToBitmap([1, 2, 3]) AS rbm );  -- { serverError 36 }

select bitmapMultiCountWithDate(20190101_1,'20190101_2','20190101_3')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap64_expression_calc_20021; -- { serverError 47 }
select bitmapMultiExtractWithDate('20190101_1|20190101_2','20190101_3',47)(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap64_expression_calc_20021; -- { serverError 169 }
select bitmapMultiExtractWithDate('20190101_1|20190101_2','20190101_c')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap64_expression_calc_20021;
select bitmapMultiExtractWithDate('20190101_1|20190101_2','20190101_c','47')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap64_expression_calc_20021;


drop table if exists test_bitmap64_expression_calc_20021;