drop table if exists test_bitmap64_expression_v2_20021;

create table test_bitmap64_expression_v2_20021 (p_date Date, tag_id String, uid BitMap64) engine = CnchMergeTree partition by p_date order by tag_id settings index_granularity = 128;
insert into test_bitmap64_expression_v2_20021 values ('2022-01-01', 'a', [1,2,3]), ('2022-01-02', 'b', [1,2]), ('2022-01-03', 'c', [1]);
insert into test_bitmap64_expression_v2_20021 values ('2022-01-01', '一', [1,2,3]), ('2022-01-02', '二', [1,2]), ('2022-01-03', '三', [1]);

-- empty expresion
select bitmapCountV2('')(tag_id, uid) from test_bitmap64_expression_v2_20021;
select bitmapExtractV2('')(tag_id, uid) from test_bitmap64_expression_v2_20021;

select bitmapMultiCountV2('')(tag_id, uid) from test_bitmap64_expression_v2_20021;
select bitmapMultiExtractV2('')(tag_id, uid) from test_bitmap64_expression_v2_20021;

select bitmapMultiCountWithDateV2('')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uid) from test_bitmap64_expression_v2_20021;
select bitmapMultiExtractWithDateV2('')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uid) from test_bitmap64_expression_v2_20021;

-- for ascii character
select bitmapCountV2('a')(tag_id, uid) from test_bitmap64_expression_v2_20021;
select bitmapExtractV2('a')(tag_id, uid) from test_bitmap64_expression_v2_20021;

select bitmapMultiCountV2('a|b','c','b')(tag_id, uid) from test_bitmap64_expression_v2_20021;
select bitmapMultiExtractV2('a|b','c','b')(tag_id, uid) from test_bitmap64_expression_v2_20021;

select bitmapMultiCountWithDateV2('20220101_a|20220102_b','20220103_c','20220103_b')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uid) from test_bitmap64_expression_v2_20021;
select bitmapMultiExtractWithDateV2('20220101_a|20220102_b','20220103_c','20220103_b')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uid) from test_bitmap64_expression_v2_20021;

-- for Unicode character
select bitmapCountV2('一')(tag_id, uid) from test_bitmap64_expression_v2_20021;
select bitmapExtractV2('一')(tag_id, uid) from test_bitmap64_expression_v2_20021;

select bitmapMultiCountV2('一|二','三','二')(tag_id, uid) from test_bitmap64_expression_v2_20021;
select bitmapMultiExtractV2('一|二','三','二')(tag_id, uid) from test_bitmap64_expression_v2_20021;

select bitmapMultiCountWithDateV2('20220101_一|20220102_二','20220103_三','20220103_二')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uid) from test_bitmap64_expression_v2_20021;
select bitmapMultiExtractWithDateV2('20220101_一|20220102_二','20220103_三','20220103_二')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uid) from test_bitmap64_expression_v2_20021;


--- exception when key has `_`, which is a keyword for expression of previous position
select bitmapMultiCountV2('一|二','三','二&_a')(tag_id, uid) from test_bitmap64_expression_v2_20021;  --{ serverError 36 }

--- check the special keyword `_`
insert into test_bitmap64_expression_v2_20021 values ('2022-01-01', 'a1', [1,2,3]), ('2022-01-02', 'b:2', [1,2]), ('2022-01-03', 'c_3', [1]), ('2022-01-04', 'd:4c_2=3', [1,3,4]), ('2022-01-05', '_5', [2,3,4]), ('2022-01-06', '_a', [1,2]);

select bitmapMultiCountV2('a&b', 'a1&b:2', 'a|c_3')(tag_id, uid) from test_bitmap64_expression_v2_20021;
select bitmapMultiCountV2('c_3', '_1|d:4c_2=3', 'b:2&_2')(tag_id, uid) from test_bitmap64_expression_v2_20021;

select bitmapCountV2('c_3')(tag_id, uid) from test_bitmap64_expression_v2_20021;
select bitmapCountV2('_5')(tag_id, uid) from test_bitmap64_expression_v2_20021;  --{ serverError 36 }
select bitmapCountV2('_a')(tag_id, uid) from test_bitmap64_expression_v2_20021;  --{ serverError 36 }
select bitmapMultiCountV2('c_3', '_1~_5')(tag_id, uid) from test_bitmap64_expression_v2_20021;  --{ serverError 36 }
select bitmapMultiCountV2('c_3', '_1&_a')(tag_id, uid) from test_bitmap64_expression_v2_20021;  --{ serverError 36 }

select bitmapMultiCountWithDateV2('20220103_c_3', '_1|20220104_d:4c_2=3', '20220102_b:2&_2')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uid) from test_bitmap64_expression_v2_20021 where tag_id IN ('c_3', 'd:4c_2=3', 'b:2');
select bitmapMultiCountWithDateV2('20220103_c_3', '_1&20220105__5', '_2~20220106__a')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uid) from test_bitmap64_expression_v2_20021;  --{ serverError 36 }

-- for wrong type of parameters
select bitmapCountV2(a)(tag_id, uid) from test_bitmap64_expression_v2_20021;  -- { serverError 47 }
select bitmapExtractV2(a)(tag_id, uid) from test_bitmap64_expression_v2_20021;  -- { serverError 47 }

select bitmapMultiExtractV2('a|b',c,'b')(tag_id, uid) from test_bitmap64_expression_v2_20021;  -- { serverError 47 }

select bitmapMultiCountWithDateV2(20220101_a,'20220103_c','20220103_b')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uid) from test_bitmap64_expression_v2_20021; -- { serverError 47 }
select bitmapMultiExtractWithDateV2('20220101_a|20220102_b','20220103_c',47)(cast(toYYYYMMDD(p_date) as Int64), tag_id, uid) from test_bitmap64_expression_v2_20021; -- { serverError 169 }

drop table if exists test_bitmap64_expression_v2_20021 sync;


--- in Cnch, bitmapXXXV2 functions have same behaviour as bitampXXX.
drop table if exists test_bitmap_expr_str_20021 sync;
drop table if exists test_bitmap_expr_int_20021 sync;

select '====== More tests on cnch (str exprs)';
create table test_bitmap_expr_str_20021 (p_date Date, tag_id String, uids BitMap64) engine = CnchMergeTree partition by p_date order by tag_id;

insert into test_bitmap_expr_str_20021 values ('2022-01-01', 'a', [1,2,3]), ('2022-01-02', 'b', [1,2]), ('2022-01-03', 'c', [1]);
insert into test_bitmap_expr_str_20021 values ('2022-01-01', '1', [3,4,5]), ('2022-01-01', '1', [5,6,7]), ('2022-01-01', '2', [1]), ('2022-01-03', '2', [1,2,3]);

select '---- bitmapCount & bitmapExtract';
select bitmapCount('a')(tag_id, uids) from test_bitmap_expr_str_20021;
select bitmapCountV2('a')(tag_id, uids) from test_bitmap_expr_str_20021;

select bitmapExtract('a')(tag_id, uids) from test_bitmap_expr_str_20021;
select bitmapExtractV2('a')(tag_id, uids) from test_bitmap_expr_str_20021;

select bitmapCount('1')(tag_id, uids) from test_bitmap_expr_str_20021;
select bitmapCountV2('1')(tag_id, uids) from test_bitmap_expr_str_20021;

select bitmapExtract('1')(tag_id, uids) from test_bitmap_expr_str_20021;
select bitmapExtractV2('1')(tag_id, uids) from test_bitmap_expr_str_20021;

select '---- bitmapMultiCount & bitmapMultiExtract';
select bitmapMultiCount('a')(tag_id, uids) from test_bitmap_expr_str_20021;
select bitmapMultiCountV2('a')(tag_id, uids) from test_bitmap_expr_str_20021;

select bitmapMultiExtract('a')(tag_id, uids) from test_bitmap_expr_str_20021;
select bitmapMultiExtractV2('a')(tag_id, uids) from test_bitmap_expr_str_20021;

select bitmapMultiCount('a', '1', 'a&1')(tag_id, uids) from test_bitmap_expr_str_20021;
select bitmapMultiCountV2('a', '1', 'a&1')(tag_id, uids) from test_bitmap_expr_str_20021;

select bitmapMultiExtract('a', '1', 'a&1')(tag_id, uids) from test_bitmap_expr_str_20021;
select bitmapMultiExtractV2('a', '1', 'a&1')(tag_id, uids) from test_bitmap_expr_str_20021;

select '---- bitmapCountWithDate & bitmapMultiExtractWithDate';
select bitmapMultiCountWithDate('20220101_a')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_str_20021;
select bitmapMultiCountWithDateV2('20220101_a')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_str_20021;

select bitmapMultiExtractWithDate('20220101_a')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_str_20021;
select bitmapMultiExtractWithDateV2('20220101_a')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_str_20021;

select bitmapMultiCountWithDate('20220101_1')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_str_20021;
select bitmapMultiCountWithDateV2('20220101_1')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_str_20021;

select bitmapMultiExtractWithDate('20220101_1')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_str_20021;
select bitmapMultiExtractWithDateV2('20220101_1')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_str_20021;

select bitmapMultiCountWithDate('20220101_a', '20220101_1', '20220101_a&20220101_1')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_str_20021;
select bitmapMultiCountWithDateV2('20220101_a', '20220101_1', '20220101_a&20220101_1')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_str_20021;

select bitmapMultiExtractWithDate('20220101_a', '20220101_1', '20220101_a&20220101_1')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_str_20021;
select bitmapMultiExtractWithDateV2('20220101_a', '20220101_1', '20220101_a&20220101_1')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_str_20021;

select bitmapMultiCountWithDate('20220101_a', '20220101_1|20220103_2', '20220101_2', '2', '20220102_2', '20220102_d', 'd')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_str_20021;
select bitmapMultiCountWithDateV2('20220101_a', '20220101_1|20220103_2', '20220101_2', '2', '20220102_2', '20220102_d', 'd')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_str_20021;

select bitmapMultiExtractWithDate('20220101_a', '20220101_1|20220103_2', '20220101_2', '2', '20220102_2', '20220102_d', 'd')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_str_20021;
select bitmapMultiExtractWithDateV2('20220101_a', '20220101_1|20220103_2', '20220101_2', '2', '20220102_2', '20220102_d', 'd')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_str_20021;


select '====== More tests on cnch (int exprs)';
create table test_bitmap_expr_int_20021 (p_date Date, tag_id Int32, uids BitMap64) engine = CnchMergeTree partition by p_date order by tag_id;

insert into test_bitmap_expr_int_20021 values ('2022-01-01', '1', [3,4,5]), ('2022-01-01', '1', [5,6,7]), ('2022-01-01', '2', [1]), ('2022-01-03', '2', [1,2,3]);

select '---- bitmapCount & bitmapExtract';
select bitmapCount('1')(tag_id, uids) from test_bitmap_expr_int_20021;
select bitmapCountV2('1')(tag_id, uids) from test_bitmap_expr_int_20021;

select bitmapExtract('1')(tag_id, uids) from test_bitmap_expr_int_20021;
select bitmapExtractV2('1')(tag_id, uids) from test_bitmap_expr_int_20021;

select '---- bitmapMultiCount & bitmapMultiExtract';
select bitmapMultiCount('1')(tag_id, uids) from test_bitmap_expr_int_20021;
select bitmapMultiCountV2('1')(tag_id, uids) from test_bitmap_expr_int_20021;

select bitmapMultiExtract('1')(tag_id, uids) from test_bitmap_expr_int_20021;
select bitmapMultiExtractV2('1')(tag_id, uids) from test_bitmap_expr_int_20021;

select bitmapMultiCount('1', '2', '1&2')(tag_id, uids) from test_bitmap_expr_int_20021;
select bitmapMultiCountV2('1', '2', '1&2')(tag_id, uids) from test_bitmap_expr_int_20021;

select bitmapMultiExtract('1', '2', '1&2')(tag_id, uids) from test_bitmap_expr_int_20021;
select bitmapMultiExtractV2('1', '2', '1&2')(tag_id, uids) from test_bitmap_expr_int_20021;

select '---- bitmapMultiCountWithDate & bitmapMultiExtractWithDate';
select bitmapMultiCountWithDate('20220101_1')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_int_20021;
select bitmapMultiCountWithDateV2('20220101_1')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_int_20021;

select bitmapMultiExtractWithDate('20220101_1')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_int_20021;
select bitmapMultiExtractWithDateV2('20220101_1')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_int_20021;

select bitmapMultiCountWithDate('20220101_1', '20220103_2', '20220101_1&20220103_2')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_int_20021;
select bitmapMultiCountWithDateV2('20220101_1', '20220103_2', '20220101_1&20220103_2')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_int_20021;

select bitmapMultiExtractWithDate('20220101_1', '20220103_2', '20220101_1&20220103_2')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_int_20021;
select bitmapMultiExtractWithDateV2('20220101_1', '20220103_2', '20220101_1&20220103_2')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_int_20021;

select bitmapMultiCountWithDate('20230101_1', '20220101_1&20230101_1', '20220101_1&20230101_a', '20220101_1&a', 'a', '1')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_int_20021;
select bitmapMultiCountWithDateV2('20230101_1', '20220101_1&20230101_1', '20220101_1&20230101_a', '20220101_1&a', 'a', '1')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_int_20021;

select bitmapMultiExtractWithDate('20230101_1', '20220101_1&20230101_1', '20220101_1&20230101_a', '20220101_1&a', 'a', '1')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_int_20021;
select bitmapMultiExtractWithDateV2('20230101_1', '20220101_1&20230101_1', '20220101_1&20230101_a', '20220101_1&a', 'a', '1')(cast(toYYYYMMDD(p_date) as Int64), tag_id, uids) from test_bitmap_expr_int_20021;

select '---- bitmapMultiCountWithDate & bitmapMultiExtractWithDate of different type';
select bitmapMultiCountWithDate('20220101_1')(cast(toYYYYMMDD(p_date) as Int32), tag_id, uids) from test_bitmap_expr_int_20021;
select bitmapMultiExtractWithDate('20220101_1')(cast(toYYYYMMDD(p_date) as Int32), tag_id, uids) from test_bitmap_expr_int_20021;

select bitmapMultiCountWithDate('20220101_1')(cast(toYYYYMMDD(p_date) as Int64), toInt16(tag_id), uids) from test_bitmap_expr_int_20021;
select bitmapMultiExtractWithDate('20220101_1')(cast(toYYYYMMDD(p_date) as Int64), toInt16(tag_id), uids) from test_bitmap_expr_int_20021;

select bitmapMultiCountWithDate('20220101_1')(cast(toYYYYMMDD(p_date) as Int32), toString(tag_id), uids) from test_bitmap_expr_int_20021;
select bitmapMultiExtractWithDate('20220101_1')(cast(toYYYYMMDD(p_date) as Int32), toString(tag_id), uids) from test_bitmap_expr_int_20021;

drop table test_bitmap_expr_str_20021 sync;
drop table test_bitmap_expr_int_20021 sync;
