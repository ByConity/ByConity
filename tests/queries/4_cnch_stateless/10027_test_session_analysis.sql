drop table if exists test_session_analysis sync;
create table test_session_analysis (id UInt16, event String, time UInt64, `string_params` Map(String, LowCardinality(Nullable(String))), p_date Date) engine = CnchMergeTree order by id partition by p_date;

insert into test_session_analysis values(1, 'e1', 10000, {'s1':'1'}, '2022-02-02');
insert into test_session_analysis values(1, 'e2', 10001, {}, '2022-02-02');
insert into test_session_analysis values(1, 'e3', 10002, {}, '2022-02-02');

SELECT sessionAnalysis(10, '', '', '')(event, time, toUInt8(0), string_params{'e1'}) FROM test_session_analysis AS et;
SELECT vSessionAnalysis(10, '', '', '')(event, time, toUInt8(0), string_params{'e1'}) FROM test_session_analysis AS et;
SELECT sessionAnalysis(10, '', '', '')(event, time, toUInt8(0), assumeNotNull(string_params{'e1'})) FROM test_session_analysis AS et;
SELECT vSessionAnalysis(10, '', '', '')(event, time, toUInt8(0), assumeNotNull(string_params{'e1'})) FROM test_session_analysis AS et;

drop table if exists test_session_analysis sync;
