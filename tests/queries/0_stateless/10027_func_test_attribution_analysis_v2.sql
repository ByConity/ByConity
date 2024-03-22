drop table if exists test_attribution;
drop table if exists test_attribution2;

CREATE TABLE test_attribution
(
    `id` UInt64,
    `time` UInt64,
    `event` String,
    `attr` String
)
ENGINE = CnchMergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

insert into test_attribution values(1,1,'A','a'),(1,2,'B','b'),(1,3,'C','c'),(1,4,'D','d'),(1,5,'E','e'),(1,6,'F','f'),(1,7,'A','a'),(1,8,'C','c'),(1,9,'B','b'),(1,10,'E','e'),(1,11,'D','d'),(1,12,'F','f'),(1,13,'C','c'),(1,14,'F','f')

SELECT attribution([3, 5], 1000, 21)(time, toUInt16(multiIf(event = 'A', 1, event = 'B', 2, event = 'C', 3, event = 'D', 4, event = 'E', 5, event = 'F', 6, 0)), toFloat64(1)) AS inner
FROM test_attribution;

SELECT attribution([3, 5], 1000, 21, 0, 0, [0,0,0,0])(time, toUInt16(multiIf(event = 'A', 1, event = 'B', 2, event = 'C', 3, event = 'D', 4, event = 'E', 5, event = 'F', 6, 0)), toFloat64(1), [], 1) AS inner
FROM test_attribution;

SELECT attribution([3, 5], 1000, 21, 0, 0, [1, 0, 2, 0])(time, toUInt16(multiIf(event = 'A', 1, event = 'B', 2, event = 'C', 3, event = 'D', 4, event = 'E', 5, event = 'F', 6, 0)), toFloat64(1), [], attr, 'f', 'f') AS inner
FROM test_attribution;

SELECT attribution([3, 5], 1000, 21, 0, 1, [1, 0, 2, 0])(time, toUInt16(multiIf(event = 'A', 1, event = 'B', 2, event = 'C', 3, event = 'D', 4, event = 'E', 5, event = 'F', 6, 0)), toFloat64(1), [attr], attr, 'f', 'f') AS inner
FROM test_attribution;

-- [0]: placeholder
SELECT attribution([3, 5], 1000, 21, 1, 0, [0])(time, toUInt16(multiIf(event = 'A', 1, event = 'B', 2, event = 'C', 3, event = 'D', 4, event = 'E', 5, event = 'F', 6, 0)), toFloat64(1)) AS inner
FROM test_attribution;

SELECT attribution([3, 5], 1000, 21, 1, 0, [1])(time, toUInt16(multiIf(event = 'A', 1, event = 'B', 2, event = 'C', 3, event = 'D', 4, event = 'E', 5, event = 'F', 6, 0)), toFloat64(1)) AS inner
FROM test_attribution; -- { serverError 36 }

SELECT attributionCorrelation(10, 1)(inner)
FROM
(
    SELECT attribution([3, 5], 1000, 21, 0, 1, [1, 0, 2, 0])(time, toUInt16(multiIf(event = 'A', 1, event = 'B', 2, event = 'C', 3, event = 'D', 4, event = 'E', 5, event = 'F', 6, 0)), toFloat64(1), [1], 1, 1, 1) AS inner
    FROM test_attribution
);

CREATE TABLE test_attribution2
(
    `time` UInt64,
    `name` String,
    `string_params` Map(String, String),
    `int_params` Map(String, Int64),
    `float_params` Map(String, Float64),
    `string_item_profiles` Map(String, Array(String)),
    `int_item_profiles` Map(String, Array(Int64)),
    `float_item_profiles` Map(String, Array(Float64))
)
ENGINE = CnchMergeTree
PARTITION BY time
ORDER BY time
SETTINGS index_granularity = 8192;

insert into test_attribution2 values (5555,'tar',{'M':'ll','N':'aa'},{'P':5,'Q':10},{'A':1.3},{'S':['aa','bb']},{'I':[2,1]},{'F':[1.2,1.3,1.5]})                             ,(1111,'end',{'M':'bb','N':'aa'},{'P':10,'Q':5},{},{'S':['aa','bb']},{'I':[1,2]},{'F':[1.1,1.2]})                                       ,(1565,'start',{'M':'ll','N':'bb'},{'P':10,'Q':10},{},{'S':['aa','cc']},{'I':[1,2]},{'F':[1.1,1.2]})                                    ,(12222,'pro',{'M':'ll','N':'cc'},{'P':5,'Q':1},{'A':1.6},{'S':['cc','bb']},{'I':[1,2]},{'F':[1.1,1.2]})                                ,(3111,'start',{'M':'ll','N':'aa'},{'P':5,'Q':10},{},{'S':['aa','bb']},{'I':[1,2]},{'F':[1.1,1.2]})                                     ,(13333,'end',{'M':'ll','N':'aa'},{'P':5,'Q':10},{},{'S':['aa','dd']},{'I':[1,2]},{'F':[1.1,1.2]})                                      ,(3333,'end',{'M':'ll','N':'aa'},{'P':5,'Q':10},{},{'S':['aa','dd']},{'I':[1,2]},{'F':[1.1,1.2]})                                       ,(11666,'end',{'M':'cc','N':'cc'},{'P':5,'Q':10},{},{'S':['aa','bb']},{'I':[1,2]},{'F':[1.1,1.2]})                                      ,(15555,'tar',{'M':'ll','N':'aa'},{'P':5,'Q':10},{'A':1.3},{'S':['aa','bb']},{'I':[2,1]},{'F':[1.2,1.3,1.5]})                           ,(1666,'end',{'M':'cc','N':'cc'},{'P':5,'Q':10},{},{'S':['aa','bb']},{'I':[1,2]},{'F':[1.1,1.2]})                                       ,(13000,'orange',{'M':'ll','N':'aa'},{'P':5,'Q':10},{'A':1.3},{'S':['aa','bb']},{'I':[1,2]},{'F':[1.1,1.2]})                            ,(13111,'start',{'M':'ll','N':'aa'},{'P':5,'Q':10},{},{'S':['aa','bb']},{'I':[1,2]},{'F':[1.1,1.2]})                                    ,(11565,'start',{'M':'ll','N':'bb'},{'P':10,'Q':10},{},{'S':['aa','cc']},{'I':[1,2]},{'F':[1.1,1.2]})                                   ,(1000,'start',{'M':'ll','N':'aa'},{'P':5,'Q':10},{},{'S':['aa','bb']},{'I':[1,2]},{'F':[1.1,1.2]})                                     ,(11000,'start',{'M':'ll','N':'aa'},{'P':5,'Q':10},{},{'S':['aa','bb']},{'I':[1,2]},{'F':[1.1,1.2]})                                    ,(3000,'orange',{'M':'ll','N':'aa'},{'P':5,'Q':10},{'A':1.3},{'S':['aa','bb']},{'I':[1,2]},{'F':[1.1,1.2]})                             ,(14444,'pro',{'M':'ll','N':'aa'},{'P':1,'Q':1},{'A':1.1},{'S':['aa','bb']},{'I':[1,2,5]},{'F':[1.1,1.3]})                              ,(4444,'pro',{'M':'ll','N':'aa'},{'P':1,'Q':1},{'A':1.1},{'S':['aa','bb']},{'I':[1,2,5]},{'F':[1.1,1.3]})                               ,(11111,'end',{'M':'bb','N':'aa'},{'P':10,'Q':5},{},{'S':['aa','bb']},{'I':[1,2]},{'F':[1.1,1.2]})                                      ,(11555,'pro',{'M':'bb','N':'aa'},{'P':5,'Q':5},{'A':1.5},{'S':['aa','bb']},{'I':[1,2]},{'F':[1.1,1.2]})                                ,(2222,'pro',{'M':'ll','N':'cc'},{'P':5,'Q':1},{'A':1.6},{'S':['cc','bb']},{'I':[1,2]},{'F':[1.1,1.2]})                                 ,(1555,'pro',{'M':'bb','N':'aa'},{'P':5,'Q':5},{'A':1.5},{'S':['aa','bb']},{'I':[1,2]},{'F':[1.1,1.2]});

SELECT attributionCorrelation(10, 1)(inner)
FROM
(
    SELECT attribution([1, 2], 1200, 10, 0, 1, [0], [20, 30, 50])(time, multiIf(name = 'pro', 1, name = 'orange', 2, name = 'start', 3, name = 'end', 4, 0), 1, [assumeNotNull(string_params{'M'})]) AS inner
    FROM test_attribution2
);
SELECT attributionCorrelation(10, 1)(inner)
FROM
(
    SELECT attribution([2, 2], 1000, 8, 0, 0, [0], [20, 30, 50])(time, multiIf(name = 'start', 1, name = 'pro', 2, name = 'end', 3, 0), 1, []) AS inner
    FROM test_attribution2
);
SELECT attributionCorrelation(10, 1)(inner)
FROM
(
    SELECT attribution([2, 2], 1000, 31, 0, 0, [0], [0])(time, multiIf(name = 'start', 1, name = 'pro', 2, name = 'end', 3, 0), 1) AS inner
    FROM test_attribution2
);

drop table if exists test_attribution;
drop table if exists test_attribution2;