set enable_optimizer=1;

DROP TABLE IF EXISTS nation_info;

CREATE TABLE nation_info
(
    nation_id UInt64,
    area Float64,
    create_time Date,
    name String
) ENGINE = CnchMergeTree()
ORDER BY nation_id;

insert into nation_info values(8, 744.6, '2015-02-19', 'abc'), (10, 444.6, '2016-02-19', 'qqq'), (9, 732.6, '2017-03-19', 'aaaa'), (18, 362.6, '2019-10-19', 'bbb'), (21, 219.6, '2020-01-19', 'ccc'), (28, 872.6, '2023-05-19', 'dddd');

select nation_id from nation_info where nation_id > 10 and nation_id > 20;
select nation_id, area from nation_info where area > 733 and area > 733;
select nation_id, area from nation_info where area > 733.1 and  area > 733.1;
select nation_id, create_time from nation_info where create_time > '2017-02-19' and create_time > '2016-02-19';
select nation_id, create_time from nation_info where create_time < '2017-02-19' or create_time > '2016-02-19';
DROP TABLE IF EXISTS nation_info;