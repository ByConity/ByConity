select CONVERT_TZ('2022-01-01 12:00:00','GMT','MET');
select CONVERT_TZ('2022-01-01 12:00:00','MET','GMT');

select CONVERT_TZ('2022-01-01 12:00:00','-01:00','MET');
select CONVERT_TZ('2022-01-01 12:00:00','+00:00','MET');
select CONVERT_TZ('2022-01-01 12:00:00','+01:00','MET');

select CONVERT_TZ('2022-01-01 12:00:00','GMT','-01:00');
select CONVERT_TZ('2022-01-01 12:00:00','GMT','+00:00');
select CONVERT_TZ('2022-01-01 12:00:00','GMT','+01:00');

select CONVERT_TZ('2022-01-01 12:00:00','+00:00','-00:20');
select CONVERT_TZ('2022-01-01 12:00:00','+00:00','-00:10');
select CONVERT_TZ('2022-01-01 12:00:00','+00:00','+00:00');
select CONVERT_TZ('2022-01-01 12:00:00','+00:00','+00:10');
select CONVERT_TZ('2022-01-01 12:00:00','+00:00','+00:20');
select CONVERT_TZ('2022-01-01 12:00:00','+00:10','+00:20');
select CONVERT_TZ('2022-01-01 12:00:00','+00:20','+00:20');

select CONVERT_TZ('2022-01-01 12:00:00','+0:10','+0:20');
select CONVERT_TZ('2022-01-01 12:00:00','+0:10','+1:20');

select CONVERT_TZ('1980-11-11 12:34:56','Asia/Singapore','+8:00');
select CONVERT_TZ('1980-11-11 12:34:56','+8:00','Asia/Singapore');

-- Currently CONVERT_TZ does not support non-const from_tz and to_tz.
DROP TABLE IF EXISTS 13002_convert_tz;
CREATE TABLE 13002_convert_tz(
    id Int64,
    str String,
    from_tz String,
    to_tz String
)
ENGINE = CnchMergeTree()
ORDER BY id;

INSERT INTO 13002_convert_tz VALUES (0, '2022-01-01 12:00:00','+00:00','+10:00'), (1, '2022-01-01 12:00:00','+00:00','-10:00'), (2, '2022-01-01 12:00:00','+00:00','+00:30'), (3, '2022-01-01 12:00:00','+00:00','-00:30');
SELECT id, CONVERT_TZ(str, from_tz, to_tz) FROM 13002_convert_tz; -- { serverError 44 }
DROP TABLE 13002_convert_tz;