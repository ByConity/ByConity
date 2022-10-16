DROP TABLE IF EXISTS test.join_test_left;

CREATE table test.join_test_left
(
    id int,
    dname String,
    score float,
    date String
)
ENGINE = CnchHive(`thrift://10.112.121.82:9301`, `cnchhive_ci`, `join_test_left`)
PARTITION by date;


DROP TABLE IF EXISTS test.join_test_right;
CREATE table test.join_test_right
(
    id int,
    dname String,
    web String,
    develop String,
    date    String
)
ENGINE = CnchHive(`thrift://10.112.121.82:9301`, `cnchhive_ci`, `join_test_right`)
PARTITION by date;

select * from test.join_test_left l  all inner join  test.join_test_right r  on l.id = r.id order by l.id ;


DROP TABLE IF EXISTS test.join_test_left;
DROP TABLE IF EXISTS test.join_test_right;

