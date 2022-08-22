DROP DATABASE IF EXISTS test;


DROP TABLE IF EXISTS window_navigation;

CREATE TABLE window_navigation (`ts` UInt64, `user` String, `sql` String) ENGINE = CnchMergeTree() ORDER BY `user` SETTINGS index_granularity = 8192;

INSERT INTO window_navigation (`ts`, `user`, `sql`) values (1 , 'b', 's1'), (5 , 'b', 's1'), (3 , 'b', 's1'), (10, 'a', 's1'), (1 , 'a', 's1'), (5 , 'a', 's1');

select user, sql, ts, lag(ts, 1) over (partition by user order by ts) as prev, 
lead(ts, 1) over (partition by user order by ts) as follow
from window_navigation order by user, ts, prev, follow
FORMAT TabSeparated;

DROP TABLE window_navigation;