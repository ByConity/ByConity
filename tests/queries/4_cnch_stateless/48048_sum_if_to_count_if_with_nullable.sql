drop DATABASE if exists test_48048;
CREATE DATABASE test_48048;

use test_48048;
drop table if exists users;

CREATE TABLE users
(
    `project_id` UInt32,
    `string_profiles` Map(String, String)
)
ENGINE = CnchMergeTree
ORDER BY (project_id)
UNIQUE KEY (project_id);

set enable_optimizer = 1;
set optimize_rewrite_sum_if_to_count_if=0;
set enable_sum_if_to_count_if=1;

explain select sumIf(1, string_profiles{'gender'} = NULL) from users;
explain select sum(if(string_profiles{'gender'} = NULL, 1, 0)) from users;
explain select sum(if(string_profiles{'gender'} = NULL, 0, 1)) from users;


explain select sum(if(project_id < 10, 0, 1)) from users;