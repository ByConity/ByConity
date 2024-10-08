SELECT 'new*\n*line' RLIKE 'new\\*.\\*line';
SELECT 'new*\n*line' REGEXP 'new\\*.\\*line';
SELECT 'new*\n*line' RLIKE '(?-s)new\\*.\\*line';
SELECT 'new*\n*line' REGEXP '(?-s)new\\*.\\*line';
SELECT 'c' RLIKE '^[a-d]';
SELECT 'c' REGEXP '^[a-d]';
SELECT 'Michael\!' RLIKE '.*';
SELECT 'Michael\!' REGEXP '.*';
SELECT 'database' RLIKE 'd.t.*se';
SELECT 'database' REGEXP 'd.t.*ze';
SELECT '12345' RLIKE '[0-9]+';
SELECT 'abc' RLIKE '[0-9]+';
SELECT '13:42:00' RLIKE '^[0-2][0-9]:[0-5][0-9]:[0-5][0-9]$';
SELECT '13:42:70' RLIKE '^[0-2][0-9]:[0-5][0-9]:[0-5][0-9]$';
SELECT 'hello' RLIKE '^h.*o$';
SELECT 'world' RLIKE '^h.*o$';
SELECT 'blue whale' RLIKE 'blue whale|white whale';
SELECT 'killer whale' RLIKE 'blue whale|white whale';
SELECT '' REGEXP '.*';
SELECT 'Hello, world!' RLIKE 'wor';
SELECT 'Hello, world!' RLIKE 'cat';
SELECT '123-45-6789' RLIKE '^[0-9]{3}-[0-9]{2}-[0-9]{4}$';
SELECT '1234' RLIKE '^[a-zA-Z]*$';
SELECT NULL REGEXP '.*';
SELECT NULL RLIKE '.*';
SELECT '我的名字是小明' RLIKE '.*小明.*';
SELECT '早上好，这是我的工作计划。' RLIKE '^早上好.*';
SELECT '我很喜欢狗和猫。' RLIKE '^[^猫]*$';
SELECT '熊猫喜欢吃竹子' RLIKE '大熊猫|小熊猫|熊猫猫|熊猫熊';

CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS regexp_table;
CREATE TABLE regexp_table
(
    id Int64,
    name String
)
ENGINE = CnchMergeTree()
ORDER BY id;

INSERT INTO regexp_table (id, name) VALUES (1, '张三'), (2, 'John Doe'), (3, '李四'), (4, 'María García'), (5, '王五'), (6, 'François Dupont'), (7, '123-45-6789 (SSN)'), (8, '赵六'), (9, 'Kim Min-ji'), (10, '555-1234 (phone number)');
    
SELECT name
FROM regexp_table
WHERE name RLIKE '张|李|王|赵';

SELECT name
FROM regexp_table
WHERE name RLIKE '张|John|李|María|王|François|赵|Kim';

SELECT name
FROM regexp_table
WHERE name RLIKE '555|1234|6789';

DROP TABLE IF EXISTS regexp_table;

SET dialect_type='MYSQL';
SELECT 'new*\n*line' RLIKE 'new\\*.\\*line';
SELECT 'new*\n*line' REGEXP 'new\\*.\\*line';
SELECT toDecimal32(12.345, 3) REGEXP '.*';   
SELECT 123456 REGEXP '.*';
