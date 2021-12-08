SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b AND a > b;
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b AND a < b;
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b AND a >= b;
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b AND a <= b;
SELECT 1 FROM (select 2 a) A JOIN (select 1 b) B ON a <> b AND a > b;
