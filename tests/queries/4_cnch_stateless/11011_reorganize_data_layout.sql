set bsp_max_retry_num=0; -- disable bsp retry

DROP TABLE IF EXISTS 11011_reorganize_data_layout;

SELECT '---create old layout, read new layout---';
CREATE TABLE 11011_reorganize_data_layout(first_name Nullable(String), last_name String, email String, a UInt32, b Decimal(4, 3)) ENGINE = CnchMergeTree ORDER BY email SETTINGS reorganize_marks_data_layout = 0;
INSERT INTO 11011_reorganize_data_layout VALUES ('Clementine','Adamou','cadamou@bytedance.com',3,2.0), ('Marlowe','De Anesy','madamouc@bytedance.co.uk',4,5.0);
SELECT * FROM 11011_reorganize_data_layout;

ALTER TABLE 11011_reorganize_data_layout MODIFY SETTING reorganize_marks_data_layout = 1;

SELECT * FROM 11011_reorganize_data_layout;

DROP TABLE 11011_reorganize_data_layout;

SELECT '---create new layout, read old layout---';
CREATE TABLE 11011_reorganize_data_layout(first_name Nullable(String), last_name String, email String, a UInt32, b Decimal(4, 3)) ENGINE = CnchMergeTree ORDER BY email SETTINGS reorganize_marks_data_layout = 1;
INSERT INTO 11011_reorganize_data_layout VALUES ('Clementine','Adamou','cadamou@bytedance.com',3,2.0), ('Marlowe','De Anesy','madamouc@bytedance.co.uk',4,5.0);
SELECT * FROM 11011_reorganize_data_layout;

ALTER TABLE 11011_reorganize_data_layout MODIFY SETTING reorganize_marks_data_layout = 0;

SELECT * FROM 11011_reorganize_data_layout;

DROP TABLE 11011_reorganize_data_layout;
