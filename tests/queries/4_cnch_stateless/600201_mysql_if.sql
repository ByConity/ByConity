set dialect_type='MYSQL';

SELECT if(1, 1, 'NOT_ATT');
DROP TABLE IF EXISTS 600201_t1;
CREATE TABLE `600201_t1` (　 `id` bigint NOT NULL AUTO_INCREMENT COMMENT '',　 `status` tinyint DEFAULT '1' COMMENT '广告状态  待定',　 `status_att_status` tinyint DEFAULT '1' COMMENT '广告状态 可归因状态 1.可归因 2.不可归因',　 primary key (`id`)　) DISTRIBUTE BY HASH(`id`) STORAGE_POLICY='HOT' ENGINE='XUANWU' TABLE_PROPERTIES='{"format":"columnstore"}';
INSERT INTO 600201_t1(id) VALUES(1)
SELECT if(0, attmbd.status, 'NOT_ATT')　FROM 600201_t1 AS attmbd;
SELECT if(1, attmbd.status, 'NOT_ATT')　FROM 600201_t1 AS attmbd;
DROP TABLE IF EXISTS 600201_t1;