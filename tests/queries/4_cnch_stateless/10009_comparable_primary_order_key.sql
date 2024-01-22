
DROP TABLE IF EXISTS `10009_comparable_primary_order_key`;

CREATE TABLE `10009_comparable_primary_order_key` (d UInt8, m Map(String, String)) Engine=CnchMergeTree PARTITION BY m ORDER BY d; -- { serverError 549 }
CREATE TABLE `10009_comparable_primary_order_key` (d UInt8, m Map(String, String)) Engine=CnchMergeTree PARTITION BY (d, m) ORDER BY d; -- { serverError 549 }

CREATE TABLE `10009_comparable_primary_order_key` (d UInt8, m Map(String, String)) Engine=CnchMergeTree ORDER BY m; -- { serverError 549 }
CREATE TABLE `10009_comparable_primary_order_key` (d UInt8, m Map(String, String)) Engine=CnchMergeTree ORDER BY (d,m); -- { serverError 549 }

CREATE TABLE `10009_comparable_primary_order_key` (d UInt8, m Map(String, String)) Engine=CnchMergeTree ORDER BY d PRIMARY KEY m; -- { serverError 549 }
CREATE TABLE `10009_comparable_primary_order_key` (d UInt8, m Map(String, String)) Engine=CnchMergeTree ORDER BY d PRIMARY KEY (d, m); -- { serverError 549 }

CREATE TABLE `10009_comparable_primary_order_key` (d UInt8, m Map(String, String) KV) Engine=CnchMergeTree PARTITION BY m ORDER BY d;
DROP TABLE `10009_comparable_primary_order_key`;

CREATE TABLE `10009_comparable_primary_order_key` (d UInt8, m Map(String, String) KV) Engine=CnchMergeTree PARTITION BY (d, m) ORDER BY d;
DROP TABLE `10009_comparable_primary_order_key`;

CREATE TABLE `10009_comparable_primary_order_key` (d UInt8, m Map(String, String) KV) Engine=CnchMergeTree ORDER BY m;
DROP TABLE `10009_comparable_primary_order_key`;

CREATE TABLE `10009_comparable_primary_order_key` (d UInt8, m Map(String, String) KV) Engine=CnchMergeTree ORDER BY (d,m);
DROP TABLE `10009_comparable_primary_order_key`;

CREATE TABLE `10009_comparable_primary_order_key` (d UInt8, m Map(String, String)) Engine=CnchMergeTree ORDER BY d;
DROP TABLE `10009_comparable_primary_order_key`;
