DROP TABLE IF EXISTS consumer_hour;
DROP TABLE IF EXISTS sender_hour;

CREATE TABLE consumer_hour
(
    `app_id` String COMMENT '项目id',
    `third_app_id` String COMMENT '项目id',
    `task_id` String COMMENT '任务id',
    `task_type` String COMMENT '任务类型',
    `sub_task_id` String COMMENT '子任务id',
    `schedule_id` String COMMENT '调度id',
    `current_version_index` String COMMENT '文案版本',
    `status_code` String COMMENT '状态码',
    `non_uniq_count` SimpleAggregateFunction(sum, UInt64) COMMENT '不去重数量',
    `bm_log_id` AggregateFunction(groupUniqArray, UInt64) COMMENT 'logid bitmap',
    `bm_current_id` AggregateFunction(groupUniqArray, UInt64) COMMENT 'currentid bitmap',
    `update_time` DateTime COMMENT '更新时间',
    `p_date` DateTime COMMENT '数据分区-天',
    `hour` DateTime COMMENT '数据时间-小时(DateTime)',
    `extra_order_by_key` String,
    `platform_os` String DEFAULT '',
    `sub_task_type` String DEFAULT ''
)
    ENGINE = CnchAggregatingMergeTree
PARTITION BY p_date
ORDER BY (app_id, task_type, task_id, status_code, schedule_id, sub_task_id, current_version_index, extra_order_by_key, hour)
SETTINGS index_granularity = 8192;

CREATE TABLE sender_hour
(
    `app_id` String COMMENT '项目id',
    `task_type` String COMMENT '任务类型',
    `task_id` String COMMENT '任务id',
    `sub_task_id` String COMMENT '子任务id',
    `schedule_id` String COMMENT '调度id',
    `current_version_index` String COMMENT '文案版本',
    `short_link_id` String COMMENT '短链id',
    `status_code` String COMMENT '状态码',
    `non_uniq_count` SimpleAggregateFunction(sum, UInt64) COMMENT '不去重数量',
    `bm_send_id` AggregateFunction(groupUniqArray, UInt64) COMMENT 'sendid bitmap',
    `bm_current_id` AggregateFunction(groupUniqArray, UInt64) COMMENT 'currentid bitmap',
    `bm_log_id` AggregateFunction(groupUniqArray, UInt64) COMMENT 'logid bitmap',
    `update_time` DateTime COMMENT '更新时间',
    `p_date` DateTime COMMENT '数据分区-天',
    `hour` DateTime COMMENT '数据时间-小时(DateTime)',
    `extra_order_by_key` String,
    `item_id` String DEFAULT '',
    `channel_type` String DEFAULT '',
    `platform_os` String DEFAULT '',
    `sub_task_type` String DEFAULT '',
    `staff_id` String DEFAULT ''
)
    ENGINE = CnchAggregatingMergeTree
PARTITION BY p_date
ORDER BY (app_id, task_type, task_id, status_code, schedule_id, sub_task_id, current_version_index, extra_order_by_key, hour)
SETTINGS index_granularity = 8192;


SELECT
    'bm_log_id',
    length(groupUniqArrayMerge(bm_log_id)) AS status_code_value,
    status_code AS metric_key
FROM consumer_hour
WHERE ((((task_id IN ('1179')) AND (task_type IN ('task'))) AND (app_id IN ('1012733'))) AND ((p_date >= '2023-04-10') AND (p_date <= '2024-05-03'))) AND (status_code IN ('320004', '320002', '320001', '300015', '320005', '320003', '320000', '300013', '300012', '300014', '300011'))
GROUP BY status_code
UNION ALL
SELECT
    'bm_log_id',
    length(groupUniqArrayMerge(sub_metric_value)) AS status_code_value,
    'log_id_valid_req' AS metric_key
FROM
    (
        SELECT bm_log_id AS sub_metric_value
        FROM sender_hour
        WHERE ((((status_code IN ('100000', '114409', '114437', '721432', '114360', '114395', '114007', '114311', '114352', '114411', '114011', '114355', '114375', '114309', '114325', '114406', '114442', '114014', '114306', '114342', '114385', '145509', '148001', '114301', '114303', '114347', '114402', '147001', '114441', '114381', '114407', '114440', '143108', '114394', '114403', '721000', '114389', '114396', '114435', '145001', '147002', '114005', '114013', '145501', '114000', '114338', '114377', '114010', '114324', '114326', '114372', '143004', '114001', '114328', '114401', '145505', '114003', '114426', '114357', '114410', '114438', '721425', '114333', '114335', '145504', '154513', '114500', '721400', '143000', '114374', '114418', '114346', '114358', '114365', '114412', '114417', '142001', '114310', '114330', '111113', '114349', '114429', '143113', '114316', '114332', '143003', '721405', '114344', '114345', '114362', '114408', '114425', '800007', '114313', '114327', '114430', '721441', '114307', '114337', '114350', '143103', '114366', '114434', '800005', '114321', '114404', '114012', '114300', '114305', '114433', '145002', '143107', '114443', '143109', '143002', '114002', '114323', '114368', '143110', '114439', '145508', '144001', '114393', '114420', '114427', '721011', '114397', '114414', '114421', '142000', '143114', '114351', '114386', '114416', '145503', '800002', '143001', '114314', '114353', '114006', '114370', '143102', '154514', '114341', '114382', '114422', '145510', '143100', '143111', '114004', '143005', '114405', '145506', '114302', '114376', '140020', '143112', '114388', '154512', '114383', '114387', '114304', '114317', '114354', '114318', '114367', '114400', '114428', '143105', '721402', '114308', '114020', '114390', '800004', '140011', '143006', '141001', '114384', '143106', '721014', '114015', '114380', '114419', '114329', '114423', '146001', '114009', '114315', '114373', '114379', '721002', '114348', '114361', '114369', '143116', '145511', '721003', '114331', '114399', '114356', '114371', '114398', '114413', '114322', '114343', '114392', '114319', '114364', '114436', '143101', '145507', '143104', '114359', '114378', '114339', '114431', '141000', '114415', '114424', '154515', '114320', '145502', '800003', '114008', '114312', '114432', '721001', '114363', '114391', '114334', '114336')) AND (task_id IN ('1179'))) AND (task_type IN ('task'))) AND (app_id IN ('1012733'))) AND ((p_date >= '2023-04-10') AND (p_date <= '2024-05-03'))
    )
        SETTINGS enable_optimizer = 1;

DROP TABLE IF EXISTS consumer_hour;
DROP TABLE IF EXISTS sender_hour;
