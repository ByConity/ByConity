drop DATABASE if exists test_48040;
CREATE DATABASE test_48040;

use test_48040;

drop table if exists t_daily_fund_extra_all;
drop table if exists t_daily_bet_all;

CREATE TABLE t_daily_fund_extra_all 
(
    `product` String COMMENT '产品',
    `sum_date` DateTime COMMENT '汇总时间',
    `login_name` String COMMENT '登录名',
    `version_time` DateTime COMMENT '版本时间',
    `deposit_num` Int128 COMMENT '存款数量',
    `deposit_amount` Decimal(38, 6) COMMENT '存款金额',
    `withdraw_num` Int128 COMMENT '取款金额',
    `withdraw_amount` Decimal(38, 6) COMMENT '取款金额',
    `promotion_amount` Decimal(38, 6) COMMENT '优惠金额',
    `first_deposit_amount` Decimal(38, 6) COMMENT '首存金额',
    `branch_code` String COMMENT '门店code',
    `parent` String COMMENT '上级',
    `site_id` Int8 COMMENT '网站id',
    `sex` String COMMENT 'M=Male,\n \r\n\r\n\r\n\r\n\r\nF=Female',
    `channel_type` Int8 COMMENT '渠道类型 1: Market,\n \r\n\r\n\r\n\r\n\r\n2:Website,\n\r\n\r\n\r\n\r\n\r\n3:Store,\n\r\n\r\n\r\n\r\n\r\n4:glife,\n\r\n\r\n\r\n\r\n\r\n5:blue chip',
    `register_time` DateTime COMMENT '注册时间',
    `trans_site_id` String COMMENT '用户存款站点'
)
ENGINE = CnchMergeTree
PARTITION BY toYYYYMMDD(sum_date)
PRIMARY KEY (sum_date, branch_code, site_id, product, parent, sex, channel_type, login_name, trans_site_id)
ORDER BY (sum_date, branch_code, site_id, product, parent, sex, channel_type, login_name, trans_site_id)
UNIQUE KEY (sum_date, branch_code, site_id, product, parent, sex, channel_type, login_name, trans_site_id);

CREATE TABLE t_daily_bet_all
(
    `login_name` String COMMENT '登录名',
    `start_date` DateTime COMMENT '汇总日期(单位：天)',
    `product` String COMMENT '产品',
    `platform` String COMMENT '游戏厅',
    `gamekind` Int256 COMMENT '游戏厅大类',
    `gametype` String COMMENT '游戏类型',
    `totalbetamount` Decimal(38, 6) COMMENT '总投注额',
    `totalvalidamount` Decimal(38, 6) COMMENT '总有效投注额',
    `totalbettimes` Int128 COMMENT '投注次数',
    `parent` String COMMENT '上级',
    `totalheadcount` Int128 COMMENT '投注人数',
    `totalwinlostamount` Decimal(38, 6) COMMENT '总投注输赢值',
    `totaljackpotamount` Decimal(38, 6) COMMENT '总投注jackpot',
    `is_online` Int128 COMMENT '线上or线下 ',
    `branch_code` String COMMENT '门店code',
    `version_time` DateTime DEFAULT now() COMMENT '版本时间',
    `bingoggr` Decimal(38, 6) COMMENT 'bingo游戏ggr ',
    `site_id` Int128 COMMENT '网站id ',
    `jackpot_new` Decimal(38, 2) COMMENT 'jackpot增量 ',
    `winloss_new` Decimal(38, 2) COMMENT '输赢值增量 ',
    `bet_site_id` Int128 COMMENT '网站投注id ',
    `city` String COMMENT '城市',
    `channel_type` Int8 COMMENT '渠道类型 1: Market,\\\\r\\\\n\\\\r\\\\n\\\\r\\\\n\\\\r\\\\n\\\\r\\\\n2:Website,\\\\r\\\\n\\\\r\\\\n\\\\r\\\\n\\\\r\\\\n\\\\r\\\\n3:Store,\\\\r\\\\n\\\\r\\\\n\\\\r\\\\n\\\\r\\\\n\\\\r\\\\n4:glife,\\\\r\\\\n\\\\r\\\\n\\\\r\\\\n\\\\r\\\\n\\\\r\\\\n5:blue chip',
    `totaljpbetamount` Decimal(38, 6) COMMENT 'jackpot投注总额',
    `totalwonbetamount` Decimal(38, 6) COMMENT ' 类型金额',
    `hashcode` String COMMENT '校验码',
    `ggr` Decimal(38, 6) COMMENT 'ggr'
)
ENGINE = CnchMergeTree
PARTITION BY toYYYYMMDD(start_date)
PRIMARY KEY (login_name, platform, start_date, gamekind, gametype, site_id, bet_site_id, branch_code, is_online, channel_type, parent, hashcode)
ORDER BY (login_name, platform, start_date, gamekind, gametype, site_id, bet_site_id, branch_code, is_online, channel_type, parent, hashcode)
UNIQUE KEY (login_name, platform, start_date, sipHash64(gamekind), gametype, sipHash64(site_id), sipHash64(bet_site_id), branch_code, sipHash64(is_online), channel_type, parent, hashcode)
SETTINGS storage_policy = 'cnch_default_hdfs', index_granularity = 8192;

insert into t_daily_bet_all values('test', '2024-02-11', 'a', 'a', 1, 'a', 1.1, 1.1, 1, 'a', 1, 1.1, 1.1, 1, 'a', '2024-02-11', 1.1, 1, 1.1, 1.1, 1, 'hangzhou',1, 1.1, 1.1, 'a',1.1);
insert into t_daily_bet_all values('test', '2024-02-12', 'a', 'a', 1, 'a', 1.1, 1.1, 1, 'a', 1, 1.1, 1.1, 1, 'a', '2024-02-12', 1.1, 1, 1.1, 1.1, 1, 'hangzhou',1, 1.1, 1.1, 'a',1.1);

insert into t_daily_bet_all values('real', '2024-02-13', 'a', 'a', 1, 'a', 1.1, 1.1, 1, 'a', 1, 1.1, 1.1, 1, 'a', '2024-02-13', 1.1, 1, 1.1, 1.1, 1, 'hangzhou',1, 1.1, 1.1, 'a',1.1);
insert into t_daily_bet_all values('real', '2024-02-14', 'a', 'a', 1, 'a', 1.1, 1.1, 1, 'a', 1, 1.1, 1.1, 1, 'a', '2024-02-14', 1.1, 1, 1.1, 1.1, 1, 'hangzhou',1, 1.1, 1.1, 'a',1.1);

insert into t_daily_fund_extra_all values('a', '2024-02-11', 'test', '2024-02-11', 1, 1.1, 1, 1.1, 1.1, 1.1, 'a', 'a', 1, 'M', 1, '2024-02-11', '1');
insert into t_daily_fund_extra_all values('a', '2024-02-12', 'test', '2024-02-12', 1, 1.1, 1, 1.1, 1.1, 1.1, 'a', 'a', 1, 'M', 1, '2024-02-12', '1');

insert into t_daily_fund_extra_all values('a', '2024-02-11', 'xxx', '2024-02-11', 1, 1.1, 1, 1.1, 1.1, 1.1, 'a', 'a', 1, 'M', 1, '2024-02-11', '1');
insert into t_daily_fund_extra_all values('a', '2024-02-12', 'yyy', '2024-02-12', 1, 1.1, 1, 1.1, 1.1, 1.1, 'a', 'a', 1, 'M', 1, '2024-02-12', '1');

set join_use_nulls = 1;

SELECT DISTINCT
        `account`,
        `branch`,
        `deposit`,
        `withdrawal`,
        `totalheadcount`,
        `turnover`,
        `newbetAmount`,
        `parent`,
        `winAndLoss`,
        `payout`,
        `jackpot`,
        `grossGamingRevenue`
FROM
(
        SELECT
                `login_name` AS `account`,
                `if`(`tdfe`.`parent` = '', `tdb`.`parent`, `tdfe`.`parent`) AS `parent`,
                `if`(`tdfe`.`branch_code` = '', `tdb`.`branch_code`, `tdfe`.`branch_code`) AS `branch`,
                `ifNull`(`deposit`, 0) AS `deposit`,
                `ifNull`(`withdraw`, 0) AS `withdrawal`,
                `totalvalidamount` AS `turnover`,
                `totalwinlostamount` AS `winAndLoss`,
                `totalnewbetAmount` AS `newbetAmount`,
                (`newbetAmount` + `winAndLoss`) AS `payout`,
                `totalheadcount` AS `totalheadcount`,
                `totaljackpotamount` AS `jackpot`,
                `totalggr` AS `grossGamingRevenue`
        FROM
        (
                SELECT
                        `start_date`,
                        `login_name`,
                        `min`(`parent`) AS `parent`,
                        `min`(`branch_code`) AS `branch_code`,
                        `sum`(`dr_totalvalidamount`) AS `totalvalidamount`,
                        `sum`(`dr_totalwinlostamount`) AS `totalwinlostamount`,
                        `sum`(`dr_totalbetamount`) AS `totalnewbetAmount`,
                        `sum`(`dr_totalheadcount`) AS `totalheadcount`,
                        `sum`(`dr_totaljackpotamount`) AS `totaljackpotamount`,
                        `sum`(`dr_bingoggr`) AS `totalggr`
                FROM
                (
                        SELECT
                                `toDate`(`start_date`) AS `start_date`,
                                `login_name`,
                                `product`,
                                `gamekind`,
                                `platform`,
                                `gametype` AS `gametype`,
                                `site_id`,
                                CASE
                                        WHEN (`gamekind` = 1) AND (`platform` = '031') THEN 1
                                        WHEN (`gamekind` = 1) AND (`platform` = '131') THEN 2
                                        ELSE `bet_site_id`
                                END AS `bet_site_id`,
                                `parent`,
                                `branch_code`,
                                `is_online`,
                                CASE
                                        WHEN `match`(`parent`, '^mkt|^pc|^pa|^answer|^team|^vivo|^agt|^ma|^lc|^sanxingmarket|^oppomarket|^huaweiapp|^xiaomimarket') THEN '1'
                                        WHEN NOT(`match`(`parent`, '^mkt|^pc|^pa|^answer|^team|^vivo|^agt|^ma|^lc|^sanxingmarket|^oppomarket|^huaweiapp|^xiaomimarket|^glife|^a00|^b00|^c0|^bc')) THEN '2'
                                        WHEN `match`(`parent`, '^glife') THEN '4'
                                        WHEN `match`(`parent`, '^a00|^b00|^c0') THEN '3'
                                        WHEN `match`(`parent`, '^bc') THEN '5'
                                        ELSE '2'
                                END AS `channel`,
                                `totalvalidamount` AS `dr_totalvalidamount`,
                                `totalbetamount` AS `dr_totalbetamount`,
                                `totalwinlostamount` AS `dr_totalwinlostamount`,
                                `totalheadcount` AS `dr_totalheadcount`,
                                CASE
                                        WHEN `gamekind` = '17' THEN `totaljackpotamount`
                                        ELSE `jackpot_new`
                                END AS `dr_totaljackpotamount`,
                                CASE
                                        WHEN `gamekind` IN ('27', '21') THEN `bingoggr` * -1
                                        ELSE `dr_totalwinlostamount`
                                END AS `dr_bingoggr`
                        FROM `t_daily_bet_all` AS `tdba`
                )
                GROUP BY
                        `start_date`,
                        `login_name`
        ) AS `tdb`
        GLOBAL LEFT OUTER JOIN
        (
                SELECT
                        `sum_date`,
                        `login_name`,
                        `max`(`parent`) AS `parent`,
                        `max`(`branch_code`) AS `branch_code`,
                        `sum`(`dr_deposit`) AS `deposit`,
                        `sum`(`dr_withdraw`) AS `withdraw`
                FROM
                (
                        SELECT
                                `product`,
                                `toDate`(`sum_date`) AS `sum_date`,
                                `login_name`,
                                `deposit_amount` AS `dr_deposit`,
                                `withdraw_amount` AS `dr_withdraw`,
                                `parent`,
                                CASE
                                        WHEN `match`(`parent`, '^mkt|^pc|^pa|^answer|^team|^vivo|^agt|^ma|^lc|^sanxingmarket|^oppomarket|^huaweiapp|^xiaomimarket') THEN '1'
                                        WHEN NOT(`match`(`parent`, '^mkt|^pc|^pa|^answer|^team|^vivo|^agt|^ma|^lc|^sanxingmarket|^oppomarket|^huaweiapp|^xiaomimarket|^glife|^a00|^b00|^c0|^bc')) THEN '2'
                                        WHEN `match`(`parent`, '^glife') THEN '4'
                                        WHEN `match`(`parent`, '^a00|^b00|^c0') THEN '3'
                                        WHEN `match`(`parent`, '^bc') THEN '5'
                                        ELSE '2'
                                END AS `channel`,
                                `branch_code`
                        FROM `t_daily_fund_extra_all` AS `tdfea`
                )
                GROUP BY
                        `sum_date`,
                        `login_name`
        ) AS `tdfe` ON (`tdb`.`start_date` = `tdfe`.`sum_date`) AND (`tdfe`.`login_name` = `tdb`.`login_name`)
        UNION ALL
        SELECT
                `login_name` AS `account`,
                `if`(`tdfe`.`parent` = '', `tdb`.`parent`, `tdfe`.`parent`) AS `parent`,
                `if`(`tdfe`.`branch_code` = '', `tdb`.`branch_code`, `tdfe`.`branch_code`) AS `branch`,
                `ifNull`(`deposit`, 0) AS `deposit`,
                `ifNull`(`withdraw`, 0) AS `withdrawal`,
                `totalvalidamount` AS `turnover`,
                `totalwinlostamount` AS `winAndLoss`,
                `totalnewbetAmount` AS `newbetAmount`,
                (`newbetAmount` + `winAndLoss`) AS `payout`,
                `totalheadcount` AS `totalheadcount`,
                `totaljackpotamount` AS `jackpot`,
                `totalggr` AS `grossGamingRevenue`
        FROM
        (
                SELECT
                        `sum_date`,
                        `login_name`,
                        `max`(`parent`) AS `parent`,
                        `max`(`branch_code`) AS `branch_code`,
                        `sum`(`dr_deposit`) AS `deposit`,
                        `sum`(`dr_withdraw`) AS `withdraw`
                FROM
                (
                        SELECT
                                `product`,
                                `toDate`(`sum_date`) AS `sum_date`,
                                `login_name`,
                                `deposit_amount` AS `dr_deposit`,
                                `withdraw_amount` AS `dr_withdraw`,
                                `parent`,
                                CASE
                                        WHEN `match`(`parent`, '^mkt|^pc|^pa|^answer|^team|^vivo|^agt|^ma|^lc|^sanxingmarket|^oppomarket|^huaweiapp|^xiaomimarket') THEN '1'
                                        WHEN NOT(`match`(`parent`, '^mkt|^pc|^pa|^answer|^team|^vivo|^agt|^ma|^lc|^sanxingmarket|^oppomarket|^huaweiapp|^xiaomimarket|^glife|^a00|^b00|^c0|^bc')) THEN '2'
                                        WHEN `match`(`parent`, '^glife') THEN '4'
                                        WHEN `match`(`parent`, '^a00|^b00|^c0') THEN '3'
                                        WHEN `match`(`parent`, '^bc') THEN '5'
                                        ELSE '2'
                                END AS `channel`,
                                `branch_code`
                        FROM `t_daily_fund_extra_all` AS `tdfea`
                )
                GROUP BY
                        `sum_date`,
                        `login_name`
        ) AS `tdfe`
        GLOBAL LEFT OUTER JOIN
        (
                SELECT
                        `start_date`,
                        `login_name`,
                        `min`(`parent`) AS `parent`,
                        `min`(`branch_code`) AS `branch_code`,
                        `sum`(`dr_totalvalidamount`) AS `totalvalidamount`,
                        `sum`(`dr_totalwinlostamount`) AS `totalwinlostamount`,
                        `sum`(`dr_totalbetamount`) AS `totalnewbetAmount`,
                        `sum`(`dr_totalheadcount`) AS `totalheadcount`,
                        `sum`(`dr_totaljackpotamount`) AS `totaljackpotamount`,
                        `sum`(`dr_bingoggr`) AS `totalggr`
                FROM
                (
                        SELECT
                                `toDate`(`start_date`) AS `start_date`,
                                `login_name`,
                                `product`,
                                `gamekind`,
                                `platform`,
                                `gametype` AS `gametype`,
                                `site_id`,
                                CASE
                                        WHEN (`gamekind` = 1) AND (`platform` = '031') THEN 1
                                        WHEN (`gamekind` = 1) AND (`platform` = '131') THEN 2
                                        ELSE `bet_site_id`
                                END AS `bet_site_id`,
                                `parent`,
                                `branch_code`,
                                `is_online`,
                                CASE
                                        WHEN `match`(`parent`, '^mkt|^pc|^pa|^answer|^team|^vivo|^agt|^ma|^lc|^sanxingmarket|^oppomarket|^huaweiapp|^xiaomimarket') THEN '1'
                                        WHEN NOT(`match`(`parent`, '^mkt|^pc|^pa|^answer|^team|^vivo|^agt|^ma|^lc|^sanxingmarket|^oppomarket|^huaweiapp|^xiaomimarket|^glife|^a00|^b00|^c0|^bc')) THEN '2'
                                        WHEN `match`(`parent`, '^glife') THEN '4'
                                        WHEN `match`(`parent`, '^a00|^b00|^c0') THEN '3'
                                        WHEN `match`(`parent`, '^bc') THEN '5'
                                        ELSE '2'
                                END AS `channel`,
                                `totalvalidamount` AS `dr_totalvalidamount`,
                                `totalbetamount` AS `dr_totalbetamount`,
                                `totalwinlostamount` AS `dr_totalwinlostamount`,
                                `totalheadcount` AS `dr_totalheadcount`,
                                CASE
                                        WHEN `gamekind` = '17' THEN `totaljackpotamount`
                                        ELSE `jackpot_new`
                                END AS `dr_totaljackpotamount`,
                                CASE
                                        WHEN `gamekind` IN ('27', '21') THEN `bingoggr` * -1
                                        ELSE `dr_totalwinlostamount`
                                END AS `dr_bingoggr`
                        FROM `t_daily_bet_all` AS `tdba`
                )
                GROUP BY
                        `start_date`,
                        `login_name`
        ) AS `tdb` ON (`tdfe`.`sum_date` = `tdb`.`start_date`) 
)
ORDER BY
        `account`
LIMIT 20;