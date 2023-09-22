SELECT 1;
-- Reopen this testcase after RM is enabled for cnch-2.0 CI.
-- DROP WORKER GROUP IF EXISTS `wg1`;
-- DROP WORKER GROUP IF EXISTS `wg2`;
-- DROP WAREHOUSE IF EXISTS `vw1`;

-- CREATE WAREHOUSE IF NOT EXISTS `vw1` SETTINGS num_workers = 0, type = 'Default';
-- CREATE WAREHOUSE IF NOT EXISTS `vw1` SETTINGS num_workers = 0, type = 'Default';
-- CREATE WAREHOUSE `vw1` SETTINGS num_workers = 0, type = 'Default'; -- { serverError 5037 }

-- CREATE WORKER GROUP IF NOT EXISTS `wg1` IN `vw1` SETTINGS type = 'Physical';
-- CREATE WORKER GROUP IF NOT EXISTS `wg1` IN `vw1` SETTINGS type = 'Shared', shared_worker_group = 'wg_default'; -- { serverError 5041}
-- CREATE WORKER GROUP IF NOT EXISTS `wg2` IN `vw1` SETTINGS type = 'Shared', shared_worker_group = 'wg_default';

-- DROP WAREHOUSE `vw1`; -- { serverError 5043}
-- DROP WORKER GROUP IF EXISTS `wg1`;
-- DROP WORKER GROUP IF EXISTS `wg2`;
-- DROP WAREHOUSE IF EXISTS `vw1`;