set enable_optimizer=1;
select database from system.cnch_kafka_log FORMAT Null;
-- should be rewritten to cnch_system.cnch_kafka_log
explain select database from system.cnch_kafka_log;
