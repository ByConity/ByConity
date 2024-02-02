select unix_timestamp(); -- {serverError 42}
set dialect_type='MYSQL';
select unix_timestamp() = unix_timestamp(now());