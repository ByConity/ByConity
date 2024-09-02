SELECT * from remote('community-clickhouse:9000', system.one);
SELECT * from remote('community-clickhouse:9000', db.tb);
