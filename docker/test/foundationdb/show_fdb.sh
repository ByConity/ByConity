set -e -x

fdbcli -C /opt/tiger/foundationdb/config/fdb.cluster --exec "status"

clickhouse-client -q "create database fdb_test"
clickhouse-client -q "create table fdb_test.my_test(id Int32) ENGINE=CnchMergeTree partition by id order by tuple()"
clickhouse-client -q "INSERT INTO fdb_test.my_test SELECT number FROM system.numbers limit 30"
clickhouse-client -q "select count() FROM fdb_test.my_test"
fdbcli -C /opt/tiger/foundationdb/config/fdb.cluster --exec "getrange gz"
clickhouse-client -q "drop database fdb_test"