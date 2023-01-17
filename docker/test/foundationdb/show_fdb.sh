set -e -x

/opt/tiger/foundationdb/bin/fdbcli -C /opt/tiger/foundationdb/config/fdb.cluster --exec "status"

if [ -n "$ENABLE_IPV6" ]; then
  IP_ADDRESS=BYTED_HOST_IPV6
else
  IP_ADDRESS=$(hostname -I | cut -d " " -f 1) # container's ipv4 address
fi

clickhouse-client --host ${IP_ADDRESS} -q "create database fdb_test"
clickhouse-client --host ${IP_ADDRESS} -q "create table fdb_test.my_test(id Int32) ENGINE=CnchMergeTree partition by id order by tuple()"
clickhouse-client --host ${IP_ADDRESS} -q "INSERT INTO fdb_test.my_test SELECT number FROM system.numbers limit 30"
clickhouse-client --host ${IP_ADDRESS} -q "select count() FROM fdb_test.my_test"
/opt/tiger/foundationdb/bin/fdbcli -C /opt/tiger/foundationdb/config/fdb.cluster --exec "getrange gz"
clickhouse-client --host ${IP_ADDRESS} -q "drop database fdb_test"
