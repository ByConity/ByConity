set -e -x

mkdir -p /opt/tiger/foundationdb/config
cp docker/test/foundationdb/config/*  /opt/tiger/foundationdb/config/

mkdir -p /opt/tiger/foundationdb/logs
echo "clusterdsc:test@$(hostname -I | cut -d " " -f 1):4500" > /opt/tiger/foundationdb/config/fdb.cluster

chmod -R 755 /opt/tiger/foundationdb
nohup fdbmonitor --conffile /opt/tiger/foundationdb/config/foundationdb.conf --lockfile /opt/tiger/foundationdb/fdbmonitor.pid >/opt/tiger/foundationdb/logs/log 2>&1 &

sleep 20
fdbcli -C  /opt/tiger/foundationdb/config/fdb.cluster --exec "configure new single ssd"
