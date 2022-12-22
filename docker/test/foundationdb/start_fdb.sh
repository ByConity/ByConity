set -e -x
time /opt/tiger/bvc/bin/bvc clone -f data/cnch/foundationdb /opt/tiger/foundationdb --version 1.0.0.7

mkdir -p /opt/tiger/foundationdb/config
cp /home/code/docker/test/foundationdb/config/*  /opt/tiger/foundationdb/config/

mkdir -p /opt/tiger/foundationdb/logs
echo "clusterdsc:test@$(hostname -I | cut -d " " -f 1):4500" > /opt/tiger/foundationdb/config/fdb.cluster

nohup fdbmonitor --conffile /opt/tiger/foundationdb/config/foundationdb.conf --lockfile /opt/tiger/foundationdb/fdbmonitor.pid >/opt/tiger/foundationdb/logs/log 2>&1 &

sleep 20
fdbcli -C  /opt/tiger/foundationdb/config/fdb.cluster --exec "configure new single ssd"