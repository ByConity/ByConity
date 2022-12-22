#!/bin/bash
set -e

mkdir -p /etc/docker/
echo '{
    "ipv6": true,
    "fixed-cidr-v6": "fd00::/8",
    "ip-forward": true,
    "log-level": "debug",
    "insecure-registries" : ["dockerhub-proxy.sas.yp-c.yandex.net:5000"],
    "registry-mirrors" : ["http://dockerhub-proxy.sas.yp-c.yandex.net:5000"]
}' | dd of=/etc/docker/daemon.json 2>/dev/null

touch /var/log/dockerd.log
dockerd --host=unix:///var/run/docker.sock --host=tcp://0.0.0.0:2375 --default-address-pool base=172.17.0.0/12,size=24 &> /var/log/dockerd.log &

set +e
reties=0
while true; do
    docker info &>/dev/null && break
    reties=$((reties+1))
    if [[ $reties -ge 100 ]]; then # 10 sec max
        echo "Can't start docker daemon, timeout exceeded." >&2
        exit 1;
    fi
    sleep 0.1
done
set -e

# cleanup for retry run if volume is not recreated
docker kill "$(docker ps -aq)" || true
docker rm "$(docker ps -aq)" || true

echo "Start tests"
export CLICKHOUSE_TESTS_SERVER_BIN_PATH=/clickhouse/bin/clickhouse-server
export CLICKHOUSE_TESTS_CLIENT_BIN_PATH=/clickhouse/bin/clickhouse-client
export CLICKHOUSE_TESTS_BASE_CONFIG_DIR=/clickhouse/etc/clickhouse-server
export CLICKHOUSE_ODBC_BRIDGE_BINARY_PATH=/clickhouse/bin/clickhouse-odbc-bridge
export CLICKHOUSE_LIBRARY_BRIDGE_BINARY_PATH=/clickhouse/bin/clickhouse-library-bridge
export CLICKHOUSE_TESTS_INTEGRATION_PATH=/home/code/tests/integration/

export DOCKER_MYSQL_GOLANG_CLIENT_TAG=${DOCKER_MYSQL_GOLANG_CLIENT_TAG:=latest}
export DOCKER_MYSQL_JAVA_CLIENT_TAG=${DOCKER_MYSQL_JAVA_CLIENT_TAG:=latest}
export DOCKER_MYSQL_JS_CLIENT_TAG=${DOCKER_MYSQL_JS_CLIENT_TAG:=latest}
export DOCKER_MYSQL_PHP_CLIENT_TAG=${DOCKER_MYSQL_PHP_CLIENT_TAG:=latest}
export DOCKER_POSTGRESQL_JAVA_CLIENT_TAG=${DOCKER_POSTGRESQL_JAVA_CLIENT_TAG:=latest}
export DOCKER_KERBEROS_KDC_TAG=${DOCKER_KERBEROS_KDC_TAG:=latest}

cd /home/code/tests/integration/
exec "$@"
