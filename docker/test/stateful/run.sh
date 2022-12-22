#!/bin/bash

set -e -x

# dpkg -i package_folder/clickhouse-common-static_*.deb;
# dpkg -i package_folder/clickhouse-common-static-dbg_*.deb
# dpkg -i package_folder/clickhouse-server_*.deb
# dpkg -i package_folder/clickhouse-client_*.deb
# dpkg -i package_folder/clickhouse-test_*.deb
if [ $sanitizer_type == "tsan" ]; then
    /bin/bash /home/code/programs/install/ch_install.sh
else
    /clickhouse/bin/clickhouse install
fi
cp /clickhouse/bin/clickhouse-test /usr/bin/clickhouse-test
cp -r /clickhouse/share/clickhouse-test /usr/share/

# install test configs
/usr/share/clickhouse-test/config/install.sh

# prepare test_output directory
mkdir -p test_output
mkdir -p sanitizer_log_output


function start()
{
    if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
        # NOTE We run "clickhouse server" instead of "clickhouse-server"
        # to make "pidof clickhouse-server" return single pid of the main instance.
        # We wil run main instance using "service clickhouse-server start"
        clickhouse /usr/bin/clickhouse server --config /etc/clickhouse-server1/config.xml --daemon \
        -- --path /var/lib/clickhouse1/ --logger.stderr /var/log/clickhouse-server/stderr1.log \
        --logger.log /var/log/clickhouse-server/clickhouse-server1.log --logger.errorlog /var/log/clickhouse-server/clickhouse-server1.err.log \
        --tcp_port 19000 --tcp_port_secure 19440 --http_port 18123 --https_port 18443 --interserver_http_port 19009 --tcp_with_proxy_port 19010 \
        --mysql_port 19004 --postgresql_port 19005 \
        --keeper_server.tcp_port 19181 --keeper_server.server_id 2

        clickhouse /usr/bin/clickhouse server --config /etc/clickhouse-server2/config.xml --daemon \
        -- --path /var/lib/clickhouse2/ --logger.stderr /var/log/clickhouse-server/stderr2.log \
        --logger.log /var/log/clickhouse-server/clickhouse-server2.log --logger.errorlog /var/log/clickhouse-server/clickhouse-server2.err.log \
        --tcp_port 29000 --tcp_port_secure 29440 --http_port 28123 --https_port 28443 --interserver_http_port 29009 --tcp_with_proxy_port 29010 \
        --mysql_port 29004 --postgresql_port 29005 \
        --keeper_server.tcp_port 29181 --keeper_server.server_id 3
    fi
    if [ $sanitizer_type == "tsan" ]; then
        clickhouse /usr/bin/clickhouse-server --config /etc/clickhouse-server/config.xml --pid-file /var/run/clickhouse-server/clickhouse-server.pid 2> /var/log/clickhouse-server/server.log &
    else
        sudo clickhouse start
    fi
    sleep 10
    counter=0
    until clickhouse-client --query "SELECT 1"
    do
        if [ "$counter" -gt 120 ]
        then
            echo "Cannot start clickhouse-server"
            cat /var/log/clickhouse-server/stdout.log
            tail -n1000 /var/log/clickhouse-server/stderr.log
            tail -n1000 /var/log/clickhouse-server/clickhouse-server.log
            break
        fi
        if [ $sanitizer_type == "tsan" ]; then
            timeout 120 clickhouse /usr/bin/clickhouse-server --config /etc/clickhouse-server/config.xml --pid-file /var/run/clickhouse-server/clickhouse-server.pid 2> /var/log/clickhouse-server/server.log &
        else
            timeout 120 service clickhouse-server start
        fi
        sleep 0.5
        counter=$((counter + 1))
    done
}

start
ps -aux

# shellcheck disable=SC2086 # No quotes because I want to split it into words.
if [ ! -d "/ClickHouse-ci-dataset" ]; then
  git clone --depth 1 https://gitlab+deploy-token-3553:pcgsySXCwK8md2BHebPw@code.byted.org/dp/ClickHouse-ci-dataset.git # download dataset from ClickHouse-ci-dataset repo
fi
cp /home/code/docker/test/stateful/s3downloader /s3downloader -f
/s3downloader --dataset-names $DATASETS  --use-local-dataset "/ClickHouse-ci-dataset/ce/"
chmod 777 -R /var/lib/clickhouse
clickhouse-client --query "SHOW DATABASES"

clickhouse-client --query "ATTACH DATABASE datasets ENGINE = Ordinary"
#service clickhouse-server restart

# Wait for server to start accepting connections
for _ in {1..120}; do
    clickhouse-client --query "SELECT 1" && break
    sleep 1
done

clickhouse-client --query "SHOW TABLES FROM datasets"

if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    clickhouse-client --query "CREATE DATABASE test ON CLUSTER 'test_cluster_database_replicated'
        ENGINE=Replicated('/test/clickhouse/db/test', '{shard}', '{replica}')"

    clickhouse-client --query "CREATE TABLE test.hits AS datasets.hits_v1"
    clickhouse-client --query "CREATE TABLE test.visits AS datasets.visits_v1"

    clickhouse-client --query "INSERT INTO test.hits SELECT * FROM datasets.hits_v1"
    clickhouse-client --query "INSERT INTO test.visits SELECT * FROM datasets.visits_v1"

    clickhouse-client --query "DROP TABLE datasets.hits_v1"
    clickhouse-client --query "DROP TABLE datasets.visits_v1"

    MAX_RUN_TIME=$((MAX_RUN_TIME < 9000 ? MAX_RUN_TIME : 9000))  # min(MAX_RUN_TIME, 2.5 hours)
    MAX_RUN_TIME=$((MAX_RUN_TIME != 0 ? MAX_RUN_TIME : 9000))    # set to 2.5 hours if 0 (unlimited)
else
    clickhouse-client --query "CREATE DATABASE test"
    clickhouse-client --query "SHOW TABLES FROM test"
    clickhouse-client --query "RENAME TABLE datasets.hits_v1 TO test.hits"
    clickhouse-client --query "RENAME TABLE datasets.visits_v1 TO test.visits"
fi

clickhouse-client --query "SHOW TABLES FROM test"
clickhouse-client --query "SELECT count() FROM test.hits"
clickhouse-client --query "SELECT count() FROM test.visits"

function run_tests()
{
    set -x
    # We can have several additional options so we path them as array because it's
    # more idiologically correct.
    read -ra ADDITIONAL_OPTIONS <<< "${ADDITIONAL_OPTIONS:-}"

    if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
        ADDITIONAL_OPTIONS+=('--replicated-database')
    fi

    clickhouse-test --shard --zookeeper --hung-check --use-skip-list --run stateful --print-time "${ADDITIONAL_OPTIONS[@]}" \
        "$SKIP_TESTS_OPTION" 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee test_output/test_result.txt || true
}

export -f run_tests
timeout "$MAX_RUN_TIME" bash -c run_tests ||:

./process_functional_tests_result.py || echo -e "failure\tCannot parse results" > /test_output/check_status.tsv

grep -Fa "Fatal" /var/log/clickhouse-server/clickhouse-server.log ||:
pigz < /var/log/clickhouse-server/clickhouse-server.log > /test_output/clickhouse-server.log.gz ||:
mv /var/log/clickhouse-server/stderr.log /test_output/ ||:

#To print ASAN/TSAN LOG in the console
sanitizer_type="${sanitizer_type:='ASAN/TSAN'}"
if [[ -n $(find /var/log/clickhouse-server -name "*${sanitizer_type}.log*") ]];
then
    mkdir -p /test_output/${sanitizer_type}_log
    echo "${sanitizer_type^^} Logs are printed for analysis."
    if [[ -n $(find /var/log/clickhouse-server -name "${sanitizer_type}_report") ]]; then
      cat /var/log/clickhouse-server/${sanitizer_type}_report
      mv /var/log/clickhouse-server/${sanitizer_type}_report /test_output/${sanitizer_type}_log/
    else
      echo "No ${sanitizer_type^^} report exists"
    fi
    echo "Uploading ${sanitizer_type^^} log to Artifacts"
    mv /var/log/clickhouse-server/$sanitizer_type.log* /test_output/${sanitizer_type}_log/
    cp -r /test_output/${sanitizer_type}_log/ /sanitizer_log_output/
else
    echo "No ${sanitizer_type^^} logs exists"
fi

if [[ -n "$WITH_COVERAGE" ]] && [[ "$WITH_COVERAGE" -eq 1 ]]; then
    tar -chf /test_output/clickhouse_coverage.tar.gz /profraw ||:
fi
if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    grep -Fa "Fatal" /var/log/clickhouse-server/clickhouse-server1.log ||:
    grep -Fa "Fatal" /var/log/clickhouse-server/clickhouse-server2.log ||:
    pigz < /var/log/clickhouse-server/clickhouse-server1.log > /test_output/clickhouse-server1.log.gz ||:
    pigz < /var/log/clickhouse-server/clickhouse-server2.log > /test_output/clickhouse-server2.log.gz ||:
    mv /var/log/clickhouse-server/stderr1.log /test_output/ ||:
    mv /var/log/clickhouse-server/stderr2.log /test_output/ ||:
fi
