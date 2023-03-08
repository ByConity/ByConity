#!/bin/bash

function run_tso() {
    docker run -d --restart=on-failure \
    --mount type=bind,source="$(pwd)"/config,target=/root/app/config \
    --mount type=bind,source="$(pwd)"/logs,target=/root/app/logs \
    --expose 18689 \
    --network host \
    --name byconity-tso byconity/byconity-server:stable tso-server --config-file /root/app/config/tso.xml
}

function run_server() {
    docker run -d --restart=on-failure \
    --mount type=bind,source="$(pwd)"/config,target=/root/app/config \
    --mount type=bind,source="$(pwd)"/logs,target=/root/app/logs \
    --mount type=bind,source="$(pwd)"/data,target=/root/app/data \
    --expose 18684 \
    --expose 18685 \
    --expose 18686 \
    --expose 18687 \
    --expose 18688 \
    --network host \
    --name byconity-server byconity/byconity-server:stable server -C --config-file /root/app/config/server.xml 
}

function run_read_worker() {
    docker run -d --restart=on-failure \
    --mount type=bind,source="$(pwd)"/config,target=/root/app/config \
    --mount type=bind,source="$(pwd)"/logs,target=/root/app/logs \
    --mount type=bind,source="$(pwd)"/data,target=/root/app/data \
    --expose 18690 \
    --expose 18691 \
    --expose 18692 \
    --expose 18693 \
    --expose 18694 \
    --network host \
    --name byconity-read-worker byconity/byconity-server:stable server -C --config-file /root/app/config/worker.xml 
}

function run_write_worker() {
    docker run -d --restart=on-failure \
    --mount type=bind,source="$(pwd)"/config,target=/root/app/config \
    --mount type=bind,source="$(pwd)"/logs,target=/root/app/logs \
    --mount type=bind,source="$(pwd)"/data,target=/root/app/data \
    --expose 18696 \
    --expose 18697 \
    --expose 18698 \
    --expose 18699 \
    --expose 18700 \
    --network host \
    --name byconity-write-worker byconity/byconity-server:stable server -C --config-file /root/app/config/worker-write.xml 
}

function run_dm() {
    docker run -d --restart=on-failure \
    --mount type=bind,source="$(pwd)"/config,target=/root/app/config \
    --mount type=bind,source="$(pwd)"/logs,target=/root/app/logs \
    --mount type=bind,source="$(pwd)"/data,target=/root/app/data \
    --expose 18965 \
    --network host \
    --name byconity-dm byconity/byconity-server:stable daemon-manager --config-file /root/app/config/dm.xml 
}

function run_cli() {
    docker run -it\
    --network host \
    --name byconity-cli byconity/byconity-server:stable client --host 127.0.0.1 --port 18684
}

function run_cli2() {
    docker run -it\
    --network host \
    --rm byconity/byconity-server:stable client --host $1 --port 18684
}

function stop_byconity() {
    if [ "$1" = "tso" ]; then
        docker stop -t 30 byconity-tso
    elif [ "$1" = "server" ]; then
        docker stop -t 30 byconity-server
    elif [ "$1" = "read_worker" ]; then
        docker stop -t 30 byconity-read-worker
    elif [ "$1" = "write_worker" ]; then
        docker stop -t 30 byconity-write-worker
    elif [ "$1" = "dm" ]; then
        docker stop -t 30 byconity-dm
    else
        echo "valid argument stop tso, stop server, stop read_worker, stop write_worker, stop dm"
    fi
}

function start_byconity() {
    if [ "$1" = "tso" ]; then
        docker start byconity-tso
    elif [ "$1" = "server" ]; then
        docker start byconity-server
    elif [ "$1" = "read_worker" ]; then
        docker start byconity-read-worker
    elif [ "$1" = "write_worker" ]; then
        docker start byconity-write-worker
    elif [ "$1" = "dm" ]; then
        docker start byconity-dm
    elif [ "$1" = "cli" ]; then
        docker start -i byconity-cli
    else
        echo "valid argument start tso, start server, start read_worker, start write_worker, start dm, start cli"
    fi
}


if [ ! -f "config/fdb.cluster" ]; then
    echo "file config/fdb.cluster does not exist."
    exit 0
fi

if grep -q example.host.com "config/fdb.cluster"; then
    echo "file config/fdb.cluster haven't been configured properly."
    exit 0
fi

mkdir -p data/byconity_server/server_local_disk/data/0/
mkdir -p logs/

if [ "$1" = "tso" ]; then
    run_tso
elif [ "$1" = "server" ]; then
    run_server
elif [ "$1" = "read_worker" ]; then
    run_read_worker
elif [ "$1" = "write_worker" ]; then
    run_write_worker
elif [ "$1" = "dm" ]; then
    run_dm
elif [ "$1" = "cli" ]; then
    run_cli
elif [ "$1" = "cli2" ]; then
    run_cli2 $2
elif [ "$1" = "stop" ]; then
    stop_byconity $2
elif [ "$1" = "start" ]; then
    start_byconity $2
else
    echo "valid argument are tso, server, read_worker, write_worker, dm, cli, cli2, stop tso, stop server, stop read_worker, stop write_worker, stop dm, start tso, start server, start read_worker ..."
fi
