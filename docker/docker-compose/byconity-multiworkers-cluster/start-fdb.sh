#! /bin/bash
set -eu

# Attempt to connect. Configure the database if necessary.
FDB_CLUSTER_FILE=/config/fdb.cluster

if ! fdbcli -C $FDB_CLUSTER_FILE --exec status --timeout 5 ; then
    if ! fdbcli -C $FDB_CLUSTER_FILE --exec "configure new single ssd ; status" --timeout 10 ; then
        echo "Unable to configure new FDB cluster."
        exit 1
    fi
fi

echo "Can now connect to docker-based FDB cluster using $FDB_CLUSTER_FILE."
