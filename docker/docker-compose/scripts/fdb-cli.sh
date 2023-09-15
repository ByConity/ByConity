#!/bin/bash
cd "${0%/*}"
docker-compose -f ../docker-compose.essentials.yml exec tso-0 bash -c "FDB_CLUSTER_FILE=/config/fdb.cluster fdbcli"