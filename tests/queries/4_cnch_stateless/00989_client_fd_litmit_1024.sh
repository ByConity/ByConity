#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

ulimit -n 1024

$CLICKHOUSE_CLIENT -q "SELECT 1;"
