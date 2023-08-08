#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e
set -o pipefail

${CLICKHOUSE_CLIENT_BINARY} --help </dev/null | wc -L
script -e -q -c "${CLICKHOUSE_CLIENT_BINARY} --help" /dev/null </dev/null >/dev/null
