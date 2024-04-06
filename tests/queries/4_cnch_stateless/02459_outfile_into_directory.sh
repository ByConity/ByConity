#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: depends on bzip2

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_FOLDER_02459="${CLICKHOUSE_TMP}/outfile_to_directory"

rm -rf "${WORKING_FOLDER_02459}"
mkdir "${WORKING_FOLDER_02459}"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM numbers(500000) INTO OUTFILE '${WORKING_FOLDER_02459}/' FORMAT CSV"

LAST_FILE=`ls -1 "${WORKING_FOLDER_02459}" | tail -n 1`

# output last line of last file
tail -n 1 "${WORKING_FOLDER_02459}/${LAST_FILE}"

rm -rf "${WORKING_FOLDER_02459}"
