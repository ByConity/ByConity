#!/bin/bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

file_name=`basename $0 .sh`
input_file="$CURDIR/$file_name.txt"

function cat_by_step() {
    line_start=`grep -n "$1" $input_file | head -1 | cut -d ":" -f 1`
    line_end=`grep -n "end $1" $input_file | head -1 | cut -d ":" -f 1`
    sed -n "$(($line_start + 1)), $(($line_end - 1))p" $input_file
}

function execute() {
    cat_by_step "$1" | $CLICKHOUSE_CLIENT --multiquery
    sleep 10 # wait for execute mutation.
}

function execute_with_check() {
    total_mutation=`$CLICKHOUSE_CLIENT --query "select count() from system.mutations where table in ('alter_rename', 'alter_map', 'alter_bitmap') and database = 'test'"`
    unfinished_mutation=`$CLICKHOUSE_CLIENT --query "select countIf(is_done=0) from system.mutations where table in ('alter_rename', 'alter_map', 'alter_bitmap') and database = 'test'"`

    if [ $total_mutation -eq 4 -a $unfinished_mutation -eq 0 ]; then
        cat_by_step "$1" | $CLICKHOUSE_CLIENT --multiquery
    else
        cat_by_step "reference $1"
    fi
}

execute "step 1"
execute_with_check "step 2"
execute "step 3"
