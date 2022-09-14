#!/bin/bash

# fail on errors, verbose and export all env variables
set -e -x -a

function run_tests()
{
    set -x
    # We can have several additional options so we path them as array because it's
    # more idiologically correct.
    read -ra ADDITIONAL_OPTIONS <<< "${ADDITIONAL_OPTIONS:-}"
    ps -aux
    clickhouse-test --print-time --use-skip-list --order asc --test-runs 1 -q /home/code/tests/queries "${ADDITIONAL_OPTIONS[@]}" 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/test_result.txt || true
}

export -f run_tests

timeout "$MAX_RUN_TIME" bash -c run_tests ||:

./process_functional_tests_result.py || echo -e "failure\tCannot parse results" > /test_output/check_status.tsv
