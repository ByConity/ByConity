#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A question for curious reader:
# How to break shell pipeline as soon as 5 lines are found in the following command:
# ./my-program | head -n5
# When I tried to do it, pipeline was not actively terminated,
#  unless the my-program will try to output a thousand more lines overflowing pipe buffer and terminating with Broken Pipe.
# But if my program just output 5 (or slightly more) lines and hang up, the pipeline is not terminated.

function test()
{
    # set enable_distinct_to_aggregate=0 due to DISTINCT is blocking in optimizer
    timeout 5 ${CLICKHOUSE_LOCAL} --enable_distinct_to_aggregate=0 --max_execution_time 10 --query "SELECT DISTINCT number % 5 FROM system.numbers" ||:
    echo '---'
    timeout 5 ${CLICKHOUSE_CURL} -sS --no-buffer "${CLICKHOUSE_URL}&max_execution_time=10&enable_distinct_to_aggregate=0" --data-binary "SELECT DISTINCT number % 5 FROM system.numbers" ||:
    echo '---'
}

# The test depends on timeouts. And there is a chance that under high system load the query
# will not be able to finish in 5 seconds (this will lead to test flakiness).
# Let's check that is will be able to show the expected result at least once.

# limit retry to 5 times
for i in {1..5}; do
    output=$(test)
    [[ "$output" == $(echo -ne "0\n1\n2\n3\n4\n---\n0\n1\n2\n3\n4\n---\n") ]] && break
    sleep 1;
done
# ensure the result is correct
echo "$output"
