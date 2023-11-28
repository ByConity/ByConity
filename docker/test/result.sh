#!/bin/bash

set -e

#TestSuite Result Analysis
success_count=$(cut -f1 /Artifacts/check_status.tsv | awk '/success/{++count} END {print count}')
total_count=$(wc -l < /Artifacts/check_status.tsv | awk '{print $1}')

if [[ success_count -eq total_count ]]
then
    echo "all ${total_count} test suites pass!"
    exit 0
else
    cat /Artifacts/check_status.tsv
    exit 1
fi
