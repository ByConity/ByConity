#!/bin/bash

set -e

#TestSuite Result Analysis
fail_count=$(cut -f1 hi.tsv | awk '/failed/{++count}')
if [[ fail_count -eq 0 ]]
then
    echo "::add-message level=info::all cases pass!"
    exit 0
else
    echo "::add-message level=error::$(cat /Artifacts/check_status.tsv)"
    exit 1
fi
