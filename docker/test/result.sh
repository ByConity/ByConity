#!/bin/bash

set -e

#TestSuite Result Analysis
success_count=$(cut -f1 /Artifacts/check_status.tsv | awk '/success/{++count}')
if [[ success_count -eq 2 ]]
then
    echo "::add-message level=info::all cases pass!"
    exit 0
else
    echo -e "::add-message level=error::\n$(cat /Artifacts/check_status.tsv)"
    exit 1
fi
