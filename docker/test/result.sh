#!/bin/bash

set -e

#TestSuite Result Analysis
success_count=$(cut -f1 /Artifacts/check_status.tsv | awk '/success/{++count} END {print count}')
if [[ success_count -eq 3 ]]
then
    echo "::add-message level=info::all cases pass!"
    exit 0
else
    echo "::add-message level=error::$(cat /Artifacts/check_status.tsv)"
    sed -n '/\[ FAIL \]/,/\[ OK \]/p' /Artifacts/test_result.txt | head -n -1 | xargs -I {} echo ::add-message level=info::{}
    exit 1
fi
