#!/bin/bash

set -e

#TestSuite Result Analysis
success_count=$(cut -f1 /Artifacts/check_status.tsv | awk '/success/{++count} END {print count}')
if [[ success_count -eq 8 ]]
then
    echo "all cases pass!"
    exit 0
else
    cat /Artifacts/check_status.tsv
    sed -n '/\[ FAIL \]/,/\[ OK \]/p' /Artifacts/external_table_test_suite_test_result.txt | head -n -1 | xargs -I {} echo ::add-message level=info::{}
    sed -n '/\[ FAIL \]/,/\[ OK \]/p' /Artifacts/functional_stateless_wo_optimizer_test_suite_test_result.txt | head -n -1 | xargs -I {} echo ::add-message level=info::{}
    exit 1
fi
