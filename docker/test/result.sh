#!/bin/bash

set -e

#TestSuite Result Analysis
if [[ $(cut -f1 /Artifacts/check_status.tsv) == 'success' ]]
then
    echo "::add-message level=info::all cases pass!"
    exit 0
else
    echo "::add-message level=error::$(cat /Artifacts/check_status.tsv)"
    exit 1
fi
