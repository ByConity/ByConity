#!/bin/bash
set -e

if [ -n "$CUSTOM_SANITIZE" ] ; then
    exit 0
fi

UTESTS="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )/build"
cd $UTESTS
if [ -d /test_output ]; then
    set -o pipefail
    src/unit_tests_dbms --output-on-failure |& tee /test_output/unittest.log 
else
    src/unit_tests_dbms --output-on-failure 
fi

