#!/bin/bash

cd /CI/

last_line=$(tail -1 ./test_output/test_mode_2_log)

if [[ -n $(echo "$last_line" | grep "failed") ]]; then
    sed -n '/short test summary info/,$p' ./test_output/test_mode_2_log | while IFS= read -r line; 
    do
        echo "::add-message level=error::$(echo "$line" | sed 's/\x1b\[[0-9;]*m//g')";
    done
    exit 1;
else
    sed -n '/slowest 30 durations/,$p' ./test_output/test_mode_2_log | while IFS= read -r line; 
    do
        echo "::add-message level=info::$(echo "$line" | sed 's/\x1b\[[0-9;]*m//g')";
    done
    exit 0;
fi
