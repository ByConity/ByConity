#!/bin/bash
source ./proc_title.sh

#if there are github keywords like Closes #1217, it should be included
#if there are merge request/pull request from internal branches, they should be masked/ignored
#if there are references to internal branches, they should be masked/ignored
#if there are links pointing to internal urls, they should be ignored
 
inputs=("
    Merge branch 'dao_fix_remote_function' into 'cnch-dev'
    feat(byconity@m-3010023570): support using the remote function to query ClickHouse Closes #1217
    See merge request dp/ClickHouse!19813" 
    
    "
    Date:   Fri Mar 15 13:21:36 2024 +0800
    Merge pull request #1371 from ByConity/cherry-pick1
    feat: make the number of fuzzy names configurable" 

    "
    fix(clickhousech@m-3001163519): Merge 'las-hive' into 'cnch-dev'
fix(clickhousech@m-3001163519): fix reading partition col without row count
See merge request: !20115 (merged)" 

    "
    Merge 'youzhiyuan_make_empty_timezone_compatible' into 'cnch-dev'
    feat(clickhousech@m-3018479788): remove empty timezone check
    See merge request: https://code.byted.org/dp/ClickHouse/merge_requests/20156")

expected_output=(
	"feat: support using the remote function to query ClickHouse Closes #1217" 
	"feat: make the number of fuzzy names configurable"
	"fix: fix reading partition col without row count"
	"feat: remove empty timezone check"
)


for ((i=0; i<${#inputs[@]}; i++)); do
     out=$(process_header "${inputs[i]}");
	if [ "$out" != "${expected_output[i]}" ]; then
    	    echo "Case $i failed, expected=[${expected_output[i]}], get=[$out]"
	else
	    echo "Case $i passed."
	fi
done

