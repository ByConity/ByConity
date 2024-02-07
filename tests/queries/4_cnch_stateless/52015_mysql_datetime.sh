#!/bin/bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

declare -a ordered_keys=("SUBDATE" "DATEDIFF" "DAY" "DAYOFWEEK" "DAYOFYEAR" "extract" "FROM_DAYS" "HOUR" "LAST_DAY" "MINUTE" "MONTH" "QUARTER" "SECOND"
                         "TO_DAYS" "WEEK" "WEEKDAY" "WEEKOFYEAR" "YEARWEEK" "ADDDATE" "DAYNAME" "YEAR" "MONTHNAME" "unix_timestamp" "DATE")

declare -A function_args=(
  ["SUBDATE"]=2
  ["DATEDIFF"]=2
  ["DAY"]=1
  ["DAYOFWEEK"]=1
  ["DAYOFYEAR"]=1
  ["extract"]=2
  ["FROM_DAYS"]=1
  ["HOUR"]=1
  ["LAST_DAY"]=1
  ["MINUTE"]=1
  ["MONTH"]=1
  ["QUARTER"]=1
  ["SECOND"]=1
  ["TO_DAYS"]=1
  ["WEEK"]=1
  ["WEEKDAY"]=1
  ["WEEKOFYEAR"]=1
  ["YEARWEEK"]=1
  ["ADDDATE"]=2
  ["DAYNAME"]=1
  ["YEAR"]=1
  ["MONTHNAME"]=1
  ["unix_timestamp"]=1
  ["DATE"]=1
)

declare -a values=(
  20231221 \'2023-12-21\ 01:02:03\'
  20231221010203
)

function run_test_case_mysql() {
  local sql=$1 
  echo "$sql"
  ${CLICKHOUSE_CLIENT} --query="${sql} settings dialect_type='MYSQL', enable_implicit_arg_type_convert=1" 
}

for function_name in "${ordered_keys[@]}"; do
  args=${function_args[$function_name]}
  if [[ $args -eq 1 ]]; then
    for val in "${values[@]}"; do
      run_test_case_mysql "select ${function_name}(${val})"
    done
  elif [[ $args -eq 2 ]]; then
    for ((i = 0; i < ${#values[@]}; i++)); do
      for ((j = i + 1; j < ${#values[@]}; j++)); do
        run_test_case_mysql "select ${function_name}(${values[$i]}, ${values[$j]})"
      done
    done
  fi
done

# special testcase for add/subtime
for val in 123 123.456 123456.1 "'2023-12-21'"
  do 
    run_test_case_mysql "select addtime(20231221, ${val})"
done

# special testcase for date_format
for val in 20231221 20231221010203.456 "'2023-12-21'"
  do 
    run_test_case_mysql "select DATE_FORMAT_MYSQL(${val}, '%Y-%m-%d')"
done

# special testcase for time_format
for val in 1234 123.456 "'01:02:03.456'" "'2012-12-21 01:02:03.456'"
  do 
    run_test_case_mysql "select TIME_FORMAT(${val}, '%H:%i:%S')"
done
