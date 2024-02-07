#!/bin/bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Remove any predefined dialect type
CLICKHOUSE_CLIENT=$(echo "${CLICKHOUSE_CLIENT}" | sed 's/'"--dialect_type=[[:alpha:]]\+"'//g')
CLICKHOUSE_CLIENT_MYSQL="$CLICKHOUSE_CLIENT --dialect_type=MYSQL --enable_implicit_arg_type_convert=1"

declare -a ordered_keys=(
    "BIT_LENGTH"
    "CHARACTER_LENGTH"
    "CONCAT"
    "CONCAT_WS"
    "FIELD"
    "FIND_IN_SET"
    "FROM_BASE64"
    "FROM_UTF8"
    "LCASE"
    "LENGTH"
    "LOWER"
    "LTRIM"
    "OCT"
    "REPLACE"
    "REVERSE"
    "TRIM"
    "TO_BASE64"
    "TO_UTF8"
    "UCASE"
    "UPPER"
    "RTRIM"
    "SPLIT"
    "STRCMP"
)

declare -A function_args=(
  ["BIT_LENGTH"]=1
  ["CHARACTER_LENGTH"]=1
  ["CONCAT"]=2
  ["CONCAT_WS"]=2
  ["FIELD"]=2
  ["FIND_IN_SET"]=2
  ["FROM_BASE64"]=1
  ["FROM_UTF8"]=1
  ["LCASE"]=1
  ["LENGTH"]=1
  ["LOWER"]=1
  ["LTRIM"]=1
  ["OCT"]=1
  ["REPLACE"]=3
  ["REVERSE"]=1
  ["TRIM"]=1
  ["TO_BASE64"]=1
  ["TO_UTF8"]=1
  ["UCASE"]=1
  ["UPPER"]=1
  ["RTRIM"]=1
  ["SPLIT"]=2
  ["STRCMP"]=2
)

declare -a values=(
  null
  toNullable\(1\)
  5
  \'2023\'
)

function run_test_case_mysql() {
  local sql=$1 
  echo "$sql"
  ${CLICKHOUSE_CLIENT_MYSQL} --query="$sql"  
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
  elif [[ $args -eq 3 ]]; then
    for ((i = 0; i < ${#values[@]}; i++)); do
      for ((j = i + 1; j < ${#values[@]}; j++)); do
        for ((k = j + 1; k < ${#values[@]}; k++)); do
          run_test_case_mysql "select ${function_name}(${values[$i]}, ${values[$j]}, ${values[$k]})"
        done
      done
    done
  fi
done
