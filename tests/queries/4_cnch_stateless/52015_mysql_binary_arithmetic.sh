#!/bin/bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Remove any predefined dialect type
CLICKHOUSE_CLIENT=$(echo "${CLICKHOUSE_CLIENT}" | sed 's/'"--dialect_type=[[:alpha:]]\+"'//g')
CLICKHOUSE_CLIENT_MYSQL="$CLICKHOUSE_CLIENT --dialect_type=MYSQL --enable_implicit_arg_type_convert=1"

declare -a ordered_keys=("equals" "greater" "less" "bitAnd" "bitOr" "bitXor" "bitShiftLeft" "divide" "plus")

declare -A function_args=(
  ["equals"]=2
  ["greater"]=2
  ["less"]=2
  ["bitAnd"]=2
  ["bitOr"]=2
  ["bitXor"]=2
  ["bitShiftLeft"]=1
  ["divide"]=2
  ["plus"]=2
)

declare -a values=(
  null
  toNullable\(1\)
  \'123.456abc\' \'\ 123abc\ \'
  DateTime64\'2023-12-31\ 01:02:03.123\'
)

function run_test_case_mysql() {
  local sql=$1 
  echo "$sql"
  ${CLICKHOUSE_CLIENT_MYSQL} --query="$sql"  
}

for function_name in "${ordered_keys[@]}"; do
  args=${function_args[$function_name]}
  if [[ $args -eq 2 ]]; then
    for ((i = 0; i < ${#values[@]}; i++)); do
      for ((j = i + 1; j < ${#values[@]}; j++)); do
        run_test_case_mysql "select ${function_name}(${values[$i]}, ${values[$j]})"
      done
    done
  fi
done
