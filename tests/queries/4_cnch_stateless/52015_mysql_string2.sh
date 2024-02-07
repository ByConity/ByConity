#!/bin/bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Remove any predefined dialect type
CLICKHOUSE_CLIENT=$(echo "${CLICKHOUSE_CLIENT}" | sed 's/'"--dialect_type=[[:alpha:]]\+"'//g')
CLICKHOUSE_CLIENT_MYSQL="$CLICKHOUSE_CLIENT --dialect_type=MYSQL --enable_implicit_arg_type_convert=1"

declare -a ordered_keys=(
    "ASCII"
    "CHAR"
    "ELT"
    "FORMAT"
    "INSERT"
    "INSTR"
    "LOCATE"
    "LPAD"
    "MID"
    "MAKE_SET"
    "ORD"
    "POSITION"
    "REPEAT"
    "REGEXP"
    "space"
    "SPLIT_PART"
    "SUBSTR"
    "SUBSTRING_INDEX"
)

declare -A function_args=(
  ["ASCII"]=1
  ["CHAR"]=1
  ["ELT"]=2
  ["FORMAT"]=2
  ["INSERT"]=4
  ["INSTR"]=2
  ["LOCATE"]=2
  ["LPAD"]=3
  ["MID"]=2
  ["MAKE_SET"]=2
  ["ORD"]=1
  ["POSITION"]=2
  ["REPEAT"]=2
  ["REGEXP"]=2
  ["space"]=1
  ["SPLIT_PART"]=3
  ["SUBSTR"]=2
  ["SUBSTRING_INDEX"]=3
)

declare -a values=(
  null
  \'123abc\' \'1\'
  \'\ 1\ \'
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
  elif [[ $args -eq 4 ]]; then
    for ((i = 0; i < ${#values[@]}; i++)); do
      for ((j = i + 1; j < ${#values[@]}; j++)); do
        for ((k = j + 1; k < ${#values[@]}; k++)); do
            for ((m = k + 1; m < ${#values[@]}; m++)); do
                run_test_case_mysql "select ${function_name}(${values[$i]}, ${values[$j]}, ${values[$k]}, ${values[$m]})"
            done
        done
      done
    done
  fi
done
