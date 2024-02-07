#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

MYSQL=0
USER="root"

function run_test_case() 
{
  sql=$1 
  echo $sql
  if [ ${MYSQL} -eq 1 ]
  then
      mysql -u${USER} -N -B -e "${sql}"
  else
      ${CLICKHOUSE_CLIENT} --query="${sql} settings dialect_type='MYSQL', enable_implicit_arg_type_convert=1"
  fi
}


function test_agg()
{
  func=$1 
  insert_sql="insert into aggfunc.mysql_agg values (1, '1', '2023-12-12', '2023-12-12 11:11:11', '2023-12-12 12:12:12.123', '123456'), (1, '12.0sdf', '2023-12-13', '2023-12-13 11:11:11', '2023-12-13 12:12:12.123', '12345 '), (2, '2.0', '2023-12-12', '2023-12-12 11:11:11', '2023-12-12 12:12:12.123', ' 12345'), (2, '1.2e1df', '2023-12-13', '2023-12-13 11:11:11', '2023-12-13 12:12:12.123', ' 12.4 ')"

  if [ ${MYSQL} -eq 1 ]
  then
    mysql -u${USER}  -N -B -e "drop database if exists aggfunc; create database aggfunc";
    mysql -u${USER}  -N -B -D aggfunc -e "create table mysql_agg(a int, b text, c Date, d DateTime(0), e DateTime(3), f char(6))"
    mysql -u${USER}  -N -B -D aggfunc -e "${insert_sql}"
    mysql -u${USER}  -N -B -D aggfunc -e "select sum(a), avg(b), stddev(c), var_pop(d), var_samp(e), bit_and(b), bit_or(c), bit_xor(d), avg(f) from mysql_agg group by a order by a"
  else
    ${CLICKHOUSE_CLIENT} -mn --query="drop database if exists aggfunc; create database aggfunc; drop table if exists aggfunc.mysql_agg; create table aggfunc.mysql_agg(a int, b String, c Date, d DateTime, e DateTime64, f FixedString(6)) engine=CnchMergeTree() order by a"
    ${CLICKHOUSE_CLIENT} --query="${insert_sql}"
    ${CLICKHOUSE_CLIENT} --query="select sum(a), avg(b), stddevPopStable(c), varPopStable(d), varSampStable(e), groupBitAnd(b), groupBitOr(c), groupBitXor(d), avg(f) from aggfunc.mysql_agg group by a order by a settings dialect_type='MYSQL', enable_implicit_arg_type_convert=1"
  fi
}

test_agg 

function test_rounding() 
{
  func=$1
  for val in 5 -5 1234.5678 -1234.5678 5678.1234 -5678.1234 0 -0 1.2345e2 -1.2345e2 "'5'" "'-5'" "'1234.5678'" "'-1234.5678'" "'5678.1234'" "'-5678.1234'" "'0'" "'-0'" "'1.2345e2'" "'-1.2345e2'" "'0.34afds'" "'-1.8999.edf'" 
  do 
    if [ $# -eq 1 ]
    then
      run_test_case "select ${func}(${val})"
    else
      run_test_case "select ${func}(${val}, $2)"
    fi
  done
}

test_rounding ceil
test_rounding floor
test_rounding round
test_rounding round 2.0
test_rounding round -2
# truncate('1.2345e2', 2) -> 123.44 instead of 123.45 because CE parses '1.2345e2' 1.23499999
test_rounding truncate 2
test_rounding truncate -2

function test_hex() 
{
  func=$1
  if [ ${MYSQL} -eq 1 ]
  then
    vals=(5 -5 5.5 -5.5 "'5'" "'-5'" 0 -0 "'0'" "'-0'" "TIMESTAMP'2023-11-22 12:00:01'"  "TIMESTAMP'2023-11-22 12:00:01.001'" "'327f7dbd-8905-11ee-b77f-fad484e0e86b'")
  else
    vals=(5 -5 5.5 -5.5 "'5'" "'-5'" 0 -0 "'0'" "'-0'" "toDateTime('2023-11-22 12:00:01', 'Asia/Singapore')"  "toDateTime64('2023-11-22 12:00:01.001', 3, 'Asia/Singapore')" "'327f7dbd-8905-11ee-b77f-fad484e0e86b'")
  fi

  for val in "${vals[@]}"
  do 
    run_test_case "select ${func}(${val})"
  done
}

function test_bin() 
{
  if [ ${MYSQL} -eq 1 ]
  then
    vals=(5 -5 5.5 -5.5 "'5'" "'-5'" 0 -0 "'0'" "'-0'" "TIMESTAMP'2023-11-22 12:00:01'"  "TIMESTAMP'2023-11-22 12:00:01.001'" "'327f7dbd-8905-11ee-b77f-fad484e0e86b'")
  else
    vals=(5 -5 5.5 -5.5 "'5'" "'-5'" 0 -0 "'0'" "'-0'" "toDateTime('2023-11-22 12:00:01')" "toDateTime64('2023-11-22 12:00:01.001')" "'327f7dbd-8905-11ee-b77f-fad484e0e86b'")
  fi
 
  for val in "${vals[@]}"
  do 
    run_test_case "select ${func}(${val})"
  done
}

function test_unhex() 
{
  func=$1
  for val in 0 35 "'0'" "'35'" "'1f2a33'" "'f2a33'"
  do 
    run_test_case "select ${func}(${val})"
  done
}

test_hex hex
test_bin bin
test_unhex unhex

function test_unary_math()
{
  func=$1
  for val in 5 -5 5.5 -5.5 "'5'" "'-5'" "'5.5'" "'-5.5'" 0.123 -0.123 "'0.123'" #0 -0 "'0'" "'-0'"
  do 
    run_test_case "select ${func}(${val})"
  done
}

# function impelmented using FunctionUnaryArithmetic
test_unary_math 'abs'
test_unary_math 'sign'
# results differ to mysql for int8, int16, int32 and int64
if [ $MYSQL -eq 1 ]
then
  test_unary_math '-'
  test_unary_math '~'
else
  test_unary_math 'negate'
  test_unary_math 'bitNot'
fi


# function impelmented using FunctionMathUnary
# results differ in last few digits and null vs nan/inf for invalid args
test_unary_math degrees 
test_unary_math exp 
test_unary_math radians 
test_unary_math cos
test_unary_math sin
test_unary_math tan

test_unary_math log
test_unary_math log2
test_unary_math log10
test_unary_math acos 
test_unary_math asin
test_unary_math atan
test_unary_math sqrt
