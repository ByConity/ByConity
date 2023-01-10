#!/bin/bash

echo "$@"
echo "$1" "$2" "$3"
exec /root/app/usr/bin/clickhouse $@ 
