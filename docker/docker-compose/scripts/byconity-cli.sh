#!/bin/bash
cd "${0%/*}"
docker-compose -f ../docker-compose.yml exec server-0 clickhouse client --port 52145