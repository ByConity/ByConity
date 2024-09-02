#!/bin/bash
cd "${0%/*}"
docker-compose -f ../docker-compose.essentials.yml -f ../docker-compose.simple.yml exec server-0 clickhouse-client --port 52145 

