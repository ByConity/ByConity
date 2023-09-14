#!/bin/bash
cd "${0%/*}"
docker-compose -f ../docker-compose.essentials.yml exec hdfs-namenode $1