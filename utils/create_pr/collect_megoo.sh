#!/bin/bash
BASE_COMMIT=$1
TOP_COMMIT=$2

#git log $1..$2 --pretty=medium 
git log $1..$2 --merges --pretty=medium --extended-regexp --grep='^(feat|fix|build|ci|docs|perf|refactor|style|test)\((clickhousech|optimizer)+@m-[1-9][0-9]+\):+(.+)$' | grep -E "^ *(feat|fix|build|ci|docs|perf|refactor|style|test)\((clickhousech|optimizer)+@m-[1-9][0-9]+\):+(.+)$"
