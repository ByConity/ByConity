#!/bin/bash

if [ -z "$1" ]; then
    BRANCH="HEAD"
else
    BRANCH=$1
fi
RAW_TITLE=$(git log refs/heads/$BRANCH^! | tail -n +4 | grep -v "Merge branch" |sed '/^[[:space:]]*$/d' | grep -v "See merge request dp")
TITLE=$(echo $RAW_TITLE| sed -E -e 's/^(feat|fix|build|ci|docs|perf|refactor|style|test)(\((clickhousech|optimizer)+@m-[1-9][0-9]+\)):+(.+)$/\1:\4/')

echo "create pr with title: ${TITLE}"
gh pr create --base master --head $1 --title "$TITLE" --body "$TITLE"
