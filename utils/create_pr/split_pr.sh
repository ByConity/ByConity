#!/bin/bash

if [ -z "$1" ]; then
    BRANCH=$(git branch --show-current)
else
    BRANCH=$1
fi

COMMITS=$(git log --no-merges --format=%H --extended-regexp --grep='^(feat|fix|build|ci|docs|perf|refactor|style|test)(\((clickhousech|optimizer)+@m-[1-9][0-9]+\)):+(.+)$' cherry-pick ^master)
for COMMIT in $COMMITS:
do
    echo "create pr for commit $COMMIT"
    git co -b 
done

#RAW_TITLE=$(git log refs/heads/$BRANCH^! | tail -n +4 | grep -v "Merge branch" |sed '/^[[:space:]]*$/d' | grep -v "See merge request dp")
#TITLE=$(echo $RAW_TITLE| sed -E -e 's/^(feat|fix|build|ci|docs|perf|refactor|style|test)(\((clickhousech|optimizer)+@m-[1-9][0-9]+\)):+(.+)$/\1:\4/')
#
#echo "create pr with title: ${TITLE}"
#gh pr create --base master --head $BRANCH --title "$TITLE" --body "$TITLE"
