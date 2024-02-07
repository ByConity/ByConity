#!/bin/bash

REMOTE=$(git remote | grep byconity || echo "origin")

if [ -z "$1" ]; then
    BRANCH=$(git branch --show-current)
else
    BRANCH=$1
fi

ABBREVIATED_TOP_COMMIT_HASH=$(git log refs/heads/${BRANCH}^! --format=%h)
COMMITS=$(git log --no-merges --format=%H --extended-regexp --grep='^(feat|fix|build|ci|docs|perf|refactor|style|test)(\((clickhousech|optimizer)+@m-[1-9][0-9]+\)):+(.+)$' $BRANCH~ ^master)
index=1

for COMMIT in $COMMITS;
do
    echo "create pr for commit $COMMIT"
    SUB_BRANCH=${BRANCH}_${ABBREVIATED_TOP_COMMIT_HASH}_${index}
    git co -b ${SUB_BRANCH} $COMMIT
    ((index++)) # increase the index
    git push -u -f ${REMOTE} ${SUB_BRANCH}
    RAW_TITLE=$(git log refs/heads/$SUB_BRANCH^! | tail -n +4 | grep -v "Merge branch" |sed '/^[[:space:]]*$/d' | grep -v "See merge request dp"| grep -v "#")
    TITLE=$(echo $RAW_TITLE| sed -E -e 's/^(feat|fix|build|ci|docs|perf|refactor|style|test)(\((clickhousech|optimizer)+@m-[1-9][0-9]+\)):+(.+)$/\1:\4/')

    echo "create pr with title: ${TITLE}"
    gh pr create --base master --head $SUB_BRANCH --title "$TITLE" --body "$TITLE" && git br -D $SUB_BRANCH
done


