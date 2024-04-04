#!/bin/bash

REMOTE=$(git remote | grep byconity || echo "origin")

if [ -z "$2" ]; then
    BRANCH=$(git branch --show-current)
else
    BRANCH=$2
fi

if [ -z "$1" ]; then
    TARGET_BRANCH=master
else
    TARGET_BRANCH=$2
fi

ABBREVIATED_TOP_COMMIT_HASH=$(git log refs/heads/${BRANCH}^! --format=%h)
COMMITS=$(git log --no-merges --format=%H --extended-regexp --grep='^(feat|fix|build|ci|docs|perf|refactor|style|test)(\((clickhousech|optimizer)+@m-[1-9][0-9]+\))*:+(.+)$' $BRANCH~ ^${TARGET_BRANCH})
index=1
for COMMIT in $COMMITS;
do
    echo "create pr for commit $COMMIT"
    SUB_BRANCH=${BRANCH}_${ABBREVIATED_TOP_COMMIT_HASH}_${index}
    git co -b ${SUB_BRANCH} $COMMIT
    ((index++)) # increase the index
    git push -u -f ${REMOTE} ${SUB_BRANCH}
    RAW_TITLE=$(git log refs/heads/$SUB_BRANCH^! | tail -n +4 | grep -v "^ *Merge .* into " |sed '/^[[:space:]]*$/d' | grep -v "^ *See merge request"| grep -v "#")
    TITLE=$(echo $RAW_TITLE| sed -E -e 's/^(feat|fix|build|ci|docs|perf|refactor|style|test)(\((clickhousech|optimizer|byconity)+@m-[1-9][0-9]+\))*:+(.+)$/\1:\4/')

    echo "create pr with title: ${TITLE}"
    gh pr create --base ${TARGET_BRANCH} --head $SUB_BRANCH --title "$TITLE" --body "$TITLE" && ALL_SUB_BRANCHES="${ALL_SUB_BRANCHES} ${SUB_BRANCH}"
done
git co $BRANCH
echo $ALL_SUB_BRANCHES | xargs git br -D


