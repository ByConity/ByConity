#!/bin/bash
source $(dirname "$0")/proc_title.sh

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


GIT_LOG=$(git log refs/heads/$BRANCH^! | tail -n +4)
TITLE=$(process_header "$GIT_LOG")

echo "create pr with title: ${TITLE} for branch ${BRANCH}"
gh pr create --base ${TARGET_BRANCH} --head $BRANCH --title "$TITLE" --body "$TITLE"

