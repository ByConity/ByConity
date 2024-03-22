#!/bin/bash
source $(dirname "$0")/proc_title.sh

if [ -z "$1" ]; then
    BRANCH=$(git branch --show-current)
else
    BRANCH=$1
fi

GIT_LOG=$(git log refs/heads/$BRANCH^! | tail -n +4)
TITLE=$(process_header "$GIT_LOG")

echo "create pr with title: ${TITLE} for branch ${BRANCH}"
gh pr create --base master --head $BRANCH --title "$TITLE" --body "$TITLE"

