#!/bin/bash

function process_header {
	GIT_LOG=$1;
	RAW_TITLE=$(echo -e "$GIT_LOG" | grep -v "Merge branch" |sed '/^[[:space:]]*$/d' | grep -v "See merge request dp" | grep -v "request #" | grep -v "merge request" | grep -v "cnch-dev" | grep -v "Date:" | grep -v "code.byted.org"| grep -v "#");
	TITLE=$(echo $RAW_TITLE| sed -E -e 's/^(feat|fix|build|ci|docs|perf|refactor|style|test)(\((clickhousech|optimizer|byconity)+@m-[1-9][0-9]+\))*:+(.+)$/\1:\4/');
	echo $TITLE;
}
