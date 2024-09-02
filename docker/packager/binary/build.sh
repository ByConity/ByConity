#!/usr/bin/env bash
set -x -e

exec &> >(ts)

export CCACHE_DIR=/ccache

ccache_status () {
    ccache --show-config ||:
    ccache --show-stats ||:
}

[ -O /build ] || git config --global --add safe.directory /build

# export CCACHE_LOGFILE=/build/ccache.log
# export CCACHE_DEBUG=1


mkdir -p /build/build_docker
cd /build/build_docker
rm -f CMakeCache.txt
# Read cmake arguments into array (possibly empty)
read -ra CMAKE_FLAGS <<< "${CMAKE_FLAGS:-}"
env

if [ -n "$MAKE_DEB" ]; then
  rm -rf /build/packages/root
fi


ccache_status
# clear cache stats
ccache --zero-stats ||:

# Build everything
cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA "-DCMAKE_BUILD_TYPE=$BUILD_TYPE" -DENABLE_CHECK_HEAVY_BUILDS=0 -DENABLE_BREAKPAD=ON "${CMAKE_FLAGS[@]}" ..

# No quotes because I want it to expand to nothing if empty.
# shellcheck disable=SC2086 # No quotes because I want it to expand to nothing if empty.
$SCAN_WRAPPER ninja $NINJA_FLAGS

ls -la ./programs

ccache_status

if [ -n "$MAKE_DEB" ]; then
  # No quotes because I want it to expand to nothing if empty.
  # shellcheck disable=SC2086
  DESTDIR=/build/packages/root ninja $NINJA_FLAGS install
  bash -x /build/packages/build
fi

DESTDIR=/output ninja $NINJA_FLAGS install
# Folders structure looks like this:
# /output
#   etc/ (Needed by ByConity docker image)
#   usr/
#     bin/ (Needed by ByConity docker image)
#     cmake/ (same as above)
#     lib/ (...)
#     share/ (...)
#     usr/ (same as above)
#   byconity-*** (Needed by GitHub Releases)
#   unit_tests_dbms
[ -x ./programs/self-extracting/clickhouse ] && mv ./programs/self-extracting/clickhouse /output
mv ./src/unit_tests_dbms /output ||: # may not exist for some binary builds



ccache_status
ccache --evict-older-than 1d

if [ "${CCACHE_DEBUG:-}" == "1" ]
then
    find . -name '*.ccache-*' -print0 \
        | tar -c -I pixz -f /output/ccache-debug.txz --null -T -
fi

if [ -n "$CCACHE_LOGFILE" ]
then
    # Compress the log as well, or else the CI will try to compress all log
    # files in place, and will fail because this directory is not writable.
    tar -cv -I pixz -f /output/ccache.log.txz "$CCACHE_LOGFILE"
fi

ls -l /output
