#!/bin/bash

set -e
set -x
PROJECT="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P  )"

export PATH=`echo $PATH | sed -e 's/:\/opt\/tiger\/typhoon-blade//'`

python3 ./utils/bytedance-versions/check_scm_version.py ./utils/bytedance-versions/CDW.VERSION

auto_git_hash=`git rev-parse HEAD`
sed -i -- "s/VERSION_GITHASH .*)/VERSION_GITHASH $auto_git_hash)/g;" cmake/version.cmake

if [ -n "$BUILD_VERSION" ]; then
    PRODUCT_NAME=`cat utils/bytedance-versions/PRODUCT_NAME`
    SERVER_VERSION=`cat utils/bytedance-versions/CDW.VERSION`
    KERNEL_VERSION=`uname -v`

    sed -i -- "s/VERSION_SCM .*)/VERSION_SCM $PRODUCT_NAME-$SERVER_VERSION\/$BUILD_VERSION)/g;" cmake/version.cmake
    sed -i -- "s/KERNEL_VERSION .*/KERNEL_VERSION \"$KERNEL_VERSION\")/g;" cmake/version.cmake
    # GENERATE tag
    curl -I -u wujian.1415:d47698a25104dbb0fd6a98888b5e2c9e "https://old-ci.byted.org/job/ch_debian_tag/buildWithParameters?token=WvgP5dE5KieAMqtcubn2&BUILD_VERSION=${BUILD_VERSION}&BUILD_BASE_COMMIT_HASH=${BUILD_BASE_COMMIT_HASH}"
fi

rm -rf output/
mkdir -p output

git config http.postBuffer 524288000
git submodule sync
git config --global http.sslVerify "false"
http_proxy=http://sys-proxy-rd-relay.byted.org:8118 https_proxy=http://sys-proxy-rd-relay.byted.org:8118 no_proxy=.byted.org git submodule update --init --recursive

#export CMAKE_BUILD_TYPE=${CUSTOM_CMAKE_BUILD_TYPE:-RelWithDebInfo}
#export CMAKE_FLAGS="-DCMAKE_INSTALL_PREFIX=../output -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DUSE_BYTEDANCE_RDKAFKA=${CUSTOM_USE_BYTEDANCE_RDKAFKA:-1} ${CMAKE_FLAGS}"
CMAKE_FLAGS="-DCMAKE_INSTALL_PREFIX=../output ${CMAKE_FLAGS}"
CMAKE_FLAGS="-DUSE_BYTEDANCE_RDKAFKA=${CUSTOM_USE_BYTEDANCE_RDKAFKA:-1} ${CMAKE_FLAGS}"
CMAKE_FLAGS="-DCMAKE_BUILD_TYPE=${CUSTOM_CMAKE_BUILD_TYPE:-RelWithDebInfo} $CMAKE_FLAGS"
CMAKE_FLAGS="-DENABLE_BREAKPAD=ON $CMAKE_FLAGS" # enable minidump
[[ -n "$CUSTOM_SANITIZE" ]] && CMAKE_FLAGS="-DSANITIZE=$CUSTOM_SANITIZE $CMAKE_FLAGS"
[[ -n "$CUSTOM_MAX_LINKING_JOBS" ]] && CMAKE_FLAGS="-DPARALLEL_LINK_JOBS=${CUSTOM_MAX_LINKING_JOBS} ${CMAKE_FLAGS}"
[[ -n "$CUSTOM_MAX_COMPILE_JOBS" ]] && CMAKE_FLAGS="-DPARALLEL_COMPILE_JOBS=${CUSTOM_MAX_COMPILE_JOBS} ${CMAKE_FLAGS}"
export CMAKE_FLAGS


rm -rf build && mkdir build && cd build

source /etc/os-release
if [ "$NAME" == "CentOS Linux" ] && [ "$VERSION_ID" == "7" ] && hash scl 2>/dev/null; then
    echo "Found Centos 7 and scl"
    scl enable devtoolset-9 "CC=clang CXX=clang++ cmake3 ${CMAKE_FLAGS} -DCMAKE_MAKE_PROGRAM:FILEPATH=/usr/bin/ninja ../"
    scl enable devtoolset-9 "ninja"
    scl enable devtoolset-9 "ninja install"
else
    cmake ../ ${CMAKE_FLAGS} && ninja
    ninja install
fi

# copy shared libaries
cp ${PROJECT}/contrib/foundationdb/lib/libfdb_c.so ../output/lib

# create the `usr/bin` directory to keep it same with old version
mkdir -p ../output/usr
mv ../output/bin ../output/usr/

# create symlink to make CI tests happy
cd ../output
ln -s usr/bin bin

