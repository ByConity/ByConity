#!/bin/bash

set -e
set -x

export PATH=`echo $PATH | sed -e 's/:\/opt\/tiger\/typhoon-blade//'`

auto_git_hash=`git rev-parse HEAD`
sed -i -- "s/VERSION_GITHASH .*)/VERSION_GITHASH $auto_git_hash)/g;" cmake/version.cmake

if [ -n "$BUILD_VERSION" ]; then
    sed -i -- "s/VERSION_SCM .*)/VERSION_SCM $PRODUCT_NAME-$SEMVER_VERSION\/$BUILD_VERSION)/g;" cmake/version.cmake
    # GENERATE tag
    curl -I -u wujian.1415:d47698a25104dbb0fd6a98888b5e2c9e "https://old-ci.byted.org/job/ch_debian_tag/buildWithParameters?token=WvgP5dE5KieAMqtcubn2&BUILD_VERSION=${BUILD_VERSION}&BUILD_BASE_COMMIT_HASH=${BUILD_BASE_COMMIT_HASH}"
fi

rm -rf output/
mkdir -p output

git config http.postBuffer 524288000
git submodule sync
http_proxy=http://sys-proxy-rd-relay.byted.org:8118 https_proxy=http://sys-proxy-rd-relay.byted.org:8118 no_proxy=.byted.org git submodule update --init --recursive

export CMAKE_BUILD_TYPE=${CUSTOM_CMAKE_BUILD_TYPE:-RelWithDebInfo}
export CMAKE_FLAGS="-DCMAKE_INSTALL_PREFIX=../output -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} ${CMAKE_FLAGS}"

rm -rf build && mkdir build && cd build && cmake ../ ${CMAKE_FLAGS} && ninja
ninja install

