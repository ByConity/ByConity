# ByConity Dev Image

## How to use

Start by pulling the image
```
git clone https://github.com/ByConity/ByConity.git 

cd byconity/docker/debian/dev-env

# get the latest image
make pull
```

You can run the "byconity/dev-env" container with the following command:
```bash
# run container
make run
```


Inside the container, you can compile the ByConity code using the following commands.
The compiled binary will be found within your local filesystem at `${BYCONITY_SOURCE}/build_dev/program`
```bash
# get submodule, otherwise the IDE will report include head file error
git submodule update --init --recursive

cmake -S /root/ByConity -B build_dev

# 64 means use 64 threads to build, you need change it based on your computer
ninja -C build_dev clickhouse-server -j 64
```
Then you can use [docker-compose](../../docker-compose/README.md) to run the compiled binary

## Ubuntu dev-env
**WARNING: Under testing**

```bash
docker pull byconity/dev-env:ubuntu

BYCONITY_SOURCE=$(pwd)/../../..
docker run -it --rm --privileged --cap-add SYS_PTRACE \
  -v ~/.m2:/root/.m2 \
  -v ${BYCONITY_SOURCE}:/root/ByConity \
  -v ~/.ccache:/root/.ccache \
  byconity/dev-env:ubuntu bash

# build with mold
CC=clang-11 CXX=clang++-11 cmake -S /root/ByConity -B build_dev
mold -run ninja -C build_dev clickhouse-server -j 64

# verify mold is used
readelf -p .comment build_dev/programs/clickhouse
```


## Build dev-env image
```bash
DOCKER_BUILDKIT=1 docker build -t byconity/dev-env \
  --build-arg http_proxy="${http_proxy}" \
  --build-arg https_proxy="${https_proxy}" \
  -f Dockerfile .

## ubuntu
DOCKER_BUILDKIT=1 docker build -t byconity/dev-env:ubuntu \
  --build-arg http_proxy="${http_proxy}" \
  --build-arg https_proxy="${https_proxy}" \
  -f Dockerfile.ubuntu .
```
