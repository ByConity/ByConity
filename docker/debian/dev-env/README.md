# ByConity Dev Image

## How to use

Start by pulling the image
```
docker pull byconity/dev-env
```

You can run the "byconity/dev-env" container with specific configurations and mounted volumes. The following command does this:
```bash
BYCONITY_SOURCE=$(pwd)/../../..
docker run -it --privileged --cap-add SYS_PTRACE \
  -p 2222:2222 \
  -v ~/.m2:/root/.m2 \
  -v ${BYCONITY_SOURCE}:/root/ByConity \
  -v ~/.ccache:/root/.ccache \
  --name dev-env \
  -d byconity/dev-env
```
Set the environment variable `BYCONITY_SOURCE` to the path where your ByConity source code is located.


After starting the container, you can enter it with:
```bash
docker exec -it dev-env /bin/bash
```
~~ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@localhost -p 2222~~

Inside the container, you can compile the ByConity code using the following commands.
The compiled binary will be found within your local filesystem at `${BYCONITY_SOURCE}/build_dev/program`
```bash
cmake -S /root/ByConity -B build_dev
ninja -C build_dev clickhouse-server -j 64
```

The you can use [docker-compose](../../docker-compose/README.md) to run the compiled binary

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