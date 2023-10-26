# ByConity Dev Image

## How to use

Start by pulling the image
```
# 克隆仓库
git clone https://github.com/ByConity/ByConity.git 

# 进入到仓库的开发目录下
cd byconity/docker/debian/dev-env

# 拉取最新镜像
make pull
```

You can run the "byconity/dev-env" container with the following command:
```bash
# 运行容器，需要等待几分钟
make run
```


Inside the container, you can compile the ByConity code using the following commands.
The compiled binary will be found within your local filesystem at `${BYCONITY_SOURCE}/build_dev/program`
```bash
# 用于初始化、更新并检出Git仓库中的子模块。此过程需要下载大量文件，请耐心等待。
git submodule update --init --recursive

# 配置运行环境，这个命令执行成功之后，会自动解决include头文件的错误
cmake -S /root/ByConity -B build_dev

# 编译，最后面的64代表使用多少线程来构建，需要根据自己电脑情况修改
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
