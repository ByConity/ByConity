---
title: Linux 搭建 ByConity 开发环境
categories:
- Docs
- Community
tags:
- Docs
---

# Linux 搭建 ByConity 开发环境

# 环境依赖

从源码编译 ByConity 需要安装以下组件

```
Git
CMake 3.17 or newer
Ninja
C++ compiler: clang-11 or clang-12
Linker: lld
Third-Party Library: openssl

```

# Linux 开发环境

## 安装依赖

Debian 11 (Bullseye) 示例

```
sudo apt-get update
sudo apt-get install git cmake ccache python3 ninja-build libssl-dev

# install llvm 12
sudo apt install lsb-release wget software-properties-common gnupg # pre-requisites of llvm.sh
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 12

```

## 编译 ByConity

```
git clone --recursive <ByConity Repository URL> byconity

cd byconity
mkdir build && cd build
export CC=clang-12
export CXX=clang++-12
cmake ..
ninja

```

可执行文件在 programs 目录下

```
clickhouse-client    # byconity client
clickhouse-server    # byconity server
clickhouse-worker    # byconity worker
tso_server           # byconity tso
daemon_manager       # byconity daemon manager
resource_manager     # byconity resource manager

```
