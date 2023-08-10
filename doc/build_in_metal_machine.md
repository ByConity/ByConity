ByConity can also be built through the following operating systems in physical machine:

- Linux

### 1. Prepare Prerequisites
The following packages are required:

- Git
- CMake 3.17 or newer
- Ninja
- C++ compiler: clang-11 or clang-12
- Linker: lld
- FoundationDB client [library][foundationdb-client-library]

```sh
sudo apt-get update
sudo apt-get install git cmake ccache python3 ninja-build libssl-dev libsnappy-dev apt-transport-https

# install llvm 12
sudo apt install lsb-release wget software-properties-common gnupg # pre-requisites of llvm.sh
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 12
```

### 2. Checkout Source Code

```sh
git clone --recursive https://github.com/ByConity/ByConity.git
```

### 3. Build

```sh
cd ByConity
mkdir build && cd build
export CC=clang-12
export CXX=clang++-12
cmake ..
ninja
```

If the build failed, make sure that clang and lld linker is invoked in compilation. For example, you can find which tool is used by checking the `CMakeCache.txt` file which is the output file of `cmake ..` command. You should find info similar like this

```
...
//C compiler
CMAKE_C_COMPILER:FILEPATH=/usr/bin/clang-12
...
//CXX compiler
CMAKE_CXX_COMPILER:FILEPATH=/usr/bin/clang++-12
...
//Path to a program.
CMAKE_LINKER:FILEPATH=/usr/bin/ld.lld-12
...
```

After the build finish succesfully, you can find the binary in the programs folder

```sh
clickhouse-client    # byconity client
clickhouse-server    # byconity server
clickhouse-worker    # byconity worker
tso_server           # byconity tso
daemon_manager       # byconity daemon manager
resource_manager     # byconity resource manager
```

[foundationdb-client-library]: https://github.com/apple/foundationdb/releases/tag/7.1.3
