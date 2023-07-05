# Welcome to ByConity

<p align="center"> 
<img width="717" alt="ByConity Arch 2023" src="https://github.com/ByConity/ByConity/assets/23332032/c266aa89-c1b8-4a35-a47d-ee5718a9443a">

Byconity is a unique open-source project designed to streamline your data processes. This cloud-native solution stands out with the ability to ingest both batch and streaming data, perform powerful queries on large scale data sets, and run seamlessly on both Kubernetes and physical clusters. Byconity reduces complexity, boosts efficiency, and empowers you with valuable insights faster.

**Query Large Scale Data with Speed and Precision**
When dealing with large-scale data, performance is crucial. Byconity shines in this aspect by providing powerful querying capabilities that excel in large-scale environments. With Byconity, you can extract valuable insights from vast amounts of data quickly and accurately.
    
**Break Down Data Silos with Byconity**
Data silos pose significant challenges in data management. With different systems and processes often resulting in isolated islands of data, it hampers data analysis and insights. Byconity addresses this issue by seamlessly ingesting both batch-loaded data and streaming data, thus enabling your systems to break down silos for smoother data flow.

**Designed for the Cloud, Flexible for Your Needs**
Byconity is designed with a cloud-native approach, optimized to take full advantage of the cloud's scalability, resilience, and ease of deployment. It can work seamlessly on both Kubernetes clusters and physical clusters, offering you the flexibility to deploy in the environment that best meets your requirements. This broad compatibility ensures that you can leverage Byconity's benefits, irrespective of your infrastructure.

ByConity is built from [ClickHouse](https://github.com/ClickHouse/ClickHouse) and inspired by Snowflake. We appreciate the excellent work of the ClickHouse team & Snowflake team. Because of huge archtechture difference, ClickHouse team don't think it is a good idea to adopt it into ClickHouse project. Here we open source it as a downstream project. Hope to bring it as a public weal for you.

## Benefits
- **Unified Data Management**: Byconity eliminates the need to maintain separate processes for batch and streaming data, making your systems more efficient.
- **High-Performance Data Querying** : Byconity's robust querying capabilities allow for quick and accurate data retrieval from large-scale datasets.
- **Avoid Data Silos** : By handling both batch and streaming data, Byconity ensures all your data can be integrated, promoting better insights.
- **Cloud-Native Design** : Byconity is built with a cloud-native approach, allowing it to efficiently leverage the advantages of the cloud and work seamlessly on both Kubernetes and physical clusters.
- **Open Source**: Being an open-source project, Byconity encourages community collaboration. You can contribute, improve, and tailor the platform according to your needs.
    
## Useful Link
    
- [Official Website](https://byconity.github.io/): has a quick high-level overview of ByConity on the home page.
- [Documentation](https://byconity.github.io/docs/introduction/main-principle-concepts): introduce basic usage guide and tech deep dive.
- [Getting started with Kubernetes](https://byconity.github.io/docs/deployment/deploy-k8s): demonstrates how to deploy a ByConity cluster in your Kubernetes clusters.
- [Getting started with physical machines](https://byconity.github.io/docs/deployment/package-deployment): demonstrateds how to deploy ByConity in your physical clusters.
- **Contact Us** : you can easily find us in [Discord server](https://discord.gg/V4BvTWGEQJ), [Youtube Channel](https://www.youtube.com/@ByConity/featured) and [Twitter](https://twitter.com/ByConity)

## Build ByConity

The easiest way to build ByConity is built in [docker](https://github.com/ByConity/ByConity/tree/master/docker/builder). ByConity executable file depend on Foundation DB library `libfdb_c.so`. So in order to run it, we need to install the FoundationDB client package. This [link](https://apple.github.io/foundationdb/getting-started-linux.html) tells how to install. We can download client package from FoundationDB GitHub release pages, for example [here](https://github.com/apple/foundationdb/releases/tag/7.1.0).

It can also be built through the following operating systems in physical machine:

- Linux

### 1. Prepare Prerequisites
The following packages are required:

- Git
- CMake 3.17 or newer
- Ninja
- C++ compiler: clang-11 or clang-12
- Linker: lld
- FoundationDB client [library](https://github.com/apple/foundationdb/releases/tag/7.1.0)

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

Then you can find the binary in the programs folder

```sh
clickhouse-client    # byconity client
clickhouse-server    # byconity server
clickhouse-worker    # byconity worker
tso_server           # byconity tso
daemon_manager       # byconity daemon manager
resource_manager     # byconity resource manager
```

