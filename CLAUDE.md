# CLAUDE.md - ByConity Codebase Guide for AI Assistants

This document provides comprehensive guidance for AI assistants working with the ByConity codebase. It covers architecture, development workflows, coding conventions, and key patterns.

## Table of Contents
- [Repository Overview](#repository-overview)
- [Codebase Architecture](#codebase-architecture)
- [Directory Structure](#directory-structure)
- [Build System](#build-system)
- [Development Workflow](#development-workflow)
- [Coding Conventions](#coding-conventions)
- [Testing Infrastructure](#testing-infrastructure)
- [Key Components](#key-components)
- [Common Patterns](#common-patterns)
- [Tips for AI Assistants](#tips-for-ai-assistants)

---

## Repository Overview

### What is ByConity?

ByConity is an advanced cloud-native analytical database derived from ClickHouse v21.8. While it builds upon ClickHouse's robust foundation, ByConity has diverged significantly to implement:

- **Compute-Storage Separation Architecture**: Scalable independent scaling of compute and storage
- **Advanced Cost-Based Optimizer**: Cascades-based query optimization with cardinality estimation
- **Stateless Workers**: Dynamic worker scaling for distributed query execution
- **ACID Transactions**: Full transaction support with TSO (Timestamp Oracle)
- **Multi-Storage Support**: S3, HDFS, NexusFS, and various data sources

**Key Differentiators from ClickHouse:**
- Independent metadata catalog (FoundationDB-based)
- Cloud-native design with shared-storage framework
- Distributed transaction support
- Resource manager for compute allocation
- Enhanced query optimizer

---

## Codebase Architecture

### High-Level Layered Architecture

```
┌─────────────────────────────────────────────┐
│  Client Layer (TCP/HTTP/gRPC/JDBC/ODBC)    │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  Server Layer                               │
│  - Parser (SQL → AST)                       │
│  - Optimizer (Cascades CBO)                 │
│  - QueryPlan (Physical Plans)               │
│  - Interpreter (Execution Logic)            │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  Execution Layer                            │
│  - Processors (Pipeline Stages)             │
│  - DataStreams (Stream Processing)          │
│  - ExecutionPipeline                        │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  Storage Layer                              │
│  - Catalog (Metadata Management)            │
│  - Storages (Multiple Engines)              │
│  - Disks (S3/HDFS/Local)                    │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  Distributed Layer                          │
│  - Transaction (ACID)                       │
│  - TSO (Timestamp Oracle)                   │
│  - ResourceManagement                       │
│  - ServiceDiscovery                         │
└─────────────────────────────────────────────┘
```

### Core Modules

The architecture implements these essential modules:

1. **Catalog** - FoundationDB-based metadata management
2. **CloudServices** - Distributed RPC and service communication
3. **Optimizer** - Cost-based query optimization (13 subdirectories)
4. **Transaction** - Multi-level ACID transaction support
5. **TSO** - Global timestamp ordering
6. **ResourceManagement** - Compute resource allocation
7. **Storages** - Multiple storage engines (23 subdirectories)
8. **QueryPlan** - Physical query plan generation

---

## Directory Structure

### Top-Level Organization

```
ByConity/
├── src/                    # Main source code (~7,270 files, 47 modules)
├── programs/               # Executable programs (23 tools)
├── base/                   # Base libraries (11 subdirectories)
├── contrib/                # Third-party dependencies (200+ libraries)
├── cmake/                  # CMake build configuration (25+ files)
├── rust/                   # Rust components (BLAKE3, prql, skim)
├── tests/                  # Comprehensive test infrastructure
├── docker/                 # Docker containerization
├── deploy/                 # Deployment automation
├── ci_scripts/             # CI/CD pipeline scripts
├── doc/                    # Internal documentation
├── docs/                   # User-facing documentation (en/ru)
└── utils/                  # Build and maintenance utilities (47 subdirs)
```

### Key Source Directories (`/src`)

#### ByConity-Specific Modules

| Directory | Purpose | Key Files/Subdirs |
|-----------|---------|-------------------|
| **Catalog** | Metadata management with FoundationDB | Metadata schemas, catalog client |
| **Optimizer** | Advanced cost-based optimizer | Cascades, CostModel, CardinalityEstimate (13 subdirs) |
| **QueryPlan** | Query planning and optimization | Hints, Optimizations, plan nodes |
| **Transaction** | ACID transaction support | CnchServerTransaction, LockManager, IntentLock |
| **TSO** | Timestamp Oracle | Global timestamp coordination |
| **ResourceManagement** | Resource allocation | Worker scheduling, resource groups |
| **Statistics** | Optimizer statistics | Cardinality estimation, histograms |
| **CloudServices** | Distributed services | CnchServerClient, CnchWorkerClient |
| **DaemonManager** | Background task management | Background job scheduling |
| **WorkerTasks** | Distributed task execution | Worker task coordination |

#### Core Database Components

| Directory | Purpose | Subdirectories |
|-----------|---------|----------------|
| **Storages** | Storage engines | MergeTree, Distributed, HDFS, Hive, Kafka, MySQL, PostgreSQL, S3, etc. (23 subdirs) |
| **Parsers** | SQL parsing | SQL grammar, AST generation (5 subdirs) |
| **Interpreters** | Query execution | ClusterProxy, DistributedStages, PreparedStatement (13 subdirs) |
| **Processors** | Data processing pipeline | Exchange, Executors, Transforms, Formats (11 subdirs) |
| **Functions** | SQL functions | 100+ built-in functions (9 subdirs) |
| **AggregateFunctions** | Aggregate functions | Sum, Count, Avg, etc. (20+ subdirs) |
| **DataTypes** | Type system | Numeric, String, Array, Map, etc. (5 subdirs) |
| **Formats** | File format support | Parquet, ORC, CSV, JSON, Arrow (3 subdirs) |

#### Infrastructure Components

| Directory | Purpose |
|-----------|---------|
| **Server** | Main server implementation (HTTP, gRPC) |
| **Client** | CLI client implementation |
| **IO** | Input/Output operations, buffers |
| **Disks** | Storage abstraction (S3, HDFS, CloudFS) |
| **Compression** | Data compression algorithms |
| **Coordination** | Distributed coordination |
| **Access** | Authentication and access control |
| **Protos** | Protocol Buffer definitions (gRPC services) |

### Programs (`/programs`)

| Program | Purpose |
|---------|---------|
| `server` | Main ByConity server (query processing) |
| `client` | Interactive CLI client |
| `worker` | ByConity worker for distributed execution |
| `tso_server` | Timestamp Oracle server |
| `resource-manager` | Resource allocation service |
| `keeper` | Distributed coordination (ZooKeeper alternative) |
| `daemon_manager` | Background task daemon |
| `library-bridge` | External library integration |
| `odbc-bridge` | ODBC data source bridge |
| `local` | Local execution mode |
| `benchmark` | Performance benchmarking |
| `part-toolkit` | MergeTree part manipulation |
| `meta-inspector` | Metadata inspection tool |

### Test Organization (`/tests`)

```
tests/
├── queries/                # SQL test cases
│   ├── 0_stateless/       # Basic stateless tests
│   ├── 1_stateful/        # Tests requiring state/data
│   ├── 4_cnch_*/          # ByConity-specific tests (30+ categories)
│   ├── 7_clickhouse_sql/  # ClickHouse compatibility tests
│   ├── bugs/              # Regression tests
│   └── tpcds/             # TPC-DS benchmark tests
├── integration/            # Integration tests (20+ directories)
├── performance/            # Performance benchmarks
├── ci/                    # CI/CD test scripts
├── fuzz/                  # Fuzzing test inputs
└── testflows/             # Advanced test flows
```

---

## Build System

### CMake Configuration

**Primary Build Files:**
- `CMakeLists.txt` (664 lines) - Root build configuration
- `cmake/` directory - 25+ specialized CMake modules
- `.clang-format` - Code formatting rules
- `.clang-tidy` - Static analysis configuration

**Key CMake Modules:**
- `arch.cmake` - Architecture detection
- `CompileFoundationDB.cmake` - FoundationDB integration
- `sanitize.cmake` - Sanitizer configuration
- `cpu_features.cmake` - CPU feature detection
- `warnings.cmake` - Warning configuration

**Build Requirements:**
- CMake 3.17 or newer
- C++ compiler: clang-11 or clang-12 (required, not GCC)
- Linker: lld
- FoundationDB client library
- C++20 standard

**Build Configuration:**
```cmake
# Project uses C++20
# Strict compilation: CMAKE_COMPILE_WARNING_AS_ERROR ON
# Configuration types: RelWithDebInfo, Debug, Release, MinSizeRel
# Generates: compile_commands.json for tooling
```

### Building ByConity

**Option 1: Docker Dev Environment (Recommended)**
```bash
# Use the official dev environment
cd docker/debian/dev-env
# Follow docker/debian/dev-env/README.md
```

**Option 2: Metal Machine Build**
```bash
# Install prerequisites
sudo apt-get update
sudo apt-get install git cmake ccache python3 ninja-build \
  libssl-dev libsnappy-dev apt-transport-https

# Install clang-12
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 12

# Clone and build
git clone --recursive https://github.com/ByConity/ByConity.git
cd ByConity
mkdir build && cd build
export CC=clang-12
export CXX=clang++-12
cmake ..
ninja
```

**Verify build configuration in `CMakeCache.txt`:**
```
CMAKE_C_COMPILER:FILEPATH=/usr/bin/clang-12
CMAKE_CXX_COMPILER:FILEPATH=/usr/bin/clang++-12
CMAKE_LINKER:FILEPATH=/usr/bin/ld.lld-12
```

### External Dependencies

Major dependencies in `/contrib`:
- **Data Formats**: Arrow, Avro, Parquet
- **Cloud**: AWS SDK (S3, CloudWatch)
- **RPC**: Protobuf, gRPC
- **Foundation**: Boost, LLVM-Project
- **Messaging**: Kafka, RabbitMQ
- **Databases**: PostgreSQL, MySQL drivers
- **Security**: OpenSSL/BoringSSL
- **Memory**: Jemalloc
- **Consensus**: NuRaft

---

## Development Workflow

### Setting Up Development Environment

**Using Docker Compose (Recommended):**
```bash
cd docker/docker-compose
cp .env.tpl .env
# Edit .env to configure:
# - COMPOSE_PROJECT_NAME
# - DATA_DIR (local path for cache/logs)
# - SERVER_TCP_PORT, SERVER_HTTP_PORT
# - BYCONITY_IMAGE (version or byconity/debian-runner for local build)

# Start essential services + simple cluster
docker-compose -f docker-compose.essentials.yml \
  -f docker-compose.simple.yml up -d

# Access ByConity CLI
./scripts/byconity-cli.sh

# Access HDFS
./scripts/hdfs-cli.sh "hdfs dfs -ls /user/clickhouse"

# Access FoundationDB CLI
./scripts/fdb-cli.sh
```

### Running Tests

**Unit Tests:**
```bash
cd build
./src/unit_tests_dbms --output-on-failure

# Run specific tests with filter
./src/unit_tests_dbms --output-on-failure --gtest_filter='backgroundjob*'
```

**CI Tests:**
```bash
# Edit ci_scripts/run_ci_in_development_env.sh to set environment variables
ci_scripts/run_ci_in_development_env.sh
```

**Integration Tests:**
```bash
# Located in /tests/integration
# Require running ByConity cluster
# See individual test README files
```

### Deployment

**Kubernetes:**
- Follow [deployment guide](https://byconity.github.io/docs/deployment/deployment-with-k8s)
- Uses Helm charts

**Physical Machines:**
- Follow [package deployment guide](https://byconity.github.io/docs/deployment/package-deployment)
- Use `/deploy` automation scripts

---

## Coding Conventions

### C++ Style Guide

**Code Formatting (`.clang-format`):**
```yaml
BasedOnStyle: WebKit
Language: Cpp
Standard: Cpp11
ColumnLimit: 140
TabWidth: 4
UseTab: Never
PointerAlignment: Middle
IndentCaseLabels: true
AllowShortFunctionsOnASingleLine: InlineOnly
AlwaysBreakTemplateDeclarations: true
SortIncludes: true
```

**Key Style Rules:**
- **Line Length**: 140 characters maximum
- **Indentation**: 4 spaces (no tabs)
- **Braces**: Custom brace wrapping
  ```cpp
  // After class, function, namespace
  class MyClass
  {
      void myFunction()
      {
          if (condition)
          {
              // ...
          }
      }
  };
  ```
- **Pointers**: Middle alignment (`Type * ptr`)
- **Templates**: Always break template declarations
- **Include Order**: Sorted with specific priority (system → common → DB → local)

### Naming Conventions

Based on codebase patterns:

**Namespaces:**
```cpp
namespace DB {
namespace ErrorCodes {
    extern const int LOGICAL_ERROR;
}
}

namespace DB::ResourceManagement {
    // Nested namespaces for modules
}
```

**Classes:**
```cpp
class StorageCnchMergeTree  // PascalCase
class CnchServerTransaction
class QueryPlan
```

**Files:**
- Header files: `.h` extension
- Implementation: `.cpp` extension
- Match class name: `StorageCnchMergeTree.h`, `StorageCnchMergeTree.cpp`

**Functions/Methods:**
```cpp
void executeQuery()  // camelCase
void getBlocksToRead()
void optimizeQueryPlan()
```

**Constants:**
```cpp
extern const int MAX_QUERY_SIZE;  // UPPER_SNAKE_CASE (in ErrorCodes)
const std::string DEFAULT_DATABASE = "default";
```

### Common Code Patterns

**Error Handling:**
```cpp
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}

void someFunction()
{
    throw Exception("Error message", ErrorCodes::LOGICAL_ERROR);
}
}
```

**Logging:**
```cpp
#include <Poco/Logger.h>
#include <Common/Logger.h>

Poco::Logger * log = &Poco::Logger::get("ComponentName");
LOG_DEBUG(log, "Debug message: {}", value);
LOG_INFO(log, "Info message");
LOG_WARNING(log, "Warning");
LOG_ERROR(log, "Error occurred");
```

**Context Passing:**
```cpp
#include <Interpreters/Context.h>

void processQuery(ContextPtr context)
{
    // Context contains:
    // - Settings
    // - User session info
    // - Catalog access
    // - Resource manager
    auto settings = context->getSettingsRef();
    auto catalog = context->getCnchCatalog();
}
```

### File Organization

**Header File Structure:**
```cpp
#pragma once

// System includes
#include <vector>
#include <memory>

// Third-party includes
#include <Poco/Logger.h>

// Common/Base includes
#include <Common/Logger.h>

// DB includes
#include <Storages/IStorage.h>

namespace DB
{

class MyClass
{
public:
    // Public interface

private:
    // Private members
};

}
```

---

## Testing Infrastructure

### Test Categories

**Unit Tests:**
- Location: Built as `unit_tests_dbms` binary
- Framework: Google Test (gtest)
- Run: `./src/unit_tests_dbms --output-on-failure`
- Filter: `--gtest_filter='pattern*'`

**Stateless Tests:**
- Location: `/tests/queries/0_stateless`
- SQL test files with expected results
- Fast, no persistent state

**Stateful Tests:**
- Location: `/tests/queries/1_stateful`
- Require data loading
- Test data persistence

**ByConity-Specific Tests:**
- Location: `/tests/queries/4_cnch_*`
- 30+ specialized test categories:
  - `4_cnch_stateless` - Core functionality
  - `4_cnch_external_table` - External data sources
  - `4_cnch_integration` - Integration scenarios
  - `4_cnch_stateless_mysql` - MySQL compatibility
  - `4_cnch_S3_only` - S3-specific tests

**Integration Tests:**
- Location: `/tests/integration`
- Docker-based test environments
- External system integration (MySQL, PostgreSQL, Kafka, etc.)

**Performance Tests:**
- Location: `/tests/performance`
- Benchmark queries
- Performance regression detection

**Compatibility Tests:**
- Location: `/tests/queries/7_clickhouse_sql`
- ClickHouse SQL compatibility
- Hive compatibility tests

### Test Execution

**Running Specific Test Suites:**
```bash
# Unit tests
cd build
./src/unit_tests_dbms --output-on-failure

# Filter by component
./src/unit_tests_dbms --gtest_filter='optimizer*'
./src/unit_tests_dbms --gtest_filter='transaction*'

# CI tests (requires setup)
ci_scripts/run_ci_in_development_env.sh
```

### Adding New Tests

**For Unit Tests:**
1. Add test file in appropriate `/src/{Module}/tests/` directory
2. Use gtest macros: `TEST(TestSuite, TestCase)`
3. Link with `unit_tests_dbms` target in CMake

**For SQL Tests:**
1. Create `.sql` file in `/tests/queries/4_cnch_stateless/`
2. Create `.reference` file with expected output
3. Run test framework to validate

---

## Key Components

### 1. Catalog (Metadata Management)

**Location:** `/src/Catalog`

**Purpose:** Centralized metadata management using FoundationDB

**Key Concepts:**
- Metadata storage for databases, tables, parts
- FoundationDB-based persistence
- Distributed metadata access
- Schema evolution

**Usage Pattern:**
```cpp
auto catalog = context->getCnchCatalog();
auto table = catalog->getTable(database, table_name);
```

### 2. Query Optimizer

**Location:** `/src/Optimizer`

**Architecture:** Cascades-based cost optimizer

**Key Subdirectories:**
- `Cascades/` - Cascades optimization framework
- `CostModel/` - Cost estimation
- `CardinalityEstimate/` - Statistics-based cardinality
- `Rule/` - Transformation rules
- `Rewriter/` - Query rewriting
- `MaterializedView/` - MV optimization

**Optimization Flow:**
1. Parse SQL → AST
2. Build logical plan
3. Apply optimization rules
4. Cost-based plan selection
5. Generate physical plan

### 3. Transaction System

**Location:** `/src/Transaction`

**Components:**
- `CnchServerTransaction` - Server-side transaction coordination
- `CnchWorkerTransaction` - Worker-side execution
- `CnchExplicitTransaction` - User-initiated transactions
- `LockManager` - Distributed locking
- `IntentLock` - Concurrency control

**TSO Integration:**
- Global timestamp ordering via `/src/TSO`
- Timestamp allocation for transactions
- Conflict detection

### 4. Storage Engines

**Location:** `/src/Storages`

**Primary Engine:** `StorageCnchMergeTree` (ByConity-specific)

**Other Engines:**
- `StorageDistributed` - Distributed tables
- `StorageHDFS`, `StorageS3` - Object storage
- `StorageHive` - Hive integration
- `StorageKafka` - Streaming ingestion
- `StorageMySQL`, `StoragePostgreSQL` - External databases

**MergeTree Specifics:**
- Cloud-native part storage
- Compute-storage separation
- Part lifecycle management via Catalog

### 5. Resource Management

**Location:** `/src/ResourceManagement`

**Capabilities:**
- Worker group management
- Query resource allocation
- Worker health monitoring
- Dynamic scaling

**Integration:**
```cpp
auto rm = context->getResourceManagerClient();
auto worker_group = rm->getWorkerGroup(vw_name);
```

### 6. Distributed Services (CloudServices)

**Location:** `/src/CloudServices`

**RPC Services:**
- `CnchServerClient` - Client to server communication
- `CnchWorkerClient` - Client to worker communication
- Background task coordination

**Protocol:** gRPC with Protocol Buffers (`/src/Protos`)

### 7. Statistics

**Location:** `/src/Statistics`

**Purpose:** Query optimizer statistics

**Components:**
- Cardinality estimation
- Histogram generation
- Auto statistics collection
- Statistics cache

---

## Common Patterns

### 1. Context Propagation

**Pattern:** Pass `ContextPtr` through function calls

```cpp
void executeQuery(const String & query, ContextPtr context)
{
    auto settings = context->getSettingsRef();
    auto catalog = context->getCnchCatalog();
    // Use context for all subsystem access
}
```

### 2. Shared Pointer Usage

**Pattern:** Use `std::shared_ptr` for major objects

```cpp
using StoragePtr = std::shared_ptr<IStorage>;
using ContextPtr = std::shared_ptr<Context>;
using QueryPlanPtr = std::unique_ptr<QueryPlan>;
```

### 3. Processors and Pipelines

**Pattern:** Build execution pipelines from processors

```cpp
Pipe pipe;
pipe.addTransform(std::make_shared<FilterTransform>(...));
pipe.addTransform(std::make_shared<AggregatingTransform>(...));
```

### 4. Error Handling

**Pattern:** Use DB::Exception with error codes

```cpp
#include <Common/Exception.h>

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

throw Exception("Detailed error message", ErrorCodes::LOGICAL_ERROR);
```

### 5. Configuration Access

**Pattern:** Access server configuration via Context

```cpp
const auto & config = context->getConfigRef();
String value = config.getString("path.to.setting", "default");
```

### 6. Logging

**Pattern:** Use Poco logging with formatters

```cpp
Poco::Logger * log = &Poco::Logger::get("ComponentName");
LOG_DEBUG(log, "Processing {} items", count);
LOG_ERROR(log, "Failed to process: {}", error_msg);
```

### 7. Protocol Buffer Usage

**Pattern:** Use generated protobuf classes for RPC

```cpp
#include <Protos/cnch_server_rpc.pb.h>

Protos::QueryRequest request;
request.set_query(query_text);
// Send via gRPC
```

---

## Tips for AI Assistants

### Understanding the Codebase

1. **ByConity vs ClickHouse**
   - Recognize that ByConity has diverged significantly from ClickHouse
   - Don't assume ClickHouse patterns apply directly
   - Key differences: Catalog, CloudServices, Transaction, TSO modules

2. **Module Boundaries**
   - Respect module boundaries defined in `/src` subdirectories
   - Each major module has specific responsibilities
   - Use appropriate include paths

3. **Distributed Architecture**
   - Server coordinates queries
   - Workers execute tasks
   - TSO provides global ordering
   - ResourceManager allocates compute
   - Catalog manages metadata

### Making Code Changes

1. **Before Modifying Code**
   - Read existing implementation in the same module
   - Check for similar patterns in the codebase
   - Understand the module's role in the architecture
   - Review protocol buffer definitions if touching RPC code

2. **Adding New Features**
   - Follow existing naming conventions (PascalCase classes, camelCase methods)
   - Use namespace `DB` for main code
   - Add appropriate error codes in `ErrorCodes` namespace
   - Include logging at appropriate levels
   - Add unit tests in module's `tests/` directory

3. **Modifying Optimizer**
   - Optimizer changes affect query performance critically
   - Understand Cascades framework before modifying
   - Cost model changes require careful validation
   - Test with TPC-DS queries in `/tests/queries/tpcds`

4. **Storage Engine Changes**
   - Understand MergeTree part lifecycle
   - Consider Catalog interactions
   - Test with various data formats
   - Validate distributed scenarios

5. **Transaction Changes**
   - Transaction logic is complex and critical
   - Understand TSO integration
   - Test concurrency scenarios
   - Verify lock acquisition/release

### Testing Changes

1. **Always Add Tests**
   - Unit tests for new functions/classes
   - SQL tests for query behavior changes
   - Integration tests for external system changes

2. **Test Levels**
   - Unit: Fast, isolated, component-level
   - Stateless: SQL queries without persistent state
   - Stateful: Requires data loading
   - Integration: Full system with external dependencies

3. **Performance Considerations**
   - Run performance tests for optimizer changes
   - Profile memory usage for data processing changes
   - Benchmark against TPC-DS for significant changes

### Common Pitfalls to Avoid

1. **Don't Mix ClickHouse and ByConity Concepts**
   - ByConity uses Catalog, not just file-based metadata
   - Transaction semantics differ
   - Storage layer architecture differs

2. **Don't Ignore FoundationDB Dependency**
   - Catalog requires FoundationDB
   - Must have FDB client library
   - Handle FDB connection errors

3. **Don't Bypass Context**
   - Always access subsystems via Context
   - Don't create global singletons
   - Context contains session state, settings, catalog access

4. **Don't Hardcode Paths**
   - Use configuration for paths
   - Support S3, HDFS, and local storage
   - Respect storage abstraction layer

5. **Don't Forget Error Handling**
   - Use Exception with proper ErrorCodes
   - Log errors appropriately
   - Clean up resources (RAII patterns)

### Debugging Tips

1. **Useful Log Locations**
   - Server logs: Check Docker logs or configured log path
   - Query logs: `system.query_log` table
   - Part logs: `system.parts` table

2. **Debugging Tools**
   - `meta-inspector` - Inspect metadata
   - `part-toolkit` - Manipulate parts
   - FoundationDB CLI (`fdb-cli.sh`)
   - HDFS CLI (`hdfs-cli.sh`)

3. **Common Issues**
   - FoundationDB connection failures
   - Worker registration issues (check ServiceDiscovery)
   - Part visibility (check Catalog sync)
   - Transaction timeouts (check TSO)

### Best Practices

1. **Code Quality**
   - Run `clang-format` before committing
   - Fix `clang-tidy` warnings
   - Follow the 140-character line limit
   - Write self-documenting code with clear variable names

2. **Performance**
   - Minimize memory allocations in hot paths
   - Use move semantics for large objects
   - Consider pipeline parallelism
   - Profile before optimizing

3. **Maintainability**
   - Keep functions focused and small
   - Add comments for complex algorithms
   - Document protocol buffer changes
   - Update tests when changing behavior

4. **Security**
   - Validate user input
   - Use prepared statements
   - Respect access control
   - Don't log sensitive data

### File Location Quick Reference

| What to Find | Where to Look |
|--------------|---------------|
| Metadata management | `/src/Catalog` |
| Query optimization | `/src/Optimizer` |
| Transaction logic | `/src/Transaction` |
| Storage engines | `/src/Storages` |
| SQL parsing | `/src/Parsers` |
| Query execution | `/src/Interpreters` |
| Built-in functions | `/src/Functions` |
| RPC definitions | `/src/Protos` |
| Server main | `/programs/server` |
| Client CLI | `/programs/client` |
| Worker main | `/programs/worker` (check programs directory) |
| TSO server | `/programs/tso_server` |
| Build configuration | `CMakeLists.txt`, `/cmake` |
| Tests | `/tests/queries/4_cnch_*` |
| Documentation | `/doc`, `/docs`, `README.md` |

### Contributing

1. **Read Before Contributing**
   - `CONTRIBUTING.md` - Contribution guidelines
   - `CODE_OF_CONDUCT.md` - Community standards
   - `SECURITY.md` - Security policies

2. **Pull Request Process**
   - Sign CLA or provide DCO
   - Include appropriate tests
   - Follow commit message conventions
   - Run full test suite
   - Update documentation if needed

3. **Getting Help**
   - Discord: https://discord.gg/V4BvTWGEQJ
   - GitHub Issues: https://github.com/ByConity/ByConity/issues
   - Documentation: https://byconity.github.io/docs

---

## Resources

### Official Documentation
- [Website](https://byconity.github.io/)
- [Documentation](https://byconity.github.io/docs/introduction/main-principle-concepts)
- [GitHub Repository](https://github.com/ByConity/ByConity)

### Community
- [Discord Server](https://discord.gg/V4BvTWGEQJ)
- [Twitter](https://twitter.com/ByConity)
- [YouTube Channel](https://www.youtube.com/@ByConity/featured)

### Related Projects
- [ClickHouse](https://clickhouse.com/) - Original upstream project
- [FoundationDB](https://www.foundationdb.org/) - Metadata storage
- [Apache Arrow](https://arrow.apache.org/) - Columnar data format

---

## Summary Statistics

- **Total Source Files**: ~7,270 in `/src`
- **Major Modules**: 47 primary components
- **Programs**: 23 executable tools
- **Test Categories**: 40+ specialized test directories
- **External Dependencies**: 200+ in `/contrib`
- **Languages**: C++20, Protocol Buffers, Python, Rust, SQL
- **Build System**: CMake 3.17+, Ninja
- **Compiler**: Clang 11/12 required (not GCC)
- **Lines of Code**: Millions (large-scale distributed system)

---

**Last Updated:** 2026-01-10
**ByConity Version:** Based on current master branch
**Maintained By:** ByConity Community

For questions or updates to this guide, please open an issue on GitHub.
