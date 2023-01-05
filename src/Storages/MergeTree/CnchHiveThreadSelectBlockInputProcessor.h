#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Storages/MergeTree/CnchHiveReadPool.h>
#include <Storages/StorageCloudHive.h>

namespace DB
{
class CnchHiveReadPool;

class CnchHiveThreadSelectBlockInputProcessor : public SourceWithProgress
{
public:
    CnchHiveThreadSelectBlockInputProcessor(
        const size_t & thread,
        const std::shared_ptr<CnchHiveReadPool> & pool,
        const StorageCloudHive & storage,
        const StorageMetadataPtr & metadata_snapshot_,
        ContextPtr & context,
        const UInt64 & max_block_size);

    String getName() const override { return "CnchHiveThread"; }

    ~CnchHiveThreadSelectBlockInputProcessor() override;

    Block getHeader() const;

private:
    Chunk generate() override;

    bool getNewTask();

    size_t thread;

    std::shared_ptr<CnchHiveReadPool> pool;

    // const StorageCloudHive & storage;
    StorageMetadataPtr metadata_snapshot;

    ContextPtr context;
    // const UInt64 max_block_size;

    std::unique_ptr<CnchHiveReadTask> task;

    std::unique_ptr<ReadBuffer> read_buf;

    BlockInputStreamPtr parquet_stream;
};

}
