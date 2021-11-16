#pragma once
#include <Common/config.h>
#if USE_RDKAFKA

#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/KafkaLog.h>

#include <Storages/Kafka/StorageHaKafka.h>

namespace DB
{

class StorageHaKafka;

class HaKafkaBlockInputStream : public IBlockInputStream
{
public:
    HaKafkaBlockInputStream(StorageHaKafka & storage_, const StorageMetadataPtr & metadata_snapshot_, const std::shared_ptr<Context> & context_,
                            const Names & column_names_, size_t max_block_size_, size_t consumer_index_, bool need_materialized = false);
    ~HaKafkaBlockInputStream() override;

    String getName() const override { return storage.getName(); }
    Block getHeader() const override;

    void readPrefixImpl() override;
    Block readImpl() override;
    void readSuffixImpl() override;

    void forceCommit();

    void writeKafkaLog();

private:
    StorageHaKafka & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    Names column_names;
    UInt64 max_block_size;
    size_t consumer_index;
    bool need_materialized;

    static Names default_virtual_column_names;
    Names virtual_column_names;
    std::vector<int> used_column;
    MutableColumns virtual_columns;

    BufferPtr delimited_buffer;
    /// ConsumerBufferPtr buffer;
    bool broken = true;

    const Block non_virtual_header, virtual_header;
};

} // namespace DB

#endif
