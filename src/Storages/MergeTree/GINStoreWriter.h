#pragma once

#include <common/StringRef.h>
#include <Common/HashTable/StringHashMap.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/SeekableReadBuffer.h>
#include <Storages/IndexFile/RemappingEnv.h>
#include <Storages/MergeTree/GINStoreCommon.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>

namespace DB
{

class GINStoreWriter
{
public:
    static std::unique_ptr<GINStoreWriter> open(GINStoreVersion version_,
        const String& name_, GinDataPartHelperPtr&& storage_info_,
        UInt64 max_digestion_size_, Float32 density_);

    GINStoreWriter(const String& name_, GinDataPartHelperPtr&& storage_info_,
        GINStoreVersion version_, UInt64 max_digestion_size_, Float32 density_);
    virtual ~GINStoreWriter() = default;

    virtual UInt32 segmentID() const = 0;
    virtual UInt32 rowID() const = 0;
    virtual void incrementRows(UInt64 rows_) = 0;
    virtual void incrementProcessedBytes(UInt64 sz_) = 0;

    /// Append posting entry for term
    virtual void appendPostingEntry(const StringRef& term_, UInt32 row_id_) = 0;

    /// Check if we need to write segment to Gin index files
    virtual bool needToWrite() const = 0;

    /// Do last segment writing
    virtual void finalize() = 0;
    /// Method for writing segment data to Gin index files
    virtual void writeSegment() = 0;

    virtual void addToChecksums(MergeTreeDataPartChecksums& checksums_) = 0;

protected:
    const GINStoreVersion version;
    const Float32 density;
    const UInt64 max_digestion_size;
    const String name;

    GinDataPartHelperPtr storage_info;
};

/// Build a postings list for a term
class GinIndexPostingsBuilder
{
public:
    explicit GinIndexPostingsBuilder();
    /// Check whether a row_id is already added
    bool contains(UInt32 row_id_) const;

    /// Add a row_id into the builder
    void add(UInt32 row_id_);

    /// Serialize the content of builder to given WriteBuffer, returns the bytes of serialized data
    UInt64 serialize(WriteBuffer& buffer_, size_t rows_limit_);

    /// Deserialize the postings list data from given ReadBuffer, return a pointer to the GinIndexPostingsList created by deserialization
    static GinIndexPostingsListPtr deserialize(SeekableReadBuffer& buffer_,
        size_t posting_offset_);

    /// Easy for Debug
    String toString() const;

    UInt32 last_row_id = -1;
private:
    constexpr static int MIN_SIZE_FOR_ROARING_ENCODING = 16;

    static constexpr auto GIN_COMPRESSION_CODEC = "ZSTD";
    static constexpr auto GIN_COMPRESSION_LEVEL = 1;

    /// When the list length is no greater than MIN_SIZE_FOR_ROARING_ENCODING, array 'rowid_lst' is used
    /// As a special case, rowid_lst[0] == CONTAINS_ALL encodes that all rowids are set.
    std::array<UInt32, MIN_SIZE_FOR_ROARING_ENCODING> rowid_lst;

    /// When the list length is greater than MIN_SIZE_FOR_ROARING_ENCODING, roaring bitmap 'rowid_bitmap' is used
    roaring::Roaring rowid_bitmap;

    /// rowid_lst_length stores the number of row IDs in 'rowid_lst' array, can also be a flag(0xFF) indicating that roaring bitmap is used
    UInt8 rowid_lst_length = 0;

    /// Indicates that all rowids are contained, see 'rowid_lst'
    static constexpr UInt32 CONTAINS_ALL = std::numeric_limits<UInt32>::max();

    /// Indicates that roaring bitmap is used, see 'rowid_lst_length'.
    static constexpr UInt8 USES_BIT_MAP = 0xFF;

    /// Check whether the builder is using roaring bitmap
    bool useRoaring() const { return rowid_lst_length == USES_BIT_MAP; }

    /// Check whether the postings list has been flagged to contain all row ids
    bool containsAllRows() const { return rowid_lst[0] == CONTAINS_ALL; }
};

using GinIndexPostingsBuilderPtr = std::shared_ptr<GinIndexPostingsBuilder>;

class GINStoreWriterV0V1V2Base: public GINStoreWriter
{
public:
    GINStoreWriterV0V1V2Base(const String& name_, GinDataPartHelperPtr&& storage_info_,
        GINStoreVersion version_, UInt64 max_digestion_size_, Float32 density_);

    virtual UInt32 segmentID() const override;
    virtual UInt32 rowID() const override;
    virtual void incrementRows(UInt64 rows_) override;
    virtual void incrementProcessedBytes(UInt64 sz_) override;

    virtual void appendPostingEntry(const StringRef& term_, UInt32 row_id_) override;

    virtual bool needToWrite() const override;

    virtual void finalize() override;
    virtual void writeSegment() override;

    virtual void addToChecksums(MergeTreeDataPartChecksums& checksums_) override;

protected:
    /// Container for all term's Gin Index Postings List Builder
    struct GinIndexPostingsBuilderContainer
    {
        GinIndexPostingsBuilderContainer(): term_pool(std::make_unique<Arena>()) {}

        void reset()
        {
            term_pool = std::make_unique<Arena>();
            builders.clear();
        }

        ArenaPtr term_pool;
        StringHashMap<GinIndexPostingsBuilderPtr> builders;
    };

    using TokenPostingsBuilderPair = std::pair<std::string_view, GinIndexPostingsBuilderPtr>;
    using TokenPostingsBuilderPairs = std::vector<TokenPostingsBuilderPair>;

    virtual void initDictStream() = 0;
    virtual void flushDictStream() = 0;
    virtual void finalizeDictStream() = 0;
    virtual UInt64 writeDict(const TokenPostingsBuilderPairs& postings_builders_,
        const std::vector<UInt64>& posting_sizes_, size_t segment_id_) = 0;
    virtual void addDictToChecksums(MergeTreeDataPartChecksums& checksums_) = 0;

private:
    void initFileStreams();

    UInt64 current_size;
    GinIndexSegmentV0V1V2 current_segment;
    GinIndexPostingsBuilderContainer current_postings;

    std::unique_ptr<WriteBufferFromFileBase> metadata_file_stream;
    std::unique_ptr<WriteBufferFromFileBase> postings_file_stream;
};

class GINStoreWriterV0: public GINStoreWriterV0V1V2Base
{
public:
    GINStoreWriterV0(const String& name_, GinDataPartHelperPtr&& storage_info_,
        UInt64 max_digestion_size_, Float32 density_);

protected:
    virtual void initDictStream() override;
    virtual void flushDictStream() override;
    virtual void finalizeDictStream() override;
    virtual UInt64 writeDict(const TokenPostingsBuilderPairs& postings_builders_,
        const std::vector<UInt64>& posting_sizes_, size_t segment_id_) override;
    virtual void addDictToChecksums(MergeTreeDataPartChecksums& checksums_) override;

private:
    std::unique_ptr<WriteBufferFromFileBase> dict_file_stream;
};

class GINStoreWriterV2: public GINStoreWriterV0V1V2Base
{
public:
    GINStoreWriterV2(const String& name_, GinDataPartHelperPtr&& storage_info_,
        UInt64 max_digestion_size_, Float32 density_);

protected:
    virtual void initDictStream() override;
    virtual void flushDictStream() override;
    virtual void finalizeDictStream() override;
    virtual UInt64 writeDict(const TokenPostingsBuilderPairs& postings_builders_,
        const std::vector<UInt64>& posting_sizes_, size_t segment_id_) override;
    virtual void addDictToChecksums(MergeTreeDataPartChecksums& checksums_) override;

private:

    std::unique_ptr<IndexFile::WriteRemappingEnv> env;
};

}
