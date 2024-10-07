#pragma once

#include <Common/Logger.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <Columns/IColumn.h>
#include <Common/HashTable/HashMap.h>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>

namespace DB
{

class IBlockOutputStream;
class IBlockInputStream;
class StorageCloudMergeTree;

namespace IngestColumn
{

class KeyInfo
{
public:
    KeyInfo() = default;
    KeyInfo(UInt32 part_id, UInt8 exist_status);
    UInt32 getPartID() const; // for source part id
    UInt8 getExistStatus() const; // whether this key exists in target table data
    void updateExistStatus(UInt8 new_exist_status);
    bool operator == (const KeyInfo & other) const;
    static void checkNumberOfParts(const MergeTreeDataPartsVector & parts);

private:
    static constexpr UInt32 EXIST_STATUS_MASK = 1u;
    static constexpr UInt32 PART_ID_MASK = ~1u;
    UInt32 data = 0;
};

/// for maping between partname, part porter
class PartMap
{
public:
    static PartMap buildPartMap(const MergeTreeDataPartsVector & source_parts);
    MergeTreeDataPartPtr getPart(UInt32 part_id) const;
    UInt32 getPartID(const MergeTreeDataPartPtr & part) const;
private:
    PartMap(const MergeTreeDataPartsVector & source_parts);
    struct id_tag{};
    struct ptr_tag{};
    using value_type = std::pair<UInt32, const IMergeTreeDataPart *>;
    using container_type = boost::multi_index_container<
        value_type,
        boost::multi_index::indexed_by<
            boost::multi_index::ordered_unique<
                boost::multi_index::tag<id_tag>,
                boost::multi_index::member<value_type, UInt32, &value_type::first>
            >,
            boost::multi_index::ordered_unique<
                boost::multi_index::tag<ptr_tag>,
                boost::multi_index::member<value_type, const IMergeTreeDataPart *, &value_type::second>
            >
        >
    >;

    container_type id_map;
};

HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> combineHashmaps(
    std::vector<HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash>> && thread_hashmaps);

void buildHashTableFromBlock(
    const UInt32 part_id,
    const Block & block,
    const Strings & ordered_key_names,
    HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> & hash_table,
    size_t bucket_num,
    Arena & keys_pool,
    size_t number_of_buckets
);

void probeHashTableFromBlock(
    const UInt32 part_id,
    const Block & block,
    const Strings & ordered_key_names,
    HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> & hashmap,
    size_t bucket_num,
    Arena & keys_pool,
    size_t number_of_buckets,
    std::unordered_map<UInt32, std::set<UInt32>> & target_part_index
);

class TargetPartData
{
public:
    TargetPartData(
        std::vector<std::pair<Block, std::vector<FieldVector>>> data_,
        Strings ordered_key_names_,
        Strings ingest_column_names_,
        Block target_header,
        const std::vector<UInt8> & value_column_compression_statuses,
        const bool ingest_default_column_value_if_not_provided
    );

    bool updateWithSourceBlock(const Block & source_block);
    const std::vector<std::pair<Block, std::vector<FieldVector>>> & getData() const;
    void reset(); // for release memory

    const Block target_header;
    const Strings ordered_key_names;
    const Strings ingest_column_names;
    const std::vector<UInt8> ingest_column_compression_statuses;
    const bool ingest_default_column_value_if_not_provided;
    void clearUpdateOffset();

    /// for testing
    size_t getCurrentBlockIdx() const { return current_block_idx; }
    size_t getCurrentRowInBlockIdx() const { return current_row_in_block_idx; }
private:
    /// a vector of <target block, target data> where target data is a vector of column(FieldVector) with
    /// the order of column is same with ingest_column_names;
    std::vector<std::pair<Block, std::vector<FieldVector>>> data;
    size_t current_block_idx = 0;
    size_t current_row_in_block_idx = 0;
};

std::pair<size_t, size_t> updateTargetBlockWithSourceBlock(
    std::pair<Block, std::vector<FieldVector>> & target_data,
    const Block & source_block,
    size_t current_source_row_idx,
    size_t current_row_in_block_idx,
    const Strings & ordered_key_names,
    const Strings & value_names
);

BlocksList makeBlockListFromUpdateData(const TargetPartData & target_part_data);
StringRef placeKeysInPool(const size_t row, const Columns & key_columns, StringRefs & keys, Arena & pool);

void writeBlock(
    const Block & src_block,
    const Strings & ordered_key_names,
    const size_t bucket_num,
    const HashMapWithSavedHash<StringRef, IngestColumn::KeyInfo, StringRefHash> & source_key_map,
    std::mutex & new_part_output_mutex,
    const size_t number_of_buckets,
    IBlockOutputStream & new_part_output,
    const StorageMetadataPtr & target_meta_data_ptr,
    LoggerPtr log);

Names getColumnsFromSourceTableForInsertNewPart(
    const Names & ordered_key_names,
    const Names & ingest_column_names,
    const StorageMetadataPtr & source_meta_data_ptr);

IngestColumn::TargetPartData readTargetDataForUpdate(
    IBlockInputStream & in,
    const Strings & ordered_key_names,
    const Strings & ingest_column_names,
    const std::vector<UInt8> & value_column_compression_statuses,
    bool ingest_default_column_value_if_not_provided);

void updateTargetDataWithSourceData(
    IBlockInputStream & source_in,
    IngestColumn::TargetPartData & target_part_data);

MutableColumnPtr combineMapImplicitColumns(const DataTypePtr & key_data_type,
    const DataTypePtr & target_data_type, size_t rows,
    const std::unordered_map<String, ColumnPtr> & src_value_columns);

} /// end namespace IngestColumn

} /// end namespace DB

