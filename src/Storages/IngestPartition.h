#pragma once

#include <Core/NamesAndTypes.h>

#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <common/logger_useful.h>


namespace DB
{

struct IngestPart
{
    FutureMergedMutatedPart future_part;
    ReservationPtr reserved_space;

    IngestPart(const FutureMergedMutatedPart & future_part_, ReservationPtr reserved_space_)
    : future_part(future_part_)
    , reserved_space(std::move(reserved_space_)) {}
};

using IngestPartPtr = std::shared_ptr<IngestPart>;
using IngestParts = std::vector<IngestPartPtr>;

class IngestPartition
{
public:

    struct IngestSource
    {
        IngestSource(const Block & block_) : block(block_) {}
        Block block;
        mutable std::mutex mutex;
    };

    using IngestSourcePtr = std::shared_ptr<IngestSource>;
    using IngestSources = std::vector<IngestSourcePtr>;

    IngestPartition(
        const StoragePtr & target_table_,
        const StoragePtr & source_table_,
        const ASTPtr & partition_,
        const Names & column_names_,
        const Names & key_names_,
        const ContextPtr & context_);

    void ingestPartition();
    void ingestPartitionFromRemote(const String & source_replica, const String & source_database, const String & source_table);

private:

    ASTPtr getDefaultFilter(const String & column_name);
    String generateFilterString();
    String generateRemoteQuery(const String & source_database, const String & source_table, const String & partition_id, const Strings & column_lists);
    IngestParts generateIngestParts(MergeTreeData & data, const MergeTreeData::DataPartsVector & parts);

    // ------ Utilities for Ingest partition -------
    String getMapKey(const String & map_col_name, const String & map_implicit_name);
    std::optional<NameAndTypePair> tryGetMapColumn(const StorageInMemoryMetadata & data, const String & col_name);
    void writeNewPart(const StorageInMemoryMetadata & data, const IngestPartition::IngestSources & src_blocks, BlockOutputStreamPtr & output, Names & column_names);
    Names getOrderedKeys(const StorageInMemoryMetadata & data);
    void checkIngestColumns(const StorageInMemoryMetadata & data, bool & has_map_implicite_key);
    void checkColumnStructure(const StorageInMemoryMetadata & target_data, const StorageInMemoryMetadata & src_data, const Names & column_names);
    int compare(Columns & target_key_cols, Columns & src_key_cols, size_t n, size_t m);
    void checkPartitionRows(MergeTreeData::DataPartsVector & parts, const Settings & settings, const String & table_type, bool check_compact_map);
    std::vector<Names> groupColumns(const Settings & settings);
    /**
     * Outer join the target_block with the src_blocks
     *
     * @param column_names The columns that going to be ingested in src_blocks
     * @param ordered_key_names The join keys
     */
    Block blockJoinBlocks(MergeTreeData & data, Block & target_block, const IngestPartition::IngestSources & src_blocks, const Names & column_names, const Names & ordered_key_names);

    /// Perform outer join, ingest the specified columns.
    void ingestPart(MergeTreeData & data, const IngestPartPtr & ingest_part, const IngestPartition::IngestSources & src_blocks,
                const Names & ingest_column_names, const Names & ordered_key_names, const Names & all_columns,
                const Settings & settings);
    void ingestion(MergeTreeData & data, const IngestParts & parts_to_ingest, const IngestPartition::IngestSources & src_blocks,
                   const Names & ingest_column_names, const Names & ordered_key_names, const Names & all_columns, const Settings & settings);

    StoragePtr target_table;
    StoragePtr source_table;
    ASTPtr partition;
    Names column_names;
    Names key_names;
    ContextPtr context;
    Poco::Logger * log;
};

}
