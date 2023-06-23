#include <Storages/IngestColumnCnch/IngestColumnHelper.h>
#include <Catalog/DataModelPartWrapper.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <DataTypes/MapHelpers.h>
#include <Common/Exception.h>
#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Parsers/queryToString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DUPLICATE_COLUMN;
}

int compare(Columns & target_key_cols, Columns & src_key_cols, size_t n, size_t m)
{
    for (size_t i = 0; i < target_key_cols.size(); ++i)
    {
        auto order = target_key_cols[i]->compareAt(n, m, *(src_key_cols[i]), 1);
        if (order != 0)
            return order;
    }
    return 0;
}

size_t countRows(const MergeTreeDataPartsVector & parts)
{
    size_t row_count = 0;
    for (auto & part : parts)
        row_count += part->rows_count;

    return row_count;
}

void checkColumnStructure(const StorageInMemoryMetadata & target_data, const StorageInMemoryMetadata & src_data, const Names & names)
{
    for (const auto & col_name : names)
    {
        const auto & target = target_data.getColumns().getColumnOrSubcolumn(ColumnsDescription::GetFlags::AllPhysical, col_name);
        const auto & src = src_data.getColumns().getColumnOrSubcolumn(ColumnsDescription::GetFlags::AllPhysical, col_name);

        if (target.name != src.name)
            throw Exception("Column structure mismatch, found different names of column " + backQuoteIfNeed(col_name),
                            ErrorCodes::BAD_ARGUMENTS);

        if (!target.type->equals(*src.type))
            throw Exception("Column structure mismatch, found different types of column " + backQuoteIfNeed(col_name),
                            ErrorCodes::BAD_ARGUMENTS);
    }
}

Names getOrderedKeys(const Names & names_to_order, const StorageInMemoryMetadata & meta_data)
{
    auto ordered_keys = meta_data.getColumnsRequiredForPrimaryKey();

    if (names_to_order.empty())
    {
        return ordered_keys;
    }
    else
    {
        for (auto & key : names_to_order)
        {
            bool found = false;
            for (auto & table_key : ordered_keys)
            {
                if (table_key == key)
                    found = true;
            }

            if (!found)
                throw Exception("Some given keys are not part of the table's primary key, please check!", ErrorCodes::BAD_ARGUMENTS);
        }

        // get reorderd ingest key
        Names res;
        for (size_t i = 0; i < ordered_keys.size(); ++i)
        {
            for (auto & key : names_to_order)
            {
                if (key == ordered_keys[i])
                    res.push_back(key);
            }
        }

        for (size_t i = 0; i < ordered_keys.size(); ++i)
        {
            if (i < res.size() && res[i] != ordered_keys[i])
                throw Exception("Reordered ingest key must be a prefix of the primary key.", ErrorCodes::BAD_ARGUMENTS);
        }

        return res;
    }
}

void checkIngestColumns(const Strings & column_names, const StorageInMemoryMetadata & meta_data, bool & has_map_implicite_key)
{
    if (!meta_data.getColumns().getMaterialized().empty())
        throw Exception("There is materialized column in table which is not allowed!", ErrorCodes::BAD_ARGUMENTS);

    for (auto & primary_key : meta_data.getColumnsRequiredForPrimaryKey())
    {
        for (auto & col_name : column_names)
        {
            if (col_name == primary_key)
                throw Exception("Column " + backQuoteIfNeed(col_name) + " is part of the table's primary key which is not allowed!", ErrorCodes::BAD_ARGUMENTS);
        }
    }

    for (auto & partition_key : meta_data.getColumnsRequiredForPartitionKey())
    {
        for (auto & col_name : column_names)
        {
            if (col_name == partition_key)
                throw Exception("Column " + backQuoteIfNeed(col_name) + " is part of the table's partition key which is not allowed!", ErrorCodes::BAD_ARGUMENTS);
        }
    }

    std::unordered_set<String> all_columns;
    for (const auto & col_name : column_names)
    {
        /// Check for duplicates
        if (!all_columns.emplace(col_name).second)
            throw Exception("Ingest duplicate column " + backQuoteIfNeed(col_name), ErrorCodes::DUPLICATE_COLUMN);

        if (isMapImplicitKeyNotKV(col_name))
        {
            has_map_implicite_key = true;
            continue;
        }

        if (meta_data.getColumns().get(col_name).type->isMap())
            throw Exception("Ingest whole map column " + backQuoteIfNeed(col_name) +
                            " is not supported, you can specify a map key.", ErrorCodes::BAD_ARGUMENTS);
    }
}

String getMapKey(const String & map_col_name, const String & map_implicit_name)
{
    String prefix = String("__") + map_col_name + "__";
    size_t key_len = map_implicit_name.size() - prefix.size();
    return map_implicit_name.substr(prefix.size(), key_len);
}

std::optional<NameAndTypePair> tryGetMapColumn(const StorageInMemoryMetadata & meta_data, const String & col_name)
{
    if (!meta_data.getColumns().hasPhysical(col_name) && isMapImplicitKey(col_name))
    {
        auto & columns = meta_data.getColumns();
        for (auto & nt : (columns.getOrdinary()))
        {
            if (nt.type->isMap())
            {
                if (nt.type->isMapKVStore() ? (col_name == nt.name + ".key" || col_name == nt.name + ".value")
                                            : startsWith(col_name, getMapKeyPrefix(nt.name)))
                {
                    return nt;
                }
            }
        }
    }

    return std::nullopt;
}

MergeTreeMutableDataPartPtr createEmptyTempPart(
    MergeTreeMetaBase & data,
    const MergeTreeDataPartPtr & part,
    const Names & ingest_column_names,
    ReservationPtr & reserved_space,
    const ContextPtr & context)
{
    auto new_part_info = part->info;
    new_part_info.level += 1;
    new_part_info.hint_mutation = new_part_info.mutation;
    new_part_info.mutation = context->getCurrentTransactionID().toUInt64();

    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part->name, reserved_space->getDisk(), 0);

    auto new_partial_part = data.createPart(part->name, MergeTreeDataPartType::WIDE,
        new_part_info, single_disk_volume, "tmp_mut_" + part->name,
        nullptr, IStorage::StorageLocation::AUXILITY);

    new_partial_part->uuid = part->uuid;
    new_partial_part->is_temp = true;
    new_partial_part->ttl_infos = part->ttl_infos;
    new_partial_part->versions = part->versions;

    new_partial_part->index_granularity_info = part->index_granularity_info;
    new_partial_part->setColumns(part->getColumns().filter(ingest_column_names));
    new_partial_part->partition.assign(part->partition);
    new_partial_part->columns_commit_time = part->columns_commit_time;
    new_partial_part->mutation_commit_time = part->mutation_commit_time;
    if (data.isBucketTable())
        new_partial_part->bucket_number = part->bucket_number;

    new_partial_part->checksums_ptr = std::make_shared<MergeTreeData::DataPart::Checksums>();

    return new_partial_part;
}

void finalizeTempPart(
    const MergeTreeDataPartPtr & ingest_part,
    const MergeTreeMutableDataPartPtr & new_partial_part,
    const CompressionCodecPtr & codec)
{
    auto disk = new_partial_part->volume->getDisk();
    auto new_part_checksums_ptr = new_partial_part->getChecksums();

    if (new_partial_part->uuid != UUIDHelpers::Nil)
    {
        auto out = disk->writeFile(new_partial_part->getFullRelativePath() + IMergeTreeDataPart::UUID_FILE_NAME, {.buffer_size = 4096});
        HashingWriteBuffer out_hashing(*out);
        writeUUIDText(new_partial_part->uuid, out_hashing);
        new_part_checksums_ptr->files[IMergeTreeDataPart::UUID_FILE_NAME].file_size = out_hashing.count();
        new_part_checksums_ptr->files[IMergeTreeDataPart::UUID_FILE_NAME].file_hash = out_hashing.getHash();
    }

    {
        /// Write file with checksums.
        auto out_checksums = disk->writeFile(fs::path(new_partial_part->getFullRelativePath()) / "checksums.txt", {.buffer_size = 4096});
        new_part_checksums_ptr->versions = new_partial_part->versions;
        new_part_checksums_ptr->write(*out_checksums);
    } /// close fd

    {
        auto out = disk->writeFile(new_partial_part->getFullRelativePath() + IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME, {.buffer_size = 4096});
        DB::writeText(queryToString(codec->getFullCodecDesc()), *out);
    }

    {
        /// Write a file with a description of columns.
        auto out_columns = disk->writeFile(fs::path(new_partial_part->getFullRelativePath()) / "columns.txt", {.buffer_size = 4096});
        new_partial_part->getColumns().writeText(*out_columns);
    } /// close fd

    new_partial_part->rows_count = ingest_part->rows_count;
    new_partial_part->index_granularity = ingest_part->index_granularity;
    new_partial_part->index = ingest_part->getIndex();
    new_partial_part->minmax_idx = ingest_part->minmax_idx;
    new_partial_part->modification_time = time(nullptr);
    new_partial_part->loadProjections(false, false);
    new_partial_part->setBytesOnDisk(
        MergeTreeData::DataPart::calculateTotalSizeOnDisk(new_partial_part->volume->getDisk(), new_partial_part->getFullRelativePath()));
    new_partial_part->default_codec = codec;
}

void updateTempPartWithData(
    MergeTreeMutableDataPartPtr & new_partial_part,
    const MergeTreeDataPartPtr & target_part,
    const BlockInputStreamPtr & data_block_input_stream,
    const StorageMetadataPtr & target_meta_data_ptr)
{
    auto compression_codec = target_part->default_codec;

    if (!compression_codec)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown codec for mutate part: {}", target_part->name);

    MergedColumnOnlyOutputStream out(
        new_partial_part,
        target_meta_data_ptr,
        data_block_input_stream->getHeader(),
        compression_codec,
        {},
        nullptr,
        target_part->index_granularity,
        &target_part->index_granularity_info
    );

    data_block_input_stream->readPrefix();
    out.writePrefix();
    while (Block block = data_block_input_stream->read())
        out.write(block);
    data_block_input_stream->readSuffix();

    auto changed_checksums = out.writeSuffixAndGetChecksums(new_partial_part, *(new_partial_part->getChecksums()));
    new_partial_part->checksums_ptr->add(std::move(changed_checksums));
    finalizeTempPart(target_part, new_partial_part, compression_codec);
}

}
