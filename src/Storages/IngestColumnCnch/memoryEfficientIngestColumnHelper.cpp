#include <Storages/IngestColumnCnch/memoryEfficientIngestColumnHelper.h>
#include <Storages/IngestColumnCnch/IngestColumnHelper.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/StorageCloudMergeTree.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/MapHelpers.h>
#include <common/range.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace IngestColumn
{

StringRef placeKeysInPool(const size_t row, const Columns & key_columns, StringRefs & keys, Arena & pool)
{
    const auto keys_size = key_columns.size();
    size_t sum_keys_size{};

    const char * block_start = nullptr;
    for (size_t j = 0; j < keys_size; ++j)
    {
        keys[j] = key_columns[j]->serializeValueIntoArena(row, pool, block_start);
        sum_keys_size += keys[j].size;
    }

    auto key_start = block_start;
    for (size_t j = 0; j < keys_size; ++j)
    {
        keys[j].data = key_start;
        key_start += keys[j].size;
    }

    return {block_start, sum_keys_size};
}


Columns getColumnsFromBlock(const Block & block, const Strings & names)
{
    Columns res;
    std::for_each(names.begin(), names.end(),
        [& res, & block] (const String & name)
        {
            res.push_back(block.getByName(name).column);
        }
    );

    return res;
}

DataTypes getDataTypesFromBlock(const Block & block, const Strings & names)
{
    DataTypes res;
    std::for_each(names.begin(), names.end(),
        [& res, & block] (const String & name)
        {
            res.push_back(block.getByName(name).type);
        }
    );

    return res;
}

PartMap PartMap::buildPartMap(const MergeTreeDataPartsVector & source_parts)
{
    PartMap res{source_parts};
    return res;
}

PartMap::PartMap(const MergeTreeDataPartsVector & source_parts)
    :id_map{}
{
    for (uint32_t i = 0; i < source_parts.size(); ++i)
        id_map.insert(std::make_pair(i, source_parts[i].get()));
}

MergeTreeDataPartPtr PartMap::getPart(UInt32 part_id) const
{
    auto & id_index = id_map.template get<id_tag>();

    container_type::iterator it = id_index.find(part_id);
    if (it == id_index.end())
        throw Exception("Part id is not found in part map ", ErrorCodes::LOGICAL_ERROR);
    else
        return it->second->shared_from_this();
}

UInt32 PartMap::getPartID(const MergeTreeDataPartPtr & part) const
{
    auto & ptr_index = id_map.template get<ptr_tag>();
    //container_type::iterator it = ptr_index.find(part.get());
    auto it = ptr_index.find(part.get());

    if (it == ptr_index.end())
        throw Exception("Part is not found in part map ", ErrorCodes::LOGICAL_ERROR);

    return it->first;
}

HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> combineHashmaps(
    std::vector<HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash>> && hashmap_per_threads
)
{
    if (hashmap_per_threads.empty())
        return {};

    HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> res{std::move(hashmap_per_threads[0])};

    for (size_t i = 1; i < hashmap_per_threads.size(); ++i)
    {
        HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> current{std::move(hashmap_per_threads[i])};
        for (auto & p : current)
            res.insert(p.value);
    }

    return res;
}

void buildHashTableFromBlock(
    const UInt32 part_id,
    const Block & block,
    const Strings & ordered_key_names,
    HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> & hash_table,
    size_t bucket_num,
    Arena & keys_pool,
    size_t number_of_buckets
)
{
    StringRefHash hasher;
    const auto keys_size = ordered_key_names.size();
    StringRefs keys(keys_size);
    Columns key_column_ptrs;
    for (auto & key_name : ordered_key_names)
        key_column_ptrs.push_back(block.getByName(key_name).column);

    const auto rows = block.rows();
    for (const auto row_idx : collections::range(0, rows))
    {
        const auto key = placeKeysInPool(row_idx, key_column_ptrs, keys, keys_pool);

        size_t hash_value = hasher(key);
        if ((hash_value % number_of_buckets) != bucket_num)
        {
            keys_pool.rollback(key.size);
            continue;
        }
        const auto pair = hash_table.insert({key, KeyInfo{part_id, 0}});
        if (!pair.second)
            throw Exception("There is duplication in source data, key: " + key.toString(), ErrorCodes::LOGICAL_ERROR);
    }
}

void probeHashTableFromBlock(
    const UInt32 part_id,
    const Block & block,
    const Strings & ordered_key_names,
    HashMapWithSavedHash<StringRef, KeyInfo, StringRefHash> & hashmap,
    size_t bucket_num,
    Arena & keys_pool,
    size_t number_of_buckets,
    std::unordered_map<UInt32, std::set<UInt32>> & target_part_index)
{
    StringRefHash hasher;
    const auto keys_size = ordered_key_names.size();
    StringRefs keys(keys_size);
    Columns key_column_ptrs;
    for (auto & key_name : ordered_key_names)
        key_column_ptrs.push_back(block.getByName(key_name).column);

    const auto rows = block.rows();
    for (const auto row_idx : collections::range(0, rows))
    {
        const auto key = placeKeysInPool(row_idx, key_column_ptrs, keys, keys_pool);
        size_t hash_value = hasher(key);
        if ((hash_value % number_of_buckets) != bucket_num)
        {
            keys_pool.rollback(key.size);
            continue;
        }

        auto it = hashmap.find(key);
        if (it != hashmap.end())
        {
            KeyInfo & key_info = it->getMapped();
            UInt8 exist = key_info.getExistStatus();
            if (exist)
                throw Exception("There is duplication in target data, key: " + key.toString(), ErrorCodes::LOGICAL_ERROR);
            else
            {
                target_part_index[part_id].insert(key_info.getPartID());
                key_info.updateExistStatus(1u);
            }
        }

        /// free memory allocated for the key
        keys_pool.rollback(key.size);
    }
}

TargetPartData::TargetPartData(
    std::vector<std::pair<Block, std::vector<FieldVector>>> data_,
    Strings ordered_key_names_,
    Strings ingest_column_names_,
    Block target_header_,
    const std::vector<UInt8> & ingest_column_compression_statuses_,
    const bool ingest_default_column_value_if_not_provided_)
    : target_header{std::move(target_header_)}
    , ordered_key_names{std::move(ordered_key_names_)}
    , ingest_column_names{std::move(ingest_column_names_)}
    , ingest_column_compression_statuses{ingest_column_compression_statuses_}
    , ingest_default_column_value_if_not_provided{ingest_default_column_value_if_not_provided_}
    , data{std::move(data_)}
{}

bool TargetPartData::updateWithSourceBlock(const Block & source_block)
{
    size_t source_rows = source_block.rows();
    size_t current_source_row_idx = 0;
    while (current_block_idx < data.size())
    {
        std::pair<Block, std::vector<FieldVector>> & target_block_data = data[current_block_idx];
        size_t target_rows = target_block_data.first.rows();

        std::tie(current_source_row_idx, current_row_in_block_idx) =
            updateTargetBlockWithSourceBlock(
                target_block_data,
                source_block,
                current_source_row_idx,
                current_row_in_block_idx,
                ordered_key_names,
                ingest_column_names);

        if (current_row_in_block_idx == target_rows)
        {
            ++current_block_idx;
            current_row_in_block_idx = 0;
        }

        if (current_source_row_idx == source_rows)
            return false;
    }

    return true;
}

void TargetPartData::clearUpdateOffset()
{
    current_block_idx = 0;
    current_row_in_block_idx = 0;
}

const std::vector<std::pair<Block, std::vector<FieldVector>>> & TargetPartData::getData() const
{
    return data;
}

void TargetPartData::reset()
{
    std::vector<std::pair<Block, std::vector<FieldVector>>>().swap(data);
    current_block_idx = 0;
    current_row_in_block_idx = 0;
}

std::pair<size_t, size_t> updateTargetBlockWithSourceBlock(
    std::pair<Block, std::vector<FieldVector>> & target_data,
    const Block & source_block,
    size_t current_source_row_idx,
    size_t current_target_row_idx,
    const Strings & ordered_key_names,
    const Strings & ingest_column_names)
{
    const Block & target_block = target_data.first;
    std::vector<FieldVector> & update_data = target_data.second;
    size_t i = current_source_row_idx;
    size_t j = current_target_row_idx;
    size_t source_size = source_block.rows();
    size_t target_size = target_block.rows();
    Columns source_key_columns = getColumnsFromBlock(source_block, ordered_key_names);
    Columns target_key_columns = getColumnsFromBlock(target_block, ordered_key_names);
    Columns source_value_columns = getColumnsFromBlock(source_block, ingest_column_names);
    while (i < source_size && j < target_size)
    {
        auto order = compare(source_key_columns, target_key_columns, i, j);
        if (order < 0)
            ++i;
        else if (order > 0)
            ++j;
        else
        {
            for (size_t ingest_col_idx = 0; ingest_col_idx < ingest_column_names.size(); ++ingest_col_idx)
                source_value_columns[ingest_col_idx]->get(i, update_data[ingest_col_idx][j]);

            ++i;
            ++j;
        }
    }

    return std::make_pair(i, j);
}

KeyInfo::KeyInfo(UInt32 part_id, UInt8 exist_status)
    : data{(part_id * 2) + exist_status}
{}

UInt32 KeyInfo::getPartID() const
{
    return (data & PART_ID_MASK) / 2;
}

UInt8 KeyInfo::getExistStatus() const
{
    return data & EXIST_STATUS_MASK;
}

void KeyInfo::updateExistStatus(UInt8 new_exist_status)
{
    UInt32 temp = new_exist_status;
    data &= PART_ID_MASK;
    data |= temp;
}

bool KeyInfo::operator == (const KeyInfo & other) const
{
    return other.data == this->data;
}

void KeyInfo::checkNumberOfParts(const MergeTreeDataPartsVector & parts)
{
    constexpr size_t max_number_of_parts = ((std::numeric_limits<uint32_t>::max() -1)/ 2);
    if (parts.size() > max_number_of_parts)
        throw Exception("too many source part, max value is " + std::to_string(max_number_of_parts), ErrorCodes::LOGICAL_ERROR);
}

BlocksList makeBlockListFromUpdateData(const TargetPartData & target_part_data)
{
    BlocksList res;
    const std::vector<std::pair<Block, std::vector<FieldVector>>> & data =
        target_part_data.getData();

    const std::vector<UInt8> ingest_column_compression_statuses = target_part_data.ingest_column_compression_statuses;

    const Strings & ingest_column_names =
        target_part_data.ingest_column_names;
    const Block & header = target_part_data.target_header;
    Block sample;

    std::for_each(ingest_column_names.begin(), ingest_column_names.end(),
        [& sample, & header] (const String & name)
        {
            sample.insert(header.getByName(name));
        }
    );

    std::for_each(data.begin(), data.end(),
        [&sample, & res, & ingest_column_names, & ingest_column_compression_statuses, & target_part_data]
        (const std::pair<Block, std::vector<FieldVector>> & p)
        {
            Block temp_block = sample.cloneEmpty();
            MutableColumns block_columns = temp_block.mutateColumns();
            Columns target_columns = getColumnsFromBlock(p.first, ingest_column_names);
            const std::vector<FieldVector> & target_data = p.second;
            DataTypes block_data_types = getDataTypesFromBlock(temp_block, ingest_column_names);
            for (size_t col_idx = 0; col_idx < block_columns.size(); ++col_idx)
            {
                const FieldVector & update_data_for_column = target_data[col_idx];
                MutableColumnPtr & final_column = block_columns[col_idx];
                ColumnPtr & target_column = target_columns[col_idx];

                /// set compression flag
                if (ingest_column_compression_statuses[col_idx])
                    const_cast<IDataType*>(block_data_types[col_idx].get())->setFlags(TYPE_COMPRESSION_FLAG);

                for (size_t row_idx = 0; row_idx < update_data_for_column.size(); ++row_idx)
                {
                    if (update_data_for_column[row_idx].getType() == Field::Types::Null)
                    {
                        if (target_part_data.ingest_default_column_value_if_not_provided)
                            final_column->insertDefault();
                        else
                            final_column->insertFrom(*target_column, row_idx);
                    }
                    else
                        final_column->insert(update_data_for_column[row_idx]);
                }

            }

            temp_block.setColumns(std::move(block_columns));
            res.push_back(std::move(temp_block));
        }
    );
    return res;
}

Names getColumnsFromSourceTableForInsertNewPart(
    const Names & ordered_key_names,
    const Names & ingest_column_names,
    const StorageMetadataPtr & source_meta_data_ptr
)
{
    Names res = ordered_key_names;

    std::copy(
        ingest_column_names.begin(),
        ingest_column_names.end(),
        std::back_inserter(res));

    for (auto & partition_key : source_meta_data_ptr->getColumnsRequiredForPartitionKey())
    {
        auto it = std::find(res.begin(), res.end(), partition_key);
        if (it == res.end())
            res.push_back(partition_key);
    }
    return res;
}

void writeBlock(
    const Block & src_block,
    const Strings & ordered_key_names,
    const size_t bucket_num,
    const HashMapWithSavedHash<StringRef, IngestColumn::KeyInfo, StringRefHash> & source_key_map,
    std::mutex & new_part_output_mutex,
    const size_t number_of_buckets,
    IBlockOutputStream & new_part_output,
    const StorageMetadataPtr & target_meta_data_ptr,
    LoggerPtr log)
{
    Arena temporary_keys_pool;
    StringRefHash hasher;
    const auto keys_size = ordered_key_names.size();
    StringRefs keys(keys_size);
    Columns key_column_ptrs;
    for (auto & key_name : ordered_key_names)
        key_column_ptrs.push_back(src_block.getByName(key_name).column);

    const size_t rows = src_block.rows();
    IColumn::Filter filter(rows);
    size_t target_block_size = 0;
    for (size_t row_idx = 0; row_idx < rows; ++row_idx)
    {
        const auto key = IngestColumn::placeKeysInPool(row_idx, key_column_ptrs, keys, temporary_keys_pool);
        size_t hash_value = hasher(key);
        if ((hash_value % number_of_buckets) != bucket_num)
            filter[row_idx] = 0;
        else
        {
            auto it = source_key_map.find(key);
            if (it == source_key_map.end())
                throw Exception("key in source data not found in source map, program mistake, key: " +
                    key.toString(),  ErrorCodes::LOGICAL_ERROR);

            filter[row_idx] = (it->getMapped().getExistStatus() == 0u);
        }
        temporary_keys_pool.rollback(key.size);
        target_block_size += filter[row_idx];
    }

    struct MapImplicitColumnData
    {
        /// Handle map implicit column
        // Source block can have multiple column for each key's value, but target block have to combine into 1
        // map key -> column
        std::unordered_map<String, ColumnPtr> src_value_columns;
        DataTypePtr target_data_type;
        DataTypePtr key_data_type;
    };

    std::unordered_map<String, MapImplicitColumnData> map_implicit_columns;
    Block target_block;
    std::for_each(src_block.begin(), src_block.end(),
        [& target_meta_data_ptr, & target_block, & filter, & map_implicit_columns]
            (const ColumnWithTypeAndName & col_with_type_name)
        {
            std::optional<NameAndTypePair> map_col = tryGetMapColumn(*target_meta_data_ptr,
                col_with_type_name.name);

            if (map_col)
            {
                if (!map_implicit_columns.count(map_col->name))
                {
                    const auto & map_col_type = typeid_cast<const DataTypeMap &>(*(map_col->type));
                    map_implicit_columns.insert(
                        std::make_pair(map_col->name,
                            MapImplicitColumnData{
                                {},
                                map_col->type,
                                map_col_type.getKeyType()}));
                }

                const String map_key = parseKeyNameFromImplicitColName(col_with_type_name.name, map_col->name);
                map_implicit_columns.at(map_col->name).src_value_columns.insert(
                    std::make_pair(map_key, col_with_type_name.column->filter(filter, 0)));
            }
            else
                target_block.insert(
                    ColumnWithTypeAndName(
                        col_with_type_name.column->filter(filter, 0),
                        col_with_type_name.type,
                        col_with_type_name.name));
        });

    std::for_each(map_implicit_columns.begin(), map_implicit_columns.end(),
        [&target_block, target_block_size] (const std::pair<String, MapImplicitColumnData> & col)
        {
            target_block.insert(
                ColumnWithTypeAndName(
                    IngestColumn::combineMapImplicitColumns(col.second.key_data_type,
                        col.second.target_data_type, target_block_size,
                        col.second.src_value_columns),
                    col.second.target_data_type,
                    col.first));
        });

    auto write_lock = std::lock_guard<std::mutex>(new_part_output_mutex);

    if (target_block.rows() > 0)
    {
        LOG_DEBUG(log, "write data for bucket {} block size {}", bucket_num, target_block.rows());
        new_part_output.write(target_block);
    }
}

IngestColumn::TargetPartData readTargetDataForUpdate(
    IBlockInputStream & in,
    const Strings & ordered_key_names,
    const Strings & ingest_column_names,
    const std::vector<UInt8> & value_column_compression_statuses,
    bool ingest_default_column_value_if_not_provided)
{
    Block header = in.getHeader();
    std::vector<std::pair<Block, std::vector<FieldVector>>> res;
    in.readPrefix();

    while (Block block = in.read())
    {
        size_t rows = block.rows();
        std::vector<FieldVector> columns_value(ingest_column_names.size(), FieldVector(rows));
        res.push_back(std::make_pair(std::move(block), std::move(columns_value)));
    }

    in.readSuffix();
    return {res, ordered_key_names, ingest_column_names, std::move(header), value_column_compression_statuses, ingest_default_column_value_if_not_provided};
}

void updateTargetDataWithSourceData(IBlockInputStream & source_in,
    IngestColumn::TargetPartData & target_part_data)
{
    /// Join 2 parts, the data in target_part_data and source_in (data from a single source part) are both sorted
    /// so the join will end
    source_in.readPrefix();

    /// reset offset before join with new part because data is not sorted over parts, only within parts
    target_part_data.clearUpdateOffset();
    while (Block block = source_in.read())
    {
        bool reach_the_end_of_target_data = target_part_data.updateWithSourceBlock(block);
        if (reach_the_end_of_target_data)
            break;
    }

    source_in.readSuffix();
}


MutableColumnPtr combineMapImplicitColumns(const DataTypePtr & key_data_type,
    const DataTypePtr & target_data_type, size_t rows,
    const std::unordered_map<String, ColumnPtr> & src_value_columns)
{
    MutableColumnPtr res_column = target_data_type->createColumn();
    for (size_t row_idx = 0; row_idx < rows; ++row_idx)
    {
        Map map;
        std::for_each(src_value_columns.begin(),
            src_value_columns.end(),
            [& map, row_idx, & key_data_type] (const std::pair<String, ColumnPtr> & source_col)
            {
                if (source_col.second->isNullAt(row_idx))
                    return;

                Field field;
                source_col.second->get(row_idx, field);
                map.push_back(std::make_pair(key_data_type->stringToVisitorField(source_col.first), field));
            });
        res_column->insert(map);
    }

    return res_column;
}


} /// end namespace IngestColumn

} /// end namespace DB
