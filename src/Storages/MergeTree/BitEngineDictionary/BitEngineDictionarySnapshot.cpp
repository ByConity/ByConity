
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDictionarySnapshot.h>
#include <Columns/ColumnBitMap64.h>

namespace DB
{

BitEngineDictionarySnapshot::BitEngineDictionarySnapshot(const BitEngineDictionary & bitengine_dict)
{
    dict_name = bitengine_dict.dict_name;
    shard_base_offset = bitengine_dict.shard_base_offset;

    if (!bitengine_dict.key_column.column)
        throw Exception("BitEngine cannot get snapshot since the bitengine_dict " + dict_name + " is empty", ErrorCodes::LOGICAL_ERROR);

    key_column_ptr = checkAndGetColumn<ColumnUInt64>(bitengine_dict.key_column.column.get());
    key_column_size = key_column_ptr->size();
    LOG_TRACE(&Poco::Logger::get("BitEngineDictionarySnapshot"), "A new snapshot: {}, size={}", dict_name, key_column_size);
}


BitEngineDictionarySnapshot & BitEngineDictionarySnapshot::operator=(const BitEngineDictionarySnapshot & snapshot)
{
    if (this == &snapshot)
        return *this;

    dict_name = snapshot.dict_name;
    shard_base_offset = snapshot.shard_base_offset;

    if (!snapshot.key_column_ptr)
        throw Exception("BitEngine cannot copy the snapshot " + snapshot.dict_name + " since it's empty", ErrorCodes::LOGICAL_ERROR);

    key_column_ptr = snapshot.key_column_ptr;
    key_column_size = snapshot.key_column_size;

    return *this;
}

BitEngineDictionarySnapshot::BitEngineDictionarySnapshot(const BitEngineDictionarySnapshot & snapshot)
{
    if (this == &snapshot)
        return;

    dict_name = snapshot.dict_name;
    shard_base_offset = snapshot.shard_base_offset;

    if (!snapshot.key_column_ptr)
        throw Exception("BitEngine cannot copy the snapshot " + snapshot.dict_name + " since it's not exists.", ErrorCodes::LOGICAL_ERROR);

    key_column_ptr = snapshot.key_column_ptr;
    key_column_size = snapshot.key_column_size;
}

void BitEngineDictionarySnapshot::writeDataToBuffer(std::pair<UInt64, UInt64> & offsets_range, WriteBuffer & out)
{
    /// keys size in incremental bitengine_dict data
    size_t rows, start_column_offset;
    if (std::numeric_limits<UInt64>::max() == offsets_range.first &&
        offsets_range.first == offsets_range.second)  /// max_offset means whole bitengine_dict
    {
        rows = key_column_size;
        start_column_offset = 0;
    }
    else if (std::numeric_limits<UInt64>::max() == offsets_range.second)
    {
        rows = key_column_size - offsets_range.first;  // [first, column_size)
        start_column_offset = offsets_range.first;
    }
    else
    {
        rows = offsets_range.second - offsets_range.first; // [first, second)
        start_column_offset = offsets_range.first;
    }

    if (key_column_size < start_column_offset + rows)
    {
        rows = key_column_size - start_column_offset;
        LOG_ERROR(&Poco::Logger::get("BitEngineDictionarySnapshot"), "There is no enough bitengine_dict data, rows shrink to {}", rows);
    }

    writeVarUInt(start_column_offset, out); /// the first column offset in  increment
    writeVarUInt(rows, out);
    // LOG_DEBUG(&Logger::get("BitEngineDictionarySnapshot"), "start=" << start_column_offset << ", rows=" << rows);
    // LOG_DEBUG(&Logger::get("BitEngineDictionarySnapshot"), "bitengine_dict size=" << key_column_ptr->size());

    /// write all keys, [start_column_offset, start_column_offset + rows)
    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&out](ISerialization::SubstreamPath) -> WriteBuffer * { return &out;  };
    settings.position_independent_encoding = false;
    ISerialization::SerializeBinaryBulkStatePtr key_state;
    auto key_column_type = std::make_shared<DataTypeUInt64>();
    key_column_type->getDefaultSerialization()->serializeBinaryBulkStatePrefix(settings, key_state);
    key_column_type->getDefaultSerialization()->serializeBinaryBulkWithMultipleStreams(*key_column_ptr, start_column_offset , rows, settings, key_state);
    key_column_type->getDefaultSerialization()->serializeBinaryBulkStateSuffix(settings, key_state);

    LOG_TRACE(&Poco::Logger::get("BitEngineDictionarySnapshot"), "{} has writen {} bitengine_dict pairs, start={}", dict_name, rows, start_column_offset);
}

inline UInt64 BitEngineDictionarySnapshot::decodeNumber(UInt64 number)
{
    auto pos = number - shard_base_offset;
    if (pos >= key_column_size)
        throw Exception("BitEngine cannot decode number " + std::to_string(number) + " because its position in snapshot is out of bound: "
            + std::to_string(pos) + " >= " + std::to_string(key_column_size) + "(size). First check whether you have properly "
            + "use distributed_perfect_shard settings. Second check whether snapshot is up-to-date", ErrorCodes::LOGICAL_ERROR);
    return key_column_ptr->getUInt(pos);
}

BitMap64 BitEngineDictionarySnapshot::decodeBitmap(const BitMap64 & bitmap)
{
    BitMap64 res_bitmap;
    PODArray<UInt64> res_array;
    res_array.reserve(4096);
    size_t cnt = 0;

    for (auto iter = bitmap.begin(); iter != bitmap.end(); ++iter)
    {
        UInt64 num = *iter;

        res_array.push_back(decodeNumber(num));
        if (++cnt == 4096)
        {
            res_bitmap.addMany(res_array.size(), res_array.data());
            res_array.clear();
            cnt = 0;
        }
    }
    res_bitmap.addMany(cnt, res_array.data());
    return res_bitmap;
}

/// ensure that the column is ColumnBitMap64
ColumnPtr BitEngineDictionarySnapshot::decodeColumn(const IColumn & column)
{
    const auto & column_bitmap = dynamic_cast<const ColumnBitMap64 &>(column);
    auto output_column = ColumnBitMap64::create();

    for (size_t i  = 0; i < column_bitmap.size(); ++i)
    {
        const auto & bitmap = column_bitmap.getBitMapAt(i);
        output_column->insert(decodeBitmap(bitmap));
    }
    return output_column;
}

/// ensure that the column is Numeric columns, it's designed for NativeInteger type.
ColumnPtr BitEngineDictionarySnapshot::decodeNonBitEngineColumn(const IColumn & column)
{
    auto output_column = ColumnUInt64::create();

    for (size_t i = 0; i < column.size(); ++i)
    {
        UInt64 num = column.getUInt(i);
        output_column->insert(decodeNumber(num));
    }

    return output_column;
}


}
