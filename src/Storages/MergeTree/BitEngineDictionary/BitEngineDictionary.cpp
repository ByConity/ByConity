#include <algorithm>
#include <future>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeBitMap64.h>
#include <Columns/ColumnBitMap64.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDictionary.h>
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDictionarySnapshot.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeSuffix.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageHaMergeTree.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/Macros.h>
#include <common/logger_useful.h>



namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int NOT_IMPLEMENTED;
}


void IncrementDictOffset::readIncrementOffset(ReadBuffer & in)
{
    readVarUInt(dict_offset, in);
}

void IncrementDictOffset::writeIncrementOffset(WriteBuffer & out) const
{
    writeVarUInt(dict_offset, out);
}

//void IncrementColumnOffset::readIncrementOffset(ReadBuffer & in)
//{
//    size_t size;
//    readVarUInt(size, in);
//    for (size_t i = 0; i < size; ++i)
//    {
//        size_t index;
//        readVarUInt(index, in);
//        IncrementDictOffset dict_offset;
//        dict_offset.readIncrementOffset(in);
//        increment_column_offset.emplace(index, dict_offset);
//    }
//}
//
//void IncrementColumnOffset::writeIncrementOffset(WriteBuffer & out)
//{
//    size_t size = increment_column_offset.size();
//    writeVarUInt(size, out);
//    for (auto it = increment_column_offset.begin(); it != increment_column_offset.end(); ++it)
//    {
//        writeVarUInt(it->first, out);
//        it->second.writeIncrementOffset(out);
//    }
//}

void IncrementOffset::readIncrementOffset(ReadBuffer & in)
{
    size_t dicts_size;
    readVarUInt(dicts_size, in);
    for (size_t i = 0; i < dicts_size; ++i)
    {
        String dict_name;
        readStringBinary(dict_name, in);
        IncrementDictOffset dict_offset;
        dict_offset.readIncrementOffset(in);
        increment_offset.emplace(dict_name, std::move(dict_offset));
    }
}

void IncrementOffset::writeIncrementOffset(WriteBuffer & out)
{
    size_t dicts_size = increment_offset.size();
    writeVarUInt(dicts_size, out);
    for (auto & entry : increment_offset)
    {
        writeStringBinary(entry.first, out);
        entry.second.writeIncrementOffset(out);
    }
}

IncrementDictData::IncrementDictData()
{
    key_column.type = std::make_shared<DataTypeUInt64>();
    value_column.type = std::make_shared<DataTypeUInt64>();
    key_column.name = "increment_dict_data_key";
    value_column.name = "increment_dict_data_value";
}

IncrementDictData & IncrementDictData::operator=(const IncrementDictData & dict_data)
{
    if (this == &dict_data)
        return *this;

    base_offset = dict_data.base_offset;

    size_t key_size = dict_data.key_column.column->size();
    size_t value_size = dict_data.value_column.column->size();

    key_column.column = dict_data.key_column.column->cloneResized(key_size);
    key_column.type = std::make_shared<DataTypeUInt64>();
    key_column.name = dict_data.key_column.name;

    value_column.column = dict_data.value_column.column->cloneResized(value_size);
    value_column.type = std::make_shared<DataTypeUInt64>();
    value_column.name = dict_data.value_column.name;

    return *this;
}

IncrementDictData::IncrementDictData(const IncrementDictData & dict_data)
{
    if (this == &dict_data)
        return;

    base_offset = dict_data.base_offset;

    size_t key_size = dict_data.key_column.column->size();
    size_t value_size = dict_data.value_column.column->size();

    key_column.column = dict_data.key_column.column->cloneResized(key_size);
    key_column.type = std::make_shared<DataTypeUInt64>();
    key_column.name = dict_data.key_column.name;

    value_column.column = dict_data.value_column.column->cloneResized(value_size);
    value_column.type = std::make_shared<DataTypeUInt64>();
    value_column.name = dict_data.value_column.name;
}

void IncrementDictData::readIncrementData(ReadBuffer & in)
{
    readVarUInt(base_offset, in);

    String key_type_name;
    size_t key_rows = 0;
    readStringBinary(key_type_name, in);
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();
    key_column.type = data_type_factory.get(key_type_name);
    readVarUInt(key_rows, in);

    //std::cout<<" read key_type_name: " << key_type_name << " -- key_rows: " << key_rows << std::endl;

    // read keys
    ColumnPtr key_read_column = key_column.type->createColumn();

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&in](const ISerialization::SubstreamPath&) -> ReadBuffer * { return &in;  };
    settings.avg_value_size_hint = 0;
    settings.position_independent_encoding = false;

    ISerialization::DeserializeBinaryBulkStatePtr key_state;
    ISerialization::SubstreamsCache key_cache;
    key_column.type->getDefaultSerialization()->deserializeBinaryBulkStatePrefix(settings, key_state);
    key_column.type->getDefaultSerialization()->deserializeBinaryBulkWithMultipleStreams(key_read_column, key_rows, settings, key_state, &key_cache);

    if (key_read_column->size() != key_rows)
    {
        throw Exception("Cannot read all data from Dictionary key file.", ErrorCodes::CANNOT_READ_ALL_DATA);
    }

    key_column.column = std::move(key_read_column);

     // read name and rows of values
    String value_type_name;
    size_t value_rows = 0;
    readStringBinary(value_type_name, in);
    value_column.type = data_type_factory.get(value_type_name);
    readVarUInt(value_rows, in);

    // read keys
    ColumnPtr value_read_column = value_column.type->createColumn();

    ISerialization::DeserializeBinaryBulkStatePtr value_state;
    ISerialization::SubstreamsCache value_cache;
    value_column.type->getDefaultSerialization()->deserializeBinaryBulkStatePrefix(settings, value_state);
    value_column.type->getDefaultSerialization()->deserializeBinaryBulkWithMultipleStreams(value_read_column, value_rows, settings, value_state, &value_cache);

    if (value_read_column->size() != value_rows)
    {
        throw Exception("Cannot read all data from Dictionary value file.", ErrorCodes::CANNOT_READ_ALL_DATA);
    }

    value_column.column = std::move(value_read_column);
}

void IncrementDictData::writeIncrementData(WriteBuffer & out)
{
    writeVarUInt(base_offset, out);
    // write name of keys
    writeStringBinary(key_column.type->getName(), out);
    // write size of keys
    writeVarUInt(key_column.column->size(), out);

    // write keys
    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&out](const ISerialization::SubstreamPath&) -> WriteBuffer * { return &out;  };
    settings.position_independent_encoding = false;
    settings.low_cardinality_max_dictionary_size = 0;

    ISerialization::SerializeBinaryBulkStatePtr key_state;
    key_column.type->getDefaultSerialization()->serializeBinaryBulkStatePrefix(settings, key_state);
    key_column.type->getDefaultSerialization()->serializeBinaryBulkWithMultipleStreams(*key_column.column, 0, 0, settings, key_state);
    key_column.type->getDefaultSerialization()->serializeBinaryBulkStateSuffix(settings, key_state);

    // write name of values
    writeStringBinary(value_column.type->getName(), out);
    // write size of values
    writeVarUInt(value_column.column->size(), out);
    // write values
    ISerialization::SerializeBinaryBulkStatePtr value_state;
    value_column.type->getDefaultSerialization()->serializeBinaryBulkStatePrefix(settings, value_state);
    value_column.type->getDefaultSerialization()->serializeBinaryBulkWithMultipleStreams(*value_column.column, 0, 0, settings, value_state);
    value_column.type->getDefaultSerialization()->serializeBinaryBulkStateSuffix(settings, value_state);
}

//void IncrementColumnData::readIncrementData(ReadBuffer & in)
//{
//    size_t size;
//    readVarUInt(size, in);
//    for (size_t i = 0; i < size; ++i)
//    {
//        size_t index;
//        readVarUInt(index, in);
//        IncrementDictData dict_data;
//        dict_data.readIncrementData(in);
//        increment_column_data.emplace(index, std::move(dict_data));
//    }
//}
//
//void IncrementColumnData::writeIncrementData(WriteBuffer & out)
//{
//    size_t size = increment_column_data.size();
//    writeVarUInt(size, out);
//    for (auto it = increment_column_data.begin(); it != increment_column_data.end(); ++it)
//    {
//        writeVarUInt(it->first, out);
//        it->second.writeIncrementData(out);
//    }
//}

void IncrementData::readIncrementData(ReadBuffer & in)
{
    size_t dicts_size;
    readVarUInt(dicts_size, in);
    for (size_t i = 0; i < dicts_size; ++i)
    {
        String dict_name;
        readStringBinary(dict_name, in);
        IncrementDictData dict_data;
        dict_data.readIncrementData(in);
        increment_data.emplace(dict_name, dict_data);
    }
}

void IncrementData::writeIncrementData(WriteBuffer & out)
{
    size_t size = increment_data.size();
    writeVarUInt(size, out);
    for (auto & entry : increment_data)
    {
        writeStringBinary(entry.first, out);
        entry.second.writeIncrementData(out);
    }
}




/////////////////////////////     StartOf BitEngineDictionary

BitEngineDictionary::BitEngineDictionary(const String & disk_name_, const String & path, const String & name, ContextPtr context_, const size_t shard_id_, const size_t split_id_, const size_t version_)
    : disk_name(disk_name_), dict_name(name), context(context_), shard_id(shard_id_), split_id(split_id_), version(version_),
    key_column(std::make_shared<DataTypeUInt64>(), dict_name + "_key"),
    value_column(std::make_shared<DataTypeUInt64>(), dict_name + "_value"),
    log(&Poco::Logger::get("BitEngineDictionary"))
{

    file_path = fs::path(path) / String(dict_name).append("_").append(std::to_string(split_id)).append((".dict"));
    shard_base_offset = shard_id * (1UL << 48) + split_id * (1UL << 40);
    reload();
}

void BitEngineDictionary::updateVersionTo(const size_t version_)
{
    if (version_ > version)
    {
        LOG_TRACE(log, "Update version of dict {} from version {} to versin {}", std::to_string(split_id), std::to_string(version), std::to_string(version_));
        version = version_;
        dict_saved = false;
        need_update_snapshot = true;
    }
}

BitEngineDictionarySnapshot BitEngineDictionary::getSnapshot()
{
    auto read_lock = readLockForDict();
    return BitEngineDictionarySnapshot(*this);
}

IncrementDictData BitEngineDictionary::getIncrementDictData(const IncrementDictOffset & dict_offset)
{
    size_t increment_offset = dict_offset.dict_offset;
    IncrementDictData increment_data;
    size_t key_size = 0;

    {
        auto read_lock = readLockForDict();
        increment_data.base_offset = increment_offset;
        increment_data.key_column = key_column.cloneEmpty();
        increment_data.value_column = value_column.cloneEmpty();
        key_size = key_column.column->size();
    }

    //std::cout<<" ## key size: " << key_size << " xx increment_offset: " << increment_offset << std::endl;

    // There is another fix: if key_size <= offset, just return empty increment_data since the current dict is outdated, it should not send
    // any increment data.
    // Return empty increment data is safe since the case only happend if dict is increased but the version is not updated:
    // 1. dict is updated and version is increase, but the part is not committed (zookeeper timeout, process is killed)
    // 2. only dict updated
    // Thus the remote replicas has some useless dict encoding, but we should also correct the offset to make data exchanging work.
    if (key_size < increment_offset)
    {
        auto write_lock = writeLockForDict();

        //throw Exception("Cannot getIncrementDict since the start offset is large than current key size: " + std::to_string(key_size) + " < " + std::to_string(increment_offset), ErrorCodes::LOGICAL_ERROR);
        LOG_WARNING(log, "Cannot getIncrementDict since the start offset is large than current key size: {} < {}", std::to_string(key_size), std::to_string(increment_offset));

        // append default value to key_column and value_column to correct offsets
        for (; key_size < increment_offset; ++key_size)
        {
            key_column.column->assumeMutable()->insertDefault();
            value_column.column->assumeMutable()->insertDefault();
        }

        return increment_data;
    }
    else if (key_size == increment_offset)
        return increment_data;

    auto read_lock = readLockForDict();

    increment_data.key_column.column->assumeMutable()->insertRangeFrom(*(key_column.column), increment_offset, key_size - increment_offset);

    size_t value_size = value_column.column->size();
    if (value_size < increment_offset)
        throw Exception("Cannot getIncrementDict since the start offset is large than current value size: " + std::to_string(value_size) + " < " + std::to_string(increment_offset), ErrorCodes::LOGICAL_ERROR);
    if (key_size != value_size)
        throw Exception("Cannot getIncrementDict since key_size is not equal to value_size", ErrorCodes::LOGICAL_ERROR);

    increment_data.value_column.column->assumeMutable()->insertRangeFrom(*(value_column.column), increment_offset, value_size - increment_offset);
    if (increment_data.value_column.column->size() != increment_data.key_column.column->size())
        throw Exception("Cannot get increment data from offset " + std::to_string(increment_offset), ErrorCodes::LOGICAL_ERROR);
    return increment_data;
}

IncrementDictOffset BitEngineDictionary::getIncrementDictOffset()
{
    auto read_lock = readLockForDict();

    size_t key_size = key_column.column->size();
    size_t value_size = value_column.column->size();
    if (key_size != value_size)
        throw Exception("Cannot get increment dict offset", ErrorCodes::LOGICAL_ERROR);
    return IncrementDictOffset(key_size);
}

IncrementDictOffset BitEngineDictionary::getEmptyIncrementDictOffset()
{
    return IncrementDictOffset(0);
}

void BitEngineDictionary::insertIncrementDictData(const IncrementDictData & increment_data)
{
    auto write_lock = writeLockForDict();
    size_t key_size = key_column.column->size();
    size_t value_size = value_column.column->size();
    size_t offset_ = increment_data.base_offset;
    size_t start_pos = 0;

    size_t increment_key_size = increment_data.key_column.column->size();
    size_t increment_value_size = increment_data.value_column.column->size();

    if (key_size != offset_ || value_size != offset_)
    {
        // The case should not happen since dict updated only when its version is smaller. Thus there should no insert.
        // But if this happend, we choose to add increment data as much as possible.
        if (key_size > offset_)
        {
            start_pos = key_size - offset_;
            if (start_pos >= increment_key_size)
            {
                LOG_DEBUG(log, "Current offset is {} more than increment offset, which is {}", start_pos, increment_key_size);
                return;
            }
        }
        else
            throw Exception("Cannot insert increment data to offset " + std::to_string(offset_) + ", the current offset is " + std::to_string(key_size), ErrorCodes::LOGICAL_ERROR);
    }

    if (increment_key_size == 0)
        return;

    if (increment_key_size != increment_value_size)
        throw Exception("Cannot insert increment data since it is corrupt, key size is " + std::to_string(increment_key_size)
                        + ", value size is " + std::to_string(increment_value_size), ErrorCodes::LOGICAL_ERROR);

    key_column.column->assumeMutable()->insertRangeFrom(*(increment_data.key_column.column), start_pos, increment_key_size - start_pos);
    value_column.column->assumeMutable()->insertRangeFrom(*(increment_data.value_column.column), start_pos, increment_value_size - start_pos);

    if (key_column.column->size() != value_column.column->size())
    {
        is_valid = false;
        throw Exception("Got a bad bitengine dictionary after insert increment data, key size is "
            + std::to_string(key_column.column->size()) + ", value size is " + std::to_string(value_column.column->size()), ErrorCodes::LOGICAL_ERROR);
    }

    for (size_t i = start_pos; i < increment_key_size; ++i)
    {
        auto key = increment_data.key_column.column->getUInt(i);
        auto value = increment_data.value_column.column->getUInt(i);
        //std::cout<<" <<<<<<<<< read increment data >>>>>>>>>>>>> : " << key << " --- " << value << std::endl;
        dict.insert({key, value});
    }
}

UInt64 BitEngineDictionary::decodeNumber(UInt64 number)
{
    auto pos = number - shard_base_offset;
    if (pos >= key_column.column->size())
        throw Exception("BitEngine cannot decode number " + std::to_string(number)
            + " since the current position " + std::to_string(pos) + " >= " + std::to_string(key_column.column->size()), ErrorCodes::LOGICAL_ERROR);
    return key_column.column->getUInt(pos);
}

BitMap64 BitEngineDictionary::decodeImpl(const BitMap64 & bitmap_64)
{
    BitMap64 res_bitmap;
    PODArray<UInt64> res_array;
    res_array.reserve(4096);
    size_t cnt = 0;

    for (auto iter = bitmap_64.begin(); iter != bitmap_64.end(); ++iter)
    {
        auto num = *iter;
        auto pos = num - shard_base_offset;
        res_array.push_back(key_column.column->getUInt(pos));
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

ColumnPtr BitEngineDictionary::decode(const IColumn & column)
{
    auto read_lock = readLockForDict();

    const auto & column_bitmap = dynamic_cast<const ColumnBitMap64 &>(column);

    MutableColumnPtr output_column = ColumnBitMap64::create();

    for (size_t i = 0; i < column_bitmap.size(); ++i)
    {
        const BitMap64 & bitmap = column_bitmap.getBitMapAt(i);
        BitMap64 res_bitmap = decodeImpl(bitmap);
        output_column->insert(res_bitmap);
    }

    return output_column;
}

BitMap64 BitEngineDictionary::encodeImpl(const BitMap64 & bitmap_64, PODArray<UInt64> & res_array)
{
    BitMap64 res_bitmap;
//    PODArray<UInt64> res_array;
    res_array.clear();
    iterateAndEncode<BitMap64, PODArray<UInt64>>(bitmap_64, res_array);
    size_t i = 0;
    for (; i+4096 < res_array.size(); i += 4096)
        res_bitmap.addMany(4096, res_array.data() + i);
    res_bitmap.addMany(res_array.size() - i, res_array.data() + i);
    return res_bitmap;
}

BitMap64 BitEngineDictionary::encodeImplWithoutLock(const BitMap64 & bitmap_64, Float64 loss_rate)
{
    BitMap64 res_bitmap;
    PODArray<UInt64> res_array;

    iterateAndEncodeWithoutLock<BitMap64, PODArray<UInt64>>(bitmap_64, bitmap_64.cardinality(), res_array, loss_rate);
    res_bitmap.addMany(res_array.size(), res_array.data());

    return res_bitmap;
}

ColumnWithTypeAndName BitEngineDictionary::encode(const ColumnWithTypeAndName & column)
{
    if (!isBitmap64(column.type))
        throw Exception("Cannot encode column " + column.name + " which type is " + column.type->getName(), ErrorCodes::LOGICAL_ERROR);

    ColumnPtr column_ptr = column.column;

    const auto & column_bitmap = dynamic_cast<const ColumnBitMap64 &>(*column_ptr);

    ColumnWithTypeAndName encoded_column;
    encoded_column.name = column.name + BITENGINE_COLUMN_EXTENSION;
    encoded_column.type = std::make_shared<DataTypeBitMap64>();
    MutableColumnPtr encoded_column_ptr = encoded_column.type->createColumn();

    auto write_lock = writeLockForDict();
    PODArray<UInt64> res_array;
    res_array.reserve(1024 * 1024);

    for (size_t i = 0; i < column_bitmap.size(); ++i)
    {
        const BitMap64 & bitmap = column_bitmap.getBitMapAt(i);
        BitMap64 res_bitmap = encodeImpl(bitmap, res_array);
        encoded_column_ptr->insert(std::move(res_bitmap));
    }

    encoded_column.column = std::move(encoded_column_ptr);
    return encoded_column;
}

ColumnWithTypeAndName BitEngineDictionary::encodeWithoutLock(const ColumnWithTypeAndName & column, Float64 loss_rate)
{
    if (!isBitmap64(column.type))
        throw Exception("Cannot encode column " + column.name + " which type is " + column.type->getName(), ErrorCodes::LOGICAL_ERROR);

    ColumnPtr column_ptr = column.column;

    const auto & column_bitmap = dynamic_cast<const ColumnBitMap64 &>(*column_ptr);

    ColumnWithTypeAndName encoded_column;
    encoded_column.name = column.name + BITENGINE_COLUMN_EXTENSION;
    encoded_column.type = std::make_shared<DataTypeBitMap64>();
    MutableColumnPtr encoded_column_ptr = encoded_column.type->createColumn();

    for (size_t i = 0; i < column_bitmap.size(); ++i)
    {
        const BitMap64 & bitmap = column_bitmap.getBitMapAt(i);
        BitMap64 res_bitmap = encodeImplWithoutLock(bitmap, loss_rate);
        encoded_column_ptr->insert(std::move(res_bitmap));
    }
    encoded_column.column = std::move(encoded_column_ptr);
    return encoded_column;
}

ColumnWithTypeAndName
BitEngineDictionary::encodeColumn(const ColumnWithTypeAndName & column, const BitEngineEncodeSettings & encode_settings)
{
    if (encode_settings.without_lock)
        return encodeWithoutLock(column, encode_settings.getLossRate());
    else
        return encode(column);
}

bool BitEngineDictionary::replayEncodeAndCompare(const ColumnWithTypeAndName & column_to_encode, const ColumnWithTypeAndName & column_to_check)
{
    if (!isBitmap64(column_to_encode.type))
        throw Exception("Cannot replay encoding column " + column_to_encode.name + " which type is " + column_to_encode.type->getName(),
                        ErrorCodes::LOGICAL_ERROR);
    if (!isBitmap64(column_to_check.type))
        throw Exception("Column to check, " + column_to_check.name + ", which type is " + column_to_check.type->getName()
                            + " is not BitMap64", ErrorCodes::LOGICAL_ERROR);

    const auto & column_bitmap = dynamic_cast<const ColumnBitMap64 &>(*(column_to_encode.column));
    const auto & column_check = dynamic_cast<const ColumnBitMap64 &>(*(column_to_check.column));

    // auto write_lock = writeLockForDict();
    PODArray<UInt64> res_array;
    res_array.reserve(1024 * 1024);

    for (size_t i = 0; i < column_bitmap.size(); ++i)
    {
        const BitMap64 & bitmap = column_bitmap.getBitMapAt(i);
        BitMap64 res_bitmap = encodeImpl(bitmap, res_array);

        res_bitmap ^= column_check.getBitMapAt(i);

        if (!res_bitmap.isEmpty())
            return false;
        res_array.clear();
    }
    return true;
}

void BitEngineDictionary::readDataFromReadBuffer(ReadBuffer & in)
{
    auto read_lock = readLockForDict();
    // read name and rows of keys
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    readVarUInt(format_version, in);
    readVarUInt(version, in);
    readVarUInt(is_valid, in);

    String key_type_name;
    size_t key_rows = 0;
    readBinary(key_type_name, in);
    key_column.type = data_type_factory.get(key_type_name);
    readVarUInt(key_rows, in);

    //std::cout<<" read key_type_name: " << key_type_name << " -- key_rows: " << key_rows << std::endl;

    // read keys
    ColumnPtr key_read_column = key_column.type->createColumn();

    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&in](const ISerialization::SubstreamPath&) -> ReadBuffer * { return &in;  };
    settings.avg_value_size_hint = 0;
    settings.position_independent_encoding = false;

    ISerialization::DeserializeBinaryBulkStatePtr key_state;
    ISerialization::SubstreamsCache key_cache;
    key_column.type->getDefaultSerialization()->deserializeBinaryBulkStatePrefix(settings, key_state);
    key_column.type->getDefaultSerialization()->deserializeBinaryBulkWithMultipleStreams(key_read_column, key_rows, settings, key_state, &key_cache);

    if (key_read_column->size() != key_rows)
    {
        throw Exception("Cannot read all data from Dictionary key file.", ErrorCodes::CANNOT_READ_ALL_DATA);
    }

    key_column.column = std::move(key_read_column);

     // read name and rows of values
    String value_type_name;
    size_t value_rows = 0;
    readBinary(value_type_name, in);
    value_column.type = data_type_factory.get(value_type_name);
    readVarUInt(value_rows, in);

    // read keys
    ColumnPtr value_read_column = value_column.type->createColumn();

    ISerialization::DeserializeBinaryBulkStatePtr value_state;
    ISerialization::SubstreamsCache value_cache;
    value_column.type->getDefaultSerialization()->deserializeBinaryBulkStatePrefix(settings, value_state);
    value_column.type->getDefaultSerialization()->deserializeBinaryBulkWithMultipleStreams(value_read_column, value_rows, settings, value_state, &value_cache);

    if (value_read_column->size() != value_rows)
    {
        throw Exception("Cannot read all data from Dictionary value file.", ErrorCodes::CANNOT_READ_ALL_DATA);
    }

    value_column.column = std::move(value_read_column);

    size_t rows = key_column.column->size();
    if (rows != value_column.column->size())
    {
        throw Exception("Mismatch rows of key column and value column", ErrorCodes::LOGICAL_ERROR);
    }
    for (size_t i = 0; i < rows; ++i)
    {
        auto key = key_column.column->getUInt(i);
        auto value = value_column.column->getUInt(i);
        //std::cout<<" <<<<<<<<< read >>>>>>>>>>>>> : " << key << " --- " << value << std::endl;
        dict.insert({key, value});
    }
}

void BitEngineDictionary::readDataFromFile()
{
    ReadBufferFromFile in(context->getDisk(disk_name)->getPath() + file_path);

    if (in.eof())
        return;
    try{
        readDataFromReadBuffer(in);
    }catch(...){
        is_valid = false;
        throw;
    }

    in.close();
}

void BitEngineDictionary::writeDataToWriteBuffer(WriteBuffer & out)
{
    auto write_lock = writeLockForDict();

    writeVarUInt(format_version, out);
    writeVarUInt(version, out);
    writeVarUInt(is_valid, out);

    // write name of keys
    writeStringBinary(key_column.type->getName(), out);
    // write size of keys
    writeVarUInt(key_column.column->size(), out);

    // write keys
    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&out](const ISerialization::SubstreamPath&) -> WriteBuffer * { return &out;  };
    settings.position_independent_encoding = false;
    settings.low_cardinality_max_dictionary_size = 0;

    ISerialization::SerializeBinaryBulkStatePtr key_state;
    key_column.type->getDefaultSerialization()->serializeBinaryBulkStatePrefix(settings, key_state);
    key_column.type->getDefaultSerialization()->serializeBinaryBulkWithMultipleStreams(*key_column.column, 0, 0, settings, key_state);
    key_column.type->getDefaultSerialization()->serializeBinaryBulkStateSuffix(settings, key_state);

    // write name of values
    writeStringBinary(value_column.type->getName(), out);
    // write size of values
    writeVarUInt(value_column.column->size(), out);
    // write values
    ISerialization::SerializeBinaryBulkStatePtr value_state;
    value_column.type->getDefaultSerialization()->serializeBinaryBulkStatePrefix(settings, value_state);
    value_column.type->getDefaultSerialization()->serializeBinaryBulkWithMultipleStreams(*value_column.column, 0, 0, settings, value_state);
    value_column.type->getDefaultSerialization()->serializeBinaryBulkStateSuffix(settings, value_state);

}

void BitEngineDictionary::writeDataToFile(const String & path)
{
    WriteBufferFromFile out(path);

    writeDataToWriteBuffer(out);

    out.close();
}

bool BitEngineDictionary::updated()
{
    return dict_changed;
}

void BitEngineDictionary::drop()
{
    dict_changed = false;
    dict_saved = true;
    need_update_snapshot = false;
}

void BitEngineDictionary::reload()
{
    if (reloaded)
        return;

    loadDict();
    reloaded = true;
}

bool BitEngineDictionary::isValid()
{
    return is_valid;
}

void BitEngineDictionary::setValid()
{
    is_valid = true;
}

void BitEngineDictionary::setInvalid()
{
    LOG_WARNING(log, "BitEngine will set dict as invalid");
    is_valid = false;
}

void BitEngineDictionary::loadDict()
{
    if (!context->getDisk(disk_name)->exists(file_path))
        return;

    LOG_DEBUG(log, "Loading bitengine dictionary from path: {}", file_path);

    //std::cout<<" ########## Loading bitengine dictionary from path: " << file_path << std::endl;

    readDataFromFile();
}

void BitEngineDictionary::flushDict()
{
    auto lock = std::lock_guard<std::mutex>(dict_mutex);
    if (!dict_changed && dict_saved)
        return;

    String dict_path_tmp = file_path + ".tmp";
    auto disk = context->getDisk(disk_name);
    LOG_DEBUG(log, "Try to flush bitengine dictionary of file {}", file_path);

    if (!dict.empty() && !key_column.column->empty())
    {
        try
        {
            if (disk->exists(dict_path_tmp))
                disk->removeFile(dict_path_tmp);

            writeDataToFile(fs::path(disk->getPath()) / dict_path_tmp);
            if (disk->exists(dict_path_tmp))
                disk->moveFile(dict_path_tmp, file_path);
            else
            {
                throw Exception("Failed to write dictionary file " + dict_path_tmp, ErrorCodes::LOGICAL_ERROR);
            }
        }
        catch(...)
        {
            if (disk->exists(dict_path_tmp))
                disk->removeFile(dict_path_tmp);
            throw;
        }
    }
    dict_changed = false;
    dict_saved = true;
}

void BitEngineDictionary::lightFlushDict()
{
    dict_changed = false;
    need_update_snapshot = true;
}

void BitEngineDictionary::resetDictImpl()
{
    is_valid = false;
    reloaded = true;
    need_update_snapshot = false;
    dict_saved = true;
    dict_changed = false;
    shard_base_offset = shard_id * (1UL << 48) + split_id * (1UL << 40);
    version = 0;
    dict.clear();
    key_column.column = key_column.type->createColumn();
    value_column.column = value_column.type->createColumn();

    auto disk = context->getDisk(disk_name);
    if (disk->exists(file_path))
        disk->removeFile(file_path);
}

bool BitEngineDictionary::needUpdateSnapshot()
{
    return need_update_snapshot;
}

void BitEngineDictionary::resetUpdateSnapshot()
{
    need_update_snapshot = false;
}

void BitEngineDictionary::setUpdateSnapshot()
{
    need_update_snapshot = true;
}

BitEngineDictionary::~BitEngineDictionary()
{
    try{
        flushDict();
    }catch(...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}


}
