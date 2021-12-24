#pragma once

#include <Storages/MergeTree/BitEngineDictionary/BitEngineDictionary.h>


/// TODO (liuhaoqiang) remove these after all functions are implemented
#pragma  GCC diagnostic ignored  "-Wunused"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-function"



namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
}

class BitEngineDictionary;

using BitEngineDictioanryColumnPtr = const ColumnUInt64 *;

class BitEngineDictionarySnapshot
{
public:
    BitEngineDictionarySnapshot(const BitEngineDictionary & bitengine_dict);

    BitEngineDictionarySnapshot(const BitEngineDictionarySnapshot & snapshot);
    BitEngineDictionarySnapshot & operator=(const BitEngineDictionarySnapshot & snapshot);

    UInt64 decodeNumber(UInt64 number);
    BitMap64 decodeBitmap(const BitMap64 & bitmap);
    ColumnPtr decodeColumn(const IColumn & column);
    ColumnPtr decodeNonBitEngineColumn(const IColumn & column);

    bool empty() { return (!key_column_ptr || key_column_size == 0); }
    BitEngineDictioanryColumnPtr getKeyColumn() { return key_column_ptr; }
    BitEngineDictioanryColumnPtr getKeyColumn() const { return key_column_ptr; }
    size_t getOffset() const { return shard_base_offset; }
    size_t size() const { return key_column_size; }

    void writeDataToBuffer(std::pair<UInt64, UInt64> & offsets_range, WriteBuffer & out);

    template <typename T>
    void tryUpdateSnapshot(const T & bitengine_dict)
    {
        auto dict_size_now = bitengine_dict.key_column.column->size();

        if (key_column_ptr != bitengine_dict.key_column.column)
        {
            dict_name = bitengine_dict.dict_name;
            key_column_ptr = checkAndGetColumn<ColumnUInt64>(bitengine_dict.key_column.column.get());
        }
        else if (key_column_size == dict_size_now)
            return;
        else if (key_column_size > dict_size_now)
        {
            dict_name = bitengine_dict.dict_name;
            key_column_ptr = checkAndGetColumn<ColumnUInt64>(bitengine_dict.key_column.column.get());
        }

        /// update the latest key_column.size
        key_column_size = dict_size_now;
        LOG_TRACE(&Poco::Logger::get("BitEngineDictionarySnapshot"), "Snapshot {} has been updated, now rows={}", dict_name, key_column_size);
    }

private:
    String dict_name;
    size_t shard_base_offset;
    BitEngineDictioanryColumnPtr key_column_ptr;
    size_t key_column_size;
};

}
