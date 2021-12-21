#pragma once

#include <Common/SipHash.h>
#include <Common/RWLock.h>
#include <Common/HashTable/HashMap.h>
#include <Poco/File.h>


#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/BitEngineDictionary/BitEngineDataExchanger.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Functions/FunctionHelpers.h>
#include <common/logger_useful.h>

namespace DB
{

class BitEngineDictionarySnapshot;
class IncrementDictOffset;
//class IncrementColumnOffset;
class IncrementOffset;
class IncrementDictData;
//class IncrementColumnData;
class IncrementData;

class BitEngineDictionary
{
public:
    BitEngineDictionary(const String & disk_name, const String & path, const String & name, ContextPtr context, const size_t shard_id, const size_t split_id = 1, const size_t version = 0);
    BitEngineDictionary(const BitEngineDictionary & bitengine_dictionary) = delete;
    ~BitEngineDictionary();
    BitEngineDictionary & operator=(const BitEngineDictionary & bitengine_dictionary) = delete;

    void close() { /*do nothing*/ }

    UInt64 decodeNumber(UInt64 number);
    BitMap64 decodeImpl(const BitMap64 & bitmap64);
    ColumnPtr decode(const IColumn & column);

    BitMap64 encodeImpl(const BitMap64 & bitmap64, PODArray<UInt64> & res_array);
    BitMap64 encodeImplWithoutLock(const BitMap64 & bitmap_64, Float64 loss_rate = 0);
    ColumnWithTypeAndName encode(const ColumnWithTypeAndName & column);
    ColumnWithTypeAndName encodeWithoutLock(const ColumnWithTypeAndName & column, Float64 loss_rate = 0);
    ColumnWithTypeAndName encodeColumn(const ColumnWithTypeAndName & column, const BitEngineEncodeSettings & encode_settings);
    bool replayEncodeAndCompare(const ColumnWithTypeAndName & column_to_encode, const ColumnWithTypeAndName & column_to_check);

    template<typename T>
    ColumnWithTypeAndName encodeNonBitEngineColumn(const ColumnWithTypeAndName & column, bool add_new_id_to_dict, Float64 loss_rate = 0)
    {
        if (!isNativeInteger(column.type))
            throw Exception("Cannot encode column " + column.name + " which type is " + column.type->getName(), ErrorCodes::LOGICAL_ERROR);

        ColumnPtr column_ptr = column.column;

        const auto * column_data = checkAndGetColumnEvenIfConst<ColumnVector<T>>(column_ptr.get());

        ColumnWithTypeAndName encoded_column;
        encoded_column.name = column.name;
        encoded_column.type = std::make_shared<DataTypeUInt64>();
        MutableColumnPtr encoded_column_ptr = encoded_column.type->createColumn();

        const auto & source_data = column_data->getData();
        auto & res_array = typeid_cast<ColumnUInt64 *>(encoded_column_ptr.get())->getData();

        if (add_new_id_to_dict)
        {
            auto write_lock = writeLockForDict();
            iterateAndEncode<typename ColumnVector<T>::Container, typename ColumnUInt64::Container>(source_data, res_array);
        }
        else
        {
            iterateAndEncodeWithoutLock<typename ColumnVector<T>::Container, typename ColumnUInt64::Container>(source_data, source_data.size(), res_array, loss_rate);
        }

        encoded_column.column = std::move(encoded_column_ptr);
        return encoded_column;
    }

    // used by encodeImpl and encodeNonBitEngineColumn
    template <typename S, typename R>
    void iterateAndEncode(const S & data, R & res_array)
    {
        auto mutable_key_column = key_column.column->assumeMutable();
        auto mutable_value_column = value_column.column->assumeMutable();
        bool insert = false;
        bool dict_is_changed = false;

        for (auto it = data.begin(); it != data.end(); ++it)
        {
            UInt64 data_key = static_cast<UInt64>(*it);
            typename HashMap<UInt64, UInt64>::LookupResult jt;
            dict.emplace(data_key, jt, insert);
            if (insert)
            {
                UInt64 encode_num = key_column.column->size() + shard_base_offset;
                mutable_key_column->insert(data_key);
                mutable_value_column->insert(encode_num);
                new (&jt->getMapped()) HashMap<UInt64, UInt64>::mapped_type(encode_num);
                if (!dict_is_changed)
                    dict_is_changed = true;
            }
            //std::cout<<" key: " << data_key << " -- value: " << jt->getSecond() << std::endl;
            res_array.push_back(jt->getMapped());
        }
        if (dict_is_changed)
        {
            dict_changed = true;
            dict_saved = false;
            need_update_snapshot = true;
        }
    }

    // used by encodeImpl and encodeNonBitEngineColumn
    template <typename S, typename R>
    void iterateAndEncodeWithoutLock(const S & data, size_t total_count, R & res_array, Float64 loss_rate)
    {
        size_t lost_count = 0;

        for (auto it = data.begin(); it != data.end(); ++it)
        {
            UInt64 data_key = *it;
            bool insert = false;
            typename HashMap<UInt64, UInt64>::ConstLookupResult jt;

            {
                jt = dict.find(data_key);
                if (!jt)
                   insert = true;
            }
            if (!insert)
            {
                res_array.push_back(jt->getMapped());
            }
            else
            {
                if (std::is_same<S, ColumnUInt64::Container>::value)
                    res_array.push_back(std::numeric_limits<UInt64>::max());
                lost_count++;
                continue;
            }
        }

        Float64 loss_rate_now = 0;

        if (total_count > 8192)
            loss_rate_now = static_cast<Float64>(lost_count) / static_cast<Float64>(lost_count + res_array.size());

        if (loss_rate > 0 && loss_rate_now > loss_rate)
            LOG_ERROR(log, "BitEngine encode loss rate: {}. Loss rate is too large: {} > {}, lost count is {}, result size is {}", std::to_string(loss_rate_now), std::to_string(loss_rate_now), std::to_string(loss_rate), std::to_string(lost_count), std::to_string(res_array.size()));
    }

    UInt64 getColumnSize()
    {
        auto lock = readLockForDict();
        return key_column.column->size();
    }

    UInt64 getShardBaseOffset() const { return shard_base_offset; }

    void updateVersionTo(const size_t version_);
    BitEngineDictionarySnapshot getSnapshot();

    IncrementDictData getIncrementDictData(const IncrementDictOffset & dict_offset);
    IncrementDictOffset getIncrementDictOffset();
    IncrementDictOffset getEmptyIncrementDictOffset();
    void insertIncrementDictData(const IncrementDictData & increment_data);

    void readDataFromReadBuffer(ReadBuffer & in);
    void writeDataToWriteBuffer(WriteBuffer & out);
    void readIncrementDataFromReadBuffer(ReadBuffer & in);
    void writeIncrementDataToWriteBuffer(WriteBuffer & out);

    bool updated();
    void drop();
    bool isValid();
    void setValid();
    void setInvalid();
    bool empty() { return dict.empty(); }
    void reload();
    void flushDict();
    void resetDict() { resetDictImpl(); } // used in repair mode where dicts in all replicas are corrupted.

    void lightFlushDict();
    bool needUpdateSnapshot();
    void resetUpdateSnapshot();
    void setUpdateSnapshot();

    void renameTo(const String &, const String & new_dict_path)
    {
        file_path = new_dict_path + dict_name + "_" + std::to_string(split_id) + ".dict";
    }

private:
    friend BitEngineDictionarySnapshot;

    String disk_name;
    String dict_name;
    ContextPtr context;
    size_t shard_id;
    size_t split_id;
    size_t shard_base_offset;
    size_t version;
    String file_path;
    size_t format_version = 1;

    HashMap<UInt64, UInt64> dict;

    ColumnWithTypeAndName key_column;
    ColumnWithTypeAndName value_column;

    Poco::Logger * log;

    // only changed when insert new encode number
    std::atomic<bool> dict_changed {false};
    // only changed when dict is updated
    std::atomic<bool> dict_saved {false};
    std::atomic<bool> need_update_snapshot {false};
    std::atomic<bool> reloaded {false};
    std::atomic<bool> is_valid {true};

    void readDataFromFile();
    void writeDataToFile(const String & path);
    void loadDict();
    void resetDictImpl();

    // Lock for dictionary
    mutable RWLock dict_lock = RWLockImpl::create();
    using DictLock = RWLockImpl::LockHolder;
    DictLock readLockForDict(const std::string & who = RWLockImpl::NO_QUERY)
    {
        auto res = dict_lock->getLock(RWLockImpl::Read, who);
        return res;
    }

    DictLock writeLockForDict(const std::string & who = RWLockImpl::NO_QUERY)
    {
        auto res = dict_lock->getLock(RWLockImpl::Write, who);
        return res;
    }

    std::mutex dict_mutex;
};

using BitEngineDictionaryPtr = std::shared_ptr<BitEngineDictionary>;



class IncrementDictOffset
{
public:
    IncrementDictOffset(size_t dict_offset_ = 0): dict_offset(dict_offset_) {}

    void readIncrementOffset(ReadBuffer & in);
    void writeIncrementOffset(WriteBuffer & out) const;
    size_t dict_offset;
};

//class IncrementColumnOffset
//{
//public:
//    IncrementColumnOffset() {}
//
//    void readIncrementOffset(ReadBuffer & in);
//    void writeIncrementOffset(WriteBuffer & out);
//    std::map<UInt64, IncrementDictOffset> increment_column_offset;
//};

class IncrementOffset
{
public:
    IncrementOffset() {}

    void readIncrementOffset(ReadBuffer & in);
    void writeIncrementOffset(WriteBuffer & out);
    std::map<String, IncrementDictOffset> increment_offset;
};

class IncrementDictData
{
public:
    IncrementDictData();
    IncrementDictData(const IncrementDictData & dict_data);
    IncrementDictData & operator=(const IncrementDictData & dict_data);

    void readIncrementData(ReadBuffer & in);
    void writeIncrementData(WriteBuffer & out);

    size_t base_offset;
    ColumnWithTypeAndName key_column; 
    ColumnWithTypeAndName value_column;
};

//class IncrementColumnData
//{
//public:
//    IncrementColumnData() {}
//    void readIncrementData(ReadBuffer & in);
//    void writeIncrementData(WriteBuffer & out);
//    std::map<UInt64, IncrementDictData> increment_column_data;
//};

class IncrementData
{
public:
    IncrementData() {}
    void readIncrementData(ReadBuffer & in);
    void writeIncrementData(WriteBuffer & out);
    std::map<String, IncrementDictData> increment_data;
};

}
