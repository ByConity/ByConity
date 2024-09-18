//
// Created by 袁宇豪 on 9/18/22.
//

#ifndef CLICKHOUSE_AGGREGATEFUNCTIONCOUNTBYGRANULARITY_H
#define CLICKHOUSE_AGGREGATEFUNCTIONCOUNTBYGRANULARITY_H

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/FieldVisitors.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/HashTable/HashMap.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename T>
struct AggregateFunctionCountByGranularityData
{
    using Key = T;

    using Table = HashMap<Key, UInt64>;

    AggregateFunctionCountByGranularityData()
        : granularity(8192), uniq_count_table(), uniq_position_table() {}

    UInt32 granularity;

    Table uniq_count_table;

    Table uniq_position_table;

    const std::unordered_map<Key, UInt64> getCountInUnorderedMap() const
    {
        std::unordered_map<Key, UInt64> result;
        for (const auto & item : uniq_count_table)
        {
            result[item.getKey()] = item.getMapped();
        }
        return result;
    }

    template <typename Value>
    void addImpl(Value value, UInt64 now_position)
    {
        auto pos_iter = uniq_position_table.find(value);
        if ((pos_iter == uniq_position_table.end())
            || (pos_iter != uniq_position_table.end() && pos_iter->getMapped() < now_position))
        {
            if (pos_iter == uniq_position_table.end())
            {
                bool is_inserted;
                uniq_position_table.emplace(value, pos_iter, is_inserted);
            }
            pos_iter->getMapped() = now_position;
            auto count_iter = uniq_count_table.find(value);
            if (count_iter != uniq_count_table.end())
            {
                count_iter->getMapped() += 1;
            }
            else
            {
                bool is_inserted;
                uniq_count_table.emplace(value, count_iter, is_inserted);
                count_iter->getMapped() = 1;
            }
        }
    }

    template <typename Value>
    void addMany(const Value * __restrict ptr, const UInt8 * __restrict null_map, size_t count)
    {
        if (null_map)
        {
            addManyNotNull<Value>(ptr, null_map, count);
        }
        else
        {
            addMany<Value>(ptr, count);
        }
    }

    template <typename Value>
    void addMany(const Value * __restrict ptr, size_t count)
    {
        size_t total_positions = (count / granularity);
        size_t remains = count - total_positions * granularity;
        for (size_t pos = 0; pos < total_positions; ++pos)
        {
            for (size_t i = 0; i < granularity; ++i)
            {
                size_t now_iter = granularity * pos + i;
                addImpl(ptr[now_iter], pos);
            }
        }
        for (size_t i = 0; i < remains; ++i)
        {
            size_t now_iter = granularity * total_positions + i;
            addImpl(ptr[now_iter], total_positions);
        }
        uniq_position_table.clear();
    }

    template <typename Value>
    void addManyNotNull(const Value * __restrict ptr, const UInt8 * __restrict null_map, size_t count)
    {
        size_t total_positions = (count / granularity);
        size_t remains = count - total_positions * granularity;
        for (size_t pos = 0; pos < total_positions; ++pos)
        {
            for (size_t i = 0; i < granularity; ++i)
            {
                size_t now_iter = granularity * pos + i;
                if (null_map[now_iter])
                {
                    continue;
                }
                addImpl(ptr[now_iter], pos);
            }
        }
        for (size_t i = 0; i < remains; ++i)
        {
            size_t now_iter = granularity * total_positions + i;
            if (null_map[now_iter])
            {
                continue;
            }
            addImpl(ptr[now_iter], total_positions);
        }
        uniq_position_table.clear();
    }

    void merge(const AggregateFunctionCountByGranularityData<T> & other)
    {
        this->uniq_position_table.clear();
        for (const auto & item : other.uniq_count_table)
        {
            auto iter = this->uniq_count_table.find(item.getKey());
            if (iter == this->uniq_count_table.end())
            {
                bool is_inserted;
                this->uniq_count_table.emplace(item.getKey(), iter, is_inserted);
                iter->getMapped() = item.getMapped();
            }
            else
            {
                iter->getMapped() += item.getMapped();
            }
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeIntBinary(granularity, buf);
        //this->uniqCountTable.write(buf);
        writeIntBinary(this->uniq_count_table.size(), buf);
        for (const auto & item : this->uniq_count_table)
        {
            writeBinary(item.getKey(), buf);
            writeIntBinary(item.getMapped(), buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        this->uniq_count_table.clear();

        readIntBinary(granularity, buf);
        //this->uniqCountTable.read(buf);
        UInt64 size;
        readIntBinary(size, buf);
        for (UInt64 i = 0; i < size; ++i)
        {
            T key;
            UInt64 count;
            readBinary(key, buf);
            readIntBinary(count, buf);
            this->uniq_count_table[key] = count;
        }
    }

    String str() const
    {
        std::ostringstream oss;
        for (const auto & item : this->uniq_count_table)
        {
            oss << "(" << item.getKey() << ", " << item.getMapped() << ")";
        }
        return oss.str();
    }
};

template <>
struct AggregateFunctionCountByGranularityData<String>
{
    using Table = std::unordered_map<String, UInt64, std::hash<String>, std::equal_to<>, TrackAllocator<std::pair<const String, UInt64>>>;

    AggregateFunctionCountByGranularityData()
        : granularity(8192), uniq_count_table(), uniq_position_table() {}

    UInt32 granularity;

    Table uniq_count_table;

    Table uniq_position_table;

    const std::unordered_map<String , UInt64> getCountInUnorderedMap() const
    {
        std::unordered_map<String, UInt64> result;
        for (const auto & item : uniq_count_table)
        {
            result[item.first] = item.second;
        }
        return result;
    }

    void addImpl(String value, UInt64 now_position)
    {
        auto pos_iter = uniq_position_table.find(value);
        if ((pos_iter == uniq_position_table.end())
            || (pos_iter != uniq_position_table.end() && pos_iter->second < now_position))
        {
            if (pos_iter == uniq_position_table.end())
            {
                uniq_position_table.emplace(value, now_position);
            }
            else
                pos_iter->second = now_position;
            auto count_iter = uniq_count_table.find(value);
            if (count_iter != uniq_count_table.end())
            {
                count_iter->second += 1;
            }
            else
            {
                uniq_count_table.emplace(value, 1);
            }
        }
    }

    void addMany(const ColumnString & column, const UInt8 * __restrict null_map, size_t count)
    {
        if (null_map)
        {
            addManyNotNull(column, null_map, count);
        }
        else
        {
            addMany(column, count);
        }
    }

    void addMany(const ColumnString & column, size_t count)
    {
        size_t total_positions = (count / granularity);
        size_t remains = count - total_positions * granularity;
        for (size_t pos = 0; pos < total_positions; ++pos)
        {
            for (size_t i = 0; i < granularity; ++i)
            {
                size_t nowIter = granularity * pos + i;
                const auto & value = column.getDataAt(nowIter);
                addImpl(value.toString(), pos);
            }
        }
        for (size_t i = 0; i < remains; ++i)
        {
            size_t nowIter = granularity * total_positions + i;
            const auto & value = column.getDataAt(nowIter);
            addImpl(value.toString(), total_positions);
        }
        uniq_position_table.clear();
    }

    void addManyNotNull(const ColumnString & column, const UInt8 * __restrict null_map, size_t count)
    {
        size_t total_positions = (count / granularity);
        size_t remains = count - total_positions * granularity;
        for (size_t pos = 0; pos < total_positions; ++pos)
        {
            for (size_t i = 0; i < granularity; ++i)
            {
                size_t nowIter = granularity * pos + i;
                if (null_map[nowIter])
                {
                    continue;
                }
                const auto & value = column.getDataAt(nowIter);
                addImpl(value.toString(), pos);
            }
        }
        for (size_t i = 0; i < remains; ++i)
        {
            size_t nowIter = granularity * total_positions + i;
            if (null_map[nowIter])
            {
                continue;
            }
            const auto & value = column.getDataAt(nowIter);
            addImpl(value.toString(), total_positions);
        }
        uniq_position_table.clear();
    }

    void merge(const AggregateFunctionCountByGranularityData<String> & other)
    {
        this->uniq_position_table.clear();
        for (const auto & item : other.uniq_count_table)
        {
            auto iter = this->uniq_count_table.find(item.first);
            if (iter == this->uniq_count_table.end())
            {
                this->uniq_count_table.emplace(item.first, item.second);
            }
            else
            {
                iter->second += item.second;
            }
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeIntBinary(granularity, buf);
        //this->uniqCountTable.write(buf);
        writeIntBinary(this->uniq_count_table.size(), buf);
        for (const auto & item : this->uniq_count_table)
        {
            writeStringBinary(item.first, buf);
            writeIntBinary(item.second, buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        this->uniq_count_table.clear();

        readIntBinary(granularity, buf);
        //this->uniqCountTable.read(buf);
        UInt64 size;
        readIntBinary(size, buf);
        for (UInt64 i = 0; i < size; ++i)
        {
            String key;
            UInt64 count;
            readStringBinary(key, buf);
            readIntBinary(count, buf);
            this->uniq_count_table[key] = count;
        }
    }

    String str() const
    {
        std::ostringstream oss;
        for (const auto & item : this->uniq_count_table)
        {
            oss << "(" << item.first << ", " << item.second << ")";
        }
        return oss.str();
    }
};

using T=UInt8;

template <typename T>
class AggregateFunctionCountByGranularity final
    : public IAggregateFunctionDataHelper<AggregateFunctionCountByGranularityData<T>, AggregateFunctionCountByGranularity<T>>
{
public:
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;

    AggregateFunctionCountByGranularity(const DataTypes & argument_types_, const Array & params_)
        : IAggregateFunctionDataHelper<AggregateFunctionCountByGranularityData<T>, AggregateFunctionCountByGranularity>(argument_types_, params_)
    {
        if (!params_.empty())
        {
            if (params_.size() != 1)
            {
                throw Exception(
                    "Aggregate function AggregateFunctionCountByGranularity requires one parameter or less.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }

            UInt64 granularity_param = applyVisitorExplicit(FieldVisitorConvertToNumber<UInt64>(), params_[0]);

            // This range is hardcoded below
            if (granularity_param == 0)
            {
                throw Exception(
                    "Parameter for aggregate function AggregateFunctionCountByGranularity is out or range: (0,].", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
            }
            granularity = granularity_param;
        }
        else
        {
            granularity = 8192;
        }
    }

    String getName() const override { return "countByGranularity"; }

    DataTypePtr getReturnType() const override
    {
        DataTypes types;
        if constexpr (
            std::is_same_v<T, Int8>
            || std::is_same_v<T, UInt8>
            || std::is_same_v<T, Int16>
            || std::is_same_v<T, UInt16>
            || std::is_same_v<T, Int32>
            || std::is_same_v<T, UInt32>
            || std::is_same_v<T, Int64>
            || std::is_same_v<T, UInt64>
            //                || std::is_same_v<T, Int128>  TODO can't support Int128 for now
            || std::is_same_v<T, UInt128>
            || std::is_same_v<T, Float32>
            || std::is_same_v<T, Float64>
        )
        {
            types.emplace_back(std::make_shared<DataTypeNumber<T>>()); // group by
        }
        // TODO can't support Decimal for now
        //        else if constexpr (std::is_same_v<T, Decimal32> || std::is_same_v<T, Decimal64> || std::is_same_v<T, Decimal128>)
        //        {
        //            types.emplace_back(std::make_shared<DataTypeDecimal<T>>(DataTypeDecimal<T>::maxPrecision(), ????scale????)); // can't construct for now
        //        }
        else
        {
            types.emplace_back(std::make_shared<DataTypeString>()); // group by
        }
        types.emplace_back(std::make_shared<DataTypeUInt64>());  // count
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(types));
    }

    void add([[maybe_unused]] AggregateDataPtr __restrict place, const IColumn **, size_t, Arena *) const override
    {
        throw new Exception("Logical error: Count by granularity must run in batch mode.", ErrorCodes::LOGICAL_ERROR);
    }

    void addBatchSinglePlace(
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena *,[[maybe_unused]] ssize_t if_argument_pos) const override
    {
        this->data(place).granularity = this->granularity;
        if constexpr (std::is_same_v<T, String>)
        {
            const auto & string_column = static_cast<const ColumnString &>(*columns[0]);
            this->data(place).addMany(string_column, batch_size);
        }
        else
        {
            const auto & column = static_cast<const ColVecType &>(*columns[0]);
            this->data(place).addMany(column.getData().data(), batch_size);
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        [[maybe_unused]] ssize_t if_argument_pos) const override
    {
        this->data(place).granularity = this->granularity;
        if constexpr (std::is_same_v<T, String>)
        {
            const auto & string_column = static_cast<const ColumnString &>(*columns[0]);
            this->data(place).addManyNotNull(string_column, null_map, batch_size);
        }
        else
        {
            const auto & column = static_cast<const ColVecType &>(*columns[0]);
            this->data(place).addManyNotNull(column.getData().data(), null_map, batch_size);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & tuples = this->data(place).uniq_count_table;

        auto & column_res = static_cast<ColumnArray &>(to);
        auto & column_offsets = static_cast<ColumnArray::ColumnOffsets &>(column_res.getOffsetsColumn());

        auto & tuple_in_array = static_cast<ColumnTuple &>(column_res.getData());

        for (const auto & item : tuples)
        {
            if constexpr (
                std::is_same_v<T, Int8>
                || std::is_same_v<T, UInt8>
                || std::is_same_v<T, Int16>
                || std::is_same_v<T, UInt16>
                || std::is_same_v<T, Int32>
                || std::is_same_v<T, UInt32>
                || std::is_same_v<T, Int64>
                || std::is_same_v<T, UInt64>
                //                    || std::is_same_v<T, Int128> TODO can't support Int128 for now
                || std::is_same_v<T, UInt128>
                || std::is_same_v<T, Float32>
                || std::is_same_v<T, Float64>
            )
            {
                auto & column_group_by = static_cast<ColumnVector<T> &>(tuple_in_array.getColumn(0));
                column_group_by.insert(item.getKey());
            }
            // TODO can't support Decimal for now
            //            else if constexpr (std::is_same_v<T, Decimal32> || std::is_same_v<T, Decimal64> || std::is_same_v<T, Decimal128>)
            //            {
            //                auto & column_group_by = static_cast<ColumnDecimal<T> &>(tuple_in_array.getColumn(0));
            //                column_group_by.insert(item.getKey());
            //            }
            else
            {
                auto & column_group_by = static_cast<ColumnString &>(tuple_in_array.getColumn(0));
                std::ostringstream oss;
                oss << item.first;
                column_group_by.insert(oss.str());
            }

            if constexpr (std::is_same_v<T, String>)
            {
                auto & column_count = static_cast<ColumnUInt64 &>(tuple_in_array.getColumn(1));
                column_count.insert(item.second);
            }
            else
            {
                auto & column_count = static_cast<ColumnUInt64 &>(tuple_in_array.getColumn(1));
                column_count.insert(item.getMapped());
            }
        }
        column_offsets.getData().push_back(column_res.getData().size());
    }

    bool allocatesMemoryInArena() const override
    {
        return false;
    }
private:
    UInt32 granularity;
};

}

#endif //CLICKHOUSE_AGGREGATEFUNCTIONCOUNTBYGRANULARITY_H
