#include <memory>
#include <boost/icl/detail/interval_subset_comparer.hpp>
#include <gtest/gtest.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <roaring/containers/array.h>
#include "Common/Exception.h"
#include "Common/PODArray_fwd.h"
#include "common/types.h"
#include "Columns/IColumn.h"
#include "Core/Field.h"
#include "DataTypes/DataTypeObject.h"
#include "DataTypes/Serializations/ISerialization.h"
#include "DataTypes/Serializations/SerializationNumber.h"
#include "DataTypes/Serializations/SerializationString.h"
#include "DataTypes/Serializations/SerializationBigString.h"
#include "IO/ReadBuffer.h"
#include "IO/WriteBufferFromString.h"
#include "IO/ReadBufferFromString.h"

using namespace DB;

struct SerializeCtx
{
    SerializeCtx(const MutableColumnPtr& col_, SerializationPtr serialization_):
        serialization(serialization_)
    {
        ISerialization::SerializeBinaryBulkSettings settings;
        ISerialization::SerializeBinaryBulkStatePtr state;
        std::map<String, std::unique_ptr<WriteBuffer>> write_holders;
        settings.getter = [this, &write_holders](const ISerialization::SubstreamPath& substream_path) -> WriteBuffer* {
            String substream = ISerialization::getFileNameForStream("", substream_path);
            if (auto iter = buffers.find(substream); iter == buffers.end())
            {
                buffers[substream] = std::make_shared<String>();
                write_holders[substream] = std::make_unique<WriteBufferFromString>(*(buffers[substream]));
            }
            return write_holders[substream].get();
        };
        settings.low_cardinality_max_dictionary_size = 10;

        serialization->serializeBinaryBulkStatePrefix(*col_, settings, state);
        serialization->serializeBinaryBulkWithMultipleStreams(*col_, 0, col_->size(), settings, state);
        serialization->serializeBinaryBulkStateSuffix(settings, state);

        write_holders.clear();
    }

    void verify(const PaddedPODArray<UInt8>& filter, ColumnPtr expect_res)
    {
        ISerialization::DeserializeBinaryBulkSettings settings;
        settings.getter = [this](const ISerialization::SubstreamPath& substream_path) -> ReadBuffer* {
            String substream = ISerialization::getFileNameForStream("", substream_path);
            if (auto iter = buffers.find(substream); iter != buffers.end())
            {
                if (auto rd_iter = read_holders.find(substream); rd_iter == read_holders.end())
                {
                    read_holders[substream] = std::make_unique<ReadBufferFromString>(*buffers[substream]);
                }
                return read_holders[substream].get();
            }
            return nullptr;
        };
        settings.filter = filter.cbegin();

        ColumnPtr read_col = expect_res->cloneEmpty();

        ISerialization::DeserializeBinaryBulkStatePtr state;
        String stream_name = ISerialization::getFileNameForStream("", settings.path);
        if (auto iter = states.find(stream_name); iter == states.end())
        {
            serialization->deserializeBinaryBulkStatePrefix(settings, state);
            states[stream_name] = state;
        }
        else
        {
            state = iter->second;
        }

        serialization->deserializeBinaryBulkWithMultipleStreams(read_col, filter.size(), settings, state, nullptr);

        ASSERT_EQ(expect_res->size(), read_col->size());
        for (size_t i = 0; i < expect_res->size(); ++i)
        {
            ASSERT_EQ(read_col->compareAt(i, i, *expect_res, 1), 0);
        }
    }

    SerializationPtr serialization;

    std::map<String, std::shared_ptr<String>> buffers;

    std::map<String, std::unique_ptr<ReadBuffer>> read_holders;
    std::map<String, ISerialization::DeserializeBinaryBulkStatePtr> states;
};

void verifyByStep(SerializeCtx& ctx, const MutableColumnPtr& raw_data,
    const PaddedPODArray<UInt8>& raw_filter, size_t verify_step)
{
    size_t element_size = raw_data->size();
    for (size_t i = 0; i < element_size; i += verify_step)
    {
        size_t verify_size = std::min(verify_step, element_size - i);
        MutableColumnPtr tmp_col = raw_data->cloneEmpty();
        tmp_col->insertRangeFrom(*raw_data, i, verify_size);
        PaddedPODArray<UInt8> tmp_filter;
        tmp_filter.assign(raw_filter.cbegin() + i, raw_filter.cbegin() + i + verify_size);

        ColumnPtr res_col = tmp_col->filter(tmp_filter, 0);
        ASSERT_NO_FATAL_FAILURE(ctx.verify(tmp_filter, res_col));
    }
}

TEST(DeserializeFilterTest, String)
{
    DataTypeString str_type;

    size_t element_size = 100;
    size_t verify_step = 7;
    MutableColumnPtr str_col = str_type.createColumn();
    PaddedPODArray<UInt8> filter(element_size, 0);
    for (size_t i = 0; i < element_size; ++i)
    {
        str_col->insert(Field(std::to_string(i)));
        if (i % 3 == 0 || i % 4 == 0)
        {
            filter[i] = 1;
        }
    }

    SerializeCtx ctx(str_col, str_type.getDefaultSerialization());
    ASSERT_NO_FATAL_FAILURE(verifyByStep(ctx, str_col, filter, verify_step));
}

TEST(DeserializeFilterTest, BigString)
{
    size_t element_size = 100;
    size_t verify_step = 7;
    MutableColumnPtr str_col = DataTypeString().createColumn();
    PaddedPODArray<UInt8> filter(element_size, 0);
    for (size_t i = 0; i < element_size; ++i)
    {
        str_col->insert(Field(std::to_string(i)));
        if (i % 3 == 0 || i % 8 == 0)
        {
            filter[i] = 1;
        }
    }

    SerializeCtx ctx(str_col, std::make_shared<SerializationBigString>());
    ASSERT_NO_FATAL_FAILURE(verifyByStep(ctx, str_col, filter, verify_step));
}

TEST(DeserializeFilterTest, Number)
{
    size_t element_size = 100;
    size_t verify_step = 7;
    MutableColumnPtr num_col = DataTypeUInt64().createColumn();
    PaddedPODArray<UInt8> filter(element_size, 0);
    for (size_t i = 0; i < element_size; ++i)
    {
        num_col->insert(static_cast<UInt64>(i));
        if (i % 2 == 0 || i % 7 == 0)
        {
            filter[i] = 1;
        }
    }

    SerializeCtx ctx(num_col, std::make_shared<SerializationNumber<UInt64>>());
    ASSERT_NO_FATAL_FAILURE(verifyByStep(ctx, num_col, filter, verify_step));
}

TEST(DeserializeFilterTest, SerializationOversize)
{
    size_t element_size = 100;
    MutableColumnPtr num_col = DataTypeUInt64().createColumn();
    PaddedPODArray<UInt8> filter(element_size + 100, 0);
    for (size_t i = 0; i < element_size; ++i)
    {
        num_col->insert(static_cast<UInt64>(i));
        if (i % 2 == 0 || i % 7 == 0)
        {
            filter[i] = 1;
        }
    }

    SerializeCtx ctx(num_col, std::make_shared<SerializationNumber<UInt64>>());

    {
        PaddedPODArray<UInt8> tmp_filter;
        tmp_filter.insert(filter.begin(), filter.begin() + element_size);
        ColumnPtr res_col = num_col->filter(tmp_filter, 0);
        ASSERT_NO_FATAL_FAILURE(ctx.verify(filter, res_col));
    }
}

TEST(DeserializeFilterTest, Array)
{
    size_t element_size = 100;
    size_t verify_step = 7;
    DataTypePtr arr_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    MutableColumnPtr arr_col = arr_type->createColumn();
    PaddedPODArray<UInt8> filter(element_size, 0);
    for (size_t i = 0; i < element_size; ++i)
    {
        Array row_val;
        for (size_t j = 0; j <= i % 5; ++j)
        {
            row_val.push_back(i);
        }
        arr_col->insert(row_val);

        if (i % 4 == 0 || i % 9 == 0)
        {
            filter[i] = 1;
        }
    }

    SerializeCtx ctx(arr_col, arr_type->getDefaultSerialization());
    ASSERT_NO_FATAL_FAILURE(verifyByStep(ctx, arr_col, filter, verify_step));
}

TEST(DeserializeFilterTest, Map)
{
    size_t element_size = 100;
    size_t verify_step = 7;
    DataTypePtr map_type = std::make_shared<DataTypeMap>(
        std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());
    MutableColumnPtr map_col = map_type->createColumn();
    PaddedPODArray<UInt8> filter(element_size, 0);
    for (size_t i = 0; i < element_size; ++i)
    {
        Map row_val;
        for (size_t j = 0; j <= i % 5; ++j)
        {
            String key = fmt::format("{}", 'a' + (i % 26));
            row_val.push_back(std::pair<Field, Field>(key, std::to_string(i)));
        }
        map_col->insert(row_val);
        if (i % 3 == 0 || i % 11 == 0)
        {
            filter[i] = 1;
        }
    }

    SerializeCtx ctx(map_col, map_type->getDefaultSerialization());
    ASSERT_NO_FATAL_FAILURE(verifyByStep(ctx, map_col, filter, verify_step));
}

TEST(DeserializeFilterTest, LowCardinality)
{
    size_t element_size = 100;
    size_t verify_step = 7;
    DataTypePtr lc_type = std::make_shared<DataTypeLowCardinality>(
        std::make_shared<DataTypeString>());
    MutableColumnPtr lc_col = lc_type->createColumn();
    PaddedPODArray<UInt8> filter(element_size, 0);
    for (size_t i = 0; i < element_size; ++i)
    {
        lc_col->insert(std::to_string(i));
        if (i % 4 == 0 || i % 6 == 0)
        {
            filter[i] = 1;
        }
    }

    SerializeCtx ctx(lc_col, lc_type->getDefaultSerialization());
    ASSERT_NO_FATAL_FAILURE(verifyByStep(ctx, lc_col, filter, verify_step));
}

// TEST(DeserializeFilterTest, Object)
// {
//     size_t element_size = 100;
//     DataTypePtr obj_type = std::make_shared<DataTypeObject>("json", false);
//     MutableColumnPtr obj_col = obj_type->createColumn();
//     PaddedPODArray<UInt8> filter(element_size, 0);
//     for (size_t i = 0; i < element_size; ++i)
//     {
//         Object row_val;
//         for (size_t j = 0; j <= i % 5; ++j)
//         {
//             String key = fmt::format("{}", 'a' + (i % 26));
//             row_val[key] = std::to_string(i);
//         }
//         obj_col->insert(row_val);
//         if (i % 3 == 0 || i % 17 == 0)
//         {
//             filter[i] = 1;
//         }
//     }

//     ColumnPtr im_obj_col = std::move(obj_col);
//     SerializeCtx ctx(im_obj_col, obj_type->getDefaultSerialization());

//     ColumnPtr result_col = im_obj_col->filter(filter, 0);
//     ASSERT_NO_FATAL_FAILURE(ctx.verify(filter, result_col));
// }
