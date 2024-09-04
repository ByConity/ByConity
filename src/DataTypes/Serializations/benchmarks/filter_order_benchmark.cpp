#include <random>
#include <Core/Field.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <cassandra/include/cassandra.h>
#include <google-benchmark/include/benchmark/benchmark.h>

namespace DB
{

class DataSerializationCtx
{
public:
    DataSerializationCtx(std::unique_ptr<IDataType> data_type_, size_t column_size_,
        size_t filtered_size_, std::function<Field(size_t)> value_gen_):
            column_size(column_size_), filtered_size(filtered_size_),
            data_type(std::move(data_type_))
    {
        MutableColumnPtr data_col = data_type->createColumn();
        filter = ColumnUInt8::create();

        std::default_random_engine re;
        std::uniform_int_distribution<int> dist(0, column_size);
        size_t selected = 0;
        for (size_t i = 0; i < column_size; ++i)
        {
            if (value_gen_)
                data_col->insert(value_gen_(i));
            else
                data_col->insertDefault();

            UInt8 filter_val = 0;
            if (selected >= filtered_size)
                filter_val = 0;
            else if (filtered_size - selected >= column_size - i)
                filter_val = 1;
            else
                filter_val = dist(re) <= filtered_size ? 1 : 0;
            selected += filter_val;
            filter->insert(Field(filter_val));
        }

        /// Seriliazation
        std::map<String, std::unique_ptr<WriteBufferFromString>> writers;
        ISerialization::SerializeBinaryBulkSettings settings;
        settings.getter = [this, &writers](const ISerialization::SubstreamPath& path) {
            String substream_path = ISerialization::getSubcolumnNameForStream(path);
            if (auto iter = writers.find(substream_path); iter != writers.end())
            {
                return iter->second.get();
            }

            String& data = data_store[substream_path];
            writers[substream_path] = std::make_unique<WriteBufferFromString>(data);
            return writers[substream_path].get();
        };

        ISerialization::SerializeBinaryBulkStatePtr state;
        auto serialization = data_type->getDefaultSerialization();
        serialization->serializeBinaryBulkStatePrefix(*data_col, settings, state);
        serialization->serializeBinaryBulkWithMultipleStreams(*data_col, 0, data_col->size(), settings, state);
        serialization->serializeBinaryBulkStateSuffix(settings, state);
    }

    void performDeserialize(bool filter_when_read)
    {
        std::map<String, std::unique_ptr<ReadBufferFromString>> readers;
        ISerialization::DeserializeBinaryBulkSettings settings;
        settings.getter = [this, &readers](const ISerialization::SubstreamPath& path) {
            String substream_path = ISerialization::getSubcolumnNameForStream(path);
            if (auto iter = readers.find(substream_path); iter != readers.end())
            {
                return iter->second.get();
            }

            const String& data = data_store[substream_path];
            readers[substream_path] = std::make_unique<ReadBufferFromString>(data);
            return readers[substream_path].get();
        };
        settings.filter = filter_when_read ? filter->getData().data() : nullptr;

        ColumnPtr result_ptr = data_type->createColumn();

        ISerialization::SubstreamsCache cache;

        ISerialization::DeserializeBinaryBulkStatePtr state;
        auto serialization = data_type->getDefaultSerialization();
        serialization->deserializeBinaryBulkStatePrefix(settings, state);
        serialization->deserializeBinaryBulkWithMultipleStreams(result_ptr, column_size,
            settings, state, &cache);

        if (!filter_when_read)
        {
            result_ptr->filter(filter->getData(), filtered_size);
        }
    }

    size_t column_size;
    size_t filtered_size;

    std::unique_ptr<IDataType> data_type;

    std::map<String, String> data_store;
    ColumnUInt8::MutablePtr filter;
};

template<typename T, typename TValGen>
class BenchFilterFixture: public benchmark::Fixture
{
public:
    void SetUp(::benchmark::State& state) override
    {
        size_t column_size = state.range(1);
        size_t filtered_size = state.range(2);
        size_t row_size = state.range(3);

        ctx = std::make_unique<DataSerializationCtx>(std::make_unique<T>(),
            column_size, filtered_size, TValGen(row_size));
    }

    void TearDown(::benchmark::State&) override
    {
        ctx = nullptr;
    }

    std::unique_ptr<DataSerializationCtx> ctx;
};

struct UInt32ValGen
{
    explicit UInt32ValGen(size_t) {}

    Field operator()(size_t row) const
    {
        return Field(static_cast<UInt32>(row));
    }
};

String randomString(size_t length)
{
    static const char alphanum[] = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";

    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<int> distribution(0, sizeof(alphanum) / sizeof(char));

    //    srand((unsigned) time(NULL) * getpid());

    String str(length, '\0');
    for (size_t i = 0; i < length; i++)
    {
        str[i] = alphanum[distribution(generator)];
    }
    return str;
}

struct StrValueGen
{
    explicit StrValueGen(size_t row_length_): row_length(row_length_) {}

    Field operator()(size_t) const
    {
        return randomString(row_length);
    }

    size_t row_length;
};

void intArgumentsGen(benchmark::internal::Benchmark* benchmark)
{
    int total_rows = 1 * 1024 * 1024;
    for (int filtered_rows = 1; filtered_rows <= total_rows; filtered_rows += 64 * 1024)
    {
        for (int filter_when_read = 0; filter_when_read < 2; ++filter_when_read)
        {
            benchmark->Args({filter_when_read, total_rows, filtered_rows, 1});
        }
    }
}

void strArgumentsGen(benchmark::internal::Benchmark* benchmark)
{
    int total_rows = 128 * 1024;
    for (int row_size = 0; row_size < 4 * 1024; row_size += 1024)
    {
        for (int filtered_rows = 0; filtered_rows <= total_rows; filtered_rows += 64 * 1024)
        {
            for (int filter_when_read = 0; filter_when_read < 2; ++filter_when_read)
            {
                benchmark->Args({filter_when_read, total_rows, filtered_rows, row_size});
            }
        }
    }
}

BENCHMARK_TEMPLATE_DEFINE_F(BenchFilterFixture, TestUInt32Filter, DataTypeUInt32, UInt32ValGen)(benchmark::State& state)
{
    for (auto _ : state)
    {
        ctx->performDeserialize(state.range(0));
    }
}

BENCHMARK_TEMPLATE_DEFINE_F(BenchFilterFixture, TestStringFilter, DataTypeString, StrValueGen)(benchmark::State& state)
{
    for (auto _ : state)
    {
        ctx->performDeserialize(state.range(0));
    }
}

BENCHMARK_REGISTER_F(BenchFilterFixture, TestUInt32Filter)->Apply(intArgumentsGen);
BENCHMARK_REGISTER_F(BenchFilterFixture, TestStringFilter)->Apply(strArgumentsGen);

}

BENCHMARK_MAIN();
