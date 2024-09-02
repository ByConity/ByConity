#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Common/CurrentThread.h>
#include <Common/FieldVisitorToString.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>

#include <common/StringRef.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct AggregateFunctionGroupConcatData
{
private:
    using Self = AggregateFunctionGroupConcatData;

    Int32 size = -1;    /// -1 indicates that there is no value.
    String value;

    inline bool has() const
    {
        return size >= 0;
    }

    inline const char * getData() const
    {
        return value.data();
    }

    inline StringRef getStringRef() const
    {
        return StringRef(getData(), size);
    }

public:
    void insertResultInto(IColumn & to) const
    {
        if (has())
            assert_cast<ColumnString &>(to).insertData(getData(), size);
        else
            assert_cast<ColumnString &>(to).insertDefault();
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(size, buf);
        if (has())
            buf.write(getData(), size);
    }

    void read(ReadBuffer & buf, Arena *)
    {
        Int32 rhs_size;
        readBinary(rhs_size, buf);

        if (rhs_size > size)
            value.reserve(rhs_size);

        size = rhs_size;
        buf.read(value.data(), size);
    }

    void update(const std::vector<const ColumnString *> & string_columns, size_t row_num, Arena *, String separator)
    {
        if (has())
        {
            value.append(separator);
            size += separator.size();
        }
        else
            size = 0;

        for (const ColumnString * string_column : string_columns)
        {
            StringRef s = string_column->getDataAt(row_num);
            value.append(s.data);
            size += s.size;
        }
    }

    void update(const Self & to, Arena *, String separator)
    {
        StringRef s = to.getStringRef();
        if (has())
        {
            value.append(separator);
            value.append(s.data);
            size += separator.size() + s.size;
        }
        else
        {
            value = s.data;
            size = s.size;
        }
    }
};

class AggregateFunctionGroupConcat : public IAggregateFunctionDataHelper<AggregateFunctionGroupConcatData, AggregateFunctionGroupConcat>
{
private:
    String separator = ",";

public:
    explicit AggregateFunctionGroupConcat(const DataTypes & types, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupConcatData, AggregateFunctionGroupConcat>(types, {})
        {
            if (params.size() > 1)
                throw Exception("Aggregate function " + getName() + " require one parameter or less", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            if (params.size() == 1)
            {
                separator = applyVisitor(FieldVisitorToString(), params[0]);
                separator = separator.substr(1, separator.size()-2);
            }
        }

    String getName() const override { return "groupConcat"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeString>();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto to_string_func = FunctionFactory::instance().get("toString", CurrentThread::get().getQueryContext());
        DataTypePtr data_type_string = std::make_shared<DataTypeString>();
        std::vector<ColumnPtr> tmp_string_columns;
        std::vector<const ColumnString *> string_columns;
        for (size_t i = 0; i < argument_types.size(); i++)
        {
            if (!isStringOrFixedString(argument_types[i]))
            {
                ColumnsWithTypeAndName input = {{columns[i]->getPtr(), argument_types[i], std::to_string(i)}};
                tmp_string_columns.push_back(to_string_func->build(input)->execute(input, data_type_string, row_num));
                string_columns.push_back(checkAndGetColumn<ColumnString>(tmp_string_columns.back().get()));
            }
            else
                string_columns.push_back(assert_cast<const ColumnString *>(columns[i]));
        }
        
        this->data(place).update(string_columns, row_num, arena, separator);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).update(this->data(rhs), arena, separator);
    }

    bool allocatesMemoryInArena() const override { return true; }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        this->data(place).read(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to);
    }
};

struct Settings;

namespace
{

AggregateFunctionPtr createAggregateFunctionGroupConcat(const std::string & name [[maybe_unused]], const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    return AggregateFunctionPtr(new AggregateFunctionGroupConcat(argument_types, parameters));
}

}

void registerAggregateFunctionGroupConcat(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };
    factory.registerFunction("groupConcat", { createAggregateFunctionGroupConcat, properties }, AggregateFunctionFactory::CaseInsensitive);

    // Alias for compatibility with MySQL
    factory.registerAlias("GROUP_CONCAT", "groupConcat", AggregateFunctionFactory::CaseInsensitive);
}
}
