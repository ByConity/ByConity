#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/ColumnString.h>
#include <Common/FieldVisitorToString.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>

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

    void write(WriteBuffer & buf, const ISerialization & /*serialization*/) const
    {
        writeBinary(size, buf);
        if (has())
            buf.write(getData(), size);
    }

    void read(ReadBuffer & buf, const ISerialization & /*serialization*/, Arena *)
    {
        Int32 rhs_size;
        readBinary(rhs_size, buf);

        if (rhs_size > size)
            value.reserve(rhs_size);

        size = rhs_size;
        buf.read(value.data(), size);
    }

    void append(StringRef rhs, String separator = ",")
    {
        if (has())
        {
            value.append(separator);
            value.append(rhs.data);
            size += separator.size() + rhs.size;
        }
        else
        {
            value = rhs.data;
            size = rhs.size;
        }
    }

    void update(const IColumn & column, size_t row_num, Arena *, String separator = ",")
    {
        append(assert_cast<const ColumnString &>(column).getDataAt(row_num), separator);
    }

    void update(const Self & to, Arena *, String separator = ",")
    {
        append(to.getStringRef(), separator);
    }
};

class AggregateFunctionGroupConcat : public IAggregateFunctionDataHelper<AggregateFunctionGroupConcatData, AggregateFunctionGroupConcat>
{
private:
    SerializationPtr serialization;
    String separator = ",";

public:
    explicit AggregateFunctionGroupConcat(const DataTypePtr & type, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupConcatData, AggregateFunctionGroupConcat>({type}, {})
        , serialization(type->getDefaultSerialization())
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
        this->data(place).update(*columns[0], row_num, arena, separator);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).update(this->data(rhs), arena, separator);
    }

    bool allocatesMemoryInArena() const override { return true; }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf, *serialization);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        this->data(place).read(buf, *serialization, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to);
    }
};

struct Settings;

namespace
{

AggregateFunctionPtr createAggregateFunctionGroupConcat(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);

    DataTypePtr data_type(argument_types[0]);
    if (!isStringOrFixedString(data_type))
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return AggregateFunctionPtr(new AggregateFunctionGroupConcat(argument_types[0], parameters));
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
