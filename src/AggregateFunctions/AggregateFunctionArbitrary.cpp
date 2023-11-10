#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <DataTypes/IDataType.h>

#include <cstdlib>

namespace DB
{

struct AggregateFunctionArbitraryDataGeneric
{
private:
    using Self = AggregateFunctionArbitraryDataGeneric;
    Field value;

    inline bool has() const
    {
        // Arbitrary favors NULL values less and only outputs NULL if every row in the group is NULL
        return !value.isNull();
    }

    inline bool randomChoice() const
    {
        return std::rand() % 2;
    }

public:
    void insertResultInto(IColumn & to) const
    {
        if (has())
            to.insert(value);
        else
            to.insertDefault();
    }

    void set(const IColumn & column, size_t row_num, Arena *)
    {
        column.get(row_num, value);
    }

    void set(const Self & to, Arena *)
    {
        value = to.value;
    }

    void update(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (!has() || randomChoice())
        {
            set(column, row_num, arena);
        }
    }

    void update(const Self & to, Arena * arena)
    {
        if (to.has() && (!has() || randomChoice()))
        {
            set(to, arena);
        }
    }

    void write(WriteBuffer & buf, const ISerialization & serialization) const
    {
        if (!value.isNull())
        {
            writeBinary(true, buf);
            serialization.serializeBinary(value, buf);
        }
        else
            writeBinary(false, buf);
    }

    void read(ReadBuffer & buf, const ISerialization & serialization, Arena *)
    {
        bool is_not_null;
        readBinary(is_not_null, buf);

        if (is_not_null)
            serialization.deserializeBinary(value, buf);
    }
};

/// Gets random value from a column
template <typename Data>
class AggregateFunctionArbitrary : public IAggregateFunctionDataHelper<Data, AggregateFunctionArbitrary<Data>>
{
private:
    SerializationPtr serialization;

public:
    explicit AggregateFunctionArbitrary(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionArbitrary<Data>>({type}, {})
        , serialization(type->getDefaultSerialization()) {}

    String getName() const override { return "groupArbitrary"; }

    DataTypePtr getReturnType() const override
    {
        return this->argument_types.at(0);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).update(*columns[0], row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).update(this->data(rhs), arena);
    }

    bool allocatesMemoryInArena() const override { return false; }

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

AggregateFunctionPtr createAggregateFunctionArbitrary(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    return AggregateFunctionPtr(new AggregateFunctionArbitrary<AggregateFunctionArbitraryDataGeneric>(argument_types[0]));
}

}

void registerAggregateFunctionArbitrary(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupArbitrary", createAggregateFunctionArbitrary);

    /// Alias for compatibility with MySQL.
    factory.registerAlias("arbitrary", "groupArbitrary", AggregateFunctionFactory::CaseInsensitive);
}

}
