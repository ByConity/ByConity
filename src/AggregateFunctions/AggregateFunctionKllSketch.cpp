#include <AggregateFunctions/AggregateFunctionCboFamily.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <Statistics/Base64.h>
#include <Statistics/StatsKllSketchImpl.h>

// TODO: use datasketches
namespace DB
{

template <typename T>
struct KllData
{
    // TODO: use statistics object
    // datasketches::kll_sketch<T> data_{};
    using EmbeddedType = std::conditional_t<std::is_same_v<T, UUID>, UInt128, T>;
    Statistics::StatsKllSketchImpl<EmbeddedType> data_;

    void add(T value) { data_.update(value); }

    void merge(const KllData & rhs) { data_.merge(rhs.data_); }

    using BlobType = String;
    void write(WriteBuffer & buf) const
    {
        BlobType blob = data_.serialize();
        writeBinary(blob, buf);
    }

    void read(ReadBuffer & buf)
    {
        BlobType blob;
        readBinary(blob, buf);
        data_.deserialize(blob);
    }

    std::string getText() const { return data_.to_string(); }

    void insertResultInto(IColumn & to) const
    {
        auto blob_raw = data_.serialize();
        auto blob_b64 = base64Encode(blob_raw);
        static_cast<ColumnString &>(to).insertData(blob_b64.c_str(), blob_b64.size());
    }

    static String getName() { return "kll"; }
};


template <template <typename> class Function>
AggregateFunctionPtr
createAggregateFunctionKllSketch(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    DataTypePtr data_type = argument_types[0];

    // TODO: support most data_type
    if (isColumnedAsNumber(data_type))
    {
        res.reset(createWithNumericBasedType<Function>(*data_type, argument_types));
    }
    else if (isStringOrFixedString(data_type))
    {
        res = std::make_shared<AggregateFunctionCboFamilyForString<KllData<String>>>(argument_types);
    }

    if (!res)
        throw Exception(
            "Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}

template <typename T>
struct FuncImpl
{
    using Func = AggregateFunctionCboFamily<KllData, T>;
};
template <typename T>
using Func = typename FuncImpl<T>::Func;


void registerAggregateFunctionKllSketch(AggregateFunctionFactory & factory)
{
    AggregateFunctionWithProperties functor;
    functor.creator = createAggregateFunctionKllSketch<Func>;
    factory.registerFunction("kll", functor);
}

}
