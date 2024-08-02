#include <Common/FieldVisitors.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <AggregateFunctions/AggregateFunctionSketchEstimate.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <memory>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

template <UInt8 K>
struct WithK
{
    template <typename T>
    using AggregateFunction = AggregateFunctionHllSketchEstimate<T, K>;
};

template <UInt8 K>
AggregateFunctionPtr createAggregateFunctionWithK(const DataTypes & argument_types, const Array & params)
{
    // the size of argument_types is ensured by createAggregateFunctionHllSketchEstimate
    const IDataType & argument_type = *argument_types[0];
    WhichDataType which(argument_type);

    bool ignore_wrong_date = argument_types.size() == 2;

    if (which.idx == TypeIndex::SketchBinary)
    {
        return std::make_shared<typename WithK<K>::template AggregateFunction<DataTypeSketchBinary>>(argument_types, params, ignore_wrong_date);
    }
    else if (which.isAggregateFunction())
    {
        return std::make_shared<typename WithK<K>::template AggregateFunction<DataTypeAggregateFunction>>(argument_types, params, ignore_wrong_date);
    }

    return std::make_shared<typename WithK<K>::template AggregateFunction<String>>(argument_types, params, ignore_wrong_date);
}

AggregateFunctionPtr createAggregateFunctionHllSketchEstimate
    (const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    UInt8 precision = 12;
    if (!params.empty())
    {
        if (params.size() > 2 || params.size() == 0)
        {
            throw Exception(
                "Aggregate function " + name + " requires two parameter or one.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        UInt64 precision_param = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        // This range is hardcoded below
        if (precision_param > 21 || precision_param < 4)
        {
            throw Exception(
                "Parameter for aggregate function " + name + "is out or range: [4, 21].", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }

        precision = precision_param;
    }

    if (argument_types.size() != 1 && argument_types.size() != 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    switch (precision)
    {
        case 4:
            return createAggregateFunctionWithK<4>(argument_types, params);
        case 5:
            return createAggregateFunctionWithK<5>(argument_types, params);
        case 6:
            return createAggregateFunctionWithK<6>(argument_types, params);
        case 7:
            return createAggregateFunctionWithK<7>(argument_types, params);
        case 8:
            return createAggregateFunctionWithK<8>(argument_types, params);
        case 9:
            return createAggregateFunctionWithK<9>(argument_types, params);
        case 10:
            return createAggregateFunctionWithK<10>(argument_types, params);
        case 11:
            return createAggregateFunctionWithK<11>(argument_types, params);
        case 12:
            return createAggregateFunctionWithK<12>(argument_types, params);
        case 13:
            return createAggregateFunctionWithK<13>(argument_types, params);
        case 14:
            return createAggregateFunctionWithK<14>(argument_types, params);
        case 15:
            return createAggregateFunctionWithK<15>(argument_types, params);
        case 16:
            return createAggregateFunctionWithK<16>(argument_types, params);
        case 17:
            return createAggregateFunctionWithK<17>(argument_types, params);
        case 18:
            return createAggregateFunctionWithK<18>(argument_types, params);
        case 19:
            return createAggregateFunctionWithK<19>(argument_types, params);
        case 20:
            return createAggregateFunctionWithK<20>(argument_types, params);
        case 21:
            return createAggregateFunctionWithK<21>(argument_types, params);
    }

    __builtin_unreachable();
}

AggregateFunctionPtr createAggregateFunctionHllSketchUnion
    (const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    UInt8 precision = 12;
    if (!params.empty())
    {
        if (params.size() != 1)
        {
            throw Exception(
                "Aggregate function " + name + " requires one parameter or less.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        UInt64 precision_param = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        // This range is hardcoded below
        if (precision_param > 21 || precision_param < 4)
        {
            throw Exception(
                "Parameter for aggregate function " + name + "is out or range: [4, 21].", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }

        precision = precision_param;
    }

    if (argument_types.size() != 1 && argument_types.size() != 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    if (argument_types[0]->getTypeId() != TypeIndex::SketchBinary)
        throw Exception("Incorrect type of arguments for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);


    switch (precision)
    {
        case 4:
            return std::make_shared<AggregateFunctionHLLSketchUnion<4>>(argument_types, params);
        case 5:
            return std::make_shared<AggregateFunctionHLLSketchUnion<5>>(argument_types, params);
        case 6:
            return std::make_shared<AggregateFunctionHLLSketchUnion<6>>(argument_types, params);
        case 7:
            return std::make_shared<AggregateFunctionHLLSketchUnion<7>>(argument_types, params);
        case 8:
            return std::make_shared<AggregateFunctionHLLSketchUnion<8>>(argument_types, params);
        case 9:
            return std::make_shared<AggregateFunctionHLLSketchUnion<9>>(argument_types, params);
        case 10:
            return std::make_shared<AggregateFunctionHLLSketchUnion<10>>(argument_types, params);
        case 11:
            return std::make_shared<AggregateFunctionHLLSketchUnion<11>>(argument_types, params);
        case 12:
            return std::make_shared<AggregateFunctionHLLSketchUnion<12>>(argument_types, params);
        case 13:
            return std::make_shared<AggregateFunctionHLLSketchUnion<13>>(argument_types, params);
        case 14:
            return std::make_shared<AggregateFunctionHLLSketchUnion<14>>(argument_types, params);
        case 15:
            return std::make_shared<AggregateFunctionHLLSketchUnion<15>>(argument_types, params);
        case 16:
            return std::make_shared<AggregateFunctionHLLSketchUnion<16>>(argument_types, params);
        case 17:
            return std::make_shared<AggregateFunctionHLLSketchUnion<17>>(argument_types, params);
        case 18:
            return std::make_shared<AggregateFunctionHLLSketchUnion<18>>(argument_types, params);
        case 19:
            return std::make_shared<AggregateFunctionHLLSketchUnion<19>>(argument_types, params);
        case 20:
            return std::make_shared<AggregateFunctionHLLSketchUnion<20>>(argument_types, params);
        case 21:
            return std::make_shared<AggregateFunctionHLLSketchUnion<21>>(argument_types, params);
    }

    __builtin_unreachable();
}


AggregateFunctionPtr createAggregateFunctionKllSketchEstimate
        (const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.size() != 1 && argument_types.size() != 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    Float64 quantile;
    if (!params.empty())
    {
        if (params.size() != 2)
        {
            throw Exception(
                "Aggregate function " + name + " requires two parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        quantile = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
        if (quantile < 0 || quantile > 1)
            throw Exception(
                "Aggregate function " + name + " first parameter should between 0 and 1.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        String type_name = params[1].safeGet<String>();
        bool ignore_wrong_date = argument_types.size() == 2;
        
        if (type_name == "UInt8")
            return std::make_shared<AggregateFunctionKllSketchEstimate<UInt8>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "UInt16")
            return std::make_shared<AggregateFunctionKllSketchEstimate<UInt16>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "UInt32")
            return std::make_shared<AggregateFunctionKllSketchEstimate<UInt32>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "UInt64")
            return std::make_shared<AggregateFunctionKllSketchEstimate<UInt64>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "Int8")
            return std::make_shared<AggregateFunctionKllSketchEstimate<Int8>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "Int16")
            return std::make_shared<AggregateFunctionKllSketchEstimate<Int16>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "Int32")
            return std::make_shared<AggregateFunctionKllSketchEstimate<Int32>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "Int64")
            return std::make_shared<AggregateFunctionKllSketchEstimate<Int64>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "Float32")
            return std::make_shared<AggregateFunctionKllSketchEstimate<Float32>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "Float64")
            return std::make_shared<AggregateFunctionKllSketchEstimate<Float64>>(quantile, argument_types, params, ignore_wrong_date);
        else
            throw Exception(
                "Aggregate function " + name + " second parameter not correct.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
    else
        throw Exception(
            "Aggregate function " + name + " arguments and parameters not correct.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}


AggregateFunctionPtr createAggregateFunctionQuantilesSketchEstimate
    (const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.size() != 1 && argument_types.size() != 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    Float64 quantile;
    if (!params.empty())
    {
        if (params.size() != 2)
        {
            throw Exception(
                "Aggregate function " + name + " requires two parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        quantile = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
        if (quantile < 0 || quantile > 1)
            throw Exception(
                "Aggregate function " + name + " first parameter should between 0 and 1.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        String type_name = params[1].safeGet<String>();
        bool ignore_wrong_date = argument_types.size() == 2;

        if (type_name == "UInt8")
            return std::make_shared<AggregateFunctionQuantilesSketchEstimate<UInt8>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "UInt16")
            return std::make_shared<AggregateFunctionQuantilesSketchEstimate<UInt16>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "UInt32")
            return std::make_shared<AggregateFunctionQuantilesSketchEstimate<UInt32>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "UInt64")
            return std::make_shared<AggregateFunctionQuantilesSketchEstimate<UInt64>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "Int8")
            return std::make_shared<AggregateFunctionQuantilesSketchEstimate<Int8>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "Int16")
            return std::make_shared<AggregateFunctionQuantilesSketchEstimate<Int16>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "Int32")
            return std::make_shared<AggregateFunctionQuantilesSketchEstimate<Int32>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "Int64")
            return std::make_shared<AggregateFunctionQuantilesSketchEstimate<Int64>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "Float32")
            return std::make_shared<AggregateFunctionQuantilesSketchEstimate<Float32>>(quantile, argument_types, params, ignore_wrong_date);
        else if (type_name == "Float64")
            return std::make_shared<AggregateFunctionQuantilesSketchEstimate<Float64>>(quantile, argument_types, params, ignore_wrong_date);
        else
            throw Exception(
                "Aggregate function " + name + " second parameter not correct.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
    else
        throw Exception(
            "Aggregate function " + name + " arguments and parameters not correct.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

AggregateFunctionPtr createAggregateFunctionQuantilesSketchUnion
    (const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.size() != 1 && argument_types.size() != 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (!params.empty())
    {
        if (params.size() != 1)
        {
            throw Exception(
                "Aggregate function " + name + " requires one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        String type_name = params[0].safeGet<String>();
        bool ignore_wrong_date = argument_types.size() == 2;

        if (type_name == "UInt8")
            return std::make_shared<AggregateFunctionQuantilesSketchUnion<UInt8>>(argument_types, params, ignore_wrong_date);
        else if (type_name == "UInt16")
            return std::make_shared<AggregateFunctionQuantilesSketchUnion<UInt16>>(argument_types, params, ignore_wrong_date);
        else if (type_name == "UInt32")
            return std::make_shared<AggregateFunctionQuantilesSketchUnion<UInt32>>(argument_types, params, ignore_wrong_date);
        else if (type_name == "UInt64")
            return std::make_shared<AggregateFunctionQuantilesSketchUnion<UInt64>>(argument_types, params, ignore_wrong_date);
        else if (type_name == "Int8")
            return std::make_shared<AggregateFunctionQuantilesSketchUnion<Int8>>(argument_types, params, ignore_wrong_date);
        else if (type_name == "Int16")
            return std::make_shared<AggregateFunctionQuantilesSketchUnion<Int16>>(argument_types, params, ignore_wrong_date);
        else if (type_name == "Int32")
            return std::make_shared<AggregateFunctionQuantilesSketchUnion<Int32>>(argument_types, params, ignore_wrong_date);
        else if (type_name == "Int64")
            return std::make_shared<AggregateFunctionQuantilesSketchUnion<Int64>>(argument_types, params, ignore_wrong_date);
        else if (type_name == "Float32")
            return std::make_shared<AggregateFunctionQuantilesSketchUnion<Float32>>(argument_types, params, ignore_wrong_date);
        else if (type_name == "Float64")
            return std::make_shared<AggregateFunctionQuantilesSketchUnion<Float64>>(argument_types, params, ignore_wrong_date);
        else
            throw Exception(
                "Aggregate function " + name + " second parameter not correct.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
    else
        throw Exception(
            "Aggregate function " + name + " arguments and parameters not correct.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

}

void registerAggregateFunctionHllSketchEstimate(AggregateFunctionFactory & factory)
{
    factory.registerFunction("hllSketchEstimate", createAggregateFunctionHllSketchEstimate);
    factory.registerFunction("hllSketchUnion", createAggregateFunctionHllSketchUnion);
    factory.registerFunction("kllSketchEstimate", createAggregateFunctionKllSketchEstimate);
    factory.registerFunction("quantilesSketchEstimate", createAggregateFunctionQuantilesSketchEstimate);
    factory.registerFunction("quantilesSketchUnion", createAggregateFunctionQuantilesSketchUnion);
}

}
