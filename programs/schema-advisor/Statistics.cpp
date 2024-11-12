#include "Statistics.h"

#include <AggregateFunctions/AggregateFunctionNull.h>
#include <AggregateFunctions/AggregateFunctionUniq.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/HelpersMinMaxAny.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionCountByGranularity.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <Core/Field.h>
#include <Functions/FunctionHelpers.h>
#include "Columns/IColumn.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <bool is_exact, template <typename, bool> typename Data, template <bool, bool, bool> typename DataForVariadic, bool is_able_to_parallelize_merge>
AggregateFunctionPtr
createAggregateFunctionUniq(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    assertNoParameters(name, params);

    if (argument_types.empty())
        throw Exception("Incorrect number of arguments for aggregate function " + name,
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// We use exact hash function if the user wants it;
    /// or if the arguments are not contiguous in memory, because only exact hash function have support for this case.
    /// bool use_exact_hash_function = is_exact || !isAllArgumentsContiguousInMemory(argument_types);

    if (argument_types.size() == 1)
    {
        const IDataType & argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniq, Data, is_able_to_parallelize_merge>(*argument_types[0], argument_types));

        WhichDataType which(argument_type);
        if (res)
            return res;
        else if (which.isDate())
            return std::make_shared<AggregateFunctionUniq<DataTypeDate::FieldType, Data<DataTypeDate::FieldType, is_able_to_parallelize_merge>>>(argument_types);
        else if (which.isDate32())
            return std::make_shared<AggregateFunctionUniq<DataTypeDate32::FieldType, Data<DataTypeDate32::FieldType, is_able_to_parallelize_merge>>>(argument_types);
        else if (which.isDateTime())
            return std::make_shared<AggregateFunctionUniq<DataTypeDateTime::FieldType, Data<DataTypeDateTime::FieldType, is_able_to_parallelize_merge>>>(argument_types);
        else if (which.isStringOrFixedString())
            return std::make_shared<AggregateFunctionUniq<String, Data<String, is_able_to_parallelize_merge>>>(argument_types);
        else if (which.isUUID())
            return std::make_shared<AggregateFunctionUniq<DataTypeUUID::FieldType, Data<DataTypeUUID::FieldType, is_able_to_parallelize_merge>>>(argument_types);
        else if (which.isTuple())
        {
            /*
            if (use_exact_hash_function)
                return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<true, true, is_able_to_parallelize_merge>>>(argument_types);
            else
                return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<false, true, is_able_to_parallelize_merge>>>(argument_types);
            */
            throw Exception("Unsupported tuple data type for uniqExtract", ErrorCodes::BAD_ARGUMENTS);
        }
    }

    /* "Variadic" method also works as a fallback generic case for single argument.
    if (use_exact_hash_function)
        return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<true, false, is_able_to_parallelize_merge>>>(argument_types);
    else
        return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic<false, false, is_able_to_parallelize_merge>>>(argument_types);
    */
    throw Exception("Unsupported arguments size " + std::to_string(argument_types.size()), ErrorCodes::BAD_ARGUMENTS);
}

DataTypes transformArguments(const DataTypes & arguments)
{
    size_t size = arguments.size();
    DataTypes res(size);
    for (size_t i = 0; i < size; ++i)
        res[i] = removeNullable(arguments[i]);
    return res;
}

Field UniExtract::executeOnColumn(const ColumnPtr & column, const DataTypePtr & type)
{
    String name = "uniqExact";
    DataTypes argument_types(1);
    argument_types[0] = type;
    Array parameters;

    AggregateFunctionPtr nested_function =
        createAggregateFunctionUniq<true, AggregateFunctionUniqExactData, AggregateFunctionUniqExactDataForVariadic, false /* is_able_to_parallelize_merge */>
            (name, transformArguments(argument_types), parameters, nullptr);
    AggregateFunctionPtr aggregate_function = type->isNullable()
        ? std::make_shared<AggregateFunctionNullUnary<false, true>>(nested_function, argument_types, parameters)
        : nested_function;

    size_t total_size_of_aggregate_states = 0;    /// The total size of the row from the aggregate functions.
    // add info to track alignment requirement
    // If there are states whose alignment are v1, ..vn, align_aggregate_states will be max(v1, ... vn)
    size_t align_aggregate_states = 1;
    total_size_of_aggregate_states = aggregate_function->sizeOfData();
    align_aggregate_states = std::max(align_aggregate_states, aggregate_function->alignOfData());

    std::shared_ptr<Arena> aggregates_pool = std::make_shared<Arena>();    /// The pool that is currently used for allocation.
    AggregateDataPtr place = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
    try
    {
        /** An exception may occur if there is a shortage of memory.
         * In order that then everything is properly destroyed, we "roll back" some of the created states.
         * The code is not very convenient.
         */
        aggregate_function->create(place);
    }
    catch (...)
    {
        aggregate_function->destroy(place);
        throw Exception("Cannot allocate memory", ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }
    size_t rows = column->size();
    ColumnRawPtrs column_ptrs;
    column_ptrs.emplace_back(column.get());
    const IColumn ** batch_arguments = column_ptrs.data();

    aggregate_function->addBatchSinglePlace(rows, place, batch_arguments, nullptr);

    DataTypePtr result_type = std::make_shared<DataTypeUInt64>();
    ColumnPtr result_column = result_type->createColumn();
    MutableColumnPtr mutable_column = result_column->assumeMutable();
    aggregate_function->insertResultInto(place, *mutable_column, nullptr);
    return (*result_column)[0];
}

Field UniExtract::executeOnColumnArray(const ColumnPtr & column, const DataTypePtr & type)
{
    if (!isArray(type))
        return 0;

    const auto * array_type = checkAndGetDataType<DataTypeArray>(type.get());
    const auto& nested_type = array_type->getNestedType();

    String inner_func_name = "uniqExact";
    String combinator_suffix = "Array";

    DataTypes nested_argument_types{nested_type};
    DataTypes argument_types{type};
    Array parameters;

    // For inner func uniqExact
    AggregateFunctionPtr nested_function =
        createAggregateFunctionUniq<true, AggregateFunctionUniqExactData, AggregateFunctionUniqExactDataForVariadic, false /* is_able_to_parallelize_merge */>
            (inner_func_name, transformArguments(nested_argument_types), parameters, nullptr);
    AggregateFunctionPtr uniq_exact_function = type->isNullable()
        ? std::make_shared<AggregateFunctionNullUnary<false, true>>(nested_function, nested_argument_types, parameters)
        : nested_function;

    // For combinator -Array
    AggregateFunctionCombinatorPtr array_combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(combinator_suffix);
    AggregateFunctionPtr uniq_exact_array_function = array_combinator->transformAggregateFunction(
        uniq_exact_function, {}, argument_types, parameters);

    size_t total_size_of_aggregate_states = 0;
    size_t align_aggregate_states = 1;
    total_size_of_aggregate_states = uniq_exact_array_function->sizeOfData();
    align_aggregate_states = std::max(align_aggregate_states, uniq_exact_array_function->alignOfData());

    std::shared_ptr<Arena> aggregates_pool = std::make_shared<Arena>();
    AggregateDataPtr place = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
    try
    {
        uniq_exact_array_function->create(place);
    }
    catch (...)
    {
        uniq_exact_array_function->destroy(place);
        throw Exception("Cannot allocate memory", ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }
    size_t rows = column->size();
    ColumnRawPtrs column_ptrs;
    column_ptrs.emplace_back(column.get());
    const IColumn ** batch_arguments = column_ptrs.data();

    uniq_exact_array_function->addBatchSinglePlace(rows, place, batch_arguments, nullptr);

    DataTypePtr result_type = std::make_shared<DataTypeUInt64>();
    ColumnPtr result_column = result_type->createColumn();
    MutableColumnPtr mutable_column = result_column->assumeMutable();
    uniq_exact_array_function->insertResultInto(place, *mutable_column, nullptr);

    return (*result_column)[0];
}

AggregateFunctionPtr createAggregateFunctionMin(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMinData>(name, argument_types, parameters, settings));
}

Field Min::executeOnColumn(const ColumnPtr & column, const DataTypePtr & type)
{
    String name = "min";
    DataTypes argument_types(1);
    argument_types[0] = type;
    Array parameters;

    AggregateFunctionPtr nested_function = createAggregateFunctionMin(name, transformArguments(argument_types), parameters, nullptr);
    AggregateFunctionPtr aggregate_function = type->isNullable() 
        ? std::make_shared<AggregateFunctionNullUnary<false, true>>(nested_function, argument_types, parameters) 
        : nested_function;

    size_t total_size_of_aggregate_states = 0;    /// The total size of the row from the aggregate functions.
    // add info to track alignment requirement
    // If there are states whose alignment are v1, ..vn, align_aggregate_states will be max(v1, ... vn)
    size_t align_aggregate_states = 1;
    total_size_of_aggregate_states = aggregate_function->sizeOfData();
    align_aggregate_states = std::max(align_aggregate_states, aggregate_function->alignOfData());

    std::shared_ptr<Arena> aggregates_pool = std::make_shared<Arena>();    /// The pool that is currently used for allocation.
    AggregateDataPtr place = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
    try
    {
        /** An exception may occur if there is a shortage of memory.
         * In order that then everything is properly destroyed, we "roll back" some of the created states.
         * The code is not very convenient.
         */
        aggregate_function->create(place);
    }
    catch (...)
    {
        aggregate_function->destroy(place);
        throw Exception("Cannot allocate memory", ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }
    size_t rows = column->size();
    ColumnRawPtrs column_ptrs;
    column_ptrs.emplace_back(column.get());
    const IColumn ** batch_arguments = column_ptrs.data();
    
    aggregate_function->addBatchSinglePlace(rows, place, batch_arguments, nullptr);

    ColumnPtr result_column = type->createColumn();
    MutableColumnPtr mutable_column = result_column->assumeMutable();
    aggregate_function->insertResultInto(place, *mutable_column, nullptr);
    return (*result_column)[0];
}

AggregateFunctionPtr createAggregateFunctionMax(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMaxData>(name, argument_types, parameters, settings));
}

Field Max::executeOnColumn(const ColumnPtr & column, const DataTypePtr & type)
{
    String name = "max";
    DataTypes argument_types(1);
    argument_types[0] = type;
    Array parameters;

    AggregateFunctionPtr nested_function = createAggregateFunctionMax(name, transformArguments(argument_types), parameters, nullptr);
    AggregateFunctionPtr aggregate_function = type->isNullable() 
        ? std::make_shared<AggregateFunctionNullUnary<false, true>>(nested_function, argument_types, parameters) 
        : nested_function;

    size_t total_size_of_aggregate_states = 0;    /// The total size of the row from the aggregate functions.
    // add info to track alignment requirement
    // If there are states whose alignment are v1, ..vn, align_aggregate_states will be max(v1, ... vn)
    size_t align_aggregate_states = 1;
    total_size_of_aggregate_states = aggregate_function->sizeOfData();
    align_aggregate_states = std::max(align_aggregate_states, aggregate_function->alignOfData());

    std::shared_ptr<Arena> aggregates_pool = std::make_shared<Arena>();    /// The pool that is currently used for allocation.
    AggregateDataPtr place = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
    try
    {
        /** An exception may occur if there is a shortage of memory.
         * In order that then everything is properly destroyed, we "roll back" some of the created states.
         * The code is not very convenient.
         */
        aggregate_function->create(place);
    }
    catch (...)
    {
        aggregate_function->destroy(place);
        throw Exception("Cannot allocate memory", ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }
    size_t rows = column->size();
    ColumnRawPtrs column_ptrs;
    column_ptrs.emplace_back(column.get());
    const IColumn ** batch_arguments = column_ptrs.data();
    
    aggregate_function->addBatchSinglePlace(rows, place, batch_arguments, nullptr);

    ColumnPtr result_column = type->createColumn();
    MutableColumnPtr mutable_column = result_column->assumeMutable();
    aggregate_function->insertResultInto(place, *mutable_column, nullptr);
    return (*result_column)[0];
}

AggregateFunctionPtr createAggregateFunctionCountByGranularity(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.size() != 1)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const IDataType & argument_type = *argument_types[0];
    WhichDataType which(argument_type);

    if (which.isNothing() || which.isArray() || which.isFunction() || which.isAggregateFunction() || which.isMap() || which.isBitmap64()
        || which.isSet() || which.isTuple() || which.isInterval() || which.isDecimal() || which.isInt128() || which.isUInt128() || which.isDateOrDateTime())
    {
        throw Exception(
            "argument of " + name
                + " can not be "
                  "(Nothing,Array,Function,"
                  "AggregateFunction,Map,Bitmap64,"
                  "Set,Tuple,Interval,"
                  "Decimal,Int128,UInt128, DateOrDateTime)",
            ErrorCodes::BAD_ARGUMENTS);
    }
    else if (which.isStringOrFixedString())
    {
        //auto a =AggregateFunctionCountByGranularity<String>(argument_types, params);
        return std::make_shared<AggregateFunctionCountByGranularity<String>>(argument_types, params);
    }
    else if (which.isInt8())
    {
        auto a = AggregateFunctionCountByGranularity<Int8>(argument_types, params);
        return std::make_shared<AggregateFunctionCountByGranularity<Int8>>(argument_types, params);
    }
    else if (which.isUInt8() || which.isEnum8())
    {
        return std::make_shared<AggregateFunctionCountByGranularity<UInt8>>(argument_types, params);
    }
    else if (which.isInt16())
    {
        return std::make_shared<AggregateFunctionCountByGranularity<Int16>>(argument_types, params);
    }
    else if (which.isUInt16() || which.isEnum16())
    {
        return std::make_shared<AggregateFunctionCountByGranularity<UInt16>>(argument_types, params);
    }
    else if (which.isInt32())
    {
        return std::make_shared<AggregateFunctionCountByGranularity<Int32>>(argument_types, params);
    }
    else if (which.isUInt32() || which.isDateTime())
    {
        return std::make_shared<AggregateFunctionCountByGranularity<UInt32>>(argument_types, params);
    }
    else if (which.isInt64())
    {
        return std::make_shared<AggregateFunctionCountByGranularity<Int64>>(argument_types, params);
    }
    else if (which.isUInt64())
    {
        return std::make_shared<AggregateFunctionCountByGranularity<UInt64>>(argument_types, params);
    }
    //        TODO can't support Int128 for now
    //        else if (which.isInt128())
    //        {
    //            return std::make_shared<AggregateFunctionCountByGranularity<Int128>>(argument_types, params);
    //        }
    else if (which.isUInt128())
    {
        return std::make_shared<AggregateFunctionCountByGranularity<UInt128>>(argument_types, params);
    }
    else if (which.isFloat32())
    {
        return std::make_shared<AggregateFunctionCountByGranularity<Float32>>(argument_types, params);
    }
    else if (which.isFloat64())
    {
        return std::make_shared<AggregateFunctionCountByGranularity<Float64>>(argument_types, params);
    }
    // TODO can't support Decimal for now
    //        else if (which.isDecimal32())
    //        {
    //            return std::make_shared<AggregateFunctionCountByGranularity<Decimal32>>(argument_types, params);
    //        }
    //        else if (which.isDecimal64() || which.isDateTime64())
    //        {
    //            return std::make_shared<AggregateFunctionCountByGranularity<Decimal64>>(argument_types, params);
    //        }
    //        else if (which.isDecimal128())
    //        {
    //            return std::make_shared<AggregateFunctionCountByGranularity<Decimal128>>(argument_types, params);
    //        }
    else
    {
        return std::make_shared<AggregateFunctionCountByGranularity<String>>(argument_types, params);
    }

    __builtin_unreachable();
}

ColumnPtr CountByGranularity::executeOnColumn(const ColumnPtr & column, const DataTypePtr & type)
{
    String name = "countByGranularity";
    DataTypes argument_types(1);
    argument_types[0] = recursiveRemoveLowCardinality(type);
    Array parameters;

    AggregateFunctionPtr nested_function = createAggregateFunctionCountByGranularity(name, transformArguments(argument_types), parameters, nullptr);
    AggregateFunctionPtr aggregate_function = argument_types[0]->isNullable() ? std::make_shared<AggregateFunctionNullUnary<false, true>>(nested_function, argument_types, parameters) 
        : nested_function;

    size_t total_size_of_aggregate_states = 0;    /// The total size of the row from the aggregate functions.
    // add info to track alignment requirement
    // If there are states whose alignment are v1, ..vn, align_aggregate_states will be max(v1, ... vn)
    size_t align_aggregate_states = 1;
    total_size_of_aggregate_states = aggregate_function->sizeOfData();
    align_aggregate_states = std::max(align_aggregate_states, aggregate_function->alignOfData());

    std::shared_ptr<Arena> aggregates_pool = std::make_shared<Arena>();    /// The pool that is currently used for allocation.
    AggregateDataPtr place = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
    try
    {
        /** An exception may occur if there is a shortage of memory.
         * In order that then everything is properly destroyed, we "roll back" some of the created states.
         * The code is not very convenient.
         */
        aggregate_function->create(place);
    }
    catch (...)
    {
        aggregate_function->destroy(place);
        throw Exception("Cannot allocate memory", ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }
    size_t rows = column->size();
    ColumnRawPtrs column_ptrs;
    column_ptrs.emplace_back(recursiveRemoveLowCardinality(column).get());
    const IColumn ** batch_arguments = column_ptrs.data();
    
    aggregate_function->addBatchSinglePlace(rows, place, batch_arguments, nullptr);

    ColumnPtr result_column = nested_function->getReturnType()->createColumn();
    MutableColumnPtr mutable_column = result_column->assumeMutable();
    aggregate_function->insertResultInto(place, *mutable_column, nullptr);

    return result_column;
}

}
