#include "AggregateFunctionAttribution.h"
#include <string>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include "DataTypes/IDataType.h"
#include "DataTypes/getLeastSupertype.h"
#include "Functions/FunctionHelpers.h"

namespace DB
{
namespace
{
    template <typename TYPE>
    std::vector<TYPE> transformArrayIntoVector(const Array& array)
    {
        std::vector<TYPE> res;
        for (Field field : array)
            res.push_back(field.get<TYPE>());
        return res;
    }

    void checkValidAttrIndex(std::vector<UInt16> & attr_list, ssize_t size)
    {
        /// For compatible: make the specific placeholder [0] to []
        if (attr_list.size() == 1 && attr_list[0] == 0)
        {
            attr_list.clear();
            return;
        }
        for (auto index : attr_list)
        {
            if (index >= size)
                throw Exception("Index " + std::to_string(index) + " in attr_list is out of boundary " + std::to_string(size), ErrorCodes::BAD_ARGUMENTS);
        }
    }

    AggregateFunctionPtr createAggregateFunctionAttribution(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
    {
        if (params.size() < 3 ||  params.size() > 8)
            throw Exception("Aggregate function " + name + " requires 4-8 arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const int GROUP_ATTR_INDEX = 3;
        const int RELATED_ATTR_INDEX = 4;
        if (argument_types.size() < GROUP_ATTR_INDEX)
            throw Exception("Aggregate function " + name + " requires at least (time, event_id, target_value, {attrs})", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        std::vector<UInt8> events_flag = transformArrayIntoVector<UInt8>(params[0].get<Array>());
        if (events_flag.size() != 2)
            throw Exception("The first param of aggregate function " + name + " requires to contain two elements", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        UInt64 window = params[1].get<UInt64>();
        UInt8 mode_flag = params[2].get<UInt8>();
        UInt8 other_transform = false;
        UInt8 need_group = false;
        std::vector<UInt16> attr_relation_matrix{};
        if (params.size() > 3)
            other_transform = params[3].get<UInt8>();
        if (params.size() > 4) 
            need_group = params[4].get<UInt8>();
        if (params.size() > 5)
            attr_relation_matrix = transformArrayIntoVector<UInt16>(params[5].get<Array>());

        ssize_t related_attr_size = argument_types.size() - RELATED_ATTR_INDEX;
        checkValidAttrIndex(attr_relation_matrix, related_attr_size);

        // arguments for different attibution mode
        String time_zone = "Asia/Shanghai";
        std::vector<UInt64> attribution_args;
        if (params.size() > 6)
        {
            attribution_args = transformArrayIntoVector<UInt64>(params[6].get<Array>());
            /// For compatible: make the specific placeholder [0] to []
            if (attribution_args.size() == 1 && attribution_args[0] == 0)
                attribution_args.clear();
        }
        if (params.size() > 7)
            time_zone = params[7].get<String>();

        DataTypePtr attr_type = argument_types[2];
        DataTypes data_types;
        if (need_group && argument_types.size() > GROUP_ATTR_INDEX)
        {
            auto group_attr_type = argument_types[GROUP_ATTR_INDEX];
            const auto * array_type = checkAndGetDataType<DataTypeArray>(group_attr_type.get());
            if (!array_type)
                throw Exception("Aggregate function " + name + " require Array type for the event group", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            attr_type = array_type->getNestedType();
            data_types.push_back(attr_type);
        }
        if (!attr_relation_matrix.empty() && argument_types.size() > RELATED_ATTR_INDEX)
        {
            for (size_t i = RELATED_ATTR_INDEX; i < argument_types.size(); ++i)
                data_types.push_back(argument_types[i]);
        }
        if (!data_types.empty())
        {
            attr_type = getLeastSupertype(data_types);
            if (checkAndGetDataType<DataTypeNullable>(attr_type.get()))
                throw Exception("Aggregate function " + name + " do not support nullable event group type yet", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        DataTypePtr data_type = argument_types[0];
        if (isNumber(data_type))
        {
            return AggregateFunctionPtr(createWithTimeAttrTypes<AggregateFunctionAttribution>(*data_type, *attr_type, 
            events_flag, need_group, attr_relation_matrix, mode_flag, attribution_args, other_transform, window, time_zone,
            argument_types, params));
        }
        throw Exception("AggregateFunction " + name + " need int or float type for its third argument", ErrorCodes::BAD_ARGUMENTS);
    }
}

void registerAggregateFunctionAttribution(AggregateFunctionFactory & factory)
{
    factory.registerFunction("attribution", createAggregateFunctionAttribution,AggregateFunctionFactory::CaseInsensitive);
}
}
