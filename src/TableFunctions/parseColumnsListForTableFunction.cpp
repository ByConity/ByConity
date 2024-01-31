/*
 * Copyright 2016-2023 ClickHouse, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/Context.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY;
    extern const int ILLEGAL_COLUMN;

}

void validateDataType(const DataTypePtr & type, const DataTypeValidationSettings & settings)
{
    if (!settings.allow_suspicious_low_cardinality_types)
    {
        if (const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        {
            if (!isStringOrFixedString(*removeNullable(lc_type->getDictionaryType())))
                throw Exception(
                    ErrorCodes::SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY,
                    "Creating columns of type {} is prohibited by default due to expected negative impact on performance. "
                    "It can be enabled with the \"allow_suspicious_low_cardinality_types\" setting.",
                    lc_type->getName());
        }
    }

    if (!settings.allow_experimental_geo_types)
    {
        const auto & type_name = type->getName();
        if (type_name == "MultiPolygon" || type_name == "Polygon" || type_name == "Ring" || type_name == "Point")
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Cannot create column with type '{}' because experimental geo types are not allowed. Set setting "
                "allow_experimental_geo_types = 1 in order to allow it", type_name);
        }
    }

    if (!settings.allow_experimental_object_type)
    {
        // if (type->hasDynamicSubcolumns())
        // {
        //     throw Exception(
        //         ErrorCodes::ILLEGAL_COLUMN,
        //         "Cannot create column with type '{}' because experimental Object type is not allowed. "
        //         "Set setting allow_experimental_object_type = 1 in order to allow it", type->getName());
        // }
    }

    if (!settings.allow_suspicious_fixed_string_types)
    {
        auto basic_type = removeLowCardinality(removeNullable(type));
        if (const auto * fixed_string = typeid_cast<const DataTypeFixedString *>(basic_type.get()))
        {
            if (fixed_string->getN() > MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot create column with type '{}' because fixed string with size > {} is suspicious. "
                    "Set setting allow_suspicious_fixed_string_types = 1 in order to allow it",
                    type->getName(),
                    MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS);
        }
    }
}

ColumnsDescription parseColumnsListFromString(const std::string & structure, ContextPtr context)
{
    const Settings & settings = context->getSettingsRef();
    ParserColumnDeclarationList parser(ParserSettings::valueOf(settings));

    ASTPtr columns_list_raw = parseQuery(parser, structure, "columns declaration list", settings.max_query_size, settings.max_parser_depth);

    auto * columns_list = dynamic_cast<ASTExpressionList *>(columns_list_raw.get());
    if (!columns_list)
        throw Exception("Could not cast AST to ASTExpressionList", ErrorCodes::LOGICAL_ERROR);

    return InterpreterCreateQuery::getColumnsDescription(*columns_list, context, false);
}



bool tryParseColumnsListFromString(const std::string & structure, ColumnsDescription & columns, const ContextPtr & context, String & error)
{
    ParserColumnDeclarationList parser;
    const Settings & settings = context->getSettingsRef();

    const char * start = structure.data();
    const char * end = structure.data() + structure.size();
    ASTPtr columns_list_raw = tryParseQuery(parser, start, end, error, false, "columns declaration list", false, settings.max_query_size, settings.max_parser_depth);
    if (!columns_list_raw)
        return false;

    auto * columns_list = dynamic_cast<ASTExpressionList *>(columns_list_raw.get());
    if (!columns_list)
    {
        error = fmt::format("Invalid columns declaration list: \"{}\"", structure);
        return false;
    }

    try
    {
        columns = InterpreterCreateQuery::getColumnsDescription(*columns_list, context, false);
        auto validation_settings = DataTypeValidationSettings(context->getSettingsRef());
        for (const auto & [name, type] : columns.getAll())
            validateDataType(type, validation_settings);
        return true;
    }
    catch (...)
    {
        error = getCurrentExceptionMessage(false);
        return false;
    }
}

}
