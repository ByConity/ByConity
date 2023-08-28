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

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/ITableFunctionFileLike.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Storages/StorageFile.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <boost/algorithm/string.hpp>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Storages/Distributed/DirectoryMonitor.h>



namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_FILE_NAME;
    extern const int BAD_ARGUMENTS;
}

void ITableFunctionFileLike::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    // Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.at(0)->children;

    if (args.size() != 2 && args.size() != 3 && args.size() != 4 && args.size() != 6)
        throw Exception("Table function '" + getName() + "' requires exactly 2, 3, 4 or 6 arguments: url, structure, [format], [compression_method], [aws_access_key_id, aws_secret_access_key]",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (size_t i = 0; i < 1; ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    arguments.is_function_table = true;

    arguments.url = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    arguments.structure = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    if (arguments.structure.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table structure is empty for table function '{}'",
            ast_function->formatForErrorMessage());

    if (args.size() >= 3)
    {
        args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context);
        arguments.format_name = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    }

    if (args.size() >= 4)
    {
        args[3] = evaluateConstantExpressionOrIdentifierAsLiteral(args[3], context);
        arguments.compression_method = args[3]->as<ASTLiteral &>().value.safeGet<String>();
    }

    if (args.size() == 6)
    {
        args[4] = evaluateConstantExpressionOrIdentifierAsLiteral(args[4], context);
        args[5] = evaluateConstantExpressionOrIdentifierAsLiteral(args[5], context);
        arguments.access_key_id = args[4]->as<ASTLiteral &>().value.safeGet<String>();
        arguments.access_key_secret = args[5]->as<ASTLiteral &>().value.safeGet<String>();
    }
}

ColumnsDescription ITableFunctionFileLike::getActualTableStructure(ContextPtr context) const
{
    if (arguments.structure.empty())
    {
        size_t total_bytes_to_read = 0;
        Strings paths = StorageFile::getPathsList(arguments.url, context->getUserFilesPath(), context, total_bytes_to_read);
        if (paths.empty())
            throw Exception("Cannot get table structure from file, because no files match specified name", ErrorCodes::INCORRECT_FILE_NAME);
        auto read_stream = StorageDistributedDirectoryMonitor::createSourceFromFile(paths[0]);
        return ColumnsDescription{read_stream->getOutputs().front().getHeader().getNamesAndTypesList()};
    }
    return parseColumnsListFromString(arguments.structure, context);
}

StoragePtr ITableFunctionFileLike::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    StoragePtr storage = getStorage(columns, context, table_name);
    storage->startup();
    return storage;
}


}
