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

#include <iomanip>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Parsers/ASTSerDerHelper.h>
#include <QueryPlan/PlanSerDerHelper.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_USAGE_OF_INPUT;
}


void ASTInsertQuery::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    settings.ostr << (settings.hilite ? hilite_keyword : "") << (is_overwrite ? "INSERT OVERWRITE " : "INSERT INTO ");
    if (table_function)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "FUNCTION ";
        table_function->formatImpl(settings, state, frame);
        if (partition_by)
        {
            settings.ostr << " PARTITION BY ";
            partition_by->formatImpl(settings, state, frame);
        }
    }
    else
        settings.ostr << (settings.hilite ? hilite_none : "")
                      << (!table_id.database_name.empty() ? backQuoteIfNeed(table_id.database_name) + "." : "") << backQuoteIfNeed(table_id.table_name);
    if (is_overwrite && overwrite_partition)
    {
        settings.ostr << " PARTITION ";
        settings.ostr << "(";
        overwrite_partition->formatImpl(settings, state, frame);
        settings.ostr << ") ";
    }

    if (columns)
    {
        settings.ostr << " (";
        columns->formatImpl(settings, state, frame);
        settings.ostr << ")";
    }

    if (select)
    {
        settings.ostr << " ";
        select->formatImpl(settings, state, frame);
    }
    else if (watch)
    {
        settings.ostr << " ";
        watch->formatImpl(settings, state, frame);
    }
    else
    {
        if (!format.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FORMAT " << (settings.hilite ? hilite_none : "") << format;
            if (in_file)
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " INFILE " << (settings.hilite ? hilite_none : "");
                in_file->formatImpl(settings, state, frame);
            }

            if (compression)
            {
                settings.ostr
                    << (settings.hilite ? hilite_keyword : "")
                    << " COMPRESSION "
                    << (settings.hilite ? hilite_none : "");
                compression->formatImpl(settings, state, frame);
            }
        }
        else
        {
            if (in_file)
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " INFILE " << (settings.hilite ? hilite_none : "");
                in_file->formatImpl(settings, state, frame);

                if (compression)
                {
                    settings.ostr
                        << (settings.hilite ? hilite_keyword : "")
                        << " COMPRESSION "
                        << (settings.hilite ? hilite_none : "");
                    compression->formatImpl(settings, state, frame);
                }
            }
            else
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " VALUES" << (settings.hilite ? hilite_none : "");
            }
        }
    }

    if (settings_ast)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "SETTINGS " << (settings.hilite ? hilite_none : "");
        settings_ast->formatImpl(settings, state, frame);
    }
}


static void tryFindInputFunctionImpl(const ASTPtr & ast, ASTPtr & input_function)
{
    if (!ast)
        return;
    for (const auto & child : ast->children)
        tryFindInputFunctionImpl(child, input_function);

    if (const auto * table_function_ast = ast->as<ASTFunction>())
    {
        if (table_function_ast->name == "input")
        {
            if (input_function)
                throw Exception("You can use 'input()' function only once per request.", ErrorCodes::INVALID_USAGE_OF_INPUT);
            input_function = ast;
        }
    }
}


void ASTInsertQuery::tryFindInputFunction(ASTPtr & input_function) const
{
    tryFindInputFunctionImpl(select, input_function);
}

void ASTInsertQuery::serialize(WriteBuffer & buf) const
{
    writeBinary(table_id.database_name, buf);
    writeBinary(table_id.table_name, buf);
    serializeAST(columns, buf);
    writeBinary(format, buf);
    serializeAST(select, buf);
    serializeAST(watch, buf);
    serializeAST(table_function, buf);
    serializeAST(in_file, buf);
    serializeAST(compression, buf);
    serializeAST(partition_by, buf);
    serializeAST(settings_ast, buf);
    writeBinary(is_overwrite, buf);
    serializeAST(overwrite_partition, buf);
}

void ASTInsertQuery::deserializeImpl(ReadBuffer & buf)
{
    readBinary(table_id.database_name, buf);
    readBinary(table_id.table_name, buf);
    columns = deserializeAST(buf);
    readBinary(format, buf);
    select = deserializeAST(buf);
    watch = deserializeAST(buf);
    table_function = deserializeAST(buf);
    in_file = deserializeAST(buf);
    compression = deserializeAST(buf);
    partition_by = deserializeAST(buf);
    settings_ast = deserializeAST(buf);
    readBinary(is_overwrite, buf);
    overwrite_partition = deserializeAST(buf);
}

ASTPtr ASTInsertQuery::deserialize(ReadBuffer & buf)
{
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->deserializeImpl(buf);
    return insert;
}

}
